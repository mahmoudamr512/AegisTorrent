use std::collections::HashMap;
use std::time::Instant;

use crate::core::scheduler::PeerId;
use crate::core::sliding_window::SlidingWindow;
use crate::security::reputation::ReputationManager;

const RESPONSE_WINDOW: usize = 20;
const STALE_MULTIPLIER: f64 = 2.0;
const DEFAULT_TIMEOUT_MS: f64 = 5000.0;

pub struct SwarmIntel {
    peer_response_times: HashMap<PeerId, SlidingWindow>,
}

impl Default for SwarmIntel {
    fn default() -> Self {
        Self::new()
    }
}

impl SwarmIntel {
    pub fn new() -> Self {
        Self {
            peer_response_times: HashMap::new(),
        }
    }

    pub fn record_response(&mut self, peer: PeerId, duration_ms: u64) {
        let window = self
            .peer_response_times
            .entry(peer)
            .or_insert_with(|| SlidingWindow::new(RESPONSE_WINDOW));
        window.push(duration_ms as f64);
    }

    pub fn pick_best_peer(
        &self,
        candidates: &[PeerId],
        reputation: &ReputationManager,
    ) -> Option<PeerId> {
        let ranked = reputation.rank_peers(candidates);
        ranked.into_iter().next()
    }

    pub fn pipeline_depth(&self, peer: &PeerId, reputation: &ReputationManager) -> usize {
        if let Some(depth) = reputation.pipeline_depth_override(peer) {
            return depth;
        }

        let peer_speed = reputation.peer_speed_bps(peer);
        if peer_speed <= 0.0 {
            return 5;
        }

        let mut all_speeds: Vec<f64> = self
            .peer_response_times
            .keys()
            .map(|p| reputation.peer_speed_bps(p))
            .filter(|&s| s > 0.0)
            .collect();

        if all_speeds.is_empty() {
            return 5;
        }

        all_speeds.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let rank = all_speeds
            .iter()
            .position(|&s| s >= peer_speed)
            .unwrap_or(0);
        let percentile = rank as f64 / all_speeds.len() as f64;

        if percentile >= 0.75 {
            8
        } else if percentile <= 0.25 {
            2
        } else {
            5
        }
    }

    pub fn stale_requests(&self, in_flight: &HashMap<u32, (PeerId, Instant)>) -> Vec<u32> {
        let mut stale = Vec::new();
        for (&piece, (peer, sent_at)) in in_flight {
            let avg_ms = self
                .peer_response_times
                .get(peer)
                .map(|w| w.mean())
                .unwrap_or(DEFAULT_TIMEOUT_MS);
            let threshold_ms = (avg_ms * STALE_MULTIPLIER).max(1000.0);
            let elapsed_ms = sent_at.elapsed().as_millis() as f64;
            if elapsed_ms > threshold_ms {
                stale.push(piece);
            }
        }
        stale
    }

    pub fn avg_response_ms(&self, peer: &PeerId) -> f64 {
        self.peer_response_times
            .get(peer)
            .map(|w| w.mean())
            .unwrap_or(0.0)
    }

    pub fn remove_peer(&mut self, peer: &PeerId) {
        self.peer_response_times.remove(peer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(n: u8) -> PeerId {
        let mut id = [0u8; 20];
        id[0] = n;
        id
    }

    #[test]
    fn pick_best_peer_uses_reputation() {
        let mut rep = ReputationManager::new();
        for _ in 0..10 {
            rep.record_success(peer(1), 1_000_000, 50);
            rep.record_success(peer(2), 10_000, 500);
        }
        let swarm = SwarmIntel::new();
        let best = swarm.pick_best_peer(&[peer(1), peer(2)], &rep);
        assert_eq!(best, Some(peer(1)));
    }

    #[test]
    fn pick_best_peer_skips_banned() {
        let mut rep = ReputationManager::new();
        rep.record_success(peer(1), 100_000, 50);
        for _ in 0..4 {
            rep.record_failure(peer(2), crate::security::reputation::FailureKind::BadHash);
        }
        let swarm = SwarmIntel::new();
        let best = swarm.pick_best_peer(&[peer(1), peer(2)], &rep);
        assert_eq!(best, Some(peer(1)));
    }

    #[test]
    fn pipeline_depth_default() {
        let swarm = SwarmIntel::new();
        let rep = ReputationManager::new();
        assert_eq!(swarm.pipeline_depth(&peer(1), &rep), 5);
    }

    #[test]
    fn pipeline_depth_throttled_by_strikes() {
        let mut rep = ReputationManager::new();
        rep.record_failure(peer(1), crate::security::reputation::FailureKind::BadHash);
        rep.record_failure(peer(1), crate::security::reputation::FailureKind::BadHash);
        let swarm = SwarmIntel::new();
        assert_eq!(swarm.pipeline_depth(&peer(1), &rep), 1);
    }

    #[test]
    fn stale_requests_detects_slow() {
        let mut swarm = SwarmIntel::new();
        for _ in 0..10 {
            swarm.record_response(peer(1), 50);
        }
        let mut in_flight = HashMap::new();
        let sent = Instant::now() - std::time::Duration::from_millis(1500);
        in_flight.insert(0, (peer(1), sent));
        let stale = swarm.stale_requests(&in_flight);
        assert!(stale.contains(&0));
    }

    #[test]
    fn stale_requests_ignores_recent() {
        let mut swarm = SwarmIntel::new();
        for _ in 0..10 {
            swarm.record_response(peer(1), 100);
        }
        let mut in_flight = HashMap::new();
        in_flight.insert(0, (peer(1), Instant::now()));
        let stale = swarm.stale_requests(&in_flight);
        assert!(stale.is_empty());
    }

    #[test]
    fn record_response_updates_avg() {
        let mut swarm = SwarmIntel::new();
        swarm.record_response(peer(1), 100);
        swarm.record_response(peer(1), 200);
        assert!((swarm.avg_response_ms(&peer(1)) - 150.0).abs() < 1e-9);
    }
}
