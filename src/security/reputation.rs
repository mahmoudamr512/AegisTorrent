use std::collections::HashMap;
use std::time::Instant;

use crate::core::scheduler::PeerId;
use crate::core::sliding_window::SlidingWindow;

const WINDOW_SIZE: usize = 50;
const TEMP_BAN_SECS: u64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    BadHash,
    Timeout,
    Disconnect,
}

struct PeerReputation {
    speed: SlidingWindow,
    reliability: SlidingWindow,
    latency: SlidingWindow,
    strikes: u8,
    banned_until: Option<Instant>,
    session_banned: bool,
}

impl PeerReputation {
    fn new() -> Self {
        Self {
            speed: SlidingWindow::new(WINDOW_SIZE),
            reliability: SlidingWindow::new(WINDOW_SIZE),
            latency: SlidingWindow::new(WINDOW_SIZE),
            strikes: 0,
            banned_until: None,
            session_banned: false,
        }
    }

    fn composite_score(&self) -> f64 {
        if self.session_banned {
            return 0.0;
        }
        let speed_score = if self.speed.is_empty() {
            0.5
        } else {
            (self.speed.median() / 10_000_000.0).min(1.0)
        };
        let reliability_score = if self.reliability.is_empty() {
            0.5
        } else {
            self.reliability.mean()
        };
        let latency_score = if self.latency.is_empty() {
            0.5
        } else {
            (1.0 - (self.latency.median() / 1000.0).min(1.0)).max(0.0)
        };
        0.5 * speed_score + 0.3 * reliability_score + 0.2 * latency_score
    }
}

pub struct ReputationManager {
    peers: HashMap<PeerId, PeerReputation>,
}

impl Default for ReputationManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ReputationManager {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }

    pub fn record_success(&mut self, peer: PeerId, bytes: u64, duration_ms: u64) {
        let rep = self.peers.entry(peer).or_insert_with(PeerReputation::new);
        if duration_ms > 0 {
            let speed = (bytes as f64 / duration_ms as f64) * 1000.0;
            rep.speed.push(speed);
            rep.latency.push(duration_ms as f64);
        }
        rep.reliability.push(1.0);
    }

    pub fn record_failure(&mut self, peer: PeerId, kind: FailureKind) {
        let rep = self.peers.entry(peer).or_insert_with(PeerReputation::new);
        rep.reliability.push(0.0);

        if kind == FailureKind::BadHash {
            rep.strikes += 1;
            match rep.strikes {
                3 => {
                    rep.banned_until =
                        Some(Instant::now() + std::time::Duration::from_secs(TEMP_BAN_SECS));
                }
                s if s >= 4 => {
                    rep.session_banned = true;
                }
                _ => {}
            }
        }
    }

    pub fn score(&self, peer: &PeerId) -> f64 {
        self.peers
            .get(peer)
            .map(|r| r.composite_score())
            .unwrap_or(0.5)
    }

    pub fn strike_level(&self, peer: &PeerId) -> u8 {
        self.peers.get(peer).map(|r| r.strikes).unwrap_or(0)
    }

    pub fn is_banned(&self, peer: &PeerId) -> bool {
        match self.peers.get(peer) {
            None => false,
            Some(rep) => {
                if rep.session_banned {
                    return true;
                }
                if let Some(until) = rep.banned_until {
                    return Instant::now() < until;
                }
                false
            }
        }
    }

    pub fn rank_peers(&self, peers: &[PeerId]) -> Vec<PeerId> {
        let mut scored: Vec<(PeerId, f64)> = peers
            .iter()
            .filter(|p| !self.is_banned(p))
            .map(|&p| (p, self.score(&p)))
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        scored.into_iter().map(|(p, _)| p).collect()
    }

    pub fn pipeline_depth_override(&self, peer: &PeerId) -> Option<usize> {
        let rep = self.peers.get(peer)?;
        match rep.strikes {
            0 | 1 => None,
            2 => Some(1),
            _ => Some(0),
        }
    }

    pub fn peer_speed_bps(&self, peer: &PeerId) -> f64 {
        self.peers
            .get(peer)
            .map(|r| r.speed.median())
            .unwrap_or(0.0)
    }

    pub fn remove_peer(&mut self, peer: &PeerId) {
        self.peers.remove(peer);
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
    fn new_peer_has_default_score() {
        let rm = ReputationManager::new();
        assert!((rm.score(&peer(1)) - 0.5).abs() < 1e-9);
    }

    #[test]
    fn success_improves_score() {
        let mut rm = ReputationManager::new();
        for _ in 0..10 {
            rm.record_success(peer(1), 256_000, 50);
        }
        assert!(rm.score(&peer(1)) > 0.5);
    }

    #[test]
    fn failure_reduces_reliability() {
        let mut rm = ReputationManager::new();
        for _ in 0..10 {
            rm.record_success(peer(1), 256_000, 50);
        }
        let before = rm.score(&peer(1));
        for _ in 0..5 {
            rm.record_failure(peer(1), FailureKind::Timeout);
        }
        assert!(rm.score(&peer(1)) < before);
    }

    #[test]
    fn strike_1_deprioritize() {
        let mut rm = ReputationManager::new();
        rm.record_failure(peer(1), FailureKind::BadHash);
        assert_eq!(rm.strike_level(&peer(1)), 1);
        assert!(!rm.is_banned(&peer(1)));
        assert!(rm.pipeline_depth_override(&peer(1)).is_none());
    }

    #[test]
    fn strike_2_throttle() {
        let mut rm = ReputationManager::new();
        rm.record_failure(peer(1), FailureKind::BadHash);
        rm.record_failure(peer(1), FailureKind::BadHash);
        assert_eq!(rm.strike_level(&peer(1)), 2);
        assert_eq!(rm.pipeline_depth_override(&peer(1)), Some(1));
    }

    #[test]
    fn strike_3_temp_ban() {
        let mut rm = ReputationManager::new();
        for _ in 0..3 {
            rm.record_failure(peer(1), FailureKind::BadHash);
        }
        assert!(rm.is_banned(&peer(1)));
    }

    #[test]
    fn strike_4_session_ban() {
        let mut rm = ReputationManager::new();
        for _ in 0..4 {
            rm.record_failure(peer(1), FailureKind::BadHash);
        }
        assert!(rm.is_banned(&peer(1)));
        assert_eq!(rm.score(&peer(1)), 0.0);
    }

    #[test]
    fn rank_peers_by_score() {
        let mut rm = ReputationManager::new();
        for _ in 0..10 {
            rm.record_success(peer(1), 1_000_000, 50);
        }
        for _ in 0..10 {
            rm.record_success(peer(2), 10_000, 500);
        }
        let ranked = rm.rank_peers(&[peer(1), peer(2)]);
        assert_eq!(ranked[0], peer(1));
    }

    #[test]
    fn rank_excludes_banned() {
        let mut rm = ReputationManager::new();
        rm.record_success(peer(1), 256_000, 50);
        for _ in 0..4 {
            rm.record_failure(peer(2), FailureKind::BadHash);
        }
        let ranked = rm.rank_peers(&[peer(1), peer(2)]);
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0], peer(1));
    }

    #[test]
    fn timeout_does_not_strike() {
        let mut rm = ReputationManager::new();
        for _ in 0..10 {
            rm.record_failure(peer(1), FailureKind::Timeout);
        }
        assert_eq!(rm.strike_level(&peer(1)), 0);
        assert!(!rm.is_banned(&peer(1)));
    }
}
