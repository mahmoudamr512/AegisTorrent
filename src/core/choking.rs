use std::collections::HashMap;

use super::scheduler::PeerId;

const UNCHOKE_SLOTS: usize = 4;
const OPTIMISTIC_INTERVAL_TICKS: u32 = 3;

pub struct ChokingManager {
    upload_rates: HashMap<PeerId, u64>,
    unchoked: Vec<PeerId>,
    optimistic_peer: Option<PeerId>,
    tick_count: u32,
}

pub struct ChokingDecision {
    pub to_unchoke: Vec<PeerId>,
    pub to_choke: Vec<PeerId>,
}

impl ChokingManager {
    pub fn new() -> Self {
        Self {
            upload_rates: HashMap::new(),
            unchoked: Vec::new(),
            optimistic_peer: None,
            tick_count: 0,
        }
    }

    pub fn update_rate(&mut self, peer: PeerId, bytes: u64) {
        *self.upload_rates.entry(peer).or_insert(0) += bytes;
    }

    pub fn remove_peer(&mut self, peer: &PeerId) {
        self.upload_rates.remove(peer);
        self.unchoked.retain(|p| p != peer);
        if self.optimistic_peer.as_ref() == Some(peer) {
            self.optimistic_peer = None;
        }
    }

    pub fn decide(&mut self, interested_peers: &[PeerId]) -> ChokingDecision {
        self.tick_count += 1;

        let mut ranked: Vec<(PeerId, u64)> = interested_peers
            .iter()
            .filter_map(|p| self.upload_rates.get(p).map(|&r| (*p, r)))
            .collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1));

        let mut new_unchoked: Vec<PeerId> =
            ranked.iter().take(UNCHOKE_SLOTS).map(|(p, _)| *p).collect();

        if self.tick_count % OPTIMISTIC_INTERVAL_TICKS == 0 || self.optimistic_peer.is_none() {
            let choked_interested: Vec<PeerId> = interested_peers
                .iter()
                .filter(|p| !new_unchoked.contains(p))
                .copied()
                .collect();

            if !choked_interested.is_empty() {
                use rand::seq::SliceRandom;
                let mut rng = rand::thread_rng();
                self.optimistic_peer = choked_interested.choose(&mut rng).copied();
            }
        }

        if let Some(op) = self.optimistic_peer {
            if interested_peers.contains(&op) && !new_unchoked.contains(&op) {
                new_unchoked.push(op);
            }
        }

        let to_choke: Vec<PeerId> = self
            .unchoked
            .iter()
            .filter(|p| !new_unchoked.contains(p))
            .copied()
            .collect();
        let to_unchoke: Vec<PeerId> = new_unchoked
            .iter()
            .filter(|p| !self.unchoked.contains(p))
            .copied()
            .collect();

        self.unchoked = new_unchoked;
        self.upload_rates.values_mut().for_each(|v| *v = 0);

        ChokingDecision {
            to_unchoke,
            to_choke,
        }
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
    fn unchokes_fastest_peers() {
        let mut cm = ChokingManager::new();
        for i in 1..=6 {
            cm.update_rate(peer(i), i as u64 * 1000);
        }
        let interested: Vec<PeerId> = (1..=6).map(peer).collect();
        let decision = cm.decide(&interested);

        assert!(decision.to_unchoke.contains(&peer(6)));
        assert!(decision.to_unchoke.contains(&peer(5)));
        assert!(decision.to_unchoke.contains(&peer(4)));
        assert!(decision.to_unchoke.contains(&peer(3)));
    }

    #[test]
    fn chokes_slow_peers() {
        let mut cm = ChokingManager::new();
        for i in 1..=6 {
            cm.update_rate(peer(i), i as u64 * 1000);
        }
        let interested: Vec<PeerId> = (1..=6).map(peer).collect();
        let _ = cm.decide(&interested);

        for i in 1..=6 {
            cm.update_rate(peer(i), i as u64 * 1000);
        }
        let decision = cm.decide(&interested);
        assert!(!decision.to_unchoke.contains(&peer(1)) || cm.optimistic_peer == Some(peer(1)));
    }

    #[test]
    fn optimistic_unchoke_rotates() {
        let mut cm = ChokingManager::new();
        let interested: Vec<PeerId> = (1..=10).map(peer).collect();
        for i in 1..=10u8 {
            cm.update_rate(peer(i), 100);
        }

        let mut saw_different = false;
        let mut last_optimistic = None;
        for _ in 0..20 {
            for i in 1..=10u8 {
                cm.update_rate(peer(i), 100);
            }
            let _ = cm.decide(&interested);
            if last_optimistic.is_some() && cm.optimistic_peer != last_optimistic {
                saw_different = true;
            }
            last_optimistic = cm.optimistic_peer;
        }
        assert!(saw_different || interested.len() <= UNCHOKE_SLOTS + 1);
    }

    #[test]
    fn remove_peer_cleans_state() {
        let mut cm = ChokingManager::new();
        cm.update_rate(peer(1), 1000);
        cm.remove_peer(&peer(1));
        assert!(!cm.upload_rates.contains_key(&peer(1)));
    }
}
