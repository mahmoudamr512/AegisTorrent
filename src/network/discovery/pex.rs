use std::collections::HashMap;

use crate::core::scheduler::PeerId;
use crate::protocol::messages::PexPeer;

const MAX_PEX_ENTRIES: usize = 20;
const MIN_SHARE_SCORE: f64 = 0.2;

pub struct PexManager {
    known_peers: HashMap<PeerId, PexPeer>,
    last_sent: HashMap<PeerId, Vec<PeerId>>,
}

impl Default for PexManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PexManager {
    pub fn new() -> Self {
        Self {
            known_peers: HashMap::new(),
            last_sent: HashMap::new(),
        }
    }

    pub fn add_peer(&mut self, peer: PexPeer) {
        self.known_peers.insert(peer.peer_id, peer);
    }

    pub fn remove_peer(&mut self, peer_id: &PeerId) -> bool {
        self.known_peers.remove(peer_id).is_some()
    }

    pub fn update_score(&mut self, peer_id: &PeerId, score: f64) {
        if let Some(p) = self.known_peers.get_mut(peer_id) {
            p.score = score;
        }
    }

    pub fn generate_pex(&mut self, recipient: &PeerId) -> (Vec<PexPeer>, Vec<PeerId>) {
        let previously_sent: Vec<PeerId> =
            self.last_sent.get(recipient).cloned().unwrap_or_default();

        let mut added: Vec<PexPeer> = self
            .known_peers
            .values()
            .filter(|p| {
                p.peer_id != *recipient
                    && p.score >= MIN_SHARE_SCORE
                    && !previously_sent.contains(&p.peer_id)
            })
            .cloned()
            .collect();
        added.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        added.truncate(MAX_PEX_ENTRIES);

        let dropped: Vec<PeerId> = previously_sent
            .iter()
            .filter(|id| !self.known_peers.contains_key(*id))
            .copied()
            .collect();

        let mut sent_ids: Vec<PeerId> = self
            .known_peers
            .keys()
            .filter(|id| **id != *recipient)
            .copied()
            .collect();
        sent_ids.retain(|id| {
            self.known_peers
                .get(id)
                .map(|p| p.score >= MIN_SHARE_SCORE)
                .unwrap_or(false)
        });
        self.last_sent.insert(*recipient, sent_ids);

        (added, dropped)
    }

    pub fn merge_received(&mut self, peers: Vec<PexPeer>) {
        for incoming in peers {
            if let Some(existing) = self.known_peers.get_mut(&incoming.peer_id) {
                existing.score = existing.score * 0.7 + incoming.score * 0.3;
            } else {
                self.known_peers.insert(incoming.peer_id, incoming);
            }
        }
    }

    pub fn known_count(&self) -> usize {
        self.known_peers.len()
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

    fn pex_peer(n: u8, score: f64) -> PexPeer {
        PexPeer {
            peer_id: peer(n),
            addr: format!("127.0.0.1:{}", 6000 + n as u16),
            score,
        }
    }

    #[test]
    fn generate_pex_returns_known_peers() {
        let mut mgr = PexManager::new();
        mgr.add_peer(pex_peer(1, 0.9));
        mgr.add_peer(pex_peer(2, 0.8));
        let (added, dropped) = mgr.generate_pex(&peer(3));
        assert_eq!(added.len(), 2);
        assert!(dropped.is_empty());
    }

    #[test]
    fn filters_low_score_peers() {
        let mut mgr = PexManager::new();
        mgr.add_peer(pex_peer(1, 0.9));
        mgr.add_peer(pex_peer(2, 0.1));
        let (added, _) = mgr.generate_pex(&peer(3));
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].peer_id, peer(1));
    }

    #[test]
    fn merge_blends_scores() {
        let mut mgr = PexManager::new();
        mgr.add_peer(pex_peer(1, 1.0));
        mgr.merge_received(vec![pex_peer(1, 0.0)]);
        let p = mgr.known_peers.get(&peer(1)).unwrap();
        assert!((p.score - 0.7).abs() < 1e-9);
    }
}
