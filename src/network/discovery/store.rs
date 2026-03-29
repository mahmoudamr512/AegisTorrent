use std::collections::HashMap;
use std::time::Instant;

use crate::core::scheduler::PeerId;

const MAX_PEERS_PER_HASH: usize = 100;
const STALE_SECS: u64 = 900;
const RETURN_LIMIT: usize = 20;

#[derive(Debug, Clone)]
pub struct StoredPeer {
    pub peer_id: PeerId,
    pub addr: String,
    pub score: f64,
    pub announced_at: Instant,
    pub announce_count: u32,
}

impl StoredPeer {
    fn freshness(&self) -> f64 {
        let age_secs = self.announced_at.elapsed().as_secs_f64();
        (1.0 - (age_secs / STALE_SECS as f64).min(1.0)).max(0.0)
    }

    fn consistency(&self) -> f64 {
        let expected = (self.announced_at.elapsed().as_secs() / 300 + 1) as f64;
        (self.announce_count as f64 / expected).min(1.0)
    }

    pub fn rank(&self) -> f64 {
        self.score * 0.6 + self.freshness() * 0.2 + self.consistency() * 0.2
    }

    fn is_stale(&self) -> bool {
        self.announced_at.elapsed().as_secs() > STALE_SECS
    }
}

pub struct PeerStore {
    store: HashMap<[u8; 32], Vec<StoredPeer>>,
}

impl Default for PeerStore {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerStore {
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
        }
    }

    pub fn announce(&mut self, info_hash: [u8; 32], peer_id: PeerId, addr: String, score: f64) {
        let peers = self.store.entry(info_hash).or_default();

        if let Some(existing) = peers.iter_mut().find(|p| p.peer_id == peer_id) {
            existing.announced_at = Instant::now();
            existing.announce_count += 1;
            existing.score = score;
            existing.addr = addr;
            return;
        }

        if peers.len() >= MAX_PEERS_PER_HASH {
            peers.sort_by(|a, b| a.rank().partial_cmp(&b.rank()).unwrap());
            peers.remove(0);
        }

        peers.push(StoredPeer {
            peer_id,
            addr,
            score,
            announced_at: Instant::now(),
            announce_count: 1,
        });
    }

    pub fn get_peers(&self, info_hash: &[u8; 32]) -> Vec<StoredPeer> {
        match self.store.get(info_hash) {
            None => vec![],
            Some(peers) => {
                let mut active: Vec<StoredPeer> = peers
                    .iter()
                    .filter(|p| !p.is_stale())
                    .cloned()
                    .collect();
                active.sort_by(|a, b| b.rank().partial_cmp(&a.rank()).unwrap());
                active.truncate(RETURN_LIMIT);
                active
            }
        }
    }

    pub fn peer_count(&self, info_hash: &[u8; 32]) -> usize {
        self.store.get(info_hash).map(|p| p.len()).unwrap_or(0)
    }

    pub fn remove_stale(&mut self) {
        for peers in self.store.values_mut() {
            peers.retain(|p| !p.is_stale());
        }
        self.store.retain(|_, v| !v.is_empty());
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

    fn hash(n: u8) -> [u8; 32] {
        let mut h = [0u8; 32];
        h[0] = n;
        h
    }

    #[test]
    fn announce_and_get() {
        let mut store = PeerStore::new();
        store.announce(hash(1), peer(1), "127.0.0.1:6881".into(), 0.9);
        let peers = store.get_peers(&hash(1));
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].peer_id, peer(1));
    }

    #[test]
    fn re_announce_increments_count() {
        let mut store = PeerStore::new();
        store.announce(hash(1), peer(1), "127.0.0.1:6881".into(), 0.9);
        store.announce(hash(1), peer(1), "127.0.0.1:6881".into(), 0.95);
        assert_eq!(store.peer_count(&hash(1)), 1);
        let peers = store.get_peers(&hash(1));
        assert_eq!(peers[0].announce_count, 2);
        assert!((peers[0].score - 0.95).abs() < 1e-9);
    }

    #[test]
    fn returns_ranked_by_score() {
        let mut store = PeerStore::new();
        store.announce(hash(1), peer(1), "127.0.0.1:6881".into(), 0.3);
        store.announce(hash(1), peer(2), "127.0.0.1:6882".into(), 0.9);
        store.announce(hash(1), peer(3), "127.0.0.1:6883".into(), 0.6);
        let peers = store.get_peers(&hash(1));
        assert!(peers[0].score >= peers[1].score);
        assert!(peers[1].score >= peers[2].score);
    }

    #[test]
    fn caps_at_return_limit() {
        let mut store = PeerStore::new();
        for i in 0..30u8 {
            store.announce(hash(1), peer(i), format!("127.0.0.1:{}", 6000 + i as u16), 0.5);
        }
        let peers = store.get_peers(&hash(1));
        assert!(peers.len() <= RETURN_LIMIT);
    }

    #[test]
    fn empty_hash_returns_empty() {
        let store = PeerStore::new();
        assert!(store.get_peers(&hash(99)).is_empty());
    }
}
