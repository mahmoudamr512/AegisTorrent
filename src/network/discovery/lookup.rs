use std::collections::HashSet;

use crate::network::discovery::routing::{xor_distance, NodeId, NodeInfo};
use crate::network::discovery::rpc::PeerEntry;

const ALPHA: usize = 5;
const MAX_QUERIES: usize = 20;
const MAX_RESULTS: usize = 20;
const EARLY_TERMINATE_COUNT: usize = 20;
const EARLY_TERMINATE_MIN_SCORE: f64 = 0.7;

pub struct LookupState {
    target: NodeId,
    candidates: Vec<NodeInfo>,
    queried: HashSet<NodeId>,
    found_peers: Vec<PeerEntry>,
    query_count: usize,
}

impl LookupState {
    pub fn new(target: NodeId, initial_nodes: Vec<NodeInfo>) -> Self {
        let mut candidates = initial_nodes;
        candidates.sort_by_key(|n| xor_distance(&n.id, &target));
        Self {
            target,
            candidates,
            queried: HashSet::new(),
            found_peers: Vec::new(),
            query_count: 0,
        }
    }

    pub fn next_batch(&mut self) -> Vec<NodeInfo> {
        let mut batch = Vec::new();
        for node in &self.candidates {
            if batch.len() >= ALPHA {
                break;
            }
            if !self.queried.contains(&node.id) {
                self.queried.insert(node.id);
                batch.push(node.clone());
                self.query_count += 1;
            }
        }
        batch
    }

    pub fn add_nodes(&mut self, nodes: Vec<NodeInfo>) {
        for node in nodes {
            if !self.queried.contains(&node.id) && !self.candidates.iter().any(|c| c.id == node.id)
            {
                self.candidates.push(node);
            }
        }
        self.candidates
            .sort_by_key(|n| xor_distance(&n.id, &self.target));
    }

    pub fn add_peers(&mut self, peers: Vec<PeerEntry>) {
        for peer in peers {
            if !self.found_peers.iter().any(|p| p.peer_id == peer.peer_id) {
                self.found_peers.push(peer);
            }
        }
    }

    pub fn should_terminate(&self) -> bool {
        if self.query_count >= MAX_QUERIES {
            return true;
        }
        let good_peers = self
            .found_peers
            .iter()
            .filter(|p| p.score >= EARLY_TERMINATE_MIN_SCORE)
            .count();
        if good_peers >= EARLY_TERMINATE_COUNT {
            return true;
        }
        self.candidates.iter().all(|c| self.queried.contains(&c.id))
    }

    pub fn results(mut self) -> Vec<PeerEntry> {
        self.found_peers
            .sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        self.found_peers.truncate(MAX_RESULTS);
        self.found_peers
    }

    pub fn query_count(&self) -> usize {
        self.query_count
    }

    pub fn peer_count(&self) -> usize {
        self.found_peers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(byte: u8) -> NodeInfo {
        let mut id = [0u8; 20];
        id[0] = byte;
        NodeInfo {
            id,
            addr: format!("127.0.0.1:{}", 6000 + byte as u16),
        }
    }

    fn id(byte: u8) -> NodeId {
        let mut id = [0u8; 20];
        id[0] = byte;
        id
    }

    fn peer_entry(n: u8, score: f64) -> PeerEntry {
        let mut peer_id = [0u8; 20];
        peer_id[0] = n;
        PeerEntry {
            peer_id,
            addr: format!("127.0.0.1:{}", 7000 + n as u16),
            score,
        }
    }

    #[test]
    fn next_batch_returns_alpha_nodes() {
        let nodes: Vec<NodeInfo> = (1..=10).map(node).collect();
        let mut state = LookupState::new(id(0), nodes);
        let batch = state.next_batch();
        assert_eq!(batch.len(), ALPHA);
    }

    #[test]
    fn deduplicates_candidates() {
        let mut state = LookupState::new(id(0), vec![node(1), node(2)]);
        state.add_nodes(vec![node(1), node(3)]);
        let batch1 = state.next_batch();
        assert_eq!(batch1.len(), 3);
        let batch2 = state.next_batch();
        assert!(batch2.is_empty());
    }

    #[test]
    fn terminates_after_max_queries() {
        let nodes: Vec<NodeInfo> = (1..=30).map(node).collect();
        let mut state = LookupState::new(id(0), nodes);
        for _ in 0..4 {
            state.next_batch();
        }
        assert!(state.should_terminate());
    }

    #[test]
    fn early_termination_on_good_peers() {
        let mut state = LookupState::new(id(0), vec![node(1)]);
        for i in 0..20 {
            state.add_peers(vec![peer_entry(i, 0.9)]);
        }
        assert!(state.should_terminate());
    }
}
