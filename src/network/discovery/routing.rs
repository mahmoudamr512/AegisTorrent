use crate::core::scheduler::PeerId;

pub type NodeId = [u8; 20];

const K: usize = 8;
const ID_BITS: usize = 160;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub id: NodeId,
    pub addr: String,
}

pub fn xor_distance(a: &NodeId, b: &NodeId) -> NodeId {
    let mut result = [0u8; 20];
    for i in 0..20 {
        result[i] = a[i] ^ b[i];
    }
    result
}

pub fn distance_bit_length(distance: &NodeId) -> usize {
    for i in (0..20).rev() {
        if distance[i] != 0 {
            return i * 8 + (8 - distance[i].leading_zeros() as usize);
        }
    }
    0
}

pub fn bucket_index(our_id: &NodeId, other_id: &NodeId) -> usize {
    let dist = xor_distance(our_id, other_id);
    let bl = distance_bit_length(&dist);
    if bl == 0 {
        0
    } else {
        bl - 1
    }
}

pub fn node_id_from_peer_id(peer_id: &PeerId) -> NodeId {
    *peer_id
}

struct KBucket {
    nodes: Vec<NodeInfo>,
}

impl KBucket {
    fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    fn contains(&self, id: &NodeId) -> bool {
        self.nodes.iter().any(|n| n.id == *id)
    }

    fn is_full(&self) -> bool {
        self.nodes.len() >= K
    }

    fn insert(&mut self, node: NodeInfo) -> bool {
        if self.contains(&node.id) {
            self.nodes.retain(|n| n.id != node.id);
            self.nodes.push(node);
            return true;
        }
        if !self.is_full() {
            self.nodes.push(node);
            return true;
        }
        false
    }

    #[allow(dead_code)]
    fn evict_oldest(&mut self) -> Option<NodeInfo> {
        if self.nodes.is_empty() {
            None
        } else {
            Some(self.nodes.remove(0))
        }
    }

    fn remove(&mut self, id: &NodeId) {
        self.nodes.retain(|n| n.id != *id);
    }

    fn nodes(&self) -> &[NodeInfo] {
        &self.nodes
    }
}

pub struct RoutingTable {
    our_id: NodeId,
    buckets: Vec<KBucket>,
}

impl RoutingTable {
    pub fn new(our_id: NodeId) -> Self {
        let mut buckets = Vec::with_capacity(ID_BITS);
        for _ in 0..ID_BITS {
            buckets.push(KBucket::new());
        }
        Self { our_id, buckets }
    }

    pub fn our_id(&self) -> &NodeId {
        &self.our_id
    }

    pub fn insert(&mut self, node: NodeInfo) -> InsertResult {
        if node.id == self.our_id {
            return InsertResult::Ignored;
        }
        let idx = bucket_index(&self.our_id, &node.id);
        if self.buckets[idx].insert(node) {
            InsertResult::Inserted
        } else {
            let oldest = self.buckets[idx].nodes()[0].clone();
            InsertResult::BucketFull { oldest }
        }
    }

    pub fn remove(&mut self, id: &NodeId) {
        let idx = bucket_index(&self.our_id, id);
        self.buckets[idx].remove(id);
    }

    pub fn find_closest(&self, target: &NodeId, count: usize) -> Vec<NodeInfo> {
        let mut all: Vec<(NodeId, NodeInfo)> = self
            .buckets
            .iter()
            .flat_map(|b| b.nodes().iter())
            .map(|n| (xor_distance(&n.id, target), n.clone()))
            .collect();
        all.sort_by(|a, b| a.0.cmp(&b.0));
        all.into_iter().take(count).map(|(_, n)| n).collect()
    }

    pub fn node_count(&self) -> usize {
        self.buckets.iter().map(|b| b.nodes().len()).sum()
    }

    pub fn evict_and_replace(&mut self, dead_id: &NodeId, new_node: NodeInfo) {
        let idx = bucket_index(&self.our_id, dead_id);
        self.buckets[idx].remove(dead_id);
        self.buckets[idx].insert(new_node);
    }
}

#[derive(Debug, PartialEq)]
pub enum InsertResult {
    Inserted,
    Ignored,
    BucketFull { oldest: NodeInfo },
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

    #[test]
    fn xor_distance_basic() {
        let a = id(0b1010_0000);
        let b = id(0b0110_0000);
        let dist = xor_distance(&a, &b);
        assert_eq!(dist[0], 0b1100_0000);
    }

    #[test]
    fn distance_bit_length_values() {
        assert_eq!(distance_bit_length(&id(0)), 0);
        assert_eq!(distance_bit_length(&id(1)), 1);
        assert_eq!(distance_bit_length(&id(0b1000_0000)), 8);
        assert_eq!(distance_bit_length(&id(0xFF)), 8);
    }

    #[test]
    fn bucket_index_assignment() {
        let our = id(0);
        assert_eq!(bucket_index(&our, &id(1)), 0);
        assert_eq!(bucket_index(&our, &id(0b1000_0000)), 7);
    }

    #[test]
    fn insert_and_find_closest() {
        let mut rt = RoutingTable::new(id(0));
        for i in 1..=10u8 {
            rt.insert(node(i));
        }
        assert_eq!(rt.node_count(), 10);
        let closest = rt.find_closest(&id(1), 3);
        assert_eq!(closest.len(), 3);
        assert_eq!(closest[0].id, id(1));
    }

    #[test]
    fn ignores_own_id() {
        let mut rt = RoutingTable::new(id(0));
        assert_eq!(rt.insert(node(0)), InsertResult::Ignored);
    }

    #[test]
    fn bucket_full_returns_oldest() {
        let mut rt = RoutingTable::new(id(0));
        for i in 128..128 + K as u8 {
            rt.insert(node(i));
        }
        let result = rt.insert(node(128 + K as u8));
        assert!(matches!(result, InsertResult::BucketFull { .. }));
    }

    #[test]
    fn evict_and_replace() {
        let mut rt = RoutingTable::new(id(0));
        for i in 1..=K as u8 {
            rt.insert(node(i));
        }
        rt.evict_and_replace(&id(1), node(100));
        let closest = rt.find_closest(&id(100), 1);
        assert_eq!(closest[0].id, id(100));
    }

    #[test]
    fn find_closest_sorts_by_distance() {
        let mut rt = RoutingTable::new(id(0));
        rt.insert(node(0xFF));
        rt.insert(node(0x01));
        rt.insert(node(0x80));
        let closest = rt.find_closest(&id(0), 3);
        assert_eq!(closest[0].id, id(0x01));
        assert_eq!(closest[1].id, id(0x80));
        assert_eq!(closest[2].id, id(0xFF));
    }
}
