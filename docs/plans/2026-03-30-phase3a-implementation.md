# Phase 3A — Kademlia DHT + Smart PEX Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Automatic peer discovery via full Kademlia DHT over UDP with reputation-integrated responses (RIDHT), plus smart PEX over existing TCP connections. No more manual `--peer` flags.

**Architecture:** DHT runs as a standalone async task on a UDP socket, communicating with the coordinator via channels. PEX adds a new TCP message type on existing connections. Both feed discovered peers into the ConnectionPool. The RoutingTable, PeerStore, and lookup logic are pure state with no I/O — fully testable.

**Tech Stack:** Rust, Tokio (UdpSocket, mpsc, select!, interval), existing wire protocol codec, sha2 (for node ID derivation)

---

## Branch Strategy

All work on branch: `feat/phase3a-kademlia`
Created from: `main`
Merge via: PR to `main`

---

### Task 1: Create branch

**Files:**
- None

**Step 1: Create branch**

```bash
git checkout main && git pull
git checkout -b feat/phase3a-kademlia
```

**Step 2: Commit placeholder**

No code changes needed — just the branch.

---

### Task 2: Implement RoutingTable — XOR distance + k-buckets

**Files:**
- Create: `src/network/discovery/routing.rs`
- Modify: `src/network/discovery/mod.rs`

This is pure logic — no async, no I/O. The foundation of Kademlia.

**Step 1: Implement RoutingTable**

Create `src/network/discovery/routing.rs`:

```rust
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
    for i in 0..20 {
        if distance[i] != 0 {
            return (20 - i) * 8 - distance[i].leading_zeros() as usize;
        }
    }
    0
}

pub fn bucket_index(our_id: &NodeId, other_id: &NodeId) -> usize {
    let dist = xor_distance(our_id, other_id);
    let bl = distance_bit_length(&dist);
    if bl == 0 { 0 } else { bl - 1 }
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
            // Move to end (most recently seen)
            self.nodes.retain(|n| n.id != node.id);
            self.nodes.push(node);
            return true;
        }
        if !self.is_full() {
            self.nodes.push(node);
            return true;
        }
        false // bucket full, need to ping oldest
    }

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
        assert_eq!(closest[0].id, id(1)); // exact match is closest
    }

    #[test]
    fn ignores_own_id() {
        let mut rt = RoutingTable::new(id(0));
        assert_eq!(rt.insert(node(0)), InsertResult::Ignored);
    }

    #[test]
    fn bucket_full_returns_oldest() {
        let mut rt = RoutingTable::new(id(0));
        // Insert 8 nodes into same bucket (all have bit 0 = 1, same bucket index)
        for i in 1..=K as u8 {
            rt.insert(node(i));
        }
        let result = rt.insert(node(K as u8 + 1));
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
```

**Step 2: Update `src/network/discovery/mod.rs`**

Replace with:
```rust
pub mod dht;
pub mod pex;
pub mod routing;
pub mod tracker;
```

**Step 3: Run tests**

```bash
cargo test network::discovery::routing::tests -- --nocapture
```
Expected: all 8 tests PASS

**Step 4: Commit**

```bash
git add src/network/discovery/routing.rs src/network/discovery/mod.rs
git commit -m "feat(dht): implement RoutingTable with XOR distance and k-buckets"
```

---

### Task 3: Implement PeerStore — reputation-ranked peer storage

**Files:**
- Create: `src/network/discovery/store.rs`
- Modify: `src/network/discovery/mod.rs`

Pure logic. Stores peers per info_hash, returns them ranked by score + freshness + consistency.

**Step 1: Implement PeerStore**

Create `src/network/discovery/store.rs`:

```rust
use std::collections::HashMap;
use std::time::Instant;

use crate::core::scheduler::PeerId;
use crate::network::discovery::routing::NodeId;

const MAX_PEERS_PER_HASH: usize = 100;
const STALE_SECS: u64 = 900; // 15 minutes
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
            // Remove lowest-ranked peer
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
```

**Step 2: Update mod.rs**

Add `pub mod store;` to `src/network/discovery/mod.rs`.

**Step 3: Run tests**

```bash
cargo test network::discovery::store::tests -- --nocapture
```
Expected: all 5 tests PASS

**Step 4: Commit**

```bash
git add src/network/discovery/store.rs src/network/discovery/mod.rs
git commit -m "feat(dht): implement PeerStore with reputation-ranked retrieval (RIDHT)"
```

---

### Task 4: Implement DHT RPC — UDP message encoding/decoding

**Files:**
- Create: `src/network/discovery/rpc.rs`
- Modify: `src/network/discovery/mod.rs`

Binary encoding for DHT messages over UDP. Compact format: `[1-byte type][payload]`.

**Step 1: Implement RPC messages**

Create `src/network/discovery/rpc.rs`:

```rust
use crate::core::scheduler::PeerId;
use crate::network::discovery::routing::{NodeId, NodeInfo};

#[derive(Debug, Clone)]
pub enum DhtMessage {
    Ping {
        sender: NodeId,
    },
    Pong {
        sender: NodeId,
    },
    FindNode {
        sender: NodeId,
        target: NodeId,
    },
    FindNodeResponse {
        sender: NodeId,
        nodes: Vec<NodeInfo>,
    },
    GetPeers {
        sender: NodeId,
        info_hash: [u8; 32],
    },
    GetPeersResponse {
        sender: NodeId,
        peers: Vec<PeerEntry>,
        nodes: Vec<NodeInfo>,
    },
    AnnouncePeer {
        sender: NodeId,
        info_hash: [u8; 32],
        peer_port: u16,
        score: f64,
    },
}

#[derive(Debug, Clone)]
pub struct PeerEntry {
    pub peer_id: PeerId,
    pub addr: String,
    pub score: f64,
}

impl DhtMessage {
    pub fn sender(&self) -> &NodeId {
        match self {
            Self::Ping { sender } => sender,
            Self::Pong { sender } => sender,
            Self::FindNode { sender, .. } => sender,
            Self::FindNodeResponse { sender, .. } => sender,
            Self::GetPeers { sender, .. } => sender,
            Self::GetPeersResponse { sender, .. } => sender,
            Self::AnnouncePeer { sender, .. } => sender,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Self::Ping { sender } => {
                buf.push(0x00);
                buf.extend_from_slice(sender);
            }
            Self::Pong { sender } => {
                buf.push(0x01);
                buf.extend_from_slice(sender);
            }
            Self::FindNode { sender, target } => {
                buf.push(0x02);
                buf.extend_from_slice(sender);
                buf.extend_from_slice(target);
            }
            Self::FindNodeResponse { sender, nodes } => {
                buf.push(0x03);
                buf.extend_from_slice(sender);
                buf.push(nodes.len() as u8);
                for node in nodes {
                    buf.extend_from_slice(&node.id);
                    let addr_bytes = node.addr.as_bytes();
                    buf.push(addr_bytes.len() as u8);
                    buf.extend_from_slice(addr_bytes);
                }
            }
            Self::GetPeers { sender, info_hash } => {
                buf.push(0x04);
                buf.extend_from_slice(sender);
                buf.extend_from_slice(info_hash);
            }
            Self::GetPeersResponse {
                sender,
                peers,
                nodes,
            } => {
                buf.push(0x05);
                buf.extend_from_slice(sender);
                buf.push(peers.len() as u8);
                for peer in peers {
                    buf.extend_from_slice(&peer.peer_id);
                    let addr_bytes = peer.addr.as_bytes();
                    buf.push(addr_bytes.len() as u8);
                    buf.extend_from_slice(addr_bytes);
                    buf.extend_from_slice(&peer.score.to_be_bytes());
                }
                buf.push(nodes.len() as u8);
                for node in nodes {
                    buf.extend_from_slice(&node.id);
                    let addr_bytes = node.addr.as_bytes();
                    buf.push(addr_bytes.len() as u8);
                    buf.extend_from_slice(addr_bytes);
                }
            }
            Self::AnnouncePeer {
                sender,
                info_hash,
                peer_port,
                score,
            } => {
                buf.push(0x06);
                buf.extend_from_slice(sender);
                buf.extend_from_slice(info_hash);
                buf.extend_from_slice(&peer_port.to_be_bytes());
                buf.extend_from_slice(&score.to_be_bytes());
            }
        }
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let msg_type = data[0];
        let rest = &data[1..];

        match msg_type {
            0x00 => {
                if rest.len() < 20 { return None; }
                let mut sender = [0u8; 20];
                sender.copy_from_slice(&rest[..20]);
                Some(Self::Ping { sender })
            }
            0x01 => {
                if rest.len() < 20 { return None; }
                let mut sender = [0u8; 20];
                sender.copy_from_slice(&rest[..20]);
                Some(Self::Pong { sender })
            }
            0x02 => {
                if rest.len() < 40 { return None; }
                let mut sender = [0u8; 20];
                sender.copy_from_slice(&rest[..20]);
                let mut target = [0u8; 20];
                target.copy_from_slice(&rest[20..40]);
                Some(Self::FindNode { sender, target })
            }
            0x03 => {
                if rest.len() < 21 { return None; }
                let mut sender = [0u8; 20];
                sender.copy_from_slice(&rest[..20]);
                let count = rest[20] as usize;
                let mut offset = 21;
                let mut nodes = Vec::new();
                for _ in 0..count {
                    if offset + 21 > rest.len() { return None; }
                    let mut id = [0u8; 20];
                    id.copy_from_slice(&rest[offset..offset + 20]);
                    let addr_len = rest[offset + 20] as usize;
                    offset += 21;
                    if offset + addr_len > rest.len() { return None; }
                    let addr = String::from_utf8_lossy(&rest[offset..offset + addr_len]).to_string();
                    offset += addr_len;
                    nodes.push(NodeInfo { id, addr });
                }
                Some(Self::FindNodeResponse { sender, nodes })
            }
            0x04 => {
                if rest.len() < 52 { return None; }
                let mut sender = [0u8; 20];
                sender.copy_from_slice(&rest[..20]);
                let mut info_hash = [0u8; 32];
                info_hash.copy_from_slice(&rest[20..52]);
                Some(Self::GetPeers { sender, info_hash })
            }
            0x05 => {
                if rest.len() < 21 { return None; }
                let mut sender = [0u8; 20];
                sender.copy_from_slice(&rest[..20]);
                let peer_count = rest[20] as usize;
                let mut offset = 21;
                let mut peers = Vec::new();
                for _ in 0..peer_count {
                    if offset + 21 > rest.len() { return None; }
                    let mut peer_id = [0u8; 20];
                    peer_id.copy_from_slice(&rest[offset..offset + 20]);
                    let addr_len = rest[offset + 20] as usize;
                    offset += 21;
                    if offset + addr_len + 8 > rest.len() { return None; }
                    let addr = String::from_utf8_lossy(&rest[offset..offset + addr_len]).to_string();
                    offset += addr_len;
                    let score = f64::from_be_bytes(rest[offset..offset + 8].try_into().ok()?);
                    offset += 8;
                    peers.push(PeerEntry { peer_id, addr, score });
                }
                if offset >= rest.len() { return None; }
                let node_count = rest[offset] as usize;
                offset += 1;
                let mut nodes = Vec::new();
                for _ in 0..node_count {
                    if offset + 21 > rest.len() { return None; }
                    let mut id = [0u8; 20];
                    id.copy_from_slice(&rest[offset..offset + 20]);
                    let addr_len = rest[offset + 20] as usize;
                    offset += 21;
                    if offset + addr_len > rest.len() { return None; }
                    let addr = String::from_utf8_lossy(&rest[offset..offset + addr_len]).to_string();
                    offset += addr_len;
                    nodes.push(NodeInfo { id, addr });
                }
                Some(Self::GetPeersResponse { sender, peers, nodes })
            }
            0x06 => {
                if rest.len() < 62 { return None; }
                let mut sender = [0u8; 20];
                sender.copy_from_slice(&rest[..20]);
                let mut info_hash = [0u8; 32];
                info_hash.copy_from_slice(&rest[20..52]);
                let peer_port = u16::from_be_bytes([rest[52], rest[53]]);
                let score = f64::from_be_bytes(rest[54..62].try_into().ok()?);
                Some(Self::AnnouncePeer { sender, info_hash, peer_port, score })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_id(n: u8) -> NodeId {
        let mut id = [0u8; 20];
        id[0] = n;
        id
    }

    #[test]
    fn roundtrip_ping() {
        let msg = DhtMessage::Ping { sender: test_id(1) };
        let decoded = DhtMessage::decode(&msg.encode()).unwrap();
        assert_eq!(*decoded.sender(), test_id(1));
    }

    #[test]
    fn roundtrip_find_node() {
        let msg = DhtMessage::FindNode {
            sender: test_id(1),
            target: test_id(42),
        };
        let encoded = msg.encode();
        let decoded = DhtMessage::decode(&encoded).unwrap();
        if let DhtMessage::FindNode { sender, target } = decoded {
            assert_eq!(sender, test_id(1));
            assert_eq!(target, test_id(42));
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn roundtrip_find_node_response() {
        let nodes = vec![
            NodeInfo { id: test_id(2), addr: "127.0.0.1:6001".into() },
            NodeInfo { id: test_id(3), addr: "127.0.0.1:6002".into() },
        ];
        let msg = DhtMessage::FindNodeResponse { sender: test_id(1), nodes: nodes.clone() };
        let decoded = DhtMessage::decode(&msg.encode()).unwrap();
        if let DhtMessage::FindNodeResponse { nodes: decoded_nodes, .. } = decoded {
            assert_eq!(decoded_nodes.len(), 2);
            assert_eq!(decoded_nodes[0].id, test_id(2));
            assert_eq!(decoded_nodes[1].addr, "127.0.0.1:6002");
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn roundtrip_get_peers_response_with_peers() {
        let peers = vec![PeerEntry {
            peer_id: test_id(5),
            addr: "127.0.0.1:7000".into(),
            score: 0.85,
        }];
        let msg = DhtMessage::GetPeersResponse {
            sender: test_id(1),
            peers,
            nodes: vec![],
        };
        let decoded = DhtMessage::decode(&msg.encode()).unwrap();
        if let DhtMessage::GetPeersResponse { peers, nodes, .. } = decoded {
            assert_eq!(peers.len(), 1);
            assert!((peers[0].score - 0.85).abs() < 1e-9);
            assert!(nodes.is_empty());
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn roundtrip_announce_peer() {
        let msg = DhtMessage::AnnouncePeer {
            sender: test_id(1),
            info_hash: [0xAB; 32],
            peer_port: 6881,
            score: 0.72,
        };
        let decoded = DhtMessage::decode(&msg.encode()).unwrap();
        if let DhtMessage::AnnouncePeer { info_hash, peer_port, score, .. } = decoded {
            assert_eq!(info_hash, [0xAB; 32]);
            assert_eq!(peer_port, 6881);
            assert!((score - 0.72).abs() < 1e-9);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn decode_empty_returns_none() {
        assert!(DhtMessage::decode(&[]).is_none());
    }

    #[test]
    fn decode_unknown_type_returns_none() {
        assert!(DhtMessage::decode(&[0xFF]).is_none());
    }
}
```

**Step 2: Update mod.rs** — add `pub mod rpc;`

**Step 3: Run tests**

```bash
cargo test network::discovery::rpc::tests -- --nocapture
```
Expected: all 7 tests PASS

**Step 4: Commit**

```bash
git add src/network/discovery/rpc.rs src/network/discovery/mod.rs
git commit -m "feat(dht): implement DHT RPC message encoding/decoding over UDP"
```

---

### Task 5: Implement parallel iterative lookup

**Files:**
- Create: `src/network/discovery/lookup.rs`
- Modify: `src/network/discovery/mod.rs`

This is async — it sends UDP queries and collects responses. But the lookup *logic* (candidate tracking, deduplication, termination) is testable.

**Step 1: Implement lookup**

Create `src/network/discovery/lookup.rs`:

```rust
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
            if !self.queried.contains(&node.id)
                && !self.candidates.iter().any(|c| c.id == node.id)
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
        // Early termination: enough high-quality peers found
        let good_peers = self
            .found_peers
            .iter()
            .filter(|p| p.score >= EARLY_TERMINATE_MIN_SCORE)
            .count();
        if good_peers >= EARLY_TERMINATE_COUNT {
            return true;
        }
        // No more unqueried candidates
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
        state.add_nodes(vec![node(1), node(3)]); // node(1) is duplicate
        let batch1 = state.next_batch(); // gets 1, 2, 3
        assert_eq!(batch1.len(), 3);
        let batch2 = state.next_batch(); // nothing left
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
```

**Step 2: Update mod.rs** — add `pub mod lookup;`

**Step 3: Run tests**

```bash
cargo test network::discovery::lookup::tests -- --nocapture
```
Expected: all 4 tests PASS

**Step 4: Commit**

```bash
git add src/network/discovery/lookup.rs src/network/discovery/mod.rs
git commit -m "feat(dht): implement parallel iterative lookup with early termination"
```

---

### Task 6: Implement DhtNode — the UDP async driver

**Files:**
- Replace: `src/network/discovery/dht.rs`

This ties RoutingTable + PeerStore + RPC + LookupState together into an async task that runs on a UDP socket.

**Step 1: Implement DhtNode**

Replace `src/network/discovery/dht.rs`:

```rust
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use crate::core::scheduler::PeerId;
use crate::network::discovery::lookup::LookupState;
use crate::network::discovery::routing::{InsertResult, NodeId, NodeInfo, RoutingTable};
use crate::network::discovery::rpc::{DhtMessage, PeerEntry};
use crate::network::discovery::store::PeerStore;

const QUERY_TIMEOUT_MS: u64 = 500;
const ANNOUNCE_INTERVAL_SECS: u64 = 300;
const REFRESH_INTERVAL_SECS: u64 = 900;

pub struct DhtConfig {
    pub node_id: NodeId,
    pub listen_addr: String,
    pub bootstrap_addrs: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum DhtEvent {
    PeersFound {
        info_hash: [u8; 32],
        peers: Vec<PeerEntry>,
    },
}

pub struct DhtNode {
    config: DhtConfig,
    routing_table: RoutingTable,
    peer_store: PeerStore,
    event_tx: mpsc::Sender<DhtEvent>,
}

impl DhtNode {
    pub fn new(config: DhtConfig, event_tx: mpsc::Sender<DhtEvent>) -> Self {
        let routing_table = RoutingTable::new(config.node_id);
        Self {
            config,
            routing_table,
            peer_store: PeerStore::new(),
            event_tx,
        }
    }

    pub async fn run(mut self) {
        let socket = match UdpSocket::bind(&self.config.listen_addr).await {
            Ok(s) => Arc::new(s),
            Err(e) => {
                eprintln!("DHT failed to bind to {}: {e}", self.config.listen_addr);
                return;
            }
        };

        // Bootstrap
        for addr in &self.config.bootstrap_addrs {
            let msg = DhtMessage::FindNode {
                sender: self.config.node_id,
                target: self.config.node_id,
            };
            if let Ok(parsed) = addr.parse::<SocketAddr>() {
                let _ = socket.send_to(&msg.encode(), parsed).await;
            }
        }

        let mut buf = [0u8; 65535];
        let mut refresh_interval =
            tokio::time::interval(std::time::Duration::from_secs(REFRESH_INTERVAL_SECS));
        let mut stale_cleanup =
            tokio::time::interval(std::time::Duration::from_secs(REFRESH_INTERVAL_SECS));

        loop {
            tokio::select! {
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((len, src)) => {
                            if let Some(msg) = DhtMessage::decode(&buf[..len]) {
                                self.handle_message(msg, src, &socket).await;
                            }
                        }
                        Err(_) => continue,
                    }
                }
                _ = refresh_interval.tick() => {
                    // Refresh: find_node for our own ID to keep routing table fresh
                    let closest = self.routing_table.find_closest(&self.config.node_id, 5);
                    let msg = DhtMessage::FindNode {
                        sender: self.config.node_id,
                        target: self.config.node_id,
                    };
                    let encoded = msg.encode();
                    for node in closest {
                        if let Ok(addr) = node.addr.parse::<SocketAddr>() {
                            let _ = socket.send_to(&encoded, addr).await;
                        }
                    }
                }
                _ = stale_cleanup.tick() => {
                    self.peer_store.remove_stale();
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: DhtMessage, src: SocketAddr, socket: &UdpSocket) {
        let sender_id = *msg.sender();
        let sender_node = NodeInfo {
            id: sender_id,
            addr: src.to_string(),
        };

        // Update routing table with sender
        match self.routing_table.insert(sender_node.clone()) {
            InsertResult::BucketFull { oldest } => {
                // Ping oldest, if no response, evict (simplified: just try insert)
                let ping = DhtMessage::Ping {
                    sender: self.config.node_id,
                };
                if let Ok(addr) = oldest.addr.parse::<SocketAddr>() {
                    let _ = socket.send_to(&ping.encode(), addr).await;
                }
            }
            _ => {}
        }

        match msg {
            DhtMessage::Ping { .. } => {
                let pong = DhtMessage::Pong {
                    sender: self.config.node_id,
                };
                let _ = socket.send_to(&pong.encode(), src).await;
            }
            DhtMessage::Pong { .. } => {
                // Node is alive, already updated in routing table
            }
            DhtMessage::FindNode { target, .. } => {
                let closest = self.routing_table.find_closest(&target, 8);
                let response = DhtMessage::FindNodeResponse {
                    sender: self.config.node_id,
                    nodes: closest,
                };
                let _ = socket.send_to(&response.encode(), src).await;
            }
            DhtMessage::FindNodeResponse { nodes, .. } => {
                for node in nodes {
                    self.routing_table.insert(node);
                }
            }
            DhtMessage::GetPeers { info_hash, .. } => {
                let peers = self.peer_store.get_peers(&info_hash);
                if peers.is_empty() {
                    let closest = self.routing_table.find_closest(
                        &info_hash[..20].try_into().unwrap_or([0u8; 20]),
                        8,
                    );
                    let response = DhtMessage::GetPeersResponse {
                        sender: self.config.node_id,
                        peers: vec![],
                        nodes: closest,
                    };
                    let _ = socket.send_to(&response.encode(), src).await;
                } else {
                    let peer_entries: Vec<PeerEntry> = peers
                        .iter()
                        .map(|p| PeerEntry {
                            peer_id: p.peer_id,
                            addr: p.addr.clone(),
                            score: p.score,
                        })
                        .collect();
                    let response = DhtMessage::GetPeersResponse {
                        sender: self.config.node_id,
                        peers: peer_entries,
                        nodes: vec![],
                    };
                    let _ = socket.send_to(&response.encode(), src).await;
                }
            }
            DhtMessage::GetPeersResponse { peers, nodes, .. } => {
                if !peers.is_empty() {
                    // Forward discovered peers to coordinator
                    for peer in &peers {
                        self.peer_store.announce(
                            [0u8; 32], // will be set by lookup context
                            peer.peer_id,
                            peer.addr.clone(),
                            peer.score,
                        );
                    }
                }
                for node in nodes {
                    self.routing_table.insert(node);
                }
            }
            DhtMessage::AnnouncePeer {
                sender,
                info_hash,
                peer_port,
                score,
                ..
            } => {
                let addr = format!("{}:{}", src.ip(), peer_port);
                self.peer_store.announce(info_hash, sender, addr, score);
            }
        }
    }

    pub async fn find_peers(
        socket: &UdpSocket,
        our_id: NodeId,
        info_hash: [u8; 32],
        initial_nodes: Vec<NodeInfo>,
        event_tx: mpsc::Sender<DhtEvent>,
    ) {
        let target: NodeId = info_hash[..20].try_into().unwrap_or([0u8; 20]);
        let mut state = LookupState::new(target, initial_nodes);

        while !state.should_terminate() {
            let batch = state.next_batch();
            if batch.is_empty() {
                break;
            }

            let mut pending = Vec::new();
            for node in &batch {
                let msg = DhtMessage::GetPeers {
                    sender: our_id,
                    info_hash,
                };
                if let Ok(addr) = node.addr.parse::<SocketAddr>() {
                    let _ = socket.send_to(&msg.encode(), addr).await;
                    pending.push(addr);
                }
            }

            // Wait for responses with timeout
            let timeout = tokio::time::sleep(std::time::Duration::from_millis(QUERY_TIMEOUT_MS));
            tokio::pin!(timeout);
            let mut responses = 0;
            let mut buf = [0u8; 65535];

            loop {
                tokio::select! {
                    result = socket.recv_from(&mut buf) => {
                        if let Ok((len, _src)) = result {
                            if let Some(DhtMessage::GetPeersResponse { peers, nodes, .. }) =
                                DhtMessage::decode(&buf[..len])
                            {
                                state.add_peers(peers);
                                state.add_nodes(nodes);
                                responses += 1;
                                if responses >= batch.len() {
                                    break;
                                }
                            }
                        }
                    }
                    _ = &mut timeout => {
                        break;
                    }
                }
            }
        }

        let results = state.results();
        if !results.is_empty() {
            let _ = event_tx
                .send(DhtEvent::PeersFound {
                    info_hash,
                    peers: results,
                })
                .await;
        }
    }

    pub fn node_count(&self) -> usize {
        self.routing_table.node_count()
    }
}
```

**Step 2: Verify**

```bash
cargo check
```

**Step 3: Commit**

```bash
git add src/network/discovery/dht.rs
git commit -m "feat(dht): implement DhtNode with UDP transport and parallel find_peers"
```

---

### Task 7: Implement Smart PEX

**Files:**
- Replace: `src/network/discovery/pex.rs`
- Modify: `src/protocol/messages.rs`
- Modify: `src/protocol/codec.rs`

**Step 1: Add PEX message type**

In `src/protocol/messages.rs`, add to the `Message` enum:

```rust
Pex {
    added: Vec<PexPeer>,
    dropped: Vec<[u8; 20]>,
},
```

Add a new struct:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct PexPeer {
    pub peer_id: [u8; 20],
    pub addr: String,
    pub score: f64,
}
```

Add `message_type` mapping: `Self::Pex { .. } => 0x0A`

**Step 2: Add PEX codec encoding/decoding**

In `src/protocol/codec.rs`, add encode/decode for the `Pex` variant:

Encode:
```
[count_added: u16][for each: peer_id(20) + addr_len(1) + addr(N) + score(8)][count_dropped: u16][for each: peer_id(20)]
```

Decode: reverse of encode.

**Step 3: Implement PexManager**

Replace `src/network/discovery/pex.rs`:

```rust
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
        let previously_sent: Vec<PeerId> = self
            .last_sent
            .get(recipient)
            .cloned()
            .unwrap_or_default();

        // Added: peers we know that we haven't told this recipient about
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

        // Dropped: peers we previously sent that are no longer known
        let dropped: Vec<PeerId> = previously_sent
            .iter()
            .filter(|id| !self.known_peers.contains_key(id))
            .copied()
            .collect();

        // Update last_sent
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
                // Blend scores: trust local more
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
        mgr.add_peer(pex_peer(2, 0.1)); // below threshold
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
        assert!((p.score - 0.7).abs() < 1e-9); // 1.0 * 0.7 + 0.0 * 0.3
    }
}
```

**Step 4: Run all tests**

```bash
cargo test
```

**Step 5: Commit**

```bash
git add src/network/discovery/pex.rs src/protocol/messages.rs src/protocol/codec.rs
git commit -m "feat(pex): implement Smart PEX with reputation-ranked peer exchange"
```

---

### Task 8: Wire DHT + PEX into coordinator and CLI

**Files:**
- Modify: `src/network/coordinator.rs`
- Modify: `src/cli/main.rs`
- Modify: `src/cli/dashboard.rs`
- Modify: `src/network/pool.rs`

**Step 1: Add DHT event channel to coordinator**

The coordinator receives `DhtEvent::PeersFound` and connects to discovered peers. Add a `dht_event_rx: Option<mpsc::Receiver<DhtEvent>>` to the coordinator's `run()` method. In the `tokio::select!` loop, add a branch for DHT events.

**Step 2: Add PEX handling to pool events**

Add `PoolEvent::PexReceived` variant. When the pool's `peer_loop` receives a `Pex` message, it forwards it as a `PexReceived` event. The coordinator uses `PexManager` to merge and decide which new peers to connect to.

**Step 3: Add PEX tick in coordinator**

Every 30 seconds, generate PEX messages for all connected peers and send via `PoolCommand`.

Need new `PoolCommand::SendPex` variant.

**Step 4: Update CLI**

Add to `Download` and `Seed` variants:
```rust
#[arg(long)]
bootstrap: Vec<String>,
#[arg(long, default_value = "6882")]
dht_port: u16,
#[arg(long)]
no_dht: bool,
```

In `Seed`: start DHT node, announce periodically.
In `Download`: start DHT node, find_peers for info_hash, connect to discovered peers.

**Step 5: Update dashboard**

Add DHT status line showing: node count, peers found via DHT, peers found via PEX. Add peer source indicator `[D]` `[P]` `[M]` next to each peer.

**Step 6: Verify**

```bash
cargo build
```

**Step 7: Commit**

```bash
git add src/network/coordinator.rs src/network/pool.rs src/cli/main.rs src/cli/dashboard.rs
git commit -m "feat: wire DHT and PEX into coordinator, pool, CLI, and dashboard"
```

---

### Task 9: Integration test — DHT peer discovery

**Files:**
- Modify: `tests/integration/transfer.rs`

**Step 1: Add DHT discovery test**

```rust
#[tokio::test]
async fn dht_peer_discovery() {
    // 1. Start a DHT bootstrap node
    // 2. Start 2 seeders that announce on the DHT
    // 3. Start a leecher with only --bootstrap (no --peer)
    // 4. Leecher discovers seeders via DHT
    // 5. Download completes, byte-for-byte match
}
```

This test spins up UDP sockets for DHT and TCP listeners for file transfer on ephemeral ports.

**Step 2: Run**

```bash
cargo test --test integration -- --nocapture
```

**Step 3: Commit**

```bash
git add tests/integration/transfer.rs
git commit -m "test: add DHT peer discovery integration test"
```

---

### Task 10: Final verification + push

**Step 1: Format**

```bash
cargo fmt --all
```

**Step 2: Clippy**

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

**Step 3: All tests**

```bash
cargo test
```

**Step 4: Commit fixes**

```bash
git add -A && git commit -m "style: apply cargo fmt and clippy fixes"
```

**Step 5: Push**

```bash
git push -u origin feat/phase3a-kademlia
```

---

## Summary

| Task | What | Tests |
|------|------|-------|
| 1 | Create branch | — |
| 2 | RoutingTable — XOR distance, k-buckets | 8 |
| 3 | PeerStore — reputation-ranked storage (RIDHT) | 5 |
| 4 | DHT RPC — UDP message encode/decode | 7 |
| 5 | Parallel iterative lookup (α=5) | 4 |
| 6 | DhtNode — UDP async driver | — |
| 7 | Smart PEX — reputation-ranked exchange | 3 |
| 8 | Wire DHT + PEX into coordinator/CLI/dashboard | — |
| 9 | Integration test — DHT discovery E2E | 1 |
| 10 | Final verification + push | — |

**~28 new tests. After completion: ~106 total tests.**
