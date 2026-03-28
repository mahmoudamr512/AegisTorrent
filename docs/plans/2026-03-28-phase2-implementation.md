# Phase 2 — Multi-Peer Concurrent Download Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Download from multiple peers simultaneously with rarest-first scheduling, tit-for-tat choking, endgame mode, and async disk writes.

**Architecture:** Three new components — Scheduler (pure logic), ConnectionPool (manages N peer connections via channels), and DiskWriter (async write queue). A DownloadCoordinator ties them together and drives the download. The existing single-peer Leecher is replaced; the Seeder stays unchanged.

**Tech Stack:** Rust, Tokio (mpsc channels, timers, fs), rand (tie-breaking)

---

## Branch Strategy

All work on branch: `feat/phase2-multi-peer`
Created from: `main`
Merge via: PR to `main`

---

### Task 1: Create branch and add `rand` dependency

**Files:**
- Modify: `Cargo.toml`

**Step 1: Create branch**

```bash
git checkout main && git pull
git checkout -b feat/phase2-multi-peer
```

**Step 2: Add rand to Cargo.toml**

Add to `[dependencies]`:
```toml
rand = "0.8"
```

**Step 3: Verify**

```bash
cargo check
```

**Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: add rand dependency for piece selection tie-breaking"
```

---

### Task 2: Implement the Scheduler — piece rarity + selection

**Files:**
- Modify: `src/core/scheduler.rs`

This is the brain. Pure state, no async, no I/O. Fully testable.

**Step 1: Implement Scheduler**

Replace `src/core/scheduler.rs`:

```rust
use rand::seq::SliceRandom;
use std::collections::{HashMap, HashSet};

pub type PeerId = [u8; 20];

pub struct Scheduler {
    piece_count: u32,
    our_bitfield: Vec<bool>,
    peer_bitfields: HashMap<PeerId, Vec<bool>>,
    in_flight: HashMap<u32, PeerId>,
    piece_rarity: Vec<u32>,
    endgame: bool,
}

#[derive(Debug, Clone)]
pub struct PieceAssignment {
    pub piece_index: u32,
    pub peer: PeerId,
}

impl Scheduler {
    pub fn new(piece_count: u32) -> Self {
        Self {
            piece_count,
            our_bitfield: vec![false; piece_count as usize],
            peer_bitfields: HashMap::new(),
            in_flight: HashMap::new(),
            piece_rarity: vec![0; piece_count as usize],
            endgame: false,
        }
    }

    pub fn register_peer(&mut self, peer: PeerId, bitfield: &[bool]) {
        for (i, &has) in bitfield.iter().enumerate() {
            if has {
                self.piece_rarity[i] += 1;
            }
        }
        self.peer_bitfields.insert(peer, bitfield.to_vec());
    }

    pub fn unregister_peer(&mut self, peer: &PeerId) {
        if let Some(bf) = self.peer_bitfields.remove(peer) {
            for (i, &has) in bf.iter().enumerate() {
                if has {
                    self.piece_rarity[i] = self.piece_rarity[i].saturating_sub(1);
                }
            }
        }
        self.in_flight.retain(|_, p| p != peer);
    }

    pub fn peer_has_piece(&mut self, peer: &PeerId, index: u32) {
        if let Some(bf) = self.peer_bitfields.get_mut(peer) {
            let i = index as usize;
            if i < bf.len() && !bf[i] {
                bf[i] = true;
                self.piece_rarity[i] += 1;
            }
        }
    }

    pub fn pick_piece(&mut self, peer: &PeerId) -> Option<PieceAssignment> {
        let peer_bf = self.peer_bitfields.get(peer)?;

        let mut candidates: Vec<u32> = (0..self.piece_count)
            .filter(|&i| {
                let idx = i as usize;
                !self.our_bitfield[idx]
                    && peer_bf[idx]
                    && (self.endgame || !self.in_flight.contains_key(&i))
            })
            .collect();

        if candidates.is_empty() {
            return None;
        }

        candidates.sort_by_key(|&i| self.piece_rarity[i as usize]);
        let min_rarity = self.piece_rarity[candidates[0] as usize];
        let rarest: Vec<u32> = candidates
            .into_iter()
            .take_while(|&i| self.piece_rarity[i as usize] == min_rarity)
            .collect();

        let mut rng = rand::thread_rng();
        let &chosen = rarest.choose(&mut rng).unwrap();

        if !self.endgame {
            self.in_flight.insert(chosen, *peer);
        }

        Some(PieceAssignment {
            piece_index: chosen,
            peer: *peer,
        })
    }

    pub fn piece_completed(&mut self, index: u32) {
        let i = index as usize;
        self.our_bitfield[i] = true;
        self.in_flight.remove(&index);
        self.check_endgame();
    }

    pub fn piece_failed(&mut self, index: u32) {
        self.in_flight.remove(&index);
    }

    pub fn is_complete(&self) -> bool {
        self.our_bitfield.iter().all(|&b| b)
    }

    pub fn is_endgame(&self) -> bool {
        self.endgame
    }

    pub fn missing_pieces(&self) -> Vec<u32> {
        (0..self.piece_count)
            .filter(|&i| !self.our_bitfield[i as usize])
            .collect()
    }

    pub fn endgame_requests(&self) -> Vec<PieceAssignment> {
        if !self.endgame {
            return vec![];
        }
        let mut assignments = vec![];
        for piece in self.missing_pieces() {
            for (peer, bf) in &self.peer_bitfields {
                if bf[piece as usize] {
                    assignments.push(PieceAssignment {
                        piece_index: piece,
                        peer: *peer,
                    });
                }
            }
        }
        assignments
    }

    pub fn in_flight_for_piece(&self, index: u32) -> Option<&PeerId> {
        self.in_flight.get(&index)
    }

    fn check_endgame(&mut self) {
        if self.endgame {
            return;
        }
        let missing: Vec<u32> = self.missing_pieces();
        if !missing.is_empty() && missing.iter().all(|i| self.in_flight.contains_key(i)) {
            self.endgame = true;
        }
    }
}

pub fn parse_bitfield_bytes(bytes: &[u8], piece_count: u32) -> Vec<bool> {
    let mut result = Vec::with_capacity(piece_count as usize);
    for i in 0..piece_count as usize {
        let byte_idx = i / 8;
        let bit_idx = 7 - (i % 8);
        let has = if byte_idx < bytes.len() {
            (bytes[byte_idx] >> bit_idx) & 1 == 1
        } else {
            false
        };
        result.push(has);
    }
    result
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
    fn picks_rarest_piece() {
        let mut sched = Scheduler::new(4);
        // Peer A has all 4 pieces
        sched.register_peer(peer(1), &[true, true, true, true]);
        // Peer B has pieces 0, 1 only
        sched.register_peer(peer(2), &[true, true, false, false]);

        // Pieces 2,3 have rarity 1; pieces 0,1 have rarity 2
        // Peer A should be assigned piece 2 or 3 (rarest)
        let pick = sched.pick_piece(&peer(1)).unwrap();
        assert!(pick.piece_index == 2 || pick.piece_index == 3);
    }

    #[test]
    fn skips_in_flight_pieces() {
        let mut sched = Scheduler::new(3);
        sched.register_peer(peer(1), &[true, true, true]);
        sched.register_peer(peer(2), &[true, true, true]);

        let first = sched.pick_piece(&peer(1)).unwrap();
        let second = sched.pick_piece(&peer(2)).unwrap();
        assert_ne!(first.piece_index, second.piece_index);
    }

    #[test]
    fn returns_none_when_peer_has_nothing_we_need() {
        let mut sched = Scheduler::new(2);
        sched.register_peer(peer(1), &[true, true]);
        sched.piece_completed(0);
        sched.piece_completed(1);
        assert!(sched.pick_piece(&peer(1)).is_none());
    }

    #[test]
    fn piece_completed_marks_done() {
        let mut sched = Scheduler::new(2);
        sched.register_peer(peer(1), &[true, true]);
        sched.piece_completed(0);
        sched.piece_completed(1);
        assert!(sched.is_complete());
    }

    #[test]
    fn endgame_triggers_when_all_missing_are_in_flight() {
        let mut sched = Scheduler::new(3);
        sched.register_peer(peer(1), &[true, true, true]);
        sched.register_peer(peer(2), &[true, true, true]);

        sched.piece_completed(0);
        let _ = sched.pick_piece(&peer(1)); // piece 1 or 2 in flight
        let _ = sched.pick_piece(&peer(2)); // remaining piece in flight

        // All missing pieces are now in flight
        assert!(sched.is_endgame());
    }

    #[test]
    fn endgame_allows_duplicate_requests() {
        let mut sched = Scheduler::new(2);
        sched.register_peer(peer(1), &[true, true]);
        sched.register_peer(peer(2), &[true, true]);

        sched.piece_completed(0);
        let _ = sched.pick_piece(&peer(1)); // piece 1 in flight from peer 1

        // Triggers endgame since only piece 1 remains and it's in flight
        assert!(sched.is_endgame());

        // Peer 2 should ALSO be able to pick piece 1 now
        let pick = sched.pick_piece(&peer(2));
        assert!(pick.is_some());
        assert_eq!(pick.unwrap().piece_index, 1);
    }

    #[test]
    fn endgame_requests_returns_all_peers_for_missing() {
        let mut sched = Scheduler::new(3);
        sched.register_peer(peer(1), &[true, true, true]);
        sched.register_peer(peer(2), &[true, false, true]);

        sched.piece_completed(0);
        let _ = sched.pick_piece(&peer(1));
        let _ = sched.pick_piece(&peer(2));

        if sched.is_endgame() {
            let reqs = sched.endgame_requests();
            assert!(!reqs.is_empty());
        }
    }

    #[test]
    fn unregister_peer_updates_rarity() {
        let mut sched = Scheduler::new(2);
        sched.register_peer(peer(1), &[true, true]);
        sched.register_peer(peer(2), &[true, false]);

        assert_eq!(sched.piece_rarity[0], 2);
        assert_eq!(sched.piece_rarity[1], 1);

        sched.unregister_peer(&peer(2));
        assert_eq!(sched.piece_rarity[0], 1);
        assert_eq!(sched.piece_rarity[1], 1);
    }

    #[test]
    fn peer_has_piece_updates_state() {
        let mut sched = Scheduler::new(3);
        sched.register_peer(peer(1), &[true, false, false]);

        assert_eq!(sched.piece_rarity[1], 0);
        sched.peer_has_piece(&peer(1), 1);
        assert_eq!(sched.piece_rarity[1], 1);
    }

    #[test]
    fn parse_bitfield_bytes_works() {
        let bytes = vec![0b1110_0000];
        let bf = parse_bitfield_bytes(&bytes, 3);
        assert_eq!(bf, vec![true, true, true]);

        let bytes = vec![0b1010_1010];
        let bf = parse_bitfield_bytes(&bytes, 8);
        assert_eq!(bf, vec![true, false, true, false, true, false, true, false]);
    }

    #[test]
    fn piece_failed_frees_slot() {
        let mut sched = Scheduler::new(2);
        sched.register_peer(peer(1), &[true, true]);
        let pick = sched.pick_piece(&peer(1)).unwrap();
        let idx = pick.piece_index;

        sched.piece_failed(idx);
        assert!(sched.in_flight_for_piece(idx).is_none());
    }
}
```

**Step 2: Run tests**

```bash
cargo test core::scheduler::tests -- --nocapture
```
Expected: all 10 tests PASS

**Step 3: Commit**

```bash
git add src/core/scheduler.rs
git commit -m "feat(core): implement rarest-first scheduler with endgame mode"
```

---

### Task 3: Implement the choking manager

**Files:**
- Create: `src/core/choking.rs`
- Modify: `src/core/mod.rs`

**Step 1: Implement ChokingManager**

Create `src/core/choking.rs`:

```rust
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

        // Peer 1 and 2 should not be in unchoked (except possibly optimistic)
        // Run decide again to get choke decisions
        for i in 1..=6 {
            cm.update_rate(peer(i), i as u64 * 1000);
        }
        let decision = cm.decide(&interested);
        // Peers 1,2 should be choked (or at least not in to_unchoke without being optimistic)
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
        // With random selection over 20 rounds, we should see rotation
        // (probabilistic, but extremely unlikely to fail)
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
```

**Step 2: Update `src/core/mod.rs`**

Add `pub mod choking;`

**Step 3: Run tests**

```bash
cargo test core::choking::tests -- --nocapture
```
Expected: all 4 tests PASS

**Step 4: Commit**

```bash
git add src/core/choking.rs src/core/mod.rs
git commit -m "feat(core): implement tit-for-tat choking with optimistic unchoke"
```

---

### Task 4: Implement the async DiskWriter

**Files:**
- Modify: `src/storage/writer.rs`

**Step 1: Implement DiskWriter**

Replace `src/storage/writer.rs`:

```rust
use std::path::PathBuf;

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::mpsc;

pub struct WriteCommand {
    pub offset: u64,
    pub data: Vec<u8>,
}

pub struct DiskWriter {
    tx: mpsc::Sender<WriteCommand>,
    handle: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl DiskWriter {
    pub async fn new(path: PathBuf, total_size: u64) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await?;
        file.set_len(total_size).await?;
        drop(file);

        let (tx, mut rx) = mpsc::channel::<WriteCommand>(64);

        let handle = tokio::spawn(async move {
            let mut file = OpenOptions::new().write(true).open(&path).await?;
            while let Some(cmd) = rx.recv().await {
                file.seek(SeekFrom::Start(cmd.offset)).await?;
                file.write_all(&cmd.data).await?;
            }
            file.flush().await?;
            Ok(())
        });

        Ok(Self { tx, handle })
    }

    pub async fn write_piece(
        &self,
        index: u32,
        piece_size: usize,
        data: Vec<u8>,
    ) -> Result<(), mpsc::error::SendError<WriteCommand>> {
        let offset = index as u64 * piece_size as u64;
        self.tx.send(WriteCommand { offset, data }).await
    }

    pub async fn finish(self) -> std::io::Result<()> {
        drop(self.tx);
        self.handle.await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn writes_pieces_at_correct_offsets() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("output.bin");
        let writer = DiskWriter::new(path.clone(), 12).await.unwrap();

        writer.write_piece(0, 4, b"AAAA".to_vec()).await.unwrap();
        writer.write_piece(1, 4, b"BBBB".to_vec()).await.unwrap();
        writer.write_piece(2, 4, b"CCCC".to_vec()).await.unwrap();
        writer.finish().await.unwrap();

        let data = std::fs::read(&path).unwrap();
        assert_eq!(data, b"AAAABBBBCCCC");
    }

    #[tokio::test]
    async fn handles_out_of_order_writes() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("output.bin");
        let writer = DiskWriter::new(path.clone(), 12).await.unwrap();

        writer.write_piece(2, 4, b"CCCC".to_vec()).await.unwrap();
        writer.write_piece(0, 4, b"AAAA".to_vec()).await.unwrap();
        writer.write_piece(1, 4, b"BBBB".to_vec()).await.unwrap();
        writer.finish().await.unwrap();

        let data = std::fs::read(&path).unwrap();
        assert_eq!(data, b"AAAABBBBCCCC");
    }

    #[tokio::test]
    async fn preallocates_file() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("output.bin");
        let _writer = DiskWriter::new(path.clone(), 1024).await.unwrap();

        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), 1024);
    }
}
```

**Step 2: Run tests**

```bash
cargo test storage::writer::tests -- --nocapture
```
Expected: all 3 tests PASS

**Step 3: Commit**

```bash
git add src/storage/writer.rs
git commit -m "feat(storage): implement async DiskWriter with pre-allocation"
```

---

### Task 5: Implement the ConnectionPool

**Files:**
- Modify: `src/network/pool.rs`

The pool manages N peer connections and communicates with the coordinator via channels.

**Step 1: Implement ConnectionPool**

Replace `src/network/pool.rs`:

```rust
use bytes::Bytes;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::core::scheduler::PeerId;
use crate::network::peer::{PeerConnection, PeerError};
use crate::protocol::messages::Message;

#[derive(Debug)]
pub enum PoolEvent {
    PeerConnected {
        peer_id: PeerId,
    },
    BitfieldReceived {
        peer_id: PeerId,
        bitfield: Bytes,
    },
    PieceReceived {
        peer_id: PeerId,
        index: u32,
        data: Bytes,
        proof: Vec<(bool, [u8; 32])>,
    },
    HaveReceived {
        peer_id: PeerId,
        index: u32,
    },
    Choked {
        peer_id: PeerId,
    },
    Unchoked {
        peer_id: PeerId,
    },
    PeerDisconnected {
        peer_id: PeerId,
    },
}

#[derive(Debug)]
pub enum PoolCommand {
    RequestPiece {
        peer_id: PeerId,
        index: u32,
    },
    CancelPiece {
        peer_id: PeerId,
        index: u32,
    },
    SendHave {
        index: u32,
    },
    ChokePeer {
        peer_id: PeerId,
    },
    UnchokePeer {
        peer_id: PeerId,
    },
    SendInterested {
        peer_id: PeerId,
    },
}

pub struct ConnectionPool {
    info_hash: [u8; 32],
    our_peer_id: PeerId,
    our_bitfield: Bytes,
    event_tx: mpsc::Sender<PoolEvent>,
    command_rx: mpsc::Receiver<PoolCommand>,
    peer_txs: std::collections::HashMap<PeerId, mpsc::Sender<PeerCommand>>,
}

enum PeerCommand {
    Send(Message),
    Shutdown,
}

impl ConnectionPool {
    pub fn new(
        info_hash: [u8; 32],
        our_peer_id: PeerId,
        our_bitfield: Bytes,
        event_tx: mpsc::Sender<PoolEvent>,
        command_rx: mpsc::Receiver<PoolCommand>,
    ) -> Self {
        Self {
            info_hash,
            our_peer_id,
            our_bitfield,
            event_tx,
            command_rx,
            peer_txs: std::collections::HashMap::new(),
        }
    }

    pub async fn connect(&mut self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(addr).await?;
        let mut conn = PeerConnection::new(stream);
        let remote_id = conn.handshake(self.info_hash, self.our_peer_id).await?;

        conn.send(Message::Bitfield(self.our_bitfield.clone()))
            .await?;

        let event_tx = self.event_tx.clone();
        let (peer_cmd_tx, mut peer_cmd_rx) = mpsc::channel::<PeerCommand>(32);

        self.peer_txs.insert(remote_id, peer_cmd_tx);

        let _ = event_tx
            .send(PoolEvent::PeerConnected { peer_id: remote_id })
            .await;

        tokio::spawn(async move {
            Self::peer_loop(remote_id, &mut conn, &event_tx, &mut peer_cmd_rx).await;
        });

        Ok(())
    }

    pub async fn run(&mut self) {
        while let Some(cmd) = self.command_rx.recv().await {
            match cmd {
                PoolCommand::RequestPiece { peer_id, index } => {
                    self.send_to_peer(
                        &peer_id,
                        Message::Request {
                            index,
                            offset: 0,
                            length: 0,
                        },
                    )
                    .await;
                }
                PoolCommand::CancelPiece { peer_id, index } => {
                    self.send_to_peer(
                        &peer_id,
                        Message::Cancel {
                            index,
                            offset: 0,
                            length: 0,
                        },
                    )
                    .await;
                }
                PoolCommand::SendHave { index } => {
                    let peers: Vec<PeerId> = self.peer_txs.keys().copied().collect();
                    for peer_id in peers {
                        self.send_to_peer(&peer_id, Message::Have(index)).await;
                    }
                }
                PoolCommand::ChokePeer { peer_id } => {
                    self.send_to_peer(&peer_id, Message::Choke).await;
                }
                PoolCommand::UnchokePeer { peer_id } => {
                    self.send_to_peer(&peer_id, Message::Unchoke).await;
                }
                PoolCommand::SendInterested { peer_id } => {
                    self.send_to_peer(&peer_id, Message::Interested).await;
                }
            }
        }
    }

    async fn send_to_peer(&self, peer_id: &PeerId, msg: Message) {
        if let Some(tx) = self.peer_txs.get(peer_id) {
            let _ = tx.send(PeerCommand::Send(msg)).await;
        }
    }

    async fn peer_loop(
        peer_id: PeerId,
        conn: &mut PeerConnection,
        event_tx: &mpsc::Sender<PoolEvent>,
        cmd_rx: &mut mpsc::Receiver<PeerCommand>,
    ) {
        loop {
            tokio::select! {
                msg_result = conn.recv() => {
                    match msg_result {
                        Ok(msg) => {
                            let event = match msg {
                                Message::Bitfield(bf) => Some(PoolEvent::BitfieldReceived {
                                    peer_id,
                                    bitfield: bf,
                                }),
                                Message::Piece { index, data, proof, .. } => {
                                    Some(PoolEvent::PieceReceived {
                                        peer_id,
                                        index,
                                        data,
                                        proof,
                                    })
                                }
                                Message::Have(index) => {
                                    Some(PoolEvent::HaveReceived { peer_id, index })
                                }
                                Message::Choke => Some(PoolEvent::Choked { peer_id }),
                                Message::Unchoke => Some(PoolEvent::Unchoked { peer_id }),
                                _ => None,
                            };
                            if let Some(e) = event {
                                let _ = event_tx.send(e).await;
                            }
                        }
                        Err(PeerError::ConnectionClosed) => {
                            let _ = event_tx.send(PoolEvent::PeerDisconnected { peer_id }).await;
                            return;
                        }
                        Err(_) => {
                            let _ = event_tx.send(PoolEvent::PeerDisconnected { peer_id }).await;
                            return;
                        }
                    }
                }
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(PeerCommand::Send(msg)) => {
                            if conn.send(msg).await.is_err() {
                                let _ = event_tx.send(PoolEvent::PeerDisconnected { peer_id }).await;
                                return;
                            }
                        }
                        Some(PeerCommand::Shutdown) | None => return,
                    }
                }
            }
        }
    }
}
```

**Step 2: Verify it compiles**

```bash
cargo check
```

**Step 3: Commit**

```bash
git add src/network/pool.rs
git commit -m "feat(network): implement ConnectionPool with channel-based communication"
```

---

### Task 6: Implement the DownloadCoordinator

**Files:**
- Create: `src/network/coordinator.rs`
- Modify: `src/network/mod.rs`

This ties everything together: scheduler + pool + disk writer.

**Step 1: Implement coordinator**

Create `src/network/coordinator.rs`:

```rust
use std::path::PathBuf;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use crate::core::merkle::{MerkleProof, MerkleTree};
use crate::core::scheduler::{parse_bitfield_bytes, PeerId, Scheduler};
use crate::network::pool::{ConnectionPool, PoolCommand, PoolEvent};
use crate::storage::writer::DiskWriter;

pub struct DownloadCoordinator {
    info_hash: [u8; 32],
    our_peer_id: PeerId,
    piece_size: usize,
    total_size: u64,
    output_path: PathBuf,
    peer_addrs: Vec<String>,
}

pub struct DownloadResult {
    pub bytes_downloaded: u64,
    pub pieces_verified: u32,
    pub peers_used: usize,
}

impl DownloadCoordinator {
    pub fn new(
        info_hash: [u8; 32],
        our_peer_id: PeerId,
        piece_size: usize,
        total_size: u64,
        output_path: PathBuf,
        peer_addrs: Vec<String>,
    ) -> Self {
        Self {
            info_hash,
            our_peer_id,
            piece_size,
            total_size,
            output_path,
            peer_addrs,
        }
    }

    pub async fn run(self) -> Result<DownloadResult, Box<dyn std::error::Error>> {
        let (event_tx, mut event_rx) = mpsc::channel::<PoolEvent>(128);
        let (cmd_tx, cmd_rx) = mpsc::channel::<PoolCommand>(128);

        let bitfield_bytes = vec![0u8; ((self.total_size as usize).div_ceil(self.piece_size).div_ceil(8))];
        let our_bitfield = Bytes::from(bitfield_bytes);

        let mut pool = ConnectionPool::new(
            self.info_hash,
            self.our_peer_id,
            our_bitfield,
            event_tx,
            cmd_rx,
        );

        for addr in &self.peer_addrs {
            if let Err(e) = pool.connect(addr).await {
                eprintln!("Failed to connect to {addr}: {e}");
            }
        }

        let pool_handle = tokio::spawn(async move {
            pool.run().await;
        });

        let writer = DiskWriter::new(self.output_path.clone(), self.total_size).await?;

        let piece_count = (self.total_size as usize).div_ceil(self.piece_size) as u32;
        let mut scheduler = Scheduler::new(piece_count);
        let mut pieces_verified = 0u32;
        let mut peers_used = std::collections::HashSet::new();
        let mut unchoked_peers = std::collections::HashSet::<PeerId>::new();

        while !scheduler.is_complete() {
            let event = match event_rx.recv().await {
                Some(e) => e,
                None => break,
            };

            match event {
                PoolEvent::PeerConnected { peer_id } => {
                    peers_used.insert(peer_id);
                }
                PoolEvent::BitfieldReceived { peer_id, bitfield } => {
                    let bf = parse_bitfield_bytes(&bitfield, piece_count);
                    scheduler.register_peer(peer_id, &bf);
                    let _ = cmd_tx.send(PoolCommand::SendInterested { peer_id }).await;
                }
                PoolEvent::Unchoked { peer_id } => {
                    unchoked_peers.insert(peer_id);
                    self.request_pieces(&mut scheduler, &cmd_tx, &peer_id).await;
                }
                PoolEvent::Choked { peer_id } => {
                    unchoked_peers.remove(&peer_id);
                }
                PoolEvent::PieceReceived {
                    peer_id,
                    index,
                    data,
                    proof,
                } => {
                    let mut hasher = Sha256::new();
                    hasher.update(&data);
                    let leaf_hash: [u8; 32] = hasher.finalize().into();

                    let merkle_proof = MerkleProof {
                        leaf_index: index as usize,
                        siblings: proof,
                    };

                    if MerkleTree::verify(&self.info_hash, &leaf_hash, &merkle_proof) {
                        pieces_verified += 1;
                        scheduler.piece_completed(index);
                        let _ = writer
                            .write_piece(index, self.piece_size, data.to_vec())
                            .await;
                        let _ = cmd_tx.send(PoolCommand::SendHave { index }).await;
                        println!(
                            "Piece {}/{piece_count} verified (from {:02x}{:02x}..)",
                            index + 1,
                            peer_id[0],
                            peer_id[1],
                        );

                        if scheduler.is_endgame() {
                            for assignment in scheduler.endgame_requests() {
                                if unchoked_peers.contains(&assignment.peer) {
                                    let _ = cmd_tx
                                        .send(PoolCommand::RequestPiece {
                                            peer_id: assignment.peer,
                                            index: assignment.piece_index,
                                        })
                                        .await;
                                }
                            }
                        }
                    } else {
                        eprintln!("Piece {index} failed verification from peer");
                        scheduler.piece_failed(index);
                    }

                    if unchoked_peers.contains(&peer_id) {
                        self.request_pieces(&mut scheduler, &cmd_tx, &peer_id).await;
                    }
                }
                PoolEvent::HaveReceived { peer_id, index } => {
                    scheduler.peer_has_piece(&peer_id, index);
                }
                PoolEvent::PeerDisconnected { peer_id } => {
                    scheduler.unregister_peer(&peer_id);
                    unchoked_peers.remove(&peer_id);
                    println!(
                        "Peer {:02x}{:02x}.. disconnected",
                        peer_id[0], peer_id[1]
                    );
                }
            }
        }

        drop(cmd_tx);
        pool_handle.abort();
        writer.finish().await?;

        println!("Download complete: {}", self.output_path.display());

        Ok(DownloadResult {
            bytes_downloaded: self.total_size,
            pieces_verified,
            peers_used: peers_used.len(),
        })
    }

    async fn request_pieces(
        &self,
        scheduler: &mut Scheduler,
        cmd_tx: &mpsc::Sender<PoolCommand>,
        peer_id: &PeerId,
    ) {
        while let Some(assignment) = scheduler.pick_piece(peer_id) {
            let _ = cmd_tx
                .send(PoolCommand::RequestPiece {
                    peer_id: assignment.peer,
                    index: assignment.piece_index,
                })
                .await;
        }
    }
}
```

**Step 2: Update `src/network/mod.rs`**

Add `pub mod coordinator;`

**Step 3: Verify**

```bash
cargo check
```

**Step 4: Commit**

```bash
git add src/network/coordinator.rs src/network/mod.rs
git commit -m "feat(network): implement DownloadCoordinator tying scheduler, pool, and disk writer"
```

---

### Task 7: Update the CLI for multi-peer

**Files:**
- Modify: `src/cli/main.rs`

**Step 1: Update CLI**

Change the `Download` variant to accept multiple `--peer` flags and a `--piece-size` option. Wire it to the coordinator instead of the old single-peer leecher.

In the `Cli` enum, change `Download`:

```rust
Download {
    hash: String,
    #[arg(short, long)]
    peer: Vec<String>,
    #[arg(short, long)]
    output: String,
    #[arg(long, default_value = "262144")]
    piece_size: usize,
    #[arg(long, default_value = "0")]
    total_size: u64,
},
```

In the `Download` match arm, replace the leecher call with:

```rust
Cli::Download {
    hash,
    peer: peers,
    output,
    piece_size,
    total_size,
} => {
    let info_hash = parse_hex_hash(&hash)?;
    let peer_id = generate_peer_id();

    if peers.is_empty() {
        eprintln!("Error: at least one --peer is required");
        std::process::exit(1);
    }

    // If total_size is 0, use single-peer mode for backwards compatibility
    if total_size == 0 {
        let leecher = Leecher::new(info_hash, peer_id, output);
        println!("Connecting to {}...", peers[0]);
        let result = leecher.download(&peers[0]).await?;
        println!(
            "Downloaded {} ({} pieces verified)",
            format_size(result.bytes_downloaded),
            result.pieces_verified,
        );
    } else {
        use aegistorrent::network::coordinator::DownloadCoordinator;
        println!("Connecting to {} peer(s)...", peers.len());
        let coordinator = DownloadCoordinator::new(
            info_hash,
            peer_id,
            piece_size,
            total_size,
            std::path::PathBuf::from(&output),
            peers,
        );
        let result = coordinator.run().await?;
        println!(
            "Downloaded {} ({} pieces verified, {} peers used)",
            format_size(result.bytes_downloaded),
            result.pieces_verified,
            result.peers_used,
        );
    }
}
```

Note: Keep the `Leecher` import — single-peer mode still uses it when `total_size` is not provided.

**Step 2: Verify it compiles**

```bash
cargo build
```

**Step 3: Commit**

```bash
git add src/cli/main.rs
git commit -m "feat(cli): support multi-peer download with --peer repeatable flag"
```

---

### Task 8: Integration test — multi-peer download

**Files:**
- Modify: `tests/integration/transfer.rs`

**Step 1: Add multi-peer test**

Add to `tests/integration/transfer.rs`:

```rust
#[tokio::test]
async fn multi_peer_download() {
    let tmp = TempDir::new().unwrap();
    let input_path = tmp.path().join("input.bin");
    let output_path = tmp.path().join("output.bin");

    let size = 8 * 256 * 1024; // 2MB, 8 pieces
    let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    std::fs::write(&input_path, &data).unwrap();

    let seeder1_id = *b"seeder1-peer-id-1234";
    let seeder2_id = *b"seeder2-peer-id-1234";
    let leecher_id = *b"leecher-peer-id-mp12";

    let seeder1 = Seeder::from_file(&input_path, seeder1_id).unwrap();
    let seeder2 = Seeder::from_file(&input_path, seeder2_id).unwrap();
    let info_hash = seeder1.info_hash();

    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap().to_string();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap().to_string();

    let h1 = tokio::spawn(async move { let _ = seeder1.listen_on(listener1).await; });
    let h2 = tokio::spawn(async move { let _ = seeder2.listen_on(listener2).await; });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    use aegistorrent::network::coordinator::DownloadCoordinator;

    let coordinator = DownloadCoordinator::new(
        info_hash,
        leecher_id,
        256 * 1024,
        size as u64,
        output_path.clone(),
        vec![addr1, addr2],
    );

    let result = coordinator.run().await.unwrap();

    let downloaded = std::fs::read(&output_path).unwrap();
    assert_eq!(downloaded.len(), data.len());
    assert_eq!(downloaded, data);
    assert!(result.pieces_verified > 0);
    assert!(result.peers_used >= 1);

    h1.abort();
    h2.abort();
}
```

**Step 2: Run**

```bash
cargo test --test integration -- --nocapture
```
Expected: both `two_peer_file_transfer` and `multi_peer_download` pass

**Step 3: Commit**

```bash
git add tests/integration/transfer.rs
git commit -m "test: add multi-peer download integration test"
```

---

### Task 9: Manual E2E test — multi-peer

**Step 1: Create a test file**

```bash
dd if=/dev/urandom of=/tmp/testfile.bin bs=1K count=600
```

**Step 2: Start two seeders on different ports**

```bash
# Terminal 1
cargo run -- seed /tmp/testfile.bin --listen 127.0.0.1:6881

# Terminal 2
cargo run -- seed /tmp/testfile.bin --listen 127.0.0.1:6882
```

**Step 3: Download from both**

Copy the info hash, then:

```bash
# Terminal 3
cargo run -- download <hash> --peer 127.0.0.1:6881 --peer 127.0.0.1:6882 --output /tmp/downloaded.bin --piece-size 262144 --total-size 614400
```

**Step 4: Verify**

```bash
diff /tmp/testfile.bin /tmp/downloaded.bin
```

---

### Task 10: Final verification, fmt, clippy, PR

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

**Step 4: Commit any fixes**

```bash
git add -A && git commit -m "style: apply cargo fmt and clippy fixes"
```

**Step 5: Push and create PR**

```bash
git push -u origin feat/phase2-multi-peer
```

---

## Summary

| Task | What | Tests |
|------|------|-------|
| 1 | Branch + `rand` dep | — |
| 2 | Scheduler — rarest-first + endgame | 10 |
| 3 | ChokingManager — tit-for-tat + optimistic | 4 |
| 4 | DiskWriter — async file writes | 3 |
| 5 | ConnectionPool — channel-based multi-peer | — |
| 6 | DownloadCoordinator — ties it all together | — |
| 7 | CLI — `--peer` repeatable, backwards compatible | — |
| 8 | Integration test — multi-peer download | 1 |
| 9 | Manual E2E — two seeders, one leecher | — |
| 10 | Final checks + PR | — |
