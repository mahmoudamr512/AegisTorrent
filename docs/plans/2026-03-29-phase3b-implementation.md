# Phase 3B — Swarm Intelligence + Peer Reputation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make downloads smarter — adaptive pipelining, peer-aware piece selection, proactive re-requests, tiered reputation scoring, enriched dashboard, and a JSON stats endpoint.

**Architecture:** Two pure-logic modules (SwarmIntel, ReputationManager) with no async or I/O, fully testable. The coordinator consults both when making decisions and feeds them measurements. A shared `DashboardState` struct (Arc<Mutex>) bridges the coordinator to the dashboard and stats server.

**Tech Stack:** Rust, Tokio, ratatui (existing), serde + serde_json (new, for stats endpoint)

---

## Branch Strategy

All work on branch: `feat/phase3b-intelligence`
Created from: `main`
Merge via: PR to `main`

---

### Task 1: Create branch and add serde dependency

**Files:**
- Modify: `Cargo.toml`

**Step 1: Create branch**

```bash
git checkout main && git pull
git checkout -b feat/phase3b-intelligence
```

**Step 2: Add serde + serde_json to Cargo.toml**

Add to `[dependencies]`:
```toml
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

**Step 3: Verify**

```bash
cargo check
```

**Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: add serde and serde_json for stats endpoint"
```

---

### Task 2: Implement SlidingWindow utility

**Files:**
- Create: `src/core/sliding_window.rs`
- Modify: `src/core/mod.rs`

This is a reusable data structure used by both SwarmIntel and ReputationManager.

**Step 1: Implement SlidingWindow**

Create `src/core/sliding_window.rs`:

```rust
pub struct SlidingWindow {
    values: Vec<f64>,
    capacity: usize,
    cursor: usize,
    count: usize,
}

impl SlidingWindow {
    pub fn new(capacity: usize) -> Self {
        Self {
            values: vec![0.0; capacity],
            capacity,
            cursor: 0,
            count: 0,
        }
    }

    pub fn push(&mut self, value: f64) {
        self.values[self.cursor] = value;
        self.cursor = (self.cursor + 1) % self.capacity;
        if self.count < self.capacity {
            self.count += 1;
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let sum: f64 = self.values[..self.count].iter().sum();
        sum / self.count as f64
    }

    pub fn median(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let mut sorted: Vec<f64> = self.values[..self.count].to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        if self.count % 2 == 0 {
            (sorted[self.count / 2 - 1] + sorted[self.count / 2]) / 2.0
        } else {
            sorted[self.count / 2]
        }
    }

    pub fn percentile(&self, p: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let mut sorted: Vec<f64> = self.values[..self.count].to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((p / 100.0) * (self.count - 1) as f64).round() as usize;
        sorted[idx.min(self.count - 1)]
    }

    pub fn sum(&self) -> f64 {
        self.values[..self.count].iter().sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_window() {
        let w = SlidingWindow::new(10);
        assert!(w.is_empty());
        assert_eq!(w.mean(), 0.0);
        assert_eq!(w.median(), 0.0);
    }

    #[test]
    fn push_and_mean() {
        let mut w = SlidingWindow::new(5);
        w.push(10.0);
        w.push(20.0);
        w.push(30.0);
        assert_eq!(w.len(), 3);
        assert!((w.mean() - 20.0).abs() < 1e-9);
    }

    #[test]
    fn median_odd() {
        let mut w = SlidingWindow::new(5);
        w.push(3.0);
        w.push(1.0);
        w.push(2.0);
        assert!((w.median() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn median_even() {
        let mut w = SlidingWindow::new(5);
        w.push(1.0);
        w.push(2.0);
        w.push(3.0);
        w.push(4.0);
        assert!((w.median() - 2.5).abs() < 1e-9);
    }

    #[test]
    fn wraps_around() {
        let mut w = SlidingWindow::new(3);
        w.push(1.0);
        w.push(2.0);
        w.push(3.0);
        w.push(100.0); // overwrites 1.0
        assert_eq!(w.len(), 3);
        assert!((w.mean() - 35.0).abs() < 1e-9); // (2+3+100)/3
    }

    #[test]
    fn percentile_values() {
        let mut w = SlidingWindow::new(100);
        for i in 1..=100 {
            w.push(i as f64);
        }
        assert!((w.percentile(50.0) - 50.0).abs() < 1.5);
        assert!((w.percentile(90.0) - 90.0).abs() < 1.5);
    }
}
```

**Step 2: Add to `src/core/mod.rs`**

Add `pub mod sliding_window;`

**Step 3: Run tests**

```bash
cargo test core::sliding_window::tests -- --nocapture
```
Expected: all 6 tests PASS

**Step 4: Commit**

```bash
git add src/core/sliding_window.rs src/core/mod.rs
git commit -m "feat(core): implement SlidingWindow for reputation and swarm metrics"
```

---

### Task 3: Implement ReputationManager

**Files:**
- Replace: `src/security/reputation.rs`

**Step 1: Implement ReputationManager**

Replace `src/security/reputation.rs`:

```rust
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
            // Normalize: assume 10 MB/s is perfect
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
            // Lower is better: 10ms = 1.0, 1000ms = 0.0
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
            let speed = (bytes as f64 / duration_ms as f64) * 1000.0; // bytes/sec
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
            0 => None,
            1 => None, // deprioritize only, no depth change
            2 => Some(1),
            _ => Some(0), // banned
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
        // peer 1: fast
        for _ in 0..10 {
            rm.record_success(peer(1), 1_000_000, 50);
        }
        // peer 2: slow
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
```

**Step 2: Run tests**

```bash
cargo test security::reputation::tests -- --nocapture
```
Expected: all 10 tests PASS

**Step 3: Commit**

```bash
git add src/security/reputation.rs
git commit -m "feat(security): implement ReputationManager with sliding window and tiered blacklist"
```

---

### Task 4: Implement SwarmIntel

**Files:**
- Replace: `src/core/swarm.rs`

**Step 1: Implement SwarmIntel**

Replace `src/core/swarm.rs`:

```rust
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
        // Check reputation override first (strike-based throttling)
        if let Some(depth) = reputation.pipeline_depth_override(peer) {
            return depth;
        }

        // Adaptive: compute peer's speed percentile relative to all peers
        let peer_speed = reputation.peer_speed_bps(peer);
        if peer_speed <= 0.0 {
            return 5; // default for unknown peers
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
            8 // fast peer
        } else if percentile <= 0.25 {
            2 // slow peer
        } else {
            5 // average
        }
    }

    pub fn stale_requests(
        &self,
        in_flight: &HashMap<u32, (PeerId, Instant)>,
    ) -> Vec<u32> {
        let mut stale = Vec::new();
        for (&piece, &(ref peer, ref sent_at)) in in_flight {
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
        // Record fast response times
        for _ in 0..10 {
            swarm.record_response(peer(1), 50);
        }
        // Simulate a request sent 500ms ago (threshold = 100ms for 50ms avg)
        let mut in_flight = HashMap::new();
        let sent = Instant::now() - std::time::Duration::from_millis(500);
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
```

**Step 2: Run tests**

```bash
cargo test core::swarm::tests -- --nocapture
```
Expected: all 7 tests PASS

**Step 3: Commit**

```bash
git add src/core/swarm.rs
git commit -m "feat(core): implement SwarmIntel with adaptive pipelining and stale detection"
```

---

### Task 5: Add DisconnectPeer to ConnectionPool

**Files:**
- Modify: `src/network/pool.rs`

**Step 1: Add DisconnectPeer variant to PoolCommand**

Add to the `PoolCommand` enum:
```rust
DisconnectPeer { peer_id: PeerId },
```

**Step 2: Handle it in `run()`**

Add to the match in `run()`:
```rust
PoolCommand::DisconnectPeer { peer_id } => {
    self.peer_txs.remove(&peer_id);
}
```

Dropping the sender closes the channel, causing the peer_loop to exit.

**Step 3: Verify**

```bash
cargo check
```

**Step 4: Commit**

```bash
git add src/network/pool.rs
git commit -m "feat(network): add DisconnectPeer command to ConnectionPool"
```

---

### Task 6: Enrich DownloadProgress with per-peer stats and rarity

**Files:**
- Modify: `src/network/coordinator.rs`

The `DownloadProgress` struct needs to carry per-peer data and piece rarity for the dashboard and stats server.

**Step 1: Update DownloadProgress**

Add `serde::Serialize` derive and new fields:

```rust
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct PeerStats {
    pub id: String,
    pub speed_bps: f64,
    pub score: f64,
    pub strikes: u8,
    pub pipeline: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct DownloadProgress {
    pub pieces_done: u32,
    pub piece_count: u32,
    pub peers_connected: usize,
    pub bytes_done: u64,
    pub bytes_total: u64,
    pub complete: bool,
    pub peer_stats: Vec<PeerStats>,
    pub piece_rarity: Vec<u32>,
}
```

Update `DownloadProgress::new()` to accept `piece_count` and `bytes_total`:

```rust
impl DownloadProgress {
    pub fn new(piece_count: u32, bytes_total: u64) -> Self {
        Self {
            pieces_done: 0,
            piece_count,
            peers_connected: 0,
            bytes_done: 0,
            bytes_total,
            complete: false,
            peer_stats: Vec::new(),
            piece_rarity: vec![0; piece_count as usize],
        }
    }
}
```

**Step 2: Update call sites**

In `src/cli/main.rs`, update `DownloadProgress::new()` call to pass `piece_count` and `total_size`.

In `tests/integration/transfer.rs`, if the test passes `None` for progress, no change needed.

**Step 3: Verify**

```bash
cargo check
```

**Step 4: Commit**

```bash
git add src/network/coordinator.rs src/cli/main.rs
git commit -m "feat(network): enrich DownloadProgress with per-peer stats and rarity"
```

---

### Task 7: Wire SwarmIntel + ReputationManager into the coordinator

**Files:**
- Modify: `src/network/coordinator.rs`

This is the big integration task. The coordinator's `run()` method changes to:

1. Create `ReputationManager` and `SwarmIntel`
2. Track request timestamps in `HashMap<u32, (PeerId, Instant)>`
3. On piece verified: record success with duration, update swarm
4. On piece failed hash: record BadHash, check banned → disconnect
5. Use `swarm.pipeline_depth()` instead of hardcoded value
6. Use `swarm.pick_best_peer()` when scheduler returns candidates
7. Add `tokio::time::interval(500ms)` in event loop via `tokio::select!` for stale request checking
8. Update `DownloadProgress.peer_stats` and `piece_rarity` on each event

**Step 1: Update imports at top of coordinator.rs**

```rust
use std::time::Instant;

use crate::core::swarm::SwarmIntel;
use crate::security::reputation::{FailureKind, ReputationManager};
```

**Step 2: Add new state after existing variables in `run()`**

```rust
let mut reputation = ReputationManager::new();
let mut swarm = SwarmIntel::new();
let mut request_timestamps: HashMap<u32, (PeerId, Instant)> = HashMap::new();
```

**Step 3: Change the event loop to use `tokio::select!`**

Replace the `while !scheduler.is_complete()` loop with:

```rust
let mut stale_check = tokio::time::interval(std::time::Duration::from_millis(500));

while !scheduler.is_complete() {
    tokio::select! {
        event = event_rx.recv() => {
            let event = match event {
                Some(e) => e,
                None => break,
            };
            // ... existing match arms, modified below ...
        }
        _ = stale_check.tick() => {
            let stale = swarm.stale_requests(&request_timestamps);
            for piece in stale {
                if let Some((old_peer, _)) = request_timestamps.remove(&piece) {
                    // Find another peer for this piece
                    let peers_with_piece: Vec<PeerId> = scheduler
                        .peers_with_piece(piece)
                        .into_iter()
                        .filter(|p| *p != old_peer && unchoked_peers.contains(p) && !reputation.is_banned(p))
                        .collect();
                    if let Some(best) = swarm.pick_best_peer(&peers_with_piece, &reputation) {
                        let _ = cmd_tx
                            .send(PoolCommand::RequestPiece { peer_id: best, index: piece })
                            .await;
                        request_timestamps.insert(piece, (best, Instant::now()));
                    }
                }
            }
        }
    }
}
```

**Step 4: Modify PieceReceived (verified) arm**

After `scheduler.piece_completed(index)`, add:
```rust
if let Some((req_peer, sent_at)) = request_timestamps.remove(&index) {
    let duration = sent_at.elapsed();
    reputation.record_success(req_peer, data.len() as u64, duration.as_millis() as u64);
    swarm.record_response(req_peer, duration.as_millis() as u64);
}
```

**Step 5: Modify PieceReceived (failed hash) arm**

Replace the existing failed verification handling:
```rust
} else {
    eprintln!("Piece {index} failed verification from peer");
    scheduler.piece_failed(index);
    request_timestamps.remove(&index);
    reputation.record_failure(peer_id, FailureKind::BadHash);
    if reputation.is_banned(&peer_id) {
        let _ = cmd_tx
            .send(PoolCommand::DisconnectPeer { peer_id })
            .await;
        unchoked_peers.remove(&peer_id);
    }
}
```

**Step 6: Modify PeerDisconnected arm**

Add cleanup:
```rust
PoolEvent::PeerDisconnected { peer_id } => {
    update_progress(&progress, &|p| {
        p.peers_connected = p.peers_connected.saturating_sub(1);
    });
    scheduler.unregister_peer(&peer_id);
    unchoked_peers.remove(&peer_id);
    reputation.remove_peer(&peer_id);
    swarm.remove_peer(&peer_id);
    request_timestamps.retain(|_, (p, _)| *p != peer_id);
}
```

**Step 7: Update `request_pieces` to use swarm + reputation**

```rust
async fn request_pieces(
    scheduler: &mut Scheduler,
    cmd_tx: &mpsc::Sender<PoolCommand>,
    peer_id: &PeerId,
    swarm: &SwarmIntel,
    reputation: &ReputationManager,
    request_timestamps: &mut HashMap<u32, (PeerId, Instant)>,
) {
    if reputation.is_banned(peer_id) {
        return;
    }
    let depth = swarm.pipeline_depth(peer_id, reputation);
    let max = if scheduler.is_endgame() { 1 } else { depth };
    for _ in 0..max {
        match scheduler.pick_piece(peer_id) {
            Some(assignment) => {
                let _ = cmd_tx
                    .send(PoolCommand::RequestPiece {
                        peer_id: assignment.peer,
                        index: assignment.piece_index,
                    })
                    .await;
                request_timestamps.insert(assignment.piece_index, (assignment.peer, Instant::now()));
            }
            None => break,
        }
    }
}
```

Update all call sites to pass the new args.

**Step 8: Update progress with per-peer stats after each event**

After each match arm, before the loop continues:
```rust
// Update peer stats in progress
update_progress(&progress, &|p| {
    p.peer_stats = unchoked_peers
        .iter()
        .map(|pid| PeerStats {
            id: format!("{:02x}{:02x}", pid[0], pid[1]),
            speed_bps: reputation.peer_speed_bps(pid),
            score: reputation.score(pid),
            strikes: reputation.strike_level(pid),
            pipeline: swarm.pipeline_depth(pid, &reputation),
        })
        .collect();
    p.piece_rarity = (0..piece_count)
        .map(|i| scheduler.piece_rarity(i))
        .collect();
});
```

**Step 9: Add `peers_with_piece()` and `piece_rarity()` to Scheduler**

In `src/core/scheduler.rs`, add:

```rust
pub fn peers_with_piece(&self, index: u32) -> Vec<PeerId> {
    self.peer_bitfields
        .iter()
        .filter_map(|(peer, bf)| {
            if bf.get(index as usize).copied().unwrap_or(false) {
                Some(*peer)
            } else {
                None
            }
        })
        .collect()
}

pub fn piece_rarity(&self, index: u32) -> u32 {
    self.piece_rarity.get(index as usize).copied().unwrap_or(0)
}
```

**Step 10: Verify**

```bash
cargo test
```
Expected: all existing tests still pass

**Step 11: Commit**

```bash
git add src/network/coordinator.rs src/core/scheduler.rs
git commit -m "feat(network): wire SwarmIntel and ReputationManager into coordinator"
```

---

### Task 8: Enhance the dashboard with per-peer table and rarity heatmap

**Files:**
- Modify: `src/cli/dashboard.rs`

**Step 1: Update dashboard to read enriched DownloadProgress**

The `render` function needs two new layout rows:
- Row 4: per-peer table (id, speed bar, score, pipeline, strikes)
- Row 5: piece rarity heatmap

Update `run_dashboard` to remove the `piece_count` and `bytes_total` parameters since they're now in `DownloadProgress`.

Update `render` to add:

```rust
// Row 4: Peer table
let peer_lines: Vec<Line> = view.progress.peer_stats.iter().map(|p| {
    let bar_width = ((p.speed_bps / 10_000_000.0) * 10.0).min(10.0) as usize;
    let bar: String = "█".repeat(bar_width) + &"░".repeat(10 - bar_width);
    let status = if p.strikes >= 4 {
        " ✗".to_string()
    } else if p.strikes >= 1 {
        format!(" ⚠{}", p.strikes)
    } else {
        String::new()
    };
    Line::from(format!(
        "  {}  {} {}/s  score:{:.2}  [{}]{status}",
        p.id, bar,
        format_bytes(p.speed_bps as u64),
        p.score, p.pipeline,
    ))
}).collect();
let peers_widget = Paragraph::new(peer_lines)
    .block(Block::default().borders(Borders::ALL).title("Peers"));
frame.render_widget(peers_widget, chunks[3]);
```

```rust
// Row 5: Rarity heatmap
let rarity_cells: String = view.progress.piece_rarity.iter().map(|&r| {
    match r {
        0 => "!!",
        1 => "░░",
        2 => "▓▓",
        _ => "██",
    }
}).collect::<Vec<_>>().join("");
let rarity_widget = Paragraph::new(rarity_cells)
    .block(Block::default().borders(Borders::ALL).title("Piece Rarity"));
frame.render_widget(rarity_widget, chunks[4]);
```

Update layout constraints to add two more rows:
```rust
let chunks = Layout::default()
    .direction(Direction::Vertical)
    .constraints([
        Constraint::Length(3),  // title
        Constraint::Length(3),  // progress bar
        Constraint::Length(3),  // stats
        Constraint::Min(5),    // peer table
        Constraint::Length(3), // rarity heatmap
    ])
    .split(area);
```

**Step 2: Update `run_dashboard` signature and `main.rs` call site**

Since `piece_count` and `bytes_total` are now in `DownloadProgress`, simplify:

```rust
pub async fn run_dashboard(progress: Arc<Mutex<DownloadProgress>>) {
```

Update `main.rs` accordingly.

**Step 3: Verify it compiles**

```bash
cargo build
```

**Step 4: Commit**

```bash
git add src/cli/dashboard.rs src/cli/main.rs
git commit -m "feat(cli): enhance dashboard with per-peer stats and rarity heatmap"
```

---

### Task 9: Implement JSON stats server

**Files:**
- Create: `src/observability/stats_server.rs`
- Modify: `src/observability/mod.rs`
- Modify: `src/cli/main.rs`

**Step 1: Implement stats_server.rs**

Create `src/observability/stats_server.rs`:

```rust
use std::sync::{Arc, Mutex};

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

use crate::network::coordinator::DownloadProgress;

pub async fn run_stats_server(
    addr: &str,
    progress: Arc<Mutex<DownloadProgress>>,
) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Stats server failed to bind to {addr}: {e}");
            return;
        }
    };

    loop {
        let (mut stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(_) => continue,
        };

        let snapshot = {
            let p = progress.lock().unwrap();
            p.clone()
        };

        let body = match serde_json::to_string_pretty(&snapshot) {
            Ok(json) => json,
            Err(_) => continue,
        };

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );

        let _ = stream.write_all(response.as_bytes()).await;
    }
}
```

**Step 2: Update `src/observability/mod.rs`**

Add `pub mod stats_server;`

**Step 3: Add `--stats-port` CLI flag**

In `src/cli/main.rs`, add to the `Download` variant:
```rust
#[arg(long, default_value = "9090")]
stats_port: u16,
```

In the multi-peer download branch, spawn the stats server:
```rust
let stats_addr = format!("127.0.0.1:{stats_port}");
let stats_progress = Arc::clone(&progress);
tokio::spawn(async move {
    aegistorrent::observability::stats_server::run_stats_server(
        &stats_addr,
        stats_progress,
    ).await;
});
```

**Step 4: Verify**

```bash
cargo build
```

**Step 5: Commit**

```bash
git add src/observability/stats_server.rs src/observability/mod.rs src/cli/main.rs
git commit -m "feat(observability): add JSON stats server on /stats endpoint"
```

---

### Task 10: Integration test — reputation and adaptive behavior

**Files:**
- Modify: `tests/integration/transfer.rs`

Add a test that verifies multi-peer download still works with the new intelligence wiring. The existing `multi_peer_download` test covers this, but run the full suite:

```bash
cargo test
```

Expected: all tests pass.

**Commit:**

```bash
git add tests/integration/transfer.rs
git commit -m "test: verify intelligence wiring with existing integration tests"
```

(Only commit if changes were needed to fix tests.)

---

### Task 11: Final verification, fmt, clippy, push

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

**Step 5: Push**

```bash
git push -u origin feat/phase3b-intelligence
```

---

## Summary

| Task | What | Tests |
|------|------|-------|
| 1 | Branch + serde dep | — |
| 2 | SlidingWindow utility | 6 |
| 3 | ReputationManager — sliding window + tiered blacklist | 10 |
| 4 | SwarmIntel — adaptive pipelining + stale detection | 7 |
| 5 | DisconnectPeer pool command | — |
| 6 | Enrich DownloadProgress with per-peer stats | — |
| 7 | Wire swarm + reputation into coordinator | — |
| 8 | Enhanced dashboard — peer table + rarity heatmap | — |
| 9 | JSON stats server on localhost:9090 | — |
| 10 | Integration test verification | — |
| 11 | Final checks + push | — |
