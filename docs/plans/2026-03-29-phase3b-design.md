# Phase 3B — Swarm Intelligence + Peer Reputation Design

## Goal

Make AegisTorrent's downloads smarter and faster than BitTorrent by adding adaptive peer-aware piece selection, proactive re-requests, tiered peer reputation with blacklisting, an enriched dashboard, and a lightweight JSON stats endpoint.

## Architecture

Four deliverables, two pure-logic modules + two UI/observability components:

```
src/core/
├── swarm.rs            # Adaptive piece selection + pipelining + stale detection
├── reputation.rs       # Sliding window scorer + tiered blacklist

src/network/
├── coordinator.rs      # Updated — consults swarm + reputation for all decisions

src/cli/
├── dashboard.rs        # Updated — per-peer stats, reputation bars, rarity heatmap

src/observability/
├── stats_server.rs     # NEW — JSON stats on localhost:9090
```

Data flow:
1. Coordinator receives piece events → feeds measurements to Reputation and SwarmIntel
2. When requesting pieces, coordinator asks SwarmIntel for best peer and pipeline depth
3. Reputation provides peer scores, strike levels, ban decisions
4. Dashboard and stats server read shared state via Arc<Mutex<_>>

Key principle: SwarmIntel and ReputationManager are pure logic — no async, no I/O, fully testable.

---

## Module 1: Swarm Intelligence (`src/core/swarm.rs`)

### Peer-aware piece selection

When multiple peers have the same rare piece, pick the peer with the highest reputation score rather than random. SwarmIntel asks ReputationManager to rank candidates.

### Adaptive pipeline depth

Dynamic per-peer pipeline depth based on speed percentile and reputation:

| Peer tier | Pipeline depth |
|-----------|---------------|
| Fast (top 25% by speed) | 8 |
| Average | 5 |
| Slow (bottom 25%) | 2 |
| Throttled (strike 2) | 1 |

Recalculated each time the coordinator requests pieces from a peer.

### Proactive re-requests

If a piece has been in-flight for longer than 2x the peer's average response time, send a duplicate request to another peer without waiting for timeout. Checked every 500ms in the coordinator's event loop.

### API

```rust
pub struct SwarmIntel {
    peer_response_times: HashMap<PeerId, SlidingWindow>,
}

impl SwarmIntel {
    pub fn new() -> Self
    pub fn record_response(&mut self, peer: PeerId, duration_ms: u64)
    pub fn pick_best_peer(&self, candidates: &[PeerId], reputation: &ReputationManager) -> Option<PeerId>
    pub fn pipeline_depth(&self, peer: &PeerId, reputation: &ReputationManager) -> usize
    pub fn stale_requests(&self, in_flight: &HashMap<u32, (PeerId, Instant)>) -> Vec<u32>
}
```

---

## Module 2: Peer Reputation (`src/core/reputation.rs`)

### Sliding window

Keep last 50 measurements per peer per metric. Enough for accurate medians, bounded memory.

### Metrics

| Metric | Measured | Unit |
|--------|----------|------|
| Speed | bytes received / response time | bytes/sec |
| Reliability | 1.0 for success, 0.0 for failure | ratio |
| Latency | time from Request sent to Piece received | ms |

### Composite score

Weighted combination normalized to 0.0–1.0:

```
score = 0.5 * speed_percentile + 0.3 * reliability + 0.2 * latency_percentile
```

Speed weighted heaviest because download speed is the primary differentiator.

### Tiered blacklist

| Strike | Action | Duration |
|--------|--------|----------|
| 1 | Deprioritize — picked last | permanent |
| 2 | Throttle — pipeline depth 1 | permanent |
| 3 | Temp ban — no requests | 60 seconds |
| 4+ | Session ban — disconnected | rest of session |

Strikes trigger on failed hash verification. Timeouts/disconnects reduce reliability metric but don't trigger strikes.

### API

```rust
pub struct ReputationManager {
    peers: HashMap<PeerId, PeerReputation>,
}

impl ReputationManager {
    pub fn new() -> Self
    pub fn record_success(&mut self, peer: PeerId, bytes: u64, duration_ms: u64)
    pub fn record_failure(&mut self, peer: PeerId, kind: FailureKind)
    pub fn score(&self, peer: &PeerId) -> f64
    pub fn strike_level(&self, peer: &PeerId) -> u8
    pub fn is_banned(&self, peer: &PeerId) -> bool
    pub fn rank_peers(&self, peers: &[PeerId]) -> Vec<PeerId>
    pub fn pipeline_depth_override(&self, peer: &PeerId) -> Option<usize>
}

pub enum FailureKind {
    BadHash,
    Timeout,
    Disconnect,
}
```

---

## Module 3: Enhanced Dashboard

New rows added below existing layout:

### Per-peer table (row 4)

```
  Peers:
    7365.. ████████░░ 1.8 MB/s  score:0.92  [5 pipe]
    a1b2.. ████░░░░░░ 0.3 MB/s  score:0.61  [2 pipe] ⚠
    c3d4.. ░░░░░░░░░░ BANNED     score:0.00  strike:4 ✗
```

Each peer shows: speed bar, download speed, composite score, current pipeline depth, strike indicator.

### Piece rarity heatmap (row 5)

```
  Rarity: ██ common  ▓▓ uncommon  ░░ rare  !! critical
  [██][██][▓▓][██][░░][!!][██][▓▓]
```

Visual indicator per piece based on how many peers have it.

### Shared state

Dashboard reads from an enriched `DownloadProgress` struct (or separate `DashboardState`) that includes per-peer stats and rarity data, updated by the coordinator.

---

## Module 4: JSON Stats Server (`src/observability/stats_server.rs`)

Lightweight TCP server on `127.0.0.1:9090` (configurable via `--stats-port` CLI flag).

Single endpoint: `GET /stats`

```json
{
  "progress": {
    "pieces_done": 5,
    "piece_count": 8,
    "bytes_done": 1310720,
    "bytes_total": 2097152
  },
  "peers": [
    { "id": "7365..", "speed_bps": 1887436, "score": 0.92, "strikes": 0, "pipeline": 5 }
  ],
  "rarity": [2, 2, 1, 2, 1, 0, 2, 1]
}
```

Uses `tokio::net::TcpListener` directly — no HTTP framework. Parses the GET request line, returns JSON with Content-Type header. Starts only in multi-peer download mode.

---

## Coordinator Integration

### New state

```rust
let mut reputation = ReputationManager::new();
let mut swarm = SwarmIntel::new();
let mut request_timestamps: HashMap<u32, (PeerId, Instant)> = HashMap::new();
```

### Event handling changes

**PieceReceived (verified):** record success with duration, update swarm response time.

**PieceReceived (bad hash):** record BadHash failure, check if peer is now banned → send DisconnectPeer command.

**Requesting pieces:** use `swarm.pipeline_depth(peer, &reputation)` for depth, `swarm.pick_best_peer(candidates, &reputation)` for peer selection.

**Stale request check:** every 500ms via `tokio::select!` with `tokio::time::interval`, call `swarm.stale_requests()` and re-request stale pieces from different peers.

### New PoolCommand variant

```rust
PoolCommand::DisconnectPeer { peer_id: PeerId }
```

Pool closes the connection and cleans up the peer's command channel.
