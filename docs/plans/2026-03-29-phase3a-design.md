# Phase 3A — Kademlia DHT + Smart PEX Design

## Goal

Automatic peer discovery via a full Kademlia DHT over UDP, with reputation-integrated responses (RIDHT) and smart PEX over existing TCP connections. Users no longer need `--peer` flags — peers find each other.

## Architecture

```
src/network/discovery/
├── dht.rs          # DhtNode — main entry point, owns routing table + store + socket
├── routing.rs      # RoutingTable — 160 k-buckets (k=8), XOR distance
├── rpc.rs          # UDP transport — encode/decode/send/receive DHT messages
├── lookup.rs       # Parallel iterative lookups (α=5) with early termination
├── store.rs        # PeerStore — info_hash → Vec<StoredPeer> with reputation ranking
├── pex.rs          # Smart PEX — reputation-ranked peer exchange over TCP
```

Transport: UDP for DHT queries (small, fire-and-forget), TCP for file transfer + PEX (existing connections). Two separate layers.

---

## Kademlia Fundamentals

Every node gets a 160-bit Node ID (SHA-256 of peer ID, truncated). Distance = `XOR(id_a, id_b)`. Closer in XOR space = responsible for similar keys.

### K-Buckets (RoutingTable)

160 buckets. Bucket `i` stores up to 8 nodes whose XOR distance from us has bit length `i`. When a bucket is full and a new node arrives, ping the oldest — if it doesn't respond, evict it. Naturally keeps the table fresh with live nodes.

### DHT Messages (UDP, compact binary)

| Type | Purpose |
|------|---------|
| `Ping` / `Pong` | Liveness check |
| `FindNode(target_id)` | "Give me 8 closest nodes to this target" |
| `FindNodeResponse(nodes)` | Returns up to 8 `(NodeId, IP, Port)` |
| `GetPeers(info_hash)` | "Who's downloading this file?" |
| `GetPeersResponse(peers_or_nodes)` | Returns reputation-ranked peers OR closer nodes |
| `AnnouncePeer(info_hash, score)` | "I'm downloading this, here's my score" |

### Bootstrap

Connect to 1+ known bootstrap nodes, do `find_node` for your own ID. Populates routing table with nearby nodes. Re-bootstrap every 15 minutes if routing table has fewer than 20 nodes.

---

## Parallel Iterative Lookups (α=5)

Faster than BitTorrent's α=3. Flow for `get_peers(info_hash)`:

1. Pick 5 closest nodes from routing table (by XOR distance to info_hash)
2. Send `GetPeers` to all 5 in parallel
3. Responses return either peers (with reputation scores) or closer nodes
4. Add closer nodes to candidate list, pick next 5 closest unqueried
5. Repeat until no closer nodes found or 20 queries hit
6. Return all discovered peers sorted by reputation score

### Improvements over BitTorrent

- **α=5 concurrency** — 67% more queries per round than BitTorrent's α=3
- **500ms timeout** — aggressive because reputation data identifies reliable nodes fast (BitTorrent uses 2-5 seconds)
- **Early termination** — if 20+ peers found with score > 0.7, stop looking

Implemented as a single async task using `tokio::net::UdpSocket` with `tokio::select!` across all pending responses + timeout.

---

## Reputation-Integrated DHT (RIDHT)

The key differentiator. When responding to `GetPeers`, nodes return the **best** peers, not random ones.

### PeerStore

```rust
struct StoredPeer {
    peer_id: PeerId,
    addr: String,
    score: f64,
    announced_at: Instant,
    announce_count: u32,
}
```

### Ranking formula for GetPeers responses

```
rank = score * 0.6 + freshness * 0.2 + consistency * 0.2
```

- `freshness` = how recently peer announced (newer = higher)
- `consistency` = announce_count / expected_count (reliable re-announcers score higher)

Filter out peers that haven't re-announced in 15 minutes. Return top 20.

### AnnouncePeer behavior

- Include reputation score in announcement
- Re-announce every 5 minutes to signal liveness
- Nodes that only announce once get deprioritized

### Why this matters

BitTorrent's DHT returns random peers — a new downloader might get 8 dead nodes and 2 slow ones. RIDHT returns peers that are known-alive, known-fast, and recently active. First piece arrives faster.

---

## Smart PEX — Reputation-Ranked Peer Exchange

New message type `0x0A PEX` on existing TCP connections. Sent every 30 seconds.

### Message format

```
PEX {
    added: Vec<PexEntry>,   // new peers since last PEX
    dropped: Vec<PeerId>,   // peers that disconnected
}

PexEntry {
    peer_id: [u8; 20],
    addr: String,
    score: f64,
}
```

### Smart behaviors

- Entries sorted by score descending — best peers first
- Receivers merge scores: `merged = local * 0.7 + remote * 0.3`
- Peers with score < 0.2 are never shared
- Capped at 20 entries per message

### Integration

Coordinator receives PEX → passes new peers to ConnectionPool. Dashboard shows peer source: `[D]` DHT, `[P]` PEX, `[M]` manual.

---

## CLI Changes

### New user experience

```bash
# Before — manual peer flags required
aegis download <hash> --peer 127.0.0.1:6881 --output file.bin --total-size 614400

# After — automatic discovery
aegis download <hash> --output file.bin --total-size 614400 --bootstrap 127.0.0.1:6882
```

### New flags

| Flag | Default | Purpose |
|------|---------|---------|
| `--bootstrap <addr>` | none | DHT bootstrap node (repeatable) |
| `--dht-port <port>` | listen_port + 1 | UDP port for DHT |
| `--no-dht` | false | Disable DHT, use `--peer` only |

`--peer` flags still work alongside DHT for backwards compatibility.

### Seeder behavior

On startup: join DHT, announce for info_hash every 5 minutes, respond to PEX.

### Dashboard additions

- DHT status line: `DHT: 45 nodes | 12 peers found | 3 via PEX`
- Peer source indicator: `[D]` `[P]` `[M]` next to each peer

---

## Testing

### Unit tests (~22)

| Module | Tests | Count |
|--------|-------|-------|
| RoutingTable | XOR distance, k-bucket insert/evict, find_closest, bucket splitting | 8 |
| PeerStore | insert, expiry, reputation-sorted retrieval, re-announce counting | 5 |
| Lookup logic | candidate sorting, early termination, deduplication | 4 |
| PEX codec | encode/decode roundtrip, score filtering, cap at 20 | 3 |
| Smart ranking | score + freshness + consistency formula | 2 |

### Integration tests (~4)

1. DHT bootstrap — 3 nodes, verify routing tables populated
2. Peer discovery E2E — seeder announces, leecher finds via DHT, downloads
3. PEX propagation — peer A shares peer C with peer B
4. Reputation flow — high-score seeder returned first from GetPeers
