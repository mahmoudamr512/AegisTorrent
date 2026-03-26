<div align="center">

```
 █████╗ ███████╗ ██████╗ ██╗███████╗    ████████╗ ██████╗ ██████╗ ██████╗ ███████╗███╗   ██╗████████╗
██╔══██╗██╔════╝██╔════╝ ██║██╔════╝    ╚══██╔══╝██╔═══██╗██╔══██╗██╔══██╗██╔════╝████╗  ██║╚══██╔══╝
███████║█████╗  ██║  ███╗██║███████╗       ██║   ██║   ██║██████╔╝██████╔╝█████╗  ██╔██╗ ██║   ██║
██╔══██║██╔══╝  ██║   ██║██║╚════██║       ██║   ██║   ██║██╔══██╗██╔══██╗██╔══╝  ██║╚██╗██║   ██║
██║  ██║███████╗╚██████╔╝██║███████║       ██║   ╚██████╔╝██║  ██║██║  ██║███████╗██║ ╚████║   ██║
╚═╝  ╚═╝╚══════╝ ╚═════╝ ╚═╝╚══════╝       ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═══╝   ╚═╝
```

**High-performance · Resilient · Decentralized P2P File Distribution**

*Built from first principles. Every layer owned. No wrappers.*

<br/>

<!-- Build & Quality -->
[![Build](https://img.shields.io/github/actions/workflow/status/mahmoudamr512/aegistorrent/ci.yml?branch=main&style=flat-square&logo=github&label=build&color=4ade80)](https://github.com/mahmoudamr512/aegistorrent/actions)
[![Tests](https://img.shields.io/github/actions/workflow/status/mahmoudamr512/aegistorrent/test.yml?branch=main&style=flat-square&logo=jest&label=tests&color=4ade80)](https://github.com/mahmoudamr512/aegistorrent/actions)
[![Coverage](https://img.shields.io/codecov/c/github/mahmoudamr512/aegistorrent?style=flat-square&logo=codecov&color=4ade80)](https://codecov.io/gh/mahmoudamr512/aegistorrent)
[![Security](https://img.shields.io/snyk/vulnerabilities/github/mahmoudamr512/aegistorrent?style=flat-square&logo=snyk)](https://snyk.io/test/github/mahmoudamr512/aegistorrent)

<!-- Tech Stack -->
[![Rust](https://img.shields.io/badge/Rust-1.75+-DEA584?style=flat-square&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Tokio](https://img.shields.io/badge/Tokio-async%20runtime-blue?style=flat-square)](https://tokio.rs/)
[![License: MIT](https://img.shields.io/badge/license-MIT-818cf8?style=flat-square)](LICENSE)
[![Version](https://img.shields.io/github/v/release/mahmoudamr512/aegistorrent?style=flat-square&logo=github&color=818cf8&label=version)](https://github.com/mahmoudamr512/aegistorrent/releases)

<!-- Community -->
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-f59e0b?style=flat-square)](CONTRIBUTING.md)
[![Issues](https://img.shields.io/github/issues/mahmoudamr512/aegistorrent?style=flat-square&color=f59e0b)](https://github.com/mahmoudamr512/aegistorrent/issues)
[![Stars](https://img.shields.io/github/stars/mahmoudamr512/aegistorrent?style=flat-square&color=f59e0b&logo=github)](https://github.com/mahmoudamr512/aegistorrent/stargazers)
[![Discord](https://img.shields.io/badge/community-discord-5865f2?style=flat-square&logo=discord&logoColor=white)](https://discord.gg/aegistorrent)

<br/>

[**Docs**](https://aegistorrent.dev/docs) · [**Quickstart**](#-quickstart) · [**Architecture**](#-architecture) · [**Roadmap**](#-roadmap) · [**Contributing**](#-contributing)

</div>

---

## 🛡 What is AegisTorrent?

AegisTorrent is a **from-scratch, production-grade P2P file distribution engine** designed as both a working system and a deep study in distributed systems engineering.

This is not a BitTorrent library wrapper. Every layer — transport, protocol framing, piece scheduling, Merkle verification, peer discovery, NAT traversal — is implemented from first principles, with deliberate attention to the tradeoffs at each decision point.

| Principle | What it means in practice |
|---|---|
| 🔐 **Protocol correctness** | Every connection is governed by an explicit state machine. Invalid messages in wrong states are rejected, not silently ignored. |
| 📡 **Adaptive scheduling** | Rarest-first for swarm health. Sequential mode for streaming. Endgame mode to eliminate the last-mile stall. |
| 🌐 **Network resilience** | STUN discovery + UDP hole punching + relay fallback. Operates behind symmetric NATs. |
| ⚔️ **Adversarial tolerance** | Merkle tree verification on every piece. Local peer reputation. Automatic blacklisting on data corruption. |
| 📊 **Observability-first** | Prometheus metrics on every hot path. Structured JSON logs with levels. Real-time swarm dashboard. |

---

## 🏗 Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Application Layer                            │
│                  CLI  ·  Dashboard UI  ·  File Manager              │
└────────────────────────────┬─────────────────────────────────────────┘
                             │
┌────────────────────────────▼─────────────────────────────────────────┐
│                           Core Layer                                  │
│         Piece Scheduler  ·  Reputation Engine  ·  Swarm Intel        │
└───────────┬─────────────────────────────┬────────────────────────────┘
            │                             │
┌───────────▼──────────┐   ┌──────────────▼──────────────────────────┐
│    Network Layer     │   │             Storage Layer               │
│  Peer Manager        │   │  Chunker · Merkle Tree · Piece Store    │
│  Discovery           │   │  Content-Addressable Block Storage      │
│   └ Tracker          │   └─────────────────────────────────────────┘
│   └ DHT (Kademlia)   │
│   └ PEX              │
│  NAT Traversal       │
│   └ STUN             │
│   └ Hole Punching    │
│   └ Relay Fallback   │
└───────────┬──────────┘
            │
┌───────────▼──────────────────────────────────────────────────────────┐
│                        Transport Layer                                │
│       TCP Baseline  ·  Length-Prefix Framing  ·  State Machine       │
│       UDP + Reliability (Phase 4)  ·  TLS (Phase 4)                  │
└──────────────────────────────────────────────────────────────────────┘
```

### Wire Protocol

```
 ┌──────────────┬───────────────┬──────────────────────────────────────┐
 │   4 bytes    │    1 byte     │   N bytes                            │
 │   Length     │  Message Type │   Payload                            │
 └──────────────┴───────────────┴──────────────────────────────────────┘

 Message Types
 ─────────────────────────────────────────────────────────────────────
  0x00  HANDSHAKE    ── version, info-hash, peer ID, capabilities
  0x01  BITFIELD     ── bitmask of available pieces
  0x02  REQUEST      ── index, offset, length of desired block
  0x03  PIECE        ── index, offset, block payload
  0x04  HAVE         ── announce newly completed piece
  0x05  CANCEL       ── cancel in-flight request (endgame mode)
  0x06  CHOKE        ── stop serving requests
  0x07  UNCHOKE      ── resume serving requests
  0x08  INTERESTED   ── signal download intent
  0x09  NOT_INTERESTED
```

### Connection State Machine

```
  ╔═══════════════╗
  ║ DISCONNECTED  ║◀══════════════════════════════════╗
  ╚═══════╤═══════╝                                   ║ error / timeout / close()
          │ connect()                                 ║
  ╔═══════▼═══════╗                                   ║
  ║  CONNECTING   ║                                   ║
  ╚═══════╤═══════╝                                   ║
          │ TCP established                           ║
  ╔═══════▼═══════╗                                   ║
  ║  HANDSHAKING  ║                                   ║
  ╚═══════╤═══════╝                                   ║
          │ HANDSHAKE + BITFIELD exchanged            ║
  ╔═══════▼═══════╗    CHOKE msg     ╔══════════════╗ ║
  ║    CHOKED     ║◀─────────────────║  DOWNLOADING ║ ║
  ╚═══════╤═══════╝                  ╚══════╤═══════╝ ║
          │ UNCHOKE msg                     │ 100%    ║
          └─────────────────────────► ╔═════▼═══════╗ ║
                                      ║   SEEDING   ║═╝
                                      ╚═════════════╝
  Invalid message in any state → log warn, close connection
```

---

## 🚀 Quickstart

### Prerequisites

```bash
rustc --version   # requires >= 1.75.0
cargo --version   # ships with rustc
```

### Install & Build

```bash
git clone https://github.com/mahmoudamr512/aegistorrent.git
cd aegistorrent
cargo build --release
```

### Use

```bash
# Download from a torrent file
aegis download ./ubuntu-24.04.torrent

# Seed a file (creates .torrent + starts serving)
aegis seed ./myfile.mp4

# Launch real-time dashboard
aegis dashboard

# Show swarm stats for a running download
aegis stats
```

### CLI Dashboard Preview

```
╔══════════════════════════════════════════════════════════════════╗
║  AegisTorrent v0.1.0                                             ║
║  ubuntu-24.04-desktop-amd64.iso  ·  2.1 GB                      ║
╠══════════════════════════════════════════════════════════════════╣
║  Peers      34 connected  (12 uploading · 19 idle · 3 choked)   ║
║  Download   ↓ 5.2 MB/s        Upload  ↑ 1.1 MB/s               ║
║  Progress   ████████████████░░░░░░░░░░░░░░  52%  (1.09 GB)     ║
║  ETA        4m 38s                                               ║
╠══════════════════════════════════════════════════════════════════╣
║  Top Peers                                           Reputation  ║
║    ◉  142.250.10.5       ↓ 2.1 MB/s    good         ●●●●●      ║
║    ◉  31.13.84.36        ↓ 1.8 MB/s    good         ●●●●○      ║
║    ◉  104.18.22.197      ↓ 0.9 MB/s    good         ●●●●○      ║
║    ◎  192.168.1.50       ↓ 0.1 MB/s    slow         ●●○○○      ║
╠══════════════════════════════════════════════════════════════════╣
║  ⚠  Pieces 412, 891 failed Merkle verification                  ║
║  ⚠  Peer 10.0.0.8 blacklisted (corrupt data — 3 violations)    ║
╚══════════════════════════════════════════════════════════════════╝
```

---

## ✨ Features

<table>
<tr>
<td width="50%" valign="top">

### 🔒 Security & Integrity
- **Merkle tree** verification on every piece
- **SHA-256** content-addressable storage
- Per-peer **reputation scoring** (local, non-Sybilable)
- Automatic **blacklisting** on corruption threshold
- **Public-key** peer identity *(Phase 4)*
- **TLS** encrypted peer sessions *(Phase 4)*

</td>
<td width="50%" valign="top">

### 🌐 Peer Discovery
- **Tracker** bootstrap (centralized, fast)
- **Kademlia DHT** (decentralized, resilient)
- **Peer Exchange (PEX)** (organic swarm growth)
- **NAT traversal**: STUN + UDP hole punching
- Relay **fallback** for symmetric NATs
- IPv4 + IPv6 **dual-stack**

</td>
</tr>
<tr>
<td valign="top">

### ⚡ Scheduling & Performance
- **Rarest-first** piece selection (swarm-optimal)
- **Endgame mode** (eliminates last-mile stalls)
- **Tit-for-tat** choking (self-enforcing fairness)
- **epoll/mio**-based I/O (100+ peers, one thread)
- Async **disk write pool** (tokio::fs, off main loop)
- Sequential mode for **streaming playback**

</td>
<td valign="top">

### 📊 Observability
- **Prometheus**-compatible `/metrics` endpoint
- **Structured JSON** logging (leveled)
- Real-time **terminal dashboard**
- Piece **availability heatmap**
- Per-peer **throughput + latency graphs**
- Distributed **trace correlation IDs** *(Phase 5)*

</td>
</tr>
</table>

---

## 🗺 Roadmap

> Each phase is a **working, testable system** — not scaffolding. Phases build on each other but can be tested in isolation.

```
Phase 1 — Foundation                                    [ In Progress ]
────────────────────────────────────────────────────────────────────────
  ✅  TCP socket layer with length-prefix message framing
  ✅  Deterministic file chunking (piece size: power of 2)
  ✅  Merkle tree construction + single-piece proof verification
  🔄  Handshake protocol (version + info-hash + peer ID)
  🔄  BITFIELD exchange between two peers
  🔄  Basic REQUEST / PIECE / HAVE loop
  ⬜  Two-peer local download (seeder → leecher, single file)
  ⬜  Unit tests: chunker, Merkle prover, codec, state machine

Phase 2 — Real System                                   [ Planned ]
────────────────────────────────────────────────────────────────────────
  ⬜  Rarest-first piece scheduler (bitfield aggregation)
  ⬜  Multi-peer concurrent download (tokio/mio connection pool)
  ⬜  Endgame mode trigger + CANCEL message handling
  ⬜  Choking / unchoking (tit-for-tat, optimistic unchoke)
  ⬜  Async disk write queue (separate thread pool)
  ⬜  CLI dashboard v1 (live throughput, peer list, progress bar)

Phase 3 — Distributed Intelligence                      [ Planned ]
────────────────────────────────────────────────────────────────────────
  ⬜  Kademlia DHT implementation (k-buckets, XOR routing)
  ⬜  DHT bootstrap + iterative find_node
  ⬜  Peer Exchange (PEX) messages
  ⬜  Swarm intelligence engine (piece rarity tracking)
  ⬜  Peer reputation scoring + blacklist
  ⬜  Prometheus metrics endpoint (/metrics)

Phase 4 — Advanced Networking                           [ Planned ]
────────────────────────────────────────────────────────────────────────
  ⬜  STUN-like external IP/port discovery
  ⬜  UDP hole punching (cone NATs)
  ⬜  Relay fallback (symmetric NAT)
  ⬜  UDP reliability layer (sequencing, ACKs, retransmit)
  ⬜  TLS session establishment
  ⬜  Public-key peer identity (keypair on startup, DH exchange)

Phase 5 — Production                                    [ Planned ]
────────────────────────────────────────────────────────────────────────
  ⬜  Log levels (ERROR / WARN / INFO / DEBUG)
  ⬜  Distributed trace IDs across peer message flows
  ⬜  Grafana dashboard (pre-provisioned)
  ⬜  Chaos test suite (packet loss, slow peers, corrupt data)
  ⬜  Benchmarking suite (throughput, latency, memory)
  ⬜  Web UI (swarm graph, piece heatmap, reputation table)
```

---

## 📁 Project Structure

```
aegistorrent/
│
├── src/
│   ├── core/
│   │   ├── chunker.rs          # File → pieces → blocks (deterministic)
│   │   ├── merkle.rs           # Merkle tree build + proof generation
│   │   ├── scheduler.rs        # Rarest-first + endgame mode
│   │   └── swarm.rs            # Piece rarity tracking across peers
│   │
│   ├── network/
│   │   ├── peer.rs             # Single peer connection + lifecycle
│   │   ├── pool.rs             # epoll/mio-based connection pool (100+ peers)
│   │   └── discovery/
│   │       ├── tracker.rs      # HTTP tracker client
│   │       ├── dht.rs          # Kademlia DHT (Phase 3)
│   │       └── pex.rs          # Peer Exchange (Phase 3)
│   │
│   ├── nat/
│   │   ├── stun.rs             # External address discovery (Phase 4)
│   │   └── punch.rs            # UDP hole punching + relay (Phase 4)
│   │
│   ├── protocol/
│   │   ├── messages.rs         # Message type definitions + encoding
│   │   ├── codec.rs            # Length-prefix framing: encode/decode
│   │   └── state.rs            # FSM: DISCONNECTED → SEEDING
│   │
│   ├── storage/
│   │   ├── store.rs            # Content-addressable block store
│   │   └── writer.rs           # Async disk write pool (tokio::fs)
│   │
│   ├── security/
│   │   ├── reputation.rs       # Per-peer scoring, blacklist, decay
│   │   └── crypto.rs           # SHA-256, keypair gen, DH exchange
│   │
│   ├── observability/
│   │   ├── metrics.rs          # Prometheus counters / gauges / histograms
│   │   └── logger.rs           # Structured JSON logger (tracing)
│   │
│   ├── cli/
│   │   ├── main.rs             # CLI entry point (clap)
│   │   └── dashboard.rs        # Real-time terminal dashboard (ratatui)
│   │
│   └── lib.rs                  # Crate root, module declarations
│
├── tests/
│   ├── unit/                   # Pure function tests (chunker, Merkle, codec)
│   ├── integration/            # Local two-peer download tests
│   └── chaos/                  # Fault injection (packet loss, corrupt peers)
│
├── docs/
│   ├── protocol.md             # Wire format byte-level specification
│   ├── architecture.md         # Design rationale per module
│   └── tradeoffs.md            # Every major decision + alternatives considered
│
├── assets/                     # README banners, icons
├── .github/
│   ├── workflows/
│   │   ├── ci.yml              # Lint + build on every push
│   │   └── test.yml            # Unit + integration on PR
│   └── ISSUE_TEMPLATE/
│       ├── bug_report.md
│       └── feature_request.md
│
├── Cargo.toml                  # Dependencies + workspace config
├── CONTRIBUTING.md
├── LICENSE
└── README.md
```

---

## 🧪 Testing

```bash
# Run everything
cargo test

# Unit tests (fast, no network)
cargo test --lib

# Integration tests (spins up two local peers)
cargo test --test integration

# Chaos suite (requires Docker)
cargo test --test chaos

# Coverage report (requires cargo-llvm-cov)
cargo llvm-cov --html
```

### Chaos Test Matrix

| Scenario | What it simulates | Expected behavior |
|---|---|---|
| `packet-loss-30` | 30% random packet drop | Retransmit without crash |
| `slow-peer` | Peer throttled to 1 KB/s | Choked, fast peers preferred |
| `corrupt-data` | Peer sends bad piece data | Merkle fail → blacklist peer |
| `tracker-down` | Tracker unreachable | Fall back to DHT |
| `peer-churn` | Peers drop mid-download | Resume from remaining peers |
| `eclipse-attempt` | All connected peers adversarial | No data corruption, progress stalls gracefully |
| `disk-slow` | Disk writes 200ms latency | Backpressure applied, no OOM |

---

## 📖 Reference Material

| Topic | Resource |
|---|---|
| Kademlia DHT | [Kademlia: A Peer-to-peer Information System — Maymounkov & Mazières, 2002](https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf) |
| BitTorrent protocol | [BEP-0003: The BitTorrent Protocol Specification](https://www.bittorrent.org/beps/bep_0003.html) |
| NAT traversal | [Peer-to-Peer Communication Across NATs — Ford et al., 2005](https://pdos.csail.mit.edu/papers/p2pnat.pdf) |
| Merkle trees | [A Digital Signature Based on a Conventional Encryption Function — Merkle, 1987](https://link.springer.com/chapter/10.1007/3-540-48184-2_32) |
| QUIC transport | [RFC 9000: QUIC: A UDP-Based Multiplexed and Secure Transport](https://www.rfc-editor.org/rfc/rfc9000) |
| Congestion control | [RFC 5681: TCP Congestion Control](https://www.rfc-editor.org/rfc/rfc5681) |

---

## 🤝 Contributing

AegisTorrent is a learning-first systems project. If you're studying distributed systems, networking, or protocol design — this is a great place to implement a real module and understand it deeply.

```bash
# Fork, clone, branch
git clone https://github.com/YOUR_USERNAME/aegistorrent.git
git checkout -b feat/kademlia-routing-table

# Make changes, add tests
cargo test

# Commit with conventional commits
git commit -m "feat(dht): implement Kademlia k-bucket routing table"

# Push and open PR
git push origin feat/kademlia-routing-table
```

**Labels to look for:**

| Label | Meaning |
|---|---|
| `good-first-issue` | Small, well-scoped, great entry point |
| `help-wanted` | Medium-effort, needs ownership |
| `research-needed` | Protocol question before coding can begin |
| `phase-N` | Belongs to a specific roadmap phase |

Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting a PR.

---

## 📄 License

MIT © 2025 — See [LICENSE](LICENSE) for details.

---

<div align="center">

*Every design decision is documented. Every tradeoff is explicit.*  
*This is how systems engineering should be learned.*

<br/>

[![Star on GitHub](https://img.shields.io/github/stars/mahmoudamr512/aegistorrent?style=for-the-badge&logo=github&color=f59e0b&label=Star+this+repo)](https://github.com/mahmoudamr512/aegistorrent)

</div>
