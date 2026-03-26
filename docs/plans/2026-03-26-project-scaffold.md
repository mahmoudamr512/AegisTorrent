# Project Scaffold Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Scaffold the entire Rust project structure so Phase 1 development can begin immediately on a clean foundation.

**Architecture:** A Cargo workspace with a single crate. Module tree mirrors the README's project structure. All modules exist as stubs with public type signatures. CI runs `cargo fmt`, `cargo clippy`, `cargo test`, and `cargo build`. Branch protection enforced by workflow.

**Tech Stack:** Rust 1.75+, Tokio 1.x, SHA2, clap, ratatui, tracing, prometheus-client

---

## Branch Strategy

All work on branch: `feat/project-scaffold`
Created from: `main`
Merge via: PR to `main`

---

### Task 1: Create the feature branch

**Step 1: Create and checkout the branch**

```bash
git checkout -b feat/project-scaffold
```

**Step 2: Verify**

```bash
git branch --show-current
```
Expected: `feat/project-scaffold`

---

### Task 2: Cargo.toml and crate root

**Files:**
- Create: `Cargo.toml`
- Create: `src/lib.rs`
- Create: `src/cli/main.rs`

**Step 1: Create `Cargo.toml`**

```toml
[package]
name = "aegistorrent"
version = "0.1.0"
edition = "2021"
rust-version = "1.75"
description = "High-performance P2P file distribution engine"
license = "MIT"
repository = "https://github.com/mahmoudamr512/aegistorrent"

[[bin]]
name = "aegis"
path = "src/cli/main.rs"

[dependencies]
tokio = { version = "1", features = ["full"] }
sha2 = "0.10"
clap = { version = "4", features = ["derive"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }
thiserror = "2"
bytes = "1"

[dev-dependencies]
tokio-test = "0.4"
tempfile = "3"
```

**Step 2: Create `src/lib.rs`**

Module declarations only — one line per module, no comments.

```rust
pub mod core;
pub mod network;
pub mod protocol;
pub mod storage;
pub mod security;
pub mod observability;
```

**Step 3: Create `src/cli/main.rs`**

Minimal CLI entry point that compiles:

```rust
use clap::Parser;

#[derive(Parser)]
#[command(name = "aegis", version, about = "AegisTorrent P2P file distribution")]
enum Cli {
    /// Download from a torrent file
    Download {
        /// Path to .torrent file
        path: String,
    },
    /// Seed a file
    Seed {
        /// Path to file to seed
        path: String,
    },
}

fn main() {
    let _cli = Cli::parse();
}
```

**Step 4: Verify it compiles**

```bash
cargo check
```
Expected: compiles (will fail until module stubs exist — that's Task 3)

---

### Task 3: Module stubs — core

**Files:**
- Create: `src/core/mod.rs`
- Create: `src/core/chunker.rs`
- Create: `src/core/merkle.rs`
- Create: `src/core/scheduler.rs`
- Create: `src/core/swarm.rs`

**Step 1: Create `src/core/mod.rs`**

```rust
pub mod chunker;
pub mod merkle;
pub mod scheduler;
pub mod swarm;
```

**Step 2: Create `src/core/chunker.rs`**

```rust
use sha2::{Sha256, Digest};

const DEFAULT_PIECE_SIZE: usize = 256 * 1024; // 256 KiB

pub struct Chunker {
    piece_size: usize,
}

#[derive(Debug, Clone)]
pub struct Piece {
    pub index: u32,
    pub hash: [u8; 32],
    pub length: usize,
}

impl Chunker {
    pub fn new(piece_size: usize) -> Self {
        assert!(piece_size.is_power_of_two(), "piece size must be a power of 2");
        Self { piece_size }
    }

    pub fn with_default() -> Self {
        Self::new(DEFAULT_PIECE_SIZE)
    }

    pub fn chunk(&self, data: &[u8]) -> Vec<Piece> {
        data.chunks(self.piece_size)
            .enumerate()
            .map(|(i, chunk)| {
                let mut hasher = Sha256::new();
                hasher.update(chunk);
                let hash: [u8; 32] = hasher.finalize().into();
                Piece {
                    index: i as u32,
                    hash,
                    length: chunk.len(),
                }
            })
            .collect()
    }

    pub fn piece_size(&self) -> usize {
        self.piece_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_data_into_pieces() {
        let chunker = Chunker::new(4);
        let data = b"helloworldtest";
        let pieces = chunker.chunk(data);

        assert_eq!(pieces.len(), 4);
        assert_eq!(pieces[0].index, 0);
        assert_eq!(pieces[0].length, 4);
        assert_eq!(pieces[3].length, 2); // remainder
    }

    #[test]
    fn consistent_hashing() {
        let chunker = Chunker::new(8);
        let data = b"repeatable";
        let a = chunker.chunk(data);
        let b = chunker.chunk(data);
        assert_eq!(a[0].hash, b[0].hash);
    }

    #[test]
    #[should_panic(expected = "power of 2")]
    fn rejects_non_power_of_two() {
        Chunker::new(100);
    }

    #[test]
    fn empty_data_yields_no_pieces() {
        let chunker = Chunker::new(4);
        let pieces = chunker.chunk(b"");
        assert!(pieces.is_empty());
    }
}
```

**Step 3: Create `src/core/merkle.rs`**

```rust
use sha2::{Sha256, Digest};

#[derive(Debug, Clone)]
pub struct MerkleTree {
    nodes: Vec<[u8; 32]>,
    leaf_count: usize,
}

#[derive(Debug, Clone)]
pub struct MerkleProof {
    pub leaf_index: usize,
    pub siblings: Vec<(bool, [u8; 32])>,
}

impl MerkleTree {
    pub fn from_leaves(leaves: &[[u8; 32]]) -> Self {
        assert!(!leaves.is_empty(), "cannot build tree from empty leaves");

        let leaf_count = leaves.len().next_power_of_two();
        let mut nodes = vec![[0u8; 32]; 2 * leaf_count];

        for (i, leaf) in leaves.iter().enumerate() {
            nodes[leaf_count + i] = *leaf;
        }
        // duplicate last leaf to fill to power-of-two
        for i in leaves.len()..leaf_count {
            nodes[leaf_count + i] = nodes[leaf_count + leaves.len() - 1];
        }

        for i in (1..leaf_count).rev() {
            nodes[i] = hash_pair(&nodes[2 * i], &nodes[2 * i + 1]);
        }

        Self {
            nodes,
            leaf_count,
        }
    }

    pub fn root(&self) -> [u8; 32] {
        self.nodes[1]
    }

    pub fn proof(&self, leaf_index: usize) -> MerkleProof {
        let mut siblings = Vec::new();
        let mut idx = self.leaf_count + leaf_index;

        while idx > 1 {
            let sibling = if idx % 2 == 0 { idx + 1 } else { idx - 1 };
            let is_left = idx % 2 != 0;
            siblings.push((is_left, self.nodes[sibling]));
            idx /= 2;
        }

        MerkleProof {
            leaf_index,
            siblings,
        }
    }

    pub fn verify(root: &[u8; 32], leaf: &[u8; 32], proof: &MerkleProof) -> bool {
        let mut current = *leaf;

        for (is_left, sibling) in &proof.siblings {
            current = if *is_left {
                hash_pair(sibling, &current)
            } else {
                hash_pair(&current, sibling)
            };
        }

        current == *root
    }
}

fn hash_pair(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;

    fn hash(data: &[u8]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(data);
        h.finalize().into()
    }

    #[test]
    fn single_leaf_tree() {
        let leaves = vec![hash(b"block0")];
        let tree = MerkleTree::from_leaves(&leaves);
        let proof = tree.proof(0);
        assert!(MerkleTree::verify(&tree.root(), &leaves[0], &proof));
    }

    #[test]
    fn four_leaf_tree() {
        let leaves: Vec<[u8; 32]> = (0..4).map(|i| hash(format!("block{i}").as_bytes())).collect();
        let tree = MerkleTree::from_leaves(&leaves);

        for i in 0..4 {
            let proof = tree.proof(i);
            assert!(MerkleTree::verify(&tree.root(), &leaves[i], &proof));
        }
    }

    #[test]
    fn tampered_leaf_fails_verification() {
        let leaves: Vec<[u8; 32]> = (0..4).map(|i| hash(format!("block{i}").as_bytes())).collect();
        let tree = MerkleTree::from_leaves(&leaves);
        let proof = tree.proof(0);
        let fake = hash(b"tampered");
        assert!(!MerkleTree::verify(&tree.root(), &fake, &proof));
    }

    #[test]
    fn non_power_of_two_leaves() {
        let leaves: Vec<[u8; 32]> = (0..5).map(|i| hash(format!("block{i}").as_bytes())).collect();
        let tree = MerkleTree::from_leaves(&leaves);
        let proof = tree.proof(2);
        assert!(MerkleTree::verify(&tree.root(), &leaves[2], &proof));
    }
}
```

**Step 4: Create `src/core/scheduler.rs`**

```rust
pub struct Scheduler;
```

**Step 5: Create `src/core/swarm.rs`**

```rust
pub struct SwarmIntel;
```

**Step 6: Verify**

```bash
cargo check
```

---

### Task 4: Module stubs — protocol

**Files:**
- Create: `src/protocol/mod.rs`
- Create: `src/protocol/messages.rs`
- Create: `src/protocol/codec.rs`
- Create: `src/protocol/state.rs`

**Step 1: Create `src/protocol/mod.rs`**

```rust
pub mod messages;
pub mod codec;
pub mod state;
```

**Step 2: Create `src/protocol/messages.rs`**

```rust
use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    Handshake {
        version: u8,
        info_hash: [u8; 32],
        peer_id: [u8; 20],
    },
    Bitfield(Bytes),
    Request {
        index: u32,
        offset: u32,
        length: u32,
    },
    Piece {
        index: u32,
        offset: u32,
        data: Bytes,
    },
    Have(u32),
    Cancel {
        index: u32,
        offset: u32,
        length: u32,
    },
    Choke,
    Unchoke,
    Interested,
    NotInterested,
}

#[derive(Debug, Error)]
pub enum MessageError {
    #[error("unknown message type: {0:#x}")]
    UnknownType(u8),
    #[error("buffer too short: need {need}, have {have}")]
    BufferTooShort { need: usize, have: usize },
    #[error("payload too large: {0} bytes")]
    PayloadTooLarge(usize),
}

impl Message {
    pub fn message_type(&self) -> u8 {
        match self {
            Self::Handshake { .. } => 0x00,
            Self::Bitfield(_) => 0x01,
            Self::Request { .. } => 0x02,
            Self::Piece { .. } => 0x03,
            Self::Have(_) => 0x04,
            Self::Cancel { .. } => 0x05,
            Self::Choke => 0x06,
            Self::Unchoke => 0x07,
            Self::Interested => 0x08,
            Self::NotInterested => 0x09,
        }
    }
}
```

**Step 3: Create `src/protocol/codec.rs`**

```rust
pub struct LengthPrefixCodec;
```

**Step 4: Create `src/protocol/state.rs`**

```rust
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Handshaking,
    Choked,
    Downloading,
    Seeding,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self::Disconnected
    }
}
```

**Step 5: Verify**

```bash
cargo check
```

---

### Task 5: Module stubs — network

**Files:**
- Create: `src/network/mod.rs`
- Create: `src/network/peer.rs`
- Create: `src/network/pool.rs`
- Create: `src/network/discovery/mod.rs`
- Create: `src/network/discovery/tracker.rs`
- Create: `src/network/discovery/dht.rs`
- Create: `src/network/discovery/pex.rs`

**Step 1: Create `src/network/mod.rs`**

```rust
pub mod peer;
pub mod pool;
pub mod discovery;
```

**Step 2: Create stubs**

`src/network/peer.rs`:
```rust
pub struct PeerConnection;
```

`src/network/pool.rs`:
```rust
pub struct ConnectionPool;
```

`src/network/discovery/mod.rs`:
```rust
pub mod tracker;
pub mod dht;
pub mod pex;
```

`src/network/discovery/tracker.rs`:
```rust
pub struct TrackerClient;
```

`src/network/discovery/dht.rs`:
```rust
pub struct Dht;
```

`src/network/discovery/pex.rs`:
```rust
pub struct PeerExchange;
```

**Step 3: Verify**

```bash
cargo check
```

---

### Task 6: Module stubs — storage, security, observability, nat

**Files:**
- Create: `src/storage/mod.rs`
- Create: `src/storage/store.rs`
- Create: `src/storage/writer.rs`
- Create: `src/security/mod.rs`
- Create: `src/security/reputation.rs`
- Create: `src/security/crypto.rs`
- Create: `src/observability/mod.rs`
- Create: `src/observability/metrics.rs`
- Create: `src/observability/logger.rs`
- Create: `src/nat/mod.rs`
- Create: `src/nat/stun.rs`
- Create: `src/nat/punch.rs`

**Step 1: Create all stubs**

`src/storage/mod.rs`:
```rust
pub mod store;
pub mod writer;
```

`src/storage/store.rs`:
```rust
pub struct PieceStore;
```

`src/storage/writer.rs`:
```rust
pub struct DiskWriter;
```

`src/security/mod.rs`:
```rust
pub mod reputation;
pub mod crypto;
```

`src/security/reputation.rs`:
```rust
pub struct ReputationEngine;
```

`src/security/crypto.rs`:
```rust
pub struct CryptoProvider;
```

`src/observability/mod.rs`:
```rust
pub mod metrics;
pub mod logger;
```

`src/observability/metrics.rs`:
```rust
pub struct MetricsRegistry;
```

`src/observability/logger.rs`:
```rust
pub struct Logger;
```

`src/nat/mod.rs`:
```rust
pub mod stun;
pub mod punch;
```

`src/nat/stun.rs`:
```rust
pub struct StunClient;
```

`src/nat/punch.rs`:
```rust
pub struct HolePuncher;
```

**Step 2: Add `nat` module to `src/lib.rs`**

Update `src/lib.rs` to:
```rust
pub mod core;
pub mod nat;
pub mod network;
pub mod observability;
pub mod protocol;
pub mod security;
pub mod storage;
```

**Step 3: Verify**

```bash
cargo check
```

**Step 4: Run tests**

```bash
cargo test
```
Expected: all chunker and merkle tests pass.

**Step 5: Commit**

```bash
git add -A
git commit -m "feat: scaffold project structure with core module implementations

Cargo.toml with deps, CLI entry point, full module tree.
Chunker and Merkle tree have working implementations with tests.
All other modules are stubs ready for Phase 1 development."
```

---

### Task 7: CI workflow — replace Node.js with Rust

**Files:**
- Modify: `.github/workflows/ci.yml`

**Step 1: Replace CI with Rust workflow**

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

env:
  CARGO_TERM_COLOR: always

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy

      - uses: Swatinem/rust-cache@v2

      - name: Format
        run: cargo fmt --all -- --check

      - name: Clippy
        run: cargo clippy --all-targets --all-features -- -D warnings

      - name: Build
        run: cargo build --all-targets

      - name: Test
        run: cargo test --all-targets
```

**Step 2: Verify YAML is valid**

Eyeball check — no Node references remain.

**Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "chore(ci): replace Node.js workflow with Rust (fmt, clippy, build, test)"
```

---

### Task 8: Add .gitignore for Rust

**Files:**
- Create: `.gitignore`

**Step 1: Create `.gitignore`**

```gitignore
/target
**/*.rs.bk
*.pdb
```

**Step 2: Commit**

```bash
git add .gitignore
git commit -m "chore: add Rust .gitignore"
```

---

### Task 9: Test directory structure

**Files:**
- Create: `tests/unit/.gitkeep`
- Create: `tests/integration/.gitkeep`
- Create: `tests/chaos/.gitkeep`

**Step 1: Create test directories**

```bash
mkdir -p tests/unit tests/integration tests/chaos
touch tests/unit/.gitkeep tests/integration/.gitkeep tests/chaos/.gitkeep
```

**Step 2: Commit**

```bash
git add tests/
git commit -m "chore: add test directory structure"
```

---

### Task 10: Final verification and format

**Step 1: Format**

```bash
cargo fmt --all
```

**Step 2: Clippy**

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

**Step 3: Full test suite**

```bash
cargo test
```

**Step 4: Commit any formatting fixes**

```bash
git add -A
git commit -m "style: apply cargo fmt"
```
(Only if there were changes)

**Step 5: Push and create PR**

```bash
git push -u origin feat/project-scaffold
```

Then create PR:
- Title: `feat: scaffold Rust project structure`
- Body: Summary of what was scaffolded, link to Phase 1 roadmap items

---

## Summary

| Task | What | Key deliverable |
|------|------|-----------------|
| 1 | Feature branch | `feat/project-scaffold` |
| 2 | Cargo.toml + crate root + CLI | Compiling binary |
| 3 | Core modules | Chunker + Merkle with tests |
| 4 | Protocol modules | Messages enum + state machine |
| 5 | Network modules | Peer, pool, discovery stubs |
| 6 | Remaining modules | Storage, security, observability, NAT stubs |
| 7 | CI workflow | Rust fmt + clippy + build + test |
| 8 | .gitignore | Rust ignores |
| 9 | Test directories | unit / integration / chaos |
| 10 | Final checks + PR | Clean, passing, ready for review |
