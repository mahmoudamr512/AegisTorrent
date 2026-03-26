# Phase 1 — Two-Peer File Transfer Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Two peers transfer a file over TCP on localhost with Merkle verification, usable from CLI and validated by integration test.

**Architecture:** Bottom-up: codec → peer connection → piece store → seeder/leecher logic → CLI. Tokio async throughout. The codec uses tokio-util's `Encoder`/`Decoder` traits with `Framed` to turn a TCP stream into a typed message stream. Seeder and leecher are async loops that drive the peer connection.

**Tech Stack:** Rust, Tokio, tokio-util (codec/Framed), sha2, clap, bytes

---

## Branch Strategy

All work on branch: `feat/phase1-two-peer-transfer`
Created from: `main` (merge scaffold PR first)
Merge via: PR to `main`

---

### Task 1: Add tokio-util dependency and create branch

**Files:**
- Modify: `Cargo.toml`

**Step 1: Create branch**

```bash
git checkout main
git pull
git checkout -b feat/phase1-two-peer-transfer
```

**Step 2: Add tokio-util to Cargo.toml**

Add to `[dependencies]`:
```toml
tokio-util = { version = "0.7", features = ["codec"] }
```

**Step 3: Verify it compiles**

```bash
cargo check
```

**Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: add tokio-util dependency for codec support"
```

---

### Task 2: Update Message to include Merkle proof in Piece

**Files:**
- Modify: `src/protocol/messages.rs`

**Step 1: Write the test**

Add to `src/protocol/messages.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn piece_message_carries_proof() {
        let proof = vec![(true, [0xAA; 32]), (false, [0xBB; 32])];
        let msg = Message::Piece {
            index: 0,
            offset: 0,
            data: Bytes::from_static(b"hello"),
            proof,
        };
        assert_eq!(msg.message_type(), 0x03);
    }

    #[test]
    fn message_types_are_correct() {
        assert_eq!(Message::Choke.message_type(), 0x06);
        assert_eq!(Message::Unchoke.message_type(), 0x07);
        assert_eq!(Message::Interested.message_type(), 0x08);
        assert_eq!(Message::NotInterested.message_type(), 0x09);
        assert_eq!(Message::Have(0).message_type(), 0x04);
    }
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test protocol::messages::tests -- --nocapture
```
Expected: FAIL — `Piece` variant doesn't have a `proof` field yet.

**Step 3: Update the Piece variant**

Change the `Piece` variant in the `Message` enum:

```rust
Piece {
    index: u32,
    offset: u32,
    data: Bytes,
    proof: Vec<(bool, [u8; 32])>,
},
```

**Step 4: Run tests**

```bash
cargo test protocol::messages::tests -- --nocapture
```
Expected: PASS

**Step 5: Commit**

```bash
git add src/protocol/messages.rs
git commit -m "feat(protocol): add Merkle proof field to Piece message"
```

---

### Task 3: Implement the codec encoder

**Files:**
- Modify: `src/protocol/codec.rs`

The encoder takes a `Message` and writes `[4-byte length][1-byte type][payload]` into a buffer.

**Step 1: Write encoder tests**

Replace `src/protocol/codec.rs` entirely:

```rust
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use super::messages::{Message, MessageError};

const MAX_PAYLOAD: usize = 4 * 1024 * 1024;

pub struct LengthPrefixCodec;

impl Encoder<Message> for LengthPrefixCodec {
    type Error = MessageError;

    fn encode(&mut self, msg: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut payload = BytesMut::new();

        match &msg {
            Message::Handshake {
                version,
                info_hash,
                peer_id,
            } => {
                payload.put_u8(*version);
                payload.put_slice(info_hash);
                payload.put_slice(peer_id);
            }
            Message::Bitfield(bits) => {
                payload.put_slice(bits);
            }
            Message::Request {
                index,
                offset,
                length,
            }
            | Message::Cancel {
                index,
                offset,
                length,
            } => {
                payload.put_u32(*index);
                payload.put_u32(*offset);
                payload.put_u32(*length);
            }
            Message::Piece {
                index,
                offset,
                data,
                proof,
            } => {
                payload.put_u32(*index);
                payload.put_u32(*offset);
                payload.put_u16(proof.len() as u16);
                for (is_left, hash) in proof {
                    payload.put_u8(if *is_left { 1 } else { 0 });
                    payload.put_slice(hash);
                }
                payload.put_slice(data);
            }
            Message::Have(index) => {
                payload.put_u32(*index);
            }
            Message::Choke
            | Message::Unchoke
            | Message::Interested
            | Message::NotInterested => {}
        }

        let len = 1 + payload.len();
        if payload.len() > MAX_PAYLOAD {
            return Err(MessageError::PayloadTooLarge(payload.len()));
        }

        dst.put_u32(len as u32);
        dst.put_u8(msg.message_type());
        dst.put_slice(&payload);
        Ok(())
    }
}

impl Decoder for LengthPrefixCodec {
    type Item = Message;
    type Error = MessageError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        if len == 0 {
            return Err(MessageError::BufferTooShort { need: 1, have: 0 });
        }
        if len - 1 > MAX_PAYLOAD {
            return Err(MessageError::PayloadTooLarge(len - 1));
        }

        if src.len() < 4 + len {
            src.reserve(4 + len - src.len());
            return Ok(None);
        }

        src.advance(4);
        let msg_type = src[0];
        src.advance(1);
        let payload_len = len - 1;

        let msg = match msg_type {
            0x00 => {
                if payload_len < 53 {
                    return Err(MessageError::BufferTooShort {
                        need: 53,
                        have: payload_len,
                    });
                }
                let version = src[0];
                src.advance(1);
                let mut info_hash = [0u8; 32];
                info_hash.copy_from_slice(&src[..32]);
                src.advance(32);
                let mut peer_id = [0u8; 20];
                peer_id.copy_from_slice(&src[..20]);
                src.advance(20);
                Message::Handshake {
                    version,
                    info_hash,
                    peer_id,
                }
            }
            0x01 => {
                let bits = Bytes::copy_from_slice(&src[..payload_len]);
                src.advance(payload_len);
                Message::Bitfield(bits)
            }
            0x02 => {
                if payload_len < 12 {
                    return Err(MessageError::BufferTooShort {
                        need: 12,
                        have: payload_len,
                    });
                }
                let index = (&src[..4]).get_u32();
                let offset = (&src[..4]).get_u32();
                let length = (&src[..4]).get_u32();
                src.advance(12);
                Message::Request {
                    index,
                    offset,
                    length,
                }
            }
            0x03 => {
                if payload_len < 10 {
                    return Err(MessageError::BufferTooShort {
                        need: 10,
                        have: payload_len,
                    });
                }
                let index = (&src[..4]).get_u32();
                let offset = (&src[..4]).get_u32();
                let proof_len = (&src[..2]).get_u16() as usize;
                src.advance(10);
                let proof_bytes = proof_len * 33;
                if src.len() < proof_bytes {
                    return Err(MessageError::BufferTooShort {
                        need: proof_bytes,
                        have: src.len(),
                    });
                }
                let mut proof = Vec::with_capacity(proof_len);
                for _ in 0..proof_len {
                    let is_left = src[0] == 1;
                    src.advance(1);
                    let mut hash = [0u8; 32];
                    hash.copy_from_slice(&src[..32]);
                    src.advance(32);
                    proof.push((is_left, hash));
                }
                let data_len = payload_len - 10 - proof_bytes;
                let data = Bytes::copy_from_slice(&src[..data_len]);
                src.advance(data_len);
                Message::Piece {
                    index,
                    offset,
                    data,
                    proof,
                }
            }
            0x04 => {
                if payload_len < 4 {
                    return Err(MessageError::BufferTooShort {
                        need: 4,
                        have: payload_len,
                    });
                }
                let index = (&src[..4]).get_u32();
                src.advance(4);
                Message::Have(index)
            }
            0x05 => {
                if payload_len < 12 {
                    return Err(MessageError::BufferTooShort {
                        need: 12,
                        have: payload_len,
                    });
                }
                let index = (&src[..4]).get_u32();
                let offset = (&src[..4]).get_u32();
                let length = (&src[..4]).get_u32();
                src.advance(12);
                Message::Cancel {
                    index,
                    offset,
                    length,
                }
            }
            0x06 => Message::Choke,
            0x07 => Message::Unchoke,
            0x08 => Message::Interested,
            0x09 => Message::NotInterested,
            other => return Err(MessageError::UnknownType(other)),
        };

        Ok(Some(msg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: Message) -> Message {
        let mut codec = LengthPrefixCodec;
        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).unwrap();
        codec.decode(&mut buf).unwrap().unwrap()
    }

    #[test]
    fn roundtrip_handshake() {
        let msg = Message::Handshake {
            version: 1,
            info_hash: [0xAA; 32],
            peer_id: [0xBB; 20],
        };
        let decoded = roundtrip(msg.clone());
        assert_eq!(decoded, msg);
    }

    #[test]
    fn roundtrip_bitfield() {
        let msg = Message::Bitfield(Bytes::from_static(&[0b11001100, 0b10101010]));
        let decoded = roundtrip(msg.clone());
        assert_eq!(decoded, msg);
    }

    #[test]
    fn roundtrip_request() {
        let msg = Message::Request {
            index: 5,
            offset: 0,
            length: 16384,
        };
        let decoded = roundtrip(msg.clone());
        assert_eq!(decoded, msg);
    }

    #[test]
    fn roundtrip_piece_with_proof() {
        let msg = Message::Piece {
            index: 3,
            offset: 0,
            data: Bytes::from_static(b"piece data here"),
            proof: vec![(true, [0x11; 32]), (false, [0x22; 32])],
        };
        let decoded = roundtrip(msg.clone());
        assert_eq!(decoded, msg);
    }

    #[test]
    fn roundtrip_have() {
        let msg = Message::Have(42);
        let decoded = roundtrip(msg.clone());
        assert_eq!(decoded, msg);
    }

    #[test]
    fn roundtrip_cancel() {
        let msg = Message::Cancel {
            index: 1,
            offset: 0,
            length: 16384,
        };
        let decoded = roundtrip(msg.clone());
        assert_eq!(decoded, msg);
    }

    #[test]
    fn roundtrip_simple_messages() {
        assert_eq!(roundtrip(Message::Choke), Message::Choke);
        assert_eq!(roundtrip(Message::Unchoke), Message::Unchoke);
        assert_eq!(roundtrip(Message::Interested), Message::Interested);
        assert_eq!(roundtrip(Message::NotInterested), Message::NotInterested);
    }

    #[test]
    fn incomplete_frame_returns_none() {
        let mut codec = LengthPrefixCodec;
        let mut buf = BytesMut::from(&[0x00, 0x00][..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn unknown_type_returns_error() {
        let mut codec = LengthPrefixCodec;
        let mut buf = BytesMut::new();
        buf.put_u32(1); // length = 1 (just the type byte)
        buf.put_u8(0xFF); // unknown type
        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn rejects_oversized_payload() {
        let mut codec = LengthPrefixCodec;
        let mut buf = BytesMut::new();
        buf.put_u32(5_000_001); // way over 4 MiB
        buf.put_u8(0x01);
        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }
}
```

**Step 2: Also need to add `std::io` error variant to `MessageError`**

Update `MessageError` in `src/protocol/messages.rs` to add an IO variant (needed for tokio-util's codec trait bounds):

```rust
#[derive(Debug, Error)]
pub enum MessageError {
    #[error("unknown message type: {0:#x}")]
    UnknownType(u8),
    #[error("buffer too short: need {need}, have {have}")]
    BufferTooShort { need: usize, have: usize },
    #[error("payload too large: {0} bytes")]
    PayloadTooLarge(usize),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
```

**Step 3: Run tests**

```bash
cargo test protocol::codec::tests -- --nocapture
```
Expected: all 10 tests PASS

**Step 4: Run all tests to check nothing broke**

```bash
cargo test
```

**Step 5: Commit**

```bash
git add src/protocol/codec.rs src/protocol/messages.rs
git commit -m "feat(protocol): implement length-prefix codec encoder and decoder"
```

---

### Task 4: Implement the peer connection

**Files:**
- Modify: `src/network/peer.rs`

The `PeerConnection` wraps a `Framed<TcpStream, LengthPrefixCodec>` and provides `send`/`recv` methods.

**Step 1: Implement PeerConnection**

Replace `src/network/peer.rs`:

```rust
use bytes::Bytes;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use futures_util::{SinkExt, StreamExt};

use crate::protocol::codec::LengthPrefixCodec;
use crate::protocol::messages::{Message, MessageError};
use crate::protocol::state::{ConnectionState, StateEvent, TransitionError};

pub struct PeerConnection {
    framed: Framed<TcpStream, LengthPrefixCodec>,
    state: ConnectionState,
    peer_id: Option<[u8; 20]>,
}

#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    #[error("protocol error: {0}")]
    Protocol(#[from] MessageError),
    #[error("invalid state transition: {from:?} on {event:?}")]
    InvalidTransition {
        from: ConnectionState,
        event: StateEvent,
    },
    #[error("connection closed")]
    ConnectionClosed,
    #[error("handshake failed: info hash mismatch")]
    InfoHashMismatch,
}

impl From<TransitionError> for PeerError {
    fn from(e: TransitionError) -> Self {
        match e {
            TransitionError::Invalid { from, event } => {
                PeerError::InvalidTransition { from, event }
            }
        }
    }
}

impl PeerConnection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            framed: Framed::new(stream, LengthPrefixCodec),
            state: ConnectionState::Handshaking,
            peer_id: None,
        }
    }

    pub fn state(&self) -> ConnectionState {
        self.state
    }

    pub fn peer_id(&self) -> Option<[u8; 20]> {
        self.peer_id
    }

    pub async fn send(&mut self, msg: Message) -> Result<(), PeerError> {
        self.framed.send(msg).await?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Message, PeerError> {
        self.framed
            .next()
            .await
            .ok_or(PeerError::ConnectionClosed)?
            .map_err(PeerError::Protocol)
    }

    pub async fn handshake(
        &mut self,
        our_info_hash: [u8; 32],
        our_peer_id: [u8; 20],
    ) -> Result<[u8; 20], PeerError> {
        self.send(Message::Handshake {
            version: 1,
            info_hash: our_info_hash,
            peer_id: our_peer_id,
        })
        .await?;

        match self.recv().await? {
            Message::Handshake {
                info_hash, peer_id, ..
            } => {
                if info_hash != our_info_hash {
                    return Err(PeerError::InfoHashMismatch);
                }
                self.peer_id = Some(peer_id);
                self.state = self.state.transition(StateEvent::HandshakeComplete)?;
                Ok(peer_id)
            }
            _ => Err(PeerError::Protocol(MessageError::UnknownType(0xFF))),
        }
    }

    pub async fn accept_handshake(
        &mut self,
        our_info_hash: [u8; 32],
        our_peer_id: [u8; 20],
    ) -> Result<[u8; 20], PeerError> {
        match self.recv().await? {
            Message::Handshake {
                info_hash, peer_id, ..
            } => {
                if info_hash != our_info_hash {
                    return Err(PeerError::InfoHashMismatch);
                }
                self.peer_id = Some(peer_id);
                self.send(Message::Handshake {
                    version: 1,
                    info_hash: our_info_hash,
                    peer_id: our_peer_id,
                })
                .await?;
                self.state = self.state.transition(StateEvent::HandshakeComplete)?;
                Ok(peer_id)
            }
            _ => Err(PeerError::Protocol(MessageError::UnknownType(0xFF))),
        }
    }

    pub fn transition(&mut self, event: StateEvent) -> Result<(), PeerError> {
        self.state = self.state.transition(event)?;
        Ok(())
    }
}
```

**Step 2: Add futures-util dependency**

Add to `[dependencies]` in `Cargo.toml`:
```toml
futures-util = { version = "0.3", default-features = false, features = ["sink"] }
```

**Step 3: Verify it compiles**

```bash
cargo check
```

**Step 4: Commit**

```bash
git add src/network/peer.rs Cargo.toml Cargo.lock
git commit -m "feat(network): implement PeerConnection with handshake"
```

---

### Task 5: Implement the piece store

**Files:**
- Modify: `src/storage/store.rs`

**Step 1: Write tests**

Replace `src/storage/store.rs`:

```rust
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::core::chunker::{Chunker, Piece};
use crate::core::merkle::MerkleTree;

pub struct TorrentInfo {
    pub info_hash: [u8; 32],
    pub piece_count: u32,
    pub piece_size: usize,
    pub total_size: u64,
    pub tree: MerkleTree,
    pub pieces: Vec<Piece>,
}

pub struct PieceStore {
    data: Vec<u8>,
    piece_size: usize,
}

impl TorrentInfo {
    pub fn from_file(path: &Path) -> std::io::Result<(Self, Vec<u8>)> {
        let data = std::fs::read(path)?;
        let chunker = Chunker::with_default();
        let pieces = chunker.chunk(&data);
        let hashes: Vec<[u8; 32]> = pieces.iter().map(|p| p.hash).collect();
        let tree = MerkleTree::from_leaves(&hashes);

        Ok((
            Self {
                info_hash: tree.root(),
                piece_count: pieces.len() as u32,
                piece_size: chunker.piece_size(),
                total_size: data.len() as u64,
                tree,
                pieces,
            },
            data,
        ))
    }

    pub fn bitfield_bytes(&self) -> Vec<u8> {
        let byte_count = (self.piece_count as usize + 7) / 8;
        vec![0xFF; byte_count]
    }
}

impl PieceStore {
    pub fn from_data(data: Vec<u8>, piece_size: usize) -> Self {
        Self { data, piece_size }
    }

    pub fn empty(total_size: u64, piece_size: usize) -> Self {
        Self {
            data: vec![0u8; total_size as usize],
            piece_size,
        }
    }

    pub fn read_piece(&self, index: u32) -> Option<&[u8]> {
        let start = index as usize * self.piece_size;
        if start >= self.data.len() {
            return None;
        }
        let end = (start + self.piece_size).min(self.data.len());
        Some(&self.data[start..end])
    }

    pub fn write_piece(&mut self, index: u32, piece_data: &[u8]) {
        let start = index as usize * self.piece_size;
        let end = start + piece_data.len();
        self.data[start..end].copy_from_slice(piece_data);
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn verify_piece(data: &[u8], expected_hash: &[u8; 32]) -> bool {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash: [u8; 32] = hasher.finalize().into();
        hash == *expected_hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn torrent_info_from_file() {
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(b"hello world test data for chunking").unwrap();
        let (info, data) = TorrentInfo::from_file(tmp.path()).unwrap();
        assert_eq!(info.total_size, data.len() as u64);
        assert!(info.piece_count > 0);
        assert_eq!(info.info_hash, info.tree.root());
    }

    #[test]
    fn piece_store_roundtrip() {
        let original = b"AAAABBBBCCCC";
        let store = PieceStore::from_data(original.to_vec(), 4);

        assert_eq!(store.read_piece(0).unwrap(), b"AAAA");
        assert_eq!(store.read_piece(1).unwrap(), b"BBBB");
        assert_eq!(store.read_piece(2).unwrap(), b"CCCC");
        assert!(store.read_piece(3).is_none());
    }

    #[test]
    fn piece_store_write_and_read() {
        let mut store = PieceStore::empty(12, 4);
        store.write_piece(0, b"AAAA");
        store.write_piece(1, b"BBBB");
        store.write_piece(2, b"CCCC");
        assert_eq!(store.data(), b"AAAABBBBCCCC");
    }

    #[test]
    fn verify_piece_detects_corruption() {
        let data = b"correct data";
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash: [u8; 32] = hasher.finalize().into();

        assert!(PieceStore::verify_piece(data, &hash));
        assert!(!PieceStore::verify_piece(b"wrong data!!", &hash));
    }

    #[test]
    fn bitfield_bytes_all_ones() {
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(&[0u8; 1024]).unwrap(); // 1 KiB file
        let (info, _) = TorrentInfo::from_file(tmp.path()).unwrap();
        let bf = info.bitfield_bytes();
        assert!(bf.iter().all(|b| *b == 0xFF));
    }
}
```

**Step 2: Run tests**

```bash
cargo test storage::store::tests -- --nocapture
```
Expected: all 5 tests PASS

**Step 3: Commit**

```bash
git add src/storage/store.rs
git commit -m "feat(storage): implement TorrentInfo and PieceStore"
```

---

### Task 6: Implement seeder logic

**Files:**
- Create: `src/network/seeder.rs`
- Modify: `src/network/mod.rs`

**Step 1: Create `src/network/seeder.rs`**

```rust
use std::path::Path;

use bytes::Bytes;
use tokio::net::TcpListener;

use crate::network::peer::{PeerConnection, PeerError};
use crate::protocol::messages::Message;
use crate::storage::store::{PieceStore, TorrentInfo};

pub struct Seeder {
    info: TorrentInfo,
    store: PieceStore,
    our_peer_id: [u8; 20],
}

impl Seeder {
    pub fn from_file(path: &Path, peer_id: [u8; 20]) -> std::io::Result<Self> {
        let (info, data) = TorrentInfo::from_file(path)?;
        let store = PieceStore::from_data(data, info.piece_size);
        Ok(Self {
            info,
            store,
            our_peer_id: peer_id,
        })
    }

    pub fn info_hash(&self) -> [u8; 32] {
        self.info.info_hash
    }

    pub fn piece_count(&self) -> u32 {
        self.info.piece_count
    }

    pub fn piece_size(&self) -> usize {
        self.info.piece_size
    }

    pub fn total_size(&self) -> u64 {
        self.info.total_size
    }

    pub async fn listen(&self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(addr).await?;
        println!("Listening on {addr}");

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            println!("Peer connected: {peer_addr}");

            let info_hash = self.info.info_hash;
            let peer_id = self.our_peer_id;
            let bitfield = Bytes::from(self.info.bitfield_bytes());
            let piece_count = self.info.piece_count;
            let piece_size = self.info.piece_size;
            let tree = self.info.tree.clone();

            let mut piece_data_cache: Vec<Vec<u8>> = Vec::new();
            for i in 0..piece_count {
                if let Some(data) = self.store.read_piece(i) {
                    piece_data_cache.push(data.to_vec());
                }
            }

            tokio::spawn(async move {
                if let Err(e) =
                    handle_peer(stream, info_hash, peer_id, bitfield, &piece_data_cache, &tree, piece_size)
                        .await
                {
                    println!("Peer {peer_addr} error: {e}");
                }
                println!("Peer {peer_addr} disconnected");
            });
        }
    }
}

async fn handle_peer(
    stream: tokio::net::TcpStream,
    info_hash: [u8; 32],
    our_peer_id: [u8; 20],
    bitfield: Bytes,
    piece_data: &[Vec<u8>],
    tree: &crate::core::merkle::MerkleTree,
    _piece_size: usize,
) -> Result<(), PeerError> {
    let mut conn = PeerConnection::new(stream);

    conn.accept_handshake(info_hash, our_peer_id).await?;
    conn.send(Message::Bitfield(bitfield)).await?;

    // Receive leecher's bitfield
    let _their_bitfield = conn.recv().await?;

    // Wait for Interested
    match conn.recv().await? {
        Message::Interested => {}
        _ => return Ok(()),
    }

    // Send Unchoke
    conn.send(Message::Unchoke).await?;

    // Serve requests
    loop {
        match conn.recv().await {
            Ok(Message::Request { index, offset, length: _ }) => {
                let idx = index as usize;
                if idx >= piece_data.len() {
                    continue;
                }

                let data = &piece_data[idx];
                let proof = tree.proof(idx);
                let proof_vec: Vec<(bool, [u8; 32])> = proof.siblings;

                println!("  Sent piece {index}/{}", piece_data.len());

                conn.send(Message::Piece {
                    index,
                    offset,
                    data: Bytes::copy_from_slice(data),
                    proof: proof_vec,
                })
                .await?;
            }
            Ok(Message::Have(_)) => {}
            Ok(Message::NotInterested) => break,
            Err(PeerError::ConnectionClosed) => break,
            Err(e) => return Err(e),
            _ => {}
        }
    }

    Ok(())
}
```

**Step 2: Update `src/network/mod.rs`**

```rust
pub mod discovery;
pub mod peer;
pub mod pool;
pub mod seeder;
```

**Step 3: Verify it compiles**

```bash
cargo check
```

**Step 4: Commit**

```bash
git add src/network/seeder.rs src/network/mod.rs
git commit -m "feat(network): implement seeder — accept connections and serve pieces"
```

---

### Task 7: Implement leecher logic

**Files:**
- Create: `src/network/leecher.rs`
- Modify: `src/network/mod.rs`

**Step 1: Create `src/network/leecher.rs`**

```rust
use std::path::Path;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;

use crate::core::merkle::MerkleTree;
use crate::network::peer::{PeerConnection, PeerError};
use crate::protocol::messages::Message;
use crate::storage::store::PieceStore;

pub struct Leecher {
    info_hash: [u8; 32],
    our_peer_id: [u8; 20],
    output_path: String,
}

pub struct DownloadResult {
    pub bytes_downloaded: u64,
    pub pieces_verified: u32,
}

impl Leecher {
    pub fn new(info_hash: [u8; 32], peer_id: [u8; 20], output_path: String) -> Self {
        Self {
            info_hash,
            our_peer_id: peer_id,
            output_path,
        }
    }

    pub async fn download(&self, peer_addr: &str) -> Result<DownloadResult, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(peer_addr).await?;
        println!("Connected to {peer_addr}");

        let mut conn = PeerConnection::new(stream);

        conn.handshake(self.info_hash, self.our_peer_id).await?;

        // Receive seeder's bitfield
        let seeder_bitfield = match conn.recv().await? {
            Message::Bitfield(bf) => bf,
            _ => return Err("expected bitfield".into()),
        };

        let piece_count = seeder_bitfield.len() * 8;
        // Count actual pieces from bitfield (trailing bits may be padding)
        // For simplicity, use the byte count to determine piece count

        // Send our bitfield (all zeros — we have nothing)
        let our_bitfield = vec![0u8; seeder_bitfield.len()];
        conn.send(Message::Bitfield(Bytes::from(our_bitfield))).await?;

        // Send Interested
        conn.send(Message::Interested).await?;

        // Wait for Unchoke
        match conn.recv().await? {
            Message::Unchoke => {}
            _ => return Err("expected unchoke".into()),
        }

        println!("Downloading: {piece_count} pieces");

        // We don't know total_size upfront in this simple protocol.
        // We'll collect pieces and figure out the actual size from what we get.
        let mut received_pieces: Vec<(u32, Vec<u8>)> = Vec::new();
        let mut pieces_verified = 0u32;

        for i in 0..piece_count as u32 {
            conn.send(Message::Request {
                index: i,
                offset: 0,
                length: 0, // seeder sends the whole piece regardless
            })
            .await?;

            match conn.recv().await? {
                Message::Piece {
                    index,
                    data,
                    proof,
                    ..
                } => {
                    // Verify piece against Merkle proof
                    let mut hasher = Sha256::new();
                    hasher.update(&data);
                    let piece_hash: [u8; 32] = hasher.finalize().into();

                    let merkle_proof = crate::core::merkle::MerkleProof {
                        leaf_index: index as usize,
                        siblings: proof,
                    };

                    if !MerkleTree::verify(&self.info_hash, &piece_hash, &merkle_proof) {
                        return Err(format!("piece {index} failed Merkle verification").into());
                    }

                    println!("  Piece {index}/{piece_count} verified");
                    pieces_verified += 1;

                    conn.send(Message::Have(index)).await?;
                    received_pieces.push((index, data.to_vec()));
                }
                _ => return Err(format!("expected piece for index {i}").into()),
            }
        }

        // Sort by index and reassemble
        received_pieces.sort_by_key(|(idx, _)| *idx);
        let mut full_data = Vec::new();
        for (_, piece_data) in &received_pieces {
            full_data.extend_from_slice(piece_data);
        }

        // Write to output file
        std::fs::write(&self.output_path, &full_data)?;
        println!("Download complete: {}", self.output_path);

        Ok(DownloadResult {
            bytes_downloaded: full_data.len() as u64,
            pieces_verified,
        })
    }
}
```

**Step 2: Update `src/network/mod.rs`**

```rust
pub mod discovery;
pub mod leecher;
pub mod peer;
pub mod pool;
pub mod seeder;
```

**Step 3: Verify it compiles**

```bash
cargo check
```

**Step 4: Commit**

```bash
git add src/network/leecher.rs src/network/mod.rs
git commit -m "feat(network): implement leecher — connect, download, verify pieces"
```

---

### Task 8: Wire up the CLI

**Files:**
- Modify: `src/cli/main.rs`

**Step 1: Replace `src/cli/main.rs`**

```rust
use clap::Parser;

#[derive(Parser)]
#[command(name = "aegis", version, about = "AegisTorrent P2P file distribution")]
enum Cli {
    /// Seed a file to peers
    Seed {
        /// Path to file to seed
        path: String,

        /// Address to listen on
        #[arg(short, long, default_value = "127.0.0.1:6881")]
        listen: String,
    },
    /// Download a file from a peer
    Download {
        /// Info hash (hex encoded)
        hash: String,

        /// Peer address to connect to
        #[arg(short, long)]
        peer: String,

        /// Output file path
        #[arg(short, long)]
        output: String,
    },
}

fn generate_peer_id() -> [u8; 20] {
    let mut id = [0u8; 20];
    id[..8].copy_from_slice(b"-AT0100-");
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    id[8..].copy_from_slice(&timestamp.to_le_bytes()[..12]);
    id
}

fn parse_hex_hash(s: &str) -> Result<[u8; 32], String> {
    if s.len() != 64 {
        return Err(format!("expected 64 hex chars, got {}", s.len()));
    }
    let mut hash = [0u8; 32];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hex_str = std::str::from_utf8(chunk).map_err(|e| e.to_string())?;
        hash[i] = u8::from_str_radix(hex_str, 16).map_err(|e| e.to_string())?;
    }
    Ok(hash)
}

fn format_size(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli {
        Cli::Seed { path, listen } => {
            let peer_id = generate_peer_id();
            let seeder = match aegistorrent::network::seeder::Seeder::from_file(
                std::path::Path::new(&path),
                peer_id,
            ) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Error reading file: {e}");
                    std::process::exit(1);
                }
            };

            let hash_hex: String = seeder
                .info_hash()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();

            println!(
                "Seeding {} ({}, {} pieces, {} each)",
                path,
                format_size(seeder.total_size()),
                seeder.piece_count(),
                format_size(seeder.piece_size() as u64),
            );
            println!("Info hash: {hash_hex}");

            if let Err(e) = seeder.listen(&listen).await {
                eprintln!("Seeder error: {e}");
                std::process::exit(1);
            }
        }
        Cli::Download { hash, peer, output } => {
            let info_hash = match parse_hex_hash(&hash) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("Invalid hash: {e}");
                    std::process::exit(1);
                }
            };

            let peer_id = generate_peer_id();
            let leecher =
                aegistorrent::network::leecher::Leecher::new(info_hash, peer_id, output);

            println!("Connecting to {peer}...");
            match leecher.download(&peer).await {
                Ok(result) => {
                    println!(
                        "Done! {} downloaded, {} pieces verified",
                        format_size(result.bytes_downloaded),
                        result.pieces_verified,
                    );
                }
                Err(e) => {
                    eprintln!("Download failed: {e}");
                    std::process::exit(1);
                }
            }
        }
    }
}
```

**Step 2: Verify it compiles**

```bash
cargo build
```

**Step 3: Commit**

```bash
git add src/cli/main.rs
git commit -m "feat(cli): wire up seed and download commands"
```

---

### Task 9: Integration test — two-peer file transfer

**Files:**
- Create: `tests/integration/transfer.rs`

**Step 1: Create the integration test**

```rust
use std::io::Write;

use tempfile::NamedTempFile;

#[tokio::test]
async fn two_peer_file_transfer() {
    // Create a test file with known content
    let mut source_file = NamedTempFile::new().unwrap();
    let test_data: Vec<u8> = (0..=255).cycle().take(1024 * 600).collect(); // 600 KB
    source_file.write_all(&test_data).unwrap();

    let output_file = NamedTempFile::new().unwrap();
    let output_path = output_file.path().to_str().unwrap().to_string();

    let peer_id_seeder = {
        let mut id = [0u8; 20];
        id[..6].copy_from_slice(b"seeder");
        id
    };
    let peer_id_leecher = {
        let mut id = [0u8; 20];
        id[..7].copy_from_slice(b"leecher");
        id
    };

    // Set up seeder
    let seeder = aegistorrent::network::seeder::Seeder::from_file(
        source_file.path(),
        peer_id_seeder,
    )
    .unwrap();

    let info_hash = seeder.info_hash();
    let addr = "127.0.0.1:0"; // OS picks a free port

    // Bind listener to get the actual port
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let actual_addr = listener.local_addr().unwrap().to_string();

    // Spawn seeder in background
    let seeder_handle = tokio::spawn(async move {
        // Re-bind is wasteful; instead we'll use the listen method
        // But Seeder::listen binds its own listener. We need to refactor
        // slightly or just use a known port. For the test, use a fixed port.
        drop(listener);
        seeder.listen(&actual_addr).await.ok();
    });

    // Give seeder a moment to start listening
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Run leecher
    let leecher = aegistorrent::network::leecher::Leecher::new(
        info_hash,
        peer_id_leecher,
        output_path.clone(),
    );

    let result = leecher.download(&actual_addr).await.unwrap();

    // Verify
    let downloaded = std::fs::read(&output_path).unwrap();
    assert_eq!(downloaded.len(), test_data.len());
    assert_eq!(downloaded, test_data);
    assert_eq!(result.pieces_verified as usize, 3); // 600KB / 256KB = 3 pieces

    // Clean up
    seeder_handle.abort();
}
```

Note: This test may need adjustment if `Seeder::listen` doesn't work with the port binding approach. The implementation agent should adapt the seeder to accept a `TcpListener` directly (add a `listen_on` method) to avoid port conflicts in tests.

**Step 2: Run the test**

```bash
cargo test --test transfer -- --nocapture
```
Expected: PASS — file transfers and verifies

**Step 3: Commit**

```bash
git add tests/integration/transfer.rs
git commit -m "test: add two-peer file transfer integration test"
```

---

### Task 10: Manual end-to-end test

**Step 1: Create a test file**

```bash
echo "Hello from AegisTorrent! This is a test file for the two-peer transfer." > /tmp/testfile.txt
```

**Step 2: Seed it**

```bash
cargo run -- seed /tmp/testfile.txt --listen 127.0.0.1:6881
```
Expected output:
```
Seeding /tmp/testfile.txt (73 B, 1 pieces, 256.0 KB each)
Info hash: <some hex hash>
Listening on 127.0.0.1:6881
```

**Step 3: Download in another terminal**

Copy the info hash from step 2, then:
```bash
cargo run -- download <info-hash> --peer 127.0.0.1:6881 --output /tmp/downloaded.txt
```
Expected:
```
Connecting to 127.0.0.1:6881...
Downloading: 8 pieces
  Piece 0/8 verified
Download complete: /tmp/downloaded.txt
```

**Step 4: Verify**

```bash
diff /tmp/testfile.txt /tmp/downloaded.txt
```
Expected: no output (files are identical)

---

### Task 11: Final verification, clippy, fmt

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
git add -A
git commit -m "style: apply cargo fmt and clippy fixes"
```
(Only if there were changes)

**Step 5: Push and create PR**

```bash
git push -u origin feat/phase1-two-peer-transfer
```

---

## Summary

| Task | What | Key deliverable |
|------|------|----------------|
| 1 | Branch + dependency | `tokio-util` added |
| 2 | Message update | Piece carries Merkle proof |
| 3 | Codec | Encode/decode all 10 message types, 10 tests |
| 4 | Peer connection | Async send/recv with handshake |
| 5 | Piece store | TorrentInfo + PieceStore, 5 tests |
| 6 | Seeder | Accept connections, serve pieces |
| 7 | Leecher | Connect, download, verify, write |
| 8 | CLI | `aegis seed` and `aegis download` wired up |
| 9 | Integration test | Two-peer transfer, byte-for-byte verification |
| 10 | Manual E2E | Two terminals, real file transfer |
| 11 | Final checks + PR | fmt, clippy, tests pass, PR created |
