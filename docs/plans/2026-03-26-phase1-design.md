# Phase 1 — Foundation: Two-Peer File Transfer

## Goal

A working end-to-end file transfer between two peers over TCP on localhost, with Merkle verification on every piece. Usable from the CLI (`aegis seed` / `aegis download`) and validated by an integration test.

## Architecture

Six layers, built bottom-up:

1. **Codec** — encode/decode `Message` to length-prefixed bytes on TCP
2. **Peer connection** — async TCP wrapper using the codec + state machine
3. **Piece store** — chunk files, serve pieces, receive and reassemble
4. **Seeder** — accept connections, serve requested pieces with Merkle proofs
5. **Leecher** — connect, request pieces sequentially, verify, write to disk
6. **CLI** — wire `aegis seed` and `aegis download` to the above

## Wire Format

```
┌──────────┬──────────┬──────────┐
│ 4 bytes  │ 1 byte   │ N bytes  │
│ length   │ msg type │ payload  │
└──────────┴──────────┴──────────┘
```

Length = 1 (type) + N (payload). Max payload: 4 MiB.

### Payload Layouts

| Message | Type | Payload |
|---------|------|---------|
| Handshake | 0x00 | version(1) + info_hash(32) + peer_id(20) |
| Bitfield | 0x01 | variable bytes (1 bit per piece) |
| Request | 0x02 | index(4) + offset(4) + length(4) |
| Piece | 0x03 | index(4) + offset(4) + proof_len(2) + proof_data(var) + piece_data(var) |
| Have | 0x04 | index(4) |
| Cancel | 0x05 | index(4) + offset(4) + length(4) |
| Choke | 0x06 | (empty) |
| Unchoke | 0x07 | (empty) |
| Interested | 0x08 | (empty) |
| NotInterested | 0x09 | (empty) |

Piece message includes the Merkle proof inline (log2(n) × 33 bytes: 1 bool + 32 hash per sibling).

## Connection Flow

```
Leecher                          Seeder
   │──── TCP connect ──────────────▶│
   │──── Handshake ────────────────▶│
   │◀─── Handshake ─────────────────│  verify info_hash match
   │◀─── Bitfield ──────────────────│  seeder: all 1s
   │──── Bitfield ─────────────────▶│  leecher: all 0s
   │──── Interested ───────────────▶│
   │◀─── Unchoke ──────────────────│
   │──── Request{i} ──────────────▶│  ┐
   │◀─── Piece{i,proof,data} ──────│  │ per piece
   │──── Have{i} ──────────────────▶│  ┘
   │──── close ────────────────────▶│
```

## Piece Verification

Each Piece message includes a Merkle proof (sibling hashes from leaf to root). The leecher:

1. SHA-256 hashes the received piece data
2. Walks the proof up to the root
3. Compares against the known info_hash (Merkle root)
4. Rejects the piece if verification fails

## CLI

```bash
aegis seed <file> --listen <addr>        # chunks, prints info-hash, serves
aegis download <hash> --peer <addr> --output <path>  # connects, downloads, verifies
```

## Testing

Integration test: spawn seeder + leecher as async tasks, transfer a temp file, assert byte-for-byte match.

## New Dependency

- `tokio-util` (codec traits + `Framed` adapter)
