# Phase 2 — Multi-Peer Concurrent Download

## Goal

Multiple peers download from and upload to each other simultaneously, with intelligent piece scheduling, fairness enforcement, and non-blocking disk I/O.

## Architecture

Three new components replace the single-peer leecher:

```
DownloadCoordinator
  │
  ├── Scheduler (rarest-first, endgame, choking decisions)
  │
  ├── ConnectionPool
  │     ├── PeerConnection #1 ─── TCP ─── Peer A
  │     ├── PeerConnection #2 ─── TCP ─── Peer B
  │     └── PeerConnection #3 ─── TCP ─── Peer C
  │
  └── DiskWriter (async write queue)
```

Communication via tokio mpsc channels:
- Pool → Coordinator: events (peer connected, bitfield received, piece received, peer disconnected)
- Coordinator → Pool: commands (request piece, cancel piece, choke/unchoke peer)

## Scheduler

Pure state machine, no I/O. Tracks:
- Piece rarity map (piece index → holder count)
- Our bitfield (completed pieces)
- In-flight requests (piece → peer)

### Rarest-First Selection
1. Filter to pieces the target peer has
2. Remove completed and in-flight pieces
3. Sort by rarity ascending
4. Break ties randomly
5. Return winning piece index

### Endgame Mode
Triggers when all remaining pieces are already in-flight. Sends duplicate requests to every peer that has the piece. First response wins, CANCEL sent to others.

### Choking (every 10 seconds)
- Rank peers by upload speed to us (bytes in last 30s)
- Unchoke top 4 fastest
- Optimistic unchoke 1 random choked peer (rotate every 30s)
- Choke everyone else

## Connection Pool

Manages N PeerConnections. Per peer:
- Handshake + bitfield exchange
- Message loop forwarding events to coordinator
- Accepts commands from coordinator

## Async Disk Writer

Background task receiving `(index, data)` over a channel. Pre-allocates output file to full size. Writes pieces at correct offsets using tokio::fs. Coordinator marks pieces as verified immediately without waiting for disk.

## CLI Changes

```bash
aegis download <hash> --peer 127.0.0.1:6881 --peer 127.0.0.1:6882 --output ./file.txt
```

Seeder unchanged — already handles multiple connections.

## Testing

- Unit tests: scheduler (rarest-first, endgame, choking) with mock bitfields
- Integration: 2-3 seeders on localhost, one multi-peer leecher, byte-for-byte verification
- Endgame: test with slow peer to verify CANCEL works
