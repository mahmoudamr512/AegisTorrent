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

        let bitfield_bytes =
            vec![0u8; (self.total_size as usize).div_ceil(self.piece_size).div_ceil(8)];
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
        let mut completed_pieces = std::collections::HashSet::<u32>::new();

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
                    let _ = cmd_tx
                        .send(PoolCommand::SendInterested { peer_id })
                        .await;
                }
                PoolEvent::Unchoked { peer_id } => {
                    unchoked_peers.insert(peer_id);
                    Self::request_pieces(&mut scheduler, &cmd_tx, &peer_id).await;
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
                    if completed_pieces.contains(&index) {
                        continue;
                    }

                    let mut hasher = Sha256::new();
                    hasher.update(&data);
                    let leaf_hash: [u8; 32] = hasher.finalize().into();

                    let merkle_proof = MerkleProof {
                        leaf_index: index as usize,
                        siblings: proof,
                    };

                    if MerkleTree::verify(&self.info_hash, &leaf_hash, &merkle_proof) {
                        pieces_verified += 1;
                        completed_pieces.insert(index);
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
                        Self::request_pieces(&mut scheduler, &cmd_tx, &peer_id).await;
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
        scheduler: &mut Scheduler,
        cmd_tx: &mpsc::Sender<PoolCommand>,
        peer_id: &PeerId,
    ) {
        if let Some(assignment) = scheduler.pick_piece(peer_id) {
            let _ = cmd_tx
                .send(PoolCommand::RequestPiece {
                    peer_id: assignment.peer,
                    index: assignment.piece_index,
                })
                .await;
        }
    }
}
