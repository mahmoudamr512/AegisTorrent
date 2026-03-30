use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use crate::core::merkle::{MerkleProof, MerkleTree};
use crate::core::scheduler::{parse_bitfield_bytes, PeerId, Scheduler};
use crate::core::swarm::SwarmIntel;
use crate::network::discovery::pex::PexManager;
use crate::network::pool::{ConnectionPool, PoolCommand, PoolEvent};
use crate::protocol::messages::PexPeer;
use crate::security::reputation::{FailureKind, ReputationManager};
use crate::storage::writer::DiskWriter;

pub struct DownloadCoordinator {
    info_hash: [u8; 32],
    our_peer_id: PeerId,
    piece_size: usize,
    total_size: u64,
    output_path: PathBuf,
    peer_addrs: Vec<String>,
    dht_addrs: HashSet<String>,
}

pub struct DownloadResult {
    pub bytes_downloaded: u64,
    pub pieces_verified: u32,
    pub peers_used: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct PeerStats {
    pub id: String,
    pub speed_bps: f64,
    pub score: f64,
    pub strikes: u8,
    pub pipeline: usize,
    pub source: String,
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
    pub dht_nodes: usize,
    pub dht_peers_found: usize,
    pub pex_peers_found: usize,
}

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
            dht_nodes: 0,
            dht_peers_found: 0,
            pex_peers_found: 0,
        }
    }
}

impl DownloadCoordinator {
    pub fn new(
        info_hash: [u8; 32],
        our_peer_id: PeerId,
        piece_size: usize,
        total_size: u64,
        output_path: PathBuf,
        peer_addrs: Vec<String>,
        dht_addrs: Vec<String>,
    ) -> Self {
        Self {
            info_hash,
            our_peer_id,
            piece_size,
            total_size,
            output_path,
            peer_addrs,
            dht_addrs: dht_addrs.into_iter().collect(),
        }
    }

    pub async fn run(
        self,
        progress: Option<Arc<Mutex<DownloadProgress>>>,
    ) -> Result<DownloadResult, Box<dyn std::error::Error + Send + Sync>> {
        let update_progress = |progress: &Option<Arc<Mutex<DownloadProgress>>>,
                               f: &dyn Fn(&mut DownloadProgress)| {
            if let Some(p) = progress {
                if let Ok(mut guard) = p.lock() {
                    f(&mut guard);
                }
            }
        };

        let (event_tx, mut event_rx) = mpsc::channel::<PoolEvent>(128);
        let (cmd_tx, cmd_rx) = mpsc::channel::<PoolCommand>(128);

        let bitfield_bytes = vec![
            0u8;
            (self.total_size as usize)
                .div_ceil(self.piece_size)
                .div_ceil(8)
        ];
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

        let mut reputation = ReputationManager::new();
        let mut swarm = SwarmIntel::new();
        let mut request_timestamps: HashMap<u32, (PeerId, Instant)> = HashMap::new();
        let mut peer_sources: HashMap<PeerId, String> = HashMap::new();

        let mut pex_manager = PexManager::new();
        let dht_peers_found = self.dht_addrs.len();
        let dht_node_count = dht_peers_found;
        let mut pex_peers_found = 0usize;

        update_progress(&progress, &|p| {
            p.dht_nodes = dht_node_count;
            p.dht_peers_found = dht_peers_found;
        });

        let mut stale_check = tokio::time::interval(tokio::time::Duration::from_millis(500));
        let mut pex_tick = tokio::time::interval(tokio::time::Duration::from_secs(30));

        while !scheduler.is_complete() {
            let event = tokio::select! {
                ev = event_rx.recv() => {
                    match ev {
                        Some(e) => e,
                        None => break,
                    }
                }
                _ = stale_check.tick() => {
                    let stale = swarm.stale_requests(&request_timestamps);
                    for piece in stale {
                        if let Some((old_peer, _)) = request_timestamps.remove(&piece) {
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
                    continue;
                }
                _ = pex_tick.tick() => {
                    let peer_list: Vec<PeerId> = unchoked_peers.iter().copied().collect();
                    for peer_id in peer_list {
                        let (added, dropped) = pex_manager.generate_pex(&peer_id);
                        if !added.is_empty() || !dropped.is_empty() {
                            let _ = cmd_tx.send(PoolCommand::SendPex { peer_id, added, dropped }).await;
                        }
                    }
                    continue;
                }
            };

            match event {
                PoolEvent::PeerConnected { peer_id, addr } => {
                    peers_used.insert(peer_id);
                    let source = if self.dht_addrs.contains(&addr) {
                        "D".to_string()
                    } else {
                        "M".to_string()
                    };
                    peer_sources.entry(peer_id).or_insert(source);
                    pex_manager.add_peer(PexPeer {
                        peer_id,
                        addr: String::new(),
                        score: 1.0,
                    });
                    update_progress(&progress, &|p| p.peers_connected += 1);
                }
                PoolEvent::BitfieldReceived { peer_id, bitfield } => {
                    let bf = parse_bitfield_bytes(&bitfield, piece_count);
                    scheduler.register_peer(peer_id, &bf);
                    let _ = cmd_tx.send(PoolCommand::SendInterested { peer_id }).await;
                }
                PoolEvent::Unchoked { peer_id } => {
                    unchoked_peers.insert(peer_id);
                    request_pieces(
                        &mut scheduler,
                        &cmd_tx,
                        &peer_id,
                        &swarm,
                        &reputation,
                        &mut request_timestamps,
                    )
                    .await;
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

                        if let Some((req_peer, sent_at)) = request_timestamps.remove(&index) {
                            let duration = sent_at.elapsed();
                            reputation.record_success(
                                req_peer,
                                data.len() as u64,
                                duration.as_millis() as u64,
                            );
                            swarm.record_response(req_peer, duration.as_millis() as u64);
                        }

                        let _ = writer
                            .write_piece(index, self.piece_size, data.to_vec())
                            .await;
                        let _ = cmd_tx.send(PoolCommand::SendHave { index }).await;
                        update_progress(&progress, &|p| {
                            p.pieces_done += 1;
                            p.bytes_done = p.pieces_done as u64 * self.piece_size as u64;
                        });

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
                        request_timestamps.remove(&index);
                        reputation.record_failure(peer_id, FailureKind::BadHash);
                        if reputation.is_banned(&peer_id) {
                            let _ = cmd_tx.send(PoolCommand::DisconnectPeer { peer_id }).await;
                            unchoked_peers.remove(&peer_id);
                        }
                    }

                    if unchoked_peers.contains(&peer_id) {
                        request_pieces(
                            &mut scheduler,
                            &cmd_tx,
                            &peer_id,
                            &swarm,
                            &reputation,
                            &mut request_timestamps,
                        )
                        .await;
                    }
                }
                PoolEvent::HaveReceived { peer_id, index } => {
                    scheduler.peer_has_piece(&peer_id, index);
                }
                PoolEvent::PeerDisconnected { peer_id } => {
                    update_progress(&progress, &|p| {
                        p.peers_connected = p.peers_connected.saturating_sub(1);
                    });
                    scheduler.unregister_peer(&peer_id);
                    unchoked_peers.remove(&peer_id);
                    reputation.remove_peer(&peer_id);
                    swarm.remove_peer(&peer_id);
                    pex_manager.remove_peer(&peer_id);
                    peer_sources.remove(&peer_id);
                    request_timestamps.retain(|_, (p, _)| *p != peer_id);
                }
                PoolEvent::PexReceived {
                    peer_id: _,
                    added,
                    dropped,
                } => {
                    for peer in &added {
                        if !unchoked_peers.contains(&peer.peer_id)
                            && !reputation.is_banned(&peer.peer_id)
                        {
                            let _ = cmd_tx
                                .send(PoolCommand::ConnectPeer {
                                    addr: peer.addr.clone(),
                                })
                                .await;
                            peer_sources.insert(peer.peer_id, "P".to_string());
                        }
                    }
                    pex_peers_found += added.len();
                    pex_manager.merge_received(added);
                    for id in dropped {
                        pex_manager.remove_peer(&id);
                    }
                    update_progress(&progress, &|p| {
                        p.pex_peers_found = pex_peers_found;
                        p.dht_peers_found = dht_peers_found;
                    });
                }
            }

            // Update peer stats and rarity after each event
            let peer_stats: Vec<PeerStats> = unchoked_peers
                .iter()
                .map(|pid| PeerStats {
                    id: format!("{:02x}{:02x}", pid[0], pid[1]),
                    speed_bps: reputation.peer_speed_bps(pid),
                    score: reputation.score(pid),
                    strikes: reputation.strike_level(pid),
                    pipeline: swarm.pipeline_depth(pid, &reputation),
                    source: peer_sources
                        .get(pid)
                        .cloned()
                        .unwrap_or_else(|| "M".to_string()),
                })
                .collect();
            let rarity: Vec<u32> = (0..piece_count)
                .map(|i| scheduler.piece_rarity(i))
                .collect();
            update_progress(&progress, &|p| {
                p.peer_stats = peer_stats.clone();
                p.piece_rarity = rarity.clone();
            });
        }

        update_progress(&progress, &|p| p.complete = true);

        drop(cmd_tx);
        pool_handle.abort();
        writer.finish().await?;

        Ok(DownloadResult {
            bytes_downloaded: self.total_size,
            pieces_verified,
            peers_used: peers_used.len(),
        })
    }
}

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
                request_timestamps
                    .insert(assignment.piece_index, (assignment.peer, Instant::now()));
            }
            None => break,
        }
    }
}
