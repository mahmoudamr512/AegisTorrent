use bytes::Bytes;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::core::scheduler::PeerId;
use crate::network::peer::{PeerConnection, PeerError};
use crate::protocol::messages::{Message, PexPeer};

#[derive(Debug)]
pub enum PoolEvent {
    PeerConnected {
        peer_id: PeerId,
    },
    BitfieldReceived {
        peer_id: PeerId,
        bitfield: Bytes,
    },
    PieceReceived {
        peer_id: PeerId,
        index: u32,
        data: Bytes,
        proof: Vec<(bool, [u8; 32])>,
    },
    HaveReceived {
        peer_id: PeerId,
        index: u32,
    },
    Choked {
        peer_id: PeerId,
    },
    Unchoked {
        peer_id: PeerId,
    },
    PeerDisconnected {
        peer_id: PeerId,
    },
    PexReceived {
        peer_id: PeerId,
        added: Vec<PexPeer>,
        dropped: Vec<[u8; 20]>,
    },
}

#[derive(Debug)]
pub enum PoolCommand {
    RequestPiece {
        peer_id: PeerId,
        index: u32,
    },
    CancelPiece {
        peer_id: PeerId,
        index: u32,
    },
    SendHave {
        index: u32,
    },
    ChokePeer {
        peer_id: PeerId,
    },
    UnchokePeer {
        peer_id: PeerId,
    },
    SendInterested {
        peer_id: PeerId,
    },
    DisconnectPeer {
        peer_id: PeerId,
    },
    SendPex {
        peer_id: PeerId,
        added: Vec<PexPeer>,
        dropped: Vec<[u8; 20]>,
    },
}

pub struct ConnectionPool {
    info_hash: [u8; 32],
    our_peer_id: PeerId,
    our_bitfield: Bytes,
    event_tx: mpsc::Sender<PoolEvent>,
    command_rx: mpsc::Receiver<PoolCommand>,
    peer_txs: std::collections::HashMap<PeerId, mpsc::Sender<PeerCommand>>,
}

enum PeerCommand {
    Send(Message),
}

impl ConnectionPool {
    pub fn new(
        info_hash: [u8; 32],
        our_peer_id: PeerId,
        our_bitfield: Bytes,
        event_tx: mpsc::Sender<PoolEvent>,
        command_rx: mpsc::Receiver<PoolCommand>,
    ) -> Self {
        Self {
            info_hash,
            our_peer_id,
            our_bitfield,
            event_tx,
            command_rx,
            peer_txs: std::collections::HashMap::new(),
        }
    }

    pub async fn connect(&mut self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(addr).await?;
        let mut conn = PeerConnection::new(stream);
        let remote_id = conn.handshake(self.info_hash, self.our_peer_id).await?;

        conn.send(Message::Bitfield(self.our_bitfield.clone()))
            .await?;

        let event_tx = self.event_tx.clone();
        let (peer_cmd_tx, mut peer_cmd_rx) = mpsc::channel::<PeerCommand>(32);

        self.peer_txs.insert(remote_id, peer_cmd_tx);

        let _ = event_tx
            .send(PoolEvent::PeerConnected { peer_id: remote_id })
            .await;

        tokio::spawn(async move {
            Self::peer_loop(remote_id, &mut conn, &event_tx, &mut peer_cmd_rx).await;
        });

        Ok(())
    }

    pub async fn run(&mut self) {
        while let Some(cmd) = self.command_rx.recv().await {
            match cmd {
                PoolCommand::RequestPiece { peer_id, index } => {
                    self.send_to_peer(
                        &peer_id,
                        Message::Request {
                            index,
                            offset: 0,
                            length: 0,
                        },
                    )
                    .await;
                }
                PoolCommand::CancelPiece { peer_id, index } => {
                    self.send_to_peer(
                        &peer_id,
                        Message::Cancel {
                            index,
                            offset: 0,
                            length: 0,
                        },
                    )
                    .await;
                }
                PoolCommand::SendHave { index } => {
                    let peers: Vec<PeerId> = self.peer_txs.keys().copied().collect();
                    for peer_id in peers {
                        self.send_to_peer(&peer_id, Message::Have(index)).await;
                    }
                }
                PoolCommand::ChokePeer { peer_id } => {
                    self.send_to_peer(&peer_id, Message::Choke).await;
                }
                PoolCommand::UnchokePeer { peer_id } => {
                    self.send_to_peer(&peer_id, Message::Unchoke).await;
                }
                PoolCommand::SendInterested { peer_id } => {
                    self.send_to_peer(&peer_id, Message::Interested).await;
                }
                PoolCommand::DisconnectPeer { peer_id } => {
                    self.peer_txs.remove(&peer_id);
                }
                PoolCommand::SendPex {
                    peer_id,
                    added,
                    dropped,
                } => {
                    self.send_to_peer(&peer_id, Message::Pex { added, dropped })
                        .await;
                }
            }
        }
    }

    async fn send_to_peer(&self, peer_id: &PeerId, msg: Message) {
        if let Some(tx) = self.peer_txs.get(peer_id) {
            let _ = tx.send(PeerCommand::Send(msg)).await;
        }
    }

    async fn peer_loop(
        peer_id: PeerId,
        conn: &mut PeerConnection,
        event_tx: &mpsc::Sender<PoolEvent>,
        cmd_rx: &mut mpsc::Receiver<PeerCommand>,
    ) {
        loop {
            tokio::select! {
                msg_result = conn.recv() => {
                    match msg_result {
                        Ok(msg) => {
                            let event = match msg {
                                Message::Bitfield(bf) => Some(PoolEvent::BitfieldReceived {
                                    peer_id,
                                    bitfield: bf,
                                }),
                                Message::Piece { index, data, proof, .. } => {
                                    Some(PoolEvent::PieceReceived {
                                        peer_id,
                                        index,
                                        data,
                                        proof,
                                    })
                                }
                                Message::Have(index) => {
                                    Some(PoolEvent::HaveReceived { peer_id, index })
                                }
                                Message::Choke => Some(PoolEvent::Choked { peer_id }),
                                Message::Unchoke => Some(PoolEvent::Unchoked { peer_id }),
                                Message::Pex { added, dropped } => Some(PoolEvent::PexReceived { peer_id, added, dropped }),
                                _ => None,
                            };
                            if let Some(e) = event {
                                let _ = event_tx.send(e).await;
                            }
                        }
                        Err(PeerError::ConnectionClosed) => {
                            let _ = event_tx.send(PoolEvent::PeerDisconnected { peer_id }).await;
                            return;
                        }
                        Err(_) => {
                            let _ = event_tx.send(PoolEvent::PeerDisconnected { peer_id }).await;
                            return;
                        }
                    }
                }
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(PeerCommand::Send(msg)) => {
                            if conn.send(msg).await.is_err() {
                                let _ = event_tx.send(PoolEvent::PeerDisconnected { peer_id }).await;
                                return;
                            }
                        }
                        None => return,
                    }
                }
            }
        }
    }
}
