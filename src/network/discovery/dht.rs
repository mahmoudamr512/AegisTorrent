use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use crate::network::discovery::lookup::LookupState;
use crate::network::discovery::routing::{InsertResult, NodeId, NodeInfo, RoutingTable};
use crate::network::discovery::rpc::{DhtMessage, PeerEntry};
use crate::network::discovery::store::PeerStore;

const REFRESH_INTERVAL_SECS: u64 = 900;
const QUERY_TIMEOUT_MS: u64 = 500;

pub struct DhtConfig {
    pub node_id: NodeId,
    pub listen_addr: String,
    pub bootstrap_addrs: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum DhtEvent {
    PeersFound {
        info_hash: [u8; 32],
        peers: Vec<PeerEntry>,
    },
}

pub struct DhtNode {
    config: DhtConfig,
    routing_table: RoutingTable,
    peer_store: PeerStore,
    event_tx: mpsc::Sender<DhtEvent>,
}

impl DhtNode {
    pub fn new(config: DhtConfig, event_tx: mpsc::Sender<DhtEvent>) -> Self {
        let routing_table = RoutingTable::new(config.node_id);
        Self {
            config,
            routing_table,
            peer_store: PeerStore::new(),
            event_tx,
        }
    }

    pub async fn run(mut self) {
        let socket = match UdpSocket::bind(&self.config.listen_addr).await {
            Ok(s) => Arc::new(s),
            Err(e) => {
                eprintln!("DHT failed to bind to {}: {e}", self.config.listen_addr);
                return;
            }
        };

        for addr in &self.config.bootstrap_addrs {
            let msg = DhtMessage::FindNode {
                sender: self.config.node_id,
                target: self.config.node_id,
            };
            if let Ok(parsed) = addr.parse::<SocketAddr>() {
                let _ = socket.send_to(&msg.encode(), parsed).await;
            }
        }

        let mut buf = [0u8; 65535];
        let mut refresh =
            tokio::time::interval(std::time::Duration::from_secs(REFRESH_INTERVAL_SECS));
        let mut cleanup =
            tokio::time::interval(std::time::Duration::from_secs(REFRESH_INTERVAL_SECS));

        loop {
            tokio::select! {
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((len, src)) => {
                            if let Some(msg) = DhtMessage::decode(&buf[..len]) {
                                self.handle_message(msg, src, &socket).await;
                            }
                        }
                        Err(_) => continue,
                    }
                }
                _ = refresh.tick() => {
                    let closest = self.routing_table.find_closest(&self.config.node_id, 5);
                    let msg = DhtMessage::FindNode {
                        sender: self.config.node_id,
                        target: self.config.node_id,
                    };
                    let encoded = msg.encode();
                    for node in closest {
                        if let Ok(addr) = node.addr.parse::<SocketAddr>() {
                            let _ = socket.send_to(&encoded, addr).await;
                        }
                    }
                }
                _ = cleanup.tick() => {
                    self.peer_store.remove_stale();
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: DhtMessage, src: SocketAddr, socket: &UdpSocket) {
        let sender_id = *msg.sender();
        let sender_node = NodeInfo {
            id: sender_id,
            addr: src.to_string(),
        };

        if let InsertResult::BucketFull { oldest } = self.routing_table.insert(sender_node.clone())
        {
            let ping = DhtMessage::Ping {
                sender: self.config.node_id,
            };
            if let Ok(addr) = oldest.addr.parse::<SocketAddr>() {
                let _ = socket.send_to(&ping.encode(), addr).await;
            }
        }

        match msg {
            DhtMessage::Ping { .. } => {
                let pong = DhtMessage::Pong {
                    sender: self.config.node_id,
                };
                let _ = socket.send_to(&pong.encode(), src).await;
            }
            DhtMessage::Pong { .. } => {}
            DhtMessage::FindNode { target, .. } => {
                let closest = self.routing_table.find_closest(&target, 8);
                let response = DhtMessage::FindNodeResponse {
                    sender: self.config.node_id,
                    nodes: closest,
                };
                let _ = socket.send_to(&response.encode(), src).await;
            }
            DhtMessage::FindNodeResponse { nodes, .. } => {
                for node in nodes {
                    self.routing_table.insert(node);
                }
            }
            DhtMessage::GetPeers { info_hash, .. } => {
                let peers = self.peer_store.get_peers(&info_hash);
                if peers.is_empty() {
                    let target_id: NodeId = info_hash[..20].try_into().unwrap_or([0u8; 20]);
                    let closest = self.routing_table.find_closest(&target_id, 8);
                    let response = DhtMessage::GetPeersResponse {
                        sender: self.config.node_id,
                        peers: vec![],
                        nodes: closest,
                    };
                    let _ = socket.send_to(&response.encode(), src).await;
                } else {
                    let peer_entries: Vec<PeerEntry> = peers
                        .iter()
                        .map(|p| PeerEntry {
                            peer_id: p.peer_id,
                            addr: p.addr.clone(),
                            score: p.score,
                        })
                        .collect();
                    let response = DhtMessage::GetPeersResponse {
                        sender: self.config.node_id,
                        peers: peer_entries,
                        nodes: vec![],
                    };
                    let _ = socket.send_to(&response.encode(), src).await;
                }
            }
            DhtMessage::GetPeersResponse { peers, nodes, .. } => {
                for node in nodes {
                    self.routing_table.insert(node);
                }
                if !peers.is_empty() {
                    let _ = self
                        .event_tx
                        .send(DhtEvent::PeersFound {
                            info_hash: [0u8; 32],
                            peers,
                        })
                        .await;
                }
            }
            DhtMessage::AnnouncePeer {
                sender,
                info_hash,
                peer_port,
                score,
                ..
            } => {
                let addr = format!("{}:{}", src.ip(), peer_port);
                self.peer_store.announce(info_hash, sender, addr, score);
            }
        }
    }

    pub fn node_count(&self) -> usize {
        self.routing_table.node_count()
    }

    pub async fn find_peers(
        socket: &UdpSocket,
        our_id: NodeId,
        info_hash: [u8; 32],
        initial_nodes: Vec<NodeInfo>,
        event_tx: mpsc::Sender<DhtEvent>,
    ) {
        let target: NodeId = info_hash[..20].try_into().unwrap_or([0u8; 20]);
        let mut state = LookupState::new(target, initial_nodes);

        while !state.should_terminate() {
            let batch = state.next_batch();
            if batch.is_empty() {
                break;
            }

            let mut pending_count = 0;
            for node in &batch {
                let msg = DhtMessage::GetPeers {
                    sender: our_id,
                    info_hash,
                };
                if let Ok(addr) = node.addr.parse::<SocketAddr>() {
                    let _ = socket.send_to(&msg.encode(), addr).await;
                    pending_count += 1;
                }
            }

            let timeout = tokio::time::sleep(std::time::Duration::from_millis(QUERY_TIMEOUT_MS));
            tokio::pin!(timeout);
            let mut responses = 0;
            let mut buf = [0u8; 65535];

            loop {
                tokio::select! {
                    result = socket.recv_from(&mut buf) => {
                        if let Ok((len, _)) = result {
                            if let Some(DhtMessage::GetPeersResponse { peers, nodes, .. }) =
                                DhtMessage::decode(&buf[..len])
                            {
                                state.add_peers(peers);
                                state.add_nodes(nodes);
                                responses += 1;
                                if responses >= pending_count { break; }
                            }
                        }
                    }
                    _ = &mut timeout => { break; }
                }
            }
        }

        let results = state.results();
        if !results.is_empty() {
            let _ = event_tx
                .send(DhtEvent::PeersFound {
                    info_hash,
                    peers: results,
                })
                .await;
        }
    }
}
