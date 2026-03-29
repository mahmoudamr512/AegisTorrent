use crate::core::scheduler::PeerId;
use crate::network::discovery::routing::{NodeId, NodeInfo};

#[derive(Debug, Clone)]
pub enum DhtMessage {
    Ping {
        sender: NodeId,
    },
    Pong {
        sender: NodeId,
    },
    FindNode {
        sender: NodeId,
        target: NodeId,
    },
    FindNodeResponse {
        sender: NodeId,
        nodes: Vec<NodeInfo>,
    },
    GetPeers {
        sender: NodeId,
        info_hash: [u8; 32],
    },
    GetPeersResponse {
        sender: NodeId,
        peers: Vec<PeerEntry>,
        nodes: Vec<NodeInfo>,
    },
    AnnouncePeer {
        sender: NodeId,
        info_hash: [u8; 32],
        peer_port: u16,
        score: f64,
    },
}

#[derive(Debug, Clone)]
pub struct PeerEntry {
    pub peer_id: PeerId,
    pub addr: String,
    pub score: f64,
}

impl DhtMessage {
    pub fn sender(&self) -> &NodeId {
        match self {
            Self::Ping { sender } | Self::Pong { sender } => sender,
            Self::FindNode { sender, .. } | Self::FindNodeResponse { sender, .. } => sender,
            Self::GetPeers { sender, .. } | Self::GetPeersResponse { sender, .. } => sender,
            Self::AnnouncePeer { sender, .. } => sender,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Self::Ping { sender } => {
                buf.push(0x00);
                buf.extend_from_slice(sender);
            }
            Self::Pong { sender } => {
                buf.push(0x01);
                buf.extend_from_slice(sender);
            }
            Self::FindNode { sender, target } => {
                buf.push(0x02);
                buf.extend_from_slice(sender);
                buf.extend_from_slice(target);
            }
            Self::FindNodeResponse { sender, nodes } => {
                buf.push(0x03);
                buf.extend_from_slice(sender);
                buf.push(nodes.len() as u8);
                for node in nodes {
                    buf.extend_from_slice(&node.id);
                    let addr_bytes = node.addr.as_bytes();
                    buf.push(addr_bytes.len() as u8);
                    buf.extend_from_slice(addr_bytes);
                }
            }
            Self::GetPeers { sender, info_hash } => {
                buf.push(0x04);
                buf.extend_from_slice(sender);
                buf.extend_from_slice(info_hash);
            }
            Self::GetPeersResponse {
                sender,
                peers,
                nodes,
            } => {
                buf.push(0x05);
                buf.extend_from_slice(sender);
                buf.push(peers.len() as u8);
                for peer in peers {
                    buf.extend_from_slice(&peer.peer_id);
                    let addr_bytes = peer.addr.as_bytes();
                    buf.push(addr_bytes.len() as u8);
                    buf.extend_from_slice(addr_bytes);
                    buf.extend_from_slice(&peer.score.to_be_bytes());
                }
                buf.push(nodes.len() as u8);
                for node in nodes {
                    buf.extend_from_slice(&node.id);
                    let addr_bytes = node.addr.as_bytes();
                    buf.push(addr_bytes.len() as u8);
                    buf.extend_from_slice(addr_bytes);
                }
            }
            Self::AnnouncePeer {
                sender,
                info_hash,
                peer_port,
                score,
            } => {
                buf.push(0x06);
                buf.extend_from_slice(sender);
                buf.extend_from_slice(info_hash);
                buf.extend_from_slice(&peer_port.to_be_bytes());
                buf.extend_from_slice(&score.to_be_bytes());
            }
        }
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let msg_type = data[0];
        let rest = &data[1..];

        match msg_type {
            0x00 => {
                if rest.len() < 20 {
                    return None;
                }
                let mut sender = [0u8; 20];
                sender.copy_from_slice(&rest[..20]);
                Some(Self::Ping { sender })
            }
            0x01 => {
                if rest.len() < 20 {
                    return None;
                }
                let mut sender = [0u8; 20];
                sender.copy_from_slice(&rest[..20]);
                Some(Self::Pong { sender })
            }
            0x02 => {
                if rest.len() < 40 {
                    return None;
                }
                let mut sender = [0u8; 20];
                sender.copy_from_slice(&rest[..20]);
                let mut target = [0u8; 20];
                target.copy_from_slice(&rest[20..40]);
                Some(Self::FindNode { sender, target })
            }
            0x03 => {
                if rest.len() < 21 {
                    return None;
                }
                let mut sender = [0u8; 20];
                sender.copy_from_slice(&rest[..20]);
                let count = rest[20] as usize;
                let mut offset = 21;
                let mut nodes = Vec::new();
                for _ in 0..count {
                    if offset + 21 > rest.len() {
                        return None;
                    }
                    let mut id = [0u8; 20];
                    id.copy_from_slice(&rest[offset..offset + 20]);
                    let addr_len = rest[offset + 20] as usize;
                    offset += 21;
                    if offset + addr_len > rest.len() {
                        return None;
                    }
                    let addr =
                        String::from_utf8_lossy(&rest[offset..offset + addr_len]).to_string();
                    offset += addr_len;
                    nodes.push(NodeInfo { id, addr });
                }
                Some(Self::FindNodeResponse { sender, nodes })
            }
            0x04 => {
                if rest.len() < 52 {
                    return None;
                }
                let mut sender = [0u8; 20];
                sender.copy_from_slice(&rest[..20]);
                let mut info_hash = [0u8; 32];
                info_hash.copy_from_slice(&rest[20..52]);
                Some(Self::GetPeers { sender, info_hash })
            }
            0x05 => {
                if rest.len() < 21 {
                    return None;
                }
                let mut sender = [0u8; 20];
                sender.copy_from_slice(&rest[..20]);
                let peer_count = rest[20] as usize;
                let mut offset = 21;
                let mut peers = Vec::new();
                for _ in 0..peer_count {
                    if offset + 21 > rest.len() {
                        return None;
                    }
                    let mut peer_id = [0u8; 20];
                    peer_id.copy_from_slice(&rest[offset..offset + 20]);
                    let addr_len = rest[offset + 20] as usize;
                    offset += 21;
                    if offset + addr_len + 8 > rest.len() {
                        return None;
                    }
                    let addr =
                        String::from_utf8_lossy(&rest[offset..offset + addr_len]).to_string();
                    offset += addr_len;
                    let score = f64::from_be_bytes(rest[offset..offset + 8].try_into().ok()?);
                    offset += 8;
                    peers.push(PeerEntry {
                        peer_id,
                        addr,
                        score,
                    });
                }
                if offset >= rest.len() {
                    return None;
                }
                let node_count = rest[offset] as usize;
                offset += 1;
                let mut nodes = Vec::new();
                for _ in 0..node_count {
                    if offset + 21 > rest.len() {
                        return None;
                    }
                    let mut id = [0u8; 20];
                    id.copy_from_slice(&rest[offset..offset + 20]);
                    let addr_len = rest[offset + 20] as usize;
                    offset += 21;
                    if offset + addr_len > rest.len() {
                        return None;
                    }
                    let addr =
                        String::from_utf8_lossy(&rest[offset..offset + addr_len]).to_string();
                    offset += addr_len;
                    nodes.push(NodeInfo { id, addr });
                }
                Some(Self::GetPeersResponse {
                    sender,
                    peers,
                    nodes,
                })
            }
            0x06 => {
                if rest.len() < 62 {
                    return None;
                }
                let mut sender = [0u8; 20];
                sender.copy_from_slice(&rest[..20]);
                let mut info_hash = [0u8; 32];
                info_hash.copy_from_slice(&rest[20..52]);
                let peer_port = u16::from_be_bytes([rest[52], rest[53]]);
                let score = f64::from_be_bytes(rest[54..62].try_into().ok()?);
                Some(Self::AnnouncePeer {
                    sender,
                    info_hash,
                    peer_port,
                    score,
                })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_id(n: u8) -> NodeId {
        let mut id = [0u8; 20];
        id[0] = n;
        id
    }

    #[test]
    fn roundtrip_ping() {
        let msg = DhtMessage::Ping { sender: test_id(1) };
        let decoded = DhtMessage::decode(&msg.encode()).unwrap();
        assert_eq!(*decoded.sender(), test_id(1));
    }

    #[test]
    fn roundtrip_find_node() {
        let msg = DhtMessage::FindNode {
            sender: test_id(1),
            target: test_id(42),
        };
        let decoded = DhtMessage::decode(&msg.encode()).unwrap();
        if let DhtMessage::FindNode { sender, target } = decoded {
            assert_eq!(sender, test_id(1));
            assert_eq!(target, test_id(42));
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn roundtrip_find_node_response() {
        let nodes = vec![
            NodeInfo {
                id: test_id(2),
                addr: "127.0.0.1:6001".into(),
            },
            NodeInfo {
                id: test_id(3),
                addr: "127.0.0.1:6002".into(),
            },
        ];
        let msg = DhtMessage::FindNodeResponse {
            sender: test_id(1),
            nodes,
        };
        let decoded = DhtMessage::decode(&msg.encode()).unwrap();
        if let DhtMessage::FindNodeResponse { nodes, .. } = decoded {
            assert_eq!(nodes.len(), 2);
            assert_eq!(nodes[0].id, test_id(2));
            assert_eq!(nodes[1].addr, "127.0.0.1:6002");
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn roundtrip_get_peers_response() {
        let peers = vec![PeerEntry {
            peer_id: test_id(5),
            addr: "127.0.0.1:7000".into(),
            score: 0.85,
        }];
        let msg = DhtMessage::GetPeersResponse {
            sender: test_id(1),
            peers,
            nodes: vec![],
        };
        let decoded = DhtMessage::decode(&msg.encode()).unwrap();
        if let DhtMessage::GetPeersResponse { peers, nodes, .. } = decoded {
            assert_eq!(peers.len(), 1);
            assert!((peers[0].score - 0.85).abs() < 1e-9);
            assert!(nodes.is_empty());
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn roundtrip_announce_peer() {
        let msg = DhtMessage::AnnouncePeer {
            sender: test_id(1),
            info_hash: [0xAB; 32],
            peer_port: 6881,
            score: 0.72,
        };
        let decoded = DhtMessage::decode(&msg.encode()).unwrap();
        if let DhtMessage::AnnouncePeer {
            info_hash,
            peer_port,
            score,
            ..
        } = decoded
        {
            assert_eq!(info_hash, [0xAB; 32]);
            assert_eq!(peer_port, 6881);
            assert!((score - 0.72).abs() < 1e-9);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn decode_empty_returns_none() {
        assert!(DhtMessage::decode(&[]).is_none());
    }

    #[test]
    fn decode_unknown_type_returns_none() {
        assert!(DhtMessage::decode(&[0xFF]).is_none());
    }
}
