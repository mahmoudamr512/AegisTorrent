use std::io;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use tokio::net::TcpListener;

use crate::core::merkle::MerkleTree;
use crate::network::peer::PeerConnection;
use crate::protocol::messages::Message;
use crate::storage::store::{PieceStore, TorrentInfo};

pub struct Seeder {
    info: TorrentInfo,
    store: PieceStore,
    our_peer_id: [u8; 20],
}

impl Seeder {
    pub fn from_file(path: &Path, peer_id: [u8; 20]) -> io::Result<Self> {
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
        self.listen_on(listener).await
    }

    pub async fn listen_on(
        &self,
        listener: TcpListener,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let pieces: Vec<Vec<u8>> = (0..self.info.piece_count)
            .map(|i| self.store.read_piece(i).unwrap().to_vec())
            .collect();
        let pieces = Arc::new(pieces);
        let tree = Arc::new(self.info.tree.clone());
        let info_hash = self.info.info_hash;
        let peer_id = self.our_peer_id;
        let piece_count = self.info.piece_count;
        let bitfield = Bytes::from(self.info.bitfield_bytes());

        loop {
            let (stream, addr) = listener.accept().await?;
            println!("Accepted connection from {addr}");

            let pieces = Arc::clone(&pieces);
            let tree = Arc::clone(&tree);
            let bitfield = bitfield.clone();

            tokio::spawn(async move {
                if let Err(e) =
                    handle_peer(stream, info_hash, peer_id, piece_count, bitfield, pieces, tree)
                        .await
                {
                    eprintln!("Peer {addr} error: {e}");
                }
            });
        }
    }
}

async fn handle_peer(
    stream: tokio::net::TcpStream,
    info_hash: [u8; 32],
    peer_id: [u8; 20],
    piece_count: u32,
    bitfield: Bytes,
    pieces: Arc<Vec<Vec<u8>>>,
    tree: Arc<MerkleTree>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut conn = PeerConnection::new(stream);
    conn.accept_handshake(info_hash, peer_id).await?;

    conn.send(Message::Bitfield(bitfield)).await?;

    let _their_bitfield = conn.recv().await?;

    loop {
        let msg = conn.recv().await?;
        match msg {
            Message::Interested => break,
            _ => continue,
        }
    }

    conn.send(Message::Unchoke).await?;

    loop {
        let msg = conn.recv().await?;
        match msg {
            Message::Request {
                index,
                offset,
                length: _,
            } => {
                let data = &pieces[index as usize];
                let proof = tree.proof(index as usize);
                conn.send(Message::Piece {
                    index,
                    offset,
                    proof: proof.siblings,
                    data: Bytes::copy_from_slice(data),
                })
                .await?;
                println!("Sent piece {}/{piece_count}", index + 1);
            }
            Message::Have(_) => {}
            Message::NotInterested => break,
            _ => {}
        }
    }

    Ok(())
}
