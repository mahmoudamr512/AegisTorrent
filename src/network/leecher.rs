use sha2::{Digest, Sha256};
use tokio::net::TcpStream;

use crate::core::merkle::{MerkleProof, MerkleTree};
use crate::network::peer::PeerConnection;
use crate::protocol::messages::Message;

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
        let mut conn = PeerConnection::new(stream);
        conn.handshake(self.info_hash, self.our_peer_id).await?;

        let bitfield = match conn.recv().await? {
            Message::Bitfield(bf) => bf,
            other => return Err(format!("expected Bitfield, got type {:#x}", other.message_type()).into()),
        };

        let piece_count = bitfield.len() * 8;
        let our_bitfield = vec![0u8; bitfield.len()];
        conn.send(Message::Bitfield(our_bitfield.into())).await?;
        conn.send(Message::Interested).await?;

        match conn.recv().await? {
            Message::Unchoke => {}
            other => return Err(format!("expected Unchoke, got type {:#x}", other.message_type()).into()),
        }

        let mut pieces: Vec<(u32, Vec<u8>)> = Vec::with_capacity(piece_count);

        for i in 0..piece_count as u32 {
            conn.send(Message::Request {
                index: i,
                offset: 0,
                length: 0,
            })
            .await?;

            let (index, data, proof) = match conn.recv().await? {
                Message::Piece { index, data, proof, .. } => (index, data, proof),
                other => return Err(format!("expected Piece, got type {:#x}", other.message_type()).into()),
            };

            let mut hasher = Sha256::new();
            hasher.update(&data);
            let leaf_hash: [u8; 32] = hasher.finalize().into();

            let merkle_proof = MerkleProof {
                leaf_index: index as usize,
                siblings: proof,
            };

            if !MerkleTree::verify(&self.info_hash, &leaf_hash, &merkle_proof) {
                return Err(format!("piece {} failed merkle verification", index).into());
            }

            println!("Piece {index}/{piece_count} verified");

            conn.send(Message::Have(index)).await?;
            pieces.push((index, data.to_vec()));
        }

        pieces.sort_by_key(|(idx, _)| *idx);
        let mut output = Vec::new();
        for (_, data) in &pieces {
            output.extend_from_slice(data);
        }

        tokio::fs::write(&self.output_path, &output).await?;
        println!("Download complete: {}", self.output_path);

        Ok(DownloadResult {
            bytes_downloaded: output.len() as u64,
            pieces_verified: piece_count as u32,
        })
    }
}
