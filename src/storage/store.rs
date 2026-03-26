use std::io;
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

impl TorrentInfo {
    pub fn from_file(path: &Path) -> io::Result<(Self, Vec<u8>)> {
        let data = std::fs::read(path)?;
        let chunker = Chunker::new(256 * 1024);
        let pieces = chunker.chunk(&data);
        let leaves: Vec<[u8; 32]> = pieces.iter().map(|p| p.hash).collect();
        let tree = MerkleTree::from_leaves(&leaves);

        let info = Self {
            info_hash: tree.root(),
            piece_count: pieces.len() as u32,
            piece_size: chunker.piece_size(),
            total_size: data.len() as u64,
            tree,
            pieces,
        };

        Ok((info, data))
    }

    pub fn bitfield_bytes(&self) -> Vec<u8> {
        let count = self.piece_count as usize;
        let byte_count = count.div_ceil(8);
        let mut bytes = vec![0u8; byte_count];
        for i in 0..count {
            bytes[i / 8] |= 1 << (7 - (i % 8));
        }
        bytes
    }
}

pub struct PieceStore {
    data: Vec<u8>,
    piece_size: usize,
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

    pub fn write_piece(&mut self, index: u32, data: &[u8]) {
        let start = index as usize * self.piece_size;
        let end = start + data.len();
        self.data[start..end].copy_from_slice(data);
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
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn torrent_info_from_file() {
        let mut f = NamedTempFile::new().unwrap();
        let payload = vec![0xAB; 512 * 1024 + 100];
        f.write_all(&payload).unwrap();

        let (info, data) = TorrentInfo::from_file(f.path()).unwrap();

        assert_eq!(data.len(), payload.len());
        assert_eq!(info.total_size, payload.len() as u64);
        assert_eq!(info.piece_count, 3);
        assert_eq!(info.piece_size, 256 * 1024);
        assert_eq!(info.pieces.len(), 3);
        assert_eq!(info.info_hash, info.tree.root());
    }

    #[test]
    fn piece_store_roundtrip() {
        let data = vec![1u8; 1024];
        let store = PieceStore::from_data(data.clone(), 256);

        for i in 0..4 {
            let piece = store.read_piece(i).unwrap();
            assert_eq!(piece.len(), 256);
            assert!(piece.iter().all(|&b| b == 1));
        }
        assert!(store.read_piece(4).is_none());
    }

    #[test]
    fn piece_store_write_and_read() {
        let mut store = PieceStore::empty(8, 4);

        store.write_piece(0, &[1, 2, 3, 4]);
        store.write_piece(1, &[5, 6, 7, 8]);

        assert_eq!(store.data(), &[1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn verify_piece_detects_corruption() {
        let data = b"hello world";
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash: [u8; 32] = hasher.finalize().into();

        assert!(PieceStore::verify_piece(data, &hash));
        assert!(!PieceStore::verify_piece(b"wrong data", &hash));
    }

    #[test]
    fn bitfield_bytes_correct_bits() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(&vec![0u8; 256 * 1024 * 3]).unwrap();

        let (info, _) = TorrentInfo::from_file(f.path()).unwrap();
        assert_eq!(info.piece_count, 3);
        let bf = info.bitfield_bytes();

        assert_eq!(bf.len(), 1);
        assert_eq!(bf[0], 0b1110_0000); // 3 pieces = 3 high bits set
    }

    #[test]
    fn bitfield_bytes_full_byte() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(&vec![0u8; 256 * 1024 * 8]).unwrap();

        let (info, _) = TorrentInfo::from_file(f.path()).unwrap();
        assert_eq!(info.piece_count, 8);
        let bf = info.bitfield_bytes();

        assert_eq!(bf.len(), 1);
        assert_eq!(bf[0], 0xFF);
    }
}
