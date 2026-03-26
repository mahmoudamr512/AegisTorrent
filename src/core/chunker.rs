use sha2::{Digest, Sha256};

const DEFAULT_PIECE_SIZE: usize = 256 * 1024;

pub struct Chunker {
    piece_size: usize,
}

#[derive(Debug, Clone)]
pub struct Piece {
    pub index: u32,
    pub hash: [u8; 32],
    pub length: usize,
}

impl Chunker {
    pub fn new(piece_size: usize) -> Self {
        assert!(
            piece_size.is_power_of_two(),
            "piece size must be a power of 2"
        );
        Self { piece_size }
    }

    pub fn with_default() -> Self {
        Self::new(DEFAULT_PIECE_SIZE)
    }

    pub fn chunk(&self, data: &[u8]) -> Vec<Piece> {
        data.chunks(self.piece_size)
            .enumerate()
            .map(|(i, chunk)| {
                let mut hasher = Sha256::new();
                hasher.update(chunk);
                let hash: [u8; 32] = hasher.finalize().into();
                Piece {
                    index: i as u32,
                    hash,
                    length: chunk.len(),
                }
            })
            .collect()
    }

    pub fn piece_size(&self) -> usize {
        self.piece_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_data_into_pieces() {
        let chunker = Chunker::new(4);
        let data = b"helloworldtest";
        let pieces = chunker.chunk(data);

        assert_eq!(pieces.len(), 4);
        assert_eq!(pieces[0].index, 0);
        assert_eq!(pieces[0].length, 4);
        assert_eq!(pieces[3].length, 2);
    }

    #[test]
    fn consistent_hashing() {
        let chunker = Chunker::new(8);
        let data = b"repeatable";
        let a = chunker.chunk(data);
        let b = chunker.chunk(data);
        assert_eq!(a[0].hash, b[0].hash);
    }

    #[test]
    #[should_panic(expected = "power of 2")]
    fn rejects_non_power_of_two() {
        Chunker::new(100);
    }

    #[test]
    fn empty_data_yields_no_pieces() {
        let chunker = Chunker::new(4);
        let pieces = chunker.chunk(b"");
        assert!(pieces.is_empty());
    }
}
