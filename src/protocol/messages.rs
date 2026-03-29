use bytes::Bytes;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct PexPeer {
    pub peer_id: [u8; 20],
    pub addr: String,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    Handshake {
        version: u8,
        info_hash: [u8; 32],
        peer_id: [u8; 20],
    },
    Bitfield(Bytes),
    Request {
        index: u32,
        offset: u32,
        length: u32,
    },
    Piece {
        index: u32,
        offset: u32,
        proof: Vec<(bool, [u8; 32])>,
        data: Bytes,
    },
    Have(u32),
    Cancel {
        index: u32,
        offset: u32,
        length: u32,
    },
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Pex {
        added: Vec<PexPeer>,
        dropped: Vec<[u8; 20]>,
    },
}

#[derive(Debug, Error)]
pub enum MessageError {
    #[error("unknown message type: {0:#x}")]
    UnknownType(u8),
    #[error("buffer too short: need {need}, have {have}")]
    BufferTooShort { need: usize, have: usize },
    #[error("payload too large: {0} bytes")]
    PayloadTooLarge(usize),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl Message {
    pub fn message_type(&self) -> u8 {
        match self {
            Self::Handshake { .. } => 0x00,
            Self::Bitfield(_) => 0x01,
            Self::Request { .. } => 0x02,
            Self::Piece { .. } => 0x03,
            Self::Have(_) => 0x04,
            Self::Cancel { .. } => 0x05,
            Self::Choke => 0x06,
            Self::Unchoke => 0x07,
            Self::Interested => 0x08,
            Self::NotInterested => 0x09,
            Self::Pex { .. } => 0x0A,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_type_codes() {
        let handshake = Message::Handshake {
            version: 1,
            info_hash: [0u8; 32],
            peer_id: [0u8; 20],
        };
        assert_eq!(handshake.message_type(), 0x00);
        assert_eq!(Message::Bitfield(Bytes::new()).message_type(), 0x01);
        assert_eq!(
            Message::Request {
                index: 0,
                offset: 0,
                length: 0
            }
            .message_type(),
            0x02
        );
        assert_eq!(
            Message::Piece {
                index: 0,
                offset: 0,
                proof: vec![],
                data: Bytes::new()
            }
            .message_type(),
            0x03
        );
        assert_eq!(Message::Have(0).message_type(), 0x04);
        assert_eq!(
            Message::Cancel {
                index: 0,
                offset: 0,
                length: 0
            }
            .message_type(),
            0x05
        );
        assert_eq!(Message::Choke.message_type(), 0x06);
        assert_eq!(Message::Unchoke.message_type(), 0x07);
        assert_eq!(Message::Interested.message_type(), 0x08);
        assert_eq!(Message::NotInterested.message_type(), 0x09);
    }

    #[test]
    fn piece_with_proof_field() {
        let proof = vec![(true, [0xAA; 32]), (false, [0xBB; 32])];
        let msg = Message::Piece {
            index: 42,
            offset: 1024,
            proof: proof.clone(),
            data: Bytes::from_static(b"hello"),
        };
        if let Message::Piece {
            index,
            offset,
            proof: p,
            data,
        } = msg
        {
            assert_eq!(index, 42);
            assert_eq!(offset, 1024);
            assert_eq!(p, proof);
            assert_eq!(data, &b"hello"[..]);
        } else {
            panic!("expected Piece variant");
        }
    }

    #[test]
    fn message_error_io_variant() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken");
        let msg_err: MessageError = io_err.into();
        assert!(matches!(msg_err, MessageError::Io(_)));
        assert!(msg_err.to_string().contains("broken"));
    }

    #[test]
    fn message_clone_and_eq() {
        let msg = Message::Request {
            index: 1,
            offset: 2,
            length: 3,
        };
        let cloned = msg.clone();
        assert_eq!(msg, cloned);
    }
}
