use bytes::Bytes;
use thiserror::Error;

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
}

#[derive(Debug, Error)]
pub enum MessageError {
    #[error("unknown message type: {0:#x}")]
    UnknownType(u8),
    #[error("buffer too short: need {need}, have {have}")]
    BufferTooShort { need: usize, have: usize },
    #[error("payload too large: {0} bytes")]
    PayloadTooLarge(usize),
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
        }
    }
}
