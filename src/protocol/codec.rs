use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use super::messages::{Message, MessageError};

const MAX_PAYLOAD: usize = 4 * 1024 * 1024;
const HEADER_LEN: usize = 4;

pub struct LengthPrefixCodec;

impl Encoder<Message> for LengthPrefixCodec {
    type Error = MessageError;

    fn encode(&mut self, msg: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let msg_type = msg.message_type();

        match msg {
            Message::Handshake {
                version,
                info_hash,
                peer_id,
            } => {
                let payload_len = 1 + 32 + 20;
                dst.put_u32(1 + payload_len as u32);
                dst.put_u8(msg_type);
                dst.put_u8(version);
                dst.put_slice(&info_hash);
                dst.put_slice(&peer_id);
            }
            Message::Bitfield(bits) => {
                dst.put_u32(1 + bits.len() as u32);
                dst.put_u8(msg_type);
                dst.put_slice(&bits);
            }
            Message::Request {
                index,
                offset,
                length,
            }
            | Message::Cancel {
                index,
                offset,
                length,
            } => {
                dst.put_u32(1 + 12);
                dst.put_u8(msg_type);
                dst.put_u32(index);
                dst.put_u32(offset);
                dst.put_u32(length);
            }
            Message::Piece {
                index,
                offset,
                proof,
                data,
            } => {
                let proof_bytes = 2 + proof.len() * 33;
                let payload_len = 4 + 4 + proof_bytes + data.len();
                dst.put_u32(1 + payload_len as u32);
                dst.put_u8(msg_type);
                dst.put_u32(index);
                dst.put_u32(offset);
                dst.put_u16(proof.len() as u16);
                for (is_left, hash) in &proof {
                    dst.put_u8(u8::from(*is_left));
                    dst.put_slice(hash);
                }
                dst.put_slice(&data);
            }
            Message::Have(index) => {
                dst.put_u32(1 + 4);
                dst.put_u8(msg_type);
                dst.put_u32(index);
            }
            Message::Choke | Message::Unchoke | Message::Interested | Message::NotInterested => {
                dst.put_u32(1);
                dst.put_u8(msg_type);
            }
        }

        Ok(())
    }
}

impl Decoder for LengthPrefixCodec {
    type Item = Message;
    type Error = MessageError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < HEADER_LEN {
            return Ok(None);
        }

        let length = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        if length == 0 {
            src.advance(HEADER_LEN);
            return Err(MessageError::BufferTooShort { need: 1, have: 0 });
        }

        let payload_len = length - 1;
        if payload_len > MAX_PAYLOAD {
            return Err(MessageError::PayloadTooLarge(payload_len));
        }

        let frame_len = HEADER_LEN + length;
        if src.len() < frame_len {
            src.reserve(frame_len - src.len());
            return Ok(None);
        }

        src.advance(HEADER_LEN);
        let msg_type = src[0];
        src.advance(1);

        let mut payload = src.split_to(payload_len);

        match msg_type {
            0x00 => {
                if payload.len() < 53 {
                    return Err(MessageError::BufferTooShort {
                        need: 53,
                        have: payload.len(),
                    });
                }
                let version = payload.get_u8();
                let mut info_hash = [0u8; 32];
                payload.copy_to_slice(&mut info_hash);
                let mut peer_id = [0u8; 20];
                payload.copy_to_slice(&mut peer_id);
                Ok(Some(Message::Handshake {
                    version,
                    info_hash,
                    peer_id,
                }))
            }
            0x01 => Ok(Some(Message::Bitfield(payload.freeze()))),
            0x02 => {
                if payload.len() < 12 {
                    return Err(MessageError::BufferTooShort {
                        need: 12,
                        have: payload.len(),
                    });
                }
                let index = payload.get_u32();
                let offset = payload.get_u32();
                let length = payload.get_u32();
                Ok(Some(Message::Request {
                    index,
                    offset,
                    length,
                }))
            }
            0x03 => {
                if payload.len() < 10 {
                    return Err(MessageError::BufferTooShort {
                        need: 10,
                        have: payload.len(),
                    });
                }
                let index = payload.get_u32();
                let offset = payload.get_u32();
                let proof_len = payload.get_u16() as usize;
                let proof_bytes = proof_len * 33;
                if payload.len() < proof_bytes {
                    return Err(MessageError::BufferTooShort {
                        need: proof_bytes,
                        have: payload.len(),
                    });
                }
                let mut proof = Vec::with_capacity(proof_len);
                for _ in 0..proof_len {
                    let is_left = payload.get_u8() != 0;
                    let mut hash = [0u8; 32];
                    payload.copy_to_slice(&mut hash);
                    proof.push((is_left, hash));
                }
                let data = payload.freeze();
                Ok(Some(Message::Piece {
                    index,
                    offset,
                    proof,
                    data,
                }))
            }
            0x04 => {
                if payload.len() < 4 {
                    return Err(MessageError::BufferTooShort {
                        need: 4,
                        have: payload.len(),
                    });
                }
                let index = payload.get_u32();
                Ok(Some(Message::Have(index)))
            }
            0x05 => {
                if payload.len() < 12 {
                    return Err(MessageError::BufferTooShort {
                        need: 12,
                        have: payload.len(),
                    });
                }
                let index = payload.get_u32();
                let offset = payload.get_u32();
                let length = payload.get_u32();
                Ok(Some(Message::Cancel {
                    index,
                    offset,
                    length,
                }))
            }
            0x06 => Ok(Some(Message::Choke)),
            0x07 => Ok(Some(Message::Unchoke)),
            0x08 => Ok(Some(Message::Interested)),
            0x09 => Ok(Some(Message::NotInterested)),
            other => Err(MessageError::UnknownType(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn roundtrip(msg: Message) -> Message {
        let mut codec = LengthPrefixCodec;
        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).unwrap();
        codec.decode(&mut buf).unwrap().unwrap()
    }

    #[test]
    fn roundtrip_handshake() {
        let msg = Message::Handshake {
            version: 1,
            info_hash: [0xAB; 32],
            peer_id: [0xCD; 20],
        };
        assert_eq!(roundtrip(msg.clone()), msg);
    }

    #[test]
    fn roundtrip_bitfield() {
        let msg = Message::Bitfield(Bytes::from_static(&[0xFF, 0x00, 0xAA]));
        assert_eq!(roundtrip(msg.clone()), msg);
    }

    #[test]
    fn roundtrip_request() {
        let msg = Message::Request {
            index: 7,
            offset: 16384,
            length: 32768,
        };
        assert_eq!(roundtrip(msg.clone()), msg);
    }

    #[test]
    fn roundtrip_piece_with_proof() {
        let msg = Message::Piece {
            index: 3,
            offset: 0,
            proof: vec![(true, [0x11; 32]), (false, [0x22; 32])],
            data: Bytes::from_static(b"piece data here"),
        };
        assert_eq!(roundtrip(msg.clone()), msg);
    }

    #[test]
    fn roundtrip_have() {
        let msg = Message::Have(42);
        assert_eq!(roundtrip(msg.clone()), msg);
    }

    #[test]
    fn roundtrip_cancel() {
        let msg = Message::Cancel {
            index: 1,
            offset: 0,
            length: 16384,
        };
        assert_eq!(roundtrip(msg.clone()), msg);
    }

    #[test]
    fn roundtrip_simple_messages() {
        for msg in [
            Message::Choke,
            Message::Unchoke,
            Message::Interested,
            Message::NotInterested,
        ] {
            assert_eq!(roundtrip(msg.clone()), msg);
        }
    }

    #[test]
    fn incomplete_frame_returns_none() {
        let mut codec = LengthPrefixCodec;
        let mut buf = BytesMut::new();
        buf.put_u32(10);
        buf.put_u8(0x06);
        // Only 5 bytes, but frame needs 4 + 10 = 14
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn unknown_type_returns_error() {
        let mut codec = LengthPrefixCodec;
        let mut buf = BytesMut::new();
        buf.put_u32(1);
        buf.put_u8(0xFF);
        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn rejects_oversized_payload() {
        let mut codec = LengthPrefixCodec;
        let mut buf = BytesMut::new();
        buf.put_u32(1 + MAX_PAYLOAD as u32 + 1);
        buf.put_u8(0x01);
        let result = codec.decode(&mut buf);
        assert!(matches!(result, Err(MessageError::PayloadTooLarge(_))));
    }
}
