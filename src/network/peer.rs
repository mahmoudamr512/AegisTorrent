use futures_util::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::protocol::codec::LengthPrefixCodec;
use crate::protocol::messages::{Message, MessageError};
use crate::protocol::state::{ConnectionState, StateEvent, TransitionError};

#[derive(Debug, Error)]
pub enum PeerError {
    #[error(transparent)]
    Protocol(#[from] MessageError),
    #[error("invalid state transition from {from:?} on {event:?}")]
    InvalidTransition {
        from: ConnectionState,
        event: StateEvent,
    },
    #[error("connection closed")]
    ConnectionClosed,
    #[error("info hash mismatch")]
    InfoHashMismatch,
}

impl From<TransitionError> for PeerError {
    fn from(err: TransitionError) -> Self {
        match err {
            TransitionError::Invalid { from, event } => {
                PeerError::InvalidTransition { from, event }
            }
        }
    }
}

pub struct PeerConnection {
    framed: Framed<TcpStream, LengthPrefixCodec>,
    state: ConnectionState,
    remote_peer_id: Option<[u8; 20]>,
}

impl PeerConnection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            framed: Framed::new(stream, LengthPrefixCodec),
            state: ConnectionState::Handshaking,
            remote_peer_id: None,
        }
    }

    pub async fn send(&mut self, msg: Message) -> Result<(), PeerError> {
        self.framed.send(msg).await?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Message, PeerError> {
        self.framed
            .next()
            .await
            .ok_or(PeerError::ConnectionClosed)?
            .map_err(PeerError::from)
    }

    pub async fn handshake(
        &mut self,
        info_hash: [u8; 32],
        peer_id: [u8; 20],
    ) -> Result<[u8; 20], PeerError> {
        self.send(Message::Handshake {
            version: 1,
            info_hash,
            peer_id,
        })
        .await?;

        let msg = self.recv().await?;
        match msg {
            Message::Handshake {
                info_hash: remote_hash,
                peer_id: remote_id,
                ..
            } => {
                if remote_hash != info_hash {
                    return Err(PeerError::InfoHashMismatch);
                }
                self.remote_peer_id = Some(remote_id);
                self.state = self.state.transition(StateEvent::HandshakeComplete)?;
                Ok(remote_id)
            }
            _ => Err(PeerError::ConnectionClosed),
        }
    }

    pub async fn accept_handshake(
        &mut self,
        info_hash: [u8; 32],
        peer_id: [u8; 20],
    ) -> Result<[u8; 20], PeerError> {
        let msg = self.recv().await?;
        match msg {
            Message::Handshake {
                info_hash: remote_hash,
                peer_id: remote_id,
                ..
            } => {
                if remote_hash != info_hash {
                    return Err(PeerError::InfoHashMismatch);
                }
                self.send(Message::Handshake {
                    version: 1,
                    info_hash,
                    peer_id,
                })
                .await?;
                self.remote_peer_id = Some(remote_id);
                self.state = self.state.transition(StateEvent::HandshakeComplete)?;
                Ok(remote_id)
            }
            _ => Err(PeerError::ConnectionClosed),
        }
    }

    pub fn state(&self) -> ConnectionState {
        self.state
    }

    pub fn peer_id(&self) -> Option<[u8; 20]> {
        self.remote_peer_id
    }

    pub fn transition(&mut self, event: StateEvent) -> Result<(), PeerError> {
        self.state = self.state.transition(event)?;
        Ok(())
    }
}
