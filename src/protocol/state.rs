#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum ConnectionState {
    #[default]
    Disconnected,
    Connecting,
    Handshaking,
    Choked,
    Downloading,
    Seeding,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StateEvent {
    Connect,
    TcpEstablished,
    HandshakeComplete,
    Unchoke,
    Choke,
    DownloadComplete,
    Error,
}

#[derive(Debug, PartialEq)]
pub enum TransitionError {
    Invalid {
        from: ConnectionState,
        event: StateEvent,
    },
}

impl ConnectionState {
    pub fn transition(self, event: StateEvent) -> Result<Self, TransitionError> {
        match (self, event) {
            (Self::Disconnected, StateEvent::Connect) => Ok(Self::Connecting),
            (Self::Connecting, StateEvent::TcpEstablished) => Ok(Self::Handshaking),
            (Self::Handshaking, StateEvent::HandshakeComplete) => Ok(Self::Choked),
            (Self::Choked, StateEvent::Unchoke) => Ok(Self::Downloading),
            (Self::Downloading, StateEvent::Choke) => Ok(Self::Choked),
            (Self::Downloading, StateEvent::DownloadComplete) => Ok(Self::Seeding),
            (_, StateEvent::Error) => Ok(Self::Disconnected),
            (from, event) => Err(TransitionError::Invalid { from, event }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path_to_seeding() {
        let state = ConnectionState::default();
        assert_eq!(state, ConnectionState::Disconnected);

        let state = state.transition(StateEvent::Connect).unwrap();
        assert_eq!(state, ConnectionState::Connecting);

        let state = state.transition(StateEvent::TcpEstablished).unwrap();
        assert_eq!(state, ConnectionState::Handshaking);

        let state = state.transition(StateEvent::HandshakeComplete).unwrap();
        assert_eq!(state, ConnectionState::Choked);

        let state = state.transition(StateEvent::Unchoke).unwrap();
        assert_eq!(state, ConnectionState::Downloading);

        let state = state.transition(StateEvent::DownloadComplete).unwrap();
        assert_eq!(state, ConnectionState::Seeding);
    }

    #[test]
    fn rejects_invalid_transition() {
        let state = ConnectionState::Disconnected;
        let result = state.transition(StateEvent::Unchoke);
        assert_eq!(
            result,
            Err(TransitionError::Invalid {
                from: ConnectionState::Disconnected,
                event: StateEvent::Unchoke,
            })
        );
    }

    #[test]
    fn error_always_disconnects() {
        let state = ConnectionState::Downloading;
        let state = state.transition(StateEvent::Error).unwrap();
        assert_eq!(state, ConnectionState::Disconnected);
    }

    #[test]
    fn choke_unchoke_cycle() {
        let state = ConnectionState::Choked;
        let state = state.transition(StateEvent::Unchoke).unwrap();
        assert_eq!(state, ConnectionState::Downloading);

        let state = state.transition(StateEvent::Choke).unwrap();
        assert_eq!(state, ConnectionState::Choked);
    }
}
