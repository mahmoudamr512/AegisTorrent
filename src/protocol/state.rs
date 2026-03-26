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
