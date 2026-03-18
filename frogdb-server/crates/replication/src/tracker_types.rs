//! Replica information and state types for replication tracking.

use std::net::SocketAddr;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    pub id: u64,
    pub address: SocketAddr,
    pub listening_port: u16,
    pub acked_offset: u64,
    pub last_ack_time: Instant,
    pub connected_at: Instant,
    pub state: ReplicaState,
    pub capabilities: ReplicaCapabilities,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaState {
    Connecting,
    Handshaking,
    Syncing,
    Streaming,
    Disconnected,
}

impl std::fmt::Display for ReplicaState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicaState::Connecting => write!(f, "connecting"),
            ReplicaState::Handshaking => write!(f, "handshaking"),
            ReplicaState::Syncing => write!(f, "syncing"),
            ReplicaState::Streaming => write!(f, "online"),
            ReplicaState::Disconnected => write!(f, "disconnected"),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReplicaCapabilities {
    pub eof: bool,
    pub psync2: bool,
}

impl ReplicaCapabilities {
    pub fn parse_capa(capabilities: &[&str]) -> Self {
        let mut caps = Self::default();
        for cap in capabilities {
            match *cap {
                "eof" => caps.eof = true,
                "psync2" => caps.psync2 = true,
                _ => {}
            }
        }
        caps
    }
}
