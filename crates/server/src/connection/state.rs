//! Connection state types.
//!
//! This module contains all state types used by the connection handler:
//! - [`ConnectionState`] - Full connection state
//! - [`TransactionState`] - MULTI/EXEC transaction state
//! - [`PubSubState`] - Pub/Sub subscription state
//! - [`AuthState`] - Authentication state
//! - [`BlockedState`] - Blocking command wait state
//! - [`LocalClientStats`] - Per-connection stats accumulator

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use bytes::Bytes;
use frogdb_core::{AuthenticatedUser, ClientStatsDelta};
use frogdb_protocol::{ParsedCommand, ProtocolVersion};

/// Target shard(s) for a transaction (prepared for future multi-shard support).
#[derive(Debug, Clone, Default)]
pub enum TransactionTarget {
    /// No keys yet - target undetermined.
    #[default]
    None,
    /// Single shard - execute directly.
    Single(usize),
    /// Multiple shards detected - error in Phase 7.1, VLL in future.
    Multi(Vec<usize>),
}

/// State for MULTI/EXEC transactions.
#[derive(Debug, Default)]
pub struct TransactionState {
    /// Queue of commands to execute at EXEC time (None = not in transaction).
    pub queue: Option<Vec<ParsedCommand>>,
    /// Watched keys: key -> (shard_id, version_at_watch_time).
    pub watches: HashMap<Bytes, (usize, u64)>,
    /// Target shard(s) for this transaction.
    pub target: TransactionTarget,
    /// Whether any queued command had an error (abort at EXEC).
    pub exec_abort: bool,
    /// Error messages for commands that had syntax errors during queuing.
    pub queued_errors: Vec<String>,
    /// Start time of the transaction (when MULTI was called).
    pub start_time: Option<std::time::Instant>,
}

/// Pub/Sub state for a connection.
#[derive(Debug, Default)]
pub struct PubSubState {
    /// Broadcast channel subscriptions.
    pub subscriptions: HashSet<Bytes>,
    /// Pattern subscriptions.
    pub patterns: HashSet<Bytes>,
    /// Sharded channel subscriptions.
    pub sharded_subscriptions: HashSet<Bytes>,
}

impl PubSubState {
    /// Check if connection is in pub/sub mode.
    pub fn in_pubsub_mode(&self) -> bool {
        !self.subscriptions.is_empty()
            || !self.patterns.is_empty()
            || !self.sharded_subscriptions.is_empty()
    }

    /// Get total subscription count.
    pub fn total_count(&self) -> usize {
        self.subscriptions.len() + self.patterns.len() + self.sharded_subscriptions.len()
    }

    /// Get subscription count (channels + patterns, not sharded).
    pub fn sub_count(&self) -> usize {
        self.subscriptions.len() + self.patterns.len()
    }
}

/// Authentication state for a connection.
#[derive(Debug, Clone)]
pub enum AuthState {
    /// Not authenticated yet (default when requirepass is set).
    NotAuthenticated,
    /// Authenticated with a specific user.
    Authenticated(AuthenticatedUser),
}

impl Default for AuthState {
    fn default() -> Self {
        // By default, use the default user with full permissions
        AuthState::Authenticated(AuthenticatedUser::default_user())
    }
}

impl AuthState {
    /// Check if the connection is authenticated.
    pub fn is_authenticated(&self) -> bool {
        matches!(self, AuthState::Authenticated(_))
    }

    /// Get the authenticated user, if any.
    pub fn user(&self) -> Option<&AuthenticatedUser> {
        match self {
            AuthState::Authenticated(user) => Some(user),
            AuthState::NotAuthenticated => None,
        }
    }

    /// Get the username.
    pub fn username(&self) -> &str {
        match self {
            AuthState::Authenticated(user) => &user.username,
            AuthState::NotAuthenticated => "(not authenticated)",
        }
    }
}

/// Blocked state for connections waiting on blocking commands.
#[derive(Debug, Clone)]
pub struct BlockedState {
    /// Shard ID where the wait is registered.
    pub shard_id: usize,
    /// Keys the client is waiting on.
    pub keys: Vec<Bytes>,
}

/// Reply mode for CLIENT REPLY command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplyMode {
    /// Normal reply mode (default).
    #[default]
    On,
    /// No replies to client commands.
    Off,
}

/// Interval for syncing local stats to the registry (in commands).
pub const STATS_SYNC_INTERVAL_COMMANDS: u64 = 100;

/// Interval for syncing local stats to the registry (in milliseconds).
pub const STATS_SYNC_INTERVAL_MS: u64 = 1000;

/// Connection-local stats accumulator.
/// This is kept locally in the connection handler to minimize lock contention,
/// and synced periodically to the ClientRegistry.
#[derive(Debug, Default)]
pub struct LocalClientStats {
    /// Total commands processed since last sync.
    pub commands_total: u64,
    /// Total latency accumulated since last sync (microseconds).
    pub latency_total_us: u64,
    /// Total bytes received since last sync.
    pub bytes_recv: u64,
    /// Total bytes sent since last sync.
    pub bytes_sent: u64,
    /// Per-command latencies: (command_name, latency_us).
    pub command_latencies: Vec<(String, u64)>,
}

impl LocalClientStats {
    /// Record a command execution.
    pub fn record_command(&mut self, cmd_name: &str, latency_us: u64) {
        self.commands_total += 1;
        self.latency_total_us += latency_us;
        self.command_latencies.push((cmd_name.to_string(), latency_us));
    }

    /// Add bytes received.
    pub fn add_bytes_recv(&mut self, bytes: u64) {
        self.bytes_recv += bytes;
    }

    /// Add bytes sent.
    pub fn add_bytes_sent(&mut self, bytes: u64) {
        self.bytes_sent += bytes;
    }

    /// Convert to a ClientStatsDelta for syncing to the registry.
    pub fn to_delta(&self) -> ClientStatsDelta {
        ClientStatsDelta {
            commands_processed: self.commands_total,
            total_latency_us: self.latency_total_us,
            bytes_recv: self.bytes_recv,
            bytes_sent: self.bytes_sent,
            command_latencies: self.command_latencies.clone(),
        }
    }

    /// Check if there's data to sync.
    pub fn has_data(&self) -> bool {
        self.commands_total > 0 || self.bytes_recv > 0 || self.bytes_sent > 0
    }

    /// Clear after syncing.
    pub fn clear(&mut self) {
        self.commands_total = 0;
        self.latency_total_us = 0;
        self.bytes_recv = 0;
        self.bytes_sent = 0;
        self.command_latencies.clear();
    }
}

/// Connection state.
#[allow(dead_code)]
pub struct ConnectionState {
    /// Unique connection ID.
    pub id: u64,

    /// Client address.
    pub addr: SocketAddr,

    /// Connection creation time.
    pub created_at: std::time::Instant,

    /// Protocol version.
    pub protocol_version: ProtocolVersion,

    /// Whether HELLO has been received on this connection.
    pub hello_received: bool,

    /// When HELLO was received (for debugging/monitoring).
    pub hello_at: Option<std::time::Instant>,

    /// Client name (from CLIENT SETNAME).
    pub name: Option<Bytes>,

    /// Transaction state for MULTI/EXEC.
    pub transaction: TransactionState,

    /// Pub/Sub state.
    pub pubsub: PubSubState,

    /// Authentication state.
    pub auth: AuthState,

    /// Blocked state for blocking commands (None = not blocked).
    pub blocked: Option<BlockedState>,

    /// Reply mode (from CLIENT REPLY).
    pub reply_mode: ReplyMode,

    /// Skip the next reply (for CLIENT REPLY SKIP).
    pub skip_next_reply: bool,

    /// ASKING flag for cluster slot migration (cleared after use).
    pub asking: bool,

    /// READONLY flag for allowing reads on cluster replicas.
    pub readonly: bool,

    /// Local stats accumulator (synced to registry periodically).
    pub local_stats: LocalClientStats,

    /// Last sync time for stats.
    pub last_stats_sync: std::time::Instant,
}

impl ConnectionState {
    /// Create a new connection state.
    pub fn new(id: u64, addr: SocketAddr, requires_auth: bool) -> Self {
        let now = std::time::Instant::now();
        Self {
            id,
            addr,
            created_at: now,
            protocol_version: ProtocolVersion::default(),
            hello_received: false,
            hello_at: None,
            name: None,
            transaction: TransactionState::default(),
            pubsub: PubSubState::default(),
            auth: if requires_auth {
                AuthState::NotAuthenticated
            } else {
                AuthState::default()
            },
            blocked: None,
            reply_mode: ReplyMode::default(),
            skip_next_reply: false,
            asking: false,
            readonly: false,
            local_stats: LocalClientStats::default(),
            last_stats_sync: now,
        }
    }
}
