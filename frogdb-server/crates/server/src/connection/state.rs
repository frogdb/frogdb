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
use frogdb_core::{
    AuthenticatedUser, ClientStatsDelta, MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION,
    MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION, MAX_SUBSCRIPTIONS_PER_CONNECTION,
};
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
    /// First slot seen in cluster mode (for slot-level CROSSSLOT detection).
    /// Redis requires all keys in a MULTI/EXEC to hash to the same slot,
    /// which is stricter than shard-level detection.
    pub cluster_first_slot: Option<u16>,
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
    /// Whether the 80% channel subscription warning has been emitted.
    pub warned_sub_80: bool,
    /// Whether the 80% pattern subscription warning has been emitted.
    pub warned_pattern_80: bool,
    /// Whether the 80% sharded subscription warning has been emitted.
    pub warned_sharded_80: bool,
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

/// Tracking mode for CLIENT TRACKING.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TrackingMode {
    /// Default mode: all reads are tracked.
    #[default]
    Default,
    /// Opt-in mode: reads tracked only after CLIENT CACHING YES.
    OptIn,
    /// Opt-out mode: reads tracked unless CLIENT CACHING NO.
    OptOut,
    /// Broadcast mode: invalidations based on prefix matching, no per-read tracking.
    Broadcast,
}

/// Client-side caching tracking state.
#[derive(Debug, Default)]
pub struct TrackingState {
    /// Whether tracking is enabled.
    pub enabled: bool,
    /// Tracking mode (Default, OptIn, OptOut, Broadcast).
    pub mode: TrackingMode,
    /// NOLOOP flag: don't send invalidation to the connection that modified the key.
    pub noloop: bool,
    /// Per-command caching override (consumed after next read command).
    /// `Some(true)` = CLIENT CACHING YES, `Some(false)` = CLIENT CACHING NO.
    pub caching_override: Option<bool>,
    /// BCAST registered prefixes (empty = match all keys).
    pub prefixes: Vec<bytes::Bytes>,
    /// REDIRECT target connection ID (0 = no redirect).
    pub redirect: u64,
}

impl TrackingState {
    /// Compute whether the next command's reads should be tracked.
    /// Consumes `caching_override`.
    pub fn should_track_read(&mut self) -> bool {
        if !self.enabled {
            return false;
        }
        let ov = self.caching_override.take();
        match self.mode {
            TrackingMode::Default => true,
            TrackingMode::OptIn => ov == Some(true),
            TrackingMode::OptOut => ov != Some(false),
            // BCAST mode doesn't do per-read tracking — invalidation is prefix-based
            TrackingMode::Broadcast => false,
        }
    }
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

// ============================================================================
// Connection state-machine transition types
//
// These are the small interface vocabulary for [`ConnectionState`]'s named
// transitions: callers ask ("admit these subscriptions", "take the queued
// transaction", "what is the next reply's fate?") rather than poking fields.
// ============================================================================

/// Which pub/sub subscription set a transition targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubKind {
    /// Broadcast channel subscriptions (SUBSCRIBE).
    Channel,
    /// Pattern subscriptions (PSUBSCRIBE).
    Pattern,
    /// Sharded channel subscriptions (SSUBSCRIBE).
    Sharded,
}

/// Outcome of admitting a batch of subscriptions of a given [`SubKind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeOutcome {
    /// Adding the batch would exceed the per-connection limit; reject it
    /// wholesale (nothing is inserted).
    LimitReached,
    /// The batch is admitted. `crossed_80` is true iff this call tipped the
    /// one-shot 80% warning latch (used by tests; the warning is emitted
    /// internally).
    Admitted {
        /// Whether the 80% warning latch fired on this call.
        crossed_80: bool,
    },
}

/// Read-only snapshot of per-connection subscription counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SubCounts {
    /// Number of channel subscriptions.
    pub channels: usize,
    /// Number of pattern subscriptions.
    pub patterns: usize,
    /// Number of sharded channel subscriptions.
    pub sharded: usize,
}

/// Error returned by a transaction lifecycle transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnError {
    /// MULTI issued while already in a transaction.
    Nested,
}

/// Snapshot of a transaction captured atomically by EXEC.
///
/// Taking the summary leaves the connection's transaction state clean, so the
/// EXEC handler never needs to clear fields by hand.
#[derive(Debug)]
pub struct TxnSummary {
    /// Queued commands, in submission order.
    pub queue: Vec<ParsedCommand>,
    /// Watched keys with their watch-time versions.
    pub watches: Vec<(Bytes, u64)>,
    /// Target shard(s) folded from the queued commands and watches.
    pub target: TransactionTarget,
    /// Whether a command failed during queuing (EXEC must abort).
    pub exec_abort: bool,
    /// When MULTI was issued, for duration metrics.
    pub start_time: Option<std::time::Instant>,
}

/// Lightweight metrics captured by DISCARD.
#[derive(Debug, Clone, Copy)]
pub struct TxnMetrics {
    /// Number of commands that had been queued.
    pub queued_count: usize,
    /// When MULTI was issued, for duration metrics.
    pub start_time: Option<std::time::Instant>,
}

/// Disposition of the next reply, decided by [`ConnectionState::consume_reply_disposition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyDisposition {
    /// Send the reply to the client.
    Send,
    /// Suppress the reply (CLIENT REPLY OFF, or a consumed SKIP).
    Suppress,
}

/// What RESET found active, so the handler can perform the matching I/O half
/// (shard notifications, channel/redirect teardown).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResetEffects {
    /// The connection was in pub/sub mode before RESET.
    pub was_in_pubsub: bool,
    /// Client-side caching tracking was enabled before RESET.
    pub tracking_was_enabled: bool,
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
        self.command_latencies
            .push((cmd_name.to_string(), latency_us));
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

    /// Client-side caching tracking state.
    pub tracking: TrackingState,

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
            tracking: TrackingState::default(),
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

    // ------------------------------------------------------------------------
    // Pub/sub subscriptions
    // ------------------------------------------------------------------------

    /// Whether the connection is in pub/sub mode (has any subscription).
    pub fn in_pubsub_mode(&self) -> bool {
        self.pubsub.in_pubsub_mode()
    }

    /// Read-only snapshot of subscription counts (for DEBUG / CLIENT INFO).
    pub fn subscription_counts(&self) -> SubCounts {
        SubCounts {
            channels: self.pubsub.subscriptions.len(),
            patterns: self.pubsub.patterns.len(),
            sharded: self.pubsub.sharded_subscriptions.len(),
        }
    }

    /// Owned snapshot of all subscription names of `kind` (for unsubscribe-all
    /// fan-out, where the caller mutates the set while iterating).
    pub fn subscriptions(&self, kind: SubKind) -> Vec<Bytes> {
        match kind {
            SubKind::Channel => self.pubsub.subscriptions.iter().cloned().collect(),
            SubKind::Pattern => self.pubsub.patterns.iter().cloned().collect(),
            SubKind::Sharded => self.pubsub.sharded_subscriptions.iter().cloned().collect(),
        }
    }

    /// Read-only iterator over every subscription name (channels, patterns, and
    /// sharded), for cleanup/memory accounting.
    pub fn subscription_name_iter(&self) -> impl Iterator<Item = &Bytes> {
        self.pubsub
            .subscriptions
            .iter()
            .chain(self.pubsub.patterns.iter())
            .chain(self.pubsub.sharded_subscriptions.iter())
    }

    /// Admit `additional` subscriptions of `kind`, enforcing the per-connection
    /// limit and updating the one-shot 80% warning latch (emitting the warning
    /// internally when it first crosses the threshold).
    ///
    /// This does not insert — callers add names one at a time via
    /// [`add_subscription`](Self::add_subscription) so they can interleave shard
    /// fan-out between insertions.
    pub fn admit_subscriptions(&mut self, kind: SubKind, additional: usize) -> SubscribeOutcome {
        let (current, max, label) = match kind {
            SubKind::Channel => (
                self.pubsub.subscriptions.len(),
                MAX_SUBSCRIPTIONS_PER_CONNECTION,
                "channel",
            ),
            SubKind::Pattern => (
                self.pubsub.patterns.len(),
                MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION,
                "pattern",
            ),
            SubKind::Sharded => (
                self.pubsub.sharded_subscriptions.len(),
                MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION,
                "sharded",
            ),
        };

        let new_count = current + additional;
        if new_count > max {
            return SubscribeOutcome::LimitReached;
        }

        let threshold_80 = max * 4 / 5;
        let already_warned = match kind {
            SubKind::Channel => self.pubsub.warned_sub_80,
            SubKind::Pattern => self.pubsub.warned_pattern_80,
            SubKind::Sharded => self.pubsub.warned_sharded_80,
        };
        let crossed_80 = new_count >= threshold_80 && !already_warned;
        if crossed_80 {
            match kind {
                SubKind::Channel => self.pubsub.warned_sub_80 = true,
                SubKind::Pattern => self.pubsub.warned_pattern_80 = true,
                SubKind::Sharded => self.pubsub.warned_sharded_80 = true,
            }
            tracing::warn!(
                conn_id = self.id,
                current = new_count,
                limit = max,
                "Connection approaching {label} subscription limit (80%)"
            );
        }

        SubscribeOutcome::Admitted { crossed_80 }
    }

    /// Insert one subscription of `kind`; returns the count to report in the
    /// confirmation reply (channels + patterns for Channel/Pattern; the sharded
    /// count for Sharded).
    pub fn add_subscription(&mut self, kind: SubKind, name: Bytes) -> usize {
        match kind {
            SubKind::Channel => {
                self.pubsub.subscriptions.insert(name);
                self.pubsub.sub_count()
            }
            SubKind::Pattern => {
                self.pubsub.patterns.insert(name);
                self.pubsub.sub_count()
            }
            SubKind::Sharded => {
                self.pubsub.sharded_subscriptions.insert(name);
                self.pubsub.sharded_subscriptions.len()
            }
        }
    }

    /// Remove one subscription of `kind`; returns the count to report in the
    /// confirmation reply (same convention as [`add_subscription`](Self::add_subscription)).
    pub fn remove_subscription(&mut self, kind: SubKind, name: &Bytes) -> usize {
        match kind {
            SubKind::Channel => {
                self.pubsub.subscriptions.remove(name);
                self.pubsub.sub_count()
            }
            SubKind::Pattern => {
                self.pubsub.patterns.remove(name);
                self.pubsub.sub_count()
            }
            SubKind::Sharded => {
                self.pubsub.sharded_subscriptions.remove(name);
                self.pubsub.sharded_subscriptions.len()
            }
        }
    }

    /// Re-arm the one-shot 80% warning latch for `kind` once the set has fallen
    /// back below the threshold (called after an unsubscribe batch).
    pub fn rearm_subscription_warning(&mut self, kind: SubKind) {
        match kind {
            SubKind::Channel => {
                if self.pubsub.subscriptions.len() < MAX_SUBSCRIPTIONS_PER_CONNECTION * 4 / 5 {
                    self.pubsub.warned_sub_80 = false;
                }
            }
            SubKind::Pattern => {
                if self.pubsub.patterns.len() < MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION * 4 / 5 {
                    self.pubsub.warned_pattern_80 = false;
                }
            }
            SubKind::Sharded => {
                if self.pubsub.sharded_subscriptions.len()
                    < MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION * 4 / 5
                {
                    self.pubsub.warned_sharded_80 = false;
                }
            }
        }
    }

    /// Drop all subscriptions; returns whether the connection had been in
    /// pub/sub mode (so the caller can decide whether to notify shards).
    pub fn exit_pubsub(&mut self) -> bool {
        let was_in_pubsub = self.pubsub.in_pubsub_mode();
        self.pubsub = PubSubState::default();
        was_in_pubsub
    }

    // ------------------------------------------------------------------------
    // Transactions (MULTI / EXEC / DISCARD / WATCH)
    // ------------------------------------------------------------------------

    /// Whether a transaction is open (a command queue exists).
    pub fn in_transaction(&self) -> bool {
        self.transaction.queue.is_some()
    }

    /// Begin a transaction (MULTI). Errors with [`TxnError::Nested`] if one is
    /// already open. Existing watches are preserved (WATCH before MULTI).
    pub fn begin_transaction(&mut self) -> Result<(), TxnError> {
        if self.transaction.queue.is_some() {
            return Err(TxnError::Nested);
        }
        self.transaction.queue = Some(Vec::new());
        self.transaction.target = TransactionTarget::None;
        self.transaction.exec_abort = false;
        self.transaction.queued_errors.clear();
        self.transaction.start_time = Some(std::time::Instant::now());
        Ok(())
    }

    /// Push a validated command onto the transaction queue (no-op outside a
    /// transaction, matching the historical guard).
    pub fn push_queued_command(&mut self, cmd: ParsedCommand) {
        if let Some(ref mut queue) = self.transaction.queue {
            queue.push(cmd);
        }
    }

    /// Mark the transaction poisoned so EXEC aborts. An accompanying error
    /// message, if any, is recorded for diagnostics.
    pub fn abort_transaction(&mut self, error: Option<String>) {
        self.transaction.exec_abort = true;
        if let Some(error) = error {
            self.transaction.queued_errors.push(error);
        }
    }

    /// Fold a shard into the transaction target (None → Single → Multi).
    pub fn add_transaction_shard(&mut self, shard_id: usize) {
        self.transaction.target = match &self.transaction.target {
            TransactionTarget::None => TransactionTarget::Single(shard_id),
            TransactionTarget::Single(s) if *s == shard_id => TransactionTarget::Single(shard_id),
            TransactionTarget::Single(s) => TransactionTarget::Multi(vec![*s, shard_id]),
            TransactionTarget::Multi(shards) => {
                let mut shards = shards.clone();
                if !shards.contains(&shard_id) {
                    shards.push(shard_id);
                }
                TransactionTarget::Multi(shards)
            }
        };
    }

    /// Record a key's slot during cluster-mode queuing. The first slot seen is
    /// remembered; a later key in a different slot forces the target to `Multi`
    /// (so EXEC returns CROSSSLOT). Returns `true` when this key was cross-slot,
    /// signalling the caller to skip the normal per-shard fold.
    pub fn note_cluster_slot(&mut self, slot: u16, shard_id: usize) -> bool {
        match self.transaction.cluster_first_slot {
            None => {
                self.transaction.cluster_first_slot = Some(slot);
                false
            }
            Some(s) if s != slot => {
                self.transaction.target = match &self.transaction.target {
                    TransactionTarget::None => TransactionTarget::Multi(vec![shard_id]),
                    TransactionTarget::Single(s) => TransactionTarget::Multi(vec![*s, shard_id]),
                    TransactionTarget::Multi(shards) => {
                        let mut shards = shards.clone();
                        if !shards.contains(&shard_id) {
                            shards.push(shard_id);
                        }
                        TransactionTarget::Multi(shards)
                    }
                };
                true
            }
            _ => false,
        }
    }

    /// Record a watched key with its watch-time version and shard.
    pub fn watch_key(&mut self, key: Bytes, shard_id: usize, version: u64) {
        self.transaction.watches.insert(key, (shard_id, version));
    }

    /// Forget all watched keys (UNWATCH).
    pub fn unwatch_all(&mut self) {
        self.transaction.watches.clear();
    }

    /// EXEC: take the queue and watches atomically, leaving the transaction
    /// state clean. Returns `None` for EXEC without MULTI.
    pub fn take_transaction(&mut self) -> Option<TxnSummary> {
        self.transaction.queue.as_ref()?;
        let txn = std::mem::take(&mut self.transaction);
        Some(TxnSummary {
            queue: txn.queue.expect("queue presence checked above"),
            watches: txn
                .watches
                .into_iter()
                .map(|(key, (_, version))| (key, version))
                .collect(),
            target: txn.target,
            exec_abort: txn.exec_abort,
            start_time: txn.start_time,
        })
    }

    /// DISCARD: drop the whole transaction including watches. Returns `None` for
    /// DISCARD without MULTI; otherwise lightweight metrics for the caller.
    pub fn discard_transaction(&mut self) -> Option<TxnMetrics> {
        let queued_count = self.transaction.queue.as_ref()?.len();
        let start_time = self.transaction.start_time;
        self.transaction = TransactionState::default();
        Some(TxnMetrics {
            queued_count,
            start_time,
        })
    }

    /// Clear the entire transaction state unconditionally (QUIT / RESET).
    pub fn clear_transaction(&mut self) {
        self.transaction = TransactionState::default();
    }

    // ------------------------------------------------------------------------
    // Cluster flags: ASKING / READONLY
    // ------------------------------------------------------------------------

    /// Set the one-shot ASKING flag (ASKING command).
    pub fn set_asking(&mut self) {
        self.asking = true;
    }

    /// Read and clear the ASKING flag. Returns the value it held; a second call
    /// without an intervening [`set_asking`](Self::set_asking) returns `false`.
    pub fn take_asking(&mut self) -> bool {
        std::mem::replace(&mut self.asking, false)
    }

    /// Set or clear the READONLY replica-read flag (READONLY / READWRITE).
    pub fn set_readonly(&mut self, readonly: bool) {
        self.readonly = readonly;
    }

    /// Whether the connection is in READONLY mode.
    pub fn is_readonly(&self) -> bool {
        self.readonly
    }

    // ------------------------------------------------------------------------
    // Reply control (CLIENT REPLY)
    // ------------------------------------------------------------------------

    /// Enable replies (CLIENT REPLY ON).
    pub fn reply_on(&mut self) {
        self.reply_mode = ReplyMode::On;
    }

    /// Disable replies (CLIENT REPLY OFF).
    pub fn reply_off(&mut self) {
        self.reply_mode = ReplyMode::Off;
    }

    /// Suppress the next reply (CLIENT REPLY SKIP).
    pub fn reply_skip_next(&mut self) {
        self.skip_next_reply = true;
    }

    /// Decide the fate of the next reply, consuming the one-shot SKIP latch.
    pub fn consume_reply_disposition(&mut self) -> ReplyDisposition {
        match self.reply_mode {
            ReplyMode::Off => ReplyDisposition::Suppress,
            ReplyMode::On => {
                if self.skip_next_reply {
                    self.skip_next_reply = false;
                    ReplyDisposition::Suppress
                } else {
                    ReplyDisposition::Send
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    // RESET
    // ------------------------------------------------------------------------

    /// State half of the RESET command: exit pub/sub mode, clear tracking and
    /// transaction state, reset the protocol to RESP2, and clear the client
    /// name. Returns what was active so the caller can perform the I/O half
    /// (shard notifications, invalidation/redirect teardown).
    ///
    /// Per Redis semantics (and matching prior behavior), RESET does not touch
    /// the ASKING / READONLY / reply-mode flags.
    pub fn reset(&mut self) -> ResetEffects {
        let tracking_was_enabled = self.tracking.enabled;
        let was_in_pubsub = self.exit_pubsub();
        self.tracking = TrackingState::default();
        self.transaction = TransactionState::default();
        self.protocol_version = ProtocolVersion::Resp2;
        self.name = None;
        ResetEffects {
            was_in_pubsub,
            tracking_was_enabled,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> ConnectionState {
        ConnectionState::new(1, "127.0.0.1:0".parse().unwrap(), false)
    }

    fn cmd(name: &'static [u8]) -> ParsedCommand {
        ParsedCommand::new(Bytes::from_static(name), vec![Bytes::from_static(b"k")])
    }

    // ---- ASKING -----------------------------------------------------------

    #[test]
    fn asking_is_one_shot() {
        let mut s = state();
        assert!(!s.take_asking(), "fresh connection has no ASKING");
        s.set_asking();
        assert!(s.take_asking(), "ASKING returns true exactly once");
        assert!(!s.take_asking(), "ASKING cleared after a single command");
    }

    #[test]
    fn readonly_toggles() {
        let mut s = state();
        assert!(!s.is_readonly());
        s.set_readonly(true);
        assert!(s.is_readonly());
        s.set_readonly(false);
        assert!(!s.is_readonly());
    }

    // ---- Pub/sub mode entry/exit -----------------------------------------

    #[test]
    fn pubsub_mode_entry_and_exit() {
        let mut s = state();
        assert!(!s.in_pubsub_mode());

        let count = s.add_subscription(SubKind::Channel, Bytes::from_static(b"c1"));
        assert_eq!(count, 1);
        assert!(s.in_pubsub_mode());
        assert_eq!(s.subscription_counts().channels, 1);

        // sub_count for channel/pattern confirmations is channels + patterns.
        let count = s.add_subscription(SubKind::Pattern, Bytes::from_static(b"p*"));
        assert_eq!(count, 2);

        assert_eq!(
            s.subscriptions(SubKind::Channel),
            vec![Bytes::from_static(b"c1")]
        );

        let remaining = s.remove_subscription(SubKind::Channel, &Bytes::from_static(b"c1"));
        assert_eq!(remaining, 1, "one pattern remains");
        assert!(s.in_pubsub_mode(), "still in pub/sub via the pattern");

        let was_in_pubsub = s.exit_pubsub();
        assert!(was_in_pubsub);
        assert!(!s.in_pubsub_mode());
        assert_eq!(s.subscription_counts(), SubCounts::default());
    }

    #[test]
    fn subscribe_limit_and_80pct_latch() {
        let mut s = state();
        let threshold = MAX_SUBSCRIPTIONS_PER_CONNECTION * 4 / 5;

        // First crossing of 80% fires the latch.
        assert_eq!(
            s.admit_subscriptions(SubKind::Channel, threshold),
            SubscribeOutcome::Admitted { crossed_80: true }
        );
        // Latch is one-shot: a second crossing does not re-fire.
        assert_eq!(
            s.admit_subscriptions(SubKind::Channel, threshold),
            SubscribeOutcome::Admitted { crossed_80: false }
        );
        // Dropping below threshold re-arms the latch.
        s.rearm_subscription_warning(SubKind::Channel);
        assert_eq!(
            s.admit_subscriptions(SubKind::Channel, threshold),
            SubscribeOutcome::Admitted { crossed_80: true }
        );
    }

    #[test]
    fn subscribe_rejects_over_limit() {
        let mut s = state();
        for i in 0..MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION {
            s.add_subscription(SubKind::Pattern, Bytes::from(format!("p{i}")));
        }
        assert_eq!(
            s.subscription_counts().patterns,
            MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION
        );
        assert_eq!(
            s.admit_subscriptions(SubKind::Pattern, 1),
            SubscribeOutcome::LimitReached
        );
    }

    // ---- Transaction lifecycle -------------------------------------------

    #[test]
    fn transaction_lifecycle_begin_queue_take() {
        let mut s = state();
        assert!(!s.in_transaction());
        assert!(s.take_transaction().is_none(), "EXEC without MULTI");
        assert!(s.discard_transaction().is_none(), "DISCARD without MULTI");

        s.begin_transaction().expect("first MULTI succeeds");
        assert!(s.in_transaction());
        assert_eq!(s.begin_transaction(), Err(TxnError::Nested));

        s.push_queued_command(cmd(b"GET"));
        s.push_queued_command(cmd(b"SET"));
        s.add_transaction_shard(2);
        s.watch_key(Bytes::from_static(b"k"), 2, 7);

        let summary = s.take_transaction().expect("in transaction");
        assert_eq!(summary.queue.len(), 2);
        assert!(!summary.exec_abort);
        assert_eq!(summary.watches, vec![(Bytes::from_static(b"k"), 7)]);
        assert!(matches!(summary.target, TransactionTarget::Single(2)));

        // take_transaction leaves the state clean (all five fields reset).
        assert!(!s.in_transaction());
        assert!(s.take_transaction().is_none());
    }

    #[test]
    fn transaction_abort_marks_summary() {
        let mut s = state();
        s.begin_transaction().unwrap();
        s.push_queued_command(cmd(b"GET"));
        s.abort_transaction(Some("ERR boom".to_string()));

        let summary = s.take_transaction().expect("in transaction");
        assert!(summary.exec_abort, "poisoned transaction reported at EXEC");
    }

    #[test]
    fn discard_resets_everything_including_watches() {
        let mut s = state();
        s.begin_transaction().unwrap();
        s.push_queued_command(cmd(b"GET"));
        s.watch_key(Bytes::from_static(b"k"), 0, 1);

        let metrics = s.discard_transaction().expect("in transaction");
        assert_eq!(metrics.queued_count, 1);
        assert!(!s.in_transaction());

        // Watches are dropped by DISCARD: a fresh transaction sees none.
        s.begin_transaction().unwrap();
        let summary = s.take_transaction().unwrap();
        assert!(summary.watches.is_empty());
    }

    #[test]
    fn note_cluster_slot_forces_multi_on_crossslot() {
        let mut s = state();
        s.begin_transaction().unwrap();
        // First key establishes the slot; not cross-slot.
        assert!(!s.note_cluster_slot(100, 0));
        s.add_transaction_shard(0);
        // A different slot forces Multi and signals the caller to skip the fold.
        assert!(s.note_cluster_slot(200, 1));
        let summary = s.take_transaction().unwrap();
        assert!(matches!(summary.target, TransactionTarget::Multi(_)));
    }

    // ---- Reply control ----------------------------------------------------

    #[test]
    fn reply_disposition_transitions() {
        let mut s = state();
        // Default mode replies.
        assert_eq!(s.consume_reply_disposition(), ReplyDisposition::Send);

        // SKIP suppresses exactly one reply.
        s.reply_skip_next();
        assert_eq!(s.consume_reply_disposition(), ReplyDisposition::Suppress);
        assert_eq!(s.consume_reply_disposition(), ReplyDisposition::Send);

        // OFF suppresses every reply.
        s.reply_off();
        assert_eq!(s.consume_reply_disposition(), ReplyDisposition::Suppress);
        assert_eq!(s.consume_reply_disposition(), ReplyDisposition::Suppress);

        // ON restores replies.
        s.reply_on();
        assert_eq!(s.consume_reply_disposition(), ReplyDisposition::Send);
    }

    // ---- RESET ------------------------------------------------------------

    #[test]
    fn reset_clears_covered_state() {
        let mut s = state();
        s.add_subscription(SubKind::Channel, Bytes::from_static(b"c"));
        s.begin_transaction().unwrap();
        s.tracking.enabled = true;
        s.protocol_version = ProtocolVersion::Resp3;
        s.name = Some(Bytes::from_static(b"foo"));

        let effects = s.reset();
        assert!(effects.was_in_pubsub);
        assert!(effects.tracking_was_enabled);

        assert!(!s.in_pubsub_mode());
        assert!(!s.in_transaction());
        assert!(!s.tracking.enabled);
        assert!(matches!(s.protocol_version, ProtocolVersion::Resp2));
        assert!(s.name.is_none());
    }

    #[test]
    fn reset_leaves_cluster_and_reply_flags_untouched() {
        let mut s = state();
        s.set_asking();
        s.set_readonly(true);
        s.reply_off();

        let _ = s.reset();

        // RESET does not clear these, matching prior behavior.
        assert!(s.take_asking());
        assert!(s.is_readonly());
        assert_eq!(s.consume_reply_disposition(), ReplyDisposition::Suppress);
    }
}
