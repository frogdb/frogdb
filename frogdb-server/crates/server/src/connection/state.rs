//! Connection state types.
//!
//! This module contains all state types used by the connection handler:
//! - [`ConnectionState`] - Full connection state
//! - [`PubSubState`] - Pub/Sub subscription state
//! - [`AuthState`] - Authentication state
//! - [`BlockedState`] - Blocking command wait state
//! - [`LocalClientStats`] - Per-connection stats accumulator
//!
//! The MULTI/EXEC slice — [`TransactionState`] and its summary/target/error
//! vocabulary — lives in the `frogdb-txn` crate alongside the EXEC algorithm
//! that consumes it, and is re-exported here so the connection layer keeps one
//! import site for connection state.

use std::collections::HashSet;
use std::net::SocketAddr;

use bytes::Bytes;
use frogdb_core::{
    AuthenticatedUser, ClientStatsDelta, MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION,
    MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION, MAX_SUBSCRIPTIONS_PER_CONNECTION,
};
use frogdb_protocol::{ParsedCommand, ProtocolVersion};

pub use frogdb_txn::{TransactionState, TransactionTarget, TxnError, TxnMetrics, TxnSummary};

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

    /// The subscription set backing `kind`.
    pub fn set(&self, kind: SubKind) -> &HashSet<Bytes> {
        match kind {
            SubKind::Channel => &self.subscriptions,
            SubKind::Pattern => &self.patterns,
            SubKind::Sharded => &self.sharded_subscriptions,
        }
    }

    /// How many of `names` would genuinely grow the `kind` set: names that are
    /// not already subscribed, counting a name repeated within the batch once.
    ///
    /// This is the real cost of a subscribe batch, since the sets are
    /// [`HashSet`]s — the raw argument count over-charges duplicates and
    /// re-subscribes.
    pub fn new_name_count(&self, kind: SubKind, names: &[Bytes]) -> usize {
        let held = self.set(kind);
        let mut seen: HashSet<&Bytes> = HashSet::new();
        names
            .iter()
            .filter(|name| !held.contains(*name) && seen.insert(name))
            .count()
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

/// Options parsed from `CLIENT TRACKING ON ...` — the argument vocabulary for
/// the [`ConnectionState::enable_tracking`] transition. Raw flags (not a
/// resolved [`TrackingMode`]) so the transition can enforce Redis's
/// flag-compatibility rules in Redis's check order.
#[derive(Debug, Clone, Default)]
pub struct TrackingEnableRequest {
    /// BCAST flag: broadcast (prefix-based) invalidation.
    pub bcast: bool,
    /// OPTIN flag: track reads only after CLIENT CACHING YES.
    pub optin: bool,
    /// OPTOUT flag: track reads unless CLIENT CACHING NO.
    pub optout: bool,
    /// NOLOOP flag: suppress invalidations caused by this connection's writes.
    pub noloop: bool,
    /// PREFIX arguments (BCAST only). Empty means "match all keys".
    pub prefixes: Vec<Bytes>,
    /// REDIRECT target connection ID (0 = no redirect).
    pub redirect: u64,
}

/// Why a `CLIENT TRACKING ON` transition was rejected. Mirrors Redis's rules
/// (networking.c `clientCommand` + tracking.c `checkPrefixCollisionsOrReply`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrackingEnableError {
    /// PREFIX given without BCAST.
    PrefixRequiresBcast,
    /// Tracking is enabled and the new call flips BCAST on/off.
    BcastModeSwitch,
    /// OPTIN/OPTOUT combined with BCAST.
    OptinOptoutWithBcast,
    /// OPTIN and OPTOUT together.
    OptinAndOptout,
    /// Tracking is enabled in OPTIN (resp. OPTOUT) mode and the new call
    /// requests the opposite.
    OptinOptoutSwitch,
    /// A new prefix overlaps a prefix already registered on this connection.
    PrefixOverlapsExisting { new: Bytes, existing: Bytes },
    /// Two prefixes within the same call overlap each other.
    PrefixOverlapsBatch { new: Bytes, other: Bytes },
}

impl std::fmt::Display for TrackingEnableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PrefixRequiresBcast => {
                write!(f, "PREFIX option requires BCAST mode to be enabled")
            }
            Self::BcastModeSwitch => write!(
                f,
                "You can't switch BCAST mode on/off before disabling tracking \
                 for this client, and then re-enabling it with a different mode."
            ),
            Self::OptinOptoutWithBcast => {
                write!(f, "OPTIN and OPTOUT are not compatible with BCAST")
            }
            Self::OptinAndOptout => write!(f, "OPTIN and OPTOUT are mutually exclusive"),
            Self::OptinOptoutSwitch => write!(
                f,
                "You can't switch OPTIN/OPTOUT mode before disabling tracking \
                 for this client, and then re-enabling it with a different mode."
            ),
            Self::PrefixOverlapsExisting { new, existing } => write!(
                f,
                "Prefix '{}' overlaps with an existing prefix '{}'. \
                 Prefixes for a single client must not overlap.",
                String::from_utf8_lossy(new),
                String::from_utf8_lossy(existing)
            ),
            Self::PrefixOverlapsBatch { new, other } => write!(
                f,
                "Prefix '{}' overlaps with another provided prefix '{}'. \
                 Prefixes for a single client must not overlap.",
                String::from_utf8_lossy(new),
                String::from_utf8_lossy(other)
            ),
        }
    }
}

/// Two prefixes overlap when either is a prefix of the other (equal strings
/// and the empty prefix both count — Redis's `stringCheckPrefix`).
fn prefixes_overlap(a: &Bytes, b: &Bytes) -> bool {
    a.starts_with(b.as_ref()) || b.starts_with(a.as_ref())
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
    ///
    /// `cmd_name` is `None` when the client sent a name the command registry
    /// does not know: the totals still count the round trip, but no per-command
    /// sample is produced, so client-supplied garbage cannot grow the
    /// `cmdstat_*` map without bound (Redis likewise has nowhere to count a
    /// command that isn't in its command table).
    pub fn record_command(&mut self, cmd_name: Option<&str>, latency_us: u64) {
        self.commands_total += 1;
        self.latency_total_us += latency_us;
        if let Some(cmd_name) = cmd_name {
            self.command_latencies
                .push((cmd_name.to_string(), latency_us));
        }
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

    /// Transaction state for MULTI/EXEC. Private: mutate via the transaction
    /// lifecycle methods (`begin_transaction`, `take_transaction`, ...).
    transaction: TransactionState,

    /// Pub/Sub state. Private: mutate via the subscription methods
    /// (`admit_subscriptions`, `add_subscription`, `exit_pubsub`, ...).
    pubsub: PubSubState,

    /// Client-side caching tracking state. Private: read via `tracking()`,
    /// mutate via `enable_tracking`/`disable_tracking`/`set_caching_override`/
    /// `should_track_read`.
    tracking: TrackingState,

    /// Authentication state. Private: read via `is_authenticated`/
    /// `authenticated_user`/`username`, transition via `authenticate`.
    auth: AuthState,

    /// Blocked state for blocking commands (None = not blocked). Private:
    /// transition via `begin_block`/`end_block`, read via `blocked_shard`.
    blocked: Option<BlockedState>,

    /// Reply mode (from CLIENT REPLY). Private: transition via `reply_on`/
    /// `reply_off`/`consume_reply_disposition`.
    reply_mode: ReplyMode,

    /// Skip the next reply (for CLIENT REPLY SKIP). Private: see `reply_skip_next`.
    skip_next_reply: bool,

    /// ASKING flag for cluster slot migration. Private one-shot flag: set via
    /// `set_asking`, read-and-clear via `take_asking` — except inside an open
    /// MULTI, where it is sticky for the whole block and consumed by
    /// `take_transaction` (see [`ConnectionState::take_asking`]).
    asking: bool,

    /// READONLY flag for allowing reads on cluster replicas. Private: set via
    /// `set_readonly`, read via `is_readonly`.
    readonly: bool,

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
        self.pubsub.set(kind).iter().cloned().collect()
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

    /// Admit a batch of `names` as subscriptions of `kind`, enforcing the
    /// per-connection limit and updating the one-shot 80% warning latch
    /// (emitting the warning internally when it first crosses the threshold).
    ///
    /// Only the genuine growth of the subscription set is charged against the
    /// limit: names already held, and names repeated within the batch, cost no
    /// headroom because the set is a [`HashSet`]. Admission stays
    /// all-or-nothing — a batch whose *unique new* names do not fit is rejected
    /// in full.
    ///
    /// This does not insert — callers add names one at a time via
    /// [`add_subscription`](Self::add_subscription) so they can interleave shard
    /// fan-out between insertions.
    pub fn admit_subscriptions(&mut self, kind: SubKind, names: &[Bytes]) -> SubscribeOutcome {
        let (max, label) = match kind {
            SubKind::Channel => (MAX_SUBSCRIPTIONS_PER_CONNECTION, "channel"),
            SubKind::Pattern => (MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION, "pattern"),
            SubKind::Sharded => (MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION, "sharded"),
        };

        let new_count = self.pubsub.set(kind).len() + self.pubsub.new_name_count(kind, names);
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
        self.transaction.is_open()
    }

    /// Read-only view of the queued commands, if a transaction is open
    /// (for DEBUG MEMORY accounting).
    pub fn queued_commands(&self) -> Option<&[ParsedCommand]> {
        self.transaction.queued_commands()
    }

    /// Read-only iterator over watched keys (for DEBUG MEMORY accounting).
    pub fn watched_key_iter(&self) -> impl Iterator<Item = &Bytes> {
        self.transaction.watched_key_iter()
    }

    /// Begin a transaction (MULTI). Errors with [`TxnError::Nested`] if one is
    /// already open. Existing watches are preserved (WATCH before MULTI).
    pub fn begin_transaction(&mut self) -> Result<(), TxnError> {
        self.transaction.begin()
    }

    /// Push a validated command onto the transaction queue (no-op outside a
    /// transaction, matching the historical guard).
    pub fn push_queued_command(&mut self, cmd: ParsedCommand) {
        self.transaction.push_queued_command(cmd);
    }

    /// Mark the transaction poisoned so EXEC aborts. An accompanying error
    /// message, if any, is recorded for diagnostics.
    pub fn abort_transaction(&mut self, error: Option<String>) {
        self.transaction.abort(error);
    }

    /// Fold one queued command's keys into the transaction target. In cluster
    /// mode a slot mismatch promotes the target to `Multi` (EXEC returns
    /// CROSSSLOT); in standalone mode a shard mismatch does. The transaction
    /// state's slot accumulator owns the rule.
    pub fn fold_transaction_keys<K: AsRef<[u8]>>(
        &mut self,
        keys: &[K],
        num_shards: usize,
        is_cluster: bool,
    ) {
        self.transaction.fold_keys(keys, num_shards, is_cluster);
    }

    /// Record a watched key with its watch-time version, shard, and liveness.
    /// First watch wins — see [`TransactionState::watch_key`].
    pub fn watch_key(&mut self, key: Bytes, shard_id: usize, version: u64, live_at_watch: bool) {
        self.transaction
            .watch_key(key, shard_id, version, live_at_watch);
    }

    /// Forget all watched keys (UNWATCH).
    pub fn unwatch_all(&mut self) {
        self.transaction.unwatch_all();
    }

    /// EXEC: take the queue and watches atomically, leaving the transaction
    /// state clean. Returns `None` for EXEC without MULTI.
    ///
    /// ASKING was held sticky for the duration of the block; EXEC is the last
    /// reader, so it is folded into the summary and cleared here (the
    /// connection leaves EXEC with a clean flag, exactly as a non-transactional
    /// command would).
    pub fn take_transaction(&mut self) -> Option<TxnSummary> {
        let summary = self.transaction.take(self.asking)?;
        self.asking = false;
        Some(summary)
    }

    /// DISCARD: drop the whole transaction including watches. Returns `None` for
    /// DISCARD without MULTI; otherwise lightweight metrics for the caller.
    ///
    /// Also clears `ASKING`, which [`take_asking`](Self::take_asking) leaves
    /// sticky for the life of the MULTI block. Without this the flag outlives
    /// the transaction that made it sticky, and the next ordinary command on an
    /// importing target would be accepted as if the client had just said
    /// `ASKING` — a dual-homed write. Redis clears it on the same path
    /// (`discardTransaction` → `resetClient`).
    pub fn discard_transaction(&mut self) -> Option<TxnMetrics> {
        let metrics = self.transaction.discard()?;
        self.asking = false;
        Some(metrics)
    }

    /// Clear the entire transaction state unconditionally (QUIT / RESET),
    /// including the MULTI-sticky `ASKING` flag (see
    /// [`discard_transaction`](Self::discard_transaction)).
    pub fn clear_transaction(&mut self) {
        self.transaction.clear();
        self.asking = false;
    }

    // ------------------------------------------------------------------------
    // Blocking command wait state
    // ------------------------------------------------------------------------

    /// Enter the blocked state for a wait registered on `shard_id`.
    pub fn begin_block(&mut self, shard_id: usize, keys: Vec<Bytes>) {
        self.blocked = Some(BlockedState { shard_id, keys });
    }

    /// Leave the blocked state. Returns the prior [`BlockedState`], if any.
    pub fn end_block(&mut self) -> Option<BlockedState> {
        self.blocked.take()
    }

    /// The shard a blocking wait is registered on, if the connection is
    /// currently blocked (for disconnect cleanup).
    pub fn blocked_shard(&self) -> Option<usize> {
        self.blocked.as_ref().map(|b| b.shard_id)
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
    ///
    /// **Exception — inside an open MULTI the flag is sticky**: it is read but
    /// *not* cleared, so a single `ASKING` issued before `MULTI` covers every
    /// queued command *and* the EXEC-time batch re-validation
    /// ([`ConnectionHandler::execute_transaction`](crate::connection::ConnectionHandler)).
    /// [`take_transaction`](Self::take_transaction) is what consumes it. This
    /// mirrors Redis, which skips clearing `CLIENT_ASKING` while
    /// `CLIENT_MULTI` is set (`networking.c`, `commandProcessed`).
    pub fn take_asking(&mut self) -> bool {
        if self.in_transaction() {
            return self.asking;
        }
        std::mem::replace(&mut self.asking, false)
    }

    /// Read the ASKING flag **without** consuming it.
    ///
    /// For the one caller that must ask the routing seam a question on behalf of
    /// a command that is not itself the command being routed: `WATCH`
    /// ([`ConnectionHandler::dispatch_transaction_command`](crate::connection::ConnectionHandler))
    /// validates its keys' slot, but the one-shot flag belongs to the command
    /// the client sent `ASKING` for — consuming it here would strand the
    /// following `MULTI`/`EXEC` block on the importing target.
    pub fn is_asking(&self) -> bool {
        self.asking
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
    // Authentication (AUTH / HELLO)
    // ------------------------------------------------------------------------

    /// Authenticate the connection as `user` (successful AUTH / HELLO AUTH).
    pub fn authenticate(&mut self, user: AuthenticatedUser) {
        self.auth = AuthState::Authenticated(user);
    }

    /// The `id=.. addr=.. name=..` client descriptor recorded in the ACL log on
    /// an authentication attempt (AUTH / HELLO AUTH).
    pub fn client_info_string(&self) -> String {
        format!(
            "id={} addr={} name={}",
            self.id,
            self.addr,
            self.name
                .as_ref()
                .map(|b| String::from_utf8_lossy(b))
                .unwrap_or_default()
        )
    }

    /// Whether the connection has authenticated (fails closed when auth is
    /// required and no AUTH has succeeded yet).
    pub fn is_authenticated(&self) -> bool {
        self.auth.is_authenticated()
    }

    /// The authenticated user, if any (for ACL permission and rate-limit checks).
    pub fn authenticated_user(&self) -> Option<&AuthenticatedUser> {
        self.auth.user()
    }

    /// The current username, or a placeholder when not authenticated (ACL WHOAMI).
    pub fn username(&self) -> &str {
        self.auth.username()
    }

    // ------------------------------------------------------------------------
    // Client-side caching (CLIENT TRACKING / CACHING)
    // ------------------------------------------------------------------------

    /// Read-only view of the tracking state (CLIENT TRACKINGINFO / GETREDIR,
    /// cleanup accounting). All mutation goes through the transitions below.
    pub fn tracking(&self) -> &TrackingState {
        &self.tracking
    }

    /// Enable client tracking (CLIENT TRACKING ON).
    ///
    /// Redis semantics (networking.c / tracking.c):
    /// - Flags (`noloop`, OPTIN/OPTOUT) and `redirect` are *replaced* by each
    ///   call; omitting a previously-set flag clears it.
    /// - BCAST prefixes *accumulate* across calls; the new batch is checked
    ///   for overlap against the union already on the connection and within
    ///   itself. A bare `ON BCAST` registers the empty ("match all") prefix
    ///   and — as in Redis — skips the overlap check for it.
    /// - Switching BCAST on/off or OPTIN↔OPTOUT while enabled is rejected;
    ///   the client must go through `CLIENT TRACKING OFF` first.
    ///
    /// On success returns the prefixes the caller must register with the
    /// shards for this call (the new batch only — shard-side broadcast
    /// registration is additive). Empty for non-BCAST modes.
    pub fn enable_tracking(
        &mut self,
        req: TrackingEnableRequest,
    ) -> Result<Vec<Bytes>, TrackingEnableError> {
        let TrackingEnableRequest {
            bcast,
            optin,
            optout,
            noloop,
            prefixes,
            redirect,
        } = req;

        // Rejection rules, in Redis's check order.
        if !prefixes.is_empty() && !bcast {
            return Err(TrackingEnableError::PrefixRequiresBcast);
        }
        if self.tracking.enabled && bcast != (self.tracking.mode == TrackingMode::Broadcast) {
            return Err(TrackingEnableError::BcastModeSwitch);
        }
        if bcast && (optin || optout) {
            return Err(TrackingEnableError::OptinOptoutWithBcast);
        }
        if optin && optout {
            return Err(TrackingEnableError::OptinAndOptout);
        }
        if self.tracking.enabled
            && ((optin && self.tracking.mode == TrackingMode::OptOut)
                || (optout && self.tracking.mode == TrackingMode::OptIn))
        {
            return Err(TrackingEnableError::OptinOptoutSwitch);
        }
        // Overlap checks against the accumulated union, then within the new
        // batch. Redis skips both for the implicit empty prefix of a bare
        // `ON BCAST` (the empty prefix is added unchecked below).
        for (i, new_p) in prefixes.iter().enumerate() {
            for old_p in &self.tracking.prefixes {
                if prefixes_overlap(new_p, old_p) {
                    return Err(TrackingEnableError::PrefixOverlapsExisting {
                        new: new_p.clone(),
                        existing: old_p.clone(),
                    });
                }
            }
            for other in &prefixes[i + 1..] {
                if prefixes_overlap(new_p, other) {
                    return Err(TrackingEnableError::PrefixOverlapsBatch {
                        new: new_p.clone(),
                        other: other.clone(),
                    });
                }
            }
        }

        let mode = if bcast {
            TrackingMode::Broadcast
        } else if optin {
            TrackingMode::OptIn
        } else if optout {
            TrackingMode::OptOut
        } else {
            TrackingMode::Default
        };

        let registered = if bcast {
            let batch = if prefixes.is_empty() {
                vec![Bytes::new()]
            } else {
                prefixes
            };
            for p in &batch {
                if !self.tracking.prefixes.contains(p) {
                    self.tracking.prefixes.push(p.clone());
                }
            }
            batch
        } else {
            Vec::new()
        };

        self.tracking.enabled = true;
        self.tracking.mode = mode;
        self.tracking.noloop = noloop;
        self.tracking.caching_override = None;
        self.tracking.redirect = redirect;
        Ok(registered)
    }

    /// Disable client tracking (CLIENT TRACKING OFF). Returns whether tracking
    /// had been enabled, so the caller performs the shard/channel teardown only
    /// when this was not a no-op.
    pub fn disable_tracking(&mut self) -> bool {
        if !self.tracking.enabled {
            return false;
        }
        self.tracking = TrackingState::default();
        true
    }

    /// Set the one-shot per-command caching override (CLIENT CACHING YES/NO),
    /// consumed by the next [`should_track_read`](Self::should_track_read).
    pub fn set_caching_override(&mut self, track: bool) {
        self.tracking.caching_override = Some(track);
    }

    /// Flush this connection's buffered per-client stats into `registry`,
    /// clearing the local buffer and rebasing the sync clock. No-op when there
    /// is nothing to sync (CLIENT STATS forces a sync before reading).
    pub fn sync_stats_to_registry(&mut self, registry: &frogdb_core::ClientRegistry) {
        if self.local_stats.has_data() {
            let delta = self.local_stats.to_delta();
            registry.update_stats(self.id, &delta);
            self.local_stats.clear();
            self.last_stats_sync = std::time::Instant::now();
        }
    }

    /// Whether the next command's reads should be tracked, consuming the
    /// one-shot caching override (delegates to
    /// [`TrackingState::should_track_read`]).
    pub fn should_track_read(&mut self) -> bool {
        self.tracking.should_track_read()
    }

    // ------------------------------------------------------------------------
    // RESET
    // ------------------------------------------------------------------------

    /// State half of the RESET command: exit pub/sub mode, clear tracking and
    /// transaction state, reset the protocol to RESP2, reset the reply mode to
    /// ON (clearing any CLIENT REPLY OFF/SKIP), and clear the client name.
    /// Returns what was active so the caller can perform the I/O half (shard
    /// notifications, invalidation/redirect teardown).
    ///
    /// Per Redis `resetCommand`, RESET also drops the cluster session flags:
    /// `clearClientConnectionState` clears `CLIENT_ASKING | CLIENT_READONLY`
    /// alongside the reply-mode latches. Reverting authentication to the
    /// default user is driven separately by the executor via
    /// [`revert_to_default_user`](Self::revert_to_default_user) (it needs the
    /// ACL manager to re-evaluate the auth flag).
    pub fn reset(&mut self) -> ResetEffects {
        let tracking_was_enabled = self.tracking.enabled;
        let was_in_pubsub = self.exit_pubsub();
        self.tracking = TrackingState::default();
        // Clears the MULTI-sticky ASKING flag along with the queue.
        self.clear_transaction();
        self.readonly = false;
        self.protocol_version = ProtocolVersion::Resp2;
        // Reply mode → ON, clearing any CLIENT REPLY OFF/SKIP latch.
        self.reply_mode = ReplyMode::On;
        self.skip_next_reply = false;
        self.name = None;
        ResetEffects {
            was_in_pubsub,
            tracking_was_enabled,
        }
    }

    /// Revert the connection's authentication to the default user and
    /// re-evaluate whether it counts as authenticated (RESET). Mirrors Redis
    /// `resetCommand`, which sets `c->user = DefaultUser` and marks the
    /// connection authenticated only when the default user is `nopass` and
    /// enabled. When `authenticated`, the connection becomes the default
    /// authenticated user; otherwise it drops to unauthenticated (a subsequent
    /// non-AUTH command replies `NOAUTH`). The executor computes `authenticated`
    /// from the ACL manager.
    pub fn revert_to_default_user(&mut self, authenticated: bool) {
        self.auth = if authenticated {
            AuthState::Authenticated(AuthenticatedUser::default_user())
        } else {
            AuthState::NotAuthenticated
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::WatchEntry;

    fn state() -> ConnectionState {
        ConnectionState::new(1, "127.0.0.1:0".parse().unwrap(), false)
    }

    fn cmd(name: &'static [u8]) -> ParsedCommand {
        ParsedCommand::new(Bytes::from_static(name), vec![Bytes::from_static(b"k")])
    }

    // ---- ASKING -----------------------------------------------------------

    // FM-TXN-015
    #[test]
    fn asking_is_one_shot() {
        let mut s = state();
        assert!(!s.take_asking(), "fresh connection has no ASKING");
        s.set_asking();
        assert!(s.take_asking(), "ASKING returns true exactly once");
        assert!(!s.take_asking(), "ASKING cleared after a single command");
    }

    // FM-TXN-015
    /// Redis keeps `CLIENT_ASKING` set for the whole MULTI block so the EXEC-time
    /// re-validation can still take the importing-target arm. FrogDB mirrors
    /// that: inside an open transaction `take_asking` reads without clearing.
    #[test]
    fn asking_is_sticky_inside_multi_and_consumed_by_exec() {
        let mut s = state();
        s.set_asking();
        s.begin_transaction().expect("MULTI");
        assert!(s.take_asking(), "queued command 1 sees ASKING");
        assert!(s.take_asking(), "queued command 2 still sees ASKING");

        let summary = s.take_transaction().expect("in transaction");
        assert!(summary.asking, "EXEC captures the block-scoped ASKING");
        assert!(
            !s.take_asking(),
            "take_transaction consumes ASKING, leaving the connection clean"
        );
    }

    // FM-TXN-015
    /// A MULTI opened *without* a preceding ASKING never invents one.
    #[test]
    fn asking_absent_inside_multi_stays_absent() {
        let mut s = state();
        s.begin_transaction().expect("MULTI");
        assert!(!s.take_asking());
        let summary = s.take_transaction().expect("in transaction");
        assert!(!summary.asking);
    }

    // FM-TXN-004
    /// DISCARD ends the block that made ASKING sticky, so the flag must go with
    /// it. Leaking it would let the *next* ordinary command be accepted on an
    /// importing target as though the client had just said ASKING.
    #[test]
    fn asking_cleared_by_discard() {
        let mut s = state();
        s.set_asking();
        s.begin_transaction().expect("MULTI");
        assert!(s.take_asking(), "sticky inside the block");

        assert!(s.discard_transaction().is_some());
        assert!(!s.take_asking(), "DISCARD consumes the sticky ASKING");
    }

    // FM-TXN-003
    /// DISCARD outside MULTI is an error and must not consume a pending ASKING.
    #[test]
    fn asking_survives_discard_without_multi() {
        let mut s = state();
        s.set_asking();
        assert!(s.discard_transaction().is_none());
        assert!(s.take_asking());
    }

    // FM-TXN-014
    /// RESET / QUIT take the same path.
    #[test]
    fn asking_cleared_by_clear_transaction() {
        let mut s = state();
        s.set_asking();
        s.begin_transaction().expect("MULTI");
        s.clear_transaction();
        assert!(!s.take_asking());
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

    /// `n` distinct brand-new names for an admission batch.
    fn names(n: usize) -> Vec<Bytes> {
        (0..n).map(|i| Bytes::from(format!("n{i}"))).collect()
    }

    #[test]
    fn subscribe_limit_and_80pct_latch() {
        let mut s = state();
        let batch = names(MAX_SUBSCRIPTIONS_PER_CONNECTION * 4 / 5);

        // First crossing of 80% fires the latch.
        assert_eq!(
            s.admit_subscriptions(SubKind::Channel, &batch),
            SubscribeOutcome::Admitted { crossed_80: true }
        );
        // Latch is one-shot: a second crossing does not re-fire.
        assert_eq!(
            s.admit_subscriptions(SubKind::Channel, &batch),
            SubscribeOutcome::Admitted { crossed_80: false }
        );
        // Dropping below threshold re-arms the latch.
        s.rearm_subscription_warning(SubKind::Channel);
        assert_eq!(
            s.admit_subscriptions(SubKind::Channel, &batch),
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
            s.admit_subscriptions(SubKind::Pattern, &[Bytes::from_static(b"fresh")]),
            SubscribeOutcome::LimitReached
        );
    }

    #[test]
    fn subscribe_charges_only_genuine_set_growth() {
        let mut s = state();
        // Fill to one below the cap.
        for i in 0..MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION - 1 {
            s.add_subscription(SubKind::Pattern, Bytes::from(format!("p{i}")));
        }

        // A brand-new name repeated within one batch grows the set by 1, so it
        // fits the single remaining slot (previously charged as 2, which was
        // spuriously rejected).
        let dup = Bytes::from_static(b"dup");
        assert!(
            matches!(
                s.admit_subscriptions(SubKind::Pattern, &[dup.clone(), dup.clone()]),
                SubscribeOutcome::Admitted { .. }
            ),
            "a duplicated new name costs one slot, not two"
        );
        // Two *distinct* new names still do not fit the single slot.
        assert_eq!(
            s.admit_subscriptions(
                SubKind::Pattern,
                &[dup.clone(), Bytes::from_static(b"other")]
            ),
            SubscribeOutcome::LimitReached
        );

        // Now genuinely full.
        s.add_subscription(SubKind::Pattern, dup);
        assert_eq!(
            s.subscription_counts().patterns,
            MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION
        );

        // Re-subscribing to already-held names is a no-op for the set, so it is
        // admitted even at a full cap (previously charged +1 and rejected).
        assert!(
            matches!(
                s.admit_subscriptions(
                    SubKind::Pattern,
                    &[Bytes::from_static(b"p0"), Bytes::from_static(b"p0")]
                ),
                SubscribeOutcome::Admitted { .. }
            ),
            "re-subscribing to a held name must not consume headroom"
        );
        // ...but a single genuinely new name at a full cap still rejects.
        assert_eq!(
            s.admit_subscriptions(SubKind::Pattern, &[Bytes::from_static(b"brand-new")]),
            SubscribeOutcome::LimitReached
        );
    }

    #[test]
    fn new_name_count_dedupes_batch_and_skips_held() {
        let mut s = state();
        s.add_subscription(SubKind::Channel, Bytes::from_static(b"held"));
        let batch = [
            Bytes::from_static(b"held"),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
        ];
        assert_eq!(s.pubsub.new_name_count(SubKind::Channel, &batch), 2);
        // Kinds are independent: the channel set does not shadow patterns.
        assert_eq!(s.pubsub.new_name_count(SubKind::Pattern, &batch), 3);
    }

    // ---- Transaction lifecycle -------------------------------------------

    // FM-TXN-002, FM-TXN-047
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
        s.transaction.fold_shard(2);
        s.watch_key(Bytes::from_static(b"k"), 2, 7, true);

        let summary = s.take_transaction().expect("in transaction");
        assert_eq!(summary.queue.len(), 2);
        assert!(!summary.exec_abort);
        assert_eq!(
            summary.watches,
            vec![WatchEntry {
                key: Bytes::from_static(b"k"),
                version: 7,
                live_at_watch: true,
            }]
        );
        assert!(matches!(summary.target, TransactionTarget::Single(2)));

        // take_transaction leaves the state clean (all five fields reset).
        assert!(!s.in_transaction());
        assert!(s.take_transaction().is_none());
    }

    // FM-TXN-020
    #[test]
    fn take_transaction_folds_cross_shard_watch_set_to_multi() {
        // A WATCH set spanning two shards must make the transaction's target
        // `Multi` so EXEC CROSSSLOT-rejects, rather than silently version-checking
        // only one shard (a false-negative commit). The fold happens at EXEC time
        // (`take_transaction`), from the live watch set — WATCH itself records no
        // fold, and MULTI does not re-fold.
        let mut s = state();

        // WATCH before MULTI: each WATCH only records the key + shard (no fold).
        s.watch_key(Bytes::from_static(b"{t0}kv0"), 0, 11, true);
        s.watch_key(Bytes::from_static(b"{t1}kv1"), 1, 22, true);

        s.begin_transaction().expect("MULTI after WATCH");
        // Queue a single-shard command (shard 1), as seed 8 does (DEL {t1}kv1).
        s.push_queued_command(cmd(b"DEL"));
        s.transaction.fold_shard(1);

        let summary = s.take_transaction().expect("in transaction");
        // Live cross-shard watch set folded at EXEC → Multi → CROSSSLOT.
        assert!(
            matches!(summary.target, TransactionTarget::Multi(_)),
            "cross-shard WATCH set must promote target to Multi, got {:?}",
            summary.target
        );
        assert!(
            summary.target.resolve().is_err(),
            "Multi target must resolve to a CROSSSLOT rejection"
        );
    }

    // FM-TXN-013
    #[test]
    fn take_transaction_unwatch_drops_stale_cross_shard_watch_fold() {
        // Reviewer's regression: WATCH a key on shard 0, MULTI, UNWATCH (which
        // clears the watch set), then queue a single-shard command on shard 1.
        // UNWATCH inside MULTI executes immediately, so at EXEC the live watch set
        // is empty and must contribute *no* fold — the transaction stays
        // `Single(1)` and commits, rather than being spuriously CROSSSLOT-rejected
        // by a stale shard-0 watch fold.
        let mut s = state();

        s.watch_key(Bytes::from_static(b"{t0}kv0"), 0, 11, true);
        s.begin_transaction().expect("MULTI after WATCH");
        // UNWATCH inside MULTI clears the live watch set (dispatched immediately).
        s.unwatch_all();
        // Queue a single-shard command on a *different* shard than the old watch.
        s.push_queued_command(cmd(b"SET"));
        s.transaction.fold_shard(1);

        let summary = s.take_transaction().expect("in transaction");
        assert!(summary.watches.is_empty(), "UNWATCH cleared the watch set");
        assert!(
            matches!(summary.target, TransactionTarget::Single(1)),
            "UNWATCH must leave no stale fold: target should stay Single(1), got {:?}",
            summary.target
        );
        assert!(
            summary.target.resolve().is_ok(),
            "single-shard EXEC after UNWATCH must not CROSSSLOT"
        );
    }

    // FM-TXN-008
    #[test]
    fn transaction_abort_marks_summary() {
        let mut s = state();
        s.begin_transaction().unwrap();
        s.push_queued_command(cmd(b"GET"));
        s.abort_transaction(Some("ERR boom".to_string()));

        let summary = s.take_transaction().expect("in transaction");
        assert!(summary.exec_abort, "poisoned transaction reported at EXEC");
    }

    // FM-TXN-004
    #[test]
    fn discard_resets_everything_including_watches() {
        let mut s = state();
        s.begin_transaction().unwrap();
        s.push_queued_command(cmd(b"GET"));
        s.watch_key(Bytes::from_static(b"k"), 0, 1, true);

        let metrics = s.discard_transaction().expect("in transaction");
        assert_eq!(metrics.queued_count, 1);
        assert!(!s.in_transaction());

        // Watches are dropped by DISCARD: a fresh transaction sees none.
        s.begin_transaction().unwrap();
        let summary = s.take_transaction().unwrap();
        assert!(summary.watches.is_empty());
    }

    // The transaction co-location accumulator and `TransactionTarget::resolve`
    // are unit-tested in `frogdb-txn` (`state.rs`), where they live. What stays
    // here is the `ConnectionState` delegation: the transaction lifecycle as the
    // connection drives it, including the ASKING interaction that only exists at
    // this level.

    #[test]
    fn fold_transaction_keys_cross_slot_pair_forces_multi_in_cluster() {
        let mut s = state();
        s.begin_transaction().unwrap();
        // "a" and "b" hash to different slots; in cluster mode that is Multi.
        s.fold_transaction_keys(
            &[Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            4,
            true,
        );
        let summary = s.take_transaction().unwrap();
        assert!(matches!(summary.target, TransactionTarget::Multi(_)));
    }

    // ---- Blocking wait state ---------------------------------------------

    #[test]
    fn block_begin_and_end() {
        let mut s = state();
        assert!(
            s.blocked_shard().is_none(),
            "fresh connection is not blocked"
        );

        s.begin_block(
            3,
            vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")],
        );
        assert_eq!(s.blocked_shard(), Some(3));

        let prior = s.end_block().expect("was blocked");
        assert_eq!(prior.shard_id, 3);
        assert_eq!(prior.keys.len(), 2);

        // end_block is idempotent: a second call returns None and stays clear.
        assert!(s.end_block().is_none());
        assert!(s.blocked_shard().is_none());
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

    // ---- Authentication ---------------------------------------------------

    #[test]
    fn authenticate_transitions_from_unauthenticated() {
        use frogdb_core::PermissionSet;
        use std::sync::Arc;

        // A connection that requires auth starts unauthenticated.
        let mut s = ConnectionState::new(1, "127.0.0.1:0".parse().unwrap(), true);
        assert!(!s.is_authenticated());
        assert!(s.authenticated_user().is_none());
        assert_eq!(s.username(), "(not authenticated)");

        let user = AuthenticatedUser::new("alice", Arc::new(PermissionSet::allow_all()), None);
        s.authenticate(user);

        assert!(s.is_authenticated());
        assert_eq!(s.username(), "alice");
        assert_eq!(
            s.authenticated_user().map(|u| u.username.as_ref()),
            Some("alice")
        );
    }

    #[test]
    fn no_auth_required_starts_authenticated_as_default() {
        // The default helper builds a connection that does not require auth.
        let s = state();
        assert!(s.is_authenticated());
        assert_eq!(s.username(), "default");
    }

    // ---- Client tracking (CLIENT TRACKING / CACHING) ----------------------

    /// Shorthand: a BCAST enable request with the given prefixes.
    fn bcast_req(prefixes: &[&'static [u8]]) -> TrackingEnableRequest {
        TrackingEnableRequest {
            bcast: true,
            prefixes: prefixes.iter().map(|p| Bytes::from_static(p)).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn tracking_enable_disable_round_trip() {
        let mut s = state();
        assert!(!s.tracking().enabled);
        assert!(!s.should_track_read(), "no tracking => reads not tracked");

        let registered = s
            .enable_tracking(TrackingEnableRequest {
                noloop: true,
                redirect: 42,
                ..bcast_req(&[b"pfx:"])
            })
            .unwrap();
        assert_eq!(registered, vec![Bytes::from_static(b"pfx:")]);
        assert!(s.tracking().enabled);
        assert_eq!(s.tracking().mode, TrackingMode::Broadcast);
        assert!(s.tracking().noloop);
        assert_eq!(s.tracking().redirect, 42);
        assert_eq!(s.tracking().prefixes, vec![Bytes::from_static(b"pfx:")]);

        // Disable reports it was enabled and fully clears the state.
        assert!(s.disable_tracking(), "was enabled");
        assert!(!s.tracking().enabled);
        assert_eq!(s.tracking().redirect, 0);
        assert!(s.tracking().prefixes.is_empty());

        // A second disable is a no-op and reports so (drives the OFF early-return).
        assert!(!s.disable_tracking(), "already disabled");
    }

    #[test]
    fn tracking_default_mode_tracks_every_read() {
        let mut s = state();
        s.enable_tracking(TrackingEnableRequest::default()).unwrap();
        assert!(s.should_track_read());
        assert!(s.should_track_read(), "Default mode tracks unconditionally");
    }

    #[test]
    fn caching_override_is_one_shot_in_optin() {
        let mut s = state();
        s.enable_tracking(TrackingEnableRequest {
            optin: true,
            ..Default::default()
        })
        .unwrap();
        // OPTIN tracks nothing until CLIENT CACHING YES.
        assert!(!s.should_track_read());

        s.set_caching_override(true);
        assert!(s.should_track_read(), "override consumed once");
        assert!(!s.should_track_read(), "override does not persist");
    }

    #[test]
    fn tracking_bcast_prefixes_accumulate_across_calls() {
        let mut s = state();
        s.enable_tracking(bcast_req(&[b"a:"])).unwrap();
        // Second ON call adds to (not replaces) the registered prefixes; only
        // the new batch is returned for shard registration.
        let registered = s.enable_tracking(bcast_req(&[b"c:"])).unwrap();
        assert_eq!(registered, vec![Bytes::from_static(b"c:")]);
        assert_eq!(
            s.tracking().prefixes,
            vec![Bytes::from_static(b"a:"), Bytes::from_static(b"c:")],
            "prefixes accumulate across CLIENT TRACKING ON calls"
        );
    }

    #[test]
    fn tracking_bcast_overlap_rejected_against_accumulated_union() {
        let mut s = state();
        s.enable_tracking(bcast_req(&[b"a:"])).unwrap();
        s.enable_tracking(bcast_req(&[b"b:"])).unwrap();
        // "a:x" overlaps the prefix from the FIRST call, not the latest batch.
        let err = s.enable_tracking(bcast_req(&[b"a:x"])).unwrap_err();
        assert_eq!(
            err,
            TrackingEnableError::PrefixOverlapsExisting {
                new: Bytes::from_static(b"a:x"),
                existing: Bytes::from_static(b"a:"),
            }
        );
        // A rejected call must not have mutated the union.
        assert_eq!(
            s.tracking().prefixes,
            vec![Bytes::from_static(b"a:"), Bytes::from_static(b"b:")]
        );

        // Re-registering an identical prefix is also an overlap (Redis's
        // stringCheckPrefix counts equal strings).
        let err = s.enable_tracking(bcast_req(&[b"a:"])).unwrap_err();
        assert!(matches!(
            err,
            TrackingEnableError::PrefixOverlapsExisting { .. }
        ));
    }

    #[test]
    fn tracking_bcast_overlap_rejected_within_batch() {
        let mut s = state();
        let err = s
            .enable_tracking(bcast_req(&[b"foobar", b"foo"]))
            .unwrap_err();
        assert_eq!(
            err,
            TrackingEnableError::PrefixOverlapsBatch {
                new: Bytes::from_static(b"foobar"),
                other: Bytes::from_static(b"foo"),
            }
        );
        assert!(!s.tracking().enabled, "rejected call must not enable");
    }

    #[test]
    fn tracking_bare_bcast_registers_empty_prefix_unchecked() {
        let mut s = state();
        // Bare `ON BCAST` registers the "match all" empty prefix.
        let registered = s.enable_tracking(bcast_req(&[])).unwrap();
        assert_eq!(registered, vec![Bytes::new()]);
        assert_eq!(s.tracking().prefixes, vec![Bytes::new()]);

        // Repeating it is idempotent (no duplicate, no overlap error) — Redis
        // skips the overlap check for the implicit empty prefix.
        let registered = s.enable_tracking(bcast_req(&[])).unwrap();
        assert_eq!(registered, vec![Bytes::new()]);
        assert_eq!(s.tracking().prefixes, vec![Bytes::new()]);

        // But an explicit prefix collides with the registered empty prefix.
        let err = s.enable_tracking(bcast_req(&[b"a:"])).unwrap_err();
        assert_eq!(
            err,
            TrackingEnableError::PrefixOverlapsExisting {
                new: Bytes::from_static(b"a:"),
                existing: Bytes::new(),
            }
        );

        // And — Redis quirk preserved — a bare `ON BCAST` after explicit
        // prefixes silently widens the registration to all keys.
        let mut s = state();
        s.enable_tracking(bcast_req(&[b"a:"])).unwrap();
        s.enable_tracking(bcast_req(&[])).unwrap();
        assert_eq!(
            s.tracking().prefixes,
            vec![Bytes::from_static(b"a:"), Bytes::new()]
        );
    }

    #[test]
    fn tracking_mode_switch_requires_off() {
        let mut s = state();
        s.enable_tracking(TrackingEnableRequest::default()).unwrap();
        // Non-BCAST -> BCAST while enabled: rejected.
        assert_eq!(
            s.enable_tracking(bcast_req(&[])).unwrap_err(),
            TrackingEnableError::BcastModeSwitch
        );

        // BCAST -> non-BCAST while enabled: rejected.
        let mut s = state();
        s.enable_tracking(bcast_req(&[b"a:"])).unwrap();
        assert_eq!(
            s.enable_tracking(TrackingEnableRequest::default())
                .unwrap_err(),
            TrackingEnableError::BcastModeSwitch
        );

        // OPTIN -> OPTOUT (and vice versa) while enabled: rejected.
        let mut s = state();
        s.enable_tracking(TrackingEnableRequest {
            optin: true,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            s.enable_tracking(TrackingEnableRequest {
                optout: true,
                ..Default::default()
            })
            .unwrap_err(),
            TrackingEnableError::OptinOptoutSwitch
        );

        // After OFF, switching is allowed again.
        assert!(s.disable_tracking());
        s.enable_tracking(TrackingEnableRequest {
            optout: true,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(s.tracking().mode, TrackingMode::OptOut);
    }

    #[test]
    fn tracking_flags_are_replaced_prefixes_are_not() {
        let mut s = state();
        s.enable_tracking(TrackingEnableRequest {
            noloop: true,
            redirect: 7,
            ..bcast_req(&[b"a:"])
        })
        .unwrap();
        // A second call without NOLOOP/REDIRECT clears them (Redis resets the
        // flag set on every enableTracking call) but keeps the prefix union.
        s.enable_tracking(bcast_req(&[b"b:"])).unwrap();
        assert!(!s.tracking().noloop, "NOLOOP not re-specified => cleared");
        assert_eq!(
            s.tracking().redirect,
            0,
            "REDIRECT not re-specified => cleared"
        );
        assert_eq!(
            s.tracking().prefixes,
            vec![Bytes::from_static(b"a:"), Bytes::from_static(b"b:")]
        );

        // OPTIN not re-specified on a later call => back to Default mode.
        let mut s = state();
        s.enable_tracking(TrackingEnableRequest {
            optin: true,
            ..Default::default()
        })
        .unwrap();
        s.enable_tracking(TrackingEnableRequest::default()).unwrap();
        assert_eq!(s.tracking().mode, TrackingMode::Default);
    }

    #[test]
    fn tracking_flag_combination_errors() {
        let mut s = state();
        assert_eq!(
            s.enable_tracking(TrackingEnableRequest {
                prefixes: vec![Bytes::from_static(b"a:")],
                ..Default::default()
            })
            .unwrap_err(),
            TrackingEnableError::PrefixRequiresBcast
        );
        assert_eq!(
            s.enable_tracking(TrackingEnableRequest {
                bcast: true,
                optin: true,
                ..Default::default()
            })
            .unwrap_err(),
            TrackingEnableError::OptinOptoutWithBcast
        );
        assert_eq!(
            s.enable_tracking(TrackingEnableRequest {
                optin: true,
                optout: true,
                ..Default::default()
            })
            .unwrap_err(),
            TrackingEnableError::OptinAndOptout
        );
        assert!(!s.tracking().enabled, "rejected calls never enable");
    }

    #[test]
    fn tracking_teardown_via_reset_equals_teardown_via_off() {
        // RESET and CLIENT TRACKING OFF must leave identical tracking state
        // (both fully reset it; the shard-side halves are equivalent too:
        // ConnectionClosed's tracking portion == TrackingUnregister).
        let enable = |s: &mut ConnectionState| {
            s.enable_tracking(TrackingEnableRequest {
                noloop: true,
                redirect: 9,
                ..bcast_req(&[b"a:"])
            })
            .unwrap();
            s.set_caching_override(true);
        };

        let mut via_off = state();
        enable(&mut via_off);
        assert!(via_off.disable_tracking());

        let mut via_reset = state();
        enable(&mut via_reset);
        let effects = via_reset.reset();
        assert!(effects.tracking_was_enabled);

        for s in [&mut via_off, &mut via_reset] {
            assert!(!s.tracking().enabled);
            assert_eq!(s.tracking().mode, TrackingMode::Default);
            assert!(!s.tracking().noloop);
            assert_eq!(s.tracking().caching_override, None);
            assert!(s.tracking().prefixes.is_empty());
            assert_eq!(s.tracking().redirect, 0);
            assert!(!s.should_track_read());
        }
    }

    // ---- RESET ------------------------------------------------------------

    // FM-TXN-014
    #[test]
    fn reset_clears_covered_state() {
        let mut s = state();
        s.add_subscription(SubKind::Channel, Bytes::from_static(b"c"));
        s.begin_transaction().unwrap();
        s.enable_tracking(TrackingEnableRequest::default()).unwrap();
        s.protocol_version = ProtocolVersion::Resp3;
        s.name = Some(Bytes::from_static(b"foo"));

        let effects = s.reset();
        assert!(effects.was_in_pubsub);
        assert!(effects.tracking_was_enabled);

        assert!(!s.in_pubsub_mode());
        assert!(!s.in_transaction());
        assert!(!s.tracking().enabled);
        assert!(matches!(s.protocol_version, ProtocolVersion::Resp2));
        assert!(s.name.is_none());
    }

    #[test]
    fn reset_clears_cluster_flags() {
        let mut s = state();
        s.set_asking();
        s.set_readonly(true);

        let _ = s.reset();

        // Redis `resetCommand` -> `clearClientConnectionState` clears
        // CLIENT_ASKING | CLIENT_READONLY.
        assert!(!s.take_asking());
        assert!(!s.is_readonly());
    }

    #[test]
    fn reset_restores_reply_mode_to_on() {
        // CLIENT REPLY OFF is cleared by RESET (Redis `resetCommand`).
        let mut s = state();
        s.reply_off();
        let _ = s.reset();
        assert_eq!(s.consume_reply_disposition(), ReplyDisposition::Send);

        // CLIENT REPLY SKIP latch is also cleared.
        let mut s = state();
        s.reply_skip_next();
        let _ = s.reset();
        assert_eq!(s.consume_reply_disposition(), ReplyDisposition::Send);
    }

    #[test]
    fn revert_to_default_user_toggles_authentication() {
        // authenticated=false → connection drops to unauthenticated.
        let mut s = state();
        assert!(s.is_authenticated(), "default connection is authenticated");
        s.revert_to_default_user(false);
        assert!(!s.is_authenticated());

        // authenticated=true → connection is the default authenticated user.
        s.revert_to_default_user(true);
        assert!(s.is_authenticated());
        assert_eq!(s.username(), "default");
    }
}
