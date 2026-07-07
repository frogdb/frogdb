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
    MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION, MAX_SUBSCRIPTIONS_PER_CONNECTION, shard_for_key,
    slot_for_key,
};
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};

use crate::slot_migration::redirect;

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

impl TransactionTarget {
    /// EXEC-time resolution. A `Multi` target means the transaction's keys are
    /// not co-located, so it rejects with the CROSSSLOT reply from the redirect
    /// seam (never a fresh literal); `None`/`Single` pass through for the caller
    /// to route.
    #[allow(clippy::result_large_err)]
    pub fn resolve(self) -> Result<TransactionTarget, Response> {
        match self {
            TransactionTarget::Multi(_) => Err(redirect::crossslot()),
            other => Ok(other),
        }
    }
}

/// Folds queued-command keys into a [`TransactionTarget`] during MULTI queuing,
/// promoting `None → Single → Multi`. Single owner of the transaction
/// co-location rule that once lived split across three ad-hoc spots
/// (`note_cluster_slot`, `add_transaction_shard`, and the WATCH loop). In
/// cluster mode a slot mismatch promotes to `Multi` (EXEC then returns
/// CROSSSLOT); in standalone mode a shard mismatch does.
#[derive(Debug, Default)]
struct TxnSlotAccumulator {
    /// First cluster slot seen (cluster mode only), for slot-level detection.
    /// Redis requires all keys in a MULTI/EXEC to hash to the same slot, which
    /// is stricter than shard-level detection.
    first_slot: Option<u16>,
    /// Shard(s) folded so far.
    target: TransactionTarget,
}

impl TxnSlotAccumulator {
    /// Fold one command's keys. `is_cluster` selects slot-level (cluster) vs
    /// shard-level (standalone) mismatch detection.
    fn add_keys<K: AsRef<[u8]>>(&mut self, keys: &[K], num_shards: usize, is_cluster: bool) {
        for key in keys {
            let shard = shard_for_key(key.as_ref(), num_shards);
            // In cluster mode a cross-slot key already forces `Multi`; skip the
            // normal per-shard fold for it.
            if is_cluster && self.note_slot(slot_for_key(key.as_ref()), shard) {
                continue;
            }
            self.fold_shard(shard);
        }
    }

    /// Fold a single already-resolved shard into the target (None → Single →
    /// Multi). Used for a same-shard-validated key set (WATCH).
    fn fold_shard(&mut self, shard_id: usize) {
        self.target = match &self.target {
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
    /// remembered; a later key in a different slot promotes the target to
    /// `Multi`. Returns `true` when this key was cross-slot, signalling
    /// [`add_keys`](Self::add_keys) to skip the normal per-shard fold.
    fn note_slot(&mut self, slot: u16, shard_id: usize) -> bool {
        match self.first_slot {
            None => {
                self.first_slot = Some(slot);
                false
            }
            Some(s) if s != slot => {
                self.target = match &self.target {
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
}

/// State for MULTI/EXEC transactions.
#[derive(Debug, Default)]
pub struct TransactionState {
    /// Queue of commands to execute at EXEC time (None = not in transaction).
    pub queue: Option<Vec<ParsedCommand>>,
    /// Watched keys: key -> (shard_id, version_at_watch_time).
    pub watches: HashMap<Bytes, (usize, u64)>,
    /// Co-location accumulator: folds queued keys (and watched shards) into the
    /// target shard(s), owning the `Multi`-promotion rule.
    slots: TxnSlotAccumulator,
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
    /// `set_asking`, read-and-clear via `take_asking`.
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

    /// Read-only view of the queued commands, if a transaction is open
    /// (for DEBUG MEMORY accounting).
    pub fn queued_commands(&self) -> Option<&[ParsedCommand]> {
        self.transaction.queue.as_deref()
    }

    /// Read-only iterator over watched keys (for DEBUG MEMORY accounting).
    pub fn watched_key_iter(&self) -> impl Iterator<Item = &Bytes> {
        self.transaction.watches.keys()
    }

    /// Begin a transaction (MULTI). Errors with [`TxnError::Nested`] if one is
    /// already open. Existing watches are preserved (WATCH before MULTI).
    pub fn begin_transaction(&mut self) -> Result<(), TxnError> {
        if self.transaction.queue.is_some() {
            return Err(TxnError::Nested);
        }
        self.transaction.queue = Some(Vec::new());
        self.transaction.slots = TxnSlotAccumulator::default();
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

    /// Fold one queued command's keys into the transaction target. In cluster
    /// mode a slot mismatch promotes the target to `Multi` (EXEC returns
    /// CROSSSLOT); in standalone mode a shard mismatch does. The
    /// [`TxnSlotAccumulator`] owns the rule.
    pub fn fold_transaction_keys<K: AsRef<[u8]>>(
        &mut self,
        keys: &[K],
        num_shards: usize,
        is_cluster: bool,
    ) {
        self.transaction
            .slots
            .add_keys(keys, num_shards, is_cluster);
    }

    /// Fold a single already-validated shard into the transaction target
    /// (WATCH's watched keys are all same-shard).
    pub fn fold_transaction_shard(&mut self, shard_id: usize) {
        self.transaction.slots.fold_shard(shard_id);
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
            target: txn.slots.target,
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
        s.fold_transaction_shard(2);
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

    // ---- TxnSlotAccumulator (transaction co-location owner) --------------

    #[test]
    fn accumulator_shard_fold_none_single_multi() {
        let mut acc = TxnSlotAccumulator::default();
        assert!(matches!(acc.target, TransactionTarget::None));
        acc.fold_shard(1);
        assert!(matches!(acc.target, TransactionTarget::Single(1)));
        // Re-folding the same shard stays Single.
        acc.fold_shard(1);
        assert!(matches!(acc.target, TransactionTarget::Single(1)));
        // A second shard promotes to Multi.
        acc.fold_shard(2);
        assert!(matches!(acc.target, TransactionTarget::Multi(_)));
    }

    #[test]
    fn accumulator_cluster_slot_mismatch_forces_multi() {
        let mut acc = TxnSlotAccumulator::default();
        // First key establishes the slot; not cross-slot.
        assert!(!acc.note_slot(100, 0));
        acc.fold_shard(0);
        // A different slot forces Multi and signals the caller to skip the fold.
        assert!(acc.note_slot(200, 1));
        assert!(matches!(acc.target, TransactionTarget::Multi(_)));
    }

    #[test]
    fn accumulator_same_slot_stays_single() {
        let mut acc = TxnSlotAccumulator::default();
        assert!(!acc.note_slot(100, 3));
        acc.fold_shard(3);
        // Same slot again: not cross-slot, stays Single.
        assert!(!acc.note_slot(100, 3));
        acc.fold_shard(3);
        assert!(matches!(acc.target, TransactionTarget::Single(3)));
    }

    #[test]
    fn transaction_target_resolve_maps_multi_to_crossslot() {
        assert!(TransactionTarget::None.resolve().is_ok());
        assert!(TransactionTarget::Single(3).resolve().is_ok());
        let err = TransactionTarget::Multi(vec![0, 1]).resolve().unwrap_err();
        // The rejection is byte-identical to the redirect seam.
        assert_eq!(format!("{err:?}"), format!("{:?}", redirect::crossslot()));
    }

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
        use frogdb_core::UserPermissions;

        // A connection that requires auth starts unauthenticated.
        let mut s = ConnectionState::new(1, "127.0.0.1:0".parse().unwrap(), true);
        assert!(!s.is_authenticated());
        assert!(s.authenticated_user().is_none());
        assert_eq!(s.username(), "(not authenticated)");

        let user = AuthenticatedUser::new("alice", UserPermissions::allow_all(), None);
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
