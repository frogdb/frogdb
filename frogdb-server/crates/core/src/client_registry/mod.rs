//! Client registry for tracking all connected clients.
//!
//! This module provides a global registry of all connected clients, enabling:
//! - CLIENT LIST: List all connected clients
//! - CLIENT KILL: Terminate connections
//! - CLIENT PAUSE: Pause client command execution
//! - CLIENT ID/SETNAME/GETNAME/INFO: Per-client introspection
//! - CLIENT STATS: Per-client command statistics

mod info;
mod stats;

pub use info::*;
pub use stats::*;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use bitflags::bitflags;
use bytes::Bytes;
use frogdb_replication::ReplicaFeedGate;
use tokio::sync::watch;

use crate::sync::{Arc, RwLock};

/// Maximum distinct error prefixes tracked (matches Redis 7.x cap of 128).
const MAX_ERROR_TYPES: usize = 128;

/// Server-wide error statistics.
///
/// Tracks rejected calls (before execution), failed calls (during execution),
/// and per-error-prefix counts for the INFO errorstats section.
#[derive(Debug, Default)]
pub struct ErrorStats {
    /// Total error replies sent (rejected + failed).
    pub total_error_replies: AtomicU64,
    /// Commands rejected before execution.
    pub rejected_calls: AtomicU64,
    /// Commands that failed during execution.
    pub failed_calls: AtomicU64,
    /// Maps error prefix (e.g., "ERR", "WRONGTYPE") to occurrence count.
    error_type_counts: RwLock<HashMap<String, u64>>,
}

impl ErrorStats {
    /// Create a new ErrorStats instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a rejected command error.
    pub fn record_rejected(&self, error_prefix: &str) {
        self.rejected_calls.fetch_add(1, Ordering::Relaxed);
        self.total_error_replies.fetch_add(1, Ordering::Relaxed);
        self.record_error_type(error_prefix);
    }

    /// Record a failed command error.
    pub fn record_failed(&self, error_prefix: &str) {
        self.failed_calls.fetch_add(1, Ordering::Relaxed);
        self.total_error_replies.fetch_add(1, Ordering::Relaxed);
        self.record_error_type(error_prefix);
    }

    /// Increment the per-prefix counter (capped at MAX_ERROR_TYPES).
    fn record_error_type(&self, prefix: &str) {
        let mut map = self.error_type_counts.write().unwrap();
        if let Some(count) = map.get_mut(prefix) {
            *count += 1;
        } else if map.len() < MAX_ERROR_TYPES {
            map.insert(prefix.to_string(), 1);
        }
        // else: silently drop (cap reached)
    }

    /// Snapshot of per-prefix error counts for INFO output.
    pub fn error_type_snapshot(&self) -> HashMap<String, u64> {
        self.error_type_counts.read().unwrap().clone()
    }

    /// Reset all error stats (CONFIG RESETSTAT).
    pub fn reset(&self) {
        self.total_error_replies.store(0, Ordering::Relaxed);
        self.rejected_calls.store(0, Ordering::Relaxed);
        self.failed_calls.store(0, Ordering::Relaxed);
        self.error_type_counts.write().unwrap().clear();
    }
}

/// Extract error prefix from a RESP error message.
///
/// "ERR wrong number..." -> "ERR"
/// "WRONGTYPE Operation..." -> "WRONGTYPE"
/// "NOSCRIPT No matching..." -> "NOSCRIPT"
pub fn extract_error_prefix(error_bytes: &[u8]) -> &str {
    let s = std::str::from_utf8(error_bytes).unwrap_or("ERR");
    s.split_once(' ').map(|(prefix, _)| prefix).unwrap_or(s)
}

/// Per-command server-wide statistics (calls, usec, rejected, failed).
#[derive(Debug, Clone, Default)]
pub struct ServerCommandStats {
    pub calls: u64,
    pub usec: u64,
    pub rejected_calls: u64,
    pub failed_calls: u64,
}

bitflags! {
    /// Client connection flags indicating current state.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct ClientFlags: u32 {
        /// No special flags.
        const NONE = 0;
        /// Client is in MULTI/EXEC transaction.
        const MULTI = 1 << 0;
        /// Client is blocked on BLPOP/BRPOP etc.
        const BLOCKED = 1 << 1;
        /// Client is in pub/sub mode.
        const PUBSUB = 1 << 2;
        /// Client is a master (replication).
        const MASTER = 1 << 3;
        /// Client is a replica (replication).
        const REPLICA = 1 << 4;
        /// Client is protected from eviction.
        const NO_EVICT = 1 << 5;
        /// Client's accesses don't update LRU time.
        const NO_TOUCH = 1 << 6;
        /// Client is blocked by CLIENT PAUSE.
        const PAUSED = 1 << 7;
    }
}

impl ClientFlags {
    /// Convert flags to Redis-style flag string.
    pub fn to_flag_string(&self) -> String {
        let mut flags = String::new();
        if self.is_empty() {
            flags.push('N'); // Normal
        } else {
            if self.contains(ClientFlags::MULTI) {
                flags.push('x'); // multi/exec context
            }
            if self.contains(ClientFlags::BLOCKED) {
                flags.push('b'); // blocked
            }
            if self.contains(ClientFlags::PUBSUB) {
                flags.push('P'); // pubsub
            }
            if self.contains(ClientFlags::MASTER) {
                flags.push('M'); // master
            }
            if self.contains(ClientFlags::REPLICA) {
                flags.push('S'); // replica/slave
            }
            if self.contains(ClientFlags::NO_EVICT) {
                flags.push('e'); // no-evict
            }
            if self.contains(ClientFlags::NO_TOUCH) {
                flags.push('T'); // no-touch
            }
        }
        if flags.is_empty() {
            flags.push('N');
        }
        flags
    }
}

/// Pause mode for CLIENT PAUSE command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PauseMode {
    /// Block all commands.
    All,
    /// Block only write commands.
    Write,
}

impl PauseMode {
    /// The stronger of two optional modes: `All` beats `Write` beats nothing.
    ///
    /// Used both when two pauses overlap in time (Redis never downgrades an
    /// active pause) and when a node-global pause and a slot-scoped one both
    /// cover the same command.
    pub fn strongest(a: Option<Self>, b: Option<Self>) -> Option<Self> {
        match (a, b) {
            (Some(Self::All), _) | (_, Some(Self::All)) => Some(Self::All),
            (Some(Self::Write), _) | (_, Some(Self::Write)) => Some(Self::Write),
            (None, None) => None,
        }
    }
}

/// One armed pause: what it blocks and when it lapses.
#[derive(Debug, Clone, Copy)]
struct PauseEntry {
    /// What this pause blocks.
    mode: PauseMode,
    /// When it lapses. Always set — `CLIENT PAUSE` and the slot barrier both
    /// take a timeout, and an unbounded pause has no way back if its arming
    /// party dies.
    unpause_at: Instant,
}

impl PauseEntry {
    /// Whether this pause is still in force at `now`.
    fn active(&self, now: Instant) -> bool {
        now < self.unpause_at
    }

    /// Fold a newly armed pause into whatever is already there, per Redis'
    /// overlapping-pause rules: never downgrade the mode, never shorten the
    /// deadline — but only against a pause that has not already lapsed.
    fn arm(existing: Option<Self>, mode: PauseMode, unpause_at: Instant, now: Instant) -> Self {
        match existing.filter(|e| e.active(now)) {
            Some(live) => Self {
                mode: PauseMode::strongest(Some(live.mode), Some(mode))
                    .expect("both operands are Some"),
                unpause_at: live.unpause_at.max(unpause_at),
            },
            None => Self { mode, unpause_at },
        }
    }
}

/// Pause state for the client registry.
///
/// Two independent dimensions, deliberately not merged into one entry:
///
/// - `node` is the operator's `CLIENT PAUSE` — node-global, Redis semantics.
/// - `slots` are slot-scoped pauses, the machinery the slot-migration
///   finalization barrier arms on the source node. A slot-scoped pause parks
///   only commands whose keys hash to that slot.
///
/// Keeping them apart is what lets `CLIENT UNPAUSE` clear the operator's pause
/// without silently disarming a migration barrier, and lets the barrier release
/// its slot without lifting an operator pause that is still meant to be running
/// (the composition requirement in the pause-barrier brief).
#[derive(Debug, Default)]
struct PauseState {
    /// The node-global pause (`CLIENT PAUSE`), if one is armed.
    node: Option<PauseEntry>,
    /// Slot-scoped pauses, keyed by CRC16 hash slot. Composed per slot, so two
    /// concurrent slot finalizations never contend.
    slots: HashMap<u16, PauseEntry>,
}

impl PauseState {
    /// Whether any armed pause has lapsed and should be swept.
    fn has_lapsed(&self, now: Instant) -> bool {
        self.node.is_some_and(|e| !e.active(now)) || self.slots.values().any(|e| !e.active(now))
    }

    /// Drop every lapsed pause.
    fn sweep(&mut self, now: Instant) {
        self.node = self.node.filter(|e| e.active(now));
        self.slots.retain(|_, e| e.active(now));
    }

    /// Whether anything at all is paused.
    fn is_idle(&self) -> bool {
        self.node.is_none() && self.slots.is_empty()
    }

    /// When the primary's replica feed may ship again, or `None` when nothing
    /// is holding it (FM-CLUSTER-097).
    ///
    /// Only slot-scoped pauses hold the feed. They are armed by exactly one
    /// thing — the slot-handoff barrier — and the barrier is the only pause
    /// whose fenced writes can still *apply* locally (FM-CLUSTER-095), which is
    /// what makes shipping them during the handover window an anomaly. A
    /// node-global `CLIENT PAUSE` stops the writes themselves, so there is
    /// nothing new to ship and no reason to stall a replica behind it — Redis
    /// likewise reserves `PAUSE_ACTION_REPLICA` for the migration pause.
    ///
    /// The latest deadline across armed slots, so two overlapping handoffs
    /// compose the same way the pauses themselves do (never shorten).
    fn feed_hold_until(&self) -> Option<Instant> {
        frogdb_replication::feed_gate::decide_feed_hold_until(
            self.slots.values().map(|e| e.unpause_at),
        )
    }
}

/// What is paused right now, read under a single lock.
///
/// The overwhelmingly common answer is "nothing", which is why the pause gate
/// asks this question first: only a `slot_scoped` answer makes it worth
/// resolving the command's keys to a hash slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PauseOverview {
    /// The node-global pause mode, if one is in force.
    pub node: Option<PauseMode>,
    /// Whether at least one slot-scoped pause is in force.
    pub slot_scoped: bool,
}

impl PauseOverview {
    /// Whether any pause — node-global or slot-scoped — is in force.
    pub fn is_active(&self) -> bool {
        self.node.is_some() || self.slot_scoped
    }
}

/// Unblock mode for CLIENT UNBLOCK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnblockMode {
    /// Return nil/timeout response.
    Timeout,
    /// Return error response.
    Error,
}

/// Per-client memory usage breakdown, updated by the connection handler.
#[derive(Debug, Clone, Default)]
pub struct ClientMemoryUsage {
    /// Query buffer size (bytes in codec read buffer).
    pub query_buf_size: usize,
    /// High-water mark of `query_buf_size`, sampled at each memory sync.
    /// Backs CLIENT INFO/LIST's `rbp` field.
    pub query_buf_peak: usize,
    /// Argv memory (parsed command args, transient during execution).
    pub argv_mem: usize,
    /// Multi buffer memory (serialized size of queued MULTI commands).
    pub multi_mem: usize,
    /// Output buffer length (bytes in write buffer + resp3_buf).
    pub output_buf_len: usize,
    /// Output list length (pending pub/sub + invalidation messages).
    pub output_list_len: usize,
    /// Output list memory (estimated bytes in output list).
    pub output_list_mem: usize,
    /// Watched keys memory.
    pub watched_keys_mem: usize,
    /// Subscriptions memory (channels + patterns + sharded).
    pub subscriptions_mem: usize,
    /// Tracking prefixes memory.
    pub tracking_prefixes_mem: usize,
}

/// Fixed per-connection overhead estimate (struct sizes, channels, etc.).
/// This is a rough estimate of the memory used by ConnectionHandler +
/// ConnectionState + channels + codec that exists regardless of data.
const CLIENT_BASE_OVERHEAD: usize = 4096;

impl ClientMemoryUsage {
    /// Compute total client memory including fixed overhead.
    pub fn total(&self) -> usize {
        CLIENT_BASE_OVERHEAD
            + self.query_buf_size
            + self.argv_mem
            + self.multi_mem
            + self.output_buf_len
            // output_list_len is a count (oll), not bytes; omem has the byte total
            + self.output_list_mem
            + self.watched_keys_mem
            + self.subscriptions_mem
            + self.tracking_prefixes_mem
    }
}

/// Internal entry for a registered client.
struct ClientEntry {
    /// Remote client address.
    addr: SocketAddr,
    /// Local server address.
    local_addr: Option<SocketAddr>,
    /// Client name (from CLIENT SETNAME).
    name: Option<Bytes>,
    /// When the connection was created.
    created_at: Instant,
    /// When the last command was executed.
    last_command_at: Instant,
    /// Current client flags.
    flags: ClientFlags,
    /// Number of channel subscriptions.
    sub_count: usize,
    /// Number of pattern subscriptions.
    psub_count: usize,
    /// Number of sharded subscriptions.
    ssub_count: usize,
    /// Whether client is in MULTI/EXEC.
    in_multi: bool,
    /// Number of commands queued in MULTI.
    multi_queue_len: usize,
    /// Number of keys currently watched (WATCH). Backs CLIENT INFO/LIST's
    /// `watch` field.
    watch_count: usize,
    /// Watch channel sender for kill signal (true = killed).
    kill_tx: watch::Sender<bool>,
    /// Watch channel sender for unblock signal (Some = unblocked, with mode).
    unblock_tx: watch::Sender<Option<UnblockMode>>,
    /// Library name (from CLIENT SETINFO).
    lib_name: Option<Bytes>,
    /// Library version (from CLIENT SETINFO).
    lib_ver: Option<Bytes>,
    /// Per-client statistics.
    stats: ClientStats,
    /// Currently executing command (e.g. "client|list").
    current_cmd: Option<String>,
    /// Per-client memory usage breakdown.
    memory: ClientMemoryUsage,
}

impl ClientEntry {
    /// Set or clear a single flag bit based on a bool.
    ///
    /// Pure — takes no lock, just a bitwise op on `self.flags`.
    fn set_flag(&mut self, flag: ClientFlags, on: bool) {
        if on {
            self.flags |= flag;
        } else {
            self.flags.remove(flag);
        }
    }

    /// Update subscription counts and derive the PUBSUB flag from them.
    ///
    /// PUBSUB is set whenever any subscription count is non-zero, and
    /// cleared once all three drop to zero.
    fn set_subscriptions(&mut self, sub_count: usize, psub_count: usize, ssub_count: usize) {
        self.sub_count = sub_count;
        self.psub_count = psub_count;
        self.ssub_count = ssub_count;
        self.set_flag(
            ClientFlags::PUBSUB,
            sub_count > 0 || psub_count > 0 || ssub_count > 0,
        );
    }

    /// Update the BLOCKED flag (client waiting on BLPOP/BRPOP/etc).
    fn set_blocked(&mut self, blocked: bool) {
        self.set_flag(ClientFlags::BLOCKED, blocked);
    }

    /// Update the PAUSED flag (client blocked by CLIENT PAUSE).
    fn set_paused(&mut self, paused: bool) {
        self.set_flag(ClientFlags::PAUSED, paused);
    }

    /// Update MULTI/EXEC state and derive the MULTI flag from it.
    fn set_multi(&mut self, in_multi: bool, queue_len: usize) {
        self.in_multi = in_multi;
        self.multi_queue_len = queue_len;
        self.set_flag(ClientFlags::MULTI, in_multi);
    }

    /// Build a bare-bones entry for unit-testing the pure flag-derivation
    /// helpers above, without registering a client or taking any lock.
    #[cfg(test)]
    fn for_test() -> Self {
        let (kill_tx, _kill_rx) = watch::channel(false);
        let (unblock_tx, _unblock_rx) = watch::channel(None);
        let now = Instant::now();
        ClientEntry {
            addr: SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0),
            local_addr: None,
            name: None,
            created_at: now,
            last_command_at: now,
            flags: ClientFlags::NONE,
            sub_count: 0,
            psub_count: 0,
            ssub_count: 0,
            in_multi: false,
            multi_queue_len: 0,
            watch_count: 0,
            kill_tx,
            unblock_tx,
            lib_name: None,
            lib_ver: None,
            stats: ClientStats::default(),
            current_cmd: None,
            memory: ClientMemoryUsage::default(),
        }
    }
}

/// Handle for a registered client, auto-unregisters on drop.
pub struct ClientHandle {
    id: u64,
    registry: Arc<ClientRegistry>,
    kill_rx: watch::Receiver<bool>,
    unblock_rx: watch::Receiver<Option<UnblockMode>>,
}

impl ClientHandle {
    /// Get the connection ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Check if this connection has been killed.
    pub fn is_killed(&self) -> bool {
        *self.kill_rx.borrow()
    }

    /// Wait until the connection is killed.
    /// Returns when CLIENT KILL is called for this connection.
    pub async fn killed(&mut self) {
        // Wait for the value to become true
        loop {
            if *self.kill_rx.borrow() {
                return;
            }
            // Wait for change
            if self.kill_rx.changed().await.is_err() {
                // Channel closed, treat as killed
                return;
            }
        }
    }

    /// Check if an unblock was requested.
    /// Returns Some(mode) if unblocked, None otherwise.
    pub fn check_unblock(&self) -> Option<UnblockMode> {
        *self.unblock_rx.borrow()
    }

    /// Wait until client is unblocked.
    /// Returns the unblock mode when CLIENT UNBLOCK is called.
    pub async fn unblocked(&mut self) -> Option<UnblockMode> {
        loop {
            if let Some(mode) = *self.unblock_rx.borrow() {
                return Some(mode);
            }
            // Wait for change
            if self.unblock_rx.changed().await.is_err() {
                // Channel closed
                return None;
            }
        }
    }

    /// Wait for whichever admin edge — `CLIENT KILL` or `CLIENT UNBLOCK` —
    /// reaches this connection first.
    ///
    /// Both edges live on this one handle, so a caller that wants to race them
    /// cannot borrow it twice; this method owns the race instead, splitting the
    /// borrow across the two watch receivers internally. `biased;` puts the kill
    /// ahead of the unblock for the same reason the connection run loop does
    /// (`connection.rs`): a kill is rare and terminal, and must not lose a tie
    /// to a signal the connection would survive.
    ///
    /// Used by the blocking-wait coordinator so a parked client stays killable
    /// (`specs/blocking.md` TR-BLOCKING-021).
    pub async fn killed_or_unblocked(&mut self) -> ClientEdge {
        // A killed connection is killed for good — the flag is level-triggered
        // on a `watch<bool>`, never cleared — so observing it here does not
        // consume the edge the run loop's own `killed()` branch reads.
        let Self {
            kill_rx,
            unblock_rx,
            ..
        } = self;

        let killed = async {
            loop {
                if *kill_rx.borrow() {
                    return;
                }
                if kill_rx.changed().await.is_err() {
                    // Channel closed, treat as killed (same as `killed()`).
                    return;
                }
            }
        };
        let unblocked = async {
            loop {
                if let Some(mode) = *unblock_rx.borrow() {
                    return Some(mode);
                }
                if unblock_rx.changed().await.is_err() {
                    return None;
                }
            }
        };

        tokio::pin!(killed, unblocked);
        tokio::select! {
            biased;
            () = &mut killed => ClientEdge::Killed,
            mode = &mut unblocked => ClientEdge::Unblocked(mode),
        }
    }
}

/// Which admin edge released a parked client: see
/// [`ClientHandle::killed_or_unblocked`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientEdge {
    /// `CLIENT KILL` targeted this connection; it is terminal.
    Killed,
    /// `CLIENT UNBLOCK` targeted this connection, with the requested mode.
    /// `None` means the signal channel closed.
    Unblocked(Option<UnblockMode>),
}

impl Drop for ClientHandle {
    fn drop(&mut self) {
        self.registry.unregister(self.id);
    }
}

/// Global registry of all connected clients.
pub struct ClientRegistry {
    /// Map of connection ID to client entry.
    clients: RwLock<HashMap<u64, ClientEntry>>,
    /// Pause state: the node-global `CLIENT PAUSE` plus any slot-scoped pauses.
    pause_state: RwLock<PauseState>,
    /// Whether active key expiry should be paused (true while any pause is armed).
    expiry_paused: Arc<AtomicBool>,
    /// The replication half of the slot-handoff barrier: holds the primary's
    /// replica feed while a slot-scoped pause is armed (FM-CLUSTER-097).
    /// Republished from [`PauseState`] alongside `expiry_paused`, so the write
    /// barrier and the feed hold are two renderings of one fact.
    replica_feed_gate: Arc<ReplicaFeedGate>,
    /// Server-wide per-command statistics (lowercase command name → stats).
    ///
    /// Updated inside `update_stats` from each connection's
    /// `ClientStatsDelta::command_latencies`. Used by `INFO commandstats` to
    /// emit per-command `cmdstat_<name>:calls=N,...` lines.
    command_stats: RwLock<HashMap<String, ServerCommandStats>>,
    /// Server-wide error statistics (rejected, failed, per-prefix counts).
    pub error_stats: Arc<ErrorStats>,
}

impl Default for ClientRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientRegistry {
    /// Create a new client registry.
    pub fn new() -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
            pause_state: RwLock::new(PauseState::default()),
            expiry_paused: Arc::new(AtomicBool::new(false)),
            replica_feed_gate: ReplicaFeedGate::open(),
            command_stats: RwLock::new(HashMap::new()),
            error_stats: Arc::new(ErrorStats::new()),
        }
    }

    /// Get a shared handle to the expiry_paused flag.
    ///
    /// Shard workers check this flag to skip active expiry during CLIENT PAUSE
    /// (both ALL and WRITE modes).
    pub fn expiry_paused_flag(&self) -> Arc<AtomicBool> {
        self.expiry_paused.clone()
    }

    /// Get a shared handle to the replica-feed hold.
    ///
    /// Handed to the primary replication handler at boot; its streaming
    /// sessions keep frames off the wire while the hold is in force. Vended
    /// exactly like [`expiry_paused_flag`](Self::expiry_paused_flag) — the
    /// registry stays the one writer, and the consumer only reads.
    pub fn replica_feed_gate(&self) -> Arc<ReplicaFeedGate> {
        self.replica_feed_gate.clone()
    }

    /// Register a new client connection.
    pub fn register(
        self: &Arc<Self>,
        id: u64,
        addr: SocketAddr,
        local_addr: Option<SocketAddr>,
    ) -> ClientHandle {
        let now = crate::clock::now();
        let (kill_tx, kill_rx) = watch::channel(false);
        let (unblock_tx, unblock_rx) = watch::channel(None);

        let entry = ClientEntry {
            addr,
            local_addr,
            name: None,
            created_at: now,
            last_command_at: now,
            flags: ClientFlags::NONE,
            sub_count: 0,
            psub_count: 0,
            ssub_count: 0,
            in_multi: false,
            multi_queue_len: 0,
            watch_count: 0,
            kill_tx,
            unblock_tx,
            lib_name: None,
            lib_ver: None,
            stats: ClientStats::default(),
            current_cmd: None,
            memory: ClientMemoryUsage::default(),
        };

        {
            let mut clients = self.clients.write().unwrap();
            clients.insert(id, entry);
        }

        ClientHandle {
            id,
            registry: Arc::clone(self),
            kill_rx,
            unblock_rx,
        }
    }

    /// Unregister a client connection.
    fn unregister(&self, id: u64) {
        let mut clients = self.clients.write().unwrap();
        clients.remove(&id);
    }

    /// Take the write lock, look up `id`, and apply `f` to its entry.
    ///
    /// Returns `None` if the client is not registered (e.g. it disconnected
    /// concurrently) — every setter tolerates a missing id as a silent no-op,
    /// matching prior behavior.
    fn with_client_mut<R>(&self, id: u64, f: impl FnOnce(&mut ClientEntry) -> R) -> Option<R> {
        let mut clients = self.clients.write().unwrap();
        clients.get_mut(&id).map(f)
    }

    /// Get information about all clients.
    pub fn list(&self) -> Vec<ClientInfo> {
        let clients = self.clients.read().unwrap();
        clients
            .iter()
            .map(|(&id, entry)| ClientInfo {
                id,
                addr: entry.addr,
                local_addr: entry.local_addr,
                name: entry.name.clone(),
                created_at: entry.created_at,
                last_command_at: entry.last_command_at,
                flags: entry.flags,
                sub_count: entry.sub_count,
                psub_count: entry.psub_count,
                ssub_count: entry.ssub_count,
                in_multi: entry.in_multi,
                multi_queue_len: entry.multi_queue_len,
                watch_count: entry.watch_count,
                lib_name: entry.lib_name.clone(),
                lib_ver: entry.lib_ver.clone(),
                stats: Some(entry.stats.clone()),
                current_cmd: entry.current_cmd.clone(),
                memory: entry.memory.clone(),
            })
            .collect()
    }

    /// Get information about a specific client.
    pub fn get(&self, id: u64) -> Option<ClientInfo> {
        let clients = self.clients.read().unwrap();
        clients.get(&id).map(|entry| ClientInfo {
            id,
            addr: entry.addr,
            local_addr: entry.local_addr,
            name: entry.name.clone(),
            created_at: entry.created_at,
            last_command_at: entry.last_command_at,
            flags: entry.flags,
            sub_count: entry.sub_count,
            psub_count: entry.psub_count,
            ssub_count: entry.ssub_count,
            in_multi: entry.in_multi,
            multi_queue_len: entry.multi_queue_len,
            watch_count: entry.watch_count,
            lib_name: entry.lib_name.clone(),
            lib_ver: entry.lib_ver.clone(),
            stats: Some(entry.stats.clone()),
            current_cmd: entry.current_cmd.clone(),
            memory: entry.memory.clone(),
        })
    }

    /// Kill a client by ID.
    pub fn kill_by_id(&self, id: u64) -> bool {
        let clients = self.clients.read().unwrap();
        if let Some(entry) = clients.get(&id) {
            let _ = entry.kill_tx.send(true);
            true
        } else {
            false
        }
    }

    /// Unblock a blocked client by ID.
    /// Returns true if the client exists and was signaled, false if client not found.
    /// Returns false if the client is blocked by CLIENT PAUSE (cannot be externally unblocked).
    pub fn unblock(&self, id: u64, mode: UnblockMode) -> bool {
        let clients = self.clients.read().unwrap();
        if let Some(entry) = clients.get(&id) {
            // Clients blocked by CLIENT PAUSE cannot be unblocked via CLIENT UNBLOCK
            if entry.flags.contains(ClientFlags::PAUSED) {
                return false;
            }
            // Check if client is actually blocked
            if entry.flags.contains(ClientFlags::BLOCKED) {
                let _ = entry.unblock_tx.send(Some(mode));
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Reset the unblock signal for a client.
    pub fn reset_unblock(&self, id: u64) {
        let clients = self.clients.read().unwrap();
        if let Some(entry) = clients.get(&id) {
            let _ = entry.unblock_tx.send(None);
        }
    }

    /// Kill clients matching a filter.
    pub fn kill_by_filter(&self, filter: &KillFilter) -> usize {
        let clients = self.clients.read().unwrap();
        let mut killed = 0;

        for (&id, entry) in clients.iter() {
            let info = ClientInfo {
                id,
                addr: entry.addr,
                local_addr: entry.local_addr,
                name: entry.name.clone(),
                created_at: entry.created_at,
                last_command_at: entry.last_command_at,
                flags: entry.flags,
                sub_count: entry.sub_count,
                psub_count: entry.psub_count,
                ssub_count: entry.ssub_count,
                in_multi: entry.in_multi,
                multi_queue_len: entry.multi_queue_len,
                watch_count: entry.watch_count,
                lib_name: entry.lib_name.clone(),
                lib_ver: entry.lib_ver.clone(),
                stats: None,
                current_cmd: entry.current_cmd.clone(),
                memory: entry.memory.clone(),
            };

            if filter.matches(id, &info) {
                let _ = entry.kill_tx.send(true);
                killed += 1;
            }
        }

        killed
    }

    /// Update a client's name.
    pub fn update_name(&self, id: u64, name: Option<Bytes>) {
        self.with_client_mut(id, |entry| entry.name = name);
    }

    /// Update a client's library info.
    pub fn update_lib_info(&self, id: u64, lib_name: Option<Bytes>, lib_ver: Option<Bytes>) {
        self.with_client_mut(id, |entry| {
            if lib_name.is_some() {
                entry.lib_name = lib_name;
            }
            if lib_ver.is_some() {
                entry.lib_ver = lib_ver;
            }
        });
    }

    /// Update a client's last command time.
    pub fn update_last_command(&self, id: u64) {
        self.with_client_mut(id, |entry| entry.last_command_at = crate::clock::now());
    }

    /// Update a client's last command time with a pre-captured instant.
    pub fn update_last_command_at(&self, id: u64, time: Instant) {
        self.with_client_mut(id, |entry| entry.last_command_at = time);
    }

    /// Update the currently executing command for a client.
    pub fn update_current_cmd(&self, id: u64, cmd: Option<String>) {
        self.with_client_mut(id, |entry| entry.current_cmd = cmd);
    }

    /// Update client flags.
    pub fn update_flags(&self, id: u64, flags: ClientFlags) {
        self.with_client_mut(id, |entry| entry.flags = flags);
    }

    /// Update pub/sub subscription counts.
    pub fn update_subscriptions(
        &self,
        id: u64,
        sub_count: usize,
        psub_count: usize,
        ssub_count: usize,
    ) {
        self.with_client_mut(id, |entry| {
            entry.set_subscriptions(sub_count, psub_count, ssub_count)
        });
    }

    /// Update blocked state for a client.
    pub fn update_blocked_state(&self, id: u64, blocked: bool) {
        self.with_client_mut(id, |entry| entry.set_blocked(blocked));
    }

    /// Update paused state for a client (blocked by CLIENT PAUSE).
    pub fn update_paused_state(&self, id: u64, paused: bool) {
        self.with_client_mut(id, |entry| entry.set_paused(paused));
    }

    /// Count the number of currently blocked clients (BLOCKED or PAUSED).
    pub fn blocked_client_count(&self) -> usize {
        let clients = self.clients.read().unwrap();
        clients
            .values()
            .filter(|e| {
                e.flags.contains(ClientFlags::BLOCKED) || e.flags.contains(ClientFlags::PAUSED)
            })
            .count()
    }

    /// Update MULTI/EXEC state.
    pub fn update_multi_state(&self, id: u64, in_multi: bool, queue_len: usize) {
        self.with_client_mut(id, |entry| entry.set_multi(in_multi, queue_len));
    }

    /// Update the number of keys currently watched (WATCH). Synced
    /// periodically alongside memory usage — see
    /// [`crate::client_registry::ClientMemoryUsage`] and CLIENT INFO/LIST's
    /// `watch` field.
    pub fn update_watch_count(&self, id: u64, watch_count: usize) {
        self.with_client_mut(id, |entry| entry.watch_count = watch_count);
    }

    /// Arm the node-global pause (`CLIENT PAUSE`).
    ///
    /// Follows Redis semantics for overlapping pauses:
    /// - Mode precedence: ALL takes priority over WRITE (never downgrade).
    /// - Time preservation: the maximum of old and new end times is kept.
    pub fn pause(&self, mode: PauseMode, timeout_ms: u64) {
        let now = crate::clock::now();
        let unpause_at = now + std::time::Duration::from_millis(timeout_ms);
        let mut pause_state = self.pause_state.write().unwrap();
        pause_state.node = Some(PauseEntry::arm(pause_state.node, mode, unpause_at, now));
        self.publish_pause_derived_state(&pause_state);
    }

    /// Arm a pause scoped to one CRC16 hash slot.
    ///
    /// This is the slot-migration finalization barrier's half of the pause
    /// machinery: it parks only commands whose keys hash to `slot`, so the
    /// catch-up `MIGRATE` and the `CLUSTER SETSLOT` control plane keep running
    /// on the very node the barrier is armed on. Overlapping arms of the *same*
    /// slot fold with the same never-downgrade / never-shorten rule as
    /// [`pause`](Self::pause); different slots compose independently.
    pub fn pause_slot(&self, slot: u16, mode: PauseMode, timeout_ms: u64) {
        let now = crate::clock::now();
        let unpause_at = now + std::time::Duration::from_millis(timeout_ms);
        let mut pause_state = self.pause_state.write().unwrap();
        let armed = PauseEntry::arm(pause_state.slots.get(&slot).copied(), mode, unpause_at, now);
        pause_state.slots.insert(slot, armed);
        self.publish_pause_derived_state(&pause_state);
    }

    /// Clear the node-global pause (`CLIENT UNPAUSE`).
    ///
    /// Deliberately leaves slot-scoped pauses armed: an operator lifting their
    /// own pause must not disarm a migration barrier that is holding a slot's
    /// writes back across an ownership handover. A barrier is released by
    /// [`unpause_slot`](Self::unpause_slot) or by its own deadline.
    pub fn unpause(&self) {
        let mut pause_state = self.pause_state.write().unwrap();
        pause_state.node = None;
        self.publish_pause_derived_state(&pause_state);
    }

    /// Release the pause on one hash slot, leaving every other pause — the
    /// node-global one included — exactly as it was.
    pub fn unpause_slot(&self, slot: u16) {
        let mut pause_state = self.pause_state.write().unwrap();
        pause_state.slots.remove(&slot);
        self.publish_pause_derived_state(&pause_state);
    }

    /// What is paused right now: the node-global mode plus whether any
    /// slot-scoped pause is in force. Lapsed pauses are swept first, so a
    /// deadline that has passed never reads as active.
    pub fn pause_overview(&self) -> PauseOverview {
        self.sweep_lapsed_pauses();
        let pause_state = self.pause_state.read().unwrap();
        PauseOverview {
            node: pause_state.node.map(|e| e.mode),
            slot_scoped: !pause_state.slots.is_empty(),
        }
    }

    /// The slot-scoped pause covering `slot`.
    ///
    /// `None` for `slot` means "this command cannot be pinned to a single hash
    /// slot" — it names no keys, or names keys in more than one slot — and is
    /// answered fail-closed with the strongest pause armed on *any* slot: such a
    /// command may touch the barriered slot, and the barrier exists precisely to
    /// stop that. Node-global pauses are not consulted here; ask
    /// [`pause_overview`](Self::pause_overview) for those.
    pub fn slot_pause(&self, slot: Option<u16>) -> Option<PauseMode> {
        self.sweep_lapsed_pauses();
        let pause_state = self.pause_state.read().unwrap();
        match slot {
            Some(slot) => pause_state.slots.get(&slot).map(|e| e.mode),
            None => pause_state
                .slots
                .values()
                .fold(None, |acc, e| PauseMode::strongest(acc, Some(e.mode))),
        }
    }

    /// Whether any pause at all — node-global or slot-scoped — is in force.
    pub fn any_pause_active(&self) -> bool {
        self.pause_overview().is_active()
    }

    /// Drop lapsed pauses, taking the write lock only when there is something
    /// to drop.
    fn sweep_lapsed_pauses(&self) {
        let now = crate::clock::now();
        {
            let pause_state = self.pause_state.read().unwrap();
            if !pause_state.has_lapsed(now) {
                return;
            }
        }
        let mut pause_state = self.pause_state.write().unwrap();
        pause_state.sweep(crate::clock::now());
        self.publish_pause_derived_state(&pause_state);
    }

    /// Republish everything derived from the pause state: the shard-visible
    /// active-expiry suppression flag, and the replica-feed hold.
    ///
    /// Both are pure functions of [`PauseState`] and are republished from this
    /// one place on every mutation of it, so no consumer can hold a view the
    /// pause state does not justify. The feed hold's rationale is on
    /// [`PauseState::feed_hold_until`]; the expiry flag's follows.
    ///
    /// Suppressed while *any* pause is armed, slot-scoped ones included. Redis
    /// suppresses expires during `PAUSE WRITE` so the replication stream stays
    /// quiet while replicas catch up; a slot barrier wants the same thing for a
    /// stricter reason — an active-expiry deletion on the slot being handed over
    /// is exactly the orphaned write the barrier is there to prevent. Suppressing
    /// node-wide for a slot-scoped pause is broader than strictly needed, and
    /// deliberately so: it errs toward not writing, and lazy expiry still hides
    /// elapsed keys from readers.
    fn publish_pause_derived_state(&self, pause_state: &PauseState) {
        self.expiry_paused
            .store(!pause_state.is_idle(), Ordering::Relaxed);
        self.replica_feed_gate
            .publish(pause_state.feed_hold_until());
    }

    /// Get the current number of connected clients.
    pub fn client_count(&self) -> usize {
        let clients = self.clients.read().unwrap();
        clients.len()
    }

    /// Update client statistics with a delta.
    pub fn update_stats(&self, id: u64, delta: &ClientStatsDelta) {
        // Merge per-client stats.
        self.with_client_mut(id, |entry| entry.stats.merge_delta(delta));

        // Bump server-wide per-command call counters from the delta. Command
        // names are normalized to lowercase to match Redis commandstats format.
        if !delta.command_latencies.is_empty() {
            let mut stats = self.command_stats.write().unwrap();
            for (cmd, usec) in &delta.command_latencies {
                let entry = stats.entry(cmd.to_ascii_lowercase()).or_default();
                entry.calls += 1;
                entry.usec += usec;
            }
        }
    }

    /// Snapshot of server-wide per-command call counts (legacy compat).
    ///
    /// Returned as a lowercase-normalized map suitable for rendering
    /// `cmdstat_<name>:calls=N,...` lines in `INFO commandstats`.
    pub fn command_call_counts(&self) -> HashMap<String, u64> {
        self.command_stats
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.calls))
            .collect()
    }

    /// Snapshot of server-wide per-command statistics including rejected/failed.
    pub fn command_stats_snapshot(&self) -> HashMap<String, ServerCommandStats> {
        self.command_stats.read().unwrap().clone()
    }

    /// Record a rejected call for a specific command.
    pub fn record_command_rejected(&self, cmd_name: &str) {
        let mut stats = self.command_stats.write().unwrap();
        let entry = stats.entry(cmd_name.to_ascii_lowercase()).or_default();
        entry.rejected_calls += 1;
    }

    /// Record a failed call for a specific command.
    pub fn record_command_failed(&self, cmd_name: &str) {
        let mut stats = self.command_stats.write().unwrap();
        let entry = stats.entry(cmd_name.to_ascii_lowercase()).or_default();
        entry.failed_calls += 1;
    }

    /// Reset server-wide command stats (called by `CONFIG RESETSTAT`).
    pub fn reset_command_call_counts(&self) {
        self.command_stats.write().unwrap().clear();
    }

    /// Update memory usage for a client.
    pub fn update_memory(&self, id: u64, mem: ClientMemoryUsage) {
        self.with_client_mut(id, |entry| entry.memory = mem);
    }

    /// Get aggregate client memory across all connections.
    pub fn total_client_memory(&self) -> u64 {
        let clients = self.clients.read().unwrap();
        clients.values().map(|e| e.memory.total() as u64).sum()
    }

    /// Get evictable clients sorted by total memory descending.
    /// Excludes clients with NO_EVICT, MASTER, or REPLICA flags.
    /// Returns (id, tot_mem) pairs.
    pub fn eviction_candidates(&self) -> Vec<(u64, u64)> {
        let clients = self.clients.read().unwrap();
        let mut candidates: Vec<(u64, u64)> = clients
            .iter()
            .filter(|(_, entry)| {
                !entry.flags.contains(ClientFlags::NO_EVICT)
                    && !entry.flags.contains(ClientFlags::MASTER)
                    && !entry.flags.contains(ClientFlags::REPLICA)
            })
            .map(|(&id, entry)| (id, entry.memory.total() as u64))
            .collect();
        // Sort largest first
        candidates.sort_by(|a, b| b.1.cmp(&a.1));
        candidates
    }

    /// Try to evict clients until aggregate memory is below the limit.
    /// Returns number of clients evicted.
    pub fn try_evict_clients(&self, limit: u64) -> usize {
        if limit == 0 {
            return 0;
        }
        let total = self.total_client_memory();
        if total <= limit {
            return 0;
        }

        let candidates = self.eviction_candidates();
        let mut evicted = 0;

        for (id, _mem) in candidates {
            // Re-check aggregate after each eviction (other clients may have
            // disconnected concurrently)
            if self.total_client_memory() <= limit {
                break;
            }
            self.kill_by_id(id);
            evicted += 1;
        }
        evicted
    }

    /// Get statistics for a specific client.
    pub fn get_stats(&self, id: u64) -> Option<ClientStats> {
        let clients = self.clients.read().unwrap();
        clients.get(&id).map(|entry| entry.stats.clone())
    }

    /// Get statistics for all clients.
    pub fn get_all_stats(&self) -> Vec<(u64, ClientInfo, ClientStats)> {
        let clients = self.clients.read().unwrap();
        clients
            .iter()
            .map(|(&id, entry)| {
                let info = ClientInfo {
                    id,
                    addr: entry.addr,
                    local_addr: entry.local_addr,
                    name: entry.name.clone(),
                    created_at: entry.created_at,
                    last_command_at: entry.last_command_at,
                    flags: entry.flags,
                    sub_count: entry.sub_count,
                    psub_count: entry.psub_count,
                    ssub_count: entry.ssub_count,
                    in_multi: entry.in_multi,
                    multi_queue_len: entry.multi_queue_len,
                    watch_count: entry.watch_count,
                    lib_name: entry.lib_name.clone(),
                    lib_ver: entry.lib_ver.clone(),
                    stats: Some(entry.stats.clone()),
                    current_cmd: entry.current_cmd.clone(),
                    memory: entry.memory.clone(),
                };
                (id, info, entry.stats.clone())
            })
            .collect()
    }

    /// Get information and statistics for a specific client.
    pub fn get_with_stats(&self, id: u64) -> Option<(ClientInfo, ClientStats)> {
        let clients = self.clients.read().unwrap();
        clients.get(&id).map(|entry| {
            let info = ClientInfo {
                id,
                addr: entry.addr,
                local_addr: entry.local_addr,
                name: entry.name.clone(),
                created_at: entry.created_at,
                last_command_at: entry.last_command_at,
                flags: entry.flags,
                sub_count: entry.sub_count,
                psub_count: entry.psub_count,
                ssub_count: entry.ssub_count,
                in_multi: entry.in_multi,
                multi_queue_len: entry.multi_queue_len,
                watch_count: entry.watch_count,
                lib_name: entry.lib_name.clone(),
                lib_ver: entry.lib_ver.clone(),
                stats: Some(entry.stats.clone()),
                current_cmd: entry.current_cmd.clone(),
                memory: entry.memory.clone(),
            };
            (info, entry.stats.clone())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
    }

    // --- Pure flag-derivation helper tests -----------------------------
    // These construct a bare `ClientEntry` directly and never touch the
    // registry, so they exercise the derivation logic without a lock.

    #[test]
    fn test_set_flag_sets_and_clears() {
        let mut entry = ClientEntry::for_test();

        entry.set_flag(ClientFlags::NO_EVICT, true);
        assert!(entry.flags.contains(ClientFlags::NO_EVICT));

        entry.set_flag(ClientFlags::NO_EVICT, false);
        assert!(!entry.flags.contains(ClientFlags::NO_EVICT));
    }

    #[test]
    fn test_set_subscriptions_sets_pubsub_flag() {
        let mut entry = ClientEntry::for_test();

        entry.set_subscriptions(1, 0, 0);
        assert_eq!(entry.sub_count, 1);
        assert!(entry.flags.contains(ClientFlags::PUBSUB));

        entry.set_subscriptions(0, 2, 0);
        assert_eq!(entry.psub_count, 2);
        assert!(entry.flags.contains(ClientFlags::PUBSUB));

        entry.set_subscriptions(0, 0, 3);
        assert_eq!(entry.ssub_count, 3);
        assert!(entry.flags.contains(ClientFlags::PUBSUB));
    }

    #[test]
    fn test_set_subscriptions_clears_pubsub_at_zero() {
        let mut entry = ClientEntry::for_test();

        entry.set_subscriptions(1, 1, 1);
        assert!(entry.flags.contains(ClientFlags::PUBSUB));

        entry.set_subscriptions(0, 0, 0);
        assert!(!entry.flags.contains(ClientFlags::PUBSUB));
        assert_eq!(entry.sub_count, 0);
        assert_eq!(entry.psub_count, 0);
        assert_eq!(entry.ssub_count, 0);
    }

    #[test]
    fn test_set_blocked_follows_bool() {
        let mut entry = ClientEntry::for_test();

        entry.set_blocked(true);
        assert!(entry.flags.contains(ClientFlags::BLOCKED));

        entry.set_blocked(false);
        assert!(!entry.flags.contains(ClientFlags::BLOCKED));
    }

    #[test]
    fn test_set_paused_follows_bool() {
        let mut entry = ClientEntry::for_test();

        entry.set_paused(true);
        assert!(entry.flags.contains(ClientFlags::PAUSED));

        entry.set_paused(false);
        assert!(!entry.flags.contains(ClientFlags::PAUSED));
    }

    #[test]
    fn test_set_multi_follows_bool_and_updates_queue_len() {
        let mut entry = ClientEntry::for_test();

        entry.set_multi(true, 7);
        assert!(entry.in_multi);
        assert_eq!(entry.multi_queue_len, 7);
        assert!(entry.flags.contains(ClientFlags::MULTI));

        entry.set_multi(false, 0);
        assert!(!entry.in_multi);
        assert_eq!(entry.multi_queue_len, 0);
        assert!(!entry.flags.contains(ClientFlags::MULTI));
    }

    #[test]
    fn test_flag_helpers_are_independent() {
        // Setting one derived flag must not disturb the others.
        let mut entry = ClientEntry::for_test();

        entry.set_blocked(true);
        entry.set_paused(true);
        entry.set_multi(true, 1);
        entry.set_subscriptions(1, 0, 0);

        assert!(entry.flags.contains(ClientFlags::BLOCKED));
        assert!(entry.flags.contains(ClientFlags::PAUSED));
        assert!(entry.flags.contains(ClientFlags::MULTI));
        assert!(entry.flags.contains(ClientFlags::PUBSUB));

        entry.set_blocked(false);
        assert!(!entry.flags.contains(ClientFlags::BLOCKED));
        assert!(entry.flags.contains(ClientFlags::PAUSED));
        assert!(entry.flags.contains(ClientFlags::MULTI));
        assert!(entry.flags.contains(ClientFlags::PUBSUB));
    }

    #[test]
    fn test_register_unregister() {
        let registry = Arc::new(ClientRegistry::new());
        let addr = test_addr(12345);

        // Register a client
        let handle = registry.register(1, addr, None);
        assert_eq!(handle.id(), 1);
        assert_eq!(registry.client_count(), 1);

        // Get client info
        let info = registry.get(1).unwrap();
        assert_eq!(info.id, 1);
        assert_eq!(info.addr, addr);

        // Drop handle to unregister
        drop(handle);
        assert_eq!(registry.client_count(), 0);
        assert!(registry.get(1).is_none());
    }

    #[test]
    fn test_list_clients() {
        let registry = Arc::new(ClientRegistry::new());

        let h1 = registry.register(1, test_addr(1001), None);
        let h2 = registry.register(2, test_addr(1002), None);

        let clients = registry.list();
        assert_eq!(clients.len(), 2);

        drop(h1);
        drop(h2);
    }

    #[test]
    fn test_update_name() {
        let registry = Arc::new(ClientRegistry::new());
        let _handle = registry.register(1, test_addr(1001), None);

        registry.update_name(1, Some(Bytes::from_static(b"myconn")));

        let info = registry.get(1).unwrap();
        assert_eq!(info.name, Some(Bytes::from_static(b"myconn")));
    }

    #[test]
    fn test_kill_by_id() {
        let registry = Arc::new(ClientRegistry::new());
        let handle = registry.register(1, test_addr(1001), None);

        assert!(!handle.is_killed());
        assert!(registry.kill_by_id(1));
        assert!(handle.is_killed());
    }

    #[test]
    fn test_kill_by_filter() {
        let registry = Arc::new(ClientRegistry::new());
        let h1 = registry.register(1, test_addr(1001), None);
        let h2 = registry.register(2, test_addr(1002), None);

        // Kill by addr
        let filter = KillFilter {
            addr: Some(test_addr(1001)),
            ..Default::default()
        };

        let killed = registry.kill_by_filter(&filter);
        assert_eq!(killed, 1);
        assert!(h1.is_killed());
        assert!(!h2.is_killed());
    }

    #[test]
    fn test_kill_filter_skipme() {
        let registry = Arc::new(ClientRegistry::new());
        let h1 = registry.register(1, test_addr(1001), None);
        let h2 = registry.register(2, test_addr(1002), None);

        // Kill all except current connection
        let filter = KillFilter {
            skip_me: true,
            current_conn_id: Some(1),
            ..Default::default()
        };

        let killed = registry.kill_by_filter(&filter);
        assert_eq!(killed, 1);
        assert!(!h1.is_killed());
        assert!(h2.is_killed());
    }

    #[test]
    fn test_pause_unpause() {
        let registry = Arc::new(ClientRegistry::new());

        // Not paused initially
        assert!(!registry.any_pause_active());

        // Pause with long timeout
        registry.pause(PauseMode::Write, 10000);
        assert_eq!(registry.pause_overview().node, Some(PauseMode::Write));

        // Unpause
        registry.unpause();
        assert!(!registry.any_pause_active());
    }

    #[test]
    fn test_pause_auto_expire() {
        let registry = Arc::new(ClientRegistry::new());

        // Pause with 0ms timeout (immediate expiry)
        registry.pause(PauseMode::All, 0);

        // Should be expired
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(!registry.any_pause_active());
        assert!(!registry.expiry_paused_flag().load(Ordering::Relaxed));
    }

    /// A node-global pause keeps its Redis overlap rules: `ALL` never downgrades
    /// to `WRITE`, and a shorter re-arm never shortens the deadline.
    // FM-CLUSTER-082
    #[test]
    fn node_pause_never_downgrades_or_shortens() {
        let registry = ClientRegistry::new();

        registry.pause(PauseMode::All, 10_000);
        registry.pause(PauseMode::Write, 10);
        let overview = registry.pause_overview();
        assert_eq!(overview.node, Some(PauseMode::All));

        // The 10 ms re-arm must not have replaced the 10 s deadline.
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert_eq!(registry.pause_overview().node, Some(PauseMode::All));
    }

    // FM-CLUSTER-079
    /// A slot-scoped pause covers its own slot and nothing else.
    #[test]
    fn slot_pause_covers_only_its_slot() {
        let registry = ClientRegistry::new();
        registry.pause_slot(42, PauseMode::Write, 10_000);

        assert_eq!(registry.slot_pause(Some(42)), Some(PauseMode::Write));
        assert_eq!(registry.slot_pause(Some(43)), None);

        let overview = registry.pause_overview();
        assert!(overview.slot_scoped);
        assert_eq!(
            overview.node, None,
            "arming a slot barrier must not fabricate a node-global pause"
        );
    }

    // FM-CLUSTER-079
    /// A command that cannot be pinned to one slot (keyless, or cross-slot) is
    /// answered fail-closed: the strongest pause armed on *any* slot applies.
    #[test]
    fn unpinnable_command_sees_the_strongest_slot_pause() {
        let registry = ClientRegistry::new();
        assert_eq!(registry.slot_pause(None), None);

        registry.pause_slot(1, PauseMode::Write, 10_000);
        assert_eq!(registry.slot_pause(None), Some(PauseMode::Write));

        registry.pause_slot(2, PauseMode::All, 10_000);
        assert_eq!(registry.slot_pause(None), Some(PauseMode::All));
        // Per-slot composition: slot 1 is still only WRITE-paused.
        assert_eq!(registry.slot_pause(Some(1)), Some(PauseMode::Write));
    }

    // FM-CLUSTER-082
    /// The operator's pause and a slot barrier are released independently in
    /// both directions.
    #[test]
    fn node_and_slot_pauses_release_independently() {
        let registry = ClientRegistry::new();
        registry.pause(PauseMode::Write, 10_000);
        registry.pause_slot(7, PauseMode::Write, 10_000);

        // CLIENT UNPAUSE must not disarm the barrier.
        registry.unpause();
        assert_eq!(registry.pause_overview().node, None);
        assert_eq!(registry.slot_pause(Some(7)), Some(PauseMode::Write));
        assert!(registry.any_pause_active());

        // Re-arm the operator pause and release the barrier instead.
        registry.pause(PauseMode::All, 10_000);
        registry.unpause_slot(7);
        assert_eq!(registry.slot_pause(Some(7)), None);
        assert_eq!(
            registry.pause_overview().node,
            Some(PauseMode::All),
            "barrier release must not clear an operator pause"
        );
    }

    // FM-CLUSTER-082
    /// Slot pauses expire per slot, and the expiry-suppression flag tracks
    /// "anything armed", not just the node-global pause.
    #[test]
    fn slot_pauses_expire_independently() {
        let registry = ClientRegistry::new();
        registry.pause_slot(1, PauseMode::Write, 0);
        registry.pause_slot(2, PauseMode::Write, 10_000);
        assert!(registry.expiry_paused_flag().load(Ordering::Relaxed));

        std::thread::sleep(std::time::Duration::from_millis(1));
        assert_eq!(registry.slot_pause(Some(1)), None);
        assert_eq!(registry.slot_pause(Some(2)), Some(PauseMode::Write));
        assert!(
            registry.expiry_paused_flag().load(Ordering::Relaxed),
            "slot 2 is still armed, so active expiry stays suppressed"
        );

        registry.unpause_slot(2);
        assert!(!registry.expiry_paused_flag().load(Ordering::Relaxed));
    }

    // FM-CLUSTER-097
    /// The replica-feed hold is a *derivation* of the pause state, not a second
    /// flag: arming the barrier publishes a deadline, releasing it clears one,
    /// and nobody has to remember to do either.
    #[test]
    fn slot_barrier_publishes_and_clears_the_replica_feed_hold() {
        let registry = ClientRegistry::new();
        let gate = registry.replica_feed_gate();
        assert!(!gate.is_held(), "nothing is armed yet");

        registry.pause_slot(11, PauseMode::Write, 10_000);
        assert!(gate.is_held(), "arming the barrier must hold the feed");

        registry.unpause_slot(11);
        assert!(!gate.is_held(), "releasing the barrier must free the feed");
    }

    // FM-CLUSTER-097
    /// A node-global `CLIENT PAUSE` stops the writes themselves, so there is
    /// nothing new to ship and no reason to stall a replica behind it. Only the
    /// slot barrier — whose fenced writes still apply locally — holds the feed.
    #[test]
    fn a_node_pause_does_not_hold_the_replica_feed() {
        let registry = ClientRegistry::new();
        registry.pause(PauseMode::All, 10_000);
        assert!(
            !registry.replica_feed_gate().is_held(),
            "CLIENT PAUSE must not stall replication"
        );
    }

    // FM-CLUSTER-097
    /// The hold carries the barrier's own deadline, so a barrier nobody ever
    /// released — the finalizer died, the lease ran out, no sweep happened —
    /// still frees the feed. A wedged feed would be worse than the anomaly.
    #[test]
    fn a_lapsed_barrier_frees_the_feed_with_nobody_clearing_it() {
        let registry = ClientRegistry::new();
        registry.pause_slot(3, PauseMode::Write, 0);

        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(
            !registry.replica_feed_gate().is_held(),
            "the published deadline has passed, so the hold must read as released"
        );
    }

    // FM-CLUSTER-097
    /// Two overlapping handoffs compose the way the pauses themselves do: the
    /// hold runs to the later deadline, and releasing the earlier slot does not
    /// free the feed while the other barrier is still up.
    #[test]
    fn overlapping_barriers_hold_the_feed_to_the_later_deadline() {
        let registry = ClientRegistry::new();
        registry.pause_slot(1, PauseMode::Write, 10_000);
        registry.pause_slot(2, PauseMode::Write, 20_000);

        registry.unpause_slot(1);
        assert!(
            registry.replica_feed_gate().is_held(),
            "slot 2's barrier is still armed"
        );
        registry.unpause_slot(2);
        assert!(!registry.replica_feed_gate().is_held());
    }

    // FM-CLUSTER-082
    #[test]
    fn strongest_pause_mode_prefers_all() {
        use PauseMode::{All, Write};
        assert_eq!(PauseMode::strongest(None, None), None);
        assert_eq!(PauseMode::strongest(Some(Write), None), Some(Write));
        assert_eq!(PauseMode::strongest(None, Some(Write)), Some(Write));
        assert_eq!(PauseMode::strongest(Some(Write), Some(All)), Some(All));
        assert_eq!(PauseMode::strongest(Some(All), Some(Write)), Some(All));
    }

    #[test]
    fn test_client_flags() {
        let flags = ClientFlags::MULTI | ClientFlags::BLOCKED;
        assert!(flags.contains(ClientFlags::MULTI));
        assert!(flags.contains(ClientFlags::BLOCKED));
        assert!(!flags.contains(ClientFlags::PUBSUB));

        let flag_str = flags.to_flag_string();
        assert!(flag_str.contains('x')); // MULTI
        assert!(flag_str.contains('b')); // BLOCKED
    }

    #[test]
    fn test_update_subscriptions() {
        let registry = Arc::new(ClientRegistry::new());
        let _handle = registry.register(1, test_addr(1001), None);

        registry.update_subscriptions(1, 2, 1, 0);

        let info = registry.get(1).unwrap();
        assert_eq!(info.sub_count, 2);
        assert_eq!(info.psub_count, 1);
        assert_eq!(info.ssub_count, 0);
        assert!(info.flags.contains(ClientFlags::PUBSUB));
    }

    #[test]
    fn test_update_multi_state() {
        let registry = Arc::new(ClientRegistry::new());
        let _handle = registry.register(1, test_addr(1001), None);

        registry.update_multi_state(1, true, 5);

        let info = registry.get(1).unwrap();
        assert!(info.in_multi);
        assert_eq!(info.multi_queue_len, 5);
        assert!(info.flags.contains(ClientFlags::MULTI));
    }

    // issue 09: `watch` in CLIENT INFO/LIST comes from this method, wired
    // from the periodic memory-sync path (see
    // `ConnectionHandler::maybe_sync_stats`) rather than
    // `update_multi_state`'s currently-unwired call site.
    #[test]
    fn test_update_watch_count() {
        let registry = Arc::new(ClientRegistry::new());
        let _handle = registry.register(1, test_addr(1001), None);

        registry.update_watch_count(1, 4);

        let info = registry.get(1).unwrap();
        assert_eq!(info.watch_count, 4);
    }

    #[test]
    fn test_client_info_to_list_entry() {
        let info = ClientInfo {
            id: 42,
            addr: test_addr(12345),
            local_addr: Some(test_addr(6379)),
            name: Some(Bytes::from_static(b"myconn")),
            created_at: Instant::now(),
            last_command_at: Instant::now(),
            flags: ClientFlags::NONE,
            sub_count: 1,
            psub_count: 2,
            ssub_count: 3,
            in_multi: false,
            multi_queue_len: 0,
            watch_count: 0,
            lib_name: Some(Bytes::from_static(b"testlib")),
            lib_ver: Some(Bytes::from_static(b"1.0.0")),
            stats: None,
            current_cmd: None,
            memory: ClientMemoryUsage::default(),
        };

        let entry = info.to_client_list_entry();
        assert!(entry.contains("id=42"));
        assert!(entry.contains("name=myconn"));
        assert!(entry.contains("sub=1"));
        assert!(entry.contains("psub=2"));
        assert!(entry.contains("ssub=3"));
        assert!(entry.contains("lib-name=testlib"));
        assert!(entry.contains("lib-ver=1.0.0"));
        // issue 09: watch/tot-net-*/rbs/rbp/redir must be present in every
        // entry, never silently dropped.
        assert!(entry.contains("watch=0"));
        assert!(entry.contains("tot-net-in=0"));
        assert!(entry.contains("tot-net-out=0"));
        assert!(entry.contains("rbs=0"));
        assert!(entry.contains("rbp=0"));
        assert!(entry.contains("redir=-1"));
    }

    // issue 09: watch/tot-net-in/tot-net-out/rbs/rbp report real, non-zero
    // per-connection values rather than always-0 placeholders.
    #[test]
    fn test_client_info_reports_real_watch_net_and_buffer_fields() {
        let mut stats = ClientStats::default();
        stats.bytes_recv = 512;
        stats.bytes_sent = 2048;

        let info = ClientInfo {
            id: 7,
            addr: test_addr(1),
            local_addr: None,
            name: None,
            created_at: Instant::now(),
            last_command_at: Instant::now(),
            flags: ClientFlags::NONE,
            sub_count: 0,
            psub_count: 0,
            ssub_count: 0,
            in_multi: false,
            multi_queue_len: 0,
            watch_count: 3,
            lib_name: None,
            lib_ver: None,
            stats: Some(stats),
            current_cmd: None,
            memory: ClientMemoryUsage {
                query_buf_size: 64,
                query_buf_peak: 256,
                ..Default::default()
            },
        };

        let entry = info.to_client_list_entry();
        assert!(entry.contains("watch=3"), "{entry}");
        assert!(entry.contains("tot-net-in=512"), "{entry}");
        assert!(entry.contains("tot-net-out=2048"), "{entry}");
        assert!(entry.contains("rbs=64"), "{entry}");
        assert!(entry.contains("rbp=256"), "{entry}");
    }

    #[test]
    fn test_concurrent_registration() {
        use std::thread;

        let registry = Arc::new(ClientRegistry::new());
        let mut handles = vec![];

        // Spawn multiple threads registering clients
        for i in 0..10 {
            let registry = Arc::clone(&registry);
            handles.push(thread::spawn(move || {
                let _h = registry.register(i, test_addr(1000 + i as u16), None);
                // Hold handle briefly
                thread::sleep(std::time::Duration::from_millis(10));
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // All clients should be unregistered now
        assert_eq!(registry.client_count(), 0);
    }

    #[test]
    fn test_client_stats_p99() {
        let mut stats = ClientStats::default();

        // Add 100 samples: 1, 2, 3, ..., 100
        for i in 1..=100 {
            stats.record_latency_sample(i);
        }

        // p99 of 1-100 should be 99 (99th percentile)
        let p99 = stats.p99_latency_us();
        assert!((99..=100).contains(&p99), "p99 was {}", p99);
    }

    #[test]
    fn test_client_stats_circular_buffer() {
        let mut stats = ClientStats::default();

        // Fill buffer
        for i in 0..100 {
            stats.record_latency_sample(i);
        }
        assert_eq!(stats.latency_samples.len(), 100);

        // Add more samples - should wrap around
        for i in 100..150 {
            stats.record_latency_sample(i);
        }
        assert_eq!(stats.latency_samples.len(), 100);

        // p99 should be from the newer values
        let p99 = stats.p99_latency_us();
        assert!(p99 >= 140, "p99 should be high, was {}", p99);
    }

    #[test]
    fn test_client_stats_record_command() {
        let mut stats = ClientStats::default();

        stats.record_command("GET", 100);
        stats.record_command("GET", 200);
        stats.record_command("SET", 150);

        assert_eq!(stats.commands_total, 3);
        assert_eq!(stats.latency_total_us, 450);
        assert_eq!(stats.latency_max_us, 200);
        assert_eq!(stats.command_counts.len(), 2);
        assert_eq!(stats.command_counts.get("GET").unwrap().count, 2);
        assert_eq!(stats.command_counts.get("SET").unwrap().count, 1);
    }

    #[test]
    fn test_client_stats_command_limit() {
        let mut stats = ClientStats::default();

        // Add more than 50 command types
        for i in 0..60 {
            stats.record_command(&format!("CMD{}", i), 100);
        }

        // Should be limited to 50
        assert!(stats.command_counts.len() <= 50);
    }

    #[test]
    fn test_client_stats_delta_merge() {
        let mut stats = ClientStats::default();
        stats.record_command("GET", 100);

        let delta = ClientStatsDelta {
            commands_processed: 5,
            total_latency_us: 500,
            bytes_recv: 1000,
            bytes_sent: 2000,
            command_latencies: vec![("GET".to_string(), 50), ("SET".to_string(), 150)],
        };

        stats.merge_delta(&delta);

        assert_eq!(stats.commands_total, 6); // 1 + 5
        assert_eq!(stats.bytes_recv, 1000);
        assert_eq!(stats.bytes_sent, 2000);
        assert_eq!(stats.command_counts.get("GET").unwrap().count, 2);
        assert_eq!(stats.command_counts.get("SET").unwrap().count, 1);
    }

    #[test]
    fn test_update_stats() {
        let registry = Arc::new(ClientRegistry::new());
        let _handle = registry.register(1, test_addr(1001), None);

        let delta = ClientStatsDelta {
            commands_processed: 10,
            total_latency_us: 1000,
            bytes_recv: 500,
            bytes_sent: 1500,
            command_latencies: vec![("GET".to_string(), 100)],
        };

        registry.update_stats(1, &delta);

        let stats = registry.get_stats(1).unwrap();
        assert_eq!(stats.commands_total, 10);
        assert_eq!(stats.bytes_recv, 500);
        assert_eq!(stats.bytes_sent, 1500);
    }

    #[test]
    fn test_get_all_stats() {
        let registry = Arc::new(ClientRegistry::new());
        let _h1 = registry.register(1, test_addr(1001), None);
        let _h2 = registry.register(2, test_addr(1002), None);

        let delta = ClientStatsDelta {
            commands_processed: 5,
            total_latency_us: 500,
            bytes_recv: 100,
            bytes_sent: 200,
            command_latencies: vec![],
        };

        registry.update_stats(1, &delta);

        let all_stats = registry.get_all_stats();
        assert_eq!(all_stats.len(), 2);

        // Find client 1 stats
        let (_, _, stats1) = all_stats.iter().find(|(id, _, _)| *id == 1).unwrap();
        assert_eq!(stats1.commands_total, 5);
    }

    #[test]
    fn test_extract_error_prefix() {
        assert_eq!(
            extract_error_prefix(b"ERR wrong number of arguments"),
            "ERR"
        );
        assert_eq!(
            extract_error_prefix(b"WRONGTYPE Operation against a key"),
            "WRONGTYPE"
        );
        assert_eq!(
            extract_error_prefix(b"NOSCRIPT No matching script"),
            "NOSCRIPT"
        );
        assert_eq!(extract_error_prefix(b"OOM command not allowed"), "OOM");
        assert_eq!(
            extract_error_prefix(b"NOPERM this user has no permissions"),
            "NOPERM"
        );
        // No space: entire string is the prefix
        assert_eq!(extract_error_prefix(b"LOADING"), "LOADING");
    }

    #[test]
    fn test_error_stats_record_rejected() {
        let stats = ErrorStats::new();

        stats.record_rejected("ERR");
        stats.record_rejected("ERR");
        stats.record_rejected("NOPERM");

        assert_eq!(stats.rejected_calls.load(Ordering::Relaxed), 3);
        assert_eq!(stats.failed_calls.load(Ordering::Relaxed), 0);
        assert_eq!(stats.total_error_replies.load(Ordering::Relaxed), 3);

        let snapshot = stats.error_type_snapshot();
        assert_eq!(snapshot.get("ERR"), Some(&2));
        assert_eq!(snapshot.get("NOPERM"), Some(&1));
    }

    #[test]
    fn test_error_stats_record_failed() {
        let stats = ErrorStats::new();

        stats.record_failed("WRONGTYPE");
        stats.record_failed("NOSCRIPT");

        assert_eq!(stats.rejected_calls.load(Ordering::Relaxed), 0);
        assert_eq!(stats.failed_calls.load(Ordering::Relaxed), 2);
        assert_eq!(stats.total_error_replies.load(Ordering::Relaxed), 2);

        let snapshot = stats.error_type_snapshot();
        assert_eq!(snapshot.get("WRONGTYPE"), Some(&1));
        assert_eq!(snapshot.get("NOSCRIPT"), Some(&1));
    }

    #[test]
    fn test_error_stats_cap() {
        let stats = ErrorStats::new();

        // Fill up to the cap (128)
        for i in 0..150 {
            stats.record_rejected(&format!("TYPE{}", i));
        }

        let snapshot = stats.error_type_snapshot();
        // Should be capped at 128 distinct types
        assert_eq!(snapshot.len(), 128);
        // But total count should reflect all 150
        assert_eq!(stats.total_error_replies.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn test_error_stats_reset() {
        let stats = ErrorStats::new();

        stats.record_rejected("ERR");
        stats.record_failed("WRONGTYPE");

        stats.reset();

        assert_eq!(stats.total_error_replies.load(Ordering::Relaxed), 0);
        assert_eq!(stats.rejected_calls.load(Ordering::Relaxed), 0);
        assert_eq!(stats.failed_calls.load(Ordering::Relaxed), 0);
        assert!(stats.error_type_snapshot().is_empty());
    }

    #[test]
    fn test_server_command_stats() {
        let registry = Arc::new(ClientRegistry::new());
        let _handle = registry.register(1, test_addr(1001), None);

        // Simulate command calls via update_stats
        let delta = ClientStatsDelta {
            commands_processed: 3,
            total_latency_us: 300,
            bytes_recv: 0,
            bytes_sent: 0,
            command_latencies: vec![
                ("GET".to_string(), 100),
                ("GET".to_string(), 100),
                ("SET".to_string(), 100),
            ],
        };
        registry.update_stats(1, &delta);

        // Record rejected/failed
        registry.record_command_rejected("get");
        registry.record_command_failed("set");

        let snapshot = registry.command_stats_snapshot();
        let get_stats = snapshot.get("get").unwrap();
        assert_eq!(get_stats.calls, 2);
        assert_eq!(get_stats.usec, 200);
        assert_eq!(get_stats.rejected_calls, 1);
        assert_eq!(get_stats.failed_calls, 0);

        let set_stats = snapshot.get("set").unwrap();
        assert_eq!(set_stats.calls, 1);
        assert_eq!(set_stats.usec, 100);
        assert_eq!(set_stats.rejected_calls, 0);
        assert_eq!(set_stats.failed_calls, 1);
    }
}
