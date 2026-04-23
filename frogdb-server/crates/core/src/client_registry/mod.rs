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

/// Pause state for the client registry.
#[derive(Debug, Default)]
struct PauseState {
    /// Current pause mode (None if not paused).
    mode: Option<PauseMode>,
    /// When the pause should automatically expire.
    unpause_at: Option<Instant>,
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

    /// Clear the unblock signal (call after handling).
    pub fn clear_unblock(&self) {
        // The registry will reset this when needed
    }
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
    /// Pause state for CLIENT PAUSE.
    pause_state: RwLock<PauseState>,
    /// Whether active key expiry should be paused (true during PAUSE ALL or PAUSE WRITE).
    expiry_paused: Arc<AtomicBool>,
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

    /// Register a new client connection.
    pub fn register(
        self: &Arc<Self>,
        id: u64,
        addr: SocketAddr,
        local_addr: Option<SocketAddr>,
    ) -> ClientHandle {
        let now = Instant::now();
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
                lib_name: entry.lib_name.clone(),
                lib_ver: entry.lib_ver.clone(),
                stats: None,
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
            lib_name: entry.lib_name.clone(),
            lib_ver: entry.lib_ver.clone(),
            stats: None,
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
        let mut clients = self.clients.write().unwrap();
        if let Some(entry) = clients.get_mut(&id) {
            entry.name = name;
        }
    }

    /// Update a client's library info.
    pub fn update_lib_info(&self, id: u64, lib_name: Option<Bytes>, lib_ver: Option<Bytes>) {
        let mut clients = self.clients.write().unwrap();
        if let Some(entry) = clients.get_mut(&id) {
            if lib_name.is_some() {
                entry.lib_name = lib_name;
            }
            if lib_ver.is_some() {
                entry.lib_ver = lib_ver;
            }
        }
    }

    /// Update a client's last command time.
    pub fn update_last_command(&self, id: u64) {
        let mut clients = self.clients.write().unwrap();
        if let Some(entry) = clients.get_mut(&id) {
            entry.last_command_at = Instant::now();
        }
    }

    /// Update a client's last command time with a pre-captured instant.
    pub fn update_last_command_at(&self, id: u64, time: Instant) {
        let mut clients = self.clients.write().unwrap();
        if let Some(entry) = clients.get_mut(&id) {
            entry.last_command_at = time;
        }
    }

    /// Update the currently executing command for a client.
    pub fn update_current_cmd(&self, id: u64, cmd: Option<String>) {
        let mut clients = self.clients.write().unwrap();
        if let Some(entry) = clients.get_mut(&id) {
            entry.current_cmd = cmd;
        }
    }

    /// Update client flags.
    pub fn update_flags(&self, id: u64, flags: ClientFlags) {
        let mut clients = self.clients.write().unwrap();
        if let Some(entry) = clients.get_mut(&id) {
            entry.flags = flags;
        }
    }

    /// Update pub/sub subscription counts.
    pub fn update_subscriptions(
        &self,
        id: u64,
        sub_count: usize,
        psub_count: usize,
        ssub_count: usize,
    ) {
        let mut clients = self.clients.write().unwrap();
        if let Some(entry) = clients.get_mut(&id) {
            entry.sub_count = sub_count;
            entry.psub_count = psub_count;
            entry.ssub_count = ssub_count;
            // Update PUBSUB flag
            if sub_count > 0 || psub_count > 0 || ssub_count > 0 {
                entry.flags |= ClientFlags::PUBSUB;
            } else {
                entry.flags.remove(ClientFlags::PUBSUB);
            }
        }
    }

    /// Update blocked state for a client.
    pub fn update_blocked_state(&self, id: u64, blocked: bool) {
        let mut clients = self.clients.write().unwrap();
        if let Some(entry) = clients.get_mut(&id) {
            if blocked {
                entry.flags |= ClientFlags::BLOCKED;
            } else {
                entry.flags.remove(ClientFlags::BLOCKED);
            }
        }
    }

    /// Update paused state for a client (blocked by CLIENT PAUSE).
    pub fn update_paused_state(&self, id: u64, paused: bool) {
        let mut clients = self.clients.write().unwrap();
        if let Some(entry) = clients.get_mut(&id) {
            if paused {
                entry.flags |= ClientFlags::PAUSED;
            } else {
                entry.flags.remove(ClientFlags::PAUSED);
            }
        }
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
        let mut clients = self.clients.write().unwrap();
        if let Some(entry) = clients.get_mut(&id) {
            entry.in_multi = in_multi;
            entry.multi_queue_len = queue_len;
            if in_multi {
                entry.flags |= ClientFlags::MULTI;
            } else {
                entry.flags.remove(ClientFlags::MULTI);
            }
        }
    }

    /// Set pause state.
    ///
    /// Follows Redis semantics for overlapping pauses:
    /// - Mode precedence: ALL takes priority over WRITE (never downgrade).
    /// - Time preservation: the maximum of old and new end times is kept.
    pub fn pause(&self, mode: PauseMode, timeout_ms: u64) {
        let mut pause_state = self.pause_state.write().unwrap();
        let now = Instant::now();
        let new_unpause_at = now + std::time::Duration::from_millis(timeout_ms);

        // Check whether the existing pause is still active (not expired).
        let existing_active = matches!(pause_state.unpause_at, Some(t) if t > now);

        // Determine effective mode: ALL takes priority over WRITE,
        // but only if the existing pause hasn't expired yet.
        let effective_mode = if existing_active {
            match pause_state.mode {
                Some(PauseMode::All) => PauseMode::All,
                _ => mode,
            }
        } else {
            mode
        };

        // Determine effective end time: keep the later of old and new,
        // but only if the existing pause hasn't expired yet.
        let effective_unpause_at = if existing_active {
            match pause_state.unpause_at {
                Some(existing) if existing > new_unpause_at => existing,
                _ => new_unpause_at,
            }
        } else {
            new_unpause_at
        };

        pause_state.mode = Some(effective_mode);
        pause_state.unpause_at = Some(effective_unpause_at);

        // Suppress active expiry during both CLIENT PAUSE ALL and PAUSE WRITE.
        // Redis suppresses expires during PAUSE WRITE to prevent replication
        // stream writes while replicas catch up.
        self.expiry_paused.store(true, Ordering::Relaxed);
    }

    /// Clear pause state.
    pub fn unpause(&self) {
        let mut pause_state = self.pause_state.write().unwrap();
        pause_state.mode = None;
        pause_state.unpause_at = None;
        self.expiry_paused.store(false, Ordering::Relaxed);
    }

    /// Check current pause state.
    /// Returns None if not paused (including auto-expiry).
    pub fn check_pause(&self) -> Option<PauseMode> {
        // First check with read lock
        {
            let pause_state = self.pause_state.read().unwrap();
            let mode = pause_state.mode?;
            if let Some(unpause_at) = pause_state.unpause_at {
                if Instant::now() < unpause_at {
                    return Some(mode);
                }
            } else {
                return Some(mode);
            }
        }

        // Pause expired, clear it
        let mut pause_state = self.pause_state.write().unwrap();
        if let Some(unpause_at) = pause_state.unpause_at
            && Instant::now() >= unpause_at
        {
            pause_state.mode = None;
            pause_state.unpause_at = None;
            self.expiry_paused.store(false, Ordering::Relaxed);
        }
        None
    }

    /// Get the current number of connected clients.
    pub fn client_count(&self) -> usize {
        let clients = self.clients.read().unwrap();
        clients.len()
    }

    /// Update client statistics with a delta.
    pub fn update_stats(&self, id: u64, delta: &ClientStatsDelta) {
        // Merge per-client stats.
        {
            let mut clients = self.clients.write().unwrap();
            if let Some(entry) = clients.get_mut(&id) {
                entry.stats.merge_delta(delta);
            }
        }

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
        let mut clients = self.clients.write().unwrap();
        if let Some(entry) = clients.get_mut(&id) {
            entry.memory = mem;
        }
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
        assert!(registry.check_pause().is_none());

        // Pause with long timeout
        registry.pause(PauseMode::Write, 10000);
        assert_eq!(registry.check_pause(), Some(PauseMode::Write));

        // Unpause
        registry.unpause();
        assert!(registry.check_pause().is_none());
    }

    #[test]
    fn test_pause_auto_expire() {
        let registry = Arc::new(ClientRegistry::new());

        // Pause with 0ms timeout (immediate expiry)
        registry.pause(PauseMode::All, 0);

        // Should be expired
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(registry.check_pause().is_none());
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
