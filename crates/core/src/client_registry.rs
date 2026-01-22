//! Client registry for tracking all connected clients.
//!
//! This module provides a global registry of all connected clients, enabling:
//! - CLIENT LIST: List all connected clients
//! - CLIENT KILL: Terminate connections
//! - CLIENT PAUSE: Pause client command execution
//! - CLIENT ID/SETNAME/GETNAME/INFO: Per-client introspection

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;

use bitflags::bitflags;
use bytes::Bytes;
use tokio::sync::watch;

use crate::sync::{Arc, RwLock};

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
}

/// Client connection information snapshot.
#[derive(Debug, Clone)]
pub struct ClientInfo {
    /// Unique connection ID.
    pub id: u64,
    /// Remote client address.
    pub addr: SocketAddr,
    /// Local server address.
    pub local_addr: Option<SocketAddr>,
    /// Client name.
    pub name: Option<Bytes>,
    /// When the connection was created.
    pub created_at: Instant,
    /// When the last command was executed.
    pub last_command_at: Instant,
    /// Current client flags.
    pub flags: ClientFlags,
    /// Number of channel subscriptions.
    pub sub_count: usize,
    /// Number of pattern subscriptions.
    pub psub_count: usize,
    /// Number of sharded subscriptions.
    pub ssub_count: usize,
    /// Whether client is in MULTI/EXEC.
    pub in_multi: bool,
    /// Number of commands queued in MULTI.
    pub multi_queue_len: usize,
    /// Library name (from CLIENT SETINFO).
    pub lib_name: Option<Bytes>,
    /// Library version (from CLIENT SETINFO).
    pub lib_ver: Option<Bytes>,
}

impl ClientInfo {
    /// Format as CLIENT LIST entry.
    pub fn to_client_list_entry(&self) -> String {
        let age = self.created_at.elapsed().as_secs();
        let idle = self.last_command_at.elapsed().as_secs();
        let addr_str = self.addr.to_string();
        let laddr_str = self
            .local_addr
            .map(|a| a.to_string())
            .unwrap_or_default();
        let name_str = self
            .name
            .as_ref()
            .map(|n| String::from_utf8_lossy(n).to_string())
            .unwrap_or_default();
        let lib_name_str = self
            .lib_name
            .as_ref()
            .map(|n| String::from_utf8_lossy(n).to_string())
            .unwrap_or_default();
        let lib_ver_str = self
            .lib_ver
            .as_ref()
            .map(|n| String::from_utf8_lossy(n).to_string())
            .unwrap_or_default();
        let flags_str = self.flags.to_flag_string();
        let multi_qlen = if self.in_multi {
            self.multi_queue_len as i64
        } else {
            -1
        };

        format!(
            "id={} addr={} laddr={} fd=0 name={} age={} idle={} flags={} db=0 sub={} psub={} ssub={} multi={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=NULL user=default redir=-1 resp=2 lib-name={} lib-ver={}",
            self.id,
            addr_str,
            laddr_str,
            name_str,
            age,
            idle,
            flags_str,
            self.sub_count,
            self.psub_count,
            self.ssub_count,
            multi_qlen,
            lib_name_str,
            lib_ver_str
        )
    }

    /// Get client type string.
    pub fn client_type(&self) -> &'static str {
        if self.flags.contains(ClientFlags::MASTER) {
            "master"
        } else if self.flags.contains(ClientFlags::REPLICA) {
            "replica"
        } else if self.flags.contains(ClientFlags::PUBSUB) {
            "pubsub"
        } else {
            "normal"
        }
    }
}

/// Filter for CLIENT KILL command.
#[derive(Debug, Default)]
pub struct KillFilter {
    /// Kill by connection ID.
    pub id: Option<u64>,
    /// Kill by client address.
    pub addr: Option<SocketAddr>,
    /// Kill by local address.
    pub laddr: Option<SocketAddr>,
    /// Kill by client type.
    pub client_type: Option<String>,
    /// Skip the current connection.
    pub skip_me: bool,
    /// Current connection ID (for SKIPME).
    pub current_conn_id: Option<u64>,
}

impl KillFilter {
    /// Check if a client matches this filter.
    pub fn matches(&self, id: u64, info: &ClientInfo) -> bool {
        // Check SKIPME
        if self.skip_me {
            if let Some(current_id) = self.current_conn_id {
                if id == current_id {
                    return false;
                }
            }
        }

        // Check ID filter
        if let Some(filter_id) = self.id {
            if id != filter_id {
                return false;
            }
        }

        // Check ADDR filter
        if let Some(ref filter_addr) = self.addr {
            if info.addr != *filter_addr {
                return false;
            }
        }

        // Check LADDR filter
        if let Some(ref filter_laddr) = self.laddr {
            if info.local_addr.as_ref() != Some(filter_laddr) {
                return false;
            }
        }

        // Check TYPE filter
        if let Some(ref filter_type) = self.client_type {
            let client_type = info.client_type();
            if client_type != filter_type {
                return false;
            }
        }

        true
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
        }
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
    /// Note: Returns true even if client wasn't actually blocked.
    pub fn unblock(&self, id: u64, mode: UnblockMode) -> bool {
        let clients = self.clients.read().unwrap();
        if let Some(entry) = clients.get(&id) {
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

    /// Update client flags.
    pub fn update_flags(&self, id: u64, flags: ClientFlags) {
        let mut clients = self.clients.write().unwrap();
        if let Some(entry) = clients.get_mut(&id) {
            entry.flags = flags;
        }
    }

    /// Update pub/sub subscription counts.
    pub fn update_subscriptions(&self, id: u64, sub_count: usize, psub_count: usize, ssub_count: usize) {
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
    pub fn pause(&self, mode: PauseMode, timeout_ms: u64) {
        let mut pause_state = self.pause_state.write().unwrap();
        pause_state.mode = Some(mode);
        pause_state.unpause_at = Some(Instant::now() + std::time::Duration::from_millis(timeout_ms));
    }

    /// Clear pause state.
    pub fn unpause(&self) {
        let mut pause_state = self.pause_state.write().unwrap();
        pause_state.mode = None;
        pause_state.unpause_at = None;
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
        if let Some(unpause_at) = pause_state.unpause_at {
            if Instant::now() >= unpause_at {
                pause_state.mode = None;
                pause_state.unpause_at = None;
            }
        }
        None
    }

    /// Get the current number of connected clients.
    pub fn client_count(&self) -> usize {
        let clients = self.clients.read().unwrap();
        clients.len()
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
}
