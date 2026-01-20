//! Shard infrastructure for shared-nothing concurrency.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use frogdb_protocol::{ParsedCommand, Response};
use tokio::sync::{mpsc, oneshot};

use crate::command::CommandContext;
use crate::persistence::{
    NoopSnapshotCoordinator, RocksStore, RocksWalWriter, SnapshotCoordinator, WalConfig,
};
use crate::registry::CommandRegistry;
use crate::store::{HashMapStore, Store};

/// Messages sent to shard workers.
#[derive(Debug)]
pub enum ShardMessage {
    /// Execute a command on this shard.
    Execute {
        command: ParsedCommand,
        conn_id: u64,
        /// Transaction ID for VLL ordering (optional for single-shard operations).
        txid: Option<u64>,
        response_tx: oneshot::Sender<Response>,
    },

    /// Scatter-gather: partial request for multi-key operation.
    ScatterRequest {
        request_id: u64,
        keys: Vec<Bytes>,
        operation: ScatterOp,
        response_tx: oneshot::Sender<PartialResult>,
    },

    /// Shutdown signal.
    Shutdown,
}

/// Operation type for scatter-gather.
#[derive(Debug, Clone)]
pub enum ScatterOp {
    /// MGET operation - get multiple values.
    MGet,
    /// MSET operation - set multiple key-value pairs.
    MSet {
        /// Key-value pairs where keys align with the keys field in ScatterRequest.
        pairs: Vec<(Bytes, Bytes)>,
    },
    /// DELETE operation.
    Del,
    /// EXISTS operation.
    Exists,
    /// TOUCH operation.
    Touch,
    /// UNLINK operation (async delete, same as Del for now).
    Unlink,
}

/// Result from a shard for scatter-gather operations.
#[derive(Debug)]
pub struct PartialResult {
    /// Results keyed by original key position.
    pub results: Vec<(Bytes, Response)>,
}

/// A pending operation in the VLL transaction queue.
#[derive(Debug)]
#[allow(dead_code)]
pub struct PendingOp {
    /// Transaction ID.
    pub txid: u64,
    /// Keys involved in this operation.
    pub keys: Vec<Bytes>,
    /// The operation to execute.
    pub operation: ScatterOp,
}

/// VLL (Very Lightweight Locking) transaction queue stub.
///
/// This is a foundation for future conflict detection and ordering.
/// Currently serves as a placeholder for Phase 4 scatter-gather operations.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct TransactionQueue {
    /// Pending operations indexed by transaction ID.
    pending: std::collections::BTreeMap<u64, PendingOp>,
    /// Maximum queue depth before blocking new transactions.
    max_depth: usize,
}

#[allow(dead_code)]
impl TransactionQueue {
    /// Create a new transaction queue with the specified max depth.
    pub fn new(max_depth: usize) -> Self {
        Self {
            pending: std::collections::BTreeMap::new(),
            max_depth,
        }
    }

    /// Check if the queue has capacity for a new transaction.
    pub fn has_capacity(&self) -> bool {
        self.pending.len() < self.max_depth
    }

    /// Add a pending operation to the queue.
    pub fn enqueue(&mut self, op: PendingOp) {
        self.pending.insert(op.txid, op);
    }

    /// Remove a completed operation from the queue.
    pub fn dequeue(&mut self, txid: u64) -> Option<PendingOp> {
        self.pending.remove(&txid)
    }

    /// Get the number of pending operations.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Check if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

/// New connection to be handled by a shard.
pub struct NewConnection {
    /// The TCP socket.
    pub socket: tokio::net::TcpStream,
    /// Client address.
    pub addr: std::net::SocketAddr,
    /// Connection ID.
    pub conn_id: u64,
}

impl std::fmt::Debug for NewConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NewConnection")
            .field("addr", &self.addr)
            .field("conn_id", &self.conn_id)
            .finish()
    }
}

/// A shard worker that owns a partition of the data.
pub struct ShardWorker {
    /// Shard ID.
    pub shard_id: usize,

    /// Total number of shards.
    pub num_shards: usize,

    /// Local data store.
    pub store: HashMapStore,

    /// Receiver for shard messages.
    pub message_rx: mpsc::Receiver<ShardMessage>,

    /// Receiver for new connections.
    pub new_conn_rx: mpsc::Receiver<NewConnection>,

    /// Senders to all shards (for cross-shard operations).
    pub shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,

    /// Command registry.
    pub registry: Arc<CommandRegistry>,

    /// Optional RocksDB store for persistence.
    pub rocks_store: Option<Arc<RocksStore>>,

    /// WAL writer for this shard.
    pub wal_writer: Option<RocksWalWriter>,

    /// Snapshot coordinator for BGSAVE.
    pub snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
}

impl ShardWorker {
    /// Create a new shard worker without persistence.
    pub fn new(
        shard_id: usize,
        num_shards: usize,
        message_rx: mpsc::Receiver<ShardMessage>,
        new_conn_rx: mpsc::Receiver<NewConnection>,
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        registry: Arc<CommandRegistry>,
    ) -> Self {
        Self {
            shard_id,
            num_shards,
            store: HashMapStore::new(),
            message_rx,
            new_conn_rx,
            shard_senders,
            registry,
            rocks_store: None,
            wal_writer: None,
            snapshot_coordinator: Arc::new(NoopSnapshotCoordinator::new()),
        }
    }

    /// Create a new shard worker with persistence.
    pub fn with_persistence(
        shard_id: usize,
        num_shards: usize,
        store: HashMapStore,
        message_rx: mpsc::Receiver<ShardMessage>,
        new_conn_rx: mpsc::Receiver<NewConnection>,
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        registry: Arc<CommandRegistry>,
        rocks_store: Arc<RocksStore>,
        wal_config: WalConfig,
        snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
    ) -> Self {
        let wal_writer = RocksWalWriter::new(rocks_store.clone(), shard_id, wal_config);
        Self {
            shard_id,
            num_shards,
            store,
            message_rx,
            new_conn_rx,
            shard_senders,
            registry,
            rocks_store: Some(rocks_store),
            wal_writer: Some(wal_writer),
            snapshot_coordinator,
        }
    }

    /// Get the snapshot coordinator.
    pub fn snapshot_coordinator(&self) -> &Arc<dyn SnapshotCoordinator> {
        &self.snapshot_coordinator
    }

    /// Run the shard worker event loop.
    pub async fn run(mut self) {
        tracing::info!(shard_id = self.shard_id, "Shard worker started");

        // Active expiry runs every 100ms
        let mut expiry_interval = tokio::time::interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                // Handle new connections
                Some(new_conn) = self.new_conn_rx.recv() => {
                    self.handle_new_connection(new_conn).await;
                }

                // Handle shard messages
                Some(msg) = self.message_rx.recv() => {
                    match msg {
                        ShardMessage::Execute { command, conn_id, txid: _, response_tx } => {
                            let response = self.execute_command(&command, conn_id);
                            let _ = response_tx.send(response);
                        }
                        ShardMessage::ScatterRequest { request_id: _, keys, operation, response_tx } => {
                            let result = self.execute_scatter_part(&keys, &operation).await;
                            let _ = response_tx.send(result);
                        }
                        ShardMessage::Shutdown => {
                            tracing::info!(shard_id = self.shard_id, "Shard worker shutting down");
                            // Flush WAL before shutdown
                            if let Some(ref wal) = self.wal_writer {
                                if let Err(e) = wal.flush_async().await {
                                    tracing::error!(shard_id = self.shard_id, error = %e, "Failed to flush WAL on shutdown");
                                }
                            }
                            break;
                        }
                    }
                }

                // Active expiry task
                _ = expiry_interval.tick() => {
                    self.run_active_expiry();
                }

                else => break,
            }
        }

        // Final WAL flush
        if let Some(ref wal) = self.wal_writer {
            if let Err(e) = wal.flush_async().await {
                tracing::error!(shard_id = self.shard_id, error = %e, "Failed to flush WAL on exit");
            }
        }
    }

    /// Run active expiry with time budget.
    ///
    /// This method deletes expired keys up to a time budget to avoid
    /// blocking the event loop for too long.
    fn run_active_expiry(&mut self) {
        let budget = Duration::from_millis(25);
        let start = Instant::now();
        let now = Instant::now();

        // Get expired keys from the expiry index
        if let Some(expiry_index) = self.store.expiry_index() {
            let expired = expiry_index.get_expired(now);

            for key in expired {
                if start.elapsed() > budget {
                    tracing::trace!(
                        shard_id = self.shard_id,
                        "Active expiry budget exhausted"
                    );
                    break;
                }

                // Delete the key
                self.store.delete(&key);
                tracing::trace!(
                    shard_id = self.shard_id,
                    key = %String::from_utf8_lossy(&key),
                    "Active expiry deleted key"
                );
            }
        }
    }

    /// Handle a new connection assigned to this shard.
    async fn handle_new_connection(&self, new_conn: NewConnection) {
        tracing::debug!(
            shard_id = self.shard_id,
            conn_id = new_conn.conn_id,
            addr = %new_conn.addr,
            "New connection assigned to shard"
        );

        // Connection handling is spawned as a separate task
        // The actual connection loop is implemented in the server crate
    }

    /// Execute a command locally.
    fn execute_command(&mut self, command: &ParsedCommand, conn_id: u64) -> Response {
        let cmd_name = command.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);

        let handler = match self.registry.get(&cmd_name_str) {
            Some(h) => h,
            None => {
                return Response::error(format!(
                    "ERR unknown command '{}', with args beginning with:",
                    cmd_name_str
                ))
            }
        };

        // Validate arity
        if !handler.arity().check(command.args.len()) {
            return Response::error(format!(
                "ERR wrong number of arguments for '{}' command",
                handler.name()
            ));
        }

        // Create command context
        // Note: We need a mutable reference to the store, but we're inside ShardWorker
        // This is safe because each shard is single-threaded
        let store = &mut self.store as &mut dyn Store;
        let mut ctx = CommandContext::new(
            store,
            &self.shard_senders,
            self.shard_id,
            self.num_shards,
            conn_id,
        );

        // Execute
        match handler.execute(&mut ctx, &command.args) {
            Ok(response) => response,
            Err(err) => err.to_response(),
        }
    }

    /// Execute part of a scatter-gather operation.
    async fn execute_scatter_part(&mut self, keys: &[Bytes], operation: &ScatterOp) -> PartialResult {
        use crate::types::{KeyMetadata, Value};

        let results = match operation {
            ScatterOp::MGet => {
                keys.iter()
                    .map(|key| {
                        let response = match self.store.get(key) {
                            Some(value) => {
                                if let Some(sv) = value.as_string() {
                                    Response::bulk(sv.as_bytes())
                                } else {
                                    Response::null()
                                }
                            }
                            None => Response::null(),
                        };
                        (key.clone(), response)
                    })
                    .collect()
            }
            ScatterOp::MSet { pairs } => {
                let mut results = Vec::with_capacity(pairs.len());
                for (key, value) in pairs {
                    let val = Value::string(value.clone());
                    self.store.set(key.clone(), val.clone());

                    // Persist to WAL if enabled
                    if let Some(ref wal) = self.wal_writer {
                        let metadata = KeyMetadata::new(val.memory_size());
                        if let Err(e) = wal.write_set(key, &val, &metadata).await {
                            tracing::error!(key = %String::from_utf8_lossy(key), error = %e, "Failed to persist MSET");
                        }
                    }

                    results.push((key.clone(), Response::ok()));
                }
                results
            }
            ScatterOp::Del | ScatterOp::Unlink => {
                let mut results = Vec::with_capacity(keys.len());
                for key in keys {
                    let deleted = self.store.delete(key);

                    // Persist delete to WAL if enabled
                    if deleted {
                        if let Some(ref wal) = self.wal_writer {
                            if let Err(e) = wal.write_delete(key).await {
                                tracing::error!(key = %String::from_utf8_lossy(key), error = %e, "Failed to persist DEL");
                            }
                        }
                    }

                    results.push((key.clone(), Response::Integer(if deleted { 1 } else { 0 })));
                }
                results
            }
            ScatterOp::Exists => {
                keys.iter()
                    .map(|key| {
                        let exists = self.store.contains(key);
                        (key.clone(), Response::Integer(if exists { 1 } else { 0 }))
                    })
                    .collect()
            }
            ScatterOp::Touch => {
                keys.iter()
                    .map(|key| {
                        let touched = self.store.touch(key);
                        (key.clone(), Response::Integer(if touched { 1 } else { 0 }))
                    })
                    .collect()
            }
        };

        PartialResult { results }
    }
}

/// Extract hash tag from a key (Redis-compatible).
///
/// Rules:
/// - First `{` that has a matching `}` with at least one character between
/// - Nested braces: outer wins (first valid match)
/// - Empty braces `{}` are ignored (hash entire key)
pub fn extract_hash_tag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|&b| b == b'{')?;
    let close_offset = key[open + 1..].iter().position(|&b| b == b'}')?;
    let tag = &key[open + 1..open + 1 + close_offset];
    if tag.is_empty() {
        None
    } else {
        Some(tag)
    }
}

/// Number of Redis cluster hash slots.
pub const REDIS_CLUSTER_SLOTS: usize = 16384;

/// Determine which shard owns a key using Redis-compatible CRC16 hashing.
///
/// Uses the XMODEM variant of CRC16, same as Redis cluster.
/// The slot is calculated as: CRC16(key) % 16384 % num_shards
pub fn shard_for_key(key: &[u8], num_shards: usize) -> usize {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    let slot = crc16::State::<crc16::XMODEM>::calculate(hash_key) as usize % REDIS_CLUSTER_SLOTS;
    slot % num_shards
}

/// Calculate the Redis cluster slot for a key (0-16383).
pub fn slot_for_key(key: &[u8]) -> u16 {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    crc16::State::<crc16::XMODEM>::calculate(hash_key) % REDIS_CLUSTER_SLOTS as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_hash_tag_simple() {
        assert_eq!(extract_hash_tag(b"{user:1}:profile"), Some(b"user:1".as_slice()));
        assert_eq!(extract_hash_tag(b"{user:1}:settings"), Some(b"user:1".as_slice()));
    }

    #[test]
    fn test_extract_hash_tag_no_braces() {
        assert_eq!(extract_hash_tag(b"user:1:profile"), None);
    }

    #[test]
    fn test_extract_hash_tag_empty_braces() {
        assert_eq!(extract_hash_tag(b"foo{}bar"), None);
        assert_eq!(extract_hash_tag(b"{}"), None);
    }

    #[test]
    fn test_extract_hash_tag_nested() {
        // First { to first } after it
        assert_eq!(extract_hash_tag(b"{{foo}}"), Some(b"{foo".as_slice()));
    }

    #[test]
    fn test_extract_hash_tag_multiple() {
        // First valid tag wins
        assert_eq!(extract_hash_tag(b"foo{bar}{zap}"), Some(b"bar".as_slice()));
    }

    #[test]
    fn test_extract_hash_tag_empty_then_valid() {
        // Empty first braces skipped
        assert_eq!(extract_hash_tag(b"{}{valid}"), None); // Actually returns None because {} comes first
    }

    #[test]
    fn test_shard_for_key_consistent() {
        let key1 = b"user:123";
        let key2 = b"user:123";
        assert_eq!(shard_for_key(key1, 4), shard_for_key(key2, 4));
    }

    #[test]
    fn test_shard_for_key_hash_tag() {
        // Keys with same hash tag should go to same shard
        let key1 = b"{user:1}:profile";
        let key2 = b"{user:1}:settings";
        assert_eq!(shard_for_key(key1, 4), shard_for_key(key2, 4));
    }

    #[test]
    fn test_slot_for_key_hash_tag_colocation() {
        // Keys with same hash tag should map to the same slot
        let key1 = b"{user:1}:profile";
        let key2 = b"{user:1}:session";
        let key3 = b"{user:1}:settings";

        let slot1 = slot_for_key(key1);
        let slot2 = slot_for_key(key2);
        let slot3 = slot_for_key(key3);

        assert_eq!(slot1, slot2);
        assert_eq!(slot2, slot3);
    }

    #[test]
    fn test_slot_for_key_range() {
        // Slots should be in range 0-16383
        for i in 0..1000 {
            let key = format!("key:{}", i);
            let slot = slot_for_key(key.as_bytes());
            assert!(slot < REDIS_CLUSTER_SLOTS as u16);
        }
    }

    #[test]
    fn test_shard_distribution() {
        // Test that keys distribute across shards
        let num_shards = 4;
        let mut shard_counts = vec![0usize; num_shards];

        for i in 0..1000 {
            let key = format!("key:{}", i);
            let shard = shard_for_key(key.as_bytes(), num_shards);
            shard_counts[shard] += 1;
        }

        // Each shard should have at least some keys (distribution check)
        for count in &shard_counts {
            assert!(*count > 0, "Shard has no keys assigned");
        }
    }

    #[test]
    fn test_crc16_known_values() {
        // Test against known Redis CRC16 values
        // "123456789" should hash to 0x31C3 (12739) using XMODEM
        let crc = crc16::State::<crc16::XMODEM>::calculate(b"123456789");
        assert_eq!(crc, 0x31C3);
    }
}
