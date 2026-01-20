//! Shard infrastructure for shared-nothing concurrency.

use std::sync::Arc;

use bytes::Bytes;
use frogdb_protocol::{ParsedCommand, Response};
use tokio::sync::{mpsc, oneshot};
use xxhash_rust::xxh64::xxh64;

use crate::command::{Command, CommandContext};
use crate::registry::CommandRegistry;
use crate::store::{HashMapStore, Store};

/// Messages sent to shard workers.
#[derive(Debug)]
pub enum ShardMessage {
    /// Execute a command on this shard.
    Execute {
        command: ParsedCommand,
        conn_id: u64,
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
    /// GET operation.
    MGet,
    /// DELETE operation.
    Del,
    /// EXISTS operation.
    Exists,
}

/// Result from a shard for scatter-gather operations.
#[derive(Debug)]
pub struct PartialResult {
    /// Results keyed by original key position.
    pub results: Vec<(Bytes, Response)>,
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
}

impl ShardWorker {
    /// Create a new shard worker.
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
        }
    }

    /// Run the shard worker event loop.
    pub async fn run(mut self) {
        tracing::info!(shard_id = self.shard_id, "Shard worker started");

        loop {
            tokio::select! {
                // Handle new connections
                Some(new_conn) = self.new_conn_rx.recv() => {
                    self.handle_new_connection(new_conn).await;
                }

                // Handle shard messages
                Some(msg) = self.message_rx.recv() => {
                    match msg {
                        ShardMessage::Execute { command, conn_id, response_tx } => {
                            let response = self.execute_command(&command, conn_id);
                            let _ = response_tx.send(response);
                        }
                        ShardMessage::ScatterRequest { request_id: _, keys, operation, response_tx } => {
                            let result = self.execute_scatter_part(&keys, &operation);
                            let _ = response_tx.send(result);
                        }
                        ShardMessage::Shutdown => {
                            tracing::info!(shard_id = self.shard_id, "Shard worker shutting down");
                            break;
                        }
                    }
                }

                else => break,
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
    fn execute_scatter_part(&mut self, keys: &[Bytes], operation: &ScatterOp) -> PartialResult {
        let results = keys
            .iter()
            .map(|key| {
                let response = match operation {
                    ScatterOp::MGet => {
                        match self.store.get(key) {
                            Some(value) => {
                                if let Some(sv) = value.as_string() {
                                    Response::bulk(sv.as_bytes())
                                } else {
                                    Response::null()
                                }
                            }
                            None => Response::null(),
                        }
                    }
                    ScatterOp::Del => {
                        let deleted = self.store.delete(key);
                        Response::Integer(if deleted { 1 } else { 0 })
                    }
                    ScatterOp::Exists => {
                        let exists = self.store.contains(key);
                        Response::Integer(if exists { 1 } else { 0 })
                    }
                };
                (key.clone(), response)
            })
            .collect();

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

/// Determine which shard owns a key.
pub fn shard_for_key(key: &[u8], num_shards: usize) -> usize {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    let hash = xxh64(hash_key, 0);
    (hash as usize) % num_shards
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
}
