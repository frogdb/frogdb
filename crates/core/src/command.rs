//! Command trait and related types.

use std::sync::Arc;
use std::time::Duration;

use bitflags::bitflags;
use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};
use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::cluster::{ClusterNetworkFactory, ClusterRaft, ClusterState};
use crate::error::CommandError;
use crate::replication::{ReplicationState, ReplicationTrackerImpl};
use crate::shard::ShardMessage;
use crate::store::{Store, ValueType};

/// Defines HOW a command should be executed by the connection handler.
///
/// This replaces string-based routing decisions with explicit, type-safe
/// declarations of execution patterns.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum ExecutionStrategy {
    /// Standard: route to shard based on key, execute, return response.
    /// This is the default for most commands.
    #[default]
    Standard,

    /// Connection-level: handled directly by ConnectionHandler.
    /// The command's `execute()` method is NOT called through normal routing.
    ConnectionLevel(ConnectionLevelOp),

    /// Blocking: command may block waiting for data.
    /// Handler must be prepared to wait and manage blocked state.
    Blocking {
        /// Default timeout if not specified in command args.
        default_timeout: Option<Duration>,
    },

    /// Scatter-gather: distribute across multiple shards, merge results.
    /// Used for multi-key commands that span shards.
    ScatterGather {
        /// How to merge results from multiple shards.
        merge: MergeStrategy,
    },

    /// Raft consensus: requires cluster consensus before execution.
    /// Used for cluster topology changes.
    RaftConsensus,

    /// Async external: performs async I/O outside the normal shard path.
    /// Used for MIGRATE, DEBUG SLEEP, and similar commands.
    AsyncExternal,

    /// Server-wide: command needs to execute across all shards and merge results.
    /// Used for commands like SCAN, KEYS, DBSIZE, RANDOMKEY, FLUSHDB, FLUSHALL.
    ServerWide(ServerWideOp),
}

/// Operations that execute across all shards (server-wide commands).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerWideOp {
    /// SCAN: iterate keys across all shards with cursor-based pagination.
    Scan,
    /// KEYS: pattern match keys across all shards.
    Keys,
    /// DBSIZE: count keys across all shards and sum results.
    DbSize,
    /// RANDOMKEY: pick a random key from any shard.
    RandomKey,
    /// FLUSHDB: flush all keys in the database.
    FlushDb,
    /// FLUSHALL: flush all databases (same as FLUSHDB for single-db).
    FlushAll,
    /// SHUTDOWN: gracefully shutdown the server.
    Shutdown,
}

/// Operations handled at the connection level (not routed to shards).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionLevelOp {
    /// Pub/Sub commands: SUBSCRIBE, PUBLISH, etc.
    PubSub,
    /// Transaction commands: MULTI, EXEC, DISCARD, WATCH.
    Transaction,
    /// Scripting commands: EVAL, EVALSHA, SCRIPT.
    Scripting,
    /// Admin commands: CLIENT, CONFIG, ACL, DEBUG.
    Admin,
    /// Authentication: AUTH, HELLO.
    Auth,
    /// Connection state: ASKING, READONLY, READWRITE, QUIT, RESET.
    ConnectionState,
    /// Replication: PSYNC - requires connection takeover.
    Replication,
}

/// Strategy for merging results from scatter-gather operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeStrategy {
    /// Preserve key order in response (MGET).
    /// Results are assembled to match original key positions.
    OrderedArray,

    /// Sum integer results (DEL, EXISTS, TOUCH, UNLINK, DBSIZE).
    SumIntegers,

    /// Collect all keys into single array (KEYS).
    CollectKeys,

    /// Merge cursored scan results (SCAN).
    CursoredScan,

    /// All shards must return OK (MSET, FLUSHDB).
    AllOk,

    /// Command implements custom merge logic.
    Custom,
}

/// Trait for checking if the local node can form a quorum.
/// This is implemented by the failure detector to provide local quorum status.
pub trait QuorumChecker: Send + Sync {
    /// Check if this node can form a quorum with reachable nodes.
    fn has_quorum(&self) -> bool;

    /// Count the number of nodes reachable from this node's perspective.
    fn count_reachable_nodes(&self) -> usize;
}

/// Command trait that all Redis commands implement.
pub trait Command: Send + Sync {
    /// Command name (e.g., "GET", "SET", "ZADD").
    fn name(&self) -> &'static str;

    /// Expected argument count.
    fn arity(&self) -> Arity;

    /// Command behavior flags.
    fn flags(&self) -> CommandFlags;

    /// How this command should be executed.
    ///
    /// Defaults to `ExecutionStrategy::Standard` for backward compatibility.
    /// Override this to declare special execution patterns like blocking,
    /// scatter-gather, or connection-level handling.
    fn execution_strategy(&self) -> ExecutionStrategy {
        ExecutionStrategy::Standard
    }

    /// Execute the command.
    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError>;

    /// Extract key(s) from arguments for routing.
    ///
    /// Returns empty slice for keyless commands (PING, INFO, etc.).
    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]>;

    /// Whether this command requires all keys to be in the same slot.
    ///
    /// When true, the command will return CROSSSLOT error even if
    /// `allow_cross_slot_standalone` is enabled. This is used for
    /// commands like MSETNX that require atomicity across all keys.
    fn requires_same_slot(&self) -> bool {
        false
    }
}

/// Metadata trait for commands that don't execute through the normal path.
///
/// This is used for commands like pub/sub and transaction commands where the
/// connection handler intercepts them before normal routing. These commands
/// need metadata for introspection (COMMAND INFO) but don't implement full execution.
pub trait CommandMetadata: Send + Sync {
    /// Command name (e.g., "SUBSCRIBE", "MULTI").
    fn name(&self) -> &'static str;

    /// Expected argument count.
    fn arity(&self) -> Arity;

    /// Command behavior flags.
    fn flags(&self) -> CommandFlags;

    /// How this command should be executed.
    fn execution_strategy(&self) -> ExecutionStrategy;

    /// Extract key(s) from arguments for routing.
    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]>;
}

/// Specifies the expected number of arguments for a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arity {
    /// Exactly N arguments (e.g., GET = Fixed(1)).
    Fixed(usize),

    /// At least N arguments (e.g., MGET = AtLeast(1)).
    AtLeast(usize),

    /// Between min and max arguments inclusive.
    Range { min: usize, max: usize },
}

impl Arity {
    /// Check if the given argument count is valid for this arity.
    pub fn check(&self, count: usize) -> bool {
        match self {
            Arity::Fixed(n) => count == *n,
            Arity::AtLeast(n) => count >= *n,
            Arity::Range { min, max } => count >= *min && count <= *max,
        }
    }
}

bitflags! {
    /// Command behavior flags for routing and optimization.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CommandFlags: u32 {
        /// Command modifies data (SET, DEL, ZADD).
        const WRITE = 0b0000_0000_0001;

        /// Command only reads data (GET, ZRANGE).
        const READONLY = 0b0000_0000_0010;

        /// O(1) operation, suitable for latency-sensitive paths.
        const FAST = 0b0000_0000_0100;

        /// May block the connection (BLPOP, BRPOP).
        const BLOCKING = 0b0000_0000_1000;

        /// Operates on multiple keys (MGET, MSET, DEL with multiple keys).
        const MULTI_KEY = 0b0000_0001_0000;

        /// Pub/sub command, connection enters pub/sub mode.
        const PUBSUB = 0b0000_0010_0000;

        /// Script execution (EVAL, EVALSHA).
        const SCRIPT = 0b0000_0100_0000;

        /// Command cannot be called from Lua scripts.
        const NOSCRIPT = 0b0000_1000_0000;

        /// Command allowed during database loading (startup recovery).
        const LOADING = 0b0001_0000_0000;

        /// Command allowed on stale replica.
        const STALE = 0b0010_0000_0000;

        /// Command should not be logged to slowlog.
        const SKIP_SLOWLOG = 0b0100_0000_0000;

        /// Command may involve random data.
        const RANDOM = 0b1000_0000_0000;

        /// Command modifies server state (not data).
        const ADMIN = 0b0001_0000_0000_0000;

        /// Command returns data that varies by time.
        const NONDETERMINISTIC = 0b0010_0000_0000_0000;

        /// Command should not be propagated to replicas.
        const NO_PROPAGATE = 0b0100_0000_0000_0000;
    }
}

// ============================================================================
// Context Helper Structs
// ============================================================================

/// Cluster-related context fields (populated in cluster mode).
///
/// This groups cluster-specific fields from CommandContext for cleaner access.
pub struct ClusterContextRef<'a> {
    /// Cluster state with slot assignments and node topology.
    pub state: &'a Arc<ClusterState>,
    /// This node's ID in the cluster.
    pub node_id: u64,
    /// Raft instance for consensus operations.
    pub raft: &'a Arc<ClusterRaft>,
    /// Network factory for cluster communications.
    pub network_factory: &'a Arc<ClusterNetworkFactory>,
}

/// Replication-related context fields (populated when replication is enabled).
pub struct ReplicationContextRef<'a> {
    /// Replication tracker for WAIT command and replica ACKs.
    pub tracker: &'a Arc<ReplicationTrackerImpl>,
    /// Replication state for INFO replication section.
    pub state: Option<&'a Arc<RwLock<ReplicationState>>>,
}

// ============================================================================
// CommandContextCore
// ============================================================================

/// Core context fields that 90% of commands need.
///
/// This is a lightweight view into `CommandContext` containing only the
/// essential fields. Use this for commands that don't need cluster or
/// replication features.
///
/// # Example
///
/// ```rust,ignore
/// fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
///     // Get just the core fields we need
///     let core = ctx.as_core();
///
///     // Access store, shard info, connection details
///     let value = core.store.get(&args[0]);
///     let shard = core.shard_id;
/// }
/// ```
pub struct CommandContextCore<'a> {
    /// Local shard's data store.
    pub store: &'a mut dyn Store,

    /// For commands that need to reach other shards.
    pub shard_senders: &'a Arc<Vec<mpsc::Sender<ShardMessage>>>,

    /// This shard's ID.
    pub shard_id: usize,

    /// Total number of shards.
    pub num_shards: usize,

    /// Connection ID (for client-specific operations).
    pub conn_id: u64,

    /// Protocol version for response encoding (RESP2 or RESP3).
    pub protocol_version: ProtocolVersion,
}

impl<'a> CommandContextCore<'a> {
    /// Get or create a value of a specific type.
    ///
    /// If the key doesn't exist, creates a new default value of type `T`.
    /// If the key exists but is the wrong type, returns `WrongType` error.
    pub fn get_or_create<T: ValueType>(&mut self, key: &Bytes) -> Result<&mut T, CommandError> {
        // Check if key exists and is wrong type
        if let Some(value) = self.store.get(key) {
            if T::from_value(&value).is_none() {
                return Err(CommandError::WrongType);
            }
        } else {
            // Create new value
            self.store.set(key.clone(), T::create_default());
        }

        // Get mutable reference
        self.store
            .get_mut(key)
            .and_then(T::from_value_mut)
            .ok_or(CommandError::WrongType)
    }
}

// ============================================================================
// CommandContext
// ============================================================================

/// Context provided to commands during execution.
///
/// This struct provides all the context a command needs to execute, including:
/// - Access to the local shard's data store
/// - Shard routing information
/// - Connection and protocol details
/// - Optional cluster and replication context
///
/// # Cluster Mode
///
/// In cluster mode, use [`cluster_context()`](Self::cluster_context) to get a
/// typed reference to cluster-specific fields:
///
/// ```rust,ignore
/// if let Some(cluster) = ctx.cluster_context() {
///     let state = cluster.state;
///     let node_id = cluster.node_id;
/// }
/// ```
///
/// # Replication
///
/// For replication-aware commands, use [`replication_context()`](Self::replication_context):
///
/// ```rust,ignore
/// if let Some(repl) = ctx.replication_context() {
///     let acks = repl.tracker.wait_for_acks(offset, count, timeout).await;
/// }
/// ```
pub struct CommandContext<'a> {
    /// Local shard's data store.
    pub store: &'a mut dyn Store,

    /// For commands that need to reach other shards.
    pub shard_senders: &'a Arc<Vec<mpsc::Sender<ShardMessage>>>,

    /// This shard's ID.
    pub shard_id: usize,

    /// Total number of shards.
    pub num_shards: usize,

    /// Connection ID (for client-specific operations).
    pub conn_id: u64,

    /// Protocol version for response encoding (RESP2 or RESP3).
    pub protocol_version: ProtocolVersion,

    /// Optional replication tracker for WAIT command and replica ACKs.
    pub replication_tracker: Option<&'a Arc<ReplicationTrackerImpl>>,

    /// Optional replication state for INFO replication section.
    pub replication_state: Option<&'a Arc<RwLock<ReplicationState>>>,

    /// Optional cluster state for cluster commands and routing.
    pub cluster_state: Option<&'a Arc<ClusterState>>,

    /// This node's ID (for cluster mode).
    pub node_id: Option<u64>,

    /// Optional Raft instance for cluster command execution.
    pub raft: Option<&'a Arc<ClusterRaft>>,

    /// Optional network factory for cluster node management.
    pub network_factory: Option<&'a Arc<ClusterNetworkFactory>>,

    /// Optional quorum checker for local cluster health detection.
    pub quorum_checker: Option<&'a dyn QuorumChecker>,
}

impl<'a> CommandContext<'a> {
    /// Create a new command context.
    pub fn new(
        store: &'a mut dyn Store,
        shard_senders: &'a Arc<Vec<mpsc::Sender<ShardMessage>>>,
        shard_id: usize,
        num_shards: usize,
        conn_id: u64,
        protocol_version: ProtocolVersion,
    ) -> Self {
        Self {
            store,
            shard_senders,
            shard_id,
            num_shards,
            conn_id,
            protocol_version,
            replication_tracker: None,
            replication_state: None,
            cluster_state: None,
            node_id: None,
            raft: None,
            network_factory: None,
            quorum_checker: None,
        }
    }

    /// Create a new command context with cluster/replication support.
    #[allow(clippy::too_many_arguments)]
    pub fn with_cluster(
        store: &'a mut dyn Store,
        shard_senders: &'a Arc<Vec<mpsc::Sender<ShardMessage>>>,
        shard_id: usize,
        num_shards: usize,
        conn_id: u64,
        protocol_version: ProtocolVersion,
        replication_tracker: Option<&'a Arc<ReplicationTrackerImpl>>,
        replication_state: Option<&'a Arc<RwLock<ReplicationState>>>,
        cluster_state: Option<&'a Arc<ClusterState>>,
        node_id: Option<u64>,
        raft: Option<&'a Arc<ClusterRaft>>,
        network_factory: Option<&'a Arc<ClusterNetworkFactory>>,
        quorum_checker: Option<&'a dyn QuorumChecker>,
    ) -> Self {
        Self {
            store,
            shard_senders,
            shard_id,
            num_shards,
            conn_id,
            protocol_version,
            replication_tracker,
            replication_state,
            cluster_state,
            node_id,
            raft,
            network_factory,
            quorum_checker,
        }
    }

    /// Check if this context is in cluster mode.
    #[inline]
    pub fn is_cluster_mode(&self) -> bool {
        self.cluster_state.is_some()
    }

    /// Extract a core context view containing only essential fields.
    ///
    /// This is useful for commands that don't need cluster or replication
    /// features. The extraction is zero-cost - it just reborrows fields.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    ///     let core = ctx.as_core();
    ///     // Use core.store, core.shard_id, etc.
    /// }
    /// ```
    ///
    /// # Note
    ///
    /// Because this borrows `self.store` mutably, you cannot use the original
    /// `CommandContext` while the `CommandContextCore` is alive. This is
    /// enforced by the borrow checker.
    #[inline]
    pub fn as_core(&mut self) -> CommandContextCore<'_> {
        CommandContextCore {
            store: self.store,
            shard_senders: self.shard_senders,
            shard_id: self.shard_id,
            num_shards: self.num_shards,
            conn_id: self.conn_id,
            protocol_version: self.protocol_version,
        }
    }

    /// Get cluster context if in cluster mode.
    ///
    /// Returns `None` in standalone mode.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// if let Some(cluster) = ctx.cluster_context() {
    ///     println!("Node ID: {}", cluster.node_id);
    ///     println!("Cluster state: {:?}", cluster.state);
    /// }
    /// ```
    pub fn cluster_context(&self) -> Option<ClusterContextRef<'_>> {
        let state = self.cluster_state.as_ref()?;
        let node_id = self.node_id?;
        let raft = self.raft.as_ref()?;
        let network_factory = self.network_factory.as_ref()?;

        Some(ClusterContextRef {
            state,
            node_id,
            raft,
            network_factory,
        })
    }

    /// Get replication context if replication is enabled.
    ///
    /// Returns `None` if replication is not configured.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// if let Some(repl) = ctx.replication_context() {
    ///     let current_offset = repl.tracker.current_offset();
    /// }
    /// ```
    pub fn replication_context(&self) -> Option<ReplicationContextRef<'_>> {
        let tracker = self.replication_tracker.as_ref()?;

        Some(ReplicationContextRef {
            tracker,
            state: self.replication_state,
        })
    }

    /// Check if replication is enabled.
    #[inline]
    pub fn has_replication(&self) -> bool {
        self.replication_tracker.is_some()
    }

    /// Get cluster context, returning an error if not in cluster mode.
    ///
    /// Use this when a command requires cluster mode to execute.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let cluster = ctx.require_cluster()?;
    /// let node_id = cluster.node_id;
    /// ```
    #[inline]
    pub fn require_cluster(&self) -> Result<ClusterContextRef<'_>, CommandError> {
        self.cluster_context().ok_or(CommandError::ClusterDisabled)
    }

    /// Get replication context, returning an error if replication is not enabled.
    ///
    /// Use this when a command requires replication to execute.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let repl = ctx.require_replication()?;
    /// let offset = repl.tracker.current_offset();
    /// ```
    #[inline]
    pub fn require_replication(&self) -> Result<ReplicationContextRef<'_>, CommandError> {
        self.replication_context()
            .ok_or(CommandError::ReplicationDisabled)
    }

    /// Get or create a value of a specific type.
    ///
    /// If the key doesn't exist, creates a new default value of type `T`.
    /// If the key exists but is the wrong type, returns `WrongType` error.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use frogdb_core::{CommandContext, ListValue};
    ///
    /// fn push_to_list(ctx: &mut CommandContext, key: &Bytes) -> Result<(), CommandError> {
    ///     let list = ctx.get_or_create::<ListValue>(key)?;
    ///     list.push_back(Bytes::from("item"));
    ///     Ok(())
    /// }
    /// ```
    pub fn get_or_create<T: ValueType>(&mut self, key: &Bytes) -> Result<&mut T, CommandError> {
        // Check if key exists and is wrong type
        if let Some(value) = self.store.get(key) {
            if T::from_value(&value).is_none() {
                return Err(CommandError::WrongType);
            }
        } else {
            // Create new value
            self.store.set(key.clone(), T::create_default());
        }

        // Get mutable reference
        self.store
            .get_mut(key)
            .and_then(T::from_value_mut)
            .ok_or(CommandError::WrongType)
    }
}

/// Get or create a value of a specific type from a command context.
///
/// This is a standalone function for cases where you need to call it
/// without borrowing the full context.
///
/// If the key doesn't exist, creates a new default value of type `T`.
/// If the key exists but is the wrong type, returns `WrongType` error.
///
/// # Example
///
/// ```rust,ignore
/// use frogdb_core::{get_or_create, ListValue, CommandContext};
///
/// fn push_to_list(ctx: &mut CommandContext, key: &Bytes) -> Result<(), CommandError> {
///     let list = get_or_create::<ListValue>(ctx, key)?;
///     list.push_back(Bytes::from("item"));
///     Ok(())
/// }
/// ```
pub fn get_or_create<'a, T: ValueType>(
    ctx: &'a mut CommandContext,
    key: &Bytes,
) -> Result<&'a mut T, CommandError> {
    ctx.get_or_create(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arity_fixed() {
        let arity = Arity::Fixed(2);
        assert!(!arity.check(1));
        assert!(arity.check(2));
        assert!(!arity.check(3));
    }

    #[test]
    fn test_arity_at_least() {
        let arity = Arity::AtLeast(1);
        assert!(!arity.check(0));
        assert!(arity.check(1));
        assert!(arity.check(5));
    }

    #[test]
    fn test_arity_range() {
        let arity = Arity::Range { min: 1, max: 3 };
        assert!(!arity.check(0));
        assert!(arity.check(1));
        assert!(arity.check(2));
        assert!(arity.check(3));
        assert!(!arity.check(4));
    }

    #[test]
    fn test_command_flags() {
        let flags = CommandFlags::READONLY | CommandFlags::FAST;
        assert!(flags.contains(CommandFlags::READONLY));
        assert!(flags.contains(CommandFlags::FAST));
        assert!(!flags.contains(CommandFlags::WRITE));
    }
}
