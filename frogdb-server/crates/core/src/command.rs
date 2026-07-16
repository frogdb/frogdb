//! Command trait and related types.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use crate::cluster::{ClusterNetworkFactory, ClusterRaft, ClusterState};
use crate::command_spec::{AccessSpec, CommandSpec, KeySpec};
use crate::error::CommandError;
use crate::keyspace_event::KeyspaceEventFlags;
use crate::registry::CommandRegistry;
use crate::replication::ReplicationTrackerImpl;
use crate::shard::ShardSender;
use crate::store::{DefaultValueType, Store, StoreTypedExt};
use crate::types::ListpackThresholds;
use bitflags::bitflags;
use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};
use smallvec::{SmallVec, smallvec};

/// Listpack encoding configuration for hash and set types.
///
/// These values are read from CONFIG at command execution time and passed
/// to mutating hash/set operations to control when to promote from
/// listpack to the standard hash table / hash set encoding.
#[derive(Clone, Copy, Debug)]
pub struct ListpackConfig {
    pub hash_max_entries: usize,
    pub hash_max_value: usize,
    pub set_max_entries: usize,
    pub set_max_value: usize,
}

impl Default for ListpackConfig {
    fn default() -> Self {
        Self {
            hash_max_entries: 128,
            hash_max_value: 64,
            set_max_entries: 128,
            set_max_value: 64,
        }
    }
}

impl ListpackConfig {
    /// Get hash thresholds.
    pub fn hash_thresholds(&self) -> ListpackThresholds {
        ListpackThresholds {
            max_entries: self.hash_max_entries,
            max_value_bytes: self.hash_max_value,
        }
    }

    /// Get set thresholds.
    pub fn set_thresholds(&self) -> ListpackThresholds {
        ListpackThresholds {
            max_entries: self.set_max_entries,
            max_value_bytes: self.set_max_value,
        }
    }
}

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

/// Identifies a server-wide command (executes across all shards via a
/// ConnectionHandler method). Mirrors [`ConnectionLevelOp`]: pure command
/// identity — handler calling conventions live in the dispatch match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerWideOp {
    Scan,
    Keys,
    DbSize,
    RandomKey,
    FlushDb,
    FlushAll,
    Migrate,
    Shutdown,
    TsQueryIndex,
    TsMGet,
    TsMRange,
    TsMRevRange,
    FtCreate,
    FtSearch,
    FtDropIndex,
    FtInfo,
    FtList,
    FtAlter,
    FtSynUpdate,
    FtSynDump,
    FtAggregate,
    FtHybrid,
    FtAliasAdd,
    FtAliasDel,
    FtAliasUpdate,
    FtTagVals,
    FtDictAdd,
    FtDictDel,
    FtDictDump,
    FtConfig,
    FtSpellCheck,
    FtExplain,
    FtExplainCli,
    FtProfile,
    EsAll,
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
    /// Persistence: BGSAVE, LASTSAVE.
    Persistence,
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

/// What kind of blocking waiter a write command may satisfy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaiterKind {
    List,
    SortedSet,
    Stream,
}

/// Describes which blocking waiter kinds a write command may satisfy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaiterWake {
    /// Does not wake any waiters (default).
    None,
    /// Wakes waiters of a specific kind.
    Kind(WaiterKind),
    /// Wakes waiters of all kinds (used by RENAME which can move any type).
    All,
}

/// Declares how a write command's effects should be persisted to the WAL.
///
/// Every command overrides `wal_strategy()` to declare its persistence
/// behavior. The default is `NoOp`, which is correct for read commands;
/// every `WRITE`-flagged command must declare an explicit non-default
/// strategy.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum WalStrategy {
    /// Persist the current value of the first key (args[0]).
    /// Used by SET, APPEND, INCR, LPUSH, SADD, ZADD, HSET, etc.
    PersistFirstKey,

    /// Persist deletion for each arg key that no longer exists.
    /// Used by DEL, UNLINK, GETDEL.
    DeleteKeys,

    /// If the first key still exists, persist it; otherwise persist deletion.
    /// Used by LPOP, RPOP, SPOP, SREM, ZPOPMIN, HDEL, LTRIM, etc.
    PersistOrDeleteFirstKey,

    /// Delete old key (args[0]) if gone, persist new key (args[1]).
    /// Used by RENAME, RENAMENX.
    RenameKeys,

    /// Persist-or-delete source (args[0]), persist destination (args[1]).
    /// Used by RPOPLPUSH, LMOVE.
    MoveKeys,

    /// Persist destination key at the given arg index if it exists.
    /// Used by SINTERSTORE (0), LMOVE dest (1), COPY dest (1), ZRANGESTORE (0).
    PersistDestination(usize),

    /// If the destination key at the given arg index still exists, persist it;
    /// otherwise persist its deletion. Used by STORE-style commands that delete
    /// the destination on an empty result (BITOP dest (1)): unlike
    /// [`WalStrategy::PersistDestination`], a delete-on-empty must be written to
    /// the WAL so the removal survives a restart, rather than leaving the stale
    /// prior value authoritative on disk.
    PersistOrDeleteDestination(usize),

    /// Persist the first key (args[0]); but if the command deposited a
    /// HyperLogLog register delta on the context
    /// ([`CommandEffects::hll_wal_delta`](crate::command::CommandEffects::hll_wal_delta)), emit a `Merge` operand carrying
    /// only the raised registers instead of a full `Put`. Used by PFADD on a
    /// dense existing HLL: the delta serializes far smaller than the 12 KiB
    /// dense value, and the CF merge operator folds it onto the base on replay.
    /// With no delta (sparse or newly-created value) it falls back to a full
    /// `Persist` of the first key.
    MergeDeltaOrPersistFirstKey,

    /// Persist every write-access key returned by the command's key
    /// extraction, deriving destinations from values rather than fixed arg
    /// indexes. Used by commands whose destination key is positionally dynamic
    /// (SORT…STORE, GEORADIUS…STORE), where an index-based strategy cannot
    /// locate the destination. Resolved by [`Command::wal_actions`] against
    /// the command's `keys_with_flags`, not by [`WalStrategy::actions`].
    Dynamic,

    /// Clear the entire shard: persist a full-range delete of the shard's
    /// primary column family through the WAL flush pipeline. Used by FLUSHDB and
    /// FLUSHALL, whose clear is unconditional (not `Dynamic`). The in-memory
    /// store — and, when tiered storage is enabled, the warm column family — are
    /// cleared synchronously by the command's `store.clear()`; this strategy adds
    /// the primary-CF range tombstone so a flush survives a restart.
    ClearShard,

    /// No WAL action needed.
    /// Used by read commands (and other writes whose effects are not persisted).
    #[default]
    NoOp,
}

/// A typed WAL persistence action against a single key.
///
/// `WalStrategy::actions()` resolves a strategy + args to a sequence of these.
/// Each action describes precisely what should be written to the WAL for one key,
/// independent of how the command itself executes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalAction<'a> {
    /// Always write the current in-store value for this key.
    Persist(&'a [u8]),

    /// Write a delete for this key iff `!store.contains(key)`. (No-op if the key
    /// still exists — the prior value is still authoritative.)
    DeleteIfMissing(&'a [u8]),

    /// If the key exists, write its current value; otherwise write a delete.
    PersistOrDelete(&'a [u8]),

    /// If the key exists, write its current value; otherwise do nothing.
    PersistIfExists(&'a [u8]),

    /// Fold a HyperLogLog register-max delta onto the key's on-disk value via
    /// the CF merge operator, instead of rewriting the full value. `pairs` are
    /// the `(register_index, new_value)` entries this write raised.
    MergeHllDelta {
        key: &'a [u8],
        pairs: &'a [(u16, u8)],
    },

    /// Clear the entire shard: persist a full-range delete of the shard's
    /// primary column family. Carries no key — it targets the whole CF. Emitted
    /// only by [`WalStrategy::ClearShard`] (FLUSHDB / FLUSHALL).
    ClearShard,
}

impl<'a> WalAction<'a> {
    /// Get the key this action targets, or an empty slice for keyless actions
    /// ([`WalAction::ClearShard`] targets the whole CF, not a single key).
    pub fn key(&self) -> &'a [u8] {
        match self {
            WalAction::Persist(k)
            | WalAction::DeleteIfMissing(k)
            | WalAction::PersistOrDelete(k)
            | WalAction::PersistIfExists(k)
            | WalAction::MergeHllDelta { key: k, .. } => k,
            WalAction::ClearShard => &[],
        }
    }
}

/// One keyspace event deposited by an [`crate::command_spec::EventSpec::Dynamic`]
/// command via [`CommandContext::notify_event`]: the written key, the event
/// name, and its class.
pub type KeyspaceEventDeposit = (Bytes, &'static str, KeyspaceEventFlags);

/// The per-command deposit buffer for [`KeyspaceEventDeposit`]s. Sized for the
/// dominant Dynamic shapes (a move/rename touches two keys; a pop touches one).
pub type KeyspaceEventDeposits = SmallVec<[KeyspaceEventDeposit; 2]>;

/// One write performed by a command execution — the unit the post-execution
/// pipeline and the rollback path both operate on.
///
/// Carries the handler and its args (for keyspace notifications, replication,
/// tracking, search) plus the runtime facts the handler deposited on its
/// [`CommandContext`]: an optional HyperLogLog register delta (lets
/// [`WalStrategy::MergeDeltaOrPersistFirstKey`] persist a compact `Merge`
/// operand rather than a full value `Put`) and the keyspace events of an
/// [`crate::command_spec::EventSpec::Dynamic`] command.
#[derive(Clone, Copy)]
pub struct WriteRecord<'a> {
    /// The command handler that performed the write.
    pub handler: &'a dyn Command,
    /// The command's arguments.
    pub args: &'a [Bytes],
    /// HyperLogLog register delta, if this write deposited one (dense PFADD).
    pub hll_wal_delta: Option<&'a [(u16, u8)]>,
    /// Keyspace events deposited via [`CommandContext::notify_event`]. Only
    /// consulted for [`crate::command_spec::EventSpec::Dynamic`] commands;
    /// empty for every other write.
    pub keyspace_events: &'a [KeyspaceEventDeposit],
}

impl<'a> WriteRecord<'a> {
    /// Create a record with no HLL delta and no deposited events (the common case).
    pub fn new(handler: &'a dyn Command, args: &'a [Bytes]) -> Self {
        Self {
            handler,
            args,
            hll_wal_delta: None,
            keyspace_events: &[],
        }
    }

    /// Resolve this record to its concrete WAL actions.
    ///
    /// The single delta-vs-full routing decision, shared by both the write-effect
    /// path ([`super::shard`] `persist_by_strategy`) and the rollback path
    /// (`persist_and_confirm` / `persist_transaction_to_wal`), so the two can
    /// never disagree on whether a dense PFADD becomes a `Merge` or a `Put`.
    pub fn wal_actions(&self) -> SmallVec<[WalAction<'_>; 2]> {
        self.handler
            .wal_actions_with_delta(self.args, self.hll_wal_delta)
    }
}

/// An owned [`WriteRecord`]: one effective write performed by a script
/// sub-command (`redis.call` / `redis.pcall` executing on the local shard).
///
/// The scripting seam cannot run write effects mid-script — the store is
/// mutably borrowed by the [`ScriptInvoker`](crate::scripting) for the whole
/// execution — so each effective write is captured with owned data here and
/// drained after the script completes, when the shard worker routes the batch
/// through the same `WRITE_EFFECT_ORDER` pipeline as direct commands.
pub struct ScriptWriteRecord {
    /// The command handler that performed the write.
    pub handler: Arc<dyn Command>,
    /// The sub-command's arguments (owned: the Lua-side buffers do not outlive
    /// the call).
    pub args: Vec<Bytes>,
    /// The dirty delta the sub-command reported (WATCH no-op rule input).
    pub dirty_delta: i64,
    /// HyperLogLog register delta, if this write deposited one (dense PFADD).
    pub hll_wal_delta: Option<SmallVec<[(u16, u8); 8]>>,
    /// Keyspace events deposited via [`CommandContext::notify_event`]
    /// (consulted for [`crate::command_spec::EventSpec::Dynamic`] commands).
    pub keyspace_events: KeyspaceEventDeposits,
}

impl ScriptWriteRecord {
    /// View this owned record as the borrowed [`WriteRecord`] the write-effect
    /// pipeline consumes.
    pub fn as_write_record(&self) -> WriteRecord<'_> {
        WriteRecord {
            handler: self.handler.as_ref(),
            args: self.args.as_slice(),
            hll_wal_delta: self.hll_wal_delta.as_deref(),
            keyspace_events: self.keyspace_events.as_slice(),
        }
    }
}

impl WalStrategy {
    /// Resolve this strategy + args to the concrete sequence of per-key WAL actions.
    ///
    /// Convenience wrapper over [`WalStrategy::actions_with_delta`] for callers
    /// that never carry a HyperLogLog register delta.
    pub fn actions<'a>(&self, args: &'a [Bytes]) -> SmallVec<[WalAction<'a>; 2]> {
        self.actions_with_delta(args, None)
    }

    /// Resolve this strategy + args (+ optional HLL register delta) to the
    /// concrete sequence of per-key WAL actions.
    ///
    /// This is the single source of truth that maps a strategy variant to actions.
    /// Adding a new strategy variant requires extending this match — and only this match.
    /// `hll_delta` is consulted only by [`WalStrategy::MergeDeltaOrPersistFirstKey`];
    /// every other variant ignores it.
    pub fn actions_with_delta<'a>(
        &self,
        args: &'a [Bytes],
        hll_delta: Option<&'a [(u16, u8)]>,
    ) -> SmallVec<[WalAction<'a>; 2]> {
        match self {
            WalStrategy::MergeDeltaOrPersistFirstKey => match args.first() {
                Some(key) => match hll_delta {
                    Some(pairs) => smallvec![WalAction::MergeHllDelta { key, pairs }],
                    None => smallvec![WalAction::Persist(key)],
                },
                None => SmallVec::new(),
            },
            WalStrategy::PersistFirstKey => match args.first() {
                Some(key) => smallvec![WalAction::Persist(key)],
                None => SmallVec::new(),
            },
            WalStrategy::DeleteKeys => args
                .iter()
                .map(|arg| WalAction::DeleteIfMissing(arg.as_ref()))
                .collect(),
            WalStrategy::PersistOrDeleteFirstKey => match args.first() {
                Some(key) => smallvec![WalAction::PersistOrDelete(key)],
                None => SmallVec::new(),
            },
            WalStrategy::RenameKeys => {
                if args.len() >= 2 {
                    smallvec![
                        WalAction::DeleteIfMissing(&args[0]),
                        WalAction::Persist(&args[1]),
                    ]
                } else {
                    SmallVec::new()
                }
            }
            WalStrategy::MoveKeys => {
                if args.len() >= 2 {
                    smallvec![
                        WalAction::PersistOrDelete(&args[0]),
                        WalAction::Persist(&args[1]),
                    ]
                } else {
                    SmallVec::new()
                }
            }
            WalStrategy::PersistDestination(idx) => match args.get(*idx) {
                Some(dest) => smallvec![WalAction::PersistIfExists(dest)],
                None => SmallVec::new(),
            },
            WalStrategy::PersistOrDeleteDestination(idx) => match args.get(*idx) {
                Some(dest) => smallvec![WalAction::PersistOrDelete(dest)],
                None => SmallVec::new(),
            },
            // Unconditional full-shard clear: one keyless action, independent of
            // args (FLUSHDB/FLUSHALL take no key arguments).
            WalStrategy::ClearShard => smallvec![WalAction::ClearShard],
            // Dynamic is resolved against the command's extracted write keys by
            // `Command::wal_actions`, which has access to `keys_with_flags`.
            // Resolving it from raw args alone is impossible, so this yields
            // nothing — callers must go through `Command::wal_actions`.
            WalStrategy::Dynamic | WalStrategy::NoOp => SmallVec::new(),
        }
    }
}

/// Command trait that all Redis commands implement.
///
/// Every mechanical fact about a command (name, arity, flags, key extraction,
/// keyspace event, WAL persistence, waiter waking, key access flags) is
/// declared once as a [`CommandSpec`] returned by [`Command::spec`]. The
/// remaining methods below are *derived* from the spec and are not overridable
/// per command. The only two things a command implements are [`Command::spec`]
/// and [`Command::execute`] (plus the [`Command::dynamic_keys`] escape hatch
/// for value-dependent key layouts).
pub trait Command: Send + Sync {
    /// Declarative specification of this command's mechanics. The single source
    /// of truth from which all derived methods below read.
    fn spec(&self) -> &'static CommandSpec;

    /// Execute the command.
    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError>;

    /// Command name (e.g., "GET", "SET", "ZADD"). Derived from the spec.
    fn name(&self) -> &'static str {
        self.spec().name
    }

    /// Expected argument count. Derived from the spec.
    fn arity(&self) -> Arity {
        self.spec().arity
    }

    /// Command behavior flags. Derived from the spec.
    fn flags(&self) -> CommandFlags {
        self.spec().flags
    }

    /// How this command should be executed. Derived from the spec's
    /// [`CommandSpec::strategy`] field — declared once, alongside every other
    /// mechanical fact, rather than as a per-command trait override.
    fn execution_strategy(&self) -> ExecutionStrategy {
        self.spec().strategy.clone()
    }

    /// How this command's effects are persisted to the WAL. Derived from the
    /// spec. For dynamic destinations, see [`Command::wal_actions`].
    fn wal_strategy(&self) -> WalStrategy {
        self.spec().wal.clone()
    }

    /// The concrete sequence of WAL actions for this command on `args`.
    ///
    /// Resolves the command's [`WalStrategy`] against its arguments. The
    /// [`WalStrategy::Dynamic`] strategy is resolved here against the command's
    /// extracted write-access keys (the same key extraction used for routing),
    /// so positionally-dynamic destinations (SORT…STORE, GEORADIUS…STORE) are
    /// persisted even though no fixed arg index locates them.
    fn wal_actions<'a>(&self, args: &'a [Bytes]) -> SmallVec<[WalAction<'a>; 2]> {
        self.wal_actions_with_delta(args, None)
    }

    /// Like [`Command::wal_actions`], but threads an optional HyperLogLog
    /// register delta through so [`WalStrategy::MergeDeltaOrPersistFirstKey`]
    /// can emit a `Merge` operand instead of a full `Put`. This is the single
    /// routing point shared by the effect path and the rollback path (see
    /// [`WriteRecord::wal_actions`]); the delta-vs-full decision lives here and
    /// nowhere else.
    fn wal_actions_with_delta<'a>(
        &self,
        args: &'a [Bytes],
        hll_delta: Option<&'a [(u16, u8)]>,
    ) -> SmallVec<[WalAction<'a>; 2]> {
        match self.wal_strategy() {
            WalStrategy::Dynamic => self
                .keys_with_flags(args)
                .into_iter()
                .filter(|(_, flags)| {
                    flags.iter().any(|f| {
                        matches!(f, KeyAccessFlag::W | KeyAccessFlag::OW | KeyAccessFlag::RW)
                    })
                })
                .map(|(key, _)| WalAction::PersistOrDelete(key))
                .collect(),
            other => other.actions_with_delta(args, hll_delta),
        }
    }

    /// Extract key(s) from arguments for routing.
    ///
    /// Derived from the spec's [`KeySpec`]. Dynamic key layouts defer to
    /// [`Command::dynamic_keys`]. Returns empty for keyless commands.
    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        match self.spec().keys {
            KeySpec::Dynamic => self.dynamic_keys(args),
            shape => shape.extract(args),
        }
    }

    /// Escape hatch for value-dependent key extraction.
    ///
    /// Only consulted when `spec().keys == KeySpec::Dynamic`. Implemented by
    /// SORT…STORE, GEORADIUS…STORE, XREAD STREAMS, and EVAL-style commands.
    fn dynamic_keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        Vec::new()
    }

    /// Extract key(s) with per-key access flags for `COMMAND GETKEYSANDFLAGS`.
    ///
    /// Derived from the spec's [`AccessSpec`]. Dynamic access defers to
    /// [`Command::dynamic_keys_with_flags`].
    fn keys_with_flags<'a>(&self, args: &'a [Bytes]) -> Vec<(&'a [u8], Vec<KeyAccessFlag>)> {
        let spec = self.spec();
        match spec.access {
            AccessSpec::Dynamic => self.dynamic_keys_with_flags(args),
            access => access.resolve(self.keys(args), spec.is_write()),
        }
    }

    /// Escape hatch for value-dependent per-key access flags.
    ///
    /// Only consulted when `spec().access == AccessSpec::Dynamic`. Defaults to
    /// deriving a uniform flag from `CommandFlags` over [`Command::dynamic_keys`].
    fn dynamic_keys_with_flags<'a>(
        &self,
        args: &'a [Bytes],
    ) -> Vec<(&'a [u8], Vec<KeyAccessFlag>)> {
        let keys = self.dynamic_keys(args);
        let flag = if self.flags().contains(CommandFlags::WRITE) {
            KeyAccessFlag::OW
        } else {
            KeyAccessFlag::R
        };
        keys.into_iter().map(|k| (k, vec![flag])).collect()
    }

    /// Whether this command requires all keys to be in the same slot. Derived
    /// from the spec. When true, the command returns CROSSSLOT even if
    /// `allow_cross_slot_standalone` is enabled (e.g. MSETNX).
    fn requires_same_slot(&self) -> bool {
        self.spec().requires_same_slot
    }
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

    /// The minimum number of arguments this arity accepts.
    pub fn min(&self) -> usize {
        match self {
            Arity::Fixed(n) | Arity::AtLeast(n) => *n,
            Arity::Range { min, .. } => *min,
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

        // Bit 0b1_0000_0000_0000_0000 was `TRACKS_KEYSPACE`; keyspace hit/miss
        // participation is now declared by `CommandSpec::lookup` (LookupSpec).

        /// Key positions depend on argument values (SORT, EVAL, MSETEX, XREAD).
        ///
        /// Commands with this flag cannot have their keys determined from a
        /// static specification alone; the argument values must be inspected.
        /// Reported by `COMMAND INFO` and used by `COMMAND GETKEYSANDFLAGS`.
        const MOVABLEKEYS = 0b10_0000_0000_0000_0000;
    }
}

/// Per-key access flag for `COMMAND GETKEYSANDFLAGS`.
///
/// Indicates how a command accesses each of its keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyAccessFlag {
    /// Read-only access.
    R,
    /// Write access (insert or overwrite).
    W,
    /// Overwrite-only access (key must already exist, or is created as a side effect).
    OW,
    /// Read and write access.
    RW,
}

impl KeyAccessFlag {
    /// Return the Redis-canonical string for this flag.
    pub fn as_str(&self) -> &'static str {
        match self {
            KeyAccessFlag::R => "R",
            KeyAccessFlag::W => "W",
            KeyAccessFlag::OW => "OW",
            KeyAccessFlag::RW => "RW",
        }
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
    pub shard_senders: &'a Arc<Vec<ShardSender>>,

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
    pub fn get_or_create<T: DefaultValueType>(
        &mut self,
        key: &Bytes,
    ) -> Result<&mut T, CommandError> {
        Ok(self.store.get_or_create_typed(key)?)
    }
}

// ============================================================================
// CommandEffects
// ============================================================================

/// Everything a command execution *produces* besides its [`Response`] — the
/// out-buffer half of [`CommandContext`], as one named value.
///
/// Handlers deposit into this struct (via [`CommandContext::notify_event`],
/// [`CommandContext::record_lookup`], or direct `ctx.effects.…` assignment)
/// and the execution seam drains it in one move —
/// `std::mem::take(&mut ctx.effects)` — then consumes it exhaustively, so
/// adding an effect field is a compile error at every drain site instead of a
/// silent drop.
///
/// The no-op suppression rule ("a write that declared itself a no-op discards
/// its whole effect payload") has a single home here: `into_write_meta` in
/// `shard::execution` (which `into_script_record` chains through).
#[derive(Default)]
pub struct CommandEffects {
    /// Number of dirty changes made by this command (for rdb_changes_since_last_save).
    ///
    /// Defaults to 0. Write commands that modify data should set this to indicate
    /// how many logical changes were made. The shard uses this to track
    /// `rdb_changes_since_last_save` in INFO persistence.
    ///
    /// Commands like SETBIT and BITFIELD SET should only set this when the value
    /// actually changed.
    pub dirty_delta: i64,

    /// Number of objects freed via lazyfree operations (UNLINK, FLUSHALL ASYNC).
    ///
    /// Defaults to 0. The UNLINK command sets this to the number of keys deleted.
    /// The shard adds this to its `lazyfreed_objects` counter after command execution.
    pub lazyfreed_delta: u64,

    /// Number of keyspace read lookups that hit an existing key.
    ///
    /// Only populated by [`LookupSpec::Reported`] commands, which call
    /// [`CommandContext::record_lookup`]. `FirstKey` / `EveryKey` commands are
    /// counted by the execution seam from actual key existence and never touch
    /// these fields. Mirrors Redis's `lookupKeyReadWithFlags`, which increments
    /// `keyspace_hits`/`keyspace_misses` based on whether the looked-up KEY
    /// exists in the keyspace dictionary — not on the shape of the reply.
    ///
    /// [`LookupSpec::Reported`]: crate::command_spec::LookupSpec::Reported
    pub keyspace_hits: u64,

    /// Number of keyspace read lookups that missed (the key did not exist).
    ///
    /// See [`CommandEffects::keyspace_hits`].
    pub keyspace_misses: u64,

    /// A WRITE command sets this `true` to declare it verified it made NO
    /// change (e.g. PFADD where no register moved), so the execution seam
    /// skips the entire write-effect pipeline: WAL persist, replication
    /// broadcast, version bump (WATCH), keyspace notification, and tracking
    /// invalidation. Matches Redis, which does not propagate a no-op write.
    ///
    /// Distinct from `dirty_delta = -1`, which only suppresses the version
    /// bump and dirty counter while the write still persists and replicates.
    pub write_was_noop: bool,

    /// HyperLogLog register delta deposited by a write for the
    /// [`WalStrategy::MergeDeltaOrPersistFirstKey`] strategy.
    ///
    /// PFADD sets this to the `(register_index, new_value)` pairs it raised
    /// **only when the post-add value is dense** — so the WAL persists a compact
    /// `Merge` operand instead of the full ~12 KiB dense value. Left `None` for
    /// sparse or newly-created values (which keep a full `Put`: sparse serializes
    /// small already, and creation must establish the base on disk). Reset per
    /// command (it lives on the per-command context).
    pub hll_wal_delta: Option<SmallVec<[(u16, u8); 8]>>,

    /// Keyspace events deposited by an
    /// [`EventSpec::Dynamic`](crate::command_spec::EventSpec::Dynamic) command.
    ///
    /// Handlers must go through [`CommandContext::notify_event`]; the execution
    /// seam drains the deposits and routes them to the notifications write
    /// effect. A `write_was_noop` command discards its deposits wholesale (the
    /// effect pipeline is skipped), same contract as `hll_wal_delta`.
    pub keyspace_events: KeyspaceEventDeposits,

    /// Effective writes performed by script sub-commands during an
    /// EVAL/EVALSHA/FCALL running on this context.
    ///
    /// Populated by the scripting seam (`ScriptInvoker::run_local`) for each
    /// `redis.call`/`redis.pcall` write that actually changed data; drained by
    /// the shard worker after the script completes and routed through the same
    /// `WRITE_EFFECT_ORDER` pipeline as direct commands (notifications, WATCH
    /// bump, tracking invalidation, waiter wake, WAL, replication). Always
    /// empty for ordinary (non-script) command executions.
    pub script_writes: Vec<ScriptWriteRecord>,
}

impl CommandEffects {
    /// Convert the drained effects of a script sub-command into the owned
    /// [`ScriptWriteRecord`] the shard worker replays after the script.
    ///
    /// Returns `None` for a `write_was_noop` sub-command — the same no-op
    /// suppression rule as the direct path, because this chains through
    /// `into_write_meta` (the rule's single home, in `shard::execution`).
    /// The caller still owns the "was it a successful WRITE command?" check;
    /// this method owns only what the *effects* say.
    pub(crate) fn into_script_record(
        self,
        handler: Arc<dyn Command>,
        args: Vec<Bytes>,
    ) -> Option<ScriptWriteRecord> {
        self.into_write_meta(handler).map(|meta| ScriptWriteRecord {
            handler: meta.handler,
            args,
            dirty_delta: meta.dirty_delta,
            hll_wal_delta: meta.hll_wal_delta,
            keyspace_events: meta.keyspace_events,
        })
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
/// # Design Rationale
///
/// This is intentionally a flat struct rather than a hierarchy of nested contexts.
/// 90%+ of commands only use `store` + `protocol_version`. Cluster fields
/// (`cluster_state`, `node_id`, `raft`, `network_factory`, `quorum_checker`) are
/// only used by ~8 cluster commands in the `cluster` crate, and replication fields
/// (`replication_tracker`) by ~3 replication commands. These usage patterns are
/// naturally isolated to their respective command crates, so a flat struct with
/// direct field access is simpler than nested sub-contexts. Helper methods like
/// [`cluster_context()`](Self::cluster_context) exist for bundle access but
/// direct field access is equally valid.
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
    pub shard_senders: &'a Arc<Vec<ShardSender>>,

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

    /// Optional command registry for introspection commands (COMMAND GETKEYS).
    pub command_registry: Option<&'a Arc<CommandRegistry>>,

    /// Whether this server is running as a replica.
    ///
    /// Used by ROLE and INFO replication to report the correct role.
    pub is_replica: bool,

    /// Shared flag for the server's replica status.
    ///
    /// Commands like REPLICAOF NO ONE can set this to update the server-wide
    /// replica status atomically. This is the same `Arc<AtomicBool>` shared
    /// by shard workers, the acceptor, and all connection handlers.
    pub is_replica_flag: Option<Arc<AtomicBool>>,

    /// Primary host (set when running as a replica, for INFO replication).
    pub master_host: Option<String>,

    /// Primary port (set when running as a replica, for INFO replication).
    pub master_port: Option<u16>,

    /// Everything this execution *produces* besides the [`Response`] — the
    /// command's out-buffer, drained as one value by the execution seam via
    /// `std::mem::take(&mut ctx.effects)`. See [`CommandEffects`].
    pub effects: CommandEffects,
}

impl<'a> CommandContext<'a> {
    /// Create a new command context.
    pub fn new(
        store: &'a mut dyn Store,
        shard_senders: &'a Arc<Vec<ShardSender>>,
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
            cluster_state: None,
            node_id: None,
            raft: None,
            network_factory: None,
            quorum_checker: None,
            command_registry: None,
            is_replica: false,
            is_replica_flag: None,
            master_host: None,
            master_port: None,
            effects: CommandEffects::default(),
        }
    }

    /// Deposit a keyspace event for this command's write.
    ///
    /// Only meaningful on commands declaring
    /// [`EventSpec::Dynamic`](crate::command_spec::EventSpec::Dynamic) — the
    /// notifications write effect emits exactly the deposited events, in
    /// deposit order, instead of iterating the extracted key list. Call it
    /// once per (key, event) the command actually wrote; skip it entirely on
    /// no-op paths so nothing is emitted.
    #[inline]
    pub fn notify_event(&mut self, key: Bytes, name: &'static str, class: KeyspaceEventFlags) {
        self.effects.keyspace_events.push((key, name, class));
    }

    /// Drain the deposited keyspace events.
    ///
    /// Leaves the buffer empty; the caller owns routing the events into the
    /// write-effect pipeline (or dropping them for a `write_was_noop` command).
    /// The execution seam drains the whole [`CommandEffects`] value instead;
    /// this remains for callers that only need the event deposits.
    #[inline]
    pub fn take_keyspace_events(&mut self) -> KeyspaceEventDeposits {
        std::mem::take(&mut self.effects.keyspace_events)
    }

    /// Report a single keyspace lookup from a [`LookupSpec::Reported`] command.
    ///
    /// This is the *only* handler-facing keyspace-accounting entry point, and it
    /// exists solely for irregular reads whose counted lookup is neither the
    /// command's first key nor every key. Commands classified `FirstKey` /
    /// `EveryKey` are counted by the execution seam from actual key existence
    /// and must not call this.
    ///
    /// [`LookupOutcome::Hit`] means the looked-up KEY existed — independent of
    /// the reply shape (HGET on a missing field is a hit; GET on a missing key
    /// is a miss) — matching Redis's `lookupKeyReadWithFlags`.
    #[inline]
    pub fn record_lookup(&mut self, outcome: crate::command_spec::LookupOutcome) {
        match outcome {
            crate::command_spec::LookupOutcome::Hit => self.effects.keyspace_hits += 1,
            crate::command_spec::LookupOutcome::Miss => self.effects.keyspace_misses += 1,
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

        Some(ReplicationContextRef { tracker })
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
    pub fn get_or_create<T: DefaultValueType>(
        &mut self,
        key: &Bytes,
    ) -> Result<&mut T, CommandError> {
        Ok(self.store.get_or_create_typed(key)?)
    }
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

    fn args(values: &[&[u8]]) -> Vec<Bytes> {
        values.iter().map(|v| Bytes::copy_from_slice(v)).collect()
    }

    #[test]
    fn wal_strategy_default_is_noop() {
        // Read commands rely on this default.
        assert_eq!(WalStrategy::default(), WalStrategy::NoOp);
    }

    #[test]
    fn wal_strategy_persist_first_key() {
        let args = args(&[b"foo", b"value"]);
        let actions = WalStrategy::PersistFirstKey.actions(&args);
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], WalAction::Persist(k) if k == b"foo"));

        // No args (defensive — every PersistFirstKey command has at least one).
        let empty: Vec<Bytes> = Vec::new();
        assert!(WalStrategy::PersistFirstKey.actions(&empty).is_empty());
    }

    #[test]
    fn wal_strategy_clear_shard() {
        // Unconditional: one keyless ClearShard action regardless of args.
        let with_args = args(&[b"ignored"]);
        let actions = WalStrategy::ClearShard.actions(&with_args);
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], WalAction::ClearShard));
        assert_eq!(actions[0].key(), b"");

        // Also emitted with no args (FLUSHDB/FLUSHALL take none).
        let empty: Vec<Bytes> = Vec::new();
        let actions = WalStrategy::ClearShard.actions(&empty);
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], WalAction::ClearShard));
    }

    #[test]
    fn wal_strategy_delete_keys() {
        let args = args(&[b"a", b"b", b"c"]);
        let actions = WalStrategy::DeleteKeys.actions(&args);
        assert_eq!(actions.len(), 3);
        assert!(matches!(actions[0], WalAction::DeleteIfMissing(k) if k == b"a"));
        assert!(matches!(actions[1], WalAction::DeleteIfMissing(k) if k == b"b"));
        assert!(matches!(actions[2], WalAction::DeleteIfMissing(k) if k == b"c"));

        // No args.
        let empty: Vec<Bytes> = Vec::new();
        assert!(WalStrategy::DeleteKeys.actions(&empty).is_empty());
    }

    #[test]
    fn wal_strategy_persist_or_delete_first_key() {
        let args = args(&[b"foo"]);
        let actions = WalStrategy::PersistOrDeleteFirstKey.actions(&args);
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], WalAction::PersistOrDelete(k) if k == b"foo"));

        let empty: Vec<Bytes> = Vec::new();
        assert!(
            WalStrategy::PersistOrDeleteFirstKey
                .actions(&empty)
                .is_empty()
        );
    }

    #[test]
    fn wal_strategy_rename_keys() {
        let args = args(&[b"old", b"new"]);
        let actions = WalStrategy::RenameKeys.actions(&args);
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], WalAction::DeleteIfMissing(k) if k == b"old"));
        assert!(matches!(actions[1], WalAction::Persist(k) if k == b"new"));

        // Insufficient args — yields nothing rather than panicking.
        let one = args[..1].to_vec();
        assert!(WalStrategy::RenameKeys.actions(&one).is_empty());
    }

    #[test]
    fn wal_strategy_move_keys() {
        let args = args(&[b"src", b"dst"]);
        let actions = WalStrategy::MoveKeys.actions(&args);
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], WalAction::PersistOrDelete(k) if k == b"src"));
        assert!(matches!(actions[1], WalAction::Persist(k) if k == b"dst"));

        let one = args[..1].to_vec();
        assert!(WalStrategy::MoveKeys.actions(&one).is_empty());
    }

    #[test]
    fn wal_strategy_persist_destination() {
        // Index 0 — SINTERSTORE-style.
        let args = args(&[b"dest", b"src1", b"src2"]);
        let actions = WalStrategy::PersistDestination(0).actions(&args);
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], WalAction::PersistIfExists(k) if k == b"dest"));

        // Index 1 — COPY-style.
        let actions = WalStrategy::PersistDestination(1).actions(&args);
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], WalAction::PersistIfExists(k) if k == b"src1"));

        // Out-of-bounds index — yields nothing.
        assert!(
            WalStrategy::PersistDestination(99)
                .actions(&args)
                .is_empty()
        );
    }

    #[test]
    fn wal_strategy_persist_or_delete_destination() {
        // BITOP-style: destination at index 1, persist-or-delete so a
        // delete-on-empty reaches the WAL.
        let args = args(&[b"AND", b"dest", b"src1", b"src2"]);
        let actions = WalStrategy::PersistOrDeleteDestination(1).actions(&args);
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], WalAction::PersistOrDelete(k) if k == b"dest"));

        // Out-of-bounds index — yields nothing.
        assert!(
            WalStrategy::PersistOrDeleteDestination(99)
                .actions(&args)
                .is_empty()
        );
    }

    #[test]
    fn wal_strategy_noop() {
        let args = args(&[b"foo", b"bar"]);
        assert!(WalStrategy::NoOp.actions(&args).is_empty());
        let empty: Vec<Bytes> = Vec::new();
        assert!(WalStrategy::NoOp.actions(&empty).is_empty());
    }

    #[test]
    fn wal_action_key_returns_target() {
        let key: &[u8] = b"k";
        assert_eq!(WalAction::Persist(key).key(), key);
        assert_eq!(WalAction::DeleteIfMissing(key).key(), key);
        assert_eq!(WalAction::PersistOrDelete(key).key(), key);
        assert_eq!(WalAction::PersistIfExists(key).key(), key);
        assert_eq!(
            WalAction::MergeHllDelta {
                key,
                pairs: &[(1, 5)]
            }
            .key(),
            key
        );
    }

    #[test]
    fn merge_delta_strategy_routes_delta_vs_full() {
        let args = args(&[b"hll"]);
        let pairs: [(u16, u8); 2] = [(1, 5), (42, 3)];

        // With a delta present, the strategy emits a single Merge operand
        // carrying exactly the raised registers.
        let with_delta =
            WalStrategy::MergeDeltaOrPersistFirstKey.actions_with_delta(&args, Some(&pairs));
        assert_eq!(with_delta.len(), 1);
        assert_eq!(
            with_delta[0],
            WalAction::MergeHllDelta {
                key: b"hll",
                pairs: &pairs
            }
        );

        // With no delta (sparse / newly-created value), it falls back to a full
        // Persist of the first key.
        let without_delta =
            WalStrategy::MergeDeltaOrPersistFirstKey.actions_with_delta(&args, None);
        assert_eq!(without_delta.len(), 1);
        assert_eq!(without_delta[0], WalAction::Persist(b"hll"));

        // The plain `actions` shorthand is the no-delta case.
        assert_eq!(
            WalStrategy::MergeDeltaOrPersistFirstKey.actions(&args)[0],
            WalAction::Persist(b"hll")
        );

        // No key → no actions, regardless of a (nonsensical) delta.
        let empty: Vec<Bytes> = Vec::new();
        assert!(
            WalStrategy::MergeDeltaOrPersistFirstKey
                .actions_with_delta(&empty, Some(&pairs))
                .is_empty()
        );
    }

    fn deposit_test_context() -> CommandContext<'static> {
        let store = Box::leak(Box::new(crate::store::HashMapStore::new()));
        let shard_senders = Box::leak(Box::new(Arc::new(Vec::new())));
        CommandContext::new(store, shard_senders, 0, 1, 0, ProtocolVersion::Resp2)
    }

    /// `notify_event` deposits accumulate in call order and `take` drains them,
    /// leaving the buffer empty for reuse (the seam relies on both).
    #[test]
    fn keyspace_event_deposits_drain_in_order() {
        let mut ctx = deposit_test_context();
        assert!(ctx.take_keyspace_events().is_empty());

        ctx.notify_event(
            Bytes::from_static(b"src"),
            "rename_from",
            crate::keyspace_event::KeyspaceEventFlags::GENERIC,
        );
        ctx.notify_event(
            Bytes::from_static(b"dst"),
            "rename_from",
            crate::keyspace_event::KeyspaceEventFlags::GENERIC,
        );

        let events = ctx.take_keyspace_events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].0, Bytes::from_static(b"src"));
        assert_eq!(events[1].0, Bytes::from_static(b"dst"));
        assert!(events.iter().all(|(_, name, class)| {
            *name == "rename_from" && *class == crate::keyspace_event::KeyspaceEventFlags::GENERIC
        }));

        // Drained: a second take yields nothing.
        assert!(ctx.take_keyspace_events().is_empty());
    }
}
