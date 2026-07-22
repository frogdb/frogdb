//! Declarative command specification.
//!
//! A [`CommandSpec`] is the single, `const`-constructible description of a
//! command's mechanical facts: how its keys are located, which keyspace event
//! it emits, how it persists to the WAL, which blocking waiters it wakes, and
//! its per-key access flags. The dispatcher derives behavior from this data
//! instead of consulting a handful of opt-in trait methods per command.
//!
//! See `todo/proposals/01-declarative-command-spec.md`.

use bytes::Bytes;
use smallvec::{SmallVec, smallvec};

use crate::command::{
    Arity, CommandFlags, ConnMutation, ConnectionLevelOp, ExecutionStrategy, KeyAccessFlag,
    WaiterWake, WalStrategy,
};
use crate::keyspace_event::KeyspaceEventFlags;

/// How a command's keys are located in its argument list.
///
/// Covers every *static* key pattern in the codebase; [`KeySpec::Dynamic`] is
/// the escape hatch for commands whose keys depend on argument values
/// (SORT…STORE, GEORADIUS…STORE, XREAD STREAMS, EVAL numkeys).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeySpec {
    /// No keys (PING, INFO).
    None,
    /// `args[0]` is the only key (GET, SET, ZADD — the dominant pattern).
    First,
    /// `args[0]` and `args[1]` (RENAME, COPY, SMOVE, LMOVE, RPOPLPUSH).
    FirstTwo,
    /// Every argument is a key (DEL, MGET, EXISTS).
    All,
    /// Every argument except the last is a key (BLPOP/BRPOP/BZPOPMIN: the
    /// trailing argument is a timeout, not a key).
    AllButLast,
    /// Every `step`-th argument starting at 0 (MSET/MSETNX: `key value …`).
    Stride { step: usize },
    /// `args[n..]` are all keys (PFMERGE-style: skip leading non-key args).
    Skip(usize),
    /// A single key at a fixed index (`args[idx]`).
    Index(usize),
    /// `args[numkeys]` holds a count N; N keys start at `first`
    /// (EVAL, ZADD-style numkeys layouts, SINTERCARD, LMPOP, ZMPOP).
    NumkeysAt { numkeys: usize, first: usize },
    /// `args[0]` is a destination key, then `args[numkeys]` holds a count N and
    /// N source keys start at `first` (ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE:
    /// `dest numkeys key …`).
    DestThenNumkeys { numkeys: usize, first: usize },
    /// Keys depend on argument values; the command implements
    /// [`Command::dynamic_keys`](crate::command::Command::dynamic_keys). The
    /// registry asserts this variant appears iff `CommandFlags::MOVABLEKEYS`
    /// is set.
    Dynamic,
}

impl KeySpec {
    /// The single derivation point — replaces the hand-rolled `keys()` bodies.
    ///
    /// For [`KeySpec::Dynamic`] this returns an empty slice: dynamic extraction
    /// is the command's responsibility via `dynamic_keys`, and the dispatcher
    /// routes to it before reaching here.
    pub fn extract<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        match *self {
            KeySpec::None | KeySpec::Dynamic => Vec::new(),
            KeySpec::First => match args.first() {
                Some(k) => vec![k.as_ref()],
                None => Vec::new(),
            },
            KeySpec::FirstTwo => {
                if args.len() >= 2 {
                    vec![args[0].as_ref(), args[1].as_ref()]
                } else {
                    Vec::new()
                }
            }
            KeySpec::All => args.iter().map(|a| a.as_ref()).collect(),
            KeySpec::AllButLast => {
                if args.len() < 2 {
                    Vec::new()
                } else {
                    args[..args.len() - 1].iter().map(|a| a.as_ref()).collect()
                }
            }
            KeySpec::Stride { step } => {
                let step = step.max(1);
                args.iter().step_by(step).map(|a| a.as_ref()).collect()
            }
            KeySpec::Skip(n) => {
                if args.len() <= n {
                    Vec::new()
                } else {
                    args[n..].iter().map(|a| a.as_ref()).collect()
                }
            }
            KeySpec::Index(idx) => match args.get(idx) {
                Some(k) => vec![k.as_ref()],
                None => Vec::new(),
            },
            KeySpec::NumkeysAt { numkeys, first } => {
                let Some(count) = args
                    .get(numkeys)
                    .and_then(|a| std::str::from_utf8(a).ok())
                    .and_then(|s| s.parse::<usize>().ok())
                else {
                    return Vec::new();
                };
                if first > args.len() {
                    return Vec::new();
                }
                args[first..]
                    .iter()
                    .take(count)
                    .map(|a| a.as_ref())
                    .collect()
            }
            KeySpec::DestThenNumkeys { numkeys, first } => {
                let Some(dest) = args.first() else {
                    return Vec::new();
                };
                let mut keys = vec![dest.as_ref()];
                if let Some(count) = args
                    .get(numkeys)
                    .and_then(|a| std::str::from_utf8(a).ok())
                    .and_then(|s| s.parse::<usize>().ok())
                    && first <= args.len()
                {
                    keys.extend(args[first..].iter().take(count).map(|a| a.as_ref()));
                }
                keys
            }
        }
    }

    /// The highest fixed argument index this spec reads, if statically known.
    ///
    /// Used by [`CommandSpec::validate`] to check that the declared arity
    /// minimum is large enough to actually contain the keys.
    fn min_required_args(&self) -> Option<usize> {
        match *self {
            KeySpec::First => Some(1),
            KeySpec::FirstTwo => Some(2),
            KeySpec::Index(idx) => Some(idx + 1),
            KeySpec::NumkeysAt { numkeys, .. } => Some(numkeys + 1),
            KeySpec::DestThenNumkeys { numkeys, .. } => Some(numkeys + 1),
            // Variable / unbounded patterns — nothing to assert.
            KeySpec::None
            | KeySpec::All
            | KeySpec::AllButLast
            | KeySpec::Stride { .. }
            | KeySpec::Skip(_)
            | KeySpec::Dynamic => Option::None,
        }
    }

    /// The number of keys this spec is *guaranteed* to extract given `min_arity`,
    /// or `None` when the count is genuinely value-dependent (`Stride`,
    /// `NumkeysAt`, `Dynamic`).
    ///
    /// Used by [`CommandSpec::validate`] to statically bound an
    /// [`EventSpec::EmitsAt`] `key_index`: `key_index` must be `< min_key_count`,
    /// so the seam can never index past the extracted key list for any valid
    /// input. Conservative — it returns the guaranteed *lower bound*, so a valid
    /// index is never rejected.
    fn min_key_count(&self, min_arity: usize) -> Option<usize> {
        match *self {
            KeySpec::None => Some(0),
            KeySpec::First | KeySpec::Index(_) => Some(1),
            KeySpec::FirstTwo => Some(2),
            KeySpec::All => Some(min_arity),
            KeySpec::AllButLast => Some(min_arity.saturating_sub(1)),
            KeySpec::Skip(n) => Some(min_arity.saturating_sub(n)),
            // `DestThenNumkeys` always yields the destination key at index 0.
            KeySpec::DestThenNumkeys { .. } => Some(1),
            // Value-dependent counts — nothing statically guaranteed.
            KeySpec::Stride { .. } | KeySpec::NumkeysAt { .. } | KeySpec::Dynamic => Option::None,
        }
    }
}

/// What keyspace notification a write command emits.
///
/// Deliberately not an `Option`: "forgot to declare" is unrepresentable. A
/// write command that emits nothing must say [`EventSpec::Suppressed`] and
/// justify it at the declaration site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSpec {
    /// Emits `class` events named `name` for each extracted key
    /// (e.g. `LIST` / `"lpush"`). Only legitimate on single-key writes and the
    /// genuinely per-key multi-key writes (DEL, UNLINK, MSET, MSETNX);
    /// [`CommandSpec::validate`] rejects any other multi-key `Emits`.
    Emits {
        class: KeyspaceEventFlags,
        name: &'static str,
    },
    /// Emits `class`/`name` on exactly one extracted key: `keys()[key_index]`.
    /// For STORE-family commands whose remaining keys are read-only sources
    /// (ZRANGESTORE, S*STORE, Z*STORE emit on the destination only). The index
    /// is into the *extracted key list*, not raw args, so it works uniformly
    /// across `FirstTwo`, `All`, `DestThenNumkeys`, etc. An out-of-range index
    /// is a `debug_assert` at emission time and emits nothing in release.
    EmitsAt {
        class: KeyspaceEventFlags,
        name: &'static str,
        key_index: usize,
    },
    /// The handler deposits its events at runtime via
    /// [`CommandContext::notify_event`](crate::command::CommandContext::notify_event);
    /// the notifications seam drains them. For commands whose emitted key/name
    /// depends on execution (renames, moves, and the actually-popped key of a
    /// multi-key pop). Deposits from a `write_was_noop` path are naturally
    /// discarded — the whole effect pipeline is skipped.
    Dynamic,
    /// Write command that deliberately emits nothing (e.g. FLUSHDB).
    Suppressed,
    /// Read command — notifications do not apply.
    NotApplicable,
}

/// Per-key access flags for `COMMAND GETKEYSANDFLAGS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessSpec {
    /// Derive one flag from `CommandFlags`: `WRITE` → `OW`, otherwise `R`.
    Uniform,
    /// Every key is read-and-write (`RW`), independent of `CommandFlags`.
    ///
    /// For read-modify-write commands that both *read* the stored value and
    /// mutate it (INCR/DECR, the pop/GETDEL/GETSET/GETEX families, HINCRBY,
    /// ZINCRBY, MIGRATE, …). Redis marks these key-specs `RW,ACCESS,UPDATE|
    /// DELETE`, so ACL requires read **and** write. The flag-derived
    /// [`AccessSpec::Uniform`] would collapse them to write-only `OW` and let a
    /// `%W~`-only principal execute them and read back the stored value — an
    /// ACL bypass once per-key enforcement is active. `RW` also keeps the key
    /// in the WAL write-set (it passes the `W`/`OW`/`RW` filter), so
    /// flag-derived WAL strategies still persist it.
    UniformRW,
    /// Explicit flag per key position; the last entry repeats for any
    /// trailing keys.
    Positional(&'static [KeyAccessFlag]),
    /// Defer to
    /// [`Command::dynamic_keys_with_flags`](crate::command::Command::dynamic_keys_with_flags)
    /// (SORT, GEORADIUS).
    Dynamic,
}

impl AccessSpec {
    /// Resolve per-key access flags for a concrete key list.
    ///
    /// `write` is whether the command carries `CommandFlags::WRITE`, used by
    /// [`AccessSpec::Uniform`]. [`AccessSpec::Dynamic`] is resolved by the
    /// command itself and falls back to `Uniform` here.
    pub fn resolve<'a>(
        &self,
        keys: Vec<&'a [u8]>,
        write: bool,
    ) -> Vec<(&'a [u8], Vec<KeyAccessFlag>)> {
        match self {
            AccessSpec::Uniform | AccessSpec::Dynamic => {
                let flag = if write {
                    KeyAccessFlag::OW
                } else {
                    KeyAccessFlag::R
                };
                keys.into_iter().map(|k| (k, vec![flag])).collect()
            }
            AccessSpec::UniformRW => keys
                .into_iter()
                .map(|k| (k, vec![KeyAccessFlag::RW]))
                .collect(),
            AccessSpec::Positional(flags) => keys
                .into_iter()
                .enumerate()
                .map(|(i, k)| {
                    let flag = flags
                        .get(i)
                        .or_else(|| flags.last())
                        .copied()
                        .unwrap_or(KeyAccessFlag::R);
                    (k, vec![flag])
                })
                .collect(),
        }
    }
}

/// How a command contributes to keyspace hit/miss accounting.
///
/// Declared once on the [`CommandSpec`]; the execution seam — never the handler
/// body — does the counting. "Hit" always means the looked-up KEY existed,
/// independent of the reply shape: HGET on an existing hash with a missing field
/// is a hit, GET on a missing key is a miss. This mirrors Redis's lookup-level
/// `keyspace_hits`/`keyspace_misses` accounting (`lookupKeyReadWithFlags`),
/// which a reply-shape heuristic cannot reproduce (a nil bulk reply is ambiguous
/// between the two cases).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupSpec {
    /// Not a keyspace-counted command: writes, admin, pub/sub, and
    /// dictionary-iterating reads (SCAN, RANDOMKEY) that never resolve a
    /// *named* key. Matches Redis, which does not route these through
    /// `lookupKeyRead`.
    None,
    /// Exactly one lookup against the command's primary key; hit iff that key
    /// exists. The seam records it from actual key existence; the handler writes
    /// no accounting code. (GET, HGET, LINDEX, STRLEN, LLEN, TYPE, …)
    FirstKey,
    /// One lookup per key in the command's key set; per-key hit/miss.
    /// (MGET, EXISTS, TOUCH.)
    EveryKey,
    /// Irregular read whose counted lookup is neither "first key" nor "every
    /// key"; the handler reports it once via
    /// [`CommandContext::record_lookup`](crate::command::CommandContext::record_lookup).
    Reported,
}

/// The outcome of a single keyspace lookup a [`LookupSpec::Reported`] handler
/// reports to the seam. "Hit" means the looked-up KEY existed, independent of
/// the reply shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupOutcome {
    /// The looked-up key existed.
    Hit,
    /// The looked-up key was absent (or logically expired).
    Miss,
}

/// The index-source type a reindex action projects a key into.
///
/// A hash-source index reads the key as a hash; a JSON-source index reads it as
/// a JSON document. The split is load-bearing: JSON-source indexes filter on
/// [`frogdb_search::IndexSource::Json`], while the hash path matches by prefix
/// alone (see `search_hook.rs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    /// Read the key as a hash and reindex into matching hash-source indexes.
    Hash,
    /// Read the key as a JSON document and reindex into matching JSON-source
    /// indexes.
    Json,
}

/// How a write command's effect on the search index is derived — the search
/// analogue of [`WalStrategy`].
///
/// Declared once per command, next to `wal`/`event`/`wakes`; the `SearchIndex`
/// write effect resolves it to [`ReindexAction`]s instead of matching the
/// command's name string. The spec states *what* to reindex; the `ShardWorker`
/// owns *how* (reading the stored value and updating the index bodies).
///
/// [`ReindexSpec::None`] is the correct default for reads and for the vast
/// majority of writes (every string/list/set/zset/stream mutation, plus admin
/// and per-field-TTL commands that change no indexable value such as
/// `HPERSIST`), so — unlike `WalStrategy`/`EventSpec` — its default cannot serve
/// as a completeness tripwire. See the registry conformance test in
/// `server/src/server/register.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReindexSpec {
    /// Read command, or a write that never changes indexable content
    /// (string/list/set/zset/stream mutations, admin, `HPERSIST`). Default.
    #[default]
    None,
    /// Reindex `args[0]` as `kind`; the key still exists after the write
    /// (HSET/HMSET/HINCRBY/HSETEX…; JSON.SET/JSON.MERGE/JSON.NUMINCRBY…).
    /// Mirrors [`WalStrategy::PersistFirstKey`].
    FirstKey { kind: IndexKind },
    /// Reindex `args[0]` as `kind` if it still exists, else drop it from the
    /// indexes (HDEL/HGETDEL/H(P)EXPIRE(AT)/HGETEX; JSON.DEL/JSON.CLEAR) —
    /// mirrors [`WalStrategy::PersistOrDeleteFirstKey`].
    FirstKeyOrDelete { kind: IndexKind },
    /// Drop every arg key from all matching indexes (DEL/UNLINK) — mirrors
    /// [`WalStrategy::DeleteKeys`].
    DeleteKeys,
    /// Drop `args[0]`, reindex `args[1]` as a hash (RENAME/RENAMENX) — mirrors
    /// [`WalStrategy::RenameKeys`]. Requires [`KeySpec::FirstTwo`].
    Rename,
}

/// A typed reindex action against a single key.
///
/// [`ReindexSpec::actions`] resolves a spec + args to a sequence of these; the
/// `ShardWorker` applies each. The search analogue of
/// [`WalAction`](crate::command::WalAction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReindexAction<'a> {
    /// Reindex the key as `kind` (its value is read from the store).
    Reindex { key: &'a [u8], kind: IndexKind },
    /// Reindex the key as `kind` if present, else delete it from the indexes.
    ReindexOrDelete { key: &'a [u8], kind: IndexKind },
    /// Delete the key from all matching indexes.
    Delete { key: &'a [u8] },
}

impl ReindexSpec {
    /// Resolve this spec + args to the concrete sequence of per-key reindex
    /// actions — the search analogue of [`WalStrategy::actions`].
    ///
    /// This is the single source of truth mapping a variant to actions. Adding a
    /// variant extends only this match.
    pub fn actions<'a>(&self, args: &'a [Bytes]) -> SmallVec<[ReindexAction<'a>; 2]> {
        match self {
            ReindexSpec::None => SmallVec::new(),
            ReindexSpec::FirstKey { kind } => match args.first() {
                Some(key) => smallvec![ReindexAction::Reindex {
                    key: key.as_ref(),
                    kind: *kind,
                }],
                None => SmallVec::new(),
            },
            ReindexSpec::FirstKeyOrDelete { kind } => match args.first() {
                Some(key) => smallvec![ReindexAction::ReindexOrDelete {
                    key: key.as_ref(),
                    kind: *kind,
                }],
                None => SmallVec::new(),
            },
            ReindexSpec::DeleteKeys => args
                .iter()
                .map(|arg| ReindexAction::Delete { key: arg.as_ref() })
                .collect(),
            ReindexSpec::Rename => {
                if args.len() >= 2 {
                    smallvec![
                        ReindexAction::Delete {
                            key: args[0].as_ref(),
                        },
                        ReindexAction::Reindex {
                            key: args[1].as_ref(),
                            kind: IndexKind::Hash,
                        },
                    ]
                } else {
                    SmallVec::new()
                }
            }
        }
    }
}

/// Declarative description of a command's mechanics. One `static` per command.
#[derive(Debug, Clone)]
pub struct CommandSpec {
    /// Command name (uppercase, e.g. `"GET"`).
    pub name: &'static str,
    /// Expected argument count.
    pub arity: Arity,
    /// Behavior flags.
    pub flags: CommandFlags,
    /// How keys are located in the argument list.
    pub keys: KeySpec,
    /// Per-key access flags for `COMMAND GETKEYSANDFLAGS`.
    pub access: AccessSpec,
    /// How the command's effects are persisted to the WAL.
    pub wal: WalStrategy,
    /// Which blocking waiters the command may satisfy.
    pub wakes: WaiterWake,
    /// Which keyspace notification the command emits.
    pub event: EventSpec,
    /// Whether all keys must hash to the same slot (MSETNX).
    pub requires_same_slot: bool,
    /// How the command contributes to keyspace hit/miss accounting. The
    /// execution seam reads this; the handler body never counts.
    pub lookup: LookupSpec,
    /// How the connection handler routes and executes this command. Folds the
    /// former `Command::execution_strategy()` override into the spec so routing
    /// is declared once, alongside every other mechanical fact.
    pub strategy: ExecutionStrategy,
    /// Which connection-local mutable capabilities this command's `ConnCtx`
    /// needs wired. Declared here so the connection dispatcher selects the
    /// `ConnCtx` builder from this datum, never from the command's string name.
    /// [`ConnMutation::None`] for every non-connection command and every
    /// pure-read connection command; [`validate`](Self::validate) cross-checks
    /// it against [`strategy`](Self::strategy). See [`ConnMutation`].
    pub mutation: ConnMutation,
    /// How the command's writes are projected into search indexes. `None` for
    /// reads and for writes that change no indexable hash/JSON content; declared
    /// next to `wal`/`event` and validated against `flags`/`keys`. See
    /// [`ReindexSpec`].
    pub reindex: ReindexSpec,
}

/// A cross-field inconsistency detected by [`CommandSpec::validate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpecError {
    /// A `WRITE` command left `event` as `NotApplicable` (must be
    /// `Emits` or `Suppressed`).
    WriteWithoutEvent,
    /// `EventSpec::Suppressed` declared on a non-`WRITE` command.
    SuppressedOnReadCommand,
    /// `KeySpec::Dynamic` set without `MOVABLEKEYS`, or vice versa.
    DynamicKeysMovableMismatch,
    /// The arity minimum is too small to contain the keys the `KeySpec` reads.
    ArityTooSmallForKeys { needs: usize, min: usize },
    /// A `ConnectionLevel` command declared a shard-side WAL effect. These
    /// commands are intercepted before shard routing, so they must be
    /// `WalStrategy::NoOp`.
    ConnectionLevelWithWal,
    /// The declared [`ConnMutation`] capability is illegal for the command's
    /// [`ExecutionStrategy`]: a non-`None` mutation on a command that is not
    /// `ConnectionLevel` (the capability selects a `ConnCtx` builder that only
    /// the connection dispatcher wires), or a `PubSub`/`ConnectionLevel(PubSub)`
    /// disagreement (pub/sub is the only family dispatched through the
    /// multi-response `execute_multi` seam).
    ConnMutationStrategyMismatch,
    /// An [`EventSpec::EmitsAt`] `key_index` cannot be statically proven to fall
    /// within the keys the `KeySpec` extracts for the command's minimum arity.
    EmitsAtIndexOutOfRange { key_index: usize, min_keys: usize },
    /// A multi-key `KeySpec` paired with a blanket [`EventSpec::Emits`] on a
    /// command that is not on the genuinely-per-key allowlist (DEL/UNLINK/MSET/
    /// MSETNX). Such a command would emit its event on read-only source keys —
    /// the STORE-family over-emission bug this invariant closes. The fix is
    /// [`EventSpec::EmitsAt`] (fixed destination) or [`EventSpec::Dynamic`]
    /// (runtime-resolved keys).
    MultiKeyBlanketEmits,
    /// A flag-derived WAL strategy ([`WalStrategy::Dynamic`] or
    /// [`WalStrategy::PersistDestination`]) paired with an
    /// [`AccessSpec::Positional`] list that declares no write flag
    /// (`W`/`OW`/`RW`). Such a command derives its WAL destination from its
    /// write-access keys, so a positional access list with only read flags makes
    /// the strategy a silent no-op — nothing is ever persisted.
    WalDestinationWithoutWriteKey,
    /// A non-`WRITE` command declared a non-[`ReindexSpec::None`] reindex fact.
    /// Reindexing is a write side effect; a read cannot mutate an index.
    ReindexOnReadCommand,
    /// [`ReindexSpec::Rename`] reindexes `args[1]`, so it requires
    /// [`KeySpec::FirstTwo`]; any other key shape cannot supply the second key.
    RenameReindexRequiresFirstTwo,
}

impl std::fmt::Display for SpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpecError::WriteWithoutEvent => {
                write!(
                    f,
                    "WRITE command must declare Emits or Suppressed, not NotApplicable"
                )
            }
            SpecError::SuppressedOnReadCommand => {
                write!(f, "EventSpec::Suppressed is only valid on WRITE commands")
            }
            SpecError::DynamicKeysMovableMismatch => {
                write!(
                    f,
                    "KeySpec::Dynamic must coincide with CommandFlags::MOVABLEKEYS"
                )
            }
            SpecError::ArityTooSmallForKeys { needs, min } => {
                write!(
                    f,
                    "arity minimum {min} cannot contain keys requiring at least {needs} args"
                )
            }
            SpecError::ConnectionLevelWithWal => {
                write!(
                    f,
                    "ConnectionLevel command must declare WalStrategy::NoOp (intercepted before shard routing)"
                )
            }
            SpecError::ConnMutationStrategyMismatch => {
                write!(
                    f,
                    "declared ConnMutation is illegal for the command's ExecutionStrategy (non-None mutation requires ConnectionLevel; PubSub mutation ⇔ ConnectionLevel(PubSub))"
                )
            }
            SpecError::EmitsAtIndexOutOfRange {
                key_index,
                min_keys,
            } => {
                write!(
                    f,
                    "EventSpec::EmitsAt key_index {key_index} is out of range: the KeySpec guarantees only {min_keys} key(s)"
                )
            }
            SpecError::MultiKeyBlanketEmits => {
                write!(
                    f,
                    "multi-key KeySpec with blanket EventSpec::Emits would over-emit on read-only keys; use EmitsAt or Dynamic (or add to the per-key allowlist)"
                )
            }
            SpecError::WalDestinationWithoutWriteKey => {
                write!(
                    f,
                    "flag-derived WAL strategy (Dynamic/PersistDestination) with a Positional access list declaring no write flag (W/OW/RW) persists nothing"
                )
            }
            SpecError::ReindexOnReadCommand => {
                write!(
                    f,
                    "a non-WRITE command must declare ReindexSpec::None (reindexing is a write side effect)"
                )
            }
            SpecError::RenameReindexRequiresFirstTwo => {
                write!(
                    f,
                    "ReindexSpec::Rename reindexes args[1] and requires KeySpec::FirstTwo"
                )
            }
        }
    }
}

/// Commands whose multi-key `KeySpec` genuinely writes *every* extracted key, so
/// a blanket [`EventSpec::Emits`] correctly fires on each. Anything else with a
/// multi-key spec must name its destination ([`EventSpec::EmitsAt`]) or resolve
/// keys at runtime ([`EventSpec::Dynamic`]); see [`SpecError::MultiKeyBlanketEmits`].
const MULTI_KEY_EMITS_ALLOWLIST: &[&str] = &["DEL", "UNLINK", "MSET", "MSETNX"];

impl CommandSpec {
    /// Whether this command carries `CommandFlags::WRITE`.
    pub fn is_write(&self) -> bool {
        self.flags.contains(CommandFlags::WRITE)
    }

    /// Check cross-field invariants. Run at registry build (debug) and by the
    /// exhaustiveness test suite.
    ///
    /// Invariants:
    /// - `WRITE`        ⇒ `event != NotApplicable`
    /// - `Suppressed`   ⇒ `WRITE`
    /// - `Dynamic` keys ⇔ `MOVABLEKEYS`
    /// - arity minimum covers the highest fixed arg index the `KeySpec` reads
    pub fn validate(&self) -> Result<(), SpecError> {
        let write = self.is_write();

        if write && matches!(self.event, EventSpec::NotApplicable) {
            return Err(SpecError::WriteWithoutEvent);
        }
        if !write && matches!(self.event, EventSpec::Suppressed) {
            return Err(SpecError::SuppressedOnReadCommand);
        }

        let dynamic_keys = matches!(self.keys, KeySpec::Dynamic);
        let movable = self.flags.contains(CommandFlags::MOVABLEKEYS);
        if dynamic_keys != movable {
            return Err(SpecError::DynamicKeysMovableMismatch);
        }

        if let Some(needs) = self.keys.min_required_args() {
            let min = self.arity.min();
            if min < needs {
                return Err(SpecError::ArityTooSmallForKeys { needs, min });
            }
        }

        if matches!(self.strategy, ExecutionStrategy::ConnectionLevel(_))
            && !matches!(self.wal, WalStrategy::NoOp)
        {
            return Err(SpecError::ConnectionLevelWithWal);
        }

        // The declared connection-mutation capability selects the `ConnCtx`
        // builder the connection dispatcher wires, so it is only meaningful for a
        // `ConnectionLevel` command. `None` is always legal (every non-connection
        // command and every pure-read connection command). Any other capability
        // requires `ConnectionLevel`, and the `PubSub` capability must coincide
        // with the `ConnectionLevel(PubSub)` routing op — pub/sub is the only
        // family dispatched through the multi-response `execute_multi` seam.
        match (&self.strategy, self.mutation) {
            (_, ConnMutation::None) => {}
            (ExecutionStrategy::ConnectionLevel(op), mutation) => {
                let pubsub_op = matches!(op, ConnectionLevelOp::PubSub);
                let pubsub_mutation = matches!(mutation, ConnMutation::PubSub);
                if pubsub_op != pubsub_mutation {
                    return Err(SpecError::ConnMutationStrategyMismatch);
                }
            }
            (_, _) => return Err(SpecError::ConnMutationStrategyMismatch),
        }

        // A flag-derived WAL strategy derives its destination from the command's
        // write-access keys (`keys_with_flags` filtered to W/OW/RW). With an
        // explicit `Positional` access list that names no write flag, that
        // extraction is always empty, so the strategy silently persists nothing —
        // reject it at declaration time. (`Uniform`/`Dynamic` access derive a
        // write flag from `CommandFlags::WRITE`, so they cannot hit this.)
        if matches!(
            self.wal,
            WalStrategy::Dynamic | WalStrategy::PersistDestination
        ) && let AccessSpec::Positional(flags) = self.access
            && !flags
                .iter()
                .any(|f| matches!(f, KeyAccessFlag::W | KeyAccessFlag::OW | KeyAccessFlag::RW))
        {
            return Err(SpecError::WalDestinationWithoutWriteKey);
        }

        // `EmitsAt.key_index` must be statically provable in-bounds for the
        // spec's minimum arity, so the emission seam can never index past the
        // extracted key list. Value-dependent KeySpecs (`Stride`, `NumkeysAt`,
        // `Dynamic`) guarantee nothing statically and therefore cannot host
        // `EmitsAt` — such commands use `EventSpec::Dynamic` instead.
        if let EventSpec::EmitsAt { key_index, .. } = self.event {
            let min_keys = self.keys.min_key_count(self.arity.min()).unwrap_or(0);
            if key_index >= min_keys {
                return Err(SpecError::EmitsAtIndexOutOfRange {
                    key_index,
                    min_keys,
                });
            }
        }

        // A blanket `Emits` on a multi-key KeySpec fires on every extracted
        // key — only legitimate for commands that genuinely write every key
        // (the explicit allowlist). Everything else must use `EmitsAt` or
        // `Dynamic`, so the next STORE-style command cannot silently
        // reintroduce the source-key over-emission bug.
        if matches!(self.event, EventSpec::Emits { .. })
            && matches!(
                self.keys,
                KeySpec::FirstTwo
                    | KeySpec::All
                    | KeySpec::AllButLast
                    | KeySpec::Stride { .. }
                    | KeySpec::Skip(_)
                    | KeySpec::NumkeysAt { .. }
                    | KeySpec::DestThenNumkeys { .. }
                    // Dynamic key extraction can also yield multiple keys
                    // (e.g. SORT...STORE returns [source, dest]), so a blanket
                    // `Emits` is just as wrong there.
                    | KeySpec::Dynamic
            )
            && !MULTI_KEY_EMITS_ALLOWLIST.contains(&self.name)
        {
            return Err(SpecError::MultiKeyBlanketEmits);
        }

        // Reindexing is a write side effect: a non-`None` reindex fact on a read
        // command is meaningless (the effect pipeline never runs it). This is a
        // structural guard — it trips for any command whose fields contradict,
        // no allowlist involved.
        if !write && !matches!(self.reindex, ReindexSpec::None) {
            return Err(SpecError::ReindexOnReadCommand);
        }

        // `ReindexSpec::Rename` reindexes `args[1]`, so the command must extract
        // a second key. Only `KeySpec::FirstTwo` guarantees it.
        if matches!(self.reindex, ReindexSpec::Rename) && !matches!(self.keys, KeySpec::FirstTwo) {
            return Err(SpecError::RenameReindexRequiresFirstTwo);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&[u8]]) -> Vec<Bytes> {
        values.iter().map(|v| Bytes::copy_from_slice(v)).collect()
    }

    #[test]
    fn extract_none() {
        assert!(KeySpec::None.extract(&args(&[b"a", b"b"])).is_empty());
        assert!(KeySpec::None.extract(&[]).is_empty());
    }

    #[test]
    fn extract_first() {
        assert_eq!(
            KeySpec::First.extract(&args(&[b"k", b"v"])),
            vec![b"k" as &[u8]]
        );
        assert!(KeySpec::First.extract(&[]).is_empty());
    }

    #[test]
    fn extract_first_two() {
        assert_eq!(
            KeySpec::FirstTwo.extract(&args(&[b"a", b"b", b"c"])),
            vec![b"a" as &[u8], b"b"]
        );
        // Fewer than two args yields nothing (matches legacy `two` pattern).
        assert!(KeySpec::FirstTwo.extract(&args(&[b"a"])).is_empty());
        assert!(KeySpec::FirstTwo.extract(&[]).is_empty());
    }

    #[test]
    fn extract_all() {
        assert_eq!(
            KeySpec::All.extract(&args(&[b"a", b"b", b"c"])),
            vec![b"a" as &[u8], b"b", b"c"]
        );
        assert!(KeySpec::All.extract(&[]).is_empty());
    }

    #[test]
    fn extract_stride() {
        // MSET: key value key value -> keys at 0, 2
        assert_eq!(
            KeySpec::Stride { step: 2 }.extract(&args(&[b"k1", b"v1", b"k2", b"v2"])),
            vec![b"k1" as &[u8], b"k2"]
        );
        // step 3: indices 0, 3
        assert_eq!(
            KeySpec::Stride { step: 3 }.extract(&args(&[b"k1", b"a", b"b", b"k2", b"c", b"d"])),
            vec![b"k1" as &[u8], b"k2"]
        );
        // step 0 is treated as 1 (defensive).
        assert_eq!(
            KeySpec::Stride { step: 0 }.extract(&args(&[b"a", b"b"])),
            vec![b"a" as &[u8], b"b"]
        );
    }

    #[test]
    fn extract_skip() {
        assert_eq!(
            KeySpec::Skip(1).extract(&args(&[b"dest", b"s1", b"s2"])),
            vec![b"s1" as &[u8], b"s2"]
        );
        assert!(KeySpec::Skip(1).extract(&args(&[b"only"])).is_empty());
        assert!(KeySpec::Skip(2).extract(&args(&[b"a", b"b"])).is_empty());
    }

    #[test]
    fn extract_index() {
        assert_eq!(
            KeySpec::Index(1).extract(&args(&[b"sub", b"key"])),
            vec![b"key" as &[u8]]
        );
        assert!(KeySpec::Index(1).extract(&args(&[b"sub"])).is_empty());
    }

    #[test]
    fn extract_numkeys_at() {
        // EVAL script numkeys key... -> numkeys at index 1, keys from index 2
        let spec = KeySpec::NumkeysAt {
            numkeys: 1,
            first: 2,
        };
        assert_eq!(
            spec.extract(&args(&[b"script", b"2", b"k1", b"k2", b"arg"])),
            vec![b"k1" as &[u8], b"k2"]
        );
        // numkeys 0 -> no keys
        assert!(spec.extract(&args(&[b"script", b"0", b"arg"])).is_empty());
        // count at index 0 (SINTERCARD numkeys key...)
        let spec0 = KeySpec::NumkeysAt {
            numkeys: 0,
            first: 1,
        };
        assert_eq!(
            spec0.extract(&args(&[b"2", b"a", b"b", b"LIMIT", b"3"])),
            vec![b"a" as &[u8], b"b"]
        );
        // unparseable count -> nothing
        assert!(spec0.extract(&args(&[b"xx", b"a"])).is_empty());
        // count larger than available keys -> take what exists
        assert_eq!(
            spec0.extract(&args(&[b"9", b"a", b"b"])),
            vec![b"a" as &[u8], b"b"]
        );
        // empty args -> nothing
        assert!(spec0.extract(&[]).is_empty());
    }

    #[test]
    fn extract_all_but_last() {
        // BLPOP key1 key2 timeout -> keys = key1, key2
        assert_eq!(
            KeySpec::AllButLast.extract(&args(&[b"k1", b"k2", b"5"])),
            vec![b"k1" as &[u8], b"k2"]
        );
        // Only a timeout, no keys.
        assert!(KeySpec::AllButLast.extract(&args(&[b"5"])).is_empty());
        assert!(KeySpec::AllButLast.extract(&[]).is_empty());
    }

    #[test]
    fn extract_dest_then_numkeys() {
        // ZUNIONSTORE dest 2 k1 k2 WEIGHTS .. -> dest, k1, k2
        let spec = KeySpec::DestThenNumkeys {
            numkeys: 1,
            first: 2,
        };
        assert_eq!(
            spec.extract(&args(&[
                b"dest", b"2", b"k1", b"k2", b"WEIGHTS", b"1", b"2"
            ])),
            vec![b"dest" as &[u8], b"k1", b"k2"]
        );
        // Unparseable count -> just the destination.
        assert_eq!(
            spec.extract(&args(&[b"dest", b"xx", b"k1"])),
            vec![b"dest" as &[u8]]
        );
        // Empty args -> nothing.
        assert!(spec.extract(&[]).is_empty());
    }

    #[test]
    fn extract_dynamic_is_empty() {
        // Dynamic is resolved by the command, not here.
        assert!(KeySpec::Dynamic.extract(&args(&[b"a", b"b"])).is_empty());
    }

    #[test]
    fn access_uniform() {
        let keys = vec![b"a" as &[u8], b"b"];
        let resolved = AccessSpec::Uniform.resolve(keys.clone(), true);
        assert_eq!(
            resolved,
            vec![
                (b"a" as &[u8], vec![KeyAccessFlag::OW]),
                (b"b", vec![KeyAccessFlag::OW])
            ]
        );
        let resolved = AccessSpec::Uniform.resolve(keys, false);
        assert_eq!(
            resolved,
            vec![
                (b"a" as &[u8], vec![KeyAccessFlag::R]),
                (b"b", vec![KeyAccessFlag::R])
            ]
        );
    }

    #[test]
    fn access_uniform_rw() {
        // Every key resolves to RW regardless of the command's WRITE flag —
        // the read-modify-write case (INCR, pops, GETDEL, …).
        let keys = vec![b"a" as &[u8], b"b"];
        for write in [true, false] {
            let resolved = AccessSpec::UniformRW.resolve(keys.clone(), write);
            assert_eq!(
                resolved,
                vec![
                    (b"a" as &[u8], vec![KeyAccessFlag::RW]),
                    (b"b", vec![KeyAccessFlag::RW])
                ],
                "UniformRW must be RW independent of write={write}"
            );
        }
    }

    #[test]
    fn access_positional() {
        let spec = AccessSpec::Positional(&[KeyAccessFlag::RW, KeyAccessFlag::OW]);
        let keys = vec![b"src" as &[u8], b"dst"];
        assert_eq!(
            spec.resolve(keys, true),
            vec![
                (b"src" as &[u8], vec![KeyAccessFlag::RW]),
                (b"dst", vec![KeyAccessFlag::OW])
            ]
        );
        // Trailing keys repeat the last entry.
        let spec = AccessSpec::Positional(&[KeyAccessFlag::R]);
        let keys = vec![b"a" as &[u8], b"b", b"c"];
        let resolved = spec.resolve(keys, false);
        assert!(resolved.iter().all(|(_, f)| f == &vec![KeyAccessFlag::R]));
    }

    fn base_write_spec() -> CommandSpec {
        CommandSpec {
            name: "TESTW",
            arity: Arity::Fixed(1),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::STRING,
                name: "testw",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
            mutation: crate::command::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
            reindex: ReindexSpec::None,
        }
    }

    #[test]
    fn validate_ok() {
        assert_eq!(base_write_spec().validate(), Ok(()));
    }

    #[test]
    fn validate_write_without_event() {
        let mut spec = base_write_spec();
        spec.event = EventSpec::NotApplicable;
        assert_eq!(spec.validate(), Err(SpecError::WriteWithoutEvent));
    }

    /// A non-`None` [`ConnMutation`] on a command that is not `ConnectionLevel`
    /// is a mis-declaration: the capability selects a `ConnCtx` builder that only
    /// the connection dispatcher wires, so it is meaningless on a shard command.
    #[test]
    fn validate_mutation_requires_connection_level() {
        let mut spec = base_write_spec();
        spec.mutation = ConnMutation::Auth;
        // `strategy` is still `Standard` — mismatch.
        assert_eq!(
            spec.validate(),
            Err(SpecError::ConnMutationStrategyMismatch)
        );
    }

    /// The `PubSub` capability and the `ConnectionLevel(PubSub)` routing op must
    /// agree in both directions: `PubSub` mutation on a non-PubSub connection op
    /// errors, and a non-`PubSub` mutation on the PubSub op errors. A read-only
    /// connection command (`None` on any connection op) is always legal.
    #[test]
    fn validate_pubsub_mutation_matches_pubsub_op() {
        let mut spec = base_write_spec();
        // A pub/sub command: ConnectionLevel(PubSub) must carry NoOp WAL.
        spec.flags = CommandFlags::PUBSUB;
        spec.wal = WalStrategy::NoOp;
        spec.event = EventSpec::NotApplicable;

        // PubSub op + PubSub mutation -> ok.
        spec.strategy = ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub);
        spec.mutation = ConnMutation::PubSub;
        assert_eq!(spec.validate(), Ok(()));

        // PubSub op + non-PubSub mutation -> mismatch.
        spec.mutation = ConnMutation::Auth;
        assert_eq!(
            spec.validate(),
            Err(SpecError::ConnMutationStrategyMismatch)
        );

        // Non-PubSub connection op + PubSub mutation -> mismatch.
        spec.strategy = ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin);
        spec.mutation = ConnMutation::PubSub;
        assert_eq!(
            spec.validate(),
            Err(SpecError::ConnMutationStrategyMismatch)
        );

        // Non-PubSub connection op + Auth mutation -> ok (e.g. CLIENT-class Admin
        // carrying conn_state); and `None` is always legal.
        spec.mutation = ConnMutation::Auth;
        assert_eq!(spec.validate(), Ok(()));
        spec.mutation = ConnMutation::None;
        assert_eq!(spec.validate(), Ok(()));
    }

    #[test]
    fn validate_suppressed_on_read() {
        let mut spec = base_write_spec();
        spec.flags = CommandFlags::READONLY;
        spec.event = EventSpec::Suppressed;
        assert_eq!(spec.validate(), Err(SpecError::SuppressedOnReadCommand));
    }

    #[test]
    fn validate_dynamic_requires_movable() {
        let mut spec = base_write_spec();
        spec.keys = KeySpec::Dynamic;
        // Dynamic keys with a blanket `Emits` is rejected on its own (see
        // validate_rejects_multi_key_blanket_emits) — use a Dynamic event so
        // this test isolates the MOVABLEKEYS invariant.
        spec.event = EventSpec::Dynamic;
        // Dynamic without MOVABLEKEYS -> error
        assert_eq!(spec.validate(), Err(SpecError::DynamicKeysMovableMismatch));
        // With MOVABLEKEYS -> ok
        spec.flags = CommandFlags::WRITE | CommandFlags::MOVABLEKEYS;
        spec.arity = Arity::AtLeast(2);
        assert_eq!(spec.validate(), Ok(()));
        // MOVABLEKEYS without Dynamic -> error
        spec.keys = KeySpec::First;
        assert_eq!(spec.validate(), Err(SpecError::DynamicKeysMovableMismatch));
    }

    #[test]
    fn validate_arity_too_small() {
        let mut spec = base_write_spec();
        spec.keys = KeySpec::FirstTwo;
        // Dynamic event: multi-key blanket Emits is rejected on its own
        // (see validate_rejects_multi_key_blanket_emits).
        spec.event = EventSpec::Dynamic;
        spec.arity = Arity::Fixed(1); // needs >= 2
        assert_eq!(
            spec.validate(),
            Err(SpecError::ArityTooSmallForKeys { needs: 2, min: 1 })
        );
        spec.arity = Arity::AtLeast(2);
        assert_eq!(spec.validate(), Ok(()));
    }

    /// Build a write spec with the given key shape, minimum arity, and an
    /// `EmitsAt` event at `key_index`.
    fn emits_at_spec(keys: KeySpec, min_arity: usize, key_index: usize) -> CommandSpec {
        let mut spec = base_write_spec();
        spec.keys = keys;
        spec.arity = Arity::AtLeast(min_arity);
        spec.event = EventSpec::EmitsAt {
            class: KeyspaceEventFlags::STRING,
            name: "testw",
            key_index,
        };
        spec
    }

    #[test]
    fn validate_emits_at_first_two() {
        // FirstTwo guarantees 2 keys: indices 0 and 1 are provable, 2 is not.
        assert_eq!(emits_at_spec(KeySpec::FirstTwo, 2, 0).validate(), Ok(()));
        assert_eq!(emits_at_spec(KeySpec::FirstTwo, 2, 1).validate(), Ok(()));
        assert_eq!(
            emits_at_spec(KeySpec::FirstTwo, 2, 2).validate(),
            Err(SpecError::EmitsAtIndexOutOfRange {
                key_index: 2,
                min_keys: 2
            })
        );
    }

    #[test]
    fn validate_emits_at_all() {
        // All with min arity 2 guarantees 2 keys.
        assert_eq!(emits_at_spec(KeySpec::All, 2, 0).validate(), Ok(()));
        assert_eq!(emits_at_spec(KeySpec::All, 2, 1).validate(), Ok(()));
        assert_eq!(
            emits_at_spec(KeySpec::All, 2, 2).validate(),
            Err(SpecError::EmitsAtIndexOutOfRange {
                key_index: 2,
                min_keys: 2
            })
        );
    }

    #[test]
    fn validate_emits_at_all_but_last() {
        // AllButLast with min arity 3 guarantees 2 keys.
        assert_eq!(emits_at_spec(KeySpec::AllButLast, 3, 1).validate(), Ok(()));
        assert_eq!(
            emits_at_spec(KeySpec::AllButLast, 3, 2).validate(),
            Err(SpecError::EmitsAtIndexOutOfRange {
                key_index: 2,
                min_keys: 2
            })
        );
    }

    #[test]
    fn validate_emits_at_skip() {
        // Skip(1) with min arity 3 guarantees 2 keys.
        assert_eq!(emits_at_spec(KeySpec::Skip(1), 3, 1).validate(), Ok(()));
        assert_eq!(
            emits_at_spec(KeySpec::Skip(1), 3, 2).validate(),
            Err(SpecError::EmitsAtIndexOutOfRange {
                key_index: 2,
                min_keys: 2
            })
        );
    }

    #[test]
    fn validate_emits_at_dest_then_numkeys() {
        // Only the destination (index 0) is statically guaranteed.
        let keys = KeySpec::DestThenNumkeys {
            numkeys: 1,
            first: 2,
        };
        assert_eq!(emits_at_spec(keys, 3, 0).validate(), Ok(()));
        assert_eq!(
            emits_at_spec(keys, 3, 1).validate(),
            Err(SpecError::EmitsAtIndexOutOfRange {
                key_index: 1,
                min_keys: 1
            })
        );
    }

    #[test]
    fn validate_rejects_multi_key_blanket_emits() {
        // A multi-key KeySpec with blanket Emits would fire on read-only
        // source keys (the STORE-family bug); validate() rejects it unless the
        // command is on the genuinely-per-key allowlist.
        for keys in [
            KeySpec::FirstTwo,
            KeySpec::All,
            KeySpec::AllButLast,
            KeySpec::Stride { step: 2 },
            KeySpec::Skip(1),
            KeySpec::NumkeysAt {
                numkeys: 0,
                first: 1,
            },
            KeySpec::DestThenNumkeys {
                numkeys: 1,
                first: 2,
            },
        ] {
            let mut spec = base_write_spec();
            spec.keys = keys;
            spec.arity = Arity::AtLeast(3);
            assert_eq!(
                spec.validate(),
                Err(SpecError::MultiKeyBlanketEmits),
                "{keys:?} with blanket Emits must be rejected"
            );
        }

        // Allowlisted per-key writers keep blanket Emits.
        let mut spec = base_write_spec();
        spec.name = "DEL";
        spec.keys = KeySpec::All;
        spec.arity = Arity::AtLeast(1);
        assert_eq!(spec.validate(), Ok(()));

        // Single-key Emits is unaffected.
        assert_eq!(base_write_spec().validate(), Ok(()));

        // Multi-key EmitsAt/Dynamic/Suppressed are the sanctioned shapes.
        let mut spec = base_write_spec();
        spec.keys = KeySpec::FirstTwo;
        spec.arity = Arity::AtLeast(2);
        spec.event = EventSpec::Dynamic;
        assert_eq!(spec.validate(), Ok(()));
    }

    #[test]
    fn validate_emits_at_rejects_value_dependent_key_specs() {
        // Stride/NumkeysAt/Dynamic guarantee no static key count, so EmitsAt
        // is never provable — those commands must use EventSpec::Dynamic.
        let numkeys = KeySpec::NumkeysAt {
            numkeys: 0,
            first: 1,
        };
        assert_eq!(
            emits_at_spec(numkeys, 3, 0).validate(),
            Err(SpecError::EmitsAtIndexOutOfRange {
                key_index: 0,
                min_keys: 0
            })
        );
        assert_eq!(
            emits_at_spec(KeySpec::Stride { step: 2 }, 2, 0).validate(),
            Err(SpecError::EmitsAtIndexOutOfRange {
                key_index: 0,
                min_keys: 0
            })
        );
    }

    // --- ReindexSpec::actions resolver (mirrors WalStrategy::actions tests) ---

    #[test]
    fn reindex_none_yields_nothing() {
        let a = args(&[b"k", b"v"]);
        assert!(ReindexSpec::None.actions(&a).is_empty());
        assert!(ReindexSpec::None.actions(&[]).is_empty());
    }

    #[test]
    fn reindex_first_key() {
        let a = args(&[b"user:1", b"name", b"Alice"]);
        let actions = ReindexSpec::FirstKey {
            kind: IndexKind::Hash,
        }
        .actions(&a);
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            actions[0],
            ReindexAction::Reindex {
                key,
                kind: IndexKind::Hash
            } if key == b"user:1"
        ));

        // JSON kind is carried through.
        let a = args(&[b"doc:1", b"$", b"{}"]);
        let actions = ReindexSpec::FirstKey {
            kind: IndexKind::Json,
        }
        .actions(&a);
        assert!(matches!(
            actions[0],
            ReindexAction::Reindex {
                kind: IndexKind::Json,
                ..
            }
        ));

        // No args — defensive.
        assert!(
            ReindexSpec::FirstKey {
                kind: IndexKind::Hash
            }
            .actions(&[])
            .is_empty()
        );
    }

    #[test]
    fn reindex_first_key_or_delete() {
        let a = args(&[b"user:1", b"FIELDS", b"1", b"name"]);
        let actions = ReindexSpec::FirstKeyOrDelete {
            kind: IndexKind::Hash,
        }
        .actions(&a);
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            actions[0],
            ReindexAction::ReindexOrDelete {
                key,
                kind: IndexKind::Hash
            } if key == b"user:1"
        ));
        assert!(
            ReindexSpec::FirstKeyOrDelete {
                kind: IndexKind::Json
            }
            .actions(&[])
            .is_empty()
        );
    }

    #[test]
    fn reindex_delete_keys() {
        let a = args(&[b"a", b"b", b"c"]);
        let actions = ReindexSpec::DeleteKeys.actions(&a);
        assert_eq!(actions.len(), 3);
        assert!(matches!(actions[0], ReindexAction::Delete { key } if key == b"a"));
        assert!(matches!(actions[1], ReindexAction::Delete { key } if key == b"b"));
        assert!(matches!(actions[2], ReindexAction::Delete { key } if key == b"c"));
        assert!(ReindexSpec::DeleteKeys.actions(&[]).is_empty());
    }

    #[test]
    fn reindex_rename() {
        let a = args(&[b"old", b"new"]);
        let actions = ReindexSpec::Rename.actions(&a);
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], ReindexAction::Delete { key } if key == b"old"));
        assert!(matches!(
            actions[1],
            ReindexAction::Reindex {
                key,
                kind: IndexKind::Hash
            } if key == b"new"
        ));
        // Insufficient args yields nothing rather than panicking.
        let one = args(&[b"old"]);
        assert!(ReindexSpec::Rename.actions(&one).is_empty());
    }

    // --- ReindexSpec cross-field validation ---

    #[test]
    fn validate_reindex_on_read_command() {
        let mut spec = base_write_spec();
        spec.flags = CommandFlags::READONLY;
        spec.event = EventSpec::NotApplicable;
        spec.reindex = ReindexSpec::FirstKey {
            kind: IndexKind::Hash,
        };
        assert_eq!(spec.validate(), Err(SpecError::ReindexOnReadCommand));
        // `None` on a read is fine.
        spec.reindex = ReindexSpec::None;
        assert_eq!(spec.validate(), Ok(()));
    }

    #[test]
    fn validate_rename_reindex_requires_first_two() {
        let mut spec = base_write_spec();
        spec.reindex = ReindexSpec::Rename;
        // `KeySpec::First` cannot supply args[1].
        assert_eq!(
            spec.validate(),
            Err(SpecError::RenameReindexRequiresFirstTwo)
        );
        // With FirstTwo it validates. Use a Dynamic event so the multi-key
        // blanket-Emits guard does not fire first.
        spec.keys = KeySpec::FirstTwo;
        spec.arity = Arity::AtLeast(2);
        spec.event = EventSpec::Dynamic;
        assert_eq!(spec.validate(), Ok(()));
    }
}
