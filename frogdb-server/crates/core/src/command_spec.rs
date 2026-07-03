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

use crate::command::{Arity, CommandFlags, KeyAccessFlag, WaiterWake, WalStrategy};
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
}

/// What keyspace notification a write command emits.
///
/// Deliberately not an `Option`: "forgot to declare" is unrepresentable. A
/// write command that emits nothing must say [`EventSpec::Suppressed`] and
/// justify it at the declaration site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSpec {
    /// Emits `class` events named `name` for each extracted key
    /// (e.g. `LIST` / `"lpush"`).
    Emits {
        class: KeyspaceEventFlags,
        name: &'static str,
    },
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
        }
    }
}

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
        spec.arity = Arity::Fixed(1); // needs >= 2
        assert_eq!(
            spec.validate(),
            Err(SpecError::ArityTooSmallForKeys { needs: 2, min: 1 })
        );
        spec.arity = Arity::AtLeast(2);
        assert_eq!(spec.validate(), Ok(()));
    }
}
