# Proposal: Declarative Command Spec

Status: proposed
Date: 2026-06-12

## Summary

The `Command` trait is a shallow interface: every mechanical fact about a command — which args are
keys, which keyspace event it emits, which blocked clients it wakes — is an *opt-in* method that
each of ~297 `impl Command` blocks must remember to hand-implement. Forgetting an override compiles
cleanly and fails silently at runtime. This proposal deepens the interface: commands declare one
`CommandSpec` constant (data, not code), and the dispatcher derives key extraction, event emission,
waiter waking, WAL persistence, and routing from it. Commands implement `execute()` + one spec.

---

## Problem

The trait at `frogdb-server/crates/core/src/command.rs:297-374` has eleven methods. Three of them
(`wakes_waiters`, `keyspace_event_type`, `keys_with_flags`) have silent defaults, and one (`keys`)
must be hand-rolled per command. The result is that correctness lives in what each command author
*remembered to write*, and bugs hide in what was forgotten — invisible to the compiler, to review
diffs, and to most tests.

### Verified evidence (counts in `frogdb-server/crates/commands/src/`)

| Fact | Count | Consequence of omission |
|------|-------|-------------------------|
| `impl Command` blocks | 297 (383 repo-wide) | — |
| Hand-rolled `fn keys<'a>` bodies | 255 (355 repo-wide) | Most are the identical "first arg" pattern |
| `impl_keys_first!` macro uses | 42 (44 repo-wide) | Macro exists but 85% of impls don't use it |
| `keyspace_event_type()` overrides | 68 of 297 | Missing override → no keyspace notification, silently |
| `wakes_waiters()` overrides | 15 of 297 | Missing override → blocked BLPOP/BZPOPMIN never wakes |
| `keys_with_flags()` overrides | 7 (8 repo-wide) | `COMMAND GETKEYSANDFLAGS` reports wrong access flags |

### Real bugs found while verifying this proposal

These are not hypothetical. Each is a write command where an opt-in method was forgotten:

- **LREM emits no keyspace notification.** `commands/src/list.rs:713-763` is `WRITE`-flagged with a
  `wal_strategy` but no `keyspace_event_type()`. Redis emits `lrem`. FrogDB silently emits nothing.
- **RPOPLPUSH / LMOVE never wake destination waiters.** `commands/src/list.rs:951` and
  `list.rs:1040` push an element into the destination list but implement no `wakes_waiters()`, so
  `satisfy_waiters_for_command` (`core/src/shard/pipeline.rs:193-207`) hits the `WaiterWake::None`
  arm and a client blocked in `BLPOP dest` is not woken by the move. Same gap for `ZINCRBY` (can
  create a member that should satisfy `BZPOPMIN`; it is not among the 15 commands that wake).
- **GEORADIUS STORE loses its destination key entirely.** `commands/src/geo.rs:535-541` returns
  only `args[0]` from `keys()`, so the `STORE` destination is invisible to routing, ACL checks,
  client-tracking invalidation, and notifications — and the `wal_strategy` comment at
  `geo.rs:464-468` admits the destination "is not currently captured here", i.e. the stored zset
  is never WAL-persisted.

### Why the macros didn't fix it

`define_command!` (`core/src/command_macro.rs:109`) already has declarative syntax (`keys: first`),
yet it has **zero** uses outside its own tests. The `impl_keys_*` macros (`command_macro.rs:730-805`)
cover only `keys()` and are used by a sixth of impls. Codegen sugar over a shallow interface adds
no leverage: the dispatcher still consults per-command opt-in methods, so every new command still
has five chances to forget something and no place where a reviewer can see all five facts at once.
The fix belongs in the interface, not the syntax.

### Where the dispatcher consumes these methods

Every consumer is a seam that trusts the opt-in methods were implemented:

| Call site | Consumes | Purpose |
|-----------|----------|---------|
| `server/src/connection/routing.rs:53` | `keys()` | Shard routing + ACL key checks |
| `core/src/shard/pipeline.rs:50,125` | `keys()` | Client-tracking invalidation |
| `core/src/shard/pipeline.rs:66,141` | `keyspace_event_type()` via notify | Keyspace notifications |
| `core/src/shard/pipeline.rs:194` | `wakes_waiters()` + `keys()` | Wake blocked clients |
| `core/src/shard/keyspace_notify.rs:76-80` | `keyspace_event_type()`, `keys()` | Event emission |
| `core/src/shard/execution.rs:110` | `keys()` | Expired-key handling |
| `core/src/registry.rs:56-68` | `keys()`, `keys_with_flags()` | `COMMAND GETKEYS[ANDFLAGS]` |
| `server/src/connection/guards.rs:256,343,434` | `keys()` | Cross-slot / cluster guards |
| `server/src/connection/handlers/transaction.rs:482` | `keys()` | WATCH/MULTI key collection |
| `server/src/connection/handlers/hotkeys.rs:540` | `keys()` | Hotkey tracking |

`keyspace_notify.rs:93-176` adds a second, parallel source of truth: an 84-line name-keyed
`command_to_event_name` adapter table mapping `"LPUSH"` → `"lpush"`, with a `Box::leak` fallback
(`keyspace_notify.rs:170-174`) for names the table forgot. Event *class* lives on the command;
event *name* lives in a match in another crate. Poor locality, two places to forget.

---

## Current state

A typical command hand-implements every mechanical fact. SETNX, copied verbatim from
`commands/src/string.rs:28-73`:

```rust
pub struct SetnxCommand;

impl Command for SetnxCommand {
    fn name(&self) -> &'static str {
        "SETNX"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // ... 13 lines of actual logic ...
    }

    fn keyspace_event_type(&self) -> Option<KeyspaceEventFlags> {
        Some(KeyspaceEventFlags::STRING)
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}
```

That `keys()` body — 7 lines of "first arg is the key" — appears ~255 times. The trait defaults
that make forgetting silent, from `core/src/command.rs:330-340`:

```rust
    fn wakes_waiters(&self) -> WaiterWake {
        WaiterWake::None
    }

    fn keyspace_event_type(&self) -> Option<KeyspaceEventFlags> {
        None
    }
```

And the forgotten case, RPOPLPUSH (`commands/src/list.rs:951-1032`, abridged) — note what is *not*
there:

```rust
impl Command for RpoplpushCommand {
    fn name(&self) -> &'static str { "RPOPLPUSH" }
    fn arity(&self) -> Arity { Arity::Fixed(2) }
    fn flags(&self) -> CommandFlags { CommandFlags::WRITE }
    fn wal_strategy(&self) -> WalStrategy { WalStrategy::MoveKeys }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // ... pops from source, then:
        let dest_list = get_or_create_list(ctx, dest)?;
        dest_list.push_front(element.clone());      // data arrives at dest...
        Ok(Response::bulk(element))
    }
    // ...keys(), keys_with_flags() ...
    // NO wakes_waiters() — BLPOP blocked on dest is never woken.
}
```

The dispatcher faithfully derives nothing from this, because there is nothing to derive from
(`core/src/shard/pipeline.rs:193-207`):

```rust
    fn satisfy_waiters_for_command(&mut self, handler: &dyn Command, args: &[Bytes]) {
        match handler.wakes_waiters() {
            WaiterWake::None => {}                  // ← RPOPLPUSH lands here
            WaiterWake::Kind(kind) => {
                let keys = handler.keys(args);
                self.satisfy_waiters(kind, &keys);
            }
            // ...
        }
    }
```

`WalStrategy` (`core/src/command.rs:183-294`) already proves the better pattern works here: it is
declared as data, resolved by a single `actions()` derivation function, and unit-tested once. This
proposal extends that pattern to the rest of the command mechanics.

---

## Proposed design

Deepen the `Command` module: shrink the interface to **one spec constant + `execute()`**, and move
all derivation behind the seam. The spec is plain `const`-constructible data, so it can be
inspected, table-tested, snapshot-diffed, and validated exhaustively at registry-build time.

### The spec type (new `core/src/command_spec.rs`)

```rust
/// How a command's keys are located in its argument list.
/// Covers every static pattern in the codebase today; `Dynamic` is the escape hatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeySpec {
    /// No keys (PING, INFO).
    None,
    /// args[0] is the only key (GET, SET, ZADD — the ~255-copy pattern).
    First,
    /// args[0] and args[1] (RENAME, COPY, SMOVE, LMOVE, RPOPLPUSH).
    FirstTwo,
    /// Every argument is a key (DEL, MGET, EXISTS).
    All,
    /// Every `step`-th argument starting at 0 (MSET, MSETNX: key value key value...).
    Stride { step: usize },
    /// args[n..] are all keys (PFMERGE-style).
    Skip(usize),
    /// args[numkeys] holds a count N; N keys start at `first` (EVAL, ZUNIONSTORE).
    NumkeysAt { numkeys: usize, first: usize },
    /// Keys depend on argument values (SORT...STORE, GEORADIUS...STORE).
    /// The command must implement `Command::dynamic_keys`; the registry asserts
    /// this variant appears iff `CommandFlags::MOVABLEKEYS` is set.
    Dynamic,
}

impl KeySpec {
    /// The single derivation point — replaces ~255 hand-rolled `keys()` bodies.
    pub fn extract<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> { /* one match */ }
}

/// What keyspace notification a write command emits.
/// Deliberately not an `Option`: "forgot to declare" is unrepresentable. A write
/// command that emits nothing must say `Suppressed` and justify it at the site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSpec {
    /// Emits `class` events named `name` for each extracted key (e.g. LIST/"lpush").
    Emits { class: KeyspaceEventFlags, name: &'static str },
    /// Write command that deliberately emits nothing (e.g. FLUSHDB).
    Suppressed,
    /// Read command — notifications do not apply.
    NotApplicable,
}

/// Per-key access flags for COMMAND GETKEYSANDFLAGS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessSpec {
    /// Derive one flag from CommandFlags (today's default, command.rs:356-364).
    Uniform,
    /// Explicit flag per key position; last entry repeats for trailing keys.
    Positional(&'static [KeyAccessFlag]),
    /// Defer to `Command::dynamic_keys_with_flags` (SORT).
    Dynamic,
}

/// Declarative description of a command's mechanics. One `static` per command.
#[derive(Debug, Clone)]
pub struct CommandSpec {
    pub name: &'static str,
    pub arity: Arity,
    pub flags: CommandFlags,
    pub keys: KeySpec,
    pub access: AccessSpec,
    pub wal: WalStrategy,          // already declarative — folds in unchanged
    pub wakes: WaiterWake,         // already a data type — folds in unchanged
    pub event: EventSpec,
    pub requires_same_slot: bool,
}

impl CommandSpec {
    /// Cross-field invariants, checked at registry build (debug) and by tests:
    ///   WRITE  => event != NotApplicable
    ///   WRITE  => wal != NoOp            (explicit allowlist: FLUSHDB, FLUSHALL)
    ///   Dynamic keys <=> MOVABLEKEYS flag
    ///   arity minimum covers the highest arg index the KeySpec reads
    pub fn validate(&self) -> Result<(), SpecError> { /* ... */ }
}
```

### The deepened trait

```rust
pub trait Command: Send + Sync {
    /// All mechanical behavior is derived from this.
    fn spec(&self) -> &'static CommandSpec;

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError>;

    /// Escape hatch — only consulted when `spec().keys == KeySpec::Dynamic`.
    fn dynamic_keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        Vec::new()
    }

    /// Escape hatch — only consulted when `spec().access == AccessSpec::Dynamic`.
    fn dynamic_keys_with_flags<'a>(&self, args: &'a [Bytes]) -> Vec<(&'a [u8], Vec<KeyAccessFlag>)> {
        // default derives from dynamic_keys + Uniform
    }
}

impl dyn Command + '_ {
    /// Derived once here — not 297 times.
    pub fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        match self.spec().keys {
            KeySpec::Dynamic => self.dynamic_keys(args),
            shape => shape.extract(args),
        }
    }
}
```

### Two example commands, after

LPUSH (today: `list.rs:30-74`, 7 method bodies) becomes one constant plus unchanged `execute`:

```rust
pub struct LpushCommand;

impl Command for LpushCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "LPUSH",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::Kind(WaiterKind::List),
            event: EventSpec::Emits { class: KeyspaceEventFlags::LIST, name: "lpush" },
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // unchanged from list.rs:51-61
    }
}
```

RPOPLPUSH — the migration audit is forced to confront every field, which is exactly how the
missing-wake bug gets found and fixed:

```rust
impl Command for RpoplpushCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "RPOPLPUSH",
            arity: Arity::Fixed(2),
            flags: CommandFlags::WRITE,
            keys: KeySpec::FirstTwo,
            access: AccessSpec::Positional(&[KeyAccessFlag::RW, KeyAccessFlag::RW]),
            wal: WalStrategy::MoveKeys,
            wakes: WaiterWake::Kind(WaiterKind::List),   // ← was silently missing before
            event: EventSpec::Emits { class: KeyspaceEventFlags::LIST, name: "rpoplpush" },
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // unchanged from list.rs:968-1009
    }
}
```

### Dispatcher derivation, after

`keyspace_notify.rs:64-86` shrinks, and the entire 84-line `command_to_event_name` adapter table —
including its `Box::leak` fallback — passes the deletion test, because the event name now lives in
the spec next to the command (locality):

```rust
pub(crate) fn emit_keyspace_notifications_for_command(&self, handler: &dyn Command, args: &[Bytes]) {
    if self.notify_keyspace_events.load(Ordering::Relaxed) == 0 {
        return;
    }
    let EventSpec::Emits { class, name } = handler.spec().event else {
        return;
    };
    for key in handler.keys(args) {
        self.emit_keyspace_notification(key, name, class);
    }
}
```

`satisfy_waiters_for_command` (`pipeline.rs:193`) keeps identical logic, sourcing
`handler.spec().wakes` instead of `handler.wakes_waiters()`. Routing (`routing.rs:53`), guards,
WATCH collection, and hotkeys all keep calling `handler.keys(args)` — the derivation just moved
behind the interface, so call sites do not churn.

### Why this is the right shape

- **Depth.** The interface drops from eleven members to two-plus-escape-hatches. Today's trait is
  shallow — its surface mirrors the dispatcher's internals, and every command author must know
  which six call sites consume which optional method. After, the seam between commands and
  dispatcher is a single data structure; the derivation machinery is hidden behind it.
- **Leverage.** One `KeySpec::extract` serves routing, ACL, tracking-invalidation, notification,
  waking, WATCH, hotkeys, and `COMMAND GETKEYS` — eight consumers, one tested implementation.
  `WalStrategy::actions()` (command.rs:248-293) already demonstrates the payoff of this pattern.
- **Locality.** Everything mechanically true about LPUSH is in one ~10-line constant in one file,
  reviewable at a glance, instead of being smeared across five optional methods plus a name-keyed
  match in another crate.
- **Deletion test.** The end state deletes: the four `impl_keys_*` macros, ~255 `keys()` bodies
  (~1,500 lines), the `keyspace_event_type`/`wakes_waiters`/`keys_with_flags`/`requires_same_slot`
  trait methods and their defaults, the `command_to_event_name` table, and the duplicated default
  in `CommandMetadata` (command.rs:398-406). If any of that can't be deleted at the end, the design
  has failed.

---

## Migration plan

Incremental; each step ships green and is independently revertable.

| Step | Work | Notes |
|------|------|-------|
| 1 | Add `command_spec` module: `CommandSpec`, `KeySpec::extract`, `EventSpec`, `AccessSpec`, `validate()`. Table-driven unit tests for `extract` (port the cases from `command_macro.rs` tests). | No consumers yet; zero risk. |
| 2 | Add transitional `fn spec(&self) -> Option<&'static CommandSpec> { None }` to `Command`. Add derived helpers on `CommandEntry` (`registry.rs`) that prefer the spec and fall back to legacy methods — an adapter that lets old and new impls coexist. Point all ten dispatcher call sites at the helpers. | Behavior identical; the adapter is the temporary seam. |
| 3 | Migrate command families one PR each: `string` → `list` → `hash`/`set` → `sorted_set/` → `stream/` → `generic`/`expiry` → `bitmap`/`geo`/`hyperloglog` → `json/` → probabilistic (`bloom`,`cms`,`cuckoo`,`topk`,`tdigest`) → `timeseries`/`vectorset`/`event_sourcing` → `scripting` + server-crate commands. Each PR: write specs, cross-check event names against Redis `notify-keyspace-events` docs and the legacy `command_to_event_name` table, audit `wakes`/`wal`. `execute()` bodies untouched. | This audit is where LREM, RPOPLPUSH, LMOVE, ZINCRBY, and GEORADIUS-STORE class bugs get fixed, each with a regression test. |
| 4 | Flip `spec()` to required (`-> &'static CommandSpec`); fold `CommandMetadata` onto `CommandSpec`; delete legacy trait methods, `impl_keys_*` macros, `command_to_event_name`, and `define_command!` (or rewrite it as thin sugar that emits a spec). | The deletion-test payoff. Mechanical; use `sed`/`awk` for the residue. |
| 5 | Enable `CommandSpec::validate()` at registry build (debug assert) and land the exhaustiveness test suite (below). | Locks the door behind us. |

Redis/Valkey precedent: this is the same direction as Redis's static command table
(`commands.def`, generated from per-command JSON with declarative key specs — `begin_search` /
`find_keys`) and Valkey's identical scheme; DragonflyDB likewise registers commands as table
entries with key-range descriptors rather than per-command virtual key extraction. The proposed
`KeySpec` is deliberately close to Redis's `key_specs` so future `COMMAND DOCS`/`COMMAND INFO`
parity falls out of the same data.

---

## Testing impact

The spec turns per-command, per-method behavior into registry-wide, machine-checkable properties:

- **One table test replaces 255 implicit ones.** `KeySpec::extract` gets exhaustive
  empty-args/short-args/each-variant coverage once; every migrated command inherits it.
- **Exhaustiveness over `registry.iter()`** (`registry.rs:151`) — impossible today because
  "missing" and "deliberately none" are indistinguishable:

  ```rust
  #[test]
  fn every_write_command_declares_event_and_wal() {
      for (name, entry) in build_full_registry().iter() {
          let spec = entry.spec();
          if spec.flags.contains(CommandFlags::WRITE) {
              assert!(!matches!(spec.event, EventSpec::NotApplicable),
                  "{name}: write command must declare Emits or Suppressed");
              assert!(!matches!(spec.wal, WalStrategy::NoOp) || WAL_NOOP_ALLOWLIST.contains(&name),
                  "{name}: write command must declare a WAL strategy");
          }
      }
  }
  ```

- **Cross-field consistency:** `KeySpec::Dynamic` ⇔ `MOVABLEKEYS`; arity minimum covers the
  highest arg index the `KeySpec` reads (e.g. `FirstTwo` requires `min >= 2`); `Suppressed` only on
  `WRITE` commands.
- **Spec snapshot test:** dump all specs as a golden table. Any change to any command's mechanics
  becomes a reviewable one-line diff; drift against Redis (`COMMAND INFO`, the redis-regression
  crate) becomes diffable data instead of greppable code.
- **Waiter waking** cannot be fully machine-checked (whether a command *adds* data is semantic),
  but a heuristic test — commands whose event class is LIST/ZSET/STREAM and whose WAL strategy
  persists a key must declare `wakes`, with an explicit allowlist for the pop/trim exceptions —
  would have caught all three gaps found above. Plus targeted integration regressions:
  `BLPOP` + `LMOVE`, `BZPOPMIN` + `ZINCRBY`.

---

## Risks / open questions

- **Genuinely dynamic key extraction.** SORT with `STORE` (`sort.rs:459-492`), EVAL/EVALSHA
  numkeys (`server/src/commands/scripting.rs:52-68`), and GEORADIUS `STORE` keep hand-written
  extractors behind `KeySpec::Dynamic`. The escape hatch is explicit and enumerable: the registry
  can assert exactly which commands use it, and each one needs the `MOVABLEKEYS` flag. GEORADIUS is
  *currently wrong* (dest never extracted, never WAL-persisted per the comment at `geo.rs:464-468`);
  migration must fix it, which likely also needs a dynamic-destination WAL escape hatch (e.g.
  `WalStrategy` gaining a variant resolved against extracted keys rather than raw arg indexes).
- **Conditional emission.** Redis notifies only when a command actually modifies data (ZADD GT
  that changes nothing emits nothing). Today FrogDB emits for every executed WRITE command
  (`pipeline.rs:66`) regardless of `dirty_delta`. The static spec doesn't change that, but the
  single derivation point makes gating emission on `dirty_delta` a one-line follow-up. Decide
  whether to fold that into step 3 or track separately.
- **Subcommand-shaped commands.** XGROUP's event name is `xgroup-create` only for one subcommand.
  Options: split per-subcommand command structs (matches Redis's container-command model), or an
  `EventSpec::Dynamic` escape hatch. Prefer splitting; decide during the `stream/` family PR.
- **Const-constructibility.** All spec fields are `const`-constructible today (`bitflags` unions,
  plain enums, `usize`). If a future field isn't, per-command `LazyLock` works; minor.
- **Should `ExecutionStrategy` fold into the spec?** It is already declarative data
  (`command.rs:62-102`) and belongs there for the single-spec story, but it adds churn to step 4.
  Lean yes, as a follow-up once the trait methods above are gone.
- **Churn.** ~300 impl blocks touched. Mitigated by: family-at-a-time PRs, `execute()` bodies
  untouched, `sed`/`awk` for the mechanical residue, and the snapshot test making each PR's
  behavioral delta (the audit fixes) explicit rather than buried in boilerplate.
