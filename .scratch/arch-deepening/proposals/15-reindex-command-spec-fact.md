# Proposal 15 — Make search reindexing a `CommandSpec` fact, not a hardcoded command-name string match

## Summary

Every write side effect in the post-execution pipeline is derived from a declarative
[`CommandSpec`](../../../frogdb-server/crates/core/src/command_spec.rs) fact — WAL persistence from
`WalStrategy`, keyspace notifications from `EventSpec`, waiter wakes from `WaiterWake`, tracking
invalidation and the dirty counter from the key set. Search-index upkeep is the one exception: the
`SearchIndex` effect throws away the handler it holds and re-dispatches on the command's *string
name* against a hand-maintained `match` (`search_hook.rs:17-75`). The completeness of that list is
guaranteed by nothing — and it is **already incomplete**: at least **seven** shipping hash-mutating
write commands are absent from it — `HSETEX`, `HGETDEL`, `HGETEX`, and the whole per-field-TTL
family `HEXPIRE`/`HPEXPIRE`/`HEXPIREAT`/`HPEXPIREAT` (which synchronously *delete* hash fields on a
past/zero time) — so on an `ON HASH` search index they silently leave the index stale. The list also
carries a dead arm (`JSON.MSET`, not a registered command). This proposal promotes reindexing to a
`reindex: ReindexSpec` fact on `CommandSpec`, resolves it to typed actions the way `WalStrategy`
resolves to `WalAction`, and adds a registry conformance test that pins the reindex fact for the
hash/JSON write families.

The primary, defensible wins are structural: a **single declaration site** for reindex behavior
(co-located with `wal`/`event`/`wakes`, with the locality those facts enjoy), **typed actions**
replacing string dispatch, deletion of the dead `JSON.MSET` arm, and the concrete
`HSETEX`/`HGETDEL`/`HGETEX`/TTL-family bug fixes. The conformance test is a real but *partial*
backstop — a reviewer-maintained positive allowlist, not an automatic completeness guarantee (see
[Enforcement](#enforcement-what-the-conformance-test-can-and-cannot-do)); the case for the refactor
rests on the structural/locality gains, not on the test making staleness impossible.

**Recommendation: proceed.** The core premise is verified and is stronger than the candidate stated:
the staleness is not merely a future risk when someone *adds* a command — it is a present, live
defect for `HSETEX`, `HGETDEL`, `HGETEX`, and the four `H(P)EXPIRE(AT)` commands today.

## Problem

The unified write-effect pipeline (`post_execution.rs`) exists precisely to make "which side effect
runs, and how" a property of declared data rather than hand-maintained control flow. Its module doc
is explicit: the two axes of variation are `WalPhase` and `EffectScope`, "consumed as data," and
"there is now exactly one statement of the write-effect set and order for every write path"
(`post_execution.rs:1-31`). Each effect in [`WRITE_EFFECT_ORDER`] reads a `CommandSpec` fact:

- `VersionIncrement` / `DirtyCounter` — from the accumulated `dirty_delta`.
- `TrackingInvalidation` — from `handler.keys(args)` (i.e. `KeySpec`).
- `KeyspaceNotifications` — from `EventSpec` (via `emit_keyspace_notifications_for_command`).
- `WaiterSatisfaction` — from `handler.spec().wakes` (`WaiterWake`) — see
  `satisfy_waiters_for_command` (`post_execution.rs:426-440`).
- `WalPersistence` — from `WalStrategy` (via `record.wal_actions()`).
- `ReplicationBroadcast` — from `EffectScope` framing.

Exactly one effect breaks the pattern. `SearchIndex` does not read a spec fact; it re-derives
behavior from the command's **name string**:

```rust
// post_execution.rs:241-247
WriteEffectKind::SearchIndex => {
    if !self.search.indexes.is_empty() {
        for record in summary.writes {
            self.update_search_indexes(record.handler.name(), record.args);
        }
    }
}
```

`update_search_indexes` (`search_hook.rs:16-76`) is a `match cmd_name { "HSET" | "HSETNX" | ... }`
over a hand-maintained allowlist of names. The handler — which knows its own identity through its
`CommandSpec` — is discarded, and the pipeline reconstructs a weaker notion of that identity by
string comparison against a list no compiler and no test holds to completeness.

### The list is already incomplete — a live defect, not a hypothetical

The candidate framed this as a future hazard ("new/renamed command compiles fine and silently leaves
indexes stale"). Verified against the registered command set, it is a **present** defect:

- **`HSETEX`** (`commands/src/hash.rs:1888`) — `CommandFlags::WRITE`, `KeySpec::First`,
  `EventSpec::Emits { class: HASH, name: "hset" }`, `WalStrategy::PersistFirstKey`. It writes hash
  field values. It is **not** in `update_search_indexes`'s match. On an `ON HASH` index whose prefix
  matches the key, `HSETEX user:1 FIELDS 1 name Alice` persists and replicates correctly but the
  search index is never updated — the document is unsearchable / stale. → `FirstKey { Hash }`.
- **`HGETDEL`** (`commands/src/hash.rs:1701`) — `CommandFlags::WRITE`, `KeySpec::First`,
  `WalStrategy::PersistOrDeleteFirstKey`. It deletes hash fields (and can empty the key). It is
  **not** in the match, so deleting an indexed field leaves stale index/vector entries pointing at
  values that no longer exist. → `FirstKeyOrDelete { Hash }`.
- **`HEXPIRE` / `HPEXPIRE` / `HEXPIREAT` / `HPEXPIREAT`** (`commands/src/hash.rs:1188/1231/1274/1317`)
  — all `CommandFlags::WRITE`, `KeySpec::First`, `WalStrategy::PersistFirstKey`,
  `EventSpec::Suppressed`. These are **not** pure TTL-setters: on a past/zero expiry time they
  *synchronously delete the field* (`FieldAction::Delete`, `hash.rs:1017/1033/1049`), which can empty
  the key. Deleting an indexed field changes hash content, so on an `ON HASH` index they leave the
  document stale exactly as `HDEL` would — and none of the four is in the match. → `FirstKeyOrDelete
  { Hash }` (over-declaring the reindex on the pure-TTL path is safe; see Alternative A).
- **`HGETEX`** (`commands/src/hash.rs:1783`) — `CommandFlags::WRITE`, `KeySpec::First`,
  `WalStrategy::PersistFirstKey`, `EventSpec::Suppressed`. Sets/clears per-field TTLs and calls
  `purge_expired_hash_fields` (`hash.rs:1810`), which can remove already-expired fields (and empty
  the key). Not in the match. → `FirstKeyOrDelete { Hash }` (conservative; its own field *values* are
  unchanged, but the lazy purge can change the field set). See the lazy-purge caveat under
  [Risks](#risks--alternatives).

Not every hash WRITE reindexes: **`HPERSIST`** (`commands/src/hash.rs:1495`, `CommandFlags::WRITE`,
`WalStrategy::PersistFirstKey`) removes a field's TTL and leaves every field *value* unchanged, so it
correctly declares `ReindexSpec::None`. This is the concrete carve-out that makes a naive "every hash
WRITE ⇒ non-`None`" assertion false and forces the conformance guard to be hand-curated (see
[Enforcement](#enforcement-what-the-conformance-test-can-and-cannot-do)).

`update_search_indexes` is the *only* reindex-on-write path (verified: the sole caller is
`post_execution.rs:244`). The only other search-index writer is the active-expiry seam
(`event_loop.rs:163,184`), which calls `delete_from_search_indexes` directly for TTL-expired and
hash-field-emptied keys — a legitimate direct call unrelated to command dispatch. So nothing else
compensates for the two missing commands.

The list also rots in the other direction: it dispatches on `"JSON.MSET"` (`search_hook.rs:50-57`),
but no command named `JSON.MSET` is registered (verified: zero hits for `name: "JSON.MSET"` in
`commands/src`). That arm is dead code maintained by hand.

### Why it is shallow / fragmented (architecture vocabulary)

**The seam re-derives an identity the caller already holds.** The `SearchIndex` effect is handed a
`WriteRecord` carrying `&dyn Command` — the richest possible statement of "what command is this."
It throws that away and rebuilds a lossy copy (`&str`) to look up in a side table. That is a
**locality** failure: the fact "this command reindexes its first key as a hash" lives *twice* — once
implicitly in `HSetCommand`'s existence and once explicitly as the string `"HSET"` in a `match` in a
different module — kept consistent by hand. Every other effect reads the fact from the one place it
is declared; only search reaches around the spec.

**It is the one write effect whose fact lives outside the declaration site.** `WalStrategy`,
`EventSpec`, and `WaiterWake` are all `const` fields the author fills in where the command is
declared, next to its flags and keys. The reindex behavior has no such home — it lives as a string in
a `match` in another module, and the command compiles, registers, persists, replicates, and emits
events correctly while only the search index is silently wrong. Promoting reindex to a `const` field
gives it the same locality: the one place you read a command's identity is the one place you state
how it reindexes.

A caveat on how far the *enforcement* analogy carries. `every_write_command_declares_wal` /
`_event` are structural guards: they iterate **all** commands and a WRITE command that leaves `wal`
at the `NoOp` default (or `event` at `NotApplicable`) auto-fails unless explicitly allowlisted — the
default is itself the tripwire. Reindex cannot use that shape, because `ReindexSpec::None` is the
*correct* default for the overwhelming majority of WRITE commands (every string/list/set/zset/stream
mutation, plus `HPERSIST`). So the guard cannot be "no WRITE defaults to `None`"; it must be a
positive allowlist of the hash/JSON write families with per-command carve-outs (`HPERSIST` ⇒ `None`).
That is a genuine improvement in locality and a real regression backstop, but it is a
*reviewer-maintained* list, not an automatic completeness proof — spelled out under
[Enforcement](#enforcement-what-the-conformance-test-can-and-cannot-do).

**It duplicates a distinction `WalStrategy` already draws.** The match already re-implements, in
string-keyed form, the persist-vs-persist-or-delete decision `WalStrategy` encodes as types: `HSET`
→ unconditional reindex mirrors `PersistFirstKey`; `HDEL`/`JSON.DEL`/`JSON.CLEAR` → "reindex if the
key survives, else delete" mirrors `PersistOrDeleteFirstKey`; `DEL`/`UNLINK` → delete-all mirrors
`DeleteKeys`; `RENAME` → delete-source-reindex-dest mirrors `RenameKeys`. The reindex logic is a
second, weaker copy of the WAL write-set shape, expressed in an incompatible vocabulary.

## Evidence (verified file:line)

| Claim | Location | Verified |
| --- | --- | --- |
| Search effect dispatches on string name, discarding the handler | `core/src/shard/post_execution.rs:241-247` | ✓ |
| `update_search_indexes` is a `match cmd_name { ... }` over a hand-maintained list | `core/src/shard/search_hook.rs:16-76` | ✓ |
| Sole caller of `update_search_indexes` | `core/src/shard/post_execution.rs:244` (only hit) | ✓ |
| Every *other* write effect derives from a spec fact (`WalStrategy`, `EventSpec`, `WaiterWake`, `KeySpec`) | `core/src/shard/post_execution.rs:194-296`, `426-440` | ✓ |
| `HSETEX` is WRITE, mutates hash fields, absent from the match | `commands/src/hash.rs:1888-1904` (spec); `search_hook.rs:17-75` (no arm) | ✓ |
| `HGETDEL` is WRITE, deletes hash fields, absent from the match | `commands/src/hash.rs:1701-1714` (spec); `search_hook.rs:17-75` (no arm) | ✓ |
| `HEXPIRE`/`HPEXPIRE`/`HEXPIREAT`/`HPEXPIREAT` are WRITE and *synchronously delete* fields on past/zero time, absent from the match | `commands/src/hash.rs:1188/1231/1274/1317` (specs), `hash.rs:1017/1033/1049` (`FieldAction::Delete`); `search_hook.rs:17-75` (no arm) | ✓ |
| `HGETEX` is WRITE, lazily purges expired fields, absent from the match | `commands/src/hash.rs:1783` (spec), `hash.rs:1810` (`purge_expired_hash_fields`); `search_hook.rs:17-75` (no arm) | ✓ |
| `HPERSIST` is WRITE but changes no field value ⇒ legitimately `ReindexSpec::None` (carve-out) | `commands/src/hash.rs:1495` (spec) | ✓ |
| Structural guards iterate all commands and trip on the default; reindex `None` is a legit default ⇒ positive allowlist needed | `server/src/server/register.rs:319-333` (`_declares_wal`), `303-315` (`_declares_event`) | ✓ |
| `JSON.MSET` arm is dead (no such registered command) | `search_hook.rs:50-57`; zero `name: "JSON.MSET"` in `commands/src` | ✓ |
| Only other index writer is active-expiry delete (not command dispatch) | `core/src/shard/event_loop.rs:163,184` | ✓ |
| `CommandSpec` has no reindex/search field today | `core/src/command_spec.rs:326-361` (no such field) | ✓ |
| `WalStrategy` → `WalAction` resolver is the model to mirror | `core/src/command.rs:316-393` (enum), `535-598` (`actions_with_delta`) | ✓ |
| Registry conformance test pattern to mirror | `server/src/server/register.rs:302-333` (`every_write_command_declares_{event,wal}`) | ✓ |
| Positive-allowlist conformance test pattern to mirror | `server/src/server/register.rs:338-437` | ✓ |

**Path correction (recorded as a discrepancy):** the candidate cited `core/src/shard/*`; the actual
crate path is `frogdb-server/crates/core/src/shard/*`. All line ranges the candidate gave
(`search_hook.rs:16-76`, `post_execution.rs:241-246`) are otherwise accurate.

## Proposed design (Rust interface sketch)

Add one `const` field to `CommandSpec` and one resolver, mirroring `WalStrategy` → `WalAction`
exactly. Keep the enum **flat and pure-identity** (per the `feedback_spec_enums_pure_identity`
guideline): the spec states *what to reindex*; the `ShardWorker` owns *how* (the existing
`reindex_hash_key` / `reindex_json_key` / `delete_from_search_indexes` bodies are unchanged).

```rust
// core/src/command_spec.rs

/// The index-source type a reindex action projects a key into. A hash-source
/// index reads the key as a hash; a JSON-source index reads it as a JSON doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind { Hash, Json }

/// How a write command's effect on the search index is derived — the search
/// analogue of `WalStrategy`. Declared once per command; the `SearchIndex`
/// write effect resolves it to `ReindexAction`s instead of matching the name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReindexSpec {
    /// Read command, or a write that never changes indexable content
    /// (string/list/set/zset/stream mutations, admin). Default.
    #[default]
    None,
    /// Reindex `args[0]` as `kind`; the key exists after the write
    /// (HSET/HMSET/HINCRBY…; JSON.SET/JSON.MERGE/JSON.NUMINCRBY…).
    FirstKey { kind: IndexKind },
    /// Reindex `args[0]` as `kind` if it still exists, else drop it from the
    /// indexes (HDEL/HGETDEL; JSON.DEL/JSON.CLEAR) — mirrors
    /// `WalStrategy::PersistOrDeleteFirstKey`.
    FirstKeyOrDelete { kind: IndexKind },
    /// Drop every arg key from all matching indexes (DEL/UNLINK) — mirrors
    /// `WalStrategy::DeleteKeys`.
    DeleteKeys,
    /// Drop `args[0]`, reindex `args[1]` as a hash (RENAME/RENAMENX) — mirrors
    /// `WalStrategy::RenameKeys`.
    Rename,
}

/// A typed reindex action against a single key. `ReindexSpec::actions` resolves
/// a spec + args to a sequence of these; the `ShardWorker` applies each.
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
    /// The single source of truth mapping a variant to actions — the search
    /// analogue of `WalStrategy::actions`. Adding a variant extends only this
    /// match.
    pub fn actions<'a>(&self, args: &'a [Bytes]) -> SmallVec<[ReindexAction<'a>; 2]>;
}
```

`CommandSpec` gains one field:

```rust
pub struct CommandSpec {
    // … existing fields …
    /// How the command's writes are projected into search indexes. `None` for
    /// reads and non-hash/non-JSON writes; validated against `flags`.
    pub reindex: ReindexSpec,
}
```

The `SearchIndex` effect stops naming strings and iterates the resolved actions:

```rust
// post_execution.rs — SearchIndex arm
WriteEffectKind::SearchIndex => {
    if !self.search.indexes.is_empty() {
        for record in summary.writes {
            self.apply_reindex(record.handler.spec().reindex, record.args);
        }
    }
}

impl ShardWorker {
    /// Resolve and apply a command's reindex fact. Replaces the string match.
    fn apply_reindex(&mut self, spec: ReindexSpec, args: &[Bytes]) {
        for action in spec.actions(args) {
            match action {
                ReindexAction::Reindex { key, kind } => self.reindex(key, kind),
                ReindexAction::ReindexOrDelete { key, kind } => {
                    if self.store.contains(key) { self.reindex(key, kind) }
                    else { self.delete_from_search_indexes(&Bytes::copy_from_slice(key)) }
                }
                ReindexAction::Delete { key } =>
                    self.delete_from_search_indexes(&Bytes::copy_from_slice(key)),
            }
        }
    }
    // `reindex(key, IndexKind::Hash|Json)` dispatches to the existing
    // `reindex_hash_key` / `reindex_json_key` bodies — moved, not rewritten.
}
```

**Validation.** `CommandSpec::validate` gains cheap cross-field checks (a new `SpecError` variant):
`ReindexSpec != None ⇒ WRITE` (reindexing a read is meaningless), and `ReindexSpec::Rename ⇒
KeySpec::FirstTwo` (it indexes `args[1]`). These are genuine structural guards — they trip on any
command whose fields contradict each other, over the whole registry.

### Enforcement: what the conformance test can and cannot do

Be precise about the backstop, because it is weaker than the WAL/event guards it is modeled on.

- **What is structurally enforced (all commands, automatic).** The two `validate` checks above
  (`reindex != None ⇒ WRITE`; `Rename ⇒ FirstTwo`) fire for any command that violates them, no
  allowlist involved. This catches the "declared a reindex on a read" and "`Rename` on a
  single-key command" mistakes.
- **What is *not* structurally provable — and why.** Completeness ("every command that mutates
  indexable hash/JSON content declares a non-`None` reindex of the right `IndexKind`") is **not**
  derivable from the spec, because the value type a key holds is not in the spec, and — critically —
  `ReindexSpec::None` is the *correct* default for the vast majority of WRITE commands. So the
  `every_write_command_declares_wal` shape (iterate all commands, a `NoOp`/default trips the assert
  unless allowlisted) **cannot** be reused: "no WRITE is `None`" is a false statement (`HPERSIST`,
  and every string/list/set/zset/stream write, are legitimately `None`). Nor is `EventSpec` a clean
  proxy: the TTL family, `HGETEX`, and `HPERSIST` are all `EventSpec::Suppressed`, while `HSETEX`
  `Emits`, so "HASH-class event ⇒ reindex" neither includes all the stale commands nor excludes the
  carve-out.
- **What the conformance test actually is: a reviewer-maintained positive allowlist.** Like
  `data_adding_commands_wake_blocked_clients` (`register.rs:338`) and
  `multi_key_commands_declare_accurate_events` (`register.rs:371`), the reindex test enumerates the
  hash-family and JSON-family write commands by name and asserts each declares the expected
  `ReindexSpec`, plus an explicit carve-out set (`HPERSIST`) asserted to be `None`. This is a real
  regression backstop for the commands it lists, and it turns "silently stale forever" into "a test
  the reviewer updates" — but a **newly-added** hash write that the reviewer forgets to add to *both*
  the spec field *and* the allowlist still ships stale. That is the same failure mode as today's
  string `match`, relocated to a declaration-adjacent field with better locality — an honest,
  incremental improvement, not an automatic completeness proof. The proposal's value rests on the
  locality/typed-action/dead-code/bug-fix gains, not on eliminating the reviewer's obligation.

**Crate direction.** `ReindexSpec` and `IndexKind` are flat enums with no `frogdb_search` dependency,
so they live in `core` alongside the other spec facts; `core` already depends on `frogdb_search`
(`search_hook.rs` uses `frogdb_search::IndexSource`/`extract_json_fields`). No new dependency edges;
`persistence`/`replication`/`cluster` are untouched. The data path stays off Raft (ADR-0001
unaffected — this is a shard-local, in-memory index concern).

## Migration plan (ordered)

1. **Add the types, no behavior change.** Introduce `IndexKind`, `ReindexSpec` (with `#[default]
   None`), `ReindexAction`, and `ReindexSpec::actions` in `core/src/command_spec.rs`, with unit
   tests for `actions` mirroring the `WalStrategy::actions` tests (`command.rs:1486-1568`).
2. **Add the field with a defaulted value.** Add `reindex: ReindexSpec` to `CommandSpec`. Because
   `CommandSpec` literals are `const` and exhaustive, every command's spec must name the field —
   land this as a mechanical `sed`/rustfmt pass inserting `reindex: ReindexSpec::None,` everywhere,
   then override only the hash/JSON commands. (Alternatively, stage via a `Default`-backed builder,
   but the codebase constructs specs as exhaustive struct literals, so the explicit field is
   consistent with `wal`, `event`, etc.)
3. **Declare the real facts on hash/JSON commands.** Set `reindex` on **every** hash-mutating WRITE,
   not just the five already in the string match. The complete hash roster and its target variant:
   - `HSET`/`HSETNX`/`HMSET`/`HINCRBY`/`HINCRBYFLOAT`/`HSETEX` → `FirstKey { Hash }` (key survives).
   - `HDEL`/`HGETDEL` → `FirstKeyOrDelete { Hash }` (may empty the key).
   - `HEXPIRE`/`HPEXPIRE`/`HEXPIREAT`/`HPEXPIREAT`/`HGETEX` → `FirstKeyOrDelete { Hash }` (delete a
     field on past-time / lazy purge; may empty the key). **These four TTL commands and `HGETEX` are
     the additional live bugs beyond `HSETEX`/`HGETDEL`; a migration that stops at the five names in
     today's match re-ships their staleness.**
   - `HPERSIST` → `None` (explicit; removes a TTL, no value change) — and it must also be listed as
     the carve-out in the conformance test.

   Then every JSON mutation (`FirstKey`/`FirstKeyOrDelete { kind: Json }`), `DEL`/`UNLINK`
   (`DeleteKeys`), and `RENAME`/`RENAMENX` (`Rename`). This is where `HSETEX`, `HGETDEL`, `HGETEX`,
   and the four `H(P)EXPIRE(AT)` commands get their (currently missing) declarations — closing the
   live bugs.
4. **Rewire the effect.** Replace `update_search_indexes(name, args)` with
   `apply_reindex(spec.reindex, args)`; move `reindex_hash_key`/`reindex_json_key` behind
   `reindex(key, kind)`. Delete the string `match` and its dead `JSON.MSET` arm. `RENAME`'s wake to
   `WaiterWake::All` and `delete_from_search_indexes` are untouched.
5. **Add conformance tests** (next section) and the `validate` cross-field checks.
6. **Delete-test the old path.** Confirm no remaining caller of `update_search_indexes` by name;
   remove it.

Steps 1–2 are no-ops; the behavior change (and bug fix) is isolated to steps 3–4, and every step is
compiler-driven.

## Test plan

- **Live-bug regression (the headline win).** Integration tests in `server/tests/search.rs`, one per
  currently-stale command. With `FT.CREATE idx ON HASH PREFIX 1 user: SCHEMA name TEXT`:
  - `HSETEX user:1 FIELDS 1 name Alice` → `FT.SEARCH idx "Alice"` returns `user:1`.
  - after `HGETDEL user:1 FIELDS 1 name` → the field is gone from the index.
  - `HEXPIREAT user:1 1 FIELDS 1 name` (past timestamp) deletes the field → `FT.SEARCH idx "Alice"`
    no longer returns `user:1` (repeat for `HEXPIRE … 0`, `HPEXPIRE`, `HPEXPIREAT` to pin all four).
  - `HGETEX` on a key with an already-lapsed field TTL → the purged field leaves the index.
  - **Carve-out**: `HPERSIST user:1 FIELDS 1 name` leaves the document searchable and unchanged
    (guards against the migration over-reacting and, e.g., dropping the doc).

  All the non-`HPERSIST` cases fail today and pass after step 3.
- **Registry conformance (completeness guard — a positive allowlist, not an automatic proof; see
  [Enforcement](#enforcement-what-the-conformance-test-can-and-cannot-do)).** In
  `server/src/server/register.rs`, mirroring `data_adding_commands_wake_blocked_clients`
  (`register.rs:338`): enumerate the full hash-family and JSON-family write commands **by name** —
  including the TTL family and `HGETEX` — and assert each declares the expected `ReindexSpec` variant
  and `IndexKind`. Enumerate the carve-outs separately (`HPERSIST` ⇒ `None`) and assert those. This
  is a reviewer-maintained list: it catches a *listed* command whose spec field drifts, but a
  newly-added hash write the reviewer forgets to add here (and to the spec) still ships stale — the
  test does not, and structurally cannot, iterate "all writes and trip on the `None` default," since
  `None` is the correct default for most writes. Include the structural inverse (this one *is*
  automatic, from `validate`): no non-WRITE command declares a non-`None` reindex fact.
- **Resolver unit tests** (`command_spec.rs`): `ReindexSpec::actions` for each variant — `FirstKey`
  → one `Reindex`, `FirstKeyOrDelete` → one `ReindexOrDelete`, `DeleteKeys` → one `Delete` per arg,
  `Rename` → `Delete(args[0]) + Reindex(args[1], Hash)`, empty args → empty.
- **`validate` cross-field tests**: `reindex != None` on a read → new `SpecError`; `Rename` without
  `KeySpec::FirstTwo` → error.
- **Existing search integration suite** (`server/tests/search.rs`) must stay green — it already
  covers `HSET`/`HDEL`/`RENAME`/`DEL`/`JSON.SET` reindexing, pinning behavioral parity for the
  commands that *were* in the list.

## Risks & alternatives

- **Alternative A — derive reindexing from the WAL write-set instead of a new field (deeper, but
  with a gap).** Because `reindex_hash_key`/`reindex_json_key` already read the stored value and
  no-op if it is the wrong type (`as_hash`/`as_json` → `None`), **over-declaring reindex is safe** —
  the risk is only *under*-declaring. That suggests deriving reindex from `record.wal_actions()`: a
  `Persist`/`PersistOrDelete` key → reindex-or-delete by inspecting its stored type; a delete action
  → index-delete; `ClearShard` → clear all indexes (which, notably, would also fix FLUSHDB, which
  the current string list ignores entirely). This needs **zero** new spec field — search becomes a
  pure projection of the already-declared WAL write-set. The gap: it couples reindex `IndexKind` to
  runtime value inspection rather than declaration, and it misses any `WalStrategy::NoOp` command
  that still mutates an indexable in-memory value. It is the more elegant unification but a larger
  behavioral change; the `ReindexSpec` fact is the conservative, candidate-requested shape and is
  recommended for this round, with WAL-derivation flagged as a follow-up worth prototyping.
- **Alternative B — infer `IndexKind` at reindex time, collapsing `Hash`/`Json`.** Read the key's
  stored `Value` and reindex into whichever indexes match, dropping the `kind` parameter. Rejected
  for now: JSON-source indexes filter on `IndexSource::Json` (`search_hook.rs:143`) while the hash
  path does not, so the current typed split is load-bearing; unifying it is a separate change.
- **`const` literal churn.** Adding a `CommandSpec` field forces a one-line edit to every command
  spec literal. Mechanical and compiler-enforced (a missing field fails to compile), same class of
  churn as any past spec-field addition; use `sed` per the sweeping-change guideline.
- **Completeness is a reviewer-maintained allowlist, and weaker than the WAL guard it resembles.**
  `every_write_command_declares_wal` is a structural guard (a WRITE that defaults `wal` to `NoOp`
  trips it); the reindex guard *cannot* take that form, because `ReindexSpec::None` is the correct
  default for most WRITE commands, so its backstop is a positive allowlist of hash/JSON families with
  an `HPERSIST` carve-out. This is strictly better than today, where *nothing* enforces it, but a
  new hash write forgotten in both the spec field and the allowlist still ships stale — the same
  failure mode, relocated with better locality. Fully spelled out under
  [Enforcement](#enforcement-what-the-conformance-test-can-and-cannot-do); do not oversell it as
  "cannot ship stale."
- **Lazy field-expiry purge is a residual, pre-existing gap this proposal does not fully close.**
  `purge_expired_hash_fields` runs inside *read* commands too (`HGET`/`HGETALL`/… call it before
  reading), and a READONLY command cannot carry a reindex fact. So a field that lapses and is purged
  during a pure read leaves the index stale until the next write or the active-expiry seam
  (`event_loop.rs`) touches the key. `ReindexSpec` fixes the *write* paths (including `HGETEX`, which
  is WRITE); the read-path lazy purge is out of scope and unchanged from today. Flagged as a separate
  follow-up (the active-expiry seam already deletes hash-field-emptied keys from the index, so the
  window is bounded, not unbounded).
- **Behavioral parity for the migrated commands.** The existing `search.rs` suite pins
  `HSET`/`HDEL`/`RENAME`/`DEL`/`JSON.*` behavior; the resolver must reproduce the exact
  reindex-vs-delete branch the old `match` took (notably `HDEL`/`JSON.DEL`/`JSON.CLEAR`'s
  contains-check → `FirstKeyOrDelete`). Verified against `search_hook.rs:28-67`.

## Effort

**M.** The type additions and resolver are small and mirror `WalStrategy` closely; the `ShardWorker`
rewire is a move, not a rewrite. The bulk is the mechanical `CommandSpec`-literal field addition
across all commands (compiler-driven) plus declaring the real fact on the ~20 hash + ~12 JSON write
commands and `DEL`/`UNLINK`/`RENAME`. Confined to `core` + the `commands` crate's spec literals +
one conformance test in `server`; no cross-crate signature churn, no async or ordering changes (the
`SearchIndex` effect keeps its slot in `WRITE_EFFECT_ORDER`). Not S because of the spec-literal
sweep and the parity care for the persist-or-delete branch; not L because it is one effect, one new
field, and the compiler drives the migration.

## Related

- **EventSpec `EmitsAt`/`Dynamic` (round 4)** — the direct precedent: keyspace notifications were
  moved from ad-hoc handler code to a declarative `EventSpec` fact with a `validate()` guard and the
  `every_write_command_declares_event` conformance test. `ReindexSpec` is the same move for the one
  remaining string-dispatched write effect.
- **CommandSpec discipline** — `WalStrategy` (`command.rs:316-598`), `WaiterWake`, `LookupSpec`, and
  `AccessSpec` establish the pattern: mechanical facts are declared once, resolved to typed actions
  by a single `actions()` match, and enforced by `CommandSpec::validate` +
  `every_write_command_declares_*`. This proposal brings search upkeep into that discipline.
- **`feedback_spec_enums_pure_identity`** — `ReindexSpec` is a flat, pure-identity enum in `core`;
  the reindex *implementation* (`reindex_hash_key`/`reindex_json_key`) stays in the `ShardWorker`.
- **Proposal 10 / 12** — same round's theme: a fact re-derived by a caller (event mapping in `apply`;
  coalesce decision split across two seam shapes) collapsed into a single declared owner.

## Adversarial review

**Verdict: AMEND** (both issues major, both addressed in place; core design accepted as correct and
consistent with `WalStrategy`/`EventSpec` precedent). All cited structural evidence re-verified
against source during this revision.

**Issue 1 (major) — the stale-command set is larger than `HSETEX`/`HGETDEL`; a migration following
the old step-3 enumeration re-ships the bug for the TTL family and `HGETEX`.** *Confirmed and fixed.*
Verified in `commands/src/hash.rs`: `HEXPIRE`/`HPEXPIRE`/`HEXPIREAT`/`HPEXPIREAT` (specs at
1188/1231/1274/1317) are all `CommandFlags::WRITE`, `WalStrategy::PersistFirstKey`, and
*synchronously delete* a hash field on a past/zero time (`FieldAction::Delete`, hash.rs:1017/1033/1049)
— none is in `search_hook.rs`'s match. `HGETEX` (1783, WRITE) calls `purge_expired_hash_fields`
(1810), which removes lapsed fields. All are stale today for the same reason as `HSETEX`/`HGETDEL`.
Resolution: broadened the Summary and live-defect section from two commands to seven; added the full
hash roster with target variants to migration step 3 (TTL family + `HGETEX` ⇒ `FirstKeyOrDelete
{ Hash }`); added per-command regression tests to the test plan; added Evidence rows. Also verified
and incorporated the reviewer's carve-out: `HPERSIST` (1495, WRITE) changes no field value and
correctly stays `ReindexSpec::None`, so a naive "every hash WRITE ⇒ non-`None`" assertion is false.

**Issue 2 (major) — the "conformance test so a … command that fails to declare its reindex fact
cannot ship" / "completeness … enforced" framing overstates the guard, which is a reviewer-maintained
positive allowlist, not a structural completeness proof.** *Confirmed and fixed.* Verified
`every_write_command_declares_wal` (register.rs:319) iterates all commands and trips on the `NoOp`
default unless allowlisted — a structural guard. Reindex cannot reuse that shape because
`ReindexSpec::None` is the correct default for most WRITE commands (`HPERSIST` + every
string/list/set/zset/stream write), and `EventSpec` is not a clean proxy (TTL family, `HGETEX`,
`HPERSIST` are all `Suppressed`; `HSETEX` `Emits`). Resolution: removed the "cannot ship" claim from
the Summary; rewrote the "Why it is shallow" enforcement paragraph to distinguish structural guards
from the allowlist; added a dedicated **Enforcement: what the conformance test can and cannot do**
subsection stating exactly what is automatic (`validate` cross-field checks) vs. reviewer-maintained
(the family allowlist with the `HPERSIST` carve-out); reframed the wins as locality / typed actions /
dead-`JSON.MSET` removal / concrete bug fixes; and softened the Risks bullet accordingly. Added a
Risks bullet disclosing the residual lazy-purge gap on READONLY commands, which `ReindexSpec` does
not and cannot close.

**Reviewer notes accepted without change:** premise sound (if understated); all structural evidence
(post_execution.rs:241-247 discards the handler; dead `JSON.MSET` arm; active-expiry as the only
other legitimate index writer; core→frogdb-search edge already present; `IndexSource::Json` filter
makes the Hash/Json split load-bearing; `WalStrategy::actions` a faithful model); no conflict with
rounds 1-9; `WRITE_EFFECT_ORDER` slot preserved; off-Raft/ADR-0001 unaffected; no new crate edges.
Cost/benefit noted in-doc: the known bugs are independently fixable in a few lines, so the refactor's
justification rests on the structural/locality gains — now stated without enforcement overclaim.
