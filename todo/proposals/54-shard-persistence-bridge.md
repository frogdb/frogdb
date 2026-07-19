# Proposal: Shard Persistence Bridge

Status: implemented
Date: 2026-07-16

## Problem

There is one decision between a `WriteRecord` and the `WriteSink` seam — *given a command's
resolved WAL actions, write them and (rollback only) confirm they are durable* — but it has no
module. It is spread across three functions in two files, expressed as three copies of the same
loop distinguished by a single boolean, and it has **zero** inline tests. The one layer that is
genuinely interesting — how `execute_wal_action` probes `store.contains()` *relative to* when the
handler mutated the store — is reachable only through a fully-assembled `ShardWorker`, so the bugs
that live there can only be exercised end-to-end.

| Symptom | Where |
|---------|-------|
| Three persist functions, each iterating `for action in record.wal_actions() { execute_wal_action }`, differing only in durability confirmation | `shard/post_execution.rs:448` `persist_by_strategy` (fire-and-log); `shard/persistence.rs:141` `persist_and_confirm` (`flush_through(start_seq)`); `shard/persistence.rs:168` `persist_transaction_to_wal` (batch, then one `flush_through`) |
| The action→sink map and its store-existence probes | `shard/persistence.rs:88-117` `execute_wal_action` (`store.contains()` gates `PersistOrDelete` / `DeleteIfMissing` / `PersistIfExists`) |
| No unit coverage of the bridge — every assertion is integration | `shard/persistence.rs` (0 `#[test]`); `crates/server/tests/integration_persistence.rs` (1449 lines) |
| Positional arg indices re-encode key-layout knowledge the router already owns | `core/src/command.rs:250` `PersistDestination(idx)`, `:258` `PersistOrDeleteDestination(idx)` |
| The pattern this should copy, one layer down | `crates/persistence/src/wal/flush.rs:103` `WriteSink` — a real two-adapter seam (`RocksSink` + failure-injecting `TestSink`, 18 tests in `wal/tests.rs`) |

Two consequences:

1. **One boolean of variation, expressed as three functions.** The only real difference between the
   three persist loops is *durability confirmation*: the rollback paths (`persist_and_confirm`,
   `persist_transaction_to_wal`) snapshot `wal.sequence()` and call `flush_through(start_seq)` so an
   acked write can never outrun a swallowed flush failure; the effect path (`persist_by_strategy`)
   fires and logs on error. This is exactly the anti-pattern the post-execution module docs describe
   eliminating — the "four near-identical functions" story at `post_execution.rs:6`, where a
   `(WAL phase × scope)` matrix of clones was collapsed into `WalPhase`/`EffectScope` *data* iterated
   by one pipeline (proposal 03). The persist bridge never got the same treatment: the confirmation
   axis is still control-flow duplication, not a parameter.

2. **The interesting layer is untestable in isolation.** Whether `PersistOrDelete` probes the store
   *after* the handler mutated it, whether a rollback restores the exact pre-image, whether a
   `DeleteIfMissing` correctly no-ops when the key survived — these are properties of how
   `execute_wal_action` calls `store.contains()` in the execution ordering, and today they are only
   observable through a live shard with RocksDB behind it. The layer *below* (`WriteSink`) is a clean
   seam with an in-memory adapter and 18 unit tests; the layer *above* it is welded to a ~40-line
   worker builder. The seam stopped one layer too low.

Context: FrogDB's "WAL" is not an append-only redo log — it is an async write-behind pipeline
staging RocksDB `WriteBatch` entries; on flush failure the batch is *dropped*
(`flush.rs:95-98`). Durability here means redo-of-current-in-memory-state, so "confirm" means
"the flush that carried my entries did not fail", not "my log record is fsync'd".

## Design

Two moves, both deepening existing seams rather than forking new ones.

### 1. One persist function, durability as data

Collapse the three loops into one entry point on the bridge, with the confirmation axis expressed
as an enum the way `WalPhase`/`EffectScope` express theirs:

```rust
/// Whether the persist bridge confirms durability after staging a command's
/// WAL actions. The single axis of variation that used to be three functions.
pub(crate) enum Durability {
    /// Rollback mode: snapshot the sequence, stage, then `flush_through` it, so
    /// the caller can propagate a flush failure (an acked write must not outrun
    /// a swallowed flush). Mirrors `WalPhase`'s treatment in proposal 03.
    Confirm,
    /// Effect (hot) path: stage and log on error; the flush pipeline owns
    /// durability asynchronously.
    FireAndForget,
}

impl ShardWorker {
    /// The one place a batch of `WriteRecord`s becomes WAL writes. Absorbs
    /// `persist_by_strategy`, `persist_and_confirm`, and
    /// `persist_transaction_to_wal`; the single-record callers pass a
    /// one-element slice.
    pub(crate) async fn persist(
        &self,
        records: &[WriteRecord<'_>],
        durability: Durability,
    ) -> std::io::Result<()> {
        let Some(wal) = self.persistence.wal_writer() else { return Ok(()) };
        let start_seq = matches!(durability, Durability::Confirm).then(|| wal.sequence());

        for record in records {
            for action in record.wal_actions() {
                let target = self.wal_target();
                match durability {
                    Durability::Confirm => execute_wal_action(&target, &action).await?,
                    Durability::FireAndForget => {
                        let _ = execute_wal_action(&target, &action)
                            .await
                            .inspect_err(|e| tracing::error!(error = %e, "WAL persist failed"));
                    }
                }
            }
        }
        match start_seq {
            Some(seq) => wal.flush_through(seq).await, // Confirm
            None => Ok(()),                            // FireAndForget
        }
    }
}
```

The effect path calls `persist(&[record], FireAndForget)`; `persist_and_confirm` becomes
`persist(&[record], Confirm)`; `persist_transaction_to_wal` becomes `persist(write_infos, Confirm)`.
Delta-vs-full routing stays where it already is — inside `WriteRecord::wal_actions`
(`command.rs:390`) — so the two paths still cannot disagree on whether a dense PFADD becomes a
`Merge` or a `Put`.

### 2. `WalTarget` — a narrow store-view seam

`execute_wal_action` needs exactly two capabilities from its environment: **write** (set / delete /
merge / clear an entry) and **probe** (`does this key currently exist?`). Today it reaches straight
into `self.store` and `self.persistence.wal_writer()`, which is what welds it to `ShardWorker`.
Extract the two capabilities as a trait and make `execute_wal_action` a free function over it —
mirroring `WriteSink` one layer down:

```rust
/// The store-view + WAL-write surface `execute_wal_action` needs, as a seam.
/// `ShardWorker` is the production adapter (probe → `self.store`, writes →
/// `self.persistence.wal_writer()`); tests supply an in-memory adapter that
/// records writes and answers `contains` from a set — no RocksDB, no worker.
/// (The `WriteSink` precedent, `flush.rs:103`, one layer up.)
pub(crate) trait WalTarget {
    fn contains(&self, key: &[u8]) -> bool;
    async fn write_set(&self, key: &[u8]) -> std::io::Result<()>;
    async fn write_delete(&self, key: &[u8]) -> std::io::Result<()>;
    async fn write_merge(&self, key: &[u8], pairs: &[(u16, u8)]) -> std::io::Result<()>;
    async fn write_clear(&self) -> std::io::Result<()>;
}

/// Resolve one `WalAction` against a target. No `self` — pure over the seam,
/// so the probe-vs-write ordering is unit-testable directly.
async fn execute_wal_action(t: &impl WalTarget, action: &WalAction<'_>) -> std::io::Result<()> {
    match action {
        WalAction::Persist(k)             => t.write_set(k).await,
        WalAction::DeleteIfMissing(k)     => if !t.contains(k) { t.write_delete(k).await } else { Ok(()) },
        WalAction::PersistOrDelete(k)     => if t.contains(k) { t.write_set(k).await } else { t.write_delete(k).await },
        WalAction::PersistIfExists(k)     => if t.contains(k) { t.write_set(k).await } else { Ok(()) },
        WalAction::MergeHllDelta { key, pairs } => t.write_merge(key, pairs).await,
        WalAction::ClearShard             => t.write_clear().await,
    }
}
```

The production `WalTarget for ShardWorker` keeps the existing metadata-lookup behavior
(`persist_key_to_wal`'s `get_metadata`/`get_hot`, the `WalMergeOperands` metric). The test adapter
answers `contains` from a `HashSet` and records the write calls in order, so the probe-relative
ordering the integration suite guards today becomes a three-line unit assertion.

### 3. Fold in: converge index-based `WalStrategy` variants onto `keys_with_flags`

`WalStrategy` (`command.rs:227-511`) still carries positional destination variants —
`PersistDestination(idx)` and `PersistOrDeleteDestination(idx)` — that hard-code arg indices
(`SINTERSTORE` dest 0, `COPY` dest 1, `BITOP` dest 1, …). Those indices duplicate key-layout
knowledge the router already owns in `KeySpec`/`keys_with_flags`, with **no compile-time tie** to
the handler body: change a command's arg order and the WAL index silently points at the wrong slot.
The `Dynamic` variant already shows the fix — it resolves against `keys_with_flags` and filters to
write-access keys (`command.rs:581-589`, via `wal_actions_with_delta`).

Migration sizing (grepped):

| Variant | Spec sites | Notes |
|---------|-----------:|-------|
| `PersistDestination(idx)` | 10 | geo, stream consumer-groups, generic COPY, sorted-set set-ops (×3) / store-remove, set (×3) |
| `PersistOrDeleteDestination(idx)` | 1 | `BITOP` (delete-on-empty dest) |
| `MoveKeys` (fixed 0/1) | 4 | RPOPLPUSH/LMOVE + blocking variants |
| `RenameKeys` (fixed 0/1) | 3 | RENAME/RENAMENX + rollback replay |
| `Dynamic` (already `keys_with_flags`) | 8 | SORT…STORE, GEORADIUS…STORE |

The 11 index-based positional sites are the migration target: a command declares which keys it
writes *once*, via the `W`/`OW`/`RW` flags on its `KeySpec`, and the WAL derives destinations from
that same extraction the router uses. `PersistDestination` becomes "persist-if-exists over the
write-access keys"; `PersistOrDeleteDestination` keeps its delete-on-empty semantics but locates the
key through `keys_with_flags` instead of an integer. `MoveKeys`/`RenameKeys` stay as named
two-key variants (their semantics — persist-or-delete source, persist dest — are not merely "write
these keys" and are worth keeping explicit), so this is a targeted convergence, not a flattening of
every variant onto `Dynamic`.

## Why this is the right depth

- **Locality.** The whole persist decision — action→sink map, store probes, the one durability axis
  — becomes one module with one entry point (`persist`) over one seam (`WalTarget`). The change
  surface for "add a WAL action" was already one match (`execute_wal_action`); the change surface
  for "how do we confirm durability" collapses from three functions in two files to one enum arm.
- **Leverage.** `WalTarget`'s in-memory adapter makes every probe-ordering property a fast unit test
  — the exact leverage `WriteSink`'s `TestSink` already buys the flush engine (18 tests, no
  RocksDB). The convergence in move 3 makes the WAL destination a *derivation* of the declarative
  `KeySpec` (proposal 01) rather than a parallel integer, so a command declares its written keys
  once and the router and the WAL cannot disagree.
- **Deletion test.** The migration deletes whole artifacts: two persist functions merge into one
  (net −2 functions), and the `PersistDestination`/`PersistOrDeleteDestination` integer variants
  can be removed from the `WalStrategy` enum and its resolution match once their 11 sites move to
  flag-derived extraction. What's deleted is duplicated control flow and re-encoded key layout — the
  signature of a seam placed at the right height.
- **Deepens, does not fork.** Durability-as-data follows proposal 03's `WalPhase`/`EffectScope`
  precedent; `WalTarget` follows proposal 29's `WriteSink` adapter convention (`flush.rs:103`); the
  key-extraction convergence rides proposal 01's `KeySpec`/`keys_with_flags`, the same extraction the
  router already trusts. No second durability path, no second key-layout home, no new sink.

## Testing impact

- **Bridge unit tests** (new, in `persistence.rs` behind the `WalTarget` adapter): `PersistOrDelete`
  writes a `set` when the probe says present and a `delete` when absent; `DeleteIfMissing` no-ops on
  a surviving key and writes a `delete` on a gone key; `PersistIfExists` no-ops when absent;
  `MergeHllDelta` routes to `write_merge`; `ClearShard` routes to `write_clear`; a `Confirm` persist
  propagates an injected `flush_through` error while `FireAndForget` swallows and logs it. None of
  these exist today.
- **Durability-axis pin**: one test asserts `persist(records, Confirm)` snapshots the sequence
  *before* the first write and confirms *once* after the last (the transaction-batch invariant that
  `persist_transaction_to_wal` encodes implicitly), and that `FireAndForget` never calls
  `flush_through`.
- **Convergence regression**: for each migrated command a unit test asserts the flag-derived
  destination equals the old index-derived one (e.g. `BITOP` still emits `PersistOrDelete` over dest;
  `COPY` still `PersistIfExists` over dest 1). This is where the integration suite's implicit
  coverage becomes explicit and cheap.
- **Existing suites**: `integration_persistence.rs` (1449 lines) stays as the end-to-end backstop —
  restart-recovery, rollback pre-image restoration, delete-on-empty survival — but is no longer the
  *only* place these properties are checked. The `wal/tests.rs` sink tests are untouched (below the
  refactor line).

## Risks / open questions

- **`WalTarget` async-trait ergonomics.** Four async methods on a `pub(crate)` trait; if native
  async-trait dispatch is awkward for the in-memory adapter, the fallback is `-> impl Future` on the
  production impl and a small hand-rolled future in the test adapter (the `WriteSink` seam sidesteps
  this by being sync — its staging is not `async`). Worth prototyping the adapter first.
- **Metadata lookup stays on the production adapter.** `write_set` must preserve
  `persist_key_to_wal`'s `get_metadata`/`get_hot` framing and the `MergeHllDelta` fallback comment
  about the unreachable-in-practice miss; the seam must expose *set-this-key*, not *set key=value*,
  so the adapter — not the free function — owns the store read. Getting this boundary wrong would
  move store-read policy into the pure function.
- **`MoveKeys`/`RenameKeys` deliberately not converged.** Their semantics are two-key and
  order-sensitive (persist-or-delete source, then persist dest), not "write my write-access keys", so
  flattening them onto `keys_with_flags` would lose the ordering guarantee. Left as named variants;
  revisit only if a third two-key shape appears.
- **Scope creep vs. proposal 03.** The persist bridge sits directly under `run_write_effects`'
  `WalPersistence` step. Move 1 must not re-open the effect-ordering question proposal 03 settled —
  it changes *how* the persist step runs, never *when* it runs relative to the other effects. The
  `WRITE_EFFECT_ORDER` pin (`post_execution.rs` tests) stays green untouched.

## Implementation notes (2026-07-16)

Landed as designed, reconciled with the branch's post-proposal shape (proposal 55's
`ctx.effects`, proposal 59's metadata deletion — neither touches this bridge's callers).

**Move 1 — one persist function.** `ShardWorker::persist(&[WriteRecord<'_>], Durability)` lives
in `shard/persistence.rs` and absorbs all three former loops. `Durability::Confirm` snapshots
`wal.sequence()` before the first write and `flush_through`s it once after the last (propagating
failure via `?`); `Durability::FireAndForget` stages each action and logs on error, never calling
`flush_through`. Callers: the effect path (`post_execution.rs` `WalPersistence` step) passes the
whole `summary.writes` slice as `FireAndForget` (equivalent to the old per-record
`persist_by_strategy` loop, no flush between records); the two rollback sites in `execution.rs`
pass `Confirm` (single record via `slice::from_ref`, transaction batch via the whole slice).

**Move 2 — `WalTarget` seam.** `execute_wal_action` is now a free `async fn` over
`impl WalTarget` (probe `contains` + `write_set`/`write_delete`/`write_merge`/`write_clear`),
so the `store.contains()`-relative probe ordering is unit-testable directly. `ShardWorker` is the
production adapter (the former `persist_key_to_wal`/`persist_delete_to_wal`/`merge_hll_delta_to_wal`/
`clear_shard_to_wal` helper bodies folded into it verbatim — `write_set`/`write_merge` still own the
`get_hot`/`get_metadata` read and the `WalMergeOperands` metric, keeping the free function pure over
*set-this-key*). An in-memory `TestTarget` (a `HashSet` for `contains`, a `RefCell<Vec<Write>>`
recorder, an optional failure flag) backs 8 new `#[tokio::test]`s: one per action variant plus the
probe-branch cases (`PersistOrDelete` present→set / absent→delete, `DeleteIfMissing` survived→no-op /
gone→delete, `PersistIfExists` present→set / absent→no-op) and a failure-propagation test (the
error a `Confirm` persist surfaces via `?`). Native `async fn` in traits was used (edition 2024);
`execute_wal_action` takes `impl WalTarget` (static dispatch), so no `dyn` concerns.

**Move 3 — flag-derived WAL destinations.** The 10 `PersistDestination(idx)` spec sites became the
unit `WalStrategy::PersistDestination` (persist-if-exists over `write_access_keys` —
`keys_with_flags` filtered to `W`/`OW`/`RW`); BITOP's `PersistOrDeleteDestination(1)` converged onto
`Dynamic` (persist-or-delete over the same extraction), since `Dynamic` already emits
persist-or-delete over write keys. Both index variants were deleted from the enum and
`actions_with_delta` (which now routes `Dynamic | PersistDestination | NoOp` to the "resolved via
`Command::wal_actions`" empty arm). The shared filter is a new `write_access_keys` default method on
`Command` (returns a concrete `SmallVec`, **not** `impl Iterator`, to keep the trait dyn-compatible —
it is used as `dyn Command`).

The one non-obvious dependency the proposal glossed: most migrated store commands declared
`AccessSpec::Uniform`, which marks **every** key write — so deriving the WAL destination from
`keys_with_flags` would have emitted spurious actions for the read-only source keys. Preserving
byte-for-byte WAL equivalence therefore required fixing nine commands' access flags to
`Positional([OW, R])` (SINTERSTORE/SUNIONSTORE/SDIFFSTORE, ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE,
ZRANGESTORE, GEOSEARCHSTORE, BITOP). This is strictly more correct — `COMMAND GETKEYSANDFLAGS` now
reports sources as `R` (Redis parity), verified by the introspection regression suite. COPY already
had `Positional([R, OW])`; XGROUP keeps `Uniform` (its single `Index(1)` key *is* the write
destination). A new `SpecError::WalDestinationWithoutWriteKey` invariant in `CommandSpec::validate`
rejects a `Dynamic`/`PersistDestination` strategy paired with a `Positional` list that names no write
flag (silent-no-op guard); `Uniform`/`Dynamic` access derive the flag from `CommandFlags::WRITE` and
cannot trip it.

**Deviation from the sketch.** The proposal's `Durability` doc and testing section imagined the
`Confirm`-vs-`FireAndForget` *flush* behavior as new unit tests. Kept `persist` on `ShardWorker`
(reading the concrete `RocksWalWriter` for `sequence`/`flush_through`) exactly as the sketch's code
block shows, rather than threading flush confirmation through the seam — so the flush-confirm axis
stays covered by `integration_persistence.rs` (rollback pre-image, ack-doesn't-outrun-flush) and
`wal/tests.rs`, while the new unit tests cover the genuinely-interesting layer the seam exposes
(action resolution + store-probe ordering + write-error propagation). Threading `sequence`/`flush`
through `WalTarget` too would let the durability axis be unit-tested without a `ShardWorker`, but it
widens the seam past the two capabilities the proposal scoped it to; left as a possible follow-up.

**Follow-up landed.** Flush-confirm is now threaded through the seam: `WalTarget` gained
`wal_sequence()` (`None` = no WAL configured, replacing the concrete `wal_writer()` early-return) and
`flush_through(after_seq)`, the persist body moved to a free `persist_records(&impl WalTarget, …)`
(with `ShardWorker::persist` a thin delegator), and the durability axis is unit-tested against
`TestTarget` (sequence counter + flush-failure injection): Confirm's before-first-write snapshot and
single flush, FireAndForget's log-and-continue/never-flush, flush- and write-error propagation, and
the no-WAL short-circuit.
