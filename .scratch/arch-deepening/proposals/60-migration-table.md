# Proposal 60 — `MigrationTable`: the slot-handoff lifecycle behind one owning module

Round 38 · lane: replication+cluster · covers exploration-lane candidate **RC8** · effort **M–L**
(two tiers, see [Effort](#effort)) · **LOCKED** area (`frogdb-cluster`, mutation gate 0.80)

Verified against the current tree at `07be27a0` (lane HEAD `08c143d6` plus sibling proposal
commits). Every path, line number and test name below was read in this tree, not inherited from
the candidate brief. Where the brief and the tree disagree, the tree wins and the disagreement is
called out.

## Summary

The two-phase slot handoff — begin, prepare, drain, confirm, complete/abort, with a barrier
window, a record lease, and a replicated generation counter — is FrogDB's most safety-critical
metadata transition. It exists to close the acknowledged-then-orphaned write of FM-CLUSTER-037,
and eight failure-mode rows (084–089, 100, plus 087's release obligation) are written against it.

Its **interface** today is two `pub` fields on a struct:

```rust
// state.rs:115, :126
pub migrations: BTreeMap<u16, SlotMigration>,
pub handoff_seq: u64,
```

Its **implementation** — the protocol — is distributed across nine arms of `apply_command` plus
two free helpers in `commands.rs`, each of which reaches into that pair directly and each of which
must independently remember the rules. There is no **module** whose job is the lifecycle. The
`release_events` funnel (`commands.rs:18-33`) is the one piece that was pulled out, and it is
already half-bypassed: `CompleteSlotMigration` removes a record carrying a prepared handoff and
constructs the release event by hand (`:755-759`) instead of routing through the helper — which is
exactly what FM-CLUSTER-087's Invariant cell says never happens.

The proposal is to add `cluster/src/migrations.rs` containing a `MigrationTable` **module** that
owns the map and the generation counter *together*, and to make its named lifecycle methods the
only way `commands.rs` mutates either. The nine arms keep their validation, their logging and
their events; what they lose is direct access to the representation.

**This proposal claims no live bug.** Two verified defects are documented, both latent and both
independently landable without the refactor:

1. **Spec-prose drift (LOCKED spec).** FM-CLUSTER-087's Invariant cell asserts *"every removing arm
   routes through it"* about `release_events`; `CompleteSlotMigration` does not. The emitted bytes
   are identical today, so nothing is broken — but the sentence a reviewer would rely on is false,
   and `scripts/failure-modes.py` never parses Invariant prose, so nothing catches it.
2. **Citation gap (LOCKED spec).** FM-CLUSTER-087's Trigger cell names `CompleteSlotMigration` as
   one of the five removing arms, but its `Forced by` list contains no `Complete` test. The
   assertion exists (`prepare_then_drain_then_complete_moves_ownership`, `commands.rs:1428`,
   asserting the `[Completed, Released]` pair at `:1446-1460`) — it is simply credited to
   FM-CLUSTER-084 only. Tag and row both need one line.

**Two lane claims are refuted.** FM-CLUSTER-097 is **not** in this proposal's scope, and
FM-CLUSTER-090/091 only marginally are — see
[Spec rows actually touched](#spec-rows-actually-touched-lane-brief-corrected).

## Files involved

Package names are `frogdb-cluster` / `frogdb-cluster-runtime` / `frogdb-server`; the tree lays them
out under `frogdb-server/crates/`. Line counts are `wc -l` at `07be27a0`.

| path | lines | what 60 touches |
|---|---:|---|
| `frogdb-server/crates/cluster/src/commands.rs` | 2101 | **the change.** `release_events` `:10-33`; `prune_migrations_naming` `:35-56`; `apply_command` `:94-111`; `apply_to` `:126-864`. Arms: `RemoveNode` `:215-236` (prune at `:230`), `Failover` `:393-499` (prune at `:450`), `BeginSlotMigration` `:527-563`, `PrepareSlotHandoff` `:565-625`, `ConfirmSlotHandoffDrained` `:627-648`, `AbortSlotHandoff` `:650-676`, `CompleteSlotMigration` `:678-762`, `CancelSlotMigration` `:764-772`, `ResetCluster` `:816-862` (migration clear at `:823-830`). Tests `:868-2101` |
| `frogdb-server/crates/cluster/src/migrations.rs` | — | **new.** `MigrationTable` |
| `frogdb-server/crates/cluster/src/state.rs` | 4402 | `ClusterStateInner` `:107-135` (`migrations` `:115`, `handoff_seq` `:116-126`); `from_snapshot` `:145-172` (construction `:146-159`); `is_slot_migrating` `:319-322`; `get_slot_migration` `:324-327`; `arm_handoff_for_test` `:344-377`; `to_snapshot` `:433-447`; `mint_config_epoch` `:458-469` — **the shape precedent, unchanged** |
| `frogdb-server/crates/cluster/src/types.rs` | 1313 | read-mostly. `ClusterError::HandoffNotReady` `:629-637`; `is_retryable` `:640-645`; constants `:647-685`; `SlotHandoff` `:687-710` + impl `:712-739`; `SlotMigration` `:741-777` + impl `:779-798`; `ClusterSnapshot.migrations` `:810-811` and `.handoff_seq` `:812-822` — **DTO, deliberately unchanged**. Test `only_a_not_ready_handoff_is_retryable` `:923` (tag `:921`) |
| `frogdb-server/crates/cluster/src/invariants.rs` | 1161 | read-only in tier 1. Catalog `INV-HANDOFF-1` `:232-237`, `INV-HANDOFF-2` `:238-243`, `INV-MIG-1` `:244-249`; checks `check_handoff_1` `:536-552`, `check_handoff_2` `:567-590`, `check_mig_1` `:604-621`, `check_ref_2` `:348` (via `:350`), `check_slot_1` `:633-650` (via `:643`). Test fixtures inserting into the map: `:728`, `:932`, `:945`, `:1043`, `:1062`, `:1102` |
| `frogdb-server/crates/cluster/src/encoding_golden.rs` | 359 | **the wire guard.** Module doc `:1-29`; `state_fixture` `:268-326` (map built at `:283-302`, `handoff_seq: 5` at `:322`); fixtures in `testdata/encoding/` incl. `state-inner.json` and the four handoff command files |
| `frogdb-server/crates/cluster/src/properties.rs` | 1528 | read-only. `migration_ref` `:381-390`, `:407`, `:491`, `:602`, `handoff_ref` `:710-761`, `known_defect` `:770-779`, `live_handoffs` `:888-890`, `:1031`, `:1346` |
| `frogdb-server/crates/cluster/src/model/mod.rs` | 812 | read-only. `:428`, `:438`, `:454-456`, `:665-671` (`handoff_seqs_never_reused`), `:706` |
| `frogdb-server/crates/cluster/src/wire.rs` | 415 | test fixtures `:266-267` |
| `frogdb-server/crates/cluster/src/lib.rs` | 95 | module list `:46-61`; `pub use types::{…}` `:76-82` |
| `frogdb-server/crates/cluster-runtime/src/handoff_barrier.rs` | 465 | **not touched.** The counter-example the lane names as already deep. Owns FM-CLUSTER-089's `handoff_now_ms_reads_the_clock_seam` `:456` and all four FM-CLUSTER-090 tests (`:307`, `:343`, `:365`, `:396`) and two of FM-CLUSTER-091's (`:421`, `:444`) |
| `frogdb-server/crates/server/src/slot_migration/mod.rs` | 338 | **not touched — proposal 62's file.** `complete` `:208-276`, `poll_handoff` `:278-303`, `commit` `:309-336`. The `is_retryable` → `TRYAGAIN`/`ERR` render at `:324-329` is why `HandoffNotReady`'s payload text is client-visible |
| `frogdb-server/crates/server/tests/cluster_handoff_barrier.rs` | — | `a_source_that_cannot_drain_aborts_the_finalization` `:299` (tag `:288`, `FM-CLUSTER-091, FM-CLUSTER-087`); `the_barrier_holds_the_replica_feed_until_the_handoff_releases_it` `:635` (tag `:614`) |
| `.scratch/hardening/specs/cluster-failure-modes.md` | — | FM-CLUSTER-084 `:1199-1211`, -085 `:1212-1223`, -086 `:1224-1236`, -087 `:1237-1247`, -088 `:1249-1261`, -089 `:1262-1273`, -090 `:1274-1286`, -091 `:1287-1299`, -097 `:1374-1389`, -100 `:1441-1452` |

### Spec rows actually touched (lane brief, corrected)

The lane names `FM-CLUSTER-089/090/091/097`. Enumerating each row's `Forced by` cell and locating
every named test:

| row | forcing tests | where they live | in scope for 60? |
|---|---|---|---|
| **087** | `abort_releases_the_barrier_and_keeps_the_migration` `commands.rs:1578`, `cancel_releases_a_prepared_handoff` `:1610`, `cancel_without_a_handoff_emits_nothing` `:1631`, `force_failover_releases_the_handoffs_it_prunes` `:1643`, `reset_releases_prepared_handoffs` `:1675`, `a_source_that_cannot_drain_aborts_the_finalization` (`server/tests/`) | **5 of 6 in `commands.rs`** | **yes — the core row** |
| **084** | 4 tests, `commands.rs:1402/:1415/:1428/:1470` | `commands.rs` | **yes** |
| **085** | 4 tests, `commands.rs:1494/:1516/:1741` + `types.rs` | `commands.rs`, `types.rs` | **yes** |
| **086** | `commands.rs:1550/:1697` + `handoff_seq` claims | `commands.rs` | **yes** |
| **088** | `concurrent_handoffs_on_two_slots_do_not_interfere` `commands.rs:1761`, `handoff_model_full_cross_slot` (`model/tests.rs:42`) | `commands.rs`, `model/` | **yes** |
| **089** | `handoff_deadlines_are_pure_functions_of_replicated_data` `commands.rs:1801`; `handoff_now_ms_reads_the_clock_seam` `cluster-runtime/src/handoff_barrier.rs:456` | split 1/1 | **half** — the `commands.rs` half only |
| **100** | 3 tests `state.rs:4095/:4117/:4135` + 3 model tests | `state.rs`, `model/tests.rs` | **yes** (the counter moves into the table) |
| **090** | `only_the_source_node_acts_on_a_handoff` `:307`, `drain_targets_slot_modulo_num_shards_and_survives_zero` `:343`, `a_prepare_arms_the_barrier_drains_the_shard_and_confirms` `:365`, `a_release_lifts_the_barrier_its_prepare_armed` `:396` | **all four in `cluster-runtime/src/handoff_barrier.rs`** | **no** — 60 does not open that file |
| **091** | `a_shard_that_never_answers_is_never_confirmed` `:421`, `a_missing_shard_fails_the_drain` `:444` (both `handoff_barrier.rs`), `a_source_that_cannot_drain_aborts_the_finalization` (`server/tests/cluster_handoff_barrier.rs:299`), `only_a_not_ready_handoff_is_retryable` (`types.rs:923`) | 3 outside, 1 in `types.rs` | **marginal** — only the `types.rs` test, and only if `is_retryable` changes, which it must not |
| **097** | 11 tests: 4 in `core/src/client_registry/mod.rs` (`:1504/:1521/:1535/:1551`), 6 in `replication/src/feed_gate.rs` (`:133/:143/:224/…`), 1 in `server/tests/cluster_handoff_barrier.rs:635` | **zero in `frogdb-cluster` or `frogdb-cluster-runtime`** | **no — refuted** |

**FM-CLUSTER-097 is out of scope entirely.** It landed with the ReplicaFeedGate work (issue 12,
`7bc8520e`) and its whole mechanism — `PauseState::feed_hold_until`,
`ClientRegistry::publish_pause_derived_state`, `ReplicaFeedGate`, `ReplicaSession::start_streaming` —
lives in `frogdb-core` and `frogdb-replication`. The Raft state machine never learns the feed is
held. The lane citing it was reasonable at a glance (the row's title says "slot-handoff barrier")
and wrong on inspection. Nothing in this proposal can affect it, and the re-gate plan below does
not include those crates.

The rows this proposal is really written against are **084, 085, 086, 087, 088, 100**, with **089
half-in**. That set is *larger* and *closer to the code* than the lane's — which strengthens the
"most safety-critical" claim while narrowing the crate set to one.

## Problem (verified evidence)

### 1. The lifecycle's interface is a `BTreeMap`, and its API is nine `match` arms

Every mutation of the handoff protocol, in file order:

| site | arm / helper | what it does to the representation |
|---|---|---|
| `commands.rs:53` | `prune_migrations_naming` | `migrations.remove(&slot)` × N, then `release_events` |
| `:230` | `RemoveNode` | calls the prune helper |
| `:450` | `Failover { force: true }` | calls the prune helper |
| `:533` | `BeginSlotMigration` | `migrations.get(&slot)` — idempotence + conflict check |
| `:558-560` | `BeginSlotMigration` | `migrations.insert(slot, SlotMigration::new(..))` |
| `:573` | `PrepareSlotHandoff` | `migrations.get(&slot)` — existence |
| `:593-594` | `PrepareSlotHandoff` | `handoff_seq += 1`; read back as `seq` |
| `:595-605` | `PrepareSlotHandoff` | `migrations.get_mut(&slot)` + `.expect(..)`, then `migration.handoff = Some(..)` |
| `:633-645` | `ConfirmSlotHandoffDrained` | `get_mut` → `handoff.as_mut()` → `filter(seq)` → `drained = true` |
| `:654-662` | `AbortSlotHandoff` | `get_mut` → `filter(seq)` → `handoff = None`, release event hand-built at `:667-671` |
| `:684-691` | `CompleteSlotMigration` | `migrations.get(&slot)` — existence |
| `:717-735` | `CompleteSlotMigration` | reads `migration.handoff`, branches on `admits_complete_at` |
| `:739` | `CompleteSlotMigration` | `migrations.remove(&slot)`, release event hand-built at `:755-759` |
| `:765-769` | `CancelSlotMigration` | `migrations.remove(&slot).map(release_events)` |
| `:826-830` | `ResetCluster` | `mem::take(&mut migrations)` → `release_events`; `handoff_seq = 0` |

Fifteen direct field touches across nine arms and two helpers, plus five more in `state.rs`
(`:115`, `:150`, `:321`, `:326`, `:443`). Twenty production sites reaching into a representation
whose rules are written in Markdown.

The *shape* of that list is the problem. `PrepareSlotHandoff` performs a `get`, drops the borrow,
bumps a counter on a sibling field, re-acquires the same entry with `get_mut` and an `.expect`
whose justification is a comment (`"migration presence checked above"`, `:598`). That double
lookup and that `expect` exist only because the arm is manipulating two fields of a struct it does
not own; a method on a type that owns both writes it once, infallibly.

### 2. The release funnel is documented as total and is not

`release_events` (`:10-33`) carries the strongest doc comment in the file:

> Every arm that drops a migration record funnels through here so the invariant holds
> unconditionally: **a prepared handoff never disappears without a
> [`ClusterEvent::SlotHandoffReleased`]**.

FM-CLUSTER-087's Invariant cell repeats it as spec:

> One helper, `release_events(migration)`, renders a removed record into its release events, and
> **every removing arm routes through it** (`cluster/src/commands.rs`).

Three arms do (`:53`, `:768`, `:828`). `CompleteSlotMigration` does not: it captures `seq` at
`:718`, removes the record at `:739`, and constructs `SlotHandoffReleased` inline at `:755-759`.
`AbortSlotHandoff` also builds its own (`:667-671`), though that one is defensible — it clears
`handoff` without removing the record, so it is not "a removed record" at all and the helper's
signature does not fit.

`Complete`'s is not defensible; it is simply a second implementation. It is *correct* today, and
provably so: `source_node` was validated equal to the record's at `:693`, the handoff is `Some`
with that `seq` by `:717-735`, so `release_events(removed)` yields byte-identical output. But
"correct because I re-derived the same three fields" is the failure mode the funnel was introduced
to end. The spec sentence a reviewer trusts is false, and the failure-modes lint cannot see it:
`scripts/failure-modes.py` binds only the backticked names in `Forced by` to `// FM-` tags and
never parses Invariant prose (`scripts/failure-modes.py:7-15`, `:206-240`).

### 3. The generation counter is minted inline, three hundred lines from the counter that isn't

`handoff_seq` is a replicated fencing generation. FM-CLUSTER-100 exists because losing it lets a
restored node re-mint a spent `seq`, which makes `SlotFence` compare equal and admit a command it
should refuse. INV-HANDOFF-1 (`invariants.rs:232-235`) is the hard invariant that guards it.

Its mutation surface is two lines in one arm (`commands.rs:593-594`) and one line in another
(`:830`). Nothing names the operation. Meanwhile the *other* replicated counter on the same struct
has exactly the method this one lacks:

```rust
// state.rs:466-469 (doc :458-465)
fn mint_config_epoch(&mut self) -> ConfigEpoch {
    self.config_epoch = self.config_epoch.max(self.max_node_epoch()) + 1;
    self.config_epoch
}
```

named, documented, with its invariant argued in the doc comment. (The two mints are *deliberately*
different — the config epoch ratchets past every node's claim, the handoff generation is a plain
successor — so this is a shape precedent, not an unapplied one. The point is that one counter has
a name and the other is `+= 1`.)

### 4. The parameter-match preamble is written twice, verbatim

`PrepareSlotHandoff` `:577-581` and `CompleteSlotMigration` `:693-697`:

```rust
if migration.source_node != source_node || migration.target_node != target_node {
    return Err(ClusterError::InvalidOperation(
        "migration parameters don't match".to_string(),
    ));
}
```

Character-identical, including the error string, which is client-visible through
`slot_migration/mod.rs:324-329`.

### 5. `HandoffNotReady` reasons are stringly-typed at five sites, and they reach the client

`ClusterError::HandoffNotReady(u16, String)` is constructed at `commands.rs:574`, `:587-590`,
`:638-643`, `:727`, `:730-733`. `SlotMigrationCoordinator::commit` renders it as
`format!("{} {}", prefix, msg)` with `prefix = "TRYAGAIN"` (`slot_migration/mod.rs:324-329`), so
every one of those strings is protocol surface. Today nothing enumerates them and nothing pins
them; the closest thing is `only_a_not_ready_handoff_is_retryable` (`types.rs:923`), which pins the
*prefix* decision, not the bodies. This is a **scope boundary, not a change**: proposal 60 must
move these constructions without altering a byte, and proposal 62 is the one that owns the
`TRYAGAIN` rendering.

### Why this is shallow, in architecture vocabulary

- **Depth.** The module's interface should be small relative to the behavior behind it. Here the
  interface is *the representation itself* — a map and an integer — so the ratio is inverted: nine
  callers each carry a slice of the protocol. `SlotHandoff` (`types.rs:712-739`) is the
  counter-example already in this file: five one-line predicates whose doc comments explain why the
  arithmetic is centralised ("so there is one place for the arithmetic to be wrong",
  FM-CLUSTER-089's Invariant cell). The *record* got that treatment; the *table* did not.
- **Locality.** Deciding whether a `Complete` may proceed requires reading `commands.rs:678-762`,
  `types.rs:712-739`, `state.rs:115-126` and FM-CLUSTER-084/085/087. The release obligation is a
  fourth place. A `MigrationTable` module puts the transition, the obligation and the generation
  in one file.
- **Seam.** `apply_to` (`:126`) is already the transition seam — deliberately split out so the
  invariant hook has one attachment point (`:113-125`). What it lacks below it is a *state* seam:
  a boundary between "the command's validation and logging" and "the lifecycle's representation".
- **Leverage.** One new type is paid for once and is read by nine arms, three invariant checks, a
  property generator, a model checker and six spec rows.
- **Adapter.** `ClusterSnapshot` (`types.rs:801-829`) is already the read-side adapter for
  everything outside the crate — `slot_migration/routing.rs`, `slot_fence.rs`, `guards.rs`,
  `debug_providers.rs`, the debug web UI. That adapter is what makes this change containable.

### The deletion test

*If `MigrationTable` were deleted, what would have to come back?* — Nine arms reaching into two
sibling fields, a double `get`/`get_mut` with an `.expect`, two hand-built release events, an
inline `+= 1` on a fencing generation, and a duplicated parameter check. The type does not wrap
anything for the sake of wrapping: it exists to make one obligation (*a removed prepared handoff
owes a release event*) and one resource (*the generation*) inseparable from the storage they
constrain. It passes.

*Conversely* — would a `MigrationTable` that merely forwarded `get`/`insert`/`remove` pass? No.
That is the failure mode this proposal must avoid, and the acceptance criteria below are written to
prevent it: the type exposes **lifecycle** operations named after the protocol, not map operations.

## Proposed change

### The module

A new `cluster/src/migrations.rs`. The critical design decision is **how it owns the two fields**,
and it is driven by a wire constraint (see [Risks](#risk-1-the-raft-snapshot-is-clusterstateinner-serialized-whole)):
`ClusterStateInner` is serialized whole into Raft snapshots and pinned byte-for-byte by
`testdata/encoding/state-inner.json`. So the fields **stay where they are** and `MigrationTable`
is a *borrowed façade* over both:

```rust
/// The open slot migrations and the handoff generation that fences them.
///
/// Borrowed, not owned: `ClusterStateInner` is serialized whole into Raft
/// snapshots (`encoding_golden`), so the two fields keep their position and
/// their names. What this type owns is the *protocol* — every transition of the
/// two-phase handoff, and the release obligation a removal incurs.
pub(crate) struct MigrationTable<'a> {
    open: &'a mut BTreeMap<u16, SlotMigration>,
    generation: &'a mut u64,
}

impl ClusterStateInner {
    pub(crate) fn migration_table(&mut self) -> MigrationTable<'_> { … }
}
```

Its interface is the protocol, not the map. Sketch, with the arm each method replaces:

| method | replaces | note |
|---|---|---|
| `open(slot, source, target) -> Result<Opened, ClusterError>` | `BeginSlotMigration` `:533-560` | folds the idempotence check, the conflict error and the insert |
| `matching(slot, source, target) -> Result<&SlotMigration, ClusterError>` | the duplicated preamble `:573-581` / `:684-697` | one construction of `"migration parameters don't match"` |
| `prepare(slot, at_ms, barrier_ms, lease_ms) -> Result<u64, ClusterError>` | `:586-605` | mints the generation *and* installs the record in one borrow — no `get`/`get_mut` pair, no `.expect` |
| `confirm_drained(slot, seq) -> Result<(), ClusterError>` | `:633-645` | |
| `abort(slot, seq) -> Vec<ClusterEvent>` | `:654-671` | idempotent, as today |
| `complete(slot, at_ms) -> Result<Completed, ClusterError>` | `:717-739` | returns the removed record; the arm writes `slot_assignment` and emits |
| `cancel(slot) -> Vec<ClusterEvent>` | `:765-769` | |
| `prune_naming(node_id) -> Vec<ClusterEvent>` | `prune_migrations_naming` `:44-56` | moves verbatim |
| `reset() -> Vec<ClusterEvent>` | `:826-830` | rewinds the generation in the same call that clears the map — the pairing FM-CLUSTER-086 and -100 both rely on |
| `release_events(SlotMigration) -> Vec<ClusterEvent>` | `:18-33` | moves verbatim; becomes **private** to the module, which is what makes the funnel total |

The two removal paths that hand-build their events today (`Complete` `:755-759`, `Abort`
`:667-671`) both go through the private helper. That is the whole enforcement mechanism: with
`release_events` private and every removal inside the module, "every removing arm routes through
it" stops being prose and becomes a visibility rule.

### What deliberately does not change

| unit | why |
|---|---|
| `ClusterStateInner`'s field names, types, order, `#[serde(default)]` | the Raft snapshot wire format (`encoding_golden.rs:1-29`) |
| `ClusterSnapshot` (`types.rs:801-829`) and `to_snapshot` (`state.rs:433-447`) | the read-side adapter every consumer outside `frogdb-cluster` uses |
| `SlotHandoff` / `SlotMigration` and their impls (`types.rs:687-798`) | already deep; FM-CLUSTER-089's Invariant cell cites them by name |
| every `ClusterError` payload string | client-visible via `slot_migration/mod.rs:324-329`; proposal 62's territory |
| every `ClusterEvent` emitted, and its order within an arm's `Vec` | asserted verbatim by the FM-084/086/087 tests |
| `commands.rs` validation order, `tracing` calls and their fields | the arms keep everything except the field access |
| `cluster-runtime/src/handoff_barrier.rs` | the lane's own counter-example; FM-090/091's forcing tests live here |
| `server/src/slot_migration/*` | proposal 62 |

### Hard acceptance criteria (LOCKED crate)

1. **`UPDATE_GOLDEN` is never run.** `just test frogdb-cluster encoding_golden` passes untouched.
   If `state-inner.json` needs regenerating, the design is wrong — revert to the borrowed façade.
2. **Zero net change to emitted events.** Every `ClusterEvent` variant, field value and position in
   the returned `Vec` is what it is today, for all nine arms. The FM-084/086/087 tests assert these
   verbatim and must pass **unmodified**.
3. **Zero net change to error text.** Every `ClusterError` payload string is byte-identical.
4. **`release_events` is private to `migrations.rs`** and has no caller outside it. This is the
   criterion that makes the change worth making; without it the proposal is a rename.
5. **`debug_assert_clean` still runs at exactly the three seams it runs at today**
   (`commands.rs:108`, `state.rs:166`, and the `restore_from_snapshot` twin) — the façade must not
   introduce a fourth or drop one.
6. **No new `.expect()`/`.unwrap()` in the moved code.** `commands.rs:598`'s
   `expect("migration presence checked above")` is deleted, not relocated.
7. **Spec edits are documentation-only** (see below) — no `Observable` or `NOT observable` cell
   changes, because no behavior changes.

### Spec impact (spec-first discipline)

This is a pure restructuring: no `Observable` cell moves, so it is **not** a spec-first behavior
change and needs no new failure-mode row. Three documentation edits to
`.scratch/hardening/specs/cluster-failure-modes.md`, all of which make existing prose *true* rather
than changing what is claimed:

- **FM-CLUSTER-087 Invariant** (`:1244`): re-point at `cluster/src/migrations.rs` and state the
  enforcement mechanism (the helper is private; every removal is inside the module) instead of
  asserting a convention. **This edit is only honest once `Complete` actually routes through the
  helper** — which is hotfix A below, landable first and independently.
- **FM-CLUSTER-087 `Forced by`** (`:1246`): add
  `prepare_then_drain_then_complete_moves_ownership`, and add `FM-CLUSTER-087` to that test's tag
  line (`commands.rs:1426`). Required in both directions —
  `scripts/failure-modes.py:7-15` enforces spec→test *and* test→spec. This is hotfix B, also
  independently landable.
- **FM-CLUSTER-086** (`:1231`) and **FM-CLUSTER-100** (`:1448`): both cite
  `ClusterStateInner::handoff_seq` and `cluster/src/{state,commands}.rs` for the mint and the
  rewind. Re-cite the mint/rewind to `migrations.rs`; the field itself stays on
  `ClusterStateInner`, so the serde half of FM-100's cell is untouched.

### Re-gate plan

- Push discipline: `just mutants-diff frogdb-cluster`.
- Full gate before merge: `just mutants frogdb-cluster` + `just mutants-gate frogdb-cluster 0.80`.
- **`frogdb-cluster-runtime` is not re-gated by this proposal** — no file in it changes. Sequenced
  after proposal 59 (also `frogdb-cluster`), one full gate run covers both; see
  [Boundaries](#solo-last-verdict).
- New mutants land in `migrations.rs`, and its own tests must kill them. Per CLAUDE.md,
  `cargo mutants -p frogdb-cluster` runs only that package's tests — every method above is reachable
  from `commands.rs`'s in-crate test module, so no forcing test needs to move.
- `just lint-failure-modes` after the spec edits (`uv run --offline`).
- `just lint-gates` — the clock seam is the one that matters here, and the proposal adds no clock
  read: `MigrationTable::prepare` takes `at_ms` as a parameter, exactly as `apply_command` does
  today (FM-CLUSTER-089).

## Testability improvement

Today a test of the release obligation must go through `apply_command` with a fully built
`ClusterStateInner` — a node table, a slot assignment, a migration, a prepare and a confirm. The
five FM-087 tests all do (`commands.rs:1578-1695`), each re-deriving the preamble; `arm_handoff_for_test`
(`state.rs:344-377`) exists precisely to stop that duplication from getting worse.

After the change:

1. **The obligation is unit-testable in isolation.** `MigrationTable::{cancel, complete, abort,
   prune_naming, reset}` take a borrowed map and integer. A table test — *for each removal method,
   with and without a prepared handoff, assert the release events* — is a `#[test]` with no
   `ClusterState`, no node table and no Raft. That is a genuinely new test *shape*: today the
   obligation can only be witnessed one arm at a time, which is why the row has five separate
   tests and still misses `Complete`.
2. **Exhaustiveness becomes checkable.** With every removal inside one `impl`, "did I cover every
   removing path?" is answerable by reading one file, and a new one that forgot the helper cannot
   compile against a private function it did not call.
3. **New mutants land where tests can kill them.** `mutants` on `frogdb-cluster` currently has to
   kill mutations scattered through a 738-line `match`; concentrating the arithmetic and the
   removals raises the density of *observable* mutants, which is what the 0.80 gate measures.
4. **The property harness gets a cheaper oracle.** `properties.rs:888-890`'s `live_handoffs` and
   `model/mod.rs:665-671`'s `handoff_seqs_never_reused` both re-derive facts the table could
   answer directly. Not required by this proposal, but it becomes possible.
5. **Hotfix B closes a real citation gap** independently of all of the above.

## Risks / scope boundaries vs sibling proposals

### Risk 1 — the Raft snapshot *is* `ClusterStateInner` serialized whole

This is the dominant risk and the reason for the borrowed-façade design.
`encoding_golden.rs:1-29` is explicit: a Raft snapshot is `ClusterStateInner` serialized as
`serde_json`, so *field names and variant tags are the wire format*, and a change breaks a node
reading a peer's snapshot during a rolling upgrade — surfacing as a follower that cannot apply
entries, not as a compile error.

Two designs were considered and rejected:

- **Owning newtype over the map** (`struct MigrationTable(BTreeMap<…>)` as the field type). Needs
  `#[serde(transparent)]` to stay wire-identical; that works, but it leaves `handoff_seq` outside
  the type, which forfeits the FM-100/086 pairing that is half the point.
- **Owning struct over both fields, `#[serde(flatten)]`**. Would keep the JSON shape, but `flatten`
  routes deserialization through a buffering intermediate and interacts badly with `#[serde(default)]`
  — and both fields depend on `default` for backward compatibility (`state.rs:125`, `types.rs:821`).
  Not worth the risk for a refactor that claims to change nothing.

The borrowed façade changes no type that serde sees. **Mitigation:** criterion 1 above —
`state-inner.json` must not be regenerated, and the reviewer should treat any diff to
`testdata/encoding/` as a rejection.

### Risk 2 — a façade that is only a rename

If the methods end up as `get`/`insert`/`remove` with new names, the crate gains a type and loses
nothing. **Mitigation:** criterion 4 (`release_events` private with no external caller) is the
falsifiable test of whether the boundary is real, and the method table above is named after
protocol transitions, not map operations.

### Risk 3 — borrow-checker friction in `Failover` and `ResetCluster`

`Failover` mutates `inner.nodes`, `inner.slot_assignment` and `inner.config_epoch` interleaved with
the migration prune (`:417-470`); `ResetCluster` clears three collections (`:820-830`). A `&mut`
façade held across those lines will not borrow-check. **Mitigation:** the façade is acquired,
used, and dropped at the single point where the migration mutation happens — `prune_naming` at
`:450` and `reset` at `:826` are each one statement in today's code and stay one statement. This
is a real constraint on the implementation, not a blocker, but it is the thing most likely to
tempt an implementer into changing statement order — which criterion 2 forbids.

### Boundary vs proposal 62 (handoff finalizer, RC11) — state machine / finalizer

62 moves `SlotMigrationCoordinator::complete`/`poll_handoff` (`slot_migration/mod.rs:208-303`) out
of `frogdb-server` into `frogdb-cluster-runtime` beside `handoff_barrier.rs`, preserving `TRYAGAIN`
strings verbatim. The two proposals sit on opposite sides of Raft:

| unit | owner |
|---|---|
| `commands.rs` arms, `migrations.rs`, `ClusterStateInner`'s two fields | **60** |
| `slot_migration/mod.rs`, `poll_handoff`, `commit`, the `TRYAGAIN`/`ERR` prefix at `:324-329` | **62** |
| `handoff_barrier.rs` (`plan_handoff_action`, `drain_shard`, `handoff_now_ms`) | **neither** |
| `ClusterError::HandoffNotReady` payload strings (`types.rs:637`; constructed at `commands.rs:574/:587/:638/:727/:730`) | **shared, frozen by both** |

The shared row is the live edge and it is one-directional: 60 *moves* the construction sites, 62
*renders* them. Criterion 3 (byte-identical error text) is what keeps 60 from breaking 62, and it
should be restated in 62's own acceptance criteria. There is no file overlap.

They also share spec rows: 62 re-cites FM-CLUSTER-091's Invariant cell (`slot_migration/mod.rs` →
new home), 60 re-cites 086/087/100. Disjoint cells, same file — a trivial merge, but they must not
land in the same commit.

### Boundary vs proposal 59 (cluster event router, RC7) — same crate, same file, disjoint regions

59 targets `state.rs:879-1005` (the `apply` fan-out), `:563-576` and `:1007-1018`. 60 touches
`state.rs` at `:107-135`, `:145-159`, `:319-327`, `:344-377` and `:433-447`. **No region overlap.**
The real coupling is the mutation gate: both change `frogdb-cluster`, so the honest sequencing is
*land 59, land 60, then one `just mutants frogdb-cluster` + `mutants-gate … 0.80` covering both*,
with `mutants-diff` per PR as push discipline. Running the full gate twice is ~2× a testbox-class
workload for no extra information.

### Boundary vs proposal 58 (auto-failover propose-retry, FM-CLUSTER-009/010/011)

58 targets `cluster-runtime/src/failure_detector.rs:461-511`/`:517-659` and
`cluster/src/network.rs:687-741` — the *proposer* of `Failover`. 60 touches the `Failover` **apply
arm** (`commands.rs:393-499`), and only its one call to the prune helper at `:450`. Different
files, different sides of Raft, no shared spec row (58 is 009/010/011; 60 is 084-089/100). 58 is
spec-first and 60 is not, so they should not share a PR, but they can land in either order.

### Boundary vs proposal 61 (primary snapshot hooks, RC9+RC12)

`frogdb-replication` only (`primary/mod.rs`), plus a one-line amendment to ADR-0004's cost
paragraph. No overlap with 60 in files, crates or spec rows.

### Solo-last verdict

**Partially confirmed, for a different reason than the lane gives.**

- *Refuted:* "solo" in the sense of textual conflict. Verified region-by-region, 60 does not
  overlap 58 (different crate files), 59 (same file, disjoint regions), 61 (different crate) or 62
  (different crate). It could be developed in parallel with all four.
- *Confirmed:* "last" as a **gate-ordering** constraint. 60 is the second `frogdb-cluster` change
  in the round (59 is the first) and the one whose value is measured by the 0.80 mutation gate.
  Landing it last means the full gate runs once, over both, and any surviving mutant is attributed
  to the final shape rather than to an intermediate one.
- *Confirmed:* "last" as a **review** constraint, which is the strongest form. 60's entire claim is
  *nothing changed* across six LOCKED failure-mode rows. That claim is only reviewable against a
  stable baseline: if 62 has concurrently moved the `TRYAGAIN` renderer, a reviewer diffing 60
  cannot tell which proposal owns a changed error string. Land 60 after 62 and the frozen-strings
  criterion is checkable by inspection.
- *Refuted:* the lane's "PURE move". It is not a pure move — `PrepareSlotHandoff`'s double lookup
  collapses to one, and `Complete`'s hand-built release event is replaced by the funnel. Both are
  behavior-preserving and both are asserted by existing tests, but "pure move" would let a reviewer
  skip exactly the two places where care is needed.

### Size verdict

**The lane's "L" is right only if the enforcement tier is included.** Two separable tiers:

- **Tier 1 (M)** — `migrations.rs` + the façade + nine arms rewritten + the two fields narrowed
  from `pub` to `pub(crate)` (verified free: nothing outside `frogdb-cluster` reads
  `ClusterStateInner.migrations`; external readers all go through `ClusterSnapshot.migrations`).
  Every read site in `invariants.rs`, `properties.rs`, `model/` and the test fixtures compiles
  **unchanged**, because the fields still exist. Delivers criteria 1–7 and every testability claim
  except #2's compile-time guarantee.
- **Tier 2 (S–M, on top)** — privatize both fields behind read accessors, so the funnel is
  unbypassable within the crate too. Costs the mechanical fan-out the lane priced in: 5 reads in
  `invariants.rs` (`:350`, `:538`, `:569`, `:606`, `:643`), ~10 in `properties.rs`, 4 in
  `model/mod.rs`, 3 in `state.rs`, and ~8 test-fixture `insert`s (`invariants.rs:728/:932/:945/:1043/:1102`,
  `wire.rs:266-267`, `encoding_golden.rs:283-302`) needing a seeding constructor.

Tier 1 is where the safety argument is won; tier 2 is where it is made permanent. Recommend both,
in two commits, with tier 1 reviewable on its own.

## Effort

| step | scope | size |
|---|---|---|
| **Hotfix A** — route `CompleteSlotMigration` through `release_events` | `commands.rs:739-761`: `let removed = inner.migrations.remove(&slot);` then `events.extend(release_events(removed))`. Provably byte-identical (`source_node` validated equal at `:693`; `handoff` is `Some(seq)` by `:717-735`). Amend FM-CLUSTER-087's Invariant cell to stop over-claiming, or leave it — it becomes true either way. **No refactor, no new test, makes a LOCKED spec sentence true.** | **S** — ~4 lines + 1 spec line |
| **Hotfix B** — credit the `Complete` release assertion to FM-CLUSTER-087 | Add `FM-CLUSTER-087` to the tag at `commands.rs:1426` and `prepare_then_drain_then_complete_moves_ownership` to 087's `Forced by` (`:1246`). Both directions required by `scripts/failure-modes.py`. Closes the gap between 087's Trigger cell (which names `Complete`) and its forcing set (which does not). | **S** — 2 lines |
| **1 — the module** | New `cluster/src/migrations.rs`: `MigrationTable<'a>`, the ten methods above, `release_events` moved and made private, `prune_migrations_naming` moved. `lib.rs:46-61` gains `mod migrations;` (private — nothing outside the crate needs it). Unit tests for the removal/obligation table. | **M** — ~230 new, ~half tests |
| **2 — rewire the arms** | Nine arms in `commands.rs` rewritten to acquire the façade at the single mutation point. `commands.rs:593-605`'s double lookup and `.expect` deleted. Existing tests unmodified. | **M** — ~120 changed, net negative in `commands.rs` |
| **3 — narrow visibility (tier 1)** | `state.rs:115`/`:126` `pub` → `pub(crate)`; `migration_table()` accessor. | **S** |
| **4 — spec re-cites** | Three documentation edits above; `just lint-failure-modes`. | **S** |
| **5 — privatize (tier 2, separate commit)** | Read accessors + ~22 mechanical call-site edits + a test seeding constructor. | **S–M** |
| **Re-gate** | `mutants-diff` per PR; one `mutants frogdb-cluster` + `mutants-gate frogdb-cluster 0.80` shared with proposal 59. Testbox-class workload. | — |

Both hotfixes are independently valuable, need no part of the refactor, and are worth landing even
if this proposal is rejected — A because it retires a duplicate implementation of a safety
obligation, B because it makes a LOCKED row's own Trigger cell traceable to a test.
