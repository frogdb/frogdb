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
only way `commands.rs` mutates either — a *compile-time* property only at tier 2, see
[Size verdict](#size-verdict). The nine arms keep their validation, their logging and their
events; what they lose is direct access to the representation.

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
| `frogdb-server/crates/cluster/src/invariants.rs` | 1161 | read-only in tier 1. Catalog `INV-HANDOFF-1` `:232-237`, `INV-HANDOFF-2` `:238-243`, `INV-MIG-1` `:244-249`; checks `check_handoff_1` `:536-552`, `check_handoff_2` `:567-590`, `check_mig_1` `:604-621`, `check_ref_2` `:348` (via `:350`), `check_slot_1` `:633-650` (via `:643`). `debug_assert_clean` `:302-311`, compiled `#[cfg(any(test, debug_assertions))]`. Test fixtures touching the pair: the `clean_state` literal (map `:728-762`, `handoff_seq: 4` `:763`), inserts `:932`, `:945`, `:1043`, `:1102`, a `migrations.get_mut` at `:1062`, and `handoff_seq` writes at `:1035` and `:1131` |
| `frogdb-server/crates/cluster/src/encoding_golden.rs` | 359 | **the wire guard.** Module doc `:1-29`; `state_fixture` `:268-326` (map built at `:283-302`, `handoff_seq: 5` at `:322`); fixtures in `testdata/encoding/` incl. `state-inner.json` and the four handoff command files |
| `frogdb-server/crates/cluster/src/properties.rs` | 1528 | read-only. `migration_ref` `:381-390`, `:407`, `:491`, `:602`, `handoff_ref` `:710-761`, `known_defect` `:770-779`, `live_handoffs` `:888-890`, `:1031`, `:1346` |
| `frogdb-server/crates/cluster/src/model/mod.rs` | 812 | **outside the blast radius, tier 1 *and* tier 2.** Every migration/`handoff_seq` read here is against `ClusterSnapshot`, which this proposal declares unchanged: `observed_prepare` `:427-434` and `observed_drained` `:437-442` take `view: &ClusterSnapshot`; `digest` `:452-456` likewise; `Node.view` is `Arc<ClusterSnapshot>` (`:220`), so `handoff_seqs_never_reused` `:665-671` and `:706` read through the DTO too |
| `frogdb-server/crates/cluster/src/wire.rs` | 415 | **outside the blast radius.** `fixture()` returns `ClusterSnapshot` (`:252`), so `:266-267` seeds the DTO, not `ClusterStateInner` |
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
| **084** | **7 tests**: `complete_is_refused_without_a_prepared_handoff` `commands.rs:1402`, `complete_is_refused_while_the_handoff_is_undrained` `:1415`, `prepare_then_drain_then_complete_moves_ownership` `:1428`, `complete_is_refused_once_the_barrier_window_elapsed` `:1470`, plus the **three model configs** `handoff_model_smoke` / `_full_cross_slot` / `_full_deep` (`model/tests.rs` fns `:34`/`:45`/`:55`, tags `:32`/`:42`/`:52`) | `commands.rs`, `model/` | **yes** |
| **085** | `complete_is_refused_once_the_lease_expired` `commands.rs:1494`, `a_second_prepare_waits_for_the_lease_but_not_forever` `:1516`, `a_late_confirm_cannot_resurrect_an_expired_handoff` `:1741`, `handoff_model_full_deep` (`model/tests.rs:55`, tag `:52`) — **not `types.rs`**, which the earlier draft of this table claimed | `commands.rs`, `model/` | **yes** |
| **086** | `a_stale_drain_ack_cannot_vouch_for_the_next_attempt` `commands.rs:1550`, `prepare_requires_a_migration_and_matching_parameters` `:1697`, **+3 model tests** (`smoke`/`full_cross_slot`/`full_deep`) | `commands.rs`, `model/` | **yes** |
| **088** | `concurrent_handoffs_on_two_slots_do_not_interfere` `commands.rs:1761`, `handoff_model_full_cross_slot` (`model/tests.rs:45`, tag `:42`) | `commands.rs`, `model/` | **yes** |
| **089** | `handoff_deadlines_are_pure_functions_of_replicated_data` `commands.rs:1801`; `handoff_now_ms_reads_the_clock_seam` `cluster-runtime/src/handoff_barrier.rs:456` | split 1/1 | **half** — the `commands.rs` half only |
| **100** | 3 tests `state.rs:4095/:4117/:4135` + 3 model tests | `state.rs`, `model/tests.rs` | **yes** (the counter moves into the table) |
| **090** | `only_the_source_node_acts_on_a_handoff` `:307`, `drain_targets_slot_modulo_num_shards_and_survives_zero` `:343`, `a_prepare_arms_the_barrier_drains_the_shard_and_confirms` `:365`, `a_release_lifts_the_barrier_its_prepare_armed` `:396` | **all four in `cluster-runtime/src/handoff_barrier.rs`** | **no** — 60 does not open that file |
| **091** | `a_shard_that_never_answers_is_never_confirmed` `:421`, `a_missing_shard_fails_the_drain` `:444` (both `handoff_barrier.rs`), `a_source_that_cannot_drain_aborts_the_finalization` (`server/tests/cluster_handoff_barrier.rs:299`), `only_a_not_ready_handoff_is_retryable` (`types.rs:923`, tag `:921` — the only tag in `types.rs` belonging to the 084–091 handoff family, though the file carries 19 `FM-CLUSTER-*` tags overall) | 3 outside, 1 in `types.rs` | **marginal** — only the `types.rs` test, and only if `is_retryable` changes, which it must not |
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

Three arms do — the helper has exactly three callers, all in this file (`:54`, `:768`, `:828`).
`CompleteSlotMigration` does not: it captures `seq` at
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

- **Depth.** Depth is *leverage*: how much behavior one call stands in front of. It is not a
  line ratio, and the point here is not that the interface is large — it is that there is no call
  to stand in front of anything. The interface is *the representation itself*, a map and an
  integer, so every one of nine callers carries its own slice of the protocol and the leverage of
  the boundary is zero. `SlotHandoff` (`types.rs:712-739`) is the
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
- **Leverage.** One new type is paid for once and is read by nine arms, **five** invariant checks
  (`check_ref_2` `:350`, `check_handoff_1` `:538`, `check_handoff_2` `:569`, `check_mig_1` `:606`,
  `check_slot_1` `:643`), a property generator and six spec rows. The model checker is *not* on
  that list: it reads `ClusterSnapshot` throughout (`model/mod.rs:220`, `:427`, `:437`, `:452`),
  which this proposal leaves untouched.
- **Read-side interface.** `ClusterSnapshot` (`types.rs:801-829`) is the crate's read-side
  *interface* — the published shape every consumer outside `frogdb-cluster` already goes through
  (`slot_migration/routing.rs`, `slot_fence.rs`, `guards.rs`, `debug_providers.rs`, the debug web
  UI, and the in-crate model checker). It is not an adapter: it converts nothing and translates
  nothing, it is simply what the crate exports instead of its internals. That interface is what
  makes this change containable — rearranging the inside of `frogdb-cluster` cannot be seen
  through it.

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
and it is driven by a wire constraint (see [Risks](#risk-1--the-raft-snapshot-is-clusterstateinner-serialized-whole)):
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
| `matching(slot, source, target) -> Result<SlotMigration, ClusterError>` | the duplicated preamble `:573-581` / `:684-697` | one construction of `"migration parameters don't match"`. **Returns owned data, never `&SlotMigration`** — a reference borrowed from the façade cannot outlive the `migration_table()` temporary (E0716); see [Risk 3](#risk-3--borrow-checker-friction-at-every-arm-that-interleaves-inner-fields). The record is five scalars plus an `Option<SlotHandoff>` of five more (`types.rs:741-777`, `:687-710`), so the copy is free |
| `prepare(slot, at_ms, barrier_ms, lease_ms) -> Result<u64, ClusterError>` | `:586-605` | mints the generation *and* installs the record in one borrow — no `get`/`get_mut` pair, no `.expect` |
| `confirm_drained(slot, seq) -> Result<(), ClusterError>` | `:633-645` | |
| `abort(slot, seq) -> Vec<ClusterEvent>` | `:654-671` | idempotent, as today |
| `admits_complete(slot, at_ms) -> Result<u64, ClusterError>` | `:717-735` | the `admits_complete_at` decision, returning the `seq`; the three `HandoffNotReady` bodies move byte-identical. **Separate from `complete` below** — it is fallible, and FM-CLUSTER-084's NOT-observable cell requires the check to precede every mutation, so it cannot be folded past the `slot_assignment` write at `:738` |
| `complete(slot) -> Vec<ClusterEvent>` | `:739` + `:755-759` | removes the record and returns the release events it owes, through the private helper — this is [hotfix A](#hotfix-a-in-full) relocated |
| `cancel(slot) -> Vec<ClusterEvent>` | `:765-769` | |
| `prune_naming(node_id) -> Vec<ClusterEvent>` | `prune_migrations_naming` `:44-56` | moves verbatim |
| `reset() -> Vec<ClusterEvent>` | `:826-830` | rewinds the generation in the same call that clears the map — the pairing FM-CLUSTER-086 and -100 both rely on |
| `release_events(SlotMigration) -> Vec<ClusterEvent>` | `:18-33` | moves verbatim; stays a private free function, now in `migrations.rs` |

The two removal paths that hand-build their events today (`Complete` `:755-759`, `Abort`
`:667-671`) both go through the helper instead.

**What that does and does not buy — corrected.** An earlier draft of this proposal claimed the
move makes `release_events` private "and that is the whole enforcement mechanism". That claim is
false and is withdrawn. `release_events` is **already** private in the strongest sense Rust
offers: it is declared `fn release_events(...)` with no `pub` (`commands.rs:18`) inside `mod
commands;`, which `lib.rs:45` declares without `pub`. It has exactly three callers, all in the
same file (`:54`, `:768`, `:828`). Moving it to `migrations.rs` changes its visibility from
"private to `commands`" to "private to `migrations`" — a lateral move, not a narrowing, and no
arm that could call it before is prevented from calling it after.

The enforcement that actually exists is different, and it is a **tier-2** property, not a
tier-1 one:

- **the fields, not the helper.** Once `ClusterStateInner::{migrations, handoff_seq}` are private
  to `crate::state` and `migration_table()` is the only `&mut` door (read accessors hand out
  `&BTreeMap`, never `&mut`), an arm *cannot* remove a record without calling a `MigrationTable`
  method, and every such method calls the helper. That is a compile-time property. It requires
  tier 2, because tier 1 leaves the fields `pub(crate)` and any arm can still write
  `inner.migrations.remove(&slot)` directly.
- **`#[must_use]` on the removal methods.** Visibility cannot force an arm to *emit* the
  `Vec<ClusterEvent>` it was handed — an arm that calls `table.cancel(slot)` and drops the result
  has satisfied every rule above and still stranded a barrier. `#[must_use]` on `abort`, `cancel`,
  `complete`, `prune_naming` and `reset` is what turns "you owe a release event" into a warning
  the build denies, and it is the only mechanism in the design that addresses the *emission* half
  of the obligation.

### What deliberately does not change

| unit | why |
|---|---|
| `ClusterStateInner`'s field names, types, order, `#[serde(default)]` | the Raft snapshot wire format (`encoding_golden.rs:1-29`) |
| `ClusterSnapshot` (`types.rs:801-829`) and `to_snapshot` (`state.rs:433-447`) | the read-side *interface* every consumer outside `frogdb-cluster` uses — and, inside the crate, the model checker (`model/mod.rs:220`) and `wire.rs`'s fixture (`:252`) |
| `SlotHandoff` / `SlotMigration` and their impls (`types.rs:687-798`) | already deep; FM-CLUSTER-089's Invariant cell cites them by name. **They do not move into `migrations.rs`** — `MigrationTable` borrows `ClusterStateInner`'s fields, it is not a new home for the record types. This is what refutes proposal 62's ordering edge 2 |
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
4. **The removal obligation is enforced by construction — a TIER-2 criterion.** Split, because
   only one half is available in tier 1:
   - *(tier 1)* **`#[must_use]` on every method that can return release events** — `abort`,
     `cancel`, `complete`, `prune_naming`, `reset`. Dropping the returned `Vec<ClusterEvent>` must
     not compile clean. This is the only mechanism that binds the *emission* half of the
     obligation, and visibility can never substitute for it.
   - *(tier 2)* **`ClusterStateInner::{migrations, handoff_seq}` are private to `crate::state`,
     and `migration_table()` is the sole `&mut` door.** Read accessors return `&BTreeMap<u16,
     SlotMigration>` / `u64` and there is no `&mut` accessor of any kind. `grep -rn
     'migrations\s*\.\s*\(remove\|insert\|get_mut\)' cluster/src` returns hits only inside
     `state.rs` and `migrations.rs`. **This** is what makes "every removing arm routes through the
     funnel" a compile-time fact rather than a convention.

   Note what this criterion is *not*: "`release_events` is private". It already is — a bare `fn`
   (`commands.rs:18`) in a private `mod commands` (`lib.rs:45`) with three in-file callers.
   Restating that as an acceptance criterion would be a no-op dressed as a gate.
5. **`debug_assert_clean` still runs at exactly the three seams it runs at today** —
   `commands.rs:108` (`"apply_command"`), `state.rs:166` (`"from_snapshot"`) and `state.rs:267`
   (`"restore_from_snapshot"`) — the façade must not introduce a fourth or drop one.
6. **No new `.expect()`/`.unwrap()` in the moved code.** `commands.rs:598`'s
   `expect("migration presence checked above")` is deleted, not relocated.
7. **Spec edits are documentation-only** (see below) — no `Observable` or `NOT observable` cell
   changes, because no behavior changes.

### Spec impact (spec-first discipline)

This is a pure restructuring: no `Observable` cell moves, so it is **not** a spec-first behavior
change and needs no new failure-mode row. **Four cells** in
`.scratch/hardening/specs/cluster-failure-modes.md`, all of which make existing prose *true* rather
than changing what is claimed:

- **FM-CLUSTER-087 Invariant** (`:1244`): re-point at `cluster/src/migrations.rs` and state the
  enforcement mechanism honestly — the removal methods are `#[must_use]` and (after tier 2) the
  fields they mutate are unreachable from `commands.rs` — instead of asserting a convention.
  **This edit is only honest once `Complete` actually routes through the helper** — which is
  hotfix A below, landable first and independently.
- **FM-CLUSTER-087 `Forced by`** (`:1246`): add
  `prepare_then_drain_then_complete_moves_ownership`, and add `FM-CLUSTER-087` to that test's tag
  line (`commands.rs:1426`). Required in both directions —
  `scripts/failure-modes.py:7-15` enforces spec→test *and* test→spec. This is hotfix B, also
  independently landable. **Contended cell — see [the 59 edge](#boundary-vs-proposal-59-cluster-event-router-rc7--same-crate-same-file-disjoint-regions).**
- **FM-CLUSTER-086 Invariant** (`:1231`): cites `ClusterStateInner::handoff_seq` and
  `cluster/src/{state,commands}.rs` for the mint and for `ResetCluster`'s rewind. Both operations
  move into `migrations.rs` (`prepare`, `reset`); re-cite. The field itself stays on
  `ClusterStateInner`, so the "replicated counter" claim is untouched.
- **FM-CLUSTER-084 Invariant** (`:1206`): *(added on review — this row was missed by the earlier
  draft.)* The cell says `admits_complete_at` is the single predicate and that
  *"`apply_command`'s `CompleteSlotMigration` arm consults it after the parameter match and before
  touching `slot_assignment` (`cluster/src/{types,commands}.rs`)"*. After the change the predicate
  is consulted inside `MigrationTable::admits_complete(slot, at_ms)`, and the arm's remaining job
  is the node check and the slot-map write. Left alone, this sentence drifts in exactly the way 087's already
  has — an Invariant cell naming a location the code left, which `scripts/failure-modes.py` never
  reads. Re-cite to `cluster/src/{types,migrations}.rs` and name the method.

**FM-CLUSTER-100 (`:1448`) is *not* edited.** Its Invariant cell is about the *projection* —
`handoff_seq` living on `ClusterStateInner` and being carried by both restore vehicles
(`encode_snapshot`/`install_snapshot` and `to_snapshot`/`from_snapshot`). This proposal moves
neither the field nor `to_snapshot` (`state.rs:433-447`), so nothing in that cell becomes false.
The earlier draft listed it and then half-retracted it in the same sentence ("the serde half …
is untouched"); the whole cell is untouched.

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
2. **Exhaustiveness becomes checkable — by reading, in tier 1; by the compiler, in tier 2.** With
   every removal inside one `impl`, "did I cover every removing path?" is answerable by reading
   one file. It becomes a *compile-time* answer only once the fields are private (tier 2), at
   which point a new removing path cannot exist outside that `impl` at all. The earlier draft
   claimed the private helper alone gave this ("a new arm cannot compile against a private
   function it did not call") — that is backwards: a function you do not call constrains nothing.
   What constrains a new arm is being unable to reach the map.
3. **New mutants land where tests can kill them.** `mutants` on `frogdb-cluster` currently has to
   kill mutations scattered through a 738-line `match`; concentrating the arithmetic and the
   removals raises the density of *observable* mutants, which is what the 0.80 gate measures.
4. **The property harness gets a cheaper oracle.** `properties.rs:888-890`'s `live_handoffs`
   re-derives from `&ClusterStateInner` a fact the table could answer directly; so do
   `migration_ref` (`:381-390`) and `handoff_ref` (`:710-761`). Not required by this proposal, but
   it becomes possible. (`model/mod.rs:665-671`'s `handoff_seqs_never_reused` is **not** on this
   list — it reads `ClusterSnapshot` through `Node.view` `:220`, so the table is not in its
   path.)
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
nothing.

The earlier draft's mitigation — "criterion 4, `release_events` private with no external caller" —
was **circular and is withdrawn**: `release_events` is already private with no external caller
(`commands.rs:18`, `lib.rs:45`, callers `:54`/`:768`/`:828`), so that criterion is satisfied by
today's tree and falsifies nothing. **Mitigations that are actually falsifiable:**

- **The tier-2 half of criterion 4.** If the fields end up private and `migration_table()` is the
  only `&mut` door, a rename-only façade is impossible: a method named `remove` that returns
  `Option<SlotMigration>` and lets the caller decide about events would fail the `#[must_use]`
  and release-event review, and the removal set is enumerable by reading one `impl`.
- **The method table is a review artifact.** Every method above is named for a protocol
  transition, takes the protocol's parameters (`at_ms`, `seq`, `barrier_ms`, `lease_ms`) and
  returns either a `Result` carrying the arm's decision or the events the removal owes. A
  reviewer can check that list against the arm list in [Problem §1](#1-the-lifecycles-interface-is-a-btreemap-and-its-api-is-nine-match-arms)
  without reading the bodies.
- **The double lookup is the falsifier for `prepare`.** If `commands.rs:595-598`'s
  `get_mut(...).expect(...)` survives anywhere in the diff, the façade did not take ownership of
  the transition (criterion 6).

### Risk 3 — borrow-checker friction at every arm that interleaves `inner` fields

Three arms interleave migration mutation with mutation or reads of sibling `ClusterStateInner`
fields, and a `&mut` façade held across those lines will not borrow-check:

- **`Failover`** mutates `inner.nodes`, `inner.slot_assignment` and `inner.config_epoch`
  interleaved with the migration prune (`:417-470`).
- **`ResetCluster`** clears three collections (`:820-830`).
- **`CompleteSlotMigration`** *(added on review — the earlier draft named only the first two, and
  it is the hardest of the three)*. It alternates across the façade boundary **five times** in the
  fifty-odd lines from `:684` to `:739`: `matching` at `:684-697` (façade),
  `inner.nodes.contains_key` at `:708` (not), the `admits_complete_at` decision at `:717-735`
  (façade), `inner.slot_assignment.insert` at `:738` (not), `migrations.remove` at `:739`
  (façade). The order is not negotiable — the three façade steps are separated by two non-façade
  ones, the first two façade steps are **fallible**, and FM-CLUSTER-084's NOT-observable cell
  (`:1205`) requires *"the check precedes every mutation"*, so no fallible step may be folded past
  `:738`. The implementation must therefore acquire and drop the façade three separate times, which
  is why the method table splits `admits_complete` from `complete`.

**Mitigation:** the façade is acquired, used, and dropped within a single statement at each
mutation point — `prune_naming` at `:450`, `reset` at `:826`, and each of `Complete`'s three
steps. This is what forces `matching` to return **owned** data rather than
`Result<&SlotMigration, ClusterError>`: a reference into the map borrowed from a
`self.migration_table()` temporary cannot survive the end of the statement that created it
(E0716), and the arm needs the record's fields alive across `:708` and `:738`. The two designs are
mutually exclusive; owned wins, and the copy is five scalars plus an `Option<SlotHandoff>`.

This is a real constraint on the implementation, not a blocker, but it is the thing most likely to
tempt an implementer into changing statement order — which criterion 2 forbids. **Note the tier
interaction:** today an implementer who loses this fight can fall back on split-field borrows
(`let (m, s) = (&mut inner.migrations, &mut inner.handoff_seq)`), because the fields are `pub`.
Tier 2 removes that escape hatch by construction, so the borrow story must be genuinely sound
before tier 2 lands rather than after.

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

They also share spec rows: 62 re-cites FM-CLUSTER-089/091's Invariant cells (`slot_migration/mod.rs`
and `handoff_barrier.rs` → new homes), 60 re-cites 084/086/087. Disjoint cells, same file — a
trivial merge, but they must not land in the same commit.

**62's stated reason for ordering itself first is refuted by this proposal's own scope.** 62's
conflict edge 2 reads: *"62's `observe` takes `Option<&SlotHandoff>` and reads `prepared_at_ms`,
`seq`, `drained`. If 60 lands first and relocates `SlotHandoff` into `migrations.rs` behind a
`MigrationTable` accessor, 62's imports and possibly its signature move with it."* 60 does not
relocate `SlotHandoff`. [What deliberately does not change](#what-deliberately-does-not-change)
freezes `SlotHandoff` / `SlotMigration` and their impls at `types.rs:687-798` — FM-CLUSTER-089's
Invariant cell cites them by name, and `MigrationTable` is a *borrowed façade over
`ClusterStateInner`'s fields*, not a new home for the record types. `use crate::types::SlotHandoff`
resolves identically before and after. **62's edge 2 therefore does not bind**, and if it was 62's
only ordering argument against 60 the two may be order-free. Flagged for the orchestrator's
dependency graph rather than ruled here: it is 62's edge and 62's to withdraw, and edge 3 (shared
`frogdb-cluster` re-gate, do not run concurrently) stands regardless.

Two descriptions in 62's boundary table are stale and should be corrected when 62 is next revised,
because a reviewer comparing the two proposals will trip on them:

- 62 says *"the **eight** `apply_command` arms in `cluster/src/commands.rs:48-783`"*. There are
  **nine**: `RemoveNode` `:215-236`, `Failover` `:393-499`, `BeginSlotMigration` `:527-563`,
  `PrepareSlotHandoff` `:565-625`, `ConfirmSlotHandoffDrained` `:627-648`, `AbortSlotHandoff`
  `:650-676`, `CompleteSlotMigration` `:678-762`, `CancelSlotMigration` `:764-772`, `ResetCluster`
  `:816-862`. The span `:48-783` also omits `ResetCluster`, which is where the generation is
  rewound.
- 62 says 60 introduces *"`drop_handoff` as the sole removal path"*. No such method is proposed;
  the removal surface is five methods (`abort`, `cancel`, `complete`, `prune_naming`, `reset`),
  all routed through the private `release_events`. The name appears nowhere in this proposal.

### Boundary vs proposal 59 (cluster event router, RC7) — same crate, same file, disjoint regions

*Re-derived from 59's own Files table on disk, not from the lane brief.* The earlier draft of this
section quoted stale line numbers (`state.rs:879-1005`, `:563-576`, `:1007-1018`) that 59 no longer
uses; 59 itself notes the lane's citations were ~35-45 lines stale.

59's actual `state.rs` footprint: `apply` `:914-1040` with the fan-out at `:942-1024`;
`SlotHandoffEvent` `:562-595` and the other three channel-facing event types `:518-560`;
`ClusterStateMachine` fields `:598-611`; ctors `:615-624`, `:627-636`, `:1042-1054`; the identity
handles `:26-28`, `:210-218`; `apply_local` `:338-342`; the three `enable_*` methods `:700-735`;
`self_role` / reconciler `:744-796`, `:824-829`, `:845-890`; `install_snapshot` role diff
`:1086`, `:1093-1096`; and the test module `:1166-4402` (blanket citation — 59's criterion 1 is
*zero edits to existing tests*, so its only test-module change is appending six new ones).

60's `state.rs` footprint: `:107-135`, `:145-159`, `:319-327`, `:344-377`, `:433-447`.

**Conclusion survives: no region overlap.** Note one adjacency a rebase will meet — 59 cites
`apply_local` at `:338-342`, and 60 owns `arm_handoff_for_test` at `:344-377`, two lines apart.
Whichever lands second re-anchors; neither edits the other's lines.

**59 lists a `cluster_init.rs` conflict edge with 60. There is none.** 59's Files table marks
`server/src/server/cluster_init.rs` (`:200`, `:202`, `:216`, `:226`, `:237`, `:240`, `:421`,
`:704-727`) as a *"Conflict edge with proposals 62 and 60"*. 60 does not open that file, does not
change any `frogdb-cluster` public signature (`lib.rs:76-82`'s `pub use` list is unchanged;
`mod migrations;` is private), and touches no `frogdb-server` file at all. The 62 half of that edge
may well be real; the 60 half is not.

#### Contended spec cell — FM-CLUSTER-087 `Forced by` (`:1246`)

**This is the one place 59 and 60 collide, and it is a same-line collision in a LOCKED spec.**

59's acceptance criterion 4 requires two new tests — `a_prepared_handoff_routes_to_the_handoff_channel`
and `a_released_handoff_routes_to_the_handoff_channel` — to be added to FM-CLUSTER-087's
`Forced by` cell (59 pins them to `-087` explicitly, and its criterion 6 states that `Forced by`
cells change *only* by those additions). 60's **hotfix B** adds
`prepare_then_drain_then_complete_moves_ownership` to the same cell. `Forced by` is a single
Markdown table cell on line `:1246`; two branches editing it produce a textual conflict, and the
cell claimed "independently landable" is not independent of 59.

*(59 is being revised concurrently and its criterion numbering may shift. The substance is what
binds: whichever criterion carries 59's two new routing tests targets this cell.)*

**Sequencing, in preference order:**

1. **Land hotfix B first, alone, before either proposal.** It is two lines (`:1246` plus the tag at
   `commands.rs:1426`), needs no code change and no mutation gate, and 59 then adds its two tests
   to a cell that already reads correctly. This is the recommended order and costs nothing.
2. If hotfix B has not landed by the time 59 does, 60 rebases the cell — a one-line merge, but a
   one-line merge in a LOCKED spec, which means `just lint-failure-modes` must be re-run after the
   rebase (it enforces spec→test *and* test→spec, `scripts/failure-modes.py:7-15`) and the merged
   cell re-read by hand.
3. Under no circumstances do 59 and 60 hold concurrent uncommitted edits to
   `cluster-failure-modes.md`. This is the shared-tree trap, and the file is the contract.

#### The rest of the 59 coupling is the gate

Both change `frogdb-cluster`, so the honest sequencing is *land 59, land 60, then one
`just mutants frogdb-cluster` + `mutants-gate … 0.80` covering both*, with `mutants-diff` per PR as
push discipline. Running the full gate twice is ~2× a testbox-class workload for no extra
information. 59's criterion 7 asks for the same thing from its side.

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

- *Refuted:* "solo" in the sense of **production-file** conflict. Verified region-by-region, 60
  does not overlap 58 (different crate files), 59 (same file, disjoint regions), 61 (different
  crate) or 62 (different crate).
- *But not solo in the spec.* 60 and 59 both edit FM-CLUSTER-087's `Forced by` cell at
  `cluster-failure-modes.md:1246` — [the one same-line collision](#contended-spec-cell--fm-cluster-087-forced-by-1246),
  in a LOCKED file. That is a real serialization point, and it is why hotfix B should land alone
  and first.
- *Confirmed:* "last" as a **gate-ordering** constraint. 60 is the second `frogdb-cluster` change
  in the round (59 is the first) and the one whose value is measured by the 0.80 mutation gate.
  Landing it last means the full gate runs once, over both, and any surviving mutant is attributed
  to the final shape rather than to an intermediate one. Both siblings ask for this independently:
  59's criterion 7 ("chain with proposals 57/58/60 and gate once") and 62's conflict edge 3 ("run
  the full gate once at the end of the chain, not twice … do not run them concurrently").
- *Withdrawn:* the earlier draft's claim that "last" is a **review** constraint and that this is
  its strongest form. The argument was that a reviewer diffing 60 could not tell whether a changed
  `TRYAGAIN` string belonged to 60 or to a concurrent 62 — but 60 and 62 share **no file**
  ([boundary table above](#boundary-vs-proposal-62-handoff-finalizer-rc11--state-machine--finalizer)),
  so a reviewer reading 60's diff sees no `TRYAGAIN` rendering change in either order. Criterion 3
  is checkable by inspection of 60's own diff against `types.rs:629-645` and the five construction
  sites, with or without 62 in flight. The real argument for serializing the two is the plain one:
  **shared-tree concurrency** — two changes restructuring one LOCKED crate's handoff code
  simultaneously, sharing a mutation gate and a spec file, is the trap, and it does not need a
  review-legibility story on top.
- *Refuted:* the lane's "PURE move". It is not a pure move — `PrepareSlotHandoff`'s double lookup
  collapses to one, and `Complete`'s hand-built release event is replaced by the funnel. Both are
  behavior-preserving and both are asserted by existing tests, but "pure move" would let a reviewer
  skip exactly the two places where care is needed.

**Net ordering statement, for the orchestrator's dependency graph:**

1. **Hotfix B alone, first** — it is the only same-line contention in the round
   (`cluster-failure-modes.md:1246`, shared with 59), and landing it standalone dissolves the
   conflict for two proposals at the cost of two lines.
2. **Hotfix A next, also standalone** — independent of everything, and it retires the funnel bypass
   before the refactor that would otherwise inherit it.
3. **59, then 60**, serialized on the shared tree and the shared `frogdb-cluster` gate; one full
   `mutants` + `mutants-gate 0.80` at the end of the chain.
4. **62 relative to 60: no ordering constraint that this proposal can find.** 62 asks to go first
   because it fears `SlotHandoff` relocating; it does not relocate. If 62 has no other reason, the
   two are order-free and need only avoid *concurrency* (shared gate, shared spec file, different
   cells). **This is flagged, not ruled** — it is 62's edge to withdraw.
5. **58 and 61: either order**, no constraint beyond 58's shared `frogdb-cluster` gate.

### Size verdict

**The lane's "L" is right only if the enforcement tier is included.** Two separable tiers:

- **Tier 1 (M)** — `migrations.rs` + the façade + nine arms rewritten + `#[must_use]` on the five
  removal methods + the two fields narrowed from `pub` to `pub(crate)` (verified free: nothing
  outside `frogdb-cluster` reads `ClusterStateInner.migrations`; external readers all go through
  `ClusterSnapshot.migrations`). Every read site in `invariants.rs`, `properties.rs` and the test
  fixtures compiles **unchanged**, because the fields still exist. Delivers criteria 1, 2, 3, 5, 6,
  7 and the tier-1 half of criterion 4 (`#[must_use]`), plus every testability claim except #2's
  compile-time guarantee. **It does not deliver the enforcement half of criterion 4** — that is
  tier 2 by construction, and no amount of tier-1 work substitutes for it.
- **Tier 2 (S–M, on top)** — make both fields private to `crate::state` behind read accessors and
  `migration_table()`, so the funnel is unbypassable within the crate too. This is the tier that
  turns FM-CLUSTER-087's Invariant sentence into a compile-time fact.

  Cost, recounted against the tree (the earlier draft priced ~22 sites and mis-attributed six of
  them):

  | site class | count | note |
  |---|---:|---|
  | `invariants.rs` production reads | 5 | `check_ref_2` `:350`, `check_handoff_1` `:538`, `check_handoff_2` `:569`, `check_mig_1` `:606`, `check_slot_1` `:643` |
  | `properties.rs` reads | ~10 | `:382`, `:390`, `:407`, `:491`, `:602`, `:761`, `:779`, `:890`, `:1031`, `:1346` — all against `&ClusterStateInner` |
  | `state.rs` (`:150`, `:321`, `:326`, `:443`) | 0 | **free** — the fields stay visible inside `crate::state`, which is where these live |
  | `model/mod.rs` (`:427`, `:437`, `:452-456`, `:665-671`, `:706`) | 0 | **outside the blast radius** — `ClusterSnapshot`, declared unchanged |
  | `wire.rs:266-267` | 0 | **outside the blast radius** — `fixture() -> ClusterSnapshot` (`:252`) |
  | test fixtures needing a seeding constructor | ~9 | `invariants.rs` `clean_state` literal (`:728-763`, map + `handoff_seq: 4`), inserts `:932`/`:945`/`:1043`/`:1102`, `get_mut` `:1062`, `handoff_seq` writes `:1035`/`:1131`; `encoding_golden.rs` `state_fixture` (`:283-302` + `:322`) |

  **Net ≈ 15 call-site edits plus one seeding constructor and ~9 fixture rewrites — smaller than
  priced**, because the six `model/mod.rs` and `wire.rs` citations the earlier draft counted were
  never in scope: they read the DTO this proposal freezes.

Tier 1 is where the safety argument is won; tier 2 is where it is made permanent — and, per the
corrected criterion 4, tier 2 is where the *enforcement* claim is made at all. Recommend both, in
two commits, with tier 1 reviewable on its own.

## Effort

| step | scope | size |
|---|---|---|
| **Hotfix A** — route `CompleteSlotMigration` through `release_events` | `commands.rs:738-761`, replacing the hand-built `SlotHandoffReleased` at `:755-759`. See the proof and the corrected sketch [below](#hotfix-a-in-full). **No refactor, no new test, makes a LOCKED spec sentence true.** | **S** — ~6 lines + 1 spec line |
| **Hotfix B** — credit the `Complete` release assertion to FM-CLUSTER-087 | Add `FM-CLUSTER-087` to the tag at `commands.rs:1426` and `prepare_then_drain_then_complete_moves_ownership` to 087's `Forced by` (`:1246`). Both directions required by `scripts/failure-modes.py`. Closes the gap between 087's Trigger cell (which names `Complete`) and its forcing set (which does not). | **S** — 2 lines |
| **1 — the module** | New `cluster/src/migrations.rs`: `MigrationTable<'a>`, the eleven methods above with `#[must_use]` on the five that can return release events, `release_events` moved (still private), `prune_migrations_naming` moved. `lib.rs:46-61` gains `mod migrations;` (private — nothing outside the crate needs it). Unit tests for the removal/obligation table. | **M** — ~230 new, ~half tests |
| **2 — rewire the arms** | Nine arms in `commands.rs` rewritten to acquire the façade at each single mutation point (`Complete` has three — [Risk 3](#risk-3--borrow-checker-friction-at-every-arm-that-interleaves-inner-fields)). `commands.rs:595-598`'s double lookup and `.expect` deleted. Existing tests unmodified. | **M** — ~120 changed, net negative in `commands.rs` |
| **3 — narrow visibility (tier 1)** | `state.rs:115`/`:126` `pub` → `pub(crate)`; `migration_table()` accessor. Does **not** satisfy criterion 4's enforcement half. | **S** |
| **4 — spec re-cites** | The four cells above (087 Invariant, 087 `Forced by`, 086 Invariant, 084 Invariant); `just lint-failure-modes` (`uv run --offline`). Coordinate `:1246` with proposal 59. | **S** |
| **5 — privatize (tier 2, separate commit)** | Fields private to `crate::state`, read accessors returning `&BTreeMap`/`u64` and no `&mut` accessor; ~15 call-site edits + a test seeding constructor + ~9 fixture rewrites (see [Size verdict](#size-verdict)). **This is the commit that delivers criterion 4.** | **S–M** |
| **Re-gate** | `mutants-diff` per PR; one `mutants frogdb-cluster` + `mutants-gate frogdb-cluster 0.80` shared with proposal 59. Testbox-class workload. | — |

Both hotfixes are independently valuable, need no part of the refactor, and are worth landing even
if this proposal is rejected — A because it retires a duplicate implementation of a safety
obligation, B because it makes a LOCKED row's own Trigger cell traceable to a test. **B is not
independent of proposal 59**: see [the contended spec cell](#contended-spec-cell--fm-cluster-087-forced-by-1246).

### Hotfix A in full

**The claim, stated precisely: byte-identical *given INV-HANDOFF-2*.** `release_events` destructures
the record (`commands.rs:19-24`) and emits `SlotHandoffReleased { slot: migration.slot,
source_node: migration.source_node, seq: h.seq }`. Today's arm emits the **command's** `slot` and
`source_node` and the `seq` it captured at `:717-735`. Three legs, and the earlier draft named only
two:

1. **`source_node`.** Validated equal to `migration.source_node` at `:693-697`, or the arm returned
   `InvalidOperation`. Unconditional.
2. **`seq`.** `seq` is `h.seq` from the record's own `handoff`, taken at `:718`, and the record is
   not mutated between `:718` and the `remove` at `:739`. Unconditional.
3. **`slot`** *(the missing leg)*. The arm looks the record up by the command's `slot` (`:684-687`)
   but `release_events` reads `migration.slot` — the record's own field. These agree iff a
   migration is filed under its own slot, which is precisely **INV-HANDOFF-2**
   (`invariants.rs:238-243`, claim *"a handoff lives inside the migration for its own slot"*;
   checked at `:569-571`). That is a **HARD** invariant, but it is asserted only through
   `debug_assert_clean`, compiled `#[cfg(any(test, debug_assertions))]` (`invariants.rs:302-311`)
   at three seams (`commands.rs:108`, `state.rs:166`, `:267`). In a release build nothing enforces
   it. So the honest statement is *"byte-identical given INV-HANDOFF-2"*, not *"byte-identical"* —
   and the same dependency already exists today in `Cancel`, `Abort`'s prune path and every other
   `release_events` caller, so the hotfix does not add exposure. It removes the one arm that was
   avoiding the funnel.

**The sketch, corrected.** The earlier draft's `let removed = inner.migrations.remove(&slot); …
release_events(removed)` does not compile: `BTreeMap::remove` returns `Option<SlotMigration>`, and
`release_events` takes `SlotMigration` by value. Use the pattern `CancelSlotMigration` already uses
at `:765-769`, keep `let seq` (it is consumed by the `tracing::info!` field at `:744`), and build
the `Completed` event **first**, then `extend` — the `[SlotMigrationCompleted, SlotHandoffReleased]`
order is pinned by `prepare_then_drain_then_complete_moves_ownership` (`commands.rs:1428`,
assertion `:1446-1460`) and restated in FM-CLUSTER-084's Outcome-variant cell (`:1208`):

```rust
// commands.rs:738-761
inner.slot_assignment.insert(slot, target_node);
let released = inner
    .migrations
    .remove(&slot)
    .map(release_events)
    .unwrap_or_default();
tracing::info!(slot, source_node, target_node, seq, "Completed slot migration");

let mut events = vec![ClusterEvent::SlotMigrationCompleted {
    slot,
    source_node,
    target_node,
}];
events.extend(released);
Ok((ClusterResponse::Ok, events))
```

Six lines net. `prepare_then_drain_then_complete_moves_ownership` is the regression test and it
already exists, unmodified.

### What hotfix A costs this proposal, said plainly

Hotfix A is recommended unconditionally — but landing it retires the headline item of
[the deletion test](#the-deletion-test). Once `Complete` routes through the funnel, "two hand-built
release events" becomes one (`Abort`'s, which is defensible: it clears `handoff` without removing
the record, so the helper's signature does not fit), and the most vivid symptom of the shallow
boundary is gone from the tree.

The residual case for the refactor, stated without the symptom:

- **What remains, tier 1:** an M-sized cleanup with a genuine unit-testability upside on the
  removal obligation — the five removal paths become callable without a `ClusterState`, a node
  table or Raft, which is a test *shape* the crate does not have today (see
  [Testability improvement](#testability-improvement) #1). Plus the double lookup and `.expect`
  deleted, the duplicated parameter check unified, and the generation mint given a name.
- **What remains, tier 2:** the compile-time enforcement of FM-CLUSTER-087's Invariant sentence.
- **What is *not* claimed:** that tier 1 is "the enforcement mechanism for a LOCKED safety
  obligation". It is not; `#[must_use]` is a warning and the fields are still `pub(crate)`.
  Enforcement is deferred to tier 2, and a triage that approves tier 1 while deferring tier 2
  should understand it is buying testability and legibility, not a guarantee.

That is a weaker case than the earlier draft made, and it is still a positive one — but a reviewer
should weigh it after hotfix A has landed, not before, because before is when the tree still
contains the evidence that flatters it.
