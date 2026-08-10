# Proposal 41 — Persistence small dedups: WalSink delegation, clear/flush outcome block, deletion tests

Lane candidates: **PE5 + PE7 + PE10**.

## Summary

Three small, independent cleanups in `frogdb-persistence`, bound by one through-line:
**the deletion test** — imagine the module gone; if complexity vanishes it was a
pass-through, if complexity reappears across N callers it was earning its keep.

- **PE5 (WalSink asymmetry).** `RocksWalWriter` publishes the same twelve methods
  twice: once as inherent `impl` bodies, once as a `WalSink` trait `impl` whose every
  method is a verbatim `RocksWalWriter::foo(self, …)` re-call (writer.rs:333–381).
  Every production caller reaches the writer through `dyn WalSink`; the inherent
  half is a second interface kept alive only by in-crate tests. Move the bodies into
  the trait `impl`, delete the duplicate half. Also delete
  `RocksWalWriter::committed_sequence` (writer.rs:314–317) — one test caller, and the
  datum it returns is already published on `WalLagStats`.
- **PE7 (clear/flush outcome duplication).** `apply_clear` (flush.rs:584–612) and
  `flush` (flush.rs:640–677) each carry their own copy of the commit-outcome block:
  the same success/failure match, the same `record_success`/`record_failure` calls,
  and the same four metric families (`WalFlushDuration`, `WalFlushFailures`,
  `WalLostOps`, `WalLostBytes`). Extract one `record_commit_outcome`.
- **PE10 (deletion tests).** Two kills and one **rejected** kill.
  `SnapshotStager.data_dir` (stager.rs:70–74) is `#[allow(dead_code)]` behind a
  28-line justification comment; `SnapshotScheduler::request()` / `schedule()`
  (scheduler.rs:124–134, 191–199) are legacy siblings of `request_mode`, with zero
  production callers. **`RecoveredState.installed_staged_checkpoint` is NOT dead** —
  it is the named `Outcome variant` of two locked spec rows (see the verification
  note). Deleting it is refused here.

None of the three changes behavior. All three are inside the **locked** persistence
area (`frogdb-persistence` + `frogdb-recovery`, gate 0.85), so they carry a mutation
re-gate; PE10 additionally carries a **required spec edit**, because three of the
tests it deletes are named in `Forced by` lists and `just lint-failure-modes` fails
the moment a spec row names a test that no longer exists.

## Files involved

| Path | Lines | Role in this proposal |
|---|---|---|
| `frogdb-server/crates/persistence/src/wal/writer.rs` | 394 | PE5: inherent `impl` + the 12-method `WalSink` delegation `impl` (333–381); `committed_sequence` (314–317) |
| `frogdb-server/crates/persistence/src/wal/sink.rs` | 53 | PE5: the `WalSink` trait — the surviving interface. Unchanged, but it is the seam the change consolidates onto |
| `frogdb-server/crates/persistence/src/wal/flush.rs` | 827 | PE7: `apply_clear` (559–613) and `flush` (620–678); `FlushOutcomes::record_success`/`record_failure` (271–290) |
| `frogdb-server/crates/persistence/src/wal/tests.rs` | 2343 | PE5: ~30 concrete-typed call sites; the lone `wal.committed_sequence()` caller (2035) |
| `frogdb-server/crates/persistence/src/snapshot/scheduler.rs` | 212 | PE10: `request()` (124–134) and `schedule()` (191–199) beside `request_mode` (144–152) |
| `frogdb-server/crates/persistence/src/snapshot/stager.rs` | 347 | PE10: the `data_dir` field (70–74) and its justification comment (98–125) |
| `frogdb-server/crates/persistence/src/snapshot/tests.rs` | 1456 | PE10: the scheduler tests that pin `request()` / `schedule()` (686–689, 737, 772, 815–841, 881) |
| `frogdb-server/crates/core/src/persistence/tests.rs` | — | PE5: concrete-typed `wal.write_set` / `wal.flush_async` (146–148) — needs `WalSink` in scope |
| `frogdb-server/crates/core/src/persistence/crash_recovery_tests.rs` | — | PE5: same, at 1697–1710 and 1758 |
| `.scratch/hardening/specs/persistence-failure-modes.md` | — | PE10: `Forced by` lists of FM-PERSISTENCE-015 (line 252) and FM-PERSISTENCE-016 (line 264) must be edited in the same commit |
| `frogdb-server/crates/recovery/src/lib.rs` | 270 | **Read-only here.** `installed_staged_checkpoint` (126) is spec-visible; handed to proposal 43 (see Risks) |

## Problem

### PE5 — one module, two interfaces, one of them a pure pass-through

`WalSink` (sink.rs:13–53) is the WAL-write seam: twelve methods, documented as "the
WAL-write surface a shard needs, as a seam," with two adapters —
`RocksWalWriter` (production) and `FakeWalSink` (deterministic test double). Two
adapters means the seam is real, and it stays.

`RocksWalWriter` then implements that seam by calling itself. Verbatim,
writer.rs:333–381:

```rust
#[async_trait::async_trait]
impl super::WalSink for RocksWalWriter {
    async fn write_set(&self, key: &[u8], value: &Value, metadata: &KeyMetadata) -> std::io::Result<u64> {
        RocksWalWriter::write_set(self, key, value, metadata).await
    }
    …
    fn shard_id(&self) -> usize {
        RocksWalWriter::shard_id(self)
    }
}
```

All twelve are that shape: `write_set`, `write_merge`, `write_delete`,
`write_clear`, `begin_group`, `end_group`, `flush_async`, `flush_through`,
`sequence`, `durable_sequence`, `lag_stats`, `shard_id`. Forty-nine lines whose
entire content is "the trait method is the inherent method."

Apply the deletion test to the **inherent** half. Delete it, and does complexity
reappear at callers? Production: no. Every production construction site boxes the
writer immediately —

```
frogdb-server/crates/core/src/shard/builder.rs:379
    (Some(rocks), Some(wal_config)) => Some(Box::new(RocksWalWriter::new(
```

— so the shard only ever holds `Box<dyn WalSink>` and can only call trait methods.
`grep -rn RocksWalWriter --include='*.rs'` outside `writer.rs` returns 40 hits: one
production construction (builder.rs:379, boxed), one re-export chain
(`persistence/src/lib.rs:39`, `wal/mod.rs:13`, `core/src/lib.rs:118`), nine
doc-comment mentions, and the rest are test constructions. **Zero production call
sites use the inherent methods.** The complexity that would "reappear" is a
`use frogdb_persistence::WalSink;` line in the handful of test modules that hold a
concrete `RocksWalWriter` — which is the correct outcome: the tests then cross the
same seam production does. *The interface is the test surface.*

The cost of the duplicate half is not the 49 lines, it is that the module has **two
interfaces to keep in agreement**, with nothing enforcing the agreement beyond
copy-paste. A method added to one half and forgotten on the other is a silent
divergence; today the only guard is `sink_tests::rocks_wal_writer_is_a_wal_sink`
(writer.rs:387–393), which proves the trait is *satisfied*, not that the two halves
say the same thing.

**`committed_sequence` (writer.rs:314–317).** Not on the trait at all — a
thirteenth, trait-less accessor:

```rust
pub fn committed_sequence(&self) -> u64 {
    self.outcomes.committed_sequence()
}
```

`grep -rn committed_sequence --include='*.rs' --include='*.md'` (37 hits) resolves to
four distinct things, and only one of them is this method:

| Symbol | Where | Callers |
|---|---|---|
| `FlushOutcomes::committed_sequence` (flush.rs:294) | engine-internal | flush.rs:355, 360, 372; writer.rs:292; ~20 test assertions |
| `WalLagStats.committed_sequence` (config.rs:114) | the published datum | writer.rs:292 populates it; fake.rs:283; observability aggregate keeps it per-shard |
| `DurableSyncTarget::committed_sequence` (rocks/mod.rs:37) | out-of-band-sync trait | rocks/mod.rs:336 |
| **`RocksWalWriter::committed_sequence` (writer.rs:315)** | **inherent** | **wal/tests.rs:2035 — that is all** |

The one caller is `sync_mode_accessors_report_the_writers_own_shard_and_watermarks`
(tests.rs:2011), which is **not FM-tagged** (nearest tags are line 1937 and line
2096) and asserts a value already reachable as
`wal.lag_stats().committed_sequence`. The spec's only mention of the name
(persistence-failure-modes.md:107) is explicitly of the *`WalLagStats` field*
— "reported separately as `WalLagStats::committed_sequence`" — which survives.

### PE7 — the commit-outcome block, written twice

`apply_clear` and `flush` are genuinely different **before** the commit: `flush`
short-circuits an empty batch, snapshots and resets `batch_size`/`batch_ops`/
`batch_max_seq`, and decrements the pending gauges (flush.rs:621–639); `apply_clear`
first drains as a barrier, then stages a range delete and counts the clear as a
write (flush.rs:560–579). After the commit they are the same routine, twice:

| Step | `apply_clear` | `flush` |
|---|---|---|
| `match self.sink.commit(self.is_sync)` | 586 | 640/642 |
| `outcomes.record_success(seq, self.is_sync)` | 588 | 644 |
| `lag.last_flush_timestamp_ms.store(current_timestamp_ms())` | 589–591 | 645–647 |
| `WalFlushDuration::observe(…, start.elapsed(), &shard_label)` | 592–596 | 649–653 |
| success `trace!` | 597 | 654–660 |
| `outcomes.record_failure(seq, ops, bytes, &e.to_string())` | 600–601 | 664–665 |
| `WalFlushFailures::inc` | 602 | 666 |
| `WalLostOps::inc_by` | 603 | 667 |
| `WalLostBytes::inc_by` | 604 | 668 |
| `self.log_failure(&e, ops, bytes, msg)` | 605–610 | 669–674 |

Four metric families emitted from two places each. The **locality** cost is the
point: "what a WAL commit records" is knowledge that lives in two textual places, so
a new counter, a changed label, or a fixed ordering has to be applied twice and
nothing catches the half-applied case. FM-PERSISTENCE-012 (spec:198–210) makes the
clear path's outcome recording load-bearing — "The durable sequence advances across
the clear, so a `Confirm` immediately after a clear is answerable" — and that
guarantee currently depends on a hand-copied block staying in step with its twin.

Two real deltas the extraction must preserve rather than "clean up":

1. **`last_flush` is assigned at different times.** `apply_clear` sets
   `self.last_flush = clock::now()` **before** `commit` (flush.rs:585); `flush` sets
   it **after** (flush.rs:641). `last_flush` drives the batch-timeout trigger, so
   this is a behavioral difference, not a stylistic one. The extraction keeps each
   call site's existing assignment point; the extracted function must not take it
   over silently.
2. **The log/trace text and the ops/bytes attribution differ.** The clear reports
   `(1, size_estimate)` with `"WAL shard clear committed"` / `"WAL clear flush
   failed; batch dropped"`; the batch reports `(flushed_ops, flushed_bytes)` with a
   richer `trace!` carrying `entries`/`bytes`/`duration_ms`. These stay as
   parameters — collapsing them would change operator-visible output.

### PE10 — three deletion tests, two pass, one is refused

**(a) `SnapshotStager.data_dir` — delete.** stager.rs:70–74:

```rust
/// The live data dir. Retained on the stager (the coordinator constructs it)
/// as the re-add anchor for a future search-sidecar copy; unread today since
/// the copy path was removed (proposal 23) — see [`SnapshotStager::run`].
#[allow(dead_code)]
pub(crate) data_dir: PathBuf,
```

Grepping `data_dir` across `persistence/src` confirms it: inside the stager the
name appears only in that field declaration and in prose (stager.rs:10, 99, 107,
124). It is constructed at rocks_coordinator.rs:171 and 261 and at tests.rs:262 and
949, and read nowhere. It is not an accident — proposal 23 deliberately kept it as a
"re-add anchor," documented across a 28-line comment (stager.rs:98–125).

That comment is the argument for deletion. Per CLAUDE.md, "if you need a
paragraph-long comment to justify why the workaround is OK, the code is wrong."
More directly: a field that exists so a *hypothetical future* consumer has somewhere
to plug in is the definition of a hypothetical seam — **one adapter means a
hypothetical seam; two means a real one**, and here there are zero. Git history is
the archive; proposal 23's re-add recipe (`SearchSidecar::copy_into`) is written down
in the same comment and in the proposal, and re-adding one `PathBuf` field is a
one-line change if it is ever needed. The `#[allow(dead_code)]` is currently
suppressing exactly the signal that would tell us the truth.

Scope note: the *prose* at stager.rs:98–125 explaining why the sidecar is not copied
stays (it documents a real, test-pinned exclusion,
`stager_excludes_search_sidecar`). Only the field, its `#[allow]`, the four
construction sites, and the final "`data_dir` is retained on the stager as that
anchor" sentence go.

**(b) `SnapshotScheduler::request()` and `::schedule()` — delete, with a spec edit.**
Both are legacy siblings of `request_mode`. Production callers, verified:

```
$ grep -rn "request_mode" --include='*.rs'
  snapshot/noop.rs:101:            match self.scheduler.request_mode(mode) {
  snapshot/rocks_coordinator.rs:202:        match self.scheduler.request_mode(mode) {
  snapshot/scheduler.rs:144  (definition)
  snapshot/tests.rs:702–722, and a doc reference in server/…/persistence_conn_command.rs:282

$ grep -rn "\.request()" --include='*.rs'
  snapshot/tests.rs:686, 688, 737, 772, 847, 881        ← tests only

$ grep -rn "\.schedule()" --include='*.rs'
  snapshot/tests.rs:815, 818, 828, 837                  ← tests only
```

Both coordinator adapters — the only two — go through `request_mode`. `request()` and
`schedule()` are kept alive purely by the tests that assert on them. `schedule()`'s
own doc comment says so: "Back the legacy `schedule_snapshot` protocol"
(scheduler.rs:191) — a protocol whose trait surface proposal 06/21 already deleted
(snapshot/tests.rs:823: "Relocated from the (deleted)
`NoopSnapshotCoordinator::schedule_snapshot` trait-surface tests").

This is the shallow-interface symptom on a module that is otherwise deep: the
scheduler's implementation is a genuinely tricky double-CAS handshake, but its
interface offers **three** entry points into the same state machine (`request`,
`request_mode`, `schedule`) where callers need one. Each extra entry point is another
combination a maintainer must reason about when changing the handshake, and — as
shown below — `request()` is not even a special case of `request_mode`; it is a
*fourth* behavior.

**The spec constraint (this is the part that makes it not a pure delete).** The tests
that pin `request()` / `schedule()` are FM-tagged and named in locked rows:

| Test (snapshot/tests.rs) | Uses | Named in |
|---|---|---|
| `test_scheduler_request_while_running_coalesces` (~683) | `request()` | FM-PERSISTENCE-015 `Forced by` (spec:252) |
| `test_scheduler_schedule_only_while_running` (~812) | `schedule()` | FM-PERSISTENCE-015 (spec:252) |
| `test_schedule_false` (~826) | `schedule()` | FM-PERSISTENCE-015 (spec:252) |
| `test_schedule_true` (~833) | `schedule()` | FM-PERSISTENCE-015 (spec:252) |
| `test_scheduler_request_after_finish_starts` (~766) | `request()` | FM-PERSISTENCE-016 `Forced by` (spec:264) |
| `test_scheduler_epoch_monotonic_across_cycle` (~843) | `request()` | FM-PERSISTENCE-016 (spec:264) |
| `test_scheduler_concurrent_request_storm` (~866) | `request()` | FM-PERSISTENCE-016 (spec:264) |

`just lint-failure-modes` (part of `just lint`) enforces spec↔test agreement in both
directions, so the spec edit is not optional and not deferrable — it lands in the
same commit.

**The behavioral subtlety that makes the rewrite load-bearing.** `request()` is *not*
`request_mode(Schedule)`. Compare scheduler.rs:124–134 with 144–152:

- `request()` first *loads* `in_progress`. On the idle branch it calls `try_begin`
  and, **if that CAS loses**, returns `Coalesced` **without arming** a follow-up
  (the "a fresh save began after our decision point" case, documented at
  scheduler.rs:121–123).
- `request_mode(Schedule)` calls `try_begin` unconditionally and routes *any* CAS
  failure into `arm_follow_up`, which arms `scheduled` before re-checking.

So porting a `request()` test to `request_mode(SnapshotMode::Schedule)` changes which
branch of the handshake it drives. This matters most for
`test_scheduler_concurrent_request_storm`, whose own comment states it is "the only
place the `finish_and_maybe_rebegin` re-CAS-failure branch … is exercised." The
rewrite must be verified to still reach that branch, not assumed to.

**(c) `RecoveredState.installed_staged_checkpoint` — deletion REFUSED.** See the
verification note below. It is the named `Outcome variant` of two locked rows and is
asserted by four tests. It is not dead in the sense the deletion test cares about:
deleting it would delete an *observable* from a locked contract, which is a
spec-first behavior change, not a cleanup. Out of scope here.

## Proposed change

### PE5 — one interface for the WAL sink

1. Move each of the twelve bodies from the inherent `impl RocksWalWriter` into
   `impl WalSink for RocksWalWriter`, deleting the delegating wrapper. The trait's
   doc comments (sink.rs) become the single place the interface is described; the
   per-method prose currently on the inherent methods (e.g. the `flush_through`
   contract at writer.rs:250–258, which is the richer of the two descriptions) moves
   with the body.
2. Delete `RocksWalWriter::committed_sequence` (314–317). Repoint the doc link at
   writer.rs:310 (`Use [Self::committed_sequence] for "reached storage"`) to
   `WalLagStats::committed_sequence`. Change tests.rs:2035 to
   `wal.lag_stats().committed_sequence`.
3. Add `use crate::wal::WalSink;` (or the crate-appropriate path) to the test modules
   that hold a concrete `RocksWalWriter`: `persistence/src/wal/tests.rs`,
   `core/src/persistence/tests.rs`, `core/src/persistence/crash_recovery_tests.rs`,
   `core/src/persistence/test_harness.rs`.
4. Keep `RocksWalWriter::new` and `Drop` inherent — construction is not part of the
   seam, and `new` takes concrete `RocksStore`/`WalConfig` arguments.
5. Keep `sink_tests::rocks_wal_writer_is_a_wal_sink` (writer.rs:387–393): it still
   pins object-safety, which is what `Box<dyn WalSink>` at builder.rs:379 needs.

Note on cost: after the move, the previously-inherent async calls from tests go
through `#[async_trait]`'s boxed futures. Production is unaffected — it already
calls through `dyn WalSink`, so it already pays that boxing.

### PE7 — `record_commit_outcome`

Add one private method on the flush engine, taking exactly the facts that differ:

```rust
/// Record the outcome of one committed batch: advance the committed (and, when
/// the commit fsynced, the durable) sequence on success, or count the loss on
/// failure. The single place a WAL commit turns into recorded state + metrics.
///
/// `ops`/`bytes` are what this commit is accountable for — the batch's totals
/// for a normal flush, `(1, size_estimate)` for a clear. `started` is the
/// commit's own start instant (`WalFlushDuration` is per-commit).
fn record_commit_outcome(
    &mut self,
    result: std::io::Result<()>,
    max_seq: u64,
    ops: usize,
    bytes: usize,
    started: Instant,
    labels: CommitLog,   // { success_trace: …, failure_msg: &'static str }
) -> std::io::Result<()>;
```

Both call sites keep their own pre-commit work, their own `self.last_flush`
assignment point (the delta noted above), and their own log text; `flush` keeps
returning the result to its waiter. The success `trace!` shapes differ enough
(`entries`/`bytes`/`duration_ms` vs `seq`) that the cleanest split is to leave the
two `trace!`/`error!` emissions at the call sites and have
`record_commit_outcome` own only the state + metric half — that is where all the
duplication actually is, and it keeps the extracted function free of
formatting concerns. Either shaping is acceptable; the requirement is that the four
metric families and the two `FlushOutcomes` calls appear **once**.

Exactly one behavior question to settle explicitly in the change (not silently):
whether `apply_clear`'s pre-commit `last_flush` assignment is intentional. Default
answer for this proposal: **preserve it verbatim**. If it is a latent bug, it is a
separate spec-first change against FM-PERSISTENCE-012.

### PE10 — the deletions

1. Delete `SnapshotStager.data_dir`, its `#[allow(dead_code)]`, the four construction
   sites (rocks_coordinator.rs:171, 261; tests.rs:262, 949 — plus the `data_dir`
   parameter threading at tests.rs:234–262 and 938–949 if it becomes unused), and the
   trailing "`data_dir` is retained on the stager as that anchor" sentence
   (stager.rs:124–125). Keep the rest of the proposal-23 exclusion prose.
   `stager_excludes_search_sidecar` continues to pin the exclusion.
2. Delete `SnapshotScheduler::request()` and `::schedule()`. Port their tests to
   `request_mode(SnapshotMode::Schedule)` / `try_begin` + `arm_follow_up`, choosing
   per test the call that drives the *same branch* the test was written to pin.
   `test_schedule_true` / `test_schedule_false` (relocated trait-surface leftovers
   asserting only "arms iff running") fold into
   `test_scheduler_schedule_only_while_running`'s successor rather than surviving as
   three near-duplicates.
3. **Spec edit, same commit:** rewrite the `Forced by` lists of FM-PERSISTENCE-015
   (spec:252) and FM-PERSISTENCE-016 (spec:264) to name the surviving tests. Neither
   row's `Trigger`/`Observable`/`NOT observable`/`Invariant` changes — the guarantees
   are unchanged, only the names of the tests forcing them. FM-PERSISTENCE-015's
   `Invariant` already describes `request_mode` and nothing else, so the spec text is
   already ahead of the code here.
4. Verify with `just lint-failure-modes` before anything else, then
   `just mutants-diff frogdb-persistence`.

## Testability improvement

- **PE5 collapses the test surface onto the production seam.** Today a test holding a
  concrete `RocksWalWriter` exercises the inherent method, while production
  exercises the trait method — different code paths, however trivially. After the
  move there is one body, so a test that passes proves the thing production runs.
  It also removes the standing risk that a method drifts on one half only.
- **PE5 removes an untested accessor** rather than adding coverage for it:
  `committed_sequence` had exactly one test and no production consumer, so the
  honest fix is deletion, and the datum stays asserted through `lag_stats()` —
  which *is* what INFO and the replication ack path read.
- **PE7 gives the commit-outcome contract one place to test.** Today "a failed clear
  increments `WalFlushFailures`/`WalLostOps`/`WalLostBytes` and records the failed
  sequence" and the same statement for a normal batch are two independent facts
  needing two tests, and a mutant that breaks only the clear copy can survive if the
  clear-failure path is thinner (which it is — the existing clear tests at
  tests.rs:572–594 and 923+ target FM-PERSISTENCE-012's barrier semantics, not its
  failure metrics). One function means one mutation target and one test that covers
  both callers' shared behavior, plus small per-caller tests for the parts that
  genuinely differ (attribution `(1, size)` vs `(ops, bytes)`; log text).
- **PE10 shrinks the scheduler's interface from four entry points to two**
  (`try_begin`/`request_mode` + the finish half), so the state machine's test matrix
  stops multiplying by an entry point no caller uses. Every FM-015/016 guarantee
  keeps a forcing test — through the seam production actually uses.
- **PE10(a) restores the dead-code signal.** With `#[allow(dead_code)]` gone from the
  stager, the compiler resumes reporting the next unused field instead of being
  pre-silenced.

## Risks / scope boundaries

### Locked-area obligations

- `frogdb-persistence` is LOCKED (spec header: "Phase 2 mutation gate passed …
  vs an 85% gate"). Run `just mutants-diff frogdb-persistence` before pushing.
  PE7 shifts mutation targets (two copies → one), so a previously-caught mutant may
  reappear as a new one on the extracted function; the forcing test must live in
  `frogdb-persistence` itself, since `cargo mutants -p` runs only that package's
  tests.
- No FM row may silently lose a forcing test. PE10's spec edit is a **required step**,
  not a follow-up. PE5 and PE7 touch no FM-named test (verified:
  `sync_mode_accessors_report_the_writers_own_shard_and_watermarks` is untagged; the
  clear/flush tests keep their names and behavior).
- PE7 sits under FM-PERSISTENCE-012's `Invariant` ("`apply_clear` commits what is
  staged, applies the range delete, and commits again"). The extraction must not
  reorder the three phases or move the range-delete staging.

### Boundaries vs sibling proposals in this round

| Sibling | Owns | Overlap with 41 | Resolution |
|---|---|---|---|
| **38** snapshot-layout-fs-read (PE1) | `snapshot/rocks_coordinator.rs` (112–124, 136–145, 257–259, 283), `snapshot/stager.rs` (133/136 name authoring), `fs_seam.rs` | **Same files** as PE10(a): the stager's path fields and the coordinator's two `SnapshotStager{…}` constructions | Different fields. 38 owns `snapshot_dir`/`tmp`/`final_dir`/`name` (the *layout*); 41 owns only `data_dir` (not a layout input — it is the live data dir). Land **41 first** (it is a one-field delete) so 38's `SnapshotLayout` is authored against a stager with no vestigial field. If 38 lands first, 41 rebases to the new constructor shape — the delete is unaffected either way. |
| **39** recovery-replay-driver (PE2) | `frogdb-core/src/.../store_recovery.rs:78–126`, new `persistence::recovery` | None. 41 does not touch core recovery or `RecoveredState` construction | — |
| **40** frame-codec-unify (PE3+PE4) | `wal/…/mod.rs:88–148` frame build/decode, `probabilistic.rs:513–540`, `dataset.rs:84–106` | None. 41 touches `wal/writer.rs`, `wal/flush.rs`, `wal/sink.rs` — no frame encode/decode | — |
| **42** rockssink-commit-effects (PE6) | `flush.rs:181–211` — `RocksSink::commit`, `spawn_clear_reclamation` + `record_wal_watermark`, returning `CommitEffects` | **Same file, adjacent concern.** 42 changes what `commit` *returns*; 41 changes what its two *callers* do with the return | Sequence **41 before 42** (the lane already marks 42 "sequence last"). 41 makes 42 strictly easier: after the extraction there is **one** place that consumes a commit result instead of two, so `CommitEffects` threads through a single function. If 42 landed first, 41 would have to duplicate the effects handling twice before deduplicating it. 41 must not touch `RocksSink::commit` or `record_wal_watermark`. |
| **43** recovery-wrappers-staged-checkpoint (PE8) | `frogdb-recovery` phase wrappers, `staged.rs`/`replication.rs`/`functions.rs`, checkpoint/cluster module fold | **`RecoveredState.installed_staged_checkpoint`** — PE10's third item | **Handed to 43.** 41 makes no change in `frogdb-recovery`. See the verification note: the field is spec-visible, so any change to it is a spec-first decision that belongs with 43's staged-checkpoint rerouting, which is already spec-adjacent. |
| **44** rocksstore-open-options (PE9) | `rocks/mod.rs` open shims, test-support shadow API, `columns.rs` | Minor: 41 *reads* `rocks/mod.rs:25–45` (`DurableSyncTarget`) as evidence but changes nothing there | 41 touches no file under `rocks/`. |

### Other risks

- **Cross-crate test churn (PE5).** Deleting the inherent methods forces `use
  WalSink` into four test modules across two crates (`frogdb-persistence`,
  `frogdb-core`). Mechanical, but it means the change does not compile
  crate-locally — plan a `just check frogdb-core` in the loop, not just
  `just check frogdb-persistence`.
- **`FlushOutcomes`' own delegation is NOT part of this.** `impl DurableSyncTarget
  for FlushOutcomes` (flush.rs:370–378) has the same
  `FlushOutcomes::committed_sequence(self)` shape, but the inherent method has four
  real in-crate callers (flush.rs:355, 360; writer.rs:292) and is `pub(super)`, so
  both halves earn their keep. Leave it.
- **PE10(b) is the only item with any chance of a behavior delta**, via the
  `request()` → `request_mode(Schedule)` branch difference described above. If the
  storm test cannot be made to reach the `finish_and_maybe_rebegin` re-CAS-failure
  branch through `request_mode`, keep `request()` (as `pub(super)`, like
  `arm_follow_up`, with its doc comment saying it exists to drive that branch
  deterministically) and delete only `schedule()`. That is a legitimate partial
  outcome, not a failure — a method kept because a test needs a specific internal
  seam is an **internal seam**, which is a real thing; a method kept because three
  tests happen to call it is not.

## Effort estimate

**S overall**, three separable commits:

| Item | Effort | Notes |
|---|---|---|
| PE5 | S | Mechanical code motion + four `use` lines + one test-assertion rewrite. Largest diff, lowest risk. |
| PE7 | S | One extracted function, two call sites. Needs care on the `last_flush` timing delta and the log text. |
| PE10(a) stager `data_dir` | XS | One field, four construction sites, one comment sentence. |
| PE10(b) scheduler legacy | S | The code delete is trivial; the test port + spec edit is the work, and the storm-test branch coverage must be re-verified. |
| Mutation re-gate | — | `just mutants-diff frogdb-persistence`, testbox-class workload. |

**Independently-landable hotfix: none.** Nothing here is a live bug — all three items
are duplication or dead interface, and every behavior stays byte-identical. The
closest thing to an urgent item is PE10(a), which is XS and can land on its own the
moment proposal 38's stager shaping is settled.

**Recommended landing order:** PE10(a) → PE5 → PE7 → PE10(b) (last, because it is the
only one carrying a spec edit and a coverage re-verification), all before proposal
42.
