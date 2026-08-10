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
  `SnapshotStager.data_dir` (stager.rs:74) is `#[allow(dead_code)]` and read by
  nobody — but it is the *tail* of a four-level, three-package cascade
  (`RocksSnapshotCoordinator::new`'s parameter → coordinator field → `SnapshotRun`
  field → stager field), so the delete is **S**, not the one-field XS an earlier
  draft claimed. `SnapshotScheduler::request()` / `schedule()`
  (scheduler.rs:124–134, 191–199) are legacy siblings of `request_mode`, with zero
  production callers. **`RecoveredState.installed_staged_checkpoint` is NOT dead** —
  it is the named `Outcome variant` of two locked spec rows (see the verification
  note). Deleting it is refused here; proposal 43 independently confirms it stays.

None of the three changes behavior. All three are inside the **locked** persistence
area (`frogdb-persistence` + `frogdb-recovery`, gate 0.85), so they carry a mutation
re-gate; PE10 additionally carries a **narrow, required spec edit**, because two of
the tests it deletes are named in an FM `Forced by` list and `just
lint-failure-modes` fails the moment a spec row names a test that no longer exists.

## Files involved

| Path | Lines | Role in this proposal |
|---|---|---|
| `frogdb-server/crates/persistence/src/wal/writer.rs` | 394 | PE5: inherent `impl` + the 12-method `WalSink` delegation `impl` (333–381); `committed_sequence` (314–317) |
| `frogdb-server/crates/persistence/src/wal/sink.rs` | 53 | PE5: the `WalSink` trait — the surviving interface. Unchanged, but it is the seam the change consolidates onto |
| `frogdb-server/crates/persistence/src/wal/flush.rs` | 827 | PE7: `apply_clear` (559–613) and `flush` (620–678); `FlushOutcomes::record_success`/`record_failure` (271–290) |
| `frogdb-server/crates/persistence/src/wal/tests.rs` | 2343 | PE5: ~30 concrete-typed call sites; the lone `wal.committed_sequence()` caller (2035); the seven test-only `batch_size_threshold()` / `set_batch_size_threshold()` callers (118, 121, 122, 1593, 1597, 1601, 1614). **No new import** — `use super::*` (tests.rs:6) already re-exports `WalSink` via `wal/mod.rs:12` |
| `frogdb-server/crates/persistence/src/snapshot/scheduler.rs` | 212 | PE10: `request()` (124–134) and `schedule()` (191–199) beside `request_mode` (144–152) |
| `frogdb-server/crates/persistence/src/snapshot/stager.rs` | 347 | PE10: the `data_dir` field (70–74). The sidecar-exclusion prose (98–123) **stays**; only the trailing anchor sentence (124–125) goes |
| `frogdb-server/crates/persistence/src/snapshot/rocks_coordinator.rs` | 348 | PE10: the rest of the `data_dir` cascade — `new`'s parameter (50), the coordinator field (43, set 96), `SnapshotRun.data_dir` (224, set 171), the `execute` local (251), the `SnapshotStager` literal (261) |
| `frogdb-server/crates/persistence/src/snapshot/tests.rs` | 1456 | PE10: the scheduler tests that pin `request()` / `schedule()` (686–689, 737, 772, 815–841, 881); the `stager` / `stager_on` helpers' `data_dir` parameter (234, 240, 251, 262) with 12 call sites; the `coordinator` helper's `data_dir` parameter (938, 949) with 13 call sites |
| `frogdb-server/crates/core/src/persistence/tests.rs` | — | PE5: concrete-typed `wal.write_set` / `wal.flush_async` (147–148). **No new import** — `use crate::persistence::*` (tests.rs:8) re-exports `WalSink` through `persistence/mod.rs:13`'s glob of `frogdb_persistence::*` |
| `frogdb-server/crates/core/src/persistence/crash_recovery_tests.rs` | — | PE5: concrete-typed calls at 1704, 1711, 1763–1764 — **needs `WalSink` added to the explicit `use super::wal::{DurabilityMode, WalConfig};` at line 26** (this file has no glob). PE10: `RocksSnapshotCoordinator::new`'s fourth argument (48, `db_path`) |
| `frogdb-server/benchmarks/benches/persistence.rs` | 350 | **PE5, third package (`frogdb-benches`).** Constructs concrete `RocksWalWriter` (202, 252, 301) and calls `wal.write_set` (220, 270, 319); needs `WalSink` added to the explicit `use frogdb_core::persistence::{…}` at line 11–13. Compiled by neither `just check frogdb-persistence` nor `just check frogdb-core` |
| `frogdb-server/crates/server/src/server/init.rs` | — | PE10: the **production** `RocksSnapshotCoordinator::new` call site (297), fourth argument `config.persistence.data_dir.clone()` (300) |
| `frogdb-server/crates/server/src/connection/persistence_conn_command.rs` | — | PE10: `#[cfg(test)]` fixture call site (357), fourth argument at 365 |
| `.scratch/hardening/specs/persistence-failure-modes.md` | — | PE10: **one** edit — drop `test_schedule_true` and `test_schedule_false` from FM-PERSISTENCE-015's `Forced by` (line 252). FM-PERSISTENCE-016 (line 264) needs **no** edit |
| `frogdb-server/crates/recovery/src/lib.rs` | 270 | **Read-only here.** `installed_staged_checkpoint` (126) is spec-visible; proposal 43 confirms it stays (see Risks) |

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
doc-comment mentions, and the rest are test and benchmark constructions. **Zero
production call sites use the inherent methods.**

The complexity that would "reappear" is exactly **two `use`-list additions**, verified
against the imports each caller already has:

| Module holding a concrete `RocksWalWriter` | Calls trait methods on it? | Import edit |
|---|---|---|
| `persistence/src/wal/tests.rs` | yes | none — `use super::*` (line 6) already brings `WalSink` in from `wal/mod.rs:12` |
| `core/src/persistence/tests.rs` | yes (147–148) | none — `use crate::persistence::*` (line 8) globs `frogdb_persistence::*`, which exports `WalSink` (`persistence/src/lib.rs:40`) |
| `core/src/persistence/crash_recovery_tests.rs` | yes (1704, 1711, 1763–1764) | **add `WalSink`** to `use super::wal::{DurabilityMode, WalConfig};` (line 26) |
| `core/src/persistence/test_harness.rs` | **no** — `create_wal_writer` (134) only constructs and returns | none (see the defect note below) |
| `benchmarks/benches/persistence.rs` | yes (220, 270, 319) | **add `WalSink`** to `use frogdb_core::persistence::{…}` (11–13) |

That is the correct outcome: the callers then cross the same seam production does.
*The interface is the test surface.*

Defect found while verifying: `CrashTestHarness::create_wal_writer`
(`core/src/persistence/test_harness.rs:134`) has **zero callers** anywhere in the
tree, hidden by the file-level `#![allow(dead_code)] // Many helpers available for
future tests` at line 6. It is the same smell PE10(a) condemns, one crate over and at
file scope rather than field scope. Out of scope for 41 (it is `frogdb-core`, not the
locked persistence crates, and it is not on the PE5 path), but it is a standing
deletion-test candidate worth its own item.

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

1. **`last_flush` is assigned at different times — accidental copy-paste drift,
   preserved for safety.** `apply_clear` sets `self.last_flush = clock::now()`
   **before** `commit` (flush.rs:585); `flush` sets it **after** (flush.rs:641).
   `last_flush` is the `Instant` field at flush.rs:395 that feeds
   `since_last_flush()` (441) and therefore the batch-timeout arm of `should_flush`
   (492). Nothing distinguishes the two paths, so this reads as drift rather than
   design — but the drift's direction is known and benign, which is why it is
   preserved rather than "fixed" here: setting it early makes the post-clear timeout
   window short by one commit's duration, so the next timeout-triggered flush fires
   slightly early. Never late, never skipped. In the common case it is invisible
   anyway: `apply_clear`'s own barrier flush (flush.rs:561) runs `flush`, which
   stamps `last_flush` at 641, and 585 then re-stamps it microseconds later.
   The extraction keeps each call site's existing assignment point verbatim; the
   extracted function must not take it over silently. This is **not** promoted to a
   spec-first change — there is no observable to write a row against.
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

Grepping `data_dir` across `persistence/src` confirms the field is never read: inside
the stager the name appears only in that declaration and in prose (stager.rs:10, 99,
107, 124). It is not an accident — proposal 23 deliberately kept it as a "re-add
anchor" (`03e7f39f^:.scratch/arch-deepening/proposals/23-search-sidecar-layout.md`;
the file was pruned in 03e7f39f, so cite it through git).

The argument for deletion is the zero-reader argument, and it stands on its own: a
field that exists so a *hypothetical future* consumer has somewhere to plug in is the
definition of a hypothetical seam — **one adapter means a hypothetical seam; two
means a real one**, and here there are zero. Git history is the archive; proposal
23's re-add recipe (`SearchSidecar::copy_into`) is written down in the surviving
comment and in the proposal, and re-adding the field is mechanical if it is ever
needed. The `#[allow(dead_code)]` is currently suppressing exactly the signal that
would tell us the truth. No ADR check is owed — `adr/` holds only 0001–0004, and none
of them speaks to the sidecar.

**The cascade — this is why it is not a one-field delete.** The stager field is the
tail of a four-level chain, verified end to end:

```
RocksSnapshotCoordinator::new(…, data_dir: PathBuf)     rocks_coordinator.rs:50   ← 4 external call sites
  → RocksSnapshotCoordinator.data_dir                   rocks_coordinator.rs:43 (set 96)
    → SnapshotRun.data_dir                              rocks_coordinator.rs:224 (set 171)
      → let data_dir = self.data_dir.clone()            rocks_coordinator.rs:251
        → SnapshotStager.data_dir                       rocks_coordinator.rs:261 → stager.rs:74
```

Deleting the stager field makes every level above it unread in turn, all the way up to
the `new` parameter, whose four call sites live in **three packages**:

| Call site | Package | Kind |
|---|---|---|
| `server/src/server/init.rs:297` (arg at 300, `config.persistence.data_dir.clone()`) | `frogdb-server` | **production** |
| `server/src/connection/persistence_conn_command.rs:357` (arg at 365) | `frogdb-server` | `#[cfg(test)]` fixture |
| `core/src/persistence/crash_recovery_tests.rs:48` (arg at 56, `db_path`) | `frogdb-core` | test |
| `persistence/src/snapshot/tests.rs:941` (arg at 949) | `frogdb-persistence` | test |

Two branches are available, and this proposal picks one explicitly:

- **(chosen) Propagate the deletion to the top.** Drop the parameter and every
  argument expression. Nothing further cascades: `init.rs`'s
  `config.persistence.data_dir` is a live config field used elsewhere,
  `persistence_conn_command.rs`'s `db_dir` is retained as `_db_dir`, and
  `crash_recovery_tests.rs`'s `db_path` is also `RocksStore::open`'s argument. Inside
  `snapshot/tests.rs` the `data_dir` parameter also disappears from the `stager` /
  `stager_on` helpers (234, 240, 251, 262; 12 call sites) and the `coordinator`
  helper (938, 949; 13 call sites) — the tests that genuinely need a live data dir
  (`stager_excludes_search_sidecar`, via `write_search_sidecar`) keep their local
  path, the rest drop it.
- **(rejected) Stop the cascade with `#[allow(dead_code)]` on the coordinator
  field.** One line, no cross-crate churn — and it relocates the smell one level up
  rather than removing it, leaving the same suppressed signal behind a different
  struct. That is not a deletion test, it is a deletion *gesture*.

Choosing propagation re-prices this item from XS to **S**: one parameter, three
fields, one local, four external call sites, two test-helper signatures and ~25
helper call sites, spread over three packages. It is still entirely mechanical and
still behavior-free, but it is not a one-liner and must not be scheduled as one.

Scope note: the *prose* at stager.rs:98–123 explaining why the sidecar is not copied
stays (it documents a real, test-pinned exclusion,
`stager_excludes_search_sidecar`) — only lines 124–125, the "`data_dir` is retained
on the stager as that anchor" sentence, concern the field. The field's own
justification is the three-line doc comment at stager.rs:70–72.

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

| Test (snapshot/tests.rs) | Uses | Named in | Fate |
|---|---|---|---|
| `test_scheduler_request_while_running_coalesces` (683) | `request()` (686, 688) | FM-PERSISTENCE-015 `Forced by` (spec:252) | ported in place, name kept |
| `test_scheduler_schedule_only_while_running` (813) | `schedule()` (815, 818) | FM-PERSISTENCE-015 (spec:252) | ported in place, name kept |
| `test_schedule_false` (826) | `schedule()` (828) | FM-PERSISTENCE-015 (spec:252) | **deleted** (folds in) |
| `test_schedule_true` (834) | `schedule()` (837) | FM-PERSISTENCE-015 (spec:252) | **deleted** (folds in) |
| `test_scheduler_finish_with_pending_reschedule_reruns_once` (734) | `request()` (737) | FM-PERSISTENCE-016 `Forced by` (spec:264) | ported in place, name kept |
| `test_scheduler_request_after_finish_starts` (768) | `request()` (772) | FM-PERSISTENCE-016 (spec:264) | ported in place, name kept |
| `test_scheduler_epoch_monotonic_across_cycle` (844) | `request()` (847) | FM-PERSISTENCE-016 (spec:264) | ported in place, name kept |
| `test_scheduler_concurrent_request_storm` (866) | `request()` (881) | FM-PERSISTENCE-016 (spec:264) | ported in place, name kept |

`just lint-failure-modes` (part of `just lint`) enforces spec↔test agreement in both
directions, so the spec edit is not optional and not deferrable — it lands in the
same commit. But it is **narrow**, and deliberately so: porting a test's call site
does not rename it, so only the two genuinely-deleted names move.

- **FM-PERSISTENCE-016 (spec:264): no edit at all.** All four of its
  `request()`-using tests keep their names.
- **FM-PERSISTENCE-015 (spec:252): remove exactly two names** — `test_schedule_true`
  and `test_schedule_false`. The other ten entries in that list are untouched.

Rewriting more of a LOCKED row than the code forces is itself a risk: the row's
`Trigger` / `Observable` / `NOT observable` / `Invariant` / `Outcome variant` fields
describe guarantees this proposal does not change, and every gratuitous word changed
there is a word a future reader has to re-derive. Two names out; nothing else.

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
3. Add `WalSink` to exactly two existing `use` lists — `core/src/persistence/
   crash_recovery_tests.rs:26` and `benchmarks/benches/persistence.rs:11–13`. The
   other three modules holding a concrete `RocksWalWriter` need nothing (see the
   import table under *Problem*).
4. **Enumerate what survives on the inherent `impl` — all four of them.** After the
   twelve trait methods and `committed_sequence` move out, `impl RocksWalWriter`
   still holds:

   | Survivor | Line | Verdict |
   |---|---|---|
   | `new` | 30 | **Keep.** Construction is not part of the seam; `new` takes concrete `RocksStore` / `WalConfig` arguments and every caller must know them. |
   | `batch_size_threshold` | 96 | **Condemn — test-only.** |
   | `set_batch_size_threshold` | 102 | **Condemn — test-only.** |
   | `Drop` (separate `impl`, 323) | 323 | **Keep.** A trait impl of its own; not part of the duplication. |

   `batch_size_threshold` / `set_batch_size_threshold` fail the same deletion test
   PE5 applies to everything else. Every caller is a test: `wal/tests.rs:118, 121,
   122, 1593, 1597, 1601, 1614`. Production never retunes the threshold through
   them — it hands the writer a *shared* `Arc<AtomicUsize>` at construction
   (`WalConfig::batch_size_threshold_handle`, `wal/config.rs:85`, adopted at
   `wal/writer.rs:60–62`), wired from `server/src/server/init.rs:405` and driven live
   by `CONFIG SET` through `runtime_config.rs:3065–3070`. So these two are exactly
   the shape PE5 condemns: **a second interface kept alive by tests**, this time a
   mutating one that lets a test change a value production reaches by a different
   route entirely.

   The honest resolution is to delete both and have the seven test call sites read
   and store the shared atomic directly — the same handle production uses — which
   also makes `test_wal_adopts_shared_batch_size_threshold_handle`
   (`tests.rs:1576`) assert against the mechanism rather than a proxy for it. The
   only assertion that cannot do this trivially is `tests.rs:1614`, which constructs a
   writer with **no** handle (`solo`) and asserts the writer minted its own atomic
   from `WalConfig::batch_size_threshold`; that assertion is about a real production
   fallback (`writer.rs:62`), so if it cannot be expressed without the accessor, keep
   `batch_size_threshold` as `pub(super)` with a doc comment saying it exists to
   observe the mint-vs-adopt fallback, and delete `set_batch_size_threshold`
   unconditionally. A getter kept for one specific internal observation is an
   internal seam; a setter kept because tests find it convenient is not.

   **Spec caution, found while verifying:** both callers are FM-tagged. The seven
   call sites belong to exactly two tests — `test_wal_batch_threshold_live_retune`
   (`tests.rs:91`, tag at 83) and `test_wal_adopts_shared_batch_size_threshold_handle`
   (`tests.rs:1576`, tag at 1569) — and **both are named in FM-PERSISTENCE-013's
   `Forced by` list** (spec:219). This is an in-place call-site port, so both keep
   their names and FM-013 needs no edit; but it means this sub-item is not the
   spec-free cleanup the rest of PE5 is, and `just lint-failure-modes` must be run
   after it. If the port cannot preserve what FM-013 forces — "the writer *adopts*
   the shared threshold cell, and a live set retunes the flush thread without a
   restart" — abandon the sub-item and leave both accessors; PE5's main claim does
   not depend on it.
5. Keep `sink_tests::rocks_wal_writer_is_a_wal_sink` (writer.rs:387–393): it still
   pins object-safety, which is what `Box<dyn WalSink>` at builder.rs:379 needs.

Note on cost: after the move, the previously-inherent async calls from tests go
through `#[async_trait]`'s boxed futures. Production is unaffected — it already
calls through `dyn WalSink`, so it already pays that boxing.

**Benchmark discontinuity — call this out in the commit message.**
`benchmarks/benches/persistence.rs` currently calls `wal.write_set` on a *concrete*
`RocksWalWriter` in three hot loops (220, 270, 319), so today it measures the
inherent method with no boxing. After PE5 the same loops go through
`#[async_trait]`'s boxed future — a per-write heap allocation the benchmark did not
previously pay. The published `persistence` bench numbers will shift, and the shift
is an artifact of the benchmark being brought onto the production path, not a
regression in production. Anyone comparing across this commit needs to know that.
Note also that the alternative — leaving the benchmark on the inherent methods — is
worse: it would mean the benchmark measures a code path production never runs.

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

1. Delete the whole `data_dir` cascade, top to bottom: the `SnapshotStager.data_dir`
   field and its `#[allow(dead_code)]` (stager.rs:73–74), `SnapshotRun.data_dir`
   (rocks_coordinator.rs:224) with its assignment (171), local (251) and stager-literal
   argument (261), the coordinator field (43, 96), the `new` parameter (50), and the
   fourth argument at all four call sites (init.rs:300,
   persistence_conn_command.rs:365, crash_recovery_tests.rs:56, snapshot/tests.rs:949).
   Then the two test-helper signatures — `stager` / `stager_on` (tests.rs:234, 251)
   and `coordinator` (tests.rs:938) — and their ~25 call sites. Finally the trailing
   "`data_dir` is retained on the stager as that anchor" sentence
   (stager.rs:124–125). Keep the rest of the proposal-23 exclusion prose;
   `stager_excludes_search_sidecar` continues to pin the exclusion, and keeps its own
   local data dir because `write_search_sidecar` writes into it.
2. Delete `SnapshotScheduler::request()` and `::schedule()`. Port their tests to
   `request_mode(SnapshotMode::Schedule)` / `try_begin` + `arm_follow_up`, choosing
   per test the call that drives the *same branch* the test was written to pin, and
   **keeping every ported test's name** — that is what holds the spec edit to two
   lines. `test_schedule_true` / `test_schedule_false` (relocated trait-surface
   leftovers asserting only "arms iff running") fold into
   `test_scheduler_schedule_only_while_running` rather than surviving as three
   near-duplicates; they are the only two names that disappear.
3. **Spec edit, same commit — two names, one row.** Remove `test_schedule_true` and
   `test_schedule_false` from FM-PERSISTENCE-015's `Forced by` (spec:252). Change
   nothing else in that row, and nothing at all in FM-PERSISTENCE-016 (spec:264).
   FM-PERSISTENCE-015's `Invariant` already describes `request_mode` and nothing
   else, so the spec text is already ahead of the code here — which is another reason
   not to touch it.
4. Verify with `just lint-failure-modes` and `just lint-gates` before anything else,
   then `just mutants-diff frogdb-persistence`. `lint-gates` is the compile-free
   seam-gate family and is cheap; it matters here because **PE7** relocates four
   typed metric emissions (`WalFlushDuration`, `WalFlushFailures`, `WalLostOps`,
   `WalLostBytes`) and a `current_timestamp_ms()` read into a new function, and
   `lint-metrics-chokepoint` and `lint-clock-seam` are exactly the chokepoints those
   two kinds of call must pass through. (`current_timestamp_ms` at flush.rs:90
   already routes through `clock::system_now()`, and `flush.rs` carries no
   `clock-seam.py` allowlist entry, so the expectation is a clean pass — the point is
   to find out before pushing, not after.)
5. The compile loop spans **three** packages, so a crate-scoped check is not
   sufficient for PE5: `just check frogdb-persistence`, then `just check
   frogdb-core`, then a build of the benches (`frogdb-benches` — neither of the
   first two compiles `benchmarks/benches/persistence.rs`), and `just check
   frogdb-server` for PE10's `init.rs` / `persistence_conn_command.rs` call sites.

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
- **PE10(a) restores the dead-code signal, and shortens three constructor
  interfaces.** With `#[allow(dead_code)]` gone from the stager, the compiler resumes
  reporting the next unused field instead of being pre-silenced. The cascade
  deletion also removes a parameter from `RocksSnapshotCoordinator::new` and from the
  `stager` / `stager_on` / `coordinator` test helpers — every one of those is a fact
  a caller currently has to know and supply for no effect, which is interface weight
  in the exact sense this vocabulary uses. Four test files stop threading a path that
  goes nowhere.

## Risks / scope boundaries

### Locked-area obligations

- `frogdb-persistence` is LOCKED (spec header: "Phase 2 mutation gate passed …
  vs an 85% gate"). Run `just mutants-diff frogdb-persistence` before pushing.
  PE7 shifts mutation targets (two copies → one), so a previously-caught mutant may
  reappear as a new one on the extracted function; the forcing test must live in
  `frogdb-persistence` itself, since `cargo mutants -p` runs only that package's
  tests.
- No FM row may silently lose a forcing test. PE10's spec edit is a **required step**,
  not a follow-up. **Correction to an earlier draft: PE5 does touch FM-named tests**,
  in one place — the `batch_size_threshold` / `set_batch_size_threshold` sub-item
  rewrites call sites inside `test_wal_batch_threshold_live_retune` and
  `test_wal_adopts_shared_batch_size_threshold_handle`, both named in
  FM-PERSISTENCE-013's `Forced by` (spec:219). Names are preserved, so no spec edit
  is owed, but `just lint-failure-modes` is not optional after PE5 either. The rest of
  PE5 and all of PE7 touch no FM-named test (verified:
  `sync_mode_accessors_report_the_writers_own_shard_and_watermarks` — the lone
  `committed_sequence` caller — is untagged, nearest tags at tests.rs:1937 and 2096;
  the clear/flush tests keep their names and behavior).
- PE7 sits under FM-PERSISTENCE-012's `Invariant` ("`apply_clear` commits what is
  staged, applies the range delete, and commits again"). The extraction must not
  reorder the three phases or move the range-delete staging.

### Boundaries vs sibling proposals in this round

| Sibling | Owns | Overlap with 41 | Resolution |
|---|---|---|---|
| **38** snapshot-layout-fs-read (PE1, REVISED) | New `snapshot/layout.rs`; `snapshot/rocks_coordinator.rs` (107–152 read half, 256–266 stager literal, 283); `snapshot/stager.rs` (61–84 fields, plus the literal/parser sites); `fs_seam.rs` (additive); `snapshot/tests.rs` (`stager_on`, 249–267) | **Real, and larger than the earlier draft admitted.** Per the cascade above, 41 is no longer a one-field delete: it edits `rocks_coordinator.rs` at 43, 50, 96, 171, 224, 251 **and** 261, and it changes the `stager` / `stager_on` / `coordinator` test-helper signatures. 38 restructures the same file's read half and rewrites the same `SnapshotStager` literal (256–266, of which 41 deletes 261) and the same `stager_on` helper (249–267, of which 41 deletes 262 plus the parameter). Direct textual collision: the stager literal and the test helper. Adjacent-but-disjoint: 38 does not touch `new`'s signature (46–51) or the coordinator's field list (34–44); 41 does not touch the layout fields or the parsers. | **The "land 41 first, it's a one-field delete" rationale is void** — 41(a) is S and cross-package, so it buys 38 nothing to wait for, and 38's own copy of that rationale (38's boundary table, 41 row) needs the same correction. Either order works mechanically; pick one and rebase, do not run them concurrently. Recommended: **land 38 first.** It has the larger, more structural diff in `rocks_coordinator.rs` / `stager.rs` / `stager_on`, and 41's deletion is a pure subtraction that rebases onto whatever shape 38 leaves (the `data_dir` field survives 38 untouched, since it is not a layout input). Reversing that makes 38 re-derive its line numbers against a moved constructor for no benefit. Whichever lands second re-verifies its cited line numbers before starting — every citation in both proposals for `rocks_coordinator.rs` above line 43 and `stager.rs` above line 74 shifts. |
| **39** recovery-replay-driver (PE2) | `frogdb-core/src/.../store_recovery.rs:78–126`, new `persistence::recovery` | None. 41 does not touch core recovery or `RecoveredState` construction | — |
| **40** frame-codec-unify (PE3+PE4) | `wal/…/mod.rs:88–148` frame build/decode, `probabilistic.rs:513–540`, `dataset.rs:84–106` | None. 41 touches `wal/writer.rs`, `wal/flush.rs`, `wal/sink.rs` — no frame encode/decode | — |
| **42** rockssink-commit-effects (PE6, REVISED) | `flush.rs:181–211` — `RocksSink::commit`, `spawn_clear_reclamation` + `record_wal_watermark`, returning `CommitEffects`; a new `WriteSink::settle` | **Same file, same lines.** 42 changes what `commit` *returns*; 41 changes what its two *callers* do with the return | Sequence **41 before 42** (the lane already marks 42 "sequence last"), and 42's revised text now agrees explicitly: its Risks section fixes the inherited signature — once PE7 lands, `record_commit_outcome` is widened from `result: io::Result<()>` to `result: io::Result<CommitEffects>` and `self.sink.settle(effects)` moves **inside** it, on the `Ok` arm before `record_success`. **Consequence to record: every `flush.rs` line 42 cites shifts once PE7 lands** — 42's `586`/`588`/`640`/`644`, its `559–613` and `620–678` ranges, and its `621–623` early-return citation all move or disappear into the extracted function. Implementation must therefore either (a) serialize 41 → *re-verify every 42 citation against the post-41 tree* → 42, or (b) assign both proposals to a single agent who carries the line numbers across. Do not hand 42 to a fresh agent working from its current citations. 41 must not touch `RocksSink::commit` or `record_wal_watermark`. |
| **43** recovery-wrappers-staged-checkpoint (PE8) | `frogdb-recovery` phase wrappers, `staged.rs`/`replication.rs`/`functions.rs`, checkpoint/cluster module fold | **`RecoveredState.installed_staged_checkpoint`** — PE10's third item | **43 confirms it stays.** 43's own text rules "**Do not delete `RecoveredState.installed_staged_checkpoint`**" (43:402–407) on the same grounds 41 reaches independently, so this is agreement between two proposals rather than a hand-off of an open question. 41 makes no change in `frogdb-recovery`. |
| **44** rocksstore-open-options (PE9) | `rocks/mod.rs` open shims, test-support shadow API, `columns.rs` | Minor: 41 *reads* `rocks/mod.rs:25–45` (`DurableSyncTarget`) as evidence but changes nothing there | 41 touches no file under `rocks/`. |

### Other risks

- **Cross-package churn (PE5).** Deleting the inherent methods reaches three
  packages: `frogdb-persistence` (no import edit), `frogdb-core` (one `use`-list
  addition in `crash_recovery_tests.rs:26`), and `frogdb-benches` (one in
  `benchmarks/benches/persistence.rs:11–13`). It therefore does not compile
  crate-locally, and — the trap — **neither `just check frogdb-persistence` nor
  `just check frogdb-core` builds the benchmark crate at all**, so a green
  single-crate loop proves nothing about it. Build the benches explicitly before
  pushing. PE10 adds a fourth package to the check loop (`frogdb-server`, for the
  two `RocksSnapshotCoordinator::new` call sites).
- **`FlushOutcomes`' own delegation is NOT part of this.** `impl DurableSyncTarget
  for FlushOutcomes` (flush.rs:370–378) has the same
  `FlushOutcomes::committed_sequence(self)` shape as the `RocksWalWriter` wrapper PE5
  deletes, but it survives the same criterion PE5 applies: the inherent method
  (flush.rs:294, `pub(super)`) has **three production callers of the inherent half** —
  flush.rs:355, flush.rs:360, writer.rs:292 — so it is load-bearing independently of
  the trait. (flush.rs:372 is the trait delegation itself and is not evidence of
  anything; the earlier draft's count of "four" double-counted it.) Both halves earn
  their keep. Leave it.
- **PE10(b) is the only item with any chance of a behavior delta**, via the
  `request()` → `request_mode(Schedule)` branch difference described above. If the
  storm test cannot be made to reach the `finish_and_maybe_rebegin` re-CAS-failure
  branch through `request_mode`, keep `request()` (as `pub(super)`, like
  `arm_follow_up`, with its doc comment saying it exists to drive that branch
  deterministically) and delete only `schedule()`. That is a legitimate partial
  outcome, not a failure — a method kept because a test needs a specific internal
  seam is an **internal seam**, which is a real thing; a method kept because five
  tests happen to call it is not.

## Effort estimate

**S–M overall**, four separable commits:

| Item | Effort | Notes |
|---|---|---|
| PE5 | S | Mechanical code motion + **two** `use`-list additions + one test-assertion rewrite, across three packages. Largest diff, lowest risk. The `batch_size_threshold` accessor question (step 4) is the only judgement call. |
| PE7 | S | One extracted function, two call sites. Needs care on the `last_flush` timing drift and the log text. Adds a `lint-gates` obligation (relocated metric emissions + clock read). |
| PE10(a) `data_dir` cascade | **S** (was mis-estimated XS) | Not one field: one `new` parameter, three fields, one local, four external call sites across three packages, two test-helper signatures, ~25 helper call sites. Mechanical throughout, but it is a rebase-sensitive diff that collides with proposal 38. |
| PE10(b) scheduler legacy | S | The code delete is trivial; the test port is the work, and the storm-test branch coverage must be re-verified. The spec edit is two names on one row. |
| Mutation re-gate | — | `just mutants-diff frogdb-persistence`, testbox-class workload. |

**Independently-landable hotfix: none.** Nothing here is a live bug — all four items
are duplication or dead interface, and every behavior stays byte-identical (modulo the
benchmark's boxing discontinuity, which is measurement, not production).

**Recommended landing order:** PE5 → PE7 → PE10(b) → PE10(a), with PE7 before proposal
42 and PE10(a) **after** proposal 38. PE10(a) moves to the end precisely because it is
no longer the cheap opener the earlier draft assumed: it is the one item with a real
file-level collision against a sibling, so it should land when 38's shaping is
settled rather than racing it. PE10(b) stays late among the rest because it is the
only item carrying a spec edit and a coverage re-verification.
