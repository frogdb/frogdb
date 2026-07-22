# Proposal 21 — Back NoopSnapshotCoordinator with the real SnapshotScheduler, deleting its weaker hand-rolled copy

## Summary

`NoopSnapshotCoordinator` (`noop.rs`) hand-rolls its own copy of the three scheduling atomics
(`in_progress`, `scheduled`, `epoch`) and its own BGSAVE coalesce body — a **weaker copy** of the
state machine that `SnapshotScheduler` (`scheduler.rs`) already owns for the Rocks coordinator. The
copy is not merely redundant: its `request_snapshot` (`noop.rs:78-82`) is the *exact* naive body
`if in_progress { scheduled.store(true) }` that `scheduler.rs:72` documents as the lost-wakeup bug
and that [issue 08](../issues/08-snapshot-coalesce-lost-wakeup.md) closed **in the scheduler on
2026-07-20** — a fix that never reached the copy. The correctness-critical handshake now exists in
two places, one fixed and one not, kept in sync by nobody.

This proposal makes `NoopSnapshotCoordinator` hold an `Arc<SnapshotScheduler>` and delegate
`request_snapshot` / `in_progress` / the coalesce decision to it — exactly as
`RocksSnapshotCoordinator` already does — keeping only the genuinely noop-specific behaviour:
there is no disk work, so a Snapshot completes *synchronously and instantly* by draining the
scheduler's reschedule loop inline instead of on a spawned `run_loop`. The weaker copy is deleted;
the Noop path inherits the scheduler's exhaustive lost-wakeup tests (`tests.rs:479-678`) for free.

One correction to the exploration note is load-bearing (see Evidence): the Noop has **no reschedule
consumer** — it never calls `finish_and_maybe_rebegin` and runs no background loop — so its armed
`scheduled` flag is never drained into a follow-up Snapshot. The lost-wakeup is therefore *latent*
in the Noop today (an inert flag, not an observably dropped Snapshot); the defect this proposal
fixes is the **duplicated, diverged state machine**, not a live missed save. That distinction sets
the effort at S and the risk low.

## Problem (concrete verified evidence)

### The Noop re-implements the scheduler's atomics and its coalesce body

`NoopSnapshotCoordinator` carries its own copy of the three scheduling atomics
(`noop.rs:10-13`):

```rust
pub struct NoopSnapshotCoordinator {
    last_save: RwLock<Option<Instant>>,
    in_progress: Arc<AtomicBool>,   // Arc-wrapped ONLY to share with the completing handle
    epoch: AtomicU64,
    scheduled: AtomicBool,
}
```

These are the same `{ in_progress, scheduled, epoch }` triple that `SnapshotScheduler`
(`scheduler.rs:13-17`) owns and that `RocksSnapshotCoordinator` holds via
`scheduler: Arc<SnapshotScheduler>` (`rocks_coordinator.rs:26`). The Rocks adapter delegates every
scheduling decision to the scheduler (`try_begin` → `rocks_coordinator.rs:116`, `request` →
`rocks_coordinator.rs:142`, `finish_and_maybe_rebegin` → `rocks_coordinator.rs:253`,
`schedule`/`is_scheduled`/`in_progress` → `rocks_coordinator.rs:126,136,139`). The Noop instead
hand-rolls all of it against its private atomics.

### The copy is the pre-fix naive body the scheduler explicitly warns about

`NoopSnapshotCoordinator::request_snapshot` (`noop.rs:78-93`), coalesce branch verbatim
(`noop.rs:79-81`):

```rust
fn request_snapshot(&self) -> SnapshotRequest {
    if self.in_progress() {
        self.scheduled.store(true, Ordering::SeqCst);
        return SnapshotRequest::Coalesced;
    }
    ...
}
```

`SnapshotScheduler::request`'s own documentation (`scheduler.rs:70-77`) names this exact shape as
the bug:

> The naive body (`if in_progress { scheduled.store(true) }`) has a race: between our
> `in_progress.load() == true` and our `scheduled.store(true)`, the runner we observed can reach
> `finish_and_maybe_rebegin` and run its `scheduled.swap(false)` *before* our store lands —
> consuming nothing. It then exits with `in_progress == false`, and our `scheduled == true` arms no
> follow-up: a lost wakeup.

[Issue 08](../issues/08-snapshot-coalesce-lost-wakeup.md) (implemented 2026-07-20, all acceptance
criteria met) closed this window in `SnapshotScheduler::request` / `arm_follow_up`
(`scheduler.rs:99-146`) with the double-check protocol and pinned it with
`test_scheduler_arm_follow_up_after_runner_exit_starts` (`tests.rs:571-592`) plus the strengthened
500×6-thread storm (`tests.rs:627-678`). **That fix was never propagated to the Noop copy.** The
scheduler is now correct; `noop.rs:79-81` is still the pre-fix body. This is the divergence the
exploration note predicted, now realized in the tree: a correctness-critical state machine
duplicated, the copy silently left behind by a fix to the original.

### The copy is also *weaker than* the pre-fix original, because it has no consumer

The material nuance (a correction to the exploration note, which framed this as a live "known
lost-wakeup"): the Noop has **no `run_loop` and never calls `finish_and_maybe_rebegin`.** Its
`start_snapshot` (`noop.rs:31-49`) hands out a `completing` handle whose `Drop` clears
`in_progress` (`handle.rs:62-67`), modelling instant completion — but nothing ever consumes the
`scheduled` flag to *begin* the coalesced follow-up. So on the Noop:

- A `Coalesced` request arms `scheduled = true`, and that flag is read only by `is_scheduled`
  (`noop.rs:75-77`) — which [proposal 12](12-snapshot-coordinator-surface-trim.md) shows has zero
  production callers. It is **never** drained into a Snapshot.
- The `in_progress` window is vanishingly small: `request_snapshot`'s `Started` arm calls
  `start_snapshot` and immediately `drop`s the handle (`noop.rs:86-89`), toggling `in_progress`
  true→false synchronously. A concurrent caller observes `in_progress == true` only inside that
  race.

So the Noop's coalesce is effectively inert: it almost always `Started`s a fresh instant Snapshot,
and the rare `Coalesced` does nothing. The lost-wakeup cannot manifest as a dropped follow-up
*because there is no follow-up mechanism at all.* The harm today is therefore architectural, not
behavioural: **two copies of a subtle handshake, one fixed and one not**, and a Noop whose BGSAVE
coalesce semantics silently disagree with the Rocks path's. The runtime blast radius is small
(BGSAVE against a persistence-disabled server, where a save is meaningless —
`frogdb-server/crates/server/src/server/init.rs:285,290` wires the Noop with
`snapshot_handle = None`, so the periodic task never even runs against it), which is
exactly why this is a clean, low-risk deepening rather than an urgent bug.

### The Arc on `in_progress` exists only to feed the handle

`in_progress: Arc<AtomicBool>` (`noop.rs:11`) is Arc-wrapped for one reason: `start_snapshot` clones
it into `SnapshotHandle::completing(epoch, self.in_progress.clone())` (`noop.rs:43`) so the handle's
`Drop` can release it. This is the same RAII completion mechanism that [proposal
06](06-snapshot-scheduler.md) flagged as inert on the Rocks path (Rocks tracks completion via the
scheduler, not the handle) and that [proposal 12](12-snapshot-coordinator-surface-trim.md) proposes
trimming out of `SnapshotHandle`. Backing the Noop with a scheduler that owns `in_progress` removes
the last real user of that Arc-shared-flag mechanism — a direct synergy with 06 and 12.

## Why it is shallow/fragmented (architecture vocabulary)

**The correctness-critical Module was cloned instead of shared.** `SnapshotScheduler` is a deep
Module: a small Interface (`try_begin`, `finish_and_maybe_rebegin`, `request`, `schedule`) over a
subtle Implementation (the double-CAS lost-wakeup handshake). The Rocks adapter treats it as the
seam and delegates. The Noop, instead of consuming that Module, **re-implements its
Implementation** against private atomics — the opposite of leverage. The whole point of extracting
the scheduler (proposal 06) was to have *one* place the handshake lives and is tested; the Noop copy
quietly reintroduces the second place, and issue 08 proves the failure mode of that duplication:
a fix lands in one copy and the other rots.

**The Seam is drawn in the wrong place.** The right seam between "a coordinator" and "the schedule
state machine" is the `SnapshotScheduler` interface. Rocks sits on the correct side of that seam;
Noop reaches *through* it and rebuilds the machine. The two adapters therefore present the same
trait (`SnapshotCoordinator`) but only one is backed by the shared, tested core — an asymmetry
invisible at the trait boundary and discoverable only by reading both bodies.

**Low locality of the invariant.** "How does a BGSAVE coalesce with a running save?" now has two
answers in two files that must be read together to know they *disagree* (`noop.rs:78-82` naive vs
`scheduler.rs:99-146` fixed). After delegation there is one answer, in one place, already proven by
the storm test.

**Deletion test.** Delete `noop.rs`'s `scheduled` field, its `schedule_snapshot`/`is_scheduled`/
coalesce bodies, and its private `in_progress`/`epoch` atomics, and replace them with an
`Arc<SnapshotScheduler>`: no production behaviour is lost (the copy's only distinct behaviour is a
*bug*), and the Noop gains the scheduler's guarantee. The copy fails the deletion test — it is
surface that duplicates a deeper Module and diverges from it.

## Proposed design (Rust interface sketch — signatures/types only)

Make the Noop an adapter over `SnapshotScheduler`, mirroring the Rocks adapter, with instant
synchronous completion in place of a spawned `run_loop`.

```rust
// noop.rs — after
pub struct NoopSnapshotCoordinator {
    last_save: RwLock<Option<Instant>>,
    scheduler: Arc<SnapshotScheduler>,   // replaces in_progress + scheduled + epoch
}

impl NoopSnapshotCoordinator {
    pub fn new() -> Self;                 // scheduler: Arc::new(SnapshotScheduler::with_epoch(0))

    /// The one noop-specific move: a Snapshot has no disk work, so it completes
    /// instantly. Release the slot and drain any coalesced follow-up inline —
    /// the synchronous analogue of the Rocks `run_loop`'s
    /// `finish_and_maybe_rebegin` loop (`rocks_coordinator.rs:247-265`). Returns
    /// the last epoch that "ran".
    fn complete_instantly(&self, started_epoch: u64) -> u64;
}

impl SnapshotCoordinator for NoopSnapshotCoordinator {
    fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError>;   // try_begin → complete_instantly → SnapshotHandle::new(epoch)
    fn last_save_time(&self) -> Option<Instant>;                         // unchanged
    fn in_progress(&self) -> bool;                                       // self.scheduler.in_progress()
    fn request_snapshot(&self) -> SnapshotRequest;                       // self.scheduler.request(), draining on Started

    // The following three delegate to the scheduler IF proposal 12 has not yet
    // removed them from the trait (see Related). Each becomes a one-line forward:
    fn last_snapshot_metadata(&self) -> Option<SnapshotMetadata>;        // derive from last_save + scheduler.current_epoch()
    fn schedule_snapshot(&self) -> bool;                                 // self.scheduler.schedule()
    fn is_scheduled(&self) -> bool;                                      // self.scheduler.is_scheduled()
}
```

Behavioural shape of the two rewritten methods (bodies illustrative, not to be pasted):

```rust
fn request_snapshot(&self) -> SnapshotRequest {
    match self.scheduler.request() {          // <-- the FIXED handshake, shared with Rocks
        SnapshotRequest::Started(epoch) => {
            *self.last_save.write().unwrap() = Some(Instant::now());
            SnapshotRequest::Started(self.complete_instantly(epoch))
        }
        SnapshotRequest::Coalesced => SnapshotRequest::Coalesced,
    }
}

fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError> {
    let epoch = self.scheduler.try_begin().ok_or(SnapshotError::AlreadyInProgress)?;
    *self.last_save.write().unwrap() = Some(Instant::now());
    Ok(SnapshotHandle::new(self.complete_instantly(epoch)))   // bare epoch carrier — no completing handle
}
```

`complete_instantly` is the synchronous mirror of the Rocks `run_loop` tail: `while let Some(next) =
self.scheduler.finish_and_maybe_rebegin() { epoch = next; touch last_save; }`. Because completion is
now expressed *through the scheduler's `finish_and_maybe_rebegin`*, a `Coalesced` request that arms
`scheduled` **is** drained into a real (instant) follow-up run — closing on the Noop the very window
issue 08 closed on Rocks, for free.

### What this removes

- The private `in_progress: Arc<AtomicBool>`, `scheduled: AtomicBool`, `epoch: AtomicU64`
  (`noop.rs:11-13`) — subsumed by the scheduler.
- The hand-rolled coalesce body (`noop.rs:78-82`) and `schedule_snapshot` body (`noop.rs:68-74`).
- The last production use of `SnapshotHandle::completing` (`noop.rs:43`): the Noop now returns
  `SnapshotHandle::new(epoch)`, identical to the Rocks handle. This makes `SnapshotHandle`'s
  `completing` constructor and its `Drop`/`on_complete` apparatus **fully dead in production**,
  finishing what proposal 06's secondary finding started and reinforcing proposal 12's handle
  slim-down (the two can then collapse `SnapshotHandle` to `{ epoch }`).

## Migration plan (ordered steps)

1. **Add a scheduler-level regression pin first** (before touching the Noop): the Noop-relevant
   distinction is that `request()` never strands `scheduled == true` with no run. This is already
   covered by `test_scheduler_arm_follow_up_after_runner_exit_starts` and the storm test; add one
   focused `NoopSnapshotCoordinator` test asserting `request_snapshot` while a (now-instant) save is
   notionally running leaves no dangling `scheduled` flag — i.e. the delegation actually inherits
   the guarantee. Watch it fail against today's copy, pass after the swap.
2. **Swap the fields:** replace `in_progress`/`scheduled`/`epoch` in `NoopSnapshotCoordinator` with
   `scheduler: Arc<SnapshotScheduler>`; update `new()`.
3. **Rewire the methods:** `in_progress`/`request_snapshot`/`start_snapshot`/`schedule_snapshot`/
   `is_scheduled`/`last_snapshot_metadata` delegate to the scheduler as sketched; add the private
   `complete_instantly` drain helper. Drop the `Ordering`/`AtomicBool` imports that become unused.
4. **Return `SnapshotHandle::new`** from `start_snapshot`/`request_snapshot` instead of
   `completing`; drop the `self.in_progress.clone()` handle wiring.
5. **Update the affected unit tests** to the new observable model — see Test plan. Two groups
   change:
   - The persistence-crate Noop tests (`tests.rs:8-91`).
   - **Two `server`-crate BGSAVE tests** that depend on the removed "handle-held keeps the slot
     claimed" behaviour, both in
     `frogdb-server/crates/server/src/connection/persistence_conn_command.rs`:
     `bgsave_reports_already_in_progress` (line 244) and
     `bgsave_schedule_while_in_progress_schedules` (line 257). Both call `start_snapshot().unwrap()`
     then `std::mem::forget(handle)` to pin `in_progress == true`, then assert the second BGSAVE
     sees a save already running. Under instant completion the slot is released before
     `start_snapshot` returns, so `mem::forget` is inert and the second BGSAVE observes idle —
     `bgsave_reports_already_in_progress` gets `Background saving started` (handler line 122-129) and
     `bgsave_schedule_while_in_progress_schedules` gets `Started`→`Background saving started` with
     `is_scheduled() == false` (handler line 110-118). Both must be rewritten: either drop the
     `mem::forget` in-progress fixture (an instant-completion coordinator has no genuinely concurrent
     in-flight save to assert against), or reframe them around epoch monotonicity, mirroring the
     `test_noop_rejects_concurrent` update. This is the same deliberate semantic flip called out in
     Risks, now correctly accounted for in the `server` crate as well.
6. **`cargo check` / `just test frogdb-persistence snapshot` / `just test frogdb-server bgsave`** —
   the trait signature is unchanged, so there is no `server`-crate *compile* churn, but the two
   BGSAVE tests above are behaviourally coupled to the removed in-progress window and must be updated
   in the same change; the compiler drives the field/method edits in `noop.rs`.

## Test plan

- **New:** a Noop-level test that a coalescing request leaves no stranded `scheduled` flag at
  quiescence (the delegated lost-wakeup guarantee) — the acceptance guard for the swap.
- **Inherited for free:** `test_scheduler_arm_follow_up_after_runner_exit_starts` (`tests.rs:571`)
  and `test_scheduler_concurrent_request_storm` (`tests.rs:627`) now cover the Noop's coalesce path
  by construction, because it *is* the scheduler. This is the headline test win: the Noop path goes
  from an untested weaker copy to sharing the scheduler's exhaustive suite.
- **Updated existing Noop tests** (behaviour changes to flag, all in `tests.rs`):
  - `test_noop_coordinator` (`tests.rs:8-19`): today it asserts `in_progress()` is *true* while a
    handle is held and *false* after drop. Under instant completion the slot is released before
    `start_snapshot` returns, so `in_progress()` is false throughout. Rewrite to assert the epoch
    advances (`h.epoch() == 1`) and `last_save_time()` is set — the observable noop contract — not
    the handle-held in-progress window.
  - `test_noop_rejects_concurrent` (`tests.rs:20-28`): holding a handle no longer keeps the slot
    claimed (instant completion), so a second `start_snapshot` now succeeds at epoch 2 rather than
    returning `AlreadyInProgress`. This is a deliberate, correct semantic change (an instant-
    completion coordinator has no genuinely concurrent in-flight save); update the assertion to
    epoch monotonicity. Flag it explicitly in the PR as the one observable behaviour change.
  - `test_schedule_false`/`test_schedule_true` (`tests.rs:80-91`) and `test_noop_metadata`
    (`tests.rs:71-79`): keep as thin delegation smoke tests, or delete if landing after proposal 12
    removes those trait methods.
- **Updated `server`-crate BGSAVE tests** (behaviour changes, in
  `frogdb-server/crates/server/src/connection/persistence_conn_command.rs`) — these are *not*
  untouched, contrary to an earlier framing:
  - `bgsave_reports_already_in_progress` (`persistence_conn_command.rs:244`): pins
    `in_progress == true` via `start_snapshot().unwrap()` + `std::mem::forget(handle)`, then asserts
    the second BGSAVE returns `Background save already in progress`. Instant completion releases the
    slot before `start_snapshot` returns, so the second BGSAVE `Started`s instead and returns
    `Background saving started`. Rewrite to assert the instant-completion contract (each BGSAVE
    `Started`s and the epoch advances), dropping the `mem::forget` in-progress fixture.
  - `bgsave_schedule_while_in_progress_schedules` (`persistence_conn_command.rs:257`): same
    `mem::forget` fixture, then asserts `BGSAVE SCHEDULE` returns `Background saving scheduled` and
    `is_scheduled() == true`. Under instant completion the `SCHEDULE` request `Started`s a fresh
    instant save (`Background saving started`) with `is_scheduled() == false`. Rewrite around the new
    coalesce semantics or fold into an epoch-monotonicity assertion.
- **Full run:** `just test frogdb-persistence` (whole crate) plus `just test frogdb-server bgsave`
  for the two updated BGSAVE tests and a `just check` of `server` to confirm the unchanged trait
  signature still compiles. Per the repo's remote-execution policy, run the whole-crate/workspace
  passes on a Blacksmith testbox (`just tb-run`).

## Risks & alternatives

- **Observable behaviour change (the one real risk):** the "handle-held keeps the slot claimed"
  window disappears — a second immediate `start_snapshot` now succeeds instead of returning
  `AlreadyInProgress`, and a `mem::forget`-held handle no longer pins `in_progress == true`. This is
  correct for an instant-completion model. At *runtime* it only affects BGSAVE against a
  persistence-disabled server (Noop wiring at
  `frogdb-server/crates/server/src/server/init.rs:285,290`), where a save is a no-op anyway. But the
  same semantic flip is *observed by tests* in two crates, and those tests must be updated in this
  change (see Test plan / Migration step 5):
  - persistence crate: `test_noop_coordinator`, `test_noop_rejects_concurrent` (`tests.rs`).
  - `server` crate: `bgsave_reports_already_in_progress` and
    `bgsave_schedule_while_in_progress_schedules`
    (`frogdb-server/crates/server/src/connection/persistence_conn_command.rs:244,257`), which use
    `std::mem::forget(handle)` to pin the in-progress window this design removes. An earlier draft
    incorrectly claimed the `server` crate was untouched; it is not — the trait *signature* is
    unchanged (no compile churn), but these two behavioural tests break and are part of the change.
  Called out here so the reviewer signs off on it deliberately rather than discovering it as a
  "regression."
- **Alternative A — scheduler-backed RAII handle (rejected).** Keep the "`in_progress` true while
  the handle is held" model by having the returned handle call `finish_and_maybe_rebegin` on `Drop`.
  This preserves today's `in_progress` window but couples `SnapshotHandle` to `SnapshotScheduler`
  (the handle must hold an `Arc<SnapshotScheduler>` and drain on drop), *widening* the handle's
  surface at the exact moment proposals 06/12 are trying to shrink it. It also keeps a second
  completion path alive. Instant synchronous completion is the deeper choice: a coordinator with no
  background work has no reason to model an in-flight window, and it lets `SnapshotHandle` collapse
  to a bare epoch carrier shared with Rocks.
- **Alternative B — leave the copy, just port the issue-08 fix into `noop.rs` (rejected).** This
  fixes the immediate divergence but *entrenches* the duplication: two hand-maintained copies of the
  handshake forever, the next fix free to skip one again. It also does not remove the inert
  `scheduled`/`completing`/Arc-flag surface. Delegation removes the class of bug, not just this
  instance.
- **Interaction with proposal 12.** 12 proposes deleting `schedule_snapshot`/`is_scheduled`/
  `last_snapshot_metadata` from the trait. The two are complementary and order-independent: if 12
  lands first, the Noop simply doesn't implement those three and this proposal shrinks further; if
  21 lands first, they become trivial one-line scheduler delegations that make 12's deletion
  *obviously* safe (no bespoke logic to preserve). Neither blocks the other; note the shared files
  (`noop.rs`, `handle.rs`, and the `server`-crate `persistence_conn_command.rs` BGSAVE tests) to
  avoid a merge race if both are in flight.
- **Ordering preserved.** No new atomics and no ordering choices are introduced — all sequencing
  moves *into* the already-`SeqCst` scheduler. This is code deletion plus delegation, not a
  concurrency redesign.
- **ADR / glossary alignment.** Uses the canonical **Snapshot** (forkless RocksDB **Checkpoint**),
  **Snapshot Epoch** (`SnapshotScheduler`'s monotonic counter, `frogdb-server/CONTEXT.md:67-71`),
  and **Store** vocabulary. Touches no ADR: `0001-raft-cluster-metadata` (data path never through
  Raft) and `0002-single-database` are unaffected — this is confined to the `persistence` crate's
  snapshot module and changes no cross-crate signatures. Respects crate dependency direction:
  `persistence` already owns `SnapshotScheduler`; nothing new is imported.

## Effort

**S.** One file substantively (`noop.rs`), plus test updates in **two crates**: ~4 Noop tests in the
persistence crate's `tests.rs`, and two BGSAVE tests in the `server` crate
(`persistence_conn_command.rs:244,257`). The trait *signature* is unchanged, so there is no
`server`-crate *compile* ripple and the scheduler is reused as-is — but the design's observable
semantic flip (a `mem::forget`-held handle no longer pins `in_progress`) breaks those two `server`
BGSAVE tests, so they are part of the change, not untouched. The care points are the deliberate
test-contract updates across both crates (`test_noop_coordinator`, `test_noop_rejects_concurrent`,
`bgsave_reports_already_in_progress`, `bgsave_schedule_while_in_progress_schedules`) and confirming
the instant-completion drain releases the slot on every arm. Still ~S — the edits are mechanical and
the production blast radius is a persistence-disabled server where BGSAVE is a no-op — but scoped as
persistence + two `server`-crate test edits, not persistence-only.

## Related

- [Proposal 06 — Extract a pure SnapshotScheduler](06-snapshot-scheduler.md): created the Module this
  proposal makes the Noop consume; its Risks section explicitly flagged "`Noop` coordinator
  alignment … could share `SnapshotScheduler`, which would also let it drop its completion stash" as
  deferred scope. This is that follow-up.
- [Issue 08 — Close the SnapshotScheduler coalesce lost-wakeup window](../issues/08-snapshot-coalesce-lost-wakeup.md):
  fixed the handshake in the scheduler (2026-07-20) but not in the Noop copy — the concrete
  divergence this proposal eliminates.
- [Proposal 12 — Trim the SnapshotCoordinator surface](12-snapshot-coordinator-surface-trim.md):
  complementary; together they shrink both the trait surface (12) and the Noop's implementation
  (21), and jointly render `SnapshotHandle`'s `completing`/`on_complete`/`Drop` apparatus fully dead
  in production.

## Adversarial review

**Verdict: AMEND** — architectural case CONFIRMED against source; one substantive defect (a
blast-radius / effort miscount) plus one minor path-citation nit. Both fixed in place.

- **Major — "zero server-crate ripple" / S-effort framing missed two breaking server tests
  (resolved).** The review verified that two `server`-crate tests in
  `frogdb-server/crates/server/src/connection/persistence_conn_command.rs` —
  `bgsave_reports_already_in_progress` (line 244) and
  `bgsave_schedule_while_in_progress_schedules` (line 257) — pin `in_progress == true` via
  `start_snapshot().unwrap()` + `std::mem::forget(handle)`, then assert the second BGSAVE sees a save
  already running (`is_scheduled() == true`). Confirmed against source: the proposed instant-
  completion `start_snapshot` drains `finish_and_maybe_rebegin` inline and returns
  `SnapshotHandle::new(epoch)`, releasing the slot before the call returns, so `mem::forget` is inert
  and both tests observe idle instead (`Background saving started` / `is_scheduled() == false` per
  handler lines 122-129 and 110-118). The original text called the `server` crate untouched and rated
  the change S on that basis. Resolved by (a) adding both tests to Migration step 5 and the Test plan
  as required updates, (b) retracting the "zero server-crate ripple" claim in Risks and Effort and
  reframing it as "no compile ripple, but two behavioural test edits," and (c) keeping the effort at
  ~S while re-scoping it as persistence + two `server`-crate test edits. The semantic flip is the
  *same one* already owned in Risks (`mem::forget` no longer holds `in_progress`); the amendment
  extends that accounting to the `server` crate.

- **Minor — `init.rs` cited without full path (resolved).** Both citations of `init.rs:285,290` now
  read `frogdb-server/crates/server/src/server/init.rs:285,290`.

The reviewer's notes confirm the core premise against source (noop.rs:78-82 is the verbatim pre-fix
naive body; scheduler.rs:70-98/121-146 implement the issue-08 double-check fix; Rocks delegates while
the Noop hand-rolls private atomics), the deletion test, the crate direction, and the instant-
completion design's feasibility and inherited lost-wakeup guarantee. No borrow-checker or wire-compat
issue was raised. The architectural case stands; proceeding after the migration/test/effort/risk
amendments above.
