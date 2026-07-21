# Proposal 12 — Trim the SnapshotCoordinator surface: delete vestigial trait methods and collapse SnapshotHandle

## Summary

Prior rounds extracted the pure `SnapshotScheduler` and fixed the coalesce lost-wakeup (see
[proposal 06](06-snapshot-scheduler.md)), but they left the `SnapshotCoordinator` **Interface**
carrying three methods that no production code path calls — `schedule_snapshot`, `is_scheduled`,
`last_snapshot_metadata` — each of which both adapters (`RocksSnapshotCoordinator`,
`NoopSnapshotCoordinator`) must still implement. Every one fails a **deletion test**: remove it and
no production behavior changes, only a handful of tests that exercise the constructor they call.
Separately, `SnapshotHandle` is a shallow four-state value whose production callers read *only*
`.epoch()`; its `noop()` constructor and `is_noop()` accessor are test-only, and its `complete()`
method is redundant with `Drop`.

This proposal deletes the three dead trait methods (demoting `is_scheduled` to the scheduler's own
API, where the unit tests already live), unifies the two BGSAVE modes so the coalesce decision reads
from **one** seam instead of splitting a `Result`-match (plain BGSAVE) against an enum-match (BGSAVE
SCHEDULE), and strips `SnapshotHandle` to its real state (`epoch` + an optional completion flag).
The one non-obvious finding — verified below — is that plain BGSAVE and BGSAVE SCHEDULE differ
**deliberately** (no-queue vs queue), so unifying them is *not* a free swap onto `request_snapshot`;
it needs a third request outcome to preserve the no-queue semantics.

## Files involved (verified paths + current line counts)

| File | Lines | Role in the current design |
| --- | --- | --- |
| `frogdb-server/crates/persistence/src/snapshot/mod.rs` | 51 | `SnapshotCoordinator` trait (L40-51, 7 methods) + `SnapshotRequest` enum (L32-39) |
| `frogdb-server/crates/persistence/src/snapshot/handle.rs` | 68 | `SnapshotHandle` — 3 constructors (`noop`/`new`/`completing`), `is_noop()`, `complete()`, `Drop` |
| `frogdb-server/crates/persistence/src/snapshot/rocks_coordinator.rs` | 265 | Rocks adapter; impls all 7 trait methods (L113-150) incl. the 3 vestigial ones (L128-140) |
| `frogdb-server/crates/persistence/src/snapshot/noop.rs` | 94 | Noop adapter; impls all 7 (L30-93) incl. `schedule_snapshot`/`is_scheduled`/`last_snapshot_metadata` (L56-77) |
| `frogdb-server/crates/persistence/src/snapshot/scheduler.rs` | 169 | Pure state machine; already owns `is_scheduled()` (L162) + `schedule()` (L150) |
| `frogdb-server/crates/persistence/src/snapshot/tests.rs` | 678 | Only caller of the vestigial trait methods; also the only `noop()`/`is_noop()`/`complete()` caller |
| `frogdb-server/crates/server/src/connection/persistence_conn_command.rs` | 304 | `handle_bgsave` (L102-133): plain BGSAVE → `start_snapshot()`, BGSAVE SCHEDULE → `request_snapshot()` |
| `frogdb-server/crates/server/src/server/startup.rs` | — | Periodic task: `start_snapshot()` (L29), reads `handle.epoch()` (L31) |
| `frogdb-server/crates/server/src/connection/info_handler.rs` | — | Reads `in_progress()` (L163) + `last_save_time()` (L153) — the load-bearing readers |

## Problem (concrete verified evidence)

### Three trait methods with zero production callers

A workspace grep for each method (excluding trait/impl definitions) gives exact caller counts:

**`schedule_snapshot` — 0 production callers.** The only calls are `tests.rs:83` and `tests.rs:89`,
both on a `NoopSnapshotCoordinator`. Both adapters still implement it (`rocks_coordinator.rs:135`,
`noop.rs:68`), and the Rocks impl just forwards to `self.scheduler.schedule()`. It is a public trait
method whose sole reason to exist is a legacy protocol the caller no longer uses — BGSAVE SCHEDULE
was migrated to `request_snapshot` in a prior round.

**`is_scheduled` — 0 production callers; 1 test-assertion caller.** The one call through the *trait*
is `persistence_conn_command.rs:269`, inside `bgsave_schedule_while_in_progress_schedules`, a unit
test asserting the follow-up was armed. Every other `is_scheduled` in the tree
(`tests.rs:512, 514, 525, 530, 584, 591, 599, 602, 661`) is called on the **pure `SnapshotScheduler`**,
not the trait — that is where the scheduler's own unit tests already live. So the trait method exists
only to let one BGSAVE test peek at internal state that the scheduler already exposes.

**`last_snapshot_metadata` — 0 production callers.** No `server` crate code reads it; a grep for
`last_snapshot_metadata` and for `.to_metadata()` across `crates/server` returns nothing. The only
calls are `tests.rs:74` and `tests.rs:76` (`test_noop_metadata`), which test the accessor it calls.
The Rocks impl (`rocks_coordinator.rs:128-134`) maps `last_metadata` through `to_metadata()` for a
consumer that does not exist; INFO reports save state via `last_save_time()` + `in_progress()`
instead (`info_handler.rs:153, 163`).

Contrast the methods that **are** load-bearing and must stay: `in_progress()` (read at
`info_handler.rs:163`, `startup.rs:24`, `subsystems.rs:596-598`), `last_save_time()`
(`persistence_conn_command.rs:139`, `info_handler.rs:153`), `start_snapshot()` (`startup.rs:29`,
`persistence_conn_command.rs:122`), and `request_snapshot()` (`persistence_conn_command.rs:110`).

### The coalesce decision is split across two seam shapes

`handle_bgsave` (`persistence_conn_command.rs:102-133`) answers the same question — "start a save, or
fold into the running one?" — through **two different return shapes** depending on one arg:

```rust
// BGSAVE SCHEDULE — reads the coalesce decision as an enum from the seam:
return match ctx.snapshot_coordinator.request_snapshot() {
    SnapshotRequest::Coalesced      => /* "Background saving scheduled" */,
    SnapshotRequest::Started(epoch) => /* "Background saving started"   */,
};
// plain BGSAVE — reconstructs the same decision from a Result + error variant:
match ctx.snapshot_coordinator.start_snapshot() {
    Ok(handle)                          => /* "Background saving started"        */,
    Err(SnapshotError::AlreadyInProgress) => /* "Background save already in progress" */,
    Err(e)                              => /* "ERR {e}" */,
}
```

One path reads a purpose-built `SnapshotRequest` enum; the other re-derives "already running" from a
`Result::Err(AlreadyInProgress)` and manufactures a `SnapshotHandle` it only uses to log
`handle.epoch()` (L124). Two vocabularies for one decision, chosen by a caller-side `if`.

### VERIFIED: the two BGSAVE modes differ on purpose — plain does NOT queue

This is the finding that governs the design. The behaviours, traced through the scheduler:

- **Plain BGSAVE** → `start_snapshot()` → `scheduler.try_begin()`. If a save runs, `try_begin`
  returns `None` → `AlreadyInProgress` → `"Background save already in progress"`. It **does not arm
  a follow-up** — no `scheduled` flag is set, no extra snapshot is queued.
- **BGSAVE SCHEDULE** → `request_snapshot()` → `scheduler.request()`. If a save runs, it **arms a
  coalesced follow-up** (`scheduled = true`, closing the lost-wakeup window) → `Coalesced` →
  `"Background saving scheduled"`, guaranteeing a post-request snapshot.

This queue-vs-no-queue split is the intended Redis semantics: without `SCHEDULE`, BGSAVE refuses when
a save is in progress; with `SCHEDULE`, it defers/queues instead of failing. (FrogDB deviates from
Redis in two minor, orthogonal ways worth flagging, not fixing here: plain BGSAVE returns a `+Simple`
status string rather than Redis's `-ERR Background save already in progress`; and Redis's SCHEDULE
specifically defers around an *AOF rewrite*, a mechanism FrogDB has no analogue for — FrogDB queues
around an in-flight *snapshot* instead. The "queue vs no-queue" intent matches; the mechanism does
not.)

**What this means for the proposed unification:** you cannot naively "route plain BGSAVE through
`request_snapshot`." `request_snapshot` *arms a follow-up*; plain BGSAVE must not. A naive swap would
silently give plain BGSAVE the SCHEDULE behaviour — every plain BGSAVE issued during a running save
would queue an extra snapshot, a real behavioural regression. Unifying the seam therefore requires a
**third outcome** that expresses "a save is already running, nothing was queued" (see Proposed
change), not a straight substitution.

### SnapshotHandle is a shallow four-state value read for one field

Verified usage map for `SnapshotHandle`:

| Member | Production callers | Test-only callers |
| --- | --- | --- |
| `new(epoch)` | `rocks_coordinator.rs:120` | `tests.rs:59` |
| `completing(epoch, flag)` | `noop.rs:43` | `tests.rs:35, 50` |
| `noop()` | **none** | `tests.rs:66` |
| `.epoch()` | `noop.rs:87`, `startup.rs:31`, `persistence_conn_command.rs:124` | several |
| `.is_noop()` | **none** | `tests.rs:60, 67` |
| `.complete()` | **none** (Noop uses `drop(handle)` at `noop.rs:88`) | `tests.rs:38` |

So of the type's public surface — three constructors, `epoch()`, `is_noop()`, `complete()`, `Drop` —
production exercises exactly two constructors and `epoch()`. The `is_noop` field is written by every
constructor but read only by tests; the `noop()` constructor has no production caller at all; and
`complete()` is functionally identical to the existing `Drop` (both `store(false)` the flag), with no
production caller — `noop.rs:88` releases the flag via `drop(handle)`, not `complete()`.

## Why it is shallow/fragmented (architecture vocabulary)

**Three interface methods that fail the deletion test.** A deep **Module** offers a small
**Interface** over substantial **Implementation**. `SnapshotCoordinator` does the opposite for these
three methods: `schedule_snapshot`, `is_scheduled`, and `last_snapshot_metadata` widen the interface
of *both* adapters without gating any behaviour a caller depends on. By Ousterhout's deletion test,
an interface member whose removal changes zero production behaviour is not an interface — it is
surface area. Here it is worse than decoration: it is a **tax on every adapter**, because a trait
method must be implemented by both `Rocks` and `Noop` (and any future coordinator) even though no one
calls it.

**Depth was added below but the surface above was never trimmed.** Proposal 06 correctly pushed the
coalesce state machine *down* into the pure `SnapshotScheduler`, which now owns `is_scheduled()`,
`schedule()`, `try_begin()`, `request()`, and the double-CAS handshake with its own exhaustive unit
tests (`tests.rs:479-670`). But the trait above still re-exports `is_scheduled` and `schedule_snapshot`
as pass-throughs — the old shallow surface left standing on top of the new deep core. The scheduler
is the real **Module**; the trait forwarders are a vestigial **Adapter** layer over an API the
scheduler already exposes at the right depth.

**The coalesce **Seam** is duplicated by return shape.** "Did we start a save, or fold into one?" is
one decision, but it is expressed as a `SnapshotRequest` enum on the SCHEDULE path and as
`Result<SnapshotHandle, SnapshotError>` on the plain path. Two adapters over one seam that never
meet: the caller bridges them with an `if opt == "SCHEDULE"`. **Locality** is poor — to know how
BGSAVE decides to coalesce you must read both a `match` on an enum and a `match` on a `Result`, and
know that `AlreadyInProgress` (an *error*) and `Coalesced` (a *success*) describe the same physical
state (a save is running) with opposite valence.

**`SnapshotHandle` has low **Leverage**.** The type models four states (`noop` / production / instant-
completing / completed) to serve a production surface of one field. The `is_noop` discriminant exists
so a caller can branch on adapter identity — but no production caller does; branching on which
coordinator you hold is exactly the coupling a trait is meant to remove. `complete()` duplicates
`Drop`. The value carries more shape than any consumer reads.

## Proposed change

Three coordinated edits in the `snapshot` module plus one BGSAVE caller, all compiler-guided.

### 1. Delete the three vestigial trait methods

Remove `schedule_snapshot`, `is_scheduled`, and `last_snapshot_metadata` from the
`SnapshotCoordinator` trait (`mod.rs:44-46`) and from both adapters (`rocks_coordinator.rs:128-140`,
`noop.rs:56-77`). The scheduler retains `schedule()` and `is_scheduled()` as its own API (they are
already there, already unit-tested at `tests.rs:595-604`); the deleted trait methods were only thin
forwarders. The two BGSAVE tests that reached through the trait move to the scheduler:

- `tests.rs:80-91` (`test_schedule_false` / `test_schedule_true`) → drive `SnapshotScheduler`
  directly (they are testing coalesce arming, which is scheduler behaviour, not coordinator surface).
- The `is_scheduled()` assertion at `persistence_conn_command.rs:269` → assert the BGSAVE SCHEDULE
  *response* (`"Background saving scheduled"`), which is the observable contract; internal-flag
  verification belongs to the scheduler's own tests.
- `test_noop_metadata` (`tests.rs:71-79`) is deleted with the method it tests.

### 2. Unify the BGSAVE seam with a third request outcome (semantics preserved)

Extend `SnapshotRequest` (`mod.rs:32-39`) with a variant for the no-queue "already running" case, and
give the coordinator a single `request_snapshot(mode)` entry point:

```rust
pub enum SnapshotMode {
    /// Plain BGSAVE: start if idle, else report already-running WITHOUT queuing.
    Immediate,
    /// BGSAVE SCHEDULE: start if idle, else coalesce a single follow-up.
    Schedule,
}

pub enum SnapshotRequest {
    Started(u64),   // claimed the slot; a save at this epoch is running
    Coalesced,      // a save was running; a follow-up is armed (Schedule only)
    AlreadyRunning, // a save was running; nothing queued (Immediate only)
}
```

The scheduler grows one method that keeps the no-queue path as a *scheduler outcome*, not a
caller-side `Result` check — plain BGSAVE's "ERR if a save runs" is expressed here, once:

```rust
pub fn request_mode(&self, mode: SnapshotMode) -> SnapshotRequest {
    match (self.try_begin(), mode) {
        (Some(epoch), _)              => SnapshotRequest::Started(epoch),
        (None, SnapshotMode::Schedule)  => self.arm_follow_up(), // Coalesced (or Started on race)
        (None, SnapshotMode::Immediate) => SnapshotRequest::AlreadyRunning,
    }
}
```

`handle_bgsave` collapses to one match over one seam — no `Result`, no manufactured handle:

```rust
let mode = if is_schedule { SnapshotMode::Schedule } else { SnapshotMode::Immediate };
match ctx.snapshot_coordinator.request_snapshot(mode) {
    SnapshotRequest::Started(epoch) => { tracing::info!(epoch, "BGSAVE started");
        Response::Simple(Bytes::from_static(b"Background saving started")) }
    SnapshotRequest::Coalesced =>
        Response::Simple(Bytes::from_static(b"Background saving scheduled")),
    SnapshotRequest::AlreadyRunning =>
        Response::Simple(Bytes::from_static(b"Background save already in progress")),
}
```

`start_snapshot()` stays on the trait (the periodic task at `startup.rs:29` still wants a
`SnapshotHandle` to log its epoch), but BGSAVE no longer routes through it — the coalesce decision now
lives entirely behind `request_snapshot`, and the `AlreadyInProgress` *error* is no longer overloaded
to mean "a save is running" at a call site.

### 3. Strip SnapshotHandle to `{ epoch, on_complete }`

Delete the `is_noop` field, the `noop()` constructor, `is_noop()`, and `complete()`. Keep `new(epoch)`
(Rocks), `completing(epoch, flag)` (Noop), `epoch()`, and `Drop` (which already releases the flag —
`noop.rs:88` relies on `drop`, not `complete`). The type becomes exactly "epoch + optional completion
flag," its true state.

## Before / After

### Before — trait carries 7 methods, 3 with no production caller (`mod.rs:40-51`)

```rust
pub trait SnapshotCoordinator: Send + Sync {
    fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError>;
    fn last_save_time(&self) -> Option<Instant>;
    fn in_progress(&self) -> bool;
    fn last_snapshot_metadata(&self) -> Option<SnapshotMetadata>; // 0 prod callers
    fn schedule_snapshot(&self) -> bool;                          // 0 prod callers
    fn is_scheduled(&self) -> bool;                               // 0 prod callers
    fn request_snapshot(&self) -> SnapshotRequest;
}
```

### After — 5 methods, all load-bearing; BGSAVE reads one seam

```rust
pub trait SnapshotCoordinator: Send + Sync {
    fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError>; // periodic task
    fn last_save_time(&self) -> Option<Instant>;                       // LASTSAVE, INFO
    fn in_progress(&self) -> bool;                                     // INFO, drain, periodic
    fn request_snapshot(&self, mode: SnapshotMode) -> SnapshotRequest; // BGSAVE [SCHEDULE]
}
```

Both adapters shed three method bodies each; `Noop` drops `schedule_snapshot`/`is_scheduled`/
`last_snapshot_metadata` (`noop.rs:56-77`), and `Rocks` drops the three forwarders
(`rocks_coordinator.rs:128-140`). The scheduler is unchanged except for the added `request_mode`.

### Before — SnapshotHandle: four states, one read (`handle.rs:17-68`)

```rust
pub struct SnapshotHandle { epoch: u64, is_noop: bool, on_complete: Option<Arc<AtomicBool>> }
impl SnapshotHandle {
    pub fn noop() -> Self { … }                 // test-only
    pub fn new(epoch: u64) -> Self { … }
    pub fn completing(epoch, in_progress) { … }
    pub fn epoch(&self) -> u64 { … }
    pub fn is_noop(&self) -> bool { … }         // test-only
    pub fn complete(mut self) { … }             // test-only; == Drop
}
impl Drop for SnapshotHandle { … }
```

### After — SnapshotHandle: epoch + optional completion flag

```rust
pub struct SnapshotHandle { epoch: u64, on_complete: Option<Arc<AtomicBool>> }
impl SnapshotHandle {
    pub fn new(epoch: u64) -> Self { Self { epoch, on_complete: None } }
    pub fn completing(epoch: u64, in_progress: Arc<AtomicBool>) -> Self {
        Self { epoch, on_complete: Some(in_progress) }
    }
    pub fn epoch(&self) -> u64 { self.epoch }
}
impl Drop for SnapshotHandle { /* store(false) if on_complete */ }
```

## Testability improvement

**The coalesce assertions move to the type that owns them.** Today the only test that the coalesce
follow-up is armed reaches through the trait method `is_scheduled` from a *BGSAVE command* test
(`persistence_conn_command.rs:258-270`), coupling a server-crate command test to a persistence-crate
internal flag. After the change, that invariant is asserted where it belongs — the scheduler's own
exhaustive suite (`tests.rs:479-670`, already present) — and the command test asserts only the
observable RESP response. Deleting `schedule_snapshot`/`is_scheduled`/`last_snapshot_metadata` from
the trait deletes their per-adapter test doubles (`tests.rs:71-91`) with no loss of coverage: the
scheduler tests already cover `schedule()`/`is_scheduled()` behaviour directly.

**The third BGSAVE outcome becomes a first-class, socket-free test.** `SnapshotMode::Immediate` while
a save runs → `AlreadyRunning` **without** arming `scheduled`, versus `SnapshotMode::Schedule` →
`Coalesced` **with** `scheduled` armed, is a two-line synchronous unit test on `SnapshotScheduler`
(no coordinator, no Tokio, no disk):

```rust
let s = SnapshotScheduler::with_epoch(0);
assert!(matches!(s.request_mode(Immediate), Started(1)));
assert!(matches!(s.request_mode(Immediate), AlreadyRunning)); // no-queue preserved
assert!(!s.is_scheduled());                                    // <-- the regression guard
assert!(matches!(s.request_mode(Schedule),  Coalesced));
assert!(s.is_scheduled());
```

That directly pins the queue-vs-no-queue distinction the current split leaves implicit — the exact
behaviour a naive `request_snapshot` swap would silently break.

**SnapshotHandle tests shrink to what survives.** `test_handle_noop` (`tests.rs:64-70`) and the
`is_noop()` assertions (`tests.rs:60`) delete with the members they test; `test_handle_complete_releases_flag`
(`tests.rs:29-40`) folds into `test_handle_drop_releases_flag` (`tests.rs:41-54`) since `complete()`
and `Drop` are now one path. Net: fewer tests, none of them testing a member no production code uses.

## Risks / open questions

- **BGSAVE behavioural parity is the whole point — guard it.** The `AlreadyRunning` variant exists
  precisely so plain BGSAVE keeps its no-queue semantics. The migration MUST add the scheduler-level
  test above *before* rewiring `handle_bgsave`, so a regression to "plain BGSAVE now queues" is caught
  synchronously rather than in a booted integration test. This is the one place a careless edit
  changes observable server behaviour.
- **`start_snapshot` stays on the trait.** The periodic task (`startup.rs:29`) and one BGSAVE test
  path still use it, and it is the only method that returns a `SnapshotHandle`. Do not fold it into
  `request_snapshot` in this pass — that would either force `request_snapshot` to return a handle
  (widening the seam again) or force the periodic task to stop logging its epoch. Keep them distinct.
- **`request_snapshot` signature change ripples to the one caller.** Adding the `SnapshotMode`
  parameter touches the trait, both adapters, and `persistence_conn_command.rs:110`. Small and
  compiler-driven (every impl errors until updated), but it is a public-trait signature change.
- **`SnapshotMetadata` may become unused in `server`.** After deleting `last_snapshot_metadata`,
  confirm nothing in `server` still imports `SnapshotMetadata` solely for that method; if the re-export
  at `core/src/lib.rs:115` / `persistence/src/lib.rs:30` goes unused, trim it (a `cargo check` warning
  will flag it). Low risk.
- **Noop's `request_snapshot` reshape.** `NoopSnapshotCoordinator::request_snapshot` (`noop.rs:78-93`)
  currently mirrors the two-outcome enum; it must gain the `Immediate` arm returning `AlreadyRunning`.
  Its instant-completion model (drop the handle to release `in_progress`) is unchanged. Verify the
  Noop path still releases `in_progress` on the `Started` arm (it drops the handle at `noop.rs:88`).
- **No mock coordinator remains.** Confirmed only two `impl SnapshotCoordinator` (`Rocks`, `Noop`);
  the mock was deleted in a prior round (`mock_snapshot` / `MockSnapshot` — 0 hits workspace-wide),
  and `crates/test-harness` / testing crates reference none of these methods. So the trait trim has no
  hidden third implementor to update.

## Effort estimate

**S–M.** The trait-method deletions are mechanical and compiler-guided: remove three methods from the
trait and six method bodies across the two adapters, delete/relocate ~4 small tests. The
`SnapshotHandle` slim-down is a localized single-file edit plus test consolidation. The one part that
earns the M is the BGSAVE unification: it adds a `SnapshotMode` enum + `SnapshotRequest::AlreadyRunning`
variant, a `request_mode` scheduler method, and a rewrite of `handle_bgsave`, and it must be done
test-first to guarantee the deliberate plain-vs-SCHEDULE no-queue distinction survives. No async, disk,
or ordering changes — the scheduler's `SeqCst` handshake is untouched — and the blast radius is three
files in `persistence` plus one caller in `server`.
