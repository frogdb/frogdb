# Proposal 06 — Extract a pure SnapshotScheduler from RocksSnapshotCoordinator

## Summary

`RocksSnapshotCoordinator::start_snapshot` is a single ~48-line function that
interleaves five unrelated concerns: the coalesce/reschedule **state machine**
(three atomics: `in_progress`, `scheduled`, `epoch`), async task orchestration
(`tokio::spawn` + `spawn_blocking`), disk work (`SnapshotStager::run`), metrics,
and logging. The genuinely tricky, correctness-critical part — the **double-CAS
reschedule handshake** that coalesces a concurrent BGSAVE request into exactly
one follow-up run — is buried inside an anonymous 22-line `tokio::spawn` closure
built from twelve move-captured two-letter locals, and it has **zero test
coverage**. Meanwhile the coordinator's public interface leaks the three raw
scheduling atomics (`in_progress` / `is_scheduled` / `schedule_snapshot`) as its
seam, forcing every caller to re-derive the coalesce sequencing by hand.

This proposal extracts a pure, `tokio`-free, disk-free **`SnapshotScheduler`**
value type that owns `{in_progress, scheduled, epoch}` and exposes deep,
intention-revealing methods (`try_begin`, `finish_and_maybe_rebegin`, `request`).
`start_snapshot` collapses to "ask the scheduler for an epoch, spawn a named
`run_loop`". The state machine then becomes exhaustively unit-testable with no
runtime and no filesystem. A secondary finding: `SnapshotHandle`'s RAII
completion callback is **inert in production** (the Rocks coordinator passes an
empty `|| {}`), so its Drop-fires-callback machinery is dead weight on the hot
path — a candidate for a deletion test.

## Files involved (verified paths + counts)

| Path | Lines | Role |
|------|-------|------|
| `frogdb-server/crates/persistence/src/snapshot/rocks_coordinator.rs` | 166 | Home of the dense `start_snapshot` (lines 94–141) and the leaked scheduling atomics |
| `frogdb-server/crates/persistence/src/snapshot/handle.rs` | 41 | `SnapshotHandle` RAII wrapper (inert in production) |
| `frogdb-server/crates/persistence/src/snapshot/tests.rs` | 442 | Tests `Noop` + `SnapshotStager::run` thoroughly; **never** constructs `RocksSnapshotCoordinator` |
| `frogdb-server/crates/persistence/src/snapshot/mod.rs` | 33 | `SnapshotCoordinator` trait + `SnapshotError` |
| `frogdb-server/crates/persistence/src/snapshot/noop.rs` | 81 | Only impl that actually exercises the `SnapshotHandle` completion closure (via `unsafe` raw pointer) |
| `frogdb-server/crates/server/src/connection/persistence_conn_command.rs` | (BGSAVE at 100–126) | Caller that must hand-sequence the 3-boolean protocol |

## Problem

### What the one function interleaves

`start_snapshot` (rocks_coordinator.rs:94–141) does all of the following in a
straight line, with no seam between the layers:

1. **CAS guard** (lines 95–101): `in_progress.compare_exchange(false, true, …)`;
   on failure return `SnapshotError::AlreadyInProgress`.
2. **Epoch bump + eager metrics** (102–105): `epoch.fetch_add(1)`, then
   `SnapshotInProgress::set(…, 1.0)` and `SnapshotEpoch::set(…, ie)`.
3. **Twelve move-capture clones** (106–117) so the spawned task can own its
   state — eight of them literal `Arc::clone`, the rest `PathBuf`/`Copy`.
4. **A 22-line anonymous `tokio::spawn` closure** (118–139) that owns the whole
   run loop, the `spawn_blocking(Stager::run)` call, the success/error/panic
   match, and the reschedule handshake — all as effectively one statement.
5. **An empty `SnapshotHandle::new(ie, || {})`** (140) whose callback does
   nothing (see the secondary finding).

### The two-letter locals

Every captured local is a two-letter (or three-letter) abbreviation. Verbatim
from lines 106–120:

```rust
let rs = self.rocks_store.clone();      // rocks store
let sd = self.snapshot_dir.clone();     // snapshot dir
let ip = self.in_progress.clone();      // in-progress atomic
let sc = self.scheduled.clone();        // scheduled atomic
let ec = self.epoch.clone();            // epoch counter
let lst = self.last_save_time.clone();  // last save time
let lm = self.last_metadata.clone();    // last metadata
let mt = self.metrics_recorder.clone(); // metrics recorder
let ms = self.max_snapshots;            // max snapshots
let ns = self.num_shards;               // num shards
let psh = self.pre_snapshot_hook.clone();
let dd = self.data_dir.clone();         // data dir
```

and inside the closure, the running-epoch / saved-epoch pair:

```rust
async move { let mut ce = ie; …          // current epoch
    let rs2 = rs.clone(); … let se = ce; // snapshot epoch for this iteration
```

So the fourteen the reader must decode are: **`rs, sd, ip, sc, ec, lst, lm, mt,
ms, ns, psh, dd, ce, se`** — plus `ie` (initial epoch) from the outer scope.
Nothing in the names distinguishes the three correctness-critical atomics
(`ip`, `sc`, `ec`) from the incidental plumbing (`sd`, `dd`, `mt`).

### The anonymous 22-line closure

The entire background task is one `tokio::spawn(async move { … })` argument
(lines 118–139). Its body is dense to the point that the success arm at line 134
is a **single physical line** packing eight statements (elapsed, sequence,
path, two `RwLock` writes, three metric emits, and a log). There is no named
function anywhere — the reschedule state machine, the disk call, and the metrics
are all textually fused.

### The 3-boolean protocol callers must sequence

The trait (mod.rs:25–31) exposes three separate booleans that only make sense
when sequenced in a specific order:

- `in_progress() -> bool` — is a save running right now?
- `schedule_snapshot() -> bool` — request a coalesced follow-up; **only succeeds
  if `in_progress`** (rocks_coordinator.rs:155–158 returns `false` otherwise).
- `is_scheduled() -> bool` — is a follow-up pending?

The BGSAVE handler has to reconstruct the coalesce decision by hand
(persistence_conn_command.rs:107–115):

```rust
if ctx.snapshot_coordinator.in_progress() {
    ctx.snapshot_coordinator.schedule_snapshot();
    return Response::Simple(Bytes::from_static(b"Background saving scheduled"));
}
// No save in progress, fall through to start one immediately.
…
match ctx.snapshot_coordinator.start_snapshot() { … }
```

This is a check-then-act across two atomic calls: the coordinator exposes the
raw levers and trusts each caller to pull them in the right order. The
"only succeeds if `in_progress`" precondition on `schedule_snapshot` is exactly
the sort of coupling that should live *inside* one method (`request()`), not be
re-derived at every call site.

### The reschedule double-CAS — buried and untested

The genuinely tricky logic is the tail of the loop (lines 135–138):

```rust
ip.store(false, Ordering::SeqCst);
if !sc.swap(false, Ordering::SeqCst) { SnapshotInProgress::set(&*mt, 0.0); break; }
if ip.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() { SnapshotInProgress::set(&*mt, 0.0); break; }
ce = ec.fetch_add(1, Ordering::SeqCst) + 1; start = Instant::now(); SnapshotEpoch::set(&*mt, ce as f64); tracing::info!(epoch = ce, "Starting scheduled snapshot");
```

The sequence is: (1) release `in_progress`; (2) atomically read-and-clear
`scheduled` — if nothing was scheduled, drop `in_progress`-metric and exit;
(3) **re-acquire** `in_progress` via a second CAS — if some other caller won the
race and started a fresh save in the gap, bail out and let *them* own the run;
(4) otherwise bump the epoch and loop. This is a coalescing handshake with two
distinct race windows (between the store and the swap; between the swap and the
re-CAS), and its correctness is the whole point of the module. It is embedded in
an async closure that can only be reached with a **real `RocksStore` + a Tokio
runtime + a writable filesystem**, and `tests.rs` never constructs a
`RocksSnapshotCoordinator` at all — so this handshake has **no test whatsoever**.

## Why it is shallow/fragmented (architecture vocab)

Using Ousterhout's lens:

- **The interface is shallow and leaky.** A deep module offers a simple
  interface over substantial functionality. Here the coordinator instead
  *publishes its implementation*: the three atomics `in_progress`,
  `schedule_snapshot`, `is_scheduled` are the seam. Callers get no abstraction —
  they get the raw gears and must assemble the coalesce machine themselves
  (the check-then-act in persistence_conn_command.rs). The knowledge "schedule
  only counts while a save is in flight" lives in the caller's head, not the
  type.
- **Poor locality / fragmented responsibility.** The state machine's four moves
  (`try_begin`, release, coalesce-check, re-begin) are scattered across the top
  of `start_snapshot` (95–102) and the bottom of the spawn closure (135–138),
  with 30 lines of disk/metrics/logging wedged between them. To reason about the
  atomics you must read past everything you don't care about.
- **No seam at the depth boundary.** The one part worth testing in isolation —
  pure atomic sequencing — is welded to the parts that *can't* be unit-tested
  cheaply (real RocksDB checkpoint I/O, Tokio, fs). There is no adapter that
  lets you drive the state machine without the world. Consequently the deepest,
  most failure-prone code is the least reachable.
- **Low leverage.** The abbreviated locals and the single-statement closure make
  every future change (add a metric, change an ordering, add a third coalesce
  state) a rewrite of the whole function rather than a localized edit.

## Secondary finding: SnapshotHandle RAII is inert

`SnapshotHandle` (handle.rs:3–41) is a RAII wrapper:
`{ epoch, is_noop, complete_fn: Option<Box<dyn FnOnce() + Send>> }` with a
`complete()` that fires the callback and a `Drop` (35–40) that fires it if not
already consumed. The intent is "completion runs when the handle is dropped or
explicitly completed."

But `RocksSnapshotCoordinator::start_snapshot` constructs it with an **empty
callback** (rocks_coordinator.rs:140):

```rust
Ok(SnapshotHandle::new(ie, || {}))
```

Real completion in production is tracked entirely by the `in_progress` atomic,
which the background loop clears at line 135 (`ip.store(false, …)`), *not* by the
handle. The handle the caller receives is a bare epoch carrier; its Drop runs a
no-op. The RAII mechanism is therefore **dead weight on the production path**.

The only code that gives the callback teeth is `NoopSnapshotCoordinator`
(noop.rs:41–45), which routes completion through an `unsafe` raw-pointer stash to
clear its own `in_progress` on drop — plus the unit test `test_handle_complete`
(tests.rs:29–41). So the entire `complete_fn` / Drop apparatus exists to serve
one test double and one unit test.

**Deletion test.** Would removing `complete_fn` (making `SnapshotHandle` just
`{ epoch, is_noop }`) break production? No — Rocks already passes `|| {}`. The
Noop coordinator can track its own `in_progress` release without laundering it
through the handle (e.g. clear the flag in `start_snapshot`'s own scope or via a
dedicated Noop-only guard). If a feature *ever* needs caller-driven completion,
reintroduce it deliberately with a real consumer. As-is it fails the deletion
test: the interface carries a mechanism no production caller uses.

## Proposed change

Introduce a pure value type in the `snapshot` module (no `tokio`, no `RocksStore`,
no fs), owning the three scheduling atomics:

```rust
/// Pure coalesce/reschedule state machine over the snapshot lifecycle.
/// No Tokio, no disk — just the three atomics and their handshake.
pub struct SnapshotScheduler {
    in_progress: AtomicBool,
    scheduled: AtomicBool,
    epoch: AtomicU64,
}

pub enum Request {
    /// A save was idle; this call started a fresh one at the given epoch.
    Started(u64),
    /// A save is already running; this call coalesced into a pending follow-up.
    Coalesced,
}

impl SnapshotScheduler {
    /// Try to claim the in-progress slot. Returns the new epoch on success,
    /// `None` if a save is already running (the `AlreadyInProgress` case).
    pub fn try_begin(&self) -> Option<u64> {
        self.in_progress
            .compare_exchange(false, true, SeqCst, SeqCst)
            .ok()?;
        Some(self.epoch.fetch_add(1, SeqCst) + 1)
    }

    /// End the current run; if a follow-up was scheduled and we can re-acquire
    /// the slot, return the next epoch to run. Otherwise `None` (fully idle).
    /// This is the double-CAS handshake, now in one named place.
    pub fn finish_and_maybe_rebegin(&self) -> Option<u64> {
        self.in_progress.store(false, SeqCst);
        if !self.scheduled.swap(false, SeqCst) {
            return None;
        }
        self.in_progress
            .compare_exchange(false, true, SeqCst, SeqCst)
            .ok()?;
        Some(self.epoch.fetch_add(1, SeqCst) + 1)
    }

    /// Caller-facing coalesce decision (replaces the check-then-act at BGSAVE).
    /// If a save is running, mark a follow-up and report `Coalesced`; otherwise
    /// begin immediately and report `Started(epoch)`.
    pub fn request(&self) -> Request {
        if self.in_progress.load(SeqCst) {
            self.scheduled.store(true, SeqCst);
            Request::Coalesced
        } else {
            match self.try_begin() {
                Some(e) => Request::Started(e),
                None => Request::Coalesced, // lost the race; someone else runs
            }
        }
    }

    pub fn in_progress(&self) -> bool { self.in_progress.load(SeqCst) }
    pub fn is_scheduled(&self) -> bool { self.scheduled.load(SeqCst) }
    pub fn current_epoch(&self) -> u64 { self.epoch.load(SeqCst) }
}
```

`start_snapshot` then becomes a thin adapter: claim an epoch, spawn a **named**
`run_loop`. All the abbreviated captures move into a small
`RunParams`/`RunDeps` struct (or are read from `self` before spawn) with real
field names. Metrics move into the loop but read as ordinary statements, not a
one-liner. The double-CAS lives once, in `finish_and_maybe_rebegin`.

Additionally: slim `SnapshotHandle` to `{ epoch, is_noop }` (drop the inert
`complete_fn` + `Drop`), or, if we keep it for the Noop double, make the
production path stop paying for it.

## Before / After

### Before — the real current `start_snapshot` (rocks_coordinator.rs:94–141, verbatim)

```rust
fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError> {
    if self
        .in_progress
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Err(SnapshotError::AlreadyInProgress);
    }
    let ie = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
    SnapshotInProgress::set(&*self.metrics_recorder, 1.0);
    SnapshotEpoch::set(&*self.metrics_recorder, ie as f64);
    tracing::info!(epoch = ie, "Snapshot started");
    let rs = self.rocks_store.clone();
    let sd = self.snapshot_dir.clone();
    let ip = self.in_progress.clone();
    let sc = self.scheduled.clone();
    let ec = self.epoch.clone();
    let lst = self.last_save_time.clone();
    let lm = self.last_metadata.clone();
    let mt = self.metrics_recorder.clone();
    let ms = self.max_snapshots;
    let ns = self.num_shards;
    let psh = self.pre_snapshot_hook.clone();
    let dd = self.data_dir.clone();
    tokio::spawn(async move { let mut ce = ie; let mut start = Instant::now(); loop {
        { let h = psh.read().unwrap().clone(); if let Some(h) = h { h().await; } }
        let rs2 = rs.clone(); let sd2 = sd.clone(); let dd2 = dd.clone(); let se = ce;
        let result = tokio::task::spawn_blocking(move || {
            SnapshotStager {
                tmp: sd2.join(format!(".snapshot_{:05}.tmp", se)),
                final_dir: sd2.join(format!("snapshot_{:05}", se)),
                name: format!("snapshot_{:05}", se),
                snapshot_dir: sd2,
                data_dir: dd2,
                epoch: se,
                num_shards: ns,
                max_snapshots: ms,
            }
            .run(&rs2)
        }).await;
        match result { Ok(Ok(md)) => { let el = start.elapsed(); let seq = md.sequence_number; let sp = sd.join(format!("snapshot_{:05}", ce)); *lst.write().unwrap() = Some(Instant::now()); *lm.write().unwrap() = Some(md.clone()); SnapshotDuration::observe(&*mt, el.as_secs_f64()); SnapshotSizeBytes::set(&*mt, md.size_bytes as f64); SnapshotLastTimestamp::set(&*mt, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs_f64()); tracing::info!(epoch = ce, sequence = seq, path = %sp.display(), size_bytes = md.size_bytes, duration_ms = el.as_millis(), "Snapshot completed"); } Ok(Err(e)) => { PersistenceErrors::inc(&*mt, PersistenceErrorType::Snapshot); tracing::error!(epoch = ce, error = %e, "Snapshot failed"); } Err(e) => { PersistenceErrors::inc(&*mt, PersistenceErrorType::Snapshot); tracing::error!(epoch = ce, error = %e, "Snapshot task panicked"); } }
        ip.store(false, Ordering::SeqCst);
        if !sc.swap(false, Ordering::SeqCst) { SnapshotInProgress::set(&*mt, 0.0); break; }
        if ip.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() { SnapshotInProgress::set(&*mt, 0.0); break; }
        ce = ec.fetch_add(1, Ordering::SeqCst) + 1; start = Instant::now(); SnapshotEpoch::set(&*mt, ce as f64); tracing::info!(epoch = ce, "Starting scheduled snapshot");
    } }.instrument(tracing::info_span!("snapshot_create")));
    Ok(SnapshotHandle::new(ie, || {}))
}
```

### After — sketch (scheduler extracted, named run loop, slimmed handle)

The coordinator holds an `Arc<SnapshotScheduler>` instead of three loose
`Arc<Atomic*>` fields. `start_snapshot` is now a claim + spawn:

```rust
fn start_snapshot(&self) -> Result<SnapshotHandle, SnapshotError> {
    let epoch = self
        .scheduler
        .try_begin()
        .ok_or(SnapshotError::AlreadyInProgress)?;

    SnapshotInProgress::set(&*self.metrics_recorder, 1.0);
    SnapshotEpoch::set(&*self.metrics_recorder, epoch as f64);
    tracing::info!(epoch, "Snapshot started");

    let deps = SnapshotRun {
        scheduler: self.scheduler.clone(),
        rocks_store: self.rocks_store.clone(),
        snapshot_dir: self.snapshot_dir.clone(),
        data_dir: self.data_dir.clone(),
        last_save_time: self.last_save_time.clone(),
        last_metadata: self.last_metadata.clone(),
        metrics: self.metrics_recorder.clone(),
        pre_snapshot_hook: self.pre_snapshot_hook.clone(),
        num_shards: self.num_shards,
        max_snapshots: self.max_snapshots,
    };
    tokio::spawn(
        run_loop(deps, epoch).instrument(tracing::info_span!("snapshot_create")),
    );
    Ok(SnapshotHandle::new(epoch)) // no inert callback
}

/// One background save, then coalesced re-runs, until the scheduler says idle.
async fn run_loop(deps: SnapshotRun, mut epoch: u64) {
    loop {
        if let Some(hook) = deps.pre_snapshot_hook.read().unwrap().clone() {
            hook().await;
        }
        let started = Instant::now();
        let result = deps.run_one(epoch).await; // wraps spawn_blocking(Stager::run)
        deps.record(epoch, started, result);     // metrics + last_metadata + logs

        match deps.scheduler.finish_and_maybe_rebegin() {
            None => {
                SnapshotInProgress::set(&*deps.metrics, 0.0);
                break;
            }
            Some(next) => {
                epoch = next;
                SnapshotEpoch::set(&*deps.metrics, epoch as f64);
                tracing::info!(epoch, "Starting scheduled snapshot");
            }
        }
    }
}
```

BGSAVE's caller collapses from check-then-act to one call:

```rust
match ctx.snapshot_coordinator.request() {
    Request::Coalesced => Response::Simple(Bytes::from_static(b"Background saving scheduled")),
    Request::Started(epoch) => {
        tracing::info!(epoch, "BGSAVE started");
        Response::Simple(Bytes::from_static(b"Background saving started"))
    }
}
```

(Exact trait shaping is an open question — see Risks. The point is that the
coalesce decision moves behind one method and off every call site.)

## Testability improvement

Today the coalesce/reschedule handshake is reachable only through a live
`RocksSnapshotCoordinator`, which needs a real `RocksStore`, a Tokio runtime, and
a writable snapshot dir — so `tests.rs` tests it **not at all** (it exercises
`NoopSnapshotCoordinator` and `SnapshotStager::run`, never the reschedule loop).

With `SnapshotScheduler` extracted as a pure type, the state machine gets
exhaustive synchronous unit tests — no runtime, no disk:

- `try_begin` succeeds once, returns `Some(1)`; a second concurrent `try_begin`
  returns `None` (the `AlreadyInProgress` guard).
- **Concurrent request during a run → exactly one re-run**: `try_begin()` →
  `request()` (while in progress) sets `scheduled` and reports `Coalesced` →
  `finish_and_maybe_rebegin()` returns `Some(2)` **once**, and a subsequent
  `finish_and_maybe_rebegin()` returns `None`. Assert the follow-up fires exactly
  once no matter how many `request()`s land during the run.
- **Request after finish → none**: `try_begin` → `finish_and_maybe_rebegin`
  (returns `None`, idle) → a later `request()` reports `Started`, not a phantom
  coalesce.
- **Re-CAS race**: after `finish_and_maybe_rebegin` releases and something else
  wins the slot, the losing path returns `None` and does not double-run an epoch.
- Epoch monotonicity across a `begin → coalesce → rebegin` cycle: 1 then 2, never
  skipped or reused.

These mirror the concurrency intent already probed indirectly in
`core/tests/concurrency.rs:1290–1470` and `core/tests/common/mock_snapshot.rs`
(a hand-rolled CAS mock that exists precisely because the real state machine is
untestable) — the extraction lets those assertions target the production type
directly. Follow the existing `tests.rs` pattern for `NoopSnapshotCoordinator`
(tests.rs:8–69, plain `#[test]` fns asserting on atomics) and extend it to the
scheduler; `mock_snapshot.rs` can then be deleted (another deletion-test win).

## Risks / open questions

- **Memory ordering must be preserved exactly.** The current code uses `SeqCst`
  throughout; the extraction must keep every ordering identical. This is a pure
  code-motion refactor of the atomics — no ordering "cleanup" in the same change.
- **Trait shape.** Adding `request() -> Request` while keeping/removing
  `schedule_snapshot`/`is_scheduled`/`in_progress` touches the
  `SnapshotCoordinator` trait, both impls (`Rocks`, `Noop`), and callers in
  `persistence_conn_command.rs`, `subsystems.rs`, `startup.rs`,
  `handlers/info.rs`. `in_progress()` is still read by INFO
  (`bgsave_in_progress`, info.rs:154) and shutdown drain (subsystems.rs:573–575),
  so it stays. Decide whether `is_scheduled`/`schedule_snapshot` remain public or
  fold entirely into `request()`.
- **`Noop` coordinator alignment.** `NoopSnapshotCoordinator` duplicates the same
  three atomics (noop.rs:9–14) and could share `SnapshotScheduler`, which would
  also let it drop its `unsafe` raw-pointer completion stash (noop.rs:41–45).
  Worth doing in the same pass, but expands scope.
- **`SnapshotHandle` change is caller-visible.** `handle.epoch()` is used at call
  sites (persistence_conn_command.rs:117, startup.rs); removing `complete_fn`/
  `Drop`/`complete()` is safe for those, but confirm no external consumer relies
  on drop-completion before deleting.
- **Doc/impl mismatch to note (not part of this change).** `frogdb-server/
  CONTEXT.md` describes snapshots via a "COW Buffer," but the implementation is
  forkless: a RocksDB checkpoint cut at a sequence number plus a
  `pre_snapshot_hook` (rocks_coordinator.rs:20, 116, 119) — there is no
  in-memory COW buffer counted toward maxmemory. Flag the CONTEXT.md wording for
  a separate doc fix; this proposal does not depend on it.

## Effort estimate

**M.** The `SnapshotScheduler` type plus its unit tests are small and mechanical
(S on their own — pure atomics, no async, no I/O). What pushes it to M is the
blast radius of reshaping the `SnapshotCoordinator` trait: both coordinator
impls, the BGSAVE caller, and the INFO/shutdown/startup readers all touch the
scheduling booleans, and the async `run_loop` extraction must be done carefully
to preserve `SeqCst` ordering and the exact metric-emission points. No new
concurrency semantics are introduced — it is code motion behind a name — but it
spans ~6 files across `persistence` and `server`. Bundling the `SnapshotHandle`
slim-down and the `Noop`/`mock_snapshot.rs` deletions would add modest scope but
pays for itself in removed `unsafe` and removed dead interface.
