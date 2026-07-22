# Proposal 29 — Move the split-brain divergence-window computation into the primary handler, so `SplitBrainLogger` only formats and writes

## Summary

`SplitBrainLogger::log` (`frogdb-server/crates/server/src/server/cluster_init.rs:616-684`) reconstructs the split-brain
**divergence window** `(min_acked, current]` and its divergent-writes predicate by hand from three
separately-injected collaborators — a tracker, a broadcaster, and the primary handler — inside an
operator-observability struct in the `server` crate. The deeper finding (verified below) is that on a
Primary **all three are the same object**: the `broadcaster` field *is* the `Arc<PrimaryReplicationHandler>`
(`frogdb-server/crates/server/src/server/replication_init.rs:103`), the `tracker` is that handler's own tracker (`frogdb-server/crates/server/src/server/replication_init.rs:71-77`),
and the `OffsetCoordinator` the handler already owns *already* exposes both halves of the window —
`current()` and `min_acked()`. The window boundary, the predicate, and the extraction are all
answerable from the handler's own **coordinator + backlog**, yet they are assembled a level up from
three injected handles.

This proposal gives `PrimaryReplicationHandler` one method that returns the whole divergence record
`{ start, end, writes }`, computed from its own `offsets` (the [`OffsetCoordinator`]) and `replay`
(the Replication Backlog). `SplitBrainLogger` drops its `broadcaster` and `tracker` fields, holds only
the optional handler, and shrinks to "ask for the record, format the header from the `DemotionEvent`,
write the log, bump telemetry." The window math becomes a synchronous crate-local unit test on a
seeded backlog + acks, instead of being reachable only through the cluster role-demotion integration
path.

## Problem

The split-brain divergence window is a **Replication Backlog** concept: "which writes did this
demoted Primary commit past the point the new Primary had acknowledged?" That question has a natural
owner — the primary handler, which owns the live offset, the per-replica acked offsets, and the
backlog of recent writes. But the *computation* of the window lives in `SplitBrainLogger::log`, a
`server`-crate struct whose real job is formatting a log line and bumping telemetry. `log` reaches
across the module seam into three collaborators to reassemble a value the handler could hand it whole:

- the **lower bound** `min_acked` — via `tracker.min_acked_offset()`,
- the **upper bound** `current` — via `broadcaster.current_offset()`,
- the **predicate + payload** — via `primary_handler.extract_divergent_writes(min_acked)`, gated on
  `current > min_acked` and then on `!writes.is_empty()`.

Three handles, one derived fact. This is poor **Locality**: to know how the divergence boundary is
computed you must read a `server`-crate observability struct, and to know it is *correct* you must
know that its three injected collaborators are wired (on a Primary) to the same handler that already
knows all three quantities. The predicate `current > min_acked` — the definition of "did this node
diverge?" — lives outside the module that owns the offsets it compares. `SplitBrainLogger` is a
**shallow** consumer forced to reach *back into* the replication module to reconstruct the module's
own output, exactly because the module's interface is too thin to hand it over.

The seam is also **leaky in the dependency direction that matters for testing**: because the window
math sits in `server`, the only way to exercise "these acks + this backlog produce this divergence
record" is through `DemotionConsumer::handle` → cluster role-demotion wiring. The `replication` crate,
which owns every input to the computation, cannot unit-test the computation it is the sole source of.

## Evidence (verified file:line)

**`SplitBrainLogger` holds five fields; three exist only to recompute the window**
(`frogdb-server/crates/server/src/server/cluster_init.rs:608-614`):

```rust
struct SplitBrainLogger {
    data_dir: std::path::PathBuf,
    broadcaster: SharedBroadcaster,                               // L610
    primary_handler: Option<Arc<...PrimaryReplicationHandler>>,   // L611
    tracker: Option<Arc<ReplicationTrackerImpl>>,                 // L612
    metrics: Arc<dyn MetricsRecorder>,
}
```

**`log` assembles the window from all three** (`frogdb-server/crates/server/src/server/cluster_init.rs:616-684`, boundary math L627-653):

```rust
let min_acked = self.tracker.as_ref()
    .and_then(|t| t.min_acked_offset()).unwrap_or(0);   // L628-632  (lower bound)
let current = self.broadcaster.current_offset();         // L633      (upper bound)
if current > min_acked {                                 // L635      (divergence predicate)
    let divergent = self.primary_handler.as_ref()
        .map(|h| h.extract_divergent_writes(min_acked))
        .unwrap_or_default();                            // L636-640  (payload)
    if !divergent.is_empty() {                           // L641      (write gate)
        let header = SplitBrainLogHeader {
            /* old_primary/new_primary/epoch_old/epoch_new from the DemotionEvent */
            seq_diverge_start: min_acked,                // L651
            seq_diverge_end:   current,                  // L652
            ops_discarded:     divergent.len(),          // L653
        };
        write_log(&self.data_dir, header, &divergent) /* L656-660 */;
        /* SplitBrainEventsTotal / SplitBrainOpsDiscardedTotal /
           SplitBrainRecoveryPending telemetry L675-680 */
    }
}
```

**The three collaborators are the same object on a Primary.** In `frogdb-server/crates/server/src/server/replication_init.rs` the primary
branch builds one handler and hands the *same* `Arc` back as the broadcaster, and the *same* tracker
it constructed the handler with:

- `let tracker = Arc::new(ReplicationTrackerImpl::new()); ... PrimaryReplicationHandler::new(.., tracker.clone(), ..)`
  (`frogdb-server/crates/server/src/server/replication_init.rs:71-77`)
- `primary_replication_handler = Some(handler.clone());` (`frogdb-server/crates/server/src/server/replication_init.rs:101`)
- `(handler as SharedBroadcaster, Some(tracker))` (`frogdb-server/crates/server/src/server/replication_init.rs:103`)

and `frogdb-server/crates/server/src/server/cluster_init.rs:504-518` wires all three logger fields from those same values
(`broadcaster: replication_broadcaster.clone()`, `primary_handler: primary_replication_handler.cloned()`,
`tracker: replication_tracker.clone()`).

**Both window bounds are already single-seam questions on the handler's `OffsetCoordinator`.** The
handler owns `offsets: Arc<OffsetCoordinator>` (`frogdb-server/crates/replication/src/primary/mod.rs:79`) and `replay: PartialSyncReplay`
(`frogdb-server/crates/replication/src/primary/mod.rs:72`). The coordinator already vends:

- `OffsetCoordinator::current()` (`frogdb-server/crates/replication/src/offset_coordinator.rs:113-115`) — the live upper bound;
- `OffsetCoordinator::min_acked()` (`frogdb-server/crates/replication/src/offset_coordinator.rs:146-148`), which is literally
  `self.tracker.min_acked_offset()` — the lower bound.

So `broadcaster.current_offset()` == `handler.offsets.current()` (the handler's own
`ReplicationBroadcaster::current_offset` impl is exactly `self.offsets.current()`,
`frogdb-server/crates/replication/src/primary/mod.rs:317-319`), and `tracker.min_acked_offset()` == `handler.offsets.min_acked()`. The
window `(min_acked, current]` is a value the handler can compute from *one* of its own fields.

**Extraction already belongs to the handler → backlog, and owns only extraction — not the window.**
`PrimaryReplicationHandler::extract_divergent_writes` (`frogdb-server/crates/replication/src/primary/mod.rs:230-232`) forwards to
`PartialSyncReplay::extract_divergent_writes` (`frogdb-server/crates/replication/src/primary/replay.rs:119-122`), which forwards to
`ReplicationRingBuffer::extract_divergent_writes` (`frogdb-server/crates/replication/src/primary/ring_buffer.rs:75-82`) — a pure
`offset > last` filter. None of these three layers computes the window boundary or the predicate; they
only slice the backlog given a boundary the caller supplies. Today that caller is `server`.

**The header type already lives in `replication`** (`frogdb-server/crates/replication/src/split_brain_log.rs:30-47`, `write_log` at
`frogdb-server/crates/replication/src/split_brain_log.rs:134`), and its `seq_diverge_start` / `seq_diverge_end` / `ops_discarded` fields
are exactly the divergence-record fields; the remaining header fields (`old_primary`, `new_primary`,
`epoch_old`, `epoch_new`) come from the `DemotionEvent`, which the handler has no business knowing.

**Existing tests that pin current behavior.** The `server` side is covered only through the
demotion path: `noop_logger()` (`frogdb-server/crates/server/src/server/cluster_init.rs:817-825`) constructs a `SplitBrainLogger` with a
`NoopBroadcaster` (current_offset 0, `frogdb-server/crates/replication/src/lib.rs:133-135`), `primary_handler: None`, `tracker: None` — so
`current(0) > min_acked(0)` is false and it takes the no-divergence fast path; `demotion_fires_when_
split_brain_log_disabled` (`frogdb-server/crates/server/src/server/cluster_init.rs:793`) and `demotion_identical_whether_or_not_log_enabled`
(`frogdb-server/crates/server/src/server/cluster_init.rs:830`) assert the demotion side-effect, not the window. The **actual extraction** is
unit-tested only at the raw ring-buffer layer (`frogdb-server/crates/replication/src/primary/tests.rs:65-125`,
`test_ring_buffer_push_and_extract` etc.) — there is **no** test anywhere that pins "these acks + this
live offset ⇒ this `(start, end)` window and this write set," because no single type owns that
computation.

**Verification note (candidate refinement).** The candidate framed the inputs as "three
collaborators." Verified: on a Primary two of the three (`broadcaster`, `tracker`) are the *same*
handler / the handler's own tracker, and the `OffsetCoordinator` already collapses both offset reads
into one seam. The premise is therefore *stronger* than stated — this is one owner reached three ways,
not three independent owners — which makes the consolidation strictly cleaner. Also: the candidate
cited `frogdb-server/crates/replication/src/primary/replay.rs:119-122` as the extraction owner; the method the `server` actually calls is
`PrimaryReplicationHandler::extract_divergent_writes` (`frogdb-server/crates/replication/src/primary/mod.rs:230`), which *delegates* to
`frogdb-server/crates/replication/src/primary/replay.rs:119`. The substance ("the backlog layers own only extraction, not the window") is correct;
the citation was one indirection shallow.

## Proposed design (Rust interface sketch)

A divergence-record type and one query method, both in the `replication` crate. Signatures/types
only.

```rust
// frogdb-server/crates/replication/src/primary/mod.rs (or a small frogdb-server/crates/replication/src/primary/split_brain.rs sibling)

/// The split-brain divergence window this (demoted) Primary computed against the
/// last offset the cluster had acknowledged — the writes it committed past that
/// point and must surrender to the new Primary.
///
/// Constructed only when the node actually diverged: `end > start` AND `writes`
/// is non-empty. A caught-up (or write-less) demotion yields `None`, matching
/// today's `current > min_acked && !divergent.is_empty()` gate.
#[derive(Debug)]
pub struct DivergenceRecord {
    /// Lower bound: the minimum acked offset across streaming replicas
    /// (== today's `seq_diverge_start`). `min_acked().unwrap_or(0)`.
    pub start: u64,
    /// Upper bound: the live write position at demotion time
    /// (== today's `seq_diverge_end`).
    pub end: u64,
    /// The divergent writes `(offset, RESP)` with `offset > start`, offset-ordered
    /// (== today's `divergent`; `writes.len()` is `ops_discarded`).
    pub writes: Vec<(u64, Bytes)>,
}

impl PrimaryReplicationHandler {
    /// Compute the split-brain divergence window from this handler's own offset
    /// coordinator and Replication Backlog. Pure read; no I/O, no telemetry, no
    /// logging. `None` when the node did not diverge (caught up, or nothing in the
    /// backlog past `start`).
    ///
    /// Replaces the three-collaborator reconstruction in `SplitBrainLogger::log`.
    pub fn divergence_record(&self) -> Option<DivergenceRecord>;
}
```

`SplitBrainLogger` loses its `broadcaster` and `tracker` fields; the `Option<Arc<...>>` handler
becomes the single divergence source, and `log` becomes format-and-write:

```rust
// frogdb-server/crates/server/src/server/cluster_init.rs
struct SplitBrainLogger {
    data_dir: std::path::PathBuf,
    primary_handler: Option<Arc<PrimaryReplicationHandler>>,
    metrics: Arc<dyn MetricsRecorder>,
}

impl SplitBrainLogger {
    fn log(&self, event: &frogdb_core::DemotionEvent) {
        // warn! "Split-brain demotion detected" (unchanged)
        let Some(record) = self.primary_handler.as_ref().and_then(|h| h.divergence_record())
        else { return };
        let header = SplitBrainLogHeader {
            // timestamp + old_primary/new_primary/epoch_old/epoch_new derived from
            // `event` exactly as today (all five assigned explicitly — the header
            // derives no `Default`, so there is no struct-update `..` to lean on):
            timestamp:  /* iso8601(now) — unchanged */,
            old_primary: /* from `event` — unchanged */,
            new_primary: /* from `event` — unchanged */,
            epoch_old:   /* from `event` — unchanged */,
            epoch_new:   /* from `event` — unchanged */,
            seq_diverge_start: record.start,
            seq_diverge_end:   record.end,
            ops_discarded:     record.writes.len(),
        };
        // write_log(...) + the three telemetry bumps, keyed on record.writes  (unchanged)
    }
}
```

The predicate `end > start && !writes.is_empty()` moves inside `divergence_record` (it is the
constructor's own invariant); the `unwrap_or(0)` lower-bound default moves with it. `PrimaryReplicationHandler::extract_divergent_writes` (`frogdb-server/crates/replication/src/primary/mod.rs:230`) can then be made
`pub(crate)` or folded into `divergence_record`, since the only external caller was the logger.

## Migration plan (ordered steps)

1. **Add `DivergenceRecord` + `PrimaryReplicationHandler::divergence_record()`** in `replication`,
   implemented from `self.offsets.current()`, `self.offsets.min_acked().unwrap_or(0)`, and
   `self.replay.extract_divergent_writes(start)`. Preserve the exact gate: `end > start` then
   non-empty writes, else `None`. No behavior in `server` yet.
2. **Add the crate-local unit tests** (see Test plan) for `divergence_record` against a seeded
   backlog + acks — landing the coverage that does not exist today *before* the `server` rewrite.
3. **Rewrite `SplitBrainLogger::log`** to call `divergence_record()` and format/write from the record;
   delete the `broadcaster` and `tracker` fields from the struct (`frogdb-server/crates/server/src/server/cluster_init.rs:610, 612`) and drop
   them from the construction site (`frogdb-server/crates/server/src/server/cluster_init.rs:507, 513`). The `warn!` line, `write_log`, and the
   three telemetry increments are copied verbatim, keyed on `record`.
4. **Update `noop_logger()`** (`frogdb-server/crates/server/src/server/cluster_init.rs:817-825`) to the two-field-lighter shape
   (`primary_handler: None` → `divergence_record` is never reached → same no-divergence path). The two
   demotion tests (`frogdb-server/crates/server/src/server/cluster_init.rs:793, 830`) are unaffected — they assert the demotion side-effect,
   which is untouched.
5. **Tighten visibility**: demote `PrimaryReplicationHandler::extract_divergent_writes` to
   `pub(crate)` (or inline it) once `server` no longer calls it; a `cargo check` confirms no remaining
   external caller. Trim any now-unused `SharedBroadcaster` / `ReplicationTrackerImpl` imports in
   `frogdb-server/crates/server/src/server/cluster_init.rs` (compiler-flagged).
6. **`just check` / `just test frogdb-server` / `just test frogdb-replication`** — the return-type and
   field changes are compiler-guided and confined to two crates whose dependency edge
   (`server → replication`) already exists.

## Test plan

- **New crate-local unit tests in `replication`** (the coverage that is impossible today), driving a
  `PrimaryReplicationHandler` (or the `offsets`+`replay` pair directly) with seeded acks + backlog:
  - `divergence_record_none_when_caught_up`: `current == min_acked` ⇒ `None` (pins the `end > start`
    gate that is `current > min_acked` today).
  - `divergence_record_none_when_backlog_empty_past_start`: `current > min_acked` but no writes with
    `offset > start` ⇒ `None` (pins the `!writes.is_empty()` gate).
  - `divergence_record_window_and_writes`: acks at `min_acked`, several writes past it ⇒
    `Some { start == min_acked, end == current, writes == (start, current] }`, offset-ordered — the
    exact fact no current test covers.
  - `divergence_record_no_streaming_replicas_uses_zero_floor`: `min_acked()` is `None` ⇒ `start == 0`,
    whole backlog is divergent (pins the `unwrap_or(0)` default).
- **`server` tests unchanged in intent**: `demotion_fires_when_split_brain_log_disabled` and
  `demotion_identical_whether_or_not_log_enabled` (`frogdb-server/crates/server/src/server/cluster_init.rs:793, 830`) stay green (demotion
  side-effect untouched); `noop_logger` still exercises the logger-present, no-divergence arm with a
  two-field-lighter struct.
- **Regression guard**: the `SplitBrainLogHeader` field mapping (`start→seq_diverge_start`,
  `end→seq_diverge_end`, `writes.len()→ops_discarded`) is asserted once in `server` so the format
  contract cannot silently drift when the computation moves.
- Full-suite `just test` on a Blacksmith testbox after the two-crate change.

## Risks & alternatives

- **Behavioral parity of the `unwrap_or(0)` floor and the two gates.** The whole value is that these
  move *unchanged*; the migration lands the `replication` unit tests (step 2) before the `server`
  rewrite (step 3) so any drift in the `end > start` / non-empty / zero-floor semantics is caught
  synchronously, not through a booted demotion path. This is the one place a careless edit changes
  observable behavior (a spurious or missing split-brain log + telemetry).
- **Snapshot-vs-live read.** `log` today reads `current` and `min_acked` at slightly different
  instants across two handles; `divergence_record` reads them from one coordinator, still without a
  lock spanning both. The window is advisory operator evidence (Redis writes a discarded-writes log
  too), and the extraction filter is `offset > start` against a non-destructive backlog, so a
  concurrent `advance` between the two reads only widens `end` — never truncates the write set below
  `start`. Parity with today (no tighter, no looser). No new lock is warranted; if exactness were ever
  required it would be a *separate* coordinator concern, not this proposal's.
- **`DivergenceRecord` allocation.** Returns an owned `Vec<(u64, Bytes)>` exactly as
  `extract_divergent_writes` does today (`Bytes` clones are refcount bumps); the demotion path is
  rare, so no `SmallVec` optimization is warranted.
- **Alternative — push the whole header into `replication`.** Rejected: `old_primary` / `new_primary`
  / `epoch_old` / `epoch_new` come from the `DemotionEvent`, a Raft-Metadata-Plane value the handler
  must not know (ADR-0001 keeps metadata out of the data-path module). The clean seam is
  handler-returns-`{start,end,writes}`, `server`-formats-the-header — which is precisely this split.
- **Alternative — put `divergence_record` on `OffsetCoordinator`.** Rejected: the coordinator owns
  offsets but not the backlog (`replay`); the record needs both. The handler is the one type that owns
  the coordinator *and* the backlog, so it is the correct owner — the same "one place that knows what
  the backlog contains and what offsets it can serve" locality `PartialSyncReplay`'s doc already claims
  for the PSYNC side (`frogdb-server/crates/replication/src/primary/replay.rs:36-40`).
- **Crate dependency direction.** Unchanged and respected: the new type/method live in `replication`;
  `server` already depends on `replication`; nothing in `replication` gains a dependency (the
  `DemotionEvent` and telemetry stay entirely in `server`). No crate reaches into another's internals
  it did not already.

## Effort

**S–M.** Add one struct + one method in `replication` (computed from two fields it already owns),
port the extraction visibility, and rewrite one `server` struct + its `log` body to drop two fields
and read one record. Blast radius: two files in `replication` (`frogdb-server/crates/replication/src/primary/mod.rs`, plus tests) and one
in `server` (`frogdb-server/crates/server/src/server/cluster_init.rs`), all on an existing crate edge, compiler-guided. The M half is
writing the four `replication` unit tests that pin behavior nothing currently covers — which is the
point of the change, and cheap now that the computation has a single owner.

## Related

- **Issue 07 (demotion decoupling).** `DemotionConsumer` was already refactored so that split-brain
  *logging* is an opt-out side-effect while the Role Demotion is not (`frogdb-server/crates/server/src/server/cluster_init.rs:686-708`
  comments; "the seam that decouples cluster failover behavior from logging configuration (issue 07)").
  This proposal completes that decoupling one layer down: it moves the *computation* the logger
  performs into the module that owns its inputs, so the `server`-side logger is purely a formatter and
  the divergence math is testable without the demotion wiring at all.
- **Proposal 10 (`apply_command` owns event derivation).** Same shape on the cluster side — a `server`
  consumer re-deriving a value its owning module should return. Here the owner is
  `PrimaryReplicationHandler`; there it is `apply_command`.
- **Proposal 06 / 12 (SnapshotScheduler depth, coordinator surface trim).** Same "push the computation
  down to the deep module, leave the caller a thin formatter" pattern; 12's deletion-test framing
  applies to the `broadcaster`/`tracker` logger fields deleted here.
- **ADR-0001 (Raft owns metadata; data path never through Raft).** Untouched: the divergence record is
  a pure data-path/offset computation; the `DemotionEvent` (metadata-plane) fields stay in `server`.

## Adversarial review

**Verdict: CONFIRMED.** The premise verified fully against source. The load-bearing equivalence — that
on a Primary the `SplitBrainLogger`'s three collaborators (broadcaster, tracker, primary_handler) all
resolve to the one `PrimaryReplicationHandler` and its `OffsetCoordinator` — is provable, not merely
plausible: `replication_init.rs:71-103` threads a single `tracker` `Arc` into both the handler and the
returned `Some(tracker)`, and `PrimaryReplicationHandler::new` (`primary/mod.rs:103`) builds its
`OffsetCoordinator` from that same `tracker.clone()`. So `handler.offsets.min_acked()` ==
`tracker.min_acked_offset()` and `handler.offsets.current()` == `broadcaster.current_offset()`
(`primary/mod.rs:317-319`). Behavior is preserved bit-for-bit: the two gates (`end > start`,
`!writes.is_empty()`), the `offset > start` extraction filter (`ring_buffer.rs:79`), and the
`unwrap_or(0)` floor all move unchanged, and the short-circuit order (no extraction when not diverged)
is preservable. Feasibility confirmed: all three reads are synchronous, the crate direction
`server → replication` is unchanged with no new dependency, `DemotionEvent`/telemetry correctly stay in
`server`, and the path is failover-only so there is no hot-path cost. Visibility tightening is safe —
`cluster_init.rs:639` is the sole external caller of the handler's `extract_divergent_writes`. The
testability gap is genuine: no current test pins "these acks + this backlog ⇒ this `(start, end, writes)`"
at a single owner.

Both issues raised were **minor, doc-level, and required no design change**:

1. **Abbreviated file:line paths** (e.g. `server/src/server/cluster_init.rs`). *Resolved.* Every
   citation is now rooted at the real tree location under `frogdb-server/crates/` (server files under
   `frogdb-server/crates/server/src/server/…`, replication files under
   `frogdb-server/crates/replication/src/…`). Line numbers were already exact and every file resolved;
   this was a cosmetic prefix omission only.

2. **`SplitBrainLogHeader { …, .. }` struct-update in the `log` sketch.** The reviewer noted
   `SplitBrainLogHeader` (`frogdb-server/crates/replication/src/split_brain_log.rs:30-47`) derives no
   `Default`, so a trailing `..` would not compile — the `timestamp` field plus the four
   `DemotionEvent`-derived fields must be assigned explicitly. *Resolved.* The sketch now lists all five
   header fields (`timestamp`, `old_primary`, `new_primary`, `epoch_old`, `epoch_new`) as explicit
   assignments derived from `event` exactly as today, with no `..`. This was sketch imprecision only —
   the proposal always labeled the block "Signatures/types only" and specified the header construction
   is copied unchanged.
