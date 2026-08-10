# Proposal 42 — `WriteSink::commit` returns `CommitEffects`; the flush engine applies them

*Round 38–99, persistence lane (candidate PE6). **Spec-first**: touches
FM-PERSISTENCE-035 and FM-PERSISTENCE-012. LOCKED crate — `frogdb-persistence`,
mutation gate 0.85.*

## Summary

`WriteSink` is the storage seam the WAL flush engine writes through
(`flush.rs:112–126`). Its **interface** says a commit does one thing: "atomically
commit all staged entries, syncing to disk if `sync`" (`flush.rs:122–123`). The
production **adapter** does three: it writes the batch, it spawns an
asynchronous space-reclamation pass when the batch carried a full-shard range
tombstone, and — on a `sync = true` commit — it rewrites the persisted
durable-sync WAL watermark, the side-file FM-PERSISTENCE-035 reads at the next
open to decide whether to tell an operator they lost data
(`flush.rs:181–210`).

Neither extra effect appears in the signature, the trait doc, or the return
value. Two consequences follow. First, the **durability-mode policy is split in
half across two modules**: the engine decides `is_sync` from `DurabilityMode`
and applies the in-memory half itself (`record_success(max_seq, self.is_sync)`
at `flush.rs:588`/`644` → `FlushOutcomes::synced_seq`), while the on-disk half
of the very same decision is re-derived inside the adapter from the `sync`
argument it was handed. Second, **the hidden half is only reachable through a
real RocksDB**: the engine's two substitute adapters (`TestSink`,
`PageCacheSink`) cannot express either effect, so every assertion about the
watermark has to open a store on disk.

This proposal makes `commit` return a `CommitEffects` value describing what the
commit earned, and gives the seam an explicit `settle` step the engine calls to
apply it. Zero behavior change is the acceptance criterion — the same watermark
writes and the same reclamation spawns, in the same order, from the same
threads. What changes is that the effects become **declared, ordered by the
caller, and assertable without RocksDB**.

## Files involved (verified paths + counts)

| Path | Lines | Role |
|------|-------|------|
| `frogdb-server/crates/persistence/src/wal/flush.rs` | 827 | `WriteSink` trait (112–126), `RocksSink` (129–215) incl. the dishonest `commit` (181–210); `FlushEngine` commit sites (586, 640); `FlushOutcomes::record_success` (271–280) |
| `frogdb-server/crates/persistence/src/wal/tests.rs` | 2343 | `TestSink` (242–296), `PageCacheSink` (1061–1103), the direct-`RocksSink` block (1618–1712) incl. the FM-035 forcing test (1649–1685) |
| `frogdb-server/crates/persistence/src/wal/writer.rs` | 394 | Sole production construction site: `FlushEngine::new(RocksSink::new(rocks, shard_id), …)` (64–71); `register_sync_target` (51–56) |
| `frogdb-server/crates/persistence/src/rocks/checkpoint.rs` | 132 | `RocksStore::record_wal_watermark` (22–34) — the hidden effect's implementation |
| `frogdb-server/crates/persistence/src/rocks/mod.rs` | 692 | `durable_sync` (320–347, records the watermark at 345); `batch_clear_shard` (502–513); `spawn_clear_reclamation` (562–570) |
| `frogdb-server/crates/persistence/src/rocks/reclaim.rs` | 241 | The reclamation pass and its per-`(tier, shard)` coalescing guard (105–138) |
| `frogdb-server/crates/persistence/src/rocks/wal_watermark.rs` | 250 | Watermark side-file read/write + `detect_and_reset`; the "can only lag" argument (1–35) |
| `.scratch/hardening/specs/persistence-failure-modes.md` | — | FM-PERSISTENCE-012 (198–208), FM-PERSISTENCE-035 (602–612), FM-PERSISTENCE-043 (102–112) |

Blast radius outside the crate: **none**. `WriteSink`, `RocksSink` and
`FlushEngine` are all `pub(super)` inside `wal`; the only production caller is
`RocksWalWriter::new`.

## Problem

### The interface and the implementation disagree

The declared contract (`flush.rs:122–123`, verbatim):

```rust
/// Atomically commit all staged entries, syncing to disk if `sync`.
fn commit(&mut self, sync: bool) -> std::io::Result<()>;
```

The production adapter (`flush.rs:181–210`, verbatim body after the batch write):

```rust
        if result.is_ok()
            && let Some(upper) = pending_clear
        {
            self.rocks
                .spawn_clear_reclamation(crate::rocks::CfTier::Main, self.shard_id, upper);
        }
        // A successful `sync` commit has fsync'd the WAL through this batch's
        // sequence, so advance the durable-sync watermark. …
        if result.is_ok() && sync {
            self.rocks.record_wal_watermark();
        }
        result
```

So a call the caller reads as "commit this batch" may also (a) spawn an OS
thread that runs `delete_file_in_range_cf` plus a forced bottommost
`CompactRange` over the shard (`reclaim.rs:105–138`), and (b) perform a
temp-write + rename of a file inside the RocksDB directory
(`wal_watermark.rs:69+`) whose contents an operator-facing data-loss alarm is
computed from on the next boot. Per LANGUAGE.md, **interface** is *everything a
caller must know to use the module correctly*. The type signature is the whole
of what this seam publishes, and it omits both.

### One policy, two homes

The watermark rule is a durability-mode rule, and the engine already owns
durability mode. `FlushEngine` derives `is_sync` from `DurabilityMode` once
(`flush.rs:424`) and applies the in-memory half of the rule itself
(`flush.rs:271–280`):

```rust
    pub(super) fn record_success(&self, max_seq: u64, synced: bool) {
        self.committed_seq.store(max_seq, Ordering::Release);
        if synced {
            self.synced_seq.fetch_max(max_seq, Ordering::AcqRel);
        }
```

The on-disk half of the *same* rule lives in the adapter, gated on the same
condition (`flush.rs:206`, `result.is_ok() && sync`). Two modules independently
answer "did this commit make anything durable?", and the spec — FM-PERSISTENCE-043
for the in-memory sequence, FM-PERSISTENCE-035 for the on-disk watermark — treats
them as one guarantee. A future change to one is not forced to visit the other.

### The consequence for tests

The engine's whole test story is that the batching, threshold, timeout and
error-propagation logic is exercised against substitute adapters with no
RocksDB (`tests.rs:226–232`, verbatim: *"These drive the real flush thread loop
against an in-memory sink with failure injection…"*). Both substitutes
necessarily drop the hidden effects on the floor — `TestSink::commit`
(`tests.rs:286–294`) ignores `sync` entirely (`_sync`), and
`PageCacheSink::commit` (`tests.rs:1088–1101`) models fsync but knows nothing of
a watermark. Therefore the only place the watermark rule is asserted is
`only_a_synced_rocks_sink_commit_records_the_durable_watermark`
(`tests.rs:1649–1685`), which opens a real store in a `TempDir` and reads the
side-file back off disk. The interface *is* the test surface; when the
interface hides an effect, the test has to go around it.

### A trap the value type has to respect

The watermark records `self.db.latest_sequence_number()` — **RocksDB's**
sequence (`checkpoint.rs:29–33`). The engine's `batch_max_seq` is FrogDB's own
per-shard WAL counter, a different numbering space entirely. `CommitEffects`
must therefore carry *"a durable point was reached"*, never *"record sequence
N"*: a value type that looked like it carried the sequence would invite exactly
the conflation FM-PERSISTENCE-043 exists to prevent.

## Spec position — this change is spec-first with a zero-delta target

| Row | Where | What it pins here |
|---|---|---|
| **FM-PERSISTENCE-035** (spec:602–612) | the watermark | *"`frogdb_wal_watermark` … is advanced only by successful sync commits — never by a checkpoint"*; the detector must never false-alarm |
| **FM-PERSISTENCE-043** (spec:102–112) | `synced_seq` | *"`synced_seq` is `fetch_max`'d only when that commit passed `sync = true`"* |
| **FM-PERSISTENCE-012** (spec:198–208) | the clear barrier | *"Reclamation … runs **after** the clear commits and is coalesced per `(tier, shard)`"* |

Forcing tests, all inside `frogdb-persistence` (so `cargo mutants -p
frogdb-persistence` actually runs them):

- FM-035: `only_a_synced_rocks_sink_commit_records_the_durable_watermark`
  (`wal/tests.rs:1655`), `test_wal_sequence` (`wal/tests.rs:145`),
  `the_watermark_file_round_trips_and_degrades_to_none` /
  `a_recovery_that_reaches_the_watermark_reports_nothing` /
  `a_short_recovery_reports_the_gap_once_and_re_baselines`
  (`rocks/wal_watermark.rs:162, 189, 220`) — plus `test_wal_sequence_persistence`
  and `test_async_wal_writer_recovery`.
- FM-043: `durable_sequence_tracks_only_fsynced_commits_in_periodic_mode`
  (`wal/tests.rs:1432`), `sync_mode_durable_sequence_equals_committed_sequence`
  (1518), `empty_flush_advances_neither_sequence` (1540),
  `durable_sync_publishes_the_flushed_sequence_to_every_registered_writer`
  (`rocks/tests.rs:1355`).
- FM-012: `test_sink_clear_is_a_flush_barrier_between_batches` (543),
  `test_sink_clear_advances_committed_sequence` (578),
  `test_wal_clear_reclamation_end_to_end` (929),
  `clear_reclamation_keeps_data_gone_after_reopen` (`rocks/tests.rs:796`),
  `clear_reclamation_preserves_post_clear_writes` (878).

**Acceptance criterion (state it in the PR and hold to it).** The change is a
pure interface move: after it lands, a production run must issue *the same
watermark writes and the same reclamation spawns, for the same commits, in the
same order, on the same threads* as before. Concretely: `write_batch_opt` →
`spawn_clear_reclamation` → `record_wal_watermark` → `record_success`, exactly
as today (the effects are applied **before** `record_success`, preserving the
current ordering of the file write relative to the `synced_seq` store — see
Risks). Because the observable behavior is unchanged, **no FM row's
Trigger/Observable/NOT observable/Invariant text changes** and **no test is
renamed**, so the `Forced by` lists stay byte-identical and
`just lint-failure-modes` needs no edit. The forcing tests are *edited in place*
(they gain the `settle` call), not moved. If review concludes any ordering must
shift, that is a behavior change and the row is edited first.

**One spec-text amendment is warranted and should ride along (doc-only).**
FM-PERSISTENCE-035's Invariant says the watermark *"is advanced only by
successful sync commits"*, but `RocksStore::durable_sync` also records it
(`rocks/mod.rs:345`) — which is what FM-PERSISTENCE-003's Invariant (spec:85)
already describes (*"re-record the on-disk WAL watermark"*). The 035 sentence is
incomplete as written. Correcting it to "advanced only by a successful *sync*
commit or by an out-of-band `durable_sync` — never by a checkpoint" costs
nothing and removes a contradiction this proposal would otherwise inherit.

## Proposed change

Add a small value type and one method to the seam; move both effect
applications out of `commit` into the method the caller drives.

```rust
/// What a successful commit earned beyond "the batch reached storage".
///
/// Pure data. A sink *reports* effects; the flush engine — which owns
/// durability policy — decides when to apply them, via [`WriteSink::settle`].
/// Deliberately carries no sequence number: the durable watermark is
/// RocksDB's sequence space, not the engine's WAL counter.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct CommitEffects {
    /// The commit fsynced through its own batch, so the persisted durable-sync
    /// watermark may be advanced (FM-PERSISTENCE-035 / FM-PERSISTENCE-043).
    pub(super) durable: bool,
    /// The commit landed a full-shard range tombstone, so post-clear space
    /// reclamation is owed for this shard (FM-PERSISTENCE-012).
    pub(super) cleared_shard: bool,
}

pub(super) trait WriteSink: Send {
    // … stage_put / stage_delete / stage_merge / stage_clear unchanged …

    /// Atomically commit all staged entries, syncing to disk if `sync`.
    /// Reports what the commit earned; applying it is [`Self::settle`]'s job,
    /// so this call performs no work beyond the commit itself.
    fn commit(&mut self, sync: bool) -> std::io::Result<CommitEffects>;

    /// Apply the follow-ups a successful commit earned. Never on the
    /// correctness path: a sink that no-ops here still commits correctly — it
    /// merely reclaims disk space later and forfeits a corruption-detection
    /// signal on the next open. Called only for a commit that returned `Ok`.
    fn settle(&mut self, effects: CommitEffects) {}
}
```

`RocksSink::commit` shrinks to the batch write plus a report:

```rust
    fn commit(&mut self, sync: bool) -> std::io::Result<CommitEffects> {
        let batch = std::mem::take(&mut self.batch);
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(sync);
        let pending_clear = self.pending_clear_upper.take();
        self.rocks
            .write_batch_opt(batch, &write_opts)
            .map_err(std::io::Error::other)?;   // a failed commit drops the
                                                // staged clear bound, as today
        self.pending_clear_bound = pending_clear;   // consumed by `settle`
        Ok(CommitEffects { durable: sync, cleared_shard: pending_clear_was_some })
    }

    fn settle(&mut self, effects: CommitEffects) {
        if effects.cleared_shard && let Some(upper) = self.pending_clear_bound.take() {
            self.rocks
                .spawn_clear_reclamation(crate::rocks::CfTier::Main, self.shard_id, upper);
        }
        if effects.durable {
            self.rocks.record_wal_watermark();
        }
    }
```

(The bound itself stays inside the sink — it is a RocksDB key, not something the
engine should hold. `CommitEffects` carries the *decision*; the sink keeps the
*datum*. An implementation that prefers to carry the `Vec<u8>` in the effects
value is acceptable but leaks a storage detail into the engine's vocabulary and
is not recommended.)

Both `FlushEngine` commit sites become:

```rust
        match self.sink.commit(self.is_sync) {
            Ok(effects) => {
                self.sink.settle(effects);                       // same order as today
                self.outcomes.record_success(max_seq, effects.durable);
                …
```

Note `record_success(max_seq, effects.durable)` replaces
`record_success(max_seq, self.is_sync)` (`flush.rs:588`, `644`): the engine now
reads "did it fsync?" from the adapter that performed the commit rather than
re-deriving it from configuration. Every adapter returns `durable: sync`, so the
value is identical today — but the two halves of the durability rule are now
sourced from **one** answer instead of two, which is the point.

### Alternatives considered

- **Inject a separate `CommitEffectApplier` collaborator into `FlushEngine`.**
  Rejected: it adds a *second* seam whose only production adapter is the
  `RocksStore` the sink already holds, and splits "who talks to storage" across
  two engine fields. One adapter is a hypothetical seam.
- **Route effects up to `RocksWalWriter`, which holds the `Arc<RocksStore>`.**
  Rejected: the writer never sees individual commits — the flush thread owns the
  engine (`writer.rs:64–82`). Carrying per-commit effects back across that thread
  boundary is strictly worse than the two-call protocol.
- **Document-only: declare both effects in the trait doc, change no code.** This
  is the cheap fallback and is worth landing on its own regardless (see
  *Independently-landable hotfix*). It fixes the honesty gap for a human reader
  but not for a test: the substitutes still cannot express the effects, and the
  policy stays split across two modules.

## Testability improvement

**Yes — the effects become assertable without a real RocksDB, which is the
concrete win.** `TestSink` grows a recorded `Vec<CommitEffects>` (three lines
beside its existing `committed` log at `tests.rs:245`), and the following become
plain in-memory engine tests on the flush thread that already exists:

- A `sync`-mode engine reports `durable: true` on every successful commit; a
  `periodic`/`async` engine never does — the FM-043 rule asserted at the seam
  rather than only through `FlushOutcomes`.
- A commit that **fails** settles nothing: `settle` is unreachable on the `Err`
  arm by construction, so "a dropped batch never advances the watermark" is
  pinned by the type, not by a comment.
- A clear commit reports `cleared_shard: true`; the *next* ordinary commit
  reports `false` — i.e. the pending bound is consumed exactly once, which is
  today only implied by `pending_clear_upper.take()`.
- Ordering: `settle` is observed before the `synced_seq` store, so the on-disk
  watermark can never trail a published in-memory durable sequence.
- `empty_flush_advances_neither_sequence` extends naturally to "an empty flush
  settles nothing" — the `staged_len() == 0` early return (`flush.rs:621–623`)
  never reaches the seam at all.

What stays RocksDB-backed, deliberately: the FM-035 forcing test
`only_a_synced_rocks_sink_commit_records_the_durable_watermark`
(`tests.rs:1655`) keeps opening a real store and reading the side-file — it is
the only thing that proves the *file* is written. It gains one line
(`let fx = sink.commit(true).unwrap(); sink.settle(fx);`) and keeps its name, so
the FM row's `Forced by` list is untouched. Likewise the reclamation end-to-end
tests (`tests.rs:929`, `rocks/tests.rs:796, 878`) drive the real writer and are
unaffected.

**Mutation re-gate.** New mutable surface is small and each mutant has an
in-crate killer: `durable: sync` → `true`/`false` dies on
`only_a_synced_rocks_sink_commit_records_the_durable_watermark` and the FM-043
trio; `if effects.durable` → `true` dies on the same; `cleared_shard` mutants
die on `clear_reclamation_*` and `test_wal_clear_reclamation_end_to_end`. The
default `settle` body (`{}`) on the trait is a genuine mutation-testing hazard —
it is already the identity — so give the trait method **no** default body and
implement it explicitly (as a no-op) on `TestSink`/`PageCacheSink`, which also
forces any future adapter to make a conscious choice. Run
`just mutants-diff frogdb-persistence` before pushing; the crate sits at 99.1%
against an 0.85 gate, so there is no headroom argument for skipping it.

## Risks / open questions

- **Highest blast radius in the persistence lane — sequence this proposal
  last.** It is the only lane item that edits the durability decision itself,
  and it rewrites the two commit sites (`flush.rs:586`, `640`) that proposals
  **41** (`record_commit_outcome` extraction, PE7 — merges the duplicated
  outcome/metrics blocks at `flush.rs:559–613` and `620–678`) also rewrites.
  Land **41 first**: it collapses the two success arms into one function, after
  which this proposal edits *one* site instead of two, and the conflict
  disappears. Ordering hint for the dependency graph: **41 → 42**; 42 after
  every other persistence item.
- **Effect-vs-`record_success` ordering is behavior.** Applying `settle` after
  `record_success` would let a reader observe an advanced `synced_seq` while the
  watermark file still holds the older value. That direction is *safe* under
  FM-035's "may only lag" asymmetry, but it is still an observable change and
  therefore out of scope for a zero-delta refactor. Apply effects first, exactly
  as `commit` does today. Reviewers should treat any reordering as requiring a
  spec edit.
- **`CommitEffects` must not grow a sequence number.** See the numbering-space
  trap above. If a later change genuinely needs the sequence, it belongs in
  RocksDB's space and should be read from the store, not passed through the
  engine.
- **`durable` currently equals `sync` in every adapter — is the field earning
  its keep?** Today it is a rename of an argument the caller already has. It
  earns its keep the moment an adapter's commit can fsync-or-not for reasons the
  caller does not control (a batched group commit, a `WriteOptions` fallback), and
  it is what makes `record_success` source both halves of the rule from one
  answer. Reviewers who disagree should say so explicitly — dropping `durable`
  and keeping only `cleared_shard` is a coherent smaller version of this
  proposal, at the cost of leaving the policy split.
- **Scope boundaries vs siblings.** No overlap with **38**
  (`SnapshotLayout`/`SnapshotFs` read half) or **44** (`RocksStore::open` shims —
  the open path's FM-tagged ordering is a different file region and a different
  spec row). **40** (frame codec) is a separate module entirely. Only **41**
  overlaps, and only textually; see the ordering hint. This proposal deliberately
  does **not** touch `RocksStore::durable_sync` (`rocks/mod.rs:328–347`) or
  `clear_tier_shard` (`rocks/mod.rs:539–556`), both of which call the same two
  store methods on their own paths — folding those in would be a real behavior
  question, not a code move.
- **Narrow, unverified adjacency worth a reviewer's eye, not a claim.** The
  watermark records the store-global `latest_sequence_number()` while the fsync
  it is credited to covered one shard's batch. RocksDB publishes a sequence only
  after the write group's WAL write (and its sync) completes, so a concurrent
  shard's unsynced write should not be visible in that read — but the argument
  rests on RocksDB internals, not on anything FrogDB pins. It is unchanged by
  this proposal either way; flagged only because making the effect explicit is
  what surfaces the question.

## Effort estimate

**M.** The mechanical part is small and contained — one value type, one trait
method, one adapter rewritten, two engine call sites, three test sinks — and it
does not leave the `wal` module. What makes it M rather than S is the
surrounding discipline: it is a LOCKED crate touching three FM rows, so it costs
a spec read, in-place edits to the FM-035 and FM-012 forcing tests, a
`just lint-failure-modes` pass, and a `just mutants-diff frogdb-persistence`
run before push. Budget the review, not the diff.

### Independently-landable hotfix

**Yes, and it should land regardless of whether the refactor is accepted.** Two
zero-risk, no-behavior edits that remove most of the *documented* dishonesty
immediately:

1. **Amend the `WriteSink::commit` doc** (`flush.rs:122–123`) to name both
   effects and say that they run only on success — so the next reader of the
   seam learns from the interface instead of from the adapter body. ~5 lines,
   no code.
2. **Fix the FM-PERSISTENCE-035 Invariant sentence** (spec:609) to include
   `durable_sync` as an advancer, reconciling it with FM-PERSISTENCE-003's
   Invariant (spec:85). Doc-only; no test or `Forced by` change.

Neither conflicts with proposal 41, and both are strictly subsumed by this
proposal if it lands.
