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

Forcing tests. These rows are **not** forced exclusively from inside
`frogdb-persistence`: two of the three name tests that live in `frogdb-core`,
which `cargo mutants -p frogdb-persistence` never runs. The in-crate subset is
what the mutation gate actually sees, so it is called out separately below.

- FM-035, **in crate** (these are the killers the gate can use):
  `only_a_synced_rocks_sink_commit_records_the_durable_watermark`
  (`wal/tests.rs:1655`), `test_wal_sequence` (`wal/tests.rs:146`),
  `wal_clean_reopen_recovers_all_without_signal` (`rocks/tests.rs:1290`),
  `the_watermark_file_round_trips_and_degrades_to_none` /
  `a_recovery_that_reaches_the_watermark_reports_nothing` /
  `a_short_recovery_reports_the_gap_once_and_re_baselines`
  (`rocks/wal_watermark.rs:167, 194, 225`). **Out of crate**, and therefore
  worth nothing to the gate: `test_wal_sequence_persistence`
  (`core/src/persistence/crash_recovery_tests.rs:1752`) and
  `test_async_wal_writer_recovery` (same file, 1685).
- FM-043, all in crate:
  `durable_sequence_tracks_only_fsynced_commits_in_periodic_mode`
  (`wal/tests.rs:1432`), `…_in_async_mode` (1488),
  `sync_mode_durable_sequence_equals_committed_sequence` (1518),
  `empty_flush_advances_neither_sequence` (1540),
  `durable_sync_publishes_the_flushed_sequence_to_every_registered_writer`
  (`rocks/tests.rs:1355`).
- FM-012, **in crate**: `test_sink_clear_is_a_flush_barrier_between_batches`
  (`wal/tests.rs:543`), `test_sink_clear_advances_committed_sequence` (578),
  `test_wal_clear_reclamation_end_to_end` (929),
  `clear_reclamation_keeps_data_gone_after_reopen` (`rocks/tests.rs:796`),
  `clear_reclamation_preserves_post_clear_writes` (878). **Out of crate**:
  `clear_shard_routes_to_clear` (`core/src/shard/persistence.rs:631`).

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

**A spec-text contradiction was found here but is deliberately *not* part of
this proposal.** FM-PERSISTENCE-035's Invariant says the watermark *"is advanced
only by successful *sync* commits — never by a checkpoint"*, and that is
incomplete: three other paths write it — `RocksStore::durable_sync`
(`rocks/mod.rs:345`), the fresh-open seed (`rocks/mod.rs:295`), and
`detect_and_reset`'s post-recovery re-baseline (`rocks/wal_watermark.rs:121`).
The module doc already uses the umbrella phrasing this needs
(`wal_watermark.rs:23–25`, *"only ever advanced after a durable sync"*), and
FM-PERSISTENCE-043's Invariant (spec:109) shows the spec's own
explicit-inclusion style (*"or by `publish_synced_through` from
`RocksStore::durable_sync`"*). Editing a locked row here would contradict this
proposal's own zero-spec-delta acceptance criterion, so it is **filed as a
standalone doc-only change owned by the orchestrator**, on its own: replace the
sentence with *"advanced only after something durable — a sync commit, an
out-of-band `durable_sync`, or an open-time (re-)baseline — never by a
checkpoint."* Proposal 42 inherits whichever wording is in the file when it
lands and changes no spec text itself.

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
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct CommitEffects {
    /// The commit fsynced through its own batch, so the persisted durable-sync
    /// watermark may be advanced (FM-PERSISTENCE-035 / FM-PERSISTENCE-043).
    pub(super) durable: bool,
    /// The commit landed a full-shard range tombstone whose upper bound this
    /// is, so post-clear space reclamation is owed for this shard
    /// (FM-PERSISTENCE-012). `None` for an ordinary commit. The bound travels
    /// *in* the value so it can be owed at most once — see below.
    pub(super) cleared_shard: Option<Vec<u8>>,
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
    ///
    /// No default body on purpose: every adapter states its choice (see the
    /// mutation re-gate note).
    fn settle(&mut self, effects: CommitEffects);
}
```

`RocksSink::commit` shrinks to the batch write plus a report:

```rust
    fn commit(&mut self, sync: bool) -> std::io::Result<CommitEffects> {
        let batch = std::mem::take(&mut self.batch);
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(sync);
        // Taken before the write, so a failed commit drops the staged bound
        // exactly as today (`flush.rs:185`).
        let cleared_shard = self.pending_clear_upper.take();
        self.rocks
            .write_batch_opt(batch, &write_opts)
            .map_err(std::io::Error::other)?;
        Ok(CommitEffects { durable: sync, cleared_shard })
    }

    fn settle(&mut self, effects: CommitEffects) {
        if let Some(upper) = effects.cleared_shard {
            self.rocks
                .spawn_clear_reclamation(crate::rocks::CfTier::Main, self.shard_id, upper);
        }
        if effects.durable {
            self.rocks.record_wal_watermark();
        }
    }
```

**Carry the bound in the value, not in a parked sink field.** The obvious
alternative — keep a `pending_clear_bound: Option<Vec<u8>>` on the sink and let
`CommitEffects` carry only a `cleared_shard: bool` decision — was considered and
rejected. It makes "settle exactly once per commit" a *temporal* invariant
nothing enforces: a second `settle` (or a settle-less commit path) silently
mis-reclaims, and `stage_clear` would be writing `pending_clear_upper` while
`commit` writes a second field holding the same datum. Carrying the `Option<Vec<u8>>`
keeps today's single-expression consumption (`take()` on the way out at
`flush.rs:185`, consumed once at `195–200`) and makes over-settling
unrepresentable — the bound is *moved* into the effects value and moved out of
it again. The "it leaks a storage detail into the engine's vocabulary" objection
is thin: the engine never inspects the bytes, and it already passes opaque
storage keys straight through `stage_put`/`stage_delete`. Cost: `CommitEffects`
is `Clone` but not `Copy`, so the engine reads the flag out before handing the
value over (below).

Both `FlushEngine` commit sites become (or, once **41** has landed, the single
`record_commit_outcome` body — see the ordering bullet in *Risks* for the exact
inherited signature):

```rust
        match self.sink.commit(self.is_sync) {
            Ok(effects) => {
                let durable = effects.durable;                   // effects is moved next
                self.sink.settle(effects);                       // same order as today
                self.outcomes.record_success(max_seq, durable);
                …
```

Note `record_success(max_seq, durable)` replaces
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

- A commit that **fails** settles nothing: the `Err` arm has no `settle` call,
  so "a dropped batch never advances the watermark" becomes a property of the
  `match` shape plus a test that injects a commit failure and asserts an empty
  settle log. (It is *not* pinned by the type — `CommitEffects: Default` makes
  `sink.settle(CommitEffects::default())` a one-liner anyone could add. The
  guarantee is the call-site shape, held by a test.)
- Ordering: `settle` is observed before the `synced_seq` store, so the on-disk
  watermark can never trail a published in-memory durable sequence. This is the
  ordering assertion that has no equivalent today at all.
- `empty_flush_advances_neither_sequence` extends naturally to "an empty flush
  settles nothing" — the `staged_len() == 0` early return (`flush.rs:621–623`)
  never reaches the seam at all.

Two wins that look available but are **not** worth banking:

- *"A `sync`-mode engine reports `durable: true`, a `periodic`/`async` engine
  never does."* This is already asserted RocksDB-free by
  `test_fsync_seam_sync_flag_matches_mode` (`tests.rs:1261–1278`), which reads
  `PageCacheSink`'s recorded `commit_syncs` for all three modes. A `durable`
  assertion would restate it one field over. Real gain: nil.
- *"The pending clear bound is consumed exactly once."* Against `TestSink` this
  would test a re-implementation, not the production rule — `TestSink::stage_clear`
  (`tests.rs:281–287`) pushes a `TestOp::Clear` and tracks no bound at all, so
  the substitute would have to grow the very `take()` semantics under test. The
  honest coverage for consume-once stays RocksDB-backed
  (`test_wal_clear_reclamation_end_to_end`, `clear_reclamation_*`); what the
  refactor adds there is that the rule is now *unrepresentably* violable (the
  bound is moved, see the shape argument above) rather than merely tested.

What stays RocksDB-backed, deliberately: the FM-035 forcing test
`only_a_synced_rocks_sink_commit_records_the_durable_watermark`
(`tests.rs:1655`) keeps opening a real store and reading the side-file — it is
the only thing that proves the *file* is written. It gains one line
(`let fx = sink.commit(true).unwrap(); sink.settle(fx);`) and keeps its name, so
the FM row's `Forced by` list is untouched. Likewise the reclamation end-to-end
tests (`tests.rs:929`, `rocks/tests.rs:796, 878`) drive the real writer and are
unaffected.

**Mutation re-gate.** The new mutable surface is `RocksSink::settle` and the two
fields `commit` fills in. `RocksSink::settle` has a **non-empty body**, so
cargo-mutants will generate mutants for it (delete the `if effects.durable`
guard, replace it with `true`/`false`, delete the `spawn_clear_reclamation`
call), and the **only in-crate test that can kill the watermark ones is
`only_a_synced_rocks_sink_commit_records_the_durable_watermark`
(`wal/tests.rs:1655`) — after it gains the `settle` call.** That one-line test
edit is therefore *gate-load-bearing*, not cosmetic: skip it and the watermark
effect becomes unreachable from any `-p frogdb-persistence` test, and the
mutants survive. The `cleared_shard` mutants die on `clear_reclamation_*`
(`rocks/tests.rs:796, 878`) and `test_wal_clear_reclamation_end_to_end`
(`wal/tests.rs:929`), which drive the real writer and need no edit. `durable:
sync` → `true`/`false` in `commit` dies on the same watermark test.

Give the trait method **no** default body and implement it explicitly (as a
no-op) on `TestSink`/`PageCacheSink` — on **design** grounds: it forces any
future adapter to make a conscious choice about the effects rather than
inheriting silence. It is *not* a mutation-testing argument: cargo-mutants
27.1.0 (pinned in `.mise.toml`) skips empty-body functions, trait defaults
included, so a defaulted `fn settle(&mut self, _: CommitEffects) {}` generates
zero mutants — as do the explicit no-op impls on the test sinks, which are
`#[cfg(test)]` and skipped outright. Run `just mutants-diff frogdb-persistence`
before pushing; the crate sits at 99.1% against an 0.85 gate, so there is no
headroom argument for skipping it.

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

  **The inherited signature, stated so both implementers agree.** 41's PE7
  extraction takes `result: std::io::Result<()>` and leaves the `self.sink.commit(…)`
  call at *both* original sites, passing the result down. 42 therefore makes two
  changes to 41's output, not one:

  1. widen the parameter to `result: std::io::Result<CommitEffects>`;
  2. move `self.sink.settle(effects)` **inside** `record_commit_outcome`, on the
     `Ok` arm, immediately before `record_success` — so the
     settle-before-`record_success` ordering (a behavior guarantee, see the next
     bullet) lives in exactly one place instead of being duplicated at the two
     call sites 41 just finished de-duplicating.

  With that contract fixed, the sequencing 41 → 42 is sufficient; no other
  coordination between the two changes is needed.
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

**Yes, and it should land regardless of whether the refactor is accepted.** One
zero-risk, no-behavior edit removes most of the *documented* dishonesty
immediately: **amend the `WriteSink::commit` doc** (`flush.rs:122–123`) to name
both effects and say that they run only on success — so the next reader of the
seam learns from the interface instead of from the adapter body. ~5 lines, no
code. It does not conflict with proposal 41 and is strictly subsumed by this
proposal if it lands.

(The FM-PERSISTENCE-035 Invariant wording fix is deliberately **not** bundled
here — see *Spec position* above. It is a standalone doc-only change owned by
the orchestrator, because this proposal's acceptance criterion is zero spec
delta.)
