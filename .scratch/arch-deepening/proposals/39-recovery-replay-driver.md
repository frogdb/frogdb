# Proposal 39 — Move the recovery replay driver into `frogdb-persistence`

## Summary

`frogdb-core::persistence::store_recovery` opens with a module doc that calls
itself "the thin store-side adapter" whose only job is "deciding *how* a
recovered entry lands in a `HashMapStore`". The `RestoreSink` implementation
above it (store_recovery.rs:40–58) is exactly that. The function below it is
not: `recover_all_shards` (store_recovery.rs:78–126) owns **shard iteration
order**, **hot-before-warm pairing per shard**, **sink lifetime across the
hot/warm pair** (the thing that makes warm-tier precedence work at all), and
**first-failure precedence across shards and tiers**. Every one of those is a
rule of the persistence *protocol*, not of the store — and the protocol's own
module says so: `frogdb-persistence`'s `recovery.rs` doc states it "owns the
*sequencing* of turning persisted RocksDB state back into live entries"
(recovery.rs:3–10). Today it owns the sequencing of one shard; the whole-database
sequencing lives one crate away, behind a seam that was drawn for sink policy.

The consequence is not hypothetical. The failure-mode spec already reaches
across the seam to describe it: FM-PERSISTENCE-047's Invariant reads
"…and folded first-wins again in `recover_all_shards` where per-shard stats meet
the total" (persistence-failure-modes.md:525). A locked persistence failure mode
is partly implemented in `frogdb-core`, whose mutation gate is not the one that
covers it — and `cargo mutants -p frogdb-persistence` runs only that package's
own tests, so the fold is invisible to the 0.85 gate that is supposed to guard
it.

This proposal moves the driver to `recover_database_into` in
`frogdb_persistence::recovery`, generic over `RestoreSink`, and leaves
`recover_all_shards` in core as a sink factory plus unpacking — its public
interface unchanged, so all **14** call sites are untouched. The move deepens
the persistence recovery module (one call instead of three plus a fold), pulls a
spec-tagged rule under the gate that owns it, and makes the whole-database
protocol testable against the existing `MockSink` with no core store. It also
lands the `RecoveryStats::duration_ms` deletion, which is not optional: an
unasserted field in an 0.85-gated crate is an unkillable mutant, and the move is
what puts its last writer inside the gate.

## Files involved

| Path | Lines | Role |
|------|-------|------|
| `frogdb-server/crates/core/src/persistence/store_recovery.rs` | 309 | Home of the misplaced driver (`recover_all_shards`, 78–126) and of the genuine adapter (`StoreRestoreSink`, 28–58); `unit_tests` at 128–309 carry six FM tags |
| `frogdb-server/crates/persistence/src/recovery.rs` | 664 | The protocol module the driver belongs in: `RestoreSink` (114–130), `recover_shard_into` (138–194), `recover_warm_shard_into` (202–254), `RecoveryStats::record_first_failure` (89–93), `MockSink` (276–297) |
| `frogdb-server/crates/persistence/src/lib.rs` | 45 | Crate re-export surface: `pub use recovery::{RecoveryError, RecoveryStats, RestoreSink, recover_shard_into, recover_warm_shard_into}` (lines 25–27) **gains `recover_database_into`** — the third edited file |
| `frogdb-server/crates/core/src/persistence/mod.rs` | 26 | Re-export surface (`pub use store_recovery::{recover_all_shards, recover_shard}`, line 17) — unchanged by this proposal |
| `frogdb-server/crates/recovery/src/shards.rs` | 183 | Sole production caller (`restore`, line 50); reads the aggregate for FM-033/045/047 surfacing. The `recover_all_shards` call is untouched — signature is preserved — but **line 56 changes**: it binds `duration_ms = stats.duration_ms` in the "Recovery complete" `info!`, and the required `duration_ms` deletion (below) replaces it with a local `Instant` elapsed. One line, no logic |
| `frogdb-server/crates/replication-runtime/src/install.rs` | — | The **second** whole-database driver (`read_snapshot`, 237–256), spec-pinned at `replication-failure-modes.md:1136`. **Not edited, and deliberately not converged** — see [The second whole-database driver stays divergent](#the-second-whole-database-driver-stays-divergent) |
| `.scratch/hardening/specs/persistence-failure-modes.md` | — | FM-PERSISTENCE-047 Invariant (line 525) names `recover_all_shards` by name; FM-033/041 `Forced by` rows name tests that live in the moved file |
| `.scratch/testing-improvements-round2/issues/open/03-injectable-clock-seam.md` | — | Line 82 cites `core/src/persistence/store_recovery.rs` as a live `clock::now()` consumer; the move takes that read away. One-line landing step, see [Landing steps](#landing-steps) |
| `frogdb-server/crates/core/src/persistence/crash_recovery_tests.rs`, `test_harness.rs`, `tests.rs`, `core/tests/tiered_storage.rs` | — | Callers of `recover_all_shards` in core's own tests; unchanged |

## Problem

### The driver is protocol, and it sits in the store crate

Verbatim, store_recovery.rs:85–109:

```rust
    for shard_id in 0..rocks.num_shards() {
        // Keep one sink alive across hot + warm recovery so warm-tier
        // precedence (`contains`) sees the hot keys restored this pass.
        let mut sink = StoreRestoreSink::default();
        let stats = recover_shard_into(rocks, shard_id, &mut sink)?;

        total_stats.keys_loaded += stats.keys_loaded;
        total_stats.keys_expired_skipped += stats.keys_expired_skipped;
        total_stats.bytes_loaded += stats.bytes_loaded;
        total_stats.keys_failed += stats.keys_failed;
        // First-wins across the whole database, not just within a shard: shards
        // are walked in ascending id and the hot tier before the warm one, so
        // folding here — after this shard's hot pass, before its warm pass —
        // makes "first" mean the same thing at both levels.
        if let Some(failure) = stats.first_failure {
            total_stats.record_first_failure(|| failure);
        }

        // Recover warm entries if warm tier is enabled
        if rocks.warm_enabled() {
            recover_warm_shard_into(rocks, shard_id, &mut sink, &mut total_stats)?;
        }

        results.push(sink.into_parts());
    }
```

Four rules of the on-disk format are encoded here, and none of them is about
`HashMapStore`:

1. **Shard order is ascending id** — load-bearing, because "first failure" is
   defined relative to it (the comment at 96–98 says exactly that).
2. **Hot before warm, per shard** — not hot-for-all-shards then warm-for-all.
   The interleaving is what makes `record_first_failure` mean "first in recovery
   order" at both levels.
3. **One sink lives across the hot/warm pair** — the comment at 86–87 is the
   only statement of the rule that makes `RestoreSink::contains` (declared at
   recovery.rs:129, documented at recovery.rs:127–128 as "Drives warm-tier
   precedence: a hot copy always wins over a warm copy") function. `contains` is a
   persistence-protocol contract whose *lifetime precondition* is enforced a
   crate away from the trait that declares it.
4. **Which stats fold and which are written through** — the hot pass returns a
   per-shard `RecoveryStats` that is folded field-by-field; the warm pass is
   handed `&mut total_stats` (line 105) and writes into the aggregate directly.

### The fold is a hand-maintained field list, one crate from the struct

`RecoveryStats` has seven counters plus `first_failure` (recovery.rs:22–54). The
fold at store_recovery.rs:91–101 handles four of them plus `first_failure`.
`warm_keys_loaded` / `warm_keys_stale` are correct-by-accident: the hot pass
never sets them, so nothing is lost — but nothing in either crate says so, and
the fold is not where the struct is. `duration_ms` is silently dropped by the
fold and then overwritten at line 111 with the wall-clock elapsed of the whole
loop. Adding an eighth counter to `RecoveryStats` in the persistence crate
produces a silently-wrong total until someone remembers to edit a `for` loop in
`frogdb-core`. That is the definition of poor **locality**: the knowledge and its
verification live apart.

### A locked spec row is implemented outside its gate

FM-PERSISTENCE-047's Invariant (persistence-failure-modes.md:525) closes with:

> The context itself is captured in the format layer (`recover_shard_into` /
> `recover_warm_shard_into`) at the only place the key, the tier, and the
> `SerializationError` are all in scope, stored first-wins so the cost is bounded
> at one key regardless of how many fail, **and folded first-wins again in
> `recover_all_shards` where per-shard stats meet the total**.

`recover_all_shards` is in `frogdb-core`. `frogdb-persistence` carries an 0.85
mutation gate (CLAUDE.md; ADR-0003 records 99.1% at the Phase 2 lock);
`frogdb-core` carries none. Because `cargo mutants -p <crate>` runs only that
package's own tests, a mutant that deletes the `record_first_failure` call at
store_recovery.rs:100 — turning cross-shard first-wins into per-shard
last-wins — is not even generated by the persistence run, and no core gate
generates it either. The rule the spec says exists is the one the gate cannot
see.

The same crate split shows up in `RecoveryStats::record_first_failure`'s own doc
(recovery.rs:85–88): it is documented as `pub` *specifically* because
"`recover_all_shards` in the store crate" needs it. A method made public to
serve one caller in a downstream crate is the seam telling you the caller is on
the wrong side of it.

### The module doc already disagrees with the module

store_recovery.rs:5–9 claims the module "is the thin store-side adapter: it
provides the `RestoreSink` that persistence drives, deciding *how* a recovered
entry lands in a `HashMapStore` (and its expiry index), and re-assembles the
per-shard results the server expects." "Re-assembles the per-shard results" is
doing a great deal of work for a phrase covering shard ordering, tier pairing,
sink lifetime, and failure precedence. Under the **deletion test**: delete
`StoreRestoreSink` and real complexity reappears at every caller (each would
hand-roll expiry mirroring). Delete `recover_all_shards`'s *body* and the
complexity reappears in exactly one place — `frogdb_persistence::recovery`,
where the rest of the protocol already is.

The move is measurable, not cosmetic: store_recovery.rs's **non-test** portion
(lines 1–127; `unit_tests` starts at 128) goes from 127 lines to roughly 87, a
**−31%** cut, with the whole file 309 → ~271. What is left is the module doc's
own claim — a `RestoreSink` impl, a factory closure, and unpacking — and nothing
else.

### Latent: `duration_ms` is a log-only, unasserted, doubly-computed output

`RecoveryStats::duration_ms` (recovery.rs:33) is written twice — once per shard
at recovery.rs:181, once for the whole database at store_recovery.rs:111 — and
read in exactly two places, both `tracing` fields: recovery.rs:189 and
store_recovery.rs:121, plus a third re-log of the aggregate at
`recovery/src/shards.rs:56`. It reaches no `INFO` field, no metric, and no
assertion: no test in the workspace reads it, and no spec row names it as an
Observable or an Outcome variant (unlike `keys_failed`, `keys_expired_skipped`,
and `first_failure`, all of which are). The per-shard value returned to callers
by `recover_shard` is discarded by every caller.

This is not a side note the move can leave alone. Today one of the two writes
sits in `frogdb-core`, which has no gate; the move drags it into an 0.85-gated
crate, where an unassertable write is a mutant nothing can kill. Deleting the
field is therefore part of the move, not a companion to it — see
[Required with the move](#required-with-the-move-delete-recoverystatsduration_ms).

## Proposed change

Add one function to `frogdb_persistence::recovery`, generic over the sink, taking
a **sink factory** so persistence owns the vector it produces:

```rust
/// Recover every shard's hot and warm entries, one sink per shard.
///
/// Calls `make_sink(shard_id)` once per shard, walking shards in ascending id
/// and within each shard the hot tier before the warm one, holding that shard's
/// sink across the pair so `RestoreSink::contains` sees this shard's hot keys
/// when warm-tier precedence is decided. Returns the sinks in shard order —
/// exactly `rocks.num_shards()` of them — and the whole-database statistics,
/// with `first_failure` first-wins in that same recovery order.
pub fn recover_database_into<S: RestoreSink>(
    rocks: &RocksStore,
    mut make_sink: impl FnMut(usize) -> S,
) -> Result<(Vec<S>, RecoveryStats), RecoveryError>
```

The caller supplies only *how to make a sink*, so persistence still never learns
what a `HashMapStore` is — but it now owns **how many** sinks exist and **what
order** they come back in, alongside the order they are driven in and how their
statistics combine. That is what makes FM-PERSISTENCE-041's "exactly one store
per configured shard, in shard order" a property forced *inside*
`frogdb-persistence`: with the slice form it would be the caller who sized the
vector, and a length mismatch would be an error case persistence had to invent
(a new `RecoveryError` variant, plus a row to describe it) rather than a
condition it makes unrepresentable. The factory form is the design; it is not an
open question.

The fold moves next to `RecoveryStats`, as a `RecoveryStats::absorb(&mut self,
other: RecoveryStats)` method on the struct itself, so adding a counter and
forgetting to fold it becomes impossible-by-construction rather than
remembered-by-comment.

`frogdb-core` keeps sink construction and unpacking — the concern its module doc
already claims:

```rust
pub fn recover_all_shards(
    rocks: &Arc<RocksStore>,
) -> Result<(Vec<(HashMapStore, ExpiryIndex)>, RecoveryStats), RecoveryError> {
    let (sinks, stats) = recover_database_into(rocks, |_| StoreRestoreSink::default())?;
    let results = sinks.into_iter().map(StoreRestoreSink::into_parts).collect();
    Ok((results, stats))
}
```

(`&Arc<RocksStore>` deref-coerces to `&RocksStore` at the call, exactly as it
already does for `recover_shard_into` at store_recovery.rs:88.)

The **interface of `recover_all_shards` does not change** — same name, same
arguments, same return tuple, same error type. All **14** call sites compile
untouched (13 in tests, 1 in production at `recovery/src/shards.rs:50`), as do
the **5** top-level import/re-export sites — `core/src/lib.rs:121`,
`core/src/persistence/mod.rs:17`, `core/src/persistence/test_harness.rs:18`,
`core/src/persistence/crash_recovery_tests.rs:18`, `recovery/src/shards.rs:11` —
plus the two function-local `use` lines in `core/tests/tiered_storage.rs:345,403`.

Three details the move must carry:

- `crate::clock::now()` (store_recovery.rs:81) becomes `frogdb_types::clock::now()`,
  which is the identical function — core's `lib.rs:7` is `pub use frogdb_types::*;`
  — and is what `recovery.rs:12` already imports, so the clock seam lint is
  satisfied by the existing import.
- The whole-database `tracing::info!` at store_recovery.rs:113–123 moves with the
  driver. Its `num_shards` / `warm_keys` / `warm_stale` fields are all available
  from `rocks` and the aggregate; nothing in it touches a core type.
- `frogdb-persistence`'s crate re-export list (`lib.rs:25–27`) gains
  `recover_database_into` next to `recover_shard_into` / `recover_warm_shard_into`,
  so `frogdb-core` imports it exactly the way it already imports those two
  (store_recovery.rs:14–16). That makes **three** edited files, not two.

### Required with the move: delete `RecoveryStats::duration_ms`

Not an optional companion — the deletion lands in the same commit. `duration_ms`
is a `pub` field on a struct in an 0.85-gated crate that **no test asserts**: a
mutant replacing `stats.duration_ms = start.elapsed().as_millis() as u64` with
`= 0` (recovery.rs:181, and store_recovery.rs:111 once it moves under the gate)
survives by construction, and no test can be written to kill it that would not be
asserting on wall-clock timing. Moving the second writer into
`frogdb-persistence` without deleting the field would *import a guaranteed
surviving mutant into the gated crate* — it makes the 0.85 number worse for no
behavioral gain. Delete it with the move:

- Drop the field (recovery.rs:33) and both writes (recovery.rs:181, and the
  aggregate write that would otherwise ride along from store_recovery.rs:111).
- Replace the three read sites — all `tracing` field bindings, recovery.rs:189,
  store_recovery.rs:121, `recovery/src/shards.rs:56` — with a local `Instant`
  elapsed computed at each logging site. The log output is unchanged; only the
  carrier is. This is the one line `shards.rs` gives up in exchange for the
  interface staying frozen.

The field is written twice, read by three log bindings, asserted by no test,
surfaced in no `INFO` field or metric, and named by no spec row — it fails the
deletion test outright. Note the lane's characterization of it as a "dead output"
is imprecise: it is not dead, it is **log-only and unasserted**, which is exactly
the shape that becomes a liability the moment it crosses into a gated crate.

### The second whole-database driver stays divergent

`frogdb-persistence` will own *boot's* whole-database ordering rules. It will not
own everyone's. `frogdb-replication-runtime`'s `read_snapshot`
(`install.rs:237–256`) runs a structurally identical loop over the staged
checkpoint — `for shard_id in 0..num_shards`, fresh sink per shard,
`recover_shard_into`, then warm if enabled, push in shard order — and it is
**spec-pinned in its own right**: FM-REPLICATION-053 ("a received checkpoint
installs every shard of the staged DB, warm tier materialized",
replication-failure-modes.md:1129, Invariant at line 1136) states the loop, the
shard-order positionality of the returned vector, and the hot-beats-warm rule,
forced by four named tests. It is a LOCKED replication row at gate 0.85.

It differs in two ways that are not incidental:

1. Its warm pass is `SnapshotSink::absorb_warm(&rocks, shard_id)`, not
   `recover_warm_shard_into`. `absorb_warm` **materializes warm keys as hot
   entries** (the staged DB is discarded after the install, so a warm key cannot
   stay warm) and skips undecodable warm values with a warning rather than
   counting them — behavior boot must not have.
2. It folds **no statistics at all**. There is no `RecoveryStats` to combine; the
   loop's product is `Vec<Vec<SnapshotEntry>>`, an install plan.

**Ruling: it stays divergent by design.** Converging it onto
`recover_database_into` would require a warm-pass hook in the persistence
signature — a second callback, or a `RestoreSink` method that lets the sink
substitute its own warm traversal — bought purely to share a five-line `for`
loop, and it would put a replication-only behavior (warm→hot materialization)
inside the function whose whole justification is that it states *boot's* rules
unambiguously. The shared thing is already shared at the right granularity:
`recover_shard_into`, the per-shard format read. This proposal therefore claims
persistence supplies every ordering rule **for boot recovery** — the path
`recover_all_shards` serves — and explicitly not that it becomes the single
whole-database driver in the workspace. Any later attempt to unify the two
belongs in its own proposal, with both spec rows in scope.

### Dependency direction — the move is acyclic

Verified from the manifests:

- `frogdb-server/crates/core/Cargo.toml` lists `frogdb-persistence.workspace = true`
  under `[dependencies]` (and again under `[dev-dependencies]` with
  `features = ["test-support"]`).
- `frogdb-server/crates/persistence/Cargo.toml` depends only on `frogdb-types`,
  `frogdb-config`, and third-party crates. **It does not depend on `frogdb-core`,
  and must not.**

So the edge is `core → persistence`, one way. The move pushes code *down* that
edge, which is the direction that cannot create a cycle. It stays acyclic because
`recover_database_into` is generic over `RestoreSink` and never names a core
type: `HashMapStore`, `ExpiryIndex`, and `StoreRestoreSink` all stay in
`frogdb-core`, on the caller's side of the trait. Every type in the new
signature — `RocksStore`, `RecoveryStats`, `RecoveryError`, `RestoreSink` — is
already defined in `frogdb-persistence`. No manifest changes, in either
direction.

## Testability improvement

**What is untestable today.** The whole-database protocol can only be exercised
through `recover_all_shards`, which requires a `HashMapStore` and an
`ExpiryIndex` — i.e. from `frogdb-core` or downstream. `frogdb-persistence`'s own
suite has a `MockSink` (recovery.rs:276–297) that mirrors the hot-wins rule and
round-trips the format beautifully, but it can only drive **one shard**
(`round_trips_format_through_mock_sink` at recovery.rs:302 calls
`recover_shard_into` then `recover_warm_shard_into` by hand for shard 0). The
cross-shard rules have no reachable seam in the crate that owns them.

**What becomes testable.** With `recover_database_into` in the persistence crate,
`MockSink` drives the whole database, and each of the four protocol rules gets a
direct assertion with no core store, no `HashMapStore`, no server:

- **Cross-shard first-failure precedence.** Corrupt a value in shard 2 and
  another in shard 0; assert `first_failure.shard_id == 0`. Today this is only
  reachable through `decode_failure_context_is_first_wins` in
  `frogdb-recovery`'s suite — two dependency hops from the fold it forces
  (`frogdb-recovery` → `frogdb-core`, where the fold is) and from the crate that
  owns the rule (`frogdb-core` → `frogdb-persistence`).
- **Hot-beats-warm across shards.** Corrupt a hot value in shard 1 and a warm
  value in shard 0; assert the *warm* shard-0 failure wins, because shard 0 is
  walked first — the exact interleaving the comment at store_recovery.rs:96–98
  asserts in prose and nothing asserts in code.
- **Sink lifetime across the hot/warm pair.** A `dup` key hot in shard 0 and warm
  in shard 0 must resolve hot-wins; the same key hot in shard 0 and warm in
  shard 1 must resolve as two independent entries. That pair pins "one sink per
  shard, not one per database" — currently unpinned in any crate.
- **Fold completeness.** Assert `warm_keys_loaded` / `warm_keys_stale` /
  `bytes_loaded` on a multi-shard, tiered database. If `absorb` lands, a single
  unit test over two `RecoveryStats` values covers every counter without touching
  RocksDB at all.
- **Shard-count fidelity (FM-PERSISTENCE-041).** The returned `Vec<S>` is
  `num_shards` long and in shard order — forced inside `frogdb-persistence`,
  because with the factory form `frogdb-persistence` is the crate that *builds*
  the vector. `MockSink`s made by a factory that records its `shard_id` argument
  pin both the count and the order in one assertion, with no core store.

**Mutation reachability — the real win.** Every mutant of the loop above is
currently generated by no gated run. After the move, `cargo mutants -p
frogdb-persistence` generates and (with the tests above) kills mutants of the
shard loop, the tier interleaving, and the first-wins fold — which is what makes
FM-PERSISTENCE-047's Invariant a claim the 0.85 gate actually checks.

## Risks / scope boundaries vs sibling proposals

**Spec impact — no failure-mode row changes.** Verified row-by-row against
`.scratch/hardening/specs/persistence-failure-modes.md`. No Trigger, Observable,
NOT-observable, or Outcome-variant field changes for FM-PERSISTENCE-029, -033,
-036, -041, -045, or -047: the behavior is identical and `recover_all_shards`
keeps its interface. Two prose/reference edits are required, and are the only
spec edits:

1. **FM-PERSISTENCE-047, Invariant (line 525)** names `recover_all_shards` as the
   fold site. The sentence must be re-pointed at `recover_database_into` (and the
   parenthetical "in the store crate" on `record_first_failure`'s doc,
   recovery.rs:87, along with it). This is a pointer correction, not a contract
   change — and it *improves* the spec, because the row currently describes a
   persistence failure mode by naming a function in another crate.
2. **`Forced by` paths, if tests move crates.** Six tagged tests live in the file
   being edited — store_recovery.rs `unit_tests`: `test_recover_empty_shard`
   (FM-029), `test_recover_with_data` / `test_recover_sorted_set` /
   `test_recover_all_shards` (FM-033; `test_recover_all_shards` also FM-041),
   `test_recover_with_expiry` / `test_recover_skips_expired` (FM-036). The
   `Forced by` rows name tests **by bare name**, and `scripts/failure-modes.py`
   resolves them against a `cargo nextest list` spanning `frogdb-txn`,
   `frogdb-vll`, `frogdb-server`, `frogdb-persistence`, `frogdb-recovery`, and
   `frogdb-core` — so a test that changes crate *within that set* needs no spec
   edit at all, provided its name and its `// FM-<AREA>-NNN` tag travel with it.

**Where the forcing tests must live post-move.** Repo rule: `cargo mutants -p
<crate>` runs only that package's own tests, so a row forced solely from another
crate contributes nothing to the owning crate's score. The recommendation:

- **Keep** all six existing core tests where they are. They assert on
  `HashMapStore` and `ExpiryIndex` — genuinely core-side observations (the
  expiry-index mirroring of FM-036 in particular is `StoreRestoreSink`'s job, not
  persistence's). Moving them would weaken FM-036's forcing, not strengthen it.
- **Add** the new multi-shard / cross-tier tests listed above **in
  `frogdb-persistence`**, tagged to the rows they force (`// FM-PERSISTENCE-041`
  for the shard-count and ordering assertions, `// FM-PERSISTENCE-047` for
  cross-shard first-wins), and extend those rows' `Forced by` lists. That is an
  additive spec edit — new names appended, none removed — which the lint accepts
  in both directions and which is the whole point of the move.

**Mutation re-gate required.** `frogdb-persistence` is a LOCKED crate at gate
0.85. This change adds real branching logic to it, so: `just mutants-diff
frogdb-persistence` as push discipline, and `just mutants frogdb-persistence` +
`just mutants-gate frogdb-persistence 0.85` for the full run. Expect the score to
*move*, not merely hold — new mutable code lands in the gated crate, and the
tests above exist to kill it. Deleting `duration_ms` removes two guaranteed
survivors from the gated crate's denominator, which is the other half of why the
deletion is required rather than optional. `frogdb-core` has no gate.

**Behavior-preservation risk.** The loop is order-sensitive in three coupled
ways (shard ascending, hot-before-warm, one sink per pair), and the warm pass
writes into the aggregate while the hot pass folds through a temporary. A move
that "tidies" this into hot-all-then-warm-all, or into one sink for the database,
silently changes which failure is reported as first and which tier wins a
duplicate key — both spec-visible. This must be pure code motion; the fold's
field-by-field arithmetic and the `record_first_failure` placement (after the hot
pass, **before** the warm pass) must be preserved exactly, and the new
cross-shard tests should land *before* the move so they pin the current behavior
rather than the moved behavior.

**`frogdb-recovery` re-gate is trivial but not zero.** `frogdb-recovery` also
carries an 0.85 gate, and the `duration_ms` deletion changes one line in it
(`shards.rs:56`, a `tracing` field binding). That line is unasserted before and
after, so the score should not move; run `just mutants-diff frogdb-recovery`
anyway, since the diff touches the crate.

**Sibling boundaries — no file overlap.**

- **Proposal 43 (recovery wrappers / `StagedCheckpoint`)** operates on
  `frogdb-recovery`: its phase-wrapper modules, `replication.rs`, `functions.rs`,
  `staged.rs`. This proposal touches `frogdb-recovery` **not at all** —
  `shards.rs` is only a caller, and preserving `recover_all_shards`'s interface is
  what keeps it out of the diff. If 43 lands first, `shards.rs:44–67` may be
  reshaped; this proposal is indifferent to that shape because it does not edit
  the file.
- **Proposal 41 (small dedups)** must not claim `store_recovery.rs` or
  `recovery.rs`. The `duration_ms` deletion belongs to **this** proposal outright
  — it is a required part of the move, not a loose cleanup either could take (41's
  own `duration_ms` mentions are the WAL `trace!` fields at its lines 176/337, an
  unrelated value). 41 stays clear of both files.
- **Sequencing with 41.** No file overlap, but both land in `frogdb-persistence`
  and both therefore re-gate it. Run the two mutation gates **serially**, not
  concurrently: `just mutants` on a crate two branches are editing gives a score
  neither branch can attribute, and `mutants-diff` on the second one to land needs
  the first already merged to compute a meaningful diff. Either order works;
  land-then-gate-then-land is the constraint, not the direction.
- **Lane item 1 (`SnapshotLayout`)** and **item 6 (`RocksSink::commit` /
  `CommitEffects`)** are in `frogdb-persistence` but in `snapshot/` and `flush.rs`
  — no overlap with `recovery.rs`. Item 6 has the larger blast radius and is
  spec-tagged on watermark rows; sequencing this proposal *before* it keeps the
  persistence re-gates independent.

## Effort

**M.** The code motion itself is S — one function moved down a dependency edge it
already crosses, a generic parameter and a factory argument added, and a caller
that shrinks to three lines with an unchanged interface. What makes it M is the
locked-crate tax: writing the multi-shard/cross-tier tests that justify the move,
appending them to two `Forced by` rows, re-pointing FM-PERSISTENCE-047's
Invariant, and running the persistence mutation gate. Three files carry the move
(`store_recovery.rs`, `persistence/src/recovery.rs`, `persistence/src/lib.rs`)
plus one line in `recovery/src/shards.rs` for the `duration_ms` deletion, one
spec file touched in two places, no manifest changes, no caller churn.

### Landing steps

Beyond the code motion and the tests, four things land in the same commit:

1. Delete `RecoveryStats::duration_ms` and re-source the three log fields from
   local `Instant`s (see above).
2. Re-point FM-PERSISTENCE-047's Invariant (persistence-failure-modes.md:525) and
   `record_first_failure`'s doc (recovery.rs:85–88) at `recover_database_into`.
3. Append the new persistence tests to FM-PERSISTENCE-041's and -047's
   `Forced by` rows.
4. Fix the stale citation at
   `.scratch/testing-improvements-round2/issues/open/03-injectable-clock-seam.md:82`,
   which lists `core/src/persistence/store_recovery.rs` among the files that read
   `clock::now()`/`system_now()`. After the move the only clock read in that file
   is gone — the consumer becomes `persistence/src/recovery.rs`, which the same
   sentence already needs to name. One-line edit; leaving it stale would leave a
   clock-seam audit trail pointing at a file that no longer reads the clock.

### Independently-landable hotfix

**Correct the stale module doc (S, zero risk, no gate).**
`store_recovery.rs:5–9` describes the module as "the thin store-side adapter"
that "re-assembles the per-shard results" — which is not what lines 78–126 do.
A two-sentence doc fix naming the four protocol rules the module currently owns
(shard order, hot-before-warm, sink lifetime, first-failure precedence) makes the
misplacement legible to the next reader, is worth landing whether or not the move
happens, and touches nothing gated. It is also the cheapest way to stop the
comment at 96–98 from being the only record of a rule the spec depends on.

The `RecoveryStats::duration_ms` deletion is **not** on this list. It was
previously written up here as an optional companion; it is now a required part of
the move — see [Required with the move](#required-with-the-move-delete-recoverystatsduration_ms).
It could still be landed on its own ahead of the move (it needs the persistence
re-gate either way), but it cannot be dropped from the move, because the move is
what carries a second unassertable write into the gated crate.
