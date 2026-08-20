// lib_selftest — the executable self-test for the shared helper modules
// specs/quint/lib_option.qnt and specs/quint/lib_monotone.qnt (formal-spec issue 01;
// .scratch/formal-spec/issues/open/01-quint-shared-library.md).
//
// Why this file exists at all: the helper modules are `pure def`s with no state, so
// `just quint-check` typechecks them but nothing *executes* them — and a `run` test
// parked in a module that no runner ever opens is a test that cannot fail. The runners
// key off scripts/quint-models.sh, which lists a file iff it declares at least one
// `val inv_*` (see that script's header). So the helper laws are stated here, as the
// invariants of the smallest state machine that has the shape both consumers have — a
// monotone position watermark, an `Option`-shaped stored `(generation, position)` pair
// admitted only on strict lexicographic domination, and a ghost latch recording that a
// non-dominating observation was refused. `just quint-run` then executes the `run` tests
// below *and* samples the laws, exactly as it does for the two cluster design models.
//
// This is a **test model, not a design model.** It claims nothing about FrogDB: no row
// below is modeled here, and nothing in this file is evidence about the system. The rows
// are cited because they are what the helpers under test serve in the real models, and
// because the shapes exercised here are lifted from them rather than invented:
//   TR-CLUSTER-010 — the slot-migration lifecycle: `Option[Migration]`-shaped per-slot
//     reads (lib_option) and the copy/ack watermarks (`max2`).
//   TR-CLUSTER-018 — failover application: the `defects`/`coverage` ghost battery whose
//     every field is a `latch`, and `identityOrderOk`'s strict `(incarnation,
//     identity_seq)` domination, which `lexGt` and the `admit` guard below reproduce.
//   TR-REPLICATION-015 / TR-REPLICATION-019 — a full resync installing an absolute offset
//     pair (which moves the landed offset *backward*) and a granted `+CONTINUE` shifting
//     the inherited id at a validated id. `staleGenerationIsRefusedTest` and
//     `watermarkSurvivesGenerationShiftTest` below are that pair of hazards in miniature,
//     and the phase-3 full-sync/PSYNC handoff model (campaign W3,
//     .scratch/formal-spec/2026-08-19-quint-completeness-campaign.md) is the consumer.
//   TR-REPLICATION-033 — "a replica ACKs the landed offset, never the claimed or received
//     one": the never-retreating watermark `max2` is the primitive for.
//
// A mutation check, for whoever changes a helper — each of these was run against this
// file, and each is caught (by `quint test`, and the first three also by the sampled
// `quint run --invariants` pass, which is the tier `just quint-run` gates on):
//   - `max2` returning its second argument instead of the larger — violates
//     `inv_watermark_dominates_admitted`.
//   - `lexGt` weakened to non-strict domination on the position — violates
//     `inv_stored_dominates_admitted` (the equal-pair replay is admitted).
//   - `latch` dropping its accumulator (`seen` instead of `prev or seen`) — violates
//     `inv_refusal_evidence_latches` on the first `admit` after a `refuse`.
//   - `optExists` and `optForall` swapping their absent-option arms — inverts which
//     action is enabled at `stored == None` and fails every `run` test below.
//
// No sampled witnesses: with exactly two actions, both reached by named `run` tests
// below, `--witnesses` would restate what the tests already pin.
module lib_selftest {
  import cluster_common_types.* from "./cluster_common_types"
  import lib_option.* from "./lib_option"
  import lib_monotone.* from "./lib_monotone"

  // An observation is a `(generation, position)` pair — the shape of the migration
  // model's `(incarnation, identity_seq)` and of a replication `(replid generation,
  // offset)`. Kept tiny: the laws are quantifier-free in the bound.
  pure val GENERATIONS: Set[int] = Set(1, 2)
  pure val POSITIONS: Set[int] = Set(1, 2)

  // The highest position ever admitted — advanced with `max2`, never assigned directly.
  var acked: int
  // The dominating observation so far, or `None` before the first one.
  var stored: Option[(int, int)]
  // Every observation that was admitted, so the laws can quantify over the whole trace.
  var admitted: Set[(int, int)]
  // How many observations were refused, and the ghost latch that records that any were.
  var refusals: int
  var sawRefusal: bool

  action keepAll: bool = all {
    acked' = acked,
    stored' = stored,
    admitted' = admitted,
    refusals' = refusals,
    sawRefusal' = sawRefusal,
  }

  action init: bool = all {
    acked' = 0,
    stored' = None,
    admitted' = Set(),
    refusals' = 0,
    sawRefusal' = false,
  }

  // `identityOrderOk`'s guard, verbatim in the shared vocabulary: nothing stored yet
  // admits anything (`optForall`'s vacuous arm); otherwise the pair must strictly
  // dominate what is stored.
  action admit(obs: (int, int)): bool = all {
    optForall(stored, s => lexGt(obs, s)),
    stored' = Some(obs),
    acked' = max2(acked, obs._2),
    admitted' = admitted.union(Set(obs)),
    refusals' = refusals,
    sawRefusal' = latch(sawRefusal, false),
  }

  // The complementary arm — there *is* a stored pair and the observation fails to
  // dominate it (`optExists`'s conservative arm). A refusal is a no-op on the data, and
  // is recorded twice: a plain counter, and the monotone ghost the law below relates it
  // to.
  action refuse(obs: (int, int)): bool = all {
    optExists(stored, s => not(lexGt(obs, s))),
    stored' = stored,
    acked' = acked,
    admitted' = admitted,
    refusals' = refusals + 1,
    sawRefusal' = latch(sawRefusal, true),
  }

  action step: bool = {
    nondet g = GENERATIONS.oneOf()
    nondet p = POSITIONS.oneOf()
    any {
      admit((g, p)),
      refuse((g, p)),
    }
  }

  // `max2`'s law: the watermark dominates every position ever admitted. A `max2` that
  // returns its second argument (or the smaller value) loses a high-water mark as soon as
  // a later observation carries a lower position — which is TR-REPLICATION-015's
  // backward-moving offset, and the reason the real models never assign a watermark
  // directly.
  val inv_watermark_dominates_admitted: bool =
    admitted.forall(o => acked >= o._2)

  // `lexGt`'s law, read through `optForall`: whatever is stored lexicographically
  // dominates everything admitted, because that is the only way anything got stored. A
  // non-strict `lexGt` breaks this on the equal-generation arm.
  val inv_stored_dominates_admitted: bool =
    optForall(stored, s => admitted.forall(o => not(lexGt(o, s))))

  // `latch`'s law: the ghost is evidence about the whole trace, not about the last step.
  // Dropping the accumulator satisfies this only until an `admit` follows a `refuse`.
  val inv_refusal_evidence_latches: bool =
    refusals > 0 implies sawRefusal

  // The `None` arm of both helpers: with nothing stored, `admit` is enabled (`optForall`
  // vacuous) and `refuse` is not (`optExists` conservative). Swapping the two helpers
  // inverts this and leaves the model with no enabled action at all in the initial state.
  run firstObservationIsAdmittedTest: bool = {
    init
      .then(admit((1, 1)))
      .expect(all { stored == Some((1, 1)), acked == 1, refusals == 0, not(sawRefusal) })
  }

  // The other half of that fork, as its own trace because `.fail()` is only legal as a
  // trace's terminal step (see the quint-lang skill's tests guideline): with nothing
  // stored there is nothing an observation could fail to dominate, so `refuse` is
  // disabled rather than firing on a vacuous comparison.
  run refuseNeedsAStoredPairTest: bool = {
    init
      .then(refuse((1, 1)).fail())
  }

  // Strictness: replaying the stored pair is refused, and the refusal latches and stays
  // latched across the later admitted observation. The `.fail()` terminal step is the
  // stale-generation hazard — an observation with a *higher* position but an older
  // generation is refused, which is TR-REPLICATION-019's validated-id gate and
  // `identityOrderOk`'s reordered-report refusal in one step.
  run staleGenerationIsRefusedTest: bool = {
    init
      .then(admit((2, 1)))
      .then(refuse((2, 1)))
      .expect(all { refusals == 1, sawRefusal, stored == Some((2, 1)) })
      .then(admit((2, 2)))
      .expect(all { stored == Some((2, 2)), sawRefusal, refusals == 1 })
      .then(all { assert(inv_refusal_evidence_latches), keepAll })
      .then(admit((1, 2)).fail())
  }

  // `max2` under a generation shift: the new generation restarts positions low, and the
  // watermark does not follow it down.
  run watermarkSurvivesGenerationShiftTest: bool = {
    init
      .then(admit((1, 2)))
      .expect(acked == 2)
      .then(admit((2, 1)))
      .expect(all { acked == 2, stored == Some((2, 1)) })
      .then(all { assert(inv_watermark_dominates_admitted), keepAll })
  }
}
