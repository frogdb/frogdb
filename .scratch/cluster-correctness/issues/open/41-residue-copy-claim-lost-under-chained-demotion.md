# 41: Residue copy-claim lost under chained demotion (one-hop shard closure)

Status: ready-for-human

**R1 BUILT 2026-08-20 (`d1e15ab2`, `cc4a917b`) — but the acceptance criterion is not met:
the steered walk is still red on three *further* roots, all pre-existing and all outside
R1's two ruled items. They need a ruling. See "After R1" below.**

**RULED 2026-08-20 (R1, campaign ledger): repoint + tighten guard — both sub-roots.**
Demotion/adoption repoints a demoted primary's dependants at the new primary
(Redis/Valkey semantics; chained and cyclic primary-less topologies become unreachable,
one-hop `shardPrimary` stays as-is), AND `canRetargetSlotResidue` only re-homes onto a
node that physically holds the slot. See the acceptance list below for the landing order.

Found 2026-08-19 by the quint-completeness campaign's walk-steering task (T5): steering the
migration-model walks toward deep protocol states surfaced a reproducible violation of
`inv_source_keeps_its_copy_until_promotion_attested` that the flat walk never sampled
(pre-steering: 0/10 seeds at 2000x40; steered: 8/8 at 500x40). The transition relation did
not change — the state was always reachable.

Full trace, reproduction command, and analysis:
[.scratch/formal-spec/t5-blocked.md](../../../formal-spec/t5-blocked.md).

## Shape of the violation

`completeMigration` leaves an unpromoted residue entry whose source holds the only copy.
`adoptReplicatedRole` then demotes that source under the new owner (chain `1 -> 4`),
`retargetSlotResidue` re-homes the entry onto the new owner — which holds the copy only
**derivatively**, through the one-hop shard closure — and a later staged-flip demotion of
that owner (chain `1 -> 4 -> 3`) cuts the closure edge. Nothing is deleted; the derived
reachability relation (`shardHoldsCopy` / `shardPrimary`, one hop, live-Primary parent
only) loses the copy.

## The ruling needed (do not pick unilaterally)

1. **Chained replicas are reachable but the shard abstraction is flat.** Either demotion
   must repoint dependants (Redis/Valkey re-parent a demoted primary's replicas) or
   `shardPrimary` must walk transitively with a cycle bound.
   `inv_slot_copy_survives_until_owned_and_served` and `keyIsTracked` lean on the same
   one-hop closure and move with this option.
2. **`canRetargetSlotResidue` is too weak.** It requires only
   `shardPrimary(r.source) != r.source`, never that the new source physically holds the
   slot. Tighten to demand direct holding (or pin the entry to the physical holder).

Both readings are consistent with the design doc as written
(`.scratch/cluster-correctness/2026-08-14-issue31-migration-design.md`).

## Scope: this is a four-invariant family, not one property

Progressive quarantine was attempted and abandoned — each removal exposed the next
symptom of the same root. Under the steered walk at 500x40, all of these violate:

| invariant | symptom |
|---|---|
| `inv_source_keeps_its_copy_until_promotion_attested` | entry's copy-claim rests on a closure edge a later demotion cuts |
| `inv_residue_has_an_effective_remover` | **zero live primaries + parent cycle (1<->4, all nodes Replicas)**: every remover guard false simultaneously |
| `inv_slot_copy_survives_until_owned_and_served` | after retarget re-homes the claim off the physical holder, `completeAdoption`'s ruled discard leg (M37, commit `2eb66e35`) lawfully drops the *last physical copy* — the tracking hole becomes modeled data loss |
| `inv_no_serve_before_attestation` | same family, serve-side |

Two sub-roots, both needing the ruling:
- `canRetargetSlotResidue` re-homes onto a derivative (non-physical) holder;
- the demotion/adoption family (`stageFlip`/`adoptReplicatedRole`/`setRole`) permits
  chained and cyclic primary-less topologies the one-hop `shardPrimary` cannot walk.

Note the M37 interaction: the discard-leg ruling is itself sound — it deletes only
copies the replicated map gives no lawful claim to — but this hole makes a *lawful*
claim evaporate first, so the discard turns tracking-loss into copy-loss. Any fix must
be re-checked against `witnessAdoptionDiscardsUnclaimedCopy` traces.

## Interim state

**The steering is parked, not the invariants**: commit `9c5d6f17` reverts the steering
commit `477b813c` (sampling distribution only; transition relation untouched), restoring
the flat walk under which all four invariants are checked and clean (0/10 seeds at
2000x40). Quarantining four core safety invariants was rejected — it would gut the
model's checking. To reproduce the violations: revert `9c5d6f17` and run the deep-walk
command in `t5-blocked.md`.

## After R1 (2026-08-20)

Both ruled items are built, plus two companion rules their counterexamples forced (the
staged-flip adoption now obeys TR-CLUSTER-004's live-primary-upstream rule, and
`residueFollowsDemotion` moves the claim with the copy when the demoted node no longer
holds it). Details in `d1e15ab2`; battery rows R1-1a…R1-4b in the
[Q4 report addendum](../../quint-rework-reports/issue31-quint-q4-report.md).

**Measured effect on the steered walk** (revert `9c5d6f17` applied to the working tree,
500x40, per-invariant, one run each):

| seed | source_keeps | effective_remover | copy_survives | no_serve_before_attestation |
|---|---|---|---|---|
| 2 | violated | violated | ok | ok |
| 777 | violated | ok | ok | violated |
| 12345 | violated | ok | ok | violated |
| 20260819 | violated | violated | ok | violated |

Baseline before R1 was 13/16 cells red; it is now 9/16, and
`inv_slot_copy_survives_until_owned_and_served` — the one that turns tracking-loss into
modelled data loss through M37 — is clean on all four seeds. The steering was therefore
**not** re-landed: landing it makes `just quint-run` (random seed, whole directory)
stochastically red, which is a PR-lane gate failure, not a finding.

### The three residual roots (each needs a ruling; none is R1)

1. **`retargetResidueOnDemotion`'s target arm serves before attestation.**
   `applyFailoverCommon` hands the successor the moved slots *and their keys*, and the same
   step re-points an **unpromoted** entry's `target` at that successor. The new target then
   holds the slot's keys with `prev_keys` empty and `promoted == false`, which is exactly
   `inv_no_serve_before_attestation`. Trace: `demoteNode(4, 2)` at the final step of the
   seed-777 walk. Nothing R1 ruled on touches the target arm.
2. **`retargetResidueOnDemotion`'s source arm re-homes onto a derivative holder — the same
   defect R1 fixed for `canRetargetSlotResidue`, in the failover path.** `demoteNode(1, 3)`
   moves the entry's `source` to the successor, which holds the slot only through the
   demoted node's closure edge (the slot was not in the successor's moved set). A later
   `setRole` that re-parents the *physical holder* into another shard cuts that edge and
   `inv_source_keeps_its_copy_until_promotion_attested` goes red with nothing deleted. The
   obvious fix is R1's physical-holding principle applied to the failover re-home (the same
   post-state condition `residueFollowsDemotion` already uses) — but R1 named
   `canRetargetSlotResidue` specifically, so this was not built unilaterally.
3. **A demoted-but-still-holding source has no remover.** R1's rule correctly *keeps* the
   claim on the physical holder when a `setRole` demotes it. But `canFailPromotion` requires
   the source to be a **live primary**, and `canRetargetSlotResidue` now (correctly) refuses
   a shard primary that holds nothing, so an unpromoted entry whose source was demoted while
   still holding the copy has every remover disjunct false:
   `inv_residue_has_an_effective_remover` goes red. Trace: `completeMigration` →
   `failPromotion` → `setRole(1, Replica, Some(3))` on the seed-2 walk. Candidate rulings:
   let `canFailPromotion` read the *holder* rather than the role, block the demotion of an
   unpromoted entry's source, or fail the promotion as part of the demotion.

Roots 2 and 3 are the same principle R1 ruled on (a copy claim rests on a physical holder)
reaching two call sites the ruling did not name; root 1 is a distinct semantic question
about when the failover successor may be named the residue's target.

## Acceptance

- [x] Ruling between the two sub-root fixes recorded here (2026-08-20: both — repoint
      dependants on demotion + physical-holder retarget guard)
- [x] Model fixed per the ruling (`d1e15ab2`)
- [ ] Steering re-landed (revert `9c5d6f17`); steered walk clean at 500x40 across seeds
      with all four invariants restored — **blocked on the three residual roots above**;
      steering deliberately left parked so `just quint-run` stays deterministic-green
- [x] Battery row(s) added for the chosen fix's guard/effect; M37 rows re-checked
      against the fixed claim semantics (`cc4a917b` + Q4 report addendum)
