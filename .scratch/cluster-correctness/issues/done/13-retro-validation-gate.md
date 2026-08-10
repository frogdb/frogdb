# 13 — Retro-validation gate: the machine catches what the audit caught

Status: done

## Parent

[PRD](../../PRD.md) §5 sequencing gate + §6 exit criterion 8.

## What to build

In a scratch branch, revert each of the five 2026-08-08 audit fixes one at a time
(FM-CLUSTER-098 vote durability, 099 log-reader cache, 100 handoff_seq restore, 101
voter removal, 102 detector clamp) and run the layers. Record, per defect, which layers
flag it (L1 hooks / P1–P4 / stateright / seeded schedules / Jepsen checker). Every one
of the five must be caught by at least one layer — this is the PRD's falsifiable exit
claim. A defect no layer catches is a gap issue filed against the responsible
workstream, and the PRD does not exit until it closes.

Deliverable: a results table appended to the PRD (§6) with the evidence.

## Acceptance criteria

- All five reverts attempted, each against the full layer stack
- Catching layer(s) named per defect
- Every miss filed as a gap issue against the responsible workstream, to close before PRD exit
- Results table in the PRD

## Blocked by

- Issues 02, 03, 09, 10 (`.scratch/cluster-correctness/issues/`) — needs the layers it
  measures (04, 06, 07 strengthen the count but the gate can run once these four exist).

## Resolution

Ran 2026-08-09, local mode. Full evidence and the per-defect table are in
[PRD §6.1](../../PRD.md#61-retro-validation-results-issue-13-run-2026-08-09); this is the
short form.

**3 of 5 caught by a non-forcing layer. The gate does not pass** — exit criterion 8 stays
open behind two new gap issues.

| defect | caught by | notes |
|---|---|---|
| 098 vote durability | `just lint-durable-ack` (campaign-2 seam gate) | none of this PRD's five layers; the gate had `save_vote` allowlisted at audit time |
| 099 log-reader cache | **nothing** | → issue 21 |
| 100 handoff generation | L1 (INV-HANDOFF-1 via the `from_snapshot` hook), L2 (P2), L3 (`handoff_model_smoke`, `stale_source_admits_writes_after_ownership_moves`) | confirms and extends the experiment banked in issue 04 |
| 101 voter removal | L4 — `just cluster-seeds 100`, new seed 35 (`XNODE-SLOT-1`, two nodes claiming 5462 slots) | the regression-seed and determinism failures in the same run are *not* catches — see §6.1 |
| 102 detector clamp | **nothing** | → issue 22 |

Method notes worth carrying forward:

- Reverts were inverse patches in the working tree, never commits; the final branch carries
  only `.scratch` changes. One trap: a failing proptest run appends its shrunk case to
  `proptest-regressions/properties.txt`, which must be reverted with the source — otherwise
  the experiment leaks a "regression" seed derived from code that no longer exists.
- Spec forcing tests were excluded from every verdict. For 098, 099, 101 and 102 the forcing
  tests were the *only* failures at L1, which is the whole point of the exercise: a point
  witness proves the fix, it does not prove the machine would have found the bug.
- Jepsen (L5) is N/A throughout — issue 07 (the invariant checker) is still open.
- Two structural findings fell out of the run and are recorded at the end of §6.1: the five
  layers reach exactly as far as the cluster state machine (both misses are outside it — one
  below, one beside), and L1's hook is what multiplied a single 100 violation into three
  independent witnesses.

New issues: [21](../open/21-no-layer-sees-the-raft-log-store.md) (openraft storage-conformance layer),
[22](../open/22-no-layer-generates-runtime-config-values.md) (generated config admission). This gate
should be re-run once both land.
