# Quint completeness campaign — scope rulings and wave plan

Date: 2026-08-19. Ruled interactively (grill session); every item below is a user decision,
not a proposal. Parent program: [formal-state-spec design](2026-08-12-formal-state-spec-design.md).

## Trigger findings

- **Main is red**: `quint_conformance.rs` (frogdb-cluster) was never updated for the issue-31
  Q1–Q4 model rework (`ce75ec0d`). It references deleted model run tests
  (`abortRepatriatesTest`, `feedHoldOverflowDisconnectsTest`, …) and projects the deleted
  6-var state shape (model now has 14 vars). `just quint-conformance` fails 6/6 live tests;
  `just test frogdb-cluster` fails with it. The code↔model link is severed — the models
  currently verify only themselves.
- The admission model's checking discipline lags the migration model's (4 run tests, no
  documented battery, filed guard defects in formal-spec issue 03).
- Q2–Q4 left 12 design-owner flags; four are model-shaped and were ruled below.

## Scope ruling

All three readings of "completeness" are in scope: (1) harden the existing cluster models,
(2) admit specific new scenarios into them, (3) start phase-3 replication models.
"Raise nightly verify depths" was considered and ruled out (marginal).

## Design-owner flag rulings (4 of the 12; the rest stay with the impl-campaign decomposition)

| Flag | Ruling |
|---|---|
| **M37 — staged-flip dataset-discard leg unmodelled** | **Add the discard leg.** `completeAdoption` drops from the adopting node's `keys` every slot the replicated map gives it no lawful copy claim to (owner/source/target/residue-tracked exempt), plus the `assignSlots`-interleaved schedule the design doc names. Re-arms battery rows M37/M38/M39 exactly as the doc wrote them; the wiped-source leg of `inv_no_acked_write_lost` becomes checkable. |
| **`inv_no_hold_during_staged_flip` false-of-model (V12-M1)** | **Effect-based closure**: `stageFlip` empties the node's hold region in the same step (held writes answered — refusal/redirect, as befits an about-to-demote primary), and hold-latching is disabled at a node with a pending record. The invariant is then asserted. Rationale: demotion must not be blockable by a wedged drain (the issue-17 class); matches the doc's "answered, never held" literally. The guard-based alternative (refuse staging while holds outstanding) was rejected for wiring demotion liveness to drain-release liveness. |
| **M32 — cross-shard demotion successor has no observable** | **Defect ghost restating the guard**: `demoteNode`'s apply recomputes "was the chosen successor a replica of the demoted primary in the pre-state" and sets `defects.crossShardSuccessor`; invariant `not(defects.crossShardSuccessor)`. Postcondition-derived per the defects-family convention → M32 becomes CAUGHT-P. Recorded: the *semantic* observable (successor holds the shard's data) is phase-3 replication-model territory. |
| **Dead arm 4b of `isRefusalTerminal` (`Ordering ∧ stored == None`)** | **Delete from the design doc** — dead by construction: an Ordering refusal presupposes a stored cell; Fenced/Membership own the empty-cell refusal cases. Comment at `refusalIsTerminal` cites this ruling. If the spec amendment surfaces an impl path emitting Ordering against an empty cell, that is a new ruling with a forcing test. |

Accepted structural limits, documented at the model, no further action: **M22**
(step-unwiring needs a temporal operator — mitigated by the temporal-nightly item below),
**M06** (compound-only visibility with M08). Detector-attribution flags (F6/F7/F13,
M38/M39, M34 form) are mechanical design-doc text corrections — bundled into W1.

## Model lifecycle standardized

The Q1→Q4 pipeline (build → modularize → properties → documented mutation battery → gap
closure with honest-miss analysis) is now the **mandatory lifecycle for every model**,
recorded in the design doc §3. No small-model exemption. Exhaustiveness of a model's
checking = its battery verdict table, not its invariant count.

## Wave plan

### W0 — urgent, immediately: get main green

Two independent breaks, both confirmed on clean main 2026-08-19:

1. **Conformance harness**: point it at surviving model run tests; quarantine (`#[ignore]`
   with issue citations) every trace that diverges. The full 14-var projection rebuild is
   **deliberately deferred** to the issue-31 implementation campaign, where each impl wave
   un-quarantines its traces — do not build the projection twice while ~40 LOCKED rows are
   pending amendment.
2. **`just lint-spec` red**: the QR modularization left `cluster_common_types.qnt` and
   `cluster_migration_failover_machine.qnt` headers without spec-id citations (tripwire-3
   rule: no citation-free helper models). Add the citations for the rows each file supports.

### W1 — cluster model hardening (after W0)

- Apply the four flag rulings above to model + design doc; re-run the affected battery rows
  (M32 → CAUGHT-P, M37/M38/M39 per doc detectors) and update the Q4 report table.
- Design-doc text corrections for the detector-attribution flags.
- **Admission model**: formal-spec issue 03's admission items (`canMeet` physicality
  ruling, `applyRepurpose` intent rewrite, `removeNode` churn gating) + a Q3-style
  documented mutation battery over its invariants/guards, then Q4-style closure.
  No conformance harness for admission (premature: `cluster_init.rs` still pre-ruling).
- **Issue 33**: fix the model's `removeNode` ghost role/parent tombstones (verdict already
  filed: fix the model, not the code).
- **Walk steering**: coin-gate `removeNode` in `step` (issue 03 remedy), extend the
  existing candidate biasing toward commit-enabling states. Acceptance:
  `witnessResiduePending` + the commit-family witnesses reached in >0 sampled traces.
- **Temporal properties in the nightly verify lane**: state `inv_no_stuck_handoff` and
  held-set-eventually-empties as `temporal` properties checked only by Apalache nightly.
  Timeboxed: if Apalache doesn't converge at useful depth, document and keep the
  by-construction argument. Closes the M22 detection hole if it lands.
- **Bidirectional lint** (formal-spec issue 02): `Model:` cell on TR/FM rows + reverse
  check; an `inv_*` no spec row claims becomes a lint error.
- **New scenarios admitted** (defect-class justified):
  - *Node-id instability* (issue 35 class): extend the admission model — fresh id minted
    per boot unless configured; intent cell keyed by what actually persists. Removes the
    model's disclosed stable-`NodeId` assumption that contradicts the spec row.
  - *Anti-churn no-bump* (TR-025 / FM-CLUSTER-014): postcondition ghost + invariant that
    `markNodeFailed`/`markNodeRecovered` never bump the config epoch (~10 lines).
- **Rejected for now** (no defect class behind them): RESET HARD epoch-zeroing
  (reset-era-scoped epoch invariants), TR-021 numeric scoring formula (belongs to
  spec-level unit tests).

### W3 — phase-3 replication models (parallel track to W1; different files/area)

Prerequisite: **formal-spec issue 01** (shared quint lib) lands first.

- **Feed-gate/barrier session model** (replication issue 26 class — the d-ii revert
  survives every layer today). Quint model of the session's hold/flush/barrier sequencing
  + the issue-26 option-1 seam extraction so quint-connect drives the *real* decision
  points, not a transcription. FM-CLUSTER-097's node-wide deadline-hold folds into this
  model rather than standing alone. Acceptance = the revert test: bypassing the gate in
  `replica_session.rs` turns the harness red (something other than FM-CLUSTER-097's own
  forcing test).
- **Full-sync/PSYNC handoff model** (fullsync checkpoint acked-write-loss class + the
  issue-24 replid2-shift/atomic-offset-pairing rulings). Models the sync→stream handoff
  window: checkpoint cut, backlog splice, replid/offset pairing across promotion.
- Both models follow the mandatory lifecycle (battery included) and encode the
  replication-correctness ruled ledger (issues 16–19/21–24/26, settled 2026-08-13).

Sequencing ruled: W0 today; W1 and W3 run as parallel tracks (no shared files beyond the
lib; worktree agents keep them apart). Strict-serial and phase-3-first were considered and
rejected.

## Out of scope (this campaign)

- The issue-31 spec amendment + implementation campaign (~40 LOCKED-row rewrites + Rust) —
  separate campaign, still pending decomposition and human staging ruling. The full
  harness rebuild and the remaining 8 design-owner flags ride with it.
- Persistence/txn/blocking models (later phases per §7); representation-level failure
  modes (may never earn a model, per §3).
