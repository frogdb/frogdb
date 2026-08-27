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
| **`inv_no_hold_during_staged_flip` false-of-model (V12-M1)** | **Effect-based closure**: `stageFlip` empties the node's hold region in the same step (held writes answered — refusal/redirect, as befits an about-to-demote primary), and hold-latching is disabled at a node with a pending record. The invariant is then asserted. Rationale: demotion must not be blockable by a wedged drain (the issue-17 class); matches the doc's "answered, never held" literally. The guard-based alternative (refuse staging while holds outstanding) was rejected for wiring demotion liveness to drain-release liveness. **Extended by R3 (2026-08-22, cluster issue 43)**: this ruling stands as written (the invariant stays asserted); R3 adds the *adoption-time* companion `inv_adopted_flip_leaves_no_sourced_work` — an applied role flip leaves the node sourcing no open migration and holding no slot, cancellation and hold-release atomic with the applied write. |
| **M32 — cross-shard demotion successor has no observable** | **Defect ghost restating the guard**: `demoteNode`'s apply recomputes "was the chosen successor a replica of the demoted primary in the pre-state" and sets `defects.crossShardSuccessor`; invariant `not(defects.crossShardSuccessor)`. Postcondition-derived per the defects-family convention → M32 becomes CAUGHT-P. Recorded: the *semantic* observable (successor holds the shard's data) is phase-3 replication-model territory. |
| **Dead arm 4b of `isRefusalTerminal` (`Ordering ∧ stored == None`)** | **Delete from the design doc** — dead by construction: an Ordering refusal presupposes a stored cell; Fenced/Membership own the empty-cell refusal cases. Comment at `refusalIsTerminal` cites this ruling. If the spec amendment surfaces an impl path emitting Ordering against an empty cell, that is a new ruling with a forcing test. **Withdrawn** (see the R2 note below: the kind-scoped absent-operand arm makes arm 4b live), and **superseded by R4 (2026-08-22, cluster issue 43)**: the refusal class is minted with the verdict and carried in the payload; terminality is decided at delivery against the operand the observing node stores then. Arm 4b's reachability is pinned by `delayedOrderingRefusalIsTerminalAfterRejoinTest` (the V27-M2 fixture). |

Accepted structural limits, documented at the model, no further action: **M22**
(step-unwiring needs a temporal operator — mitigated by the temporal-nightly item below),
**M06** (compound-only visibility with M08). Detector-attribution flags (F6/F7/F13,
M38/M39, M34 form) are mechanical design-doc text corrections — bundled into W1.

### Batch disposition — cluster issue 44 / R2 (landed 2026-08-23)

The five attribution corrections and the four accepted-limitation notes were re-measured
against the **post-R3/R4/R5 model** (cluster issue 43) one mutation at a time — mutate,
`quint test` the migration model, walk the full 42-invariant conjunction, revert — and
recorded at their sites. Two of the Q4-era corrections were themselves wrong and are
now superseded:

| Row | Corrected attribution (measured 2026-08-23) | Site |
|---|---|---|
| **F6 / M65** (delete `clearStaleRecord`) | **CAUGHT-T, five run tests**: `recordOutlivesRegistrationTest`, `staleRecordAfterForgetAndRejoinTest`, `staleRecordAfterOtherMemberResetTest`, `staleRecordAcrossWipeAndRebootTest`, `foreignRefusalDoesNotDisposeTest`. No invariant fires (2000×40, seeds `0x1`/`0x2`); `fenceClearsWithinThreeStepsTest` **passes**, so the `witnessFenceClears` half does *not* bite either — the Q4 correction had the halves inverted. R5's `inv_stale_record_never_admits` is untouched by this mutant. | design doc, ext-18 mutation (15) |
| **F7 / M59** (drop the refusal's `stage_id` binding half) | **MISSED**. 93/93 run tests pass, walk clean at 2000×40 — the surviving `reg_seq` half of `refusalBinds` discriminates in every trace the battery reaches. `inv_no_live_record_disposed_by_a_foreign_refusal` / `foreignRefusalDoesNotDisposeTest`, named by the Q4 correction, are the **other** half's detector (row **M68**, drop `reg_seq`): measured, that mutant fails exactly `foreignRefusalDoesNotDisposeTest`. Closing M59 needs a fixture that re-stages under the *same* registration (via `completeAdoption`) with a dead stage's refusal in flight — model work, not a doc fix. | design doc, ext-17 mutation (9) |
| **F13 / M21** (target-departure unassign+mark) | Unassign half: `inv_slot_owner_valid` at 500×20 + `orphanRehomeToSourceTest`, `removeNodeForceEvictsLiveOwnerTest` (unchanged). Mark half: no invariant at 2000×40, **three** tests — `reapDeferredWhileTargetGoneTest`, `orphanRehomeToSourceTest`, `orphanRehomeToAnotherPrimaryTest`; `failPromotionRefusedAfterSourceDepartedTest` dropped from the list. | design doc, ext-12 residue row |
| **M38 / M39** (volatile record / adoption edged on the admission apply) | Correction stands: both are CAUGHT-T, `inv_member_keyspace_is_tracked` green at 2000×40 for each. M38 now also fails R5's `staleRecordAcrossWipeAndRebootTest`; M39 fails the four already named. | design doc, ext-15 staged-flip row |
| **M34** (`admitted` vs `identityWritten` binding) | **Equivalent mutation, no kill expected**: `identityOrderOk`'s `Some` arm is `lexGt` (strict domination) and its `None` arm makes `Some(identity) != None` trivial, so `admitted ≡ identityWritten` in this model. The row's killing form is the unconditional one (cancel on a *refused* report), caught by `inv_no_spurious_cancel`. | comment at `identityWritten`, `cluster_migration_failover_machine.qnt` |

Accepted-limitation notes (each ending "revisit only if campaign work makes the observable
cheap") were recorded at the model: **M06** at `reportTargetIngest`, **M22** in
`cluster_migration_failover_temporal.qnt`'s header, **M32** at the
`defects.crossShardSuccessor` field, **M37** at `completeAdoption`. M32 and M37 are
CAUGHT since `2eb66e35`; what stays accepted is the *residual* — M32's kill is a
postcondition ghost restating the guard rather than a doc-level observable (the semantic
observable is phase-3 territory), and M37's `keys` is a slot-id set, so the modelled
discard is the slot-claim projection of a byte-level dataset drop.

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

---

## 2026-08-20 post-execution rulings (design owner)

The campaign executed 2026-08-19/20 (all 15 tasks; commits `75ca5def`…`4b22a62e` + the
harness-coverage follow-up). Six questions surfaced by the execution were put to the design
owner on 2026-08-20 and ruled as follows. Each ruling follows spec-first order where it
touches a LOCKED row (row → failing/forcing test → fix).

### R1 — Issue 41 (chained-demotion copy-claim family): repoint + tighten guard

Both sub-roots are fixed, matching Redis/Valkey semantics:

- **Demotion/adoption repoints dependants.** The demotion family (`stageFlip` /
  `adoptReplicatedRole` / `setRole`) re-parents a demoted primary's replicas at the new
  primary. Chained (`1→4→3`) and cyclic/primary-less (`1↔4`) topologies become unreachable;
  the one-hop `shardPrimary` closure stays sound as-is.
- **`canRetargetSlotResidue` demands physical holding.** A residue entry only re-homes onto
  a node whose own `keys` contain the slot — never onto a derivative (closure-edge) holder.

Then: model fixed per both, steering re-landed (revert `9c5d6f17`), steered walk clean at
500x40 across seeds with all four family invariants restored, battery rows added for the
new guard/effect, M37 discard rows re-checked (per issue 41's acceptance list). Issue 41
moves to ready-for-agent.

### R2 — Arm 4b of `isRefusalTerminal`: model the V18-M1 narrowing

Option 1 of `t1-blocked.md`. `identityOrderOk` becomes kind-sensitive — false for
`kind = Demotion` against an absent stored cell — making arm 4b (`Ordering ∧ stored ==
None`, the terminal clearing arm) live in the model with a forcing run test. The design doc
stays as written; the 2026-08-19 "delete arm 4b" ruling is **withdrawn** (its premise was a
model gap, not a dead doc arm).

### R3 — FM-CLUSTER-097 vs ending-drain: amend the row

Ruling 1 of `t9b-blocked.md`. FM-CLUSTER-097's Observable gains an explicit
"unless the link is ending" clause: a session that has already classified its departure
(`SourceClosed`/`SourceLagged`) drains what it already accepted past the barrier floor —
dropping the tail is strictly worse for a replica about to reconnect and PSYNC from its own
offset. The row must also state the protection that makes this safe: **a departing replica
is not a promotion candidate for the slot under handoff** (this closes the
Graceful-disarm ⊕ delivered-tail combination with FM-REPLICATION-062). The model's
`isEnding(ss)` carve-out in `inv_no_ship_inside_barrier_window` stays, now ruling-backed;
`closeInsideWindowDrainsThenEndsGracefulTest` remains the pin.

### R4 — Fullsync checkpoint-cut reading: offset-addressed skip

The overlap stays structural and harmless **by construction**: replay is offset-addressed
and the replica skips frames at or below its applied offset, so re-delivery is a no-op.
FM-REPLICATION-004/001 stand; issue-24 amendment point 2 narrows to what it actually
forces — the persisted offset commits atomically with the write it names. The skip rule is
stated in a spec row and modeled (the model's `coverage.reapplied` record becomes the
witness that the skip is exercised).

### R5 — FM-REPLICATION-004 overshipped-tail residue: truncate above claim

New replica-side rule (new row): on accepting a partial resync under a changed history
(the replid2 window), the replica discards/truncates everything above its claimed offset
before splicing — Raft-style divergence truncation. The residue class (a forked position
the new history never reaches) is removed by construction. Forcing test + model action
required; `overshippedTailMeetsFailoverTest` / `coverage.forkedTailReplaced` extend to pin
the truncation.

### R6 — I09: widen `inv_identity_pair_monotone`

Ruled on operator expectation: the `(generation, offset)` identity pair is an
operator-facing total order — later observation ≥ earlier, lexicographically, across ANY
step into Primary (promotion included). The issue-24 pairing row is amended to state
gen-domination on promotion as a guarantee (promotion mints a strictly greater
generation), the invariant's `prev.primary` antecedent is dropped, and a forcing test
pins the promotion step. Battery row I09 flips to caught-by-construction.

### R7 — R4 residue: replica-side skip at-or-below the applied head

R4's harmless-by-construction premise was half false: the offset-addressed skip is
sender-side only, keyed on the replica's *claimed* offset (`ring_buffer.rs` backlog
extraction, `feed_sequencer.rs` resume buffer). The checkpoint cut captures
`snapshot_offset` before the cut, so the payload deliberately overships *above* the
claim — a range no sender-side skip covers — and the replica's `consume_frames` applies
verbatim with no dedup; propagation is non-idempotent (`INCR`/`LPUSH`/`APPEND`).

Ruling (2026-08-20): **replica-side skip** — the replica ignores frames at or below its
applied head. Receiver-authoritative dedup, Raft-style; covers this hole and any future
sender-side accounting bug. New failure-mode row + forcing test in `frogdb-replication`;
FM-REPLICATION-001's corrected non-guarantee bullet (landed `a2334b00`) upgrades to a
guarantee once the row lands; model's `inv_reapply_is_a_noop` graduates from abstraction
guard to modeled behavior. Tracked as replication-correctness issue 34.

### R8 — steered-walk residual root 1: repoint resets attestation

`retargetResidueOnDemotion`'s target arm may re-point an unpromoted dependant at the
failover successor (which legitimately holds the slot's keys post-`applyFailoverCommon`),
but the repointed dependant must re-attest against its new home before serving.
Serve-before-attest is the violation; the repoint itself is sound. Trace:
`demoteNode(4, 2)`, seed 777.

### R9 — steered-walk residual roots 2+3: extend R1 family-wide

R1's physical-holder principle applies at every retarget/re-home site, not only
`canRetargetSlotResidue`: (2) the source arm must not re-home onto a successor whose
hold exists only through the demoted node's closure edge; (3) a source demoted while
still holding keeps the claim but must retain a remover disjunct — the removal path
cannot be gated on a live primary (`canFailPromotion`), or the residue is unreapable.
Consistent with the slot-handoff-barrier family-sweep precedent.

### R10 — R2 absent cell: model the forget-identity arm

`stored_identity` gains a clearing arm (reset/forget) so the absent-operand cell of
`identityOrderOk` is reachable by the walk, not only by constructed-view test. Model
completeness ruling — land regardless of whether Rust exposes an equivalent path today;
if Rust has a real forget path (CLUSTER RESET / state-file loss), align the arm's guard
with it.

### R11 — steering disposition: opt-in lane + nightly

Steered walk lands as a dedicated recipe (`just quint-run-steered` or similar) plus a
nightly CI lane; default `just quint-run` stays deterministic-green unsteered. Batteries
and campaigns run the steered lane. Re-land gated on the steered walk coming back clean
after R8/R9 fixes.

### R12 — D1: the normal full-sync handoff double-applies the overshipped range (new defect, own issue)

Investigation of TR-REPLICATION-034's premise (2026-08-21) closed a chain worse than the
row's own precondition — no link break and no failover needed. `snapshot_offset` is
captured before drain and cut (`replica_session.rs:1199`), writes landing during the
drain enter the checkpoint above it (`checkpoint_quiesce.rs:34-35`), the offset
`fetch_add` sits in `ReplicationBroadcast` (the *last* write effect) so a write is
drainable before it is even counted, and the handoff then replays
`(snapshot_offset, current]` — re-executing the overshipped writes the installed
keyspace already holds. FM-REPLICATION-065's skip is at-or-below the head only; these
sit above. Verbatim non-idempotent stream ⇒ silent divergence in the healthy path. No
test pins it (the FM-004 forcing tests prove no-loss only; the integration test uses
idempotent `SET`s). Ruling: file as its own replication-correctness issue with a
forcing-test mandate; TR-REPLICATION-034 folds into the same fix campaign.

### R13 — fix direction: per-shard coverage vector now (V), offset-stamped batches later (S)

Three flavors of one root (payload/keyspace coverage runs ahead of the claimed offset):
fullsync overship (D1), restart offset-bias (`replication.md:360`, ruled-not-implemented),
and TR-034's replaced-history residue (D2). Ruling: **V now** — the full-sync trailer
carries per-shard coverage watermarks `Y_s` (each shard's last-broadcast offset at its
drain-ack / export-message capture point; exact because shards are single-threaded), the
replica installs them as per-shard skip floors extending `covers()`, and a refusal rule
handles D2. **S later, separate issue** — offset-stamped RocksDB batches (per-shard stamp
keys; a single global key would overclaim) make the artifact self-describing and fix the
restart bias; it is a *sender-side source swap* under V's unchanged wire format and
replica logic, not an alternative (the live-dataset path needs V's capture regardless).
Checkpoint path additionally holds the flush engine between a shard's drain-ack and the
cut so nothing above `Y_s` slips into RocksDB (cut is hard-link-fast).

### R14 — refusal scope: window grants only, guarded by `applied < max(Y_s)`

Same-history `+CONTINUE` is safe at any applied head: same replid ⇒ the overshipped
effects are a prefix of exactly what will be replayed, and the floors dedup them exactly
(a shard-`s` frame never straddles `Y_s` — it is that shard's own frame boundary; mixed
skip/apply inside a cross-shard group is correct because it mends the torn checkpoint
exactly once). A window grant with `applied ≥ max(Y_s)` is a clean shared-prefix state
(exactly-once already holds for `[0, applied]`, and the window gives `applied ≤
second_repl_offset`). The only unsafe cell is a window grant with
`applied < max(Y_s)` — old-history effects above the claim may never be reproduced by
the granting history — and that cell is refused (degrade to full resync). Floors reset
at each install, so stale vectors cannot linger across successive full syncs.

### R15 — the floor vector persists with the staged install metadata

The `Y_s` vector rides in the FM-PERSISTENCE-039 staged `replication_metadata.json`
(stamped by the replica at install), so a crash between install and reconcile recovers
the floors instead of reopening the D1 window on a persistence-enabled replica.

### R16 — interim recovery rule: a floorless crash-recovered stint refuses window grants

A stint whose offsets came from crash-recovery (not from a completed sync or clean
shutdown) has a keyspace possibly ahead of its claim and no vector — V's guard is blind
there. Until S lands, such a stint refuses window grants unconditionally (one extra full
resync in the rare crash+failover shape). The same-history restart bias itself — the
offset stamped low against RocksDB contents — has no sound interim short of full resync
on every restart and stays a documented gap owned by the S issue.

## 2026-08-22 — issue-36 redesign grill: rulings R17–R24

Grill session over issues 35/36 (post-landing). Investigation (read-only agent) found three
structural facts that invalidated issue 36's original sketch: (1) the staged WAL entry is
already owned by the flush thread by mint time (`writer.rs:110-135` sends a fully-owned
entry at effect 6; mint is effect 8), so "single-threaded write-back" does not exist;
(2) under sync durability the batch commits **before any write effect** — before the offset
exists (`execution.rs:463-496`), so a same-batch stamp is impossible without reordering; and
(3) `is_active()` gates minting entirely (`primary/mod.rs:1023`), so a never-had-a-replica
primary mints nothing and stamps would be absent on standalone nodes. Additionally the
restart path keeps the same replid at a rewound `offset_at_save` with an empty backlog, so
exact stamps alone cannot make same-replid restart safe: the broadcast-but-unflushed tail
under relaxed durability is unknowable at recovery, and its offsets were already shipped.

### R17 — scope: full redesign, primary side included

Not replica-only. The primary-side ordering problems are fixed at the root rather than
guarded around.

### R18 — mint+enqueue moves to the persist point (CRDB applied-index pattern)

The replication stream is treated as a log: the offset is assigned *before* staging, and
the per-shard "max offset flushed" stamp commits in the **same RocksDB WriteBatch** as the
data it covers — the direct transliteration of CockroachDB's `RaftAppliedState`-in-the-
apply-batch (position exists before application, rides the batch) and FDB's
sequencer-before-durability. Mint and backlog-enqueue stay **fused** (they are one critical
section today, which is what makes mint order = wire order); both move to the persist
point. Under `should_confirm` the pre-effect Committed persist carries the stamp, so the
client ack covers data+stamp atomically with no added latency. Effect 8
(`ReplicationBroadcast`) shrinks to bookkeeping. Consequence accepted: replicas can receive
a frame before primary-local effects (notifications, waiters) run — same exposure direction
as today, earlier arrival, no cross-node invariant depends on it. The unified-log limit
case (stream *is* the WAL) was considered and deliberately not taken — right invariant,
disproportionate replumb.

### R19 — count always, enqueue when active

The offset counter advances for every replicable write on every node (form length is
knowable at persist; suppressed/`NO_PROPAGATE` forms advance by 0 and stay outside the
stream claim — their effects ship only via payloads). Backlog append and socket feed stay
gated on activity, so standalone nodes pay no memory. Stamps are therefore exact on every
node, and a late-attaching replica is consistent by construction (payload carries all
effects up to the counter; the stream continues above it).

### R20 — primary boot rotates the replid unless the shutdown was clean

Refines issue 24's 2026-08-13 ruling ((a)+(b), Redis PSYNC2 shape) with the mechanism and
the clean-shutdown carve-out: an unclean boot shifts the loaded id into `secondary_id`
bounded at the recovered head (max over per-shard stamps) and mints a fresh primary id; a
clean shutdown (drained + flushed + marker recording head == stamps) keeps the identity so
rolling restarts do not force fleet-wide full resyncs. Rotation is what makes the
unknowable shipped-but-unflushed tail harmless — offset reuse across distinct ids cannot
divergently continue. A restarted primary has an empty backlog and can never serve
`+CONTINUE` regardless, so rotation costs nothing at reconnect time.

### R21 — the FlushHold dies

With stamps in the batches, the cut artifact self-describes its coverage: the sender opens
the checkpoint's CFs and reads each shard's stamp as `Y_s`. Any write that slips past the
drain carries its own stamp, so the artifact and its claim cannot disagree. The hold, the
breach-abort machinery, and the interim breach counter (ruled earlier this session as
issue-35 polish) are all deleted. The drain stays (bounds payload staleness). The
live-dataset path keeps issue-35's export-message capture — it has no artifact.

### R22 — floors unify into the stamps; R15 plumbing and R16 refusal retire

An installed checkpoint already *contains* the primary's stamp keys, so install adopts the
floors automatically — in RocksDB, atomic with the data, crash-safe by construction.
Replica-side per-frame stamping (the replica knows the frame's offset before applying;
the persist seam takes the offset — primary mints there, replica supplies) keeps them
current. The staged `replication_metadata.json` coverage vector and
`ReplicationState.coverage_at_save` are deleted — one source of truth. A crash-recovered
replica stint reconstructs exact floors and an exact applied head from the stamps, so the
R16 unconditional window-grant refusal retires with it. The trailer `ShardCoverage` field
**stays** as the wire representation (the live-dataset path has no artifact; install
writes the trailer's values as stamps; the checkpoint path carries it for uniformity).

### R23 — the stamp lives in a reserved per-shard metadata CF

RocksDB WriteBatch atomicity spans column families, so the stamp does not need to live in
the shard's Main CF to commit atomically with it. A reserved metadata CF (search_meta
reserved-prefix precedent) keeps the user keyspace clean — no SCAN/RANDOMKEY/DBSIZE
filtering hazard.

### R24 — model the restart properly; PRD + four sequenced issues

The fullsync model's documented `applyRestart` gap (no reachable recovered-node states) is
opened up rather than widened: restart transitions (lose the unflushed tail, recover
stamps, rotate-unless-clean) with invariants for no-offset-reuse-within-a-history and
claim == coverage after recovery. Work lands as PRD (issue 36 rewritten) + four issues:
**37** mint-at-persist + count-always + primary stamps; **38** replica stamps + floors
unification + R15/R16 retirement; **24** (existing, amended) identity rotation +
clean-shutdown marker + state-file demotion; **39** hold deletion + sender-reads-artifact.
38/24/39 depend on 37.

Interim note: the breach counter ruled at the top of this session (issue-35 close-out
gate) still lands — it is real observability until 39 deletes the machinery it counts.
