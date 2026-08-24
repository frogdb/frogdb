# Quint model semantics fixes from the 2026-08-22 rulings (R3/R4/R5)

Status: done

Size: M

> **Ruling (2026-08-22,
> [work-item rulings](../../2026-08-22-work-item-rulings.md) R3, R4, R5):** the three
> semantics flags raised by the issue-31 Quint rework (Q2-Q4) are ruled; this issue
> applies the rulings to `specs/quint/cluster_migration_failover.qnt` (+
> `cluster_common_types.qnt` as needed). The design doc
> (`../../2026-08-14-issue31-migration-design.md`) stays sole authority; where a ruling
> obliges a design-doc clarifying sentence, that lands with the campaign wave that owns
> the row — not here.

## What to build

### 1. R3 — adoption-time invariant replaces `inv_no_hold_during_staged_flip`

The staging-time invariant is false of the design (coexistence of a latched drain-hold and a
*staged* unadopted flip is lawful — `stagedFlipLeavesHoldRegionTest` at
`cluster_migration_failover.qnt:2214-2229` already witnesses the coexistence). Keep that test.
Work:

- Delete the unasserted `inv_no_hold_during_staged_flip` block and its analysis comment
  (`cluster_migration_failover.qnt:904-911`).
- Add the **adoption-time invariant**: an applied role flip on a node leaves no sourced open
  migration and no held slot on that node — cancellation and hold-release atomic with the
  adoption's applied write.
- Verify the model's `adoptReplicatedRole` (or equivalent applied-write action) actually cancels
  sourced migrations and releases holds in the same step; add the conjunct if missing (that is a
  model-fidelity fix, not a design change — V17-M1 arms already say reports cancel sourced
  migrations).
- Assert the new invariant in the checked set; confirm the mutation battery gives it kill-power
  (at minimum: deleting the cancellation conjunct or the release conjunct must trip it).

### 2. R4 — refusal class minted at verdict, carried in payload

Arm 4b of `isRefusalTerminal` is reachable in the design via the V27-M2 negative-control trace;
the model diverged by recomputing the refusal class at delivery (`identityOrderOk(None, ·)`
vacuously true against an absent stored identity). Work:

- Mint the refusal class at verdict time and carry it in the refusal payload; delivery splits
  arm 4a/4b on the current stored operand only (design doc arm definition at doc:790-812).
- Add the V27-M2 fixture as a model run test: counter loss → FORGET + re-MEET → delayed
  `ordering`-class refusal delivered against the re-created cell (stored identity absent) →
  arm 4b terminal clearing. This pins the arm's reachability.
- Re-check the existing refusal invariants/witnesses still pass with the carried class.

### 3. R5 — `inv_no_record_outlives_its_registration` becomes stale-never-admits

The outliving state is lawful (crash-durable node-local record, level-triggered clearing), so the
containment invariant is unstatable. Work:

- Delete the unasserted block and its analysis comment
  (`cluster_migration_failover.qnt:912-919`).
- Add **stale-never-admits**: no applied adoption/stamp fires from a record whose
  `staged_registration_seq` mismatches the live cell. Assert it; confirm kill-power under
  guard-deleting mutations (deleting the seq-match guard must trip it).
- Add run tests pinning each cell-recreation path followed by `clearStaleRecord`: FORGET +
  re-MEET; other-member reset; HARD reset across reboot.
- Eventual-clearing liveness stays with the Rust forcing tests
  (`staged_record_from_a_dead_registration_is_cleared_at_boot` et al.) — do not attempt a
  temporal liveness property in the model.

## Acceptance criteria

- [x] Both unasserted-invariant comment blocks replaced by asserted invariants per above
- [x] Refusal class carried in payload; V27-M2 fixture run test passes and reaches arm 4b
- [x] New run tests for the three cell-recreation paths pass
- [x] Full Quint suite green (`just` quint recipes / existing invocation used by the rework)
- [x] Mutation spot-checks: guard-deleting mutations for the two new invariants are killed
- [x] No design-doc edits (campaign owns those); no Rust changes

## Blocked by

None - can start immediately. Independent of the issue-31 implementation campaign (model-only),
but should merge before the campaign wave that cites these invariants.

## Resolution

Model-only, `specs/quint/cluster_migration_failover{,_types,_logic,_machine}.qnt`.

**R3.** `adoptReplicatedRole` now cancels the migrations the flipping node sources and releases
its hold region (`held.exclude(cancelled)`, barriers disarmed, feed bytes zeroed) atomically with
the applied local-plane write; `reportRunIdentity`'s admitted-Demotion arm already did the
cancellation and now releases the same way. New ghost `defects.adoptedFlipLeftWork`, latched from
the post-state at both applied-write sites, asserted as
`inv_adopted_flip_leaves_no_sourced_work`. New run test
`adoptedFlipCancelsSourcedMigrationTest`.

*Deviation from the ruling text:* `inv_no_hold_during_staged_flip` is **kept asserted**. The
tree moved under the ruling — V12-M1 (2026-08-19, quint-completeness campaign ledger row 29)
already closed that invariant *effect-based* (`stageFlip` empties the hold region; hold-latching
is disabled at a node with a pending record) and asserted it, and mutation rows M17/M44a draw
kill-power from it. Deleting it would revert a separately settled ruling. Only the two stale
"stated in the doc, NOT stated here" bullets were deleted; the two rulings are reconciled in the
model comments and in the campaign ledger. The witness the issue cites as
`stagedFlipLeavesHoldRegionTest` is `stagedFlipAnswersHeldWritesTest` in the current tree (V12-M1
renamed it with the semantics change); it is kept.

**R4.** `Refusal` loses its mint-time `terminal: bool`; the class is minted with the verdict and
carried, and `observeRefusal` evaluates `isRefusalTerminal(stored_identity, r.class)` against the
operand the observing node holds at delivery — never recomputing the class. New run test
`delayedOrderingRefusalIsTerminalAfterRejoinTest` is the V27-M2 fixture (counter loss → Ordering
refusal minted against a live cell → FORGET + re-MEET → arm 4b terminal clearing), pinning arm
4b's reachability. Existing refusal invariants and witnesses unchanged and green.

**R5.** New `recordPairsWithRegistration` in the logic module; `reportRunIdentity` declines to
*apply* a transition from a record whose `staged_reg_seq` no longer names the live registration
cell. Scoped off the refusal path (a refused report from a stale record is load-bearing — it is
how the rejoin traces mint the refusal that later fails to bind) and off the `Boot` arm (a Boot
report reads nothing the record supplies, so blocking it would strand a rebooted node for no
safety gain). New ghost `defects.staleAdmit`, asserted as `inv_stale_record_never_admits`; also
watched (not guarded) at `completeAdoption`, where a stale admission is structurally unreachable
because every cell-recreating action clears `admitted_stage`. New run tests
`staleRecordAfterForgetAndRejoinTest`, `staleRecordAfterOtherMemberResetTest`,
`staleRecordAcrossWipeAndRebootTest` (HARD reset is not modelled — `resetCluster` is SOFT only by
design, so the path is modelled as wipe + rejoin + `sourceRestart`), each ending in
`clearStaleRecord`, plus the kill-witness `staleRecordCannotAdmitAfterRejoinTest`. No temporal
liveness property attempted.

Verification (local): `just quint-check` clean (21 files); `quint test
cluster_migration_failover.qnt` 93 passing (was 87); `just quint-run` green; witness floor
(2000x40, seeds 0x1/0x2) green with no witness lost. Mutation spot-checks, each reverted:
dropping the cancellation write in `reportRunIdentity` kills
`adoptedFlipCancelsSourcedMigrationTest`; dropping `stageFlip`'s hold-emptying kills
`stagedFlipAnswersHeldWritesTest`, and dropping the adoption-time release on top of it kills
`adoptedFlipCancelsSourcedMigrationTest`; dropping the R5 conjunct kills
`staleRecordCannotAdmitAfterRejoinTest`. All three combinations are deep for uniform sampling
(unreached at 8000x40), so the run tests are the carriers — the same arrangement the model
already uses for its exempted deep witnesses.
