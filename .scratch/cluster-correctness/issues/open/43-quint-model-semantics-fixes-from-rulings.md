# Quint model semantics fixes from the 2026-08-22 rulings (R3/R4/R5)

Status: ready-for-agent

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

- [ ] Both unasserted-invariant comment blocks replaced by asserted invariants per above
- [ ] Refusal class carried in payload; V27-M2 fixture run test passes and reaches arm 4b
- [ ] New run tests for the three cell-recreation paths pass
- [ ] Full Quint suite green (`just` quint recipes / existing invocation used by the rework)
- [ ] Mutation spot-checks: guard-deleting mutations for the two new invariants are killed
- [ ] No design-doc edits (campaign owns those); no Rust changes

## Blocked by

None - can start immediately. Independent of the issue-31 implementation campaign (model-only),
but should merge before the campaign wave that cites these invariants.
