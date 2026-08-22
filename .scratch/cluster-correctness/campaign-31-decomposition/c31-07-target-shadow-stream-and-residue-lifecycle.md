# c31-07: The target-side shadow, the migration stream, and the residue lifecycle

Status: DRAFT — pending wave-0 approval
Wave: 4 (parallel with c31-06)
Size: XL
Crates: `frogdb-cluster`, `frogdb-replication`, `frogdb-persistence`

> Per [R1](../2026-08-22-work-item-rulings.md#r1--issue-31-campaign-staging-clustered-waves),
> each LOCKED row lands atomically with its forcing test and implementation.
>
> [R11](../2026-08-22-work-item-rulings.md#r11--q6-promotion-retry-boundedness-folded-into-campaign-brief)
> applies **primarily** here. There is no standalone issue for Q6 promotion-retry boundedness; it
> is a candidate doc extension. This cluster owns the shadow's `ReportSlotPromoted` path and the
> failed-promotion rollback — the promotion whose retry behaviour Q6 asks about. **If this cluster
> touches promotion retry, the design owner adds the boundedness bound to the design doc during
> this wave, and the Quint model row follows.** The decision is the design owner's, taken during
> the wave, not by the implementer.

This cluster owns everything that happens on the *target* node: receiving the migration stream,
holding the data in a shadow store, replicating and persisting that shadow, promoting it into the
live keyspace, and reaping what is left behind. It is the campaign's other XL cluster and the one
that spans the most crates.

## Owned rows

| Row | Verdict | Semantics | Design-doc citation |
|---|---|---|---|
| TR-CLUSTER-008 | Rewritten | `AssignSlots` refuses all migration phases **and** `handoff_residue` entries, with exactly **two declared arms**: the rollback arm and the orphan re-home arm | blast radius "Rewritten" |
| TR-CLUSTER-009 | Rewritten | `RemoveSlots`' refusal strengthened identically | blast radius "Rewritten" |
| FM-CLUSTER-037 | Rewritten | The commit-to-apply window, now with the §5 pre-promotion refusal | blast radius "Rewritten" |
| FM-CLUSTER-097 | **Retired** | The `ReplicaFeedGate`'s purpose re-derives to nothing under source authority. The row is **rewritten to assert the absence** of migration feed holds; the gate itself is deleted | blast radius "Retired" (§8) |
| FM-REPLICATION-021 | Cited, unmoved | *(declared in c31-01; consumed here)* | blast radius, revision-18 additions |

### New rows this cluster writes

**Target ingest and the stream session (§4, §8)**

1. Target ingest and resume, with `(run_id, position)` **receipt assertions**, each assertion
   carrying its own declared consequence — including the zero-advance no-op.
2. The **per-migration backlog "history intact" floor**
   (`cluster-migration-backlog-max-bytes`, 64 MiB, stamped at `Begin` by c31-05).
3. The **coverage obligations**: the drain flush and the periodic coverage report.
4. The **received-head versus applied-head** definitions.
5. **`ReportMigrationIngest`** admission effects on the target side.
6. **`ReportTargetReplicaAck`** and the `target_replicas_acked_pos` field, gated by
   `cluster-migration-require-target-replica-ack` (default off).
7. The **migration-stream session family**, including resume-from-own-backlog.
8. The **durable-attestation rule** — the target proposes only fsync-durable positions — and its
   replica-side fsync-attestation half.
9. The **`target_attesting_run` / `target_run` pairing** with cross-run replacement semantics, and
   `Complete`'s current-run conjunct.
10. **`covered_applied` durability inheritance.**

**The shadow store (§5)**

11. **Shadow durability** and the full-sync payload row, plus the **replication-side
    `+FULLRESYNC` payload-contents row** in `specs/replication.md`.
12. **Shadow replication through the target feed**, the **honest no-escape-hatch row**, and the
    replicated durability conjunct.
13. **Shadow promotion** and the **pre-promotion refusal**, including the replica window.
14. **Target discard**: the residue-guarded level sweep, reset-discard, and Begin-time
    discard-then-ingest.
15. **FLUSH and memory-pressure aborts** with the target as proposer.
16. **The expiry-convergence row.**
17. The **applied-attestation serving gate** and the rollback re-label reversal.
18. The **crash-atomic re-label** with its named intermediate state.

**The residue lifecycle (§7)**

19. **`handoff_residue` lifecycle**, **`ReportSlotPromoted`**, **`ConfirmSlotDeleted`**, the
    **attestation-gated delete**, and the **failed-promotion rollback**. *(R11 attaches here.)*
20. **Replicated residue deletion**, the **defer-while-Replica guard**, notification suppression,
    and ordering.
21. **`ClearSlotResidue`** as an operator verb, with its no-lawful-automatic-remover admission
    rule, its `promoted == true` conjunct, and its `target_gone == false` conjunct.
22. The **orphan re-home arm's case-4b split**.
23. **Residue lifecycle under membership change.**
24. The **at-most-one-residue-entry-per-slot invariant**.

## What to build

### 1. Spec deltas (first)

1. `specs/cluster.md`: the §4 ingest and stream-session rows, then the §5 shadow rows, then the §7
   residue rows, then TR-CLUSTER-008/009's strengthened refusals with their two arms, then
   FM-CLUSTER-037's rewrite, then FM-CLUSTER-097's retirement.
2. `specs/replication.md`: the `+FULLRESYNC` payload-contents row. This is the campaign's only
   replication-spec addition; FM-REPLICATION-021 is cited and not contradicted.
3. The at-most-one-residue-entry-per-slot invariant as a state-space invariant.

### 2. Forcing tests (second, observed failing)

Ingest and stream tests in `frogdb-cluster` / `frogdb-replication`; shadow persistence tests in
`frogdb-persistence`; residue tests in `frogdb-cluster`.

- `ingest_receipt_asserts_run_id_and_position_with_declared_consequences`
- `a_zero_advance_receipt_is_a_no_op`
- `backlog_beyond_the_floor_is_refused_with_history_intact`
- `drain_flush_and_periodic_reports_cover_the_stream`
- `received_head_and_applied_head_are_distinct_and_both_defined`
- `report_target_replica_ack_advances_target_replicas_acked_pos`
- `require_target_replica_ack_off_does_not_gate_complete`
- `a_session_resumes_from_its_own_backlog`
- `the_target_proposes_only_fsync_durable_positions`
- `a_cross_run_attestation_replaces_rather_than_merges`
- `complete_requires_the_current_target_run`
- `shadow_survives_restart_and_is_carried_by_full_sync`
- `shadow_replicates_through_the_target_feed`
- `there_is_no_escape_hatch_that_serves_shadow_data_early`
- `promotion_is_refused_before_the_replica_window_closes`
- `a_failed_promotion_rolls_back_and_reverses_the_re_label`
- `the_re_label_is_crash_atomic_with_a_named_intermediate_state`
- `begin_discards_any_prior_shadow_before_ingest`
- `reset_discards_the_shadow`
- `a_level_sweep_discards_only_shadows_with_no_residue_guard`
- `flush_and_memory_pressure_abort_with_the_target_as_proposer`
- `expiry_converges_between_shadow_and_live`
- `assign_slots_refuses_a_slot_with_a_residue_entry`
- `assign_slots_rollback_arm_and_orphan_rehome_arm_are_the_only_two`
- `orphan_rehome_case_4b_is_split_and_reachable`
- `clear_slot_residue_requires_promoted_true_and_target_gone_false`
- `clear_slot_residue_is_refused_while_a_lawful_automatic_remover_exists`
- `residue_deletion_defers_while_the_node_is_a_replica`
- `at_most_one_residue_entry_per_slot`
- `no_migration_feed_hold_exists` (the inverted FM-CLUSTER-097)

### 3. Implementation surface

- `frogdb-cluster-runtime/src/bus.rs` and a new stream-session module — the migration stream, its
  backlog, and resume.
- **Delete `ReplicaFeedGate`** and its wiring. Hardening-2 rework 12 introduced it (FM-CLUSTER-097,
  the node-wide deadline-hold); §8 removes it. That issue gets a superseding note referencing this
  cluster.
- `frogdb-cluster/src/commands.rs` — the residue arms: `ReportSlotPromoted`, `ConfirmSlotDeleted`,
  `ClearSlotResidue`, and `AssignSlots`/`RemoveSlots`' strengthened refusals with the two arms.
- `frogdb-cluster/src/storage/` — the shadow store, its re-label, and its discard paths.
- `frogdb-replication` — the shadow's carriage in the full-sync payload and the target feed.
- `frogdb-persistence` — shadow durability and the crash-atomic re-label.

## Acceptance criteria

- [ ] All owned rows amended and all twenty-four new-row items landed across `specs/cluster.md` and
      `specs/replication.md`.
- [ ] FM-CLUSTER-097 rewritten as an **absence** assertion and `ReplicaFeedGate` deleted, with its
      forcing tests re-pointed in the same change. Hardening-2 rework 12's issue gets a superseding
      note.
- [ ] `just lint-spec` green across both specs.
- [ ] Every forcing test observed failing before implementation, green after.
- [ ] `just mutants-diff frogdb-cluster`, `just mutants-diff frogdb-replication`, and
      `just mutants-diff frogdb-persistence` run and triaged.
- [ ] `just lint` / `just lint-gates` green — the **durable-ack-write seam** is directly
      load-bearing (the target proposes only fsync-durable positions).
- [ ] **R11 discharged**: either the design owner added the promotion-retry boundedness extension
      during this wave (recorded here with the doc citation), or the implementer recorded that the
      promotion-retry path was not touched.
- [ ] Cluster-correctness issue 15 closed as superseded, and issue 16 closed as subsumed, with
      notes pointing at the rows that discharge them.

## Blocked by

- **c31-05** (wave 3) — `handoff_residue` is written by `Complete`; the reaper's fence and the
  ingest admission conjuncts are c31-05's; the backlog and replica-ack knobs are `Begin`-stamped
  there.
- **c31-01** (wave 1) — `run_identity` and the run triple underpin the attestation pairing.
- **c31-03** (wave 2) — the residue marking (`source_gone` / `target_gone`) is that cluster's, and
  `ClearSlotResidue`'s `target_gone == false` conjunct reads it.
- spec-gaps issue 12 (watermark carries the covered position) — **already landed** at `eedb76d0`;
  it is the substrate `covered_applied` inherits from.

## Blocks

- c31-08 (TR-CLUSTER-035's shadow-discard trigger needs the shadow to exist; `CLUSTER MIGRATIONS`
  renders residue entries).

## Related trackers

- Closes cluster-correctness issue 15 (repatriation, superseded by source-authoritative-until-
  commit) once the §4 endpoint failover/restart rows land.
- Closes cluster-correctness issue 16 (`AssignSlots` ignores open migrations), subsumed by
  TR-CLUSTER-008/009's strengthening.
- Supersedes hardening-2 rework 12 (`ReplicaFeedGate`).
