# c31-05: The migration record, the phase machine, and admission

Status: DRAFT — pending wave-0 approval
Wave: 3 (alone — this is the campaign's critical path)
Size: XL
Crates: `frogdb-cluster`

> Per [R1](../2026-08-22-work-item-rulings.md#r1--issue-31-campaign-staging-clustered-waves),
> each LOCKED row lands atomically with its forcing test and implementation.
>
> [R3](../2026-08-22-work-item-rulings.md#r3--inv_no_hold_during_staged_flip-adoption-time-rule)
> binds this cluster's cancellation semantics: cancellation is **atomic with the adoption's applied
> write**. The adoption-time invariant — an applied role flip on a node leaves no sourced open
> migration — is forced jointly with c31-04; this cluster owns the cancel half.

This is the heart of the redesign: the replicated migration record, the phase machine that walks
it from `Snapshotting` to committed, and the admission conjunctions that make every step
deterministic. It runs alone in wave 3 because both wave-4 clusters consume it, and it is the
cluster most likely to want splitting (see the sequencing plan's section 8 for the seam).

## Owned rows

### Transitions

| Row | Verdict | Semantics | Design-doc citation |
|---|---|---|---|
| TR-CLUSTER-010 | Rewritten | `BeginSlotMigration` creates a record at `phase = Snapshotting`; requires `slot_map[slot] == source` | blast radius "Rewritten" |
| TR-CLUSTER-011 | Rewritten | `PrepareSlotHandoff` may only be proposed at parity; it arms the barrier and increments the generation | blast radius "Rewritten" |
| TR-CLUSTER-012 | Rewritten | `ConfirmSlotHandoffDrained` seals the slot and sets `drained_pos` | blast radius "Rewritten" |
| TR-CLUSTER-013 | Rewritten | `CompleteSlotMigration` is admitted on the **logical `handoff_seq` token** and reads the slot map (`slot_map[slot] == record.source`) | blast radius "Rewritten" |
| TR-CLUSTER-014 | Rewritten | `AbortSlotHandoff` clears `drained_pos` and `attempt_id`, and increments the attempt count | blast radius "Rewritten" |
| TR-CLUSTER-015 | Rewritten | `CancelSlotMigration`: the repatriation precondition is **retired**; the transition emits target-discard and release events; the target joins the proposer set | blast radius "Rewritten" |

### Failure modes

| Row | Verdict | Semantics | Design-doc citation |
|---|---|---|---|
| FM-CLUSTER-031 | Rewritten | The `SETSLOT` surface, with owned idempotency: a re-issued `MIGRATING` is `Ok` and the one-shot attempts counter resets | blast radius "Rewritten" |
| FM-CLUSTER-032 | Rewritten; one arm **retired** | Membership arms kept. **V7-M4 reverses V4-m5**: the unassigned-slot arm is retired — migrating an unassigned slot is refused, with the error directing the operator to `AssignSlots` | blast radius "Rewritten" (V7-M4) |
| FM-CLUSTER-033 | Rewritten, **headline inverted** | `Complete` **does** read the slot map. The row now specifies the `SlotAlreadyAssigned`-at-completion refusal and the `target ∈ nodes` conjunct | blast radius "Rewritten" |
| FM-CLUSTER-034 | Rewritten | The client `ClusterEvent` contract: exactly one `SlotMigrationCompleted` on success, nothing on failure, nothing at begin. The §9 operator log is a **distinct stream** | blast radius "Rewritten" |
| FM-CLUSTER-035 | Rewritten | `SETSLOT … STABLE` cancel semantics under the new record | blast radius "Rewritten" |
| FM-CLUSTER-084 | Rewritten | Admission conjunctions including the run and proposer guards on every position writer | blast radius "Rewritten" |
| FM-CLUSTER-085 | **Retired** | The handoff lease. Its property — a dead finalizer cannot wedge a slot — is re-provided by the observation bound plus the leader auto-`Complete`; a replacement row states this explicitly | blast radius "Retired" |
| FM-CLUSTER-086 | Rewritten | Attempt stamping; the generation is incremented by **`Begin` and by `Prepare`** | blast radius "Rewritten" |
| FM-CLUSTER-087 | Rewritten | Release events fire from Cancel, Abort, Complete, prune, and self-fence | blast radius "Rewritten" |
| FM-CLUSTER-089 | Rewritten, **not retired** | With the deadline mechanism gone, the row asserts the determinism rule directly: no replicated admission predicate reads node-local state. **Both existing forcing tests are re-pointed**, not deleted | blast radius "Rewritten" |
| FM-CLUSTER-090 | Rewritten | Barrier action under the new phase machine | blast radius "Rewritten" |
| FM-CLUSTER-091 | Rewritten, **split** | The drain-wait `-TRYAGAIN` refusal is **retired** (a write is held or acked, never bounced); the bounded drain-wait property becomes the pre-`Confirm` unconditional observation bound | blast radius "Rewritten" (V5-M1) |

### State-space rows

The five migration-record SS rows — "Open migrations", "A migration's prepared handoff", "A
handoff's attempt number", "A handoff's deadlines", "Handoff attempt counter" — are recorded by
revision 16 as **joined, not newly amended** (V16-M2). This cluster owns all five.
`ReportRunIdentity` is a declared writer of "Open migrations" (V17-M1), so the writer-join row is
amended and the corresponding §3 exit row is added by c31-06.

**"A handoff's deadlines" is open question OQ-2** in the sequencing plan: with the deadline
mechanism gone, wave 0 believes the row is Retired, but the join implies otherwise. Do not amend
that row until OQ-2 is ruled.

### New rows this cluster writes

- The **typed record declaration** (the replicated migration record's fields and types) and the
  `handoff_residue` map type. The residue *lifecycle* is c31-07's; the type declaration is here so
  c31-07 and c31-06 both have it.
- The **four captured parameters as immutable record fields**, stamped at `Begin`, plus the
  **no-re-stamp rule**: a config change mid-migration does not retroactively change a record.
- **`Begin` / re-issue residue conjuncts** and the reaper's residue fence — the admission side only.
- The **progress-sensitive observation bound**: the narrowed reset, the `last_observation` dedup,
  and the full `ObserveMigration` conjunction.
- The **leader auto-`Complete`**.
- **`Confirm` resets the observation counter** — the field-writers table's third reset trigger,
  with `≥`-bound semantics.
- The **pre/post-`Confirm` observation-bound split**, the tick-cadence knob, the **calibration
  obligation**, and the one-shot attempts reset.
- **`Complete`'s target-role conjunct** (`nodes[record.target].role == Primary`).
- The **counted-replica-set definition**, with empty-set and unset-false handling, and the
  set-change floor clearing.
- The **bootstrap and join proposal orderings** plus the `node ∈ nodes` conjunct.
- The **`Prepare` / `Begin` / `Complete` absence tests** (the §0 absent-operand rule's
  applications).
- The **§1 operator-exit row** for a pre-`Draining` wedged source.
- **Full payload declarations for every transition** in the migration family.
- **`CancelSlotMigration`'s widened proposer set** (the target joins it).
- **`RecordSnapshotPosition`** and **`ReportMigrationIngest`** admission (the transitions'
  target-side effects are c31-07's; their admission conjunctions are here).

### Config parameters stamped at `Begin`

Per the design's config table, knobs enter replicated state **only** as `Begin`-stamped immutable
record fields. This cluster lands that rule and the parameters it governs:
`cluster-migration-parity-threshold-bytes` (1 MiB), `cluster-migration-max-handoff-attempts` (3),
`cluster-migration-stall-strikes` (3), `cluster-migration-preconfirm-observations` (30),
`cluster-migration-draining-observations` (3), `cluster-migration-observation-tick-ms` (1000).
`cluster-migration-barrier-max-bytes` (4 MiB) and `cluster-migration-backlog-max-bytes` (64 MiB)
are stamped here but consumed by c31-06 and c31-07 respectively;
`cluster-migration-require-target-replica-ack` (off) is stamped here and consumed by c31-07.

## What to build

### 1. Spec deltas (first)

1. The typed record declaration and the five SS rows' writer joins (holding OQ-2's row).
2. The six transitions, in phase order, each with its full payload declaration and admission
   conjunction in declared evaluation order.
3. The failure-mode rewrites, with FM-CLUSTER-032's retired arm and FM-CLUSTER-033's inverted
   headline handled as deliberate reversals — both contradict an earlier design revision and both
   need a note saying so, or a future reader will read the row as a mistake.
4. FM-CLUSTER-085's retirement plus its replacement row.
5. FM-CLUSTER-089's rewrite with **both** existing forcing tests re-pointed.
6. FM-CLUSTER-091's split: retire the `-TRYAGAIN` arm, add the observation-bound row.
7. The new liveness rows: the observation bound, the auto-`Complete`, the counted-replica set, the
   calibration obligation, the operator exit.
8. The config rows for the `Begin`-stamped parameters.

### 2. Forcing tests (second, observed failing)

In `frogdb-cluster`:

- `begin_creates_the_record_at_snapshotting_and_requires_source_ownership`
- `prepare_is_refused_below_parity`
- `prepare_and_begin_both_increment_the_generation`
- `confirm_seals_the_slot_and_sets_drained_pos`
- `complete_is_admitted_on_the_logical_handoff_seq_token`
- `complete_reads_the_slot_map_and_refuses_slot_already_assigned`
- `complete_requires_the_target_to_be_primary`
- `complete_requires_target_in_nodes`
- `abort_clears_drained_pos_and_attempt_id_and_increments_attempts`
- `cancel_has_no_repatriation_precondition`
- `cancel_may_be_proposed_by_the_target`
- `migrating_an_unassigned_slot_is_refused_and_names_assign_slots`
- `reissued_migrating_is_ok_and_resets_the_one_shot_attempts`
- `exactly_one_slot_migration_completed_event_on_success`
- `no_client_event_at_begin_and_none_on_failure`
- `no_admission_predicate_reads_node_local_state` (FM-CLUSTER-089's direct assertion, plus the two
  re-pointed tests)
- `a_dead_finalizer_cannot_wedge_a_slot` (the replacement for FM-CLUSTER-085)
- `a_held_write_is_never_answered_tryagain_during_drain_wait`
- `observation_bound_resets_only_on_narrowed_progress`
- `duplicate_observations_are_deduped_by_last_observation`
- `confirm_resets_the_observation_counter`
- `leader_auto_completes_a_drained_record`
- `begin_stamps_the_config_parameters_and_a_later_config_set_does_not_re_stamp`
- `counted_replica_set_empty_is_unset_false`
- `a_replica_set_change_clears_the_floor`
- `role_flip_cancels_every_migration_this_node_sources_atomically` (**[R3]**, joint with c31-04)

### 3. Implementation surface

- `frogdb-cluster/src/state.rs` — the typed record, `handoff_residue`, the observation counters.
- `frogdb-cluster/src/commands.rs` — the migration-arm region: rewrite the six transitions, add
  `RecordSnapshotPosition`, `ReportMigrationIngest`, `ObserveMigration`.
- `frogdb-cluster/src/invariants.rs` — the phase machine's reachability invariants and the
  determinism property.
- `frogdb-cluster/src/wire.rs`, `encoding_golden.rs`, `version_gate.rs` — the record and residue
  cross the wire.
- `frogdb-cluster/src/stats.rs` — the observation counters feed c31-08's surfaces.

## Acceptance criteria

- [ ] All six transitions and all twelve failure-mode rows landed; FM-CLUSTER-085 retired **with
      its forcing tests deleted or re-pointed in the same change**; FM-CLUSTER-091's `-TRYAGAIN`
      arm retired the same way.
- [ ] FM-CLUSTER-089's two existing forcing tests re-pointed, not deleted.
- [ ] FM-CLUSTER-032's retired arm and FM-CLUSTER-033's inverted headline each carry a note
      recording that they reverse an earlier design revision (V7-M4 reverses V4-m5).
- [ ] OQ-2 ruled before the "A handoff's deadlines" row is touched.
- [ ] `just lint-spec` green.
- [ ] Every forcing test observed failing before implementation, green after.
- [ ] `just mutants-diff frogdb-cluster` run and triaged. This cluster carries the largest
      admission-predicate surface in the campaign; expect a long triage and budget for it.
- [ ] `just lint` / `just lint-gates` green — the clock-read seam is directly load-bearing here
      (FM-CLUSTER-089).
- [ ] The **calibration obligation** discharged: the observation-bound defaults are justified
      against the tick cadence, not merely chosen.
- [ ] c31-01's deferred `handoff_residue` snapshot-carriage assertion (OQ-5) added here, closing
      that follow-up.

## Blocked by

- **c31-01** (wave 1) — `run_identity`, `proposer`, `RefusalClass`, the determinism principle.
- **c31-03** (wave 2) — `nodes[target].role == Primary` is meaningful only once SS-11 and the role
  writers are settled; the demotion-cancel arm fires from c31-03's transitions.

## Blocks

- c31-06 (the held-write disposition table is a total function of this cluster's exits)
- c31-07 (the reaper and the promotion path consume `handoff_residue`, written by `Complete`)

## Related trackers

Closes on landing: cluster-correctness issue 24 (residual real-clock dependence — FM-CLUSTER-089
asserts determinism directly) and the bound half of issue 17/18 (barrier liveness = the reconcile
orphan-abort plus the observation bound).
