# c31-04: Role authority — the staged flip, the pending record, and the refusal partition

Status: DRAFT — pending wave-0 approval
Wave: 2 (parallel with c31-03)
Size: L
Crates: `frogdb-cluster-runtime`, `frogdb-server`, `frogdb-cluster`

> Per [R1](../2026-08-22-work-item-rulings.md#r1--issue-31-campaign-staging-clustered-waves),
> each LOCKED row lands atomically with its forcing test and implementation.
>
> [R4](../2026-08-22-work-item-rulings.md#r4--isrefusalterminal-arm-4b-carry-the-class-in-the-payload)
> binds gate 3 (d) directly: **arm 4b is reachable** — the V27-M2 negative-control trace reaches it
> — and the delivery split between arms 4a and 4b is made on the **current stored operand only**,
> with the class taken from the refusal payload rather than recomputed. The 2026-08-19 "arm 4b
> deleted" ruling is superseded.
>
> [R5](../2026-08-22-work-item-rulings.md#r5--inv_no_record_outlives_its_registration-stale-never-admits)
> binds the record-clearing clause: the outliving state (a crash-durable node-local record whose
> registration cell is gone) is **lawful**; the invariant that must hold is
> **stale-never-admits** — no applied adoption or stamp ever fires from a record whose
> `staged_registration_seq` mismatches the live cell. Eventual clearing is a liveness property that
> lives with the Rust forcing tests (`staged_record_from_a_dead_registration_is_cleared_at_boot`
> and siblings), not with an invariant.
>
> [R3](../2026-08-22-work-item-rulings.md#r3--inv_no_hold_during_staged_flip-adoption-time-rule)
> binds the staged-flip fence row: coexistence of a latched drain-hold region and a *staged*
> (pending, unadopted) role flip is **lawful**. The safety boundary is the **applied write**, not
> the staging. The design doc gains a clarifying sentence at the staged-flip fence row during this
> wave — that sentence is this cluster's to write.

This cluster owns node-local role authority: how a node learns its own role is wrong, how a
replication-command-driven role change is staged durably and adopted exactly once, and how a
refusal is classified so the delivery side can tell a terminal refusal from a stale one. It is
almost entirely node-local, which is what makes it parallel-safe with c31-03's replicated-apply
sweep — the two clusters share `commands.rs` only where c31-04 reads.

## Owned rows

| Row | Verdict | Semantics | Design-doc citation |
|---|---|---|---|
| TR-CLUSTER-033 | Adopted and rewritten | The replicated→local `SelfRoleReconciler` becomes the declared role-authority convergence for **unexplained** disagreements, gaining the pending-record exclusion and the lineage guard. The persisting-across-ticks damping is subsumed by the resolved-stage selector | blast radius "Rewritten" (V12-C1/M2, V13-C1, V14-M1, V15-C2) |
| FM-CLUSTER-046 | Rewritten | The reconciler is **unconditional and load-bearing**; the feature gate is removed. `test_self_role_reconciler_absent_until_detection_is_enabled` must be **deleted or inverted in the same change** | blast radius "Rewritten" (V12-M2, revision-13 note) |
| FM-CLUSTER-101 | Read, unmoved | Read at the untrusted-state exits (demote-don't-remove). Record as confronted; do not amend | blast radius, revision-23 list |
| TR-CLUSTER-005, TR-CLUSTER-041, FM-CLUSTER-100, SS-1/2/3 | Read, unmoved | Revision 20 read all of these and moved none. Record as confronted | blast radius, revision-20 entry |

### Revisions 19-22: no LOCKED row changes, substantial mechanism

Three revisions of design work here produced **no** LOCKED-row verdict, because the whole mechanism
is node-local. That does not make it small — it makes it invisible to `lint-spec`, which is exactly
why the forcing tests must be node-local-durability tests *in the crate that owns the record*.

Revision 22's "no LOCKED row changes" verdict was **withdrawn in revision 23** for its
`DATA_DIR_LAYOUT_VERSION` half; that half is c31-02's. The mint-time floor and the corrected
untrusted-state exits stay here.

### New rows this cluster writes

1. **Gate 3, the replication-command gate** — the full gate with all its arms.
2. **The staged flip** — the protocol, its ordering, and its client-visible replies.
3. **The durable pending-transition record** `{kind, target_upstream, stage_id, adopted,
   staged_registration_seq}` and its fsync discipline. (Its durable *home* is c31-02's.)
4. **Level-triggered destructive adoption** with its five-operand binding.
5. **The stage-resolution precedence rule.**
6. **Gate 3 (d)'s six-class refusal partition**, total over
   `proposer | membership | ordering | fence | upstream-validity | shard-relationship`, with the
   **4a/4b arm split**. **[R4]** The class is read from the payload; the arm is selected on the
   current stored operand.
7. **The paired record-binding conjunct** (revision 20).
8. **The lineage guard**, bound to `promoted_from`, with TR-CLUSTER-042's removed-upstream disjunct.
9. **The record's single-writer rule.**
10. **The effect-keyed record-clearing clause. [R5]**
11. **The reply-token totality rule** — every gate-3 path produces exactly one reply token.
12. **The fail-closed recovery discipline** for a record read at boot.
13. **The stage-counter rules** and the `stage-counter-untrusted` state with its enumerated exits
    (revision 21's refinement: the boot rule becomes a **staging refusal**, arm 5 gets its reply).
14. **The replicated floor on the mint** and the mint-time floor (revision 22).
15. **The two permanent disciplines** stated in revision 21.
16. **The `cluster-staged-flip-reply-timeout` config row** (default 5 s).
17. **The §3 staged-flip fence row** with its **member-enumerated exempt set** — distinct from the
    slot-pause seal's exempt set, which is `CLUSTER` only and belongs to c31-06 (the V14-m4
    disambiguation). Both rows must cross-reference each other or a future reader will conflate
    them. **[R3]** supplies this row's clarifying sentence.
18. **The cluster-mode role-command surface declaration** — in cluster mode the spelling is
    `CLUSTER REPLICATE`, not `REPLICAOF`.
19. **The quiesced-arm unreachability statement.**
20. The **`CLUSTER INFO` node-local fail-closed fields** are *produced* here
    (`pending-stage`, `stage-counter-untrusted`); their reporting surface is c31-08's.

## What to build

### 1. Spec deltas (first)

1. TR-CLUSTER-033's rewrite and FM-CLUSTER-046's degating, **with the stale forcing test deleted
   or inverted in the same commit**. This is the campaign's clearest lint-spec trap: leaving
   `test_self_role_reconciler_absent_until_detection_is_enabled` alive pins the feature gate the
   rewrite removes.
2. The gate-3 family of new rows, in the order listed above — the gate, then the record, then the
   adoption, then the partition, then the guards, then the counter, then the config row.
3. The §3 staged-flip fence row, with its exempt set enumerated by member and a cross-reference to
   c31-06's slot-pause seal row.
4. `FM-CLUSTER-101`, `TR-CLUSTER-005`, `TR-CLUSTER-041`, `FM-CLUSTER-100`, `SS-1/2/3` recorded as
   read-and-confronted in the implementation notes.

### 2. Forcing tests (second, observed failing)

Node-local durability tests belong in the crate that owns the record — `frogdb-cluster-runtime`
for the gate and record, `frogdb-server` only for the client-reply surface.

- `staged_record_from_a_dead_registration_is_cleared_at_boot` (named in R5)
- `stale_record_never_admits_an_adoption_or_a_stamp` (the R5 invariant, as a property)
- `adoption_is_level_triggered_and_fires_exactly_once`
- `adoption_binds_all_five_operands`
- `refusal_partition_is_total_over_the_six_classes`
- `delayed_ordering_refusal_with_absent_stored_identity_takes_arm_4b` (the V27-M2 fixture; **[R4]**
  — this test pins arm 4b's reachability and must not be written as a negative)
- `arm_4a_and_4b_split_on_the_current_stored_operand_only`
- `refusal_class_comes_from_the_payload_not_from_a_recompute`
- `lineage_guard_rejects_an_upstream_that_is_not_this_nodes_promoted_from`
- `lineage_guard_admits_the_removed_upstream_disjunct`
- `record_has_exactly_one_writer`
- `every_gate_three_path_produces_exactly_one_reply_token`
- `a_record_read_at_boot_that_fails_validation_fails_closed`
- `stage_counter_untrusted_refuses_staging_and_exits_only_as_declared`
- `mint_respects_the_replicated_floor`
- `staged_flip_reply_timeout_is_honoured`
- `staged_flip_fence_exempt_set_is_enumerated_by_member`
- `a_latched_hold_coexists_with_a_staged_unadopted_flip` (**[R3]** — the *lawful* case)
- `an_applied_role_flip_leaves_no_sourced_open_migration_and_no_held_slot` (**[R3]** — the
  adoption-time invariant; the migration-cancel half is forced jointly with c31-05, the
  hold-release half jointly with c31-06)
- `self_role_reconciler_is_unconditional` (replacing the degated test)
- `reconciler_excludes_a_node_with_a_pending_record`

### 3. Implementation surface

- `frogdb-cluster-runtime/src/flags.rs` — the FM-CLUSTER-046 feature gate is **removed**, not
  defaulted on.
- New module in `frogdb-cluster-runtime` for the pending-transition record and the stage counter,
  reading and writing through c31-02's durable stores.
- `frogdb-server` command dispatch — gate 3 sits in front of the cluster-mode role commands;
  the reply-token totality rule is enforced here.
- `frogdb-cluster/src/state.rs` — read-only access to `admitted_stage`, `registration_seq`,
  `promoted_from` (declared by c31-01). This cluster does **not** write replicated state; the
  adoption's applied write is proposed and lands through c31-03's arms.

## Acceptance criteria

- [ ] TR-CLUSTER-033 and FM-CLUSTER-046 amended, with the degated test deleted or inverted **in the
      same change**.
- [ ] All twenty new-row items above landed in `specs/cluster.md`, with forcing tests named at the
      mechanism and tags carried into test bodies.
- [ ] The arm-4b fixture is present and passing as a **positive** reachability test. **[R4]**
- [ ] The R5 stale-never-admits property is expressed as an invariant with kill-power under
      guard-deleting mutations — verify this by checking that `mutants-diff` kills a mutant that
      removes the `staged_registration_seq` comparison.
- [ ] The R3 clarifying sentence written at the staged-flip fence row, and both the lawful
      coexistence test and the adoption-time invariant test present.
- [ ] The staged-flip fence row and c31-06's slot-pause seal row cross-reference each other and
      state that their exempt sets differ (V14-m4).
- [ ] `just lint-spec` green.
- [ ] Every forcing test observed failing before implementation, green after.
- [ ] `just mutants-diff frogdb-cluster-runtime` and `just mutants-diff frogdb-cluster` run and
      triaged.
- [ ] `just lint` / `just lint-gates` green — the redirect-reply seam covers gate 3's replies.
- [ ] OQ-6's resolution recorded: this cluster owns the **arm mapping**; c31-01 owns the **declared
      evaluation order**. They must agree exactly.

## Blocked by

- **c31-01** (wave 1) — `RefusalClass`, `admitted_stage`, `registration_seq`, `promoted_from`.
- **c31-02** (wave 1) — the durable homes for the pending record and `stage_counter_state`.
- Cluster-correctness issue 43 — **hard requirement** before this wave; R4's payload-carried class
  and R5's stale-never-admits must be settled in the model before they are implemented here.

## Blocks

- Nothing structurally, but c31-06's hold-release half of the R3 adoption-time invariant is forced
  jointly with this cluster's test, so the two must agree on the assertion's wording.
