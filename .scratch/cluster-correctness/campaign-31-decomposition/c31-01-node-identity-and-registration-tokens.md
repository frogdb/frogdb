# c31-01: Node identity, registration tokens, and the run-identity protocol

Status: DRAFT — pending wave-0 approval
Wave: 1 (parallel with c31-02)
Size: L
Crates: `frogdb-cluster` (primary); `frogdb-replication` (read-only citation)

> Per [R1](../2026-08-22-work-item-rulings.md#r1--issue-31-campaign-staging-clustered-waves),
> the issue-31 campaign runs as conflict-clustered sequential waves, one implementer per
> cluster, each LOCKED row landing atomically with its forcing test and implementation.
> [R4](../2026-08-22-work-item-rulings.md#r4--isrefusalterminal-arm-4b-carry-the-class-in-the-payload)
> binds this cluster directly: the refusal class is **minted with the verdict and carried in the
> refusal payload**, never recomputed at delivery.

This is the campaign's foundation cluster. It lands the whole replicated data model that every
later wave writes into — the identity triple, the four companion fields, the parent sequence, and
the registration token — plus the two transitions that own them and the snapshot-carriage rule
that keeps them across an install. Nothing downstream can express its invariants until these cells
exist, which is why this cluster is wave 1 and why it deliberately declares fields whose *writers*
belong to c31-03 and whose *lifecycle* belongs to c31-05/c31-07.

## Owned rows

| Row | Verdict | Semantics | Design-doc citation |
|---|---|---|---|
| SS-1 (membership) | Amended | The `AddNode` **fresh** arm becomes the sole writer of `ClusterStateInner.registration_seq_gen`; `ResetCluster`'s membership reduction is a declared non-writer and must not rewind the generation | blast radius, revision-18 additions (V18-C1) |
| TR-CLUSTER-001 | Rewritten | `AddNode`'s upsert is **field-wise**: it preserves `run_identity`; the fresh arm mints `registration_seq` from `registration_seq_gen` and increments the generation | blast radius "Rewritten" (V7-M3, V18-C1) |
| TR-CLUSTER-002 | Rewritten | The upsert path may touch address/port/config only; `run_identity`, `role` and `registration_seq` are written solely by their declared writers | blast radius "Rewritten" (V7-M3, V18-C1) |
| TR-CLUSTER-027 | Unchanged, stated — extended | Live `CONFIG SET` re-registration routes through the same field-wise upsert and must additionally preserve `synced`, alongside `run_identity` / `promoted_from` / `admitted_stage` / `registration_seq` | blast radius, revision-15 additions (V15-M2, V18-C1) |
| TR-CLUSTER-041 | Unchanged, cited | Follower proposals answer `Proposed::Forwarded` and carry no `ClusterResponse` — this is *why* the refusal class cannot ride the reply channel and must be a committed apply outcome | blast radius, revision-15 additions |
| FM-CLUSTER-047 | Extended | The committed rejection records `refused(class)`, where `class` is the first failing conjunct in the row's **declared evaluation order** — a pure function of payload plus pre-apply state, so FM-CLUSTER-089's determinism claim holds | blast radius "Rewritten" (V14-M3); **[R4]** |
| FM-CLUSTER-100 | Unchanged, stated — extended | Snapshot install carries `NodeInfo.run_identity`, `NodeInfo.registration_seq`, `ClusterStateInner.registration_seq_gen` and `handoff_residue` | blast radius "Unchanged, stated"; see OQ-5 in the sequencing plan |
| FM-REPLICATION-021 | Cited, not contradicted | Revision 17's fresh-`replid` claim is deleted; the design asks LOCKED replication for nothing here | blast radius, revision-18 additions (V18-M3) |
| FM-REPLICATION-022 | Noted, narrowed | The run-identity report is kind-stamped (`Boot` / `Demotion`) in the committed payload; the row's `REPLICAOF` spelling is standalone-only — in cluster mode the spelling is `CLUSTER REPLICATE` | blast radius "Rewritten" (V7-C1, V14-C1) |

### New rows this cluster writes

All of these are brand-new `specs/cluster.md` rows (state-space cells, transitions, or failure
modes) enumerated in the design's "New rows" bullet:

1. **`NodeInfo.run_identity`** and the **`ReportRunIdentity`** transition: the three proposing
   moments, the `(incarnation, identity_seq)` ordering, the boot ordering rule, and the
   source-side-only cancellation clause.
2. The **incarnation durability contract** — what the incarnation must survive and who fsyncs it.
   (The durable *home* is c31-02's; the contract statement is here.)
3. The **run-identity triple** with three-component equality, replacing any two-component reading.
4. The **split ordering conjunct**: `≥` admits, strict `>` gates the identity-field write, equality
   is a topology-only re-proposal — plus the one-in-flight damping rule.
5. The **`RefusalClass` six-value partition as a committed apply outcome**
   (`proposer | membership | ordering | fence | upstream-validity | shard-relationship`) and the
   declared evaluation order that determines which class a refusal carries. **[R4]**
   *Consumption* of the class — gate 3 (d)'s six-arm total partition and the 4a/4b delivery split —
   is c31-04's; see OQ-6.
6. **`AttestReplicaSynced`** and the **`NodeInfo.synced`** field, including the fence fields
   (`observed_parent_seq`, `observed_registration_seq`) and the `(parent_seq, registration_seq)`
   operand pair. Revision 17's `observed_run` is **superseded** by revision 18 and must not be
   built.
7. **`NodeInfo.promoted_from`**, **`NodeInfo.admitted_stage`**, **`NodeInfo.parent_seq`** field
   declarations under the companion-field rule. The rule's *enforcement over every parent-pointer
   writer* is c31-03's; the declarations and their invariants are here.
8. **`NodeInfo.registration_seq`** and **`ClusterStateInner.registration_seq_gen`**, explicitly
   outside the companion-field and parenting enumerations, with a writer set of exactly one.
9. The **`run_identity` lifecycle** across `RemoveNode` / re-`MEET`, `ResetCluster` and snapshot
   install.
10. The **§0 absent-operand rule** with its named exceptions, and the value-read versus
    absence-test distinction. The absent-operand exception narrows to `kind = Boot` for
    `ReportRunIdentity` (V18-M1).
11. The **`proposer` payload field** on every transition, with the stamping rule and the
    payload-versus-`self_node_id` determinism argument.
12. The **global apply-determinism principle** with its node-local-reaction carve-out: no
    replicated admission predicate reads node-local state.

### Citation fixes this cluster raises

These are pre-existing `specs/cluster.md` citation defects the design doc noticed. They are
**raised** here (recorded in the row's notes and reported to spec-gaps issue 29, which owns the
row-edit sweep); this cluster does not perform the sweep.

- SS-1 `Writer(s)` span should be `commands.rs:833-854`, not the currently cited span.
- SS-4 `Writer(s)` should name both epoch writes (`:841`, `:843`) and the re-key at `:842`.
- TR-CLUSTER-001 / -002 / -027 should state the fresh/upsert arm split explicitly rather than
  describing `AddNode` as one behaviour.

## What to build

### 1. Spec deltas (first)

Amend `specs/cluster.md` in this order, because later rows read earlier declarations:

1. SS-1: add `registration_seq_gen` to the state space with `AddNode` (fresh arm) as its sole
   writer and an explicit non-writer note for `ResetCluster`.
2. New state-space cells for `run_identity`, `promoted_from`, `synced`, `admitted_stage`,
   `parent_seq`, `registration_seq`, each with its `Writer(s)` cell. For the four companion fields,
   the `Writer(s)` cell forward-references the role writers c31-03 will enumerate; write the cell
   with the writer list the design doc gives and let c31-03 verify it against the code sweep.
3. TR-CLUSTER-001 / -002 / -027: the field-wise upsert, the fresh-arm mint, the preservation list.
4. New transitions `ReportRunIdentity` and `AttestReplicaSynced` with full payload declarations,
   admission conjunctions in declared evaluation order, and apply effects.
5. New failure-mode rows for the ordering rule, the damping rule, the absent-operand rule, the
   `proposer` stamping rule, the apply-determinism principle, and the `RefusalClass` partition.
6. FM-CLUSTER-047's extension and FM-CLUSTER-100's carriage extension.
7. A citation note on TR-CLUSTER-041 recording why the class is a committed outcome.

### 2. Forcing tests (second, observed failing)

In `frogdb-cluster` — this is a mutation-score requirement, not a preference; a row forced only
from `frogdb-server` integration tests contributes nothing to `frogdb-cluster`'s score.

- `add_node_upsert_preserves_run_identity_and_registration_seq`
- `add_node_fresh_arm_mints_registration_seq_and_increments_the_generation`
- `reset_cluster_does_not_rewind_the_registration_generation`
- `config_set_reregistration_preserves_synced`
- `report_run_identity_equal_ordering_is_a_topology_only_reproposal`
- `report_run_identity_strictly_greater_writes_the_identity_field`
- `report_run_identity_lower_is_refused_with_class_ordering`
- `boot_kind_is_the_only_absent_operand_exception`
- `attest_replica_synced_is_fenced_on_parent_seq_and_registration_seq`
- `attest_replica_synced_from_a_stale_registration_is_refused`
- `refusal_class_is_the_first_failing_conjunct_in_declared_order`
- `refusal_class_is_a_pure_function_of_payload_and_pre_apply_state`
- `snapshot_install_carries_run_identity_and_registration_state`
- `proposer_is_read_from_the_payload_not_from_self_node_id`
- `demotion_arm_cancels_only_migrations_this_node_sources`

Each test must be observed failing before the implementation lands. The
`snapshot_install_carries_...` test's `handoff_residue` half cannot be written until c31-05
declares the residue type; per OQ-5 the recommendation is to land the row now and add that
assertion as a wave-3 follow-up, tracked on this issue rather than dropped.

### 3. Implementation surface

- `frogdb-cluster/src/state.rs` — `NodeInfo` gains six fields; `ClusterStateInner` gains
  `registration_seq_gen`.
- `frogdb-cluster/src/types.rs` — `RunIdentity` triple, `RunIdentityKind` (`Boot` / `Demotion`),
  `RefusalClass`.
- `frogdb-cluster/src/commands.rs` — `ClusterCommand` gains `ReportRunIdentity` and
  `AttestReplicaSynced`; the `AddNode` arm (`:132`, the `existed` split) becomes an explicit
  fresh/upsert branch with a field-wise merge.
- `frogdb-cluster/src/invariants.rs` — the companion-field declaration invariants and the
  at-most-one-in-flight damping check.
- `frogdb-cluster/src/wire.rs`, `encoding_golden.rs`, `version_gate.rs` — the new fields cross the
  wire and the snapshot; the golden encoding test grows and the version gate needs an entry.

**Deliberate scope note:** land the whole struct and enum churn here. Later clusters then add apply
*arms* rather than *fields*, which is what keeps the wave-2 and wave-4 pairs from colliding in
`commands.rs`.

## Acceptance criteria

- [ ] Every owned row amended in `specs/cluster.md`, and every new row added, with the forcing
      tests named at the mechanism and the `FM-`/`TR-` tags carried into the test bodies (cluster
      rows have no `Forced by` column).
- [ ] `just lint-spec` green — no row without tests, no tagged test without a row.
- [ ] Every forcing test above observed failing before implementation, and green after.
- [ ] `just mutants-diff frogdb-cluster` run and triaged; any surviving mutant either killed by a
      new test or documented at the code with why it is unobservable.
- [ ] `just lint` and `just lint-gates` green — in particular the clock-read seam (the
      apply-determinism principle asserts no node-local reads in admission).
- [ ] The five citation fixes reported to spec-gaps issue 29 with their corrected spans.
- [ ] The `handoff_residue` half of FM-CLUSTER-100's carriage test recorded as a wave-3 follow-up
      on this issue (see OQ-5).

## Blocked by

- Cluster-correctness issue 43 (Quint model semantics fixes, R3/R4/R5) — **preferred** before this
  cluster starts, so FM-CLUSTER-047's extension is written against a model that already mints the
  class at the verdict. **Hard requirement** before wave 2.

## Blocks

- c31-03 (every role writer writes this cluster's companion fields)
- c31-04 (gate 3 consumes `RefusalClass`, `admitted_stage`, `registration_seq`)
- c31-05, c31-07 (admission conjuncts read `run_identity`; the demotion arm cancels their records)
