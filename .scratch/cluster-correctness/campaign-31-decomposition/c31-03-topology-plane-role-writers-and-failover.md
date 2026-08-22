# c31-03: Topology plane — role writers, companion fields, and failover

Status: DRAFT — pending wave-0 approval
Wave: 2 (parallel with c31-04)
Size: L
Crates: `frogdb-cluster`

> Per [R1](../2026-08-22-work-item-rulings.md#r1--issue-31-campaign-staging-clustered-waves),
> each LOCKED row lands atomically with its forcing test and implementation.
> [R11](../2026-08-22-work-item-rulings.md#r11--q6-promotion-retry-boundedness-folded-into-campaign-brief)
> applies **secondarily** here: if this cluster's failover work touches promotion retry (the
> missed-failover re-drive of cluster-correctness issue 18), the design owner adds the
> boundedness bound to the design doc during this wave and the Quint model row follows. The
> decision is the design owner's, taken during the wave, not by the implementer.

This cluster sweeps every writer of `role` and `primary_id` in the replicated state machine and
makes them all obey the companion-field rule that c31-01 declared: no parent-pointer write without
its four companions and a `parent_seq` increment. It also lands the residue-marking half of node
departure and the `RetargetSlotResidue` transition. It is the cluster with the largest number of
touched transitions but the shallowest per-transition delta — most of the work is making one rule
total over a writer set.

## Owned rows

| Row | Verdict | Semantics | Design-doc citation |
|---|---|---|---|
| SS-2 (`role`) | Amended | The writer enumeration is restated as the LOCKED cell verbatim with every writer marked; `ReportRunIdentity`'s Demotion arm is added. The cell's omission of `AddNode` is **flagged**, not silently chosen | blast radius, revision-15 additions (V15-C1) |
| SS-3 (`primary_id`) | Amended | All five LOCKED writers amended, including `Failover`'s three pointer writes and `RemoveNode`'s detach; the Demotion arm added; the Promotion arm removed. Every `primary_id` write also writes the three companions and increments `parent_seq` | blast radius, revisions 15-16 (V15-C1, V16-C1) |
| SS-11 | Amended | No slot is assigned to a node whose role is `Replica`; every role→Replica writer re-homes in the same apply, and every slot writer requires the assignee to be `Primary` | blast radius "Rewritten" (V8-C6) |
| TR-CLUSTER-003 | Rewritten | Prune **marks** residue (`source_gone` / `target_gone` plus unassign) and never removes; it cancels the departed node's open migrations; the detach writes the companion fields; the row gains a held-write disposition row | blast radius "Rewritten" (V7-m3, V15-C1, V16-M1) |
| TR-CLUSTER-004 | Rewritten | `SetRole` re-targets residue and re-homes owned slots in the same apply, carries the per-object epoch fence, and takes its destination from the **validated successor** — never a post-write `shard_primary` walk. Writes `synced = false`, `promoted_from = None`, `admitted_stage = None` | blast radius "Rewritten" (V8-M5/C2, V9-C1, V14-C2) |
| TR-CLUSTER-017 | See OQ-1 | Provisionally: **unchanged, stated** per V15-M3 — the planned-failover barrier/drain/offset-parity wait is the attestation trigger, and the flow orders the attestation's commit *before* the `Failover` proposal. No exemption is added: one belt, uniformly | blast radius "Rewritten" list **and** revision-15 additions (V15-M3) |
| TR-CLUSTER-018 | Rewritten + amended | Candidacy belt gains the `synced` conjunct; the promotion and demotion writes carry `promoted_from` / `synced` / `admitted_stage`; each sibling's re-parent apply writes `synced = false`, `admitted_stage = None`, `promoted_from = None` | blast radius "Rewritten" and revision-15 additions (V14-C2/C3, V15-C1) |
| TR-CLUSTER-019 | Rewritten | Automatic failover proposal under the §4 endpoint rules | blast radius "Rewritten" |
| TR-CLUSTER-020 | Unchanged, cited | The replica-issued `CLUSTER FAILOVER` is the declared operator escape for a shard whose replicas are all unsynced | blast radius, revision-15 additions |
| TR-CLUSTER-021 | Amended | Candidacy gains `nodes[candidate].synced == true`. A shard whose only replicas are unsynced gets no automatic failover — a **stated availability loss** with TR-CLUSTER-020 as the escape | blast radius "Rewritten" (V14-C3) |
| TR-CLUSTER-042 | Rewritten | Outright-removal semantics own the residue marking; the promotion write stamps `promoted_from` / `synced` / `admitted_stage`; sibling re-parent clears the companions; the row is cited by c31-04's lineage guard as its removed-upstream disjunct | blast radius "Rewritten" (V8-M5, V13-M1, V14-C2/C3, V15-C1) |
| FM-CLUSTER-002 | Unchanged, stated | `role == Replica ∧ primary_id == None` is a **reachable, declared** state (the detached replica). `remove_node_prunes_migrations_and_detaches_replicas` stays valid and gains two assertions | blast radius, revision-15 additions |
| FM-CLUSTER-036 | Rewritten | A force failover cancels the migrations of the node it removes; residue re-target rides the role→Replica transition | blast radius "Rewritten" |

### New rows this cluster writes

- The **demotion re-target on both sides** — a demotion re-targets residue where the node was
  source and where it was target.
- The **`RetargetSlotResidue`** transition (V8-C4). Note that V9-M1's target arm was **removed** by
  V10-C4; build the source arm only.
- The **validated-successor rule**: the successor is chosen at admission time with a
  shard-relationship conjunct, and the destination is never re-derived by a post-write
  `shard_primary` walk.
- The **§1 residue liveness obligation** — residue must not accumulate without a lawful remover.
- The **`shard_primary(n)` definition** with its None-fails-closed rule.
- The **`in_shard_parent(u, n)` definition** as a one-hop, pre-apply pointer read.

### Citation fix this cluster raises

SS-2's `Writer(s)` cell omits `AddNode`. The writer-join **records the disagreement** between the
LOCKED cell and the code rather than choosing a side; report it to spec-gaps issue 29.

## What to build

### 1. Spec deltas (first)

1. SS-2 and SS-3: restate the writer enumerations with every writer marked, add the Demotion arm,
   remove SS-3's Promotion arm, and add the companion-write plus `parent_seq`-increment obligation
   to the Invariant cell. SS-3 also carries the revision-16 note about `parent_seq` (the field
   itself is declared by c31-01; SS-3 gains the note, not a new writer).
2. SS-11: the Replica-holds-no-slot invariant, stated as a two-sided obligation (role writers
   re-home; slot writers require `Primary`).
3. TR-CLUSTER-003 / -004 / -042: the residue marking, the companion writes, the validated
   successor, the epoch fence.
4. TR-CLUSTER-018 / -019 / -021 and the TR-CLUSTER-017 treatment settled by OQ-1.
5. FM-CLUSTER-002's clarification and FM-CLUSTER-036's rewrite.
6. New rows for `RetargetSlotResidue`, the validated-successor rule, the residue liveness
   obligation, `shard_primary`, and `in_shard_parent`.

### 2. Forcing tests (second, observed failing)

In `frogdb-cluster`:

- `every_primary_id_writer_writes_the_companion_fields`
- `every_primary_id_writer_increments_parent_seq`
- `failover_sibling_reparent_clears_companion_stamps` (named in the design doc)
- `promotion_stamps_promoted_from_and_clears_synced`
- `set_role_rehomes_owned_slots_in_the_same_apply`
- `set_role_destination_is_the_validated_successor_not_a_post_write_walk`
- `no_slot_is_assigned_to_a_replica` (an invariant test over the whole transition set)
- `remove_node_prunes_migrations_and_detaches_replicas` — **extend** with the two new assertions;
  do not replace
- `remove_node_marks_residue_source_gone_and_target_gone`
- `detached_replica_is_a_reachable_declared_state`
- `unsynced_replica_is_not_an_automatic_failover_candidate`
- `all_unsynced_shard_has_no_automatic_failover_and_the_operator_escape_works`
- `force_failover_cancels_the_removed_nodes_migrations`
- `retarget_slot_residue_has_no_target_arm`
- `shard_primary_none_fails_closed`

### 3. Implementation surface

- `frogdb-cluster/src/commands.rs` — the `SetRole`, `Failover`, `RemoveNode` and
  `reparent_children` regions (`:231`, `:233`, `:459`). Add `RetargetSlotResidue`. The
  `ClusterCommand` enum and `NodeInfo` already carry everything needed, because c31-01 landed them
  in wave 1; this cluster adds arms, not fields.
- `frogdb-cluster/src/invariants.rs` — SS-11 as a checked invariant, plus a
  `companion_fields_written_by_every_parent_writer` sweep test that walks the transition set rather
  than enumerating writers by hand. That sweep is the single highest-value artifact of this
  cluster: it is what makes the companion rule *total* rather than a list someone maintains.
- `frogdb-cluster/src/state.rs` — `shard_primary` / `in_shard_parent` helpers with their
  fail-closed semantics.

## Acceptance criteria

- [ ] All owned rows amended; all six new rows added; forcing tests named at the mechanism and the
      `FM-`/`TR-` tags carried into the test bodies.
- [ ] `just lint-spec` green.
- [ ] The companion-field sweep test is written as a walk over the transition set, so a *future*
      transition that writes `primary_id` without companions fails it.
- [ ] Every forcing test observed failing before implementation, green after.
- [ ] `just mutants-diff frogdb-cluster` run and triaged.
- [ ] `just lint` / `just lint-gates` green.
- [ ] OQ-1's resolution recorded on the issue before the row lands; the row must not be amended
      twice.
- [ ] SS-2's `AddNode` omission reported to spec-gaps issue 29 as a recorded disagreement.
- [ ] If the wave touches promotion retry, R11's boundedness note is raised with the design owner
      and the outcome recorded here.

## Blocked by

- **c31-01** (wave 1) — every companion field, `parent_seq`, and the Demotion arm of
  `ReportRunIdentity` must exist.
- Cluster-correctness issue 43 — **hard requirement** before this wave.

## Blocks

- c31-05 (admission reads `nodes[target].role == Primary`; the demotion arm cancels records)
- c31-07 (residue marking is the reaper's input)

## Related trackers

Closes or supersedes, on landing: cluster-correctness issues 19 and 20 (forced failover inherits
nothing / evicts the old primary — the `promoted_from`/`synced` stamps plus demote-don't-remove),
issue 28 (`CLUSTER FAILOVER` refused on a primary — TR-CLUSTER-020 is the declared escape), and the
retry half of issue 18.
