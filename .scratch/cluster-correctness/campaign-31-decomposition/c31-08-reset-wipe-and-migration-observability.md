# c31-08: Reset, wipe, and migration observability

Status: DRAFT — pending wave-0 approval
Wave: 5 (alone — campaign exit)
Size: M
Crates: `frogdb-cluster`, `frogdb-recovery`, `frogdb-server`

> Per [R1](../2026-08-22-work-item-rulings.md#r1--issue-31-campaign-staging-clustered-waves),
> each LOCKED row lands atomically with its forcing test and implementation.

This cluster closes the campaign. It lands the three accumulated deltas on `TR-CLUSTER-035` in one
change, rewrites the join and wipe gates, and surfaces everything the previous seven clusters built
to operators. It runs last because two of `TR-CLUSTER-035`'s deltas require the shadow store and
the barrier to exist, and because `CLUSTER MIGRATIONS` renders both open records (wave 3) and
residue entries (wave 4).

**It carries the campaign's one blocking external prerequisite.** See "Blocked by".

## Owned rows

| Row | Verdict | Semantics | Design-doc citation |
|---|---|---|---|
| TR-CLUSTER-035 | Rewritten — **three deltas, landed together** | (a) the shadow-discard trigger (§5); (b) the held-write obligation paid to clients as `-CLUSTERDOWN` (V16-M1); (c) the HARD path's Raft-store discard obligation — mark, then wipe on restart — attached to the **Precondition** (V24-M4, declared persistence amendment **(3)**) | blast radius "Rewritten"; revision-16 additions; revision-23 declared amendment 3 |
| TR-CLUSTER-005 | Rewritten | Issue 25's empty-Raft-state precondition composes with the join-empty admission gate into **one** stated precondition; the wipe path is **two** paths; the row carries a blocking undischarged dependency | blast radius "Rewritten" (V9-M5, V23-M2, V25-M1) |
| FM-CLUSTER-061 | Unchanged, stated | Admin-gating class semantics | blast radius "Unchanged, stated" |
| FM-CLUSTER-062 | Unchanged, stated | Admin-gating class semantics | blast radius "Unchanged, stated" |
| FM-CLUSTER-063 | Unchanged, stated | Admin-gating class semantics | blast radius "Unchanged, stated" |
| FM-CLUSTER-064 | Rewritten | The admin-gated fail-closed split table gains a **`CLUSTER MIGRATIONS` row** | blast radius "Rewritten" |
| FM-CLUSTER-006 | Read, unmoved — citation raised | Confronted at gate 1 and **unbreached**. Its `Invariant` cell carries a stale citation (`commands.rs:478-518`, should be `816-862`) — a **pre-existing LOCKED-spec defect, not this design's to fix**; raise it to spec-gaps issue 29 | blast radius, revision-23 list and citation-fix bullet |
| FM-CLUSTER-101 | *(read by c31-04)* | Cross-referenced: demote-don't-remove at the untrusted exits | — |

### New rows this cluster writes

1. The **join-empty admission gate** and `ResetCluster`'s **non-empty refusal** (V8-C1).
2. The **SOFT/HARD `ResetCluster` identity-cell distinction** — what each path does to
   `run_identity`.
3. **`ResetCluster`'s re-mint is stamped `kind = Boot`.**
4. **`CLUSTER MIGRATIONS`** (§9): its admin gating and its reply shape, rendering open records
   **and** residue entries.
5. The **`CLUSTER INFO` node-local fail-closed fields**: `pending-stage`,
   `stage-counter-untrusted` (both produced by c31-04) and `raft_discard_pending` (produced by
   c31-02). Reporting these is additive per revision 21.
6. The **§9 observability surface**: metrics, the operator event log (a stream **distinct** from
   the client `ClusterEvent` contract that FM-CLUSTER-034 defines), `frogctl` output, and the
   Grafana panels.

Remember that the Grafana dashboards and several YAML/JSON artifacts are **generated** — make the
change in the generator, not the generated file.

## What to build

### 1. Spec deltas (first)

1. `TR-CLUSTER-035` with all three deltas in one amendment. Delta (c) attaches to the
   **Precondition** cell, not the Effect — V24-M4 is explicit about this, and getting it wrong
   makes the discard obligation look optional.
2. `TR-CLUSTER-005`: the composed precondition and the two wipe paths.
3. `FM-CLUSTER-064`'s new table row; `FM-CLUSTER-061/062/063` restated as unchanged with a
   cross-reference.
4. The join-empty gate, the SOFT/HARD identity distinction, the `kind = Boot` re-mint.
5. The §9 rows: `CLUSTER MIGRATIONS`, the `CLUSTER INFO` fields, the observability surface.
6. Record `FM-CLUSTER-006` as read-and-unbreached, with the stale citation raised to spec-gaps
   issue 29 and explicitly **not** fixed here.

### 2. Forcing tests (second, observed failing)

- `a_wiped_node_awaits_meet_instead_of_solo_bootstrapping` — **the blocking prerequisite's forcing
  test** (see below). The discard-mark mechanism may not ship ahead of it.
- `cluster_reset_pays_the_held_writes_a_real_reply` (named in the design doc, V16-M1) — the reply
  is `-CLUSTERDOWN`
- `hard_reset_marks_the_raft_store_for_discard_before_it_returns`
- `a_marked_raft_store_is_wiped_on_the_next_restart`
- `a_crash_between_the_mark_and_the_commit_does_not_produce_a_second_raft_group`
- `reset_discards_the_shadow_store`
- `soft_and_hard_reset_differ_in_the_identity_cell_as_declared`
- `reset_remints_with_kind_boot`
- `reset_cluster_is_refused_on_a_non_empty_cluster`
- `join_is_refused_unless_the_joiner_is_empty`
- `cluster_migrations_is_admin_gated_and_fails_closed`
- `cluster_migrations_renders_open_records_and_residue_entries`
- `cluster_info_reports_pending_stage_and_stage_counter_untrusted_and_raft_discard_pending`
- `the_operator_event_log_is_distinct_from_the_client_cluster_event_stream`

### 3. Implementation surface

- `frogdb-cluster/src/commands.rs` — the `ResetCluster` arm (`:816-862`, with `:826-829`, `:830`,
  `:833-854`, `:837`, `:841-844` as the sub-regions the design cites) and the join gate.
- `frogdb-recovery/src/cluster.rs` (`:23-25`) and `frogdb-recovery/src/lib.rs` (`:188-190`) — the
  boot-time read of `frogdb_raft_discard` (created by c31-02) before the Raft store opens.
- `frogdb-server` — `CLUSTER MIGRATIONS`, the `CLUSTER INFO` fields, `frogctl` output.
- `frogdb-cluster/src/stats.rs` and the metrics generator — the §9 metrics.
- The Grafana generator — the migration panels.

## Acceptance criteria

- [ ] `TR-CLUSTER-035`'s three deltas landed in one amendment, with delta (c) on the Precondition.
- [ ] Declared persistence amendment **(3)** discharged, closing c31-02's deferred item so the
      revision-23 numbered list reads complete end to end.
- [ ] **Issue 25's forcing test present and green before the discard mark ships.** This is a
      safety gate, not a nicety: the mark-then-wipe-on-restart mechanism plus solo-bootstrap is a
      split-brain source if a node crashes between the mark and the commit.
- [ ] `just lint-spec` green.
- [ ] Every forcing test observed failing before implementation, green after.
- [ ] `just mutants-diff frogdb-cluster` and `just mutants-diff frogdb-recovery` run and triaged.
- [ ] `just lint` / `just lint-gates` green — the **metrics-emission seam** covers the §9 surface.
- [ ] Generated artifacts (Grafana, metrics docs) regenerated from their generators, not hand-edited.
- [ ] `FM-CLUSTER-006`'s stale citation raised to spec-gaps issue 29 and **not** fixed here.
- [ ] **Campaign exit gates**: `just mutants frogdb-cluster` + `just mutants-gate frogdb-cluster
      0.80`, and the same pair for `frogdb-cluster-runtime`, run once at this wave's boundary.
- [ ] Cluster-correctness issue 31's tracker banner updated to record the campaign as complete,
      with the eight cluster commits cited.

## Blocked by

- **Cluster-correctness issue 25 (solo-bootstrap usurper) — BLOCKING.** An empty Raft store
  solo-bootstraps today (`cluster_init.rs:383-391`, `:442-460`), re-populating what a wipe
  removed. For an operator wipe that is an availability nuisance; for a crash between the discard
  mark and the commit it is a **split-brain source** — the node boots, wipes its own Raft store,
  and forms a second Raft group while the survivors still list it in theirs. The sequencing plan
  assumes this cluster **absorbs** issue 25; if the design owner would rather it land
  independently, it must land first.
- **c31-02** (wave 1) — `frogdb_raft_discard` must exist.
- **c31-06** (wave 4) — the held-write disposition table must exist before `ResetCluster` can pay
  into it.
- **c31-07** (wave 4) — the shadow store must exist before reset can discard it; `CLUSTER
  MIGRATIONS` renders residue entries.
- **c31-05** (wave 3) — `CLUSTER MIGRATIONS` renders open records.

## Blocks

Nothing. This is the campaign's last cluster.
