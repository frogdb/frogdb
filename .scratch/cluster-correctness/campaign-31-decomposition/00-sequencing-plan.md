# Issue-31 campaign — wave-0 decomposition and sequencing plan

Status: DRAFT — pending human approval. No implementation wave may start before approval.

Author: wave-0 decomposition agent, 2026-08-22.

Inputs, in authority order:

1. [`2026-08-22-work-item-rulings.md`](../2026-08-22-work-item-rulings.md) — binding rulings.
   R1 fixes the campaign shape; R3/R4/R5 fix semantics on the model-adjacent rows; R11 folds the
   Q6 promotion-retry boundedness question into this brief.
2. [`2026-08-14-issue31-migration-design.md`](../2026-08-14-issue31-migration-design.md) — sole
   authority on migration semantics; its `## Spec / impl blast radius — full verdicts` section
   (lines 8793-10108) is the row inventory this plan decomposes.
3. [`issues/open/31-slot-migration-redesign-source-authoritative-until-commit.md`](../issues/open/31-slot-migration-redesign-source-authoritative-until-commit.md)
   — the tracker issue the campaign discharges.

Where the design doc and the current Quint model disagree, the design doc governs. Where the
rulings ledger speaks, it governs over both.

This document is planning only. Nothing in this directory edits a spec, a model, or a line of
code. The issue drafts alongside it are numbered `c31-NN` in wave order; they get renumbered into
`issues/open/` only after human approval.

---

## 1. What the campaign has to land

The design replaces the Redis-style delete-as-you-copy slot migration with a
source-authoritative-until-commit shape, and in the course of five review cycles it accumulated a
much larger blast radius than "the migration rows": a node-identity protocol, a registration-token
protocol, a companion-field discipline over every parent-pointer writer, a staged role-flip
protocol with a durable node-local record, a data-directory layout raise, and a target-side shadow
store with its own replication and persistence representation.

The blast-radius section carries **107 distinct verdict entries** across **83 distinct LOCKED /
state-space row identities** plus the new-row and retirement families. Section 6 below is the
complete ledger; every entry there is mapped to exactly one cluster, or explicitly marked
read-only / no-op with the reason.

The campaign is *spec-first* throughout, per CLAUDE.md's locked-core-area discipline and R1: for
each row, the spec row is amended first, then the forcing test is written and observed failing,
then the implementation lands — all in one commit or one tightly-ordered commit pair, so
`just lint-spec` never sees a row without its tests or a tagged test without its row.

`specs/cluster.md` has no `Forced by` column (unlike `specs/persistence.md`), so a cluster row's
forcing tests are named in the design doc and at the mechanism, and the implementer carries the
`FM-`/`TR-` tags into the test bodies where `just lint-spec` can see them. This is stated in the
design doc at the revision-23 entry and is a standing obligation for every cluster that touches
`specs/cluster.md`.

---

## 2. Clustering criterion

R1 asks for *conflict-clustered* waves: one implementer per cluster, clusters within a wave
running in parallel, merging to main at wave boundaries. The clustering criterion is therefore
"two implementers in the same wave must not collide". Collision has three surfaces, and the
clusters below are drawn to be disjoint on all three:

- **Spec rows.** No two clusters own the same `FM-`/`TR-`/`SS-` row. This is the hard criterion:
  a row is amended by exactly one cluster, atomically, with its forcing tests. Where three
  different design deltas want the same row (TR-CLUSTER-035 is the worst case, with a
  shadow-discard trigger, a held-write disposition and a Raft-store discard mark), the row goes
  to a single late cluster that lands all its deltas together rather than being touched three
  times.
- **Apply arms.** `frogdb-server/crates/cluster/src/commands.rs` is one file and nearly every
  cluster touches it, so file-level disjointness is not achievable and is not the criterion.
  Arm-level disjointness is: the `AddNode` arm, the `Failover`/`SetRole` arms, the slot-migration
  arms and the `ResetCluster` arm are four separate regions of that file, and no two same-wave
  clusters write the same region. The residual textual conflicts are confined to two places —
  the `ClusterCommand` enum and the `NodeInfo` / `ClusterStateInner` structs — which is why
  **wave 1 lands the whole data model** (all new `NodeInfo` fields, the cluster-level counter, the
  typed migration record and residue declarations) so that later waves add arms rather than
  fields.
- **Node-local surfaces.** `cluster-runtime/src/handoff_barrier.rs`, the server's routing and
  command-dispatch paths, the recovery/persistence data-directory code, and the replication
  full-sync path are distinct crates or distinct modules; same-wave clusters are drawn so at most
  one owns each.

Test files follow the row ownership: a forcing test lives in the crate that owns the mutated code
(`cargo mutants -p <crate>` only runs that package's own tests, so a row forced solely from
`frogdb-server` integration tests contributes nothing to `frogdb-cluster`'s mutation score).

---

## 3. Wave sequence

| Wave | Clusters | Merges to main at boundary |
|---|---|---|
| 0 | *(this decomposition; human approval gate)* | — |
| 1 | c31-01 (identity & registration), c31-02 (data-directory & node-local durable state) | the whole replicated field set + the node-local durable substrate |
| 2 | c31-03 (topology plane), c31-04 (role authority) | every role/`primary_id` writer and the staged-flip protocol |
| 3 | c31-05 (migration record & phase machine) | the replicated migration protocol |
| 4 | c31-06 (barrier & client-visible semantics), c31-07 (target side, stream, residue) | the node-local halves of the migration |
| 5 | c31-08 (reset, wipe, observability) | the campaign |

### Why this order — the dependency edges

**Wave 1 → wave 2.** Every role writer in wave 2 must write the companion fields
(`promoted_from`, `synced`, `admitted_stage`, `parent_seq`) that wave 1 declares, and the staged
flip's adoption operand is `nodes[self].admitted_stage`, minted against
`nodes[self].registration_seq`. Wave 2 cannot express its own invariants until those cells exist.
c31-04's refusal partition is keyed on the `RefusalClass` that c31-01 makes a committed apply
outcome (FM-CLUSTER-047's extension), so c31-04 consumes a c31-01 product directly. c31-02 supplies
the durable home for the pending-transition record and the stage counter that c31-04's protocol is
written around, and the `DATA_DIR_LAYOUT_VERSION` raise that makes that home non-erasable.

**Wave 2 → wave 3.** The migration record's admission conjuncts read
`nodes[record.target].role == Primary` and `nodes[record.source].run_identity`, and the
demotion-cancel semantics — "the identity field write cancels every migration this node sources" —
is a wave-2 transition arm firing on a wave-3 record. Landing the record first would leave the
cancel clause referring to a writer that does not yet exist. Equally, SS-11's "no slot is assigned
to a node whose role is `Replica`" is a wave-2 invariant that every wave-3 slot writer must respect;
it is cheaper to establish it first than to retrofit it.

**Wave 3 → wave 4.** c31-06's held-write disposition table is a total function of the record's
exits; c31-07's reaper, promotion and rollback consume the `handoff_residue` entry that `Complete`'s
apply writes. Both are consumers of the wave-3 phase machine and neither can be written against a
record that does not exist.

**Wave 4 → wave 5.** c31-08's `TR-CLUSTER-035` amendment lands three clauses at once, two of which
(the shadow-discard trigger and the `-CLUSTERDOWN` held-write disposition) require the shadow store
and the barrier to exist. `CLUSTER MIGRATIONS` renders both open records and residue entries, so it
needs wave 3 and wave 4 both merged. The `TR-CLUSTER-005` join-gate rewrite is last because its
discard-mark half has a **blocking external prerequisite** (see section 5).

### Wave-internal parallelism

Waves 1, 2 and 4 run two implementers; waves 3 and 5 run one. The pairs are chosen for surface
disjointness:

- **c31-01 / c31-02**: c31-01 is entirely `frogdb-cluster` (replicated state, transitions,
  snapshot carriage); c31-02 is entirely `frogdb-persistence` + `frogdb-recovery` (data-directory
  layout, marker, identity store). Zero row overlap — c31-02 touches no `specs/cluster.md` row at
  all, only `specs/persistence.md`.
- **c31-03 / c31-04**: c31-03 writes the replicated role/`primary_id` apply arms in
  `commands.rs`; c31-04 writes node-local gate and record code in `cluster-runtime` and the server
  command path, and its two `specs/cluster.md` rows (TR-CLUSTER-033, FM-CLUSTER-046) are the
  self-role reconciler pair, which c31-03 does not touch. c31-04 *reads* TR-CLUSTER-042 and
  TR-CLUSTER-018 for its lineage guard and cites them; it does not move them.
- **c31-06 / c31-07**: c31-06 owns the source-side barrier and the client-facing routing/EXEC/WATCH
  rows; c31-07 owns the target-side shadow, the stream session family, and the residue lifecycle.
  The one place they meet is the `Complete` exit — c31-06 disposes of the held writes, c31-07
  starts the reaper — and the ordering rule between them (release fires after the assignment
  mutation and before deletion begins, §7) is stated in the design doc, so neither has to
  negotiate it with the other at implementation time.

---

## 4. Per-wave verification gates

Every wave boundary is a merge to `main`, and a wave may not merge until all of the following are
green for every cluster in it:

1. **`just lint-spec` green.** Spec↔test agreement: every amended or new `FM-CLUSTER-NNN` /
   `TR-CLUSTER-NNN` / `FM-PERSISTENCE-NNN` row names its forcing tests, and every tagged test
   matches a row. A retired row's forcing tests are deleted or re-pointed *in the same change* —
   this is not optional and is the trap the design doc calls out explicitly for
   `test_self_role_reconciler_absent_until_detection_is_enabled` (FM-CLUSTER-046, c31-04) and
   `only_migrate_and_cluster_are_slot_pause_exempt` (FM-CLUSTER-080, c31-06). A retired row left
   with a live forcing test pins the behaviour the campaign is removing.
2. **Forcing tests green**, and each one observed *failing first* against the pre-amendment
   behaviour. A forcing test that passes before the implementation lands is not forcing anything;
   the cluster's acceptance criteria name the negative-control observation explicitly.
3. **`just mutants-diff frogdb-cluster` triaged** for every cluster that touched
   `frogdb-cluster`, and `just mutants-diff frogdb-cluster-runtime` for every cluster that touched
   `frogdb-cluster-runtime`. Clusters touching `frogdb-persistence` / `frogdb-recovery` /
   `frogdb-replication` run `mutants-diff` on those crates too. Surviving mutants are either
   killed by a new test or documented at the code with why they are unobservable — never a blanket
   skip, per CLAUDE.md.
4. **Full-run gate checks at campaign end.** `just mutants frogdb-cluster` +
   `just mutants-gate frogdb-cluster 0.80` (and `frogdb-cluster-runtime` at the same gate) run once
   at the wave-5 boundary, not per wave — the per-wave discipline is `mutants-diff`, which is push
   discipline rather than a CI gate.
5. **`just lint` and `just lint-gates`.** The seam-lint family is unconditional; two seams are
   directly in this campaign's path — the clock-read seam (the no-wall-clock global ruling, which
   FM-CLUSTER-089's rewrite asserts directly) and the redirect-reply seam (every `-MOVED` /
   `-TRYAGAIN` / `-CLUSTERDOWN` in the held-write disposition table).
6. **Quint model agreement** for the waves that cite model invariants — see section 5.

The forcing tests must live in the crate that owns the mutated code. Several of this design's
tests are named as "node-local-durability tests in the crate that owns the record" in the design
doc; that placement is a mutation-score requirement, not a style preference.

---

## 5. External dependencies and gates

**Cluster-correctness issue 43 (Quint model semantics fixes, R3/R4/R5) — merge before wave 1.**
It is model-only and independent of the implementation, but three of its products are cited by the
campaign as settled semantics rather than as open questions:

- **R4** (refusal class minted at verdict, carried in the payload; arm 4b reachable) is the
  semantics c31-01 implements in FM-CLUSTER-047's extension and c31-04 implements in gate 3 (d)'s
  six-class partition. If issue 43 has not landed, c31-01 and c31-04 are implementing against a
  model that still recomputes the class at delivery.
- **R3** (adoption-time invariant: an applied role flip leaves no sourced open migration and no
  held slot on that node) is cited by c31-04 (the applied write is the boundary, not the staging)
  and forced by c31-05/c31-06 (the cancellation and hold-release are atomic with the adoption's
  applied write).
- **R5** (stale-never-admits) is cited by c31-04's effect-keyed record-clearing clause.

The hard requirement is that issue 43 merges **before wave 2**; the preference is before wave 1,
so c31-01 writes FM-CLUSTER-047's extension against a model that already carries the class.

**Cluster-correctness issue 44 (design-doc attribution corrections batch) — independent.** It
edits the design doc's attribution prose only. It can run at any point, in parallel with any wave,
and blocks nothing. It should not be folded into a cluster.

**Cluster-correctness issue 25 (solo-bootstrap usurper) — BLOCKING for c31-08.** The design doc is
explicit (V25-M1): an empty Raft store solo-bootstraps today
(`cluster_init.rs:383-391`, `:442-460`), which re-populates what a wipe removed. For an
operator-issued wipe that is an availability nuisance; for a **crash between the discard mark and
the commit** it is a split-brain source — the node boots, wipes its own Raft store, and forms a
second Raft group while the survivors still list it in theirs. The `TR-CLUSTER-035` discard-mark
mechanism **may not ship ahead of** the forcing test
`a_wiped_node_awaits_meet_instead_of_solo_bootstrapping`. c31-08 therefore either absorbs issue 25
or is gated on it landing first; the sequencing plan assumes absorption, and c31-08's draft says so.

**Cross-tracker items the design doc names**, to be dispositioned as the owning cluster lands:

| Item | Disposition | Cluster |
|---|---|---|
| cluster-correctness issue 15 (repatriation) | closes as superseded when §4's endpoint failover/restart rows land | c31-07 |
| cluster-correctness issue 16 (`AssignSlots` ignores open migrations) | subsumed by TR-CLUSTER-008/009's strengthening | c31-07 |
| cluster-correctness issue 17/18 (stale source / missed failover retry) | barrier liveness = the reconcile orphan-abort + the observation bound | c31-05 (bound), c31-03 (retry) |
| cluster-correctness issue 19/20 (forced failover inherits nothing / evicts old primary) | the `promoted_from`/`synced` stamps and demote-don't-remove | c31-03 |
| cluster-correctness issue 24 (residual real-clock dependence) | FM-CLUSTER-089's direct determinism assertion | c31-05 |
| cluster-correctness issue 28 (`CLUSTER FAILOVER` refused on a primary) | TR-CLUSTER-020, the declared operator escape for an all-unsynced shard | c31-03 |
| cluster-correctness issue 29 (cap ambiguity) | resolved by the config table's per-migration cap | c31-06 |
| cluster-correctness issue 30 (`CONFIG SET` replica priority re-registers) | TR-CLUSTER-027's preservation rule | c31-01 |
| cluster-correctness issue 32 (restarted source never re-arms its barrier) | FM-CLUSTER-104's restart arm / §3 fence reconstruction | c31-06 |
| spec-gaps issue 29 (spec row-edit sweep) | receives the five citation fixes the design doc offers | c31-01 raises them; the sweep applies them |
| spec-gaps issue 12 (watermark carries covered position) | landed `eedb76d0`; the snapshot-position substrate | c31-07 (consumer) |
| replication issue 24 (replid/offset pairing) | the replid half of `run_id` | c31-01 (consumer) |
| hardening-2 rework 12 (`ReplicaFeedGate`) | **removed** by §8; its issue gets a superseding note | c31-07 |
| spec-gaps issue 32 (`--force-fresh-data-dir` refuses foreign entries, R6) | persistence-area, **outside** this campaign | — |

Wave 0 has not assessed the remaining open cluster-correctness issues (14, 26, 27, 33, 34, 35, 36,
38, 39, 40) against the campaign. Several are plainly adjacent — issue 34 (`CLUSTER RESET` becomes
node-local) sits on TR-CLUSTER-035, issue 35 (node identity outlives the process) sits on
`run_identity`, issue 14 (role transitions admit malformed parents) sits on the companion-field
rule, issue 26 (planned-failover barrier) sits on TR-CLUSTER-017 — and the design owner should
decide at approval time whether each is absorbed by its cluster or stays independent. They are
listed here so the decision is made deliberately rather than discovered mid-wave.

**R11 — Q6 promotion-retry boundedness.** Per the ruling, no standalone issue: it is a *candidate
doc extension*. Two clusters can touch promotion-retry, and both drafts carry the note:

- **c31-07** (primary): the shadow's `ReportSlotPromoted` / failed-promotion rollback path is the
  promotion whose retry behaviour Q6 asks about. If c31-07 touches that retry, the design owner
  adds a boundedness bound to the design doc during that wave and the Quint model row follows.
- **c31-03** (secondary): if the failover work touches promotion retry (the missed-failover
  re-drive of issue 18), the same applies there.

The decision is the design owner's, taken *during* the wave, not by the implementer.

---

## 6. Row-coverage ledger

Every verdict entry in `## Spec / impl blast radius — full verdicts` (design doc lines
8793-10108), in the order the section states them, with its owning cluster. Nothing is dropped;
entries that carry no work are marked and the reason given.

### 6.1 The "Rewritten" bullet (design doc ~8795-9000)

| Row | Verdict | Intended semantics (one line) | Cluster |
|---|---|---|---|
| TR-CLUSTER-010 | rewritten | `BeginSlotMigration` creates a record at `phase = Snapshotting`; requires `slot_map[slot] == source` | c31-05 |
| TR-CLUSTER-011 | rewritten | `PrepareSlotHandoff` proposed only at parity; arms the barrier, increments the generation | c31-05 |
| TR-CLUSTER-012 | rewritten | `ConfirmSlotHandoffDrained` seals the slot and sets `drained_pos` | c31-05 |
| TR-CLUSTER-013 | rewritten | `CompleteSlotMigration` admitted on the logical `handoff_seq` token; reads the slot map (`slot_map[slot] == record.source`) | c31-05 |
| TR-CLUSTER-014 | rewritten | `AbortSlotHandoff` clears `drained_pos`/`attempt_id`, increments attempts | c31-05 |
| TR-CLUSTER-015 | rewritten | `CancelSlotMigration`: repatriation precondition retired, target-discard + release events, target joins the proposer set | c31-05 |
| TR-CLUSTER-016 | rewritten | its precondition named the slot-scoped write barrier §8 deletes; rewritten to ordinary replica-feed backpressure | c31-06 |
| TR-CLUSTER-035 | rewritten (three deltas) | shadow-discard trigger (§5); held-write obligation paid to clients as `-CLUSTERDOWN`; HARD-path Raft-store discard mark (declaration (3), re-shaped to the Precondition) | c31-08 |
| FM-CLUSTER-026 | rewritten | importing gates collapse — `ASKING` is a no-op under source authority | c31-06 |
| FM-CLUSTER-027 | rewritten | `RESTORE` exemption retired; routing `MOVED <slot> <source>` is the winning gate | c31-06 |
| FM-CLUSTER-028 | rewritten | the source serves the whole slot and never `ASK`s; clients never observe a split slot | c31-06 |
| FM-CLUSTER-029 | rewritten | `WATCH` routing re-derived under source authority | c31-06 |
| FM-CLUSTER-031 | rewritten | `SETSLOT` surface; owned idempotency (re-issued `MIGRATING` is `Ok`, one-shot attempts reset) | c31-05 |
| FM-CLUSTER-032 | rewritten; unassigned-slot arm **retired** | membership arms kept; V7-M4 reverses V4-m5 — migrating an unassigned slot is refused with direction to `AssignSlots` | c31-05 |
| FM-CLUSTER-033 | rewritten, **headline inverted** | `Complete` *does* read the slot map; the row now specifies the `SlotAlreadyAssigned`-at-completion refusal and the `target ∈ nodes` conjunct | c31-05 |
| FM-CLUSTER-035 | rewritten | `SETSLOT … STABLE` cancel semantics under the new record | c31-05 |
| FM-CLUSTER-036 | rewritten | a force failover cancels the migrations of the node it removes; residue re-target rides the role→Replica transition | c31-03 |
| TR-CLUSTER-017 | rewritten (early) / **unchanged, stated** (V15-M3) | see open question OQ-1; V15-M3's treatment is the planned-failover attestation ordering | c31-03 |
| TR-CLUSTER-018 | rewritten + amended (V14-C2/C3, V15-C1) | `synced` conjunct on the belt; promotion/demotion writes carry `promoted_from`/`synced`/`admitted_stage`; sibling re-parent writes the companions | c31-03 |
| TR-CLUSTER-019 | rewritten | automatic failover proposal under the §4 endpoint rules | c31-03 |
| FM-CLUSTER-034 | rewritten | client `ClusterEvent` contract restated: exactly one `SlotMigrationCompleted` on success, nothing on failure, nothing at begin; the §9 operator log is a distinct stream | c31-05 |
| FM-CLUSTER-064 | rewritten | the admin-gated fail-closed split table gains the `CLUSTER MIGRATIONS` row | c31-08 |
| FM-CLUSTER-037 | rewritten | commit-to-apply window, now with the §5 pre-promotion refusal | c31-07 |
| FM-CLUSTER-084 | rewritten | admission conjunctions incl. run/proposer guards on every position writer | c31-05 |
| FM-CLUSTER-086 | rewritten | attempt stamping; the generation is incremented by `Begin` **and** by `Prepare` | c31-05 |
| FM-CLUSTER-087 | rewritten | release events from Cancel/Abort/Complete/prune/self-fence | c31-05 |
| FM-CLUSTER-089 | rewritten, **not retired** | with deadlines gone the row asserts the determinism rule directly — no replicated admission predicate reads node-local state; both existing forcing tests re-pointed | c31-05 |
| FM-CLUSTER-090 | rewritten | barrier action under the new phase machine | c31-05 |
| FM-CLUSTER-091 | rewritten, split (V5-M1) | drain-wait `-TRYAGAIN` refusal retired (held or acked, never bounced); the bounded drain-wait property becomes the pre-`Confirm` unconditional observation bound | c31-05 |
| TR-CLUSTER-008 | rewritten | `AssignSlots` refuses all phases **and** `handoff_residue` entries, with exactly two declared arms (rollback, orphan re-home) | c31-07 |
| TR-CLUSTER-009 | rewritten | `RemoveSlots` refusal strengthened identically | c31-07 |
| TR-CLUSTER-001 | rewritten (V7-M3, V18-C1) | `AddNode` upsert is field-wise and preserves `run_identity`; the fresh arm mints `registration_seq` from `registration_seq_gen` and increments it | c31-01 |
| TR-CLUSTER-002 | rewritten (V7-M3, V18-C1) | the upsert may touch address/port/config only; `run_identity`, `role` and `registration_seq` are written only by their declared writers | c31-01 |
| TR-CLUSTER-027 | unchanged, stated — extended (V15-M2, V18-C1) | live `CONFIG SET` re-registration routes through the same field-wise upsert and must preserve `synced` as well as `run_identity`/`promoted_from`/`admitted_stage`/`registration_seq` | c31-01 |
| TR-CLUSTER-003 | rewritten (V7-m3, V15-C1, V16-M1) | prune *marks* residue (`source_gone`/`target_gone` + unassign), never removes; cancels the departed node's open migrations; the detach writes the companions; gains the held-write disposition row | c31-03 |
| FM-CLUSTER-092 | rewritten | a write caught by the barrier wakes redirected — inverted under source authority | c31-06 |
| FM-CLUSTER-093 | rewritten | a transaction parked across finalization is redirected, not committed | c31-06 |
| FM-CLUSTER-094 | rewritten | a script in flight across a handoff leaves no write on the former owner | c31-06 |
| FM-CLUSTER-095 | rewritten, arm split | finalization-refusal arm **retired**, ownership-moved arm kept; the SlotFence generation mechanism is unchanged | c31-06 |
| FM-CLUSTER-096 | rewritten | unpinnable batches: the parked-batch disposition at each exit; the drain-covers-continuations containment | c31-06 |
| FM-CLUSTER-104 | rewritten, extended (V7-m5) | same-node re-arm only, successor never inherits; §3's boot-reconstruction rule becomes the row's restart arm | c31-06 |
| FM-REPLICATION-022 | noted, narrowed (V7-C1, V14-C1) | the run-identity report is kind-stamped (`Boot`/`Demotion`) in the committed payload; the row's `REPLICAOF` spelling is standalone-only — in cluster mode the spelling is `CLUSTER REPLICATE` | c31-01 |
| TR-CLUSTER-033 | adopted and rewritten (V12-C1/M2, V13-C1, V14-M1, V15-C2) | the replicated→local `SelfRoleReconciler` becomes the declared role-authority convergence for *unexplained* disagreements, gaining the pending-record exclusion and the lineage guard; the persisting-across-ticks damping is subsumed by the resolved-stage selector | c31-04 |
| FM-CLUSTER-046 | rewritten (V12-M2, V13 note) | the reconciler is unconditional and load-bearing; the feature gate is removed; `test_self_role_reconciler_absent_until_detection_is_enabled` must be **deleted or inverted in the same change** | c31-04 |
| FM-CLUSTER-079 | rewritten (V8-M5) | unpinnable-command folding is the mechanism behind §3's unpinnable held batches; the row gains the byte cap and the per-exit disposition table | c31-06 |
| FM-CLUSTER-082 | rewritten (V8-M5) | "neither can release the other" now composes with a barrier armed by replicated phase and re-derived at boot; the independence claim survives | c31-06 |
| FM-CLUSTER-083 | rewritten (V8-M5) | the Outcome enumeration gains a third: a parked `EXEC` whose batch is unpinnable is answered `-TRYAGAIN` | c31-06 |
| TR-CLUSTER-004 | rewritten (V8-M5/C2, V9-C1, V14-C2) | `SetRole` re-targets residue and re-homes owned slots in the same apply; carries the per-object epoch fence; the destination is the **validated successor**, never a post-write `shard_primary` walk; writes `synced = false`, `promoted_from = None`, `admitted_stage = None` | c31-03 |
| TR-CLUSTER-005 | rewritten (V9-M5, V23-M2, V25-M1) | the issue-25 empty-Raft-state precondition composes with the join-empty admission gate into one stated precondition; the wipe path is two paths; a **BLOCKING** undischarged dependency on issue 25 rides the row | c31-08 |
| TR-CLUSTER-042 | rewritten (V8-M5, V13-M1, V14-C2/C3, V15-C1) | outright-removal semantics own the residue marking; the promotion write stamps `promoted_from`/`synced`/`admitted_stage`; sibling re-parent clears the companions; cited by the lineage guard's removed-upstream disjunct | c31-03 |
| TR-CLUSTER-021 | amended (V14-C3) | candidacy gains `nodes[candidate].synced == true`; a shard whose only replicas are unsynced gets no auto-failover (stated availability loss) | c31-03 |
| FM-CLUSTER-047 | extended (V14-M3) | the committed rejection records `refused(class)`, `class` = the first failing conjunct in declared evaluation order; a pure function of payload + pre-apply state, so FM-CLUSTER-089 determinism holds — **R4 governs: minted at verdict, carried in the payload** | c31-01 |
| SS-11 | amended (V8-C6) | "no slot is assigned to a node whose role is `Replica`"; every role→Replica writer re-homes in the same apply; every slot writer requires the assignee to be `Primary` | c31-03 |
| SS-2 (`role`) | amended (V15-C1) | the writer enumeration stated as the LOCKED cell verbatim with every writer marked; `ReportRunIdentity`'s Demotion arm added; the cell's omission of `AddNode` is flagged, not silently chosen | c31-03 |
| SS-3 (`primary_id`) | amended (V15-C1, V16-C1) | all five LOCKED writers amended incl. `Failover`'s three pointer writes and `RemoveNode`'s detach; the Demotion arm added; the Promotion arm removed; every `primary_id` write also writes the three companions and increments `parent_seq` | c31-03 |
| SS-1 (membership) | amended (V18-C1) | the `AddNode` **fresh** arm is `registration_seq_gen`'s sole writer; `ResetCluster`'s membership reduction is a non-writer that must not rewind the generation | c31-01 |
| New `NodeInfo` fields (V14) | new | `promoted_from: Option<NodeId>`, `synced: bool`, `admitted_stage: Option<u64>`, each under the companion-field rule | c31-01 declares; c31-03 enumerates writers |
| New `NodeInfo` field (V16) | new | `parent_seq: u64`, incremented by every `primary_id` writer, decreased by none, re-initialized by a later registration | c31-01 declares; c31-03 enumerates writers |
| New field + counter (V18) | new | `NodeInfo.registration_seq` and `ClusterStateInner.registration_seq_gen`, outside the companion/parenting enumerations, writer set of exactly one (`AddNode`'s fresh arm) | c31-01 |

### 6.2 "Added in revision 15" (design doc ~9000-9200)

| Entry | Verdict | Cluster |
|---|---|---|
| TR-CLUSTER-018 + TR-CLUSTER-042, sibling-re-parent halves | amended — each sibling's apply writes `synced = false`, `admitted_stage = None`, `promoted_from = None`; new test `failover_sibling_reparent_clears_companion_stamps` | c31-03 |
| TR-CLUSTER-003, the detach half | amended — `reparent_children(.., None)` carries the companion writes | c31-03 |
| FM-CLUSTER-002 | **unchanged, stated** — `role == Replica ∧ primary_id == None` is a reachable, declared state (the detached replica); `remove_node_prunes_migrations_and_detaches_replicas` stays valid and gains two assertions; two new tests | c31-03 |
| TR-CLUSTER-017 | unchanged, stated (V15-M3) — the planned-failover barrier/drain/offset-parity wait is the attestation trigger; the flow orders the attestation's commit before the `Failover` proposal; **no exemption added** | c31-03 |
| TR-CLUSTER-020 | unchanged, cited — the replica-issued `CLUSTER FAILOVER` is the declared operator escape for an all-unsynced shard | c31-03 |
| TR-CLUSTER-027 | unchanged, stated — extended (see 6.1) | c31-01 |
| TR-CLUSTER-041 | unchanged, cited — follower proposals answer `Proposed::Forwarded` and carry no `ClusterResponse`, which is *why* the `RefusalClass` cannot ride the reply channel | c31-01 |
| `AttestReplicaSynced`'s fence fields | new row content (V15-C4, V16-C1, V17-C1, superseded V18-C1) | c31-01 |

### 6.3 "Added in revision 16" (design doc ~9200-9260)

| Entry | Verdict | Cluster |
|---|---|---|
| `NodeInfo.parent_seq` | **New row** (V16-C1); SS-3 gains a note rather than a writer | c31-01 declares; c31-03 lands the SS-3 note with the writer sweep |
| TR-CLUSTER-035, second obligation (V16-M1) | the record-removing transition owes §3's held-write disposition; test `cluster_reset_pays_the_held_writes_a_real_reply` | c31-08 |
| TR-CLUSTER-003 / FM-CLUSTER-002, held-write disposition (V16-M1) | §3 gains a `RemoveNode` prune row with both arms | c31-06 writes the §3 row; c31-03 owns the transitions |
| The five migration-record SS rows (Open migrations; A migration's prepared handoff; A handoff's attempt number; A handoff's deadlines; Handoff attempt counter) | **joined, not newly amended** (V16-M2) — but see OQ-2 on the deadlines row | c31-05 |

### 6.4 "Added in revision 17" (design doc ~9260-9310)

| Entry | Verdict | Cluster |
|---|---|---|
| `AttestReplicaSynced.observed_run` | superseded in revision 18 — **no work**, recorded as history | — (no-op) |
| `ReportRunIdentity` declared a writer of SS "Open migrations" (V17-M1) | writer-join row amended + a §3 exit row added; test `every_record_removing_transition_has_a_held_write_exit_row` walks the join row | c31-05 owns SS "Open migrations"; c31-06 owns the §3 exit row |

### 6.5 "Added in revision 18" (design doc ~9310-9360)

| Entry | Verdict | Cluster |
|---|---|---|
| `NodeInfo.registration_seq` + `ClusterStateInner.registration_seq_gen` (V18-C1) | new field + counter; TR-CLUSTER-001/002/027 and SS-1 amended; FM-CLUSTER-100 extended to carry both through snapshot install | c31-01 |
| `AttestReplicaSynced.observed_registration_seq` (V18-C1) | new payload field + admission conjunct; operand pair becomes `(parent_seq, registration_seq)` | c31-01 |
| `ReportRunIdentity.observed_registration_seq` (V18-M1) | new payload field + a third conjunct on the Demotion arm's fence; the absent-operand exception narrowed to `kind = Boot` | c31-01 |
| FM-REPLICATION-021 (V18-M3) | **cited, not contradicted** — revision 17's fresh-`replid` claim is deleted; the design asks LOCKED replication for nothing | c31-01 |

### 6.6 Revisions 19-22 (design doc ~9360-9560)

| Entry | Verdict | Cluster |
|---|---|---|
| Revision 19 (`staged_registration_seq`, the effect-keyed clearing clause, gate 3 (d) arm 4a/4b split) | **no row Amended, no row New, no row Retired** — node-local and design-internal; two node-local-durability forcing tests | c31-04 (mechanism), c31-02 (durable home) |
| Revision 20 (paired record-binding conjunct; fail-closed stage-counter rules; the clearing clause's client reply; the own-`ResetCluster` correction) | **no LOCKED row changes**; LOCKED rows read and not moved: TR-CLUSTER-005, TR-CLUSTER-041, FM-CLUSTER-100, SS-1/2/3 | c31-04 |
| Revision 21 (stage-counter boot rule becomes a staging refusal; arm 5's reply; the replicated floor; the two permanent disciplines) | **no LOCKED row changes**; `CLUSTER INFO` gains a reported field (additive, §9) | c31-04, with the `CLUSTER INFO` field surfaced by c31-08 |
| Revision 22 (`stage_counter_state` field; `DATA_DIR_LAYOUT_VERSION` 1→2; the mint-time floor; corrected untrusted-state exits) | its "no LOCKED row changes" verdict is **withdrawn in revision 23**; the true verdict is FM-PERSISTENCE-049 Amended twice | c31-02 (layout raise), c31-04 (mint-time floor, untrusted exits) |

### 6.7 Revisions 23-25 — the declared persistence amendments (design doc ~9560-9712)

| # | Row | Verdict | Cluster |
|---|---|---|---|
| (1) | FM-PERSISTENCE-049 | Amended — `verify`'s outcome set gains "an existing one with its `layout_version` raised", published by a second marker write; the fourth exit (the populated-directory bail) must be named wherever the outcome set is | c31-02 |
| (2) | FM-PERSISTENCE-049 | Amended — the marker phase moves **out of** the `rocks_backed` gate; the marker becomes unconditional | c31-02 |
| (3) | TR-CLUSTER-035 | Amended — the HARD path gains a Raft-store discard obligation, mark-then-wipe-on-restart, attached to the **Precondition** (V24-M4) | c31-08 |
| (4) | FM-PERSISTENCE-048 | Amended — `contains_foreign_files`'s excused set widens by exactly two FrogDB-owned top-level entries (`raft`, `replication_state.json`), "and nothing else" retained. **Sequencing is a hard constraint**: (2) must not ship without (4) | c31-02 |
| (5) | FM-PERSISTENCE-057 | Amended — the layout's path set: four paths become six (`frogdb_node_identity`, `frogdb_raft_discard`), single-owner rule extended over both | c31-02 |
| (6a) | FM-PERSISTENCE-057 | Amended — the row's own excused-set sentence widens with (4); second delta on the same Invariant | c31-02 |
| (6b) | FM-PERSISTENCE-059 | Amended — the row's excused-set sentence widens for `contains_foreign_files` but **not** for `pending_install`, whose "exactly those names" stays `staging*`/`backup` | c31-02 |
| — | FM-PERSISTENCE-050, -051, -023, -059 (mint-once), TR-PERSISTENCE-051 | read as binding constraints, **not moved**; -050 gains a stated downgrade cost and a recovery row | c31-02 |
| — | FM-CLUSTER-006 | read, confronted at gate 1, **unbreached**; its Invariant cell carries a stale citation (see 6.10) | c31-08 |
| — | FM-CLUSTER-101 | read at the untrusted exits (demote-don't-remove), unmoved | c31-04 |

### 6.8 The "Retired" bullet (design doc ~9712-9727)

| Row | Verdict | Cluster |
|---|---|---|
| FM-CLUSTER-085 | **Retired** — the handoff lease; its property ("a dead finalizer cannot wedge a slot") is re-provided by the observation bound plus the leader auto-`Complete`; a replacement row states this | c31-05 |
| FM-CLUSTER-097 + the `ReplicaFeedGate` | **Retired** — purpose re-derived to nothing under source authority; the row is rewritten to assert the *absence* of migration feed holds | c31-07 |
| FM-CLUSTER-080's `MIGRATE` slot-pause exemption | **Retired** — under §3's exempt-set rule `MIGRATE`/`RESTORE` are held like every other write; `only_migrate_and_cluster_are_slot_pause_exempt` is re-pointed at the new exempt set (`CLUSTER` only). **Disambiguation (V14-m4)**: this is the *slot-pause seal's* set; the staged-flip fence has a different, member-enumerated set | c31-06 (with the c31-04 disambiguation cross-reference) |

### 6.9 The "Unchanged, stated" bullet (design doc ~9727-9745)

| Row | Verdict | Cluster |
|---|---|---|
| FM-CLUSTER-038 | unchanged, stated — blocked-client wake at `Complete` | c31-06 |
| FM-CLUSTER-061 / -062 / -063 | unchanged, stated — the admin-gating class's semantics; only 064's table gains a row | c31-08 |
| FM-CLUSTER-095's SlotFence generation input | unchanged (the row itself is arm-split, 6.1) | c31-06 |
| FM-CLUSTER-100 | unchanged, stated — **extended** to carry `NodeInfo.run_identity`, `handoff_residue`, `NodeInfo.registration_seq` and `ClusterStateInner.registration_seq_gen` through snapshot install | c31-01 |
| TR-CLUSTER-026 | unchanged, stated — the self-fence gains the held-set release row (§3) | c31-06 |
| TR-CLUSTER-034 | unchanged, stated — per-node arm/release reaction | c31-06 |
| FM-CLUSTER-081 | unchanged, stated — the `CLUSTER` exemption survives as the sole member of the slot-pause seal's exempt set | c31-06 |
| FM-CLUSTER-088 | unchanged, stated — cross-slot independence holds because the held-byte cap is per-migration | c31-06 |

### 6.10 The "New rows" bullet (design doc ~9745-10044)

This bullet enumerates the mechanisms that get brand-new spec rows. Grouped by owning cluster:

- **c31-01** — `NodeInfo.run_identity` + `ReportRunIdentity` incl. the three proposing moments, the
  `(incarnation, identity_seq)` ordering, the boot ordering rule, source-side-only cancellation;
  the incarnation durability contract; the run-identity **triple** with three-component equality;
  the split ordering conjunct (`≥` admits, strict `>` gates the field write, equality is a
  topology-only re-proposal) with the one-in-flight damping rule; the `RefusalClass` six-value
  partition **as a committed apply outcome** (its *consumption* is c31-04's); `AttestReplicaSynced`
  + `synced`; `promoted_from`; `admitted_stage`; `parent_seq`; `registration_seq` /
  `registration_seq_gen`; the `run_identity` lifecycle across `RemoveNode`/re-`MEET`,
  `ResetCluster` and snapshot install; the §0 absent-operand rule with its named exceptions and the
  value-read/absence-test distinction; the `proposer` payload field on every transition with the
  stamping rule and the payload-vs-`self_node_id` determinism argument; the global
  apply-determinism principle with its node-local-reaction carve-out.
- **c31-03** — the demotion re-target on both sides; the `RetargetSlotResidue` transition (V8-C4,
  with V9-M1's target arm removed per V10-C4); the validated-successor rule with the
  admission-time shard-relationship conjunct and the never-a-post-write-walk rule; the §1 residue
  liveness obligation; the `shard_primary(n)` definition with its None-fails-closed rule;
  `in_shard_parent(u, n)` as the one-hop pre-apply pointer read.
- **c31-04** — gate 3, the replication-command gate; the staged flip; the durable
  pending-transition record (`{kind, target_upstream, stage_id, adopted,
  staged_registration_seq}`) and its fsync discipline; the level-triggered destructive adoption
  with its five-operand binding; the stage-resolution precedence rule; gate 3 (d)'s six-class
  refusal partition incl. the 4a/4b split **[R4]**; the paired record-binding conjunct; the
  lineage guard bound to `promoted_from`; the record's single-writer rule; the effect-keyed
  record-clearing clause **[R5]**; the reply-token totality rule; the fail-closed recovery
  discipline; the stage-counter rules and the `stage-counter-untrusted` state with its exits; the
  replicated floor on the mint; the `cluster-staged-flip-reply-timeout` config row; the §3
  staged-flip fence row with its member-enumerated exempt set; the cluster-mode role-command
  surface declaration; the quiesced-arm unreachability statement.
- **c31-05** — the typed record declaration; the four captured parameters as immutable record
  fields + the no-re-stamp rule; `Begin`/re-issue residue conjuncts + the reaper's residue fence
  (admission side); the progress-sensitive observation bound with narrowed reset,
  `last_observation` dedup and the full `ObserveMigration` conjunction; the leader
  auto-`Complete`; `Confirm`-resets-observation-counter with the field-writers table's third reset
  trigger and `≥`-bound semantics; the pre/post-`Confirm` observation-bound split, the tick-cadence
  knob, the calibration obligation and the one-shot attempts reset; `Complete`'s target-role
  conjunct; the counted-replica-set definition with empty-set/unset-false and the set-change floor
  clearing; the bootstrap/join proposal orderings + `node ∈ nodes`; the Prepare/Begin/Complete
  absence tests; the operator-exit row for a pre-Draining wedged source (§1); full payload
  declarations for every transition; `CancelSlotMigration`'s widened proposer set.
- **c31-06** — the sealed-fence / local-fence-never-weaker invariant and the self-fence held-set
  release; the held-write disposition table incl. unpinnable batches and the demotion-wins
  precedence rule; the totality claim over record-removing transitions; the §3 exempt-set rule
  (seal exempts only `CLUSTER`); the enumerated non-command mutators (eviction and active expiry
  suspended for a sealed slot) and the lazy-expiry-on-read classification; the fence-reconstruction
  at-boot rule; the per-migration cap scoping with the node-wide sizing note; the automatic-cutover
  deviation row (§6); the Redis-deviation rows (no `ASK`, `ASKING` no-op, `MIGRATE` not used for
  resharding, no split markers); the latch level rule with its armed-barrier scope; the §3
  `RemoveNode` and `ReportRunIdentity` exit rows.
- **c31-07** — target ingest/resume + `(run_id, position)` receipt assertions with per-assertion
  consequences incl. the zero-advance no-op; the per-migration-backlog "history intact" floor; the
  coverage obligations (drain-flush + periodic); the received-vs-applied head definitions;
  `ReportMigrationIngest` admission; `ReportTargetReplicaAck` + `target_replicas_acked_pos`;
  `handoff_residue` lifecycle + `ReportSlotPromoted` + `ConfirmSlotDeleted` + the
  attestation-gated delete + the failed-promotion rollback; the target-discard +
  residue-guarded level sweep + reset-discard + Begin-time discard-then-ingest; shadow durability
  + the full-sync payload row (and the replication-side `+FULLRESYNC` payload-contents row);
  shadow replication through the target feed + the honest no-escape-hatch row + the replicated
  durability conjunct; shadow promotion + the pre-promotion refusal incl. the replica window;
  FLUSH/memory-pressure aborts with the target as proposer; the expiry-convergence row; the
  replicated-residue deletion + defer-while-Replica guard + notification suppression + ordering;
  the migration-stream session family incl. resume-from-own-backlog; `covered_applied` durability
  inheritance; the applied-attestation serving gate + rollback re-label reversal; the crash-atomic
  re-label with its named intermediate state; the `ClearSlotResidue` operator verb with its
  no-lawful-automatic-remover admission, the `promoted == true` conjunct and the
  `target_gone == false` conjunct; the orphan re-home arm's case-4b split; residue lifecycle under
  membership change; the at-most-one-residue-entry-per-slot invariant; the durable-attestation
  rule (the target proposes only fsync-durable positions) and the replica-side fsync-attestation
  half; the `target_attesting_run`/`target_run` pairing with cross-run replacement semantics and
  `Complete`'s current-run conjunct.
- **c31-08** — the join-empty admission gate + `ResetCluster`'s non-empty refusal (V8-C1); the
  SOFT/HARD `ResetCluster` identity-cell distinction; `ResetCluster`'s re-mint stamped
  `kind = Boot`; `CLUSTER MIGRATIONS` gating/reply incl. residue entries (§9); the `CLUSTER INFO`
  node-local fail-closed fields (`pending-stage`, `stage-counter-untrusted`,
  `raft_discard_pending`); the metrics, operator event log, `frogctl` and Grafana surfaces.

### 6.11 Cross-tracker and citation-fix entries (design doc ~10044-10108)

| Entry | Disposition | Cluster |
|---|---|---|
| Cross-tracker bullet (issues 15, 16, 17/18, 12, replication 24, FM-REPLICATION-014/-021/-022/-023/-030, INV-OFFSET-2, hardening-2 rework 12, cluster issue 29 cap ambiguity, FM-CLUSTER-096's cross-shard VLL hole) | see section 5's table; the VLL hole **remains open**, restated with its changed consequence and containment | various |
| SS-1 `Writer(s)` span `833-854` | citation fix, carried into spec-gaps issue 29 | c31-01 raises |
| SS-4 `Writer(s)` cell (two epoch resets + the re-key) | citation fix | c31-01 raises |
| SS-2 `Writer(s)` omits `AddNode` | citation fix; the writer-join records the disagreement rather than choosing | c31-03 raises |
| TR-CLUSTER-001/002/027 fresh/upsert arm split | citation fix — state the split explicitly | c31-01 raises |
| FM-CLUSTER-006 `Invariant` stale citation (`commands.rs:478-518` → `816-862`) | pre-existing LOCKED-spec defect, **not this design's to fix**; recorded | c31-08 raises |
| Owed follow-up: `--force-fresh-data-dir` beside foreign entries | **ruled R6**, filed as spec-gaps issue 32; outside this campaign | — |

---

## 7. Cluster summary

| # | Slug | Wave | Size | Primary crates |
|---|---|---|---|---|
| c31-01 | node-identity-and-registration-tokens | 1 | L | `frogdb-cluster` |
| c31-02 | data-directory-layout-and-node-local-durable-state | 1 | M | `frogdb-persistence`, `frogdb-recovery` |
| c31-03 | topology-plane-role-writers-and-failover | 2 | L | `frogdb-cluster` |
| c31-04 | role-authority-staged-flip-and-refusal-partition | 2 | L | `frogdb-cluster-runtime`, `frogdb-server`, `frogdb-cluster` |
| c31-05 | migration-record-and-phase-machine | 3 | XL | `frogdb-cluster` |
| c31-06 | drain-barrier-and-client-visible-slot-semantics | 4 | L | `frogdb-cluster-runtime`, `frogdb-server` |
| c31-07 | target-shadow-stream-and-residue-lifecycle | 4 | XL | `frogdb-cluster`, `frogdb-replication`, `frogdb-persistence` |
| c31-08 | reset-wipe-and-migration-observability | 5 | M | `frogdb-cluster`, `frogdb-recovery`, `frogdb-server` |

Sizes are calibrated against the spec-gaps P1..S1 campaign: **S** ≈ one row family and its tests;
**M** ≈ 5-10 rows or one self-contained mechanism; **L** ≈ 10-20 rows or two coupled mechanisms;
**XL** ≈ 20+ rows or a mechanism with its own protocol surface. c31-05 and c31-07 are XL and are
the two clusters most likely to want splitting if a wave stalls; section 8 records where the
natural seam is in each.

---

## 8. Notes for the approver

- **c31-05 is the campaign's critical path.** It is XL, it runs alone in wave 3, and both wave-4
  clusters consume it. If it needs splitting, the natural seam is *record + admission + phases*
  (TR-CLUSTER-010..015, FM-CLUSTER-031/032/033/035/084/086) versus *liveness + release*
  (FM-CLUSTER-085 retire, -087, -089, -090, -091, the observation bound, the leader
  auto-`Complete`). The two halves share the record type but not the admission conjuncts, so the
  split is workable at the cost of one extra wave.
- **c31-07's natural seam** is *stream + target ingest* (§4, §8, `ReportMigrationIngest`,
  `ReportTargetReplicaAck`, the resume rule) versus *shadow + residue* (§5, §7,
  `ReportSlotPromoted`, `ConfirmSlotDeleted`, `ClearSlotResidue`, `AssignSlots`' two arms,
  TR-CLUSTER-008/009). The full-sync payload row sits with the shadow half.
- **The `commands.rs` enum and struct churn is concentrated in wave 1 deliberately.** If the
  approver would rather see each cluster add its own transitions, expect wave-2 and wave-4 merge
  conflicts in the `ClusterCommand` enum and `NodeInfo`, and budget for them.
- **Nothing here shortens the campaign by parallelising more.** The dependency edges in section 3
  are semantic, not organisational: wave 2 genuinely cannot write the companion-field sweep before
  the fields exist, and wave 4 genuinely cannot dispose of held writes before the exits exist.

---

## Open questions for the design owner

**OQ-1 — TR-CLUSTER-017 carries two verdicts.** The "Rewritten" bullet lists it in the
`FM-CLUSTER-036 + TR-CLUSTER-017/018/019` group ("failover/restart naming an endpoint — §4 rules,
now asymmetric per V4-M5"), while the revision-15 additions list it as **unchanged, stated**
(V15-M3: the planned-failover barrier/drain/offset-parity wait is the attestation trigger, and
"no exemption is added — one belt, uniformly"). Both can be true of different clauses of the row,
but the campaign needs one verdict to land atomically. Wave 0 has provisionally assigned the row
to c31-03 with the revision-15 treatment as the governing one (later revision wins) and the
endpoint-abort rules landing at TR-CLUSTER-018/019. **Confirm or correct.**

**OQ-2 — the SS "A handoff's deadlines" row.** FM-CLUSTER-089's rewrite is predicated on "the
deadline mechanism gone", and the design's `ObserveMigration` bound replaces every deadline; but
revision 16's writer join records all five migration-record SS rows as "joined, not newly amended",
which reads as *no verdict* for the deadlines row. Wave 0 believes the row is **Retired** (its
state has no writer left) and has assigned it to c31-05 on that basis, but retiring a state-space
row is a bigger act than the join implies. **Rule: retired, or does it keep a residual meaning
(e.g. re-purposed as the observation counters' home)?**

**OQ-3 — the candidacy-adjacent rows the section never dispositions.** TR-CLUSTER-021 gains a
`synced` conjunct, but the blast radius gives no verdict for **TR-CLUSTER-043** (the
automatic-promotion staleness bound), **FM-CLUSTER-105** (an undetermined offset is a promotion
tier, not a promotion score) or **FM-CLUSTER-106** (a departing replica is not a promotion
candidate for a slot under handoff). All three describe the same candidate set the amendment
narrows, and FM-CLUSTER-106 is explicitly about a slot *under handoff* — a phrase whose meaning
the phase-machine rewrite changes. Wave 0 has **not** assigned them. **Do they need verdicts, and
if so in c31-03 (candidacy) or c31-05 (handoff phases)?**

**OQ-4 — the failover-family FM rows the section never dispositions.** TR-CLUSTER-018 and
TR-CLUSTER-042 are amended in five places between them, yet **FM-CLUSTER-039** (a failover
validates completely before it mutates anything), **-040** (a force failover removes the old
primary and transfers everything in one entry), **-041** (a graceful failover demotes rather than
removes), **-042** (a replayed failover is safe), **-043**/**-044** (role events) and **-045**
(installing a snapshot synthesizes the role change it skipped) carry no verdict. -039 and -042 in
particular are claims about the very apply arms the companion-field rule widens, and -045 is
adjacent to `admitted_stage`'s meaning across a snapshot install. Wave 0 has **not** assigned them.
**Confirm they are genuinely untouched, or assign verdicts to c31-03.**

**OQ-5 — FM-CLUSTER-100's ownership across two waves.** The row's extension enumerates four new
carried items: `NodeInfo.run_identity` and `registration_seq` / `registration_seq_gen` (c31-01,
wave 1) and `handoff_residue` (declared by c31-05 in wave 3, with its lifecycle in c31-07). Wave 0
has assigned the whole row to c31-01 and asks it to write the enumeration from the design doc,
including the residue entry, ahead of the residue type existing — which means one of its forcing
tests (`snapshot install carries the residue map`) cannot be written until wave 3. The
alternatives are (a) c31-01 lands the row with a wave-3 follow-up test, (b) the row moves to c31-05
and the identity fields go uncarried across waves 1-2, or (c) the row is split into two
amendments, which breaches the one-row-one-cluster rule. **Wave 0 recommends (a); confirm.**

**OQ-6 — where the `RefusalClass` partition is *forced*.** c31-01 makes the class a committed
apply outcome (FM-CLUSTER-047) and c31-04 consumes it in gate 3 (d)'s six-class total partition.
R4 says the class is minted with the verdict and carried in the payload. The totality of the
partition is a c31-04 property, but the *evaluation order* that defines "the first failing
conjunct" is a c31-01 property, and the two must agree exactly or a refusal selects the wrong arm.
Wave 0 has put the declared evaluation order in c31-01 and the arm mapping in c31-04. **Confirm
that split, or fold the partition into c31-01.**
