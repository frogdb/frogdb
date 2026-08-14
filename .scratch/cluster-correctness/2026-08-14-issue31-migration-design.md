# Slot migration redesign — source-authoritative-until-commit (v4)

Status: revision 4 — approved 2026-08-14, under adversarial re-review. Revision 3 resolved review v3's findings
(5C/13M/8m new); independent re-review v4 found 3 new CRITICAL / 13 MAJOR / 10 MINOR —
all three CRITICALs were collisions between or holes in v3's own fixes, not structural
defects — and 13 incompletely-resolved priors; this revision resolves all of them.
Reviews: issue31-adversarial-review-v2/-v3/-v4, job dir 2026-08-14.
Issue: [31](issues/open/31-slot-migration-redesign-source-authoritative-until-commit.md)
Origin ruling: distsys review MAJ-5 (see `.scratch/formal-spec/2026-08-13-distsys-review-rulings.md`)

## Problem

The Redis-style bulk phase deletes keys from the source per `MIGRATE`, so mid-migration
state is a split keyspace. That split is the only reason abort needs repatriation
(superseded issue 15) and the only reason a dead target is hard to abort away from
(MAJ-5: the issue-17 finalization write pause had no release path when the target dies
mid-handoff).

## Design

**The source remains the sole authority for the slot until `CompleteSlotMigration`
applies.** The target builds a shadow copy (snapshot + slot-filtered mutation stream) and
takes over atomically at commit. Abort is target-discard and is safe in every phase.

Industry grounding (full survey: issue-31 research report, 2026-08-14): Valkey 9.0 atomic
slot migration is the closest analog — source-authoritative throughout, byte-parity
cutover handshake, cutover automatic once the operator starts the migration. CockroachDB
contributes commit-time re-verification against replicated state. FoundationDB's
dual-authoritative `fetchKeys` is the documented anti-pattern (needs global MVCC + short
transactions). DragonflyDB's global client-pause finalize is rejected as too coarse.

Structural advantage worth stating: `shard = slot % num_shards`, so a slot lives in
exactly one shard. The per-slot snapshot and the drain are single-shard operations; the
protocol never crosses shards on either endpoint.

### §0 Positions, identities, and the one sequence space (v2-C6, v3 N-C2/N-C4/N-M7, v4 C2/M3/M4)

One position space for the entire protocol: the **source's replication byte offset**
(the same space the replication link and full-sync checkpoint already use). Every
position-valued quantity below — `snapshot_pos`, `drained_pos`, `target_ingested_pos`,
`target_replicas_acked_pos`, per-batch stamps, parity lag — is denominated in this
space. No RocksDB/WAL sequence appears anywhere in the protocol. (The issue-12 "the
snapshot carries its covered position, never a post-hoc global read" rule is preserved
verbatim — the covered position is the replication offset captured before the cut,
after a shard drain, exactly the shape the full-sync path uses.)

**Run identity** (N-C2): `run_id = (replid, incarnation)`. `replid` is the source's
replication history id with the atomic replid/offset pairing of replication issue 24.
`incarnation` is a per-boot token the node mints once at process start. The pair exists
because FM-REPLICATION-021 makes a plain restart keep the replid and re-advance the head
raise-only from `offset_at_save` (INV-OFFSET-2: the file may claim more than the live
head reached) — same identity, forward position, hole in the data — so replid alone
cannot signal the discontinuity. **Every restart of the source, with or without a replid
change, changes `run_id` and is an explicit discontinuity: resume is refused and the
migration is cancelled.** A position is meaningful only within its `run_id`.

**Incarnation durability** (V4-M3 — the token the whole restart-detection argument
rests on gets a stated writer and durability contract): the incarnation is a
node-durable counter in its **own file** — never the replication state file, whose save
is deliberately un-fsynced (FM-REPLICATION-021's stated non-guarantee) — incremented and
**fsynced (file and parent directory) at boot, before the node admits any client write
or proposes any command**. A node that cannot read or durably increment the counter
boots with a **freshly minted identity** (new replid — FM-REPLICATION-021's
corrupt-state-file behaviour), never a reused one. A lost increment therefore cannot
reproduce a prior `run_id`.

**The run identity is replicated state** (N-C4/N-M11): `NodeInfo` gains a
`run_identity: Option<(replid, incarnation)>` field, written by a new replicated
transition `ReportRunIdentity{node, run_id, identity_seq}`. **Proposing moments**
(V4-M4a — the FM-REPLICATION-023 one-cell-per-process identity changes, all three):
**boot**, **promotion**, and **demotion / history adoption** (FM-REPLICATION-022: a bare
`REPLICAOF` demotion ends the stint and `adopt_replication_history` replaces the replid
on link-up — a full history discontinuity that must reach the replicated field). Every
admission conjunct that mentions a run identity reads **this replicated field**, never a
node-local cell — `apply` stays a pure function of replicated state (the FM-CLUSTER-089
determinism rule, preserved). `BeginSlotMigration`'s apply captures `record.run_id` from
the source's replicated `run_identity` at apply time (defined, observable writer —
refused if the field is absent).

**Identity ordering** (V4-M4b — incarnation is constant within a boot, but
promote→demote→re-promote changes identity three times inside one boot, so incarnation
alone cannot order the reports): each node keeps `identity_seq`, a monotone counter
**persisted with the incarnation** and bumped on every identity change (boot, promotion,
demotion/adoption). `ReportRunIdentity` admission: proposer is `node` ∧
`(incarnation, identity_seq) >` the stored pair (lexicographic). A retried or reordered
report from earlier in the boot is a refused no-op — it can neither regress the field
nor spuriously cancel migrations.

**Boot ordering rule** (V4-C2b): **a node proposes no other Raft command until its boot
`ReportRunIdentity` has applied.** Without this, every source-proposed transition has a
window where the replicated field still holds the previous boot's identity and a
run-guard conjunct passes vacuously.

**Target-side heads, classified** (N-M7 — two distinct positions, per §0's own rule):

- `covered_received` — highest contiguous position received on the stream. Operand of
  the per-batch contiguity assertions (checked at receipt, §4).
- `covered_applied` — highest position **durably applied** into the shadow. The value
  reported in `ReportMigrationIngest`, the resume point after any target crash or
  session re-establishment (§4 — a target restart resumes; only a *source* restart
  cancels), and the stream's ack unit (§8). The received-but-unapplied window
  (`covered_received − covered_applied`) is lost on a target crash by design and
  re-requested at resume — never silently skipped.

Only `covered_applied` (and its replica-side floor `target_replicas_acked_pos`, §5)
ever reaches an admission predicate.

Two distinct identity counters, never conflated (v2-C3/M1):

- **`attempt_id`** — minted from the LOCKED cluster-wide generation
  `ClusterStateInner.handoff_seq` at every accepted `PrepareSlotHandoff`. Monotone,
  never reused across snapshot restore (FM-CLUSTER-086/095/100 unchanged in mechanism;
  `SlotFence` keeps this as its input). Carried by every subsequent message of that
  handoff attempt; mismatched messages are refused.
- **`migration_id`** — minted from the same generation at `BeginSlotMigration`.
  Identifies the migration across handoff attempts; keys the target's shadow store and
  every abort/discard. `ResetCluster` rewinds the generation (TR-CLUSTER-035) — the
  reset interaction is handled in §5 (N-M2/V4-M8), not ignored.

### Replicated migration record

```
{ slot, source, target, migration_id, run_id, phase,
  attempt_id: Option, snapshot_pos: Option, drained_pos: Option,
  target_ingested_pos: Option, target_replicas_acked_pos: Option,
  attempts, observations, last_observation: Option<(term, tick)> }
phase ∈ { Snapshotting, Streaming, Draining }   (terminal Complete/Aborted = record removed)
```

**Handoff residue** (V4-C1/M7/M11 — one replicated structure closes all three):
`ClusterStateInner` gains `handoff_residue: Map<(slot, migration_id),
{source, target, promoted: bool}>`. `CompleteSlotMigration`'s apply writes the entry
(with `promoted = false`); `ReportSlotPromoted` (target) sets `promoted`;
`ConfirmSlotDeleted` (source) removes it. Both maps are carried in
`ClusterSnapshot`/`from_snapshot` (FM-CLUSTER-100 extended), so neither a crash between
apply and any node-local write nor a snapshot-shipped follower can lose the pending
promotion or the pending delete.

Field writers (every field has exactly one writing transition):

| Field | Written by | Notes |
|-------|-----------|-------|
| slot, source, target, migration_id | `BeginSlotMigration` | immutable |
| run_id | `BeginSlotMigration` (from source's replicated `NodeInfo.run_identity`) | immutable; §0 |
| snapshot_pos | `RecordSnapshotPosition` (source) | `Option` — absent ≠ 0; immutable once set; phase → Streaming |
| attempt_id | `PrepareSlotHandoff` (mints from generation) | replaced per attempt |
| drained_pos | `ConfirmSlotHandoffDrained` (source) | **cleared** by `AbortSlotHandoff` |
| target_ingested_pos | `ReportMigrationIngest` (target) | `covered_applied`; monotone within run_id |
| target_replicas_acked_pos | `ReportTargetReplicaAck` (target) | source-space replica-ack floor (§5, V4-C3); monotone within run_id |
| attempts | `AbortSlotHandoff` (+1); reset by re-issued `MIGRATING` (N-m4, §6) | any failed attempt counts |
| observations | `ObserveMigration` (+1); **reset by a `ReportMigrationIngest` that advances `target_ingested_pos` to a value still below `drained_pos`** (N-M6, narrowed per V4-M2) and by phase change | replicated; survives leader change |
| last_observation | `ObserveMigration` | dedup state for the counter (N-M5) |
| handoff_residue entry | `CompleteSlotMigration` (creates); `ReportSlotPromoted` (sets promoted); `ConfirmSlotDeleted` (removes) | replicated, snapshot-carried |

### Transitions

Code-true names; `AbortSlotHandoff` (existing TR-CLUSTER-014) is the attempt-release;
`CancelSlotMigration` (existing TR-CLUSTER-015) is the whole-migration abort.
**Every transition names its proposer constraint explicitly** (V4-M10); where a
transition deliberately has none, the row says so and why.

- **`BeginSlotMigration`** (proposed by the node named `source` — the `MIGRATING` verb
  is issued on the source, §6) → record created, phase=Snapshotting, attempts=0,
  observations=0, run_id captured per §0. Admission (N-M12): proposer is `source` ∧
  (slot owned by `source` **or slot unowned** — FM-CLUSTER-032's ruled unassigned-slot
  arm, kept with its own verdict (V4-m5): a follower's slot map may legitimately be
  empty, and `Begin` on an unowned slot claims it for the source) ∧ `source != target`
  ∧ both endpoints are cluster members (FM-CLUSTER-032's `NodeNotFound` arms) ∧
  target's role is primary ∧ source's replicated `run_identity` present ∧ neither
  endpoint FAIL-flagged ∧ no open record for the slot. **Idempotency owned explicitly**
  (FM-CLUSTER-031's surviving half): a re-issued `MIGRATING` naming the same (slot,
  source, target) over an open record answers `Ok` without a new record — and resets
  `attempts` to 0 (the operator's "try again" verb, N-m4) — **admissible only while
  `phase ∈ {Snapshotting, Streaming}` and `record.run_id ==
  nodes[source].run_identity`** (V4-m7: mid-Draining it must not defuse an
  about-to-fire attempts bound, and it must not keep a stale-run record alive across
  the cancel `ReportRunIdentity` is about to deliver). `AssignSlots` refuses any slot
  with an open record.
- **`RecordSnapshotPosition{migration_id, run_id, pos}`** (source-proposed, after the
  snapshot is cut) → snapshot_pos=pos, phase=Streaming. Admission (N-M12, run/proposer
  guards per V4-C2): record exists ∧ migration_id matches ∧ phase==Snapshotting ∧
  **proposer is `record.source`** ∧ **`run_id == record.run_id ==
  nodes[record.source].run_identity`**. Duplicate proposal with the same value = no-op
  `Ok`; with a different value = refused (the field is immutable). Target ingests the
  snapshot, then the tail from `pos` exclusive.
- **`ReportMigrationIngest{migration_id, run_id, applied_pos}`** (target-proposed,
  periodically). Admission (N-M4): record exists ∧ migration_id matches ∧ run_id
  matches `record.run_id` ∧ proposer is `record.target` ∧ `applied_pos ≥` current
  `target_ingested_pos` (an equal or lower value applies as a no-op — reports are
  idempotent, refusal is reserved for identity mismatches). Writes
  target_ingested_pos=applied_pos; if it advanced **to a value still below
  `drained_pos`** (or `drained_pos` is unset), resets `observations` to 0 (N-M6,
  narrowed per V4-M2 — progress *toward* the token defers the bound; progress at or
  past the token must not, or a completable-but-never-completed record would defer it
  forever). No attempt_id: the position space is per-run, not per-attempt, so reports
  are attempt-independent. Report cadence is node-local and is never an admission
  input — only the replicated value is.
- **`ReportTargetReplicaAck{migration_id, run_id, pos}`** (target-proposed,
  periodically; **new**, V4-C3) → writes `target_replicas_acked_pos = pos`. Admission
  mirrors `ReportMigrationIngest`: record exists ∧ migration_id matches ∧ run_id
  matches `record.run_id` ∧ proposer is `record.target` ∧ `pos ≥` current value (≤ is
  a no-op). `pos` is denominated in the **source's** position space: it is the highest
  source-space batch stamp (`to_pos`, §4) through which **every replica the target
  counts** has durably applied the shadow — the target knows each fed batch's
  source-space stamp and each replica's ack, so the floor is computable node-locally
  and *attested* by this replicated write. The optional `Complete` conjunct reads only
  the replicated field — two replicated numbers, one space, deterministic at apply on
  every node.
- **`PrepareSlotHandoff`** (source-proposed) → mints attempt_id, phase=Draining, barrier
  arms on the source (per-object, issue 17/19 semantics). Full admission conjunction
  (run/proposer guards per V4-C2): phase==Streaming ∧ record matches (source, target)
  ∧ **proposer is `record.source`** ∧ **`record.run_id ==
  nodes[record.source].run_identity`** ∧ attempts < max-handoff-attempts ∧ neither
  endpoint FAIL-flagged ∧ no live attempt_id. The source *chooses* to propose at
  parity (`feed_head − target_ingested_pos ≤` parity threshold) — a scheduling
  heuristic, not a correctness input.
- **`ConfirmSlotHandoffDrained{attempt_id, run_id, pos}`** (source-proposed once its
  shard has no in-flight write below `pos` and the barrier holds) → drained_pos=pos.
  Admission (run/proposer guards per V4-C2 — this transition writes the position
  `Complete` compares against, so it carries the full guard): phase==Draining ∧
  attempt_id matches ∧ **proposer is `record.source`** ∧ **`run_id == record.run_id ==
  nodes[record.source].run_identity`**. **Drain-completeness precondition** (V4-M13):
  the source proposes only when its shard has no in-flight write below `pos` **and no
  outstanding cross-shard VLL continuation holds a lock that can still produce a write
  to the slot** — under source authority the source *acks* what it executes, so a
  continuation that slipped a shard-round-trip drain would produce an acked write
  above `drained_pos` that never reaches the target (acked-write loss, not the old
  retryable ambiguity; see §6). From this point the source's fence is **sealed**: it
  must not execute another write for the slot until it *applies* `AbortSlotHandoff`,
  `CancelSlotMigration`, or `CompleteSlotMigration`. **Coverage obligation** (N-C5):
  proposing this transition obliges the source to emit, on the migration stream, a
  coverage batch `(run_id, covered_head, pos)` — empty payload if the range holds no
  slot traffic — so the target's covered position can actually reach `pos` without
  depending on client traffic. (§4 adds the periodic coverage rule; together they make
  `Complete` genuinely traffic-independent.)
- **`CompleteSlotMigration{attempt_id, token}`** (target- **or leader-proposed**,
  V4-M2: every conjunct below is a pure function of replicated state, so the leader
  evaluates it exactly as the target can and **auto-proposes when it holds** — the
  `Draining` exit is then leader-driven and traffic-independent, not hostage to the
  target's completion path. Deliberately **no proposer conjunct** (V4-M10 verdict): the
  conjunction is sound regardless of who proposes it):

  ```
  phase == Draining
  ∧ attempt_id == record.attempt_id
  ∧ record.drained_pos == Some(token)
  ∧ record.target_ingested_pos >= record.drained_pos     // covered_applied — possession
  ∧ record.run_id == nodes[record.source].run_identity   // replicated field, §0 (N-C4)
  ∧ slot_map[slot] == record.source                      // source still owner
  ∧ record.target ∈ nodes                                // membership — FM-CLUSTER-033's
                                                         // ghost-owner guard (V4-m4)
  ∧ record.target not FAIL-flagged                       // N-m6, symmetric with Prepare
  ∧ [if cluster-migration-require-target-replica-ack]    // optional durability conjunct,
      record.target_replicas_acked_pos >= record.drained_pos
                                                         // replicated, source-space (V4-C3)
  ```

  On apply: ownership flips, `MOVED` correct, barrier release event emitted **after**
  the assignment mutation (FM-CLUSTER-092 ordering preserved), record removed, and the
  apply writes the replicated `handoff_residue` entry `{source, target,
  promoted: false}` (V4-C1/M7/M11) — the durable, snapshot-carried registration of the
  target's pending promotion and the source's pending delete. Neither deletion nor
  promotion runs inside apply.
- **`ReportSlotPromoted{slot, migration_id}`** (target-proposed; **new**, V4-M11) →
  sets `promoted = true` on the residue entry. Admission: entry exists ∧ proposer is
  the entry's `target`. Idempotent no-op if already set. Proposed by the target after
  its node-local promotion (§5) finishes.
- **`ConfirmSlotDeleted{slot, migration_id}`** (source-proposed; **new**, V4-M7) →
  removes the residue entry. Admission: entry exists ∧ proposer is the entry's `source`
  ∧ `promoted == true` (the source's delete is gated on the promotion attestation,
  V4-M11 — see §7).
- **`AbortSlotHandoff{attempt_id}`** (source- or leader-proposed; stale proposals are
  screened by the attempt_id conjunct, so no proposer conjunct is load-bearing —
  stated per V4-M10) → clears drained_pos and attempt_id, attempts+=1,
  phase=Streaming, emits the barrier-release event (FM-CLUSTER-087). Refused on
  attempt_id mismatch. If attempts ≥ max-handoff-attempts, the applying transition
  instead cancels the migration.
- **`ObserveMigration{migration_id, attempt_id, leader_term, tick}`** (leader-proposed
  each reconcile tick **while a record sits in Draining** — V4-M2 drops v3's "without
  a completable token" qualifier, so a completable-but-uncompleted record still
  accrues) → observations+=1. **Full admission conjunction** (V4-M10): record exists ∧
  migration_id matches ∧ phase==Draining ∧ `attempt_id == record.attempt_id` ∧
  `(leader_term, tick) >` `record.last_observation` — a stale observation from a
  finished attempt or a prior phase can neither count nor force an abort. Dedup
  (N-M5): the record's `last_observation` stores the last accepted pair; `tick` is a
  leader-local monotone counter; `leader_term` is the proposer's Raft term carried as
  opaque command data (N-m7 — the state machine compares the pair and consumes no
  other Raft metadata). When observations reaches
  `cluster-migration-draining-observations`, the apply forces the `AbortSlotHandoff`
  outcome. With the narrowed reset rule (V4-M2) the bound reads: **abort after N
  leader observations with no target progress below the token** — a wedged drain
  exits, a large healthy shrinking drain does not, and a record whose token is
  completable but never completed is exited by the leader's auto-`Complete` or, if the
  conjunction cannot hold (e.g. the durability conjunct with a dead target replica),
  by this bound.
- **`ReportRunIdentity{node, run_id, identity_seq}`** (proposed by each node at boot,
  at promotion, and at demotion/history adoption — §0, V4-M4) → writes
  `NodeInfo.run_identity`. Admission: proposer is `node` ∧ `(incarnation,
  identity_seq) >` stored pair (§0). Applying a run_identity change for a node that is
  the **source** of any open migration **cancels those migrations** — the replicated
  form of "source restart aborts" (§4). **A target's identity change does not cancel**
  (V4-M5 — asymmetric by design: positions are denominated in the *source's* history,
  so a source discontinuity invalidates them, while a target restart invalidates
  nothing about the position space; the target's boot reconcile resumes from
  `covered_applied` or, if its shadow is unavailable, proposes `CancelSlotMigration`
  itself, §4/§5).
- **`CancelSlotMigration{migration_id}`** (operator / source / **target** / leader —
  V4-M12: §5 assigns the target three cancel duties only it can observe — `FLUSHALL`/
  `FLUSHDB` on itself, its own memory pressure, a shadow it cannot furnish or promote
  — so it must have standing; admission is unconditional so no conjunct changes) —
  admitted in **every** phase: record removed, release event emitted if a barrier was
  armed, target (and its replicas) discard the shadow store keyed by migration_id
  (§5). Idempotent: cancelling a slot with no open migration replies `Ok`
  (FM-CLUSTER-035 preserved). A cancel that races and loses to `Complete` replies an
  error stating the migration committed.

Ordering races all resolve mechanically by the conjunctions above: a stale
`ConfirmSlotHandoffDrained` after `AbortSlotHandoff` is refused (attempt_id cleared); a
`Complete` after `AbortSlotHandoff` is refused (phase, attempt_id, drained_pos all
fail); two `Prepare`s cannot both mint (second refused: live attempt_id); a
`Confirm`/`Prepare`/`RecordSnapshotPosition` from a restarted source is refused (run
guard — the boot `ReportRunIdentity` applies before the source can propose anything
else, §0's boot ordering rule, and from then on the run guard fails for the old
record); `Cancel` beats everything except an already-applied `Complete`.

### §1 Liveness (v2-C4/C5/C8, v3 N-M6/N-M13, v4 M2)

Three layers, each with a named input the acting node **can observe**:

- **Link layer** (source-local): the migration stream session has keepalive/timeout via
  the clock seam, like every session. Dead session → source retries; resume refused
  (§4) → source proposes `CancelSlotMigration`.
- **Progress layer** (source-local — the source holds the datum: its own feed head and
  the session's ack state): strike when `covered_applied < source feed head` **and**
  the acked head has not advanced since the last check — lag that is not shrinking. K
  consecutive strikes (`cluster-migration-stall-strikes`, default 3, cadence = the
  source's periodic tick — cadence only, never admission) → source proposes
  `CancelSlotMigration`. **Strike-counter reset rule** (prior-C5 residue, now stated):
  the counter resets to 0 whenever the acked head advances and whenever the session is
  (re-)established.
- **Leader layer** (leader-local inputs only — replicated state): (a) either endpoint
  carries the replicated FAIL flag → propose `CancelSlotMigration` immediately (ruled
  criterion). Stated availability property: a leader↔source partition FAIL-flags a
  healthy serving source and aborts its migrations — safe under target-discard,
  accepted. (b) **the leader auto-proposes `CompleteSlotMigration` whenever its
  conjunction holds** (V4-M2 — every conjunct is replicated state, so the exit from a
  completable `Draining` record does not depend on the target's completion path). (c)
  the `ObserveMigration` bound — the replicated, progress-sensitive observation
  counter is the Draining exit that needs no client traffic and survives leader churn;
  with the narrowed reset rule it also fires when the token is completable but
  `Complete` is inadmissible (the durability conjunct with a dead target replica). It
  also backstops a lost overflow-abort proposal (its input is the replicated record,
  not a cross-node offset).

**Stated limit** (N-M13): the layers above cover a *dead* endpoint in any phase (FAIL
flag) and a *wedged target* (observation bound in Draining; the source's progress layer
in Streaming). A **wedged-but-listening source** in Snapshotting/Streaming — alive to
TCP probes (the spec's GAPS entry 4 liveness limitation), proposing nothing — has **no
automatic exit**; the exit is the operator (`CancelSlotMigration` via `CLUSTER SETSLOT
… STABLE` or frogctl), and the stuck record is visible in `CLUSTER MIGRATIONS` with a
static phase. The universal "no reachable state lacks an exit" claim is therefore
scoped: every *Draining* state has an automatic exit (leader auto-`Complete` when the
token conjunction holds; the observation bound, the cap breach, and the self-fence
release otherwise); pre-Draining states have automatic exits for dead endpoints and
wedged targets, and an operator exit otherwise. No layer consumes a datum its actor
cannot observe.

### §2 Parity — threshold-initiate, exact-commit

- **Initiate** (scheduling heuristic, not correctness): source proposes
  `PrepareSlotHandoff` when `feed_head − target_ingested_pos ≤
  cluster-migration-parity-threshold-bytes`.
- **Commit** (correctness): the `Complete` conjunction above. Exactness lives in two
  replicated attestations — the source's seal (`drained_pos`: "I admitted nothing past
  X") and the target's report (`target_ingested_pos ≥ X`: "I durably applied through
  X") — because neither party can attest the other's fact. The coverage obligations
  (Transitions, §4) make the target's attestation reachable without client traffic.
- Two knobs, deliberately: the parity threshold tunes when to attempt cutover; the
  barrier byte cap (§3) bounds client impact during it.

### §3 Drain bound — fail-closed byte cap (v2-C2, v3 N-M1)

During Draining, writes to the migrating slot are **held** (queued, byte-accounted) up
to `cluster-migration-barrier-max-bytes`. On breach:

- the source **does not execute the held writes**. Writes beyond the cap are answered
  `-TRYAGAIN` immediately; already-held writes remain held;
- the source proposes `AbortSlotHandoff{attempt_id}` and keeps its fence until that
  proposal **applies** (fail-closed);
- on apply: phase→Streaming, attempts+=1, barrier releases, held writes execute at the
  source and are acknowledged normally.

**Named invariant: the source's local fence is never weaker than the replicated phase
implies.** A node-local decision may fence *more*, never less.

Held-write disposition on every exit (every held client gets a real reply):

| Exit | Reply to held writes |
|------|---------------------|
| `Complete` applies | pinnable writes: `MOVED <slot> <target>` (FM-CLUSTER-092 amended); **unpinnable held batches** (FM-CLUSTER-096: straddling slots or keyless): `-TRYAGAIN` — one `MOVED` slot cannot describe them; the client's retry re-routes per key (N-m3) |
| `AbortSlotHandoff` applies | execute at source, acknowledged normally |
| Cap breach, pre-apply | beyond-cap writes: `-TRYAGAIN`; held set: unchanged until apply |
| `CancelSlotMigration` applies | execute at source, acknowledged normally (release event) |
| **Self-fence latch arms** (TR-CLUSTER-026: no Raft leader contact within an election timeout) | answer the **entire held set** `-TRYAGAIN` and **keep the fence** (N-M1) — a sealed source that cannot apply must not make held clients wait out a partition; erroring a held write is *more* fenced, not less, so the §3 invariant holds and the sealed rule ("no further execution until an exit applies") is untouched |
| Client disconnects while held | held entry dropped with the connection (no reply owed) |
| `CLIENT UNBLOCK`/`KILL` on a held client | `-UNBLOCKED` / connection close, per blocking rows |
| Failover prunes the record | release event (FM-CLUSTER-087); writes follow new topology |

`Draining`'s exits: `Complete` (traffic-independent — the coverage obligations make the
token reachable on a quiet source, and the leader auto-proposes once it is), cap breach
(traffic-driven), the progress-sensitive observation bound (traffic-independent), and
the self-fence client release above.

### §4 Snapshot + stream — run-identified, coverage-stamped (v2-C6, v3 N-C2/N-C5/N-M7, v4 M1/M5/M9)

- The slot snapshot is cut after a shard drain; it **carries** its covered replication
  offset `snapshot_pos` (issue-12 rule, preserved verbatim). The mutation stream is
  slot-filtered at the source feed and starts at `snapshot_pos`, exclusive.
- Every batch is stamped `(run_id, from_pos, to_pos)` with **coverage** semantics: the
  range covers filtered-out traffic too, so a quiet slot on a busy node still advances
  the target's covered position. **Periodic coverage rule** (N-C5): the stream emits a
  coverage batch (empty payload) on its keepalive cadence regardless of traffic, so
  `covered_received` tracks the source head even on a fully quiet node.
  **Zero-advance batches are the keepalive's normal state on a fully quiet source**
  (V4-M1): a coverage batch with `to_pos == covered_received` is an admitted no-op,
  not a fault. Cadence is node-local and never an admission input — only the resulting
  replicated report is.
- Target assertions at **receipt**, against `covered_received`, in order — **each with
  its own scoped consequence** (V4-M1: "discard" is not one action):
  1. `run_id` equality. Mismatch is an explicit discontinuity → tear down the session
     and propose `CancelSlotMigration`; the shadow is discarded on the applied cancel
     (§5 — never unilaterally at receipt).
  2. `from_pos == covered_received`. A forward gap → **drop the batch** and re-request
     resume at `covered_applied` (session-level recovery; the shadow is untouched).
  3. `to_pos ≥ covered_received`, where equality (the zero-advance coverage batch) is
     an admitted no-op. Strict regression `to_pos < covered_received` → tear down the
     session and re-attempt resume — detected, never silently re-applied; the shadow
     is untouched.
- **Resume rule** (N-C2 — both clauses required): after any session re-establishment or
  target restart, the target requests resume at `(run_id, covered_applied)`. The source
  admits the resume iff **(a)** its current run identity equals the requested `run_id`
  — and because `incarnation` changes on every source restart, a restarted source
  always refuses, even though FM-REPLICATION-021 keeps its replid and re-advances its
  head forward over a hole — and **(b)** the **per-migration backlog's armed floor** ≤
  `covered_applied` (V4-M9 — the resume is served from the migration stream's own
  bounded backlog, §8, *not* the replica-feed backlog; it is that backlog's floor,
  armed per FM-REPLICATION-014's rule with the `>=`-at-the-floor boundary reused
  verbatim, that defines **"history intact"** here — "a resume is never served over a
  hole"). The per-migration backlog **continues to accumulate while the session is
  dead**, up to its cap; overflow → the source proposes `CancelSlotMigration` (§4
  backpressure). Either refusal → `CancelSlotMigration`. Never resume across a run_id
  change.
- **Endpoint failover / restart rules** (V4-M5 — asymmetric, and §0/§4/§5 now say the
  same thing): a failover naming the **source** aborts the migration (prune per
  TR-CLUSTER-018/FM-CLUSTER-036; release events paid, FM-CLUSTER-087; a successor
  never inherits a barrier — FM-CLUSTER-104 stays same-node-only). A failover naming
  the **target** mid-ingest aborts the migration. A **source restart** aborts via
  `ReportRunIdentity` (the incarnation change cancels migrations the node *sources*,
  Transitions) — positions are denominated in the source's history, so its
  discontinuity invalidates them. A **target restart resumes**: the shadow and
  `covered_applied` survive in the target's own persistence (§5), the position space
  is untouched, and the target's boot reconcile requests resume at `(run_id,
  covered_applied)` — subject to the source's resume admission above; a target that
  boots without its shadow (no persistence, storage loss) proposes
  `CancelSlotMigration` itself. A transient blip never destroys ingest progress.
  Retargeting is future work, deliberately out of scope. These rows — not
  target-discard alone — are what supersedes issue 15.
- **Backpressure**: per-migration bounded backlog (bytes) on the source, separate from
  the replica feed. Overflow → source drops the session and proposes
  `CancelSlotMigration`; the observation bound backstops a lost proposal. On session
  death the target keeps its shadow and awaits resume; discard happens only on
  replicated cancel/abort.

### §5 Target-side shadow, discard, and promotion (v2-C7, v3 N-C3/N-M2/N-M10/N-m1/N-m5, v4 C1/C3/M6/M8/M11/M12)

- The shadow store is keyed by `(slot, migration_id)` and lives outside the target's
  main keyspace. By construction it is invisible to `SCAN`, `KEYS`, `DBSIZE`,
  `RANDOMKEY`, `INFO keyspace`, and RDB/AOF of the main keyspace. TTLs are stored,
  never enforced during ingest; **expiry interaction stated** (N-m5): per
  FM-REPLICATION-030 each node expires on its own clock and no expiry `DEL` propagates
  on this stream, so the source's logical expiries never reach the shadow — the copies
  converge through post-promotion lazy expiry, exactly as a promoted replica converges.
  Eviction never selects shadow keys — target memory pressure instead **aborts the
  migration** (the target proposes `CancelSlotMigration` — it has standing, V4-M12).
- **The shadow is durable and full-sync-visible** (V4-M6 + M5): the shadow store —
  keys, `covered_applied`, and its `(slot, migration_id)` tag — is part of the
  target's **own persistence** (this is what makes a target restart a resume, §4) and
  part of the target's **full-sync payload**: a replica that attaches mid-ingest, or
  re-syncs past the backlog floor (`+FULLRESYNC`, FM-REPLICATION-014), receives the
  shadow with the base — so every counted replica can promote at `Complete`, and a
  post-`Complete` failover never lands on a replica holding the base without the
  shadow. A target that cannot furnish the shadow to a replica it counts (storage
  error) proposes `CancelSlotMigration` (V4-M12). The main-keyspace invisibility list
  above is scoped to the *main* keyspace's surfaces; the shadow has its own
  persistence and replication representation, stated here.
- `FLUSHALL`/`FLUSHDB` on the target aborts open migrations targeting it (the target
  proposes the cancel — V4-M12) rather than silently corrupting the shadow.
  **`RESTORE` — or any key write — into the target's main keyspace for an importing
  slot never reaches an importing-specific check** (V4-m1 rescopes N-m1's row): under
  source authority the target does not own the slot, `ASKING` is a no-op (§6) and
  FM-CLUSTER-027's exemption is retired, so **routing answers `MOVED <slot> <source>`
  first** — the one reply, stated as the winning gate. Nothing can be written into the
  main keyspace of an importing slot on the target, so the shadow's promotion
  precedence is never contested; the row asserts that, not a second refusal.
- **Target-replica durability attestation** (V4-C3 — the knob's conjunct, made
  replicated and same-space): during ingest the target stamps every batch it feeds its
  replicas with the batch's source-space `to_pos`; each replica's ack therefore yields
  a source-space floor, and the target periodically proposes `ReportTargetReplicaAck`
  (Transitions) carrying the minimum across the replicas it counts. `Complete`'s
  optional conjunct reads `record.target_replicas_acked_pos ≥ record.drained_pos` —
  two replicated numbers in the one §0 space; no node-local state, no cross-space
  comparison, deterministic on every applier. `Complete` does **not** require
  target-replica parity by default; the residual window — target fails over between
  `Complete` and its replicas catching up on the tail — is an explicit accepted-mode
  row. **Stated honestly** (N-M10): this window is *not* TR-CLUSTER-019's shape,
  because `WAIT` is no escape hatch here — a write acked on the source and
  `WAIT`-confirmed against the *source's* replicas moves at `Complete` to a
  replication group that never counted it. The row states plainly: **by default no
  per-write durability escape hatch exists across a slot migration**. Operators who
  need one enable `cluster-migration-require-target-replica-ack`, which adds the
  optional conjunct at the cost of cutover latency (and, with a dead target replica,
  of the migration aborting via the observation bound rather than completing — §1).
- **Promotion** (N-C3, hardened by V4-C1/M11): the shadow becomes the live keyspace as
  a **node-local, idempotent, resumable consequence** of the applied
  `SlotMigrationCompleted` event — never inside apply. Promotion is a **metadata
  operation** — the shadow region is re-labelled as the slot's live data (the store is
  slot-keyed already; no per-key copy) — so the window is O(1), not O(keys).
  Fail-closed belt-and-braces regardless: **from the instant `Complete` applies until
  promotion completes, the target answers requests for the slot `-TRYAGAIN`** — it
  never serves the slot from its (empty) main keyspace, so no client can read nil for
  a live key or write a value the promotion would clobber. **The target's replicas
  promote on the same applied event and answer `-TRYAGAIN` for `READONLY` reads of the
  slot during their own promotion window** (V4-m6). A target-side reconcile resumes an
  interrupted promotion at boot (the replicated residue entry survives every crash and
  snapshot, so the work is never lost — V4-M7's class). When promotion finishes, the
  target proposes `ReportSlotPromoted` (Transitions) — the replicated attestation the
  source's delete waits for (§7).
  **Failed promotion has a defined outcome** (V4-M11): if promotion fails rather than
  being interrupted (storage error; a shadow the target cannot re-label), the residue
  entry stays `promoted = false`, the slot is owned by the target but unserved
  (`-TRYAGAIN`), and — because the source's delete is gated on the attestation — **the
  source still holds a complete copy**. The recovery verb is the operator re-assigning
  the slot to the source (`CLUSTER SETSLOT <slot> NODE <source>` / frogctl): admissible
  while a residue entry for the slot exists with `promoted == false` (a race with
  `ReportSlotPromoted` is resolved by Raft order); its apply re-assigns the slot,
  removes the residue entry, and the target's discard reaper then reclaims the shadow.
  The failed state is visible (residue entries in `CLUSTER MIGRATIONS`, §9), bounded
  by an operator action, and never a restore-from-backup.
- **Discard** (V4-C1 — the reaper must never race the promotion for a committed
  shadow): a shadow is deleted **only** when its `(slot, migration_id)` has **no live
  migration record and no `handoff_residue` entry**. A `Complete`-terminated shadow
  always has a residue entry from the same apply (written inside apply, so there is no
  window), is **consumed by promotion, never by discard**, and becomes reapable only
  after the residue entry is removed (`ConfirmSlotDeleted`, or the failed-promotion
  re-assignment above). Discard triggers: the applied `CancelSlotMigration` /
  terminal-abort outcome (the normal case — propagated to the target's replicas as a
  shadow delete-range), plus a **level-triggered sweep** on boot and on every observed
  change to the replicated migration/residue sets, deleting any shadow matching the
  predicate above (N-M2's orphan coverage, now with the residue guard).
  **`ResetCluster` is an explicit shadow-discard trigger on every node** (N-M2):
  TR-CLUSTER-035 rewinds the `handoff_seq` generation to 0, so ids become mintable
  again; discarding all shadows at reset closes the contamination path for every node
  that applies the reset. A node that was down or partitioned across the reset can
  still rejoin holding an orphan shadow whose id collides with a *live* record —
  which the level sweep must not touch — so **Begin-time collision is handled by
  discard, not refusal** (V4-M8): the target **discards any existing shadow store for
  its `(slot, migration_id)` before first ingest** — a shadow for a migration whose
  ingest has not begun cannot be that migration's — closing the wedge v3's
  refuse-rule created.

### §6 Client-visible semantics (v2-M3/M6, v3 N-M3/N-M9/N-m2/N-m3, v4 M13/m1/m6/m7)

Clients never observe a split slot. No `ASK` phase exists (Redis deviation, documented
as an improvement). `MOVED` is correct only after `Complete` applies.

- **In-flight MULTI/EXEC and scripts at barrier-arm time: run to completion; their
  writes count into `drained_pos`.** This inverts locked FM-CLUSTER-092/093/094/095's
  redirect-don't-ack semantics — correct under source authority, and safe because
  `Complete` requires the target to have applied them. **FM-CLUSTER-095's two arms get
  explicit verdicts** (N-M3): the `-TRYAGAIN Slot <slot> finalization in progress` arm
  (refuse a command validated pre-prepare, executed post-prepare) is **retired** —
  under source authority executing and acking it is correct; the
  ownership-already-moved arm (`MOVED`/`CLUSTERDOWN` after the epoch actually changed)
  is **kept** — that is FM-CLUSTER-037's window, unchanged. The spec update enumerates
  which of the row's forcing tests survive (those exercising the second arm) and which
  retire with the first. **FM-CLUSTER-096's cross-shard VLL continuation hole is
  carried forward with its consequence restated** (V4-M13): under the old semantics a
  continuation that slipped the barrier cost a retryable unacked write; under source
  authority the source *acks* what it executes, so the same slip would be an
  **acknowledged write above `drained_pos` that never reaches the target** — lost at
  `Complete`. The containment is `ConfirmSlotHandoffDrained`'s drain-completeness
  precondition (Transitions): the source does not seal while an outstanding
  cross-shard continuation can still write the slot, making the drain a true barrier
  rather than a shard round trip. The row records both the changed consequence and
  the precondition that contains it.
- **FM-CLUSTER-028's key-presence probe collapses to "serve locally, always"** — the
  source holds every key of the slot until Complete. Row rewritten to state the
  simplification.
- **Blocked clients** (BLPOP family) on the migrated slot: woken with `MOVED` at
  `Complete` — FM-CLUSTER-038 kept, explicitly.
- **WATCH**: FM-CLUSTER-029 kept; EXEC after cutover fails with `MOVED`.
- **SCAN cursors**: node-scoped; a completed migration moves keys to a node the old
  cursor will never visit, so a key present throughout MAY be missed — documented
  honestly.
- **Cutover is automatic** (N-M9 — stated as the deviation it is): once the operator
  issues `MIGRATING` (= `BeginSlotMigration`), the source auto-prepares at parity and
  the target — or the leader (V4-M2) — proposes `Complete`; the slot moves without a
  second operator action. This deviates from the Redis reshard flow — where nothing
  moves until `SETSLOT NODE` — and matches Valkey 9.0's atomic migration, where
  starting the migration is the operator's one decision. Operator control is:
  visibility via `CLUSTER MIGRATIONS`, cancellation via `STABLE`/frogctl at any
  pre-`Complete` moment, and the optional durability conjunct (§5). `SETSLOT NODE`
  survives as a compat verb: with an open matching record it proposes `Complete`
  eagerly (replying `-TRYAGAIN` if the conjunction does not yet hold — harmless from
  tooling, a no-op in the automatic flow); **with a `handoff_residue` entry for the
  slot at `promoted == false`, `SETSLOT NODE <source>` is the failed-promotion
  rollback verb** (§5); with no open record or residue, bare `NODE` on a non-migrating
  slot remains the existing topology-repair verb, and `AssignSlots`' refusal of slots
  with open records prevents it bypassing a live migration.
- **Operator surface**, rest: `CLUSTER SETSLOT <slot> MIGRATING <target>` =
  `BeginSlotMigration` (one-sided, issued on the source; `IMPORTING` on the target is
  a no-op ack for tooling compat — FM-CLUSTER-031's two-sided handshake retired;
  re-issued `MIGRATING` = `Ok` + attempts reset, **pre-Draining and current-run only**
  (V4-m7), Transitions). `STABLE` = `CancelSlotMigration` (idempotent `Ok`,
  FM-CLUSTER-035 preserved). `MIGRATE`/`RESTORE` survive as key-level commands but
  resharding no longer uses them (`RESTORE` into an importing slot on the target is
  answered `MOVED <source>` by routing — §5, V4-m1); `ASKING` is accepted as a no-op
  (`+OK`); `-ASK` is never emitted. Deviation rows for all three. `CLUSTER
  SLOTS`/`SHARDS`/`NODES` render the slot under the source until `Complete`; no split
  markers ever appear.
- **`-TRYAGAIN` inventory** (N-m2, revised per V4-m1/m6): cap-breach fail-closed
  (§3); self-fence held-set release (§3); unpinnable held batch at `Complete` (§3,
  N-m3); `SETSLOT NODE` before the conjunction holds (§6); target pre-promotion
  refusal (§5); **target-replica pre-promotion refusal of `READONLY` reads** (§5,
  V4-m6). The v3 `RESTORE`-into-importing entry is removed — that request is answered
  `MOVED` by routing before any importing check (V4-m1). FM-CLUSTER-095's
  finalization `-TRYAGAIN` is retired (above); FM-CLUSTER-091's drain-wait refusal is
  retired with it — under source authority nothing is refused while the source still
  owns the slot: writes are held or acked, never bounced.

### §7 Source-side deletion — replicated residue, attestation-gated (v2-M13, v3 N-C1, v4 M7/M11)

Deletion of the slot's keys on the source is an **event-driven, node-local consequence**
— never inside the Raft apply — but its **registration is replicated** (V4-M7):
`Complete`'s apply writes the `handoff_residue` entry (Transitions) *inside the apply*,
so the pending delete survives any crash at any point (the entry is in the replicated
state and every `ClusterSnapshot`; a snapshot taken after `Complete` still carries it —
v3's node-durable work item had a crash window between apply and its separate write,
and no backstop once the record was gone). **The reaper consumes exactly this list,
never a global predicate** (N-C1): it deletes the keys of slots whose residue entries
name this node as `source` **and have `promoted == true`** (V4-M11 — the delete is
gated on the target's replicated promotion attestation, so a failed promotion always
leaves the source's copy intact for the §5 rollback verb), then proposes
`ConfirmSlotDeleted` to clear the entry. Resumable and idempotent; crash-mid-delete
residue is covered because the entry is replicated. **The reaper has no other
trigger**: it never evaluates "slots I do not own" — a predicate that would delete
every replica's entire dataset (slot ownership names primaries, so a replica owns zero
slots), every legitimately-empty-map follower's (FM-CLUSTER-032's invariant: bootstrap
assigns slots locally, not through Raft), and a just-demoted node's (TR-CLUSTER-018 +
issue 20's demote-don't-remove). Stated guards, belt-and-braces: on a node whose role
is Replica the reaper **defers** — it never runs while demoted (replicas receive the
delete-range through the feed) but the entry persists and the delete resumes if the
node is later re-promoted (V4-M7 — v3's "never on a Replica" permanently stranded the
list on a demoted source), and an empty residue list means no deletion, whatever the
slot map says.

Keyspace notifications are suppressed for migration deletes; the deletes replicate to
the source's own replicas as a bounded-rate delete-range. Barrier-release ordering:
release fires after the assignment mutation and before deletion begins — a woken write
sees `MOVED`, never a half-deleted locally-served slot (FM-CLUSTER-092 ordering
preserved).

### §8 Migration stream session; the replica-feed hold is retired (v3 N-M8)

The mutation stream is its **own session family**, not a replica session: it shares the
wire framing/backlog code but has its own ack unit (`covered_applied`, §0), its own
keepalive (which also drives the periodic coverage batches, §4), and its own bounded
backlog — **which is also what serves a resume** (V4-M9): resume is answered from the
per-migration backlog, whose armed floor is the clause-(b) test in §4's resume rule;
the replica-feed backlog is never consulted.

**FM-CLUSTER-097's node-wide replica-feed hold is retired, with its purpose
re-derived** (N-M8). The hold existed as the companion of FM-CLUSTER-095's
fenced-but-applied writes: a write whose client was told to retry elsewhere must not
ship to replicas. Under source authority that premise is gone — §6 retires the refusal
arm, and §3's barrier holds writes **before execution**, so a fenced write never enters
the feed at all: there is nothing unshippable to hold back. Writes executed before the
barrier armed are ≤ `drained_pos`, legitimately owned and acked, and ship normally. The
hold's costs (node-wide, all slots; duration bounded only by reconcile cadence; a
TR-CLUSTER-016 cap breach disconnects a session that reconnects into the same hold)
therefore buy nothing, and the mechanism — including the issue-12 `ReplicaFeedGate` —
is removed by this design. FM-CLUSTER-097 is rewritten to assert the absence: *no
migration state ever holds the replica feed*. TR-CLUSTER-016's byte cap remains as the
feed's ordinary backpressure bound, unconnected to migration.

### §9 Observability

- **Phases** include `Snapshotting`: the operator's first question — "still shipping
  the snapshot or tailing near parity" — is answerable from the phase gauge.
- **Metrics**: per-migration phase; lag bytes (feed head − covered_applied);
  streamed/covered totals; attempts; observations; held-write count and held bytes;
  shadow-store bytes (target); target-replica-ack lag (drained_pos −
  target_replicas_acked_pos, when the knob is on); residue-entry count and age,
  labeled by `promoted` state (a stuck `promoted == false` entry is the
  failed-promotion signal, §5); dual-storage overhead; complete/cancel counters
  labeled by reason (operator, stall, FAIL, observation-bound, overflow,
  run-id-change, restart, failover, flush, memory, shadow-unavailable).
- **`CLUSTER MIGRATIONS`**: admin-gated in the FM-CLUSTER-061..064 fail-closed class;
  RESP3 map reply, one entry per open record **and one per `handoff_residue` entry**
  (slot, source, target, migration_id, promoted): slot, source, target, migration_id,
  phase, attempt_id, attempts, observations, snapshot_pos, drained_pos,
  target_ingested_pos, target_replicas_acked_pos, lag. Served from replicated state on
  any node (follower-safe; values may lag the leader — documented). Target-side
  node-local detail (ingest position, last apply error, shadow bytes, promotion state)
  appears in `INFO` section `migrations` and the debug web page on the target.
- **frogctl**: `frogctl cluster migrations` mirrors the command; `frogctl cluster
  migrate-slot --cancel` drives `CancelSlotMigration`; re-issuing the migrate verb on
  a max-attempts migration retries it (attempts reset, §6); the failed-promotion
  rollback (§5) is `frogctl cluster assign-slot --to <source>`.
- **Events**: cluster event-log row per transition and per cancel (with reason).
- **Grafana**: migration panel via the dashboard generator (never the JSON).

### Cost (v3 N-m8, v4 m9)

**Transient overhead** while one slot migrates: `(1 + R_target) ×` slot bytes (the
target's shadow plus its replicas' shadows), plus the bounded stream backlog and up to
`cluster-migration-barrier-max-bytes` of held writes. **Total residency** counting the
copies that exist anyway: `(2 + R_source + R_target) ×` slot bytes (V4-m9 — source
group and target group each hold primary + replica copies) — the source group holds
the slot throughout and then absorbs the delete-range. The spec states both numbers;
the *overhead* figure is the capacity-planning bound.

## Config

| Knob | Default | Denomination |
|------|---------|--------------|
| `cluster-migration-parity-threshold-bytes` | 1 MiB | bytes (initiate heuristic, §2) |
| `cluster-migration-barrier-max-bytes` | 4 MiB | bytes (held-write cap, §3; resolves the issue-29 ambiguity — the surviving issue-17 bound) |
| `cluster-migration-max-handoff-attempts` | 3 | attempts (any failed attempt counts; reset by re-issued `MIGRATING`) |
| `cluster-migration-stall-strikes` | 3 | source-local strikes (§1, reset on progress or session re-establishment) |
| `cluster-migration-draining-observations` | 3 | leader observations with no target progress below the token (§1) |
| `cluster-migration-backlog-max-bytes` | 64 MiB | bytes (per-migration stream backlog, §4/§8 — also the resume buffer) |
| `cluster-migration-require-target-replica-ack` | off | optional `Complete` durability conjunct via `ReportTargetReplicaAck` (§5, N-M10/V4-C3) |

**Zero wall-clock in any *replicated admission* predicate** (V4-m8 — scoped to what §1
actually claims); node-local timers drive cadence and fail-closed stops (the link
layer's keepalive timeout, the strike and observation cadences) — legitimate under the
settled ruling: fail-closed, node-local, never an admission input.

## Spec / impl blast radius — full verdicts

Every touched row gets an explicit verdict in the spec change; summary:

- **Rewritten**: TR-CLUSTER-010..013 (phase machine above); TR-CLUSTER-014
  (`AbortSlotHandoff` — clears drained_pos/attempt_id, increments attempts);
  TR-CLUSTER-015 (`CancelSlotMigration` — repatriation precondition retired,
  target-discard + release events, target added to the proposer set); TR-CLUSTER-016
  (V4-m2 — its Precondition names the slot-scoped write barrier §8 deletes; rewritten
  to the ordinary replica-feed backpressure bound with a precondition that exists);
  TR-CLUSTER-035 (`ResetCluster` — adds the shadow-discard trigger, §5);
  FM-CLUSTER-026/027/028 (importing gates: probe collapse; RESTORE exemption →
  routing-`MOVED` precedence row, §5/V4-m1); FM-CLUSTER-029 (WATCH, re-derived);
  FM-CLUSTER-031/032/033/035 (SETSLOT surface incl. owned idempotency + membership
  arms; FM-CLUSTER-032's unassigned-slot arm **kept** with its own verdict — V4-m5;
  FM-CLUSTER-033's membership check appears as `Complete`'s `target ∈ nodes` conjunct
  — V4-m4); FM-CLUSTER-036 + TR-CLUSTER-017/018/019 (failover/restart naming an
  endpoint — §4 rules, now asymmetric per V4-M5); FM-CLUSTER-037 (commit-to-apply
  window, now with the pre-promotion refusal, §5); FM-CLUSTER-084 (admission
  conjunctions above, incl. run/proposer guards on every position writer — V4-C2);
  FM-CLUSTER-086 (attempt stamping; **the generation is incremented by `Begin` and by
  `Prepare`** — V4-m10: the row's "incremented on every accepted `PrepareSlotHandoff`"
  text is rewritten so `SlotFence`'s unminted-seq-0 reasoning still describes the
  counter's actual behaviour); FM-CLUSTER-087 (release events from
  Cancel/Abort/Complete/prune/self-fence); FM-CLUSTER-089 (V4-m3 — **rewritten, not
  retired**: with the deadline mechanism gone the row asserts the determinism rule
  directly — no replicated admission predicate reads node-local state — keeping the
  settled ruling's locked home and its forcing tests
  `handoff_deadlines_are_pure_functions_of_replicated_data` /
  `handoff_now_ms_reads_the_clock_seam` re-pointed at the new conjunctions);
  FM-CLUSTER-090/091 (barrier action; drain-wait `-TRYAGAIN` retired — held or acked,
  never bounced); FM-CLUSTER-092/093/094 (inversion under source authority, §6);
  FM-CLUSTER-095 (arm split: finalization-refusal arm retired, ownership-moved arm
  kept; SlotFence generation mechanism unchanged); FM-CLUSTER-096 (unpinnable batches:
  parked-batch disposition at each exit, §3; **consequence restated + drain-covers-
  continuations containment** — V4-M13, §6); FM-CLUSTER-104 (same-node re-arm only;
  successor never inherits).
- **Retired**: FM-CLUSTER-085 (handoff lease — its property, "a dead finalizer cannot
  wedge a slot", is re-provided by the observation bound *plus the leader
  auto-`Complete`* (V4-M2), which together exit every Draining state; replacement row
  states this); **FM-CLUSTER-097 + the `ReplicaFeedGate`** (§8 — purpose re-derived to
  nothing under source authority; row rewritten to assert the absence of migration
  feed holds).
- **Unchanged, stated**: FM-CLUSTER-038 (blocked-client wake at Complete);
  FM-CLUSTER-095's SlotFence generation input; FM-CLUSTER-100 (generation survives
  snapshots — extended to the new record fields, `NodeInfo.run_identity`, and
  `handoff_residue`); TR-CLUSTER-008/009 (open-record refusal of `AssignSlots` —
  strengthened to all phases); TR-CLUSTER-026 (self-fence — gains the held-set release
  row, §3); TR-CLUSTER-034 (per-node arm/release reaction).
- **New rows**: `NodeInfo.run_identity` + `ReportRunIdentity` incl. the three
  proposing moments, the `(incarnation, identity_seq)` ordering, the boot ordering
  rule, and source-side-only cancellation (§0/Transitions — V4-M4/M5/C2); incarnation
  durability contract (§0 — V4-M3); target ingest/resume + `(run_id, position)`
  receipt assertions with per-assertion consequences incl. the zero-advance no-op
  (§4 — V4-M1) and the per-migration-backlog "history intact" floor (§4 — V4-M9);
  coverage obligations — drain-flush + periodic (§4/Transitions); received-vs-applied
  head definitions (§0); `ReportMigrationIngest` admission (Transitions);
  `ReportTargetReplicaAck` + `target_replicas_acked_pos` (§5/Transitions — V4-C3);
  `handoff_residue` + `ReportSlotPromoted` + `ConfirmSlotDeleted` + the
  attestation-gated delete + failed-promotion rollback (§5/§7/Transitions —
  V4-C1/M7/M11); progress-sensitive observation bound (narrowed reset) +
  `last_observation` dedup + full `ObserveMigration` conjunction (Transitions —
  V4-M2/M10); leader auto-`Complete` (§1/Transitions — V4-M2); sealed-fence /
  local-fence-never-weaker invariant + self-fence held-set release (§3); held-write
  disposition table incl. unpinnable batches (§3); target-discard +
  residue-guarded level sweep + reset-discard + Begin-time discard-then-ingest
  (§5 — V4-C1/M8); shadow durability + full-sync payload row (§5 — V4-M6); shadow
  replication through the target feed + honest no-escape-hatch row + replicated
  durability conjunct (§5); shadow promotion + pre-promotion refusal incl. the
  replica window (§5 — V4-m6); FLUSH/memory-pressure aborts with the target as
  proposer (§5 — V4-M12); expiry-convergence row (§5); replicated-residue deletion +
  defer-while-Replica guard + notification suppression + ordering (§7);
  migration-stream session family incl. resume-from-own-backlog (§8);
  automatic-cutover deviation row (§6); `CLUSTER MIGRATIONS` gating/reply incl.
  residue entries (§9); operator-exit row for pre-Draining wedged source (§1);
  Redis-deviation rows: no `ASK`, `ASKING` no-op, `MIGRATE` not used for resharding,
  no split markers.
- **Cross-tracker**: issue 15 closes only when §4's endpoint-failover/restart rows
  land; spec-gaps issue 12 (watermark carries covered position — landed `eedb76d0`) is
  the snapshot-position substrate; replication issue 24 (replid/offset pairing) is the
  replid half of `run_id`; FM-REPLICATION-014 (backlog floor — reused as the
  per-migration backlog's floor rule, §4), -021 (restart identity — plus the
  incarnation file's independence from its un-fsynced state file, §0), -022
  (demotion/adoption as an identity moment, §0 — V4-M4), -023 (identity cell), -030
  (per-node expiry) and INV-OFFSET-2 are load-bearing dependencies, cited not
  contradicted; hardening-2 rework 12's `ReplicaFeedGate` is **removed** by §8 — its
  issue gets a superseding note; cluster issue 16 (`AssignSlots` refusal) is subsumed;
  issue 29's cap ambiguity resolved by the config table; FM-CLUSTER-096's cross-shard
  VLL continuation hole remains open, restated **with its changed consequence and its
  containment** (§6 — V4-M13).

## Quint rework (v3 review adopted in full; v4 adequacy audit adopted)

`specs/quint/cluster_migration_failover*.qnt` (now four files):

- Keep the model's `handoff_seq` variable and `feed_bytes` / `disconnected_feed`, now
  modelling ordinary backpressure (§8). **`inv_handoff_seq_never_reused` is
  re-keyed by reset epoch** (V4 audit, ext. 10 was self-defeating as written):
  `spent: Set[(reset_epoch, seq)]` — TR-CLUSTER-035 legitimately rewinds the counter,
  so a post-reset re-mint of a spent seq is *correct* behaviour, not a violation;
  shadows are tagged with the epoch they were minted under, so an orphan shadow whose
  tag predates the current epoch is a distinguishable reachable state.
- Drop `repatriating`, `inv_abort_repatriates`, `completeRepatriation`.
- Extensions, each named for the finding it must catch (acceptance bar: reverting the
  design fix in the model must violate the named property):
  1. `target_copy` / `source_log` high-water vars + `inv_no_acked_write_lost` (v2-C1).
     **`confirmDrained` carries a position drawn from the same tagged space as
     `source_log`, and `prepareHandoff`/`confirmDrained` carry the run tag** (V4-C2):
     a `Complete` admitted on a cross-tag comparison must violate the invariant; the
     reachable space sequences `sourceRestart` *before* `prepareHandoff`, not only
     mid-Streaming.
  2. `fenced` tracked separately from `phase` + `localReleaseOnCapBreach` +
     `inv_complete_requires_fenced_source` (v2-C2).
  3. Attempt stamping on confirm/release/complete/cancel +
     `inv_complete_requires_draining_phase` (v2-C3).
  4. `reconcileTick` + replicated `observations` with the narrowed progress-reset +
     the liveness pair (v2-C4/N-M6): bounded witness `witnessDrainingHeldForKTicks`
     **and invariant** `inv_progressing_migration_never_aborts`.
  5. `sourceFailover` (identity change + backward jump) **and** `sourceRestart`
     (N-C2's live hazard: **same** replid, position dropped to a save point, then
     re-advanced forward over a hole) + `inv_stream_history_sound` strengthened from
     contiguity to history soundness via per-position history tags. **Plus
     `targetRestart`** (V4-M5): encodes the asymmetric rule — a target restart
     resumes from `covered_applied` and must not violate `inv_no_acked_write_lost`;
     the model is the cheapest place to show "resume" and "cancel" cannot both be
     consistent with it.
  6. `target_replica_copy` + `inv_target_replicas_hold_committed_slot` (v2-C7).
     **Plus `attachTargetReplica(n)`** (V4-M6): a replica attaching mid-`Streaming`
     with `target_replica_copy = 0`, then a target failover onto it — the existing
     invariant must fail unless the full-sync payload carries the shadow.
  7. Per-node keyspace `source_keys: NodeId -> Set[SlotId]` + `reapSlots(n)` action +
     `inv_node_keeps_slots_it_owns` (N-C1 — the model must be able to express
     over-deletion to prove its absence).
  8. **`shadow: SlotId -> Option[MigrationId]` as a first-class variable** (V4-C1 —
     the audit's highest-value change: without a shadow variable the reaper/promotion
     race is unrepresentable): `promoteShadow(s)` requires a non-empty shadow and sets
     `promoted`; `discardShadow(s)` is a *separately schedulable* action whose guard
     is §5's stated predicate (no live record ∧ no residue entry).
     `inv_owner_serves_promoted_slot` fails on discard-before-promote if the residue
     guard is reverted.
  9. Coverage-batch action (empty-payload advance) + witness
     `witnessCompleteEnabledOnQuietSlot` (N-C5), **with the receipt assertions as
     guards on the coverage/stream action** (V4-M1): witness
     `witnessEmptyCoverageBatchIsANoOp` (a zero-advance batch is admitted) + an
     invariant that no batch-level fault discards the shadow.
  10. `ResetCluster` action + the epoch-keyed `spent` set above + Begin-time
      discard-then-ingest + `inv_no_orphan_shadow_blocks_ingest` (N-M2/V4-M8 — an
      orphan shadow from a pre-reset epoch must be reachable *and* must not wedge the
      new migration).
  11. `sourceCannotApply` flag + witness that the held set empties (self-fence
      release) while the flag holds (N-M1). **Plus `targetSilent`** (V4-M2 — the
      mirror escape: alive-but-unable-to-act on the target side): bounded witness
      `witnessDrainingWedgedWithCompletableToken` — `Complete`'s guard holds,
      `observations` pinned by ingest progress, held set non-empty; with the V4-M2
      fixes (leader auto-`Complete` + narrowed reset) the witness must become
      **unreachable**, a clean mutation test.
- **Stated structural limits** (recorded in the rework section, not silent): the model
  has one global applied view, so the "node acts on state it has not applied / cannot
  observe" defect class (v2-C2/C8, v3 N-C4, N-M1's cause) is discharged by spec review
  and the seam lints, not by Quint. **The review checklist for that class must
  enumerate every conjunct of every admission conjunction against the replicated
  state-space table** (V4 audit note: two consecutive rounds each produced one
  non-replicated-conjunct instance — N-C4, then V4-C3 — so the class needs a
  mechanical check, not vigilance). The model carries no temporal operators: liveness
  is expressed as named bounded witnesses with stated step counts, plus the invariant
  in (4); each property's shape (invariant vs bounded witness) is stated next to it —
  and this limit fixes the *shape* of the no-exit findings' properties (V4-M2/M8/M11
  are all bounded-witness-expressible), never an excuse to omit them (V4 audit note 1).

## Testing

- Spec-first: every rewritten/new row lands with its forcing test or an explicit
  temporary hole.
- Quint: the eleven extensions above, mutation-validated against their named findings
  (revert the design fix in the model → the named property must fail).
- Turmoil/Jepsen: quiet-source cutover (coverage obligation + leader auto-`Complete`
  make the slot move with zero slot traffic — N-C5/V4-M2 as a *pass*); quiet-node
  keepalive soak (zero-advance coverage batches for many cadences: no assertion trips,
  no discard — V4-M1); wedged-alive target on a cold slot in Draining
  (progress-sensitive bound fires; a healthy shrinking drain survives past N ticks —
  N-M6); completable-token-but-target-never-completes (leader auto-`Complete` moves
  the slot — V4-M2); knob-enabled with one dead target replica (observation bound
  aborts; no wedge — V4-M2/C3); source crash-restart mid-stream with `offset_at_save`
  behind the streamed head (resume refused via incarnation — N-C2's splice must be
  impossible); **source crash-restart racing `Prepare`/`Confirm`** (boot ordering +
  run guards refuse the cross-run `drained_pos` — V4-C2's interleaving as a forced
  test); target restart mid-ingest (resumes from `covered_applied`; no re-snapshot;
  no cancel — V4-M5); target replica attaches mid-ingest then target fails over onto
  it post-`Complete` (slot served in full — V4-M6); replica / empty-slot-map follower
  / just-demoted node datasets untouched by the reaper (N-C1); demoted source
  re-promoted later completes its deferred delete (V4-M7); crash between `Complete`
  apply and any source-local action (residue entry survives via snapshot; delete
  eventually runs — V4-M7); **discard-reaper vs promotion race at `Complete`**
  (shadow consumed by promotion, never the reaper — V4-C1's interleaving as a forced
  test); failed promotion (residue stays `promoted == false`; source copy intact;
  operator rollback re-assigns and reaper then discards — V4-M11); target serves
  `-TRYAGAIN` between `Complete` apply and promotion completion, no nil reads
  (N-C3); target-replica `READONLY` reads during its promotion window (V4-m6);
  `Complete` conjunction evaluated identically on leader and follower snapshots —
  including with the replica-ack knob on (N-C4/V4-C3 — determinism harness);
  partitioned sealed source releases its held clients via the self-fence row while
  staying fenced (N-M1); `ResetCluster` discards shadows on all applying nodes; a
  node rejoining with a pre-reset orphan shadow colliding with a live id ingests
  after Begin-time discard (V4-M8); Release/Complete and Cancel/Complete races
  (v2-C3); promote→demote→re-promote in one boot: `ReportRunIdentity` ordering by
  `identity_seq`, no regression, no spurious cancel (V4-M4); demotion via bare
  `REPLICAOF` mid-migration as source: identity change cancels (V4-M4a); leader churn
  during a dead-target Draining (replicated observations converge); FLUSHALL on
  target (target-proposed cancel — V4-M12); cross-shard VLL continuation outstanding
  at seal time: `Confirm` not proposed until it resolves; its write counts into
  `drained_pos` (V4-M13).
