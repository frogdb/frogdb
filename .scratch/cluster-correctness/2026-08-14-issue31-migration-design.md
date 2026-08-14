# Slot migration redesign — source-authoritative-until-commit (v3)

Status: revision 3 — pending user approval. Revision 2 resolved review v2's findings
(8C/13M/8m); independent re-review v3 found 5 new CRITICAL / 13 MAJOR / 8 MINOR and 15
incompletely-resolved priors; this revision resolves all of them. Reviews:
issue31-adversarial-review-v2/-v3, job dir 2026-08-14.
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

### §0 Positions, identities, and the one sequence space (v2-C6, v3 N-C2/N-C4/N-M7)

One position space for the entire protocol: the **source's replication byte offset**
(the same space the replication link and full-sync checkpoint already use). Every
position-valued quantity below — `snapshot_pos`, `drained_pos`, `target_ingested_pos`,
per-batch stamps, parity lag — is denominated in this space. No RocksDB/WAL sequence
appears anywhere in the protocol. (The issue-12 "the snapshot carries its covered
position, never a post-hoc global read" rule is preserved verbatim — the covered position
is the replication offset captured before the cut, after a shard drain, exactly the shape
the full-sync path uses.)

**Run identity** (N-C2): `run_id = (replid, incarnation)`. `replid` is the source's
replication history id with the atomic replid/offset pairing of replication issue 24.
`incarnation` is a per-boot token the node mints once at process start (a persisted
per-node counter incremented at every boot — monotone across restarts). The pair exists
because FM-REPLICATION-021 makes a plain restart keep the replid and re-advance the head
raise-only from `offset_at_save` (INV-OFFSET-2: the file may claim more than the live
head reached) — same identity, forward position, hole in the data — so replid alone
cannot signal the discontinuity. **Every restart of the source, with or without a replid
change, changes `run_id` and is an explicit discontinuity: resume is refused and the
migration is cancelled.** A position is meaningful only within its `run_id`.

**The run identity is replicated state** (N-C4/N-M11): `NodeInfo` gains a
`run_identity: Option<(replid, incarnation)>` field, written by a new replicated
transition `ReportRunIdentity{node, run_id}` that each node proposes at boot and at
every promotion (the moments FM-REPLICATION-023's one-cell-per-process identity
changes). Every admission conjunct that mentions a run identity reads **this replicated
field**, never a node-local cell — `apply` stays a pure function of replicated state
(the FM-CLUSTER-089 determinism rule, preserved). `BeginSlotMigration`'s apply captures
`record.run_id` from the source's replicated `run_identity` at apply time (defined,
observable writer — refused if the field is absent).

**Target-side heads, classified** (N-M7 — two distinct positions, per §0's own rule):

- `covered_received` — highest contiguous position received on the stream. Operand of
  the per-batch contiguity assertions (checked at receipt, §4).
- `covered_applied` — highest position **durably applied** into the shadow. The value
  reported in `ReportMigrationIngest`, the resume point after any target crash or
  session re-establishment, and the stream's ack unit (§8). The received-but-unapplied
  window (`covered_received − covered_applied`) is lost on a target crash by design and
  re-requested at resume — never silently skipped.

Only `covered_applied` ever reaches an admission predicate.

Two distinct identity counters, never conflated (v2-C3/M1):

- **`attempt_id`** — minted from the LOCKED cluster-wide generation
  `ClusterStateInner.handoff_seq` at every accepted `PrepareSlotHandoff`. Monotone,
  never reused across snapshot restore (FM-CLUSTER-086/095/100 unchanged in mechanism;
  `SlotFence` keeps this as its input). Carried by every subsequent message of that
  handoff attempt; mismatched messages are refused.
- **`migration_id`** — minted from the same generation at `BeginSlotMigration`.
  Identifies the migration across handoff attempts; keys the target's shadow store and
  every abort/discard. `ResetCluster` rewinds the generation (TR-CLUSTER-035) — the
  reset interaction is handled in §5 (N-M2), not ignored.

### Replicated migration record

```
{ slot, source, target, migration_id, run_id, phase,
  attempt_id: Option, snapshot_pos: Option, drained_pos: Option,
  target_ingested_pos: Option, attempts, observations,
  last_observation: Option<(term, tick)> }
phase ∈ { Snapshotting, Streaming, Draining }   (terminal Complete/Aborted = record removed)
```

Field writers (every field has exactly one writing transition):

| Field | Written by | Notes |
|-------|-----------|-------|
| slot, source, target, migration_id | `BeginSlotMigration` | immutable |
| run_id | `BeginSlotMigration` (from source's replicated `NodeInfo.run_identity`) | immutable; §0 |
| snapshot_pos | `RecordSnapshotPosition` (source) | `Option` — absent ≠ 0; immutable once set; phase → Streaming |
| attempt_id | `PrepareSlotHandoff` (mints from generation) | replaced per attempt |
| drained_pos | `ConfirmSlotHandoffDrained` (source) | **cleared** by `AbortSlotHandoff` |
| target_ingested_pos | `ReportMigrationIngest` (target) | `covered_applied`; monotone within run_id |
| attempts | `AbortSlotHandoff` (+1); reset by re-issued `MIGRATING` (N-m4, §6) | any failed attempt counts |
| observations | `ObserveMigration` (+1); **reset by any `ReportMigrationIngest` that advances `target_ingested_pos`** (N-M6) and by phase change | replicated; survives leader change |
| last_observation | `ObserveMigration` | dedup state for the counter (N-M5) |

All fields carried in `ClusterSnapshot`/`from_snapshot` (FM-CLUSTER-100 extended).

### Transitions

Code-true names; `AbortSlotHandoff` (existing TR-CLUSTER-014) is the attempt-release;
`CancelSlotMigration` (existing TR-CLUSTER-015) is the whole-migration abort.

- **`BeginSlotMigration`** → record created, phase=Snapshotting, attempts=0,
  observations=0, run_id captured per §0. Admission (N-M12): slot owned by `source` ∧
  `source != target` ∧ both endpoints are cluster members (FM-CLUSTER-032's
  `NodeNotFound` arms) ∧ target's role is primary ∧ source's replicated `run_identity`
  present ∧ neither endpoint FAIL-flagged ∧ no open record for the slot. **Idempotency
  owned explicitly** (FM-CLUSTER-031's surviving half): a re-issued `MIGRATING` naming
  the same (slot, source, target) over an open record answers `Ok` without a new record
  — and resets `attempts` to 0 (the operator's "try again" verb, N-m4). `AssignSlots`
  refuses any slot with an open record.
- **`RecordSnapshotPosition{migration_id, pos}`** (source-proposed, after the snapshot
  is cut) → snapshot_pos=pos, phase=Streaming. Admission (N-M12): record exists ∧
  migration_id matches ∧ phase==Snapshotting. Duplicate proposal with the same value =
  no-op `Ok`; with a different value = refused (the field is immutable). Target ingests
  the snapshot, then the tail from `pos` exclusive.
- **`ReportMigrationIngest{migration_id, run_id, applied_pos}`** (target-proposed,
  periodically). Admission (N-M4): record exists ∧ migration_id matches ∧ run_id
  matches `record.run_id` ∧ proposer is `record.target` ∧ `applied_pos ≥` current
  `target_ingested_pos` (an equal or lower value applies as a no-op — reports are
  idempotent, refusal is reserved for identity mismatches). Writes
  target_ingested_pos=applied_pos; if it advanced, resets `observations` to 0 (N-M6).
  No attempt_id: the position space is per-run, not per-attempt, so reports are
  attempt-independent. Report cadence is node-local and is never an admission input —
  only the replicated value is.
- **`PrepareSlotHandoff`** (source-proposed) → mints attempt_id, phase=Draining, barrier
  arms on the source (per-object, issue 17/19 semantics). Full admission conjunction:
  phase==Streaming ∧ record matches (source, target) ∧ attempts < max-handoff-attempts
  ∧ neither endpoint FAIL-flagged ∧ no live attempt_id. The source *chooses* to propose
  at parity (`feed_head − target_ingested_pos ≤` parity threshold) — a scheduling
  heuristic, not a correctness input.
- **`ConfirmSlotHandoffDrained{attempt_id, pos}`** (source-proposed once its shard has no
  in-flight write below `pos` and the barrier holds) → drained_pos=pos. Refused unless
  phase==Draining ∧ attempt_id matches. From this point the source's fence is
  **sealed**: it must not execute another write for the slot until it *applies*
  `AbortSlotHandoff`, `CancelSlotMigration`, or `CompleteSlotMigration`. **Coverage
  obligation** (N-C5): proposing this transition obliges the source to emit, on the
  migration stream, a coverage batch `(run_id, covered_head, pos)` — empty payload if
  the range holds no slot traffic — so the target's covered position can actually reach
  `pos` without depending on client traffic. (§4 adds the periodic coverage rule;
  together they make `Complete` genuinely traffic-independent.)
- **`CompleteSlotMigration{attempt_id, token}`** (target-proposed) — admission is the
  full conjunction, every conjunct a pure function of replicated state:

  ```
  phase == Draining
  ∧ attempt_id == record.attempt_id
  ∧ record.drained_pos == Some(token)
  ∧ record.target_ingested_pos >= record.drained_pos     // covered_applied — possession
  ∧ record.run_id == nodes[record.source].run_identity   // replicated field, §0 (N-C4)
  ∧ slot_map[slot] == record.source                      // source still owner
  ∧ record.target not FAIL-flagged                       // N-m6, symmetric with Prepare
  ∧ [if cluster-migration-require-target-replica-ack]    // optional durability conjunct,
      target replicas acked through drained_pos          // off by default (N-M10)
  ```

  On apply: ownership flips, `MOVED` correct, barrier release event emitted **after**
  the assignment mutation (FM-CLUSTER-092 ordering preserved), and the apply records
  two node-local work items: the source's pending delete (§7) and the target's pending
  promotion (§5). Neither deletion nor promotion runs inside apply.
- **`AbortSlotHandoff{attempt_id}`** (source- or leader-proposed) → clears drained_pos
  and attempt_id, attempts+=1, phase=Streaming, emits the barrier-release event
  (FM-CLUSTER-087). Refused on attempt_id mismatch. If attempts ≥
  max-handoff-attempts, the applying transition instead cancels the migration.
- **`ObserveMigration{migration_id, leader_term, tick}`** (leader-proposed each
  reconcile tick while a record sits in Draining without a completable token) →
  observations+=1. Dedup (N-M5): the record's `last_observation` stores the last
  accepted `(term, tick)` pair; a proposal with a pair ≤ the stored one applies as a
  no-op, so a retried or duplicated proposal cannot double-count. `tick` is a
  leader-local monotone counter; `leader_term` is the proposer's Raft term carried as
  opaque command data (N-m7 — the carrier is the command payload; the state machine
  compares the pair and consumes no other Raft metadata). When observations reaches
  `cluster-migration-draining-observations`, the apply forces the `AbortSlotHandoff`
  outcome. Because `ReportMigrationIngest` resets the counter on progress (N-M6), the
  bound reads: **abort after N leader observations with no target progress** — a
  wedged drain exits, a large healthy shrinking drain does not.
- **`ReportRunIdentity{node, run_id}`** (each node, at boot and at promotion) → writes
  `NodeInfo.run_identity`. Admission: proposer is `node`; the value's incarnation ≥
  the stored one. Applying a run_identity change for a node that is the source or
  target of any open migration **cancels those migrations** — the replicated form of
  "restart aborts" (§4).
- **`CancelSlotMigration{migration_id}`** (operator / source / leader) — admitted in
  **every** phase: record removed, release event emitted if a barrier was armed, target
  (and its replicas) discard the shadow store keyed by migration_id (§5). Idempotent:
  cancelling a slot with no open migration replies `Ok` (FM-CLUSTER-035 preserved). A
  cancel that races and loses to `Complete` replies an error stating the migration
  committed.

Ordering races all resolve mechanically by the conjunctions above: a stale
`ConfirmSlotHandoffDrained` after `AbortSlotHandoff` is refused (attempt_id cleared); a
`Complete` after `AbortSlotHandoff` is refused (phase, attempt_id, drained_pos all
fail); two `Prepare`s cannot both mint (second refused: live attempt_id); `Cancel`
beats everything except an already-applied `Complete`.

### §1 Liveness (v2-C4/C5/C8, v3 N-M6/N-M13)

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
  accepted. (b) the `ObserveMigration` bound — the replicated, progress-sensitive
  observation counter is the Draining exit that needs no client traffic and survives
  leader churn. It also backstops a lost overflow-abort proposal (its input is the
  replicated record, not a cross-node offset).

**Stated limit** (N-M13): the layers above cover a *dead* endpoint in any phase (FAIL
flag) and a *wedged target* (observation bound in Draining; the source's progress layer
in Streaming). A **wedged-but-listening source** in Snapshotting/Streaming — alive to
TCP probes (the spec's GAPS entry 4 liveness limitation), proposing nothing — has **no
automatic exit**; the exit is the operator (`CancelSlotMigration` via `CLUSTER SETSLOT
… STABLE` or frogctl), and the stuck record is visible in `CLUSTER MIGRATIONS` with a
static phase. The universal "no reachable state lacks an exit" claim is therefore
scoped: every *Draining* state has an automatic exit; pre-Draining states have
automatic exits for dead endpoints and wedged targets, and an operator exit otherwise.
No layer consumes a datum its actor cannot observe.

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
token reachable on a quiet source), cap breach (traffic-driven), the progress-sensitive
observation bound (traffic-independent), and the self-fence client release above.

### §4 Snapshot + stream — run-identified, coverage-stamped (v2-C6, v3 N-C2/N-C5/N-M7)

- The slot snapshot is cut after a shard drain; it **carries** its covered replication
  offset `snapshot_pos` (issue-12 rule, preserved verbatim). The mutation stream is
  slot-filtered at the source feed and starts at `snapshot_pos`, exclusive.
- Every batch is stamped `(run_id, from_pos, to_pos)` with **coverage** semantics: the
  range covers filtered-out traffic too, so a quiet slot on a busy node still advances
  the target's covered position. **Periodic coverage rule** (N-C5): the stream emits a
  coverage batch (empty payload) on its keepalive cadence regardless of traffic, so
  `covered_received` tracks the source head even on a fully quiet node. Cadence is
  node-local and never an admission input — only the resulting replicated report is.
- Target assertions at **receipt**, against `covered_received`, in order: `run_id`
  equality (a change is an explicit discontinuity → discard, never an implicit gap);
  `from_pos == covered_received` (a forward gap → discard); `to_pos > covered_received`
  (a backward or overlapping batch → discard — regression is detected, not silently
  re-applied).
- **Resume rule** (N-C2 — both clauses required): after any session re-establishment or
  target restart, the target requests resume at `(run_id, covered_applied)`. The source
  admits the resume iff **(a)** its current run identity equals the requested `run_id`
  — and because `incarnation` changes on every source restart, a restarted source
  always refuses, even though FM-REPLICATION-021 keeps its replid and re-advances its
  head forward over a hole — and **(b)** the source's replication **backlog floor** ≤
  `covered_applied`: the FM-REPLICATION-014 armed-floor test ("a `+CONTINUE` is never
  served over a hole"), reused verbatim as the definition of **"history intact"**.
  Either refusal → `CancelSlotMigration`. Never resume across a run_id change.
- **Endpoint failover / restart rules**: a failover naming the **source** aborts the
  migration (prune per TR-CLUSTER-018/FM-CLUSTER-036; release events paid,
  FM-CLUSTER-087; a successor never inherits a barrier — FM-CLUSTER-104 stays
  same-node-only). A failover naming the **target** mid-ingest aborts the migration. A
  **restart of either endpoint** aborts via `ReportRunIdentity` (the incarnation change
  cancels that node's open migrations, Transitions). Retargeting is future work,
  deliberately out of scope. These rows — not target-discard alone — are what
  supersedes issue 15.
- **Backpressure**: per-migration bounded backlog (bytes) on the source, separate from
  the replica feed. Overflow → source drops the session and proposes
  `CancelSlotMigration`; the observation bound backstops a lost proposal. On session
  death the target keeps its shadow and awaits resume; discard happens only on
  replicated cancel/abort — a transient blip never destroys ingest progress.

### §5 Target-side shadow, discard, and promotion (v2-C7, v3 N-C3/N-M2/N-M10/N-m1/N-m5)

- The shadow store is keyed by `(slot, migration_id)` and lives outside the target's
  main keyspace. By construction it is invisible to `SCAN`, `KEYS`, `DBSIZE`,
  `RANDOMKEY`, `INFO keyspace`, and RDB/AOF of the main keyspace. TTLs are stored,
  never enforced during ingest; **expiry interaction stated** (N-m5): per
  FM-REPLICATION-030 each node expires on its own clock and no expiry `DEL` propagates
  on this stream, so the source's logical expiries never reach the shadow — the copies
  converge through post-promotion lazy expiry, exactly as a promoted replica converges.
  Eviction never selects shadow keys — target memory pressure instead **aborts the
  migration**.
- `FLUSHALL`/`FLUSHDB` on the target aborts open migrations targeting it (explicit row)
  rather than silently corrupting the shadow. **`RESTORE` — or any key write — into the
  target's main keyspace for a slot the target is importing is refused `-TRYAGAIN`
  while the record is open** (N-m1): otherwise its precedence against the
  about-to-be-promoted shadow would be undefined. FM-CLUSTER-027's
  RESTORE-into-importing exemption is retired with this row.
- **The shadow replicates through the target's normal feed during ingest**: the
  target's replicas build the shadow alongside it. `Complete` does **not** require
  target-replica parity by default; the residual window — target fails over between
  `Complete` and its replicas catching up on the tail — is an explicit accepted-mode
  row. **Stated honestly** (N-M10): this window is *not* TR-CLUSTER-019's shape,
  because `WAIT` is no escape hatch here — a write acked on the source and
  `WAIT`-confirmed against the *source's* replicas moves at `Complete` to a
  replication group that never counted it. The row states plainly: **by default no
  per-write durability escape hatch exists across a slot migration**. Operators who
  need one enable `cluster-migration-require-target-replica-ack`, which adds the
  optional `Complete` conjunct (target replicas acked through `drained_pos`) at the
  cost of cutover latency.
- **Promotion** (N-C3 — the mirror of §7, previously unspecified): the shadow becomes
  the live keyspace as a **node-local, idempotent, resumable consequence** of the
  applied `SlotMigrationCompleted` event — never inside apply. Promotion is a
  **metadata operation** — the shadow region is re-labelled as the slot's live data
  (the store is slot-keyed already; no per-key copy) — so the window is O(1), not
  O(keys). Fail-closed belt-and-braces regardless: **from the instant `Complete`
  applies until promotion completes, the target answers requests for the slot
  `-TRYAGAIN`** — it never serves the slot from its (empty) main keyspace, so no
  client can read nil for a live key or write a value the promotion would clobber. A
  target-side reconcile resumes an interrupted promotion at boot. The target's
  replicas promote on the same applied event.
- **Discard** (on any cancel/abort) is keyed by migration_id and propagates to the
  target's replicas as a shadow delete-range. **The discard reaper is level-triggered**
  (N-M2): it runs on every observed change to the replicated migration set — not only
  at boot — deleting any shadow store whose migration_id has no live record.
  **`ResetCluster` is an explicit shadow-discard trigger on every node** (N-M2):
  TR-CLUSTER-035 rewinds the `handoff_seq` generation to 0, so ids become mintable
  again; discarding all shadows at reset closes the contamination/wedge path, and the
  reset row says so. Ingest is refused for a migration_id whose shadow store is
  non-empty at Begin-time — with the two rules above this is a pure assertion, since
  no orphan shadow survives to trip it.

### §6 Client-visible semantics (v2-M3/M6, v3 N-M3/N-M9/N-m2/N-m3)

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
  retire with the first.
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
  the target auto-proposes `Complete`; the slot moves without a second operator
  action. This deviates from the Redis reshard flow — where nothing moves until
  `SETSLOT NODE` — and matches Valkey 9.0's atomic migration, where starting the
  migration is the operator's one decision. Operator control is: visibility via
  `CLUSTER MIGRATIONS`, cancellation via `STABLE`/frogctl at any pre-`Complete`
  moment, and the optional durability conjunct (§5). `SETSLOT NODE` survives as a
  compat verb: with an open matching record it proposes `Complete` eagerly (replying
  `-TRYAGAIN` if the conjunction does not yet hold — harmless from tooling, a no-op in
  the automatic flow); with no open record, bare `NODE` on a non-migrating slot
  remains the existing topology-repair verb, and `AssignSlots`' refusal of slots with
  open records prevents it bypassing a live migration.
- **Operator surface**, rest: `CLUSTER SETSLOT <slot> MIGRATING <target>` =
  `BeginSlotMigration` (one-sided; `IMPORTING` on the target is a no-op ack for
  tooling compat — FM-CLUSTER-031's two-sided handshake retired; re-issued `MIGRATING`
  = `Ok` + attempts reset, Transitions). `STABLE` = `CancelSlotMigration` (idempotent
  `Ok`, FM-CLUSTER-035 preserved). `MIGRATE`/`RESTORE` survive as key-level commands
  but resharding no longer uses them (`RESTORE` importing-slot refusal per §5);
  `ASKING` is accepted as a no-op (`+OK`); `-ASK` is never emitted. Deviation rows for
  all three. `CLUSTER SLOTS`/`SHARDS`/`NODES` render the slot under the source until
  `Complete`; no split markers ever appear.
- **`-TRYAGAIN` inventory** (N-m2 — enumerated, replacing v2's false "exactly two
  paths"): cap-breach fail-closed (§3); self-fence held-set release (§3); unpinnable
  held batch at `Complete` (§3, N-m3); `SETSLOT NODE` before the conjunction holds
  (§6); target pre-promotion refusal (§5); `RESTORE`/key-write into an importing slot
  (§5). FM-CLUSTER-095's finalization `-TRYAGAIN` is retired (above);
  FM-CLUSTER-091's drain-wait refusal is retired with it — under source authority
  nothing is refused while the source still owns the slot: writes are held or acked,
  never bounced.

### §7 Source-side deletion — scoped work list (v2-M13, v3 N-C1)

Deletion of the slot's keys on the source is an **event-driven, node-local consequence**
of the applied `SlotMigrationCompleted` event (the FM-CLUSTER-090 shape) — never inside
the Raft apply. **The reaper consumes a named work list, never a global predicate**
(N-C1): applying `Complete` records a node-durable work item `{slot, migration_id}` on
the source (and, via its feed, on the source's replicas as the delete-range below); the
reaper deletes exactly the keys of the slots in its work list and clears each entry when
its delete finishes. Crash-mid-delete residue is covered because the entry survives
restart (resumable, idempotent). **The reaper has no other trigger**: it never evaluates
"slots I do not own" — a predicate that would delete every replica's entire dataset
(slot ownership names primaries, so a replica owns zero slots), every
legitimately-empty-map follower's (FM-CLUSTER-032's invariant: bootstrap assigns slots
locally, not through Raft), and a just-demoted node's (TR-CLUSTER-018 + issue 20's
demote-don't-remove). Stated guards, belt-and-braces: the reaper never runs against its
own list on a node whose role is Replica (replicas receive the delete-range through the
feed), and an empty work list means no deletion, whatever the slot map says.

Keyspace notifications are suppressed for migration deletes; the deletes replicate to
the source's own replicas as a bounded-rate delete-range. Barrier-release ordering:
release fires after the assignment mutation and before deletion begins — a woken write
sees `MOVED`, never a half-deleted locally-served slot (FM-CLUSTER-092 ordering
preserved).

### §8 Migration stream session; the replica-feed hold is retired (v3 N-M8)

The mutation stream is its **own session family**, not a replica session: it shares the
wire framing/backlog code but has its own ack unit (`covered_applied`, §0), its own
keepalive (which also drives the periodic coverage batches, §4), and its own bounded
backlog.

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
  shadow-store bytes (target); pending-promotion and pending-delete gauges;
  dual-storage overhead; complete/cancel counters labeled by reason (operator, stall,
  FAIL, observation-bound, overflow, run-id-change, restart, failover, flush, memory).
- **`CLUSTER MIGRATIONS`**: admin-gated in the FM-CLUSTER-061..064 fail-closed class;
  RESP3 map reply, one entry per open record: slot, source, target, migration_id,
  phase, attempt_id, attempts, observations, snapshot_pos, drained_pos,
  target_ingested_pos, lag. Served from replicated state on any node (follower-safe;
  values may lag the leader — documented). Target-side node-local detail (ingest
  position, last apply error, shadow bytes, promotion state) appears in `INFO` section
  `migrations` and the debug web page on the target.
- **frogctl**: `frogctl cluster migrations` mirrors the command; `frogctl cluster
  migrate-slot --cancel` drives `CancelSlotMigration`; re-issuing the migrate verb on
  a max-attempts migration retries it (attempts reset, §6).
- **Events**: cluster event-log row per transition and per cancel (with reason).
- **Grafana**: migration panel via the dashboard generator (never the JSON).

### Cost (v3 N-m8)

**Transient overhead** while one slot migrates: `(1 + R_target) ×` slot bytes (the
target's shadow plus its replicas' shadows), plus the bounded stream backlog and up to
`cluster-migration-barrier-max-bytes` of held writes. **Total residency** counting the
copies that exist anyway: `(1 + R_source) + (1 + R_target) ×` slot bytes — the source
group holds the slot throughout and then absorbs the delete-range. The spec states both
numbers; the *overhead* figure is the capacity-planning bound.

## Config

| Knob | Default | Denomination |
|------|---------|--------------|
| `cluster-migration-parity-threshold-bytes` | 1 MiB | bytes (initiate heuristic, §2) |
| `cluster-migration-barrier-max-bytes` | 4 MiB | bytes (held-write cap, §3; resolves the issue-29 ambiguity — the surviving issue-17 bound) |
| `cluster-migration-max-handoff-attempts` | 3 | attempts (any failed attempt counts; reset by re-issued `MIGRATING`) |
| `cluster-migration-stall-strikes` | 3 | source-local strikes (§1, reset on progress or session re-establishment) |
| `cluster-migration-draining-observations` | 3 | leader observations with no target progress (§1) |
| `cluster-migration-backlog-max-bytes` | 64 MiB | bytes (per-migration stream backlog, §4) |
| `cluster-migration-require-target-replica-ack` | off | optional `Complete` durability conjunct (§5, N-M10) |

Zero wall-clock in any admission or abort predicate; timers drive cadence only.

## Spec / impl blast radius — full verdicts

Every touched row gets an explicit verdict in the spec change; summary:

- **Rewritten**: TR-CLUSTER-010..013 (phase machine above); TR-CLUSTER-014
  (`AbortSlotHandoff` — clears drained_pos/attempt_id, increments attempts);
  TR-CLUSTER-015 (`CancelSlotMigration` — repatriation precondition retired,
  target-discard + release events); TR-CLUSTER-035 (`ResetCluster` — adds the
  shadow-discard trigger, §5); FM-CLUSTER-026/027/028 (importing gates: probe
  collapse; RESTORE exemption → refusal, §5); FM-CLUSTER-029 (WATCH, re-derived);
  FM-CLUSTER-031/032/033/035 (SETSLOT surface incl. owned idempotency + membership
  arms, §6/Transitions); FM-CLUSTER-036 + TR-CLUSTER-017/018/019 (failover/restart
  naming an endpoint — §4 rules); FM-CLUSTER-037 (commit-to-apply window, now with the
  pre-promotion refusal, §5); FM-CLUSTER-084 (admission conjunctions above);
  FM-CLUSTER-086 (attempt stamping); FM-CLUSTER-087 (release events from
  Cancel/Abort/Complete/prune/self-fence); FM-CLUSTER-090/091 (barrier action;
  drain-wait `-TRYAGAIN` retired — held or acked, never bounced);
  FM-CLUSTER-092/093/094 (inversion under source authority, §6); FM-CLUSTER-095 (arm
  split: finalization-refusal arm retired, ownership-moved arm kept; SlotFence
  generation mechanism unchanged); FM-CLUSTER-096 (unpinnable batches: parked-batch
  disposition at each exit, §3); FM-CLUSTER-104 (same-node re-arm only; successor
  never inherits).
- **Retired**: FM-CLUSTER-085 (handoff lease — its property, "a dead finalizer cannot
  wedge a slot", is re-provided by the observation bound; replacement row states
  this); FM-CLUSTER-089 (proposer-minted deadlines — no deadlines remain; its
  determinism rule survives and §0 obeys it via the replicated run identity);
  **FM-CLUSTER-097 + the `ReplicaFeedGate`** (§8 — purpose re-derived to nothing under
  source authority; row rewritten to assert the absence of migration feed holds).
- **Unchanged, stated**: FM-CLUSTER-038 (blocked-client wake at Complete);
  FM-CLUSTER-095's SlotFence generation input; FM-CLUSTER-100 (generation survives
  snapshots — extended to the new record fields and `NodeInfo.run_identity`);
  TR-CLUSTER-008/009 (open-record refusal of `AssignSlots` — strengthened to all
  phases); TR-CLUSTER-016 (feed byte cap — scoped back to ordinary backpressure, §8);
  TR-CLUSTER-026 (self-fence — gains the held-set release row, §3); TR-CLUSTER-034
  (per-node arm/release reaction).
- **New rows**: `NodeInfo.run_identity` + `ReportRunIdentity` incl.
  restart-cancels-migrations (§0/Transitions); target ingest/resume + `(run_id,
  position)` assertions incl. backward detection and the backlog-floor "history
  intact" test (§4); coverage obligations — drain-flush + periodic (§4/Transitions);
  received-vs-applied head definitions (§0); `ReportMigrationIngest` admission
  (Transitions); progress-sensitive observation bound + `last_observation` dedup
  (Transitions); sealed-fence / local-fence-never-weaker invariant + self-fence
  held-set release (§3); held-write disposition table incl. unpinnable batches (§3);
  target-discard + level-triggered reaper + reset-discard (§5); shadow replication
  through the target feed + honest no-escape-hatch row + optional durability conjunct
  (§5); shadow promotion + pre-promotion refusal (§5); RESTORE-into-importing refusal
  (§5); FLUSH/memory-pressure aborts (§5); expiry-convergence row (§5); scoped
  deletion work-list + guards + notification suppression + ordering (§7);
  migration-stream session family (§8); automatic-cutover deviation row (§6);
  `CLUSTER MIGRATIONS` gating/reply (§9); operator-exit row for pre-Draining wedged
  source (§1); Redis-deviation rows: no `ASK`, `ASKING` no-op, `MIGRATE` not used for
  resharding, no split markers.
- **Cross-tracker**: issue 15 closes only when §4's endpoint-failover/restart rows
  land; spec-gaps issue 12 (watermark carries covered position — landed `eedb76d0`) is
  the snapshot-position substrate; replication issue 24 (replid/offset pairing) is the
  replid half of `run_id`; FM-REPLICATION-014 (backlog floor), -021 (restart identity),
  -023 (identity cell), -030 (per-node expiry) and INV-OFFSET-2 are load-bearing
  dependencies, cited not contradicted; hardening-2 rework 12's `ReplicaFeedGate` is
  **removed** by §8 — its issue gets a superseding note; cluster issue 16
  (`AssignSlots` refusal) is subsumed; issue 29's cap ambiguity resolved by the config
  table; FM-CLUSTER-096's cross-shard VLL continuation hole remains open, restated.

## Quint rework (v3 review adopted in full)

`specs/quint/cluster_migration_failover*.qnt` (now four files):

- Keep the model's `handoff_seq` variable and `inv_handoff_seq_never_reused` **as-is**.
  Keep `feed_bytes` / `disconnected_feed`, now modelling ordinary backpressure (§8).
- Drop `repatriating`, `inv_abort_repatriates`, `completeRepatriation`.
- Extensions, each named for the finding it must catch (acceptance bar: reverting the
  design fix in the model must violate the named property):
  1. `target_copy` / `source_log` high-water vars + `inv_no_acked_write_lost` (v2-C1).
  2. `fenced` tracked separately from `phase` + `localReleaseOnCapBreach` +
     `inv_complete_requires_fenced_source` (v2-C2).
  3. Attempt stamping on confirm/release/complete/cancel +
     `inv_complete_requires_draining_phase` (v2-C3).
  4. `reconcileTick` + replicated `observations` with progress-reset + the liveness
     pair (v2-C4/N-M6): bounded witness `witnessDrainingHeldForKTicks` **and
     invariant** `inv_progressing_migration_never_aborts` — the v2 model's witness
     `witnessHealthyMigrationAborted` documented the N-M6 bug as expected behaviour;
     the invariant makes it a violation instead.
  5. `sourceFailover` (identity change + backward jump) **and** `sourceRestart`
     (N-C2's live hazard: **same** replid, position dropped to a save point, then
     re-advanced forward over a hole) + `inv_stream_history_sound` strengthened from
     contiguity to history soundness via per-position history tags.
  6. `target_replica_copy` + `inv_target_replicas_hold_committed_slot` (v2-C7).
  7. Per-node keyspace `source_keys: NodeId -> Set[SlotId]` + `reapSlots(n)` action +
     `inv_node_keeps_slots_it_owns` (N-C1 — the model must be able to express
     over-deletion to prove its absence).
  8. `promoted: Set[SlotId]` + `inv_owner_serves_promoted_slot` (N-C3).
  9. Coverage-batch action (empty-payload advance) + witness
     `witnessCompleteEnabledOnQuietSlot` (N-C5 — reachability of the completable
     token with zero slot traffic).
  10. `ResetCluster` action exercising `inv_handoff_seq_never_reused` and the §5
      shadow-discard (N-M2).
  11. `sourceCannotApply` flag + witness that the held set empties (self-fence
      release) while the flag holds (N-M1).
- **Stated structural limits** (recorded in the rework section, not silent): the model
  has one global applied view, so the "node acts on state it has not applied / cannot
  observe" defect class (v2-C2/C8, v3 N-C4, N-M1's cause) is discharged by spec review
  and the seam lints, not by Quint — N-C4's fix (replicated `run_identity`) is checked
  by review of the conjunction text, and the rework section says so. The model carries
  no temporal operators: liveness is expressed as named bounded witnesses with stated
  step counts, plus the invariant in (4); each property's shape (invariant vs bounded
  witness) is stated next to it.

## Testing

- Spec-first: every rewritten/new row lands with its forcing test or an explicit
  temporary hole.
- Quint: the eleven extensions above, mutation-validated against their named findings
  (revert the design fix in the model → the named property must fail).
- Turmoil/Jepsen: quiet-source cutover (coverage obligation makes `Complete`
  admissible with zero slot traffic — N-C5's scenario as a *pass*); wedged-alive
  target on a cold slot in Draining (progress-sensitive bound fires; a healthy
  shrinking drain survives past N ticks — N-M6); source crash-restart mid-stream with
  `offset_at_save` behind the streamed head (resume refused via incarnation — N-C2's
  splice must be impossible); replica / empty-slot-map follower / just-demoted node
  datasets untouched by the reaper (N-C1); target serves `-TRYAGAIN` between
  `Complete` apply and promotion completion, no nil reads (N-C3); `Complete`
  conjunction evaluated identically on leader and follower snapshots (N-C4 —
  determinism harness); partitioned sealed source releases its held clients via the
  self-fence row while staying fenced (N-M1); `ResetCluster` discards shadows on all
  nodes (N-M2); Release/Complete and Cancel/Complete races (v2-C3); target failover
  mid-ingest and just-after-`Complete` (accepted-mode tail row; with the durability
  conjunct enabled the tail is empty); leader churn during a dead-target Draining
  (replicated observations converge); FLUSHALL on target; RESTORE-into-importing
  refused.
