# Slot migration redesign — source-authoritative-until-commit (v2)

Status: revision 2 — pending user approval (v1 approved 2026-08-14, then found UNSOUND by
independent adversarial review; this revision resolves review v2's 8 CRITICAL / 13 MAJOR /
8 MINOR findings. Review: issue31-adversarial-review-v2, job dir 2026-08-14.)
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
cutover handshake. CockroachDB contributes commit-time re-verification against replicated
state. FoundationDB's dual-authoritative `fetchKeys` is the documented anti-pattern
(needs global MVCC + short transactions). DragonflyDB's global client-pause finalize is
rejected as too coarse.

Structural advantage worth stating: `shard = slot % num_shards`, so a slot lives in
exactly one shard. The per-slot snapshot and the drain are single-shard operations; the
protocol never crosses shards on either endpoint.

### §0 Positions, identities, and the one sequence space (review C6, M1, M4)

One position space for the entire protocol: the **source's replication byte offset**
(the same space the replication link and full-sync checkpoint already use). Every
position-valued quantity below — `snapshot_pos`, `drained_pos`, `target_ingested_pos`,
per-batch stamps, parity lag — is denominated in this space. No RocksDB/WAL sequence
appears anywhere in the protocol. (The issue-12 "the snapshot carries its covered
position, never a post-hoc global read" rule is preserved verbatim — the covered position
is the replication offset captured before the cut, after a shard drain, exactly the shape
the full-sync path uses.)

Every position is paired with a **run identity**: `run_id` = the source's replication
history id, with the atomic replid/offset pairing of replication issue 24. A position is
meaningful only within its `run_id`. Failover mints a new replid; crash-restart can
reseed the offset head below what was already streamed. Both surface as a `run_id`
mismatch or a backward position — never as a silent continuation (§4).

Two distinct identity counters, never conflated (v1 conflated them — review C3/M1):

- **`attempt_id`** — minted from the LOCKED cluster-wide generation
  `ClusterStateInner.handoff_seq` at every accepted `PrepareSlotHandoff`. Monotone,
  never reused, survives snapshot restore (FM-CLUSTER-086/095/100 unchanged in
  mechanism; `SlotFence` keeps this as its input). Carried by **every** subsequent
  message of that handoff attempt; mismatched messages are refused.
- **`migration_id`** — minted from the same generation at `BeginSlotMigration`.
  Identifies the migration across handoff attempts; keys the target's shadow store and
  every abort/discard.

For each target-side position, the spec states whether it is the *received* head or the
*applied* head. Only the **applied** head appears in any admission predicate.

### Replicated migration record

```
{ slot, source, target, migration_id, run_id, phase,
  attempt_id: Option, snapshot_pos: Option, drained_pos: Option,
  target_ingested_pos: Option, attempts, observations }
phase ∈ { Snapshotting, Streaming, Draining }   (terminal Complete/Aborted = record removed)
```

Field writers (every field has exactly one writing transition — review M9, v1-M10):

| Field | Written by | Notes |
|-------|-----------|-------|
| slot, source, target, migration_id, run_id | `BeginSlotMigration` | immutable |
| snapshot_pos | `RecordSnapshotPosition` (source) | `Option` — absent ≠ 0; immutable once set; phase → Streaming |
| attempt_id | `PrepareSlotHandoff` (mints from generation) | replaced per attempt |
| drained_pos | `ConfirmSlotHandoffDrained` (source) | **cleared** by `AbortSlotHandoff` |
| target_ingested_pos | `ReportMigrationIngest` (target) | applied head; monotone within (run_id, attempt) |
| attempts | `AbortSlotHandoff` (+1) | any failed attempt, whatever the cause (m3) |
| observations | `ObserveMigration` (leader) | replicated; survives leader change (C4/M12) |

All fields carried in `ClusterSnapshot`/`from_snapshot` (FM-CLUSTER-100 extended).

### Transitions

Code-true names; `AbortSlotHandoff` (existing TR-CLUSTER-014) is the attempt-release —
v1's separate `ReleaseSlotHandoff` is dropped (m5). `CancelSlotMigration` (existing
TR-CLUSTER-015) is the whole-migration abort — v1's `AbortSlotMigration` is dropped.

- **`BeginSlotMigration`** → record created, phase=Snapshotting, attempts=0,
  observations=0. Admission: slot owned by `source`; no open record for slot; neither
  endpoint FAIL-flagged. `AssignSlots` refuses any slot with an open record (M6).
- **`RecordSnapshotPosition{migration_id, pos}`** (source-proposed, after the snapshot is
  cut) → snapshot_pos=pos, phase=Streaming. Target ingests the snapshot, then the tail
  from `pos` exclusive.
- **`ReportMigrationIngest{migration_id, attempt_id?, run_id, applied_pos}`**
  (target-proposed, periodically) → target_ingested_pos=applied_pos. Cadence is
  node-local and is **not** an admission input — only the replicated value is. This
  amends v1's "nothing about the target's progress is Raft state": the applied ingest
  head IS replicated state, proposed by the only actor that knows it (review C1/C8).
- **`PrepareSlotHandoff`** (source-proposed) → mints attempt_id, phase=Draining, barrier
  arms on the source (per-object, issue 17/19 semantics). Full admission conjunction
  (M7): phase==Streaming ∧ record matches (source, target) ∧
  attempts < max-handoff-attempts ∧ neither endpoint FAIL-flagged ∧ no live attempt_id.
  The source *chooses* to propose at parity (`feed_head − target_ingested_pos ≤`
  parity threshold) — a scheduling heuristic, **not** a correctness input (C1).
- **`ConfirmSlotHandoffDrained{attempt_id, pos}`** (source-proposed once its shard has no
  in-flight write below `pos` and the barrier holds) → drained_pos=pos. Refused unless
  phase==Draining ∧ attempt_id matches (C3). From this point the source's fence is
  **sealed**: it must not execute another write for the slot until it *applies*
  `AbortSlotHandoff`, `CancelSlotMigration`, or `CompleteSlotMigration` (C2).
- **`CompleteSlotMigration{attempt_id, token}`** (target-proposed) — admission is the
  full conjunction, every conjunct a pure function of the replicated record (C1/C3/C6):

  ```
  phase == Draining
  ∧ attempt_id == record.attempt_id
  ∧ record.drained_pos == Some(token)
  ∧ record.target_ingested_pos >= record.drained_pos      // applied head — possession
  ∧ record.run_id == current source run identity           // no history change
  ∧ slot_map[slot] == record.source                        // source still owner
  ```

  On apply: ownership flips, `MOVED` correct, barrier release event emitted **after**
  the assignment mutation (FM-CLUSTER-092 ordering preserved). Source-side key deletion
  is NOT part of apply — see §7.
- **`AbortSlotHandoff{attempt_id}`** (source- or leader-proposed) → clears drained_pos
  and attempt_id, attempts+=1, phase=Streaming, emits the barrier-release event
  (FM-CLUSTER-087). Refused on attempt_id mismatch. If attempts ≥
  max-handoff-attempts, the applying transition instead cancels the migration.
- **`ObserveMigration{migration_id, term, tick}`** (leader-proposed each reconcile tick
  while a record is in Draining without a completable token) → observations+=1
  (deduplicated on (term, tick) — a retried proposal cannot double-count; M12).
  When observations reaches `cluster-migration-draining-observations`, the apply forces
  the `AbortSlotHandoff` outcome (C4 — a traffic-independent, logical bound; counts of
  leader observations are data, not clocks). observations resets on phase change.
- **`CancelSlotMigration{migration_id}`** (operator / source / leader) — admitted in
  **every** phase: record removed, release event emitted if a barrier was armed (M2),
  target (and its replicas) discard the shadow store keyed by migration_id (§5).
  Idempotent: cancelling a slot with no open migration replies `Ok`
  (FM-CLUSTER-035 property preserved; M11). A cancel that races and loses to
  `Complete` replies an error stating the migration committed (v1-m7).

Ordering races (M7) all resolve mechanically by the conjunctions above: a stale
`ConfirmSlotHandoffDrained` after `AbortSlotHandoff` is refused (attempt_id cleared); a
`Complete` after `AbortSlotHandoff` is refused (phase, attempt_id, drained_pos all fail);
two `Prepare`s cannot both mint (second refused: live attempt_id); `Cancel` beats
everything except an already-applied `Complete`.

### §1 Liveness (review C4, C5, C8, M8, M12, M10)

Three layers, each with a named input the acting node **can observe**:

- **Link layer** (source-local): the migration stream session has keepalive/timeout via
  the clock seam, like every session. Dead session → source retries; resume refused
  (§4) → source proposes `CancelSlotMigration`.
- **Progress layer** (source-local — the source holds the datum: its own feed head and
  the session's ack state): strike when `target applied head < source feed head` **and**
  the applied head has not advanced since the last check — i.e. *lag that is not
  shrinking* (C5: a caught-up target never strikes; a wedged one always does). K
  consecutive strikes (`cluster-migration-stall-strikes`, default 3, cadence = the
  source's periodic tick — cadence only, never admission) → source proposes
  `CancelSlotMigration`.
- **Leader layer** (leader-local inputs only — replicated state): (a) either endpoint
  carries the replicated FAIL flag → propose `CancelSlotMigration` immediately (ruled
  criterion). Stated availability property (M8): a leader↔source partition FAIL-flags a
  healthy serving source and aborts its migrations — safe under target-discard,
  accepted. (b) the `ObserveMigration` bound above — the replicated observation counter
  is the Draining exit that needs no client traffic and survives leader churn (C4/M12).
  It also backstops a lost overflow-abort proposal (M10 — the backstop is functional
  because its input is the replicated record, not a cross-node offset).

No layer consumes a datum its actor cannot observe (C8: the leader never reads
per-session ack state; the commit predicate never reads a node-local observation).

### §2 Parity — threshold-initiate, exact-commit (review C1, m8)

- **Initiate** (scheduling heuristic, not correctness): source proposes
  `PrepareSlotHandoff` when `feed_head − target_ingested_pos ≤
  cluster-migration-parity-threshold-bytes`.
- **Commit** (correctness): the `Complete` conjunction above. Exactness lives in two
  replicated attestations — the source's seal (`drained_pos`: "I admitted nothing past
  X") and the target's report (`target_ingested_pos ≥ X`: "I durably applied through
  X") — because neither party can attest the other's fact (C1).
- Two knobs, deliberately (m8): the parity threshold tunes when to attempt cutover; the
  barrier byte cap (§3) bounds client impact during it. v1's "one knob" coupled them.

### §3 Drain bound — fail-closed byte cap (review C2, C4, M2)

During Draining, writes to the migrating slot are **held** (queued, byte-accounted) up
to `cluster-migration-barrier-max-bytes`. On breach:

- the source **does not execute the held writes** (v1's "no loss, no error" auto-resume
  was the split-authority hole — C2). Writes beyond the cap are answered `-TRYAGAIN`
  immediately; already-held writes remain held;
- the source proposes `AbortSlotHandoff{attempt_id}` and keeps its fence until that
  proposal **applies** (fail-closed; C2(a));
- on apply: phase→Streaming, attempts+=1, barrier releases, held writes execute at the
  source and are acknowledged normally.

**Named invariant (C2): the source's local fence is never weaker than the replicated
phase implies.** A node-local decision may fence *more*, never less (the clock-bound
"may narrow, never widen" preamble rule, generalized to node-local decisions).

Held-write disposition on every exit (MAJ-11 / M2):

| Exit | Reply to held writes |
|------|---------------------|
| `Complete` applies | woken with `MOVED <slot> <target>` (FM-CLUSTER-092 amended) |
| `AbortSlotHandoff` applies | execute at source, acknowledged normally |
| Cap breach, pre-apply | beyond-cap writes: `-TRYAGAIN`; held set: unchanged until apply |
| `CancelSlotMigration` applies | execute at source, acknowledged normally (release event, M2) |
| Client disconnects while held | held entry dropped with the connection (no reply owed) |
| `CLIENT UNBLOCK`/`KILL` on a held client | `-UNBLOCKED` / connection close, per blocking rows |
| Failover prunes the record | release event (FM-CLUSTER-087); writes follow new topology |

`Draining`'s exits are therefore: `Complete` (traffic-independent once parity holds),
cap breach (traffic-driven), and the leader observation bound (traffic-independent,
C4). No reachable state lacks an exit, and per FM-CLUSTER-097 the node-wide feed hold
is bounded by the same three exits — FM-CLUSTER-097's "nor a feed that can wedge"
survives with the observation bound replacing the deleted deadline; TR-CLUSTER-016's
buffered-frame byte cap additionally bounds hold *memory* (the two guarantees are
distinct and both stated).

### §4 Snapshot + stream — run-identified, coverage-stamped (review C6)

- The slot snapshot is cut after a shard drain; it **carries** its covered replication
  offset `snapshot_pos` (issue-12 rule, preserved verbatim). The mutation stream is
  slot-filtered at the source feed and starts at `snapshot_pos`, exclusive.
- Every batch is stamped `(run_id, from_pos, to_pos)` with **coverage** semantics: the
  range covers filtered-out traffic too, so a quiet slot on a busy node still advances
  the target's covered position (makes the exact token reachable; M4).
- Target assertions, in order: `run_id` equality (a change is an explicit
  discontinuity → discard, never an implicit gap); `from_pos == covered_head`
  (a **forward gap** → discard); `to_pos > covered_head` (a **backward or overlapping
  batch** → discard — regression is detected, not silently re-applied; C6 part 2).
- **Session re-establishment / source restart rule** (C6): target requests resume at
  `(run_id, covered_head)`; the source admits the resume iff its current run_id matches
  and its head ≥ covered_head with history intact; otherwise it refuses and either side
  proposes `CancelSlotMigration`. Never resume across a run_id change.
- **Endpoint failover rules** (C6 part 3 — these rows, not target-discard alone, are
  what supersedes issue 15): a failover naming the **source** aborts the migration
  (prune per TR-CLUSTER-018/FM-CLUSTER-036, adopted; release events paid,
  FM-CLUSTER-087; a successor never inherits a barrier — FM-CLUSTER-104 stays
  same-node-only). A failover naming the **target** mid-ingest aborts the migration.
  Retargeting is future work, deliberately out of scope.
- **Backpressure**: per-migration bounded backlog (bytes) on the source, separate from
  the replica-feed backlog (m9 of review v1; the migration stream is its own session
  family — see §8). Overflow → source drops the session and proposes
  `CancelSlotMigration`; the leader observation bound backstops a lost proposal (M10).
  On session death the target keeps its shadow and awaits resume; discard happens only
  on replicated cancel/abort — a transient blip never destroys ingest progress (M10).

### §5 Target-side shadow (review C7, M5, m1)

- The shadow store is keyed by `(slot, migration_id)` and lives outside the target's
  main keyspace. By construction it is invisible to `SCAN`, `KEYS`, `DBSIZE`,
  `RANDOMKEY`, `INFO keyspace`, and RDB/AOF of the main keyspace (m1). TTLs are stored,
  never enforced during Ingesting; eviction never selects shadow keys — target memory
  pressure instead **aborts the migration** (replica-semantics masking).
- `FLUSHALL`/`FLUSHDB` on the target aborts open migrations targeting it (explicit row,
  m1) rather than silently corrupting the shadow.
- **The shadow replicates through the target's normal feed during Ingesting** (C7):
  the target's replicas build the shadow alongside it. `Complete` does **not** require
  target-replica parity (coupling two replication topologies at commit is rejected);
  the residual window — target fails over between `Complete` and its replicas catching
  up on the *tail* — is an explicit accepted-mode row, same shape as TR-CLUSTER-019's
  async-lossy failover cost. This is strictly better than v1 (whole-slot loss window)
  and no worse than the Redis-style design it replaces.
- Discard (on any cancel/abort) is keyed by migration_id and propagates to the target's
  replicas as a shadow delete-range. A target restarting with a shadow store that has no
  live replicated record reaps it (target-side reconcile — the mirror of
  FM-CLUSTER-104's re-arm; M5). Ingest is refused for a migration_id whose shadow store
  is non-empty at Begin-time (a retry always gets a fresh migration_id, so stale residue
  can never contaminate a new attempt; M5).

### §6 Client-visible semantics (review M3, M6, v1 rows)

Clients never observe a split slot. No `ASK` phase exists (Redis deviation, documented
as an improvement). `MOVED` is correct only after `Complete` applies.

- **In-flight MULTI/EXEC and scripts at barrier-arm time: run to completion; their
  writes count into `drained_pos`.** This deliberately **inverts** locked
  FM-CLUSTER-092/093/094/095's redirect-don't-ack semantics — under
  source-authoritative-until-commit, acknowledging on the still-authoritative source is
  correct, and the writes are safe because `Complete` now requires the target to have
  *applied* them (C1's fix is what makes §6 sound — M3). The four rows are rewritten,
  not silently contradicted; `SlotFence` (FM-CLUSTER-095) keeps its
  generation-comparison mechanism with the sealed-fence rule of §3 layered on top.
- **FM-CLUSTER-028's key-presence probe collapses to "serve locally, always"** — the
  source holds every key of the slot until Complete. Row rewritten to state the
  simplification (M6).
- **Blocked clients** (BLPOP family) on the migrated slot: woken with `MOVED` at
  `Complete` — FM-CLUSTER-038 kept, explicitly (review blast-radius note).
- **WATCH**: FM-CLUSTER-029 kept; EXEC after cutover fails with `MOVED`.
- **SCAN cursors**: node-scoped; a completed migration moves keys to a node the old
  cursor will never visit, so a key present throughout MAY be missed — documented
  honestly as the deviation Redis also has in practice (v1-m4 wording fix).
- **Operator surface** (M6, M11):
  - `CLUSTER SETSLOT <slot> MIGRATING <target>` = `BeginSlotMigration` (one-sided;
    `IMPORTING` on the target is accepted as a no-op ack for tooling compat —
    FM-CLUSTER-031's two-sided handshake retired, row rewritten).
  - `CLUSTER SETSLOT <slot> NODE <target>`: with an open matching record = propose
    `CompleteSlotMigration` (reply `-TRYAGAIN` if the parity conjunct does not yet
    hold); with no open record it does **not** fall back to bare reassignment —
    `AssignSlots` refuses slots with open records, and bare `NODE` on a non-migrating
    slot remains the existing topology-repair verb.
  - `CLUSTER SETSLOT <slot> STABLE` = `CancelSlotMigration` (idempotent `Ok`,
    FM-CLUSTER-035 preserved).
  - `MIGRATE`/`RESTORE` survive as key-level commands (FM-CLUSTER-027's RESTORE gate
    re-derived) but resharding no longer uses them; `ASKING` is accepted as a no-op
    (`+OK`) for client-library compat; `-ASK` is never emitted. Deviation rows for all
    three.
  - `CLUSTER SLOTS`/`SHARDS`/`NODES` render the slot under the source until `Complete`;
    no split markers ever appear. Migration introspection lives in
    `CLUSTER MIGRATIONS` (§8).
  - `-TRYAGAIN` survives on exactly two paths: cap-breach fail-closed (§3) and
    `SETSLOT NODE` before parity.

### §7 Source-side deletion (review M13, v1-M8)

Deletion of the slot's keys on the source is an **event-driven, node-local consequence**
of the applied `SlotMigrationCompleted` event (the FM-CLUSTER-090 shape) — never inside
the Raft apply. It is idempotent and resumable: a source-side reconcile deletes keys
belonging to slots the replicated map says the node does not own, which also covers
crash-mid-delete residue (and is the natural mirror of §5's target-side shadow reaper).
Keyspace notifications are suppressed for migration deletes (no `del` storm to
subscribers); the deletes replicate to the source's own replicas as a bounded-rate
delete-range, not a key-at-a-time burst. Barrier-release ordering: release fires after
the assignment mutation and before deletion begins — a woken write sees `MOVED`, never a
half-deleted locally-served slot (FM-CLUSTER-092 ordering preserved).

### §8 Migration stream session (v1-review C1 — carried forward)

The mutation stream is its **own session family**, not a replica session: it shares the
wire framing/backlog code but has its own ack unit (covered position, §4), its own
keepalive, its own bounded backlog, and is **exempt by construction** from the
replica-feed hold (FM-CLUSTER-097 holds replica sessions; a migration stream feeding the
drain must not be held by the barrier it drains — the v1 self-deadlock). FM-CLUSTER-097
and TR-CLUSTER-016 are amended to state the exemption and its safety argument: the
stream carries only migrating-slot data to the migration target, which is not a replica
and never serves the slot before `Complete`.

### §9 Observability (review m6, M11, v1 §6)

- **Phases** include `Snapshotting` (m6): the operator's first question — "still
  shipping the snapshot or tailing near parity" — is answerable from the phase gauge.
- **Metrics**: per-migration phase; lag bytes (feed head − target applied);
  streamed/covered totals; attempts; observations; held-write count and held bytes;
  shadow-store bytes (target); dual-storage overhead; complete/cancel counters labeled
  by reason (operator, stall, FAIL, observation-bound, overflow, run-id-change,
  failover, flush, memory).
- **`CLUSTER MIGRATIONS`** (M11): admin-gated in the FM-CLUSTER-061..064 fail-closed
  class; RESP3 map reply, one entry per open record: slot, source, target,
  migration_id, phase, attempt_id, attempts, observations, snapshot_pos, drained_pos,
  target_ingested_pos, lag. Served from replicated state on any node (follower-safe;
  values may lag the leader — documented). Target-side node-local detail (ingest
  position, last apply error, shadow bytes — v1-m8) appears in `INFO` section
  `migrations` and the debug web page on the target.
- **frogctl**: `frogctl cluster migrations` mirrors the command; `frogctl cluster
  migrate-slot --cancel` drives `CancelSlotMigration`.
- **Events**: cluster event-log row per transition and per cancel (with reason).
- **Grafana**: migration panel via the dashboard generator (never the JSON).

### Cost (review m4)

Transient storage for one migrating slot: source copy + target shadow + target's
`R_target` replica shadows = `(2 + R_target) ×` slot bytes, plus the bounded stream
backlog and up to `cluster-migration-barrier-max-bytes` of held writes. Stated in the
spec as the bound.

## Config

| Knob | Default | Denomination |
|------|---------|--------------|
| `cluster-migration-parity-threshold-bytes` | 1 MiB | bytes (initiate heuristic, §2) |
| `cluster-migration-barrier-max-bytes` | 4 MiB | bytes (held-write cap, §3; resolves the issue-29 ambiguity — this is the surviving issue-17 bound, and it is a value, m7) |
| `cluster-migration-max-handoff-attempts` | 3 | attempts (any failed attempt counts, m3) |
| `cluster-migration-stall-strikes` | 3 | source-local observations (§1) |
| `cluster-migration-draining-observations` | 3 | leader reconcile observations (§1/C4) |
| `cluster-migration-backlog-max-bytes` | 64 MiB | bytes (per-migration stream backlog, §4) |

Zero wall-clock in any admission or abort predicate; timers drive cadence only.

## Spec / impl blast radius (review M3, M6, blast-radius section — full verdicts)

Every touched row gets an explicit verdict in the spec change; summary:

- **Rewritten**: TR-CLUSTER-010..013 (phase machine above); TR-CLUSTER-014
  (`AbortSlotHandoff` — clears drained_pos/attempt_id, increments attempts);
  TR-CLUSTER-015 (`CancelSlotMigration` — repatriation precondition retired,
  target-discard + release events); TR-CLUSTER-016 + FM-CLUSTER-097 (feed hold: bound =
  observation-bound exits + buffered-byte memory cap; migration-stream exemption, §8);
  FM-CLUSTER-026/027/028 (importing gates + probe collapse); FM-CLUSTER-029 (WATCH,
  kept, re-derived); FM-CLUSTER-031/032/033/035 (SETSLOT surface, §6);
  FM-CLUSTER-036 + TR-CLUSTER-017/018/019 (failover naming an endpoint — §4 rules);
  FM-CLUSTER-037 (commit-to-apply window with shadow); FM-CLUSTER-084 (admission
  conjunction above); FM-CLUSTER-086 (attempt stamping — every message);
  FM-CLUSTER-087 (release events from Cancel/Abort/Complete/prune);
  FM-CLUSTER-090/091 (barrier action; drain-that-never-arrives → observation bound);
  FM-CLUSTER-092/093/094/095 (inversion under source-authority, §6 — with the sealed
  fence rule); FM-CLUSTER-104 (same-node re-arm only; successor never inherits).
- **Retired**: FM-CLUSTER-085 (handoff lease — its property, "a dead finalizer cannot
  wedge a slot", is re-provided by the observation bound; replacement row states this);
  FM-CLUSTER-089 (proposer-minted deadlines — no deadlines remain).
- **Unchanged, stated**: FM-CLUSTER-038 (blocked-client wake at Complete);
  FM-CLUSTER-095's SlotFence generation input (attempt_id = same generation);
  FM-CLUSTER-100 (generation survives snapshots — extended to the new record fields);
  TR-CLUSTER-008/009 (open-record refusal of `AssignSlots` — strengthened to all
  phases); TR-CLUSTER-034 (per-node arm/release reaction).
- **New rows**: target ingest/resume + `(run_id, position)` assertions incl. backward
  detection (§4); ingest-report transition; observation bound (§1); sealed-fence /
  local-fence-never-weaker invariant (§3); held-write disposition table (§3);
  target-discard + reaper + generation keying (§5); shadow replication through the
  target feed + accepted-mode tail-loss row (§5); FLUSH/memory-pressure aborts (§5);
  source deletion as resumable step + notification suppression + ordering (§7);
  migration-stream session family + feed-hold exemption (§8); `CLUSTER MIGRATIONS`
  gating/reply (§9); Redis-deviation rows: no `ASK`, `ASKING` no-op, `MIGRATE` not
  used for resharding, no split markers in `CLUSTER SLOTS/SHARDS/NODES`.
- **Cross-tracker**: issue 15 closes only when §4's endpoint-failover rows land (review
  cross-tracker note); spec-gaps issue 12 (watermark carries covered position) is a
  stated prerequisite; replication issue 24 (replid/offset pairing) is the run_id
  substrate; cluster issue 16 (`AssignSlots` refusal) is subsumed by the strengthened
  TR-CLUSTER-008/009; issue 29's cap ambiguity resolved by the config table (m7);
  m2's cross-shard VLL continuation hole is restated in FM-CLUSTER-096's trailer as
  still-open (not silently absorbed).

## Quint rework (review quint section — adopted in full)

`specs/quint/cluster_migration_failover.qnt`:

- Keep the model's `handoff_seq` variable and `inv_handoff_seq_never_reused` **as-is**
  (it models the attempt generation; renaming it would delete a working check).
- Drop `repatriating`, `inv_abort_repatriates`, `completeRepatriation`.
- Add, per the review's six extensions: (1) `target_copy` / `source_log` high-water
  vars + `inv_no_acked_write_lost` (C1); (2) `fenced` tracked separately from `phase`
  + `localReleaseOnCapBreach` + `inv_complete_requires_fenced_source` (C2);
  (3) attempt stamping on confirm/release/complete/cancel +
  `inv_complete_requires_draining_phase` (C3); (4) `reconcileTick` + replicated
  `observations` + bounded witnesses `witnessDrainingHeldForKTicks` /
  `witnessHealthyMigrationAborted` (C4/C5/M12); (5) `sourceFailover` changing identity
  AND position space including a backward jump + `inv_stream_history_sound` (C6);
  (6) `target_replica_copy` + `inv_target_replicas_hold_committed_slot` (C7).
- Keep `feed_bytes`/`disconnected_feed` and extend for the feed-hold bound.

## Testing

- Spec-first: every rewritten/new row lands with its forcing test or an explicit
  temporary hole.
- Quint: the six extensions above are the acceptance bar — each must catch its C-finding
  when the fix is reverted in the model (mutation-style validation of the model itself).
- Turmoil/Jepsen: wedged-alive target on a cold slot in Draining (observation bound must
  fire; the feed hold must release); stalled-stream + in-flight-EXEC cutover (C1's
  scenario — Complete must be refused until the target's applied report covers the
  drained position); partition of source from leader during Draining with cap breach
  (C2 — held writes must error, never execute-then-lose); Release/Complete and
  Cancel/Complete races (C3); source failover mid-stream (run_id abort, no silent
  regression); target failover mid-ingest and just-after-Complete (accepted-mode tail
  row); leader churn during a dead-target Draining (replicated observations must still
  converge; M12); shadow reaper after target restart (M5); FLUSHALL on target (m1).
