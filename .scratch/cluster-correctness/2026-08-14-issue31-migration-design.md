# Slot migration redesign — source-authoritative-until-commit (v6)

Status: revision 6 — draft, pending approval. Review v6 (of revision 5) found 3 new
CRITICAL / 5 MAJOR / 6 MINOR — the two mechanical-check CRITICALs (`proposer` with no
declared home; node-local config knobs in replicated predicates) plus a residue-lifecycle
demotion hole — and 11/13 prior findings RESOLVED, 2 PARTIAL, 0 REGRESSED; this revision
resolves all fourteen.
Reviews: issue31-adversarial-review-v2/-v3/-v4/-v5/-v6, job dir 2026-08-14.
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

**Run identity** (N-C2, widened per V5-C2): `run_id = (replid, incarnation,
identity_seq)` — the full identity triple, including the ordering component defined
below, because every identity change (each of which bumps `identity_seq`) is a genuine
discontinuity that must invalidate the positions minted under it. `replid` is the source's
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
corrupt-state-file behaviour), never a reused one — and mints its ordering pair
**above its own replicated record** (V6-M2): it reads `nodes[self].run_identity` from
its applied cluster state and mints `(stored.incarnation + 1, 0)` (no stored value →
`(1, 0)`), re-fsyncing the counter file with the minted value before use. Without
this, a counter-loss node would mint `(0, 0)`, its boot `ReportRunIdentity` would be
refused by the ordering conjunct against the stored pair, and the boot ordering rule
below would mute the node permanently. A boot report refused because applied state
moved past the read re-mints above the now-stored pair and retries, a small fixed
number of times; persistent refusal **fails the boot loudly** (operator-visible
error) — the node never proceeds under an unreported identity. A lost increment
therefore cannot reproduce a prior `run_id`, and cannot mute the node.

**The run identity is replicated state** (N-C4/N-M11, type widened per V5-C2):
`NodeInfo` gains a `run_identity: Option<{replid, incarnation, identity_seq}>` field —
**all three components live in this one replicated cell**; `identity_seq` has no other
authoritative home — written in one transition by a new replicated
`ReportRunIdentity{node, run_id}` (the payload's `run_id` is the full triple).
**Proposing moments**
(V4-M4a — the FM-REPLICATION-023 one-cell-per-process identity changes, all three):
**boot**, **promotion**, and **demotion / history adoption** (FM-REPLICATION-022: a bare
`REPLICAOF` demotion ends the stint and `adopt_replication_history` replaces the replid
on link-up — a full history discontinuity that must reach the replicated field). Every
admission conjunct that mentions a run identity reads **this replicated field**, never a
node-local cell — `apply` stays a pure function of replicated state (the FM-CLUSTER-089
determinism rule, preserved). `BeginSlotMigration`'s apply captures `record.run_id` from
the source's replicated `run_identity` at apply time (defined, observable writer —
refused if the field is absent).

**Identity ordering** (V4-M4b, replicated home per V5-C2 — incarnation is constant
within a boot, but promote→demote→re-promote changes identity three times inside one
boot, so incarnation alone cannot order the reports): each node keeps `identity_seq`, a
monotone counter **persisted with the incarnation** and bumped on every identity change
(boot, promotion, demotion/adoption) — that node-local durable counter is the *mint*;
the *authoritative comparison operand* is the copy inside the replicated
`NodeInfo.run_identity` triple. `ReportRunIdentity` admission: proposer is `node` ∧
`(payload.incarnation, payload.identity_seq) > (stored.incarnation,
stored.identity_seq)` (lexicographic), **both sides read from replicated state and the
committed payload — never a node-local cell at apply** (FM-CLUSTER-089 determinism
preserved; the conjunct is evaluable identically on every applier). A retried or
reordered report from earlier in the boot is a refused no-op — it can neither regress
the field nor spuriously cancel migrations. **Every equality on a run identity
(`record.run_id == nodes[record.source].run_identity`, the receipt assertions of §4,
the resume rule) compares all three components** — a component-dropping comparison
would readmit exactly the reordering this field exists to refuse.

**Boot ordering rule** (V4-C2b; defined and scoped per V6-M2/M3): **a node proposes no
other Raft command until its boot `ReportRunIdentity` has applied** — where "has
applied" means the node's applied state shows `nodes[self].run_identity` **equal to
the reported triple** (a refused report does not satisfy the rule; it re-mints and
retries per the durability paragraph above — the loose "log-applied" reading would
re-open the vacuous-run-guard hole this rule exists to close). Without this, every
source-proposed transition has a window where the replicated field still holds the
previous boot's identity and a run-guard conjunct passes vacuously. **The rule binds
only from the moment the node's own `NodeInfo` exists in applied state** (V6-M3 —
before that there is no cell to write and the rule is vacuous), which makes the two
membership-creation flows well-defined rather than unsatisfiable: **bootstrap** orders
`AddNode(self) → ReportRunIdentity(self) → everything else` (TR-CLUSTER-028/030), and
**join** orders `AddNode` (proposed by the meeting node — TR-CLUSTER-029/031) `→
ReportRunIdentity(self) → everything else`. `ReportRunIdentity` accordingly gains the
admission conjunct `node ∈ nodes`, and its ordering conjunct is **true** against an
absent stored value — a node's first report is always admitted (the absent-operand
exception below).

**Absent-operand rule** (V6-m2 — the record's `Option` fields appear as comparison
operands, so the truth value at `None` must be defined once, not per reader): in every
admission or apply comparison, an absent (`None`) operand makes the conjunct
**false**, except where a row states otherwise. The stated exceptions, each marked at
its row: `ReportMigrationIngest`'s and `ReportTargetReplicaAck`'s monotonicity
conjuncts (`pos ≥` current) are **true** at `None` — the first report is always
admissible; `ReportTargetReplicaAck`'s upper bound (`pos ≤ target_ingested_pos`)
stays **false** at `None` — no replica ack precedes the first ingest report,
preserving V5-M3's ordering; `ObserveMigration`'s dedup (`(term, tick) >
last_observation`) is **true** at `None` — the first observation counts; and
`ReportRunIdentity`'s ordering conjunct is **true** against an absent stored value
(V6-M3, above). §9's derived metrics (the ack-lag subtraction) render only when both
operands are set.

**Target-side heads, classified** (N-M7 — two distinct positions, per §0's own rule):

- `covered_received` — highest contiguous position received on the stream. Operand of
  the per-batch contiguity assertions (checked at receipt, §4).
- `covered_applied` — highest position **durably applied** into the shadow, where
  "durably" **inherits the target's configured durability** (V5-m6): the shadow write
  path uses the same `Durability` setting as the target's ordinary writes, no more —
  a target running relaxed durability attests `covered_applied` only as strongly as
  it attests its own keyspace, and a crash can regress the shadow exactly as far as
  it can regress the main store (the resume rule then re-requests the lost window;
  the accepted-mode row in §5 states the `Complete`-side consequence). The value
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
  attempts, attempts_reset_used: bool, observations,
  last_observation: Option<(term, tick)>,
  require_target_replica_ack: bool, max_handoff_attempts: u32,
  preconfirm_observations: u32, draining_observations: u32 }
phase ∈ { Snapshotting, Streaming, Draining }   (terminal Complete/Aborted = record removed)
```

**Captured parameters** (V6-C2): the last four fields are the migration's
*parameters* — stamped into `BeginSlotMigration`'s payload from the **proposer's**
local config at proposal time and written immutably by its apply. Every replicated
predicate that consults a bound reads the **record field, never the config**:
`Prepare`'s attempts bound and `Abort`'s cancel arm read
`record.max_handoff_attempts`; the two observation bounds read
`record.preconfirm_observations` / `record.draining_observations`; `Complete`'s
optional durability conjunct exists iff `record.require_target_replica_ack`. A config
value read at admission or apply is node-local state in a replicated predicate: under
a rolling `CONFIG SET` one applier admits what another refuses — permanent Raft
divergence — and a knob flipped mid-migration silently voids the durability
conjunct the operator believed was armed. A re-issued `MIGRATING` does **not**
re-stamp (the fields are immutable); changing an open migration's parameters is
`CancelSlotMigration` + a fresh `Begin`.

**Handoff residue** (V4-C1/M7/M11 — one replicated structure closes all three):
`ClusterStateInner` gains `handoff_residue: Map<(slot, migration_id),
{source, target, promoted: bool, source_gone: bool}>` (`source_gone` per V5-M2,
below). `CompleteSlotMigration`'s apply writes the entry
(with `promoted = false`); `ReportSlotPromoted` (target) sets `promoted`;
`ConfirmSlotDeleted` (source) removes it. Both maps are carried in
`ClusterSnapshot`/`from_snapshot` (FM-CLUSTER-100 extended), so neither a crash between
apply and any node-local write nor a snapshot-shipped follower can lose the pending
promotion or the pending delete.

**Residue lifecycle under membership change and reset** (V5-M2 — both removal paths
above require the entry's `source` to still be a member, so the map needs rules for
when it is not):

- **Prune on removal, fate per `promoted` value.** When a node leaves membership
  (`RemoveNode`/`CLUSTER FORGET`, `Failover { force: true }` — the same helper
  FM-CLUSTER-036 already routes `prune_migrations_naming` through, extended to this
  map): for each residue entry naming it as **source**, if `promoted == true` the
  entry is **removed, after work-item conversion** (V6-M5, below — the previous "the
  node and its data are gone" reasoning was unfounded: `FORGET` removes membership,
  not disk, and TR-CLUSTER-035 says nothing about the keyspace; a forgotten node can
  be `MEET`-ed back holding a stale complete copy frozen at `drained_pos`,
  undeletable by the list-driven reaper once the entry is gone and re-servable
  because a later `AssignSlots` has nothing left to refuse on); if
  `promoted == false` the entry is **kept, with `source_gone = true` recorded on
  it** — dropping it would un-gate nothing and erase the only record of why the slot
  is owned-but-unserved. Entries naming the departing node as **target**: at
  `promoted == true` the entry is unaffected — the delete it gates involves only the
  source, which remains able to confirm. At `promoted == false`, a failover with a
  successor updates the entry's `target` to the promoted replica (which holds base +
  shadow via §5's full-sync rule and resumes the promotion — `ReportSlotPromoted`'s
  proposer conjunct reads the updated field); a removal with no successor leaves the
  entry in place, `source` intact, and the lossless rollback verb (`SETSLOT <slot>
  NODE <source>`) is the exit.
- **Demotion re-targets, never strands** (V6-C3 — issue 20's settled ruling makes
  demote-the-old-primary the *default* failover outcome, TR-CLUSTER-018/019, so a
  residue entry naming a demoted node is the common case, not a corner): any
  transition that changes a node's role to Replica while keeping it a member —
  `Failover { force: false }`, `SetRole`, and the demotion/adoption arm of
  `ReportRunIdentity` — **re-targets, in the same apply, every residue entry naming
  that node as `source` to the shard's current primary** (the successor holds the
  same keys via replication and its feed already reaches the shard's replicas; the
  successor's reaper performs the delete and proposes `ConfirmSlotDeleted`).
  Symmetrically, an entry naming the demoted node as **target** at
  `promoted == false` re-targets its `target` to the successor (the same rule as the
  failover-with-successor arm above). Where the role change carries no successor in
  the same apply, the entry keeps its field and the broadened admission below is the
  exit. **Broadened `ConfirmSlotDeleted`** (belt-and-braces): admissible when the
  proposer is the entry's `source`, **or** — while `nodes[entry.source].role !=
  Primary` — the current primary of the source's shard. Without this arm a
  `promoted == true` entry with a demoted source is immortal — the reaper defers on
  a Replica, the old admission named only `source`, no prune fires (the node is
  still a member), the rollback arm is `promoted == false`-only — and V5-C1's
  conjuncts then freeze the slot's entire topology surface (`Begin`, `AssignSlots`,
  `RemoveSlots` all refuse) forever.
- **Source-independent exit for `promoted == false`** (the rollback verb generalised):
  `SETSLOT <slot> NODE <n>` is admissible for any member `n` **whose role is
  Primary** (V6-M1 — a replica assignee would own a slot it cannot serve, with the
  one-shot rollback verb spent because the apply removed the entry) while a residue
  entry for the slot sits at `promoted == false`. **The lossless assignee is the
  current primary of the entry's `source`'s shard** (V6-M1 — under the demotion
  re-target rule the entry's `source` field always names that node, so "re-assign to
  the entry's `source`" remains the verb; without the definition, a demotion would
  invert the labels — the demoted node marked lossless while the successor holding
  the data required `--accept-data-loss`). Re-assigning to the lossless assignee is
  the lossless rollback (§5) — **only while `source_gone == false`** (V6-m3: a
  departed-and-rejoined source has been through `ResetCluster` and its copy is no
  longer attested; the flag is deliberately never cleared on re-join — the data's
  state is uncertain, so the conservative label sticks). Every other assignment —
  any other primary, or the source's shard with `source_gone == true` — is a
  **data-losing operator action** and is refused unless the verb carries the
  explicit loss acknowledgement (frogctl `--accept-data-loss`; the raw command form
  documents the same token), with the refusal error naming the residue entry and the
  data it abandons. Its apply re-assigns the slot and removes the entry; the target's
  discard reaper then reclaims the shadow.
- **Abandonment converts to a durable work item** (V6-M5): whenever a node's applied
  state loses a residue entry naming **itself** as `source` at `promoted == true` by
  any path other than its own reap-then-`ConfirmSlotDeleted` — membership prune,
  `ResetCluster`, snapshot install — it **first records a durable node-local delete
  work item** (journaled in the node's own persistence) for the entry's
  `(slot, migration_id)`. The reaper (§7) consumes work items alongside replicated
  entries with the same batch fences — the item itself standing in for the
  entry-exists check — and the same stop-if-this-node-owns-the-slot guard. A node
  down across its own removal misses that apply, but it re-enters a cluster only via
  `ResetCluster` + `MEET` (TR-CLUSTER-035's join path) or by catching up via
  snapshot install, and both paths run the same conversion.
- **`ResetCluster` clears `handoff_residue`** entirely, alongside its shadow-discard
  trigger (§5, TR-CLUSTER-035) — a reset abandons all pending promotions and deletes
  by construction; entries naming the applying node as `source` at
  `promoted == true` convert to work items first (the rule above).

Field writers (every field has exactly one writing transition):

| Field | Written by | Notes |
|-------|-----------|-------|
| slot, source, target, migration_id | `BeginSlotMigration` | immutable |
| run_id | `BeginSlotMigration` (from source's replicated `NodeInfo.run_identity`) | immutable; §0 |
| require_target_replica_ack, max_handoff_attempts, preconfirm_observations, draining_observations | `BeginSlotMigration` (stamped into the payload from the proposer's config at proposal time) | immutable captured parameters; V6-C2 |
| snapshot_pos | `RecordSnapshotPosition` (source) | `Option` — absent ≠ 0; immutable once set; phase → Streaming |
| attempt_id | `PrepareSlotHandoff` (mints from generation) | replaced per attempt |
| drained_pos | `ConfirmSlotHandoffDrained` (source) | **cleared** by `AbortSlotHandoff` |
| target_ingested_pos | `ReportMigrationIngest` (target) | `covered_applied`; monotone within run_id |
| target_replicas_acked_pos | `ReportTargetReplicaAck` (target) | source-space replica-ack floor (§5, V4-C3); monotone within run_id |
| attempts | `AbortSlotHandoff` (+1); reset by re-issued `MIGRATING` **at most once per record** (N-m4, §6; V6-M4) | any failed attempt counts |
| attempts_reset_used | `BeginSlotMigration` (false); re-issue arm (sets true when it resets `attempts`) | one-shot reset latch; V6-M4 |
| observations | `ObserveMigration` (+1); **reset by a `ReportMigrationIngest` that advances `target_ingested_pos` to a value still below a *set* `drained_pos`** (N-M6, narrowed per V4-M2/V5-M1 — no reset while `drained_pos` is unset) and by phase change | replicated; survives leader change |
| last_observation | `ObserveMigration` | dedup state for the counter (N-M5) |
| handoff_residue entry | `CompleteSlotMigration` (creates); `ReportSlotPromoted` (sets promoted); `ConfirmSlotDeleted` (removes); membership prune (removes-with-work-item at `promoted == true` source-departure / sets `source_gone` / re-targets on target failover — V5-M2/V6-M5); demotion transitions (re-target `source`/`target` to the shard successor — V6-C3); rollback `SETSLOT NODE` (removes); `ResetCluster` (clears map, work-item conversion first) | replicated, snapshot-carried |

### Transitions

Code-true names; `AbortSlotHandoff` (existing TR-CLUSTER-014) is the attempt-release;
`CancelSlotMigration` (existing TR-CLUSTER-015) is the whole-migration abort.
**Every transition names its proposer constraint explicitly** (V4-M10); where a
transition deliberately has none, the row says so and why.

**Global apply-determinism principle** (V6-C1/C2, stated once, the peer of the
no-wall-clock ruling): **no replicated admission or apply path reads node identity or
node-local configuration; both enter the protocol only as proposer-captured payload
fields or Begin-stamped record fields.** Admission and apply are pure functions of
replicated state plus the committed payload — the CockroachDB below-Raft rule, adopted
here because the two divergence sources it forbids (who-am-I reads and
setting-gated apply during rolling config changes) are exactly V6-C1 and V6-C2. The
two paragraphs below instantiate it.

**Payloads are fully declared** (V6-m1): each row below declares its committed
payload. Every post-`Begin` payload names `slot` — the migrations map is the
slot-keyed `BTreeMap<u16, _>`, the only declared index — and `migration_id` (matched
against the record; a mismatch is a refused stale message).

**Proposer is a committed payload field** (V6-C1): every transition whose admission
carries a proposer conjunct declares `proposer: NodeId` in its payload, stamped by
the proposing node from its own identity **at proposal time** — node-local reads at
proposal are legitimate, because the stamped value enters the log and apply stays a
pure function of replicated state plus committed payload (FM-CLUSTER-089). Every
"proposer is X" conjunct below compares `payload.proposer` against the named
replicated field — **never** `ClusterState.self_node_id`, a node-local cell whose use
in apply diverges across appliers (each applier would evaluate "am *I* the source?",
so the transition would apply on one node and refuse on the rest — permanent Raft
divergence). Same technique as TR-CLUSTER-018's proposal "carrying `old_primary_id`
as read by the proposer". Trust model unchanged: members are fault-prone, never
adversarial — the field exists for apply determinism, not authentication; a
mis-stamped proposer is screened exactly like any other faulty proposal (refused
conjuncts, dedup, run guards).

- **`BeginSlotMigration{slot, source, target, proposer, require_target_replica_ack,
  max_handoff_attempts, preconfirm_observations, draining_observations}`** (V6-C1/C2/m1
  — the four parameter fields are stamped from the proposer's config at proposal time;
  proposed by the node named `source` — the `MIGRATING` verb
  is issued on the source, §6) → record created, phase=Snapshotting, attempts=0,
  attempts_reset_used=false, observations=0, run_id captured per §0, captured
  parameters written from the payload. Admission (N-M12): proposer is `source` ∧
  (slot owned by `source` **or slot unowned** — FM-CLUSTER-032's ruled unassigned-slot
  arm, kept with its own verdict (V4-m5): a follower's slot map may legitimately be
  empty, and `Begin` on an unowned slot claims it for the source) ∧ `source != target`
  ∧ both endpoints are cluster members (FM-CLUSTER-032's `NodeNotFound` arms) ∧
  target's role is primary ∧ source's replicated `run_identity` present ∧ neither
  endpoint FAIL-flagged ∧ no open record for the slot ∧ **no `handoff_residue` entry
  for the slot** (V5-C1). The residue conjunct is load-bearing because the residue
  window is a state no other conjunct sees: after `Complete` the slot is owned by the
  target while its live data still sits in the shadow, and **the promotion re-label is
  a node-local metadata operation that emits no bytes into the source position space —
  a slot snapshot cut before promotion completes can never be repaired by the stream**.
  Equivalently: a node may not source a migration of a slot it owns but has not
  promoted, and a slot may not be migrated back to a source whose attestation-gated
  reaper may still be consuming its residue entry. **Idempotency owned explicitly**
  (FM-CLUSTER-031's surviving half): a re-issued `MIGRATING` naming the same (slot,
  source, target) over an open record answers `Ok` without a new record — and resets
  `attempts` to 0 **at most once per record** (the operator's "try again" verb, N-m4;
  one-shot per V6-M4: the reset sets `attempts_reset_used`, and further re-issues
  answer `Ok` without resetting, so the attempts bound eventually fires — a re-issue
  loop cannot defer exhaustion forever; the parameters are **not** re-stamped, V6-C2)
  — **admissible only while
  `phase ∈ {Snapshotting, Streaming}` and `record.run_id ==
  nodes[source].run_identity`** (V4-m7: mid-Draining it must not defuse an
  about-to-fire attempts bound, and it must not keep a stale-run record alive across
  the cancel `ReportRunIdentity` is about to deliver). The re-issue arm also requires
  **no `handoff_residue` entry for the slot** (V5-C1). `AssignSlots`/`RemoveSlots`
  refuse any slot with an open record **or a `handoff_residue` entry**
  (TR-CLUSTER-008/009 strengthened, V5-C1) — with the single exception of the rollback
  arm: `SETSLOT <slot> NODE <n>` against a residue entry at `promoted == false` is the
  failed-promotion recovery verb (§5/§6, incl. its V5-M2 data-loss-confirmed form) and
  is the one assignment that must remain admissible in that state; its apply removes
  the entry.
- **`RecordSnapshotPosition{slot, migration_id, run_id, pos, proposer}`** (source-proposed, after the
  snapshot is cut) → snapshot_pos=pos, phase=Streaming. Admission (N-M12, run/proposer
  guards per V4-C2): record exists ∧ migration_id matches ∧ phase==Snapshotting ∧
  **proposer is `record.source`** ∧ **`run_id == record.run_id ==
  nodes[record.source].run_identity`**. Duplicate proposal with the same value = no-op
  `Ok`; with a different value = refused (the field is immutable). Target ingests the
  snapshot, then the tail from `pos` exclusive.
- **`ReportMigrationIngest{slot, migration_id, run_id, applied_pos, proposer}`**
  (target-proposed,
  periodically). Admission (N-M4): record exists ∧ migration_id matches ∧ run_id
  matches `record.run_id` ∧ proposer is `record.target` ∧ `applied_pos ≥` current
  `target_ingested_pos` (**true at `None`** — the §0 absent-operand exception; an
  equal or lower value applies as a no-op — reports are
  idempotent, refusal is reserved for identity mismatches). Writes
  target_ingested_pos=applied_pos; if it advanced **to a value still below a *set*
  `drained_pos`**, resets `observations` to 0 (N-M6, narrowed per V4-M2, re-narrowed
  per V5-M1 — progress *toward* the token defers the bound; progress at or past the
  token must not, or a completable-but-never-completed record would defer it forever;
  and **while `drained_pos` is unset, nothing resets the counter**: in that sub-state
  the counter is measuring the *source's* failure to seal, not the target's ingest,
  and the target's coverage-driven progress is irrelevant to it — the previous "(or
  `drained_pos` is unset)" arm let source-driven keepalive coverage batches reset the
  counter forever in pre-`Confirm` Draining, a wedge with held clients and no exit).
  No attempt_id: the position space is per-run, not per-attempt, so reports
  are attempt-independent. Report cadence is node-local and is never an admission
  input — only the replicated value is.
- **`ReportTargetReplicaAck{slot, migration_id, run_id, pos, proposer}`**
  (target-proposed,
  periodically; **new**, V4-C3; counted set defined per V5-M3) → writes
  `target_replicas_acked_pos = pos`. Admission mirrors `ReportMigrationIngest`: record
  exists ∧ migration_id matches ∧ run_id matches `record.run_id` ∧ proposer is
  `record.target` ∧ `pos ≥` current value (**true at `None`**, ≤ is a no-op) ∧ `pos ≤
  record.target_ingested_pos` (**false at `None`** — both per §0's absent-operand
  exceptions; the replica floor can never attest past the primary's
  own attested possession — the two replicated positions are ordered by construction,
  V5-M3). `pos` is denominated in the **source's** position space: it is the highest
  source-space batch stamp (`to_pos`, §4) through which **every counted replica** has
  durably applied the shadow. **The counted set is the target's replica set as
  recorded in replicated cluster state** (the `NodeInfo` parent/replica relationships
  the cluster already tracks for failover scoring) — **never the target's node-local
  session table** (V5-M3: a set defined by live sessions shrinks on a single TCP
  disconnect, silently voiding the durability the knob exists to provide; a
  disconnected-but-member replica instead stalls the floor, and the migration exits
  via the observation bound, exactly the dead-replica behaviour §1 already names).
  The floor is the minimum over *that* set; the target knows each fed batch's
  source-space stamp and each counted replica's ack, so the floor is computable
  node-locally and *attested* by this replicated write. **Empty-set and unset rules**
  (V5-M3/V5-m4): with the knob on and zero counted replicas, and likewise while
  `target_replicas_acked_pos` is still `None`, `Complete`'s optional conjunct is
  **false** — never vacuously true — and the migration exits by the observation
  bound. The optional `Complete` conjunct reads only the replicated field — two
  replicated numbers, one space, deterministic at apply on every node.
- **`PrepareSlotHandoff{slot, migration_id, proposer}`** (V6-m1 — the record the
  admission reads is named by the payload, not implied; source-proposed) → mints
  attempt_id, phase=Draining, barrier
  arms on the source (per-object, issue 17/19 semantics). Full admission conjunction
  (run/proposer guards per V4-C2): record exists ∧ migration_id matches ∧
  phase==Streaming
  ∧ **proposer is `record.source`** ∧ **`record.run_id ==
  nodes[record.source].run_identity`** ∧ attempts < `record.max_handoff_attempts`
  (the captured parameter, V6-C2) ∧ neither
  endpoint FAIL-flagged ∧ no live attempt_id. The source *chooses* to propose at
  parity (`feed_head − target_ingested_pos ≤` parity threshold) — a scheduling
  heuristic, not a correctness input.
- **`ConfirmSlotHandoffDrained{slot, migration_id, attempt_id, run_id, pos,
  proposer}`** (V6-m1 — names the slot and migration it seals; source-proposed once its
  shard has no in-flight write below `pos` and the barrier holds) → drained_pos=pos.
  Admission (run/proposer guards per V4-C2 — this transition writes the position
  `Complete` compares against, so it carries the full guard): phase==Draining ∧
  attempt_id matches ∧ **proposer is `record.source`** ∧ **`run_id == record.run_id ==
  nodes[record.source].run_identity`** ∧ `pos ≥ record.snapshot_pos` (a seal below the
  snapshot's own covered position is nonsensical, V5-m1). **`drained_pos` is immutable
  within an attempt** (V5-m1, mirroring `RecordSnapshotPosition`'s rule): a duplicate
  proposal with the same value is a no-op `Ok`; a different value is refused — a
  re-seal, and in particular a *lower* re-seal that would weaken `Complete`'s
  possession proof, requires a new attempt (`AbortSlotHandoff` clears the field).
  **Drain-completeness precondition** (V4-M13):
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
- **`CompleteSlotMigration{slot, migration_id, attempt_id, token}`** (payload names
  the record it acts on, like every other transition — V5-m3; the conjunction below
  additionally requires `migration_id == record.migration_id`) (target- **or
  leader-proposed**,
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
  ∧ [if record.require_target_replica_ack]               // optional durability conjunct;
      record.target_replicas_acked_pos >= record.drained_pos
                                                         // replicated, source-space (V4-C3);
                                                         // the guard reads the captured
                                                         // record field, never the config
                                                         // knob (V6-C2)
  ```

  On apply: ownership flips, `MOVED` correct, barrier release event emitted **after**
  the assignment mutation (FM-CLUSTER-092 ordering preserved), record removed, and the
  apply writes the replicated `handoff_residue` entry `{source, target,
  promoted: false}` (V4-C1/M7/M11) — the durable, snapshot-carried registration of the
  target's pending promotion and the source's pending delete. Neither deletion nor
  promotion runs inside apply.
- **`ReportSlotPromoted{slot, migration_id, proposer}`** (target-proposed; **new**,
  V4-M11) →
  sets `promoted = true` on the residue entry. Admission: entry exists ∧ proposer is
  the entry's `target` ∧ `nodes[proposer].role == Primary` (V6-M1 — a demoted target
  must not attest a promotion; ordinarily the demotion re-target arm has already
  moved the entry's `target` to the successor, so the proposer conjunct refuses the
  old node — the role conjunct covers the successor-less window). Idempotent no-op
  if already set. Proposed by the target after
  its node-local promotion (§5) finishes.
- **`ConfirmSlotDeleted{slot, migration_id, proposer}`** (source-proposed; **new**,
  V4-M7) →
  removes the residue entry. Admission: entry exists ∧ (proposer is the entry's
  `source`, **or** — while `nodes[entry.source].role != Primary` — proposer is the
  current primary of the source's shard: the V6-C3 broadened arm, so a demoted
  source's residue always has a live remover)
  ∧ `promoted == true` (the source's delete is gated on the promotion attestation,
  V4-M11 — see §7).
- **`AbortSlotHandoff{slot, migration_id, attempt_id}`** (payload names the record —
  V5-m3) (source- or leader-proposed; stale proposals are
  screened by the attempt_id conjunct, so no proposer conjunct is load-bearing —
  stated per V4-M10) → clears drained_pos and attempt_id, attempts+=1,
  phase=Streaming, emits the barrier-release event (FM-CLUSTER-087). Refused on
  attempt_id mismatch. If attempts ≥ `record.max_handoff_attempts` (the captured
  parameter, V6-C2), the applying transition
  instead cancels the migration.
- **`ObserveMigration{slot, migration_id, attempt_id, leader_term, tick}`** (leader-proposed
  each reconcile tick **while a record sits in Draining** — V4-M2 drops v3's "without
  a completable token" qualifier, so a completable-but-uncompleted record still
  accrues) → observations+=1. **Full admission conjunction** (V4-M10): record exists ∧
  migration_id matches ∧ phase==Draining ∧ `attempt_id == record.attempt_id` ∧
  `(leader_term, tick) >` `record.last_observation` (**true at `None`** — the first
  observation counts, §0's absent-operand exception) — a stale observation from a
  finished attempt or a prior phase can neither count nor force an abort. Dedup
  (N-M5): the record's `last_observation` stores the last accepted pair; `tick` is a
  leader-local monotone counter; `leader_term` is the proposer's Raft term carried as
  opaque command data (N-m7 — the state machine compares the pair and consumes no
  other Raft metadata). **Deliberately no proposer conjunct, stated per V4-M10's rule**
  (V5-m2): cluster members are fault-prone, never adversarial — the trust model of
  every row in specs/cluster.md — and a mis-proposed observation can at worst
  accelerate the abort/attempts bounds, a retryable liveness outcome and never a
  safety one, while the `(leader_term, tick) > last_observation` dedup already screens
  replays and reordering; no proposer conjunct is load-bearing. When observations
  reaches the applicable captured bound — `record.preconfirm_observations` while
  `drained_pos` is unset, `record.draining_observations` once it is set (both
  Begin-captured record fields, never the config knobs — V6-C2/M4) — the apply forces
  the
  `AbortSlotHandoff` outcome. With the reset rule above (V4-M2, re-narrowed V5-M1) the
  bound reads as two sentences, one per `Draining` sub-state: **pre-`Confirm`
  (`drained_pos` unset), abort after `preconfirm_observations` leader observations,
  unconditionally — this is
  FM-CLUSTER-091's bounded drain-wait in log-ordered form** (a source that arms the
  barrier and never seals is exited, and its held clients are answered, no matter how
  healthily the target's coverage advances); **post-`Confirm`, abort after N leader
  observations with no target progress below the token** — a wedged drain exits, a
  large healthy shrinking drain does not, and a record whose token is completable but
  never completed is exited by the leader's auto-`Complete` or, if the conjunction
  cannot hold (e.g. the durability conjunct with a dead target replica), by this
  bound.
- **`ReportRunIdentity{node, run_id, proposer}`** (the payload's `run_id` is the full
  triple —
  `identity_seq` rides inside it, §0/V5-C2) (proposed by each node at boot,
  at promotion, and at demotion/history adoption — §0, V4-M4) → writes
  `NodeInfo.run_identity`. Admission: proposer is `node` (`payload.proposer ==
  payload.node`, V6-C1) ∧ `node ∈ nodes` (V6-M3) ∧ `(incarnation,
  identity_seq) >` stored pair (§0; **true at absent stored value** — the first
  report is always admitted, V6-M3). Applying a run_identity change for a node that is
  the **source** of any open migration **cancels those migrations** — the replicated
  form of "source restart aborts" (§4). The demotion/adoption arm additionally
  re-targets residue entries naming the node (§0's residue-lifecycle rule, V6-C3). **A target's identity change does not cancel**
  (V4-M5 — asymmetric by design: positions are denominated in the *source's* history,
  so a source discontinuity invalidates them, while a target restart invalidates
  nothing about the position space; the target's boot reconcile resumes from
  `covered_applied` or, if its shadow is unavailable, proposes `CancelSlotMigration`
  itself, §4/§5).
- **`CancelSlotMigration{slot, migration_id}`** (V6-m1; operator / source / **target** / leader —
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
  counter is the Draining exit that needs no client traffic and survives leader churn.
  Its two sub-state readings (Transitions, V5-M1): **pre-`Confirm` it fires
  unconditionally after N observations** — a source that armed the barrier but never
  seals is exited and its held clients answered, regardless of target coverage
  progress — and post-`Confirm` it fires after N observations with no target progress
  below the token, which also covers a completable token whose `Complete` is
  inadmissible (the durability conjunct with a dead target replica). It
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

**Residue liveness obligation** (V6-C3): every entry in the residue map has, at all
times, a **remover** that is either a live primary or an operator verb requiring no
member removal. While the entry's `source` is a primary, the source is the remover
(reap → `ConfirmSlotDeleted`). When the source is demoted retaining membership, the
demotion re-target (§0 residue lifecycle) moves the entry to the shard's current
primary in the same apply — and the broadened `ConfirmSlotDeleted` arm admits that
primary even for an entry the re-target missed. At `promoted == false`, rollback
(assigned to a primary, V6-M1) is the remover. Membership prune / `ResetCluster` /
snapshot install convert the entry to a durable node-local work item (V6-M5) whose
remover is the node's own reaper. A source that is *down but still a member* is the
one unremoved case, and it is bounded: either the node returns (its reaper resumes) or
the shard fails over, which re-targets the entry to the successor. No entry can reach
a state where deleting the stale copy requires removing a member.

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
| `CancelSlotMigration` applies | execute at source, acknowledged normally (release event) — **provided the source is still the slot's serving primary at apply** |
| **Cancel caused by the source's own demotion** (V5-m5: the `ReportRunIdentity` demotion/adoption arm cancels the migrations the node sources; a bare `REPLICAOF` mid-Draining lands here) | the held set is answered with **the reply the node's new role implies** — `-MOVED` to the new primary once known, else the role's refusal — and is **never executed**: a demoted node executing queued writes would fork history against its new primary |
| **Self-fence latch arms** (TR-CLUSTER-026: no Raft leader contact within an election timeout) | answer the **entire held set** `-TRYAGAIN` and **keep the fence** (N-M1) — a sealed source that cannot apply must not make held clients wait out a partition; erroring a held write is *more* fenced, not less, so the §3 invariant holds and the sealed rule ("no further execution until an exit applies") is untouched |
| Client disconnects while held | held entry dropped with the connection (no reply owed) |
| `CLIENT UNBLOCK`/`KILL` on a held client | `-UNBLOCKED` / connection close, per blocking rows |
| Failover prunes the record (a failover **not demoting this node** — V6-m6) | release event (FM-CLUSTER-087); writes follow new topology |

When one graceful failover both demotes this source and prunes its record, the
**demotion-cancel row wins**: the held set is answered per the new role and never
executed (V6-m6 — the two rows previously both matched with contradictory
dispositions).

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
  error) proposes `CancelSlotMigration` (V4-M12). **The full-sync vehicle is named,
  with verdicts on the LOCKED replication/persistence rows it rides through** (V5-M4):
  the shadow is **not** part of the staged full-sync checkpoint — FM-REPLICATION-021's
  staged-checkpoint machinery (arm/disarm, boot-time discard of an un-promoted stage)
  is untouched and its verdict is **Unchanged**; a shadow inside the stage would be
  silently dropped by exactly that disarm/discard path on the replica's next boot.
  Instead the shadow travels as a **separate, self-delimiting section of the full-sync
  payload**, after the base checkpoint, with its **own completion marker**, and is
  installed directly into the replica's shadow store keyed `(slot, migration_id)` —
  idempotent, so a re-received section overwrites cleanly. An install whose completion
  marker is absent at replica boot is **discarded and re-requested** (the replica
  re-enters resume with whatever `covered_applied` its durable shadow state attests —
  possibly none, which is a fresh section request); it is never promoted. The spec
  change adds the **replication-side row naming the `+FULLRESYNC` payload contents**
  (base checkpoint + zero or more shadow sections + markers) so the payload format has
  a LOCKED home rather than an implied shape. The main-keyspace invisibility list
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
  per-write durability escape hatch exists across a slot migration**. The row also
  absorbs V5-m6: `Complete`'s possession conjunct (`target_ingested_pos ≥
  drained_pos`) is an attestation whose disk strength is the **target's** configured
  durability — a relaxed-durability target that crashes immediately after `Complete`
  can lose the applied tail like any of its acked writes; the migration does not
  upgrade (or degrade) either node's durability contract. Operators who
  need one enable `cluster-migration-require-target-replica-ack`, which adds the
  optional conjunct at the cost of cutover latency (and, with a dead target replica,
  of the migration aborting via the observation bound rather than completing — §1).
- **Promotion** (N-C3, hardened by V4-C1/M11, gated by V5-m7): the shadow becomes the
  live keyspace as a **node-local, idempotent, resumable consequence** of the applied
  `SlotMigrationCompleted` event — never inside apply. Promotion is a **metadata
  operation** — the shadow region is re-labelled as the slot's live data (the store is
  slot-keyed already; no per-key copy) — so the window is O(1), not O(keys). **The
  re-label is crash-atomic** (V5-M4): it is a single atomic metadata flip with a named
  intermediate state (the re-label journal record), so a boot reconcile observes the
  region as *shadow* or as *live*, never half of each; an intermediate journal record
  at boot is rolled forward or back deterministically. Fail-closed belt-and-braces
  regardless: **from the instant `Complete` applies until the target's own
  `ReportSlotPromoted` applies, the target answers requests for the slot `-TRYAGAIN`**
  (V5-m7 — the gate is the *applied replicated attestation*, not local re-label
  completion: a target that serves the moment its re-label finishes has a window where
  a §5 rollback verb, racing `ReportSlotPromoted` through Raft and winning, re-assigns
  the slot to the source and silently discards writes the target already acknowledged;
  gating on the applied attestation closes that window at the cost of one Raft
  round-trip before first service, and Raft order then makes rollback-vs-promotion a
  true either/or). It never serves the slot from its (empty) main keyspace, so no
  client can read nil for a live key or write a value the promotion would clobber.
  **If the rollback verb wins the race** against a target whose local re-label already
  completed, the target's apply of that rollback **reverses the re-label back to
  shadow** — the same O(1) metadata flip, run backward — so the discard reaper finds a
  shadow, not live data; no writes were served (the serving gate above), so the
  reversal loses nothing. **The target's replicas promote on the same applied
  `ReportSlotPromoted` and answer `-TRYAGAIN` for `READONLY` reads of the slot until
  it applies** (V4-m6, re-gated per V5-m7). A target-side reconcile resumes an
  interrupted promotion at boot (the replicated residue entry survives every crash and
  snapshot, so the work is never lost — V4-M7's class). When the local re-label
  finishes, the target proposes `ReportSlotPromoted` (Transitions) — the replicated
  attestation that opens service and that the source's delete waits for (§7).
  **Failed promotion has a defined outcome** (V4-M11): if promotion fails rather than
  being interrupted (storage error; a shadow the target cannot re-label), the residue
  entry stays `promoted = false`, the slot is owned by the target but unserved
  (`-TRYAGAIN`), and — because the source's delete is gated on the attestation — **the
  source still holds a complete copy**. The recovery verb is the operator re-assigning
  the slot to the source (`CLUSTER SETSLOT <slot> NODE <source>` / frogctl): admissible
  while a residue entry for the slot exists with `promoted == false` (a race with
  `ReportSlotPromoted` is resolved by Raft order — and V5-m7's serving gate means the
  losing target has served nothing); its apply re-assigns the slot, removes the
  residue entry (reversing a completed local re-label, above), and the target's
  discard reaper then reclaims the shadow. When the source has left membership, the
  verb generalises per the §0 residue-lifecycle rules: any **primary** member is an
  admissible assignee (V6-M1 — a slot is never assigned to a replica), with
  `--accept-data-loss` required for any assignee other than the **lossless assignee**:
  the *current primary of the entry's source's shard*, and only while `source_gone ==
  false` (V5-M2/V6-M1/V6-m3 — after demotion the copy lives with the shard, addressed
  through its current primary; after prune the copy's continued existence is
  unattested and no assignee is lossless).
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
  re-issued `MIGRATING` = `Ok` + attempts reset, **pre-Draining and current-run only,
  at most once per record** (V4-m7/V6-M4), Transitions). `STABLE` = `CancelSlotMigration` (idempotent `Ok`,
  FM-CLUSTER-035 preserved). `MIGRATE`/`RESTORE` survive as key-level commands but
  resharding no longer uses them (`RESTORE` into an importing slot on the target is
  answered `MOVED <source>` by routing — §5, V4-m1); `ASKING` is accepted as a no-op
  (`+OK`); `-ASK` is never emitted. Deviation rows for all three. `CLUSTER
  SLOTS`/`SHARDS`/`NODES` render the slot under the source until `Complete`; no split
  markers ever appear.
- **`-TRYAGAIN` inventory** (N-m2, revised per V4-m1/m6): cap-breach fail-closed
  (§3); self-fence held-set release (§3); demotion-cancel held-set disposition uses
  the new role's reply, not `-TRYAGAIN` (§3, V5-m5); unpinnable held batch at
  `Complete` (§3, N-m3); `SETSLOT NODE` before the conjunction holds (§6); **target
  refusal from `Complete` apply until its `ReportSlotPromoted` applies** (§5, V5-m7 —
  the gate is the applied attestation, not local re-label completion);
  **target-replica refusal of `READONLY` reads over the same window** (§5,
  V4-m6/V5-m7). The v3 `RESTORE`-into-importing entry is removed — that request is
  answered `MOVED` by routing before any importing check (V4-m1). FM-CLUSTER-095's
  finalization `-TRYAGAIN` is retired (above). FM-CLUSTER-091 splits (V5-M1): its
  drain-wait *refusal* is retired — under source authority nothing is refused while
  the source still owns the slot: writes are held or acked, never bounced — while its
  *bounded drain-wait* property is **rewritten, not retired**, into the pre-`Confirm`
  observation bound (Transitions): a source that arms and never seals is exited after
  N leader observations, the same bound in log-ordered form.

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
residue is covered because the entry is replicated. **The delete-range is fenced by
the residue entry it consumes** (V5-C1): each bounded batch re-checks, against applied
state, that the entry still exists with `promoted == true` and that this node does not
own the slot; if the entry is gone or the slot has been re-assigned to this node, the
delete stops. (With the V5-C1 admission conjuncts this is belt-and-braces — `Begin`
and `AssignSlots` refuse the slot while the entry exists, and the rollback arm is
`promoted == false` only — but the reaper must not rely on admissions elsewhere for
its own safety.) **The reaper has no other
trigger**: it never evaluates "slots I do not own" — a predicate that would delete
every replica's entire dataset (slot ownership names primaries, so a replica owns zero
slots), every legitimately-empty-map follower's (FM-CLUSTER-032's invariant: bootstrap
assigns slots locally, not through Raft), and a just-demoted node's (TR-CLUSTER-018 +
issue 20's demote-don't-remove). Stated guards, belt-and-braces: on a node whose role
is Replica the reaper **defers** — it never runs while demoted (replicas receive the
delete-range through the feed) but the entry persists and the delete resumes if the
node is later re-promoted (V4-M7 — v3's "never on a Replica" permanently stranded the
list on a demoted source), and an empty residue list means no deletion, whatever the
slot map says. Two V6 additions: **(a)** the reaper also consumes the **durable
node-local delete work items** minted by the §0 abandonment-conversion rule (V6-M5 —
membership prune / `ResetCluster` / snapshot install removed the replicated entry
first). A work item runs under the same fences with one substitution: the item itself
is the entry-exists fence (it is deleted, atomically with the last batch, when the
delete completes), and the stop-if-this-node-owns-the-slot re-check applies unchanged.
**(b)** after a **demotion re-target** (§0, V6-C3) the entry names the shard's new
primary as `source`, so the *successor's* reaper picks it up; the defer-while-Replica
guard on the demoted node covers only the window before the re-target applies —
nothing is stranded (§1's residue liveness obligation).

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
  target_replicas_acked_pos, when the record's captured
  `require_target_replica_ack` is on — **rendered only when both operands are set**,
  V6-m2); residue-entry count and age,
  labeled by `promoted` state (a stuck `promoted == false` entry is the
  failed-promotion signal, §5); dual-storage overhead; complete/cancel counters
  labeled by reason (operator, stall, FAIL, observation-bound, overflow,
  run-id-change, restart, failover, flush, memory, shadow-unavailable).
- **`CLUSTER MIGRATIONS`**: admin-gated in the FM-CLUSTER-061..064 fail-closed class;
  RESP3 map reply, one entry per open record **and one per `handoff_residue` entry**
  (slot, source, target, migration_id, promoted, source_gone): slot, source, target,
  migration_id,
  phase, attempt_id, attempts, observations, snapshot_pos, drained_pos,
  target_ingested_pos, target_replicas_acked_pos, lag. Served from replicated state on
  any node (follower-safe; values may lag the leader — documented). Target-side
  node-local detail (ingest position, last apply error, shadow bytes, promotion state)
  appears in `INFO` section `migrations` and the debug web page on the target.
- **frogctl**: `frogctl cluster migrations` mirrors the command; `frogctl cluster
  migrate-slot --cancel` drives `CancelSlotMigration`; re-issuing the migrate verb on
  a max-attempts migration retries it once (one-shot attempts reset, §6/V6-M4 —
  after that, retry = cancel + fresh migrate); the failed-promotion
  rollback (§5) is `frogctl cluster assign-slot --to <source>`.
- **Events**: cluster **operator event log** row per transition and per cancel (with
  reason). This is a **separate stream from the client-visible `ClusterEvent`
  contract** (V6-m4): FM-CLUSTER-034's client stream is unchanged — exactly one
  `SlotMigrationCompleted` on success, nothing on failure, nothing at begin
  (FM-CLUSTER-038's wake-ups depend on that shape); per-transition rows exist only in
  the operator log.
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
| `cluster-migration-max-handoff-attempts` | 3 | attempts (any failed attempt counts; re-issued `MIGRATING` resets **once per record** — `attempts_reset_used`, V6-M4) |
| `cluster-migration-stall-strikes` | 3 | source-local strikes (§1, reset on progress or session re-establishment) |
| `cluster-migration-preconfirm-observations` | 30 | leader observations pre-`Confirm` (unconditional Draining bound while `drained_pos` unset, §1/V6-M4) |
| `cluster-migration-draining-observations` | 3 | leader observations with no target progress below the token, post-`Confirm` (§1) |
| `cluster-migration-observation-tick-ms` | 1000 ms | leader `ObserveMigration` cadence (node-local; cadence only, **never an admission input** — V6-M4) |
| `cluster-migration-backlog-max-bytes` | 64 MiB | bytes (per-migration stream backlog, §4/§8 — also the resume buffer) |
| `cluster-migration-require-target-replica-ack` | off | optional `Complete` durability conjunct via `ReportTargetReplicaAck` (§5, N-M10/V4-C3) |

**Calibration obligation** (V6-M4): `preconfirm-observations × observation-tick-ms`
must comfortably exceed the worst-case *lawful* drain — the run-to-completion tail of
§6, including a V4-M13 cross-shard VLL continuation — or the bound converts a healthy
long drain into an abort loop. This product is the design's drain-time budget in
FM-CLUSTER-091's sense; the defaults give 30 s against a barrier whose cap (§3)
already bounds held bytes.

**Knobs never enter replicated predicates directly** (V6-C2): the four admission-
relevant values (`require-target-replica-ack`, `max-handoff-attempts`,
`preconfirm-observations`, `draining-observations`) reach replicated state **only** as
immutable record fields stamped from the proposer's config into `Begin`'s committed
payload; every replicated predicate reads the record field. A `CONFIG SET` therefore
affects only migrations begun after it; changing a running migration's parameters is
`CancelSlotMigration` + fresh `Begin`.

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
  endpoint — §4 rules, now asymmetric per V4-M5; **and the residue-map demotion
  re-target rides every role→Replica transition in this family** — §0/V6-C3);
  FM-CLUSTER-034 (V6-m4 — the client-visible `ClusterEvent` contract restated:
  exactly one `SlotMigrationCompleted` on success, nothing on failure, nothing at
  begin; the §9 operator event log is a distinct stream);
  FM-CLUSTER-064 (V6-m5 — the admin-gated fail-closed split table gains the
  `CLUSTER MIGRATIONS` row); FM-CLUSTER-037 (commit-to-apply
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
  FM-CLUSTER-090/091 (barrier action; 091 splits per V5-M1: the drain-wait `-TRYAGAIN`
  refusal retired — held or acked, never bounced — and the bounded drain-wait property
  rewritten into the pre-`Confirm` unconditional observation bound, §6/Transitions);
  TR-CLUSTER-008/009 (`AssignSlots`/`RemoveSlots` refusal — strengthened to all phases
  **and to `handoff_residue` entries**, with the single rollback-arm exception —
  V5-C1); FM-CLUSTER-092/093/094 (inversion under source authority, §6);
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
  FM-CLUSTER-061..063 (the admin-gating class's semantics — V6-m5; only 064's table
  gains a row); FM-CLUSTER-095's SlotFence generation input; FM-CLUSTER-100
  (generation survives
  snapshots — extended to the new record fields, `NodeInfo.run_identity`, and
  `handoff_residue`); TR-CLUSTER-026 (self-fence — gains the held-set release
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
  no split markers. From review v5: the run-identity **triple** with three-component
  equality (V5-C2); `Begin`/re-issue residue conjuncts + the reaper's residue fence
  (V5-C1); residue lifecycle under membership change — prune-per-`promoted`,
  `source_gone`, target re-targeting on failover, generalised rollback with
  `--accept-data-loss` (V5-M2); counted-replica-set definition + empty-set/unset-false
  (V5-M3); shadow full-sync payload section with completion marker + crash-atomic
  re-label with named intermediate state (V5-M4); demotion-cancel held-write
  disposition (V5-m5); `covered_applied` durability inheritance (V5-m6);
  applied-attestation serving gate + rollback re-label reversal (V5-m7). From review
  v6: `proposer` as a committed payload field on every transition with a proposer
  conjunct, with the stamping rule and the payload-vs-`self_node_id` determinism
  argument (V6-C1); the four captured parameters as immutable record fields + the
  no-re-stamp rule (V6-C2); demotion re-target both sides + broadened
  `ConfirmSlotDeleted` proposer arm + the §1 residue liveness obligation (V6-C3);
  primary-role conjuncts on `ReportSlotPromoted` and rollback + the lossless-assignee
  definition (V6-M1); counter-loss re-mint above the replicated pair +
  "has applied" defined + loud boot failure (V6-M2); bootstrap/join proposal
  orderings + `node ∈ nodes` (V6-M3); pre/post-`Confirm` observation-bound split +
  the tick-cadence knob + the calibration obligation + one-shot attempts reset
  (V6-M4); abandonment-to-work-item conversion + reaper consumption (V6-M5); full
  payload declarations for every transition (V6-m1); the §0 absent-operand rule with
  its named exceptions (V6-m2); `source_gone` as a rollback admission operand
  (V6-m3); the held-write table's demotion-wins scoping (V6-m6).
- **Cross-tracker**: issue 15 closes only when §4's endpoint-failover/restart rows
  land; spec-gaps issue 12 (watermark carries covered position — landed `eedb76d0`) is
  the snapshot-position substrate; replication issue 24 (replid/offset pairing) is the
  replid half of `run_id`; FM-REPLICATION-014 (backlog floor — reused as the
  per-migration backlog's floor rule, §4), -021 (restart identity — plus the
  incarnation file's independence from its un-fsynced state file, §0; **and, per
  V5-M4, its staged-checkpoint disarm/discard machinery is Unchanged because the
  shadow deliberately travels *outside* the stage** — the spec change adds the
  replication-side `+FULLRESYNC` payload-contents row, §5), -022
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
     **and invariant** `inv_progressing_migration_never_aborts`. **Plus** (V6-M4) the
     bound reads the record's captured pre/post-`Confirm` fields; bounded witness
     `witnessHealthyDrainAbortedByBound` — a lawfully-draining pre-`Confirm`
     migration aborted by an undersized `preconfirm_observations` — must be
     *reachable* (the calibration obligation is real, the model shows the failure the
     obligation prevents) and must become unreachable at the default sizing.
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
     invariant must fail unless the full-sync payload carries the shadow. **Plus
     `crashDuringShadowSection(n)`** (V6, from V5-M4): a replica crash mid-section
     leaves an install without its completion marker; witness that the incomplete
     install is discarded-and-re-requested at boot and is never promoted.
  7. Per-node keyspace `source_keys: NodeId -> Set[SlotId]` + `reapSlots(n)` action +
     `inv_node_keeps_slots_it_owns` (N-C1 — the model must be able to express
     over-deletion to prove its absence).
  8. **`shadow: SlotId -> Option[MigrationId]` as a first-class variable** (V4-C1 —
     the audit's highest-value change: without a shadow variable the reaper/promotion
     race is unrepresentable): promotion is **split into two actions** (V6, from
     V5-m7): `relabelShadow(s)` (the local flip) and `reportPromoted(s)` (the
     replicated attestation), with `inv_no_serve_before_attestation` — the target
     serves the slot only after `reportPromoted` applies — and the
     rollback-wins-the-race trace (rollback admitted between the two actions
     reverses the re-label; nothing served) as a named reachable behaviour.
     `discardShadow(s)` is a *separately schedulable* action whose guard
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
      new migration). **Plus `inv_no_undeletable_stale_copy`** (V6-M5): after any
      reset/prune that removes a residue entry at `promoted == true`, the model's
      work-item variable must still hold a remover for the stale copy — reverting the
      conversion rule must violate it.
  11. `sourceCannotApply` flag + witness that the held set empties (self-fence
      release) while the flag holds (N-M1). **Plus `targetSilent`** (V4-M2 — the
      mirror escape: alive-but-unable-to-act on the target side): bounded witness
      `witnessDrainingWedgedWithCompletableToken` — `Complete`'s guard holds,
      `observations` pinned by ingest progress, held set non-empty; with the V4-M2
      fixes (leader auto-`Complete` + narrowed reset) the witness must become
      **unreachable**, a clean mutation test.
  12. **`residue: (SlotId, MigrationId) -> {source, target, promoted, source_gone}`
      as a first-class variable** (V5 audit's highest-value change — V5-C1/M2's
      states are
      unrepresentable without it; type gains `source_gone` per V6-m3): written by
      `completeMigration`, mutated by
      `failPromotion(s)` (promotion attempted, fails, entry stays
      `promoted == false`) and `removeNode(n)` (the prune-per-`promoted` rules);
      `reapSlots(n)` gains the `promoted == true` gate; `beginMigration` gains the
      residue guard, with witness `witnessBeginRefusedOverResidue`. Headline
      invariant: `inv_source_keeps_its_copy_until_promotion_attested` — reverting
      either the reaper gate or the `Begin` conjunct must violate it (the V5-C1
      snapshot-of-unpromoted-slot loss is its failure trace). **Plus `demoteNode(n)`**
      (V6-C3 — the round's highest-leverage addition: role becomes a modelled
      variable, member retained, a successor promoted in the same step) with the
      demotion re-target rules, and invariant **`residueHasARemover`** — §1's
      liveness obligation as a machine-checked property: every residue entry's
      `source` is a live primary, or a work item exists, or a rollback/prune verb is
      admissible; reverting the re-target arm must violate it.
  13. `sourceCannotDrain` flag (alive source that never proposes `Confirm`) + bounded
      witness `witnessDrainingWedgedBeforeConfirm` — reachable under v4's "(or
      `drained_pos` unset)" reset arm, **must become unreachable** with the V5-M1
      narrowing; `inv_progressing_migration_never_aborts` is **re-scoped to
      post-`Confirm` progress below the token** (pre-`Confirm` the bound fires
      regardless of progress, by design).
  14. `detachTargetReplica(n)` + the empty-counted-set state (V5-M3): with the knob
      on and zero counted replicas the `Complete` guard must be false and the
      migration must exit via the observation bound — reverting the empty-set-false
      rule must violate `inv_target_replicas_hold_committed_slot`.
  15. `reportRunIdentity(n, incarnation, identity_seq)` as a replayable action (V5-C2)
      + the boot-ordering rule as a **guard** (a node proposes no other action until
      its boot report is applied): invariants `inv_run_identity_never_regresses` and
      `inv_no_spurious_cancel` — a replayed or reordered report from earlier in the
      boot must be a refused no-op, never a migration cancel. **Plus
      `loseIncarnationCounter(n)`** (V6-M2/M3) and the bootstrap/join proposal
      sequences (`AddNode` before `ReportRunIdentity` before anything else), with
      liveness witness `witnessNodeCanAlwaysReportIdentity` — a counter-loss node
      re-minting above the replicated pair always has an admissible boot report;
      reverting the re-mint rule (minting `(0,0)`) must make the witness
      unreachable, the mute-node defect as a mutation test.
- **Stated structural limits** (recorded in the rework section, not silent): the model
  has one global applied view, so the "node acts on state it has not applied / cannot
  observe" defect class (v2-C2/C8, v3 N-C4, N-M1's cause) is discharged by spec review
  and the seam lints, not by Quint — and so is the V6-C1/C2 class (per-node
  provenance and per-node config divergence: in a single-global-view model every
  action parameter is well-defined and every knob is one value, so the model would
  have shown green over both CRITICALs). **The review checklist for that class must
  enumerate, against the replicated state-space table or a declared committed
  payload: (a) every *operand* of every admission conjunct, (b) every *guard that
  selects whether a conjunct is evaluated at all* (a bracketed `[if knob]` is an
  operand too — V6-C2's hole), and (c) every *origin predicate* ("proposer is X" —
  V6-C1's hole: an origin fact is only evaluable at apply if it is a committed
  payload field), and for each verify the field with a declared type exists and name
  the component read — a name merely appearing somewhere in the document does not
  count** (V4 audit note, strengthened per V5 and again per V6: **four consecutive
  rounds each produced instances of exactly this class** — N-C4, V4-C3, V5-C2, then
  V6-C1/C2, where the un-amended check passed revision 5 clean while both CRITICALs
  stood — so the check must bind to declared types, not names, and must cover guards
  and origins, not just comparison operands).
  **Staged-checkpoint boundary note** (V5-M4): the property "a shadow inside the
  staged checkpoint would be discarded by FM-REPLICATION-021's disarm path" belongs to
  the *replication* model's scope, not this one; the design discharges it by routing
  the shadow outside the stage, and the replication-side model (phase 3) owes the
  verdict when it lands. The model carries no temporal operators: liveness
  is expressed as named bounded witnesses with stated step counts, plus the invariant
  in (4); each property's shape (invariant vs bounded witness) is stated next to it —
  and this limit fixes the *shape* of the no-exit findings' properties (V4-M2/M8/M11
  are all bounded-witness-expressible), never an excuse to omit them (V4 audit note 1).

## Testing

- Spec-first: every rewritten/new row lands with its forcing test or an explicit
  temporary hole.
- Quint: the fifteen extensions above, mutation-validated against their named findings
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
  `drained_pos` (V4-M13). From review v5: `Begin`/`MIGRATING` over a residue entry
  refused — including migrate-onward of an unpromoted slot and migrate-back to a
  source with a live reaper (V5-C1); reaper batch stops when its residue entry
  disappears mid-delete (V5-C1 fence); source removed from membership at
  `promoted == false`: entry survives with `source_gone`, rollback to a non-source
  member refused without `--accept-data-loss`, admitted with it (V5-M2); target
  failover at `promoted == false`: successor holds base + shadow, entry re-targeted,
  successor's `ReportSlotPromoted` admitted (V5-M2); pre-`Confirm` wedged sealer with
  healthy target coverage: observation bound fires after N, held clients answered
  (V5-M1 — the v4 wedge as a forced test); knob on with zero counted replicas:
  `Complete` inadmissible, exit via bound (V5-M3); replica full-sync crash
  mid-shadow-section: incomplete install discarded at boot and re-requested, never
  promoted (V5-M4); rollback verb wins the Raft race against `ReportSlotPromoted`
  after local re-label completed: no write was served, re-label reversed, reaper
  reclaims shadow (V5-m7); demotion-cancel mid-Draining: held set answered with the
  new role's reply, never executed (V5-m5); replayed boot-era `ReportRunIdentity`
  after promotion: refused no-op, no spurious cancel (V5-C2/§0). From review v6:
  determinism harness extended to proposer-stamped payloads — every proposer conjunct
  evaluates identically on appliers whose `self_node_id` differs (V6-C1); rolling
  `CONFIG SET` of a captured knob mid-migration: `Complete` evaluates identically on
  differently-configured appliers and the open migration keeps its Begin-captured
  parameters (V6-C2); graceful failover of the source at `promoted == true`: entry
  re-targeted to the successor, successor's reaper deletes and its
  `ConfirmSlotDeleted` is admitted (V6-C3); rollback after source demotion targets
  the successor primary; a replica assignee is refused (V6-M1); lost incarnation
  counter: node re-mints above the replicated pair and boots; persistent report
  refusal fails the boot loudly, never a mute node (V6-M2); bootstrap and join
  proposal orderings satisfy the boot rule (`AddNode` then `ReportRunIdentity` then
  the rest — V6-M3); lawful long drain (cross-shard VLL continuation) survives the
  pre-`Confirm` bound at defaults; a re-issue loop cannot defer exhaustion past the
  one-shot reset (V6-M4); `FORGET`/reset at `promoted == true` converts to a work
  item, the stale copy is deleted, and a rejoined forgotten node serves no stale
  slot data (V6-M5); first `ReportMigrationIngest`/first `ObserveMigration` against
  `None` operands admitted per the §0 exceptions (V6-m2); rollback with
  `source_gone == true` requires `--accept-data-loss` (V6-m3); client `ClusterEvent`
  stream shows exactly one `SlotMigrationCompleted` despite per-transition operator
  event-log rows (V6-m4); graceful failover matching both held-write rows: the
  demotion disposition wins, held set never executed (V6-m6).
