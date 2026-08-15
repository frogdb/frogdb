# Slot migration redesign — source-authoritative-until-commit (v8)

Status: revision 8 — draft, pending approval. Review v8 (of revision 7) found 6
CRITICAL / 6 MAJOR / 5 MINOR: two of revision 7's fixes were regressions (the
self-cleanse rule was the predicate §7 forbids — V8-C1; the Demotion arm was an
unfenced role writer with no slot re-homing — V8-C2/C6), plus the target-attestation
durability hole (V8-C3), the volatile proxy precondition (V8-C4), and the LOCKED
`MIGRATE` pause-exemption seal hole (V8-C5). 9/16 of v7's findings RESOLVED, 3
PARTIAL, 2 REGRESSED. This revision resolves all of v8's findings.
Reviews: issue31-adversarial-review-v2/-v3/-v4/-v5/-v6/-v7/-v8, job dir 2026-08-14.
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
`ReportRunIdentity{node, run_id, kind, new_primary_id, proposer}` (the payload's
`run_id` is the full triple; the full payload is declared in Transitions).
**Proposing moments, each stamped into the payload as `kind`** (V7-C1 — the three
moments produce indistinguishable `(node, run_id)` triples, per FM-REPLICATION-022 a
bare-`REPLICAOF` demotion and a boot both bump `identity_seq`, so the arm a report
selects must be a committed payload fact, never inferred at apply)
(V4-M4a — the FM-REPLICATION-023 one-cell-per-process identity changes, all three):
**boot** (`kind = Boot`), **promotion** (`kind = Promotion`), and **demotion / history
adoption** (`kind = Demotion`; FM-REPLICATION-022: a bare
`REPLICAOF` demotion ends the stint and `adopt_replication_history` replaces the replid
on link-up — a full history discontinuity that must reach the replicated field).
A `Demotion` report additionally carries `new_primary_id: Option<NodeId>` — the
upstream the node now replicates, stamped at proposal time (its NodeId if the upstream
is a cluster member, else `None`). Every
admission conjunct that mentions a run identity reads **this replicated field**, never a
node-local cell — `apply` stays a pure function of replicated state (the FM-CLUSTER-089
determinism rule, preserved). `BeginSlotMigration`'s apply captures `record.run_id` from
the source's replicated `run_identity` at apply time (defined, observable writer —
refused if the field is absent). **`AddNode`'s upsert preserves the stored
`run_identity`** (V7-M3 — TR-CLUSTER-002's LOCKED rule makes `AddNode` on an existing
NodeId an upsert, and TR-CLUSTER-027 fires it live via `CONFIG SET
cluster-replica-priority`; `run_identity` is not in `AddNode`'s payload, so the upsert
must be stated field-wise: it writes only the fields its payload carries and leaves
`run_identity` untouched — a cleared cell would mute the node, since no proposing
moment exists to re-report an identity that never changed. A genuinely fresh
registration initializes the field absent, and the boot report fills it.)

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

**`run_identity` lifecycle across membership resets** (V8-M3 — the cell's life is
stated for every flow that destroys or replaces it, not only steady state):

- **`RemoveNode` / `CLUSTER FORGET` + re-`MEET`**: removal deletes the node's
  `NodeInfo` outright, `run_identity` cell included. The later re-`MEET` runs the
  **join** flow above — a fresh `AddNode` initializes the field absent, the boot
  rule re-binds from that moment, and the node's next `ReportRunIdentity` is a
  first report against an absent stored value (always admitted). The node-local
  mint (`identity_seq`, persisted with the incarnation) is *not* reset by removal —
  it keeps counting monotonically, which is harmless (admission against an absent
  cell ignores it) and load-bearing if a stale pre-removal report is still in
  flight: the re-mint after re-`MEET` orders above it.
- **`ResetCluster`**: rewinds the membership generation (TR-CLUSTER-035) and
  re-registers the node fresh — the same "field initializes absent, boot report
  fills it" path as bootstrap. The reset node re-mints (identity change: reset is
  an identity event, `identity_seq` bumps) and must complete its boot
  `ReportRunIdentity` before any other proposal, per the boot rule.
- **Raft snapshot install**: the installing node adopts the replicated `nodes` map
  wholesale, including `nodes[self].run_identity`. The installed value may be
  *older* than the node's current minted triple (the snapshot predates its latest
  report). No special arm is needed: the standing level-triggered reconcile rule
  (the `ReportRunIdentity` re-proposal arm — replicated identity facts differing
  from local facts trigger a re-report) fires on exactly this divergence, and the
  boot rule's "has applied" test is evaluated against post-install applied state,
  so a node mid-install cannot satisfy it with pre-install state.

**Absent-operand rule** (V6-m2 — the record's `Option` fields appear as comparison
operands, so the truth value at `None` must be defined once, not per reader;
scope sharpened per V8-M6): the rule governs conjuncts that **read** an optional
operand's *value* — a comparison, arithmetic, or lookup through it — in every
admission or apply comparison such a conjunct is
**false** at `None`, except where a row states otherwise. A conjunct that **tests
for absence itself** — `x == None` as the predicate — is not a value read and the
rule does not apply to it; any row using an absence test says so explicitly at the
row (without this distinction, `ClearSlotResidue`'s `shard_primary(entry.source)
== None` conjunct would be false-at-`None` under the rule's own text and the
last-resort verb would be dead in exactly the successor-less state it exists
for). The stated value-read exceptions, each marked at
its row: `ReportMigrationIngest`'s and `ReportTargetReplicaAck`'s monotonicity
conjuncts (`pos ≥` current) are **true** at `None` — the first report is always
admissible; `ReportTargetReplicaAck`'s upper bound (`pos ≤ target_ingested_pos`)
stays **false** at `None` — no replica ack precedes the first ingest report,
preserving V5-M3's ordering; `ObserveMigration`'s dedup (`(term, tick) >
last_observation`) is **true** at `None` — the first observation counts; and
`ReportRunIdentity`'s ordering conjunct is **true** against an absent stored value
(V6-M3, above). The declared absence tests: `ClearSlotResidue`'s
`shard_primary(entry.source) == None` conjunct (its row, V8-M6). §9's derived
metrics (the ack-lag subtraction) render only when both
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
  the accepted-mode row in §5 states the `Complete`-side consequence; and because
  the replicated `target_ingested_pos` is monotone only *within* a target run, the
  post-crash report **replaces** the stale-high value rather than being refused —
  V8-C3's attesting-run pairing on the two report transitions, with `Complete`
  requiring the attesting run to be the target's current one). The value
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
{ slot: SlotId (u16), source: NodeId, target: NodeId,
  migration_id: MigrationId (u64, minted from handoff_seq), run_id: RunId (the §0 triple),
  phase: Phase,
  attempt_id: Option<AttemptId (u64, minted from handoff_seq)>,
  snapshot_pos: Option<ReplPos (u64, source replication offset)>,
  drained_pos: Option<ReplPos>,
  target_ingested_pos: Option<ReplPos>, target_replicas_acked_pos: Option<ReplPos>,
  target_attesting_run: Option<RunId (the §0 triple)>,
  attempts: u32, attempts_reset_used: bool, observations: u32,
  last_observation: Option<(term: u64, tick: u64)>,
  require_target_replica_ack: bool, max_handoff_attempts: u32,
  preconfirm_observations: u32, draining_observations: u32 }
phase ∈ { Snapshotting, Streaming, Draining }   (terminal Complete/Aborted = record removed)
```

(V7-m4: every field carries its type; all four `Option` positions share `ReplPos`,
the §0 source-replication-offset space.)

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
`ClusterStateInner` gains `handoff_residue: Map<(slot: SlotId, migration_id:
MigrationId), {source: NodeId, target: NodeId, promoted: bool, source_gone: bool,
target_gone: bool}>` (`source_gone` per V5-M2, `target_gone` per V7-M6,
below). `CompleteSlotMigration`'s apply writes the entry
(with `promoted = false`, both flags false); `ReportSlotPromoted` (target) sets
`promoted`; `ConfirmSlotDeleted` (source) removes it. Both maps are carried in
`ClusterSnapshot`/`from_snapshot` (FM-CLUSTER-100 extended), so neither a crash between
apply and any node-local write nor a snapshot-shipped follower can lose the pending
promotion or the pending delete.

**Invariant: at most one `handoff_residue` entry per slot** (V8-m5 — the map is keyed
`(slot, migration_id)`, so nothing structural prevents two). Forcing argument:
`Complete` is the entry's **sole creator**, and `BeginSlotMigration` **refuses any
slot with an existing residue entry** (V5-C1's conjunct) — so a second migration of
the slot, whose `Complete` would mint the second entry, cannot begin until the first
entry's remover (§1's enumeration) has run. Every non-creating writer mutates or
removes the existing entry; none inserts. State this as a checked invariant
(`inv_at_most_one_residue_per_slot`) rather than an assumption — it is one dropped
`Begin` conjunct away from false.

**`shard_primary(n)` — the derivation every "current primary of the shard" phrase
below means, defined once** (V7-M2 — the state space has no shard object, so the
phrase must be a total function of declared fields): `shard_primary(n)` = `n` itself
if `nodes[n].role == Primary`; otherwise follow `nodes[n].primary_id` (SS-3)
transitively, at most `|nodes|` steps. The result is `None` — not an error — when the
pointer is absent (`primary_id == None`), dangling (names a non-member), or the walk
cycles or exhausts its step bound without reaching a Primary (transient states
`RemoveNode` reparenting or interleaved role writes can produce). Every conjunct that
reads `shard_primary(x)` is **false at `None`** (the §0 absent-operand rule); every
re-target arm that writes it **skips** at `None` (the entry keeps its field; the §1
remover enumeration's operator verb is the exit). The **lossless assignee** of a
residue entry is `shard_primary(entry.source)`; at `None` there is no lossless
assignee and every assignment of the slot requires the explicit loss acknowledgement.
The function reads only replicated `NodeInfo` fields, so it is evaluable identically
on every applier.

**Residue lifecycle under membership change and reset** (V5-M2 — both removal paths
above require the entry's `source` to still be a member, so the map needs rules for
when it is not):

- **Prune on removal marks, never removes** (V7-C2 revises V6-M5's conversion — the
  work-item mechanism is dropped entirely: it was edge-triggered on the pruned node's
  *own* apply of the removal, an apply a node down across its `FORGET` never
  performs, and it downgraded the replicated guard that `Begin`/`AssignSlots`/the
  re-issue arm read into a node-local item none of them can see). When a node leaves
  membership (`RemoveNode`/`CLUSTER FORGET`, `Failover { force: true }` — the same
  helper FM-CLUSTER-036 already routes `prune_migrations_naming` through, extended
  to this map): for each residue entry naming it as **source**, whatever the
  `promoted` value, the entry is **kept, with `source_gone = true` recorded on it**
  — `FORGET` removes membership, not disk (TR-CLUSTER-035 says nothing about the
  keyspace), so a forgotten node can be `MEET`-ed back holding a stale complete copy
  frozen at `drained_pos`; the surviving replicated entry keeps `Begin` and
  `AssignSlots` refusing the slot (V5-C1's conjuncts read this map), and the §1
  remover enumeration names the entry's exit. The stale copy itself never re-enters
  a cluster — the **join-empty admission gate** (below, V8-C1) refuses the
  rejoining node until the copy is wiped — and the surviving entry's exit is the
  token-gated `ClearSlotResidue` verb. Entries naming the departing node as
  **target**: at `promoted == true` the entry is kept, and **if the removal leaves
  the slot unassigned** (`RemoveNode` unassigns the departed owner's slots) the
  entry records `target_gone = true` (V7-M6) — while that flag is set **the
  source's reaper defers** (§7): the source's copy is now the last in-cluster copy
  of an owner-less slot, and reaping it on one `CLUSTER FORGET` would destroy it.
  The exits are token-gated (both are data-losing with respect to the departed
  target's post-promotion writes): `AssignSlots` of the slot **to the entry's
  source** with the loss acknowledgement removes the entry — the source keeps and
  serves its retained copy; `AssignSlots` **to any other node** with the
  acknowledgement clears `target_gone` and keeps the entry (it becomes ordinary
  `promoted == true` residue: the source reaps and confirms). At
  `promoted == false`, a failover with a
  successor updates the entry's `target` to the promoted replica (which holds base +
  shadow via §5's full-sync rule and resumes the promotion — `ReportSlotPromoted`'s
  proposer conjunct reads the updated field); a removal with no successor leaves the
  entry in place, `source` intact, and the lossless rollback verb (the declared
  `AssignSlots` rollback arm, below) is the exit.
- **Demotion re-targets, never strands** (V6-C3 — issue 20's settled ruling makes
  demote-the-old-primary the *default* failover outcome, TR-CLUSTER-018/019, so a
  residue entry naming a demoted node is the common case, not a corner): any
  transition that changes a node's role to Replica while keeping it a member —
  `Failover { force: false }`, `SetRole`, and the `kind == Demotion` arm of
  `ReportRunIdentity` — **re-targets, in the same apply, every residue entry naming
  that node as `source` to `shard_primary(node)`** (evaluated after the arm's own
  role/`primary_id` writes; the successor holds the
  same keys via replication and its feed already reaches the shard's replicas; the
  successor's reaper performs the delete and proposes `ConfirmSlotDeleted`).
  Symmetrically, an entry naming the demoted node as **target** at
  `promoted == false` re-targets its `target` to the successor (the same rule as the
  failover-with-successor arm above). Where `shard_primary(node)` is `None` — the
  role change carries no successor —
  the re-target **skips**: the entry keeps its field, and the level-triggered
  `RetargetSlotResidue` transition (below) is the catch-up. **`RetargetSlotResidue`
  — the level-triggered residue re-home** (V8-C4 replaces V7-C1's broadened
  `ConfirmSlotDeleted` proxy arm: that arm's proposal precondition — "the demoted
  source has completed a full resync from the proxy" — was node-local, volatile
  across proxy restarts, and **never true on the partial-resync path**
  (FM-REPLICATION-021/022's replid2 lineage makes a demoted primary's catch-up a
  *partial* resync precisely when nothing diverged), so the nominal remover existed
  on paper, never in fact, and `ClearSlotResidue` refused because a lawful remover
  "existed" — an immortal entry with no operator exit): any primary `P` that
  observes, in applied state, a residue entry whose `source` is a non-primary
  member of `P`'s own shard (`nodes[entry.source].role != Primary ∧
  shard_primary(entry.source) == P`) proposes re-writing the entry's `source` to
  `P` (declared transition, Transitions below). The entry thereby converges, while
  any successor exists, onto a live primary that holds the same copy via
  replication — whose reaper then runs the ordinary attested path (§7: delete own
  copy, propose `ConfirmSlotDeleted` as the entry's source). Level-triggered from
  current applied state, so it needs no observation of the demotion itself: a
  successor promoted years later still picks the entry up. The in-apply demotion
  re-target above remains the fast path; this transition is the re-derivable rule
  that survives missed edges (the V7-m1 discipline). Without *some* such rule a
  `promoted == true` entry with a demoted source is immortal — the reaper defers on
  a Replica, the base admission names only `source`, no prune fires (the node is
  still a member), the rollback arm is `promoted == false`-only — and V5-C1's
  conjuncts then freeze the slot's entire topology surface (`Begin`, `AssignSlots`,
  `RemoveSlots` all refuse) forever.
- **Source-independent exit for `promoted == false`** (the rollback verb generalised;
  **declared as the `AssignSlots` rollback arm in Transitions** — V7-M5: the verb's
  admission reads `accept_data_loss` and the assignee's role, so it must be a
  declared transition with a declared payload, not a §5 narrative):
  `SETSLOT <slot> NODE <n>` is admissible for any member `n` **whose role is
  Primary** (V6-M1 — a replica assignee would own a slot it cannot serve, with the
  one-shot rollback verb spent because the apply removed the entry) while a residue
  entry for the slot sits at `promoted == false`. **The lossless assignee is
  `shard_primary(entry.source)`** (V6-M1/V7-M2 — under the demotion
  re-target rule the entry's `source` field always names that node, so "re-assign to
  the entry's `source`" remains the verb; without the definition, a demotion would
  invert the labels — the demoted node marked lossless while the successor holding
  the data required `--accept-data-loss`; at `shard_primary == None` there is no
  lossless assignee). Re-assigning to the lossless assignee is
  the lossless rollback (§5) — **only while `source_gone == false`** (V6-m3: a
  departed-and-rejoined source has been through `ResetCluster` and its copy is no
  longer attested; the flag is deliberately never cleared on re-join — the data's
  state is uncertain, so the conservative label sticks). Every other assignment —
  any other primary, or the source's shard with `source_gone == true` — is a
  **data-losing operator action** and is refused unless the payload carries
  `accept_data_loss = true` (stamped from frogctl `--accept-data-loss`; the raw
  command form
  documents the same token), with the refusal error naming the residue entry and the
  data it abandons. Its apply re-assigns the slot and removes the entry; the target's
  discard reaper then reclaims the shadow.
- **Join-empty admission: stale copies never enter a cluster untracked** (V8-C1
  replaces V7-C2's self-cleanse — that rule's deletion predicate, "slots not
  assigned to me, not in an open record, not in a residue entry naming me", is
  exactly the ownership-absence predicate §7 forbids: slot ownership names
  *primaries*, so on every replica all 16384 slots satisfied it and the rule would
  have deleted every replica's dataset, every empty-slot-map follower's
  (FM-CLUSTER-032: bootstrap assigns slots locally before the map replicates), and
  every just-demoted node's. No unilateral deletion rule survives this revision;
  the design closes the *entrance* instead, with two node-local fail-closed
  admission gates — the Redis analogs):
  1. **Join-empty**: a node accepts cluster membership — the `MEET`/`AddNode`
     handshake, both first join (TR-CLUSTER-029/031) and re-join after `FORGET` —
     only while its main keyspace is empty. A non-empty joiner **refuses the
     handshake node-locally**, emitting an operator event naming the non-empty
     slots (Redis's rule: a node holding data cannot join; the operator wipes
     first — `FLUSHALL`, or decommission). The refusal is proposal-side/handshake-
     side, never an apply-time deletion: fail-closed, deletes nothing.
  2. **`ResetCluster` refuses on a non-empty main keyspace** (Redis `CLUSTER RESET
     HARD` requires an empty DB): reset is the path back toward a re-join, so the
     same gate holds there; the error names the non-empty slots and the wipe the
     operator must perform deliberately.

  With both gates, a member node only ever holds slot data in a *tracked* state —
  owner, replica of an owner, open-record source/target, or residue-entry source —
  and every tracked state has a declared remover (§1's enumeration: reaper,
  rollback arm, re-target rules, `ClearSlotResidue`). The untracked-stale-copy
  class the self-cleanse chased can no longer be created inside a cluster. §5's
  promotion precondition (live region empty) stays as defence-in-depth. Residual,
  stated: a stale copy on a node that never rejoins sits outside every rule's
  reach — it is the operator's disk; `ClearSlotResidue`'s `accept_stale_copy`
  token is precisely the operator's attestation of having dealt with it.
- **`ResetCluster` clears `handoff_residue`** entirely, alongside its shadow-discard
  trigger (§5, TR-CLUSTER-035) — a reset abandons all pending promotions and deletes
  by construction; the stale copies those entries gated cannot re-enter a cluster,
  because the join-empty gate (above) refuses the rejoining node until its keyspace
  is wiped.

Field writers (every field has exactly one writing transition):

| Field | Written by | Notes |
|-------|-----------|-------|
| slot, source, target, migration_id | `BeginSlotMigration` | immutable |
| run_id | `BeginSlotMigration` (from source's replicated `NodeInfo.run_identity`) | immutable; §0 |
| require_target_replica_ack, max_handoff_attempts, preconfirm_observations, draining_observations | `BeginSlotMigration` (stamped into the payload from the proposer's config at proposal time) | immutable captured parameters; V6-C2 |
| snapshot_pos | `RecordSnapshotPosition` (source) | `Option` — absent ≠ 0; immutable once set; phase → Streaming |
| attempt_id | `PrepareSlotHandoff` (mints from generation) | replaced per attempt |
| drained_pos | `ConfirmSlotHandoffDrained` (source) | **cleared** by `AbortSlotHandoff` |
| target_ingested_pos | `ReportMigrationIngest` (target) | `covered_applied`; monotone within the target's attesting run; a cross-run report **replaces** (may regress — V8-C3) |
| target_replicas_acked_pos | `ReportTargetReplicaAck` (target); **cleared** by a cross-run `ReportMigrationIngest` (V8-C3) | source-space replica-ack floor (§5, V4-C3); monotone within the target's attesting run |
| target_attesting_run | `ReportMigrationIngest` (target — set/replaced with each admitted report's `target_run`; V8-C3) | which target run attests the two positions above; `Complete` requires it current |
| attempts | `AbortSlotHandoff` (+1); reset by re-issued `MIGRATING` **at most once per record** (N-m4, §6; V6-M4) | any failed attempt counts |
| attempts_reset_used | `BeginSlotMigration` (false); re-issue arm (sets true when it resets `attempts`) | one-shot reset latch; V6-M4 |
| observations | `ObserveMigration` (+1); **reset by a `ReportMigrationIngest` that advances `target_ingested_pos` to a value still below a *set* `drained_pos`** (N-M6, narrowed per V4-M2/V5-M1 — no reset while `drained_pos` is unset); **reset by a `ReportTargetReplicaAck` that advances `target_replicas_acked_pos` to a value still below a *set* `drained_pos`** (V8-M2 — same toward-the-token rule); **reset by `ConfirmSlotHandoffDrained`'s apply** (V7-C3 — the seal switches the applicable bound from `preconfirm_observations` to the smaller `draining_observations`, a phase change in all but name; carrying pre-seal ticks across it would abort every lawful drain longer than the small bound at stock defaults); and by phase change | replicated; survives leader change |
| last_observation | `ObserveMigration`; **cleared by `ConfirmSlotHandoffDrained`'s apply** (with the counter — V7-C3) | dedup state for the counter (N-M5) |
| handoff_residue entry | `CompleteSlotMigration` (creates); `ReportSlotPromoted` (sets promoted); `ConfirmSlotDeleted` (removes); membership prune (sets `source_gone` on source-departure, sets `target_gone` on target-departure that unassigns the slot, re-targets on target failover — V5-M2/V7-C2/V7-M6); demotion transitions (re-target `source`/`target` to `shard_primary` — V6-C3; skip at `None`); `RetargetSlotResidue` (re-writes `source` to the shard's current primary — V8-C4); `AssignSlots` rollback arm (removes; clears `target_gone` when re-homing an orphaned `promoted == true` entry — V7-M5/M6); `ClearSlotResidue` (removes — V7); `ResetCluster` (clears map) | replicated, snapshot-carried |

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
two paragraphs below instantiate it. **Scope carve-out** (V7-m1 — the principle
binds admission and apply, not everything downstream): *post-apply node-local
reactions* — the reaper (§7), promotion (§5), the join-empty admission gate (§0),
barrier arm/release — legitimately read `self_node_id` to decide whether the applied state
assigns *them* work; that read diverges across nodes by design and never feeds back
into a replicated predicate. The discipline for this class: every durable effect of
a reaction must be **level-triggered** — re-derivable from current applied state
alone, never dependent on having observed a particular past transition (V7-C2 is
what happens otherwise).

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
  `slot_map[slot] == source` (**the unowned-slot arm is dropped** — V7-M4:
  the arm's "claims it for the source" named no slot-map writer, `Begin`'s apply
  writes no assignment, and SS-6's writer list excludes it — so a migration begun on
  an unowned slot reached `Complete` with `slot_map[slot] == record.source`
  guaranteed false: a doomed record holding client writes each attempt. Migrating an
  unassigned slot is meaningless — assign it; the refusal message directs the
  operator to `AssignSlots`. FM-CLUSTER-032's unassigned-slot arm is **retired**
  with its own verdict) ∧ `source != target`
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
  (TR-CLUSTER-008/009 strengthened, V5-C1) — with exactly two exceptions, both
  declared as `AssignSlots` arms in Transitions (V7-M5/M6): the **rollback arm**
  against a residue entry at `promoted == false` (the
  failed-promotion recovery verb, §5/§6, incl. its V5-M2 data-loss-confirmed form;
  its apply removes the entry) and the **orphan re-home arm** against an entry at
  `promoted == true ∧ target_gone == true` (V7-M6 — the exit for a slot whose owner
  was forgotten).
- **`RecordSnapshotPosition{slot, migration_id, run_id, pos, proposer}`** (source-proposed, after the
  snapshot is cut) → snapshot_pos=pos, phase=Streaming. Admission (N-M12, run/proposer
  guards per V4-C2): record exists ∧ migration_id matches ∧ phase==Snapshotting ∧
  **proposer is `record.source`** ∧ **`run_id == record.run_id ==
  nodes[record.source].run_identity`**. Duplicate proposal with the same value = no-op
  `Ok`; with a different value = refused (the field is immutable). Target ingests the
  snapshot, then the tail from `pos` exclusive.
- **`ReportMigrationIngest{slot, migration_id, run_id, target_run, applied_pos,
  proposer}`**
  (target-proposed,
  periodically; `target_run` is the **target's own** §0 run-identity triple,
  stamped at proposal — V8-C3). Admission (N-M4): record exists ∧ migration_id
  matches ∧ run_id
  matches `record.run_id` ∧ proposer is `record.target` ∧ **`target_run ==
  nodes[record.target].run_identity`** (V8-C3 — the boot-ordering rule guarantees
  the target's replicated identity is current before it proposes anything else, so
  this conjunct refuses exactly the stale-run reports; replicated-field
  comparison, deterministic at apply) ∧ the position rule, split by attesting run:
  **same-run** (`target_run == record.target_attesting_run`, or
  `target_attesting_run` is `None` — first report): `applied_pos ≥` current
  `target_ingested_pos` (**true at `None`** — the §0 absent-operand exception; an
  equal or lower value applies as a no-op — reports are
  idempotent, refusal is reserved for identity mismatches); **cross-run**
  (`target_run ≠ record.target_attesting_run`): **no monotonicity conjunct — the
  report REPLACES** (V8-C3: `covered_applied` inherits the target's configured
  durability (§0), so a target crash-restart can lawfully *regress* the shadow
  below the last reported value while the replicated `target_ingested_pos`,
  monotone within a run, stays stale-high; a monotone-across-runs field is a
  possession proof the possessor no longer backs, and the leader's auto-`Complete`
  would harvest it — every acked write in `(durable shadow floor, drained_pos]`
  lost forever, silently). Writes `target_ingested_pos = applied_pos`,
  `target_attesting_run = target_run`, and — on a cross-run replacement — **clears
  `target_replicas_acked_pos`** (the replica floor was attested under the old run;
  the new run's floor must be re-reported). If the write advanced the position
  **to a value still below a *set*
  `drained_pos`**, resets `observations` to 0 (N-M6, narrowed per V4-M2, re-narrowed
  per V5-M1 — progress *toward* the token defers the bound; progress at or past the
  token must not, or a completable-but-never-completed record would defer it forever;
  and **while `drained_pos` is unset, nothing resets the counter**: in that sub-state
  the counter is measuring the *source's* failure to seal, not the target's ingest,
  and the target's coverage-driven progress is irrelevant to it — the previous "(or
  `drained_pos` is unset)" arm let source-driven keepalive coverage batches reset the
  counter forever in pre-`Confirm` Draining, a wedge with held clients and no exit; a
  cross-run *regression* is not an advance and never resets).
  No attempt_id: the position space is per-run, not per-attempt, so reports
  are attempt-independent. Report cadence is node-local and is never an admission
  input — only the replicated value is.
- **`ReportTargetReplicaAck{slot, migration_id, run_id, target_run, pos, proposer}`**
  (target-proposed,
  periodically; **new**, V4-C3; counted set defined per V5-M3) → writes
  `target_replicas_acked_pos = pos`. Admission mirrors `ReportMigrationIngest`: record
  exists ∧ migration_id matches ∧ run_id matches `record.run_id` ∧ proposer is
  `record.target` ∧ **`target_run == nodes[record.target].run_identity` ∧
  `target_run == record.target_attesting_run`** (V8-C3 — the floor attests under
  the same target run as the possession value it bounds; **false at `None`** — no
  ack before the run's first ingest report, which also preserves V5-M3's
  ordering) ∧ `pos ≥` current value (**true at `None`**, ≤ is a no-op) ∧ `pos ≤
  record.target_ingested_pos` (**false at `None`** — both per §0's absent-operand
  exceptions; the replica floor can never attest past the primary's
  own attested possession — the two replicated positions are ordered by construction,
  V5-M3). **If the write advanced the floor to a value still below a *set*
  `drained_pos`, the apply resets `observations` to 0** (V8-M2 — the same
  toward-the-token rule as the ingest reset: with
  `require_target_replica_ack` on, post-`Confirm` completion waits on *this*
  floor, and replica-ack progress that defers the bound is exactly as lawful as
  ingest progress; without the reset, a healthy replica set that needs more than
  `draining_observations` ticks (default 3) to drain the fence window aborts
  every knob-on migration at stock defaults). `pos` is denominated in the **source's** position space: it is the highest
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
  shard has no in-flight write below `pos` and the barrier holds) → drained_pos=pos,
  **and the apply resets `observations` to 0 and clears `last_observation`**
  (V7-C3 — the seal switches the applicable bound from `preconfirm_observations`
  (default 30) to `draining_observations` (default 3); without the reset, any lawful
  pre-seal drain that accrued more than the *smaller* bound's worth of ticks would
  abort on the first post-seal observation — every drain longer than 3 ticks, at
  stock defaults. The sub-state change is a phase change in all but name and gets
  the phase-change reset).
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
  ∧ record.target_attesting_run                          // the possession proof is
      == Some(nodes[record.target].run_identity)         // backed by the target's
                                                         // CURRENT run (V8-C3): any
                                                         // target restart bumps
                                                         // incarnation, falsifying
                                                         // this until the new run
                                                         // re-attests its (possibly
                                                         // regressed) coverage
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
  removes the residue entry. Admission: entry exists ∧ proposer is the entry's
  `source` (V8-C4 reverts the V7-C1 broadened proxy arm — its resync
  precondition was node-local, volatile, and never true on the partial-resync
  path, §0; a demoted source's entry instead **converges onto a live primary**
  via the level-triggered `RetargetSlotResidue` below, after which the ordinary
  source-only admission holds)
  ∧ `promoted == true` (the source's delete is gated on the promotion attestation,
  V4-M11 — see §7) ∧ `target_gone == false` (V7-M6 — while the flag is set the
  source's copy is the last in-cluster copy; the re-home arm clears it first).
- **`RetargetSlotResidue{slot, migration_id, new_source, proposer}`**
  (primary-proposed; **new**, V8-C4 — §0's level-triggered residue re-home) →
  re-writes the entry's `source` to `payload.new_source`, touching nothing else.
  Admission: entry exists ∧ migration_id matches ∧ `nodes[entry.source].role !=
  Primary` ∧ `payload.new_source == shard_primary(entry.source)` (§0's total
  definition; false at `None`, so a successor-less shard refuses — `ClearSlotResidue`
  is that state's exit) ∧ `nodes[new_source].role == Primary` ∧ `proposer ==
  new_source` (the proposer re-homes the entry to *itself*: it holds the copy via
  replication and its reaper takes over). Proposal side is the level rule of §0:
  every primary's reconcile scans applied residue entries and proposes for each one
  whose `source` is a non-primary in its own shard. Idempotent by admission (once
  re-written, `entry.source` is a Primary and the arm refuses); a race between two
  successors across a failover resolves by the `shard_primary` conjunct — only the
  current successor's proposal is admissible at apply.
- **`AssignSlots{slots, node, proposer, accept_data_loss: bool}`** (operator-proposed;
  the existing TR-CLUSTER-008 transition, extended — V7-M5: the rollback verb's
  admission reads role, `source_gone`, and the loss token, so the verb is this
  declared transition, not a narrative; `accept_data_loss` defaults false and is
  stamped from frogctl `--accept-data-loss` / the raw `SETSLOT NODE` token). For a
  slot with **no** open record and **no** residue entry: the ordinary assignment,
  unchanged. For a slot with an open record: refused (TR-CLUSTER-008/009, V5-C1).
  For a slot with a residue entry, exactly two admissible arms:
  **rollback arm** (entry at `promoted == false`): admissible iff
  `nodes[node].role == Primary` ∧ (`node == shard_primary(entry.source)` ∧
  `entry.source_gone == false`, **or** `accept_data_loss == true`); apply re-assigns
  the slot to `node` and **removes the entry** (the target's discard reaper then
  reclaims the shadow — §5).
  **Orphan re-home arm** (entry at `promoted == true ∧ target_gone == true` —
  V7-M6): admissible iff `nodes[node].role == Primary` ∧ `accept_data_loss == true`
  (every exit from this state abandons the departed target's post-promotion
  writes); apply: if `node == entry.source`, re-assign the slot to the source and
  **remove the entry** — the source keeps and serves its retained copy; otherwise
  re-assign to `node` and **clear `target_gone`, keeping the entry** — ordinary
  `promoted == true` residue whose source reaps and confirms. All other
  assignments over residue: refused, error naming the entry.
- **`ClearSlotResidue{slot, migration_id, proposer, accept_stale_copy: bool}`**
  (operator-proposed; **new**, V7 — the §1 remover enumeration's last-resort verb) →
  removes the residue entry **without** touching the slot map. Admissible iff the
  entry exists ∧ `accept_stale_copy == true` ∧ **no effective automatic remover
  exists**: `entry.source_gone == true`, or (`nodes[entry.source].role != Primary` ∧
  `shard_primary(entry.source) == None` — a **declared absence test**, §0's
  V8-M6 carve-out: the conjunct *is* the `None` check, evaluated as written, not
  false-at-`None` under the value-read rule; every conjunct here is re-derivable
  from replicated state alone, so "effective" means what it says — no arm whose
  admissibility depends on unobservable node-local facts counts as a remover,
  the V7-C1 proxy-arm lesson). The operator attests that the stale copy
  the entry gated has been dealt with out of band (node decommissioned, disk wiped,
  copy manually deleted); the join-empty admission gate (§0, V8-C1) is the backstop
  if the departed node ever attempts to rejoin still holding the copy. Refused
  without the token, and refused while a lawful remover
  exists — the verb must never shortcut the attested paths.
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
  replays and reordering; no proposer conjunct is load-bearing. When the incremented
  `observations` is **`≥` the applicable captured bound** (V7-C3 — "reaches" means
  `≥`, stated so the exit exists even if the counter ever jumps the boundary) —
  `record.preconfirm_observations` while
  `drained_pos` is unset, `record.draining_observations` once it is set (both
  Begin-captured record fields, never the config knobs — V6-C2/M4; the counter
  restarts at 0 when `ConfirmSlotHandoffDrained` applies, so each bound meters only
  its own sub-state's ticks — V7-C3) — the apply forces
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
- **`ReportRunIdentity{node, run_id, kind: Boot | Promotion | Demotion,
  new_primary_id: Option<NodeId>, observed_role: Role, observed_config_epoch: u64,
  proposer}`** (the payload's `run_id` is the full
  triple —
  `identity_seq` rides inside it, §0/V5-C2; `kind` is stamped at proposal from the
  moment that minted the identity change, and `new_primary_id` is set on `Demotion`
  to the node's new upstream if a cluster member, else `None` — V7-C1: the three
  proposing moments are indistinguishable in `(node, run_id)` alone, per
  FM-REPLICATION-022 both a boot and a bare-`REPLICAOF` demotion bump
  `identity_seq`, so an arm selected by anything but a committed payload field is
  unselectable at apply; `observed_role`/`observed_config_epoch` snapshot
  `nodes[node].role` and the node's shard config epoch from the *proposer's applied
  state at proposal* — the V8-C2 per-object fence fields, below) (proposed by each
  node at boot,
  at promotion, and at demotion/history adoption — §0, V4-M4) → writes
  `NodeInfo.run_identity`. Admission: proposer is `node` (`payload.proposer ==
  payload.node`, V6-C1) ∧ `node ∈ nodes` (V6-M3) ∧ `(incarnation,
  identity_seq) >` stored pair (§0; **true at absent stored value** — the first
  report is always admitted, V6-M3). Applying a run_identity change for a node that is
  the **source** of any open migration **cancels those migrations** — the replicated
  form of "source restart aborts" (§4). The **`kind == Demotion` arm** is a
  **fenced role writer** (V8-C2 — identity monotonicity is not topology recency: a
  demotion report proposed before a `Failover` promotion but applied after it would
  otherwise clobber the promotion, exactly the stale-writer class issue 19's
  per-object fence rule and TR-CLUSTER-018/042's epoch fences exist for). Its two
  additional apply-time conjuncts, both refusing on mismatch: **(fence)**
  `nodes[node].role == payload.observed_role` ∧ the node's shard config epoch `==
  payload.observed_config_epoch` — the report only lands on the topology it was
  minted against; **(upstream validity, V8-C2)** `payload.new_primary_id`, when
  `Some`, names a current member whose `role == Primary` — validated *at apply*,
  not merely at proposal (TR-CLUSTER-001's issue-14 ruling: payload references are
  re-checked against applied state). When admitted, the arm
  writes **`nodes[node].role = Replica` and `nodes[node].primary_id =
  payload.new_primary_id`** (V7-C1 — without the role write, a bare-`REPLICAOF`
  demotion the failover machinery never saw leaves `nodes[node].role` at Primary
  forever: §7's defer guard never defers, `RetargetSlotResidue`'s
  `role != Primary` gate never opens, and the re-target rule never fires — a
  `promoted == true` entry becomes immortal and V5-C1's conjuncts freeze the slot's
  topology surface permanently; SS-2/SS-3's writer lists gain this transition — a
  LOCKED amendment with its own Rewritten verdict), **re-homes every slot the node
  owns to `shard_primary(node)` in the same apply** (V8-C6 — a role write without
  the slot re-home leaves a Replica owning slots: it answers slot lookups `MOVED`
  to itself and every client loops forever while every health read shows a live
  primary-less-but-assigned slot; the re-home and the role write are one atomic
  apply, so no applied state ever shows a Replica owning a slot — **invariant: no
  slot is assigned to a node whose role is Replica**, SS-11 amendment + forcing
  test), and then
  re-targets residue entries naming the node (§0's residue-lifecycle rule, V6-C3 —
  `shard_primary` evaluated after these writes). **Where `shard_primary(node)` is
  `None` and the node owns slots, the arm refuses whole** (no successor exists to
  re-home to; deleting the assignment would orphan the slots): the demotion is
  deferred, not lost — the re-proposal rule below re-proposes once a successor
  exists or the slots are moved, and until then the replicated state keeps the node
  Primary, which is the truth the topology can serve.
  `Boot` and `Promotion` arms write no role — promotion's role change belongs to
  the failover transitions that carry it (TR-CLUSTER-017/018) — and carry the fence
  fields inert (admission ignores them; only the role-writing arm is fenced).
  **Level-triggered re-proposal** (V8-C2/V8-M3 — a refused report must not strand
  the truth): each node runs a standing reconcile — whenever its current local
  identity/topology facts (`run_identity` triple, role, upstream) differ from its
  own replicated `NodeInfo` in applied state, it re-proposes `ReportRunIdentity`
  with **fresh** observations (current `observed_role`/`observed_config_epoch`,
  current `new_primary_id`). A report refused by the fence — the topology moved
  underneath it — is therefore retried against the topology that actually holds,
  and converges or is superseded by a newer identity change; the boot report stops
  being a one-shot edge (the V8-M3 refill hole) and becomes the level rule's first
  firing. **A target's identity change does not cancel**
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

**Residue liveness obligation** (V6-C3, enumeration rewritten per V7 — every
reachable entry state names its remover, and every remover is a declared transition):

1. **Source is a live primary** (`nodes[entry.source].role == Primary`): the source
   reaps (§7) and proposes `ConfirmSlotDeleted`.
2. **Source demoted, shard has a primary** (`role != Primary ∧
   shard_primary(entry.source) != None`): the demotion re-target (§0) moves the
   entry to that primary in the same apply, and the level-triggered
   `RetargetSlotResidue` (V8-C4) catches every entry the in-apply re-target
   skipped (successor-less window followed by a later promotion) — after either,
   case 1 applies to the new source.
3. **`promoted == false`**: the `AssignSlots` rollback arm (Transitions, V7-M5) —
   lossless to `shard_primary(entry.source)` while `source_gone == false`,
   token-gated otherwise.
4. **No lawful automatic remover** (`source_gone == true`, or `role != Primary ∧
   shard_primary(entry.source) == None`) — and, at `promoted == true ∧ target_gone
   == true`, the orphaned-slot state: the token-gated operator verbs —
   `ClearSlotResidue{accept_stale_copy}` for the entry,
   the `AssignSlots` orphan re-home arm for the slot (Transitions, V7-M6).
5. **Departed source rejoins under the same NodeId** (re-`MEET`): the join-empty
   admission gate (§0, V8-C1) refuses the handshake while the stale copy exists —
   the node rejoins only after a wipe, so it arrives holding nothing the entry no
   longer accounts for, and cases 1/2 then apply to the surviving entry.

A source that is *down but still a member* is the one unremoved case, and it is
bounded: either the node returns (its reaper resumes) or the shard fails over, which
re-targets the entry to the successor. No entry can reach a state where deleting the
stale copy requires removing a member, and no state's only exit is an undeclared
verb.

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
to `cluster-migration-barrier-max-bytes` — a **per-migration** cap (V8-m2: each
migrating slot's barrier accounts and breaches independently, preserving
FM-CLUSTER-088's cross-slot independence; the node-wide hold memory is therefore
bounded by concurrent-migrations × cap, which the config table states as the
operator's sizing note). On breach:

- the source **does not execute the held writes**. Writes beyond the cap are answered
  `-TRYAGAIN` immediately; already-held writes remain held;
- the source proposes `AbortSlotHandoff{attempt_id}` and keeps its fence until that
  proposal **applies** (fail-closed);
- on apply: phase→Streaming, attempts+=1, barrier releases, held writes execute at the
  source and are acknowledged normally.

**Named invariant: the source's local fence is never weaker than the replicated phase
implies.** A node-local decision may fence *more*, never less.

**The seal's exempt set is enumerated, and it is only `CLUSTER`** (V8-C5 — a seal
with an unenumerated exemption is not a seal): once `ConfirmSlotHandoffDrained`
attests "no write below `drained_pos` remains and none will follow", **every**
command that can mutate the slot's keys is subject to the hold — ordinary writes,
scripts, and expressly **`MIGRATE` and `RESTORE` naming keys in the sealed slot**
(LOCKED FM-CLUSTER-080's `MIGRATE` slot-pause exemption is **Retired**: its purpose
was the retired Redis-style bulk phase's catch-up `MIGRATE`, which no longer exists;
an exempt `MIGRATE` past the seal would delete keys the source has acked drained —
an acked mutation above `drained_pos` the target never sees, the exact class the
seal exists to exclude). The only exemption is the **`CLUSTER` command family**
(admin plane — it must stay reachable on a wedged barrier so the operator can
`STABLE`/cancel; FM-CLUSTER-081's exemption carries the cancel and is
Unchanged-stated). Client contract: a `MIGRATE`/`RESTORE` against a held slot is
held or answered `-TRYAGAIN` exactly like any other write (§6's inventory).

**Fence reconstruction at boot** (V7-m5 — the invariant needs a stated restart rule
or a crash discharges it silently): on source restart, **before admitting any client
write to a slot**, the node re-derives the fence from applied replicated state — a
record in `phase == Draining` naming it as source re-arms the barrier, and a set
`drained_pos` additionally re-seals it. (A source restart also changes `run_id`, so
the migration is being cancelled — but the cancel is a Raft round-trip away, and the
fence must hold from first write admission, not from cancel apply. Level-triggered
re-derivation, per the determinism carve-out's discipline.)

Held-write disposition on every exit (every held client gets a real reply):

| Exit | Reply to held writes |
|------|---------------------|
| `Complete` applies | pinnable writes: `MOVED <slot> <target>` (FM-CLUSTER-092 amended); **unpinnable held batches** (FM-CLUSTER-096: straddling slots or keyless): `-TRYAGAIN` — one `MOVED` slot cannot describe them; the client's retry re-routes per key (N-m3) |
| `AbortSlotHandoff` applies | execute at source, acknowledged normally |
| Cap breach, pre-apply | beyond-cap writes: `-TRYAGAIN`; held set: unchanged until apply |
| `CancelSlotMigration` applies | execute at source, acknowledged normally (release event) — **provided the source is still the slot's serving primary at apply** |
| **Cancel caused by the source's own demotion** (V5-m5: the `ReportRunIdentity` demotion/adoption arm cancels the migrations the node sources; a bare `REPLICAOF` mid-Draining lands here) | the held set is answered with **the reply the node's new role implies** — `-MOVED` to the new primary when the local `shards` view names one, **`-TRYAGAIN` when `primary_id == None`** (V8-M4: the disposition is total; a successor-less demotion answers retryably and the client's retry lands wherever the eventual topology says) — and is **never executed**: a demoted node executing queued writes would fork history against its new primary |
| **Self-fence latch arms** (TR-CLUSTER-026: no Raft leader contact within an election timeout) | answer the **entire held set** `-TRYAGAIN` and **keep the fence** (N-M1) — a sealed source that cannot apply must not make held clients wait out a partition; erroring a held write is *more* fenced, not less, so the §3 invariant holds and the sealed rule ("no further execution until an exit applies") is untouched. **Level rule, not an edge** (V8-M1): while the latch is armed and the slot sealed, the invariant is *the held set is empty* — no new hold is ever admitted, and every arriving write (including one racing the flush) is answered `-TRYAGAIN` immediately. The one-shot flush is merely the transition into that region; a write arriving after it does not wait out the partition |
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
  upgrade (or degrade) either node's durability contract. The accepted exposure is
  strictly **post-`Complete`** (V8-C3): a crash *before* `Complete` falsifies the
  attesting-run conjunct — the restarted run must re-report its regressed
  coverage, and the stale-high proof can never be harvested. Operators who
  need one enable `cluster-migration-require-target-replica-ack`, which adds the
  optional conjunct at the cost of cutover latency (and, with a dead target replica,
  of the migration aborting via the observation bound rather than completing — §1).
- **Promotion** (N-C3, hardened by V4-C1/M11, gated by V5-m7): the shadow becomes the
  live keyspace as a **node-local, idempotent, resumable consequence** of the applied
  `SlotMigrationCompleted` event — never inside apply. Promotion is a **metadata
  operation** — the shadow region is re-labelled as the slot's live data (the store is
  slot-keyed already; no per-key copy) — so the window is O(1), not O(keys).
  **Precondition: the slot's live region in the main keyspace is empty** (V7-C2,
  retained under V8-C1 as defence-in-depth — the join-empty admission gate (§0)
  means no lawful history puts untracked data here, so a non-empty live region at
  this moment signals a gate bypass, a bug, or an out-of-band restore, exactly the
  class a last-line check exists for). A non-empty live region is a **promotion
  failure, never a silent
  merge** — re-labelling over it would resurrect deleted keys beside the migrated
  copy: the target refuses to re-label, surfaces an operator event naming the slot
  and the stale key count, and the entry stays `promoted == false` — the V4-M11
  failed-promotion state, exited by the rollback arm; the stale live region itself
  is operator-owned (the event names it; the operator wipes it out of band — no
  unilateral deletion rule exists, §0/§7), and the migration can be retried clean
  after the wipe. **The
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
  source still holds a complete copy**. The recovery verb is the **`AssignSlots` rollback
  arm** (Transitions, V7-M5 — `CLUSTER SETSLOT <slot> NODE <source>` / frogctl map to
  it): admissible
  while a residue entry for the slot exists with `promoted == false` (a race with
  `ReportSlotPromoted` is resolved by Raft order — and V5-m7's serving gate means the
  losing target has served nothing); its apply re-assigns the slot, removes the
  residue entry (reversing a completed local re-label, above), and the target's
  discard reaper then reclaims the shadow. When the source has left membership, the
  arm generalises per the §0 residue-lifecycle rules: any **primary** member is an
  admissible assignee (V6-M1 — a slot is never assigned to a replica), with
  `accept_data_loss` required for any assignee other than the **lossless assignee**,
  `shard_primary(entry.source)` (§0), and only while `source_gone ==
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
  V4-m6/V5-m7) — these last two are the inventory's **longest-lived members**
  (V8-m3): ordinarily one Raft round-trip, but **unbounded in the failed-promotion
  state** — a slot whose promotion refused (§5's non-empty-live-region failure)
  answers `-TRYAGAIN` until the operator runs the rollback arm, the client contract
  states so, and §9's operator surface flags the state; **`MIGRATE`/`RESTORE`
  naming a sealed slot** are held/`-TRYAGAIN`-answered like every other write
  (V8-C5, §3's exempt-set rule); and the **demotion-cancel disposition at
  `primary_id == None`** (§3, V8-M4). The v3 `RESTORE`-into-importing entry is removed — that request is
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
is Replica the reaper **defers** — the role it reads is the **replicated
`nodes[self].role`** in applied state (SS-2, V7-C1 — written by the
`ReportRunIdentity{kind: Demotion}` apply, never a node-local flag), so the defer
decision is a pure function of applied state plus `self_node_id` (the V7-m1
carve-out). It never runs while demoted (replicas receive the delete-range through
the feed) but the entry persists and the delete resumes if the node is later
re-promoted (V4-M7 — v3's "never on a Replica" permanently stranded the list on a
demoted source), and an empty residue list means no deletion, whatever the slot map
says. The reaper also **defers while the entry has `target_gone == true`** (V7-M6):
the source's copy is the only surviving one, and `ConfirmSlotDeleted`'s admission
refuses it anyway (`target_gone == false` conjunct) — the §0 orphan re-home arm
resolves the entry first. Two V6 additions, one revised in V7: **(a)** ~~work
items~~ — the V6-M5 abandonment-conversion rule is **dropped** (V7-C2/V7-M1):
membership prune and reset now *mark* the entry (`source_gone`) instead of removing
it, so the reaper still consumes only replicated residue entries and nothing else;
the reaper is thus **the only rule in the design that deletes main-keyspace slot
data** (V8-C1 — the V7-C2 self-cleanse is gone; stale copies whose entries no
longer exist, e.g. after `ResetCluster`, live only on nodes *outside* the cluster,
where the §0 join-empty admission gate keeps them until an operator wipes).
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
  (slot, source, target, migration_id, promoted, source_gone, target_gone): slot,
  source, target,
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
| `cluster-migration-barrier-max-bytes` | 4 MiB | bytes (held-write cap, §3; resolves the issue-29 ambiguity — the surviving issue-17 bound). **Per-migration** (V8-m2): node-wide held-write memory is `concurrent migrations sourced by this node × cap` — an operator sizing memory headroom multiplies by the migration concurrency they drive, and §3's total-residency note bounds the product |
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
already bounds held bytes. **The obligation extends to the post-`Confirm` product**
(V8-m4): with `require-target-replica-ack` on, `draining-observations ×
observation-tick-ms` (defaults: 3 s) must exceed the time the *target's replicas* take
to ack through the token — the post-seal bound counts observations with no
target-side progress below `drained_pos`, and with the knob on, "progress" includes
the replica-ack position (its report resets the counter, V8-M2). A replica fleet that
lags more than this product behind the target aborts an otherwise-lawful drain;
operators enabling the knob calibrate the pair against their replica ack lag, not
just the source drain time.

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
  arms; FM-CLUSTER-032's unassigned-slot arm is **retired** — V7-M4 reverses V4-m5:
  `Begin` now requires `slot_map[slot] == source`, so migrating an unassigned slot is
  refused with direction to `AssignSlots` first — migrating from nobody is
  meaningless, and the arm's untracked `slot_map[slot] == None → target` write was an
  unfenced assignment bypassing every residue/rollback guard;
  FM-CLUSTER-033 is rewritten with its **headline inverted** (V7-m2): `Complete` now
  *does* read the slot map — its `slot_map[slot] == record.source` conjunct — so the
  row's old "completion never consults the slot map" claim is false under this
  design; the row now specifies the `SlotAlreadyAssigned`-at-completion refusal as
  observable behaviour, and the membership check appears as `Complete`'s `target ∈
  nodes` conjunct — V4-m4); FM-CLUSTER-036 + TR-CLUSTER-017/018/019 (failover/restart naming an
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
  **and to `handoff_residue` entries**, with exactly two declared-arm exceptions: the
  failed-promotion rollback arm and the orphan re-home arm — V5-C1/V7-M5/V7-M6);
  TR-CLUSTER-001 (`AddNode` — **rewritten**, V7-M3: the upsert is field-wise and
  **preserves `run_identity`**; a fresh registration initializes it absent);
  TR-CLUSTER-002 (**rewritten**, V7-M3: the LOCKED upsert-on-existing-NodeId rule now
  states which fields the upsert may touch — address/port/config — and that
  `run_identity` and `role` are written only by their declared writers);
  TR-CLUSTER-027 (**unchanged, stated**, V7-M3: live `CONFIG SET` re-registration
  routes through the same field-wise upsert, so it cannot erase `run_identity`);
  TR-CLUSTER-003 (`RemoveNode` — **rewritten**, V7-m3: prune now *marks* residue
  entries — `source_gone` on source departure, `target_gone` + unassign on target
  departure — never removes them, and cancels the departed node's open migrations);
  FM-CLUSTER-092/093/094 (inversion under source authority, §6);
  FM-CLUSTER-095 (arm split: finalization-refusal arm retired, ownership-moved arm
  kept; SlotFence generation mechanism unchanged); FM-CLUSTER-096 (unpinnable batches:
  parked-batch disposition at each exit, §3; **consequence restated + drain-covers-
  continuations containment** — V4-M13, §6); FM-CLUSTER-104 (same-node re-arm only;
  successor never inherits; **extended**, V7-m5: the §3 boot-reconstruction rule —
  fence state re-derived from `phase == Draining` / set `drained_pos` before the slot
  admits client writes — becomes the row's restart arm);
  FM-REPLICATION-022 (**noted**, V7-C1: the run-identity report is now
  kind-stamped — `Boot` / `Promotion` / `Demotion` in the committed payload — so the
  row's "a bare `REPLICAOF` demotion bumps identity" behaviour is carried by an
  explicit payload discriminant, not inferred at apply time);
  FM-CLUSTER-079 (**rewritten**, V8-M5: the unpinnable-command folding — "parks
  against the strongest pause armed on any slot" — is the mechanism behind §3's
  unpinnable held batches, and the row gains the §3 byte cap and the per-exit
  disposition table it previously did not know about);
  FM-CLUSTER-082 (**rewritten**, V8-M5: "neither can release the other" now composes
  with a barrier armed and released by replicated phase and re-derived at boot —
  §3's fence-reconstruction rule — not by `plan_handoff_action` alone; the
  independence claim itself survives);
  FM-CLUSTER-083 (**rewritten**, V8-M5: its Outcome enumeration — `Response::Array`
  / `-MOVED` — gains the third outcome §3 introduces: a parked `EXEC` whose batch is
  unpinnable is answered `-TRYAGAIN`, FM-CLUSTER-096's rule applied to the
  transaction surface);
  TR-CLUSTER-004 (`SetRole` — **rewritten**, V8-M5/C2: as one of the three
  demotion-writing transitions it re-targets the demoted node's residue entries and
  re-homes its owned slots in the same apply, and it carries issue 19's per-object
  epoch fence — the arm V8-C2's stale-report clobber would otherwise race);
  TR-CLUSTER-042 (`Failover{force:true}` — **rewritten**, V8-M5: the row owns
  outright-removal semantics, so the residue map's `source_gone`/`target_gone`
  marking rides it explicitly — prune marks, never removes — and issue 20's
  demote-don't-remove default is restated at the row as the preferred path);
  SS-11 (**amended**, V8-C6: gains the invariant "no slot is assigned to a node
  whose role is `Replica`" — every writer that flips a role to `Replica` re-homes
  the node's slots in the same apply, and every slot writer requires the assignee's
  role to be `Primary`; forcing test asserts the invariant over every transition
  interleaving, `inv_slots_only_assigned_to_primaries`).
- **Retired**: FM-CLUSTER-085 (handoff lease — its property, "a dead finalizer cannot
  wedge a slot", is re-provided by the observation bound *plus the leader
  auto-`Complete`* (V4-M2), which together exit every Draining state; replacement row
  states this); **FM-CLUSTER-097 + the `ReplicaFeedGate`** (§8 — purpose re-derived to
  nothing under source authority; row rewritten to assert the absence of migration
  feed holds); **FM-CLUSTER-080's `MIGRATE` slot-pause exemption** (V8-C5/M5: its
  purpose was the retired Redis-style bulk phase's catch-up `MIGRATE`; under the §3
  exempt-set rule `MIGRATE`/`RESTORE` are held like every other write, and the
  forcing test `only_migrate_and_cluster_are_slot_pause_exempt` is re-pointed at the
  new exempt set — `CLUSTER` only).
- **Unchanged, stated**: FM-CLUSTER-038 (blocked-client wake at Complete);
  FM-CLUSTER-061..063 (the admin-gating class's semantics — V6-m5; only 064's table
  gains a row); FM-CLUSTER-095's SlotFence generation input; FM-CLUSTER-100
  (generation survives
  snapshots — extended to the new record fields, `NodeInfo.run_identity`, and
  `handoff_residue`); TR-CLUSTER-026 (self-fence — gains the held-set release
  row, §3); TR-CLUSTER-034 (per-node arm/release reaction);
  FM-CLUSTER-081 (V8-M5: the `CLUSTER` exemption **survives** — it carries
  `SETSLOT … STABLE`, the operator's cancel, which §3's exit table depends on; it is
  the sole member of the §3 exempt set); FM-CLUSTER-088 (V8-M5/m2: cross-slot
  independence — "aborting or completing one leaves the other untouched" — holds
  because the held-byte cap is **per-migration** (§3): one slot's write volume can
  never force another slot's cap breach).
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
  no-re-stamp rule (V6-C2); demotion re-target both sides + ~~broadened
  `ConfirmSlotDeleted` proposer arm~~ (**superseded in V8**: `RetargetSlotResidue`
  replaces the proxy arm, V8-C4) + the §1 residue liveness obligation (V6-C3);
  primary-role conjuncts on `ReportSlotPromoted` and rollback + the lossless-assignee
  definition (V6-M1); counter-loss re-mint above the replicated pair +
  "has applied" defined + loud boot failure (V6-M2); bootstrap/join proposal
  orderings + `node ∈ nodes` (V6-M3); pre/post-`Confirm` observation-bound split +
  the tick-cadence knob + the calibration obligation + one-shot attempts reset
  (V6-M4); ~~abandonment-to-work-item conversion~~ (V6-M5 — **superseded in V7**:
  prune marks instead of removes, V7-C2/M1; the V7 self-cleanse itself
  **superseded in V8** by the join-empty admission gate, V8-C1); full
  payload declarations for every transition (V6-m1); the §0 absent-operand rule with
  its named exceptions (V6-m2); `source_gone` as a rollback admission operand
  (V6-m3); the held-write table's demotion-wins scoping (V6-m6). From review v7:
  `ReportRunIdentity`'s kind-stamped payload (`Boot`/`Promotion`/`Demotion` +
  `new_primary_id`) with the Demotion arm as SS-2/SS-3's declared writer — role and
  primary_id flip inside the apply, sourced migrations cancelled, residue re-targeted
  post-write (V7-C1); prune-marks-never-removes + ~~the level-triggered self-cleanse
  rule~~ (**superseded in V8**: the deletion predicate was §7's forbidden
  ownership-absence rule — replaced by the join-empty admission gate +
  `ResetCluster`'s non-empty refusal, V8-C1) + the §5 empty-live-region promotion
  precondition (V7-C2, absorbing V7-M1);
  `Confirm`-resets-observation-counter with the field-writers table's third reset
  trigger and `≥`-bound semantics (V7-C3); the §0 `shard_primary(n)` definition with
  its None-fails-closed rule (V7-M2); `AddNode`'s field-wise
  `run_identity`-preserving upsert (V7-M3); `Begin`'s `slot_map[slot] == source`
  requirement — unassigned-slot arm retired (V7-M4); `AssignSlots` as a declared
  transition with the rollback arm and `accept_data_loss` payload (V7-M5);
  `target_gone` + the orphan re-home arm + the reaper/`ConfirmSlotDeleted` defer
  (V7-M6); the `ClearSlotResidue` operator verb with its no-lawful-automatic-remover
  admission (V7 §1 remover enumeration); the determinism carve-out for node-local
  reactions (V7-m1); FM-CLUSTER-033's inverted headline (V7-m2); TR-CLUSTER-003's
  mark-don't-remove rewrite (V7-m3); fully-typed record and residue declarations
  (V7-m4); the §3 fence-reconstruction-at-boot rule (V7-m5). From review v8: the
  join-empty admission gate + `ResetCluster`'s non-empty refusal replacing the
  self-cleanse — no automatic ownership-absence deletion anywhere; the
  never-rejoining node's copy is operator-owned (V8-C1); `observed_role`/
  `observed_config_epoch` fencing on `ReportRunIdentity`'s Demotion arm +
  apply-time `new_primary_id` validation + same-apply slot re-home + the
  successor-less whole-refusal (V8-C2); `target_attesting_run` + `target_run`
  pairing on both target reports, cross-run replacement semantics, and `Complete`'s
  current-run conjunct — a possession proof dies with the run that minted it
  (V8-C3); the `RetargetSlotResidue` transition replacing the broadened
  `ConfirmSlotDeleted` proxy arm (V8-C4); the §3 exempt-set rule — seal exempts
  only `CLUSTER`; `MIGRATE`/`RESTORE` held like every write (V8-C5); the SS-11
  no-slot-owned-by-Replica invariant + level-triggered identity re-proposal
  (V8-C6); the self-fence latch as a level rule — held set empty while latched
  (V8-M1); replica-ack observation reset (V8-M2); the §0 `run_identity` lifecycle
  across `RemoveNode`/re-`MEET`, `ResetCluster`, and snapshot install (V8-M3);
  the demotion-cancel disposition made total at `primary_id == None` (V8-M4); the
  eight LOCKED-row verdicts above (V8-M5); the absent-operand rule's
  value-read/absence-test distinction (V8-M6); per-migration cap scoping + the
  node-wide sizing note (V8-m2); the failed-promotion unbounded-hold note in §6's
  inventory (V8-m3); the post-`Confirm` calibration product (V8-m4); the
  at-most-one-residue-entry-per-slot invariant (V8-m5).
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
     **Plus** (V7-C3) `confirmDrained` **resets the observation counter in the
     model** (and clears `last_observation`), with a third property: a migration is
     never abortable by the bound until the current phase-side counter has accrued
     `draining_observations` *post-seal* observations — reverting the reset (letting
     pre-seal ticks count against the post-`Confirm` bound) must violate it (the
     30→3 collapse at stock defaults is its failure trace).
  5. `sourceFailover` (identity change + backward jump) **and** `sourceRestart`
     (N-C2's live hazard: **same** replid, position dropped to a save point, then
     re-advanced forward over a hole) + `inv_stream_history_sound` strengthened from
     contiguity to history soundness via per-position history tags. **Plus
     `targetRestart`** (V4-M5): encodes the asymmetric rule — a target restart
     resumes from `covered_applied` and must not violate `inv_no_acked_write_lost`;
     the model is the cheapest place to show "resume" and "cancel" cannot both be
     consistent with it. **`targetRestart` additionally regresses the durability
     positions** (V8-C3 — power loss, not clean restart: `covered_applied` falls
     back to the last durable point, leaving the old run's replicated ingest and
     replica-ack positions stale-high): the target's reports carry a `target_run`
     tag, admission requires it to equal the target's current replicated run, a
     cross-run report **replaces** (may regress) rather than maxes, and `Complete`
     requires the record's attesting run to be the target's current run. Mutation
     test: reverting the cross-run replacement (keeping same-run monotone max
     across the restart) or dropping `Complete`'s attesting-run conjunct must
     violate `inv_no_acked_write_lost` via the stale-high possession-proof trace.
  6. `target_replica_copy` + `inv_target_replicas_hold_committed_slot` (v2-C7).
     **Plus `attachTargetReplica(n)`** (V4-M6): a replica attaching mid-`Streaming`
     with `target_replica_copy = 0`, then a target failover onto it — the existing
     invariant must fail unless the full-sync payload carries the shadow. **Plus
     `crashDuringShadowSection(n)`** (V6, from V5-M4): a replica crash mid-section
     leaves an install without its completion marker; witness that the incomplete
     install is discarded-and-re-requested at boot and is never promoted.
  7. Per-node keyspace `source_keys: NodeId -> Set[SlotId]` + `reapSlots(n)` action +
     `inv_node_keeps_slots_it_owns` (N-C1 — the model must be able to express
     over-deletion to prove its absence). **`reapSlots` is the model's only
     member-keyspace deleter** (V8-C1 — §7's rule as a structural fact: no other
     action removes from a member's `source_keys`; adding any second deleter must
     violate `inv_node_keeps_slots_it_owns`'s forcing mutation).
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
      new migration). **Reworked for kept-entry semantics** (V7, superseding V6-M5's
      work-item property): membership prune *marks* the entry (`source_gone`) and
      never removes it, so the model asserts `inv_begin_refuses_slot_with_residue` —
      a marked entry still blocks `Begin` over the slot — and reverting the
      mark-don't-remove rule must violate it. **Reworked again for the admission
      gates** (V8-C1 — the self-cleanse is gone, so its scope exclusion goes with
      it): `meetNode(n)` (join) gains the guard `source_keys(n)` restricted to the
      main keyspace is empty — a non-empty joiner is refused, witness
      `witnessNonEmptyJoinRefused`; `resetCluster(n)` gains the same non-empty
      refusal (`witnessNonEmptyResetRefused`). With both gates,
      `inv_member_keyspace_is_tracked` — every slot copy held by a *member* is
      reachable from a live record, a residue entry, or the slot map — must hold;
      reverting either gate must violate it (the untracked-stale-copy trace). A
      never-rejoining node's copy is outside the membership and outside the model's
      claims (operator-owned, §7).
  11. `sourceCannotApply` flag + witness that the held set empties (self-fence
      release) while the flag holds (N-M1). **Plus the level form** (V8-M1):
      invariant `inv_held_set_empty_while_latched` — in every state with the latch
      armed and the slot sealed, the held set is empty; a write action scheduled in
      that region is answered, never held. Reverting the level rule (flushing once
      and letting later arrivals hold) must violate it. **Plus `targetSilent`** (V4-M2 — the
      mirror escape: alive-but-unable-to-act on the target side): bounded witness
      `witnessDrainingWedgedWithCompletableToken` — `Complete`'s guard holds,
      `observations` pinned by ingest progress, held set non-empty; with the V4-M2
      fixes (leader auto-`Complete` + narrowed reset) the witness must become
      **unreachable**, a clean mutation test.
  12. **`residue: (SlotId, MigrationId) -> {source, target, promoted, source_gone,
      target_gone}`
      as a first-class variable** (V5 audit's highest-value change — V5-C1/M2's
      states are
      unrepresentable without it; type gains `source_gone` per V6-m3 and
      `target_gone` per V7-M6): written by
      `completeMigration`, mutated by
      `failPromotion(s)` (promotion attempted, fails, entry stays
      `promoted == false`) and `removeNode(n)` (the mark-don't-remove rules:
      `source_gone` on source departure, `target_gone` + unassign on target
      departure — V7-m3);
      `reapSlots(n)` gains the `promoted == true` gate **and the
      `target_gone == false` defer** (V7-M6); `beginMigration` gains the
      residue guard, with witness `witnessBeginRefusedOverResidue`. Headline
      invariant: `inv_source_keeps_its_copy_until_promotion_attested` — reverting
      either the reaper gate or the `Begin` conjunct must violate it (the V5-C1
      snapshot-of-unpromoted-slot loss is its failure trace). `removeNode(n)` on a
      target additionally checks **`inv_slot_copy_survives_until_owned_and_served`**
      (V7-M6): at every state, each slot either has an owner in the map serving a
      complete copy, or a residue entry naming a surviving copy-holder — reverting
      the target-departure unassign+mark rule (silently leaving the slot assigned to
      a dead node, or removing the entry) must violate it; the orphan re-home arm
      (`promoted == true ∧ target_gone`, re-assign to source removing the entry or
      to another primary clearing the flag) is its recovery action. **Plus
      `demoteNode(n)`**
      (V6-C3 — the round's highest-leverage addition: role becomes a modelled
      variable, member retained, a successor promoted in the same step) with the
      demotion re-target rules, **and `demoteNodeExternal(n)`** (V7-m7): a
      successor-less demotion — role→Replica, `primary_id`→None, no promotion in the
      step — under which invariant **`residueHasAnEffectiveRemover`** (V8-M6
      renames and sharpens `residueHasARemover`: an admissible verb whose
      admission is *false in the very state that needs it* is not a remover) —
      §1's liveness obligation as a machine-checked property: every residue
      entry's `source` resolves via `shard_primary` to a live primary whose
      `ConfirmSlotDeleted` is admissible, or a `retargetSlotResidue` re-home is
      admissible, or the entry is marked (`source_gone`/`target_gone`) with its
      operator arm admissible, or a rollback/prune verb is admissible, or
      `ClearSlotResidue`'s no-effective-automatic-remover admission is true —
      must still hold. **Plus `retargetSlotResidue(s, m)`** (V8-C4, replacing the
      V6-C3 proxy `ConfirmSlotDeleted` arm in the model too): level-triggered
      re-home of an entry whose `source` is no longer a primary, admissible
      whenever `shard_primary(entry.source)` is a live primary proposing for
      itself; reverting it, the in-apply demotion re-target, or the verb's
      admission must each violate the invariant. **Plus invariant
      `inv_slots_only_assigned_to_primaries`** (V8-C6 — SS-11's amendment as a
      model property): in every reachable state, `slot_map` assigns no slot to a
      node whose role is `Replica`; reverting the Demotion arm's same-apply slot
      re-home (or its successor-less whole-refusal) must violate it.
  13. `sourceCannotDrain` flag (alive source that never proposes `Confirm`) + bounded
      witness `witnessDrainingWedgedBeforeConfirm` — reachable under v4's "(or
      `drained_pos` unset)" reset arm, **must become unreachable** with the V5-M1
      narrowing; `inv_progressing_migration_never_aborts` is **re-scoped to
      post-`Confirm` progress below the token** (pre-`Confirm` the bound fires
      regardless of progress, by design), **evaluated against the post-`Confirm`
      counter that started from zero at the seal** (V7-C3 — the ext-4 reset property
      is what makes this re-scoping honest: without the reset the "post-`Confirm`
      bound" is mostly consumed pre-seal).
  14. `detachTargetReplica(n)` + the empty-counted-set state (V5-M3): with the knob
      on and zero counted replicas the `Complete` guard must be false and the
      migration must exit via the observation bound — reverting the empty-set-false
      rule must violate `inv_target_replicas_hold_committed_slot`. **Plus bounded
      witness `witnessKnobOnMigrationCompletes`** (V8-m4, with ext-4's bound
      fields): a knob-on migration with *healthy, lagging-but-progressing* target
      replicas completes — reachable **at stock defaults** (`draining_observations
      = 3`, replica acks arriving within the bound; the V8-M2 replica-ack counter
      reset is what makes it reachable — reverting that reset must make the
      witness unreachable at defaults, the abort-every-lawful-drain defect as a
      mutation test).
  15. `reportRunIdentity(n, incarnation, identity_seq, kind)` as a replayable action
      (V5-C2; **kind-stamped per V7-C1** — the `Demotion` arm writes
      `role`/`primary_id` in the model too, and `inv_role_written_only_by_declared_writers`
      asserts SS-2/SS-3 change only through this action's Demotion arm or the
      failover/promotion writers)
      + the boot-ordering rule as a **guard** (a node proposes no other action until
      its boot report is applied): invariants `inv_run_identity_never_regresses` and
      `inv_no_spurious_cancel` — a replayed or reordered report from earlier in the
      boot must be a refused no-op, never a migration cancel. **Plus
      `loseIncarnationCounter(n)`** (V6-M2/M3) and the bootstrap/join proposal
      sequences (`AddNode` before `ReportRunIdentity` before anything else), with
      liveness witness `witnessNodeCanAlwaysReportIdentity` — a counter-loss node
      re-minting above the replicated pair always has an admissible boot report;
      reverting the re-mint rule (minting `(0,0)`) must make the witness
      unreachable, the mute-node defect as a mutation test. **Plus the Demotion
      fence** (V8-C2): the report payload carries `observed_role`/
      `observed_config_epoch`, and the Demotion arm refuses when either differs
      from the stored cell at apply; invariant
      `inv_promotion_is_not_reverted_without_a_failover` — a node promoted by a
      failover writer is never returned to `Replica` by a report minted before
      that promotion; reverting the fence must violate it (the stale-demotion
      clobber trace). **Plus the level-triggered re-proposal** (V8-C6): an action
      `reconcileIdentity(n)`, admissible whenever the replicated `NodeInfo`
      identity/topology facts differ from the node's local facts, re-proposes
      with fresh observations — the liveness half that makes the fence's refusals
      safe (a refused stale report is eventually superseded, never silently
      dropped).
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
  operand too — V6-C2's hole), (c) every *origin predicate* ("proposer is X" —
  V6-C1's hole: an origin fact is only evaluable at apply if it is a committed
  payload field), (d) every *arm selector* — where one transition's apply performs
  one of several behaviours, the fact selecting the arm must be a declared payload
  field or a replicated field (V7-C1's hole: "a demotion report" selected an arm,
  but nothing declared carried demotion-ness), and (e) every *edge-triggered rule* —
  a trigger that is a *change* rather than a *value* must name the two states being
  compared and the replicated field holding each, or be restated level-triggered
  (V7-C2's hole: "on abandonment, convert" compared a before to an after that no
  applier of a snapshot ever observes) — and for each verify the field with a
  declared type exists and name
  the component read — a name merely appearing somewhere in the document does not
  count** (V4 audit note, strengthened per V5, V6, V7, and again per V8: **six
  consecutive
  rounds each produced instances of exactly this class** — N-C4, V4-C3, V5-C2,
  V6-C1/C2, V7-C1/C2, then V8-C2/C6 (an unfenced arm selector and an
  edge-triggered reaction — items (d) and (e) exactly, found one round after
  those items were added to the check) — so the check must bind to declared
  types, not names, and must cover
  guards, origins, arm selectors, and edge triggers, not just comparison operands).
  **Staged-checkpoint boundary note** (V5-M4): the property "a shadow inside the
  staged checkpoint would be discarded by FM-REPLICATION-021's disarm path" belongs to
  the *replication* model's scope, not this one; the design discharges it by routing
  the shadow outside the stage, and the replication-side model (phase 3) owes the
  verdict when it lands. **Command-level pause exemptions are unmodelled** (V8-C5's
  class): the model's write action is one abstract "write to slot"; which client
  commands the seal exempts (§3's exempt-set rule) is below its granularity, so the
  exempt-set enumeration is discharged by the forcing test
  (`only_migrate_and_cluster_are_slot_pause_exempt`, re-pointed) and spec review —
  a future exemption added in code without a spec row is exactly what that lint-level
  test exists to catch. **Node-local durability classes are modelled only where a
  finding forced them** (V8-C3's class): ext-5's `targetRestart` regression is the
  one place the model distinguishes "applied" from "durable"; every other position
  in the model is implicitly durable, so any *new* report field derived from
  node-local volatile state owes the same regression treatment before the model can
  discharge it. The model carries no temporal operators: liveness
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
  one-shot reset (V6-M4); `FORGET` at `promoted == true` marks the entry
  `source_gone`, the entry survives, and a forgotten node attempting to rejoin
  while still holding its stale copy is **refused at the join-empty gate** — the
  operator event names the non-empty keyspace, the operator wipes, and the retried
  `MEET` admits an empty node serving nothing stale (V6-M5 as revised by V7-C2,
  re-revised by V8-C1); first `ReportMigrationIngest`/first `ObserveMigration` against
  `None` operands admitted per the §0 exceptions (V6-m2); rollback with
  `source_gone == true` requires `--accept-data-loss` (V6-m3); client `ClusterEvent`
  stream shows exactly one `SlotMigrationCompleted` despite per-transition operator
  event-log rows (V6-m4); graceful failover matching both held-write rows: the
  demotion disposition wins, held set never executed (V6-m6). From review v7:
  source down across its own `FORGET` (never observes the prune), later attempts
  rejoin via `MEET` holding a full stale copy of a slot whose residue entry
  `ResetCluster` has since removed: the join-empty gate refuses the handshake and
  the cluster's tracked state never includes the stale copy; after an operator
  wipe the rejoin succeeds, and while any entry *does* exist `Begin` over the slot
  stays refused (V7-C2 as re-revised by V8-C1); migrate-back after abandonment
  where the old source holds partial stale residue: §5's empty-live-region
  precondition refuses the promotion re-label, the operator event fires with the
  stale key count, rollback + an operator-ordered wipe (`accept_stale_copy`'s
  documented path) clear the region, and a clean retry promotes (V7-C2 as
  re-revised by V8-C1);
  lawful drain crossing the pre→post-`Confirm` boundary having consumed more than
  `postconfirm_observations` ticks pre-seal at stock defaults: survives, because
  `Confirm`'s apply reset the counter — reverting the reset makes this test fail
  (V7-C3). From review v8: `MEET` from a node with a non-empty main keyspace
  refused with the operator event; empty node admitted (V8-C1); `CLUSTER RESET`
  on a non-empty node refused; `FLUSHALL` + `RESET` succeeds (V8-C1);
  stale-demotion vs failover race — demotion report minted before a failover
  promotes the node arrives after: refused by the `observed_role`/epoch fence, no
  role revert, no slot stranded on a replica; the level-triggered re-proposal
  converges the cell (V8-C2/C6); target power-loss mid-ingest with
  `covered_applied` regressed below the reported floor: cross-run re-attestation
  replaces the stale-high positions, `Complete` held until the new run's proof
  covers the token — reverting the attesting-run conjunct loses acked writes
  (V8-C3); source demoted mid-residue with a successor: `RetargetSlotResidue`
  re-homes the entry, the successor's reaper deletes, `ConfirmSlotDeleted`
  admitted from the successor (V8-C4); `MIGRATE`/`RESTORE` naming keys in a
  sealed slot: held with the other writes and answered per the exit table, never
  executed past the seal (V8-C5); writes arriving *after* the self-fence latch
  flushed the held set: answered `-TRYAGAIN` immediately for the whole latched
  period — the second cohort never waits out the partition (V8-M1); knob-on
  migration with healthy lagging replicas completes at stock defaults — the
  replica-ack counter reset forced (V8-M2/m4); demotion-cancel of a
  successor-less source: held set answered `-TRYAGAIN`, never `-MOVED` to nobody,
  never executed (V8-M4).
