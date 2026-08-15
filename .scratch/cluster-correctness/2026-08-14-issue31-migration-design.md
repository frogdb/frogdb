# Slot migration redesign — source-authoritative-until-commit (v15)

Status: revision 15 — **candidate, UNSOUND-pending**: not approved, and not
claimed sound until a review round finds nothing structural. Review v15 (of
revision 14) found 4 CRITICAL / 5 MAJOR / 5 MINOR, and — for the **sixth
consecutive round**, by the tally the mechanical-check section keeps — the
worst defects were in the *previous round's own fix text*, all four
CRITICALs inside the three adjudication stamps revision 14 introduced one
round earlier: the companion-field rule was keyed on writers of
**`role`**, while two LOCKED appliers write a node's **parent pointer**
without writing its role — the failover **sibling re-parent**
(`reparent_children(.., Some(new_primary))`, TR-CLUSTER-018/042) and
`RemoveNode`'s **detach** (`reparent_children(.., None)`,
TR-CLUSTER-003/FM-CLUSTER-002) — so every surviving sibling of a failover
kept a stale `admitted_stage`/`synced` (V15-C1); the staged adoption's
operand list and the reconcile guard could both claim the same state after a
re-parent, with no rule saying which (V15-C2); the shape
`role == Replica ∧ primary_id == None` was declared *unreachable* when
FM-CLUSTER-002 specifies it, force-tested, as the deliberate **detached
replica** (V15-C3); and `AttestReplicaSynced` carried no parent fence, so an
attestation minted before a re-parent could stamp `synced = true` about a
parent whose history the node holds none of — the zero-byte candidate V14-C3
existed to prevent (V15-C4). Revision 15 resolves all fourteen findings —
centrally by **re-basing the companion-field rule on `primary_id` writers**
(with two named carve-outs), by **collapsing the adoption's five operands to
one selector**, `admitted_stage == Some(record.stage_id)`, which the
reconcile guard reads in its negated direction so the two can never contend,
by **declaring the detached replica** as a tracked state with named exits,
by **fencing the attestation** with an observed parent/epoch pair, by
**binding every refusal disposition to its record's `stage_id`**, and by a
new permanent **writer-join subsection** that joins this design's writer
claims against LOCKED `specs/cluster.md` cells verbatim — the mechanical
check that would have caught all three of revision 14's CRITICALs and the
one this round adds. Review v14 (of revision 13) found 3
CRITICAL / 7 MAJOR / 5 MINOR, and this revision's fixes are **structural rather
than textual**: v13's whole role-transition surface was written against
`REPLICAOF` spellings the server refuses at dispatch in cluster mode, so the
`Promotion` kind, the pending-promotion record and the Promotion role-writer arm
described paths no client can take, while the adoption's four-operand binding
was unsatisfiable whenever the staged upstream left membership (V14-C1); the
lineage guard's bare `u ∉ nodes` disjunct admitted a *foreign* lineage as
`Primary` — cross-shard data substitution (V14-C2); the staged flip published a
replicated replica pointer before a byte of data flowed, making a zero-byte node
a lawful failover candidate (V14-C3); and v13's own crash-durability fix
re-minted the very operand it made load-bearing, so no post-crash adoption could
ever fire (V14-M1). Revision 14 resolves all fifteen findings — centrally by
**deleting the promotion surface** and re-grounding the demotion surface on
`CLUSTER REPLICATE`/`CLUSTER FAILOVER`, by three new replicated adjudication
stamps (`promoted_from`, `synced`, `admitted_stage`) each written by every writer
of the role fact it companions, by the record's crash-durable **`stage_id`** and
**`adopted`** flag with a stage-resolution precedence rule, and by making
**`RefusalClass`** a committed apply outcome so gate 3 (d)'s refusal partition is
six declared classes rather than three prose arms. Review v13 (of revision 12) found 1
CRITICAL / 6 MAJOR / 3 MINOR, all in v12's own record machinery: the adoption's
two-operand firing condition (record present ∧ replicated `Replica`) fired on
*any* writer of `Replica` — a concurrent `Failover{force: false}` demoting the
node under an in-shard successor made the adoption wipe the keyspace toward the
record's upstream on someone else's admission (V13-C1); the lineage guard's
universal ("in every reachable state the guard passes") was false under LOCKED
TR-CLUSTER-042's outright removal of the old primary — re-creating V12-M2's
mute node on the ordinary forced-failover-plus-crash path (V13-M1); "in-shard
member" had no declared predicate (V13-M2); the crash-durable candidate triple
conflicted with §0's per-boot mint (V13-M3); adoption was undefined at
`role == Replica ∧ primary_id == None` — a permanent proposal loop (V13-M4);
a second staged command while a record is pending was undeclared (V13-M5); and
the fence-refused staged Demotion had contradictory dispositions across three
passages (V13-M6). Revision 13 resolved all nine findings — centrally by the
**four-operand adoption binding** (record present ∧ replicated role, upstream,
*and* run-identity all matching the record's own admitted report — discard
fires only on the record's own admission, never someone else's), a **total
three-arm refusal partition** (revert / fence-retry with the record persisting
/ supersession), the declared one-hop **`in_shard_parent`** predicate, the
**widened lineage guard** (`u == None ∨ u ∉ nodes ∨ in_shard_parent(u, self)`,
citing TR-CLUSTER-042), per-boot **candidate re-derivation**, the
**quiesced-replica** arm, and the record's **single-writer rule**.
Review v12 (of revision 11) found 1
CRITICAL / 2 MAJOR / 3 MINOR, all in v11's own staging machinery: the staged
flip's reconcile and its kind rule read the same plane-disagreement predicate
in opposite directions — a reconcile tick in the post-admission/pre-adoption
window stamped a spurious `Promotion` (re-opening SS-11's assignee door), and
a crash in that window booted a `Promotion`-stamped Primary serving an
untracked stale copy — while colliding uncited with TR-CLUSTER-033's LOCKED
opposite-direction reconciler (V12-C1); the staged fence had no §3 row, so
"holding per §3" bound to nothing and held clients indefinitely under
partition (V12-M1); and "converges any split however reached" was false for
the local-Replica/replicated-Primary direction — the Demotion arm's
upstream-validity conjunct refused forever and the boot-ordering rule muted
the node (V12-M2); plus the candidate triple with no declared durable home
(V12-m1), its reconcile-membership undefined (V12-m2), and a vacuous
demotion-disposition trigger (V12-m3). 3/5 of v11's findings RESOLVED, 2
PARTIAL, no regressions. Revision 12 resolved all of v12's findings —
centrally by the **durable pending-transition record**: fsynced before the
report proposes, it is the kind rule's sole discriminator, the candidate
triple's durable home, the key of the new §3 staged-flip fence row
(nothing held, uniform immediate `-TRYAGAIN`), and the trigger of the
**level-triggered** adoption; its *absence* plus a declared **role
authority** (replicated plane wins; local plane originates transitions only
through the record protocol) converges unexplained splits replicated→local
behind a lineage guard, stamped `Boot`.
Reviews: issue31-adversarial-review-v2 through -v14, job dir 2026-08-14.
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
**Proposing moments, each stamped into the payload as `kind`** (V7-C1 — the
moments produce indistinguishable `(node, run_id)` triples, per FM-REPLICATION-022 a
demotion and a boot both bump `identity_seq`, so the arm a report
selects must be a committed payload fact, never inferred at apply)
(V4-M4a — the FM-REPLICATION-023 one-cell-per-process identity changes):
**boot** (`kind = Boot`; a **`ResetCluster` re-mint also reports `kind = Boot`** —
V10-m4: reset is a third identity *moment* but deliberately not a third *arm*,
because `ResetCluster`'s own apply writes the node's role and membership, so the
Boot arm's write-only-the-identity behavior is exactly what the reset report
needs — stated again at the lifecycle bullet below) and **node-originated
demotion / history adoption** (`kind = Demotion` — gate 3's staged flip on
`CLUSTER REPLICATE <target-node-id>`, backed by the
durable pending-transition record;
FM-REPLICATION-022: a demotion ends the stint and
`adopt_replication_history` replaces the replid
on link-up — a full history discontinuity that must reach the replicated field).
**`kind ∈ {Boot, Demotion}`; there is no `Promotion` kind and no `Promotion`
arm** (V14-C1/V14-M5 — see the cluster-mode command surface declared at gate 3:
the only minting moment a `Promotion` arm ever had was `REPLICAOF NO ONE`, which
cluster mode refuses at dispatch, so the arm had no producer).
A **failover-driven** role change — the *only* promotion path in cluster mode —
is an identity moment but not a
`Demotion` report: the failover apply already wrote the role, so
the node's history-adoption report is stamped `kind = Boot` (the same
moment-not-arm shape as the `ResetCluster` re-mint above — V12-C1's stamping
rule, discriminated by the durable pending record, never bare plane
comparison).
On a `Demotion` report the payload's `new_primary_id: Option<NodeId>` is
**always `Some(record.target_upstream)`** — the upstream the node now
replicates, stamped at proposal time from the pending-transition record's typed
`target_upstream` field (V14-C1/V14-m2 — the record's `target_upstream` is the
`CLUSTER REPLICATE` argument, a NodeId; a non-member or address-shaped upstream
is unrepresentable, and a `Demotion` payload carrying `new_primary_id == None`
is **refused as malformed** at the row, since no producer mints one). A
`Demotion` report likewise always carries `Some(stage_id)` in the payload's
`stage_id: Option<u64>`, stamped from the record (V14-M1 — declared at gate 3).
A `Boot` report carries `None` in both. Every
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
(boot, promotion, demotion/adoption, cluster reset — and **only** these genuine
discontinuities, V10-m4/V10-M1) — that node-local durable counter is the *mint*;
the *authoritative comparison operand* is the copy inside the replicated
`NodeInfo.run_identity` triple. `ReportRunIdentity` admission: proposer is `node` ∧
`(payload.incarnation, payload.identity_seq) ≥ (stored.incarnation,
stored.identity_seq)` (lexicographic), **both sides read from replicated state and the
committed payload — never a node-local cell at apply** (FM-CLUSTER-089 determinism
preserved; the conjunct is evaluable identically on every applier). **The ordering
conjunct is split (V10-M1)**: the **strict** comparison (`>`) guards only the
`run_identity` **field write** — apply writes the field iff the payload triple orders
strictly above the stored one. At **equality** the report is still admitted, as a
**topology-only re-proposal**: the field write is skipped (it would be idempotent) and
the selected arm's role/upstream writes proceed under their own per-object fence
conjuncts, which consume the payload's fresh committed topology facts. This is what
lets a level-triggered reconcile re-report the *same* identity with corrected
role/upstream facts without minting a fake discontinuity. A payload triple ordering
strictly *below* the stored one is a refused no-op — it can neither regress
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
**join** orders `AddNode` (proposed by the meeting node — TR-CLUSTER-005's
`MEET → AddNode` handshake) `→
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
- **`ResetCluster`**: rewinds the membership generation and reduces membership
  **to just this node** (TR-CLUSTER-035) — the node's own `NodeInfo` is
  **retained**, `run_identity` cell included (V9-m3 corrects V8-M3's "re-registers
  fresh" reading: only the HARD path's `node_id → new_node_id` produces a fresh,
  absent cell, because a new `node_id` is a new map key). A **SOFT** reset
  therefore does *not* re-run the "field initializes absent" path: the reset node
  re-mints (identity change: reset is an identity event, `identity_seq` bumps),
  the re-mint orders strictly above the retained pair, so the ordering conjunct
  admits the boot report against the retained cell. A **HARD** reset is the
  bootstrap-like case — fresh absent cell, first report always admitted. Either
  way the node must complete its boot `ReportRunIdentity` before any other
  proposal, per the boot rule.
- **Raft snapshot install**: the installing node adopts the replicated `nodes` map
  wholesale, including `nodes[self].run_identity`. The installed value may be
  *older* than the node's current minted triple (the snapshot predates its latest
  report). No special arm is needed: the standing level-triggered reconcile rule
  (the `ReportRunIdentity` re-proposal arm — replicated identity facts differing
  from local facts trigger a re-report) fires on exactly this divergence, and the
  boot rule's "has applied" test is evaluated against post-install applied state,
  so a node mid-install cannot satisfy it with pre-install state.
- **The pending-transition record across these flows**
  (V13-m2 — the record is node-local durable state, so the flows that destroy
  the identity cell must state what happens to it, else a surviving record
  makes the kind-stamping rule and a flow's own `Boot`-report rule claim the
  same report; singular since V14-C1 deleted the pending-promotion record):
  **`ResetCluster` (both SOFT and HARD), removal of self
  (observing one's own `RemoveNode`/`FORGET` apply, or discovering it at
  rejoin), and a fresh join (`MEET` handshake admitting this node)** each
  **clear the record — fsynced — before the re-mint/boot report**. The staged
  intent named a `target_upstream` adjudicated in the *previous* cluster
  generation; no rule carries it across a membership reset, so the
  post-reset/post-join first report is stamped `Boot` per the kind rule's
  default, with no competing claim. **Residue of an in-flight report**
  (V14-m4): clearing the record does not withdraw a report already proposed
  into the *old* cluster's log — it may still apply there, writing a
  `role = Replica, primary_id = Some(target)` entry for a node that has since
  left. That entry is **inert**: the departed node never proposes
  `AttestReplicaSynced` in that cluster, so its `synced` stays `false` and
  TR-CLUSTER-021's amended candidacy gate (F3/V14-C3) can never select it;
  the standard operator `CLUSTER FORGET` of the departed node completes the
  cleanup. `stage_id` is **not** reset by any of these flows — like the
  incarnation mint it counts monotonically, so a post-reset stage can never
  collide with a pre-reset one (V14-M7).

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
(V6-M3, above). The declared absence tests, each also stated at its row (V10-m6
completes the enumeration): `ClearSlotResidue`'s `shard_primary(entry.source) ==
None` conjunct (V8-M6); `PrepareSlotHandoff`'s `record.attempt_id == None`
conjunct (no live attempt may exist — an absence test, not a value read); and
`BeginSlotMigration`'s two map-absence conjuncts (`slot ∉ migrations`,
`slot ∉ handoff_residue` — key-absence tests on maps, to which the Option rule
does not apply but which are declared here for the same reason).
`CompleteSlotMigration`'s `record.drained_pos == Some(token)` is a **value read**
— false at `None` under the standing rule — stated at its row. §9's derived
metrics (the ack-lag subtraction) render only when both
operands are set.

**Target-side heads, classified** (N-M7 — two distinct positions, per §0's own rule):

- `covered_received` — highest contiguous position received on the stream. Operand of
  the per-batch contiguity assertions (checked at receipt, §4).
- `covered_applied` — highest position **durably applied** into the shadow, where
  "durably" means **fsync-durable, regardless of the target's configured
  durability** (V9-C2 revises V5-m6's durability inheritance: the attested
  position is a *protocol commitment* that `Complete`'s admission consumes, not an
  ordinary keyspace write — under inheritance, a relaxed-durability target could
  crash back below its own last report, and the stale-high replicated value would
  satisfy `Complete` while the target is down, because only the target itself
  ever falsifies the attesting-run conjunct. The shadow's *writes* still ride the
  ordinary write path; the **report** is what is gated: `ReportMigrationIngest`
  is proposed only for positions the shadow has been fsynced through, so no
  crash can regress the shadow below any position the target has attested, and
  a stale-high replicated attestation cannot exist. V8-C3's cross-run
  replacement — the attesting-run pairing, with `Complete` requiring the
  attesting run current — remains as belt-and-braces for the
  restart-that-reports path). The value
  reported in `ReportMigrationIngest`, the resume point after any target crash or
  session re-establishment (§4 — a target restart resumes; only a *source* restart
  cancels), and the stream's ack unit (§8). The received-but-unapplied window
  (`covered_received − covered_applied`) is lost on a target crash by design and
  re-requested at resume — never silently skipped.

Only `covered_applied` (and its replica-side floor `target_replicas_acked_pos`, §5)
ever reaches an admission predicate. **The floor carries the same durable-attestation
meaning (V10-C2)**: a counted replica's `ReportTargetReplicaAck` position attests
**fsync-durable application on that replica, regardless of the replica's configured
durability** — the same protocol-commitment argument as `covered_applied` above, with
the same consequence: no replica crash can regress a replica below any position it
has attested, so the replicated floor can never sit above what every counted replica
durably holds. Stated again at the report's row and in §5's install contract.

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
transitively, at most `|nodes|` steps. The result is `None` — not an error — when
`n ∉ nodes` (V10-m5: the function is total over all `NodeId` arguments — callers may
hold an `entry.source` whose node has since been removed), when the
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

**`in_shard_parent(u, n)` — the lineage guard's one-hop parent relation, defined
once** (V13-M2 — the guard's "in-shard member" phrase named no declared predicate,
and the only total reading available was the multi-hop `shard_primary` walk V10-C4
removed the residue target arm for — an unproven pointer under re-parenting):
`in_shard_parent(u, n)` ≜ `u ∈ nodes ∧ nodes[u].primary_id == Some(n)` — a direct
pre-apply pointer read, the same discipline as the Demotion arm's
shard-relationship conjunct, **never a `shard_primary` walk** (a foreign upstream
later re-parented under `n` via a multi-hop chain would make a walk pass while the
one-hop relation stays false only for genuinely un-adjudicated lineages; the
one-hop read admits exactly the demoted-under-`n` case a failover produces). It
reads only replicated fields and is total over all arguments (`false` when
`u ∉ nodes` — that case is admitted by the lineage guard's own removed-upstream
disjunct, not by this predicate).

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
  that node as `source` to the validated successor `p`** (V9-C1: `p` is the
  demotion's admission-proven successor — the promoted replica for `Failover`,
  `payload.new_primary_id` for `SetRole` and the `Demotion` arm, each proven a
  current in-shard replica of the demoting node against **pre-apply** state by
  the transition's own admission conjunct; the destination is never derived from
  a pointer the same apply writes, and never a `shard_primary` walk evaluated
  after the arm's own role/`primary_id` writes — post-write, that walk can land
  on a *foreign* shard's primary, a token-free re-assignment of the slot to a
  node holding none of its data. The successor holds the
  same keys via replication and its feed already reaches the shard's replicas; the
  successor's reaper performs the delete and proposes `ConfirmSlotDeleted`).
  Symmetrically, an entry naming the demoted node as **target** at
  `promoted == false` re-targets its `target` to `p` (the same rule as the
  failover-with-successor arm above). **This in-apply re-target is the *only*
  target-side re-home** (V10-C4 — the level-triggered target arm of
  `RetargetSlotResidue` reproduced V9-C1's unproven pointer walk: once the
  demotion has applied, no predicate over *current* state can distinguish the
  demoted target's lineage successor from a foreign primary the target was later
  re-parented onto, so a sound destination proof does not exist outside the
  demotion's own apply, and the arm is removed rather than guarded. In-apply
  re-targeting is total nonetheless: `Complete` creates residue entries only
  while the target's role is Primary — its V10-C3 conjunct — and every
  transition that writes role → Replica on a member (the three above) carries an
  admission-validated successor and re-targets every residue entry naming that
  node in the same apply, with refuse-whole covering the successor-less case; so
  no reachable state holds an unpromoted entry naming a non-primary target, and
  no level-triggered target catch-up is needed. This supersedes V9-M1's stuck
  state by construction, not by an extra remover.) A demotion that carries no validated
  successor while the node owns slots or is named by a residue entry **refuses
  whole** at admission (the `ReportRunIdentity` row and TR-CLUSTER-004's ruled
  precondition, V9-C1) — so within these three transitions the skip case is
  unreachable; the level-triggered
  `RetargetSlotResidue` transition (below, **source arm only** — V10-C4) remains the
  re-derivable catch-up for any entry that nonetheless names a non-primary
  **source**. **`RetargetSlotResidue`
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
  the design closes the *entrance* instead, with three node-local fail-closed
  admission gates — the Redis analogs):
  1. **Join-empty**: a node accepts cluster membership — the `MEET`/`AddNode`
     handshake (TR-CLUSTER-005), both first join and re-join after `FORGET` —
     only while its main keyspace is empty. A non-empty joiner **refuses the
     handshake node-locally**, emitting an operator event naming the non-empty
     slots (Redis's rule: a node holding data cannot join; the operator wipes
     first). The refusal is proposal-side/handshake-
     side, never an apply-time deletion: fail-closed, deletes nothing. This gate
     **composes with** TR-CLUSTER-005's issue-25 ruled precondition (empty local
     Raft state) — the row's verdict in the blast-radius list states the composed
     precondition, and the refusal names *which* gate fired. The complete wipe
     sequence a rejoining node's operator must perform, stated once:
     `CLUSTER RESET HARD` (clears Raft state per TR-CLUSTER-035's HARD path *and*
     requires/effects the empty DB — gate 2 below refuses it on a non-empty
     keyspace, so `FLUSHALL` first when data remains). `FLUSHALL` alone is
     insufficient — the node still fails the empty-Raft-state precondition.
  2. **`ResetCluster` refuses on a non-empty main keyspace** (Redis `CLUSTER RESET
     HARD` requires an empty DB): reset is the path back toward a re-join, so the
     same gate holds there; the error names the non-empty slots and the wipe the
     operator must perform deliberately.
  3. **Replication-command gate** (V10-C1). First, the **cluster-mode role-command
     surface**, declared once here and referenced from §0, the Transitions rows,
     §3 and the Quint section (V14-C1/V14-M5/V14-m1 — revision 13 treated bare
     `REPLICAOF <host> <port>` as a lawful gate-3 path and `REPLICAOF NO ONE` as
     the node-originated promotion; both are wrong against the implementation and
     against Redis):
     - **Every `REPLICAOF`/`SLAVEOF` spelling — `<host> <port>` and `NO ONE`
       alike — is refused at dispatch in cluster mode** with
       `-ERR REPLICAOF not allowed in cluster mode.` This is existing implemented
       behavior, not a new mandate: `frogdb-server/crates/server/src/commands/replication.rs`
       returns that error whenever `ctx.cluster_state.is_some()` (Redis parity).
       Such a command therefore never reaches the staged fence, the single-writer
       rule, or any gate below — which is why the fence's exempt set and the
       single-writer rule cannot disagree about its reply (V14-m1).
     - **Node-originated demotion has exactly one spelling:
       `CLUSTER REPLICATE <target-node-id>`** — a NodeId argument, which is what
       makes the pending record's `target_upstream` a NodeId rather than an
       address (V14-m2).
     - **There is no node-originated promotion spelling at all.**
       `CLUSTER FAILOVER` in every variant (plain, `FORCE`, `TAKEOVER`) proposes a
       replicated `Failover` op (`frogdb-server/crates/server/src/commands/cluster/admin.rs`,
       `cluster_failover` → `Response::RaftNeeded`), so every cluster-mode
       promotion is failover-driven and replicated-first, and the promotee's
       history-adoption report is stamped `kind = Boot` by §0's moment-not-arm
       rule. Consequently **the `Promotion` arm and the pending-promotion record
       are deleted** (`kind ∈ {Boot, Demotion}`): V9-M3's worry — a promotion the
       failover machinery never saw — had `REPLICAOF NO ONE` as its only
       producer, and that producer does not exist.

     The gate proper: a node refuses `CLUSTER REPLICATE` **node-locally** while,
     per its applied state, the slot map assigns it any slot **or** any
     `handoff_residue` entry names it; it *also* refuses when the named
     `<target-node-id>` is **not a current member whose replicated
     `role == Primary`** (V14-C1 — a fast node-local pre-check of the same fact
     the Demotion arm's upstream-validity conjunct re-validates authoritatively
     at apply; the pre-check keeps the common operator typo cheap, the conjunct
     is what makes it safe). The refusal error names the binding slots/entries or
     the invalid upstream, and the lawful paths out: a failover that demotes the
     node with a validated successor, or moving the slots first
     (`AssignSlots`/`SETSLOT`, with the loss token where the residue rules demand
     one). The gate is the fast, node-local refusal (Redis parity:
     `CLUSTER REPLICATE` refuses on a node that owns slots or holds keys), and it
     is what makes the common case cheap — but it reads applied state, so a
     concurrent apply (an `AssignSlots` landing between the gate check and the
     flip) can bind the node after the check passed.
     **The flip is therefore staged** (V11-C2, revising V10-C1's declared-window
     disposition — the window's *data*-plane consequence had propagated nowhere:
     adopting a foreign upstream discards the node's dataset
     (FM-REPLICATION-022), so any exit that returned a slot to a node that had
     already flipped — a failover verdict, or §1 case 3's "lossless" rollback —
     would have resumed serving out of a wiped keyspace: acked-write loss with
     no token anywhere). A `CLUSTER REPLICATE` that passes the gate
     executes in stages: the node **(a)** durably records the staged intent and
     fences — it fsyncs a **pending-transition record**
     `{kind: Demotion, target_upstream: NodeId, stage_id: u64,
     candidate_triple: {replid, incarnation, identity_seq}, adopted: bool}`
     to its own node-local file (every field typed — V14-m2; the
     incarnation-counter discipline: file and parent directory fsynced before
     any further step — V12-C1/V12-m1: the record is what makes every later
     step *level-triggered re-derivable* rather than an edge a crash can lose,
     and it is the declared writer and durable home of the **candidate**
     post-demotion triple, minted here but not yet effective in the node's
     replication runtime. Crash-durability is per component (V13-M3):
     `{kind, target_upstream, stage_id, adopted}` survive a crash as written,
     but the
     `candidate_triple` is **re-derived at every boot** — a node booting with
     a pending record re-mints the candidate against its *current* incarnation
     (freshly incremented and fsynced per §0) and rewrites the record, fsynced,
     before re-proposing, so the record's report *is* the boot report and
     carries the boot's incarnation: §0's per-boot mint rule and the
     boot-ordering rule's "has applied" test agree on one triple, and a
     pre-crash candidate never outlives its boot. **`stage_id` is the stage's
     boot-stable identity** (V14-M1/V14-M7): a node-local monotone `u64`
     counter persisted and fsynced under the same discipline as the
     incarnation, **incremented when a record is created and never re-minted
     for an existing record** — so unlike the candidate triple it is the same
     value before and after any number of crashes, which is what lets the
     adoption's firing condition survive the re-mint. `adopted: bool` is the
     adoption's own crash-idempotence flag, initialized `false`, described at
     (c). The record has **exactly one
     writer per pending stage** (V13-M5, respelled per V14-m1): a second
     `CLUSTER REPLICATE`, or a `CLUSTER FAILOVER` in any variant, arriving
     while a record is pending is refused
     node-locally with an error naming the pending stage and never overwrites
     the record — both are fence-exempt (the exempt set below) precisely so
     they reach this rule and receive that one consistent reply, while
     `REPLICAOF` spellings die at dispatch and never arrive.
     `CLUSTER INFO`'s pending-stage field is the read path, so
     the advertised retry observes the outcome by being refused while pending
     and re-evaluated by gate 3 once the stage resolves), then fences the
     **whole node** — the disposition is determinate (V12-M1) and its scope is
     enumerated, not qualified (V14-m5/V14-m4: "every command that would
     execute on this node as a slot-serving primary" bound to nothing a reader
     could evaluate): **nothing is held**; **every command arriving at the
     staged node — keyed or keyless, read or write, admin included, and
     including writes for a slot an `AssignSlots` assigns it *after* the gate
     check (the race interleaving staging exists for) — is answered
     `-TRYAGAIN` immediately**, per §3's **staged-flip fence row**, except the
     enumerated **exempt set**:
     - *read-only introspection* — `CLUSTER INFO` (the pending-stage read
       path), `CLUSTER MYID`, `CLUSTER NODES`, `CLUSTER SHARDS`,
       `CLUSTER SLOTS`, `CLUSTER COUNTKEYSINSLOT`, `CLUSTER GETKEYSINSLOT`;
     - *gate-reaching* — `CLUSTER REPLICATE` and `CLUSTER FAILOVER` (any
       variant), which must reach the single-writer rule above for its
       declared pending-stage refusal;
     - *operator escape* — `CLUSTER RESET` (still gated by gate 2's
       empty-keyspace rule) and `CLUSTER FORGET`.

     **Everything else is `-TRYAGAIN`, explicitly including
     `CLUSTER FLUSHSLOTS` and every other mutating `CLUSTER` subcommand** — the
     staging exists to keep the dataset and the slot map intact until the
     replicated veto lands, so a mutating admin verb is exactly what it must
     not admit — **dataset untouched**;
     **(b)** proposes its `ReportRunIdentity{kind: Demotion}` with
     fresh observations, carrying the candidate triple **and the `stage_id`**
     from the record; the
     candidate's fate on refusal follows (d)'s refusal partition (V13-M6):
     on a refuse-whole revert or a supersession the candidate is discarded
     with the record, while on a fence refusal it **persists in the record**
     and the re-proposal carries it unchanged (the strict-branch admission
     in the role-authority passage). In every discard case the stint
     never ended, no discontinuity occurred and no bump is visible anywhere
     (§0's "bumps only at genuine discontinuities" claim holds on both
     branches — the bump becomes real exactly when the admission's field write
     lands); **(c)** adopts the foreign upstream — the destructive
     discard that ends the stint — gated on **that report's admission**, by
     which point the Demotion arm has re-homed every owned slot and re-targeted
     every residue entry to a validated successor, or refused whole. The
     adoption is **level-triggered, not edge-triggered** (V12-C1, the V7-C2
     discipline applied to v11's own fix text), and its firing condition
     names **five operands** — the record's presence, the `adopted`
     idempotence flag, the replicated role, the replicated upstream, and the
     replicated stage stamp — which bind it to *this record's own admitted
     report*, never to any demotion (V13-C1 — the two-operand form *pending
     record ∧ replicated role `Replica`* was satisfied by every other writer
     of `role = Replica`, e.g. a concurrent `Failover{force: false}`
     (TR-CLUSTER-018) demoting the node under an in-shard successor, and
     would have fired the destructive discard toward the *record's* upstream
     on someone else's admission): *pending record present ∧
     `record.adopted == false` ∧ applied
     replicated `nodes[self].role == Replica` ∧ applied replicated
     `nodes[self].primary_id == Some(record.target_upstream)` ∧ applied
     replicated `nodes[self].admitted_stage == Some(record.stage_id)`*.
     The load-bearing operand is the replicated
     **`admitted_stage: Option<u64>`**
     field (V14-M1, replacing revision 13's `run_identity == Some(record's
     candidate triple)` operand, which the per-boot candidate re-mint
     falsified at exactly the boot that had to re-derive the adoption): the
     Demotion arm is its **sole minting site**, and **every writer of
     `nodes[self].primary_id` clears it** (§0's companion-field rule), so only
     the record's *own* report can set it to this record's `stage_id`.

     **The five operands collapse to one selector** (V15-C2, this round's
     other structural correction). By the companion-field rule any apply that
     moves `primary_id` clears `admitted_stage`; and by LOCKED SS-2/SS-3 every
     writer of `role` is also a writer of `primary_id`. Therefore
     `admitted_stage == Some(record.stage_id)` **implies** both
     `primary_id == Some(record.target_upstream)` and `role == Replica`: the
     Demotion arm wrote all three in one apply, and nothing has written
     `primary_id` since. Operands 3 and 4 are kept in the statement above for
     legibility, but they **cannot be false while operand 5 is true — they go
     false together, or not at all**. V13-C1's separation now falls out of
     that general rule rather than an arm-specific stipulation: a
     `Failover{force: false}` demoting the staged node writes its
     `primary_id` to the successor, hence clears `admitted_stage`, hence
     leaves the condition false, so someone else's admission can never fire
     this record's destructive discard. What remains is a single predicate
     over durable state and one replicated field:
     - **resolved** — `admitted_stage == Some(record.stage_id)`: this
       record's own report is admitted and the node's parent has not moved
       since. *Adopt*; **never propose**.
     - **unresolved** — `admitted_stage ≠ Some(record.stage_id)`: the report
       has not landed, or a later apply moved the node's parent out from
       under it. *Propose* (the reconcile guard below); **never adopt**.

     That one predicate selects between (c)'s adoption and the reconcile's
     proposal — the two rules revision 14 left both claiming the same state
     (V15-C2: a re-parent after admission falsified the adoption's
     `primary_id` operand while `admitted_stage` stayed stamped, so (c)
     waited forever on a condition nothing could restore while the reconcile
     read the stage as resolved and proposed nothing — an immortal record
     under a never-lifting whole-node fence whose only exits destroyed the
     node's dataset). **The trace in its live form**, which the re-based rule
     discharges end to end: the staged node's report admits (stamp `Some(s)`,
     parent `U`); a `Failover` then demotes `U` and `reparent_children` moves
     the staged node onto the successor `S`; that write clears
     `admitted_stage`; the stage reads **unresolved**; the reconcile
     re-proposes the record's own report; the report is refused
     `upstream-validity` (`U` is no longer a member whose `role == Primary`)
     at a node that is no longer replicated-`Primary`, which is gate 3 (d)
     **arm 3 — supersession**: record cleared, fence lifted, client answered,
     and the node converges to the failover's verdict through role-authority
     adoption. Nothing is destroyed on the way out, and the fence lifts in
     bounded time (one reconcile round plus one Raft round). If `U` is still
     a lawful primary when the re-proposal applies, the report simply
     **re-admits** — the node owns no slots and no residue names it, so the
     shard-relationship conjunct does not bind — the stamp is re-written, the
     stage reads resolved, and the adoption fires: the operator's intended
     outcome, one round later. All observation operands are
     replicated fields (plus
     the durable record), and **every one of them survives the candidate
     re-mint and the boot**, so the condition is genuinely
     level-re-derivable at boot;
     it is evaluated node-locally, never at apply (FM-CLUSTER-089
     untouched). This binding also subsumes the "persisting across ticks"
     damping TR-CLUSTER-033's LOCKED row carries (see Transitions): under
     that row a transiently stale applied view re-emits an idempotent event,
     but here the consequence is a destructive discard, so the guard binds
     to admitted operands instead of tick-persistence — strictly stronger.
     Observing the admission apply is merely the common first
     evaluation. **Completion is crash-idempotent through the record's
     `adopted` flag** (V14-M1 — revision 13 promised a node booting after a
     durable adoption would "just clear the record", but backed the promise
     with a re-evaluation the candidate re-mint falsified): the order is
     *fire the destructive discard and link the new upstream* → *fsync
     `record.adopted = true`* → *clear the record (fsynced)*. The whole
     machinery — at a boot and at every later tick alike — is therefore the
     **one three-way rule** below, evaluated over durable state only, with no
     "observed", "since" or "already tried" operand anywhere (V15-m3):
     1. `record.adopted == true` → **skip the discard and clear the record**,
        reading a durable flag rather than re-deriving anything;
     2. `record.adopted == false` ∧ stage **resolved** → **adopt**: fire the
        discard, link the upstream, then (1)'s ordering;
     3. `record.adopted == false` ∧ stage **unresolved** → **propose**: at a
        boot, first re-mint the candidate and rewrite the record (**same
        `stage_id`**); then propose the record's report and wait.

     Every terminal refusal clears the record (fsynced) *before* the client is
     answered, so "a record is present" is itself the durable statement that
     no terminal outcome has been reached — which is why no volatile
     "outcome observed" predicate appears in this rule or in the reconcile
     guard below (V15-m3: revision 14 carried one in both places, an operand
     with no declared type and no durable home); **(d)** a
     refusal's disposition is selected by **two declared facts** — the
     refusal's declared **`RefusalClass`** (a committed apply outcome, declared
     at the `ReportRunIdentity` row — V14-M3) and, where the class needs it,
     the applied replicated `nodes[self].role` at the refusal (V13-M6/V13-C1:
     v12's unqualified revert claimed states where "both planes agree" and
     "nothing was destroyed" were false, and contradicted the convergence
     paragraph's fence-retry rule).

     **Record-binding conjunct** (V15-M1, stated once and a conjunct of
     **every one of the six arms below**): an arm fires only for a refusal
     whose payload belongs to *this* record —
     `refused_payload.stage_id == Some(record.stage_id)`. A refusal carrying
     any other `stage_id` — a `None`-stamped `Boot` report's refusal, or a
     refusal belonging to an **earlier, already-cleared** record — is a
     **log-only no-op**: no arm is selected, the record and fence are
     untouched. Revision 14 declared per-proposal attribution unnecessary on
     the strength of the precedence rule below, but that rule only neutralises
     duplicates of the *live* stage; a refusal of stage `S1`, whose record was
     cleared and re-staged as `S2` before the refusal was observed, routed
     into arms 1/3 and disposed of a live record — clearing `S2`'s record and
     dropping its fence while `S2`'s report was still in flight, which
     re-opens exactly the revert-then-admit race the staging exists to close.
     The conjunct is evaluable because `stage_id` is a committed payload field
     (`Some` on every `Demotion` report, `None` on every `Boot` report) and
     `record.stage_id` is durable node-local state; it is compared node-locally
     at the proposer, never at apply (FM-CLUSTER-089 untouched). This also
     subsumes V14-M7's volatile-damping residue: the "at most one proposal in
     flight" damping is a *flooding* control, and no longer carries any
     correctness weight in selecting a disposition — a refusal that arrives
     after the damping state was lost to a crash is disposed of by the two
     durable facts (`record.stage_id`, `admitted_stage`) and by nothing else.

     **Stage-resolution precedence rule** (V14-M7, stated once, checked before
     any arm): a refusal's disposition applies **only while the record's stage
     is unresolved**. Once applied state shows
     `nodes[self].admitted_stage == Some(record.stage_id)` the stage is
     **resolved-admitted**, and every later refusal of a same-stage report —
     for instance the pre-crash duplicate that the boot re-proposal raced — is
     a **stale-duplicate no-op** whatever its class: the record and fence are
     untouched and the adoption machinery runs to completion. A refusal is
     never acted on against a stage whose admission is already stamped in
     replicated state. **Same-stage duplicates are therefore idempotent, and
     the argument is durable-state-only** (V15-M1/V15-m3): a same-stage
     refusal observed in the *resolved* state is a no-op by this rule; a
     same-stage refusal observed in the *unresolved* state selects an arm, and
     every arm is itself idempotent under repetition — arms 1/3/5 clear an
     already-cleared record (a no-op, and the client is answered once because
     the reply is bound to the record's presence), arm 2 re-proposes a report
     the damping rule already has in flight (a no-op), and arm 4 is a no-op by
     definition. Repetition therefore cannot change the outcome; only a
     *different* record could be harmed, and the record-binding conjunct above
     is what excludes it.

     With those two checks first — record-binding, then stage-resolution — the
     partition over the six declared refusal classes is **total**:
     1. **`upstream-validity` / `shard-relationship`** (jointly the
        *refuse-whole* verdict — a standing condition retrying cannot fix)
        ∧ `role == Primary`: revert — clear the record (fsynced), discard the
        candidate, un-fence, resume serving as primary (in *this* arm both
        planes do agree and nothing was destroyed: the adoption requires
        replicated `Replica`, so it cannot have fired), answer the initiating
        client with the refusal's error. Two sub-cases differ only in what
        the node still owes the identity protocol (V14-M6):
        - **live-run refusal** — the pre-stage run is still live and its own
          boot report applied long ago, so the pre-stage triple remains the
          node's applied identity and **no new report is owed**;
        - **boot-window refusal** — the record's report *was* this boot's
          report (the node booted staged), so after clearing the record and
          un-fencing the node **re-proposes the same boot-minted triple as a
          `Boot` report** (the record is gone, so the kind rule stamps
          `Boot`); the boot-ordering rule keeps binding until that report
          applies, exactly as at any ordinary boot. No second mint: same
          triple, different kind stamp — which is what closes the
          vacuous-run-guard window a bare revert would have left open.
     2. **`fence` class** (`observed_role`/
        `observed_config_epoch` moved underneath the report while the staged
        premise still stands — ordinary, not exotic: TR-CLUSTER-027 fires
        `AddNode` epoch bumps live via `CONFIG SET
        cluster-replica-priority`): the record and fence **persist**; the
        reconcile re-proposes the record's report with fresh observations,
        which admits under the strict branch carrying the same candidate and
        the same `stage_id`;
        the client's reply stays deadline-bounded. Never a revert — a fence
        refusal is transient topology motion, not a verdict, and reverting
        would fail an admissible command spuriously with an error
        indistinguishable from refuse-whole.
     3. **`upstream-validity` / `shard-relationship` ∧ `role != Primary`** —
        the stint was ended by **someone else's
        admission** (e.g. `Failover{force: false}`) while the report was in
        flight: the staged intent is **superseded** — clear the record
        (fsynced), answer the initiating client with an error naming the
        supersession, and converge through the adoption machinery: the
        resolved-stage selector binds the destructive adoption to the
        record's own report, so this arm never wipes toward the record's
        upstream on someone else's admission (V13-C1's trace dies here);
        with the record cleared the disagreement is unexplained, and §0's
        role-authority adoption (lineage-guarded) adopts the replicated
        verdict. The node un-fences only per its adopted role's reply
        rules — never "resume serving as primary" on the strength of a
        stale local plane.
     4. **`ordering` class** — the refused entry is by construction strictly
        below the stored pair, i.e. a superseded proposal and never the live
        stage's verdict: **stale-duplicate no-op**, record and fence
        untouched (V14-M3 — revision 13's two-fact selector left this class
        in no arm at all).
     5. **`membership` class** (`node ∉ nodes`) — the node was removed while
        staged: the `run_identity` lifecycle bullet's **removal-of-self flow**
        governs (clear the record fsynced, un-fence, behave as a non-member
        awaiting `MEET`/reset). The refusal and the locally-observed removal
        **commute** — both terminate in the cleared state — and the role
        selector is never read on this class, which is why "the role at
        `self ∉ nodes`" needs no definition: class is selected first, and
        every class that *does* read the role implies the membership conjunct
        passed, so `nodes[self]` exists wherever the role is read (V14-M3).
     6. **`proposer` class** — structurally unsatisfiable for a record's own
        report (gate 3 mints `proposer = node`), so a `proposer`-class refusal
        never belongs to any pending stage and has no disposition here.
     The combination *refused ∧ adoption already fired* is unreachable, and now
     for a durable reason rather than an argument from payload identity
     (V14-M1 killed the latter: a boot re-proposal carries a *fresh*
     incarnation and takes the strict branch, so it is refusable): the
     adoption fires only from applied state carrying
     `admitted_stage == Some(record.stage_id)`, and from that state the
     reconcile proposes nothing for this stage (the reconcile guard below), so
     the only same-stage reports that can still be in flight are pre-crash
     duplicates — whose refusals the stage-resolution precedence rule makes
     no-ops before any arm is consulted. The **initiating client's deferred
     reply is deadline-bounded** (V12-M1, settled ruling 2 — every held client
     gets a real reply): if neither (c) nor (d) has fired within
     `cluster-staged-flip-reply-timeout` (node-local, client-reply bounding
     only — never a replicated admission input, so the no-wall-clock ruling is
     untouched), the client is answered `-TRYAGAIN` with a stated
     **ambiguous outcome**: the stage remains pending and resolves to adoption
     or revert when the report resolves — reverting on timeout is *unsafe*
     (the report may still admit, and a revert-then-admit re-opens the race
     this staging closes), so the timeout bounds the reply, never the stage; a
     retried `CLUSTER REPLICATE` is refused with the pending-stage error while
     the stage is unresolved (single-writer rule above) and re-evaluated by
     gate 3 after resolution, and `CLUSTER INFO`'s pending-stage field reads
     the outcome at any time.
     **Reconcile guard — the same one selector, in its proposing direction.**
     While the record is pending, §0's reconcile proposes **nothing but the
     staged report itself**, and proposes it **exactly when case 3 of (c)'s
     three-way rule holds**: `record present ∧ record.adopted == false ∧
     applied nodes[self].admitted_stage ≠ Some(record.stage_id)` — the
     *unresolved* branch, and nothing else (V15-C2: this is now the **same
     predicate** (c) reads, so the two rules cannot claim the same state; and
     V15-m3: the operand set is durable state only — revision 14's trailing
     "and no terminal outcome observed" is deleted, since a terminal arm
     clears the record fsynced before answering, making *record present* the
     durable statement that operand was groping for). In the
     post-admission/pre-adoption window the stage reads *resolved* and the
     reconcile proposes **nothing**, because completing the adoption is local
     machinery, not a proposal (V14-m3). The window is a *planned* plane disagreement
     (V12-C1: local Primary, replicated Replica) that no level rule may read as
     a promotion — and since V14-C1 there is no `Promotion` kind to stamp at
     all, so the hole is closed by the enum, not only by the guard. The staging
     is what makes the replicated
     refuse-whole veto *effective* rather than after-the-fact: the veto lands
     before the destruction it exists to prevent. **No reachable state destroys
     a copy that replicated state still counts** — the discard is gated on the
     admission of the record's *own* report (the resolved-stage selector),
     which
     removed every such count for exactly the transition the record names;
     an admission that demotes the node some *other* way leaves the
     binding false and routes through arm 3 — and the
     locally-replica/replicated-Primary-with-slots race state is unreachable
     through this path (post-admission the node is replicated-Replica, and
     SS-11's amended invariant refuses a Replica as an `AssignSlots` assignee;
     the state cannot re-open from the reconcile side either, because the
     report-kind enum has no `Promotion` member to stamp — V12-C1's
     trace is dead at the enum, a fortiori at the stamping rule). §3's demotion-disposition row keeps
     its self-fence passage as defence-in-depth for the plane-split state,
     which no spec path now reaches.

  With these gates, a member node only ever holds slot data in a *tracked* state —
  owner, replica of an owner, open-record source/target, residue-entry source, or
  **detached replica** (V15-C3, declared below at the role-authority passage: the
  state LOCKED FM-CLUSTER-002 creates when `CLUSTER FORGET` removes a primary and
  `reparent_children(.., None)` clears its replicas' parent pointers **without
  changing their role**) —
  and every tracked state has a declared remover (§1's enumeration: reaper,
  rollback arm, re-target rules, `ClearSlotResidue`; for the detached replica, the
  operator's re-home or `CLUSTER FORGET` of the replica itself). The untracked-stale-copy
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
| target_replicas_acked_pos | `ReportTargetReplicaAck` (target); **cleared** by a cross-run `ReportMigrationIngest` (V8-C3) **and by any apply that changes `counted_replicas(record.target)`** — a member added to or removed from the target's counted replica set stales the "every counted replica" claim in both directions (V10-M2; the set-changing writers: `AddNode` registration parenting onto the target, `RemoveNode`/`CLUSTER FORGET`, the `ReportRunIdentity` Demotion arm's re-parent, failover transitions, `SetRole`) | source-space replica-ack floor (§5, V4-C3); monotone within the target's attesting run |
| target_attesting_run | `ReportMigrationIngest` (target — set/replaced with each admitted report's `target_run`; V8-C3) | which target run attests the two positions above; `Complete` requires it current |
| attempts | `AbortSlotHandoff` (+1); reset by re-issued `MIGRATING` **at most once per record** (N-m4, §6; V6-M4) | any failed attempt counts |
| attempts_reset_used | `BeginSlotMigration` (false); re-issue arm (sets true when it resets `attempts`) | one-shot reset latch; V6-M4 |
| observations | `ObserveMigration` (+1); **reset by a `ReportMigrationIngest` that advances `target_ingested_pos` to a value still below a *set* `drained_pos`** (N-M6, narrowed per V4-M2/V5-M1 — no reset while `drained_pos` is unset); **reset by a `ReportTargetReplicaAck` that advances `target_replicas_acked_pos` to a value still below a *set* `drained_pos`** (V8-M2 — same toward-the-token rule); **reset by `ConfirmSlotHandoffDrained`'s apply** (V7-C3 — the seal switches the applicable bound from `preconfirm_observations` to the smaller `draining_observations`, a phase change in all but name; carrying pre-seal ticks across it would abort every lawful drain longer than the small bound at stock defaults); and by phase change | replicated; survives leader change |
| last_observation | `ObserveMigration`; **cleared by `ConfirmSlotHandoffDrained`'s apply** (with the counter — V7-C3) | dedup state for the counter (N-M5) |
| handoff_residue entry | `CompleteSlotMigration` (creates); `ReportSlotPromoted` (sets promoted); `ConfirmSlotDeleted` (removes); membership prune (sets `source_gone` on source-departure, sets `target_gone` on target-departure that unassigns the slot, re-targets on target failover — V5-M2/V7-C2/V7-M6); demotion transitions (re-target `source`/`target` to the admission-validated successor — V6-C3/V9-C1); `RetargetSlotResidue` (re-writes `source` to the shard's current primary — V8-C4; **source arm only** — the V9-M1 target arm is removed, V10-C4: target re-home is exclusively the demotion transitions' in-apply re-target); `AssignSlots` rollback arm (removes; clears `target_gone` when re-homing an orphaned `promoted == true` entry — V7-M5/M6); `ClearSlotResidue` (removes — V7; `promoted == true` only, V9-M4); `ResetCluster` (clears map) | replicated, snapshot-carried |

The three `NodeInfo` fields this design adds (V14, **re-based on `primary_id`
writes in V15**) have several declared writers each, so they are tabulated
separately — the "exactly one writing transition" rule above governs the migration
record. For these three the governing rule is stated **once**, here, and every
other passage that mentions them references it rather than re-listing writers:

> **Companion-field rule** (V15-C1/C2/C3, replacing revision 14's *"every writer
> of the governed role fact also writes the companion field"*): **every apply that
> writes `nodes[n].primary_id` — to any value, `Some` or `None` — also writes
> `nodes[n].synced`, `nodes[n].admitted_stage` and `nodes[n].promoted_from` for
> that same `n`, in the same apply.** The default write is
> `synced = false, admitted_stage = None, promoted_from = None`, with exactly
> **two carve-outs**:
>
> 1. **the minting site** — the `ReportRunIdentity` **Demotion arm** writes
>    `admitted_stage = Some(payload.stage_id)` (with the default
>    `synced = false`, `promoted_from = None`). It is the *only* writer that ever
>    stamps a stage; every other `primary_id` writer clears it.
> 2. **applies that force `role = Primary`** — both failover **promotion** arms,
>    LOCKED `SetRole` writing `role = Primary`, `ResetCluster` (SOFT and HARD,
>    which force this node's role to `Primary` and null its parent pointer), and
>    a fresh `AddNode`/`MEET` registration that registers the node **as a
>    primary** — write **`synced = true`** (a primary is trivially current with
>    itself; V15-m4 — revision 14 had `ResetCluster` write `false` here while the
>    promotion arms wrote `true`, two writers of one governed fact stamping the
>    companion differently) and `admitted_stage = None`. The failover promotion
>    arms additionally write `promoted_from = Some(old_primary_id)`; every other
>    member of this carve-out writes `promoted_from = None`.
>
> **Why `primary_id` and not `role`** — the round's structural correction
> (V15-C1/C2/C3). What `synced` and `admitted_stage` qualify is *who this node's
> parent is*: "does it hold **that parent's** history", "which adjudication gave
> it **that parent**". Revision 14 keyed both on writers of `role`, and **two
> LOCKED appliers write `primary_id` without writing `role`**:
> - `reparent_children(.., Some(new_primary))` in **both** failover arms — LOCKED
>   TR-CLUSTER-018 "siblings of `old_primary_id` re-parented to `new_primary_id`"
>   and TR-CLUSTER-042 "`old_primary_id`'s remaining replicas … are re-parented to
>   `new_primary_id` (`reparent_children`, `commands.rs:459`)": a sibling's *role*
>   is untouched;
> - `reparent_children(.., None)` in **`RemoveNode`** — LOCKED TR-CLUSTER-003 /
>   FM-CLUSTER-002 "Every replica parented to it is *detached*: its `primary_id`
>   clears, **its role does not change**" (`commands.rs:231`).
>
> Under the v14 rule a sibling re-parented onto a brand-new primary kept the
> `synced == true` it earned under its **old** parent and was instantly an
> auto-failover candidate holding none of the new shard's history — V15-C1's
> zero-history promotion, `inv_no_acked_write_lost` — and a detached replica kept
> a stage stamp naming an adjudication that no longer described it, which is half
> of V15-C2's stranded record. Keying the rule on `primary_id` makes the
> companions' writer set **exactly** the writer set of the fact they qualify.
> Two consequences worth stating because later passages turn on them:
> - **V13-C1's separation now falls out of the general rule** rather than being
>   an arm-specific stipulation: a `Failover{force: false}` demoting the staged
>   node writes that node's `primary_id` to the successor, hence
>   `admitted_stage = None`, hence the adoption's stage operand is false and
>   someone else's admission can never fire this record's destructive discard.
> - **`admitted_stage == Some(s)` implies the parent has not moved since `s` was
>   stamped**, because *every* `primary_id` write clears the stamp. This is what
>   collapses gate 3's adoption operands to a single selector (see (c)).
>
> **Two writers of a companion that are not `primary_id` writers**, both declared
> here so the rule's converse is never assumed: `AttestReplicaSynced` writes
> `synced = true` under the parent-fenced conjuncts declared at its Transitions
> row — it is the *only* transition that raises `synced` for a replica; and
> nothing else. In particular the `Boot` arm of `ReportRunIdentity` writes **no**
> companion field at all (V15-m1 — revision 14 had it clear `admitted_stage` as
> "garbage collection", a write that bought nothing and whose accompanying claim
> was false; see that arm).
>
> **Reading discipline** (V15-C1): `synced(n)` is consulted **only where
> `nodes[n].role == Replica`** — the failover-candidacy conjunct is its sole
> reader. Its value on a primary is stamped for stamp-consistency and never read.
> `admitted_stage(n)` is read only by `n` itself, against its own pending record.

| `NodeInfo` field | Written by | Notes |
|-------|-----------|-------|
| `promoted_from: Option<NodeId>` | **every `primary_id` writer** (companion-field rule): failover **promotion** arms write `Some(old_primary_id)` on the promotee; every other `primary_id` writer writes `None` — the `ReportRunIdentity` Demotion arm, LOCKED `SetRole` in either direction, the failover **demote-and-re-parent** of the old primary, the failover **sibling re-parent** (`reparent_children(.., Some(new_primary))`, TR-CLUSTER-018/042), `RemoveNode`'s **detach** (`reparent_children(.., None)`, TR-CLUSTER-003/FM-CLUSTER-002), `ResetCluster`, and a fresh `AddNode`/`MEET` registration. An `AddNode` **upsert** — a bare re-registration, which per LOCKED TR-CLUSTER-002 writes neither role nor parent pointer ("are **not** downgraded by a bare re-registration") — writes no `primary_id` and therefore **preserves** all three companions field-wise (V7-M3's rule, V15-M2) | V14-C2 — the replicated record of *which* primary a promotion adjudicated away from; sole operand of the lineage guard's third disjunct |
| `synced: bool` | the same writer set, by the same rule: default `false`; the `role = Primary` carve-out writes `true`; **plus** `AttestReplicaSynced`, the one non-`primary_id` writer, which writes `true` only under its declared parent fence (`observed_primary_id`/`observed_config_epoch`, V15-C4) | V14-C3 — data-possession gate on failover candidacy (TR-CLUSTER-021); read only where `role == Replica`; no wall clock anywhere |
| `admitted_stage: Option<u64>` | the same writer set, by the same rule: default `None`; the `ReportRunIdentity` **Demotion arm** is the sole minting site, writing `Some(payload.stage_id)`. No other transition writes it — in particular the `Boot` arm does not (V15-m1) | V14-M1 — names the adjudication that gave the node its **current** parent, so the staged adoption binds to *its own* admission across a boot; stage ids are per-node monotone and never reused, so a stamp left over from an earlier stage satisfies no later record's operand |

#### Writer join against the LOCKED state space (V15-F10, permanent)

Revision 14's three CRITICALs were one omission repeated across three
enumerations, and all three would have been caught by joining this design's
writer claims against `specs/cluster.md`'s **State space** rows instead of
against the design's own prose. That join is therefore a permanent subsection,
re-run every revision. For each replicated field this design reads or stamps,
the LOCKED `Writer(s)` cell is quoted **verbatim** and every writer in it is
marked *carried* (the design leaves it alone) or *amended* (with what the
amendment adds). **A LOCKED-named writer that appears in neither column is a
defect, not an omission.**

| Field (LOCKED row) | LOCKED `Writer(s)`, verbatim | Per-writer disposition |
|---|---|---|
| **Node role** (SS-2, `NodeInfo.role`) | "`apply_command`: `SetRole`, `Failover` (promote/demote), `ResetCluster` (forces this node's role to `Primary`, `commands.rs:834`)" | `SetRole` — **amended** (companion-field rule; to-`Primary` takes carve-out 2, to-`Replica` the default). `Failover` (promote/demote) — **amended**: the *promote* half takes carve-out 2 and writes `promoted_from = Some(old_primary_id)`; the *demote* half takes the default. `ResetCluster` — **amended**: carve-out 2, so `synced = true` (V15-m4 corrects v14). **Added by this design**: the `ReportRunIdentity` **Demotion arm** (carve-out 1) — the LOCKED row gains it, blast-radius verdict Amended. **Flagged LOCKED-internal disagreement**: SS-2's writer cell omits `AddNode`, while TR-CLUSTER-002's postcondition says "'Node role', 'Node parent pointer' … all set from the command's `node` fields" at a fresh registration. This design treats a fresh `AddNode`/`MEET` registration as a role writer (carve-out 2) and the blast-radius entry asks SS-2's cell to name it; the alternative reading — that registration is not a role write — would leave a registered node's role written by nobody. |
| **Node parent pointer** (SS-3, `NodeInfo.primary_id`) | "`apply_command`: `AddNode`, `SetRole`, `Failover` (re-parent), `RemoveNode` (re-parents the departing node's children via `reparent_children`, `commands.rs:231`), `ResetCluster` (nulls this node's own parent pointer, `commands.rs:835`)" | **All five carried, all five amended by the companion-field rule** — this is the row the rule is keyed on, so no member of it may be skipped. `AddNode` — fresh registration writes the companions (carve-out 2 when it registers a primary, default otherwise); a bare **upsert** writes no `primary_id` and preserves them (V15-M2, LOCKED TR-CLUSTER-027 fires exactly this path live). `SetRole` — default or carve-out 2 by direction. `Failover` (re-parent) — **three distinct writes**, each taking the rule separately: the promotee's parent **cleared** (carve-out 2), the old primary **re-parented** to the successor (default), and the old primary's **siblings re-parented** via `reparent_children(.., Some(new_primary))` (default — this is the write revision 14 missed, V15-C1). `RemoveNode` — `reparent_children(.., None)` **detaches** every child (default write: `synced = false`, `admitted_stage = None`, `promoted_from = None`), producing the **detached replica** tracked state declared below (V15-C3). `ResetCluster` — carve-out 2. **Added by this design**: the Demotion arm (carve-out 1). |
| **Node's own config epoch** (SS-4, `NodeInfo.config_epoch`) | "`apply_command`: `AddNode` (initial), `SetConfigEpoch`, `Failover` (stamp on promotion), `ResetCluster` (HARD only: reset to 0, `commands.rs:843`)" | **All four carried, none amended** — this design writes no epoch. It *reads* the field in three admission conjuncts (the Demotion arm's fence, `AttestReplicaSynced`'s new `observed_config_epoch` conjunct, and, via TR-CLUSTER-018/042, the failover fences), so each of these four writers can refuse an in-flight report or attestation: that is the fence working, and gate 3 (d) arm 2 plus the attestation's re-attest disposition are the declared responses. |
| **Slot ownership** (SS-11, `slot_assignment`) | "`apply_command`: `AssignSlots`, `RemoveSlots`, `Failover` (transfer), `CompleteSlotMigration` (move), `RemoveNode` (unassigns the departing node's slots, left unassigned rather than retargeted, `commands.rs:222-224`), `ResetCluster` (clears all, `commands.rs:821`)" | `AssignSlots` — **amended** (rollback arm; assignee-role conjunct; `accept_data_loss` token rules). `RemoveSlots`, `Failover` (transfer), `ResetCluster` — **carried**. `CompleteSlotMigration` — **amended** (rewritten wholesale by this design). `RemoveNode` — **carried**, and its unassign is what makes the detached replica's slots ownerless rather than re-homed (FM-CLUSTER-002's "NOT observable" is explicit that inventing a successor here is forbidden). **Added by this design**: the Demotion arm's in-apply re-home to the validated successor. Invariant added: no slot is assigned to a node whose role is `Replica`. |
| **Node membership** (SS-1, `nodes`) | "`apply_command`: `AddNode`, `RemoveNode`, `Failover` (remove/re-parent), `ResetCluster` (clears to just this node, `commands.rs:832-855`; if `node_id` is not a member — a reset racing a `FORGET` — the `else` branch at `commands.rs:855-858` clears membership to *empty* instead and still returns `Ok`)" | **All four carried, none amended.** The design *reads* membership in the `membership` and `upstream-validity` conjuncts and in the lineage guard's third disjunct; `RemoveNode` and `Failover{force: true}` are the two writers that can falsify an upstream mid-flight, and both route to declared arms (gate 3 (d) arms 3/5). |
| **`NodeInfo.run_identity`**, **`.synced`**, **`.admitted_stage`**, **`.promoted_from`** | *No LOCKED row* — all four are new replicated fields introduced by this design | Each lands as a **New row** in the blast-radius list with its own writer enumeration (the tables above), and each is bound by the companion-field rule or, for `run_identity`, by the `ReportRunIdentity` row. Nothing to carry; everything to declare. |

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
  report REPLACES** (V8-C3, retained per V9-C2 as belt-and-braces: under §0's
  durable-attestation rule the shadow is fsync-durable through every attested
  position, so a restarted target's re-report can no longer regress below its
  own last attested value and the regression case is unreachable in the
  modelled crash space; the replacement arm stays because a
  monotone-across-runs conjunct would still be a possession proof pinned to a
  dead run, and replacement keeps the field honest under faults outside the
  model — disk replacement, restore from backup). Writes `target_ingested_pos = applied_pos`,
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
  **Durable attestation** (V9-C2): the target proposes `applied_pos` only for
  positions its shadow store is **fsync-durable through, regardless of the
  target's configured `Durability`** — the attested position is a protocol
  commitment `Complete`'s admission consumes (§0's `covered_applied` rule), so
  a stale-high replicated value backed by nothing cannot exist, even while a
  crashed target is down and its attesting-run conjunct still matches the
  stored run.
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
  durably applied the shadow — where "durably applied" means **fsync-durable
  through that stamp on that replica, regardless of the replica's configured
  `Durability`** (V10-C2 — §0's durable-attestation rule, propagated to the
  floor it was minted for: a counted replica acks a batch to the target only
  once its store is fsynced through it, and the target folds only such acks
  into the floor it attests; §5's install contract states the replica-side
  half. Consequence, mirroring `covered_applied`'s: no replica crash can
  regress a replica below a position folded into an attested floor, so the
  replicated floor never sits above what every counted replica durably holds
  — the within-history regression `Complete` would otherwise inherit is
  unreachable). **The counted set is `counted_replicas(record.target)`** — the
  members whose replicated `primary_id` names the target, i.e. the target's
  replica set as recorded in replicated cluster state (the `NodeInfo`
  parent/replica relationships the cluster already tracks for failover
  scoring; a pure function of applied state, like `shard_primary`) — **never
  the target's node-local
  session table** (V5-M3: a set defined by live sessions shrinks on a single TCP
  disconnect, silently voiding the durability the knob exists to provide; a
  disconnected-but-member replica instead stalls the floor, and the migration exits
  via the observation bound, exactly the dead-replica behaviour §1 already names).
  The floor is the minimum over *that* set; the target knows each fed batch's
  source-space stamp and each counted replica's ack, so the floor is computable
  node-locally and *attested* by this replicated write. **Set-change staleness**
  (V10-M2): a stored floor attests a claim about **every member of the counted
  set as it stood when the floor was written**; any apply that changes
  `counted_replicas(record.target)` — a member removed *or* a newcomer added —
  stales that claim, so it **clears `target_replicas_acked_pos`** in the same
  apply (the field-writers table names the set-changing writers). **Empty-set
  and unset rules** (V5-M3/V5-m4, strengthened per V10-M2): `Complete`'s
  optional conjunct carries `counted_replicas(record.target) ≠ ∅` **directly**
  — revision 9 enforced the empty-set case only through the floor being
  `None`, which a floor written before the last counted replica left would
  have defeated; with the set-change clearing above the stale-floor state is
  unreachable anyway, and the direct conjunct makes the rule independent of
  that clearing — and while `target_replicas_acked_pos` is `None` the conjunct
  is **false** — never vacuously true — so the migration exits by the
  observation bound. The optional `Complete` conjunct reads only
  replicated operands — deterministic at apply on every node.
- **`PrepareSlotHandoff{slot, migration_id, proposer}`** (V6-m1 — the record the
  admission reads is named by the payload, not implied; source-proposed) → mints
  attempt_id, phase=Draining, barrier
  arms on the source (per-object, issue 17/19 semantics). Full admission conjunction
  (run/proposer guards per V4-C2): record exists ∧ migration_id matches ∧
  phase==Streaming
  ∧ **proposer is `record.source`** ∧ **`record.run_id ==
  nodes[record.source].run_identity`** ∧ attempts < `record.max_handoff_attempts`
  (the captured parameter, V6-C2) ∧ neither
  endpoint FAIL-flagged ∧ `record.attempt_id == None` (no live attempt — a
  **declared absence test**, §0's enumeration, V10-m6). The source *chooses* to propose at
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
  ∧ record.drained_pos == Some(token)                    // value read — false at None
                                                         // (§0 rule, stated per V10-m6)
  ∧ record.target_ingested_pos >= record.drained_pos     // covered_applied — possession
  ∧ record.target_attesting_run                          // the possession proof is
      == Some(nodes[record.target].run_identity)         // backed by the target's
                                                         // CURRENT run (V8-C3). A
                                                         // crashed, still-down
                                                         // target MATCHES its
                                                         // stored run — only its
                                                         // restart proposes a new
                                                         // one — so this conjunct
                                                         // alone is
                                                         // stale-satisfiable;
                                                         // safety rests on V9-C2's
                                                         // durable attestation
                                                         // (§0): the position is
                                                         // fsynced, so even a down
                                                         // target backs it on
                                                         // disk. This conjunct is
                                                         // belt-and-braces for the
                                                         // restart-that-reports
                                                         // path
  ∧ record.run_id == nodes[record.source].run_identity   // replicated field, §0 (N-C4)
  ∧ slot_map[slot] == record.source                      // source still owner
  ∧ record.target ∈ nodes                                // membership — FM-CLUSTER-033's
                                                         // ghost-owner guard (V4-m4)
  ∧ nodes[record.target].role == Primary                 // commit-time re-verification
                                                         // (V10-C3): Begin checked the
                                                         // role once, a mid-migration
                                                         // demotion of the target is
                                                         // lawful, and assigning the
                                                         // slot to a Replica would
                                                         // violate SS-11's amended rule
                                                         // and wedge the residue entry
                                                         // (ReportSlotPromoted's role
                                                         // conjunct refuses forever)
  ∧ record.target not FAIL-flagged                       // N-m6, symmetric with Prepare
  ∧ [if record.require_target_replica_ack]               // optional durability conjunct;
      counted_replicas(record.target) ≠ ∅                // direct empty-set refusal
                                                         // (V10-M2)
      ∧ record.target_replicas_acked_pos >= record.drained_pos
                                                         // replicated, source-space (V4-C3);
                                                         // the guard reads the captured
                                                         // record field, never the config
                                                         // knob (V6-C2)
  ```

  The role conjunct is the CockroachDB commit-time-re-verification discipline: a
  target demoted mid-`Draining` **stalls `Complete`** — the conjunction stays false
  until the target is re-promoted or the migration exits by the standing `Draining`
  exits (the observation bound aborts the attempt; `CancelSlotMigration` aborts the
  whole migration) — it never assigns a slot to a Replica. It also guarantees every
  `handoff_residue` entry is **created with a Primary target**, the base fact §0's
  V10-C4 in-apply-re-target totality argument stands on.

  On apply: ownership flips, `MOVED` correct, barrier release event emitted **after**
  the assignment mutation (FM-CLUSTER-092 ordering preserved), record removed, and the
  apply writes the replicated `handoff_residue` entry `{source, target,
  promoted: false, source_gone: false, target_gone: false}` (V4-C1/M7/M11;
  V11-m2 — the initializer names every field of §0's declared type, matching
  §0's "both flags false" prose) — the durable, snapshot-carried registration of the
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
- **`RetargetSlotResidue{slot, migration_id, new_source: NodeId, proposer}`**
  (primary-proposed; **new**, V8-C4 — §0's level-triggered residue re-home;
  **source arm only**: V9-M1's target arm is removed by V10-C4 — once a target
  demotion has applied, `shard_primary(entry.target)` is an unproven pointer
  walk: a demoted target later re-parented onto a *foreign* shard's primary is,
  in current state, indistinguishable from one parented onto its lineage
  successor, so the arm could hand the entry to a primary that never received
  the shadow, whose `ReportSlotPromoted` would then be admissible and the
  source's reaper would delete the last complete copy. No sound guard exists —
  the discriminating fact lives only in the demotion's own apply — so target
  re-home is **exclusively** the demotion transitions' in-apply re-target,
  which §0's V10-C4 argument proves total; V9-M1's stuck state is closed by
  construction, not by a level arm): re-writes the entry's `source` to
  `payload.new_source`, touching nothing else. Admission: entry exists ∧
  migration_id matches ∧ `nodes[entry.source].role !=
  Primary` ∧ `payload.new_source == shard_primary(entry.source)` (§0's total
  definition; false at `None`, so a successor-less shard refuses — `ClearSlotResidue`
  is that state's exit) ∧ `nodes[new_source].role == Primary` ∧ `proposer ==
  new_source` (the proposer re-homes the entry to *itself*: it holds the copy via
  replication and its reaper takes over). **Why this walk is proven where the
  target arm's was not** (V10-C4): `entry.source` is residue-named, so every
  role→Replica write on it binds the refuse-whole conjunct (V9-C1) — it must
  carry an admission-validated in-shard successor and it re-parents the node
  onto that successor in the same apply — and the V10-C1 local gate refuses a
  `CLUSTER REPLICATE` on it outright (the only node-originated demotion
  spelling in cluster mode — V14-C1), so a foreign re-parent
  of a residue-named source is refused on both planes.
  `shard_primary(entry.source)` therefore walks only validated-successor edges
  and can land only on the lineage successor, which holds the entry's copy via
  replication.
  Proposal side is the level rule of §0:
  every primary's reconcile scans applied residue entries and proposes for each
  one whose `source` is a non-primary in its own shard. Idempotent by admission
  (once re-written, `entry.source` is a Primary and the arm refuses); a race
  between two successors across a failover resolves by the `shard_primary`
  conjunct — only the current successor's proposal is admissible at apply.
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
  entry exists ∧ `accept_stale_copy == true` ∧ **`entry.promoted == true`**
  (V9-M4 — the verb exists to retire the *source's* stale copy after the target
  owns and serves the slot, which is only meaningful once the promotion is
  attested. At `promoted == false` the declared exit is the `AssignSlots`
  rollback arm, whose apply moves the slot map and whose token names the loss
  honestly; without this conjunct the verb would delete the entry
  `ReportSlotPromoted` requires — the slot `-TRYAGAIN` forever on the target
  and its replicas — and would make the still-live shadow satisfy §5's discard
  predicate, deleting the last copy once the source is gone) ∧
  **`entry.target_gone == false`** (V10-m8 — at `promoted == true ∧
  target_gone == true` the source's retained copy is the last in-cluster copy
  of an owner-less slot, and §1 case 4b's sole exit is the `AssignSlots`
  orphan re-home arm, whose `accept_data_loss` token names what is actually
  abandoned; without this conjunct, the `source_gone` disjunct below would
  admit removal of that last copy's tracking entry under `accept_stale_copy` —
  a token attesting a *different*, milder loss) ∧ **no effective
  automatic remover exists**, carried as the conjuncts that exclude each
  remover in §1's enumeration (V9-M4 re-words the guard as its checkable form):
  the case-1 reaper and the case-2 re-targets are excluded by
  `entry.source_gone == true`, or by (`nodes[entry.source].role != Primary` ∧
  `shard_primary(entry.source) == None` — a **declared absence test**, §0's
  V8-M6 carve-out: the conjunct *is* the `None` check, evaluated as written, not
  false-at-`None` under the value-read rule); the case-3 rollback arm is
  excluded by the `promoted == true` conjunct above; case 4b is excluded by
  the `target_gone == false` conjunct above. Every conjunct here is
  re-derivable
  from replicated state alone, so "effective" means what it says — no arm whose
  admissibility depends on unobservable node-local facts counts as a remover,
  the V7-C1 proxy-arm lesson. The operator attests that the stale copy
  the entry gated has been dealt with out of band (node decommissioned, disk wiped,
  copy manually deleted); the join-empty admission gate (§0, V8-C1) is the backstop
  if the departed node ever attempts to rejoin still holding the copy. Refused
  without the token, and refused while a lawful remover exists — a property the
  admission now carries by construction rather than assertion.
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
- **`ReportRunIdentity{node, run_id, kind: Boot | Demotion,
  new_primary_id: Option<NodeId>, stage_id: Option<u64>, observed_role: Role,
  observed_config_epoch:
  ConfigEpoch, proposer}`** (the payload's `run_id` is the full
  triple —
  `identity_seq` rides inside it, §0/V5-C2; `kind` names **the node-originated
  transition the report asks replicated state to record, discriminated by the
  node's durable pending-transition record — never by a bare plane
  comparison** (V12-C1, revising V11-M1's disagreement-direction rule, which
  itself revised V7-C1's minting-moment rule: the disagreement rule made the
  reconcile a second, *opposite-direction* role authority — during gate 3's
  staging window the local-Primary/replicated-Replica disagreement is
  *planned*, and a rule reading it as a promotion would ask replicated state to
  revert the demotion the node itself just landed, re-opening the
  Replica-assignee door SS-11 closed and, on a crash before adoption,
  minting a Primary holding an untracked stale copy): `Demotion` is
  stamped **iff** the node's durable pending-transition
  record (gate 3's staged-flip record) is present; `Boot` in every other
  case — an ordinary boot, and every reconcile of an *unexplained* role
  disagreement, which the node first converges **locally** by adopting the
  replicated role (the role-authority rule at the reconcile paragraph below —
  replicated plane authoritative, TR-CLUSTER-033's direction) so the Boot
  report proposes with planes already agreeing. **There is no `Promotion`
  kind** (V14-C1/V14-M5 — see gate 3's cluster-mode role-command surface):
  a failover-driven role change
  is deliberately **not** a `Demotion` report: the failover
  transition's own apply wrote the role, so the node's subsequent
  history-adoption report needs exactly the Boot arm's
  write-only-the-identity behavior (the V10-m4 `ResetCluster` precedent —
  a further identity *moment*, not a further *arm*), and its identity **field
  write** still cancels the migrations the node sources (§4's rule binds to
  the field write, arm-independent) — and since a failover is the *only*
  promotion path in cluster mode, promotion has no arm to be stamped for.
  `new_primary_id` is `Some(record.target_upstream)` on
  every `Demotion` report and `None` on every `Boot` report (V14-C1 — the
  "member, else `None`" clause is deleted: the upstream is the
  `CLUSTER REPLICATE` argument, always a NodeId, and a `None`-carrying
  `Demotion` payload is refused as malformed below);
  `stage_id` is `Some(record.stage_id)` on every `Demotion` report and `None`
  on every `Boot` report (V14-M1); V7-C1's reason for stamping the kind is
  unchanged — the
  proposing moments are indistinguishable in `(node, run_id)` alone, per
  FM-REPLICATION-022 both a boot and a demotion bump
  `identity_seq`, so an arm selected by anything but a committed payload field is
  unselectable at apply; `observed_role`/`observed_config_epoch` snapshot
  `nodes[node].role` and `nodes[node].config_epoch` from the *proposer's applied
  state at proposal* — the V8-C2 per-object fence fields, below) (proposed by each
  node at boot
  and at demotion/history adoption — §0, V4-M4) → writes
  `NodeInfo.run_identity`. Admission conjuncts, **in this declared evaluation
  order** (V14-M3 — the order is normative because the refusal's class is the
  *first* failing conjunct): **(1 proposer)** `payload.proposer ==
  payload.node` (V6-C1) ∧ **(2 membership)** `node ∈ nodes` (V6-M3) ∧
  **(3 ordering)** `(incarnation,
  identity_seq) ≥` stored pair (§0's **split ordering conjunct**, V10-M1: strict
  `>` gates the `run_identity` **field write**; **equality** admits the report as
  a topology-only re-proposal whose arm writes land under the per-object fences
  with the payload's fresh observations, the field write skipped; **true at
  absent stored value** — the first
  report is always admitted, V6-M3) ∧, on the `Demotion` arm only,
  **(4 fence)**, **(5 upstream validity)** and **(6 shard relationship)** as
  declared at the arm below.

  **`RefusalClass`, a declared committed outcome** (V14-M3 — revision 13's
  gate-3 partition selected an arm on "the refusal class the Demotion arm
  already distinguishes", which was prose, not a fact any node could read):
  when this transition refuses, its apply records the committed outcome
  **`refused(class)`** with
  `class ∈ {proposer, membership, ordering, fence, upstream-validity,
  shard-relationship}` — **the first conjunct that failed in the evaluation
  order above**. The class is a pure function of the committed payload and
  pre-apply replicated state, so every applier computes the same value
  (FM-CLUSTER-089 determinism preserved, no node-local operand).
  **Read path, declared** (V15-m2): the proposing node obtains the class by
  **computing it locally at its own apply of the committed entry** — every node
  applies every entry, `payload.proposer` is a committed field, so the proposer
  recognises its own report and evaluates the same pure function every other
  applier does. It does **not** read the class out of a `ClusterResponse`: a
  staged node is usually **not** the Raft leader, and LOCKED TR-CLUSTER-041
  answers a forwarded proposal with `Proposed::Forwarded` and **no**
  `ClusterResponse` at all. FM-CLUSTER-047's committed-rejection rule (a
  rejection rides back through the success channel) is the *leader-side*
  companion and stays exactly as LOCKED — the amendment this design asks of it
  is only that the refusal's **class** be part of the recorded outcome, so that
  the local computation and the leader-side reply agree by construction. Both
  rows are carried in the blast-radius list. Gate 3 (d)'s
  disposition partition is keyed on this outcome; a `Demotion` payload with
  `new_primary_id == None` is malformed and refuses in the
  `upstream-validity` class. Applying a run_identity **field write** for a node
  that is
  the **source** of any open migration **cancels those migrations** — the replicated
  form of "source restart aborts" (§4). The cancel binds to the **field write**,
  never to mere admission: a topology-only re-proposal at equality changes no
  identity and cancels nothing (V10-M1). The **`kind == Demotion` arm** is a
  **fenced role writer** (V8-C2 — identity monotonicity is not topology recency: a
  demotion report proposed before a `Failover` promotion but applied after it would
  otherwise clobber the promotion, exactly the stale-writer class issue 19's
  per-object fence rule and TR-CLUSTER-018/042's epoch fences exist for). Its two
  additional apply-time conjuncts, both refusing on mismatch: **(fence)**
  `nodes[node].role == payload.observed_role` ∧ `nodes[node].config_epoch ==
  payload.observed_config_epoch` (V9-m1 — the declared field TR-CLUSTER-018/042's
  per-object fences already use, not a prose phrase) — the report only lands on
  the topology it was
  minted against; **(upstream validity, V8-C2, tightened per V14-C1)**
  `payload.new_primary_id == Some(u)` — a `Demotion` carrying `None` is
  **malformed and refused**, since the only producer stamps the record's
  `target_upstream` (this is what makes the conjunct non-vacuous: revision 13
  let `None` satisfy the "when `Some`" qualifier trivially, so a demotion toward
  a non-member admitted and left an adoption condition no state could satisfy)
  — with `u` a current member whose `role == Primary`, validated *at apply*,
  not merely at proposal (TR-CLUSTER-001's issue-14 ruling: payload references are
  re-checked against applied state); **(shard relationship, V9-C1)** whenever the
  node **owns slots or is named by any residue entry**, admission additionally
  requires `payload.new_primary_id == Some(p)` with `nodes[p].primary_id ==
  Some(node)`, evaluated against **pre-apply** state — `p` must be a *current
  in-shard replica of the demoting node*, the successor about to inherit its
  data. Without this conjunct a mis-targeted
  `CLUSTER REPLICATE <foreign-primary>` at a
  slot-owning node admits: the arm writes `primary_id` first, a post-write
  `shard_primary(node)` walk lands on the foreign primary, and every owned slot
  is re-assigned, token-free, to a node holding none of its data — which the
  rollback arm then reads as the "lossless" assignee — acked-write loss
  cluster-wide with no `accept_data_loss` anywhere. Note the conjunction is
  deliberately hard to satisfy: upstream validity wants `nodes[p].role ==
  Primary` while the shard conjunct wants `nodes[p].primary_id == Some(node)`,
  a pair that co-holds only in the narrow interval before `p`'s own promotion
  writes clear its upstream pointer — so in practice **a `CLUSTER REPLICATE`
  demotion of a slot-owning primary refuses whole**, and that is the intended
  fail-closed outcome, not a defect (the lawful paths are below).
  **Reachability under gate 3** (V10-C1, staging per V11-C2): the node-local
  replication-command gate (§0) refuses the `CLUSTER
  REPLICATE` up front when applied state binds; a command that passes executes
  **staged**, so this arm's veto lands *before* the local history adoption it
  exists to prevent — refuse-whole here is the effective line for whatever the
  gate's applied-state read missed, no longer an after-the-fact recording
  refusal. When admitted,
  the arm
  writes **`nodes[node].role = Replica`, `nodes[node].primary_id =
  payload.new_primary_id`, `nodes[node].admitted_stage =
  payload.stage_id` (V14-M1 — the stamp gate 3 (c)'s adoption binds to;
  `payload.stage_id` is `Some` by construction on this arm),
  `nodes[node].synced = false` (V14-C3 — the node has just been given a new
  parent and holds none of that parent's history yet, so it is not a failover
  candidate until it attests) and `nodes[node].promoted_from = None`
  (V14-C2 — the promotion adjudication is cleared by every `primary_id`
  writer)** — this arm is **carve-out 1 of §0's companion-field rule**, the
  sole minting site of `admitted_stage`, and the only place any of the three
  companion fields is written to anything but its default — (V7-C1 — without the role write, a node-originated
  demotion the failover machinery never saw leaves `nodes[node].role` at Primary
  forever: §7's defer guard never defers, `RetargetSlotResidue`'s
  `role != Primary` gate never opens, and the re-target rule never fires — a
  `promoted == true` entry becomes immortal and V5-C1's conjuncts freeze the slot's
  topology surface permanently; SS-2/SS-3's writer lists gain this transition — a
  LOCKED amendment with its own Rewritten verdict), **re-homes every slot the node
  owns to the validated successor `p` in the same apply** (V8-C6/V9-C1 — the
  destination is the admission-proven `p`, never a `shard_primary` walk through
  the pointer this same apply just wrote; a role write without
  the slot re-home leaves a Replica owning slots: it answers slot lookups `MOVED`
  to itself and every client loops forever while every health read shows a live
  primary-less-but-assigned slot; the re-home and the role write are one atomic
  apply, so no applied state ever shows a Replica owning a slot — **invariant: no
  slot is assigned to a node whose role is Replica**, SS-11 amendment + forcing
  test), and then
  re-targets residue entries naming the node to `p` (§0's residue-lifecycle rule,
  V6-C3/V9-C1). **Where the shard-relationship conjunct cannot hold — the node
  owns slots or residue and `new_primary_id` names a non-member or a node that
  is not a current in-shard replica — the arm refuses whole** (deleting or
  mis-homing the
  assignment would orphan or lose the slots). **Refuse-whole is a verdict, not
  a deferral** (V14-M2 — revision 13 said "deferred, not lost … the re-proposal
  rule retries as facts change", which contradicted gate 3 (d)'s partition and
  described an automatic retry no rule performs): replicated state keeps the
  node Primary; the node-local disposition is gate 3 (d) arm 1 (revert, clear
  the record, un-fence, resume serving); the initiating client is answered the
  refusal's error; and the **operator** re-issues after changing the facts. The
  refusing conjuncts are standing conditions — a slot the node still owns, an
  upstream outside its shard — that no amount of retrying alters, which is
  exactly why the reconcile does not retry them. **Node-local disposition of a
  refused Demotion report** (V11-C2, revising V10-C1's self-fence reading — a
  refusal here now reaches a node that has **staged, not executed**, its flip:
  gate 3's staging rule sequences the destructive upstream adoption *after*
  this arm's admission, so at refusal the dataset is intact and both planes
  still call the node the serving primary): **the disposition is gate 3 (d)'s
  refusal partition, keyed on the declared `RefusalClass` and — where the class
  reads it — the applied role** (V14-M2/V14-M3, replacing revision 13's
  unqualified "the node reverts", which was true of one arm out of six).
  In one line each: `upstream-validity`/`shard-relationship` at a still-Primary
  node → revert (record cleared, candidate discarded, un-fence, resume serving,
  client answered the refusal's error — nothing was lost and nothing needs
  re-aligning); the same classes at a no-longer-Primary node → supersession
  (record cleared, client answered the supersession error, convergence via
  role-authority adoption); `fence` → record and fence **persist**, re-propose
  with fresh observations; `ordering` → stale-duplicate no-op; `membership` →
  the removal-of-self flow; `proposer` → unreachable for a record's own report.
  And, before any of them, gate 3 (d)'s stage-resolution precedence rule: a
  refusal arriving after `admitted_stage == Some(record.stage_id)` is a no-op.
  Should a plane-split state
  (locally replica, replicated-Primary with slots) nonetheless present — a bug,
  not a reachable spec state — the node self-fences and answers `-TRYAGAIN`
  (§3's demotion-disposition row retains that passage as defence-in-depth;
  the role-authority rule's lineage guard is what routes this state here
  instead of adopting Primary over an unverified dataset — V12-C1/M2),
  because serving there would hand clients a `MOVED` destination that answers
  `-READONLY`, or stale reads from a locally-abandoned dataset. The refusal
  itself remains a deliberate, stated **availability loss over silent
  data loss**. The lawful paths to demote a slot-owning primary are the failover
  transitions, which carry succession atomically (TR-CLUSTER-017/018), or
  `AssignSlots` first: an operator intending a cross-lineage re-parent issues the
  token-gated `AssignSlots{accept_data_loss: true}` for the owned slots before
  the `CLUSTER REPLICATE`, after which the demotion re-homes nothing and the
  shard conjunct no longer binds.
  The `Boot` arm writes no role and no `primary_id`, and therefore — by §0's
  companion-field rule, which is keyed on `primary_id` writes — **writes none
  of the three companion fields**. Revision 14 had it clear `admitted_stage`
  as "the stamp's garbage collection: a stage stamp outlives its record by at
  most one report"; **that write is deleted and the claim withdrawn**
  (V15-m1). The claim was false: gate 3 (d) arm 1's **live-run** sub-case
  explicitly owes no further report, so a stamp from an earlier admitted
  demotion can outlive its record indefinitely — and nothing was buying the
  write, because **stage ids are per-node monotone and never reused**, so a
  leftover stamp `Some(s)` satisfies no later record's operand
  (`s ≠ record.stage_id` for every subsequently minted record) and, by the
  reconcile guard, reads as *unresolved* — the safe direction, in which the
  node proposes rather than adopts. What the stamp means is therefore stated
  positively and without a lifetime claim: **`admitted_stage(n)` names the
  adjudication that gave `n` its current parent**, and is cleared by every
  writer of that parent. The `Boot` arm
  carries the fence fields inert (admission
  ignores them on that arm). **There is no `Promotion` arm** (V14-C1, deleting
  revision 13's fenced Promotion writer and the pending-promotion record that
  discriminated it): V9-M3's worry was a promotion the failover machinery never
  saw leaving `nodes[node].role` at `Replica` forever, and its **only** minting
  moment was `REPLICAOF NO ONE` — which cluster mode refuses at dispatch (gate
  3's role-command surface). Every cluster-mode promotion is a `Failover` apply,
  which writes the role itself, so the report kind enum is `{Boot, Demotion}`
  and SS-2/SS-3's writer lists name exactly the Demotion arm plus **the LOCKED
  cells' own writers, in full** — `SetRole`, both failover halves *including
  the sibling re-parent*, `RemoveNode`'s detach, `ResetCluster`, `AddNode`
  (V15-C1: revision 14 wrote "the LOCKED failover and `SetRole` writers" here
  and in two other places, a prose list short by two) — and the Quint
  role-writer property (`inv_role_written_only_by_declared_writers`,
  **re-based on `primaryId` in ext-17**) is tightened accordingly, not
  weakened.
  **Level-triggered re-proposal** (V8-C2/V8-M3 — a refused report must not strand
  the truth): each node runs a standing reconcile — whenever its current local
  identity/topology facts (`run_identity` triple, role, upstream) differ from its
  own replicated `NodeInfo` in applied state, it re-proposes `ReportRunIdentity`
  with **fresh** observations (current `observed_role`/`observed_config_epoch`,
  current `new_primary_id`). Its operands are declared (V12-m2, **scoped to the
  pre-stage run** per V14-M6): the local
  triple compared is the **effective** one, and which triple that is depends on
  which side of a reboot the node stands:
  - **While the pre-stage run is live** — the node staged and has not
    restarted — a staged *candidate* triple
    (pending-transition record, gate 3) is **not** a current local identity fact
    and never enters the difference test: the node still serves under its
    pre-stage triple, which is the difference test's local operand, so staging
    alone can neither trip the
    reconcile nor stamp a `Boot` report whose field write would cancel the
    node's sourced migrations on a non-event.
  - **After a reboot with the record still pending**, §0's per-boot mint and the
    record rewrite produce exactly **one** triple: the re-minted candidate *is*
    the boot triple, *is* the node's effective local identity, and *is* the
    difference test's operand (V13-M3's re-derivation). Both statements are
    true, each on its own side of the reboot — which is what gate 3 (d) arm 1's
    boot-window sub-case turns on.

  **While a pending-transition
  record exists, the reconcile's only proposal is that record's own report,
  and it proposes only while the stage is unresolved** — the single rule
  stated at gate 3 (c), read here in its proposing direction: *record present
  ∧ `record.adopted == false` ∧ applied
  `nodes[self].admitted_stage ≠ Some(record.stage_id)`*, and **nothing else**
  (V15-m3 deletes the "and no terminal outcome observed" qualifier this
  passage used to carry: it was a node-local *volatile* predicate with no
  declared type or durable home, and it is unnecessary — every terminal arm
  clears the record, fsynced, before answering, so a terminal outcome is
  already the absence of a record)
  (V12-C1 — the guard the difference test was missing: the staging window's
  role disagreement is planned, and the pending report already in flight under
  the damping rule *is* its convergence — plus V14-m3's qualifier: in the
  post-admission/pre-adoption window the stage reads **resolved** and the
  reconcile proposes **nothing**, because completing the adoption is local
  machinery, not a proposal). **Role authority, stated** (V12-C1 —
  settled ruling 4's question "which plane is authoritative" answered for
  `role`/`primary_id`): **the replicated plane is authoritative; the local
  plane originates role transitions only through the pending-record
  protocol.** An *unexplained* role disagreement — no pending record — is
  therefore converged **replicated→local**: the node adopts the replicated
  role and upstream locally (this is `specs/cluster.md` TR-CLUSTER-033's
  level-triggered `SelfRoleReconciler`, FM-CLUSTER-046 — adopted here as the
  design's convergence direction, cited in the blast-radius list; the two
  reconcilers never contend, because the local→replicated direction fires
  only on a durable pending record and the replicated→local direction only in
  its absence). The adoption carries one **lineage guard**: adopting
  `Primary` requires the node's local replication lineage to be verified as
  the shard's — its local upstream `u` satisfies
  **`u == None ∨ in_shard_parent(u, self) ∨ (u ∉ nodes ∧
  nodes[self].promoted_from == Some(u))`** (the one-hop
  parent relation declared in §0 — V13-M2 — never a `shard_primary` walk).
  The third disjunct is V13-M1's mute-node fix made **specific** (V14-C2 —
  revision 13's bare `u ∉ nodes` was a false generalization: it read *any*
  absent upstream as adjudicated, so a node that `SetRole` had lawfully
  re-parented onto a foreign primary `P`, with `P` later `FORGET`-ten and the
  node then promoted by a failover, passed the guard and began serving the
  shard's slots out of a **foreign-lineage dataset** — cross-shard data
  substitution with no token anywhere). The specific form reads the replicated
  **`promoted_from: Option<NodeId>`** field, which the failover promotion arms
  write as `Some(old_primary_id)` and every writer of `role = Replica` clears
  to `None`: the promotee of a forced failover was promoted **from** the
  removed primary, and that adjudication is recorded in replicated state, so
  the disjunct is satisfied exactly when the absent upstream is the one the
  promotion adjudicated away from. `Failover{force: true}`
  removes the old primary from membership **outright** (LOCKED
  TR-CLUSTER-042, cited in the blast-radius list), and `CLUSTER FORGET` of a
  dead demoted primary reaches the same shape;
  without the disjunct, a node promoted by a forced failover that
  crashes before its local promotion takes effect boots with a
  guard-failing upstream and re-creates V12-M2's mute node — the whole
  shard's slots fenced unserved pending operator action. V14-C2's
  foreign-lineage trace now fails the disjunct
  (`promoted_from == Some(P) ≠ Some(F)`) and routes to the fail-closed branch
  below: availability loss, never data substitution. A node locally
  replicating a **foreign live member** upstream, or an absent upstream its
  own promotion did not adjudicate away from (all three disjuncts
  false), while replicated state calls it a Primary with slots — the
  plane-split bug state, unreachable through any spec path —
  does **not** adopt (its local dataset's lineage is unverified; serving it
  would hand out stale or wiped reads): it self-fences per §3's
  defence-in-depth passage and surfaces the operator error. Restated as the
  universal it now supports: in every state
  reachable through the design's own transitions **plus the LOCKED
  failover/removal rows** (TR-CLUSTER-018, TR-CLUSTER-042, `FORGET`), a node
  that adopts `Primary` does so over a dataset whose lineage some committed
  adjudication — an in-shard parent edge, or its own promotion's
  `promoted_from` stamp — names as this shard's; the
  guard passes on exactly those states, the node adopts, then reports `Boot`
  with the planes already agreeing — a
  history adoption on the demotion direction bumps the triple, and the Boot
  arm lands the field write without touching the role the adoption already
  matched. This is what closes V12-M2's mute-boot trace: a node the failover
  machinery promoted that crashed before its local promotion took effect
  boots local-Replica/replicated-Primary with no record, adopts Primary
  locally, and its Boot report admits without the Demotion arm's
  upstream-validity conjunct ever evaluating — no refusal loop, no
  permanently mute node. The adoption is **total over the replicated shapes**
  (V13-M4, re-grounded per V14-M4): at `role == Replica ∧ primary_id ==
  Some(p)` the node adopts replica-of-`p`. The remaining shape
  **`role == Replica ∧ primary_id == None`** is **reachable, and it is a
  declared tracked state: the detached replica** (V15-C3 — revision 14
  called this shape "unreachable by writer enumeration", which is **false
  against LOCKED FM-CLUSTER-002**: the detach is deliberate, specified, and
  force-tested by `remove_node_prunes_migrations_and_detaches_replicas`).
  Its producer is LOCKED `RemoveNode`'s `reparent_children(.., None)`
  (TR-CLUSTER-003), whose FM-CLUSTER-002 *Observable* row reads: "Every
  replica parented to it is *detached*: its `primary_id` clears, its role
  does not change, and `CLUSTER NODES` renders `-` for its master id" — and
  whose *NOT observable* half is equally binding: the orphans are **not**
  re-parented and **not** promoted, so nothing may invent a successor for
  them. (`ResetCluster` also nulls a parent pointer, but forces
  `role = Primary` in the same apply, so it never lands this shape; the
  `role = Replica` writers other than the detach — the Demotion arm, LOCKED
  `SetRole`-to-replica, the failover demote-and-re-parent arm — do each
  write a `Some` upstream in the same apply, which is why
  `reparent_children(.., None)` is the *only* producer.)
  **The adoption over it is replica-of-nobody**, which is what keeps the
  role-authority rule total: the node keeps `Replica`, keeps its dataset,
  has no upstream to replicate from, and **serves per its role's existing
  read rules — no new fence, no new refusal**, which is exactly
  FM-CLUSTER-002's minimal text. Three properties follow from rows already
  LOCKED or amended here: it is in **no failover candidate set** (it is
  nobody's replica, so no verdict enumerates it — and the amended
  TR-CLUSTER-021 would refuse it regardless, its `synced` having been
  cleared by the detach's `primary_id` write per §0's companion-field rule);
  **`AssignSlots` refuses it** under the amended SS-11 (a `Replica` is never
  an assignee); and it holds a slot-worth of data, so it is **its own arm of
  §0's tracked-state enumeration** (owner, replica of an owner, open-record
  source/target, residue-entry source, **detached replica**), which is what
  keeps `inv_member_keyspace_is_tracked` total over it rather than excusing
  it.
  **Declared exits, non-destructive.** The operator **re-homes** it, and the
  declared spelling is the **LOCKED `SetRole` re-parent** (TR-CLUSTER-004):
  its precondition already validates that the named `p` is present with
  `role == Primary`, its postcondition writes the new parent pointer, and it
  fires `Demoted` even on an already-`Replica` node precisely so the
  replication stream re-points — one lawful replicated write, which clears
  `admitted_stage`/`synced`/`promoted_from` with it per §0's companion-field
  rule (**decision, V15-F3**). This design's **staged** gate-3 machinery is
  *not* required for it: staging exists for a slotless **Primary** flipping
  to Replica, where an `AssignSlots` can bind the node between the gate's
  applied-state read and the flip; a node already replicated-`Replica`
  cannot be assigned slots at all under the amended SS-11, so that race has
  no producer here.
  `CLUSTER REPLICATE <new-primary>` issued **at** a detached replica is
  nonetheless **admissible, not refused** — it is the operator-facing
  spelling of the same re-home, and it converges to the same state through a
  **degenerate stage**: gate 3 binds only on owned slots or a residue entry
  naming the node, and a detached replica owns no slots (SS-11), so the gate
  passes; the Demotion arm's **shard-relationship conjunct is vacuous** (it
  is qualified on owning slots or residue), leaving **upstream-validity** as
  the only live check; the arm admits and writes the same
  `(role, primary_id)` pair the `SetRole` path writes, plus the stage stamp.
  The stage's whole-node `-TRYAGAIN` fence applies for its duration and
  costs nothing that was being served as an owner. Both paths land the same
  state, so no reader has to reconcile two outcomes. (Where a residue entry
  *does* still name the node, gate 3 refuses the `CLUSTER REPLICATE` by
  rule and the `SetRole` path is the exit.)
  **Declared exit, destructive:** `CLUSTER FORGET` of the node itself
  remains the removal path, and is what removes its keyspace from the
  tracked enumeration. What is **not** an exit is an automatic one: nothing
  in this design re-parents or promotes a detached replica on its own,
  because FM-CLUSTER-002 says the system does not, and adding one here would
  amend a LOCKED row with no failure-mode row asking for it.
  (Revision 13 gave this shape a **quiesced-replica arm of the report** — a
  proposal path — and revision 14 deleted the arm *and* the shape. The arm's
  deletion stands, for a sharper reason than revision 14 gave: no
  *node-originated* transition lands here, the detach being authored by
  another node's `RemoveNode`, so the shape needs a declared **state**, not
  a report arm. The unreachability claim that replaced them is withdrawn.
  The two "exits that cannot fire" revision 14 charged against the quiesced
  arm — `AssignSlots` and a failover verdict — were correctly identified as
  non-exits, and are indeed *not* among the exits declared above.) A detached replica does **not** loop
  proposals, because the **difference test treats a locally detached link as
  matching replicated `primary_id == None`** (retained from V13-M4 — without
  it the upstream operand stays unequal forever and the damped reconcile
  re-proposes a no-op report every observation cycle, a permanent proposal
  loop that falsifies `witnessSplitPlanesConverge`); with the operand equal
  the reconcile has nothing to report and the node sits in its declared
  state until an operator exit above fires. The **self-fence per §3's
  defence-in-depth passage is scoped to genuinely-unreachable bug states** —
  the plane-split shape (locally a replica, replicated-Primary **with
  slots**) — and explicitly **not** to a detached replica, which is a lawful
  state of the LOCKED system and serves per its role's read rules (V15-C3
  corrects revision 14, which fenced this shape as a bug state). The
  retained dataset does not weaken `inv_member_keyspace_is_tracked`: the
  **detached-replica arm** of the tracked-state enumeration is what
  discharges it, and the model carries that arm rather than an
  unreachability invariant (Quint ext-16). A report refused by the fence — the topology moved
  underneath it — is retried against the topology that actually holds,
  and converges or is superseded by a newer identity change (for a staged
  Demotion this is gate 3 (d)'s fence arm: the record and fence persist and
  the re-proposal carries the record's own candidate; supersession — the
  node no longer replicated-Primary — is (d)'s third arm, which clears the
  record instead); the boot report stops
  being a one-shot edge (the V8-M3 refill hole) and becomes the level rule's first
  firing. **Convergence without re-mint** (V10-M1 replaces V9-M3's universal
  re-mint; branch mechanics corrected V11-M1): a re-proposal carries the node's
  **current** identity triple *unchanged* — nothing discontinuous happened, so
  `identity_seq` does not bump. **After a fence refusal the re-proposal admits
  under the *strict* branch**, not the equality branch: the fence refusal
  landed nothing — admission refusal is atomic over the whole payload — so
  the stored triple is still the *pre-mint* value, strictly
  below the payload's — the field write and the arm writes land together under
  the fresh observations, and that is how a `Demotion` role write
  lands after a fence refusal without minting a fake discontinuity. The
  **equality branch's territory** is the re-proposal whose identity write
  already landed but whose topology facts have since drifted, plus idempotent
  re-delivery — there the arm writes land alone, the field write skipped. On
  **either** branch the arm is selected by the payload's `kind`, which names
  the node-originated transition per the durable pending record (the stamping
  rule above), never the historical minting moment and never a bare plane
  comparison. **Split-plane convergence, per direction** (V12-C1/V12-M2,
  replacing V11-M1's "converges any split however reached" — an unqualified
  universal that was false in one direction and destructive in the other): a
  split *explained* by a pending record converges local→replicated through
  that record's report (the demotion's adoption completes on admission — the
  only kind a record can carry since V14-C1); an *unexplained* split converges
  replicated→local through the role-authority adoption above, then a `Boot`
  report — never a `Demotion` report the arm conjuncts would refuse forever
  (V12-M2's mute trace: local-Replica/replicated-Primary stamps `Demotion`
  with `new_primary_id` naming the demoted old primary, the upstream-validity
  conjunct refuses, the boot-ordering rule mutes the node while §3's
  defence-in-depth fences the shard's slots unserved), and never a promotion
  report that would revert an admitted demotion (V12-C1's trace — now dead at
  the kind enum itself, V14-C1).
  V11-M1's stranded-Boot trace stays closed — the unexplained-split `Boot`
  report proposes only *after* the local adoption aligned the planes, so its
  no-role-write arm is sufficient, not a strander. (The universal re-mint falsified
  §0's "`identity_seq` bumps only at genuine discontinuities" claim and had two
  concrete failure legs: a Raft-snapshot-install divergence would re-mint, and
  that *identity change* would cancel every migration the node sources — §4's
  cancel rule firing on a non-event; and the boot rule's "has applied" test
  could livelock — a node re-minting faster than Raft applies its reports never
  sees its own triple in applied state, staying muted with an fsync per tick.)
  **Damping rule, stated**: a node keeps **at most one** `ReportRunIdentity`
  proposal in flight — the reconcile re-proposes only after observing the
  previous proposal applied or refused — so the level rule converges without
  flooding the log; a genuine identity change mid-flight simply supersedes it
  (the new mint orders strictly above, the stale report refuses). **A target's identity change does not cancel**
  (V4-M5 — asymmetric by design: positions are denominated in the *source's* history,
  so a source discontinuity invalidates them, while a target restart invalidates
  nothing about the position space; the target's boot reconcile resumes from
  `covered_applied` or, if its shadow is unavailable, proposes `CancelSlotMigration`
  itself, §4/§5).
- **`AttestReplicaSynced{node, proposer, observed_primary_id: NodeId,
  observed_config_epoch: u64}`** (new — V14-C3; payload **fenced** per
  V15-C4). **The trigger, stated once and covering both resync shapes**
  (V15-M3): the replica proposes when its **replication link with its
  current upstream is established and its replica offset has reached that
  link's sync point**. An *initial full sync* completing satisfies it; so
  does a *partial resync* — the PSYNC continuation, including the `replid2`
  path a promotion opens — catching up to the upstream head observed when
  the link was established. Revision 14 named only the full sync, which left
  every partially-resynced replica with no route back to `synced == true`
  and, after V15-C1 zeroed the failover siblings' stamps, no route back into
  any candidate set (V15-M3's liveness hole). One trigger, both shapes, no
  second mechanism. Admission: `payload.proposer == payload.node` (V6-C1's
  origin rule) ∧ `node ∈ nodes` ∧ `nodes[node].role == Replica` ∧ — new,
  V15-C4 — **`nodes[node].primary_id == Some(payload.observed_primary_id)`
  ∧ `nodes[node].config_epoch == payload.observed_config_epoch`** — the
  Demotion arm's per-object fence with its **first operand re-pointed from
  `role` to `primary_id`**, because what this transition qualifies is the
  *parent*, not the role (the epoch half is unchanged, and is the same
  declared `NodeInfo.config_epoch` field TR-CLUSTER-018/042's fences use, not
  a prose phrase). Apply: writes `nodes[node].synced = true`.
  The observed pair is read from **applied replicated state at mint** and
  **re-checked against applied replicated state at apply**, so admission
  reads only replicated state plus the committed payload and never a
  node-local value (FM-CLUSTER-089 apply-determinism) — and **no wall clock
  anywhere** (settled ruling 1): the attestation is an event the replica
  observes locally and stamps into a committed payload, and the field it
  writes is the *only* thing any later predicate reads.
  Two mechanisms, not one, make the attestation *about the current parent*:
  every apply that gives the node a **new parent** clears `synced` in the
  same apply (§0's companion-field rule, keyed on the `primary_id` write —
  the Demotion arm, LOCKED `SetRole`-to-replica, the failover
  demote-and-re-parent arm, the failover **sibling** re-parent, and
  `RemoveNode`'s detach), **and** the fence above refuses an attestation
  *already in flight* when the re-parent lands first. Without the fence the
  companion rule is not enough: a replica of `A` mints an attestation, a
  re-parent to `B` commits, the stale attestation applies afterwards and
  stamps `synced = true` for a parent whose history the node holds none of —
  a zero-byte promotee re-created behind the very gate V14-C3 added
  (V15-C4). **Refusal disposition** (V15-m5): a refused attestation is a
  **no-op at apply** — nothing is written, nothing is recorded, no record or
  fence exists to dispose of. The proposer re-evaluates its trigger against
  its *current* upstream and re-proposes if (still) caught up; since a
  re-parent necessarily establishes a new link, the trigger fires again on
  that link's own sync point, so the retry needs no extra rule. Gate 3 (d)'s
  **`RefusalClass` enumeration is scoped to `ReportRunIdentity`** and does
  not extend here: attestation refusals are uniformly no-op-and-re-attest.
  **Failover-candidacy amendment** (LOCKED TR-CLUSTER-021, with
  TR-CLUSTER-018's belt gaining the same conjunct — blast-radius entries
  below): the candidate set for a failing primary is its replicas **with
  `synced == true`**. `Failover{force: false}` — the auto path and the default
  manual one — refuses an unsynced candidate; `Failover{force: true}`, the
  operator's declared lossy override (consistent with TR-CLUSTER-042's
  outright-removal semantics), bypasses the gate. A shard whose only replicas
  are unsynced therefore gets **no auto-failover**: a stated availability loss,
  chosen over whole-shard substitution, because a replica that has been *named*
  as a replica in replicated state but has not yet received a byte is exactly
  the zero-byte promotee V14-C3 traced — promoting it silently substitutes an
  empty keyspace for the shard's, violating `inv_no_acked_write_lost`, which is
  this design's declared priority.
  **A planned failover is never refused by this belt** (V15-M4, reconciling
  LOCKED TR-CLUSTER-017): the planned flow already arms a barrier, drains,
  and **waits for offset parity** between the primary and the chosen
  successor before proposing the `Failover` — and offset parity on the
  successor's live link *is* this attestation's trigger. The flow therefore
  orders the attestation's commit ahead of the `Failover` proposal: wait for
  parity, verify applied `nodes[successor].synced == true` (re-attestation
  being the successor's own level-triggered duty if the observed pair moved
  under it), then propose. **No exemption is carved for the planned path —
  one belt, uniformly**; parity is simply the proof the belt was already
  asking for, which is why adding the conjunct costs the planned flow no new
  wait. The residual availability case — *every* replica of a dead shard
  unsynced, so no auto-failover — has a declared operator escape: LOCKED
  TR-CLUSTER-020's replica-issued `CLUSTER FAILOVER` (issue 28 refuses the
  command on a primary, so it is issued **at** the chosen replica), whose
  `force: true` branch bypasses the gate with the substitution accepted
  explicitly rather than silently.
  Both failover **promotion** arms write
  `synced = true` and `promoted_from = Some(old_primary_id)` on the promotee
  (V14-C2/V14-C3), and `admitted_stage = None` (V14-M1).
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
   entry to the validated successor in the same apply, and the level-triggered
   `RetargetSlotResidue` **source arm** (V8-C4) catches every entry the in-apply
   re-target missed (an entry created after the demotion applied, or a
   successor promoted later) — after either, case 1 applies to the new source.
   For the **target field** no level-triggered catch-up exists *or is needed*
   (V10-C4 supersedes V9-M1's target arm): entries are created only with a
   Primary target (`Complete`'s role conjunct, V10-C3), and every lawful
   role→Replica write on a residue-named member carries a validated successor
   and re-targets the entry **in the same apply** (§0 — refuse-whole covers the
   successor-less case), so no reachable state holds an unpromoted entry naming
   a non-primary target **that is still a member** (V11-m1 — the qualifier is
   load-bearing: a removal with no successor lawfully leaves an unpromoted
   entry naming the *departed* target, §0's lifecycle rule, and that state
   discharges through case 3, the rollback arm, not through this argument);
   the in-apply successor holds base + shadow (§5's
   full-sync rule) and resumes the promotion.
3. **`promoted == false`**: the `AssignSlots` rollback arm (Transitions, V7-M5) —
   lossless to `shard_primary(entry.source)` while `source_gone == false`,
   token-gated otherwise. **"Lossless" here rests on gate 3's staging rule**
   (V11-C2 binds the qualifier): the one path that could have destroyed a live
   member source's copy pre-demotion — a local `CLUSTER REPLICATE` adopting a
   foreign upstream (V14-C1: `REPLICAOF` cannot reach this path in cluster mode
   at all, so the staging rule is the *only* remaining path and the binding is
   tighter than v11 claimed) — discards only after the Demotion report's admission has re-homed
   the node's slots and re-targeted its residue entries, so in every reachable
   state where this arm's lossless disjunct holds, the source's copy exists.
4a. **No lawful automatic remover, promotion attested** (`promoted == true ∧
   target_gone == false` — V10-m8 scopes the case, and
   `source_gone == true` or `role != Primary ∧ shard_primary(entry.source) ==
   None`): the token-gated `ClearSlotResidue{accept_stale_copy}` for the entry
   (V9-M4 — the verb admits **only** at `promoted == true`; the same source
   states at `promoted == false` are case 3's territory, where the rollback
   arm's apply moves the slot map and its token names the loss honestly).
4b. **Orphaned slot** (`promoted == true ∧ target_gone == true`):
   `ClearSlotResidue` refuses here **unconditionally** — its
   `target_gone == false` conjunct (V10-m8; V9-m2's reading relied on the
   source typically being a live Primary, which the `source_gone` disjunct
   defeated exactly when the source was *also* gone — removal of the last
   in-cluster copy's tracking entry under a token attesting a milder loss).
   The `AssignSlots` orphan re-home arm for the slot
   (Transitions, V7-M6) is the state's **sole** exit.
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
**Non-command mutators are enumerated too** (V9-m4 — maxmemory eviction and the
active-expiry cycle mutate without a command, emitting `DEL`/`UNLINK` into the
feed above `drained_pos`; unenumerated, they falsify the seal's headline claim
exactly like an exempt `MIGRATE` would — the target resurrects any key they
delete post-seal): **both are suspended for a sealed slot**. Eviction selects
victims from other slots (the sealed slot's memory is about to leave the node
either way); the active-expiry cycle skips the sealed slot. **The one permitted
post-seal mutator, classified** (V10-m7 — revision 9 permitted it in passing
without classifying it against the seal's headline): **lazy expiry-on-read** in
the sealed slot continues — a read of a key whose TTL has passed answers absent
and removes the key locally. It is sound where the two suspended mutators are
not, on two grounds: it is **feed-invisible** (the local removal emits no `DEL`
on the migration stream — expiry deletes never propagate there, §4/§5: each
side expires on its own clock, FM-REPLICATION-030's rule) and **logically
idempotent across the pair** (the key's logical value is already "absent" on
both sides once its TTL passes — the target holds the same stored TTL, §5, and
its own lazy expiry converges to the identical state after promotion). The
seal's headline claim is therefore **scoped, stated**: "no write below
`drained_pos` remains and none will follow" quantifies over **feed-visible
mutations of logical state**; lazy expiry mutates only the physical store, in a
way both sides reach independently. The suspension window is bounded by
`Draining`'s exits
— the observation bound, cap breach, or `Complete` — so neither suspended
mutator is deferred unboundedly.

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
| `AbortSlotHandoff` applies | execute at source, acknowledged normally — **provided the source is still the slot's serving primary at apply** (V11-C1: the same else-branch as the Cancel row, previously stated on one execute-at-source exit but not the other); **otherwise** the demotion-disposition row below governs: answered per the new role, never executed |
| Cap breach, pre-apply | beyond-cap writes: `-TRYAGAIN`; held set: unchanged until apply |
| `CancelSlotMigration` applies | execute at source, acknowledged normally (release event) — **provided the source is still the slot's serving primary at apply**; **otherwise** (V10-C1 completes the else-branch) the demotion-cancel row below governs: answered per the new role, never executed |
| **Cancel caused by the source's own demotion** (the **demotion-disposition row** — V5-m5: the `ReportRunIdentity` demotion/adoption arm's *field write* cancels the migrations the node sources. Reachability under V10-C1/V11-C2: gate 3 refuses a `CLUSTER REPLICATE` node-locally on every migration source — a `Begin` source owns its slot, so the gate always binds — so this row's triggers are the failover-driven demotions, `Failover{force: false}`/`SetRole`, whose subsequent history-adoption report's identity **field write** cancels (stamped `kind = Boot` per V12-C1's rule — the failover apply already wrote the role; the cancel binds to the field write, arm-independent). The staged-flip *admission* is deliberately **not** a trigger (V12-m3 — the v11 text listed it vacuously): an admitted staged report implies the shard-relationship conjunct did not bind, i.e. the node owned no slots and armed no barrier at apply, so there is no barrier-held set for this row to dispose of; that window's client disposition is the **staged-flip fence row** below (nothing held, everything `-TRYAGAIN`). A *refused* staged report is disposed of by **gate 3 (d)'s refusal partition, keyed on the declared `RefusalClass`** (V14-M2 — the v13 text's unqualified "reverts, and anything arriving after the revert executes normally" was true only of the refuse-whole-at-a-still-Primary-node arm): refuse-whole (`upstream-validity`/`shard-relationship`) at a still-Primary node reverts and subsequent commands execute normally, the source being still the serving primary; a `fence`-class refusal keeps **both the record and the fence up**, so subsequent commands keep answering `-TRYAGAIN`; a supersession (same classes, node no longer Primary) clears the record and replies per the node's newly adopted role; `ordering` is a stale-duplicate no-op that changes nothing; `membership` runs the removal-of-self flow | the held set is answered with **the reply the node's new role implies** — `-MOVED` to the new primary when the local `shards` view names one, **`-TRYAGAIN` when `primary_id == None`** (V8-M4: the disposition is total; a successor-less demotion answers retryably and the client's retry lands wherever the eventual topology says) — and is **never executed**: a demoted node executing queued writes would fork history against its new primary. **This row retains the self-fence demotion-disposition as defence-in-depth** (V10-C1, scope narrowed V11-C2: the plane-split state — locally a replica, replicated-Primary with slots — is unreachable through gate 3's staged path; a staged report's refusal reverts instead): should the state nonetheless present, the node answers its held set and every subsequent command for those slots `-TRYAGAIN` — never `-MOVED` (replicated state names *itself*, a loop) and never executed — until an operator exit re-aligns the planes — failover of the shard, `AssignSlots` away with the loss token, or `CLUSTER FAILOVER TAKEOVER` on the shard's surviving member (V14-C1 replaces the v13 text's `REPLICAOF NO ONE`, which cluster mode refuses at dispatch; `TAKEOVER` proposes a replicated `Failover`, i.e. the replicated-plane exit), with the stated caution that in a plane-split state the local dataset's lineage is unverified, so these are operator guidance for a bug state outside the spec's reachable set, **not** verified-lossless rules (V11-C2) |
| **Staged-flip fence** (gate 3's pending-transition record present, report unresolved — V12-M1: gate 3 passing implies no owned slots and no armed migration barrier, so the staged fence inherited no cap, no exit row, and no latch coverage from this table; this row is its coverage) | **nothing is ever held** — the level rule is the self-fence latch row's shape, keyed on the pending record instead of an armed barrier: while the record is pending, the held set for this fence is *empty* by invariant; the fence's scope is the **whole node** (V14-m5 replaces the v13 text's unbound "every command that would execute on this node as a slot-serving primary"): **every** command arriving at the staged node — keyed or keyless, read or write, admin included, and including writes for a slot an `AssignSlots` assigned it after the gate check, the race interleaving staging exists for — is answered `-TRYAGAIN` immediately, so no client ever waits out a partition on this fence (settled ruling 2) and no byte cap is needed (nothing accumulates). The initiating `CLUSTER REPLICATE` client's deferred reply is bounded by `cluster-staged-flip-reply-timeout` (§0 gate 3 — answered `-TRYAGAIN` with the stated ambiguous outcome; the stage itself never times out, because revert-then-admit would re-open the closed race). **The fence's exempt set is enumerated by member, like the seal's** (V13-m3, made explicit per V14-m4 — the v13 text exempted the whole **`CLUSTER` family**, which would have admitted `CLUSTER FLUSHSLOTS` and `CLUSTER SETSLOT` against the very dataset and slot map the staging exists to preserve): *read-only introspection* — `CLUSTER INFO` (the pending-stage read path), `CLUSTER MYID`, `CLUSTER NODES`, `CLUSTER SHARDS`, `CLUSTER SLOTS`, `CLUSTER COUNTKEYSINSLOT`, `CLUSTER GETKEYSINSLOT`; *gate-reaching* — `CLUSTER REPLICATE` and `CLUSTER FAILOVER` (any variant), which must reach gate 3's single-writer rule to receive the declared pending-stage refusal rather than a bare `-TRYAGAIN`; *operator escape* — `CLUSTER RESET` (still gated by gate 2's empty-keyspace rule) and `CLUSTER FORGET`. **Everything else follows this row, explicitly including `CLUSTER FLUSHSLOTS` and every other mutating `CLUSTER` subcommand.** `REPLICAOF`/`SLAVEOF` in any spelling never reaches this fence at all — cluster mode refuses them at dispatch (§0 gate 3's role-command surface), so the fence and the single-writer rule cannot disagree about their reply (V14-m1). Resolution exits, enumerated per gate 3 (d)'s partition (V14-M2): admission → adoption proceeds, subsequent commands answered per the node's new role (`-MOVED`/`-TRYAGAIN` as the demotion-disposition row's reply rule); **refuse-whole at a still-Primary node** → revert, node resumes serving, subsequent commands execute normally; **supersession** → record cleared, replies per the newly adopted role. A **`fence`-class refusal is explicitly not an exit** — the record and this fence both persist and the report is re-proposed with fresh observations; likewise an `ordering`-class stale-duplicate refusal changes nothing. A `membership`-class refusal exits via the removal-of-self flow (record cleared, fence dropped, non-member behavior) |
| **Self-fence latch arms** (TR-CLUSTER-026: no Raft leader contact within an election timeout) | answer the **entire held set** `-TRYAGAIN` and **keep the fence** (N-M1) — a sealed source that cannot apply must not make held clients wait out a partition; erroring a held write is *more* fenced, not less, so the §3 invariant holds and the sealed rule ("no further execution until an exit applies") is untouched. **Level rule, not an edge** (V8-M1; scope corrected V9-M2 — the earlier "and the slot sealed" qualifier left pre-`Confirm` Draining uncovered: a write arriving after the one-shot flush, before any seal, was held again and waited out the partition, the indefinite hold settled ruling 2 forbids): while the latch is armed **and this node's barrier is armed for the slot — sealed or not** — the invariant is *the held set is empty*: no new hold is ever admitted, and every arriving write (including one racing the flush) is answered `-TRYAGAIN` immediately. Erroring is strictly more fenced than holding in both sub-states, so §3's "never weaker" invariant holds. The one-shot flush is merely the transition into that region; a write arriving after it does not wait out the partition |
| Client disconnects while held | held entry dropped with the connection (no reply owed) |
| `CLIENT UNBLOCK`/`KILL` on a held client | `-UNBLOCKED` / connection close, per blocking rows |
| Failover prunes the record (a failover **not demoting this node** — V6-m6) | release event (FM-CLUSTER-087); writes follow new topology |

When one graceful failover both demotes this source and prunes its record, the
**demotion-cancel row wins**: the held set is answered per the new role and never
executed (V6-m6 — the two rows previously both matched with contradictory
dispositions). **Precedence, stated generally** (V11-C1 — V10-C1's else-branch
was propagated to the Cancel row but not the Abort row, the same class of miss
the diff-scoped check exists for): whenever the demotion-disposition row
matches — the node is not, at apply, the slot's serving primary — it wins over
**every** other exit row in this table. No exit row ever executes a held write
on a node that has been demoted (or whose demotion is staged, gate 3): executing
there forks history against the shard's new primary regardless of which
transition triggered the exit. Each execute-at-source row states the proviso
locally; this sentence is the rule they instantiate, so a future exit row
inherits it by default.

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
  (Transitions) carrying the minimum across the replicas it counts. **The replica-side
  install contract** (V10-C2 — the half §0's durable-attestation note promises): a
  counted replica acks a stamped batch only once its shadow store is **fsync-durable
  (file and parent directory) through that batch, regardless of the replica's
  configured `Durability`** — the ack is an attestation in the same sense as the
  target's own ingest reports, and the target folds only such acks into the minimum
  it proposes. Without this rule a `relaxed`-durability replica's ack would let the
  optional conjunct's "N durable copies" claim admit copies a single crash erases;
  with it, the conjunct's meaning is configuration-independent. `Complete`'s
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
  states the disk contract precisely (V9-C2 revises V5-m6's absorption):
  `Complete`'s possession conjunct (`target_ingested_pos ≥ drained_pos`) is
  backed by §0's durable-attestation rule — the shadow is **fsync-durable
  through every attested position, regardless of the target's configured
  durability** — so a target crash at any moment, before or after `Complete`,
  cannot lose the migrated span below `drained_pos`. The accepted exposure is
  therefore honestly **post-`Complete` only**: the replica-lag window above,
  plus the ordinary writes the promoted target serves *after* cutover, which
  carry the target's configured durability exactly as its other writes do — the
  migration hardens the attestation, never either node's steady-state
  durability contract. (V8-C3's attesting-run conjunct remains belt-and-braces
  for the restart-that-reports path; before V9-C2 it was the sole guard, and a
  crashed, still-down target satisfied it with its stale-high stored run — the
  claim that a pre-`Complete` crash "falsifies" it was false while the target
  stayed down, which is exactly when the leader's auto-`Complete` fires.)
  Operators who
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
`nodes[self].role`** in applied state (SS-2, V7-C1/V9-M3 — written only by its
declared writers: the `ReportRunIdentity` Demotion arm and the
failover/`SetRole` transitions, never a node-local flag — the Promotion arm is
deleted, V14-C1), so the defer
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
| `cluster-staged-flip-reply-timeout` | 5 s | node-local bound on the staged flip's initiating `CLUSTER REPLICATE` client's deferred reply (V14-C1 — the only spelling that reaches the stage) (§0 gate 3, §3 staged-flip fence row). **Client-reply bounding only** — the stage itself never times out (revert-on-timeout would re-open the closed race) and the value is **never an admission input** (V13-m1) |

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
  FM-REPLICATION-022 (**noted**, V7-C1, narrowed V14-C1: the run-identity report is now
  kind-stamped — `Boot` / `Demotion` in the committed payload — so the
  row's "a demotion bumps identity" behaviour is carried by an
  explicit payload discriminant, not inferred at apply time; the row's
  `REPLICAOF` spelling is a **standalone-mode** spelling — in cluster mode the
  command is refused at dispatch and the demotion spelling is
  `CLUSTER REPLICATE`, which is what the discriminant is stamped from);
  TR-CLUSTER-033 (**adopted and rewritten**, V12-C1/V12-M2 — previously uncited
  while the design ran a level-triggered local→replicated reconcile on the
  identical plane-disagreement predicate in the opposite direction, an
  undeclared-precedence contradiction with a LOCKED row: the row's
  replicated→local `SelfRoleReconciler` becomes the design's declared
  role-authority convergence for *unexplained* disagreements, gaining two
  arms — the **pending-record exclusion** (no adoption while a durable
  pending-transition record explains the disagreement; that record's report
  owns convergence, so the two reconcilers act on disjoint predicates and
  never contend) and the **lineage guard** (never adopt `Primary` over a
  foreign-lineage local dataset; that state routes to §3's defence-in-depth
  self-fence); the row's **"persisting across ticks" damping is subsumed**,
  not dropped (V13-C1): the destructive demotion-adoption no longer fires on
  observing `role == Replica` for any number of ticks — it fires only on
  gate 3's **resolved-stage selector**, which names the record's own admitted
  report (the replicated `admitted_stage` stamp — V14-M1 replaces v13's
  `run_identity` operand, which the per-boot candidate re-mint falsified;
  V15-C2 then collapses the remaining operands into this one, the role and
  pointer facts being theorems of the companion-field rule rather than
  independent guards), and which is strictly stronger than any tick count —
  and, crucially, the **same** predicate the pending-record exclusion reads in
  its negated direction, so this LOCKED reconciler and the staged adoption can
  never both claim a state);
  FM-CLUSTER-046 (**rewritten**, V12-M2: the row records that the reconciler
  "does not exist at all until role-change detection is enabled" — under this
  design it is unconditional and load-bearing (it is what discharges the
  crashed-before-local-promotion boot, which would otherwise refuse forever
  on the Demotion arm's upstream-validity conjunct and sit mute under the
  boot-ordering rule), so the feature gate is removed and the forcing tests
  `test_reconcile_self_role_demotes_a_restored_replica` /
  `test_reconcile_self_role_promotes_a_restored_primary` gain the
  pending-record-exclusion and lineage-guard arms; the forcing test
  `test_self_role_reconciler_absent_until_detection_is_enabled` forces the
  *removed* "absent until enabled" clause and must be **deleted or
  inverted in the same change** — left standing it would fail against the
  unconditional reconciler and pin the old behaviour (V13 blast-radius
  note));
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
  TR-CLUSTER-004 (`SetRole` — **rewritten**, V8-M5/C2/V9-C1: as one of the three
  demotion-writing transitions it re-targets the demoted node's residue entries and
  re-homes its owned slots in the same apply, and it carries issue 19's per-object
  epoch fence — the arm V8-C2's stale-report clobber would otherwise race; per
  V9-C1 the re-home/re-target destination is the **validated successor**: whenever
  the demoted node owns slots or is named by any residue entry, admission requires
  `payload.new_primary_id == Some(p)` with `nodes[p].primary_id == Some(node)`
  checked pre-apply, and a payload failing the conjunct **refuses whole** — the
  destination is never derived by a post-write `shard_primary` walk.
  **Foreign re-parent, discussed** (V14-C2 — the v14 review flagged this row as
  an uncited collision): `SetRole` may lawfully re-parent a **slotless**
  replica onto a foreign primary, so a node's local upstream can legitimately
  belong to another shard's lineage. Two of this design's rules are what keep
  that lawful write from turning into a data-substitution or empty-candidate
  path: the lineage guard's `promoted_from` binding (an absent upstream admits
  a `Primary` adoption only when *this node's own promotion* adjudicated it
  away, so a `SetRole` re-parent followed by `FORGET` and a later promotion
  fails the guard and self-fences instead of serving foreign data), and the
  `synced` gate (a `SetRole` re-parent writes `synced = false`, so the
  re-parented node is not a failover candidate until it has actually taken its
  new upstream's history). This row's writes therefore carry
  `synced = false`, `promoted_from = None` and `admitted_stage = None`);
  TR-CLUSTER-005 (`MEET → AddNode` handshake — **rewritten**, V9-M5: the row's
  issue-25 ruled precondition (empty local Raft state) **composes with** the
  join-empty admission gate (§0, V8-C1) into one stated precondition: a node
  accepts the handshake — first join and re-join after `FORGET` alike — only while
  its local Raft state is empty *and* its main keyspace is empty; both checks are
  node-local and fail-closed, the refusal is handshake-side (deletes nothing) and
  names *which* gate fired; the wipe path is `CLUSTER RESET HARD`
  (TR-CLUSTER-035's HARD path clears the Raft state, and its own non-empty-keyspace
  refusal — gate 2 — forces `FLUSHALL` first when data remains; `FLUSHALL` alone
  never satisfies the Raft-state half); forcing tests:
  `join_refused_while_keyspace_nonempty`, `join_refused_while_raft_state_nonempty`,
  `join_refusal_names_the_failing_gate`,
  `reset_hard_then_rejoin_succeeds_flushall_alone_does_not`);
  TR-CLUSTER-042 (`Failover{force:true}` — **rewritten**, V8-M5: the row owns
  outright-removal semantics, so the residue map's `source_gone`/`target_gone`
  marking rides it explicitly — prune marks, never removes — and issue 20's
  demote-don't-remove default is restated at the row as the preferred path;
  additionally **cited by §0's lineage guard** (V13-M1): the guard's
  removed-upstream disjunct exists because this row removes
  the old primary from membership outright, so a node promoted by a forced
  failover that crashes before its local promotion took effect boots with
  an upstream absent from `nodes` — a lawful state the guard must admit;
  **further amended, V14-C2/V14-C3**: this row's promotion write now also
  stamps `nodes[promoted].promoted_from = Some(old_primary_id)` and
  `nodes[promoted].synced = true` (and `admitted_stage = None`, V14-M1). The
  `promoted_from` stamp is what makes the lineage guard's third disjunct
  *specific* — `u ∉ nodes ∧ promoted_from == Some(u)` — instead of admitting
  any absent upstream, which let a `SetRole`-re-parented, later-`FORGET`-ten
  foreign lineage pass; the `synced` stamp keeps the promotee a lawful
  candidate under the amended TR-CLUSTER-021 without a separate attestation);
  TR-CLUSTER-021 (failover candidacy — **amended**, V14-C3: the candidate set
  gains the conjunct `nodes[candidate].synced == true`, the new replicated
  data-possession fact written `false` by every apply that gives a node a new
  parent and `true` only by the promotion arms or the node's own
  `AttestReplicaSynced`. Without it, a replica that replicated state merely
  *names* — a `SetRole`/`CLUSTER REPLICATE` pointer written before a single
  byte flowed — is a lawful failover candidate, and promoting it substitutes an
  empty keyspace for the shard's entire dataset with no token anywhere. Stated
  consequence: a shard whose only replicas are unsynced gets no auto-failover —
  availability loss, chosen over whole-shard substitution. Forcing tests:
  `unsynced_sole_replica_is_not_a_failover_candidate`,
  `forced_failover_over_unsynced_replica_is_the_declared_lossy_override`);
  TR-CLUSTER-018 (`Failover{force:false}` — **amended**, V14-C3/V14-C2: its
  belt gains the same `synced` conjunct, and its promotion/demotion writes
  carry `promoted_from` and `synced` per the field-writer table; its demote-
  and-re-parent arm writes `synced = false`, `promoted_from = None`, and
  `admitted_stage = None` — the last is what keeps V13-C1's trace dead now
  that gate 3's adoption binds to `admitted_stage` rather than to
  `run_identity`);
  FM-CLUSTER-047 (**extended**, V14-M3: the row's committed-rejection rule
  gains the rejection's **class** — `ReportRunIdentity`'s apply records
  `refused(class)` with `class` the first failing conjunct in the declared
  evaluation order — so a proposer can key a node-local disposition on *why*
  its report was refused rather than on prose. The class is a pure function of
  committed payload plus pre-apply state, so FM-CLUSTER-089 determinism is
  preserved);
  SS-11 (**amended**, V8-C6: gains the invariant "no slot is assigned to a node
  whose role is `Replica`" — every writer that flips a role to `Replica` re-homes
  the node's slots in the same apply, and every slot writer requires the assignee's
  role to be `Primary`; forcing test asserts the invariant over every transition
  interleaving, `inv_slots_only_assigned_to_primaries`);
  SS-2/SS-3 (**amended**, V10-m3, tightened V14-C1, **re-based on the LOCKED
  writer cells in V15** — V15-C1: revision 14's "closed writer enumeration"
  was the *design's own prose list*, and it **omitted two LOCKED writers**,
  which is exactly how the companion stamps went unwritten on the failover
  **sibling re-parent** and on `RemoveNode`'s **detach**. The enumeration is
  therefore stated as the LOCKED cells verbatim with every writer marked, and
  §0's writer-join subsection re-runs that join every revision:
  **SS-2 `role`** — "`apply_command`: `SetRole`, `Failover` (promote/demote),
  `ResetCluster` (forces this node's role to `Primary`, `commands.rs:834`)":
  `SetRole` **amended**; `Failover` promote/demote **amended**;
  `ResetCluster` **amended** (carve-out 2, so `synced = true` — V15-m4);
  **added by this design**: the `ReportRunIdentity` **Demotion arm**. Flagged
  for the spec change: SS-2's cell omits `AddNode` although TR-CLUSTER-002's
  postcondition sets role at a fresh registration — the row should name it
  (§0's writer-join records the disagreement rather than silently choosing).
  **SS-3 `primary_id`** — "`apply_command`: `AddNode`, `SetRole`, `Failover`
  (re-parent), `RemoveNode` (re-parents the departing node's children via
  `reparent_children`, `commands.rs:231`), `ResetCluster` (nulls this node's
  own parent pointer, `commands.rs:835`)": **all five amended**, this being
  the row the companion-field rule is keyed on — including `Failover`'s
  *three* distinct pointer writes (promotee cleared, old primary re-parented,
  **siblings re-parented**) and `RemoveNode`'s `reparent_children(.., None)`
  detach; **added by this design**: the Demotion arm. The Promotion arm is
  **removed** from the enumeration, since cluster mode has no node-originated
  promotion spelling at all.
  Forcing tests: every role/`primary_id` change in a transition sweep comes
  from the enumerated set, **and every `primary_id` write in that sweep also
  writes the three companion fields** —
  `companion_fields_written_by_every_parent_writer`, the v15 test that fails
  against revision 14's rule; the Quint property
  `inv_role_written_only_by_declared_writers` is re-based on `primaryId` as
  its model twin.
  **New replicated fields, V14**: `promoted_from: Option<NodeId>`,
  `synced: bool` and `admitted_stage: Option<u64>` join the `NodeInfo` rows,
  each with the writer enumeration in §0's field-writer table and each subject
  to the same rule — *every writer of the node's parent pointer also writes
  the companion field* (V15-C1 re-bases the rule from the role fact to the
  `primary_id` fact, the two LOCKED detach/re-parent writers being
  `primary_id`-only writers), which is what lets a reader treat them as
  adjudication stamps. `AddNode`'s upsert preserves **all three** —
  `promoted_from`, `admitted_stage` *and* `synced` — field-wise (V7-M3's
  rule, extended per V15-M2); `ResetCluster` clears the pointer stamps and,
  forcing `role = Primary`, writes `synced = true`).
- **Added in revision 15** (the rows the v15 review found the design reading,
  amending, or contradicting while citing none of them — each with verdict and
  forcing-test disposition):
  **TR-CLUSTER-018 + TR-CLUSTER-042, sibling-re-parent halves**
  (**amended**, V15-C1): "siblings of `old_primary_id` re-parented to
  `new_primary_id`" / "`old_primary_id`'s remaining replicas … are re-parented
  to `new_primary_id` (`reparent_children`, `commands.rs:459`)" is a
  `primary_id` write on *every* sibling, so each sibling's apply now also
  writes `synced = false`, `admitted_stage = None`, `promoted_from = None`.
  Revision 14 wrote none of them, leaving the whole surviving replica set
  carrying stale stamps — the stage stamp of a sibling being the operand gate
  3's destructive adoption binds to. Existing forcing tests for both rows stay
  valid; new forcing test
  `failover_sibling_reparent_clears_companion_stamps`.
  **TR-CLUSTER-003 + FM-CLUSTER-002, the detach half** (TR-CLUSTER-003
  **amended** — its prune rewrite above now also carries the companion writes
  on `reparent_children(.., None)`; FM-CLUSTER-002 **unchanged, stated** —
  behaviour preserved exactly, V15-C3): the row's *Observable* detach and its
  *NOT observable* "no re-parent, no promotion" are what make
  `role == Replica ∧ primary_id == None` a **reachable, declared** state, the
  detached replica, which revision 14 wrongly called unreachable and fenced.
  FM-CLUSTER-002 appeared **nowhere** in revisions 1–14; that omission is the
  defect this entry closes. Forcing test
  `remove_node_prunes_migrations_and_detaches_replicas` **stays valid
  unchanged** and gains two assertions (the detached replica's companions are
  cleared; it is in no candidate set); new forcing tests
  `detached_replica_serves_per_its_role_and_is_no_candidate` and
  `detached_replica_is_rehomed_by_setrole`.
  **TR-CLUSTER-017** (**unchanged, stated**, V15-M4): the planned-failover
  barrier/drain/**offset-parity** wait is the same condition as this design's
  attestation trigger, so the flow orders the attestation's commit before the
  `Failover` proposal and the amended TR-CLUSTER-021 belt never refuses a
  parity-proven planned failover. **No exemption is added** — one belt,
  uniformly. Forcing test
  `planned_failover_waits_for_the_successors_attestation`.
  **TR-CLUSTER-020** (**unchanged, cited**, issue 28): its replica-issued
  `CLUSTER FAILOVER` — refused on a primary — is the declared operator escape
  for a shard whose replicas are all unsynced, reaching the `force: true`
  branch that bypasses the candidacy belt with the substitution accepted
  explicitly.
  **TR-CLUSTER-027** (**unchanged, stated — extended**, V15-M2): the live
  `CONFIG SET cluster-replica-priority` re-registration routes through the
  field-wise `AddNode` upsert, so it must preserve `synced` as well as
  `run_identity`/`promoted_from`/`admitted_stage` — otherwise a routine
  priority change silently de-candidates a synced replica. Forcing test
  `priority_config_set_preserves_synced`.
  **TR-CLUSTER-041** (**unchanged, cited**, V15-m2): follower proposals answer
  `Proposed::Forwarded` and carry no `ClusterResponse`, which is *why* the
  `RefusalClass` cannot ride the reply channel and is instead computed
  locally, deterministically, at every node's own apply (FM-CLUSTER-047's
  extension is the leader-side companion; FM-CLUSTER-089 determinism is what
  makes the two agree).
  **`AttestReplicaSynced`'s fence fields** (**new row content**, V15-C4): the
  transition's payload gains `observed_primary_id`/`observed_config_epoch`
  and its admission the matching per-object conjuncts, the same fence
  TR-CLUSTER-018/042 already use; forcing test
  `attestation_minted_under_the_old_parent_is_refused_after_a_reparent`.
- **Retired**: FM-CLUSTER-085 (handoff lease — its property, "a dead finalizer cannot
  wedge a slot", is re-provided by the observation bound *plus the leader
  auto-`Complete`* (V4-M2), which together exit every Draining state; replacement row
  states this); **FM-CLUSTER-097 + the `ReplicaFeedGate`** (§8 — purpose re-derived to
  nothing under source authority; row rewritten to assert the absence of migration
  feed holds); **FM-CLUSTER-080's `MIGRATE` slot-pause exemption** (V8-C5/M5: its
  purpose was the retired Redis-style bulk phase's catch-up `MIGRATE`; under the §3
  exempt-set rule `MIGRATE`/`RESTORE` are held like every other write, and the
  forcing test `only_migrate_and_cluster_are_slot_pause_exempt` is re-pointed at the
  new exempt set — `CLUSTER` only. **Disambiguation, V14-m4**: this is the
  *slot-pause seal's* exempt set, which stays whole-family; the **staged-flip
  fence** of gate 3 has a *different*, member-enumerated exempt set (§3's
  staged-flip fence row) that does **not** admit `CLUSTER FLUSHSLOTS` or any
  other mutating `CLUSTER` subcommand. The two sets are never interchangeable
  and no row may cite one for the other).
- **Unchanged, stated**: FM-CLUSTER-038 (blocked-client wake at Complete);
  FM-CLUSTER-061..063 (the admin-gating class's semantics — V6-m5; only 064's table
  gains a row); FM-CLUSTER-095's SlotFence generation input; FM-CLUSTER-100
  (generation survives
  snapshots — extended to the new record fields, `NodeInfo.run_identity`, and
  `handoff_residue`); TR-CLUSTER-026 (self-fence — gains the held-set release
  row, §3); TR-CLUSTER-034 (per-node arm/release reaction);
  FM-CLUSTER-081 (V8-M5: the `CLUSTER` exemption **survives** — it carries
  `SETSLOT … STABLE`, the operator's cancel, which §3's exit table depends on; it is
  the sole member of the §3 **slot-pause seal's** exempt set — V14-m4 scopes the
  phrase, since the staged-flip fence's exempt set is enumerated by member and is
  not this one); FM-CLUSTER-088 (V8-M5/m2: cross-slot
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
  `ReportRunIdentity`'s kind-stamped payload (`Boot`/~~`Promotion`~~/`Demotion` +
  `new_primary_id`; **narrowed in V14** to `{Boot, Demotion}`, V14-C1)
  with the Demotion arm as SS-2/SS-3's declared writer — role and
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
  at-most-one-residue-entry-per-slot invariant (V8-m5). From review v9: the
  demotion re-home destination as the **validated successor** — the admission-time
  shard-relationship conjunct (`new_primary_id` names a current in-shard replica,
  proven pre-apply), refuse-whole on failure as stated
  availability-loss-over-silent-data-loss, and the never-a-post-write-walk rule
  (V9-C1); **durable attestation** — the target proposes only fsync-durable
  positions regardless of configured durability, making `Complete` safe while the
  target is down and demoting V8-C3's replacement machinery to belt-and-braces
  (V9-C2); ~~`RetargetSlotResidue`'s **target arm**~~ (V9-M1 — **superseded in
  V10**: the target arm was an unproven pointer walk, removed by V10-C4; target
  re-home is exclusively the demotion transitions' in-apply re-target, total via
  V10-C3's Primary-target-at-creation conjunct); the latch level rule's scope
  corrected to **armed-barrier, sealed
  or not** (V9-M2); ~~the **Promotion arm as a fenced role writer**~~
  (**superseded in V14**: the arm is deleted with the command surface that
  would have reached it, V14-C1) ~~with the
  universal
  re-mint convergence rule on re-proposal~~ (V9-M3 — the arm survived v10-v13; the
  universal re-mint is **superseded in V10**: it falsified §0's
  identity-discontinuity claim, replaced by V10-M1's split ordering conjunct with
  topology-only re-proposal at equality); `ClearSlotResidue`'s
  `promoted == true` conjunct closing the case-3 short-circuit (V9-M4); the
  join-empty gate **composed with TR-CLUSTER-005's issue-25 empty-Raft-state
  precondition** — one stated precondition, refusal names the failing gate, wipe
  sequence stated once (V9-M5); `observed_config_epoch` typed `ConfigEpoch` and
  bound to `nodes[node].config_epoch` (V9-m1); the orphan re-home arm's case-4b
  split from `ClearSlotResidue`'s case-4a (V9-m2); the SOFT/HARD `ResetCluster`
  identity-cell distinction (V9-m3); eviction and active expiry **suspended for a
  sealed slot** as enumerated non-command mutators (V9-m4). From review v10: the
  **replication-command gate** — gate 3, refusing ~~`REPLICAOF`/`SLAVEOF`~~/`CLUSTER
  REPLICATE` (**narrowed in V14**: the `REPLICAOF`/`SLAVEOF` spellings die at
  dispatch and never reach the gate, V14-C1)
  node-locally while the node owns slots or is residue-named, making
  refuse-whole defence-in-depth rather than the only line, with the declared
  gate-race self-fence disposition (§3's demotion-disposition row) and its
  operator exits (V10-C1); the **replica-side fsync-attestation half** of the
  ack floor — a counted replica acks only fsync-durable application regardless
  of its configured `Durability`, stated at §0's floor note,
  `ReportTargetReplicaAck`'s row, and §5's install contract (V10-C2);
  `Complete`'s **target-role conjunct** (`nodes[record.target].role == Primary`)
  — commit-time re-verification guaranteeing residue entries are created only
  with a Primary target (V10-C3); the **target arm removed** from
  `RetargetSlotResidue` — payload back to single `new_source`, target re-home
  exclusively in-apply, with the source-arm walk's guard-chain proof (V10-C4);
  the **split ordering conjunct** — `≥` admits, strict `>` gates the
  `run_identity` field write, equality is a topology-only re-proposal, the
  cancel binds to the field write, with the one-in-flight damping rule
  (V10-M1, replacing V9-M3's universal re-mint); `counted_replicas(record.target)`
  as a named pure function with **set-change floor clearing** — any apply that
  changes the counted set clears `target_replicas_acked_pos`, and `Complete`
  carries `counted_replicas ≠ ∅` directly (V10-M2); `ResetCluster`'s re-mint
  stamped `kind = Boot` (V10-m4); `shard_primary` total via the `n ∉ nodes` None
  case (V10-m5); Prepare/Begin/Complete absence tests declared in §0's
  enumeration (V10-m6); **lazy expiry-on-read classified** as the one permitted
  post-seal mutator, with the seal headline scoped to feed-visible mutations of
  logical state (V10-m7); `ClearSlotResidue`'s `target_gone == false` conjunct —
  case 4b refused unconditionally, orphan re-home arm sole exit (V10-m8).
  From review v11: the **staged flip** — a gate-3-passing ~~`REPLICAOF`~~/`CLUSTER
  REPLICATE` (**superseded in V14**: `REPLICAOF` never reaches gate 3 in cluster
  mode, V14-C1) fences locally, proposes its Demotion report, and performs the
  destructive upstream adoption **only on the report's admission apply**,
  reverting (dataset intact) on refusal — the race window closed by
  construction, ~~the declared gate-race self-fence disposition~~ narrowed to
  defence-in-depth for a state no spec path reaches, and the rollback arm's
  "lossless" bound to the staging rule (V11-C2); the **Abort row's
  else-branch** plus the general exit-row precedence rule — the
  demotion-disposition row wins over every exit row whenever the node is not
  the slot's serving primary at apply (V11-C1); **`kind` re-bound to the
  transition requested** — ~~plane-disagreement selects Demotion/Promotion~~
  (**superseded in V14**: there is no `Promotion` kind; `kind ∈ {Boot,
  Demotion}` and `Demotion` is stamped from the record, V14-C1),
  `Boot` only for an agreeing boot report — with the convergence paragraph's
  branch mechanics corrected: post-fence-refusal re-proposals admit under the
  *strict* branch (stored triple unchanged by refuse-whole), equality is
  drifted-topology re-proposal and idempotent re-delivery (V11-M1); the
  totality invariant's **still-a-member qualifier**, departed-target states
  discharging via case 3 (V11-m1); `Complete`'s residue initializer naming
  **all five fields** (V11-m2).
  From review v12: the **durable pending-transition record** — fsynced
  (file + parent dir, incarnation-counter discipline) *before* the staged
  flip's report proposes, carrying `{kind, target_upstream,
  ~~candidate_triple~~}` (**superseded in V14**: the record carries
  `stage_id: u64` and `adopted: bool`; the candidate triple is no longer an
  adoption operand, V14-M1); the destructive adoption re-derived **level-triggered**
  from *record present ∧ applied replicated role `Replica`* (crash
  re-derives at boot, idempotent completion, record cleared fsynced only
  after adoption durable), replacing v11's edge-triggered
  adoption-on-observed-apply (V12-C1); **`kind` stamped solely from the
  record** — `Demotion` iff a staged flip's record, ~~`Promotion` iff a
  pending-promotion record (`REPLICAOF NO ONE` fsyncs one before flipping)~~
  (**superseded in V14**: cluster mode has no node-originated promotion
  spelling, so the pending-promotion record and the `Promotion` kind are
  deleted outright, V14-C1/M5),
  `Boot` otherwise; never a bare plane comparison — killing the spurious
  `Promotion` in the post-admission/pre-adoption window (V12-C1); **role
  authority declared** — the replicated plane is authoritative for
  `role`/`primary_id`; the local plane originates transitions only through
  the record protocol; an unexplained disagreement converges
  replicated→local (TR-CLUSTER-033's direction, adopted and cited, with
  FM-CLUSTER-046's forcing tests inheriting the new arms) behind the
  **lineage guard** (adopting Primary requires local upstream `None` or
  in-shard — **widened in V13, then bound in V14** to
  `promoted_from`-adjudicated absence, V14-C2; the foreign-upstream bug state
  self-fences per §3
  defence-in-depth instead), then reports `Boot` — un-muting V12-M2's
  forever-refused node; **reconcile damping** — the candidate triple never
  enters the difference test (V12-m2), and while a record is pending the
  reconcile's only proposal is that record's own report — and only *while the
  stage is unresolved* (**refined in V14**: once
  `admitted_stage == Some(record.stage_id)` the reconcile proposes nothing,
  V14-m3) — (V12-C1); the new
  §3 **staged-flip fence row** — nothing held ever, uniform immediate
  `-TRYAGAIN` keyed on the record (no byte cap needed), the initiating
  client's deferred reply bounded by `cluster-staged-flip-reply-timeout`
  with a documented ambiguous outcome, the stage itself never timing out
  (V12-M1); the demotion-disposition row's vacuous staged-admission trigger
  removed (V12-m3); the candidate triple's declared durable home is the
  record (V12-m1).
  From review v13: the adoption's firing condition strengthened to the
  **four-operand binding** — record present ∧ replicated `role == Replica`
  ∧ `primary_id == Some(record.target_upstream)` ∧
  ~~`run_identity == Some(record's candidate triple)`~~ (**superseded in
  V14**: operand 4 is `admitted_stage == Some(record.stage_id)`, and a fifth
  operand `record.adopted == false` supplies boot idempotence — the candidate
  triple was re-minted at every boot and so could never be matched after a
  crash, V14-M1/M7) — so the destructive
  discard fires only on the admission of the record's *own* report, never
  on a concurrent failover's demotion (subsumes TR-CLUSTER-033's
  persisting-across-ticks damping, strictly stronger) (V13-C1); gate 3 (d)
  split into a ~~**total three-arm refusal partition**~~ (**superseded in
  V14**: the three arms were not total and keyed on prose rather than a
  declared fact; V14 keys six arms on the committed `RefusalClass`,
  V14-M2/M3) — refuse-whole while
  replicated-Primary → revert; fence refusal while replicated-Primary →
  record and fence persist, re-propose with fresh observations under the
  strict branch; `role != Primary` → superseded, record cleared,
  role-authority adoption — with "refused ∧ adoption fired" proven
  unreachable (V13-M6/C1); **`in_shard_parent(u, n)`** declared in §0 as
  the one-hop pre-apply pointer read, never a `shard_primary` walk
  (V13-M2); the lineage guard **widened and grounded** —
  ~~`u == None ∨ u ∉ nodes ∨ in_shard_parent(u, self)`~~ (**superseded in
  V14**: the bare `u ∉ nodes` disjunct admitted a foreign lineage; the
  disjunct is now `u ∉ nodes ∧ nodes[self].promoted_from == Some(u)`,
  V14-C2), the removed-upstream
  disjunct justified by TR-CLUSTER-042's outright removal (cited) and
  `FORGET` of a dead old primary, the universal restated over the design's
  transitions plus the LOCKED failover/removal rows (V13-M1); ~~the
  candidate triple **re-derived at every boot** against the fresh
  incarnation~~ (**superseded in V14**: re-derivation is what falsified
  operand 4 across a crash; the crash-durable adoption operand is
  `stage_id`, and the triple's only remaining role is ordering the report,
  V14-M1) — `{kind, target_upstream, stage_id, adopted}` are the
  crash-durable fields,
  the record is rewritten fsynced before re-proposing, the record's report
  is the boot report (V13-M3); ~~the **quiesced-replica arm** — replicated
  `role == Replica ∧ primary_id == None` adopts as detached-link,
  dataset-retained, serving per the demotion-disposition row, with the
  difference test matching detached to `None` (no proposal loop)~~
  (**superseded in V14**: with the writer enumeration closed, no writer
  produces `role == Replica ∧ primary_id == None`, so the arm is stated as
  **unreachable** — a Quint invariant and a mutation, not a live adoption
  path; §3's `-TRYAGAIN`-at-`primary_id == None` reply clause stays as
  reply-totality defence-in-depth, V14-M4) (V13-M4);
  the record's **single-writer rule** — a second staged command while a
  record is pending is refused with the pending-stage error, `CLUSTER
  INFO` is the read path (V13-M5, respelled in V14 to `CLUSTER REPLICATE` /
  `CLUSTER FAILOVER`); ~~the staged-flip fence row's **exempt
  set enumerated** (`CLUSTER` family only)~~ (**superseded in V14**: a
  whole-family exemption admitted `CLUSTER FLUSHSLOTS` and `CLUSTER SETSLOT`
  against the very dataset the stage exists to preserve; the set is now
  enumerated by member, V14-m4) (V13-m3); the
  `cluster-staged-flip-reply-timeout` config row added (V13-m1); record
  lifecycle across `ResetCluster`/`FORGET`/re-`MEET` declared — cleared
  fsynced before any re-mint or boot report (V13-m2).
  From review v14: the **cluster-mode role-command surface** declared from the
  implementation — `REPLICAOF`/`SLAVEOF` (every spelling, `NO ONE` included) are
  refused at dispatch whenever the node is in cluster mode, `CLUSTER REPLICATE`
  is the only demotion spelling and `CLUSTER FAILOVER` the only promotion
  spelling, so the `Promotion` kind, the pending-promotion record and the
  Promotion role-writer arm are **deleted** rather than repaired (V14-C1/M5/m1);
  `promoted_from: Option<NodeId>` as the lineage guard's third disjunct's
  binding — `u ∉ nodes ∧ promoted_from == Some(u)`, replacing the bare
  absent-upstream disjunct that admitted a foreign lineage (V14-C2);
  `synced: bool` + the `AttestReplicaSynced` transition + TR-CLUSTER-021/018's
  candidacy conjunct, so a replicated replica pointer written before a byte
  flowed is never a failover candidate (V14-C3); `stage_id: u64` on the record
  and `admitted_stage: Option<u64>` on `NodeInfo` — the crash-durable adoption
  operand replacing the per-boot-re-minted candidate triple — plus the record's
  `adopted: bool` and the **stage-resolution precedence rule** that makes a
  stale-duplicate refusal a no-op against a resolved stage (V14-M1/M7);
  `RefusalClass ∈ {proposer, membership, ordering, fence, upstream-validity,
  shard-relationship}` as a committed apply outcome and gate 3 (d)'s **six-class
  total partition** keyed on it (V14-M2/M3); the quiesced arm re-derived to
  **unreachability by writer enumeration** (V14-M4); the record's typed
  declaration and the whole-node fence with its **member-enumerated** exempt set
  (V14-m2/m4/m5); the reconcile guard restated at both passages — a pending
  record's stage, once resolved, proposes nothing (V14-m3).
  **Implementation note** (V14-C1): the cluster-mode `REPLICAOF`/`SLAVEOF`
  refusal is *existing implemented behaviour* —
  `frogdb-server/crates/server/src/commands/replication.rs` already returns
  `ERR REPLICAOF not allowed in cluster mode.` whenever the node has cluster
  state — so this design mandates **no new code** for that surface; it only
  writes the fact down where the gates and the LOCKED rows can cite it. The
  same holds for `CLUSTER FAILOVER`'s always-propose-a-`Failover`-op path
  (`commands/cluster/admin.rs`).
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
     **Durable attestation supersedes the regression as the primary defence**
     (V9-C2): the target's report action gains the guard that a proposed
     `applied_pos` is drawn from the model's durable set (the position survives
     `targetRestart` unchanged), so the stale-high state is unreachable through a
     lawful report — the replacement machinery and attesting-run conjunct are
     belt-and-braces. Mutation test: reverting the durable guard (letting the
     report propose an applied-but-volatile position) and then scheduling
     `targetRestart` while the target stays down through the leader
     auto-`Complete` must violate `inv_no_acked_write_lost` **even with the
     attesting-run conjunct intact** — V9-C2's exact trace: the conjunct is
     satisfied by the stale pre-crash cell because no new run has reported yet.
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
      release) while the flag holds (N-M1). **Plus the level form** (V8-M1,
      scope per V9-M2/V10-m2):
      invariant `inv_held_set_empty_while_latched` — in every state with the latch
      armed and the **barrier armed, sealed or not** (the latch covers the whole
      hold region, not only post-seal), the held set is empty; a write action
      scheduled in
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
      `completeMigration` — which initializes **every** field, `{source, target,
      promoted: false, source_gone: false, target_gone: false}` (V11-m2, matching
      the design text's full initializer) — mutated by
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
      V6-C3 proxy `ConfirmSlotDeleted` arm in the model too; **source arm only
      per V10-C4** — the model carries no target arm): level-triggered
      re-home of an entry whose `source` is no longer a primary, admissible
      whenever `shard_primary(entry.source)` is a live primary proposing for
      itself; reverting it, the in-apply demotion re-target, or the verb's
      admission must each violate the invariant. **Target-side totality is
      in-apply** (V10-C4): `demoteNode(n)` on a residue-named *target* re-targets
      the unpromoted entry in the same step; mutation test — reverting that
      in-apply target re-home (demoting a target and leaving the entry naming it)
      must violate `residueHasAnEffectiveRemover` (no level-triggered target
      re-home exists to recover, by design — the invariant is what proves none is
      needed when the in-apply rule is intact). **Plus `Complete`'s target-role
      conjunct** (V10-C3): `completeMigration` gains the guard
      `nodes[record.target].role == Primary`; mutation test — dropping the
      conjunct and scheduling `demoteNodeExternal(target)` mid-Draining, then
      `completeMigration`, must violate `inv_slots_only_assigned_to_primaries`
      (the slot lands on a Replica) — and with the conjunct intact the same
      schedule stalls at `Complete`, exiting via the observation bound.
      **Plus invariant
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
      asserts SS-2/SS-3 change only through the declared writer set — **per V9-M3/m3,
      narrowed by V14-C1, that set is: this action's Demotion arm, the
      failover transitions, `setRole`, `resetCluster`'s SOFT NodeInfo handling, and a fresh
      registration in `meetNode`/`AddNode`** — any other action touching
      `role`/`primary_id` must violate it (**re-based in ext-17**: the
      enumeration is the LOCKED SS-2/SS-3 cells, so it also contains
      `removeNode`'s `reparentChildren(.., None)` detach and the failover
      **sibling** re-parent — two writers this list omitted, which is why the
      property went green over V15-C1's hole). ~~The **Promotion arm** is modelled as a
      fenced role write (`role → Primary`, `primary_id → None`, no slot-map touch);
      mutation test: removing the arm and then scheduling a bare `REPLICAOF NO ONE`
      promotion must violate `residueHasAnEffectiveRemover` — the immortal-entry
      trace V9-M3 mirrors from V7-C1.~~ (**superseded in V14**, V14-C1/M5: cluster
      mode refuses `REPLICAOF` at dispatch, so no `REPLICAOF NO ONE` action exists
      in the model to promote a node and no `Promotion` arm is modelled; the
      residue-removal obligation V9-M3 protected rides the **failover transitions'**
      promotion writes instead, and `residueHasAnEffectiveRemover` is re-pointed at
      them — removing the re-target from the failover promotion is now the mutation
      that must falsify it.) The **Demotion arm's shard-relationship
      conjunct** (V9-C1) is modelled directly: mutation test — reverting it (deriving
      the re-home destination by a post-write `shard_primary` walk, or accepting a
      cross-shard `new_primary_id`) and demoting a slot-owning primary with a
      mis-targeted payload must violate `inv_slot_copy_survives_until_owned_and_served`
      (the cross-shard mis-home trace) or `inv_slots_only_assigned_to_primaries`.
      The **`clearSlotResidue` verb gains the `promoted == true` conjunct** (V9-M4);
      mutation test: reverting it and clearing a `promoted == false` entry must
      violate `inv_member_keyspace_is_tracked` — the source's still-authoritative
      copy loses its tracking entry while the rollback arm (its declared exit,
      which requires the entry) is still owed, the case-3 short-circuit V9-M4
      names.
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
      dropped). **Plus the staged flip** (V11-C2, mechanics per V12-C1): a
      per-node variable `pendingTransition: NodeId -> Option<{kind, upstream,
      candidateTriple}>` that **survives `crashRestart`** (it models the
      fsynced record; **retyped in ext-16** to
      `{kind, upstream, stageId, adopted}`, V14-M1/M7), and three actions — `stageReplicaof(n)` (guard: gate
      3's applied read; effect: write the record, fence, dataset variable
      untouched), `completeAdoption(n)` — **level-triggered**, guard is the
      ~~**four-operand binding** (V13-C1): *record present with kind Demotion
      ∧ replicated `role(n) == Replica` ∧ replicated
      `primaryId(n) == Some(record.upstream)` ∧ replicated
      `runIdentity(n) == Some(record.candidateTriple)`*~~ (**superseded in
      ext-16**: operand 4 becomes `admittedStage(n) == Some(record.stageId)`
      and a fifth operand `record.adopted == false` is added — the model's
      `crashRestart` re-mints the candidate triple, so the v13 operand made
      the guard unsatisfiable after a crash, V14-M1),
      schedulable at any later point including after a `crashRestart`, effect
      is the discard + upstream adoption + record clear — and the **refusal
      partition** (V13-M6) replacing the single revert action:
      `revertReplicaof(n)` (guard: refuse-whole refusal ∧ replicated
      `role(n) == Primary`; clears the record, node resumes),
      `retryStagedReport(n)` (guard: fence refusal ∧ replicated
      `role(n) == Primary`; record persists, re-proposes with fresh
      observations carrying the same candidate), and `supersedeStaged(n)`
      (guard: refusal ∧ replicated `role(n) != Primary`; clears the record,
      convergence via `adoptReplicatedRole`). `crashRestart` **re-derives
      the candidate** (V13-M3): the surviving record's `candidateTriple` is
      rewritten from the fresh boot incarnation before any re-proposal —
      **which is exactly why the triple cannot also be an adoption operand**
      (V14-M1; ext-16 moves that role to `stageId`, a field `crashRestart`
      leaves untouched, and keeps the re-derivation for report *ordering*).
      Mutation tests:
      collapsing stage and discard (discarding at stage time) with an
      `assignSlots` interleaved between gate check and report apply must
      violate `inv_no_acked_write_lost` (the wiped-source leg) or
      `inv_member_keyspace_is_tracked` (V11-C2); making `pendingTransition`
      volatile (cleared by `crashRestart`) and scheduling the crash between
      admission and adoption must violate `inv_member_keyspace_is_tracked` —
      the untracked-stale-copy trace, V12-C1; re-edging the adoption (firing
      only on the observed admission apply, so a crash loses it) must violate
      the same invariant; **weakening the guard to two operands** (record ∧
      role Replica) and interleaving a `Failover{force: false}` toward an
      in-shard successor while the staged report is in flight must violate
      `inv_member_keyspace_is_tracked` or `inv_no_acked_write_lost` — the
      keyspace wiped toward the record's upstream on someone else's
      admission, V13-C1's trace; **dropping the refusal partition** (reverting
      on a fence refusal) must violate `inv_no_acked_write_lost`
      (revert-then-admit re-opens the closed race); **skipping the boot
      re-derivation** (re-proposing the pre-crash candidate) must violate
      `inv_run_identity_never_regresses` (a stale-incarnation triple admitted
      over the boot mint); **allowing a record overwrite** (a second
      `stageReplicaof` toward a different upstream while pending) must
      violate `inv_member_keyspace_is_tracked` (adoption toward B on an
      admission for A). **Plus the staged-fence level rule** (V12-M1):
      invariant `inv_no_hold_during_staged_flip` — in every state with a
      pending record, that node's held set is empty and a write action
      scheduled there is answered, never held; reverting to holding and
      scheduling a partition must violate it. **Plus the
      kind-selection rule** (V12-C1, replacing V11-M1's plane-disagreement
      stamping): `reconcileIdentity` stamps `kind` from the
      `pendingTransition` record — `Demotion` iff the record
      names it, `Boot` otherwise (**narrowed in ext-16**: `kind ∈ {Boot,
      Demotion}`; the `Promotion` alternative is deleted with the arm,
      V14-C1) — and a new action
      `adoptReplicatedRole(n)` (guard: no record ∧ local role differs from
      replicated ∧ the lineage guard — local upstream `u` satisfies
      ~~`u == None ∨ u ∉ nodes ∨ in_shard_parent(u, n)`~~, V13-M1/M2;
      **retightened in ext-16** to
      `u == None ∨ in_shard_parent(u, n) ∨ (u ∉ nodes ∧ promotedFrom(n) ==
      Some(u))`, V14-C2)
      converges unexplained splits replicated→local before the `Boot`
      report proposes (TR-CLUSTER-033 modelled); ~~its **quiesced arm**
      (V13-M4) covers replicated `role == Replica ∧ primaryId == None` — a
      local `detached` upstream state that the difference test counts as
      matching `None`, so the reconcile proposes nothing there~~
      (**superseded in ext-16**: that state has no writer, so the model
      asserts its unreachability instead of covering it, V14-M4)
      (**re-corrected in ext-17**: the state *does* have a writer —
      `removeNode`'s detach — so the struck text's *behaviour* was right all
      along: at a detached replica the difference test counts the local
      `detached` upstream as matching replicated `None` and the reconcile
      proposes nothing. What was wrong in ext-15 was calling it a **report
      arm**; in ext-17 it is a declared **state** with the same silence,
      V15-C3). Mutation
      tests — each must
      fail its named property: ~~stamping from bare plane disagreement (the
      v11 rule) and scheduling `reconcileIdentity` inside the
      post-admission/pre-adoption window must violate
      `inv_member_keyspace_is_tracked` or `inv_no_acked_write_lost` (the
      spurious-`Promotion` re-opens the assignee door SS-11 closed —
      V12-C1's trace)~~ (**superseded in ext-16**: with no `Promotion`
      kind the spurious stamp is not expressible; the surviving mutation of
      the same shape is ext-16's mutation (1) — re-adding the stage-free
      binding so an unrelated demotion apply fires the adoption, V14-C1/M1);
      removing `adoptReplicatedRole` and booting a node
      crashed before its failover promotion took local effect must make the
      bounded witness `witnessSplitPlanesConverge` unreachable (the node
      refuses forever on the Demotion arm and sits mute — V12-M2's trace);
      dropping the lineage guard and injecting the plane-split bug state
      must violate `inv_no_acked_write_lost` (Primary adopted over a
      foreign-lineage dataset); **narrowing the lineage guard** to
      `u == None ∨ in_shard_parent(u, n)` (dropping the removed-upstream
      disjunct entirely) and
      booting a node whose forced failover removed its old upstream
      (TR-CLUSTER-042) must make `witnessSplitPlanesConverge` unreachable —
      V12-M2's mute node re-created, V13-M1's trace; ~~**removing the
      quiesced arm** and driving a successor-less demotion must make the
      reconcile propose in every observation cycle from that state — the
      permanent proposal loop falsifying the witness's silence, V13-M4~~
      (**superseded in ext-16** by mutation (6): the state is unreachable,
      so the property is an unreachability invariant and the mutation is
      *re-adding* a `None`-writing demotion, V14-M4)
      (**restored, re-aimed, in ext-17**: the state is reachable via the
      detach, ext-16's mutation (6) is deleted with its property, and the
      honest mutation is **dropping the difference test's
      detached-matches-`None` equivalence** and scheduling
      `reconcileIdentity` at a detached replica — the permanent proposal loop
      must falsify `witnessSplitPlanesConverge`'s silence, V13-M4's original
      trace now aimed at a state that exists, V15-C3).
      `witnessSplitPlanesConverge` itself is
      retained: **no reachable state keeps a
      node self-fenced with a stable local/replicated role disagreement** —
      convergence now runs through the pending record's report or the
      adoption + `Boot` path, per direction. Its bounded runs no longer
      claim to **cover the successor-less demotion state** (V14-M4 — that
      claim was false the moment the state became unreachable); that state
      is discharged by ext-16's `inv_no_replica_without_a_primary_pointer`
      instead (**re-corrected in ext-17**: the state is *reachable* — it is
      the detached replica — so neither the witness's coverage claim nor
      ext-16's unreachability invariant was right; the declared state plus
      `inv_member_keyspace_is_tracked`'s detached-replica arm is what covers
      it, V15-C3). **Plus the exit-row precedence**
      (V11-C1): held-set release actions (`abortHandoff`, `cancelMigration`)
      carry the serving-primary guard; mutation test — dropping it from
      *either* action and scheduling it after a demotion apply must violate
      `inv_no_execution_after_demotion` (held writes execute on a demoted
      source: forked history).
  16. **The v14 structural corrections** (V14-C1/C2/C3/M1/M3/M4/M7), stated as
      one extension because they are one mechanism: the stage as the adoption's
      crash-durable identity, and the three replicated adjudication stamps.
      *State*: `pendingTransition` is retyped
      `{kind: Boot | Demotion, upstream: NodeId, stageId: int, adopted: bool}`
      — no `candidateTriple` operand, no `Promotion` alternative — and survives
      `crashRestart` **field-for-field** (`stageId` and `adopted` are the fields
      the model must not touch at boot; the candidate triple, still re-minted,
      moves out of the record's adoption role and into report ordering only).
      Three replicated per-node variables join `role`/`primaryId`:
      `admittedStage: NodeId -> Option[int]`, `promotedFrom: NodeId ->
      Option[NodeId]`, `synced: NodeId -> bool`. Every writer of `role` in the
      declared writer set writes all three in the same step — the model's form
      of "the companion field is written by whoever writes the governed fact"
      (**re-keyed in ext-17**: the writer set is `primaryId`'s, not `role`'s —
      keying it on `role` is precisely what let the two LOCKED pointer-only
      writers escape the rule, V15-C1).
      *Actions*: `reportRunIdentity`'s Demotion arm writes
      `admittedStage(n) = Some(payload.stageId)`, `synced(n) = false`,
      `promotedFrom(n) = None`; the failover promote arm writes
      `promotedFrom(p) = Some(oldPrimary)`, `synced(p) = true`,
      `admittedStage(p) = None`; `setRole` writes `synced = false`,
      `promotedFrom = None`, `admittedStage = None`; a new action
      `attestReplicaSynced(n)` (guard: `n ∈ nodes ∧ role(n) == Replica ∧`
      the model's link-caught-up flag; effect: `synced(n) = true`) is the only
      other `true` writer (**fenced in ext-17**: the action mints an
      observed-parent/epoch pair and its apply guard re-checks it, so a
      re-parent interleaved between mint and apply refuses it). `completeAdoption(n)`'s guard becomes *record present
      ∧ `record.adopted == false` ∧ `role(n) == Replica` ∧
      `primaryId(n) == Some(record.upstream)` ∧ `admittedStage(n) ==
      Some(record.stageId)`*, and its effect orders discard+link, then
      `record.adopted = true`, then record clear — the three-step the crash
      schedule must be able to interrupt at each point.
      *Refusal class*: `reportRunIdentity`'s apply yields
      `refused(class)` with `class` the first failing conjunct in the declared
      order, and the model's disposition actions (`revertReplicaof`,
      `retryStagedReport`, `supersedeStaged`, plus the membership-exit and
      ordering-no-op arms) are guarded on that value rather than on a modeller's
      paraphrase — with the **stage-resolution precedence rule** as a guard
      conjunct: a `refused(ordering)` arriving while
      `admittedStage(n) == Some(record.stageId)` is a no-op.
      *Failover candidacy*: the failover action's candidate guard gains
      `synced(c) == true`, bypassed only by `force: true`.
      *Properties*: `inv_role_written_only_by_declared_writers` extends to the
      three companion fields (a writer touching `role` without them must
      violate it — **re-based on `primaryId` in ext-17**, the `role`-keyed
      form being green over both of V15-C1's writers); new
      `inv_no_replica_without_a_primary_pointer` — no reachable
      state has `role(n) == Replica ∧ primaryId(n) == None` — is the V13-M4
      quiesced shape's replacement, an unreachability claim rather than a
      covered arm (**withdrawn in ext-17**: the property is **false of the
      LOCKED system** — `removeNode`'s `reparentChildren(.., None)` writes
      exactly that state, TR-CLUSTER-003/FM-CLUSTER-002 — and a model carrying
      it would have gone green only by omitting the detach writer, V15-C3);
      `inv_no_acked_write_lost` and
      `inv_member_keyspace_is_tracked` are the traces' targets as before.
      *Mutations, each named with the property it must falsify*:
      (1) **stage-free binding** — revert `completeAdoption`'s guard to the
      two-operand form (record ∧ `role == Replica`, no `admittedStage`) and
      interleave a `Failover{force: false}` demotion of the staged node while
      its report is in flight → `inv_member_keyspace_is_tracked` /
      `inv_no_acked_write_lost` (V13-C1's trace, revived);
      (2) **unspecific lineage disjunct** — replace the guard's third disjunct
      with bare `u ∉ nodes`, then `setRole` a slotless replica onto a foreign
      primary, `removeNode` that primary, and drive `adoptReplicatedRole` →
      `inv_no_acked_write_lost` (foreign-lineage substitution, V14-C2);
      (3) **no synced gate** — drop `synced(c) == true` from the candidate
      guard, stage a replica pointer with `attestReplicaSynced` never scheduled,
      and fail the primary → `inv_no_acked_write_lost` (whole-shard
      substitution by an empty candidate, V14-C3);
      (4) **stage re-mint at boot** — let `crashRestart` re-mint `stageId` the
      way it re-mints the candidate triple, crash between admission and
      adoption → `completeAdoption` is never again enabled;
      `witnessStagedFlipCompletesAcrossCrash` (new bounded witness: a staged
      flip whose adoption fires only after a `crashRestart`) becomes
      unreachable (V14-M1);
      (5) **no precedence rule** — drop the `admittedStage` conjunct from the
      `refused(ordering)` disposition, deliver a pre-crash duplicate report
      after the boot proposal was admitted → the live stage is cleared and
      `witnessStagedFlipCompletesAcrossCrash` becomes unreachable (V14-M7);
      (6) **`None`-writing demotion re-added** — give any writer a
      `primaryId = None` demotion write → `inv_no_replica_without_a_primary_pointer`
      (V14-M4; this is the mutation that keeps the deleted quiesced arm honest)
      (**deleted in ext-17** with the property it targeted: the state is
      lawful, so writing it is not a bug and the mutation had no signal —
      ext-17's mutation (10) is its honest replacement).
  17. **The v15 corrections** (V15-C1/C2/C3/C4/M1/M2/M3), again one mechanism:
      the companion rule keyed on the **parent pointer**, the adoption
      collapsed to **one selector**, and the two LOCKED writers revision 14
      never modelled.
      *State/actions*: the model gains `removeNode`'s
      `reparentChildren(.., None)` **detach** and the failover
      **sibling re-parent** `reparentChildren(.., Some(newPrimary))` as
      first-class actions — without them the model cannot even express the
      defects. Both write the three companions (`synced = false`,
      `admittedStage = None`, `promotedFrom = None`) exactly as every other
      `primaryId` writer does. `addNode`'s upsert writes no `primaryId` and
      preserves all three (V15-M2). `attestReplicaSynced(n)` carries
      `observedPrimary`/`observedEpoch` minted from applied state, re-checked
      in its apply guard (V15-C4), and its trigger flag is set by **either**
      resync shape (V15-M3). `completeAdoption(n)`'s guard drops to the
      **single selector** `record present ∧ record.adopted == false ∧
      admittedStage(n) == Some(record.stageId)` — the role and pointer
      operands are theorems of the companion rule, not guards (V15-C2) — and
      the reconcile action's guard is the same predicate negated on its last
      conjunct, so the two can never both be enabled. Every disposition arm
      gains `refusedPayload.stageId == Some(record.stageId)` (V15-M1).
      *Properties*: `inv_role_written_only_by_declared_writers` is **re-based
      on `primaryId`** — every step that changes `primaryId(n)` comes from the
      declared writer set **and writes all three companions in the same step**
      (this is the property whose `role`-keyed form went green over revision
      14's two holes); `inv_no_replica_without_a_primary_pointer` is
      **deleted**, its intent re-provided by `inv_member_keyspace_is_tracked`
      quantifying over the **detached-replica** arm; `inv_no_acked_write_lost`
      remains the traces' target.
      *Mutations, each with the property it must falsify*:
      (7) **sibling stamp drop** — let the sibling re-parent write `primaryId`
      alone, then run a failover of the shard followed by a staged flip at a
      sibling → `inv_no_acked_write_lost` (a sibling's stale `admittedStage`
      satisfies a later record's operand and fires the destructive discard on
      someone else's adjudication, V15-C1); the same mutation applied to the
      **detach** action must falsify the re-based writer property directly.
      (8) **attestation fence drop** — remove the observed-pair conjunct and
      interleave a re-parent between mint and apply →
      `inv_no_acked_write_lost` (a `synced` stamp about the old parent makes a
      zero-byte candidate promotable, V15-C4).
      (9) **stage-unbound refusal** — drop the
      `refusedPayload.stageId == Some(record.stageId)` conjunct and deliver a
      refusal minted for a cleared stage while a second stage is live →
      `witnessStagedFlipCompletesAcrossCrash` unreachable (the live record is
      cleared by the dead stage's refusal, V15-M1).
      (10) **detach unmodelled** — remove the detach action (revision 14's
      state of the model) → every property still holds, which is the point:
      the mutation's *pass* is the signal, and it is asserted as a
      **negative** check in the rework section (a model that cannot express a
      LOCKED writer proves nothing about it, V15-C3).
      (11) **full-sync-only trigger** — restrict `attestReplicaSynced`'s
      trigger to the full-sync shape, then re-parent a caught-up replica by
      partial resync and fail its primary → the bounded witness
      `witnessResyncedReplicaBecomesCandidate` (new) is unreachable: the
      liveness hole V15-M3 found.
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
  applier of a snapshot ever observes), and (f) every **meaning-qualifier** — an
  adverb or adjective doing load-bearing work inside a conjunct or claim
  ("durably", "refuses", "validated", "proven") must be bound to a declared rule,
  gate, or class stated in the document, not left as prose whose reader supplies
  the binding — and for each verify the field with a
  declared type exists and name
  the component read — a name merely appearing somewhere in the document does not
  count** (V4 audit note, strengthened per V5, V6, V7, V8, V9, V10, V11, V12,
  V13, and again per V14:
  **twelve
  consecutive
  rounds each produced instances of exactly this class** — N-C4, V4-C3, V5-C2,
  V6-C1/C2, V7-C1/C2, V8-C2/C6 (an unfenced arm selector and an
  edge-triggered reaction — items (d) and (e) exactly, found one round after
  those items were added to the check), then V9-C1/C2 (an unvalidated re-home
  destination read post-write instead of proven pre-apply, and a conjunct whose
  operand's *meaning* — "durable" — was never bound to a declared durability
  class, so the conjunct stayed satisfiable by a stale cell), then V10-C1/C2/C4
  (three meaning-qualifiers at once — "refuses" with no node-local gate carrying
  the refusal to the command path, "durably" unbound a *second* time on the
  replica side of the same floor, and a target-arm walk asserted safe by analogy
  to a proven source-side argument that did not transfer — item (f) exactly,
  added this round), then V11-C1/C2 (both **propagation misses of the previous
  round's own fix text** — an else-branch added to one of two sibling
  execute-at-source exit rows but not the other, and a declared race window
  whose data-plane consequence made "lossless" false downstream while no
  downstream rule heard of it — the diff-scoped rule's exact target, found one
  round after that rule was adopted), then V12-C1/M1 (the **third consecutive
  round of fix-text defects**: v11's staged flip left its adoption
  edge-triggered on the observed admission apply — item (e) — and its
  reconcile read the same plane-disagreement predicate the kind rule read, in
  the opposite direction, with no guard selecting which rule owned the state —
  item (b) — while colliding uncited with TR-CLUSTER-033's LOCKED
  opposite-direction reconciler; and V12-M1's "holding/`-TRYAGAIN`-ing per §3"
  was an undetermined disjunction — a meaning-qualifier, item (f), whose
  reader had to supply which disposition applied and against which §3 row),
  then V13-C1/M6/M2/M3 (the **fourth consecutive round of fix-text
  defects**, all in v12's own staging text: the adoption guard's two
  operands under-bound the trigger to its cause — item (a): "record present
  ∧ replicated `Replica`" fired on *any* writer of `Replica`, not the
  record's own admitted report; the fence-refusal disposition was an
  undeclared partition — item (b): (d)'s unqualified revert and the
  convergence paragraph's fence-retry both claimed the same state with no
  selector; "in-shard member" was a load-bearing name with no declared
  predicate — items (a)/(f); and the crash-durable candidate triple was an
  incarnation-bound value surviving into the next incarnation — item (e)'s
  shape carried by identity instead of a trigger), then V14-C1/C2/C3/M1/M3
  (the **fifth consecutive round of fix-text defects**, and the round in which
  the doc's own items (a)-(f) failed **four of six** over v13's diff: the
  adoption's fourth operand was an *un-typed relation to an ungrounded
  command surface* — v13 wrote gates for `REPLICAOF` spellings the server
  refuses at dispatch, so a whole `Promotion` arm, a pending-promotion record
  and a role-writer entry described a path no client can take, item (a)
  against the *implementation* rather than the document; the lineage guard's
  removed-upstream disjunct was a bare membership test standing in for an
  adjudication no field carried — item (a) again, and the fix is a declared
  field, `promoted_from`; the staged flip published a replicated replica
  pointer before any data moved and nothing anywhere recorded
  data-possession — item (a) on a *missing* operand, fixed by `synced`; v13's
  own crash-durability fix re-minted the operand it made load-bearing —
  item (e) exactly, one round after item (e) caught the same shape; and the
  refusal partition was three prose arms where six declared classes were
  needed — items (b)/(d), fixed by making `RefusalClass` a committed apply
  outcome), then V15-C1/C2/C3/C4 (the **sixth consecutive round of
  fix-text defects**, and the sharpest lesson yet about *what* the check is
  run against: all four CRITICALs were in v14's own stamp machinery, and
  three of them were **joins the document never performed against the LOCKED
  spec** — item (f) applied to `specs/cluster.md` rather than to the doc's
  own prose. v14 wrote its "closed writer enumeration" from its own
  paragraphs and it was short by two LOCKED writers (V15-C1); it declared a
  state unreachable that a LOCKED failure-mode row specifies and a LOCKED
  forcing test exercises (V15-C3); and it fenced its Demotion payload against
  concurrent topology change while leaving the *other* new transition it
  introduced in the same revision, `AttestReplicaSynced`, unfenced against
  the same race — item (b), the two siblings again (V15-C4). V15-C2 is
  item (b) in its pure form once more: after a re-parent the adoption's
  operand list and the reconcile guard both claimed the same state, with
  nothing selecting an owner. **Process consequence, adopted this round**:
  item (f) is executed as a *literal join* — for every replicated field the
  design reads or writes, the LOCKED `Writer(s)` cell is quoted verbatim and
  each writer marked carried or amended — and that join is a **permanent
  subsection of §0**, not a per-round chore, because a prose restatement of a
  LOCKED list is precisely what failed here) —
  so the check must
  bind to declared
  types, not names, and must cover
  guards, origins, arm selectors, edge triggers, and meaning-qualifiers, not just
  comparison operands. **And the check is diff-scoped per revision** (V10's
  process lesson): a round's *fix text* is text like any other — three of v10's
  four CRITICALs lived in text v9 introduced — so each revision runs the
  mechanical check over its own diff before dispatch, not only over the
  original body).
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
- Quint: the sixteen extensions above, mutation-validated against their named findings
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
  `identity_seq`, no regression, no spurious cancel (V4-M4); demotion via
  ~~bare `REPLICAOF`~~ `CLUSTER REPLICATE` (**re-spelled in V14**, V14-C1)
  mid-migration as source: identity change cancels (V4-M4a); leader churn
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
  never executed (V8-M4). From review v9: mis-targeted ~~`REPLICAOF`~~
  `CLUSTER REPLICATE`/`SetRole` (**re-spelled in V14**, V14-C1) demotion of a
  slot-owning primary — `new_primary_id` names a cross-shard node or a non-replica:
  the report **refuses whole**, no slot re-homed, no data lost, replicated state
  keeps the node Primary (the stated availability-loss outcome), and the
  `AssignSlots{accept_data_loss: true}`-first path then admits the demotion
  (V9-C1); target power-loss *before* `Complete` under relaxed configured
  durability: every attested position is fsync-durable, the restarted run resumes
  at or above the attested floor, and `Complete` — including the leader
  auto-`Complete` while the target stays down — never lands above what the disk
  holds; reverting the durable-attestation gate loses acked writes even with
  V8-C3's conjunct intact (V9-C2); target demoted at `promoted == false`:
  the demotion's **own apply** re-targets the entry to the validated successor
  (V10-C4's in-apply totality — V9-M1's target arm is removed; the outcome, not
  the mechanism, survives),
  which finishes ingest and promotes — no wedge, no rollback forced (V9-M1); write
  arriving post-flush while the latch is still armed on an *unsealed* barrier slot:
  answered `-TRYAGAIN` immediately, never held for the remaining partition
  (V9-M2); ~~bare `REPLICAOF NO ONE` on a replica holding a residue entry as source:
  the Promotion arm's fenced role write lands via the split-rule re-proposal
  (topology-only at equality — V10-M1 revises V9-M3's re-mint), the reaper's
  role guard opens, the entry drains~~ (**superseded in V14**, V14-C1: cluster
  mode refuses `REPLICAOF NO ONE` at dispatch and there is no Promotion arm; the
  replacement scenario is **`CLUSTER FAILOVER TAKEOVER`** on that replica — the
  failover transition's promotion write opens the reaper's role guard and the
  entry drains, the same obligation carried by the only spelling that exists)
  (V9-M3); `ClearSlotResidue` against a
  `promoted == false` entry: refused, the rollback arm remains the only exit
  (V9-M4); eviction pressure and active expiry against keys in a sealed slot:
  both suspended — no mutation lands past the seal, eviction victims come from
  other slots, lazy expiry on reads still answers correctly (V9-m4); joiner with
  empty keyspace but non-empty local Raft state (and the converse): each refused
  by the composed TR-CLUSTER-005 precondition with the refusal naming the failing
  gate; `CLUSTER RESET HARD` then admits (V9-M5). From review v10: ~~bare
  `REPLICAOF <other>`~~ `CLUSTER REPLICATE <other>` (**re-spelled in V14**,
  V14-C1 — the `REPLICAOF` spelling gets the dispatch refusal instead, its own
  v14 scenario below) issued to a node that owns slots or is residue-named as
  source: **refused node-locally by gate 3** with the error naming the owned
  slots/entries, replicated state untouched, the migration continues (V10-C1);
  ~~the gate race forced (command wins locally, Demotion report then refused
  whole)~~ (**superseded in V11**: the staged flip makes the race state
  unreachable through the spec path — the V11 test block below replaces this
  scenario with the staged-refusal revert; the self-fence answer survives only
  as the defence-in-depth assertion against an injected plane-split state)
  (V10-C1); a counted replica configured `relaxed` durability:
  its ack is withheld until its shadow store is fsynced through the batch —
  kill -9 of every counted replica immediately after their acks, then target
  failover, loses nothing below the acked floor; reverting the replica-side
  fsync rule loses acked writes with the knob on (V10-C2); target demoted
  mid-`Draining` with the knob off: `Complete` — including leader
  auto-`Complete` — stalls on the role conjunct, the migration exits via the
  observation bound, and **no residue entry naming a Replica target is ever
  created**; dropping the conjunct in the model violates
  `inv_slots_only_assigned_to_primaries` (V10-C3); model mutation — reverting
  the in-apply target re-home (demote a residue-named target, leave the entry)
  violates `residueHasAnEffectiveRemover`, proving the removed target arm is
  not needed rather than merely absent (V10-C4); a topology-only re-proposal
  (equal triple, changed role facts) admitted mid-`Draining`: **no migration
  cancels** — the cancel binds to the field write; a genuine bump (strict
  ordering) still cancels the node's sourced migrations (V10-M1); replica
  added to the target's shard after acks accrue: `target_replicas_acked_pos`
  clears, `Complete` waits for the *new* counted set's floor — and
  symmetrically on removal (V10-M2); `ResetCluster` re-mint carries
  `kind = Boot` and drives no demotion/cancel arm (V10-m4); each declared
  absence test (`Prepare`'s `attempt_id == None`, `Begin`'s two map absences,
  `Complete`'s `Some(token)` value read) exercised against both present and
  absent states (V10-m6); read of an expired key in a sealed slot: answers
  absent, emits no `DEL` on the migration stream, and source/target state
  converge after promotion — the seal's feed-visible quantification holds
  (V10-m7); `ClearSlotResidue` against a `target_gone == true` entry: refused
  unconditionally even with `accept_stale_copy`; the orphan re-home arm is the
  only admitted exit (V10-m8). From review v11: `AssignSlots` lands between a
  passing gate-3 check and the Demotion report's apply — the report **refuses
  whole** (shard-relationship conjunct), the node **reverts with its dataset
  intact** and resumes serving the newly-assigned slot; no `-TRYAGAIN` wedge,
  no wiped keyspace, the client's `CLUSTER REPLICATE` answers the refusal error
  (V11-C2, re-spelled V14-C1); ~~crash between stage and admission: the node boots with its
  durable role unflipped (adoption never ran), planes agree, no stranded
  state~~ (**superseded in V12**: the stage now writes the durable
  pending-transition record first, so the booted node is *staged, not clean* —
  the V12 test block below replaces this scenario with the
  re-propose-from-record boot) (V11-C2/M1); the staged report **admits**: held
  set answered per the staged-flip fence row, adoption proceeds only after the
  same apply re-homed slots and residue (V11-C2); `AbortSlotHandoff` applying after a
  failover demoted the source: held set answered per the new role, **never
  executed** — same assertion as the Cancel-row test, now on the Abort exit;
  model mutation drops the serving-primary guard from either release action →
  `inv_no_execution_after_demotion` violated (V11-C1); ~~crash in a forced
  plane-split state (bug injection): the booted node's reconcile stamps
  `kind = Demotion` from the disagreement — not `Boot` — and the divergence
  converges via the strict branch~~ (**superseded in V12**: kind is stamped
  from the pending-transition record, never a bare plane comparison; an
  unexplained split converges replicated→local via the lineage-guarded
  adoption then a `Boot` report — the V12 test block below carries the
  replacement scenarios) (V11-M1); removal-with-no-successor
  of an unpromoted entry's target: the entry lawfully names the departed
  non-member target, the totality invariant's member qualifier excludes it, and
  the rollback arm discharges the state (V11-m1); a fresh residue entry read
  immediately after `Complete`'s apply: both `source_gone` and `target_gone`
  are `false`, never unset (V11-m2). From review v12: reconcile tick during
  the post-admission/pre-adoption window: proposes **nothing** — while the
  pending-transition record exists the reconcile's only proposal is that
  record's own report, and the report is already admitted (**V14-m3 sharpens
  the assertion**: the stage reads *resolved*, `admitted_stage ==
  Some(record.stage_id)`, which is the declared reason the reconcile proposes
  nothing); no spurious
  `Promotion` — unstampable since V14-C1 deleted the kind — and no re-opened
  assignee door (V12-C1); crash between admission
  and adoption: the booted node re-derives the level-triggered adoption from
  its durable record — replicated role already `Replica` — completes the
  discard + upstream adoption, clears the record, and at no point reports
  `Promotion` (unstampable since V14-C1) or holds an untracked stale copy —
  **and the adoption operand this scenario turns on is `admitted_stage`, not
  the re-minted candidate triple** (V14-M1) (V12-C1); ~~crash between stage
  and admission: the node boots *staged* — record present, fence up per the
  staged-flip fence row — and re-proposes the record's own candidate triple~~
  (**superseded in V13**: the candidate is re-derived at every boot against
  the fresh incarnation — the V13 test block below carries the replacement
  scenario) (V12-C1/m1); node crashed
  after a failover promoted it in replicated state but before any local
  effect: boots locally-Replica/replicated-Primary with **no** pending
  record, the lineage guard passes (upstream `None`, removed from `nodes`,
  or an in-shard parent), the node
  adopts `Primary` locally, its boot report stamps `kind = Boot`, admission
  proceeds — never the mute forever-refusing Demotion arm (V12-M2); same
  boot with a **foreign** local upstream (injected plane-split bug state):
  adoption refused by the lineage guard, node self-fences per §3
  defence-in-depth, serves nothing stale (V12-M2 boundary); staged flip
  under partition: writes arriving at the staged node answered `-TRYAGAIN`
  **immediately** — nothing held — and the initiating client's deferred
  reply lands by `cluster-staged-flip-reply-timeout` with the documented
  ambiguous-outcome error; the stage itself never times out, and a later
  heal resolves it via admission or refusal (V12-M1); ~~`REPLICAOF NO ONE`
  crash after the pending-promotion record fsyncs but before the Promotion
  report admits: the booted node re-proposes from the record — never
  silently self-demotes under role authority~~ (**superseded in V14**,
  V14-C1/M5: there is no `REPLICAOF NO ONE` path in cluster mode, no
  pending-promotion record and no Promotion report; the property it protected —
  a booted node never silently self-demotes under role authority — is carried
  by the lineage-guarded `adoptReplicatedRole` scenario above and by the v14
  block's dispatch-refusal scenario) (V12-C1); failover-driven
  history adoption reporting `kind = Boot`: the report's identity **field
  write** still cancels the node's sourced migrations — the cancel binds to
  the write, not the arm (V12-m3/V10-M1). From review v13: a
  `Failover{force: false}` toward an in-shard successor admits while the
  staged Demotion report is in flight: the adoption condition stays
  false (`primary_id` names the successor, not the record's upstream; and
  `admitted_stage` is unchanged by the failover's apply — **V14-M1 re-bases
  this scenario off `run_identity`**, whose per-boot re-mint made the v13
  operand unusable across a crash) — **no
  adoption fires, the keyspace survives**, the staged report's refusal
  routes to arm 3 (superseded), the record clears, and the node converges
  to the failover's verdict via role-authority adoption (V13-C1); a node
  promoted by `Failover{force: true}` crashes before local effect and
  boots with its old upstream **absent from `nodes`**: the lineage guard
  passes on the removed-upstream disjunct, the node adopts `Primary`,
  reports `Boot` — no mute node (V13-M1, now additionally asserting the
  guard's `promoted_from == Some(old_primary)` binding: the same boot with
  `promoted_from == None` or naming a *different* removed node self-fences,
  V14-C2); ~~a demotion admitted with
  `new_primary_id == None`: the node adopts the **quiesced-replica** state
  — link detached, dataset retained, everything answered per the
  demotion-disposition row — and the reconcile proposes **nothing** from
  that state (difference test matches detached to `None`): no proposal
  loop~~ (**superseded in V14**, V14-M4: no writer produces that state, so the
  scenario is replaced by the v14 block's unreachability assertion plus the
  injected-bug-state self-fence assertion) (**re-corrected in V15**, V15-C3:
  a writer does produce it — `RemoveNode`'s detach — so the v14 replacement
  is withdrawn and the v15 block's **detached-replica** scenarios carry the
  behaviour, including the "proposes nothing" half the struck text had right;
  what stays superseded is the *demotion admitted with `new_primary_id ==
  None`* premise, which V14-C1 made unreachable at the payload) (V13-M4); a second `CLUSTER REPLICATE` toward a different upstream
  while a record is pending: refused with the pending-stage error, the
  record's fields unchanged, `CLUSTER INFO` reports the pending stage
  (V13-M5); a staged report refused by the fence while the node is still
  replicated-Primary after a concurrent epoch bump: record and fence
  **persist**, the re-proposal carries the same candidate with fresh
  observations and admits under the strict branch (V13-M6 arm 2); a staged
  report refused whole (shard-relationship conjunct): revert with the
  dataset intact — same assertion as the V11-C2 test, now asserting the
  record cleared and the candidate discarded (V13-M6 arm 1); crash between
  stage and admission: the node boots staged, **re-mints the candidate
  under the boot's fresh incarnation**, rewrites the record fsynced, and
  the record's report *is* the boot report — the admitted triple carries
  the boot incarnation, never the pre-crash one; **and the scenario now also
  asserts `stage_id` is *not* re-minted** (V14-M1 — the record's `stage_id`
  and `adopted` survive the boot byte-for-byte, which is what lets the
  adoption fire at all after a crash) (V13-M3); ~~commands at a
  staged node: everything `-TRYAGAIN` immediately except the `CLUSTER`
  family~~ (**superseded in V14**, V14-m4: the exemption is enumerated by
  member, so the scenario is replaced by the v14 block's fence-exempt-set
  scenario, which asserts `CLUSTER FLUSHSLOTS` is *not* exempt) —
  `CLUSTER INFO` reads the pending stage, a retried
  `CLUSTER REPLICATE` receives the pending-stage refusal, not `-TRYAGAIN`
  (V13-m3); `ResetCluster` (both paths) and `FORGET`-of-self with a
  pending record present: the record is cleared, fsynced, before the
  re-mint/boot report — the first post-reset report is `Boot` with no
  competing staged claim (V13-m2). From review v14: **`REPLICAOF`/`SLAVEOF`
  in cluster mode**, every spelling including `NO ONE`, against a node with
  slots, without slots, and already a replica — each answers
  `ERR REPLICAOF not allowed in cluster mode.`, never reaches gate 3, never
  writes a pending record, and leaves replicated state untouched; the
  regression is a pin on existing behaviour
  (`replicaof_refused_in_cluster_mode_every_spelling`, V14-C1/M5/m1);
  **`CLUSTER REPLICATE <node-id>` naming a non-member** (never joined, or
  `FORGET`-ten between the client's read and the command): refused at gate 3's
  upstream pre-check *before* any record is written — no fence, no fsync, no
  proposal — and the symmetric replicated conjunct refuses a report whose
  `new_primary_id` left `nodes` between fsync and apply, routing to the
  `upstream-validity` class and arm 1's revert; **the record is never
  immortal** (V14-C1's trace: the v13 shape could satisfy neither adoption nor
  any exit) (`cluster_replicate_nonmember_refused_before_record`);
  **staged flip crash-reboot completes adoption**: crash after admission, boot,
  and the level-triggered adoption fires on
  `admitted_stage == Some(record.stage_id)` **notwithstanding the candidate
  triple's re-mint** — the v13 four-operand form is asserted *unsatisfiable* in
  the same test as a negative control (V14-M1);
  **crash after adoption durable, before record clear**: boot finds
  `record.adopted == true`, clears the record, and performs **no second
  discard** — the idempotence the `adopted` flag buys, asserted by a keyspace
  digest taken before the crash and after the boot (V14-M7);
  **pre-crash duplicate report delivered after the boot proposal admitted**:
  refused `ordering`, and the stage-resolution precedence rule makes it a
  **no-op** — record intact, fence state unchanged, adoption still pending or
  already done; reverting the precedence rule clears a live stage and strands
  the node (V14-M7);
  **`fence`-class refusal**: the report is refused on `observed_config_epoch`
  after a concurrent bump; the record and the whole-node fence both **persist**,
  `CLUSTER FLUSHSLOTS` at the staged node still answers `-TRYAGAIN` (it is not
  in the enumerated exempt set) while `CLUSTER INFO` answers the pending stage,
  and the re-proposal admits under the strict branch (V14-M2/M3/m4);
  **foreign-lineage boot**: a node whose local upstream is absent from `nodes`
  but whose `promoted_from` is `None` (or names a different node) boots to
  replicated `Primary` — the lineage guard **refuses**, the node self-fences per
  §3 defence-in-depth and serves nothing; flipping `promoted_from` to
  `Some(u)` in the same fixture admits, proving the disjunct is doing the
  adjudication and not the membership test (V14-C2);
  **unsynced sole replica**: a replica whose replicated pointer was written by
  `CLUSTER REPLICATE`/`SetRole` but which never attested — `synced == false` —
  is **not** an auto-failover candidate; the shard stays unavailable, no empty
  keyspace is substituted, and the availability loss is the asserted outcome
  (`unsynced_sole_replica_is_not_a_failover_candidate`, V14-C3);
  **`force: true` over an unsynced replica**: promoted, with the loss recorded
  as the declared lossy override — the operator escape stays open and is
  distinguishable in the event log from a lawful failover
  (`forced_failover_over_unsynced_replica_is_the_declared_lossy_override`,
  V14-C3);
  **supersession while staged**: a concurrent failover demotes the staged node,
  its report is refused with the node no longer replicated-Primary — the record
  clears, the fence drops, and the node converges by role-authority adoption to
  the failover's verdict (V14-M2's arm partition, exercised through the
  `RefusalClass` value rather than through prose);
  plus two **negative** assertions carried as invariant checks over the
  transition sweep: ~~no reachable state has `role == Replica ∧
  primary_id == None`~~ (**superseded in v15**, V15-C3: that state is the
  lawful **detached replica** of LOCKED FM-CLUSTER-002 — the assertion would
  have failed against `remove_node_prunes_migrations_and_detaches_replicas`;
  the v15 block below carries the state's own scenarios instead), and every
  writer of `role` in the same sweep also writes `synced`, `promoted_from`
  and `admitted_stage` (V14-C2/C3/M1's companion-field rule — **re-based in
  v15** onto every writer of **`primary_id`**, V15-C1, which is the form that
  fails against revision 14's rule,
  `companion_fields_written_by_every_parent_writer`).
  From review v15: **failover sibling re-parent clears the companion stamps**
  — a shard with a primary and two replicas fails over; the surviving
  sibling's `admitted_stage`, `synced` and `promoted_from` are all cleared by
  the `reparent_children(.., Some(new_primary))` half, and a subsequent
  staged flip at that sibling does **not** fire its adoption on a stale
  stamp — keyspace digest unchanged across the failover
  (`failover_sibling_reparent_clears_companion_stamps`, V15-C1);
  **`RemoveNode`'s detach is a companion writer too** — `CLUSTER FORGET` of a
  primary detaches its replicas; each detached replica's three companions are
  cleared and it is in no candidate set, while the LOCKED forcing test
  `remove_node_prunes_migrations_and_detaches_replicas` still passes
  unchanged (V15-C1/C3);
  **the detached replica is a declared state** — after the detach the node
  serves per its role's existing read rules (it is **not** fenced), proposes
  **nothing** over any number of observation cycles, is refused as an
  `AssignSlots` assignee, and is re-homed by a `SetRole` re-parent naming a
  live primary; the same fixture driven with `CLUSTER REPLICATE` *at* the
  detached node converges to the identical applied state through the
  degenerate stage
  (`detached_replica_serves_per_its_role_and_is_no_candidate`,
  `detached_replica_is_rehomed_by_setrole`, V15-C3);
  **attestation minted under the old parent** — a replica reaches its sync
  point, its attestation is held in flight, a re-parent commits, and the
  attestation then applies: **refused** on the observed-parent conjunct,
  `synced` stays `false`, the node is not promotable; dropping the conjunct
  in the same fixture promotes a zero-byte candidate, and the assertion is
  the keyspace digest after that promotion
  (`attestation_minted_under_the_old_parent_is_refused_after_a_reparent`,
  V15-C4);
  **partial resync attests** — a replica re-parented onto a primary it can
  PSYNC-continue from (the `replid2` path) reaches the sync point without a
  full sync, attests, and becomes a lawful candidate; under revision 14's
  full-sync-only trigger it never does — the liveness hole is the negative
  control (`partial_resync_replica_attests_and_becomes_a_candidate`, V15-M3);
  **planned failover waits for the attestation** — TR-CLUSTER-017's
  parity-proven flow proposes its `Failover` only after the successor's
  attestation has committed, so the amended TR-CLUSTER-021 belt never refuses
  a planned failover and no exemption exists in the belt to exercise
  (`planned_failover_waits_for_the_successors_attestation`, V15-M4);
  **priority `CONFIG SET` preserves `synced`** — TR-CLUSTER-027's live
  re-registration of a synced replica leaves it a candidate; an upsert that
  reset the field would de-candidate a healthy shard member
  (`priority_config_set_preserves_synced`, V15-M2);
  **stage-bound refusals** — a refusal minted for stage *S1*, delivered after
  *S1*'s record was cleared and *S2* staged, is a **log-only no-op**: *S2*'s
  record, fence and eventual adoption are untouched; removing the `stage_id`
  conjunct strands the node with a fence nothing lifts
  (`refusal_for_a_dead_stage_does_not_disturb_the_live_one`, V15-M1);
  **one selector, both directions** — over the staged-flip fixture's entire
  crash schedule, the adoption and the reconcile guard are asserted **never
  simultaneously enabled**, the executable form of the collapse to
  `admitted_stage == Some(record.stage_id)`
  (`adoption_and_reconcile_are_mutually_exclusive`, V15-C2).
