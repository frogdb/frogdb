# Cluster — failure modes

Status: LOCKED (2026-08-05) — Phase 4 mutation gate passed (`frogdb-cluster` 99.6% on 496
mutants, `frogdb-cluster-runtime` 99.0% on 224, vs an 0.80 gate; the four surviving mutants are
all documented equivalents at the code). Behavior changes to this area are spec-first: edit the
row, update the forcing test, then the code. Before pushing changes that touch these crates, run
`just mutants-diff <crate>`. See CLAUDE.md "Locked core areas".

Every way FrogDB's cluster layer can refuse, redirect, reassign, or succeed, one table per mode.
This is the reference the mutation run is measured against: a mutant that survives is a row nothing
forces.

**Clocks, stated as a principle.** Every wall-clock read in this area is judged against one rule:
using a clock to **stop** serving is fail-closed and safe; using a clock to **admit** traffic is the
rejected anti-pattern. A deadline that only ever *narrows* what a node will do — the self-fence
election-timeout window (FM-CLUSTER-059) is the canonical example — degrades safely on a clock that
runs fast, slow, or not at all: the node refuses more, never less. A deadline that *widens* what a
node will do — the `barrier_ms` wall-clock window `CompleteSlotMigration` used to consult before
admitting ownership to move — does the opposite: a clock skewed forward admits a transition no
quorum agreed the source had actually quiesced for, which is exactly the acknowledged-then-orphaned
write this area's handoff machinery exists to close (issue 17's ruling deletes that window for this
reason). The handoff lease (FM-CLUSTER-085) is not a clean example of either shape on its own: a
lapsed lease *narrows* one thing (`CompleteSlotMigration`/`AbortSlotHandoff` are refused once it has
expired) while simultaneously *widening* another (a later `PrepareSlotHandoff` is admitted once the
lease is no longer live, minting a fresh attempt) — which is exactly why issue 17's resolution of
which bound survives has to be written down rather than inferred from "a deadline only narrows"; see
TR-CLUSTER-013's caveat. This is also why every handoff deadline is proposer-minted data rather than
a clock read during `apply` (FM-CLUSTER-089): the *admission* decision must be a pure function of
replicated data, never of the applying node's own clock.

Scope. The cluster area is one replicated state machine plus the seams that read it:

* **Topology and membership (FM-CLUSTER-001..009, 078)** — the `ClusterCommand` arms that add,
  remove, re-role, and reset nodes, the bootstrap slot split, the rolling-upgrade gate, and how the
  applied topology is published to readers
  (`frogdb-server/crates/cluster/src/{commands,state,types,version_gate}.rs`).
* **Config epoch (010..017, 076)** — the two epochs (the replicated counter and the per-node value),
  who may raise them, the collision rule, and what survives a restart
  (`cluster/src/{state,commands,storage}.rs`, `server/src/commands/cluster/mod.rs`).
* **Slot ownership and routing (018..030)** — keyslot mapping, the `RouteDecision` verdicts and
  their wire renderings, CROSSSLOT, READONLY, ASKING, and `WATCH` registration
  (`server/src/slot_migration/{routing,validator}.rs`, `core/src/shard/partition.rs`).
* **Slot migration (031..038)** — the `SETSLOT` state machine as it exists today: a two-point
  ownership swap with no intermediate migration state, its idempotency, its cancel, and the
  documented residual window between commit and apply (`cluster/src/commands.rs:361-436`,
  `server/src/slot_migration/`).
* **Failover, promotion, and the propose path (039..051)** — the atomic `Failover` entry, the role
  events it emits, the reconciler that re-drives a data path that disagrees, and every error
  surface of `ClusterWriter::propose` (`cluster/src/{commands,state,writer,network}.rs`).
* **Failure detection (052..060)** — the leader-only, level-triggered detector: latching
  hysteresis, staleness, quorum arithmetic, promotion scoring, and the live runtime flags
  (`cluster-runtime/src/{failure_detector,flags}.rs`).
* **Admin gating (061..064)** — the per-subcommand admin surface for `CLUSTER`, including both
  fail-closed defaults (`core/src/command_spec.rs`, `server/src/connection/guards.rs`).
* **Bus, pub/sub, and reporting (065..075, 077)** — the cluster bus transport decision, the pub/sub
  RPC outcome mapping, the bus packet counters, and what `CLUSTER NODES`/`SLOTS`/`SHARDS`/`INFO`
  render
  (`cluster-runtime/src/{bus,pubsub}.rs`, `cluster/src/wire.rs`,
  `server/src/commands/cluster/mod.rs`).

* **Slot-scoped pause (079..083)** — the pause dimension the migration finalization barrier arms:
  which commands one slot's pause parks, the two exemptions that keep it from deadlocking the
  handover, and how it composes with the operator's node-global `CLIENT PAUSE`
  (`core/src/client_registry/mod.rs`, `server/src/connection/{pause_gate,lifecycle}.rs`).
* **Raft log storage (098..099, 103)** — what the RocksDB-backed log store owes openraft below the
  state machine: the durability class each metadata record is written at, the coherence of the
  log cache the long-lived log reader shares with the writing handle, and the range each of
  `truncate`/`purge` is contracted to remove
  (`cluster/src/storage.rs`).

Out of scope, deliberately: the rest of the pause-barrier design — the Raft `PrepareSlotHandoff`
op, the drain round trip, the handoff lease, and the fencing token are phase 2 of
[rework issue 02](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md)
and get their rows when they land. Also out of scope: the operator and `frogctl`, and the frozen
redis-regression suite. `WAIT` has
no cluster-mode special case at all — it is keyless, counts *this node's* replicas, and never
redirects — so it is specced once in
[Replication — failure modes](replication.md) (FM-REPLICATION-037..043) and not
duplicated here.

## State space

The projection-completeness anchor: every variable a transition can read or write. `Authoritative
field` names the concrete struct field; `Writer(s)` names the command arm(s) or seam that mutate it;
`Persistence` names the storage vehicle; `Survives restart` states whether the value is available
again after a process restart (as opposed to a Raft-level "does the cluster remember it").
Replicated fields (rows 1-20) live on `ClusterStateInner`
(`frogdb-server/crates/cluster/src/state.rs:107-135`) and are the openraft state machine's
`R`-generation payload: every replicated field is written only inside `apply_command`, persisted
through the Raft log (`cluster/src/storage.rs`) and the periodic snapshot, and rebuilt on restart by
replaying the log from the last snapshot. Node-local fields (rows 21-34) are never part of that
payload and diverge freely node to node by design.

| Variable | Authoritative field | Writer(s) | Persistence | Survives restart |
|---|---|---|---|---|
| Node membership | `ClusterStateInner.nodes: BTreeMap<NodeId, NodeInfo>` | `apply_command`: `AddNode`, `RemoveNode`, `Failover` (remove/re-parent), `ResetCluster` (clears to just this node, `commands.rs:832-855`; if `node_id` is not a member — a reset racing a `FORGET` — the `else` branch at `commands.rs:855-858` clears membership to *empty* instead and still returns `Ok`) | Raft log + snapshot | Yes |
| Node role | `NodeInfo.role: NodeRole` | `apply_command`: `SetRole`, `Failover` (promote/demote), `ResetCluster` (forces this node's role to `Primary`, `commands.rs:834`) | Raft log + snapshot | Yes |
| Node parent pointer | `NodeInfo.primary_id: Option<NodeId>` | `apply_command`: `AddNode`, `SetRole`, `Failover` (re-parent), `RemoveNode` (re-parents the departing node's children via `reparent_children`, `commands.rs:231`), `ResetCluster` (nulls this node's own parent pointer, `commands.rs:835`) | Raft log + snapshot | Yes |
| Node's own config epoch | `NodeInfo.config_epoch: ConfigEpoch` | `apply_command`: `AddNode` (initial), `SetConfigEpoch`, `Failover` (stamp on promotion), `ResetCluster` (HARD only: reset to 0, `commands.rs:843`) | Raft log + snapshot | Yes |
| Node FAIL flag | `NodeInfo.flags.fail: bool` | `apply_command`: `MarkNodeFailed`, `MarkNodeRecovered` | Raft log + snapshot | Yes |
| Node PFAIL flag | `NodeInfo.flags.pfail: bool` | None in production — structurally present (`types.rs:140`) but no production writer found anywhere in `cluster`/`cluster-runtime`; only test fixtures set it directly, confirmed by the Redis-deviations table (bottom of this spec): "structurally supported, never produced." | Raft log + snapshot (if ever written) | Yes (if ever written) |
| Node HANDSHAKE flag | `NodeInfo.flags.handshake: bool` | None in production — no production writer found in either crate | Raft log + snapshot (if ever written) | Yes (if ever written) |
| Node NOADDR flag | `NodeInfo.flags.noaddr: bool` | None in production — no production writer found in either crate | Raft log + snapshot (if ever written) | Yes (if ever written) |
| Node's replicated promotion priority | `NodeInfo.replica_priority: u32` | `apply_command`: `AddNode` (registration/re-registration) | Raft log + snapshot | Yes |
| Node's advertised version | `NodeInfo.version: String` | `apply_command`: `AddNode` (registration/re-registration) | Raft log + snapshot | Yes |
| Slot ownership | `ClusterStateInner.slot_assignment: BTreeMap<u16, NodeId>` | `apply_command`: `AssignSlots`, `RemoveSlots`, `Failover` (transfer), `CompleteSlotMigration` (move), `RemoveNode` (unassigns the departing node's slots, left unassigned rather than retargeted, `commands.rs:222-224`), `ResetCluster` (clears all, `commands.rs:821`) | Raft log + snapshot | Yes |
| Cluster config epoch (replicated counter) | `ClusterStateInner.config_epoch: ConfigEpoch` | `apply_command`: `IncrementEpoch`, `SetConfigEpoch`, `MarkNodeFailed` (bump), `Failover` (bump), `AddNode` (via `reconcile_incoming_epoch`: an uncontested nonzero claim raises the counter to `max(counter, claimed)`, `state.rs:488-515`, invoked at `commands.rs:191`), `ResetCluster` (HARD only: reset to 0, `commands.rs:841`) | Raft log + snapshot | Yes |
| Open migrations | `ClusterStateInner.migrations: BTreeMap<u16, SlotMigration>` | `apply_command`: `BeginSlotMigration`, `CompleteSlotMigration`/`CancelSlotMigration` (remove), `Failover` (prune naming the demoted/removed node), `RemoveNode` (`prune_migrations_naming`, `commands.rs:230`), `ResetCluster` (drains all, paying every release event the prepared handoffs owe, `commands.rs:826-829`) | Raft log + snapshot | Yes |
| A migration's prepared handoff | `SlotMigration.handoff: Option<SlotHandoff>` | `apply_command`: `PrepareSlotHandoff` (set), `ConfirmSlotHandoffDrained` (mark drained), `AbortSlotHandoff`/`CompleteSlotMigration` (clear) | Raft log + snapshot | Yes |
| A handoff's attempt number | `SlotHandoff.seq: u64` | `apply_command`: `PrepareSlotHandoff`, minted from `ClusterStateInner.handoff_seq` | Raft log + snapshot | Yes |
| A handoff's deadlines | `SlotHandoff.{prepared_at_ms, barrier_ms, lease_ms}` | `apply_command`: `PrepareSlotHandoff`, values proposer-minted through the clock seam (`handoff_now_ms`) and carried as replicated data, never read at apply | Raft log + snapshot | Yes |
| Handoff attempt counter | `ClusterStateInner.handoff_seq: u64` | `apply_command`: `PrepareSlotHandoff` (increment), `ResetCluster` (rewind to 0) | Raft log + snapshot; both restore vehicles (`from_snapshot`, `install_snapshot`) carry it (FM-CLUSTER-100) | Yes |
| Raft-applied index | `ClusterStateInner.last_applied_log: Option<LogId<NodeId>>` | `apply()` on every entry, including one whose command errored | Raft log + snapshot | Yes |
| Raft membership | `ClusterStateInner.last_membership` | openraft membership-change application, driven by `voter_change`'s side effects on `AddNode`/`RemoveNode`/`Failover` | Raft log + snapshot | Yes |
| Rolling-upgrade version gate | `ClusterStateInner.active_version: Option<String>` | `apply_command`: `FinalizeUpgrade` | Raft log + snapshot (`#[serde(default)]`, absent reads as `None`/pre-versioning) | Yes |
| This node's own id | `ClusterState.self_node_id: Arc<AtomicU64>` (node-local, **not** on `ClusterStateInner`) | `set_self_node_id`, called at boot and preserved across `from_snapshot` calls | No independent on-disk record; re-derived at every boot by `ClusterConfigSection::effective_node_id` (`frogdb-server/crates/config/src/cluster.rs:231-243`), called from `server/src/config/mod.rs:78` and re-supplied to `ClusterState::from_snapshot` rather than itself round-tripping through the snapshot | No, unless `cluster-node-id` is explicitly configured non-zero — `effective_node_id` then returns that configured value unchanged every boot. Left at the default 0, a fresh id `(timestamp << 16) \| random_bits` is minted on every boot instead. |
| Raft vote (this node's candidacy or a grant) | RocksDB `raft_meta` CF, key `KEY_VOTE` | `save_vote` (`cluster/src/storage.rs:579`) | RocksDB, **synced** write (`MetaDurability::for_key` classifies `KEY_VOTE` synced) | Yes |
| Raft committed index (local cache) | RocksDB `raft_meta` CF, key `KEY_COMMITTED` | `save_committed` | RocksDB, buffered/unsynced — deliberately never read back; openraft re-derives it from the leader | Not relied upon (may be stale/lost) |
| Raft last-purged watermark | RocksDB `raft_meta` CF, key `KEY_LAST_PURGED` | `purge()` | RocksDB, buffered/unsynced | Not relied upon (losing it only costs a repeat purge) |
| Raft log entries | RocksDB `raft_logs` CF | `append`/`truncate`/`purge` (`cluster/src/storage.rs`) | RocksDB; `append` acks a log write non-synced (still open, per FM-CLUSTER-098's note) | Yes, subject to that durability caveat |
| Raft log reader cache | `Arc<RwLock<BTreeMap<u64, Entry>>>`, shared by the writing handle and every reader | `append` (fill), `truncate`/`purge` (invalidate over the same range, FM-CLUSTER-099) | In-memory only | No — rebuilt from `raft_logs` on next read |
| `cluster-auto-failover` runtime flag | `ClusterRuntimeFlags.auto_failover: AtomicBool` (node-local, not replicated) | `CONFIG SET cluster-auto-failover`; constructed at boot from `ClusterConfigSection` | In-memory only | No — re-derived from config at boot (default off) |
| `cluster-self-fence-on-quorum-loss` runtime flag | `ClusterRuntimeFlags.self_fence_on_quorum_loss: AtomicBool` (node-local) | `CONFIG SET cluster-self-fence-on-quorum-loss`; constructed at boot | In-memory only | No — re-derived from config at boot (default on) |
| `cluster-replica-priority` runtime flag | `ClusterRuntimeFlags.replica_priority: AtomicU32` (node-local) | `CONFIG SET cluster-replica-priority`; constructed at boot | In-memory only | No — re-derived from config at boot (default 100). Ruled (issue 30): the replicated `NodeInfo.replica_priority` above is the single authority for cross-node scoring, and `CONFIG SET` converges it by proposing an `AddNode` re-registration with the new value; this atomic keeps one authoritative role — the node's own immediate view, so priority 0 removes the node from its own candidate set before the Raft commit lands (FM-CLUSTER-058). Today the re-registration proposal does not exist, leaving peers on the stale replicated value until the next boot-time registration — [issue 30](../.scratch/cluster-correctness/issues/open/30-config-set-replica-priority-re-registers-through-raft.md) tracks code catch-up |
| Per-peer local health | `HealthTable.health: HashMap<NodeId, NodeHealth>` (node-local, leader-only, not replicated) | `record_failure`/`record_success` from the probe loop (`failure_detector.rs`) | In-memory only | No — rebuilt from empty; every peer reads `Unknown` until re-probed, which is fail-safe under FM-CLUSTER-055's conservative-quorum rule |
| Failure-detector tuning | `FailureDetectorConfig.{check_interval_ms, connect_timeout_ms, fail_threshold}` (node-local) | Constructed once at `FailureDetector::new`, clamped into `[MIN_*, MAX_*]` (FM-CLUSTER-102) | In-memory only | No — re-derived from config at boot |
| Slot-scoped write barrier | `PauseState.slots: HashMap<u16, PauseEntry>` (node-local, `core/src/client_registry/mod.rs`) | `handoff_barrier.rs`'s `plan_handoff_action` (`Arm`/`Release`), driven by replicated `SlotHandoffPrepared`/`SlotHandoffReleased` events | In-memory only | No — and it **must be reconstructed** from the replicated `migrations` map on both restore paths (boot-time snapshot restore and live `install_snapshot`), or a restarted source serves its migrating slot unfenced (FM-CLUSTER-104). The barrier is a pure function of the replicated handoff state, but nothing in the code performs the reconstruction today — [issue 32](../.scratch/cluster-correctness/issues/open/32-restarted-source-never-re-arms-its-slot-write-barrier.md) tracks code catch-up |
| Replica-feed hold deadline | `ReplicaFeedGate` internal `Option<Instant>` (node-local, `replication/src/feed_gate.rs`), derived via `PauseState::feed_hold_until` | `ClientRegistry::publish_pause_derived_state`, called on every pause mutation | In-memory only (an `Instant`, meaningless across a restart) | No |
| Bootstrap-vs-join intent | Ruled (issue 25): an explicit, node-local **persisted** record (etcd `initial-cluster-state` shape) written when the node first commits to bootstrapping or joining, so a restart mid-bootstrap replays the recorded decision instead of re-deriving it (TR-CLUSTER-028/029 carry the transition semantics). No such field exists in the code today — `should_bootstrap` (`server/src/server/cluster_init.rs:386`) is computed fresh every boot from config (`initial_members.keys().next().copied() == Some(node_id)`); [issue 25](../.scratch/cluster-correctness/issues/open/25-newly-started-node-briefly-usurps-leadership-via-solo-bootstrap.md) tracks code catch-up | Ruled: written once at first-boot mode decision; never rewritten by config changes | Ruled: node-local durable record (vehicle chosen at implementation); survives restart |

## Transitions

One `## TR-CLUSTER-NNN` section per action source, enumerated mechanically from the `ClusterCommand`
enum (`cluster/src/types.rs:295-438`), the failure detector's tick outcomes, runtime-flag mutations,
recovery/restart steps, and the admin-command entry points that construct these commands. Where a
2026-08-13 ruling amends a transition's semantics, the row states the **ruled/target** semantics —
not necessarily what the code does today — and carries a `Pending` field linking the issue that
tracks code catch-up.

Two Quint design models formalize a slice of these rows for exploration and code-conformance
checking: [`cluster_admission.qnt`](quint/cluster_admission.qnt) covers the bootstrap/join/MEET
admission window (TR-CLUSTER-005, 028, 029, 030, 035); [`cluster_migration_failover.qnt`](quint/cluster_migration_failover.qnt)
covers slot migration interleaved with failover (TR-CLUSTER-003, 004, 008–019, 021, 024, 025, 034,
042). Both models' traces are checked against production code by the quint-connect conformance
harness in [`quint_conformance.rs`](../frogdb-server/crates/cluster/tests/quint_conformance.rs);
tests disclosed `#[ignore]` name the owning issue rather than being evidence the row already holds.
A counterexample or a conformance divergence is triaged with this spec as the arbiter: if the model
diverges from a ruled row above it is model-wrong and the model gets fixed, otherwise it is
code-wrong and gets filed or attached to an issue in the area's tracker with the trace as evidence.

### Group A — Topology and membership

## TR-CLUSTER-001 — `AddNode` registers a new member

| Field | Value |
|---|---|
| Precondition | `node.id` not in State-space "Node membership"; OR `node.id` already present (re-registration, TR-CLUSTER-002). Ruled (issue 14): if `node.primary_id` is `Some(p)`, `p` must be present in "Node membership" and `Node role`(p) = `Primary`. |
| Postcondition | "Node membership" gains `node.id` -> `NodeInfo{..}`; "Node role", "Node parent pointer", "Node's replicated promotion priority", "Node's advertised version" all set from the command's `node` fields. "Node's own config epoch" is **not** a bare copy of `node.config_epoch`: `reconcile_incoming_epoch` (`state.rs:488-515`) resolves it against the recorded state in one of three ways — a claimed epoch of 0 against a node with a recorded nonzero epoch is `Preserved` (the incoming 0 is discarded, the existing value kept); a nonzero claim **by a primary** colliding with another primary's current epoch is `Reassigned` a fresh epoch via `mint_config_epoch()` (an incoming *replica* whose claim collides is `Accepted` unchanged — `reconcile_incoming_epoch`'s own doc-comment calls this deliberate Redis parity, since only a primary's epoch arbitrates slot ownership); an uncontested nonzero claim is `Accepted` as given. The `Accepted` and `Reassigned` outcomes also raise "Cluster config epoch" to `max(counter, resolved value)` — `AddNode` is the one command that can carry an epoch the replicated counter did not itself mint, so it is the one path that can introduce (and must resolve) a collision. Ruled (issue 14): a dangling or replica-naming `primary_id` is refused (`ClusterError`) rather than admitted — no state changes on that path. |
| Source | `cluster/src/commands.rs` (`AddNode` arm, `reconcile_incoming_epoch` call at `commands.rs:191`); `cluster/src/state.rs:488-515` (`reconcile_incoming_epoch`); ruled `primary_id` precondition not yet in code |
| Rulings | Issue 14 |
| Pending | [issue 14](../.scratch/cluster-correctness/issues/open/14-role-transitions-admit-malformed-parents.md) |

## TR-CLUSTER-002 — `AddNode` re-registration upserts, never demotes by restart

| Field | Value |
|---|---|
| Precondition | `node.id` already present in "Node membership". |
| Postcondition | "Node membership"(node.id)'s address/priority/version fields overwrite to the incoming values; "Node role" and "Node parent pointer" are **not** downgraded by a bare re-registration — a restarting primary that re-`AddNode`s itself does not silently become a replica. |
| Source | `cluster/src/commands.rs` (`AddNode` arm, upsert branch); FM-CLUSTER-001 |
| Rulings | — |

## TR-CLUSTER-003 — `RemoveNode` (`CLUSTER FORGET`)

| Field | Value |
|---|---|
| Precondition | `node_id` present in "Node membership". Ruled (issue 20, amendment): refused unless the node has already been demoted off every slot it owns and out of the primary role (i.e. is a replica or already ownerless) — a `FORCE` escape bypasses this check for the documented "the node is gone for good" operator case. |
| Postcondition | "Node membership" loses `node_id`; "Slot ownership" entries naming it and "Open migrations" naming it are pruned in the same entry (FM-CLUSTER-002/036 helper `prune_migrations_naming`); Raft membership drops the node as a voter (TR-CLUSTER-039, `voter_change`) unless it was already not a voter. |
| Source | `cluster/src/commands.rs` (`RemoveNode` arm); `cluster/src/network.rs` (`voter_change`, `spawn_remove_raft_voter`) |
| Rulings | Issue 20 (amendment: eviction fence required unless `FORCE`) |
| Pending | [issue 20](../.scratch/cluster-correctness/issues/open/20-force-failover-evicts-the-old-primary-from-raft-so-it-never-learns-it-lost-its-slots.md) |

## TR-CLUSTER-004 — `SetRole` promotes/demotes a node

| Field | Value |
|---|---|
| Precondition | `node_id` present in "Node membership". Ruled (issue 14): if `role = Replica` and `primary_id = Some(p)`, `p` must be present and `Node role`(p) = `Primary`. |
| Postcondition | "Node role"(node_id) = `role`; "Node parent pointer"(node_id) = `primary_id`. A demotion of `self_node_id` fires exactly one `Demoted` event iff the role actually changes (FM-CLUSTER-043); re-asserting `Replica` on an already-replica node still fires `Demoted` (re-parenting must re-point the replication stream). |
| Source | `cluster/src/commands.rs` (`SetRole` arm); `cluster/src/state.rs:518-527` (`DemotionEvent`); `cluster/src/state.rs:825-890` (`SelfRoleReconciler`, the periodic re-emission this row's demotion-event guarantee ultimately depends on) |
| Rulings | Issue 14 |
| Pending | [issue 14](../.scratch/cluster-correctness/issues/open/14-role-transitions-admit-malformed-parents.md) |

## TR-CLUSTER-005 — `CLUSTER MEET` -> `AddNode`, ruled join-safety gates

| Field | Value |
|---|---|
| Precondition | Ruled (issue 25, amendment): the MEET'd node's local Raft state — State-space "Raft vote" and "Raft log entries" (named, not by row number, since a table edit shifts ordinals) — must be empty — a node that was ever a member of another cluster is refused, mirroring etcd's "wipe the data dir first" rule. |
| Postcondition | On acceptance, proceeds as TR-CLUSTER-001. On refusal, no state changes and the operator is told to wipe local state first. |
| Source | Not yet in code — the MEET handler constructs `AddNode` with no such precheck today (`server/src/server/cluster_init.rs`) |
| Rulings | Issue 25 |
| Pending | [issue 25](../.scratch/cluster-correctness/issues/open/25-newly-started-node-briefly-usurps-leadership-via-solo-bootstrap.md) |

### Group B — Config epoch

## TR-CLUSTER-006 — `IncrementEpoch`

| Field | Value |
|---|---|
| Precondition | None (always admitted; FM-CLUSTER-010 dominance invariant — bumping is always allowed). |
| Postcondition | "Cluster config epoch" += 1; response carries the new `ConfigEpoch`. |
| Source | `cluster/src/commands.rs` (`IncrementEpoch` arm) |
| Rulings | — |

## TR-CLUSTER-007 — `SetConfigEpoch` (`CLUSTER SET-CONFIG-EPOCH`)

| Field | Value |
|---|---|
| Precondition | `node_id`'s own view knows no peer, and "Node's own config epoch"(node_id) = 0 (Redis-parity bootstrap-only guard, FM-CLUSTER-076). |
| Postcondition | "Node's own config epoch"(node_id) = `epoch` exactly (not a bump — a direct assignment). |
| Source | `cluster/src/commands.rs` (`SetConfigEpoch` arm) |
| Rulings | — |

### Group C — Slot migration and handoff

## TR-CLUSTER-008 — `AssignSlots`

| Field | Value |
|---|---|
| Precondition | Every named slot's current owner (if any) accepts the reassignment; validate-all-then-apply. Ruled (issue 16): a slot with an entry in "Open migrations" is refused outright — no mutation-while-migrating, source-only exception dropped. |
| Postcondition | "Slot ownership" set for every named slot to `node_id`, all-or-nothing. |
| Source | `cluster/src/commands.rs` (`AssignSlots` arm) |
| Rulings | Issue 16 |
| Pending | [issue 16](../.scratch/cluster-correctness/issues/open/16-assign-slots-ignores-open-migrations.md) |

## TR-CLUSTER-009 — `RemoveSlots`

| Field | Value |
|---|---|
| Precondition | Same open-migration refusal as TR-CLUSTER-008 (issue 16). |
| Postcondition | "Slot ownership" cleared for every named slot, all-or-nothing. |
| Source | `cluster/src/commands.rs` (`RemoveSlots` arm) |
| Rulings | Issue 16 |
| Pending | [issue 16](../.scratch/cluster-correctness/issues/open/16-assign-slots-ignores-open-migrations.md) |

## TR-CLUSTER-010 — `BeginSlotMigration` (`CLUSTER SETSLOT ... MIGRATING`/`IMPORTING`)

| Field | Value |
|---|---|
| Precondition | `slot`'s current owner is `source_node`; no existing "Open migrations" entry for `slot` (or the same source/target — idempotent, FM-CLUSTER-031). |
| Postcondition | "Open migrations"(slot) = `SlotMigration{source_node, target_node, handoff: None}`. |
| Source | `cluster/src/commands.rs` (`BeginSlotMigration` arm) |
| Rulings | — |

## TR-CLUSTER-011 — `PrepareSlotHandoff`

| Field | Value |
|---|---|
| Precondition | "Open migrations"(slot) exists with matching `source_node`/`target_node`. |
| Postcondition | "Handoff attempt counter" += 1; "A migration's prepared handoff"(slot) = `SlotHandoff{seq: new counter value, prepared_at_ms: proposed_at_ms, barrier_ms, lease_ms, drained: false}`; emits `SlotHandoffPrepared` on every node (only the source node arms its write barrier, TR-CLUSTER-034). |
| Source | `cluster/src/commands.rs` (`PrepareSlotHandoff` arm); `cluster/src/state.rs` (`handoff_seq` increment) |
| Rulings | — |

## TR-CLUSTER-012 — `ConfirmSlotHandoffDrained`

| Field | Value |
|---|---|
| Precondition | "A migration's prepared handoff"(slot) exists and its `seq` equals the command's `seq` (stale acks from a superseded attempt are refused). |
| Postcondition | `SlotHandoff.drained` = true. Does not by itself move ownership. |
| Source | `cluster/src/commands.rs` (`ConfirmSlotHandoffDrained` arm) |
| Rulings | — |

## TR-CLUSTER-013 — `CompleteSlotMigration` (`CLUSTER SETSLOT ... NODE`)

| Field | Value |
|---|---|
| Precondition | Ruled (issue 17): "A migration's prepared handoff"(slot) exists, its `seq` matches, and `drained` = true. The `barrier_ms` wall-clock admission window is **removed** — `Complete` is admitted purely on the log-ordered drained-and-matching-seq predicate, not on whether a proposer-minted deadline has elapsed. **Open (issue 29):** whether `lease_ms` survives unchanged as a second, separate bound on how long the *prepared record itself* lives before a later `Prepare` can supersede it (FM-CLUSTER-085) is this drafter's reading of the ruling text, not a settled fact — issue 29 flags the ruling as ambiguous on exactly this point (which of the two deadline fields governs which behavior after `barrier_ms`'s deletion). Re-confirm against issue 29's resolution before this row is implemented; do not treat the `lease_ms` reading above as authoritative until then. |
| Postcondition | "Slot ownership"(slot) = `target_node`; "Open migrations"(slot) removed; emits `SlotMigrationCompleted` and `SlotHandoffReleased`. |
| Source | `cluster/src/commands.rs` (`CompleteSlotMigration` arm); `cluster/src/types.rs` `SlotHandoff::admits_complete_at` (today includes `!barrier_expired`, which the ruling removes) |
| Rulings | Issue 17; ambiguity on the `lease_ms` reading above flagged open by issue 29 |
| Pending | [issue 17](../.scratch/cluster-correctness/issues/open/17-stale-source-outlives-its-write-barrier.md), [issue 29](../.scratch/cluster-correctness/issues/open/29-spec-row-edit-sweep-from-the-2026-08-13-rulings.md) (open: which of `barrier_ms`/`lease_ms` survives) |

## TR-CLUSTER-014 — `AbortSlotHandoff`

| Field | Value |
|---|---|
| Precondition | "A migration's prepared handoff"(slot) exists with matching `seq` (or the attempt is already gone — succeeds as a no-op, FM-CLUSTER-086). Ruled (issue 17): admission is log-ordered, no wall-clock window. |
| Postcondition | "A migration's prepared handoff"(slot) = `None`; "Open migrations"(slot) unchanged (the migration itself survives); emits `SlotHandoffReleased`. |
| Source | `cluster/src/commands.rs` (`AbortSlotHandoff` arm) |
| Rulings | Issue 17 |
| Pending | [issue 17](../.scratch/cluster-correctness/issues/open/17-stale-source-outlives-its-write-barrier.md) |

## TR-CLUSTER-015 — `CancelSlotMigration` (`CLUSTER SETSLOT ... STABLE`)

| Field | Value |
|---|---|
| Precondition | "Open migrations"(slot) exists. Ruled (issue 15, amendment): if slot data has already been repatriated to the source in whole or in part by the abort path, that repatriation must complete *before* `Cancel`/`Abort` applies cleanly — cancellation is not just a state-machine record deletion when data has moved. |
| Postcondition | "Open migrations"(slot) removed; "A migration's prepared handoff"(slot), if any, released (`SlotHandoffReleased`) in the same entry. |
| Source | `cluster/src/commands.rs` (`CancelSlotMigration` arm) |
| Rulings | Issue 15 |
| Pending | [issue 15](../.scratch/cluster-correctness/issues/open/15-graceful-failover-leaves-migrations-sourced-at-the-old-primary.md) |

## TR-CLUSTER-016 — Replica-feed hold during an armed slot barrier

| Field | Value |
|---|---|
| Precondition | "Slot-scoped write barrier"(slot) is armed on a node that is also a replication primary. Ruled (issue 17, amendment): the hold is bounded by a byte cap on buffered frames, not solely by the barrier's wall-clock deadline — a byte-cap-exceeding hold releases the session (disconnect) rather than buffering unboundedly, since the wall-clock admission window it used to inherit its bound from is gone. |
| Postcondition | "Replica-feed hold deadline" set to the latest deadline across armed barriers (`decide_hold`); frames withheld from the replica's stream and buffered per-session in offset order until release or the byte cap is exceeded. |
| Source | `core/src/client_registry/mod.rs` (`PauseState::feed_hold_until`); `replication/src/feed_gate.rs` (`ReplicaFeedGate`); byte cap not yet in code |
| Rulings | Issue 17 (amendment) |
| Pending | [issue 17](../.scratch/cluster-correctness/issues/open/17-stale-source-outlives-its-write-barrier.md) |

### Group D — Failover and promotion

## TR-CLUSTER-017 — Planned `CLUSTER FAILOVER` (graceful, `old_primary` healthy)

| Field | Value |
|---|---|
| Precondition | `old_primary_id` present in "Node membership" with "Node role" = `Primary`; `new_primary_id` != `old_primary_id`, present, and a replica of `old_primary_id`. Ruled (issue 26): reuses the slot-migration handoff machinery rather than a bespoke mechanism — before proposing `Failover`, arm a slot-scoped write barrier (`SlotFence`) on every slot the old primary owns (FM-CLUSTER-079 machinery), drain in-flight writes (FM-CLUSTER-090/091 machinery), and wait for `new_primary_id`'s replication offset to reach parity with `old_primary_id`'s head. Only then propose. |
| Postcondition | Same postcondition as TR-CLUSTER-018 with `force: false` (demote, not remove) — but reached only after the barrier/drain/parity sequence above, making the handover lossless by construction: the `SlotFence` carries the ownership token that the barrier/drain sequence arms and releases, so a delayed write from the old primary that arrives after the swap finds no live fence to accept it under its stale ownership and is rejected rather than silently applied out of order. No write acknowledged by the old primary is discarded by the resync that follows demotion. |
| Source | `server/src/commands/cluster/admin.rs` (handler, immediate-propose today); `cluster/src/commands.rs` (`Failover` arm, `force: false` path) |
| Rulings | Issue 26 |
| Pending | [issue 26](../.scratch/cluster-correctness/issues/open/26-planned-failover-gets-a-drain-and-offset-parity-barrier.md) |

## TR-CLUSTER-018 — `Failover` state-machine application (graceful, `force: false`)

| Field | Value |
|---|---|
| Precondition | Every check in FM-CLUSTER-039/041 passes (known members, distinct nodes, old primary still a member for the graceful path). Ruled + amended (issue 19): the proposal carries `old_primary_id`'s observed "Node's own config epoch" and role (its role-version, as read by the proposer at observation time); apply additionally refuses — the *fence* — if *that node's* current "Node's own config epoch" or "Node role" has changed since observation (a per-object fence, not a global one: a change to any other node's epoch/role does not block this apply). This replaces a global epoch-CAS shape the amendment rejected as starvable under correlated failure. Separately, the *belt* (retained through the amendment, "The belt (refuse no-op moves) stays") refuses the move outright if `old_primary_id` is not a member, owns no slots, or `new_primary_id` is not `old_primary_id`'s replica — the fence catches a stale observation of a node that still matters; the belt catches a proposal that was never going to move anything. |
| Postcondition | "Slot ownership" entries owned by `old_primary_id` -> `new_primary_id`; "Node role"(old_primary_id) = `Replica`, "Node parent pointer"(old_primary_id) = `new_primary_id`; "Node role"(new_primary_id) = `Primary`, its parent pointer cleared; siblings of `old_primary_id` re-parented to `new_primary_id`; "Cluster config epoch" += 1, stamped onto `new_primary_id`'s "Node's own config epoch"; "Open migrations" naming `old_primary_id` pruned (issue 15: on every failover naming the demoted node, not force-only) with releases emitted. |
| Source | `cluster/src/commands.rs:393-499` (`Failover` arm, `force: false` path within it); per-object epoch/role fence not yet in code |
| Rulings | Issue 15 (migration-cancellation scope), Issue 19 (per-object epoch/role fence, amended) |
| Pending | [issue 15](../.scratch/cluster-correctness/issues/open/15-graceful-failover-leaves-migrations-sourced-at-the-old-primary.md), [issue 19](../.scratch/cluster-correctness/issues/open/19-a-forced-failover-promotes-a-node-that-inherits-nothing.md) |

## TR-CLUSTER-019 — Automatic failover proposal (failure-detector-decided)

| Field | Value |
|---|---|
| Precondition | Leader's `reconcile_topology` has latched `old_primary_id` as FAILed (TR-CLUSTER-023/024); `trigger_auto_failover` selects a successor by TR-CLUSTER-021's scoring. Ruled (issue 20): proposes `Failover{force: false}` by default — demote, not remove — routing the automatic path through the same shape as the planned path, so a partitioned-but-alive primary is not evicted from the topology by a leader that merely cannot reach it. |
| Postcondition | Same shape as TR-CLUSTER-018, including its same fence and belt (issue 19, amended): the proposal carries `old_primary_id`'s "Node's own config epoch" and "Node role" as observed at scoring time, and apply refuses — the fence — if that node's own state has since changed, rather than fencing on the cluster-wide "Cluster config epoch" (a global fence was rejected: FM-CLUSTER-013's unconditional epoch bump on every `MarkNodeFailed` would let one flapping node starve every unrelated in-flight failover); apply also refuses outright — the belt, "refuse no-op moves," retained through the amendment — if `old_primary_id` is not a member, owns no slots, or `new_primary_id` is not `old_primary_id`'s replica. If `old_primary_id` turns out to still be a member and healthy from its own point of view, it is demoted (not removed) and self-corrects via the level-triggered reconciliation pass (issue 18) rather than requiring a separate re-add. Ruled (issue 26): the automatic path stays asynchronous-lossy — no barrier/drain/parity wait — so an acked-but-unshipped write tail may be lost; this is stated as the honest cost of async PSYNC, with the divergence log recording the window and `WAIT` as the per-write escape hatch an operator already has. |
| Source | `cluster-runtime/src/failure_detector.rs:594-745` (`trigger_auto_failover`); per-object epoch/role fence not yet in code |
| Rulings | Issue 19 (per-object epoch/role fence, amended), Issue 20, Issue 26 |
| Pending | [issue 19](../.scratch/cluster-correctness/issues/open/19-a-forced-failover-promotes-a-node-that-inherits-nothing.md), [issue 20](../.scratch/cluster-correctness/issues/open/20-force-failover-evicts-the-old-primary-from-raft-so-it-never-learns-it-lost-its-slots.md), [issue 26](../.scratch/cluster-correctness/issues/open/26-planned-failover-gets-a-drain-and-offset-parity-barrier.md) |

## TR-CLUSTER-020 — `CLUSTER FAILOVER FORCE`/`TAKEOVER` issued to a primary

| Field | Value |
|---|---|
| Precondition | The receiving node's "Node role" = `Primary`. |
| Postcondition | Ruled (issue 28): refused unconditionally with "`CLUSTER FAILOVER` can only be run on a replica" — regardless of `force`/`takeover`. The primary-absorbs-primary absorb mode (select an arbitrary peer primary by `HashMap` iteration order, evict it) is **removed**, not made deterministic. |
| Source | `server/src/commands/cluster/admin.rs:284-308` (today: constructs `Failover{force: true}` against a `HashMap`-order-selected peer primary) |
| Rulings | Issue 28 |
| Pending | [issue 28](../.scratch/cluster-correctness/issues/open/28-cluster-failover-refused-on-a-primary.md) |

## TR-CLUSTER-021 — Failover successor selection

| Field | Value |
|---|---|
| Precondition | Candidates = replicas of the failing primary. |
| Postcondition | Score = `priority * 100_000 + (max_offset - replica_offset) * 1_000` (saturating); priority-0 candidates excluded outright; ties broken by lower node id; `None` if every candidate is priority-0 or the replica set is empty (failover abandoned with a warning). |
| Source | `cluster-runtime/src/failure_detector.rs` (`compute_replica_score`, `select_failover_target`) |
| Rulings | — |

### Group E — Failure detection and runtime flags

## TR-CLUSTER-022 — Probe result updates local health

| Field | Value |
|---|---|
| Precondition | A TCP probe to a peer completes (success or failure/timeout). |
| Postcondition | Success: "Per-peer local health"(peer).`last_seen` = now and `failure_count` = 0 always; `success_count` increments **only while the peer is latched** (`is_marked_fail`) and, once it reaches `fail_threshold`, un-latches and resets `success_count` to 0 — an already-healthy (not latched) peer's `success_count` never accumulates, so a single flap does not pre-load the un-latch counter. Failure: `failure_count` += 1, `success_count` = 0; if `failure_count` reaches `fail_threshold`, latch (`is_marked_fail` = true). |
| Source | `cluster-runtime/src/failure_detector.rs:262-273` (`record_success`), `:280-288` (`record_failure`) |
| Rulings | — |

## TR-CLUSTER-023 — Leader reconciliation tick, level-triggered

| Field | Value |
|---|---|
| Precondition | Leader evaluates `LocalVerdict` for every peer on every tick — this pass (`reconcile_topology`'s mark/recover half) is already level-triggered today, re-deciding from current state on every call rather than reacting to a transition. The edge-triggered defect GAP 1/issue 18 targets is downstream of this row, in `trigger_auto_failover`'s own retry loop (TR-CLUSTER-019's `Source`), not in this reconciliation pass itself. |
| Postcondition | Ruled (issue 18, amendment): a `Failed` verdict for a node not yet FAIL-flagged proposes `MarkNodeFailed`; a `Healthy` verdict for a FAIL-flagged node proposes `MarkNodeRecovered`; an already-consistent pair (verdict matches flag) proposes nothing (FM-CLUSTER-014's anti-churn property). The retry loop this row hands off to (`trigger_auto_failover`) is bounded by an in-flight guard and backoff, never permanently abandons a failed primary (today's `MAX_ATTEMPTS = 3` give-up at `failure_detector.rs:687` is deleted), and emits observability on every attempt including the give-up case GAP 3 flags as untested today. |
| Source | `cluster-runtime/src/failure_detector.rs:474-500` (`reconcile_topology`) |
| Rulings | Issue 18 |
| Pending | [issue 18](../.scratch/cluster-correctness/issues/open/18-a-missed-failover-is-never-retried.md) |

## TR-CLUSTER-024 — `MarkNodeFailed` application

| Field | Value |
|---|---|
| Precondition | None enforced beyond `node_id` presence. |
| Postcondition | "Node FAIL flag"(node_id) = true; "Cluster config epoch" += 1 (unconditional bump on every mark — cross-referenced by issue 19's amendment as the reason the epoch fence had to move to per-object, since an unconditional global bump on every mark starves a global fence under correlated failure). |
| Source | `cluster/src/commands.rs` (`MarkNodeFailed` arm) |
| Rulings | Issue 19 (cross-reference only — this transition's own behavior is unchanged by 19) |

## TR-CLUSTER-025 — `MarkNodeRecovered` application

| Field | Value |
|---|---|
| Precondition | None enforced beyond `node_id` presence. |
| Postcondition | "Node FAIL flag"(node_id) = false. No epoch bump (FM-CLUSTER-014: bumping here would let a flapping peer churn the epoch once per round — the anti-churn reasoning issue 19's amendment leans on when explaining why a *global* epoch CAS was the wrong granularity). |
| Source | `cluster/src/commands.rs` (`MarkNodeRecovered` arm) |
| Rulings | Issue 19 (cross-reference only) |

## TR-CLUSTER-026 — Write-admission self-fence check

| Field | Value |
|---|---|
| Precondition | A keyed write reaches the write-fence gate. |
| Postcondition | Ruled (issue 27): fences (refuses) iff this node has not heard from, or applied an entry from, a Raft leader within an election timeout — the CheckQuorum shape. TCP-probe-derived local quorum (today's `HealthTable`-based "Per-peer local health" -> `reachable_count`/quorum arithmetic, FM-CLUSTER-055/059) is **no longer admission evidence**: a wedged-but-listening peer or a same-partition peer must not count toward admitting writes. Per the preamble principle above, this is a clock used to *stop* serving (fail-closed), the opposite of the deleted `barrier_ms` admission window. |
| Source | `cluster-runtime/src/flags.rs` (`SelfFenceGate::has_quorum`, today keyed on `HealthTable` quorum, FM-CLUSTER-055/059); election-timeout fence not yet in code |
| Rulings | Issue 27 |
| Pending | [issue 27](../.scratch/cluster-correctness/issues/open/27-self-fence-quorum-derives-from-raft-liveness-not-tcp-probes.md) |

## TR-CLUSTER-027 — Runtime flag mutation (`CONFIG SET`)

| Field | Value |
|---|---|
| Precondition | `CONFIG SET cluster-auto-failover \| cluster-self-fence-on-quorum-loss \| cluster-replica-priority <value>`. |
| Postcondition | The matching `ClusterRuntimeFlags` atomic is stored, effective immediately for this node (no restart) — priority 0 removes this node from its own candidate set in TR-CLUSTER-021 before any Raft round-trip. Ruled (issue 30): for `cluster-replica-priority`, a successful `CONFIG SET` additionally proposes an `AddNode` re-registration carrying the new priority, converging the replicated `NodeInfo.replica_priority` peers score from within one Raft commit; the local store is not gated on that proposal succeeding. Today no such proposal exists — peers keep scoring the stale replicated field until the node's next boot-time registration (FM-CLUSTER-058's documented asymmetry, to be amended by the issue). |
| Source | `cluster-runtime/src/flags.rs` |
| Rulings | Issue 30 |
| Pending | [issue 30](../.scratch/cluster-correctness/issues/open/30-config-set-replica-priority-re-registers-through-raft.md) |

### Group F — Bootstrap

## TR-CLUSTER-028 — Explicit fresh-deployment bootstrap

| Field | Value |
|---|---|
| Precondition | Ruled (issue 25): a node is **explicitly configured** to bootstrap a fresh deployment (etcd `initial-cluster-state=new` shape) — not merely computed as "I'm first in `initial_members`" from ordinary join config, which is what `should_bootstrap` does today. |
| Postcondition | `raft.initialize` runs and the node becomes an externally-routable leader of its own group; "Bootstrap-vs-join intent" persisted so a restart mid-bootstrap does not re-decide. |
| Source | `server/src/server/cluster_init.rs:386,437-450` (today: `should_bootstrap` computed from config every boot, no persisted intent) |
| Rulings | Issue 25 |
| Pending | [issue 25](../.scratch/cluster-correctness/issues/open/25-newly-started-node-briefly-usurps-leadership-via-solo-bootstrap.md) |

## TR-CLUSTER-029 — Joining node defers election until MEET folds it in

| Field | Value |
|---|---|
| Precondition | Ruled (issue 25): a node **not** explicitly configured to bootstrap defers `raft.initialize`/leader election entirely until `CLUSTER MEET` from a real cluster folds it in — it must never self-elect into an externally routable "leader" of a solo one-node group in the meantime (today's defect: exactly this happens, and a learner-promotion conflict during the ~5s window it takes to resolve gets mistranslated into a client-facing `-REDIRECT` to the not-yet-real leader). |
| Postcondition | No externally-observable leadership claim until membership is real; `has to forward request to: Some(<node>)` originating from a learner-promotion conflict is no longer translated into a `-REDIRECT` (error-shape fix, second half of the ruling). |
| Source | `server/src/server/cluster_init.rs:437-450` (today: unconditional solo bootstrap for `member_count=1`); `cluster/src/network.rs:779` (`add_learner` retry, `MAX_ATTEMPTS=5`) |
| Rulings | Issue 25 |
| Pending | [issue 25](../.scratch/cluster-correctness/issues/open/25-newly-started-node-briefly-usurps-leadership-via-solo-bootstrap.md) |

## TR-CLUSTER-030 — Bootstrap slot seeding

| Field | Value |
|---|---|
| Precondition | `should_bootstrap && !initial_members.is_empty() && !cluster.all_slots_assigned()`. |
| Postcondition | The 16384 slots are split evenly across `initial_members` and assigned via the normal `AssignSlots` mutation (TR-CLUSTER-008's postcondition, without the open-migration precondition since none exist yet). |
| Source | `server/src/server/cluster_init.rs:530-` |
| Rulings | — |

### Group G — Recovery and restart

## TR-CLUSTER-031 — Log replay on restart

| Field | Value |
|---|---|
| Precondition | Node restarts with a persisted log tail beyond the last snapshot. |
| Postcondition | Every `ClusterStateInner` field replayed identically to the original `apply_command` sequence (FM-CLUSTER-042/089: replay and handoff deadlines are pure functions of replicated data, never a clock read at apply, so replay is safe regardless of wall-clock skew between original apply and replay). |
| Source | openraft's own log-replay path over `cluster/src/storage.rs`'s `raft_logs` CF |
| Rulings | — |

## TR-CLUSTER-032 — Snapshot install (`install_snapshot`)

| Field | Value |
|---|---|
| Precondition | This node fell far enough behind that the leader ships a snapshot instead of log entries. |
| Postcondition | `ClusterStateInner` replaced wholesale from the snapshot DTO; "Handoff attempt counter" carried through (not rewound, FM-CLUSTER-100); a role change the skipped entries would have produced is synthesized (`RoleChangeEvent`, FM-CLUSTER-045); `self_node_id` preserved across the swap (State-space row "This node's own id"). |
| Source | `cluster/src/state.rs:791-796` (`emit_self_role_change`, the role-change synthesis called by snapshot installs); `state.rs:1066` (`install_snapshot`); `state.rs:145-170` (`from_snapshot`) |
| Rulings | — |

## TR-CLUSTER-033 — Self-role reconciliation tick

| Field | Value |
|---|---|
| Precondition | The replicated "Node role"(self_node_id) and the data path's own believed role disagree, persisting across ticks. |
| Postcondition | Re-emits the correct `RoleChangeEvent` every tick until agreement (`ReDriven`); reports `Agreed` once converged; reports `Detached` if the consumer (a weak sender) is gone rather than leaking a send into nothing. |
| Source | `cluster/src/state.rs:825-890` (`SelfRoleReconciler`: `self_role`, `reconcile`, `emit`) |
| Rulings | — |

### Group H — Admin edge cases

## TR-CLUSTER-034 — Handoff barrier arm/release (per-node reaction to a replicated handoff event)

| Field | Value |
|---|---|
| Precondition | `SlotHandoffPrepared` or `SlotHandoffReleased` applies (on every node, since Raft entries apply everywhere). |
| Postcondition | `plan_handoff_action` returns `Arm` only on the node whose id equals `source_node` for a `Prepared` event: "Slot-scoped write barrier"(slot) armed (`WRITE` mode), shard drain spawned. `Release`: barrier lifted. Every other node: `Ignore`. |
| Source | `cluster-runtime/src/handoff_barrier.rs` (`plan_handoff_action`, the runner's recv loop) |
| Rulings | — |

## TR-CLUSTER-035 — `CLUSTER RESET SOFT`/`HARD`

| Field | Value |
|---|---|
| Precondition | Node-issued `ResetCluster{node_id, new_node_id}`; `new_node_id: Some` selects HARD. |
| Postcondition | Both SOFT and HARD, unconditionally: "Slot ownership" cleared entirely (`commands.rs:821`, not preserved by SOFT); "Node membership" reduced to just this node (`commands.rs:833,837`); this node's own "Node role" forced to `Primary` with "Node parent pointer" nulled (`commands.rs:834-835`, even if it was a replica); "Open migrations"/handoffs cleared, paying every release event the prepared handoffs owe (`commands.rs:826-829`); "Handoff attempt counter" rewound to 0 (the one rewind, FM-CLUSTER-086/100). HARD only, additionally: `node_id` -> `new_node_id` in "Node membership" (`commands.rs:842,844`) and "Node's own config epoch"/"Cluster config epoch" both reset to 0 (`commands.rs:841,843`). If `node_id` is not a member — a reset racing a `FORGET` — the `else` branch (`commands.rs:855-858`) leaves "Node membership" *empty* instead of self-only, and the reset still succeeds (a GAP candidate: nothing forces this branch today). |
| Source | `cluster/src/commands.rs:816-863` (`ResetCluster` arm) |
| Rulings | — |

## TR-CLUSTER-036 — `FinalizeUpgrade`

| Field | Value |
|---|---|
| Precondition | Proposed only after all nodes are externally verified at the target version (operator-level precondition, not state-machine-checked). |
| Postcondition | "Rolling-upgrade version gate" = `version`, irreversibly (no command rewinds `active_version` to `None`). |
| Source | `cluster/src/commands.rs` (`FinalizeUpgrade` arm) |
| Rulings | — |

## TR-CLUSTER-037 — Raft vote persistence

| Field | Value |
|---|---|
| Precondition | openraft calls `save_vote` (this node's own candidacy or a grant to a peer). |
| Postcondition | State-space "Raft vote" written **synced** to `raft_meta`/`KEY_VOTE` before `save_vote` returns `Ok` — the acknowledgement and the durability are the same event (FM-CLUSTER-098). |
| Source | `cluster/src/storage.rs:579` (`save_vote`) |
| Rulings | — |

## TR-CLUSTER-038 — Raft log truncate/purge

| Field | Value |
|---|---|
| Precondition | openraft issues `DeleteConflictLog{since}` (truncate) on a leadership-flap conflict, or a purge past a snapshot's watermark. |
| Postcondition | `truncate(log_id)` removes `[log_id.index, +oo)` from both "Raft log entries" and "Raft log reader cache" (same bound, one call, FM-CLUSTER-099/103); `purge(log_id)` removes `[0, log_id.index]` inclusive and updates "Raft last-purged watermark". |
| Source | `cluster/src/storage.rs` (`truncate`, `purge`) |
| Rulings | — |

## TR-CLUSTER-039 — Raft voter-set side effect of a membership-changing command

| Field | Value |
|---|---|
| Precondition | A committed `ClusterCommand` changes "Node membership" or a node's voter status: `AddNode` (new member), `RemoveNode`, `Failover{force: true}` (remove path only — a graceful/demote `Failover` leaves the demoted node a voting member, FM-CLUSTER-101). |
| Postcondition | `voter_change` maps the committed command to `AddVoters`/`RemoveVoters`/`RemoveNodes`/none, planned against the membership actually in force (`plan_voter_removal`); the last voter is never removed; a command the state machine itself refused never reaches this side effect at all. |
| Source | `cluster/src/network.rs` (`voter_change`, `spawn_add_raft_voter`, `spawn_remove_raft_voter`); three commit sites consult it: leader-local (`connection/cluster.rs`), follower-forward (`network.rs` `ForwardedWrite` receiver), auto-failover (`cluster-runtime/src/failure_detector.rs` via the `DetectorRaft` seam) |
| Rulings | Issue 20 (changes which `Failover` calls reach the remove-voter path, since the automatic path defaults to `force: false`) |

## TR-CLUSTER-040 — `ClusterWriter::propose` on the leader

| Field | Value |
|---|---|
| Precondition | A `ClusterCommand` proposed on the current Raft leader. |
| Postcondition | Raft commits; the state machine's own accept/refuse verdict rides back through `ClusterResponse::{Ok, Epoch, Error}` on the *success* channel (`Proposed::Committed`) — a state-machine rejection is never reported as a transport failure (FM-CLUSTER-047). |
| Source | `cluster/src/writer.rs` |
| Rulings | — |

## TR-CLUSTER-041 — `ClusterWriter::propose` on a follower

| Field | Value |
|---|---|
| Precondition | A `ClusterCommand` proposed on a non-leader node. |
| Postcondition | Forwards over the cluster bus; on success answers `Proposed::Forwarded` (the follower performs none of the leader's side effects, e.g. does not double-run `voter_change`); on failure with a known, addressable leader answers `REDIRECT <id> <addr>`; with no addressable leader, `CLUSTERDOWN No leader available` (FM-CLUSTER-048/049); any other Raft failure surfaces as `ProposeError::Raft(message)`, never laundered into a redirect (FM-CLUSTER-050). |
| Source | `cluster/src/{writer,network}.rs` |
| Rulings | — |

## TR-CLUSTER-042 — `Failover` state-machine application (`force: true`)

*Belongs topically to Group D — Failover and promotion; appended here with the next free id rather
than renumbering TR-CLUSTER-021..041 in place.*

| Field | Value |
|---|---|
| Precondition | `new_primary_id` present in "Node membership"; `old_primary_id` != `new_primary_id`. Unlike TR-CLUSTER-018's graceful path, today's code lets `old_primary_id` be absent from "Node membership" — the `!old_exists && !force` guard is waived when `force = true` (`commands.rs:408-412`); under issue 19's ruled belt that waiver becomes unreachable on the accept path, since an absent `old_primary_id` always fails the no-op-move check below, leaving `force: true` distinguished from `force: false` only by the removal-vs-demotion fate of a *present* old primary — making the waiver at `commands.rs:408-412` a deletion candidate once issue 19 lands. Ruled + amended (issue 19, the defect this row exists to close): apply carries the same per-object fence as TR-CLUSTER-018 when `old_primary_id` **is** present and was observed by the proposer — refuses if that node's "Node's own config epoch" or "Node role" changed since observation. When `old_primary_id` is **absent**, the fence has nothing to check against, so the belt issue 19 also specifies is what remains: apply refuses outright as a no-op move if `old_primary_id` is not a member, owns no slots, or `new_primary_id` is not `old_primary_id`'s replica — the exact case a second, redundant `FAILOVER FORCE`/`TAKEOVER` (or a race against an already-completed prior failover) produces today, where the command currently proceeds and promotes a successor that inherits nothing (issue 19's title scenario). |
| Postcondition | Once past the fence/belt: same mutation shape as TR-CLUSTER-018 except step 3 — "Node membership" loses `old_primary_id` outright (removed, `commands.rs:442`) rather than demoted in place; no `NodeDemoted` event is emitted for it (`graceful_demotion = !force` is `false`, `commands.rs:438`); "Open migrations" naming `old_primary_id` are pruned with releases (`prune_migrations_naming`, `commands.rs:450`); `old_primary_id`'s remaining replicas (if any were still parented to it) are re-parented to `new_primary_id` (`reparent_children`, `commands.rs:459`); "Cluster config epoch" += 1, stamped onto `new_primary_id`'s "Node's own config epoch" (`commands.rs:466-469`); `NodePromoted` fires iff `new_primary_id` was a replica. Raft membership also drops `old_primary_id` as a voter in the same commit (TR-CLUSTER-039). |
| Source | `cluster/src/commands.rs:393-499` (`Failover` arm, `force: true` path within it); `server/src/commands/cluster/admin.rs:312-347` (the replica-issued `FAILOVER FORCE`/`TAKEOVER` handler that constructs this command, `force: force \|\| takeover` at line 342); fence/belt not yet in code |
| Rulings | Issue 19 (per-object fence + no-op belt, amended) |
| Pending | [issue 19](../.scratch/cluster-correctness/issues/open/19-a-forced-failover-promotes-a-node-that-inherits-nothing.md) |

Which admin path reaches this row: after issue 28's ruling lands (TR-CLUSTER-020), the
primary-issued absorb branch of `cluster_failover` (`admin.rs:277-310`) is refused unconditionally,
so the only surviving caller is the replica-issued branch (`admin.rs:312-347`) — a replica naming
its own recorded primary and forcing past the health check. That branch, not the absorb branch, is
this row's `Source`.

---

## How to read a row

| Field | Meaning |
|---|---|
| Trigger | The concrete precondition or interleaving that puts the cluster in this mode. |
| Observable | What a client of some node sees: the reply, the redirect, `CLUSTER INFO`/`NODES`, the slot map. |
| NOT observable | What must never appear in this mode. This is the half mutation testing attacks. |
| Invariant | The internal guarantee, named at the mechanism that provides it. |
| Catalog | *Optional.* The invariant-catalog entries that check this row's guarantee **universally** — see below. Absent on rows nothing in the catalog generalizes. |
| Outcome variant | The enum variant / error string the mode reports through, or `n/a`. |
| Forced by | The test(s) that fail if the behavior changes. Every one carries a `// FM-CLUSTER-NNN` tag at its definition site; `just lint-spec` enforces both directions. |
| Bug refs | Known issues that touch this mode. |

Test names are bare function names, resolved against the crate list in `scripts/spec-lint.py`
(`NEXTEST_CRATES`).

### The `Catalog` field

A row states its guarantee for *one* transition; the invariant catalog
(`frogdb-server/crates/cluster/src/invariants.rs`) states a well-formedness claim for *every*
state, and `debug_assert_clean` evaluates the HARD tier at all three seams that produce
one — `apply_command` (on the accepted **and** the rejected path), `from_snapshot`, and
`restore_from_snapshot`. Every test in the crate that applies a command is therefore also an
invariant test, and the property harness (`properties.rs`) quantifies the same catalog over
generated command sequences.

A row carries a `Catalog` field when a catalog entry generalizes the guarantee the row states
point-wise: deleting the code the row names would make that entry fire. The cross-reference runs
both ways — each entry's `check_*` function names the rows it generalizes — and
`just lint-spec` checks that every `INV-*` id a spec mentions exists in the catalog.
`INV-SLOT-1` (slot keys below `CLUSTER_SLOTS`) is deliberately cited by no row: FM-CLUSTER-018
derives the range by hashing and FM-CLUSTER-075 enforces it at the `SlotRange` parse boundary, but
no row states it of the replicated slot map, so the entry is a backstop rather than a
generalization.

---

## FM-CLUSTER-001 — `AddNode` is an upsert that can never demote a node by restarting it

| Field | Value |
|---|---|
| Trigger | A node re-registers itself through `CLUSTER MEET` or on its own boot. It sends the only role it can know unaided — `Primary` — even when the cluster has since made it a replica. |
| Observable | The node's `addr` and `cluster_addr` are refreshed to the values in the incoming record; its `role` and `primary_id` in `CLUSTER NODES` are exactly what they were before. A node that has never been seen is added verbatim. The command always answers `ClusterResponse::Ok`. |
| NOT observable | A restart flipping a replica back to `master` in `CLUSTER NODES` — which would point its replication stream at itself and orphan the primary's write path. Nor `NodeAlreadyExists`: that variant exists in `ClusterError` but `AddNode` never constructs it, so a re-register is never an error. Nor a role/promotion event: an `AddNode` that changes nothing emits nothing. |
| Invariant | `apply_command`'s `AddNode` arm overwrites the incoming `role`/`primary_id` from the existing record before inserting (`commands.rs:43-54`), so role transitions have exactly one source — `SetRole` and `Failover` — and re-registration is not one of them. A binary-version mismatch against the highest version any *other* versioned peer reports warns and never rejects, so a mixed-version cluster still converges on membership; the warning carries that value under `max_peer_version`, not a name like `cluster_version` that would misread it as the cluster's consensus version. |
| Catalog | `INV-REF-4` — the arm restores role and `primary_id` together, so no re-registration can leave a primary carrying a parent. |
| Outcome variant | `ClusterResponse::Ok` |
| Forced by | `test_add_node`, `test_add_duplicate_node`, `test_add_node_reregistration_keeps_recorded_role`, `test_add_node_mixed_version_succeeds`, `test_apply_local_shares_validated_path`, `a_disagreeing_re_registration_is_reported_on_either_half`, `test_node_flags_handshaking_sets_only_the_handshake_bit`, `mixed_version_warning_compares_against_the_other_versioned_nodes` |
| Bug refs | — |

`MEET` and `FORGET` have no `ClusterCommand` of their own: they are server-layer verbs that lower to
`AddNode` / `RemoveNode`, so their replicated semantics are exactly rows 001 and 002.

## FM-CLUSTER-002 — `RemoveNode` orphans the departing node's slots and prunes every reference to it

| Field | Value |
|---|---|
| Trigger | `CLUSTER FORGET <node>` against a node that owns slots, has open migrations, or has replicas parented to it. |
| Observable | The node disappears from `CLUSTER NODES`. Every slot it owned becomes *unassigned* — a client keyed into one of those slots gets `CLUSTERDOWN`, not a `MOVED` to some successor. Every migration naming it as source *or* target is cancelled, and a cancelled migration that had a prepared handoff emits its `SlotHandoffReleased` so the surviving source drops the barrier it armed. Every replica parented to it is *detached*: its `primary_id` clears, its role does not change, and `CLUSTER NODES` renders `-` for its master id. Migrations between two surviving nodes and replicas of other primaries are untouched. Forgetting an unknown node is `NodeNotFound`. |
| NOT observable | Slots silently retargeted at another node: `FORGET` is not a failover, and inventing a successor here would hand a keyspace to a node that never received its data. For the same reason the orphaned replicas are not re-parented onto some other primary — `FORGET` names no successor, so there is nobody to re-parent them onto. Nor a surviving migration or `primary_id` that names a node the cluster no longer has: the migration could never complete and would block its slot, and the parent pointer would render a dangling master id in `CLUSTER NODES` and point a replication stream at a node the topology has dropped. Nor an epoch bump — removing a node changes no node's authority. Nor a promotion of the detached replicas: minting a replication identity is a role transition, and `FORGET` is not one of the two commands that own those (FM-CLUSTER-001). |
| Invariant | The arm mirrors the removal half of `Failover { force: true }` (FM-CLUSTER-036) through the same two helpers, so the two removal paths cannot drift: `slot_assignment.retain(owner != node_id)`, then `prune_migrations_naming` (whose release events are the `release_events` contract), then `reparent_children(.., None)` — the same helper the failover calls with `Some(successor)` — then `nodes.remove` (`commands.rs`, `RemoveNode` arm). The paths differ only in whether a successor was named: transfer of slots and role still live only in `Failover`, which validates that successor first. Redis parity: `clusterDelNode` clears the node's `migrating_slots_to`/`importing_slots_from` entries and `freeClusterNode` nulls its slaves' `slaveof` without promoting them. |
| Catalog | `INV-REF-1`, `INV-REF-2`, `INV-REF-3` — the three dangling-reference shapes this arm prunes, checked after every transition rather than only after this one. |
| Outcome variant | `ClusterResponse::Ok` / `ClusterError::NodeNotFound`; `ClusterEvent::SlotHandoffReleased` per pruned prepared handoff |
| Forced by | `test_remove_node_clears_slots`, `test_remove_node_nonexistent`, `remove_node_prunes_migrations_and_detaches_replicas`, `remove_node_prunes_a_migration_that_only_targets_it_and_releases_its_barrier`, `remove_node_keeps_migrations_and_parents_that_do_not_name_it` |
| Bug refs | fixed: issue 01, `.scratch/cluster-correctness/issues/`; round-2 issue 62 (11/F5), `.scratch/testing-improvements-round2/issues/` |

The dangling references `RemoveNode` used to leave — pinned until 2026-08-08 by
`remove_node_leaves_migrations_and_replicas_dangling`, which now asserts the pruning under its new
name — were the ruling PRD §8 D2 reversed: fix the behavior rather than register the dirty state as
an invariant exception. The slots staying *unassigned* is the part that remains deliberate.

## FM-CLUSTER-003 — slot assignment is validate-all-then-apply

| Field | Value |
|---|---|
| Trigger | `CLUSTER ADDSLOTS` naming a range in which one slot is already owned by a *different* node. |
| Observable | The whole command is refused with `SlotAlreadyAssigned(slot, owner)` and **no** slot in the batch changes hands. Re-assigning a slot to the node that already owns it is idempotent, not an error. Assigning to an unknown node is `NodeNotFound`. |
| NOT observable | A partially applied batch — the prefix of the range moved and the suffix refused. That would leave the caller with an error and the cluster with a slot map neither side believes in. |
| Invariant | The arm walks every slot of every range in a validation pass before it mutates anything (`commands.rs:118-140`), and `apply_command` holds one write lock for the whole command, so the transition is a single linearization point. |
| Catalog | `INV-REF-1` — the `NodeNotFound` half: a slot assigned to a non-member is the ghost owner the catalog rejects wherever it appears. |
| Outcome variant | `ClusterError::SlotAlreadyAssigned` |
| Forced by | `test_assign_slots`, `test_assign_slots_node_not_found`, `test_assign_slots_already_assigned`, `test_assign_slots_idempotent`, `assign_slots_rejects_the_whole_batch_on_one_conflict` |
| Bug refs | — |

## FM-CLUSTER-004 — `RemoveSlots` unassigns only slots the named node owns

| Field | Value |
|---|---|
| Trigger | `CLUSTER DELSLOTS` naming slots that are assigned to a node other than the one in the command. |
| Observable | The whole batch is refused with `InvalidOperation("slot N is owned by X, not Y")` and no slot changes hands. The owner removing its own slots succeeds. Naming a slot nobody owns refuses the whole batch with `SlotNotAssigned(slot)`. |
| NOT observable | A partially applied batch — the same validate-all-then-apply shape as FM-CLUSTER-003. Nor a node's slots being unassigned out from under it by a command naming a different node: every key in them would answer `CLUSTERDOWN` until someone re-assigned them. |
| Invariant | The validation pass asserts both that each slot has an owner and that the owner is `node_id` (`commands.rs:142-170`), matching the strictness of every sibling slot-mutating arm (`AssignSlots`, `BeginSlotMigration`, `CompleteSlotMigration`). Redis' `CLUSTER DELSLOTS` is a *local* unassignment with no node argument, so ownership is structural there; FrogDB's is replicated and carries an explicit id, so the assertion has to be made rather than assumed. The server-layer handler only ever fills `node_id` from `ctx.node_id`, so the check is invisible to a well-formed `CLUSTER DELSLOTS`. |
| Outcome variant | `ClusterError::{SlotNotAssigned, InvalidOperation}` |
| Forced by | `test_remove_slots_success`, `test_remove_slots_not_assigned`, `remove_slots_rejects_a_slot_owned_by_another_node`, `remove_slots_rejects_the_whole_batch_when_one_slot_is_foreign` |
| Bug refs | fixed: [33-remove-slots-is-owner-blind.md](../.scratch/hardening/issues/done/33-remove-slots-is-owner-blind.md) |

## FM-CLUSTER-005 — a replica role always names a primary that exists

| Field | Value |
|---|---|
| Trigger | `CLUSTER REPLICATE <node>`, or any `SetRole` proposal. |
| Observable | `SetRole{Replica}` with no `primary_id` is `InvalidOperation("replica requires a primary_id")`; with a `primary_id` naming a node that is not a member it is `NodeNotFound(pid)`. Only a fully resolvable pairing is recorded. |
| NOT observable | A replica recorded against a primary the cluster has never heard of — its data path would have nothing to attach to and `CLUSTER NODES` would render a dangling master id. |
| Invariant | Both checks run before any mutation (`commands.rs:172-182`), so a rejected `SetRole` leaves role, parent, and events untouched. |
| Catalog | `INV-REF-3` — the existence half, universally. The *parent is a Primary* half is the catalog's one `Tier::DocumentedException`, `INV-REF-3B`, because `AddNode` and `SetRole` still admit a replica as a parent (issue 14, `.scratch/cluster-correctness/issues/`). |
| Outcome variant | `ClusterError::{InvalidOperation, NodeNotFound}` |
| Forced by | `test_set_role`, `test_set_role_node_not_found`, `test_set_role_replica_without_primary`, `test_set_role_replica_primary_not_found` |
| Bug refs | — |

## FM-CLUSTER-006 — `CLUSTER RESET` SOFT keeps this node's identity, HARD mints a new one

| Field | Value |
|---|---|
| Trigger | `CLUSTER RESET [SOFT|HARD]` on a node with an empty keyspace (a non-empty one is refused at the command layer before it reaches Raft). |
| Observable | Both forms clear the slot map, clear all migrations, forget every peer, and force this node to `Primary` with no parent. SOFT keeps the node id and both epochs. HARD re-keys the node under a freshly minted id and zeroes both the cluster counter and the node's own epoch. A reset naming a node that is not a member still clears everything and answers `Ok`. On a follower the reset is forwarded to the leader, or answered `REDIRECT`/`CLUSTERDOWN` if it cannot be — reset proposes through `ClusterWriter` like every other admin command (FM-CLUSTER-047..050). |
| NOT observable | A reset that fails: the arm is infallible by construction, because a half-reset node is worse than a fully reset one. Nor a raw `ERR Raft error: APIError(ForwardToLeader(..))` on a follower — the `Debug` rendering of an openraft internal is not a redirect any client can follow. Nor a node that re-keyed itself locally after a reset that never landed. |
| Invariant | `commands.rs:478-518` clears the slot map and migrations unconditionally before it branches, so no slot or migration can survive either form. `ClusterWriter::propose_reset` snapshots the peer list *before* proposing (a committed reset empties the topology), routes through `propose`, and applies the local identity update only on `Applied`. |
| Catalog | `INV-REF-4` — "Primary with no parent" as a property of every state, not only of the one a reset produces. |
| Outcome variant | `ClusterResponse::Ok` / `ResetProposed::{Applied, Rejected}` / `ProposeError::{Redirect, Raft}` |
| Forced by | `test_reset_cluster_soft`, `test_reset_cluster_hard`, `test_reset_cluster_nonexistent_node`, `reset_on_a_follower_yields_a_redirect_not_a_raft_error`, `reset_forwarded_to_the_leader_reports_forwarded`, `reset_committed_on_the_leader_keeps_a_soft_reset_identity`, `reset_rejected_by_the_state_machine_changes_nothing_local`, `self_node_id_treats_zero_as_unset`, `from_snapshot_restores_the_table_and_keeps_the_local_identity` |
| Bug refs | fixed: [39-cluster-reset-bypasses-the-cluster-writer.md](../.scratch/hardening/issues/done/39-cluster-reset-bypasses-the-cluster-writer.md) |

`CLUSTER RESET HARD` is why the config epoch is **not** globally monotonic — see FM-CLUSTER-010's
non-guarantee. It also changes the node's identity inside replicated state; the local
`self_node_id` atomic is updated by `propose_reset` on the `Applied` path — including the forwarded
one, because the entry that re-keyed this node was applied on the leader and a follower still
announcing its old id would contradict the topology it is about to receive. The connection layer
keeps only the transport cleanup (`network_factory.remove_node`), which is the piece the cluster
crate cannot reach.

## FM-CLUSTER-007 — one function owns the bootstrap slot split

| Field | Value |
|---|---|
| Trigger | Bootstrapping a cluster, through either the local-seed path or the Raft-replicated path. |
| Observable | With N nodes each gets a contiguous `16384/N`-slot range in the order the ids were given; the last node absorbs the remainder, so every one of the 16384 slots is owned exactly once. Position 0 always owns the range starting at slot 0, whatever the numeric ids are. Zero nodes yields no ranges. |
| NOT observable | A gap or an overlap in the split — either one is a keyspace a client can address and no node will serve. Nor a different split depending on which bootstrap path ran. |
| Invariant | `even_slot_ranges` is the single source both paths call, and `test_even_slot_ranges_cross_path_equivalence` compares the two command sequences directly rather than trusting the call sites. |
| Outcome variant | n/a |
| Forced by | `test_even_slot_ranges_partition_correctness`, `test_even_slot_ranges_order_preservation`, `test_even_slot_ranges_exact_boundaries`, `test_even_slot_ranges_empty`, `test_even_slot_ranges_cross_path_equivalence` |
| Bug refs | — |

## FM-CLUSTER-008 — a rolling upgrade cannot be finalized while a straggler is behind

| Field | Value |
|---|---|
| Trigger | `FinalizeUpgrade{version}` while some member is still running an older binary, a pre-versioning binary (empty version string), or an unparseable one. |
| Observable | `InvalidOperation` naming the offending node and its version; `active_version` is unchanged, so every version-gated feature stays off. An unparseable *target* is `InvalidOperation("invalid target version …")` whether or not the cluster has members. With every node at or above the target it succeeds, and re-finalizing the same version succeeds again. A cluster with no members at all accepts a valid target (bootstrap). |
| NOT observable | A finalization that half-applies — the check loop runs to completion before `active_version` is written. Nor a rejection of a node that is *ahead* of the target: a mixed cluster mid-upgrade is the normal state. Nor a target version that no gate can parse being accepted: `is_gate_active_in` reads an unparseable active version as *inactive* (FM-CLUSTER-009), so storing one silently disables every gate, and the operation is documented irreversible. |
| Invariant | The target is semver-parsed **before** the per-node loop it is invariant over (`commands.rs:456-470`), so the zero-member case — where the loop body never runs — is validated by the same parse as every other case. Every node is then parsed and compared before `active_version` is set. |
| Outcome variant | `ClusterError::InvalidOperation` |
| Forced by | `test_finalize_upgrade_succeeds_when_all_nodes_at_target`, `test_finalize_upgrade_rejects_when_node_behind`, `test_finalize_upgrade_rejects_empty_version_node`, `test_finalize_upgrade_allows_nodes_ahead_of_target`, `test_finalize_upgrade_invalid_target_version`, `test_finalize_upgrade_idempotent`, `finalize_upgrade_rejects_an_unparseable_target_with_no_nodes`, `finalize_upgrade_with_no_nodes_accepts_a_valid_target` |
| Bug refs | fixed: [34-finalize-upgrade-accepts-garbage-with-no-nodes.md](../.scratch/hardening/issues/done/34-finalize-upgrade-accepts-garbage-with-no-nodes.md) |

## FM-CLUSTER-009 — a version gate refuses until finalization raises the active version

| Field | Value |
|---|---|
| Trigger | Asking whether a gated feature may run, in any state of the rolling upgrade. |
| Observable | The gate is active only when `active_version` parses and is at or above the gate's minimum. No active version, an unparseable active version, and an unknown gate name all report inactive. `pending_gates` lists what a given target would unlock, excluding what is already active, and is empty for a patch bump that unlocks nothing. |
| NOT observable | A gate defaulting to *on* in any ambiguous state — a feature that half the cluster cannot parse is the failure this exists to prevent. An unparseable target version yields an empty pending list rather than an error, so the caller never treats a parse failure as "everything is unlocked". |
| Invariant | `is_gate_active_in` is a pure lookup with a single `>=` comparison and `find(..).is_some_and(..)`, so every refusal path collapses to `false` (`version_gate.rs:47-75`). |
| Outcome variant | `bool` / `Vec<&VersionGateEntry>` |
| Forced by | `gate_active_when_version_meets_minimum`, `gate_inactive_when_version_below_minimum`, `gate_inactive_when_no_active_version`, `none_active_version_means_inactive`, `invalid_active_version_means_inactive`, `no_gates_means_nothing_active`, `static_gate_active_when_version_sufficient`, `pending_gates_static_registry`, `pending_gates_returns_unlockable_gates`, `pending_gates_excludes_already_active`, `pending_gates_empty_for_patch_upgrade`, `gate_suppression_is_reported_once_and_only_when_suppressed` |
| Bug refs | — |

---

## FM-CLUSTER-010 — the cluster counter never sits below any node's epoch

| Field | Value |
|---|---|
| Trigger | Any sequence of `AddNode` / `IncrementEpoch` / `Failover` / `MarkNodeFailed` / reset. |
| Observable | `cluster_current_epoch` in `CLUSTER INFO` is always at least the largest `config_epoch` in `CLUSTER NODES`, and no two *primaries* share a nonzero epoch. A collision mint always lands strictly above the counter's previous value. |
| NOT observable | Two primaries claiming the same nonzero epoch — the state in which neither can be said to have superseded the other. Nor a mint that produces a value some node already holds. |
| Invariant | `mint_config_epoch` is `counter.max(max_node_epoch()) + 1` (`state.rs:261-264`), so the mint reads the node epochs rather than trusting the counter, and the uncontested branch of `reconcile_incoming_epoch` ratchets the counter up to any accepted claim. |
| Catalog | `INV-EPOCH-1`, `INV-EPOCH-2` — the dominance relation and the primary-uniqueness claim this row states, evaluated after every transition instead of over one generated command sequence. |
| Outcome variant | `EpochReconciliation::{Accepted, Reassigned}` |
| Forced by | `test_config_epoch_counter_dominates_every_node_epoch_across_command_sequence`, `test_add_node_collision_mints_above_cluster_counter`, `test_add_node_uncontested_epoch_raises_cluster_counter`, `max_node_epoch_tracks_the_highest_claim` |
| Bug refs | — |

Deliberate non-guarantee: the epoch is **not** globally monotonic across `CLUSTER RESET HARD`, which
zeroes the counter (FM-CLUSTER-006). Assertions of the form `cluster_current_epoch` only ever
increases are wrong; the invariant is the dominance relation above, within a cluster's lifetime.

## FM-CLUSTER-011 — an epoch collision is resolved against the node that arrives

| Field | Value |
|---|---|
| Trigger | An `AddNode` for a primary whose claimed nonzero `config_epoch` is already held by a *different* primary. |
| Observable | The incumbent keeps the contested epoch; the arriving node is stamped with a freshly minted one and a warning is logged. A node re-claiming *its own* recorded epoch is an ordinary upsert, not a collision. A *replica* claiming a primary's epoch is not a collision either. |
| NOT observable | The incumbent being renumbered — the node that is already serving the slot map at that epoch is the one whose authority must not move. Nor a rejection: `AddNode` never refuses on epoch grounds, because refusing membership over a numbering conflict partitions the cluster instead of healing it. |
| Invariant | The collision test requires `node.is_primary()` and a *different* id at the same epoch (`state.rs:296-300`), matching Redis' `clusterHandleConfigEpochCollision`, which likewise renumbers only one side and exempts replicas. |
| Catalog | `INV-EPOCH-1`, `INV-EPOCH-2` — a mint that failed to clear the counter, or that left both primaries on the contested epoch, is caught wherever it happens. |
| Outcome variant | `EpochReconciliation::Reassigned{claimed, assigned}` |
| Forced by | `test_add_node_epoch_collision_reassigns_incoming_node`, `test_add_node_self_epoch_is_not_a_collision`, `test_add_node_replica_sharing_primary_epoch_is_not_a_collision` |
| Bug refs | — |

## FM-CLUSTER-012 — epoch 0 means "unassigned" and is preserved, never contested

| Field | Value |
|---|---|
| Trigger | A node registering with `config_epoch: 0` — the value `NodeInfo::new_primary` produces, i.e. every fresh boot. |
| Observable | If the cluster already holds a nonzero epoch for that node id, the recorded value is preserved and the incoming 0 is discarded (the address is still refreshed). If it does not, 0 is accepted and the cluster counter is left alone. Two nodes may both sit at 0. |
| NOT observable | A restart resetting a node's epoch to 0 and thereby forfeiting its authority. Nor two nodes at 0 being treated as a collision and burning a mint on every boot. |
| Invariant | The zero branch of `reconcile_incoming_epoch` runs *before* the collision test and returns early (`state.rs:286-292`), so 0 can neither collide nor move the counter. |
| Catalog | `INV-EPOCH-2` — the entry skips epoch `0` for the reason this row gives: it is "unassigned", not a claim, so it cannot collide. |
| Outcome variant | `EpochReconciliation::{Preserved, Accepted}` |
| Forced by | `test_add_node_zero_epoch_is_not_a_collision`, `test_add_node_zero_epoch_preserves_recorded_epoch` |
| Bug refs | — |

## FM-CLUSTER-013 — flagging a node FAIL and bumping the epoch is one transition

| Field | Value |
|---|---|
| Trigger | The leader's reconciliation writes `MarkNodeFailed{node}`. |
| Observable | `CLUSTER NODES` shows the `fail` flag and `CLUSTER INFO` shows a raised `cluster_current_epoch`, together, in the entry that carried them. Marking an unknown node is `NodeNotFound` and moves no epoch. The response is `Ok`, not `Epoch`. |
| NOT observable | A FAIL flag visible at a stale epoch — a peer that read the flag but not the bump would treat the failed node's slot claims as still current. That is exactly what a separate `IncrementEpoch` entry would allow if the leader crashed between the two. |
| Invariant | The arm sets `flags.fail` and increments `config_epoch` inside one `apply_command` write lock (`commands.rs:335-348`), so no reader can observe one without the other. |
| Outcome variant | `ClusterResponse::Ok` / `ClusterError::NodeNotFound` |
| Forced by | `test_mark_node_failed`, `test_mark_node_failed_nonexistent`, `test_mark_node_failed_bumps_epoch_atomically`, `test_mark_node_failed_missing_node_does_not_bump_epoch` |
| Bug refs | — |

## FM-CLUSTER-014 — recovery clears both failure flags and moves no epoch

| Field | Value |
|---|---|
| Trigger | The leader's reconciliation writes `MarkNodeRecovered{node}` after the peer answers probes again. |
| Observable | Both `fail` and `pfail` are cleared. `cluster_current_epoch` is unchanged. An unknown node is `NodeNotFound`. |
| NOT observable | An epoch bump on recovery. The asymmetry with FM-CLUSTER-013 is deliberate: marking a node down changes who may claim its slots, so it needs a new epoch; welcoming it back changes nothing about authority, and bumping there would let a flapping peer churn the epoch once per round. |
| Invariant | The arm clears both flags and touches nothing else (`commands.rs:350-359`); the anti-churn guarantee upstream of it is the detector's hysteresis (FM-CLUSTER-052) plus level-triggered reconciliation, which performs zero writes on a converged cluster. |
| Outcome variant | `ClusterResponse::Ok` / `ClusterError::NodeNotFound` |
| Forced by | `test_mark_node_recovered`, `test_mark_node_recovered_nonexistent` |
| Bug refs | — |

`pfail` is structurally supported everywhere (flags, health counting, `CLUSTER NODES` rendering) but
is **never produced** — FrogDB has no gossip suspicion phase, so nothing sets it and only
`MarkNodeRecovered` clears it. Rows 073 and 071 pin the rendering anyway, so a future suspicion
phase inherits a specced surface.

## FM-CLUSTER-015 — a proposal's result carries its type, not a string

| Field | Value |
|---|---|
| Trigger | Any `ClusterCommand` applied through Raft. |
| Observable | `IncrementEpoch` answers `ClusterResponse::Epoch(n)` with the new value; a rejected command answers `ClusterResponse::Error(e)` carrying the typed `ClusterError`; everything else answers `Ok`. All three survive a JSON round trip across the apply boundary. |
| NOT observable | An error flattened to a string before it crosses the state-machine boundary — the caller could no longer distinguish `SlotAlreadyAssigned` from `NodeNotFound` without parsing prose. |
| Invariant | `ClusterResponse` is openraft's `R` type, so the typed value is what `client_write` returns; `as_error()` is the only accessor and it matches a single variant. |
| Outcome variant | `ClusterResponse::{Ok, Epoch, Error}` |
| Forced by | `test_increment_epoch`, `increment_epoch_returns_typed_epoch`, `test_cluster_response_as_error`, `test_cluster_response_serde_round_trip`, `non_event_command_returns_no_events` |
| Bug refs | — |

## FM-CLUSTER-016 — `CLUSTER INFO` reports the replicated counter verbatim and the Raft term separately

| Field | Value |
|---|---|
| Trigger | `CLUSTER INFO` on any node. |
| Observable | `cluster_current_epoch` is the replicated counter exactly as stored — a bump is visible even while the Raft term is higher. `cluster_my_epoch` is *this node's* `NodeInfo::config_epoch`, which is generally not the counter. `cluster_raft_term` is a FrogDB extension reported as its own field; on a standalone node the line is omitted entirely. |
| NOT observable | The historical `max(counter, raft_term)` fold, which made an election look like an epoch bump and hid a real bump under a higher term. Nor `cluster_raft_term:0` on a node with no Raft — a field with no meaning reported as a number is worse than an absent field. |
| Invariant | `ClusterInfoReport` takes the counter and the term as separate inputs and renders them as separate lines, with the term as an `Option` so absence is representable. |
| Outcome variant | `CLUSTER INFO` fields |
| Forced by | `test_cluster_info_reports_config_epoch_counter_verbatim`, `test_cluster_info_reports_raft_term_as_its_own_field`, `test_cluster_info_epoch_bump_is_visible_under_a_higher_term`, `test_cluster_info_my_epoch_is_the_per_node_value_not_the_counter`, `test_cluster_info_standalone_omits_the_raft_term_line` |
| Bug refs | — |

## FM-CLUSTER-017 — cluster metadata survives a restart through the log and through a snapshot

| Field | Value |
|---|---|
| Trigger | Restarting a node, with and without a durable state-machine snapshot, and with the log purged behind the snapshot. |
| Observable | Replaying the log into a fresh state machine reproduces the config epoch with no election involved. With a snapshot on disk and an empty log, the epoch and `last_applied` are still correct and the snapshot is what `get_current_snapshot` advertises. A store with nothing persisted advertises nothing rather than synthesizing from live state. A save whose `last_log_id` is at or below the stored one is a silent no-op. |
| NOT observable | A snapshot going backwards, or a snapshot openraft believes exists that is not on disk — openraft purges log entries on the strength of an advertised snapshot, so a synthesized one would discard entries nothing can reproduce. |
| Invariant | `ClusterSnapshotStore::save` compares `last_log_id.index` under a mutex and writes meta plus payload in one `WriteBatch` with `set_sync(true)`; `get_current_snapshot` reads the store rather than the live state whenever a store is attached (`storage.rs:111-147`, `state.rs:808-837`). |
| Outcome variant | `StorageError` / `Option<Snapshot>` |
| Forced by | `test_config_epoch_round_trips_through_storage_restart`, `test_state_machine_snapshot_survives_restart_without_log_replay`, `test_snapshot_save_never_moves_backwards`, `test_snapshot_store_is_opt_in`, `test_storage_open_and_close`, `test_storage_vote`, `test_storage_metadata`, `test_storage_committed`, `membership_entries_are_recorded_for_applied_state`, `snapshot_builder_shares_the_live_state`, `begin_receiving_snapshot_hands_back_an_empty_buffer`, `log_keys_round_trip_and_sort_in_index_order`, `save_committed_writes_then_deletes_the_persisted_key`, `cache_evicts_the_oldest_only_once_over_the_bound`, `cache_invalidation_respects_both_range_ends`, `excluded_start_bound_skips_that_index`, `append_persists_entries_and_log_state_reports_the_tail` |
| Bug refs | — |

## FM-CLUSTER-076 — `SET-CONFIG-EPOCH` assigns the exact value, and only at bootstrap

Numbered out of sequence because it was added after the first pass; it belongs to the config-epoch
group (010..017) and sits here rather than at the end of the file.

| Field | Value |
|---|---|
| Trigger | `CLUSTER SET-CONFIG-EPOCH <n>` on any node, at any point in its life. |
| Observable | On a node that knows no other member and whose own `config_epoch` is still 0, the node's epoch becomes exactly `n` and `cluster_current_epoch` is ratcheted up to `n` if it was lower. Otherwise the command is refused: `InvalidOperation` naming which guard fired, or `NodeNotFound` if the receiving node is not in the topology. A refusal moves no epoch. |
| NOT observable | Success reported for an assignment that did not happen — the previous behavior parsed `n`, discarded it, and issued a plain `IncrementEpoch`, so a bootstrap script asking for epoch 100 got epoch 1 and a `+OK`. Nor a live cluster renumbering a node by hand: FrogDB resolves collisions through FM-CLUSTER-011, so a manual assignment mid-life could only manufacture one. Nor the counter following an assignment *downwards* (FM-CLUSTER-010). |
| Invariant | The `SetConfigEpoch` arm applies Redis' two `clusterCommand` guards (`dictSize(server.cluster->nodes) > 1`, `myself->configEpoch != 0`) before mutating, then sets the field and takes `config_epoch.max(epoch)` on the counter (`commands.rs:238-272`). The server handler is a pure adapter: it parses the argument and emits `RaftClusterOp::SetConfigEpoch { node_id: ctx.node_id, epoch }`, so the value can no longer be dropped between parse and apply. |
| Catalog | `INV-EPOCH-1` — the counter never follows an assignment downwards. |
| Outcome variant | `ClusterResponse::Epoch` / `ClusterError::{InvalidOperation, NodeNotFound}` |
| Forced by | `set_config_epoch_assigns_the_exact_value_requested`, `set_config_epoch_never_lowers_the_cluster_counter`, `set_config_epoch_refused_once_the_node_knows_another_node`, `set_config_epoch_refused_once_the_node_holds_an_epoch`, `set_config_epoch_on_an_unknown_node_is_not_found`, `test_cluster_set_config_epoch_assigns_the_exact_value_on_a_lone_node`, `test_cluster_set_config_epoch_refused_once_the_node_knows_a_peer` |
| Bug refs | fixed: [38-set-config-epoch-discards-its-argument.md](../.scratch/hardening/issues/done/38-set-config-epoch-discards-its-argument.md) |

---

## FM-CLUSTER-018 — a key's slot is its hash tag's slot, and a slot never spans shards

| Field | Value |
|---|---|
| Trigger | Any keyed command in cluster mode. |
| Observable | `CLUSTER KEYSLOT` and the routing layer agree: the slot is CRC16-XMODEM of the hash tag when one is present and of the whole key otherwise, mod 16384. Keys sharing a hash tag share a slot, and keys sharing a slot always share a local shard. |
| NOT observable | Two keys in the same slot landing on different shards — every cross-key guarantee in the codebase (`MULTI`, scripts, multi-key commands) is built on slot co-location implying shard co-location. |
| Invariant | Shard selection is `slot_for_key(key) % num_shards`, derived from the same slot, so the implication holds by construction rather than by agreement between two hashes; `same_slot_implies_same_shard_over_many_keys` sweeps it rather than spot-checking. |
| Outcome variant | n/a |
| Forced by | `same_shard_single_key_co_locates`, `same_slot_single_key_co_locates`, `hash_tagged_keys_co_locate`, `same_slot_implies_same_shard_over_many_keys`, `accepts_borrowed_slice_keys` |
| Bug refs | — |

## FM-CLUSTER-019 — a keyless command is never routed

| Field | Value |
|---|---|
| Trigger | A command with no keys — `PING`, `INFO`, a `MULTI` block containing only keyless commands. |
| Observable | It is served locally on whichever node received it. Slot validation returns "no verdict" rather than a slot. |
| NOT observable | `CLUSTERDOWN` or `MOVED` for a command that names no key: there is no slot to be wrong about, and refusing keyless traffic would make an unassigned slot map take the whole node offline for administration. |
| Invariant | Both validators return `Ok(None)` on an empty key set before computing anything (`validator.rs`), and `route_queued_batch` serves locally when the batch contributed no keys. |
| Outcome variant | `Option<Response>` = `None` / `RouteOutcome::ServeLocal` |
| Forced by | `same_shard_empty_is_none`, `same_slot_empty_is_none`, `batch_with_no_keyed_command_serves_locally` |
| Bug refs | — |

## FM-CLUSTER-020 — keys in two slots are refused, never partially served

| Field | Value |
|---|---|
| Trigger | A multi-key command, a `MULTI` batch, or a `WATCH` set spanning two slots. |
| Observable | `CROSSSLOT Keys in request don't hash to the same slot`, with the identical wire string from all three entry points. |
| NOT observable | The first slot's keys being served and the rest redirected — a partially executed multi-key command has no correct client recovery. |
| Invariant | All three paths funnel through the same seam string; the batch and watch routers compute the slot set first and refuse before any per-slot verdict is taken. |
| Outcome variant | `CROSSSLOT` error |
| Forced by | `cross_slot_pair_rejects_with_seam_string`, `batch_spanning_two_slots_is_crossslot`, `watch_across_two_slots_is_crossslot` |
| Bug refs | — |

## FM-CLUSTER-021 — an owned, stable slot is served locally

| Field | Value |
|---|---|
| Trigger | A key whose slot this node owns, with no migration open on it. |
| Observable | The command executes here. No redirect of any kind is emitted, with or without `ASKING`, with or without `READONLY`. |
| NOT observable | A redirect on the happy path — the cost of a spurious `MOVED` is a client-side topology refresh on every request. |
| Invariant | `route_with_snapshot` is a pure function of `(snapshot, self id, slot, asking, is_restore)`; the local-serve arm is taken before any migration or ownership branch (`routing.rs:133-168`). |
| Outcome variant | `RouteDecision::LocalServe` → `RouteOutcome::ServeLocal` |
| Forced by | `route_local_serve_when_self_owns_no_migration`, `to_response_local_arms_serve_locally`, `batch_on_owned_stable_slot_serves_locally` |
| Bug refs | — |

## FM-CLUSTER-022 — `MOVED` names the owner's client address

| Field | Value |
|---|---|
| Trigger | A key whose slot another node owns. |
| Observable | `MOVED <slot> <host>:<port>`, where the address is the owner's *client* address, not its bus address. An IPv6 owner is rendered bracketed so the client's `host:port` split is unambiguous. The same verdict is reached for a single command and for a queued `MULTI` batch. |
| NOT observable | A `MOVED` naming the cluster bus port — the client would connect to a port that speaks postcard-framed RPC, not RESP. |
| Invariant | The address comes from `NodeInfo::addr` (the client address) and never from `cluster_addr`; rendering goes through one `to_response` so single and batch paths cannot diverge. |
| Outcome variant | `RouteDecision::Moved` → `MOVED` |
| Forced by | `route_moved_when_other_node_owns_no_asking`, `to_response_moved_with_addr_emits_moved`, `to_response_moved_ipv6_is_bracketed`, `batch_on_foreign_slot_is_moved_to_the_owner` |
| Bug refs | `.scratch/hardening/issues/open/31-raft-suite-queued-txn-moved-redirect-and-key-routing-wrong-value-reads.md` |

## FM-CLUSTER-023 — an owner this node cannot address degrades to `CLUSTERDOWN`

| Field | Value |
|---|---|
| Trigger | The slot has an owner id, but that id is not in this node's view of cluster state — a topology entry the local snapshot has not caught up with. |
| Observable | `CLUSTERDOWN`, not a `MOVED`. |
| NOT observable | A `MOVED` with an empty or fabricated address. A client that follows a malformed redirect fails in the client library rather than at the protocol level, which is far harder to diagnose than an explicit `CLUSTERDOWN`. |
| Invariant | `RouteDecision::Moved` carries an `Option<SocketAddr>`, so "known owner, unknown address" is representable and `to_response` maps the `None` arm to `CLUSTERDOWN` rather than formatting a hole. |
| Outcome variant | `RouteDecision::Moved(None)` → `CLUSTERDOWN` |
| Forced by | `route_moved_with_no_addr_when_owner_node_unknown`, `to_response_moved_without_addr_emits_clusterdown` |
| Bug refs | `.scratch/hardening/issues/done/30-self-fence-flake-and-misleading-clusterdown.md` |

## FM-CLUSTER-024 — an unassigned slot is `CLUSTERDOWN`

| Field | Value |
|---|---|
| Trigger | A key whose slot nobody owns — mid-bootstrap, after a `FORGET` (FM-CLUSTER-002), or during a reset. |
| Observable | `CLUSTERDOWN` from the single-command path, the queued-batch path, and `WATCH` registration alike. |
| NOT observable | The receiving node serving the key on the grounds that nobody else claims it — that is how two nodes end up owning the same keyspace. |
| Invariant | `Unassigned` is a distinct `RouteDecision` variant rather than a `Moved` with no target, so it cannot be rescued by the `READONLY` branch (FM-CLUSTER-025). |
| Outcome variant | `RouteDecision::Unassigned` → `CLUSTERDOWN` |
| Forced by | `route_unassigned_when_no_owner_no_migration`, `to_response_unassigned_emits_clusterdown`, `batch_on_unassigned_slot_is_clusterdown`, `watch_on_an_unassigned_slot_is_clusterdown` |
| Bug refs | — |

## FM-CLUSTER-025 — `READONLY` rescues a `MOVED`, never an unassigned slot

| Field | Value |
|---|---|
| Trigger | A `READONLY` connection on a replica reading a slot owned elsewhere, and the same connection issuing a batch that contains a write. |
| Observable | A read-only batch on a foreign slot is served locally. A batch containing any write is not — it takes the ordinary `MOVED`. A `READONLY` connection addressing an *unassigned* slot still gets `CLUSTERDOWN`. |
| NOT observable | A write accepted on a node that does not own the slot because the connection happened to be in `READONLY` mode — that is an acked write on the wrong node, the same class as FM-CLUSTER-030. Nor `READONLY` manufacturing an owner for a slot that has none. |
| Invariant | `to_response(readonly_eligible)` applies the override to the `Moved` arm only, and eligibility is computed from the batch's own write flags rather than from the connection state alone (`routing.rs:83-103`). |
| Outcome variant | `RouteDecision::Moved` + `readonly_eligible` → `ServeLocal` |
| Forced by | `to_response_moved_readonly_eligible_serves_locally`, `to_response_unassigned_ignores_readonly_override`, `batch_readonly_eligible_serves_a_foreign_slot_locally`, `batch_readonly_ineligible_when_it_contains_a_write`, `batch_readonly_never_rescues_an_unassigned_slot` |
| Bug refs | — |

## FM-CLUSTER-026 — `ASKING` is the only key to an importing slot

| Field | Value |
|---|---|
| Trigger | A client addressing the *target* of an open migration, with and without a preceding `ASKING`. |
| Observable | With `ASKING`, the target accepts the command. Without it the client is sent back to the current owner with `MOVED` — or `CLUSTERDOWN` when the slot has no owner yet. The same holds for a queued `MULTI` batch, where `ASKING` is sticky for the whole block. |
| NOT observable | An importing node serving un-`ASKING`ed traffic. Until the migration commits, the source is still the authority; serving here would let two nodes answer for one slot with no ordering between them. |
| Invariant | The importing arms of `route_with_snapshot` are all gated on the `asking` flag (or on `is_restore`, FM-CLUSTER-027); nothing else opens them. |
| Outcome variant | `RouteDecision::{AcceptImporting, Moved, Unassigned}` |
| Forced by | `route_accept_importing_when_self_is_target_and_asking`, `route_moved_when_self_is_target_but_no_asking`, `route_unassigned_when_self_is_target_but_no_asking_or_restore`, `route_accept_importing_when_unassigned_self_target_with_asking`, `batch_on_import_target_with_asking_probes_importing`, `batch_on_import_target_without_asking_is_moved` |
| Bug refs | — |

## FM-CLUSTER-027 — `RESTORE` crosses into an importing slot without `ASKING`

| Field | Value |
|---|---|
| Trigger | The `RESTORE` half of a `MIGRATE`, landing on the migration target. |
| Observable | Accepted as importing even though the connection never sent `ASKING`. |
| NOT observable | The data-moving half of a migration being redirected back to the source — the migration would never make progress and `MIGRATE` would loop. |
| Invariant | `is_restore` is a distinct input to `route_with_snapshot`, so the exemption is one named flag on one command rather than a general relaxation of FM-CLUSTER-026. |
| Outcome variant | `RouteDecision::AcceptImporting` |
| Forced by | `route_accept_importing_for_restore_without_asking` |
| Bug refs | — |

## FM-CLUSTER-028 — the migrating source serves what it still holds and `ASK`s what has moved

| Field | Value |
|---|---|
| Trigger | A keyed command on a slot this node still owns while a migration off it is open — at **any arity**, plain or scripted, inside or outside `MULTI`. `MIGRATE` deletes each key as it hands it over, so "we own the slot" and "we hold the key" are different questions for the whole duration of the window. |
| Observable | Key presence decides, and it is consulted **before the command executes**: every key still here → served locally; every key already gone → `-ASK <slot> <importing node>`; some here and some gone → `-TRYAGAIN`. A probe the shard does not answer → `ERR shard unavailable`. The single-key path, the scripting family, and the EXEC/batch path give the same answer for the same key set. If the importing node's address is unknown, the source serves locally rather than emitting a redirect it cannot address. A node that is the migration *source* but not the current owner takes the ordinary `MOVED`. Node-scoped commands (`SCAN`, `MIGRATE`, `CLUSTER`, the connection-level families) are never probed — no slot owns them, so no migration can redirect them. |
| NOT observable | A write to an already-migrated key answered `+OK` on the source. That re-creates the key behind the migration, and `CLUSTER SETSLOT <slot> NODE` then hands the slot away and destroys the acknowledged write — 11 acked writes lost in a fault-free cluster in the run issue 40 was filed from. Nor a reply-shape stand-in for presence: a nil reply does not mean the key is gone (`HGET` on a live hash, `LPOS` on a live list, a `BLPOP` timeout are all nil) and a non-nil reply does not mean it is here (`SET` answers `+OK` on the key it just re-created), so "convert nil replies to `ASK` after execution" is both over- and under-inclusive and is unsound for writes by construction. Nor the mirror-image over-correction — a blanket refusal of a migrating slot: Redis serves what it still holds, and a blanket `TRYAGAIN` would stall the slot for the migration's whole duration. |
| Invariant | One decision, one site. `route_migrating_source` (`server/src/slot_migration/routing.rs`) turns "this node owns the slot, a migration off it is open, and the ASK address renders" into a probe instruction, reusing `route_with_snapshot`'s `LocalServeMigrating` arm and the batch path's `migration_target_addr`; both callers — the per-command `MigratingSourceProbe` stage and `validate_queued_batch` — map the shared `probe_key_presence` verdict through the single `KeyPresence::migrating_source_reply`. Arity is not a gate anywhere on that path. The probe is ordered after `ClusterSlotValidation` and before `ConnectionCommand` (`MUST_PRECEDE`), so the scripting family cannot outrun it the way FM-CLUSTER-030 describes; it reads slot state first, so a slot with no open migration costs one snapshot read and no keyspace lookup. |
| Outcome variant | `RouteDecision::LocalServeMigrating` → `KeyPresence` → `None` / `-ASK` / `-TRYAGAIN` / `ERR shard unavailable` |
| Forced by | `route_local_serve_migrating_when_self_is_source`, `route_moved_when_self_is_source_but_other_owns`, `migrating_source_probe_names_the_importing_node`, `migrating_source_probe_is_none_without_an_open_migration`, `migrating_source_probe_is_none_when_the_ask_address_is_unknown`, `batch_on_migrating_source_probes_with_the_ask_target`, `batch_on_migrating_source_with_unknown_target_serves_locally`, `migrating_source_asks_a_single_key_write_whose_key_moved`, `migrating_source_serves_a_single_key_command_still_held_here`, `migrating_source_tryagain_when_the_keys_straddle_the_slot`, `migrating_source_probes_the_scripting_family`, `migrating_source_refuses_when_the_shard_does_not_answer`, `migrating_source_probe_is_skipped_when_the_slot_is_not_migrating`, `migrating_source_probe_is_skipped_for_node_scoped_commands`, `test_single_key_write_on_migrating_source_asks_and_never_orphans` |
| Bug refs | `.scratch/hardening/issues/done/40-single-key-write-on-migrating-source-acks-then-orphans.md` (fixed); issue 31 §"Bug X" (the jepsen forensics it was split out of) |

## FM-CLUSTER-029 — `WATCH` is routed when it is registered, not when `EXEC` runs

| Field | Value |
|---|---|
| Trigger | `WATCH` naming a key on a foreign slot, on two slots, on a slot with an open migration, and on an unassigned slot. |
| Observable | A foreign slot is refused at `WATCH` time with `MOVED`; two slots with `CROSSSLOT`; an unassigned slot with `CLUSTERDOWN`; a slot with an open migration is accepted. |
| NOT observable | A `WATCH` silently accepted on a slot this node does not serve, only for the `EXEC` to fail much later — the client would have built a whole transaction on a watch that was never valid. Nor a `READONLY` rescue: watch routing hard-codes `readonly_eligible = false`, because a watch exists to guard a subsequent write. |
| Invariant | `route_watched_keys` and `watch_slot_is_locally_served` share the routing verdicts with the command path and pass `readonly_eligible = false` explicitly (`routing.rs:317-350`). |
| Outcome variant | `Option<Response>` |
| Forced by | `watch_on_a_foreign_slot_is_refused_with_moved`, `watch_on_an_open_migration_is_accepted`, `watch_slot_locally_served_accepts_an_open_migration`, `watch_slot_locally_served_rejects_a_slot_owned_elsewhere` |
| Bug refs | — |

## FM-CLUSTER-030 — A bare script is redirected, never executed on a non-owner

| Field | Value |
|---|---|
| Trigger | A bare `EVAL` / `EVAL_RO` / `EVALSHA` / `EVALSHA_RO` / `FCALL` / `FCALL_RO` (outside `MULTI`) naming a key owned by another node, sent to the non-owner. |
| Observable | `MOVED <slot> <owner>`, identical to the plain command on the same key — for every member of the family, and before the script body or function ever runs. A script that declares `numkeys 0` (and the keyless `SCRIPT LOAD`) still runs on any node. |
| NOT observable | Any script side effect on a non-owner. Before the fix the non-owner answered `Integer(1)` to a script whose body was `redis.call('SET', KEYS[1], …); return 1` — an acked write on a node that does not own the slot — while the control `GET` on the same key and connection was correctly `MOVED`. That is strictly wider than the finalization window of rework 02/03: not a race, but unconditional for every bare script on a non-owner. Nor the inverse over-correction: a keyless script being assigned a slot and redirected. |
| Invariant | `is_cluster_exempt` refuses to exempt `ConnectionLevelOp::Scripting` (`guards.rs:696-713`), and `PRE_DISPATCH_ORDER` runs `ClusterSlotValidation` *before* `ConnectionCommand`, the stage that dispatches the scripting family. The ordering is pinned structurally by the `MUST_PRECEDE` pair `(ClusterSlotValidation, ConnectionCommand)` (`dispatch.rs`), validated by `load_bearing_ordering_invariants` — so a reorder that compiles but re-opens the hole fails the unit test. The verdict itself has one source: `validate_cluster_slots` → `coordinator.route`, the same seam every keyed command uses. |
| Outcome variant | `RouteDecision::Moved` → `RouteOutcome::Reply(MOVED)` |
| Forced by | `test_bare_eval_on_non_owner_returns_moved`, `test_bare_script_family_on_non_owner_returns_moved`, `test_keyless_script_is_never_redirected`, `load_bearing_ordering_invariants` |
| Bug refs | `.scratch/replication-cluster-rework/issues/done/11-bare-eval-may-skip-cluster-slot-validation.md` (fixed); rework issues 02 and 03 (the narrower finalization-window form) |

Residual, deliberately out of scope here: a script's *undeclared* runtime writes (`redis.call` on a
key the invocation never named) are still unvalidated — Redis re-checks on every call via
`scriptVerifyClusterState`, and that is rework issue 03.

The sibling gap this section used to record — the migrating-source probe sitting *after*
`ConnectionCommand`, so a script against a half-migrated slot was served locally instead of
redirected — is closed. Issue 40 hoisted the stage (now `MigratingSourceProbe`) to the same
position for the same reason, pinned by the `MUST_PRECEDE` pair
`(MigratingSourceProbe, ConnectionCommand)`. The concern that blocked the hoist —
newly subjecting `MIGRATE` (server-wide, `KeySpec::Dynamic`) to the probe — is answered by the
stage sharing `is_cluster_exempt` with slot validation, so every node-scoped command keeps
skipping it. See FM-CLUSTER-028.

---

## FM-CLUSTER-031 — `IMPORTING` and `MIGRATING` converge on one idempotent begin

| Field | Value |
|---|---|
| Trigger | The Redis migration handshake: `SETSLOT <slot> IMPORTING` on the target and `SETSLOT <slot> MIGRATING` on the source, in either order, both replicated through the leader. |
| Observable | Both lower to the same `BeginSlotMigration{slot, source, target}`. The second one finds an identical migration and answers `Ok`. A *different* migration proposed for the same slot is `MigrationInProgress(slot)`. The slot then reports as migrating with the recorded source and target. |
| NOT observable | The second half of the handshake failing merely because the first half already ran — the operator's script would abort a migration that is in fact healthy. Nor a second, conflicting migration silently replacing the first. |
| Invariant | The idempotency check compares source *and* target and runs before the node-existence checks (`commands.rs:367-372`), so the converging pair is recognised as one intent rather than two. |
| Outcome variant | `ClusterResponse::Ok` / `ClusterError::MigrationInProgress` |
| Forced by | `test_slot_migration`, `test_begin_migration_idempotent`, `test_begin_migration_already_in_progress` |
| Bug refs | — |

## FM-CLUSTER-032 — a begin validates both endpoints, and the owner only when the slot is assigned

| Field | Value |
|---|---|
| Trigger | `SETSLOT … IMPORTING/MIGRATING` naming an unknown node, naming a source that does not own the slot, or naming a slot nobody owns yet. |
| Observable | An unknown source or target is `NodeNotFound`. A source that is not the recorded owner is `InvalidOperation("slot N is owned by X, not Y")`. A slot with no recorded owner is accepted. |
| NOT observable | A migration recorded against a node that is not a member — it could never complete, and it would block the slot until someone cancelled it. |
| Invariant | The owner check is conditional on the slot being present in `slot_assignment` (`commands.rs:381-390`), because a follower's slot map may legitimately be empty: bootstrap assigns slots locally, not through Raft. |
| Catalog | `INV-REF-2`, `INV-MIG-1` — the endpoint-existence and owner-is-source claims, held for the life of the record rather than only at begin time. |
| Outcome variant | `ClusterError::{NodeNotFound, InvalidOperation}` |
| Forced by | `test_begin_migration_source_not_found`, `test_begin_migration_target_not_found`, `test_begin_migration_wrong_owner`, `begin_migration_accepts_an_unassigned_slot` |
| Bug refs | — |

## FM-CLUSTER-033 — completing a migration consults the node table, never the slot map

| Field | Value |
|---|---|
| Trigger | `SETSLOT <slot> NODE <target>` closing an open migration, including onto a slot the map still shows as owned by the source, and one whose target is no longer a cluster member. |
| Observable | Ownership moves to the target and the migration record is removed. A target that is not in `nodes` is `NodeNotFound(target)` and nothing moves — not the slot, not the migration record. With no migration open the command is `InvalidOperation("no migration in progress for slot N")`; with mismatched source or target it is `InvalidOperation("migration parameters don't match")`. |
| NOT observable | A `SlotAlreadyAssigned` refusal at the moment of completion — that guard exists to stop *concurrent claims*, and a completion is the resolution of a claim that was already accepted at begin time. Nor an epoch bump: the ownership swap is authorized by the migration record, not by a new epoch. Nor a slot owner that is not a member (the "ghost owner" of INV-REF-1): the slot map would name a node no client can be redirected to and no operator can `FORGET`, and the coverage readers count such a slot as `ok` (FM-CLUSTER-073), so the cluster would report itself healthy while a shard of the keyspace was unreachable. |
| Invariant | Two guards, and deliberately only two: the parameter match against the recorded migration (only the exact pair that opened the migration may close it) and a membership check on the target, which is the node the arm is about to write into `slot_assignment` (`commands.rs`, `CompleteSlotMigration` arm). The slot map is still not consulted — the migration record authorizes the transfer. The node table is, because a migration record can outlive the membership it was validated against at begin time (FM-CLUSTER-032): a snapshot installed from a leader running an older binary, or any future arm that drops a node without pruning, presents exactly that shape. The source is not checked: it is never written into the slot map, and its release event on a node that has left is a no-op. |
| Catalog | `INV-REF-1`, `INV-MIG-1` — the ghost owner this row's `NOT observable` already names, plus the record/ownership agreement the swap resolves. |
| Outcome variant | `ClusterResponse::Ok` / `ClusterError::{InvalidOperation, NodeNotFound}` |
| Forced by | `test_complete_migration_no_active`, `test_complete_migration_params_mismatch`, `complete_migration_transfers_ownership_over_an_existing_owner`, `complete_migration_refuses_a_target_that_is_no_longer_a_member` |
| Bug refs | fixed: issue 01, `.scratch/cluster-correctness/issues/`; round-2 issue 62 (11/F5 ghost-owner consequence), `.scratch/testing-improvements-round2/issues/` |

## FM-CLUSTER-034 — a completed migration emits exactly one event, and a failed one emits none

| Field | Value |
|---|---|
| Trigger | `CompleteSlotMigration`, succeeding and failing; and any non-event command. |
| Observable | Success emits exactly one `SlotMigrationCompleted{slot, source, target}`, delivered on *every* node (unlike role events, which are filtered to self). Failure emits nothing. A begin emits nothing. |
| NOT observable | An event emitted on a rejected command — a consumer would wake blocked clients and redirect them on a migration that never happened. |
| Invariant | Events are pushed only on the `Ok` path of the mutating arm, so emit-on-failure is structurally impossible (`commands.rs:10-17`); the migration event is forwarded without the self-filter because every node's blocked clients need to learn the slot moved. |
| Outcome variant | `ClusterEvent::SlotMigrationCompleted` |
| Forced by | `test_migration_complete_event_fires`, `complete_migration_emits_event_on_success`, `complete_migration_emits_no_event_on_error` |
| Bug refs | — |

## FM-CLUSTER-035 — `SETSLOT … STABLE` cancels whether or not a migration is open

| Field | Value |
|---|---|
| Trigger | `SETSLOT <slot> STABLE`, on a slot with an open migration and on one without. |
| Observable | The slot stops reporting as migrating. Both cases answer `Ok`. |
| NOT observable | An error for cancelling a migration that is not open. `STABLE` is the operator's recovery verb — for a half-finished handshake, for a migration whose finalizer died before it could complete, for an aborted script — and a recovery verb that fails when the state is already clean cannot be used blindly. A migration whose endpoint was forgotten is no longer one of its cases: `FORGET` prunes those itself (FM-CLUSTER-002). |
| Invariant | The arm is an unconditional `migrations.remove(&slot)` returning `Ok` (`commands.rs:438-442`), so it is idempotent by construction rather than by a presence check. |
| Outcome variant | `ClusterResponse::Ok` |
| Forced by | `test_cancel_slot_migration`, `test_cancel_slot_migration_nonexistent` |
| Bug refs | — |

## FM-CLUSTER-036 — a force failover cancels the migrations of the node it removes

| Field | Value |
|---|---|
| Trigger | An automatic (force) failover of a primary that is the source or target of an open migration, and a graceful failover alongside an unrelated migration. |
| Observable | Migrations naming the removed node are gone. Migrations between two other nodes survive both forms of failover untouched. |
| NOT observable | A migration left pointing at a node that is no longer a member — it can never complete, and its slot stays in a migrating state that nothing will clear except manual intervention. |
| Invariant | The force branch calls `prune_migrations_naming(old_primary_id)` in the same transition that removes the node (`commands.rs`, `Failover` arm), so no window exists in which the node is gone and its migrations are not. `RemoveNode` calls the same helper (FM-CLUSTER-002), which is what keeps the two removal paths from drifting apart again. |
| Catalog | `INV-REF-2` — a migration naming a departed node, checked everywhere rather than only on the paths that prune one. |
| Outcome variant | `ClusterResponse::Epoch` |
| Forced by | `test_failover_force_cancels_migrations_of_removed_node`, `test_failover_graceful_keeps_unrelated_migrations`, `force_failover_reports_moved_slots_and_prunes_only_related_migrations` |
| Bug refs | — |

## FM-CLUSTER-037 — the commit-to-apply window is a known, bounded, accepted mode

| Field | Value |
|---|---|
| Trigger | A `CompleteSlotMigration` that commits through Raft microseconds after a batch has already been validated against the cluster snapshot read at `EXEC` entry. |
| Observable | That already-validated batch executes against the *pre-completion* ownership: it serves locally a slot whose ownership has just moved. The next batch, reading a fresh snapshot, is redirected. |
| NOT observable | An unbounded window. The exposure is bounded by Raft apply latency, not by the client's think time — the snapshot is taken at `EXEC` entry, not at `MULTI` entry, so a client that sits in an open transaction for minutes does not widen it. |
| Invariant | Routing is a pure function of an explicitly supplied `ClusterSnapshot` (`route_with_snapshot`, `route_queued_batch`), which is what makes the window a single named read rather than a scatter of live lookups that could each disagree. This is the same window every Redis Cluster node has, and the two-point ownership swap (no intermediate migration state machine) is the deliberate design — see `website/src/content/docs/architecture/clustering.md`. **This row is a documented accepted mode, not a bug.** |
| Outcome variant | n/a |
| Forced by | `batch_on_owned_stable_slot_serves_locally`, `batch_on_foreign_slot_is_moved_to_the_owner` |
| Bug refs | `.scratch/replication-cluster-rework/issues/open/02`, `.scratch/replication-cluster-rework/issues/open/03` (the finalization-window pair — narrowing this window is their subject) |

## FM-CLUSTER-038 — clients blocked on a migrated slot are woken and redirected

| Field | Value |
|---|---|
| Trigger | A `SlotMigrationCompleteEvent` reaching the per-node dispatcher while clients are blocked (`BLPOP` and friends) on keys in that slot. |
| Observable | Blocked clients are woken with `MOVED` to the migration target — or, when this node cannot name the target, with `CLUSTERDOWN Hash slot <n> not served` (the rendering routing already uses for "owner known, address unknown", FM-CLUSTER-023). Both losses of contact — a shard index that does not exist, a shard channel that is closed — are logged at `error`. |
| NOT observable | A client left blocked forever on a slot this node no longer owns — its key can never be written here again, so nothing will ever wake it. In particular an event is never `continue`d past because its target is unresolvable. |
| Invariant | `plan_migration_notice` maps the event to a `MigrationNotice{shard, slot, target_addr: Option<SocketAddr>}` and `run_slot_migration_event_dispatcher` delivers *every* notice (`cluster-runtime/src/migration_events.rs`); the shard renders `MOVED` for `Some` and `CLUSTERDOWN` for `None` (`core/src/shard/blocking.rs:118`). |
| Outcome variant | `MigrationNotice` / `ClusterMsg::SlotMigrated{target_addr: Option<SocketAddr>}` |
| Forced by | `migration_event_with_a_known_target_notifies_the_owning_shard`, `migration_event_with_an_unknown_target_still_wakes_blocked_clients`, `migration_event_routes_to_slot_modulo_num_shards`, `migration_event_reports_a_closed_shard_channel`, `slot_migrated_without_a_known_target_replies_clusterdown` |
| Bug refs | fixed: [35-migration-complete-event-dropped-when-target-unknown.md](../.scratch/hardening/issues/done/35-migration-complete-event-dropped-when-target-unknown.md) |

The dispatcher moved out of `frogdb-server` (`slot_migration/events.rs`, deleted) into the
mutation-gated `frogdb-cluster-runtime`, split into a pure planning function and a delivery step that
reports its own failures. `BlockedMigrationMoved` counts only the `MOVED` wake-ups, so the counter
keeps meaning exactly what its name says; the `CLUSTERDOWN` path is visible in the logs instead.

---

## FM-CLUSTER-039 — a failover validates completely before it mutates anything

| Field | Value |
|---|---|
| Trigger | `Failover` naming an unknown successor, naming the same node twice, or gracefully failing over a primary that is no longer a member. |
| Observable | `NodeNotFound(new)` / `InvalidOperation("failover source and target are the same node")` / `NodeNotFound(old)`. Membership, roles, slot ownership, epochs, and parenting are all exactly as they were. |
| NOT observable | A partially applied failover — slots transferred but no successor promoted, or a promotion with no slots. That is the ownerless-slot window the atomic `Failover` entry exists to close; it is what the old multi-entry `RemoveNode`/`SetRole`/`AssignSlots`/`IncrementEpoch` sequence exposed on a leader crash. |
| Invariant | The arm is strictly two-phase: every check runs first, and from `commands.rs:247` onward the mutation is infallible. A *force* failover of an already-removed node is deliberately allowed, because that is precisely the situation an automatic failover fires in. |
| Outcome variant | `ClusterError::{NodeNotFound, InvalidOperation}` |
| Forced by | `test_failover_validation_failure_mutates_nothing`, `test_failover_same_node_rejected`, `test_failover_graceful_requires_old_node` |
| Bug refs | — |

## FM-CLUSTER-040 — a force failover removes the old primary and transfers everything in one entry

| Field | Value |
|---|---|
| Trigger | An automatic failover (`force: true`) of a failed primary, and the primary-absorbs-primary case. |
| Observable | The old primary is gone from `CLUSTER NODES`; every slot it owned is owned by the successor; the successor is a primary with no parent; the old primary's other replicas are re-parented to the successor; the epoch is bumped once and the successor's `config_epoch` is stamped with it. |
| NOT observable | Any intermediate state in which the failed primary's slots are ownerless, or in which the successor owns slots at a stale epoch. Slot ownership and the epoch that authorizes it propagate together, as in Redis. |
| Invariant | One replicated log entry performs remove, transfer, promote, re-parent, and bump; there is no sequence of entries a crash can land between (`commands.rs:226-333`). |
| Catalog | `INV-REF-1`, `INV-REF-3`, `INV-REF-4`, `INV-EPOCH-1`, `INV-EPOCH-2` — the coherence of the composite entry: no orphaned slot, no dangling or self-parented node, no epoch above the counter or shared with another primary. |
| Outcome variant | `ClusterResponse::Epoch` |
| Forced by | `test_failover_force_removes_old_and_transfers_everything`, `test_failover_absorb_between_primaries` |
| Bug refs | — |

## FM-CLUSTER-041 — a graceful failover demotes the old primary rather than removing it

| Field | Value |
|---|---|
| Trigger | An operator-initiated `CLUSTER FAILOVER` with the old primary still healthy and reachable. |
| Observable | The old primary stays a member, recorded as a replica of the successor; its slots move to the successor; sibling replicas are re-parented; the epoch is bumped once. |
| NOT observable | A healthy node being evicted from the cluster by a planned failover — it is the new replica, and removing it would force a full re-`MEET` and a full resync. |
| Invariant | The `force` flag selects between remove and demote inside the one atomic entry, so both shapes share the transfer/promote/bump path and cannot drift. |
| Catalog | `INV-REF-1`, `INV-REF-3`, `INV-REF-4`, `INV-EPOCH-1`, `INV-EPOCH-2` — the demote-instead-of-remove shape shares the transfer/promote/bump path, so it owes the same postconditions as FM-CLUSTER-040. |
| Outcome variant | `ClusterResponse::Epoch` |
| Forced by | `test_failover_graceful_demotes_old_primary`, `failover_re_parents_only_the_old_primarys_replicas` |
| Bug refs | — |

## FM-CLUSTER-042 — a replayed failover is safe

| Field | Value |
|---|---|
| Trigger | Log replay, or a retry of the same `Failover` entry after a leader change. |
| Observable | The second application finds the old primary already gone, transfers zero slots, and no-ops the promotion; the end state is coherent (old removed, successor primary, slots owned by the successor). The command and the resulting state both survive a JSON round trip. |
| NOT observable | A replay corrupting the topology — replay is not an exceptional path, it is how a restarting node rebuilds state. |
| Invariant | `force` allows an absent old primary, and promotion is idempotent because it sets rather than toggles. The one non-idempotent effect is the epoch, which is bumped again; that is safe under FM-CLUSTER-010's dominance invariant (bumping is always allowed) and is noted here so a future reader does not mistake it for a bug. |
| Catalog | `INV-REF-1`, `INV-REF-4`, `INV-EPOCH-1` — "the end state is coherent" stated as checks, so a replay that corrupted the topology fails at the apply seam rather than at whatever later command tripped over it. |
| Outcome variant | `ClusterResponse::Epoch` |
| Forced by | `test_failover_force_replay_is_safe`, `test_failover_command_serde_roundtrip`, `test_state_snapshot_roundtrip_after_failover` |
| Bug refs | — |

## FM-CLUSTER-043 — role events fire only for this node, and only on a real transition

| Field | Value |
|---|---|
| Trigger | `SetRole` naming this node, naming another node, being rejected, and re-asserting a role the node already holds. |
| Observable | A demotion of self delivers exactly one `Demoted{self, new_primary}`. A role change naming another node delivers nothing here. A rejected `SetRole` delivers nothing. Re-asserting `Primary` on a node that is already primary delivers nothing; re-asserting `Replica` *does* deliver a demotion, because re-parenting must re-point the replication stream even when the role word is unchanged. |
| NOT observable | A promotion event on a replay that changed nothing — the data path would tear down and rebuild its replication link for no reason, and on a restarting node that is a spurious full resync. |
| Invariant | `apply` filters role events against `self_node_id` before forwarding them, and the `Primary` arm emits only when `!was_primary` (`commands.rs:204-216`, `state.rs:688-728`). |
| Outcome variant | `RoleChangeEvent::{Demoted, Promoted}` |
| Forced by | `test_demotion_detection_fires_for_self`, `test_demotion_detection_ignores_other_nodes`, `test_demotion_detection_not_fired_for_rejected_set_role`, `test_promotion_detection_silent_when_already_primary`, `set_role_replica_emits_node_demoted`, `set_role_primary_emits_no_event`, `set_role_self_demotion_emits_no_event_on_error` |
| Bug refs | — |

## FM-CLUSTER-044 — a failover's role events arrive in apply order on one channel

| Field | Value |
|---|---|
| Trigger | A failover round that demotes and promotes within a few entries, on both the old primary and the successor. |
| Observable | A graceful failover emits demotion then promotion, in that order. A force failover emits only the promotion (the old primary was removed, not demoted). A successor that was already a primary is not "promoted" again. A failed failover emits nothing. Each node sees only its own transitions. |
| NOT observable | The two events reordered or split across channels. A promotion applied before the demotion it supersedes leaves the data path a replica of a node that no longer leads; the transitions are not commutative. |
| Invariant | One unbounded channel with one consumer, fed in `apply` order (`state.rs:333-347`), so ordering is a property of the queue rather than of consumer discipline. |
| Outcome variant | `RoleChangeEvent` |
| Forced by | `graceful_failover_emits_node_demoted_for_old_primary`, `force_failover_emits_promotion_only`, `test_role_changes_preserve_apply_order`, `test_promotion_detection_fires_for_failover_successor`, `test_force_failover_still_promotes_the_successor`, `test_demotion_detection_fires_for_graceful_failover_of_self`, `test_demotion_detection_not_fired_for_failed_failover`, `test_demotion_detection_not_fired_for_force_failover`, `test_promotion_detection_fires_for_self_set_role` |
| Bug refs | — |

## FM-CLUSTER-045 — installing a snapshot synthesizes the role change it skipped

| Field | Value |
|---|---|
| Trigger | A node that fell far enough behind to receive a snapshot rather than log entries, where the snapshot's topology gives it a different role than it had. |
| Observable | It emits the promotion or demotion the skipped entries would have produced. A snapshot that does not change this node's role emits nothing. |
| NOT observable | A restored replica booting with `role:master` on its data path. A role folded into a snapshot produces no entry to replay, so without this the transition would be lost silently — the node would serve reads as a primary and never attach to its actual primary. |
| Invariant | `install_snapshot` samples `self_role()` before and after the restore and emits on a difference (`state.rs:787-797`); the persist happens before the in-memory restore, so a snapshot visible in memory is always already on disk. |
| Outcome variant | `RoleChangeEvent` |
| Forced by | `test_install_snapshot_emits_promotion_when_self_role_flipped`, `test_install_snapshot_emits_demotion_when_self_role_flipped`, `test_install_snapshot_silent_when_self_role_unchanged` |
| Bug refs | — |

## FM-CLUSTER-046 — the reconciler re-drives a disagreeing data path, and detaches when nobody is listening

| Field | Value |
|---|---|
| Trigger | The replicated role and the data path's role disagree — after a restore, after a missed event — and the disagreement persists across ticks. Separately: the consumer is dropped at shutdown. |
| Observable | Every tick that finds a disagreement re-emits the correct role and reports `ReDriven(role)`; once the data path agrees it reports `Agreed` and goes silent. With the consumer gone it reports `Detached`. A node that is not a member reports `Agreed` and emits nothing. The reconciler does not exist at all until role-change detection is enabled. |
| NOT observable | A single-shot correction that gives up while the views still disagree, leaving a replica advertising `role:master` indefinitely. Nor the reconciler keeping the consumer — and through it the storage engine and its RocksDB lock — alive past shutdown, which breaks restart-in-process. |
| Invariant | The reconciler holds a *weak* sender, so `Detached` is a real state rather than a send that silently succeeds into a leaked consumer (`state.rs:517-527`). |
| Outcome variant | `RoleReconcile::{Agreed, ReDriven, Detached}` |
| Forced by | `test_reconcile_self_role_demotes_a_restored_replica`, `test_reconcile_self_role_promotes_a_restored_primary`, `test_reconcile_self_role_silent_when_views_agree`, `test_reconcile_self_role_noop_when_self_absent`, `test_reconciler_re_emits_while_the_views_disagree`, `test_reconciler_detaches_once_the_consumer_is_gone`, `test_self_role_reconciler_absent_until_detection_is_enabled` |
| Bug refs | — |

## FM-CLUSTER-047 — a committed proposal may still carry a state-machine rejection

| Field | Value |
|---|---|
| Trigger | A `ClusterCommand` the leader commits successfully but the state machine refuses — `SlotAlreadyAssigned`, `NodeNotFound`, and every other validated arm. |
| Observable | `propose` returns `Ok(Proposed::Committed(ClusterResponse::Error(e)))`. The caller renders `ERR <message>`. A command the state machine accepts returns `Committed(Ok)` and the forwarder is never consulted. |
| NOT observable | A state-machine rejection surfacing as a *transport* error. The entry really was replicated and applied; reporting it as a Raft failure would invite a retry that must fail identically, and would hide a durable decision behind a transient-looking error. |
| Invariant | `ClusterResponse` is openraft's response type, so a rejection rides back through the success channel by construction (`writer.rs`). |
| Outcome variant | `Proposed::Committed(ClusterResponse::Error)` |
| Forced by | `leader_commit_ok_is_committed`, `leader_commit_state_machine_error_is_committed_error` |
| Bug refs | — |

## FM-CLUSTER-048 — a follower forwards to the leader, and falls back to a redirect it can name

| Field | Value |
|---|---|
| Trigger | An administrative `CLUSTER` command received by a follower. |
| Observable | The follower forwards the write over the cluster bus and answers `Proposed::Forwarded` — an `+OK` to the client, with no redirect. If the forward fails, the client gets `REDIRECT <leader-id> <leader-client-addr>`. |
| NOT observable | The follower performing the leader's *side effects* on a forwarded write — the leader already added the Raft voter, and doing it twice on an existing voter would demote it to a learner and cost the cluster a quorum member. |
| Invariant | `Forwarded` is a distinct variant from `Committed`, so the caller can tell "the leader did it" from "I did it" and skip the local side effects; `resolve_redirect` is a pure function of `(leader_id, cluster_state)`. |
| Outcome variant | `Proposed::Forwarded` / `ProposeError::Redirect` |
| Forced by | `follower_forward_success_is_forwarded`, `follower_forward_failure_is_redirect`, `resolve_redirect_known_leader_yields_redirect`, `a_forwarded_write_reports_the_leaders_verdict`, `the_bus_forwarder_reports_whether_the_leader_took_the_write` |
| Bug refs | — |

## FM-CLUSTER-049 — no addressable leader degrades to `CLUSTERDOWN`

| Field | Value |
|---|---|
| Trigger | A forward failing with no leader known, or with a leader id that is not in this node's cluster state (so it has no client address). |
| Observable | `CLUSTERDOWN No leader available: <id-or-None>`. The wire strings for both this and the `REDIRECT` form are pinned exactly. |
| NOT observable | A `REDIRECT` naming an address the node does not have — the same class of malformed redirect as FM-CLUSTER-023. |
| Invariant | `LeaderRedirect` carries `Option<NodeId>` and `Option<SocketAddr>` independently, so "leader known, address unknown" is representable and renders as `CLUSTERDOWN` rather than a hole in a `REDIRECT`. |
| Outcome variant | `CLUSTERDOWN` |
| Forced by | `follower_forward_failure_unknown_leader_is_clusterdown_redirect`, `resolve_redirect_leader_not_in_state_yields_clusterdown`, `resolve_redirect_no_leader_yields_clusterdown`, `redirect_to_response_renders_wire_strings` |
| Bug refs | `.scratch/hardening/issues/done/30-self-fence-flake-and-misleading-clusterdown.md` |

## FM-CLUSTER-050 — any other Raft failure is reported as itself

| Field | Value |
|---|---|
| Trigger | A Raft error that is not `ForwardToLeader` — a fatal storage error, a panicked state machine, an election timeout. |
| Observable | `ProposeError::Raft(message)`, rendered as `ERR Raft error: …` with the underlying message preserved. |
| NOT observable | A fatal error being laundered into a redirect. A client that follows a redirect after a storage failure retries against a node that will fail identically, and the operator loses the only signal that the failure was local and permanent. |
| Invariant | The `ForwardToLeader` match is on the specific `ClientWriteError` variant, so everything else falls through to the `Raft` arm rather than being classified by string matching. |
| Outcome variant | `ProposeError::Raft` |
| Forced by | `non_forward_raft_error_is_raft_error` |
| Bug refs | — |

`ClusterWriter::propose` has exactly these two error variants and no quorum-loss variant. Quorum loss
reaches a client one of three ways: the write-path self-fence rejects it before it ever proposes
(FM-CLUSTER-059), a redirect cannot be resolved (FM-CLUSTER-049), or it surfaces here as
`Raft(String)`. That is a deliberate consequence of quorum being a *detector* concept rather than a
propose-time one; it is recorded so a reader does not go looking for the missing variant.

## FM-CLUSTER-051 — the bus envelope cannot confuse a Raft RPC with a bus RPC

| Field | Value |
|---|---|
| Trigger | Any inter-node message: `AppendEntries`, `Vote`, `InstallSnapshot`, `ForwardedWrite`, `PubSubBroadcast`, `PubSubForward`, `HealthProbe`. |
| Observable | Every variant round-trips through the postcard framing unchanged, and each subset's `From` shim places it on the matching envelope arm. The address registry resolves, updates and removes node addresses. |
| NOT observable | A bus RPC being dispatched into the Raft handler or vice versa — the handler for one would treat the other's payload as a consensus message. |
| Invariant | `ClusterRpcRequest::{Raft, Bus}` is a typed split and `BusRpc` names only the serviceable variants, so `handle_bus_rpc` is exhaustive by construction and cannot receive a Raft RPC. |
| Outcome variant | `ClusterRpcRequest` / `ClusterRpcResponse` |
| Forced by | `test_from_shims_wrap_correct_arm`, `test_all_rpc_variants_roundtrip`, `test_rpc_request_serialization`, `test_network_factory_node_registration`, `the_bus_serves_probes_and_raft_rpcs_on_one_connection`, `the_pool_keeps_one_slot_per_peer_until_the_peer_is_removed`, `the_factory_reports_every_registered_address`, `the_voter_retry_schedule_backs_off_and_then_stops`, `adding_a_voter_runs_for_a_stranger_and_skips_an_existing_member` |
| Bug refs | — |

---

## FM-CLUSTER-052 — failure latching is symmetric and hysteretic

| Field | Value |
|---|---|
| Trigger | A peer failing probes up to and past the threshold, then answering them again; and a peer that answers roughly every other probe. |
| Observable | `fail_threshold - 1` consecutive failures do not latch; the Nth does. Once latched, `fail_threshold - 1` consecutive successes do not clear it; the Nth does. Any single failure resets an in-progress recovery run, so a peer alternating success and failure never clears. A single success resets the failure run of a peer that has not yet latched. |
| NOT observable | A flapping peer churning `MarkNodeFailed`/`MarkNodeRecovered` pairs — each one is a replicated entry and each `MarkNodeFailed` bumps the config epoch (FM-CLUSTER-013), so an unhysteretic detector turns a marginal link into unbounded epoch churn across the whole cluster. |
| Invariant | `record_failure` zeroes `success_count` and `record_success` zeroes `failure_count`, and the un-latch branch runs only while latched (`failure_detector.rs:162-188`) — the hysteresis is symmetric because both counters are consecutive-run counters, not totals. |
| Outcome variant | `NodeHealth::is_marked_fail` |
| Forced by | `test_health_table_threshold_latching_is_symmetric`, `test_health_table_latch_survives_flapping_recovery`, `test_health_table_success_resets_failure_run`, `test_node_health_default`, `local_probe_results_land_in_the_health_table` |
| Bug refs | — |

## FM-CLUSTER-053 — the verdict is level-triggered and a latch never decays

| Field | Value |
|---|---|
| Trigger | Polling a peer's verdict: never probed, healthy, failing below threshold, latched, latched an hour ago, recovered, and then gone quiet. |
| Observable | Never probed is `Unknown`; a success is `Healthy`; a sub-threshold failure is still `Healthy`; a latch is `Failed` on every subsequent poll including an hour later; after recovery it is `Healthy`, and it decays to `Unknown` once it goes stale. |
| NOT observable | A latched peer silently decaying to `Unknown` through staleness, which would make the leader stop reconciling it and leave a dead node unflagged in the replicated topology forever. That is why the `is_marked_fail` check precedes the staleness check. |
| Invariant | The verdict is recomputed from state on every poll rather than latched at a transition; that is what lets a *new* leader converge on a peer whose threshold was crossed while it was still a follower — the transition-triggered version drops that write and never retries it. |
| Outcome variant | `LocalVerdict::{Failed, Healthy, Unknown}` |
| Forced by | `test_health_table_verdict_is_level_triggered` |
| Bug refs | — |

## FM-CLUSTER-054 — staleness has a closed, threshold-derived boundary

| Field | Value |
|---|---|
| Trigger | A peer last seen exactly at, just inside, and past the staleness window. |
| Observable | The window is `check_interval * (fail_threshold + 2)`. Just inside it the peer counts as reachable; exactly at the boundary and beyond it does not (the comparison is strict `<`). The defaults are `check_interval_ms = 1000`, `connect_timeout_ms = 500`, `fail_threshold = 5`. |
| NOT observable | A staleness window unrelated to the probe cadence. Deriving it from `check_interval` and `fail_threshold` means retuning either one keeps the two consistent; a hard-coded window would silently become either trigger-happy or useless. |
| Invariant | `stale_threshold()` is the single derivation, and every time-dependent method takes `now` as a parameter, so the whole table is a pure state machine testable without a clock. |
| Outcome variant | `Duration` |
| Forced by | `test_health_table_staleness_boundary`, `test_failure_detector_config_default` |
| Bug refs | — |

## FM-CLUSTER-055 — quorum counts conservatively

| Field | Value |
|---|---|
| Trigger | Evaluating quorum at startup, with peers latched failed, with peers never probed, and across cluster sizes 1..5. |
| Observable | Quorum is `total/2 + 1`. This node always counts itself. A peer that is latched failed does not count. A peer that has never been probed does not count. So a freshly started node reports *no* quorum until its probes land. |
| NOT observable | An unprobed peer counted as reachable. That would let a node that has just booted — or one whose detector task is wedged — believe it has quorum and accept writes it cannot replicate. Optimism here is exactly the split-brain failure the self-fence exists to prevent. |
| Invariant | The "no entry" arm of `reachable_count` returns false explicitly rather than falling through to a default (`failure_detector.rs:245-249`). |
| Outcome variant | `bool` |
| Forced by | `test_health_table_quorum_arithmetic`, `test_health_table_reachable_never_seen_and_failed`, `quorum_follows_the_locally_probed_peers` |
| Bug refs | — |

## FM-CLUSTER-056 — the promotion score is priority first, replication lag second

| Field | Value |
|---|---|
| Trigger | Scoring candidate replicas for an automatic failover. |
| Observable | The score is `priority * 100_000 + (max_offset - replica_offset) * 1_000`, lower is better. At equal offsets the lower priority wins; at equal priorities the higher offset (less lag) wins. A candidate whose offset could not be determined is scored as if it had the worst offset, not excluded. |
| NOT observable | A more-lagged replica outscoring a less-lagged one at the same priority — that is data loss chosen deliberately. The lag term uses `saturating_sub`, so a stale or absent offset can only make a candidate look worse. |
| Invariant | `compute_replica_score` is a pure function of three numbers, so the weighting is auditable in one place rather than distributed across the selection loop. |
| Outcome variant | `u64` score |
| Forced by | `test_compute_replica_score_formula`, `test_compute_replica_score_lower_priority_is_better`, `test_compute_replica_score_higher_offset_is_better`, `test_compute_replica_score_no_offset_found`, `auto_failover_promotes_the_replica_with_the_freshest_offset` |
| Bug refs | — |

## FM-CLUSTER-057 — priority 0 is never promoted

| Field | Value |
|---|---|
| Trigger | A failover where some or all candidates carry `replica_priority = 0`. |
| Observable | A priority-0 candidate scores `u64::MAX` and is filtered out of selection entirely. If every candidate is priority 0, selection returns `None` and the failover is abandoned with a warning rather than promoting someone. |
| NOT observable | A node the operator marked never-promote being promoted anyway — that setting is how a cross-region or backup-only replica is kept out of the primary role, and violating it is a latency or durability regression the operator explicitly ruled out. |
| Invariant | Belt and braces: an explicit `filter(priority != 0)` *and* the `u64::MAX` score, so removing either one alone still refuses. |
| Outcome variant | `Option<&NodeInfo>` = `None` |
| Forced by | `test_compute_replica_score_priority_zero_excluded`, `test_select_failover_target_all_never_promote`, `test_missing_replica_priority_defaults_to_the_neutral_value` |
| Bug refs | — |

## FM-CLUSTER-058 — failover target selection is deterministic and live-tunable

| Field | Value |
|---|---|
| Trigger | Two candidates with identical scores; and `CONFIG SET cluster-replica-priority` on a running node. |
| Observable | The tie is broken by the lower node id, so every node computing the selection independently reaches the same answer. Changing this node's priority at runtime changes the selection it computes immediately — no restart, no re-published `NodeInfo`. Setting it to 0 removes this node from its own candidate set. |
| NOT observable | Two nodes disagreeing about the winner. Selection is not itself replicated — the decision is made locally and then proposed — so a nondeterministic tiebreak is a route to two competing `Failover` proposals. |
| Invariant | `min_by(score).then_with(id)` makes the ordering total; the priority read goes through `ClusterRuntimeFlags`, which is atomics behind an `Arc` read at decision time. |
| Outcome variant | `Option<&NodeInfo>` |
| Forced by | `test_compute_replica_score_equal_scores_tiebreak_by_node_id`, `test_replica_priority_store_changes_failover_target`, `effective_priority_is_live_for_this_node_and_published_for_peers` |
| Bug refs | — |

Documented asymmetry: a live priority change is authoritative only for *this* node's own score.
Peers keep scoring this node from the Raft-replicated `NodeInfo.replica_priority` until it
re-registers, so two nodes running selection concurrently mid-change can pick different targets.

## FM-CLUSTER-059 — the self-fence knob is live, and its reported reason matches the verdict it gates on

| Field | Value |
|---|---|
| Trigger | `CONFIG SET cluster-self-fence-on-quorum-loss` and `cluster-auto-failover` on a running node with quorum already lost. |
| Observable | Disabling the fence makes writes acceptable again on the same gate instance; re-enabling it fences again. With healthy quorum the flag makes no difference. The write-fence reason reported to observability is `"cluster quorum lost"` exactly when the gate fences, and absent when it does not. |
| NOT observable | A knob that only takes effect at startup. Gating *installation* of the checker rather than its verdict is what made this startup-only before, and an operator riding out a partition cannot restart the node to change it. Nor `/status` claiming a fence reason the write path is not enforcing. |
| Invariant | `SelfFenceGate::has_quorum` is `!flag || inner.has_quorum()` and `write_fence_reason` is derived from that same call, so the report cannot disagree with the gate (`flags.rs:126-139`). The wire wording is the *inner* checker's: `quorum_lost_error()` forwards to `inner.quorum_lost_error()`, whose `QuorumChecker` default is `CLUSTER_DOWN_QUORUM_LOST`, so a cluster fence keeps answering `CLUSTERDOWN` while the replication self-fence behind the same write gate answers its own `SELFFENCE` code (FM-REPLICATION-041). The gate never spells the string itself — a wrapper that hardcoded one would re-label whatever it wraps. |
| Outcome variant | `Option<&'static str>` = `Some("cluster quorum lost")`; `CLUSTERDOWN The cluster is down (quorum lost, writes rejected)` |
| Forced by | `cluster_flag_sets_reach_the_live_flags`, `auto_failover_flag_is_live`, `self_fence_gate_follows_live_flag`, `self_fence_gate_passes_through_healthy_quorum`, `self_fence_gate_reports_the_reason_it_gates_on`, `self_fence_gate_delegates_the_quorum_lost_wording` |
| Bug refs | `.scratch/hardening/issues/done/30-self-fence-flake-and-misleading-clusterdown.md` |

## FM-CLUSTER-060 — runtime flags derive from config with no field drift

| Field | Value |
|---|---|
| Trigger | Building the runtime flags from a cluster config section, and constructing them with no config at all. |
| Observable | Each of the three fields lands on its own flag — auto-failover, self-fence, replica priority — with no transposition. The defaults are auto-failover **off**, self-fence **on**, priority 100, matching `ClusterConfigSection::default()`. |
| NOT observable | Two same-typed flags swapped. `auto_failover` and `self_fence_on_quorum_loss` are both `bool` with *different* defaults, so a transposition silently inverts both policies: failover fires unbidden and writes are never fenced. Nor the runtime defaults drifting from the config defaults, which would make the same deployment behave differently depending on whether a config file was supplied. |
| Invariant | `Default` is implemented by delegating to `ClusterConfigSection::default()` and then `from_config`, so the two cannot diverge by construction. |
| Outcome variant | `Arc<ClusterRuntimeFlags>` |
| Forced by | `runtime_flags_from_config_carry_each_field`, `runtime_flags_default_matches_the_config_default` |
| Bug refs | — |

---

## FM-CLUSTER-061 — `CLUSTER`'s discovery subcommands stay reachable from the client port

| Field | Value |
|---|---|
| Trigger | A non-admin connection on a server with `admin_enabled` issuing `CLUSTER INFO`/`NODES`/`MYID`/`SLOTS`/`SHARDS`/`KEYSLOT`/`COUNTKEYSINSLOT`/`GETKEYSINSLOT`/`HELP`. |
| Observable | All nine are answered. Every other subcommand — `SETSLOT`, `FORGET`, `MEET`, `RESET`, `FAILOVER`, `ADDSLOTS`, … — is `NOADMIN Admin commands are disabled on this port. Use the admin port.` Subcommand matching is case-insensitive, as is the command name. |
| NOT observable | Topology discovery being admin-gated. Every cluster-aware client bootstraps with `CLUSTER SLOTS` or `CLUSTER SHARDS`; gating those makes the client port unusable for its intended purpose. |
| Invariant | `AdminSurface::SplitBySubcommand{public}` is consulted per subcommand rather than per command, so the split is a data table rather than a branch in the guard. |
| Outcome variant | `AdminSurface::SplitBySubcommand` / `NOADMIN` |
| Forced by | `admin_surface_split_allows_public_subcommands`, `admin_surface_matches_names_case_insensitively` |
| Bug refs | — |

## FM-CLUSTER-062 — a bare container command fails closed

| Field | Value |
|---|---|
| Trigger | `CLUSTER` with no subcommand at all, from a non-admin connection. Likewise `CONFIG`, `ACL`, `CLIENT`, `MEMORY`. |
| Observable | `NOADMIN`. |
| NOT observable | A bare container command being treated as public because there is no subcommand to look up. `requires_admin(None)` returning false would open every split command's arity-error path to unauthenticated probing. |
| Invariant | The `None` arm of `requires_admin` returns `true` explicitly (`command_spec.rs`), and `extract_subcommand` returns `None` exactly for the bare form. |
| Outcome variant | `NOADMIN` |
| Forced by | `admin_surface_split_gates_missing_subcommand` |
| Bug refs | — |

## FM-CLUSTER-063 — an unknown subcommand fails closed

| Field | Value |
|---|---|
| Trigger | `CLUSTER NOT-A-SUBCOMMAND` from a non-admin connection, and any subcommand added to the dispatcher but not to the public list. |
| Observable | `NOADMIN`, before the dispatcher gets a chance to report an unknown subcommand. |
| NOT observable | A newly added subcommand being public by default. This is the property that makes the split table safe to extend: a `CLUSTER` subcommand added next year is admin-only until someone deliberately declares it public. |
| Invariant | The public list is an allow-list matched with `any(..)`, so anything absent is gated — there is no deny-list to keep in sync. |
| Outcome variant | `NOADMIN` |
| Forced by | `admin_surface_split_gates_unknown_subcommand` |
| Bug refs | — |

## FM-CLUSTER-064 — the split table is the whole admin contract, and the registry agrees with it

| Field | Value |
|---|---|
| Trigger | Resolving the admin surface for a split command, a whole-command admin command, and an ungated command. |
| Observable | The per-command classification of all five split commands is pinned, so a change to the admin-port contract shows up in a diff. A command carrying `ADMIN` with no split entry is gated in every form. A command with neither is `Open`. Live registry entries never carry `ADMIN` *and* a split entry. |
| NOT observable | A command gated by two mechanisms that disagree — the split table wins over the `ADMIN` flag, so a command carrying both would have its flag silently ignored, and a reader auditing flags would reach the wrong conclusion. The registry coherence test makes that combination impossible rather than merely discouraged. |
| Invariant | `admin_surface(command, flags)` consults the split table first and falls back to the flag; `split_admin_surfaces_agree_with_command_flags` walks the *live* registry rather than a copy of the table. |
| Outcome variant | `AdminSurface::{Open, Whole, SplitBySubcommand}` |
| Forced by | `admin_surface_split_table_classification`, `admin_surface_open_when_no_admin_flag_and_no_split`, `admin_surface_whole_command_gates_every_form`, `split_admin_surfaces_agree_with_command_flags` |
| Bug refs | — |

---

## FM-CLUSTER-065 — strict mode on the cluster bus never downgrades

| Field | Value |
|---|---|
| Trigger | An inbound bus connection while dual-accept is off, whatever its first byte — a TLS `ClientHello` (`0x16`), a plaintext frame, or an immediate EOF. |
| Observable | The connection is always handed to the TLS acceptor. A plaintext peer fails the handshake. |
| NOT observable | A plaintext bus connection accepted while strict mode is on. The bus carries Raft RPCs, forwarded writes, and pub/sub payloads with no authentication of its own — anything that can dial it in plaintext can propose cluster state. |
| Invariant | `choose_transport` returns early on `!migration` before it inspects the sniffed byte at all (`bus.rs:129-130`), so no byte value can reach the plaintext arm. |
| Outcome variant | `BusTransport::Tls` |
| Forced by | `strict_mode_always_requires_tls` |
| Bug refs | — |

## FM-CLUSTER-066 — dual-accept sniffs the ClientHello, and the decision is re-read per connection

| Field | Value |
|---|---|
| Trigger | A rolling TLS migration with dual-accept enabled, receiving both TLS and plaintext peers; and the flag being flipped between connections. |
| Observable | A first byte of `0x16` goes to TLS; anything else, including an immediate EOF, is treated as plaintext and fails downstream on its own terms. Flipping the flag changes the next connection's decision with no restart and no listener rebind. |
| NOT observable | The transport decision being cached for the lifetime of the listener — a migration would then need a restart at exactly the moment the operator is trying to avoid one. |
| Invariant | `dual_accept()` and `handshake_timeout()` are read inside `handle_connection`, once per connection; the listener itself is pre-bound and never re-bound, so there is no port TOCTOU. |
| Outcome variant | `BusTransport::{Tls, Plaintext}` |
| Forced by | `dual_accept_sniffs_the_client_hello`, `flipping_the_runtime_flag_changes_the_next_connection`, `test_cluster_bus_bind_fails_on_invalid_addr`, `a_peek_of_zero_bytes_is_not_a_first_byte` |
| Bug refs | — |

## FM-CLUSTER-067 — no cross-node pub/sub RPC can hang the publisher

| Field | Value |
|---|---|
| Trigger | A peer that accepts the pub/sub RPC and never answers. |
| Observable | The call resolves as a timeout after `PUBSUB_RPC_TIMEOUT` (2s) and the publisher's `PUBLISH` returns. |
| NOT observable | A `PUBLISH` blocked indefinitely on one unresponsive peer. The fan-out awaits every peer, so an unbounded wait on one of them stalls the publishing *client*, not merely the delivery. |
| Invariant | `send_pubsub_rpc` is the single owner of the timeout and wraps the future rather than the transport, so every call site inherits it and none can opt out. |
| Outcome variant | `PubSubRpcError::Timeout` |
| Forced by | `test_rpc_timeout_maps_to_timeout_variant` |
| Bug refs | — |

## FM-CLUSTER-068 — a protocol-shape mismatch is never reported as a delivery count

| Field | Value |
|---|---|
| Trigger | A peer answering a broadcast request with a forward result, or with an explicit protocol error; and the ordinary success and transport-failure paths. |
| Observable | A well-shaped answer yields its subscriber count. A shape mismatch is `UnexpectedResponse` and logs the full response at `warn`. A transport failure is `Rpc`. The extractors match only their own result variant and reject an error response. |
| NOT observable | A peer bug rendered as `Ok(0)`. "The peer answered with the wrong message type" and "the peer has no subscribers" are different operational problems, and collapsing the first into the second makes it invisible. |
| Invariant | Extraction is a function pointer returning `Option`, and the `None` arm is the only path to `UnexpectedResponse` — so the distinction is structural rather than a log level. |
| Outcome variant | `PubSubRpcError::{UnexpectedResponse, Rpc}` |
| Forced by | `test_rpc_expected_shape_yields_count`, `test_rpc_shape_mismatch_is_distinguishable_not_zero`, `test_rpc_transport_error_maps_to_rpc_variant`, `test_forward_extractor_matches_only_forward_results` |
| Bug refs | — |

Accepted non-guarantee: all three error variants are folded to `0` by both call sites, so a client
cannot distinguish a total partition from a delivered-to-nobody broadcast. `PUBLISH` returns one
integer and Redis has the same property; the distinction lives in the logs.

## FM-CLUSTER-069 — pub/sub fan-out is a no-op when there is nobody to fan out to

| Field | Value |
|---|---|
| Trigger | `PUBLISH` on a standalone node, and on a clustered node whose address registry holds one entry or none. |
| Observable | The remote subscriber count is 0 and no RPC is attempted. `SPUBLISH` on a standalone node returns `None`, meaning "deliver locally". |
| NOT observable | A standalone node attempting cluster RPCs, or a single-node cluster broadcasting to itself and double-counting its own subscribers. |
| Invariant | Both methods `let-else` on the `Cluster` variant at the top, and the cluster path returns early on `all_nodes.len() <= 1` before building any task. |
| Outcome variant | `usize` = 0 / `Option<usize>` = `None` |
| Forced by | `test_local_forwarder_broadcast_is_noop`, `test_local_forwarder_forward_returns_none`, `cluster_broadcast_is_a_noop_below_two_nodes`, `cluster_broadcast_sums_every_other_reachable_peer_once`, `cluster_broadcast_folds_an_unreachable_peer_into_zero` |
| Bug refs | — |

## FM-CLUSTER-070 — `SPUBLISH` falls back to local delivery when it cannot name a remote owner

| Field | Value |
|---|---|
| Trigger | `SPUBLISH` to a shard channel whose slot this node owns, whose slot a reachable peer owns, whose slot nobody owns, and whose owner has no address in the registry. |
| Observable | Four distinct outcomes. Only a reachable remote owner forwards (`Forwarded(count)`); the other three deliver locally and say which one they are — `Local(Local)`, `Local(Unowned{slot})`, `Local(OwnerUnaddressable{owner,slot})`. Both fallbacks warn-log naming the slot. The client-visible reply is identical across all three local outcomes. |
| NOT observable | The message being dropped — the local path still runs in every non-`Remote` case, so a subscriber attached to this node still receives it. Nor a fallback delivery reported identically to a correct one: a slot-assignment gap that silently delivers shard messages on the wrong node used to be indistinguishable from correct ownership. |
| Invariant | `route_shard_channel_in` is a pure function of the slot map plus the address registry and returns a four-variant `ShardRoute`; `forward_spublish` matches it exhaustively, so a new unresolvable case cannot be added as another silent `None`. |
| Outcome variant | `ShardRoute::{Local, Remote, Unowned, OwnerUnaddressable}` / `SpublishOutcome::{Forwarded, Local}` |
| Forced by | `cluster_forward_returns_none_when_this_node_owns_the_slot`, `cluster_forward_distinguishes_an_unowned_slot_from_local_ownership`, `cluster_forward_distinguishes_an_unaddressable_owner_from_local_ownership`, `cluster_route_names_a_reachable_remote_owner`, `test_local_forwarder_forward_returns_none`, `cluster_forward_reports_the_owners_subscriber_count` |
| Bug refs | fixed: [36-spublish-conflates-unowned-slot-with-local-ownership.md](../.scratch/hardening/issues/done/36-spublish-conflates-unowned-slot-with-local-ownership.md) |

The local fallback itself stays deliberate: delivering locally is strictly better than dropping,
because FrogDB's `SSUBSCRIBE` does not slot-route subscribers, so no other node would deliver the
message either. Redis answers `MOVED`/`CLUSTERDOWN` for a shard channel it does not serve; matching
that is a client-visible change gated on the broader "should shard pub/sub slot-route at all"
question, and is not this row's subject. What this row pins is that the fallback is *named* rather
than disguised as correct ownership.

## FM-CLUSTER-071 — `CLUSTER NODES` renders one exact line per node

| Field | Value |
|---|---|
| Trigger | `CLUSTER NODES` on a clustered node and on a standalone one. |
| Observable | Node ids are 40 hex characters (a `u64` zero-padded to Redis' width). Flags render in the order `myself`, `master`/`slave`, `fail`, `fail?`, `handshake`, `noaddr`. Health is `fail` > `loading` (pfail) > `online`. `ping-sent`/`pong-recv` are `0 0` and the link state is `connected`, always. Migration markers are appended as `[slot->-<target>]` / `[slot-<-<source>]`. A slotless node yields a trailing double space. Standalone renders one `myself,master` line owning `0-16383`. |
| NOT observable | A node id that is not 40 hex characters — every Redis client parses this field positionally and by width. Nor fabricated gossip timings: FrogDB has no gossip layer, so `0 0` is the honest value rather than a synthesized one. |
| Invariant | One renderer serves both the clustered and standalone paths (`standalone_snapshot` builds a snapshot rather than a second formatter), and the golden test pins the whole line including the whitespace artifact. |
| Outcome variant | `CLUSTER NODES` body |
| Forced by | `test_render_cluster_nodes_golden`, `test_render_cluster_nodes_standalone`, `test_format_node_id_is_40_hex`, `test_node_flags_label`, `test_node_health`, `test_cluster_snapshot_partitions_nodes_by_role` |
| Bug refs | — |

## FM-CLUSTER-072 — `SLOTS` drops slotless primaries, `SHARDS` keeps them

| Field | Value |
|---|---|
| Trigger | A cluster containing a primary that owns no slots, and a `CLUSTER SHARDS` on a node with replicas. |
| Observable | `CLUSTER SLOTS` omits the slotless primary entirely; `CLUSTER SHARDS` includes it with an empty slot list. Shards are ordered by primary id whatever the insertion order, replicas nested in id order. A node's slots are compacted into contiguous ranges. `replication-offset` is reported only for the local node; peers omit the field. |
| NOT observable | A `CLUSTER SLOTS` entry with an empty slot range — a client would parse it as a mapping and route nothing to it. Nor a fabricated replication offset for a peer this node cannot measure. |
| Invariant | `shard_views` deliberately keeps zero-slot primaries and documents that the SLOTS mapper must filter them itself, so the two commands share one grouping with one explicit difference. |
| Outcome variant | `CLUSTER SLOTS` / `CLUSTER SHARDS` replies |
| Forced by | `test_shard_views_grouping_and_order`, `test_shard_views_includes_zero_slot_primary`, `test_map_slots_response_skips_zero_slot_primary`, `test_map_shards_response_reports_offset_for_local_node_only`, `test_cluster_snapshot_get_node_slots` |
| Bug refs | — |

## FM-CLUSTER-073 — slot health counts every assigned slot exactly once

| Field | Value |
|---|---|
| Trigger | `CLUSTER INFO` with owners flagged `fail`, flagged `pfail`, flagged both, recovered, and with slots whose owner is not a member. |
| Observable | `cluster_slots_ok + cluster_slots_pfail + cluster_slots_fail == cluster_slots_assigned`, always. `fail` beats `pfail` for a doubly flagged owner. Flagging one owner leaves other owners' slots untouched. A flagged owner with no slots moves no counts. A slot whose owner is unknown counts as `ok` rather than being dropped. Recovery restores the full `ok` count. |
| NOT observable | Counters that do not sum to the assigned total. An operator sizing an incident from these numbers needs them to partition the slot space; a dropped slot understates the outage. |
| Invariant | `count_slot_health` walks the assignment map once and assigns each slot to exactly one bucket, with the unknown-owner case landing in `ok` explicitly rather than falling out of the loop. |
| Outcome variant | `SlotHealthCounts` |
| Forced by | `test_count_slot_health_totals_always_equal_slots_assigned`, `test_count_slot_health_all_ok_when_no_node_flagged`, `test_count_slot_health_fail_flagged_owner_counts_as_fail_not_ok`, `test_count_slot_health_pfail_flagged_owner_counts_distinct_from_fail`, `test_count_slot_health_fail_takes_precedence_over_pfail`, `test_count_slot_health_recovery_restores_full_ok`, `test_count_slot_health_only_counts_flagged_owners_slots_others_unaffected`, `test_count_slot_health_fail_flagged_slotless_primary_leaves_slots_ok`, `test_count_slot_health_unknown_owner_counted_ok_not_dropped`, `test_cluster_snapshot_slot_coverage_readers_agree` |
| Bug refs | fixed: [37-cluster-stats-messages-hardcoded-zero.md](../.scratch/hardening/issues/done/37-cluster-stats-messages-hardcoded-zero.md) |

## FM-CLUSTER-074 — `CLUSTER INFO` is CRLF-framed `key:value` lines

| Field | Value |
|---|---|
| Trigger | `CLUSTER INFO` on any node. |
| Observable | Every line is `key:value` terminated by CRLF, the framing every Redis client's INFO parser expects. |
| NOT observable | LF-only framing, which some clients accept and others silently truncate on. |
| Invariant | One `render` builds the whole body, so a field added later inherits the framing rather than choosing its own. |
| Outcome variant | `CLUSTER INFO` body |
| Forced by | `test_cluster_info_render_is_crlf_framed_key_value_lines` |
| Bug refs | fixed: [37-cluster-stats-messages-hardcoded-zero.md](../.scratch/hardening/issues/done/37-cluster-stats-messages-hardcoded-zero.md) |

## FM-CLUSTER-075 — slot ranges parse and render Redis-style

| Field | Value |
|---|---|
| Trigger | Parsing and rendering slot ranges from `CLUSTER NODES` and from operator input. |
| Observable | `"0-100"` round-trips; a single slot renders bare (`"42"`) and parses back. `"invalid"`, an inverted range (`"100-50"`), and an out-of-range slot (`"20000"`) are all `InvalidSlot`. Containment is inclusive at both ends. |
| NOT observable | An inverted or out-of-range range being accepted from a string — every downstream length computation assumes `start <= end < 16384`, and an inverted range underflows it. |
| Invariant | `FromStr` validates the ordering and the upper bound before constructing, so the parse boundary is where the invariant is established. |
| Outcome variant | `ClusterError::InvalidSlot` |
| Forced by | `test_slot_range_contains`, `test_slot_range_display`, `test_slot_range_parse`, `test_slot_range_len_counts_both_endpoints`, `test_slot_range_parse_accepts_a_degenerate_range` |
| Bug refs | — |

## FM-CLUSTER-077 — `CLUSTER INFO`'s bus counters are measured or absent, never fabricated

| Field | Value |
|---|---|
| Trigger | `CLUSTER INFO` on a node whose bus has carried traffic, on a node whose bus has carried none, and on a node with no handle on a bus at all. |
| Observable | `cluster_stats_messages_sent`/`_received` are live counts of frames that crossed the cluster bus — requests written and responses read by the client side, requests read and responses written by the bus loop, both directions accumulating into one node-wide pair. A node that cannot read them omits both lines. `total_cluster_links_buffer_limit_exceeded` is `0` because there is no per-link output buffer limit to exceed. |
| NOT observable | A `ping`/`pong` per-type line: FrogDB runs no gossip protocol, and Redis omits a per-type line whose counter is zero. Nor a counted frame that never crossed the wire — a request that failed to serialize, or a connection that never opened, is not traffic. |
| Invariant | Counting happens at the four wire seams and nowhere else: `try_send_on_framed` after the write and after the response frame (`network.rs`), `parse_rpc_message` after a frame arrives, `send_rpc_response` after the write. The rendering treats an unreadable counter as `None` and omits the line, the way `cluster_raft_term` already does. |
| Outcome variant | `ClusterBusStatsSnapshot` / `CLUSTER INFO` body |
| Forced by | `a_fresh_counter_pair_reads_zero`, `the_two_directions_are_counted_independently`, `counters_accumulate_across_threads`, `cluster_stats_messages_sent_grows_with_bus_traffic`, `cluster_stats_messages_received_grows_on_the_receiving_node`, `a_connection_that_never_opens_counts_nothing`, `cluster_info_omits_gossip_counters_that_have_no_source`, `cluster_info_reports_the_live_bus_counters`, `cluster_info_omits_the_bus_totals_when_they_cannot_be_read`, `a_peer_handle_shares_the_factorys_counters` |
| Bug refs | fixed: [37-cluster-stats-messages-hardcoded-zero.md](../.scratch/hardening/issues/done/37-cluster-stats-messages-hardcoded-zero.md) |

## FM-CLUSTER-078 — the published snapshot is never older than an applied mutation

| Field | Value |
|---|---|
| Trigger | Any topology mutation the state machine applies — a `ClusterCommand` through `apply_command` (including one it rejects), or a whole-state replacement through `restore_from_snapshot` — followed by a `ClusterState::snapshot()`. |
| Observable | The next `snapshot()` observes the mutation. Two `snapshot()` calls with no intervening mutation return the *same* allocation (`Arc::ptr_eq`), and read-only accessors do not invalidate it. A snapshot a reader already holds is immutable: a later mutation cannot reach back into a decision made against it. |
| NOT observable | A reader routing against a topology the state machine has already moved past — a stale slot table is a `MOVED` to the wrong node or a local serve of a slot this node no longer owns. Nor a per-read copy of the 16384-entry slot table: `snapshot()` is on the keyed-command path (`SlotMigrationCoordinator::route`), so a copy there is per-command cost. |
| Invariant | `StateCell` holds the authoritative `ClusterStateInner` and the published `Arc<ClusterSnapshot>` under one lock, and `PublishOnDrop` — the only handle that hands out a `&mut ClusterStateInner` — rebuilds the published value in its `Drop`, inside the same critical section. An early `return`, a `?`, or an unwinding panic all run that `Drop`. The openraft bookkeeping fields (`last_applied_log`, `last_membership`) are deliberately not snapshot-visible and have their own no-republish setters (`state.rs`). |
| Outcome variant | `Arc<ClusterSnapshot>` |
| Forced by | `test_snapshot_observes_topology_applied_since_the_last_read`, `test_repeated_snapshots_without_mutation_share_one_allocation`, `test_rejected_command_leaves_snapshot_agreeing_with_state`, `test_snapshot_install_republishes_the_reader_view`, `state_readers_report_the_applied_table` |
| Bug refs | fixed: [10-cluster-snapshot-clone-cost.md](../.scratch/replication-cluster-rework/issues/done/10-cluster-snapshot-clone-cost.md) |

---

## FM-CLUSTER-079 — a slot-scoped pause parks its slot's writes and treats an unpinnable command as touching every slot

| Field | Value |
|---|---|
| Trigger | A pause is armed on one hash slot (`ClientRegistry::pause_slot`, as the migration finalization barrier does) while clients keep issuing commands across the keyspace. |
| Observable | A write whose keys all hash to the barriered slot parks until the slot is released or the pause lapses; a write to any other slot is served immediately; a read of the barriered slot is served immediately under a `WRITE`-mode pause. A command that names no keys (`FLUSHALL`) or names keys in more than one slot cannot be pinned, so it parks against the *strongest* pause armed on any slot. |
| NOT observable | A slot pause quiescing the node — that is `CLIENT PAUSE`'s job and the whole point of scoping is that the other 16383 slots keep serving. Nor `FLUSHALL` slipping past a barrier because it names no keys: it erases the barriered slot along with everything else, so "no slot" must fail closed rather than open. Nor a cross-slot key set answering `CROSSSLOT` from the pause gate: the gate is not the slot validator and runs before it, so a straddling key set collapses to the same unpinnable case rather than an error. |
| Invariant | `PauseState` carries two independent dimensions — one node-global `PauseEntry` and a `HashMap<u16, PauseEntry>` — so per-slot barriers compose without interfering. `command_pause_slot` resolves a command's slot through the registry's own key extractor and `SlotValidator::same_slot`, answering `None` for both the keyless and the straddling case; `ClientRegistry::slot_pause(None)` folds the strongest live per-slot mode (`client_registry/mod.rs`, `connection/pause_gate.rs`). `should_pause_command` resolves the slot only when a slot-scoped pause actually exists, so the common unbarriered path costs one lock. |
| Outcome variant | `PauseOverview { node, slot_scoped }` / `Option<PauseMode>` |
| Forced by | `slot_pause_covers_only_its_slot`, `unpinnable_command_sees_the_strongest_slot_pause`, `single_key_command_pins_to_its_key_slot`, `hash_tagged_keys_in_one_slot_pin_together`, `unpinnable_commands_answer_none`, `slot_scoped_pause_parks_only_its_slots_writes` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

## FM-CLUSTER-080 — the catch-up `MIGRATE` is exempt from the barrier on the slot it is draining

| Field | Value |
|---|---|
| Trigger | A slot-scoped pause is armed on the slot a migration is finalizing, and the source node then runs the `MIGRATE` that ships the slot's remaining keys to the target. |
| Observable | `MIGRATE` executes and answers normally (`+OK`, or `NOKEY` when nothing matched) while ordinary writes to that same slot are still parked. |
| NOT observable | `MIGRATE` parking. It is `WRITE`-flagged and runs over an ordinary client connection, so without an exemption the barrier would park the very work whose completion releases it — a self-deadlock that no timeout short of the pause's own deadline can break, and that leaves the slot half-copied. |
| Invariant | `exempt_from_slot_pause` names `MIGRATE` explicitly, and `should_pause_command` consults it before resolving a slot, so the exemption cannot be defeated by which keys the `MIGRATE` happens to name (`connection/pause_gate.rs`). The exemption is scoped to slot pauses alone: a node-global `CLIENT PAUSE` still parks `MIGRATE` exactly as Redis does. |
| Outcome variant | n/a |
| Forced by | `only_migrate_and_cluster_are_slot_pause_exempt`, `catch_up_migrate_runs_while_its_slot_is_barriered` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

## FM-CLUSTER-081 — a slot barrier never parks the cluster control plane, including its own cancel

| Field | Value |
|---|---|
| Trigger | A slot-scoped pause is armed — including in `ALL` mode, which parks reads too — and an operator issues a `CLUSTER` subcommand, notably `CLUSTER SETSLOT <slot> STABLE` against the barriered slot. |
| Observable | Every `CLUSTER` subcommand is dispatched immediately and answers on its own merits. `CLUSTER SETSLOT <slot> STABLE` cancels the migration (FM-CLUSTER-035) and, with it, the operator's route out of a barriered slot. |
| NOT observable | `CLUSTER` parking behind a slot pause. A barrier that can swallow its own cancel command is unrecoverable without restarting the node, which for the source of a half-finished migration is the worst available option. |
| Invariant | `exempt_from_slot_pause` names `CLUSTER` as a whole rather than enumerating subcommands, so a new subcommand cannot be added into the trap by omission (`connection/pause_gate.rs`). As with `MIGRATE`, the exemption applies to slot-scoped pauses only. |
| Outcome variant | n/a |
| Forced by | `only_migrate_and_cluster_are_slot_pause_exempt`, `cluster_control_plane_is_never_parked_by_a_slot_barrier` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

## FM-CLUSTER-082 — the operator's pause and a migration's barrier compose, and neither can release the other

| Field | Value |
|---|---|
| Trigger | A node-global `CLIENT PAUSE` and one or more slot-scoped barriers are in force at the same time, and one of them is released or lapses. |
| Observable | While both are armed the stronger of the two modes applies to any command they both cover. `CLIENT UNPAUSE` clears the node-global pause and leaves every slot barrier armed; releasing a slot barrier leaves the node-global pause in force; two slot barriers expire on their own deadlines. Node-global re-arming keeps its Redis merge rules — `ALL` never downgrades to `WRITE`, a shorter re-arm never shortens the deadline — and per-slot re-arming merges the same way within its own slot. Active expiry stays suppressed while *any* pause is armed. |
| NOT observable | `CLIENT UNPAUSE` disarming a migration barrier. An operator command that silently cancels an in-flight handover's safety property would reintroduce exactly the acknowledged-then-orphaned write the barrier exists to prevent, and the operator has no way to know they did it. Nor the reverse: a barrier release ending an operator's deliberate quiesce early. |
| Invariant | The node dimension and the per-slot map are separate fields of `PauseState`; `unpause` touches only the former and `unpause_slot` only one entry of the latter. `PauseMode::strongest` folds the two dimensions at the gate rather than merging them in storage, so neither can overwrite the other. `PauseEntry::arm` implements the max-mode/max-deadline merge once, for both dimensions (`client_registry/mod.rs`). |
| Outcome variant | `PauseOverview` |
| Forced by | `node_and_slot_pauses_release_independently`, `slot_pauses_expire_independently`, `node_pause_never_downgrades_or_shortens`, `strongest_pause_mode_prefers_all`, `node_pause_and_slot_barrier_release_independently` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

## FM-CLUSTER-083 — a write transaction parks at `EXEC`, not at queue time, and re-validates after the barrier

| Field | Value |
|---|---|
| Trigger | A client queues writes in a `MULTI` and issues `EXEC` while a pause — node-global or slot-scoped — is armed. |
| Observable | Queuing is never parked: each queued command answers `QUEUED` immediately. `EXEC` parks and answers only after the pause releases, at which point the batch's slot routing is re-validated against the topology that exists *then* (`txn/src/exec.rs`), so a batch whose slot moved during the barrier is redirected rather than committed on the former owner. |
| NOT observable | A write batch committing while a barrier is up. That is the acknowledged-then-orphaned write of FM-CLUSTER-037 with a whole transaction behind it. Nor `MULTI`/queuing itself parking: queuing performs no keyspace work, and blocking it would hold the pause's cost against clients that may still `DISCARD`. |
| Invariant | `TxnHost::wait_if_paused` is handed the queued batch, so the host resolves which slot the batch is pinned to before deciding (FM-CLUSTER-096); the decision itself stays on the host, where the registry and the pause state live. The two halves are ordered: the batch's slots are validated *before* the wait, and re-validated *only* if it actually parked (`txn/src/exec.rs`), so an unparked `EXEC` pays nothing and a parked one cannot resume past the topology change that parked it. |
| Outcome variant | `Response::Array` / `-MOVED` |
| Forced by | `write_exec_parks_on_a_slot_barrier_and_commits_after_release`, `an_exec_parked_by_the_barrier_wakes_up_redirected`, `the_pause_barrier_is_handed_the_whole_batch` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

Phase 1 shipped this deliberately coarse — the seam was handed no queue, so a write `EXEC` parked on
*any* armed pause, over-parking batches that could not reach the barriered slot at all. Phase 2b
widened the seam and narrowed the park; the residual over-park is now exactly the unpinnable batch,
which is fail-closed rather than coarse (FM-CLUSTER-096).

## FM-CLUSTER-084 — ownership moves only under a prepared, drained, still-armed handoff

| Field | Value |
|---|---|
| Trigger | `CLUSTER SETSLOT <slot> NODE <target>` against an open migration. The finalizer proposes `PrepareSlotHandoff`, waits for the source's drain acknowledgement, and proposes `CompleteSlotMigration`. |
| Observable | `Complete` moves the slot's owner and clears the migration only when the slot's record carries a handoff that is drained and whose barrier window has not elapsed. Otherwise it is refused with a retryable `HandoffNotReady` naming which of the three conditions failed, and nothing changes: the owner, the migration, and the prepared record are exactly as they were. |
| NOT observable | Ownership moving on a `Complete` that arrived after the barrier window lapsed. By then the source has resumed serving the slot, so the writes it accepted in the interval would be stranded on a node that no longer owns them — the acknowledged-then-orphaned write of FM-CLUSTER-037, which this whole mechanism exists to close. Nor a partially applied `Complete`: the check precedes every mutation. |
| Invariant | `SlotHandoff::admits_complete_at` is the single predicate — `drained && !barrier_expired && !lease_expired` — and `apply_command`'s `CompleteSlotMigration` arm consults it after the parameter match and before touching `slot_assignment` (`cluster/src/{types,commands}.rs`). The finalizer's own wait budget (`HANDOFF_DRAIN_WAIT_MS`) is deliberately shorter than the barrier window so both a successful `Complete` and an abort still land inside it. |
| Catalog | `INV-MIG-1`, `INV-HANDOFF-2` — ownership and the migration record move together, and `drained` is only ever set on an attempt a prepare minted. |
| Outcome variant | `ClusterResponse::Ok` + `[SlotMigrationCompleted, SlotHandoffReleased]` / `ClusterError::HandoffNotReady` |
| Forced by | `complete_is_refused_without_a_prepared_handoff`, `complete_is_refused_while_the_handoff_is_undrained`, `prepare_then_drain_then_complete_moves_ownership`, `complete_is_refused_once_the_barrier_window_elapsed`, `handoff_model_smoke`, `handoff_model_full_cross_slot`, `handoff_model_full_deep` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

## FM-CLUSTER-085 — the prepared record is leased, so a dead finalizer cannot wedge a slot

| Field | Value |
|---|---|
| Trigger | A finalizer proposes `PrepareSlotHandoff` and then dies, is partitioned, or simply stalls past `HANDOFF_LEASE_MS` without proposing either `CompleteSlotMigration` or `AbortSlotHandoff`. |
| Observable | The prepared record stays visible in the replicated migration (`snapshot().migrations`) until its lease expires. A `Complete` proposed after expiry is refused. A *later* `PrepareSlotHandoff` is refused while the lease is live and accepted once it is not, minting a fresh attempt. A `ConfirmSlotHandoffDrained` that arrives after expiry marks the record drained but still cannot let ownership move. |
| NOT observable | A slot permanently unfinalizable because the node that prepared it never came back. Nor an expired prepare silently becoming a live one: the lease is checked at `Complete`, not only at `Prepare`, so marking a lapsed record drained buys nothing. |
| Invariant | Two bounds, at two levels, both derived from `prepared_at_ms`: `barrier_ms` bounds the *source's local pause* and the window in which `Complete` is admissible; the longer `lease_ms` bounds the *replicated record*, and is what a later attempt tests with `SlotMigration::live_handoff_at`. Neither is a clock read at apply (FM-CLUSTER-089). The operator's escape hatch is unchanged — `SETSLOT … STABLE` cancels the migration and the prepared record with it (FM-CLUSTER-087). |
| Outcome variant | `ClusterError::HandoffNotReady` |
| Forced by | `complete_is_refused_once_the_lease_expired`, `a_second_prepare_waits_for_the_lease_but_not_forever`, `a_late_confirm_cannot_resurrect_an_expired_handoff`, `handoff_model_full_deep` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

## FM-CLUSTER-086 — every handoff message names the attempt it belongs to

| Field | Value |
|---|---|
| Trigger | A slot's handoff is attempted more than once — the first attempt aborted or lapsed — and a message from the earlier attempt (a drain acknowledgement, an abort) arrives after the later one is prepared. Or a `PrepareSlotHandoff` names a source/target pair that does not match the open migration. |
| Observable | `ConfirmSlotHandoffDrained` and `AbortSlotHandoff` take effect only when their `seq` matches the record's current attempt; otherwise they are refused (`HandoffNotReady`) or, for an abort of an attempt that is already gone, succeed with no event. `PrepareSlotHandoff` against a slot with no migration, or with mismatched endpoints, is refused without creating a record. |
| NOT observable | A stale drain acknowledgement vouching for a fresh attempt. The stale ack was earned against a shard state that predates the current barrier, so honouring it would complete a handoff whose drain never happened — the same exposure as no barrier at all. |
| Invariant | `ClusterStateInner::handoff_seq` is a replicated counter incremented on every accepted `PrepareSlotHandoff`; the minted value is carried in `SlotHandoffPrepared` and echoed by both follow-up commands, which filter on `h.seq == seq` before mutating (`cluster/src/{state,commands}.rs`). Because the counter is replicated it advances identically on every node; `ResetCluster` is the only thing that rewinds it, and it clears every migration in the same entry — a snapshot restore does not rewind it either (FM-CLUSTER-100). |
| Catalog | `INV-HANDOFF-1`, `INV-HANDOFF-2` — the counter dominates every live attempt, and a `drained` record carrying the unminted seq `0` is a confirm that matched no attempt at all. |
| Outcome variant | `ClusterError::HandoffNotReady` / `ClusterError::InvalidOperation` |
| Forced by | `a_stale_drain_ack_cannot_vouch_for_the_next_attempt`, `prepare_requires_a_migration_and_matching_parameters`, `handoff_model_smoke`, `handoff_model_full_cross_slot`, `handoff_model_full_deep` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

## FM-CLUSTER-087 — a prepared handoff never disappears without a release event

| Field | Value |
|---|---|
| Trigger | Any command that removes or replaces a migration record carrying a prepared handoff: `AbortSlotHandoff`, `CancelSlotMigration` (`SETSLOT … STABLE`), a forced `Failover` that prunes the migrations of the node it demotes, `ResetCluster`, and `CompleteSlotMigration` itself. |
| Observable | Each one emits `SlotHandoffReleased` for every prepared handoff it removed, so the source lifts its barrier. `Abort` keeps the migration record and drops only the handoff; `Cancel` drops both; `Failover` and `Reset` drop whole sets and emit one release each. Removing a migration that had no prepared handoff emits nothing. |
| NOT observable | A slot left fenced because the record that would have released it was deleted by an unrelated command. The source's pause has its own deadline, so the worst case is bounded — but a slot silently frozen for the whole barrier window after an operator cancelled the migration is the kind of surprise that gets a barrier ripped out. |
| Invariant | One helper, `release_events(migration)`, renders a removed record into its release events, and every removing arm routes through it (`cluster/src/commands.rs`). The events are emitted from `apply`, so they reach every node's dispatcher and the source recognises its own by node id (FM-CLUSTER-090). |
| Outcome variant | `ClusterEvent::SlotHandoffReleased` |
| Forced by | `abort_releases_the_barrier_and_keeps_the_migration`, `cancel_releases_a_prepared_handoff`, `cancel_without_a_handoff_emits_nothing`, `force_failover_releases_the_handoffs_it_prunes`, `reset_releases_prepared_handoffs`, `a_source_that_cannot_drain_aborts_the_finalization` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

## FM-CLUSTER-088 — two slots finalize concurrently without serialising on the shard they share

| Field | Value |
|---|---|
| Trigger | Two migrations on different slots — possibly slots that map to the same shard — are finalized at overlapping times. |
| Observable | Both prepare, both drain, both complete. Each barrier fences only its own slot's writes; each attempt carries its own `seq`; aborting or completing one leaves the other untouched. |
| NOT observable | A second finalization blocked behind the first, or one abort tearing down both barriers. Serialising them would make a busy shard's rebalance take a barrier window per slot, and the fencing has no cross-slot invariant to protect. |
| Invariant | Handoffs are stored per migration record, keyed by slot, and the barrier is stored in `PauseState`'s slot-keyed map (FM-CLUSTER-079/082), so there is no shared cell for two slots to contend on. The drain is a shard *round trip*, not a lock: two `ClusterMsg::DrainSlot` messages for two slots on one shard are answered in order and neither holds the shard between messages. |
| Catalog | `INV-HANDOFF-2` — a handoff filed under a slot it does not name is exactly the shared cell this row says two slots do not have. |
| Outcome variant | n/a |
| Forced by | `concurrent_handoffs_on_two_slots_do_not_interfere`, `handoff_model_full_cross_slot` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

## FM-CLUSTER-089 — every handoff deadline is proposer-minted data, never a clock read during apply

| Field | Value |
|---|---|
| Trigger | Any node applies a handoff entry — at commit time, replaying the log after a restart, or catching up from a snapshot, all at different wall-clock instants. |
| Observable | The prepared record's barrier and lease expiry are functions of `prepared_at_ms`, `barrier_ms`, and `lease_ms` alone, all of which travelled in the log entry. Two nodes applying the same entries reach byte-identical state whatever the hour. |
| NOT observable | A state machine that reads a clock during `apply`. A node replaying yesterday's log would compute today's deadlines, decide a `Complete` was inadmissible where the original leader decided it was, and diverge — a split-brain over slot ownership produced by nothing but a restart. |
| Invariant | The proposer mints every timestamp through `handoff_now_ms`, which reads `frogdb_core::clock::system_now` (the clock seam, `just lint-clock-seam`); `apply_command` only ever compares two values that arrived as data (`cluster-runtime/src/handoff_barrier.rs`, `cluster/src/types.rs`). The comparison helpers are on `SlotHandoff` rather than inline so there is one place for the arithmetic to be wrong. |
| Outcome variant | n/a |
| Forced by | `handoff_deadlines_are_pure_functions_of_replicated_data`, `handoff_now_ms_reads_the_clock_seam` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

## FM-CLUSTER-090 — only the migration source acts on a handoff, and it fences before it drains

| Field | Value |
|---|---|
| Trigger | `SlotHandoffPrepared` and `SlotHandoffReleased` apply on every node in the cluster, because Raft entries apply everywhere. |
| Observable | The node whose id equals the handoff's `source_node` arms a `WRITE`-mode pause on the slot and sends `ClusterMsg::DrainSlot` to shard `slot % num_shards`, confirming the drain only after the shard answers. Every other node ignores both events. A release lifts the barrier that node's own prepare armed. |
| NOT observable | A node fencing a slot it does not own, which would park writes nobody asked to park. Nor the arm being deferred: `Prepared` and `Released` are not commutative, so arming from the spawned drain task could let a release be handled first and leave the slot fenced with nothing left to lift it. Only the drain round trip — the part that can block on a busy shard — is spawned. |
| Invariant | `plan_handoff_action` is a pure function of the event, the node's own id, and the shard count, returning `Arm`/`Release`/`Ignore`; the runner arms and releases inline in its recv loop and spawns only `drain_shard` (`cluster-runtime/src/handoff_barrier.rs`). `num_shards == 0` is folded to shard 0 rather than dividing by zero. The pause is `WRITE`, not `ALL`: the source still holds the data and still owns the slot until `Complete`, so reads stay served — Redis/Valkey atomic slot migration pauses writes too. |
| Catalog | `INV-HANDOFF-2` — the record's own slot is the slot armed; a migration filed under a foreign slot would fence one slot on another's behalf. |
| Outcome variant | `HandoffAction::{Arm, Release, Ignore}` |
| Forced by | `only_the_source_node_acts_on_a_handoff`, `drain_targets_slot_modulo_num_shards_and_survives_zero`, `a_prepare_arms_the_barrier_drains_the_shard_and_confirms`, `a_release_lifts_the_barrier_its_prepare_armed` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

## FM-CLUSTER-091 — a drain that never arrives aborts the finalization instead of proceeding

| Field | Value |
|---|---|
| Trigger | The source cannot answer the drain: its shard is wedged past `HANDOFF_DRAIN_TIMEOUT_MS`, the shard channel is gone, or the node itself is dead before the prepare applies. |
| Observable | The source never proposes `ConfirmSlotHandoffDrained`. The finalizer's `HANDOFF_DRAIN_WAIT_MS` budget lapses, it proposes `AbortSlotHandoff`, and it answers `TRYAGAIN`. Ownership does not move, the migration record survives with no prepared handoff, and `CLUSTER SETSLOT <slot> STABLE` still cancels it. |
| NOT observable | The barrier being dropped so the finalization can proceed anyway — DragonflyDB's choice, and the reason its pause is explicitly not a correctness mechanism. Proceeding would move ownership with an unbounded set of writes still in flight on the former owner, which is the exposure being fenced. Nor a dangling prepared record: the finalizer aborts its own attempt, and the lease (FM-CLUSTER-085) covers the case where it cannot. |
| Invariant | `drain_shard` bounds the round trip with `tokio::time::timeout` and returns `false` on expiry or a missing shard, and the runner calls `confirm` only on `true` (`cluster-runtime/src/handoff_barrier.rs`). `SlotMigrationCoordinator::complete` polls its own replicated state for the acknowledgement, so a finalizer that is neither source nor leader still sees it; on timeout it aborts and renders `ClusterError::is_retryable` as `TRYAGAIN` rather than `ERR` (`server/src/slot_migration/mod.rs`). |
| Outcome variant | `-TRYAGAIN` |
| Forced by | `a_shard_that_never_answers_is_never_confirmed`, `a_missing_shard_fails_the_drain`, `a_source_that_cannot_drain_aborts_the_finalization`, `only_a_not_ready_handoff_is_retryable` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

## FM-CLUSTER-092 — a write caught by the barrier wakes up redirected, never acknowledged on the former owner

| Field | Value |
|---|---|
| Trigger | A client writes a key in the migrating slot while the finalization barrier is armed on the source. |
| Observable | The write parks. The finalization runs to completion underneath it — prepare, drain, confirm, complete — and the release that follows wakes the write, which is then re-validated against the topology that exists *now* and answered `MOVED <slot> <target>`. |
| NOT observable | The write being acknowledged by the source. That acknowledgement is the bug: the client is told the write succeeded, the slot then belongs to a node that never saw it, and nothing in the protocol tells anyone. Nor the write waking before ownership moves: ownership and the release event come from the same `apply`, in that order. |
| Invariant | The pause gate sits ahead of `ClusterSlotValidation` in the dispatch pipeline, so a parked command re-enters slot validation on release rather than resuming past it (`connection/dispatch.rs`). The release is emitted by `CompleteSlotMigration` *after* the assignment is mutated, so there is no ordering in which a woken write sees the old owner. |
| Outcome variant | `-MOVED` |
| Forced by | `a_write_parked_by_the_barrier_wakes_up_redirected` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

## FM-CLUSTER-093 — a transaction parked across a finalization is redirected, not committed

| Field | Value |
|---|---|
| Trigger | A client queues writes for a key in the migrating slot, issues `EXEC` while the finalization barrier is armed on the source, and the handoff completes underneath the parked batch. |
| Observable | `EXEC` parks. On release the batch's slot routing is re-validated and the reply is `MOVED <slot> <target>`. The former owner's `DBSIZE` is unchanged: a queued `DEL` against a key that was resident before the migration opened leaves that key in place. |
| NOT observable | The batch committing on the former owner and answering an array of results. Nor the redirect arriving with the write applied anyway — being told `MOVED` is the client's half of the property; nothing landing on the node that stopped owning the slot is the cluster's half, and a `-MOVED` with the `DEL` applied would satisfy the first while violating the second. |
| Invariant | `EXEC` reaches the barrier by a different route than a bare write: it is neither `WRITE`- nor `SCRIPT`-flagged, so the pause gate waves it through and the batch parks inside `handle_exec` through the `TxnHost` seam, after its slot verdict was already taken. The post-wait re-validation in `txn/src/exec.rs` is what closes that gap, and it runs only when the batch actually parked (FM-CLUSTER-083). |
| Outcome variant | `-MOVED` |
| Forced by | `an_exec_parked_by_the_barrier_wakes_up_redirected` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

## FM-CLUSTER-094 — a script in flight across a handoff leaves no write on the former owner

| Field | Value |
|---|---|
| Trigger | An `EVAL` whose declared key is in the migrating slot is issued while the finalization barrier is armed, and the handoff completes while the script is in flight. |
| Observable | The script parks, and on release answers `MOVED <slot> <target>`. `DBSIZE` on the former owner stays `0`: the `redis.call('SET', ...)` inside the script never becomes a key on the node that stopped owning the slot. |
| NOT observable | The script's write surviving on the former owner. Scripts *declare* their keys rather than having them derived, and for a long time nothing validated the declaration at all (FM-CLUSTER-030) — a handoff is exactly the case where an unvalidated declaration turns into a silently orphaned write. Nor the script being answered `MOVED` after its effects were already applied and replicated. |
| Invariant | A script is `SCRIPT`-flagged, so the pause gate parks it like a write and it re-enters `ClusterSlotValidation` on release; the execute-seam fence (FM-CLUSTER-095) covers the case where the handoff is prepared *after* validation admitted it. Redis reaches the same place from the other direction, re-checking cluster state per `redis.call` in `scriptVerifyClusterState`; FrogDB checks once at the seam because a FrogDB script's writes are applied as one effect set rather than call by call. |
| Outcome variant | `-MOVED` |
| Forced by | `a_script_in_flight_across_a_handoff_leaves_no_write_on_the_former_owner` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

## FM-CLUSTER-095 — a command validated before a handoff was prepared is refused at execution, not served

| Field | Value |
|---|---|
| Trigger | A command's slot is validated on the owner, and a `PrepareSlotHandoff` for that slot applies on the same node before the command's response is produced. Includes the case where ownership itself has already moved on. |
| Observable | The command is refused instead of acknowledged. `TRYAGAIN Slot <slot> finalization in progress` while the node is still the owner — a prepared handoff can still abort, so naming the target would bounce the client to a node that may never take the slot. `MOVED <slot> <addr>` once ownership has actually moved, and `CLUSTERDOWN` when the new owner cannot be addressed. An unchanged generation admits the command with no extra work. |
| NOT observable | The refusal firing on a handoff prepared for a *different* slot: the token carries one slot's generation, not a node-wide epoch, so unrelated cluster traffic (`SETSLOT … STABLE` churn on another slot) cannot cause spurious refusals. Nor the target of a migration fencing itself — only the owner is stamped, since an importing node answering `MOVED` would name itself. Nor the token consulting the handoff *lease*: expiry is a function of the clock, and a lease-filtered token would read "unchanged" across a prepare whose lease had lapsed. The raw replicated `seq` is pure data (FM-CLUSTER-089). |
| Invariant | `stamp_fence` records `(slot, owner, handoff_seq)` at validation time and `fence_verdict` compares it against the snapshot at execution time; the coordinator and the dispatcher share one `Arc<ClusterState>`, so the two reads are of the same published view (`slot_migration/slot_fence.rs`, `connection/{guards,dispatch}.rs`). The check is spent at the driver's single short-circuit arm, which covers shard commands, bare scripts and `EXEC` at once, and runs before error accounting so the recorded response is the final one. `handoff_seq` moves at *prepare*, a full Raft round trip before ownership does, which is why the token closes a window that "am I still the owner?" cannot: the source only learns ownership moved by applying `Complete`, and that lag is the window. The `TRYAGAIN` body is minted by the redirect seam like every other redirect (`types/src/redirect.rs`), forced there by `tryagain_slot_handoff_names_the_slot` and `the_two_tryagain_forms_share_a_prefix_and_differ_in_body` — `frogdb-types` is outside the crate set this lint tracks, so those two are named here rather than in `Forced by`. |
| Outcome variant | `-TRYAGAIN` / `-MOVED` / `-CLUSTERDOWN` |
| Forced by | `only_the_owner_is_stamped`, `a_prepared_handoff_is_part_of_the_stamp`, `an_unchanged_generation_admits_the_command`, `a_handoff_prepared_after_validation_refuses_with_tryagain`, `a_superseding_attempt_refuses_too`, `an_aborted_attempt_leaves_the_generation_where_it_was`, `ownership_that_moved_refuses_with_moved_at_the_new_owner`, `an_owner_we_cannot_address_degrades_to_clusterdown`, `a_slot_that_lost_its_owner_degrades_to_clusterdown`, `a_handoff_on_another_slot_is_invisible`, `no_write_is_acknowledged_after_the_slot_is_handed_over_under_load` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

A fenced command may still have *applied* locally — the fence refuses the acknowledgement, not the
work. Its status is therefore in doubt exactly as a write whose connection dropped is, and the
client's retry lands on the new owner. That is the ordinary at-least-once contract rather than the
acknowledged-then-orphaned write of FM-CLUSTER-037, which is the whole distinction the barrier
exists to draw.

## FM-CLUSTER-096 — a barrier parks a transaction only if the batch can reach the barriered slot

| Field | Value |
|---|---|
| Trigger | A slot barrier is armed and a client `EXEC`s a batch: all of whose commands pin to that slot, all to some other single slot, or which cannot be pinned at all. |
| Observable | The batch pins to a slot only when every queued command pins to that *same* slot, matched case-insensitively against the client's own casing. A batch pinned elsewhere commits while the barrier is up; a batch pinned to the barriered slot parks; an unpinnable batch — straddling slots, containing a keyless command such as `FLUSHALL`, or empty — parks on any armed slot barrier. A node-global `CLIENT PAUSE` still parks everything. |
| NOT observable | A barrier on one slot disarming for the others: a second `EXEC` on the barriered slot in the same test still parks, so this is a narrowing, not a disarm. Nor a straddling batch answering `CROSSSLOT` from the pause gate — a transaction may legitimately straddle slots where a single command may not, and the gate is only asked whether the barrier can safely let the batch past. Nor an unpinnable batch slipping through: `FLUSHALL` erases the barriered slot along with everything else. |
| Invariant | `queue_pause_slot` folds the batch's commands through the same `command_pause_slot` a single command uses, returning `None` — fail-closed — on the first unpinnable command or the first disagreement (`connection/pause_gate.rs`). The fold is a pure function of the registry and the queue, so it is forced without a socket. `wait_if_paused` carries the queue across the `TxnHost` seam and `pause_active_for_batch` consults the node dimension first, the slot map only if a slot-scoped pause exists at all, so the unbarriered path costs one lock (`txn/src/host.rs`, `connection/{transaction,lifecycle}.rs`). |
| Outcome variant | `Option<u16>` / `Response::Array` |
| Forced by | `a_batch_whose_commands_share_a_slot_pins_to_it`, `a_batch_straddling_slots_cannot_be_pinned`, `one_unpinnable_command_unpins_the_whole_batch`, `an_empty_batch_is_unpinnable`, `queued_command_names_are_matched_case_insensitively`, `the_pause_barrier_is_handed_the_whole_batch`, `an_exec_on_an_unbarriered_slot_runs_while_the_barrier_is_up` |
| Bug refs | [02-migration-finalization-pause-barrier.md](../.scratch/replication-cluster-rework/issues/done/02-migration-finalization-pause-barrier.md) |

Still unpinned after phase 2b, deliberately. A cross-shard script holding a VLL continuation across
a handoff has no row: the continuation-lock gate has two tracked bypasses of its own
(`MULTI`/`EXEC`, `FCALL`), so a row written now would pin the barrier's behaviour on top of a seam
that is itself known-leaky, and it belongs with those issues rather than ahead of them. Whether the
barrier should also stall a replication primary's replica feed is no longer undecided — it does,
and FM-CLUSTER-097 is that row. The residual window itself (`t_source - t_leader`, still ~0.7 ms p50 under load) is a replication lag
and is not claimed to be closed; what FM-CLUSTER-095 claims is that nothing client-visible happens
inside it.

## FM-CLUSTER-097 — a slot-handoff barrier holds the node's replica feed for the barrier window

| Field | Value |
|---|---|
| Trigger | A slot-scoped pause is armed on a node that is also a replication primary — the slot-handoff write barrier, hand-armed by `DEBUG PAUSE-SLOT` or armed by the handoff itself — while a replica is attached and streaming. |
| Observable | Nothing the node applies during the barrier window reaches the replica: its `DBSIZE` does not move, on the barriered slot or any other. When the barrier ends — the handoff completes, aborts, or the pause simply lapses — the feed resumes and the replica converges on everything held, in offset order and with nothing dropped. A replica that PSYNCs *into* the window waits before it replays the backlog tail, so the held writes do not leak out of the other lane. |
| NOT observable | A node-global `CLIENT PAUSE` holding the feed: it stops the writes themselves, so there is nothing new to ship and stalling a replica behind it would be pure lag — Redis likewise reserves `PAUSE_ACTION_REPLICA` for the migration pause. Nor a per-shard hold: frames carry a shard id but there is one monotonic replication offset (`OffsetCoordinator::advance`), so shipping shard B while holding shard A would put offsets on the wire out of order, which is worse than the anomaly. Nor rollback of the fenced-but-applied writes of FM-CLUSTER-095 — they are legitimate local state and they do ship once the hold ends; what the hold buys is that they do not ship *while the client is being told to retry elsewhere*. Nor a feed that can wedge: the hold carries the barrier's own deadline, so a finalizer that dies mid-handoff cannot leave the feed held. |
| Invariant | `PauseState::feed_hold_until` derives the hold — the latest deadline across armed slot pauses — from the same pause state the write barrier reads, and `ClientRegistry::publish_pause_derived_state` republishes it to a `ReplicaFeedGate` on *every* pause mutation, so the two halves of the barrier cannot disagree about whether it is up (`core/src/client_registry/mod.rs`, `replication/src/feed_gate.rs`). The gate stores an `Instant`, not a latch: `hold_deadline` answers `None` once `clock::now()` passes it, so normal completion, abort, and a lapsed lease all release without anyone clearing anything. Both of those rules are pure functions of plain data — `decide_publish(current, next)` (store-and-wake exactly when the value changes, in either direction) and `decide_hold(published, now)` (in force strictly before the deadline) — with `ReplicaFeedGate` owning only the mutex, the `Notify` and the clock read. `ReplicaSession::start_streaming` waits on it after subscribing and buffers frames in a per-session `VecDeque` while held, draining in offset order on release, so ordering survives the hold and a held session cannot be `Lagged` off the broadcast channel. |
| Outcome variant | `Option<Instant>` |
| Forced by | `the_barrier_holds_the_replica_feed_until_the_handoff_releases_it`, `slot_barrier_publishes_and_clears_the_replica_feed_hold`, `a_node_pause_does_not_hold_the_replica_feed`, `a_lapsed_barrier_frees_the_feed_with_nobody_clearing_it`, `overlapping_barriers_hold_the_feed_to_the_later_deadline`, `a_published_deadline_holds_the_feed_until_it_lapses`, `a_lapsed_hold_releases_a_waiter_without_an_explicit_release`, `an_explicit_release_wakes_a_waiter_early`, `a_shortened_deadline_is_honoured`, `an_open_gate_holds_nothing`, `republishing_the_same_deadline_is_a_no_op`, `a_publish_stores_and_wakes_exactly_when_it_changes_the_value`, `a_shorter_deadline_is_a_change`, `the_hold_is_the_latest_deadline_across_armed_barriers`, `a_published_hold_is_in_force_strictly_before_its_deadline`, `a_lapsed_hold_never_comes_back`, `feed_gate_model_smoke`, `feed_gate_model_full_overlapping`, `feed_gate_model_full_churn`, `a_feed_that_ignores_the_gate_ships_inside_a_barrier_window`, `the_pre_fix_write_task_ships_inside_the_barrier_window`, `the_shipped_write_task_refuses_that_counterexample`, `a_release_does_not_open_a_feed_another_barrier_still_holds` |
| Bug refs | [12-barrier-vs-replica-feed-policy.md](../.scratch/replication-cluster-rework/issues/done/12-barrier-vs-replica-feed-policy.md) |

The hold is node-wide for the barrier window (≤100 ms in production, backstopped by the handoff
lease), which is what Redis 8.4 and Valkey 9.0 do: their atomic-slot-migration pause is node-wide
and includes `PAUSE_ACTION_REPLICA`, stopping the primary from flushing replica output buffers so
replicas cannot run ahead of a primary that is fencing its own clients.

## FM-CLUSTER-098 — an acknowledged vote is on the platter, not in an unsynced memtable

| Field | Value |
|---|---|
| Trigger | openraft persists a vote — the node's own candidacy or a grant to a peer — and the machine loses power immediately after `save_vote` returns, before any later write happens to reach the disk. |
| Observable | The restarted node's `read_vote` returns the vote that was acknowledged. The record lives in the `raft_meta` column family and is written with `sync`, so the acknowledgement and the durability are the same event. |
| NOT observable | An `Ok` from `save_vote` that rests on `DB::flush()`: that flushes the **default** column family, which this storage never writes to at all, while the vote sits in `raft_meta` behind an unsynced WAL. Downstream, the thing that must never be observable is the node granting a second vote in the same term after a restart — Raft's single-leader argument is exactly the claim that this cannot happen, so a lost vote is a split-brain precondition rather than a performance detail. Nor the durability decision being the caller's: a future `set_meta` caller cannot write the vote buffered by forgetting to ask. |
| Invariant | `MetaDurability::for_key` classifies each metadata key — `KEY_VOTE` synced, `KEY_COMMITTED` and `KEY_LAST_PURGED` buffered — and `set_meta`/`delete_meta` render that class into the write options at the one chokepoint every metadata write passes through (`cluster/src/storage.rs`). `save_vote` is then just that write: no `flush()`, misleading or otherwise. The two buffered keys are buffered on purpose — the committed index is deliberately never read back (openraft re-derives it from the leader), and losing the purge watermark only costs a repeat purge. `ClusterSnapshotStore::save` reached the same conclusion from the snapshot side and is the pattern this follows. |
| Outcome variant | n/a |
| Forced by | `the_vote_is_written_synced_to_the_meta_column_family` |
| Bug refs | fixed: [01-save-vote-flushes-the-wrong-column-family.md](../.scratch/hardening-2/issues/done/01-save-vote-flushes-the-wrong-column-family.md) |

The witness is level 2, and deliberately so: RocksDB exposes no getter for `sync` and an fsync
cannot be observed without cutting power, so the test pins the *classification* the vote path
resolves to plus the fact that the default column family is empty — proving the old `flush()` could
never have been what made the vote durable. The level-5 witness (kill the process between the ack
and the restart, assert the restored vote) needs the campaign-2 crash harness and belongs to
[73](../.scratch/testing-improvements-round2/issues/) as much as to this row; `append` acks a log write
non-synced for the same family of reasons and is still open.

## FM-CLUSTER-099 — a log reader never serves an entry the writing handle truncated

| Field | Value |
|---|---|
| Trigger | A leadership flap: entries are cached while the node is leader, it steps down, the conflicting suffix is truncated and different content is appended at the same indexes, and the log reader openraft created at startup is asked for those indexes again. Same shape for `purge`. |
| Observable | The reader returns the entry that is on disk *now* — the replacement's log id and payload — and an index the owner invalidated is gone from the reader's view rather than answered from a copy. |
| NOT observable | An entry from an overwritten term served at an index that was truncated and re-appended. openraft builds the reader once and holds it for the node's lifetime, so a detached cache is stale for the rest of the process: the leader would ship entries no quorum ever agreed on, or membership recovery would read the wrong entry — Raft log divergence produced entirely below the state machine. Nor the inverse: a reader filling the cache while the owner cannot see the fill, which would make coherence depend on which handle happened to read first. |
| Invariant | One `Arc<RwLock<BTreeMap<u64, Entry>>>` is shared by the writing handle and every reader `get_log_reader` hands out, alongside the `DB` and the snapshot lock (`cluster/src/storage.rs`). `invalidate_cache_range` — the only invalidation, reached from `truncate` and `purge` — therefore reaches every reader by construction, instead of the owner invalidating a copy nobody reads. Sharing rather than dropping the cache keeps the read path's benefit; the coherence machinery is one `Arc`, because there is exactly one cache. |
| Outcome variant | n/a |
| Forced by | `a_log_reader_never_serves_an_entry_the_owner_truncated`, `cache_invalidation_reaches_a_reader`, `openraft_conformance_suite_through_a_long_lived_reader`, `a_reader_and_its_owner_never_disagree_with_the_column_family` |
| Bug refs | fixed: [53-stale-raft-log-reader-cache.md](../.scratch/testing-improvements-round2/issues/done/53-stale-raft-log-reader-cache.md) |

The first two witnesses are crate-level and deterministic, and they were the first tests anywhere to
construct a reader through `get_log_reader`: the defect was found by reading, not by failure,
because nothing outside openraft itself had ever exercised the reader clone. The last two are the
generated form of the same guarantee
([issue 21](../.scratch/cluster-correctness/issues/done/21-no-layer-sees-the-raft-log-store.md)) — the
whole openraft conformance suite re-run with every read served by a reader taken before the first
write, and a property over generated append/truncate/purge sequences that judges both handles
against the `raft_logs` column family itself rather than against another read through the same
cache.

Of the two generated witnesses only the property discriminates: re-reverting the fix leaves the
reader-backed suite green, because openraft's own cases never read an index, overwrite it, and read
it again through the same handle — the exact interleaving the detached cache needs. The property
shrinks that interleaving to a single operation and reports it as a term mismatch at a named index.
The reader-backed suite is listed here anyway because it is what makes the reader path conform at
all — before it, no case in the suite ran through a reader — and it is the layer that would catch a
reader diverging for any *other* reason.

## FM-CLUSTER-100 — the handoff generation survives a snapshot restore instead of restarting at zero

| Field | Value |
|---|---|
| Trigger | A node rebuilds its cluster state from a snapshot rather than by replaying the log — openraft's `install_snapshot` after it fell too far behind, the persisted snapshot reloaded at boot, or the `ClusterSnapshot` DTO handed to `ClusterState::from_snapshot`. Some handoffs before that snapshot have already completed or aborted, so the records that carried their `seq` are gone. |
| Observable | The next `PrepareSlotHandoff` the restored node applies mints a `seq` strictly greater than every `seq` the pre-snapshot cluster minted — the counter picks up where it left off through either restore vehicle. `ResetCluster` still rewinds it to `0`, and that rewind is itself what a later snapshot carries. |
| NOT observable | A restored node re-minting a `seq` that is already spent. `SlotFence` compares `(owner, handoff_seq)` as a pure replicated fencing token (FM-CLUSTER-095) and reads absent-handoff as `0`, so a reused generation makes a stale fence compare equal and admit a command the prepare it was stamped before should have refused. Nor the counter being re-derived as `max(seq)` over the surviving migrations: a completed or aborted handoff removes its record and keeps its generation spent, so the derived value is exactly the reused one. |
| Invariant | `handoff_seq` lives on `ClusterStateInner` and is projected by `to_snapshot`, so **both** vehicles carry it: openraft ships the serialized `ClusterStateInner` (`encode_snapshot`/`install_snapshot`/`attach_snapshot_store`), and the reader DTO carries the field for the sake of `from_snapshot`, which is the only restore path that goes through the projection (`cluster/src/{state,types}.rs`). Both sides are `#[serde(default)]`, so a snapshot written before the field existed reads as `0` — the pre-handoff state, where nothing has been minted. `ResetCluster` remains the sole rewind (FM-CLUSTER-086), and it clears every migration in the same entry, so no fence can outlive it. |
| Catalog | `INV-HANDOFF-1` — the counter dominates every surviving `seq` through both restore vehicles: the hook runs at `from_snapshot` and `restore_from_snapshot` as well as at `apply_command`. |
| Outcome variant | `ClusterEvent::SlotHandoffPrepared { seq }` |
| Forced by | `an_installed_snapshot_carries_the_handoff_generation`, `from_snapshot_carries_the_handoff_generation`, `a_reset_is_the_one_rewind_and_it_survives_a_restore`, `handoff_model_smoke`, `handoff_model_full_cross_slot`, `handoff_model_full_deep` |
| Bug refs | n/a — found by inspection while auditing the fencing token; the reader DTO dropped the counter and `from_snapshot` documented the reset as safe. |

## FM-CLUSTER-101 — a node removed from the topology stops being a Raft voter

| Field | Value |
|---|---|
| Trigger | A committed cluster command removes a node from the topology: `CLUSTER FORGET` (`RemoveNode`), or a forced failover (`Failover { force: true }`) — client-issued or decided by the failure detector — which removes the old primary outright. Includes the removal of a node that is already down, of the Raft leader itself, and a `FORGET` repeated on every surviving node. |
| Observable | The removed node leaves the Raft membership, so quorum is recomputed over the survivors: four voters shrunk to three survive one further failure, which four voters minus a departed one cannot. A node Raft knows only as a learner leaves the membership too. A removal that changes no topology proposes no membership change — a command the state machine refuses (`node … not found`, a rejected failover) never reaches the voter side effect, and a removal that names a node Raft never knew proposes nothing either, so the second and third `FORGET` of the same id, the documented operator procedure, leave the membership entry in force exactly where it was. The last voter is never removed. |
| NOT observable | The removal waiting on the removed node: a membership change commits on the quorum of the *new* configuration, so the node being retired — normally already dead, which is why it is being retired — cannot block its own removal. Nor the leader refusing to remove itself: `FORGET` is always executed on the leader (directly, or by the leader-side `ForwardedWrite` receiver after a follower forwarded it), so a self-skip would make forgetting the leader permanently impossible; openraft steps the leader down after the entry commits. Nor `ResetCluster` shrinking the voter set — a reset tears the whole cluster down and re-forms it, and its own `forget_nodes` handling drives the transport, so it is deliberately outside this rule. Nor a graceful `Failover { force: false }`, which demotes the old primary and leaves it a member: a demoted primary still votes. |
| Invariant | `voter_change` is the single rule mapping a committed `ClusterCommand` to its voter-set effect, and all three leader-side commit sites consult it — the leader-local arm in `connection/cluster.rs`, the `ForwardedWrite` receiver in `network.rs`, and `trigger_auto_failover` in `cluster-runtime/src/failure_detector.rs`, which reaches Raft through the `DetectorRaft` seam because it writes through a fake in tests — so a forwarded `FORGET`, a locally-proposed one, and a failover nobody typed shrink membership identically, and the forced failover is covered without the connection layer's `unregister_node` hint ever naming it. `spawn_remove_raft_voter` mirrors `spawn_add_raft_voter`: same `MAX_ATTEMPTS`, same `voter_retry_delay` backoff, both spawned off the commit path so a membership round trip cannot stall the client reply. Which change to propose is classified against the membership actually in force (`plan_voter_removal`), because the two openraft changes are not interchangeable: `RemoveVoters` cannot reach a learner and `RemoveNodes` is refused while the node still votes. `retain: false` drops the ex-voter from the node map rather than demoting it to a learner Raft would keep replicating to. |
| Outcome variant | `Option<VoterChange>` / `VoterRemoval` |
| Forced by | `every_command_that_moves_the_node_set_owes_a_voter_change`, `a_removal_is_planned_against_the_membership_in_force`, `forgetting_a_learner_drops_it_from_the_membership`, `forgetting_a_stranger_proposes_nothing`, `the_last_voter_is_never_removed`, `a_voter_change_dispatches_to_the_matching_raft_side_effect`, `the_production_seam_reaches_raft_with_the_voter_change`, `auto_failover_removes_the_failed_primary_from_the_voter_set`, `a_rejected_auto_failover_leaves_the_voter_set_alone`, `test_cluster_forget_shrinks_the_raft_voter_set` |
| Bug refs | [87-cluster-residual-test-gaps.md](../.scratch/testing-improvements-round2/issues/open/87-cluster-residual-test-gaps.md) |

The add half of this rule shipped with `CLUSTER MEET`; the remove half did not, so until now
`change_membership` appeared exactly once in the repository. The asymmetry is silent — the topology
`CLUSTER NODES` renders was correct the whole time — and it degrades availability in the direction
operators least expect: retiring a dead node left it in every quorum computation for ever, so a
cluster that had "removed" a failed node was less fault-tolerant than one that had not touched it.

## FM-CLUSTER-102 — degenerate failure-detector config is clamped, not trusted verbatim

| Field | Value |
|---|---|
| Trigger | `FailureDetectorConfig { check_interval_ms, connect_timeout_ms, fail_threshold }` constructed with a zero, or a `u64`/`u32::MAX`, value in any of the three fields. |
| Observable | `FailureDetector::new` clamps every field into `[MIN_*, MAX_*]` before storing it, and `FailureDetector::config()` reports the clamped values — never the ones the caller passed. At the clamped minimum (`fail_threshold = 1`) latching still works: one failure latches, one success clears it. The detector's background task starts and keeps ticking on a clamped `check_interval_ms` instead of panicking on its first tick. |
| NOT observable | A `tokio::time::interval` or `tokio::time::timeout` panic from a zero `check_interval_ms`/`connect_timeout_ms`. Nor a `Duration` multiplication overflow (`Mul<u32>` panics on overflow) out of `HealthTable::stale_threshold`'s `check_interval * (fail_threshold + 2)` at a huge `fail_threshold`. Nor mass false failures from a zero `connect_timeout_ms`, where `tokio::time::timeout` would treat every probe as already elapsed regardless of whether the peer answers. |
| Invariant | `FailureDetectorConfig::clamped()` is a pure `.clamp(MIN, MAX)` per field, called once at the top of `FailureDetector::new` before the value is stored or handed to `HealthTable::new` (`failure_detector.rs`). Every later reader — `raft_write_timeout`, `spawn_failure_detector_task`'s interval/timeout construction, `trigger_auto_failover`'s probe timeout, `HealthTable::stale_threshold` — reads `self.config`/the table's own copy, so clamping once at construction is sufficient; there is no second path that could see the unclamped value. Out-of-range values are pulled to the nearest bound rather than rejected: this is a bare timing knob with no cross-field relationship to enforce (unlike `heartbeat_interval_ms < election_timeout_ms` in `ClusterConfigSection::validate()`, which rejects at config-parse time), so silently sanitizing it at construction matches this codebase's precedent for that shape of value (`tls_watch.rs` floors `watch_debounce_ms` with `Duration::from_millis(x).max(MIN_POLL_INTERVAL)` rather than refusing to start). |
| Outcome variant | `FailureDetectorConfig` (clamped fields) |
| Forced by | `degenerate_zero_config_is_clamped_to_safe_minimums`, `huge_config_values_are_clamped_and_do_not_overflow_the_staleness_multiply`, `a_zero_check_interval_does_not_panic_the_detector_task`, `c1_admission_is_total`, `c2_every_derived_duration_is_finite_and_nonzero` |
| Bug refs | — |

## FM-CLUSTER-103 — `truncate` removes the conflicting entry it names, not only what follows it

| Field | Value |
|---|---|
| Trigger | openraft finds a follower's log conflicting with the leader's and issues `DeleteConflictLog { since }` (`raft_core.rs`, from `FollowingHandler::truncate_logs`). Two shapes reach the store: an `AppendEntries` batch that disagrees at `since`, where the leader's entries are re-appended immediately after the truncate, and a `prev_log_id` that does not match, where the append is **rejected** and nothing is written after the truncate at all. |
| Observable | Everything from `log_id.index` upward is gone — from the `raft_logs` column family and from the log cache — so `get_log_state` names `log_id.index - 1` as the tail and a reopened store serves exactly the prefix openraft kept in memory. `truncate` at the very first index empties the log. This is openraft's own contract ("Truncate logs since `log_id`, inclusive"), so the statement of it is openraft's storage conformance suite rather than a reading of it. |
| NOT observable | The entry *at* `log_id.index` surviving. openraft drops it from `RaftState::log_ids` the moment it emits the command, so a store that keeps it disagrees with the state that decided to remove it — and in the rejected-append shape nothing is written afterwards to overwrite the survivor. A restart then hands `get_log_state` a `last_log_id` from a term the leader already ruled conflicting, and the node campaigns and answers `AppendEntries` on the strength of an entry no quorum agreed on: Raft log divergence one index wide, manufactured by the store. Nor the mirror error at the cache: an entry deleted from RocksDB but left in the cache would be served by the very next read. |
| Invariant | `truncate(log_id)` deletes the key range `[log_id.index, +oo)` and invalidates the cache over the same `[log_id.index, ..]` — one bound, written once and used for both, so the disk and the cache cannot drift apart by an index. `purge(log_id)` is the mirror at the other end, `[0, log_id.index]` inclusive, and the two together mean every deletion boundary in the store is inclusive of the index it names (`cluster/src/storage.rs`). |
| Outcome variant | n/a |
| Forced by | `truncate_removes_the_named_index_and_everything_after_it`, `openraft_conformance_suite`, `openraft_conformance_suite_through_a_long_lived_reader` |
| Bug refs | fixed: [21-no-layer-sees-the-raft-log-store.md](../.scratch/cluster-correctness/issues/done/21-no-layer-sees-the-raft-log-store.md) |

Found by the layer [issue 21](../.scratch/cluster-correctness/issues/done/21-no-layer-sees-the-raft-log-store.md)
built, on its first run: `Suite::delete_logs_since_0` truncates a ten-entry log at index 0 and
expects nothing left, and the store left one entry behind. Every FrogDB test that had ever exercised
`truncate` was written against the store's own (exclusive) reading of the contract, so the
off-by-one was self-consistent everywhere below openraft and invisible to every layer above it —
which is exactly the claim the issue was filed to make.

## FM-CLUSTER-104 — a restored source re-arms its slot write barrier before admitting a write

| Field | Value |
|---|---|
| Trigger | A migration source armed its barrier on `SlotHandoffPrepared`, then restarts — or is caught up via `install_snapshot` — while the replicated `migrations` map still carries the prepared handoff naming it as source. |
| Observable | The barrier is armed again before the node admits any client write to the migrating slot. Both restore paths participate: boot-time snapshot restore replays the handoff state into the same `Arm` decisions the live event stream would have produced (the analogue of `reconcile_self_role` for the role view), and a live `install_snapshot` emits `Prepared`/`Released` on any handoff-state delta the same way it emits role changes. |
| NOT observable | The restarted source acknowledging a write to the migrating slot with its barrier unarmed. That write is the lost-write bug: the target completes the handoff on the source's *earlier* drain confirmation, so a post-restart write lands on a node the cluster is about to rule a non-owner, and nothing in the protocol tells anyone. FM-CLUSTER-095's `SlotFence` does not close the window — the stamp and the verdict read the same unchanged `handoff_seq`, so a post-restart write validates consistently. Nor is the barrier rebuilt from anything node-local: the replicated `migrations` map is the only authority, per the state-space table above. |
| Invariant | Ruled (issue 32, from distsys-review CRIT-1): a `reconcile_slot_handoff(...)` beside `reconcile_self_role` in `server/src/server/cluster_init.rs`, driven from the restored `migrations` map, emitting the `Prepared` events the barrier task would have seen live; `install_snapshot` (`cluster/src/state.rs`) gains the handoff analogue of `emit_self_role_change`. Today the barrier is armed exclusively from the live `SlotHandoffEvent` stream (`cluster-runtime/src/handoff_barrier.rs`, `run_slot_handoff_barrier`), `restore_from_snapshot` assigns without emitting, and boot ordering attaches the snapshot store before `enable_slot_handoff_notification()` — so the miss is guaranteed, not racy. |
| Outcome variant | `HandoffAction::Arm` (replayed on restore) |
| Forced by | MISSING ([gap: 32-restarted-source-never-re-arms-its-slot-write-barrier.md](../.scratch/cluster-correctness/issues/open/32-restarted-source-never-re-arms-its-slot-write-barrier.md)) — the forcing test (restart a source mid-handoff, assert a write to the migrating slot is refused) lands with the reconcile fix; spec row first per the spec-first flow. |
| Bug refs | [32-restarted-source-never-re-arms-its-slot-write-barrier.md](../.scratch/cluster-correctness/issues/open/32-restarted-source-never-re-arms-its-slot-write-barrier.md) |

Found by the independent distsys review (CRIT-1,
[2026-08-13-independent-distsys-review.md](../.scratch/formal-spec/2026-08-13-independent-distsys-review.md)):
the role view had the identical bug and issue 37 (arch-deepening) added `reconcile_self_role` for
it — "a role folded into a snapshot produced no log entry to replay". The barrier is the same
lesson, unapplied; CRDB rebuilds leaseholder/latch state from replicated range state on every
restart for exactly this reason. Survives issue 31's migration redesign unchanged: 31 keeps the
Prepare→drain→Complete finalization, and a restart mid-drain still requires re-arming.

---

## GAPS — behavior nothing forces

Listed so the mutation run's survivors can be triaged against a known list rather than
rediscovered. Each is a candidate row for the gap-filling step of this phase.

1. **`reconcile_topology`'s 2×3 decision matrix** (`failure_detector.rs:367-390`) — the whole
   leader-side reconciliation, including the "converged cluster performs zero Raft writes" property
   that FM-CLUSTER-014 leans on, has no unit test. It needs a `ClusterRaft`, so forcing it means
   extracting the matrix into a pure function first.
2. **`InflightGuard` dedup and drop** (`failure_detector.rs:107-123, 394-414`) — the at-most-one-
   write-per-peer property and the poison-recovering `Drop` are untested. Same blocker.
3. **`trigger_auto_failover` end to end** (`failure_detector.rs:487-629`) — the primary check, the
   empty-replica abandon, sequential offset probing, "unreachable scores 0 but stays a candidate",
   the 3-attempt retry loop, and the permanent give-up. The give-up is the highest-consequence
   untested path in the area: after it, the FAIL flag is already set, so level-triggered
   reconciliation never calls `mark_node_failed` again and no further automatic attempt is made.
4. **`check_node_reachable`** — liveness is a bare TCP connect, so a wedged-but-listening node reads
   as healthy. Nothing pins that, and nothing pins that the bus `HealthProbe` answers
   unconditionally without consulting quorum, fence, or loading state.
5. **The `Cluster` arm of `broadcast_publish`** — self-skip, the `flags.fail` skip, `JoinSet`
   aggregation, and the silently dropped `JoinError` from a panicked fan-out task. The new tests
   added for FM-CLUSTER-069/070 cover the early returns and the forward path, not the fan-out.
6. **`ClusterState::from_snapshot` zeroing `last_applied_log`**, and `apply` advancing
   `last_applied_log` even for entries whose command errored (`state.rs:65, 742`).
7. **Storage corruption paths** — `try_get_log_entries` with an `Excluded(0)` end bound silently
   reads to the end of the log; `purge` writes `last_purged` before the delete batch; the CF
   `.expect`s and `decode_log_key`'s fixed-width copy panic on a tampered DB.
8. ~~**Unvalidated detector config** — `check_interval_ms = 0` panics `tokio::time::interval`, and a
    huge `fail_threshold` panics the staleness multiply. Nothing rejects either at load.~~ Fixed:
    `FailureDetector::new` clamps all three fields at construction. See FM-CLUSTER-102.

## Tagging notes for whoever lands these

* Rows 018-030 (routing) and 061-064 (admin gating) are forced by unit tests that live **outside**
  the two crates this phase's mutation gate measures — `frogdb-server` and `frogdb-core`
  respectively. They are real unit tests, not integration tests, and they run in seconds; but
  `cargo mutants -p frogdb-cluster` will not execute them, so they contribute nothing to this
  area's score. The routing seam physically lives in `frogdb-server/crates/server/src/slot_migration/`; moving it
  would be a Phase 5 extraction, not a spec edit.
* Row 030's witness is `#[ignore]`d and therefore absent from `cargo nextest list`, so the row
  cites its issue as a gap instead. Re-point it when the bug is fixed.
* Row 038's witness does not exist; the dispatcher needs a seam first.
* The `cluster_*` integration binaries (`frogdb-server/crates/server/tests/cluster_{topology,
  slots,migration,failover,misc}.rs`, 185 tests) are end-to-end witnesses for most of these rows
  and may be added to `Forced by` cells, but no row above relies on one as its *only* witness —
  with two exceptions, both in the barrier family. FM-CLUSTER-093's and FM-CLUSTER-094's only
  witnesses are integration tests in `cluster_handoff_barrier.rs`, because a parked `EXEC` and an
  in-flight script are both reached through the `TxnHost` seam from a live connection and the
  unit-level guard fixture (`connection/guards.rs`'s `ViewFixture`) holds no client registry.
  FM-CLUSTER-083 was in the same position until phase 2b; widening the seam gave it a socketless
  witness (`the_pause_barrier_is_handed_the_whole_batch`, in `frogdb-txn`), which is a second
  argument for the widening beyond the over-park it narrowed. Note that `frogdb-txn`'s tests are
  outside this area's `cargo mutants -p frogdb-cluster` gate, exactly as rows 018-030 are.

## Redis deviations

| Mode | FrogDB | Redis | Rationale |
|---|---|---|---|
| Topology consensus | Raft (openraft) over a dedicated bus, metadata only | Gossip with per-node config epochs and majority agreement on failover | A replicated log gives an ordered, atomic `Failover` entry (FM-CLUSTER-040); gossip needs a multi-step protocol to reach the same place. Data replication is still PSYNC, so this is a control-plane change only. |
| Slot migration states | Two-point ownership swap; no intermediate migration state machine | `MIGRATING`/`IMPORTING` per-slot flags with the same two endpoints | The intermediate enum existed and only ever had one variant constructed. The importing/migrating *roles* are still derived, by comparing the local id against the migration's source and target. |
| `CLUSTER SETSLOT IMPORTING`/`MIGRATING` | Both lower to the same replicated `BeginSlotMigration` | Two independent local flags on two different nodes | Replication makes the pair converge on one record, which is why idempotency is load-bearing (FM-CLUSTER-031). |
| Gossip counters in `CLUSTER INFO` | No per-type lines at all; `cluster_stats_messages_sent`/`_received` count real bus frames | Per-type lines for every non-zero counter, then the same two totals | Not a deviation any more. Redis skips a per-type line whose counter is zero, and FrogDB's per-type counters are *structurally* zero (no gossip protocol), so the lines are absent for the same reason they would be absent on an idle Redis node. `total_cluster_links_buffer_limit_exceeded` stays `0`: there is no per-link output buffer limit to exceed, so zero is measured, not fabricated. See FM-CLUSTER-077. |
| `ping-sent`/`pong-recv` in `CLUSTER NODES` | Always `0 0` | Real timestamps | Same reason; positional fields cannot be omitted without breaking every client's parser, so `0` is the honest placeholder here. |
| `PFAIL` | Structurally supported, never produced | Set by gossip suspicion before a majority confirms `FAIL` | Leader-only detection has no suspicion phase: the leader either has enough consecutive failures to latch or it does not. |
| Shard pub/sub slot routing | `SPUBLISH` forwards to the slot owner when it can name one, and otherwise delivers locally; `SSUBSCRIBE` does not slot-route subscribers | Both are slot-routed like keyed commands, answering `MOVED`/`CLUSTERDOWN` | Since subscribers are not pinned to the owner, refusing a publish would drop a message no other node would deliver. The fallback is named rather than silent (FM-CLUSTER-070); adopting Redis' refusal means routing subscribers first. |
| `CLUSTER BUMPEPOCH` | Not supported | Supported | Epoch changes are authorized by the replicated log; a manual bump would create a claim no entry justifies. |
| `CLUSTER SET-CONFIG-EPOCH` | Sets the exact value; refused unless the node knows no peer and holds epoch 0 | Same two guards, same exact assignment | Identical (FM-CLUSTER-076). The command is replicated through Raft rather than applied locally, so the assignment is durable on the log; the guards are Redis' verbatim. |
| `WAIT` in cluster mode | Per-node, keyless, never redirects | Per-node, keyless, never redirects | Identical. Cluster replicas attach over the same PSYNC link and ACK into the same tracker, so there is no cluster-mode branch to deviate in. |
| Script slot validation | Once at the dispatch seam before the body runs, plus a fencing token re-checked at execution | Re-validated per `redis.call` (`scriptVerifyClusterState`) | Same window, closed at a different granularity. The absence of any check was a confirmed bug, fixed under FM-CLUSTER-030. Redis re-checks per call because each `redis.call` executes against live state as it runs; a FrogDB script's writes are applied as one effect set, so one verdict at the seam plus the execute-seam fence (FM-CLUSTER-095) covers what the per-call check covers. |
