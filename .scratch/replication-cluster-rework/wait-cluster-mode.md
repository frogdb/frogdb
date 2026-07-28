# PRD: Wire WAIT in cluster mode — and the replication plane it depends on

Status: draft
Area: Cluster / Replication
Origin: follow-up to issue 37 (`.scratch/testing-improvements/issues/37-wait-in-cluster.md`, done —
the divergence was *pinned*, not fixed). Written 2026-07-28.

Related issues:

- [37 — WAIT has zero coverage in cluster mode](../testing-improvements/issues/37-wait-in-cluster.md) (done; three pinning
  tests added to `integration_cluster.rs`, root cause traced, follow-ups deferred here)
- [34 — Promotion replid asserts](../testing-improvements/issues/34-promotion-replid-asserts.md) (done; documented that a
  runtime-promoted node never establishes a `PrimaryReplicationHandler` and rejects downstream PSYNC)
- [61 — Runtime resync staged-not-installed](../testing-improvements/issues/61-runtime-resync-staged-not-installed.md)
  (open; a full-sync checkpoint is staged but not installed until reboot — a hard prerequisite here)
- [48 — Chained-replication rejection contract](../testing-improvements/issues/48-chained-replication-contract.md) (done;
  pins "FrogDB does not implement transitive replication" — Task 5 would flip it, see risk R4b)
- [23 — Turmoil replication topology](../testing-improvements/issues/23-turmoil-replication-topology.md)
- [11 — Turmoil cluster raft topology](../testing-improvements/issues/11-turmoil-cluster-raft-topology.md)

---

## 1. Summary

`WAIT numreplicas timeout` is inert in cluster mode: a cluster primary always replies `0` in under a
millisecond, and a cluster replica rejects the command. Issue 37 pinned this behavior with tests but
deliberately left it unfixed.

Tracing the cause turns up something larger than a WAIT bug. WAIT's ack machinery hangs off
`PrimaryReplicationHandler`, which is constructed **only** when the static config string
`replication.role == "primary"`
(`frogdb-server/crates/server/src/server/replication_init.rs:57`, `:101`). Cluster bootstrap never
sets that string — cluster roles live in Raft state, entirely separately — so a cluster node keeps the
default `"standalone"` (`frogdb-server/crates/config/src/replication.rs:144`). The *same* handle also
gates PSYNC (`frogdb-server/crates/server/src/connection/dispatch.rs:472-476`). A cluster node in the
default configuration therefore **refuses the very replication stream that `CLUSTER REPLICATE` tells
its replica to open**.

So the honest framing is: **cluster-mode WAIT is unwired because cluster-mode data replication is
unwired.** Wiring WAIT alone would be wiring a counter to a link that does not exist. This PRD covers
both, in dependency order, and specifies the WAIT contract FrogDB should commit to.

**Recommendation (§5): Option A — one replication plane.** Construct the primary-side replication
machinery from the *data-path role* rather than the config string, add the missing Raft
promotion→data-path bridge, and let cluster WAIT be exactly Redis's WAIT: count the direct PSYNC
replicas of the node the client is talking to. Reject the Raft-quorum reinterpretation (Option B) —
Raft carries no key data in FrogDB, so a Raft-commit-based WAIT would be a durability claim about
metadata, not about the write.

---

## 2. Current state

### 2.1 How WAIT is dispatched

WAIT is intercepted by name at the connection layer, in its own dispatch stage, before shard routing:

| Step | Location |
|---|---|
| Dispatch stage | `frogdb-server/crates/server/src/connection/dispatch.rs:495-502` (`DispatchStage::WaitIntercept`) |
| Handler | `frogdb-server/crates/server/src/connection/blocking.rs:215-275` (`handle_wait_command`) |
| Replica rejection (pre-parse, Redis parity) | `frogdb-server/crates/server/src/connection/blocking.rs:217-219`, reading the data-path `is_replica` atomic |
| Arg parsing | `frogdb-server/crates/server/src/commands/replication.rs:parse_wait_args` (validation at `:505-519`) |
| **The gate that breaks cluster mode** | `frogdb-server/crates/server/src/connection/blocking.rs:228-230` |
| Quorum decision | `frogdb-server/crates/replication/src/wait_coordinator.rs:131-157` (`WaitCoordinator::wait_for_replicas`) |
| Offset snapshot | `frogdb-server/crates/replication/src/wait_coordinator.rs:103-105` (`target_offset` = global live offset) |
| Ack counting | `frogdb-server/crates/replication/src/wait_coordinator.rs:112-114` → `frogdb-server/crates/replication/src/tracker.rs:182-190` (`count_acked`, derived from `get_streaming_replicas` at `:131`) |
| GETACK solicitation | `frogdb-server/crates/replication/src/wait_coordinator.rs:145-147` → `PrimaryReplicationHandler::request_acks` |
| CLIENT UNBLOCK race | `frogdb-server/crates/server/src/connection/blocking.rs:256-267` |
| MULTI/EXEC path (deny-blocking) | `frogdb-server/crates/server/src/commands/replication.rs:560-576` (`WaitCommand::execute`) — returns `count_acked(current_offset)`, or `0` when `replication_tracker` is `None` |

The breaking line is verbatim:

```rust
// Standalone: no primary replication handler means no replica can ever
// attach, so the quorum is decided now.
let Some(primary) = self.cluster.primary_replication_handler.clone() else {
    return Response::Integer(0);
};
```
— `frogdb-server/crates/server/src/connection/blocking.rs:226-230`

The comment's premise ("standalone ⇒ no replica can ever attach") is true for standalone and **false
for cluster mode**, where a replica is supposed to attach via `CLUSTER REPLICATE`. The `Option` is
being used as a proxy for "replication is impossible here", and in cluster mode that proxy is wrong.

### 2.2 Why cluster mode never has a `PrimaryReplicationHandler`

`init_replication` branches on the static config string:

- `frogdb-server/crates/server/src/server/replication_init.rs:57` — `if config.replication.is_primary()`
  builds the tracker + `PrimaryReplicationHandler` and stores it at `:101`.
- `:104` — `else if config.replication.is_replica()` builds the replica handler; returns
  `(Arc::new(NoopBroadcaster), None)` at `:201-202`.
- `:204-205` — the standalone fallthrough also returns `(Arc::new(NoopBroadcaster), None)`.

`is_primary()` is `role == "primary"` (`frogdb-server/crates/config/src/replication.rs:299-301`); the
default is `"standalone"` (`:144-146`); and `role` carries `#[param(skip)]` with the comment
"replication role set via REPLICAOF/failover, not CONFIG"
(`frogdb-server/crates/config/src/replication.rs:19-20`), so it is not runtime-settable either.

Cluster bootstrap never touches it. `init_cluster` seeds the data-path flag from static config only —

```rust
let is_replica_flag = Arc::new(std::sync::atomic::AtomicBool::new(
    config.replication.is_replica(),
));
```
— `frogdb-server/crates/server/src/server/cluster_init.rs:79-81`

— and every bootstrap path registers peers and self as `NodeInfo::new_primary(...)` in *Raft* state
(`frogdb-server/crates/server/src/server/cluster_init.rs:340-355`, `:387-395`), which is a different
representation entirely. The test harness likewise starts cluster nodes as `ServerRole::Standalone`
(`frogdb-server/crates/test-harness/src/server.rs:468-472`), and `ClusterTestHarness::add_replica`
issues `CLUSTER REPLICATE` then waits only for *cluster-state* role propagation
(`frogdb-server/crates/test-harness/src/cluster_harness.rs:973-1005`, wait at `:1008-1033`).

Consequences, in order of severity:

1. **PSYNC is refused by every default-config cluster primary.**
   `frogdb-server/crates/server/src/connection/dispatch.rs:472-476` short-circuits with
   `"ERR PSYNC not supported - server is not running as primary"` whenever
   `primary_replication_handler.is_none()`. The replica opened by `CLUSTER REPLICATE` dials the
   primary's *client* port (§2.3) and speaks PSYNC, so it is rejected. Cluster-mode data replication
   does not happen.
2. **WAIT returns 0 instantly** (`blocking.rs:228-230`), and inside MULTI returns 0 because
   `replication_tracker` is `None` (`commands/replication.rs:571-574`).
3. **No ack registry exists**, so nothing could be counted even if the gate were removed.

Issue 37's empirical probe corroborates: on a 2-primary + 1-replica cluster after a `SET`,
`INFO replication` on the owning primary reported `connected_slaves:0` and `master_repl_offset:0`
(`.scratch/testing-improvements/issues/37-wait-in-cluster.md:74-79`).

> **Premise to re-verify before implementation (Task 0).** The "cluster data replication is inert"
> conclusion is a code-path deduction plus issue 37's INFO probe; no test asserts that a
> `CLUSTER REPLICATE`d replica *does* or *does not* hold the primary's keys. The existing cluster
> replication tests are assertion-free documentation tests — `test_replica_receives_writes`
> (`frogdb-server/crates/server/tests/integration_cluster.rs:2589-2639`) and
> `test_promoted_replica_has_all_data` (`:2706-2780`) both only `eprintln!` their findings and accept
> `MOVED` as success. Task 0 turns this into a hard assertion before anything else is built.

### 2.3 How cluster-mode replication is *supposed* to work

Raft is a metadata plane only. The `ClusterCommand` vocabulary
(`frogdb-server/crates/cluster/src/types.rs:288-376`) has no variant carrying a key or a value; the
state machine (`ClusterStateMachine`, `frogdb-server/crates/cluster/src/state.rs:201-209`, `apply` at
`:286-363`) stores membership, slot assignment, config epoch and migrations
(`frogdb-server/crates/cluster/src/state.rs:31-48`). This is ADR-0001, documented at
`website/src/content/docs/architecture/clustering.md:26-30` ("The data path never goes through Raft")
and `:107-116` (the "What Raft Owns (Metadata Only)" table).

Data replication is intended to be a **per-node PSYNC link**, identical in mechanism to standalone
replication — `website/src/content/docs/architecture/clustering.md:363-372` lists
"Replication | Replica → Primary | PSYNC checkpoint + command stream". The attach flow:

1. `CLUSTER REPLICATE <id>` → `frogdb-server/crates/server/src/commands/cluster/admin.rs:352-384`,
   which mutates nothing and returns `RaftClusterOp::SetRole { is_replica: true, primary_id }`.
2. Proposed through the Raft writer
   (`frogdb-server/crates/server/src/connection/cluster.rs:34-115`).
3. Applied: `frogdb-server/crates/cluster/src/commands.rs:114-154`; role set at `:138-141`; a
   `ClusterEvent::NodeDemoted` is emitted **only for the Replica direction** at `:146-152`.
4. Self-filtered in `apply` (`frogdb-server/crates/cluster/src/state.rs:314-330`) and pushed to
   `demotion_tx`.
5. `DemotionConsumer::handle` (`frogdb-server/crates/server/src/server/cluster_init.rs:744-774`)
   resolves the new primary's **client address** from the cluster snapshot and calls
   `role_controller.request_demote(addr)` at `:763`.
6. `RoleManager::demote` (`frogdb-server/crates/server/src/role_manager.rs:181-199`) fences
   `is_replica.store(true, Release)` at `:187` and starts a real stream via `RealReplicaStreamer`
   (`frogdb-server/crates/server/src/role_manager.rs:282-348`, constructed at
   `frogdb-server/crates/server/src/server/cluster_init.rs:96-105`).
7. …which performs PSYNC against the primary's client port and is refused at `dispatch.rs:472-476`.

Note there is **one link per node, not per shard**: the streamer is handed `shard_senders` and
`num_shards` (`cluster_init.rs:99-100`) and the frame consumer fans out across shards
(`frogdb-server/crates/server/src/server/subsystems.rs:336-345`). So "the replica set of a shard" and
"the replica set of a node" are the same set in FrogDB — a simplification that makes Redis's
per-shard WAIT contract map cleanly (§4.1).

### 2.4 The two role systems, and the missing promotion bridge

| | Cluster-state role | Data-path role |
|---|---|---|
| Representation | `NodeRole::Primary`/`Replica` in Raft (`frogdb-server/crates/cluster/src/types.rs:36-43`) | `Arc<AtomicBool> is_replica` (`frogdb-server/crates/server/src/server/cluster_init.rs:79-81`) |
| Sole writer | Raft apply (`frogdb-server/crates/cluster/src/commands.rs:138-141`, `:196-200`, `:216-218`) | `RoleManager` (`frogdb-server/crates/server/src/role_manager.rs:163-199`) |
| Read by | `CLUSTER NODES`/`SLOTS`/`SHARDS` (`frogdb-server/crates/cluster/src/wire.rs:41-51`, `:93-96`), `CLUSTER FAILOVER` (`frogdb-server/crates/server/src/commands/cluster/admin.rs:280`) | write guard (`frogdb-server/crates/server/src/connection/guards.rs:286`), WAIT (`blocking.rs:217`), PSYNC handoff, `INFO`/`ROLE` (`frogdb-server/crates/server/src/server/subsystems.rs:58-67`), shard workers (`frogdb-server/crates/core/src/shard/types.rs:33`) |

The only bridge from cluster state to data path is **demotion**. There is no promotion bridge:

- `ClusterEvent` has no promoted variant (`frogdb-server/crates/cluster/src/types.rs:415-436`).
- `Failover` apply silently sets `new_node.role = NodeRole::Primary`
  (`frogdb-server/crates/cluster/src/commands.rs:196-200`) and emits an event only for the *graceful*
  demotion of the old primary (`:249-256`).
- `SetRole` emits only on the Replica direction (`frogdb-server/crates/cluster/src/commands.rs:146-152`).
- The only production caller of `request_promote` (`frogdb-server/crates/server/src/role_manager.rs:253`)
  is `REPLICAOF NO ONE` (`frogdb-server/crates/server/src/commands/replication.rs:81`).
- `RoleManager`'s own module doc claims otherwise — "Raft decides the topology change and emits a
  demotion/promotion event" (`frogdb-server/crates/server/src/role_manager.rs:19-27`). The promotion
  half is aspirational.

This is the "staged-not-installed" divergence pinned by issue 37 §"Pinned semantics" item 5 and by
`frogdb-server/crates/server/tests/integration_cluster.rs:11309-11315`: a node promoted in cluster
state keeps `is_replica == true`, keeps answering `-READONLY`
(`frogdb-server/crates/server/src/connection/guards.rs:286`), and keeps rejecting WAIT.

**A second, related hole:** `install_snapshot`
(`frogdb-server/crates/cluster/src/state.rs:380-404`) replaces the whole state map and emits **no**
events. A node that learns it is a replica by receiving a Raft snapshot (rather than by applying the
`SetRole` entry) never demotes its data path at all. Same class of bug, different trigger.

### 2.5 Offsets in cluster mode today

There is no per-replica ack registry in cluster mode. What exists:

- A single shared offset atomic minted unconditionally in cluster mode
  (`frogdb-server/crates/server/src/server/cluster_init.rs:88-95`), published by the cluster bus
  `HealthProbe` (`frogdb-server/crates/server/src/cluster_bus.rs:232-234`, response type at
  `frogdb-server/crates/cluster/src/network.rs:161-164`).
- Its sole consumer is failover replica scoring
  (`frogdb-server/crates/server/src/failure_detector.rs:353-372`, `compute_replica_score` at `:486-497`).
  It is transient and never written into Raft state — `NodeInfo`
  (`frogdb-server/crates/cluster/src/types.rs:55-79`) has no offset field.
- `CLUSTER SHARDS`/`SLOTS` report `local_replication_offset`
  (`frogdb-server/crates/server/src/commands/cluster/mod.rs:64-77`), which returns the **Raft
  last-applied log index** when raft is present and only falls back to the replication tracker
  otherwise; every non-local node in the shard is reported as `replication-offset 0`
  (`frogdb-server/crates/server/src/commands/cluster/mod.rs:486-489`). Two of the pinning tests here
  (`integration_cluster.rs:6072`, `:6144`) assert only *non-zero*, which the Raft index satisfies.
- `REPLCONF ACK` / `GETACK` exist but are reachable only inside a PSYNC session
  (`frogdb-server/crates/server/src/commands/replication.rs:184-287`); no cluster-bus ack RPC exists
  (`frogdb-server/crates/cluster/src/network.rs:96-117` is the exhaustive RPC set).

### 2.6 What issue 37 pinned (the tests this PRD will have to rewrite)

`frogdb-server/crates/server/tests/integration_cluster.rs`:

- `test_wait_in_cluster_returns_zero_immediately` (`:11148`) — asserts `WAIT {0,1,2}` each return `0`
  **well under** their own timeouts.
- `test_wait_rejected_on_cluster_replica` (`:11215`).
- `test_wait_in_cluster_does_not_hang_across_failover` (`:11250-11359`) — hard-asserts that a
  cluster-promoted node *still* answers the replica rejection, precisely so the test trips when
  promotion starts installing the data-path role.

That third test is a deliberate tripwire for exactly this work. Expect it to fail at Task 2 and be
rewritten to the new contract, not deleted.

---

## 3. Reference behavior

### 3.1 Redis

`waitCommand` (`redis/src/replication.c`, unstable):

```c
long long offset = c->woff;
if (server.masterhost) { addReplyError(c,"WAIT cannot be used with replica instances. ..."); return; }
...
ackreplicas = replicationCountAcksByOffset(c->woff);
if (ackreplicas >= numreplicas || c->flags & CLIENT_DENY_BLOCKING) { addReplyLongLong(c,ackreplicas); return; }
blockForReplication(c,timeout,offset,numreplicas);
replicationRequestAckFromSlaves();
```

- **What it counts:** `replicationCountAcksByOffset` walks `server.slaves` — replicas *directly
  attached to this node* — counting those with `replstate == SLAVE_STATE_ONLINE` and
  `repl_ack_off >= offset`. Replicas mid-full-sync are never counted. Sub-replicas are not counted.
- **`c->woff`** is a per-connection watermark stamped at the end of `call()` only when that command
  actually propagated (`redis/src/server.c:4258-4260`). The offset is global; the *choice* of offset
  is per-connection.
- **`numreplicas` > attached replicas:** no error, no early return. Redis blocks to `timeout` and then
  replies with the count (`redis/src/blocked.c:253`). `timeout 0` blocks forever.
- **GETACK:** `replicationRequestAckFromSlaves()` only sets a flag; `beforeSleep()` emits a single
  `REPLCONF GETACK *` per event-loop iteration, batching all waiters. GETACK is itself part of the
  stream and advances `master_repl_offset`.
- **In MULTI / scripts:** `CLIENT_DENY_BLOCKING` → return the count immediately.

**Cluster mode has no WAIT-specific code path at all.** Consequences:

- WAIT is keyless, so `getNodeByQuery()` never redirects it — **WAIT never produces `-MOVED`/`-ASK`.**
  It runs on whichever node the connection is attached to and counts only that primary's direct
  replicas. There is no server-side cluster-wide WAIT.
- Cluster-wide WAIT is a **client-side fan-out**, declared via command tips in
  `redis/src/commands/wait.json`: `"REQUEST_POLICY:ALL_SHARDS"`, `"RESPONSE_POLICY:AGG_MIN"` — "the
  aggregate reply from a cluster-wide `WAIT` command should be the minimal value (number of
  synchronized replicas) from all shards."
- A cluster replica always has `server.masterhost` set → WAIT errors, `READONLY` mode notwithstanding.
- A blocked WAIT is **not** interrupted by topology changes: `clusterRedirectBlockedClientIfNeeded`
  handles only LIST/ZSET/STREAM/MODULE block types, never `BLOCKED_WAIT`. No `CLUSTERDOWN`, no
  `MOVED`, while parked in WAIT.
- **Demotion mid-WAIT does fire:** `replicationSetMaster()` calls `disconnectAllBlockedClients()`,
  replying `-UNBLOCKED force unblock from blocking operation, instance state changed (master ->
  replica?)` and closing the connection.
- **Documented durability caveat** (redis.io/docs/latest/commands/wait/): "`WAIT` does not make Redis
  a strongly consistent store… in the context of Sentinel or Redis Cluster failover, `WAIT` improves
  the real world data safety… **However this is just a best-effort attempt so it is possible to still
  lose a write synchronously replicated to multiple replicas.**"

`WAITAOF numlocal numreplicas timeout` (7.2+) is the fsync-durability sibling; same `c->woff`, counts
`repl_aof_off`, same `ALL_SHARDS`/`AGG_MIN` tips, same replica rejection. Out of scope here, but the
design should not preclude it.

### 3.2 Valkey

Structurally identical (`valkey/src/replication.c`, same tips, same replica rejection). Divergences
worth stealing or avoiding:

1. **Input validation:** `getRangeLongFromObjectOrReply(c, argv[1], 0, INT_MAX, ...)` — negative
   `numreplicas` is a hard error. Redis silently accepts it. (FrogDB already rejects negatives —
   `frogdb-server/crates/server/src/commands/replication.rs:parse_wait_args`.)
2. **Script-aware offset:** `getClientWriteOffset()` substitutes the *calling* client's `woff` when a
   script is running.
3. **Block-type consolidation** (valkey PR #562) merged `BLOCKED_WAIT`, `BLOCKED_WAITAOF` and
   `BLOCKED_WAIT_PREREPL` — the last being `CLUSTER SETSLOT`.
4. **Most design-relevant:** Valkey reuses the WAIT ack machinery to make *control-plane* transitions
   synchronous. `cluster_legacy.c:7895-7909` blocks `CLUSTER SETSLOT` on all eligible replicas at
   `primary_repl_offset + 1` before applying it. That is Valkey's answer to "slot ownership must
   survive promotion" — a targeted synchronous replication of one metadata op, not per-write
   consensus. FrogDB gets this property from Raft already, for free.
5. Dual-channel replication (Valkey 8) does not change WAIT semantics: a replica in `wait_bgsave` is
   not `ONLINE` and so is not countable.
6. Atomic slot migration (Valkey 9, `cluster_migrateslots.c`) does *not* register the migration target
   in `server.replicas`, so a migration target is never counted by WAIT on the source primary. Worth
   mirroring: a FrogDB slot-migration target must not inflate the WAIT count.

### 3.3 Dragonfly — the closest architectural analogue

Dragonfly is thread-per-shard with a per-shard journal LSN and no global replication offset, exactly
FrogDB's shape. It implemented WAIT only recently (issue #7369 → PR #7454, merged 2026-07-10;
`ServerFamily::Wait`, `src/server/server_family.cc:3608`, registered
`CI{"WAIT", CO::NOSCRIPT | CO::BLOCKING, 3, 0, 0, acl::kWait}` — arity 3, no keys). Its stated design
decisions map onto every question this PRD has to answer:

- **Global LSN snapshot, not per-connection `woff`.** Dragonfly deliberately waits for *every* write
  up to now, from any connection: "journal writes happen post-commit on the shard's own fiber,
  decoupled from the issuing connection… tracking a precise per-connection LSN would mean extra
  bookkeeping on every write's hot path for comparatively little benefit." This is a strictly
  stronger guarantee than Redis's. **FrogDB already made the same call** and documented it at
  `frogdb-server/crates/replication/src/wait_coordinator.rs:96-105`.
- **Per-shard target, all-shards rule:** `target_lsns[sid]` captured per shard; a replica counts only
  if `last_acked_lsn >= target_lsns[sid]` for **every** shard.
- **`timeout 0` is capped at 10 minutes**, not infinite, because the poll loop is not wired into the
  transaction cancellation machinery.
- **Early exit when unreachable:** if every still-running tracked replica has acked and the count is
  still short of `numreplicas`, return immediately rather than burning the timeout. (Redis blocks.)
- **Role change is checked per-shard, at the site that touches that shard's journal**, because
  `is_master` is per-thread and the fan-out has no cross-thread ordering guarantee. On observing a
  role change it returns the replica-instance error, since "`target_lsns` may span a stale journal
  epoch from before the transition."
- **Cluster mode:** Dragonfly ships a data plane only ("it does NOT provide a control plane"), so WAIT
  remains per-node, counting that node's replica flows. No cluster-wide aggregation server-side.

### 3.4 Consensus control plane + async data plane

- **Redis Enterprise:** replicated Cluster Configuration Store for the control plane, classic async
  per-shard replication for data. WAIT is *not* replaced by consensus — it remains the per-write
  durability knob ("the application only gets the acknowledgment from the write after durability is
  achieved with replication to the replica for `WAIT`"). Cross-instance (Active-Active) replication
  does not support WAIT.
- **RedisRaft** (archived) put the *data* through Raft, making every write effectively
  `WAIT majority` with a guarantee. The cost was giving up Redis Cluster routing, multi-DB, WATCH and
  streams. FrogDB explicitly chose the opposite side of this trade (ADR-0001).

**The consistent answer across every reference implementation: WAIT is a data-plane, per-node,
best-effort primitive. No system reinterprets it against its consensus layer.**

---

## 4. Design options

### 4.0 Contract questions any option must answer

| # | Question | Redis | Dragonfly | Proposed FrogDB |
|---|---|---|---|---|
| Q1 | Which replicas count? | direct replicas of the contacted node | same | same (node == shard replica set, §2.3) |
| Q2 | Which offset? | per-connection `woff` | global, all shards | global (already: `wait_coordinator.rs:103-105`) |
| Q3 | Redirect in cluster? | never (keyless) | never | never |
| Q4 | Cluster-wide? | client-side fan-out via tips | n/a | client-side; document, no server-side fan-out |
| Q5 | On a replica? | error | error | error (already: `blocking.rs:217-219`) |
| Q6 | `numreplicas` unreachable? | block to timeout | early-return | block to timeout (Redis parity) |
| Q7 | `timeout 0`? | block forever | 10-min cap | block forever + CLIENT UNBLOCK (already) |
| Q8 | Demotion mid-WAIT? | `-UNBLOCKED`, close conn | replica error | `-UNBLOCKED` (Redis parity) |
| Q9 | Slot-migration target counts? | no | n/a | no |

### 4.1 Option A — One replication plane (recommended)

Make the primary-side replication machinery a function of the **data-path role**, not of the config
string, and complete the Raft→data-path role bridge. Cluster WAIT then needs no new concept: it is
standalone WAIT, working, because a real PSYNC replica is attached.

Changes:

1. **Construct `PrimaryReplicationHandler` + `ReplicationTrackerImpl` whenever the node can serve as a
   primary** — i.e. `is_primary() || is_standalone() || cluster.enabled` — rather than only on
   `role == "primary"` (`replication_init.rs:57`). Constructing it is cheap and passive: nothing is
   broadcast until a replica attaches. (Alternative, more surgical: keep the branch but add
   `|| config.cluster.enabled`. Cleaner still: install/uninstall the handler from
   `RoleManager::promote`/`demote` so it tracks the live role. See §6 Task 2 for the sequencing.)
2. **Gate PSYNC on the data-path role, not on handler presence.** `dispatch.rs:472-476` becomes "a
   *replica* cannot serve PSYNC" (`is_replica.load()`), which is the real invariant; the handler is
   then guaranteed present.
3. **Add the promotion bridge.** New `ClusterEvent::NodePromoted` emitted by `SetRole { Primary }`
   (`cluster/src/commands.rs:146-152`) and by `Failover`'s successor promotion (`:196-200`);
   self-filtered in `state.rs:314-330`; a `PromotionConsumer` (sibling of `DemotionConsumer`,
   `cluster_init.rs:733-774`) calls `request_promote()`. Also reconcile role on `install_snapshot`
   (`state.rs:380-404`) so a snapshot-installed role change is not silently dropped.
4. **Delete the standalone early-return** at `blocking.rs:228-230`; with (1) the handler is always
   present when the node is a primary, and the "no replica can ever attach" shortcut is no longer
   needed (a replica-less primary already returns `count_acked == 0` via the fast path at
   `wait_coordinator.rs:140-143`). Note this changes standalone behavior: `WAIT 1 5000` on a
   replica-less standalone node would now block for 5s instead of returning instantly. That is Redis
   parity, but it is a **behavior change to an already-pinned contract** — see risk R4.
5. **Unblock parked WAITs on demotion** (Q8): `RoleManager::demote` signals blocked WAIT clients with
   `-UNBLOCKED ... instance state changed (master -> replica?)`.

Pros: no new protocol, no new offset plane; WAIT/`INFO replication`/`ROLE`/`CLUSTER SHARDS` all become
consistent by construction; fixes cluster replication itself, which is a far larger correctness win
than WAIT; matches Redis and Dragonfly exactly; the per-node link means "shard-local replica set" and
"my direct replicas" are the same set, so Redis's per-shard contract holds without per-shard
bookkeeping.

Cons: touches the boot-time wiring seam that several subsystems read; the promotion bridge is gated on
issue 61 (a promoted node's staged checkpoint is not installed until reboot); changes standalone WAIT
behavior for the unreachable-quorum case.

### 4.2 Option B — Raft-quorum WAIT

Reinterpret `WAIT numreplicas timeout` in cluster mode as "block until this write is committed by
`numreplicas` Raft voters", using `raft.metrics().last_applied`.

**Reject.** Raft in FrogDB carries no key data (`cluster/src/types.rs:288-376` — no variant has a key;
`clustering.md:107-116`). A Raft-commit-based WAIT would report a durability property of *metadata*
while the client's write lives entirely on the data plane. It would return a number that looks like
Redis's and means something unrelated — an observability field that is confidently wrong, which is
worse than one that is absent. It is also already half-shipped in a misleading way: `CLUSTER SHARDS`
reports the Raft last-applied index as `replication-offset` (`commands/cluster/mod.rs:64-77`) — that
should arguably be fixed, not extended.

The only version of Option B that is honest is RedisRaft's: put writes through Raft. That is a
different product and contradicts ADR-0001.

### 4.3 Option C — Cluster-bus ack plane

Leave PSYNC alone; add a `BusRpc::ReplicaAck`/`AckProbe` to the cluster bus
(`cluster/src/network.rs:96-117`) and have WAIT poll each replica of the contacted node for its
applied offset, reusing the `HealthProbe` shape already used by failover scoring
(`failure_detector.rs:353-372`).

Pros: no change to the boot wiring; works even while the PSYNC gate stays broken; naturally per-shard
if extended to a per-shard LSN vector; the polling shape mirrors Dragonfly's 100 ms loop.

Cons: builds a *second* ack path with different liveness properties from `REPLCONF ACK`, so WAIT and
`INFO replication` can disagree — exactly the divergence `wait_coordinator.rs:107-114` was written to
eliminate ("WAIT and ROLE can never disagree about which replicas exist"). Polling adds latency Redis
avoids with GETACK. And crucially, if PSYNC is refused, the replica has no data — WAIT would report
acks for a stream that carries nothing. **Option C makes WAIT return a plausible number over a broken
data path**, which is worse than returning 0.

Viable only as a *supplement* to A, if per-shard granularity is ever needed (e.g. if FrogDB moves to
per-shard replication links).

### 4.4 Option D — Honest failure (interim)

Return an explicit error in cluster mode — `ERR WAIT is not supported in cluster mode` — instead of a
silent `0`.

Pros: one-line change; removes the trap where a client reads `0` as "timed out, 0 replicas caught up"
when the truth is "not implemented"; cheap to ship immediately.

Cons: diverges from Redis (WAIT is always valid on a primary); breaks clients that call WAIT
defensively; solves nothing.

Recommended **only** if Option A slips — as a stopgap that keeps users from trusting a meaningless
answer, replaced by A. Note the current `0` is arguably the more dangerous reply: a client asking
`WAIT 2 1000` and receiving `0` will conclude its replicas are lagging, not that the feature is inert.

---

## 5. Recommendation

**Adopt Option A**, sequenced so that WAIT is the *last* thing wired, not the first.

Rationale:

1. **The bug is not in WAIT.** WAIT is the symptom of a replication plane that is never constructed in
   cluster mode. Fixing WAIT alone (Option C) would produce a number about a link that carries no
   data. Fixing the plane makes WAIT correct as a side effect and fixes cluster replication, failover
   data integrity, `INFO replication`, and PSYNC-to-sub-replica (issue 34) at the same time.
2. **It is what every reference implementation does.** Redis, Valkey and Dragonfly all treat WAIT as a
   per-node data-plane primitive with no cluster-specific path. FrogDB's per-node (not per-shard)
   replication link means the Redis contract maps without adaptation.
3. **FrogDB's existing WAIT machinery is already correct** — `WaitCoordinator`
   (`wait_coordinator.rs:80-158`) already does snapshot → fast path → GETACK solicitation →
   quorum-or-deadline, already documents its one deliberate divergence (global offset instead of
   `c->woff`, `:96-105`) and that divergence is the same one Dragonfly chose for the same
   thread-per-shard reason. Nothing about the quorum logic needs to change.
4. **Cluster-wide WAIT stays a client concern.** Do not build server-side fan-out. Document the
   `ALL_SHARDS`/`AGG_MIN` contract; if FrogDB later grows a `COMMAND DOCS` tips surface (none exists
   today — no `request_policy`/`command_tips` anywhere in the tree), WAIT is a natural first entry.

### 5.1 Risk assessment

| # | Risk | Severity | Mitigation |
|---|---|---|---|
| R1 | **Issue 61 blocks promotion.** A promoted node's full-sync checkpoint is staged, not installed until reboot, so a "promoted primary" may serve stale data — and WAIT would be counting acks against it. | **High — hard prerequisite** | Land issue 61 before Task 3 (the promotion bridge). Until then a promoted node must keep failing loudly, not start silently answering WAIT. |
| R2 | Constructing `PrimaryReplicationHandler` for every cluster node changes boot-time wiring read by shards, INFO, the split-brain logger (`cluster_init.rs:536-549`) and the quorum checker. | Medium | Task 2 is wiring-only with no behavior change beyond handler presence; land it separately with the full suite green before Task 4. |
| R3 | **Enabling replication turns on the self-fence / `min-replicas-to-write` machinery** for cluster nodes that previously had `replication_quorum_checker == None` (`replication_init.rs`, consumed at `guards.rs:290+`). A cluster primary could start refusing writes. | Medium | Keep the quorum checker gated on its own config flags (`self_fence_on_replica_loss`), never on handler presence. Add an explicit test that a cluster primary with default config still accepts writes with zero replicas. |
| R4 | **Standalone-role WAIT behavior changes** if the `blocking.rs:228-230` early return is removed: `WAIT 1 5000` on a `role == "standalone"` node would block 5 s instead of returning `0`. Verified blast radius: **no existing test exercises WAIT on a standalone-role node** — every WAIT test in `integration_replication.rs` uses `start_primary*` (`:156`, `:481`, `:649`, `:724`, `:1074`, `:2933`, …), so this is a pure behavior change with no test churn. | Medium | Redis parity says block. Decide explicitly (§6.1 Q1), update the doc comment at `blocking.rs:211-214`, add a test either way, and call it out in `CHANGELOG.md`. |
| R4b | **Task 5 breaks two deliberately pinned divergences.** Installing the primary handler on promotion makes a promoted node able to serve PSYNC, which trips `test_promoted_node_via_replicaof_no_one_rejects_downstream_psync` (`integration_replication.rs:1742-1760`) and — because the same gate is what blocks chaining — `test_chained_replication_rejected_sub_replica_never_receives_data` (`:1820-1850`, the pinned contract from issue 48). | Medium | These are the *intended* outcomes (issue 34 names the fix as a "candidate follow-up"), but issue 48's no-chained-replication contract is documented in `operations/replication.md` and must be re-decided, not silently flipped. Either scope Task 5 to cluster-driven promotion only, or reopen issue 48. |
| R5 | Split-brain window widens: a real replication stream in cluster mode means a partitioned old primary can now accumulate divergent writes in the backlog that the split-brain logger will surface. | Medium | This is the *intended* behavior the logger (`cluster_init.rs:639-724`) was built for; verify the log fires in the failover test. |
| R6 | WAIT counts a slot-migration target as a replica (Q9). | Low | Migration uses `MIGRATE`, not PSYNC (`clustering.md:363-372`), so targets never enter `get_streaming_replicas`. Assert it. |
| R7 | Snapshot-installed role changes still drop on the floor (`state.rs:380-404`). | Medium | Reconcile role after `install_snapshot` in Task 3; test with a node that joins late enough to be snapshot-caught-up. |
| R8 | Failover scoring reads the shared offset atomic (`failure_detector.rs:353-372`), which today is ~always 0 in cluster mode; once real offsets flow, replica selection changes behavior for the first time. | Low–Medium | Good change, but it makes `test_auto_failover_selects_most_caught_up_replica` (`integration_cluster.rs:7512`) meaningful for the first time — expect it to start actually discriminating. |
| R9 | Performance: every cluster primary now maintains a replication backlog and offset coordinator. | Low | Already the standalone-primary cost; measure with `just benchmark` before/after. |

### 5.2 Testing plan

**Unit** (`frogdb-server/crates/replication/`, `frogdb-server/crates/cluster/`)

- `WaitCoordinator` already has mock-solicitor tests (`wait_coordinator.rs:160-190+`); add cases for
  "replica attaches mid-wait" and "replica detaches mid-wait".
- `cluster/src/commands.rs`: `SetRole { Primary }` and `Failover` emit `NodePromoted`; the graceful
  path emits both demotion and promotion; a replayed/idempotent apply does not double-emit.
- `state.rs`: self-filter routes promotion to the right channel; `install_snapshot` reconciles role.
- `role_manager.rs`: `promote()` installs the primary handler; `demote()` tears it down and unblocks
  WAIT waiters (extend the existing fake-streamer tests).

**Integration** (`frogdb-server/crates/server/tests/integration_cluster.rs`)

- **Task 0 (do first):** `test_cluster_replica_receives_writes_asserted` — 2-primary cluster,
  `add_replica`, write to the owning shard, assert the replica actually holds the key (via `READONLY`
  + `GET`, or `DEBUG`-level introspection), and assert `INFO replication` on the primary reports
  `connected_slaves:1`. **This test must fail on `main`.** It is the premise check for the whole PRD.
- Rewrite the three issue-37 tests to the new contract:
  - `test_wait_in_cluster_returns_zero_immediately` → `test_wait_in_cluster_counts_shard_replicas`:
    `WAIT 1 2000` returns `1` after a write to the owning shard; `WAIT 0 100` returns the live count
    immediately.
  - `test_wait_rejected_on_cluster_replica` (`:11215`) — unchanged, must still pass.
  - `test_wait_in_cluster_does_not_hang_across_failover` (`:11250`) — rewrite: after promotion the new
    primary must *accept* WAIT (this is the tripwire the test was designed to be).
- `test_wait_numreplicas_exceeds_actual_blocks_to_timeout` — `WAIT 3 500` with 1 replica returns `1`
  after ≥500 ms (Q6, Redis parity; explicitly *not* Dragonfly's early exit).
- `test_wait_does_not_count_other_shards_replicas` — 2 shards each with a replica; WAIT on shard A's
  primary returns 1, not 2 (Q1).
- `test_wait_never_redirects_in_cluster` — WAIT on a node owning no slots returns an integer, never
  `MOVED` (Q3).
- `test_wait_unblocked_on_demotion` — park a `WAIT 5 0`, demote the node via `CLUSTER REPLICATE`,
  assert `-UNBLOCKED` (Q8).
- `test_wait_ignores_slot_migration_target` (Q9).
- `test_cluster_primary_writes_without_replicas` — R3 guard.
- `test_cluster_shards_reports_real_replication_offset` — replace the Raft-index proxy
  (`commands/cluster/mod.rs:64-77`) and strengthen `integration_cluster.rs:6072`/`:6144` from
  "non-zero" to "matches the primary's `master_repl_offset`".

**Turmoil** (`frogdb-server/crates/server/tests/simulation.rs`; see issues 11 and 23)

Standalone replication already drives WAIT under turmoil (`simulation.rs:4481`, `:4545`, `:4716`,
`:4787`, `:5961`, `:5985`, `:6145` — all `WAIT 1 <ms>` against a primary), so these scenarios are
extensions of an existing pattern, not new machinery:

- Deterministic partition between a cluster primary and its replica: WAIT must return the reduced
  count at the deadline, never hang past it, and recover after heal.
- Partition + failover: a WAIT parked across a promotion resolves to a documented answer under
  deterministic scheduling (this is where the `-UNBLOCKED` contract earns its keep).

**Jepsen** (`testing/jepsen/frogdb/src/jepsen/frogdb/`)

- `replication.clj` already models `:write-sync` (SET + WAIT for replica ACK) vs async `:write`
  (`replication.clj:7-13`, `:63`) — currently exercised only against standalone. Extend
  `replication_failover.clj` / `cluster_db.clj` so the sync-write workload runs against a **cluster**
  topology: a `:write-sync` that returned `≥1` must survive a kill-and-promote of its primary strictly
  more often than an async `:write`. This is the only test that verifies WAIT means anything.
- `lag.clj` should observe non-zero, monotone cluster replication lag once real offsets flow.
- Carry Redis's caveat into the checker's docstring: WAIT is best-effort even at `≥1` — failover
  election ranks by offset (`failure_detector.rs:486-497`) but does not *require* the acked replica to
  win, so the checker must assert a probability/ordering property, not an absolute guarantee.

**Docs**

- `website/src/content/docs/architecture/clustering.md` — the "Replication | Replica → Primary | PSYNC"
  row (`:363-372`) currently describes an intended design as if it were live. Reconcile.
- Add a WAIT semantics section stating: per-node/per-shard counting, no redirect, replica rejection,
  cluster-wide = client-side `ALL_SHARDS`/`AGG_MIN`, the global-offset divergence
  (`wait_coordinator.rs:96-105`), and the best-effort caveat.

---

## 6. Task breakdown

Ordered; each task is independently landable with the suite green.

| # | Task | Scope | Files |
|---|---|---|---|
| **0** | **Verify the premise.** Add the failing assertion test that a `CLUSTER REPLICATE`d replica does/does not receive writes, plus `connected_slaves` assertion. Do not fix anything. If it passes on `main`, this PRD's §2.2 conclusion is wrong and everything below must be re-derived. | S (~80 lines, 1 file) | `frogdb-server/crates/server/tests/integration_cluster.rs`; possibly `frogdb-server/crates/test-harness/src/cluster_harness.rs` (a `wait_for_replication_link` helper next to `wait_for_role_propagation:1008`) |
| **1** | **Land issue 61** (staged-not-installed full-sync checkpoint). Hard prerequisite for Task 3. | L — see issue 61 | `frogdb-server/crates/replication/src/replica/connection.rs` (~L263-296) and the checkpoint install path |
| **2** | **Decouple primary replication wiring from the config-role string.** Construct the tracker + `PrimaryReplicationHandler` when the node can serve as a primary (incl. `cluster.enabled`). Keep the quorum checker on its own flags (R3). No behavior change beyond handler presence. | M (~150 lines, 3-4 files) | `frogdb-server/crates/server/src/server/replication_init.rs:41-227`; `frogdb-server/crates/server/src/server/cluster_init.rs:79-105`; `frogdb-server/crates/server/src/server/mod.rs:178-398`; `frogdb-server/crates/server/src/server/subsystems.rs:303, 433, 644` |
| **3** | **Gate PSYNC on the data-path role** instead of handler presence. Cluster replication starts working here — expect Task 0's test to flip green. | S (~30 lines, 1-2 files) | `frogdb-server/crates/server/src/connection/dispatch.rs:467-487`; `frogdb-server/crates/server/src/connection/deps.rs:103-148` |
| **4** | **Promotion bridge.** `ClusterEvent::NodePromoted`; emit from `SetRole { Primary }` and `Failover`; self-filter in `apply`; `PromotionConsumer` calling `request_promote()`; reconcile role after `install_snapshot` (R7). Fix the now-true module doc at `role_manager.rs:19-27`. | M (~250 lines, 4 files) | `frogdb-server/crates/cluster/src/types.rs:415-436`; `frogdb-server/crates/cluster/src/commands.rs:114-258`; `frogdb-server/crates/cluster/src/state.rs:236-244, 286-363, 380-404`; `frogdb-server/crates/server/src/server/cluster_init.rs:529-560, 733-774` |
| **5** | **Install/uninstall the primary handler on role transition** so a promoted node can serve PSYNC to sub-replicas (closes issue 34's residue) and a demoted node stops. **Requires re-deciding issue 48's pinned no-chained-replication contract first (R4b)** — this task is what unblocks chaining. | M (~150 lines, 2 files) | `frogdb-server/crates/server/src/role_manager.rs:163-199, 252-268`; `frogdb-server/crates/server/src/server/subsystems.rs`; pinning tests at `frogdb-server/crates/server/tests/integration_replication.rs:1742-1850` |
| **6** | **Wire WAIT.** Remove/narrow the early return at `blocking.rs:228-230`; update its doc comment (`:211-214`) and `WaitCommand`'s (`commands/replication.rs:531-537`); decide R4 and update the standalone pinning tests. | S (~40 lines, 2 files) | `frogdb-server/crates/server/src/connection/blocking.rs:194-275`; `frogdb-server/crates/server/src/commands/replication.rs:522-577` |
| **7** | **Unblock parked WAITs on demotion** with `-UNBLOCKED ... instance state changed (master -> replica?)` (Q8). | S–M (~100 lines, 2 files) | `frogdb-server/crates/server/src/role_manager.rs:181-199`; `frogdb-server/crates/server/src/connection/blocking.rs:243-272`; client registry |
| **8** | **Observability reconciliation.** `CLUSTER SHARDS`/`SLOTS` should report the real replication offset, not the Raft last-applied index; `INFO replication` `connected_slaves`/`master_repl_offset` should be live in cluster mode. | M (~120 lines, 2 files) | `frogdb-server/crates/server/src/commands/cluster/mod.rs:63-77, 459-502`; `frogdb-server/crates/server/src/server/subsystems.rs:58-67` |
| **9** | **Integration tests** per §5.2 (rewrite the three issue-37 tests; add the seven new ones). | M–L (~500 lines, 1 file) | `frogdb-server/crates/server/tests/integration_cluster.rs:11091-11359` + new |
| **10** | **Turmoil scenarios** (partition, partition+failover). | M (~250 lines) | `frogdb-server/crates/server/tests/simulation.rs` |
| **11** | **Jepsen cluster sync-write workload** + lag observation. | M–L | `testing/jepsen/frogdb/src/jepsen/frogdb/replication.clj`, `replication_failover.clj`, `cluster_db.clj`, `lag.clj`, `core.clj` |
| **12** | **Docs**: clustering replication reality-check + WAIT semantics section; `CHANGELOG.md` for the R4 standalone behavior change. | S | `website/src/content/docs/architecture/clustering.md:20-30, 100-120, 358-375`; `website/src/content/docs/architecture/replication.md`; `CHANGELOG.md` |
| **13** | **Fix the contradictory role views** surfaced by issue 37: a cluster replica's own `CLUSTER NODES` can report `myself … master` while `is_replica == true`. After Task 4 the two views converge for role *changes*; audit the boot path (`cluster_init.rs:79-81, 387-395`) for the remaining boot-time divergence. | S–M | `frogdb-server/crates/cluster/src/wire.rs:41-69`; `frogdb-server/crates/server/src/server/cluster_init.rs:79-81, 387-395` |

**Suggested landing waves:** [0] → [1] → [2, 3] → [4, 5] → [6, 7, 8] → [9] → [10, 11] → [12, 13].
Tasks 0 and 9 bracket the work: 0 proves the problem exists, 9 proves it is gone.

### 6.1 Open questions for the author

1. **R4:** should standalone `WAIT 1 5000` with zero replicas block for 5 s (Redis parity) or keep
   FrogDB's documented instant-`0` shortcut? Parity argues block; the existing doc comment
   (`blocking.rs:211-214`) argues the shortcut was a deliberate choice. Pick one and pin it.
2. **Q6 vs Dragonfly:** adopt Dragonfly's early-exit when every tracked replica has already acked and
   the target is still unreachable? It is strictly friendlier and observationally distinguishable only
   by latency. Recommend **no** — Redis parity, and the early exit is wrong if a replica is mid-attach.
3. Should `WAITAOF` be in scope? It is currently a stub returning "not implemented"
   (`integration_replication.rs:3274-3294`) and shares every seam touched here. Recommend a follow-up
   issue rather than folding it in.
4. Is per-shard replication (multiple PSYNC links per node) on the roadmap? If so, Option C's per-shard
   LSN vector becomes relevant and Q1's "node == shard" simplification expires.
