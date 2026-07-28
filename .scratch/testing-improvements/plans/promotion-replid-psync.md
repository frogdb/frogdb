# PRD: Promotion mints a replication ID and a promoted node can head a replication chain

Status: draft
Type: AFK (design + implementation)
Area: replication (area E)
Origin: testing-gap issue 34 Resolution (2026-07-27) — divergence pinned, follow-up "not filed"
Author: planning agent, 2026-07-28

## Links

- Originating issue: `.scratch/testing-improvements/issues/34-promotion-replid-asserts.md`
  (Resolution §"Candidate follow-up (not filed)")
- Sibling boundary: `.scratch/testing-improvements/issues/61-runtime-resync-staged-not-installed.md`
  (runtime full-resync stages but does not install; **being implemented in parallel — this PRD
  assumes it lands**)
- Sim that had to route around this: `.scratch/testing-improvements/issues/23-turmoil-replication-topology.md`
  (§Boundaries), test at `frogdb-server/crates/server/tests/simulation.rs:6095-6105`
- Adjacent contract that must NOT regress: `.scratch/testing-improvements/issues/48-chained-replication-contract.md`
  (chained replication is **rejected**, deliberately)
- Docs page whose stated behavior is currently false:
  `website/src/content/docs/operations/replication.md:15,78,87`

---

## 1. Summary

`REPLICAOF NO ONE` (and the Raft/`CLUSTER FAILOVER` path, which is even less wired) produces a
**writable standalone node, not a replication source**:

1. It does **not** mint a new replication ID, and does not retain the inherited ID as
   `master_replid2` with a continuation boundary — so there is no PSYNC2 failover window at all.
2. It does **not** stand up a `PrimaryReplicationHandler`, so the promoted node answers every
   downstream `PSYNC` with `-ERR PSYNC not supported - server is not running as primary`.

Consequence: a promoted node cannot serve a surviving sibling replica or a demoted ex-primary. The
only supported reconvergence path after a failover is to fail *back* toward a recovered
boot-configured primary — which is exactly the workaround issue 23's turmoil sim had to adopt
(`frogdb-server/crates/server/tests/simulation.rs:6095-6105`), and it contradicts the shipped ops
documentation at `website/src/content/docs/operations/replication.md:87` ("the promoted node becomes
a primary with a fresh replication history, and surviving replicas reconnect to it via PSYNC") and
`:15` ("...only exists on a node that is actually a primary (booted **or promoted** as one)").

This PRD specifies minting + window freeze on promotion, a role-gated primary handler so a promoted
node can serve PSYNC, and the offset/backlog invariants that make partial resync across a promotion
**safe** rather than a silent-divergence trap.

---

## 2. Current state (traced)

### 2.1 What `REPLICAOF NO ONE` actually does

`ReplicaofCommand::execute` handles the promotion arm at
`frogdb-server/crates/server/src/commands/replication.rs:74-88`: it sets `ctx.is_replica = false`
and calls `RoleController::request_promote()` (`:81`). In cluster mode `REPLICAOF` is rejected
outright (`:60-64`) — cluster topology is Raft-owned per ADR-0001.

`RoleManagerHandle::request_promote` → `RoleManager::promote`
(`frogdb-server/crates/server/src/role_manager.rs:252-255`, `:163-175`). The entire body is:

- drop the runtime-demotion stream handle (`:165`),
- `stop()` the boot-spawned replica handler if registered (`:169-171`),
- clear `primary_target` (`:172`),
- `is_replica.store(false)` (`:173`).

There is **no** replication-state mutation, **no** replid rotation, **no** handler construction, and
no state-file save. The frame consumer notices the flag and exits
(`frogdb-server/crates/replication/src/apply.rs:113-118`).

Production callers of `request_promote`: only `commands/replication.rs:81` and
`frogdb-server/crates/server/src/debug_providers.rs:288` (a DEBUG provider). **Cluster mode has no
promotion consumer at all** — `cluster_init.rs:529-556` spawns only a *demotion*-event consumer
(`:763` `request_demote`). So the Raft/`CLUSTER FAILOVER` path
(`frogdb-server/crates/server/src/commands/cluster/admin.rs:243-320`) changes Raft topology metadata
but never performs a data-path promotion either.

### 2.2 The replid-rotation machinery exists but has no production caller

`ReplicationState::new_replication_id(live_offset)`
(`frogdb-server/crates/replication/src/state.rs:248-261`) does exactly the right thing — stores the
current id as `secondary_id`, freezes `secondary_offset` from the **live** offset argument, mints a
fresh id. It is called only from unit tests (`state.rs:440`, `:472`;
`frogdb-server/crates/replication/src/primary/replay.rs:327`;
`frogdb-server/crates/server/src/info/sections.rs` window-rendering tests). Nothing in `server/src`
calls it.

Downstream consumers are already wired and would light up for free:

- `ReplicationState::window_contains` (`state.rs:279-300`) — the secondary branch (`:291-296`)
  grants continuation when `requested_id == secondary_id && requested_offset <= secondary_offset`.
- `PartialSyncReplay::can_replay` (`primary/replay.rs:172-213`) composes that with the backlog
  lower bound, and classifies `FullResyncReason::ReplidMismatch` (`:189-193`).
- INFO rendering: `connection/info_handler.rs:96-113` reads `secondary_id`/`secondary_offset` and
  surfaces the pair only when `secondary_offset >= 0`; `info/sections.rs:364,376-399` renders
  `master_replid2` / `second_repl_offset` (all-zero + `-1` sentinel when absent). The **inclusive**
  boundary convention is documented at `info/mod.rs:305-320`.

So today INFO on a promoted node reports `master_replid` = the inherited primary's id (unchanged),
`master_replid2` = all zeros, `second_repl_offset` = `-1`. That is what issue 34's tests pin
(`frogdb-server/crates/server/tests/integration_replication.rs:1675` `test_secondary_replication_id_failover`,
`:4420` `test_psync2_failover_partial_sync`).

### 2.3 What rejects the downstream PSYNC

Single gate, in the dispatch stage:
`frogdb-server/crates/server/src/connection/dispatch.rs:467-487` — `DispatchStage::PsyncIntercept`
short-circuits with `-ERR PSYNC not supported - server is not running as primary` when
`self.cluster.primary_replication_handler.is_none()` (`:472-476`). Only if the handler is present is
a typed `PsyncHandoff` yielded and later carried out as a raw-socket takeover
(`frogdb-server/crates/server/src/connection.rs:774-828`, with the invariant `expect` at `:794-798`).

`primary_replication_handler` is `Option<Arc<PrimaryReplicationHandler>>` on `ClusterDeps`
(`frogdb-server/crates/server/src/connection/deps.rs:103`). It is populated **once, at boot**:

- `frogdb-server/crates/server/src/server/replication_init.rs:57-103` — the `config.replication.is_primary()`
  branch constructs the handler (`:74-91`) and stores it (`:101`);
- the `is_replica()` branch (`:104-202`) constructs a `ReplicaReplicationHandler` and returns
  `NoopBroadcaster` (`:201-202`), leaving `primary_replication_handler = None` (`:50`);
- standalone (`:203-206`) likewise.

`ClusterDeps` is then assembled once in
`frogdb-server/crates/server/src/server/subsystems.rs:426-437` (`primary_replication_handler:
self.primary_replication_handler.clone()` at `:433`) and cloned into every accepted connection.
**There is no runtime write path to that field** — promotion cannot install a handler today even if
it built one.

Same gate shape governs WAIT: `frogdb-server/crates/server/src/connection/blocking.rs:227-229`
returns `:0` immediately when the handler is absent — so `WAIT` on a promoted node always answers 0.

### 2.4 The write path on a promoted node is inert

Shard workers capture `ctx.replication_broadcaster` **by value at construction**
(`frogdb-server/crates/server/src/server/shards.rs:114,136,149`). A replica-booted node's
broadcaster is `NoopBroadcaster` (`replication_init.rs:201-202`;
`frogdb-server/crates/replication/src/lib.rs:130-144`, `is_active() -> false`). Post-execution
gates broadcast on `self.replication_broadcaster.is_active()`
(`frogdb-server/crates/core/src/shard/post_execution.rs:429-431`).

So on a promoted node, **writes are never stamped with an offset and never recorded into any
backlog**. Even if PSYNC were unblocked, there would be no stream to serve.

### 2.5 How replid/offset get set at boot, per role

- **Primary boot**: recovery phase 5 hands `recovered_replication` (a `ReplicationState` loaded from
  `data_dir/<state-file>` via `ReplicationState::load_or_create`, `state.rs:160-201`) into
  `init_replication`. Tracker offset is seeded from `offset_at_save` (`replication_init.rs:71-72`),
  and `PrimaryReplicationHandler::new` (`primary/mod.rs:110-138`) wraps the state in
  `Arc<RwLock<..>>` (`:122`) and builds the `OffsetCoordinator` over the tracker's offset atomic
  (`:123`; `offset_coordinator.rs:64-76`). The live head is advanced only by
  `broadcast_tagged` → `offsets.advance` (`primary/mod.rs:282-299`), and reconciled into
  `offset_at_save` at save points (`primary/mod.rs:165-168`).
- **Replica boot**: `ReplicaReplicationHandler::new` (`replica/mod.rs:94-111`) seeds its own live
  atomic from `offset_at_save` (`:107`) and owns its own `Arc<RwLock<ReplicationState>>` (`:111`).
  The replid is **adopted from the primary** on `+FULLRESYNC`
  (`replica/connection.rs:185-186`: `state.replication_id = new_repl_id; offsets.reset_to(new_offset)`)
  or from a `+CONTINUE` reply carrying a new id (`:219-225`). Live offset advances per ingested
  frame (`replica/streaming.rs:33` → `replica/offset.rs:40-44`).
- **Runtime demotion** (`REPLICAOF host port`): `RealReplicaStreamer::build_handler`
  (`role_manager.rs:355-433`) constructs a **brand-new `ReplicationState::new()`** (`:367`) — a
  fresh random replid, in a fresh `Arc` that no INFO reader holds. Related latent defect: INFO's
  `replication_state` was snapshotted at boot (`subsystems.rs:302-306`, `:434`), so after a runtime
  `REPLICAOF` the reported `master_replid` is stale/wrong. Called out here because the fix (one
  shared identity cell) is the same fix this PRD needs.
- **Secondary window is never cleared.** No code anywhere assigns `secondary_id`/`secondary_offset`
  outside `new_replication_id` (`state.rs:250-251`). In particular `+FULLRESYNC` adoption
  (`replica/connection.rs:185`) and `apply_staged_metadata` (`state.rs:318-322`) overwrite
  `replication_id` but leave a stale window intact. Inert today (nothing sets it); **a live
  correctness hazard the moment promotion starts setting it** — see §6.3.

### 2.6 Backlog / resume machinery (recently changed)

`PartialSyncReplay` (`primary/replay.rs:41-47`) owns the backlog and the whole grant decision.
Relevant invariants:

- `has_resume_history()` (`:144-146`) = `enabled && backlog.oldest_offset().is_some()`.
- `PrimaryReplicationHandler::is_active()` (`primary/mod.rs:353-363`) = `replica_count() > 0 ||
  has_resume_history()` — this is the "broadcast gate stays open while the backlog can serve a
  resume" change. Its stated rationale is exactly the hazard this PRD must respect: *"every write in
  the meantime must have an offset and a backlog entry or the replica resumes past a hole and
  silently diverges"*.
- Lower bound (`:198-212`): grant iff `req_offset >= oldest_offset()`; with an **empty** backlog the
  code grants only when `req_offset == current_offset` (`:205-211`).
- `ReplicationRingBuffer` (`primary/ring_buffer.rs:38-52,93-95`) has **no explicit floor** — the
  oldest retained *entry* is the only lower bound. Backlog config defaults live at
  `frogdb-server/crates/config/src/replication.rs:107-121,198-207`
  (`split_brain_log_enabled`, `split_brain_buffer_size`, `split_brain_buffer_max_mb`).

The empty-backlog `req_offset == current_offset` grant is safe **today** only by accident: a replica
whose live offset is 0 sends `PSYNC ? -1` (`replica/connection.rs:58-63`), so the branch is
unreachable at a non-zero offset on a boot primary. **Promotion makes it reachable at a non-zero
offset** (promoted node: live offset = applied offset ≫ 0, backlog empty). See §6.2.

---

## 3. Reference behavior (Redis 7.4, `src/replication.c`)

Verified against `https://raw.githubusercontent.com/redis/redis/7.4/src/replication.c`.

**Promotion shifts the replication id** — `replicationUnsetMaster` (`replication.c:3053-3098`) frees
the master link, discards the cached master, cancels any handshake, then:

```c
/* When a slave is turned into a master, the current replication ID
 * (that was inherited from the master at synchronization time) is
 * used as secondary ID up to the current offset, and a new replication
 * ID is created to continue with a new replication history. */
shiftReplicationId();                       /* replication.c:3073 */
disconnectSlaves();                         /* replication.c:3078 */
```

`shiftReplicationId` (`replication.c:1698-1710`):

```c
memcpy(server.replid2, server.replid, sizeof(server.replid));
server.second_replid_offset = server.master_repl_offset + 1;   /* :1707 */
changeReplicationId();                                         /* :1708 */
```

The `+1` exists because a Redis replica asks for **the first byte it has not yet received**
(`replication.c:1700-1706`, verbatim rationale). Sub-replicas are then force-disconnected precisely
so they re-handshake and learn the new id — and Redis notes they *"will be able to partially resync
with us, so it will be a very fast reconnection"* (`:3074-3077`).

**Serving PSYNC across the shift** — `masterTryPartialResynchronization` (`replication.c:718-763`):

```c
if (strcasecmp(master_replid, server.replid) &&
    (strcasecmp(master_replid, server.replid2) ||
     psync_offset > server.second_replid_offset))     /* :730-732 */
    goto need_full_resync;

if (!server.repl_backlog ||                            /* :756 */
    psync_offset < server.repl_backlog->offset ||
    psync_offset > (server.repl_backlog->offset + server.repl_backlog->histlen))
    goto need_full_resync;
```

Two bounds, mirroring FrogDB's `window_contains` + `can_replay` split: replid/second-offset window
first, backlog coverage second. Note Redis full-resyncs unconditionally when **no backlog exists** —
it has no "empty backlog but you're caught up" grant.

**Backlog floor** — `createReplicationBacklog` (`replication.c:102-112`) sets
`server.repl_backlog->offset = server.master_repl_offset + 1`: an explicit floor independent of
buffered entries. FrogDB has no equivalent (§2.6).

**Replicas accumulate a backlog specifically so promotion works** —
`readSyncBulkPayload` (`replication.c:2265-2276`):

```c
memcpy(server.replid, server.master->replid, sizeof(server.replid));
server.master_repl_offset = server.master->reploff;
clearReplicationId2();                      /* :2270 — full resync clears the window */
/* Let's create the replication backlog if needed. Slaves need to
 * accumulate the backlog regardless of the fact they have sub-slaves
 * or not, in order to behave correctly if they are promoted to
 * masters after a failover. */
if (server.repl_backlog == NULL) createReplicationBacklog();   /* :2276 */
```

Two things FrogDB is missing appear here: (a) `clearReplicationId2()` on full resync, and (b) the
replica-side backlog kept *solely* for post-promotion continuity (also recreated on a `+CONTINUE`
resume, `:2577`).

**Propagation gate** — `replicationFeedSlaves` (`replication.c:421-448`) returns early if
`server.masterhost != NULL` (a replica never originates stream bytes) and if
`repl_backlog == NULL && no slaves`. FrogDB's `is_active()` is the same gate; the parallel is worth
preserving deliberately.

---

## 4. Design

### 4.1 Goal / non-goals

**Goal.** After `REPLICAOF NO ONE`, the node is a real replication source: a fresh `master_replid`,
a truthful `master_replid2`/`second_repl_offset` window, an active broadcast path, and a
`PrimaryReplicationHandler` that serves PSYNC — granting `+CONTINUE` exactly when it is safe and
`+FULLRESYNC` otherwise.

**Non-goals.**

- **Chained replication stays rejected** (issue 48). A node that is *currently a replica* must keep
  answering PSYNC with `-ERR PSYNC not supported - server is not running as primary`. The gate
  changes from "handler present" to "handler present **and not currently a replica**", which
  preserves the pinned contract by construction.
- No new failover *controller*. Promotion is still operator/Raft-driven.
- Cluster-mode data-path promotion (no `request_promote` consumer exists at all, §2.1) is
  **staged as a follow-up task** here, not folded into the standalone fix.

### 4.2 Core structural change: one replication identity, one live offset, both roles

Today the two roles own disjoint `Arc<RwLock<ReplicationState>>` + live-offset atoms (§2.5). Every
promotion problem downstream flows from that. Introduce a single per-process replication identity
cell, constructed in `init_replication` before the role branch and handed to **both** handlers:

```
ReplicationIdentity {
    state: Arc<RwLock<ReplicationState>>,   // replid, replid2, second offset, offset_at_save
    live:  Arc<AtomicU64>,                  // the one live offset atomic
}
```

- `PrimaryReplicationHandler::new` takes the cell instead of `state: ReplicationState`
  (`primary/mod.rs:110-138`); `OffsetCoordinator::new` already adopts the tracker's atomic
  (`offset_coordinator.rs:64-76`) — seed the tracker's atomic from the cell (or vend the cell's
  atomic to the tracker) so there is exactly one.
- `ReplicaReplicationHandler::new` takes the cell instead of minting its own
  (`replica/mod.rs:94-111`), and `RealReplicaStreamer::build_handler` stops calling
  `ReplicationState::new()` (`role_manager.rs:367`) — it reuses the cell.
- `ClusterDeps.replication_state` (`deps.rs:113`, `subsystems.rs:302-306`) becomes the cell, so INFO
  reports live truth across every runtime role change (fixes the §2.5 INFO staleness defect for
  free).

### 4.3 Always construct the primary handler; gate it on role

Rather than hot-swapping `Option<Arc<PrimaryReplicationHandler>>` in deps that were already cloned
into live connections (`deps.rs:103`, `subsystems.rs:433`), **construct the handler unconditionally**
for any node in standalone-replication mode (primary, replica, or plain standalone) and gate
*exposure* on the live role flag:

- `replication_init.rs` builds the handler in all branches (it needs `rocks_store` on the replica
  branch too, for checkpoint full-sync — currently only passed on the primary branch, `:74-91`).
- The shard broadcaster becomes a role-aware wrapper holding the handler + `is_replica` flag:
  `is_active()` returns `false` while `is_replica` (a replica must never originate stream bytes —
  Redis `replication.c:434`), and delegates to the handler otherwise. `NoopBroadcaster` remains for
  test/no-replication wiring. This removes the "broadcaster is frozen at shard construction"
  problem (`shards.rs:114,136,149`) without touching shard code.
- `DispatchStage::PsyncIntercept` (`dispatch.rs:467-487`) gate becomes `handler.is_some() &&
  !is_replica` — the connection already carries `is_replica`
  (`subsystems.rs:455`, `info_handler.rs:113`). Same for the WAIT gate
  (`blocking.rs:227-229`) and the INFO `connected_slaves`/primary branch (already role-gated,
  `info_handler.rs:113-126`).

Alternative considered: `ArcSwapOption<PrimaryReplicationHandler>` installed at promotion. Rejected
— it adds a swap seam and a construction-at-promotion failure mode (checkpoint dir, tracker seeding,
config plumbing) on the *least* testable path, and leaves two divergent construction sites.

### 4.4 Promotion sequence (`RoleManager::promote`, `role_manager.rs:163-175`)

Order is load-bearing. The write gate must not open until the new identity is in place, or a write
could be stamped under the inherited replid.

1. Tear down inbound replication (existing `:165-171`) and clear `primary_target` (`:172`).
2. Read the **live applied offset** `Y` from the shared cell (`ReplicaOffset::current`,
   `replica/offset.rs:46-48`) — never `offset_at_save`, which lags (`state.rs:110-118`).
3. `state.new_replication_id(Y)` (`state.rs:248-261`): `secondary_id = <inherited replid>`,
   `secondary_offset = Y`, fresh `replication_id`.
4. **Arm the backlog floor** at `Y` (§4.5) so `has_resume_history()` is true and therefore
   `is_active()` is true from the first post-promotion write.
5. Persist: `save_state()` (`primary/mod.rs:165-168`) so a restart does not lose the window.
6. `is_replica.store(false)` — last. This is the fence that opens writes and un-gates PSYNC.

Steps 2-5 need the async state lock; `promote()` is currently sync under a `std::sync::Mutex`
(`role_manager.rs:220-222`). Simplest resolution: give the identity cell a small sync-safe surface
(`parking_lot::RwLock`, or a `blocking_write`-free helper that the promote path uses) — decided in
task T2, not left to the implementer's discretion at the call site.

### 4.5 Backlog floor (`repl_backlog_off` equivalent)

Add an explicit floor to `PartialSyncReplay` mirroring Redis `replication.c:112`:

- `backlog_start() -> Option<u64>` = `oldest_offset().or(armed_floor)`.
- `has_resume_history()` (`replay.rs:144-146`) = `enabled && backlog_start().is_some()`.
- `can_replay` lower bound (`replay.rs:198-212`) = `req_offset >= backlog_start()`; the
  special-case `None => req_offset == current_offset` arm (`:205-211`) **folds away** — with a floor
  armed at `Y`, `req_offset == Y` is grantable by the general rule and `req_offset < Y` is
  `BacklogEvicted`, matching Redis's two-sided check exactly.
- `arm_floor(offset)` is called on promotion (§4.4 step 4) and at the point today's boot primary
  first admits a replica, so behavior for boot primaries is unchanged in observable terms.

Without this, the promoted node's `is_active()` is `false` (no replicas, empty backlog) and every
post-promotion write is silently unstamped — the exact hole `is_active()`'s doc-comment warns about
(`primary/mod.rs:354-361`), now reachable at a non-zero offset (§6.2).

### 4.6 Clear the secondary window when history is replaced

Add `ReplicationState::clear_secondary_window()` (Redis `clearReplicationId2()`,
`replication.c:2270`) setting `secondary_id = None`, `secondary_offset = -1`, and call it from:

- `+FULLRESYNC` adoption (`replica/connection.rs:185-186`),
- staged-checkpoint adoption `apply_staged_metadata` (`state.rs:318-322`) and the boot install path
  that consumes it,
- issue 61's **live** checkpoint install (see §4.7).

A node that adopts someone else's snapshot has no claim to its former history; leaving a stale
`replid2@offset` advertised would let it grant `+CONTINUE` over data it no longer holds.

### 4.7 Interaction with issue 61 (staged-checkpoint live install — landing in parallel)

Issue 61 makes a runtime full resync install the checkpoint into the live store instead of deferring
to the next boot (`replica/connection.rs:257-296`). Coupling points, assuming it lands first:

- **The failback loop closes.** With live install, a demoted ex-primary that gets `+FULLRESYNC` from
  the promoted node actually discards its forked keyspace. Without issue 61, a `+FULLRESYNC` answer
  from a promoted node is *accepted but not applied until reboot* — i.e. this PRD's PSYNC-serving
  capability would be only half-useful and the "reconverge on the promoted node" story would still
  be false. **Recommendation: sequence this PRD after issue 61 merges**, or at minimum gate the
  turmoil chain test on it.
- **Install must go through the shared identity cell** (§4.2): the install adopts replid + offset
  (`replica/connection.rs:288-292`, `state.rs:318-322`) and must additionally clear the secondary
  window (§4.6) and reset the backlog/floor — a node that just replaced its whole dataset has no
  replayable history.
- **Ordering with promotion**: install and promote both mutate the identity cell. Both run under the
  same lock; the fence (`is_replica`) must be flipped only by the promote path, and an in-flight
  install on a node that has just been promoted must abort (the frame consumer already exits on the
  flag, `apply.rs:113-118` — the checkpoint receiver needs the same check).

### 4.8 Interaction with the backlog/resume machinery

`is_active()` (`primary/mod.rs:353-363`) already encodes "keep stamping while a resume is possible".
The floor (§4.5) is the minimal extension that makes that predicate true for a promoted node.
`broadcast_tagged` (`primary/mod.rs:282-299`) and `request_acks` (`:301-312`) then record every
post-promotion write into the shared backlog, so the tail a `+CONTINUE` replays is complete.

**Optional Phase 2 (Redis parity, `replication.c:2271-2276`): replica-side backlog accumulation.**
Feed the same backlog from the replica ingest path (`replica/streaming.rs:33`, where
`frame_advance` already yields `(offset, frame)`), so a promoted node can serve `+CONTINUE` to a
replica that is *behind* the promotion point, not merely exactly at it. Without Phase 2 the practical
grant set is "sibling replicas that are exactly caught up"; everything else full-resyncs (safe, just
more expensive). Phase 2 does **not** enable chaining — PSYNC stays gated on `!is_replica` (§4.3).

---

## 5. Recommendation

Implement Phase 1 (§4.2-§4.6) as a single change set, sequenced **after** issue 61's live install
lands, and take Phase 2 (replica-side backlog) as a follow-up measured by how often post-failover
resyncs fall back to full.

Rationale: Phase 1 turns promotion from "writable island" into a correct replication source with a
truthful PSYNC2 window, and it removes a documented-but-false claim from the ops page. The residual
cost of skipping Phase 2 is *extra full resyncs*, never incorrect data — a good trade for a first
landing. The shared-identity refactor (§4.2) is unavoidable either way and independently fixes the
stale-INFO-replid defect after runtime `REPLICAOF` (§2.5).

Alternatives rejected:

- *Keep the divergence and fix the docs only.* Cheapest, but leaves failover permanently
  unidirectional (fail-back-only), keeps issue 23's sim contorted, and blocks any future automated
  failover story. Only advisable if promotion is being redefined as a Raft-only operation.
- *Construct the handler at promotion time and swap it in* (§4.3 alternative). Rejected as above.
- *Mimic Redis's `second_repl_offset = offset + 1` exactly.* Rejected: FrogDB replicas request with
  their **applied** offset and the predicate is `<=` (`state.rs:294`,
  `replica/connection.rs:58-63`); adding one would widen the window by a byte-position with no
  corresponding request convention. The inclusive convention is already documented and INFO renders
  it verbatim (`info/mod.rs:305-320`) — keep it, and pin it with a test.

---

## 6. Risks

### 6.1 Offset semantics — spell it out (the classic data-loss trap)

Let `Y` = the promoted node's **live applied offset at promotion**, `A` = the inherited replid,
`B` = the newly minted replid.

- Freeze `secondary_offset = Y` **inclusive**: `window_contains` grants the `A` branch iff
  `requested_offset <= Y` (`state.rs:291-296`).
- **Freezing too low is safe** (over-rejects → extra full resyncs). **Freezing too high is
  corruption**: a peer whose applied offset is `> Y` holds bytes of history `A` that the promoted
  node never applied. Granting it `+CONTINUE` would leave those writes resident on the peer forever
  while the promoted node streams unrelated history `B` from `Y` onward — permanent silent
  divergence, undetectable by offset comparison because the offsets stay numerically contiguous.
- Therefore `secondary_offset` MUST be taken from the **live** applied head
  (`ReplicaOffset::current`), never `offset_at_save` (which lags, `state.rs:110-118`) — that is
  exactly why `new_replication_id` takes `live_offset` as an argument (`state.rs:243-251`) — and
  MUST NOT be `Y + 1` (§5, Redis's `+1` pairs with a different request convention).
- The promoted node's own live offset continues from `Y` under replid `B`. Contiguous numbering
  across an identity change is precisely PSYNC2's model and is safe *only* because everything at or
  below `Y` is shared history. Everything above `Y` under `A` is, by definition, history the promoted
  node does not have.

### 6.2 Empty backlog at a non-zero offset (new hazard introduced by promotion)

`can_replay`'s empty-backlog arm grants `+CONTINUE` when `req_offset == current_offset`
(`replay.rs:205-211`). Unreachable today at non-zero offsets (§2.6), reachable the moment a node is
promoted at offset `Y > 0`. If post-promotion writes are *not* stamped (because `is_active()` is
false with an empty backlog), a peer arriving at exactly `Y` is told "you're caught up" while the
promoted node's dataset has moved on — silent divergence. §4.5's floor removes both the hole
(writes are stamped) and the special case (the arm folds into the general rule). **This is the single
highest-risk item in the change set; it must land in the same commit as the promotion mint.**

### 6.3 Stale secondary window across a role round-trip

Promote (window `A@Y` set) → demote → full resync from a different primary. Without §4.6 the node
still advertises `A@Y` and can grant `+CONTINUE` on history it discarded. Redis clears it
(`replication.c:2270`). Must land with §4.4, not after.

### 6.4 Write-gate ordering

Flipping `is_replica` before minting would let a client write be stamped under replid `A` at an
offset above `Y` — creating exactly the "peer above the window" state of §6.1 on the promoted node
itself. §4.4's ordering (mint → arm → persist → unfence) is mandatory; add a unit test on
`RoleManager::promote` asserting the state mutation happens while the flag is still `true`.

### 6.5 Regression surface

- **Chained replication (issue 48)** must stay rejected — the `!is_replica` term in the PSYNC gate is
  the guard; `test_chained_replication_rejected_sub_replica_never_receives_data` is the canary.
- **`WAIT` on a standalone node** currently short-circuits to `0` on handler absence
  (`blocking.rs:227-229`); with a handler always constructed, the role/replica-count gate must
  reproduce that answer exactly (`integration_cluster.rs:11098-11110` documents the current
  reasoning).
- **Cost on standalone nodes**: constructing a handler that is never used adds a
  `broadcast::channel(10000)` (`primary/mod.rs:120`) plus a ring buffer sized by config. Acceptable;
  measure with the existing bench harness if the allocation shows up.
- **Issue-34 pin tests must be rewritten, not deleted** — `test_secondary_replication_id_failover`
  (`integration_replication.rs:1675`), `test_promoted_node_via_replicaof_no_one_rejects_downstream_psync`
  (`:1763`), `test_psync2_failover_partial_sync` (`:4420`) all pin the *divergence*. They flip to
  parity assertions; the issue-34 file gets a Comments entry recording the reversal.

---

## 7. Testing plan

**Unit (crate `frogdb-replication`, `frogdb-server`)**

1. `state.rs`: `clear_secondary_window` resets id + `-1` sentinel; `window_contains` rejects
   `requested_offset == secondary_offset + 1` on the secondary branch (the §6.1 boundary).
2. `replay.rs`: floor semantics — armed floor with an empty buffer grants `req == floor`, rejects
   `req < floor` as `BacklogEvicted`; `has_resume_history()` true on a floor alone.
3. `role_manager.rs`: `promote()` mutates identity **before** clearing `is_replica` (observe via a
   fake that records the flag value at mutation time); `promote()` is idempotent (second call does
   not re-mint / does not move `secondary_offset` forward).
4. Role-gated broadcaster: `is_active()` false while `is_replica`, true after promotion with an
   armed floor.

**Integration (`frogdb-server/crates/server/tests/integration_replication.rs`)**

5. Rewrite `test_secondary_replication_id_failover` (`:1675`): capture `master_replid` before and
   after `REPLICAOF NO ONE`; assert it **changed**, that `master_replid2` equals the pre-promotion
   replid, and that `second_repl_offset` equals the promoted node's `master_repl_offset` at
   promotion (inclusive convention, `info/mod.rs:305-320`).
6. Replace `test_promoted_node_via_replicaof_no_one_rejects_downstream_psync` (`:1763`) with
   `..._serves_downstream_psync`: raw `PSYNC <old-replid> <offset==Y>` gets `+CONTINUE`;
   `PSYNC <old-replid> <Y+1>` gets `+FULLRESYNC`; `PSYNC <garbage> 0` gets `+FULLRESYNC`.
7. New end-to-end chain: primary P + replicas R1, R2 → kill P → promote R1 → point R2 at R1 →
   assert R2 links up (`master_link_status:up`), receives a post-promotion write, and that R1's
   `connected_slaves` is 1 and `WAIT 1 <t>` returns ≥ 1.
8. Negative: a node that is still a replica (never promoted) keeps rejecting PSYNC — issue 48's
   `test_chained_replication_rejected_sub_replica_never_receives_data` re-run unchanged.
9. Round-trip: promote → demote → full resync from a new primary → assert `master_replid2` is back to
   all-zero and `second_repl_offset` is `-1` (§6.3).
10. Restart continuity: promote, restart the promoted node, assert `master_replid`/`master_replid2`/
    `second_repl_offset` survive (state-file persistence, §4.4 step 5).

**Turmoil / deterministic sim (`frogdb-server/crates/server/tests/simulation.rs`)**

11. Extend `test_replication_failover_wgl_linearizable`: add a 3-node variant where, after promoting
    `R`, the *surviving sibling* re-attaches **to the promoted node** (not to the recovered
    boot-primary) — i.e. delete the workaround documented at `:6095-6105`, and assert convergence
    plus a WGL verdict as today. Keep the existing fail-back arm as a second scenario.
12. Seeded fault injection around the promotion instant: isolate the sibling during steps 2-5 of
    §4.4 and assert that a `+CONTINUE` is never granted above `Y` (assert on the reply, not just on
    final convergence).

**Jepsen tie-in**

13. `jepsen/`: the failover-durability workload (issue 03) currently exercises promote-then-read.
    Add a variant that re-attaches the surviving replica to the promoted node and runs the register
    checker across the transition — this is the only harness that will catch a window computed one
    byte too wide under real concurrency. Nightly tier (issue 10 wiring), not per-PR.

**Docs**

14. `website/src/content/docs/operations/replication.md`: `:78` (replace the "treat the hand-off
    qualitatively" hedge with the actual replid/replid2 contract and the inclusive
    `second_repl_offset` convention), `:87` (now true — keep, and state the full-resync fallback),
    `:15` (chained replication still rejected — reword so "promoted" is accurate rather than
    aspirational). `just doc-sync` / the `doc-sync` skill for link integrity.

---

## 8. Task breakdown (ordered)

| # | Task | Scope (files) | Depends on |
|---|------|---------------|-----------|
| T0 | Land issue 61 (live checkpoint install) | `replication/src/replica/connection.rs`, `fullsync/` | — (parallel agent) |
| T1 | `clear_secondary_window()` + unit tests; call from `+FULLRESYNC` adoption and staged-metadata adoption | `replication/src/state.rs:248-322`, `replication/src/replica/connection.rs:185-186,288-292` | — |
| T2 | Shared replication-identity cell (state + live atomic) threaded to both handlers; drop `ReplicationState::new()` in `build_handler`; point `ClusterDeps.replication_state` at it | `replication/src/{primary/mod.rs:110-138,replica/mod.rs:94-111,offset_coordinator.rs}`, `server/src/server/replication_init.rs`, `server/src/role_manager.rs:355-433`, `server/src/server/subsystems.rs:302-306,426-437` | T1 |
| T3 | Backlog floor (`arm_floor`/`backlog_start`), fold away the empty-backlog special case, extend `has_resume_history` | `replication/src/primary/replay.rs:96-213`, `primary/ring_buffer.rs` | — |
| T4 | Construct `PrimaryReplicationHandler` in every standalone-replication branch (incl. `rocks_store` on the replica branch); role-gated broadcaster wrapper | `server/src/server/replication_init.rs:41-231`, `replication/src/lib.rs:84-147`, `server/src/server/shards.rs:114,136,149` | T2 |
| T5 | Role-gate PSYNC / WAIT / INFO on `!is_replica` instead of handler presence | `server/src/connection/dispatch.rs:467-487`, `server/src/connection/blocking.rs:227-229`, `server/src/connection/info_handler.rs:113-126` | T4 |
| T6 | `RoleManager::promote` sequence: mint → arm floor → persist → unfence; sync-safe access to the cell | `server/src/role_manager.rs:163-175,202-250` | T2, T3, T4 |
| T7 | Unit tests (§7.1-4) | `replication/src/{state.rs,primary/replay.rs}`, `server/src/role_manager.rs` tests | T1, T3, T6 |
| T8 | Integration tests (§7.5-10); rewrite the three issue-34 pins | `server/tests/integration_replication.rs:1519,1623,1675,1763,4420,5284` | T5, T6 |
| T9 | Turmoil: promoted node heads the chain; delete the `:6095-6105` workaround; add the promotion-instant fault variant | `server/tests/simulation.rs` | T0, T8 |
| T10 | Docs: `operations/replication.md:15,78,87` + `doc-sync` | `website/src/content/docs/operations/replication.md` | T8 |
| T11 | Jepsen failover variant re-attaching to the promoted node (nightly tier) | `jepsen/` | T9 |
| T12 | *Follow-up issue, not in this change set*: Phase 2 replica-side backlog accumulation (Redis `replication.c:2271-2276`) | `replication/src/replica/streaming.rs:33`, `primary/replay.rs` | T9 |
| T13 | *Follow-up issue, not in this change set*: cluster-mode data-path promotion consumer (no `request_promote` caller exists, §2.1) | `server/src/server/cluster_init.rs:529-556` | T6 |

Suggested landing order: T1 → T3 → T2 → T4 → T5 → T6 → T7 → T8 → (T0 merged) → T9 → T10 → T11.
T1 and T3 are independently safe and can go first to shrink the risky middle.

---

## 9. Open questions

1. **Sequencing vs. issue 61.** Confirm the parallel agent's landing shape before T2 — if its
   install path introduces its own identity plumbing, T2 should absorb it rather than conflict.
2. **Disconnect existing downstream sessions on promotion?** Redis calls `disconnectSlaves()`
   (`replication.c:3078`) so peers re-handshake and learn the new id. On a promoted FrogDB node the
   downstream session set is empty by construction (it was a replica), so this is a no-op today —
   but it becomes required if Phase 2 / chaining is ever revisited. Decide whether to add it now for
   symmetry.
3. **Should a plain standalone node (no `replicaof`, never promoted) get a handler and answer
   PSYNC?** Today it does not, and issue 48's error message is shared with that case. Recommend
   keeping standalone gated off (config-driven), so the only behavioral change is on the promotion
   path.
4. **`second_repl_offset` reporting.** Confirm the inclusive convention stays (§5) — an operator
   diffing against Redis will see an off-by-one versus `redis-cli INFO`. It is already documented
   (`info/mod.rs:305-320`); this PRD proposes pinning it with a test rather than changing it.

## 10. Out of scope

- Automated failover / leader election on the standalone replication path.
- Chained replication (issue 48 contract stands).
- Any change to the full-sync checkpoint wire format or the `FULLRESYNC` cooldown / lag-threshold
  disconnection policy (`primary/mod.rs:57-69`).
