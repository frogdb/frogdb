# Design brief — rework issue 02: source-side pause barrier at slot-migration finalization

Research output (read-only agent, 2026-08-04) for
[issues/open/02-migration-finalization-pause-barrier.md](issues/open/02-migration-finalization-pause-barrier.md).
Orchestrator decision pending: adopt Option A (recommended below) or hold.
§9's side finding is filed as
[issues/open/11-bare-eval-may-skip-cluster-slot-validation.md](issues/open/11-bare-eval-may-skip-cluster-slot-validation.md)
and must be verified/fixed before issue 02's Lua acceptance test means anything.

## 1. FrogDB finalization path (as implemented today)

Chain, `CLUSTER SETSLOT <slot> NODE <target>`:

| Step | Location |
|---|---|
| Parse + decide | `frogdb-server/crates/server/src/commands/cluster/admin.rs:475` (`cluster_setslot`), NODE arm `:603-644` |
| → `Response::SlotMigrationNeeded{ Complete{..} }` or `Response::RaftNeeded{ AssignSlots }` | `admin.rs:603-644` |
| Internal-action seam | `frogdb-server/crates/server/src/connection/dispatch.rs:293` (`handle_internal_action`, `:279-296`) |
| Delegation | `frogdb-server/crates/server/src/connection/cluster.rs:122-140` (`handle_slot_migration`) |
| Coordinator | `frogdb-server/crates/server/src/slot_migration/mod.rs:115-128` (`complete`), `:134` (`commit`) |
| Raft propose | `frogdb-server/crates/cluster/src/writer.rs:162` (`ClusterWriter::propose`) |
| State-machine apply | `frogdb-server/crates/cluster/src/commands.rs:404-436` — `slot_assignment.insert(slot, target); migrations.remove(&slot);` + `ClusterEvent::SlotMigrationCompleted` |
| Post-apply side effect (source) | `frogdb-server/crates/server/src/slot_migration/events.rs:18-47` — one `ClusterMsg::SlotMigrated{slot,target_addr}` to shard `slot % num_shards`, waking blocked clients with MOVED |

**No quiesce, drain, or pause anywhere on this path.** `SlotMigration` is a two-point
ownership swap with no intermediate states, deliberately (`frogdb-server/crates/cluster/src/types.rs:540-561`).
Routing verdicts: `frogdb-server/crates/server/src/slot_migration/routing.rs:25-57, :133-168`.

Residual window (risk 7): between Raft **commit** and Raft **apply on the losing node**, the
source's snapshot still names itself owner → validates and serves. Documented at
`website/src/content/docs/architecture/clustering.md:330-334`.

## 2. Pause machinery (the thing the issue says to reuse)

- `frogdb-server/crates/core/src/client_registry/mod.rs:171-187` — `PauseMode::{All,Write}`,
  `PauseState{mode, unpause_at}`. **Node-global. No slot dimension, no shard dimension.**
- `:731-768` `pause(mode, timeout_ms)` (ALL beats WRITE, keeps the later deadline, sets
  `expiry_paused`); `:771` `unpause`; `:780` `check_pause` (auto-expiry).
- `frogdb-server/crates/server/src/connection/lifecycle.rs:341-395` `should_pause_command` —
  exempt list `CLIENT|PING|QUIT|RESET|INFO|CONFIG|DEBUG|SLOWLOG`. PAUSE Write blocks
  `WRITE|SCRIPT|PFCOUNT|PUBLISH|SPUBLISH`, exempting `_RO` scripts and `#!lua flags=no-writes`.
- `:459-481` `wait_if_paused` (10 ms poll loop); `:490-513` `wait_if_paused_for_transaction`
  → returns whether it actually parked.
- Gauntlet position: `dispatch.rs:119-137` `PRE_DISPATCH_ORDER` — `PauseGate` stage **8**,
  `ConnectionCommand` **9**, `ClusterSlotValidation` **14**. Pause sits upstream of slot
  validation — same relative order as Dragonfly's `CheckPauseState` → `VerifyCommandState`.

**Contract that must survive**: `frogdb-server/crates/txn/src/exec.rs:165-209` — pre-pause
`validate_queued_batch` (:188), `let paused = host.wait_if_paused().await` (:195), then
re-validation at `:207` if parked. Comment `:201-206` already names migration-barrier pause as
the motivating case. Server side: `frogdb-server/crates/server/src/connection/transaction.rs:166-183`;
trait method `frogdb-server/crates/txn/src/host.rs:106`.

## 3. Drain primitives already in the tree

- **Shard mailbox is itself a drain point.** Scripts execute inline in the shard event-loop
  iteration: `frogdb-server/crates/core/src/shard/dispatch_scripting.rs:6-32` awaits
  `handle_eval_script` before returning to the `select!`
  (`frogdb-server/crates/core/src/shard/event_loop.rs:19-137`). Single-shard scripts bypass VLL
  (`frogdb-server/crates/server/src/connection/scripting/eval.rs:87-113`). Consequence: **a
  round-trip message to shard S is a complete drain of S's in-flight work, scripts included.**
- **VLL continuation lock** = shard-exclusive barrier with parked requests and a deadline:
  `frogdb-server/crates/vll/src/shard.rs:37` (`CONTINUATION_DRAIN_TIMEOUT = 2000ms`),
  `:137-183`, `:250-279`, `:302-326` (`request_continuation_lock` never waits), `:334-336`
  `is_drained`, `:393-412` `DrainTimedOut`. Event-loop arm `event_loop.rs:101-113`.
  Admission is fail-fast (`worker.rs:807-814` → `ERR shard busy with continuation lock`), so
  it's the internal drain, not the client-facing barrier. Cross-shard scripts use it
  (`eval.rs:119-169`).
- **Fan-out drain with typed failure**: `frogdb-server/crates/server/src/server/checkpoint_quiesce.rs`
  (`QuiesceIncomplete{stage, shards, total}`).

## 4. Hazards constraining any design

1. `SETSLOT NODE` may be issued to any node (integration helper sends to the *target*:
   old `integration_cluster.rs:5611-5740`) → barrier must be armable on a *remote* source.
2. **No RPC exists to arm anything on a remote node.** `frogdb-server/crates/cluster/src/network.rs:104-113`
   `RaftRpc::{AppendEntries,Vote,InstallSnapshot,ForwardedWrite}`; `:118-125`
   `BusRpc::{PubSubBroadcast,PubSubForward,HealthProbe}`.
3. **`MIGRATE` is WRITE-flagged** (`migrate_cmd.rs:22-28`) and runs on a normal client
   connection on the source — naive `PAUSE WRITE` deadlocks catch-up MIGRATE.
4. **`CLUSTER` is not pause-exempt** (`lifecycle.rs:343-346`) — `PAUSE ALL` blocks SETSLOT
   itself, including the STABLE/cancel escape hatch.
5. frogdb-core shard layer is cluster-agnostic; `shard == slot % num_shards` → shard-level
   barrier is coarse (1/num_shards of keyspace).
6. Existing `ClusterMsg` channel is the natural carrier
   (`frogdb-server/crates/core/src/shard/message.rs:744-762`; handled
   `frogdb-server/crates/core/src/shard/dispatch_cluster.rs:8`).
7. Blocked clients (BLPOP) already handled out-of-band by `SlotMigrated` → wake-with-MOVED
   (`events.rs:18-47`). Keep; don't drain them.

## 5. External findings

### DragonflyDB — node-wide `CLIENT PAUSE ALL`, best-effort, NOT the correctness mechanism

`src/server/cluster/outgoing_slot_migration.cc`, `OutgoingMigration::FinalizeMigration()` ~:378:
whole-node pause (reads+writes, all slots; admin port exempt via `GetNonPriviligedListeners()`);
in-flight not aborted (`DispatchTracker` + `SendCheckpoint`, `--pause_wait_timeout` 1 s); new
commands block pre-transaction-init → wake to fresh config → plain `-MOVED` (no ASK/TRYAGAIN);
running Lua not interrupted (pause check gated on `transaction == nullptr`,
`main_service.cc:1529-1531`). **On pause timeout it logs and continues unpaused** — safe for
Dragonfly only because the journal `LSN(attempt)` barrier is the real correctness mechanism
(target `Join(attempt)` requires all flows parked at the marker; post-marker write fails the
attempt, retry indefinitely at 500 ms). Pause held across the ownership drop (absl::Cleanup
ordering; their docs §6.3 claims otherwise — code is authoritative). The "TODO implement
blocking on migrated slots only" is still a TODO.
Sources: github.com/dragonflydb/dragonfly docs/cluster-mode.md ·
src/server/cluster/outgoing_slot_migration.cc · dragonflydb.io/blog/a-preview-of-dragonfly-cluster

### Redis legacy MIGRATE + SETSLOT — accepts the race, real data loss

Silent key deletion: SETSLOT NODE on target bumps configEpoch + broadcasts before source
applies; source still serves & acks writes (`cluster.c:1437-1447`), then higher-epoch PONG →
`clusterUpdateSlotsConfigWith` → dirty-slot → `clusterDelKeysInSlot()`
(`cluster_legacy.c:2447-2454, 2540-2549`). Acked write destroyed silently. Migration state
primary-local, not replicated (SETSLOT rejected on replicas, `cluster_legacy.c:6221-6223`);
canonical open issue redis/redis#6339. ASK/TRYAGAIN ladder `cluster.c:1436-1461`; MIGRATE
exempt `:1430-1433`. PR #10517 (replicate SETSLOT) closed unmerged; **Valkey #445** (merged
2024-05, Valkey 8.0) ships `CLUSTER SETSLOT ... [TIMEOUT ms]` with replica-ack block, default 2 s.

### Redis Lua under slot change — per-`redis.call` recheck (ships today)

`script.c:493-554` `scriptVerifyClusterState()` on **every** `redis.call` (call site `:678`) —
converts MOVED/ASK/UNSTABLE/CROSS_SLOT into script errors, never redirects. This is issue 03
option (a), shipped by Redis. Caveat: past `busy-reply-threshold`, `processEventsWhileBlocked`
can apply a config change mid-script → per-call check aborts partway with write-dirty set
(code-derived inference).

### Redis 8.4 ASM / Valkey 9.0 ASM — both pause, both server-wide, both reuse `pauseActions()`

| | Valkey 9.0 (#1949, 2025-08) | Redis 8.4 (#14414, 2025-10) |
|---|---|---|
| Driver | source-driven `CLUSTER MIGRATESLOTS` | destination-driven `CLUSTER MIGRATION IMPORT` |
| Pause | `PAUSE_DURING_SLOT_MIGRATION`, write+expire+evict+replica, server-wide | same shape |
| Entry gate | drain to 0 buffered bytes (default) | ≤1 MiB lag |
| Ceiling | ~10 s (renewed +2 s on REQUEST-FAILOVER) | pause unbounded; task failed at 10 s |
| On timeout | job fails, operator retry | task failed, auto-retry ~1 s |
| Extra | importing slots hidden from KEYS/SCAN | `-TRYAGAIN Slot is being trimmed`, refuses to start under existing pause (`cluster_asm.c:1830-1841`) |

Redis bench: 3→4 shards, 10M×512 B: 6.4 s, <5% latency bump ~2 s, 2.1 MOVED/s vs 241.6 legacy.

**Convergent verdict: write pause held across the ownership flip, server-wide, hard timeout
that fails the MIGRATION rather than dropping the pause.** Dragonfly's drop-on-timeout is
affordable only via its LSN backstop — FrogDB has none (the Raft entry IS the cutover), so
FrogDB must be fail-closed.

## 6. Design options

### Option A — Raft-carried barrier lease (two-phase finalize) — RECOMMENDED

New `ClusterCommand::PrepareSlotHandoff{slot, source_node, target_node, lease_ms}`;
`SlotMigration` gains a prepared state (updates the design note at `types.rs:540-552`).

1. `handle_slot_migration(Complete)` proposes `PrepareSlotHandoff` instead of Complete.
2. Apply on every node → `ClusterEvent::SlotHandoffPrepared`; the source arms a
   **slot-scoped write barrier** (§7) + drain round-trip to shard `slot % num_shards`
   (new `ClusterMsg::DrainSlot{slot, ack}`).
3. Source acks (new `BusRpc::HandoffDrained` or follow-up Raft entry) → finalizer proposes
   `CompleteSlotMigration` as today.
4. Apply of Complete → existing `SlotMigrationCompleted` → release. Lease expiry = backstop
   (release + leader proposes `CancelSlotMigration`).

Bounds: drain deadline ~2 s (CONTINUATION_DRAIN_TIMEOUT shape) → abort finalization,
retryable error, migration record intact. Lease ~10 s (Redis/Valkey parity). Source death →
prepared record visible in `snapshot().migrations`, `SETSLOT STABLE` clears. Cost: two Raft
round trips per slot — batch by slot *range* for resharding (where Redis/Valkey landed).
Closes issue 03: drain + mailbox serialization ⇒ no script mid-execution post-ack; cross-shard
scripts need fan-out drain or VLL gating.

### Option B — Bus-RPC barrier, no Raft schema change

`BusRpc::{ArmSlotBarrier, ReleaseSlotBarrier}`. Cheapest; but barrier invisible to consensus
(must be fail-closed when source unreachable), and finalizer death leaves only the deadline to
release — deadline becomes load-bearing, not backstop.

### Option C — Fencing token instead of a pause

Stamp writes with slot-ownership generation at validation, re-check at execution; per-
`redis.call` epoch check inside Lua (= Redis's `scriptVerifyClusterState`). Zero availability
cost, closes both windows; but pushes cluster awareness into cluster-agnostic frogdb-core,
taxes every write forever, and needs a partial-effects answer. Epoch rides the same mailbox as
writes (`ClusterMsg`), so it is sound. Shelf as follow-up if barrier availability cost proves
unacceptable.

## 7. Recommendation

**Option A, slot-scoped, fail-closed, drain = shard-mailbox round trip.**
1. Raft is already the serialization point — arm/release as state-machine events beat
   Redis/Valkey's timeout-guarded unaudited pause.
2. Slot-scoping is affordable ONLY here: `PauseGate` at stage 8 has `cmd.args` +
   `SlotValidator` available; `PauseState` variant carrying `Option<u16>` slot dodges hazards
   3 and 4 outright (Dragonfly's unshipped TODO).
3. Drain nearly free (inline script execution ⇒ one round trip proves shard empty).
4. Closes issue 03 without hot-path cost.

Do NOT copy Dragonfly's drop-the-pause-on-timeout. Drain timeout must abort finalization.

Sequencing ("measure first" stands):
1. Measure the commit→apply residual window p99 on the losing node; record in PRD dir. If
   sub-ms, record decision before building.
2. Land slot-scoped `PauseState` + `should_pause_command` slot awareness independently
   (testable via CLIENT PAUSE).
3. Then `PrepareSlotHandoff` + drain + lease.
4. Preserve `exec.rs:207` post-wait re-validation verbatim — barrier makes it MORE
   load-bearing (first genuine parked-EXEC-across-finalization test hook; PRD note at
   `.scratch/replication-cluster-rework/exec-slot-revalidation.md:790-798`).

## 8. Failure-mode rows for the cluster spec (if Option A adopted)

FM-CLUSTER-BARRIER-001..014 — see table below (Trigger / Invariant):

| ID | Trigger | Invariant |
|---|---|---|
| 001 | Write on source between Prepare apply and Complete apply | parks; on release observes new owner → -MOVED; never acked on former owner |
| 002 | EXEC parked by barrier, slot moves, release | re-validation at exec.rs:207 → -MOVED, batch not applied |
| 003 | Single-shard Lua in flight when barrier arms | drain ack waits for script; no post-Complete redis.call write (issue 03 criterion) |
| 004 | Cross-shard Lua holding VLL continuation when barrier arms | drain waits or DrainTimedOut; never proceeds live |
| 005 | Drain misses deadline | finalization aborted, barrier released, record intact, retryable error — never silent unpaused proceed |
| 006 | Source dies armed | lease expires; prepared record visible; STABLE/cancel clears; nobody else blocked |
| 007 | Finalizer dies between Prepare and Complete | lease expiry reopens writes; slot still source-owned; no split ownership |
| 008 | Raft leadership change between phases | ProposeError::Redirect surfaces; lease bounds; retry idempotent |
| 009 | Catch-up MIGRATE on source while armed | exempt from the barrier — pin it (else self-deadlock) |
| 010 | SETSLOT STABLE while armed | accepted; barrier released; migration cancelled |
| 011 | Second slot's finalization on the same shard, concurrent | compose per slot OR explicitly serialized — decide and pin |
| 012 | BLPOP client on migrating slot at completion | SlotMigrated wake-with-MOVED still fires |
| 013 | Operator CLIENT PAUSE active when barrier arms (and inverse) | composition pinned; barrier release must not clear operator pause; consider Redis ASM's refuse-to-start |
| 014 | Barrier on a replication primary | decide whether replica feed stalls (Redis/Valkey include PAUSE_ACTION_REPLICA); pin |

## 9. Side finding — filed as rework issue 11

Bare EVAL/EVALSHA/FCALL appear never to reach `ClusterSlotValidation` (`ConnectionCommand`
stage 9 short-circuits before stage 14; no MUST_PRECEDE pair; no bare-EVAL -MOVED test).
Verify/fix BEFORE issue 02's Lua acceptance test — it passes vacuously otherwise. Details in
the issue file.

## 10. Caveats

- `ClusterEvent` (`types.rs:415-447`) has no "prepared" variant; Option A adds one and
  updates the "no intermediate state machine" note at `types.rs:540-552` (update, don't
  work around).
- No measurement of commit→apply window exists — step 1 is a genuine prerequisite.
- Dragonfly mid-script interleaving finding is code-derived inference, not documented.
