# PRD: EXEC-time slot re-validation for MULTI transactions

Status: implemented (pending review)
Type: design + implementation follow-up
Area: Transactions / Cluster
Author: planning pass 2026-07-28
Originating issues:

- [`issues/55-multi-exec-migration-boundary.md`](../testing-improvements/issues/55-multi-exec-migration-boundary.md)
  — pinned the current (broken) contract and escalated the fix per its acceptance criterion 3.
- [`issues/33-fence-min-replicas-e2e.md`](../testing-improvements/issues/33-fence-min-replicas-e2e.md) — documented the
  sibling divergence: MULTI queue-time rejections do not poison the transaction, so EXEC returns a
  short/empty array instead of `EXECABORT`. This PRD subsumes that fix.

> Every claim about current FrogDB behavior below carries a `file:line` citation against the main
> checkout at commit `9dc2e655`. Redis/Dragonfly citations name upstream `unstable`/`main` sources.

---

## 1. Problem statement

A `MULTI` transaction whose commands were slot-validated at queue time executes at `EXEC` time
without any re-validation. If the slot moved (or entered `MIGRATING`) in between, FrogDB commits the
batch on the **former owner**:

- **Orphan write.** Post-migration the batch commits locally, the same connection's next ordinary
  `GET` returns `MOVED`, and the new owner still holds the pre-migration value. The transaction's
  write is invisible cluster-wide and leaves a stale copy behind
  (issue 55 Resolution, measured case 1).
- **Key resurrection.** With `MIGRATING`/`IMPORTING` open and the key already `MIGRATE`d, `EXEC`
  re-creates the key on the source, so it exists on both sides of the open slot with different
  values, and the source stops `ASK`-redirecting for it
  (issue 55 Resolution, measured case 2).

Both are **silent divergence and data loss**, not just a redirect-protocol nit: a client that
retries against the new owner sees its committed write vanish. This is worse than the L1/C2
severity issue 55 was filed at — that score covered the *unpinned-contract* test gap, not the
data-loss consequence, which is C3.

---

## 2. Current state

### 2.1 The pre-dispatch gauntlet

Every command traverses a `const`-declared stage order,
`PRE_DISPATCH_ORDER` (`frogdb-server/crates/server/src/connection/dispatch.rs:73-90`), driven by
`route_and_execute_with_transaction`. The stages relevant here, in order:

| idx | stage | arm |
|-----|-------|-----|
| 1 | `PreChecks` | `dispatch.rs:358-365` |
| 4 | `TransactionControl` | recognizes MULTI/EXEC/DISCARD |
| 5 | `TransactionQueue` | `dispatch.rs:414-419` |
| 13 | `ClusterSlotValidation` | `dispatch.rs:548-553` |
| 14 | `MigratingTryAgain` | `dispatch.rs:556-561` |
| 15 | `Execute` | terminal |

The ordering invariant is asserted, not incidental: `MUST_PRECEDE`
(`dispatch.rs:687-716`) contains `(TransactionQueue, ClusterSlotValidation)` with the comment
"Queued commands slot-validate at queue time; the standalone slot-validation stage runs later on the
non-transaction path." So on the MULTI path, stages 13 and 14 are **never reached** — the queue
stage short-circuits at index 5.

### 2.2 Queue time: the only slot check

`PreDispatchView::try_queue_in_transaction` (`guards.rs:484-502`) runs
`validate_cluster_slots` before enqueuing, and on error calls `state.abort_transaction(msg)` and
returns the redirect. Its comment states the intent explicitly: "commands that would get MOVED
should fail immediately rather than succeeding at EXEC time."

`validate_cluster_slots` (`guards.rs:606-663`) is the redirect seam: it skips cluster-exempt
commands (`is_cluster_exempt`, `guards.rs:587-601`), extracts keys from the registry, enforces
same-slot via `SlotValidator::same_slot`
(`frogdb-server/crates/server/src/slot_migration/validator.rs:50-60`), consumes the one-shot ASKING
flag via `take_asking` (`connection/state.rs:963-964`), and calls
`SlotMigrationCoordinator::route` → `RouteDecision::to_response(readonly_eligible)`.

`queue_command` (`guards.rs:509-583`) then enqueues and folds the command's keys through
`fold_transaction_keys` (`state.rs:859`).

### 2.3 EXEC time: no slot check at all

`handle_exec` (`connection/transaction.rs:88-103`) snapshots the transaction atomically with
`take_transaction` (`state.rs:884-908`) into a `TxnSummary` (`state.rs:435-446`: `queue`, `watches`,
`target`, `exec_abort`, `start_time`) and calls `execute_transaction`
(`transaction.rs:138-316`). That function, in order:

1. `exec_abort` → `EXECABORT` (`transaction.rs:152-159`).
2. Rate-limit batch check (`transaction.rs:162-177`).
3. Empty queue → `*0` (`transaction.rs:180-185`).
4. `wait_if_paused_for_transaction` if the batch has writes (`transaction.rs:190-192`).
5. Partition into shard commands vs. deferred connection-level / server-wide
   (`transaction.rs:204-226`).
6. **Target resolution — the only "routing" that happens** (`transaction.rs:237-242`):
   ```rust
   let target_shard = match target.resolve() {
       Ok(TransactionTarget::None) => self.shard_id,
       Ok(TransactionTarget::Single(shard)) => shard,
       Ok(TransactionTarget::Multi(_)) => unreachable!("resolve() maps Multi to Err"),
       Err(crossslot) => return (TransactionOutcome::CrossSlot, vec![crossslot]),
   };
   ```
7. `run_shard_transaction` (`transaction.rs:323-364`) → `CoreMsg::ExecTransaction`.
8. Merge deferred results (`transaction.rs:272-299`).

`TransactionTarget` (`state.rs:26-34`) is `None | Single(usize) | Multi(Vec<usize>)` — an **intra-node
shard index**, not a node. `resolve()` (`state.rs:42-47`) maps `Multi` → `redirect::crossslot()`.
The accumulator that produces it, `TxnSlotAccumulator` (`state.rs:56-125`), folds shard co-location
and (in cluster mode) slot equality — it never consults slot *ownership*, migrations, or node
identity. There is no call to `SlotMigrationCoordinator::route`, `snapshot()`, `get_slot_owner`, or
`take_asking` anywhere in `transaction.rs`.

### 2.4 The shard side does not backstop it either

`CoreMsg::ExecTransaction` (`frogdb-server/crates/core/src/shard/message.rs:235-243`) is dispatched
at `core/src/shard/dispatch_core.rs:95-115` straight into `ShardWorker::execute_transaction`
(`core/src/shard/execution.rs:521-669`). Its only pre-execution gate is watch validation:
`purge_expired_watches` (`execution.rs:543`) then `check_watches` → early
`TransactionResult::WatchAborted` (`execution.rs:546-548`). Then the per-command loop
(`execution.rs:560-601`) mutates the store immediately, and effects are applied in one batch
afterwards (`execution.rs:603-666`).

The shard *holds* cluster state — `ShardCluster { cluster_state: Option<Arc<ClusterState>> }`
(`core/src/shard/types.rs:560-568`), accessor `types.rs:598-600` — and threads it into every
`CommandContext` (`core/src/shard/worker.rs:318`), but `get_slot_owner` / `is_slot_migrating` /
`get_slot_migration` (`frogdb-server/crates/cluster/src/state.rs:113,128,133`) have **zero call
sites** in `crates/core/src` or `crates/commands/src`. The shard's only migration awareness is
`ClusterMsg::SlotMigrated` (`core/src/shard/dispatch_cluster.rs:6-11`) →
`handle_slot_migrated` (`core/src/shard/blocking.rs:117-155`), which wakes *blocked* clients with
`-MOVED`. That is a waiter drain, not a write gate.

### 2.5 What propagates (replication implications)

Write effects run through the single funnel `run_write_effects`
(`core/src/shard/post_execution.rs:305-495`), ordered by `WRITE_EFFECT_ORDER`
(`post_execution.rs:282-292`). For `EffectScope::Transaction` (`post_execution.rs:462-475`) the
write records are flat-mapped through `replication_forms` (`post_execution.rs:87-111`) and shipped
as one `broadcast_transaction_on_shard`, whose default impl
(`frogdb-server/crates/replication/src/lib.rs:120-126`) frames them as `MULTI` … `EXEC` tagged with
the origin shard. An empty command list ships nothing.

Consequences that constrain the design:

- **Effects are all-or-nothing w.r.t. observers** (one post-loop application), so replicas never see
  a torn intermediate — *but not* w.r.t. command failure: a command that errors mid-batch
  contributes no write meta and the loop continues, so a partially-successful transaction
  propagates as a `MULTI`…`EXEC` block containing only the subset that wrote
  (`execution.rs:560-601`, `post_execution.rs:462-475`).
- The only rollback is WAL-failure rollback under `rollback_mode`
  (`execution.rs:550`, `execution.rs:617-640`), which restores snapshots in reverse and never
  reaches `run_write_effects` — nothing is broadcast, and the client gets `EXECABORT`×N.
- Therefore **any per-command "stop mid-batch on MOVED" design would leak an orphan partial
  transaction to the replicas of the former owner.** Validation must be complete *before* the shard
  round-trip.

### 2.6 Slot-ownership state and how it changes

Slot ownership is Raft-replicated. `ClusterState` (`frogdb-server/crates/cluster/src/state.rs:22-27`)
wraps `ClusterStateInner` (`state.rs:31-48`) holding `slot_assignment: BTreeMap<u16, NodeId>`,
`migrations: BTreeMap<u16, SlotMigration>` (`cluster/src/types.rs:541-549`), `nodes`, and
`config_epoch`. Readers take `snapshot()` (`state.rs:91-100`) → `ClusterSnapshot`
(`cluster/src/types.rs:554-570`); routing is a pure function of that snapshot,
`route_with_snapshot` (`server/src/slot_migration/routing.rs:131-166`):

| snapshot condition | `RouteDecision` |
|---|---|
| owner == self, no migration | `LocalServe` |
| owner == self, migration present | `LocalServeMigrating` |
| other owner, migration.target == self, (`ASKING` or `RESTORE`) | `AcceptImporting` |
| other owner otherwise | `Moved { slot, owner, addr }` |
| no owner | `Unassigned { slot }` |

`RouteDecision::to_response` (`routing.rs:81-100`) renders `MOVED` / `CLUSTERDOWN` through
`frogdb-server/crates/types/src/redirect.rs:21-40` (the single owner of the wire formats).

Transitions are applied by the Raft state machine: `BeginSlotMigration`
(`cluster/src/commands.rs:286-327`) inserts into `migrations`; `CompleteSlotMigration`
(`commands.rs:329-361`) does `slot_assignment.insert(slot, target_node)` +
`migrations.remove(&slot)`. **Neither bumps `config_epoch`** — so there is no cheap monotonic
"did the slot table change since MULTI?" counter available today. A design that wants an epoch fence
must add one (see §4, Option D).

A node learns of a migration only when the Raft entry applies locally; `snapshot()` is read live on
every routing decision, so the visibility gap is bounded by Raft apply latency, not by cache TTL.

### 2.7 Multi-key presence probing (`TRYAGAIN` / `ASK`)

For non-transaction traffic, stage 14 `check_migrating_multikey` (`guards.rs:673-747`) scatters an
`EXISTS` (`ScatterOp::Exists`, `guards.rs:707-724`) over the command's keys when the slot is
`MIGRATING`, then: mixed presence → `"TRYAGAIN Multiple keys request during rehashing of slot"`
(`guards.rs:734-737`); all absent → `ASK` (`guards.rs:741-743`); all present → serve locally. Note
this stage only fires for `keys.len() >= 2` (`guards.rs:690-692`); single-key commands are handled
post-execution by `migrating_ask_for_nil` (`guards.rs:145-183`).

There is no `redirect::tryagain()` helper — the string is a literal at `guards.rs:735`, and
`frogdb-server/crates/types/src/redirect.rs` owns only `moved` / `ask` / `clusterdown_slot` /
`crossslot`.

### 2.8 The sibling divergence (issue 33) is broader than documented

`DispatchStage::PreChecks` (`dispatch.rs:358-365`) short-circuits with the error and calls
`record_error_response(...)` but **never** calls `abort_transaction`. Because `PreChecks` is index 1
and `TransactionQueue` is index 5, *every* pre-check rejection inside a MULTI — NOAUTH,
replica-`READONLY`, self-fence `CLUSTERDOWN` (`guards.rs:291-299`), `NOREPLICAS`
(`guards.rs:316-334`), `NOADMIN`, ACL command `NOPERM`, pubsub-mode — leaves the transaction
un-poisoned. With one queued command that yields `*0`; with several it yields a **partial** EXEC
array. Issue 33 recorded only the single-command `*0` case; the partial-array case is worse and is
currently unpinned.

Redis's `rejectCommand` (`server.c:4277-4287`) calls `flagTransaction(c)` (`multi.c:86-89`) first,
setting `CLIENT_DIRTY_EXEC`, so EXEC replies `-EXECABORT Transaction discarded because of: %s`
(`multi.c:111-124`, `multi.c:149-157`).

Note also that ACL key/channel denials in `queue_command` (`guards.rs:509-583`) return the error but
do **not** abort, while unknown-command and arity errors do — a third inconsistency in the same
seam.

---

## 3. Reference behavior

### 3.1 Redis (validated against `redis/unstable`, Redis 8.x)

Redis validates **twice**: once when the command is queued, once for the whole transaction at EXEC.

**Queue time.** The cluster redirect block in `processCommand` (`server.c:4587-4609`) runs *before*
`queueMultiCommand` (`server.c:4819-4827`). Rejections go through `rejectCommand`
(`server.c:4277-4287`) → `flagTransaction` → `CLIENT_DIRTY_EXEC` → EXEC answers `-EXECABORT`.

**EXEC time.** `getNodeByQuery` special-cases EXEC (`cluster.c:1261-1266`):

```c
if (cmd->proc == execCommand) {
    if (!(c->flags & CLIENT_MULTI)) return myself;
    ms = &c->mstate;
}
```

It then re-resolves slot / owner / `migrating_slot` / `importing_slot` / `existing_keys` /
`missing_keys` over **every queued command**. The decision table:

| condition | reply | source |
|---|---|---|
| keys span slots | `-CROSSSLOT Keys in request don't hash to the same slot` | `cluster.c:1499-1522` |
| slot `MIGRATING`, some keys missing, some existing | `-TRYAGAIN Multiple keys request during rehashing of slot` (`CLUSTER_REDIR_UNSTABLE`) | `cluster.c:1436-1446` |
| slot `MIGRATING`, all keys missing | `-ASK <slot> <target>` | `cluster.c:1436-1446` |
| slot `IMPORTING` + (`CLIENT_ASKING` \|\| `CMD_ASKING`), multiple keys and some missing | `-TRYAGAIN` | `cluster.c:1449-1460` |
| slot `IMPORTING` + `ASKING`, otherwise | serve locally (`return myself`) | `cluster.c:1449-1460` |
| resolved owner != myself | `-MOVED <slot> <owner>` | `cluster.c:1487` |

Write-ness of the *transaction* is folded, not of EXEC itself (`cluster.c:1466-1467`):
`is_write_command = (cmd_flags & CMD_WRITE) || (c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_WRITE))`.

On any EXEC-time redirect, `processCommand` **discards the queue** rather than flagging it
(`server.c:4587-4609`):

```c
if (c->cmd->proc == execCommand) discardTransaction(c);
else flagTransaction(c);
clusterRedirectClient(c, n, c->slot, error_code);
```

So the client sees the bare `-MOVED` / `-ASK` / `-TRYAGAIN` / `-CROSSSLOT` **as the EXEC reply** (not
an array, not `EXECABORT`), and the transaction state is gone — a retry must re-issue `MULTI`.

**`ASKING` interaction.** `CLIENT_ASKING` is explicitly *not* cleared while inside a MULTI
(`networking.c:3050-3056`):

```c
/* We clear the ASKING flag as well if we are not inside a MULTI, and
 * if what we just executed is not the ASKING command itself. */
if (c->flags & CLIENT_ASKING && !(c->flags & CLIENT_MULTI) && prevcmd != askingCommand)
    c->flags &= ~CLIENT_ASKING;
```

So a single `ASKING` before `MULTI` covers the whole transaction, and the EXEC-time
`importing_slot` branch can honor it. FrogDB's `take_asking` (`state.rs:963-964`) is strictly
one-shot with no MULTI exception (`asking_is_one_shot` test, `state.rs:1262-1267`) — a divergence
this PRD must fix to make the `AcceptImporting` path reachable at EXEC.

### 3.2 Valkey

Valkey 8.x inherits Redis 7.x `getNodeByQuery`/`processCommand` structure unchanged for this path
(the EXEC special-case, `discardTransaction`-on-redirect, and the `CLUSTER_REDIR_*` codes are
identical). Valkey's atomic-slot-migration work (VLK RFC "Atomic slot migration") changes *how*
slots move, not the EXEC-time redirect contract. No divergence to design against.

### 3.3 Dragonfly

Dragonfly has genuine multi-shard transactions and takes the opposite trade:

- Ownership is checked at **dispatch/queue** time only — `Service::CheckKeysOwnership`
  (`src/server/main_service.cc:1227-1266`), called from `VerifyCommandState`
  (`main_service.cc:1430-1433`, inside `if (IsClusterEnabled())`), producing `SlotOwnershipError`
  → `-MOVED <slot> <ip>:<port>` (`src/server/cluster/cluster_defs.cc:67-79`).
- At EXEC, it re-checks **only CROSSSLOT** (`main_service.cc:2549-2557`: a `UniqueSlotChecker` over
  `CollectAllKeys`), never ownership. `EXEC_ERROR` →
  `-EXECABORT Transaction discarded because of previous errors` (`main_service.cc:2530-2532`).
- The straddle window is closed **operationally, not per-command**: `FinalizeMigration`
  (`src/server/cluster/outgoing_slot_migration.cc:360-404`) wraps finalization in
  `dfly::Pause(GetNonPriviligedListeners(), …, ClientPause::ALL, …)` with the comment "Migration
  finalization has to be done via client pause because commands need to be blocked on coordinator
  level to avoid intializing transactions with stale cluster slot info. TODO implement blocking on
  migrated slots only."

**Relevance to FrogDB:** Dragonfly's model is only safe because *nothing* can be in flight across
the finalization barrier. FrogDB has neither the EXEC-time recheck nor the pause barrier, so it gets
the worst of both. FrogDB does have pause machinery (`lifecycle.rs:341-395` `should_pause_command`,
`lifecycle.rs:459-481` `wait_if_paused`, `lifecycle.rs:485-507` `wait_if_paused_for_transaction`),
which makes a Dragonfly-style barrier *implementable* — see Option C — but it is a coarser and less
composable answer than re-validation.

---

## 4. Design options

Shared requirement derived from §2.5: **validation must complete before the shard round-trip**, because
partial execution propagates. That rules out any "abort mid-batch" shape.

### Option A — per-command re-validation at EXEC dequeue

Re-run `validate_cluster_slots` per queued command inside the `execute_transaction` partition loop
(`transaction.rs:204-226`), failing the whole transaction on the first redirect.

- **Pro:** minimal new code; reuses the existing per-command seam verbatim.
- **Con:** N `snapshot()` calls, so a migration applying mid-loop can produce an internally
  inconsistent verdict (command 1 validated against the old table, command 5 against the new).
- **Con:** the one-shot `take_asking` is already consumed; per-command re-validation would need N
  asking reads.
- **Con:** temptation to "execute what validated" — which §2.5 shows would propagate an orphan
  partial transaction. Rejected.

### Option B — single batch validation at EXEC entry against one snapshot

Take exactly one `ClusterState::snapshot()` at the top of `execute_transaction`, fold every queued
command's keys into a slot set, route the single resulting slot through the existing
`route_with_snapshot` seam, run the presence probe when the slot is `MIGRATING`, and reply with a
bare `-MOVED` / `-ASK` / `-TRYAGAIN` / `-CROSSSLOT` while discarding the queue.

- **Pro:** exact Redis parity, including the "EXEC replies with the redirect itself, not an array"
  shape and the queue-discard.
- **Pro:** one atomic snapshot → no torn verdict.
- **Pro:** reuses `route_with_snapshot` (`routing.rs:131-166`), which is already pure over a
  `ClusterSnapshot` and unit-testable without a live Raft — the seam was factored for exactly this.
- **Pro:** no shard-side change; the redirect seam stays in one crate.
- **Con:** the `MIGRATING` presence probe is an `await` on a scatter round-trip
  (`guards.rs:707-724`), adding latency to every EXEC on a migrating slot. Bounded: it fires only
  when `route_with_snapshot` returns `LocalServeMigrating`/`AcceptImporting`, i.e. only during an
  active migration of the transaction's own slot.
- **Con:** requires making `ASKING` sticky for the duration of a MULTI (Redis
  `networking.c:3050-3056`) so the `AcceptImporting` arm is reachable.

### Option C — Dragonfly-style pause barrier at migration finalization

Have `CompleteSlotMigration` (`cluster/src/commands.rs:329-361`) drive a client pause through the
existing machinery (`lifecycle.rs:459-507`), so no EXEC can straddle the transition.

- **Pro:** closes the window for Lua and any other future batch primitive at the same time, not just
  MULTI.
- **Con:** does not fix the *completed-migration* case at all — a transaction queued long before the
  pause and EXEC'd after it still validates against nothing. It narrows the race; it does not
  eliminate the orphan write.
- **Con:** global latency hit on every migration; Dragonfly's own TODO calls the global scope wrong.
- **Con:** availability regression during rebalance.
- Verdict: complementary hardening at best, not a substitute.

### Option D — epoch fence

Bump `config_epoch` on `BeginSlotMigration`/`CompleteSlotMigration` (`commands.rs:286-367`, neither
bumps today), record the epoch in `TxnSummary` at `MULTI` time, and fail EXEC when it changed.

- **Pro:** O(1) check, no key extraction, no scatter.
- **Con:** far too coarse — *any* slot's migration anywhere in the cluster aborts *every* open
  transaction. Under continuous rebalance that is a livelock.
- **Con:** `config_epoch` has independent failover semantics (see the open issue-47 epoch-fold work);
  overloading it couples two unrelated concerns.
- **Con:** cannot produce the right reply (`MOVED` vs `ASK` vs `TRYAGAIN`) — only a generic abort.
- Verdict: rejected as the primary mechanism. A dedicated monotonic `slot_table_version`, bumped by
  the three slot-migration commands, is worth adding **as a fast path**: when the version is
  unchanged since `MULTI`, skip the fold and probe entirely. That is a pure optimization layered on
  Option B and should be deferred to a follow-up, not built first.

### Option E — shard-side validation

Insert the check at `execution.rs:546-549`, right after `check_watches`, returning
`TransactionResult::Error(String)` (which exists at
`core/src/shard/types.rs:1081-1089` and is already mapped to an error reply at
`transaction.rs:358`, but is never produced today).

- **Pro:** structurally the last possible gate; a future non-connection caller of `ExecTransaction`
  would also be covered.
- **Con:** the shard has `cluster_state` (`types.rs:598-600`) but not `node_id`, the connection's
  `ASKING` flag, `readonly` mode, or the address-rendering seam — all of which the decision needs.
  Threading them in duplicates the redirect seam into a second crate, violating the "one home for
  MOVED/ASK formats" invariant that `types/src/redirect.rs:1-9` was created to enforce.
- **Con:** the replica-apply path re-enters the same `ExecTransaction`
  (`server/src/replication/executor.rs:83-116`) with `REPLICA_INTERNAL_CONN_ID`; a shard-side gate
  would have to special-case it or replicas would reject their own primary's stream.
- Verdict: rejected.

---

## 5. Recommendation

**Adopt Option B**, plus two scoped corrections it depends on or naturally subsumes.

### 5.1 Core change

Add one validation pass to `execute_transaction` (`transaction.rs:138-316`), placed **after** the
`exec_abort` check (`transaction.rs:152-159`) and **before** target resolution
(`transaction.rs:237-242`) — specifically after the empty-queue early return
(`transaction.rs:180-185`) and before the pause wait (`transaction.rs:190-192`), so an EXEC destined
for a redirect does not first block on a pause.

Shape (new code lives in the redirect seam, not in `transaction.rs`):

```rust
// slot_migration/routing.rs — pure, snapshot-driven, unit-testable
pub(crate) enum BatchRoute {
    ServeLocal,
    Redirect(Response),
    ProbeMigrating { slot: u16, keys: Vec<Bytes>, target: SocketAddr },
}

pub(crate) fn route_queued_batch(
    snapshot: &ClusterSnapshot,
    slot_keys: &[(u16, Vec<Bytes>)],   // folded from the queue by the caller
    asking: bool,
    self_node_id: NodeId,
    readonly_eligible: bool,
) -> BatchRoute
```

- Fold: for each queued command, skip if `is_cluster_exempt` (`guards.rs:587-601`), else extract keys
  via the registry (same call shape as `guards.rs:684-689`) and collect distinct slots.
- Zero keyed commands → `ServeLocal` (a MULTI of only keyless/deferred commands must not be
  redirected — matches Redis, whose `getNodeByQuery` returns `myself` when no slot is resolved).
- More than one distinct slot → `Redirect(redirect::crossslot())`.
- Exactly one slot → `route_with_snapshot(snapshot, slot, "EXEC", asking, self_node_id)` and project
  through the existing `to_response(readonly_eligible)` (`routing.rs:81-100`), except that
  `LocalServeMigrating` / `AcceptImporting` return `ProbeMigrating` so the caller can run the
  async presence check.
- `readonly_eligible` for the batch = connection is `READONLY` **and every** queued command is
  read-only (a batch containing a write must never be rescued by the READONLY override). This mirrors
  Redis folding `mstate.cmd_flags & CMD_WRITE` (`cluster.c:1466-1467`).

The caller in `transaction.rs` then, on `ProbeMigrating`, runs the same `ScatterOp::Exists` probe as
`check_migrating_multikey` (`guards.rs:707-737`) over the union of the batch's keys — mixed →
`TRYAGAIN`, all absent → `ASK`, all present → serve. Factor the probe out of
`check_migrating_multikey` into a shared helper so the two paths cannot drift; the existing
non-transaction stage keeps its `keys.len() >= 2` gate, the batch path does not (a one-key
transaction on a migrating slot must still `ASK`).

**Reply shape:** the redirect is the EXEC reply itself (bare error, not an array element, not
`EXECABORT`), and the queue is already gone because `take_transaction` (`state.rs:884-908`) consumed
it — matching Redis's `discardTransaction` before `clusterRedirectClient`.

**Metrics:** add `TransactionOutcome::Redirected` to the enum (`transaction.rs:35-50`) with label
`"redirected"` in `metric_label` (`transaction.rs:74-83`). That match is deliberately exhaustive with
no wildcard arm, so the compiler forces the label choice; the pinning test at `transaction.rs:447-461`
must be extended.

### 5.2 Dependency: sticky `ASKING` inside MULTI

`take_asking` (`state.rs:963-964`) is one-shot, so by EXEC the flag is long gone and the
`AcceptImporting` arm (`routing.rs:148-153`) is unreachable for transactions. Mirror Redis
(`networking.c:3050-3056`): while `in_transaction()`, do not clear `asking` on read; capture it into
`TxnSummary` (`state.rs:435-446`) at `take_transaction` and clear it there. The existing
`asking_is_one_shot` test (`state.rs:1262-1267`) stays valid for the non-transaction path and gains a
MULTI-scoped sibling.

### 5.3 Subsumed: issue-33 `EXECABORT` divergence

Route MULTI-time `PreChecks` rejections through `abort_transaction` so EXEC replies `EXECABORT`
rather than a short or empty array. Concretely, in the `PreChecks` arm (`dispatch.rs:358-365`), when
`state.in_transaction()`, call `state.abort_transaction(Some(msg))` before short-circuiting. Do the
same for the ACL key/channel denials in `queue_command` (`guards.rs:509-583`) that currently return
without aborting, so all three queue-time rejection classes behave identically (unknown command and
arity already abort).

This is a behavior change to the pinned tests in
`frogdb-server/crates/server/tests/integration_replication.rs`
(`test_self_fence_multi_rejected_at_queue_time`, `test_min_replicas_to_write_multi_and_lua_paths`) —
they must be updated, with the old `*0` expectation replaced by `EXECABORT` and a comment pointing at
issue 33.

Doing this in the same change is deliberate: without it, FrogDB would have Redis-parity EXEC-time
redirects but non-parity queue-time aborts, which is a more confusing contract than either extreme.

### 5.4 Not in scope

- Option C's pause barrier (file as a follow-up; it hardens Lua and future batch primitives).
- Option D's `slot_table_version` fast path (follow-up optimization).
- The Lua-internal write bypass noted in issue 33 (`EVAL` lacks the WRITE flag, so `run_pre_checks`
  never sees `redis.call('SET', …)`), which has the same "batch validated once, executed later"
  shape and deserves its own PRD.

---

## 6. Risks

| # | Risk | Mitigation |
|---|---|---|
| 1 | **Latency on migrating slots.** The presence probe adds a scatter round-trip to EXEC. | Probe only on `LocalServeMigrating`/`AcceptImporting` for the transaction's own slot; reuse the existing `scatter_gather_timeout`. Follow-up `slot_table_version` fast path skips even the fold in the common case. |
| 2 | **Deliberate tripwire tests break.** Issue 55's two tests exist *to fail loudly* when EXEC learns to re-validate (issue 55 Resolution, "Escalation"). | Rewrite them in the same commit, keeping the section banner (`integration_cluster.rs:11771`) and replacing the contract comment with the new one plus a link to this PRD. |
| 3 | **Clients that retry EXEC blindly.** A bare `-MOVED` where an array was expected can confuse a client that does not treat EXEC replies as redirectable. | This is exactly Redis's behavior; redis-py/Lettuce/go-redis cluster clients all handle it. Document in `website/src/content/docs/compatibility/overview.mdx`. |
| 4 | **Keyless-only transactions.** Over-eager redirect of a MULTI containing only `PING`/`INFO`/deferred commands would be a regression. | Explicit "zero keyed commands → ServeLocal" rule; dedicated unit test. |
| 5 | **`READONLY` replica transactions.** Folding write-ness wrong could either break read-only replica MULTI or let a write through on a replica. | Batch `readonly_eligible` requires *all* commands read-only; test both directions against the existing replica MULTI coverage (`integration_cluster.rs:6655-6776`). |
| 6 | **Replica-apply path.** The replication executor re-enters `CoreMsg::ExecTransaction` (`server/src/replication/executor.rs:83-116`). | Unaffected: the new gate is connection-side in `transaction.rs`, which the executor does not call. Assert this with a replication test that a primary's MULTI still applies on a replica that does not own the slot. |
| 7 | **Snapshot staleness.** Raft apply latency means a node can still validate against a slot table one entry behind. | Unavoidable and identical to the non-transaction path; this PRD closes the *seconds-to-minutes* MULTI window, not the sub-apply-latency one. State this explicitly in the new contract comment. |
| 8 | **Dispatch-invariant drift.** `MUST_PRECEDE` (`dispatch.rs:687-716`) encodes "queued commands slot-validate at queue time" as a comment. | Update that comment; the pairwise constraint itself is unchanged (EXEC-time validation lives in `transaction.rs`, not as a new gauntlet stage). Do **not** add a 17th stage — `every_stage_appears_exactly_once` (`dispatch.rs:673`) asserts the length, and EXEC validation is not a per-command pre-dispatch concern. |

---

## 7. Testing plan

### 7.1 Unit (pure, no Raft)

In `slot_migration/routing.rs` (the module already has snapshot-driven unit tests because
`route_with_snapshot` was factored for that reason, `routing.rs:126-131`):

- keyless-only batch → `ServeLocal`
- single slot, owner == self, no migration → `ServeLocal`
- two distinct slots → `CROSSSLOT`
- single slot, owner != self → `MOVED` with the owner's rendered address
- single slot, owner != self, we are the import target, `asking` → `ProbeMigrating`
- single slot, owner != self, import target, **no** `asking` → `MOVED`
- unassigned slot → `CLUSTERDOWN`
- `READONLY` connection + all-read batch on a foreign-owned slot → `ServeLocal`;
  same batch with one write → `MOVED`

In `connection/state.rs`: `ASKING` survives across queued commands inside MULTI and is cleared by
`take_transaction`; unchanged one-shot behavior outside MULTI.

In `connection/transaction.rs`: extend the `TransactionOutcome` label-pinning test
(`transaction.rs:447-461`) for the new `Redirected` variant.

### 7.2 Integration — rewrite the issue-55 tests

`frogdb-server/crates/server/tests/integration_cluster.rs`, same section banner (`:11771`):

- `test_multi_exec_after_completed_slot_migration_commits_on_former_owner` → rename to
  `..._redirects_with_moved`. Assert: EXEC reply is `MOVED <slot> <new-owner>`; the former owner does
  **not** hold the written value; the new owner still holds the pre-migration value; a subsequent
  `MULTI`/`EXEC` against the new owner succeeds. Keep the existing control assertion (queue-time
  `MOVED` → `EXECABORT`) to prove both seams fire.
- `test_multi_exec_during_in_flight_slot_migration_commits_on_source` → rename to
  `..._asks_when_keys_migrated`. Assert: EXEC reply is `ASK <slot> <target>`; the key is **not**
  resurrected on the source; the source still `ASK`-redirects an ordinary read of it.

New cases in the same section:

- **TRYAGAIN**: two keys in one slot, one already `MIGRATE`d, one not; EXEC →
  `TRYAGAIN Multiple keys request during rehashing of slot`.
- **Serve-through**: slot `MIGRATING` but all the batch's keys still present on the source; EXEC
  commits normally.
- **ASKING-scoped import**: `ASKING`; `MULTI`; queue writes for an importing slot; `EXEC` → commits
  on the target (proves §5.2 and the `AcceptImporting` arm).
- **Keyless MULTI during migration**: `MULTI`; `PING`; `EXEC` → `*1 +PONG`, no redirect.
- **CROSSSLOT at EXEC**: queue two commands that were individually valid but whose slots diverged
  because one slot migrated → `CROSSSLOT`, not a partial commit.
- **No-orphan invariant**: after any redirected EXEC, assert the former owner's `DBSIZE`/key read is
  unchanged — this is the assertion that actually catches the data-loss bug, independent of the
  reply string.

Replica/regression: extend the existing replica MULTI coverage
(`integration_cluster.rs:6655-6776`) with a `READONLY`+all-reads batch (must serve) and a
`READONLY`+one-write batch (must `MOVED`).

Issue-33 subsumption: update `test_self_fence_multi_rejected_at_queue_time` and
`test_min_replicas_to_write_multi_and_lua_paths` in `integration_replication.rs` from `*0` to
`EXECABORT`, and add a multi-command case asserting no **partial** array is ever returned.

### 7.3 Jepsen

`testing/jepsen/frogdb/src/jepsen/frogdb/slot_migration.clj` already drives migrations from the
client (ops at `:162-261`, 9-phase generator at `:321-383`, real checker at `:414-579` with four
gating properties at `:547-551`) and there is a ready-made persistent-connection abstraction
(`cluster_client.clj:228-234` `make-conn-single`, precedent at `list_append.clj:142-159`).

Add a `:queued-txn` op that, on a pinned single connection, issues `MULTI` + queued writes in one
`wcar`, lets the generator interleave a migration of that slot, then issues `EXEC` in a later
`wcar`. Carmine reads replies eagerly, so the `MULTI` and its queued commands must share a `wcar`
and the connection must persist — hence `make-conn-single`.

Then add a fifth checker property: **no orphaned write.** The existing checker has no way to detect a
write that landed on the source after the slot moved; the final-read window
(`slot_migration.clj:395`) reads through the cluster client, which follows `MOVED` to the new owner
and therefore never sees the orphan. The new property must read the **former owner directly** with
`ASKING`-free raw connections and assert no key of a migrated slot is present there.

`transaction.clj:101-108`'s `:multi-write` op is dead code (its generator at `:143-153` never emits
it, and the workload runs single-node only per `run.py:108-110,184-186`) — either wire it up or
delete it as part of this work, so the repo has exactly one MULTI-under-migration workload.

### 7.4 Verification commands

- `just test frogdb-server 'multi_exec'` — targeted.
- `just test frogdb-server` on a testbox (`just tb-run`) for the full cluster suite.
- `just lint` (testbox) and `just fmt`.
- 8 consecutive `--retries 0` runs of the rewritten boundary tests, matching the flake-hunting bar
  issue 55 set (issue 55 Resolution, "Verification").

---

## 8. Task breakdown

Ordered; each is independently reviewable and leaves the tree green.

**T1 — Sticky `ASKING` inside MULTI.**
`connection/state.rs` (`take_asking` :963-964, `TxnSummary` :435-446, `take_transaction` :884-908),
plus the `asking_is_one_shot` test (:1262-1267). No behavior change outside MULTI.

**T2 — `redirect::tryagain()` helper.**
`frogdb-server/crates/types/src/redirect.rs` (alongside `moved`/`ask`/`clusterdown_slot`/`crossslot`
at :21-40); replace the literal at `connection/guards.rs:735`. Pure refactor, keeps the "one home for
wire formats" invariant.

**T3 — Extract the migrating presence probe.**
Split the `ScatterOp::Exists` probe out of `check_migrating_multikey`
(`connection/guards.rs:673-747`) into a helper callable with an explicit key list and slot, so the
batch path and the per-command path share it. `check_migrating_multikey` keeps its `keys.len() >= 2`
gate at its own call site (`guards.rs:690-692`).

**T4 — `route_queued_batch` in the redirect seam.**
New pure function + `BatchRoute` enum in `slot_migration/routing.rs` (next to `route_with_snapshot`
:131-166 and `to_response` :81-100). Key extraction mirrors `guards.rs:684-689`; exemption mirrors
`is_cluster_exempt` (`guards.rs:587-601`). Ships with the §7.1 unit tests. No call sites yet.

**T5 — Wire it into EXEC.**
`connection/transaction.rs`: call the fold + `route_queued_batch` + probe between the empty-queue
return (:180-185) and the pause wait (:190-192); add `TransactionOutcome::Redirected` (:35-50) and
its `metric_label` (:74-83); extend the label test (:447-461). Update the `MUST_PRECEDE` comment at
`dispatch.rs:711-713`.

**T6 — Rewrite the issue-55 boundary tests + add the new cases.**
`frogdb-server/crates/server/tests/integration_cluster.rs:11770-12109`, including a replacement
contract comment under the existing banner that states the new semantics, cites Redis
(`cluster.c:1261-1266`, `server.c:4587-4609`), and names the residual Raft-apply-latency window
(risk 7).

**T7 — Issue-33 subsumption: `EXECABORT` on queue-time rejection.**
`connection/dispatch.rs:358-365` (abort when `in_transaction()`), `connection/guards.rs:509-583`
(ACL denials abort). Update `integration_replication.rs`
(`test_self_fence_multi_rejected_at_queue_time`, `test_min_replicas_to_write_multi_and_lua_paths`)
and add the no-partial-array case.

**T8 — Docs.**
`website/src/content/docs/architecture/clustering.md` (redirect seam),
`architecture/connection.md:103-104` (state table),
`architecture/consistency.md:125-140` (MULTI/EXEC atomicity section),
`compatibility/overview.mdx` (EXEC returns a bare redirect). Keep single-source-of-truth: one
canonical description in `clustering.md`, links elsewhere.

**T9 — Jepsen `:queued-txn` op + orphan-write checker property.**
`testing/jepsen/frogdb/src/jepsen/frogdb/slot_migration.clj` (ops :162-261, generator :321-383,
checker :414-579, final read :395) using `cluster_client.clj:228-234` `make-conn-single`. Resolve the
dead `:multi-write` op in `transaction.clj:101-108`.

**T10 — Follow-ups to file (not build here).**
(a) `slot_table_version` fast path (Option D as an optimization, requires bumping it in
`cluster/src/commands.rs:286-367`); (b) pause barrier at migration finalization (Option C,
Dragonfly parity, hardens Lua); (c) Lua-internal write validation, the same "validated once,
executed later" shape flagged in issue 33.

---

## Implementation notes

Implemented 2026-07-28 on branch `worktree-agent-a62ab6b4fde7fcca5`. All ten tasks are done; T10 was
"file follow-ups", and the three issues are linked below.

### Task status

| Task | Status | Landed in |
|---|---|---|
| T1 Sticky `ASKING` inside MULTI | done | `connection/state.rs` |
| T2 `redirect::tryagain()` | done | `types/src/redirect.rs`, `connection/guards.rs` |
| T3 Extract the migrating presence probe | done | `connection/guards.rs` (`probe_key_presence`) |
| T4 `route_queued_batch` | done | `slot_migration/routing.rs` + `slot_migration/tests.rs` |
| T5 Wire it into EXEC | done | `connection/transaction.rs` |
| T6 Rewrite the issue-55 boundary tests | done | `tests/integration_cluster.rs` |
| T7 Issue-33 subsumption (`EXECABORT`) | done | `connection/dispatch.rs`, `connection/guards.rs`, `tests/integration_replication.rs` |
| T8 Docs | done | `website/.../clustering.md` (canonical) + 3 linking pages |
| T9 Jepsen `:queued-txn` + orphan checker | done | `testing/jepsen/.../slot_migration.clj`, `transaction.clj` |
| T10 Follow-ups filed | done | [`issues/01`](issues/01-exec-slot-table-version-fast-path.md), [`issues/02`](issues/02-migration-finalization-pause-barrier.md), [`issues/03`](issues/03-lua-internal-write-validation.md) |

### Ordering invariant, as built

`Connection::exec` now runs, in order: `exec_abort` check → rate limit → empty-queue early return →
**`validate_queued_batch`** → `CLIENT PAUSE` wait → shard round-trip. The validation therefore
completes before any shard sees a single queued command, which is what keeps
`broadcast_transaction_on_shard` from ever framing a partial `MULTI`…`EXEC` for replicas. A redirect
returns `(TransactionOutcome::Redirected, vec![redirect])` — one bare error reply, queue discarded.

### Deviations from the PRD

1. **`BatchKeys` instead of `&[(u16, Vec<Bytes>)]`.** §5.1 sketched passing a slice of
   `(slot, keys)` pairs. The implementation introduces a `BatchKeys` type (`BTreeSet<u16>` of slots
   plus a flat `Vec<Bytes>` of keys) with `add_key`/`keys`/`single_slot`/`is_keyless`. The fold is
   then a single pass with no intermediate grouping, and the CROSSSLOT decision is `slots.len() > 1`
   rather than a scan. Per the repo's encapsulation rule, the routing code asks the type questions
   instead of destructuring a tuple slice.
2. **`ProbeMigrating` split into `ProbeMigratingSource` and `ProbeImporting`.** One variant would
   have forced the caller to re-derive which side of the migration it was on in order to know which
   reply to build. Two variants make the caller's match exhaustive over the two genuinely different
   follow-ups (source: all-present → serve, none-present → `ASK`, split → `TRYAGAIN`; target: keys
   must be absent from the source's perspective, `ASKING` already consumed).
3. **The probe returns facts, not a `Response`.** `probe_key_presence` returns a `KeyPresence`
   (`All` / `None` / `Split`) rather than a ready-made error reply, so `route_queued_batch` stays a
   pure function over a snapshot and the reply construction lives in one place.
4. **`readonly_eligible` is folded only over non-cluster-exempt commands.** A `READONLY` session's
   batch is eligible for local service on a replica only when *every* keyed, non-exempt command is a
   read. Exempt commands (`PING`, `ECHO`, connection-state commands) neither contribute keys nor
   disqualify the batch. The PRD did not specify the exempt-command interaction. (Revised in the
   review round: the `WRITE` flag is now consulted *before* the exemption test, so no classification
   bug can rescue a write. See "Review round" below.)
5. **Jepsen: `:read-orphans` gates, `:exec-queued-txn` reports.** The PRD asked for one new property.
   As built, the *orphan* property gates `:valid?` (`no-orphaned-writes?`); the straddling
   transaction's own outcome (`:redirected` vs `:executed`) is reported but not gated, because both
   are legal — only a write on a former owner is a fault. The transaction uses a second key in the
   same slot (`{migration-test}:txn`, alongside the register's `{migration-test}:key`) so a legally
   refused transaction write cannot perturb the existing durability / value-correctness properties.
6. **`transaction.clj`'s dead `:multi-write` op was deleted, not wired up.** The PRD allowed either.
   Deleting leaves exactly one MULTI-under-migration workload in the repo, which is the stated goal;
   wiring it up in a single-node workload would not have exercised migration at all.

### Test results

All local, targeted (the orchestrator gates the full suite):

- `just test frogdb-server 'multi_exec|slot_migration|self_fence|min_replicas_to_write|asking|transaction'`
  → **135 passed**, 1947 skipped.
- `just test frogdb-server 'slot_migration::tests'` → 30 passed (12 new `route_queued_batch` cases).
- `just check frogdb-server` → clean.
- Flake bar from issue 55: 8 consecutive `cargo nextest run --retries 0 -E
  'test(/multi_exec|keyless_multi|self_fence_multi|min_replicas_to_write_multi/)'` runs → **8/8 ×
  29 passed**, zero flakes.
- Clojure changes are paren-balanced and reference only existing `cluster-db` / `frogdb` fns; the
  Jepsen suite itself needs a Docker cluster and was not executed here.

### Not run here (deliberate, per the task's working rules)

`just lint`, the full `just test`, and every `tb-*` testbox command.

### Review round (adversarial review verdict: fix-first)

Nine findings, all fixed in the same branch.

| # | Severity | Finding | Fix |
|---|----------|---------|-----|
| F1 | CRITICAL | `is_cluster_exempt` treated *every* `ScatterGather` and `ConnectionLevel` command as node-scoped, so `MSET`/`MGET`/`DEL`/`EXISTS`/`TOUCH`/`UNLINK` and the whole scripting family folded to an empty key set and took the keyless fast path — committing on a former owner and replicating an orphan `MULTI`…`EXEC`. | Narrowed the predicate to what is genuinely node-scoped: `ServerWide(_)`, non-scripting `ConnectionLevel(_)`, and the hard-coded name list. Scatter-gather commands and scripting's `dynamic_keys` now fold. |
| F2 | MAJOR | Sticky `ASKING` leaked: `take_asking` stops consuming inside a transaction, but `discard_transaction` / `clear_transaction` never cleared the flag, so `ASKING`→`MULTI`→`DISCARD` left the next ordinary command wrongly `AcceptImporting`. | Both clear `asking`. `DISCARD` without a `MULTI` (an error) still leaves a pending `ASKING` intact. |
| F3 | MAJOR | The `WRITE`-flag fold ran *after* the exemption `continue`, so an exempt write never cleared `all_readonly`. | `WRITE` is evaluated before any exemption test; the invariant is structural, not a consequence of correct classification. |
| F4 | MAJOR | Validation ran before the `CLIENT PAUSE` wait and never re-ran, so an `EXEC` parked across a finalization resumed on a stale verdict. | `wait_if_paused_for_transaction` returns whether it actually parked; validation re-runs only then, so the unpaused path still pays one snapshot. |
| F5 | MINOR | `probe_key_presence` failed open (`_ => {}` swallowed non-`Integer` results; an empty keyed slice scored `AllPresent`). | Any reply that is not `Integer(0|1)`, or fewer replies than keys, returns `Unavailable`. |
| F6 | MINOR | `reset()`'s comment claimed Redis's `resetCommand` leaves `ASKING`/`READONLY` alone; `clearClientConnectionState` clears both. | `reset()` clears both; comment corrected. |
| F7 | NIT | `BatchKeys::add_key` pushed duplicates. | Deduped through a `HashSet`, keeping the fold linear. |
| F8 | NIT | `validate_queued_batch` gated on `cluster_state`, `validate_cluster_slots` on `slot_migration`. | Both gate on both, so one seam cannot be silently disabled. |
| F9 | Jepsen | `error-message` returned `nil` for non-`Exception` error carriers (a real `MOVED` scored `:executed`); `:queued-txn` ignored its `MULTI`/`SET` replies; `no-orphaned-writes?` gated on *all* orphans, including ones the ordinary `:write` generator can produce through the documented raft-apply window. | Error carriers extended to maps and raw error strings; `:queued-txn` records a queue-time refusal as `:queue-refused` and pins nothing; the orphan gate is scoped to keys the straddling transaction wrote, with all orphans still reported. |

Disagreements: none — every finding was reproduced or is a straightforward invariant tightening. The
only judgement call is F1's blast radius: the same predicate also guards the *non*-transactional
path (`connection/routing.rs`), where a same-slot `MSET` or a keyed `EVAL` on a non-owner was
likewise served locally instead of `MOVED`. Narrowing the shared predicate fixes both at one seam
rather than special-casing the fold; the whole `integration_cluster` suite (174 tests) still passes,
so the wider fix is contained.

F4 has no integration coverage: driving it needs a `CLIENT PAUSE WRITE` on the source that outlives
a slot handover, and the handover's own `MIGRATE` runs through a client connection on that same
paused node. Documented in the test banner and in
[issue 02](issues/02-migration-finalization-pause-barrier.md), which gains a natural hook for it.

Review-round test results (all targeted, local):

- `just test frogdb-server 'integration_cluster'` → **174 passed**, 1914 skipped.
- `just test frogdb-server 'script|eval|reset|readonly|asking|multi|transaction|mset|mget|state::tests|guards'`
  → **282 passed**, 1806 skipped.
- `just check frogdb-server`, `just lint frogdb-server` (clippy `-D warnings`, both feature sets),
  `just fmt` → clean.
- `lein check` over the Jepsen project → `jepsen.frogdb.slot-migration` compiles with no warnings.
- Negative control: reverting `is_cluster_exempt` to its pre-fix form makes
  `test_multi_exec_scatter_gather_batch_is_slot_validated` and
  `test_multi_exec_eval_with_declared_keys_is_slot_validated` fail (the scatter batch executes
  locally: `Array([Simple("OK"), Integer(1), Array([Bulk(None), Bulk("v1")])])` instead of `MOVED`).

### Known gaps

- The residual Raft-apply-latency window (risk 7) is unchanged and is now documented in
  `clustering.md`; closing it needs [issue 02](issues/02-migration-finalization-pause-barrier.md).
- Lua's "validate once, write later" shape is untouched — [issue 03](issues/03-lua-internal-write-validation.md).
- Validation is unconditional; the `slot_table_version` fast path is
  [issue 01](issues/01-exec-slot-table-version-fast-path.md).
