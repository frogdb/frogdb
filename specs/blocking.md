# Blocking commands — failure modes

Every way a blocking command (`BLPOP`/`BRPOP`/`BLMOVE`/`BRPOPLPUSH`/`BLMPOP`/`BZPOPMIN`/
`BZPOPMAX`/`XREAD BLOCK`/`WAIT`) can be resolved on the connection side, one table per mode.
Same contract as [Transactions — failure modes](txn.md): a mutant that survives
is a row nothing forces.

Scope: the **connection-side** blocking-wait path — `handle_blocking_wait` /`register_wait` /
`cleanup_wait` / `reconcile_unregister` in
`frogdb-server/crates/server/src/connection/blocking.rs`, and the race arbitration in
`frogdb-server/crates/server/src/connection/blocking/coordinator.rs` over the four inputs a
parked wait supervises: the shard's response, the client-admin edges (`CLIENT KILL`,
`CLIENT UNBLOCK`), the peer's socket, and the deadline. Every blocked state is leavable, and
the last two are why: without them a park is a suspended read loop that no disconnect and no
operator command can end. Rows stop at what the
connection observes across the shard channel; the shard-side wait queue (registration, FIFO
`pop_oldest_*`, the acknowledged `UnregisterWait` handshake, restore-on-send-failure) lives in
`frogdb-core` and gets its own spec.

## State space

"Survives restart" is `No` for every row in this table: the whole area is in-memory,
connection- and shard-process-lifetime state with no WAL/snapshot participation: a restart
(or a shard crash) always drops every parked wait, which is exactly why TR-BLOCKING-014
(shard death) and its ruling exist.

| Variable | Authoritative field | Writer(s) | Persistence | Survives restart |
|---|---|---|---|---|
| Connection blocked-flag + target | `ConnectionState.blocked: Option<BlockedState>` (`BlockedState { shard_id, keys }` type at `connection/state.rs:129-134`; field itself at `state.rs:461-463`) | `begin_block`/`end_block` (`state.rs:777-783`), read by `blocked_shard()` | In-memory only | No |
| Client-registry blocked mirror | `ClientRegistry::update_blocked_state(id, bool)` (`admin.client_registry`, called from `register_wait`/`cleanup_wait`, `connection/blocking.rs:115-117,142-144`; also set directly by `handle_wait_command` for WAIT, `:274-277,288-291`, with no paired shard registration). **Ordering, `register_wait` side:** the mirror is raised *before* the `BlockWait` send and lowered again only if that send fails (`mirror_blocked_then_register`, `blocking.rs:478-499`) — never after the send returns. The send awaits, so "after" leaves a window in which the connection is committed to blocking while `CLIENT UNBLOCK` finds no `ClientFlags::BLOCKED` and answers `0` (FM-BLOCKING-003). The inverted window is the safe one: an UNBLOCK arriving early sets the unblock signal, the coordinator sees it the moment it starts waiting, and the resulting `UnregisterWait` is ordered behind this `BlockWait` on the shard's own channel either way | Same call sites as the connection-side flag | In-memory only | No |
| Per-key wait FIFO | `ShardWaitQueue.waiters_by_key: HashMap<Bytes, VecDeque<usize>>` (`shard/wait_queue.rs:51-81`). The deque's push order **is** the per-key FIFO authority (what "How to read a row" and TR-BLOCKING-001/006 depend on): every `pop_oldest_*` walks it front-to-back and no pop path consults the registration ordinal. The HashMap's own iteration order is never an ordering authority — the one cross-key consumer, `drain_waiters_for_slot`, sorts by the registration ordinal instead | `register`/`unregister`/`pop_oldest_waiter*`/`drain_waiters_for_slot` | In-memory only | No |
| Wait-entry slab | `ShardWaitQueue.entries: Vec<Option<WaitEntry>>` + `free_slots`, `conn_entries: HashMap<u64, Vec<usize>>` (`wait_queue.rs:51-81`) | `register`/`unregister` | In-memory only | No |
| Registration ordinal | `ShardWaitQueue.next_seq: u64` (`wait_queue.rs`), `seq_by_slot: Vec<u64>` (parallel to the slab) — the global registration order `register` stamps on every accepted entry. It is the authority for **cross-key** drain order only: `drain_waiters_for_slot` sorts the slot's entries by it, so a slot migration answers the oldest registered waiter first regardless of `waiters_by_key`'s HashMap iteration order. It is **not** consulted by any per-key pop path — per-key FIFO is the deque row above — and reusing a slab slot overwrites the stale ordinal before it can be read | `register` only; read by `drain_waiters_for_slot` (order authority) and by `dump()`/the wait-queue-log diagnostic | In-memory only | No |
| Per-shard waiter count | `ShardWaitQueue.waiter_count: usize` (`wait_queue.rs:61`) | Incremented in `register`, decremented in `unregister`/pop paths | In-memory only | No |
| Admission limits | `ShardWaitQueue.max_waiters_per_key`, `max_blocked_connections`; `0` disables the corresponding check. `register`'s guards run *before* the entry takes a slab slot and hand the rejected `WaitEntry` back to the caller as `RegisterRefused { entry, message }`, so the refusal can be answered on the entry's own `response_tx` (FM-BLOCKING-006) instead of dropping it | Production default `BlockingConfig` (`frogdb-server/crates/config/src/blocking.rs:16-24`, `DEFAULT_MAX_WAITERS_PER_KEY = 10000`, `DEFAULT_MAX_BLOCKED_CONNECTIONS = 50000`), applied once via `ShardWorker::set_wait_queue_limits` (`worker.rs:515-521`) from `server/shards.rs:193` at shard construction; `ShardWaitQueue::new()`'s `with_limits(10000, 50000)` (`wait_queue.rs:116-118`) is only the `Default`/test path, not the production wire. `#[param(skip)]` on both config fields — not `CONFIG SET`-mutable; `set_wait_queue_limits` replaces the whole queue, so it is never called again after construction. | In-memory only, config-derived | No (re-derived from config at boot) |
| Per-waiter deadline | `WaitEntry.deadline: Option<tokio::time::Instant>` (`wait_queue.rs:13-33`; mirrored connection-side as the `deadline` local in `handle_blocking_wait`, `connection/blocking.rs:34-48`, also `tokio::time::Instant`) | Set once at registration from the command's timeout; never mutated | In-memory only | No |
| Timeout authority | Connection-side `BlockingWaitCoordinator::wait_for_response`'s `timeout_fut` (`connection/blocking/coordinator.rs:78-111`, the sleep at `:85-90`) is canonical/precise; shard-side `check_waiter_timeouts` (`shard/blocking.rs:182-211`, ticked every 100ms from `shard/event_loop.rs:31,83-85`) is a GC safety net only — it removes expired entries and best-effort sends `op.timeout_reply()` (a no-op if the server already replied and dropped the receiver), it never consumes store data | Server: `tokio::time::sleep_until(deadline)`; Shard: periodic `collect_expired(now)` | In-memory only | No |
| Wake token (response channel) | `WaitEntry.response_tx: oneshot::Sender<Response>` (`wait_queue.rs:13-33`), paired with the connection's borrowed `response_rx` | Always *sent on*, never dropped as a signal: `drive_satisfaction_body` (serve), `check_waiter_timeouts` (GC), the admission refusal (FM-BLOCKING-006), the deadline fast-path (TR-BLOCKING-007), the `Satisfaction::Retry` arm, the slot-migration drain (FM-BLOCKING-008) and the demotion release (FM-BLOCKING-007) each send a real reply. The only remaining drop-without-send is shard teardown, which is what makes the connection's `Err(_)` mean shard death and nothing else (FM-BLOCKING-004). Borrowed-not-consumed by the connection so a raced serve stays drainable | In-memory only | No |
| Serve-vs-timeout reconciliation state | `UnregisterAck::{Unregistered, AlreadyServed}` (`shard/message.rs:545-554`), decided by whether `wait_queue.unregister(conn_id)` still found the entry | Computed synchronously inside `handle_unregister_wait` from the shard's serial mailbox order | In-memory only, transient (one round trip) | No |
| WAIT quorum snapshot | `WaitCoordinator` (`replication/src/wait_coordinator.rs:114-127`): `offsets: Arc<OffsetCoordinator>` (target offset, snapshotted at WAIT call time via `target_offset()`, `:173-180`), `tracker: Arc<ReplicationTrackerImpl>` (ack counting via `count_acked`, `:182-189`) | `offsets`/`tracker` are shared, continuously-updated seams; the per-call target offset is a local snapshot, not stored state | In-memory only | No |
| WAIT role fence | `WaitCoordinator.role_fence: tokio::sync::watch::Sender<()>` (`wait_coordinator.rs:126`), observed via `RoleFence` (`:85-106`), taken by `role_fence()` (`:159-161`) | Bumped by `RoleManager::demote` (`server/role_manager.rs:362-403`, fence flip at `:371`) → `PrimaryReplicationHandler::end_primary_stint`; subscribed *before* the replica-role check in `handle_wait_command` (`connection/blocking.rs:253-256`, ordering is load-bearing, see `wait_coordinator.rs:85-95`) | In-memory only | No |
| Node role flag | `is_replica: Arc<AtomicBool>` (`RoleManager`, flipped `false→true` at `role_manager.rs:371` inside `demote`, before the fence's consequences propagate). Read directly (not through the fence) by `handle_wait_command`'s pre-parse rejection (`connection/blocking.rs:228`) and its post-fence re-check (`:254`) | `RoleManager::demote`/promote-equivalent path | In-memory only | No |
| CLIENT UNBLOCK + CLIENT KILL signals | `ClientSignals` trait yielding `ClientEdge::{Killed, Unblocked(Option<UnblockMode>)}`, prod impl on `ClientHandle` via `killed_or_unblocked` (`client_registry/mod.rs`). One seam for both edges because they are two watch receivers on the same handle, which a `select!` cannot borrow twice; the kill is biased first inside it | Set by the `CLIENT UNBLOCK` / `CLIENT KILL` handler on another connection; observed by the parked wait's `select!` (non-WAIT: `coordinator.rs` `wait_for_response`; WAIT: `resolve_wait_race`, `connection/blocking.rs`). Both are *level-triggered* watches, so a signal that lands before the first poll is still seen | In-memory only | No |
| Parked-wait supervision | `PeerLiveness` trait, prod impl `SocketWatch` over the connection's `Framed` (`connection/blocking.rs`), used by both the non-WAIT park and `WAIT`. The parked wait, not the run loop, owns the socket for the duration of the park — this is what makes a blocked client a *supervised state on a readable connection* rather than a suspended read loop (TR-BLOCKING-013, TR-BLOCKING-021) | Polled only inside `wait_for_response`; resolves exactly once, on EOF | In-memory only | No |
| Parked pipeline buffer | `ConnectionHandler.parked_frames: VecDeque<Result<BytesFrame, _>>`, bounded by `MAX_PARKED_PIPELINE_FRAMES` (`connection.rs`) | Pushed by `SocketWatch` for frames that arrive while a wait is parked; drained ahead of the socket by `try_next_frame` so pipelined commands run *after* the blocking command they were pipelined behind, in arrival order (Redis ordering). At the cap the watch stops reading, applying backpressure instead of growing unboundedly | In-memory only | No |
| Pending serve-propagation buffer | `ShardWorker.pending_serve_propagations: Vec<SynthesizedCommand>` (pushed at `shard/blocking.rs:359-361`) | `drive_satisfaction_body`, on every committed serve (never on a restored/rejected one); flushed to replicas by the terminal `ReplicationBroadcast` write effect, after the waking write's own broadcast | In-memory only | No |
| Blocking metrics | `BlockedClients` (gauge, set on register/unregister/serve/restore/GC/demotion-release), `BlockedSatisfiedTotal` (inc on a committed serve, via `record_blocked_waiter_satisfied`), `BlockedTimeoutTotal` (inc per GC-reclaimed entry **and** per waiter the satisfaction path's deadline fast-path replies to — the two are the same event seen by the two timeout authorities, and exactly one of them ever reaches a given waiter because the fast-path removes it from the queue), `BlockedMigrationMoved` (inc only for the `-MOVED` case of slot migration, not `-CLUSTERDOWN`) | The shard worker, at each corresponding transition | In-memory only (exported, not restart-durable) | No |
| Demotion-triggered wait unblock | `BlockingMsg::ReleaseAllWaiters`, sent to every shard by `RoleManager::demote` (`server/role_manager.rs`) through the `BlockedWaiterFence` seam (`ShardWaiterFence`, a `try_send` fan-out over the node's `ShardSender`s) after the `is_replica` fence flip and *before* the inbound stream is opened; the shard's `handle_release_all_waiters` drains `ShardWaitQueue` whole and answers every entry with `frogdb_core::ROLE_CHANGED_UNBLOCK_ERR` (Redis `disconnectAllBlockedClients` verbatim, the same constant `WAIT`'s role-change reply uses). Ordering is the mechanism, not a race: the release is enqueued on each shard's serial mailbox before the stream that will deliver replicated writes even exists, so no replicated write can reach a waiter that the demotion was supposed to release (FM-BLOCKING-007). | `RoleManager::demote` → `ShardWaiterFence` → each shard's `handle_release_all_waiters` | In-memory only | No |

## Transitions

Rows below encode what the code does today and what it is ruled to keep doing. Where a ruling
settles semantics the code does not yet implement, a row splits into `Postcondition (ruled)` /
`Postcondition (current code)`, carries a `Pending` field naming the issue that will close the
gap, and a forcing test must not be written against the `(current code)` cell.
[Issue 08](../.scratch/spec-gaps/issues/done/08-blocking-command-rows.md) — the ruling
authority for the admission-limit, shard-death, demotion, slot-migration, disconnect and
`WAIT` role-change rows in this file — has landed, so no split row remains here.

`Forced by` citations below follow the same bare-function-name convention as the FM rows (see
"How to read a row"), but `just lint-spec`'s `Forced by` enforcement today covers only `FM` and
`LV` rows — `TR` rows are parsed but not required to carry the field (`scripts/spec-lint.py`'s
`REQUIRED_FIELDS` loop and its `row.kind != "LV"` skip). Citations here are added ahead of that
coverage landing, in the same spirit as the FM rows; a row with no located test says so plainly
rather than naming one that doesn't force the behavior.

## TR-BLOCKING-001 — park: list/zset/stream pop family (BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP)

| Field | Value |
| --- | --- |
| Precondition | The immediate (non-blocking) attempt found no data; connection's blocked-flag is `None`; `timeout ≥ 0`; registration is not refused by the admission guard (TR-BLOCKING-003 is the refusal branch of this same call). |
| Postcondition | Connection blocked-flag = `Some(BlockedState{shard_id, keys})`; client-registry blocked mirror = true; a `WaitEntry` exists in the target shard's per-key FIFO for every key in the command, pushed at the tail of that key's `VecDeque` (deque push order is the per-key FIFO authority; the `next_seq` ordinal stamped alongside it orders cross-key slot drains, not per-key pops); per-shard waiter count incremented; `BlockedClients` gauge set to the new count; per-waiter deadline set from `timeout` (`None` if `timeout == 0`, i.e. indefinite). |
| Source | `connection/blocking.rs:34-75` (`handle_blocking_wait`), `:82-120` (`register_wait`); `shard/blocking.rs:30-74` (`handle_block_wait`); `shard/wait_queue.rs:142-209` (`register`) |
| Forced by | `debug_waitqueue_reports_blocked_client`, `test_xread_block_timeout`, `test_xreadgroup_block_timeout` — registration is not independently forced beyond these; the pop-family park itself is exercised end-to-end by the TR-BLOCKING-006 wake tests below (a park that silently failed would make every one of them hang or fail). |
| Rulings | — |

## TR-BLOCKING-002 — registration refused before park (shard unreachable / invalid shard)

| Field | Value |
| --- | --- |
| Precondition | `register_wait` cannot even reach the target shard: the connection holds no sender for the computed `shard_id` (topology/config mismatch), or the shard's message channel send fails (shard task gone). No `WaitEntry` is ever constructed — this fires before `handle_block_wait` runs, so it is disjoint from TR-BLOCKING-003 (which fires inside the shard, after a `WaitEntry` exists). |
| Postcondition | `Response::error("ERR Internal error: invalid shard")` (no sender case) or `Response::error("ERR Internal error: shard unreachable")` (send-failed case); connection's blocked-flag never set; client-registry blocked mirror never set. |
| Source | `connection/blocking.rs:96` (no-sender error text), `:111` (send-failed error text), both inside `register_wait` (`:82-120`) |
| Forced by | MISSING — no test drives `register_wait`'s no-sender/send-failed branches (`connection/blocking.rs:96,111`); both require a fault-injected shard channel, not present in today's suite. |
| Rulings | — |

## TR-BLOCKING-003 — registration refused at the admission limit

| Field | Value |
| --- | --- |
| Precondition | The `WaitEntry` reaches the shard (TR-BLOCKING-001's precondition otherwise holds) but at registration time either the shard's total waiter count — `waiter_count`, the same field the "Per-shard waiter count" state-space row names, compared directly against the limit despite `max_blocked_connections`'s connection-count name — has already reached `max_blocked_connections` (default 50000), or, for some key in the command, that key's waiter count has already reached `max_waiters_per_key` (default 10000, `0` disables the corresponding check). |
| Postcondition | An error reply reaches the client stating the limit was hit (`-ERR max blocked connections limit reached` global / `-ERR max waiters per key limit reached` per-key, global checked first), sent on the wait's own `response_tx` and returned immediately — the connection never parks. Neither the per-shard waiter count nor any per-key FIFO nor the slab is mutated by a refused registration. See FM-BLOCKING-006. The reply is an explicit error rather than a dropped `response_tx`, and the two are **not** interchangeable: the refusal used to drop the sender, which the coordinator turned into a bare RESP nil (`$-1`) — *not* the op-aware `op.timeout_reply()` an ordinary timeout sends (`*-1` for BLPOP, TR-BLOCKING-009). A client hitting the admission limit was therefore answered with a shape that means "no such element", indistinguishable from an empty pop, which is what made the limit look benign on the wire. Issue 08's MAJ-11 drop-elimination removed that path entirely; a dropped sender now uniquely means shard death (`-ERR shard unavailable`, FM-BLOCKING-004), so no shard-side path may signal a refusal or a timeout by dropping. |
| Source | `shard/wait_queue.rs` (`register`, the two admission checks and the `RegisterRefused` hand-back), defaults at `frogdb-server/crates/config/src/blocking.rs`; refusal handling at `shard/blocking.rs` (`handle_block_wait`) |
| Forced by | unit-level (frogdb-core, `shard/blocking.rs`): `admission_refusal_at_the_global_limit_replies_and_registers_nothing`, `admission_refusal_at_the_per_key_limit_replies_and_registers_nothing` |
| Rulings | [issue 08](../.scratch/spec-gaps/issues/done/08-blocking-command-rows.md) (H7; MAJ-11 is the drop-elimination this row's Postcondition now describes), [issue 26](../.scratch/spec-gaps/issues/done/26-distsys-review-minors-sweep.md) (MIN-8: the pre-MAJ-11 cell claimed the dropped sender resolved "exactly as an ordinary timeout would", which was false in shape — `$-1` vs `*-1` — and hid the admission-limit bug's severity) |

## TR-BLOCKING-004 — WAIT resolves without parking (replica rejection / quorum already met)

| Field | Value |
| --- | --- |
| Precondition | Either this node is a replica at the pre-parse check (before `role_fence` is even subscribed), or it is a primary and the live offset/ack snapshot already meets `numreplicas` at first check. |
| Postcondition | Replica case: `Response::error(WAIT_ON_REPLICA_ERR)`, returned before any argument parsing beyond the role check — no `RoleFence` subscription, no target-offset snapshot, no ack solicitation. Quorum-already-met case: `Response::Integer(count)` returned from the fast-path check, again without parking; no `WaitEntry`-equivalent bookkeeping exists for WAIT (it never registers with `ShardWaitQueue` — see the "Client-registry blocked mirror" state-space row). |
| Source | `connection/blocking.rs:228-230` (`WAIT_ON_REPLICA_ERR`, pre-parse), `:253` (`role_fence()` subscription), `:261-264` (fast-path quorum check, `count_acked` ≥ `num_replicas` → immediate `Response::Integer`) |
| Forced by | `test_wait_on_replica_is_an_error`; the quorum-already-met branch is not independently forced (`test_wait_zero_timeout_blocks_until_ack` exercises the adjacent solicited-quorum path, not the immediate fast-path return). |
| Rulings | — |

## TR-BLOCKING-005 — park: WAIT

| Field | Value |
| --- | --- |
| Precondition | Node is a primary at the pre-parse check (TR-BLOCKING-004's replica branch did not fire); after subscribing to `role_fence`, the post-fence role re-check also finds this node still primary (closing the demotion-race window between the two checks); the fast-path quorum check did not already satisfy `numreplicas`. |
| Postcondition | `RoleFence` subscription held for the duration of the wait; target offset snapshotted via `target_offset()`; ack solicitation sent once to streaming replicas; client-registry blocked mirror set directly (no shard-side `WaitEntry`, unlike TR-BLOCKING-001); the WAIT is parked on `wait.wait_for_replicas(...)` raced against the client-signal seam (`signals.next_edge()`) and the peer's socket via `resolve_wait_race`. |
| Source | `connection/blocking.rs:226-294` (`handle_wait_command`), `:253-256` (fence-then-role-check ordering), `:274-277` (blocked-mirror set) `:319-347` (`resolve_wait_race`); `replication/src/wait_coordinator.rs:159-161` (`role_fence`), `:173-180` (`target_offset`), `:214-256` (`wait_for_replicas`) |
| Forced by | `test_wait_blocks_until_ack`, `test_wait_zero_timeout_blocks_until_ack`, `test_wait_zero_timeout_blocks_without_quorum`, `test_wait_inside_multi_nonzero_timeout_does_not_block` |
| Rulings | — |

## TR-BLOCKING-006 — wake by push (serve)

| Field | Value |
| --- | --- |
| Precondition | A write command (`LPUSH`/`RPUSH`/`LPUSHX`/`RPUSHX`/`BLMOVE`/`BRPOPLPUSH` for `WaiterKind::List`; `ZADD` for `SortedSet`; `XADD` etc. for `Stream`, per `WaiterWake::{Kind,All}` on the executed command's spec) leaves data at a key with a live waiter of a matching kind, `response_tx` still open, deadline not yet elapsed at the pre-send check (TR-BLOCKING-007 is the elapsed-at-check branch of this same loop). |
| Postcondition | Oldest matching waiter (FIFO within kind: the first entry of that kind walking the key's `VecDeque` front-to-back) is popped from the per-key queue and removed from the slab; `response_tx.send(reply)` succeeds; the served element is consumed from the store (not restored); a synthesized deterministic command (e.g. `BLPOP`→`LPOP`, `BLMOVE`→`LMOVE`) is appended to `pending_serve_propagations` so a replica reproduces the same mutation, flushed at the terminal `ReplicationBroadcast` write effect after the waking write's own broadcast; for `BLMOVE`/`BRPOPLPUSH` cascades, the destination key is re-checked for its own waiters up to `MAX_BLMOVE_FANOUT_DEPTH` (16). |
| Source | `shard/blocking.rs:255` (`drive_satisfaction`), `:271-390` (`drive_satisfaction_body`), `:341` (`response_tx.send`, `Ok(())` arm), `:359-361` (propagation push), `:26` (`MAX_BLMOVE_FANOUT_DEPTH`), `WaiterSatisfaction` impls |
| Forced by | unit-level (frogdb-core, `shard/blocking.rs`, not covered by this spec's `-p frogdb-server` test-resolution profile): `blpop_satisfy_pops_and_replies`, `push_to_live_waiter_still_consumes_element`, `served_blpop_records_pending_propagation`, `blmove_success_moves_and_cascades`, `bzpopmin_satisfy_pops_min`; integration (in profile): `test_blpop_woken_notifies_lpop`, `test_brpop_woken_notifies_rpop`, `test_bzpopmin_woken_notifies_zpopmin`, `test_bzpopmax_woken_notifies_zpopmax`, `test_brpoplpush_woken_notifies_rpop_and_lpush`, `test_eval_scripted_lpush_wakes_blocked_blpop`, `test_xread_block_satisfied_by_xadd`, `test_xreadgroup_block_satisfied_by_xadd` |
| Rulings | — |

## TR-BLOCKING-007 — wake-by-push: popped waiter's deadline already elapsed at re-validation (no restore)

| Field | Value |
| --- | --- |
| Precondition | Same trigger as TR-BLOCKING-006, but the popped waiter's `deadline` has already elapsed (`deadline.is_some_and(|d| d <= now)`) by the time the fast-path re-validation runs, before `strat.satisfy` is ever called. |
| Postcondition | The loop `continue`s without calling `strat.satisfy` — no store data is consumed, so nothing needs restoring — and sends `entry.op.timeout_reply()` on `response_tx`, incrementing `BlockedTimeoutTotal` exactly as the GC tick (TR-BLOCKING-020) does for the same event; the waiter is not re-queued. The send is a real reply, not a drop: dropping the sender here would be indistinguishable at the coordinator from shard death, whose reply is now `-ERR shard unavailable` (FM-BLOCKING-004), and the select at `coordinator.rs` is `biased;` with `response_rx` first — so a dropped sender deterministically beats the connection's own deadline branch rather than losing a race to it. Whichever of the two lands first is a correctly-shaped timeout reply; the other is a no-op on a closed channel. |
| Source | `shard/blocking.rs` (deadline fast-path check, `timeout_reply` send and `continue`) |
| Forced by | unit-level (frogdb-core, `shard/blocking.rs`): `push_after_deadline_elapsed_does_not_consume_element`, `push_after_deadline_elapsed_replies_with_the_op_aware_nil` |
| Rulings | [issue 08](../.scratch/spec-gaps/issues/done/08-blocking-command-rows.md) (MAJ-11 amendment: drop-elimination) |

## TR-BLOCKING-008 — wake-by-push: delivery send fails after consuming data (restore)

| Field | Value |
| --- | --- |
| Precondition | Same trigger as TR-BLOCKING-006, the deadline fast-path (TR-BLOCKING-007) did not fire, `strat.satisfy` ran and consumed store data (popped the element(s)), but the subsequent `response_tx.send(reply)` fails — the receiver was already dropped (connection gave up via timeout/disconnect/unblock in the same window). |
| Postcondition | The popped element(s) are restored to the store in original order via `apply_restore`, at the correct end for the op; `BlockedClients` gauge is re-set to the current waiter count; no replication propagation is recorded for this waiter (the `propagate` command computed by `satisfy` is discarded, never pushed); no keyspace events are emitted; no cascade to a BLMove destination runs; the waiter is not re-queued (it was already popped off the slab before the send attempt). |
| Source | `shard/blocking.rs:271-390` (`drive_satisfaction_body`), `:377-385` (`Err(_)` arm: `apply_restore` + `BlockedClients::set`), `:426-485` (`apply_restore`) |
| Forced by | unit-level (frogdb-core, `shard/blocking.rs`, not covered by this spec's `-p frogdb-server` test-resolution profile): `push_after_receiver_dropped_does_not_lose_element`, `blmpop_restore_preserves_all_elements_in_order`, `bzpopmin_restore_preserves_member_and_score`, `doomed_waiter_records_no_propagation` |
| Rulings | — |

## TR-BLOCKING-009 — wake by deadline elapsing (timeout), non-WAIT parked ops

| Field | Value |
| --- | --- |
| Precondition | A registered (TR-BLOCKING-001) parked waiter's deadline elapses on the connection-side coordinator's own `timeout_fut` (the canonical timer — see the "Timeout authority" state-space row) before shard delivery or `CLIENT UNBLOCK`; the shard's 100ms GC tick (TR-BLOCKING-020) is a backstop, not this row's trigger. |
| Postcondition | Connection observes `WaitOutcome::Timeout`; op-aware nil reply per FM-BLOCKING-002; connection's blocked-flag cleared; `UnregisterWait` sent and reconciled (see TR-BLOCKING-015) before the reply is returned; if the GC tick reclaimed the entry first instead (TR-BLOCKING-020), its `entry.op.timeout_reply()` send is a no-op when the coordinator's own deadline branch already replied — exactly one of the two ever lands on an open channel. |
| Source | `connection/blocking/coordinator.rs:78-111` (`wait_for_response`, deadline branch at `:109`), `connection/blocking.rs:134-156` (`cleanup_wait`); GC backstop at `shard/event_loop.rs:31` (100ms interval), `:83-85` (biased tick calling `check_waiter_timeouts`); `shard/blocking.rs:182-211` (`check_waiter_timeouts`, TR-BLOCKING-020) |
| Forced by | MISSING — no located test drives the shard GC tick or the coordinator's local deadline branch for a non-WAIT op to a bare timeout reply in isolation; `test_xread_block_timeout`/`test_xreadgroup_block_timeout` cover the stream op family only. |
| Rulings | — |

## TR-BLOCKING-010 — wake by CLIENT UNBLOCK, non-WAIT parked ops

| Field | Value |
| --- | --- |
| Precondition | A registered (TR-BLOCKING-001) parked waiter's connection receives a `CLIENT UNBLOCK` targeting it, before shard delivery or deadline. |
| Postcondition | Connection observes `WaitOutcome::Unblocked(mode)`; `UnblockMode::Timeout` → the op's ordinary timeout nil (same shape as TR-BLOCKING-009); `UnblockMode::Error` → `-UNBLOCKED client unblocked via CLIENT UNBLOCK`; connection's blocked-flag cleared; `UnregisterWait` sent and reconciled (TR-BLOCKING-015) exactly as the deadline path does — the shard-side entry does not know *why* the connection left, only that it did. See FM-BLOCKING-003. |
| Source | `connection/blocking/coordinator.rs:30-46` (`into_response`, `Unblocked` arms), the `ClientEdge::Unblocked` branch of `wait_for_response`; FM-BLOCKING-003 |
| Forced by | `test_client_unblock_not_blocked` covers the not-blocked precondition only; no located test drives a genuine parked non-WAIT `CLIENT UNBLOCK` release end-to-end. |
| Rulings | — |

## TR-BLOCKING-011 — WAIT settles via the replication decision (quorum reached or deadline elapsed)

| Field | Value |
| --- | --- |
| Precondition | A parked WAIT (TR-BLOCKING-005) is resolved by `wait.wait_for_replicas(...)` rather than by a `ClientEdge` from the client-signal seam, in `resolve_wait_race`'s biased select. |
| Postcondition | `WaitVerdict::Reached(count)`/`WaitVerdict::TimedOut(count)` → `Response::Integer(count)`; client-registry blocked mirror cleared. Unlike non-WAIT ops (TR-BLOCKING-009), there is no separate shard-side GC tick or `UnregisterWait` reconciliation: `wait_for_replicas` owns both the ack count and the deadline internally. |
| Source | `connection/blocking.rs:319-347` (`resolve_wait_race`); `replication/src/wait_coordinator.rs:214-256` (`wait_for_replicas`, internal deadline/quorum race), `:182-189` (`count_acked`) |
| Forced by | `test_wait_blocks_until_ack`, `test_wait_zero_timeout_blocks_until_ack`, `test_wait_numreplicas_exceeds_actual_blocks_to_timeout`, `test_wait_zero_timeout_blocks_without_quorum` |
| Rulings | — |

## TR-BLOCKING-012 — WAIT settles via CLIENT UNBLOCK while parked

| Field | Value |
| --- | --- |
| Precondition | A parked WAIT (TR-BLOCKING-005) is resolved by a `CLIENT UNBLOCK` targeting the connection, racing ahead of `wait.wait_for_replicas(...)` in `resolve_wait_race`'s biased select. |
| Postcondition | `Some(UnblockMode::Error)` → `-UNBLOCKED client unblocked via CLIENT UNBLOCK`; any other unblock mode → `Response::Integer(count_acked())`, the ack count as of the unblock instant (not re-raced against a late-arriving quorum). Client-registry blocked mirror cleared. |
| Source | `connection/blocking.rs:319-347` (`resolve_wait_race`, unblock arm) |
| Forced by | `test_client_unblock_releases_wait`; unit-level: `client_unblock_error_releases_a_parked_wait`, `client_unblock_timeout_mode_reports_the_acked_count`, `a_reached_quorum_beats_a_client_unblock_in_the_same_poll` |
| Rulings | — |

## TR-BLOCKING-013 — client disconnects while parked

| Field | Value |
| --- | --- |
| Precondition | A connection with blocked-flag = `Some(shard_id, keys)` closes (any reason) before its wait resolves — including a close raised *while the wait is parked*, which the wait's own socket branch observes as EOF (see the "Parked-wait supervision" state-space row). |
| Postcondition | The parked wait resolves as `WaitOutcome::ConnectionEnded(ParkedExit::PeerGone)`: the response channel is closed *first* (so a serve still in flight fails its send and the shard restores the popped data per TR-BLOCKING-008), the blocked-flag is deliberately left set, and the run loop terminates the connection. `notify_connection_closed` then sends `BlockingMsg::UnregisterWait{conn_id, ack}` and discards the ack (no client remains to hand a raced serve back to); the shard's `wait_queue.unregister(conn_id)` removes every entry for that connection from every key it was registered under. No reply is written — the peer is gone. |
| Source | `connection/blocking/coordinator.rs` (`PeerLiveness`, `ParkedExit::PeerGone`), `connection/blocking.rs` (`SocketWatch`, `handle_blocking_wait`'s `ConnectionEnded` arm — the one exit that skips `end_block()` so the teardown branch below stays live), `connection/lifecycle.rs` (`notify_connection_closed`), `shard/wait_queue.rs` (`unregister`); see FM-BLOCKING-009, FM-BLOCKING-011 |
| Forced by | `unregister_after_disconnect_clears_every_key_of_a_multi_key_wait`; unit-level (frogdb-server, `blocking/coordinator.rs`): `peer_gone_while_parked_ends_the_connection`, `response_beats_a_peer_that_left_in_the_same_poll`; integration: `disconnect_while_parked_releases_the_waiter_and_leaves_the_push_in_the_store` |
| Rulings | [issue 08](../.scratch/spec-gaps/issues/done/08-blocking-command-rows.md) (rows the shard-side half as FM-BLOCKING-009 and forces it; the run-loop restructure stays with issue 13); [issue 13](../.scratch/spec-gaps/issues/done/13-blocking-wait-becomes-a-run-loop-state.md) (CRIT-4: makes the postcondition reachable by supervising the socket for the whole park) |

## TR-BLOCKING-014 — shard dies while a waiter is parked

| Field | Value |
| --- | --- |
| Precondition | A waiter that completed registration (TR-BLOCKING-001, a live `WaitEntry` exists in `ShardWaitQueue`) has its `response_tx` dropped by genuine shard-side death — shard task shutdown/panic/teardown tearing down the whole `ShardWaitQueue` — while the connection is still parked in `wait_for_response`. This is now the *only* in-shard path that drops a sender without sending: the admission refusal (FM-BLOCKING-006), the deadline fast-path (TR-BLOCKING-007) and the `Satisfaction::Retry` arm each send a real reply, which is precisely what makes `Err(_)` mean shard death and lets this row name it. One neighbouring case remains outside the row: a closed CLIENT-UNBLOCK signal channel (`ClientEdge::Unblocked(None)`) is a registry/connection-side failure unrelated to shard health, still resolves to `Response::Null`, and has no dedicated row. |
| Postcondition | `-ERR shard unavailable`, distinguishable from the FM-BLOCKING-002/003 nil/timeout family, matching `txn.md` FM-TXN-032's vocabulary for the same underlying event (channel closed, never accepted). Connection's blocked-flag cleared; the wait entry is gone (channel closure implies no further shard-side bookkeeping references it). |
| Source | `connection/blocking/coordinator.rs` (`wait_for_response`, `Err(_)` arm), FM-BLOCKING-004 |
| Forced by | `channel_drop_yields_shard_unavailable_error` |
| Rulings | [issue 08](../.scratch/spec-gaps/issues/done/08-blocking-command-rows.md) (H5, ordered behind the MAJ-11 drop-elimination) |

## TR-BLOCKING-015 — serve races the chosen timeout/unblock (reconciliation)

| Field | Value |
| --- | --- |
| Precondition | The coordinator has already committed to `Timeout`/`Unblocked` when the shard concurrently pops and sends a value for the same waiter. |
| Postcondition | `reconcile_unregister` sends `UnregisterWait` and awaits the ack; `UnregisterAck::Unregistered` → the timeout/unblock reply stands, nothing was consumed; `UnregisterAck::AlreadyServed` → the raced value is drained from the still-open `response_rx` and delivered instead, overriding the tentative timeout/unblock outcome. |
| Source | `connection/blocking.rs:174-193` (`reconcile_unregister`), `:358-369` (`reconcile_ack`); `shard/message.rs:545-554` (`UnregisterAck`); see FM-BLOCKING-005 |
| Forced by | `already_served_drains_the_raced_value`, `unregistered_keeps_the_timeout_reply`, `closed_ack_channel_keeps_the_timeout_reply`, `already_served_with_closed_response_channel_falls_back`; also (not independently cited per the note below the preamble) the shuttle model in `frogdb-server/crates/core/tests/concurrency.rs`. |
| Rulings | — |

## TR-BLOCKING-016 — demotion while a waiter is parked (non-WAIT ops)

| Field | Value |
| --- | --- |
| Precondition | This node stops being the primary for the shard a waiter is parked on (`RoleManager::demote`, from `REPLICAOF` or a failover). |
| Postcondition | Every parked waiter on every shard of the demoted node is released with `ROLE_CHANGED_UNBLOCK_ERR` (Redis `disconnectAllBlockedClients`), never served from data arriving over the replication stream — serving from replicated data would be a replica-side write, diverging the replica's store from its own replication stream. The release rides each shard's serial mailbox and is enqueued before the inbound stream is opened, so a replicated write cannot overtake it; the wait queue is empty and `BlockedClients` is 0 on every shard afterwards. `WAIT`'s analogue is the fence (TR-BLOCKING-017), which reports the same error text. See FM-BLOCKING-007. |
| Source | `server/role_manager.rs` (`demote` → `BlockedWaiterFence::release_all_on_demotion`, `ShardWaiterFence`); `shard/dispatch_blocking.rs` (`BlockingMsg::ReleaseAllWaiters`); `shard/blocking.rs` (`handle_release_all_waiters`); `shard/wait_queue.rs` (`drain_all`). The hazard this closes: `ReplicaCommandExecutor::apply_group`/`apply_single` (`replication-runtime/src/executor.rs`) dispatches replicated writes as ordinary `CoreMsg::Execute` with no role check, reaching `WriteEffectKind::WaiterSatisfaction` in `shard/post_execution.rs` — so without the release a parked BLPOP could be served by a replicated LPUSH on a now-replica shard. |
| Forced by | `demote_releases_every_parked_blocking_waiter`; unit-level (frogdb-core, `shard/blocking.rs`): `demotion_release_answers_every_waiter_and_empties_the_queue` |
| Rulings | [issue 08](../.scratch/spec-gaps/issues/done/08-blocking-command-rows.md) (H6) |

## TR-BLOCKING-017 — WAIT role-change while parked

| Field | Value |
| --- | --- |
| Precondition | This node's `role_fence` is bumped (demotion) while a `WAIT` is parked in `resolve_wait_race`, racing the client-signal seam. |
| Postcondition | `wait_fut` yields `WaitVerdict::RoleChanged(count)` → `Response::error(WAIT_ROLE_CHANGED_ERR)`, biased ahead of a same-poll `CLIENT UNBLOCK`. The fence is subscribed before the primary-role check in `handle_wait_command`, so a demotion landing between subscribe and check is still observed (see the `RoleFence` ordering note; this is the mechanism TR-BLOCKING-016 shows has no non-WAIT counterpart). |
| Source | `connection/blocking.rs:319-347` (`resolve_wait_race`), `:226-260` (`handle_wait_command`, fence-then-role-check ordering); `replication/src/wait_coordinator.rs:64-83` (`WaitVerdict`), `:96` (`RoleFence`) |
| Forced by | `wait_released_by_a_demotion_reports_the_role_change_even_if_client_unblock_races`; unit-level (frogdb-replication, `wait_coordinator.rs`): `role_change_releases_a_wait_parked_forever`, `role_change_releases_a_wait_with_a_deadline`, `a_fence_from_an_earlier_stint_does_not_release_a_later_wait`, `a_demotion_racing_the_role_check_still_releases_the_wait`, `a_tie_between_quorum_and_role_change_favors_role_changed_no_deadline`, `a_tie_between_quorum_and_role_change_favors_role_changed_with_deadline`, `one_fence_releases_every_wait_parked_before_it`; integration (in profile): `test_wait_unblocked_on_demotion`, `test_wait_unblocked_on_cluster_demotion` |
| Rulings | [issue 08](../.scratch/spec-gaps/issues/done/08-blocking-command-rows.md) (confirms this path already exists and is correct; rowed as a failure mode by FM-BLOCKING-010) |

## TR-BLOCKING-018 — slot migrates away while a waiter is parked (non-WAIT ops)

| Field | Value |
| --- | --- |
| Precondition | A slot containing keys a waiter is parked on migrates off this node; the cluster runtime's slot-migration-complete event reaches this shard as `ShardMessage::Cluster(ClusterMsg::SlotMigrated{slot, target_addr})`. |
| Postcondition | Every waiter for keys in that slot is drained from the queue (never served locally from a slot this node no longer owns): if `target_addr` is known, each gets `MOVED <slot> <host>:<port>` (IPv6 bracketed); if unknown, each gets `CLUSTERDOWN Hash slot <slot> not served`. The drain runs in ascending registration ordinal — oldest registered waiter answered first, across *all* the slot's keys — so a multi-key slot's replies are fair and deterministic rather than following `waiters_by_key`'s HashMap iteration order. Connection's blocked-flag is cleared on receipt. A dedicated counter (`BlockedMigrationMoved`) records only the MOVED case. |
| Source | `shard/dispatch_cluster.rs:8-10` (`ClusterMsg::SlotMigrated` dispatch to `handle_slot_migrated`); `shard/blocking.rs:118-168` (`handle_slot_migrated`); `wait_queue.rs` (`drain_waiters_for_slot`, ordinal-sorted); `shard/message.rs:776-779` (`ClusterMsg::SlotMigrated` field def); confirmed matching `website/src/content/docs/architecture/blocking.md` "Cluster Mode: Slot Migration Interaction" |
| Forced by | unit-level (frogdb-core, `shard/blocking.rs`): `slot_migrated_moved_brackets_ipv6`, `slot_migrated_moved_ipv4_plain`, `slot_migrated_without_a_known_target_replies_clusterdown`; drain order (frogdb-core, `shard/wait_queue.rs`): `slot_drain_answers_in_registration_order_across_keys`, `slot_drain_order_survives_hashmap_iteration_order`; integration: `test_blocking_command_during_migration_gets_moved` |
| Rulings | [issue 08](../.scratch/spec-gaps/issues/done/08-blocking-command-rows.md) (confirms this path, no code gap; rowed as a failure mode by FM-BLOCKING-008) |

## TR-BLOCKING-019 — stream waiter drain: the group is gone (`DrainNoGroup`)

| Field | Value |
| --- | --- |
| Precondition | A parked stream waiter's key stops backing a consumer group: the key is missing, or was lazily purged as expired, when `StreamSatisfaction::check_key` runs inside `drive_satisfaction_body`'s poll loop (`KeyReady::DrainNoGroup`). |
| Postcondition | Every **XREADGROUP** waiter on that key is drained and replied to with `NOGROUP No such consumer group '<group>' for key name '<key>'`, verbatim; the loop returns without attempting a normal serve for the rest of this pass. Plain **XREAD** waiters deliberately stay parked: a plain read needs no group, so its wait is still satisfiable — a later `XADD` recreating the key serves it (this asymmetry with TR-BLOCKING-022 is the point of splitting the two rows). |
| Source | `shard/blocking.rs` (`drive_satisfaction_body`'s `KeyReady::DrainNoGroup` arm, `drain_stream_waiters_with_error`), `wait_queue.rs` (`pop_oldest_xreadgroup_waiter`) |
| Forced by | unit-level (frogdb-core, `shard/blocking.rs`): `xreadgroup_missing_group_is_reject` forces the reply text; `a_deleted_stream_drains_xreadgroup_but_leaves_xread_parked` forces the asymmetry, including the later `XADD` that serves the surviving XREAD waiter. |
| Rulings | [issue 15](../.scratch/spec-gaps/issues/done/15-wrong-type-drain-covers-plain-xread-waiters.md) (`DrainNoGroup` keeps its XREADGROUP-only scope; the old merged row claimed "every waiter … is drained", which was false for both arms) |

## TR-BLOCKING-022 — stream waiter drain: the key is no longer a stream (`DrainWrongType`)

| Field | Value |
| --- | --- |
| Precondition | A parked stream waiter's key exists but holds a non-stream value — `SET s foo` over a stream, a `RENAME` landing another type on it — when `StreamSatisfaction::check_key` runs inside `drive_satisfaction_body`'s poll loop (`KeyReady::DrainWrongType`). |
| Postcondition | **Every** stream waiter on that key — XREAD *and* XREADGROUP — is drained and replied to with `WRONGTYPE Operation against a key holding the wrong kind of value`, verbatim; the loop returns without attempting a normal serve for the rest of this pass. A wrong-typed key makes every stream wait on it unsatisfiable, including a plain XREAD's, so leaving XREAD waiters parked created an *unleavable* blocked state: `XREAD BLOCK 0` has no deadline, and nothing re-signals the key as a stream. See [Redis deviations](#redis-deviations). |
| Source | `shard/blocking.rs` (`drive_satisfaction_body`'s `KeyReady::DrainWrongType` arm, `drain_stream_waiters_wrongtype`), `wait_queue.rs` (`pop_oldest_stream_waiter`) |
| Forced by | unit-level (frogdb-core, `shard/blocking.rs`): `a_wrong_typed_key_drains_plain_xread_waiters`, `a_wrong_typed_key_drains_xreadgroup_waiters` |
| Rulings | [issue 15](../.scratch/spec-gaps/issues/done/15-wrong-type-drain-covers-plain-xread-waiters.md) (distsys-review MAJ-12, ruled accept-plus: fix the semantics, not just the row) |

## TR-BLOCKING-020 — shard-side GC tick reclaims an expired waiter

| Field | Value |
| --- | --- |
| Precondition | The shard's 100ms waiter-timeout tick fires and finds a registered `WaitEntry` whose `deadline` has already elapsed, independent of whether the connection's own `wait_for_response` deadline branch has fired yet in that same window. The tick branch is **gated**, not unconditional: the `select!` arm is `waiter_timeout_interval.tick(), if timer_sweeps`, and `timer_sweeps = self.timer_sweeps_enabled()` is re-read every loop iteration. It folds to a constant `true` in a production build (the `shard-driver` seam is not compiled), but a **driven run** — turmoil simulations and the shard-driver harness, via `ShardWorker::set_driven_ticks(true)` — turns it off and takes over delivery: the sweep arrives as a queued `ShardMessage::DriveTick(TickKind::WaiterTimeout)` message, dispatched into the same `check_waiter_timeouts` through `drive_waiter_timeout_tick`. So the backstop is never *absent*, only re-sourced from the timer to the message queue, where it is totally ordered against commands instead of racing them (determinism audit R6). |
| Postcondition | `entry.op.timeout_reply()` is sent on `response_tx` (a no-op if the coordinator's own deadline branch already fired and the channel is gone/already observed) and `BlockedTimeoutTotal` is incremented; the entry is removed from the queue. This is the GC backstop for TR-BLOCKING-009, not an independent client-visible outcome — the client-visible reply is decided once, by whichever of the shard tick or the coordinator's local sleep resolves the race for that connection's `response_rx`/deadline pair. Identical whichever source fired it: the timer arm and the `DriveTick` route both call `check_waiter_timeouts`, so a driven run observes the same sweep, reply shape and counter as a timer-driven one. |
| Source | `shard/event_loop.rs:31` (100ms interval), `:40` (`timer_sweeps` re-read per iteration), `:93-95` (the gated `if timer_sweeps` tick arm calling `check_waiter_timeouts`), `:218-227` (`timer_sweeps_enabled`, the seam-gated twin and its production constant), `:437-443` (the `DriveTick` dispatch arm), `:475-483` (`drive_waiter_timeout_tick`); `shard/worker.rs:287-289` (`set_driven_ticks`); `shard/blocking.rs:212-252` (`check_waiter_timeouts`, `timeout_reply` send, `BlockedTimeoutTotal` increment) |
| Forced by | unit-level (frogdb-core, `shard/event_loop.rs`): `a_driven_run_gates_off_the_waiter_sweep_timer_and_sweeps_from_the_drive_tick` — asserts the gate flips with `set_driven_ticks`, then drives the tick as a queued message with no coordinator present at all, so the sweep is the only authority that can resolve the expired waiter (queue emptied, op-aware `timeout_reply()` delivered, `BlockedTimeoutTotal` = 1). End-to-end over the harness: `drive_tick_message_runs_the_waiter_timeout_sweep` (frogdb-shard-harness, `tests/shard_driver.rs`). |
| Rulings | [issue 26](../.scratch/spec-gaps/issues/done/26-distsys-review-minors-sweep.md) (MIN-9: the row cited the 100ms interval unconditionally, so a reader concluded the backstop is always armed; that omission was also half the reason `Forced by` read MISSING — isolating the sweep requires driving the tick, which requires the gate) |

## TR-BLOCKING-021 — `CLIENT KILL` against a parked client

| Field | Value |
| --- | --- |
| Precondition | `CLIENT KILL` (by id, addr, laddr, ...) matches a connection that is parked in a blocking wait. `kill_by_id`/the kill filter sets the connection's `kill_tx` watch only — it never forges a `CLIENT UNBLOCK`, so the kill must be observed by the wait itself. |
| Postcondition | The parked wait resolves as `WaitOutcome::ConnectionEnded(ParkedExit::Killed)`, on the same terms as TR-BLOCKING-013's peer-EOF exit: response channel closed first (a racing serve is restored, TR-BLOCKING-008), no reply written to the killed connection, blocked-flag left set so `notify_connection_closed` unregisters the waiter and releases the global and per-key waiter budgets, and the run loop terminates the connection. `CLIENT KILL` therefore drains parked clients, which `CLIENT UNBLOCK` cannot do (it resolves the wait but leaves the connection open, TR-BLOCKING-011). The kill signal is a level-triggered watch, so it is still observed if it lands between registration and the first poll. |
| Source | `client_registry/mod.rs` (`ClientHandle::killed_or_unblocked`, `ClientEdge`), `connection/blocking/coordinator.rs` (`ClientSignals`, kill arm, `ParkedExit::Killed`), `connection/blocking.rs` (`handle_blocking_wait`, `resolve_wait_race`); see FM-BLOCKING-012 |
| Forced by | unit-level (frogdb-server, `blocking/coordinator.rs`): `client_kill_while_parked_ends_the_connection`, `response_beats_a_kill_in_the_same_poll`; integration: `client_kill_terminates_a_parked_client_and_releases_its_waiter` |
| Rulings | [issue 13](../.scratch/spec-gaps/issues/done/13-blocking-wait-becomes-a-run-loop-state.md) (CRIT-5: before this row the coordinator had no kill branch at all, so `CLIENT KILL` reported success and did nothing, forever) |

## TR-BLOCKING-023 — a woken waiter's op has nothing to read (`Satisfaction::Retry`)

| Field | Value |
| --- | --- |
| Precondition | A write makes the key ready for the parked waiter's *kind* (`check_key` = `Yes`), the waiter is popped, and its op then produces nothing: a blocking `XREAD` whose `after_id` the stream has not reached yet, or an `XREADGROUP` whose only new entry an earlier waiter in the same satisfaction pass already consumed. Reachable only from `StreamSatisfaction`, whose readiness question (does the key hold a stream?) is weaker than its serve question (does it hold entries *this* waiter has not read?); the list/zset arms would need `check_key`'s non-emptiness answer to be stale, which the shard's serial thread forbids. |
| Postcondition | The waiter goes back into the queue unchanged and stays parked: same `response_tx` (nothing is sent — no nil, no error, and the channel is not dropped, which would read as shard death, FM-BLOCKING-004), same deadline, same registration ordinal, and at the head of each of its keys' deques, i.e. the position it was popped from — so per-key FIFO wake order and slot-drain order are both unchanged, and a later write can still serve it. The satisfaction pass continues with the next waiter; the requeue happens once the pass ends, so a waiter cannot be popped and re-popped forever within one pass. A pass that ends in a stream drain (TR-BLOCKING-019/022) requeues *first*, so the drain covers the retried waiter rather than leaving it parked on a key it just declared unusable. |
| Source | `shard/blocking.rs` (`drive_satisfaction_body`'s `Satisfaction::Retry` arm, `requeue_retried_waiters`); `shard/wait_queue.rs` (`pop_oldest_waiter_of_kind_with_seq`, `requeue_retry`, `Placement::Head`) |
| Forced by | `a_stream_waiter_the_new_entry_does_not_reach_stays_parked`, `a_retried_stream_waiter_is_served_by_a_later_write`, `a_retried_waiter_keeps_its_deque_position_and_ordinal`, `a_retried_waiter_is_still_covered_by_a_wrong_type_drain` |
| Rulings | [issue 19](../.scratch/spec-gaps/issues/done/19-satisfaction-retry-resolved-dead-or-reregister.md) (reachability settled *reachable* — the stream arms; the previous behavior answered an op-aware nil, i.e. a timeout the client never asked for. The three per-family `_ =>` kind-mismatch arms are proven dead — `pop_oldest_waiter_of_kind` filters on the same op set the strategy matches — and are now `unreachable!()` rather than a silent `Retry`) |

## TR-BLOCKING-024 — a blocking command in a deny-blocking context resolves without parking

| Field | Value |
| --- | --- |
| Precondition | A blocking command (`BLPOP`/`BRPOP`/`BLMOVE`/`BRPOPLPUSH`/`BLMPOP`/`BZPOPMIN`/`BZPOPMAX`/`BZMPOP`/`XREAD BLOCK`/`XREADGROUP ... BLOCK`) executes where it cannot block: queued inside `MULTI`/`EXEC` (`shard/execution.rs`'s per-command loop) or called from a Lua script (`scripting/bindings.rs`). The handler finds no data and returns `Response::BlockingNeeded { keys, timeout, op }` exactly as it would on a real connection. |
| Postcondition | MULTI/EXEC: the `BlockingNeeded` signal is converted in place to `op.timeout_reply()` — the same op-aware nil shape FM-BLOCKING-002 defines for a real timeout (null array for the array-returning family, null bulk for `BLMove`) — never the always-scalar `Response::Null` the conversion used before this row. No `WaitEntry` is registered (the shard never reaches `ShardWaitQueue::register`) and no blocked flag is set (the connection-side conversion happens inside the transaction's synchronous command loop, never through `handle_blocking_wait`). Lua: `internal_action_to_lua` maps every `BlockingNeeded` to `Boolean(false)`, which is correct as-is — Lua flattens both nil shapes to a single `false`, so there is no shape bug on that path and it is out of scope for this row. |
| Source | `frogdb-core/src/shard/execution.rs` (`execute_transaction`'s per-command conversion); `frogdb-protocol/src/response.rs` (`BlockingOp::timeout_reply`, the op-aware shape shared with `frogdb_types::types::BlockingOp::timeout_reply`); `frogdb-core/src/scripting/bindings.rs:184` (`internal_action_to_lua`, unchanged) |
| Forced by | `deny_blocking_blpop_in_multi_replies_null_array`, `deny_blocking_bzpopmin_in_multi_replies_null_array`, `deny_blocking_xread_block_in_multi_replies_null_array`, `deny_blocking_blmove_in_multi_replies_null_bulk`; `test_wait_inside_multi_nonzero_timeout_does_not_block` (WAIT's sibling non-parking pin — WAIT never produces `BlockingNeeded` at all, so it does not exercise this row's conversion, but it pins the same "never actually blocks inside MULTI" contract this row states for the pop/stream family) |
| Rulings | [issue 16](../.scratch/spec-gaps/issues/done/16-deny-blocking-context-returns-op-aware-nil.md) (distsys-review MAJ-13: the conversion collapsed every op to `Response::Null`, a third instance of the wrong-shape family FM-BLOCKING-002 polices, on a path FM-BLOCKING-002's `into_response` fix does not reach) |

## TR-BLOCKING-025 — a blocking command issued during CLIENT PAUSE parks immediately

| Field | Value |
| --- | --- |
| Precondition | A blocking command (`BLPOP`/`BRPOP`/`BLMOVE`/`BRPOPLPUSH`/`BLMPOP`/`BZPOPMIN`/`BZPOPMAX`/`BZMPOP`/`XREAD BLOCK`/`XREADGROUP ... BLOCK`) is dispatched while a node-global `CLIENT PAUSE` (`ALL` or `WRITE`) is active and no slot-scoped barrier covers it (TR-BLOCKING-025 does not apply when one does — see the `slot_pause_blocks_command` clause below). |
| Postcondition | `wait_if_paused` (`connection/lifecycle.rs`) returns immediately instead of entering its pause poll-loop: the client is never marked `PAUSED`, and dispatch proceeds straight into the command's own `execute()`. What the bypass buys is *parking*, never *popping*: a write-flagged blocking command that finds its data already present still refuses the immediate pop and parks — see TR-BLOCKING-026, which owns that case. Either way it registers a real wait-queue entry (`handle_blocking_wait`) with the `BLOCKED` flag set and a deadline measured from *this* instant, independent of the pause window: `blocked_clients`/`CLIENT LIST` reflect the waiter immediately, `CLIENT UNBLOCK` succeeds on it immediately (the client is genuinely `BLOCKED`, not `PAUSED` — `ClientRegistry::unblock` checks `PAUSED` first and denies, `BLOCKED` second and allows), and the waiter times out on its own deadline even though the pause is still active. The write that would satisfy such a waiter (e.g. `LPUSH`) is not itself blocking-capable, so it does not bypass: it queues behind the same gate, and the parked waiter is only actually served once the pause lifts and the write runs. `slot_pause_blocks_command` scopes this bypass to the node-global dimension alone: a slot-scoped pause (the migration finalization barrier — TR-BLOCKING/FM-CLUSTER-079 family) still holds the command back even when it would otherwise resolve synchronously, so a blocking command's immediate-pop path cannot write through an armed handoff barrier (`ReplicaFeedGate`/FM-CLUSTER-097). |
| Source | `frogdb-server/crates/server/src/connection/lifecycle.rs` (`wait_if_paused`, `is_blocking_capable_command`, `slot_pause_blocks_command`, `slot_pause_mode`) |
| Forced by | `tcl_blocking_timeout_following_pause_should_honor_the_timeout` (`redis-regression/tests/list_tcl.rs`), `tcl_client_unblock_works_for_blocking_commands_during_pause`, `tcl_blocking_satisfaction_still_respects_write_pause` (`redis-regression/tests/pause_tcl.rs`), `blocking_command_still_parks_on_a_slot_barrier_despite_the_pause_bypass` (`frogdb-server/tests/cluster_pause_barrier.rs`, the slot-scoping half) |
| Rulings | [issue 17](../.scratch/spec-gaps/issues/done/17-client-pause-honors-blocking-deadlines.md) (distsys-review MAJ-14, ruled *adopt Redis semantics*: "Blocking timeout following PAUSE should honor the timeout" — see [Redis deviations](#redis-deviations) for how FrogDB's mechanism differs from Redis's own `blockPostponeClient`/`BLOCKED_POSTPONE`, in service of the same user-visible contract) |

## TR-BLOCKING-026 — a data-present blocking pop parks rather than popping through a node-global pause

| Field | Value |
| --- | --- |
| Precondition | A *write-flagged* blocking command (`BLPOP`/`BRPOP`/`BLMOVE`/`BRPOPLPUSH`/`BLMPOP`/`BZPOPMIN`/`BZPOPMAX`/`BZMPOP`/`XREADGROUP … BLOCK` reading `>`) reaches its handler through TR-BLOCKING-025's bypass while a node-global `CLIENT PAUSE` (`ALL` or `WRITE`) is still armed, **and the data it would pop is already present**. The invocation is a real client's, not the replica apply path (`REPLICA_INTERNAL_CONN_ID`). |
| Postcondition | The handler never runs its immediate-availability scan: it returns `Response::BlockingNeeded` with exactly the keys, op and timeout the no-data path would have returned (one `park` construction site), so the wait queue cannot tell the two apart. The store is not touched — nothing is popped, no keyspace event fires, no version is bumped, no WAL record is written, nothing replicates — for as long as the pause holds, which is the whole point: a pop is a write and may not cross the drain window the pause exists to open. The waiter is an ordinary one with an ordinary *live* deadline measured from dispatch (TR-BLOCKING-025), so a pause that outlasts the deadline yields the ordinary FM-BLOCKING-002 op-aware nil rather than a late pop — deliberately not Redis's `BLOCKED_POSTPONE`, which restarts the deadline after `CLIENT UNPAUSE` (see [Redis deviations](#redis-deviations)). Once the published pause deadline lapses, the shard's own 100 ms blocking sweep (not a later write — under a pause there may never be one) runs a satisfaction pass over every parked key and serves whichever waiters the still-present data can satisfy, then runs the canonical write-effect pipeline once, over the keys actually served, through a synthetic `PausedPopServe` record: version bump, tracking invalidation, dirty counter, WAL persistence and the flush of `pending_serve_propagations` all happen exactly as on the write-woken path, so a post-pause serve can never be delivered-but-unpersisted or delivered-but-unreplicated. Keys the pass served nothing from contribute no record, so no key that was not written observes a version bump or a WAL entry. Read-only blocking (`XREAD BLOCK`) is out of scope — it is not a write and never was gated. The slot-scoped barrier path is untouched: `slot_pause_blocks_command` still holds a blocking command at the connection (TR-BLOCKING-025), and this row's gate sees only the node-global dimension. |
| Source | `frogdb-core/src/client_registry/mod.rs` (`NodeWritePauseGate`, `PauseState::node_pause_until`, `publish_pause_derived_state` — a published *deadline*, never a latch, because nothing re-sweeps an idle node's pause and a stale latch would wedge the deferred pops forever); `frogdb-server/src/server/shards.rs` (`set_node_write_pause_gate` wiring); `frogdb-core/src/shard/worker.rs` (`ShardWorker::blocking_pop_paused`, `pops_deferred_by_pause`); `frogdb-core/src/shard/execution.rs` (`execute_command_body` stamps `CommandContext::blocking_pop_paused`); `frogdb-commands/src/blocking.rs` (`pop_paused`, `park`); `frogdb-commands/src/stream/read.rs` (`XREADGROUP`'s `>` branch); `frogdb-core/src/shard/blocking.rs` (`resume_pops_deferred_by_pause`, `PausedPopServe`); `frogdb-core/src/shard/event_loop.rs` (the `waiter_timeout_interval` sweep) |
| Forced by | `tcl_a_data_present_blocking_pop_waits_out_the_write_pause`, `tcl_a_data_present_blocking_pop_still_times_out_on_its_own_deadline` (`redis-regression/tests/pause_tcl.rs`) |
| Rulings | [issue 30](../.scratch/spec-gaps/issues/done/30-blocking-immediate-pop-escapes-node-global-write-pause.md) (ruling R9: the issue-17 bypass defended only half the window — the parking half — and let the immediate-pop half write straight through a pause armed for failover drain. Ruled *gate the pop at the shard-side decision point, keep one deadline regime*: the connection cannot make this call without racing the very writes being drained, and a second, pause-relative deadline regime — Redis's — is the thing the issue-17 deviation exists to avoid) |

## How to read a row

Fields as in [txn.md](txn.md#how-to-read-a-row). `Outcome variant`
here names the `WaitOutcome` variant the coordinator settles on, the blocking-path analogue of the
txn spec's transaction outcome.

Test names are bare function names, resolved against `scripts/spec-lint.py`'s
`NEXTEST_CRATES` listing (core command profile), which covers `frogdb-core` and
`frogdb-server` among others. That listing profile does **not** build
`frogdb-shard-harness` or anything behind `--features turmoil`/`--features shuttle`, so the
following genuinely load-bearing guards exist but are deliberately not cited in `Forced by`
rows:

- `frogdb-server/crates/core/tests/concurrency.rs`, module "Blocking-pop exactly-once conservation
  (serve-vs-timeout race)" — the shuttle model of the shard-side handshake, including two
  `#[should_panic]` sensitivity witnesses.
- `frogdb-server/crates/server/tests/concurrency_workload.rs` (turmoil) — the whole-history
  `check_exactly_once_delivery` / `check_fifo_wake_order_exact` conservation checkers over generated
  seed sweeps — the latter judges *per-key* serve order against the deque authority above, not the
  registration ordinal, which only orders slot drains — plus
  `regressions::regression_drain_capture_race_multiwaiter_ops_110_seed_0`.

---

## FM-BLOCKING-001 — shard delivers a value

| Field | Value |
|---|---|
| Trigger | The shard pops an element for this parked waiter and sends it on the wait's `oneshot` response channel before the deadline elapses and before any `CLIENT UNBLOCK`. |
| Observable | The shard's `Response` verbatim — for `BLPOP` the 2-element `[key, element]` array, for `BLMOVE` the moved element. |
| NOT observable | A timeout reply while the value was in flight; the element also remaining in (or being restored to) the source key; the same element being handed to a second waiter. This is the exactly-once half: delivered XOR retained, never both, never neither. |
| Invariant | `wait_for_response` is `biased` with the response branch first, so a value that becomes ready in the same poll as an already-elapsed deadline still wins. The receiver is *borrowed*, not consumed, so a value the shard sends inside the pop→deliver window remains drainable by `cleanup_wait` (see FM-BLOCKING-005). |
| Outcome variant | `Response(_)` |
| Forced by | `response_wins`, `biased_response_beats_elapsed_deadline` |
| Bug refs | none |

## FM-BLOCKING-002 — deadline elapses with nothing delivered

| Field | Value |
|---|---|
| Trigger | A finite blocking timeout expires with the waiter still parked: no shard delivery, no `CLIENT UNBLOCK`. |
| Observable | The op-aware nil: a null **array** for array-returning ops (`BLPOP`, `BRPOP`, `BZPOPMIN`, `BZPOPMAX`, `XREAD BLOCK`), a null **bulk** for single-value ops (`BLMOVE`, `BRPOPLPUSH`). |
| NOT observable | One nil shape for both op families (the wrong-shape bug `into_response` exists to fix); an element being consumed from any key; the waiter staying registered after the reply; an error reply — a timeout is never reported as the shard-death `-ERR shard unavailable` of FM-BLOCKING-004, which is why no shard-side path may signal a timeout by dropping the response sender (TR-BLOCKING-007, TR-BLOCKING-020); an effective wait longer than the command's own requested timeout because a concurrent `CLIENT PAUSE` was active — TR-BLOCKING-025's bypass keeps the deadline running independent of the pause window, so `BLPOP k 1` under `CLIENT PAUSE 60000 WRITE` still times out at ~1s, not ~60s. |
| Invariant | `WaitOutcome::Timeout` carries no data, and `into_response` re-derives the shape from the surviving `BlockingOp`, so timing out is a pure no-op against the store. Every shard-side resolution *sends* its reply, so the coordinator's `Err(_)` arm is reserved for FM-BLOCKING-004 alone. |
| Outcome variant | `Timeout` |
| Forced by | `timeout_wins_when_idle`, `timeout_reply_picks_nil_shape_per_op`, `push_after_deadline_elapsed_replies_with_the_op_aware_nil` |
| Bug refs | none |

## FM-BLOCKING-003 — CLIENT UNBLOCK targets the parked connection

| Field | Value |
|---|---|
| Trigger | `CLIENT UNBLOCK <id>` (or `… ERROR`) fires on a connection parked in a blocking wait, before any shard delivery or the deadline. |
| Observable | `ERROR` mode: `-UNBLOCKED client unblocked via CLIENT UNBLOCK`. `TIMEOUT` mode: exactly the FM-BLOCKING-002 reply, op-aware nil shape and all. |
| NOT observable | A delivered element (unblocking must never consume); a bare `Response::Null` standing in for the array-shaped nil; the shard-death `-ERR shard unavailable` of FM-BLOCKING-004 standing in for either unblock reply; the unblock being swallowed while the wait had no deadline (an infinite `BLPOP 0` must still be unblockable); `CLIENT UNBLOCK` answering `0` — "no such blocked client" — about a connection that is committed to blocking but has not finished registering. Handing `BlockWait` to the shard *awaits*, so that window is real whenever the shard channel is full, and a `0` there is a lie the operator acts on. |
| Invariant | The unblock branch is a peer of the response and deadline branches in the same `select!`, so it resolves even when `deadline == None` (which parks the timeout branch on `pending()` forever). The registry mirror is raised *before* the registration send and lowered again if that send fails (`mirror_blocked_then_register`), so the flag `unblock` tests brackets the whole commitment rather than only the part after the shard answered — see the "Client-registry blocked mirror" state-space row for why the inverted window is the safe one. |
| Outcome variant | `Unblocked(UnblockMode::{Error,Timeout})` |
| Forced by | `unblock_wins_over_idle_wait`, `timeout_reply_picks_nil_shape_per_op`, `the_real_client_handle_reports_an_unblock_when_not_killed`, `client_unblock_inside_the_registration_window_finds_the_client_blocked`, `a_failed_registration_clears_the_mirror_it_set` |
| Bug refs | [issue 26](../.scratch/spec-gaps/issues/done/26-distsys-review-minors-sweep.md) (MIN-10) — the mirror was set after the `BlockWait` send, so an UNBLOCK inside the registration window replied `0` while the client went on to block for its full timeout |

## FM-BLOCKING-004 — response channel closed without a value

| Field | Value |
|---|---|
| Trigger | The shard drops the wait's response sender without sending — shard shutdown or worker death. Every other shard-side resolution (satisfy, timeout fast-path, GC tick, admission refusal, demotion release, disconnect drain) *sends* a reply, so a closed channel means exactly one thing: the shard serving this key is gone. |
| Observable | `-ERR shard unavailable`. The connection is released, not wedged. |
| NOT observable | The wait hanging forever on a dead channel; a panic on `RecvError`; a fabricated element; a nil of either shape — shard death is distinguishable from the FM-BLOCKING-002 timeout, so a client cannot mistake a dead shard for "no data arrived" and retry into it forever. Matching `txn.md` FM-TXN-032's vocabulary for the same underlying event. |
| Invariant | `Err(_)` from the borrowed receiver maps to `WaitOutcome::Response(Response::error("ERR shard unavailable"))`. The mapping is only sound because sender-drop has been eliminated as a signaling mechanism: TR-BLOCKING-003 hands the entry back so the refusal reply can be sent, TR-BLOCKING-007 sends `timeout_reply()` on the deadline fast-path, and `Satisfaction::Retry` sends rather than drops. |
| Outcome variant | `Response(Response::Error("ERR shard unavailable"))` |
| Forced by | `channel_drop_yields_shard_unavailable_error` |
| Bug refs | [issue 08](../.scratch/spec-gaps/issues/done/08-blocking-command-rows.md) (H5 + MAJ-11 amendment: the three-row contradiction and the drop-elimination that had to land first) |

## FM-BLOCKING-005 — serve races the chosen timeout (pop→deliver window)

| Field | Value |
|---|---|
| Trigger | The coordinator has already chosen `Timeout`/`Unblocked` when the shard, concurrently, pops an element for this same waiter and sends it. `oneshot::send` returning `Ok` does **not** mean the client saw the value. |
| Observable | Whatever the shard's authoritative ack says: on `UnregisterAck::Unregistered` the timeout reply stands and nothing was consumed; on `UnregisterAck::AlreadyServed` the raced value is drained from the still-open receiver and delivered. |
| NOT observable | An element popped by the shard and dropped on the floor because the connection had moved on — the exact "neither delivered nor in final state" loss the conservation checkers report. |
| Invariant | The shard's serial mailbox is the single serialization point: `cleanup_wait` sends `BlockingMsg::UnregisterWait { ack }` and `reconcile_unregister` awaits the ack before deciding, and the coordinator borrows rather than consumes `response_rx` precisely so the `AlreadyServed` value is still there to drain. |
| Outcome variant | `Timeout` / `Unblocked(_)`, reconciled after the fact |
| Forced by | `already_served_drains_the_raced_value`, `unregistered_keeps_the_timeout_reply`, `closed_ack_channel_keeps_the_timeout_reply`, `already_served_with_closed_response_channel_falls_back` |
| Bug refs | `.scratch/testing-improvements/issues/07` |

> The cited four exercise `reconcile_ack` — the reconciliation decision extracted out of
> `reconcile_unregister` so it can be driven over in-memory channels without a live shard — across
> both `UnregisterAck` variants plus each channel-closure degradation. The end-to-end guards for
> this mode (the shuttle conservation model in `frogdb-server/crates/core/tests/concurrency.rs`
> with its `#[should_panic]` witnesses, and the turmoil sweep's `check_exactly_once_delivery`) live
> outside the listing profile above and so cannot be cited here.

## FM-BLOCKING-006 — registration refused at the waiter limit

| Field | Value |
|---|---|
| Trigger | `register_wait` reaches an admission bound: `max_blocked_connections` waiters already parked on this shard (default 50,000), or `max_waiters_per_key` already parked on one of the requested keys (default 10,000). |
| Observable | The refusal error on this connection's own response channel — `-ERR max blocked connections limit reached` for the shard-wide bound, `-ERR max waiters per key limit reached` for the per-key bound — and the connection unparks immediately rather than waiting out its timeout. |
| NOT observable | A refused registration that nonetheless leaves an entry in the wait queue (for the multi-key case: a partial registration under the keys checked before the offending one); the refusal arriving as `-ERR shard unavailable`, which would happen if the refusal path signalled by dropping the response sender (FM-BLOCKING-004); the bound being absent, i.e. a waiter admitted past either limit. |
| Invariant | The bounds are checked over *all* requested keys before any insertion, and `ShardWaitQueue::register` returns the entry inside `Err(RegisterRefused { entry, message })` so the caller still owns the `response_tx` it needs to answer with. Queue length and `waiter_count` are therefore unchanged on refusal. |
| Outcome variant | `Response(Response::Error(_))` |
| Forced by | `admission_refusal_at_the_global_limit_replies_and_registers_nothing`, `admission_refusal_at_the_per_key_limit_replies_and_registers_nothing` |
| Bug refs | [issue 08](../.scratch/spec-gaps/issues/done/08-blocking-command-rows.md) (H7: the bound was enforced in code and rowed nowhere, so a mutant deleting it survived the gate) |

## FM-BLOCKING-007 — the node is demoted while waiters are parked

| Field | Value |
|---|---|
| Trigger | `RoleManager::demote` runs on a node with parked non-`WAIT` waiters — `REPLICAOF <host> <port>`, a cluster-driven role change, or failover-induced demotion. |
| Observable | Every parked waiter on every shard is answered `-UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)` (`frogdb_core::ROLE_CHANGED_UNBLOCK_ERR`, the same text `WAIT` uses in FM-BLOCKING-010, matching Redis's `disconnectAllBlockedClients`); afterwards the wait queue is empty and `BlockedClients` is 0. |
| NOT observable | A waiter still parked after demotion completes; a waiter served *after* demotion from an element that arrived over the replication stream — serving one is a local write on a replica, diverging its store from the stream it is applying; a timeout nil or `-ERR shard unavailable` standing in for the role-change error. |
| Invariant | `demote` flips the replica fence, then fans `BlockingMsg::ReleaseAllWaiters` out through the `BlockedWaiterFence` seam to every shard mailbox, and only then starts the inbound replication stream. Because a shard's mailbox is serial, the release is ordered ahead of any replicated push that shard could receive, which is what makes "never served from replicated data" a fact about the ordering rather than a race the node usually wins. A mailbox already at capacity does not weaken that: the fence falls back to an awaited `send`, whose permit is handed out in FIFO order ahead of any message the not-yet-created replication stream could enqueue. |
| Outcome variant | `Response(Response::Error(ROLE_CHANGED_UNBLOCK_ERR))` |
| Forced by | `demote_releases_every_parked_blocking_waiter`, `demotion_release_answers_every_waiter_and_empties_the_queue` |
| Bug refs | [issue 08](../.scratch/spec-gaps/issues/done/08-blocking-command-rows.md) (H6.1: `WAIT` handled its own role change, non-`WAIT` waiters had no path at all) |

## FM-BLOCKING-008 — the slot a waiter is parked on migrates away

| Field | Value |
|---|---|
| Trigger | A slot holding keys this waiter is parked on finishes migrating off this node; the cluster runtime delivers `ClusterMsg::SlotMigrated { slot, target_addr }` to the shard. |
| Observable | Every waiter for a key in that slot is answered `-MOVED <slot> <host>:<port>` when the target is known (IPv6 hosts bracketed, so the reply parses), and `-CLUSTERDOWN Hash slot <slot> not served` when it is not. `BlockedMigrationMoved` counts only the MOVED case. |
| NOT observable | A waiter later served locally out of a slot this node no longer owns (the blocking analogue of `txn.md` FM-TXN-049); an unbracketed IPv6 `MOVED`; a waiter left parked on a departed slot until its own timeout; a drain order that depends on `waiters_by_key`'s HashMap iteration order (two waiters on different keys of the same slot are answered oldest-registration-first, both across runs and across hash seeds). |
| Invariant | `drain_waiters_for_slot` removes the entries before any reply is sent, returns them in ascending registration ordinal, and the redirect text is produced by the shared redirect seam rather than an inline `host:port` join. |
| Outcome variant | `Response(Response::Error(_))` — `MOVED` / `CLUSTERDOWN` |
| Forced by | `slot_migrated_moved_brackets_ipv6`, `slot_migrated_moved_ipv4_plain`, `slot_migrated_without_a_known_target_replies_clusterdown`, `slot_drain_answers_in_registration_order_across_keys`, `slot_drain_order_survives_hashmap_iteration_order`, `test_blocking_command_during_migration_gets_moved` |
| Bug refs | [issue 08](../.scratch/spec-gaps/issues/done/08-blocking-command-rows.md) (H6.2: the path existed and was correct, but was rowed only as a transition) |

## FM-BLOCKING-009 — the client disconnects while parked

| Field | Value |
|---|---|
| Trigger | The connection is torn down while a blocking wait is registered, so `notify_connection_closed` runs with the connection's blocked-flag still set. Both routes reach it: a close observed between commands, and a close observed *during* the park by the wait's own socket branch (TR-BLOCKING-013), whose `ConnectionEnded` exit leaves the blocked-flag set precisely so this teardown runs. |
| Observable | Nothing on the wire (the peer is gone). Server-side: the entry is gone from the shard's wait queue under *every* key it was registered on, the removal is acknowledged (`Unregistered` the first time, `AlreadyServed` if a serve raced it), `DEBUG WAITQUEUE` reports the queue empty again, and `BlockedClients` returns to its pre-block value. |
| NOT observable | A leaked wait entry that outlives its connection — one that a later push would try to serve, consuming an element for a client that cannot receive it (a conservation loss, not merely a leak); an entry cleared from the first of a multi-key wait's keys but left under the rest; a second teardown reported as a fresh removal rather than `AlreadyServed`. |
| Invariant | `notify_connection_closed` sends `BlockingMsg::UnregisterWait { conn_id }` to the shard, whose serial mailbox makes removal atomic against a concurrent push; removal iterates `entry.keys`, so multi-key waits leave no residue. |
| Outcome variant | none — the wait is torn down, not resolved |
| Forced by | `unregister_after_disconnect_clears_every_key_of_a_multi_key_wait`; end-to-end: `disconnect_while_parked_releases_the_waiter_and_leaves_the_push_in_the_store` |
| Bug refs | [issue 08](../.scratch/spec-gaps/issues/done/08-blocking-command-rows.md) (H6.3: `cleanup_wait` was in scope but nothing forced the shard-side unregistration); [issue 13](../.scratch/spec-gaps/issues/done/13-blocking-wait-becomes-a-run-loop-state.md) (CRIT-4: a close raised *during* the wait was previously unobservable, so this row's trigger could not be reached from a real socket — the parked wait now watches it, and FM-BLOCKING-011 pins the conservation half) |

## FM-BLOCKING-010 — `WAIT` is parked when the node's role changes

| Field | Value |
|---|---|
| Trigger | This node's `role_fence` is bumped by a demotion while a `WAIT` is parked awaiting acks, possibly in the same poll as a `CLIENT UNBLOCK` for that connection. |
| Observable | `-UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)` (`WAIT_ROLE_CHANGED_ERR`, which is `frogdb_core::ROLE_CHANGED_UNBLOCK_ERR`), taking priority over a same-poll `CLIENT UNBLOCK` and over a same-poll quorum verdict. |
| NOT observable | An ack count reported as satisfied by a node that is no longer the primary those acks were counted against; the `WAIT` staying parked past the demotion (including `WAIT numreplicas 0`, which parks forever); a fence from an *earlier* primary stint releasing a `WAIT` parked after it. |
| Invariant | The fence is subscribed *before* the primary-role check in `handle_wait_command`, so a demotion landing between subscribe and check is still observed; `WaitVerdict::RoleChanged` is biased ahead of the unblock and quorum branches in `resolve_wait_race`. Cross-referenced to `replication.md` FM-REPLICATION-040. |
| Outcome variant | `Response(Response::Error(WAIT_ROLE_CHANGED_ERR))` |
| Forced by | `wait_released_by_a_demotion_reports_the_role_change_even_if_client_unblock_races`, `test_wait_unblocked_on_demotion`, `test_wait_unblocked_on_cluster_demotion` |
| Bug refs | [issue 08](../.scratch/spec-gaps/issues/done/08-blocking-command-rows.md) (H6.4: `WAIT` is in this spec's scope line but had no row) |

## FM-BLOCKING-011 — a later push must survive a peer that died mid-park

| Field | Value |
|---|---|
| Trigger | A client parks in `BLPOP k 0` and its process dies (socket EOF, no `QUIT`). Some time later another connection runs `LPUSH k v`. |
| Observable | The push stays in the store: `LLEN k` is 1 and a subsequent `BLPOP k 0` from a live client receives `v`. The dead client's entry is gone from the wait queue (`DEBUG WAITQUEUE` empty) before the push arrives, and its global/per-key waiter budgets are back to their pre-block values. |
| NOT observable | The element being popped and written to a dead socket — the conservation loss FM-BLOCKING-005 declares impossible, which a leaked-but-alive `response_tx` would make *undetectable* because the send succeeds into an orphaned task and the `Err(_)` restore arm (TR-BLOCKING-008) never fires. Also not observable: the waiter surviving the disconnect and being served minutes later; the FD, task, and waiter budget leaking for the life of the process. |
| Invariant | The wait cannot outlive its socket: the parked wait polls the socket for the whole park, and its `ConnectionEnded` exit closes `response_rx` *before* the connection is torn down, so any serve that raced the teardown fails its send and is restored rather than lost. The narrow residual race — the shard's `send` landing between the coordinator's final response poll and the `close()` — delivers to a socket that is already gone, which is exactly Redis's behaviour when it serves a blocked client whose peer has died but whose EOF has not been read yet. |
| Outcome variant | `ConnectionEnded(ParkedExit::PeerGone)` |
| Forced by | `disconnect_while_parked_releases_the_waiter_and_leaves_the_push_in_the_store` |
| Bug refs | [issue 13](../.scratch/spec-gaps/issues/done/13-blocking-wait-becomes-a-run-loop-state.md) (distsys-review CRIT-4) |

## FM-BLOCKING-012 — `CLIENT KILL` must actually terminate a parked client

| Field | Value |
|---|---|
| Trigger | An operator drains a node: `CLIENT KILL ID <n>` targets a connection parked in `BLPOP k 0`. |
| Observable | `+OK`/`:1` from the killing connection *and* the killed connection's socket closes; its waiter is unregistered, so `DEBUG WAITQUEUE` reports the queue empty and the waiter budgets are released. A push to `k` afterwards is not consumed by the killed client. |
| NOT observable | `CLIENT KILL` reporting success while the parked connection stays open and registered forever — a node that cannot be drained. A killed connection receiving a reply for the wait it never finished. A kill delivered between registration and the wait's first poll being missed (the kill is a level-triggered watch, not an edge). |
| Invariant | The kill signal and the `CLIENT UNBLOCK` signal are raced as one seam (`ClientHandle::killed_or_unblocked`, because a `select!` cannot borrow the handle twice) with the kill biased first, so a connection that is both killed and unblocked in the same poll terminates rather than replying. |
| Outcome variant | `ConnectionEnded(ParkedExit::Killed)` |
| Forced by | `client_kill_terminates_a_parked_client_and_releases_its_waiter`; unit-level: `client_kill_while_parked_ends_the_connection`, `a_kill_beats_a_simultaneous_client_unblock` |
| Bug refs | [issue 13](../.scratch/spec-gaps/issues/done/13-blocking-wait-becomes-a-run-loop-state.md) (distsys-review CRIT-5) |

---

## FM-BLOCKING-013 — the waking write says nothing this waiter can read

| Field | Value |
|---|---|
| Trigger | The satisfaction pass pops this waiter because its key became ready for the kind, then its op yields no data — a blocking `XREAD` parked on an `after_id` past the stream's tail, or an `XREADGROUP` whose new entry an earlier waiter in the same pass consumed. |
| Observable | Nothing: the client stays blocked, and a later write that *does* produce data for it serves it normally, with the original deadline still governing the timeout. |
| NOT observable | A nil (or any other reply) delivered at the moment of the unrelated write — a timeout the client never asked for and cannot distinguish from a real one; a closed response channel, which the coordinator reads as shard death (FM-BLOCKING-004); the waiter losing its FIFO position to waiters registered after it. |
| Invariant | A popped-but-unsatisfied waiter is put back with its registration ordinal and deque position, and the requeue happens before any drain the same pass triggers, so no waiter is stranded on a key the pass has already declared unusable. |
| Outcome variant | none while retried — the wait resolves later through its own transition (serve, timeout, unblock, drain, kill, disconnect) |
| Forced by | `a_stream_waiter_the_new_entry_does_not_reach_stays_parked`, `a_retried_stream_waiter_is_served_by_a_later_write`, `a_retried_waiter_keeps_its_deque_position_and_ordinal`, `a_retried_waiter_is_still_covered_by_a_wrong_type_drain` |
| Bug refs | [issue 19](../.scratch/spec-gaps/issues/done/19-satisfaction-retry-resolved-dead-or-reregister.md) (MAJ-16: `Satisfaction::Retry` was an unrowed resolution — originally the entry was dropped with its channel, later an op-aware nil; both answer a client that asked to keep waiting) |

## Redis deviations

Deliberate, known differences from Redis 8.x blocking semantics. Each is pinned by the tests named
in the rows above, so a change here is a visible spec edit rather than a silent drift.

| Row | FrogDB | Redis | Rationale |
|---|---|---|---|
| TR-BLOCKING-022 | A key that stops being a stream drains **every** stream waiter on it, plain `XREAD` included, with `WRONGTYPE Operation against a key holding the wrong kind of value` | Only readgroup clients are unblocked (`unblockDeletedStreamReadgroupClients`), and with `-UNBLOCKED the stream key no longer exists`; a plain `XREAD BLOCK` client stays parked | Deviation-as-improvement: a plain `XREAD BLOCK 0` on a wrong-typed key can never be satisfied and has no deadline, so Redis's behaviour is an unleavable blocked state that only `CLIENT UNBLOCK`/`CLIENT KILL` can end. The error text stays `WRONGTYPE` — the same text the non-blocking `XREAD` on that key would produce, so a client sees one answer for one condition rather than a blocking-only error code. |
| TR-BLOCKING-019 | A key that disappears drains XREADGROUP waiters with `NOGROUP No such consumer group '<group>' for key name '<key>'` | `-UNBLOCKED the stream key no longer exists` | Same condition, and the same reasoning as above: `NOGROUP` is what the non-blocking `XREADGROUP` against a missing key replies, so the blocking and immediate paths agree. Plain `XREAD` waiters stay parked here in both systems — a missing key is still satisfiable by a later `XADD`. |
| TR-BLOCKING-025 | A blocking command issued during `CLIENT PAUSE` bypasses the pause gate directly into its normal blocked state, with the client visibly `BLOCKED` (`blocked_clients`, `CLIENT LIST`) and unblockable via `CLIENT UNBLOCK` from the moment it parks | `processCommand` intercepts *every* command ahead of dispatch, blocking commands included, and defers it as `BLOCKED_POSTPONE` (`blockPostponeClient`) — a generic postponement, not the command's own blocking mechanism. A postponed client is invisible as a distinct wait to `CLIENT UNBLOCK`/timeout (`blockedClientMayTimeout` returns 0 for `BLOCKED_POSTPONE`); only once `CLIENT UNPAUSE` reprocesses the command from scratch does it become a real `BLOCKED_LIST` wait with its own deadline | Deviation-as-improvement: Redis's mechanism still bounds the wait to roughly the command's own timeout (the motivating upstream test, "Blocking timeout following PAUSE should honor the timeout"), but only because reprocessing happens to start the deadline late rather than because the client was ever genuinely trackable as blocked — `blocked_clients`/`CLIENT LIST` read 0 for it the whole time it is postponed, and `CLIENT UNBLOCK` always answers 0. FrogDB's bypass gives the same bound on total wait while keeping the observability-accuracy invariant (CLAUDE.md) the postponement mechanism breaks: an operator pausing during failover can see and unblock every parked client, including ones that arrived after the pause. |
| TR-BLOCKING-026 | A write-flagged blocking command whose data is already there does **not** pop during a node-global pause: it parks as an ordinary waiter on its own live deadline, and is served by the shard's own sweep once the pause lapses (or answers the ordinary timeout nil if the pause outlasts it) | The same command is postponed by `blockPostponeClient` before dispatch and re-executed from scratch on `CLIENT UNPAUSE`, at which point it pops immediately and starts a fresh deadline. A `BLPOP k 1` issued 60s into a 60s pause therefore replies at ~unpause+0s in Redis and at ~1s in FrogDB | One deadline regime, not two. Redis's `BLOCKED_POSTPONE` buys the correct "no pop through the pause" outcome at the price of a second, pause-relative clock the client never asked for and cannot observe (`blockedClientMayTimeout` returns 0 while postponed) — the same accuracy break TR-BLOCKING-025 already deviates from. FrogDB keeps the single client-requested deadline and pays for it with the documented, coherent consequence: a pause longer than the timeout yields a nil, exactly as the client's own `timeout` argument asked. The pop itself is still refused, which is the property the pause exists to provide — no write escapes the drain window. |
