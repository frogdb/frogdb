# Blocking commands — failure modes

Every way a blocking command (`BLPOP`/`BRPOP`/`BLMOVE`/`BRPOPLPUSH`/`BLMPOP`/`BZPOPMIN`/
`BZPOPMAX`/`XREAD BLOCK`/`WAIT`) can be resolved on the connection side, one table per mode.
Same contract as [Transactions — failure modes](txn.md): a mutant that survives
is a row nothing forces.

Scope: the **connection-side** blocking-wait path — `handle_blocking_wait` /`register_wait` /
`cleanup_wait` / `reconcile_unregister` in
`frogdb-server/crates/server/src/connection/blocking.rs`, and the three-way race arbitration in
`frogdb-server/crates/server/src/connection/blocking/coordinator.rs`. Rows stop at what the
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
| Client-registry blocked mirror | `ClientRegistry::update_blocked_state(id, bool)` (`admin.client_registry`, called from `register_wait`/`cleanup_wait`, `connection/blocking.rs:115-117,142-144`; also set directly by `handle_wait_command` for WAIT, `:274-277,288-291`, with no paired shard registration) | Same call sites as the connection-side flag | In-memory only | No |
| Per-key wait FIFO | `ShardWaitQueue.waiters_by_key: HashMap<Bytes, VecDeque<usize>>` (`shard/wait_queue.rs:51-81`) | `register`/`unregister`/`pop_oldest_waiter*`/`drain_waiters_for_slot` | In-memory only | No |
| Wait-entry slab | `ShardWaitQueue.entries: Vec<Option<WaitEntry>>` + `free_slots`, `conn_entries: HashMap<u64, Vec<usize>>` (`wait_queue.rs:51-81`) | `register`/`unregister` | In-memory only | No |
| Registration ordinal | `ShardWaitQueue.next_seq: u64` (`wait_queue.rs:67`), `seq_by_slot: Vec<u64>` (`:71`) — the exact tie-break `register` stamps on every accepted entry; per-key FIFO order (what "How to read a row" and TR-BLOCKING-001/013 depend on) is this ordinal, not insertion order into `waiters_by_key` alone | `register` only; read by the wait-queue-log diagnostic and by slot-scoped ordering | In-memory only | No |
| Per-shard waiter count | `ShardWaitQueue.waiter_count: usize` (`wait_queue.rs:61`) | Incremented in `register`, decremented in `unregister`/pop paths | In-memory only | No |
| Admission limits | `ShardWaitQueue.max_waiters_per_key` (`wait_queue.rs:63`), `max_blocked_connections` (`:65`); `0` disables the corresponding check, `register`'s guards at `:144` (global) and `:149` (per-key) | Production default `BlockingConfig` (`frogdb-server/crates/config/src/blocking.rs:16-24`, `DEFAULT_MAX_WAITERS_PER_KEY = 10000`, `DEFAULT_MAX_BLOCKED_CONNECTIONS = 50000`), applied once via `ShardWorker::set_wait_queue_limits` (`worker.rs:515-521`) from `server/shards.rs:193` at shard construction; `ShardWaitQueue::new()`'s `with_limits(10000, 50000)` (`wait_queue.rs:116-118`) is only the `Default`/test path, not the production wire. `#[param(skip)]` on both config fields — not `CONFIG SET`-mutable; `set_wait_queue_limits` replaces the whole queue, so it is never called again after construction. | In-memory only, config-derived | No (re-derived from config at boot) |
| Per-waiter deadline | `WaitEntry.deadline: Option<tokio::time::Instant>` (`wait_queue.rs:13-33`; mirrored connection-side as the `deadline` local in `handle_blocking_wait`, `connection/blocking.rs:34-48`, also `tokio::time::Instant`) | Set once at registration from the command's timeout; never mutated | In-memory only | No |
| Timeout authority | Connection-side `BlockingWaitCoordinator::wait_for_response`'s `timeout_fut` (`connection/blocking/coordinator.rs:78-111`, the sleep at `:85-90`) is canonical/precise; shard-side `check_waiter_timeouts` (`shard/blocking.rs:182-211`, ticked every 100ms from `shard/event_loop.rs:31,83-85`) is a GC safety net only — it removes expired entries and best-effort sends `op.timeout_reply()` (a no-op if the server already replied and dropped the receiver), it never consumes store data | Server: `tokio::time::sleep_until(deadline)`; Shard: periodic `collect_expired(now)` | In-memory only | No |
| Wake token (response channel) | `WaitEntry.response_tx: oneshot::Sender<Response>` (`wait_queue.rs:13-33`), paired with the connection's borrowed `response_rx` | Sent once by `drive_satisfaction_body` (serve, `shard/blocking.rs:341`) or by `check_waiter_timeouts` (GC, `:198`); dropped without sending on admission refusal (`shard/blocking.rs:49-57`), the deadline fast-path skip (`:320-322`), or shard teardown; borrowed-not-consumed by the connection so a raced serve stays drainable | In-memory only | No |
| Serve-vs-timeout reconciliation state | `UnregisterAck::{Unregistered, AlreadyServed}` (`shard/message.rs:545-554`), decided by whether `wait_queue.unregister(conn_id)` still found the entry | Computed synchronously inside `handle_unregister_wait` from the shard's serial mailbox order | In-memory only, transient (one round trip) | No |
| WAIT quorum snapshot | `WaitCoordinator` (`replication/src/wait_coordinator.rs:114-127`): `offsets: Arc<OffsetCoordinator>` (target offset, snapshotted at WAIT call time via `target_offset()`, `:173-180`), `tracker: Arc<ReplicationTrackerImpl>` (ack counting via `count_acked`, `:182-189`) | `offsets`/`tracker` are shared, continuously-updated seams; the per-call target offset is a local snapshot, not stored state | In-memory only | No |
| WAIT role fence | `WaitCoordinator.role_fence: tokio::sync::watch::Sender<()>` (`wait_coordinator.rs:126`), observed via `RoleFence` (`:85-106`), taken by `role_fence()` (`:159-161`) | Bumped by `RoleManager::demote` (`server/role_manager.rs:362-403`, fence flip at `:371`) → `PrimaryReplicationHandler::end_primary_stint`; subscribed *before* the replica-role check in `handle_wait_command` (`connection/blocking.rs:253-256`, ordering is load-bearing, see `wait_coordinator.rs:85-95`) | In-memory only | No |
| Node role flag | `is_replica: Arc<AtomicBool>` (`RoleManager`, flipped `false→true` at `role_manager.rs:371` inside `demote`, before the fence's consequences propagate). Read directly (not through the fence) by `handle_wait_command`'s pre-parse rejection (`connection/blocking.rs:228`) and its post-fence re-check (`:254`) | `RoleManager::demote`/promote-equivalent path | In-memory only | No |
| CLIENT UNBLOCK signal | `UnblockSignal` trait, prod impl on `ClientHandle` (`connection/blocking/coordinator.rs:55-65`) | Set by the `CLIENT UNBLOCK` handler on another connection; consumed once by the parked wait's `select!` (non-WAIT: `coordinator.rs:104-107`; WAIT: `resolve_wait_race`, `connection/blocking.rs:338-345`) | In-memory only | No |
| Pending serve-propagation buffer | `ShardWorker.pending_serve_propagations: Vec<SynthesizedCommand>` (pushed at `shard/blocking.rs:359-361`) | `drive_satisfaction_body`, on every committed serve (never on a restored/rejected one); flushed to replicas by the terminal `ReplicationBroadcast` write effect, after the waking write's own broadcast | In-memory only | No |
| Blocking metrics | `BlockedClients` (gauge, set on register/unregister/serve/restore/GC — `shard/blocking.rs:68-72,163-167,205-209,380-384`), `BlockedSatisfiedTotal` (inc on a committed serve, via `record_blocked_waiter_satisfied`), `BlockedTimeoutTotal` (inc per GC-reclaimed entry, `:201`), `BlockedMigrationMoved` (inc only for the `-MOVED` case of slot migration, not `-CLUSTERDOWN`) | The shard worker, at each corresponding transition | In-memory only (exported, not restart-durable) | No |
| Demotion-triggered wait unblock | No authoritative field exists — `RoleManager::demote` (`server/role_manager.rs:362-403`) flips `is_replica`, ends the primary stint, and tears down the outbound stream, but never sends any shard message and never touches `ShardWaitQueue`; no `disconnectAllBlockedClients`-equivalent code path exists anywhere in `frogdb-core`/`frogdb-server`. `install_snapshot` (demoted-node store reset, `shard/dispatch_replication.rs`) likewise does not touch the wait queue. `WAIT` alone has a working mechanism (the `role_fence` row above); BLPOP/BRPOP/etc. parked waiters have no equivalent today — this is a declared gap, not an unresearched cell. | — (gap, not yet built) | — | — |

## Transitions

Rows below encode two kinds of semantics, distinguished when they diverge: rows with a plain
`Postcondition` describe what the code does today and what it is ruled to keep doing. Rows
split into `Postcondition (ruled)` / `Postcondition (current code)` describe a point where the
"## Ruling (2026-08-13)" section of
[issue 08](../.scratch/spec-gaps/issues/open/08-blocking-command-rows.md) — the ruling
authority for every row in this file — settled semantics the code does not yet implement; the
row's `Pending` field points back at that issue and a forcing test must not be written against
the `(current code)` cell.

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
| Postcondition | Connection blocked-flag = `Some(BlockedState{shard_id, keys})`; client-registry blocked mirror = true; a `WaitEntry` exists in the target shard's per-key FIFO for every key in the command, at the tail, ordered by `next_seq`; per-shard waiter count incremented; `BlockedClients` gauge set to the new count; per-waiter deadline set from `timeout` (`None` if `timeout == 0`, i.e. indefinite). |
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
| Postcondition (ruled) | An error reply reaches the client stating the limit was hit (`-ERR max blocked connections limit reached` global / `-ERR max waiters per key limit reached` per-key, global checked first), returned immediately — the connection never parks. Neither the per-shard waiter count nor any per-key FIFO is mutated by a refused registration. |
| Postcondition (current code) | `self.wait_queue.register(entry)` returns `Err(e)`; `handle_block_wait` logs the refusal at `warn` and drops `entry` — which owns `response_tx` — with **no send on it and no error propagated to the connection**. The connection is left parked in `wait_for_response`/`resolve_wait_race` with no shard-side registration at all; `response_rx` later observes `Err(_)` (the sender was dropped, not merely closed-without-send) and resolves to a bare `Response::Null` exactly as an ordinary timeout would (TR-BLOCKING-009), not the admission error the client should see. (The in-code comment at this site — "The client will timeout" — is itself inaccurate: the drop resolves `response_rx` immediately rather than waiting out the deadline; not itself a spec-observable fact, noted here because it explains why this was easy to miss.) |
| Source | `shard/wait_queue.rs:142-156` (`register`, admission checks), defaults at `wait_queue.rs:63,65` (`max_waiters_per_key`/`max_blocked_connections` fields), `:144` (global check), `:149` (per-key check); refusal handling at `shard/blocking.rs:49-57` (`handle_block_wait`); client-observed effect at `connection/blocking/coordinator.rs:99-101` (`wait_for_response`, `Err(_)` arm) |
| Forced by | MISSING ([issue 08](../.scratch/spec-gaps/issues/open/08-blocking-command-rows.md)) — no test exercises `max_waiters_per_key`/`max_blocked_connections` refusal at all; this is the current-code cell C1 exists to fix, tracked by the same issue as the `Pending` ruling. |
| Rulings | [issue 08](../.scratch/spec-gaps/issues/open/08-blocking-command-rows.md) (H7) |
| Pending | [issue 08](../.scratch/spec-gaps/issues/open/08-blocking-command-rows.md) |

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
| Postcondition | `RoleFence` subscription held for the duration of the wait; target offset snapshotted via `target_offset()`; ack solicitation sent once to streaming replicas; client-registry blocked mirror set directly (no shard-side `WaitEntry`, unlike TR-BLOCKING-001); the WAIT is parked on `wait.wait_for_replicas(...)` raced against `unblock.unblocked()` via `resolve_wait_race`. |
| Source | `connection/blocking.rs:226-294` (`handle_wait_command`), `:253-256` (fence-then-role-check ordering), `:274-277` (blocked-mirror set) `:319-347` (`resolve_wait_race`); `replication/src/wait_coordinator.rs:159-161` (`role_fence`), `:173-180` (`target_offset`), `:214-256` (`wait_for_replicas`) |
| Forced by | `test_wait_blocks_until_ack`, `test_wait_zero_timeout_blocks_until_ack`, `test_wait_zero_timeout_blocks_without_quorum`, `test_wait_inside_multi_nonzero_timeout_does_not_block` |
| Rulings | — |

## TR-BLOCKING-006 — wake by push (serve)

| Field | Value |
| --- | --- |
| Precondition | A write command (`LPUSH`/`RPUSH`/`LPUSHX`/`RPUSHX`/`BLMOVE`/`BRPOPLPUSH` for `WaiterKind::List`; `ZADD` for `SortedSet`; `XADD` etc. for `Stream`, per `WaiterWake::{Kind,All}` on the executed command's spec) leaves data at a key with a live waiter of a matching kind, `response_tx` still open, deadline not yet elapsed at the pre-send check (TR-BLOCKING-007 is the elapsed-at-check branch of this same loop). |
| Postcondition | Oldest matching waiter (FIFO within kind, by registration ordinal) is popped from the per-key queue and removed from the slab; `response_tx.send(reply)` succeeds; the served element is consumed from the store (not restored); a synthesized deterministic command (e.g. `BLPOP`→`LPOP`, `BLMOVE`→`LMOVE`) is appended to `pending_serve_propagations` so a replica reproduces the same mutation, flushed at the terminal `ReplicationBroadcast` write effect after the waking write's own broadcast; for `BLMOVE`/`BRPOPLPUSH` cascades, the destination key is re-checked for its own waiters up to `MAX_BLMOVE_FANOUT_DEPTH` (16). |
| Source | `shard/blocking.rs:255` (`drive_satisfaction`), `:271-390` (`drive_satisfaction_body`), `:341` (`response_tx.send`, `Ok(())` arm), `:359-361` (propagation push), `:26` (`MAX_BLMOVE_FANOUT_DEPTH`), `WaiterSatisfaction` impls |
| Forced by | unit-level (frogdb-core, `shard/blocking.rs`, not covered by this spec's `-p frogdb-server` test-resolution profile): `blpop_satisfy_pops_and_replies`, `push_to_live_waiter_still_consumes_element`, `served_blpop_records_pending_propagation`, `blmove_success_moves_and_cascades`, `bzpopmin_satisfy_pops_min`; integration (in profile): `test_blpop_woken_notifies_lpop`, `test_brpop_woken_notifies_rpop`, `test_bzpopmin_woken_notifies_zpopmin`, `test_bzpopmax_woken_notifies_zpopmax`, `test_brpoplpush_woken_notifies_rpop_and_lpush`, `test_eval_scripted_lpush_wakes_blocked_blpop`, `test_xread_block_satisfied_by_xadd`, `test_xreadgroup_block_satisfied_by_xadd` |
| Rulings | — |

## TR-BLOCKING-007 — wake-by-push: popped waiter's deadline already elapsed at re-validation (no restore)

| Field | Value |
| --- | --- |
| Precondition | Same trigger as TR-BLOCKING-006, but the popped waiter's `deadline` has already elapsed (`deadline.is_some_and(|d| d <= now)`) by the time the fast-path re-validation runs, before `strat.satisfy` is ever called. |
| Postcondition | The loop `continue`s without calling `strat.satisfy` — no store data is consumed, so nothing needs restoring; `response_tx` is simply dropped without a send; the waiter is not re-queued. This is a cheap optimization over the deadline race, not the correctness backstop: `response_rx`'s dedicated timeout branch (TR-BLOCKING-009) or the reconciliation path (TR-BLOCKING-015) is what guarantees the client still gets a timely, correctly-shaped reply — a receiver dropped here observes the same `Err(_)`→`Response::Null` collapse as TR-BLOCKING-003 and TR-BLOCKING-014 (see the "Wake token" state-space row), which the server's own deadline-elapsed reply (already in flight or about to fire) races and normally wins first. |
| Source | `shard/blocking.rs:320-322` (deadline fast-path check and `continue`; `:316-319` is the comment explaining the race is closed elsewhere) |
| Forced by | unit-level (frogdb-core, `shard/blocking.rs`, not covered by this spec's `-p frogdb-server` test-resolution profile): `push_after_deadline_elapsed_does_not_consume_element` |
| Rulings | — |

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
| Source | `connection/blocking/coordinator.rs:30-46` (`into_response`, `Unblocked` arms), `:104-106` (`unblock.unblocked()` branch); FM-BLOCKING-003 |
| Forced by | `test_client_unblock_not_blocked` covers the not-blocked precondition only; no located test drives a genuine parked non-WAIT `CLIENT UNBLOCK` release end-to-end. |
| Rulings | — |

## TR-BLOCKING-011 — WAIT settles via the replication decision (quorum reached or deadline elapsed)

| Field | Value |
| --- | --- |
| Precondition | A parked WAIT (TR-BLOCKING-005) is resolved by `wait.wait_for_replicas(...)` rather than by `unblock.unblocked()`, in `resolve_wait_race`'s biased select. |
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
| Precondition | A connection with blocked-flag = `Some(shard_id, keys)` closes (any reason) before its wait resolves. |
| Postcondition | `notify_connection_closed` sends `BlockingMsg::UnregisterWait{conn_id, ack}` and discards the ack (no client remains to hand a raced serve back to); the shard's `wait_queue.unregister(conn_id)` removes every entry for that connection from every key it was registered under; if a serve had already raced the teardown and popped data, `apply_restore` on the shard's send-failure path (TR-BLOCKING-008) makes the store whole. |
| Source | `connection/lifecycle.rs:181-216` (`notify_connection_closed`), `shard/wait_queue.rs:214-241` (`unregister`) |
| Forced by | MISSING — no located test drives a connection disconnect while a non-WAIT waiter is parked (the shard-side `unregister` mechanics are unit-tested via `wait_queue.rs`'s FIFO/slot tests, but not the disconnect trigger itself). |
| Rulings | [issue 08](../.scratch/spec-gaps/issues/open/08-blocking-command-rows.md) (confirms this path, no code gap) |

## TR-BLOCKING-014 — shard dies while a waiter is parked

| Field | Value |
| --- | --- |
| Precondition | A waiter that completed registration (TR-BLOCKING-001, a live `WaitEntry` exists in `ShardWaitQueue`) has its `response_tx` dropped by genuine shard-side death — shard task shutdown/panic/teardown tearing down the whole `ShardWaitQueue` — while the connection is still parked in `wait_for_response`. This precondition explicitly **excludes** three other paths that also end in the coordinator's `Err(_)`/`None`→`Response::Null` collapse but are not shard death: the admission refusal at registration, before any `WaitEntry` exists (TR-BLOCKING-003); the deadline fast-path skip, where the shard is alive and the drop is a deliberate no-consume decision (TR-BLOCKING-007); and a closed CLIENT-UNBLOCK signal channel (`unblock.unblocked()` returning `None`), which is a registry/connection-side channel failure unrelated to shard health and has no dedicated row — it collapses to the same `Response::Null` as this row via `coordinator.rs:104-106` but is not itself forcing-test-worthy as a *shard* failure mode. |
| Postcondition (ruled) | `-ERR shard unavailable`, distinguishable from the FM-BLOCKING-002/003 nil/timeout family, matching `txn.md` FM-TXN-032's vocabulary for the same underlying event (channel closed, never accepted). Connection's blocked-flag cleared; the wait entry is gone (channel closure implies no further shard-side bookkeeping references it). |
| Postcondition (current code) | `Response::Null` — see FM-BLOCKING-004, `Err(_)` from the borrowed receiver maps to `WaitOutcome::Response(Response::Null)`, identically to TR-BLOCKING-003's current-code cell and the closed-unblock-channel case above; all three are indistinguishable from an ordinary timeout to the client today. |
| Source | `connection/blocking/coordinator.rs:99-101` (`wait_for_response`, `Err(_)` arm), FM-BLOCKING-004 |
| Forced by | `channel_drop_yields_null_response` forces the `(current code)` cell only, per FM-BLOCKING-004; the `(ruled)` cell is `MISSING ([issue 08](../.scratch/spec-gaps/issues/open/08-blocking-command-rows.md))` — a forcing test must not be written against it until the ruling lands. |
| Rulings | [issue 08](../.scratch/spec-gaps/issues/open/08-blocking-command-rows.md) (H5) |
| Pending | [issue 08](../.scratch/spec-gaps/issues/open/08-blocking-command-rows.md) |

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
| Precondition (ruled) | This node stops being the primary for the shard a waiter is parked on (demotion). |
| Postcondition (ruled) | Every parked waiter on the demoted shard is unblocked (Redis `disconnectAllBlockedClients` precedent), never served from data arriving over the replication stream — serving from replicated data would be a replica-side write, diverging the replica's store from its own replication stream. |
| Postcondition (current code) | No demotion-triggered drain of `ShardWaitQueue` exists: `RoleManager::demote` (`server/role_manager.rs:362-403`) flips `is_replica` (`Ordering::Release`, `:371`), ends the primary stint, and tears down the outbound stream, but never sends any shard message and never touches `ShardWaitQueue` — confirmed by reading the function in full, not by absence-of-match search alone. A demoted node continues applying replicated writes through the same generic post-execution hook that satisfies local waiters: `ReplicaCommandExecutor::apply_group`/`apply_single` (`replication-runtime/src/executor.rs:136-148`, pinned by test `a_single_replicated_command_executes_directly_on_its_tagged_shard`) dispatches an ordinary `CoreMsg::Execute` with no role check anywhere on the path, through `run_write_effects` (`shard/post_execution.rs:305`) to `WriteEffectKind::WaiterSatisfaction` (`:377-380`) to `satisfy_waiters_for_command` (`:701`) — a parked BLPOP can be served by a replicated LPUSH on a now-replica shard. `WAIT` alone has a working mechanism for the analogous race (TR-BLOCKING-005's fence-then-check ordering, TR-BLOCKING-011); non-WAIT parked waiters have no equivalent today. |
| Source | `server/role_manager.rs:362-403` (`demote`, full read, no `ShardWaitQueue` reference); `replication-runtime/src/executor.rs:136-148` (`apply_group`/`apply_single`); `shard/post_execution.rs:305` (`run_write_effects`), `:377-380` (`WaiterSatisfaction` dispatch), `:701` (`satisfy_waiters_for_command`) |
| Forced by | MISSING ([issue 08](../.scratch/spec-gaps/issues/open/08-blocking-command-rows.md)) — the `(ruled)` cell has no code to test yet; the `(current code)` cell's hazard (a demoted shard's replicated write serving a local waiter) is likewise not forced by any located test, consistent with it being an undetected gap rather than a deliberately accepted one. |
| Rulings | [issue 08](../.scratch/spec-gaps/issues/open/08-blocking-command-rows.md) (H6) |
| Pending | [issue 08](../.scratch/spec-gaps/issues/open/08-blocking-command-rows.md) |

## TR-BLOCKING-017 — WAIT role-change while parked

| Field | Value |
| --- | --- |
| Precondition | This node's `role_fence` is bumped (demotion) while a `WAIT` is parked in `resolve_wait_race`, racing `unblock.unblocked()`. |
| Postcondition | `wait_fut` yields `WaitVerdict::RoleChanged(count)` → `Response::error(WAIT_ROLE_CHANGED_ERR)`, biased ahead of a same-poll `CLIENT UNBLOCK`. The fence is subscribed before the primary-role check in `handle_wait_command`, so a demotion landing between subscribe and check is still observed (see the `RoleFence` ordering note; this is the mechanism TR-BLOCKING-016 shows has no non-WAIT counterpart). |
| Source | `connection/blocking.rs:319-347` (`resolve_wait_race`), `:226-260` (`handle_wait_command`, fence-then-role-check ordering); `replication/src/wait_coordinator.rs:64-83` (`WaitVerdict`), `:96` (`RoleFence`) |
| Forced by | `wait_released_by_a_demotion_reports_the_role_change_even_if_client_unblock_races`; unit-level (frogdb-replication, `wait_coordinator.rs`, not covered by this spec's `-p frogdb-server` test-resolution profile): `role_change_releases_a_wait_parked_forever`, `role_change_releases_a_wait_with_a_deadline`, `a_fence_from_an_earlier_stint_does_not_release_a_later_wait`, `a_demotion_racing_the_role_check_still_releases_the_wait`, `a_tie_between_quorum_and_role_change_favors_role_changed_no_deadline`, `a_tie_between_quorum_and_role_change_favors_role_changed_with_deadline`, `one_fence_releases_every_wait_parked_before_it`; integration (in profile): `test_wait_unblocked_on_demotion`, `test_wait_unblocked_on_cluster_demotion` |
| Rulings | [issue 08](../.scratch/spec-gaps/issues/open/08-blocking-command-rows.md) (confirms this path already exists and is correct, unrowed until now) |

## TR-BLOCKING-018 — slot migrates away while a waiter is parked (non-WAIT ops)

| Field | Value |
| --- | --- |
| Precondition | A slot containing keys a waiter is parked on migrates off this node; the cluster runtime's slot-migration-complete event reaches this shard as `ShardMessage::Cluster(ClusterMsg::SlotMigrated{slot, target_addr})`. |
| Postcondition | Every waiter for keys in that slot is drained from the queue (never served locally from a slot this node no longer owns): if `target_addr` is known, each gets `MOVED <slot> <host>:<port>` (IPv6 bracketed); if unknown, each gets `CLUSTERDOWN Hash slot <slot> not served`. Connection's blocked-flag is cleared on receipt. A dedicated counter (`BlockedMigrationMoved`) records only the MOVED case. |
| Source | `shard/dispatch_cluster.rs:8-10` (`ClusterMsg::SlotMigrated` dispatch to `handle_slot_migrated`); `shard/blocking.rs:118-168` (`handle_slot_migrated`); `wait_queue.rs:346-397` (`drain_waiters_for_slot`); `shard/message.rs:776-779` (`ClusterMsg::SlotMigrated` field def); confirmed matching `website/src/content/docs/architecture/blocking.md` "Cluster Mode: Slot Migration Interaction" |
| Forced by | unit-level (frogdb-core, `shard/blocking.rs`, not covered by this spec's `-p frogdb-server` test-resolution profile): `slot_migrated_moved_brackets_ipv6`, `slot_migrated_moved_ipv4_plain`, `slot_migrated_without_a_known_target_replies_clusterdown`; integration (in profile): `test_blocking_command_during_migration_gets_moved` |
| Rulings | [issue 08](../.scratch/spec-gaps/issues/open/08-blocking-command-rows.md) (confirms this path, no code gap) |

## TR-BLOCKING-019 — stream waiter drains: NOGROUP / WRONGTYPE

| Field | Value |
| --- | --- |
| Precondition | A parked `XREAD`/`XREADGROUP`-family waiter's key stops being satisfiable for a reason other than "no new data yet": the consumer group backing the read no longer exists (`KeyReady::DrainNoGroup`), or the key was overwritten with a non-stream type (`KeyReady::DrainWrongType`), discovered on `strat.check_key` during `drive_satisfaction_body`'s poll loop. |
| Postcondition | Every waiter on that key sharing the failing condition is drained and replied to with the corresponding error (`NOGROUP` / `WRONGTYPE`) rather than left parked to time out; the loop returns immediately without attempting a normal serve for the remaining waiters on that key in this pass. |
| Source | `shard/blocking.rs:271-390` (`drive_satisfaction_body`, `KeyReady::DrainNoGroup`/`DrainWrongType` arms), `:493` (`drain_stream_waiters_with_error`), `:512` (`drain_stream_waiters_wrongtype`) |
| Forced by | unit-level (frogdb-core, `shard/blocking.rs`, not covered by this spec's `-p frogdb-server` test-resolution profile): `xreadgroup_missing_group_is_reject` forces the `DrainNoGroup` arm; the `DrainWrongType` arm (a stream key overwritten with another type) has no located forcing test — `blmove_wrong_type_dest_is_reject` (also frogdb-core, unit-level) covers a different code path (a BLMOVE destination's type, not a parked stream reader's key). |
| Rulings | — |

## TR-BLOCKING-020 — shard-side GC tick reclaims an expired waiter

| Field | Value |
| --- | --- |
| Precondition | The shard's 100ms waiter-timeout tick fires and finds a registered `WaitEntry` whose `deadline` has already elapsed, independent of whether the connection's own `wait_for_response` deadline branch has fired yet in that same window. |
| Postcondition | `entry.op.timeout_reply()` is sent on `response_tx` (a no-op if the coordinator's own deadline branch already fired and the channel is gone/already observed) and `BlockedTimeoutTotal` is incremented; the entry is removed from the queue. This is the GC backstop for TR-BLOCKING-009, not an independent client-visible outcome — the client-visible reply is decided once, by whichever of the shard tick or the coordinator's local sleep resolves the race for that connection's `response_rx`/deadline pair. |
| Source | `shard/event_loop.rs:31` (100ms interval), `:83-85` (biased tick, `check_waiter_timeouts`); `shard/blocking.rs:182-211` (`check_waiter_timeouts`, `timeout_reply` send, `BlockedTimeoutTotal` increment) |
| Forced by | MISSING — no located test isolates the shard's 100ms GC tick from the coordinator's own local deadline branch; the two are only ever observed together (whichever fires first wins the race in every timeout test above). |
| Rulings | — |

## How to read a row

Fields as in [txn.md](txn.md#how-to-read-a-row). `Outcome variant`
here names the `WaitOutcome` variant the coordinator settles on, the blocking-path analogue of the
txn spec's transaction outcome.

Test names are bare function names, resolved against
`cargo nextest list -p frogdb-txn -p frogdb-vll -p frogdb-server` (core command profile).
That listing profile does **not** build `frogdb-core`, `frogdb-shard-harness`, or anything behind
`--features turmoil`/`--features shuttle`, so the following genuinely load-bearing guards exist but
are deliberately not cited in `Forced by` rows:

- `frogdb-server/crates/core/tests/concurrency.rs`, module "Blocking-pop exactly-once conservation
  (serve-vs-timeout race)" — the shuttle model of the shard-side handshake, including two
  `#[should_panic]` sensitivity witnesses.
- `frogdb-server/crates/server/tests/concurrency_workload.rs` (turmoil) — the whole-history
  `check_exactly_once_delivery` / `check_fifo_wake_order` conservation checkers over generated
  seed sweeps, plus
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
| NOT observable | One nil shape for both op families (the wrong-shape bug `into_response` exists to fix); an element being consumed from any key; the waiter staying registered after the reply. |
| Invariant | `WaitOutcome::Timeout` carries no data, and `into_response` re-derives the shape from the surviving `BlockingOp`, so timing out is a pure no-op against the store. |
| Outcome variant | `Timeout` |
| Forced by | `timeout_wins_when_idle`, `timeout_reply_picks_nil_shape_per_op` |
| Bug refs | none |

## FM-BLOCKING-003 — CLIENT UNBLOCK targets the parked connection

| Field | Value |
|---|---|
| Trigger | `CLIENT UNBLOCK <id>` (or `… ERROR`) fires on a connection parked in a blocking wait, before any shard delivery or the deadline. |
| Observable | `ERROR` mode: `-UNBLOCKED client unblocked via CLIENT UNBLOCK`. `TIMEOUT` mode: exactly the FM-BLOCKING-002 reply, op-aware nil shape and all. |
| NOT observable | A delivered element (unblocking must never consume); a bare `Response::Null` standing in for the array-shaped nil; the unblock being swallowed while the wait had no deadline (an infinite `BLPOP 0` must still be unblockable). |
| Invariant | The unblock branch is a peer of the response and deadline branches in the same `select!`, so it resolves even when `deadline == None` (which parks the timeout branch on `pending()` forever). |
| Outcome variant | `Unblocked(UnblockMode::{Error,Timeout})` |
| Forced by | `unblock_wins_over_idle_wait`, `timeout_reply_picks_nil_shape_per_op` |
| Bug refs | none |

## FM-BLOCKING-004 — response channel closed without a value

| Field | Value |
|---|---|
| Trigger | The shard drops the wait's response sender without sending — shard shutdown, or the waiter being dropped by a GC/teardown path. |
| Observable | `Response::Null`. The connection is released, not wedged. |
| NOT observable | The wait hanging forever on a dead channel; a panic on `RecvError`; a fabricated element. |
| Invariant | `Err(_)` from the borrowed receiver maps to `WaitOutcome::Response(Response::Null)`, so channel closure is a terminal outcome of the race rather than an error path. |
| Outcome variant | `Response(Response::Null)` |
| Forced by | `channel_drop_yields_null_response` |
| Bug refs | none |

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
