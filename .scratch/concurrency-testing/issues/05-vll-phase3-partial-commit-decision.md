# 05 — Design decision: durable partial commit on VLL phase-3 dispatch failure

Status: ready-for-agent
Type: HITL
Origin: phase-4b task-10 review (controller flag; documented design, not a bug)

## Decision needed

`VllCoordinator` deliberately does NOT abort shards that already received `VllExecute` when a later shard's phase-3 dispatch fails: it aborts only `shard_ids[idx..]` (`frogdb-server/crates/vll/src/coordinator.rs:266-288`, pinned by `phase3_failure_aborts_remaining_holders_not_positions` and integration-pinned two-sided in `crates/core/tests/shard_driver/scenario_s3.rs`). Consequence: a cross-shard VLL-EXEC can land as a **durable partial commit** — executed shards keep their writes, failed/undispatched shards roll back.

Rationale for the current design: post-lock-acquisition VLL determinism — participants that already executed release their own locks on completion and must not be aborted. But a phase-3 dispatch failure models shard-infra failure, which a deterministic engine would typically resolve by log-replay **re-execution** of the failed participant, not by abandoning atomicity.

## Prior art (researched 2026-07-21)

- **Redis:** EXEC has no rollback by documented design — a runtime error in one queued command does not undo the others. Partial application on error is the accepted contract.
- **DragonflyDB** ([transaction.md](https://github.com/dragonflydb/dragonfly/blob/main/docs/transaction.md), [transactions blog](https://www.dragonflydb.io/blog/transactions-in-dragonfly)): structurally sidesteps this decision.
  - Pre-execution scheduling failure (ordering conflict): "the coordinator reverts all shards and retries with a new TxId" — abort-all before anything applies (FrogDB's phase-1/2 handling matches).
  - Post-schedule: "Once scheduled, a transaction never rolls back or retries." No undo machinery. And **no shard-infra failure model**: shards are fibers in one process, dispatch is an in-process queue push that cannot fail while the process lives; thread death = process death. FrogDB's phase-3 window (`send_execute` Err = shard worker's channel closed while the node keeps serving) has no Dragonfly analogue.
  - Atomicity is even opt-out: `multi_exec_mode` non_atomic mode carries "No atomicity guarantee" as a perf feature.
  - Replica side: they DID invest — v0.13.0 "feat(replica): atomicity for multi shard commands" ([PR #598](https://github.com/dragonflydb/dragonfly/pull/598)) keeps per-shard async journal flows from exposing partial multi-shard txns on replicas. **Directly relevant to FrogDB phase 6 replication.**
- Takeaway: no engine in this space rolls back applied transaction writes; they keep the *failure model* narrow instead. That is the strongest argument for options 1/4 and against option 3.

## Options

1. **Accept as designed** — document the partial-commit window as a stated consistency property of cross-shard EXEC under infra failure (client-visible caveat). Industry-consistent (see prior art). Cost ≈ one paragraph; S3 already pins the behavior. Risk: becomes a real product behavior the moment phase 6 makes shard-level failure survivable.
2. **Re-execution path** — persist the decided VLL batch (intent record before phase-3 dispatch) and replay the failed shard's portion on recovery. Restores durable atomicity, NOT live visibility (executed shards released locks; readers see partial state until recovery). Requires: coordinator commit-point persistence in the hot path, txn framing in per-shard WALs (pairs with the rollback-mode WAL partial-append deferral), recovery scanner + deterministic re-executor, idempotence for the phase-4 executed-but-unconfirmed window (response channel dropped/timeout after VllExecute delivered). Also forces an EXEC-ack semantics decision (block / error-but-applies-later / Calvin-style ack-on-log). Durability-phase scope.
   - **2b (cheaper variant): abort-on-recovery** — intent record + per-shard completion markers; recovery **discards** incomplete cross-shard txns instead of re-executing. Sound because the coordinator died before acking EXEC to the client. Skips the deterministic-replay machinery entirely; still needs txn framing + intent record. Only coherent combined with option 4 (node must not keep serving after a phase-3 failure, else discarding at recovery contradicts already-served reads).
3. **Abort-all with executed-shard compensation** — conflicts with the VLL lock-release invariant (executed participants released their own locks); no prior art does write-undo. Dead end, listed for completeness.
4. **Fail-stop (Dragonfly-equivalent; expanded 2026-07-21)** — treat a dead shard worker as node-fatal. WAL recovery still resurrects the partial commit unless paired with 2b's txn framing — 4 + 2b together = full atomicity without live replay. See trigger analysis below for why this cannot fire intermittently, and the spectrum for graduated deployment:
   - **4a. Supervise** (minimum; arguably a standalone bug-fix): shard workers are unsupervised tokio tasks — `spawn(monitor.instrument(worker.run()))` (`server/shards.rs:279`) where `monitor` is `tokio_metrics::TaskMonitor` (metrics only); `JoinHandle`s are only joined at shutdown (`subsystems.rs:550`). A panicked worker dies silently: 1/N of the keyspace gone, blocked waiters never wake, active expiry stops for those keys, no health signal — a **zombie node**, which in an HA deployment is worse than a crash (no failover trigger). Fix: watch handles live (JoinSet/select); on JoinError → CRITICAL log, flip node health (INFO/STATUS, liveness probe) so the orchestrator restarts.
   - **4b-fence:** on detected worker death, mark the shard dead and reject cross-shard EXECs naming it **before phase 1**. Shrinks the partial-commit surface from "every transaction until someone notices" to "transactions already past lock-phase at the instant of death."
   - **4c-abort:** `process::abort()` on unexpected worker death. Simplest; the only variant that composes cleanly with 2b (abort-on-recovery). Must be gated on "shutdown not in progress" (see below).

## Trigger analysis: can this fire from normal transactional behavior? (verified 2026-07-21)

**No.** `send_execute` Err strictly ⇒ worker task dead; it is unreachable from load, contention, or slowness:

- Production sink (`server/src/vll_adapter.rs:118-127`) dispatches via `ShardSender::send().await` = tokio **bounded mpsc awaiting send** (`core/src/shard/message.rs:42-55`): a full channel *waits* (backpressure), never errors; `Err` exists only for `Closed` (receiver dropped = worker gone).
- Normal transactional failures (lock conflict, lock timeout) land in phases 1-2, which `abort_shards(ALL)` before anything executes — all-or-nothing preserved (`vll/src/coordinator.rs:250-263`).
- Slowness lands in **phase 4** (`ResultTimeout`, `coordinator.rs:299-302`) and deliberately aborts nothing: by then every participant has `VllExecute` queued and will apply. Outcome = transaction **fully commits**, client gets a timeout error — the standard ambiguous-outcome problem (same as any timed-out Redis command; a client retry re-applies on ALL shards), **not** a partial commit. Load-dependent but atomicity-preserving; document separately as EXEC-timeout ambiguity.
- Chaos/error injection paths in the sink are `#[cfg(feature = "turmoil")]` — absent from production builds.

Known causes of a closed shard channel, exhaustively:
1. **Panic unwinding the worker task** (a bug, deterministic per triggering input; workspace has no `panic = "abort"`, tokio contains the panic). This is the "already zombified — crash" case; also note the client whose EXEC hit phase-3 failure got an error reply while some shards committed, so a retry **double-applies** the executed portion — retry amplification on top of partiality.
2. **Graceful-shutdown race** (benign): subsystem teardown drops workers while a scatter is in flight. Any 4c implementation must check shutdown-in-progress before aborting, so clean shutdowns don't register as crashes.

## Decision (2026-07-21, Nathan)

**Option 4 accepted** — fail-stop: "If the node is already broken/zombified, we should crash." Confirmed after trigger analysis showed the path cannot fire from normal transactional behavior (backpressure never errors; slowness lands in atomicity-preserving phase-4 timeout). Implement now: **4a supervision + 4c abort with the shutdown-in-progress guard** (4b fence is subsumed while 4c is unconditional — revisit only if a degrade mode is ever added). **2b (txn framing + abort-on-recovery)** deferred to the durability phase, filed alongside the rollback-mode WAL partial-append work, so recovery stops resurrecting the pre-abort partial commit. Phase-4 timeout ambiguity + phase-6 replica-atomicity notes remain as documentation criteria below.

## Acceptance criteria

- [x] Human decision recorded here (option + rationale) — option 4 (4a+4c now, 2b durability-phase)
- [ ] If option 1: consistency caveat documented in the appropriate CONTEXT.md/spec — N/A, option 1 not chosen
- [x] If option 2/2b/4: follow-up implementation issue(s) filed in the durability phase (txn framing, intent record, recovery policy; escalation path + shutdown-race guard for option 4) — filed as [issue 06](./06-durability-txn-framing-abort-on-recovery.md) (`.scratch/concurrency-testing/issues/06-durability-txn-framing-abort-on-recovery.md`); 4a+4c's escalation/shutdown-race guard already covered by the line below
- [x] 4a+4c IMPLEMENTED (2026-07-21, merged `da1c0838`): `server/shard_supervisor.rs` — live supervision, injectable `FailStopHandler`, production abort with pre-abort `eprintln!` diagnostic, shutdown guard via `HealthChecker::is_shutting_down()`; reviewed + merged
- [x] EXEC phase-4 timeout ambiguity (commits despite timeout error reply) documented wherever cross-shard EXEC semantics are specified — `website/src/content/docs/architecture/vll.md`, new "Phase-4 result timeout vs phase-3 dispatch failure" subsection (atomicity table also updated to split the two failure classes into separate rows)
- [x] Phase-6 planning note: replica-side multi-shard atomicity (Dragonfly PR #598 precedent) regardless of option chosen — `docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md`, "Planning note — replica-side multi-shard transaction atomicity" paragraph in the Phase 6 section
