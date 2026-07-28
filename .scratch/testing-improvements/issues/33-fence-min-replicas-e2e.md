# Self-fence + min-replicas-to-write have no e2e coverage; min-replicas-to-write is inert as a write gate

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4) — min-replicas-to-write sub-issue arguably C3 (inert write-safety gate)
Area: replication (area E)

## Context

`self_fence_on_replica_loss` wires a `ReplicationQuorumChecker`
(`server/src/server/replication_init.rs:186-195`) that rejects writes with
`"CLUSTERDOWN The cluster is down (quorum lost, writes rejected)"` — and does so even on a
standalone node (`guards.rs:286-296`), which is a questionable error string to surface outside
cluster mode. Coverage today is unit-only: `replication_quorum.rs:85-157` and
`MockQuorumChecker` in `guards.rs:794-825`. There is no end-to-end test that actually flags a
replica as lost, confirms writes get rejected, and confirms recovery once quorum is restored.

Separately, `min_replicas_to_write` is only consulted inside `wait_for_acks`
(`replication/src/tracker.rs:254-262`) — it never gates writes at all. A Redis user configuring
`min-replicas-to-write 1` with zero connected replicas expects writes to be refused with
`NOREPLICAS`; in FrogDB the config is silently inert and writes pass through unconditionally.
This is a real behavioral gap, not just a test gap, and the adversarial verification pass flagged
it as arguably C3 (a broken write-safety guarantee) rather than C2 — filing it here as part of
the same e2e task since fixing/testing one naturally exercises the other, but the min-replicas
sub-issue should be treated as higher severity when triaged.

Verdict (adversarial pass): CONFIRMED L2/C2 overall; min-replicas-to-write sub-issue "arguably C3."

## What to build

1. An e2e test that engages the replica-loss fence (kill/disconnect the replica quorum), asserts
   writes are rejected with the documented error, then restores quorum and asserts writes recover.
2. A decision (pin via test either way) on whether `min-replicas-to-write` should actually gate
   writes like Redis's `NOREPLICAS`, or is deliberately out of scope — if it should gate writes,
   wire it into the write path and test `min-replicas-to-write 1` + zero replicas → write
   rejected. If deliberately inert, an explicit test + doc note saying so (current silent
   pass-through is the "certain gap").
3. A turmoil test partitioning a primary from its replicas within the fence timeout window,
   confirming fencing engages within bound.
4. Fix (or explicitly document/test) the standalone `CLUSTERDOWN` error string, since it's
   misleading outside cluster mode.

## Acceptance criteria

- [x] e2e test: fence engages on replica loss, write rejected with documented error, write
      succeeds again after quorum recovery.
- [x] `min-replicas-to-write` behavior is pinned by a test — the config is now an *active* write
      gate (was inert), refusing writes with `NOREPLICAS` when fewer than N good replicas are
      connected; pinned across boot-refusal, live-health, MULTI, Lua, and `CONFIG SET` paths.
- [~] Turmoil/deterministic test: DEFERRED — the sim harness has no replica-mode support (see
      `simulation.rs:3081-3082`: replica-mode tests "require replication TcpStream abstraction",
      Phase-5 deferred). The real-network e2e tests instead assert the fence engages within a
      bounded poll deadline (~0.5s freshness window vs 5s deadline). Building
      replication-over-turmoil is tracked as a follow-up, not in scope here.
- [x] Standalone `CLUSTERDOWN` error string reviewed — kept as-is (matches the existing unit
      tests) and *pinned* by exact-string assertion with a NOTE documenting the divergence from
      Redis (Redis surfaces no CLUSTERDOWN on a non-cluster primary; it would refuse via
      `NOREPLICAS`). Rewording is flagged as a follow-up candidate but not done here to avoid
      scope creep.

## Resolution

Status: **done** (1 acceptance criterion deferred with a concrete infra blocker — see above).

### Bug fixed: `min-replicas-to-write` was inert

Confirmed the audit finding: `min_replicas_to_write` was read only inside `wait_for_acks`
(`replication/src/tracker.rs`) and never gated writes. A user setting `min-replicas-to-write 1`
with zero replicas saw writes pass unconditionally. Fixed by adding a `NOREPLICAS` write gate in
`connection/guards.rs::run_pre_checks`, right after the self-fence check:

- New `ReplicationTracker::count_good_replicas(max_lag)` counts streaming replicas whose last ACK
  is within `min-replicas-max-lag` (`max_lag == 0` disables the freshness filter, matching Redis).
- The gate fires for WRITE-flagged commands when `min-replicas-to-write > 0` and
  `count_good_replicas < min-replicas-to-write`, returning
  `NOREPLICAS Not enough good replicas to write.` (exact Redis string). Read live from
  `ConfigManager`, so `CONFIG SET` applies immediately.

### Coverage profile (documented, shared bound with self-fence)

Both gates fire in `run_pre_checks`, which covers direct writes and MULTI **queue** time. Two
paths are NOT gated (pinned by tests so a future fix flips them deliberately):

1. **Lua-internal writes** — `EVAL` lacks the WRITE flag, so `redis.call('SET', ...)` bypasses
   `run_pre_checks` entirely. Uniform enforcement belongs at the shard/script-gate write seam
   (higher-risk refactor, out of scope). Tracked as a follow-up.
2. **MULTI EXEC-after-queue** — the gate runs at queue time, not at EXEC.

### Divergence finding: MULTI queue-time rejection does not abort EXEC

While pinning the MULTI path I found FrogDB rejects a fenced write at **queue** time and does not
enqueue it, so `EXEC` runs an **empty** transaction (`*0`) rather than aborting with `EXECABORT`.
Redis treats the fence as an exec-time condition (the write is queued, then fails at EXEC). The
FrogDB behavior still upholds the safety guarantee (the write does not apply — verified by reading
the key back), so it is pinned as current behavior rather than treated as a blocker; a follow-up
could align MULTI queue-time-error semantics with Redis's `EXECABORT`.

### Tests added

Nine e2e tests in `frogdb-server/crates/server/tests/integration_replication.rs` (Tier 4):

- `test_self_fence_engages_on_replica_loss` — engage + exact CLUSTERDOWN string + reads allowed.
- `test_self_fence_recovers_after_replica_reconnect` — recovery once a fresh replica restores quorum.
- `test_self_fence_unarmed_allows_writes` — a primary that never had a replica does not fence.
- `test_self_fence_multi_rejected_at_queue_time` — MULTI queue-time rejection + empty EXEC + key unchanged.
- `test_self_fence_does_not_gate_lua_writes` — pins the Lua bypass gap.
- `test_min_replicas_to_write_rejects_without_replicas` — boot refusal + exact NOREPLICAS string.
- `test_min_replicas_to_write_gate_tracks_replica_health` — allowed with a good replica, refused after loss.
- `test_min_replicas_to_write_multi_and_lua_paths` — MULTI queue-time gate + Lua bypass.
- `test_min_replicas_to_write_config_set_live` — `CONFIG SET` applies on the hot path immediately.

Test harness gained five `TestServerConfig` replication knobs (self-fence toggle, freshness
timeout, ack interval, min-replicas-to-write, min-replicas-timeout). Ran 6× locally: 0 failures
(one nextest "leaky" note on a passing run — lingering harness socket, not a failure).

### Files changed

- `frogdb-server/crates/replication/src/tracker.rs` — `count_good_replicas`.
- `frogdb-server/crates/server/src/runtime_config.rs` — `min_replicas_to_write` / `min_replicas_timeout_ms` accessors.
- `frogdb-server/crates/server/src/connection/guards.rs` — NOREPLICAS gate + `config_manager` on the pre-dispatch view + test fixture.
- `frogdb-server/crates/test-harness/src/server.rs` — five replication test knobs.
- `frogdb-server/crates/server/tests/integration_replication.rs` — nine Tier-4 e2e tests.

## Blocked by

None - can start immediately

## References

- `server/src/server/replication_init.rs:186-195`
- `server/src/connection/guards.rs:286-296,794-825`
- `replication/src/tracker.rs:254-262`
- `replication/src/primary/tests.rs` / `replication_quorum.rs:85-157`
- `.scratch/testing-improvements/audit/E-replication.md` (`self-fence-and-min-replicas-write-rejection-untested-e2e`, E#4)
- `.scratch/testing-improvements/audit/verdicts-E.md`
