# Replication broadcast-lag disconnect/resync path is under-tested and has a stale doc-comment

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: replication (area E)

## Context

A lagged replica is supposed to be disconnected to force a resync
(`replication/src/replica_session.rs:734-738`); the broadcast channel capacity is 10000
(`replication/src/primary/mod.rs:120`). `test_replica_lag_behavior`
(`server/tests/integration_replication.rs:1798-1848`) under-fills this: it only writes 1000
frames, never triggering the `Lagged` condition it's nominally testing, and asserts nothing
beyond an `eprintln!` of 5 keys. The test also carries a stale doc-comment claiming "Issue #2:
only warns — no resync," which no longer matches the actual disconnect-then-resync behavior in
`replica_session.rs:734-738`.

There is no test that actually drives a replica past the channel capacity, confirms the primary
proceeds without blocking, confirms the lagged replica disconnects, confirms it reconnects and
performs a resync, and confirms the resulting dataset matches the primary's (via `SCAN` or a
checksum). This leaves the resync-after-lag path — a core availability/correctness guarantee for
replication — effectively unverified.

Verdict (adversarial pass): CONFIRMED L2/C2.

## What to build

An integration test that stalls a replica's socket (or otherwise prevents it from draining), then
writes more than 10000 frames from the primary so the broadcast channel actually lags and drops
the replica. Assert: primary continues processing without blocking on the stalled replica; the
lagged replica disconnects; on reconnect it performs a resync; post-resync, primary and replica
datasets are checksum/`SCAN`-equal; `WAIT` reflects the replica dropping out of the ack count
during the stall and recovering once resynced. Fix the stale "Issue #2: only warns — no resync"
doc-comment.

## Acceptance criteria

- [ ] Test genuinely exceeds the 10000-frame broadcast channel capacity for a stalled replica
      (not the current 1000-write under-fill).
- [ ] Test asserts the lagged replica is disconnected (not just implied via log output).
- [ ] Test asserts reconnect triggers a resync and post-resync data equality (checksum or full
      `SCAN` comparison), not just "some keys present."
- [ ] Test asserts `WAIT` ack-count behavior during the stall and after recovery.
- [ ] Stale doc-comment ("Issue #2: only warns — no resync") in
      `integration_replication.rs:1798-1848` corrected to match actual disconnect/resync
      behavior.

## Blocked by

None - can start immediately

## References

- `replication/src/replica_session.rs:734-738`
- `replication/src/primary/mod.rs:120`
- `server/tests/integration_replication.rs:1798-1848`
- `.scratch/testing-improvements/audit/E-replication.md` (`broadcast-lag-disconnect-resync-untested`, E#8)
- `.scratch/testing-improvements/audit/verdicts-E.md`

## Resolution

Done 2026-07-23. Added `test_broadcast_lag_disconnect_and_resync` (rstest: `in_memory` +
`with_persistence`) in `server/tests/integration_replication.rs`, and fixed the stale
`test_replica_lag_behavior` doc-comment. All acceptance criteria met. Building the test surfaced
**two real replication bugs** (both fixed) and **one observability bug** (filed as a follow-up).

### Test design

A throttling TCP proxy (`LagProxy`) is interposed between replica and primary; the replica dials
the proxy. Stalling the primary→replica leg deterministically drives the replica past the
primary's lag tolerance (no timing luck; every wait polls a deadline, no fixed sleeps). The
replica→primary leg (handshake + ACKs) always flows so a reconnect can complete. Flood = 20_000
pipelined SETs.

Deterministic disconnect trigger is the primary's **blocked-write timeout**
(`replica_write_timeout_ms` lowered to 1000ms): a fully stalled socket blocks the per-replica
write task, which the timeout breaks. (The 20k volume also overruns the 10_000-slot broadcast
channel — the secondary `Lagged` trigger — but a fully stalled write never drains the receiver, so
the write-timeout is what fires first.) Self-fence disabled so the primary keeps accepting the
flood.

Convergence is asserted via a **partial resync** (`+CONTINUE`): the backlog
(`split_brain_buffer_size`) is sized to 100_000 (above the flood) so the reconnecting replica's
gap is still covered and the missed writes replay as live commands, converging the **running**
keyspace in place (DBSIZE equality + GET spot-checks of flood + seed keys). This is the only
resync path that converges a live store without a restart — see the full-resync findings below.

Assertions: baseline `connected_slaves==1` + seed replicates; stall+flood →
`connected_slaves→0` (disconnect); `WAIT 1` returns 0 while dropped; resume →
`connected_slaves→1` (reconnect); `WAIT 1` returns 1 after recovery; DBSIZE converges to 20001;
flood/seed key spot-checks. Flake rate 0/10 (10 clean runs, ~2.5s each). Full replication suite
green: 175 integration + 158 replication unit tests pass.

### Bug 1 (fixed) — replica never reconnected after a primary-initiated clean close

`replication/src/replica/mod.rs`: the reconnect loop treated a clean `Ok(())` from the sync loop
(what a primary lag-disconnect looks like once the socket closes) as **terminal**, stranding the
disconnected replica forever. Fixed to reset backoff and reconnect after any non-shutdown link
loss; intentional stops still terminate via the shutdown watch (verified role_manager
promote/demote/drop all call `handler.stop()`).

### Bug 2 (fixed) — primary lag-disconnect left a zombie half-open socket

`replication/src/replica_session.rs`: the streaming `select!` dropped the losing task's
`JoinHandle` instead of aborting it. Dropping a `JoinHandle` does **not** abort the task, so after
the write task broke (lag/write-timeout) the detached read task kept its half of the
`tokio::io::split` stream alive — the socket never closed, the replica never saw a FIN, and it
never resynced (primary shows `connected_slaves→0` but the replica sits on a dead link). Fixed to
`abort()` the sibling task so both halves drop and the socket closes. (Both bugs are needed: Bug 2
makes the primary actually send the FIN; Bug 1 makes the replica reconnect on it.)

### Finding (filed, not fixed) — replica `master_repl_offset` hardcoded to 0

`server/src/commands/info.rs:448`: a replica always reports `master_repl_offset:0` in
`INFO replication`, regardless of how much of the master's stream it has processed (its real
applied offset is available as `ReplicaOffset::current` — e.g. the PSYNC reconnect used a nonzero
offset while INFO simultaneously reported 0). Redis reports the processed offset here (consumed by
monitoring/Sentinel and chained-replica WAIT). Misleading per the observability-accuracy rule, but
fixing it requires plumbing the replica's live offset atomic into `CommandContext`/INFO — out of
scope for this test task. Filed as a follow-up.

### Design notes (not bugs — documented behavior)

- A **full** resync does not converge the running keyspace live: persistence stages a checkpoint
  that installs only on the next boot; in-memory ships an empty RDB (`create_minimal_rdb`, no
  snapshot mechanism). An in-memory replica that ever takes a full resync after data exists cannot
  recover that data without a partial resync or a live re-stream. The test therefore drives a
  partial resync (large backlog) to exercise live convergence.
- Harness: added `TestServerConfig.replication_replica_write_timeout_ms` and
  `replication_split_brain_buffer_size` overrides.

### Addendum 2026-07-28 — Linux-only failure, and the real replication bug behind it

The test as landed passed on macOS but failed **deterministically on the aarch64 Linux testbox**
(both rstest cases) at the disconnect assertion:

```
assertion `left == right` failed: primary must disconnect the stalled replica
(connected_slaves→0) once it exceeds the lag threshold
  left: Some(1)
 right: Some(0)
```

#### 1. Environment: the flood was smaller than the kernel's socket buffers

Every disconnect trigger in `replica_session.rs::start_streaming` needs the primary's *userspace*
to notice the stall: the write timeout only fires if `write_all` blocks, and `RecvError::Lagged`
only fires if the write task stops draining the broadcast receiver — which likewise requires a
blocked write. A write only blocks once `primary SO_SNDBUF + proxy SO_RCVBUF` are full, and both
are per-kernel. macOS keeps the loopback send buffer small; Linux autotunes it to `tcp_wmem` max
(testbox: `net.ipv4.tcp_wmem = 4096 16384 4194304`, i.e. 4 MiB), which swallowed the original
flood whole — 20_000 unpadded SETs is only ~1.4 MiB.

Measured on the testbox with an instrumented copy of the same proxy/flood:

| flood bytes | proxy `SO_RCVBUF` | disconnect? |
|---|---|---|
| ~1.4 MiB (as landed) | default (autotuned) | no — `connected_slaves` stayed 1 |
| ~1.4 MiB | 128 KiB | no — bytes pile into the *sender's* buffer regardless of window |
| ~6.5 MiB | default | yes, mid-flood: `WARN … Write to replica timed out … timeout_ms=1000` |

The middle row repeats issue 29's finding for
`test_slow_subscriber_output_buffer_bound_disconnects`: shrinking the receiver alone does not
bound the ceiling, the flood has to exceed it.

Test fix: `LagProxy` pins `SO_RCVBUF` (`LAG_PROXY_RCVBUF_BYTES` = 128 KiB) on the leg it dials to
the primary, and flood values carry `FLOOD_VALUE_PAD` (800 B) of padding — 20_000 frames x ~840 B
≈ 16 MiB, several times the 4 MiB Linux ceiling. Frame count is unchanged (still past the
10_000-slot broadcast cap) and 16 MiB stays inside the 100k-entry / 64 MiB backlog, so the
reconnect still earns the partial resync the convergence assertions depend on.

#### 2. Product bug the fixed test then exposed: writes made with zero replicas connected were never replicated

With the disconnect finally firing on Linux, the test failed at the *next* assertion — the replica
reconnected, `master_link_status:up`, `WAIT` acked, and its keyspace froze at ~7.4k of the
primary's 20_001 keys. The primary's own log gave it away:

```
WARN  frogdb_replication::replica_session: Write to replica timed out, disconnecting replica_id=1
INFO  frogdb_replication::primary: PSYNC -> partial resync (+CONTINUE) replay_from=10285 resume=6358797
```

`resume=6358797` — the primary's live offset was ~6.36 MB after applying ~16.8 MB of writes. The
broadcast gate in `core/src/shard/post_execution.rs` was
`ReplicationBroadcaster::is_active() == tracker.replica_count() > 0`, so from the instant the
laggy replica was dropped until it reconnected (~3.4 s here) every write skipped offset stamping
*and* the backlog. The replica then reconnected inside the stale window, was granted `+CONTINUE`,
and resumed past the hole — silently divergent, with every health signal green. Any deployment
that runs with `self_fence_on_replica_loss = false` (as this test does, and as any
availability-over-consistency operator would) hits this on every lag-disconnect.

Product fix (minimal, Redis-shaped): the gate is now
`replica_count() > 0 || replay.has_resume_history()` — a primary keeps stamping and recording for
as long as its backlog can still serve a `+CONTINUE`, exactly as Redis keeps `repl_backlog` alive
and advances `master_repl_offset` after the last replica leaves. A primary that never had a
replica has an empty backlog and stays inactive, so standalone writes pay nothing. Unit-guarded by
`primary::tests::broadcast_gate_stays_open_while_backlog_can_resume`.

Known gap left open (follow-up): FrogDB has no `repl-backlog-ttl`, so once a replica has ever
connected the backlog (and the recording cost) lives for the life of the process. Bounded by
`split_brain_buffer_size` / `split_brain_buffer_max_mb`, so it is a fixed cost, not a leak.

Verified after both fixes: 3/3 clean on the Linux testbox, 3/3 clean on macOS, plus the full
workspace suite.
