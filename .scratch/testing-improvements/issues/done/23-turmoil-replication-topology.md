# No deterministic turmoil simulation for multi-node replication topology

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: jepsen

## Context

`simulation.rs` (the turmoil-based deterministic simulation harness, 4309 lines) covers only standalone scatter-gather and client-to-server partition scenarios (module doc at line 6; `test_network_partition_client_isolated` is client isolation only). There is no multi-node replication topology under turmoil: no primary/replica failover simulation, no deterministic seed-reproducible coverage of replication-specific timing races. Today, multi-node replication is exercised either by integration tests (no fault injection, no linearizability checking) or by Jepsen (not wired into CI — see task 10 in this batch's sibling issue for that gap). This is the replication half of the broader "no deterministic sim for multi-node" gap; the cluster/raft half is tracked separately (per verdict, F#6 framing wins for that half — do not duplicate it here).

Expected: a deterministic, seed-reproducible turmoil harness exercising ≥2 FrogDB instances in a primary/replica relationship, capable of injecting partitions/timing faults and feeding resulting histories to the existing WGL (write-generalized-linearizability) checker (`crates/testing/src/checker.rs`) for automated verification — giving fast, deterministic, CI-friendly coverage of races that are currently only reachable (if at all) via slow, non-CI Jepsen runs.

## What to build

- Extend `simulation.rs` (or add a sibling module) with a turmoil topology hosting ≥2 FrogDB instances wired as primary/replica.
- Add replication-failover simulation tests: partition primary from replica, force failover/promotion, heal partition, and assert convergence.
- Feed resulting operation histories into the WGL checker (`check_linearizability`) to get automated pass/fail rather than manual inspection.
- Wire at least one such test into the per-PR or nightly test run (not just available to run by hand).

## Acceptance criteria

- [ ] Turmoil harness supports ≥2-instance primary/replica topology with deterministic, seed-reproducible scheduling.
- [ ] At least one replication-failover simulation test exists: partition, promote, heal, assert convergence.
- [ ] Test feeds its operation history to the WGL checker and asserts a checker verdict (not just "no panic").
- [ ] New test(s) run in CI (per-PR or nightly) alongside existing turmoil tests.

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/server/tests/simulation.rs` (module doc line 6; `test_network_partition_client_isolated`)
- `frogdb-server/crates/testing/src/checker.rs` (WGL linearizability checker, `check_linearizability`)
- Related: cluster/raft-topology half of this gap tracked separately (F#6 framing) — do not duplicate.

## Resolution

Done (2026-07-23). Added `test_replication_failover_wgl_linearizable` to
`frogdb-server/crates/server/tests/simulation.rs` (turmoil feature tier) — a deterministic,
seed-reproducible (`seeds = [1, 7, 42, 99]`) ≥2-instance primary/replica failover + failback
simulation that feeds the client-observed history to the WGL checker.

Scenario per seed:
1. Primary + replica come up; replica links (`WAIT 1 >= 1`).
2. **Confirmed writes** — 10 keys, each `SET` then `WAIT 1` so the client knows the replica
   received it; a concurrent reader client reads them on the primary for genuine invoke/return
   overlap. Recorded into the WGL history.
3. **Loss window** — 3 *unconfirmed* `SET`s (no `WAIT`), then the primary is isolated from the
   replica with `hold`/`release` (NOT `partition` — turmoil 0.7.1 leaks the ephemeral port of a
   dropped/redialing connection, per issue 11). Excluded from the WGL history (async carve-out per
   consistency.md).
4. **Promotion** — `REPLICAOF NO ONE` on the replica; poll until it accepts writes; apply-barrier on
   the last confirmed key, then read every confirmed key back (recorded) — a lost confirmed write
   surfaces as a nil the KVModel rejects. Asserts failover durability.
5. **Post-failover** writes/reads on the new primary (recorded, disjoint keys).
6. **Heal + failback** — `release`; demote the promoted node back to a replica of the recovered
   original primary; wait for re-attach (`WAIT 1`); assert the durable (confirmed) data set converges
   on both nodes and split-brain post-failover writes never reached the original.
7. Feed the recorded history to `check_linearizability::<KVModel>`; assert `!inconclusive` and
   `is_linearizable`.

Acceptance criteria — all met: ≥2-instance topology with deterministic seeds; a failover test with
partition + promote + heal + convergence; WGL checker verdict asserted (not "no panic"); runs in the
existing turmoil feature tier (`--features turmoil`, exercised by `just lint-turmoil` and the turmoil
nextest job).

### Product fix made

`role_manager.rs` `RealReplicaStreamer::build_handler` only injected the turmoil connect factory
under `#[cfg(not(feature = "turmoil"))]` (the TLS branch), so a **runtime** `REPLICAOF` demotion
dialed real tokio TCP and panicked ("A Tokio 1.x context was found, but IO is disabled") under
turmoil. Added the `#[cfg(feature = "turmoil")]` connect-factory block so the runtime-demote path
dials through `turmoil::net::TcpStream`, matching the boot-time replica path in `replication_init.rs`.

### Boundaries (precise, for follow-up)

- **Promoted replica cannot serve PSYNC to a sub-replica.** A `PrimaryReplicationHandler` is created
  only at boot when `config.replication.is_primary()` (`replication_init.rs`); `RoleManager::promote()`
  clears the replica flag and stops the inbound stream but does not stand one up, and a replica-booted
  node uses `NoopBroadcaster` with `primary_replication_handler = None`. So the deposed old primary
  cannot re-attach *under* the promoted node. The test therefore fails back toward the recovered
  boot-primary (which can serve PSYNC), which is the supported reconvergence path.
- **Runtime full resync is staged-not-installed** (filed as issue 61). `receive_checkpoint`
  (`replica/connection.rs`) stages the new master's snapshot to disk but leaves the live store
  unchanged until the next boot, so a demoted former-primary keeps its forked live keyspace; live
  reconvergence of non-durable keys needs a restart. The test asserts convergence of the durable
  (WAIT-confirmed) subset — the actual client guarantee — and documents this boundary inline.
