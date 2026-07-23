# Runtime REPLICAOF full resync stages the checkpoint but does not install it into the live store

Status: needs-triage
Type: AFK
Origin: task 23 turmoil replication-failover sim (2026-07-23) — surfaced by the failback convergence assertion

## Context

While building the deterministic turmoil replication-failover simulation (issue 23,
`frogdb-server/crates/server/tests/simulation.rs`
`test_replication_failover_wgl_linearizable`), the failover + failback scenario exposed a
live-convergence gap.

Scenario: primary `P` + replica `R`. Confirmed writes (SET + `WAIT 1`) replicate to `R`. `P` is
isolated from `R` (`hold`). `R` is promoted (`REPLICAOF NO ONE`) and takes post-failover writes on
disjoint keys `p0..p4`. The isolation heals (`release`) and the operator fails back: `R` is demoted
back to a replica of the recovered `P` (`REPLICAOF <P> <port>`). `R` re-attaches and `WAIT 1` on `P`
returns `>= 1`, i.e. the replication link is up and acking.

Observed: after failback, `R` still serves its forked post-failover writes (`GET p0` -> `w..._0`)
even though `P` — the authoritative master — never had them. The two live keyspaces do not
reconverge; polling for ~8s does not clear the divergence.

## Root cause

`ReplicaConnection::receive_checkpoint`
(`frogdb-server/crates/replication/src/replica/connection.rs`, ~L263-296) is explicitly
*staged-not-installed*: the incoming full-sync checkpoint is written to a scratch dir and committed
as a staged checkpoint, the replica adopts the new replication id + offset and flips to `Streaming`,
but "the on-disk DB is unchanged until the next boot loads it" (doc-comment, verbatim). The live
in-memory/RocksDB store is never swapped for the new master's snapshot. A node that took a runtime
`REPLICAOF <new-master>` therefore keeps its entire pre-existing (possibly forked) live keyspace and
merely streams the new master's WAL tail on top; the staged snapshot only takes effect after a
restart.

Consequence: a demoted former-primary does not discard the writes it accepted while it was the
(temporary) master. Live-state reconvergence after a runtime role change requires a process restart.
For a clean single-key overwrite the WAL tail can paper over the divergence, but keys that exist only
on the deposed node (or diverged in value) persist until reboot.

## Impact

- Runtime failover/failback (`REPLICAOF` at runtime, no restart) does not achieve live keyspace
  convergence on the demoted node. Split-brain writes made during a promotion window survive a
  failback.
- Durable data (WAIT-confirmed writes present on both sides before the split) is unaffected and
  converges correctly — the turmoil test asserts exactly that and passes.

## What to verify / decide

- Confirm whether this is intended (restart-to-install by design, matching a documented operational
  runbook) or a gap. If intended, document the "restart required after runtime REPLICAOF to a new
  master" contract prominently (consistency.md / operator CONTEXT.md) and add a test asserting the
  staged-then-restart install path converges.
- If not intended, install the staged checkpoint into the live store at runtime (flush live keyspace
  + load snapshot) as part of the `receive_checkpoint` commit, then assert full live-keyspace
  reconvergence (all keys, not just the durable subset) after runtime failback.

## References

- `frogdb-server/crates/replication/src/replica/connection.rs` `receive_checkpoint` (staged-not-installed)
- `frogdb-server/crates/server/tests/simulation.rs` `test_replication_failover_wgl_linearizable`
  (failback convergence asserts the durable subset only; boundary noted inline referencing this issue)
- Sibling boundary: a promoted replica does not start a `PrimaryReplicationHandler` and so cannot
  serve PSYNC to a sub-replica (see issue 23 Resolution) — which is why the test fails back toward
  the recovered boot-primary rather than re-attaching the old primary under the promoted node.
