# WAIT has zero coverage in cluster mode

Status: done
PRD: [replication-cluster-rework/wait-cluster-mode.md](../../replication-cluster-rework/wait-cluster-mode.md)
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: cluster (area F)

## Context

Grepping `WAIT` in `integration_cluster.rs` returns zero hits (verdicts pass confirms the only
hits anywhere are comments containing "Wait for", not the command). There is no test asserting
which replicas count toward `WAIT`'s numreplicas target in cluster mode (all replicas of the
slot's owning shard? cluster-wide?), nor any test of `WAIT` behavior during or immediately after
a failover — a scenario where the ack-counting logic is most likely to have edge-case bugs
(replica changing shard ownership mid-wait, target replica becoming the new primary, etc.).

Verdict (adversarial pass): CONFIRMED L2/C2.

## What to build

Integration tests exercising `WAIT` in a 3+ node cluster: normal ack counting against the correct
shard-local replica set, and `WAIT` behavior spanning a failover of the shard being written to.

## Acceptance criteria

- [x] Test: write a key in a 3-node cluster with the owning shard replicated, `WAIT <n> <timeout>`
      after the write, assert ack count matches the actual replica set for that shard (not
      cluster-wide replica count).
- [x] Test: `WAIT` issued around a failover of the shard being written to — assert it does not
      hang indefinitely and resolves per a documented/pinned semantics (e.g., counts the new
      replica set, or errors, whichever is the intended contract).
- [x] Test: `WAIT` with `numreplicas` exceeding actual replica count times out per configured
      timeout rather than hanging past it.

## Blocked by

None - can start immediately

## References

- `server/tests/integration_cluster.rs` (grep `WAIT` → 0 real hits)
- `.scratch/testing-improvements/audit/F-cluster.md` (`wait-in-cluster-untested`, F#10)
- `.scratch/testing-improvements/audit/verdicts-F.md`

## Resolution

**What WAIT means in FrogDB cluster mode: it is effectively unwired — a cluster
primary always replies `0` immediately, counting no replicas; a cluster replica
rejects it.** This was determined empirically (probe test + code trace) and is
now pinned by three integration tests in `integration_cluster.rs`.

### Root cause (traced)

`WAIT numreplicas timeout` is served by the connection-level `WaitCoordinator`
that hangs off the node's `PrimaryReplicationHandler`
(`server/src/connection/blocking.rs::handle_wait_command`). That handler is
constructed **only** when the node's *replication* role is the literal string
`"primary"` (`server/src/server/replication_init.rs:57` gated on
`config/src/replication.rs::is_primary`, which is `role == "primary"`).

A cluster node started by the harness (and by cluster bootstrap generally) keeps
the **default replication role `"standalone"`** — its master/replica status is
tracked separately in Raft-backed *cluster* state, and nothing sets
`replication.role = "primary"`. So `primary_replication_handler == None`, and
`handle_wait_command` takes the standalone early return:

```rust
let Some(primary) = self.cluster.primary_replication_handler.clone() else {
    return Response::Integer(0);
};
```

Empirical confirmation on a 2-primary + 1-replica cluster after a `SET` on the
owning shard: `WAIT 0 100`, `WAIT 1 2000`, `WAIT 2 1000` **all** returned
`Integer(0)` in <1 ms (never approaching the timeout); `INFO replication` on the
primary showed `connected_slaves:0` and `master_repl_offset:0` even after the
write — the standalone PSYNC streaming path that WAIT counts against is not used
for cluster data flow at all.

### Pinned semantics (divergences documented in-test)

1. **Ack count is always 0** — no shard replica is wired into the WAIT
   coordinator. Diverges from Redis cluster WAIT (counts the shard-local replica
   set) and from FrogDB *standalone* WAIT (counts real acks).
2. **`WAIT 0` returns immediately** (0).
3. **`numreplicas` exceeding the counted set does not hang** — it does not even
   block to the timeout; it returns `0` at once (standalone would block up to
   `timeout` and then return the count).
4. **WAIT on a cluster replica is rejected** with the standard replica error
   (data-path `is_replica` flag, checked before arg parsing).
5. **Across failover WAIT never hangs.** When the owning primary is killed and
   its replica is promoted *in cluster state* (`CLUSTER NODES` reports it master),
   the promoted node's **data-path role is not re-installed** (consistent with
   known findings 34 "promoted nodes can't serve PSYNC" and 61 "runtime resync
   staged-not-installed"): it keeps its `is_replica` flag and still *rejects*
   WAIT — while surviving sibling primaries keep replying `0`. Either way WAIT
   resolves in milliseconds.

### Tests added (`integration_cluster.rs`)

- `test_wait_in_cluster_returns_zero_immediately` — 2-primary cluster + a real
  replica on the written shard; `WAIT {0,1,2}` each returns `0` **well under its
  own timeout** (a blocking regression that returned the same `0` only after
  idling the timeout would fail the elapsed bound). Covers acceptance criteria
  1 (ack count = counted set = 0), 2 (WAIT 0 immediate), 3 (numreplicas > actual
  does not hang past timeout).
- `test_wait_rejected_on_cluster_replica` — WAIT on a replica node errors with a
  message mentioning `replica`.
- `test_wait_in_cluster_does_not_hang_across_failover` — kill the owning primary,
  wait for promotion visible in cluster state, then assert both that WAIT
  resolves in <750 ms (no hang) **and** which answer it gives: the promoted node
  still returns the replica rejection (pinning the staged-not-installed
  data-path divergence, findings 34/61), while the surviving sibling primary
  still serves `0`. Hard-asserting the rejection is what makes the test trip if
  promotion ever starts installing the data-path role.

Flake rate: 0/5 — 5 consecutive green runs of all three on an aarch64 Linux
testbox (15/15 test executions passed; per-test wall times 1.3–2.6 s, i.e. the
elapsed bounds have >2x headroom). `just fmt frogdb-server` is a no-op and
`cargo clippy -p frogdb-server --tests -- -D warnings` is clean.

### Follow-up (not in scope here)

Two latent inconsistencies surfaced while probing (both pre-existing, unrelated
to WAIT, left un-fixed):
- A cluster replica's **own** `CLUSTER NODES` can report `myself ... master` while
  its data-path `is_replica` flag is `true` — `CLUSTER FAILOVER` (reads cluster
  state) then refuses with "can only be run on a replica" while WAIT (reads the
  flag) rejects it as a replica. Same node, contradictory role views.
- Promotion staged in cluster state is not installed on the data path (findings
  34/61), so a promoted node never becomes a writable primary that could serve
  WAIT. Wiring cluster WAIT to count shard-local replicas is gated on that being
  fixed first.

### Superseded by the WAIT-cluster-mode PRD (2026-07-28)

All three pinning tests above have been rewritten, and both follow-ups are fixed,
by [`.scratch/replication-cluster-rework/wait-cluster-mode.md`](../../replication-cluster-rework/wait-cluster-mode.md)
(status: implemented, pending review):

- **The `0` divergence is gone.** WAIT in cluster mode counts this node's
  replicas from the same tracker standalone WAIT uses; there is no cluster
  special case in `handle_wait_command` at all.
  `test_wait_in_cluster_returns_zero_immediately` became
  `test_wait_in_cluster_counts_shard_replicas`.
- **`test_wait_rejected_on_cluster_replica`** survives unchanged — WAIT on a
  replica is still an error.
- **`test_wait_in_cluster_does_not_hang_across_failover`** now asserts the
  opposite outcome: a promoted node *accepts* WAIT, which is exactly the tripwire
  the original test was built to be.
- **Follow-up 1 (contradictory role views)** — fixed. Three defects had to line
  up: `AddNode` re-registration overwrote a recorded replica role with the
  self-claimed `Primary`; a role folded into a Raft snapshot reached the data
  path through no boot path; and boot peer-seeding overwrote a restored peer's
  Raft-agreed address with a guess. See PRD §7.5.
- **Follow-up 2 (promotion not installed on the data path)** — fixed by the
  raft→data-path promotion bridge (PRD Task 4). Issue 61's staged-not-installed
  checkpoint had already been fixed independently.
