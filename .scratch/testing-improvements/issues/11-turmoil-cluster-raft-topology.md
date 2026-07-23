# No deterministic (turmoil) simulation coverage for cluster/raft topology

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 3/3, consequence 2/3 (score 6)
Area: cluster

## Context

`simulation.rs` (4309 lines, the turmoil-based deterministic simulation harness) has zero cluster
coverage: no Raft, no `MOVED`/`ASK` redirects, no slot migration, no multi-node topology at all. Its
only cluster-adjacent tests are internal single-shard hash-tag tests (lines 525-536), which don't
exercise multiple nodes. Today, multi-node behavior is only exercised by integration tests (real
processes, no fault injection, no linearizability checking) or by the Jepsen suite (not in CI —
see `10-jepsen-nightly-ci.md`). This leaves a gap for fast, deterministic, seed-reproducible,
per-PR coverage of timing-sensitive cluster/raft paths — exactly the class of bug (leader partition
mid-migration, redirect races) that's hardest to catch in slower, less-reproducible test tiers.

This was independently flagged by two source reports: area F as
`no-cluster-topology-in-turmoil` (F#6) and area G as the cluster/raft half of
`no-deterministic-sim-for-replication-or-raft`. The adversarial verification pass merged them,
with F's framing (cluster+raft topology sim) winning as the primary task description; G's
*replication*-topology half of the same gap is tracked separately (outside this task's scope) as
`turmoil-replication-topology`.

## What to build

- Turmoil-based `sim_harness` extension covering cluster topology: multiple simulated nodes running
  real Raft, slot ownership, and migration state machinery.
- Writes routed through `MOVED`/`ASK` redirects deterministically.
- Scenario: leader partition occurring mid-slot-migration — assert eventual single-owner
  convergence deterministically (same seed always reproduces the same outcome, enabling bisection).

## Acceptance criteria

- [x] Turmoil simulation supports ≥2-node cluster topology with real Raft leader election
- [x] Test: writes through the simulation correctly follow `MOVED`/`ASK` redirects to converge on
      the correct owner
- [x] Test: leader partitioned from the raft group while a slot migration is in flight; assert
      deterministic single-owner convergence once the partition heals
- [x] Runs fast enough (or is tiered appropriately) for per-PR CI, given its deterministic/seeded
      nature

## Blocked by

None - can start immediately. Complements the higher-fidelity, longer-running Jepsen equivalent in
`03-jepsen-failover-durability-workload.md` and `06-jepsen-raft-chaos-blind-checker.md` (different
fault classes, same underlying leader-partition/migration risk area).

## References

- `server/tests/simulation.rs` (4309 lines total; module doc line 6; cluster-relevant tests only at
  `:525-536`, internal single-shard hash-tag only)
- `server/tests/simulation.rs` `test_network_partition_client_isolated` — client isolation only, not
  multi-node
- Source: `.scratch/testing-improvements/audit/F-cluster.md` gap #6 `no-cluster-topology-in-turmoil`,
  `.scratch/testing-improvements/audit/verdicts-F.md` #6 ("MERGE with G's raft half into one 'deterministic
  multi-node cluster+raft sim' task; keep G replication-sim half separate")
- Source: `.scratch/testing-improvements/audit/G-jepsen-harness.md`
  `no-deterministic-sim-for-replication-or-raft`, `.scratch/testing-improvements/audit/verdicts-G.md` (same,
  "DEDUP: F#6 framing wins for cluster half; keep replication half here or merge into one infra
  task")

## Resolution

Done 2026-07-23. All four acceptance criteria met. A real 3-node openraft cluster now runs
over turmoil's simulated network in `server/tests/simulation.rs`, and both scenario tests pass
deterministically per seed and fast enough for the per-PR turmoil tier.

### What was built

- **Cluster topology over turmoil.** `spawn_cluster_hosts` registers 3 hosts
  (`cluster-n1/2/3`), each running a real server via
  `real_frogdb_cluster_node` (`tests/common/sim_helpers.rs`): `cluster.enabled`, auto-hashed
  node IDs (lowest-hash node = deterministic bootstrap leader), a persistent RocksDB Raft log,
  `election_timeout_ms=300` / `heartbeat_interval_ms=50`, `auto_failover=false`. Real openraft
  leader election, slot ownership, and `CLUSTER SETSLOT` migration state all run inside the sim.
  RESP2 client helpers follow `MOVED`/`ASK`/`CLUSTERDOWN`/`TRYAGAIN` (`exec_following_redirects`,
  `assert_single_owner`, `single_owner_soft`, `node_id_of`, `owner_host_of`).
- **Test A — `test_cluster_moved_redirect_convergence`** (seeds 1, 7, 42; ~2.4s): writes 8 keys
  by following `MOVED` redirects to the owning node, then asserts each key reads back and the slot
  has exactly one local-serve owner with all peers `MOVED` to the same address. Covers criteria 1+2.
- **Test B — `test_cluster_leader_partition_mid_migration_converges`** (seeds 1, 2, 3; ~2.7s):
  begins a slot migration (`SETSLOT IMPORTING`+`MIGRATING`), isolates the bootstrap leader
  mid-migration, drives the ownership commit (`SETSLOT NODE`) which the re-elected majority
  accepts, lifts the isolation, and asserts deterministic single-owner convergence on the
  migration target across every node. Covers criterion 3.
- **CI tiering (criterion 4):** both sims live in the existing `turmoil`-feature tier
  (`just concurrency-turmoil` / `just lint-turmoil`). A `.config/nextest.toml` override gives
  `test(simulation::test_cluster_)` a 30s-slow / 120s-hard budget under the `cluster` test group
  (the default 15s cap is for lighter sims); actual runtime is ~2.5s each.

### Production changes required to run raft under turmoil

- `server/listeners.rs`: bind the cluster bus to `UNSPECIFIED:port` under `--features turmoil`
  (turmoil's `TcpListener::bind` rejects specific-IP binds); advertised address is unchanged.
- `server/failure_detector.rs`: reachability probe uses `crate::net::TcpStream` (turmoil-aware)
  instead of `tokio::net::TcpStream` (real tokio IO is disabled inside a sim).
- `server/cluster_init.rs`: inject a turmoil connect factory for the Raft network (mirrors
  `replication_init.rs`), with a 2s connect timeout — see the boundary note below.
- `server/cluster_bus.rs`: turmoil-framed `handle_connection` variant for the accept loop.

### Boundary hit and worked around: turmoil ephemeral-port leak on cancelled dials

Raft under an *indefinite* network `partition` is **not** feasible under turmoil 0.7.1. turmoil
registers a connection's ephemeral port at dial time (`StreamSocket`, `ref_ct=2`) and only reclaims
it when the `TcpStream`'s read/write halves drop — which are constructed *only after* SYN-ACK
(`turmoil/src/net/tcp/stream.rs:69-93`, `host.rs:376-389`). openraft's one-shot (unpooled) Raft
transport re-dials unreachable peers continuously, and `partition` *drops* the SYN, so every such
dial is cancelled before SYN-ACK and leaks its port permanently — a sustained partition exhausts
the host's entire port range (`Host: 'cluster-nN' ports exhausted`), regardless of connect-timeout
tuning (shorter timeout = faster re-dial = faster leak; no timeout = openraft never retries, so the
cluster can't reconverge after heal).

Test B works around this by isolating the leader with turmoil's `hold`/`release` (which *queues*
traffic rather than dropping it) instead of `partition`: the isolated leader's in-flight dials stay
pending — holding their ports without leaking — and complete on `release`, while the followers are
still starved of heartbeats past the election timeout, so the majority re-elects and commits (the
property under test). Two supporting tuning knobs on the Builder: `ephemeral_ports(2048..=65535)`
(headroom for dials in flight during the hold) and `tcp_capacity(65536)` (the queued SYNs all flush
into the victim's cluster-bus accept deque in one step on release, overflowing turmoil's default
per-socket capacity of 64). The 2s connect timeout in `cluster_init.rs` is the balance point: long
enough that the re-dial cadence doesn't leak ports faster than the brief hold window can absorb,
short enough to retry promptly once the hold lifts so the recovered node reconverges.

Follow-up (optional): a true drop-based `partition` of a running raft node would need either an
upstream turmoil fix (reclaim the ephemeral port when a pending connect future is dropped) or a
pooled/bounded-dial Raft transport. Filed as a candidate for the backlog; not required for this
task's acceptance criteria, which the `hold`-based isolation satisfies.
