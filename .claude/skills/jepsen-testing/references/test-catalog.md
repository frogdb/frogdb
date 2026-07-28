# Jepsen Test Catalog

## Test Definitions

All tests are defined in `testing/jepsen/run.py` as `TestDefinition` entries in the `TESTS` tuple.

### Single-Node Basic (no nemesis)

| Test | Workload | Nemesis | Time Limit | What It Tests |
|------|----------|---------|------------|---------------|
| `register` | register | none | 30s | Linearizable read/write/CAS on a single key (Knossos) |
| `counter` | counter | none | 30s | Atomic INCRBY — final value = sum of successful adds |
| `append` | append | none | 30s | Append-only durability (values survive restarts) |
| `transaction` | transaction | none | 30s | MULTI/EXEC atomicity across multiple keys |
| `queue` | queue | none | 30s | FIFO ordering with LPUSH/RPOP |
| `set` | set | none | 30s | Set membership with SADD/SMEMBERS |
| `hash` | hash | none | 30s | Hash field atomicity with HSET/HGET |
| `sortedset` | sortedset | none | 30s | Sorted set score/ranking consistency |
| `expiry` | expiry | none | 30s | TTL/expiration correctness |
| `blocking` | blocking | none | 30s | BLPOP/BRPOP blocking semantics |

### Single-Node Crash (with nemesis)

| Test | Workload | Nemesis | Time Limit | What It Tests |
|------|----------|---------|------------|---------------|
| `crash` | register | kill | 60s | Linearizability survives SIGKILL crash/restart |
| `counter-crash` | counter | kill | 60s | Counter correctness through crashes |
| `append-crash` | append | kill | 60s | Append durability through crashes |
| `append-rapid` | append | rapid-kill | 60s | Append durability under aggressive kill cycles |
| `transaction-crash` | transaction | kill | 60s | Transaction atomicity through crashes |
| `sortedset-crash` | sortedset | kill | 60s | Sorted set consistency through crashes |
| `expiry-crash` | expiry | kill | 60s | Expiry correctness through crashes |
| `expiry-rapid` | expiry | rapid-kill | 60s | Expiry under aggressive kill cycles |
| `blocking-crash` | blocking | kill | 60s | Blocking ops through crashes |
| `register-pause` | register | pause | 60s | Linearizability through SIGSTOP/SIGCONT pauses |
| `register-all` | register | all | 60s | Linearizability under the composed single-node kill+pause nemesis |

### Register Linearizability Under Faults (`register-fault` suite)

The Knossos cas-register (single-key linearizability) driven under fault injection, bounded
in time so the Knossos search stays tractable. `register-pause`/`register-all` are single-node;
`register-partition` pins the client to the primary (`--node n1`) on the 3-node replication
topology so the linearizable model stays valid (async replicas would be non-linearizable by
design) while the partition nemesis isolates that primary from its replicas. It also caps
`--concurrency 2` (Knossos `:linear` is exponential in concurrency).

Under partition, the isolated primary rejects writes with `CLUSTERDOWN` (quorum-safety). The
register client classifies `CLUSTERDOWN`/`READONLY` rejections as `:fail` (the write was
declined before mutating state), not the indeterminate `:info` — otherwise every rejection is
a pending op and the Knossos search explodes into OOM.

| Test | Workload | Nemesis | Topology | Time Limit | What It Tests |
|------|----------|---------|----------|------------|---------------|
| `register-pause` | register | pause | single | 60s | Linearizability through SIGSTOP/SIGCONT pauses |
| `register-all` | register | all | single | 60s | Linearizability under composed kill+pause |
| `register-partition` | register | partition | replication (client pinned to n1, `--concurrency 2`) | 30s | Primary stays available + linearizable when isolated from replicas |

### Replication

| Test | Workload | Nemesis | Time Limit | What It Tests |
|------|----------|---------|------------|---------------|
| `replication` | replication | none | 30s | 3-node replication consistency (writes propagate) |
| `lag` | lag | none | 30s | Replication lag measurement |
| `split-brain` | split-brain | partition | 60s | Behavior under network partition (primary isolation) |
| `zombie` | zombie | partition | 60s | Zombie primary detection after partition heals |
| `replication-chaos` | replication | all-replication | 120s | Replication under combined faults |
| `partition-recovery` | partition-recovery | partition | 90s | Replica catch-up + convergence after a partition heals |

### Raft Cluster Core

| Test | Workload | Nemesis | Time Limit | What It Tests |
|------|----------|---------|------------|---------------|
| `cluster-formation` | cluster-formation | none | 30s | Cluster bootstrap, CLUSTER MEET/FORGET |
| `leader-election` | leader-election | none | 30s | Raft leader election correctness |
| `slot-migration` | slot-migration | none | 60s | Hash slot redistribution during changes |
| `cross-slot` | cross-slot | none | 30s | Hash tag transactions across slots |
| `key-routing` | key-routing | none | 30s | MOVED/ASK redirect handling + data (durability + value-correctness) |

### Raft Cluster with Faults

| Test | Workload | Nemesis | Time Limit | What It Tests |
|------|----------|---------|------------|---------------|
| `leader-election-partition` | leader-election | partition | 60s | Leader election under network partitions |
| `key-routing-kill` | key-routing | kill | 60s | Key routing through node crashes + value-correctness (durability informational under kills) |
| `slot-migration-partition` | slot-migration | partition | 90s | Slot migration under partitions |
| `raft-chaos` | key-routing | raft-cluster | 120s | Key routing under combined Raft faults + data validation (durability when no kill, value-correctness always) |

### Raft Cluster Membership + Recovery

These workloads drive their own fault injection through operations (CLUSTER MEET,
kill-leader, restart-node, slot migration), so they run with the `none` nemesis.

| Test | Workload | Nemesis | Time Limit | What It Tests |
|------|----------|---------|------------|---------------|
| `migration-recovery` | migration-recovery | none | 120s | Leader crash mid slot-migration; cluster recovers, no data loss |
| `concurrent-migration` | concurrent-migration | none | 90s | 4 parallel slot migrations converge to consistent owners |
| `membership-routing` | membership-routing | none | 120s | Add node (CLUSTER MEET) + slot handoff; MOVED handling, durability |
| `rolling-restart` | rolling-restart | none | 120s | Sequential node restarts; availability >80%, no data loss |

### Raft Extended Nemesis

| Test | Workload | Nemesis | Time Limit | What It Tests |
|------|----------|---------|------------|---------------|
| `clock-skew` | register | clock-skew | 60s | Linearizability under clock manipulation |
| `disk-failure` | register | disk-failure | 60s | Linearizability under disk faults |
| `slow-network` | register | slow-network | 60s | Linearizability under network latency |
| `memory-pressure` | register | memory-pressure | 60s | Linearizability under memory exhaustion |

## Suite Definitions

Counts below reflect the current `TESTS` tuple; run `uv run run.py list` for the authoritative set.

| Suite | Tests | Description |
|-------|-------|-------------|
| `single` | 11 basic single-node tests | Baseline correctness without faults |
| `crash` | 22 | Single-node basics + crash/pause/composed nemesis variants |
| `replication` | 7 | 3-node replication topology |
| `replication-extended` | 2 | Clock-skew + slow-network on the replication topology |
| `raft` | 18 | 5-node Raft cluster (core + faults + membership/recovery) |
| `raft-extended` | 4 | Advanced fault injection on Raft (Elle) |
| `register-fault` | 3 | Knossos register under pause / partition / composed faults |
| `all` | 47 | Everything except the `*-extended` suites and the pinned `register-partition` variant |

## Nemesis Types

| Nemesis | Description | Topology |
|---------|-------------|----------|
| `none` | No faults (baseline) | Any |
| `kill` | SIGKILL + restart cycles | Any |
| `pause` | SIGSTOP/SIGCONT process pauses | Any |
| `rapid-kill` | Aggressive kill/restart (3s interval, 1s restart delay) | Any |
| `partition` | iptables network partitions (primary isolation, halves) | Replication, Raft |
| `clock-skew` | Clock manipulation via libfaketime | Raft |
| `disk-failure` | Read-only remount, disk full (dd) | Raft |
| `slow-network` | tc/netem latency and packet loss | Raft |
| `memory-pressure` | stress-ng memory exhaustion | Raft |
| `all` | Combined single-node faults (kill + pause) | Single |
| `all-replication` | Combined replication faults (kill + partition) | Replication |
| `raft-cluster` | Combined Raft faults (kill + partition) | Raft |
| `raft-cluster-membership` | Raft faults + membership changes | Raft |

## CLI Option Reference

### Via `just jepsen`

```bash
just jepsen <test-name> [options]
```

Options are passed through to `lein run test`:

| Flag | Default | Description |
|------|---------|-------------|
| `-w`, `--workload NAME` | register | Workload to run |
| `--nemesis NAME` | none | Nemesis type (see table above) |
| `-r`, `--rate N` | 10 | Operations per second |
| `--interval N` | 10 | Nemesis interval in seconds |
| `--independent` | false | Multi-key register testing |
| `--docker` | false | Docker testing mode |
| `--local` | false | Local testing mode (no Docker) |
| `--replication` | false | Use 3-node replication |
| `--cluster` | false | Use Raft cluster mode |
| `--cluster-nodes N` | 3 | Number of cluster nodes (1-5) |
| `--base-port N` | 16379 | Base host port for Docker mapping |
| `--time-limit N` | (from test def) | Test duration in seconds |

### Via `run.py`

```bash
uv run testing/jepsen/run.py <subcommand> [options]
```

| Subcommand | Description |
|-----------|-------------|
| `run [TEST]` | Run a single test or suite |
| `run --suite NAME` | Run a test suite |
| `list` | List all tests and suites |
| `build` | Force rebuild Docker image |
| `up TOPOLOGY` | Start Docker Compose for topology |
| `down [TOPOLOGY]` | Stop topology (all if omitted) |
| `clean` | Remove all test results from store/ |
| `results` | Open latest results in browser |
| `summary` | Print pass/fail summary |

`run` subcommand flags:

| Flag | Description |
|------|-------------|
| `--build` | Auto-rebuild Docker image if source changed |
| `--stop-on-failure` | Stop suite on first test failure |
| `--parallel` | Run tests across topologies in parallel |
| `--time-limit N` | Override time limit for all tests |
