---
name: jepsen-testing
description: >
  Running Jepsen tests, debugging Jepsen failures, writing new Jepsen workloads,
  interpreting Jepsen results (Elle, Knossos, checker output), Docker topology
  management (single/replication/raft), cluster convergence issues, nemesis/fault
  injection, consistency testing, lein commands, and any work touching files under
  testing/jepsen/.
---

# Jepsen Testing for FrogDB

FrogDB uses [Jepsen](https://jepsen.io) for distributed systems correctness testing.
Tests verify linearizability, serializability, durability, and consistency properties
across three topologies: single-node, 3-node replication, and 5-node Raft cluster.

## Architecture

```
Host machine
├── run.py              Orchestration (Docker lifecycle, test execution, build detection)
├── docker-compose.yml  Single-node topology
└── frogdb/
    ├── project.clj                    Leiningen project (Jepsen 0.3.5, Carmine 3.3.2)
    ├── docker-compose.replication.yml 3-node primary + 2 replicas
    ├── docker-compose.raft-cluster.yml 5-node Raft cluster
    └── src/jepsen/frogdb/
        ├── core.clj         Entry point, test construction, CLI options, workload dispatch
        ├── client.clj        RESP client (Carmine), connection setup, operations
        ├── db.clj            Database lifecycle, Docker container management
        ├── cluster_db.clj    Raft cluster DB lifecycle, CLUSTER commands, convergence
        ├── cluster_client.clj CRC16, slot mapping, MOVED/ASK handling
        ├── nemesis.clj       Fault injection (kill, pause, partition, clock, disk, network)
        ├── membership.clj    Cluster membership utilities
        └── <workload>.clj    20 workload files (register, counter, list_append, etc.)
```

## Running Tests

### Primary interface: `just` recipes

```bash
# Single test
just jepsen register --time-limit 30

# Run a suite (single, crash, replication, raft, raft-extended, all)
just jepsen-suite raft

# Docker lifecycle
just jepsen-up single          # Start topology
just jepsen-up replication
just jepsen-up raft
just jepsen-down               # Stop all topologies
just jepsen-down raft          # Stop specific topology

# Inspect
just jepsen-list               # List all tests and suites
just jepsen-summary            # Pass/fail summary from store/
just jepsen-results            # Open latest results in browser
just jepsen-image-info         # Docker image labels (git hash, build timestamp)
```

### Using run.py directly (advanced)

```bash
cd testing/jepsen
uv run run.py run register --time-limit 30
uv run run.py run --suite raft --build            # Auto-rebuild if source changed
uv run run.py run --suite all --stop-on-failure   # Stop on first failure
uv run run.py build                                # Force rebuild
uv run run.py up raft                              # Start topology
uv run run.py down                                 # Stop all
uv run run.py summary
```

### IMPORTANT: Rebuild after server code changes

After modifying FrogDB server code (anything under `frogdb-server/`), you **must** rebuild
the Docker image before running Jepsen tests:

```bash
just docker-build-debug   # Debug build (recommended for Jepsen — includes debug tools)
```

The `--build` flag on `just jepsen-suite` and `run.py` auto-detects source changes via
SHA256 stamp at `/tmp/frogdb-jepsen-build-stamp`.

## Interpreting Results

### Results location

```
testing/jepsen/frogdb/store/
├── latest -> <most-recent-test>/    # Symlink
└── frogdb-<workload>[-<nemesis>]-<topology>-<timestamp>/
    ├── results.edn      # Verdict and checker output
    ├── jepsen.log       # Full execution log
    ├── history.txt      # Operation history (invoke/ok/fail/info)
    └── ...
```

### Reading results.edn

- **`:valid? true`** — All checkers passed
- **`:valid? false`** — At least one checker found a violation
- **`:valid? :unknown`** — Couldn't determine (usually timeout/crash)

### Checker-specific output

**Knossos (linearizability)** — Used by `register` workload:
- `:model` shows the expected state model
- If invalid, `:final-paths` shows the non-linearizable operation sequence
- Look for competing reads/writes that violate single-register semantics

**Elle (serializability)** — Used by `list-append` workload:
- `:anomalies` map shows detected anomalies by type:
  - `G0` (write cycle) — Two transactions each overwrote the other's write
  - `G1a` (aborted read) — Read a value written by an aborted transaction
  - `G1b` (intermediate read) — Read an intermediate state of another transaction
  - `G1c` (circular information flow) — Cycle in write-read dependencies
  - `G2` (anti-dependency cycle) — Cycle involving anti-dependencies
- `:not #{:strict-serializable}` means strict serializability was violated
- `:cycle-search-timeout` means Elle couldn't finish checking (not a failure)

**Counter checker** — Used by `counter` workload:
- Compares `:expected` (sum of all successful `:add` operations) vs `:actual` (final read)
- Mismatch means lost or duplicate increments

**Stats checker** (always present):
- `:ok-count`, `:fail-count`, `:info-count` — operation outcome counts
- High `:info-count` usually means timeouts, not real failures

### Common "red herrings"

- **`:info` operations** are indeterminate — the client couldn't confirm success or failure
  (usually timeout). These are NOT failures. Only `:fail` with unexpected results are failures.
- **"Couldn't find a linearization"** with very short time limits may be a timeout, not a real
  violation — increase `--time-limit`.

## Docker Topology Details

| Topology | Compose File | Nodes | Network CIDR | Base Ports |
|----------|-------------|-------|-------------|------------|
| single | `testing/jepsen/docker-compose.yml` | n1 | default bridge | 16379 |
| replication | `frogdb/docker-compose.replication.yml` | n1-n3 | 172.20.0.0/16 | 16379-16381 |
| raft | `frogdb/docker-compose.raft-cluster.yml` | n1-n5 | 172.21.0.0/16 | 16379-16383 (client), 26379-26383 (bus) |

### Parallel mode port ranges

When run.py runs tests in parallel, each topology gets a non-conflicting base port:
- Single: 16379
- Replication: 17379
- Raft: 18379

### Container capabilities

- **Replication**: `NET_ADMIN` (iptables-based network partitions)
- **Raft**: `NET_ADMIN` + `SYS_PTRACE` (partitions + process manipulation)

### Stale container cleanup

If tests fail with connection errors or stale state:

```bash
just jepsen-down                    # Stop all topologies
docker ps -a | grep frogdb          # Verify no containers remain
just jepsen-up <topology>           # Fresh start
```

For stubborn containers: `docker compose -f <compose-file> down -v` to remove volumes too.

### Health checks

All containers have `redis-cli -p 6379 PING` health checks (1s interval, 30 retries).
`docker compose --wait` (used by run.py) blocks until health checks pass.

## Cluster Convergence Debugging

Raft clusters auto-bootstrap via `FROGDB_CLUSTER__INITIAL_NODES` environment variable.

### Convergence sequence

1. Nodes start and discover peers via initial nodes list
2. Raft leader election (timeout: 30s in `cluster_db.clj`)
3. 16384 hash slots distributed evenly across masters (assigned in batches of 1000)
4. Cluster state transitions to `"ok"` (timeout: 90s in `cluster_db.clj`)

### Key convergence checks

```bash
# Inside a container (via docker exec)
redis-cli -p 6379 CLUSTER INFO     # cluster_state:ok, cluster_known_nodes:5
redis-cli -p 6379 CLUSTER NODES    # All nodes visible, slots assigned
```

### Common convergence failures

- **Stale Raft state**: Containers from previous runs have stale data in `/data/raft` and
  `/data/cluster`. Fix: `just jepsen-down` + re-up (volumes are recreated).
  `cluster_db.clj` also wipes these directories on convergence failure.
- **Leader election timeout**: Usually caused by network issues between containers or
  too few nodes. Check Docker network connectivity.
- **Slot assignment stuck**: Look for "slot addresses resolved" in Jepsen log. If not all
  slots are assigned, check that enough masters are up.

## Key Technical Details

- **AWT headless**: `-Djava.awt.headless=true` in `project.clj` prevents Elle visualization
  from hanging on macOS (blocks on XPC connection to hiservices without a display).
- **Hash tags**: Use `{tag}key` to colocate keys to the same hash slot in multi-shard tests.
  CRC16 is computed on the `{tag}` portion only.
- **Network partitions**: Implemented via `iptables` rules inside containers. Requires
  `NET_ADMIN` capability. `heal-all!()` flushes all rules with `iptables -F`.
- **Process control**: `docker exec` + `pkill` for SIGKILL/SIGSTOP/SIGCONT. Not SSH.
- **Carmine connection pools**: Memoized by default. Each client uses
  `{:id (UUID/randomUUID)}` to get isolated per-thread pools — see `client.clj`
  `single-conn-pool-opts()`.
- **Cluster client slot routing**: `cluster_client.clj` implements CRC16 (XMODEM variant),
  hash tag extraction, slot→node mapping via `CLUSTER SLOTS`, and MOVED/ASK redirect handling
  with Docker IP remapping (172.21.0.x → localhost:hostPort).

## Reference Files

For detailed information beyond this overview, read these reference files:

- **`references/test-catalog.md`** — Full catalog of all 34 tests with workload, nemesis,
  topology, time limit, and suite membership. Read this when choosing which tests to run
  or understanding test coverage.

- **`references/writing-workloads.md`** — Guide to authoring new Jepsen workloads. Read this
  when creating or modifying a workload: file structure, client protocol, generator patterns,
  checker selection, registration in core.clj and run.py.

- **`references/troubleshooting.md`** — Common failure modes and how to fix them. Read this
  when diagnosing test failures: Docker issues, cluster convergence, checker interpretation,
  network problems, JVM issues.
