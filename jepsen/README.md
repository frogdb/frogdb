# FrogDB Jepsen Tests

Distributed consistency tests for FrogDB using the [Jepsen](https://jepsen.io/) framework. These tests verify linearizability, durability, and correctness under fault injection across single-node, replication, and Raft cluster topologies.

## Prerequisites

| Dependency | Purpose |
|---|---|
| Docker Desktop | Runs FrogDB nodes in containers |
| Java (JDK 11+) | Runtime for Leiningen/Clojure |
| Leiningen | Clojure build tool that runs Jepsen |
| Zig | Cross-compiler for building Linux binaries on macOS/ARM (not needed on native Linux x86_64) |
| cargo-zigbuild | Cargo plugin for zig-based cross-compilation (not needed on native Linux x86_64) |

### macOS (Homebrew)

Install Docker Desktop manually, then:

```bash
brew bundle                    # Installs everything from Brewfile
cargo install cargo-zigbuild   # Or: just cross-install
```

### Linux (Nix)

Install Docker as a system service, then:

```bash
nix-shell                      # Loads all deps from shell.nix
cargo install cargo-zigbuild   # Or: just cross-install
```

### Debian/Ubuntu (apt)

```bash
sudo apt install default-jdk leiningen zig docker.io
cargo install cargo-zigbuild
```

### Arch Linux (pacman)

```bash
sudo pacman -S jdk-openjdk leiningen zig docker
cargo install cargo-zigbuild
```

> **Note:** On native Linux x86_64, `zig` and `cargo-zigbuild` are unnecessary — you can build the FrogDB binary directly with `cargo build --release` and copy it into the Docker image.

## Quick Start

All commands run from the repository root:

```bash
just jepsen-build                        # Cross-compile + build Docker image
just jepsen register --time-limit 30     # Run a single test (auto-starts topology)
just jepsen-results                      # Open results in browser (macOS)
just jepsen-down                         # Tear down containers
```

## Running Tests

### Single Test

Use `just jepsen <name>` to run any test by name. The script automatically starts the required Docker Compose topology:

```bash
just jepsen register --time-limit 30         # Single-node linearizability
just jepsen append-crash --time-limit 60     # Single-node crash recovery
just jepsen split-brain --time-limit 60      # Replication partition test
just jepsen raft-chaos --time-limit 120      # Raft combined faults
```

Extra flags are passed through to `lein`:

```bash
just jepsen register --time-limit 60 --rate 20
```

### Test Suites

Run predefined groups of tests. Suite commands auto-build and manage topology lifecycle:

```bash
just jepsen-all              # All single + crash + replication + raft tests
just jepsen-replication-all  # All 3-node replication tests
just jepsen-raft-all         # All 5-node Raft cluster tests
```

Or use `run.py` directly for more control:

```bash
uv run jepsen/run.py run --suite single             # 10 basic single-node tests
uv run jepsen/run.py run --suite crash              # 19 tests: basic + crash variants
uv run jepsen/run.py run --suite replication        # 5 replication workloads
uv run jepsen/run.py run --suite raft               # 9 core Raft tests
uv run jepsen/run.py run --suite raft-extended      # 4 extended nemesis tests
uv run jepsen/run.py run --suite all                # Everything except raft-extended
uv run jepsen/run.py run --suite all --build        # Build first, then run all
uv run jepsen/run.py run --suite crash --teardown   # Tear down containers after
uv run jepsen/run.py run --suite all --stop-on-failure  # Abort on first failure
```

### Listing Tests

```bash
just jepsen-list                                     # All tests and suites
uv run jepsen/run.py list --topology single          # Filter by topology
uv run jepsen/run.py list --topology raft            # Only Raft tests
```

## `run.py` CLI Reference

```
jepsen/run.py run <test>                 # Run a single named test
jepsen/run.py run --suite <name>         # Run a predefined suite
jepsen/run.py list [--topology X]        # List tests/suites
jepsen/run.py build                      # Cross-compile + docker build
jepsen/run.py up <single|replication|raft>
jepsen/run.py down [topology]            # Omit = tear down all
jepsen/run.py clean                      # rm -rf jepsen/frogdb/store/
jepsen/run.py results                    # Open latest results in browser
```

### `run` Flags

| Flag | Default | Description |
|---|---|---|
| `--build / --no-build` | no-build | Cross-compile + docker build before running |
| `--teardown / --no-teardown` | no-teardown | Tear down compose after running |
| `--time-limit N` | per-test default | Override default time-limit for all tests |
| `--stop-on-failure` | off | Abort suite on first failure |
| `--no-color` | auto-detect | Disable ANSI colors |

Extra arguments after the test name are passed through to `lein run test`.

## Test Suites

### Single-Node Tests (`single` suite)

Tests run against a single FrogDB container.

| Test | Workload | Nemesis | What it tests |
|---|---|---|---|
| `register` | register | none | Linearizable reads/writes |
| `counter` | counter | none | Atomic increment/decrement |
| `append` | append | none | List append durability |
| `transaction` | transaction | none | Multi-key atomicity |
| `queue` | queue | none | FIFO ordering |
| `set` | set | none | Set membership consistency |
| `hash` | hash | none | Field-level atomicity |
| `sortedset` | sortedset | none | Score/ranking consistency |
| `expiry` | expiry | none | TTL/expiration correctness |
| `blocking` | blocking | none | BLPOP/BRPOP semantics |

### Crash Tests (`crash` suite)

Includes all single-node basic tests plus crash variants:

| Test | Workload | Nemesis | What it tests |
|---|---|---|---|
| `crash` | register | kill | Register durability across crashes |
| `counter-crash` | counter | kill | Counter durability across crashes |
| `append-crash` | append | kill | Append durability across crashes |
| `append-rapid` | append | rapid-kill | Append durability under rapid kills |
| `transaction-crash` | transaction | kill | Transaction atomicity across crashes |
| `sortedset-crash` | sortedset | kill | Sorted set durability across crashes |
| `expiry-crash` | expiry | kill | TTL correctness across crashes |
| `expiry-rapid` | expiry | rapid-kill | TTL correctness under rapid kills |
| `blocking-crash` | blocking | kill | Blocking op semantics across crashes |

### Replication Tests (`replication` suite, 3-node)

| Test | Workload | Nemesis | What it tests |
|---|---|---|---|
| `replication` | replication | none | Replication consistency |
| `lag` | lag | none | Replication lag measurement |
| `split-brain` | split-brain | partition | Split-brain detection |
| `zombie` | zombie | partition | Zombie primary detection |
| `replication-chaos` | replication | all-replication | Combined kill + pause + partition faults |

### Raft Cluster Tests (`raft` suite, 5-node)

| Test | Workload | Nemesis | What it tests |
|---|---|---|---|
| `cluster-formation` | cluster-formation | none | Cluster membership |
| `leader-election` | leader-election | none | Raft leader election |
| `slot-migration` | slot-migration | none | Hash slot redistribution |
| `cross-slot` | cross-slot | none | Hash tag transactions |
| `key-routing` | key-routing | none | MOVED/ASK redirects |
| `leader-election-partition` | leader-election | partition | Leader election under partitions |
| `key-routing-kill` | key-routing | kill | Key routing under node kills |
| `slot-migration-partition` | slot-migration | partition | Slot migration under partitions |
| `raft-chaos` | key-routing | raft-cluster | Combined Raft cluster faults |

### Raft Extended Tests (`raft-extended` suite)

| Test | Workload | Nemesis | What it tests |
|---|---|---|---|
| `clock-skew` | register | clock-skew | Register correctness under clock skew |
| `disk-failure` | register | disk-failure | Register correctness under disk failures |
| `slow-network` | register | slow-network | Register correctness under slow network |
| `memory-pressure` | register | memory-pressure | Register correctness under memory pressure |

### Standalone Tests

| Test | Workload | Nemesis | What it tests |
|---|---|---|---|
| `nemesis-pause` | register | pause | Register correctness under SIGSTOP/SIGCONT |

## Compose Lifecycle

The `run` subcommand manages topology automatically, but you can also control it manually:

```bash
# Start/stop topologies
just jepsen-up                  # Single-node
just jepsen-down                # Single-node
just jepsen-replication-up      # 3-node replication
just jepsen-replication-down    # 3-node replication
just jepsen-raft-cluster-up     # 5-node Raft cluster
just jepsen-raft-cluster-down   # 5-node Raft cluster

# Tear down all topologies at once
uv run jepsen/run.py down
```

## Running Tests Directly

For full control, run `lein run test` from `jepsen/frogdb/`:

```bash
cd jepsen/frogdb
lein run test --workload register --nemesis none --time-limit 60
```

### CLI Options

| Flag | Default | Description |
|---|---|---|
| `-w`, `--workload WORKLOAD` | `register` | Workload to run (see workload list below) |
| `--nemesis NEMESIS` | `none` | Nemesis type: `none`, `kill`, `pause`, `rapid-kill`, `partition`, `clock-skew`, `disk-failure`, `slow-network`, `memory-pressure`, `all`, `all-replication`, `raft-cluster` |
| `-r`, `--rate RATE` | `10` | Operations per second |
| `--interval INTERVAL` | `10` | Nemesis interval in seconds |
| `--time-limit SECONDS` | (required) | Test duration in seconds |
| `--independent` | `false` | Use independent key testing for register workload |
| `--local` | `false` | Local testing mode (FrogDB already running, no Docker) |
| `--docker` | `false` | Docker testing mode (use docker-compose containers) |
| `--replication` | `false` | Use 3-node replication cluster |
| `--cluster` | `false` | Use Raft cluster mode |
| `--cluster-nodes NUM` | `3` | Number of cluster nodes (1-5) |

### Workloads

`register`, `counter`, `append`, `transaction`, `queue`, `set`, `hash`, `sortedset`, `expiry`, `blocking`, `replication`, `split-brain`, `zombie`, `lag`, `cluster-formation`, `leader-election`, `slot-migration`, `cross-slot`, `key-routing`

## Results

Test results are written to `jepsen/frogdb/store/`. Each run creates a timestamped directory, and `store/latest/` symlinks to the most recent run.

```bash
just jepsen-results  # Open latest results in browser (macOS)
just jepsen-clean    # Delete all stored results
```

Key output files per run:

| File | Contents |
|---|---|
| `results.edn` | Machine-readable test results |
| `history.txt` | Full operation history |
| `jepsen.log` | Test execution log |
| `latency-raw.png` | Latency plot |
| `rate.png` | Throughput plot |

## Architecture

```
Host machine
├── just jepsen-build
│   ├── cargo zigbuild → target/x86_64-unknown-linux-gnu/release/frogdb-server
│   └── docker build  → frogdb:latest image
│
├── jepsen/run.py (orchestration)
│   ├── Manages Docker Compose lifecycle per topology
│   ├── Groups tests by topology, starts/stops containers as needed
│   ├── Streams lein output, detects pass/fail verdict
│   └── Prints summary table with results
│
├── Docker containers (per topology)
│   ├── Single-node: n1                    (jepsen/docker-compose.yml)
│   ├── Replication: n1 (primary), n2, n3  (jepsen/frogdb/docker-compose.replication.yml)
│   └── Raft cluster: n1–n5               (jepsen/frogdb/docker-compose.raft-cluster.yml)
│
└── Jepsen test runner (lein run test)
    ├── Connects to containers via redis protocol (Carmine client)
    ├── Injects faults via docker exec (kill, pause, iptables)
    └── Verifies consistency with Jepsen checkers (Knossos, Elle)
```

Dependencies: Jepsen 0.3.5, Clojure 1.11.1, Carmine 3.3.2 (see `jepsen/frogdb/project.clj`).
