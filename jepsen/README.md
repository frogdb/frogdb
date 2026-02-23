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
just jepsen-build                    # Cross-compile + build Docker image
just jepsen-up                       # Start single-node container
just jepsen-register --time-limit 30 # Run a register linearizability test
just jepsen-results                  # Open results in browser (macOS)
just jepsen-down                     # Tear down containers
```

## Test Suites

### Single-Node Tests

Tests run against a single FrogDB container. Start with `just jepsen-up`.

| Recipe | Workload | Nemesis | What it tests |
|---|---|---|---|
| `just jepsen-register` | register | none | Linearizable reads/writes |
| `just jepsen-counter` | counter | none | Atomic increment/decrement |
| `just jepsen-append` | append | none | List append durability |
| `just jepsen-transaction` | transaction | none | Multi-key atomicity |
| `just jepsen-queue` | queue | none | FIFO ordering |
| `just jepsen-set` | set | none | Set membership consistency |
| `just jepsen-hash` | hash | none | Field-level atomicity |
| `just jepsen-sortedset` | sortedset | none | Score/ranking consistency |
| `just jepsen-expiry` | expiry | none | TTL/expiration correctness |
| `just jepsen-blocking` | blocking | none | BLPOP/BRPOP semantics |

### Single-Node Crash Tests

Same container, with the nemesis killing and restarting FrogDB mid-test.

| Recipe | Workload | Nemesis | What it tests |
|---|---|---|---|
| `just jepsen-crash` | register | kill | Register durability across crashes |
| `just jepsen-counter-crash` | counter | kill | Counter durability across crashes |
| `just jepsen-append-crash` | append | kill | Append durability across crashes |
| `just jepsen-append-rapid` | append | rapid-kill | Append durability under rapid kills |
| `just jepsen-transaction-crash` | transaction | kill | Transaction atomicity across crashes |
| `just jepsen-sortedset-crash` | sortedset | kill | Sorted set durability across crashes |
| `just jepsen-expiry-crash` | expiry | kill | TTL correctness across crashes |
| `just jepsen-expiry-rapid` | expiry | rapid-kill | TTL correctness under rapid kills |
| `just jepsen-blocking-crash` | blocking | kill | Blocking op semantics across crashes |

### Replication Tests (3-Node)

Tests run against a primary + 2 replica cluster. Start with `just jepsen-replication-up`.

| Recipe | Workload | Nemesis | What it tests |
|---|---|---|---|
| `just jepsen-replication` | replication | none | Replication consistency |
| `just jepsen-lag` | lag | none | Replication lag measurement |
| `just jepsen-split-brain` | split-brain | partition | Split-brain detection |
| `just jepsen-zombie` | zombie | partition | Zombie primary detection |
| `just jepsen-replication-chaos` | replication | all-replication | Combined kill + pause + partition faults |

### Raft Cluster Tests (5-Node)

Tests run against a 5-node Raft cluster. Start with `just jepsen-raft-cluster-up`.

| Recipe | Workload | Nemesis | What it tests |
|---|---|---|---|
| `just jepsen-cluster-formation` | cluster-formation | none | Cluster membership |
| `just jepsen-leader-election` | leader-election | none | Raft leader election |
| `just jepsen-slot-migration` | slot-migration | none | Hash slot redistribution |
| `just jepsen-cross-slot` | cross-slot | none | Hash tag transactions |
| `just jepsen-key-routing` | key-routing | none | MOVED/ASK redirects |
| `just jepsen-leader-election-partition` | leader-election | partition | Leader election under partitions |
| `just jepsen-key-routing-kill` | key-routing | kill | Key routing under node kills |
| `just jepsen-slot-migration-partition` | slot-migration | partition | Slot migration under partitions |
| `just jepsen-raft-chaos` | key-routing | raft-cluster | Combined Raft cluster faults |
| `just jepsen-clock-skew` | register | clock-skew | Register correctness under clock skew |
| `just jepsen-disk-failure` | register | disk-failure | Register correctness under disk failures |
| `just jepsen-slow-network` | register | slow-network | Register correctness under slow network |
| `just jepsen-memory-pressure` | register | memory-pressure | Register correctness under memory pressure |

### Running All Tests

```bash
just jepsen-all              # All single-node + crash tests
just jepsen-replication-all  # All 3-node replication tests
just jepsen-raft-all         # All 5-node Raft cluster tests
```

Each `*-all` recipe handles build and container startup automatically.

### Tear Down

```bash
just jepsen-down               # Single-node
just jepsen-replication-down   # 3-node replication
just jepsen-raft-cluster-down  # 5-node Raft cluster
```

## Running Tests Directly

For more control, run `lein run test` from `jepsen/frogdb/`:

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
