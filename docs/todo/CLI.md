# FrogDB CLI — `frog` Operational Tooling Spec

Design specification for the `frog` CLI binary — a pure-client tool for cluster management,
diagnostics, monitoring, backup, replication management, and data utilities.

**Status:** Spec only — implementation not started.

---

## Table of Contents

1. [Overview & Binary Design](#1-overview--binary-design)
2. [`frog config` — Configuration Management](#2-frog-config--configuration-management)
3. [`frog health` — Health Checking](#3-frog-health--health-checking)
4. [`frog cluster` — Cluster Operations](#4-frog-cluster--cluster-operations)
5. [`frog replication` — Replication Management](#5-frog-replication--replication-management)
6. [`frog debug` — Diagnostics & Debugging](#6-frog-debug--diagnostics--debugging)
7. [`frog backup` — Backup & Restore](#7-frog-backup--backup--restore)
8. [`frog stat` — Real-time Monitoring](#8-frog-stat--real-time-monitoring)
9. [`frog data` — Data Utilities](#9-frog-data--data-utilities)
10. [Design Principles](#10-design-principles)
11. [Excluded Scope](#11-excluded-scope)
12. [Server Dependencies & Gaps](#12-server-dependencies--gaps)

---

## 1. Overview & Binary Design

### Binary

Single binary: **`frog`** with nested subcommand groups via [clap](https://docs.rs/clap).

```
frog [GLOBAL OPTIONS] <COMMAND> [SUBCOMMAND] [ARGS...]
```

### Crate Location

`frog-cli/` (placeholder directory already exists). This crate has **no dependency** on server
crates (`frogdb-server`, `frogdb-core`, `frogdb-persistence`) or RocksDB. It is a pure network
client that speaks RESP, HTTP, and reads local files.

### Global Connection Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--host` | `-h` | `127.0.0.1` | Server hostname or IP |
| `--port` | `-p` | `6379` | RESP port |
| `--auth` | `-a` | — | Password (`AUTH <password>`) |
| `--user` | `-u` | — | ACL username (`AUTH <user> <password>`) |
| `--tls` | | `false` | Enable TLS for RESP connections |
| `--tls-cert` | | — | Client certificate path (mTLS) |
| `--tls-key` | | — | Client private key path (mTLS) |
| `--tls-ca` | | — | CA certificate path |
| `--admin-url` | | — | Admin HTTP base URL (e.g. `http://127.0.0.1:6380`) |
| `--metrics-url` | | — | Metrics/observability HTTP base URL (e.g. `http://127.0.0.1:9090`) |
| `--output` | `-o` | `table` | Output format: `table`, `json`, `raw` |
| `--no-color` | | `false` | Disable ANSI colors |

### Communication Channels

The CLI uses three communication channels depending on the subcommand:

| Channel | Protocol | Default Port | Used By |
|---------|----------|--------------|---------|
| **RESP** | Redis Serialization Protocol | 6379 | Most commands (INFO, CONFIG, CLUSTER, DEBUG, etc.) |
| **Admin HTTP** | REST/JSON | 6380 | `frog cluster` (topology), `frog health --admin` |
| **Metrics HTTP** | Prometheus text / JSON | 9090 | `frog health --live/--ready`, `frog stat --prometheus` |

Subcommands that need a RESP connection establish one on demand. File-only operations (e.g.
`frog config generate`) require no server connection.

---

## 2. `frog config` — Configuration Management

Manage FrogDB configuration files and inspect running configuration.

```
frog config <SUBCOMMAND>
```

### Subcommands

#### `frog config generate`

Emit an annotated default TOML configuration to stdout.

```bash
frog config generate              # Standalone server defaults
frog config generate --cluster    # Cluster-mode defaults (admin API enabled, cluster section)
frog config generate > frogdb.toml
```

Replaces `frogdb-server --generate-config` with a tool that doesn't require the server binary.
The `frog` crate embeds the default config template (not generated from server code).

**Connection required:** None (local-only).

#### `frog config validate <FILE>`

Parse a TOML config file, report errors and warnings.

```bash
frog config validate frogdb.toml
# ✓ Configuration valid (42 fields parsed)

frog config validate bad.toml
# ✗ Error at line 15: unknown field 'maxmemory' in [memory], expected 'max_memory'
# ✗ Error at line 23: persistence.durability_mode must be one of: async, periodic, sync
```

Checks:
- TOML syntax
- Known field names (warns on unknown fields)
- Value types and ranges (e.g. port 0–65535, valid durability modes)
- Cross-field consistency (e.g. `persistence.enabled = true` requires `persistence.data_dir`)

**Connection required:** None (local-only).

#### `frog config diff <FILE_A> <FILE_B>`

Side-by-side diff of two config files, highlighting differences with semantic awareness.

```bash
frog config diff prod.toml staging.toml
# [server]
#   port: 6379 → 6380
#   num_shards: 8 → 4
# [memory]
#   max_memory: 8GB → 4GB
# [persistence]
#   (identical)
```

**Connection required:** None (local-only).

#### `frog config show`

Fetch the running configuration via `CONFIG GET *` and display it as TOML.

```bash
frog config show
frog config show --section server    # Filter to [server] section
```

**Connection required:** RESP.

#### `frog config show --diff <FILE>`

Compare running config against a file. Highlights drift between deployed and on-disk config.

```bash
frog config show --diff frogdb.toml
# Differences (running vs file):
#   slowlog_log_slower_than: 5000 (running) vs 10000 (file)
#   maxmemory_policy: allkeys-lru (running) vs noeviction (file)
```

**Connection required:** RESP.

---

## 3. `frog health` — Health Checking

Quick health probes for monitoring and scripting.

```
frog health [OPTIONS]
```

### Default Mode

Sends `PING` and `INFO server` via RESP. Reports version, uptime, role, memory.

```bash
frog health
# frogdb 0.1.0 @ 127.0.0.1:6379
# Role: primary | Uptime: 3d 12h | Memory: 2.1GB/8GB (26%)
# Status: HEALTHY

echo $?  # 0
```

### Options

| Flag | Description |
|------|-------------|
| `--admin` | Check admin HTTP health endpoint (`GET /admin/health`) |
| `--live` | Check liveness probe (`GET /health/live` on metrics port) |
| `--ready` | Check readiness probe (`GET /health/ready` on metrics port) |
| `--all <addr1> <addr2> ...` | Fan-out health check across multiple nodes |
| `--json` | Force JSON output (overrides `--output`) |

### Fan-out Mode

```bash
frog health --all 10.0.0.1:6379 10.0.0.2:6379 10.0.0.3:6379
# NODE              STATUS     ROLE      UPTIME    MEMORY
# 10.0.0.1:6379     HEALTHY    primary   3d 12h    2.1GB/8GB
# 10.0.0.2:6379     HEALTHY    replica   3d 12h    2.0GB/8GB
# 10.0.0.3:6379     UNHEALTHY  primary   0d 0h     0B/8GB
```

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | All nodes healthy |
| `1` | One or more nodes unhealthy |
| `2` | One or more nodes unreachable (connection refused, timeout) |

**Connection required:** RESP (default), Admin HTTP (`--admin`), Metrics HTTP (`--live`/`--ready`).

---

## 4. `frog cluster` — Cluster Operations

Manage FrogDB clusters. These commands are Raft-aware: they wait for leader election,
check quorum before proceeding, and report Raft state alongside Redis-compatible cluster info.

FrogDB uses an **orchestrated control plane** (not gossip). The `frog cluster` commands interact
with nodes directly via RESP `CLUSTER` commands and the Admin HTTP API. They do NOT replace the
orchestrator — they are operator tools for bootstrapping, inspecting, and repairing clusters.

```
frog cluster <SUBCOMMAND>
```

### Subcommands

#### `frog cluster create <addr1> <addr2> ... [OPTIONS]`

Bootstrap a new cluster from standalone nodes.

```bash
frog cluster create 10.0.0.1:6379 10.0.0.2:6379 10.0.0.3:6379 --replicas 1
```

**Procedure:**
1. Connect to all nodes, verify they are empty and not already in a cluster
2. Issue `CLUSTER MEET` to introduce all nodes to each other
3. Wait for Raft leader election (poll `CLUSTER INFO` for `cluster_state:ok`)
4. Compute slot assignment: distribute 16384 slots evenly across primaries
5. Issue `CLUSTER ADDSLOTS` to each primary
6. Assign replicas via `CLUSTER REPLICATE` (round-robin by node, avoiding same-host pairing)
7. Verify: all 16384 slots covered, all replicas connected

| Option | Default | Description |
|--------|---------|-------------|
| `--replicas <N>` | `0` | Number of replicas per primary |
| `--yes` | `false` | Skip confirmation prompt |

**Connection required:** RESP to all nodes, Admin HTTP (optional, for topology push).

#### `frog cluster info`

Display cluster summary: nodes, slots, Raft state, health.

```bash
frog cluster info
# Cluster ID: frogdb-prod-1
# State: ok
# Epoch: 42
# Raft Leader: 10.0.0.1:6379 (node-abc123)
# Known Nodes: 6 (3 primaries, 3 replicas)
# Slots Assigned: 16384/16384
# Slots OK: 16384
```

**Connection required:** RESP (CLUSTER INFO, CLUSTER NODES).

#### `frog cluster check`

Validate cluster health invariants.

```bash
frog cluster check
# ✓ All 16384 slots assigned
# ✓ All slots served by reachable nodes
# ✓ No PFAIL/FAIL nodes
# ✓ Raft quorum OK (3/3 voters)
# ✓ All primaries have ≥1 replica
# ✓ No slots in MIGRATING/IMPORTING state
```

Checks performed:
- All 16384 slots covered
- No PFAIL or FAIL flagged nodes
- Raft quorum satisfied
- Every primary has at least one reachable replica
- No stale MIGRATING/IMPORTING state
- Consistent epoch across all nodes
- No split-brain indicators (split-brain log entries)

Exit codes: `0` = all checks pass, `1` = one or more checks fail.

**Connection required:** RESP to all cluster nodes.

#### `frog cluster fix`

Auto-repair common cluster issues.

```bash
frog cluster fix
# Found 3 issues:
#   1. Slots 5461-5470 unassigned → assigning to 10.0.0.2:6379
#   2. Slot 8000 in MIGRATING state with no IMPORTING target → clearing
#   3. Node 10.0.0.4:6379 has FAIL flag but is reachable → clearing
# Proceed? [y/N]
```

Repairs:
- Reassign orphan (unassigned) slots to available primaries
- Clear stale MIGRATING/IMPORTING flags where the counterpart is absent
- Clear FAIL flags on nodes that are now reachable
- Remove nodes that are permanently unreachable from the topology

**Connection required:** RESP to all cluster nodes.

#### `frog cluster add-node <addr> [OPTIONS]`

Add a new node to the cluster.

```bash
frog cluster add-node 10.0.0.4:6379                              # Add as primary
frog cluster add-node 10.0.0.5:6379 --replica-of 10.0.0.1:6379   # Add as replica
```

| Option | Description |
|--------|-------------|
| `--replica-of <addr>` | Join as replica of specified primary |

**Connection required:** RESP.

#### `frog cluster del-node <addr>`

Remove a node from the cluster. If the node is a primary, its slots must be migrated first
(or `--force` to drop slots).

```bash
frog cluster del-node 10.0.0.4:6379
frog cluster del-node 10.0.0.1:6379 --force   # Force-remove primary (slots become unassigned!)
```

**Connection required:** RESP.

#### `frog cluster reshard [OPTIONS]`

Migrate slots between nodes.

```bash
frog cluster reshard --from 10.0.0.1:6379 --to 10.0.0.2:6379 --slots 1000
```

| Option | Description |
|--------|-------------|
| `--from <addr>` | Source node |
| `--to <addr>` | Target node |
| `--slots <N>` | Number of slots to migrate |
| `--slot-range <start>-<end>` | Specific slot range to migrate |
| `--timeout <ms>` | Per-slot migration timeout (default: 60000) |

Displays a progress bar during migration.

**Connection required:** RESP to source and target nodes.

#### `frog cluster rebalance`

Redistribute slots proportionally across primaries. Considers key count and memory per node
to achieve balanced load. Optionally weight nodes.

```bash
frog cluster rebalance
frog cluster rebalance --weight 10.0.0.1:6379=2 --weight 10.0.0.2:6379=1
frog cluster rebalance --use-hot-shards   # Factor in hot shard data for rebalancing
```

| Option | Description |
|--------|-------------|
| `--weight <addr>=<N>` | Relative weight for node (higher = more slots) |
| `--threshold <pct>` | Rebalance only if imbalance exceeds threshold (default: 2%) |
| `--use-hot-shards` | Use `DEBUG HOTSHARDS` data to inform slot placement |
| `--dry-run` | Show planned migrations without executing |
| `--pipeline <N>` | Concurrent slot migrations (default: 1) |

**Connection required:** RESP to all cluster nodes.

#### `frog cluster failover [OPTIONS]`

Trigger manual failover on the connected node (must be a replica).

```bash
frog cluster failover                     # Coordinated failover (waits for sync)
frog cluster failover --force             # Force failover (skip sync check)
frog cluster failover --takeover          # Takeover without primary agreement
```

The Raft-aware implementation:
- Coordinated: Waits for the replica to catch up, then initiates Raft leadership transfer
- Force: Promotes replica even if not fully caught up
- Takeover: Unilateral promotion without primary cooperation (for disaster recovery)

See: CLUSTER.md (Failover),
REPLICATION.md (Replica Promotion).

**Connection required:** RESP to the replica node.

#### `frog cluster topology`

ASCII tree visualization of the cluster topology.

```bash
frog cluster topology
# Cluster: frogdb-prod-1 (epoch 42)
#
# 10.0.0.1:6379 [primary] slots: 0-5460 (5461 slots)
#   └── 10.0.0.4:6379 [replica] lag: 0 bytes
#
# 10.0.0.2:6379 [primary] slots: 5461-10922 (5462 slots)
#   └── 10.0.0.5:6379 [replica] lag: 128 bytes
#
# 10.0.0.3:6379 [primary] slots: 10923-16383 (5461 slots)
#   └── 10.0.0.6:6379 [replica] lag: 0 bytes
```

**Connection required:** RESP (CLUSTER NODES, CLUSTER SLOTS).

#### `frog cluster slots`

Dump the slot-to-node mapping.

```bash
frog cluster slots
# SLOT RANGE      NODE              ROLE      STATE
# 0-5460          10.0.0.1:6379     primary   ok
# 0-5460          10.0.0.4:6379     replica   ok
# 5461-10922      10.0.0.2:6379     primary   ok
# 5461-10922      10.0.0.5:6379     replica   ok
# 10923-16383     10.0.0.3:6379     primary   ok
# 10923-16383     10.0.0.6:6379     replica   ok

frog cluster slots --json   # Machine-readable output
```

**Connection required:** RESP (CLUSTER SLOTS).

---

## 5. `frog replication` — Replication Management

Inspect and manage primary-replica replication.

```
frog replication <SUBCOMMAND>
```

### Subcommands

#### `frog replication status`

Display replication status for the connected node.

```bash
frog replication status
# Role: primary
# Replication ID: abc123def456...
# Replication Offset: 1234567
# Connected Replicas: 2
# Min Replicas to Write: 1
# Self-Fencing: active (quorum OK)
#
# REPLICA              STATE       OFFSET      LAG (bytes)  LAG (sec)
# 10.0.0.4:6379        online      1234500     67           0
# 10.0.0.5:6379        online      1234200     367          1
```

For replica nodes:
```bash
frog replication status -h 10.0.0.4 -p 6379
# Role: replica
# Primary: 10.0.0.1:6379
# Replication ID: abc123def456...
# Replication Offset: 1234500
# Primary Offset: 1234567
# Lag: 67 bytes
# State: online (streaming)
# Read-Only: yes
```

Surfaces FrogDB-specific state:
- **Self-fencing status**: Whether the primary is self-fenced (rejecting writes due to quorum loss)
- **Split-brain log**: Recent entries from the split-brain log if any exist

**Connection required:** RESP (INFO replication).

#### `frog replication lag [OPTIONS]`

Per-replica replication lag monitoring.

```bash
frog replication lag
# REPLICA              LAG (bytes)  LAG (sec)  LAST ACK
# 10.0.0.4:6379        67           0          2024-01-15T10:30:45Z
# 10.0.0.5:6379        367          1          2024-01-15T10:30:44Z

frog replication lag --watch
# (refreshes every 1s, like `top`)
```

| Option | Description |
|--------|-------------|
| `--watch` | Continuously refresh |
| `--interval <ms>` | Refresh interval (default: 1000) |
| `--threshold <bytes>` | Highlight replicas lagging more than threshold |

**Connection required:** RESP (INFO replication).

#### `frog replication promote <replica-addr>`

Promote a replica to primary (standalone mode, not cluster failover).

```bash
frog replication promote 10.0.0.4:6379
# Sending REPLICAOF NO ONE to 10.0.0.4:6379...
# OK — 10.0.0.4:6379 is now a primary
```

For cluster failover, use `frog cluster failover` instead.

**Connection required:** RESP to the replica.

#### `frog replication topology`

ASCII tree of primary→replica relationships.

```bash
frog replication topology
# 10.0.0.1:6379 [primary] offset: 1234567
#   ├── 10.0.0.4:6379 [replica] offset: 1234500 (lag: 67 bytes)
#   └── 10.0.0.5:6379 [replica] offset: 1234200 (lag: 367 bytes)
```

In cluster mode, shows all primary→replica trees.

**Connection required:** RESP (INFO replication, CLUSTER NODES in cluster mode).

---

## 6. `frog debug` — Diagnostics & Debugging

Diagnostic tools for investigating performance issues and collecting support bundles.

```
frog debug <SUBCOMMAND>
```

### Subcommands

#### `frog debug zip [OPTIONS]`

Collect a diagnostic bundle into a ZIP archive. Gathers data from all three channels (RESP,
Admin HTTP, Metrics HTTP) and packages it for support analysis.

```bash
frog debug zip
# Collecting diagnostics from 127.0.0.1:6379...
#   INFO all              ✓
#   CONFIG GET *          ✓
#   CLIENT LIST           ✓
#   SLOWLOG GET 128       ✓
#   LATENCY LATEST        ✓
#   MEMORY STATS          ✓
#   CLUSTER INFO          ✓ (cluster mode)
#   CLUSTER NODES         ✓ (cluster mode)
#   DEBUG HOTSHARDS       ✓
#   DEBUG BUNDLE LIST     ✓
#   DEBUG BUNDLE GENERATE ✓
#   STATUS JSON           ✓
#   ACL LIST              ✓
#   /admin/health         ✓ (admin API)
#   /admin/cluster        ✓ (admin API)
#   /admin/role           ✓ (admin API)
#   /admin/nodes          ✓ (admin API)
#   /health/live          ✓ (metrics HTTP)
#   /health/ready         ✓ (metrics HTTP)
#   /metrics              ✓ (metrics HTTP)
#   /status/json          ✓ (metrics HTTP)
# Written: frogdb-debug-20240115-103045.zip

frog debug zip --nodes 10.0.0.1:6379 10.0.0.2:6379 10.0.0.3:6379
# Collects from all specified nodes into a single archive
```

| Option | Description |
|--------|-------------|
| `--nodes <addr1> <addr2> ...` | Collect from multiple nodes |
| `--output <path>` | Output file path (default: `frogdb-debug-<timestamp>.zip`) |
| `--redact` | Redact passwords and tokens from output |

Each command failure is recorded as an error in the bundle (non-fatal — partial bundles are useful).

**Relationship to `DEBUG BUNDLE GENERATE`:** The server has a built-in `DEBUG BUNDLE GENERATE`
command that creates a server-side diagnostic bundle (slowlog, traces, system info) stored in the
configured `debug_bundle.directory`. `frog debug zip` is complementary — it's a **client-side**
collection tool that gathers data from all three channels (RESP, Admin HTTP, Metrics HTTP) and
also invokes `DEBUG BUNDLE GENERATE` on the server. The server-side bundle captures internal state
that RESP commands cannot (e.g. recent tracing spans), while the client-side bundle adds HTTP
endpoint snapshots and multi-node aggregation.

**Connection required:** RESP, Admin HTTP, Metrics HTTP (all optional — collects what's available).

#### `frog debug latency [OPTIONS]`

Continuous PING round-trip latency measurement.

```bash
frog debug latency
# --- 127.0.0.1:6379 PING latency ---
# min: 0.08ms  avg: 0.12ms  max: 0.45ms  p99: 0.31ms  samples: 1000

frog debug latency --history
# Periodic snapshots (every 15s):
# TIME                  MIN     AVG     MAX     P99     SAMPLES
# 10:30:00              0.08    0.12    0.45    0.31    1000
# 10:30:15              0.09    0.13    0.52    0.35    1000

frog debug latency --dist
# ASCII histogram of latency distribution:
# 0.00-0.05ms  ████████████████████ 200
# 0.05-0.10ms  ████████████████████████████████████████ 400
# 0.10-0.20ms  ████████████████████████████ 280
# 0.20-0.50ms  ██████████ 100
# 0.50-1.00ms  ██ 20
```

| Option | Description |
|--------|-------------|
| `--samples <N>` | Number of PINGs per measurement (default: 1000) |
| `--interval <ms>` | Delay between PINGs (default: 0 — as fast as possible) |
| `--history` | Periodic snapshot mode (every 15s) |
| `--dist` | Show ASCII latency distribution histogram |

**Connection required:** RESP.

#### `frog debug latency doctor`

Fetch server-side latency analysis via `LATENCY DOCTOR`.

```bash
frog debug latency doctor
# (Displays LATENCY DOCTOR output with formatting)
```

**Connection required:** RESP.

#### `frog debug latency graph`

Fetch server-side latency history and render an ASCII graph.

```bash
frog debug latency graph command
# command latency (last 160 events):
#   250 |          *
#   200 |    *           *
#   150 | *     *  * *      *
#   100 |*  * *  **   * * *  **
#    50 |
#     0 +---+---+---+---+---+---
#       10:00  10:05  10:10  10:15
```

Uses `LATENCY HISTORY <event>` data.

**Connection required:** RESP.

#### `frog debug latency histogram [COMMAND...]`

Fetch per-command latency histograms via `LATENCY HISTOGRAM`.

```bash
frog debug latency histogram
# (Shows histograms for all commands with latency data)

frog debug latency histogram GET SET
# (Shows histograms for GET and SET only)
```

**Connection required:** RESP.

#### `frog debug memory stats`

Formatted display of `MEMORY STATS`.

```bash
frog debug memory stats
# Peak Allocated:    8.00 GB
# Total Allocated:   4.00 GB
# Startup:           512 MB
# Dataset:           3.48 GB (87%)
# Replication:       0 B
# Clients:           32 KB
# Keys:              1,000,000
# Bytes per Key:     3.65 KB
```

**Connection required:** RESP.

#### `frog debug memory doctor`

Formatted display of `MEMORY DOCTOR` with FrogDB-specific shard analysis.

```bash
frog debug memory doctor
# (Displays MEMORY DOCTOR output with color-coded severity)
```

**Connection required:** RESP.

#### `frog debug memory bigkeys [OPTIONS]`

Scan the keyspace for the largest keys, grouped by type.

```bash
frog debug memory bigkeys
# Scanning keyspace...
#
# Biggest string:     user:avatar:12345    (2.1 MB)
# Biggest hash:       session:abc123       (1.5 MB, 10000 fields)
# Biggest sorted set: leaderboard:global   (800 KB, 50000 members)
# Biggest list:       queue:tasks          (500 KB, 10000 items)
# Biggest set:        tags:popular         (200 KB, 5000 members)
```

Uses `SCAN` + `TYPE` + `DEBUG OBJECT` (or `MEMORY USAGE` when available).

| Option | Description |
|--------|-------------|
| `--type <type>` | Filter by data type (string, hash, list, set, zset) |
| `--top <N>` | Show top N keys per type (default: 1) |
| `--samples <N>` | SCAN sample count (default: 0 = full scan) |

**Connection required:** RESP.

#### `frog debug memory memkeys`

Scan every key with `MEMORY USAGE` and report a summary.

```bash
frog debug memory memkeys
# Scanned 1,000,000 keys
# Total memory: 4.00 GB
#
# Top 10 keys by memory:
#   user:avatar:12345       2.1 MB   string
#   session:abc123          1.5 MB   hash
#   ...
```

⚠️ **Warning:** This performs `MEMORY USAGE` on every key. Slow on large datasets.

**Connection required:** RESP.

#### `frog debug hotshards [OPTIONS]`

FrogDB-specific hot shard analysis via `DEBUG HOTSHARDS`.

```bash
frog debug hotshards
# Hot Shard Report (period: 10s)
# Total: 50,000 ops/sec across 8 shards
# Imbalance ratio: 2.4x (max/avg)
#
# SHARD  OPS/SEC  READS/SEC  WRITES/SEC  PCT    QUEUE  STATUS
# 3      15,000   12,000     3,000       30.0%  12     HOT
# 7      10,000   8,000      2,000       20.0%  5      WARM
# 0      6,500    5,000      1,500       13.0%  2      OK
# ...

frog debug hotshards --watch
# (refreshes continuously)

frog debug hotshards --all 10.0.0.1:6379 10.0.0.2:6379 10.0.0.3:6379
# Collects from all nodes and shows aggregate view
```

| Option | Description |
|--------|-------------|
| `--watch` | Continuously refresh |
| `--interval <ms>` | Refresh interval (default: 2000) |
| `--period <secs>` | Server-side stats collection period (default: 10) |
| `--all <addrs>` | Fan-out across multiple nodes |

**Connection required:** RESP (DEBUG HOTSHARDS).

#### `frog debug slowlog [OPTIONS]`

Inspect and analyze the slow query log.

```bash
frog debug slowlog
# ID  TIMESTAMP            DURATION   COMMAND
# 1   2024-01-15 10:30:45  15.0ms     KEYS *
# 0   2024-01-15 10:30:40  12.0ms     SMEMBERS large_set

frog debug slowlog --analyze
# Slow Query Analysis (128 entries):
#
# Top commands by frequency:
#   KEYS:      45 entries (avg: 18ms, max: 250ms)
#   SMEMBERS:  23 entries (avg: 12ms, max: 35ms)
#   SORT:      10 entries (avg: 8ms, max: 15ms)
#
# Top commands by total time:
#   KEYS:      810ms total
#   SMEMBERS:  276ms total
```

| Option | Description |
|--------|-------------|
| `--count <N>` | Number of entries to fetch (default: all) |
| `--analyze` | Aggregate analysis by command |
| `--all <addrs>` | Collect from multiple nodes |
| `--reset` | Clear the slow log after reading |

**Connection required:** RESP (SLOWLOG GET, SLOWLOG LEN, SLOWLOG RESET).

#### `frog debug vll`

FrogDB-specific VLL (Very Lightweight Locking) queue inspection.

```bash
frog debug vll
# VLL Queue Status
#
# SHARD  QUEUE DEPTH  EXECUTING TXID  CONTENTION
# 0      5            1000            low
# 1      0            —               none
# 2      12           1042            high
# ...

frog debug vll --shard 2
# Shard 2 VLL Queue (depth: 12, executing txid: 1042):
#   txid:1043  operation:SET     keys:1  queued_at:10:30:45.123
#   txid:1044  operation:MSET    keys:3  queued_at:10:30:45.456
#   ...
```

Uses `DEBUG DUMP-VLL-QUEUE`.

| Option | Description |
|--------|-------------|
| `--shard <N>` | Show detailed queue for specific shard |
| `--watch` | Continuously refresh |

**Connection required:** RESP (DEBUG DUMP-VLL-QUEUE).

#### `frog debug connections`

Formatted display of all client connections.

```bash
frog debug connections
# ID  ADDR                 NAME      AGE    IDLE   FLAGS  CMD     QBUF    OMEM
# 1   192.168.1.10:54321   worker-1  1h     0s     N      GET     0       0
# 2   192.168.1.11:54322   —         30m    5m     b      BLPOP   26      1MB
```

Uses `DEBUG DUMP-CONNECTIONS` (FrogDB-specific, includes thread/shard info) with fallback
to `CLIENT LIST` (Redis-compatible).

| Option | Description |
|--------|-------------|
| `--sort <field>` | Sort by: idle, omem, age (default: id) |
| `--filter <expr>` | Filter: `idle>300`, `flags=b`, `omem>1MB` |

**Connection required:** RESP.

---

## 7. `frog backup` — Backup & Restore

Manage snapshots and portable data export/import.

```
frog backup <SUBCOMMAND>
```

### Subcommands

#### `frog backup trigger`

Trigger a background save (snapshot).

```bash
frog backup trigger
# BGSAVE initiated
# Background save started at 2024-01-15 10:30:45
```

Sends `BGSAVE` via RESP.

**Connection required:** RESP.

#### `frog backup status`

Check snapshot/persistence status.

```bash
frog backup status
# Last Save:     2024-01-15 10:00:00 (30 min ago)
# BG Save:       not running
# Last Status:   ok
# WAL Enabled:   yes
# WAL Status:    ok
# Data Dir:      /var/lib/frogdb/data
# Snapshot Dir:  /var/lib/frogdb/snapshots
```

Uses `INFO persistence` and `LASTSAVE`.

**Connection required:** RESP.

#### `frog backup export --output <dir>`

Export the entire dataset to a portable format using `SCAN` + `DUMP`.

```bash
frog backup export --output ./backup-20240115
# Scanning keyspace...
# Exported 1,000,000 keys (4.2 GB) to ./backup-20240115/
# Manifest: ./backup-20240115/manifest.json
# Checksum: sha256:abc123...
```

Export format:
- `manifest.json` — metadata (key count, timestamp, source server info, checksum)
- `data/` — serialized key-value pairs in batches (DUMP format)

| Option | Description |
|--------|-------------|
| `--output <dir>` | Output directory (required) |
| `--match <pattern>` | SCAN pattern filter (e.g. `user:*`) |
| `--count <N>` | SCAN batch size (default: 1000) |
| `--type <type>` | Filter by data type |

**Connection required:** RESP (SCAN, DUMP, TTL).

#### `frog backup import --input <dir>`

Import a previously exported dataset using `RESTORE` pipelining.

```bash
frog backup import --input ./backup-20240115
# Verifying manifest...
# Importing 1,000,000 keys...
# Progress: ████████████████████ 100% (1,000,000/1,000,000)
# Import complete: 1,000,000 keys restored
```

| Option | Description |
|--------|-------------|
| `--input <dir>` | Input directory (required) |
| `--replace` | Overwrite existing keys (default: skip) |
| `--pipeline <N>` | RESTORE pipeline depth (default: 64) |
| `--ttl` | Preserve original TTLs (default: yes) |

**Connection required:** RESP (RESTORE).

#### `frog backup verify <dir>`

Verify integrity of an export archive.

```bash
frog backup verify ./backup-20240115
# Manifest: ✓ valid
# Files: ✓ 42 data files present
# Checksum: ✓ sha256 matches
# Keys: 1,000,000
# Total Size: 4.2 GB
```

**Connection required:** None (local-only).

---

## 8. `frog stat` — Real-time Monitoring

Continuously refreshing stats dashboard, similar to `redis-cli --stat`.

```bash
frog stat
# --- 127.0.0.1:6379 (every 1s) ---
# KEYS       MEMORY     CLIENTS  OPS/SEC   HIT RATE  NET IN    NET OUT
# 1,000,000  4.00 GB    10       15,234    80.0%     12 MB/s   8 MB/s
# 1,000,012  4.00 GB    10       15,180    79.8%     12 MB/s   8 MB/s
# ...
```

Data sources: `INFO stats`, `INFO memory`, `INFO clients`, `INFO keyspace`.

| Option | Description |
|--------|-------------|
| `--interval <ms>` | Poll interval (default: 1000) |
| `--count <N>` | Number of refreshes before exit (default: unlimited) |
| `--no-header` | Suppress header (for piping) |

TTY-aware: uses in-place refresh when connected to a terminal, line-by-line when piped.

**Connection required:** RESP.

---

## 9. `frog data` — Data Utilities

Key-level data inspection and manipulation utilities.

```
frog data <SUBCOMMAND>
```

### Subcommands

#### `frog data bigkeys` / `frog data memkeys`

Convenience aliases for `frog debug memory bigkeys` and `frog debug memory memkeys`.

#### `frog data keyspace`

Keyspace summary: key count by type, memory distribution, expiry statistics.

```bash
frog data keyspace
# Keyspace Summary (1,000,000 keys)
#
# TYPE         COUNT      PCT     MEMORY     AVG SIZE
# string       600,000    60.0%   2.40 GB    4.2 KB
# hash         200,000    20.0%   800 MB     4.1 KB
# sorted set   100,000    10.0%   400 MB     4.1 KB
# list         50,000     5.0%    200 MB     4.1 KB
# set          50,000     5.0%    200 MB     4.1 KB
#
# Expiry:
#   With TTL:     300,000 (30%)
#   No TTL:       700,000 (70%)
#   Avg TTL:      3600s
#   Expiring/sec: 83
```

Uses `SCAN` + `TYPE` + `MEMORY USAGE` (sampled) + `TTL`.

| Option | Description |
|--------|-------------|
| `--samples <N>` | Sample size for memory estimation (default: 10000) |

**Connection required:** RESP.

#### `frog data export` / `frog data import`

Convenience aliases for `frog backup export` and `frog backup import`.

#### `frog data pipe`

Pipe raw RESP commands from stdin (like `redis-cli --pipe`).

```bash
echo "SET key1 value1\r\nSET key2 value2\r\n" | frog data pipe
# Sent: 2 commands
# Received: 2 replies (2 OK, 0 errors)
# Throughput: 50,000 commands/sec

cat commands.txt | frog data pipe
```

Uses mass-insert protocol: sends commands without waiting for individual replies, then
drains all responses.

| Option | Description |
|--------|-------------|
| `--batch <N>` | Pipeline batch size (default: 1000) |

**Connection required:** RESP.

#### `frog data slot <KEY>`

Show which hash slot and node a key maps to.

```bash
frog data slot user:123
# Key:            user:123
# Hash Slot:      5649
# Cluster Node:   10.0.0.2:6379 (in cluster mode)

frog data slot "{user:1}:profile"
# Key:            {user:1}:profile
# Hash Tag:       user:1
# Hash Slot:      7998
# Cluster Node:   10.0.0.2:6379

frog data slot --internal user:123
# Key:            user:123
# Hash Slot:      5649
# Internal Shard: 3 (of 8)
# Hash:           0x7f1234567890abcd
# Cluster Node:   10.0.0.2:6379
```

The `--internal` flag uses `DEBUG HASHING` to show the FrogDB internal shard assignment
(requires server connection). Without `--internal`, slot calculation is done locally (no
server needed for standalone slot lookup, but cluster node lookup requires RESP).

| Option | Description |
|--------|-------------|
| `--internal` | Show internal shard via `DEBUG HASHING` |

**Connection required:** None for basic slot calc; RESP for `--internal` and cluster node lookup.

---

## 10. Design Principles

### FrogDB-Specific Capabilities

The `frog` CLI goes beyond a Redis-cli clone by surfacing FrogDB-specific features:

| Capability | Commands | Server Feature |
|------------|----------|----------------|
| Hot shard analysis | `frog debug hotshards` | `DEBUG HOTSHARDS` |
| VLL inspection | `frog debug vll` | `DEBUG DUMP-VLL-QUEUE` |
| Raft-aware cluster ops | `frog cluster create/check/fix` | Raft consensus, `CLUSTER INFO` |
| Split-brain visibility | `frog replication status` | Split-brain log |
| Self-fencing status | `frog replication status` | Replication quorum checker |
| Two-level slot routing | `frog data slot --internal` | `DEBUG HASHING` |
| Scatter-gather latency | `frog debug latency graph scatter-gather` | `LATENCY HISTORY scatter-gather` |
| Diagnostic bundle | `frog debug zip` | Admin HTTP + Metrics HTTP + RESP |

### Output Modes

All commands support `--output` (or `-o`):

| Mode | Description | Use Case |
|------|-------------|----------|
| `table` | Formatted ASCII table (default) | Human operators |
| `json` | JSON objects | Scripting, jq, automation |
| `raw` | Unformatted server response | Debugging, piping |

Streaming commands (`--watch`, `frog stat`) use TTY-aware output:
- **Terminal:** In-place refresh (cursor movement, clear line)
- **Pipe/redirect:** Newline-delimited output (one line per sample)

### Error Handling

- **Single-node commands:** Clear error messages with connection details and retry guidance
- **Fan-out commands** (`--all`, cluster operations): Per-node success/failure reporting

```bash
frog health --all 10.0.0.1:6379 10.0.0.2:6379 10.0.0.3:6379
# NODE              STATUS        ERROR
# 10.0.0.1:6379     HEALTHY       —
# 10.0.0.2:6379     UNHEALTHY     connection refused
# 10.0.0.3:6379     HEALTHY       —
# Exit code: 1
```

- **Auth errors:** Clear guidance on `--auth`/`--user` flags and ACL configuration
- **TLS errors:** Suggest `--tls` flag and certificate paths

### Extensibility

Future subcommand groups (not in this spec):

| Command | Purpose | Status |
|---------|---------|--------|
| `frog upgrade` | Rolling upgrade orchestration | Spec exists: [ROLLING_UPGRADE.md](ROLLING_UPGRADE.md) |
| `frog cert` | TLS certificate management | Spec exists: TLS.md, [TLS_PLAN.md](TLS_PLAN.md) |
| `frog tiered` | Two-tier storage inspection | Spec exists: TIERED.md |
| `frog shell` | Interactive REPL / query client | Separate spec TBD |
| `frog acl` | ACL management | Deferred (ACL RESP commands sufficient) |

---

## 11. Excluded Scope

The following are explicitly excluded from this spec:

| Feature | Reason |
|---------|--------|
| Interactive REPL / query client | Separate spec — different UX concerns (completion, history, multi-line) |
| Benchmarking (`frog bench`) | Existing load-test infrastructure (`frogdb-load-test`) is sufficient |
| ACL tooling (`frog acl`) | Standard ACL RESP commands (`ACL LIST`, `ACL SETUSER`, etc.) are sufficient |

---

## 12. Server Dependencies & Gaps

Features the CLI depends on that may not yet be fully implemented server-side.

### Implemented (Available Now)

| CLI Feature | Server Dependency | Status |
|-------------|-------------------|--------|
| `frog health` (default) | `PING`, `INFO` | ✅ Implemented |
| `frog health --live/--ready` | `/health/live`, `/health/ready` | ✅ Implemented |
| `frog health --admin` | `/admin/health` | ✅ Implemented |
| `frog config show` | `CONFIG GET` | ✅ Implemented |
| `frog debug latency` (client-side) | `PING` | ✅ Implemented |
| `frog debug latency doctor/graph` | `LATENCY DOCTOR`, `LATENCY HISTORY` | ✅ Implemented |
| `frog debug memory stats/doctor` | `MEMORY STATS`, `MEMORY DOCTOR` | ✅ Implemented |
| `frog debug hotshards` | `DEBUG HOTSHARDS` | ✅ Implemented |
| `frog debug vll` | `DEBUG DUMP-VLL-QUEUE` | ✅ Implemented |
| `frog debug connections` | `DEBUG DUMP-CONNECTIONS`, `CLIENT LIST` | ✅ Implemented |
| `frog debug slowlog` | `SLOWLOG GET/LEN/RESET` | ✅ Implemented |
| `frog stat` | `INFO stats/memory/clients/keyspace` | ✅ Implemented |
| `frog data slot` | CRC16 (local) + `DEBUG HASHING` | ✅ Implemented |
| `frog data pipe` | Raw RESP | ✅ Implemented |
| `frog backup trigger/status` | `BGSAVE`, `LASTSAVE`, `INFO persistence` | ✅ Implemented |
| `frog backup export/import` | `SCAN`, `DUMP`, `RESTORE`, `TTL` | ✅ Implemented |
| `frog cluster info/topology/slots` | `CLUSTER INFO`, `CLUSTER NODES`, `CLUSTER SLOTS` | ✅ Implemented |
| `frog cluster failover` | `CLUSTER FAILOVER` | ✅ Implemented |
| `frog replication status/lag` | `INFO replication` | ✅ Implemented |
| `frog replication promote` | `REPLICAOF NO ONE` | ✅ Implemented |
| Admin HTTP endpoints | `/admin/health`, `/admin/cluster`, `/admin/role`, `/admin/nodes` | ✅ Implemented |
| Metrics HTTP endpoints | `/metrics`, `/health/live`, `/health/ready`, `/status/json` | ✅ Implemented |

### Not Yet Implemented (Server-Side Gaps)

| CLI Feature | Server Dependency | Gap | Reference |
|-------------|-------------------|-----|-----------|
| `frog cluster create` | `CLUSTER MEET`, `CLUSTER ADDSLOTS`, `CLUSTER REPLICATE` | Raft-based bootstrap sequence needs validation | CLUSTER.md |
| `frog cluster reshard` | Slot migration protocol (`MIGRATE`, `IMPORTING`/`MIGRATING` state) | Atomic slot migration not yet implemented | [INDEX.md](INDEX.md) |
| `frog cluster rebalance` | Slot migration + hot shard data | Depends on slot migration | [CLUSTER_REBALANCING.md](CLUSTER_REBALANCING.md) |
| `frog cluster fix` | Various cluster repair commands | Depends on slot migration for reassignment | CLUSTER.md |
| TLS flags (`--tls`, `--tls-cert`, etc.) | Server TLS support | TLS not yet implemented | [INDEX.md](INDEX.md), [TLS_PLAN.md](TLS_PLAN.md) |
| `frog config generate` | Embedded config template | CLI needs to embed/duplicate server defaults | — |

### Cross-References

- Cluster architecture: CLUSTER.md
- Replication protocol: REPLICATION.md
- Observability stack: OBSERVABILITY.md
- Debug commands: DEBUGGING.md
- Configuration system: CONFIGURATION.md
- VLL transaction coordination: VLL.md
- Failure modes: FAILURE_MODES.md
- Deployment guide: DEPLOYMENT.md
- Slot migration: [CLUSTER_REBALANCING.md](CLUSTER_REBALANCING.md)
- TLS: TLS.md, [TLS_PLAN.md](TLS_PLAN.md)
- Rolling upgrade: [ROLLING_UPGRADE.md](ROLLING_UPGRADE.md)
- Tiered storage: TIERED.md
