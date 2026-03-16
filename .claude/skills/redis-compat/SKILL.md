---
name: redis-compat
description: >
  Redis compatibility testing, redis-compat suite, redis regression, compat failures,
  skiplist management, "does this break redis compat", "run redis tests",
  "check compatibility", anything touching testing/redis-compat/.
---

# Redis Compatibility Testing

Run the official Redis 7.2.4 TCL test suite against FrogDB, interpret results, triage
failures, and manage the skiplist. This skill encapsulates the full redis-compat workflow.

## When To Use

- Running the full redis-compat suite or a targeted subset
- Triaging test failures (deciding skip vs fix)
- Checking whether a code change breaks redis compatibility
- Managing `skiplist.txt` entries
- Creating Rust regression tests in `redis-regression` crate (when explicitly asked)

## Running Tests

### Full sweep (all non-skipped suites)

```bash
just redis-compat
```

Builds FrogDB in release mode, downloads Redis 7.2.4 source (cached), runs every
non-skipped suite with its own server instance and 60s wall-clock timeout.

### Targeted suite

```bash
just redis-compat --single unit/type/string
just redis-compat --single unit/type/zset --verbose
just redis-compat --single unit/multi --skip-build   # reuse existing binary
```

### Specific test within a suite

```bash
just redis-compat-one unit/sort "SORT extracts multiple STORE correctly"
```

This runs `--single <suite> --test <name> --skip-build --verbose`.

### Common flags

| Flag | Purpose |
|------|---------|
| `--skip-build` | Reuse existing `target/release/frogdb-server` binary |
| `--verbose` / `-v` | Full TCL test output (essential for debugging) |
| `--suite-timeout N` | Override per-suite timeout (default 60s) |
| `--keep-data` | Preserve FrogDB data dir after test |
| `--port N` | Use a specific port (default 6399, env: `FROGDB_PORT`) |
| `--tags TAG` | Additional tag filter (repeatable) |
| `--update-skiplists` | Auto-append TIMEOUT/CRASH suites to skiplist |

### Coverage summary

```bash
just redis-compat-coverage
```

### Clean cached Redis source

```bash
just redis-compat-clean
```

## Interpreting Results

### Per-suite status values

| Status | Meaning |
|--------|---------|
| **PASS** | All tests passed (exit code 0) |
| **FAIL** | Some tests failed but suite completed |
| **TIMEOUT** | Suite exceeded wall-clock timeout (hung/deadlocked) |
| **CRASH** | FrogDB server died during the suite |
| **ERROR** | Runner infrastructure error (couldn't start server, etc.) |

### Summary output

The runner prints a table:

```
Suite                       Status     Pass   Fail   Err
------------------------------------------------------------
unit/type/string            PASS       42     0      0
unit/type/list              FAIL       38     4      0
unit/multi                  TIMEOUT    12     0      0
```

Then totals and lists of TIMEOUT/CRASH/FAIL suites.

### Interpreting failing test output

In verbose mode, look for:

- `[err]: <test name>` lines — these are the actual failures
- `Expected ... but got ...` — response mismatch
- `CROSSSLOT` errors — multi-key operation on different hash slots
- `ERR unknown command` — command not implemented
- `ERR ... not implemented` — stub command hit
- Timeouts — test hung waiting for a condition that never occurred

## Failure Triage Workflow

When a test fails, follow this decision tree:

### 1. Auto-skip categories

These failures are expected due to intentional FrogDB design decisions. Add to
`skiplist.txt` with a comment explaining why:

| Category | Examples | Reason |
|----------|----------|--------|
| AOF/RDB | `integration/aof*`, `integration/rdb`, `integration/corrupt-dump*` | FrogDB uses RocksDB, not AOF/RDB |
| SELECT/SWAPDB/MOVE | Tests using `SELECT 1` | Single-DB model |
| DEBUG commands | `unit/debug`, `needs:debug` tagged | Not planned |
| MONITOR | `unit/monitor` | Not planned |
| Module API | `unit/moduleapi` | Not planned |
| Encoding internals | ziplist/listpack/zipmap migration tests | FrogDB has different internal encoding |
| CONFIG REWRITE | Tests calling `CONFIG REWRITE` | Not supported |
| Shutdown/signal handling | `unit/shutdown`, `integration/shutdown` | Different lifecycle model |
| redis-cli / redis-benchmark | `integration/redis-cli`, `integration/redis-benchmark` | Requires Redis binaries |
| Cluster gossip internals | `unit/cluster/slot-ownership`, `unit/cluster/links` | Different cluster protocol |
| Replication internals | `integration/replication*`, `integration/psync*` | Different replication model |
| TLS | `unit/tls` | Not yet supported |
| OOM score | `unit/oom-score-adj` | Linux-specific |
| Inline protocol | Tests using inline commands | FrogDB only supports RESP |

### 2. Gray areas — ask the user

For failures that don't cleanly fit an auto-skip category, present:

1. The failing test name and suite
2. What the test expects vs what FrogDB returned
3. Whether this is a data command (must be exact) or informational (can diverge)
4. Your assessment: is this a FrogDB bug, a known limitation, or an intentional difference?

Let the user decide whether to:
- Fix the FrogDB behavior
- Add to skiplist with a comment
- Investigate further

### 3. Adding to skiplist.txt

Format:

```
# Brief explanation of why this is skipped
unit/suite-name

# Individual test within a suite
unit/suite-name#Exact test name

# Regex pattern for multiple related tests
unit/suite-name#/regex pattern/
```

Group entries by category with section headers. Always add a comment line before the entry.

## Response Format Rules

### Data commands — must be Redis-identical

Commands that clients depend on for correctness must return exactly the same RESP
response as Redis:

- All read/write data commands (GET, SET, LPUSH, ZADD, HGET, etc.)
- RESP wire format must match (bulk string vs integer, nil vs empty, etc.)
- Error messages must start with the same prefix (WRONGTYPE, ERR, CROSSSLOT)

If a data command test fails, this is likely a **bug to fix**, not a skip.

### Informational commands — can diverge

These return server-internal metadata that legitimately differs in FrogDB:

- INFO sections (memory stats, persistence stats, replication details)
- CLIENT LIST (buffer sizes, flags, file descriptors)
- CONFIG GET (different config parameter names)
- DEBUG commands (not implemented)
- SLOWLOG (different format)
- MEMORY USAGE (different allocator)
- LATENCY HISTOGRAM (different internals)

If an informational command test fails, this is usually a **skip**, not a fix.

## Timeout & Deadlock Recovery

When a suite times out:

### 1. Kill the process group

The runner already does this via `os.killpg()`. If you need to manually clean up:

```bash
# Find and kill any stale FrogDB processes
pkill -f "frogdb-server.*port 6399"
```

### 2. Check FrogDB logs

```bash
# Look at stderr from the server (captured in the output)
just redis-compat --single <suite> --verbose --keep-data
# Data dir is at .redis-tests/frogdb-suite-data/
```

### 3. Retry with verbose output

```bash
just redis-compat --single <suite> --verbose --suite-timeout 120
```

### 4. Narrow to specific test

If you know which test hangs (from partial verbose output):

```bash
just redis-compat-one <suite> "test name that hangs"
```

### 5. Common timeout causes

- **Blocking commands** (BLPOP, BRPOP, BZPOPMIN): Test waits for a notification that
  FrogDB doesn't send correctly
- **CLIENT PAUSE**: Test polls `blocked_clients` which FrogDB doesn't update
- **Pub/Sub notification timing**: Test expects synchronous notification that arrives late
  or not at all due to multi-shard architecture
- **Performance tests**: Large dataset operations exceed timeout (increase `--suite-timeout`)

## Why Tests Fail: Architecture Context

FrogDB's architecture causes specific categories of Redis test failures. For the full
architectural invariants, see `~/.claude/skills/shared/frogdb-rules.md`. Below is how
those invariants specifically impact compatibility.

Read `references/architecture.md` for the detailed breakdown of each difference and
which test suites it affects.

### Key architectural differences (summary)

1. **Multi-shard threading** — Cross-slot errors, blocking command atomicity gaps, pub/sub
   fan-out issues
2. **RocksDB persistence** — No AOF/RDB, no fork-based snapshots, different BGSAVE semantics
3. **Single-DB model** — No SELECT, no SWAPDB, no MOVE
4. **Strict script key validation** — Scripts accessing undeclared keys error immediately
5. **HyperLogLog separate type** — No sparse/dense encoding, different OBJECT ENCODING output
6. **Hash unordered** — No insertion-order guarantees for HGETALL etc.
7. **BITFIELD u63 limit** — Unsigned integers limited to 63 bits, not 64

## Creating Regression Tests

When explicitly asked to create a regression test for a fixed compatibility issue, follow
the patterns in `references/regression-tests.md`.

**Do not auto-create regression tests.** Only create them when the user explicitly asks.
The redis-compat TCL suite already provides test coverage — regression tests in Rust are
for cases where we want FrogDB-specific verification alongside CI.
