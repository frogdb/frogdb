---
title: "Debugging Guide"
description: "Tools and techniques for investigating FrogDB behavior."
sidebar:
  order: 15
---
Tools and techniques for investigating FrogDB behavior.

---

## Built-in Diagnostic Commands

### DEBUG Commands

FrogDB implements a safe subset of Redis DEBUG commands for diagnostics.

#### DEBUG OBJECT

Inspect the internal representation of a key:

```
DEBUG OBJECT mykey
Value at:0x7f1234567890 refcount:1 encoding:embstr serializedlength:12 lru:1234567 lru_seconds_idle:10
```

| Field | Description | Debugging Use |
|-------|-------------|---------------|
| `encoding` | Internal representation | Identify suboptimal encodings |
| `serializedlength` | Bytes when serialized | Estimate persistence cost |
| `lru_seconds_idle` | Seconds since last access | Find cold keys |

#### DEBUG STRUCTSIZE

Display sizes of internal data structures for memory estimation.

#### DEBUG SLEEP

Block the server for a specified duration. Useful for testing client timeout handling and verifying watchdog triggers.

#### DEBUG HASHING (FrogDB-specific)

Show which internal shard a key maps to:

```
DEBUG HASHING user:123
key:user:123 hash:0x7f1234567890abcd shard:3 num_shards:8

DEBUG HASHING {user:1}:profile
key:{user:1}:profile tag:user:1 hash:0x1234567890abcdef shard:5 num_shards:8
```

Essential for understanding key distribution and diagnosing shard hotspots.

#### DEBUG DUMP-VLL-QUEUE (FrogDB-specific)

Inspect the VLL transaction queue for a shard:

```
DEBUG DUMP-VLL-QUEUE 0
shard:0 queue_depth:5 executing_txid:1000
pending:
  txid:1001 operation:SET keys:1 queued_at:1705312245.123
  txid:1002 operation:MSET keys:3 queued_at:1705312245.456
```

#### DEBUG DUMP-CONNECTIONS (FrogDB-specific)

Dump detailed state of all connections including current command, pending shards, and response status.

#### Dangerous Commands (Not Implemented)

`DEBUG SEGFAULT`, `DEBUG RELOAD`, `DEBUG CRASH-AND-RECOVER`, `DEBUG SET-ACTIVE-EXPIRE` are intentionally not implemented due to safety concerns.

---

## LATENCY Monitoring Framework

Redis-compatible latency monitoring for diagnosing latency spikes.

### Event Types

| Event | Description |
|-------|-------------|
| `command` | Individual command execution exceeding threshold |
| `fast-command` | Commands expected to be O(1) or O(log N) |
| `expire-cycle` | Active expiry sweep duration |
| `eviction-cycle` | Memory eviction sweep duration |
| `persistence-write` | WAL or snapshot write latency |
| `shard-message` (FrogDB-specific) | Cross-shard message delivery latency |
| `scatter-gather` (FrogDB-specific) | Multi-shard operation total latency |

### Commands

- **LATENCY LATEST** - Most recent latency event per type
- **LATENCY HISTORY** - Time series for a specific event type
- **LATENCY DOCTOR** - Human-readable latency analysis with recommendations
- **LATENCY RESET** - Clear latency history

---

## Memory Debugging

### MEMORY DOCTOR

Automated analysis with recommendations for peak memory, fragmentation, big keys, and shard memory imbalance.

### Per-Shard Memory Analysis (FrogDB-specific)

```
INFO frogdb

# FrogDB
frogdb_shards:8
frogdb_shard_0_keys:125000
frogdb_shard_0_memory:134217728
...
```

A healthy distribution shows relatively even values. Significant imbalance (>2:1 ratio) indicates hot keys or poor key naming patterns.

---

## Connection & Shard Debugging

### MONITOR Command

Stream all commands processed by the server. Uses bounded `tokio::sync::broadcast` channel (default capacity 4096). Zero overhead when no subscribers (single atomic load per command). AUTH commands are redacted.

### Per-Shard Statistics (FrogDB-specific)

Key metrics per shard: `queue_depth` (VLL queue), `commands_processed`, `scatter_requests`.

### Scatter-Gather Tracing (FrogDB-specific)

Enable `debug` log level to trace multi-shard operations with per-shard latency breakdown.

---

## External Introspection Tools

### Building Debug Binaries

```bash
cargo build                                    # Full symbols, no optimizations
RUSTFLAGS="-C debuginfo=2" cargo build --release  # Symbols WITH optimizations
RUSTFLAGS="-C force-frame-pointers=yes -C debuginfo=2" cargo build --release  # For profiling
```

### DTrace/USDT Probes (FrogDB-specific)

Always-on USDT probes with zero runtime overhead when no tracer is attached.

| Probe | Arguments | Description |
|-------|-----------|-------------|
| `frogdb:::command-start` | (command, key, conn_id) | Command execution begins |
| `frogdb:::command-done` | (command, latency_us, status) | Command execution completes |
| `frogdb:::shard-message-sent` | (from_shard, to_shard, msg_type) | Cross-shard message |
| `frogdb:::key-expired` | (key, shard_id) | Key expiration |
| `frogdb:::key-evicted` | (key, shard_id, policy) | Key eviction |
| `frogdb:::memory-pressure` | (used, max, action) | Memory limit approached |

### eBPF/bpftrace (Linux)

Trace Rust functions with uprobes, memory allocation with jemalloc probes, network connections.

Build with frame pointers for accurate stack traces:
```bash
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release
```

---

## Developer Debugging (FrogDB-specific)

### tokio-console

Real-time visualization of async task state. Build with `--features tokio-console` and `RUSTFLAGS="--cfg tokio_unstable"`.

| Warning | Meaning | Fix |
|---------|---------|-----|
| `lost waker` | Task waker dropped without wake | Ensure Futures are stored/polled |
| `never yielded` | Task ran >1s without yielding | Add `yield_now()` in tight loops |
| `very long poll` | Single poll >100ms | Break up blocking work |

### Concurrency Testing with loom

Exhaustive concurrency testing for lock-free data structures and atomic ordering correctness. Scale limitations: only for targeted verification of critical concurrent code.

### Randomized Testing with shuttle

Randomized concurrency testing that scales better than loom. Use `PortfolioRunner` to combine multiple scheduling strategies.

### Debug Logging Patterns

```bash
FROGDB_LOGGING__LEVEL=debug frogdb-server    # At startup
RUST_LOG=frogdb::shard=debug,frogdb::scatter=trace frogdb-server  # Targeted
CONFIG SET loglevel debug                    # At runtime
```
