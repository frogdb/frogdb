# FrogDB Debugging Guide

Tools and techniques for investigating FrogDB behavior. For symptom-driven troubleshooting, see [TROUBLESHOOTING.md](TROUBLESHOOTING.md). For passive observability (metrics, logging, tracing), see [OBSERVABILITY.md](OBSERVABILITY.md).

## Feature Labels

Throughout this document:
- **Unmarked features** are Redis-compatible
- **`[FrogDB]`** indicates FrogDB-specific features not found in Redis/Valkey

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

**Field Reference:**

| Field | Description | Debugging Use |
|-------|-------------|---------------|
| `encoding` | Internal representation | Identify suboptimal encodings |
| `serializedlength` | Bytes when serialized | Estimate persistence cost |
| `lru_seconds_idle` | Seconds since last access | Find cold keys |
| `refcount` | Internal reference count | Internal debugging only |

**Encoding Types by Data Structure:**

| Type | Encodings |
|------|-----------|
| String | `embstr` (small, <44 bytes), `raw` (large), `int` (numeric) |
| Sorted Set | `skiplist` |
| Hash | `hashtable` |
| List | `quicklist` |
| Set | `hashtable`, `intset` (small integer sets) |

#### DEBUG STRUCTSIZE

Display sizes of internal data structures:

```
DEBUG STRUCTSIZE
bits:64 robj:16 frogvalue:48 skiplistnode:48 hashtableentry:32
```

Useful for memory estimation and understanding overhead.

#### DEBUG SLEEP

Block the server for a specified duration:

```
DEBUG SLEEP 1.5    # Sleep for 1.5 seconds
```

**Use cases:**
- Testing client timeout handling
- Verifying watchdog triggers
- Simulating slow operations

**Note:** This command may be disabled in production builds via configuration.

#### DEBUG HASHING `[FrogDB]`

Show which internal shard a key maps to:

```
DEBUG HASHING user:123
key:user:123 hash:0x7f1234567890abcd shard:3 num_shards:8

DEBUG HASHING {user:1}:profile
key:{user:1}:profile tag:user:1 hash:0x1234567890abcdef shard:5 num_shards:8
```

**Output fields:**
- `key` - The full key
- `tag` - Hash tag extracted (if present)
- `hash` - xxhash64 value
- `shard` - Target shard (0-indexed)
- `num_shards` - Total internal shards

This command is essential for understanding key distribution and diagnosing shard hotspots.

#### DEBUG DUMP-VLL-QUEUE `[FrogDB]`

Inspect the VLL (Very Lightweight Locking) transaction queue for a shard:

```
DEBUG DUMP-VLL-QUEUE 0
shard:0 queue_depth:5 executing_txid:1000
pending:
  txid:1001 operation:SET keys:1 queued_at:1705312245.123
  txid:1002 operation:MSET keys:3 queued_at:1705312245.456
  txid:1003 operation:DEL keys:1 queued_at:1705312245.789
  txid:1004 operation:ZADD keys:1 queued_at:1705312246.012
  txid:1005 operation:EVAL keys:2 queued_at:1705312246.345
```

Use this to diagnose VLL queue buildup and transaction ordering issues.

#### DEBUG DUMP-CONNECTIONS `[FrogDB]`

Dump detailed state of all connections:

```
DEBUG DUMP-CONNECTIONS
connection:1 thread:2 state:executing addr:192.168.1.10:54321
  current_command:MGET keys:10 started_at:1705312245.123
  pending_shards:[0,2,5] received_responses:[0,2]
connection:2 thread:3 state:idle addr:192.168.1.11:54322
  idle_since:1705312240.000
```

#### Dangerous Commands (Not Implemented)

The following Redis DEBUG commands are intentionally **not implemented** due to safety concerns:

| Command | Risk |
|---------|------|
| `DEBUG SEGFAULT` | Crashes server |
| `DEBUG RELOAD` | Can cause data corruption |
| `DEBUG CRASH-AND-RECOVER` | Unsafe crash simulation |
| `DEBUG SET-ACTIVE-EXPIRE` | Internal state manipulation |

---

## LATENCY Monitoring Framework

FrogDB implements a Redis-compatible latency monitoring framework for diagnosing sources of latency spikes.

### Configuration

```toml
[latency]
monitor_threshold_ms = 0    # 0 = disabled, >0 = record events exceeding threshold
history_size = 160          # Events retained per event type
```

At runtime:
```
CONFIG SET latency-monitor-threshold 100    # Monitor events >100ms
CONFIG GET latency-monitor-threshold
```

### Event Types

| Event | Description |
|-------|-------------|
| `command` | Individual command execution exceeding threshold |
| `fast-command` | Commands expected to be O(1) or O(log N) |
| `expire-cycle` | Active expiry sweep duration |
| `eviction-cycle` | Memory eviction sweep duration |
| `persistence-write` | WAL or snapshot write latency |
| `shard-message` `[FrogDB]` | Cross-shard message delivery latency |
| `scatter-gather` `[FrogDB]` | Multi-shard operation total latency |

### LATENCY Commands

#### LATENCY LATEST

Show the most recent latency event for each type:

```
LATENCY LATEST
1) 1) "command"
   2) (integer) 1705312245      # Unix timestamp
   3) (integer) 150             # Latency in ms
2) 1) "expire-cycle"
   2) (integer) 1705312200
   3) (integer) 85
```

#### LATENCY HISTORY

Show time series for a specific event type:

```
LATENCY HISTORY command
1) 1) (integer) 1705312245
   2) (integer) 150
2) 1) (integer) 1705312200
   2) (integer) 120
3) 1) (integer) 1705312100
   2) (integer) 200
```

#### LATENCY DOCTOR

Get human-readable latency analysis with recommendations:

```
LATENCY DOCTOR

Sam, I have a few reports for you:

1. command: 3 latency spikes (max: 250ms, avg: 180ms).
   Worst event occurred at 14:30:45 on 2024-01-15.
   Analyze with: SLOWLOG GET
   Recommendation: Avoid O(N) commands on large collections.

2. expire-cycle: 1 latency spike (max: 150ms).
   Large number of keys expiring simultaneously.
   Recommendation: Spread expiry times to avoid thundering herd.

3. shard-message: 2 latency spikes (max: 50ms, avg: 35ms).
   Cross-shard communication delays detected.
   Recommendation: Use hash tags to colocate related keys.
```

#### LATENCY RESET

Clear latency history:

```
LATENCY RESET                    # Clear all events
LATENCY RESET command expire     # Clear specific event types
```

### Intrinsic Latency Testing

Test the baseline latency of the system (OS scheduler, virtualization, hardware):

```bash
frogdb-cli --intrinsic-latency 100    # Run for 100 seconds
```

Output:
```
Max latency so far: 0.089 ms
Max latency so far: 0.234 ms
Max latency so far: 0.892 ms

Intrinsic latency summary (100 seconds test):
  Mean: 0.045 ms
  P50:  0.032 ms
  P99:  0.156 ms
  Max:  0.892 ms

If max latency > 1ms, investigate OS/hypervisor configuration.
```

**Important:** Run this on the **server**, not the client machine. High intrinsic latency (>1ms) indicates system-level issues unrelated to FrogDB.

---

## Memory Debugging

### MEMORY Commands

See [OBSERVABILITY.md](OBSERVABILITY.md#memory) for basic MEMORY USAGE and MEMORY STATS.

#### MEMORY DOCTOR (Detailed)

MEMORY DOCTOR performs automated analysis and provides actionable recommendations:

```
MEMORY DOCTOR

Sam, I have a few reports for you:

1. Peak memory is 2x current usage.
   Peak: 8.00GB, Current: 4.00GB
   Consider restarting to reclaim fragmented memory.

2. High fragmentation detected: ratio 1.8
   RSS: 7.20GB, Used: 4.00GB
   Wasted memory: ~3.20GB
   Recommendation: Schedule restart during maintenance window.

3. Big keys detected (>1MB each):
   - user:sessions (hash): 2.5MB, 50000 fields
   - analytics:daily (sorted set): 1.8MB, 100000 members
   Consider breaking into smaller keys.

4. Shard memory imbalance: [FrogDB]
   Shard 0: 800MB, Shard 7: 200MB (4:1 ratio)
   Review key naming patterns or use hash tags for better distribution.
```

### Finding Big Keys

Scan the keyspace to identify memory-heavy keys:

```bash
frogdb-cli --bigkeys
# Scanning the entire keyspace to find biggest keys...
#
# -------- summary -------
#
# Sampled 1000000 keys
# Total key length: 12.5MB
#
# Biggest string: user:avatar:12345 (2.1MB)
# Biggest hash: session:abc123 (1.5MB, 10000 fields)
# Biggest sorted set: leaderboard:global (800KB, 50000 members)
# Biggest list: queue:tasks (500KB, 10000 items)
# Biggest set: tags:popular (200KB, 5000 members)
```

Options:
```bash
frogdb-cli --bigkeys --samples 1000    # Sample size (default: 0 = scan all)
```

### Per-Shard Memory Analysis `[FrogDB]`

View memory distribution across shards:

```
INFO frogdb

# FrogDB
frogdb_shards:8
frogdb_shard_0_keys:125000
frogdb_shard_0_memory:134217728
frogdb_shard_1_keys:130000
frogdb_shard_1_memory:142606336
frogdb_shard_2_keys:118000
frogdb_shard_2_memory:125829120
...
```

A healthy cluster shows relatively even distribution. Significant imbalance (>2:1 ratio) indicates hot keys or poor key naming patterns.

---

## Connection & Shard Debugging

### CLIENT LIST Analysis

```
CLIENT LIST
id=1 addr=192.168.1.10:54321 fd=5 name=worker-1 age=3600 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 cmd=get
id=2 addr=192.168.1.11:54322 fd=6 name= age=1800 idle=300 flags=b db=0 sub=0 psub=0 multi=-1 qbuf=26 qbuf-free=32742 obl=0 oll=8 omem=1048576 cmd=blpop
```

**Key Fields for Debugging:**

| Field | Description | Alert Threshold |
|-------|-------------|-----------------|
| `idle` | Seconds since last command | >300s for non-pub/sub |
| `flags` | Client state | Many `b` = potential issues |
| `qbuf` | Query buffer bytes | >64KB = slow client sending |
| `obl/oll/omem` | Output buffer stats | omem >1MB = slow client reading |

**Client Flags:**

| Flag | Meaning |
|------|---------|
| `N` | Normal client |
| `M` | Master (replication) |
| `S` | Replica |
| `P` | Pub/Sub subscriber |
| `x` | In MULTI/EXEC transaction |
| `b` | Blocked (BLPOP, etc.) |

**Diagnostic Patterns:**

| Pattern | Diagnosis | Action |
|---------|-----------|--------|
| Many high-idle connections | Connection pool misconfiguration | Review pool settings |
| High omem across clients | Network bottleneck / slow clients | Check network, client performance |
| Many blocked (`b` flag) | Long-running blocking commands | Review blocking timeouts |
| Growing qbuf | Slow query parsing | Check for malformed commands |

### MONITOR Command

Stream all commands processed by the server:

```
MONITOR
+OK
+1705312245.123456 [0 192.168.1.10:54321] "SET" "user:1" "data"
+1705312245.234567 [0 192.168.1.11:54322] "GET" "user:1"
+1705312245.345678 [0 192.168.1.10:54321] "ZADD" "scores" "100" "player1"
```

**Warning:** MONITOR has significant performance impact (~50% throughput reduction). Use sparingly in production.

**Use cases:**
- Debugging application command patterns
- Identifying unexpected commands
- Verifying client behavior
- Security auditing

**Security note:** AUTH commands are redacted; no passwords are logged.

### Per-Shard Statistics `[FrogDB]`

```
INFO frogdb

# FrogDB
frogdb_shards:8
frogdb_shard_0_keys:125000
frogdb_shard_0_memory:134217728
frogdb_shard_0_queue_depth:5
frogdb_shard_0_commands_processed:50000000
frogdb_shard_0_scatter_requests:1000000
frogdb_shard_1_keys:130000
...
```

**Key metrics:**
- `queue_depth` - Pending operations in VLL queue (high = hotspot)
- `commands_processed` - Total commands handled by shard
- `scatter_requests` - Cross-shard operations received

### Scatter-Gather Tracing `[FrogDB]`

Enable debug logging to trace multi-shard operations:

```
CONFIG SET loglevel debug
```

Log output shows operation flow:
```json
{"level":"DEBUG","target":"frogdb::scatter","message":"Scatter started","request_id":"abc123","operation":"MGET","shards":[0,2,5],"keys_per_shard":[3,2,1]}
{"level":"DEBUG","target":"frogdb::scatter","message":"Shard response","request_id":"abc123","shard":0,"latency_us":150,"status":"ok"}
{"level":"DEBUG","target":"frogdb::scatter","message":"Shard response","request_id":"abc123","shard":2,"latency_us":2500,"status":"ok"}
{"level":"DEBUG","target":"frogdb::scatter","message":"Shard response","request_id":"abc123","shard":5,"latency_us":180,"status":"ok"}
{"level":"DEBUG","target":"frogdb::scatter","message":"Gather complete","request_id":"abc123","total_latency_us":2600,"slowest_shard":2}
```

This helps identify which shard is causing scatter-gather delays.

---

## External Introspection Tools

### Building Debug Binaries

#### Debug Build (Full Symbols, No Optimizations)

```bash
cargo build
# Binary: target/debug/frogdb-server
# Full debug symbols, assertions enabled, no optimizations
# Best for: development debugging, step-through with GDB
```

#### Release with Debug Info

```bash
RUSTFLAGS="-C debuginfo=2" cargo build --release
# Binary: target/release/frogdb-server
# Full debug symbols WITH optimizations
# Best for: production debugging, crash analysis
```

#### Release with Frame Pointers

```bash
RUSTFLAGS="-C force-frame-pointers=yes -C debuginfo=2" cargo build --release
# Enables accurate stack traces in perf, bpftrace, flamegraphs
# Best for: production profiling
```

#### Stripping and Splitting Symbols

For smaller binaries with separate debug info:

```bash
# Extract debug symbols to separate file
objcopy --only-keep-debug target/release/frogdb-server frogdb-server.debug

# Strip debug info from binary
objcopy --strip-debug target/release/frogdb-server

# Link debug info for GDB auto-loading
objcopy --add-gnu-debuglink=frogdb-server.debug target/release/frogdb-server
```

### GDB Debugging

#### Attaching to Running Process

```bash
# Find process ID
pgrep frogdb-server

# Attach GDB
gdb -p <pid>

# Or with explicit binary (for symbols)
gdb target/release/frogdb-server -p <pid>
```

#### Useful GDB Commands for Async Rust

```gdb
# Show all threads
info threads

# Switch to specific thread (Tokio workers are numbered)
thread 2

# Backtrace (may be deep due to async state machines)
bt

# Backtrace with full local variables
bt full

# Show local variables in current frame
info locals

# Print Rust variable (GDB understands Rust types)
p *my_variable

# Print HashMap contents
p my_hashmap.table

# Conditional breakpoint
break frogdb_core::shard::Shard::execute if cmd.name == "SET"

# Catch panics
catch signal SIGABRT

# Continue after attaching
continue
```

#### Core Dump Analysis

```bash
# Enable core dumps
ulimit -c unlimited

# Set core pattern (Linux)
echo '/tmp/core.%e.%p' | sudo tee /proc/sys/kernel/core_pattern

# After crash, analyze
gdb target/release/frogdb-server /tmp/core.frogdb-server.12345

# In GDB:
bt full                    # Full backtrace with locals
info threads               # See all thread states
thread apply all bt        # Backtrace for all threads
```

### DTrace/USDT Probes `[FrogDB]`

FrogDB can be compiled with USDT (User Statically Defined Tracing) probes for zero-overhead-when-disabled tracing.

#### Enabling USDT Probes

```toml
# Cargo.toml
[features]
usdt = ["dep:usdt"]

[dependencies]
usdt = { version = "0.5", optional = true }
```

Build:
```bash
cargo build --release --features usdt
```

#### Probe Definitions

| Probe | Arguments | Description |
|-------|-----------|-------------|
| `frogdb:::command-start` | (command, key, conn_id) | Command execution begins |
| `frogdb:::command-done` | (command, latency_us, status) | Command execution completes |
| `frogdb:::shard-message-sent` | (from_shard, to_shard, msg_type) | Cross-shard message |
| `frogdb:::shard-message-received` | (shard, msg_type, queue_depth) | Message received by shard |
| `frogdb:::key-expired` | (key, shard_id) | Key expiration |
| `frogdb:::key-evicted` | (key, shard_id, policy) | Key eviction |
| `frogdb:::memory-pressure` | (used, max, action) | Memory limit approached |

#### DTrace Examples (macOS/illumos)

```bash
# List available probes
sudo dtrace -l -n 'frogdb*:::'

# Trace all commands
sudo dtrace -n 'frogdb:::command-start { printf("%s %s conn=%d\n", copyinstr(arg0), copyinstr(arg1), arg2); }'

# Command latency histogram
sudo dtrace -n '
frogdb:::command-done {
    @latency[copyinstr(arg0)] = quantize(arg1);
}
tick-10s { printa(@); clear(@); }'

# Trace slow commands (>10ms)
sudo dtrace -n '
frogdb:::command-done
/arg1 > 10000/ {
    printf("%s took %d us\n", copyinstr(arg0), arg1);
}'

# Cross-shard message tracing
sudo dtrace -n '
frogdb:::shard-message-sent {
    printf("shard %d -> shard %d: %s\n", arg0, arg1, copyinstr(arg2));
}'
```

#### Registering Probes

Probes must be registered at startup:

```rust
fn main() {
    // Register USDT probes with the system
    #[cfg(feature = "usdt")]
    frogdb::probes::register().expect("Failed to register USDT probes");

    // Start server...
}
```

### eBPF/bpftrace (Linux)

eBPF provides powerful kernel and user-space tracing on Linux.

#### Prerequisites

```bash
# Ubuntu/Debian
sudo apt install bpftrace bpfcc-tools linux-headers-$(uname -r)

# Fedora/RHEL
sudo dnf install bpftrace bcc-tools kernel-devel
```

#### Tracing Rust Functions with uprobes

```bash
# Find function symbols (Rust names are mangled)
nm target/release/frogdb-server | grep execute | rustfilt

# Trace function entry/exit with latency
sudo bpftrace -e '
uprobe:./target/release/frogdb-server:*Shard*execute* {
    @start[tid] = nsecs;
}
uretprobe:./target/release/frogdb-server:*Shard*execute* /@start[tid]/ {
    @latency_us = hist((nsecs - @start[tid]) / 1000);
    delete(@start[tid]);
}
END { print(@latency_us); }'
```

**Note:** Build with frame pointers for accurate stack traces:
```bash
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release
```

#### Memory Allocation Tracing

Trace jemalloc allocations:

```bash
# Allocation size histogram
sudo bpftrace -e '
uprobe:/lib/x86_64-linux-gnu/libjemalloc.so*:je_malloc {
    @alloc_sizes = hist(arg0);
}
interval:s:10 { print(@alloc_sizes); clear(@alloc_sizes); }'

# Large allocations (>1MB) with stack trace
sudo bpftrace -e '
uprobe:/lib/x86_64-linux-gnu/libjemalloc.so*:je_malloc /arg0 > 1048576/ {
    printf("Large alloc: %d bytes\n", arg0);
    print(ustack);
}'
```

#### Network Tracing

```bash
# New TCP connections to FrogDB port
sudo bpftrace -e '
tracepoint:sock:inet_sock_set_state
/args->newstate == 1 && args->dport == 6379/ {
    printf("New connection from %s\n", ntop(args->family, args->saddr));
}'

# Bytes per connection
sudo bpftrace -e '
tracepoint:tcp:tcp_probe
/args->dport == 6379 || args->sport == 6379/ {
    @bytes[args->saddr, args->daddr] = sum(args->data_len);
}'
```

#### Pre-built BCC Tools

```bash
# Function latency histogram
sudo funclatency-bpfcc ./target/release/frogdb-server:'*execute*' -m

# CPU profiling (stack sampling)
sudo profile-bpfcc -U -p $(pgrep frogdb-server) 10

# Off-CPU analysis (where time is spent waiting)
sudo offcputime-bpfcc -p $(pgrep frogdb-server) 10 > offcpu.stacks
```

#### Aya Framework

For building production eBPF monitoring tools in Rust:

```rust
// Example: Custom FrogDB latency monitor
use aya::programs::UProbe;
use aya::Bpf;

async fn main() -> Result<()> {
    let mut bpf = Bpf::load(include_bytes_aligned!("../target/bpfel/release/probe"))?;

    let program: &mut UProbe = bpf.program_mut("command_latency")?.try_into()?;
    program.load()?;
    program.attach(
        Some("_ZN10frogdb_core5shard5Shard7execute*"),
        0,
        "/usr/bin/frogdb-server",
        None,
    )?;

    // Read perf events from eBPF map...
    Ok(())
}
```

See [Aya documentation](https://aya-rs.dev/) for building custom monitors.

---

## Developer Debugging `[FrogDB]`

### tokio-console

[tokio-console](https://github.com/tokio-rs/console) provides real-time visualization of async task state.

#### Setup

```toml
# Cargo.toml
[dependencies]
console-subscriber = { version = "0.2", optional = true }

[features]
tokio-console = ["console-subscriber"]
```

```rust
// main.rs
#[cfg(feature = "tokio-console")]
fn init_console() {
    console_subscriber::init();
}

#[tokio::main]
async fn main() {
    #[cfg(feature = "tokio-console")]
    init_console();

    // ...
}
```

Build and run:
```bash
RUSTFLAGS="--cfg tokio_unstable" cargo build --features tokio-console
./target/debug/frogdb-server &
tokio-console    # Connect to localhost:6669
```

#### Task View

```
Tasks (5 total, 1 blocked)
ID   NAME             STATE     POLLS   BUSY      IDLE
1    acceptor         idle      1000    2ms       5m
2    shard-0          idle      50000   100ms     5m
3    shard-1          running   50001   5.2s      0ms    <- PROBLEM
4    shard-2          idle      50000   100ms     5m
5    metrics          idle      100     1ms       5m

Task 3 Details:
  State: Running (stuck in poll)
  Busy time: 5.2s
  Target: frogdb::shard::execute_command
  Location: src/shard.rs:234
```

#### Warnings Detected

| Warning | Meaning | Fix |
|---------|---------|-----|
| `lost waker` | Task waker dropped without wake | Ensure Futures are stored/polled |
| `never yielded` | Task ran >1s without yielding | Add `yield_now()` in tight loops |
| `very long poll` | Single poll >100ms | Break up blocking work |
| `waker thrashing` | Excessive wake/poll cycles | Review task notification logic |

### Concurrency Testing with loom

[loom](https://github.com/tokio-rs/loom) performs exhaustive concurrency testing by exploring all possible interleavings.

#### When to Use

- Testing lock-free data structures
- Verifying atomic ordering correctness
- Finding deterministic repros for race conditions

#### Setup

```toml
[dev-dependencies]
loom = "0.7"

[features]
loom = []
```

#### Writing loom Tests

```rust
#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicU64, Ordering};
    use loom::thread;

    #[test]
    fn test_txid_counter_ordering() {
        loom::model(|| {
            let counter = Arc::new(AtomicU64::new(0));

            let handles: Vec<_> = (0..2).map(|_| {
                let counter = counter.clone();
                thread::spawn(move || {
                    counter.fetch_add(1, Ordering::SeqCst)
                })
            }).collect();

            let results: Vec<_> = handles.into_iter()
                .map(|h| h.join().unwrap())
                .collect();

            // Each thread got a unique ID
            assert!(results[0] != results[1]);
        });
    }
}
```

Running:
```bash
RUSTFLAGS="--cfg loom" cargo test --features loom -- loom_tests
```

**Limitations:**
- Scales exponentially with threads/sync points
- Only tests code inside `loom::model` closure
- Use for targeted verification of critical concurrent code

### Randomized Testing with shuttle

[shuttle](https://github.com/awslabs/shuttle) (AWS Labs) provides randomized concurrency testing that scales better than loom.

#### When to Use

- Testing larger concurrent systems
- Integration-level concurrency tests
- When loom's exhaustive approach is too slow

#### Setup

```toml
[dev-dependencies]
shuttle = "0.7"
```

#### Writing shuttle Tests

```rust
#[cfg(test)]
mod shuttle_tests {
    use shuttle::sync::Arc;
    use shuttle::thread;

    #[test]
    fn test_vll_ordering() {
        shuttle::check_random(|| {
            let queue = Arc::new(ShardTransactionQueue::new());

            let handles: Vec<_> = (0..4).map(|_| {
                let queue = queue.clone();
                thread::spawn(move || {
                    let txid = acquire_txid();
                    queue.enqueue(txid, mock_operation());
                })
            }).collect();

            for h in handles {
                h.join().unwrap();
            }

            assert!(queue.operations_in_txid_order());
        }, 1000); // Run 1000 random schedules
    }
}
```

#### Parallel Exploration with PortfolioRunner

```rust
#[test]
fn test_comprehensive() {
    use shuttle::scheduler::{RandomScheduler, PCTScheduler};

    let runner = shuttle::PortfolioRunner::new()
        .add(RandomScheduler::new(500))
        .add(PCTScheduler::new(500, 3));

    runner.run(|| {
        // Your concurrent test code
    });
}
```

### Debug Logging Patterns

#### Enabling Debug Logs

At startup:
```bash
FROGDB_LOGGING__LEVEL=debug frogdb-server
# Or
RUST_LOG=frogdb=debug frogdb-server
```

At runtime:
```
CONFIG SET loglevel debug
```

#### Targeted Logging with Spans

```rust
#[tracing::instrument(level = "debug", skip(self), fields(conn_id = %ctx.conn_id))]
async fn execute_command(&self, cmd: &Command, ctx: &Context) -> Result<Response> {
    tracing::debug!(command = %cmd.name(), key = ?cmd.key(), "Executing");

    // All nested logs include conn_id
    let result = self.dispatch(cmd).await;

    tracing::debug!(result = ?result, "Completed");
    result
}
```

Enable specific targets:
```bash
RUST_LOG=frogdb::shard=debug,frogdb::scatter=trace frogdb-server
```

#### Request ID Correlation

```rust
let request_id = uuid::Uuid::new_v4();
let span = tracing::info_span!("request", %request_id);
let _guard = span.enter();

// All logs in this scope include request_id
tracing::info!("Processing command");
```

### Deterministic Simulation Testing

Inspired by [FoundationDB's simulation testing](https://apple.github.io/foundationdb/testing.html), FrogDB can be tested with deterministic I/O simulation.

#### Concept

Replace real I/O with simulated components to enable:
- Deterministic replay of failures
- Fast-forward time simulation
- Injection of arbitrary failures
- Finding rare bugs in hours instead of months

#### SimulationContext Trait

```rust
/// Abstraction over real vs simulated I/O
pub trait SimulationContext: Send + Sync {
    /// Current time (simulated or real)
    fn now(&self) -> Instant;

    /// Sleep (instant in simulation)
    async fn sleep(&self, duration: Duration);

    /// Send network message (may fail/delay in simulation)
    async fn send(&self, addr: SocketAddr, data: &[u8]) -> io::Result<usize>;

    /// Check for injected failure at this point
    fn maybe_fail(&self, failure_point: &str) -> Result<(), SimulatedFailure>;
}
```

#### Running Simulation Tests

```rust
#[test]
fn test_shard_failover_deterministic() {
    let seed = 12345u64;
    let mut sim = Simulation::new(seed);

    // Inject failures
    sim.inject_failure_at(100.ms(), "shard-2-execute", FailureKind::Panic);
    sim.inject_network_partition(200.ms(), &[0, 1], &[2, 3]);

    sim.run(|| async {
        let server = SimulatedServer::new(&sim, 4);

        server.set("key1", "value1").await?;
        server.set("key2", "value2").await?;

        sim.advance_time(150.ms()).await;

        // Shard 2 has panicked, verify graceful degradation
        assert!(server.get("key1").await.is_ok());

        Ok(())
    });
}
```

This approach finds rare bugs that would require months of production operation to encounter naturally.

---

## References

### Internal Documentation

- [OBSERVABILITY.md](OBSERVABILITY.md) - Metrics, logging, tracing configuration
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Symptom-driven diagnosis runbooks
- [FAILURE_MODES.md](FAILURE_MODES.md) - Error handling and recovery procedures
- [TESTING.md](TESTING.md) - Test strategy and running tests
- [CONFIGURATION.md](CONFIGURATION.md) - Configuration system
- [CONCURRENCY.md](CONCURRENCY.md) - Thread architecture and VLL

### External Tools

- [tokio-console](https://github.com/tokio-rs/console) - Async Rust debugger
- [loom](https://github.com/tokio-rs/loom) - Concurrency model checker
- [shuttle](https://github.com/awslabs/shuttle) - Randomized concurrency testing
- [bpftrace](https://github.com/iovisor/bpftrace) - eBPF tracing language
- [Aya](https://aya-rs.dev/) - Rust eBPF framework
- [usdt crate](https://github.com/oxidecomputer/usdt) - USDT probes for Rust

### Prior Art

- [Redis Debugging Guide](https://redis.io/docs/latest/operate/oss_and_stack/management/debugging/)
- [Valkey Latency Monitoring](https://valkey.io/topics/latency-monitor/)
- [DragonflyDB Monitoring](https://www.dragonflydb.io/docs/managing-dragonfly/monitoring)
- [FoundationDB Testing](https://apple.github.io/foundationdb/testing.html)
- [CockroachDB Debug Pages](https://www.cockroachlabs.com/docs/stable/ui-debug-pages)
