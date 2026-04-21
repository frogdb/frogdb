# 5. Latency Histograms for INFO (6 tests — `info_tcl.rs`)

**Status**: Not implemented. Per-client latency tracking exists (`LocalClientStats`, `ClientStats`);
server-wide per-command latency histograms for `INFO latencystats` do not.

**Source files (tests)**:
- `frogdb-server/crates/redis-regression/tests/info_tcl.rs` (lines 26-38)
- `website/src/data/compat-exclusions.json` (latencystats entries)

**Scope**: Implement the `latencystats` section in INFO with configurable percentile reporting,
matching Redis 7.x `latency-tracking` behavior.

---

## Architecture

### Histogram Strategy: Fixed-Bucket (not HDR)

Use a **fixed-bucket histogram** rather than HdrHistogram:

- No external crate dependency needed (no `hdrhistogram` in any workspace Cargo.toml currently).
- Redis itself uses a fixed-bucket approach with power-of-2 microsecond buckets (1us, 2us, 4us, ...
  up to ~4 seconds), which maps well to percentile approximation.
- Per-command overhead stays minimal: a small array of `AtomicU64` counters per bucket.
- Percentile computation is a simple linear scan over cumulative bucket counts.

Bucket design: 64 buckets covering 1us to ~8.6 seconds (bucket `i` covers
`[2^(i/4), 2^((i+1)/4))` microseconds, giving quarter-power-of-2 granularity). This matches
Redis's histogram precision while keeping memory at 512 bytes per command.

### Per-Command Storage

A shared `CommandLatencyHistograms` structure (behind `Arc`) holds a `DashMap<String, LatencyHistogram>`
keyed by lowercase command name (e.g., `"get"`, `"client|id"`, `"config|set"`). This is the same
key format already used by `command_call_counts` in `ClientRegistry`.

### Percentile Computation

Configurable via `latency-tracking-info-percentiles` (default `"50 99 99.9"`). On `INFO latencystats`
request, iterate each command's bucket array, compute cumulative count, and interpolate
the requested percentiles. Output format per Redis:

```
latencystats_get:p50=1.003,p99=2.015,p99.9=3.007
latencystats_client|id:p50=0.501,p99=1.003,p99.9=1.503
```

### Enable/Disable Toggle

`latency-tracking` config param (boolean, default `yes`). When disabled, the recording call in
`process_one_command` is a no-op (checked via `AtomicBool`). Histograms are retained when
disabled so re-enabling continues accumulating. `CONFIG RESETSTAT` clears all histograms.

---

## Implementation Steps

### Step 1: Add `LatencyHistogram` struct

**File**: `frogdb-server/crates/core/src/latency_histogram.rs` (new)

```rust
/// Number of histogram buckets (quarter-power-of-2 from 1us to ~8.6s).
const NUM_BUCKETS: usize = 64;

/// Fixed-bucket latency histogram using atomic counters.
pub struct LatencyHistogram {
    /// Bucket counts. Bucket i covers [bucket_lower_bound(i), bucket_lower_bound(i+1)).
    buckets: [AtomicU64; NUM_BUCKETS],
    /// Total number of samples recorded.
    count: AtomicU64,
}

impl LatencyHistogram {
    pub fn new() -> Self;
    pub fn record(&self, latency_us: u64);
    pub fn percentile(&self, p: f64) -> f64; // returns microseconds
    pub fn count(&self) -> u64;
    pub fn reset(&self);
}

/// Converts a latency in microseconds to a bucket index.
fn bucket_index(latency_us: u64) -> usize;

/// Returns the lower bound (in microseconds) for a given bucket.
fn bucket_lower_bound(index: usize) -> f64;
```

### Step 2: Add `CommandLatencyHistograms` container

**File**: `frogdb-server/crates/core/src/latency_histogram.rs` (same file)

```rust
use dashmap::DashMap;

/// Server-wide per-command latency histograms.
pub struct CommandLatencyHistograms {
    /// Map from lowercase command name to its histogram.
    histograms: DashMap<String, LatencyHistogram>,
    /// Whether tracking is enabled (checked before recording).
    enabled: AtomicBool,
}

impl CommandLatencyHistograms {
    pub fn new(enabled: bool) -> Self;
    pub fn record(&self, cmd_name: &str, latency_us: u64);
    pub fn is_enabled(&self) -> bool;
    pub fn set_enabled(&self, enabled: bool);
    pub fn reset(&self); // clears all histograms
    pub fn percentiles_for(&self, cmd_name: &str, percentiles: &[f64]) -> Option<Vec<(f64, f64)>>;
    pub fn all_commands(&self) -> Vec<String>;
}
```

### Step 3: Wire into server initialization

**File**: `frogdb-server/crates/server/src/server/init.rs`

- Create `Arc<CommandLatencyHistograms>` during server startup.
- Pass into `ConnectionConfig` or `ObservabilityDeps` (prefer `ObservabilityDeps` since it already
  holds `metrics_recorder`).

**File**: `frogdb-server/crates/server/src/connection/deps.rs`

- Add `pub latency_histograms: Arc<CommandLatencyHistograms>` to `ObservabilityDeps`.

### Step 4: Record command latency

**File**: `frogdb-server/crates/server/src/connection.rs` (~line 526)

After the existing `self.state.local_stats.record_command(&cmd_name, elapsed_us);` line, add:

```rust
// Record into server-wide latency histograms (for INFO latencystats)
self.observability.latency_histograms.record(&cmd_name, elapsed_us);
```

The `cmd_name` at this point is already uppercase (e.g., `"GET"`, `"CLIENT"`). The histogram
container should normalize to lowercase internally to match Redis format.

**Subcommand granularity**: The `cmd_name` variable is the top-level command name only (e.g.,
`"CLIENT"` not `"CLIENT|ID"`). For subcommand tracking, access `cmd.args[0]` for commands in the
subcommand list (matching the existing logic at line ~401-414 in `connection.rs` that builds
`cmd_name|sub` for CLIENT LIST). Build the key as `"client|id"` format before recording.

### Step 5: Add CONFIG parameters

**File**: `frogdb-server/crates/config/src/params.rs`

Add two new entries to `config_param_registry()`:

```rust
ConfigParamInfo {
    name: "latency-tracking",
    section: Some("observability"),
    field: Some("latency-tracking"),
    mutable: true,
    noop: false,
},
ConfigParamInfo {
    name: "latency-tracking-info-percentiles",
    section: Some("observability"),
    field: Some("latency-tracking-info-percentiles"),
    mutable: true,
    noop: false,
},
```

**File**: `frogdb-server/crates/server/src/runtime_config.rs`

Add `ParamMeta` entries with getter/setter implementations:

- `latency-tracking`: getter returns `"yes"/"no"` from the `AtomicBool`; setter calls
  `latency_histograms.set_enabled(value == "yes")`.
- `latency-tracking-info-percentiles`: getter returns the current percentile list as a
  space-separated string; setter validates each value is numeric and in range `[0, 100]`,
  stores in a shared `RwLock<Vec<f64>>`.

Validation for `latency-tracking-info-percentiles`:
- Reject non-numeric values (test: "bad configure percentiles").
- Reject values > 100 or < 0.
- Empty string disables percentile output.

### Step 6: Add `INFO latencystats` section

**File**: `frogdb-server/crates/server/src/commands/info.rs`

1. Add `b"latencystats"` to `EXTRA_SECTIONS` (so it appears in `INFO all`/`INFO everything`
   but not in `INFO` with no args, matching Redis behavior).

2. Add match arm in `append_section`:
   ```rust
   b"latencystats" => build_latencystats_info(ctx),
   ```

3. Implement `build_latencystats_info`:
   - Access `CommandLatencyHistograms` from context (needs to be threaded through `CommandContext`
     or handled in the scatter-gather patcher in `handlers/scatter.rs`, similar to how
     `commandstats` is patched).
   - For each command that has at least one recorded sample, emit a line:
     ```
     latencystats_<cmd>:p50=<val>,p99=<val>,p99.9=<val>
     ```
   - Percentile values are in milliseconds with 3 decimal places (matching Redis).

**File**: `frogdb-server/crates/server/src/connection/handlers/scatter.rs`

The `commandstats` section is already patched in the scatter-gather handler (lines 490-517)
because it needs server-wide data. Apply the same pattern for `latencystats`:
- After the commandstats patch block, add a similar block that patches the `# Latencystats`
  section with percentile data from `self.observability.latency_histograms`.

### Step 7: Integrate with CONFIG RESETSTAT

**File**: `frogdb-server/crates/server/src/connection/handlers/config.rs`

In `handle_config_resetstat`, after clearing command call counts, also reset latency histograms:

```rust
self.observability.latency_histograms.reset();
```

---

## Integration Points

### Command Execution Timing

Timing is already captured in `process_one_command` (`connection.rs` line 394):
```rust
let now = std::time::Instant::now();
```
And elapsed computed at line 513:
```rust
let elapsed_us = now.elapsed().as_micros() as u64;
```

This measures wall-clock time from frame parse to response generation, which includes:
- Command routing
- Shard message send/receive (for standard commands)
- Actual execution
- Internal action handling (blocking waits, raft consensus)

This is the correct measurement point for Redis-compatible latency tracking.

### CONFIG SET Handler Changes

`ConfigManager::set_async` (runtime_config.rs line 1267) already handles parameter changes
through the `ParamMeta` setter function. The new `latency-tracking` and
`latency-tracking-info-percentiles` setters will modify shared atomic state that takes effect
on the next command without needing shard notification.

### Subcommand Key Format

Redis uses `command|subcommand` format for latencystats keys (e.g., `client|id`, `config|set`).
FrogDB already builds this format for CLIENT LIST (connection.rs lines 401-414). Reuse the same
logic to build the histogram key. The list of commands with subcommands is already defined there:
`client`, `config`, `command`, `object`, `debug`, `memory`, `cluster`, `acl`, `xinfo`, `xgroup`,
`script`, `function`, `slowlog`, `latency`, `module`.

---

## FrogDB Adaptations

### Async Execution Model

Redis measures latency synchronously within its single-threaded event loop. FrogDB's latency
measurement includes async overhead:

1. **Tokio task scheduling**: Time between `Instant::now()` and actual shard execution includes
   potential task scheduling delays. This is acceptable because it reflects true client-perceived
   latency.

2. **Shard message round-trip**: Standard commands (GET, SET, etc.) are sent to a shard worker
   via channel and the response is awaited. This round-trip is part of the measured latency,
   which is correct for observability purposes.

3. **Blocking commands**: Commands like BLPOP block in `handle_blocking_wait`. The current timing
   (`now.elapsed()`) will include the block duration. Redis also includes blocking time in its
   latencystats. The test `latencystats: blocking commands` verifies this behavior.

4. **Connection-level commands**: Commands like CLIENT, CONFIG, INFO execute directly on the
   connection task without shard routing. Their latency is measured identically.

### Concurrency Considerations

- `CommandLatencyHistograms` uses `DashMap` for the command map (concurrent reads/writes from
  many connection tasks).
- Each `LatencyHistogram` uses `AtomicU64` counters, allowing lock-free concurrent recording.
- Percentile reads during `INFO latencystats` iterate atomics; slight inconsistency during
  concurrent writes is acceptable (same as Redis during high load).
- The `enabled` flag is an `AtomicBool` checked before each record call; toggling it via
  CONFIG SET takes effect immediately without locking.

---

## Tests

- `latencystats: disable/enable`
- `latencystats: configure percentiles`
- `latencystats: bad configure percentiles`
- `latencystats: blocking commands`
- `latencystats: subcommands`
- `latencystats: measure latency`

---

## Verification

### Unit Tests

- `LatencyHistogram::record` + `percentile` with known values.
- `bucket_index` boundary conditions (0us, 1us, max).
- `CommandLatencyHistograms::set_enabled(false)` stops accumulating.
- Percentile validation rejects non-numeric and out-of-range values.

### Integration Tests

Run the 6 regression tests after removing them from `compat-exclusions.json`:

```bash
just test redis-regression latencystats
```

Expected test behaviors:
1. **disable/enable**: CONFIG SET latency-tracking no, verify INFO latencystats is empty;
   re-enable, run commands, verify percentiles appear.
2. **configure percentiles**: CONFIG SET latency-tracking-info-percentiles "50 99",
   verify only those percentiles are output.
3. **bad configure percentiles**: CONFIG SET with "abc" or "101" returns error.
4. **blocking commands**: BLPOP with timeout, verify latencystats_blpop shows latency >= timeout.
5. **subcommands**: CLIENT ID, CONFIG SET; verify `latencystats_client|id` and
   `latencystats_config|set` keys appear.
6. **measure latency**: DEBUG SLEEP (or equivalent delay), verify reported latency is in
   expected range.

### Manual Smoke Test

```bash
# Start FrogDB
just dev

# In redis-cli:
CONFIG SET latency-tracking yes
CONFIG SET latency-tracking-info-percentiles "50 99 99.9"
SET foo bar
GET foo
INFO latencystats
# Should show latencystats_set and latencystats_get lines with p50/p99/p99.9
```
