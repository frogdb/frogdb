# Split-Brain Replay

## Overview

When a network partition heals, FrogDB's demoted primary logs divergent writes to disk before
performing a full resync. Today, operators must manually parse these log files and replay operations
via a standard Redis client. This spec adds three replay paths — all sharing the same log parser and
conflict-detection engine:

1. **`SPLITBRAIN` server command** — in-process replay triggered via Redis protocol
2. **Automatic replay on recovery** — config-driven replay when the demoted node reconnects
3. **`frogdb-admin split-brain-replay` CLI** — standalone binary for offline/manual replay

---

## Current State

### What exists

| Component | Status | Location |
|-----------|--------|----------|
| Split-brain log writer | Implemented | `frogdb-replication/src/split_brain_log.rs` |
| Replication ring buffer | Implemented | `frogdb-replication/src/primary.rs` — `ReplicationRingBuffer` |
| Demotion handler | Implemented | `frogdb-server/src/server/mod.rs` (lines 787–861) |
| Demotion event channel | Implemented | `frogdb-cluster/src/state.rs` — `DemotionEvent` |
| Metrics | Implemented | `frogdb_split_brain_events_total`, `frogdb_split_brain_ops_discarded_total`, `frogdb_split_brain_recovery_pending` |
| `has_pending_logs()` | Implemented | Scans data dir for `split_brain_discarded_*.log` files |
| Self-fencing | Implemented | `self_fence_on_replica_loss` + `self_fence_on_quorum_loss` |

### Key files

- `frogdb-server/crates/replication/src/split_brain_log.rs` — log writer, header struct, `has_pending_logs()`
- `frogdb-server/crates/replication/src/primary.rs` — `SplitBrainBufferConfig`, `ReplicationRingBuffer`
- `frogdb-server/crates/server/src/server/mod.rs` — demotion handler task
- `frogdb-server/crates/server/src/config/replication.rs` — `split_brain_log_enabled`, `split_brain_buffer_size`, `split_brain_buffer_max_mb`
- `frogdb-server/crates/cluster/src/state.rs` — `DemotionEvent`, `enable_demotion_detection()`
- `frogdb-server/crates/telemetry/src/definitions.rs` — split-brain metric definitions

### Log file format (existing)

```text
# split_brain_discarded_20240115T103045Z.log
timestamp=2024-01-15T10:30:45Z
old_primary=abc123
new_primary=def456
epoch_old=41
epoch_new=42
seq_diverge_start=12345
seq_diverge_end=12400
ops_discarded=55

*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n
*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n
```

Header is plain text key=value pairs, blank line separator, then raw RESP-encoded commands.

### What's missing

- No log **parser** — only a writer
- No conflict detection
- No replay mechanism (manual CLI piping only)
- No archive/rotation of processed logs
- `frogdb_split_brain_recovery_pending` is set to 1 but never cleared

---

## Shared Design: Log Parser & Conflict Engine

All three replay paths share the same core library crate.

### Log parser

**New module:** `frogdb-replication/src/split_brain_log.rs` (extend existing file)

```rust
pub struct SplitBrainLog {
    pub header: SplitBrainLogHeader,
    pub entries: Vec<ParsedEntry>,
}

pub struct ParsedEntry {
    /// Replication offset of this command (extracted from log ordering + header.seq_diverge_start)
    pub offset: u64,
    /// Parsed RESP command (e.g. ["SET", "key1", "value1"])
    pub command: Vec<Bytes>,
    /// Raw RESP bytes (for replay)
    pub raw_resp: Bytes,
}

/// Parse a split-brain log file into structured form.
pub fn parse_log(path: &Path) -> io::Result<SplitBrainLog> { ... }
```

The parser reads header key=value lines until the blank line, then uses the existing RESP decoder to
frame each command from the remaining bytes.

### Conflict detection

Before replaying a command, the replay engine checks whether the key's current state would conflict:

| Command Type | Conflict Condition | Example |
|---|---|---|
| `SET` / `MSET` | Key exists with different value than pre-split-brain state | Old primary SET key1=A, new primary SET key1=B |
| `DEL` | Key was recreated after split-brain | Old primary DEL key1, new primary SET key1=C |
| `INCR` / `DECRBY` / etc. | Key has been modified since divergence | Counter drifted on both sides |
| `LPUSH` / `RPUSH` / etc. | List exists with different length or contents | Both sides appended |
| `EXPIRE` / `PEXPIRE` | TTL was reset by new primary | Conflicting expiration |

Conflict detection is **best-effort** — it checks current key state via `EXISTS` + `TYPE` + `GET`
(or type-specific read) before each write. This is a heuristic, not a transactional guarantee.

### Conflict strategies

```rust
pub enum ConflictStrategy {
    /// Skip conflicting commands, log them to a separate file
    Skip,
    /// Overwrite current state with the replayed command (last-writer-wins)
    Force,
    /// Abort the entire replay on first conflict
    Abort,
    /// Skip conflicting commands, but only for keys that have been modified
    /// since the split-brain event (uses key version/timestamp heuristic)
    SkipIfModified,
}
```

### Archive behavior

After successful replay (or explicit discard), the log file is renamed:

```
split_brain_discarded_20240115T103045Z.log
  → split_brain_replayed_20240115T103045Z.log      (replayed)
  → split_brain_discarded_20240115T103045Z.log      (skipped/discarded — left as-is)
```

Renaming the prefix from `discarded` to `replayed` ensures `has_pending_logs()` no longer matches it.
The `frogdb_split_brain_recovery_pending` gauge is set back to 0 after processing.

---

## 1. `SPLITBRAIN` Server Command

In-process command accessible via any Redis client connection.

### Subcommands

```
SPLITBRAIN LIST
SPLITBRAIN REPLAY <filename> [STRATEGY skip|force|abort|skip-if-modified] [DRY-RUN]
SPLITBRAIN DISCARD <filename>
SPLITBRAIN INFO <filename>
```

#### `SPLITBRAIN LIST`

Returns an array of pending log filenames in the data directory.

```
> SPLITBRAIN LIST
1) "split_brain_discarded_20240115T103045Z.log"
2) "split_brain_discarded_20240210T142200Z.log"
```

Returns empty array if no pending logs.

#### `SPLITBRAIN INFO <filename>`

Parses the header and returns structured metadata without replaying.

```
> SPLITBRAIN INFO split_brain_discarded_20240115T103045Z.log
 1) "timestamp"
 2) "2024-01-15T10:30:45Z"
 3) "old_primary"
 4) "abc123"
 5) "new_primary"
 6) "def456"
 7) "epoch_old"
 8) (integer) 41
 9) "epoch_new"
10) (integer) 42
11) "seq_diverge_start"
12) (integer) 12345
13) "seq_diverge_end"
14) (integer) 12400
15) "ops_discarded"
16) (integer) 55
```

#### `SPLITBRAIN REPLAY <filename> [STRATEGY ...] [DRY-RUN]`

Replays the log file against the current node. Must be executed on a primary (replicas reject with
`-READONLY`). Default strategy is `skip`.

**DRY-RUN mode:** Parses the log and performs conflict detection but does not execute any writes.
Returns a report of what would happen:

```
> SPLITBRAIN REPLAY split_brain_discarded_20240115T103045Z.log DRY-RUN
1) "total_ops"
2) (integer) 55
3) "would_apply"
4) (integer) 48
5) "conflicts"
6) (integer) 7
7) "conflict_keys"
8) 1) "counter"
   2) "session:abc"
   ...
```

**Live replay:** Executes commands sequentially, respecting the conflict strategy. Returns a summary:

```
> SPLITBRAIN REPLAY split_brain_discarded_20240115T103045Z.log STRATEGY skip
1) "total_ops"
2) (integer) 55
3) "applied"
4) (integer) 48
5) "skipped"
6) (integer) 7
7) "errors"
8) (integer) 0
```

On completion, archives the log file and clears the `frogdb_split_brain_recovery_pending` gauge.

#### `SPLITBRAIN DISCARD <filename>`

Archives the log without replaying. For when operators have reviewed the log and determined the
divergent writes are not needed.

```
> SPLITBRAIN DISCARD split_brain_discarded_20240115T103045Z.log
OK
```

### ACL

`SPLITBRAIN` requires the `admin` ACL category (same as `CLUSTER`, `CONFIG`, `DEBUG`).

### Command routing

In cluster mode, `SPLITBRAIN` is node-local — it operates on the log files of the connected node.
It is not redirected or broadcast. Clients must connect directly to the node that experienced the
split-brain.

---

## 2. Automatic Replay on Recovery

Optional config-driven replay that fires after a demoted primary completes full resync and is
promoted back (or finishes catching up as a replica).

### Trigger

After the demotion handler writes the log file, if `split_brain_auto_replay` is enabled:

1. Node completes full resync with the new primary
2. Node reaches a consistent replication offset (fully caught up)
3. If the node is promoted back to primary: replay fires automatically
4. If the node remains a replica: replay is deferred until next promotion (replicas can't write)

This avoids replaying into a stale state — the node must be caught up before replay.

### Configuration

```toml
[replication]
# Automatically replay split-brain logs after recovery.
# Only fires when the recovered node becomes primary again.
# Default: false (operator must manually replay)
split_brain_auto_replay = false

# Conflict strategy for automatic replay.
# Options: "skip", "force", "abort", "skip-if-modified"
# Default: "skip" (safest — conflicting writes are logged but not applied)
split_brain_replay_strategy = "skip"
```

### Behavior

- Automatic replay uses the same engine as `SPLITBRAIN REPLAY`
- Results are logged at `WARN` level with full summary (applied/skipped/errors)
- If strategy is `abort` and a conflict is hit, the log file is left as pending and the gauge
  remains at 1 — operator must intervene via `SPLITBRAIN` command or CLI
- A new metric `frogdb_split_brain_auto_replays_total` tracks automatic replay attempts

---

## 3. `frogdb-admin split-brain-replay` CLI Tool

Standalone binary for offline replay. Connects to a running FrogDB node as a standard Redis client
and replays the log file.

### Usage

```bash
# Show help
frogdb-admin split-brain-replay --help

# List pending logs on a node
frogdb-admin split-brain-replay --host 10.0.1.5 --port 6379 list

# Show log info without replaying
frogdb-admin split-brain-replay info data/split_brain_discarded_20240115T103045Z.log

# Dry-run: show what would happen
frogdb-admin split-brain-replay \
  --host 10.0.1.5 --port 6379 \
  replay --dry-run data/split_brain_discarded_20240115T103045Z.log

# Replay with skip strategy
frogdb-admin split-brain-replay \
  --host 10.0.1.5 --port 6379 \
  replay --strategy skip data/split_brain_discarded_20240115T103045Z.log

# Force replay (overwrite conflicts)
frogdb-admin split-brain-replay \
  --host 10.0.1.5 --port 6379 \
  replay --strategy force data/split_brain_discarded_20240115T103045Z.log

# Discard a log (mark as processed without replaying)
frogdb-admin split-brain-replay \
  --host 10.0.1.5 --port 6379 \
  discard data/split_brain_discarded_20240115T103045Z.log
```

### Architecture

The CLI tool has two modes:

**Local mode** (no `--host`): Parses the log file directly from disk. Only `info` and `list`
subcommands work — replay requires a running server.

**Remote mode** (`--host` + `--port`): Connects to the server and issues `SPLITBRAIN` protocol
commands. The CLI is a thin wrapper around the server command. This avoids duplicating the replay
engine.

```
┌──────────────────────┐         ┌──────────────────────┐
│  frogdb-admin CLI    │  RESP   │  FrogDB Server       │
│                      │────────▶│                      │
│  • Parse log (local) │         │  SPLITBRAIN REPLAY   │
│  • Send SPLITBRAIN   │         │  • Parse log         │
│    commands           │         │  • Detect conflicts  │
│  • Display results   │◀────────│  • Execute writes    │
│                      │         │  • Archive log       │
└──────────────────────┘         └──────────────────────┘
```

### Authentication

If the server requires authentication, the CLI supports:

```bash
frogdb-admin split-brain-replay \
  --host 10.0.1.5 --port 6379 \
  --password SECRET \
  replay --strategy skip data/split_brain_discarded_20240115T103045Z.log
```

Or via environment variable: `FROGDB_PASSWORD`.

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success (all ops applied or skipped per strategy) |
| 1 | Error (connection failure, auth failure, parse error) |
| 2 | Conflict abort (strategy=abort and conflict detected) |
| 3 | Partial failure (some ops errored during replay) |

---

## Configuration Summary

### Existing fields (no changes)

```toml
[replication]
split_brain_log_enabled = true         # Enable split-brain logging (default: true)
split_brain_buffer_size = 10000        # Ring buffer max entries (default: 10,000)
split_brain_buffer_max_mb = 64         # Ring buffer max memory in MB (default: 64)
```

### New fields

```toml
[replication]
# Automatically replay split-brain logs when recovered node is promoted.
# Default: false
split_brain_auto_replay = false

# Conflict strategy for automatic and default SPLITBRAIN REPLAY.
# Options: "skip", "force", "abort", "skip-if-modified"
# Default: "skip"
split_brain_replay_strategy = "skip"
```

---

## Metrics

### Existing (no changes)

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_split_brain_events_total` | Counter | Split-brain events detected |
| `frogdb_split_brain_ops_discarded_total` | Counter | Operations discarded during demotion |
| `frogdb_split_brain_recovery_pending` | Gauge | 1 if unprocessed log files exist |

### New

| Metric | Type | Description |
|--------|------|-------------|
| `frogdb_split_brain_replays_total` | Counter | Total replay attempts (manual + auto) |
| `frogdb_split_brain_replay_ops_applied_total` | Counter | Operations successfully replayed |
| `frogdb_split_brain_replay_ops_skipped_total` | Counter | Operations skipped due to conflicts |
| `frogdb_split_brain_replay_ops_errored_total` | Counter | Operations that errored during replay |
| `frogdb_split_brain_auto_replays_total` | Counter | Automatic replay attempts |

---

## Implementation Phases

### Phase 1: Log Parser & Conflict Engine

**Crate:** `frogdb-replication`

1. Add `parse_log()` to `split_brain_log.rs`
2. Add `ConflictStrategy` enum and conflict checker
3. Add replay executor (takes parsed log + strategy, returns summary)
4. Clear `recovery_pending` gauge after archive
5. Unit tests for parser round-trip (write → parse → verify)
6. Unit tests for conflict detection scenarios

### Phase 2: `SPLITBRAIN` Server Command

**Crates:** `frogdb-server` (command handler), `frogdb-replication` (engine)

1. Register `SPLITBRAIN` command with `LIST`, `INFO`, `REPLAY`, `DISCARD` subcommands
2. Wire subcommands to the parser/replay engine from Phase 1
3. Add ACL category
4. Integration tests: write a log, replay it, verify key state
5. Integration tests: DRY-RUN mode, conflict strategies, archive behavior

### Phase 3: Automatic Replay

**Crate:** `frogdb-server`

1. Add `split_brain_auto_replay` and `split_brain_replay_strategy` config fields
2. Hook into post-resync / post-promotion path
3. Invoke replay engine with configured strategy
4. Add `frogdb_split_brain_auto_replays_total` metric
5. Integration test: simulate demotion → resync → promotion → verify auto-replay

### Phase 4: CLI Tool

**Crate:** `frogdb-admin` (new binary crate or subcommand of existing admin binary)

1. Add `split-brain-replay` subcommand with `list`, `info`, `replay`, `discard`
2. Local mode: direct file parsing for `info` and `list`
3. Remote mode: RESP client issuing `SPLITBRAIN` commands
4. `--dry-run`, `--strategy`, `--host`, `--port`, `--password` flags
5. Structured output (table for human, JSON with `--json` flag)
6. End-to-end test: spin up server, create split-brain log, replay via CLI
