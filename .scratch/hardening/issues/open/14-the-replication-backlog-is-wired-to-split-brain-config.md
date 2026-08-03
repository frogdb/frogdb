# The replication backlog is wired to the split-brain config keys

Status: needs-triage
Type: bug (availability + correctness)
Severity: likelihood 2/3 (three ordinary-looking config keys, one of which is documented as having
no effect on replication), consequence 3/3 (`split-brain-buffer-size = 0` hangs the server in a
spin loop under a lock; `split-brain-log-enabled = false` silently disables partial resync) —
score 6
Area: replication / backlog + config

## Problem

`replication_init.rs:121-124` builds the partial-resync backlog out of three fields whose names,
docs, and intent all belong to split-brain logging:

```rust
BacklogConfig {
    enabled: config.replication.split_brain_log_enabled,
    max_entries: config.replication.split_brain_buffer_size,
    max_bytes: config.replication.split_brain_buffer_max_mb * 1024 * 1024,
    ttl_secs: config.replication.backlog_ttl_secs,
},
```

Only `ttl_secs` comes from a backlog key. The other three are the split-brain keys, and
`frogdb-server/crates/config/src/replication.rs:100-121` documents `split_brain_log_enabled` as:

> This flag controls ONLY that logging; it does not affect cluster behavior.

That is false. Setting it to `false` — which an operator would reasonably do to stop split-brain
log files accumulating — disables the backlog, and with it partial resync: every reconnecting
replica falls back to a full checkpoint transfer. `split_brain_buffer_size` is likewise documented
as "Maximum number of recent commands to buffer for split-brain detection", not as the thing that
decides how far a replica may fall behind and still reconnect cheaply.

Worse, `split_brain_buffer_size = 0` is accepted and is a hang. `RingBuffer::push`
(`primary/ring_buffer.rs:129-141`) evicts with:

```rust
while entries.len() >= self.max_entries
    || (self.current_bytes.load(Ordering::Relaxed) + entry_size > self.max_bytes
        && !entries.is_empty())
{
    if let Some(evicted) = entries.pop_front() { ... }
}
```

With `max_entries == 0` the first clause is `0 >= 0` — true — and on an empty deque `pop_front()`
returns `None`, so the `if let` body never runs, nothing changes, and the loop spins forever while
holding `self.entries.lock()`. Every subsequent write that touches the backlog blocks behind it.
`ReplicationConfig::validate()` (`config/src/replication.rs:269-304`) bounds `ack_interval_ms`,
`connect_timeout_ms`, `handshake_timeout_ms`, `reconnect_backoff_initial_ms` and
`replica_freshness_timeout_ms`, but has no bound on either buffer field, so this is reachable from
a config file with no error and no warning. `split_brain_buffer_max_mb * 1024 * 1024` is also an
unchecked multiplication on a `usize`.

## Candidate fix

Two separable pieces; the second is the urgent one.

1. Give the backlog its own config keys (`backlog-enabled`, `backlog-size`, `backlog-max-mb`
   alongside the existing `backlog-ttl-secs`), defaulting to today's effective values, and point
   the split-brain keys back at split-brain logging only. Since FrogDB is pre-release, renaming
   rather than aliasing is fine. Fix the `split_brain_log_enabled` doc comment either way — it
   currently states the opposite of what the code does.
2. Make `RingBuffer::push` unhangable regardless of config: bound the loop on `entries.is_empty()`
   in the first clause too, so an empty deque always exits. Then reject `0` for both size fields in
   `ReplicationConfig::validate()` (and in the corresponding `CONFIG SET` validators), and use
   `checked_mul` for the MB conversion. The loop fix is not redundant with validation — validation
   guards the config path, the loop guards every path.

## Forcing tests

A `ring_buffer` unit test constructing a buffer with `max_entries: 0` and calling `push`, asserting
it returns (today it never does — the test needs a timeout harness or the fix landed first). A
`validate()` test asserting `0` is rejected for both fields. An integration test that sets
`split_brain_log_enabled = false` and asserts a reconnecting replica still gets `+CONTINUE`, which
pins the wiring itself and would have caught the swap. Spec rows FM-REPLICATION-012/013/014 (armed
floor, `+CONTINUE` replay, backlog bounded on both axes) are the rows this belongs under.
