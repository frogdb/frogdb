# The replication backlog is wired to the split-brain config keys

Status: done
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

## Resolution

Both pieces fixed as proposed, renaming rather than aliasing.

**The wiring.** `ReplicationConfigSection` gained `backlog_enabled` / `backlog_size` /
`backlog_max_mb` (joining the existing `backlog_ttl_secs`), and `split_brain_buffer_size` /
`split_brain_buffer_max_mb` were **removed** rather than kept as aliases. There is only one ring
buffer — it serves both the partial-resync replay and the split-brain divergence capture from the
same entries — so those two keys never described a buffer that exists independently of the backlog;
they described the backlog under a name that hid what it did. `split_brain_log_enabled` survives as
a genuinely log-only flag (`SplitBrainLogger` is already `Option`, `None` when it is off), and its
doc comment now says what the code does. Defaults are today's effective values (`true`, `10_000`,
`64`).

The mapping itself was extracted from the middle of `init_replication` into a pure
`fn backlog_config(&ReplicationConfigSection) -> BacklogConfig`, precisely so a unit test can hold
it — the defect was entirely in the mapping, which is why no replication test could see it (the
backlog behaved correctly throughout).

**Key mutability: TOML-only (`#[param(skip)]`), not `CONFIG SET`-able.** `ReplicationRingBuffer::new`
fixes `max_entries`/`max_bytes` at construction and there is no live-resize path, so a `CONFIG SET`
could only report a change the running buffer never made — the same lying-observability class this
campaign is closing (cf. FM-REPLICATION-046). Redis's live `repl-backlog-size` resize is unbuilt in
FrogDB; if it is ever built, these become mutable then. `backlog_ttl_secs` stays
`#[param(mutable, name = "repl-backlog-ttl")]` because `BacklogTtl` is a live `Arc` seam a retune
actually reaches. Shape follows the `4ced6229` precedent (`skip` + a `validate()` that rejects 0).

**The hang.** `push`'s eviction loop now leads with `!entries.is_empty() &&`, so the guard covers
**both** caps instead of only the byte one and an empty deque always exits. `validate()` rejects `0`
on both caps with an error that also names `backlog_enabled` (the switch the operator wanted), and
rejects a `backlog_max_mb` whose byte form overflows; `backlog_max_bytes()` is the single
`checked_mul(1024 * 1024)` spelling that validation and the wiring share, and the wiring saturates
rather than wraps on the (now unreachable) overflow. No `CONFIG SET` validator was needed — the caps
are not settable.

**The hang was real and was reproduced.** The forcing test was written first and hung: it failed
after the 10 s harness timeout with `max_entries = 0: push never returned — the eviction loop cannot
drain an empty deque`, and passed once the guard was hoisted. It runs `push` on a spawned thread
behind `recv_timeout` so a regression fails the test rather than wedging the suite, and covers
`max_entries = 0`, `max_bytes = 0` and both-zero.

Forcing tests: `ring_buffer_push_terminates_under_a_degenerate_cap` (frogdb-replication),
`zero_backlog_caps_are_rejected_and_the_mb_conversion_is_checked` (frogdb-config),
`the_backlog_is_configured_by_backlog_keys_only` and
`an_overflowing_backlog_mb_saturates_rather_than_wrapping` (frogdb-server, unit — deliberately not
the integration suite, which `cargo mutants -p` never runs), plus
`partial_resync_survives_split_brain_logging_disabled` (integration; boots with
`split_brain_log_enabled = false` and asserts a raw `PSYNC` still gets `+CONTINUE`). Spec row
**FM-REPLICATION-047**, plus an amendment to FM-REPLICATION-016, whose "Deliberate non-guarantees"
section documented this hang as a live bug.

**Two claims in this issue were off.** The suggested rows were FM-REPLICATION-012/013/014; the row
that actually documents the hang — and cites this issue in its `Bug refs` — is **016** (the
both-axes bound), not 014. And "reject `0` ... in the corresponding `CONFIG SET` validators" has no
target: the caps were `skip` params before this fix and stayed `skip` after, so `CONFIG SET` never
reached them.
