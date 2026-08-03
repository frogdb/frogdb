# min-replicas-max-lag cannot round-trip a sub-second window, and 0 disables the gate

Status: done
Type: bug (correctness)
Severity: likelihood 2/3 (any deployment with a sub-second `min-replicas-timeout-ms`, plus any
config rewrite that reads back and re-applies), consequence 3/3 (the `NOREPLICAS` write gate stops
filtering on freshness, so dead replica sessions count as good and writes are accepted that the
operator configured the server to refuse) — score 6
Area: replication / config

## Problem

`min-replicas-max-lag` is stored in milliseconds and exposed in seconds
(`frogdb-server/crates/server/src/runtime_config.rs:2038-2048`):

```rust
get: |mgr| {
    MinReplicasMaxLagSecs(
        mgr.runtime.read().unwrap().min_replicas_timeout_ms / 1000,
    )
},
apply: |mgr, MinReplicasMaxLagSecs(secs)| {
    mgr.runtime.write().unwrap().min_replicas_timeout_ms = secs * 1000;
```

Integer division truncates, so any `min_replicas_timeout_ms` below 1000 reads back as `0`. That
value is not merely lossy — feeding it back through `apply` (which `CONFIG REWRITE` and any
read-modify-write tooling does) stores `0`, and `0` means something different from "sub-second":

```rust
// tracker.rs:172-177
pub fn count_good_replicas(&self, max_lag: Duration) -> u32 {
    self.get_streaming_replicas()
        .iter()
        .filter(|r| max_lag.is_zero() || r.last_ack_time.elapsed() < max_lag)
        .count() as u32
}
```

`max_lag.is_zero()` short-circuits the freshness filter, so every session in `Streaming` phase
counts as good regardless of when it last ACKed. The `NOREPLICAS` guard
(`connection/guards.rs:376-386`) then admits writes on the strength of replicas that may have been
silent for hours. A 500 ms window — which `integration_replication.rs:6614` uses, so it is a shape
the project already exercises — degrades in one round trip from the tightest possible freshness
requirement to none at all.

Two smaller problems sit in the same parameter. `validate` is `ConfigParam::no_validate`, so
`CONFIG SET min-replicas-max-lag 0` is accepted directly with the same effect, and `secs * 1000` is
an unchecked multiplication that overflows for large inputs.

The neighbouring `replica-freshness-timeout-ms` parameter (`runtime_config.rs:3081-3090`) gets this
right — it rejects `0` explicitly — which makes the omission here look like an oversight rather
than a decision.

## Candidate fix

Decide what `0` means and enforce it in one place. Redis treats `min-replicas-max-lag 0` as
"disable the lag check", so keeping that meaning is the compatible choice — but then the truncation
must not be able to *produce* it. Options, roughly in order of preference:

1. Expose the parameter in milliseconds under its own name (`min-replicas-max-lag-ms`) and keep the
   seconds alias for Redis compatibility, with the alias's `get` rounding *up* to 1 for any non-zero
   sub-second value so a round trip can never widen the window to "off".
2. Failing that, at minimum make `get` round up rather than truncate, and use `checked_mul` in
   `apply`.

Either way, `count_good_replicas`'s `max_lag.is_zero()` branch deserves a comment naming it as the
deliberate disable, since today it reads like a guard against a degenerate duration.

## Forcing tests

A round-trip test: set `min_replicas_timeout_ms = 500` in config, `CONFIG GET
min-replicas-max-lag`, `CONFIG SET` that value back, assert the stored ms is still a freshness
window and not `0`. A `count_good_replicas` unit test with a stale session asserting it is excluded
at a 500 ms window and included at `0`, which pins the disable semantics explicitly. An integration
test asserting `NOREPLICAS` still fires after a replica goes silent — the audit found no test
covers the freshness filter at all today.

## Resolution

Fixed as candidate option 1, with one deviation from the proposed shape.

**Deviation: rename + virtual row, not an alias.** The plan said "keep the seconds alias". A
literal alias — a second derived registry row naming the same `section`/`field` — would have made
`ConfigManager::config_updates()` emit two writes for one TOML key on `CONFIG REWRITE`, and since
the seconds view rounds, the losing writer would silently retune the operator's file. Instead the
derived row was **renamed** to `min-replicas-max-lag-ms` (it keeps `replication.min-replicas-timeout-ms`
and is the only spelling `CONFIG REWRITE` persists), and Redis's seconds spelling was registered as
a **virtual** row (`section: None, field: None`) — a pure live-value view that `CONFIG GET`/`SET`
reach and `CONFIG REWRITE` skips. The TOML key is unchanged, so no config file breaks.

- `MinReplicasMaxLagSecs::from_millis` is `div_ceil(1000)` — rounds up, so no non-zero window can
  ever report as `0`.
- `MinReplicasMaxLagSecs::to_millis` is `checked_mul(1000)`, wired as the parameter's `validate`,
  so an overflowing seconds value is rejected before `apply` touches the runtime cell.
- `0` keeps Redis's "disable the lag check" meaning, and `count_good_replicas`'s `is_zero()`
  disjunct now carries a comment naming it as the deliberate disable.
- The freshness comparison moved into `frogdb_replication::ack_is_fresh(ack_age, window)`, shared
  with `ReplicationQuorumChecker::count_fresh_streaming_replicas`, so `<` vs `<=` is assertable
  without racing a wall clock. `ReplicaSession::backdate_last_ack_for_test` makes staleness exact.

Spec: new row **FM-REPLICATION-046**, plus an amended `Forced by` cell on FM-REPLICATION-042.
Closes GAP-1 and GAP-4.

**One claim in this issue was stale.** "`count_good_replicas`'s `max_lag.is_zero()` branch reads
like a guard against a degenerate duration" — the function already carried a doc comment naming
Redis's `min-replicas-max-lag 0` semantics. The comment was still worth moving to the branch itself
and expanding on why inverting the sentinel would fence a healthy primary, but the code was not as
unexplained as described.
