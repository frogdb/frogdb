# min-replicas-max-lag cannot round-trip a sub-second window, and 0 disables the gate

Status: needs-triage
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
