# Integration-level coverage for pub/sub subscription cap: error text, batch admission, duplicate-count quirk

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 1/3 (score 2)
Area: Pub/Sub

## Context

`admit_subscriptions` (`server/src/connection/state.rs:685-731`) enforces a subscription cap with a
`LimitReached` error, a one-shot 80%-capacity warning latch, and is consulted from `subscribe_kind`
(`pubsub_conn_command.rs:315-321`) with distinct errors per subscription kind (channel/pattern/shard).

The original audit claimed zero test coverage for this. That claim is **false and must not be
re-asserted**: `state.rs` unit tests `subscribe_limit_and_80pct_latch` and
`subscribe_rejects_over_limit` already exist and cover the core admission logic in isolation
(verdict ADJUSTED L2/C1 — the finder missed in-crate unit tests).

The real, still-open gap is integration-level, i.e. behavior as observed through a real client
connection rather than direct unit calls:

- **(a)** The exact error text/kind surfaced to a real client at the protocol layer, for each
  subscription kind, is not verified end-to-end.
- **(b)** All-or-nothing batch admission is unverified: `SUBSCRIBE a b c` when only 2 slots remain
  should reject the entire batch (not partially subscribe 2 of 3), and prior subscriptions must
  survive the rejected attempt.
- **(c)** A duplicate-count quirk: `admit_subscriptions` counts `args.len()` including duplicate
  channel names in a single command, but `add_subscription` inserts into a `HashSet`. Re-subscribing
  to an already-held channel can therefore spuriously consume cap headroom without actually growing
  the subscription set — this asymmetry is unpinned in either direction.

## What to build

Integration (client-level, not unit) tests covering (a), (b), and (c) above.

## Acceptance criteria

- [x] Integration test drives a real client to the subscription cap for each kind (channel/pattern/
      shard) and asserts the exact error text/kind returned.
- [x] Integration test proves all-or-nothing batch admission: a `SUBSCRIBE` (or `PSUBSCRIBE`/
      `SSUBSCRIBE`) batch that doesn't fully fit in remaining capacity is rejected in its entirety;
      pre-existing subscriptions are unaffected.
- [x] Test pins the current duplicate-count behavior (re-subscribing to an already-held channel
      counts toward the limit via `args.len()` even though the `HashSet` doesn't grow) — either
      documented as accepted-as-is, or filed as a follow-up bug if it diverges from expected/Redis
      behavior. (Filed as follow-up bug: issue 61.)
- [x] Test comments cross-reference the existing unit coverage (`state.rs`
      `subscribe_limit_and_80pct_latch`, `subscribe_rejects_over_limit`) to avoid future duplicate
      "zero coverage" claims.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/C-pubsub-streams.md (`pubsub-subscription-cap-enforcement-untested`)
- .scratch/testing-improvements/audit/verdicts-C.md (ADJUSTED L2/C1 — unit tests exist, integration residue only)
- server/src/connection/state.rs:685-731
- server/src/connection/pubsub_conn_command.rs:315-321

## Resolution

Added three integration tests to `frogdb-server/crates/server/tests/integration_pubsub.rs`
(`test_subscribe_cap_channel_kind_integration`, `_pattern_kind_integration`,
`_sharded_kind_integration`), sharing a `run_subscription_cap_scenario` helper driven by a
`CapKindSpec` (subscribe/publish command, exact limit-error text, cap constant, name prefix) — one
scenario per subscription kind, each over a real `TestServer` + `TestClient` (real RESP, not direct
`ConnectionState` calls).

**Config-knob note**: there is no runtime cap knob to lower for the harness — I verified
`MAX_SUBSCRIPTIONS_PER_CONNECTION` (10_000), `MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION` (1_000), and
`MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION` (10_000) are compile-time `const`s in
`frogdb_core::pubsub`, never wired into the config crate (no `max-subscriptions`/`subscription-limit`
param exists anywhere). The tests therefore drive real clients to the actual production caps rather
than a lowered value; this turned out to be cheap in practice — even the two 10_000-scale scenarios
run in ~0.09s each (a single large multi-bulk SUBSCRIBE/SSUBSCRIBE frame plus a sequential drain of
the per-channel confirmations), so there was no need to invent a knob for this task.

Each scenario, on one connection: (1) batches to `max - 3` subscriptions; (2) **(a)+(b)** sends a
4-name over-cap batch and asserts the exact per-kind error text (`"ERR max subscriptions reached"` /
`"ERR max pattern subscriptions reached"` / `"ERR max sharded subscriptions reached"`), then proves
the rejection was all-or-nothing by publishing to a pre-existing channel (still delivered) and by
subscribing one new legit name (lands at exactly `base.len() + 1`, not `+ 4`); (3) climbs to exactly
`max - 1`; (4) **(c)** pins the duplicate-count quirk two ways — a duplicated brand-new name in one
command at `max - 1` headroom is spuriously rejected (real growth would be `+1`, which fits exactly),
and a re-subscribe to an already-held channel once genuinely full is also spuriously rejected (real
growth is `0`). Both assert the *current* (surprising) behavior honestly, as the acceptance criteria
allows, with comments cross-referencing the existing `state.rs` unit tests
(`subscribe_limit_and_80pct_latch`, `subscribe_rejects_over_limit`) so this residue isn't re-filed as
"zero coverage" again.

Filed `.scratch/testing-improvements/issues/62-subscription-cap-duplicate-count-quirk.md` as the
follow-up bug for the quirk itself (fix: charge the real HashSet delta, not raw `args.len()`) —
option (b) of the acceptance criteria, since both pinned cases are genuine surprises (idempotent
re-subscribe-on-reconnect failing exactly at a full cap is the more concerning of the two).

**Verification**: `just fmt frogdb-server` (no changes), `just lint frogdb-server` (clean, 0
warnings), `just test frogdb-server integration_pubsub` (149 passed / 149, no regressions), and the
three new tests run individually plus 3x together
(`just test frogdb-server test_subscribe_cap_`) — 3/3 passed every run
(`3 tests run: 3 passed, 1917 skipped`, ~0.09-0.10s each run).
