# Integration-level coverage for pub/sub subscription cap: error text, batch admission, duplicate-count quirk

Status: needs-triage
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

- [ ] Integration test drives a real client to the subscription cap for each kind (channel/pattern/
      shard) and asserts the exact error text/kind returned.
- [ ] Integration test proves all-or-nothing batch admission: a `SUBSCRIBE` (or `PSUBSCRIBE`/
      `SSUBSCRIBE`) batch that doesn't fully fit in remaining capacity is rejected in its entirety;
      pre-existing subscriptions are unaffected.
- [ ] Test pins the current duplicate-count behavior (re-subscribing to an already-held channel
      counts toward the limit via `args.len()` even though the `HashSet` doesn't grow) — either
      documented as accepted-as-is, or filed as a follow-up bug if it diverges from expected/Redis
      behavior.
- [ ] Test comments cross-reference the existing unit coverage (`state.rs`
      `subscribe_limit_and_80pct_latch`, `subscribe_rejects_over_limit`) to avoid future duplicate
      "zero coverage" claims.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/C-pubsub-streams.md (`pubsub-subscription-cap-enforcement-untested`)
- .scratch/testing-improvements/audit/verdicts-C.md (ADJUSTED L2/C1 — unit tests exist, integration residue only)
- server/src/connection/state.rs:685-731
- server/src/connection/pubsub_conn_command.rs:315-321
