# Subscription cap admits by `args.len()`, not real HashSet growth: spurious rejections at the boundary

Status: needs-triage
Type: bug (behavior)
Origin: testing-gap issue 50 (integration-level subscription-cap coverage), pinned by
`test_subscribe_cap_channel_kind_integration` / `_pattern_kind_integration` / `_sharded_kind_integration`
in `frogdb-server/crates/server/tests/integration_pubsub.rs`
Severity: likelihood 2/3, consequence 1/3 (score 2) — carried over from the parent audit line

## Context

`ConnectionState::admit_subscriptions` (`frogdb-server/crates/server/src/connection/state.rs:685-731`)
enforces the per-connection subscribe cap by charging the *raw argument count* of a SUBSCRIBE/
PSUBSCRIBE/SSUBSCRIBE command (`args.len()`, passed in from `pubsub_conn_command.rs:323`) against the
remaining headroom. But the actual subscription set is a `HashSet` (`add_subscription`,
`state.rs:736-751`), which only grows for genuinely new names. This creates two provable divergences,
both now pinned end-to-end over real RESP by the tests filed for issue 50:

1. **Duplicate name within one command.** With exactly one real slot free, `SUBSCRIBE new new` (the
   same brand-new channel twice) is charged `args.len() == 2` and rejected with the kind's limit
   error — even though the real growth would be `+1`, which fits exactly at the cap.
2. **Re-subscribing to an already-held channel at a full cap.** With the cap already fully consumed,
   re-issuing `SUBSCRIBE` for a channel the connection is *already subscribed to* is charged `+1` and
   rejected — even though it is logically a no-op (the `HashSet` doesn't grow at all).

Both are spurious rejections: a connection sitting exactly at (or one below) its cap can be kicked
into an error reply by a command that would not actually have grown its subscription set. Case 2 is
the more concerning of the two in practice — idempotent re-subscribe-on-reconnect is a common client
pattern (e.g. resubscribing after a network blip without tracking exactly what was already held), and
it fails here specifically when the connection is already maximally subscribed, which is exactly the
scenario a cap is meant to guard gracefully rather than punish.

## What to build

Change `admit_subscriptions` (or its caller) to charge only the delta that will actually land in the
`HashSet`, not the raw arg count:

- Deduplicate the incoming batch before charging (fixes case 1).
- Skip already-held names when computing the charge (fixes case 2) — this likely means passing the
  channel list itself (or a pre-computed "genuinely new" count) into `admit_subscriptions` instead of
  a bare `usize`, since the current signature can't distinguish "2 new names" from "1 new name typed
  twice" or "1 name that's already held".

Keep the same fail-fast, all-or-nothing contract (a batch that doesn't fit is still rejected in full)
— only the *counting* changes, not the atomicity.

## Acceptance criteria

- [ ] `SUBSCRIBE <held> <held>` (both already-held, or a duplicate of the same already-held name) at a
      full cap succeeds as a no-op (returns the normal confirmation(s), does not consume headroom).
- [ ] `SUBSCRIBE new new` with exactly one real slot free succeeds (charges `+1`, not `+2`).
- [ ] A genuinely over-cap batch (more *unique new* names than remaining headroom) still rejects in
      full — no regression to the all-or-nothing contract.
- [ ] Update the three cap-quirk assertions in `integration_pubsub.rs`
      (`test_subscribe_cap_{channel,pattern,sharded}_kind_integration`) to match the corrected
      behavior (they currently assert the *quirky* rejection as an explicit pin — flip them to assert
      success once fixed, keeping a comment noting the prior surprising behavior for history).
- [ ] Same fix applies uniformly to all three subscription kinds (channel/pattern/sharded), since all
      three share `admit_subscriptions`.

## Blocked by

None — independent, can start immediately. Not blocking issue 50 (which pins current behavior as its
acceptance criterion explicitly allows).

## References

- .scratch/testing-improvements/issues/50-subscription-cap-integration.md
- .scratch/testing-improvements/audit/C-pubsub-streams.md (`pubsub-subscription-cap-enforcement-untested`)
- frogdb-server/crates/server/src/connection/state.rs:685-731 (`admit_subscriptions`)
- frogdb-server/crates/server/src/connection/pubsub_conn_command.rs:316-327 (`subscribe_kind`)
- frogdb-server/crates/server/tests/integration_pubsub.rs (`run_subscription_cap_scenario` and its
  three per-kind callers)
