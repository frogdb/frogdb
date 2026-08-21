# 29 — Sharded channels escape the unique-channel threshold entirely

Status: needs-triage

## What to build

`ShardSubscriptions::unique_channel_count` (`frogdb-server/crates/core/src/pubsub.rs:866-868`) is
`self.channel_subs.len()` — **broadcast channels only**. It is the sole input to the
`MAX_UNIQUE_CHANNELS_PER_SHARD` warning inside `check_thresholds_after_subscribe`
(`pubsub.rs:881-934`; the comparison is at `:905`, `MAX_UNIQUE_CHANNELS_PER_SHARD = 100_000` at
`:285`, and the reset hysteresis reads the same counter at `:944`). `sharded_subs` is never
consulted. A shard can therefore accumulate an unbounded number of unique SSUBSCRIBE channels and
the operator-facing scale warning **never fires**, no matter how large the table grows.

This is a behaviour bug in the guard, not a performance one: the guard exists precisely to tell an
operator "your pub/sub table has grown past the size this design is comfortable with", and for one
of the three subscription kinds it is structurally silent. The failure is one-sided and quiet —
nothing errors, nothing logs, the number just stays below the threshold forever. Compare the
per-connection side, which *does* account for all three kinds independently
(`frogdb-server/crates/server/src/connection/state.rs:566-576` dispatches on `SubKind` to
`MAX_SUBSCRIPTIONS_PER_CONNECTION` / `MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION` /
`MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION`); the shard-wide threshold is the only place where
sharded channels are silently omitted. Note also that `MAX_UNIQUE_CHANNELS_PER_SHARD` is a warning
threshold with no admission check behind it, so the miss is purely observability — but per the
standing "observability accuracy over parity" preference, a threshold that structurally cannot fire
is worse than one that is absent.

Fixing this is a **policy decision, which is why it is filed rather than patched**: should
broadcast and sharded channels share one 100 000 ceiling, or should sharded channels get their own
constant and their own warning (and their own `warned_channel_90` latch, since the two would
otherwise fight over one flag at `:937-953`)? Redis tracks `server.pubsubshard_channels` as a dict
separate from `server.pubsub_channels` and reports them as separate `INFO` fields, which argues for
a separate ceiling and a separate warning. Either answer is a cheap field read once the decision is
made; making the decision is the work.

Adjacent but distinct: issue 78 in `.scratch/testing-improvements-round2/issues/open/` covers the
naming/semantics of the `PubsubSubscribers` gauge fed from `shard/diagnostics.rs`, not this
threshold gap.

## Acceptance criteria

- [ ] A shard whose sharded-channel count crosses the chosen ceiling emits the scale warning
      exactly once, with the count and limit that the ruling settles on (shared or separate).
- [ ] The hysteresis reset (`reset_thresholds_if_needed`, `pubsub.rs:937-953`) re-arms the sharded
      warning on the same terms as the broadcast one, without the two latches interfering.
- [ ] Regression test `test_sharded_channels_count_toward_unique_channel_threshold`
      (`frogdb-server/crates/core/src/pubsub.rs` test module): drive SSUBSCRIBE-only registrations
      past 90% of the ceiling on a single `ShardSubscriptions` and assert the threshold fires
      (today it never does), plus a paired test asserting the warning does not re-fire until after
      the reset hysteresis.
- [ ] `just test frogdb-core unique_channel_threshold` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 82 (`.scratch/arch-deepening/proposals/82-pubsub-channel-table.md`),
companion issue candidate recorded under §Hotfixes and §Problem 1 (review note N7).

## Comments
