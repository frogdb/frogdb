# 24 — Slot drain sends the wrong SUNSUBSCRIBE count and never repairs `ConnectionState`

Status: needs-triage

## What to build

`ShardSubscriptions::drain_sharded_channels_for_slot` (`frogdb-server/crates/core/src/pubsub.rs:668-700`)
evicts every sharded channel belonging to a migrating slot and synthesizes an
`SUnsubscribe { channel, count }` confirmation per affected subscriber. The defect has two
independent halves, both LIVE on main today.

**(a) Wrong authority for `count`.** The `remaining` value at `pubsub.rs:683-687` is computed by
walking `self.sharded_subs` — *this shard's* map — and counting how many of its channels still
carry that `conn_id`. Every other path that emits an SUNSUBSCRIBE count uses the
**connection-global** figure: `ConnectionState::remove_subscription` returns
`self.pubsub.sharded_subscriptions.len()`
(`frogdb-server/crates/server/src/connection/state.rs:635-636`), which spans all shards. With
`num_shards >= 2` the two figures diverge, and a client that drains a slot on shard 1 is told it
has fewer sharded subscriptions than it really does. Redis's `removeChannelsInSlot` reports the
client's total shard-channel count, not a per-partition one, so this is a compatibility deviation
as well as an internal inconsistency. The two figures coincide only at the default
`num_shards == 1`, which is why the defect survived.

**(b) `ConnectionState` is never repaired — bites at ANY shard count, including the default 1.**
The drain reaches the client purely as a rendered frame: the pub/sub delivery arm at
`frogdb-server/crates/server/src/connection.rs:700-754` calls
`pubsub_msg.to_response_with_protocol(...)` and feeds the bytes, and never touches
`self.state.pubsub`. Repo-wide the only removals from `ConnectionState.pubsub.sharded_subscriptions`
are the client-driven SUNSUBSCRIBE at `state.rs:635` and the wholesale reset in `exit_pubsub`
(`state.rs:667-670`, the reset at `:668`). So the migrated-away channels stay in the connection's
set **permanently**. Two observable consequences: every subsequent SUNSUBSCRIBE on that connection
reports an inflated count (it subtracts from a set that still contains dead channels), and the
`MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION` admission check at `state.rs:566-576` (`new_count =
self.pubsub.set(kind).len() + ...`) permanently loses that much headroom — a long-lived subscriber
that rides several reshardings can be refused SSUBSCRIBE for channels it is not actually
subscribed to.

Fix direction: the drain must hand the connection task a real state-repair message rather than a
pre-rendered confirmation. That is a design decision, not a one-line edit — it needs a new variant
on the connection message channel carrying the evicted channel names, so the connection task can
remove them from `state.pubsub.sharded_subscriptions` and derive `count` from the connection-global
set exactly as the SUNSUBSCRIBE path does. Half (a) then falls out for free: once the count is
computed on the connection side there is no shard-local figure to get wrong.

Note that the existing end-to-end test does not catch this. `redis-regression`'s
`test_ssubscribe_client_receives_sunsubscribe_on_slot_migration` (`integration_pubsub.rs:1277`)
asserts `Integer(0)` at `:1394-1398`, but it is degenerate: one channel, one subscription, one
shard — `remaining` is 0 under both authorities and the stale `ConnectionState` entry is never
observed afterwards. Zero `FM-` rows govern the drain.

## Acceptance criteria

- [ ] After a slot drain evicts a connection's sharded channels, `PUBSUB SHARDCHANNELS`-visible
      state and the connection's own admission accounting agree that those channels are gone: a
      subsequent SSUBSCRIBE of `MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION` fresh channels succeeds
      rather than hitting the limit.
- [ ] The `count` field of a drain-emitted SUNSUBSCRIBE equals the connection-global remaining
      sharded-subscription count, identical to what a client-issued SUNSUBSCRIBE would report at
      the same point, under `num_shards >= 2`.
- [ ] Regression test `test_slot_drain_repairs_connection_state_and_reports_global_count`
      (`frogdb-server/crates/server/tests/integration_pubsub.rs`): a client SSUBSCRIBEs to several
      channels spread across at least two shards and two slots, one slot migrates, and the test
      asserts (i) the drain SUNSUBSCRIBE counts descend through the connection-global figures, and
      (ii) a following client-issued SUNSUBSCRIBE on a surviving channel reports the correct count
      rather than an inflated one. Fails against today's tree on both assertions.
- [ ] `test_ssubscribe_client_receives_sunsubscribe_on_slot_migration` stays green unchanged.
- [ ] `just test frogdb-server slot_drain` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 82 (`.scratch/arch-deepening/proposals/82-pubsub-channel-table.md`),
defect H4.

## Comments
