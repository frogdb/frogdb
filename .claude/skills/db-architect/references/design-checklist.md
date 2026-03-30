# Design Checklist

Use this checklist for full designs. Not every item applies to every change — skip items that are
clearly irrelevant, but explicitly mark them as "N/A" in the design doc rather than silently
omitting them.

## Threading & Concurrency

- [ ] Shared-nothing invariant maintained? No new shared mutable state between shards?
- [ ] Cross-shard communication uses `mpsc` channels with `ShardMessage` variants only?
- [ ] No mutex/rwlock for cross-shard data? (message passing instead)
- [ ] No `.await` while holding mutable store reference?
- [ ] No `spawn_blocking` in command hot path?
- [ ] Bounded channel backpressure considered? (1024 capacity)
- [ ] If adding new `ShardMessage` variant: response via `oneshot` channel?

## Persistence

- [ ] Explicit `WalStrategy` declared? (not `Infer`)
- [ ] Correct for all three durability modes (Async, Periodic, Sync)?
- [ ] Recovery from WAL produces correct state?
- [ ] Snapshot serialization/deserialization handles the new data?
- [ ] Large values don't block the WAL writer?

## Replication

- [ ] Write commands propagated to replicas correctly?
- [ ] Safe behavior on replicas? (READONLY flag if needed)
- [ ] Replication frame encoding handles new data types/commands?
- [ ] Full sync (FULLRESYNC) includes new data?

## Cluster

- [ ] CROSSSLOT validation for multi-key commands?
- [ ] Works in both standalone and cluster mode?
- [ ] Slot migration handles the new data/command correctly?
- [ ] Hash tag colocation considered for related keys?

## Transactions

- [ ] Correct behavior inside MULTI/EXEC? (queueable?)
- [ ] WATCH interaction handled? (dirty tracking)
- [ ] DISCARD cleans up any intermediate state?

## Scripting

- [ ] Callable from Lua `redis.call()`?
- [ ] Keys properly declared for script key validation?
- [ ] Deterministic behavior? (no randomness unless flagged NONDETERMINISTIC)
- [ ] NOSCRIPT flag set if command should not be callable from scripts?

## ACL

- [ ] Command category assigned? (@read, @write, @admin, etc.)
- [ ] Key access type correct? (read, write, or both)
- [ ] Channel access checked for pub/sub commands?

## Memory

- [ ] Memory accounting updated for new data structures?
- [ ] Eviction policy handles the new key type?
- [ ] `maxmemory` check on write path?
- [ ] Memory overhead documented?

## Observability

- [ ] Prometheus metrics added for new operations?
- [ ] INFO section updated with relevant stats?
- [ ] Slowlog captures the command?
- [ ] Keyspace hit/miss tracking (TRACKS_KEYSPACE flag)?
- [ ] Latency measurement for new code paths?

## Configuration

- [ ] New parameters added to config system (Figment)?
- [ ] Mutable at runtime via CONFIG SET? Or startup-only?
- [ ] Default values reasonable?
- [ ] Documented in config schema?

## Error Handling

- [ ] Correct error types? (`CommandError` variants)
- [ ] Redis-compatible error messages?
- [ ] WRONGTYPE for type mismatches?
- [ ] Edge cases: empty keys, zero-length args, overflow?

## Testing

- [ ] Unit tests for core logic?
- [ ] Integration tests via test harness?
- [ ] Hash tags used for multi-key tests? (`{tag}key`)
- [ ] Edge cases: empty input, max values, concurrent access?
- [ ] Concurrency tests needed? (Shuttle/Turmoil)
- [ ] Property-based tests for complex logic?

## Documentation

- [ ] Website docs updated for the affected subsystem? (`website/src/content/docs/`)
- [ ] ROADMAP.md updated if feature milestone changed?
- [ ] Compatibility docs updated for new Redis command support?

## Performance

- [ ] Time complexity documented?
- [ ] O(N) on user data? Bounded or unbounded?
- [ ] Backpressure under load?
- [ ] Allocation-heavy paths identified?
- [ ] Benchmark added for perf-critical paths?
