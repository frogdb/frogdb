# Sync-durability must gate the client ack

Status: done

## Parent

[spec-review-persistence.md](../../../formal-spec/reviews/2026-08-13-antipattern/spec-review-persistence.md)
— Finding C1 (CRITICAL): "FM-PERSISTENCE-002: `sync` durability does not gate the ack under the
*default* failure policy".

## What is wrong

FM-PERSISTENCE-002's Observable is unconditional — *"Every write the client saw a reply for is in
the recovered keyspace"* — grounded in `persist_records` calling `flush_through(start_seq)` under
`Durability::Confirm`. But `Durability::Confirm` is selected by one predicate, and it is not the
durability mode:

- `frogdb-server/crates/core/src/shard/execution.rs:436` —
  `let rollback_mode = is_write && self.persistence.has_wal() && self.persistence.should_rollback();`
- `should_rollback()` (`frogdb-server/crates/core/src/shard/types.rs:426`) is only
  `policy == WalFailurePolicy::Rollback`.
- The `Confirm` call sites (`execution.rs:465`, `execution.rs:653`) are both inside
  `if rollback_mode`.
- The default path — `WriteEffectKind::WalPersistence` in
  `frogdb-server/crates/core/src/shard/post_execution.rs:401` — persists with
  `Durability::FireAndForget`, which per FM-PERSISTENCE-005's own forcing test
  (`fire_and_forget_never_flushes_and_continues_on_error`) never calls `flush_through`.

`persistence.wal-failure-policy` defaults to `continue` (FM-PERSISTENCE-005 calls it "the
default"). So with `durability-mode = sync` and the shipped default failure policy, the WAL entry
is only enqueued on the bounded channel when the reply is produced; the flush thread commits it
later (size threshold / batch timeout) with `sync = true`, after the client already has its `+OK`.
A power loss in that window loses an acknowledged write in the mode whose entire contract is that
it cannot.

None of FM-PERSISTENCE-002's forcing tests exercise the shard ack path: `test_sync_mode_zero_acked_write_loss`
assumes the property under test (a `PageCacheWal` model flushing after every write); the
`CrashTestHarness::put_direct`-based tests (`frogdb-server/crates/core/src/persistence/test_harness.rs:158`)
bypass the shard and `persist_records` entirely; `confirm_snapshots_sequence_then_flushes_once`
passes `Durability::Confirm` in by hand. The row is forced at the sink layer and asserted at the
client layer, with the layer that connects them untested.

## What to build

1. Make `Durability` selection a function of the durability mode *or* the failure policy, not the
   policy alone: `Confirm` iff `should_rollback() || durability_mode == Sync` (keep rollback-on-error
   gated on the policy as today).
2. Rewrite FM-PERSISTENCE-002's Invariant to name both conditions explicitly.
3. Add to FM-PERSISTENCE-002's NOT observable: "a `sync`-mode write acknowledged with only a
   `FireAndForget` stage behind it."
4. Add an end-to-end forcing test in `frogdb-core` that goes through `execute_command` with
   `durability-mode = sync` **and** `wal-failure-policy = continue`, asserts a `flush_through`
   reached the sink before the reply value was produced, and crashes the `PageCacheSink`
   immediately after the reply. This test must fail against today's code.
5. Until the fix lands, this scoping is provisional: FM-PERSISTENCE-002 is re-scoped in writing to
   `wal-failure-policy = rollback` only, and the `sync`+`continue` combination is documented as a
   loss window in the Redis deviations table — it is currently a silent, undocumented deviation
   from `appendfsync always`.

## Acceptance criteria

- [ ] `Durability::Confirm` selection changed to `should_rollback() || durability_mode == Sync`
- [ ] FM-PERSISTENCE-002's Invariant and NOT observable updated; `just lint-spec` green
- [ ] New end-to-end forcing test (durability=sync, wal-failure-policy=continue, crash-after-reply)
      added and shown to fail against the pre-fix code, then passes post-fix
- [ ] Redis deviations table updated if any scoping remains provisional at merge time
- [ ] `just mutants-diff frogdb-core` (and `frogdb-persistence` if touched) triaged on touched code

## Blocked by

None.

## Ruling (2026-08-13)

`Durability::Confirm` is selected iff `should_rollback() || durability_mode == Sync` — durability
level and failure policy are orthogonal knobs (Postgres/InnoDB/RocksDB shape). Rewrite
FM-PERSISTENCE-002's Invariant to name both conditions, add NOT-observable "a sync-mode write
acked with only FireAndForget persistence behind it". New end-to-end forcing test through
execute_command with durability=sync + wal-failure-policy=continue, crashing the sink after the
reply — must fail today. Until the fix lands, FM-002 is re-scoped to the rollback policy and the
sync+continue loss window is documented in the deviations table.
