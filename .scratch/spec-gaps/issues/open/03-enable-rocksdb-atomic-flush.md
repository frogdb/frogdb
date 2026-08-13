# Enable RocksDB atomic_flush across column families

Status: ready-for-agent

## Parent

[spec-review-persistence.md](../../../formal-spec/reviews/2026-08-13-antipattern/spec-review-persistence.md)
— Finding H3 ("FM-PERSISTENCE-034: point-in-time recovery over many column families without
atomic flush").

## What is wrong

FrogDB's database is a multi-column-family RocksDB instance: `shard_<n>` per shard, `warm_<n>`
when tiering is on, plus a search-meta CF. FM-PERSISTENCE-034 pins `DBRecoveryMode::PointInTime`
but the options builder (`frogdb-server/crates/persistence/src/rocks/mod.rs:188`) sets the
recovery mode and does **not** set `atomic_flush`. Without `atomic_flush`, each CF flushes its
memtable independently, so after a crash different CFs recover to different points; combined with
`kPointInTimeRecovery` (which stops replay at the first inconsistency), CF A can contain writes
from the same batch that CF B does not.

Two CFs hold one logical key in this codebase:

- A tiering demotion is (warm put, hot delete). Recover the warm put without the hot delete → both
  copies live; recover the hot delete without the warm put → the key is gone. FM-PERSISTENCE-045's
  `warm_keys_stale` term exists precisely because a warm entry can be shadowed by a hot copy — the
  invariant deciding which wins is a cross-CF invariant that non-atomic recovery can break.
- FM-PERSISTENCE-036 additionally deletes expired warm entries from disk at recovery time, so a
  divergent recovery is then made durable.
- The search-meta CF vs. the data CF is the same shape (FM-PERSISTENCE-019 drains search indexes
  and the WAL as two separate waves).

The rest of FM-PERSISTENCE-001's atomicity story is genuinely safe (a single-shard write group is
one `WriteBatch` into one CF) — this is specifically about the tier/index CFs.

## What to build

1. Set `db_opts.set_atomic_flush(true)` in `frogdb-server/crates/persistence/src/rocks/mod.rs`.
2. Pin it with a test in the style of `wal_recovery_mode_is_pinned_to_point_in_time`.
3. Extend FM-PERSISTENCE-034's Invariant to state the CF-consistency requirement, and its NOT
   observable to include "a recovered database in which the hot and warm column families of the
   same shard disagree about a key."
4. Add a forcing test that crashes between a warm put and its hot delete, and asserts the
   recovered database is consistent across the two CFs.
5. If `atomic_flush` is ever deliberately declined for write-throughput reasons in the future, that
   trade must be written down with the resurrection risk named — not the default posture here.

## Acceptance criteria

- [ ] `db_opts.set_atomic_flush(true)` set in the RocksDB options builder
- [ ] FM-PERSISTENCE-034 Invariant + NOT observable updated; `just lint-spec` green
- [ ] Forcing test: crash between warm-put and hot-delete, asserts no cross-CF divergence; fails
      before the fix, passes after
- [ ] Flush-latency cost noted/measured and accepted (no functional regression expected)
- [ ] `just mutants-diff frogdb-persistence` triaged on touched code

## Blocked by

None.

## Ruling (2026-08-13)

Set `db_opts.set_atomic_flush(true)` (multi-CF hot/warm tiers can otherwise recover at divergent
sequence points and resurrect deleted keys). Pin test in the style of
wal_recovery_mode_is_pinned_to_point_in_time; NOT-observable "cross-CF recovery divergence";
crash-between-hot-delete-and-warm-flush forcing test. Accepted small flush-latency cost.
