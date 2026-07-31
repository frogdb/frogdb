# No test ever restores a server from an actual BGSAVE snapshot artifact

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: persistence

## Context

No test in the suite ever recovers a server from a BGSAVE checkpoint via the operator-documented
restore path. `test_bgsave_snapshot_survives_restart`
(`server/tests/integration_persistence.rs:176-222`) performs `BGSAVE`, writes an additional key,
does a graceful shutdown, and then reopens the *same live directory* — this exercises ordinary WAL
replay, not snapshot loading. The `snapshot_NNNNN` checkpoint directory and its `latest` symlink
(`snapshot/stager.rs:60-66`) are never consulted by any automated path in the test suite: server
boot only ever installs a replica's `checkpoint_ready` staged directory or opens the live DB
directly (`operations/persistence.md:95-99`). The operator-facing "restore from a BGSAVE snapshot"
procedure has zero end-to-end automated coverage confirming it actually works.

Existing coverage is all at the mechanism level, not the end-to-end restore path:
`snapshot/tests.rs:251-486` tests staging/rename/retention mechanics; `rocks/tests.rs:419-654` tests
`load_staged_checkpoint` against hand-written, artificially small 1-key test databases, not a
snapshot produced by an actual `BGSAVE` run. The ops doc itself independently confirms the gap: "no
snapshot-load step at startup ... primary recovery never reads them."

A BGSAVE checkpoint is the operator's disaster-recovery artifact — Redis verifies RDB loadability on
restart; FrogDB has no equivalent verification that a BGSAVE snapshot is actually restorable.

## What to build

- Integration test: run `BGSAVE` against a populated server, then install the resulting
  `snapshot_NNNNN` checkpoint directory into a *fresh* data directory (following the documented
  operator restore procedure exactly), start a server against that fresh directory, and assert all
  keys and types survive intact.
- This test should cover the actual documented operator restore procedure end-to-end, not a
  synthetic hand-built checkpoint.

## Acceptance criteria

- [ ] Integration test performs `BGSAVE`, installs the real resulting snapshot artifact into a
      fresh directory (per the documented restore procedure), starts a server against it, and
      asserts data integrity across multiple key types
- [ ] Test fails if the documented restore procedure is broken (i.e. it exercises the real
      operator-facing steps, not a shortcut)

## Blocked by

None - can start immediately.

## References

- `server/tests/integration_persistence.rs:176-222` — `test_bgsave_snapshot_survives_restart`
  (WAL-replay path, not snapshot load)
- `snapshot/stager.rs:60-66` — checkpoint directory + `latest` symlink, never consulted by any
  automated path
- `operations/persistence.md:95-99` — documented boot behavior (no snapshot-load step)
- `snapshot/tests.rs:251-486` — staging/rename/retention mechanics only
- `rocks/tests.rs:419-654` — `load_staged_checkpoint` against hand-written 1-key DBs
- Source: `.scratch/testing-improvements/audit/D-persistence.md` gap #2, `.scratch/testing-improvements/audit/verdicts-D.md`
  #2 ("Ops doc independently confirms 'no snapshot-load step at startup ... primary recovery never
  reads them'")

## Resolution

Done 2026-07-23. Two end-to-end tests added in `server/tests/integration_persistence.rs`, and
building them surfaced a **real BGSAVE data-loss bug** in the product, now fixed.

### Restore path (as-built)

There is no snapshot-load-at-startup path; the operator restore procedure reuses the replica
full-sync staged-install machinery. The documented steps are: stop the server, copy the
`snapshot_NNNNN/checkpoint/` contents into a `checkpoint_ready` directory that is a *sibling* of the
data dir (`data_dir.parent()/checkpoint_ready`), and restart. On boot,
`RocksStore::load_staged_checkpoint` validates the staged dir carries a `CURRENT` manifest, moves any
existing data dir aside to `<db>_backup_<ts>`, atomically renames `checkpoint_ready` into place, then
opens it. Both new tests drive exactly this path against the *real* artifact produced by `BGSAVE`.

### Product bug found and fixed: BGSAVE snapshotted a RocksDB missing recently-acked writes

Under the default `periodic` durability mode a write is acknowledged to the client as soon as it is
staged in the per-shard WAL flush engine; it is committed to RocksDB only on a later
size/timeout/periodic-sync trigger. `BGSAVE`'s `pre_snapshot_hook` flushed search indexes and
persisted the replication offset but **never drained the shard WAL flush engines**, so
`create_checkpoint` captured a RocksDB that was missing the most-recently-acked writes — a silently
incomplete, lossy recovery artifact. Instrumentation confirmed it live: at snapshot time the shard
reported `seq=6, durable=4` (two acked writes still buffered), and the resulting checkpoint contained
zero SSTs and a 0-byte WAL.

Fix: a new `SearchMsg::FlushWal { response_tx }` shard message whose handler awaits
`wal_writer().flush_async()` (draining the flush engine into RocksDB). The `pre_snapshot_hook`
(`server/src/server/init.rs`) now fans this out to every shard and awaits all responses after the
search-index flush and before the checkpoint stages. Handled in the async event loop
(`core/src/shard/event_loop.rs`) because it must `.await`; the synchronous `dispatch_search` carries
an `unreachable!` arm for exhaustiveness.

### Test-determinism fix

`tokio::time::interval`'s first tick fires immediately, so the periodic snapshot task (spawned
whenever `snapshot_interval_secs > 0`, default 3600) writes an *empty startup snapshot* before any
data is written — which races and can be mistaken for the manual `BGSAVE` artifact. Added a
`snapshot_interval_secs: Option<u64>` override to `TestServerConfig`; the new tests set it to
`Some(0)` to disable the periodic task for a single deterministic `BGSAVE` artifact. This is a latent
trap for any future BGSAVE-artifact assertion, not just these tests.

### Tests added

- `test_bgsave_snapshot_restores_into_fresh_data_dir` — populate string/TTL/list/hash/set/zset,
  `BGSAVE`, restore the real artifact into a fresh dir via the documented procedure, assert every
  type survives and that a key written *after* the snapshot cut is correctly absent (proves the
  point-in-time image is exercised, not the live dir).
- `test_restored_snapshot_accepts_and_replays_new_writes` — restore from a `BGSAVE` artifact, confirm
  the base key, write a new key against the restored server, restart the restored dir, and assert both
  the checkpoint's data and the post-restore write replay from the WAL.

Both pass on the aarch64 Linux testbox. Full `frogdb-persistence` (164), `frogdb-core` (846), and
workspace clippy are clean.

### Files changed

- `server/tests/integration_persistence.rs` — two new e2e tests + helpers (`copy_dir_recursive`,
  `wait_for_snapshot_checkpoint`)
- `server/src/server/init.rs` — `pre_snapshot_hook` drains all shard WALs before checkpoint
- `core/src/shard/message.rs` — new `SearchMsg::FlushWal` variant (+ `probe_type_str`)
- `core/src/shard/event_loop.rs` — async `FlushWal` handler
- `core/src/shard/dispatch_search.rs` — exhaustiveness `unreachable!` arm
- `test-harness/src/server.rs` — `snapshot_interval_secs` override
