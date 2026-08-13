# A shard that drops out of the checkpoint quiesce does so silently

Status: done
Type: bug (observability)
Severity: likelihood 1/3 (needs a wedged, panicked, or shutting-down shard), consequence 3/3
(a checkpoint or full-sync artifact is missing acknowledged writes with no signal) — score 3
Area: server / checkpoint quiesce

## Problem

`quiesce_shards_for_checkpoint`
(`frogdb-server/crates/server/src/server/checkpoint_quiesce.rs`) is what makes a checkpoint contain
every write acknowledged before the cut (FM-PERSISTENCE-019). It fans out `FlushSearchIndexes` then
`FlushWal` to every shard and awaits the acks. Both halves discard their result:

```rust
let _ = sender.send(message(tx)).await;   // shard channel closed -> ignored
...
let _ = rx.await;                          // ack dropped -> ignored
```

The module doc states the intent — "a shard that cannot be reached (or that drops the ack) leaves
its writes uncaptured, which is exactly the pre-existing behaviour ... it must not abort the
checkpoint" — and not aborting is the right call. The problem is that it is also not *reported*.
There is no counter, no `WARN`, and no field in the resulting metadata. The outcomes are:

- **`BGSAVE` / periodic snapshot:** a recovery artifact silently missing one shard's recent writes.
  It still reports success, still advances `LASTSAVE`, still writes a `metadata.json` with the
  completion marker. Nothing distinguishes it from a good snapshot at restore time.
- **`FULLRESYNC` checkpoint for a replica:** worse, per the module's own reasoning. With no replica
  attached there is nothing in the backlog to replay, so a write missing from the checkpoint is
  missing from that replica *forever* — permanent, undetectable divergence from one dropped ack.

The failure is genuinely low-likelihood (a shard task must be wedged, panicked, or already shut
down), but it is exactly the case where a silent partial artifact is most damaging, and the
information is free: the two `let _ =` sites already know.

Current behavior pinned by FM-PERSISTENCE-020 in
`specs/persistence.md`, which carries
`Forced by | MISSING ([gap: ...])` pointing here.

## Candidate fixes

1. **Count and log.** Return the number of shards that failed to send or ack; `WARN` with the shard
   indices and bump a `checkpoint_quiesce_incomplete_shards_total` counter. Does not change
   behavior, makes the artifact's incompleteness visible after the fact. Smallest useful change.
2. **Record it in the artifact.** Carry the incomplete-shard set into `metadata.json` so a restore
   can refuse (or warn loudly about) a snapshot that was cut over a wedged shard. Follows from 1.
3. **Refuse for the full-sync path only.** Keep `BGSAVE` best-effort (an incomplete backup beats no
   backup) but fail the `FULLRESYNC` handshake when the quiesce is incomplete, so the replica
   retries instead of silently inheriting a hole. This is the one where "must not abort" does not
   obviously hold — the artifact is not a backup, it is the replica's entire dataset.

## Forcing test

Needs a shard sender whose task never acks (a dropped receiver or a parked task) plus a checkpoint
cut over it, which the current `checkpoint_quiesce` module has no seam for — it takes
`&[ShardSender]` and lives in the server crate. A test-only sender built from a channel whose
receiver is dropped would force the send-side branch of fix 1 cheaply once there is something to
assert.

## Resolution

Went past all three candidates: **an undrained shard now fails the checkpoint on both paths**, and
the issue's premise that "not aborting is the right call" is what got revisited.

The premise assumed a failed exchange might mean "this shard is slow". It cannot. `fan_out` has no
timeout, so a slow shard still blocks and is still waited for — unchanged. An exchange can only
fail when the shard task is *gone*: `send` errors on a closed channel, `rx.await` errors on a
responder dropped without answering. Both mean the shard is not running, and a checkpoint of a
database whose shards are not all running is not something to hand an operator or a replica. Redis
likewise reports a background save as failed rather than publishing a knowingly-partial RDB, and
snapshot retention means the previous known-good artifact stays the newest one on disk — so
"an incomplete backup beats no backup" (candidate 3's rationale for keeping BGSAVE best-effort)
does not apply: the alternative to the incomplete artifact is the *previous* artifact, not nothing.

```rust
pub(super) struct QuiesceIncomplete { stage: &'static str, shards: Vec<usize>, total: usize }

pub(super) async fn quiesce_shards_for_checkpoint(
    senders: &[ShardSender],
) -> Result<(), QuiesceIncomplete>
```

`fan_out` collects *every* failing shard rather than returning at the first, so one `ERROR` line
names the whole blast radius and the wave it happened in (`search-index flush` / `WAL drain`). It is
logged there, the only place that knows which shards; both callers just translate:

- **BGSAVE / periodic** — `PreSnapshotHook` became fallible
  (`Fn() -> Pin<Box<dyn Future<Output = Result<(), SnapshotError>> + Send>>`) and `SnapshotRun::execute`
  returns `Ok(Err(e))` on a failed hook, which routes through the existing `record_failure` from
  issue 03. So a wedged shard shows up as `rdb_last_bgsave_status:err` with the cause in
  `rdb_last_bgsave_error`, `rdb_bgsave_failures` incremented, `rdb_saves`/`LASTSAVE` unmoved, and
  `frogdb_persistence_errors{type="snapshot"}` — new `SnapshotError::PreSnapshot(String)` variant,
  no new metric needed.
- **FULLRESYNC** — `PreCheckpointHook` became `io::Result<()>`-returning and `replica_session`
  fails the handshake before any checkpoint is written, so the replica retries on its reconnect
  backoff instead of inheriting a permanent hole. Candidate 3's path, now the same rule as BGSAVE's.

Candidate 2 (recording the incomplete-shard set in `metadata.json`) is moot: no incomplete artifact
is produced, so there is nothing for a restore to detect.

Forcing tests, all tagged `FM-PERSISTENCE-020`: `quiesce_succeeds_when_every_shard_acks`,
`quiesce_fails_when_a_shard_channel_is_closed`, `quiesce_fails_when_a_shard_drops_the_ack`,
`quiesce_reports_every_undrained_shard` (unit, over fake shards built from `ShardSender::new` /
`ShardReceiver::new` — the seam the "Forcing test" section above said was missing),
`test_failed_pre_snapshot_hook_fails_the_save_and_cuts_nothing` (the stats + on-disk consequence
for BGSAVE), and `fullresync_fails_when_the_pre_checkpoint_drain_fails` (handshake fails, no
checkpoint dir, `sync_checkpoint_path` left unset).

Spec: FM-PERSISTENCE-020 retitled "a shard that cannot drain fails the checkpoint", rewritten from a
gap row to forced behavior, and cross-referenced from FM-PERSISTENCE-019.
