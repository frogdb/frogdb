# A shard that drops out of the checkpoint quiesce does so silently

Status: open
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
`.scratch/hardening/specs/persistence-failure-modes.md`, which carries
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
