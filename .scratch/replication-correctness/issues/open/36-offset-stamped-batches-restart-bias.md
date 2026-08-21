# 36: Offset-stamped batches (S) — the artifact self-describes its coverage; restart bias closed

Status: needs-triage

Sequenced after issue 35.

Ruled 2026-08-21, campaign ledger R13/R16
(`.scratch/formal-spec/2026-08-19-quint-completeness-campaign.md`): the follow-up half
of the V-now/S-later ruling. Owns the `replication.md:360` gap — "the persisted offset
commits atomically with the write it names (not stamped at manifest time, which biases
it low and makes replay non-idempotent for INCR/LPUSH/APPEND)" — which is the *restart*
flavor of issue 35's root: keyspace coverage runs ahead of the claimed offset.

## Shape

Stamp each shard's RocksDB write batch with a per-shard `max offset flushed` key
(**per-shard keys are mandatory** — a single global key overclaims whenever another
shard's flush lags, which turns into holes on replay). Then:

- **Recovery** reads exact coverage out of RocksDB instead of the low-biased state-file
  offset — the same-history restart re-execution gap (issue 35's R16 residue) closes.
- **The checkpoint sender** reads `Y_s` out of the cut artifact instead of capturing at
  drain-ack, and the drain-to-cut flush hold can go. This is a *source swap* under
  issue 35's unchanged wire format and replica logic — S is not an alternative to V
  (the live-dataset path has no RocksDB and keeps V's export-message capture).
- **Issue 35's R16 interim rule** (floorless recovered stints refuse window grants) is
  retired: a recovered stint reconstructs its floors from the stamps.

## Design questions to settle before implementation

- The offset is minted at `ReplicationBroadcast`, the **last** write effect
  (`post_execution.rs:282-292`), after WAL staging (effect 6). Stamping therefore
  attaches the offset to the staged entry post-mint (single-threaded shard task makes
  the write-back trivial) — but under Sync durability the flush-engine commit is
  in-line (`wal/flush.rs:196-217`) and its ordering against the broadcast must be
  pinned. Pre-minting instead creates failure-gap problems in the stream (a failed
  write would consume an offset) — avoid.
- Interaction with FM-PERSISTENCE-035/039 and the existing RocksDB *sequence-number*
  watermark side-file (`wal_watermark.rs` — a different quantity; not in the cut, not
  fsynced, not atomic with batches). Do not conflate them.
- Cross-shard groups: per-shard stamps must reflect the same per-CF halves the batches
  actually carry (the issue-35 torn-checkpoint mend argument depends on it).

## Acceptance (sketch — refine at triage)

- [ ] Per-shard offset stamp committed atomically with each batch; forcing test proves
      a crash after flush recovers claim == coverage (non-idempotent write replays
      exactly once across restart)
- [ ] Checkpoint sender sources `Y_s` from the artifact; drain-to-cut flush hold
      removed; issue-35 wire format and replica logic unchanged
- [ ] Issue-35 R16 interim refusal retired; recovered stints reconstruct floors
- [ ] `replication.md:360` clause flips to implemented; persistence spec rows updated;
      `just lint-spec` green; `just mutants-diff` on touched locked crates
