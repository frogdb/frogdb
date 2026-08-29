# 36: PRD — log-first offsets: stamps, rotation, and the death of the flush hold

Status: ready-for-agent

PRD umbrella — rulings settled 2026-08-22; work lands via issues 37, 38, 24, 39.
Close when those land.

Ruled 2026-08-22, campaign ledger R17–R24
(`.scratch/formal-spec/2026-08-19-quint-completeness-campaign.md`). This document is the
design record; it owns no implementation checkboxes. Sub-issues:

| Issue | Owns | Depends on |
|---|---|---|
| [37](./37-mint-at-persist-and-primary-stamps.md) | mint+enqueue at persist, count-always, primary-side per-shard stamps | — |
| [38](./38-replica-stamps-floors-unification.md) | replica per-frame stamps, floors-from-stamps, R15/R16 retirement | 37 |
| [24](./24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md) | replid rotate-unless-clean, clean-shutdown marker, state-file demotion | 37 |
| [39](./39-flush-hold-deletion-sender-reads-artifact.md) | FlushHold deletion, sender reads `Y_s` from the cut artifact | 37 |

## The problem (why the original sketch died)

The original issue assumed the offset could be written back into the staged WAL entry
post-mint ("single-threaded shard task makes the write-back trivial"). Investigation
(2026-08-22) found three structural facts against it:

1. **The entry is gone by mint time.** WAL staging (effect 6 of `WRITE_EFFECT_ORDER`,
   `post_execution.rs:282-292`) serializes and `send_async`s a fully-owned entry to the
   dedicated flush thread (`writer.rs:110-135`). The mint is effect 8
   (`offset_coordinator.rs:108`). Nothing remains on the shard task to write into.
2. **Sync durability commits before the offset exists.** Under `should_confirm`
   (`types.rs:515-517`) the persist runs `Committed` *before any write effect*
   (`execution.rs:463-496`). The batch is durable before the mint. A same-batch stamp is
   structurally impossible without reordering.
3. **Standalone primaries mint nothing.** `is_active()` =
   `replica_count() > 0 || has_resume_history()` (`primary/mod.rs:1023`). A
   never-had-a-replica node writes data and advances no offset, so stamps derived from the
   broadcast offset would be absent exactly where recovery reads them.

And a fourth, found walking the restart branch: exact stamps alone cannot make restart
safe. A rebooted primary keeps its replid at a rewound `offset_at_save`
(`recovery/replication.rs:27-75` — no rotation anywhere on the boot path) with an empty
backlog; the broadcast-but-unflushed tail under relaxed durability is unknowable at
recovery and its offsets were already shipped. Same-replid offset reuse ⇒ a later
`+CONTINUE` serves different-history bytes at overlapping offsets. This is issue 24's
finding; stamps sharpen it but do not close it.

## The design (rulings R17–R24)

**R18 — log-first mint (the CRDB applied-index pattern).** Mint and backlog-enqueue stay
fused (one critical section = mint order is wire order) and both move to the persist
point, before staging. The per-shard "max offset flushed" stamp is added to the same
`WriteBatch` as the data (`RocksSink::commit` seam, `flush.rs:181-220`). Under
`should_confirm` the pre-effect persist carries the stamp — the client ack covers
data+stamp atomically, zero added latency. Effect 8 shrinks to bookkeeping. This is
CockroachDB's `RaftAppliedState`-in-the-apply-batch and FDB's sequencer-before-durability,
transliterated: position assigned before application, position rides the batch. Accepted
consequence: replicas can receive a frame before primary-local effects (notifications,
waiters) run — same exposure direction as today, earlier arrival.

**R19 — count always, enqueue when active.** The counter advances for every replicable
write on every node; suppressed/`NO_PROPAGATE` forms advance by 0 (outside the stream
claim — their effects ship only via payloads). Backlog append and socket feed stay gated
on `is_active()`, so standalone pays no memory. Stamps are exact everywhere; a late
replica attach is consistent by construction (payload carries everything up to the
counter).

**R20 — rotate unless clean (refines issue 24's ruling with the mechanism).** Unclean
boot: loaded id → `secondary_id` bounded at the recovered head (max over stamps), fresh
primary id minted. Clean shutdown (drained + flushed + marker proving head == stamps)
keeps the identity, so rolling restarts don't force fleet-wide resyncs. Rotation is what
makes the unknowable shipped-tail harmless; the restarted primary's backlog is empty so it
could never serve `+CONTINUE` anyway.

**R21 — the FlushHold dies (issue 39).** The cut artifact self-describes coverage: the
sender reads each shard's stamp out of the checkpoint's CFs as `Y_s`. A write slipping
past the drain carries its own stamp — artifact and claim cannot disagree. Hold,
breach-abort, and the interim breach counter all delete. Drain stays (payload staleness
bound). Live-dataset path keeps issue-35's export-message capture.

**R22 — floors unify into stamps (issue 38).** An installed checkpoint *contains* the
primary's stamps: install adopts the floors automatically, atomic with the data. Replica
per-frame stamping (offset known before apply; the persist seam takes the offset —
primary mints, replica supplies) keeps them current. Staged-metadata coverage vector and
`coverage_at_save` delete; the R16 unconditional refusal retires (a recovered stint
reconstructs exact floors and head). The trailer `ShardCoverage` **stays** as the wire
form — the live path has no artifact; install writes trailer values as stamps.

**R23 — stamp key in a reserved per-shard metadata CF.** WriteBatch atomicity spans CFs;
the user keyspace stays clean (no SCAN/DBSIZE filtering hazard). `search_meta_<n>`
reserved-prefix precedent.

**R24 — model restart properly.** The fullsync model's `applyRestart` gap opens up:
restart transitions (lose unflushed tail, recover stamps, rotate-unless-clean), invariants
for no-offset-reuse-within-a-history and claim == coverage after recovery. Battery rows
per the documented-battery mandate.

## Cross-references

- Issue 24 carries the identity half (ruled 2026-08-13, amended for mechanism 2026-08-22)
  and still lands **before** issue 17.
- Issue 30 (state-file atomic/durable/single-writer) is partially mooted by 24's R4
  state-file demotion — re-triage after 24 lands.
- Issue 17 (save-point above live head) and 32 (FM-019 offset-rewind reconciliation)
  re-read against the stamp source of truth once 37/38 land.
- `replication.md:360` ("persisted offset commits atomically with the write it names")
  flips to implemented in issue 37's change set.
- `wal_watermark.rs` records RocksDB *sequence numbers* (FM-PERSISTENCE-035) — a different
  quantity. Untouched by this design; do not conflate.
