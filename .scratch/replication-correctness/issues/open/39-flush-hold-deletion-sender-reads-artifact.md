# 39: The flush hold dies — the sender reads Y_s out of the cut artifact

Status: ready-for-agent

Sequenced after issue 37 (the dependency below).

Parent: [PRD 36](./36-offset-stamped-batches-restart-bias.md), ruling R21 (campaign
ledger 2026-08-22). Depends on [37](./37-mint-at-persist-and-primary-stamps.md).

## Scope

1. **Sender source swap.** On the checkpoint full-sync path, `Y_s` comes from the cut
   artifact's per-shard stamp keys (open the checkpoint's CFs, per-CF get) instead of the
   drain-ack capture. The artifact self-describes its coverage: any write that slips past
   the drain carries its own stamp, so payload and claim cannot disagree — the property
   the hold existed to enforce, now held by construction.
2. **Delete the hold machinery.** `FlushHold`/`FULL_SYNC_HOLD` (`wal/hold.rs`),
   `CaptureHold`/`FullSyncHoldGuard` (`checkpoint_quiesce.rs`),
   `SessionEvent::CoverageBreached` + `SyncFailure::CoverageHoldBreached` and the
   session-machine abort arm, and the hold-breach counter (interim observability landed
   2026-08-22 — it counts a machinery that no longer exists). The drain itself stays: it
   bounds payload staleness and remains the WAL-completeness barrier.
3. **Sync-durability ack stall disappears.** The hold briefly held Sync-durability acks
   between drain and cut (documented bound, up to 10s); with no hold there is nothing to
   document — remove the bound note from the spec.
4. **Live-dataset path unchanged** — export-message capture stays (no artifact there);
   issue 38 owns how its trailer values become replica stamps.

## Spec / model

- FM-REPLICATION-066 rewritten: capture-at-drain-ack + hold + breach-abort clauses
  replaced by artifact-sourced coverage; the breach forcing tests deleted with the
  machinery; new forcing test — a write landing between drain-ack and cut appears in
  *both* the artifact and its stamp (the exact shape the hold guarded, now proven
  harmless).
- Model: `witnessTornPayloadCut` machinery re-read — the torn cut is still reachable and
  still mended by floors (unchanged); hold-related battery rows (if 37/38 added any)
  removed.

## Acceptance

- [ ] Sender reads per-shard stamps from the artifact; trailer populated from them;
      forcing test: drain→cut straggler write is covered (in artifact + in `Y_s`, replica
      floor skips its frame exactly)
- [ ] Hold machinery + breach counter fully deleted; no orphaned config/metrics/docs
- [ ] Spec rows updated, `just lint-spec` green; `just mutants-diff frogdb-replication` +
      `frogdb-persistence` before push
