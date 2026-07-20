# 10 — Fix CONTEXT.md Snapshot/COW Buffer entries to match the forkless-checkpoint implementation

Status: needs-triage

## What to build

`frogdb-server/CONTEXT.md` describes Snapshots via an in-memory **COW Buffer** "holding
pre-snapshot values for keys written during a snapshot; counted toward `maxmemory`", and the
Relationships section says a Snapshot "diverts concurrent writes to the COW Buffer". The
implementation has no such buffer: Snapshots are forkless via a RocksDB checkpoint at a
sequence number plus `pre_snapshot_hook`; nothing snapshot-related is counted toward
`maxmemory`.

Doc-only change: rewrite the **COW Buffer** and **Snapshot**/**Snapshot Epoch** glossary
entries and the Relationships bullet to describe the checkpoint-at-sequence mechanism (either
retire the COW Buffer term or redefine it to what actually exists). Per repo doc rules, sync
the website glossary (`website/src/content/docs/architecture/glossary.md`) and fix any links —
follow `/doc-sync` conventions.

## Acceptance criteria

- [ ] CONTEXT.md Snapshot/Snapshot Epoch/COW Buffer entries match the implementation (reviewed against `persistence/src/snapshot/`)
- [ ] Relationships bullet corrected
- [ ] Website glossary synced; no dangling links (run link check if available)
- [ ] No implementation changes

## Blocked by

None - can start immediately

## Source

Round-8 P06 agent report; discrepancy verified against `rocks_coordinator.rs` during proposal 06.
