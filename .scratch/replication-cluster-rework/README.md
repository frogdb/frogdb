# replication-cluster-rework — replication/cluster rework PRDs

State: active

Four rework PRDs (EXEC-slot revalidation, promotion replid/PSYNC, epoch-fold redesign,
WAIT in cluster mode), all implemented, adversarially reviewed, and merged 2026-07-30
(→ ebdf7d9e). Multiple CRITICALs were found and fixed pre-merge; two real bugs surfaced as
a bonus (dead auto-failover from an edge-triggered detector; fullsync checkpoint acked-write
loss from a missing WAL drain).

The PRDs are done. The follow-up issues they filed are mostly closed — 2 open, 9 closed.

## Layout

| path | what |
|---|---|
| `PRD.md` | umbrella brief |
| `exec-slot-revalidation.md`, `promotion-replid-psync.md`, `epoch-fold-redesign.md`, `wait-cluster-mode.md` | the four PRDs, implemented |
| `issues/open/` | 2 open follow-ups |
| `issues/done/` | 9 closed follow-ups |

## Still open (2)

02 migration finalization pause barrier · 03 Lua internal write validation

## Inbound references

- `frogdb-server/crates/server/src/slot_migration/tests.rs`, `…/tests/integration_cluster.rs`
- `testing/jepsen/frogdb/src/jepsen/frogdb/slot_migration.clj`
