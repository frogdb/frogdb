# replication-cluster-rework — replication/cluster rework PRDs

State: active

Four rework PRDs (EXEC-slot revalidation, promotion replid/PSYNC, epoch-fold redesign,
WAIT in cluster mode), all implemented, adversarially reviewed, and merged 2026-07-30
(→ ebdf7d9e). Multiple CRITICALs were found and fixed pre-merge; two real bugs surfaced as
a bonus (dead auto-failover from an edge-triggered detector; fullsync checkpoint acked-write
loss from a missing WAL drain).

The PRDs are done. The follow-up issues they filed are not — 4 open, 0 closed.

## Layout

| path | what |
|---|---|
| `PRD.md` | umbrella brief |
| `exec-slot-revalidation.md`, `promotion-replid-psync.md`, `epoch-fold-redesign.md`, `wait-cluster-mode.md` | the four PRDs, implemented |
| `issues/open/` | 4 open follow-ups |

## Still open (4)

01 EXEC slot-table version fast path · 02 migration finalization pause barrier ·
03 Lua internal write validation · 04 WATCH slot validation

Issue **05** (CLUSTER admin-gating) is client-breaking — do not implement it without an
explicit decision.

## Inbound references

- `frogdb-server/crates/server/src/slot_migration/tests.rs`, `…/tests/integration_cluster.rs`
- `testing/jepsen/frogdb/src/jepsen/frogdb/slot_migration.clj`
