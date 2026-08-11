# 06 — `DEBUG CHANGE-REPL-ID` + fullsync-storm nemesis

Status: needs-triage

## Parent

[PRD](../../PRD.md) W2.

## What to build

Redis-parity `DEBUG CHANGE-REPL-ID`: regenerate the primary's replication id in place,
invalidating every replica's partial-resync token at once. Research the Redis
implementation first (it exists there for exactly this test purpose) plus Valkey/DragonflyDB
variants. Nemesis on issue 01's plumbing; a fullsync-storm schedule fires it mid-load and
asserts the fleet reconverges through full resyncs.

Directly probes the replication-correctness issue-18 class (wire replication-id
validation): the regenerated id must go through the same validation as any adopted id.

## Acceptance criteria

- [ ] Command implemented with Redis-compatible name/semantics; deviations documented
- [ ] Locked-crate discipline (replication owns id minting; mutation gates; spec impact
      per D2)
- [ ] Fullsync-storm nemesis + schedule: all replicas observed re-syncing fully and
      reconverging; clean `:valid? true` store id cited
- [ ] Regenerated id provably passes the same validation path as wire-adopted ids (test
      cites issue 18's resolution once ruled; until then the test pins current behavior)

## Blocked by

- Issue 01 (`.scratch/jepsen-fault-injection/issues/`) — plumbing.
- PRD rulings D1/D2/D4 (interacts with replication-correctness issue 18's ruling).
