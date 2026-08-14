# 31: Divergence audit floor comes from the successor's offset, never a zero fallback

Status: ready-for-agent

## Origin

Distsys-review MAJ-1 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled accept-and-file by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

FM-REPLICATION-024's NOT-observable cell promises the split-brain discard audit floors
at "the acked offset, not `0`". The code floors at the minimum over *currently
streaming* sessions (`primary/mod.rs:838-849`
`let start = self.offsets.min_acked().unwrap_or(0);` → `offset_coordinator.rs:288-290`
→ `tracker.rs:398-403`). In a network partition — the only realistic split-brain
trigger — the diverged primary has no streaming replicas, `min_acked()` is `None`, and
the floor silently becomes 0. A freshly re-attached replica gives the same answer
(`acked_offset` = 0; `min_acked_offset_ignores_a_resume_seed`, `tracker.rs:827+`,
deliberately pins `Some(0)`). Worse, the row's own `Forced by` list names
`divergence_record_no_streaming_replicas_uses_zero_floor` — a test that pins exactly
the behavior the row forbids.

Consequences: the audit lists every backlog write as discarded, including writes the
new primary already holds — `ops_discarded` inflated by orders of magnitude
(observability-accuracy principle: misleading data is not ok). And since the audit is
meant to be replayable, replaying it re-applies non-idempotent writes (`INCR`, `LPUSH`,
`APPEND`) the successor already applied — a diagnostic turned corruption tool.

Mature-system shape: the correct floor is the successor's match index — what CRDB and
any Raft implementation use to decide which suffix of a deposed leader's log is
genuinely uncommitted. FrogDB already carries it: the `DemotionEvent` names the
successor's acked offset.

## What to build (spec-first)

1. Amend FM-REPLICATION-024: the floor is the `DemotionEvent`'s successor offset; when
   that is unavailable, the audit is emitted tagged `unknown-floor` — never a silent 0
   substitute. State the replay hazard the floor prevents.
2. Code: thread the successor offset from the `DemotionEvent` into the audit's start
   offset; add the `unknown-floor` variant to the audit record (and its rendering in
   whatever surfaces it — metrics/logs/introspection).
3. Invert `divergence_record_no_streaming_replicas_uses_zero_floor`: no streaming
   replicas + known successor offset → floor = successor offset; no successor offset →
   `unknown-floor`, never 0. Rewrite `min_acked_offset_ignores_a_resume_seed`'s role in
   the chain or scope it away from the audit path.
4. Forcing test: partition-shaped scenario (no streaming sessions at demotion) asserts
   the audit floors at the successor offset from the `DemotionEvent` and that writes at
   or below it never appear as discarded.

## Acceptance criteria

- [ ] FM-REPLICATION-024 amended; `just lint-spec` green
- [ ] Floor sourced from `DemotionEvent` successor offset; `unwrap_or(0)` gone from the
      audit path
- [ ] `unknown-floor` variant exists and is emitted when the successor offset is absent
- [ ] Zero-floor pinning test inverted; forcing test fails pre-fix, passes post-fix
- [ ] `just mutants-diff` on frogdb-replication (locked, gate 0.85) triaged

## Blocked by

None — can start immediately.
