# Define and test cluster-mode keyspace notification emitter + SCAN per-node semantics

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 1/3, consequence 1/3 (score 1)
Area: Cluster / Pub/Sub

## Context

Zero cluster-mode tests exist for keyspace notifications (grep `keyspace|keyevent` in
`integration_cluster.rs` → 0 hits) or for `SCAN` in cluster mode (grep `"SCAN"` in
`integration_cluster.rs` → 0 hits). Two related but distinct semantics are undocumented and
unverified:

- **Which node emits a keyspace notification event** for a given key mutation in a multi-node
  cluster — only the slot-owning primary? all primaries? replicas too? Nothing pins this today.
- **SCAN's per-node cursor semantics** in cluster mode — does `SCAN` issued against one node only
  iterate that node's locally-owned slots, requiring a client to fan out `SCAN` to every primary and
  union the results for a full-keyspace scan? Is the union guaranteed free of duplicates and free of
  cross-node "bleed" (a key appearing via a non-owning node's scan)?

Verdict CONFIRMED L1/C1.

## What to build

1. Define and document the intended contract: keyspace notifications are emitted only by the
   slot-owning primary (not replicas, not non-owning primaries); `SCAN` on a given node iterates only
   that node's locally-owned keys, and a full-keyspace scan requires client-side fan-out across every
   primary with result union.
2. Integration test: subscribe to `__keyevent@0__:set` (or equivalent) on every node in a multi-node
   cluster; write a key; assert the event fires exactly once, and only via the owning primary's
   subscription channel.
3. Integration test: write a known key set spread across primaries by hash slot; `SCAN` each primary
   to completion; assert the union of results equals the full written keyspace with no duplicates and
   no cross-node bleed.

## Acceptance criteria

- [ ] Contract documented (code comment and/or docs) for which node emits keyspace notifications in
      cluster mode.
- [ ] Integration test: a keyspace event for a key mutation is observed exactly once, only via the
      owning primary's subscription channel, and not observed via other primaries' channels.
- [ ] Contract documented for `SCAN` per-node semantics in cluster mode (local-slots-only; full
      coverage requires client-side fan-out across primaries).
- [ ] Integration test: `SCAN`-to-completion on each primary in a multi-primary cluster; union of
      results equals the full written keyspace, with no duplicates and no cross-node bleed.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/F-cluster.md #11 (`keyspace-notifications-and-scan-in-cluster-undefined`)
- .scratch/testing-improvements/audit/verdicts-F.md #11 (CONFIRMED L1/C1)
- frogdb-server/crates/server/tests/integration_cluster.rs (grep `keyspace`/`keyevent`/`SCAN` → 0 hits)
