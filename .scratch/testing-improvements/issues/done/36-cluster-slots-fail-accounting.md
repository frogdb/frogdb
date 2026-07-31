# CLUSTER INFO slots_fail/slots_pfail are hardcoded to 0, slots_ok is unconditional

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: cluster (area F)

## Context

`cluster_slots_pfail` and `cluster_slots_fail` are hardcoded to the literal `0`
(`commands/cluster/mod.rs:254-255`), and `cluster_slots_ok` is computed unconditionally as
`slots_assigned` (`mod.rs:269`, marked "for now" in the source). This means a slot owned by a
primary that FrogDB itself has already flagged as `FAIL` (the `cluster_state` computation at
`mod.rs:205-208` does correctly detect this for the overall cluster-state field) still gets
counted as `slots_ok` in the per-field breakdown. Tooling and operators that key off
`slots_fail`/`slots_pfail`/`slots_ok` — the standard Redis Cluster health signals — get an
incorrect picture even though the coarser `cluster_state` field is accurate. There is zero test
coverage (`grep slots_fail` finds nothing).

Verdict (adversarial pass): CONFIRMED L2/C2, noting the impact is partially mitigated because
`cluster_state:fail` does still surface correctly — this is specifically about the granular
`slots_*` accounting fields being wrong, not total loss of failure visibility.

## What to build

Wire `cluster_slots_fail`/`cluster_slots_pfail` to real per-slot FAIL/PFAIL state, and make
`cluster_slots_ok` exclude slots owned by a FAIL-flagged node. Add tests using the existing
mark-primary-failed setup.

## Acceptance criteria

- [x] Mark a slot-owning primary as failed, then assert `CLUSTER INFO` reports
      `cluster_slots_fail > 0`.
- [x] Same test asserts `cluster_slots_ok < cluster_slots_assigned` while the primary is failed.
- [x] PFAIL (unconfirmed/single-observer failure) path covered separately, asserting
      `cluster_slots_pfail > 0` distinct from `cluster_slots_fail`.
- [x] Recovery path (primary rejoins/is replaced) asserted to restore `slots_ok` to full count.

## Resolution

Implemented. `cluster_slots_{ok,pfail,fail}` are now derived per slot from the owning node's
flags instead of being literals.

**Implementation** (`server/src/commands/cluster/mod.rs`): new `SlotHealthCounts` +
`count_slot_health(&ClusterSnapshot)` walks `slot_assignment` and buckets every assigned slot
exactly once by its owner's flags — `fail` first, then `pfail`, else `ok` (same precedence as
`wire::node_health`). `cluster_info` renders the three counts from it. Invariant:
`ok + pfail + fail == slots_assigned` always; a slot whose owner is missing from `nodes`
(should not happen) is counted `ok` rather than dropped, so nothing can silently vanish from the
total.

**`cluster_state` unchanged** — it already reported `fail` whenever any primary carried the FAIL
flag (or local quorum was lost). This issue was only about the granular breakdown disagreeing
with that coarser field; they now agree.

**PFAIL: no producer exists — documented rather than faked.** FrogDB has no gossip suspicion
phase. Failure detection is the Raft leader's single-observer TCP probe, which commits
`MarkNodeFailed` (`flags.fail`) directly; `flags.pfail` is only ever *cleared*
(`cluster/src/commands.rs`, `MarkNodeRecovered`) and never set by any production path. So
`cluster_slots_pfail` still reports 0 in practice — but it is now a *derived* 0 (accurate: no
node is ever PFAIL) rather than a hardcoded one, and it starts reporting real counts for free if
a suspicion phase ever lands. The bucketing is pinned by unit tests since it is unreachable
end-to-end. This is recorded on the `SlotHealthCounts` doc comment.

**Tests**
- Unit, `server/src/commands/cluster/mod.rs` (`mod tests`) — 8 tests on `count_slot_health`:
  all-ok baseline; FAIL owner counts `fail` not `ok`; PFAIL owner counts distinct from FAIL;
  FAIL takes precedence over PFAIL (no double counting); recovery restores full `ok`; a flagged
  owner does not taint another primary's slots; `ok+pfail+fail == slots_assigned` under mixed
  flags; unknown owner counted, not dropped.
- Integration, `server/tests/integration_cluster.rs` —
  `test_cluster_info_slot_health_breakdown_healthy_cluster` pins the healthy end at the wire
  level on all 3 nodes (the sum invariant plus `ok == assigned`, both failure buckets 0).
- Turmoil, `server/tests/simulation.rs` — assertions 5 and 6 added to
  `run_cluster_asymmetric_partition_false_failover` (issue 18's FAIL-flag lifecycle sim, seeds
  1/2/3): while the slot-owning victim is FAIL-flagged, `cluster_slots_fail > 0` and
  `cluster_slots_ok < cluster_slots_assigned` observed from a third node; after heal/recovery
  `cluster_slots_ok` returns to the full assigned count and `cluster_slots_fail` to 0. New
  helper `cluster_info_field` parses a named `CLUSTER INFO` field. This is the only reachable
  end-to-end FAIL path — there is no client or admin command to mark a node failed.

**Note:** the issue's `integration_cluster.rs:9631` reference was stale (that line is an
admin upgrade-status test). The real failed-primary setups are
`test_mark_node_failed_cluster_state_degrades` and the turmoil sim above.

**Drive-by fix:** `integration_cluster.rs` did not parse on `main` — commit a3bbb204 (issue 55)
clobbered the tail of `test_cluster_scan_is_per_node_and_unions_to_full_keyspace` (issue 58),
eating its `harness.shutdown_all().await; }` and the following banner's opening rule, leaving an
unclosed delimiter that broke `cargo fmt` and the whole test binary. Restored.

## Blocked by

None - can start immediately

## References

- `crates/commands/src/cluster/mod.rs:205-208,247,254-255,269`
- `server/tests/integration_cluster.rs:9631` (failed-primary setup to reuse)
- `.scratch/testing-improvements/audit/F-cluster.md` (`info-slots-fail-pfail-hardcoded`, F#2)
- `.scratch/testing-improvements/audit/verdicts-F.md`
