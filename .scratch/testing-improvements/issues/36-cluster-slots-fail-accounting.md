# CLUSTER INFO slots_fail/slots_pfail are hardcoded to 0, slots_ok is unconditional

Status: needs-triage
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

- [ ] Mark a slot-owning primary as failed (setup exists at `integration_cluster.rs:9631`), then
      assert `CLUSTER INFO` reports `cluster_slots_fail > 0`.
- [ ] Same test asserts `cluster_slots_ok < cluster_slots_assigned` while the primary is failed.
- [ ] PFAIL (unconfirmed/single-observer failure) path covered separately, asserting
      `cluster_slots_pfail > 0` distinct from `cluster_slots_fail`.
- [ ] Recovery path (primary rejoins/is replaced) asserted to restore `slots_ok` to full count.

## Blocked by

None - can start immediately

## References

- `crates/commands/src/cluster/mod.rs:205-208,247,254-255,269`
- `server/tests/integration_cluster.rs:9631` (failed-primary setup to reuse)
- `.scratch/testing-improvements/audit/F-cluster.md` (`info-slots-fail-pfail-hardcoded`, F#2)
- `.scratch/testing-improvements/audit/verdicts-F.md`
