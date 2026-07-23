# Register linearizability workload has thin fault coverage; :all and nemesis-pause are dead/unreachable

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: jepsen

## Context

`register.clj` implements a Knossos-checked linearizable CAS-register workload (lines 161-165) — the harness's canonical single-key linearizability check. It currently only runs paired with `register/none` and `register/crash|kill` on a single node (`run.py`). It never runs against the replication or raft topologies, and never under clock, disk, network, memory, or pause faults — the extended-fault suite swaps register out for Elle specifically to avoid Knossos OOM at scale (`run.py:256-258`), leaving no linearizability check under any fault beyond crash/kill on a single node.

Two related dead spots compound the gap: the `:all` composed nemesis (`nemesis.clj:1251`) has no `TestDefinition` referencing it at all, and `nemesis-pause` is defined with `suites=()` in `run.py:143` — meaning it exists in the harness's option space but is unreachable through any suite grouping.

## What to build

- Add `register+pause` `TestDefinition`: register workload + pause nemesis (bounded time-limit to keep Knossos checking tractable).
- Add `register+partition` `TestDefinition`: register workload + partition nemesis (bounded time-limit, single-node or small replication topology to bound state space).
- Fix `nemesis-pause`'s `suites=()` in `run.py:143` so it's reachable via at least one suite, or remove it if genuinely unused.
- Either give `:all` (`nemesis.clj:1251`) a real `TestDefinition`, or delete the dead composed-nemesis definition if it's not meant to be used standalone.

## Acceptance criteria

- [ ] `register+pause` and `register+partition` `TestDefinition`s exist in `run.py`, bounded in time/state-space to avoid Knossos OOM.
- [ ] `nemesis-pause` is reachable through at least one suite grouping (fixed `suites=` list), or is removed along with dead references if not needed.
- [ ] `:all` composed nemesis (`nemesis.clj:1251`) either has a real `TestDefinition` or is deleted.
- [ ] New register+fault runs execute successfully (or with a triaged, understood failure) in a manual harness invocation before merging.

## Blocked by

None - can start immediately.

## References

- `testing/jepsen/frogdb/src/jepsen/frogdb/register.clj:161-165` (Knossos linearizable CAS-register check)
- `testing/jepsen/frogdb/run.py:256-258` (extended-fault suite swaps register for Elle citing Knossos OOM)
- `testing/jepsen/frogdb/run.py:143` (`nemesis-pause`, `suites=()` — unreachable)
- `testing/jepsen/frogdb/src/jepsen/frogdb/nemesis.clj:1251` (`:all` composed nemesis, no `TestDefinition`)
