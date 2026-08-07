# hardening-2 — second hardening campaign (detection-first)

State: active

Working directory for the follow-on to the [foundation-hardening
campaign](../hardening/README.md). Campaign 1 locked four areas by *extraction + failure-mode
spec + mutation gate*. The round-2 backlog re-triage
([`../testing-improvements-round2/re-triage-2026-08-06.md`](../testing-improvements-round2/re-triage-2026-08-06.md))
then found 18 live production defects that machinery structurally could not have found — including
two inside the locked perimeter.

Campaign 2 is therefore **detection-first**: build the mechanisms that would have caught those
defects before a human audit did, then run the backlog through them.

| path | what |
|---|---|
| `PRD.md` | the plan — evidence, workstreams, sequencing, exit criteria |
| `issues/open/`, `issues/done/` | campaign-2 issues per the [issue-tracker](../../docs/agents/issue-tracker.md) conventions |

The live-defect backlog itself stays where it was filed
(`../testing-improvements-round2/issues/open/`); campaign 2 references those numbers rather than
copying them. The PRD writes campaign-2 issues as `c2-NN` where both are cited together.

## Issues found while drafting the plan

The three planning surveys (durable-write extraction, chokepoint-lint candidates, witness quality)
each turned up defects nobody had filed:

| # | what | severity |
|---|---|--:|
| 01 | `save_vote` flushes the wrong column family — raft vote durability | 9 |
| 02 | the ACL file is never read at boot; every `ACL SETUSER` is lost on restart | 9 |
| 03 | six hand-rolled durable writers at four correctness levels | 6 |
| 04 | `durability_mode` is parsed twice, independently | 3 |
| 05 | `ScriptingMsg::FunctionCall` bypasses the VLL continuation gate | 6 |
| 06 | the 11 seam-lint gates run in neither CI nor agent commits | 9 |
| 07 | no panic isolation at the shard boundary | 6 |
| 08 | `CrashTestHarness` does not crash, and its durability modes are decorative | 9 |
| 09 | two FM rows cite tests that do not witness them | 6 |
| 10 | the client-visible scatter error replies have no tests | 4 |
