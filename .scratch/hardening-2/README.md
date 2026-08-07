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
copying them.
