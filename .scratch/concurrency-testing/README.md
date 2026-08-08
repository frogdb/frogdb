# concurrency-testing — concurrency invariant testing

State: active

Phases 1–5 (incl. 4a/4b/4c) merged 2026-07-22; nightly CI live. Four issues remain open,
including a harness defect that makes every seed-indexed finding a sample rather than a
fact (14).

## Layout

| path | what |
|---|---|
| `proposals/` | design proposals |
| `audit/` | determinism audit backing issue 14 |
| `issues/open/` | 4 open |
| `issues/done/` | closed issues; unreferenced ones pruned (git history) |

## Still open (4)

| # | issue | note |
|---|---|---|
| 05 | VLL phase-3 partial-commit decision | open design question, referenced from shipped docs |
| 06 | durability txn framing abort-on-recovery | the fix deferred by 05 |
| 10 | notify-capture broader scenario rollout | |
| 14 | workload harness not reproducible | same seed → different history and different verdict; seed pins, repro files and nightly triage all rest on a contract that does not hold |

Issue 11 (the nightly smoke findings) is closed: Finding A was the harness drain race, Finding C the
ZSet model artifact, and Finding B split into 12 (product bug, fixed), 13 and 16 (checker
defects, fixed).

## Inbound references

- `Justfile:110`, `workflow_gen/…/concurrency_nightly.py`
- `frogdb-server/crates/server/tests/concurrency_workload.rs`, `…/integration_persistence.rs`
- `website/src/content/docs/architecture/vll.md`
