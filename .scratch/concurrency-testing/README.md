# concurrency-testing — concurrency invariant testing

State: active

Phases 1–5 (incl. 4a/4b/4c) merged 2026-07-22; nightly CI live. Four issues remain open,
including one **real, un-root-caused bug**.

## Layout

| path | what |
|---|---|
| `proposals/` | design proposals |
| `issues/open/` | 4 open |
| `issues/done/` | 7 closed |

## Still open (4)

| # | issue | note |
|---|---|---|
| 05 | VLL phase-3 partial-commit decision | open design question, referenced from shipped docs |
| 06 | durability txn framing abort-on-recovery | the fix deferred by 05 |
| 10 | notify-capture broader scenario rollout | |
| 11 | nightly smoke findings | **real bug** — MultiWaiter data loss ≳90 ops (Finding A). Its `## Resolution shipped in phase 5 (CI wiring)` heading resolves only the CI sub-part; findings A/B/C are still live. Do not read that heading as "closed" |

## Inbound references

- `Justfile:110`, `workflow_gen/…/concurrency_nightly.py`
- `frogdb-server/crates/server/tests/concurrency_workload.rs`, `…/integration_persistence.rs`
- `docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md`,
  `docs/superpowers/plans/2026-07-21-lazy-expiry-parity-fix.md`
- `.superpowers/sdd/failstop/report.md`, `.superpowers/sdd/lazy-expiry/progress.md`
- `website/src/content/docs/architecture/vll.md`
