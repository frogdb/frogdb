# concurrency-testing — concurrency invariant testing

State: active

Phases 1–5 (incl. 4a/4b/4c) merged 2026-07-22; nightly CI live. Eight issues remain open,
including one **real product bug** (12) and a harness defect that makes every seed-indexed
finding a sample rather than a fact (14).

## Layout

| path | what |
|---|---|
| `proposals/` | design proposals |
| `issues/open/` | 8 open |
| `issues/done/` | 8 closed |

## Still open (8)

| # | issue | note |
|---|---|---|
| 05 | VLL phase-3 partial-commit decision | open design question, referenced from shipped docs |
| 06 | durability txn framing abort-on-recovery | the fix deferred by 05 |
| 10 | notify-capture broader scenario rollout | |
| 12 | re-WATCH resets watch snapshot | **real product bug** — second `WATCH k` overwrites the first snapshot, EXEC commits over an interfering write |
| 13 | WATCH checker counts a no-op DEL as a write | checker false positive — breaks the checker's no-false-positive claim |
| 14 | workload harness not reproducible | same seed → different history and different verdict; seed pins, repro files and nightly triage all rest on a contract that does not hold |
| 15 | ZSet model BZPOPMAX tie rule | model artifact — source of every `(ZSet) not linearizable` report |
| 16 | exact FIFO checker degrades to an unsound proxy | checker false positive — registration ordinals are almost never captured, so wake order is judged by record-arrival order |

Issue 11 (the nightly smoke findings) is closed: Finding A was the harness drain race, Finding C the
ZSet model artifact, and Finding B split into 12 (product), 13 and 16 (checker).

## Inbound references

- `Justfile:110`, `workflow_gen/…/concurrency_nightly.py`
- `frogdb-server/crates/server/tests/concurrency_workload.rs`, `…/integration_persistence.rs`
- `docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md`,
  `docs/superpowers/plans/2026-07-21-lazy-expiry-parity-fix.md`
- `.superpowers/sdd/failstop/report.md`, `.superpowers/sdd/lazy-expiry/progress.md`
- `website/src/content/docs/architecture/vll.md`
