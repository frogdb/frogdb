# Hardening campaign: inner-loop cost

Warm incremental medians after touching the area's primary file: `check` = `cargo check --all-targets` (type-check feedback), `test build` = `cargo nextest list` (test-binary codegen + link, the real edit->run-a-test latency), plus the area crate's test count. Recorded by `just loop-cost <area>` (scripts/loop-cost.py).

| date | rev | area | crate | check (s) | test build (s) | tests |
|---|---|---|---|---|---|---|
| 2026-07-31 | 1b6b5166 | txn | frogdb-server | 7.9 | 12.1 | 2130 |
| 2026-07-31 | 1b6b5166 | persistence | frogdb-server | 8.4 | 11.6 | 2130 |
| 2026-07-31 | 1b6b5166 | replication | frogdb-server | 8.9 | 14.7 | 2130 |
| 2026-07-31 | 1b6b5166 | cluster | frogdb-server | 9.2 | 12.4 | 2130 |
| 2026-07-31 | 0a42c374 | txn | frogdb-server | 9.1 | 10.9 | 1915 |
| 2026-07-31 | 0a42c374 | persistence | frogdb-server | 11.4 | 12.9 | 1915 |
| 2026-07-31 | 0a42c374 | replication | frogdb-server | 11.8 | 13.6 | 1915 |
| 2026-07-31 | 0a42c374 | cluster | frogdb-server | 11.2 | 12.9 | 1915 |
| 2026-07-31 | dd2f6704 | txn | frogdb-txn | 1.8 | 2.6 | 27 |
| 2026-08-01 | cf7c95d3 | persistence | frogdb-recovery | 4.7 | 4.0 | 11 |
