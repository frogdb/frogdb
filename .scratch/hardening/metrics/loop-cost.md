# Hardening campaign: inner-loop cost

Warm incremental medians after touching the area's primary file: `check` = `cargo check --all-targets` (type-check feedback), `test build` = `cargo nextest list` (test-binary codegen + link, the real edit->run-a-test latency), plus the area crate's test count. Recorded by `just loop-cost <area>` (scripts/loop-cost.py).

| date | rev | area | crate | check (s) | test build (s) | tests |
|---|---|---|---|---|---|---|
| 2026-07-31 | 1b6b5166 | txn | frogdb-server | 7.9 | 12.1 | 2130 |
| 2026-07-31 | 1b6b5166 | persistence | frogdb-server | 8.4 | 11.6 | 2130 |
| 2026-07-31 | 1b6b5166 | replication | frogdb-server | 8.9 | 14.7 | 2130 |
| 2026-07-31 | 1b6b5166 | cluster | frogdb-server | 9.2 | 12.4 | 2130 |
