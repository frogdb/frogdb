# Coverage summary (cargo llvm-cov nextest --all, fixed pipeline, 2026-08-07)

Re-baselined 2026-08-07 from the fixed `just coverage-lcov` pipeline (issue 27). The
prior TOTAL of **84.0% (105531/125629)** dated 2026-07-22 was read from a stale,
near-empty `lcov.info`: the recipe aborted before writing a fresh report (parent dir
`target/llvm-cov/` did not exist on a clean checkout -> `os error 2`) and the old
on-disk artifact was consumed as if real. The recipe now `mkdir -p`s the dir and
deletes any stale file up front, so a failed run leaves no file rather than a
misleading one. Full `--all --features frogctl/cli-tests --ignore-default-filter` run.

## Per-crate line coverage
TOTAL 86.6% (132894/153502)
Lows: frogdb-macros 0.0% (217L), frogdb-server bin/ops 5.7% (1483L), frogctl 49.3%, net 61.1% (18L), debug 64.6%, config-derive 73.9%.
Core crates: scripting 83.7%, commands 85.0%, recovery 85.1%, server 85.3%, search 86.2%, core 87.6%, protocol 87.8%, test-harness 88.3%, replication-runtime 91.8%, telemetry 91.9%, types 91.9%, config 92.6%, persistence 93.0%, testing 93.1%, cluster 93.7%, vll 93.7%, acl 94.6%, cluster-runtime 94.7%, replication 94.7%, txn 95.6%.

## Worst files (>=100 lines, <65%), server-relevant only
 46.9% 204/435  server/src/commands/info.rs        <- was misreported 0.8% off the stale lcov; real figure is 46.9%
  0.0%   0/176  server/src/connection/builder.rs
 29.6%  97/328  server/src/config/loader.rs
 34.4% 111/323  core/src/store/mod.rs
 36.4%  83/228  server/src/admin/handlers.rs
 42.1% 502/1193 debug/src/web_ui/handlers.rs
 40.9%  83/203  server/src/connection/persistence_handler.rs
 52.1%  98/188  server/src/connection/routing.rs
 52.7%  87/165  telemetry/src/otlp.rs
 53.4% 260/487  server/src/commands/search.rs
 54.6% 107/196  server/src/connection/scripting/eval.rs
 56.2% 100/178  commands/src/hyperloglog.rs
 56.9%  58/102  config/src/cluster.rs
(also 0%: macros/src/command.rs 214L, frogctl benchmark/replication/search cmds, ops codegen mains — tooling/proc-macro, low priority)

## Structural sanity
Nonzero-DA ratio 0.8727 (128637/147408 line records executed) — a healthy full-suite
run. The CI plausibility gate (issue 27) fails the nightly when this drops below 0.30
or LH == 0, the signature of the stale/empty artifact that produced the old 84.0%.
