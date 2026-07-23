# Coverage summary (cargo llvm-cov nextest --all, aarch64 testbox, 2026-07-22)

6824 tests: 6823 pass, 1 flaky (integration_cluster::test_frogdb_version_reports_cluster_info — FLAKY 2/3 under coverage env; passed run 2).

## Per-crate line coverage
TOTAL 84.0% (105531/125629)
Lows: frogdb-macros 0.0% (217L), frogdb-server bin 5.7%, frogctl 46.6%, debug 56.8%, tokio-coz 73.6%, config-derive 73.9%.
Core crates: server 82.5%, scripting 82.9%, commands 84.7%, protocol 85.2%, search 85.9%, core 86.6%, cluster 88.3%, vll 88.9%, telemetry 89.7%, persistence 90.4%, config 91.5%, types 91.7%, replication 92.4%, testing 92.7%, acl 94.5%.

## Worst files (>=100 lines, <65%), server-relevant only
  0.8%   3/397   server/src/commands/info.rs        <- near-zero; possibly legacy/dead vs server/src/info/sections.rs
  0.0%   0/175   server/src/connection/builder.rs
 29.8%  98/329   server/src/config/loader.rs
 34.7% 111/320   core/src/store/mod.rs
 36.9%  83/225   server/src/admin/handlers.rs
 37.5% 414/1104  debug/src/web_ui/handlers.rs
 38.2%  63/165   telemetry/src/otlp.rs
 40.9%  83/203   server/src/connection/persistence_handler.rs
 52.1%  98/188   server/src/connection/routing.rs
 53.4% 260/487   server/src/commands/search.rs
 55.6% 109/196   server/src/connection/scripting/eval.rs
 56.2% 100/178   commands/src/hyperloglog.rs
 56.9%  58/102   config/src/cluster.rs
 60.5% 121/200   commands/src/vectorset/vsim.rs
 61.0%  61/100   server/src/connection/search/hybrid.rs
 61.4% 162/264   core/src/conn_command.rs
 62.1% 435/701   core/src/shard/search/query.rs
 64.1% 329/513   server/src/commands/cluster/admin.rs
(also 0%: frogctl benchmark/replication/search cmds, ops codegen mains — tooling, low priority)
