# Coverage depth (per-line exec counts + per-function test diversity, 2026-08-07)

8031 per-test profiles joined against one aggregate `llvm-cov export`; name join hit-rate 100.00%.
Toolchain: rustc 1.92.0 (ded5c06cf 2025-12-08) — stable 1.92.0: -Z coverage-options=branch and MC/DC are nightly-only; region coverage reported instead.

## Totals

Lines 85.3% (137324/160964) · Regions 86.2% (229227/265792)

De-duplicated per-file line view: 132959/154631 (86.0%). The totals above are llvm-cov's own per-file summaries, which sum *per function*, so a line in several function records is counted once per function; the two differ in 444/565 files. The de-duplicated figure uses the same per-line counting as `llvm-cov export --format=lcov`'s `DA` records (what the HTML gutter shows); `just coverage-lcov` writes that lcov, and `coverage-depth.py report` cross-checks the two totals when the file is present.

Function classes are counted over **source functions**: 39811 raw records (one per monomorphization + zeroed `::<_>` placeholders) fold to 17115 functions. Both counts below.

| class | functions (deduped) | raw records | meaning |
|---|---:|---:|---|
| `untested` | 2414 | 15791 | no test reaches it at all |
| `single-test` | 5959 | 7949 | one test is the entire safety net |
| `monoculture` | 1679 | 5535 | reached by >1 test but only one suite |
| `hot-but-shallow` | 2 | 13 | exec_total >= 11656 but <= 3 tests |
| `covered` | 772 | 1351 | middling breadth |
| `well-covered` | 6289 | 9172 | >= 5 tests across >= 2 suites |

## Per-crate

| crate | lines | line % | regions | region % |
|---|---:|---:|---:|---:|
| config-derive | 0/199 | 0.0% | 0/296 | 0.0% |
| deb | 0/139 | 0.0% | 0/267 | 0.0% |
| docs-gen | 0/404 | 0.0% | 0/666 | 0.0% |
| frogctl | 0/4370 | 0.0% | 0/7479 | 0.0% |
| frogdb-macros | 0/217 | 0.0% | 0/404 | 0.0% |
| grafana | 0/477 | 0.0% | 0/633 | 0.0% |
| helm | 0/224 | 0.0% | 0/332 | 0.0% |
| metrics-derive | 0/191 | 0.0% | 0/335 | 0.0% |
| frogdb-admin | 84/239 | 35.1% | 138/384 | 35.9% |
| net | 11/18 | 61.1% | 24/35 | 68.6% |
| debug | 1660/2570 | 64.6% | 2083/3101 | 67.2% |
| tokio-coz | 725/985 | 73.6% | 1228/1633 | 75.2% |
| scripting | 1705/2038 | 83.7% | 2826/3477 | 81.3% |
| commands | 11667/13722 | 85.0% | 20788/23885 | 87.0% |
| server | 28534/33431 | 85.4% | 43331/50047 | 86.6% |
| search | 6157/7145 | 86.2% | 10521/12181 | 86.4% |
| protocol | 928/1057 | 87.8% | 1513/1736 | 87.2% |
| core | 23168/26274 | 88.2% | 40117/44625 | 89.9% |
| test-harness | 1942/2178 | 89.2% | 2894/3263 | 88.7% |
| replication-runtime | 1408/1533 | 91.8% | 2375/2568 | 92.5% |
| telemetry | 3497/3804 | 91.9% | 5529/5977 | 92.5% |
| types | 11053/12022 | 91.9% | 19279/20925 | 92.1% |
| config | 2616/2824 | 92.6% | 3543/3841 | 92.2% |
| recovery | 1239/1337 | 92.7% | 2115/2235 | 94.6% |
| testing | 5702/6126 | 93.1% | 12300/13050 | 94.3% |
| persistence | 9118/9776 | 93.3% | 16806/18007 | 93.3% |
| vll | 1437/1534 | 93.7% | 2703/2934 | 92.1% |
| cluster | 5878/6273 | 93.7% | 8548/9241 | 92.5% |
| shard-harness | 666/708 | 94.1% | 839/901 | 93.1% |
| acl | 3834/4054 | 94.6% | 5718/6084 | 94.0% |
| replication | 11769/12437 | 94.6% | 20083/21105 | 95.2% |
| cluster-runtime | 2044/2154 | 94.9% | 3035/3194 | 95.0% |
| txn | 482/504 | 95.6% | 891/951 | 93.7% |

## Untested functions

`test_count == 0` — instrumented, instantiated, never entered by any test. Ranked by region count (bigger = more untested logic).

| function | location | exec | tests | covering tests |
|---|---|---:|---:|---|
| `frogctl::ops::config::generate_default_config` | frogctl/src/ops/config.rs:26 | 0 | 0 | — |
| `frogctl::ops::config::validate_config` | frogctl/src/ops/config.rs:121 | 0 | 0 | — |
| `dashboard_gen::generate_dashboard` | ops/grafana/dashboard-gen/src/main.rs:53 | 0 | 0 | — |
| `<frogdb_server::connection::ConnectionHandler>::handle_migrate_command::{closure#0}` | server/src/connection/persistence_handler.rs:31 | 0 | 0 | — |
| `frogctl::commands::stat::run::{closure#0}` | frogctl/src/commands/stat.rs:29 | 0 | 0 | — |
| `frogctl::commands::scan::run::{closure#0}` | frogctl/src/commands/scan.rs:134 | 0 | 0 | — |
| `frogdb_metrics_derive::define_metrics` | metrics-derive/src/lib.rs:240 | 0 | 0 | — |
| `frogctl::commands::benchmark::run::{closure#0}` | frogctl/src/commands/benchmark.rs:77 | 0 | 0 | — |
| `frogdb_debug::web_ui::handlers::render_cluster_tab_html` | debug/src/web_ui/handlers.rs:682 | 0 | 0 | — |
| `frogctl::ops::backup::verify_export` | frogctl/src/ops/backup.rs:339 | 0 | 0 | — |
| `frogdb_server::config::loader::init_logging_inner::<tracing_subscriber::layer::Identity>` | server/src/config/loader.rs:237 | 0 | 0 | — |
| `<frogdb_server::connection::ConnectionHandler>::execute_cross_shard_copy::{closure#0}` | server/src/connection/routing.rs:197 | 0 | 0 | — |
| `dashboard_gen::create_histogram_panel` | ops/grafana/dashboard-gen/src/main.rs:450 | 0 | 0 | — |
| `docs_gen::main` | ops/docs-gen/src/main.rs:282 | 0 | 0 | — |
| `<frogctl::commands::scan::ScanResult as frogctl::output::Renderable>::render_table` | frogctl/src/commands/scan.rs:58 | 0 | 0 | — |
| `dashboard_gen::create_gauge_panel` | ops/grafana/dashboard-gen/src/main.rs:370 | 0 | 0 | — |
| `frogctl::commands::health::check_node_health::{closure#0}` | frogctl/src/commands/health.rs:183 | 0 | 0 | — |
| `frogctl::commands::replication::run_status::{closure#0}` | frogctl/src/commands/replication.rs:118 | 0 | 0 | — |
| `deb_gen::generate_files` | ops/deb/deb-gen/src/main.rs:121 | 0 | 0 | — |
| `<frogdb_metrics_derive::MetricsInput as syn::parse::Parse>::parse` | metrics-derive/src/lib.rs:143 | 0 | 0 | — |
| `<frogdb_core::shard::worker::ShardWorker>::format_scatter_op` | core/src/shard/diagnostics.rs:302 | 0 | 0 | — |
| `dashboard_gen::create_counter_panel` | ops/grafana/dashboard-gen/src/main.rs:295 | 0 | 0 | — |
| `frogdb_server::main` | server/src/main.rs:19 | 0 | 0 | — |
| `frogctl::commands::debug::run_latency::{closure#0}` | frogctl/src/commands/debug.rs:449 | 0 | 0 | — |
| `frogctl::commands::health::run_admin_health::{closure#0}` | frogctl/src/commands/health.rs:363 | 0 | 0 | — |
| `frogctl::commands::upgrade::run_status::{closure#0}` | frogctl/src/commands/upgrade.rs:134 | 0 | 0 | — |
| `frogctl::run::{closure#0}` | frogctl/src/main.rs:17 | 0 | 0 | — |
| `helm_gen::generate_files` | ops/helm/helm-gen/src/main.rs:83 | 0 | 0 | — |
| `frogctl::ops::scan::enrich_keys::{closure#0}` | frogctl/src/ops/scan.rs:108 | 0 | 0 | — |
| `tokio_coz::reporter::print_summary` | tokio-coz/src/reporter.rs:25 | 0 | 0 | — |
| `<tokio_coz::experiment::ExperimentEngine>::run::{closure#0}` | tokio-coz/src/experiment.rs:38 | 0 | 0 | — |
| `frogctl::commands::benchmark::run::{closure#0}::{closure#1}` | frogctl/src/commands/benchmark.rs:99 | 0 | 0 | — |
| `frogctl::commands::upgrade::run_plan::{closure#0}` | frogctl/src/commands/upgrade.rs:268 | 0 | 0 | — |
| `helm_gen::check_files` | ops/helm/helm-gen/src/main.rs:122 | 0 | 0 | — |
| `frogctl::ops::config::diff_configs` | frogctl/src/ops/config.rs:284 | 0 | 0 | — |
| `frogctl::commands::search::run_query::{closure#0}` | frogctl/src/commands/search.rs:216 | 0 | 0 | — |
| `<helm_gen::HelmValues>::from_config` | ops/helm/helm-gen/src/main.rs:433 | 0 | 0 | — |
| `<frogdb_core::shard::worker::ShardWorker>::scatter_copy_set::{closure#0}` | core/src/shard/execution.rs:1087 | 0 | 0 | — |
| `docs_gen::generate_config_reference` | ops/docs-gen/src/main.rs:390 | 0 | 0 | — |
| `frogctl::ops::backup::parse_data_file` | frogctl/src/ops/backup.rs:299 | 0 | 0 | — |
| `frogctl::ops::latency::render_ascii_graph` | frogctl/src/ops/latency.rs:135 | 0 | 0 | — |
| `frogdb_macros::command::parse_arity_spec` | frogdb-macros/src/command.rs:132 | 0 | 0 | — |
| `frogdb_admin::run::{closure#0}` | ops/frogdb-admin/src/main.rs:77 | 0 | 0 | — |
| `frogctl::commands::health::check_remote_health::{closure#0}::{closure#0}` | frogctl/src/commands/health.rs:233 | 0 | 0 | — |
| `<frogdb_commands::hyperloglog::PfselftestCommand as frogdb_core::command::Command>::execute` | commands/src/hyperloglog.rs:337 | 0 | 0 | — |
| `frogdb_debug::web_ui::handlers::render_cluster_node_html` | debug/src/web_ui/handlers.rs:842 | 0 | 0 | — |
| `frogctl::commands::debug::parse_client_line` | frogctl/src/commands/debug.rs:577 | 0 | 0 | — |
| `<frogdb_commands::scan::ScanCommand as frogdb_core::command::Command>::execute` | commands/src/scan.rs:67 | 0 | 0 | — |
| `frogctl::commands::upgrade::run_finalize::{closure#0}` | frogctl/src/commands/upgrade.rs:365 | 0 | 0 | — |
| `frogdb_config_derive::expand` | config-derive/src/lib.rs:121 | 0 | 0 | — |
| `frogctl::commands::search::run_create::{closure#0}` | frogctl/src/commands/search.rs:310 | 0 | 0 | — |
| `frogctl::commands::acl::parse_log_entry` | frogctl/src/commands/acl.rs:332 | 0 | 0 | — |
| `frogdb_server::admin::handlers::nodes::{closure#0}::{closure#0}` | server/src/admin/handlers.rs:199 | 0 | 0 | — |
| `<frogdb_telemetry::prometheus_recorder::PrometheusRecorder>::get_histogram_quantiles` | telemetry/src/prometheus_recorder.rs:461 | 0 | 0 | — |
| `<frogdb_core::shard::wait_queue::ShardWaitQueue>::pop_oldest_waiter` | core/src/shard/wait_queue.rs:246 | 0 | 0 | — |
| `frogctl::commands::health::run_probe::{closure#0}` | frogctl/src/commands/health.rs:314 | 0 | 0 | — |
| `frogctl::commands::debug::run_memory_stats::{closure#0}` | frogctl/src/commands/debug.rs:724 | 0 | 0 | — |
| `frogctl::output::format_value` | frogctl/src/output.rs:31 | 0 | 0 | — |
| `docs_gen::extract_fields` | ops/docs-gen/src/main.rs:645 | 0 | 0 | — |
| `frogdb_macros::command::parse_command_attr::{closure#0}` | frogdb-macros/src/command.rs:101 | 0 | 0 | — |

_showing 60 of 2414; full list in `depth.json`._

## Single-test functions

One test is the entire safety net. Deleting or weakening that test silently removes all coverage of this function.

| function | location | exec | tests | covering tests |
|---|---|---:|---:|---|
| `frogdb_persistence::recovery::tests::round_trips_format_through_mock_sink` | persistence/src/recovery.rs:303 | 1 | 1 | frogdb_persistence::recovery::tests::round_trips_format_through_mock_sink |
| `frogdb_acl::categories::data::COMMAND_CATEGORIES::{closure#0}` | acl/src/categories/data.rs:10 | 1 | 1 | frogdb_acl::categories::tests::test_command_category |
| `frogdb_server::server::register::spec_exhaustiveness::acl_category_gap_allowlist` | server/src/server/register.rs:614 | 1 | 1 | frogdb_server::server::register::spec_exhaustiveness::every_registered_command_has_acl_category_or_is_allowlisted |
| `<frogdb_config::Config as frogdb_server::config::loader::ConfigLoader>::load` | server/src/config/loader.rs:68 | 1 | 1 | frogdb_server::config::tests::test_load_explicit_config_file_not_found |
| `frogdb_replication::replica_session::tests::fullresync_offset_and_metadata_come_from_live_tracker::{closure#0}` | replication/src/replica_session.rs:3194 | 1 | 1 | frogdb_replication::replica_session::tests::fullresync_offset_and_metadata_come_from_live_tracker |
| `frogdb_server::server::cluster_init::tests::split_brain_lifecycle_captures_audit_and_initiates_discard::{closu` | server/src/server/cluster_init.rs:1555 | 1 | 1 | frogdb_server::server::cluster_init::tests::split_brain_lifecycle_captures_audit_and_initiates_discard |
| `frogdb_core::persistence::tests::integration::test_mixed_types_recovery` | core/src/persistence/tests.rs:558 | 1 | 1 | frogdb_core::persistence::tests::integration::test_mixed_types_recovery |
| `frogdb_cluster::network::tests::test_all_rpc_variants_roundtrip` | cluster/src/network.rs:891 | 1 | 1 | frogdb_cluster::network::tests::test_all_rpc_variants_roundtrip |
| `<frogdb_commands::cuckoo::CfLoadchunk as frogdb_core::command::Command>::execute` | commands/src/cuckoo.rs:653 | 1 | 1 | main::bloom_regression::cf_scandump_loadchunk_roundtrip |
| `frogdb_persistence::serialization::unit_tests::copy_codec_round_trips_all_value_variants` | persistence/src/serialization/mod.rs:266 | 1 | 1 | frogdb_persistence::serialization::unit_tests::copy_codec_round_trips_all_value_variants |
| `frogdb_search::aggregate::tests::test_parse_new_reducers` | search/src/aggregate.rs:1270 | 1 | 1 | frogdb_search::aggregate::tests::test_parse_new_reducers |
| `frogdb_telemetry::status::tests::status_thresholds_are_live` | telemetry/src/status.rs:1477 | 1 | 1 | frogdb_telemetry::status::tests::status_thresholds_are_live |
| `frogdb_persistence::serialization::stream::tests::round_trips_groups_pel_and_consumers` | persistence/src/serialization/stream.rs:307 | 1 | 1 | frogdb_persistence::serialization::stream::tests::round_trips_groups_pel_and_consumers |
| `frogdb_core::persistence::tests::integration::test_roundtrip_persistence` | core/src/persistence/tests.rs:20 | 1 | 1 | frogdb_core::persistence::tests::integration::test_roundtrip_persistence |
| `frogdb_cluster::storage::tests::test_state_machine_snapshot_survives_restart_without_log_replay::{closure#0}` | cluster/src/storage.rs:752 | 1 | 1 | frogdb_cluster::storage::tests::test_state_machine_snapshot_survives_restart_without_log_replay |
| `frogdb_replication_runtime::install::tests::a_checkpoint_this_node_cannot_read_is_refused_and_touches_no_shard` | replication-runtime/src/install.rs:745 | 1 | 1 | frogdb_replication_runtime::install::tests::a_checkpoint_this_node_cannot_read_is_refused_and_touches_no_shard |
| `frogdb_replication::replica::connection::tests::a_full_sync_that_never_delivers_a_dataset_leaves_the_old_histo` | replication/src/replica/connection.rs:1383 | 1 | 1 | frogdb_replication::replica::connection::tests::a_full_sync_that_never_delivers_a_dataset_leaves_the_old_history_alone |
| `frogdb_replication::replica_session::tests::fullresync_cuts_the_checkpoint_after_the_pre_checkpoint_hook::{clo` | replication/src/replica_session.rs:2753 | 1 | 1 | frogdb_replication::replica_session::tests::fullresync_cuts_the_checkpoint_after_the_pre_checkpoint_hook |
| `<frogdb_commands::bloom::BfLoadchunk as frogdb_core::command::Command>::execute` | commands/src/bloom.rs:592 | 1 | 1 | main::bloom_regression::bf_scandump_loadchunk_roundtrip |
| `frogdb_persistence::serialization::unit_tests::collection_contents_survive_round_trip` | persistence/src/serialization/mod.rs:383 | 1 | 1 | frogdb_persistence::serialization::unit_tests::collection_contents_survive_round_trip |
| `frogdb_replication::replica::connection::tests::a_checkpoint_that_dies_mid_transfer_leaves_the_old_history_alo` | replication/src/replica/connection.rs:1521 | 1 | 1 | frogdb_replication::replica::connection::tests::a_checkpoint_that_dies_mid_transfer_leaves_the_old_history_alone |
| `frogdb_server::connection::codec::tests::decode_edge_cases_table` | server/src/connection/codec.rs:564 | 1 | 1 | frogdb_server::connection::codec::tests::decode_edge_cases_table |
| `frogdb_replication::replica_session::tests::handle_partial_replays_backlog_then_live_tail::{closure#0}` | replication/src/replica_session.rs:2148 | 1 | 1 | frogdb_replication::replica_session::tests::handle_partial_replays_backlog_then_live_tail |
| `frogdb_core::shard::panic_guard::isolation_tests::a_panicking_vll_op_releases_its_locks_and_the_shard_keeps_se` | core/src/shard/panic_guard.rs:495 | 1 | 1 | frogdb_core::shard::panic_guard::isolation_tests::a_panicking_vll_op_releases_its_locks_and_the_shard_keeps_serving |
| `frogdb_search::expression::tests::test_timefmt_strftime_basic` | search/src/expression.rs:1319 | 1 | 1 | frogdb_search::expression::tests::test_timefmt_strftime_basic |
| `frogdb_replication::replica_session::tests::run_full_sync_without_rocks_streams_the_live_dataset::{closure#0}` | replication/src/replica_session.rs:2503 | 1 | 1 | frogdb_replication::replica_session::tests::run_full_sync_without_rocks_streams_the_live_dataset |
| `frogdb_core::shard::execution::scatter_effect_tests::scatter_del_emits_notification_broadcast_and_wakes_waiter` | core/src/shard/execution.rs:1424 | 1 | 1 | frogdb_core::shard::execution::scatter_effect_tests::scatter_del_emits_notification_broadcast_and_wakes_waiter |
| `frogdb_testing::conservation::tests::watch_partial_multi_key_del_not_flagged` | testing/src/conservation.rs:1893 | 1 | 1 | frogdb_testing::conservation::tests::watch_partial_multi_key_del_not_flagged |
| `frogdb_server::connection::observability_conn_command::tests::status_json_renders_from_shared_collector_and_ag` | server/src/connection/observability_conn_command.rs:1263 | 1 | 1 | frogdb_server::connection::observability_conn_command::tests::status_json_renders_from_shared_collector_and_agrees_with_http |
| `frogdb_types::json::create_path` | types/src/json.rs:1131 | 1 | 1 | frogdb_types::json::tests::test_set_nx |
| `frogdb_replication::replica_session::tests::full_sync_replays_writes_made_during_handoff::{closure#0}` | replication/src/replica_session.rs:2074 | 1 | 1 | frogdb_replication::replica_session::tests::full_sync_replays_writes_made_during_handoff |
| `frogdb_testing::conservation::tests::pel_cross_stream_not_contaminated` | testing/src/conservation.rs:2075 | 1 | 1 | frogdb_testing::conservation::tests::pel_cross_stream_not_contaminated |
| `frogdb_persistence::rocks::tests::hll_merge_operand_folds_and_survives_reopen` | persistence/src/rocks/tests.rs:77 | 1 | 1 | frogdb_persistence::rocks::tests::hll_merge_operand_folds_and_survives_reopen |
| `frogdb_persistence::wal::tests::test_wal_clear_reclamation_end_to_end::{closure#0}` | persistence/src/wal/tests.rs:929 | 1 | 1 | frogdb_persistence::wal::tests::test_wal_clear_reclamation_end_to_end |
| `<frogdb_server::connection::ConnectionHandler>::handle_ft_profile::{closure#0}` | server/src/connection/search/profile.rs:14 | 1 | 1 | main::search_regression::ft_profile_search |
| `frogdb_replication::fullsync::tests::test_checkpoint_checksum_agreement::{closure#0}` | replication/src/fullsync.rs:739 | 1 | 1 | frogdb_replication::fullsync::tests::test_checkpoint_checksum_agreement |
| `frogdb_server::commands::info::build_replication_info` | server/src/commands/info.rs:450 | 2 | 1 | main::integration_replication::info_reports_the_configured_backlog_geometry_through_both_renderers |
| `frogdb_core::conn_command::tests::new_defaults_are_placeholders_and_with_full_reads_overrides_them::{closure#0` | core/src/conn_command.rs:1093 | 1 | 1 | frogdb_core::conn_command::tests::new_defaults_are_placeholders_and_with_full_reads_overrides_them |
| `frogdb_core::persistence::crash_recovery_tests::recovery_correctness::test_all_types_crash_recovery` | core/src/persistence/crash_recovery_tests.rs:452 | 1 | 1 | frogdb_core::persistence::crash_recovery_tests::recovery_correctness::test_all_types_crash_recovery |
| `frogdb_server::role_manager::tests::promote_stops_registered_boot_replica_handler::{closure#0}` | server/src/role_manager.rs:1556 | 1 | 1 | frogdb_server::role_manager::tests::promote_stops_registered_boot_replica_handler |
| `frogdb_testing::partition::tests::default_keys_of_stream_group_ops` | testing/src/partition.rs:441 | 1 | 1 | frogdb_testing::partition::tests::default_keys_of_stream_group_ops |
| `frogdb_cluster::wire::tests::test_shard_views_grouping_and_order` | cluster/src/wire.rs:369 | 1 | 1 | frogdb_cluster::wire::tests::test_shard_views_grouping_and_order |
| `frogdb_core::persistence::crash_recovery_tests::recovery_correctness::test_sorted_set_index_rebuilt` | core/src/persistence/crash_recovery_tests.rs:579 | 1 | 1 | frogdb_core::persistence::crash_recovery_tests::recovery_correctness::test_sorted_set_index_rebuilt |
| `frogdb_replication::replica_session::tests::a_full_sync_whose_handoff_window_is_evicted_abandons_the_link::{cl` | replication/src/replica_session.rs:2362 | 1 | 1 | frogdb_replication::replica_session::tests::a_full_sync_whose_handoff_window_is_evicted_abandons_the_link |
| `frogdb_search::wire::tests::test_search_request_full_grammar` | search/src/wire.rs:561 | 1 | 1 | frogdb_search::wire::tests::test_search_request_full_grammar |
| `frogdb_server::commands::cluster::cluster_help` | server/src/commands/cluster/mod.rs:703 | 1 | 1 | cluster_topology::test_cluster_help_command |
| `frogdb_cluster_runtime::failure_detector::tests::test_health_table_threshold_latching_is_symmetric` | cluster-runtime/src/failure_detector.rs:846 | 1 | 1 | frogdb_cluster_runtime::failure_detector::tests::test_health_table_threshold_latching_is_symmetric |
| `frogdb_persistence::rocks::tests::search_meta_shims_address_their_own_tier_and_shard` | persistence/src/rocks/tests.rs:1649 | 1 | 1 | frogdb_persistence::rocks::tests::search_meta_shims_address_their_own_tier_and_shard |
| `frogdb_persistence::wal::tests::the_wal_sink_trait_object_forwards_every_method_to_the_writer::{closure#0}` | persistence/src/wal/tests.rs:2233 | 1 | 1 | frogdb_persistence::wal::tests::the_wal_sink_trait_object_forwards_every_method_to_the_writer |
| `frogdb_replication::replica_session::tests::a_resume_evicted_after_the_grant_is_abandoned_not_truncated::{clos` | replication/src/replica_session.rs:2293 | 1 | 1 | frogdb_replication::replica_session::tests::a_resume_evicted_after_the_grant_is_abandoned_not_truncated |
| `frogdb_testing::fault_injection::tests::lose_stream_entry_is_caught` | testing/src/fault_injection.rs:427 | 1 | 1 | frogdb_testing::fault_injection::tests::lose_stream_entry_is_caught |
| `frogdb_scripting::sandbox::msgpack_to_lua` | scripting/src/sandbox.rs:931 | 2 | 1 | frogdb_scripting::sandbox::tests::test_cmsgpack_in_both_modes |
| `frogdb_server::connection::conn_command::tests::ft_cursor_read_pages_and_exhausts::{closure#0}` | server/src/connection/conn_command.rs:676 | 1 | 1 | frogdb_server::connection::conn_command::tests::ft_cursor_read_pages_and_exhausts |
| `frogdb_server::server::cluster_init::tests::split_brain_buffer_overflow_truncates_audit_silently` | server/src/server/cluster_init.rs:1823 | 1 | 1 | frogdb_server::server::cluster_init::tests::split_brain_buffer_overflow_truncates_audit_silently |
| `frogdb_types::timeseries::label_index::tests::test_add_and_query` | types/src/timeseries/label_index.rs:334 | 1 | 1 | frogdb_types::timeseries::label_index::tests::test_add_and_query |
| `frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order` | core/src/shard/blocking.rs:1711 | 1 | 1 | frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order |
| `frogdb_replication::replica_session::tests::fullresync_fails_when_the_pre_checkpoint_drain_fails::{closure#0}` | replication/src/replica_session.rs:2849 | 1 | 1 | frogdb_replication::replica_session::tests::fullresync_fails_when_the_pre_checkpoint_drain_fails |
| `frogdb_server::runtime_config::tests::rewrite_with_tls_enabled_and_no_optional_paths_still_boots` | server/src/runtime_config.rs:5732 | 1 | 1 | frogdb_server::runtime_config::tests::rewrite_with_tls_enabled_and_no_optional_paths_still_boots |
| `frogdb_persistence::snapshot::tests::test_coordinator_records_failed_then_recovered_save::{closure#0}` | persistence/src/snapshot/tests.rs:1129 | 1 | 1 | frogdb_persistence::snapshot::tests::test_coordinator_records_failed_then_recovered_save |
| `frogdb_replication::replica::connection::tests::psync_rejects_a_payload_that_carries_no_dataset::{closure#0}` | replication/src/replica/connection.rs:1306 | 1 | 1 | frogdb_replication::replica::connection::tests::psync_rejects_a_payload_that_carries_no_dataset |

_showing 60 of 5959; full list in `depth.json`._

## Monoculture functions

Reached by several tests, but all from a single suite. High line coverage here hides the fact that only one angle of attack is represented.

| function | location | exec | tests | covering tests |
|---|---|---:|---:|---|
| `frogdb_cluster::state::tests::test_addr` | cluster/src/state.rs:1000 | 418 | 91 | frogdb_cluster::state::tests::complete_migration_emits_event_on_success, frogdb_cluster::state::tests::complete_migration_emits_no_event_on_error, frogdb_cluster::state::tests::force_failover_emits_promotion_only, +88 more |
| `<frogdb_server::connection::deps::ClusterDeps>::standalone` | server/src/connection/deps.rs:132 | 88 | 88 | frogdb_server::connection::acl_conn_command::tests::acl_cat_lists_categories_and_category_commands, frogdb_server::connection::acl_conn_command::tests::acl_deluser_removes_user, frogdb_server::connection::acl_conn_command::tests::acl_dryrun_denied_command_reports_permission, +85 more |
| `<frogdb_core::conn_command::ConnCtx>::with_username` | core/src/conn_command.rs:838 | 82 | 66 | frogdb_server::connection::acl_conn_command::tests::acl_cat_lists_categories_and_category_commands, frogdb_server::connection::acl_conn_command::tests::acl_deluser_removes_user, frogdb_server::connection::acl_conn_command::tests::acl_dryrun_denied_command_reports_permission, +63 more |
| `frogdb_server::runtime_config::tests::test_config` | server/src/runtime_config.rs:3768 | 67 | 65 | frogdb_server::runtime_config::tests::batch_size_threshold_set_reaches_the_shared_wal_cell, frogdb_server::runtime_config::tests::cluster_flag_sets_reach_the_live_flags, frogdb_server::runtime_config::tests::hotshard_threshold_sets_enforce_the_section_validator_bounds, +62 more |
| `frogdb_replication::replica_session::tests::addr` | replication/src/replica_session.rs:1539 | 54 | 51 | frogdb_replication::replica_session::tests::a_full_sync_whose_handoff_window_is_evicted_abandons_the_link, frogdb_replication::replica_session::tests::a_lag_disconnect_is_a_lost_departure, frogdb_replication::replica_session::tests::a_new_streaming_generation_clears_the_previous_departure, +48 more |
| `frogdb_testing::conservation::tests::b` | testing/src/conservation.rs:1150 | 599 | 51 | frogdb_testing::conservation::tests::delivery_blpop_hit_parsed, frogdb_testing::conservation::tests::delivery_counts_list_effect_scripts_as_pushes, frogdb_testing::conservation::tests::delivery_detects_double_pop, +48 more |
| `frogdb_server::info::test_support::sources` | server/src/info/mod.rs:860 | 50 | 42 | frogdb_server::info::sections::tests::a_link_that_is_merely_down_renders_no_sync_error, frogdb_server::info::sections::tests::a_replica_reports_its_configured_capacity_with_no_window, frogdb_server::info::sections::tests::a_replica_that_gave_up_names_the_mismatch_in_info, +39 more |
| `frogdb_server::info::sections::tests::render` | server/src/info/sections.rs:723 | 51 | 40 | frogdb_server::info::sections::tests::a_link_that_is_merely_down_renders_no_sync_error, frogdb_server::info::sections::tests::a_replica_reports_its_configured_capacity_with_no_window, frogdb_server::info::sections::tests::a_replica_that_gave_up_names_the_mismatch_in_info, +37 more |
| `frogdb_recovery::tests::persistence_config` | recovery/src/tests.rs:25 | 38 | 36 | frogdb_recovery::tests::a_corrupt_marker_refuses_the_boot, frogdb_recovery::tests::a_failing_key_is_previewed_whole_up_to_the_limit_and_marked_when_cut, frogdb_recovery::tests::a_fresh_data_dir_boots_and_stamps_the_marker, +33 more |
| `frogdb_recovery::tests::replication_config` | recovery/src/tests.rs:34 | 43 | 36 | frogdb_recovery::tests::a_corrupt_marker_refuses_the_boot, frogdb_recovery::tests::a_failing_key_is_previewed_whole_up_to_the_limit_and_marked_when_cut, frogdb_recovery::tests::a_fresh_data_dir_boots_and_stamps_the_marker, +33 more |
| `frogdb_recovery::tests::cluster_config` | recovery/src/tests.rs:62 | 43 | 36 | frogdb_recovery::tests::a_corrupt_marker_refuses_the_boot, frogdb_recovery::tests::a_failing_key_is_previewed_whole_up_to_the_limit_and_marked_when_cut, frogdb_recovery::tests::a_fresh_data_dir_boots_and_stamps_the_marker, +33 more |
| `frogdb_server::connection::state::tests::state` | server/src/connection/state.rs:1118 | 42 | 36 | frogdb_server::connection::state::tests::asking_absent_inside_multi_stays_absent, frogdb_server::connection::state::tests::asking_cleared_by_clear_transaction, frogdb_server::connection::state::tests::asking_cleared_by_discard, +33 more |
| `frogdb_server::slot_migration::tests::test_addr` | server/src/slot_migration/tests.rs:22 | 151 | 34 | frogdb_server::slot_migration::tests::batch_on_foreign_slot_is_moved_to_the_owner, frogdb_server::slot_migration::tests::batch_on_import_target_with_asking_probes_importing, frogdb_server::slot_migration::tests::batch_on_import_target_without_asking_is_moved, +31 more |
| `frogdb_server::slot_migration::tests::empty_snapshot` | server/src/slot_migration/tests.rs:28 | 36 | 32 | frogdb_server::slot_migration::tests::batch_on_foreign_slot_is_moved_to_the_owner, frogdb_server::slot_migration::tests::batch_on_import_target_with_asking_probes_importing, frogdb_server::slot_migration::tests::batch_on_import_target_without_asking_is_moved, +29 more |
| `frogdb_recovery::tests::continue_policy` | recovery/src/tests.rs:46 | 37 | 31 | frogdb_recovery::tests::a_corrupt_marker_refuses_the_boot, frogdb_recovery::tests::a_fresh_data_dir_boots_and_stamps_the_marker, frogdb_recovery::tests::an_installed_checkpoint_leaves_the_data_dir_marked, +28 more |
| `<frogdb_server::connection::observability_conn_command::tests::Fixture>::with_status_collector` | server/src/connection/observability_conn_command.rs:942 | 30 | 30 | frogdb_server::connection::observability_conn_command::tests::latency_bands_disabled_by_default_errors, frogdb_server::connection::observability_conn_command::tests::latency_empty_args_errors, frogdb_server::connection::observability_conn_command::tests::latency_graph_missing_event_errors, +27 more |
| `<frogdb_server::connection::observability_conn_command::tests::Fixture>::ctx` | server/src/connection/observability_conn_command.rs:960 | 30 | 30 | frogdb_server::connection::observability_conn_command::tests::latency_bands_disabled_by_default_errors, frogdb_server::connection::observability_conn_command::tests::latency_empty_args_errors, frogdb_server::connection::observability_conn_command::tests::latency_graph_missing_event_errors, +27 more |
| `frogdb_cluster::state::tests::failover_fixture` | cluster/src/state.rs:2658 | 33 | 29 | frogdb_cluster::state::tests::force_failover_emits_promotion_only, frogdb_cluster::state::tests::graceful_failover_emits_node_demoted_for_old_primary, frogdb_cluster::state::tests::test_demotion_detection_fires_for_graceful_failover_of_self, +26 more |
| `frogdb_server::connection::observability_conn_command::tests::default_status_collector` | server/src/connection/observability_conn_command.rs:921 | 29 | 29 | frogdb_server::connection::observability_conn_command::tests::latency_bands_disabled_by_default_errors, frogdb_server::connection::observability_conn_command::tests::latency_empty_args_errors, frogdb_server::connection::observability_conn_command::tests::latency_graph_missing_event_errors, +26 more |
| `<frogdb_server::connection::observability_conn_command::tests::Fixture>::new` | server/src/connection/observability_conn_command.rs:938 | 29 | 29 | frogdb_server::connection::observability_conn_command::tests::latency_bands_disabled_by_default_errors, frogdb_server::connection::observability_conn_command::tests::latency_empty_args_errors, frogdb_server::connection::observability_conn_command::tests::latency_graph_missing_event_errors, +26 more |
| `frogdb_core::shard::blocking::tests::make_entry` | core/src/shard/blocking.rs:1268 | 46 | 27 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmove_propagates_as_lmove_with_directions, +24 more |
| `<frogdb_core::persistence::test_harness::CrashTestHarness>::with_config` | core/src/persistence/test_harness.rs:51 | 26 | 26 | frogdb_core::persistence::crash_recovery_tests::atomicity::test_single_key_atomic, frogdb_core::persistence::crash_recovery_tests::disk_failure::test_binary_data_crash_recovery, frogdb_core::persistence::crash_recovery_tests::disk_failure::test_unicode_data_crash_recovery, +23 more |
| `frogdb_recovery::tests::mark` | recovery/src/tests.rs:76 | 34 | 26 | frogdb_recovery::tests::a_corrupt_marker_refuses_the_boot, frogdb_recovery::tests::a_failing_key_is_previewed_whole_up_to_the_limit_and_marked_when_cut, frogdb_recovery::tests::an_installed_checkpoint_leaves_the_data_dir_marked, +23 more |
| `frogdb_server::connection::observability_conn_command::tests::arg` | server/src/connection/observability_conn_command.rs:984 | 30 | 26 | frogdb_server::connection::observability_conn_command::tests::latency_bands_disabled_by_default_errors, frogdb_server::connection::observability_conn_command::tests::latency_graph_missing_event_errors, frogdb_server::connection::observability_conn_command::tests::latency_help_lists_subcommands, +23 more |
| `frogdb_search::query::tests::parse_ast` | search/src/query.rs:1015 | 25 | 25 | frogdb_search::query::tests::test_and_terms, frogdb_search::query::tests::test_boolean_or_parens, frogdb_search::query::tests::test_exclusive_numeric_range, +22 more |
| `<frogdb_core::persistence::test_harness::CrashTestHarness>::rocks` | core/src/persistence/test_harness.rs:108 | 11843 | 24 | frogdb_core::persistence::crash_recovery_tests::atomicity::test_single_key_atomic, frogdb_core::persistence::crash_recovery_tests::disk_failure::test_binary_data_crash_recovery, frogdb_core::persistence::crash_recovery_tests::disk_failure::test_unicode_data_crash_recovery, +21 more |
| `<frogdb_core::persistence::test_harness::CrashTestHarness>::crash` | core/src/persistence/test_harness.rs:183 | 24 | 24 | frogdb_core::persistence::crash_recovery_tests::atomicity::test_single_key_atomic, frogdb_core::persistence::crash_recovery_tests::disk_failure::test_binary_data_crash_recovery, frogdb_core::persistence::crash_recovery_tests::disk_failure::test_unicode_data_crash_recovery, +21 more |
| `<frogdb_core::persistence::test_harness::CrashTestHarness>::reopen` | core/src/persistence/test_harness.rs:197 | 24 | 24 | frogdb_core::persistence::crash_recovery_tests::atomicity::test_single_key_atomic, frogdb_core::persistence::crash_recovery_tests::disk_failure::test_binary_data_crash_recovery, frogdb_core::persistence::crash_recovery_tests::disk_failure::test_unicode_data_crash_recovery, +21 more |
| `frogdb_core::scripting::gate::tests::detached_gate` | core/src/scripting/gate.rs:548 | 24 | 24 | frogdb_core::scripting::gate::tests::classify_allows_read_in_readonly, frogdb_core::scripting::gate::tests::classify_cross_slot_span_allowed_when_not_enforced, frogdb_core::scripting::gate::tests::classify_cross_slot_span_rejected_when_enforced, +21 more |
| `frogdb_core::scripting::gate::tests::part` | core/src/scripting/gate.rs:566 | 53 | 24 | frogdb_core::scripting::gate::tests::classify_allows_read_in_readonly, frogdb_core::scripting::gate::tests::classify_cross_slot_span_allowed_when_not_enforced, frogdb_core::scripting::gate::tests::classify_cross_slot_span_rejected_when_enforced, +21 more |
| `frogdb_server::role_manager::tests::addr` | server/src/role_manager.rs:874 | 26 | 24 | frogdb_server::role_manager::tests::a_no_op_demotion_does_not_end_the_stint_again, frogdb_server::role_manager::tests::boot_target_seeds_primary_target, frogdb_server::role_manager::tests::demote_ends_the_primary_stint_while_the_node_is_already_fenced, +21 more |
| `frogdb_types::types::stream::claim_tests::name` | types/src/types/stream.rs:1965 | 115 | 24 | frogdb_types::types::stream::claim_tests::autoclaim_scan_filters_min_idle_and_paginates, frogdb_types::types::stream::claim_tests::autoclaim_scan_skips_below_min_idle, frogdb_types::types::stream::claim_tests::claim_creates_missing_target_consumer, +21 more |
| `frogdb_types::types::stream::claim_tests::assert_invariant` | types/src/types/stream.rs:1975 | 40 | 24 | frogdb_types::types::stream::claim_tests::autoclaim_scan_filters_min_idle_and_paginates, frogdb_types::types::stream::claim_tests::autoclaim_scan_skips_below_min_idle, frogdb_types::types::stream::claim_tests::claim_creates_missing_target_consumer, +21 more |
| `frogdb_types::types::stream::claim_tests::group_with` | types/src/types/stream.rs:1986 | 24 | 24 | frogdb_types::types::stream::claim_tests::autoclaim_scan_filters_min_idle_and_paginates, frogdb_types::types::stream::claim_tests::autoclaim_scan_skips_below_min_idle, frogdb_types::types::stream::claim_tests::claim_creates_missing_target_consumer, +21 more |
| `<frogdb_core::persistence::test_harness::CrashTestHarness>::recover` | core/src/persistence/test_harness.rs:208 | 23 | 23 | frogdb_core::persistence::crash_recovery_tests::atomicity::test_single_key_atomic, frogdb_core::persistence::crash_recovery_tests::disk_failure::test_binary_data_crash_recovery, frogdb_core::persistence::crash_recovery_tests::disk_failure::test_unicode_data_crash_recovery, +20 more |
| `frogdb_types::types::stream::claim_tests::id` | types/src/types/stream.rs:1969 | 37 | 23 | frogdb_types::types::stream::claim_tests::autoclaim_scan_filters_min_idle_and_paginates, frogdb_types::types::stream::claim_tests::autoclaim_scan_skips_below_min_idle, frogdb_types::types::stream::claim_tests::claim_creates_missing_target_consumer, +20 more |
| `frogdb_replication::apply::tests::frame_on` | replication/src/apply.rs:734 | 583 | 22 | frogdb_replication::apply::tests::a_continue_resume_still_applies_the_frames_it_left_queued, frogdb_replication::apply::tests::a_diverged_applier_resumes_on_the_history_a_resync_installs, frogdb_replication::apply::tests::a_failed_apply_stops_the_history_it_happened_on, +19 more |
| `frogdb_replication::apply::tests::frame_on::{closure#0}` | replication/src/apply.rs:737 | 1121 | 22 | frogdb_replication::apply::tests::a_continue_resume_still_applies_the_frames_it_left_queued, frogdb_replication::apply::tests::a_diverged_applier_resumes_on_the_history_a_resync_installs, frogdb_replication::apply::tests::a_failed_apply_stops_the_history_it_happened_on, +19 more |
| `frogdb_replication::replica_session::tests::read_response_line` | replication/src/replica_session.rs:3304 | 48 | 22 | frogdb_replication::replica_session::tests::a_full_sync_whose_handoff_window_is_evicted_abandons_the_link, frogdb_replication::replica_session::tests::a_lag_disconnect_is_a_lost_departure, frogdb_replication::replica_session::tests::a_new_streaming_generation_clears_the_previous_departure, +19 more |
| `frogdb_replication::replica_session::tests::read_response_line::{closure#0}` | replication/src/replica_session.rs:3304 | 48 | 22 | frogdb_replication::replica_session::tests::a_full_sync_whose_handoff_window_is_evicted_abandons_the_link, frogdb_replication::replica_session::tests::a_lag_disconnect_is_a_lost_departure, frogdb_replication::replica_session::tests::a_new_streaming_generation_clears_the_previous_departure, +19 more |
| `<frogdb_commands::basic::SetCommand>::execute_with_if_condition` | commands/src/basic.rs:696 | 23 | 21 | main::string_tcl::tcl_extended_set_case_insensitive_conditions, main::string_tcl::tcl_extended_set_with_ifdeq_key_doesnt_exist, main::string_tcl::tcl_extended_set_with_ifdeq_key_exists_and_digest_matches, +18 more |
| `<frogdb_core::persistence::test_harness::CrashTestHarness>::put_direct` | core/src/persistence/test_harness.rs:130 | 11210 | 21 | frogdb_core::persistence::crash_recovery_tests::disk_failure::test_binary_data_crash_recovery, frogdb_core::persistence::crash_recovery_tests::disk_failure::test_unicode_data_crash_recovery, frogdb_core::persistence::crash_recovery_tests::durability_mode::test_async_mode_explicit_flush, +18 more |
| `<frogdb_core::shard::execution::scatter_effect_tests::MockSet as frogdb_core::command::Command>::spec` | core/src/shard/execution.rs:1254 | 51 | 21 | frogdb_core::shard::dispatch_replication::tests::export_of_an_empty_shard_is_an_empty_blob, frogdb_core::shard::dispatch_replication::tests::export_snapshot_drops_expired_keys, frogdb_core::shard::dispatch_replication::tests::export_snapshot_round_trips_through_install, +18 more |
| `<frogdb_core::shard::execution::scatter_effect_tests::MockDel as frogdb_core::command::Command>::spec` | core/src/shard/execution.rs:1288 | 51 | 21 | frogdb_core::shard::dispatch_replication::tests::export_of_an_empty_shard_is_an_empty_blob, frogdb_core::shard::dispatch_replication::tests::export_snapshot_drops_expired_keys, frogdb_core::shard::dispatch_replication::tests::export_snapshot_round_trips_through_install, +18 more |
| `<frogdb_core::shard::execution::scatter_effect_tests::MockFlushDb as frogdb_core::command::Command>::spec` | core/src/shard/execution.rs:1322 | 69 | 21 | frogdb_core::shard::dispatch_replication::tests::export_of_an_empty_shard_is_an_empty_blob, frogdb_core::shard::dispatch_replication::tests::export_snapshot_drops_expired_keys, frogdb_core::shard::dispatch_replication::tests::export_snapshot_round_trips_through_install, +18 more |
| `frogdb_core::shard::execution::scatter_effect_tests::scatter_worker` | core/src/shard/execution.rs:1357 | 22 | 21 | frogdb_core::shard::dispatch_replication::tests::export_of_an_empty_shard_is_an_empty_blob, frogdb_core::shard::dispatch_replication::tests::export_snapshot_drops_expired_keys, frogdb_core::shard::dispatch_replication::tests::export_snapshot_round_trips_through_install, +18 more |
| `frogdb_replication::apply::tests::live` | replication/src/apply.rs:764 | 576 | 21 | frogdb_replication::apply::tests::a_diverged_applier_resumes_on_the_history_a_resync_installs, frogdb_replication::apply::tests::a_failed_apply_stops_the_history_it_happened_on, frogdb_replication::apply::tests::a_frame_stepped_over_inside_a_group_still_rides_with_its_claim, +18 more |
| `<frogdb_server::connection::guards::tests::ViewFixture>::new` | server/src/connection/guards.rs:1131 | 22 | 21 | frogdb_server::connection::guards::tests::migrating_source_asks_a_single_key_write_whose_key_moved, frogdb_server::connection::guards::tests::migrating_source_probe_is_skipped_for_node_scoped_commands, frogdb_server::connection::guards::tests::migrating_source_probe_is_skipped_when_the_slot_is_not_migrating, +18 more |
| `<frogdb_server::connection::guards::tests::ViewFixture>::view` | server/src/connection/guards.rs:1153 | 62 | 21 | frogdb_server::connection::guards::tests::migrating_source_asks_a_single_key_write_whose_key_moved, frogdb_server::connection::guards::tests::migrating_source_probe_is_skipped_for_node_scoped_commands, frogdb_server::connection::guards::tests::migrating_source_probe_is_skipped_when_the_slot_is_not_migrating, +18 more |
| `frogdb_commands::string::expiry_grammar_pin_tests::ctx` | commands/src/string.rs:1574 | 20 | 20 | frogdb_commands::string::expiry_grammar_pin_tests::getex_ex_negative_message, frogdb_commands::string::expiry_grammar_pin_tests::getex_ex_secs_overflow_rejected, frogdb_commands::string::expiry_grammar_pin_tests::getex_ex_zero_message, +17 more |
| `frogdb_commands::string::expiry_grammar_pin_tests::args` | commands/src/string.rs:1580 | 20 | 20 | frogdb_commands::string::expiry_grammar_pin_tests::getex_ex_negative_message, frogdb_commands::string::expiry_grammar_pin_tests::getex_ex_secs_overflow_rejected, frogdb_commands::string::expiry_grammar_pin_tests::getex_ex_zero_message, +17 more |
| `frogdb_commands::string::expiry_grammar_pin_tests::args::{closure#0}` | commands/src/string.rs:1581 | 74 | 20 | frogdb_commands::string::expiry_grammar_pin_tests::getex_ex_negative_message, frogdb_commands::string::expiry_grammar_pin_tests::getex_ex_secs_overflow_rejected, frogdb_commands::string::expiry_grammar_pin_tests::getex_ex_zero_message, +17 more |
| `<frogdb_core::persistence::test_harness::CrashTestHarness>::new` | core/src/persistence/test_harness.rs:46 | 20 | 20 | frogdb_core::persistence::crash_recovery_tests::atomicity::test_single_key_atomic, frogdb_core::persistence::crash_recovery_tests::disk_failure::test_binary_data_crash_recovery, frogdb_core::persistence::crash_recovery_tests::disk_failure::test_unicode_data_crash_recovery, +17 more |
| `<frogdb_core::persistence::test_harness::CrashTestHarness>::flush` | core/src/persistence/test_harness.rs:170 | 22 | 20 | frogdb_core::persistence::crash_recovery_tests::disk_failure::test_binary_data_crash_recovery, frogdb_core::persistence::crash_recovery_tests::disk_failure::test_unicode_data_crash_recovery, frogdb_core::persistence::crash_recovery_tests::durability_mode::test_async_mode_explicit_flush, +17 more |
| `frogdb_persistence::wal::tests::take_one` | persistence/src/wal/tests.rs:300 | 71 | 20 | frogdb_persistence::wal::tests::a_partial_batch_older_than_the_timeout_meets_the_flush_trigger, frogdb_persistence::wal::tests::an_oversized_entry_closes_its_batch_instead_of_pulling_the_queue_in, frogdb_persistence::wal::tests::repeated_flush_failures_log_once_at_error_and_then_at_debug, +17 more |
| `frogdb_persistence::wal::tests::take_one::{closure#0}` | persistence/src/wal/tests.rs:302 | 71 | 20 | frogdb_persistence::wal::tests::a_partial_batch_older_than_the_timeout_meets_the_flush_trigger, frogdb_persistence::wal::tests::an_oversized_entry_closes_its_batch_instead_of_pulling_the_queue_in, frogdb_persistence::wal::tests::repeated_flush_failures_log_once_at_error_and_then_at_debug, +17 more |
| `frogdb_test_harness::server::parse_simple_string` | test-harness/src/server.rs:1357 | 24 | 20 | main::integration_replication::test_fullresync_interrupted_resume::case_1_in_memory, main::integration_replication::test_fullresync_interrupted_resume::case_2_with_persistence, main::integration_replication::test_min_replicas_to_write_gate_tracks_replica_health, +17 more |
| `frogdb_vll::shard::tests::channels` | vll/src/shard.rs:514 | 8040 | 20 | frogdb_vll::shard::tests::abort_of_pending_op_removes_it_from_sca_ordering, frogdb_vll::shard::tests::abort_releases_intents_and_advances_waiters, frogdb_vll::shard::tests::continuation_grant_skipped_when_the_requester_gave_up, +17 more |
| `frogdb_commands::hash::expiry_grammar_pin_tests::ctx` | commands/src/hash.rs:2104 | 20 | 19 | frogdb_commands::hash::expiry_grammar_pin_tests::hexpire_above_bound_rejected, frogdb_commands::hash::expiry_grammar_pin_tests::hexpireat_above_bound_rejected, frogdb_commands::hash::expiry_grammar_pin_tests::hgetex_ex_secs_overflow_rejected, +16 more |
| `frogdb_commands::hash::expiry_grammar_pin_tests::args` | commands/src/hash.rs:2110 | 20 | 19 | frogdb_commands::hash::expiry_grammar_pin_tests::hexpire_above_bound_rejected, frogdb_commands::hash::expiry_grammar_pin_tests::hexpireat_above_bound_rejected, frogdb_commands::hash::expiry_grammar_pin_tests::hgetex_ex_secs_overflow_rejected, +16 more |

_showing 60 of 1679; full list in `depth.json`._

## Hot but shallow

The class that justifies this whole exercise: enormous exec counts, almost no test breadth. Both today's coverage percentage and raw exec counts report these as healthy.

| function | location | exec | tests | covering tests |
|---|---|---:|---:|---|
| `frogdb_test_harness::response::extract_bulk_strings::{closure#0}` | test-harness/src/response.rs:95 | 41301 | 2 | main::keyspace_tcl::tcl_untagged_multi_key_commands, main::scripting_tcl::tcl_eval_redis_integer_to_lua_type |
| `<frogdb_server::connection::ConnectionHandler>::dispatch_scatter::{closure#0}::{closure#0}` | server/src/connection/routing.rs:164 | 35608 | 3 | main::integration_client::test_tracking_bcast_scatter_mset_invalidation, main::integration_persistence::test_checkpoint_cross_shard_mset_contract_under_concurrent_bgsave, main::integration_transactions::test_multi_cross_shard_crossslot_with_allow_cross_slot_standalone |

## Cold lines (`count == 1`)

Lines executed exactly once across the entire suite — almost always an incidental touch on the way to something else, not a tested path.

| file | cold lines | first few |
|---|---:|---|
| cluster/src/state.rs | 1910 | 725, 733, 883, 884, 885, 890, 891, 1022, 1023, 1024, … |
| replication/src/replica_session.rs | 1493 | 492, 493, 494, 607, 816, 817, 818, 819, 993, 1290, … |
| server/src/runtime_config.rs | 1151 | 109, 110, 504, 1151, 1152, 1153, 1233, 1238, 1239, 1240, … |
| persistence/src/wal/tests.rs | 977 | 19, 20, 21, 22, 23, 24, 25, 27, 28, 29, … |
| recovery/src/tests.rs | 813 | 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, … |
| persistence/src/rocks/tests.rs | 735 | 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, … |
| search/src/aggregate.rs | 718 | 156, 331, 333, 334, 335, 336, 339, 341, 342, 343, … |
| core/src/store/hashmap.rs | 645 | 379, 553, 556, 870, 871, 872, 1000, 1001, 1002, 1003, … |
| persistence/src/snapshot/tests.rs | 639 | 9, 10, 11, 12, 18, 19, 20, 21, 23, 24, … |
| server/src/info/sections.rs | 637 | 95, 96, 97, 98, 101, 102, 103, 104, 519, 520, … |
| core/src/persistence/tests.rs | 606 | 20, 21, 22, 25, 26, 27, 28, 29, 30, 31, … |
| testing/src/conservation.rs | 604 | 163, 164, 165, 166, 167, 168, 169, 170, 592, 606, … |
| replication/src/primary/tests.rs | 590 | 74, 78, 79, 83, 84, 89, 90, 91, 92, 93, … |
| types/src/types/mod.rs | 585 | 164, 165, 166, 169, 170, 171, 174, 175, 176, 312, … |
| replication/src/replica/connection.rs | 568 | 83, 88, 244, 245, 280, 281, 282, 283, 310, 311, … |
| core/src/persistence/crash_recovery_tests.rs | 503 | 46, 47, 50, 58, 61, 62, 78, 86, 87, 90, … |
| core/src/shard/post_execution.rs | 498 | 99, 103, 104, 105, 106, 780, 783, 785, 793, 794, … |
| search/src/index.rs | 496 | 165, 167, 173, 174, 343, 482, 519, 525, 531, 532, … |
| replication/src/apply.rs | 495 | 381, 382, 405, 406, 503, 504, 506, 507, 511, 513, … |
| server/src/connection/state.rs | 485 | 122, 223, 224, 225, 226, 227, 589, 1130, 1131, 1132, … |
| core/src/shard/blocking.rs | 471 | 147, 278, 284, 444, 445, 447, 448, 450, 451, 514, … |
| core/src/shard/execution.rs | 457 | 441, 454, 455, 456, 457, 458, 459, 460, 461, 462, … |
| core/src/command_spec.rs | 456 | 69, 99, 120, 130, 430, 437, 454, 459, 463, 655, … |
| core/src/client_registry/mod.rs | 448 | 143, 463, 464, 465, 701, 786, 787, 788, 789, 790, … |
| protocol/src/response.rs | 446 | 237, 238, 239, 247, 248, 249, 252, 253, 254, 307, … |
| cluster-runtime/src/failure_detector.rs | 418 | 523, 551, 555, 626, 627, 638, 639, 644, 652, 654, … |
| types/src/types/stream.rs | 416 | 32, 33, 34, 35, 36, 37, 143, 148, 194, 195, … |
| telemetry/src/status.rs | 403 | 786, 787, 788, 789, 790, 791, 792, 793, 794, 796, … |
| vll/src/shard.rs | 390 | 159, 160, 161, 162, 163, 353, 431, 432, 433, 434, … |
| replication/src/fullsync.rs | 388 | 76, 77, 78, 79, 232, 233, 236, 255, 256, 259, … |
| commands/src/sort.rs | 383 | 135, 302, 303, 308, 315, 316, 379, 393, 567, 568, … |
| cluster/src/commands.rs | 382 | 195, 198, 199, 200, 255, 268, 269, 270, 271, 289, … |
| replication/src/frame.rs | 370 | 296, 297, 298, 299, 300, 301, 302, 303, 304, 325, … |
| search/src/expression.rs | 364 | 69, 226, 227, 228, 229, 230, 235, 236, 242, 243, … |
| server/src/server/cluster_init.rs | 363 | 224, 831, 832, 838, 970, 1008, 1103, 1104, 1105, 1106, … |
| core/src/pubsub.rs | 360 | 119, 123, 195, 413, 414, 415, 577, 579, 614, 615, … |
| server/src/role_manager.rs | 343 | 586, 588, 589, 590, 591, 905, 933, 934, 936, 938, … |
| server/src/slot_migration/tests.rs | 343 | 51, 52, 53, 55, 56, 57, 61, 62, 63, 64, … |
| core/src/scripting/gate.rs | 337 | 227, 234, 279, 464, 465, 466, 467, 582, 583, 584, … |
| server/src/server/register.rs | 336 | 298, 306, 311, 323, 328, 341, 347, 349, 350, 351, … |
| server/src/connection/search/merge.rs | 334 | 43, 44, 45, 171, 172, 189, 221, 222, 268, 327, … |
| replication/src/tracker.rs | 329 | 558, 614, 615, 616, 617, 618, 619, 620, 621, 622, … |
| testing/src/models/kv.rs | 322 | 43, 138, 145, 147, 148, 149, 150, 151, 156, 248, … |
| replication/src/wait_coordinator.rs | 319 | 308, 309, 310, 311, 313, 314, 315, 316, 317, 318, … |
| cluster/src/network.rs | 318 | 389, 390, 391, 392, 393, 394, 395, 396, 397, 398, … |
| telemetry/src/tracing.rs | 310 | 466, 467, 469, 470, 517, 518, 519, 523, 524, 526, … |
| replication/src/state.rs | 306 | 60, 64, 76, 136, 143, 148, 149, 244, 247, 248, … |
| server/src/scatter/broadcast.rs | 305 | 170, 172, 178, 208, 214, 220, 226, 228, 324, 325, … |
| server/src/connection/codec.rs | 304 | 184, 204, 339, 340, 438, 443, 454, 460, 461, 564, … |
| server/src/config/mod.rs | 294 | 59, 60, 62, 63, 64, 181, 182, 183, 184, 185, … |
| types/src/json.rs | 291 | 202, 291, 292, 294, 295, 296, 303, 446, 554, 604, … |
| types/src/vectorset.rs | 291 | 79, 80, 81, 82, 83, 84, 85, 87, 88, 89, … |
| core/src/scripting/executor.rs | 287 | 65, 66, 67, 190, 192, 193, 194, 271, 283, 385, … |
| cluster/src/storage.rs | 282 | 124, 229, 230, 231, 232, 234, 353, 354, 355, 384, … |
| persistence/src/serialization/probabilistic.rs | 279 | 338, 339, 340, 514, 532, 588, 618, 722, 723, 724, … |
| replication/src/replica/tests.rs | 279 | 17, 18, 19, 20, 23, 24, 25, 26, 27, 28, … |
| telemetry/src/testing.rs | 277 | 185, 322, 534, 535, 536, 683, 684, 685, 686, 693, … |
| search/src/schema.rs | 274 | 220, 221, 282, 283, 284, 285, 332, 333, 337, 338, … |
| server/src/connection/observability_conn_command.rs | 273 | 156, 316, 387, 469, 474, 475, 501, 549, 574, 575, … |
| debug/src/web_ui/handlers.rs | 266 | 61, 62, 64, 65, 66, 67, 68, 69, 70, 71, … |

_showing 60 of 437 files; full list in `depth.json`._

