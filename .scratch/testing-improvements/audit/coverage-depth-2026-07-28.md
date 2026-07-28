# Coverage depth (per-line exec counts + per-function test diversity, 2026-07-28)

7258 per-test profiles joined against one aggregate `llvm-cov export`; name join hit-rate 100.00%.
Toolchain: rustc 1.92.0 (ded5c06cf 2025-12-08) — stable 1.92.0: -Z coverage-options=branch and MC/DC are nightly-only; region coverage reported instead.

## Totals

Lines 85.0% (116592/137185) · Regions 86.1% (195312/226745)

De-duplicated per-file line view: 113496/132407 (85.7%). The totals above are llvm-cov's own per-file summaries, which sum *per function*, so a line in several function records is counted once per function; the two differ in 419/534 files. The de-duplicated figure is what the HTML gutter shows and matches `llvm-cov export --format=lcov` exactly.

| class | functions | meaning |
|---|---:|---|
| `untested` | 14849 | no test reaches it at all |
| `single-test` | 6475 | one test is the entire safety net |
| `monoculture` | 4325 | reached by >1 test but only one suite |
| `hot-but-shallow` | 13 | exec_total >= 5636 but <= 3 tests |
| `covered` | 1249 | middling breadth |
| `well-covered` | 8329 | >= 5 tests across >= 2 suites |

## Per-crate

| crate | lines | line % | regions | region % |
|---|---:|---:|---:|---:|
| config-derive | 0/199 | 0.0% | 0/296 | 0.0% |
| deb | 0/139 | 0.0% | 0/267 | 0.0% |
| docs-gen | 0/404 | 0.0% | 0/666 | 0.0% |
| frogdb-macros | 0/217 | 0.0% | 0/404 | 0.0% |
| grafana | 0/477 | 0.0% | 0/633 | 0.0% |
| helm | 0/224 | 0.0% | 0/332 | 0.0% |
| metrics-derive | 0/191 | 0.0% | 0/335 | 0.0% |
| frogdb-admin | 84/239 | 35.1% | 138/384 | 35.9% |
| frogctl | 2153/4370 | 49.3% | 3672/7479 | 49.1% |
| debug | 1660/2559 | 64.9% | 2082/3091 | 67.4% |
| tokio-coz | 725/985 | 73.6% | 1228/1633 | 75.2% |
| scripting | 1634/1970 | 82.9% | 2739/3387 | 80.9% |
| server | 27000/32028 | 84.3% | 41281/48099 | 85.8% |
| commands | 11652/13712 | 85.0% | 20837/23945 | 87.0% |
| protocol | 780/911 | 85.6% | 1270/1499 | 84.7% |
| search | 6022/7007 | 85.9% | 10342/11998 | 86.2% |
| core | 21852/24794 | 88.1% | 37956/42269 | 89.8% |
| cluster | 3380/3787 | 89.3% | 4947/5616 | 88.1% |
| test-harness | 1787/2002 | 89.3% | 2661/2982 | 89.2% |
| vll | 1002/1106 | 90.6% | 1778/1987 | 89.5% |
| types | 10926/11908 | 91.8% | 19053/20714 | 92.0% |
| telemetry | 3495/3802 | 91.9% | 5525/5973 | 92.5% |
| persistence | 6129/6661 | 92.0% | 11345/12338 | 92.0% |
| config | 2436/2640 | 92.3% | 3319/3607 | 92.0% |
| testing | 5191/5599 | 92.7% | 10884/11594 | 93.9% |
| replication | 4853/5200 | 93.3% | 8543/9133 | 93.5% |
| acl | 3831/4054 | 94.5% | 5712/6084 | 93.9% |

## Untested functions

`test_count == 0` — instrumented, instantiated, never entered by any test. Ranked by region count (bigger = more untested logic).

| function | location | exec | tests | covering tests |
|---|---|---:|---:|---|
| `frogdb_commands::register_all` | commands/src/lib.rs:37 | 0 | 0 | — |
| `<frogdb_core::shard::worker::ShardWorker>::execute_ft_hybrid` | core/src/shard/search/query.rs:255 | 0 | 0 | — |
| `<frogdb_server::server::Server>::start_subsystems` | server/src/server/subsystems.rs:78 | 0 | 0 | — |
| `<frogdb_testing::models::stream_group::StreamGroupModel as frogdb_testing::models::Model>::step` | testing/src/models/stream_group.rs:96 | 0 | 0 | — |
| `<frogdb_testing::models::kv::KVModel as frogdb_testing::models::Model>::step` | testing/src/models/kv.rs:27 | 0 | 0 | — |
| `frogdb_server::server::cluster_init::init_cluster::{closure#0}` | server/src/server/cluster_init.rs:88 | 0 | 0 | — |
| `frogdb_server::server::cluster_init::init_cluster::{closure#0}` | server/src/server/cluster_init.rs:88 | 0 | 0 | — |
| `frogdb_server::server::init::init_infrastructure::{closure#0}` | server/src/server/init.rs:108 | 0 | 0 | — |
| `frogdb_server::server::init::init_infrastructure::{closure#0}` | server/src/server/init.rs:108 | 0 | 0 | — |
| `<frogdb_test_harness::server::TestServer>::try_start_with_config::{closure#0}` | test-harness/src/server.rs:410 | 0 | 0 | — |
| `<frogdb_test_harness::server::TestServer>::try_start_with_config::{closure#0}` | test-harness/src/server.rs:410 | 0 | 0 | — |
| `<frogdb_commands::basic::CommandCommand as frogdb_core::command::Command>::execute` | commands/src/basic.rs:137 | 0 | 0 | — |
| `frogdb_acl::categories::data::COMMAND_CATEGORIES::{closure#0}` | acl/src/categories/data.rs:10 | 0 | 0 | — |
| `<frogdb_server::connection::ConnectionHandler>::run::{closure#0}` | server/src/connection.rs:554 | 0 | 0 | — |
| `<frogdb_core::shard::worker::ShardWorker>::execute_scatter_part_body::{closure#0}` | core/src/shard/execution.rs:721 | 0 | 0 | — |
| `<frogdb_commands::vectorset::vadd::VaddCommand as frogdb_core::command::Command>::execute` | commands/src/vectorset/vadd.rs:36 | 0 | 0 | — |
| `frogdb_commands::geo::parse_geosearch_options` | commands/src/geo.rs:866 | 0 | 0 | — |
| `<frogdb_commands::vectorset::vsim::VsimCommand as frogdb_core::command::Command>::execute` | commands/src/vectorset/vsim.rs:37 | 0 | 0 | — |
| `frogdb_testing::conservation::check_pel_conservation` | testing/src/conservation.rs:682 | 0 | 0 | — |
| `frogdb_server::connection::hotkeys::hotkeys_start` | server/src/connection/hotkeys.rs:100 | 0 | 0 | — |
| `<frogdb_server::connection::ConnectionHandler>::process_one_command::{closure#0}` | server/src/connection.rs:300 | 0 | 0 | — |
| `frogctl::ops::config::generate_default_config` | frogctl/src/ops/config.rs:26 | 0 | 0 | — |
| `frogdb_server::server::shards::spawn_shard_workers` | server/src/server/shards.rs:62 | 0 | 0 | — |
| `<frogdb_commands::stream::read::XreadgroupCommand as frogdb_core::command::Command>::execute` | commands/src/stream/read.rs:200 | 0 | 0 | — |
| `frogctl::ops::backup::export_dataset::<_>::{closure#0}` | frogctl/src/ops/backup.rs:76 | 0 | 0 | — |
| `frogctl::ops::backup::export_dataset::<_>::{closure#0}` | frogctl/src/ops/backup.rs:76 | 0 | 0 | — |
| `<frogdb_server::connection::ConnectionHandler>::handle_migrate::{closure#0}` | server/src/connection/persistence_handler.rs:185 | 0 | 0 | — |
| `frogctl::ops::config::validate_config` | frogctl/src/ops/config.rs:121 | 0 | 0 | — |
| `dashboard_gen::generate_dashboard` | ops/grafana/dashboard-gen/src/main.rs:53 | 0 | 0 | — |
| `frogdb_search::expression::format_strftime` | search/src/expression.rs:886 | 0 | 0 | — |
| `<frogdb_server::connection::ConnectionHandler>::run_stage::{closure#0}` | server/src/connection/dispatch.rs:379 | 0 | 0 | — |
| `<frogdb_server::server::Server>::with_listeners::{closure#0}` | server/src/server/mod.rs:244 | 0 | 0 | — |
| `<frogdb_server::server::Server>::with_listeners::{closure#0}` | server/src/server/mod.rs:244 | 0 | 0 | — |
| `<frogdb_commands::sorted_set::basic::ZaddCommand as frogdb_core::command::Command>::execute` | commands/src/sorted_set/basic.rs:40 | 0 | 0 | — |
| `<frogdb_server::connection::ConnectionHandler>::execute_transaction::{closure#0}` | server/src/connection/transaction.rs:141 | 0 | 0 | — |
| `frogdb_testing::workload::gen_list` | testing/src/workload.rs:569 | 0 | 0 | — |
| `<frogdb_testing::models::zset::ZSetModel as frogdb_testing::models::Model>::step` | testing/src/models/zset.rs:21 | 0 | 0 | — |
| `<frogdb_config::Config as frogdb_server::config::loader::ConfigLoader>::load` | server/src/config/loader.rs:68 | 0 | 0 | — |
| `<frogdb_server::connection::ConnectionHandler>::handle_migrate_command::{closure#0}` | server/src/connection/persistence_handler.rs:30 | 0 | 0 | — |
| `<frogdb_server::connection::ConnectionHandler>::handle_migrate_command::{closure#0}` | server/src/connection/persistence_handler.rs:30 | 0 | 0 | — |
| `<frogdb_testing::models::hash::HashModel as frogdb_testing::models::Model>::step` | testing/src/models/hash.rs:21 | 0 | 0 | — |
| `<frogdb_commands::string::LcsCommand as frogdb_core::command::Command>::execute` | commands/src/string.rs:982 | 0 | 0 | — |
| `<frogdb_commands::stream::pending::XclaimCommand as frogdb_core::command::Command>::execute` | commands/src/stream/pending.rs:154 | 0 | 0 | — |
| `<frogdb_server::commands::search::FtSuggetCommand as frogdb_core::command::Command>::execute` | server/src/commands/search.rs:971 | 0 | 0 | — |
| `<frogdb_commands::sorted_set::store_remove::ZrangestoreCommand as frogdb_core::command::Command>::execute` | commands/src/sorted_set/store_remove.rs:42 | 0 | 0 | — |
| `<frogdb_commands::stream::pending::XautoclaimCommand as frogdb_core::command::Command>::execute` | commands/src/stream/pending.rs:320 | 0 | 0 | — |
| `<frogdb_commands::json::basic::JsonGetCommand as frogdb_core::command::Command>::execute` | commands/src/json/basic.rs:149 | 0 | 0 | — |
| `frogdb_debug::web_ui::routes::handle_debug_request::{closure#0}` | debug/src/web_ui/routes.rs:34 | 0 | 0 | — |
| `frogctl::commands::stat::run::{closure#0}` | frogctl/src/commands/stat.rs:29 | 0 | 0 | — |
| `frogctl::commands::stat::run::{closure#0}` | frogctl/src/commands/stat.rs:29 | 0 | 0 | — |
| `frogdb_commands::timeseries::execute_range` | commands/src/timeseries.rs:814 | 0 | 0 | — |
| `<frogdb_core::shard::worker::ShardWorker>::execute_command_body::{closure#0}` | core/src/shard/execution.rs:125 | 0 | 0 | — |
| `frogdb_commands::stream::info::xinfo_stream` | commands/src/stream/info.rs:65 | 0 | 0 | — |
| `<frogdb_core::shard::worker::ShardWorker>::dispatch_observability` | core/src/shard/dispatch_observability.rs:6 | 0 | 0 | — |
| `<frogdb_commands::sorted_set::range::ZrangeCommand as frogdb_core::command::Command>::execute` | commands/src/sorted_set/range.rs:40 | 0 | 0 | — |
| `<frogdb_server::connection::ConnectionHandler>::handle_scan::{closure#0}` | server/src/connection/scatter.rs:43 | 0 | 0 | — |
| `frogdb_core::shard::timeseries_execution::parse_mrange_args` | core/src/shard/timeseries_execution.rs:182 | 0 | 0 | — |
| `frogdb_server::connection::client_conn_command::client_kill` | server/src/connection/client_conn_command.rs:348 | 0 | 0 | — |
| `<frogdb_vll::coordinator::VllCoordinator<_, _>>::scatter::{closure#0}` | vll/src/coordinator.rs:209 | 0 | 0 | — |
| `frogdb_commands::geo::execute_geosearch` | commands/src/geo.rs:1121 | 0 | 0 | — |

_showing 60 of 14849; full list in `depth.json`._

## Single-test functions

One test is the entire safety net. Deleting or weakening that test silently removes all coverage of this function.

| function | location | exec | tests | covering tests |
|---|---|---:|---:|---|
| `frogdb_persistence::recovery::tests::round_trips_format_through_mock_sink` | persistence/src/recovery.rs:239 | 1 | 1 | frogdb_persistence::recovery::tests::round_trips_format_through_mock_sink |
| `<frogdb_testing::models::list::ListModel as frogdb_testing::models::Model>::step` | testing/src/models/list.rs:47 | 3 | 1 | public_api::oracle_api_is_reachable_from_root |
| `frogdb_acl::categories::data::COMMAND_CATEGORIES::{closure#0}` | acl/src/categories/data.rs:10 | 1 | 1 | frogdb_acl::categories::tests::test_command_category |
| `frogdb_commands::hash::execute_hexpire_common::<<frogdb_commands::hash::HexpireCommand as frogdb_core::command` | commands/src/hash.rs:987 | 1 | 1 | frogdb_commands::hash::expiry_grammar_pin_tests::hexpire_above_bound_rejected |
| `frogdb_commands::hash::execute_hexpire_common::<<frogdb_commands::hash::HexpireatCommand as frogdb_core::comma` | commands/src/hash.rs:987 | 1 | 1 | frogdb_commands::hash::expiry_grammar_pin_tests::hexpireat_above_bound_rejected |
| `frogdb_commands::hash::execute_hexpire_common::<<frogdb_commands::hash::HpexpireCommand as frogdb_core::comman` | commands/src/hash.rs:987 | 1 | 1 | frogdb_commands::hash::expiry_grammar_pin_tests::hpexpire_above_bound_rejected |
| `<frogdb_config::Config as frogdb_server::config::loader::ConfigLoader>::load` | server/src/config/loader.rs:68 | 1 | 1 | frogdb_server::config::tests::test_load_explicit_config_file_not_found |
| `frogdb_persistence::serialization::registry::tests::samples_for` | persistence/src/serialization/registry.rs:415 | 17 | 1 | frogdb_persistence::serialization::registry::tests::every_marker_round_trips |
| `frogdb_core::persistence::tests::integration::test_mixed_types_recovery` | core/src/persistence/tests.rs:557 | 1 | 1 | frogdb_core::persistence::tests::integration::test_mixed_types_recovery |
| `frogdb_replication::replica_session::tests::fullresync_offset_and_metadata_come_from_live_tracker::{closure#0}` | replication/src/replica_session.rs:1454 | 1 | 1 | frogdb_replication::replica_session::tests::fullresync_offset_and_metadata_come_from_live_tracker |
| `frogdb_testing::partition::project_for_key` | testing/src/partition.rs:117 | 4 | 1 | public_api::oracle_api_is_reachable_from_root |
| `frogdb_testing::conservation::check_exactly_once_delivery` | testing/src/conservation.rs:121 | 1 | 1 | public_api::oracle_api_is_reachable_from_root |
| `<frogdb_persistence::rocks::RocksStore>::open_with_cf_lister::<frogdb_persistence::rocks::tests::test_cf_enume` | persistence/src/rocks/mod.rs:124 | 1 | 1 | frogdb_persistence::rocks::tests::test_cf_enumeration_failure_propagates_and_preserves_data |
| `<frogdb_telemetry::prometheus_recorder::PrometheusRecorder>::dashboard_snapshot` | telemetry/src/prometheus_recorder.rs:343 | 1 | 1 | frogdb_telemetry::prometheus_recorder::tests::test_dashboard_snapshot_reads_typed_emissions |
| `frogdb_persistence::serialization::search::deserialize_vectorset` | persistence/src/serialization/search.rs:73 | 3 | 1 | main::proptest_serialization::random_type_byte_doesnt_panic |
| `frogdb_cluster::network::tests::test_all_rpc_variants_roundtrip` | cluster/src/network.rs:827 | 1 | 1 | frogdb_cluster::network::tests::test_all_rpc_variants_roundtrip |
| `frogdb_server::server::cluster_init::tests::split_brain_lifecycle_captures_audit_and_initiates_discard` | server/src/server/cluster_init.rs:1098 | 1 | 1 | frogdb_server::server::cluster_init::tests::split_brain_lifecycle_captures_audit_and_initiates_discard |
| `<frogdb_commands::cuckoo::CfLoadchunk as frogdb_core::command::Command>::execute` | commands/src/cuckoo.rs:653 | 1 | 1 | main::bloom_regression::cf_scandump_loadchunk_roundtrip |
| `frogdb_persistence::serialization::unit_tests::copy_codec_round_trips_all_value_variants` | persistence/src/serialization/mod.rs:258 | 1 | 1 | frogdb_persistence::serialization::unit_tests::copy_codec_round_trips_all_value_variants |
| `frogdb_search::aggregate::tests::test_parse_new_reducers` | search/src/aggregate.rs:1270 | 1 | 1 | frogdb_search::aggregate::tests::test_parse_new_reducers |
| `<frogdb_config::Config>::validate` | config/src/lib.rs:299 | 1 | 1 | frogdb_config::tests::test_validate_rejects_invalid_hotshards_section |
| `frogdb_telemetry::status::tests::status_thresholds_are_live` | telemetry/src/status.rs:1476 | 1 | 1 | frogdb_telemetry::status::tests::status_thresholds_are_live |
| `<frogdb_replication::replica::connection::ReplicaConnection>::psync::{closure#0}` | replication/src/replica/connection.rs:163 | 1 | 1 | frogdb_replication::replica::connection::tests::psync_places_live_offset_not_offset_at_save_in_the_request |
| `frogdb_server::connection::observability_conn_command::memory_stats::{closure#0}` | server/src/connection/observability_conn_command.rs:399 | 1 | 1 | main::integration_admin::test_memory_stats |
| `frogdb_server::connection::observability_conn_command::memory_stats::{closure#0}` | server/src/connection/observability_conn_command.rs:399 | 1 | 1 | frogdb_server::connection::observability_conn_command::tests::memory_stats_with_no_shards_returns_array |
| `<frogdb_core::shard::worker::ShardWorker>::dispatch_pubsub` | core/src/shard/dispatch_pubsub.rs:8 | 1 | 1 | frogdb_core::shard::dispatch_pubsub::tests::subscribe_ack_fires_after_registration_is_visible |
| `frogdb_core::latency::generate_latency_graph` | core/src/latency.rs:470 | 1 | 1 | main::integration_admin::test_latency_graph_valid_event |
| `frogdb_persistence::serialization::stream::tests::round_trips_groups_pel_and_consumers` | persistence/src/serialization/stream.rs:305 | 1 | 1 | frogdb_persistence::serialization::stream::tests::round_trips_groups_pel_and_consumers |
| `frogdb_core::persistence::tests::integration::test_roundtrip_persistence` | core/src/persistence/tests.rs:20 | 1 | 1 | frogdb_core::persistence::tests::integration::test_roundtrip_persistence |
| `<frogdb_commands::bloom::BfLoadchunk as frogdb_core::command::Command>::execute` | commands/src/bloom.rs:592 | 1 | 1 | main::bloom_regression::bf_scandump_loadchunk_roundtrip |
| `<frogdb_replication::replica_session::ReplicaSession>::stream_checkpoint::{closure#0}` | replication/src/replica_session.rs:502 | 1 | 1 | frogdb_replication::replica_session::tests::fullresync_offset_and_metadata_come_from_live_tracker |
| `frogdb_persistence::serialization::unit_tests::collection_contents_survive_round_trip` | persistence/src/serialization/mod.rs:375 | 1 | 1 | frogdb_persistence::serialization::unit_tests::collection_contents_survive_round_trip |
| `frogdb_server::connection::codec::tests::decode_edge_cases_table` | server/src/connection/codec.rs:562 | 1 | 1 | frogdb_server::connection::codec::tests::decode_edge_cases_table |
| `frogdb_replication::replica_session::tests::handle_partial_replays_backlog_then_live_tail::{closure#0}` | replication/src/replica_session.rs:1166 | 1 | 1 | frogdb_replication::replica_session::tests::handle_partial_replays_backlog_then_live_tail |
| `frogdb_search::expression::tests::test_timefmt_strftime_basic` | search/src/expression.rs:1319 | 1 | 1 | frogdb_search::expression::tests::test_timefmt_strftime_basic |
| `<frogdb_search::index::ShardSearchIndex>::reopen_with_def` | search/src/index.rs:987 | 1 | 1 | frogdb_search::index::tests::test_reopen_with_def_adds_field |
| `frogdb_replication::replica_session::tests::full_sync_replays_writes_made_during_handoff::{closure#0}` | replication/src/replica_session.rs:1091 | 1 | 1 | frogdb_replication::replica_session::tests::full_sync_replays_writes_made_during_handoff |
| `frogdb_core::shard::execution::scatter_effect_tests::scatter_del_emits_notification_broadcast_and_wakes_waiter` | core/src/shard/execution.rs:1392 | 1 | 1 | frogdb_core::shard::execution::scatter_effect_tests::scatter_del_emits_notification_broadcast_and_wakes_waiter |
| `frogdb_persistence::serialization::probabilistic::deserialize_topk` | persistence/src/serialization/probabilistic.rs:587 | 6 | 1 | main::proptest_serialization::random_type_byte_doesnt_panic |
| `frogdb_testing::partition::default_keys_of` | testing/src/partition.rs:76 | 4 | 1 | public_api::oracle_api_is_reachable_from_root |
| `frogdb_server::connection::observability_conn_command::tests::status_json_renders_from_shared_collector_and_ag` | server/src/connection/observability_conn_command.rs:1263 | 1 | 1 | frogdb_server::connection::observability_conn_command::tests::status_json_renders_from_shared_collector_and_agrees_with_http |
| `frogdb_types::json::create_path` | types/src/json.rs:1131 | 1 | 1 | frogdb_types::json::tests::test_set_nx |
| `frogdb_testing::conservation::tests::pel_cross_stream_not_contaminated` | testing/src/conservation.rs:1430 | 1 | 1 | frogdb_testing::conservation::tests::pel_cross_stream_not_contaminated |
| `frogdb_persistence::rocks::tests::hll_merge_operand_folds_and_survives_reopen` | persistence/src/rocks/tests.rs:73 | 1 | 1 | frogdb_persistence::rocks::tests::hll_merge_operand_folds_and_survives_reopen |
| `frogdb_persistence::wal::tests::test_wal_clear_reclamation_end_to_end::{closure#0}` | persistence/src/wal/tests.rs:703 | 1 | 1 | frogdb_persistence::wal::tests::test_wal_clear_reclamation_end_to_end |
| `<frogdb_server::connection::ConnectionHandler>::handle_ft_profile::{closure#0}` | server/src/connection/search/profile.rs:13 | 1 | 1 | main::search_regression::ft_profile_search |
| `frogdb_replication::fullsync::tests::test_checkpoint_checksum_agreement::{closure#0}` | replication/src/fullsync.rs:608 | 1 | 1 | frogdb_replication::fullsync::tests::test_checkpoint_checksum_agreement |
| `frogdb_core::conn_command::tests::new_defaults_are_placeholders_and_with_full_reads_overrides_them::{closure#0` | core/src/conn_command.rs:1061 | 1 | 1 | frogdb_core::conn_command::tests::new_defaults_are_placeholders_and_with_full_reads_overrides_them |
| `frogdb_core::persistence::crash_recovery_tests::recovery_correctness::test_all_types_crash_recovery` | core/src/persistence/crash_recovery_tests.rs:447 | 1 | 1 | frogdb_core::persistence::crash_recovery_tests::recovery_correctness::test_all_types_crash_recovery |
| `frogdb_testing::partition::tests::default_keys_of_stream_group_ops` | testing/src/partition.rs:441 | 1 | 1 | frogdb_testing::partition::tests::default_keys_of_stream_group_ops |
| `frogdb_cluster::wire::tests::test_shard_views_grouping_and_order` | cluster/src/wire.rs:363 | 1 | 1 | frogdb_cluster::wire::tests::test_shard_views_grouping_and_order |
| `frogdb_core::persistence::crash_recovery_tests::recovery_correctness::test_sorted_set_index_rebuilt` | core/src/persistence/crash_recovery_tests.rs:572 | 1 | 1 | frogdb_core::persistence::crash_recovery_tests::recovery_correctness::test_sorted_set_index_rebuilt |
| `frogdb_search::wire::tests::test_search_request_full_grammar` | search/src/wire.rs:561 | 1 | 1 | frogdb_search::wire::tests::test_search_request_full_grammar |
| `frogdb_server::commands::cluster::cluster_help` | server/src/commands/cluster/mod.rs:600 | 1 | 1 | main::integration_cluster::test_cluster_help_command |
| `frogdb_server::role_manager::tests::promote_stops_registered_boot_replica_handler::{closure#0}` | server/src/role_manager.rs:938 | 1 | 1 | frogdb_server::role_manager::tests::promote_stops_registered_boot_replica_handler |
| `frogdb_persistence::serialization::probabilistic::deserialize_tdigest` | persistence/src/serialization/probabilistic.rs:360 | 6 | 1 | main::proptest_serialization::random_type_byte_doesnt_panic |
| `<frogdb_server::connection::conn_command::FtCursorConnCommand as frogdb_core::conn_command::ConnectionCommand>` | server/src/connection/conn_command.rs:408 | 1 | 1 | main::integration_transactions::test_transaction_conn_command_hotkeys_ftcursor_execute |
| `frogdb_testing::fault_injection::tests::lose_stream_entry_is_caught` | testing/src/fault_injection.rs:307 | 1 | 1 | frogdb_testing::fault_injection::tests::lose_stream_entry_is_caught |
| `<frogdb_core::shard::worker::ShardWorker>::dispatch_message::{closure#0}` | core/src/shard/event_loop.rs:279 | 2 | 1 | frogdb_core::shard::event_loop::seam_reachability_tests::promoted_seams_are_reachable_in_crate |
| `frogdb_scripting::sandbox::msgpack_to_lua` | scripting/src/sandbox.rs:888 | 2 | 1 | frogdb_scripting::sandbox::tests::test_cmsgpack_in_both_modes |

_showing 60 of 6475; full list in `depth.json`._

## Monoculture functions

Reached by several tests, but all from a single suite. High line coverage here hides the fact that only one angle of attack is represented.

| function | location | exec | tests | covering tests |
|---|---|---:|---:|---|
| `<frogdb_test_harness::server::TestServer>::start_primary_with_config` | test-harness/src/server.rs:366 | 182 | 172 | main::integration_replication::test_boot_configured_replica_reports_primary_target::case_1_in_memory, main::integration_replication::test_boot_configured_replica_reports_primary_target::case_2_with_persistence, main::integration_replication::test_broadcast_lag_disconnect_and_resync::case_1_in_memory, +169 more |
| `<frogdb_test_harness::server::TestServer>::start_primary_with_config::{closure#0}` | test-harness/src/server.rs:366 | 182 | 172 | main::integration_replication::test_boot_configured_replica_reports_primary_target::case_1_in_memory, main::integration_replication::test_boot_configured_replica_reports_primary_target::case_2_with_persistence, main::integration_replication::test_broadcast_lag_disconnect_and_resync::case_1_in_memory, +169 more |
| `<frogdb_test_harness::server::TestServer>::start_replica_with_config` | test-harness/src/server.rs:376 | 151 | 127 | main::integration_replication::test_boot_configured_replica_reports_primary_target::case_1_in_memory, main::integration_replication::test_boot_configured_replica_reports_primary_target::case_2_with_persistence, main::integration_replication::test_chained_replication_rejected_sub_replica_never_receives_data::case_1_in_memory, +124 more |
| `<frogdb_test_harness::server::TestServer>::start_replica_with_config::{closure#0}` | test-harness/src/server.rs:379 | 151 | 127 | main::integration_replication::test_boot_configured_replica_reports_primary_target::case_1_in_memory, main::integration_replication::test_boot_configured_replica_reports_primary_target::case_2_with_persistence, main::integration_replication::test_chained_replication_rejected_sub_replica_never_receives_data::case_1_in_memory, +124 more |
| `<frogdb_core::shard::wait_queue::ShardWaitQueue>::new` | core/src/shard/wait_queue.rs:68 | 91 | 89 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +86 more |
| `<frogdb_core::shard::wait_queue::ShardWaitQueue>::with_limits` | core/src/shard/wait_queue.rs:73 | 91 | 89 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +86 more |
| `<frogdb_core::shard::search::lifecycle::IndexLifecycleManager>::new` | core/src/shard/search/lifecycle.rs:80 | 93 | 88 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +85 more |
| `<frogdb_core::shard::active_expiry::ActiveExpiryCoordinator as core::default::Default>::default` | core/src/shard/active_expiry.rs:86 | 89 | 87 | frogdb_core::shard::active_expiry::tests::budget_zero_stops_early_without_deleting_everything, frogdb_core::shard::active_expiry::tests::dedups_multiple_expired_fields_on_one_key, frogdb_core::shard::active_expiry::tests::deletes_all_due_keys_within_budget, +84 more |
| `<frogdb_core::shard::counters::OperationCounters>::new` | core/src/shard/counters.rs:109 | 89 | 87 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +84 more |
| `frogdb_test_harness::server::parse_integer` | test-harness/src/server.rs:1331 | 195 | 86 | main::integration_replication::test_broadcast_lag_disconnect_and_resync::case_1_in_memory, main::integration_replication::test_broadcast_lag_disconnect_and_resync::case_2_with_persistence, main::integration_replication::test_chained_replication_rejected_sub_replica_never_receives_data::case_1_in_memory, +83 more |
| `<frogdb_server::connection::deps::ClusterDeps>::standalone` | server/src/connection/deps.rs:124 | 85 | 85 | frogdb_server::connection::acl_conn_command::tests::acl_cat_lists_categories_and_category_commands, frogdb_server::connection::acl_conn_command::tests::acl_deluser_removes_user, frogdb_server::connection::acl_conn_command::tests::acl_dryrun_denied_command_reports_permission, +82 more |
| `<frogdb_core::shard::types::ShardIdentity>::new` | core/src/shard/types.rs:42 | 87 | 84 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +81 more |
| `<frogdb_core::shard::keyspace_coordinator::KeyspaceNotificationCoordinator>::new` | core/src/shard/keyspace_coordinator.rs:67 | 84 | 82 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +79 more |
| `<frogdb_core::latency::LatencyMonitor>::default_monitor` | core/src/latency.rs:202 | 83 | 81 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +78 more |
| `<frogdb_core::shard::types::ShardObservability>::new` | core/src/shard/types.rs:163 | 83 | 81 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +78 more |
| `<frogdb_core::shard::types::ShardEviction>::new` | core/src/shard/types.rs:282 | 83 | 81 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +78 more |
| `<frogdb_core::shard::types::ShardEviction>::per_shard_limit` | core/src/shard/types.rs:290 | 84 | 81 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +78 more |
| `<frogdb_core::shard::types::ShardPersistence>::new` | core/src/shard/types.rs:363 | 82 | 80 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +77 more |
| `<frogdb_core::shard::types::ShardTracking as core::default::Default>::default` | core/src/shard/types.rs:421 | 82 | 80 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +77 more |
| `<frogdb_core::shard::types::ShardCluster>::new` | core/src/shard/types.rs:564 | 82 | 80 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +77 more |
| `<frogdb_core::shard::types::ShardScripting>::new` | core/src/shard/types.rs:507 | 81 | 79 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +76 more |
| `<frogdb_core::shard::builder::ShardWorkerBuilder>::new` | core/src/shard/builder.rs:126 | 80 | 78 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +75 more |
| `<frogdb_core::shard::builder::ShardWorkerBuilder>::with_message_rx` | core/src/shard/builder.rs:159 | 80 | 78 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +75 more |
| `<frogdb_core::shard::builder::ShardWorkerBuilder>::with_new_conn_rx` | core/src/shard/builder.rs:165 | 80 | 78 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +75 more |
| `<frogdb_core::shard::builder::ShardWorkerBuilder>::with_shard_senders` | core/src/shard/builder.rs:171 | 80 | 78 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +75 more |
| `<frogdb_core::shard::builder::ShardWorkerBuilder>::with_registry` | core/src/shard/builder.rs:177 | 80 | 78 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +75 more |
| `<frogdb_core::shard::builder::ShardWorkerBuilder>::try_build` | core/src/shard/builder.rs:322 | 80 | 78 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +75 more |
| `<frogdb_core::shard::builder::ShardWorkerBuilder>::try_build::{closure#3}` | core/src/shard/builder.rs:350 | 80 | 78 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +75 more |
| `<frogdb_core::shard::builder::ShardWorkerBuilder>::try_build::{closure#4}` | core/src/shard/builder.rs:361 | 80 | 78 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +75 more |
| `<frogdb_core::shard::builder::ShardWorkerBuilder>::try_build::{closure#6}` | core/src/shard/builder.rs:415 | 80 | 78 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +75 more |
| `<frogdb_core::shard::builder::ShardWorkerBuilder>::build` | core/src/shard/builder.rs:469 | 80 | 78 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +75 more |
| `<frogdb_core::shard::message::ShardReceiver>::new` | core/src/shard/message.rs:87 | 80 | 78 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +75 more |
| `<frogdb_vll::shard::VllShardState<frogdb_core::shard::message::ScatterOp> as core::default::Default>::default` | vll/src/shard.rs:52 | 80 | 78 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +75 more |
| `<frogdb_vll::shard::VllShardState<frogdb_core::shard::message::ScatterOp>>::with_max_queue_depth` | vll/src/shard.rs:59 | 80 | 78 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +75 more |
| `<frogdb_core::shard::builder::ShardWorkerBuilder>::with_metrics` | core/src/shard/builder.rs:189 | 75 | 73 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +70 more |
| `<frogdb_core::shard::builder::ShardWorkerBuilder>::with_replication` | core/src/shard/builder.rs:201 | 75 | 73 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +70 more |
| `<frogdb_core::shard::builder::ShardWorkerBuilder>::with_slowlog_id` | core/src/shard/builder.rs:195 | 74 | 72 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +69 more |
| `<frogdb_core::shard::builder::ShardWorkerBuilder>::with_eviction` | core/src/shard/builder.rs:207 | 74 | 72 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +69 more |
| `<frogdb_core::shard::worker::ShardWorker>::with_eviction` | core/src/shard/worker.rs:354 | 74 | 72 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +69 more |
| `<frogdb_server::connection::state::ConnectionState>::new` | server/src/connection/state.rs:644 | 77 | 70 | frogdb_server::connection::auth_conn_command::tests::auth_empty_args_errors, frogdb_server::connection::auth_conn_command::tests::auth_named_user_success_and_wrong_password, frogdb_server::connection::auth_conn_command::tests::hello_auth_clause_authenticates, +67 more |
| `<frogdb_server::connection::state::AuthState as core::default::Default>::default` | server/src/connection/state.rs:219 | 75 | 69 | frogdb_server::connection::auth_conn_command::tests::auth_empty_args_errors, frogdb_server::connection::auth_conn_command::tests::auth_named_user_success_and_wrong_password, frogdb_server::connection::auth_conn_command::tests::hello_auth_clause_authenticates, +66 more |
| `frogdb_cluster::state::tests::test_addr` | cluster/src/state.rs:573 | 266 | 66 | frogdb_cluster::state::tests::complete_migration_emits_event_on_success, frogdb_cluster::state::tests::complete_migration_emits_no_event_on_error, frogdb_cluster::state::tests::force_failover_emits_no_event, +63 more |
| `<frogdb_core::conn_command::ConnCtx>::with_username` | core/src/conn_command.rs:823 | 79 | 63 | frogdb_server::connection::acl_conn_command::tests::acl_cat_lists_categories_and_category_commands, frogdb_server::connection::acl_conn_command::tests::acl_deluser_removes_user, frogdb_server::connection::acl_conn_command::tests::acl_dryrun_denied_command_reports_permission, +60 more |
| `frogdb_server::runtime_config::tests::test_config` | server/src/runtime_config.rs:3566 | 62 | 61 | frogdb_server::runtime_config::tests::batch_size_threshold_set_reaches_the_shared_wal_cell, frogdb_server::runtime_config::tests::cluster_flag_sets_reach_the_live_flags, frogdb_server::runtime_config::tests::hotshard_threshold_sets_enforce_the_section_validator_bounds, +58 more |
| `<frogdb_server::runtime_config::ConfigManager>::set` | server/src/runtime_config.rs:3189 | 161 | 52 | frogdb_server::runtime_config::tests::batch_size_threshold_set_reaches_the_shared_wal_cell, frogdb_server::runtime_config::tests::cluster_flag_sets_reach_the_live_flags, frogdb_server::runtime_config::tests::hotshard_threshold_sets_enforce_the_section_validator_bounds, +49 more |
| `<frogdb_server::runtime_config::ConfigManager>::set::{closure#1}` | server/src/runtime_config.rs:3201 | 11696 | 52 | frogdb_server::runtime_config::tests::batch_size_threshold_set_reaches_the_shared_wal_cell, frogdb_server::runtime_config::tests::cluster_flag_sets_reach_the_live_flags, frogdb_server::runtime_config::tests::hotshard_threshold_sets_enforce_the_section_validator_bounds, +49 more |
| `<frogdb_core::shard::types::ShardIdentity>::shard_id` | core/src/shard/types.rs:53 | 92 | 51 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +48 more |
| `<frogdb_core::shard::worker::ShardWorker>::shard_id` | core/src/shard/worker.rs:206 | 83 | 47 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +44 more |
| `<frogdb_cluster::state::ClusterState>::apply_command::{closure#0}` | cluster/src/commands.rs:39 | 87 | 45 | frogdb_cluster::state::tests::complete_migration_emits_event_on_success, frogdb_cluster::state::tests::complete_migration_emits_no_event_on_error, frogdb_cluster::state::tests::force_failover_emits_no_event, +42 more |
| `<frogdb_cluster::state::ClusterState>::apply_command::{closure#1}` | cluster/src/commands.rs:40 | 84 | 42 | frogdb_cluster::state::tests::complete_migration_emits_event_on_success, frogdb_cluster::state::tests::complete_migration_emits_no_event_on_error, frogdb_cluster::state::tests::force_failover_emits_no_event, +39 more |
| `<frogdb_core::shard::worker::SlotVersions>::bump_slot` | core/src/shard/worker.rs:76 | 71 | 40 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::push_to_live_waiter_still_consumes_element, +37 more |
| `<frogdb_core::shard::worker::ShardWorker>::reindex_shrunk_hash_keys` | core/src/shard/search_hook.rs:101 | 102 | 39 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +36 more |
| `<frogdb_core::shard::worker::ShardWorker>::apply_lazy_purge_effects` | core/src/shard/worker.rs:663 | 100 | 39 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +36 more |
| `<frogdb_core::shard::worker::ShardWorker>::drain_lazy_purge_effects` | core/src/shard/worker.rs:689 | 100 | 39 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +36 more |
| `<frogdb_core::store::hashmap::HashMapStore as frogdb_core::store::Store>::take_lazily_emptied` | core/src/store/hashmap.rs:1172 | 104 | 39 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +36 more |
| `<frogdb_core::store::hashmap::HashMapStore as frogdb_core::store::Store>::take_lazily_expired_fields` | core/src/store/hashmap.rs:1176 | 104 | 39 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +36 more |
| `<frogdb_core::store::hashmap::HashMapStore as frogdb_core::store::Store>::take_lazily_shrunk` | core/src/store/hashmap.rs:1180 | 102 | 39 | frogdb_core::shard::blocking::tests::blmove_cascade_records_ordered_propagations, frogdb_core::shard::blocking::tests::blmove_fanout_stops_at_depth_cap, frogdb_core::shard::blocking::tests::blmpop_restore_preserves_all_elements_in_order, +36 more |
| `<frogdb_core::shard::types::ShardTracking>::has_tracking_clients` | core/src/shard/types.rs:433 | 44 | 37 | frogdb_core::shard::blocking::tests::waiter_satisfaction_drains_lazy_purge_report, frogdb_core::shard::event_loop::effect_tests::active_sweep_emptied_key_does_not_double_fire_del, frogdb_core::shard::event_loop::effect_tests::expired_keys_stat_counts_both_paths_without_double_count, +34 more |
| `<frogdb_types::types::stream::StreamId>::new` | types/src/types/stream.rs:22 | 112 | 36 | frogdb_types::types::stream::claim_tests::autoclaim_scan_filters_min_idle_and_paginates, frogdb_types::types::stream::claim_tests::autoclaim_scan_skips_below_min_idle, frogdb_types::types::stream::claim_tests::claim_creates_missing_target_consumer, +33 more |
| `frogdb_core::persistence::store_recovery::recover_all_shards` | core/src/persistence/store_recovery.rs:79 | 40 | 35 | frogdb_core::persistence::crash_recovery_tests::async_wal::test_async_wal_writer_recovery, frogdb_core::persistence::crash_recovery_tests::atomicity::test_batch_delete_atomic, frogdb_core::persistence::crash_recovery_tests::atomicity::test_cross_shard_batch, +32 more |

_showing 60 of 4325; full list in `depth.json`._

## Hot but shallow

The class that justifies this whole exercise: enormous exec counts, almost no test breadth. Both today's coverage percentage and raw exec counts report these as healthy.

| function | location | exec | tests | covering tests |
|---|---|---:|---:|---|
| `<frogdb_core::eviction::ranker::TtlRanker as frogdb_core::eviction::ranker::EvictionRanker>::rank` | core/src/eviction/ranker.rs:61 | 281973 | 3 | main::maxmemory_regression::volatile_ttl_honors_memory_limit, main::maxmemory_tcl::tcl_maxmemory_limit_honoured_volatile_ttl, main::maxmemory_tcl::tcl_maxmemory_volatile_only_volatile_ttl |
| `<frogdb_core::eviction::ranker::TtlRanker as frogdb_core::eviction::ranker::EvictionRanker>::rank::{closure#0}` | core/src/eviction/ranker.rs:62 | 281973 | 3 | main::maxmemory_regression::volatile_ttl_honors_memory_limit, main::maxmemory_tcl::tcl_maxmemory_limit_honoured_volatile_ttl, main::maxmemory_tcl::tcl_maxmemory_volatile_only_volatile_ttl |
| `<frogdb_core::eviction::pool::EvictionPool>::maybe_insert_with_ranker::<frogdb_core::eviction::ranker::TtlRank` | core/src/eviction/pool.rs:141 | 188887 | 3 | main::maxmemory_regression::volatile_ttl_honors_memory_limit, main::maxmemory_tcl::tcl_maxmemory_limit_honoured_volatile_ttl, main::maxmemory_tcl::tcl_maxmemory_volatile_only_volatile_ttl |
| `<frogdb_core::eviction::pool::EvictionPool>::maybe_insert_with_ranker::<frogdb_core::eviction::ranker::TtlRank` | core/src/eviction/pool.rs:152 | 143664 | 3 | main::maxmemory_regression::volatile_ttl_honors_memory_limit, main::maxmemory_tcl::tcl_maxmemory_limit_honoured_volatile_ttl, main::maxmemory_tcl::tcl_maxmemory_volatile_only_volatile_ttl |
| `<frogdb_core::eviction::pool::EvictionPool>::maybe_insert_with_ranker::<frogdb_core::eviction::ranker::TtlRank` | core/src/eviction/pool.rs:175 | 65715 | 3 | main::maxmemory_regression::volatile_ttl_honors_memory_limit, main::maxmemory_tcl::tcl_maxmemory_limit_honoured_volatile_ttl, main::maxmemory_tcl::tcl_maxmemory_volatile_only_volatile_ttl |
| `<frogdb_core::eviction::pool::EvictionPool>::maybe_insert_with_ranker::<frogdb_core::eviction::ranker::TtlRank` | core/src/eviction/pool.rs:163 | 60144 | 3 | main::maxmemory_regression::volatile_ttl_honors_memory_limit, main::maxmemory_tcl::tcl_maxmemory_limit_honoured_volatile_ttl, main::maxmemory_tcl::tcl_maxmemory_volatile_only_volatile_ttl |
| `frogdb_test_harness::response::extract_bulk_strings::{closure#0}` | test-harness/src/response.rs:95 | 41301 | 2 | main::keyspace_tcl::tcl_untagged_multi_key_commands, main::scripting_tcl::tcl_eval_redis_integer_to_lua_type |
| `<frogdb_core::shard::worker::ShardWorker>::scatter_mset::{closure#0}::{closure#0}` | core/src/shard/execution.rs:972 | 27688 | 3 | main::integration_client::test_tracking_bcast_scatter_mset_invalidation, main::integration_persistence::test_checkpoint_cross_shard_mset_contract_under_concurrent_bgsave, main::integration_transactions::test_multi_cross_shard_crossslot_with_allow_cross_slot_standalone |
| `<frogdb_server::connection::ConnectionHandler>::dispatch_scatter::{closure#0}::{closure#0}` | server/src/connection/routing.rs:164 | 27688 | 3 | main::integration_client::test_tracking_bcast_scatter_mset_invalidation, main::integration_persistence::test_checkpoint_cross_shard_mset_contract_under_concurrent_bgsave, main::integration_transactions::test_multi_cross_shard_crossslot_with_allow_cross_slot_standalone |
| `<frogdb_core::eviction::pool::EvictionPool>::maybe_insert_with_ranker::<frogdb_core::eviction::ranker::TtlRank` | core/src/eviction/pool.rs:130 | 12450 | 3 | main::maxmemory_regression::volatile_ttl_honors_memory_limit, main::maxmemory_tcl::tcl_maxmemory_limit_honoured_volatile_ttl, main::maxmemory_tcl::tcl_maxmemory_volatile_only_volatile_ttl |
| `<frogdb_core::shard::types::ShardEviction>::consider_candidate::<frogdb_core::eviction::ranker::TtlRanker>` | core/src/shard/types.rs:329 | 12450 | 3 | main::maxmemory_regression::volatile_ttl_honors_memory_limit, main::maxmemory_tcl::tcl_maxmemory_limit_honoured_volatile_ttl, main::maxmemory_tcl::tcl_maxmemory_volatile_only_volatile_ttl |
| `<frogdb_core::shard::worker::ShardWorker>::scatter_mset::{closure#0}` | core/src/shard/execution.rs:957 | 9232 | 3 | main::integration_client::test_tracking_bcast_scatter_mset_invalidation, main::integration_persistence::test_checkpoint_cross_shard_mset_contract_under_concurrent_bgsave, main::integration_transactions::test_multi_cross_shard_crossslot_with_allow_cross_slot_standalone |
| `<frogdb_server::scatter::strategies::MSetStrategy as frogdb_server::scatter::ScatterGatherStrategy>::partition` | server/src/scatter/strategies.rs:122 | 9232 | 3 | main::integration_client::test_tracking_bcast_scatter_mset_invalidation, main::integration_persistence::test_checkpoint_cross_shard_mset_contract_under_concurrent_bgsave, main::integration_transactions::test_multi_cross_shard_crossslot_with_allow_cross_slot_standalone |

## Cold lines (`count == 1`)

Lines executed exactly once across the entire suite — almost always an incidental touch on the way to something else, not a tested path.

| file | cold lines | first few |
|---|---:|---|
| cluster/src/state.rs | 1337 | 307, 308, 309, 310, 311, 312, 313, 314, 578, 579, … |
| server/src/runtime_config.rs | 1089 | 109, 110, 455, 1044, 1045, 1046, 1126, 1131, 1132, 1133, … |
| search/src/aggregate.rs | 717 | 156, 331, 333, 334, 335, 336, 339, 341, 342, 343, … |
| core/src/persistence/tests.rs | 606 | 20, 21, 22, 25, 26, 27, 28, 29, 30, 31, … |
| core/src/store/hashmap.rs | 604 | 379, 551, 554, 752, 868, 869, 870, 998, 999, 1000, … |
| types/src/types/mod.rs | 577 | 164, 165, 166, 169, 170, 171, 174, 175, 176, 312, … |
| persistence/src/rocks/tests.rs | 558 | 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, … |
| replication/src/replica_session.rs | 533 | 101, 354, 453, 457, 458, 718, 744, 802, 803, 804, … |
| core/src/persistence/crash_recovery_tests.rs | 503 | 45, 46, 49, 57, 60, 61, 77, 84, 85, 88, … |
| core/src/shard/post_execution.rs | 501 | 99, 103, 104, 105, 106, 472, 473, 474, 780, 783, … |
| server/src/connection/state.rs | 482 | 88, 89, 90, 92, 93, 243, 344, 345, 346, 347, … |
| core/src/shard/blocking.rs | 456 | 265, 271, 427, 428, 430, 431, 433, 434, 497, 498, … |
| persistence/src/wal/tests.rs | 452 | 18, 19, 20, 21, 22, 23, 24, 26, 27, 28, … |
| protocol/src/response.rs | 430 | 188, 189, 190, 198, 199, 200, 203, 204, 205, 262, … |
| search/src/index.rs | 405 | 165, 167, 173, 174, 343, 482, 519, 525, 531, 532, … |
| telemetry/src/status.rs | 403 | 786, 787, 788, 789, 790, 791, 792, 793, 794, 796, … |
| core/src/shard/execution.rs | 401 | 633, 641, 643, 649, 797, 798, 799, 827, 1020, 1141, … |
| core/src/client_registry/mod.rs | 385 | 143, 365, 366, 367, 603, 688, 689, 690, 691, 692, … |
| commands/src/sort.rs | 383 | 135, 302, 303, 308, 315, 316, 379, 393, 567, 568, … |
| core/src/command_spec.rs | 380 | 69, 99, 120, 130, 430, 437, 454, 459, 463, 665, … |
| types/src/types/stream.rs | 379 | 32, 33, 34, 35, 36, 37, 143, 148, 886, 1123, … |
| testing/src/conservation.rs | 366 | 163, 164, 165, 166, 167, 168, 169, 170, 442, 456, … |
| search/src/expression.rs | 364 | 69, 226, 227, 228, 229, 230, 235, 236, 242, 243, … |
| core/src/pubsub.rs | 360 | 104, 108, 180, 398, 399, 400, 562, 564, 599, 600, … |
| persistence/src/snapshot/tests.rs | 356 | 8, 9, 10, 11, 17, 18, 19, 20, 22, 23, … |
| server/src/connection/search/merge.rs | 334 | 43, 44, 45, 171, 172, 189, 221, 222, 268, 327, … |
| testing/src/models/kv.rs | 322 | 43, 138, 145, 147, 148, 149, 150, 151, 156, 248, … |
| telemetry/src/tracing.rs | 310 | 466, 467, 469, 470, 517, 518, 519, 523, 524, 526, … |
| server/src/scatter/broadcast.rs | 305 | 170, 172, 178, 197, 203, 209, 215, 217, 308, 309, … |
| server/src/connection/codec.rs | 304 | 182, 202, 337, 338, 436, 441, 452, 458, 459, 562, … |
| server/src/config/mod.rs | 294 | 59, 60, 62, 63, 64, 181, 182, 183, 184, 185, … |
| types/src/vectorset.rs | 291 | 79, 80, 81, 82, 83, 84, 85, 87, 88, 89, … |
| types/src/json.rs | 290 | 202, 291, 292, 294, 295, 296, 303, 446, 554, 604, … |
| replication/src/fullsync.rs | 289 | 70, 71, 72, 73, 195, 196, 199, 201, 202, 203, … |
| core/src/scripting/executor.rs | 287 | 66, 67, 68, 191, 193, 194, 195, 272, 284, 386, … |
| core/src/scripting/gate.rs | 286 | 223, 230, 275, 427, 428, 429, 430, 533, 534, 535, … |
| server/src/info/sections.rs | 286 | 91, 92, 93, 94, 97, 98, 99, 100, 491, 492, … |
| server/src/connection/observability_conn_command.rs | 280 | 156, 316, 387, 469, 474, 475, 501, 549, 574, 575, … |
| telemetry/src/testing.rs | 277 | 185, 322, 534, 535, 536, 683, 684, 685, 686, 693, … |
| search/src/schema.rs | 274 | 220, 221, 282, 283, 284, 285, 332, 333, 337, 338, … |
| server/src/recovery/tests.rs | 268 | 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, … |
| debug/src/web_ui/handlers.rs | 266 | 61, 62, 64, 65, 66, 67, 68, 69, 70, 71, … |
| acl/src/parser.rs | 264 | 46, 47, 48, 49, 50, 51, 105, 106, 110, 114, … |
| testing/src/partition.rs | 248 | 399, 400, 401, 402, 403, 405, 406, 407, 409, 410, … |
| types/src/timeseries/label_index.rs | 238 | 43, 47, 49, 50, 52, 61, 65, 67, 146, 178, … |
| server/src/connection/debug_conn_command.rs | 237 | 87, 88, 117, 155, 161, 173, 200, 383, 387, 388, … |
| core/src/shard/event_loop.rs | 236 | 581, 582, 583, 584, 585, 586, 589, 590, 591, 592, … |
| search/src/wire.rs | 233 | 183, 184, 211, 212, 213, 214, 215, 216, 217, 218, … |
| core/src/latency.rs | 229 | 40, 225, 260, 279, 282, 283, 284, 285, 286, 287, … |
| replication/src/primary/tests.rs | 228 | 72, 76, 77, 81, 82, 87, 88, 89, 90, 91, … |
| search/src/query.rs | 228 | 139, 140, 141, 142, 151, 152, 153, 154, 214, 307, … |
| server/src/role_manager.rs | 228 | 208, 209, 360, 362, 363, 364, 365, 581, 582, 583, … |
| core/src/tracking.rs | 218 | 106, 130, 131, 207, 208, 210, 213, 214, 215, 216, … |
| search/src/vector.rs | 217 | 76, 161, 162, 163, 164, 165, 212, 213, 214, 217, … |
| server/src/commands/migrate_cmd.rs | 216 | 95, 97, 98, 99, 101, 104, 106, 107, 108, 111, … |
| persistence/src/serialization/mod.rs | 213 | 258, 271, 272, 274, 275, 276, 279, 280, 281, 282, … |
| server/src/tls_runtime.rs | 204 | 427, 428, 429, 430, 431, 433, 434, 435, 436, 437, … |
| scripting/src/loader.rs | 200 | 123, 124, 125, 135, 136, 137, 141, 142, 143, 144, … |
| core/src/command.rs | 190 | 434, 435, 437, 617, 621, 629, 638, 648, 1431, 1581, … |
| core/src/shard/active_expiry.rs | 190 | 98, 99, 100, 101, 102, 103, 128, 129, 139, 140, … |

_showing 60 of 419 files; full list in `depth.json`._

