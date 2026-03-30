//! Integration tests for the FrogDB operator.
//!
//! These tests verify that the operator's generated configurations produce
//! working FrogDB server instances using the real TestServer and
//! ClusterTestHarness from frogdb-test-harness.

use std::time::Duration;

use frogdb_operator::config_gen;
use frogdb_operator::crd::*;
use frogdb_operator::resources::configmap;
use frogdb_operator::testing::{cluster_spec, default_spec};
use frogdb_test_harness::cluster_harness::{ClusterNodeConfig, ClusterTestHarness};
use frogdb_test_harness::cluster_helpers::{get_error_message, is_error, is_moved_redirect};
use frogdb_test_harness::server::{
    is_ok, parse_bulk_string, parse_simple_string, TestServer, TestServerConfig,
};

// ---------------------------------------------------------------------------
// Config bridge: translate operator CRD spec → TestServerConfig
// ---------------------------------------------------------------------------

/// Convert an operator FrogDBSpec into a TestServerConfig suitable for local
/// testing. This bridges the gap between "what the operator would deploy to
/// K8s" and "what we can run locally with TestServer".
fn operator_spec_to_test_config(spec: &FrogDBSpec) -> TestServerConfig {
    TestServerConfig {
        num_shards: Some(spec.config.num_shards),
        persistence: spec.config.persistence.enabled,
        ..Default::default()
    }
}

/// Convert an operator FrogDBSpec (cluster mode) into a ClusterNodeConfig.
fn operator_spec_to_cluster_config(spec: &FrogDBSpec) -> ClusterNodeConfig {
    let cluster = spec.cluster.as_ref().cloned().unwrap_or_default();
    ClusterNodeConfig {
        num_shards: Some(spec.config.num_shards),
        persistence: spec.config.persistence.enabled,
        election_timeout_ms: cluster.election_timeout_ms,
        heartbeat_interval_ms: cluster.heartbeat_interval_ms,
        ..Default::default()
    }
}

/// Assert PING returns PONG.
async fn assert_ping(server: &TestServer) {
    let resp = server.send("PING", &[]).await;
    let msg = parse_simple_string(&resp);
    assert_eq!(msg, Some("PONG"), "Expected PONG, got {:?}", resp);
}

/// Write a key and read it back.
async fn assert_set_get(server: &TestServer, key: &str, value: &str) {
    let set_resp = server.send("SET", &[key, value]).await;
    assert!(is_ok(&set_resp), "SET failed: {:?}", set_resp);

    let get_resp = server.send("GET", &[key]).await;
    let data = parse_bulk_string(&get_resp).expect("Expected bulk string response");
    assert_eq!(
        std::str::from_utf8(data).unwrap(),
        value,
        "GET returned wrong value"
    );
}

/// Write a key in cluster mode, handling REDIRECT responses by retrying
/// on the correct node.
async fn cluster_set(
    harness: &ClusterTestHarness,
    key: &str,
    value: &str,
) -> bool {
    // Try the leader first
    let leader_id = match harness.get_leader().await {
        Some(id) => id,
        None => return false,
    };

    let node = harness.node(leader_id).unwrap();
    let resp = node.send("SET", &[key, value]).await;

    if is_ok(&resp) {
        return true;
    }

    // Handle MOVED redirect
    if let Some((_slot, addr)) = is_moved_redirect(&resp) {
        for &node_id in &harness.node_ids() {
            if let Some(n) = harness.node(node_id)
                && n.client_addr() == addr
            {
                let retry = n.send("SET", &[key, value]).await;
                return is_ok(&retry);
            }
        }
    }

    // Handle REDIRECT (Raft leader forwarding)
    if is_error(&resp)
        && let Some(err) = get_error_message(&resp)
        && err.starts_with("REDIRECT ")
    {
        let parts: Vec<&str> = err.split_whitespace().collect();
        if parts.len() >= 2 {
            let addr = parts[1];
            for &node_id in &harness.node_ids() {
                if let Some(n) = harness.node(node_id)
                    && n.client_addr() == addr
                {
                    let retry = n.send("SET", &[key, value]).await;
                    return is_ok(&retry);
                }
            }
        }
    }

    false
}

/// Read a key from cluster, trying all nodes if needed (handles MOVED).
async fn cluster_get(harness: &ClusterTestHarness, key: &str) -> Option<String> {
    for &node_id in &harness.node_ids() {
        if let Some(node) = harness.node(node_id) {
            if !node.is_running() {
                continue;
            }
            let resp = node.send("GET", &[key]).await;
            if let Some(data) = parse_bulk_string(&resp) {
                return Some(std::str::from_utf8(data).unwrap().to_string());
            }
        }
    }
    None
}

// ===========================================================================
// Config generation tests
// ===========================================================================

mod config_gen_tests {
    use super::*;

    #[test]
    fn generated_toml_parses_as_frogdb_config() {
        let spec = default_spec();
        let toml_str = config_gen::generate_toml(&spec.config);
        let result: Result<frogdb_config::Config, _> = toml::from_str(&toml_str);
        assert!(
            result.is_ok(),
            "Operator TOML failed to parse: {}",
            result.unwrap_err()
        );
    }

    #[test]
    fn generated_toml_custom_config_parses() {
        let spec = FrogDBSpec {
            config: FrogDBConfigSpec {
                port: 6380,
                num_shards: 8,
                persistence: PersistenceSpec {
                    enabled: false,
                    durability_mode: "async".into(),
                },
                metrics: MetricsSpec {
                    enabled: false,
                    port: 9191,
                },
                logging: LoggingSpec {
                    level: "debug".into(),
                },
                memory: MemorySpec {
                    maxmemory: 1073741824,
                    policy: "allkeys-lru".into(),
                },
            },
            ..default_spec()
        };
        let toml_str = config_gen::generate_toml(&spec.config);
        let result: Result<frogdb_config::Config, _> = toml::from_str(&toml_str);
        assert!(
            result.is_ok(),
            "Custom TOML failed to parse: {}",
            result.unwrap_err()
        );
    }

    #[test]
    fn config_change_produces_different_hash() {
        let spec1 = default_spec();
        let spec2 = FrogDBSpec {
            config: FrogDBConfigSpec {
                port: 6380,
                ..Default::default()
            },
            ..default_spec()
        };
        let hash1 = configmap::config_hash(&config_gen::generate_toml(&spec1.config));
        let hash2 = configmap::config_hash(&config_gen::generate_toml(&spec2.config));
        assert_ne!(hash1, hash2, "Different configs should produce different hashes");
    }
}

// ===========================================================================
// Standalone server tests
// ===========================================================================

mod standalone_tests {
    use super::*;

    #[tokio::test]
    async fn server_starts_with_default_spec() {
        let spec = default_spec();
        let config = operator_spec_to_test_config(&spec);
        let server = TestServer::start_standalone_with_config(config).await;
        assert_ping(&server).await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn server_set_get() {
        let spec = default_spec();
        let config = operator_spec_to_test_config(&spec);
        let server = TestServer::start_standalone_with_config(config).await;
        assert_set_get(&server, "foo", "bar").await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn server_custom_shards() {
        let spec = FrogDBSpec {
            config: FrogDBConfigSpec {
                num_shards: 8,
                ..Default::default()
            },
            ..default_spec()
        };
        let config = operator_spec_to_test_config(&spec);
        let server = TestServer::start_standalone_with_config(config).await;
        assert_ping(&server).await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn server_persistence_disabled() {
        let spec = FrogDBSpec {
            config: FrogDBConfigSpec {
                persistence: PersistenceSpec {
                    enabled: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..default_spec()
        };
        let config = operator_spec_to_test_config(&spec);
        let server = TestServer::start_standalone_with_config(config).await;
        assert_set_get(&server, "key1", "value1").await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn server_with_replicas() {
        // Simulates what the operator does for standalone mode with replicas > 1:
        // pod-0 is primary, others are replicas.
        let primary = TestServer::start_primary().await;
        let replica = TestServer::start_replica(&primary).await;

        // Wait for replication handshake to complete
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Write to primary
        assert_set_get(&primary, "replicated_key", "replicated_value").await;

        // WAIT for 1 replica to acknowledge (up to 5s)
        primary.send("WAIT", &["1", "5000"]).await;

        // Read from replica
        let resp = replica.send("GET", &["replicated_key"]).await;
        let data = parse_bulk_string(&resp).expect("Expected replicated data");
        assert_eq!(std::str::from_utf8(data).unwrap(), "replicated_value");

        replica.shutdown().await;
        primary.shutdown().await;
    }
}

// ===========================================================================
// Cluster tests
// ===========================================================================

mod cluster_tests {
    use super::*;

    #[tokio::test]
    async fn cluster_3_node_forms() {
        let spec = cluster_spec(3);
        let config = operator_spec_to_cluster_config(&spec);

        let mut harness = ClusterTestHarness::with_config(config);
        harness.start_cluster(3).await.unwrap();
        harness
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
        harness
            .wait_for_cluster_convergence(Duration::from_secs(10))
            .await
            .unwrap();

        let node_ids = harness.node_ids();
        for &id in &node_ids {
            let info = harness.get_cluster_info(id).unwrap();
            assert_eq!(info.cluster_known_nodes, 3);
        }

        harness.shutdown_all().await;
    }

    #[tokio::test]
    async fn cluster_5_node_forms() {
        let spec = cluster_spec(5);
        let config = operator_spec_to_cluster_config(&spec);

        let mut harness = ClusterTestHarness::with_config(config);
        harness.start_cluster(5).await.unwrap();
        harness
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
        harness
            .wait_for_cluster_convergence(Duration::from_secs(15))
            .await
            .unwrap();

        let node_ids = harness.node_ids();
        for &id in &node_ids {
            let info = harness.get_cluster_info(id).unwrap();
            assert_eq!(info.cluster_known_nodes, 5);
        }

        harness.shutdown_all().await;
    }

    #[tokio::test]
    async fn cluster_set_get_via_leader() {
        let spec = cluster_spec(3);
        let config = operator_spec_to_cluster_config(&spec);

        let mut harness = ClusterTestHarness::with_config(config);
        harness.start_cluster(3).await.unwrap();
        harness
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
        harness
            .wait_for_cluster_convergence(Duration::from_secs(10))
            .await
            .unwrap();

        assert!(
            cluster_set(&harness, "cluster_key", "cluster_value").await,
            "SET should succeed on cluster"
        );

        let value = cluster_get(&harness, "cluster_key").await;
        assert_eq!(value.as_deref(), Some("cluster_value"));

        harness.shutdown_all().await;
    }

    #[tokio::test]
    async fn cluster_scale_up_add_node() {
        let spec = cluster_spec(3);
        let config = operator_spec_to_cluster_config(&spec);

        let mut harness = ClusterTestHarness::with_config(config);
        harness.start_cluster(3).await.unwrap();
        harness
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
        harness
            .wait_for_cluster_convergence(Duration::from_secs(10))
            .await
            .unwrap();

        // Add a new node (simulates operator scaling StatefulSet up)
        let new_node = harness.add_node().await.unwrap();
        harness
            .wait_for_node_recognized(new_node, Duration::from_secs(10))
            .await
            .unwrap();

        // All nodes should see 4 members
        for &id in &harness.node_ids() {
            let info = harness.get_cluster_info(id).unwrap();
            assert_eq!(info.cluster_known_nodes, 4);
        }

        harness.shutdown_all().await;
    }

    #[tokio::test]
    async fn cluster_scale_down_5_to_3() {
        let spec = cluster_spec(5);
        let config = operator_spec_to_cluster_config(&spec);

        let mut harness = ClusterTestHarness::with_config(config);
        harness.start_cluster(5).await.unwrap();
        harness
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
        harness
            .wait_for_cluster_convergence(Duration::from_secs(15))
            .await
            .unwrap();

        // Remove 2 non-leader nodes
        let leader_id = harness.get_leader().await.unwrap();
        let removable: Vec<u64> = harness
            .node_ids()
            .into_iter()
            .filter(|&id| id != leader_id)
            .take(2)
            .collect();

        for id in removable {
            harness.remove_node(id).await.unwrap();
        }

        harness
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(harness.node_ids().len(), 3);

        harness.shutdown_all().await;
    }

    #[tokio::test]
    async fn cluster_rolling_restart() {
        let spec = cluster_spec(3);
        let config = operator_spec_to_cluster_config(&spec);

        let mut harness = ClusterTestHarness::with_config(config);
        harness.start_cluster(3).await.unwrap();
        harness
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
        harness
            .wait_for_cluster_convergence(Duration::from_secs(10))
            .await
            .unwrap();

        // Write data before rolling restart
        assert!(
            cluster_set(&harness, "survive_restart", "yes").await,
            "Pre-restart SET should succeed"
        );

        // Allow Raft replication
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Rolling restart: restart each node one at a time
        let node_ids = harness.node_ids();
        for &id in &node_ids {
            harness.restart_node(id).await.unwrap();
            harness
                .wait_for_leader(Duration::from_secs(10))
                .await
                .unwrap();
        }

        // Verify cluster is healthy and data survived
        harness
            .wait_for_cluster_convergence(Duration::from_secs(10))
            .await
            .unwrap();

        let value = cluster_get(&harness, "survive_restart").await;
        assert_eq!(
            value.as_deref(),
            Some("yes"),
            "Data should survive rolling restart"
        );

        harness.shutdown_all().await;
    }

    #[tokio::test]
    async fn cluster_leader_failover() {
        let spec = cluster_spec(3);
        let config = operator_spec_to_cluster_config(&spec);

        let mut harness = ClusterTestHarness::with_config(config);
        harness.start_cluster(3).await.unwrap();
        let leader_id = harness
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
        harness
            .wait_for_cluster_convergence(Duration::from_secs(10))
            .await
            .unwrap();

        // Kill the leader
        harness.kill_node(leader_id).await;

        // Wait for a new leader — this is the key assertion:
        // the cluster can elect a new leader after losing the old one
        let new_leader_id = harness
            .wait_for_new_leader(leader_id, Duration::from_secs(15))
            .await
            .unwrap();
        assert_ne!(new_leader_id, leader_id);

        // Verify surviving nodes respond to PING
        for &node_id in &harness.node_ids() {
            if node_id == leader_id {
                continue;
            }
            let node = harness.node(node_id).unwrap();
            let resp = node.send("PING", &[]).await;
            assert_eq!(
                parse_simple_string(&resp),
                Some("PONG"),
                "Node {} should respond after failover",
                node_id
            );
        }

        harness.shutdown_all().await;
    }

    #[tokio::test]
    async fn cluster_config_change_rolling() {
        // Verify that changing config (which changes the hash) allows a rolling
        // restart without data loss
        let spec = cluster_spec(3);
        let config = operator_spec_to_cluster_config(&spec);

        // Compute hashes for original vs modified config
        let original_hash =
            configmap::config_hash(&config_gen::generate_toml(&spec.config));
        let modified_spec = FrogDBSpec {
            config: FrogDBConfigSpec {
                num_shards: 8,
                ..spec.config.clone()
            },
            ..spec.clone()
        };
        let modified_hash =
            configmap::config_hash(&config_gen::generate_toml(&modified_spec.config));
        assert_ne!(original_hash, modified_hash);

        // Start cluster, write data
        let mut harness = ClusterTestHarness::with_config(config);
        harness.start_cluster(3).await.unwrap();
        harness
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
        harness
            .wait_for_cluster_convergence(Duration::from_secs(10))
            .await
            .unwrap();

        assert!(
            cluster_set(&harness, "config_change_key", "config_change_value").await,
            "SET should succeed before config change"
        );

        // Allow Raft replication
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Rolling restart (simulating the config change rollout)
        let node_ids = harness.node_ids();
        for &id in &node_ids {
            harness.restart_node(id).await.unwrap();
            harness
                .wait_for_leader(Duration::from_secs(10))
                .await
                .unwrap();
        }

        harness
            .wait_for_cluster_convergence(Duration::from_secs(10))
            .await
            .unwrap();

        let value = cluster_get(&harness, "config_change_key").await;
        assert_eq!(
            value.as_deref(),
            Some("config_change_value"),
            "Data should survive config change"
        );

        harness.shutdown_all().await;
    }

    #[tokio::test]
    async fn cluster_pdb_quorum_honored() {
        // Verify that with 5 nodes, killing a minority (2) still leaves quorum (3).
        // The PDB minAvailable=3 means at most 2 nodes can be disrupted.
        let spec = cluster_spec(5);
        let config = operator_spec_to_cluster_config(&spec);
        let pdb_min = spec.effective_min_available();
        assert_eq!(pdb_min, 3, "PDB quorum for 5 nodes should be 3");

        let mut harness = ClusterTestHarness::with_config(config);
        harness.start_cluster(5).await.unwrap();
        harness
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
        harness
            .wait_for_cluster_convergence(Duration::from_secs(15))
            .await
            .unwrap();

        // Kill 2 non-leader nodes (minority — PDB would allow this)
        let leader_id = harness.get_leader().await.unwrap();
        let victims: Vec<u64> = harness
            .node_ids()
            .into_iter()
            .filter(|&id| id != leader_id)
            .take(2)
            .collect();
        for id in &victims {
            harness.kill_node(*id).await;
        }

        // Cluster should still have a leader with 3 remaining nodes
        harness
            .wait_for_leader(Duration::from_secs(15))
            .await
            .unwrap();

        // Give the cluster time to stabilize after losing nodes
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Cluster should still be operational — surviving nodes respond
        for &node_id in &harness.node_ids() {
            if victims.contains(&node_id) {
                continue;
            }
            let node = harness.node(node_id).unwrap();
            if !node.is_running() {
                continue;
            }
            let resp = node.send("PING", &[]).await;
            assert_eq!(
                parse_simple_string(&resp),
                Some("PONG"),
                "Node {} should respond with quorum intact",
                node_id
            );
        }

        harness.shutdown_all().await;
    }
}
