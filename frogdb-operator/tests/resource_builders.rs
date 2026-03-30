//! Resource builder tests for the FrogDB operator.
//!
//! Tests that the Kubernetes resource builders produce structurally correct
//! StatefulSets, Services, PDBs, and ConfigMaps from FrogDB CR specs.

use std::collections::BTreeMap;

use frogdb_operator::crd::*;
use frogdb_operator::resources::{self, configmap, pdb, service, statefulset};
use frogdb_operator::testing::{
    cluster_spec_default_timeouts as cluster_spec, default_spec, make_frogdb,
};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

/// Get the first container from a StatefulSet.
fn container(
    sts: &k8s_openapi::api::apps::v1::StatefulSet,
) -> &k8s_openapi::api::core::v1::Container {
    &sts.spec
        .as_ref()
        .unwrap()
        .template
        .spec
        .as_ref()
        .unwrap()
        .containers[0]
}

/// Get env var value by name from a container.
fn env_value(container: &k8s_openapi::api::core::v1::Container, name: &str) -> Option<String> {
    container
        .env
        .as_ref()?
        .iter()
        .find(|e| e.name == name)
        .and_then(|e| e.value.clone())
}

/// Get pod template annotations from a StatefulSet.
fn pod_annotations(sts: &k8s_openapi::api::apps::v1::StatefulSet) -> &BTreeMap<String, String> {
    sts.spec
        .as_ref()
        .unwrap()
        .template
        .metadata
        .as_ref()
        .unwrap()
        .annotations
        .as_ref()
        .unwrap()
}

/// Get container port names from a StatefulSet.
fn port_names(sts: &k8s_openapi::api::apps::v1::StatefulSet) -> Vec<String> {
    container(sts)
        .ports
        .as_ref()
        .unwrap()
        .iter()
        .filter_map(|p| p.name.clone())
        .collect()
}

// ===========================================================================
// StatefulSet builder tests
// ===========================================================================

mod statefulset_tests {
    use super::*;

    #[test]
    fn standalone_single_replica() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let sts = statefulset::build(&frogdb, "abc123");

        let spec = sts.spec.as_ref().unwrap();
        assert_eq!(spec.replicas, Some(1));

        let c = container(&sts);
        // Single standalone uses direct command, no shell wrapper
        assert_eq!(c.command.as_ref().unwrap()[0], "frogdb-server");
        assert!(c.args.is_none());

        // No cluster env vars
        assert!(env_value(c, "FROGDB_CLUSTER__ENABLED").is_none());
        assert!(env_value(c, "FROGDB_CLUSTER__INITIAL_NODES").is_none());
    }

    #[test]
    fn standalone_multi_replica_shell_wrapper() {
        let spec = FrogDBSpec {
            replicas: 3,
            ..default_spec()
        };
        let frogdb = make_frogdb("mydb", "prod", spec);
        let sts = statefulset::build(&frogdb, "abc123");

        let c = container(&sts);
        // Multi-replica standalone uses shell wrapper
        assert_eq!(c.command.as_ref().unwrap()[0], "/bin/sh");
        let script = &c.args.as_ref().unwrap()[1]; // args[0] = "-c", args[1] = script
        assert!(script.contains("FROGDB_REPLICATION__ROLE=primary"));
        assert!(script.contains("FROGDB_REPLICATION__ROLE=replica"));
        assert!(script.contains("mydb-0.mydb-headless.prod.svc.cluster.local"));
    }

    #[test]
    fn cluster_mode_env_vars() {
        let frogdb = make_frogdb("mydb", "default", cluster_spec(3));
        let sts = statefulset::build(&frogdb, "abc123");

        let c = container(&sts);
        assert_eq!(
            env_value(c, "FROGDB_CLUSTER__ENABLED").as_deref(),
            Some("true")
        );
        assert!(env_value(c, "FROGDB_CLUSTER__CLUSTER_BUS_ADDR").is_some());
        assert!(env_value(c, "FROGDB_CLUSTER__CLIENT_ADDR").is_some());
    }

    #[test]
    fn cluster_initial_nodes_3() {
        let frogdb = make_frogdb("mydb", "ns1", cluster_spec(3));
        let sts = statefulset::build(&frogdb, "abc123");

        let c = container(&sts);
        let nodes = env_value(c, "FROGDB_CLUSTER__INITIAL_NODES").unwrap();
        let entries: Vec<&str> = nodes.split(',').collect();
        assert_eq!(entries.len(), 3);
        assert!(entries[0].contains("mydb-0.mydb-headless.ns1.svc.cluster.local:16379"));
        assert!(entries[1].contains("mydb-1.mydb-headless.ns1.svc.cluster.local:16379"));
        assert!(entries[2].contains("mydb-2.mydb-headless.ns1.svc.cluster.local:16379"));
    }

    #[test]
    fn cluster_initial_nodes_5() {
        let frogdb = make_frogdb("mydb", "default", cluster_spec(5));
        let sts = statefulset::build(&frogdb, "abc123");

        let c = container(&sts);
        let nodes = env_value(c, "FROGDB_CLUSTER__INITIAL_NODES").unwrap();
        assert_eq!(nodes.split(',').count(), 5);
    }

    #[test]
    fn cluster_scale_changes_initial_nodes() {
        let frogdb3 = make_frogdb("mydb", "default", cluster_spec(3));
        let frogdb5 = make_frogdb("mydb", "default", cluster_spec(5));

        let sts3 = statefulset::build(&frogdb3, "abc");
        let sts5 = statefulset::build(&frogdb5, "abc");

        let nodes3 = env_value(container(&sts3), "FROGDB_CLUSTER__INITIAL_NODES").unwrap();
        let nodes5 = env_value(container(&sts5), "FROGDB_CLUSTER__INITIAL_NODES").unwrap();

        assert_eq!(nodes3.split(',').count(), 3);
        assert_eq!(nodes5.split(',').count(), 5);
        assert_ne!(nodes3, nodes5);
    }

    #[test]
    fn config_hash_annotation() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let sts = statefulset::build(&frogdb, "deadbeef01234567");

        let ann = pod_annotations(&sts);
        assert_eq!(
            ann.get(resources::CONFIG_HASH_ANNOTATION),
            Some(&"deadbeef01234567".to_string())
        );
    }

    #[test]
    fn config_change_triggers_annotation_diff() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let sts1 = statefulset::build(&frogdb, "hash_a");
        let sts2 = statefulset::build(&frogdb, "hash_b");

        let ann1 = pod_annotations(&sts1);
        let ann2 = pod_annotations(&sts2);
        assert_ne!(
            ann1.get(resources::CONFIG_HASH_ANNOTATION),
            ann2.get(resources::CONFIG_HASH_ANNOTATION),
        );
    }

    #[test]
    fn same_config_idempotent() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let sts1 = statefulset::build(&frogdb, "same_hash");
        let sts2 = statefulset::build(&frogdb, "same_hash");

        // Serialize both and compare — identical specs should produce identical output
        let json1 = serde_json::to_string(&sts1).unwrap();
        let json2 = serde_json::to_string(&sts2).unwrap();
        assert_eq!(json1, json2);
    }

    #[test]
    fn metrics_port_present_when_enabled() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        // Default has metrics enabled
        assert!(frogdb.spec.config.metrics.enabled);

        let sts = statefulset::build(&frogdb, "abc");
        assert!(port_names(&sts).contains(&"metrics".to_string()));
    }

    #[test]
    fn metrics_port_absent_when_disabled() {
        let spec = FrogDBSpec {
            config: FrogDBConfigSpec {
                metrics: MetricsSpec {
                    enabled: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..default_spec()
        };
        let frogdb = make_frogdb("mydb", "default", spec);
        let sts = statefulset::build(&frogdb, "abc");
        assert!(!port_names(&sts).contains(&"metrics".to_string()));
    }

    #[test]
    fn cluster_bus_port_present() {
        let frogdb = make_frogdb("mydb", "default", cluster_spec(3));
        let sts = statefulset::build(&frogdb, "abc");
        assert!(port_names(&sts).contains(&"cluster-bus".to_string()));
    }

    #[test]
    fn pvc_defaults() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let sts = statefulset::build(&frogdb, "abc");

        let pvc = &sts
            .spec
            .as_ref()
            .unwrap()
            .volume_claim_templates
            .as_ref()
            .unwrap()[0];
        let pvc_spec = pvc.spec.as_ref().unwrap();
        let storage = pvc_spec
            .resources
            .as_ref()
            .unwrap()
            .requests
            .as_ref()
            .unwrap()
            .get("storage")
            .unwrap();
        assert_eq!(storage.0, "10Gi");
        assert!(pvc_spec.storage_class_name.is_none());
    }

    #[test]
    fn pvc_custom() {
        let spec = FrogDBSpec {
            storage: StorageSpec {
                size: "50Gi".into(),
                storage_class: "fast-ssd".into(),
            },
            ..default_spec()
        };
        let frogdb = make_frogdb("mydb", "default", spec);
        let sts = statefulset::build(&frogdb, "abc");

        let pvc = &sts
            .spec
            .as_ref()
            .unwrap()
            .volume_claim_templates
            .as_ref()
            .unwrap()[0];
        let pvc_spec = pvc.spec.as_ref().unwrap();
        let storage = pvc_spec
            .resources
            .as_ref()
            .unwrap()
            .requests
            .as_ref()
            .unwrap()
            .get("storage")
            .unwrap();
        assert_eq!(storage.0, "50Gi");
        assert_eq!(pvc_spec.storage_class_name.as_deref(), Some("fast-ssd"));
    }

    #[test]
    fn resource_requests_limits() {
        let mut requests = BTreeMap::new();
        requests.insert("cpu".into(), "500m".into());
        requests.insert("memory".into(), "1Gi".into());
        let mut limits = BTreeMap::new();
        limits.insert("cpu".into(), "2".into());
        limits.insert("memory".into(), "4Gi".into());

        let spec = FrogDBSpec {
            resources: Some(ResourceRequirements {
                requests: Some(requests),
                limits: Some(limits),
            }),
            ..default_spec()
        };
        let frogdb = make_frogdb("mydb", "default", spec);
        let sts = statefulset::build(&frogdb, "abc");

        let c = container(&sts);
        let res = c.resources.as_ref().unwrap();
        assert_eq!(res.requests.as_ref().unwrap().get("cpu").unwrap().0, "500m");
        assert_eq!(res.limits.as_ref().unwrap().get("memory").unwrap().0, "4Gi");
    }

    #[test]
    fn owner_reference() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let sts = statefulset::build(&frogdb, "abc");

        let owner_refs = sts.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "FrogDB");
        assert_eq!(owner_refs[0].name, "mydb");
        assert_eq!(owner_refs[0].uid, "test-uid-12345");
        assert_eq!(owner_refs[0].controller, Some(true));
        assert_eq!(owner_refs[0].block_owner_deletion, Some(true));
    }

    #[test]
    fn standard_labels() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let sts = statefulset::build(&frogdb, "abc");

        // StatefulSet labels
        let labels = sts.metadata.labels.as_ref().unwrap();
        assert_eq!(labels.get("app.kubernetes.io/name").unwrap(), "frogdb");
        assert_eq!(labels.get("app.kubernetes.io/instance").unwrap(), "mydb");
        assert_eq!(
            labels.get("app.kubernetes.io/managed-by").unwrap(),
            "frogdb-operator"
        );

        // Pod template labels
        let pod_labels = sts
            .spec
            .as_ref()
            .unwrap()
            .template
            .metadata
            .as_ref()
            .unwrap()
            .labels
            .as_ref()
            .unwrap();
        assert_eq!(pod_labels.get("app.kubernetes.io/name").unwrap(), "frogdb");
        assert_eq!(
            pod_labels.get("app.kubernetes.io/instance").unwrap(),
            "mydb"
        );
    }

    #[test]
    fn service_name_headless() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let sts = statefulset::build(&frogdb, "abc");
        assert_eq!(
            sts.spec.as_ref().unwrap().service_name,
            Some("mydb-headless".to_string())
        );
    }

    #[test]
    fn volume_mounts() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let sts = statefulset::build(&frogdb, "abc");

        let c = container(&sts);
        let mounts = c.volume_mounts.as_ref().unwrap();
        let config_mount = mounts.iter().find(|m| m.name == "config").unwrap();
        assert_eq!(config_mount.mount_path, "/etc/frogdb");

        let data_mount = mounts.iter().find(|m| m.name == "data").unwrap();
        assert_eq!(data_mount.mount_path, "/data");
    }

    #[test]
    fn config_volume_source() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let sts = statefulset::build(&frogdb, "abc");

        let volumes = sts
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .volumes
            .as_ref()
            .unwrap();
        let config_vol = volumes.iter().find(|v| v.name == "config").unwrap();
        assert_eq!(config_vol.config_map.as_ref().unwrap().name, "mydb-config");
    }
}

// ===========================================================================
// Service builder tests
// ===========================================================================

mod service_tests {
    use super::*;

    #[test]
    fn headless_cluster_ip_none() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let svc = service::build_headless(&frogdb);
        assert_eq!(
            svc.spec.as_ref().unwrap().cluster_ip.as_deref(),
            Some("None")
        );
    }

    #[test]
    fn headless_name() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let svc = service::build_headless(&frogdb);
        assert_eq!(svc.metadata.name.as_deref(), Some("mydb-headless"));
    }

    #[test]
    fn headless_standalone_ports() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let svc = service::build_headless(&frogdb);

        let port_names: Vec<_> = svc
            .spec
            .as_ref()
            .unwrap()
            .ports
            .as_ref()
            .unwrap()
            .iter()
            .filter_map(|p| p.name.as_deref())
            .collect();
        assert!(port_names.contains(&"frogdb"));
        assert!(port_names.contains(&"metrics")); // metrics enabled by default
        assert!(!port_names.contains(&"cluster-bus"));
    }

    #[test]
    fn headless_cluster_bus_port() {
        let frogdb = make_frogdb("mydb", "default", cluster_spec(3));
        let svc = service::build_headless(&frogdb);

        let port_names: Vec<_> = svc
            .spec
            .as_ref()
            .unwrap()
            .ports
            .as_ref()
            .unwrap()
            .iter()
            .filter_map(|p| p.name.as_deref())
            .collect();
        assert!(port_names.contains(&"cluster-bus"));
    }

    #[test]
    fn client_name() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let svc = service::build_client(&frogdb);
        assert_eq!(svc.metadata.name.as_deref(), Some("mydb"));
    }

    #[test]
    fn client_frogdb_port_only() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let svc = service::build_client(&frogdb);

        let ports = svc.spec.as_ref().unwrap().ports.as_ref().unwrap();
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].name.as_deref(), Some("frogdb"));
    }

    #[test]
    fn client_service_no_cluster_ip_none() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let svc = service::build_client(&frogdb);
        // Client service should NOT have clusterIP: None
        assert!(svc.spec.as_ref().unwrap().cluster_ip.is_none());
    }

    #[test]
    fn service_owner_references() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let headless = service::build_headless(&frogdb);
        let client = service::build_client(&frogdb);

        for svc in [&headless, &client] {
            let refs = svc.metadata.owner_references.as_ref().unwrap();
            assert_eq!(refs[0].kind, "FrogDB");
            assert_eq!(refs[0].name, "mydb");
        }
    }
}

// ===========================================================================
// PDB builder tests
// ===========================================================================

mod pdb_tests {
    use super::*;

    #[test]
    fn disabled_returns_none() {
        let spec = FrogDBSpec {
            pod_disruption_budget: PDBSpec {
                enabled: false,
                ..Default::default()
            },
            ..default_spec()
        };
        let frogdb = make_frogdb("mydb", "default", spec);
        assert!(pdb::build(&frogdb).is_none());
    }

    #[test]
    fn cluster_quorum_3() {
        let frogdb = make_frogdb("mydb", "default", cluster_spec(3));
        let p = pdb::build(&frogdb).unwrap();
        assert_eq!(
            p.spec.as_ref().unwrap().min_available,
            Some(IntOrString::Int(2))
        );
    }

    #[test]
    fn cluster_quorum_5() {
        let frogdb = make_frogdb("mydb", "default", cluster_spec(5));
        let p = pdb::build(&frogdb).unwrap();
        assert_eq!(
            p.spec.as_ref().unwrap().min_available,
            Some(IntOrString::Int(3))
        );
    }

    #[test]
    fn cluster_quorum_7() {
        let frogdb = make_frogdb("mydb", "default", cluster_spec(7));
        let p = pdb::build(&frogdb).unwrap();
        assert_eq!(
            p.spec.as_ref().unwrap().min_available,
            Some(IntOrString::Int(4))
        );
    }

    #[test]
    fn standalone_uses_configured() {
        let spec = FrogDBSpec {
            pod_disruption_budget: PDBSpec {
                enabled: true,
                min_available: Some(2),
            },
            ..default_spec()
        };
        let frogdb = make_frogdb("mydb", "default", spec);
        let p = pdb::build(&frogdb).unwrap();
        assert_eq!(
            p.spec.as_ref().unwrap().min_available,
            Some(IntOrString::Int(2))
        );
    }

    #[test]
    fn pdb_name() {
        let frogdb = make_frogdb("mydb", "default", cluster_spec(3));
        let p = pdb::build(&frogdb).unwrap();
        assert_eq!(p.metadata.name.as_deref(), Some("mydb-pdb"));
    }

    #[test]
    fn selector_matches_statefulset() {
        let frogdb = make_frogdb("mydb", "default", cluster_spec(3));
        let p = pdb::build(&frogdb).unwrap();
        let sts = statefulset::build(&frogdb, "abc");

        let pdb_selector = p
            .spec
            .as_ref()
            .unwrap()
            .selector
            .as_ref()
            .unwrap()
            .match_labels
            .as_ref()
            .unwrap();
        let sts_selector = sts
            .spec
            .as_ref()
            .unwrap()
            .selector
            .match_labels
            .as_ref()
            .unwrap();
        assert_eq!(pdb_selector, sts_selector);
    }
}

// ===========================================================================
// ConfigMap builder tests
// ===========================================================================

mod configmap_tests {
    use super::*;

    #[test]
    fn configmap_name() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let cm = configmap::build(&frogdb, "test toml content");
        assert_eq!(cm.metadata.name.as_deref(), Some("mydb-config"));
    }

    #[test]
    fn configmap_contains_toml() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let toml = "port = 6379\n";
        let cm = configmap::build(&frogdb, toml);
        assert_eq!(cm.data.as_ref().unwrap().get("frogdb.toml").unwrap(), toml);
    }

    #[test]
    fn configmap_owner_reference() {
        let frogdb = make_frogdb("mydb", "default", default_spec());
        let cm = configmap::build(&frogdb, "");
        let refs = cm.metadata.owner_references.as_ref().unwrap();
        assert_eq!(refs[0].kind, "FrogDB");
        assert_eq!(refs[0].uid, "test-uid-12345");
    }
}
