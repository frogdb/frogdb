//! Test helpers for the FrogDB operator.
//!
//! Provides shared fixtures and config bridge helpers for both resource
//! builder tests and live integration tests.

use crate::crd::*;
use kube::api::ObjectMeta;

// ---------------------------------------------------------------------------
// FrogDB CR fixtures
// ---------------------------------------------------------------------------

/// Construct a FrogDB CR with proper metadata for tests.
pub fn make_frogdb(name: &str, ns: &str, spec: FrogDBSpec) -> FrogDB {
    FrogDB {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(ns.to_string()),
            uid: Some("test-uid-12345".to_string()),
            generation: Some(1),
            ..Default::default()
        },
        spec,
        status: None,
    }
}

/// Default standalone spec (1 replica, default config).
pub fn default_spec() -> FrogDBSpec {
    FrogDBSpec {
        mode: "standalone".into(),
        replicas: 1,
        image: ImageSpec::default(),
        config: FrogDBConfigSpec::default(),
        cluster: None,
        resources: None,
        storage: StorageSpec::default(),
        pod_disruption_budget: PDBSpec::default(),
        upgrade: None,
    }
}

/// Cluster mode spec with fast test timeouts.
pub fn cluster_spec(replicas: i32) -> FrogDBSpec {
    FrogDBSpec {
        mode: "cluster".into(),
        replicas,
        cluster: Some(ClusterSpec {
            election_timeout_ms: 300,
            heartbeat_interval_ms: 100,
            ..Default::default()
        }),
        ..default_spec()
    }
}

/// Cluster spec using default (non-test) timeouts — for resource builder
/// tests that don't need fast election.
pub fn cluster_spec_default_timeouts(replicas: i32) -> FrogDBSpec {
    FrogDBSpec {
        mode: "cluster".into(),
        replicas,
        cluster: Some(ClusterSpec::default()),
        ..default_spec()
    }
}
