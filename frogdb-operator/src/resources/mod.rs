//! Kubernetes resource builders for FrogDB.

pub mod configmap;
pub mod pdb;
pub mod service;
pub mod servicemonitor;
pub mod statefulset;

/// Standard labels applied to all resources.
pub fn standard_labels(name: &str) -> std::collections::BTreeMap<String, String> {
    let mut labels = std::collections::BTreeMap::new();
    labels.insert("app.kubernetes.io/name".into(), "frogdb".into());
    labels.insert("app.kubernetes.io/instance".into(), name.into());
    labels.insert(
        "app.kubernetes.io/managed-by".into(),
        "frogdb-operator".into(),
    );
    labels
}

/// Config hash annotation key.
pub const CONFIG_HASH_ANNOTATION: &str = "frogdb.io/config-hash";
