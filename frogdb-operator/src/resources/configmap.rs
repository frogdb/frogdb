//! ConfigMap builder for frogdb.toml.

use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::ObjectMeta;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use crate::crd::FrogDB;

/// Build the ConfigMap containing frogdb.toml.
pub fn build(frogdb: &FrogDB, toml_content: &str) -> ConfigMap {
    let name = frogdb.metadata.name.as_deref().unwrap_or("frogdb");
    let namespace = frogdb.metadata.namespace.as_deref().unwrap_or("default");

    let mut data = BTreeMap::new();
    data.insert("frogdb.toml".to_string(), toml_content.to_string());

    ConfigMap {
        metadata: ObjectMeta {
            name: Some(format!("{}-config", name)),
            namespace: Some(namespace.to_string()),
            labels: Some(super::standard_labels(name)),
            owner_references: Some(vec![owner_ref_from(frogdb)]),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    }
}

/// Compute SHA256 hash of the TOML content for rollout triggering.
pub fn config_hash(toml_content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(toml_content.as_bytes());
    hex::encode(hasher.finalize())[..16].to_string()
}

/// Build an owner reference for the FrogDB CR.
pub fn owner_ref_from(frogdb: &FrogDB) -> k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
    k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
        api_version: "frogdb.io/v1alpha1".to_string(),
        kind: "FrogDB".to_string(),
        name: frogdb
            .metadata
            .name
            .clone()
            .unwrap_or_default(),
        uid: frogdb.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_hash_deterministic() {
        let h1 = config_hash("hello");
        let h2 = config_hash("hello");
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 16);
    }

    #[test]
    fn test_config_hash_different_content() {
        let h1 = config_hash("hello");
        let h2 = config_hash("world");
        assert_ne!(h1, h2);
    }
}
