//! ServiceMonitor builder for Prometheus Operator integration.
//!
//! Uses raw JSON since ServiceMonitor is a CRD not in k8s-openapi.

use serde_json::json;

use crate::crd::FrogDB;

/// Build a ServiceMonitor as a DynamicObject JSON value.
#[allow(dead_code)]
pub fn build(frogdb: &FrogDB) -> Option<serde_json::Value> {
    if !frogdb.spec.config.metrics.enabled {
        return None;
    }

    let name = frogdb.metadata.name.as_deref().unwrap_or("frogdb");
    let ns = frogdb.metadata.namespace.as_deref().unwrap_or("default");
    let labels = super::standard_labels(name);
    let uid = frogdb.metadata.uid.as_deref().unwrap_or("");

    Some(json!({
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "ServiceMonitor",
        "metadata": {
            "name": format!("{}-metrics", name),
            "namespace": ns,
            "labels": labels,
            "ownerReferences": [{
                "apiVersion": "frogdb.io/v1alpha1",
                "kind": "FrogDB",
                "name": name,
                "uid": uid,
                "controller": true,
                "blockOwnerDeletion": true,
            }]
        },
        "spec": {
            "selector": {
                "matchLabels": labels,
            },
            "endpoints": [{
                "port": "metrics",
                "interval": "30s",
                "scrapeTimeout": "10s",
            }]
        }
    }))
}
