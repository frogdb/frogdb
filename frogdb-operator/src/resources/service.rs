//! Service builders for FrogDB.

use k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec};
use kube::api::ObjectMeta;

use crate::crd::FrogDB;

/// Build the headless Service for StatefulSet DNS and cluster discovery.
pub fn build_headless(frogdb: &FrogDB) -> Service {
    let name = frogdb.metadata.name.as_deref().unwrap_or("frogdb");
    let ns = frogdb.metadata.namespace.as_deref().unwrap_or("default");
    let port = frogdb.spec.config.port;

    let mut ports = vec![ServicePort {
        name: Some("frogdb".into()),
        port: port as i32,
        ..Default::default()
    }];

    if frogdb.spec.config.metrics.enabled {
        ports.push(ServicePort {
            name: Some("metrics".into()),
            port: frogdb.spec.config.metrics.port as i32,
            ..Default::default()
        });
    }

    if frogdb.spec.mode == "cluster" {
        let bus_port = frogdb
            .spec
            .cluster
            .as_ref()
            .map(|c| c.bus_port)
            .unwrap_or(16379);
        ports.push(ServicePort {
            name: Some("cluster-bus".into()),
            port: bus_port as i32,
            ..Default::default()
        });
    }

    Service {
        metadata: ObjectMeta {
            name: Some(format!("{}-headless", name)),
            namespace: Some(ns.to_string()),
            labels: Some(super::standard_labels(name)),
            owner_references: Some(vec![super::configmap::owner_ref_from(frogdb)]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(super::standard_labels(name)),
            ports: Some(ports),
            cluster_ip: Some("None".into()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Build the ClusterIP Service for client access.
pub fn build_client(frogdb: &FrogDB) -> Service {
    let name = frogdb.metadata.name.as_deref().unwrap_or("frogdb");
    let ns = frogdb.metadata.namespace.as_deref().unwrap_or("default");
    let port = frogdb.spec.config.port;

    Service {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(ns.to_string()),
            labels: Some(super::standard_labels(name)),
            owner_references: Some(vec![super::configmap::owner_ref_from(frogdb)]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(super::standard_labels(name)),
            ports: Some(vec![ServicePort {
                name: Some("frogdb".into()),
                port: port as i32,
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}
