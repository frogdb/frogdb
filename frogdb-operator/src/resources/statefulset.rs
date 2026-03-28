//! StatefulSet builder for FrogDB.

use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVar, EnvVarSource, ObjectFieldSelector,
    PersistentVolumeClaim, PersistentVolumeClaimSpec, PodSpec, PodTemplateSpec,
    ResourceRequirements as K8sResourceRequirements, Volume, VolumeMount,
    VolumeResourceRequirements,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::api::ObjectMeta;
use std::collections::BTreeMap;

use crate::crd::FrogDB;

/// Build the StatefulSet for a FrogDB instance.
pub fn build(frogdb: &FrogDB, config_hash: &str) -> StatefulSet {
    let name = frogdb.metadata.name.as_deref().unwrap_or("frogdb");
    let ns = frogdb.metadata.namespace.as_deref().unwrap_or("default");
    let labels = super::standard_labels(name);
    let spec = &frogdb.spec;

    let mut annotations = BTreeMap::new();
    annotations.insert(
        super::CONFIG_HASH_ANNOTATION.to_string(),
        config_hash.to_string(),
    );

    let mut env = vec![
        EnvVar {
            name: "POD_NAME".into(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    field_path: "metadata.name".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "POD_IP".into(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    field_path: "status.podIP".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
    ];

    let mut ports = vec![ContainerPort {
        name: Some("frogdb".into()),
        container_port: spec.config.port as i32,
        ..Default::default()
    }];

    if spec.config.metrics.enabled {
        ports.push(ContainerPort {
            name: Some("metrics".into()),
            container_port: spec.config.metrics.port as i32,
            ..Default::default()
        });
    }

    // Mode-specific configuration
    let command = match spec.mode.as_str() {
        "cluster" => {
            let cluster = spec.cluster.as_ref().cloned().unwrap_or_default();
            ports.push(ContainerPort {
                name: Some("cluster-bus".into()),
                container_port: cluster.bus_port as i32,
                ..Default::default()
            });

            env.push(EnvVar {
                name: "FROGDB_CLUSTER__ENABLED".into(),
                value: Some("true".into()),
                ..Default::default()
            });
            env.push(EnvVar {
                name: "FROGDB_CLUSTER__CLUSTER_BUS_ADDR".into(),
                value: Some(format!("$(POD_IP):{}", cluster.bus_port)),
                ..Default::default()
            });
            env.push(EnvVar {
                name: "FROGDB_CLUSTER__CLIENT_ADDR".into(),
                value: Some(format!("$(POD_IP):{}", spec.config.port)),
                ..Default::default()
            });

            // Build initial_nodes list
            let initial_nodes: Vec<String> = (0..spec.replicas)
                .map(|i| {
                    format!(
                        "{name}-{i}.{name}-headless.{ns}.svc.cluster.local:{}",
                        cluster.bus_port
                    )
                })
                .collect();
            env.push(EnvVar {
                name: "FROGDB_CLUSTER__INITIAL_NODES".into(),
                value: Some(initial_nodes.join(",")),
                ..Default::default()
            });

            vec![
                "frogdb-server".to_string(),
                "--config".to_string(),
                "/etc/frogdb/frogdb.toml".to_string(),
            ]
        }
        _ if spec.replicas > 1 => {
            // Standalone with replicas: shell wrapper for ordinal-based role
            vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                format!(
                    r#"ORDINAL=${{HOSTNAME##*-}}
if [ "$ORDINAL" = "0" ]; then
  export FROGDB_REPLICATION__ROLE=primary
else
  export FROGDB_REPLICATION__ROLE=replica
  export FROGDB_REPLICATION__PRIMARY_HOST={name}-0.{name}-headless.{ns}.svc.cluster.local
  export FROGDB_REPLICATION__PRIMARY_PORT={port}
fi
exec frogdb-server --config /etc/frogdb/frogdb.toml"#,
                    port = spec.config.port
                ),
            ]
        }
        _ => {
            // Single standalone
            vec![
                "frogdb-server".to_string(),
                "--config".to_string(),
                "/etc/frogdb/frogdb.toml".to_string(),
            ]
        }
    };

    let (command_field, args_field) = if command[0] == "/bin/sh" {
        (Some(vec![command[0].clone()]), Some(command[1..].to_vec()))
    } else {
        (Some(command), None)
    };

    // Resource requirements
    let resources = spec.resources.as_ref().map(|r| {
        K8sResourceRequirements {
            requests: r.requests.as_ref().map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), Quantity(v.clone())))
                    .collect()
            }),
            limits: r.limits.as_ref().map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), Quantity(v.clone())))
                    .collect()
            }),
            ..Default::default()
        }
    });

    let container = Container {
        name: "frogdb".into(),
        image: Some(format!("{}:{}", spec.image.repository, spec.image.tag)),
        image_pull_policy: Some(spec.image.pull_policy.clone()),
        command: command_field,
        args: args_field,
        ports: Some(ports),
        env: Some(env),
        resources,
        volume_mounts: Some(vec![
            VolumeMount {
                name: "config".into(),
                mount_path: "/etc/frogdb".into(),
                ..Default::default()
            },
            VolumeMount {
                name: "data".into(),
                mount_path: "/data".into(),
                ..Default::default()
            },
        ]),
        ..Default::default()
    };

    // PVC template
    let mut pvc_resources = BTreeMap::new();
    pvc_resources.insert("storage".to_string(), Quantity(spec.storage.size.clone()));

    let storage_class = if spec.storage.storage_class.is_empty() {
        None
    } else {
        Some(spec.storage.storage_class.clone())
    };

    let pvc_template = PersistentVolumeClaim {
        metadata: ObjectMeta {
            name: Some("data".into()),
            ..Default::default()
        },
        spec: Some(PersistentVolumeClaimSpec {
            access_modes: Some(vec!["ReadWriteOnce".into()]),
            resources: Some(VolumeResourceRequirements {
                requests: Some(pvc_resources),
                ..Default::default()
            }),
            storage_class_name: storage_class,
            ..Default::default()
        }),
        ..Default::default()
    };

    StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(ns.to_string()),
            labels: Some(labels.clone()),
            owner_references: Some(vec![super::configmap::owner_ref_from(frogdb)]),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(spec.replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            service_name: format!("{}-headless", name),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    annotations: Some(annotations),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container],
                    volumes: Some(vec![Volume {
                        name: "config".into(),
                        config_map: Some(
                            k8s_openapi::api::core::v1::ConfigMapVolumeSource {
                                name: format!("{}-config", name),
                                ..Default::default()
                            },
                        ),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }),
            },
            volume_claim_templates: Some(vec![pvc_template]),
            ..Default::default()
        }),
        ..Default::default()
    }
}
