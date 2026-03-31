//! FrogDB reconciliation controller.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher::Config as WatcherConfig;
use kube::{Client, ResourceExt};
use tracing::{error, info, warn};

use crate::crd::{FrogDB, FrogDBCondition, FrogDBStatus};
use crate::{config_gen, resources};

/// Error types for the controller.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Validation error: {0}")]
    Validation(String),
}

/// Shared state for the controller.
pub struct Context {
    pub client: Client,
}

/// Start the controller.
pub async fn run(client: Client) -> anyhow::Result<()> {
    let frogdbs: Api<FrogDB> = Api::all(client.clone());
    let statefulsets: Api<StatefulSet> = Api::all(client.clone());
    let services: Api<Service> = Api::all(client.clone());
    let configmaps: Api<ConfigMap> = Api::all(client.clone());

    let ctx = Arc::new(Context { client });

    info!("Starting FrogDB controller");

    Controller::new(frogdbs, WatcherConfig::default())
        .owns(statefulsets, WatcherConfig::default())
        .owns(services, WatcherConfig::default())
        .owns(configmaps, WatcherConfig::default())
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!(resource = %o.0.name, "Reconciled"),
                Err(e) => error!(%e, "Reconciliation failed"),
            }
        })
        .await;

    Ok(())
}

/// Main reconcile function.
async fn reconcile(frogdb: Arc<FrogDB>, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = frogdb.name_any();
    let namespace = frogdb.namespace().unwrap_or_else(|| "default".to_string());

    info!(%name, %namespace, "Reconciling FrogDB");

    // 1. Validate spec
    if let Err(e) = frogdb.spec.validate() {
        warn!(%name, error = %e, "Invalid FrogDB spec");
        update_status(
            &ctx.client,
            &namespace,
            &name,
            &frogdb,
            0,
            vec![condition("Available", "False", "ValidationFailed", &e)],
            UpgradeState {
                in_progress: false,
                current_version: None,
                target_version: None,
            },
        )
        .await?;
        return Err(Error::Validation(e));
    }

    // 2. Generate frogdb.toml from spec
    let toml_content = config_gen::generate_toml(&frogdb.spec.config);
    let hash = resources::configmap::config_hash(&toml_content);

    // 3. Reconcile ConfigMap
    let cm = resources::configmap::build(&frogdb, &toml_content);
    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &namespace);
    cm_api
        .patch(
            cm.metadata.name.as_deref().unwrap(),
            &PatchParams::apply("frogdb-operator"),
            &Patch::Apply(&cm),
        )
        .await?;

    // 4. Reconcile headless Service
    let headless = resources::service::build_headless(&frogdb);
    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &namespace);
    svc_api
        .patch(
            headless.metadata.name.as_deref().unwrap(),
            &PatchParams::apply("frogdb-operator"),
            &Patch::Apply(&headless),
        )
        .await?;

    // 5. Reconcile client Service
    let client_svc = resources::service::build_client(&frogdb);
    svc_api
        .patch(
            client_svc.metadata.name.as_deref().unwrap(),
            &PatchParams::apply("frogdb-operator"),
            &Patch::Apply(&client_svc),
        )
        .await?;

    // 6. Detect image tag change (upgrade detection)
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &namespace);
    let desired_tag = &frogdb.spec.image.tag;
    let current_tag = sts_api
        .get_opt(&name)
        .await?
        .and_then(|sts| {
            sts.spec
                .and_then(|s| s.template.spec)
                .and_then(|ps| ps.containers.first().cloned())
                .and_then(|c| c.image)
        })
        .and_then(|image| image.rsplit_once(':').map(|(_, tag)| tag.to_string()));
    let is_tag_change = current_tag
        .as_ref()
        .is_some_and(|current| current != desired_tag);

    if is_tag_change {
        info!(
            %name,
            current = current_tag.as_deref().unwrap_or("unknown"),
            target = %desired_tag,
            "Image tag change detected — rolling upgrade in progress"
        );
    }

    // 7. Reconcile StatefulSet
    let sts = resources::statefulset::build(&frogdb, &hash);
    sts_api
        .patch(
            sts.metadata.name.as_deref().unwrap(),
            &PatchParams::apply("frogdb-operator"),
            &Patch::Apply(&sts),
        )
        .await?;

    // 8. Reconcile PDB
    if let Some(pdb) = resources::pdb::build(&frogdb) {
        let pdb_api: Api<PodDisruptionBudget> = Api::namespaced(ctx.client.clone(), &namespace);
        pdb_api
            .patch(
                pdb.metadata.name.as_deref().unwrap(),
                &PatchParams::apply("frogdb-operator"),
                &Patch::Apply(&pdb),
            )
            .await?;
    }

    // 9. Update status with upgrade awareness
    let ready = count_ready_pods(&ctx.client, &namespace, &name).await;
    let upgrade_in_progress = is_tag_change
        || (ready < frogdb.spec.replicas && current_tag.as_deref() != Some(desired_tag));
    let mut conditions = vec![if ready >= frogdb.spec.replicas {
        condition(
            "Available",
            "True",
            "AllReplicasReady",
            "All replicas ready",
        )
    } else {
        condition(
            "Available",
            "False",
            "ReplicasNotReady",
            &format!("{}/{} replicas ready", ready, frogdb.spec.replicas),
        )
    }];

    if upgrade_in_progress {
        conditions.push(condition(
            "Upgrading",
            "True",
            "RollingUpgrade",
            &format!(
                "Upgrading from {} to {}",
                current_tag.as_deref().unwrap_or("unknown"),
                desired_tag,
            ),
        ));
    } else {
        conditions.push(condition(
            "Progressing",
            "False",
            "ReconcileComplete",
            "Reconciliation complete",
        ));
    }

    update_status(
        &ctx.client,
        &namespace,
        &name,
        &frogdb,
        ready,
        conditions,
        UpgradeState {
            in_progress: upgrade_in_progress,
            current_version: current_tag.as_deref(),
            target_version: if upgrade_in_progress {
                Some(desired_tag)
            } else {
                None
            },
        },
    )
    .await?;

    // 9. Requeue after 30s
    Ok(Action::requeue(Duration::from_secs(30)))
}

/// Error policy: requeue with backoff on failure.
fn error_policy(_frogdb: Arc<FrogDB>, error: &Error, _ctx: Arc<Context>) -> Action {
    warn!(%error, "Reconciliation error, requeueing");
    Action::requeue(Duration::from_secs(15))
}

/// Count ready pods in the StatefulSet.
async fn count_ready_pods(client: &Client, namespace: &str, name: &str) -> i32 {
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
    match sts_api.get(name).await {
        Ok(sts) => sts.status.and_then(|s| s.ready_replicas).unwrap_or(0),
        Err(_) => 0,
    }
}

/// Upgrade state for status updates.
struct UpgradeState<'a> {
    in_progress: bool,
    current_version: Option<&'a str>,
    target_version: Option<&'a str>,
}

/// Update the FrogDB status subresource.
async fn update_status(
    client: &Client,
    namespace: &str,
    name: &str,
    frogdb: &FrogDB,
    ready_replicas: i32,
    conditions: Vec<FrogDBCondition>,
    upgrade: UpgradeState<'_>,
) -> Result<(), Error> {
    let api: Api<FrogDB> = Api::namespaced(client.clone(), namespace);

    let status = FrogDBStatus {
        replicas: frogdb.spec.replicas,
        ready_replicas,
        observed_generation: frogdb.metadata.generation.unwrap_or(0),
        primary_pod: if frogdb.spec.replicas > 1 || frogdb.spec.mode == "cluster" {
            Some(format!("{}-0", name))
        } else {
            None
        },
        upgrade_in_progress: upgrade.in_progress,
        current_version: upgrade.current_version.map(|s| s.to_string()),
        target_version: upgrade.target_version.map(|s| s.to_string()),
        conditions,
    };

    let patch = serde_json::json!({ "status": status });
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;

    Ok(())
}

fn condition(type_: &str, status: &str, reason: &str, message: &str) -> FrogDBCondition {
    FrogDBCondition {
        type_: type_.to_string(),
        status: status.to_string(),
        last_transition_time: Some(chrono_now()),
        reason: Some(reason.to_string()),
        message: Some(message.to_string()),
    }
}

fn chrono_now() -> String {
    // ISO 8601 format without chrono dependency
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    format!("{}Z", now)
}
