//! PodDisruptionBudget builder for FrogDB.

use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::api::policy::v1::PodDisruptionBudgetSpec;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::ObjectMeta;

use crate::crd::FrogDB;

/// Build a PodDisruptionBudget for a FrogDB instance.
pub fn build(frogdb: &FrogDB) -> Option<PodDisruptionBudget> {
    if !frogdb.spec.pod_disruption_budget.enabled {
        return None;
    }

    let name = frogdb.metadata.name.as_deref().unwrap_or("frogdb");
    let ns = frogdb.metadata.namespace.as_deref().unwrap_or("default");
    let min_available = frogdb.spec.effective_min_available();

    Some(PodDisruptionBudget {
        metadata: ObjectMeta {
            name: Some(format!("{}-pdb", name)),
            namespace: Some(ns.to_string()),
            labels: Some(super::standard_labels(name)),
            owner_references: Some(vec![super::configmap::owner_ref_from(frogdb)]),
            ..Default::default()
        },
        spec: Some(PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(min_available)),
            selector: Some(LabelSelector {
                match_labels: Some(super::standard_labels(name)),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    })
}
