//! Integration tests for workflow-gen.
//!
//! These tests verify that the generated workflows are valid GitHub Actions YAML.

use workflow_gen::workflows;

/// Test that all workflows can be serialized to YAML without errors.
#[test]
fn test_all_workflows_serialize() {
    // Test workflow
    let test = workflows::test::test_workflow();
    let yaml = test.to_string().expect("test workflow should serialize");
    assert!(yaml.contains("name: Test"));
    assert!(yaml.contains("lint"));
    assert!(yaml.contains("unit-tests"));

    // Build workflow
    let build = workflows::build::build_workflow();
    let yaml = build.to_string().expect("build workflow should serialize");
    assert!(yaml.contains("name: Build"));
    assert!(yaml.contains("docker"));

    // Release workflow
    let release = workflows::release::release_workflow();
    let yaml = release.to_string().expect("release workflow should serialize");
    assert!(yaml.contains("name: Release"));
    assert!(yaml.contains("build-binaries"));
    assert!(yaml.contains("helm"));

    // Deploy workflow
    let deploy = workflows::deploy::deploy_workflow();
    let yaml = deploy.to_string().expect("deploy workflow should serialize");
    assert!(yaml.contains("name: Deploy"));
    assert!(yaml.contains("workflow_dispatch"));
    assert!(yaml.contains("terraform"));
}

/// Test that the test workflow has all expected jobs.
#[test]
fn test_workflow_has_expected_jobs() {
    let workflow = workflows::test::test_workflow();
    let yaml = workflow.to_string().unwrap();

    // Check all expected jobs are present
    let expected_jobs = [
        "lint",
        "unit-tests",
        "shuttle-tests",
        "turmoil-tests",
        "helm-gen-check",
        "workflow-gen-check",
        "helm-lint",
    ];

    for job in expected_jobs {
        assert!(
            yaml.contains(&format!("{}:", job)),
            "Expected job '{}' not found in test workflow",
            job
        );
    }
}

/// Test that the build workflow has matrix strategy.
#[test]
fn test_build_workflow_has_matrix() {
    let workflow = workflows::build::build_workflow();
    let yaml = workflow.to_string().unwrap();

    // Check matrix strategy is present
    assert!(yaml.contains("matrix"), "Build workflow should have matrix");
    assert!(
        yaml.contains("x86_64-unknown-linux-gnu"),
        "Build should target x86_64 linux"
    );
    assert!(
        yaml.contains("aarch64-unknown-linux-gnu"),
        "Build should target aarch64 linux"
    );
}

/// Test that the release workflow has all platforms.
#[test]
fn test_release_workflow_has_platforms() {
    let workflow = workflows::release::release_workflow();
    let yaml = workflow.to_string().unwrap();

    // Check all platforms
    let expected_targets = [
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
        "x86_64-apple-darwin",
        "aarch64-apple-darwin",
    ];

    for target in expected_targets {
        assert!(
            yaml.contains(target),
            "Expected target '{}' not found in release workflow",
            target
        );
    }
}

/// Test that the deploy workflow has all cloud providers.
#[test]
fn test_deploy_workflow_has_cloud_providers() {
    let workflow = workflows::deploy::deploy_workflow();
    let yaml = workflow.to_string().unwrap();

    // Check cloud provider options
    assert!(yaml.contains("- aws"), "Deploy should support AWS");
    assert!(yaml.contains("- gcp"), "Deploy should support GCP");
    assert!(yaml.contains("- azure"), "Deploy should support Azure");

    // Check environment options
    assert!(yaml.contains("- dev"), "Deploy should support dev environment");
    assert!(
        yaml.contains("- staging"),
        "Deploy should support staging environment"
    );
    assert!(
        yaml.contains("- production"),
        "Deploy should support production environment"
    );

    // Check terraform actions
    assert!(yaml.contains("- plan"), "Deploy should support plan action");
    assert!(yaml.contains("- apply"), "Deploy should support apply action");
    assert!(
        yaml.contains("- destroy"),
        "Deploy should support destroy action"
    );
}

/// Test that workflows have proper triggers.
#[test]
fn test_workflow_triggers() {
    // Test workflow triggers on push and pull_request to main
    let test = workflows::test::test_workflow();
    let yaml = test.to_string().unwrap();
    assert!(yaml.contains("push:"), "Test workflow should trigger on push");
    assert!(
        yaml.contains("pull_request:"),
        "Test workflow should trigger on pull_request"
    );

    // Build workflow triggers on push and pull_request to main
    let build = workflows::build::build_workflow();
    let yaml = build.to_string().unwrap();
    assert!(
        yaml.contains("push:"),
        "Build workflow should trigger on push"
    );
    assert!(
        yaml.contains("pull_request:"),
        "Build workflow should trigger on pull_request"
    );

    // Release workflow triggers on version tags
    let release = workflows::release::release_workflow();
    let yaml = release.to_string().unwrap();
    assert!(
        yaml.contains("v*"),
        "Release workflow should trigger on version tags"
    );

    // Deploy workflow triggers on workflow_dispatch
    let deploy = workflows::deploy::deploy_workflow();
    let yaml = deploy.to_string().unwrap();
    assert!(
        yaml.contains("workflow_dispatch:"),
        "Deploy workflow should trigger on workflow_dispatch"
    );
}

// Snapshot tests are available but require cargo-insta to be installed.
// Run `cargo install cargo-insta` then `cargo insta test --accept -p workflow-gen`
// to generate and accept snapshots.
//
// #[cfg(test)]
// mod snapshot_tests {
//     use super::*;
//
//     #[test]
//     fn test_workflow_snapshot() {
//         let workflow = workflows::test::test_workflow();
//         let yaml = workflow.to_string().unwrap();
//         insta::assert_snapshot!("test_workflow", yaml);
//     }
//
//     // Additional snapshot tests for build, release, and deploy workflows
// }
