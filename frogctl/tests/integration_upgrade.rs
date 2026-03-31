use crate::common::setup::{ctx_for_server, ctx_with_admin};
use frogctl::commands::upgrade::{self, UpgradeCommand};
use frogdb_test_harness::server::{TestServer, TestServerConfig};

#[tokio::test]
async fn test_upgrade_status_resp_fallback() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let exit_code = upgrade::run(&UpgradeCommand::Status, &mut ctx)
        .await
        .unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_upgrade_status_admin() {
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        admin_enabled: true,
        ..TestServerConfig::default()
    })
    .await;
    let mut ctx = ctx_with_admin(&server);

    let exit_code = upgrade::run(&UpgradeCommand::Status, &mut ctx)
        .await
        .unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_upgrade_check_valid_jump() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    // Current version is 0.1.0, so 0.2.0 is one minor up
    let exit_code = upgrade::run(
        &UpgradeCommand::Check {
            target_version: "0.2.0".to_string(),
        },
        &mut ctx,
    )
    .await
    .unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_upgrade_check_too_large_jump() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    // 0.1.0 → 0.3.0 skips a minor version
    let exit_code = upgrade::run(
        &UpgradeCommand::Check {
            target_version: "0.3.0".to_string(),
        },
        &mut ctx,
    )
    .await
    .unwrap();
    assert_eq!(exit_code, 1);
}

#[tokio::test]
async fn test_upgrade_check_downgrade() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let exit_code = upgrade::run(
        &UpgradeCommand::Check {
            target_version: "0.0.1".to_string(),
        },
        &mut ctx,
    )
    .await
    .unwrap();
    assert_eq!(exit_code, 1);
}

#[tokio::test]
async fn test_upgrade_plan_standalone() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let exit_code = upgrade::run(
        &UpgradeCommand::Plan {
            target_version: "0.2.0".to_string(),
        },
        &mut ctx,
    )
    .await
    .unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_upgrade_rollback_safe() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    // No finalization — rollback should be safe
    let exit_code = upgrade::run(&UpgradeCommand::Rollback, &mut ctx)
        .await
        .unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_upgrade_finalize_version_mismatch() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    // Target "99.0.0" doesn't match cluster version "0.1.0"
    let exit_code = upgrade::run(
        &UpgradeCommand::Finalize {
            version: "99.0.0".to_string(),
            yes: true,
        },
        &mut ctx,
    )
    .await
    .unwrap();
    assert_eq!(exit_code, 1);
}
