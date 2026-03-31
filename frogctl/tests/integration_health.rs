use crate::common::setup::{
    ctx_for_port, ctx_for_server, ctx_for_server_json, ctx_with_admin, ctx_with_metrics,
};
use frogctl::commands::health::{self, HealthArgs};
use frogdb_test_harness::server::{TestServer, TestServerConfig};

fn default_health_args() -> HealthArgs {
    HealthArgs {
        admin: false,
        live: false,
        ready: false,
        all: None,
        json: false,
    }
}

#[tokio::test]
async fn test_health_resp() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let args = default_health_args();
    let exit_code = health::run(&args, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_health_unreachable() {
    // Use a port that nothing is listening on
    let mut ctx = ctx_for_port(1);

    let args = default_health_args();
    let exit_code = health::run(&args, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 2); // Unreachable
}

#[tokio::test]
async fn test_health_json_output() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server_json(&server);

    let args = HealthArgs {
        json: true,
        ..default_health_args()
    };
    let exit_code = health::run(&args, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_health_admin() {
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        admin_enabled: true,
        ..TestServerConfig::default()
    })
    .await;
    let mut ctx = ctx_with_admin(&server);

    let args = HealthArgs {
        admin: true,
        ..default_health_args()
    };
    let exit_code = health::run(&args, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_health_live_probe() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_with_metrics(&server);

    let args = HealthArgs {
        live: true,
        ..default_health_args()
    };
    let exit_code = health::run(&args, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_health_fanout_single_node() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let addr = format!("127.0.0.1:{}", server.port());
    let args = HealthArgs {
        all: Some(vec![addr]),
        ..default_health_args()
    };
    let exit_code = health::run(&args, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}
