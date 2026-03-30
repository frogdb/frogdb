use crate::common::setup::ctx_for_server;
use frog_cli::commands::scan::{self, ScanArgs};
use frogdb_test_harness::server::TestServer;

fn default_scan_args() -> ScanArgs {
    ScanArgs {
        match_pattern: "*".to_string(),
        key_type: None,
        with_ttl: false,
        with_type: false,
        with_memory: false,
        limit: None,
        count: 100,
    }
}

/// Helper to populate keys via the ConnectionContext.
async fn populate_keys(ctx: &mut frog_cli::connection::ConnectionContext, count: usize) {
    for i in 0..count {
        ctx.cmd("SET", &[&format!("key:{i}"), &format!("val:{i}")])
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_scan_empty() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let args = default_scan_args();
    let exit_code = scan::run(&args, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_scan_with_keys() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    populate_keys(&mut ctx, 5).await;

    let args = default_scan_args();
    let exit_code = scan::run(&args, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_scan_with_pattern() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    populate_keys(&mut ctx, 5).await;
    // Add some keys with a different prefix
    ctx.cmd("SET", &["other:1", "val"]).await.unwrap();
    ctx.cmd("SET", &["other:2", "val"]).await.unwrap();

    let args = ScanArgs {
        match_pattern: "key:*".to_string(),
        ..default_scan_args()
    };
    let exit_code = scan::run(&args, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_scan_with_limit() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    populate_keys(&mut ctx, 10).await;

    let args = ScanArgs {
        limit: Some(3),
        ..default_scan_args()
    };
    let exit_code = scan::run(&args, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_scan_with_enrichment() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    populate_keys(&mut ctx, 3).await;

    let args = ScanArgs {
        with_ttl: true,
        with_type: true,
        with_memory: true,
        ..default_scan_args()
    };
    let exit_code = scan::run(&args, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}
