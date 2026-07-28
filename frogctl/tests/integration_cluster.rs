use crate::common::setup::{ctx_for_port, ctx_for_server, ctx_for_server_json};
use frogctl::commands::cluster::{self, ClusterCommand};
use frogdb_test_harness::server::TestServer;

/// End-to-end: `cluster check` reads a live server's `CLUSTER NODES`, finds no
/// epoch collision, and exits 0. A standalone server renders a single-node
/// table, which is the trivially-consistent case.
#[tokio::test]
async fn test_cluster_check_passes_against_live_server() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let exit_code = cluster::run(&ClusterCommand::Check, &mut ctx)
        .await
        .unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_cluster_check_json_output() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server_json(&server);

    let exit_code = cluster::run(&ClusterCommand::Check, &mut ctx)
        .await
        .unwrap();
    assert_eq!(exit_code, 0);
}

/// An unreachable server is an error, not a passing check: reporting "no
/// collisions" for a cluster it never read would be worse than failing loudly.
#[tokio::test]
async fn test_cluster_check_unreachable_server_errors() {
    let mut ctx = ctx_for_port(1);

    let result = cluster::run(&ClusterCommand::Check, &mut ctx).await;
    assert!(result.is_err(), "expected an error, got {result:?}");
}
