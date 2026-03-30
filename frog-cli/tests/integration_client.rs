use crate::common::setup::ctx_for_server;
use frog_cli::commands::client::{self, ClientCommand};
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn test_client_list() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let cmd = ClientCommand::List { client_type: None };
    let exit_code = client::run(&cmd, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_client_info() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let exit_code = client::run(&ClientCommand::Info, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}
