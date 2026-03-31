use crate::common::setup::ctx_for_server;
use frogctl::commands::config::{self, ConfigCommand};
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn test_config_show() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let cmd = ConfigCommand::Show {
        section: None,
        diff: None,
    };
    let exit_code = config::run(&cmd, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_config_show_section() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let cmd = ConfigCommand::Show {
        section: Some("maxmemory".to_string()),
        diff: None,
    };
    let exit_code = config::run(&cmd, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}
