use crate::common::setup::ctx_for_server;
use frogctl::commands::data::{self, DataCommand};
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn test_data_slot() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let cmd = DataCommand::Slot {
        key: "mykey".to_string(),
        internal: false,
    };
    let exit_code = data::run(&cmd, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_data_slot_internal() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let cmd = DataCommand::Slot {
        key: "mykey".to_string(),
        internal: true,
    };
    let exit_code = data::run(&cmd, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}
