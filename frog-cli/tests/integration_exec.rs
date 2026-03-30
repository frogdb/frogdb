use crate::common::setup::{ctx_for_server, ctx_for_server_json};
use frog_cli::commands::exec::{self, ExecArgs};
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn test_exec_ping() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let args = ExecArgs {
        command: "PING".to_string(),
        args: vec![],
        repeat: 1,
        interval: 0,
    };
    let exit_code = exec::run(&args, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_exec_set_get() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let set_args = ExecArgs {
        command: "SET".to_string(),
        args: vec!["mykey".to_string(), "myvalue".to_string()],
        repeat: 1,
        interval: 0,
    };
    let exit_code = exec::run(&set_args, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);

    let get_args = ExecArgs {
        command: "GET".to_string(),
        args: vec!["mykey".to_string()],
        repeat: 1,
        interval: 0,
    };
    let exit_code = exec::run(&get_args, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_exec_invalid_command() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let args = ExecArgs {
        command: "NOTAREALCOMMAND".to_string(),
        args: vec![],
        repeat: 1,
        interval: 0,
    };
    let result = exec::run(&args, &mut ctx).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_exec_repeat() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let args = ExecArgs {
        command: "PING".to_string(),
        args: vec![],
        repeat: 3,
        interval: 0,
    };
    let exit_code = exec::run(&args, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_exec_json_output() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server_json(&server);

    let args = ExecArgs {
        command: "SET".to_string(),
        args: vec!["k".to_string(), "v".to_string()],
        repeat: 1,
        interval: 0,
    };
    let exit_code = exec::run(&args, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}
