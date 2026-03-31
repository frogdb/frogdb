use crate::common::setup::ctx_for_server;
use frogctl::commands::acl::{self, AclCommand};
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn test_acl_whoami() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let exit_code = acl::run(&AclCommand::Whoami, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_acl_list() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let exit_code = acl::run(&AclCommand::List, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_acl_users() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let exit_code = acl::run(&AclCommand::Users, &mut ctx).await.unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_acl_setuser_getuser() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    // Create a user
    let exit_code = acl::run(
        &AclCommand::Setuser {
            name: "testuser".to_string(),
            rules: vec![
                "on".to_string(),
                ">password123".to_string(),
                "~*".to_string(),
                "+@all".to_string(),
            ],
        },
        &mut ctx,
    )
    .await
    .unwrap();
    assert_eq!(exit_code, 0);

    // Get the user
    let exit_code = acl::run(
        &AclCommand::Getuser {
            name: "testuser".to_string(),
        },
        &mut ctx,
    )
    .await
    .unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_acl_deluser() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    // Create then delete
    acl::run(
        &AclCommand::Setuser {
            name: "tmpuser".to_string(),
            rules: vec!["on".to_string()],
        },
        &mut ctx,
    )
    .await
    .unwrap();

    let exit_code = acl::run(
        &AclCommand::Deluser {
            names: vec!["tmpuser".to_string()],
        },
        &mut ctx,
    )
    .await
    .unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_acl_genpass() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let exit_code = acl::run(&AclCommand::Genpass { bits: None }, &mut ctx)
        .await
        .unwrap();
    assert_eq!(exit_code, 0);
}

#[tokio::test]
async fn test_acl_cat() {
    let server = TestServer::start_standalone().await;
    let mut ctx = ctx_for_server(&server);

    let exit_code = acl::run(&AclCommand::Cat { category: None }, &mut ctx)
        .await
        .unwrap();
    assert_eq!(exit_code, 0);
}
