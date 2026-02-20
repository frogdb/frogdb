//! Helpers for ACL integration tests.
//!
//! Extracts the repeated "start server with security, authenticate admin,
//! create user, authenticate user" setup flow.

use frogdb_protocol::Response;

use super::test_server::{TestClient, TestServer};

/// Start a server with `requirepass` set and return an already-authenticated
/// admin client (the `default` user).
pub async fn start_server_with_admin(password: &str) -> (TestServer, TestClient) {
    let server = TestServer::start_with_security(password).await;
    let mut admin = server.connect().await;

    let response = admin.command(&["AUTH", password]).await;
    assert_eq!(response, Response::ok(), "admin AUTH failed");

    (server, admin)
}

/// Create a new user via `ACL SETUSER`, connect a fresh client, and
/// authenticate it.  Returns the authenticated client.
///
/// `permissions` are the ACL rule tokens after `on >password`, e.g.
/// `&["+@all", "~*"]`.
pub async fn create_and_auth_user(
    server: &TestServer,
    admin: &mut TestClient,
    username: &str,
    password: &str,
    permissions: &[&str],
) -> TestClient {
    let pass_rule = format!(">{password}");
    let mut cmd_args: Vec<String> = vec![
        "ACL".into(),
        "SETUSER".into(),
        username.into(),
        "on".into(),
        pass_rule,
    ];
    for p in permissions {
        cmd_args.push((*p).into());
    }
    let cmd_refs: Vec<&str> = cmd_args.iter().map(|s| s.as_str()).collect();

    let response = admin.command(&cmd_refs).await;
    assert_eq!(response, Response::ok(), "ACL SETUSER {username} failed");

    let mut client = server.connect().await;
    let response = client.command(&["AUTH", username, password]).await;
    assert_eq!(response, Response::ok(), "AUTH {username} failed");

    client
}
