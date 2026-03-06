use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn auth_with_correct_password_succeeds() {
    let server = TestServer::start_with_security("foobar").await;
    let mut client = server.connect().await;

    let resp = client.command(&["AUTH", "foobar"]).await;
    assert_ok(&resp);
}

#[tokio::test]
async fn auth_with_wrong_password_returns_error() {
    let server = TestServer::start_with_security("foobar").await;
    let mut client = server.connect().await;

    let resp = client.command(&["AUTH", "wrongpass"]).await;
    assert_error_prefix(&resp, "WRONGPASS");
}

#[tokio::test]
async fn commands_rejected_before_auth() {
    let server = TestServer::start_with_security("foobar").await;
    let mut client = server.connect().await;

    // Non-exempt commands require AUTH when requirepass is set
    let resp = client.command(&["SET", "foo", "bar"]).await;
    assert_error_prefix(&resp, "NOAUTH");

    let resp = client.command(&["GET", "foo"]).await;
    assert_error_prefix(&resp, "NOAUTH");
}

#[tokio::test]
async fn auth_arity_check() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // AUTH with wrong number of args: zero args (just AUTH) should error
    let resp = client.command(&["AUTH"]).await;
    assert!(
        matches!(resp, frogdb_protocol::Response::Error(_)),
        "AUTH with no args should return an error, got {resp:?}"
    );
}

#[tokio::test]
async fn after_auth_commands_succeed() {
    let server = TestServer::start_with_security("secret").await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["AUTH", "secret"]).await);
    assert_ok(&client.command(&["SET", "foo", "bar"]).await);
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"bar");
}
