use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

#[tokio::test]
async fn command_info_returns_correct_structure() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // COMMAND INFO GET should return info with 6-element array
    let resp = client.command(&["COMMAND", "INFO", "GET"]).await;
    let outer = unwrap_array(resp);
    assert_eq!(outer.len(), 1);

    let info = unwrap_array(outer.into_iter().next().unwrap());
    assert!(
        info.len() >= 6,
        "COMMAND INFO should return at least 6 fields"
    );

    // First element is the command name
    let name = String::from_utf8(unwrap_bulk(&info[0]).to_vec()).unwrap();
    assert_eq!(name.to_uppercase(), "GET");

    // Second element is arity (integer)
    let _arity = unwrap_integer(&info[1]);

    // Third element is flags (array)
    match &info[2] {
        frogdb_protocol::Response::Array(_) => {}
        other => panic!("expected flags array, got {other:?}"),
    }
}

#[tokio::test]
async fn command_info_multiple_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "INFO", "SET", "GET", "DEL"])
        .await;
    let outer = unwrap_array(resp);
    assert_eq!(outer.len(), 3, "should return info for 3 commands");
}

#[tokio::test]
async fn command_info_returns_integer_arity() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Verify that arity is an integer for known commands
    for cmd in &["SET", "GET", "DEL", "PING"] {
        let resp = client.command(&["COMMAND", "INFO", cmd]).await;
        let outer = unwrap_array(resp);
        let info = unwrap_array(outer.into_iter().next().unwrap());
        // Arity must be an integer
        let _arity = unwrap_integer(&info[1]);
    }
}
