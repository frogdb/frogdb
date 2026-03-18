use crate::replica::ReplicaReplicationHandler;
use crate::replica::connection::ConnectionState;
use crate::state::ReplicationState;
use std::net::SocketAddr;
use std::path::PathBuf;

#[test]
fn test_connection_state_display() {
    assert_eq!(format!("{}", ConnectionState::Disconnected), "disconnected");
    assert_eq!(format!("{}", ConnectionState::Streaming), "streaming");
}

#[tokio::test]
async fn test_replica_handler_creation() {
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    let state = ReplicationState::new();
    let data_dir = PathBuf::from("/tmp/frogdb-test");
    let (handler, _rx) = ReplicaReplicationHandler::new(addr, 6380, state, data_dir);
    let current_state = handler.state().await;
    assert!(!current_state.replication_id.is_empty());
}
