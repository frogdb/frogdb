//! End-to-end lifecycle of a *parked* blocking command: the two ways a
//! connection can end underneath one.
//!
//! A blocked client is a state on a still-readable connection, not a suspended
//! read loop (`specs/blocking.md` TR-BLOCKING-013, TR-BLOCKING-021). These tests
//! drive that through a real socket, which is the only place the distinction is
//! observable: they close / kill a connection that is parked in `BLPOP key 0`
//! and assert the server reclaims the waiter and conserves the data.

use std::time::Duration;

use crate::common::test_server::TestServer;
use frogdb_protocol::Response;

/// Poll `DEBUG WAITQUEUE` until it reports the wanted emptiness, or give up.
///
/// The empty queue is the `# wait queue is empty` bulk sentinel; a populated one
/// is a (non-empty) RESP2-flattened map.
async fn wait_for_queue_empty(
    probe: &mut crate::common::test_server::TestClient,
    want_empty: bool,
) {
    for _ in 0..100 {
        let resp = probe.command(&["DEBUG", "WAITQUEUE"]).await;
        let is_empty = match &resp {
            Response::Bulk(Some(b)) => &b[..] == b"# wait queue is empty",
            Response::Array(items) => items.is_empty(),
            other => panic!("unexpected DEBUG WAITQUEUE reply: {other:?}"),
        };
        if is_empty == want_empty {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "DEBUG WAITQUEUE never became {}",
        if want_empty { "empty" } else { "populated" }
    );
}

fn integer(resp: &Response) -> i64 {
    match resp {
        Response::Integer(n) => *n,
        other => panic!("expected an integer reply, got {other:?}"),
    }
}

/// distsys-review CRIT-4. A client parked in `BLPOP` whose process dies must not
/// leave a waiter behind — otherwise a later `LPUSH` pops the element and writes
/// it to a dead socket, and the write is simply gone.
// TR-BLOCKING-013
// FM-BLOCKING-009, FM-BLOCKING-011
#[tokio::test]
async fn disconnect_while_parked_releases_the_waiter_and_leaves_the_push_in_the_store() {
    let server = TestServer::start_standalone().await;
    let mut probe = server.connect().await;

    {
        let mut blocker = server.connect().await;
        blocker.send_only(&["BLPOP", "crit4-key", "0"]).await;
        wait_for_queue_empty(&mut probe, false).await;
        // The peer vanishes mid-park, without QUIT.
    }

    // The waiter must be reclaimed on the strength of the EOF alone.
    wait_for_queue_empty(&mut probe, true).await;

    // ... and only then does a producer arrive. The element must stay in the
    // store: there is nobody it could have been delivered to.
    let mut producer = server.connect().await;
    assert_eq!(
        integer(&producer.command(&["LPUSH", "crit4-key", "v"]).await),
        1
    );
    assert_eq!(integer(&producer.command(&["LLEN", "crit4-key"]).await), 1);

    // And a live client can still claim it.
    let mut consumer = server.connect().await;
    let resp = consumer.command(&["BLPOP", "crit4-key", "1"]).await;
    let Response::Array(items) = resp else {
        panic!("expected the pushed element, got {resp:?}");
    };
    assert_eq!(
        items.len(),
        2,
        "BLPOP must return key + value, got {items:?}"
    );
    assert_eq!(integer(&producer.command(&["LLEN", "crit4-key"]).await), 0);

    server.shutdown().await;
}

/// distsys-review CRIT-5. `CLIENT KILL` is the documented node-drain primitive.
/// Against a parked client it must actually close the connection and release the
/// waiter — reporting success and doing nothing leaves a node undrainable.
// TR-BLOCKING-021
// FM-BLOCKING-012
#[tokio::test]
async fn client_kill_terminates_a_parked_client_and_releases_its_waiter() {
    use futures::StreamExt;

    let server = TestServer::start_standalone().await;
    let mut probe = server.connect().await;

    let mut victim = server.connect().await;
    let victim_id = integer(&victim.command(&["CLIENT", "ID"]).await);
    victim.send_only(&["BLPOP", "crit5-key", "0"]).await;
    wait_for_queue_empty(&mut probe, false).await;

    let killed = probe
        .command(&["CLIENT", "KILL", "ID", &victim_id.to_string()])
        .await;
    assert_eq!(integer(&killed), 1, "CLIENT KILL must report the match");

    // The killed connection's socket must actually reach EOF. `None` from the
    // stream is the close; a timeout here is the bug (the command reporting
    // success while the connection stays parked forever).
    let eof = tokio::time::timeout(Duration::from_secs(5), victim.framed.next()).await;
    match eof {
        Ok(None) => {}
        Ok(Some(frame)) => panic!("a killed parked client must not be replied to, got {frame:?}"),
        Err(_) => panic!("CLIENT KILL did not close a connection parked in BLPOP"),
    }

    // ... and its waiter and budgets must be released.
    wait_for_queue_empty(&mut probe, true).await;
    assert_eq!(
        integer(&probe.command(&["LPUSH", "crit5-key", "v"]).await),
        1
    );
    assert_eq!(integer(&probe.command(&["LLEN", "crit5-key"]).await), 1);

    server.shutdown().await;
}

/// A command pipelined behind a blocking one must run *after* it, not ahead of
/// it: the parked wait reads the socket to watch for EOF, and what it reads is
/// buffered rather than executed (`specs/blocking.md`, "Parked pipeline buffer").
// TR-BLOCKING-013
#[tokio::test]
async fn a_command_pipelined_behind_a_blocking_one_runs_after_it() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Both frames go out together; the BLPOP times out on its own.
    client.send_only(&["BLPOP", "pipeline-key", "1"]).await;
    client.send_only(&["SET", "pipeline-marker", "after"]).await;

    let first = client
        .read_response(Duration::from_secs(10))
        .await
        .expect("the blocking command must answer first");
    // The RESP2 null array arrives at the harness codec as an untyped nil.
    assert!(
        matches!(
            first,
            Response::NullArray | Response::Null | Response::Bulk(None)
        ),
        "expected the BLPOP timeout reply first, got {first:?}"
    );
    let second = client
        .read_response(Duration::from_secs(10))
        .await
        .expect("the pipelined command must answer second");
    assert!(matches!(second, Response::Simple(_)), "got {second:?}");

    server.shutdown().await;
}
