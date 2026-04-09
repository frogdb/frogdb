//! Rust port of Redis 8.6.0 `unit/networking.tcl` test suite.
//!
//! The upstream file exercises runtime `CONFIG SET port`, `CONFIG SET bind`,
//! `CONFIG SET bind-source-addr`, default-bind handling, Redis' protected
//! mode, the Redis 8.x IO-threaded prefetch subsystem (6 tests), the
//! client-idle `timeout` config, and an internal pending-command pool.
//!
//! None of these map to a FrogDB feature:
//!
//! - `bind` and `port` are immutable config parameters in FrogDB — see the
//!   entries in `frogdb-server/crates/config/src/params.rs:310-323` with
//!   `mutable: false`. `CONFIG SET port` / `bind` / `bind-source-addr` would
//!   return `ERR CONFIG SET failed`, which matches upstream's `external:skip`
//!   tagging (these tests spawn fresh Redis instances).
//! - FrogDB does not implement protected mode (there's no loopback vs
//!   non-loopback DENIED behavior).
//! - FrogDB's IO model uses Tokio's async runtime, not Redis' 8.x IO-threaded
//!   prefetch batching. There is no `prefetch-batch-max-size` config,
//!   `io_threaded_total_prefetch_entries` info field, or pending-command pool
//!   to exercise.
//! - FrogDB does not implement a client-idle `timeout` config parameter (see
//!   the `params.rs` catalog; no `timeout` entry).
//!
//! The existing `networking_regression.rs` file already covers FrogDB's
//! networking surface (CLIENT SETNAME/GETNAME/ID/LIST). This port file
//! exists to make the audit classify the upstream tests as documented
//! exclusions. It includes one small smoke test that exercises `CLIENT KILL`
//! on a second connection — the one networking-relevant command from the
//! guidance that FrogDB implements — to justify the file compiling into the
//! test binary.
//!
//! ## Intentional exclusions
//!
//! Runtime CONFIG SET of immutable network parameters (FrogDB `bind` and
//! `port` are `mutable: false`):
//! - `CONFIG SET port number` — external:skip; FrogDB port is immutable
//! - `CONFIG SET bind address` — external:skip; FrogDB bind is immutable
//! - `CONFIG SET bind-source-addr` — external:skip; FrogDB does not implement
//!   bind-source-addr
//! - `Default bind address configuration handling` — external:skip; depends
//!   on CONFIG REWRITE and runtime bind mutation
//!
//! Protected mode (FrogDB does not implement loopback-only protected mode):
//! - `Protected mode works as expected` — FrogDB does not implement protected
//!   mode; no DENIED response for non-loopback clients
//!
//! IO-threaded prefetch subsystem (Redis 8.x feature; FrogDB uses Tokio async):
//! - `prefetch works as expected when killing a client from the middle of prefetch commands batch` —
//!   Redis-internal io-threads prefetch feature
//! - `prefetch works as expected when changing the batch size while executing the commands batch` —
//!   Redis-internal io-threads prefetch feature
//! - `no prefetch when the batch size is set to 0` —
//!   Redis-internal io-threads prefetch feature
//! - `Prefetch can resume working when the configuration option is set to a non-zero value` —
//!   Redis-internal io-threads prefetch feature
//! - `Prefetch works with batch size greater than 16 (buffer overflow regression test)` —
//!   Redis-internal io-threads prefetch feature
//! - `Prefetch works with maximum batch size of 128 and client number larger than batch size` —
//!   Redis-internal io-threads prefetch feature
//!
//! Client idle timeout (FrogDB does not implement the `timeout` config):
//! - `Multiple clients idle timeout test` — FrogDB does not implement
//!   client-idle `timeout` config
//!
//! Internal command pool sizing (Redis-internal):
//! - `Pending command pool expansion and shrinking` — Redis-internal pending
//!   command pool; FrogDB uses Tokio channels with different sizing semantics

use std::time::Duration;

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

/// Smoke test: `CLIENT KILL ADDR <ip:port>` terminates a peer connection.
///
/// This is not a direct port of any upstream test (networking.tcl uses
/// `client kill id` embedded inside the io-threads prefetch test). It gives
/// this file a real compiled test and exercises FrogDB's `CLIENT KILL` code
/// path, which is the networking command highlighted in the port task notes.
#[tokio::test]
async fn client_kill_addr_terminates_peer_connection() {
    let server = TestServer::start_standalone().await;
    let mut killer = server.connect().await;
    let mut victim = server.connect().await;

    // Verify the victim is functional before we kill it.
    assert!(matches!(
        victim.command(&["PING"]).await,
        frogdb_protocol::Response::Simple(ref s) if s == "PONG"
    ));

    // Find the victim's address in CLIENT LIST. Entries look like
    //   id=<n> addr=<ip:port> ... name=<name> ...
    // We need to know *our* (killer's) client ID first so we don't kill
    // ourselves by accident.
    let my_id = unwrap_integer(&killer.command(&["CLIENT", "ID"]).await);
    let list_resp = killer.command(&["CLIENT", "LIST"]).await;
    let text = String::from_utf8_lossy(unwrap_bulk(&list_resp)).to_string();

    let mut victim_addr: Option<String> = None;
    for line in text.lines() {
        // Parse "id=N" field.
        let mut this_id: Option<i64> = None;
        let mut this_addr: Option<&str> = None;
        for field in line.split_ascii_whitespace() {
            if let Some(v) = field.strip_prefix("id=") {
                this_id = v.parse().ok();
            } else if let Some(v) = field.strip_prefix("addr=") {
                this_addr = Some(v);
            }
        }
        if let (Some(id), Some(addr)) = (this_id, this_addr)
            && id != my_id
        {
            victim_addr = Some(addr.to_string());
            break;
        }
    }
    let victim_addr = victim_addr.expect("should find victim client in CLIENT LIST");

    // Kill the victim by address.
    let kill_resp = killer
        .command(&["CLIENT", "KILL", "ADDR", &victim_addr])
        .await;
    // CLIENT KILL ADDR <ip:port> returns Simple "OK" in old-style Redis; the
    // new-style with ADDR filter returns the integer count. FrogDB's handler
    // returns Integer(1) for ADDR-filter syntax.
    match kill_resp {
        frogdb_protocol::Response::Integer(n) => assert!(
            n >= 1,
            "CLIENT KILL ADDR should report at least 1 killed client, got {n}"
        ),
        frogdb_protocol::Response::Simple(ref s) if s == "OK" => {}
        other => panic!("unexpected CLIENT KILL response: {other:?}"),
    }

    // The victim's connection should be closed — any follow-up read should
    // time out / return None.
    victim.send_only(&["PING"]).await;
    let resp = victim.read_response(Duration::from_millis(500)).await;
    assert!(
        resp.is_none(),
        "victim connection should be closed after CLIENT KILL ADDR"
    );
}
