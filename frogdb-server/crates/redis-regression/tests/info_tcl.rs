//! Rust port of Redis 8.6.0 `unit/info.tcl` test suite.
//!
//! Most upstream tests are excluded. FrogDB has a different architecture than
//! Redis (multi-threaded sharded vs single-threaded event loop), so many
//! metrics either don't apply or need different implementations. This file
//! categorizes each test as either permanently out of scope or as a potential
//! observability gap to revisit.
//!
//! ## Intentional exclusions
//!
//! ### Not applicable to FrogDB architecture
//!
//! These tests exercise Redis-internal metrics that have no meaningful
//! equivalent in FrogDB's multi-threaded, sharded architecture:
//!
//! - `stats: eventloop metrics` — redis-specific — Redis single-threaded event loop cycle tracking
//! - `stats: instantaneous metrics` — redis-specific — Redis event loop instantaneous sampling
//! - `stats: debug metrics` — redis-specific — Redis DEBUG info section (AOF/cron duration sums)
//! - `stats: client input and output buffer limit disconnections` — redis-specific — Redis buffer limit stats; also needs DEBUG
//! - `memory: database and pubsub overhead and rehashing dict count` — redis-specific — Redis dict/rehashing internals (MEMORY STATS)
//! - `memory: used_memory_peak_time is updated when used_memory_peak is updated` — redis-specific — Redis-specific peak timestamp tracking
//! - `Verify that LUT overhead is properly updated when dicts are emptied or reused` — intentional-incompatibility:cluster — cluster-specific Redis dict internals
//! - `errorstats: limit errors will not increase indefinitely` — intentional-incompatibility:observability — Redis-internal 128-error-type cap behavior (FrogDB has the same 128-type cap on `error_type_counts`, see `MAX_ERROR_TYPES` in `client_registry/mod.rs`, but the upstream test also asserts on Redis-internal error strings this port doesn't reproduce)
//! - `errorstats: blocking commands` — intentional-incompatibility:observability — CLIENT UNBLOCK error type tracking (UNBLOCKED error prefix)
//!
//! ### Observability gap: per-command latency tracking
//!
//! FrogDB does not yet implement per-command latency percentile tracking
//! (Redis `latency-tracking` config + `latencystats_*` INFO fields). If
//! per-command latency observability is desired, these tests define the
//! expected behavior:
//!
//! - `latencystats: disable/enable` — intentional-incompatibility:observability — CONFIG SET latency-tracking yes/no, p50/p99/p99.9 output
//! - `latencystats: configure percentiles` — intentional-incompatibility:observability — CONFIG SET latency-tracking-info-percentiles
//! - `latencystats: bad configure percentiles` — intentional-incompatibility:observability — config validation (non-numeric, >100)
//! - `latencystats: blocking commands` — intentional-incompatibility:observability — latency tracking for BLPOP and similar
//! - `latencystats: subcommands` — intentional-incompatibility:observability — per-subcommand latency (CLIENT|ID, CONFIG|SET)
//! - `latencystats: measure latency` — intentional-incompatibility:observability — verify latency magnitude (also needs:debug)
//!
//! ### errorstats / commandstats: rejected vs. failed call accounting (task 44)
//!
//! `INFO errorstats` (`errorstat_<PREFIX>:count=N`) and `INFO commandstats`
//! (`cmdstat_<name>:calls=N,...,rejected_calls=N,failed_calls=N`) ARE
//! implemented and are exercised end to end by the tests below plus the
//! prior-art `tcl_errors_stats_for_geoadd` test in `introspection2_tcl.rs`
//! (asserts `cmdstat_geoadd` `failed_calls` e2e; not duplicated here).
//!
//! Empirically probing every upstream scenario against the current dispatch
//! pipeline (`server/src/connection/dispatch.rs`'s `DispatchStage` gauntlet)
//! shows the split is real but **narrower** than a naive port would assume:
//! `record_error_response` (the only place that increments error/commandstats)
//! is called from exactly three stages — `PreChecks` and `Arity` (both
//! `is_rejected = true`) and the terminal `Execute` stage (always
//! `is_rejected = false`, since by definition it observed a real command
//! execution). Errors surfaced by any *other* stage (`PreAuthIntercept`,
//! `TransactionControl`, `TransactionQueue`, `ConnectionCommand`, and others)
//! are never recorded at all. See
//! `.scratch/testing-improvements/issues/63-errorstats-dispatch-stage-coverage-gap.md`
//! for the follow-up filed against the gaps found here.
//!
//! Below, each upstream scenario is re-ported with an assertion of FrogDB's
//! *actual* behavior — never a faked Redis-parity assertion:
//!
//! - `errorstats: failed call NOGROUP error` → ported as
//!   [`errorstats_nogroup_is_failed_call`]: matches Redis (XGROUP is a
//!   `Standard` shard command; its error surfaces via the `Execute` stage).
//! - `errorstats: rejected call due to wrong arity` → ported as
//!   [`errorstats_wrong_arity_is_rejected_call`]: matches Redis (`Arity` is a
//!   shared pre-dispatch stage for every registered command).
//! - `errorstats: rejected call by authorization error` → ported as
//!   [`errorstats_nopermission_is_rejected_call`]: matches Redis (ACL checks
//!   run in the shared `PreChecks` stage).
//! - `errorstats: rejected call unknown command` → ported as
//!   [`errorstats_unknown_command_counts_as_err_but_not_rejected`]: the
//!   `errorstat_ERR` count and `total_error_replies` DO increment (matching
//!   Redis), but **diverges** from Redis: `route_and_execute`'s own
//!   unknown-command check runs inside the terminal `Execute` stage, so it is
//!   recorded as `is_rejected = false` (a failed call), not `rejected_calls`.
//!   A second, independent divergence found while writing this test: an
//!   unknown command also creates a `cmdstat_<garbage-name>` entry keyed
//!   directly off client-supplied input, with no cap analogous to
//!   errorstats' `MAX_ERROR_TYPES` — an unbounded-cardinality growth vector.
//! - `errorstats: rejected call by OOM error` → ported as
//!   [`errorstats_oom_is_failed_not_rejected_call_divergence`]: **diverges**
//!   from Redis. Real Redis checks `maxmemory` before dispatch (a rejection);
//!   FrogDB's OOM check (`core/src/shard/eviction.rs`) runs during shard-side
//!   write execution, downstream of the `Execute` stage's routing, so it is
//!   recorded as a failed call, not a rejected one. `errorstat_OOM` and
//!   `total_error_replies` are still correct.
//! - `errorstats: failed call authentication error`,
//!   `errorstats: failed call within MULTI/EXEC`,
//!   `errorstats: rejected call within MULTI/EXEC`,
//!   `errorstats: failed call NOSCRIPT error`,
//!   `errorstats: failed call within LUA` → all five pin a genuine
//!   **observability gap**, not a divergence in classification: AUTH
//!   (`PreAuthIntercept`), MULTI/EXEC/queued-command errors
//!   (`TransactionControl`/`TransactionQueue`), and EVALSHA/NOSCRIPT (EVAL and
//!   EVALSHA dispatch via `ConnectionCommand`, not `Execute`) are none of them
//!   recorded at all — no errorstat increment, no cmdstat increment. Ported as
//!   [`errorstats_auth_failure_is_untracked_gap`],
//!   [`errorstats_multi_exec_errors_are_untracked_gap`], and
//!   [`errorstats_evalsha_noscript_is_untracked_gap`]. Redis-invoked nested
//!   commands (`redis.call`/`redis.pcall` inside EVAL) go through a separate,
//!   lower-level executor path entirely (`core/src/scripting/executor.rs`)
//!   that never touches `ClientRegistry`, so they can never be tracked with
//!   the current architecture; documented inline in
//!   [`errorstats_evalsha_noscript_is_untracked_gap`] rather than given its
//!   own test, since it's the same root cause as the EVAL/EVALSHA gap.
//!
//! ### Observability gap: client stats
//!
//! FrogDB does not yet expose `pubsub_clients`, `watching_clients`, or
//! `total_watched_keys` in INFO clients. These are trackable with current
//! architecture (connection state already knows pubsub/watch status):
//!
//! - `clients: pubsub clients` — intentional-incompatibility:observability — pubsub_clients count in INFO clients section
//! - `clients: watching clients` — intentional-incompatibility:observability — watching_clients, total_watched_keys in INFO clients; watch=N in CLIENT INFO

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

/// Parsed `cmdstat_<name>:calls=N,usec=N,usec_per_call=N.NN,rejected_calls=N,failed_calls=N`
///
/// Duplicated from `introspection2_tcl.rs` per this crate's convention: each
/// `tests/*.rs` file compiles as a module of a single `main` test binary
/// (see `tests/main.rs`), so private helpers aren't shared across files.
#[derive(Debug, Default)]
struct CmdStat {
    calls: u64,
    rejected_calls: u64,
    failed_calls: u64,
}

fn parse_cmdstat(info: &str, cmd_name: &str) -> Option<CmdStat> {
    let prefix = format!("cmdstat_{}:", cmd_name.to_lowercase());
    for line in info.lines() {
        if let Some(kv_part) = line.strip_prefix(&prefix) {
            let mut stat = CmdStat::default();
            for pair in kv_part.split(',') {
                let (key, val) = pair.split_once('=')?;
                match key {
                    "calls" => stat.calls = val.parse().ok()?,
                    "rejected_calls" => stat.rejected_calls = val.parse().ok()?,
                    "failed_calls" => stat.failed_calls = val.parse().ok()?,
                    _ => {}
                }
            }
            return Some(stat);
        }
    }
    None
}

/// Extract `errorstat_<PREFIX>:count=N` for a given prefix, if present.
fn errorstat_count(info: &str, prefix: &str) -> Option<u64> {
    let line_prefix = format!("errorstat_{prefix}:count=");
    info.lines()
        .find_map(|l| l.strip_prefix(&line_prefix)?.parse().ok())
}

fn total_error_replies(info: &str) -> u64 {
    info.lines()
        .find_map(|l| l.strip_prefix("total_error_replies:"))
        .expect("total_error_replies missing from INFO stats")
        .trim()
        .parse()
        .expect("total_error_replies not numeric")
}

async fn info_section(
    client: &mut frogdb_test_harness::server::TestClient,
    section: &str,
) -> String {
    let resp = client.command(&["INFO", section]).await;
    match resp {
        frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8_lossy(&b).to_string(),
        other => panic!("expected bulk, got {other:?}"),
    }
}

// ============================================================================
// Core acceptance criteria (task 44): WRONGTYPE/failed, arity/rejected, sum
// ============================================================================

/// A WRONGTYPE error (execution-time, on a `Standard` shard command) is
/// recorded as a failed call: `errorstat_WRONGTYPE:count=1` and
/// `cmdstat_lpush` `failed_calls=1`, `rejected_calls=0`.
#[tokio::test]
async fn errorstats_wrongtype_is_failed_call() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    assert_ok(&client.command(&["SET", "wrongtype:key", "bar"]).await);
    let resp = client.command(&["LPUSH", "wrongtype:key", "x"]).await;
    assert_error_prefix(&resp, "WRONGTYPE");

    let errorstats = info_section(&mut client, "errorstats").await;
    assert_eq!(
        errorstat_count(&errorstats, "WRONGTYPE"),
        Some(1),
        "{errorstats}"
    );

    let cmdstats = info_section(&mut client, "commandstats").await;
    let stat = parse_cmdstat(&cmdstats, "lpush").expect("cmdstat_lpush missing");
    assert_eq!(stat.failed_calls, 1, "{cmdstats}");
    assert_eq!(stat.rejected_calls, 0, "{cmdstats}");
}

/// A wrong-arity error on a known command is recorded as a rejected call:
/// `cmdstat_set` `rejected_calls=1`, `failed_calls=0`. The `Arity`
/// pre-dispatch stage runs for every registered command before execution.
#[tokio::test]
async fn errorstats_wrong_arity_is_rejected_call() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    let resp = client.command(&["SET", "onlyonearg"]).await;
    assert_error_prefix(&resp, "ERR");

    let cmdstats = info_section(&mut client, "commandstats").await;
    let stat = parse_cmdstat(&cmdstats, "set").expect("cmdstat_set missing");
    assert_eq!(stat.rejected_calls, 1, "{cmdstats}");
    assert_eq!(stat.failed_calls, 0, "{cmdstats}");
}

/// `total_error_replies` is the sum of call-count across every error type,
/// not a count of distinct types: two WRONGTYPE errors plus one arity error
/// sum to 3, while `errorstat_WRONGTYPE:count=2` and `errorstat_ERR:count=1`
/// are tracked separately.
#[tokio::test]
async fn errorstats_total_error_replies_sums_across_types() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    assert_ok(&client.command(&["SET", "sum:key", "bar"]).await);
    assert_error_prefix(
        &client.command(&["LPUSH", "sum:key", "x"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(
        &client.command(&["LPUSH", "sum:key", "y"]).await,
        "WRONGTYPE",
    );
    assert_error_prefix(&client.command(&["SET", "onlyonearg"]).await, "ERR");

    let errorstats = info_section(&mut client, "errorstats").await;
    assert_eq!(
        errorstat_count(&errorstats, "WRONGTYPE"),
        Some(2),
        "{errorstats}"
    );
    assert_eq!(errorstat_count(&errorstats, "ERR"), Some(1), "{errorstats}");

    let stats = info_section(&mut client, "stats").await;
    assert_eq!(total_error_replies(&stats), 3, "{stats}");
}

// ============================================================================
// Re-ported upstream scenarios that match FrogDB's actual (correct) behavior
// ============================================================================

/// `errorstats: failed call NOGROUP error` (upstream), re-ported: XGROUP
/// CREATECONSUMER against a nonexistent group is a `Standard` shard command
/// error, so it is a failed call, matching Redis.
#[tokio::test]
async fn errorstats_nogroup_is_failed_call() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    client.command(&["DEL", "nogroup:stream"]).await;
    let xadd_resp = client
        .command(&["XADD", "nogroup:stream", "*", "f", "v"])
        .await;
    assert!(
        matches!(xadd_resp, frogdb_protocol::Response::Bulk(Some(_))),
        "XADD should return a stream ID: {xadd_resp:?}"
    );
    let resp = client
        .command(&[
            "XGROUP",
            "CREATECONSUMER",
            "nogroup:stream",
            "missing-group",
            "consumer",
        ])
        .await;
    assert_error_prefix(&resp, "NOGROUP");

    let errorstats = info_section(&mut client, "errorstats").await;
    assert_eq!(
        errorstat_count(&errorstats, "NOGROUP"),
        Some(1),
        "{errorstats}"
    );
    let cmdstats = info_section(&mut client, "commandstats").await;
    let stat = parse_cmdstat(&cmdstats, "xgroup").expect("cmdstat_xgroup missing");
    assert_eq!(stat.failed_calls, 1, "{cmdstats}");
    assert_eq!(stat.rejected_calls, 0, "{cmdstats}");
}

/// `errorstats: rejected call by authorization error` (upstream), re-ported:
/// an ACL NOPERM denial runs in the shared `PreChecks` stage, so it is a
/// rejected call, matching Redis.
#[tokio::test]
async fn errorstats_nopermission_is_rejected_call() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);
    assert_ok(
        &client
            .command(&[
                "ACL", "SETUSER", "limited", "on", ">pw", "~*", "+get", "+info", "+config", "+auth",
            ])
            .await,
    );

    let mut limited = server.connect().await;
    assert_ok(&limited.command(&["AUTH", "limited", "pw"]).await);
    let resp = limited.command(&["SET", "a", "b"]).await;
    assert_error_prefix(&resp, "NOPERM");

    let errorstats = info_section(&mut client, "errorstats").await;
    assert_eq!(
        errorstat_count(&errorstats, "NOPERM"),
        Some(1),
        "{errorstats}"
    );
    let cmdstats = info_section(&mut client, "commandstats").await;
    let stat = parse_cmdstat(&cmdstats, "set").expect("cmdstat_set missing");
    assert_eq!(stat.rejected_calls, 1, "{cmdstats}");
    assert_eq!(stat.failed_calls, 0, "{cmdstats}");
}

// ============================================================================
// Re-ported upstream scenarios that reveal genuine divergences
// ============================================================================

/// `errorstats: rejected call unknown command` (upstream), re-ported: FrogDB
/// DOES increment `errorstat_ERR` and `total_error_replies` for an unknown
/// command (matching Redis), but **diverges** on the rejected/failed split:
/// `route_and_execute`'s unknown-command check runs inside the terminal
/// `Execute` stage, so it is recorded `is_rejected = false` (a failed call),
/// not `rejected_calls`, unlike Redis. It also creates a
/// `cmdstat_<garbage-name>` entry keyed directly off client input with no
/// cardinality cap — see the follow-up issue for both findings.
#[tokio::test]
async fn errorstats_unknown_command_counts_as_err_but_not_rejected() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    let resp = client.command(&["ASDFNOTACOMMAND"]).await;
    assert_error_prefix(&resp, "ERR");

    let errorstats = info_section(&mut client, "errorstats").await;
    assert_eq!(errorstat_count(&errorstats, "ERR"), Some(1), "{errorstats}");
    let stats = info_section(&mut client, "stats").await;
    assert_eq!(total_error_replies(&stats), 1, "{stats}");

    // Divergence: classified as failed, not rejected, and cmdstat is keyed by
    // the raw unrecognized command name.
    let cmdstats = info_section(&mut client, "commandstats").await;
    let stat =
        parse_cmdstat(&cmdstats, "asdfnotacommand").expect("cmdstat_asdfnotacommand missing");
    assert_eq!(
        stat.failed_calls, 1,
        "divergence: FrogDB records unknown-command errors as failed, not rejected:\n{cmdstats}"
    );
    assert_eq!(stat.rejected_calls, 0, "{cmdstats}");
}

/// `errorstats: rejected call by OOM error` (upstream), re-ported:
/// **diverges** from Redis. Real Redis checks `maxmemory` before dispatch
/// (a rejected call); FrogDB's OOM check (`core/src/shard/eviction.rs`) runs
/// during shard-side write execution, downstream of the `Execute` stage's
/// routing, so it is recorded as a failed call. `errorstat_OOM` and
/// `total_error_replies` are still correct.
#[tokio::test]
async fn errorstats_oom_is_failed_not_rejected_call_divergence() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    let mem_info = info_section(&mut client, "memory").await;
    let used: u64 = mem_info
        .lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .expect("used_memory missing")
        .trim()
        .parse()
        .expect("used_memory not numeric");
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory", &(used + 50_000).to_string()])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", "noeviction"])
            .await,
    );

    let mut oom_resp = None;
    for i in 0..2000 {
        let resp = client
            .command(&["SET", &format!("oom:key:{i}"), &"x".repeat(1000)])
            .await;
        if let frogdb_protocol::Response::Error(ref msg) = resp
            && msg.starts_with(b"OOM")
        {
            oom_resp = Some(resp);
            break;
        }
    }
    client.command(&["CONFIG", "SET", "maxmemory", "0"]).await;
    let oom_resp = oom_resp.expect("expected an OOM error before exhausting the key budget");
    assert_error_prefix(&oom_resp, "OOM");

    let errorstats = info_section(&mut client, "errorstats").await;
    assert_eq!(errorstat_count(&errorstats, "OOM"), Some(1), "{errorstats}");

    let cmdstats = info_section(&mut client, "commandstats").await;
    let stat = parse_cmdstat(&cmdstats, "set").expect("cmdstat_set missing");
    assert_eq!(
        stat.failed_calls, 1,
        "divergence: FrogDB records OOM as failed, Redis records it as rejected:\n{cmdstats}"
    );
    assert_eq!(stat.rejected_calls, 0, "{cmdstats}");
}

/// `errorstats: failed call authentication error` (upstream), re-ported:
/// pins a genuine gap. AUTH is dispatched via the `PreAuthIntercept` stage,
/// which never calls `record_error_response` — the error reaches the client
/// correctly, but no errorstat or cmdstat entry is created for it at all.
#[tokio::test]
async fn errorstats_auth_failure_is_untracked_gap() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    // No password is configured, so AUTH itself is the error path (matches
    // upstream's `-ERR Client sent AUTH, but no password is set...`).
    let resp = client.command(&["AUTH", "somepass"]).await;
    assert_error_prefix(&resp, "ERR");

    let errorstats = info_section(&mut client, "errorstats").await;
    assert_eq!(
        errorstat_count(&errorstats, "ERR"),
        None,
        "gap: AUTH errors are not recorded in errorstats yet:\n{errorstats}"
    );
    let stats = info_section(&mut client, "stats").await;
    assert_eq!(
        total_error_replies(&stats),
        0,
        "gap: AUTH errors don't increment total_error_replies yet:\n{stats}"
    );
    let cmdstats = info_section(&mut client, "commandstats").await;
    let stat = parse_cmdstat(&cmdstats, "auth");
    assert!(
        stat.is_none_or(|s| s.failed_calls == 0 && s.rejected_calls == 0),
        "gap: cmdstat_auth should show no failed/rejected accounting yet:\n{cmdstats}"
    );
}

/// `errorstats: failed call within MULTI/EXEC` and
/// `errorstats: rejected call within MULTI/EXEC` (upstream), re-ported
/// together: pins a genuine gap. Both an arity error queued inside MULTI
/// (dispatched via `TransactionQueue`) and the resulting EXECABORT from EXEC
/// (dispatched via `TransactionControl`) reach the client correctly but
/// neither stage calls `record_error_response`.
#[tokio::test]
async fn errorstats_multi_exec_errors_are_untracked_gap() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    assert_ok(&client.command(&["MULTI"]).await);
    let queue_resp = client.command(&["SET"]).await;
    assert_error_prefix(&queue_resp, "ERR");
    let exec_resp = client.command(&["EXEC"]).await;
    assert_error_prefix(&exec_resp, "EXECABORT");

    let errorstats = info_section(&mut client, "errorstats").await;
    assert_eq!(
        errorstat_count(&errorstats, "ERR"),
        None,
        "gap: queue-time arity error inside MULTI isn't recorded yet:\n{errorstats}"
    );
    assert_eq!(
        errorstat_count(&errorstats, "EXECABORT"),
        None,
        "gap: EXECABORT from EXEC isn't recorded yet:\n{errorstats}"
    );
    let stats = info_section(&mut client, "stats").await;
    assert_eq!(
        total_error_replies(&stats),
        0,
        "gap: neither MULTI-queue nor EXEC errors increment total_error_replies yet:\n{stats}"
    );
}

/// `errorstats: failed call NOSCRIPT error` and
/// `errorstats: failed call within LUA` (upstream), re-ported together:
/// pins a genuine gap with a single root cause. EVAL/EVALSHA dispatch via
/// the `ConnectionCommand` stage (scripting is a `ConnectionLevel`
/// execution strategy), which never calls `record_error_response`, so
/// EVALSHA's NOSCRIPT is untracked. A command invoked from inside a Lua
/// script via `redis.call`/`redis.pcall` is untracked for a different,
/// deeper reason: script-internal command calls run through a separate,
/// lower-level executor (`core/src/scripting/executor.rs`) that calls
/// `CommandRegistry` directly and never touches `ClientRegistry` at all, so
/// there is no cmdstat/errorstat plumbing to record through even for
/// successful calls, let alone failing ones — unlike Redis, which tracks
/// stats for script-invoked commands. EVAL's own arity error, by contrast,
/// IS caught (and correctly recorded as rejected) by the universal `Arity`
/// pre-dispatch stage, since that stage runs ahead of the
/// `ConnectionCommand` dispatch for every registered command name.
#[tokio::test]
async fn errorstats_evalsha_noscript_is_untracked_gap() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    let resp = client
        .command(&["EVALSHA", "0000000000000000000000000000000000000000", "0"])
        .await;
    assert_error_prefix(&resp, "NOSCRIPT");

    let errorstats = info_section(&mut client, "errorstats").await;
    assert_eq!(
        errorstat_count(&errorstats, "NOSCRIPT"),
        None,
        "gap: EVALSHA NOSCRIPT isn't recorded yet (ConnectionCommand stage skips record_error_response):\n{errorstats}"
    );
    let cmdstats = info_section(&mut client, "commandstats").await;
    let stat = parse_cmdstat(&cmdstats, "evalsha");
    assert!(
        stat.is_none_or(|s| s.failed_calls == 0 && s.rejected_calls == 0),
        "gap: cmdstat_evalsha should show no failed/rejected accounting yet:\n{cmdstats}"
    );

    // By contrast: EVAL's own arity error IS caught by the universal Arity
    // pre-dispatch stage, correctly recorded as rejected.
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);
    let arity_resp = client.command(&["EVAL", "return 1"]).await;
    assert_error_prefix(&arity_resp, "ERR");
    let cmdstats = info_section(&mut client, "commandstats").await;
    let eval_stat = parse_cmdstat(&cmdstats, "eval").expect("cmdstat_eval missing");
    assert_eq!(eval_stat.rejected_calls, 1, "{cmdstats}");
    assert_eq!(eval_stat.failed_calls, 0, "{cmdstats}");
}
