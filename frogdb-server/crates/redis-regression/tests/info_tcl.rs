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
//! Error accounting is driven by the dispatch gauntlet
//! (`server/src/connection/dispatch.rs`): the driver records every
//! short-circuiting stage's error replies exactly once, classifying them from
//! the stage that produced them — *guard* stages reject before anything runs
//! (`rejected_calls`), *dispatch* stages terminate into an executor that ran
//! and errored (`failed_calls`). This is the same line Redis draws between
//! `processCommand`'s pre-`call()` refusals and errors raised inside `call()`.
//!
//! Task 44 originally found that `record_error_response` was wired into only
//! three stages (`PreChecks`, `Arity`, `Execute`), leaving AUTH, MULTI/EXEC and
//! EVALSHA errors invisible, and that unknown-command/OOM errors landed on the
//! wrong side of the rejected/failed split; the tests below pinned those gaps
//! until issue 63 closed them. See
//! `.scratch/testing-improvements/issues/63`.
//!
//! Each upstream scenario, and how it maps onto FrogDB:
//!
//! - `errorstats: failed call NOGROUP error` → ported as
//!   [`errorstats_nogroup_is_failed_call`]: matches Redis (XGROUP is a
//!   `Standard` shard command; its error surfaces via the `Execute` stage).
//! - `errorstats: rejected call due to wrong arity` → ported as
//!   [`errorstats_wrong_arity_is_rejected_call`]: matches Redis
//!   (`CommandLookup` is a shared pre-dispatch guard for every command).
//! - `errorstats: rejected call by authorization error` → ported as
//!   [`errorstats_nopermission_is_rejected_call`]: matches Redis (ACL checks
//!   run in the shared `PreChecks` stage).
//! - `errorstats: rejected call unknown command` → ported as
//!   [`errorstats_unknown_command_is_rejected_call`]: matches Redis. The
//!   unknown-command check is the `CommandLookup` guard, so it is a rejection;
//!   and, like Redis (whose per-command counters live on the command-table
//!   entry an unknown command doesn't have), no `cmdstat_<garbage-name>` entry
//!   is created — the unbounded-cardinality vector task 44 found. The bound is
//!   pinned by
//!   [`commandstats_unknown_command_names_do_not_grow_cmdstat_entries`].
//! - `errorstats: rejected call by OOM error` → ported as
//!   [`errorstats_oom_is_rejected_call`]: matches Redis on the observable
//!   split. FrogDB's `maxmemory` gate is shard-side
//!   (`core/src/shard/eviction.rs`) rather than pre-dispatch, but it refuses
//!   the write before executing it, so the `OOM` prefix is classified as a
//!   pre-execution rejection at the recording seam.
//! - `errorstats: failed call authentication error` → ported as
//!   [`errorstats_auth_failure_is_failed_call`]: AUTH runs its executor in the
//!   `PreAuthIntercept` dispatch stage, so an auth failure is a failed call,
//!   matching Redis.
//! - `errorstats: failed call within MULTI/EXEC` and
//!   `errorstats: rejected call within MULTI/EXEC` → ported together as
//!   [`errorstats_multi_exec_errors_are_recorded`]: a queue-time error is
//!   attributed to the *queued* command as a rejection (the `TransactionQueue`
//!   guard), and the resulting `EXECABORT` is attributed to `exec` as a failed
//!   call (the `TransactionControl` dispatch stage) — both matching Redis.
//! - `errorstats: failed call NOSCRIPT error` → ported as
//!   [`errorstats_evalsha_noscript_is_failed_call`]: EVALSHA dispatches through
//!   the `ConnectionCommand` stage, so `NOSCRIPT` is a failed call on
//!   `cmdstat_evalsha`, matching Redis.
//! - `errorstats: failed call within LUA` → **still an open gap**, carved out
//!   of issue 63 as a follow-up: a command invoked from inside a script via
//!   `redis.call`/`redis.pcall` runs through a separate, lower-level executor
//!   (`core/src/scripting/executor.rs`) that calls `CommandRegistry` directly
//!   and never touches `ClientRegistry`, so there is no cmdstat/errorstat
//!   plumbing to record through even for *successful* script-invoked calls.
//!   Documented inline in [`errorstats_evalsha_noscript_is_failed_call`]
//!   rather than given its own test.
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

/// Number of distinct `cmdstat_<name>` entries in `INFO commandstats`.
fn cmdstat_entry_count(info: &str) -> usize {
    info.lines().filter(|l| l.starts_with("cmdstat_")).count()
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
// Re-ported upstream scenarios that were divergences until issue 63
// ============================================================================

/// `errorstats: rejected call unknown command` (upstream), re-ported: an
/// unknown command increments `errorstat_ERR` and `total_error_replies` and is
/// counted as a **rejection** — the `CommandLookup` guard rejects it before any
/// executor runs, exactly like Redis's `processCommand`.
///
/// Prior behavior (issue 63): the unknown-command check lived in
/// `route_and_execute`, inside the terminal `Execute` stage, so it recorded a
/// *failed* call, and it created a `cmdstat_<garbage-name>` entry keyed
/// directly off client input — an unbounded-cardinality vector. Both are fixed:
/// no per-name entry is created for an unrecognized command (see
/// [`commandstats_unknown_command_names_do_not_grow_cmdstat_entries`]), so the
/// rejection is observable only in `errorstat_ERR`/`total_error_replies`, which
/// is what Redis reports too.
#[tokio::test]
async fn errorstats_unknown_command_is_rejected_call() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    let resp = client.command(&["ASDFNOTACOMMAND"]).await;
    assert_error_prefix(&resp, "ERR");

    let errorstats = info_section(&mut client, "errorstats").await;
    assert_eq!(errorstat_count(&errorstats, "ERR"), Some(1), "{errorstats}");
    let stats = info_section(&mut client, "stats").await;
    assert_eq!(total_error_replies(&stats), 1, "{stats}");

    // No cmdstat entry may be keyed by the unrecognized name.
    let cmdstats = info_section(&mut client, "commandstats").await;
    assert!(
        parse_cmdstat(&cmdstats, "asdfnotacommand").is_none(),
        "an unknown command must not create a cmdstat entry:\n{cmdstats}"
    );
}

/// Unrecognized command names must not grow the `cmdstat_*` map: they are raw
/// client input, and `errorstat_*` has capped its own cardinality at
/// `MAX_ERROR_TYPES` since day one for exactly this reason. N distinct garbage
/// names produce N error replies but zero new `cmdstat_*` entries.
#[tokio::test]
async fn commandstats_unknown_command_names_do_not_grow_cmdstat_entries() {
    const GARBAGE_NAMES: usize = 200;
    /// The test's own INFO/CONFIG traffic is the only legitimate growth.
    const HARNESS_ENTRY_ALLOWANCE: usize = 4;

    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    let baseline = cmdstat_entry_count(&info_section(&mut client, "commandstats").await);

    for i in 0..GARBAGE_NAMES {
        let name = format!("NOTACOMMAND{i}");
        assert_error_prefix(&client.command(&[name.as_str()]).await, "ERR");
    }

    let cmdstats = info_section(&mut client, "commandstats").await;
    for i in 0..GARBAGE_NAMES {
        assert!(
            parse_cmdstat(&cmdstats, &format!("notacommand{i}")).is_none(),
            "cmdstat_notacommand{i} must not exist:\n{cmdstats}"
        );
    }
    let after = cmdstat_entry_count(&cmdstats);
    assert!(
        after <= baseline + HARNESS_ENTRY_ALLOWANCE,
        "{GARBAGE_NAMES} distinct unknown command names grew cmdstat entries \
         from {baseline} to {after}:\n{cmdstats}"
    );

    // The errors themselves are still fully accounted for.
    let errorstats = info_section(&mut client, "errorstats").await;
    assert_eq!(
        errorstat_count(&errorstats, "ERR"),
        Some(GARBAGE_NAMES as u64),
        "{errorstats}"
    );
}

/// `errorstats: rejected call by OOM error` (upstream), re-ported: an OOM
/// error is a **rejected** call, matching Redis.
///
/// Prior behavior (issue 63): it was recorded as a *failed* call. FrogDB's
/// `maxmemory` gate is still shard-side (`core/src/shard/eviction.rs`) rather
/// than pre-dispatch — the memory accounting it reads is the shard's own — but
/// it refuses the write before executing it, so nothing ran and the recording
/// seam classifies the `OOM` prefix as a pre-execution rejection.
#[tokio::test]
async fn errorstats_oom_is_rejected_call() {
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
        stat.rejected_calls, 1,
        "an OOM error refuses the write before it runs, so it is a rejection:\n{cmdstats}"
    );
    assert_eq!(stat.failed_calls, 0, "{cmdstats}");
}

/// `errorstats: failed call authentication error` (upstream), re-ported: an
/// AUTH failure is recorded as a **failed** call on `cmdstat_auth`, matching
/// Redis (the AUTH executor ran and replied with an error).
///
/// Prior behavior (issue 63): AUTH dispatches via the `PreAuthIntercept` stage,
/// which never called `record_error_response`, so the error reached the client
/// but created no errorstat or cmdstat entry at all.
#[tokio::test]
async fn errorstats_auth_failure_is_failed_call() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    // No password is configured, so AUTH itself is the error path (matches
    // upstream's `-ERR Client sent AUTH, but no password is set...`).
    let resp = client.command(&["AUTH", "somepass"]).await;
    assert_error_prefix(&resp, "ERR");

    let errorstats = info_section(&mut client, "errorstats").await;
    assert_eq!(errorstat_count(&errorstats, "ERR"), Some(1), "{errorstats}");
    let stats = info_section(&mut client, "stats").await;
    assert_eq!(total_error_replies(&stats), 1, "{stats}");
    let cmdstats = info_section(&mut client, "commandstats").await;
    let stat = parse_cmdstat(&cmdstats, "auth").expect("cmdstat_auth missing");
    assert_eq!(stat.calls, 1, "{cmdstats}");
    assert_eq!(stat.failed_calls, 1, "{cmdstats}");
    assert_eq!(stat.rejected_calls, 0, "{cmdstats}");
}

/// `errorstats: failed call within MULTI/EXEC` and
/// `errorstats: rejected call within MULTI/EXEC` (upstream), re-ported
/// together: both the queue-time arity error and the resulting EXECABORT are
/// recorded, each attributed the way Redis attributes it — the queue-time error
/// is a **rejection** of the queued command (`cmdstat_set`, from the
/// `TransactionQueue` guard), and EXECABORT is a **failure** of `exec` itself
/// (`cmdstat_exec`, from the `TransactionControl` dispatch stage).
///
/// Prior behavior (issue 63): neither stage called `record_error_response`, so
/// both errors reached the client entirely unaccounted for.
#[tokio::test]
async fn errorstats_multi_exec_errors_are_recorded() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    assert_ok(&client.command(&["MULTI"]).await);
    let queue_resp = client.command(&["SET"]).await;
    assert_error_prefix(&queue_resp, "ERR");
    let exec_resp = client.command(&["EXEC"]).await;
    assert_error_prefix(&exec_resp, "EXECABORT");

    let errorstats = info_section(&mut client, "errorstats").await;
    assert_eq!(errorstat_count(&errorstats, "ERR"), Some(1), "{errorstats}");
    assert_eq!(
        errorstat_count(&errorstats, "EXECABORT"),
        Some(1),
        "{errorstats}"
    );
    let stats = info_section(&mut client, "stats").await;
    assert_eq!(total_error_replies(&stats), 2, "{stats}");

    let cmdstats = info_section(&mut client, "commandstats").await;
    let set_stat = parse_cmdstat(&cmdstats, "set").expect("cmdstat_set missing");
    assert_eq!(
        set_stat.rejected_calls, 1,
        "a queue-time error rejects the queued command:\n{cmdstats}"
    );
    assert_eq!(set_stat.failed_calls, 0, "{cmdstats}");
    let exec_stat = parse_cmdstat(&cmdstats, "exec").expect("cmdstat_exec missing");
    assert_eq!(
        exec_stat.failed_calls, 1,
        "EXECABORT is a failure of EXEC itself:\n{cmdstats}"
    );
    assert_eq!(exec_stat.rejected_calls, 0, "{cmdstats}");
}

/// `errorstats: failed call NOSCRIPT error` (upstream), re-ported: EVALSHA's
/// `NOSCRIPT` is recorded as a **failed** call on `cmdstat_evalsha`, matching
/// Redis — EVAL/EVALSHA dispatch through the `ConnectionCommand` stage
/// (scripting is a `ConnectionLevel` execution strategy), which runs the
/// command's executor.
///
/// Prior behavior (issue 63): the `ConnectionCommand` stage never called
/// `record_error_response`, so NOSCRIPT was untracked.
///
/// `errorstats: failed call within LUA` remains an **open follow-up**, carved
/// out of issue 63: a command invoked from inside a script via
/// `redis.call`/`redis.pcall` runs through a separate, lower-level executor
/// (`core/src/scripting/executor.rs`) that calls `CommandRegistry` directly and
/// never touches `ClientRegistry` at all, so there is no cmdstat/errorstat
/// plumbing to record through even for successful calls, let alone failing
/// ones — unlike Redis, which tracks stats for script-invoked commands. Closing
/// it means threading a `ClientRegistry` handle into the script executor.
#[tokio::test]
async fn errorstats_evalsha_noscript_is_failed_call() {
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
        Some(1),
        "{errorstats}"
    );
    let cmdstats = info_section(&mut client, "commandstats").await;
    let stat = parse_cmdstat(&cmdstats, "evalsha").expect("cmdstat_evalsha missing");
    assert_eq!(stat.failed_calls, 1, "{cmdstats}");
    assert_eq!(stat.rejected_calls, 0, "{cmdstats}");

    // By contrast: EVAL's own arity error is caught by the universal
    // `CommandLookup` pre-dispatch guard, correctly recorded as rejected.
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);
    let arity_resp = client.command(&["EVAL", "return 1"]).await;
    assert_error_prefix(&arity_resp, "ERR");
    let cmdstats = info_section(&mut client, "commandstats").await;
    let eval_stat = parse_cmdstat(&cmdstats, "eval").expect("cmdstat_eval missing");
    assert_eq!(eval_stat.rejected_calls, 1, "{cmdstats}");
    assert_eq!(eval_stat.failed_calls, 0, "{cmdstats}");
}
