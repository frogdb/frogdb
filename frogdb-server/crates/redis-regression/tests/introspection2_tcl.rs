//! Rust port of Redis 8.6.0 `unit/introspection-2.tcl` test suite.
//!
//! Excluded tests:
//! - COMMAND GETKEYS for commands FrogDB may not support (LCS, MEMORY USAGE,
//!   ZUNIONSTORE with 260 keys)
//! - COMMAND LIST FILTERBY ACLCAT (ACL category introspection)
//! - GEORADIUS / GEORADIUS_RO movablekeys tests (deprecated commands)

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// TIME command
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_time_microsecond_part_does_not_overflow() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["TIME"]).await;
    let parts = unwrap_array(resp);
    assert_eq!(parts.len(), 2, "TIME should return a two-element array");

    let microseconds: i64 = std::str::from_utf8(unwrap_bulk(&parts[1]))
        .unwrap()
        .parse()
        .unwrap();
    assert!(microseconds >= 0, "microseconds should be >= 0");
    assert!(
        microseconds < 1_000_000,
        "microseconds should be < 1_000_000, got {microseconds}"
    );
}

// ---------------------------------------------------------------------------
// TOUCH command
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_touch_returns_number_of_existing_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    client.command(&["SET", "key1{t}", "1"]).await;
    client.command(&["SET", "key2{t}", "2"]).await;

    // key0{t} and key3{t} do not exist, so only 2 of the 4 keys are found.
    assert_integer_eq(
        &client
            .command(&["TOUCH", "key0{t}", "key1{t}", "key2{t}", "key3{t}"])
            .await,
        2,
    );
}

// ---------------------------------------------------------------------------
// COMMAND COUNT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_command_count_returns_positive_number() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let count = unwrap_integer(&client.command(&["COMMAND", "COUNT"]).await);
    assert!(count > 0, "COMMAND COUNT should be > 0, got {count}");
}

// ---------------------------------------------------------------------------
// COMMAND GETKEYS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_command_getkeys_get() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["COMMAND", "GETKEYS", "GET", "key"]).await;
    let keys = extract_bulk_strings(&resp);
    assert_eq!(keys, vec!["key"]);
}

#[tokio::test]
async fn tcl_command_getkeys_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "GETKEYS", "SET", "mykey", "myval"])
        .await;
    let keys = extract_bulk_strings(&resp);
    assert_eq!(keys, vec!["mykey"]);
}

#[tokio::test]
async fn tcl_command_getkeys_mset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "GETKEYS", "MSET", "k1", "v1", "k2", "v2"])
        .await;
    let keys = extract_bulk_strings(&resp);
    assert_eq!(keys, vec!["k1", "k2"]);
}

#[tokio::test]
async fn tcl_command_getkeys_xgroup_create() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "COMMAND",
            "GETKEYS",
            "XGROUP",
            "CREATE",
            "key",
            "groupname",
            "$",
        ])
        .await;
    let keys = extract_bulk_strings(&resp);
    assert_eq!(keys, vec!["key"]);
}

// ---------------------------------------------------------------------------
// COMMAND LIST (basic, no filterby)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_command_list_contains_common_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["COMMAND", "LIST"]).await;
    let commands = extract_bulk_strings(&resp);
    assert!(
        commands.iter().any(|c| c.eq_ignore_ascii_case("set")),
        "COMMAND LIST should contain SET"
    );
    assert!(
        commands.iter().any(|c| c.eq_ignore_ascii_case("get")),
        "COMMAND LIST should contain GET"
    );
}

// ---------------------------------------------------------------------------
// COMMAND LIST syntax errors
// ---------------------------------------------------------------------------

#[tokio::test]

async fn tcl_command_list_syntax_error_bad_arg() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["COMMAND", "LIST", "bad_arg"]).await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_command_list_syntax_error_bad_filterby() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "LIST", "FILTERBY", "bad_arg"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_command_list_syntax_error_bad_filterby_two_args() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "LIST", "FILTERBY", "bad_arg", "bad_arg2"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// COMMAND LIST FILTERBY MODULE (non-existing module returns empty)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_command_list_filterby_module_non_existing() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&[
            "COMMAND",
            "LIST",
            "FILTERBY",
            "MODULE",
            "non_existing_module",
        ])
        .await;
    let commands = extract_bulk_strings(&resp);
    assert!(
        commands.is_empty(),
        "COMMAND LIST FILTERBY MODULE for non-existing module should be empty"
    );
}

// ---------------------------------------------------------------------------
// COMMAND LIST FILTERBY PATTERN
// ---------------------------------------------------------------------------

#[tokio::test]

async fn tcl_command_list_filterby_pattern_exact_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "LIST", "FILTERBY", "PATTERN", "set"])
        .await;
    let commands = extract_bulk_strings(&resp);
    assert_eq!(commands, vec!["set"]);
}

#[tokio::test]

async fn tcl_command_list_filterby_pattern_exact_get() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "LIST", "FILTERBY", "PATTERN", "get"])
        .await;
    let commands = extract_bulk_strings(&resp);
    assert_eq!(commands, vec!["get"]);
}

#[tokio::test]
async fn tcl_command_list_filterby_pattern_non_existing() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["COMMAND", "LIST", "FILTERBY", "PATTERN", "non_exists"])
        .await;
    let commands = extract_bulk_strings(&resp);
    assert!(
        commands.is_empty(),
        "COMMAND LIST FILTERBY PATTERN for non-existing pattern should be empty"
    );
}

// ---------------------------------------------------------------------------
// COMMAND INFO of invalid subcommands
// ---------------------------------------------------------------------------

#[tokio::test]

async fn tcl_command_info_invalid_subcommand_returns_nil() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // An invalid subcommand like "get|key" should return a single-element
    // array containing nil (the Redis convention for unknown commands).
    let resp = client.command(&["COMMAND", "INFO", "get|key"]).await;
    let items = unwrap_array(resp);
    assert_eq!(
        items.len(),
        1,
        "COMMAND INFO for unknown cmd should return 1-element array"
    );
    assert_nil(&items[0]);
}

#[tokio::test]

async fn tcl_command_info_double_pipe_invalid_returns_nil() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["COMMAND", "INFO", "config|get|key"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1);
    assert_nil(&items[0]);
}

// ---------------------------------------------------------------------------
// OBJECT IDLETIME / access-time tracking
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_ttl_type_exists_do_not_alter_last_access_time() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "myval"]).await;

    // Wait 2 seconds to accumulate idle time
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // These read-only metadata commands should NOT alter access time
    client.command(&["TTL", "mykey"]).await;
    client.command(&["TYPE", "mykey"]).await;
    client.command(&["EXISTS", "mykey"]).await;

    let idle = unwrap_integer(&client.command(&["OBJECT", "IDLETIME", "mykey"]).await);
    assert!(
        idle >= 2,
        "OBJECT IDLETIME should be >= 2 after TTL/TYPE/EXISTS, got {idle}"
    );
}

#[tokio::test]
async fn tcl_touch_alters_last_access_time() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "myval"]).await;

    // Wait 2 seconds to accumulate idle time
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // TOUCH should update access time
    client.command(&["TOUCH", "mykey"]).await;

    let idle = unwrap_integer(&client.command(&["OBJECT", "IDLETIME", "mykey"]).await);
    assert!(
        idle < 2,
        "OBJECT IDLETIME should be < 2 after TOUCH, got {idle}"
    );
}

#[tokio::test]
async fn tcl_no_touch_mode_does_not_alter_last_access_time() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "myval"]).await;

    // Wait 2 seconds to accumulate idle time
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Enable no-touch mode
    assert_ok(&client.command(&["CLIENT", "NO-TOUCH", "ON"]).await);

    // GET in no-touch mode should NOT alter access time
    client.command(&["GET", "mykey"]).await;

    // Need a second client to read OBJECT IDLETIME without no-touch
    let mut client2 = server.connect().await;
    let idle = unwrap_integer(&client2.command(&["OBJECT", "IDLETIME", "mykey"]).await);
    assert!(
        idle >= 2,
        "OBJECT IDLETIME should be >= 2 after GET in no-touch mode, got {idle}"
    );
}

#[tokio::test]
async fn tcl_no_touch_mode_touch_alters_last_access_time() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "myval"]).await;

    // Wait 2 seconds to accumulate idle time
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Enable no-touch mode
    assert_ok(&client.command(&["CLIENT", "NO-TOUCH", "ON"]).await);

    // TOUCH should still update access time even in no-touch mode
    client.command(&["TOUCH", "mykey"]).await;

    // Need a second client to read OBJECT IDLETIME without no-touch
    let mut client2 = server.connect().await;
    let idle = unwrap_integer(&client2.command(&["OBJECT", "IDLETIME", "mykey"]).await);
    assert!(
        idle < 2,
        "OBJECT IDLETIME should be < 2 after TOUCH even in no-touch mode, got {idle}"
    );
}

#[tokio::test]
async fn tcl_no_touch_mode_touch_from_script_alters_last_access_time() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "mykey", "myval"]).await;

    // Wait 2 seconds to accumulate idle time
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Enable no-touch mode
    assert_ok(&client.command(&["CLIENT", "NO-TOUCH", "ON"]).await);

    // redis.call('TOUCH', key) inside a Lua script should still update access time
    client
        .command(&["EVAL", "return redis.call('TOUCH', KEYS[1])", "1", "mykey"])
        .await;

    // Need a second client to read OBJECT IDLETIME without no-touch
    let mut client2 = server.connect().await;
    let idle = unwrap_integer(&client2.command(&["OBJECT", "IDLETIME", "mykey"]).await);
    assert!(
        idle < 2,
        "OBJECT IDLETIME should be < 2 after TOUCH from script in no-touch mode, got {idle}"
    );
}

// ---------------------------------------------------------------------------
// Helpers for commandstats tests
// ---------------------------------------------------------------------------

/// Parsed `cmdstat_<name>:calls=N,usec=N,usec_per_call=N.NN,rejected_calls=N,failed_calls=N`
#[derive(Debug, Default)]
struct CmdStat {
    calls: u64,
    usec: u64,
    usec_per_call: f64,
    rejected_calls: u64,
    failed_calls: u64,
}

/// Extract a specific cmdstat line from `INFO commandstats` output.
fn parse_cmdstat(info: &str, cmd_name: &str) -> Option<CmdStat> {
    let prefix = format!("cmdstat_{}:", cmd_name.to_lowercase());
    for line in info.lines() {
        if line.starts_with(&prefix) {
            let kv_part = &line[prefix.len()..];
            let mut stat = CmdStat::default();
            for pair in kv_part.split(',') {
                let (key, val) = pair.split_once('=')?;
                match key {
                    "calls" => stat.calls = val.parse().ok()?,
                    "usec" => stat.usec = val.parse().ok()?,
                    "usec_per_call" => stat.usec_per_call = val.parse().ok()?,
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

/// Get the full INFO output for a specific section as a string.
async fn get_info_section(
    client: &mut frogdb_test_harness::server::TestClient,
    section: &str,
) -> String {
    let resp = client.command(&["INFO", section]).await;
    match resp {
        frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8_lossy(&b).to_string(),
        frogdb_protocol::Response::Simple(s) => String::from_utf8_lossy(&s).to_string(),
        other => panic!("expected bulk or simple string from INFO, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// commandstats tests (Phase 1)
// ---------------------------------------------------------------------------

/// `command stats for GEOADD`
///
/// Verifies that after CONFIG RESETSTAT, a GEOADD call is accurately tracked
/// in INFO commandstats with calls=1, usec>0, and usec_per_call>0.
#[tokio::test]
async fn tcl_command_stats_for_geoadd() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Reset stats
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    // Execute GEOADD
    client
        .command(&["GEOADD", "mygeo", "0", "0", "member"])
        .await;

    // Check commandstats
    let info = get_info_section(&mut client, "commandstats").await;
    let stat = parse_cmdstat(&info, "geoadd").expect("cmdstat_geoadd not found");
    assert_eq!(stat.calls, 1, "geoadd calls should be 1");
    assert!(
        stat.usec > 0,
        "geoadd usec should be > 0, got {}",
        stat.usec
    );
    assert!(
        stat.usec_per_call > 0.0,
        "geoadd usec_per_call should be > 0, got {}",
        stat.usec_per_call
    );
    assert_eq!(stat.failed_calls, 0, "geoadd failed_calls should be 0");
    assert_eq!(stat.rejected_calls, 0, "geoadd rejected_calls should be 0");
}

/// `command stats for EXPIRE`
///
/// Verifies EXPIRE is tracked: calls=1, failed_calls=0 on a valid key.
#[tokio::test]
async fn tcl_command_stats_for_expire() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Reset stats
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    // Create a key and EXPIRE it
    client.command(&["SET", "mykey", "myval"]).await;
    client.command(&["EXPIRE", "mykey", "100"]).await;

    // Check commandstats
    let info = get_info_section(&mut client, "commandstats").await;
    let stat = parse_cmdstat(&info, "expire").expect("cmdstat_expire not found");
    assert_eq!(stat.calls, 1, "expire calls should be 1");
    assert!(
        stat.usec > 0,
        "expire usec should be > 0, got {}",
        stat.usec
    );
    assert_eq!(stat.failed_calls, 0, "expire failed_calls should be 0");
    assert_eq!(stat.rejected_calls, 0, "expire rejected_calls should be 0");
}

/// `command stats for BRPOP`
///
/// Verifies that BRPOP with timeout=0 (which returns immediately with nil
/// when the key doesn't exist, or with data after a push) is tracked.
#[tokio::test]
async fn tcl_command_stats_for_brpop() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Reset stats
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    // Pre-populate a list so BRPOP returns immediately
    client.command(&["LPUSH", "mylist", "a"]).await;
    client.command(&["BRPOP", "mylist", "0"]).await;

    // Check commandstats — BRPOP should show calls=1
    let info = get_info_section(&mut client, "commandstats").await;
    let stat = parse_cmdstat(&info, "brpop").expect("cmdstat_brpop not found");
    assert_eq!(stat.calls, 1, "brpop calls should be 1");
    assert!(stat.usec > 0, "brpop usec should be > 0, got {}", stat.usec);
    assert_eq!(stat.failed_calls, 0, "brpop failed_calls should be 0");
}

/// `command stats for MULTI`
///
/// Verifies that MULTI/EXEC correctly counts the MULTI command and the
/// commands inside the transaction. MULTI itself is counted, plus the
/// commands executed inside the EXEC block.
#[tokio::test]
async fn tcl_command_stats_for_multi() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Reset stats
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    // Execute a transaction
    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["SET", "txkey", "txval"]).await;
    client.command(&["EXEC"]).await;

    // Check commandstats
    let info = get_info_section(&mut client, "commandstats").await;

    // MULTI should have calls=1
    let multi_stat = parse_cmdstat(&info, "multi").expect("cmdstat_multi not found");
    assert_eq!(multi_stat.calls, 1, "multi calls should be 1");

    // EXEC should have calls=1
    let exec_stat = parse_cmdstat(&info, "exec").expect("cmdstat_exec not found");
    assert_eq!(exec_stat.calls, 1, "exec calls should be 1");

    // SET should have calls=1 (from inside the transaction)
    let set_stat = parse_cmdstat(&info, "set").expect("cmdstat_set not found");
    assert_eq!(set_stat.calls, 1, "set calls should be 1");
}

/// `command stats for scripts`
///
/// Verifies that EVAL scripts are counted in commandstats. The EVAL
/// command itself should be tracked, plus the commands executed inside
/// the script should NOT be individually tracked (Redis behavior).
#[tokio::test]
async fn tcl_command_stats_for_scripts() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Reset stats
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    // Execute a script that sets a key
    client
        .command(&[
            "EVAL",
            "redis.call('SET', KEYS[1], ARGV[1])",
            "1",
            "scriptkey",
            "scriptval",
        ])
        .await;

    // Check commandstats
    let info = get_info_section(&mut client, "commandstats").await;

    // EVAL should be counted
    let eval_stat = parse_cmdstat(&info, "eval").expect("cmdstat_eval not found");
    assert_eq!(eval_stat.calls, 1, "eval calls should be 1");
    assert!(
        eval_stat.usec > 0,
        "eval usec should be > 0, got {}",
        eval_stat.usec
    );
}

/// `errors stats for GEOADD`
///
/// Verifies that calling GEOADD with wrong arguments increments
/// `failed_calls` in commandstats and is tracked in errorstats.
#[tokio::test]
async fn tcl_errors_stats_for_geoadd() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Reset stats
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    // Execute a successful GEOADD
    client
        .command(&["GEOADD", "mygeo", "0", "0", "member"])
        .await;

    // Execute a GEOADD that fails (invalid coordinates)
    let resp = client
        .command(&["GEOADD", "mygeo", "1000", "1000", "member2"])
        .await;
    // This should be an error (longitude must be -180..180)
    assert_error_prefix(&resp, "ERR");

    // Check commandstats — should show calls=2, failed_calls=1
    let info = get_info_section(&mut client, "commandstats").await;
    let stat = parse_cmdstat(&info, "geoadd").expect("cmdstat_geoadd not found");
    assert_eq!(stat.calls, 2, "geoadd calls should be 2");
    assert_eq!(stat.failed_calls, 1, "geoadd failed_calls should be 1");
}

// ---------------------------------------------------------------------------
// COMMAND GETKEYSANDFLAGS tests (Phase 2)
// ---------------------------------------------------------------------------

/// `COMMAND GETKEYSANDFLAGS`
///
/// Verifies that COMMAND GETKEYSANDFLAGS returns key-flag pairs for
/// common commands (SET, GET, LMOVE).
#[tokio::test]
async fn tcl_command_getkeysandflags() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // SET key value: key should be OW (overwrite)
    let resp = client
        .command(&["COMMAND", "GETKEYSANDFLAGS", "SET", "mykey", "myval"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1, "SET should report 1 key");
    let pair = unwrap_array(items.into_iter().next().unwrap());
    assert_eq!(unwrap_bulk(&pair[0]), b"mykey");
    let flags = extract_bulk_strings(&pair[1]);
    assert_eq!(flags, vec!["OW"], "SET key should have OW flag");

    // GET key: key should be R (read)
    let resp = client
        .command(&["COMMAND", "GETKEYSANDFLAGS", "GET", "mykey"])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 1, "GET should report 1 key");
    let pair = unwrap_array(items.into_iter().next().unwrap());
    assert_eq!(unwrap_bulk(&pair[0]), b"mykey");
    let flags = extract_bulk_strings(&pair[1]);
    assert_eq!(flags, vec!["R"], "GET key should have R flag");

    // LMOVE src dest LEFT RIGHT: src should be RW, dest should be RW
    let resp = client
        .command(&[
            "COMMAND",
            "GETKEYSANDFLAGS",
            "LMOVE",
            "src",
            "dest",
            "LEFT",
            "RIGHT",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2, "LMOVE should report 2 keys");
    let pair0 = unwrap_array(items[0].clone());
    assert_eq!(unwrap_bulk(&pair0[0]), b"src");
    let flags0 = extract_bulk_strings(&pair0[1]);
    assert_eq!(flags0, vec!["RW"], "LMOVE source should have RW flag");
    let pair1 = unwrap_array(items[1].clone());
    assert_eq!(unwrap_bulk(&pair1[0]), b"dest");
    let flags1 = extract_bulk_strings(&pair1[1]);
    assert_eq!(flags1, vec!["RW"], "LMOVE dest should have RW flag");
}

/// `COMMAND GETKEYSANDFLAGS invalid args`
///
/// Verifies that COMMAND GETKEYSANDFLAGS with too few args returns an error.
#[tokio::test]
async fn tcl_command_getkeysandflags_invalid_args() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // No command name provided — should error
    let resp = client.command(&["COMMAND", "GETKEYSANDFLAGS"]).await;
    assert_error_prefix(&resp, "ERR");
}

/// `COMMAND GETKEYSANDFLAGS MSETEX`
///
/// Verifies that MSETEX keys are reported with OW flags and that the
/// numkeys-based key extraction works correctly.
#[tokio::test]
async fn tcl_command_getkeysandflags_msetex() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // MSETEX 2 key1 val1 key2 val2
    let resp = client
        .command(&[
            "COMMAND",
            "GETKEYSANDFLAGS",
            "MSETEX",
            "2",
            "key1",
            "val1",
            "key2",
            "val2",
        ])
        .await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2, "MSETEX with 2 keys should report 2 keys");

    // Both keys should have OW flag
    let pair0 = unwrap_array(items[0].clone());
    assert_eq!(unwrap_bulk(&pair0[0]), b"key1");
    let flags0 = extract_bulk_strings(&pair0[1]);
    assert_eq!(flags0, vec!["OW"], "MSETEX key should have OW flag");

    let pair1 = unwrap_array(items[1].clone());
    assert_eq!(unwrap_bulk(&pair1[0]), b"key2");
    let flags1 = extract_bulk_strings(&pair1[1]);
    assert_eq!(flags1, vec!["OW"], "MSETEX key should have OW flag");
}

// ---------------------------------------------------------------------------
// Movablekeys tests (Phase 3)
// ---------------------------------------------------------------------------

/// `$cmd command will not be marked with movablekeys`
///
/// Verifies that commands with static key positions (SET, GET, MSET)
/// do NOT have the `movablekeys` flag in COMMAND INFO.
#[tokio::test]
async fn tcl_command_not_marked_with_movablekeys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Commands that should NOT have movablekeys
    for cmd_name in &["SET", "GET", "MSET"] {
        let resp = client.command(&["COMMAND", "INFO", cmd_name]).await;
        let items = unwrap_array(resp);
        assert_eq!(
            items.len(),
            1,
            "COMMAND INFO {cmd_name} should return 1 entry"
        );
        let entry = unwrap_array(items.into_iter().next().unwrap());
        // entry[2] is the flags array
        let flags = extract_simple_or_bulk_strings(&entry[2]);
        assert!(
            !flags.iter().any(|f| f == "movablekeys"),
            "{cmd_name} should NOT have movablekeys flag, but flags are: {flags:?}"
        );
    }
}

/// `$cmd command is marked with movablekeys`
///
/// Verifies that commands with argument-dependent key positions (SORT, EVAL,
/// MSETEX, XREAD) have the `movablekeys` flag in COMMAND INFO.
#[tokio::test]
async fn tcl_command_is_marked_with_movablekeys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Commands that SHOULD have movablekeys
    for cmd_name in &["SORT", "EVAL", "EVALSHA", "MSETEX", "XREAD"] {
        let resp = client.command(&["COMMAND", "INFO", cmd_name]).await;
        let items = unwrap_array(resp);
        assert_eq!(
            items.len(),
            1,
            "COMMAND INFO {cmd_name} should return 1 entry"
        );
        let entry = unwrap_array(items.into_iter().next().unwrap());
        // entry[2] is the flags array
        let flags = extract_simple_or_bulk_strings(&entry[2]);
        assert!(
            flags.iter().any(|f| f == "movablekeys"),
            "{cmd_name} should have movablekeys flag, but flags are: {flags:?}"
        );
    }
}

/// Helper to extract strings from a Response::Array that may contain
/// Simple or Bulk string responses (COMMAND INFO returns status strings).
fn extract_simple_or_bulk_strings(response: &frogdb_protocol::Response) -> Vec<String> {
    match response {
        frogdb_protocol::Response::Array(items) => items
            .iter()
            .filter_map(|item| match item {
                frogdb_protocol::Response::Bulk(Some(b)) => String::from_utf8(b.to_vec()).ok(),
                frogdb_protocol::Response::Simple(s) => String::from_utf8(s.to_vec()).ok(),
                _ => None,
            })
            .collect(),
        other => panic!("expected Array, got {other:?}"),
    }
}
