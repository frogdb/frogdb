use frogdb_core::CommandRegistry;

/// Register all built-in commands.
pub fn register_commands(registry: &mut CommandRegistry) {
    // Register data-structure commands from the frogdb-commands crate.
    frogdb_commands::register_all(registry);

    // =====================================================================
    // Server-specific commands (depend on server infrastructure)
    // =====================================================================

    // AUTH / HELLO: migrated behind the ConnCtx seam as mutating connection
    // commands (they change per-connection auth/protocol state via
    // `ConnCtx::conn_state`). Registered as CommandImpl::Connection executors and
    // dispatched through the registry union — but intercepted *early*, before the
    // NOAUTH pre-check, so a not-yet-authenticated client can still run them.
    registry.register_connection(&crate::connection::auth_conn_command::AUTH_CONN_COMMAND);
    registry.register_connection(&crate::connection::auth_conn_command::HELLO_CONN_COMMAND);

    // Persistence commands. BGSAVE/LASTSAVE are migrated behind the ConnCtx seam
    // (registered as CommandImpl::Connection executors, dispatched through the
    // registry union rather than the legacy router→connection-handler path).
    registry.register_connection(&crate::connection::persistence_conn_command::BGSAVE_CONN_COMMAND);
    registry
        .register_connection(&crate::connection::persistence_conn_command::LASTSAVE_CONN_COMMAND);
    registry.register(crate::commands::persistence::DumpCommand);
    registry.register(crate::commands::persistence::RestoreCommand);

    // Migration commands
    registry.register(crate::commands::migrate_cmd::MigrateCommand);

    // Transaction commands (MULTI/EXEC/DISCARD/WATCH/UNWATCH): migrated behind
    // the ConnCtx seam (registered as CommandImpl::Connection executors). They
    // are intercepted before the transaction-queuing check and dispatched
    // through `dispatch_transaction_command`; MULTI/DISCARD/WATCH/UNWATCH run
    // their executors over the mutable ConnCtx, while EXEC's orchestration stays
    // in `handle_exec` (the executor is a spec carrier only).
    //
    // WATCH is the only transaction command with keys; `COMMAND GETKEYS`
    // resolves it through the registry *union* (`get_entry`), so its
    // `Connection` executor (whose default `dynamic_keys` extracts `KeySpec::All`
    // from its spec) supplies the keys — no shard-local stub is needed.
    registry.register_connection(&crate::connection::transaction_conn_command::MULTI_CONN_COMMAND);
    registry.register_connection(&crate::connection::transaction_conn_command::EXEC_CONN_COMMAND);
    registry
        .register_connection(&crate::connection::transaction_conn_command::DISCARD_CONN_COMMAND);
    registry.register_connection(&crate::connection::transaction_conn_command::WATCH_CONN_COMMAND);
    registry
        .register_connection(&crate::connection::transaction_conn_command::UNWATCH_CONN_COMMAND);

    // Scripting commands (EVAL/EVALSHA/EVAL_RO/EVALSHA_RO/SCRIPT): migrated
    // behind the ConnCtx seam (registered as CommandImpl::Connection executors,
    // dispatched through the registry union rather than the legacy
    // router→connection-handler path). `COMMAND GETKEYS` resolves them through
    // the registry *union* (`get_entry`), so each connection executor's
    // `dynamic_keys` (numkeys-based for the keyed EVAL/EVALSHA family) supplies
    // the keys directly — no shard-local key-extraction stub is needed.
    registry.register_connection(&crate::connection::scripting_conn_command::EVAL_CONN_COMMAND);
    registry.register_connection(&crate::connection::scripting_conn_command::EVALSHA_CONN_COMMAND);
    registry.register_connection(&crate::connection::scripting_conn_command::EVAL_RO_CONN_COMMAND);
    registry
        .register_connection(&crate::connection::scripting_conn_command::EVALSHA_RO_CONN_COMMAND);
    registry.register_connection(&crate::connection::scripting_conn_command::SCRIPT_CONN_COMMAND);

    // Server commands
    registry.register(crate::commands::server::DbsizeCommand);
    registry.register(crate::commands::server::FlushdbCommand);
    registry.register(crate::commands::server::FlushallCommand);
    registry.register(crate::commands::server::TimeCommand);
    registry.register(crate::commands::server::ShutdownCommand);
    registry.register(crate::commands::server::LolwutCommand);

    // Info command. Two distinct executors share the name "INFO":
    //   - The shard-local `InfoCommand` (registered here as a
    //     `CommandImpl::Shard`) serves `redis.call('INFO')` inside scripts, which
    //     runs on the owning shard and reports that shard's own view. Scripts
    //     resolve it through the `commands` map (`CommandRegistry::get`).
    //   - The connection-level `INFO_CONN_COMMAND` (registered just below as a
    //     `CommandImpl::Connection`) does the fleet scatter + connection-level
    //     aggregation for clients, dispatched through the registry union.
    // `register_connection` must run *after* `register` so the shared `entries`
    // slot for "INFO" ends up holding the `Connection` executor (connection-level
    // dispatch), while `commands["INFO"]` keeps the shard executor (scripts).
    registry.register(crate::commands::info::InfoCommand);
    registry.register_connection(&crate::connection::info_conn_command::INFO_CONN_COMMAND);

    // CLIENT: migrated behind the ConnCtx seam as a mutating connection command
    // (it changes per-connection name/reply/tracking/caching state via
    // `ConnCtx::conn_state` and drives tracking IO via `ConnCtx::tracking`).
    // Registered as a CommandImpl::Connection executor, dispatched through the
    // registry union rather than the legacy router→connection-handler path.
    registry.register_connection(&crate::connection::client_conn_command::CLIENT_CONN_COMMAND);

    // Config commands (migrated behind the ConnCtx seam: registered as a
    // CommandImpl::Connection executor, dispatched through the registry union
    // rather than the legacy router→connection-handler path).
    registry.register_connection(&crate::connection::conn_command::CONFIG_CONN_COMMAND);

    // DEBUG: migrated behind the ConnCtx seam (registered as a
    // CommandImpl::Connection executor, dispatched through the registry union
    // rather than the legacy router→connection-handler path). `COMMAND GETKEYS`
    // resolves DEBUG through the registry *union* (`get_entry`), so the
    // connection executor's `dynamic_keys` (which extracts DEBUG OBJECT's key)
    // supplies it — no shard-local stub is needed.
    registry.register_connection(&crate::connection::debug_conn_command::DEBUG_CONN_COMMAND);

    // Slowlog commands (migrated behind the ConnCtx seam: registered as a
    // CommandImpl::Connection executor, dispatched through the registry union
    // rather than the legacy router→connection-handler path).
    registry
        .register_connection(&crate::connection::observability_conn_command::SLOWLOG_CONN_COMMAND);

    // FROGDB.HOTSHARDS: renders the installed hot-shard detector's snapshot.
    // Connection-level because the collector is a node-wide handle, not shard
    // state — the collector itself does the per-shard scatter.
    registry
        .register_connection(&crate::connection::hotshards_conn_command::HOTSHARDS_CONN_COMMAND);

    // ACL migrated behind the ConnCtx seam: registered as a
    // CommandImpl::Connection executor, dispatched through the registry union
    // rather than the legacy router→connection-handler path.
    registry.register_connection(&crate::connection::acl_conn_command::ACL_CONN_COMMAND);

    // Pub/Sub commands (migrated behind the ConnCtx seam: registered as
    // CommandImpl::Connection executors, dispatched through the registry union
    // via the multi-response `execute_multi` seam — SUBSCRIBE/UNSUBSCRIBE emit
    // one confirmation per channel — rather than the legacy
    // router→connection-handler path).
    registry.register_connection(&crate::connection::pubsub_conn_command::SUBSCRIBE_CONN_COMMAND);
    registry.register_connection(&crate::connection::pubsub_conn_command::PSUBSCRIBE_CONN_COMMAND);
    registry.register_connection(&crate::connection::pubsub_conn_command::SSUBSCRIBE_CONN_COMMAND);
    registry.register_connection(&crate::connection::pubsub_conn_command::UNSUBSCRIBE_CONN_COMMAND);
    registry
        .register_connection(&crate::connection::pubsub_conn_command::PUNSUBSCRIBE_CONN_COMMAND);
    registry
        .register_connection(&crate::connection::pubsub_conn_command::SUNSUBSCRIBE_CONN_COMMAND);
    registry.register_connection(&crate::connection::pubsub_conn_command::PUBLISH_CONN_COMMAND);
    registry.register_connection(&crate::connection::pubsub_conn_command::SPUBLISH_CONN_COMMAND);
    registry.register_connection(&crate::connection::pubsub_conn_command::PUBSUB_CONN_COMMAND);

    // Cluster
    registry.register(crate::commands::cluster::ClusterCommand);

    // Connection-state commands (RESET / ASKING / READONLY / READWRITE): migrated
    // behind the ConnCtx seam as *mutating* connection commands (they change
    // per-connection state via `ConnCtx::conn_state`). Registered as
    // CommandImpl::Connection executors and dispatched through the mutable
    // `conn_ctx_for` view rather than the legacy router→connection-handler
    // path. RESET is intercepted early (direct dispatch, never queued/paused);
    // ASKING/READONLY/READWRITE dispatch through the mutable registry union.
    registry
        .register_connection(&crate::connection::connection_state_conn_command::RESET_CONN_COMMAND);
    registry.register_connection(
        &crate::connection::connection_state_conn_command::ASKING_CONN_COMMAND,
    );
    registry.register_connection(
        &crate::connection::connection_state_conn_command::READONLY_CONN_COMMAND,
    );
    registry.register_connection(
        &crate::connection::connection_state_conn_command::READWRITE_CONN_COMMAND,
    );

    // Replication
    registry.register(crate::commands::replication::ReplicaofCommand);
    registry.register(crate::commands::replication::SlaveofCommand);
    registry.register(crate::commands::replication::WaitCommand);
    registry.register(crate::commands::stub::WaitaofCommand);
    registry.register(crate::commands::replication::PsyncCommand);
    registry.register(crate::commands::replication::ReplconfCommand);
    registry.register(crate::commands::replication::RoleCommand);

    // Memory (migrated behind the ConnCtx seam).
    registry
        .register_connection(&crate::connection::observability_conn_command::MEMORY_CONN_COMMAND);

    // Latency (migrated behind the ConnCtx seam).
    registry
        .register_connection(&crate::connection::observability_conn_command::LATENCY_CONN_COMMAND);

    // Hotkeys (migrated behind the ConnCtx seam: registered as a
    // CommandImpl::Connection executor, dispatched through the registry union
    // rather than the legacy router→connection-handler path).
    registry.register_connection(&crate::connection::hotkeys::HOTKEYS_CONN_COMMAND);

    // Status (migrated behind the ConnCtx seam).
    registry
        .register_connection(&crate::connection::observability_conn_command::STATUS_CONN_COMMAND);

    // Module
    registry.register(crate::commands::stub::ModuleCommand);

    // Function (FUNCTION/FCALL/FCALL_RO): migrated behind the ConnCtx seam,
    // mirroring the scripting registration above. `COMMAND GETKEYS` resolves
    // them through the registry *union* (`get_entry`), so the connection
    // executors' `dynamic_keys` (numkeys-based for FCALL/FCALL_RO) supply the
    // keys — no shard-local key-extraction stub is needed.
    registry.register_connection(&crate::connection::scripting_conn_command::FUNCTION_CONN_COMMAND);
    registry.register_connection(&crate::connection::scripting_conn_command::FCALL_CONN_COMMAND);
    registry.register_connection(&crate::connection::scripting_conn_command::FCALL_RO_CONN_COMMAND);

    // Search commands
    registry.register(crate::commands::search::FtCreateCommand);
    registry.register(crate::commands::search::FtAlterCommand);
    registry.register(crate::commands::search::FtSearchCommand);
    registry.register(crate::commands::search::FtDropIndexCommand);
    registry.register(crate::commands::search::FtInfoCommand);
    registry.register(crate::commands::search::FtListCommand);
    registry.register(crate::commands::search::FtAggregateCommand);
    registry.register(crate::commands::search::FtHybridCommand);
    registry.register(crate::commands::search::FtSynupdateCommand);
    registry.register(crate::commands::search::FtSyndumpCommand);
    registry.register(crate::commands::search::FtSugaddCommand);
    registry.register(crate::commands::search::FtSuggetCommand);
    registry.register(crate::commands::search::FtSugdelCommand);
    registry.register(crate::commands::search::FtSuglenCommand);
    registry.register(crate::commands::search::FtAliasaddCommand);
    registry.register(crate::commands::search::FtAliasdelCommand);
    registry.register(crate::commands::search::FtAliasupdateCommand);
    registry.register(crate::commands::search::FtTagvalsCommand);
    registry.register(crate::commands::search::FtDictaddCommand);
    registry.register(crate::commands::search::FtDictdelCommand);
    registry.register(crate::commands::search::FtDictdumpCommand);
    registry.register(crate::commands::search::FtConfigCommand);
    registry.register(crate::commands::search::FtSpellcheckCommand);
    // FT.CURSOR: migrated behind the ConnCtx seam (registered as a
    // CommandImpl::Connection executor, dispatched through the registry union
    // rather than the legacy router→connection-handler path).
    registry.register_connection(&crate::connection::conn_command::FT_CURSOR_CONN_COMMAND);
    registry.register(crate::commands::search::FtExplainCommand);
    registry.register(crate::commands::search::FtExplainCliCommand);
    registry.register(crate::commands::search::FtProfileCommand);

    // Version / upgrade commands
    registry.register(crate::commands::version::FrogdbVersionCommand);
    registry.register(crate::commands::version::FrogdbFinalizeCommand);

    // Connection/Stubs
    registry.register(crate::commands::stub::SelectCommand);
    registry.register(crate::commands::stub::SwapdbCommand);
    // RESET is registered above alongside the other connection-state commands.
    registry.register(crate::commands::stub::BgrewriteaofCommand);
    // RPOPLPUSH now registered via frogdb_commands::list::RpoplpushCommand
    registry.register(crate::commands::stub::SyncCommand);
    registry.register(crate::commands::stub::SaveCommand);
    // MONITOR: migrated behind the ConnCtx seam (registered as a
    // CommandImpl::Connection executor, dispatched through the registry union via
    // `execute_monitor` rather than the legacy router→connection-handler path).
    // It registers the connection as a monitor and replies +OK; the run-loop
    // streams the executed-command feed.
    registry.register_connection(&crate::connection::monitor_conn_command::MONITOR_CONN_COMMAND);
    registry.register(crate::commands::stub::MoveCommand);
}

#[cfg(test)]
mod spec_exhaustiveness {
    //! Registry-wide machine-checks that the declarative CommandSpec stays
    //! complete. These make the silent omissions that motivated the spec
    //! (missing keyspace event, missing waiter wake, missing WAL strategy)
    //! impossible to land without a failing test.
    use super::*;
    use frogdb_core::{
        CommandFlags, EventSpec, IndexKind, ReindexSpec, WaiterKind, WaiterWake, WalStrategy,
    };

    /// WRITE commands that legitimately persist nothing through the key WAL:
    /// FLUSH* clear via RocksDB, FT.* persist through the search index, MIGRATE
    /// streams via async external I/O, and MOVE/SWAPDB reject before writing.
    const WAL_NOOP_ALLOWLIST: &[&str] = &[
        "FLUSHDB",
        "FLUSHALL",
        "FROGDB.FINALIZE",
        "FT.ALIASADD",
        "FT.ALIASDEL",
        "FT.ALIASUPDATE",
        "FT.ALTER",
        "FT.CREATE",
        "FT.DICTADD",
        "FT.DICTDEL",
        "FT.DROPINDEX",
        "FT.SUGADD",
        "FT.SUGDEL",
        "FT.SYNUPDATE",
        "MIGRATE",
        "MOVE",
        "SWAPDB",
    ];

    fn full_registry() -> CommandRegistry {
        let mut r = CommandRegistry::new();
        register_commands(&mut r);
        r
    }

    /// Every full command's spec satisfies its cross-field invariants
    /// (Dynamic keys iff MOVABLEKEYS, arity covers the keys it reads,
    /// Suppressed only on WRITE, WRITE declares an event).
    #[test]
    fn every_full_command_spec_validates() {
        for (name, entry) in full_registry().iter() {
            if let Some(cmd) = entry.as_command() {
                cmd.spec()
                    .validate()
                    .unwrap_or_else(|e| panic!("{name}: invalid spec: {e}"));
            }
        }
    }

    /// A WRITE command must declare a keyspace event (Emits or, explicitly,
    /// Suppressed) — never the read-command default NotApplicable.
    #[test]
    fn every_write_command_declares_event() {
        for (name, entry) in full_registry().iter() {
            if let Some(cmd) = entry.as_command() {
                let spec = cmd.spec();
                if spec.flags.contains(CommandFlags::WRITE) {
                    assert!(
                        !matches!(spec.event, EventSpec::NotApplicable),
                        "{name}: WRITE command must declare Emits or Suppressed"
                    );
                }
            }
        }
    }

    /// A WRITE command must declare a WAL strategy unless it is on the explicit
    /// allowlist of commands that persist through another mechanism.
    #[test]
    fn every_write_command_declares_wal() {
        for (name, entry) in full_registry().iter() {
            if let Some(cmd) = entry.as_command() {
                let spec = cmd.spec();
                if spec.flags.contains(CommandFlags::WRITE) && matches!(spec.wal, WalStrategy::NoOp)
                {
                    assert!(
                        WAL_NOOP_ALLOWLIST.contains(&name),
                        "{name}: WRITE command must declare a WAL strategy (or be allowlisted)"
                    );
                }
            }
        }
    }

    /// Regression guard for the waiter-wake bug class: commands that add data
    /// to a structure with blocking readers must wake the matching waiter kind.
    /// LREM/RPOPLPUSH/LMOVE/ZINCRBY were all silent omissions before the spec.
    #[test]
    fn data_adding_commands_wake_blocked_clients() {
        #[allow(unused_mut)]
        let mut expected: Vec<(&str, WaiterKind)> = vec![
            ("LPUSH", WaiterKind::List),
            ("RPUSH", WaiterKind::List),
            ("LINSERT", WaiterKind::List),
            ("RPOPLPUSH", WaiterKind::List),
            ("LMOVE", WaiterKind::List),
            ("ZADD", WaiterKind::SortedSet),
            ("ZINCRBY", WaiterKind::SortedSet),
        ];
        #[cfg(feature = "cmd-stream")]
        expected.push(("XADD", WaiterKind::Stream));
        let r = full_registry();
        for (name, kind) in &expected {
            let entry = r
                .get_entry(name)
                .unwrap_or_else(|| panic!("{name} not registered"));
            let cmd = entry.as_command().unwrap();
            assert_eq!(
                cmd.spec().wakes,
                WaiterWake::Kind(*kind),
                "{name} adds data and must wake {kind:?} waiters"
            );
        }
    }

    /// Regression guard for the keyspace-event over-emission bug class
    /// (proposal 44): STORE-family commands must pin their event to the
    /// destination key (`EmitsAt`), and commands whose written keys are only
    /// knowable at runtime must deposit their events (`Dynamic`). A blanket
    /// `Emits` on any of these would notify read-only source keys — and is
    /// also rejected wholesale by `CommandSpec::validate()` for multi-key
    /// KeySpecs outside the DEL/UNLINK/MSET/MSETNX allowlist.
    #[test]
    fn multi_key_commands_declare_accurate_events() {
        // (command, destination index into the extracted key list)
        #[allow(unused_mut)]
        let mut emits_at: Vec<(&str, usize)> = vec![
            ("ZRANGESTORE", 0),
            ("ZUNIONSTORE", 0),
            ("ZINTERSTORE", 0),
            ("ZDIFFSTORE", 0),
            ("SUNIONSTORE", 0),
            ("SINTERSTORE", 0),
            ("SDIFFSTORE", 0),
            ("COPY", 1),
        ];
        // Phase 2: former Suppressed under-emitters with a static dest.
        #[cfg(feature = "cmd-hyperloglog")]
        emits_at.push(("PFMERGE", 0));

        #[allow(unused_mut)]
        let mut dynamic: Vec<&str> = vec![
            "RENAME",
            "RENAMENX",
            "SMOVE",
            "LMOVE",
            "RPOPLPUSH",
            "ZMPOP",
            "BLPOP",
            "BRPOP",
            "BLMOVE",
            "BZPOPMIN",
            "BZPOPMAX",
            // Phase 2: set-or-del (BITOP) and popped-key-only (LMPOP) are
            // runtime-dependent.
            "BITOP",
            "LMPOP",
            // Phase 4: dynamic-key STORE commands (dest present only with STORE,
            // set-or-del), the last Suppressed blocking-pop members, and the
            // blocking multi-pops.
            "SORT",
            "BRPOPLPUSH",
            "BLMPOP",
            "BZMPOP",
        ];
        // GEOSEARCHSTORE is set-or-del; GEORADIUS/GEORADIUSBYMEMBER only have a
        // destination when invoked with STORE.
        #[cfg(feature = "cmd-geo")]
        dynamic.extend_from_slice(&["GEOSEARCHSTORE", "GEORADIUS", "GEORADIUSBYMEMBER"]);

        let r = full_registry();
        for (name, dest_index) in &emits_at {
            let entry = r
                .get_entry(name)
                .unwrap_or_else(|| panic!("{name} not registered"));
            let spec = entry.as_command().unwrap().spec();
            assert!(
                matches!(spec.event, EventSpec::EmitsAt { key_index, .. } if key_index == *dest_index),
                "{name} writes only its destination (key {dest_index}) and must declare EmitsAt, got {:?}",
                spec.event
            );
        }
        for name in &dynamic {
            let entry = r
                .get_entry(name)
                .unwrap_or_else(|| panic!("{name} not registered"));
            let spec = entry.as_command().unwrap().spec();
            assert_eq!(
                spec.event,
                EventSpec::Dynamic,
                "{name}'s written keys are runtime-dependent and must deposit events (Dynamic)"
            );
        }
    }

    /// Reindex conformance (proposal 15): the hash-family and JSON-family write
    /// commands — the only writes that mutate indexable content — must declare
    /// the `ReindexSpec` the `SearchIndex` write effect resolves, plus the
    /// key-set writers (DEL/UNLINK/RENAME/RENAMENX).
    ///
    /// This is a **reviewer-maintained positive allowlist**, not an automatic
    /// completeness proof. Unlike `every_write_command_declares_wal`, it cannot
    /// iterate all writes and trip on the `None` default, because `None` is the
    /// *correct* default for the vast majority of writes (every
    /// string/list/set/zset/stream mutation, plus the `HPERSIST` carve-out).
    /// It catches a *listed* command whose fact drifts; a newly-added hash write
    /// the reviewer forgets to add here (and to the spec) still ships stale — the
    /// same failure mode as the old string match, relocated to a
    /// declaration-adjacent field with better locality. Adding a hash/JSON write
    /// command means adding it here.
    #[test]
    fn reindex_facts_declared_for_hash_and_json_writes() {
        let hash = IndexKind::Hash;
        #[cfg(feature = "cmd-json")]
        let json = IndexKind::Json;
        // (command, expected ReindexSpec)
        #[allow(unused_mut)]
        let mut expected: Vec<(&str, ReindexSpec)> = vec![
            // Hash writes whose key survives — reindex the first key.
            ("HSET", ReindexSpec::FirstKey { kind: hash }),
            ("HSETNX", ReindexSpec::FirstKey { kind: hash }),
            ("HMSET", ReindexSpec::FirstKey { kind: hash }),
            ("HINCRBY", ReindexSpec::FirstKey { kind: hash }),
            ("HINCRBYFLOAT", ReindexSpec::FirstKey { kind: hash }),
            ("HSETEX", ReindexSpec::FirstKey { kind: hash }),
            // Hash writes that may empty the key (field delete / lazy purge) —
            // reindex if it survives, else drop it.
            ("HDEL", ReindexSpec::FirstKeyOrDelete { kind: hash }),
            ("HGETDEL", ReindexSpec::FirstKeyOrDelete { kind: hash }),
            ("HEXPIRE", ReindexSpec::FirstKeyOrDelete { kind: hash }),
            ("HPEXPIRE", ReindexSpec::FirstKeyOrDelete { kind: hash }),
            ("HEXPIREAT", ReindexSpec::FirstKeyOrDelete { kind: hash }),
            ("HPEXPIREAT", ReindexSpec::FirstKeyOrDelete { kind: hash }),
            ("HGETEX", ReindexSpec::FirstKeyOrDelete { kind: hash }),
            // Key-set writers.
            ("DEL", ReindexSpec::DeleteKeys),
            ("UNLINK", ReindexSpec::DeleteKeys),
            ("RENAME", ReindexSpec::Rename),
            ("RENAMENX", ReindexSpec::Rename),
            // Cross-type clobbers (round-10 follow-up 5): a write that can replace
            // an indexed hash with a non-hash value, or write a hash into an
            // index-prefix key, must reconcile the destination via Refresh so the
            // search doc is dropped (stale) or added (new hash) as appropriate.
            ("SET", ReindexSpec::RefreshFirstKey),
            ("SETEX", ReindexSpec::RefreshFirstKey),
            ("PSETEX", ReindexSpec::RefreshFirstKey),
            ("RESTORE", ReindexSpec::RefreshFirstKey),
            ("COPY", ReindexSpec::RefreshSecondKey),
        ];
        #[cfg(feature = "cmd-json")]
        expected.extend([
            // JSON writes whose key survives.
            ("JSON.SET", ReindexSpec::FirstKey { kind: json }),
            ("JSON.MERGE", ReindexSpec::FirstKey { kind: json }),
            ("JSON.STRAPPEND", ReindexSpec::FirstKey { kind: json }),
            ("JSON.TOGGLE", ReindexSpec::FirstKey { kind: json }),
            ("JSON.ARRAPPEND", ReindexSpec::FirstKey { kind: json }),
            ("JSON.ARRINSERT", ReindexSpec::FirstKey { kind: json }),
            ("JSON.ARRPOP", ReindexSpec::FirstKey { kind: json }),
            ("JSON.ARRTRIM", ReindexSpec::FirstKey { kind: json }),
            ("JSON.NUMINCRBY", ReindexSpec::FirstKey { kind: json }),
            ("JSON.NUMMULTBY", ReindexSpec::FirstKey { kind: json }),
            // JSON writes that may drop the key.
            ("JSON.DEL", ReindexSpec::FirstKeyOrDelete { kind: json }),
            ("JSON.CLEAR", ReindexSpec::FirstKeyOrDelete { kind: json }),
        ]);
        // Explicit carve-outs: WRITE hash commands that change no indexable value
        // and so legitimately declare `None` (they must NOT reindex).
        let carve_outs: &[&str] = &["HPERSIST"];

        let r = full_registry();
        for (name, want) in &expected {
            let entry = r
                .get_entry(name)
                .unwrap_or_else(|| panic!("{name} not registered"));
            let spec = entry.as_command().unwrap().spec();
            assert_eq!(
                spec.reindex, *want,
                "{name} must declare ReindexSpec {want:?} so the search index stays live",
            );
        }
        for name in carve_outs {
            let entry = r
                .get_entry(name)
                .unwrap_or_else(|| panic!("{name} not registered"));
            let spec = entry.as_command().unwrap().spec();
            assert_eq!(
                spec.reindex,
                ReindexSpec::None,
                "{name} changes no indexable value and must stay ReindexSpec::None",
            );
        }
    }

    /// The per-subcommand admin table and the whole-command `ADMIN` flag are one
    /// description, not two: a command with a split surface must not also claim
    /// to be wholly admin, and every split entry must name a live command.
    // FM-CLUSTER-064
    #[test]
    fn split_admin_surfaces_agree_with_command_flags() {
        let r = full_registry();
        for name in frogdb_core::split_admin_surface_commands() {
            let entry = r.get_entry(name).unwrap_or_else(|| {
                panic!("{name} declares a split admin surface but is not registered")
            });
            assert!(
                !entry.flags().contains(CommandFlags::ADMIN),
                "{name} declares a split admin surface, so it must not also carry the \
                 whole-command ADMIN flag",
            );
            assert!(
                matches!(
                    frogdb_core::admin_surface(name, entry.flags()),
                    frogdb_core::AdminSurface::SplitBySubcommand { .. }
                ),
                "{name} must resolve to a split admin surface",
            );
        }
    }

    /// Ratcheting registry↔ACL-category parity gate (hardening-2 C5).
    ///
    /// `CommandSpec` carries no ACL field: the ~390-command registry and the
    /// hand-written category table in `frogdb-acl` are joined only by a
    /// lowercase string, and `CommandCategory::all_for_command` ends in
    /// `.unwrap_or_default()` — a command with no category row yields an empty
    /// set, so `permissions.rs`'s deny loop never matches and every
    /// `-@category` rule is a **silent no-op** against it. That was the live
    /// defect of round2 issue 35, and it is what let `+@all -@admin` permit
    /// CLUSTER, MONITOR, MIGRATE, PSYNC, SYNC, REPLCONF and FUNCTION.
    ///
    /// The core-Redis half of that gap is **closed**: every registered command
    /// upstream documents now has a row, checked against the vendored
    /// `acl_categories` by
    /// `upstream_metadata_tests::vendored_acl_categories_agree_with_our_table`
    /// (`.scratch/redis-feel/issues/done/16-acl-category-table-gaps.md`).
    ///
    /// What remains is the module-family surface — FT.*, JSON.*, TS.*, BF.*,
    /// CF.*, CMS.*, TOPK.*, TDIGEST.*, V* and FrogDB's own ES.*. Upstream
    /// publishes no ACL categories for these: module `commands.json` files
    /// carry no `acl_categories` field at all, because a module declares its
    /// categories in C at `RedisModule_SetCommandACLCategories` time. Choosing
    /// their rows is therefore a design act, not a vendoring act, and is left
    /// as the residue of round2 issue 35 rather than guessed at here.
    ///
    /// This test does not fix that residue — it *pins* it so it can only
    /// shrink. The test fails if:
    ///   (a) a command **not** on the allowlist has no category row (a newly
    ///       added command landed category-less — the omission this gate exists
    ///       to make un-mergeable), or
    ///   (b) an allowlist entry is **stale** — the command has since gained a
    ///       category row, or no longer exists in the registry — so the list
    ///       cannot carry dead weight. Fixing the last row deletes the whole
    ///       allowlist.
    ///
    /// Feature-gating mirrors the sibling tests in this module: FT.* is
    /// registered unconditionally by the server crate and so is listed
    /// unconditionally; the exotic families follow their `cmd-*` feature so the
    /// allowlist tracks `register_commands` under both profiles the test is
    /// built with — `cmd-core` via `just test frogdb-server`, and `cmd-full`
    /// via the workspace-unified `cargo nextest run --all` in CI.
    fn acl_category_gap_allowlist() -> std::collections::BTreeSet<&'static str> {
        // Always-compiled surface (27 entries), all of them module-family.
        #[allow(unused_mut)]
        let mut set: std::collections::BTreeSet<&'static str> = [
            // Search (FT.*) — registered unconditionally by the server crate.
            "FT.AGGREGATE",
            "FT.ALIASADD",
            "FT.ALIASDEL",
            "FT.ALIASUPDATE",
            "FT.ALTER",
            "FT.CONFIG",
            "FT.CREATE",
            "FT.CURSOR",
            "FT.DICTADD",
            "FT.DICTDEL",
            "FT.DICTDUMP",
            "FT.DROPINDEX",
            "FT.EXPLAIN",
            "FT.EXPLAINCLI",
            "FT.HYBRID",
            "FT.INFO",
            "FT.PROFILE",
            "FT.SEARCH",
            "FT.SPELLCHECK",
            "FT.SUGADD",
            "FT.SUGDEL",
            "FT.SUGGET",
            "FT.SUGLEN",
            "FT.SYNDUMP",
            "FT.SYNUPDATE",
            "FT.TAGVALS",
            "FT._LIST",
        ]
        .into_iter()
        .collect();

        // Exotic command families — gated exactly as `register_commands` gates
        // them, so the allowlist and the registry appear/disappear together.
        #[cfg(feature = "cmd-json")]
        set.extend([
            "JSON.ARRAPPEND",
            "JSON.ARRINDEX",
            "JSON.ARRINSERT",
            "JSON.ARRLEN",
            "JSON.ARRPOP",
            "JSON.ARRTRIM",
            "JSON.CLEAR",
            "JSON.DEBUG",
            "JSON.DEL",
            "JSON.GET",
            "JSON.MERGE",
            "JSON.MGET",
            "JSON.NUMINCRBY",
            "JSON.NUMMULTBY",
            "JSON.OBJKEYS",
            "JSON.OBJLEN",
            "JSON.SET",
            "JSON.STRAPPEND",
            "JSON.STRLEN",
            "JSON.TOGGLE",
            "JSON.TYPE",
        ]);
        #[cfg(feature = "cmd-timeseries")]
        set.extend([
            "TS.ADD",
            "TS.ALTER",
            "TS.CREATE",
            "TS.CREATERULE",
            "TS.DECRBY",
            "TS.DEL",
            "TS.DELETERULE",
            "TS.GET",
            "TS.INCRBY",
            "TS.INFO",
            "TS.MADD",
            "TS.MGET",
            "TS.MRANGE",
            "TS.MREVRANGE",
            "TS.QUERYINDEX",
            "TS.RANGE",
            "TS.REVRANGE",
        ]);
        #[cfg(feature = "cmd-bloom")]
        set.extend([
            "BF.ADD",
            "BF.CARD",
            "BF.EXISTS",
            "BF.INFO",
            "BF.INSERT",
            "BF.LOADCHUNK",
            "BF.MADD",
            "BF.MEXISTS",
            "BF.RESERVE",
            "BF.SCANDUMP",
        ]);
        #[cfg(feature = "cmd-cuckoo")]
        set.extend([
            "CF.ADD",
            "CF.ADDNX",
            "CF.COUNT",
            "CF.DEL",
            "CF.EXISTS",
            "CF.INFO",
            "CF.INSERT",
            "CF.INSERTNX",
            "CF.LOADCHUNK",
            "CF.MEXISTS",
            "CF.RESERVE",
            "CF.SCANDUMP",
        ]);
        #[cfg(feature = "cmd-cms")]
        set.extend([
            "CMS.INCRBY",
            "CMS.INFO",
            "CMS.INITBYDIM",
            "CMS.INITBYPROB",
            "CMS.MERGE",
            "CMS.QUERY",
        ]);
        #[cfg(feature = "cmd-topk")]
        set.extend([
            "TOPK.ADD",
            "TOPK.COUNT",
            "TOPK.INCRBY",
            "TOPK.INFO",
            "TOPK.LIST",
            "TOPK.QUERY",
            "TOPK.RESERVE",
        ]);
        #[cfg(feature = "cmd-tdigest")]
        set.extend([
            "TDIGEST.ADD",
            "TDIGEST.CDF",
            "TDIGEST.CREATE",
            "TDIGEST.INFO",
            "TDIGEST.MAX",
            "TDIGEST.MERGE",
            "TDIGEST.MIN",
            "TDIGEST.QUANTILE",
            "TDIGEST.RANK",
            "TDIGEST.RESET",
            "TDIGEST.REVRANK",
            "TDIGEST.TRIMMED_MEAN",
        ]);
        #[cfg(feature = "cmd-vectorset")]
        set.extend([
            "VADD",
            "VCARD",
            "VDIM",
            "VEMB",
            "VGETATTR",
            "VINFO",
            "VLINKS",
            "VRANDMEMBER",
            "VRANGE",
            "VREM",
            "VSETATTR",
            "VSIM",
        ]);
        #[cfg(feature = "cmd-event-sourcing")]
        set.extend([
            "ES.ALL",
            "ES.APPEND",
            "ES.INFO",
            "ES.READ",
            "ES.REPLAY",
            "ES.SNAPSHOT",
        ]);

        set
    }

    /// Every registered command resolves to at least one ACL category, unless it
    /// is a known pre-existing gap on `acl_category_gap_allowlist`. This is the
    /// registry↔category parity gate: an empty category set silently defeats
    /// every `-@category` rule (round2 issue 35), so a *new* command must never
    /// land category-less, and the allowlist of existing gaps may only shrink.
    #[test]
    fn every_registered_command_has_acl_category_or_is_allowlisted() {
        use frogdb_core::CommandCategory;
        let allow = acl_category_gap_allowlist();
        let r = full_registry();

        // Direction (a): a registered command with no category row that is not
        // an accounted-for pre-existing gap. This trips when a new command is
        // added without a `COMMAND_ALL_CATEGORIES` row.
        let mut uncategorised_not_allowlisted: Vec<&str> = Vec::new();
        let mut registered: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
        for (name, _entry) in r.iter() {
            registered.insert(name);
            if CommandCategory::all_for_command(name).is_empty() && !allow.contains(name) {
                uncategorised_not_allowlisted.push(name);
            }
        }
        uncategorised_not_allowlisted.sort_unstable();
        assert!(
            uncategorised_not_allowlisted.is_empty(),
            "command(s) registered with no ACL category row and not on the pre-existing-gap \
             allowlist — every `-@category` rule is a silent no-op against these (round2 issue \
             35). Add a `COMMAND_ALL_CATEGORIES` row in frogdb-acl (preferred), or, only for a \
             genuine pre-existing gap, extend `acl_category_gap_allowlist`: \
             {uncategorised_not_allowlisted:?}"
        );

        // Direction (b): a stale allowlist entry — the command gained a category
        // row, or is no longer registered (in this feature profile). Keeping the
        // list honest is what forces it to shrink toward empty.
        let mut stale: Vec<&str> = Vec::new();
        for &name in &allow {
            // Stale for either reason: no longer registered in this profile, or it
            // gained a category row. Both retire the entry.
            if !registered.contains(name) || !CommandCategory::all_for_command(name).is_empty() {
                stale.push(name);
            }
        }
        stale.sort_unstable();
        assert!(
            stale.is_empty(),
            "stale `acl_category_gap_allowlist` entr(y/ies) — these now have a category row or \
             are no longer registered. Delete them so the ratchet keeps shrinking (fixing the \
             last entry removes the allowlist entirely): {stale:?}"
        );
    }

    /// Structural inverse (automatic, enforced by `CommandSpec::validate`): no
    /// non-WRITE command may declare a non-`None` reindex fact — reindexing is a
    /// write side effect. This iterates the whole registry and trips on any
    /// violation, no allowlist involved.
    #[test]
    fn no_read_command_declares_reindex() {
        for (name, entry) in full_registry().iter() {
            if let Some(cmd) = entry.as_command() {
                let spec = cmd.spec();
                if !spec.flags.contains(CommandFlags::WRITE) {
                    assert_eq!(
                        spec.reindex,
                        ReindexSpec::None,
                        "{name}: a non-WRITE command must not declare a reindex fact",
                    );
                }
            }
        }
    }
}
