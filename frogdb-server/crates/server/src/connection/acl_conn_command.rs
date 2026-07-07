//! ACL connection command (SETUSER/GETUSER/DELUSER/LIST/USERS/CAT/WHOAMI/
//! GENPASS/DRYRUN/LOG/SAVE/LOAD/HELP).
//!
//! Migrated behind the [`ConnectionCommand`] seam (see
//! [`crate::connection::conn_command`] and the CONFIG executor there for the
//! template). ACL reads only what it needs through its [`ConnCtx`]:
//! [`ConnCtx::acl_manager`] (the user store), [`ConnCtx::command_registry`]
//! (for DRYRUN's key-access resolution), and [`ConnCtx::username`] (the
//! connection's auth identity, for WHOAMI) — instead of taking
//! `&ConnectionHandler`. The whole subcommand tree is therefore unit-testable in
//! isolation (see `tests`).
//!
//! All three dependencies are `core` types, so ACL names them directly on the
//! `ConnCtx` rather than behind a bespoke provider trait (there is no server
//! type to abstract, unlike CONFIG's `ConfigManager`).

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, BoxFuture, CommandCategory, CommandFlags, CommandSpec, ConnCtx,
    ConnectionCommand, ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, LookupSpec,
    WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use crate::connection::util::{extract_subcommand, key_access_type_for_flags};

/// The `CommandSpec` for ACL. Declared here alongside the executor (rather than
/// in a stub `Command` impl) so the connection command is a single
/// self-contained unit. Strategy is `ConnectionLevel(Admin)`; the registry
/// validates that this agrees with the `Connection` executor variant.
static ACL_SPEC: CommandSpec = CommandSpec {
    name: "ACL",
    arity: Arity::AtLeast(1),
    flags: CommandFlags::ADMIN,
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin),
};

/// The registrable, `'static` ACL executor. Registered via
/// [`frogdb_core::CommandRegistry::register_connection`] in `server/register.rs`.
pub(crate) static ACL_CONN_COMMAND: AclConnCommand = AclConnCommand;

/// ACL — access-control-list management.
///
/// Reads a [`ConnCtx`] instead of the whole handler, so it is unit-testable in
/// isolation (see `tests` below).
pub(crate) struct AclConnCommand;

impl ConnectionCommand for AclConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &ACL_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move { handle_acl(ctx, args) })
    }
}

/// Dispatch an ACL command to its subcommand handler.
fn handle_acl(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'acl' command");
    }

    let subcmd = String::from_utf8_lossy(&args[0]).to_uppercase();
    let subcmd_args = &args[1..];

    match subcmd.as_str() {
        "WHOAMI" => acl_whoami(ctx),
        "LIST" => acl_list(ctx),
        "USERS" => acl_users(ctx),
        "GETUSER" => acl_getuser(ctx, subcmd_args),
        "SETUSER" => acl_setuser(ctx, subcmd_args),
        "DELUSER" => acl_deluser(ctx, subcmd_args),
        "CAT" => acl_cat(subcmd_args),
        "GENPASS" => acl_genpass(subcmd_args),
        "DRYRUN" => acl_dryrun(ctx, subcmd_args),
        "LOG" => acl_log(ctx, subcmd_args),
        "SAVE" => acl_save(ctx),
        "LOAD" => acl_load(ctx),
        "HELP" => {
            if !subcmd_args.is_empty() {
                return Response::error(
                    "ERR Unknown subcommand or wrong number of arguments for 'acl|help' command",
                );
            }
            acl_help()
        }
        _ => Response::error(format!(
            "ERR Unknown subcommand or wrong number of arguments for '{}'",
            subcmd
        )),
    }
}

/// ACL WHOAMI - return current username.
fn acl_whoami(ctx: &ConnCtx<'_>) -> Response {
    Response::bulk(Bytes::from(ctx.username.to_string()))
}

/// ACL LIST - list all users with their ACL rules.
fn acl_list(ctx: &ConnCtx<'_>) -> Response {
    let users = ctx.acl_manager.list_users_detailed();
    Response::Array(
        users
            .into_iter()
            .map(|s| Response::bulk(Bytes::from(s)))
            .collect(),
    )
}

/// ACL USERS - list all usernames.
fn acl_users(ctx: &ConnCtx<'_>) -> Response {
    let users = ctx.acl_manager.list_users();
    Response::Array(
        users
            .into_iter()
            .map(|s| Response::bulk(Bytes::from(s)))
            .collect(),
    )
}

/// ACL GETUSER <username> - get user info.
fn acl_getuser(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'acl|getuser' command");
    }

    let username = String::from_utf8_lossy(&args[0]);
    match ctx.acl_manager.get_user(&username) {
        Some(user) => {
            let info = user.to_getuser_info();
            let mut result = Vec::new();
            for (key, value) in info {
                result.push(Response::bulk(Bytes::from(key)));
                match value {
                    frogdb_core::acl::user::UserInfoValue::String(s) => {
                        result.push(Response::bulk(Bytes::from(s)));
                    }
                    frogdb_core::acl::user::UserInfoValue::StringArray(arr) => {
                        result.push(Response::Array(
                            arr.into_iter()
                                .map(|s| Response::bulk(Bytes::from(s)))
                                .collect(),
                        ));
                    }
                }
            }
            Response::Array(result)
        }
        None => Response::null(),
    }
}

/// ACL SETUSER <username> [rules...] - create or modify user.
fn acl_setuser(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'acl|setuser' command");
    }

    let username = String::from_utf8_lossy(&args[0]);
    let rules: Vec<&str> = args[1..]
        .iter()
        .map(|b| std::str::from_utf8(b).unwrap_or(""))
        .collect();

    match ctx.acl_manager.set_user(&username, &rules) {
        Ok(()) => Response::ok(),
        Err(e) => Response::error(e.to_string()),
    }
}

/// ACL DELUSER <username> [...] - delete users.
fn acl_deluser(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.is_empty() {
        return Response::error("ERR wrong number of arguments for 'acl|deluser' command");
    }

    let usernames: Vec<&str> = args
        .iter()
        .map(|b| std::str::from_utf8(b).unwrap_or(""))
        .collect();

    match ctx.acl_manager.delete_users(&usernames) {
        Ok(count) => Response::Integer(count as i64),
        Err(e) => Response::error(e.to_string()),
    }
}

/// ACL CAT [category] - list categories or commands in category.
fn acl_cat(args: &[Bytes]) -> Response {
    if args.is_empty() {
        // List all categories
        let categories: Vec<Response> = CommandCategory::all()
            .iter()
            .map(|c| Response::bulk(Bytes::from(c.name())))
            .collect();
        Response::Array(categories)
    } else {
        // List commands in category
        let category_name = String::from_utf8_lossy(&args[0]);
        match CommandCategory::parse(&category_name) {
            Some(category) => {
                let commands: Vec<Response> = category
                    .commands()
                    .iter()
                    .map(|c| Response::bulk(Bytes::from(*c)))
                    .collect();
                Response::Array(commands)
            }
            None => Response::error(format!("ERR Unknown ACL category '{}'", category_name)),
        }
    }
}

/// ACL DRYRUN <username> <command> [<arg> ...] - simulate permission check.
fn acl_dryrun(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if args.len() < 2 {
        return Response::error("ERR wrong number of arguments for 'acl|dryrun' command");
    }

    let username = String::from_utf8_lossy(&args[0]);
    let command = String::from_utf8_lossy(&args[1]).to_uppercase();
    let cmd_args = &args[2..];

    // Look up user
    let user = match ctx.acl_manager.get_user(&username) {
        Some(u) => u,
        None => return Response::error(format!("ERR User '{}' not found", username)),
    };

    // Extract subcommand for container commands
    let subcommand = extract_subcommand(&command, cmd_args);

    // Check command permission
    let cmd_allowed = user.check_command(&command, subcommand.as_deref());

    if !cmd_allowed {
        let msg = if let Some(ref sub) = subcommand {
            format!(
                "This user has no permissions to run the '{}|{}' command",
                command.to_lowercase(),
                sub.to_lowercase()
            )
        } else {
            format!(
                "This user has no permissions to run the '{}' command",
                command.to_lowercase()
            )
        };
        return Response::bulk(Bytes::from(msg));
    }

    // Check key permissions using the command's real key spec, so DRYRUN agrees
    // with live enforcement: access type derived from flags (read-only commands
    // check Read, not ReadWrite) and the actual key positions (incl. commands
    // whose key is not the first argument).
    if let Some(entry) = ctx.command_registry.get_entry(&command) {
        let access = key_access_type_for_flags(entry.flags());
        for key in entry.keys(cmd_args) {
            if !user.check_key_access(key, access) {
                let msg = format!(
                    "This user has no permissions to access the '{}' key",
                    String::from_utf8_lossy(key)
                );
                return Response::bulk(Bytes::from(msg));
            }
        }
    }

    Response::ok()
}

/// ACL GENPASS [bits] - generate secure random password.
///
/// The output is always at least 256 bits (64 hex chars) for security,
/// matching Redis behavior.
fn acl_genpass(args: &[Bytes]) -> Response {
    let bits = if args.is_empty() {
        256
    } else {
        match String::from_utf8_lossy(&args[0]).parse::<i64>() {
            Ok(b) if b > 0 && b <= 4096 => b as u32,
            _ => {
                return Response::error(
                    "ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096",
                );
            }
        }
    };

    let bits = bits.max(256);
    let password = frogdb_core::generate_password(bits);
    Response::bulk(Bytes::from(password))
}

/// ACL LOG [count|RESET] - view or reset security log.
fn acl_log(ctx: &ConnCtx<'_>, args: &[Bytes]) -> Response {
    if !args.is_empty() {
        let arg = String::from_utf8_lossy(&args[0]).to_uppercase();
        if arg == "RESET" {
            ctx.acl_manager.log().reset();
            return Response::ok();
        }
    }

    let count = if args.is_empty() {
        None
    } else {
        String::from_utf8_lossy(&args[0]).parse::<usize>().ok()
    };

    let entries = ctx.acl_manager.log().get(count);
    let result: Vec<Response> = entries
        .into_iter()
        .map(|entry| {
            let fields = entry.to_resp_fields();
            let mut arr = Vec::new();
            for (key, value) in fields {
                arr.push(Response::bulk(Bytes::from(key)));
                match value {
                    frogdb_core::acl::log::AclLogValue::String(s) => {
                        arr.push(Response::bulk(Bytes::from(s)));
                    }
                    frogdb_core::acl::log::AclLogValue::Integer(i) => {
                        arr.push(Response::Integer(i));
                    }
                    frogdb_core::acl::log::AclLogValue::Float(f) => {
                        arr.push(Response::bulk(Bytes::from(format!("{:.6}", f))));
                    }
                }
            }
            Response::Array(arr)
        })
        .collect();

    Response::Array(result)
}

/// ACL SAVE - save ACL to file.
fn acl_save(ctx: &ConnCtx<'_>) -> Response {
    match ctx.acl_manager.save() {
        Ok(()) => Response::ok(),
        Err(e) => Response::error(e.to_string()),
    }
}

/// ACL LOAD - load ACL from file.
fn acl_load(ctx: &ConnCtx<'_>) -> Response {
    match ctx.acl_manager.load() {
        Ok(()) => Response::ok(),
        Err(e) => Response::error(e.to_string()),
    }
}

/// ACL HELP - show help.
fn acl_help() -> Response {
    let help = vec![
        "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "CAT [<category>]",
        "    List all commands that belong to <category>, or all command categories",
        "    when no category is specified.",
        "DELUSER <username> [<username> ...]",
        "    Delete a list of users.",
        "GENPASS [<bits>]",
        "    Generate a secure random password. The optional `bits` argument specifies",
        "    the amount of bits for the password; default is 256.",
        "GETUSER <username>",
        "    Get the user details.",
        "LIST",
        "    List all users in ACL format.",
        "LOAD",
        "    Reload users from the ACL file.",
        "LOG [<count>|RESET]",
        "    List latest ACL security events, or RESET to clear log.",
        "SAVE",
        "    Save the current ACL to file.",
        "SETUSER <username> [<property> [<property> ...]]",
        "    Create or modify a user with the specified properties.",
        "USERS",
        "    List all usernames.",
        "WHOAMI",
        "    Return the current connection username.",
        "HELP",
        "    Print this help.",
    ];
    Response::Array(
        help.into_iter()
            .map(|s| Response::bulk(Bytes::from(s)))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ClusterDeps;
    use crate::cursor_store::AggregateCursorStore;
    use frogdb_core::persistence::NoopSnapshotCoordinator;
    use frogdb_core::{
        AclManager, ClientRegistry, CommandLatencyHistograms, CommandRegistry, KeyspaceStats,
        SharedHotkeySession, new_shared_hotkey_session,
    };
    use frogdb_protocol::ProtocolVersion;

    /// Build a `ConnCtx` over fixture dependencies — no socket, no
    /// `ConnectionHandler`. ACL exercises `acl_manager`, `command_registry`, and
    /// `username`; the rest are unused placeholders.
    struct Fixture {
        acl_manager: std::sync::Arc<AclManager>,
        command_registry: CommandRegistry,
        client_registry: ClientRegistry,
        latency_histograms: CommandLatencyHistograms,
        keyspace_stats: KeyspaceStats,
        config_manager: crate::runtime_config::ConfigManager,
        snapshot_coordinator: NoopSnapshotCoordinator,
        hotkey_session: SharedHotkeySession,
        cluster: ClusterDeps,
        cursor_store: AggregateCursorStore,
        username: String,
        metrics_recorder: frogdb_core::NoopMetricsRecorder,
        memory_diag: crate::connection::observability_conn_command::MemoryDiag,
    }

    impl Fixture {
        fn new() -> Self {
            let mut command_registry = CommandRegistry::new();
            crate::register_commands(&mut command_registry);
            Self {
                acl_manager: AclManager::new(Default::default()),
                command_registry,
                client_registry: ClientRegistry::new(),
                latency_histograms: CommandLatencyHistograms::new(true),
                keyspace_stats: KeyspaceStats::new(),
                config_manager: crate::runtime_config::ConfigManager::new(
                    &crate::config::Config::default(),
                ),
                snapshot_coordinator: NoopSnapshotCoordinator::new(),
                hotkey_session: new_shared_hotkey_session(),
                cluster: ClusterDeps::standalone(),
                cursor_store: AggregateCursorStore::new(),
                username: "default".to_string(),
                metrics_recorder: frogdb_core::NoopMetricsRecorder::new(),
                memory_diag: crate::connection::observability_conn_command::MemoryDiag(
                    frogdb_debug::MemoryDiagConfig::default(),
                ),
            }
        }

        fn ctx(&self) -> ConnCtx<'_> {
            ConnCtx {
                config: &self.config_manager,
                client_registry: &self.client_registry,
                latency_histograms: &self.latency_histograms,
                keyspace_stats: &self.keyspace_stats,
                shard_senders: &[],
                snapshot_coordinator: &self.snapshot_coordinator,
                hotkey_session: &self.hotkey_session,
                hotkey_cluster: &self.cluster,
                protocol_version: ProtocolVersion::Resp2,
                cursor_store: &self.cursor_store,
                acl_manager: self.acl_manager.as_ref(),
                command_registry: &self.command_registry,
                username: &self.username,
                metrics_recorder: &self.metrics_recorder,
                memory_diag: &self.memory_diag,
                num_shards: 0,
                max_clients: 10000,
                info: &frogdb_core::NoopInfoProvider,
                conn_state: None,
            }
        }
    }

    fn arg(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[tokio::test]
    async fn acl_empty_args_errors() {
        let fx = Fixture::new();
        let resp = AclConnCommand.execute(&mut fx.ctx(), &[]).await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn acl_whoami_returns_username() {
        let fx = Fixture::new();
        let resp = AclConnCommand
            .execute(&mut fx.ctx(), &[arg("WHOAMI")])
            .await;
        assert_eq!(resp, Response::bulk(Bytes::from_static(b"default")));
    }

    #[tokio::test]
    async fn acl_setuser_then_getuser_and_users() {
        let fx = Fixture::new();
        let resp = AclConnCommand
            .execute(
                &mut fx.ctx(),
                &[
                    arg("SETUSER"),
                    arg("alice"),
                    arg("on"),
                    arg("+@all"),
                    arg("~*"),
                ],
            )
            .await;
        assert_eq!(resp, Response::ok());

        let resp = AclConnCommand
            .execute(&mut fx.ctx(), &[arg("GETUSER"), arg("alice")])
            .await;
        assert!(matches!(resp, Response::Array(_)));

        let resp = AclConnCommand.execute(&mut fx.ctx(), &[arg("USERS")]).await;
        match resp {
            Response::Array(items) => assert!(
                items
                    .iter()
                    .any(|r| *r == Response::bulk(Bytes::from_static(b"alice"))),
                "USERS should include the created user"
            ),
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn acl_deluser_removes_user() {
        let fx = Fixture::new();
        let _ = AclConnCommand
            .execute(&mut fx.ctx(), &[arg("SETUSER"), arg("bob"), arg("on")])
            .await;
        let resp = AclConnCommand
            .execute(&mut fx.ctx(), &[arg("DELUSER"), arg("bob")])
            .await;
        assert_eq!(resp, Response::Integer(1));
    }

    #[tokio::test]
    async fn acl_cat_lists_categories_and_category_commands() {
        let fx = Fixture::new();
        let resp = AclConnCommand.execute(&mut fx.ctx(), &[arg("CAT")]).await;
        assert!(matches!(resp, Response::Array(_)));

        let resp = AclConnCommand
            .execute(&mut fx.ctx(), &[arg("CAT"), arg("nonsense-category")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn acl_genpass_default_and_invalid() {
        let fx = Fixture::new();
        let resp = AclConnCommand
            .execute(&mut fx.ctx(), &[arg("GENPASS")])
            .await;
        match resp {
            // 256 bits -> 64 hex chars.
            Response::Bulk(Some(b)) => assert_eq!(b.len(), 64),
            other => panic!("expected bulk password, got {other:?}"),
        }

        let resp = AclConnCommand
            .execute(&mut fx.ctx(), &[arg("GENPASS"), arg("-1")])
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn acl_dryrun_unknown_user_errors() {
        let fx = Fixture::new();
        let resp = AclConnCommand
            .execute(
                &mut fx.ctx(),
                &[arg("DRYRUN"), arg("ghost"), arg("GET"), arg("k")],
            )
            .await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[tokio::test]
    async fn acl_dryrun_denied_command_reports_permission() {
        let fx = Fixture::new();
        // A user allowed only GET; DRYRUN a SET should report a command-permission
        // denial as a bulk string (not an error).
        let _ = AclConnCommand
            .execute(
                &mut fx.ctx(),
                &[
                    arg("SETUSER"),
                    arg("carol"),
                    arg("on"),
                    arg("+get"),
                    arg("~*"),
                ],
            )
            .await;
        let resp = AclConnCommand
            .execute(
                &mut fx.ctx(),
                &[arg("DRYRUN"), arg("carol"), arg("SET"), arg("k"), arg("v")],
            )
            .await;
        match resp {
            Response::Bulk(Some(b)) => {
                let s = String::from_utf8_lossy(&b);
                assert!(s.contains("no permissions to run"), "got: {s}");
            }
            other => panic!("expected bulk denial, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn acl_help_rejects_extra_args() {
        let fx = Fixture::new();
        let resp = AclConnCommand
            .execute(&mut fx.ctx(), &[arg("HELP"), arg("extra")])
            .await;
        assert!(matches!(resp, Response::Error(_)));

        let resp = AclConnCommand.execute(&mut fx.ctx(), &[arg("HELP")]).await;
        assert!(matches!(resp, Response::Array(_)));
    }

    #[tokio::test]
    async fn acl_log_and_reset() {
        let fx = Fixture::new();
        let resp = AclConnCommand.execute(&mut fx.ctx(), &[arg("LOG")]).await;
        assert!(matches!(resp, Response::Array(_)));

        let resp = AclConnCommand
            .execute(&mut fx.ctx(), &[arg("LOG"), arg("RESET")])
            .await;
        assert_eq!(resp, Response::ok());
    }

    #[tokio::test]
    async fn acl_unknown_subcommand_errors() {
        let fx = Fixture::new();
        let resp = AclConnCommand.execute(&mut fx.ctx(), &[arg("NOPE")]).await;
        assert!(matches!(resp, Response::Error(_)));
    }

    #[test]
    fn acl_spec_is_connection_level_and_valid() {
        assert!(ACL_CONN_COMMAND.spec().validate().is_ok());
        assert!(matches!(
            ACL_CONN_COMMAND.spec().strategy,
            ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
        ));
    }
}
