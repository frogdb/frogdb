//! Authentication and ACL command handlers.
//!
//! This module handles authentication-related commands as extension methods
//! on `ConnectionHandler`:
//! - AUTH - Authenticate with username/password
//! - HELLO - Protocol negotiation and optional authentication
//! - ACL - Access Control List management
//! - RESET - Reset connection state

use bytes::Bytes;
use frogdb_core::CommandCategory;
use frogdb_protocol::{ProtocolVersion, Response};
use tracing::{info, warn};

use crate::connection::state::{AuthState, TransactionState};
use crate::connection::{ConnectionHandler, ShardMessage};

impl ConnectionHandler {
    /// Get client info string for ACL logging.
    pub(crate) fn client_info_string(&self) -> String {
        format!(
            "id={} addr={} name={}",
            self.state.id,
            self.state.addr,
            self.state.name.as_ref().map(|b| String::from_utf8_lossy(b)).unwrap_or_default()
        )
    }

    /// Handle AUTH command.
    pub(crate) async fn handle_auth(&mut self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'auth' command");
        }

        let client_info = self.client_info_string();

        let result = if args.len() == 1 {
            // AUTH <password> - authenticate as default user
            let password = String::from_utf8_lossy(&args[0]);
            self.acl_manager.authenticate_default(&password, &client_info)
        } else {
            // AUTH <username> <password>
            let username = String::from_utf8_lossy(&args[0]);
            let password = String::from_utf8_lossy(&args[1]);
            self.acl_manager.authenticate(&username, &password, &client_info)
        };

        match result {
            Ok(user) => {
                info!(conn_id = self.state.id, username = %user.username, "Client authenticated");
                self.state.auth = AuthState::Authenticated(user);
                Response::ok()
            }
            Err(e) => {
                let username = if args.len() > 1 {
                    String::from_utf8_lossy(&args[0]).to_string()
                } else {
                    "default".to_string()
                };
                warn!(
                    conn_id = self.state.id,
                    addr = %self.state.addr,
                    username = %username,
                    reason = %e,
                    "Authentication failed"
                );
                Response::error(e.to_string())
            }
        }
    }

    /// Handle HELLO command for protocol negotiation.
    ///
    /// Format: HELLO [protover [AUTH username password] [SETNAME clientname]]
    pub(crate) async fn handle_hello(&mut self, args: &[Bytes]) -> Response {
        let mut requested_version: Option<u32> = None;
        let mut auth_username: Option<&Bytes> = None;
        let mut auth_password: Option<&Bytes> = None;
        let mut setname: Option<&Bytes> = None;

        // Parse arguments
        let mut i = 0;
        while i < args.len() {
            if i == 0 {
                // First argument is protocol version
                match std::str::from_utf8(&args[i]).ok().and_then(|s| s.parse::<u32>().ok()) {
                    Some(v) => requested_version = Some(v),
                    None => return Response::error("ERR Protocol version is not an integer or out of range"),
                }
            } else {
                // Check for AUTH or SETNAME
                let arg = args[i].to_ascii_uppercase();
                if arg == b"AUTH".as_slice() {
                    // AUTH username password
                    if i + 2 >= args.len() {
                        return Response::error("ERR Syntax error in HELLO option 'AUTH'");
                    }
                    auth_username = Some(&args[i + 1]);
                    auth_password = Some(&args[i + 2]);
                    i += 2;
                } else if arg == b"SETNAME".as_slice() {
                    // SETNAME clientname
                    if i + 1 >= args.len() {
                        return Response::error("ERR Syntax error in HELLO option 'SETNAME'");
                    }
                    setname = Some(&args[i + 1]);
                    i += 1;
                } else {
                    return Response::error(format!("ERR Syntax error in HELLO option '{}'", String::from_utf8_lossy(&args[i])));
                }
            }
            i += 1;
        }

        // Handle protocol version
        if let Some(version) = requested_version {
            if !(2..=3).contains(&version) {
                // Return NOPROTO error for unsupported versions
                return Response::error("NOPROTO sorry, this protocol version is not supported");
            }

            let new_version = if version == 3 {
                ProtocolVersion::Resp3
            } else {
                ProtocolVersion::Resp2
            };

            // Check for downgrade after HELLO 3
            if self.state.hello_received && self.state.protocol_version.is_resp3() && !new_version.is_resp3() {
                return Response::error("ERR protocol downgrade from RESP3 to RESP2 is not allowed");
            }

            self.state.protocol_version = new_version;
        }

        // Handle AUTH
        if let (Some(username), Some(password)) = (auth_username, auth_password) {
            let client_info = self.client_info_string();
            let username_str = String::from_utf8_lossy(username);
            let password_str = String::from_utf8_lossy(password);

            match self.acl_manager.authenticate(&username_str, &password_str, &client_info) {
                Ok(user) => {
                    info!(conn_id = self.state.id, username = %user.username, "Client authenticated");
                    self.state.auth = AuthState::Authenticated(user);
                }
                Err(_) => {
                    warn!(
                        conn_id = self.state.id,
                        addr = %self.state.addr,
                        username = %username_str,
                        reason = "invalid username-password pair or user is disabled",
                        "Authentication failed"
                    );
                    return Response::error("WRONGPASS invalid username-password pair or user is disabled");
                }
            }
        }

        // Handle SETNAME
        if let Some(name) = setname {
            if name.is_empty() {
                self.state.name = None;
                self.client_registry.update_name(self.state.id, None);
            } else {
                self.state.name = Some(name.clone());
                self.client_registry.update_name(self.state.id, Some(name.clone()));
            }
        }

        // Mark HELLO as received
        self.state.hello_received = true;
        self.state.hello_at = Some(std::time::Instant::now());

        // Build server info response
        self.build_hello_response()
    }

    /// Build the HELLO response with server info.
    pub(crate) fn build_hello_response(&self) -> Response {
        // Server info fields
        let server = Response::bulk(Bytes::from_static(b"server"));
        let server_val = Response::bulk(Bytes::from_static(b"frogdb"));

        let version = Response::bulk(Bytes::from_static(b"version"));
        let version_val = Response::bulk(Bytes::from(env!("CARGO_PKG_VERSION")));

        let proto = Response::bulk(Bytes::from_static(b"proto"));
        let proto_val = Response::Integer(if self.state.protocol_version.is_resp3() { 3 } else { 2 });

        let id = Response::bulk(Bytes::from_static(b"id"));
        let id_val = Response::Integer(self.state.id as i64);

        let mode = Response::bulk(Bytes::from_static(b"mode"));
        let mode_val = Response::bulk(Bytes::from_static(b"standalone"));

        let role = Response::bulk(Bytes::from_static(b"role"));
        let role_val = Response::bulk(Bytes::from_static(b"master"));

        let modules = Response::bulk(Bytes::from_static(b"modules"));
        let modules_val = Response::Array(vec![]);

        if self.state.protocol_version.is_resp3() {
            // Return as Map for RESP3
            Response::Map(vec![
                (server, server_val),
                (version, version_val),
                (proto, proto_val),
                (id, id_val),
                (mode, mode_val),
                (role, role_val),
                (modules, modules_val),
            ])
        } else {
            // Return as flat array for RESP2
            Response::Array(vec![
                server, server_val,
                version, version_val,
                proto, proto_val,
                id, id_val,
                mode, mode_val,
                role, role_val,
                modules, modules_val,
            ])
        }
    }

    /// Handle ACL command and subcommands.
    pub(crate) async fn handle_acl_command(&mut self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'acl' command");
        }

        let subcmd = String::from_utf8_lossy(&args[0]).to_uppercase();
        let subcmd_args = &args[1..];

        match subcmd.as_str() {
            "WHOAMI" => self.handle_acl_whoami(),
            "LIST" => self.handle_acl_list(),
            "USERS" => self.handle_acl_users(),
            "GETUSER" => self.handle_acl_getuser(subcmd_args),
            "SETUSER" => self.handle_acl_setuser(subcmd_args),
            "DELUSER" => self.handle_acl_deluser(subcmd_args),
            "CAT" => self.handle_acl_cat(subcmd_args),
            "GENPASS" => self.handle_acl_genpass(subcmd_args),
            "LOG" => self.handle_acl_log(subcmd_args),
            "SAVE" => self.handle_acl_save(),
            "LOAD" => self.handle_acl_load(),
            "HELP" => self.handle_acl_help(),
            _ => Response::error(format!("ERR Unknown subcommand or wrong number of arguments for '{}'", subcmd)),
        }
    }

    /// ACL WHOAMI - return current username.
    fn handle_acl_whoami(&self) -> Response {
        Response::bulk(Bytes::from(self.state.auth.username().to_string()))
    }

    /// ACL LIST - list all users with their ACL rules.
    fn handle_acl_list(&self) -> Response {
        let users = self.acl_manager.list_users_detailed();
        Response::Array(users.into_iter().map(|s| Response::bulk(Bytes::from(s))).collect())
    }

    /// ACL USERS - list all usernames.
    fn handle_acl_users(&self) -> Response {
        let users = self.acl_manager.list_users();
        Response::Array(users.into_iter().map(|s| Response::bulk(Bytes::from(s))).collect())
    }

    /// ACL GETUSER <username> - get user info.
    fn handle_acl_getuser(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'acl|getuser' command");
        }

        let username = String::from_utf8_lossy(&args[0]);
        match self.acl_manager.get_user(&username) {
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
    fn handle_acl_setuser(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'acl|setuser' command");
        }

        let username = String::from_utf8_lossy(&args[0]);
        let rules: Vec<&str> = args[1..]
            .iter()
            .map(|b| std::str::from_utf8(b).unwrap_or(""))
            .collect();

        match self.acl_manager.set_user(&username, &rules) {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e.to_string()),
        }
    }

    /// ACL DELUSER <username> [...] - delete users.
    fn handle_acl_deluser(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'acl|deluser' command");
        }

        let usernames: Vec<&str> = args
            .iter()
            .map(|b| std::str::from_utf8(b).unwrap_or(""))
            .collect();

        match self.acl_manager.delete_users(&usernames) {
            Ok(count) => Response::Integer(count as i64),
            Err(e) => Response::error(e.to_string()),
        }
    }

    /// ACL CAT [category] - list categories or commands in category.
    fn handle_acl_cat(&self, args: &[Bytes]) -> Response {
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

    /// ACL GENPASS [bits] - generate secure random password.
    fn handle_acl_genpass(&self, args: &[Bytes]) -> Response {
        let bits = if args.is_empty() {
            256
        } else {
            match String::from_utf8_lossy(&args[0]).parse::<u32>() {
                Ok(b) if b > 0 => b,
                _ => return Response::error("ERR ACL GENPASS argument must be a positive integer"),
            }
        };

        let password = frogdb_core::generate_password(bits);
        Response::bulk(Bytes::from(password))
    }

    /// ACL LOG [count|RESET] - view or reset security log.
    fn handle_acl_log(&self, args: &[Bytes]) -> Response {
        if !args.is_empty() {
            let arg = String::from_utf8_lossy(&args[0]).to_uppercase();
            if arg == "RESET" {
                self.acl_manager.log().reset();
                return Response::ok();
            }
        }

        let count = if args.is_empty() {
            None
        } else {
            String::from_utf8_lossy(&args[0]).parse::<usize>().ok()
        };

        let entries = self.acl_manager.log().get(count);
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
    fn handle_acl_save(&self) -> Response {
        match self.acl_manager.save() {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e.to_string()),
        }
    }

    /// ACL LOAD - load ACL from file.
    fn handle_acl_load(&self) -> Response {
        match self.acl_manager.load() {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e.to_string()),
        }
    }

    /// ACL HELP - show help.
    fn handle_acl_help(&self) -> Response {
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
        Response::Array(help.into_iter().map(|s| Response::bulk(Bytes::from(s))).collect())
    }

    /// Handle RESET command - reset connection to initial state.
    /// This exits pub/sub mode, clears transaction state, and resets protocol to RESP2.
    pub(crate) async fn handle_reset(&mut self) -> Response {
        // 1. Exit pub/sub mode - unsubscribe from all channels
        if self.state.pubsub.in_pubsub_mode() {
            // Clear local subscription tracking
            self.state.pubsub.subscriptions.clear();
            self.state.pubsub.patterns.clear();
            self.state.pubsub.sharded_subscriptions.clear();

            // Notify all shards to remove this connection's subscriptions
            for sender in self.shard_senders.iter() {
                let _ = sender.send(ShardMessage::ConnectionClosed {
                    conn_id: self.state.id,
                }).await;
            }
        }

        // 2. Clear transaction state (abort any MULTI in progress)
        self.state.transaction = TransactionState::default();

        // 3. Reset protocol to RESP2 (per Redis behavior)
        self.state.protocol_version = ProtocolVersion::Resp2;

        // 4. Clear client name
        self.state.name = None;

        // Return RESET acknowledgment
        Response::Simple(Bytes::from_static(b"RESET"))
    }
}
