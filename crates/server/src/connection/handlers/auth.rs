//! Authentication handlers.
//!
//! This module handles authentication-related commands:
//! - AUTH - Authenticate with username/password
//! - HELLO - Protocol negotiation and optional authentication
//! - ACL - Access Control List management
//!
//! These commands manage the authentication state of the connection and
//! interact with the [`AclManager`].

use std::sync::Arc;

use bytes::Bytes;
use frogdb_core::{AclManager, AuthenticatedUser, CommandCategory};
use frogdb_protocol::{ProtocolVersion, Response};

/// Handler for authentication commands.
#[derive(Clone)]
pub struct AuthHandler {
    /// ACL manager for user authentication.
    acl_manager: Arc<AclManager>,
}

impl AuthHandler {
    /// Create a new auth handler.
    pub fn new(acl_manager: Arc<AclManager>) -> Self {
        Self { acl_manager }
    }

    /// Handle the AUTH command.
    ///
    /// AUTH [username] password
    ///
    /// # Arguments
    ///
    /// * `args` - Command arguments (password or username + password)
    /// * `client_info` - Client address for logging
    ///
    /// # Returns
    ///
    /// Returns the authenticated user on success, or an error response.
    pub fn handle_auth(
        &self,
        args: &[Bytes],
        client_info: &str,
    ) -> Result<AuthenticatedUser, Response> {
        if args.is_empty() || args.len() > 2 {
            return Err(Response::error(
                "ERR wrong number of arguments for 'auth' command",
            ));
        }

        let (username, password) = if args.len() == 1 {
            // Legacy: AUTH password (uses "default" user)
            ("default", String::from_utf8_lossy(&args[0]).to_string())
        } else {
            // ACL: AUTH username password
            (
                std::str::from_utf8(&args[0]).unwrap_or("default"),
                String::from_utf8_lossy(&args[1]).to_string(),
            )
        };

        match self.acl_manager.authenticate(username, &password, client_info) {
            Ok(user) => Ok(user),
            Err(e) => Err(Response::error(format!("WRONGPASS {}", e))),
        }
    }

    /// Handle the HELLO command.
    ///
    /// HELLO [protover [AUTH username password] [SETNAME clientname]]
    ///
    /// # Arguments
    ///
    /// * `args` - Command arguments
    /// * `client_info` - Client address for logging
    /// * `current_protocol` - Current protocol version
    /// * `current_user` - Currently authenticated user (if any)
    ///
    /// # Returns
    ///
    /// Returns (new protocol version, optional new user, optional client name, response).
    pub fn handle_hello(
        &self,
        args: &[Bytes],
        client_info: &str,
        current_protocol: ProtocolVersion,
        current_user: Option<&AuthenticatedUser>,
    ) -> HelloResult {
        let mut protocol_version = current_protocol;
        let mut new_user: Option<AuthenticatedUser> = None;
        let mut client_name: Option<String> = None;

        let mut i = 0;

        // Parse protocol version
        if !args.is_empty() {
            if let Ok(ver_str) = std::str::from_utf8(&args[0]) {
                match ver_str {
                    "2" => protocol_version = ProtocolVersion::Resp2,
                    "3" => protocol_version = ProtocolVersion::Resp3,
                    _ => {
                        return HelloResult::Error(Response::error(
                            "NOPROTO unsupported protocol version",
                        ));
                    }
                }
                i = 1;
            }
        }

        // Parse optional arguments
        while i < args.len() {
            let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
            match arg.as_str() {
                "AUTH" => {
                    if i + 2 >= args.len() {
                        return HelloResult::Error(Response::error(
                            "ERR AUTH requires username and password",
                        ));
                    }
                    let username = String::from_utf8_lossy(&args[i + 1]);
                    let password = String::from_utf8_lossy(&args[i + 2]);

                    match self.acl_manager.authenticate(&username, &password, client_info) {
                        Ok(user) => new_user = Some(user),
                        Err(e) => {
                            return HelloResult::Error(Response::error(format!("WRONGPASS {}", e)));
                        }
                    }
                    i += 3;
                }
                "SETNAME" => {
                    if i + 1 >= args.len() {
                        return HelloResult::Error(Response::error(
                            "ERR SETNAME requires a name argument",
                        ));
                    }
                    client_name = Some(String::from_utf8_lossy(&args[i + 1]).to_string());
                    i += 2;
                }
                _ => {
                    return HelloResult::Error(Response::error(format!(
                        "ERR unknown HELLO option '{}'",
                        arg
                    )));
                }
            }
        }

        // Build response
        let response = Self::build_hello_response(
            protocol_version,
            new_user.as_ref().or(current_user),
        );

        HelloResult::Success {
            protocol_version,
            new_user,
            client_name,
            response,
        }
    }

    /// Build the HELLO response map.
    fn build_hello_response(
        protocol_version: ProtocolVersion,
        user: Option<&AuthenticatedUser>,
    ) -> Response {
        let proto_num = match protocol_version {
            ProtocolVersion::Resp2 => 2,
            ProtocolVersion::Resp3 => 3,
        };

        // Build response as key-value pairs for RESP3 Map or flat array for RESP2
        let mut pairs = vec![
            (Response::bulk("server"), Response::bulk("frogdb")),
            (Response::bulk("version"), Response::bulk(env!("CARGO_PKG_VERSION"))),
            (Response::bulk("proto"), Response::Integer(proto_num)),
            (Response::bulk("id"), Response::Integer(0)), // Connection ID - caller should fill in
            (Response::bulk("mode"), Response::bulk("standalone")),
            (Response::bulk("role"), Response::bulk("master")),
            (Response::bulk("modules"), Response::Array(vec![])),
        ];

        // Add user info if authenticated
        if let Some(u) = user {
            pairs.push((Response::bulk("user"), Response::bulk(Bytes::from(u.username.to_string()))));
        }

        if matches!(protocol_version, ProtocolVersion::Resp3) {
            Response::Map(pairs)
        } else {
            // Flatten for RESP2
            let mut items = Vec::with_capacity(pairs.len() * 2);
            for (k, v) in pairs {
                items.push(k);
                items.push(v);
            }
            Response::Array(items)
        }
    }

    // ACL subcommand handlers

    /// Handle ACL WHOAMI.
    pub fn handle_acl_whoami(&self, user: Option<&AuthenticatedUser>) -> Response {
        match user {
            Some(u) => Response::bulk(Bytes::from(u.username.to_string())),
            None => Response::bulk("default"),
        }
    }

    /// Handle ACL LIST.
    pub fn handle_acl_list(&self) -> Response {
        let acl_strings = self.acl_manager.list_users_detailed();
        Response::Array(acl_strings.into_iter().map(|s| Response::bulk(Bytes::from(s))).collect())
    }

    /// Handle ACL USERS.
    pub fn handle_acl_users(&self) -> Response {
        let users = self.acl_manager.list_users();
        Response::Array(users.into_iter().map(|s| Response::bulk(Bytes::from(s))).collect())
    }

    /// Handle ACL GETUSER.
    pub fn handle_acl_getuser(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'acl getuser' command");
        }

        let username = String::from_utf8_lossy(&args[0]);
        match self.acl_manager.get_user(&username) {
            Some(user) => {
                // Simplified response - just return the ACL string
                let acl_str = user.to_acl_string();
                Response::bulk(Bytes::from(acl_str))
            }
            None => Response::Null,
        }
    }

    /// Handle ACL SETUSER.
    pub fn handle_acl_setuser(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'acl setuser' command");
        }

        let username = String::from_utf8_lossy(&args[0]);
        let rules: Vec<&str> = args[1..]
            .iter()
            .map(|b| std::str::from_utf8(b).unwrap_or(""))
            .collect();

        match self.acl_manager.set_user(&username, &rules) {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(format!("ERR {}", e)),
        }
    }

    /// Handle ACL DELUSER.
    pub fn handle_acl_deluser(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'acl deluser' command");
        }

        let usernames: Vec<&str> = args
            .iter()
            .map(|b| std::str::from_utf8(b).unwrap_or(""))
            .collect();

        match self.acl_manager.delete_users(&usernames) {
            Ok(count) => Response::Integer(count as i64),
            Err(e) => Response::error(format!("ERR {}", e)),
        }
    }

    /// Handle ACL SAVE.
    pub fn handle_acl_save(&self) -> Response {
        match self.acl_manager.save() {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(format!("ERR {}", e)),
        }
    }

    /// Handle ACL LOAD.
    pub fn handle_acl_load(&self) -> Response {
        match self.acl_manager.load() {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(format!("ERR {}", e)),
        }
    }

    /// Handle ACL CAT.
    pub fn handle_acl_cat(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            // List all categories
            let categories: Vec<Response> = CommandCategory::all()
                .iter()
                .map(|cat| Response::bulk(cat.name()))
                .collect();
            Response::Array(categories)
        } else {
            let category_name = String::from_utf8_lossy(&args[0]).to_lowercase();
            match CommandCategory::parse(&category_name) {
                Some(cat) => {
                    let commands: Vec<Response> = cat.commands()
                        .into_iter()
                        .map(|cmd| Response::bulk(cmd))
                        .collect();
                    Response::Array(commands)
                }
                None => Response::error(format!("ERR Unknown category '{}'\n", category_name)),
            }
        }
    }

    /// Handle ACL GENPASS.
    pub fn handle_acl_genpass(&self, args: &[Bytes]) -> Response {
        let bits: u32 = if args.is_empty() {
            256
        } else {
            match frogdb_core::parse_u64(&args[0]) {
                Ok(b) => b.min(4096) as u32,
                Err(_) => return Response::error("ERR bits must be a positive integer"),
            }
        };

        let password = frogdb_core::generate_password(bits / 4); // 4 bits per hex char
        Response::bulk(Bytes::from(password))
    }

    /// Handle ACL LOG.
    pub fn handle_acl_log(&self, args: &[Bytes]) -> Response {
        if !args.is_empty() {
            let subcommand = String::from_utf8_lossy(&args[0]).to_uppercase();
            if subcommand == "RESET" {
                self.acl_manager.log().reset();
                return Response::ok();
            }

            // Might be a count
            if let Ok(count) = frogdb_core::parse_usize(&args[0]) {
                let entries = self.acl_manager.log().get(Some(count));
                return Self::log_entries_to_response(entries);
            }
        }

        // Default: get 10 entries
        let entries = self.acl_manager.log().get(None);
        Self::log_entries_to_response(entries)
    }

    /// Handle ACL HELP.
    pub fn handle_acl_help(&self) -> Response {
        let help = vec![
            "ACL CAT [<category>]",
            "    List all ACL categories or commands in a category.",
            "ACL DELUSER <username> [<username> ...]",
            "    Delete ACL users.",
            "ACL GENPASS [<bits>]",
            "    Generate a random password.",
            "ACL GETUSER <username>",
            "    Get a user's ACL configuration.",
            "ACL LIST",
            "    List all users with their ACL configuration.",
            "ACL LOAD",
            "    Reload ACL from the configured ACL file.",
            "ACL LOG [<count> | RESET]",
            "    Show or reset the ACL log.",
            "ACL SAVE",
            "    Save the current ACL to the configured ACL file.",
            "ACL SETUSER <username> [<rules> ...]",
            "    Create or modify a user.",
            "ACL USERS",
            "    List all usernames.",
            "ACL WHOAMI",
            "    Get the current connection's username.",
            "ACL HELP",
            "    Show this help.",
        ];
        Response::Array(help.into_iter().map(|s| Response::bulk(s)).collect())
    }

    /// Convert ACL log entries to a RESP response.
    fn log_entries_to_response(entries: Vec<frogdb_core::acl::AclLogEntry>) -> Response {
        Response::Array(
            entries
                .into_iter()
                .map(|entry| {
                    let fields = entry.to_resp_fields();
                    Response::Array(
                        fields
                            .into_iter()
                            .flat_map(|(k, v)| {
                                use frogdb_core::acl::AclLogValue;
                                vec![
                                    Response::bulk(k),
                                    match v {
                                        AclLogValue::String(s) => Response::bulk(Bytes::from(s)),
                                        AclLogValue::Integer(i) => Response::Integer(i),
                                        AclLogValue::Float(f) => {
                                            Response::bulk(Bytes::from(format!("{:.6}", f)))
                                        }
                                    },
                                ]
                            })
                            .collect(),
                    )
                })
                .collect(),
        )
    }
}

/// Result of HELLO command processing.
#[derive(Debug)]
pub enum HelloResult {
    /// Successfully processed HELLO.
    Success {
        /// New protocol version.
        protocol_version: ProtocolVersion,
        /// New authenticated user (if AUTH was included).
        new_user: Option<AuthenticatedUser>,
        /// New client name (if SETNAME was included).
        client_name: Option<String>,
        /// Response to send to client.
        response: Response,
    },
    /// Error processing HELLO.
    Error(Response),
}
