//! Authentication command handlers.
//!
//! This module handles authentication-related commands as extension methods
//! on `ConnectionHandler`:
//! - AUTH - Authenticate with username/password
//! - HELLO - Protocol negotiation and optional authentication
//! - RESET - Reset connection state

use bytes::Bytes;
use frogdb_protocol::{MapReply, ProtocolVersion, Response};
use tracing::{info, warn};

use crate::connection::{ConnectionHandler, ShardMessage};

impl ConnectionHandler {
    /// Get client info string for ACL logging.
    pub(crate) fn client_info_string(&self) -> String {
        format!(
            "id={} addr={} name={}",
            self.state.id,
            self.state.addr,
            self.state
                .name
                .as_ref()
                .map(|b| String::from_utf8_lossy(b))
                .unwrap_or_default()
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
            self.core
                .acl_manager
                .authenticate_default(&password, &client_info)
        } else {
            // AUTH <username> <password>
            let username = String::from_utf8_lossy(&args[0]);
            let password = String::from_utf8_lossy(&args[1]);
            self.core
                .acl_manager
                .authenticate(&username, &password, &client_info)
        };

        match result {
            Ok(user) => {
                info!(conn_id = self.state.id, username = %user.username, "Client authenticated");
                self.state.authenticate(user);
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
                match std::str::from_utf8(&args[i])
                    .ok()
                    .and_then(|s| s.parse::<u32>().ok())
                {
                    Some(v) => requested_version = Some(v),
                    None => {
                        return Response::error(
                            "ERR Protocol version is not an integer or out of range",
                        );
                    }
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
                    return Response::error(format!(
                        "ERR Syntax error in HELLO option '{}'",
                        String::from_utf8_lossy(&args[i])
                    ));
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

            self.state.protocol_version = new_version;
        }

        // Handle AUTH
        if let (Some(username), Some(password)) = (auth_username, auth_password) {
            let client_info = self.client_info_string();
            let username_str = String::from_utf8_lossy(username);
            let password_str = String::from_utf8_lossy(password);

            match self
                .core
                .acl_manager
                .authenticate(&username_str, &password_str, &client_info)
            {
                Ok(user) => {
                    info!(conn_id = self.state.id, username = %user.username, "Client authenticated");
                    self.state.authenticate(user);
                }
                Err(_) => {
                    warn!(
                        conn_id = self.state.id,
                        addr = %self.state.addr,
                        username = %username_str,
                        reason = "invalid username-password pair or user is disabled",
                        "Authentication failed"
                    );
                    return Response::error(
                        "WRONGPASS invalid username-password pair or user is disabled",
                    );
                }
            }
        }

        // Handle SETNAME
        if let Some(name) = setname {
            if name.is_empty() {
                self.state.name = None;
                self.admin.client_registry.update_name(self.state.id, None);
            } else {
                self.state.name = Some(name.clone());
                self.admin
                    .client_registry
                    .update_name(self.state.id, Some(name.clone()));
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
        // The RESP3 Map vs RESP2 flat-Array shape is owned by MapReply; list the
        // fields once and let the seam pick the shape.
        let proto = if self.state.protocol_version.is_resp3() {
            3
        } else {
            2
        };

        let mut reply = MapReply::with_capacity(self.state.protocol_version, 7);
        reply.field(b"server", Response::bulk(Bytes::from_static(b"frogdb")));
        reply.field(
            b"version",
            Response::bulk(Bytes::from(env!("CARGO_PKG_VERSION"))),
        );
        reply.field(b"proto", Response::Integer(proto));
        reply.field(b"id", Response::Integer(self.state.id as i64));
        reply.field(b"mode", Response::bulk(Bytes::from_static(b"standalone")));
        reply.field(b"role", Response::bulk(Bytes::from_static(b"master")));
        reply.field(b"modules", Response::Array(vec![]));
        reply.finish()
    }

    /// Handle RESET command - reset connection to initial state.
    /// This exits pub/sub mode, clears transaction and tracking state,
    /// and resets protocol to RESP2.
    pub(crate) async fn handle_reset(&mut self) -> Response {
        // State half: exit pub/sub mode, clear tracking + transaction state,
        // reset the protocol to RESP2, and clear the client name. The returned
        // effects drive the I/O half below.
        let effects = self.state.reset();

        // 1. Notify shards to remove subscriptions and/or tracking state. The
        //    original code sent ConnectionClosed once if either was active.
        if effects.was_in_pubsub || effects.tracking_was_enabled {
            for sender in self.core.shard_senders.iter() {
                let _ = sender
                    .send(ShardMessage::ConnectionClosed {
                        conn_id: self.state.id,
                    })
                    .await;
            }
        }

        // 1.5. Tear down the tracking session's local plumbing (invalidation
        //      channels + redirect forwarder). Shard-side tracking state was
        //      removed by the ConnectionClosed fan-out above — its tracking
        //      half is identical to the TrackingUnregister that CLIENT
        //      TRACKING OFF sends. Idempotent, so no tracking_was_enabled gate.
        self.tracking_session_teardown_local();

        // 4. Exit MONITOR mode
        self.monitor_rx = None;

        // 5. Clear client name in the registry (local state cleared by reset()).
        self.admin.client_registry.update_name(self.state.id, None);

        // Note: lib_name/lib_ver are NOT cleared by RESET (per Redis semantics).
        // These persist across RESET so client libraries retain their identity.

        // Return RESET acknowledgment
        Response::Simple(Bytes::from_static(b"RESET"))
    }
}
