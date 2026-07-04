//! Unified ACL enforcement seam.
//!
//! [`PermissionGuard`] is the single place where the three parts of an ACL
//! decision live together: the permission *check* (delegated to
//! [`FullAclChecker`]), the *audit log* of a denial (delegated to
//! [`AclLog::log_*_denied`]), and the *`NOPERM` reply* (the canonical
//! [`AclError`] `Display` string). Every execution path — `run_pre_checks`
//! (command), `validate_channel_access` (channel), `route_and_execute` (key),
//! and the transaction queue (`queue_command`, key + channel) — constructs a
//! guard for the connection's current user and calls one method per check.
//!
//! Keeping check + log + reply in one location is what makes audit-log
//! completeness structural: a path either routes through the guard (and logs)
//! or it does not enforce at all. It previously lived inline at four call
//! sites that had already drifted (a queue path that skipped logging, a
//! `' '`-vs-`'|'` subcommand separator).

use bytes::Bytes;
use frogdb_core::{
    AclChecker, AclError, AclLog, AclManager, AuthenticatedUser, FullAclChecker, KeyAccessType,
    PermissionResult,
};
use frogdb_protocol::Response;
use tracing::warn;

use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Build an ACL [`PermissionGuard`] for the connection's current user.
    ///
    /// Returns `None` when the connection is unauthenticated, in which case ACL
    /// is not enforced (a no-password instance, or pre-AUTH on an exempt
    /// command). The `client-info` recorded for any denial is `ip:port`.
    pub(crate) fn permission_guard(&self) -> Option<PermissionGuard<'_>> {
        let user = self.state.authenticated_user()?;
        let client_info = format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
        Some(PermissionGuard::new(
            &self.core.acl_manager,
            user,
            client_info,
            self.state.id,
        ))
    }
}

/// ACL enforcement bound to a single connection's authenticated user.
///
/// Constructed via [`ConnectionHandler::permission_guard`](crate::connection::ConnectionHandler);
/// the connection passes `None` (no enforcement) when unauthenticated.
pub(crate) struct PermissionGuard<'a> {
    checker: FullAclChecker,
    user: &'a AuthenticatedUser,
    log: &'a AclLog,
    /// `ip:port` of the client, recorded as the ACL LOG `client-info` field.
    client_info: String,
    conn_id: u64,
}

impl<'a> PermissionGuard<'a> {
    /// Bind a guard to `user` for the connection identified by `conn_id`.
    pub(crate) fn new(
        manager: &'a AclManager,
        user: &'a AuthenticatedUser,
        client_info: String,
        conn_id: u64,
    ) -> Self {
        Self {
            checker: FullAclChecker::new(manager.requires_auth()),
            user,
            log: manager.log(),
            client_info,
            conn_id,
        }
    }

    /// Check command (and optional subcommand) permission. On denial the
    /// denial is logged to `ACL LOG` and the canonical `NOPERM` reply returned.
    #[allow(clippy::result_large_err)]
    pub(crate) fn check_command(
        &self,
        cmd_name: &str,
        subcommand: Option<&str>,
    ) -> Result<(), Response> {
        match self.checker.check_command(self.user, cmd_name, subcommand) {
            PermissionResult::Allowed => Ok(()),
            PermissionResult::Denied(err) => {
                // The AclError carries the lowercase fullname; use it for the log
                // object too so the reply and the audit line cannot diverge.
                let object = match &err {
                    AclError::NoPermissionSubcommand {
                        command,
                        subcommand,
                    } => format!("{command}|{subcommand}"),
                    AclError::NoPermissionCommand { command } => command.clone(),
                    _ => cmd_name.to_lowercase(),
                };
                self.log
                    .log_command_denied(&self.user.username, &self.client_info, &object);
                warn!(
                    conn_id = self.conn_id,
                    username = %self.user.username,
                    command = %object,
                    "ACL denied command"
                );
                Err(Self::error(err))
            }
        }
    }

    /// Check access to every key. The first denied key is logged to `ACL LOG`
    /// and its `NOPERM` reply returned; remaining keys are not checked.
    ///
    /// The command itself is validated upstream by `run_pre_checks`, so only key
    /// access is checked here.
    #[allow(clippy::result_large_err)]
    pub(crate) fn check_keys(&self, keys: &[&[u8]], access: KeyAccessType) -> Result<(), Response> {
        for key in keys {
            if let PermissionResult::Denied(err) =
                self.checker.check_key_access(self.user, key, access)
            {
                let key_str = String::from_utf8_lossy(key);
                self.log
                    .log_key_denied(&self.user.username, &self.client_info, &key_str);
                return Err(Self::error(err));
            }
        }
        Ok(())
    }

    /// Check access to every channel. The first denied channel is logged to
    /// `ACL LOG` and its `NOPERM` reply returned.
    #[allow(clippy::result_large_err)]
    pub(crate) fn check_channels(&self, channels: &[Bytes]) -> Result<(), Response> {
        for channel in channels {
            if let PermissionResult::Denied(err) =
                self.checker.check_channel_access(self.user, channel)
            {
                let channel_str = String::from_utf8_lossy(channel);
                self.log
                    .log_channel_denied(&self.user.username, &self.client_info, &channel_str);
                return Err(Self::error(err));
            }
        }
        Ok(())
    }

    /// Render an [`AclError`] as its canonical RESP error reply.
    fn error(err: AclError) -> Response {
        Response::error(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::{AclConfig, AclManager};
    use std::sync::Arc;

    const INFO: &str = "127.0.0.1:5555";

    fn manager_with_user(rules: &[&str]) -> (Arc<AclManager>, AuthenticatedUser) {
        let manager = AclManager::new(AclConfig::default());
        let mut full_rules = vec!["on", ">pass"];
        full_rules.extend_from_slice(rules);
        manager.set_user("u", &full_rules).expect("set_user");
        let user = manager
            .authenticate("u", "pass", INFO)
            .expect("authenticate");
        (manager, user)
    }

    fn err_msg(resp: &Response) -> String {
        match resp {
            Response::Error(e) => String::from_utf8_lossy(e).to_string(),
            other => panic!("expected error response, got {other:?}"),
        }
    }

    #[test]
    fn allows_permitted_command_key_and_channel() {
        let (manager, user) = manager_with_user(&["+@all", "~allowed:*", "&chat:*"]);
        let guard = PermissionGuard::new(&manager, &user, INFO.into(), 1);

        assert!(guard.check_command("GET", None).is_ok());
        assert!(
            guard
                .check_keys(&[b"allowed:x"], KeyAccessType::Read)
                .is_ok()
        );
        assert!(
            guard
                .check_channels(&[Bytes::from_static(b"chat:room")])
                .is_ok()
        );
        assert!(manager.log().is_empty(), "no denials => no ACL LOG entries");
    }

    #[test]
    fn denies_command_logs_and_formats() {
        let (manager, user) = manager_with_user(&["+get", "~*"]);
        let guard = PermissionGuard::new(&manager, &user, INFO.into(), 1);

        let err = guard.check_command("SET", None).expect_err("SET denied");
        assert_eq!(
            err_msg(&err),
            "NOPERM this user has no permissions to run the 'set' command"
        );

        let entries = manager.log().get(None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_type.name(), "command");
        assert_eq!(entries[0].object, "set");
        assert_eq!(entries[0].username, "u");
    }

    #[test]
    fn denies_subcommand_uses_lowercase_pipe() {
        let (manager, user) = manager_with_user(&["~*", "+@all", "-config|set"]);
        let guard = PermissionGuard::new(&manager, &user, INFO.into(), 1);

        let err = guard
            .check_command("CONFIG", Some("SET"))
            .expect_err("CONFIG|SET denied");
        assert_eq!(
            err_msg(&err),
            "NOPERM this user has no permissions to run the 'config|set' command"
        );

        let entries = manager.log().get(None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_type.name(), "command");
        assert_eq!(entries[0].object, "config|set");
    }

    #[test]
    fn denies_key_logs_and_formats() {
        let (manager, user) = manager_with_user(&["+@all", "~allowed:*"]);
        let guard = PermissionGuard::new(&manager, &user, INFO.into(), 1);

        let err = guard
            .check_keys(&[b"denied:k"], KeyAccessType::Write)
            .expect_err("key denied");
        assert!(err_msg(&err).starts_with("NOPERM"));

        let entries = manager.log().get(None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_type.name(), "key");
        assert_eq!(entries[0].object, "denied:k");
    }

    #[test]
    fn denies_channel_logs_and_formats() {
        let (manager, user) = manager_with_user(&["+@all", "~*", "&chat:*"]);
        let guard = PermissionGuard::new(&manager, &user, INFO.into(), 1);

        let err = guard
            .check_channels(&[Bytes::from_static(b"secret:ch")])
            .expect_err("channel denied");
        assert!(err_msg(&err).starts_with("NOPERM"));

        let entries = manager.log().get(None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_type.name(), "channel");
        assert_eq!(entries[0].object, "secret:ch");
    }

    #[test]
    fn check_keys_stops_at_first_denied_key() {
        let (manager, user) = manager_with_user(&["+@all", "~allowed:*"]);
        let guard = PermissionGuard::new(&manager, &user, INFO.into(), 1);

        // First key denied: only it is logged, the later (also denied) key is not reached.
        let err = guard
            .check_keys(&[b"denied:1", b"denied:2"], KeyAccessType::Read)
            .expect_err("first key denied");
        assert!(err_msg(&err).starts_with("NOPERM"));

        let entries = manager.log().get(None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].object, "denied:1");
    }
}
