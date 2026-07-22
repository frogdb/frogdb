//! Unified ACL enforcement seam.
//!
//! [`PermissionGuard`] is the single place where the three parts of an ACL
//! decision live together: the permission *check* (delegated to
//! [`FullAclChecker`]), the *audit log* of a denial (delegated to
//! [`AclLog::log_*_denied`]), and the *`NOPERM` reply* (the canonical
//! [`AclError`] `Display` string). Every execution path — `run_pre_checks`
//! (command), `validate_channel_access` (channel), `route_and_execute` (key),
//! and the transaction queue
//! ([`PreDispatchView::queue_command`](crate::connection::guards::PreDispatchView::queue_command),
//! key + channel) — constructs a guard for the connection's current user and
//! calls one method per check.
//!
//! Keeping check + log + reply in one location is what makes audit-log
//! completeness structural: a path either routes through the guard (and logs)
//! or it does not enforce at all. It previously lived inline at four call
//! sites that had already drifted (a queue path that skipped logging, a
//! `' '`-vs-`'|'` subcommand separator).

use bytes::Bytes;
use frogdb_core::{
    AclError, AclLog, AclManager, AuthenticatedUser, FullAclChecker, KeyAccessFlag, KeyAccessType,
    PermissionResult,
};
use frogdb_protocol::Response;
use tracing::warn;

use crate::connection::ConnectionHandler;
use crate::connection::util::required_access_for_key_flags;

impl ConnectionHandler {
    /// Build an ACL [`PermissionGuard`] for the connection's current user.
    ///
    /// Returns `None` when the connection is unauthenticated, in which case ACL
    /// is not enforced (a no-password instance, or pre-AUTH on an exempt
    /// command). The `client-info` recorded for any denial is `ip:port`.
    pub(crate) fn permission_guard(&self) -> Option<PermissionGuard<'_>> {
        build_permission_guard(&self.core.acl_manager, &self.state)
    }
}

/// Build an ACL [`PermissionGuard`] from an ACL manager and connection state,
/// independent of the [`ConnectionHandler`]. This is the single construction
/// point shared by [`ConnectionHandler::permission_guard`] (key-access checks on
/// the routing path) and the socketless pre-dispatch guard view
/// (`PreDispatchView::permission_guard`), so both enforce with identical user /
/// client-info binding.
///
/// Returns `None` when the connection is unauthenticated (ACL not enforced).
pub(crate) fn build_permission_guard<'a>(
    acl_manager: &'a AclManager,
    state: &'a crate::connection::ConnectionState,
) -> Option<PermissionGuard<'a>> {
    let user = state.authenticated_user()?;
    let client_info = format!("{}:{}", state.addr.ip(), state.addr.port());
    Some(PermissionGuard::new(
        acl_manager,
        user,
        client_info,
        state.id,
    ))
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

    /// Check access to every key using its *own* required access, derived from
    /// the per-key access flags (`COMMAND GETKEYSANDFLAGS` semantics). This is
    /// the Redis-accurate enforcement seam: a STORE-family command checks its
    /// destination for write and its sources for read, rather than applying a
    /// single command-level access to every key.
    ///
    /// The command itself is validated upstream by `run_pre_checks`, so only key
    /// access is checked here. `fallback` is the command-level access, consulted
    /// only for a key whose flag list is empty (see
    /// [`required_access_for_key_flags`]). The first denied key is logged to
    /// `ACL LOG` and its `NOPERM` reply returned; remaining keys are not checked.
    #[allow(clippy::result_large_err)]
    pub(crate) fn check_keys_with_flags(
        &self,
        keyed_flags: &[(&[u8], Vec<KeyAccessFlag>)],
        fallback: KeyAccessType,
    ) -> Result<(), Response> {
        for (key, flags) in keyed_flags {
            let access = required_access_for_key_flags(flags, fallback);
            self.check_single_key(key, access)?;
        }
        Ok(())
    }

    /// Check a single key at `access`, logging + rendering a denial identically
    /// to [`check_keys_with_flags`](Self::check_keys_with_flags), which is its
    /// sole caller. Factored out so the audit-log entry and `NOPERM` reply have
    /// exactly one construction site.
    #[allow(clippy::result_large_err)]
    fn check_single_key(&self, key: &[u8], access: KeyAccessType) -> Result<(), Response> {
        if let PermissionResult::Denied(err) = self.checker.check_key_access(self.user, key, access)
        {
            let key_str = String::from_utf8_lossy(key);
            self.log
                .log_key_denied(&self.user.username, &self.client_info, &key_str);
            return Err(Self::error(err));
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
                .check_keys_with_flags(
                    &[(b"allowed:x", vec![KeyAccessFlag::R])],
                    KeyAccessType::Read
                )
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
            .check_keys_with_flags(
                &[(b"denied:k", vec![KeyAccessFlag::OW])],
                KeyAccessType::Write,
            )
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
            .check_keys_with_flags(
                &[
                    (b"denied:1", vec![KeyAccessFlag::R]),
                    (b"denied:2", vec![KeyAccessFlag::R]),
                ],
                KeyAccessType::Read,
            )
            .expect_err("first key denied");
        assert!(err_msg(&err).starts_with("NOPERM"));

        let entries = manager.log().get(None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].object, "denied:1");
    }

    /// Redis parity for STORE-family commands: a `%R~src:* %W~dst:*` user is
    /// allowed when the destination key needs only write and the source keys
    /// need only read — the per-key flags decouple the two requirements. The
    /// old command-level derivation applied `ReadWrite` (a WRITE command) to
    /// every key and would have denied this.
    #[test]
    fn per_key_flags_allow_store_family_split_access() {
        let (manager, user) = manager_with_user(&["+@all", "%R~src:*", "%W~dst:*"]);
        let guard = PermissionGuard::new(&manager, &user, INFO.into(), 1);

        // SINTERSTORE dst:x src:a src:b — dest overwritten, sources read.
        assert!(
            guard
                .check_keys_with_flags(
                    &[
                        (b"dst:x", vec![KeyAccessFlag::OW]),
                        (b"src:a", vec![KeyAccessFlag::R]),
                        (b"src:b", vec![KeyAccessFlag::R]),
                    ],
                    KeyAccessType::ReadWrite,
                )
                .is_ok(),
            "split R/W user should run a STORE command whose keys split R/W"
        );
        assert!(manager.log().is_empty(), "no denial => no ACL LOG entry");
    }

    /// Inverse: the same split-access user is denied when a source lacks read
    /// (write on dst does not grant read on a src:) and when the dest is only
    /// readable (read on dst does not grant the write it needs).
    #[test]
    fn per_key_flags_deny_when_a_key_lacks_its_required_access() {
        let (manager, user) = manager_with_user(&["+@all", "%R~src:*", "%W~dst:*"]);
        let guard = PermissionGuard::new(&manager, &user, INFO.into(), 1);

        // Source without read grant (other:a) — read denied.
        let err = guard
            .check_keys_with_flags(
                &[
                    (b"dst:x", vec![KeyAccessFlag::OW]),
                    (b"other:a", vec![KeyAccessFlag::R]),
                ],
                KeyAccessType::ReadWrite,
            )
            .expect_err("unreadable source denied");
        assert!(err_msg(&err).starts_with("NOPERM"));

        // Destination that only has read grant but the command needs write.
        let err = guard
            .check_keys_with_flags(
                &[(b"src:a", vec![KeyAccessFlag::OW])],
                KeyAccessType::ReadWrite,
            )
            .expect_err("read-only dest denied for write");
        assert!(err_msg(&err).starts_with("NOPERM"));
    }
}
