//! The shard write seam — the one admission decision every keyspace mutation
//! passes.
//!
//! Three gates historically lived at *queue* time on the connection
//! (`connection/guards.rs`): slot ownership (`-MOVED`), ACL (`-NOPERM` plus its
//! `ACL LOG` entry) and write admission (the replication self-fence and
//! `min-replicas-to-write`'s `-NOREPLICAS`). Queue time is the wrong place for
//! all three, for the same structural reason: a gate evaluated where the
//! *command text* is seen cannot cover a write whose text never appears there.
//! A Lua script's `redis.call('SET', …)` is exactly that write — it is produced
//! by the script body at execute time, so `EVAL "redis.call('SET', …)"` reached
//! the keyspace with none of the three checks applied, while the same `SET`
//! typed directly was refused (`specs/txn.md` FM-TXN-051).
//!
//! [`ShardWriteSeam`] moves the decision to where the mutation actually happens
//! — the shard, immediately before a command touches the store — so *every*
//! producer passes it: declared and undeclared script writes, `MULTI` batches,
//! and any future internal caller. The class is closed structurally rather than
//! per-producer: a new write path that does not route through the seam is a
//! seam-lint failure (`just lint-script-write-seam`), not a silent hole.
//!
//! # The split: shard-derivable vs issuer-scoped
//!
//! Two of the three checks are answerable from handles the shard worker already
//! owns (cluster state + node id for slot ownership; the quorum checker and the
//! replication tracker for write admission). The rest — *which ACL user issued
//! this* and the live `min-replicas-to-write` config — is connection state the
//! shard cannot see, so it travels on the shard message as a [`WriteAdmission`]
//! and is combined with the shard's own handles by
//! [`ShardWriteSeam`]. A message that carries no [`WriteAdmission`] (internal
//! callers, replication apply, tests) still gets the slot-ownership and
//! self-fence halves; it just has no user to authorize and no configured
//! replica floor.

use std::sync::Arc;
use std::time::Duration;

use crate::acl::{AclManager, AuthenticatedUser, FullAclChecker, PermissionResult};
use crate::cluster::{ClusterSnapshot, ClusterState, NodeId};
use crate::command::{KeyAccessFlag, QuorumChecker, required_access_for_key_flags};
use crate::replication::ReplicationTrackerImpl;
use crate::shard::slot_for_key;

/// The error a script sub-command touching a slot this node does not serve is
/// refused with.
///
/// Redis's own wording (`scripting.c`, `luaRedisGenericCommand`): a script is
/// not redirected — it cannot be, since part of it has already run — so the
/// refusal is a plain error rather than a `-MOVED`. Building a `-MOVED` here
/// would also put a redirect reply outside `frogdb-types/src/redirect.rs`,
/// which `lint-redirect-seam` forbids.
pub const NON_LOCAL_KEY_ERR: &str =
    "ERR Lua script attempted to access a non local key in a cluster node";

/// The `min-replicas-to-write` refusal, byte-identical to the connection-level
/// gate's so a client cannot tell which producer was refused.
pub const NO_REPLICAS_ERR: &str = "NOREPLICAS Not enough good replicas to write.";

/// The issuer-scoped half of a write admission decision: what the shard cannot
/// derive from its own handles.
///
/// Built once per command at the connection (where the authenticated user and
/// the live config live) and carried on the shard message. Cheap to clone —
/// every field is an `Arc` or a scalar.
#[derive(Clone)]
pub struct WriteAdmission {
    /// The ACL identity to authorize against, or `None` when the connection is
    /// unauthenticated (ACL is not enforced then, exactly as at the
    /// connection-level seam).
    acl: Option<AclIdentity>,
    /// `min-replicas-to-write`, read live at dispatch time.
    min_replicas: u32,
    /// `min-replicas-timeout` as a duration — how fresh a replica's ACK must be
    /// to count as "good".
    min_replicas_max_lag: Duration,
    /// The write was authorized where it originated and must be applied
    /// verbatim here — see [`WriteAdmission::pre_authorized`].
    pre_authorized: bool,
}

/// The ACL half of a [`WriteAdmission`]: manager (for the audit log and the
/// `requires_auth` policy), the authenticated user, and the `ip:port` recorded
/// on a denial.
#[derive(Clone)]
pub struct AclIdentity {
    manager: Arc<AclManager>,
    user: AuthenticatedUser,
    client_info: Arc<str>,
}

impl AclIdentity {
    /// Bind an identity for `user` as authenticated on the connection reachable
    /// at `client_info` (`ip:port`).
    pub fn new(
        manager: Arc<AclManager>,
        user: AuthenticatedUser,
        client_info: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            manager,
            user,
            client_info: client_info.into(),
        }
    }
}

impl WriteAdmission {
    /// Build the issuer-scoped half of the decision.
    pub fn new(
        acl: Option<AclIdentity>,
        min_replicas: u32,
        min_replicas_max_lag: Duration,
    ) -> Self {
        Self {
            acl,
            min_replicas,
            min_replicas_max_lag,
            pre_authorized: false,
        }
    }

    /// The identity-free admission an internal caller carries: no ACL user, no
    /// configured replica floor. The slot-ownership and self-fence halves still
    /// apply — they come from the shard, not from here.
    pub fn internal() -> Self {
        Self {
            acl: None,
            min_replicas: 0,
            min_replicas_max_lag: Duration::ZERO,
            pre_authorized: false,
        }
    }

    /// The admission for a write that was authorized at its origin and must be
    /// applied here byte-for-byte: a replica applying its primary's stream, or a
    /// recovery replay of the WAL.
    ///
    /// Every gate is off, and each of the three has to be: the primary already
    /// ran the ACL check for the user that issued the write (the replica has no
    /// such user); the slot belongs to the *primary*, so a replica would refuse
    /// every replicated write as non-local; and `min-replicas-to-write` bounds
    /// how many replicas a **primary** must have, so applying it on the replica
    /// would stall the very stream that satisfies it. A replica that filters its
    /// primary's stream diverges — which is the failure this bypass prevents.
    pub fn pre_authorized() -> Self {
        Self {
            acl: None,
            min_replicas: 0,
            min_replicas_max_lag: Duration::ZERO,
            pre_authorized: true,
        }
    }
}

/// Redacted: a `WriteAdmission` rides on `ScriptingMsg`, which derives `Debug`
/// and is rendered into shard traces. The username and the client's `ip:port`
/// are identifying, and the ACL manager holds password hashes, so the identity
/// collapses to whether one is present.
impl std::fmt::Debug for WriteAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteAdmission")
            .field("acl", &self.acl.is_some())
            .field("min_replicas", &self.min_replicas)
            .field("min_replicas_max_lag", &self.min_replicas_max_lag)
            .finish()
    }
}

/// One command's admission request, as the seam sees it.
///
/// `is_write` is the caller's own classification rather than something the seam
/// re-derives: the script gate already knows it (it needs it for the read-only
/// check) and the transaction path reads it off the registry entry. Passing it
/// in keeps the seam free of a second key/flag extraction that could disagree
/// with the caller's — the same one-extraction discipline
/// [`ScriptCommandGate`](crate::scripting) applies to routing.
pub struct WriteRequest<'a> {
    /// Uppercase command name.
    pub name: &'a str,
    /// The container subcommand, uppercase, for the ACL `cmd|sub` check
    /// (`crate::command::extract_subcommand`).
    pub subcommand: Option<String>,
    /// Whether this command mutates the keyspace.
    pub is_write: bool,
    /// The command's keys with their per-key ACL access flags — the
    /// `COMMAND GETKEYSANDFLAGS` shape, so a STORE-family command authorizes
    /// its destination for write and its sources for read.
    pub keyed_flags: &'a [(&'a [u8], Vec<KeyAccessFlag>)],
    /// The command-level access, consulted only for a key whose flag list is
    /// empty.
    pub fallback_access: crate::acl::KeyAccessType,
}

/// The single mutation entry point's policy: slot ownership, ACL, and write
/// admission, in that order.
///
/// Built by the shard worker from its own cluster/replication handles plus the
/// [`WriteAdmission`] that arrived on the message. Held for the span of one
/// script execution or one transaction batch and consulted per sub-command.
///
/// Cheap to clone (every field is an `Option` of an `Arc` or a scalar): the
/// script executor's [`ScriptInvoker`](crate::scripting) holds a clone for the
/// span of one execution.
#[derive(Clone)]
pub struct ShardWriteSeam {
    admission: Option<WriteAdmission>,
    cluster_state: Option<Arc<ClusterState>>,
    node_id: Option<NodeId>,
    quorum_checker: Option<Arc<dyn QuorumChecker>>,
    replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
}

impl ShardWriteSeam {
    /// Assemble the seam from the shard's handles and the message's admission.
    pub fn new(
        admission: Option<WriteAdmission>,
        cluster_state: Option<Arc<ClusterState>>,
        node_id: Option<NodeId>,
        quorum_checker: Option<Arc<dyn QuorumChecker>>,
        replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
    ) -> Self {
        Self {
            admission,
            cluster_state,
            node_id,
            quorum_checker,
            replication_tracker,
        }
    }

    /// A seam that enforces nothing — standalone, no ACL, no replica floor.
    /// Used by the test harnesses and by paths that have already been admitted
    /// upstream (documented at each such site).
    pub fn disabled() -> Self {
        Self::new(None, None, None, None, None)
    }

    /// THE decision. `Ok(())` admits the command; `Err(msg)` is the refusal, as
    /// the exact wire error string the producer surfaces.
    ///
    /// Order matters and mirrors the connection-level gauntlet: authorization
    /// before routing before availability, so a denied user learns it is denied
    /// rather than learning the cluster's topology, and a `NOREPLICAS` never
    /// masks a `NOPERM`.
    pub fn admit(&self, req: &WriteRequest<'_>) -> Result<(), String> {
        // A replicated / replayed write is admitted at its origin, never here
        // (`WriteAdmission::pre_authorized`).
        if self.admission.as_ref().is_some_and(|a| a.pre_authorized) {
            return Ok(());
        }
        self.check_acl(req)?;
        if !req.is_write {
            return Ok(());
        }
        self.check_slot_ownership(req)?;
        self.check_write_admission()
    }

    /// ACL: command (and subcommand) permission, then per-key access. Every
    /// denial is logged to `ACL LOG` with the same `context` the non-scripted
    /// path records, so the audit trail does not depend on which producer
    /// issued the write.
    fn check_acl(&self, req: &WriteRequest<'_>) -> Result<(), String> {
        let Some(identity) = self.admission.as_ref().and_then(|a| a.acl.as_ref()) else {
            return Ok(());
        };
        let checker = FullAclChecker::new(identity.manager.requires_auth());
        let log = identity.manager.log();
        let user = &identity.user;

        if let PermissionResult::Denied(err) =
            checker.check_command(user, req.name, req.subcommand.as_deref())
        {
            let object = match req.subcommand.as_deref() {
                Some(sub) => format!("{}|{}", req.name.to_lowercase(), sub.to_lowercase()),
                None => req.name.to_lowercase(),
            };
            log.log_command_denied(&user.username, &identity.client_info, &object);
            return Err(err.to_string());
        }

        for (key, flags) in req.keyed_flags {
            let access = required_access_for_key_flags(flags, req.fallback_access);
            if let PermissionResult::Denied(err) = checker.check_key_access(user, key, access) {
                let key_str = String::from_utf8_lossy(key);
                log.log_key_denied(&user.username, &identity.client_info, &key_str);
                return Err(err.to_string());
            }
        }
        Ok(())
    }

    /// Slot ownership: in cluster mode every key this write touches must hash to
    /// a slot this node is authoritative for. This is the orphan-write gate —
    /// the shape `specs/txn.md` FM-TXN-009 calls "the orphan-write shape this
    /// campaign exists to prevent", reached by a script that writes a key it
    /// never declared.
    fn check_slot_ownership(&self, req: &WriteRequest<'_>) -> Result<(), String> {
        let (Some(cluster_state), Some(node_id)) = (self.cluster_state.as_ref(), self.node_id)
        else {
            // Standalone: no slot ownership to lose.
            return Ok(());
        };
        if req.keyed_flags.is_empty() {
            return Ok(());
        }
        let snapshot = cluster_state.snapshot();
        for (key, _) in req.keyed_flags {
            if !slot_is_locally_served(&snapshot, slot_for_key(key), node_id) {
                return Err(NON_LOCAL_KEY_ERR.to_string());
            }
        }
        Ok(())
    }

    /// Write admission: the replication self-fence / cluster quorum gate, then
    /// `min-replicas-to-write`. Same two checks, same wording and same order as
    /// `run_pre_checks` applies to a directly-issued write.
    fn check_write_admission(&self) -> Result<(), String> {
        if let Some(checker) = self.quorum_checker.as_ref()
            && !checker.has_quorum()
        {
            return Err(checker.quorum_lost_error().to_string());
        }
        let Some(admission) = self.admission.as_ref() else {
            return Ok(());
        };
        if admission.min_replicas == 0 {
            return Ok(());
        }
        let good = self
            .replication_tracker
            .as_ref()
            .map(|t| t.count_good_replicas(admission.min_replicas_max_lag))
            .unwrap_or(0);
        if (good as u32) < admission.min_replicas {
            return Err(NO_REPLICAS_ERR.to_string());
        }
        Ok(())
    }
}

/// Whether this node is authoritative for `slot` right now.
///
/// The owner always is. An *importing* target is too: it is the node the slot is
/// being handed to, and refusing its writes would break the migration itself.
/// Everything else — a slot owned elsewhere, a slot nobody owns — is not, and a
/// write landing there is the orphan write.
///
/// This is deliberately the permissive union of the connection-level router's
/// local-serve arms (`route_with_snapshot`'s `LocalServe` /
/// `LocalServeMigrating` / `AcceptImporting`), so a batch that the
/// connection-level seam already admitted can never be refused a second time
/// here; only a producer the connection-level seam never saw — a script's
/// runtime write — can be.
fn slot_is_locally_served(snapshot: &ClusterSnapshot, slot: u16, self_node_id: NodeId) -> bool {
    match snapshot.slot_assignment.get(&slot) {
        Some(&owner) => owner == self_node_id,
        None => snapshot
            .migrations
            .get(&slot)
            .is_some_and(|m| m.target_node == self_node_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::{AclConfig, KeyAccessType};
    use crate::cluster::{NodeInfo, SlotMigration};
    use std::sync::atomic::AtomicU64;

    const INFO: &str = "127.0.0.1:5555";

    fn request<'a>(
        name: &'a str,
        is_write: bool,
        keyed_flags: &'a [(&'a [u8], Vec<KeyAccessFlag>)],
    ) -> WriteRequest<'a> {
        WriteRequest {
            name,
            subcommand: None,
            is_write,
            keyed_flags,
            fallback_access: if is_write {
                KeyAccessType::Write
            } else {
                KeyAccessType::Read
            },
        }
    }

    fn identity(rules: &[&str]) -> (Arc<AclManager>, AclIdentity) {
        let manager = AclManager::new(AclConfig::default());
        let mut full = vec!["on", ">pass"];
        full.extend_from_slice(rules);
        manager.set_user("u", &full).expect("set_user");
        let user = manager
            .authenticate("u", "pass", INFO)
            .expect("authenticate");
        let identity = AclIdentity::new(Arc::clone(&manager), user, INFO);
        (manager, identity)
    }

    /// A live [`ClusterState`] whose published snapshot is exactly `snapshot`,
    /// seen from node `self_node_id`.
    fn cluster_state(snapshot: ClusterSnapshot, self_node_id: NodeId) -> Arc<ClusterState> {
        Arc::new(ClusterState::from_snapshot(
            snapshot,
            Arc::new(AtomicU64::new(self_node_id)),
        ))
    }

    fn cluster_with_slot_owner(
        owner: NodeId,
        slot: u16,
        self_node_id: NodeId,
    ) -> Arc<ClusterState> {
        let mut snapshot = ClusterSnapshot::new();
        let addr = "127.0.0.1:7000".parse().unwrap();
        snapshot
            .nodes
            .insert(owner, NodeInfo::new_primary(owner, addr, addr));
        snapshot.slot_assignment.insert(slot, owner);
        cluster_state(snapshot, self_node_id)
    }

    /// A seam with no handles at all admits everything: standalone FrogDB with
    /// no ACL configured must behave exactly as it did before the seam existed.
    #[test]
    fn a_disabled_seam_admits_every_write() {
        let seam = ShardWriteSeam::disabled();
        assert!(seam.admit(&request("SET", true, &[])).is_ok());
    }

    /// The ACL half refuses a denied command and files the `ACL LOG` entry —
    /// the audit trail a scripted write used to escape entirely.
    // FM-TXN-051
    #[test]
    fn a_denied_command_is_refused_and_logged() {
        let (manager, identity) = identity(&["+get", "~*"]);
        let seam = ShardWriteSeam::new(
            Some(WriteAdmission::new(Some(identity), 0, Duration::ZERO)),
            None,
            None,
            None,
            None,
        );
        let err = seam
            .admit(&request("SET", true, &[]))
            .expect_err("SET is not granted");
        assert!(err.starts_with("NOPERM"), "{err}");
        let entries = manager.log().get(None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_type.name(), "command");
        assert_eq!(entries[0].object, "set");
    }

    /// A key outside the user's pattern is refused with a `key`-context log
    /// entry, at the seam, for a producer that never passed the queue.
    #[test]
    fn a_denied_key_is_refused_and_logged() {
        let (manager, identity) = identity(&["+@all", "~allowed:*"]);
        let seam = ShardWriteSeam::new(
            Some(WriteAdmission::new(Some(identity), 0, Duration::ZERO)),
            None,
            None,
            None,
            None,
        );
        let keyed: Vec<(&[u8], Vec<KeyAccessFlag>)> =
            vec![(b"denied:k".as_slice(), vec![KeyAccessFlag::OW])];
        let err = seam
            .admit(&request("SET", true, &keyed))
            .expect_err("key outside ~allowed:*");
        assert!(err.starts_with("NOPERM"), "{err}");
        let entries = manager.log().get(None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_type.name(), "key");
        assert_eq!(entries[0].object, "denied:k");
    }

    /// ACL applies to reads as well as writes — the seam's first gate runs
    /// before the `is_write` short-circuit.
    #[test]
    fn acl_is_checked_for_reads_too() {
        let (_manager, identity) = identity(&["+@all", "~allowed:*"]);
        let seam = ShardWriteSeam::new(
            Some(WriteAdmission::new(Some(identity), 0, Duration::ZERO)),
            None,
            None,
            None,
            None,
        );
        let keyed: Vec<(&[u8], Vec<KeyAccessFlag>)> =
            vec![(b"denied:k".as_slice(), vec![KeyAccessFlag::R])];
        assert!(seam.admit(&request("GET", false, &keyed)).is_err());
    }

    /// The orphan-write gate: a write to a slot this node does not own is
    /// refused even though the key never appeared in any command text the
    /// connection saw.
    // FM-TXN-051
    #[test]
    fn a_write_to_a_slot_owned_elsewhere_is_refused() {
        let key = b"orphan".as_slice();
        let cluster = cluster_with_slot_owner(2, slot_for_key(key), 1);
        let seam = ShardWriteSeam::new(None, Some(cluster), Some(1), None, None);
        let keyed: Vec<(&[u8], Vec<KeyAccessFlag>)> = vec![(key, vec![KeyAccessFlag::OW])];
        assert_eq!(
            seam.admit(&request("SET", true, &keyed)).unwrap_err(),
            NON_LOCAL_KEY_ERR
        );
    }

    /// The owner serves its own slot, and a read of a slot owned elsewhere is
    /// not the seam's business (only writes are gated on ownership here).
    #[test]
    fn the_owner_admits_its_own_slot() {
        let key = b"orphan".as_slice();
        let cluster = cluster_with_slot_owner(1, slot_for_key(key), 1);
        let seam = ShardWriteSeam::new(None, Some(cluster), Some(1), None, None);
        let keyed: Vec<(&[u8], Vec<KeyAccessFlag>)> = vec![(key, vec![KeyAccessFlag::OW])];
        assert!(seam.admit(&request("SET", true, &keyed)).is_ok());
    }

    /// An importing target is authoritative enough to take the write: refusing
    /// it would break the migration that is handing it the slot.
    // FM-TXN-051
    #[test]
    fn an_importing_target_admits_the_write() {
        let key = b"orphan".as_slice();
        let slot = slot_for_key(key);
        let mut snapshot = ClusterSnapshot::new();
        let addr = "127.0.0.1:7000".parse().unwrap();
        for id in [1, 2] {
            snapshot
                .nodes
                .insert(id, NodeInfo::new_primary(id, addr, addr));
        }
        snapshot
            .migrations
            .insert(slot, SlotMigration::new(slot, 2, 1));
        let seam = ShardWriteSeam::new(None, Some(cluster_state(snapshot, 1)), Some(1), None, None);
        let keyed: Vec<(&[u8], Vec<KeyAccessFlag>)> = vec![(key, vec![KeyAccessFlag::OW])];
        assert!(seam.admit(&request("SET", true, &keyed)).is_ok());
    }

    /// `min-replicas-to-write` with no tracker (hence zero good replicas)
    /// refuses the write with the connection-level gate's exact string.
    // FM-TXN-051
    #[test]
    fn min_replicas_refuses_when_no_replica_is_good() {
        let seam = ShardWriteSeam::new(
            Some(WriteAdmission::new(None, 1, Duration::from_millis(10_000))),
            None,
            None,
            None,
            None,
        );
        assert_eq!(
            seam.admit(&request("SET", true, &[])).unwrap_err(),
            NO_REPLICAS_ERR
        );
        // A read is unaffected.
        assert!(seam.admit(&request("GET", false, &[])).is_ok());
    }

    /// The self-fence's wording belongs to the checker, not to the seam.
    // FM-TXN-051
    #[test]
    fn the_quorum_checker_owns_its_refusal_wording() {
        struct Fenced;
        impl QuorumChecker for Fenced {
            fn has_quorum(&self) -> bool {
                false
            }
            fn quorum_lost_error(&self) -> &'static str {
                "SELFFENCE fenced by a lost replica"
            }
        }
        let seam = ShardWriteSeam::new(None, None, None, Some(Arc::new(Fenced)), None);
        assert_eq!(
            seam.admit(&request("SET", true, &[])).unwrap_err(),
            "SELFFENCE fenced by a lost replica"
        );
        assert!(seam.admit(&request("GET", false, &[])).is_ok());
    }

    /// Ordering: a user who is denied the command learns that, not the
    /// cluster's topology and not the replica count.
    // FM-TXN-051
    #[test]
    fn acl_outranks_slot_ownership_and_admission() {
        let key = b"orphan".as_slice();
        let cluster = cluster_with_slot_owner(2, slot_for_key(key), 1);
        let (_manager, identity) = identity(&["+get", "~*"]);
        let seam = ShardWriteSeam::new(
            Some(WriteAdmission::new(
                Some(identity),
                1,
                Duration::from_millis(10_000),
            )),
            Some(cluster),
            Some(1),
            None,
            None,
        );
        let keyed: Vec<(&[u8], Vec<KeyAccessFlag>)> = vec![(key, vec![KeyAccessFlag::OW])];
        let err = seam.admit(&request("SET", true, &keyed)).unwrap_err();
        assert!(err.starts_with("NOPERM"), "{err}");
    }

    /// A replicated write is admitted whatever the seam's handles say. Every
    /// gate would refuse it for a reason that belongs to the primary: the slot
    /// is the primary's, the ACL user is the primary's, the replica floor is a
    /// primary-side constraint. A replica that filtered its primary's stream
    /// would diverge.
    // FM-TXN-051
    #[test]
    fn a_replicated_write_is_admitted_unconditionally() {
        let key = b"orphan".as_slice();
        let cluster = cluster_with_slot_owner(2, slot_for_key(key), 1);
        struct Fenced;
        impl QuorumChecker for Fenced {
            fn has_quorum(&self) -> bool {
                false
            }
            fn quorum_lost_error(&self) -> &'static str {
                "SELFFENCE fenced"
            }
        }
        let seam = ShardWriteSeam::new(
            Some(WriteAdmission::pre_authorized()),
            Some(cluster),
            Some(1),
            Some(Arc::new(Fenced)),
            None,
        );
        let keyed: Vec<(&[u8], Vec<KeyAccessFlag>)> = vec![(key, vec![KeyAccessFlag::OW])];
        assert!(seam.admit(&request("SET", true, &keyed)).is_ok());
    }
}
