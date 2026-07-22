//! The metadata-write seam: propose to Raft, and if this node is not the
//! leader, either forward the write to it or resolve where the client should be
//! redirected.
//!
//! Every write to the Raft Metadata Plane runs the same four-step saga by hand:
//! `client_write(cmd)` → on `ForwardToLeader` resolve the leader address →
//! `forward_write(cmd_clone)` → else surface a `REDIRECT`/`CLUSTERDOWN`. This
//! module gives that saga a single home. Callers keep only their genuinely
//! distinct policy (surface the redirect / retry-and-ignore it / run a
//! leader-only side effect); the forward-vs-redirect decision moves behind a
//! fakeable seam so it is unit-testable without a live multi-node Raft.
//!
//! The wire rendering of a [`LeaderRedirect`] into a RESP `REDIRECT`/`CLUSTERDOWN`
//! string deliberately stays in the `server` crate — `cluster` does not depend on
//! the protocol layer, so [`propose`](ClusterWriter::propose) returns a *typed*
//! [`LeaderRedirect`] and each caller renders (or ignores) it as its policy
//! dictates.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use openraft::BasicNode;
use openraft::error::{ClientWriteError, RaftError};

use crate::network::ClusterNetworkFactory;
use crate::state::ClusterState;
use crate::types::{ClusterCommand, ClusterResponse, NodeId};

/// The error type returned by [`ClusterRaft::client_write`](crate::ClusterRaft),
/// with the state-machine response already unwrapped out of the `Ok` side by the
/// [`RaftProposer`] seam.
pub type RaftClientWriteError = RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>;

/// Where a non-leader should send the client, resolved from a `ForwardToLeader`.
///
/// `leader_client_addr.is_some()` → a `REDIRECT`; `None` → a `CLUSTERDOWN`. The
/// `server` crate owns turning this into the actual wire string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderRedirect {
    /// The leader's node id, if the `ForwardToLeader` named one.
    pub leader_id: Option<NodeId>,
    /// The leader's *client* address, if known — `Some` → REDIRECT, `None` →
    /// CLUSTERDOWN.
    pub leader_client_addr: Option<SocketAddr>,
}

/// Successful outcome of a [`propose`](ClusterWriter::propose).
///
/// The leader-vs-forwarded distinction is load-bearing, not cosmetic: only the
/// caller of a *local* commit runs the leader-only side effects (e.g.
/// `spawn_add_raft_voter`); on the forward path the leader-side `ForwardedWrite`
/// receiver already performed them, so the caller must **not** repeat them.
pub enum Proposed {
    /// This node was the leader and committed locally. Carries the state
    /// machine's [`ClusterResponse`]; the caller still inspects `Error(_)` and,
    /// on success, runs its leader-only side effects.
    Committed(ClusterResponse),
    /// This node was a follower; the write was forwarded and the forward
    /// returned `Ok` (a clean remote commit). The remote receiver already
    /// performed any voter-add.
    Forwarded,
}

/// Failure outcome of a [`propose`](ClusterWriter::propose).
pub enum ProposeError {
    /// Not the leader and the forward did not land — the forward failed for any
    /// reason (network failure, a state-machine rejection on the leader, or the
    /// leader address could not be resolved to forward at all). Preserves
    /// today's behavior: any forward failure falls through to a redirect, never
    /// to an `ERR`. The caller decides whether to surface it (connection paths)
    /// or ignore it and retry (bootstrap spawns).
    Redirect(LeaderRedirect),
    /// A Raft error that was **not** `ForwardToLeader` (rendered as
    /// `ERR Raft error: {0}` by the connection paths today).
    Raft(String),
}

/// Minimal seam over `client_write` so the forward/redirect decision is fakeable
/// in unit tests without a live `openraft::Raft`. The blanket impl wraps the
/// concrete [`ClusterRaft`](crate::ClusterRaft) alias (which cannot carry
/// inherent methods itself) and unwraps `ClientWriteResponse::data`.
pub trait RaftProposer: Send + Sync {
    /// Propose `cmd` to Raft, returning the state-machine [`ClusterResponse`] on
    /// a local commit or the raft error (notably `ForwardToLeader`) otherwise.
    fn client_write(
        &self,
        cmd: ClusterCommand,
    ) -> impl Future<Output = Result<ClusterResponse, RaftClientWriteError>> + Send;
}

impl RaftProposer for Arc<crate::ClusterRaft> {
    async fn client_write(
        &self,
        cmd: ClusterCommand,
    ) -> Result<ClusterResponse, RaftClientWriteError> {
        // `(**self)` reaches the inherent `openraft::Raft::client_write`; calling
        // `self.client_write` would recurse into this trait method.
        (**self).client_write(cmd).await.map(|resp| resp.data)
    }
}

/// Seam over the cluster-bus forward leg. The blanket impl resolves the leader's
/// bus address via [`ClusterNetworkFactory::get_node_addr`] and forwards through
/// [`ClusterNetwork::forward_write`](crate::network::ClusterNetwork::forward_write);
/// a fake supplies success/failure directly in tests.
pub trait LeaderForwarder: Send + Sync {
    /// Attempt to forward `cmd` to `leader_id` over the cluster bus. `Ok(())`
    /// means a clean remote commit; `Err(())` covers every failure (address
    /// unknown, network error, or remote state-machine rejection) and drives the
    /// caller to a redirect.
    fn forward_write(
        &self,
        leader_id: NodeId,
        cmd: ClusterCommand,
    ) -> impl Future<Output = Result<(), ()>> + Send;
}

impl LeaderForwarder for Arc<ClusterNetworkFactory> {
    async fn forward_write(&self, leader_id: NodeId, cmd: ClusterCommand) -> Result<(), ()> {
        let Some(bus_addr) = self.get_node_addr(leader_id) else {
            return Err(());
        };
        let net = self.connect(leader_id, bus_addr);
        net.forward_write(cmd).await.map_err(|_| ())
    }
}

/// Owns the propose → (forward | redirect) saga. Wraps the three collaborators
/// the saga needs; nothing else in the write path needs them together.
///
/// Generic over the [`RaftProposer`] and [`LeaderForwarder`] seams so the
/// forward-vs-redirect fork is unit-testable with fakes; production callers use
/// the defaults (`Arc<ClusterRaft>` + `Arc<ClusterNetworkFactory>`).
pub struct ClusterWriter<R = Arc<crate::ClusterRaft>, F = Arc<ClusterNetworkFactory>> {
    raft: R,
    forwarder: F,
    cluster_state: Arc<ClusterState>,
}

impl<R: RaftProposer, F: LeaderForwarder> ClusterWriter<R, F> {
    /// Construct a writer over the given proposer, forwarder, and replicated
    /// state.
    pub fn new(raft: R, forwarder: F, cluster_state: Arc<ClusterState>) -> Self {
        Self {
            raft,
            forwarder,
            cluster_state,
        }
    }

    /// Propose on the leader, or forward to it.
    ///
    /// - Leader commit → `Ok(Proposed::Committed(resp))`; the caller inspects
    ///   `resp` for `Error(_)` and runs its leader-only side effects.
    /// - Follower, forward succeeds → `Ok(Proposed::Forwarded)`; the remote
    ///   receiver already added any voter, so the caller must not.
    /// - Follower, forward fails or unresolvable → `Err(Redirect(..))`.
    /// - Non-`ForwardToLeader` Raft error → `Err(Raft(..))`.
    pub async fn propose(&self, cmd: ClusterCommand) -> Result<Proposed, ProposeError> {
        // Clone before consuming in `client_write`; the retained copy is only
        // needed for the forward leg, and lives inside the writer now instead of
        // being smeared across every caller.
        match self.raft.client_write(cmd.clone()).await {
            Ok(resp) => Ok(Proposed::Committed(resp)),
            Err(e) => {
                if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &e {
                    let leader_id = forward.leader_id;
                    if let Some(leader_id) = leader_id
                        && self.forwarder.forward_write(leader_id, cmd).await.is_ok()
                    {
                        return Ok(Proposed::Forwarded);
                    }
                    Err(ProposeError::Redirect(resolve_redirect(
                        leader_id,
                        &self.cluster_state,
                    )))
                } else {
                    Err(ProposeError::Raft(e.to_string()))
                }
            }
        }
    }
}

/// Pure resolution of the *redirect fallback* — unit-testable with no async Raft
/// and no network factory. Given the `leader_id` decoded from a `ForwardToLeader`
/// plus the [`ClusterState`] client-address lookup, produce the [`LeaderRedirect`]
/// (`Some` client addr → REDIRECT, `None` → CLUSTERDOWN).
///
/// It does **not** decide the forward target: the cluster-bus forward address is
/// resolved and consumed by [`ClusterWriter::propose`] before this fallback is
/// ever reached, so the network factory is intentionally not a parameter here.
pub fn resolve_redirect(leader_id: Option<NodeId>, cluster_state: &ClusterState) -> LeaderRedirect {
    let leader_client_addr = leader_id
        .and_then(|id| cluster_state.get_node(id))
        .map(|info| info.addr);
    LeaderRedirect {
        leader_id,
        leader_client_addr,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeInfo;
    use openraft::error::{ClientWriteError, Fatal, ForwardToLeader, RaftError};

    fn state_with_leader(id: NodeId, addr: &str) -> ClusterState {
        let cs = ClusterState::new();
        let client_addr: SocketAddr = addr.parse().unwrap();
        let bus_addr: SocketAddr = "127.0.0.1:26379".parse().unwrap();
        cs.apply_local(ClusterCommand::AddNode {
            node: NodeInfo::new_primary(id, client_addr, bus_addr),
        })
        .unwrap();
        cs
    }

    // --- resolve_redirect: the pure fallback ---------------------------------

    #[test]
    fn resolve_redirect_known_leader_yields_redirect() {
        let cs = state_with_leader(7, "127.0.0.1:6379");
        let r = resolve_redirect(Some(7), &cs);
        assert_eq!(r.leader_id, Some(7));
        assert_eq!(
            r.leader_client_addr,
            Some("127.0.0.1:6379".parse().unwrap())
        );
    }

    #[test]
    fn resolve_redirect_leader_not_in_state_yields_clusterdown() {
        let cs = state_with_leader(7, "127.0.0.1:6379");
        // Leader id 9 is unknown to the state -> no client addr -> CLUSTERDOWN.
        let r = resolve_redirect(Some(9), &cs);
        assert_eq!(r.leader_id, Some(9));
        assert_eq!(r.leader_client_addr, None);
    }

    #[test]
    fn resolve_redirect_no_leader_yields_clusterdown() {
        let cs = ClusterState::new();
        let r = resolve_redirect(None, &cs);
        assert_eq!(r.leader_id, None);
        assert_eq!(r.leader_client_addr, None);
    }

    // --- propose: the forward-vs-redirect fork -------------------------------

    struct FakeProposer(Result<ClusterResponse, RaftClientWriteError>);

    impl RaftProposer for FakeProposer {
        async fn client_write(
            &self,
            _cmd: ClusterCommand,
        ) -> Result<ClusterResponse, RaftClientWriteError> {
            self.0.clone()
        }
    }

    /// Records whether the forward leg was invoked, so tests can assert the
    /// leader-commit path never forwards.
    struct FakeForwarder {
        outcome: Result<(), ()>,
    }

    impl LeaderForwarder for FakeForwarder {
        async fn forward_write(
            &self,
            _leader_id: NodeId,
            _cmd: ClusterCommand,
        ) -> Result<(), ()> {
            self.outcome
        }
    }

    fn forward_to_leader_err(leader_id: Option<NodeId>) -> RaftClientWriteError {
        RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
            leader_id,
            leader_node: None,
        }))
    }

    fn writer(
        proposer: FakeProposer,
        forwarder: FakeForwarder,
        cs: ClusterState,
    ) -> ClusterWriter<FakeProposer, FakeForwarder> {
        ClusterWriter::new(proposer, forwarder, Arc::new(cs))
    }

    #[tokio::test]
    async fn leader_commit_ok_is_committed() {
        // Forward would panic-fail if reached; leader commit must not forward.
        let w = writer(
            FakeProposer(Ok(ClusterResponse::Ok)),
            FakeForwarder { outcome: Err(()) },
            state_with_leader(1, "127.0.0.1:6379"),
        );
        match w.propose(ClusterCommand::IncrementEpoch).await {
            Ok(Proposed::Committed(ClusterResponse::Ok)) => {}
            _ => panic!("expected Committed(Ok)"),
        }
    }

    #[tokio::test]
    async fn leader_commit_state_machine_error_is_committed_error() {
        // The state machine rejected the write; the caller still inspects it.
        let w = writer(
            FakeProposer(Ok(ClusterResponse::Error(
                crate::types::ClusterError::NotLeader,
            ))),
            FakeForwarder { outcome: Err(()) },
            state_with_leader(1, "127.0.0.1:6379"),
        );
        match w.propose(ClusterCommand::IncrementEpoch).await {
            Ok(Proposed::Committed(ClusterResponse::Error(
                crate::types::ClusterError::NotLeader,
            ))) => {}
            _ => panic!("expected Committed(Error(NotLeader))"),
        }
    }

    #[tokio::test]
    async fn follower_forward_success_is_forwarded() {
        let w = writer(
            FakeProposer(Err(forward_to_leader_err(Some(2)))),
            FakeForwarder { outcome: Ok(()) },
            state_with_leader(2, "127.0.0.1:6380"),
        );
        // The writer itself never adds a voter — a forwarded write's voter-add
        // is the leader-side receiver's job. Asserting the `Forwarded` outcome
        // is exactly the signal the caller uses to skip `spawn_add_raft_voter`.
        match w.propose(ClusterCommand::IncrementEpoch).await {
            Ok(Proposed::Forwarded) => {}
            _ => panic!("expected Forwarded"),
        }
    }

    #[tokio::test]
    async fn follower_forward_failure_is_redirect() {
        let w = writer(
            FakeProposer(Err(forward_to_leader_err(Some(2)))),
            FakeForwarder { outcome: Err(()) },
            state_with_leader(2, "127.0.0.1:6380"),
        );
        match w.propose(ClusterCommand::IncrementEpoch).await {
            Err(ProposeError::Redirect(r)) => {
                assert_eq!(r.leader_id, Some(2));
                assert_eq!(
                    r.leader_client_addr,
                    Some("127.0.0.1:6380".parse().unwrap())
                );
            }
            _ => panic!("expected Redirect"),
        }
    }

    #[tokio::test]
    async fn follower_forward_failure_unknown_leader_is_clusterdown_redirect() {
        // Forward fails and the leader is not in cluster state -> CLUSTERDOWN.
        let w = writer(
            FakeProposer(Err(forward_to_leader_err(Some(2)))),
            FakeForwarder { outcome: Err(()) },
            ClusterState::new(),
        );
        match w.propose(ClusterCommand::IncrementEpoch).await {
            Err(ProposeError::Redirect(r)) => {
                assert_eq!(r.leader_id, Some(2));
                assert_eq!(r.leader_client_addr, None);
            }
            _ => panic!("expected Redirect (CLUSTERDOWN)"),
        }
    }

    #[tokio::test]
    async fn non_forward_raft_error_is_raft_error() {
        let w = writer(
            FakeProposer(Err(RaftError::Fatal(Fatal::Panicked))),
            FakeForwarder { outcome: Ok(()) },
            state_with_leader(1, "127.0.0.1:6379"),
        );
        match w.propose(ClusterCommand::IncrementEpoch).await {
            Err(ProposeError::Raft(msg)) => assert!(msg.contains("panic")),
            _ => panic!("expected Raft error"),
        }
    }
}
