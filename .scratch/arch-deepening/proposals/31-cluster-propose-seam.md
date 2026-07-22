# Proposal 31 — A `ClusterWriter` seam owns propose-then-forward-then-redirect, so callers stop hand-rolling the Raft leader saga

## Summary

Every write to the **Raft Metadata Plane** re-implements the same four-step saga by hand:
`raft.client_write(cmd)` → on `ForwardToLeader` resolve the leader address via the
`ClusterNetworkFactory` → `forward_write(cmd_clone)` → else format a `REDIRECT`/`CLUSTERDOWN`
string. The candidate flagged four sites; verification found the saga's *propose-and-check* half
is duplicated in **ten** places, its *forward* half in **four**, and its *redirect-surfacing*
half in **two byte-identical copies** — while the two bootstrap spawns silently drop the redirect
and the failure detector never forwards at all. Each site clones the command solely to keep a copy
for the forward path. This proposal introduces a single deep module — `ClusterWriter` in the
`cluster` crate, co-located with the forwarding machinery it already owns — whose interface is
`propose(cmd) -> Result<Proposed, ProposeError>`, where `Proposed` distinguishes
`Committed(resp)` (this node was the leader and committed locally) from `Forwarded` (a follower
whose write landed on the leader). That distinction is **load-bearing**, not cosmetic: on the
leader-commit path the caller must add the new node to the Raft voter set (`spawn_add_raft_voter`,
`connection/cluster.rs:86`), whereas on the forward path the voter-add already happened remotely
in the leader-side `ForwardedWrite` receiver (`network.rs:687-689`) and the caller must **not**
repeat it. Callers keep only their genuinely distinct policy (surface the redirect / retry-and-
ignore it / run a data-path side effect), and the forward-vs-redirect decision becomes
unit-testable behind a fakeable seam instead of reachable only through a live multi-node Raft
integration test.

The one correction to the candidate: `ClusterRaft` is a concrete `openraft::Raft<TypeConfig>` type
alias, so the seam is a **wrapper** (`ClusterWriter`), *not* an inherent `ClusterRaft::propose`
method — you cannot hang methods on the foreign alias.

## Files involved (verified paths + line ranges)

| File | Lines | Role in the current fragmented design |
| --- | --- | --- |
| `frogdb-server/crates/server/src/connection/cluster.rs` | 227 | `handle_raft_command` (L21-144): full saga — propose, forward (L98-121), `REDIRECT`/`CLUSTERDOWN` (L129-138), plus register/unregister side effects; `handle_reset_command` (L176-225): propose-and-check-error, **no** forward (L201-210) |
| `frogdb-server/crates/server/src/slot_migration/mod.rs` | 168 | `commit` (L129-166): full saga, byte-identical `REDIRECT`/`CLUSTERDOWN` strings (L152-161); reached from `begin`/`complete`/`cancel` (L100-123) |
| `frogdb-server/crates/server/src/server/cluster_init.rs` | 874 | self-registration spawn (L380-425): propose + forward (L404), **drops** redirect (retry loop), rebuilds `fwd_cmd` instead of cloning; slot-assignment spawn (L435-495): propose + forward (L471), **drops** redirect, uses `cmd.clone()` |
| `frogdb-server/crates/server/src/failure_detector.rs` | — | `mark_node_failed` (L293-312), `mark_node_recovered` (L314-325), `trigger_auto_failover` (L422-459): propose-and-check-error with **no** forward — a follower's detection is silently dropped |
| `frogdb-server/crates/cluster/src/network.rs` | — | `ClusterNetworkFactory` (struct L181; `get_node_addr` L245, `connect` L255, `register_node` L234); `ClusterNetwork::forward_write` (L362); `ForwardedWrite` **receiver** handler (L672-694): leader-side propose-and-check-error, terminal (no re-forward) |
| `frogdb-server/crates/cluster/src/lib.rs` | — | `pub type ClusterRaft = Raft<TypeConfig>` (L68) — concrete alias, cannot carry inherent methods; re-exports `ClusterState` (L57) |
| `frogdb-server/crates/cluster/src/types.rs` | — | `ClusterResponse { Ok, Value, Error(String) }` (L361-368); `ClusterError` (L405); `NodeId = u64` (L14) |
| `frogdb-server/crates/core/src/shard/dispatch_cluster.rs` | 27 | `RaftCommand` shard message (L12-22): raw `client_write`, `map_err(to_string)`, no error-inspection, no forward |
| `frogdb-server/docs/adr/0001-raft-cluster-metadata.md` | — | Settled: embedded Raft owns metadata; **the data path never goes through Raft.** This proposal touches only the metadata write path and does not alter that decision. |

## Problem (concrete verified evidence)

### The saga, verbatim, in two places

`handle_raft_command` (`connection/cluster.rs:96-139`) and `commit`
(`slot_migration/mod.rs:138-163`) implement the identical control flow:

```rust
Err(e) => {
    if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &e {
        if let Some(leader_id) = forward.leader_id
            && let Some(leader_addr) = /* network_factory */ .get_node_addr(leader_id)
        {
            let net = /* network_factory */ .connect(leader_id, leader_addr);
            if net.forward_write(cmd_clone).await.is_ok() { return Response::ok(); }
        }
        if let Some(leader_id) = forward.leader_id
            && let Some(leader_info) = /* cluster_state */ .get_node(leader_id)
        {
            return Response::error(format!("REDIRECT {} {}", leader_id, leader_info.addr));
        }
        return Response::error(format!("CLUSTERDOWN No leader available: {:?}", forward.leader_id));
    }
    Response::error(format!("ERR Raft error: {}", e))
}
```

The `"REDIRECT {} {}"` and `"CLUSTERDOWN No leader available: {:?}"` format strings are **byte-for-byte
identical** across the two files (`connection/cluster.rs:130,136` vs `slot_migration/mod.rs:153,159`),
maintained by hand in two crates' worth of call sites. `slot_migration/mod.rs`'s own module doc
(L14-15) admits the duplication: *"mirroring the existing pattern used by
`handle_raft_command`."*

### Per-site-invisible divergence — verified

The four *forwarding* sites do **not** agree on what happens after the forward decision:

- **`connection/cluster.rs` / `slot_migration/mod.rs`** surface the redirect to the client as a
  `REDIRECT`/`CLUSTERDOWN` wire response.
- **`cluster_init.rs` self-registration spawn (L393-424)** and **slot-assignment spawn (L462-491)**
  sit inside a 30-attempt retry loop and **silently drop the redirect**: on a non-leader they attempt
  the forward, and if it fails they `sleep(500ms)` and retry — the `REDIRECT`/`CLUSTERDOWN`
  distinction is never computed. Two of the four sites reach for the leader address only to forward,
  never to redirect.

Because each site re-implements the branch, the divergence is invisible at any one site — you must
diff all four to see that two surface the redirect and two swallow it. There is no single place
where "how a metadata write handles not-being-leader" is decided.

### The two success paths are asymmetric — and the seam must preserve that

`handle_raft_command` does **not** run the same side effects on leader-commit and on
forward-success, and any seam that flattens both into one `Ok` arm silently breaks voter promotion:

- **Leader-commit** (`connection/cluster.rs:79-88`): after checking `ClusterResponse::Error`, the
  caller calls `factory.register_node(node_id, addr)` **and** `spawn_add_raft_voter((**raft).clone(),
  node_id, addr)` (L86). On the pure-leader path there is no forward and no remote receiver, so
  **L86 is the only place the new node ever becomes a Raft voter**.
- **Forward-success** (`connection/cluster.rs:106-119`): the caller calls `register_node` **only**
  (L111) — deliberately, because the voter-add is performed by the leader-side `ForwardedWrite`
  receiver (`network.rs:687-689`) once the forwarded write commits there. `forward_write` returns
  `Ok(())` **only** on a clean remote commit (`network.rs:365`; a state-machine rejection comes back
  as `Err(msg)` via `network.rs:684`), so the forward-success caller also does not re-inspect
  `ClusterResponse::Error`.

If `propose` returned an undifferentiated `Ok(ClusterResponse)`, the caller could no longer tell
leader-commit from forward. Dropping `spawn_add_raft_voter` on the merged arm would mean a `MEET`
that commits on the leader never promotes the joining node to voter — a real correctness
regression. Calling it unconditionally would make every **follower** that forwards a write spawn a
doomed `add_learner`/voter attempt (it is not the leader; the call `ForwardToLeader`-errors and the
5 retries in `spawn_add_raft_voter` all fail). The seam therefore must carry the leader-vs-forwarded
outcome in `propose`'s success type; the flat two-arm `Result<ClusterResponse, LeaderRedirect>` the
candidate sketched cannot, which is why the design below returns `Proposed::{Committed, Forwarded}`.

### The propose-and-check half is duplicated far more widely than four sites

A workspace grep for `client_write` (excluding trait defs) returns **ten** metadata-write call
sites, every one of which independently re-encodes "propose, then inspect `resp.data` for
`ClusterResponse::Error`":

| Site | Forwards? | Surfaces redirect? | Checks `ClusterResponse::Error`? |
| --- | --- | --- | --- |
| `connection/cluster.rs:71` (`handle_raft_command`) | yes | yes (`REDIRECT`/`CLUSTERDOWN`) | yes → `ERR {msg}` |
| `slot_migration/mod.rs:131` (`commit`) | yes | yes (`REDIRECT`/`CLUSTERDOWN`) | yes → `ERR {msg}` |
| `cluster_init.rs:385` (self-register spawn) | yes | **dropped** (retry) | no (logs `Ok`) |
| `cluster_init.rs:452` (slot-assign spawn) | yes | **dropped** (retry) | no (logs `Ok`) |
| `connection/cluster.rs:201` (`handle_reset_command`) | **no** | n/a | yes → `ERR {msg}` |
| `failure_detector.rs:295` (`mark_node_failed`) | **no** | n/a | yes → warn+return |
| `failure_detector.rs:317` (`mark_node_recovered`) | **no** | n/a | no |
| `failure_detector.rs:430` (`trigger_auto_failover`) | **no** (3× retry) | n/a | yes → error+return |
| `cluster/network.rs:680` (`ForwardedWrite` receiver) | terminal | n/a | yes → `Err(msg)` |
| `core/dispatch_cluster.rs:14` (shard `RaftCommand`) | **no** | n/a | **no** (raw) |

The candidate's "four places" is the *forwarding* subset; the underlying propose-and-check idiom is
duplicated across ten sites in three crates, each spelling the `ClusterResponse::Error` inspection
slightly differently (some `ERR {msg}`, some warn-and-return, one ignores it entirely at
`dispatch_cluster.rs:14`). This is a strengthening of the premise, not a collapse.

### The command clone exists only for the saga

Both forwarding sites clone the command before consuming it, purely to retain a copy for
`forward_write`: `let cmd_clone = cmd.clone()` (`connection/cluster.rs:68`,
`slot_migration/mod.rs:130`), with the comment *"Clone cmd before consuming it in client_write —
needed for cluster bus forwarding if this node isn't the Raft leader."* The self-registration spawn
does not even clone — it **rebuilds** the command (`fwd_cmd = ClusterCommand::AddNode { node:
self_node.clone() }`, `cluster_init.rs:401-403`), a third spelling of the same "keep a copy for the
forward" need. Ownership of the retained-for-forward copy belongs inside the writer, not smeared
across every caller.

### Testability: the fork is reachable only through live multi-node Raft

Grepping the test tree, every assertion that touches the redirect path is a booted multi-node
integration test: `server/tests/integration_cluster.rs:4621,8115,8322` match on
`msg.starts_with("REDIRECT ")`, and `test-harness/src/cluster_harness.rs:543` handles `REDIRECT` in
the harness client. There is **no** unit test anywhere that exercises "given a `ForwardToLeader`,
attempt the forward, else produce a redirect" — because the decision is inlined into four async
methods that each require a real `openraft::Raft` to reach it. `ClusterNetworkFactory` is a concrete
struct (`network.rs:181`) and `ClusterRaft` a concrete `Raft<TypeConfig>` alias (`lib.rs:68`), so
neither collaborator can be faked today.

## Why it is shallow/fragmented (architecture vocabulary)

**No module owns the metadata-write seam.** The saga "propose to Raft; if I'm not the leader, either
forward to it or tell the client where it is" is one coherent responsibility, but it has no home. It
is scattered as inline code across four async methods in two crates, so the **Interface** to the
Raft Metadata Plane is effectively *raw `client_write` plus a copy-pasted 20-line error arm*. That
is a **shallow** surface: every caller must know openraft's `RaftError::APIError(ClientWriteError::
ForwardToLeader)` shape, the `ClusterNetworkFactory` resolution dance, and the exact `REDIRECT`
wire format. A deep module would hide all three behind one call.

**Poor locality forces hand-maintained consistency.** The two `REDIRECT`/`CLUSTERDOWN` strings are
identical by discipline, not by construction; the forward-then-redirect ordering is re-derived at
each site. Add a fifth metadata-write path (a plausible future admin command) and you either
copy-paste the arm a fifth time or forget the forward and silently break follower writes — exactly
the class of per-site-invisible divergence already present between the surfacing sites and the
dropping sites.

**The command clone is a leaked implementation detail.** Callers clone (or rebuild) the command only
because the *forward* leg needs a second copy — an internal concern of the saga that the shallow
interface pushes onto every caller. A deep `propose(cmd)` takes ownership once and manages its own
retained copy.

**The seam that would enable testing is missing.** Because the decision is inlined against concrete
collaborators, the only **leverage** point is a live Raft cluster. Extraction splits the win in two,
and it is worth being precise about which half is cheap: the `resolve_redirect` *fallback* (leader
addr known → REDIRECT, else CLUSTERDOWN) becomes a genuinely pure, dependency-free unit test
immediately. The higher-value branch — forward succeeds vs. forward fails → redirect — is what
today needs a live cluster, and reaching it in a unit test still requires the `RaftProposer` trait
seam **plus** a fakeable `ClusterNetworkFactory` (a concrete struct today; see Risks). So it is a
real testability win, but a bounded one that costs one trait and a factory fake, not a free
consequence of the extraction.

## Proposed design (Rust interface sketch — signatures/types only)

A single deep module in the **`cluster` crate** (the base crate that already defines `ClusterRaft`,
`ClusterState`, `ClusterNetworkFactory`, and the `ForwardedWrite` receiver — so no dependency
inversion: `core` and `server` already depend on `cluster`, not the reverse). It returns a
*typed* redirect; the `server` crate keeps the sole responsibility of rendering that into the
`REDIRECT`/`CLUSTERDOWN` RESP wire strings, since wire `Response` lives in `frogdb-protocol`/`server`,
not `cluster`.

```rust
// crates/cluster/src/writer.rs (new)

/// Where a non-leader should send the client, resolved from a `ForwardToLeader`.
pub struct LeaderRedirect {
    pub leader_id: Option<NodeId>,
    /// The leader's *client* address, if known — `Some` → REDIRECT, `None` → CLUSTERDOWN.
    pub leader_client_addr: Option<SocketAddr>,
}

/// Successful outcome of a propose. The leader-vs-forwarded distinction is
/// load-bearing: only the caller of a *local* commit adds the joining node to
/// the Raft voter set (`connection/cluster.rs:86`); on the forward path the
/// leader-side `ForwardedWrite` receiver already did (`network.rs:687-689`), so
/// the caller must not repeat it.
pub enum Proposed {
    /// This node was the leader and committed locally. Carries the state
    /// machine's `ClusterResponse`; the caller still inspects `Error(_)` and,
    /// on success, runs its leader-only side effects (e.g. `spawn_add_raft_voter`).
    Committed(ClusterResponse),
    /// This node was a follower; the write was forwarded and `forward_write`
    /// returned `Ok` (a clean remote commit — `network.rs:365`). The remote
    /// receiver already performed any voter-add.
    Forwarded,
}

/// Outcome of proposing a metadata write from a node that may not be the leader.
pub enum ProposeError {
    /// Not the leader and the forward did not land — `forward_write` returned
    /// `Err` for *any* reason (network failure **or** a state-machine rejection
    /// on the leader), or the leader address could not be resolved to forward at
    /// all. Preserves today's behavior: `connection/cluster.rs:120` falls through
    /// to REDIRECT on any forward failure, never to an `ERR`. Caller decides
    /// whether to surface (connection paths) or ignore and retry (bootstrap spawns).
    Redirect(LeaderRedirect),
    /// A Raft error that was **not** `ForwardToLeader` (formats as
    /// `ERR Raft error: {0}` today, `connection/cluster.rs:141`).
    Raft(String),
}

/// Minimal seam over `client_write` so the forward/redirect decision is fakeable
/// in unit tests without a live `openraft::Raft`. The blanket impl wraps the
/// concrete `ClusterRaft` alias (which cannot carry inherent methods itself).
#[async_trait]
pub trait RaftProposer: Send + Sync {
    async fn client_write(
        &self,
        cmd: ClusterCommand,
    ) -> Result<ClusterResponse, RaftClientWriteError>;
}

/// Owns propose → (forward | redirect). Wraps the three collaborators the saga
/// needs; nothing else in the write path needs them together.
pub struct ClusterWriter<R = Arc<ClusterRaft>> {
    raft: R,
    network_factory: Arc<ClusterNetworkFactory>,
    cluster_state: Arc<ClusterState>,
}

impl<R: RaftProposer> ClusterWriter<R> {
    pub fn new(
        raft: R,
        network_factory: Arc<ClusterNetworkFactory>,
        cluster_state: Arc<ClusterState>,
    ) -> Self;

    /// Propose on the leader, or forward to it.
    /// - Leader commit → `Ok(Proposed::Committed(resp))`; caller inspects
    ///   `resp` for `Error(_)` and runs its leader-only side effects (typed-error
    ///   folding is proposal 32's boundary).
    /// - Follower, forward succeeds → `Ok(Proposed::Forwarded)`; the remote
    ///   receiver already added any voter, so the caller must not.
    /// - Follower, forward fails or unresolvable → `Err(Redirect(..))`.
    /// - Non-`ForwardToLeader` Raft error → `Err(Raft(..))`.
    pub async fn propose(&self, cmd: ClusterCommand) -> Result<Proposed, ProposeError>;
}

/// Pure, collaborator-only resolution of the *redirect fallback* — unit-testable
/// with no async Raft and no network factory. Given the `leader_id` decoded from a
/// `ForwardToLeader` plus the `ClusterState` client-address lookup, produce the
/// `LeaderRedirect` (`Some` client addr → REDIRECT, `None` → CLUSTERDOWN). It does
/// **not** decide the forward target: the cluster-bus forward address comes from
/// `network_factory.get_node_addr` and is consumed by `propose` before it ever
/// reaches this fallback, so the factory is intentionally not a parameter here.
pub fn resolve_redirect(
    leader_id: Option<NodeId>,
    cluster_state: &ClusterState,
) -> LeaderRedirect;
```

Server-side, the wire rendering lives in one place (e.g. an extension trait or free function in
`server`), replacing the two copies of the format strings:

```rust
// crates/server/src/connection/cluster.rs (or a small shared helper)
fn redirect_to_response(r: LeaderRedirect) -> Response {
    match (r.leader_id, r.leader_client_addr) {
        (Some(id), Some(addr)) => Response::error(format!("REDIRECT {id} {addr}")),
        (id, _)                => Response::error(format!("CLUSTERDOWN No leader available: {id:?}")),
    }
}
```

Call sites collapse to their genuine policy. The four-arm match makes the asymmetric side effects
explicit instead of buried in the current success block:

```rust
// handle_raft_command
match writer.propose(cmd).await {
    Ok(Proposed::Committed(resp)) => {
        if let ClusterResponse::Error(msg) = &resp.data {
            return Response::error(format!("ERR {msg}"));
        }
        // leader-local commit → register AND add voter (today's L79-88)
        if let Some((id, addr)) = register_node { factory.register_node(id, addr);
                                                   spawn_add_raft_voter(raft.clone(), id, addr); }
        if let Some(id) = unregister_node { factory.remove_node(id); }
        Response::ok()
    }
    Ok(Proposed::Forwarded) => {
        // forwarded → register ONLY; voter-add happened on the leader (today's L107-117)
        if let Some((id, addr)) = register_node { factory.register_node(id, addr); }
        if let Some(id) = unregister_node { factory.remove_node(id); }
        Response::ok()
    }
    Err(Redirect(r)) => redirect_to_response(r),
    Err(Raft(e))     => Response::error(format!("ERR Raft error: {e}")),
}
```

  The register/unregister side effects stay in the caller (they are connection-layer concerns, not
  writer concerns); the `Committed` arm additionally runs `spawn_add_raft_voter` exactly as today's
  L86, while the `Forwarded` arm deliberately omits it (L107-117). This is the behavior the flat
  `Ok(resp)` sketch would have lost.
- **`commit`** (slot migration) — the same match, but slot-migration commits carry **no**
  register/voter side effects, so both `Committed(resp)` and `Forwarded` collapse to "check `Error`
  where applicable, then `Response::ok()`"; `begin`/`complete`/`cancel` unchanged.
- **The two bootstrap spawns** — call `writer.propose` in the retry loop, treat both `Committed`
  and `Forwarded` as success (their AddNode voter-add is handled by the receiver / by
  `handle_raft_command`'s own path, not here) and `Err(Redirect(_))` as "retry", making the
  *drop-the-redirect* choice explicit rather than an emergent consequence of not writing the branch.

## Migration plan (ordered steps)

1. **Add the seam without callers.** Introduce `writer.rs` in `cluster`: `LeaderRedirect`,
   `Proposed`, `ProposeError`, `RaftProposer` (+ blanket impl for `Arc<ClusterRaft>` forwarding to
   `client_write`), `resolve_redirect`, and `ClusterWriter::{new, propose}`. Lift the redirect
   resolution out of `connection/cluster.rs` verbatim so behavior is provably identical. Land with
   its own unit tests (see Test plan) — no production call site changed yet.
2. **Cut over `slot_migration::commit`.** It is the smallest full-saga site and already wraps
   `raft + network_factory + cluster_state` (`SlotMigrationCoordinator` fields, `mod.rs:56-59`).
   Construct a `ClusterWriter` from those three and replace `commit`'s body with `propose` +
   `redirect_to_response`. Delete the local `cmd_clone`.
3. **Cut over `handle_raft_command`.** Replace the forward/redirect arm; keep the register/unregister
   side effects in the caller, matching `propose`'s outcome to today's asymmetry — `spawn_add_raft_voter`
   runs only on the `Committed` arm (today's L86), never on `Forwarded` (today's L107-117). Route
   `handle_reset_command`
   through `propose` too (it is propose-and-check with no forward — `propose` still returns
   `Ok(Committed(resp))` on the leader and the reset path simply never expects `Redirect` for a local
   reset;
   confirm and, if reset is always leader-local, leave it on raw `client_write` to avoid behavior
   change).
4. **Cut over the two `cluster_init` spawns.** Use `propose` inside the retry loop, mapping
   `Err(Redirect(_))` → continue. This removes the `fwd_cmd` rebuild and the `cmd.clone()`.
5. **Assess the non-forwarding sites (optional, behavior-preserving only).** `failure_detector`'s
   three sites and `dispatch_cluster.rs` today do **not** forward; routing them through `propose`
   would change behavior (a follower's detection could forward to the leader). Keep them out of this
   pass; note that `ClusterWriter` makes opt-in forwarding a one-line change if desired later. The
   `ForwardedWrite` receiver (`network.rs:680`) is terminal and must **not** forward — leave it.
6. **Delete the duplicated format strings and the per-site clones.** Grep-confirm `"REDIRECT {"` and
   `"CLUSTERDOWN No leader"` now appear in exactly one non-test location.

Each step is independently compilable and testable; the saga is removed one site at a time.

## Test plan

- **New synchronous unit tests on `resolve_redirect`** (no async, no live Raft), covering the fork
  that today needs a multi-node cluster:
  - leader known + client addr resolvable → `LeaderRedirect { leader_id: Some, leader_client_addr:
    Some }` (→ `REDIRECT`).
  - leader known + addr **not** in `cluster_state` → `leader_client_addr: None` (→ `CLUSTERDOWN`).
  - `leader_id: None` → `CLUSTERDOWN`.
- **`ClusterWriter::propose` unit tests with a fake `RaftProposer`**, stubbing every outcome and
  asserting the leader-vs-forwarded discrimination that the whole design turns on:
  `Ok(ClusterResponse::Ok)` → `Ok(Committed(Ok))`; `Ok(ClusterResponse::Error(m))` →
  `Ok(Committed(Error(m)))` (caller still inspects); `Err(ForwardToLeader)` with a fake network
  factory whose `forward_write` **succeeds** → `Ok(Forwarded)` (and the test asserts the writer does
  **not** itself add a voter); same but forward **fails** → `Err(Redirect(..))`; non-forward Raft
  error → `Err(Raft(..))`. This is the coverage that is impossible today — it requires the
  `RaftProposer` trait seam (`ClusterNetworkFactory` may need a lightweight test constructor or a
  thin trait for `get_node_addr`/`connect`; note as an enabler, since it is a concrete struct today).
- **Server-side wire rendering test** for `redirect_to_response`: pin the exact `REDIRECT {id}
  {addr}` and `CLUSTERDOWN No leader available: {id:?}` bytes so the string move is provably lossless
  (guards the two former copies collapsing to one).
- **Keep every existing integration test green**: `integration_cluster.rs:4621,8115,8322` and the
  harness `REDIRECT` handling (`cluster_harness.rs:543`) now exercise the single seam end-to-end
  instead of four copies — the behavioral contract is unchanged, so they are the regression guard for
  the cutover.

## Risks & alternatives

- **`ClusterRaft` cannot carry `propose` directly.** It is `Raft<TypeConfig>`, a foreign-ish concrete
  alias; the candidate's `ClusterRaft::propose(cmd)` phrasing is not implementable as an inherent
  method. The `ClusterWriter` wrapper (the candidate's parenthetical alternative) is the correct
  shape and the one specified above.
- **Testability requires a real seam, not a free win.** Because `ClusterNetworkFactory` and
  `ClusterRaft` are concrete, the unit-test win depends on introducing `RaftProposer` (and possibly a
  thin abstraction over the two factory lookups). This is a genuine, bounded cost — one small trait —
  not the zero-cost "fake network factory + stubbed ForwardToLeader" the candidate implied. The
  `resolve_redirect` pure function is testable immediately regardless.
- **`ClusterResponse::Error` inspection stays with the caller.** `propose` deliberately carries the
  `ClusterResponse` in `Proposed::Committed` rather than folding `Error(_)` into `ProposeError`,
  because callers diverge on it (some `ERR {msg}`, some warn-and-return) and the `Committed` path also
  carries register/voter side effects that must run *after* a successful commit. Folding the
  string-typed error is **proposal 32's** boundary (typed cluster errors); this proposal keeps that
  seam untouched so the two land independently. If 32 lands first, `Committed` carries the typed value
  directly.
- **Alternative — `ClusterWriteRouter` returning `Response` directly.** Rejected: it would force the
  `cluster` crate to depend on `frogdb-protocol`'s `Response` and hard-code the RESP wire format,
  pulling wire concerns below the metadata layer. Returning a typed `LeaderRedirect` keeps the wire
  rendering in `server` where it belongs.
- **Alternative — leave the bootstrap spawns on raw `client_write`.** They drop the redirect anyway,
  so the win there is only de-duplication of the forward leg. Included because it also removes the
  `fwd_cmd` rebuild / `cmd.clone()` divergence, but it is the lowest-value site and can be deferred.
- **ADR-0001 untouched.** This restructures only *how* a metadata write reaches the leader; the
  Raft-owns-metadata / data-path-never-through-Raft decision is unchanged. The **Config Epoch** and
  role-change semantics are entirely inside the state machine, below this seam.
- **Scope discipline.** The six non-forwarding / terminal propose sites (`connection/cluster.rs:201`
  reset, the three `failure_detector` sites, `dispatch_cluster.rs:14`, and the terminal
  `network.rs:680` receiver) are catalogued to show the true blast radius, but only the four
  forwarding sites (plus the reset site's assessment) are in the migration.
  Sweeping `failure_detector` and `dispatch_cluster` into `propose` is a behavior change and is
  explicitly out of scope.

## Effort

**M.** One new file in `cluster` (~a small trait + wrapper + pure resolver), cutover of two full-saga
callers and two bootstrap spawns, and one server-side rendering helper. Compiler-guided (every
migrated site errors until updated) and confined to `cluster` + `server`. Not S because it
introduces a testability seam (`RaftProposer`) and touches four async call sites across two crates
plus their tests; not L because there is no cross-crate dependency change, no state-machine or
wire-format behavior change, and the existing integration suite pins the contract.

## Related

- **Proposal 32 (typed cluster errors — same boundary).** `propose` returns
  `Ok(Proposed::Committed(ClusterResponse))` with the string-typed `Error(String)` intact; 32 replaces
  that with a typed error. The two share
  the metadata-write boundary and compose: land 31's structural seam, then 32 sharpens its `Ok`-arm
  type. Sequencing either way works; doing 31 first gives 32 a single call site to retype instead of
  the ten scattered `ClusterResponse::Error` inspections.
- **Proposal 10 (apply owns event derivation)** operates on the *apply* side of the same state
  machine; 31 operates on the *propose* side. Together they bracket the Raft Metadata Plane's write
  path with deep modules at both ends.

## Adversarial review

**Verdict: AMEND** (premise sound, evidence accurate; interface had to carry the leader-vs-forwarded
outcome before it was safe). All three issues verified against the source and resolved.

- **[major] Success arm is asymmetric; the flat `Ok(ClusterResponse)` sketch would regress voter
  promotion.** *Confirmed* by reading `connection/cluster.rs:79-119` and `network.rs:672-694`:
  leader-commit runs `register_node` **and** `spawn_add_raft_voter` (L86 — the only voter-add on the
  pure-leader path), while forward-success runs `register_node` only (the leader-side `ForwardedWrite`
  receiver adds the voter at `network.rs:687-689`). Also confirmed via `forward_write`
  (`network.rs:362-373`) that `Ok` means a clean remote commit and any failure (network *or*
  state-machine rejection) falls through to REDIRECT today (`connection/cluster.rs:120`).
  **Resolved:** `propose` now returns `Result<Proposed, ProposeError>` with
  `Proposed::{Committed(ClusterResponse), Forwarded}`; a new Problem subsection ("The two success
  paths are asymmetric") documents the evidence; the call-site sketch, migration step 3, and the
  propose test plan now assert `spawn_add_raft_voter` runs only on `Committed`. `ProposeError::Raft`
  was also tightened to *not* claim a forwarded state-machine rejection (that path preserves today's
  fall-through-to-REDIRECT, not `ERR`).
- **[minor] Count inconsistency ("eight" vs the ten-row table).** *Confirmed* — the grep yields ten
  non-trait call sites. **Resolved:** both "eight" occurrences (Summary and Problem) changed to
  "ten"; now consistent with the table and the later "ten sites" figure.
- **[minor] `resolve_redirect` signature muddled — `network_factory` is superfluous and it does not
  return the forward target it advertised.** *Confirmed.* **Resolved:** dropped `network_factory`
  from the signature (it now takes only `leader_id` + `cluster_state`), rewrote its doc to state it
  computes the redirect *fallback* only, and tempered the "large testability win" headline in the
  shallow-analysis section to distinguish the free pure-function win from the higher-value
  forward-vs-redirect branch that still costs the `RaftProposer` trait plus a `ClusterNetworkFactory`
  fake (already disclosed in Risks).
