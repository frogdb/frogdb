# Proposal 57 — one Raft-RPC error mapping instead of three

Round 38 · lane: cluster · effort **S** · LOCKED area (**cluster**, mutation gate 0.80,
spec `.scratch/hardening/specs/cluster-failure-modes.md`)

Covers exploration-lane candidate **RC5** ("RaftNetwork 3 copies of error mapping + dead
no-op matches"). Verified against the current tree at HEAD `9a62f79b`; every citation below
was read, not inherited. The lane brief was written against `08c143d6` — `network.rs` has
since grown by 503 lines (`da1496fd`, `7f0c04dc`, `9cda642b`, all landing **after** `:660`),
so RC5's own citations (`:574-656`, `:502`, `:517-520`) still hold exactly while RC6's
(`:687-741`) no longer do. See [Boundaries](#risks--scope-boundaries-vs-siblings).

## Summary

`impl RaftNetwork<TypeConfig> for ClusterNetwork` (`network.rs:573-657`) has three methods —
`append_entries`, `vote`, `install_snapshot` — and each one ends in the **byte-identical**
nine-line error mapping (`:590-598` == `:616-624` == `:645-653`, verified equal character for
character). The three methods differ only in which request they build and which response
variant they accept; everything else, including the entire decision about *what a failed
consensus RPC means to Raft*, is written out three times.

That decision is not incidental plumbing. openraft's `RPCError` is a five-way classification
and the caller of `RaftNetwork` acts on which arm it gets: `RPCError::Unreachable` is the
**only** trigger that arms `RaftNetwork::backoff()` (openraft `replication/mod.rs:288-294`),
and `RPCError::RemoteError` is the only arm that lets the chunked snapshot transport recognise
a `SnapshotMismatch` and rewind to offset 0 (`network/snapshot_transport.rs:184-189`). All
three copies collapse every outcome onto `RPCError::Network` — the arm openraft documents as
"failed to send the RPC request and should retry immediately" (`error.rs:287-289`). The
collapse is invisible at each site because each site is only nine lines; it is only visible
when you notice that the same nine lines exist three times and that no other arm is ever
constructed anywhere in the repository.

The proposed change is the one the sibling module already made and the spec already blessed:
`send_pubsub_rpc` (`cluster-runtime/src/pubsub.rs:48-89`) is a single owner of exactly this
mapping for pub/sub RPCs, generic over the RPC future so it is unit-testable with a plain
`async` block, with per-caller `extract` function pointers — and FM-CLUSTER-068 pins the
resulting distinction as a correctness property ("A peer bug rendered as `Ok(0)` … collapsing
the first into the second makes it invisible", spec `:1006`). Proposal 57 applies that exact
shape to the Raft side: `map_raft_rpc(rpc, extract) -> Result<T, RPCError<…>>`, three
three-line methods, three extractors.

**Live-bug claims.** The refactor itself claims **none** — it is a pure extraction with
identical behaviour. Two *classification* defects are proven by code reading and labelled
honestly below: openraft's backoff is unreachable dead code in FrogDB (**live mechanism,
unmeasured symptom**), and `InstallSnapshotError::SnapshotMismatch` can never reach the
transport that handles it (**latent** — reachable only for a cluster snapshot exceeding the
3 MiB chunk size). Neither is fixed by this proposal: both are behaviour changes in a LOCKED
area, so they are **spec-first** follow-ons, and this proposal's contribution is to create the
single site where such a fix is one edit rather than three. The dead-match cleanup is carved
out as an independently-landable hotfix.

## Files involved

| path | lines | role in this proposal |
|---|---:|---|
| `frogdb-server/crates/cluster/src/network.rs` | 1877 | **the change.** `impl RaftNetwork` `:573-657` (`append_entries` `:574-601`, `vote` `:603-627`, `install_snapshot` `:629-656`; the triplicated arms `:590-598`/`:616-624`/`:645-653`); `NetworkErrorWrapper` `:89-98`; `send_rpc` `:438-453`; `send_rpc_pooled` `:461-504` (redundant arm `:502`); `send_rpc_oneshot` `:507-521` (identity match `:517-520`); `try_send_on_framed` `:528-561`; `open_framed_connection` `:564-570`; `new_client` `:326-347` (pins the Raft path to *one-shot*, `pool: None` `:341`); test module `:1024-1877` |
| `frogdb-server/crates/cluster/src/types.rs` | 1313 | `ClusterError::NetworkError(String)` `:613-615` — the flat, string-typed transport outcome the mapping has to classify from |
| `frogdb-server/crates/cluster-runtime/src/pubsub.rs` | 771 | **the precedent, not a target.** `PubSubRpcError` `:38-46`, `send_pubsub_rpc` `:48-89`, `extract_broadcast_count` `:92-97`, `extract_forward_count` `:100-105` |
| `frogdb-server/crates/cluster-runtime/src/bus.rs` | 590 | **evidence, not a target.** `is_clean_disconnect` `:272-276` — a second consumer already forced to *string-match* `ClusterError::NetworkError`'s payload; `handle_rpc_request`'s caller `serve_connection` `:282-300` |
| `frogdb-server/crates/server/src/server/cluster_init.rs` | 1938 | the production `openraft::Config` `:407-412` — `heartbeat_interval` and the election window that set the RPC deadlines openraft imposes on our impl |
| `.scratch/hardening/specs/cluster-failure-modes.md` | 1573 | FM-CLUSTER-048 `:745-756`, -050 `:769-780`, -051 `:787-798`, -067 `:989-999`, -068 `:1001-1011`, -077 `:1108-1118`, -101 `:1454-1466` |
| `frogdb-server/crates/cluster-runtime/src/failure_detector.rs` | 2381 | **not touched.** Proposal 58's file (RC6, `trigger_auto_failover` + propose-retry) |
| openraft 0.9.21 (`Cargo.lock:3328-3330`) | — | `error.rs:275-293` (`RPCError` arms + their contracts), `replication/mod.rs:283-305` + `:322-330` (backoff arming), `network/snapshot_transport.rs:117-216` (chunked send loop), `network/network.rs:150-163` (`backoff()` default) |

## Problem

### 1. The mapping is written three times, byte for byte

`append_entries` (`:584-600`), with the arms that repeat:

```rust
let request = RaftRpc::AppendEntries(req).into();
let this = self.clone();
async move {
    match this.send_rpc(request).await {
        Ok(ClusterRpcResponse::AppendEntries(resp)) => Ok(resp),          // :589 — the only unique line
        Ok(ClusterRpcResponse::Error(msg)) => Err(RPCError::Network(NetworkError::new(
            &Unreachable::new(&NetworkErrorWrapper(msg)),
        ))),                                                               // :590-592
        Ok(_) => Err(RPCError::Network(NetworkError::new(&Unreachable::new(
            &NetworkErrorWrapper("unexpected response type".to_string()),
        )))),                                                              // :593-595
        Err(e) => Err(RPCError::Network(NetworkError::new(&Unreachable::new(
            &NetworkErrorWrapper(e.to_string()),
        )))),                                                              // :596-598
    }
}
```

`vote` `:610-626` and `install_snapshot` `:639-655` are the same block with `:589` swapped for
`ClusterRpcResponse::Vote(resp)` / `::InstallSnapshot(resp)` and the request constructor swapped.
Of the 85-line impl, roughly 51 lines are duplicated; 27 of them (three × nine) are literally
identical text.

Two things follow from three copies rather than one:

* **Divergence is unpoliced.** Nothing couples them — not a type, not a test, not a lint. The
  three sites are 30 lines apart and each is individually plausible.
* **The decision is invisible.** Written once with a name, "every cluster-bus failure is an
  immediate-retry network error" is a claim a reviewer would interrogate. Written three times
  as the tail of three unrelated-looking methods, it reads as boilerplate.

### 2. What the mapping throws away — the interface is narrower than the caller needs

Follow the information backwards. `send_rpc` (`:438-453`) can fail seven distinguishable ways,
and every one becomes the same `ClusterError::NetworkError(String)` (`types.rs:613-615`):

| origin | site | string |
|---|---|---|
| request serialization | `:442-443` | `serialization failed: …` |
| connect / TLS handshake | `:565-567` | `connection failed: …` |
| request write | `:538-540` | `failed to send request: …` |
| peer hung up | `:546` | `connection closed` |
| response read | `:547-549` | `failed to read response: …` |
| response decode | `:552-553` | `deserialization failed: …` |
| our own request deadline | `:557-560` | `request timeout` |

Seven causes → one variant → one `RPCError` arm. Add the eighth input — `ClusterRpcResponse::Error(msg)`,
which is a **remote** Raft error the peer's `handle_rpc_request` flattened to a display string
(`:937`, `:941`, `:945`) — and a remote fatal storage failure on the follower is reported to
the leader as a local network failure.

This is what makes the module **shallow** in the sense that matters: the interface of
`send_rpc` (one string-typed error) carries strictly less than its caller must know. The
`RaftNetwork` boundary is required to compute a four-way classification from a value that has
already discarded it, so all three sites do the only thing they can — give up identically. The
triplication is the *symptom*; the narrow return type is the cause. A sibling module already
paid the same tax visibly: `bus.rs:272-276` classifies the very same errors by
`error_msg.contains("connection closed")`, which is precisely the string-matching
FM-CLUSTER-050's Invariant (spec `:775`) rules out for Raft errors one layer up ("the match is
on the specific `ClientWriteError` variant … rather than being classified by string matching").

Two concrete consequences of the collapse, each labelled:

**(a) openraft's backoff is unreachable dead code — live mechanism, unmeasured symptom.**
`RPCError::Unreachable` is documented as "the node is temporarily unreachable and should
backoff before retrying" (`error.rs:279-281`); it is the sole trigger for
`self.backoff = Some(self.network.backoff())` in openraft's replication loop
(`replication/mod.rs:288-294`), which is what makes `drain_events_with_backoff` (`:322-330`)
sleep instead of driving another attempt at a peer that is not there. `RPCError::Unreachable`
is **never constructed anywhere in this repository** — a repo-wide grep finds `Unreachable`
only at `network.rs:28` (the import) and inside the three copies, where it appears as the
*source error* of a `NetworkError`, never as the `RPCError` arm. `ClusterNetwork` also does not
override `backoff()`, so openraft's default 500 ms constant (`network/network.rs:161-163`) is
defined and unreachable. The observable difference in 0.9.21 is not a hot spin — both
`Unreachable` and `Network` set `retry = false` (`:288`, `:304`) — it is that a leader
replicating to a down follower gets no paced idling: each replication event drives another
one-shot connect (`new_client:341` pins `pool: None`) instead of the 500 ms-spaced attempts
openraft intends, and the "backoff resets on the first success" semantics (`:249-250`) never
apply. Mechanism proven by code; production symptom not measured — treat as latent until a
test witnesses it.

**(b) `SnapshotMismatch` can never be recognised — latent, with a stated precondition.**
`Chunked::send_snapshot` handles a `RemoteError` carrying
`InstallSnapshotError::SnapshotMismatch` by resetting `offset = 0` and restarting the transfer
(`snapshot_transport.rs:184-189`); every other `RPCError` arm falls through to a bare
`continue` (`:172-176`, `:197`) that re-sends **the same chunk at the same offset** after a
1 ms sleep (`:125-126`). The receiver raises `SnapshotMismatch` when it has no streaming state
for this snapshot id and `req.offset != 0` (`:232-245`) — a follower that restarted
mid-transfer. On our transport that error is flattened to a string at `network.rs:945` and
mapped to `RPCError::Network` at `:645-647`, so the rewind arm is unreachable and the loop
retries a permanently-rejected offset. Reachability precondition: `req.offset != 0` requires a
**second** chunk, and `snapshot_max_chunk_size` defaults to 3 MiB (openraft `config.rs:174-175`;
FrogDB takes `..Default::default()` at `cluster_init.rs:411`). A serialized `ClusterStateInner`
is far below that for any realistic topology, so this is **latent**, not live — but it is a
livelock rather than a slowdown when it does arrive, and it is invisible to every current test.

### 3. Two dead matches on the same path — and RC5's characterisation needs one correction

`send_rpc_oneshot` (`:507-521`) is the path **every Raft RPC takes** (`new_client:338-346`
constructs the handle with `pool: None`). Its tail is a pure identity:

```rust
let result =
    Self::try_send_on_framed(&mut framed, &request_bytes, timeout, &self.bus_stats).await;

match result {                    // :517-520
    Ok(r) => Ok(r),
    Err(e) => Err(e),
}
```

`send_rpc_pooled:496-503` is **not** the same thing, and the lane brief's "dead no-op matches
(`:502`, `:517-520`)" over-states it: that match's `Ok` arm carries a real side effect
(`*slot.lock().await = Some(framed)` — returning the healthy connection to the pool), so only
the `Err(e) => Err(e)` arm at `:502` is redundant. The correct rewrite is `let response =
…await?; *slot.lock().await = Some(framed); Ok(response)`, not a deletion of the match.

If the tree lints clean under `cargo clippy --all-targets -- -D warnings` (`Justfile:319-320`),
then `clippy::needless_match` is not catching the `:517-520` shape, and no existing gate will.

### 4. None of this is tested, and the crate is gated at 0.80

`impl RaftNetwork` has **zero tests**. Grepping the 853-line test module (`:1024-1877`) for
`append_entries`, `vote(`, `install_snapshot`, `RaftNetwork` or `RPCError` returns nothing;
the repo-wide grep for `RaftNetwork` returns only `network.rs` itself. `send_rpc_oneshot` is
likewise untested in-crate — the one test that forces a failed connect,
`a_connection_that_never_opens_counts_nothing` (`:1861-1876`, tagged FM-CLUSTER-077), goes
through `factory.connect(…)` and therefore exercises the **pooled** path (`:494`), not the
one-shot path Raft uses.

Whatever coverage exists is end-to-end (turmoil/jepsen raft workloads in `frogdb-server`), and
`cargo mutants -p frogdb-cluster` runs only that package's own tests — the exact trap CLAUDE.md
names. So the 27 identical lines contribute mutants three times over to a crate held to a 0.80
gate, with nothing in-crate able to kill them.

## Proposed change

One function, three extractors, three three-line methods. Same **module** (`network.rs`), no
new file, no change to the `try_send_on_framed` **seam** or to any wire byte.

```rust
/// The one place a cluster-bus RPC outcome becomes an openraft `RPCError`.
///
/// Generic over the RPC future rather than over a mocked network type, so the
/// mapping is unit-testable with a plain `async` block and no socket — the same
/// shape `send_pubsub_rpc` uses for the bus-local RPCs.
///
/// Every failure is currently reported as `RPCError::Network` ("retry
/// immediately"). That is a deliberate, and lossy, single decision: `send_rpc`
/// returns one string-typed `ClusterError::NetworkError` for causes as different
/// as a refused connect and a remote state-machine error, and openraft's
/// `Unreachable` / `Timeout` / `RemoteError` arms cannot be recomputed from a
/// string without the classification-by-string FM-CLUSTER-050 rules out. See
/// issue <N> for the typed-transport work that would let this classify.
async fn map_raft_rpc<F, T, E>(
    rpc: F,
    extract: fn(ClusterRpcResponse) -> Option<T>,
) -> Result<T, RPCError<NodeId, BasicNode, RaftError<NodeId, E>>>
where
    F: Future<Output = Result<ClusterRpcResponse, ClusterError>>,
    E: std::error::Error,
{
    let network_error = |msg: String| {
        RPCError::Network(NetworkError::new(&Unreachable::new(&NetworkErrorWrapper(msg))))
    };
    match rpc.await {
        // Kept as its own arm ahead of `extract`: a peer that answered with an
        // explicit error carries a reason, and folding it into the shape-mismatch
        // arm below would replace that reason with "unexpected response type".
        Ok(ClusterRpcResponse::Error(msg)) => Err(network_error(msg)),
        Ok(response) => extract(response)
            .ok_or_else(|| network_error("unexpected response type".to_string())),
        Err(e) => Err(network_error(e.to_string())),
    }
}

fn extract_append_entries(r: ClusterRpcResponse) -> Option<AppendEntriesResponse<NodeId>> {
    match r {
        ClusterRpcResponse::AppendEntries(resp) => Some(resp),
        _ => None,
    }
}
// … extract_vote, extract_install_snapshot — the shape of pubsub.rs:92-105
```

Each method becomes its signature plus:

```rust
fn append_entries(&mut self, req: AppendEntriesRequest<TypeConfig>, _option: RPCOption)
    -> impl Future<Output = Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>> + Send
{
    let this = self.clone();
    map_raft_rpc(async move { this.send_rpc(RaftRpc::AppendEntries(req).into()).await },
                 extract_append_entries)
}
```

Type-level notes for the implementer, checked against openraft 0.9.21:

* `RPCError<NID, N, E>` requires `E: Error` (`error.rs:275`) and `RaftError<NID, E>` is
  `thiserror`-derived with `E = Infallible` by default (`error.rs:35-45`). One bound,
  `E: std::error::Error`, covers both call shapes — `RaftError<NodeId>` for
  `append_entries`/`vote` and `RaftError<NodeId, InstallSnapshotError>` for `install_snapshot`.
  `E` is otherwise unused in the body, which is exactly the point: the mapping never
  constructs an API error.
* The methods stay non-`async` returning `impl Future + Send`; they build and return the
  helper's future. `ClusterNetwork` is `Clone` over `Arc` fields, so the `'static` + `Send`
  bounds hold as they do today.
* Arm order is load-bearing: `ClusterRpcResponse::Error(msg)` must precede the `extract` arm,
  or the peer's reason is replaced by `"unexpected response type"`. Today's three copies get
  this right; the comment above records *why* so the single copy keeps getting it right.

**Behaviour is identical.** Same `RPCError` arm, same `NetworkErrorWrapper` payloads, same
strings, same order. The proposal is scoped to *unification only* precisely because the
classification fix is a behaviour change in a LOCKED area and therefore spec-first.

### Why this is depth, not a wrapper

The **deletion test**: delete `map_raft_rpc` and "what a cluster-bus failure means to Raft"
must be re-decided at every `RaftNetwork` method — three today, and a fourth the moment
FrogDB overrides `full_snapshot` or `backoff` (the two remaining hooks on the trait, both of
which are on the table the day (a) or (b) is fixed). That is exactly today's state, and it is
why the decision is undocumented: there is no one place for the doc comment to live.

The **interface** shrinks to what a caller must actually know — "hand me the RPC and the
response shape you expect" — while the **implementation** hides the arm choice, the wrapper
type, the arm ordering, and the loss of classification. **Leverage:** 3 callers now, plus the
two unimplemented trait hooks, plus every future response variant. **Locality:** the mutants
that model this decision concentrate in one 15-line body instead of being spread across three
methods, and the one place a future typed-transport fix has to edit is one place.

The precedent is decisive and in-tree: `send_pubsub_rpc` (`pubsub.rs:48-89`) already made this
exact move for the bus-local RPCs, with the same generic-over-the-future testability argument
written into its own doc comment (`:56-57`), the same `extract: fn(&ClusterRpcResponse) -> Option<…>`
parameter, and the same "was previously spelled out by hand at each site" motivation
(`:50-54`). The codebase has already decided this shape is right for cluster-bus RPCs and
applied it to one of the two families.

## Testability improvement

From zero in-crate tests to a table-driven one, because the extraction makes a test *possible*
without a socket.

* **The mapping becomes unit-testable.** Generic over the RPC future means a test hands
  `map_raft_rpc` an `async { Err(ClusterError::NetworkError("connection failed: …".into())) }`
  or an `async { Ok(ClusterRpcResponse::Error("no leader".into())) }` and asserts the arm and
  the message. No duplex stream, no factory, no peer task. That is the property
  `send_pubsub_rpc`'s doc comment claims for the pub/sub side and that FM-CLUSTER-068's four
  forcing tests (`test_rpc_expected_shape_yields_count`,
  `test_rpc_shape_mismatch_is_distinguishable_not_zero`,
  `test_rpc_transport_error_maps_to_rpc_variant`,
  `test_forward_extractor_matches_only_forward_results`, spec `:1010`) cash in. The Raft-side
  equivalents are the same four assertions.
* **In the mutated crate.** The new tests go in `network.rs`'s own `#[cfg(test)] mod tests`, so
  they count for `cargo mutants -p frogdb-cluster` against the 0.80 gate. This matters here
  specifically: FM-CLUSTER-051's `Forced by` list already leans on
  `the_bus_serves_probes_and_raft_rpcs_on_one_connection`, which lives in
  `cluster-runtime/src/bus.rs:525` and therefore contributes nothing to `frogdb-cluster`'s
  score. Do not repeat that shape for this row.
* **One mutant set instead of three.** The 27 identical lines currently mint three copies of
  every arm-deletion and message mutant, each needing its own killer and today getting none.
  After extraction there is one set, killed by the table test. Note for the implementer: the
  mutant *population* shrinks, and a score is a ratio — if `mutants-diff` moves the number at
  all, run the full `just mutants frogdb-cluster` + `just mutants-gate frogdb-cluster 0.80`
  rather than reasoning from the diff run.
* **The extractors are individually forceable.** `extract_append_entries` handed a
  `ClusterRpcResponse::Vote(_)` must return `None` — the "an answer to another question is not
  an answer" assertion `a_forwarded_write_reports_the_leaders_verdict` (`:1430-1439`) already
  makes for `forward_write` and that no test makes for the consensus trio.
* **A regression test for the collapse itself.** Even without fixing (a)/(b), a test asserting
  that a peer's explicit `ClusterRpcResponse::Error` currently surfaces as `RPCError::Network`
  *with the peer's reason preserved* pins the behaviour a future typed-transport change must
  deliberately break, rather than silently break.

## Risks / scope boundaries vs siblings

### LOCKED-area discipline (cluster, gate 0.80)

* **Spec rows touching the target lines: none.** The spec has zero occurrences of `RPCError`,
  `RaftNetwork`, `Unreachable`, or `send_rpc`, and its two `network.rs` citations are
  FM-CLUSTER-077's Invariant (`:1115`, the counting seams) and FM-CLUSTER-101's Invariant
  (`:1461`, the voter-change commit sites) — neither reaches `:461-657`. The rows that *do*
  live in this file and must stay green:

  | row | what it owns | overlap with `:461-657` |
  |---|---|---|
  | FM-CLUSTER-051 `:787-798` | the `Raft`/`Bus` envelope split + the address registry; 7 of its 9 forcing tests are in `network.rs`'s test module | **adjacent, not overlapping.** It pins the request *types*, not the failure mapping. Nearest row, and the natural place to name the new tests if a new row is judged excessive |
  | FM-CLUSTER-077 `:1108-1118` | "counting happens at the four wire seams and nowhere else", one of which is `try_send_on_framed` | **the constraint on the hotfix.** `record_sent`/`record_received` (`:541`, `:550`) must not move, and the `open_framed_connection` failure must keep counting nothing. Forced in-crate by `a_connection_that_never_opens_counts_nothing` (`:1861`) |
  | FM-CLUSTER-048 `:745-756` | `forward_write` (`:424-435`) — same `send_rpc` chain, different method | untouched; `a_forwarded_write_reports_the_leaders_verdict` (`:1414`) must stay green |
  | FM-CLUSTER-101 `:1454-1466` | `voter_change` / `spawn_*_raft_voter` (`:669-926`) | untouched — and see the proposal-58 edge below |
  | FM-CLUSTER-045 `:709-718`, -100 `:1441-1452`, -017 | `install_snapshot` — **the `RaftStateMachine` method in `state.rs:787-797`**, not `RaftNetwork`'s | **disambiguation only.** Same name, different trait, different file. Do not read these rows as covering this code |

* **`just lint-failure-modes` is name-keyed and path-agnostic** (`scripts/failure-modes.py:5-24`):
  it parses `Forced by` cells ↔ `// FM-<AREA>-NNN` tags bidirectionally and nothing else.
  The extraction moves no tagged test and renames none, so the lint is unaffected by the
  refactor. **Invariant prose is never parsed**, so if the implementer adds the sentence this
  code deserves — "`map_raft_rpc` is the single owner of the openraft error mapping" — to
  FM-CLUSTER-051's Invariant, the lint will not check it and it will not check the lint.
  **Flag for human review**, per the standing rule.
* **New row vs. extended row.** Adding forcing tests to FM-CLUSTER-051 is additive and safe.
  Fixing (a) or (b) is *not*: both change what a caller of `RaftNetwork` observes, so both are
  spec-first — failure-mode row → failing test → fix — and neither belongs in this S refactor.
  Recommended row shape for the follow-on, for whoever files it: *"a consensus RPC failure is
  classified as what it was, not as one arm"*, NOT observable = "a remote state-machine error
  reported to the leader as a local network failure" and "a snapshot chunk retried forever at
  an offset the receiver has already refused", with the typed-transport change (replace
  `ClusterError::NetworkError(String)` on this path with a variant carrying the cause) as its
  Invariant. That change also removes the reason `bus.rs:272-276` string-matches, so file it
  with `is_clean_disconnect` in scope — and note that its own forcing test,
  `every_disconnect_phrase_ends_the_connection_quietly` (`bus.rs:496`), carries no FM tag today
  and is therefore invisible to the gate.
* **Push discipline:** `just mutants-diff frogdb-cluster` before pushing; full run + gate if the
  ratio moves (see Testability).

### Prior rulings — checked, nothing re-proposed

`.scratch/cluster-correctness/` (13 issues implemented, rulings 14-20/23 pending) contains **no
ruling on the Raft network error mapping**. The only campaign citation into `network.rs` is
open issue 20 (`20-force-failover-evicts-the-old-primary-from-raft…`, `:19`), which cites
`network.rs:719-725` — the `Failover` arm of `voter_change`, ~60 lines past this proposal's
last target line. `install_snapshot` appears in issues 02 and 04 and in `PRD.md:102`, but every
occurrence is the **state-machine** restore seam (`RaftStateMachine::install_snapshot`,
`from_snapshot`, `attach_snapshot_store`), not the network method.

### vs siblings 53 / 54 / 55 / 56 — no edge

53 (`FullSyncEmitter`), 54 (replica connection + handler wiring) and 55 (`adopt_full_sync`) are
all **replication**-side, in `frogdb-replication` under the 0.85 gate, in
`replica_session.rs` / `replica/connection.rs` / `replica/mod.rs`. 56 (RC4, `psync` prefix
chain) is not yet on disk and is replication-side by the lane's own description. This proposal
is entirely inside `frogdb-cluster` (`network.rs`), a different crate, a different gate, a
different spec file. **Verified: the Raft code is cluster-side** — `frogdb-replication` contains
no `RaftNetwork`, no `RPCError`, and no openraft dependency on this path. No shared symbol, no
shared spec row, no ordering constraint.

### vs proposal 58 (RC6, auto-failover) — same file, disjoint region, one stale citation to fix

58 owns `trigger_auto_failover` and the propose-retry loops: `failure_detector.rs:517-659` and
`:461-511`, plus **`network.rs`'s retry loops** — which at the lane's base `08c143d6` were at
`:687-741` and are now at `spawn_add_raft_voter:777-829` and `spawn_remove_raft_voter:878-926`
(the file grew by 503 lines below `:660`; `git diff 08c143d6..HEAD` shows the first hunk at
`@@ -660,6 +660,106 @@`). Two things follow:

* **58 must re-derive its `network.rs` citations against HEAD**; `:687-741` now lands in the
  middle of `VoterChange`/`voter_change` (`:685-749`), which is *not* a retry loop.
* **The edge is real but empty of conflict.** 58's `network.rs` region is `:669-926`
  (`voter_retry_delay`, `MAX_ATTEMPTS`, the two spawn loops); 57's is `:461-657`. Disjoint, in
  that order, with `impl RaftNetwork` ending at `:657` and the server-side helpers section
  beginning at `:659`. RC5's error mapping sites are **not** propose paths: `propose` reaches
  Raft through `ClusterWriter` and `client_write`, and the only `network.rs` propose site is
  `handle_rpc_request`'s `ForwardedWrite` arm (`:947-972`), which 57 does not touch and which
  is governed by FM-CLUSTER-048/101 rather than by any `RPCError` mapping. Sequence them either
  way; land whichever is ready first and rebase the other's line numbers.
* One genuine shared surface: both proposals will want `just mutants frogdb-cluster` runs. Run
  the gate once, after both merge.

### Residual risk

Low. Mechanical extraction, identical arms, identical strings, no wire byte, no config, no
public API change (`map_raft_rpc` and the extractors are private; the three trait methods keep
their signatures). Three things for review to check: the `ClusterRpcResponse::Error` arm still
precedes the extractor arm; `_option: RPCOption` stays ignored exactly as today (openraft
already imposes its own deadline around each call — `heartbeat_interval` for `append_entries`,
`replication/mod.rs:435-437`, and `install_snapshot_timeout` for snapshot chunks,
`snapshot_transport.rs:162` — so ignoring it is defensible and is *not* in scope to change
here); and `self.clone()` still happens outside the returned future.

Out of scope, noted so the next reader does not hunt: `_connect_timeout_ms`
(`:364`, `:309`, `:342`, `:394`) is dead state — the connect timeout is baked into
`plain_tcp_connect_factory` at construction (`:65-75`) and the field is never read — and
`_target` (`:358`) is *not* dead despite its underscore (`:377`, `:467`). Cosmetic; fold into
this change only if it costs nothing. And: unlike txn / persistence / replication, **cluster
has no boundary ADR** (`adr/` holds 0001-0004; 0002 txn, 0003 persistence, 0004
replication-runtime), so there is no ADR cost paragraph to amend here — the spec is the whole
boundary.

## Effort

**S.** One 15-line generic helper, three four-line extractors, three method bodies reduced to
three lines each, one table-driven test with four cases, one `Forced by` cell extended (or one
new row, if the reviewer prefers the classification statement to have its own home). Net
deletion of roughly 45 lines. No behaviour change, no spec-first work, no wire or config change.

The two classification defects are explicitly **not** in this S. Both are spec-first follow-ons
against a typed transport error; (b) additionally needs the remote error to survive the wire as
something better than a `String`, which touches `ClusterRpcResponse::Error` and is therefore a
cross-node contract change — M at least, and worth its own PRD-style ruling given
FM-CLUSTER-048's note that `ForwardedWrite` deliberately stays `Result<(), String>`
(`network.rs:959-963`).

## Independently-landable hotfix — delete the two identity matches

Zero behaviour change, no spec interaction, landable before or independently of the extraction.

**Evidence.** `send_rpc_oneshot:517-520` is a pure identity `match` — `Ok(r) => Ok(r),
Err(e) => Err(e)` — on the tail of the function, and it sits on the path **every Raft RPC
takes** (`new_client:341` pins `pool: None`). `send_rpc_pooled:502`'s `Err(e) => Err(e)` is a
redundant arm of a match whose `Ok` arm returns the connection to the pool.

**Fix.**

```rust
// send_rpc_oneshot (:507-521) — the whole tail
let mut framed = self.open_framed_connection().await?;
Self::try_send_on_framed(&mut framed, &request_bytes, timeout, &self.bus_stats).await

// send_rpc_pooled (:494-503)
let mut framed = self.open_framed_connection().await?;
let response =
    Self::try_send_on_framed(&mut framed, &request_bytes, timeout, &self.bus_stats).await?;
*slot.lock().await = Some(framed);
Ok(response)
```

**Guard.** FM-CLUSTER-077's `a_connection_that_never_opens_counts_nothing` (`:1861-1876`)
forces the pooled `open_framed_connection` failure and must stay green. It does **not** cover
`send_rpc_oneshot` — it goes through `factory.connect(…)`, which is the pooled handle — so the
one-shot rewrite is unguarded in-crate today. Land the hotfix with the one-line
`send_rpc_oneshot` test the extraction wants anyway (a duplex peer answering one
`RaftRpc::Vote`, asserting the response survives an unpooled round trip), which is the first
in-crate test the Raft transport path will have had.
