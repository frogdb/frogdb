# Proposal: Cluster Redirect Mapper

Status: proposed
Date: 2026-06-15

> **Not a re-litigation of [proposal 05](05-single-routing-decision.md).** Proposal 05 is about
> *connection-level command routing* — mapping a `ConnectionLevelOp` to the `ConnectionLevelHandler`
> that executes it (which handler runs). This proposal (17) is about *cluster slot-redirect reply
> construction* — turning a `RouteDecision` into the `MOVED` / `ASK` / `CLUSTERDOWN` wire string a
> client sees (what bytes go back on a redirect). The two seams are disjoint: 05 never touches slot
> ownership; 17 never touches handler selection. They can land in either order.

## Problem

`RouteDecision` (`slot_migration/routing.rs:21-53`) is a genuinely deep abstraction: it concentrates
the slot-ownership decision — own/migrating/importing/moved/unassigned — behind one pure function
(`route` / `route_with_snapshot`, `routing.rs:62,83`) that is unit-tested without a live cluster
(`slot_migration/tests.rs`). The decision is clean. **Converting that decision into a client reply
is not.** The `RouteDecision → Response` step is hand-rolled at five sites with three different
address renderings, and a *second* slot-routing path (`cluster_pubsub::get_slot_owner_addr`) makes
the own/moved decision again from scratch — bypassing the READONLY override and the entire migration
state machine that `route()` implements.

This is a shallow seam in the reply direction. The interface a caller must satisfy to emit a correct
redirect is "know the exact Redis wire format for MOVED, ASK, and CLUSTERDOWN; know that addresses
render as `ip:port` not `SocketAddr` Display; know that READONLY read commands serve locally on the
Moved arm but not the Unassigned arm; know that an importing target with ASKING serves locally" —
and that interface is restated, partially, at every call site. The decision module is deep; the
*projection of the decision onto the wire* leaked back out into the callers.

**Deletion test.** There is no single artifact you can delete to remove redirect-reply construction
from FrogDB. The MOVED string exists in two files, CLUSTERDOWN in three places, ASK in one helper,
and the entire own/moved decision a *second* time in `cluster_pubsub.rs`. A correct seam would make
"how do we phrase a MOVED" a one-function answer whose deletion breaks every redirect at once. Today
deleting `guards.rs`'s formatting leaves `pubsub.rs` redirecting (differently), and deleting
`get_slot_owner_addr` leaves `guards.rs` untouched. The construction has no home.

### Evidence: redirect-construction sites and their drift

All paths under `frogdb-server/crates/server/src/`.

| Site | Redirect | Source of decision | Address rendering |
|------|----------|--------------------|-------------------|
| `connection/guards.rs:265-270` | `MOVED {slot} {ip}:{port}` | `RouteDecision::Moved` | `owner_addr.ip()` + `.port()` (explicit) |
| `connection/guards.rs:271-275` | `CLUSTERDOWN Hash slot {slot} not served` | `RouteDecision::Moved { addr: None }` | — |
| `connection/guards.rs:277-280` | `CLUSTERDOWN Hash slot {slot} not served` | `RouteDecision::Unassigned` | — |
| `connection/guards.rs:417-419` | `ASK {slot} {ip}:{port}` | `migrating_ask_for_nil` / `check_migrating_multikey` | `addr.ip()` + `.port()` (explicit) |
| `connection/handlers/pubsub.rs:289` | `MOVED {slot} {addr}` | `cluster_pubsub::get_slot_owner_addr` | `SocketAddr` **Display** (`{}`) |

Two independent drifts are visible in the table:

1. **Format drift (MOVED).** `guards.rs:266` renders the owner address as `{}:{}` from `ip()` and
   `port()`; `pubsub.rs:289` renders the whole `SocketAddr` with `Display` (`{}`). These agree for
   IPv4 but **diverge for IPv6**: `SocketAddr`'s `Display` brackets the host (`[2001:db8::1]:6379`)
   while the explicit `ip():port()` form does not (`2001:db8::1:6379`). Two phrasings of the same
   reply, one of which a client cannot parse unambiguously.

2. **Decision drift (the second routing path).** `guards.rs` derives its redirect from
   `coordinator.route(...)`, which honors ASKING, the importing-target `AcceptImporting` case, the
   MIGRATING source case, and the READONLY override (`guards.rs:251-262`). `pubsub.rs:289` derives
   its redirect from `get_slot_owner_addr` (`cluster_pubsub.rs:160-179`), which knows *only* current
   slot ownership — no ASKING, no migration, no READONLY. SSUBSCRIBE is `ExecutionStrategy::
   ConnectionLevel(PubSub)` (`commands/metadata.rs:64`), so it is **cluster-exempt** in
   `validate_cluster_slots` (`guards.rs:209` returns early for connection-level commands). That
   exemption is *why* the pubsub handler grew its own redirect path — and why that path silently
   lacks every case the main path gained. (See [Correctness flags](#correctness-flags).)

Adjacent but out of scope: the leadership-failure `CLUSTERDOWN No leader available`
(`slot_migration/mod.rs:156`, `connection/handlers/cluster.rs:132`) and the quorum-fence
`CLUSTERDOWN ... quorum lost` (`guards.rs:142-144`) are *not* slot redirects — they answer "is the
cluster writable", not "who owns this slot" — so this proposal leaves them where they are.

## Current state

### `connection/guards.rs:242-282` — the main redirect path (Moved/Unassigned → reply)

```rust
match coordinator.route(first_slot, &cmd_name, asking, node_id) {
    RouteDecision::LocalServe => {
        if asking {
            self.state.set_asking();
        }
        None
    }
    RouteDecision::LocalServeMigrating => None,
    RouteDecision::AcceptImporting => None,
    RouteDecision::Moved { slot, addr, .. } => {
        // READONLY mode: allow read-only commands to execute locally
        // even though we don't own the slot (replica reads).
        if self.state.is_readonly()
            && self
                .core
                .registry
                .get(&cmd_name)
                .is_some_and(|c| c.flags().contains(CommandFlags::READONLY))
        {
            return None;
        }

        match addr {
            Some(owner_addr) => Some(Response::error(format!(
                "MOVED {} {}:{}",
                slot,
                owner_addr.ip(),
                owner_addr.port()
            ))),
            None => Some(Response::error(format!(
                "CLUSTERDOWN Hash slot {} not served",
                slot
            ))),
        }
    }
    RouteDecision::Unassigned { slot } => Some(Response::error(format!(
        "CLUSTERDOWN Hash slot {} not served",
        slot
    ))),
}
```

The `match` interleaves three concerns: (a) the READONLY override (a *policy* about whether to honor
the decision), (b) the `addr.is_some()` MOVED-vs-CLUSTERDOWN fork, and (c) the exact wire strings.
Only (a) is connection-specific; (b) and (c) are pure functions of the `RouteDecision`.

### `connection/guards.rs:417-419` — ASK construction (a fourth string, fourth site)

```rust
fn ask_response(slot: u16, addr: SocketAddr) -> Response {
    Response::error(format!("ASK {} {}:{}", slot, addr.ip(), addr.port()))
}
```

Called from `migrating_ask_for_nil` (`guards.rs:407`) and `check_migrating_multikey`
(`guards.rs:360`). ASK is the *post*-execution sibling of MOVED — it is not produced by the
`RouteDecision` match, but it shares the same `ip():port()` rendering convention, restated a third
time.

### `connection/handlers/pubsub.rs:284-291` — the second routing path (SSUBSCRIBE)

```rust
for channel in args {
    // In cluster mode, check if the slot belongs to another node
    if let Some(forwarder) = &self.cluster.pubsub_forwarder
        && let Some((slot, addr)) = forwarder.get_slot_owner_addr(channel)
    {
        responses.push(Response::error(format!("MOVED {} {}", slot, addr)));
        continue;
    }
    // ... add_subscription + route to owning shard ...
}
```

### `cluster_pubsub.rs:160-179` — `get_slot_owner_addr`, the decision behind that path

```rust
/// Get the slot owner's client address for a MOVED redirect.
pub fn get_slot_owner_addr(&self, channel: &[u8]) -> Option<(u16, SocketAddr)> {
    let Self::Cluster { cluster_state, node_id, .. } = self else {
        return None;
    };

    let slot = slot_for_key(channel);
    let owner_id = cluster_state.get_slot_owner(slot)?;

    if owner_id == *node_id {
        return None;
    }

    let owner_info = cluster_state.get_node(owner_id)?;
    Some((slot, owner_info.addr))
}
```

This is `RouteDecision`'s job re-derived from `get_slot_owner` alone: own-it → `None`, else →
`Moved`. It cannot express `AcceptImporting` (importing target + ASKING serves locally),
`LocalServeMigrating`, the READONLY override, or `Unassigned` (it folds "no owner" into "serve
locally" via the `?` on `get_slot_owner`, so SSUBSCRIBE on an unassigned slot silently subscribes
locally instead of returning CLUSTERDOWN).

### Direct `slot_assignment.get` reach-ins (bypass the `get_slot_owner` accessor)

`ClusterSnapshot::get_slot_owner` (`frogdb-cluster/src/types.rs:486-488`) is the typed accessor:

```rust
pub fn get_slot_owner(&self, slot: u16) -> Option<NodeId> {
    self.slot_assignment.get(&slot).copied()
}
```

But three sites reach past it into the raw map:

- `slot_migration/routing.rs:90` — `match snapshot.slot_assignment.get(&slot)` (defensible: this is
  the single routing decision and needs `Some/None` + owner identity together).
- `connection/guards.rs:314` — `let owner = snapshot.slot_assignment.get(&slot)?;`
- `connection/guards.rs:402` — `if let Some(&owner) = snapshot.slot_assignment.get(&slot)`

The two `guards.rs` reach-ins re-implement "do we own this slot and is it migrating" inline
(`check_migrating_multikey`, `migrating_ask_for_nil`) — logic that overlaps `route()`'s
`LocalServeMigrating` / `AcceptImporting` arms.

## Proposed design

Add one seam that owns every redirect wire string, and route the SSUBSCRIBE path through the same
`coordinator.route()` decision so there is exactly one place that decides own-vs-moved-vs-ask.

### 1. `RouteDecision::to_response` — the decision→reply projection

A method on `RouteDecision` (in `slot_migration/routing.rs`, next to the enum it projects) that owns
the `Moved`/`Unassigned` → reply mapping, including the MOVED-vs-CLUSTERDOWN fork and the READONLY
override. It returns an outcome, not a bare `Option<Response>`, so "serve locally" and "no decision"
do not collapse into the same `None`:

```rust
/// What the connection layer should do with a routing decision.
pub enum RouteOutcome {
    /// Execute the command locally (owner, importing target, or READONLY replica read).
    ServeLocal,
    /// Send this redirect/error reply to the client instead of executing.
    Reply(Response),
}

impl RouteDecision {
    /// Project a routing decision onto a client reply.
    ///
    /// `readonly_eligible` is the connection-level policy the decision itself cannot see:
    /// `true` iff the connection is in READONLY mode AND the command is flagged READONLY.
    /// It only applies to the `Moved` arm (a replica can serve a read for a slot its master
    /// owns); it never rescues `Unassigned` (no replica relationship exists for an
    /// unowned slot).
    pub fn to_response(&self, readonly_eligible: bool) -> RouteOutcome {
        match self {
            RouteDecision::LocalServe
            | RouteDecision::LocalServeMigrating
            | RouteDecision::AcceptImporting => RouteOutcome::ServeLocal,

            RouteDecision::Moved { slot, addr, .. } => {
                if readonly_eligible {
                    return RouteOutcome::ServeLocal;
                }
                match addr {
                    Some(a) => RouteOutcome::Reply(redirect::moved(*slot, *a)),
                    None => RouteOutcome::Reply(redirect::clusterdown_slot(*slot)),
                }
            }
            RouteDecision::Unassigned { slot } => {
                RouteOutcome::Reply(redirect::clusterdown_slot(*slot))
            }
        }
    }
}
```

### 2. `redirect` — the one module that owns the wire format

All four redirect/error strings live in one small module (e.g. `slot_migration/redirect.rs`), so the
exact Redis byte format is stated once:

```rust
//! The single owner of cluster redirect wire formats: MOVED / ASK / CLUSTERDOWN / CROSSSLOT.
//! Every redirect a client can receive is constructed here; nowhere else formats these.

use frogdb_protocol::Response;
use std::net::SocketAddr;

/// `MOVED <slot> <ip>:<port>` — caller should reconnect to the owner.
pub fn moved(slot: u16, addr: SocketAddr) -> Response {
    Response::error(format!("MOVED {} {}", slot, fmt_addr(addr)))
}

/// `ASK <slot> <ip>:<port>` — one-shot redirect during migration.
pub fn ask(slot: u16, addr: SocketAddr) -> Response {
    Response::error(format!("ASK {} {}", slot, fmt_addr(addr)))
}

/// `CLUSTERDOWN Hash slot <slot> not served` — slot unowned / owner unknown.
pub fn clusterdown_slot(slot: u16) -> Response {
    Response::error(format!("CLUSTERDOWN Hash slot {} not served", slot))
}

/// `CROSSSLOT Keys in request don't hash to the same slot`.
pub fn crossslot() -> Response {
    Response::error("CROSSSLOT Keys in request don't hash to the same slot")
}

/// The single decision about how an owner address is rendered on the wire.
/// `<host>:<port>`, where IPv6 hosts are bracketed (what `SocketAddr`'s Display does and
/// what redis-cli's redirect parser expects). Settles the IPv4/IPv6 drift in one place.
fn fmt_addr(addr: SocketAddr) -> String {
    addr.to_string()
}
```

`fmt_addr` is the load-bearing line: the IPv4/IPv6 question is answered once. (Whether the chosen
rendering must match Redis exactly is an open question — see [Risks](#risks--open-questions) — but
after this change there is *one* place to fix, not three.)

### 3. `guards.rs` — collapse the match onto the seam

```rust
// BEFORE: 40 lines of interleaved policy + format (guards.rs:243-281).
// AFTER:
let asking = self.state.take_asking();
let decision = coordinator.route(first_slot, &cmd_name, asking, node_id);

// LocalServe historically preserves ASKING when we fully own the slot.
if matches!(decision, RouteDecision::LocalServe) && asking {
    self.state.set_asking();
}

let readonly_eligible = self.state.is_readonly()
    && self
        .core
        .registry
        .get(&cmd_name)
        .is_some_and(|c| c.flags().contains(CommandFlags::READONLY));

match decision.to_response(readonly_eligible) {
    RouteOutcome::ServeLocal => None,
    RouteOutcome::Reply(resp) => Some(resp),
}
```

`ask_response` (`guards.rs:417`) becomes a one-line delegate to `redirect::ask`; the CROSSSLOT
string at `guards.rs:232` becomes `redirect::crossslot()`.

### 4. SSUBSCRIBE through the same decision

The SSUBSCRIBE redirect stops being a parallel implementation and becomes a *consumer* of
`route()` + `to_response`. The forwarder keeps its `forward_spublish` job (that is message delivery,
not redirect construction); `get_slot_owner_addr` is deleted.

```rust
// handlers/pubsub.rs, inside the per-channel loop — AFTER:
for channel in args {
    if let Some(reply) = self.ssubscribe_redirect(channel) {
        responses.push(reply);
        continue;
    }
    // ... add_subscription + route to owning shard (unchanged) ...
}

// New helper alongside validate_cluster_slots — reuses the one routing decision.
fn ssubscribe_redirect(&mut self, channel: &[u8]) -> Option<Response> {
    let coordinator = self.cluster.slot_migration.as_ref()?;
    let node_id = self.cluster.node_id?;
    let slot = slot_for_key(channel);
    let asking = self.state.take_asking();
    let decision = coordinator.route(slot, "SSUBSCRIBE", asking, node_id);
    match decision.to_response(/* readonly_eligible */ false) {
        RouteOutcome::ServeLocal => None,         // own / importing / migrating → subscribe here
        RouteOutcome::Reply(resp) => Some(resp),  // MOVED / CLUSTERDOWN, formatted once
    }
}
```

SSUBSCRIBE is not flagged `READONLY` (`commands/metadata.rs:63`: `PUBSUB | LOADING | STALE`), so
`readonly_eligible` is `false` today — the unified path does not silently change replica behavior;
it *gains* the migration cases (`AcceptImporting`, `LocalServeMigrating`) and the `Unassigned →
CLUSTERDOWN` case the old path lacked, and it picks up whatever READONLY policy is later decided for
sharded pub/sub for free (see [Correctness flags](#correctness-flags) and
[Risks](#risks--open-questions)).

### Why this is the right depth

- **Locality.** The wire format of every redirect lives in `redirect.rs`; the decision→reply
  projection lives in `RouteDecision::to_response`, beside the enum. Changing how an address renders,
  or how an unassigned slot is reported, is a one-function edit instead of a five-site sweep across
  three files.
- **Leverage.** One small module plus one method delete the three duplicated `ip():port()` renderings,
  the duplicated CLUSTERDOWN string, and — crucially — the *entire second routing path*
  (`get_slot_owner_addr`). The SSUBSCRIBE handler stops being a place where slot-routing logic can
  drift from the main path, because it no longer contains any.
- **Deletion test (now passes).** After the change, deleting `redirect.rs` breaks *every* redirect in
  one compile error, and deleting `get_slot_owner_addr` is the migration itself. The construction has
  exactly one home.
- **No new adapter.** `to_response` is a method on the type it projects, not a wrapper layer callers
  may bypass; `redirect` is leaf formatting, not a façade over the decision. The deep decision module
  (`route` / `RouteDecision`) is unchanged — we are extending its interface to cover the reply
  direction it always implied (`routing.rs:36-52`'s doc comments already *describe* MOVED/CLUSTERDOWN
  in prose; this makes that prose executable).
- **Round-2-deferred candidate.** This was flagged as a deepening opportunity but deferred in the
  earlier routing pass because it is reply-side and lower-risk than the decision-side work in
  proposal 05. It is now the highest-leverage remaining cluster-routing cleanup precisely because the
  decision side (`RouteDecision`) already landed and is solid — the reply side is the last shallow
  half of the seam.

This does **not** re-open proposal 05: handler selection (`ConnectionLevelHandler`,
`refine_handler`) is untouched. 17 only changes how a `RouteDecision` becomes bytes and which code
produces that decision for SSUBSCRIBE.

## Migration plan

Behavior-preserving for the keyed path; the pubsub path *gains* the migration/READONLY checks (fixing
the bug). Each phase compiles and tests independently.

1. **Phase 0 — add the seam, no call sites change.** Create `slot_migration/redirect.rs` with
   `moved` / `ask` / `clusterdown_slot` / `crossslot` / `fmt_addr`, and add `RouteOutcome` +
   `RouteDecision::to_response` in `routing.rs`. Unit-test both (matrix below). `just check
   frogdb-server && just test frogdb-server slot_migration`.
2. **Phase 1 — route `guards.rs` through the seam.** Replace the `Moved`/`Unassigned` match arms with
   `decision.to_response(readonly_eligible)`; point `ask_response` and the CROSSSLOT string at
   `redirect`. Pure refactor; the existing cluster integration suite is the safety net.
   `just test frogdb-server`.
3. **Phase 2 — route SSUBSCRIBE through the seam (the fix).** Add `ssubscribe_redirect` using
   `coordinator.route(...)` + `to_response`; replace the `get_slot_owner_addr` call in
   `handlers/pubsub.rs:286-291`. This is the only phase with a behavior change (SSUBSCRIBE now honors
   ASKING/migration/`Unassigned`), so add the redirect-parity tests here.
4. **Phase 3 — deletion pass.** Remove `ClusterPubSubForwarder::get_slot_owner_addr`
   (`cluster_pubsub.rs:160-179`) and its `test_local_forwarder_slot_owner_returns_none`
   (`cluster_pubsub.rs:200-204`); keep `forward_spublish` (SPUBLISH delivery, still needed). Replace
   the `guards.rs:314,402` raw `slot_assignment.get` reach-ins with `snapshot.get_slot_owner(slot)`
   where the migration-overlap logic isn't needed. FrogDB is pre-production — no deprecation shims.
5. **Gate (optional).** A `just lint` grep that fails if `"MOVED ` / `"ASK ` / `"CLUSTERDOWN Hash
   slot` string literals reappear outside `redirect.rs`, mirroring the `sync-toolchain-check` lefthook
   pattern, so the format cannot re-scatter.

## Testing impact

- **Per-variant reply construction, no cluster needed.** `RouteDecision::to_response` is a pure
  function over the enum + one bool; a table test covers every arm: `LocalServe` /
  `LocalServeMigrating` / `AcceptImporting` → `ServeLocal`; `Moved { addr: Some }` →
  `Reply(MOVED ...)`; `Moved { addr: None }` → `Reply(CLUSTERDOWN ...)`; `Moved` +
  `readonly_eligible` → `ServeLocal`; `Unassigned` → `Reply(CLUSTERDOWN ...)` even when
  `readonly_eligible`. Today the `Moved`/`Unassigned` reply mapping is only reachable through a live
  multi-node cluster integration test.
- **Format pinned once.** `redirect::moved` / `ask` get byte-exact assertions for IPv4 *and* IPv6
  (the case that currently diverges). One test owns the wire format; today no test asserts the IPv6
  rendering at all.
- **One redirect test exercises all paths.** Because `guards.rs` and the SSUBSCRIBE path now both
  feed through `route()` + `to_response`, a single parametrized test ("given snapshot S and command
  C, the redirect reply is R") covers keyed commands *and* SSUBSCRIBE — instead of two test suites
  that could (and did) drift. A regression test for the SSUBSCRIBE-during-migration bug
  (importing target + ASKING → `ServeLocal`, not MOVED) goes here.
- **Existing suites unchanged for the keyed path.** Phases 0-1 are behavior-preserving; the cluster
  redirect integration tests act as the end-to-end check that no keyed reply changed.

## Risks / open questions

- **Exact MOVED/ASK wire format must match Redis.** Redis emits `MOVED <slot> <ip>:<port>` /
  `ASK <slot> <ip>:<port>` and, since Redis 7, may emit a hostname/endpoint form depending on
  `cluster-preferred-endpoint-type`. `fmt_addr` centralizes the choice, but the *correct* choice
  needs confirmation against the redis-cli redirect parser, especially for IPv6 (does the reference
  client expect `[2001:db8::1]:6379` brackets, or the raw `ip:port`?). Centralizing first makes this a
  single follow-up edit; today it would be a three-site fix with one site already disagreeing.
- **READONLY + read-only-command local serve nuance.** `readonly_eligible` reproduces today's
  `guards.rs:254-261` policy (READONLY connection AND command flagged `READONLY`). Whether *sharded
  pub/sub* should honor READONLY on a replica is a separate Redis-semantics question — SSUBSCRIBE is
  not READONLY-flagged, so the unified path keeps the current (no-local-serve) behavior. Surfacing the
  flag as an explicit `to_response` parameter makes that policy a one-line change if Redis parity
  later demands it, instead of an invisible omission in a second code path.
- **ASKING flag interplay.** `route()` consumes ASKING via `take_asking()`; the `LocalServe` arm
  restores it (`guards.rs:243-247`) to preserve the quirk that fully-owning a slot doesn't consume
  ASKING. The new SSUBSCRIBE path also calls `take_asking()` per channel — for a multi-channel
  SSUBSCRIBE this means ASKING is consumed by the first channel, matching how a one-shot flag should
  behave, but it is a behavior decision worth pinning in a test (today SSUBSCRIBE ignores ASKING
  entirely). Confirm the per-channel loop's interaction with the one-shot semantics is what we want.
- **`SocketAddr` Display vs explicit `ip:port`.** Standardizing on `SocketAddr::to_string()`
  (`fmt_addr`) changes the keyed path's IPv6 output from `2001:db8::1:6379` to `[2001:db8::1]:6379`.
  This is almost certainly the *correct* fix (the unbracketed form is unparseable), but it is a
  user-visible wire change for IPv6 clusters and should be called out in the changelog even though
  FrogDB is pre-production.
- **`forward_spublish` keeps its own `get_slot_owner` call.** SPUBLISH forwarding
  (`cluster_pubsub.rs:111-154`) legitimately needs slot ownership but not a redirect reply (publish
  is fire-and-forward, no ASK/READONLY). It stays as-is; only `get_slot_owner_addr` is deleted. If
  SPUBLISH later needs migration-aware forwarding, it can adopt `route()` too — out of scope here.

## Correctness flags

- **SSUBSCRIBE bypasses the migration + READONLY routing logic**
  (`cluster_pubsub.rs:160-179` `get_slot_owner_addr`, consumed at `handlers/pubsub.rs:286-291`).
  Unlike the keyed path (`guards.rs:242-282`), the SSUBSCRIBE redirect decision is made from current
  slot ownership alone. Concretely wrong cases: (a) during a migration where this node is the
  importing target and the client sent `ASKING`, the keyed path returns `AcceptImporting` (serve
  locally) but SSUBSCRIBE returns `MOVED`; (b) SSUBSCRIBE never emits `ASK`; (c) on an *unassigned*
  slot the `?` on `get_slot_owner` (`cluster_pubsub.rs:171`) makes SSUBSCRIBE subscribe **locally**
  instead of returning `CLUSTERDOWN`; (d) the READONLY override (`guards.rs:254-261`) is absent
  (no observable effect today since SSUBSCRIBE isn't READONLY-flagged, but the divergence is real and
  would silently persist if SSUBSCRIBE's flags change). Fix: route the channel's slot through
  `coordinator.route()` + `RouteDecision::to_response` (Phase 2).

- **MOVED format drift between the two redirect sites**
  (`guards.rs:265-270` renders `format!("MOVED {} {}:{}", slot, addr.ip(), addr.port())`;
  `pubsub.rs:289` renders `format!("MOVED {} {}", slot, addr)` via `SocketAddr` `Display`). Identical
  for IPv4, divergent for IPv6: the explicit form omits the brackets `SocketAddr::Display` adds
  (`2001:db8::1:6379` vs `[2001:db8::1]:6379`), so the two paths return wire-incompatible redirects in
  an IPv6 cluster. Fix: single `redirect::fmt_addr` (Phase 0).

- **Direct `slot_assignment.get` reach-ins bypass the `get_slot_owner` accessor**
  (`guards.rs:314`, `guards.rs:402`; also `slot_migration/routing.rs:90`). The two `guards.rs` sites
  re-derive "do we own this slot / is it migrating" inline, duplicating `route()`'s
  `LocalServeMigrating` / `AcceptImporting` logic against the raw map instead of going through
  `ClusterSnapshot::get_slot_owner` (`frogdb-cluster/src/types.rs:486-488`) or `route()`. Fix: use
  the accessor where ownership is all that's needed (Phase 3); the `routing.rs:90` reach-in is
  defensible (it is the single routing decision and needs `Some/None` + owner identity jointly).
