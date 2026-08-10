# 54 — replica connection + handler wiring: one door instead of twelve fields

Size: **S + S** (two independent landings, RC2 then RC10). Area: **replication (LOCKED, 0.85
gate)**. Latent — no live bug. No failure-mode spec row pins construction; see
[Spec position](#spec-position-locked-area).

## Summary

`ReplicaConnection` is a **shallow module**: twelve `pub(crate)` fields, no constructor, and
eleven struct-literal construction sites. Its interface is as wide as its implementation —
every caller must know all twelve fields *and* the five invariants that relate them, none of
which the type can state. `ReplicaReplicationHandler` has the same shape one level up: sixteen
fields, a five-argument `new`, and five post-construction setters whose doc comments say "must
be called by every construction site" — an obligation the interface documents but cannot
enforce.

The change is to give each type one door. `ReplicaConnection::new(stream, ReplicaLink)` plus a
test-only `ReplicaLink` builder collapses eleven literals to eleven calls and lets the
constructor enforce the pairings. `ReplicaWiring` **replaces** the four *policy* setters (they
are deleted, not kept alongside — see [Spec position](#spec-position-locked-area)) with a value
that the two production wiring paths — boot (`replication_init.rs`) and runtime demotion
(`role_manager.rs`) — construct and can be compared by equality rather than by reading two
comment blocks. `set_connect_factory` stays a setter and is argued for below; it is a transport
choice, not a wiring decision.

Both are behavior-neutral. The **leverage** is that the invariants FM-REPLICATION-027 states
in prose become constructor preconditions, testable *inside the mutated crate* — where today
only half of that row's fail-closed property is forced.

## Files involved

Crate names are `frogdb-replication` and `frogdb-server`; the spec cites them by package name,
the tree lays them out under `frogdb-server/crates/`. All paths absolute-from-repo-root.
Verified against `main` (`08c143d6`).

| path | lines | what 54 touches | verified |
|---|---|---|---|
| `frogdb-server/crates/replication/src/replica/connection.rs` | 1884 | `use std::net::SocketAddr;` **:12** (sole user is the dead field); `struct ReplicaConnection` 109–153 (**12 fields**: 114, 115, 116, 117, 118, 122, 126, 130, 135, 140, 145, 152); `set_state` 159–163; `self.state` readers at **230, 324, 409, 483**; **8 test literals** at 623, 680, 802, 934, 1154, 1314, 1458, 1545 (`#[cfg(test)]` opens at 577) | ✅ |
| `frogdb-server/crates/replication/src/replica/mod.rs` | 560 | `struct ReplicaReplicationHandler` 152–215 (**16 fields**); `new` 230–269; **5 setters** at 275, 336, 345, 356, 364 (`set_snapshot_installer`'s "must be called" doc is **342–344**); **1 production literal** 490–503 inside `connect_and_sync` 482–525 | ✅ |
| `frogdb-server/crates/replication/src/replica/streaming.rs` | 842 | **2 test literals** at 337, 409 (`#[cfg(test)]` opens at 256); `const CADENCE: Duration = 60s` at **276** with its load-bearing doc at **273–275** and its consumer `timeout(CADENCE / 2, …)` at **512**; production field reads at 54 (`take_pending_stream_bytes`), 62, 99, 145, 177, 178, 185, 214, 231, 242, 248 | ✅ |
| `frogdb-server/crates/replication/src/replica/offset.rs` | 998 | read-mostly: `struct ReplicaOffset` 443–453 **already holds `state`** (452); `new` **463–475**; the stint-ordering contract in the doc comment at 446–450. Needs one added `pub(super) fn state()` | ✅ |
| `frogdb-server/crates/replication/src/replica/tests.rs` | 492 | handler `new` sites 28, 49, 102, 199, 267, 341, 386, 415, 434, 469; setter sites 61, 65, 110, 210, 274, 348, 352, 477; the crate's **only** `// FM-` tag is FM-REPLICATION-063 at **304** over the test at **313**; `link_up_reports_true_once_the_stream_is_running` at **283** (pre-`start()` assertion at **286**) | ✅ |
| `frogdb-server/crates/server/src/server/replication_init.rs` | 552 | boot wiring path: `new` 223–229, setters 230, 234, 237, 262, 294, 302 | ✅ |
| `frogdb-server/crates/server/src/role_manager.rs` | 1632 | `struct RealReplicaStreamer` 507–529+; `build_handler` 655–739, `new` at 664, setters at 673, 674, 676, 684, 708, 735; test `new` sites 1507, 1580 + `set_connect_factory` 1589 | ✅ |

**Counts differ from the candidate.** The candidate said "9 struct-literal construction sites
(mod.rs:490-503 + 8 test sites)". The true count is **11**: 1 production + 10 test (8 in
`connection.rs`, 2 in `streaming.rs` — the two `streaming.rs` fixtures were missed). The
handler numbers are exact: **16 fields**, **5 setters**, ranges 152–215 and 275–364 as cited.

`ReplicaConnection` is `pub` and re-exported **three** times: `frogdb-replication/src/lib.rs:68`,
`frogdb-core/src/lib.rs:131` (via `pub use frogdb_replication as replication` at `:35`), and
`frogdb-server/src/replication/mod.rs:24`. Nothing outside `replica/*` names the type, so all
three are unused re-exports of a name; they keep working unchanged (see
[visibility](#visibility-what-is-actually-new-public-surface-none)).

Not touched: `replica_session.rs` (4574 lines) — despite the name it is the **primary**-side
per-replica session (`//! Per-replica session state machine`, line 1), not the replica side.
See [Risks](#risks--scope-boundaries-vs-siblings).

## Problem

### RC2 — the interface is wider than the implementation

`ReplicaConnection` (`connection.rs:109-153`) has no constructor. Every caller writes the whole
struct out. The single production site:

```
mod.rs:490    let mut conn = ReplicaConnection {
mod.rs:491        stream,
mod.rs:492        _primary_addr: self.primary_addr,
...
mod.rs:503    };
```

and ten test sites repeat the same fourteen lines. Of the twelve fields, only **three** carry
information a caller actually chose — `stream`, `state`, `offsets`. The other nine are the same
constant at nearly every site: `_primary_addr: "127.0.0.1:6379".parse().unwrap()` (all ten test
sites), `ack_interval: Duration::from_secs(1)`, `snapshot_installer: None`, `sync_refusal:
Arc::new(RwLock::new(None))`, `pending_stream_bytes: BytesMut::new()`, `net_bytes:
Arc::new(NetByteCounters::default())`. The interface makes every caller restate the defaults.

Worse, the fields are *related*, and nothing states the relation. **Five field combinations are
illegal today and constructible today:**

1. **`link_up` disagreeing with `connection_state`.** `set_state`
   (`connection.rs:159-163`) is the sole maintainer of `link_up == (state == Streaming)` — and
   construction bypasses it entirely. Two sites hand-write the pair as `Streaming` + `true`
   (`streaming.rs:341/344`, `streaming.rs:413/416`); the other nine hand-write a non-`Streaming`
   state + `false`. A twelfth site writing `Streaming` + `false` (or the reverse) compiles and
   passes review. This is precisely FM-REPLICATION-027's fail-closed invariant: *"only
   `ReplicaConnection` sets it `true`, and only once it reaches `ConnectionState::Streaming`"* —
   an invariant whose only enforcement is that eleven authors happened to type it correctly.

2. **`offsets.state` diverging from `state`.** `ReplicaOffset` **already holds** an
   `Arc<RwLock<ReplicationState>>` (`offset.rs:452`). `ReplicaConnection` stores a second handle
   to the same thing, and every site passes `state.clone()` twice — once into
   `ReplicaOffset::new`, once into the literal. `psync` reads the offset from one
   (`connection.rs:229`) and the replication id from the other (`connection.rs:230`); if they
   ever pointed at different states, a reconnect would resume a live offset **under the wrong
   replication id**. Nothing ties them.

3. **`pending_stream_bytes` non-empty at construction.** The field is the hand-back for the
   full-sync trailer (hardening issue 01). `take_pending_stream_bytes`
   (`connection.rs:182-184`) hands it to the streaming decoder exactly once
   (`streaming.rs:54`). A connection born with bytes in it would seed a decoder with data no
   payload read ever produced.

4. **Fresh `Arc`s where the handler's shared ones belong.** `link_up`, `sync_refusal` and
   `net_bytes` are documented as *"Shared with the owning `ReplicaReplicationHandler`"*
   (`connection.rs:123-126`, `136-140`, `146-152`). A production path that minted fresh ones
   would silently disconnect `INFO`'s `master_link_status`, `master_sync_error`, and
   `total_net_repl_input_bytes` from what the link is actually doing — the FM-REPLICATION-027
   and FM-REPLICATION-063 observables — while every test still passed.

5. **Stint ordering.** `offset.rs:446-450` states the contract in prose: *"Callers must
   therefore open the stream's stint **before** building its connections."* A construction-order
   precondition that exists only in a doc comment on a different type.

And one field is simply dead: **`_primary_addr` is never read** — grep for it outside field
declarations returns only unrelated locals in `replication_init.rs`/`cluster_init.rs`. It is
underscore-prefixed to silence the warning and hand-written at all eleven sites, and it is the
**sole** user of `use std::net::SocketAddr;` (`connection.rs:12`).

### RC10 — an obligation the interface states but cannot enforce

`ReplicaReplicationHandler::new` (`mod.rs:230-269`) takes five arguments and leaves five
decisions to setters. Two of those setters carry doc comments that admit the problem:

```
mod.rs:342    /// Must be called by every construction site that has shards to install into
mod.rs:343    /// (boot-configured replica and runtime `REPLICAOF` demotion alike),
mod.rs:344    /// otherwise a full resync only stages for the next boot.
```

```
mod.rs:353    /// be called by every construction site that wants real input bytes
mod.rs:354    /// reported (boot-configured replica and runtime `REPLICAOF` demotion
mod.rs:355    /// alike); otherwise the handler counts into a counter nothing reads.
```

There are exactly two such sites **in production**, and they are two independent spellings of
the same five decisions:

| decision | boot (`replication_init.rs`) | demotion (`role_manager.rs::build_handler`) |
|---|---|---|
| ack cadence | `:230` from `config.replication.ack_interval_ms` | `:673` from `self.ack_interval_ms` |
| net-byte counters | `:234` unconditional, `tracker.net_bytes_handle()` | `:676` **conditional** on `self.net_bytes.is_some()` |
| snapshot installer | `:237` `LiveSnapshotInstaller::for_config(...)` | `:674` `self.snapshot_installer.clone()` |
| shared offset | `:302` gated on `config.cluster.enabled` | `:684` gated on `self.shared_offset.is_some()` |
| connect factory | `:262` turmoil / `:294` TLS | `:708` turmoil / `:735` TLS |

"Exactly two" is a statement about *production*. `set_snapshot_installer` has a **third**
caller, `connection.rs:1798`, in the crate's own test fixture — which is the point: nothing
distinguishes a wiring site from a fixture, because there is no wiring type.

The two paths already drifted once — the mutability round's issue 18 was exactly *"primary-side
replication seams now constructed for every role"*, a promotion gap found because two wiring
sites disagreed. The **locality** problem is that the answer to "is this handler fully wired?"
is spread across two files, ten call sites, and two comments, with no type that holds it.

Note that `RealReplicaStreamer` (`role_manager.rs:507-529+`) is *already* a de-facto wiring
struct: it holds `shared_offset`, `ack_interval_ms`, `snapshot_installer` and `tls` as fields
for no reason other than to stamp them onto the handler in `build_handler`. The knowledge
exists; it just lives in `frogdb-server` where the boot path cannot reach it.

## Proposed change

### RC2 — `ReplicaConnection::new(stream, ReplicaLink)`

Introduce `ReplicaLink` — the handler-owned half of a connection, everything a connection needs
that outlives it:

```rust
pub(crate) struct ReplicaLink {
    offsets: ReplicaOffset,          // carries `state`; see below
    data_dir: PathBuf,
    ack_interval: Duration,
    snapshot_installer: Option<SnapshotInstaller>,
    link_up: Arc<AtomicBool>,
    sync_refusal: Arc<RwLock<Option<String>>>,
    net_bytes: Arc<NetByteCounters>,
}

impl ReplicaConnection {
    pub(crate) fn new(stream: BoxedStream, link: ReplicaLink) -> Self { … }
}
```

Field count goes **12 → 10** on the way in:

- **`_primary_addr` deleted** — never read. (Independently landable; see
  [Effort](#effort--independently-landable-hotfix).)
- **`state` derived, not passed** — `ReplicaOffset` already owns it (`offset.rs:452`). Add
  `pub(super) fn state(&self) -> &Arc<RwLock<ReplicationState>>` and have the four
  `self.state` readers (`connection.rs:230, 324, 409, 483`) go through `self.offsets.state()`.
  This is the change that makes illegal combination **2** unrepresentable: there is now one
  `ReplicationState` handle per connection, not two that must agree. **If 55 lands first
  (recommended, see [Conflict edges](#conflict-edges)) this is three readers, not four**: 55's
  `adopt_full_sync` folds `:409` and `:483` into one.

What the constructor enforces that a literal cannot:

- **`connection_state: ConnectionState::Connected`** and **`link_up: false`** are *set by the
  constructor*, not accepted from the caller. A connection is born disconnected; the only way
  to `Streaming` is `set_state`, which publishes both halves together. Illegal combination
  **1** — and FM-REPLICATION-027's fail-closed property — becomes structural rather than
  editorial.
- **`pending_stream_bytes: BytesMut::new()`**, likewise not a parameter. Illegal combination
  **3** gone.
- **The shared trio arrives inside `ReplicaLink`**, which only the handler mints
  (`ReplicaReplicationHandler::link(&self)`). A caller cannot substitute fresh `Arc`s without
  building a `ReplicaLink`, at which point the sharing is the one obvious thing that type is
  for. Illegal combination **4** is now a visible decision instead of an invisible default.
- **Stint ordering (5)** is documented on `ReplicaLink::new` — still prose, but prose attached
  to the one place the ordering is decidable, rather than to a neighbouring type.

#### The test adapter, at the shape the ten sites actually need

An earlier draft sketched `for_test(stream, offsets)`. That covers **five** of the ten sites
(`connection.rs:623, 680, 802, 1314, 1458` — all of which take `data_dir:
PathBuf::from("/tmp/frogdb-test")`, `ack_interval: 1s`, `snapshot_installer: None`, and fresh
`Arc`s nothing reads back). The other five need more, and pretending otherwise would make the
adapter a lie:

| site | what it needs beyond `(stream, offsets)` |
|---|---|
| `connection.rs:934` (`checkpoint_fixture_with_tail`) | real `data_dir = tmp.join("db")` (the stager needs a parent dir), `Some(installer)`, and the `link_up` handle read back at `:941` (asserted at `:983`, `:1060`) |
| `connection.rs:1154` (`dataset_fixture_with_tail`) | same three (`:1161`; asserted `:1233`, `:1257`) |
| `connection.rs:1545` | `data_dir = tmp.path().join("db")`, `link_up` read back at `:1552` (asserted `:1578`), and a **pre-seeded `ReplicationState`** (`:1533-1537`) — which arrives inside `offsets`, so no extra knob |
| `streaming.rs:337` (`Link::connect`) | `ack_interval: CADENCE` (`:345`; 60 s, defined `streaming.rs:276`) and the `net_bytes` handle cloned in at `:349` and kept on the `Link` at `:362` (read back at `:722`, `:734`, `:745`, `:782`) |
| `streaming.rs:409` (`bare_connection`) | same two — `CADENCE` at `:417`, `net_bytes` cloned at `:421` and returned to the caller at `:423` |

`CADENCE` is **load-bearing, not cosmetic**: its doc (`streaming.rs:273-275`) says it is "long
enough that the `interval`'s immediate first tick is the only spontaneous ACK a test sees", and
`streaming.rs:512` asserts silence for `CADENCE / 2`. A `for_test` that hardcoded 1 s would
turn that assertion into a flake. So the adapter is a small builder on the **link**, not a
fixed-arity constructor on the connection:

```rust
#[cfg(test)]
impl ReplicaLink {
    /// The link the five plain fixtures use verbatim: `/tmp/frogdb-test`,
    /// 1 s cadence, no installer, private `link_up`/`sync_refusal`/`net_bytes`.
    pub(super) fn for_test(offsets: ReplicaOffset) -> Self;
    pub(super) fn with_data_dir(self, dir: PathBuf) -> Self;
    pub(super) fn with_installer(self, i: Option<SnapshotInstaller>) -> Self;
    pub(super) fn with_ack_interval(self, d: Duration) -> Self;
    /// Handles the fixture keeps after the connection swallows the link —
    /// this is what replaces the `link_up.clone()` / `net_bytes.clone()`
    /// hand-off the literals do today.
    pub(super) fn link_up(&self) -> Arc<AtomicBool>;
    pub(super) fn net_bytes(&self) -> Arc<NetByteCounters>;
}
```

Tests then read `ReplicaConnection::new(stream, ReplicaLink::for_test(offsets))`, or
`…::new(stream, ReplicaLink::for_test(offsets).with_data_dir(tmp.join("db")).with_installer(inst))`.
The two `streaming.rs` fixtures additionally need to start **already `Streaming`**; that is
`ReplicaConnection::new(..).into_streaming()` — a `#[cfg(test)]` helper whose entire body is
`self.set_state(ConnectionState::Streaming); self`, so even the test path goes through
`set_state` and cannot desynchronise the pair.

**One behavior delta the constructor forces, stated up front.** `connection.rs:934`, `:1154`
and `:1545` build at `ConnectionState::Syncing` today; `new` forces `Connected`. Verified
harmless against every existing assertion: the three assertions that could see it are
`assert_ne!(…, Streaming)` at `:1059`, `:1256`, `:1293` and `:1577` (all still pass), the
`assert_eq!(…, Streaming)` ones at `:982`, `:1037`, `:1232` run *after* a payload receive that
ends in `set_state(Streaming)`, and `connection_state` has no reader outside `connection.rs` at
all. It is nonetheless a real difference between what the fixture said and what it now says,
and it belongs in the commit message rather than in a reviewer's discovery.

#### Visibility: what is actually new public surface (none)

`new` is `pub(crate)` — its only production caller is `replica/mod.rs`. `ReplicaLink` is
`pub(crate)`. The builder is `#[cfg(test)] pub(super)`. **No new `pub` item**, so the three
re-exports of `ReplicaConnection` (`replication/src/lib.rs:68`, `core/src/lib.rs:131`,
`server/src/replication/mod.rs:24`) are untouched: they export a type name whose fields were
already `pub(crate)` and therefore never constructible from outside the crate. There is no
`for_test` on `ReplicaConnection` itself because none is needed — the tests are inline
`#[cfg(test)] mod tests` in the same crate and can call `pub(crate) fn new` directly.

**Closing the door.** `streaming.rs` holds an `impl ReplicaConnection` block
(`streaming.rs:43`), so the fields cannot all become module-private. Reading the production
side of that block, it touches exactly four fields — `stream` (`:145`, `:248`), `offsets`
(`:62`, `:177`, `:178`, `:185`, `:214`), `ack_interval` (`:99`) and `net_bytes` (`:231`,
`:242`) — plus the *method* `take_pending_stream_bytes()` (`:54`), not the field. So after RC2
the split is **6 private / 4 `pub(super)`**:

| private (read only in `connection.rs`) | `pub(super)` (read from `streaming.rs`) |
|---|---|
| `connection_state`, `link_up`, `data_dir`, `snapshot_installer`, `sync_refusal`, `pending_stream_bytes` | `stream`, `offsets`, `ack_interval`, `net_bytes` |

An earlier draft said "three private / seven pub(super)" and counted `state` among the three —
wrong twice over: `state` is *deleted*, not made private (so only **two** of the three named
fields drop), and four more fields become private-able the moment `mod.rs`'s literal at
`490-503` stops reading `data_dir`/`snapshot_installer`/`sync_refusal`/`net_bytes` off `self`.
Private is not a barrier to `connection.rs`'s own `#[cfg(test)] mod tests` (a child module sees
its parent's private items), which is why `:982` and friends keep compiling.

That makes combination **1** literally unconstructible outside `connection.rs`. The residual —
a future literal inside `connection.rs` itself — is closed by a sixteenth entry in the
`lint-gates` family (`agents/seam-lints.md`): *"`ReplicaConnection` is constructed only through
`new`"*, a compile-free grep in the same shape as `lint-format-float`.

### RC10 — `ReplicaWiring`

```rust
pub struct ReplicaWiring {
    pub ack_interval: Duration,
    pub snapshot_installer: Option<SnapshotInstaller>,
    pub net_bytes: Arc<NetByteCounters>,
    pub shared_offset: Option<Arc<AtomicU64>>,
}
```

Passed to `ReplicaReplicationHandler::new(primary_addr, listening_port, identity, state_path,
data_dir, wiring)`. `Default` gives today's `new` semantics (1 s, `None`, private counter,
`None`), so the "unwired" handler stays expressible — it is just no longer the *silent* case.

**The four policy setters are deleted, not kept alongside.** `set_ack_interval`,
`set_snapshot_installer`, `set_net_bytes_counters` and `set_shared_offset` go away; their
bodies move into `new`. Two of them carry logic that moves with them: `set_ack_interval`'s
zero-guard (`mod.rs:276-278` — config validation already rejects zero, so the guard becomes an
assertion at the boot site rather than a silently-ignored write), and `set_shared_offset`'s
seed-then-adopt (`mod.rs:365-367` — `new` mints `live`, so it seeds the caller's atomic from
the same value in the same order). The argument for deletion rather than coexistence is in
[Spec position](#spec-position-locked-area); it is the difference between one door and two.

**`connect_factory` deliberately stays a setter.** It is a transport choice made by
`#[cfg(feature = "turmoil")]` / TLS-config branches at **four** production sites
(`replication_init.rs:262`, `:294`; `role_manager.rs:708`, `:735`) and **six** test sites
(`tests.rs:110`, `:274`, `:348`, `:477`; `connection.rs:1797`; `role_manager.rs:1589`) — ten,
not eleven, and six test sites, not seven, as an earlier draft said. It is not a wiring
*decision*: folding it in would make `ReplicaWiring` a pass-through slot for a closure every
caller sets separately. Scoping it out is what keeps the struct honest, and it is why RC10's
claim is "one door for **policy**", not "one door for everything".

**Blast radius.** Adding a sixth parameter touches **15 `ReplicaReplicationHandler::new` call
sites across 4 files** (5 if the defining `mod.rs` counts): `role_manager.rs:664`, `:1507`,
`:1580`; `replication_init.rs:223`; `connection.rs:1790`; `tests.rs:28`, `:49`, `:102`, `:199`,
`:267`, `:341`, `:386`, `:415`, `:434`, `:469`. **Nine** of the fifteen wire no policy at all
(they set `connect_factory` or nothing) and take `ReplicaWiring::default()`; the **six** that
wire policy are where the value is: `role_manager.rs:664` (all four), `replication_init.rs:223`
(all four), `connection.rs:1790` (installer, via `:1798`), `tests.rs:49` (ack cadence, via
`:61`/`:65` — the zero-guard test), `tests.rs:199` (shared offset, via `:210`) and
`tests.rs:341` (net-byte counters, via `:352`). This is the mechanical bulk of RC10 and the
reason it is sized `S` and not `XS`.

**Deletion test on `ReplicaWiring` — does it concentrate, or relocate?** It concentrates, and
the evidence is `RealReplicaStreamer`. Delete `ReplicaWiring` and the four decisions go back to
living as `RealReplicaStreamer`'s fields *plus* six inline expressions in
`replication_init.rs`, with the two spellings only comparable by eye. Keep it and
`RealReplicaStreamer` holds one `ReplicaWiring` instead of three loose fields, `init_replication`
builds one, and "boot and demotion wire a handler identically" becomes a statement about two
values of one type — the exact drift that produced issue 18. That is concentration: the
knowledge moves from two files' worth of call-sequence into one type's field list, and the
handler stops advertising four setters that its own doc comments say are mandatory.

## Testability improvement

**The interface is the test surface.** Today the surface is twelve fields, so a test that wants
to assert one thing states twelve. That has three concrete costs, and the fix removes all three.

1. **What becomes writable that is not writable now — half of FM-REPLICATION-027, precisely.**
   An earlier draft claimed the row has *no* in-crate forcing test. That is wrong by half.
   `link_up_reports_true_once_the_stream_is_running` (`replica/tests.rs:283`, in
   `frogdb-replication`) spawns `start()` over a `+CONTINUE`-granting primary and polls
   `handler.link_up()` until it is `true` — so it **does** kill the happy-path mutants: a
   `set_state` whose body becomes `()`, and a `state == ConnectionState::Streaming` rewritten
   to `false`. What survives is the **fail-closed direction**: rewrite the predicate to `true`
   and the test still passes, because its only pre-`start()` assertion (`:286`) runs before any
   `set_state` call, and the poll loop only ever wants `true`. The row's three named `Forced by`
   tests (`test_replica_handles_rapid_reconnect`,
   `test_info_replication_master_link_status_tracks_connection`,
   `test_info_replication_master_link_status_down_before_connected`) all live in
   `frogdb-server/crates/server/tests/integration_replication.rs` (`:5539`, `:3873`, `:3909`),
   and per CLAUDE.md `cargo mutants -p frogdb-replication` runs only that package's own tests —
   so the fail-closed half is scored against nothing in the crate that owns it. With a
   constructor there is a socket-free unit test in `frogdb-replication`: *a freshly built
   connection reports link-down, and no construction path can publish `true`* — which is exactly
   the surviving direction, in the mutated crate, where the 0.85 gate can see it. The claim is
   therefore "closes the unforced half", not "closes an unforced row".

2. **What becomes simpler.** The eleven literals are ~14 lines each; the builder makes them one
   to three. More importantly the two `streaming.rs` fixtures (`Link::connect` at 328–350 and
   `bare_connection` at 400–424) exist *because* the shape is expensive — their doc comment at
   393–399 says `bare_connection` is "a `ReplicaConnection` wired the same way `Link::connect`
   wires one". Two hand-maintained near-copies of the same wiring, which is the test-side
   restatement of the production-side drift. Both collapse onto
   `ReplicaLink::for_test(..).with_ack_interval(CADENCE)`.

3. **What becomes cheap that is currently not attempted.** With a constructor, `ReplicaLink`
   is a value, so `ReplicaReplicationHandler::link()` can be asserted directly: *the link a
   handler hands its connections carries the handler's own `link_up`/`sync_refusal`/`net_bytes`
   handles* — one test covering illegal combination **4** for all three at once, where today
   FM-REPLICATION-063 needs a full duplex-primary, a spawned reconnect loop and a 5 s polling
   deadline (`replica/tests.rs:313-370`) to prove it for one of them.

For RC10, `ReplicaWiring` is a value with `PartialEq`-able policy parts, so "boot and demotion
wire a handler the same way" becomes a table-driven assertion in `frogdb-server` instead of two
comment blocks — the seam issue 18 was found at, now with a test at it.

## Spec position (LOCKED area)

`.scratch/hardening/specs/replication-failure-modes.md` is `Status: LOCKED`. Grepping it for
every file and function 54 touches:

- **No row pins construction.** No `Invariant` requires `ReplicaConnection` to be built by a
  literal, requires a specific field count, or names any of the five setters as a *mechanism*.
- **FM-REPLICATION-027** (spec `:593-603`) is the row this proposal is closest to. Its
  `Invariant` (`:600`) states the `link_up` fail-closed property and cites
  `frogdb-replication/src/replica/mod.rs:388` and `replica/mod.rs:286-341`. **Both citations
  are already stale in the current tree** — `connect_and_sync`'s `store(false)` is at
  `mod.rs:523`, and the retry loop is `start` at `mod.rs:398-480`. This proposal does not
  change the invariant's meaning; it makes it structural. Per the replication-correctness D2
  execution discipline, *"rows may move file:line citations but not meaning"* — so refreshing
  those two citations is in-bounds and is owed regardless of whether 54 lands.
- **FM-REPLICATION-063** (spec `:1472-1482`) is the one row with a name-level dependency: its
  `Forced by` list (`:1481`) includes
  `set_net_bytes_counters_wires_the_handlers_own_connections_to_it` (`replica/tests.rs:313`,
  tagged `// FM-REPLICATION-063` at `:304` — the *only* FM tag in that file).

### RC10 takes one spec edit, deliberately, and here is why

An earlier draft claimed "no spec edit required, by design", on the grounds that keeping the
setters keeps the forcing test's name and subject and therefore keeps `just lint-failure-modes`
green. Reading `scripts/failure-modes.py` shows that justification is weaker than it looked:

- The gate binds **only** backticked test names in a `Forced by` cell to `// FM-` tag comments
  (`parse_forced_by`, `BACKTICKED_RE`, `scan_tags`, `check`). It never parses `Invariant` prose
  — the sole prose-level check is `check_invariant_vocabulary`, which validates `INV-*`
  identifiers against a catalog and has nothing to do with method names.
- So **deleting `set_net_bytes_counters` does not break the gate** as long as the test *name*
  `set_net_bytes_counters_wires_the_handlers_own_connections_to_it` survives — even with its
  body rewritten over `ReplicaWiring`. "The lint forces us to keep the setters" was never true.

That removes the technical argument for coexistence and leaves only the design one, which cuts
the other way. Keeping all five setters *and* adding `ReplicaWiring` gives RC10 **two doors** —
directly contradicting this proposal's title — and, worse, leaves the silently-unwired handler
constructible: the exact defect (`mod.rs:342-344`, `:353-355`: "must be called by every
construction site … otherwise a full resync only stages for the next boot") that RC10 exists to
close. A door you can still walk around is a comment with a struct next to it.

**This proposal therefore takes the deletion**, and declares the spec edit that follows:

1. The four policy setters are deleted (`connect_factory` stays, see above).
2. `set_net_bytes_counters_wires_the_handlers_own_connections_to_it` is **renamed** —
   `replica_wiring_wires_the_handlers_own_connections_to_its_counters` — because a test named
   after a method that no longer exists is exactly the stale-citation rot the spec discipline
   forbids. Its body keeps its subject unchanged: build a handler with a `ReplicaWiring`
   carrying the caller's `Arc`, drive a frame through, assert the caller's copy observed the
   increment. The mutant it kills is the same one (a `new` that accepts the counters and drops
   them).
3. FM-REPLICATION-063's `Forced by` cell (`:1481`) is edited to carry the new name, and the
   `// FM-REPLICATION-063` tag (`:304`) travels with the test. **This is a name-level `Forced
   by` edit — declared, not incidental.** No `Observable`, `NOT observable`, `Invariant`,
   `Outcome variant` or `Bug refs` cell changes; the row forces exactly the property it forced
   before. Under the D2 phrasing ("rows may move file:line citations but not meaning") this is
   in-bounds, and it is the only spec edit RC10 needs.
4. The optional FM-REPLICATION-027 citation refresh above is separate and is a line-number
   correction, not a meaning change.

The rejected alternative — keep the name, rewrite the body — is *cheaper for the gate and worse
for the reader*: it would leave the spec's `Forced by` list naming a method the tree no longer
has, with a green lint certifying the lie. Deleting the four setters and paying one honest
rename is the trade this proposal makes.

RC2 needs **no** spec edit: it renames no test and moves no `// FM-` tag.

Push discipline: `just mutants-diff frogdb-replication` before pushing either landing. Any new
forcing test goes in `frogdb-replication`, not in `frogdb-server` integration tests — see
[Testability](#testability-improvement) point 1, which is the same mistake FM-REPLICATION-027
already made.

## Risks / scope boundaries vs siblings

**Behavioral risk: low.** Both landings are construction-path only. Every field keeps its type
and its value; the two deletions are a field nothing reads (`_primary_addr`) and a duplicate
handle to an `Arc` the connection already reaches (`state`). No wire format, no timing, no
ordering. The one delta a reader must be told about is the `Syncing → Connected` fixture change
documented above.

**Real risk: the `state` derivation.** Deriving `state` from `offsets` is the one change that
is not purely syntactic — it assumes all eleven sites pass the same `Arc` to both, which was
verified by reading all eleven, and which is what illegal combination **2** is about. If any
future path legitimately needs them to differ, this change forecloses it. Assessment: they must
not differ (`psync` correlates the offset and the replication id in adjacent lines), so
foreclosing it is the point.

**Public-API risk: deleting four `pub fn`s.** `ReplicaReplicationHandler` is re-exported from
`frogdb-core` and `frogdb-server`; deleting public methods is a source break. Every caller is
in-workspace (the 15 `new` sites and the setter sites enumerated above), FrogDB is
pre-production, and CLAUDE.md's development philosophy explicitly accepts breaking changes that
improve the implementation. Recorded so it is a decision, not an oversight.

### Conflict edges

Both siblings are **on disk** and were read: `53-fullsync-emitter.md` and
`55-adopt-full-sync-landing.md`. An earlier draft said 53 "is not yet written" and inferred its
target second-hand — that was false, and the inferred edge ("no file overlap") was wrong.

| edge | assessment |
|---|---|
| **53 (emit side)** — 53's production target is `replica_session.rs` (`stream_checkpoint` `:888-988`, `stream_live_dataset` `:1018-1104`) plus a new `fullsync/emitter.rs`; 54 touches none of those. **But 53's phase 3 ("fixture collapse", 53 Effort table) deletes `encode_checkpoint_body` (`connection.rs:852-881`) and rewrites its call site at `:922` — inside `checkpoint_fixture_with_tail` (`:912-947`), the same function that holds 54's literal at `:934`.** The dataset side has the same shape: `encode_dataset_body` (`:1076`), called at `:1142` inside `dataset_fixture_with_tail` (`:1134-1167`), 54's literal at `:1154`. (53's write-up names only `encode_checkpoint_body` and `receiver.rs::encode_envelope`; `encode_dataset_body` is the third fixture of the same kind and will follow it.) | **Same-function overlap in the test-fixture region — not the same lines.** 53 rewrites the *body-encoding* statement near the top of each fixture; 54 rewrites the *connection literal* twenty lines below it. Both are `git`-mergeable in principle and both are annoying to resolve by hand. **Land 53 first** (it is also 55's prerequisite, below): 54 then rewrites two fixtures whose top halves have already settled. Second contact point: `frogdb-replication/src/lib.rs:68` if 53 edits the re-export list — a one-line textual conflict at worst. |
| **55 (landing tail)** — the real collision, and an earlier draft named the wrong three. 55 extracts `adopt_full_sync()` from `receive_snapshot`'s tail (`connection.rs:404-418`) and `receive_checkpoint`'s tail (`:475-497`). **54 rewrites `self.state` → `self.offsets.state()` at `:409` and `:483` — two statements that sit inside those exact ranges.** | **Hard conflict: line-level, in both extracted tails.** This is a direct overlap, and it is the whole of the 55↔54 conflict. |
| **Two collisions an earlier draft named are false, and are withdrawn** | (a) `mod.rs:499` — 55's Files table *cites* the `snapshot_installer` slot at `:499` as context; it does not edit `mod.rs`'s literal, so the overlap is citation-only. (b) 55's test ranges `connection.rs:966-1065` and `:1182-1294` do **not** contain 54's literals at `:934` and `:1154` — both literals sit arithmetically before their range, and 55 states it edits no tests ("No test edits required", 55 Effort). (c) Shared `set_state` (`:159-163`) is real but **soft**: neither proposal changes its body or its signature — 55 calls it, 54 makes it the only writer. |
| **55 land order** | **55 first, 54 second**, re-derived from the real collision above. After 55, the two `self.state` readers 54 must rewrite have become **one**, inside `adopt_full_sync` — so 54's `state`-derivation diff shrinks from four readers to three and stops touching two method bodies that 55 has just rewritten. The reverse order means 55 extracting a tail whose state access 54 has already changed under it. Independently: 55 is a within-function extraction confined to method bodies, 54 rewrites the type's construction shape and every literal — rebasing the wide change onto the narrow one is the cheaper direction. The two are **mutually reinforcing**: 55's `adopt_full_sync` ends by flipping to `Streaming` through `set_state`, and 54 is what makes that the sole path — together they close FM-REPLICATION-001's ordering and FM-REPLICATION-027's fail-closed pairing at the same seam. |
| **55 (`offset.rs`)** | 55 reads `ReplicaOffset::reset_to` (`:498-516`); 54 adds `pub(super) fn state()`. **Additive, no conflict.** 54's dependency on `ReplicaOffset::new`'s three-argument signature (`:463-475`) is unchanged by 55. |
| **54 internal** | RC2 and RC10 land together but are separable: RC2 is entirely inside `frogdb-replication`; RC10 additionally edits `role_manager.rs` and `replication_init.rs`. RC2 first — `ReplicaLink` is what `ReplicaWiring` hands down. RC10 has **no overlap with 53 or 55** (neither touches `mod.rs`'s `new`, `role_manager.rs` or `replication_init.rs`) and can land in parallel with either. |

**Chain ruling (from review 55, adopted here):**

```
55's tag hotfix  →  53  →  55  →  54
```

54's own `_primary_addr` hotfix is independent of all three and belongs at the head of the
chain alongside 55's tag hotfix — it is a one-line deletion per site with no semantic content,
and any rebase resolves it trivially. (Landing it *after* 54 would be pointless; landing it
between 53 and 55 would put a whole-file sweep in the middle of a chain for no gain.)

### Relationship to `.scratch/replication-correctness/`

The campaign's **§8 D2 ruled 2026-08-10: full restructure (iii) authorized** —
`replica_session.rs` becomes an explicit phase state machine with a pure `step(view, event) ->
(phase, effects)`. Two things follow.

1. **No file collision.** D2 (iii) is `replica_session.rs` — the **primary**-side session
   (`replica_session.rs:1-3`). 54 is entirely replica-side (`replica/*`) plus two
   `frogdb-server` wiring sites. Different files, different side of the link.
2. **One genuine overlap: D2 (ii).** D2 authorizes splitting *"the PSYNC arm selection out of
   `ReplicaConnection::psync` into a pure function"*, citing `replica/connection.rs:224` — the
   same function 54 touches (54 changes only its two field reads, `:229-230`, to go through
   `offsets.state()`). These are compatible and mutually reinforcing: extracting a pure arm
   selector is easier once `psync` reads one state handle instead of two. **Coordinate: if D2
   (ii) is in flight, 54's RC2 should land first** — it is smaller, it is behavior-neutral, and
   it removes a field the extraction would otherwise have to carry.
3. **Gate implication.** D2's discipline requires *the full mutation gate (0.85), not just
   `mutants-diff`*, after (iii), "because the restructure moves most forcing-test targets". 54
   moves no forcing-test target — RC2 renames nothing, and RC10's one rename keeps the test in
   `frogdb-replication` with its tag — so `mutants-diff` is the correct bar for 54 itself. If
   54 lands *inside* a D2 issue chain, it is covered by that chain's full re-run either way.

Campaign issues are not yet decomposed (`replication-correctness/README.md`: *"Issues: none yet
— decomposition pending after PRD ruling"*), so there is no issue-level dependency to name.

## Effort + independently-landable hotfix

| item | size | notes |
|---|---|---|
| **Hotfix: delete `_primary_addr`** | **XS** | Provably dead — never read anywhere in the tree. Deletes **12 lines** (the field at `connection.rs:115` + 11 literal lines: `mod.rs:492`, `connection.rs:625/682/804/936/1156/1316/1460/1547`, `streaming.rs:339/411`) **plus `use std::net::SocketAddr;` at `connection.rs:12`**, whose only user is that field — the import must go in the **same commit** or `just lint` fails on `-D warnings` (`Justfile:320`). Zero behavior, zero spec contact. **Land at the head of the chain, on its own.** |
| RC2 — `ReplicaLink` + `new` + test builder, `state` derived, visibility tightened | **S** | 11 call sites, all in `frogdb-replication`. Plus one `pub(super) fn state()` on `ReplicaOffset` and 3–4 reader updates (3 if 55 landed first). |
| RC2 tail — `lint-gates` entry for literal construction | **XS** | Compile-free grep, same shape as the existing fifteen; `agents/seam-lints.md` table row + `Justfile:329` list. Optional but it is what keeps the door shut. |
| RC2 test — in-crate FM-REPLICATION-027 fail-closed forcing test | **XS** | Socket-free; the substantive win. Forces the direction `link_up_reports_true_once_the_stream_is_running` cannot. |
| RC10 — `ReplicaWiring` through `new`, four policy setters deleted, two wiring sites adapted | **S** | `mod.rs` + `replication_init.rs:223-303` + `role_manager.rs:655-739`, plus 15 `new` call sites across 4 files (11 of them `ReplicaWiring::default()`). |
| RC10 tail — FM-REPLICATION-063 forcing-test rename + `Forced by` edit | **XS** | One test renamed, one spec cell edited, tag travels with the test. Declared spec edit — see [Spec position](#rc10-takes-one-spec-edit-deliberately-and-here-is-why). |
| Gate | — | `just mutants-diff frogdb-replication` before push. `just lint-failure-modes` must be re-run after the RC10 rename (it is the one thing in 54 that the gate can see). |
