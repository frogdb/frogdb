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
`for_test` **adapter** collapses eleven literals to eleven calls and lets the constructor
enforce the pairings. `ReplicaWiring` folds the four *policy* setters into a value that the two
production wiring paths — boot (`replication_init.rs`) and runtime demotion
(`role_manager.rs`) — construct and can be compared by equality rather than by reading two
comment blocks.

Both are behavior-neutral. The **leverage** is that the invariants FM-REPLICATION-027 states
in prose become constructor preconditions, testable *inside the mutated crate* — where today
that row's forcing tests are not.

## Files involved

Crate names are `frogdb-replication` and `frogdb-server`; the spec cites them by package name,
the tree lays them out under `frogdb-server/crates/`. All paths absolute-from-repo-root.

| path | lines | what 54 touches | verified |
|---|---|---|---|
| `frogdb-server/crates/replication/src/replica/connection.rs` | 1884 | `struct ReplicaConnection` 109–153 (**12 fields**: 114, 115, 116, 117, 118, 122, 126, 130, 135, 140, 145, 152); `set_state` 159–163; **8 test literals** at 623, 680, 802, 934, 1154, 1314, 1458, 1545 (`#[cfg(test)]` opens at 577) | ✅ |
| `frogdb-server/crates/replication/src/replica/mod.rs` | 560 | `struct ReplicaReplicationHandler` 152–215 (**16 fields**); `new` 230–269; **5 setters** at 275, 336, 345, 356, 364; **1 production literal** 490–503 inside `connect_and_sync` 482–525 | ✅ |
| `frogdb-server/crates/replication/src/replica/streaming.rs` | 842 | **2 test literals** at 337, 409 (`#[cfg(test)]` opens at 256); production field reads at 62, 99, 145, 177, 178, 185, 214, 231, 242, 248 | ✅ |
| `frogdb-server/crates/replication/src/replica/offset.rs` | 998 | read-mostly: `struct ReplicaOffset` 443–453 **already holds `state`** (452); `new` 463–474; the stint-ordering contract in the doc comment at 446–451. Needs one added `pub(super) fn state()` | ✅ |
| `frogdb-server/crates/replication/src/replica/tests.rs` | 492 | handler-setter tests at 61, 65, 110, 210, 274, 348, 352, 477; FM-REPLICATION-063's in-crate forcing test at 304–313 | ✅ |
| `frogdb-server/crates/server/src/server/replication_init.rs` | 552 | boot wiring path: `new` 223–229, setters 230, 234, 237, 262, 294, 302 | ✅ |
| `frogdb-server/crates/server/src/role_manager.rs` | 1632 | `struct RealReplicaStreamer` 507–529+; `build_handler` 655–739, setters at 673, 674, 676, 684, 708, 735 | ✅ |

**Counts differ from the candidate.** The candidate said "9 struct-literal construction sites
(mod.rs:490-503 + 8 test sites)". The true count is **11**: 1 production + 10 test (8 in
`connection.rs`, 2 in `streaming.rs` — the two `streaming.rs` fixtures were missed). The
handler numbers are exact: **16 fields**, **5 setters**, ranges 152–215 and 275–364 as cited.

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
   (`connection.rs:182-184`) hands it to the streaming decoder exactly once. A connection born
   with bytes in it would seed a decoder with data no payload read ever produced.

4. **Fresh `Arc`s where the handler's shared ones belong.** `link_up`, `sync_refusal` and
   `net_bytes` are documented as *"Shared with the owning `ReplicaReplicationHandler`"*
   (`connection.rs:123-126`, `136-140`, `146-152`). A production path that minted fresh ones
   would silently disconnect `INFO`'s `master_link_status`, `master_sync_error`, and
   `total_net_repl_input_bytes` from what the link is actually doing — the FM-REPLICATION-027
   and FM-REPLICATION-063 observables — while every test still passed.

5. **Stint ordering.** `offset.rs:446-451` states the contract in prose: *"Callers must
   therefore open the stream's stint **before** building its connections."* A construction-order
   precondition that exists only in a doc comment on a different type.

And one field is simply dead: **`_primary_addr` is never read** — grep for it outside field
declarations returns only unrelated locals in `replication_init.rs`/`cluster_init.rs`. It is
underscore-prefixed to silence the warning and hand-written at all eleven sites.

### RC10 — an obligation the interface states but cannot enforce

`ReplicaReplicationHandler::new` (`mod.rs:230-269`) takes five arguments and leaves five
decisions to setters. Two of those setters carry doc comments that admit the problem:

```
mod.rs:341    /// Must be called by every construction site that has shards to install into
mod.rs:342    /// (boot-configured replica and runtime `REPLICAOF` demotion alike),
mod.rs:343    /// otherwise a full resync only stages for the next boot.
```

```
mod.rs:353    /// be called by every construction site that wants real input bytes
mod.rs:354    /// reported (boot-configured replica and runtime `REPLICAOF` demotion
mod.rs:355    /// alike); otherwise the handler counts into a counter nothing reads.
```

There are exactly two such sites, and they are two independent spellings of the same five
decisions:

| decision | boot (`replication_init.rs`) | demotion (`role_manager.rs::build_handler`) |
|---|---|---|
| ack cadence | `:230` from `config.replication.ack_interval_ms` | `:673` from `self.ack_interval_ms` |
| net-byte counters | `:234` unconditional, `tracker.net_bytes_handle()` | `:676` **conditional** on `self.net_bytes.is_some()` |
| snapshot installer | `:237` `LiveSnapshotInstaller::for_config(...)` | `:674` `self.snapshot_installer.clone()` |
| shared offset | `:302` gated on `config.cluster.enabled` | `:684` gated on `self.shared_offset.is_some()` |
| connect factory | `:262` turmoil / `:294` TLS | `:708` turmoil / `:735` TLS |

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
  `ReplicationState` handle per connection, not two that must agree.

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

`for_test(stream, offsets) -> ReplicaConnection` is the **adapter** between what a test actually
chooses (a duplex and an offset seed) and the ten-field shape, with a
`for_test_streaming(stream, offsets)` sibling for the two `streaming.rs` fixtures that need to
start already `Streaming` — expressed as `for_test(..).streamed()`, so even the test path goes
through `set_state` and cannot desynchronise the pair.

**Closing the door.** `streaming.rs` holds an `impl ReplicaConnection` block
(`streaming.rs:43`), so the fields cannot all become module-private. But the production reads
there are only `offsets`, `ack_interval`, `stream`, `net_bytes` (lines 62, 99, 145, 177, 178,
185, 214, 231, 242, 248) — `state`, `connection_state` and `link_up` are read **only** in
`connection.rs`. So: those three drop to private, the other seven stay `pub(super)`. That makes
combination **1** literally unconstructible outside `connection.rs`. The residual — a future
literal inside `connection.rs` itself — is closed by a sixteenth entry in the `lint-gates`
family (`agents/seam-lints.md`): *"`ReplicaConnection` is constructed only through `new` /
`for_test`"*, a compile-free grep in the same shape as `lint-format-float`.

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

**`connect_factory` deliberately stays a setter.** It is a transport choice made by
`#[cfg(feature = "turmoil")]` / TLS-config branches at four production sites and seven test
sites, and it is not a wiring *decision* — folding it in would make `ReplicaWiring` a
pass-through slot for a closure every caller sets separately. Scoping it out is what keeps the
struct honest.

**Deletion test on `ReplicaWiring` — does it concentrate, or relocate?** It concentrates, and
the evidence is `RealReplicaStreamer`. Delete `ReplicaWiring` and the four decisions go back to
living as `RealReplicaStreamer`'s fields *plus* six inline expressions in
`replication_init.rs`, with the two spellings only comparable by eye. Keep it and
`RealReplicaStreamer` holds one `ReplicaWiring` instead of three loose fields, `init_replication`
builds one, and "boot and demotion wire a handler identically" becomes a statement about two
values of one type — the exact drift that produced issue 18. That is concentration: the
knowledge moves from two files' worth of call-sequence into one type's field list, and the
handler stops advertising four setters that its own doc comments say are mandatory.

Where it would be a *pass-through* — `connect_factory` — this proposal leaves it out. The
setters are not deleted for `net_bytes` (see [Spec position](#spec-position-locked-area)); they
become the escape hatch, not the wiring path.

## Testability improvement

**The interface is the test surface.** Today the surface is twelve fields, so a test that wants
to assert one thing states twelve. That has three concrete costs, and the fix removes all three.

1. **What becomes writable that is not writable now.** FM-REPLICATION-027's invariant — a
   `link_up` that cannot be `true` without `Streaming` — has **no in-crate forcing test**. Its
   three `Forced by` tests (`test_replica_handles_rapid_reconnect`,
   `test_info_replication_master_link_status_tracks_connection`,
   `test_info_replication_master_link_status_down_before_connected`) all live in
   `frogdb-server/crates/server/tests/integration_replication.rs` (:5539, :3873, :3909). Per
   CLAUDE.md, `cargo mutants -p frogdb-replication` runs only that package's own tests, so a
   mutant in `set_state` that drops the `link_up.store` is scored against nothing in the crate
   that owns it. With a constructor there is a socket-free unit test in `frogdb-replication`:
   *a freshly built connection reports link-down; the pair moves only through `set_state`; no
   construction path can publish `true`.* That is a forcing test **in the mutated crate**, which
   is where the 0.85 gate can see it.

2. **What becomes simpler.** The eleven literals are ~14 lines each; `for_test` makes them one.
   More importantly the two `streaming.rs` fixtures (`Link::connect` at 328–350 and
   `bare_connection` at 400–424) exist *because* the shape is expensive — their doc comment at
   393–399 says `bare_connection` is "a `ReplicaConnection` wired the same way `Link::connect`
   wires one". Two hand-maintained near-copies of the same wiring, which is the test-side
   restatement of the production-side drift. Both collapse onto `for_test`.

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
- **FM-REPLICATION-027** (spec :593–609) is the row this proposal is closest to. Its
  `Invariant` states the `link_up` fail-closed property and cites
  `frogdb-replication/src/replica/mod.rs:388` and `replica/mod.rs:286-341`. **Both citations
  are already stale in the current tree** — `connect_and_sync`'s `store(false)` is at
  `mod.rs:523`, and the retry loop is `start` at `mod.rs:398-480`. This proposal does not
  change the invariant's meaning; it makes it structural. Per the replication-correctness D2
  execution discipline, *"rows may move file:line citations but not meaning"* — so refreshing
  those two citations is in-bounds and is owed regardless of whether 54 lands.
- **FM-REPLICATION-063** (spec :1472–1481) is the one row with a name-level dependency: its
  `Forced by` list includes `set_net_bytes_counters_wires_the_handlers_own_connections_to_it`
  (`replica/tests.rs:313`, tagged `// FM-REPLICATION-063` at :304). `just lint-failure-modes`
  checks spec↔test agreement in both directions and runs in `just lint`, so **renaming or
  deleting `set_net_bytes_counters` would break that gate.**

**Spec-edit expectation: none required, by design.** RC10 keeps `set_net_bytes_counters` (and
its siblings) as setters alongside the `ReplicaWiring` constructor path, so the forcing test
keeps its name and its subject. The honest alternative — deleting the setters outright — *would*
require editing FM-REPLICATION-063's `Forced by` row and retitling the test, which is a
spec edit and must be declared as one. This proposal does not take it. The optional
FM-REPLICATION-027 citation refresh is a line-number correction, not a meaning change.

Push discipline: `just mutants-diff frogdb-replication` before pushing either landing. Any new
forcing test goes in `frogdb-replication`, not in `frogdb-server` integration tests — see
[Testability](#testability-improvement) point 1, which is the same mistake FM-REPLICATION-027
already made.

## Risks / scope boundaries vs siblings

**Behavioral risk: low.** Both landings are construction-path only. Every field keeps its type
and its value; the two deletions are a field nothing reads (`_primary_addr`) and a duplicate
handle to an `Arc` the connection already reaches (`state`). No wire format, no timing, no
ordering.

**Real risk: the `state` derivation.** Deriving `state` from `offsets` is the one change that
is not purely syntactic — it assumes all eleven sites pass the same `Arc` to both, which was
verified by reading all eleven, and which is what illegal combination **2** is about. If any
future path legitimately needs them to differ, this change forecloses it. Assessment: they must
not differ (`psync` correlates the offset and the replication id in adjacent lines), so
foreclosing it is the point.

### Conflict edges

Proposal **55 is on disk** and was read; **53 is not yet written**, so its edge is stated by
file from what 55 independently identifies as 53's target.

| edge | assessment |
|---|---|
| **53 (emit side)** — 55's Files table names 53's target as `replica_session.rs` (`stream_checkpoint` :888, `stream_live_dataset` :1018), the primary's emit path. 54 touches **none** of `replica_session.rs`, `primary/mod.rs`, `primary/replay.rs`, `primary/ring_buffer.rs`. | **No file overlap.** Only contact point is `frogdb-replication/src/lib.rs:68` (`pub use replica::{ReplicaConnection, ReplicaReplicationHandler}`) if 53 edits the re-export list — a one-line textual conflict at worst. **Land order: independent.** |
| **55 (landing tail)** — **not** `offset.rs`/`apply.rs` as this proposal first assumed. 55 extracts `adopt_full_sync()` from `ReplicaConnection::receive_snapshot` (`connection.rs:404-418`) and `receive_checkpoint` (`:475-497`), and touches `replica/mod.rs:499` and `:511-519`. | **Hard conflict — same two files, overlapping hunks.** Three concrete collisions: (a) `mod.rs:499` is the `snapshot_installer:` line **inside the production literal at 490–503** that 54 replaces with a `ReplicaConnection::new(..)` call; (b) 55's test edits at `connection.rs:966-1065` and `:1182-1294` **contain 54's literal sites at :934 and :1154**; (c) both depend on `set_state` (`:159-163`) — 55 calls it as the tail's final step, 54 makes it the *only* writer of the `link_up`/`connection_state` pair. |
| **55 land order** | **55 first, 54 second.** 55 is a within-function extraction whose diff is confined to method bodies; 54 rewrites the type's construction shape and every literal. Rebasing 54 onto 55 is mechanical (the literals 54 deletes are ones 55 only reads); rebasing 55 onto 54 means re-deriving its tail against a changed constructor. The two are **mutually reinforcing**: 55's `adopt_full_sync` ends by flipping to `Streaming` through `set_state`, and 54 is what makes that the sole path — together they close FM-REPLICATION-001's ordering and FM-REPLICATION-027's fail-closed pairing at the same seam. |
| **55 (`offset.rs`)** | 55 reads `ReplicaOffset::reset_to` (`:498-516`); 54 adds `pub(super) fn state()`. **Additive, no conflict.** 54's dependency on `ReplicaOffset::new`'s three-argument signature (`:463`) is unchanged by 55. |
| **54 internal** | RC2 and RC10 land together but are separable: RC2 is entirely inside `frogdb-replication`; RC10 additionally edits `role_manager.rs` and `replication_init.rs`. RC2 first — `ReplicaLink` is what `ReplicaWiring` hands down. RC10 has **no overlap with 53 or 55** and can land in parallel with either. |

### Relationship to `.scratch/replication-correctness/`

The campaign's **§8 D2 ruled 2026-08-10: full restructure (iii) authorized** —
`replica_session.rs` becomes an explicit phase state machine with a pure `step(view, event) ->
(phase, effects)`. Two things follow.

1. **No file collision.** D2 (iii) is `replica_session.rs` — the **primary**-side session
   (`replica_session.rs:1-3`). 54 is entirely replica-side (`replica/*`) plus two
   `frogdb-server` wiring sites. Different files, different side of the link.
2. **One genuine overlap: D2 (ii).** D2 authorizes splitting *"the PSYNC arm selection out of
   `ReplicaConnection::psync` into a pure function"*, citing `replica/connection.rs:224` — the
   same function 54 touches (54 changes only its two field reads, :229–230, to go through
   `offsets.state()`). These are compatible and mutually reinforcing: extracting a pure arm
   selector is easier once `psync` reads one state handle instead of two. **Coordinate: if D2
   (ii) is in flight, 54's RC2 should land first** — it is smaller, it is behavior-neutral, and
   it removes a field the extraction would otherwise have to carry.
3. **Gate implication.** D2's discipline requires *the full mutation gate (0.85), not just
   `mutants-diff`*, after (iii), "because the restructure moves most forcing-test targets". 54
   moves no forcing-test target (no test is renamed, no `// FM-` tag moves crate), so
   `mutants-diff` is the correct bar for 54 itself. If 54 lands *inside* a D2 issue chain, it
   is covered by that chain's full re-run either way.

Campaign issues are not yet decomposed (`replication-correctness/README.md`: *"Issues: none yet
— decomposition pending after PRD ruling"*), so there is no issue-level dependency to name.

## Effort + independently-landable hotfix

| item | size | notes |
|---|---|---|
| **Hotfix: delete `_primary_addr`** | **XS** | Provably dead — never read anywhere in the tree. Removes one field and eleven lines. Zero behavior, zero spec contact, no dependency on RC2 or on siblings 53/55. **Land this first, on its own.** |
| RC2 — `ReplicaLink` + `new` + `for_test`, `state` derived, visibility tightened | **S** | 11 call sites, all in `frogdb-replication`. Plus one `pub(super) fn state()` on `ReplicaOffset` and 4 reader updates. |
| RC2 tail — `lint-gates` entry for literal construction | **XS** | Compile-free grep, same shape as the existing fifteen; `agents/seam-lints.md` table row + `Justfile:329` list. Optional but it is what keeps the door shut. |
| RC2 test — in-crate FM-REPLICATION-027 forcing test | **XS** | Socket-free; the substantive win. |
| RC10 — `ReplicaWiring` through `new`, two wiring sites adapted | **S** | `mod.rs` + `replication_init.rs:223-303` + `role_manager.rs:655-739`. Setters retained (spec-neutral). |
| Gate | — | `just mutants-diff frogdb-replication` before push. No `just lint-failure-modes` impact expected. |
