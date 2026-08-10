# 61 — `PrimarySnapshotHooks`: one door for the three seams a full resync reaches through

Size: **S/M** (RC9) **+ S doc** (RC12, independently landable). Area: **replication (LOCKED,
0.85 gate)**; the wiring half lands in `frogdb-server` (unlocked). Latent — **no live bug**,
reachability argument below. Amends **ADR-0004**'s Consequences paragraph in two places; does
not reopen its decision.

Covers exploration-lane candidates **RC9** (`Primary 3× RwLock<Option<hook>> post-construction
setters`) and **RC12** (`ADR-0004 cost paragraph stale`). Both were verified against the tree
at `9a62f79b` and **re-verified at `6e99f567`** — `git diff --stat 9a62f79b..HEAD` shows only
`.scratch/arch-deepening/proposals/*.md`, so no source line cited here has moved. The lane doc
was written at `08c143d6` and **RC12's specific claim is wrong** — see
[RC12](#rc12--the-adr-0004-cost-paragraph-is-stale-in-a-bigger-way-than-filed).

## Summary

`PrimaryReplicationHandler` reaches state it does not own through three injected seams —
`PreCheckpointHook`, `LiveSnapshotSource`, `FunctionSnapshotHook`. Each is stored as its own
`RwLock<Option<…>>` field, each has its own public setter and its own `pub(crate)` getter, and
each carries its own doc paragraph explaining that it is "installed after construction". All
three are consumed by **one function** — `ReplicaSession::handle_full` — and all three are
installed for **one** production handler. The installation is nevertheless split across two
files in two boot phases, and nothing anywhere can state, let alone assert, "this node's
Primary side is fully wired".

The change is to give the three seams one door: a plain `PrimarySnapshotHooks` value that names
all three, built **once** in the server crate by a `primary_snapshot_hooks(…)` factory — the
exact shape ADR-0004 already chose for the Replica half of the same problem, one named function
in the server crate that turns owned state into the seam values (`LiveSnapshotInstaller::for_config`)
— and handed to `PrimaryReplicationHandler::new` so a construction site that forgets a seam is a
**compile error rather than a runtime warning**. That sentence is quoted verbatim from ADR-0004
as the cost it accepts; supplying the missing half of the mitigation is why the ADR is being
amended rather than re-litigated. (Vocabulary, deliberately: the three closures are the
**adapters** — each satisfies one seam's interface. `primary_snapshot_hooks` is their factory,
not itself an adapter.)

Behavior-neutral. The **leverage** is that "the Primary's full-resync seams are all wired" stops
being a property held by reading two files in two phases and becomes a value a unit test reads —
which is the lesson the *same file* already learned once, from a real bug, and applied to
`backlog_config` but not to the hooks (`replication_init.rs:49-59`).

## Files involved (verified at `9a62f79b`, re-verified at `6e99f567`)

All paths under `frogdb-server/crates/` unless noted. Line counts are whole-file.

| path | lines | what 61 touches | verified |
|---|---|---|---|
| `replication/src/primary/mod.rs` | 865 | the three seam type aliases `PreCheckpointHook` `:44-56`, `LiveSnapshotSource` `:58-71`, `FunctionSnapshotHook` `:73-89` (**read-only** — the closure types do not change); the three fields + their doc paragraphs `:188-193`, `:194-200`, `:201-205`; `new` `:242-302` (`#[allow(clippy::too_many_arguments)]` `:244`, signature `:245-259`, the three `RwLock::new(None)` initializers `:289-291`); the three setter/getter pairs `:309-320`, `:322-333`, `:335-345` | ✅ |
| `replication/src/replica_session.rs` | 4574 | **consumers only, unchanged**: `function_snapshot_hook()` `:786-788`, `pre_checkpoint_hook()` `:807-821`, `live_snapshot_source()` `:1027-1032`. Test fixtures that construct a handler: `make_handler` ctor `:1983`, second fixture ctor `:2027`, inline ctor `:3736`; test installer `with_live_dataset` `:1907-1916` (fn line `:1910`, install `:1911`; callers `:2139`, `:2426`, `:2566`, `:2654`); `set_pre_checkpoint_hook` in tests `:2842`, `:2927` | ✅ |
| `replication/src/primary/tests.rs` | 1301 | 4 handler construction sites — `divergence_handler` ctor `:18`, inline `:615`, inline `:669`, `stint_handler` ctor `:742`; the only crate-local wiring test today, `the_function_snapshot_hook_is_installed_and_handed_back_callable` (doc `:1030-1033`, `#[test]` `:1034`, fn `:1035`, body through `:1068`) (**untagged** — no `// FM-` above it; the file's FM tags end at `:986`) | ✅ |
| `server/src/server/replication_init.rs` | 552 | `init_replication`'s **signature** `:106-129` (`#[allow(clippy::too_many_arguments)]` `:106`, `shard_senders` param `:116`, return `:129`) — gains one parameter; the single **production** construction site `:147-164`; pre-checkpoint wiring block `:165-184` (closure body `:170-184`); live-snapshot wiring `:185-192` (install `:190-192`); the `backlog_config` precedent `:49-74` and its unit tests `:429-478` inside the `#[cfg(test)] mod tests` at `:410-411`; test-only ctor `:483` | ✅ |
| `server/src/server/mod.rs` | 598 | the `init_replication` **call site** `:264-275` — gains one argument, `&infra.function_registry`; the third wiring site, one phase later: `:305-342` — **duplicated comment block `:305-311` = `:313-319` byte-for-byte** (md5 match), the shutdown-capture warning `:320-324`, the `if let Some(ref handler)` guard `:325`, the closure `:327-341` | ✅ |
| `server/src/server/init.rs` | — | `InfraResult::function_registry` `:94`, built `:412`, published `:488`; `shard_senders` built `:215-230`, published `:465` — both exist **before** phase 2 | ✅ |
| `server/src/server/subsystems.rs` | — | `start_subsystems` `:76` — where acceptors start; its only caller is `server/runtime.rs:22`, i.e. after every wiring site above (the reachability proof) | ✅ |
| `server/src/server/cluster_init.rs` | — | test-only ctor `:1492` (`split_brain_handler`) | ✅ |
| `adr/0004-replication-runtime-seams.md` | 83 | RC12: `:64-76`; RC9: `:76-83` | ✅ |
| `.scratch/hardening/specs/replication-failure-modes.md` | — | header `:3-6` (the recorded post-hardening gate numbers RC12 needs); FM-REPLICATION-001 `:95-105` (Invariant `:102`); FM-REPLICATION-055 `:1277-1287` (Invariant `:1284`) | ✅ |
| `replication-runtime/src/{export,executor,install,quorum}.rs` | 143 / 446 / 829 / 796 | **read-only, RC12 evidence**: in-crate test modules at `export.rs:59`, `executor.rs:168`, `install.rs:365`, `quorum.rs:256` | ✅ |

## Problem

### One consumer, three storage sites, two wiring sites, three doc paragraphs

Every one of the three seams is read in exactly one production place, and all three places are
inside `handle_full` (`replica_session.rs:744-880`) or its callee:

| seam | field | setter | getter | sole production consumer |
|---|---|---|---|---|
| `FunctionSnapshotHook` | `:205` | `:325` | `:331-333` | `replica_session.rs:786-788` |
| `PreCheckpointHook` | `:193` | `:312` | `:318-320` | `replica_session.rs:807-821` |
| `LiveSnapshotSource` | `:200` | `:337` | `:343-345` | `replica_session.rs:1027-1032` |

("Production" is load-bearing for the first row: `function_snapshot_hook()` is also read three
times by its own wiring test, at `primary/tests.rs:1043`, `:1054`, `:1065`. The other two
getters have no test reader at all — which is [problem 1](#testability-improvement) below.)

Three fields, three `RwLock`s, three setters, three getters, three doc paragraphs — for three
values with **identical lifecycle** (installed once at boot, read once per full resync, never
mutated in production) feeding **one** code path. That is the definition of a shallow
**module**: its interface is nearly as complex as its implementation, and none of the three
declarations can say the thing that actually matters, which is that the three belong together.

The **locality** cost is concrete. The Primary's full-resync payload contract — "checkpoint
drained before it is cut, dataset available when there is no RocksDB, function registry shipped
inside the replayed window" — is one story told at `replica_session.rs:778-868`. Its wiring is
told at `replication_init.rs:165-192` **and** `server/mod.rs:305-342`, one boot phase apart, in
two different files, with the second one guarded by an `if let Some(ref handler)` over a field
whose own doc says it is always present (`replication_init.rs:29-30`: *"Primary-side replication
handler. Always present, on every role."*).

### The stated reason for post-construction installation is false at the only production site

`primary/mod.rs:190-192` justifies the `Option`:

> Installed after construction because the shards are wired up later; a handler without one
> cuts the checkpoint straight from whatever RocksDB currently holds.

and `:195-196` inherits it (*"Installed after construction for the same reason the
pre-checkpoint hook is"*). Verified against the tree:

- `shard_senders` is built in **phase 1** (`init.rs:215-230`) and published on `InfraResult`
  (`:465`); `init_replication` takes it as a parameter (`replication_init.rs:116`) and uses it
  seven lines *after* the construction call, at `:171` and `:191`.
- `function_registry` is likewise a phase-1 value (`init.rs:94`, built `:412`, published
  `:488`), so the third hook's only capture is available at construction too — its wiring sits
  in `server/mod.rs` for no reason the code states.
- What is genuinely "wired up later" is the shard **workers** (`mod.rs:376`, phase 4), not the
  senders the closures capture. The hooks are only ever *invoked* from an accepted `PSYNC`, and
  acceptors start in `start_subsystems` (`subsystems.rs:76`), called only from
  `server/runtime.rs:22`, after `with_listeners` returns.

So there are **ten** construction sites (one production — `replication_init.rs:147` — and nine
test fixtures: `primary/tests.rs:18/:615/:669/:742`, `replica_session.rs:1983/:2027/:3736`,
`replication_init.rs:483`, `cluster_init.rs:1492`), and the single production one has every
input it needs in hand. (`grep -rn 'PrimaryReplicationHandler::new'` returns twelve hits; the
other two, `tracker.rs:104` and `:458`, are doc-comment references, not call sites.)

### No live bug — the reachability argument, stated so it is not assumed

A torn wiring window exists *in principle*: between `set_live_snapshot_source`
(`replication_init.rs:190`, phase 2) and `set_function_snapshot_hook` (`server/mod.rs:327`,
phase 2.5) the handler is partially wired, and a `PSYNC` landing in that window would serve a
full resync with no function registry — the exact hole FM-REPLICATION-055 forbids.

**Unreachable today**: the only route to `handle_full` is a connection accepted by the RESP
acceptor, and acceptors are spawned by `start_subsystems` (`subsystems.rs:76`), whose only
caller is `server/runtime.rs:22` — i.e. after `Server::with_listeners` has returned, after every
wiring site. There is no second production construction site (`REPLICAOF NO ONE` and cluster
failover re-point the *live role flag*; the Primary-side handler is built once for every role —
`replication_init.rs:83-96`, and `role_manager.rs` constructs no `PrimaryReplicationHandler` at
all), so no promotion path can produce an unwired handler either.

Recorded, not fixed as a hotfix, because it is a bug held off by an argument about *boot phase
ordering held outside both files* — precisely the class of argument that a future re-ordering of
phases silently invalidates, and precisely what the compile error this proposal buys would make
structural.

### Why the current shape is a shallow module, and what the change actually buys

- **Three `Option` seams with no shared name is an interface that cannot state its own
  invariant.** The invariant is "all three, or this node cannot honestly serve a full resync".
  Today it is spelled out in three separate doc paragraphs that each describe only their own
  degradation, and nowhere as one statement.
- **The mitigation ADR-0004 chose for the Replica half is missing on the Primary half.** The
  ADR names `LiveSnapshotInstaller::for_config` as *"the single construction site for both
  wiring points … precisely so those two cannot drift"* (`:79-81`). That is a **factory** for an
  adapter: one named function in the server crate that turns owned state into the seam value.
  The Primary side has no such function — its three closures are built inline at two call sites.
- **`PrimarySnapshotHooks` passes the deletion test.** Delete it and you get back: three
  `RwLock<Option<…>>` fields, three setters, three getters, three doc paragraphs, two wiring
  sites in two files one phase apart, one `if let Some` guard over an always-`Some` field, and
  no expression anywhere in the tree that means "the Primary's full-resync seams". Complexity
  reappears across the callers, so the type earns its keep.
- **Be precise about what kind of win this is.** `PrimarySnapshotHooks` is a plain-data holder:
  three public fields, interface ≈ implementation, no depth of its own and none claimed. The
  payoff is **locality** — every fact about how this node wires its full-resync seams
  concentrates at one construction site and one factory, so it can be fixed and *verified* once
  — plus a compile-time obligation: a construction site must name all three. The module that
  gets deeper is `PrimaryReplicationHandler`, whose interface loses three setters and three
  `RwLock` slots and gains one named argument.
- **The file already learned this lesson.** `replication_init.rs:49-59` documents *why*
  `backlog_config` was extracted as a pure function: issue 14 had three of four fields read from
  the wrong config keys, and *"a wiring mistake of that shape is invisible to every test that
  does not read the wiring, so it lives here where a unit test can pin each field to its own
  key."* The hooks are the same shape of wiring in the same function, and got no such treatment.
- **A shallower version exists and should be rejected.** Merging the three fields into one
  `RwLock<PrimarySnapshotHooks>` while keeping three setters buys the grouping but not the
  enforcement — a new wiring site can still install two of three and get a runtime warning. The
  whole reason the ADR paragraph needs amending is the compile-time half. (The same objection
  kills `#[derive(Default)]` on the value; see [the type](#one-value-type-one-factory-one-door).)

### RC12 — the ADR-0004 cost paragraph is stale in a bigger way than filed

The lane candidate says *"executor.rs/install.rs now have tests; only export.rs matches
description"*. **That is wrong in both directions.** Verified at `9a62f79b`, counts re-checked
at `6e99f567`:

| runtime-crate module | in-crate test module | tests | FM tags |
|---|---|---|---|
| `export.rs` (143) | `:59` | 3 (all `#[tokio::test]`: `:68`, `:95`, `:120`) | FM-REPLICATION-001 |
| `executor.rs` (446) | `:168` | 7 (all `#[tokio::test]`: `:184`…`:407`) | FM-REPLICATION-051, -034 |
| `install.rs` (829) | `:365` | 9 (1 sync `:445` + 8 `#[tokio::test]`: `:480`…`:744`) | FM-REPLICATION-052, -053, -061 |
| `quorum.rs` (796) | `:256` | **17** (all plain `#[test]`, zero tokio: `:273`…`:775`) | FM-REPLICATION-041, **-062** |

`export.rs` does **not** match the description — it carries the three FM-REPLICATION-001
forcing tests that proposal 53 also cites (`53:52`). All four modules carry in-crate tests, and
every whole-function survivor the ADR names by hand is now directly forced: `apply_single` ←
`a_single_replicated_command_executes_directly_on_its_tagged_shard`, `apply_transaction` ←
`a_reconstructed_transaction_is_one_atomic_shard_message`, `apply_group` ←
`an_empty_group_reaches_no_shard`, `export_live_dataset` ←
`every_shard_contributes_its_blob_in_shard_order`, `install`/`read_snapshot` ←
`a_staged_checkpoint_is_read_shard_by_shard_and_installed_into_each` and
`a_checkpoint_this_node_cannot_read_is_refused_and_touches_no_shard`. `quorum.rs` — the one
module the ADR credits with tests — has grown from that credit to seventeen and picked up a
second row (-062), so the paragraph under-describes even the module it was right about.

The paragraph is stale for a bigger reason than missing tests: **the measurement it reports has
been superseded by a recorded, dated gate pass.** ADR-0004 `:64-76` reports a pre-hardening
baseline (`frogdb-replication` 74.7% on viable, `frogdb-replication-runtime` 50.0%). The LOCKED
spec header records the Phase-3 outcome (`replication-failure-modes.md:3-6`):

> Status: LOCKED (2026-08-04) — Phase 3 mutation gate passed (frogdb-replication 98.7% on 1180
> mutants, frogdb-replication-runtime 100% of viable, vs an 85% gate; the 15 surviving mutants
> are all documented equivalents at the code).

So the ADR is a reader's only source for "how much of this is verified" and it currently answers
with a number that the tree has already beaten by 24 points on `frogdb-replication` and 50 on
`frogdb-replication-runtime`. **No re-measurement is needed to correct it** — the replacement
numbers are in the tree, dated, and are the numbers the lock was granted on. That makes RC12 a
clean doc-only hotfix; see [Effort](#effort--hotfix-candidates).

## Proposed change

### One value type, one factory, one door

**A new `PrimarySnapshotHooks` in `primary/mod.rs`**, next to the three type aliases it groups
(`:44-89`), replacing the three fields at `:188-205`:

```rust
/// Everything a `+FULLRESYNC` needs from the owner of the shards and the
/// process-wide registries. The replication crate owns none of it (ADR-0004),
/// so all three arrive together or the Primary cannot honestly serve one.
///
/// There is deliberately no `Default`: a struct literal must name every field,
/// so a wiring site cannot supply two of three and let the third default to
/// `None`. A `None` is therefore a *stated* absence — the per-seam degradations
/// are documented on each alias — and never a forgotten wiring step.
#[derive(Clone)]
pub struct PrimarySnapshotHooks {
    pub pre_checkpoint_hook: Option<PreCheckpointHook>,
    pub live_snapshot_source: Option<LiveSnapshotSource>,
    pub function_snapshot_hook: Option<FunctionSnapshotHook>,
}

impl PrimarySnapshotHooks {
    /// A handler that ships nothing: no drain, no live dataset, no libraries.
    /// The shape every in-crate test fixture wants, said out loud.
    pub fn none() -> Self {
        Self {
            pre_checkpoint_hook: None,
            live_snapshot_source: None,
            function_snapshot_hook: None,
        }
    }
}
```

Two details are load-bearing rather than incidental:

- **The field names are the spec's names.** `pre_checkpoint_hook`, `live_snapshot_source` and
  `function_snapshot_hook` are today's field names *and* the identifiers the LOCKED spec spells
  out in prose — FM-REPLICATION-001's Invariant says *"an unwired `live_snapshot_source` errors
  it too"* (`:102`) and FM-REPLICATION-055's says *"a `function_snapshot_hook` on the primary
  broadcasts …"* (`:1284`). Keeping them verbatim is free and is what makes the
  [Spec position](#spec-position-locked-area) claim "still literally true" actually true.
- **No `#[derive(Default)]`.** It would defeat the entire mechanism: with `Default` in scope a
  future wiring site can write `PrimarySnapshotHooks { live_snapshot_source: Some(x),
  ..Default::default() }`, ship two of three, and compile clean — which is exactly the runtime
  warning the ADR amendment claims has been eliminated. `none()` is the explicit spelling of
  "all three absent, on purpose", and it is one named function rather than an implicit
  fill-in-the-rest. If test ergonomics later want `with_live_snapshot_source(…)`-style chaining,
  those helpers must be `#[cfg(test)]`, or they reintroduce the same hole under a nicer name.
  (`Clone` is kept only for callers that seed more than one handler from one built value; the
  getters clone individual fields, so it is not load-bearing and can be dropped if unused.)

The handler keeps **one** `RwLock<PrimarySnapshotHooks>` field (`snapshot_hooks`) in place of
three, seeded by `new`:

```rust
pub fn new(… , hooks: PrimarySnapshotHooks) -> Self   // 9 args → 10, under the existing allow
```

and exposes **one** post-construction door in place of three setters:

```rust
/// Re-point every full-resync seam at once. Production wires at construction;
/// this exists for tests and for a future wiring path that must re-point them.
pub fn install_snapshot_hooks(&self, hooks: PrimarySnapshotHooks)
```

**The three `pub(crate)` getters keep their exact signatures** (`pre_checkpoint_hook()`,
`live_snapshot_source()`, `function_snapshot_hook()` → `Option<…>`, cloned out so no guard is
held across an `await`; each now reads `self.snapshot_hooks.read().<field>.clone()`). The three
consumers in `replica_session.rs` (`:786`, `:807`, `:1027`) are therefore **not edited at all** —
which is what keeps this proposal off proposal 53's lines.

### The factory in the server crate

**One new function in `replication_init.rs`**, sitting beside `backlog_config` and modelled on
ADR-0004's own `LiveSnapshotInstaller::for_config`:

```rust
/// The Primary-side full-resync seams, all three, from the state that owns them.
/// Extracted for the same reason `backlog_config` was (issue 14): wiring that no
/// test reads is wiring that can be wrong for a release.
///
/// The function-snapshot closure captures the registry and *nothing else*: the
/// handler outlives the storage engine, and capturing anything that reaches the
/// config manager would keep RocksDB open past shutdown (see
/// `function_store::FunctionStore::snapshot_command_args`). The signature is
/// what enforces that — borrowed handles to exactly the two things the closures
/// may hold, so a capture that reaches further does not typecheck here.
fn primary_snapshot_hooks(
    shard_senders: &Arc<Vec<ShardSender>>,
    function_registry: &SharedFunctionRegistry,
) -> PrimarySnapshotHooks
```

That doc block is **not new prose**: the shutdown hazard is recorded today at
`server/mod.rs:320-324`, and step 2 deletes the region it lives in. It must **move**, not be
lost — it is the only record in the tree of why the closure's capture list is what it is. The
move is also a small upgrade, and it is the one point in this proposal's favour that the draft
had missed: today the discipline is a comment sitting above an inline closure that could capture
anything in `with_listeners`' scope; after the change the factory's parameter list is the only
thing in scope, so the constraint is enforced structurally and the comment merely explains why.

The three closure bodies move unchanged: the `quiesce_shards_for_checkpoint` drain
(`replication_init.rs:170-184`), `crate::replication::live_snapshot_source(…)` (`:190-192`), and
the `propagation_order()` + `snapshot_command_args` broadcast (`server/mod.rs:327-341`). The
call site becomes one argument to the existing `PrimaryReplicationHandler::new` at `:147-164`.

`init_replication` gains one parameter (`function_registry: &SharedFunctionRegistry`) on its
signature at `:106-129`, passed from `infra.function_registry` at the call site
(`server/mod.rs:264-275`) — already in scope there. `server/mod.rs:305-342` is **deleted**: the
duplicated comment block, the `if let Some(ref handler)` guard over an always-`Some` field, and
the wiring itself.

### What changes for the nine test fixtures

All nine construction sites gain one argument, `PrimarySnapshotHooks::none()` —
`primary/tests.rs:18/:615/:669/:742`, `replica_session.rs:1983/:2027/:3736`,
`replication_init.rs:483`, `cluster_init.rs:1492`.

The three places that install a hook *after* construction switch from a per-seam setter to
`install_snapshot_hooks(PrimarySnapshotHooks { … })` with all three fields named:

- `with_live_dataset` (`replica_session.rs:1910-1916`) — installs a live-snapshot source;
- the two pre-checkpoint tests (`replica_session.rs:2842`, `:2927`);
- `the_function_snapshot_hook_is_installed_and_handed_back_callable` (`primary/tests.rs:1035`),
  which installs twice on purpose (to prove re-installation supersedes rather than stacks).

**Whole-value installation cannot clobber a previously installed hook here**, and that was
checked rather than assumed: `with_live_dataset`'s four callers (`:2139`, `:2426`, `:2566`,
`:2654`) are disjoint from the two `set_pre_checkpoint_hook` call sites (`:2842`, `:2927`), so no
test installs two seams in sequence. Mechanical; no assertion changes.

### ADR-0004 amendment — two Consequences edits, no decision reopened

ADR-0004's **decision** is *"the replication runtime is four seam implementations, not a host
trait"* (`:1`, argued `:26-46`). 61 changes how one of those implementations is **installed**,
not what it is or where it lives, and the sentences it makes false are both in the trailing
**Consequences** paragraph. Per the round's ADR discipline the friction is named rather than
assumed: the ADR itself records the cost, chose a mitigation for one half of it, and the other
half is what this proposal supplies.

**Seam bookkeeping, so the amendment does not appear to invent seams.** ADR-0004 counts exactly
four (`:8-24`): `ReplicaCommandExecutor`, `live_snapshot_source`, `LiveSnapshotInstaller`,
`ReplicationQuorumChecker`. Of the three hooks 61 groups, only `LiveSnapshotSource` is one of
those four. `PreCheckpointHook` and `FunctionSnapshotHook` are **primary-side host hooks whose
implementations live in the server crate** (the shard drain and the function-registry broadcast)
and are outside the ADR's count — the amendment names them because they share the installation
problem, not because the seam count changes from four to six.

**Amendment 1 (RC9)** — `adr/0004-replication-runtime-seams.md:76-83`, currently:

> The other cost is that two of the four seams are type-erased closures
> (`LiveSnapshotSource`, `SnapshotInstaller`) held as `Option` and set post-construction, so a
> wiring site that forgets one gets a runtime warning, not a compile error —
> `LiveSnapshotInstaller::for_config` exists as the single construction site for both wiring
> points (boot-configured replica and runtime `REPLICAOF` demotion) precisely so those two
> cannot drift, and the unwired paths degrade loudly: …

After 61, the `LiveSnapshotSource` half of that sentence is false: it is named at construction,
so a wiring site that forgets it does not compile. **And after 54 the `SnapshotInstaller` half is
false too** — see the [boundary vs 54](#boundary-vs-54-replicawiring--the-mirror-image-same-idiom-disjoint-lines),
which is a correction to this proposal's earlier draft. Since 61 lands **after** 54 (chain
ruling below) and 54 amends no ADR, 61 rewrites the sentence once, for both halves. Proposed
replacement for everything up to *"and the unwired paths degrade loudly"* (that clause onward is
**unchanged and still true** — `None` still fails the sync rather than sending an empty
envelope):

> The other cost is that the seams are type-erased closures held as `Option`, and the wiring
> that installs them used to be a sequence of setters, so a wiring site that forgot one got a
> runtime warning rather than a compile error. Both sides now close that structurally by naming
> every seam at construction: on the replica, `ReplicaWiring` carries `SnapshotInstaller`
> (together with the other three policy values) into `ReplicaReplicationHandler::new`, built for
> both wiring points — boot-configured replica and runtime `REPLICAOF` demotion — by
> `LiveSnapshotInstaller::for_config`, precisely so those two cannot drift; on the primary,
> `PreCheckpointHook`, `LiveSnapshotSource` and `FunctionSnapshotHook` are one
> `PrimarySnapshotHooks` value named at `PrimaryReplicationHandler::new`, built by the single
> `primary_snapshot_hooks` factory in the server crate. The value type derives no `Default`, so
> a construction site cannot install two of three. (The first and third of those three are
> server-crate host hooks, not among the four seams above.) The unwired paths still degrade
> loudly: …

**Contingency, stated so the PR does not have to re-derive it.** If 54's deletion of
`set_snapshot_installer` is dropped in review, or 54 lands after 61 after all, revert the replica
clause to the original wording — *"only one of them, `SnapshotInstaller` on the replica handler,
is still set post-construction"* — and leave the primary clause as drafted. The primary half of
this amendment is independent of 54 either way.

**Amendment 2 (RC12)** — `adr/0004-replication-runtime-seams.md:64-76`, in two parts, because
the stale measurement spans two sentences and only one of them was drafted before.

*Part A* — `:64-70`, currently:

> The measured baseline is honest about how much of that is verified: `frogdb-replication`
> scored 533 caught / 181 missed / 1 timeout / 84 unviable — 74.7% on viable mutants — and that
> number is a floor rather than a measurement, because `cargo mutants -p frogdb-replication`
> builds and runs only that package's tests. The forcing tests for most of these behaviors live
> in `frogdb-server/crates/server/tests/integration_replication.rs`, which never runs against a
> mutant, so a survivor there may well be killed by a test that was not given the chance to run;
> raising the real score means moving forcing tests down into the crates, not tuning the gate.

Proposed replacement — tense-corrected and date-stamped, with the final clause preserved
**verbatim** because it is quoted elsewhere (`56:390` cites `:64-70` for exactly that sentence):

> The baseline measured when this ADR was written was honest about how much of that was
> verified: `frogdb-replication` scored 533 caught / 181 missed / 1 timeout / 84 unviable —
> 74.7% on viable mutants — and that number was a floor rather than a measurement, because
> `cargo mutants -p frogdb-replication` builds and runs only that package's tests. The forcing
> tests for most of these behaviors lived in
> `frogdb-server/crates/server/tests/integration_replication.rs`, which never runs against a
> mutant, so a survivor there may well be killed by a test that was not given the chance to run;
> raising the real score means moving forcing tests down into the crates, not tuning the gate.
> That is what the Phase-3 hardening pass did, and the numbers the lock was granted on are
> recorded in the spec header (`.scratch/hardening/specs/replication-failure-modes.md:3-6`):
> `frogdb-replication` 98.7% of 1180 mutants, against an 85% gate, with fifteen documented
> equivalents at the code.

*Part B* — `:71-76`, currently:

> The runtime crate itself is the sharper end of the same problem: it scored 28 caught / 28
> missed / 6 unviable — 50.0% — because only `quorum.rs` carries tests today, and the
> executor/export/install seams are exercised exclusively from the server's integration suite.
> Every whole-function survivor there (`apply_single`, `apply_transaction`, `apply_group`,
> `export_live_dataset`, `install`, `read_snapshot`) is a seam whose only caller is a live
> two-node test.

Proposed replacement, sourced from `replication-failure-modes.md:3-6` and the four test modules:

> The runtime crate was the sharper end of the same problem: at the time this ADR was written it
> scored 28 caught / 28 missed / 6 unviable — 50.0% — because only `quorum.rs` carried tests and
> the executor/export/install seams were exercised exclusively from the server's integration
> suite. Rows FM-REPLICATION-051..053 were written against those three seams for exactly that
> reason, and the Phase-3 pass that closed them moved the forcing tests down into the crate:
> `export.rs` (3), `executor.rs` (7), `install.rs` (9) and `quorum.rs` (17, now also carrying
> FM-REPLICATION-062) all have in-crate test modules today, and every whole-function survivor
> named above is directly forced. The crate scored 100% of viable mutants at the lock.

Together, Parts A and B replace the whole stale measurement with the dated one and leave the
argument the paragraph makes — that the fix is moving tests down, not tuning the gate — standing
and, now, satisfied.

## Testability improvement

**The interface is the test surface.** Today the Primary's full-resync wiring has no interface,
so it has no test:

1. **Two of the three seams have no crate-local install/read test at all.** The single one that
   does — `the_function_snapshot_hook_is_installed_and_handed_back_callable`
   (`primary/tests.rs:1035-1068`) — proves *"a setter that dropped its argument … would silently
   ship a full-syncing replica a keyspace with no libraries behind it"* (its own doc,
   `:1030-1033`). That risk is identical for the other two, and neither has an equivalent — it
   is why `function_snapshot_hook()` is the only getter with a test reader in the census above.
   With one value type, one table-driven test covers all three install/read pairs, and the
   three-way duplication that would otherwise be written never gets written.
2. **The production wiring becomes readable by a test, exactly like `backlog_config`.**
   `primary_snapshot_hooks(&senders, &registry)` returns a value: a unit test in
   `replication_init.rs`'s existing `#[cfg(test)]` module (`:410-411`, where the `backlog_config`
   field-by-field tests at `:429-478` already live) asserts all three are `Some`. Today the
   equivalent assertion does not exist and could not be written without booting a server, because
   the wiring is three statements in two files with no return value. This is the **new** pin the
   change enables — it is not a claim that today's wiring is wrong.
3. **`PrimarySnapshotHooks::none()` makes the deliberate absences legible.** Nine fixtures
   currently express "no hooks" by *omission*, which is indistinguishable from forgetting. After
   the change the fixtures say `none()` and the two negative-path forcing tests —
   `full_sync_without_a_live_snapshot_source_fails_the_sync` (`replica_session.rs:2707`,
   FM-REPLICATION-001) and the pre-checkpoint failure tests (`:2812`, `:2908`) — read as
   deliberate rather than incidental.
4. **The mutation gate gets a smaller, denser target.** Three setters and three getters are six
   trivially-mutable functions whose only in-crate forcing test covers one of them; one
   constructor field-move plus one `install_snapshot_hooks` is fewer targets, each directly
   asserted by 1-3. Expect the crate's score to hold or improve; the change does not add
   unreachable surface.

## Risks / scope boundaries vs sibling proposals

### Boundary vs 53 (`FullSyncEmitter`) — the sharp one, and it is clean

53 and 61 both live in the full-resync path and must not claim the same lines. The line is drawn
by **which side of the getter** the change is on:

| unit | owner |
|---|---|
| `stream_checkpoint` (`replica_session.rs:888-988`), `stream_live_dataset` (`:1018-1104`) | **53** |
| `handle_full`'s payload-preparation branch (`:793-868`) incl. moving `stream_live_dataset`'s preflight (`:1027-1032`) up into it | **53** |
| new `fullsync/emitter.rs` | **53** |
| the three seam **type aliases** (`primary/mod.rs:44-89`) | **neither — read by both, unchanged by both** |
| the three seam **fields, setters, getters** (`primary/mod.rs:188-205`, `:289-291`, `:309-345`) | **61** |
| `PrimaryReplicationHandler::new` (`:242-302`) and its ten call sites | **61** |
| the server-crate wiring (`replication_init.rs:106-129`, `:147-192`; `server/mod.rs:264-275`, `:305-342`) | **61** |

**Zero production-line overlap.** 53's own files table lists `primary/mod.rs:67-71`, `:343`,
`:55-56` as read-only context (`53:51`, re-cited against the on-disk revision of 53 — the row
moved from `53:43` when 53 was revised at `55d73174`), and its rewrite of `handle_full` calls
`handler.live_snapshot_source()` — a getter whose signature 61 deliberately does not change.
Two shared-file edges, both small:

1. **`replica_session.rs` test fixtures.** 61 adds one argument to the ctor calls at `:1983`,
   `:2027`, `:3736`; 53 rewrites emitter tests in the `:2100-3400` region and adds a golden.
   Different hunks, same 4574-line file. **Land 53 first** — it is the M-sized, higher-risk
   change; 61 then rebases three one-line fixture edits, which is the cheapest possible direction
   for the conflict.
2. **Shared 0.85 re-gate.** Both are `frogdb-replication` changes. Run the full gate once at the
   end of the round-38 replication chain, not four times. See [Chain](#chain-order--ruled).

If 53 lands *after* 61 instead, nothing breaks: 53's prepare-then-emit rewrite reads the same
getters and constructs no handler.

### Boundary vs 54 (`ReplicaWiring`) — the mirror image, same idiom, disjoint lines

54 is the Replica-side twin of this proposal (RC2 + RC10) and shares one file:
`replication_init.rs`. The regions are disjoint — 54 owns `:223-303` (inside the
`if config.replication.is_replica()` branch) plus `role_manager.rs:655-739`; 61 owns `:106-129`
and `:147-192` (the signature and the pre-role-branch body) plus its own test ctor at `:483`,
plus `server/mod.rs`. 54's fifteen call sites are all
`ReplicaReplicationHandler::new`; 61's ten are all `PrimaryReplicationHandler::new`. No line is
claimed twice. Since 54 lands first, 61 rebases its `replication_init.rs` hunks onto shifted line
numbers — mechanical, and the reason 61 is last rather than 54.

**Correction to this proposal's earlier draft.** It claimed 54 *"keeps `SnapshotInstaller` as a
setter by design"*, cited to `54:309-311`. That is **false against the on-disk revision of 54**:
`ReplicaWiring` carries `pub snapshot_installer: Option<SnapshotInstaller>` (`54:311`), and
*"The four policy setters are deleted, not kept alongside … `set_ack_interval`,
`set_snapshot_installer`, `set_net_bytes_counters` and `set_shared_offset` go away; their bodies
move into `new`"* (`54:321-323`, sized at `54:556`). So 54 makes the *replica* half of ADR-0004
`:76-83` false, exactly as 61 makes the primary half false — and **54 amends no ADR**
(`grep -ci adr 54-replica-connection-wiring.md` → 0). That is not a conflict; it is the strongest
reason for the chain order below, and Amendment 1 above is drafted for the post-54 world.

One coordination item, non-blocking:

- **Naming.** `ReplicaWiring` and `PrimarySnapshotHooks` should be reviewed together so the repo
  ends up with one idiom for "a value that names every seam a handler needs". Landing 61 after 54
  means 54 is on disk when 61's name is reviewed, which is when that comparison is cheapest. If a
  reviewer prefers one name shape, apply it to both. The `Default` question travels with the
  naming: `ReplicaWiring` deliberately *has* a `Default` (`54:319-320` — nine of fifteen call
  sites take `ReplicaWiring::default()`), `PrimarySnapshotHooks` deliberately does not, because
  its ten call sites are one production site and nine fixtures. The divergence is defensible but
  should be a stated decision, not an accident.

### Chain order — RULED

> **`tag-hotfix` → 53 → 55 → 54 → 61**, with 61 **last**.

This extends 55's ruled chain (`tag-hotfix → 53 → 55 → 54`, `55:432`), which 53 states in the
same order (`53:614-619`). Two reasons 61 goes at the end rather than adjacent to 53:

1. **61 is the only proposal in the lane that amends ADR-0004 `:76-83`.** Verified by grep across
   the lane: 53 mentions an ADR once and only to name the gate (`53:36`); 54 and 55 mention none
   at all; 56 cites `:64-70` and `:198`/`:533-535` but proposes no edit; 57 states it has no ADR
   to amend (`57:494-495`). Because 54 nevertheless makes the replica half of that paragraph
   false, landing 61 last means the paragraph is rewritten **exactly once**, describing the tree
   as it will actually be, instead of twice with the first version obsolete on arrival.
2. **The naming/`Default` review happens with 54 on disk** — see above.

**56 slots into the same chain after 54**, as its own ruling says (`56:505`: *"53 → 55 → 54 →
56"*), and is disjoint from 61 (56 owns `replica/connection.rs:224-341`; 61 touches no replica
connection file). 61 and 56 may land in either order. Two consequences to state in the PR:

- **The full 0.85 gate runs once, after the last `frogdb-replication` change in the round lands** —
  if 56 lands after 61, the gate follows 56, not 61. `just mutants-diff frogdb-replication` still
  runs per landing (push discipline).
- **56's citation of `adr/0004-replication-runtime-seams.md:64-70` (`56:390`) shifts** when
  Amendment 2 Part A lands, because the replacement is longer than the original. The quoted
  sentence is preserved verbatim, so only the line range needs re-citing; whichever of 56 and 61
  lands second fixes it.

### Boundary vs 55, 57, 58 and the rest of the lane

- **55 (adopt-full-sync-landing)** — Replica landing tail, `replica/connection.rs`, plus its
  tag-only spec hotfix at the head of the chain. No overlap with 61.
- **57 (raft-network-send)** — cluster; explicitly notes it has no ADR to amend
  (`57:494-495` — re-cited; the row moved from `57:444-446` when 57 was revised). No overlap.
- **58 / 59 / 60 / 62 (cluster lane)** — `frogdb-cluster` + `frogdb-cluster-runtime`. 61 touches
  neither crate, so the **0.80 cluster gate and `cluster-failure-modes.md` are not in scope**.

### Risk — one `RwLock` where there were three

Read-mostly, taken once per full resync, `parking_lot`. No contention consideration applies. The
one rule that must survive: the getters clone the `Arc` out and drop the guard, because
`handle_full` awaits with the hook in hand (`replica_session.rs:808`). Preserving the getter
bodies (now one field-read deeper) preserves this; a "return a guard" refactor would deadlock and
is out of scope.

### Risk — `new` grows to ten arguments

`#[allow(clippy::too_many_arguments)]` is already on `new` (`primary/mod.rs:244`) at nine. The
honest alternative — folding the whole argument list into a `PrimaryHandlerConfig` bag — is a
different proposal with a different justification and is deliberately **not** attempted here.

### Risk — the change's whole claim is "nothing changed"

There is no behavioral acceptance test of its own; the change borrows the existing forcing
tests. The load-bearing ones, all in-crate so `cargo mutants -p frogdb-replication` sees them:
`full_sync_without_a_live_snapshot_source_fails_the_sync` (`:2707`, FM-REPLICATION-001),
`run_full_sync_without_rocks_streams_the_live_dataset` (`:2562`, -001),
`fullresync_cuts_the_checkpoint_after_the_pre_checkpoint_hook` (`:2812`),
`fullresync_fails_when_the_pre_checkpoint_drain_fails` (`:2908`), and
`the_function_snapshot_hook_is_installed_and_handed_back_callable` (`primary/tests.rs:1035`).
FM-REPLICATION-055's forcing test
`a_replica_that_full_syncs_receives_the_primarys_existing_libraries`
(`server/tests/integration_replication_functions.rs:108`) is the one that proves the moved
wiring still ships libraries; it lives in the server crate and must pass unedited.

### Spec position (LOCKED area)

**Not spec-first: no behavior changes.** Stated precisely, row by row:

| row | what it says about these seams | after 61 |
|---|---|---|
| FM-REPLICATION-001 (`:95-105`, Invariant `:102`) | *"an unwired `live_snapshot_source` errors it too"* | **Still literally true** — `live_snapshot_source` is the field's name inside `PrimarySnapshotHooks` (kept verbatim for this reason) and the getter's name, and `None` still produces the same `"no live-snapshot source"` error (asserted by substring at `:2726-2730`). No prose edit. |
| FM-REPLICATION-055 (`:1277-1287`, Invariant `:1284`) | *"a `function_snapshot_hook` on the primary broadcasts one whole-registry `FUNCTION RESTORE <dump> FLUSH` frame, invoked *after* `handle_full` captures `snapshot_offset`"* | **Unchanged** — `function_snapshot_hook` is likewise the field and getter name, and 61 moves *where the hook is installed*, never when it is invoked (`replica_session.rs:786-788` is not edited). No prose edit. |

- **No `Forced by` cell changes and no FM-tagged test is renamed.** `scripts/failure-modes.py`
  binds only backticked `Forced by` names to `// FM-` tags, so `just lint-failure-modes` is
  green either way — noted explicitly because that means the lint could not have caught a stale
  Invariant sentence, so the two rows above were checked by hand rather than by the gate. This
  mirrors proposal 53's criterion 6 and the D2 ruling's *rows may move file:line citations but
  not meaning*.
- **Re-gate.** `just mutants-diff frogdb-replication` before pushing (push discipline). Full
  `just mutants frogdb-replication` + `just mutants-gate frogdb-replication 0.85` **once at the
  end of the round-38 replication chain**, shared with 53/54/55 (and 56 if it lands after 61),
  because all of them move mutation targets inside the same crate and repeated full runs would
  measure the same crate repeatedly.
- **`frogdb-replication-runtime` is untouched** by RC9 (no file in that crate changes), so its
  half of the 0.85 pair needs no run. RC12 only *reports* its already-recorded score.

## Effort + hotfix candidates

**S/M** for RC9, in two commits; **S** for RC12, doc-only and independently landable ahead of
everything.

| step | scope | size |
|---|---|---|
| **H1 — hotfix, independently landable** | Delete `server/src/server/mod.rs:305-312` — the duplicated comment block at `:305-311` (byte-identical to `:313-319`, md5-verified) **plus the blank line at `:312`**, so the surviving block is not preceded by two blank lines. Pure deletion, no behavior, no spec, no gate. Found during verification; not in the lane doc. | **XS** |
| **H2 — hotfix, independently landable** | RC12: amend `adr/0004-replication-runtime-seams.md:64-76` — **both** Parts A (`:64-70`) and B (`:71-76`) per [Amendment 2](#adr-0004-amendment--two-consequences-edits-no-decision-reopened). Doc-only, no re-measurement needed — the replacement numbers are the recorded Phase-3 gate result in `replication-failure-modes.md:3-6` and the four in-crate test modules are in the tree. Worth landing first regardless of RC9's fate: the ADR currently under-reports the area's verification by 24 and 50 points, which is the kind of stale number that gets an area re-litigated for no reason. Re-cite `56:390` if 56 is already on disk. | **S** |
| **1 — the type and the door** | `PrimarySnapshotHooks` (fields named `pre_checkpoint_hook` / `live_snapshot_source` / `function_snapshot_hook`, **no `Default` derive**) + `none()` in `primary/mod.rs`; three fields → one `RwLock<PrimarySnapshotHooks>`; three setters → `install_snapshot_hooks`; `new` takes the value; getters unchanged in signature. Nine test fixtures gain one argument; three post-construction install sites switch to whole-value installation. The false *"because the shards are wired up later"* justification (`:190-192`) deleted rather than corrected — it is not true and the new shape does not need it. | **S** — ~75 changed, net ~-20 |
| **2 — the factory and the single wiring site** | `primary_snapshot_hooks(&shard_senders, &function_registry)` in `replication_init.rs` beside `backlog_config`, carrying the shutdown-capture warning moved from `server/mod.rs:320-324`; the three closure bodies moved verbatim; `init_replication` gains one parameter (`:106-129`) and its call site one argument (`server/mod.rs:264-275`); `server/mod.rs:305-342` deleted (comment duplication, the always-`Some` guard, the wiring). One unit test asserting all three seams are `Some`. | **S** — ~60 moved, ~40 deleted |
| **3 — ADR-0004 Amendment 1** | The `:76-83` Consequences sentence, in the same commit as step 2 so the ADR never describes a shape the tree does not have. Drafted for the post-54 tree; contingency wording above if 54's setter deletion is dropped. | **XS** |
| **Re-gate** | `mutants-diff` per commit; full `mutants` + `mutants-gate frogdb-replication 0.85` once after the last `frogdb-replication` change in the round. Testbox-class. | — |

**Recommended order in the lane:** H2 and H1 immediately and independently (neither touches a
line any lane proposal moves); then RC9 **last** in the ruled chain `tag-hotfix → 53 → 55 → 54 →
61`, for the two reasons in [Chain order](#chain-order--ruled). RC9 is not a prerequisite for any
sibling and nothing in the lane is a hard prerequisite for it — the sequencing is about amending
the ADR paragraph exactly once and keeping the `replica_session.rs` and `replication_init.rs`
rebases on the cheap side.
