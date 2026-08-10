# Proposal 67 — Server small dedups: dead connection builder, server-wide unreachable-execute, shard-0 query helper

Lane candidates: **SV5 + SV6 + SV7**.

**Verified at HEAD `6e99f567`.** The lane briefs were written against `08c143d6`; every
citation below was re-derived from the current tree, and three lane claims are corrected
(SV5's line count, SV6's macro home, SV7's "LIVE" classification).

## Summary

Three small, independent cleanups in the server crate, bound by one through-line: **the
deletion test** — imagine the module gone; if complexity vanishes it was a pass-through, if
complexity reappears across N callers it was earning its keep. Each part is independently
landable, in any order; they share no file and no type.

- **SV5 (dead `ConnectionHandlerBuilder`) — a literal deletion.** `connection/builder.rs`
  (268 lines) has **zero callers**: the only references to any of its exported names are its
  own definitions, one re-export (`connection.rs:71`), and one doc mention
  (`connection.rs:6`). It survives `dead_code` only because every item is `pub` inside a
  `pub mod connection` (lib.rs:13), i.e. it is nominally crate-public API. The cost is not
  the 268 lines: **all five of the last five commits that touched the file were pure
  field-plumbing** into a function nobody calls. Worse, it is a *second, already-wrong*
  assembly of the connection dependency graph — `with_cluster_parts` cannot set
  `pubsub_forwarder`, and `with_admin_parts` mints a per-connection cursor store where
  production shares one node-wide. Delete the file, the re-export, the doc sentence, and the
  now-caller-less `ClusterDeps::cluster`. **Latent trap, no behavior change.**
- **SV6 (server-wide unreachable `execute()`, 28×).** Twenty-eight `Command::execute`
  bodies return the identical `CommandError::Internal { message: "internal: server-wide
  command reached shard executor" }`; twenty-six of them are character-identical *including*
  a five-line explanatory comment. Give the refusal one home: a function in `frogdb-core`
  that owns the message, plus a macro that owns the method boilerplate. Verified: the
  `stub_command!` precedent is real but crate-local and small (three macros in
  `commands/stub.rs`, six invocations between them) — the irony is that the 1-invocation
  duplication got a macro and the 28-fold one did not. **Latent, no behavior change.**
- **SV7 (shard-0 query, copied four times).** `ConnectionHandler::query_shard0`
  (`connection/search/helpers.rs:48`) is bypassed by four hand-rolled copies. The transport,
  the timeout, the three failure replies and the shard-error passthrough are **identical in
  all five**; the sole divergence is the empty-result fallback, where two copies return
  `-ERR empty response` and the helper (plus the other two copies) returns `*0`.
  **Correction to the lane brief: this is latent, not live** — the empty arm is unreachable
  in the current tree (proof below). But it is a trap for the naive fix: "just call the
  helper" would silently flip FT.INFO and FT.EXPLAIN's defensive reply. Parameterize the
  fallback instead. ~90 lines out, ~10 in.

Nothing here is in a **locked** area. `frogdb-server`, `frogdb-commands` and `frogdb-core`
are outside the four locked pairs (txn/vll, persistence/recovery,
replication/replication-runtime, cluster/cluster-runtime; ADRs 0002–0004). **Zero FM-tagged
tests live in any of the ten files touched** — verified by grepping `FM-` across exactly
that file set (no matches). No spec edit is owed and no mutation re-gate is owed.

## Files involved

| Path | Lines | Part | Role |
|---|---|---|---|
| `frogdb-server/crates/server/src/connection/builder.rs` | 268 | SV5 | **Deleted in full.** `ConnectionHandlerBuilder` (42), `with_core_parts` (63), `with_admin_parts` (85), `with_cluster_parts` (111), `build` (182), `try_build` (210), `connection_builder` (237), `standalone_config` (252) |
| `frogdb-server/crates/server/src/connection.rs` | — | SV5 | Two edits: the `mod builder;`/`pub use builder::{…}` re-export (71) and the doc sentence naming the builder (6). `from_deps` (263) — the surviving seam — is unchanged |
| `frogdb-server/crates/server/src/connection/deps.rs` | — | SV5 | `ClusterDeps::cluster` (137–161): sole caller is builder.rs:124, so it falls with the builder. `ConnectionConfig` (175) and `AdminDeps` (59) are read-only evidence |
| `frogdb-server/crates/server/src/acceptor.rs` | — | SV5 | **Read-only.** The production construction path (`ConnectionConfig` literal 173, `from_deps` 335) and its comment at 168–172, which is the evidence the builder is redundant |
| `frogdb-server/crates/server/src/server/subsystems.rs` | — | SV5 | **Read-only.** The production `AdminDeps`/`ClusterDeps` assembly (527, 536) the builder's parts-constructors diverge from. Owned by sibling **64** — 67 must not edit it |
| `frogdb-server/crates/server/src/commands/search.rs` | 1335 | SV6 | **22** identical bodies: 57, 100, 144, 187, 230, 273, 317, 362, 405, 448, 494, 536, 578, 620, 662, 704, 746, 788, 830, 1246, 1289, 1332. Module doc (3–7) states the tripwire contract |
| `frogdb-server/crates/commands/src/timeseries.rs` | — | SV6 | **4** identical bodies: 1059, 1102, 1147, 1190. **Feature-gated** (`#[cfg(feature = "timeseries")]`, `commands/src/lib.rs:54`) — not in `core-profile` |
| `frogdb-server/crates/server/src/commands/migrate_cmd.rs` | 376 | SV6 | 1 site (52). Same error, **site-specific comment**; spec is `KeySpec::Dynamic` + `AccessSpec::UniformRW` + a `dynamic_keys` method |
| `frogdb-server/crates/server/src/commands/server.rs` | 271 | SV6 | 1 site (231, SHUTDOWN). Same error, site-specific comment. The other three `ServerWide` commands here (DBSIZE 40, FLUSHDB 72, FLUSHALL 120) have **real bodies** and are **not touched** |
| `frogdb-server/crates/core/src/command.rs` (or a new `core::command` item) | — | SV6 | New home for the refusal function + the `#[macro_export]` macro |
| `frogdb-server/crates/server/src/commands/stub.rs` | 306 | SV6 | **Read-only.** The precedent: `stub_command!` (23), `not_supported_command!` (134), `db_not_supported_command!` (205) |
| `frogdb-server/crates/server/src/connection/search/helpers.rs` | 76 | SV7 | `query_shard0` (48–75) — the surviving interface, gains one parameter |
| `frogdb-server/crates/server/src/connection/search/index_mgmt.rs` | 139 | SV7 | `handle_ft_info` (74–108, copy at 82–107) and `handle_ft_list` (111–138, copy at 112–137) |
| `frogdb-server/crates/server/src/connection/search/explain.rs` | 63 | SV7 | `handle_ft_explain` (12–62, copy at 21–61) — the only copy with post-processing (cli_mode, 42–53) |
| `frogdb-server/crates/server/src/connection/search/synonyms.rs` | 88 | SV7 | `handle_ft_syndump` (55–87, copy at 61–86) |
| `frogdb-server/crates/core/src/shard/execution.rs` | — | SV7 | **Read-only.** `execute_scatter_part_body` (739) and its `PartialResult::keyed(results)` tail (924) — the proof the empty arm is unreachable |
| `frogdb-server/crates/core/src/shard/dispatch_core.rs` | — | SV7 | **Read-only.** `scatter_error_reply`'s keyless arm (220) — the other half of that proof |

## Problem

### SV5 — a second interface to a graph that already has one, with zero adapters

`ConnectionHandler` is constructed through exactly one seam,
`ConnectionHandler::from_deps` (`connection.rs:263`), which takes five positional
per-connection facts plus four grouped dependency structs (`CoreDeps`, `AdminDeps`,
`ClusterDeps`, `ObservabilityDeps`). Production reaches it at `acceptor.rs:335`; the
script sub-handler reaches it at `connection/scripting/script.rs:262`. Those are the two
call sites in the tree.

`connection/builder.rs` is a parallel interface onto the same construction. Its exported
names and every reference to them, verified by
`grep -rn '<name>' --include='*.rs' --include='*.md'` over the whole tree (tests, benches,
`frogctl`, `frogdb-operator`, `ops/` included):

| Exported name | References outside builder.rs |
|---|---|
| `ConnectionHandlerBuilder` | `connection.rs:71` (re-export), `connection.rs:6` (doc prose) — and two `.scratch` notes that already flagged it |
| `connection_builder` | `connection.rs:71` only |
| `standalone_config` | `connection.rs:71` only |
| `with_core_parts` / `with_admin_parts` / `with_cluster_parts` | builder.rs internal only |
| `try_build` / `enable_admin_separation` / `as_admin` | **none at all** — not even re-exported |

There is no macro-generated caller (the crate's only `macro_rules!` are the three in
`commands/stub.rs`, which generate `Command` impls) and no test caller.

**Apply the deletion test literally.** Delete the module: does complexity reappear at
callers? There are no callers, so trivially no. The interesting question is whether the
builder was *hiding* anything from `from_deps` — and the codebase answers it, in a comment
at the live construction site (`acceptor.rs:168–172`):

> core/admin/cluster/observability move in wholesale — they are already grouped exactly as
> `from_deps` wants them. Only `ConnectionConfig` is *assembled* here…

The grouping into four dependency structs **is** the readability mechanism the builder's
own doc comment claims to provide ("clear separation of required and optional parameters",
builder.rs:3–4). That job is already done, one layer down, by the types. The builder adds a
fluent façade over a seam that needs none. In this vocabulary: it is a hypothetical seam.
*One adapter means a hypothetical seam; two means a real one* — here there are zero.

**Why it never showed up as dead code.** Every item is `pub`, and `connection` is a `pub
mod` (lib.rs:13), so the whole file is nominally public crate API and rustc's `dead_code`
pass has nothing to report. The signal that would have told us the truth is structurally
suppressed — the same shape proposal 41 condemns in `#[allow(dead_code)]`, reached by a
different route.

**The cost is the maintenance tax, and it is measurable.** `git log` for the file returns
five commits, and *every one of them* is a field being plumbed into dead code:

| Commit | Change to builder.rs |
|---|---|
| `10bb0150` bound pubsub output buffer | `+ pubsub_output_buffer_hard_limit: …DEFAULT_PUBSUB_OUTPUT_BUFFER_HARD_LIMIT,` |
| `f1bfaf0c` delete dead hotshards config | `- hotshards_config: HotShardConfig::default(),` (+ import) |
| `57ae8cef` failed shard drains fail checkpoint | `+ recovery_stats: Default::default(),` |
| `36064128` extract slot migration coordinator | `+ slot_migration: Arc<…SlotMigrationCoordinator>,` (+ arg) |
| `b02e00bc` DEBUG SLEEP gate | `+ enable_debug_command: false,` |

`standalone_config` (builder.rs:253) is an **exhaustive** `ConnectionConfig` struct literal,
so it is a compile error away from every new field. It is the third such literal in the
tree, beside the production one (`acceptor.rs:173`) and the test one
(`ConnectionConfig::default_for_testing`, `deps.rs:216`). Two of those three are real.

**And it is not merely unused — it is already wrong.** If someone ever *did* adopt it, they
would get a silently mis-assembled handler. Two verified divergences from the production
assembly at `subsystems.rs:527–545`:

| Dependency | Builder (`with_admin_parts` / `with_cluster_parts`) | Production (`subsystems.rs`) |
|---|---|---|
| `cursor_store` | `Arc::new(AggregateCursorStore::new())` minted per builder (builder.rs:97) | `cursor_store.clone()` — one store shared node-wide (subsystems.rs:531) |
| `recovery_stats` | `Default::default()` (builder.rs:98) — INFO persistence' `rdb_last_load_*` would report zeros | `self.recovery_stats.clone()` — the real boot stats (subsystems.rs:532) |
| `pubsub_forwarder` | **unreachable.** `with_cluster_parts` routes through `ClusterDeps::cluster`, which hard-codes `pubsub_forwarder: None` (deps.rs:159), and the builder only overrides `quorum_checker` (builder.rs:122–133) | set explicitly (subsystems.rs:545) |

A cluster-mode connection built through the builder would have cluster pub/sub forwarding
silently off. Deleting the file removes a trap, not just lines.

**The cascade.** `ClusterDeps::cluster` (deps.rs:137–161, 25 lines) has exactly **one**
caller in the tree: `builder.rs:124`. Production builds `ClusterDeps` as a direct literal
with all ten fields (subsystems.rs:536). So the constructor falls with the builder — see
*Proposed change* for the explicit branch.

### SV6 — one refusal, twenty-eight copies

`ExecutionStrategy::ServerWide(op)` means the command's real implementation lives at the
connection level, not on a shard. `dispatch.rs`'s `dispatch_server_wide` (223–…) is a
**total** match over `ServerWideOp` — the doc there records that totality is the point
("the compiler then forces a match arm here … so the name-keyed table plus drift tests this
replaced are gone"). Every arm routes to a `ConnectionHandler::handle_*`. The shard never
receives a `Command` for these; when a fan-out is needed, the connection sends
`CoreMsg::ScatterRequest` and the shard answers it from `execute_scatter_part_body`
(`core/shard/execution.rs:739`), a separate match that never resolves a `Command`.

The three production `handler.execute(…)` call sites, and why each is closed to
server-wide commands:

| Call site | Closed by |
|---|---|
| `core/shard/execution.rs:241` — the shard command executor | The connection intercepts `ServerWide` at `DispatchStage::ServerWide` (dispatch.rs:67, 138) and never sends a shard `Command`. **This is the regression the tripwire guards.** |
| `core/shard/scripting.rs:223` — Lua `redis.call` | `ScriptGate::reject_server_wide` (`core/scripting/gate.rs:437–451`) returns a clean error first |
| `core/scripting/gate.rs:506` | same gate |
| `connection/transaction.rs:141` → `run_server_wide` (240) | EXEC defers `ServerWide(op)` past the shard transaction into the *same* `dispatch_server_wide` |

So the body is a deliberate tripwire, exactly as `commands/search.rs:3–7` documents: "fail
loudly so a routing regression yields an ERR reply instead of a fabricated success." Good
design — written out longhand twenty-eight times.

`grep -rn "server-wide command reached shard executor" --include='*.rs'` returns **28**
hits, matching the lane count exactly:

| File | Count | Body shape |
|---|---|---|
| `server/src/commands/search.rs` | 22 | Character-identical, including the 5-line comment |
| `commands/src/timeseries.rs` | 4 | Character-identical to the search.rs 22 |
| `server/src/commands/migrate_cmd.rs` | 1 (52) | Same error, site-specific comment ("`handle_migrate` does its own parsing and async network I/O … rather than leak an internal `MigrateNeeded` signal") |
| `server/src/commands/server.rs` | 1 (231, SHUTDOWN) | Same error, site-specific comment |

**Not every `ServerWide` command is in this class**, and the difference is load-bearing:
`strategy: ExecutionStrategy::ServerWide(` appears 36 times, of which 8 are *not* in the
list above — DBSIZE (server.rs:40), FLUSHDB (72), FLUSHALL (120), SCAN/KEYS
(`commands/src/scan.rs:62,129`), RANDOMKEY (`commands/src/generic.rs:648`), ES.ALL
(`commands/src/event_sourcing/all.rs:29`), and one test probe (`gate.rs:980`). Those carry
real `execute()` bodies. SV6 touches **none** of them: the change is scoped by the identical
body, not by the strategy.

The duplication cost is locality, in the exact sense of this vocabulary. "What FrogDB
replies when a server-wide command reaches a shard executor" is one fact stored in
twenty-eight textual places. Changing the message, the error variant, or the reasoning
means twenty-eight edits with nothing enforcing that they agree; and the twenty-eight
identical five-line comments are twenty-eight copies of a *rationale* that will not stay in
step with the dispatch code it describes.

**Precedent check (lane claim, verified and refined).** `commands/stub.rs` holds three
crate-local `macro_rules!`, each generating an entire `impl Command` — spec, `is_stub`,
`execute`: `stub_command!` (23, **1** invocation), `not_supported_command!` (134, **3**),
`db_not_supported_command!` (205, **2**). None is `#[macro_export]`. So the precedent
establishes the *shape* — a macro that emits a whole `Command` impl for a class of commands
sharing a reply — but at a scale of one to three, while the 28-fold duplication has none.

**Why the whole-impl shape does not fit here** (this is the design constraint the lane brief
did not have). The 28 sites do **not** share a spec skeleton:

- `MigrateCommand` (migrate_cmd.rs:21) has `keys: KeySpec::Dynamic`, `access:
  AccessSpec::UniformRW`, a three-flag union, **and an extra `dynamic_keys` trait method**.
- `ShutdownCommand` (server.rs:200) has a four-flag union.
- Inside `search.rs`, only **7 of 13** `CommandSpec` fields are constant across all 26 impls
  (`access`, `wal`, `wakes`, `requires_same_slot`, `reindex`, `lookup`, `mutation`). `flags`
  splits 15 `READONLY` / 11 `WRITE`; `event` splits 15 `NotApplicable` / 11 `Suppressed`;
  `keys` splits 22 `None` / 4 `First` (the four `First` are the key-based FT.SUG* commands,
  which are `Standard`, not `ServerWide`).

A whole-impl macro would therefore cover 26 of 28 and leave MIGRATE and SHUTDOWN
hand-written — i.e. it would fail to unify the one thing that actually matters, the refusal
reply, while burying 26 heterogeneous specs behind a six-argument parameter list. That also
cuts against the flat, greppable command-spec literal the codebase deliberately keeps.

### SV7 — one shard-0 read, five implementations, one divergent arm

`grep -rn 'shard_senders\[0\]' --include='*.rs'` returns six hits in the connection layer.
Five are the same routine:

| Site | Command | Empty-`Keyed` fallback |
|---|---|---|
| `search/helpers.rs:48` `query_shard0` — **the helper** | callers: FT.CONFIG GET (`config.rs:23`), FT.DICTDUMP (`dict.rs:41`) | `Response::Array(vec![])` |
| `search/index_mgmt.rs:82–107` | FT.INFO | **`Response::error("ERR empty response")`** |
| `search/index_mgmt.rs:112–137` | FT._LIST | `Response::Array(vec![])` |
| `search/explain.rs:21–61` | FT.EXPLAIN / FT.EXPLAINCLI | **`Response::error("ERR empty response")`** |
| `search/synonyms.rs:61–86` | FT.SYNDUMP | `Response::Array(vec![])` |

The sixth (`debug_handler.rs:173`) is a different message type — see *Risks*.

Everything except that last column is **identical** in all five, character for character:
the `oneshot::channel()`, the `CoreMsg::ScatterRequest { request_id: next_txid(), keys:
vec![], operation, conn_id: self.state.id, response_tx }`, the send guard
(`"ERR shard unavailable"`), `tokio::time::timeout(self.scatter_gather_timeout, …)`, the
`as_shard_error()` passthrough, `"ERR shard dropped request"`, `"ERR timeout"`.
`explain.rs` additionally post-processes the success value for `cli_mode` (42–53).

**Has it already drifted? Yes — and the drift is latent, not live.** Correcting the lane
brief's "LIVE drift risk":

`into_keyed_results()` (`core/shard/types.rs:854`) yields an empty vec for any *non-`Keyed`*
`PartialResult`. So the divergent arm fires only if a shard-0 FT reply arrives as something
other than `Keyed`. Two facts close that off in the current tree:

1. **Success path.** `execute_scatter_part_body` (`execution.rs:739`) sends every FT
   shard-0 op — `FtInfo` (868), `FtList` (869), `FtSyndump`, `FtDictdump` (911), `FtConfig`
   (912), `FtExplain` (917) — down the `Vec<(Bytes, Response)>` arm and wraps the result
   with `PartialResult::keyed(results)` at 924. Each `execute_ft_*` returns exactly one
   pair on *both* branches (e.g. `core/shard/search/index_mgmt.rs:29`'s
   `execute_ft_info` returns `vec![(b"__ft_info__", Response::error("Unknown index name"))]`
   for a missing index). Never empty.
2. **Refusal / panic path.** `scatter_error_reply`'s keyless arm returns
   `PartialResult::shard_error(err)` (`dispatch_core.rs:220`) — deliberately, per the
   comment there, because the old empty-`Keyed` shape "dropped the error silently". All five
   sites check `as_shard_error()` *before* `into_keyed_results()`, so a refusal is surfaced,
   not folded into the empty arm.

So the divergence is **unobservable today**. It is still worth fixing, for two reasons.
First, it is one fact ("what a shard-0 read replies when the shard returns nothing") stored
in five places with two different answers, and only the copies' proximity to each other
makes that visible at all. Second — and this is the part that makes SV7 a design decision
rather than a sweep — **the naive dedup is a silent behavior change**: pointing all four
copies at today's `query_shard0` would flip FT.INFO and FT.EXPLAIN from `-ERR empty
response` to `*0`. And each of the two answers is *right for its command*: FT.INFO and
FT.EXPLAIN must return a value, so an empty reply is a defect worth naming; FT._LIST,
FT.SYNDUMP, FT.CONFIG GET and FT.DICTDUMP legitimately have empty results. The fallback is a
per-caller fact, so it belongs in the interface, not flattened out of it.

## Proposed change

### SV5 — delete the builder

1. `git rm frogdb-server/crates/server/src/connection/builder.rs`.
2. `connection.rs`: drop `mod builder;` and the `pub use builder::{ConnectionHandlerBuilder,
   connection_builder, standalone_config};` (71). Rewrite the doc sentence at line 6 to name
   only `from_deps` and the four dependency structs — the seam that actually exists.
3. **`ClusterDeps::cluster` (deps.rs:137–161): delete it too.** Its only caller is
   builder.rs:124. *(Alternative, explicitly rejected: keep it as "the documented cluster-mode
   assembly." It is not — it cannot express `pubsub_forwarder`, so keeping it preserves a
   constructor that builds a subtly wrong `ClusterDeps`. Keeping a wrong constructor because
   it reads like documentation is how the builder got here.)* If sibling **64** wants a named
   cluster-deps constructor as part of its `subsystems.rs` work, it should introduce a
   correct one rather than inherit this one; note the hand-off rather than blocking on it.
4. Confirm no doc link breaks: `connection.rs:6` is the only prose reference in the crate;
   the two `.scratch/testing-improvements-round2/` mentions are historical notes that
   *predicted* this deletion and need no edit (git history is the archive).

Verification: `just check frogdb-server`, `just fmt`. There is no behavior to test — the
change is a subtraction of unreachable code, which is the whole claim.

### SV6 — one refusal, two mechanisms, twenty-eight one-line call sites

Split the concern in two, because there are genuinely two facts:

**(a) The reply — a function in `frogdb-core`.** The message and the reasoning are one fact
and belong at the type that owns `CommandError`:

```rust
/// The reply a `ServerWide` command gives if it is ever executed on a shard.
///
/// `ExecutionStrategy::ServerWide` commands run at the connection level via
/// `dispatch_server_wide` (an all-shard fan-out, or a connection-local handler),
/// never on a shard, and the scripting gate refuses them before `run_local`.
/// Reaching a shard executor is therefore a routing regression — fail loudly
/// rather than fabricate a reply.
pub fn server_wide_reached_shard_executor() -> CommandError {
    CommandError::Internal {
        message: "internal: server-wide command reached shard executor".to_string(),
    }
}
```

**(b) The method boilerplate — one `#[macro_export] macro_rules!` in `frogdb-core`**,
emitting only the `execute` method (not the spec), so it fits all 28 sites regardless of
spec shape:

```rust
#[macro_export]
macro_rules! server_wide_command_execute {
    () => {
        fn execute(
            &self,
            _ctx: &mut $crate::CommandContext,
            _args: &[bytes::Bytes],
        ) -> Result<$crate::Response…, $crate::CommandError> {
            Err($crate::command::server_wide_reached_shard_executor())
        }
    };
}
```

Each of the 28 impls then reads `server_wide_command_execute!();` in place of ~13 lines.
MIGRATE and SHUTDOWN keep their site-specific prose as a one-line `//` comment above the
invocation — the part of their comment that is *shared* moves to the function's doc.

**Macro home — decide explicitly.** `frogdb-core` currently has **zero** `#[macro_export]`
(verified across `core/src` and `commands/src`; the only `macro_rules!` are crate-local:
`core/src/command.rs:1793` `wal_mock!`, `core/src/store/typed.rs:222`
`typed_family_accessors!`, `commands/src/json/mod.rs:83,100`). Exporting one puts
`frogdb_core::server_wide_command_execute!` at the crate root, which is a new precedent.

- **(chosen) One `#[macro_export]` in core.** The 28 sites span two crates
  (`frogdb-server` ×24, `frogdb-commands` ×4); a crate-local macro cannot serve both.
- **(fallback, if the export is judged too big a step) Two crate-local `macro_rules!`**, one
  per crate, matching `stub.rs` exactly. Acceptable **only because (a) already single-sources
  the message** — the two macro copies would then be pure boilerplate with no fact in them.
  Without (a), this fallback re-forks the string and is not worth doing.

Deletion test, both mechanisms: delete the macro and 28 sites regrow ~13 lines each (~364
lines) plus a re-forked comment; delete the function and the message re-forks across macro
copies. Both earn their keep.

**Rejected (larger, not precluded).** Make the tripwire structural rather than textual:
split `Command` so a `ServerWide` command has no shard executor to implement at all. That is
the real fix for "unreachable method that must nonetheless exist", but it reshapes the
`CommandRegistry`'s `dyn Command` storage and every dispatch site — L-sized, and SV6 does
not stand in its way (28 one-line invocations are *easier* to delete wholesale than 28
hand-written bodies).

**Landing shape — two commits, because of the feature gate.** `commands/src/timeseries.rs`
sits behind `#[cfg(feature = "timeseries")]` (`commands/src/lib.rs:54`), which is **not** in
`core-profile` (`commands/Cargo.toml:15–18`); the server mirrors it as `cmd-timeseries`
(`server/Cargo.toml:80`). So `just check frogdb-server` and `just check frogdb-commands` at
default features **do not compile those four sites**. Land the 24 server-crate sites first
under default features, then the 4 timeseries sites in a second commit checked with
`--features cmd-timeseries` — and do not alternate feature flags inside one iteration loop
(it thrashes the build cache).

### SV7 — parameterize the fallback, then delete the copies

One new parameter on the existing helper, one thin wrapper preserving today's default:

```rust
/// Send an operation to shard 0 only and return its response.
///
/// `on_empty` is the reply when the shard answers with no keyed result — a
/// defensive arm (every `execute_ft_*` returns exactly one pair, and a refusal
/// arrives as `PartialResult::ShardError`, caught above). Callers that must
/// return a value (FT.INFO, FT.EXPLAIN) pass an error; callers whose result may
/// legitimately be empty (FT._LIST, FT.SYNDUMP, FT.CONFIG GET, FT.DICTDUMP)
/// pass an empty array.
pub(crate) async fn query_shard0_or(&self, operation: ScatterOp, on_empty: Response) -> Response

/// Shard-0 query whose empty result is an empty array.
pub(crate) async fn query_shard0(&self, operation: ScatterOp) -> Response {
    self.query_shard0_or(operation, Response::Array(vec![])).await
}
```

Then:

1. `handle_ft_info` (index_mgmt.rs:74) →
   `self.query_shard0_or(ScatterOp::FtInfo { index_name }, Response::error("ERR empty response")).await`
2. `handle_ft_list` (index_mgmt.rs:111) → `self.query_shard0(ScatterOp::FtList).await`
3. `handle_ft_syndump` (synonyms.rs:55) →
   `self.query_shard0(ScatterOp::FtSyndump { index_name }).await`
4. `handle_ft_explain` (explain.rs:12) → `query_shard0_or(…, Response::error("ERR empty
   response"))`, then apply the existing `cli_mode` line-split to the **returned** response
   instead of inside the match arm.

**Step 4 is behavior-preserving, and the reason must be written down rather than assumed.**
Today the transform runs only on the value from `into_keyed_results()`; afterwards it runs
on whatever the helper returns. It is guarded by `if let Response::Bulk(Some(ref b)) = resp`,
and every path the helper can return *other than* the shard's own value is a
`Response::Error`: `"ERR shard unavailable"`, `"ERR timeout"`, `"ERR shard dropped
request"`, the `on_empty` error, and the shard error itself (built at
`dispatch_core.rs:220` from a `Response::Error`). None matches `Bulk(Some(_))`, so the
transform stays a no-op on all of them. Pin this with a test rather than leaving it as a
reading (see *Testability*).

Deletion test on the helper: delete `query_shard0_or` and the transport, the timeout, and
four failure replies reappear at six call sites. It earns its keep — and it is now **deep**
in the sense that matters: six callers, one interface, whose only variable is a single
`Response`.

Net: ~92 lines removed across the three copies-with-transport plus explain's, ~10 added.
Imports drop with the copies (`tokio::sync::oneshot`, `CoreMsg`, `next_txid` become unused
in `explain.rs` and are trimmed in the other two).

## Testability improvement

- **SV5.** No test change: the deleted module has no tests and no callers. The improvement
  is negative surface — the crate stops publishing a construction interface that nothing
  exercises, so `ConnectionHandler`'s test surface and its production surface become the
  same seam (`from_deps`). *The interface is the test surface*; today there are two, and
  only one of them is ever crossed.
- **SV6.** Twenty-eight untestable-by-construction bodies collapse to one function that
  **can** be unit-tested in `frogdb-core` — assert `server_wide_reached_shard_executor()`
  is `CommandError::Internal` with the expected message, in the crate that owns the type.
  Today the tripwire's payload is asserted nowhere (`grep` for the message across `tests/`,
  `redis-regression/` and `testing/` returns nothing). Worth adding alongside: a registry
  test asserting that every `ExecutionStrategy::ServerWide` command's name has a
  `ServerWideOp` arm — the dispatch match is already total, so this is cheap and pins the
  *other* half of the contract.
- **SV7.** The defensive arms — unavailable / dropped / timeout / shard-error / empty —
  become one function's behavior instead of five copies', so one test per arm covers six
  call sites rather than one. Concretely worth pinning: (i) a shard-error reply is
  surfaced verbatim and not folded into `on_empty` (the regression `dispatch_core.rs:220`
  was written to prevent), and (ii) the FT.EXPLAINCLI transform does not fire on an error
  reply — the exact invariant step 4 relies on, currently guaranteed only by reading.

## Spec / LOCKED impact

**None owed, for all three parts.**

- No locked crate is touched. The locked areas are txn (`frogdb-txn` + `frogdb-vll`),
  persistence (`frogdb-persistence` + `frogdb-recovery`), replication
  (`frogdb-replication` + `frogdb-replication-runtime`) and cluster (`frogdb-cluster` +
  `frogdb-cluster-runtime`); 67 touches `frogdb-server`, `frogdb-commands` and
  `frogdb-core`, none of which is in that set. No `just mutants-gate` obligation, and
  `just mutants-diff` is not owed as push discipline.
- **No FM-tagged test is touched.** `grep -rn 'FM-'` over exactly the ten files in the
  *Files involved* table returns nothing. (The server crate does carry FM tags — `commands/info.rs`, `commands/cluster/mod.rs` — but none of those files is in scope.)
  `just lint-failure-modes` therefore needs no spec edit; run it anyway, since it is in
  `just lint`.
- **Seam gates.** None of the fifteen `lint-gates` members covers these files:
  `lint-error-sanitize` is single-file-pinned to `protocol/src/response.rs` (the gate's own
  docstring says so); no metric emission or `clock::` read is relocated, so
  `lint-metrics-chokepoint` and `lint-clock-seam` are untouched; `lint-continuation-lock`
  counts arms of the shard `*Msg` enums, which none of the three parts edits. Run
  `just lint-gates` regardless — it is compile-free, sub-second, and lefthook runs it on
  every commit.
- **No wire-visible behavior changes.** SV6 emits a byte-identical error. SV7 preserves each
  call site's existing reply on every arm, including the two divergent ones — that
  preservation is the design constraint, not an afterthought.

## Risks / scope boundaries

### Boundaries vs sibling proposals

67 shares **no file** with any sibling. The nearest edges, stated per part:

| Sibling | Owns | Edge with 67 | Resolution |
|---|---|---|---|
| **63** server/mod.rs + init.rs | `server/mod.rs`, `server/init.rs` | None | — |
| **64** subsystems.rs | `server/subsystems.rs` | **SV5, read-only-adjacent.** 67 cites `subsystems.rs:527–545` as the production `AdminDeps`/`ClusterDeps` assembly and **edits nothing there**. But SV5 deletes `ClusterDeps::cluster` from `connection/deps.rs`, and if 64's restructure adopts that constructor, it would resurrect a caller | Land SV5 first (it is a pure subtraction) or tell 64 the constructor is going. If 64 wants a named cluster-deps constructor, it should write a correct one — this one cannot set `pubsub_forwarder`. Either way 67 does not touch `subsystems.rs` |
| **65** cluster_init.rs | `server/cluster_init.rs` | None | — |
| **66** shard builder / shards.rs | `core/src/shard/builder.rs`, `shards.rs` | **Name collision only.** There are two `builder.rs` files; 67 deletes `server/src/connection/builder.rs`, 66 owns `core/src/shard/builder.rs`. Different crates, different files | Cite full paths in commits |
| **68** exec-framing | `connection/transaction.rs`, `connection/dispatch.rs`, `connection/pubsub_conn_command.rs` | **SV6, read-only.** 67 cites `dispatch.rs:223–260` (`dispatch_server_wide`) and `transaction.rs:141,240` as the unreachability evidence and edits neither | If 68 reshapes `DispatchStage::ServerWide` or the EXEC deferral, SV6's *rationale prose* (the function doc in core) may need a wording refresh; the code does not. Whichever lands second re-reads the doc |
| **69** runtime_config.rs | `server/src/runtime_config.rs` | None | — |
| **70** acl | acl crate / `AclManager` | None. SV5 reads `CoreDeps.acl_manager` as a field name only | — |
| **71** search index.rs / merge.rs | Ambiguous by name — resolve to `crates/search/src/index.rs` and/or `connection/search/merge.rs` before starting | **SV7, adjacent; SV6, not at all.** SV6's 22 sites are in the **server** crate (`server/src/commands/search.rs` — command *specs*), not the search crate and not `frogdb-commands`; the other 4 identical sites *are* commands-crate (`commands/src/timeseries.rs`). SV7 edits four files in `connection/search/` but **not** `merge.rs`: `index_mgmt.rs` and `synonyms.rs` import `super::merge::OkOrFirstError` for their *broadcast* handlers (`handle_ft_alter`, `handle_ft_dropindex`, `handle_ft_synupdate`), which SV7 leaves untouched | Confirm 71's actual file set first. If 71 owns `connection/search/merge.rs`, the only contact is those two `use` lines, which SV7 does not modify. If 71 owns `crates/search/src/index.rs`, there is no contact at all |

### Other risks

- **SV5 — public-API removal.** `ConnectionHandlerBuilder`, `connection_builder` and
  `standalone_config` are `pub` in a `pub mod`, so this is technically a breaking change to
  `frogdb-server`'s library surface. The crate is consumed only inside this workspace
  (verified: no reference from `frogctl`, `frogdb-operator`, `ops/`, benches or tests), and
  FrogDB is pre-production, where breaking changes are policy. No deprecation cycle.
- **SV6 — macros are less greppable than the methods they replace.** After the change,
  `grep "fn execute"` no longer finds these 28 commands, and IDE "go to definition" lands on
  a macro. Mitigation: the macro name is explicit at every site
  (`server_wide_command_execute!();`) and grepping *it* finds all 28 — arguably a better
  index than today's. No lint script greps `fn execute` (verified across `scripts/*.py`).
- **SV6 — `#[macro_export]` puts a name at `frogdb_core`'s root.** That is how
  `macro_rules!` export works; the name is distinctive enough not to collide. If this is
  unwanted, take the two-crate-local-macros fallback — it costs one extra macro copy and
  loses nothing, because the message already lives in the core function.
- **SV6 — do not widen the scope by strategy.** The eight `ServerWide` commands with real
  `execute()` bodies (DBSIZE, FLUSHDB, FLUSHALL, SCAN, KEYS, RANDOMKEY, ES.ALL, and the
  gate's test probe) look like candidates and are not. DBSIZE's body reads `ctx.store.len()`;
  FLUSHDB/FLUSHALL call `ctx.store.clear()`. Whether *those* are reachable is a separate
  question this proposal deliberately does not answer or disturb.
- **SV7 — the `cli_mode` relocation is the only judgement call.** It is argued above and
  should be pinned by a test rather than accepted on reading. If that test is inconvenient,
  the conservative variant is to have `explain.rs` keep its own two-line wrapper that only
  transforms when the helper returned a non-error — no worse than the main design and still
  removes the copied transport.
- **SV7 — a sixth shard-0 send exists and is out of scope.**
  `connection/debug_handler.rs:173` sends `frogdb_core::shard::SearchMsg::GetPubSubLimitsInfo`
  to `shard_senders[0]` with its own hard-coded `Duration::from_secs(5)` instead of
  `self.scatter_gather_timeout`, and its own two error strings. It is a *different message
  type* (`SearchMsg`, not `CoreMsg::ScatterRequest`), so `query_shard0` cannot absorb it.
  But the hard-coded timeout beside a configurable one is genuine policy drift and is worth
  a follow-up item; folding it into SV7 would require a second helper for a second message
  family, which is one adapter, i.e. a hypothetical seam.
- **SV7 — do not "fix" the two divergent fallbacks in this change.** Whether FT.INFO should
  reply `-ERR empty response` or `*0` when a shard returns nothing is a behavior question on
  an unreachable arm. Preserve both verbatim. If either is wrong, that is a separate change
  with its own reasoning — and, because the arm is unreachable, probably not worth making.

## Effort estimate

**S overall**, four separable commits, no cross-part ordering constraint:

| Item | Effort | Notes |
|---|---|---|
| SV5 | **XS–S** | One file deleted, two lines edited in `connection.rs`, one constructor deleted in `deps.rs`. Compiles crate-locally (`just check frogdb-server`). The only judgement call is the `ClusterDeps::cluster` cascade, and it is argued above |
| SV6 (24 server-crate sites) | **S** | One function + one macro in core, 24 mechanical call-site edits. Largest diff (~310 lines out), lowest risk — the emitted error is byte-identical |
| SV6 (4 timeseries sites) | **XS** | Separate commit; must be checked with `--features cmd-timeseries`, which the default check does not build |
| SV7 | **S** | One parameter, one wrapper, four call sites rewritten (~92 lines out, ~10 in). The `cli_mode` relocation needs the argued check plus a test |
| Mutation re-gate | **none** | No locked crate touched |

**Independently-landable hotfix candidates: none.** Every part is latent. SV5 removes a trap
that would only fire if someone adopted the dead builder; SV6 changes no bytes on the wire;
SV7's divergence sits on an arm that the current tree cannot reach. If any one of the three
had to be picked as "most valuable first", it is **SV5** — it is the only part where the dead
code is actively *costing* something today (five of five commits to the file were tax
payments), and it is the cheapest to verify, being a literal deletion with a literal zero
callers.

**Recommended landing order:** SV5 → SV7 → SV6(server) → SV6(timeseries). SV5 first because
it is pure subtraction and unblocks any coordination with sibling 64. SV6's timeseries
commit last because it is the only one needing a non-default feature build.
