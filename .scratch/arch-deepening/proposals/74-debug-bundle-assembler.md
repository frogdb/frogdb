# Proposal 74 — Debug Bundle: one `BundleAssembler`, and turn the feature on

Round 38 · lane: frogctl / operator / telemetry · effort **M** · candidate FR3 · independent of
proposals 72/73 (no shared files)

Verified against the current tree at `0448656916439cf795153ad3110a9334996baf5a` (worktree
`arch-round-38-99`, branch `main`). The two commits since `760c2cc1` touch only
`.scratch/arch-deepening/proposals/*.md`; **no source file cited below has changed** in that
range (`git diff --stat 760c2cc1..HEAD` = 3 proposal docs). Every path, line number and count was
re-derived by reading at this SHA.

**One lane-brief claim is wrong and is corrected against a standing ruling** (FR3(d) "role
frozen" — see §Problem 0). Three brief claims are confirmed LIVE. **Seven defects the brief did
not name were found**, two of them latent security/availability bugs that any wiring change
activates.

## Summary

The Debug Bundle is FrogDB's forensic-artifact feature: an operator asks the server to zip up
what it knows about itself and hands the archive to whoever is debugging. The vocabulary is
ruled — `frogdb-server/CONTEXT.md` names it **Debug Bundle** and lists "diagnostic bundle" under
_Avoid_.

There is a full implementation of it — collector, zip generator, retention store, four HTTP
endpoints, two RESP subcommands, an HTML panel, a config section published on the website, and a
separate shipped binary (`frogdb-admin`) whose *only* command family is bundles. **None of it
works.** The HTTP path is unreachable because the one builder method that installs the store is
never called; the RESP path works but ignores the operator's configuration entirely, hardcoding
`BundleConfig::default()` at four sites. What ships in the `.deb` is an `/etc/frogdb/frogdb.toml`
section that configures nothing, a website page documenting bundle contents that are never
collected, and a `/usr/bin/frogdb-admin` that returns "Bundle support not enabled on the server."
against every FrogDB build.

The cause is that **the bundle has no module**. "What goes in a bundle, where it lands, how long
it lives" is smeared across five files in three crates, and each consumer re-assembles it by
hand: `DebugState::generate_bundle` (dead), `DebugState::generate_bundle_streaming`, and
`DebugCommandHandler::bundle_generate` are three hand-written copies of the same
collect→zip→store sequence, and they disagree — the two web copies fill `cluster_state.json` from
node state, the RESP copy ships it empty. There is no single place to configure, no single place
to test, and no single thing to wire, which is exactly why the wiring was never done.

**The change**: give the Debug Bundle a module — `BundleAssembler` in `frogdb-debug` — that owns
the whole operation behind a four-method interface, built once in `subsystems.rs` and shared by
both consumers as an `Arc`. The consumers become adapters: three lines each. `BundleConfig` and
its five duplicated default constants delete in favour of the config crate's `DebugBundleConfig`;
`DebugBundleConfigExt` (a conversion trait with zero call sites) deletes; `BundleGenerator`'s
never-read `_config` field deletes; `DebugState::generate_bundle`'s 44 dead lines delete. A
`BundleId` newtype whose only public constructor validates the character set closes the path
traversal structurally rather than with a check.

Net: ~120 lines delete outright, ~200 lines of duplicated orchestration collapse to one, and the
feature becomes reachable — which is the point. The leverage is unusual: **one wiring line lights
up four HTTP endpoints, two RESP subcommands, one web panel and an entire shipped binary.**

## Files involved

| File | Lines | Role in this change |
|---|---|---|
| `frogdb-server/crates/debug/src/bundle/mod.rs` | 57 | **the change.** `DEFAULT_*` consts `:18-30`, `BundleConfig` `:34-45`, `impl Default` `:47-57` — all delete. Becomes module decl + re-exports only |
| `frogdb-server/crates/debug/src/bundle/assembler.rs` | *new (~180)* | **the change.** `BundleAssembler`, `BundleId`, `NodeIdentity`, `BundleContext`, `Window`, `BundleError` |
| `frogdb-server/crates/debug/src/bundle/generator.rs` | 89 | **the change.** `_config` field `:13` deletes; `create_zip` `:37-88` (4 copy-paste blocks) → one `sections()` loop; `generate_id` `:23-34` moves behind `BundleId::generate` |
| `frogdb-server/crates/debug/src/bundle/store.rs` | 123 | **the change.** `bundle_path` `:89-91` takes `&BundleId`; `enforce_capacity` `:115-122` spin bug fixed by `NonZeroUsize` |
| `frogdb-server/crates/debug/src/bundle/collector.rs` | 192 | **the change.** `DiagnosticData` `:13-23` gains the node context; `cluster_state: ClusterStateJson::default()` `:118`/`:147` deletes; dead `_before` snapshot `:133` resolved |
| `frogdb-server/crates/debug/src/web_ui/state.rs` | 976 | **the change.** `bundle_store`/`bundle_config`/`shared_tracer` fields `:346-350` → one `Option<Arc<BundleAssembler>>`; `with_bundle_support` `:433-442`; `bundle_enabled` `:579-581`; `generate_bundle` `:600-643` **deletes** (zero callers); `generate_bundle_streaming` `:646-690` → 3-line adapter |
| `frogdb-server/crates/debug/src/web_ui/handlers.rs` | 1605 | **the change (4 hunks).** `handle_api_bundle_list` `:1394-1408`, `handle_api_bundle_generate` `:1414-1454`, `handle_api_bundle_download` `:1457-1490` (gains `BundleId::parse`), `handle_partial_bundles` `:1505+`. Also `<h3>Diagnostic Bundles</h3>` → naming ruling |
| `frogdb-server/crates/debug/src/web_ui/routes.rs` | 208 | **the change (1 hunk).** unvalidated id extraction `:57-64` |
| `frogdb-server/crates/debug/Cargo.toml` | — | **the change.** adds `frogdb-config` dep (verified acyclic) |
| `frogdb-server/crates/server/src/connection/debug_handler.rs` | 374 | **the change (2 hunks).** `bundle_generate` `:222-257` (hardcoded default at `:225`,`:240`,`:246`), `bundle_list` `:260-277` (`:261`). Both → adapters |
| `frogdb-server/crates/server/src/server/subsystems.rs` | 930 | **the change (2 hunks).** build the assembler once; `DebugState` `:210-222` gains `.with_bundle_support(...)`; `ObservabilityDeps` `:547-557` gains the same `Arc` |
| `frogdb-server/crates/server/src/config/mod.rs` | 650 | **the change.** `DebugBundleConfigExt` `:195-199` + impl `:201-211` **delete** (zero call sites) |
| `frogdb-server/crates/config/src/debug_bundle.rs` | 91 | **the change.** gains `validate()`; the 5 `DEFAULT_*` consts `:8-20` become the single source |
| `frogdb-server/crates/config/src/lib.rs` | 479 | **the change (1 line).** `Config::validate` `:306` gains `self.debug_bundle.validate()?` |
| `scripts/clock-seam.py` | 276 | **the change (mandatory).** ALLOWLIST `:94-107` pins exact counts for `generator.rs` (1), `store.rs` (1), `collector.rs` (2) — bidirectionally enforced |
| `frogdb-server/crates/server/tests/integration_debug_http.rs` | 1069 | **the change.** 9 bundle tests `:730,:764,:791,:828,:864,:898,:942,:995,:1021` — every one currently a no-op |
| `frogdb-server/crates/server/tests/integration_debug_bundle.rs` | 284 | **the change.** 7 RESP tests, each with an "or ERR" escape hatch |
| `website/src/content/docs/operations/diagnostics.mdx` | 340 | **the change.** `:11`, `:298-308`, `:322-330` describe contents that are never collected |
| `frogdb-server/ops/frogdb-admin/src/main.rs` | 130 | read-only evidence. Only command family is `DebugBundle {Generate,List,Download}` |
| `frogdb-server/ops/frogdb-admin/src/client.rs` | 169 | read-only evidence. `BundleInfo` `:8-12` hand-mirrors the server type; every method has `503 => bail!("Bundle support not enabled on the server.")` |
| `frogctl/src/commands/debug.rs` | 770 | **the change (1 arm).** `DebugCommand::Zip` `:17-29`, dispatch `:393-395` bails — ceded to this proposal by 73 |
| `frogctl/Cargo.toml` | 47 | **the change (1 line).** `zip.workspace = true` `:41` — zero uses; stays zero. Delete |
| `frogdb-server/crates/server/src/debug_providers.rs` | 491 | read-only evidence. `replication()` `:106-127` reads `self.mode.current()` — the live seam |
| `frogdb-server/crates/telemetry/src/status.rs` | 1561 | read-only evidence. `LiveMode` `:426-434`, `current()` `:452-460` |
| `frogdb-server/ops/deploy/deb/frogdb.toml` | 222 | read-only evidence (generated). `[debug-bundle] directory = "frogdb-data/bundles"` `:186-191` |
| `frogdb-server/ops/deploy/deb/frogdb-server.service` | 34 | read-only evidence (generated). `ProtectSystem=strict` `:26`, `ReadWritePaths=/var/lib/frogdb /var/log/frogdb` `:28`, **no `WorkingDirectory`** |

## Problem

### 0. The standing ruling: "role frozen" is not true, and the ruling wins

The lane brief lists FR3(d) as "role frozen". **That claim is stale.**
`.scratch/arch-deepening/issues/open/12-status-mode-frozen-at-startup.md` is the standing ruling,
and its Resolution section (2026-07-21) records that the freeze was fixed: the startup-captured
`ReplicationMode` was replaced by `LiveMode`, an `Arc<AtomicBool>` seam
(`frogdb-server/crates/telemetry/src/status.rs:426-434`), constructed at
`subsystems.rs:85-96` and read per-call by `DebugStatusProvider::replication`
(`debug_providers.rs:106-127`, `self.mode.current()` / `self.mode.is_replica()`).
`DebugState::role()` (`state.rs:450-452`) delegates to that provider. Role is **live**. Per the
brief's own instruction, the ruling wins and is cited here.

What *is* true, and what the brief mislabelled, are two distinct accuracy defects in the same
neighbourhood:

- **The RESP bundle's `cluster_state.json` is a lie of omission.** `collect_instant`
  (`collector.rs:106-125`) and `collect_with_duration` (`:131-154`) both hardcode
  `cluster_state: ClusterStateJson::default()` (`:118`, `:147`). The collector has no access to
  node state, so a bundle taken over `DEBUG BUNDLE GENERATE` — the only path that works at all —
  ships a `cluster_state.json` of empty strings, zero shards and `cluster_enabled: false` **on a
  live cluster node**. This is the memory-noted observability rule ("misleading data is not ok")
  violated in a forensic artifact, which is the worst place for it.
- **The web path fills `mode` with the role.** `state.rs:620-622` and the byte-identical
  `:669-671`:

  ```rust
  data.cluster_state = crate::bundle::ClusterStateJson {
      mode: self.role(),
      role: self.role(),
      ...
  ```

  Two fields with different meanings, one value. `mode` is meant to be the deployment mode
  (standalone / cluster); it gets the replication role.

### 1. The web path is dark — CONFIRMED LIVE, end-to-end

`DebugState::with_bundle_support` (`state.rs:433-442`) is the only thing that ever sets
`bundle_store`. **It has zero call sites in the workspace.** Consequently
`bundle_enabled()` (`state.rs:579-581`, `self.bundle_store.is_some() && self.shard_senders.is_some()`)
is unconditionally `false`, and all four bundle surfaces take their 503 guard:

- `handle_api_bundle_list` `:1394-1408` → 503
- `handle_api_bundle_generate` `:1414-1454` → 503
- `handle_api_bundle_download` `:1457-1490` → 503
- `handle_partial_bundles` `:1505+` → renders "Bundle support is not enabled on this server."

`subsystems.rs:210-222` builds the `DebugState` with `.with_node_state(...)`,
`.with_shard_senders(...)`, `.with_hot_shards(...)` — and stops. The trace terminates in a
shipped artifact: `frogdb-server/ops/frogdb-admin` is a binary in the `.deb` and the Docker image
whose *entire* command surface is `DebugBundle {Generate, List, Download}` (`main.rs:130` lines
total), and each client method (`client.rs`) carries
`503 => bail!("Bundle support not enabled on the server.")`. **Every invocation of the shipped
`frogdb-admin` against every FrogDB build fails with that message.** The binary has never
worked.

### 2. The config is dead — CONFIRMED LIVE, end-to-end

`DebugBundleConfig` (`config/src/debug_bundle.rs`, 91 lines) is a fully-formed config section:
`#[params(section = "debug-bundle")]`, `deny_unknown_fields`, five fields each with an explicit
`#[param(skip)]` justification. It is deserialized from the operator's TOML and it reaches
nothing.

The only bridge to the runtime type is `DebugBundleConfigExt::to_bundle_config`
(`server/src/config/mod.rs:195-211`). **Zero call sites.** The one live consumer instead
hardcodes defaults, four times in fifty lines:

```rust
// debug_handler.rs:225   let config = frogdb_debug::BundleConfig::default();
// debug_handler.rs:240   let generator = frogdb_debug::BundleGenerator::new(config.clone());
// debug_handler.rs:246   let store = frogdb_debug::BundleStore::new(config);
// debug_handler.rs:261   let config = frogdb_debug::BundleConfig::default();
```

So the `[debug-bundle]` block shipped in `/etc/frogdb/frogdb.toml`
(`ops/deploy/deb/frogdb.toml:186-191`) and documented on the website
(`diagnostics.mdx:322-330`, all five keys) is inert: setting `directory`, `max-bundles`,
`bundle-ttl-secs` or `max-trace-entries` changes nothing an operator can observe.
`max-slowlog-entries` is worse than inert — see §4.

There are also **two byte-identical copies of the default table**, `bundle/mod.rs:18-30` and
`config/src/debug_bundle.rs:8-20`, with no test forcing them to agree. Today they agree by luck.

### 3. Three hand-written copies of one operation, one of them dead

The collect→zip→store sequence exists three times:

| Copy | Location | Fills `cluster_state`? | Callers |
|---|---|---|---|
| `DebugState::generate_bundle` | `state.rs:600-643` | yes | **zero** |
| `DebugState::generate_bundle_streaming` | `state.rs:646-690` | yes (identical block) | `handlers.rs:1434` |
| `DebugCommandHandler::bundle_generate` | `debug_handler.rs:222-257` | **no** | RESP dispatch |

44 lines at `state.rs:600-643` are unreachable — `pub` on a library type, so `dead_code` never
fires. The remaining two disagree on bundle *content*, which is the §0 accuracy defect: the same
feature produces two different archives depending on which door you knock on.

### 4. The bundle does not contain what the docs promise

`website/src/content/docs/operations/diagnostics.mdx:298-308` tells operators a bundle holds
"server state, configuration, metrics, slowlog entries, and recent traces". `DiagnosticData`
(`collector.rs:13-23`) has exactly four fields: `shard_memory`, `traces`, `cluster_state`,
`metadata`. **No configuration, no metrics, no slowlog.** `create_zip` (`generator.rs:37-88`)
writes exactly four entries: `metadata.json`, `shard_memory.json`, `traces.json`,
`cluster_state.json`.

Hence `max_slowlog_entries` — a config key, a documented key, a default constant duplicated in
two crates — caps a collection that does not exist. It is the only one of the five that reaches
no code path even in principle.

`collect_with_duration` also throws away half its work: `:133`
`let _before = self.gather_memory_stats().await;` runs a full scatter-gather across every shard
and discards the result, under a doc comment (`:127-130`) claiming it "allow[s] comparison of
metrics over time". The comparison is never made. On a large instance that is a wasted round of
`ObservabilityMsg` traffic to every shard.

### 5. Path traversal — LATENT, activated by the §1 fix (security)

`routes.rs:57-64` takes the bundle id straight off the URL with no validation:

```rust
p if p.starts_with("/api/bundle/") => {
    let id = p.strip_prefix("/api/bundle/").unwrap_or("");
    if !id.is_empty() && id != "list" && id != "generate" {
        handlers::handle_api_bundle_download(state, id)
```

and `BundleStore::bundle_path` (`store.rs:89-91`) joins it unsanitized:

```rust
self.config.directory.join(format!("{}.zip", id))
```

`get()` (`:68-71`) then `fs::read`s that path. `Path::join` on a `..`-bearing segment walks up.
The bundle directory is operator-configurable and by default relative, so the reachable set is
"any `.zip` the server user can read". This is unexploitable **today only because the 503 guard
fires first** — which means the §1 fix arms it. Any change that wires the store and does not also
fix this is a net security regression. That is why the id validation is part of this proposal's
core and not a follow-up.

Note the HTTP debug surface is bearer-token protected, so the severity is
"authenticated arbitrary-file-read scoped to `.zip`", not unauthenticated. It is still a
traversal.

### 6. `max_bundles = 0` hangs the server — LATENT, activated by the §2 fix

`store.rs:115-122`:

```rust
fn enforce_capacity(&self) {
    let mut bundles = self.list();
    while bundles.len() >= self.config.max_bundles {
        if let Some(oldest) = bundles.pop() {
            let _ = fs::remove_file(self.bundle_path(&oldest.id));
        }
    }
}
```

`bundles.len() >= 0` is always true for `usize`. Once the vector drains, `pop()` returns `None`,
the body is a no-op, and the condition still holds: **infinite loop, inside a `store()` call, on
whatever task is generating the bundle.** Today the operator's `max-bundles` never reaches this
code (§2), so `0` is unreachable. Wiring the config makes `max-bundles = 0` — a plausible
"disable retention" value — a server hang.

`Config::validate()` (`config/src/lib.rs:306`) calls `.validate()` on fifteen sections.
`debug_bundle` is **not one of them**, and `DebugBundleConfig` has no `validate()` to call. So
there is no layer that would reject the value either.

### 7. The default bundle directory is unwritable under the shipped systemd unit, silently

`ops/deploy/deb/frogdb-server.service` sets `ProtectSystem=strict` (`:26`) with
`ReadWritePaths=/var/lib/frogdb /var/log/frogdb` (`:28`) and **no `WorkingDirectory`**, so
systemd runs the unit with CWD `/`. `ops/deploy/deb/frogdb.toml:187` ships
`directory = "frogdb-data/bundles"` — a *relative* path, resolving to `/frogdb-data/bundles`,
which `ProtectSystem=strict` mounts read-only. (This is the generated deb config; the FHS
overrides that rewrite `persistence.data-dir` → `/var/lib/frogdb/data` and
`snapshot.snapshot-dir` → `/var/lib/frogdb/snapshots` were never extended to `debug-bundle`.)

The failure is then swallowed. `debug_handler.rs:246-250`:

```rust
let store = frogdb_debug::BundleStore::new(config);
if let Err(e) = store.store(&id, &zip_data) {
    tracing::warn!(error = %e, "Failed to store bundle (HTTP download may not work)");
}
Response::Bulk(Some(Bytes::from(id)))
```

`DEBUG BUNDLE GENERATE` returns a bundle id the operator can never retrieve, and reports success
while doing so. On a `.deb` install this is the *only* outcome available.

### 8. Every test on this feature asserts nothing

- `integration_debug_http.rs` — 9 bundle tests (`:730, :764, :791, :828, :864, :898, :942, :995,
  :1021`). Each opens with
  `if resp.status() == StatusCode::SERVICE_UNAVAILABLE { server.shutdown().await; return; }`.
  Since §1 makes 503 unconditional, **all nine return before their first assertion, always.**
  Nine green tests, zero coverage.
- `integration_debug_bundle.rs` — 7 RESP tests, each with an
  `Response::Error(e) => assert!(err_str.contains("not enabled") || err_str.contains("ERR"))`
  escape hatch. None inspects archive contents; none opens the zip.
- `frogdb-admin`'s hand-mirrored `BundleInfo` (`client.rs:8-12`) has no test forcing it to match
  the server's `store::BundleInfo`. Two copies of a wire type, no gate.

That is why every defect above survived: the feature's test surface accepts "the feature is off"
as a pass.

## Proposed change

### The module

Add `frogdb-server/crates/debug/src/bundle/assembler.rs`, and make it the only thing outside the
crate that names bundles:

```rust
/// What the node knows about itself. Asked once, at assembly time.
pub trait NodeIdentity: Send + Sync {
    fn identity(&self) -> BundleContext;
}

pub struct BundleContext {
    pub mode: DeploymentMode,   // standalone | cluster — not the role
    pub role: String,           // live, from the LiveMode seam (issue 12)
    pub num_shards: usize,
    pub nodes: Option<Vec<NodeSummary>>,
}

/// A bundle id. The only public constructors validate; there is no
/// `From<String>` and the inner field is private.
pub struct BundleId(String);
impl BundleId {
    pub fn generate() -> Self;                    // the existing timestamp+nonce form
    pub fn parse(s: &str) -> Result<Self, BundleError>;  // rejects anything outside [0-9a-f-]
}

pub enum Window { Instant, Over(Duration) }

pub struct BundleAssembler { /* limits, senders, tracer, identity, store */ }

impl BundleAssembler {
    pub fn new(
        limits: &DebugBundleConfig,
        senders: Arc<Vec<ShardSender>>,
        tracer: Option<SharedTracer>,
        identity: Arc<dyn NodeIdentity>,
    ) -> Result<Self, BundleError>;

    pub async fn assemble(&self, window: Window) -> Result<Assembled, BundleError>;
    pub fn list(&self) -> Vec<BundleInfo>;
    pub fn fetch(&self, id: &BundleId) -> Option<Vec<u8>>;
}
```

Four methods. Behind them sit the shard scatter-gather, the tracer read, the node-identity read,
the zip codec, the filesystem retention sweep and the TTL sweep — that is the **depth**: a small
interface over a large implementation, where today the interface is the implementation, spread
across two crates and copied three times.

`create_zip`'s four copy-paste blocks (`generator.rs:48-60, 62-68, 70-76, 78-84`) become one
`sections() -> impl Iterator<Item = (String, serde_json::Value)>` and a single write loop, so
adding `config.json` or `slowlog.json` later is one line in one place rather than a fifth
copy-paste block — that is the **locality** win, and it is what makes the §4 doc-vs-reality gap
closable at all.

### The seams

- **Config**: `frogdb-debug` gains a dependency on `frogdb-config` (verified acyclic —
  `frogdb-config` is a leaf, and this is the same direction ADR-0001 already sanctions for the
  operator). `BundleConfig` and the five duplicate constants (`bundle/mod.rs:18-57`) delete;
  `DebugBundleConfigExt` (`config/mod.rs:195-211`) deletes. One config type, one default table,
  one source of truth. The duplication cannot recur because there is nothing left to duplicate.
- **Node identity**: `DebugState` already implements everything `NodeIdentity` needs
  (`role()` `:450-452` on the live seam, `server_info.num_shards`, `cluster_overview()`), so it
  implements the trait and both consumers get *the same* `cluster_state.json`. The RESP path's
  empty-cluster-state defect (§0) closes by construction — there is no code path left that can
  build a `DiagnosticData` without a `BundleContext`.
- **Ownership**: one `Arc<BundleAssembler>` built in `subsystems.rs` before both consumers, passed
  to `DebugState` (`:210-222`) and into `ObservabilityDeps` (`:547-557`). Note these sit on
  different construction conditions today — the `DebugState` block is inside
  `if let Some(ref prometheus) = self.prometheus_recorder`, while `ObservabilityDeps` is built
  unconditionally — so the assembler is constructed *above* both.

### The adapters

`debug_handler.rs::bundle_generate` (`:222-257`, 36 lines) and `bundle_list` (`:260-277`,
18 lines) become three lines each over `self.observability.bundle_assembler`. Store failure stops
being a `warn!` and becomes an error reply, so §7 is reported rather than hidden.
`state.rs::generate_bundle_streaming` (`:646-690`) becomes a delegation;
`state.rs::generate_bundle` (`:600-643`) deletes. `handle_api_bundle_download` parses the id
through `BundleId::parse` and 404s on rejection — §5 closes because
`BundleStore::bundle_path` no longer *accepts* a `&str`.

### Safety by construction, not by check

`DebugBundleConfig` gains a `validate()` joined to `Config::validate` (`config/src/lib.rs:306`),
so `max-bundles = 0` is a startup error the operator sees, with the section named — matching how
the other fifteen sections behave. Independently, `BundleAssembler` stores the limit as
`NonZeroUsize`, so `enforce_capacity`'s loop **cannot** be written to spin regardless of what the
config layer does. Two layers, and the inner one is structural.

### Deletion test

Delete `BundleAssembler` and both consumers must immediately re-grow ~120 lines of duplicated
collect→zip→store each, re-invent id validation independently (and one of them will forget, as
one of them does today), and re-acquire node state by two different routes — which is precisely
the state the tree is in. The module earns its place by being the only answer to "what is in a
bundle".

The parts that delete cleanly, with **no replacement**, are the honest measure of the accidental
complexity being removed: `BundleConfig` + 5 consts + `impl Default` (`bundle/mod.rs:18-57`,
40 lines), `DebugBundleConfigExt` + impl (`config/mod.rs:195-211`, 17 lines),
`BundleGenerator::_config` (`generator.rs:13`), `DebugState::generate_bundle`
(`state.rs:600-643`, 44 lines), `bundle_config`/`bundle_store` field pair (`state.rs:346-348`),
`frogctl`'s unused `zip` dependency (`Cargo.toml:41`), and the discarded `_before` scatter
(`collector.rs:133`). ~120 lines, zero behaviour lost.

## Testability improvement

The interface *is* the test surface, and today there isn't one — which is why nine HTTP tests and
seven RESP tests coexist with seven defects.

`BundleAssembler::new(limits, senders, tracer, identity)` takes every dependency as an argument,
so a unit test in `frogdb-debug` constructs one against a `tempfile` directory, a stub
`NodeIdentity`, and the existing shard-harness senders. That makes these testable **without a
server**, for the first time:

- **Retention.** Assemble `max_bundles + 3` bundles, assert exactly `max_bundles` remain and that
  the survivors are the newest. Today `enforce_capacity` has no test at any level.
- **TTL.** `cleanup_expired` compares against filesystem mtimes; a test sets an old mtime
  directly and asserts the sweep. (This is also why `store.rs` is clock-seam-allowlisted — the
  comparison must stay on the OS clock, and the test must too.)
- **`max_bundles = 0`.** Two assertions: `DebugBundleConfig::validate()` rejects it, and
  `BundleAssembler::new` rejects it. The second is the one that survives a future refactor of the
  first.
- **Traversal.** A table test over `BundleId::parse`: `"../../etc/passwd"`, `"a/b"`, `"..%2f.."`,
  `""`, `"list"` all rejected; a generated id accepted. This is a pure-function test with no I/O,
  which is the point of moving validation into the type.
- **Archive contents.** `assemble()` returns bytes; the test opens the zip and asserts the entry
  set and that `cluster_state.json` carries the stub identity's values — the assertion that
  would have caught §0 and that no existing test makes.
- **One answer, two doors.** Because both consumers call the same `assemble()`, a single test
  that the RESP and HTTP archives are structurally identical becomes possible; today they
  provably are not.

At the integration level the change is subtractive: the nine
`if status == SERVICE_UNAVAILABLE { return; }` early-returns
(`integration_debug_http.rs:730,764,791,828,864,898,942,995,1021`) and the seven
`|| err_str.contains("ERR")` escape hatches in `integration_debug_bundle.rs` **delete**. Once
the feature is wired, 503 is a failure, not a skip. Removing an escape hatch from a green test is
the cheapest coverage this lane offers — nine tests go from asserting nothing to asserting their
bodies, with no new test written.

The `frogdb-admin` ↔ server `BundleInfo` duplication gets a round-trip test in the same pass
(serialize the server type, deserialize the client type, compare) — the gate that does not exist
today.

## Risks / scope boundaries

### Spec / LOCKED / mutation gates — none

Verified: **no file in the table above carries an `FM-` tag**, and no row in
`.scratch/hardening/specs/*.md` cites any of them. None of the four locked crate pairs
(`frogdb-txn`+`frogdb-vll`, `frogdb-persistence`+`frogdb-recovery`,
`frogdb-replication`+`frogdb-replication-runtime`, `frogdb-cluster`+`frogdb-cluster-runtime`) is
touched; `frogdb-debug`, `frogdb-config` and `frogdb-server` are all outside the boundary ADRs
0002–0004. **No mutation gate applies and `just mutants-diff` is not required for this change.**
`just lint-failure-modes` is unaffected (no spec rows, no tagged tests).

### Hard constraint: the clock-seam allowlist must move in the same commit

`scripts/clock-seam.py` ALLOWLIST (`:94-107`) pins **exact** OS-clock read counts per file:
`bundle/generator.rs` = 1, `bundle/store.rs` = 1, `bundle/collector.rs` = 2. The gate is
bidirectional — `:251-260` errors on a file that no longer reads the clock ("drop the entry") *and*
on a count that no longer matches. All four reads are legitimately exempt (unique ids, mtime
comparison, forensic wall-clock stamps) and stay exempt, but this change moves `generate_id` into
`BundleId::generate` and merges the collector's two duplicate `SystemTime::now()` reads
(`:110`, `:139`) into one. **Any file move or call consolidation here fails `just lint-gates`
unless the allowlist is edited in the same change**, with the reasons carried over verbatim.
`lint-gates` runs unconditionally on every commit via lefthook, so this is a hard blocker, not a
CI surprise.

### Naming ruling

`.scratch/naming-cleanup/issues/open/08-server-diagnostic-bundle-drift.md` is open and explicitly
lists `crates/debug/src/bundle/*.rs` and `web_ui/handlers.rs` (`<h3>Diagnostic Bundles</h3>`).
This proposal rewrites exactly those files, so it **absorbs the wording sweep for its own file
set** rather than leaving a second pass over the same lines. Note that `DiagnosticCollector` and
`DiagnosticData` are type names, beyond issue 08's "wording only" scope — but they disappear
here anyway, folded into the assembler as private collection steps, so the naming resolves
without a rename sweep. Issue 08 narrows to the files this proposal does not touch
(`core/src/conn_command.rs`, `config/src/debug_bundle.rs` doc comments,
`connection/debug_conn_command.rs`). Note `handlers/debug.rs`, which issue 08 also lists, **does
not exist** — that entry is stale; the file is `connection/debug_handler.rs`.

### Boundary vs proposal 72 (FR2, frogctl config schema)

**No shared files.** 72 owns `frogctl/src/ops/config.rs` and `frogctl/src/commands/config.rs`;
its only contact with this proposal is that its section list (`72:123`) names `debug-bundle`
among the sections it validates. That list is derived from `Config`'s fields, which this proposal
does not change — it only adds a `validate()` impl. If 72 lands first, this proposal's
`Config::validate` line is additive; if this lands first, 72 gains one more section that
actually validates. Either order works with no merge conflict.

### Boundary vs proposal 73 (FR1, frogctl ops wiring)

73 is on disk and explicit (`73:483-488`): it leaves `commands/debug.rs:394` (`debug zip`)
bailing and cedes it, plus the unused `zip` dependency (`Cargo.toml:41`), to this proposal. The
overlap is **one `match` arm in one file plus one dependency line** — a trivially resolvable
conflict in either merge order.

**Ruling on `debug zip`:** the archive is produced *server-side* by the assembler, so `frogctl`
never needs a zip encoder — the `zip` dependency at `Cargo.toml:41` is deleted, not used. The arm
becomes a client over the same HTTP bundle contract `frogdb-admin` already implements
(`ops/frogdb-admin/src/client.rs`). That leaves a real, deliberate duplication:
two binaries with the same bundle client. **This proposal does not resolve it** — folding
`frogdb-admin` into `frogctl debug zip` and dropping `/usr/bin/frogdb-admin` from the `.deb` is a
packaging decision with an operator-visible surface, and it belongs in its own issue. Recorded
here so the follow-up is derivable rather than rediscovered.

Two flags on `DebugCommand::Zip` (`debug.rs:17-29`) have **no server-side support at all**:
`--redact` (nothing in the bundle path redacts anything) and `--nodes` (there is no multi-node
fan-out). Wiring the arm without addressing them would ship a `--redact` that silently does
nothing on an artifact operators hand to third parties. **Ruling: implement `debug zip` for the
single-node case only, and reject `--redact`/`--nodes` with an explicit "not supported" error
rather than accepting and ignoring them.** Redaction is a genuine feature with a real threat
model and gets its own issue.

### Boundary vs proposal 75 (FR4 rendering, FR5 role enum)

75 comes later in the same lane and owns `frogctl`'s rendering path and the client-side role enum
in `info_parser.rs`. This proposal's file set is pinned in the table above: **server-side, plus
exactly one arm and one dependency line in `frogctl`.** It defines a `BundleContext.role` — but
that is a server-side value read off the `LiveMode` seam and serialized into an archive, not a
parsed CLI display type. If 75's role enum ends up in a shared crate, the assembler can adopt it
later as a one-field type change; nothing here blocks that.

### Boundary vs committed proposals 63–70

- **63 (`server-subsystem-bundles`) and 64 (`subsystem-trait-lifecycle`)** both restructure
  `subsystems.rs`, which this proposal edits in two hunks (~6 added lines: build the assembler,
  hand it to `DebugState` and `ObservabilityDeps`). Under 64's `Subsystem` trait the assembler
  becomes a field on the observability subsystem's context instead of a local. **The conflict is
  mechanical and small.** Preferred order: land this first (6 lines, easy to carry) or after 64
  (cleaner home). Both hunks are named in the table so either rebase is derivable.
- **67 (`server-small-dedups`) and 71 (`search-query-plan`)** both cite `debug_handler.rs:173`
  (pubsub limits). This proposal touches `:222-277`. **Disjoint hunks, same file.**
- **68, 69, 70** — no overlap (`69` touches `config-derive` param combinators, not
  `debug_bundle.rs`, whose fields are all `#[param(skip)]`).
- A later FR12-style rewrite of `web_ui` onto an axum `Router` would replace `routes.rs:57-64`
  with a `Path<String>` extractor. `BundleId::parse` survives that unchanged — it moves from the
  hand-rolled match to the extractor, one line either way.

### Behavioural risk

Turning the feature on is the risk. Wiring `with_bundle_support` makes four HTTP endpoints and
one HTML panel live for the first time, and makes nine dormant integration tests actually
execute. Expect first-run failures in those nine — that is the tests finally doing their job, not
a regression. The bundle directory becomes a thing the server writes to at operator request; the
traversal fix (§5) and the retention fix (§6) are what make that safe, which is why they are in
the core and not deferred.

The docs (`diagnostics.mdx:11, 298-308, 322-330`) must be corrected in the same change: either
the promised config/metrics/slowlog sections are added to `sections()`, or the page stops
promising them. Shipping a wired feature that still under-delivers against its own documentation
would trade a dead feature for a misleading one.

## Effort

**M.** One new ~180-line module; six existing files edited in the debug crate; two small hunks
each in `subsystems.rs` and `debug_handler.rs`; two deletions in `config/mod.rs`; one line in
`config/src/lib.rs`; one allowlist edit in `scripts/clock-seam.py`; test escape hatches removed
in two integration files; one docs page. No LOCKED crate, no spec row, no mutation gate. The new
crate dependency (`frogdb-debug` → `frogdb-config`) is the only structural change and it is
acyclic and precedented.

Sequencing within the change: **validation and id-typing land before the wiring**, so §5 and §6
are never armed by an intermediate commit.

## Independently-landable hotfixes

Each is confirmed LIVE with an end-to-end trace, is independent of the assembler, and can land
alone.

**H1 — `enforce_capacity` cannot spin (`store.rs:115-122`).** Change `>=` to `>` plus a
`bundles.pop()`-drives-the-loop form, or take the limit as `NonZeroUsize`. **~3 lines.** Trace:
today unreachable because no caller passes a non-default config, so this is pre-emptive — but it
is the cheapest possible removal of a server hang, and it must precede any config wiring. Pair
with a `DebugBundleConfig::validate()` rejecting `max_bundles == 0` joined at
`config/src/lib.rs:306` (**~10 lines**), which gives the operator the error at startup.

**H2 — validate the bundle id before it reaches the filesystem (`routes.rs:57-64`,
`store.rs:89-91`).** Reject any id outside `[0-9a-f-]` and 404. **~6 lines.** Trace: reachable
the instant §1 is fixed, and by anyone holding the debug bearer token. Landing this *before* the
wiring is the ordering that matters. Independent of everything else here.

**H3 — stop swallowing bundle-store failure (`debug_handler.rs:246-250`).** Return
`Response::error(...)` instead of `tracing::warn!` + success. **~4 lines.** Trace: on every
`.deb` install today, `DEBUG BUNDLE GENERATE` returns an id for an archive that was never
written, because `ProtectSystem=strict` + relative `directory` makes the write fail
(`frogdb-server.service:26,28` — no `WorkingDirectory`; `frogdb.toml:187`
`directory = "frogdb-data/bundles"`). The operator gets a plausible-looking id and an empty
result from `DEBUG BUNDLE LIST`. This is the highest operator-confusion-per-line fix in the set.

**H4 — make the shipped default directory writable.** Extend the deb generator's FHS overrides
(the same mechanism that rewrites `persistence.data-dir` → `/var/lib/frogdb/data` and
`snapshot.snapshot-dir` → `/var/lib/frogdb/snapshots`) to emit
`directory = "/var/lib/frogdb/bundles"`. **~2 lines in the generator**, regenerate
`ops/deploy/deb/frogdb.toml`. Trace: same as H3, from the other end. Land H3 and H4 together —
H3 alone converts a silent failure into a loud one, which is an improvement but still a broken
feature; H4 alone hides that the error handling is wrong. **Note `ops/deploy/deb/frogdb.toml` is
generated (`# GENERATED FILE`, source `crates/config/`, `just deb-gen`) — edit the generator, not
the TOML.**

**H5 — delete `DebugState::generate_bundle` (`state.rs:600-643`) and `BundleGenerator::_config`
(`generator.rs:13`).** 45 dead lines, zero callers, verified by grep across the workspace.
**Pure deletion.** No clock-seam impact (`generate_bundle` reads no clock).

**Not a hotfix:** the config wiring (§2) and the web wiring (§1). Both are one-line changes that
*look* like hotfixes and are not — §2 arms H1's spin bug and §1 arms H2's traversal. They land
with the assembler, after H1 and H2.
