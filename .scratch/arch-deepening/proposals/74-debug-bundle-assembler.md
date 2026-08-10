# Proposal 74 — Debug Bundle: one `BundleAssembler`, and turn the feature on

Round 38 · lane: frogctl / operator / telemetry · effort **M** · candidate FR3 · no shared files
with 72, 75 or 76; one ceded `match` arm + one dependency line shared with 73; three
`subsystems.rs` hunks coordinated with 63/64

**Revised at HEAD `4c36827d` per adversarial review `@df68147e`** (verdict AMEND).
Originally verified at `04486569`. Every commit in `df68147e..4c36827d` touches only
`.scratch/arch-deepening/proposals/*.md` — `git diff --stat df68147e..HEAD -- ':!*.md'` is
**empty**, so no source file cited below moved between review and revision. Every path, line
number and count in this document was re-derived by reading the tree at `4c36827d`.

**One lane-brief claim is wrong and is corrected against a standing ruling** (FR3(d) "role
frozen" — see §Problem 0). Three brief claims are confirmed LIVE. **Seven defects the brief did
not name were found**, two of them latent security/availability bugs that any wiring change
activates.

### What the review changed

| Review item | Disposition |
|---|---|
| **B1** `NodeIdentity` seam circular + built inside the HTTP gate | **Accepted.** §Seams rewritten: identity is built from the same live inputs `ServerDebugProvider` uses, **above** the `prometheus_recorder` gate. See §Proposed change → *Node identity*. |
| **B2** `frogctl/Cargo.toml:41` is `sha2`, not `zip` | **Accepted.** `zip` is `:40`; file is **48** lines. Corrected in three places. Independently confirmed by `75:73` and `75:331`. |
| **B3** RESP tests mischaracterized; prescription would delete real assertions | **Accepted.** 8 tests, not 7; only **three** genuine feature-off escapes. §8 and §Testability retargeted at the two `if let`-wrapper tests. |
| **B4** no boundary section for 76 | **Accepted with refutation.** Section added — but 76 at HEAD declares both `routes.rs` and `subsystems.rs` **"Read-only, must NOT be edited"**, so there is no collision. See §Boundary vs proposal 76. |
| **Security** "bearer-token protected" is wrong | **Accepted.** §5 note rewritten: bearer auth is opt-in and **off by default**; severity is *unauthenticated* arbitrary-`.zip`-read in the default config. |
| Delete tally ~120 | **Accepted.** Recounted line-by-line: **107**. Stated as ~110. |
| `BundleInfo.id` typing, `list()` filter semantics | **Accepted.** Spelled out in §The module. |
| H1 `>=` → `>` variant | **Accepted (struck).** It is an off-by-one behaviour change; `NonZeroUsize` only. |
| `create_zip` `{id}/` entry prefix | **Accepted.** Archive test asserts the prefixed entry set. |
| `generate_bundle_streaming` doc lie; `diagnostics.mdx:313` `.tar.gz` | **Accepted.** Folded into §3 and §Behavioural risk. |
| `_before` at `:133` → `:132`; `subsystems.rs:85-96` → `:92-96`; 71-framing; `frogdb-admin` "no test" | **Accepted.** All four corrected. |
| Label 75/76 as "unverifiable at writing" | **Refuted.** Both proposals are on disk at `4c36827d` (`b2912487`, `ec777993`) and were read directly; forward references are cited to their text, not hedged. |

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
`BundleId` newtype whose only public constructors validate the character set closes the path
traversal — plus the two unescaped id sinks in the download handler — structurally rather than
with a check.

Net: ~110 lines delete outright (107 counted line-by-line in §Deletion test), ~200 lines of
duplicated orchestration collapse to one, and the
feature becomes reachable — which is the point. The leverage is unusual: **one wiring line lights
up four HTTP endpoints, two RESP subcommands, one web panel and an entire shipped binary.**

## Files involved

| File | Lines | Role in this change |
|---|---|---|
| `frogdb-server/crates/debug/src/bundle/mod.rs` | 57 | **the change.** `DEFAULT_*` consts `:18-30`, `BundleConfig` `:34-45`, `impl Default` `:47-57` — all delete. Becomes module decl + re-exports only |
| `frogdb-server/crates/debug/src/bundle/assembler.rs` | *new (~180)* | **the change.** `BundleAssembler`, `BundleId`, `NodeIdentity`, `BundleContext`, `Window`, `BundleError` |
| `frogdb-server/crates/debug/src/bundle/generator.rs` | 89 | **the change.** `_config` field `:13` deletes; `create_zip` `:37-88` (4 copy-paste blocks) → one `sections()` loop; `generate_id` `:23-34` moves behind `BundleId::generate` |
| `frogdb-server/crates/debug/src/bundle/store.rs` | 123 | **the change.** `bundle_path` `:89-91` takes `&BundleId`; `enforce_capacity` `:115-122` spin bug fixed by `NonZeroUsize` |
| `frogdb-server/crates/debug/src/bundle/collector.rs` | 192 | **the change.** `DiagnosticData` `:13-23` gains the node context; `cluster_state: ClusterStateJson::default()` `:118`/`:147` deletes; dead `_before` snapshot `:132` resolved |
| `frogdb-server/crates/debug/src/web_ui/state.rs` | 976 | **the change.** `bundle_store`/`bundle_config`/`shared_tracer` fields `:346-350` → one `Option<Arc<BundleAssembler>>`; `with_bundle_support` `:433-442`; `bundle_enabled` `:579-581`; `generate_bundle` `:600-643` **deletes** (zero callers); `generate_bundle_streaming` `:646-690` → 3-line adapter |
| `frogdb-server/crates/debug/src/web_ui/handlers.rs` | 1605 | **the change (4 hunks).** `handle_api_bundle_list` `:1394-1408`, `handle_api_bundle_generate` `:1414-1454`, `handle_api_bundle_download` `:1457-1490` (gains `BundleId::parse`; also the two unescaped id sinks — `Content-Disposition` `:1470-1477` and the hand-built JSON error body `:1484-1487`), `handle_partial_bundles` `:1505+`. Also `<h3>Diagnostic Bundles</h3>` → naming ruling |
| `frogdb-server/crates/debug/src/web_ui/routes.rs` | 208 | **the change (1 hunk).** unvalidated id extraction `:57-64` |
| `frogdb-server/crates/debug/Cargo.toml` | — | **the change.** adds `frogdb-config` dep (verified acyclic) |
| `frogdb-server/crates/server/src/connection/debug_handler.rs` | 374 | **the change (2 hunks).** `bundle_generate` `:222-257` (hardcoded default at `:225`,`:240`,`:246`), `bundle_list` `:260-277` (`:261`). Both → adapters |
| `frogdb-server/crates/server/src/server/subsystems.rs` | 930 | **the change (3 hunks).** (a) build the identity + assembler in the derived-collaborator block, **above** the `if let Some(ref prometheus)` gate at `:171` — beside `LiveMode::new` `:92-96`; (b) `DebugState` `:210-222` gains `.with_bundle_support(...)`; (c) `ObservabilityDeps` `:547-557` gains the same `Arc`. `.with_debug_state(debug_state)` `:264` is **not** edited |
| `frogdb-server/crates/server/src/config/mod.rs` | 650 | **the change.** `DebugBundleConfigExt` `:195-199` + impl `:201-211` **delete** (zero call sites) |
| `frogdb-server/crates/config/src/debug_bundle.rs` | 91 | **the change.** gains `validate()`; the 5 `DEFAULT_*` consts `:8-20` become the single source |
| `frogdb-server/crates/config/src/lib.rs` | 479 | **the change (1 line).** `Config::validate` `:306` gains `self.debug_bundle.validate()?` |
| `scripts/clock-seam.py` | 276 | **the change (mandatory).** ALLOWLIST `:94-107` pins exact counts for `generator.rs` (1), `store.rs` (1), `collector.rs` (2) — bidirectionally enforced |
| `frogdb-server/crates/server/tests/integration_debug_http.rs` | 1069 | **the change.** 9 bundle tests `:730,:764,:791,:828,:864,:898,:942,:995,:1021`, 9 matching 503 guards `:741,:775,:802,:839,:875,:909,:962,:1006,:1032` — every test currently a no-op |
| `frogdb-server/crates/server/tests/integration_debug_bundle.rs` | 284 | **the change (3 hunks, not a blanket sweep).** **8** RESP tests `:13,:44,:73,:101,:130,:163,:207,:260`. Only `:28,:61,:89` are feature-off escapes; `:113,:142,:270` are real error assertions and **stay**. The two no-op tests are `:163-203` (`if let` wrapper + literal empty `if found {}` `:192-196`) and `:207-251` (same wrapper) |
| `website/src/content/docs/operations/diagnostics.mdx` | 340 | **the change.** `:11`, `:298-308`, `:322-330` describe contents that are never collected |
| `frogdb-server/ops/frogdb-admin/src/main.rs` | 130 | read-only evidence. Only command family is `DebugBundle {Generate,List,Download}` |
| `frogdb-server/ops/frogdb-admin/src/client.rs` | 169 | read-only evidence. `BundleInfo` `:8-12` hand-mirrors the server type; every method has `503 => bail!("Bundle support not enabled on the server.")`. It **does** have a test (`:143`, `test_deserialize_bundle_info`) — but against a hand-written JSON literal, so nothing forces it to match the server type |
| `frogctl/src/commands/debug.rs` | 770 | **the change (1 arm).** `DebugCommand::Zip` `:17-29`, dispatch `:393-395` bails — ceded to this proposal by 73 |
| `frogctl/Cargo.toml` | **48** | **the change (1 line).** `zip.workspace = true` **`:40`** — zero uses; stays zero. Delete. **`:41` is `sha2.workspace = true`, which `ops/backup.rs` needs (`:127,:183,:340,:370,:398,:453`) — do not touch it.** Confirmed independently by `75:73` and `75:331` |
| `frogdb-server/crates/config/src/http.rs` | 91 | read-only evidence (security). `token: Option<String>` `:35`; `default_http_enabled() -> true` `:38`; `Default` `:55`/`:58` = enabled + no token; `validate` warns only when a token **is** set on `0.0.0.0` `:83-85` |
| `frogdb-server/ops/deb/deb-gen/src/main.rs` | — | **the change (H4).** `production_config` `:212`; the FHS override block `:215-217`; `DATA_DIR` const `:40`. Add `BUNDLE_DIR` + one assignment |
| `frogdb-server/ops/deploy/deb/nfpm.yaml` | — | read-only evidence (H4). `/var/lib/frogdb` is already created as a `dir` entry `:62` |
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
`subsystems.rs:92-96` and read per-call by `DebugStatusProvider::replication`
(`debug_providers.rs:106-127`, `self.mode.current()` / `self.mode.is_replica()`).
`DebugState::role()` (`state.rs:450-452`) delegates to that provider. Role is **live**. Per the
brief's own instruction, the ruling wins and is cited here.

(Issue 12's file is still filed under `issues/open/` with `Status: ready-for-human`, i.e. the
issue has not been formally closed. That does not weaken the ruling: the *code* is the evidence —
`LiveMode` exists at `status.rs:426-434`, is constructed at `subsystems.rs:92-96`, and
`debug_providers.rs:106-127` reads it per call. The stale filing state is noted so a reader who
checks `issues/open/` is not misled into re-adopting the brief's claim.)

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

The surviving copy also **lies in its own doc comment**. `state.rs:645`:

```rust
/// Generate a bundle and return it without storing (for streaming).
pub async fn generate_bundle_streaming(
```

It stores. `state.rs:685-687`:

```rust
if let Some(ref store) = self.bundle_store {
    let _ = store.store(&id, &zip_data);
}
```

That is the third documentation defect in this feature (after §4's website claim and §Behavioural
risk's `.tar.gz`), and it is the cheapest evidence for the thesis: three hand-copies of one
sequence drift not only in behaviour but in what each copy *claims* to do. One `assemble()` has
one doc comment.

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

`collect_with_duration` also throws away half its work: `:132`
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

`get()` (`:68-71`) then `fs::read`s that path. This is unexploitable **today only because the 503
guard fires first** — which means the §1 fix arms it. Any change that wires the store and does not
also fix this is a net security regression. That is why the id validation is part of this
proposal's core and not a follow-up.

**Severity: unauthenticated arbitrary-`.zip`-read in the default configuration.** An earlier draft
of this proposal claimed "bearer-token protected, so the severity is *authenticated*". That was
**wrong**, and the corrected reading is what makes the sequencing (§Effort, H2) load-bearing:

| Fact | Evidence |
|---|---|
| Bearer auth is **opt-in and off by default** | `HttpConfig.token: Option<String>` (`config/src/http.rs:35`); `Default` sets `token: None` (`:58`). `bearer_auth_middleware` (`observability_server.rs:256-271`) only compares when a token is configured |
| The HTTP surface is **on by default** | `default_http_enabled() -> true` (`http.rs:38`), used by `Default` at `:55` |
| The **shipped `.deb` enables HTTP tokenless** | `ops/deploy/deb/frogdb.toml:58-61` — `[http] enabled = true`, `bind = "127.0.0.1"`, `port = 9090`, **no `token` key** |
| `validate()` **permits tokenless `0.0.0.0`** | `http.rs:83-85` warns only in the *inverse* case (token set **and** bind `0.0.0.0`). A tokenless `0.0.0.0` bind passes validation silently — a supported, remotely-reachable configuration |

So after the §1 wiring, `GET /debug/api/bundle/<id>` is reachable **with no credential** on a
default build, and remotely on a supported one.

The reach is also wider than "up-tree". `Path::join` **replaces** the base when the joined segment
is absolute, so `/api/bundle//etc/backup` yields `/etc/backup.zip` outright — the reachable set is
*any `.zip` the server user can read anywhere on the box*, not merely paths above the bundle
directory. The `.zip` suffix is the only remaining constraint, and it is appended by
`bundle_path`, not checked.

**Adjacent sink, closed by the same newtype.** The id is not only a path segment; it is
interpolated unescaped into two response surfaces in `handlers.rs` —
`format!("attachment; filename=\"{}\"", filename)` (`:1470-1477`) and a hand-built JSON error body
`format!(r#"{{"error":"Bundle '{}' not found"}}"#, id)` (`:1484-1487`). Constraining the id to
`[0-9a-f-]` at the type boundary closes header injection and JSON-body injection at the same time
as the traversal, which is the argument for a newtype over a check at the filesystem call.

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

**The `>=` is not the bug and must not be "fixed" to `>`.** `store()` (`:74-86`) runs
`enforce_capacity()` *before* `fs::write`, so `>=` correctly drains to `max_bundles - 1` and the
pending write brings the directory back to exactly `max_bundles`. Relaxing to `>` would retain
`max_bundles` and then write, leaving `max_bundles + 1` — a silent retention off-by-one traded for
a hang fix. The condition is right; the *type* is wrong. `NonZeroUsize` (or, equivalently, a
`while !bundles.is_empty() && bundles.len() >= limit` guard) removes the hang without touching
retention semantics.

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
  :1021`) and exactly 9 matching guards (`:741, :775, :802, :839, :875, :909, :962, :1006,
  :1032`), each of the form
  `if resp.status() == StatusCode::SERVICE_UNAVAILABLE { server.shutdown().await; return; }`.
  Since §1 makes 503 unconditional, **all nine return before their first assertion, always.**
  Nine green tests, zero coverage. (A tenth `SERVICE_UNAVAILABLE` at `:726` is a comment.)
- `integration_debug_bundle.rs` — **8** RESP tests (`:13, :44, :73, :101, :130, :163, :207,
  :260`), no-op by two different mechanisms, and the two must be treated separately:

  | Mechanism | Sites | Disposition |
  |---|---|---|
  | Feature-off escape: `contains("not enabled") \|\| contains("ERR")` | `:28`, `:61`, `:89` | **Delete the escape.** Once wired, an error reply is a failure |
  | Real error assertions that happen to use `contains("ERR")` | `:113` (`DURATION` missing value), `:142` (`DURATION` invalid), `:270` (unknown subcommand) | **Keep verbatim.** These assert the parser rejects bad input; they are not escapes and deleting them removes the only argument-validation coverage this command family has |
  | `if let Response::Bulk(..) = generate_response { … }` wrapper — the whole body is skipped when generation fails | `:163-203`, `:207-251` | **Unwrap the `if let` into an assertion.** `:192-196` is the extreme case: a literal empty `if found { /* Bundle was found in the list */ }` under the comment "don't fail if not found" — a test that computes an answer and discards it |

  No test in the file opens the zip or inspects archive contents.
- `frogdb-admin`'s hand-mirrored `BundleInfo` (`client.rs:8-12`) *is* tested (`:143`,
  `test_deserialize_bundle_info`) — but against a hand-written JSON literal
  (`{"id":"abc-123","created_at":…,"size_bytes":…}`), so **no test forces it to match the server's
  `store::BundleInfo`**. Two copies of a wire type, and the test asserts against a third
  hand-transcribed copy. Changing the server type breaks nothing until an operator runs the binary.

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
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
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

**Two consequences of the newtype that must be designed, not discovered:**

- **`BundleInfo.id` becomes `BundleId`.** `BundleInfo` is serialized onto the wire (the
  `/api/bundle/list` JSON that `frogdb-admin`'s client deserializes, `client.rs:8-12`), so
  `BundleId` needs `Serialize` — and `#[serde(transparent)]` keeps the wire bytes byte-identical to
  today's `String`, so the hand-mirrored client type keeps working unchanged. The round-trip test
  below is what pins that.
- **`BundleStore::list` must *filter*, not assume.** `list()` (`store.rs:41-60`) builds each id
  from `path.file_stem()` (`:47`) over whatever `.zip` files are in the directory — an operator or
  a stray process can drop `notes.zip` in there. There is no `BundleId::from_unchecked`; `list()`
  uses `BundleId::parse(&stem).ok()?` inside the existing `filter_map`, so unparseable files are
  simply not listed. This is not merely defensive: `enforce_capacity` feeds `oldest.id` straight
  back into `bundle_path` (`:119`), so a filter here is also what keeps the retention sweep from
  ever constructing a path it did not validate.

`create_zip`'s four copy-paste blocks (`generator.rs:48-60, 62-68, 70-76, 78-84`) become one
`sections() -> impl Iterator<Item = (String, serde_json::Value)>` and a single write loop, so
adding `config.json` or `slowlog.json` later is one line in one place rather than a fifth
copy-paste block — that is the **locality** win, and it is what makes the §4 doc-vs-reality gap
closable at all. **The loop must keep the `{id}/` entry prefix**: all four current entries are
written as `format!("{id}/metadata.json")` and so on (`generator.rs:55, 63, 71, 79`), so the
archive contains a single top-level directory. `sections()` yields the bare names and the loop
applies the prefix once — which is also the only place the prefix can be forgotten, versus four
today.

### The seams

- **Config**: `frogdb-debug` gains a dependency on `frogdb-config` (verified acyclic —
  `frogdb-config` is a leaf, and this is the same direction ADR-0001 already sanctions for the
  operator). `BundleConfig` and the five duplicate constants (`bundle/mod.rs:18-57`) delete;
  `DebugBundleConfigExt` (`config/mod.rs:195-211`) deletes. One config type, one default table,
  one source of truth. The duplication cannot recur because there is nothing left to duplicate.
- **Node identity — built from the live inputs, not from `DebugState`.** An earlier draft had
  `DebugState` implement `NodeIdentity` and then passed the assembler *into* `DebugState`. That is
  **circular** (an `Arc` cycle between the two, and a chicken-and-egg at construction) and it is
  also *unavailable*: `DebugState` is built inside `if let Some(ref prometheus) =
  self.prometheus_recorder` (`subsystems.rs:171`, the block at `:210-222`), while
  `ObservabilityDeps` — the RESP consumer's route to the assembler — is built **unconditionally**
  (`:547-557`). With `http.enabled = false` there is no `DebugState`, so a `DebugState`-derived
  identity would leave the RESP path with no identity at all, destroying the "one answer, two
  doors" property this proposal exists to establish. `ServerDebugProvider` (`:198-208`) is inside
  the same gate and fails for the same reason.

  **The fix is small and stays in-tree**: build the identity from the same live inputs
  `ServerDebugProvider` itself is constructed from, in the derived-collaborator block *above* the
  gate —

  ```rust
  // subsystems.rs, beside `let mode = LiveMode::new(...)` at :92-96 — ungated.
  let bundle_identity: Arc<dyn NodeIdentity> = Arc::new(NodeIdentitySource {
      mode: mode.clone(),                          // :92-96, the live role seam (issue 12)
      cluster_state: self.cluster_state.clone(),   // deployment mode + node table
      role_manager: self.role_manager_handle.clone(),
      num_shards: self.shard_senders.len(),
  });
  let bundle_assembler = Arc::new(BundleAssembler::new(
      &self.config.debug_bundle,
      self.shard_senders.clone(),
      self.shared_tracer.clone(),
      bundle_identity,
  )?);
  ```

  These are the *same four values* the provider reads (`debug_providers.rs:106-127` reads
  `self.mode.current()`; the provider's constructor at `:198-208` takes `self.cluster_state`,
  `mode`, `self.role_manager_handle`), so identity is live and identical on both doors, and no
  accuracy is lost relative to the `DebugState` route. `DebugState` becomes a pure *consumer* of
  the assembler and implements nothing. The RESP path's empty-cluster-state defect (§0) still
  closes by construction — there is no code path left that can build a `DiagnosticData` without a
  `BundleContext`.

- **Ownership**: one `Arc<BundleAssembler>`, cloned into `DebugState` (`:210-222`, inside the gate)
  and into `ObservabilityDeps` (`:547-557`, unconditional). Because construction moves above the
  gate, this is a **third** hunk in `subsystems.rs`, not two — and that third site is inside the
  `:78-168` derived-collaborator block that proposal 64 relocates wholesale into
  `SubsystemContext::build` (`64:84`, `64:394`, `64:445-447`). See §Boundary vs committed
  proposals 63–70 for what that costs on each merge order.

### The adapters

`debug_handler.rs::bundle_generate` (`:222-257`, 36 lines) and `bundle_list` (`:260-277`,
18 lines) become three lines each over `self.observability.bundle_assembler`. Store failure stops
being a `warn!` and becomes an error reply, so §7 is reported rather than hidden.
`state.rs::generate_bundle_streaming` (`:646-690`) becomes a delegation — and its doc comment
(`:645`, "return it without storing", contradicted by `:685-687`) goes with the body it lied
about; `state.rs::generate_bundle` (`:600-643`) deletes. `handle_api_bundle_download` parses the id
through `BundleId::parse` and 404s on rejection — §5 closes because
`BundleStore::bundle_path` no longer *accepts* a `&str`.

### Safety by construction, not by check

`DebugBundleConfig` gains a `validate()` joined to `Config::validate` (`config/src/lib.rs:306`),
so `max-bundles = 0` is a startup error the operator sees, with the section named — matching how
the other fifteen sections behave. Independently, `BundleAssembler` stores the limit as
`NonZeroUsize`, so `enforce_capacity`'s loop **cannot** be written to spin regardless of what the
config layer does. Two layers, and the inner one is structural.

### Deletion test

Delete `BundleAssembler` and both consumers must immediately re-grow ~110 lines of duplicated
collect→zip→store each, re-invent id validation independently (and one of them will forget, as
one of them does today), and re-acquire node state by two different routes — which is precisely
the state the tree is in. The module earns its place by being the only answer to "what is in a
bundle".

The parts that delete cleanly, with **no replacement**, are the honest measure of the accidental
complexity being removed. Counted line-by-line rather than estimated:

| Deletion | Site | Lines |
|---|---|---|
| `BundleConfig` + 5 `DEFAULT_*` consts + `impl Default` | `bundle/mod.rs:18-57` | 40 |
| `DebugState::generate_bundle` (zero callers) | `state.rs:600-643` | 44 |
| `DebugBundleConfigExt` + impl (zero call sites) | `config/mod.rs:195-211` | 17 |
| `bundle_config`/`bundle_store` field pair | `state.rs:346-348` | 3 |
| `BundleGenerator::_config` (never read) | `generator.rs:13` | 1 |
| Discarded `_before` scatter | `collector.rs:132` | 1 |
| `frogctl`'s unused `zip` dependency | `frogctl/Cargo.toml:40` | 1 |
| | **Total** | **107** |

**~110 lines, zero behaviour lost.** (An earlier draft said ~120; the recount is above so the
number is checkable rather than asserted.)

## Testability improvement

The interface *is* the test surface, and today there isn't one — which is why nine HTTP tests and
eight RESP tests coexist with seven defects.

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
- **Traversal.** A table test over `BundleId::parse`: `"../../etc/passwd"`, `"a/b"`,
  `"/etc/backup"` (the absolute-segment case — `Path::join` *replaces* the base), `"..%2f.."`,
  `""`, `"list"` all rejected; a generated id accepted. This is a pure-function test with no I/O,
  which is the point of moving validation into the type.
- **Non-bundle files in the bundle directory.** Write `notes.zip` beside two real bundles; assert
  `list()` returns two entries and that `enforce_capacity` never targets `notes.zip`. This pins the
  `BundleId::parse(...).ok()?` *filter* semantics against a future "just use `from_unchecked`,
  we already own the directory" simplification.
- **Archive contents.** `assemble()` returns bytes; the test opens the zip and asserts the entry
  set — note the entries are **`{id}`-prefixed** today (`generator.rs:55, 63, 71, 79`:
  `{id}/metadata.json`, `{id}/shard_memory.json`, `{id}/traces.json`, `{id}/cluster_state.json`),
  so the assertion is on the prefixed names, and the prefix becomes part of the format contract
  the `sections()` loop must preserve — and that `cluster_state.json` carries the stub identity's
  values, the assertion that would have caught §0 and that no existing test makes.
- **One answer, two doors.** Because both consumers call the same `assemble()`, a single test
  that the RESP and HTTP archives are structurally identical becomes possible; today they
  provably are not.

At the integration level the change is largely subtractive, but **it is not a blanket sweep of
`contains("ERR")`** — that would delete real coverage. Three targeted edits:

1. **HTTP — delete all nine guards.** The nine
   `if resp.status() == StatusCode::SERVICE_UNAVAILABLE { server.shutdown().await; return; }`
   early-returns at `integration_debug_http.rs:741, 775, 802, 839, 875, 909, 962, 1006, 1032`
   (bodies at `:730, 764, 791, 828, 864, 898, 942, 995, 1021`) delete outright. Once the feature is
   wired, 503 is a failure, not a skip. Nine tests go from asserting nothing to asserting their
   bodies, with no new test written — the cheapest coverage this lane offers.
2. **RESP — delete exactly three escapes.** `integration_debug_bundle.rs:28`, `:61`, `:89` are
   `contains("not enabled") || contains("ERR")` feature-off hatches and delete. **`:113`, `:142`
   and `:270` must not be touched**: they assert that `DEBUG BUNDLE GENERATE DURATION` rejects a
   missing value, that it rejects an invalid value, and that an unknown subcommand errors. They
   read like escapes and are the file's only argument-validation coverage.
3. **RESP — unwrap the two silent tests.** `test_debug_bundle_list_after_generate` (`:163-203`)
   and `test_debug_bundle_list_entry_structure` (`:207-251`) wrap their entire bodies in
   `if let Response::Bulk(..) = generate_response`, so every assertion inside is skipped whenever
   generation fails — which, per §1/§7, is *always* on a `.deb` install and often elsewhere. The
   `if let` becomes an unconditional destructure with an assertion, and the literal no-op at
   `:192-196` —

   ```rust
   // Log for debugging but don't fail if not found
   // (RESP GENERATE might not store bundles, just return them)
   if found {
       // Bundle was found in the list
   }
   ```

   becomes `assert!(found, ...)`. That single line is the §1↔§2 defect written down as a test that
   declined to assert it: the comment's uncertainty about "whether RESP GENERATE stores" is exactly
   the question the assembler answers once.

The `frogdb-admin` ↔ server `BundleInfo` duplication gets a round-trip test in the same pass
(serialize the server type, deserialize the client type, compare) — the gate that does not exist
today, and the one that pins `#[serde(transparent)]` on `BundleId` so the newtype stays
wire-compatible with the hand-mirrored client.

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
bailing and cedes it, plus the unused `zip` dependency (`Cargo.toml:40`), to this proposal. The
overlap is **one `match` arm in one file plus one dependency line** — a trivially resolvable
conflict in either merge order.

**Line-number correction (`frogctl/Cargo.toml`).** An earlier draft of this proposal cited the
`zip` dependency at `:41` in three places. That is wrong and it is a *dangerous* wrong: the file is
**48 lines**, `zip.workspace = true` is **`:40`**, and **`:41` is `sha2.workspace = true`**, which
`frogctl/src/ops/backup.rs` uses at `:127`, `:183`, `:340`, `:370`, `:398` and `:453`. An
implementer deleting "line 41" as written would break the build. The off-by-one was inherited from
proposal 73's citation; 75 independently reads it correctly (`75:73` — *"`zip` `:40` — zero uses
in the crate (74 owns `zip`)"* — and `75:331`). The `zip` deletion itself is correct: zero uses in
`frogctl/src/`, verified by grep at `4c36827d`.

**Ruling on `debug zip`:** the archive is produced *server-side* by the assembler, so `frogctl`
never needs a zip encoder — the `zip` dependency at `Cargo.toml:40` is deleted, not used. The arm
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

75 is **on disk at `4c36827d`** (`b2912487`) and was read directly for this revision — the forward
reference is verified, not hedged. It owns `frogctl`'s rendering path and the client-side role
enum in `info_parser.rs`, and its own file table confirms the split from this side: `75:58`
records `info_parser.rs` as *"Explicitly disclaimed by 74 (`74:519-521`)"*, `75:73` records
`zip` `:40` as *"74 owns `zip`"*, and `75:619` records `debug.rs:394` as *"by 74 (the zip arm)"*.
This proposal's file set is pinned in the table above: **server-side, plus
exactly one arm and one dependency line in `frogctl`.** It defines a `BundleContext.role` — but
that is a server-side value read off the `LiveMode` seam and serialized into an archive, not a
parsed CLI display type. If 75's role enum ends up in a shared crate, the assembler can adopt it
later as a one-field type change; nothing here blocks that.

### Boundary vs proposal 76 (FR6/FR11, observability HTTP extractors) — no collision, verified

76 is the sibling whose scope comes closest to this proposal's **only** security hunk
(`routes.rs:57-64`, H2). Read at `4c36827d` (`ec777993`), it does not collide, and it says so
itself:

| Contact point | 76's own text | Consequence for 74 |
|---|---|---|
| `debug/src/web_ui/routes.rs` | file table `76:65`: *"**Read-only, must NOT be edited.** The private `not_found` twin `:146-151`. Owned by **74** + future 79."* | 74 keeps sole ownership of `:57-64`. **No conflict.** 76 deletes only the *telemetry* `not_found` copy (`http_handlers.rs:84-90`) |
| `server/subsystems.rs` | file table `76:62`: *"**Read-only evidence, must NOT be edited.** … Owned by **63/64/74**."* | 74's three hunks are unopposed by 76 |
| `observability_server.rs` | 76's primary file | **74 does not touch it.** `DebugState` reaches the router through `subsystems.rs:264` (`.with_debug_state(debug_state)`), which 74 leaves alone — the assembler rides inside `DebugState`, so no router signature changes |
| Boundary section on 76's side | `76:481-489`: *"74 edits `routes.rs` (1 hunk, `:57-64`) … **76 edits none of these.** … No merge conflict is possible."* | Symmetric and consistent with this section |

**Refutation recorded.** The review asked for this section on the premise that 76 "converts the
hand-rolled match to extractors" and therefore hard-collides with H2. At `4c36827d` **it does
not** — 76 explicitly stays out of routing topology and out of the debug crate. The extractor
conversion belongs to **future proposal 79** (FR12), per `76:491-497`.

**Ruling for whenever the conversion does happen (79):** `BundleId::parse` **moves into the
extractor unchanged**. The `TryFrom<&str>`-shaped validation is exactly what a `Path<BundleId>`
extractor needs, so the conversion is a relocation of one call, not a re-implementation, and the
`[0-9a-f-]` constraint is never briefly absent during the rewrite. This is the property that makes
H2 worth landing as a `&str` check *now* even though the check is later discarded: the *check* is
throwaway, the *type* is not.

### Boundary vs committed proposals 63–70

- **63 (`server-subsystem-bundles`) and 64 (`subsystem-trait-lifecycle`)** both restructure
  `subsystems.rs`, which this proposal edits in **three** hunks (~10 added lines: build identity +
  assembler above the gate; hand the `Arc` to `DebugState`; hand it to `ObservabilityDeps`).
  - 63 touches `subsystems.rs` **mechanically only** — field-path retargeting, `63:70`, `63:386`.
    All three of this proposal's hunks survive it as a rename of the `self.x` receivers.
  - 64 is the sharper edge, and it lands on the **new third hunk specifically**. 64 relocates the
    derived-collaborator block `:78-168` — which is where `LiveMode::new` `:92-96` lives and
    therefore where the identity and assembler are constructed — wholesale into
    `SubsystemContext::build` (`64:84`, `64:394`, `64:428-447`). Post-64 the assembler is a field
    on `SubsystemContext` rather than a local, and the `DebugState`/`ObservabilityDeps` hunks read
    it from `ctx` instead of from scope. 64's own T4 test (`64:593`) already asserts `Arc::ptr_eq`
    identity between the hot-shard collector reached via `ObservabilityDeps` and via `DebugState`
    — **the assembler is a fourth row for that same test**, which is an argument for 64-then-74.
  - **The conflict is mechanical and small in either order.** Preferred: after 64 (cleaner home,
    and the ptr-eq test comes free); acceptable: before (three small hunks, easy to carry).
- **67 (`server-small-dedups`)** owns `debug_handler.rs:173/:178` (the hard-coded 5 s `SearchMsg`
  pubsub-limits timeout) and explicitly parks it as an out-of-scope follow-up issue (`67:300`,
  `67:724`, `67:729`). **71 (`search-query-plan`) does not claim it** — `71:563` merely disclaims
  it, pointing back at 67 (*"Not claimed here. 67 named it an out-of-scope follow-up"*). An earlier
  draft framed this as "67 and 71 both cite `:173`", which overstated 71's involvement. This
  proposal touches `:222-277`. **Disjoint hunks, same file, and the `:173` follow-up stays 67's.**
- **68, 69, 70** — no overlap (`69` touches `config-derive` param combinators, not
  `debug_bundle.rs`, whose fields are all `#[param(skip)]`).

### Behavioural risk

Turning the feature on is the risk. Wiring `with_bundle_support` makes four HTTP endpoints and
one HTML panel live for the first time, and makes nine dormant integration tests actually
execute. Expect first-run failures in those nine — that is the tests finally doing their job, not
a regression. The bundle directory becomes a thing the server writes to at operator request; the
traversal fix (§5) and the retention fix (§6) are what make that safe, which is why they are in
the core and not deferred.

The docs (`diagnostics.mdx:11, 298-308, 313, 322-330`) must be corrected in the same change.
There are **two** defects on that page, not one:

- `:298-308` promises "server state, configuration, metrics, slowlog entries, and recent traces";
  the archive holds four files and none of the last three (§4). Either the promised sections are
  added to `sections()`, or the page stops promising them.
- `:313` tells the operator to run
  `curl … /debug/api/bundle/generate -o bundle.tar.gz` — **the artifact is a ZIP**
  (`generator.rs` writes a `zip::ZipWriter`; the handler sets `Content-Type: application/zip` and
  a `.zip` filename, `handlers.rs:1470-1477`). A one-word fix, but it is the fourth documentation
  error found in this feature (after `:298-308`, `state.rs:645`'s "without storing", and the
  `[debug-bundle]` block that configures nothing), and it is the one an operator hits first.

Shipping a wired feature that still under-delivers against its own documentation would trade a
dead feature for a misleading one.

## Effort

**M.** One new ~180-line module; six existing files edited in the debug crate; **three** small
hunks in `subsystems.rs` (the third — identity + assembler construction above the
`prometheus_recorder` gate — is the B1 fix) and two in `debug_handler.rs`; two deletions in
`config/mod.rs`; one line in `config/src/lib.rs`; one allowlist edit in `scripts/clock-seam.py`;
three targeted test edits across two integration files (nine HTTP guards, three RESP escapes, two
RESP `if let` wrappers — **not** a blanket `contains("ERR")` sweep); one docs page with two
corrections. No LOCKED crate, no spec row, no mutation gate. The new crate dependency
(`frogdb-debug` → `frogdb-config`) is the only structural change and it is acyclic and
precedented.

Sequencing within the change: **validation and id-typing land before the wiring**, so §5 and §6
are never armed by an intermediate commit. This is not a stylistic preference — per the corrected
§5, the window between "store is wired" and "id is validated" is an *unauthenticated* arbitrary-
file-read on a default build.

## Independently-landable hotfixes

Each is confirmed LIVE with an end-to-end trace, is independent of the assembler, and can land
alone. All five were upheld by review; H1 and H4 are amended below.

**H1 — `enforce_capacity` cannot spin (`store.rs:115-122`).** Take the limit as `NonZeroUsize`
(equivalently: add a `!bundles.is_empty()` guard to the loop condition). **~3 lines.** Pair with a
`DebugBundleConfig::validate()` rejecting `max_bundles == 0` joined at `config/src/lib.rs:306`
(**~10 lines**), which gives the operator the error at startup. Trace: today unreachable because
no caller passes a non-default config, so this is pre-emptive — but it is the cheapest possible
removal of a server hang, and it must precede any config wiring.

> **Amended:** an earlier draft also offered "change `>=` to `>`" as an alternative. **Struck.**
> `enforce_capacity` runs *before* `fs::write` inside `store()` (`:78-85`), so `>=` is what makes
> the post-write count equal `max_bundles`; `>` would leave `max_bundles + 1`. That is a silent
> retention off-by-one masquerading as a hang fix. The two remaining variants are equivalent and
> behaviour-preserving; `NonZeroUsize` is preferred because it is the structural one.

**H2 — validate the bundle id before it reaches the filesystem (`routes.rs:57-64`,
`store.rs:89-91`).** Reject any id outside `[0-9a-f-]` and 404. **~6 lines.** Trace: reachable
the instant §1 is fixed, **by anyone who can reach the HTTP port — no credential required**, since
bearer auth is opt-in and off by default (§5). Landing this *before* the wiring is the ordering
that matters, and it is the load-bearing part of this hotfix set.

> **On throwaway-vs-migrated:** H2 lands as a `&str` check at the two sites above, because that is
> what can ship today without the assembler. When `BundleId` arrives it **replaces** the check —
> the check is discarded, not migrated, and the `[0-9a-f-]` predicate moves into
> `BundleId::parse`. That is deliberate: six discarded lines are the price of not leaving the
> traversal armed between commits, and the character-set predicate is carried over verbatim so the
> two implementations cannot disagree.

**H3 — stop swallowing bundle-store failure (`debug_handler.rs:246-250`).** Return
`Response::error(...)` instead of `tracing::warn!` + success. **~4 lines.** Trace: on every
`.deb` install today, `DEBUG BUNDLE GENERATE` returns an id for an archive that was never
written, because `ProtectSystem=strict` + relative `directory` makes the write fail
(`frogdb-server.service:26,28` — no `WorkingDirectory`; `frogdb.toml:187`
`directory = "frogdb-data/bundles"`). The operator gets a plausible-looking id and an empty
result from `DEBUG BUNDLE LIST`. This is the highest operator-confusion-per-line fix in the set.
The behaviour change is **operator-visible only on the already-broken path** — where the store
write fails, success is currently reported for an archive that does not exist. Tests are
unaffected: they run under a writable CWD, so `store()` succeeds and the new error arm is never
taken.

**H4 — make the shipped default directory writable.** **Located precisely:**
`frogdb-server/ops/deb/deb-gen/src/main.rs`, `fn production_config()` at `:212`, whose FHS
override block at `:215-217` already rewrites `persistence.data_dir`, `snapshot.snapshot_dir` and
`cluster.data_dir` from consts declared beside `DATA_DIR` at `:40`. The change is **two lines** — a
`const BUNDLE_DIR: &str = "/var/lib/frogdb/bundles";` beside the others, and
`config.debug_bundle.directory = BUNDLE_DIR.into();` in the block. `.into()` fits because
`DebugBundleConfig::directory` is a `String` (`config/src/debug_bundle.rs:36`), not a `PathBuf`.
Then regenerate `ops/deploy/deb/frogdb.toml`.

  **No packaging change is needed**, which is what keeps this a hotfix: `/var/lib/frogdb` is
  already in the unit's `ReadWritePaths` (`frogdb-server.service:28`) and is already created with
  the right ownership by `nfpm.yaml:62`, and `BundleStore::new` does `fs::create_dir_all`
  (`store.rs:30`), so the `bundles/` subdirectory is made at runtime — no postinstall script edit.

  Land H3 and H4 **together**: H3 alone converts a silent failure into a loud one on every `.deb`
  install (silence → noise), and H4 alone fixes the path while leaving the error-swallowing
  untested and unexercised. **Note `ops/deploy/deb/frogdb.toml` is generated (`# GENERATED FILE`,
  source `crates/config/`, `just deb-gen`) — edit the generator, not the TOML.**

**H5 — delete `DebugState::generate_bundle` (`state.rs:600-643`) and `BundleGenerator::_config`
(`generator.rs:13`).** 45 dead lines, zero callers, **re-verified by grep across the workspace at
`4c36827d`**. **Pure deletion.** No clock-seam impact (`generate_bundle` reads no clock, so the
allowlist counts in `scripts/clock-seam.py:94-107` are unchanged and H5 does not need the
same-commit allowlist edit that the full change does).

**Not a hotfix:** the config wiring (§2) and the web wiring (§1). Both are genuinely one-line
changes that *look* like hotfixes and are not — §2 arms H1's spin bug and §1 arms H2's traversal
(now known to be *unauthenticated*, §5). They land with the assembler, after H1 and H2. "One line"
is a statement about diff size, not about blast radius.
