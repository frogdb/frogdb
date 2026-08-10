# Proposal 79 — The debug web UI's route table: one string match, three content contracts, one 404

Round 38 · lane: frogctl / operator / telemetry · candidate **FR12** · effort **M** ·
**no locked crate**, **zero FM tags**, **no seam-lint allowlist edit owed**

**Verified at HEAD `2e81506b70ae7f294f74604e7da215d24d83a0f0`** (worktree `arch-round-38-99`,
branch `main`). The lane brief was written against `08c143d6`; sibling proposal 76 was verified at
`4372082285`. `git diff 4372082285..HEAD -- frogdb-server/crates/debug
frogdb-server/crates/server/src/observability_server.rs` is **empty** — no source drift between
76's verification point and this one; the only commits since are proposal files. Every path, line
number and count below was re-derived by reading the tree at this SHA.

**Revision 2 (adversarial review `54baa2bb`, verdict AMEND).** Three blocking items were upheld and
are fixed in place: the `nest` trailing-slash claim was **inverted** (§Proposed change now uses
`nest_service`), the percent-decoding table's "Today" column was **false at HEAD** (§Risks now
states it as post-74-§1), and the signature inventory was **missing a fourth parameter kind**
(§Handler signatures now rules `RawQuery`). Line cites were re-derived a second time against the
working tree; the corrections are itemised in **§Review ledger** at the end, together with one
refuted review-adjacent claim of this proposal's own (the `"master"`/`"slave"` vocabulary evidence,
now deleted). All cites into *sibling proposals* (74, 76) are marked **re-verify at land time** —
74 was revised after this proposal first quoted it, and one quote did not survive.

**Brief claims corrected:**

| Brief claim (`lane-frogctl-operator-telemetry.md:16`) | Correction |
|---|---|
| "routes.rs **~40-arm** string match" | **31 arms** (`routes.rs:42-111`), carrying **32 distinct route patterns** (two arms use `\|` alternates) and dispatching to **28 distinct `handlers::handle_*` functions** plus 3 file-local ones. |
| "own 404" | True, and it is worse than "own": **one** `text/plain` 404 (`:146-151`) serves **three different content contracts** — HTML pages, a *documented public JSON API* (`diagnostics.mdx:279-296`), and HTMX HTML partials. A `curl`ing operator who typos a JSON-API path gets `Content-Type: text/plain` and the body `Not Found`. |
| "**Latent**" | **Half wrong.** One **LIVE** defect was found that the brief did not name: `GET /debug/api/cluster/node/<non-numeric>` — a documented endpoint (`diagnostics.mdx:283`) — returns **HTTP 200 OK** carrying `{"error":"Invalid node ID"}`. Same class as 76's H1, different file, independently reachable. |
| "axum Router (dep already present)" | **Present in the wrong crate.** `frogdb-server/crates/server` has `axum.workspace = true` (`Cargo.toml:157`); **`frogdb-debug` has no axum dependency at all** (`debug/Cargo.toml:11-23`). That constraint — the mirror image of the one that shapes 76 — is the central design decision of this proposal, not a detail. |

Two further findings the brief did not name, both of which change what this proposal may do:

- **A hard ordering constraint on proposal 74.** axum 0.8.8's `Path` extractor **percent-decodes**
  captured segments (`axum-0.8.8/src/extract/path/mod.rs:9,166` — `PercentDecodedStr`). Today
  `routes.rs` reads `uri.path()` **raw**, so `%2e%2e%2f` never becomes `../`. Moving to `Path<String>`
  therefore **widens 74's §5 traversal vector from literal-`..` to also-percent-encoded-`..`**. 79
  must not land before 74's `BundleId::parse`.
- **17 of the 32 route patterns are exercised by nothing in the default test suite.** The browser
  suite that would reach seven of them is `#![cfg(feature = "browser-tests")]`
  (`browser-tests/tests/debug_ui.rs:6`), off by default, and self-skips without chromedriver
  (`:20-30`).

## Summary

`web_ui/routes.rs` is a **routing engine written by hand, in a process that already links a
routing engine**. `handle_debug_request` (`:30-112`) takes a `&Uri`, strips a literal prefix,
and runs a 31-arm `match` over `&str` with four `starts_with` + `strip_prefix(...).unwrap_or("")`
pairs standing in for path parameters. The outer `axum::Router` in the server crate reaches it
through three routes and one wrapper (`observability_server.rs:233-235`, `:293-303`) — so the
request has already been routed once, by matchit, before this second, weaker router runs.

The **leverage** is not the 82 lines of `match`. It is that the hand-rolled router has **no place
to put a decision that varies per route family**, and there are three such decisions already:

1. **The 404 encoding.** One `not_found()` (`:146-151`) → `text/plain` for a JSON API, an HTML
   page tree, and an HTMX partial tree. There is no seam at which `/debug/api/*` could 404 as
   JSON, so it doesn't.
2. **The method.** Every one of the 32 patterns accepts exactly the methods the *outer* router
   grants (`get`, `:233-235`) — including `/api/bundle/generate`, which runs a scatter-gather
   across every shard and writes a zip to disk. Side effects on `GET`, with no per-route place to
   say otherwise.
3. **Path-parameter validation.** Four patterns capture a segment; each does it differently
   (`strip_prefix(...).unwrap_or("")`), and each validates — or fails to validate — in its own
   handler. 74's §5 traversal and this proposal's H1 are both instances.

Converting to an `axum::Router` built **inside `frogdb-debug`** and `nest`ed by the server gives
each of those three decisions a per-family home: three nested routers (`/` + `/assets`, `/api`,
`/partials`), three `.fallback()`s, per-route `MethodRouter`s, and `Path<T>` extractors that
validate at the boundary instead of inside handler bodies.

The **depth** argument is the honest one and it constrains the design: converting the routing
without changing the handler signatures would trade 31 match arms for **32 three-line async
adapters plus a Router** — strictly worse, and the same trap 76 correctly refused for its four
telemetry pass-throughs. So this proposal changes the 28 `pub fn handle_*` signatures in
`handlers.rs` to be axum handlers directly. **If a reviewer rules that `handlers.rs` must keep its
`&DebugState` shape, 79 should not land.** That is the deletion test and it is stated up front.

## Files involved

| Path | Lines | Role in this change |
|---|---|---|
| `frogdb-server/crates/debug/src/web_ui/routes.rs` | 208 | **Primary.** `handle_debug_request` `:30-112` (**deleted** — the 31-arm match); `serve_index` `:115-124` and `serve_asset` `:127-143` (**kept**, become handlers); `not_found` `:146-151` (**deleted**, replaced by three per-family fallbacks); `Assets` `:15-17`; tests `:154-208` (kept, 4 tests). 6 commits — **every one a feature add** (`7ee5bf19`, `6caddb2d`, `cc865961`, `84dd4861`, `cad54a5a`, `7ba151f0`): the file has never been refactored, only extended. |
| `frogdb-server/crates/debug/src/web_ui/router.rs` | *new (~110)* | **The change.** `WebUiState { debug: Arc<DebugState>, recorder: Arc<PrometheusRecorder> }`; `pub fn router() -> Router<WebUiState>`; three nested routers with three fallbacks. |
| `frogdb-server/crates/debug/src/web_ui/handlers.rs` | 1605 | **Primary (signature-only).** All **28 `pub fn handle_*`** change receiver `&DebugState` → `State(s): State<WebUiState>`; the 18 sync ones gain `async`; the 4 that also take `&Arc<PrometheusRecorder>` (`handle_api_metrics :136`, `handle_partial_metrics :1058`, `handle_partial_metrics_charts :1213`, `handle_partial_overview :1093`) read it off `WebUiState` instead; the 3 that take a `&str` id (`handle_api_bundle_download :1457`, `handle_api_cluster_node :635`, `handle_partial_cluster_node :677`) gain `Path<String>`; and **one that takes `query: Option<&str>`** (`handle_api_bundle_generate :1414-1417`) gains `RawQuery` — the **fourth** parameter kind, ruled in §Handler signatures. **Bodies unchanged** except H1. 74 owns 4 body hunks here — see §Boundaries. |
| `frogdb-server/crates/debug/src/web_ui/mod.rs` | 30 | **The change.** `pub mod routes;` `:22` and `pub use routes::handle_debug_request;` `:25` → `pub mod router; pub use router::{router, WebUiState};`. Also **`:17`** "Chota CSS" — **stale**, the shipped asset is `css/simple.min.css` (commit `cad54a5a` "Simple.css"); fixed in passing. |
| `frogdb-server/crates/debug/Cargo.toml` | 23 | **The change (2 lines, not 1).** Gains `axum.workspace = true` and a `[dev-dependencies] tower = { workspace = true, features = ["util"] }` — see §Risks/`ServiceExt`. Today: `hyper`, `http-body-util`, `bytes`, `rust-embed`, `mime_guess`, `zip` — **no axum, no tower**. |
| `frogdb-server/crates/server/src/observability_server.rs` | 363 | **The change (2 hunks).** `:233-235` (three `.route("/debug…", get(debug_handler))`) → one **`.nest_service("/debug", …)`** (not `.nest` — see the mount-form ruling in §Proposed change); `debug_handler` `:293-303` (**deleted**). **Also 76's primary file** — see §Boundaries for the exact hunk partition. |
| `frogdb-server/crates/server/src/server/subsystems.rs` | 930 | **Read-only evidence, must NOT be edited.** `with_debug_state(debug_state)` `:264` — **unconditional**, the proof that `/debug/*` is live on every node with `http.enabled`. Owned by 63 / 64 / 74. |
| `frogdb-server/crates/config/src/http.rs` | — | Read-only. `pub token: Option<String>` **`:35`** (doc `:31-32`, `#[serde(default)]` `:33`, `#[param(skip)]` `:34`) → **`None` by default**: the debug surface is unauthenticated out of the box. `default_http_enabled() -> true` **`:38-40`**. |
| `frogdb-server/crates/server/tests/integration_debug_http.rs` | 1069 | **The change (additive).** 34 tests, **34 `TestServer::start_standalone()` boots** — one server per assertion. Route coverage census in §Problem 4. |
| `frogdb-server/crates/browser-tests/tests/debug_ui.rs` | 385 | Read-only evidence. `#![cfg(feature = "browser-tests")]` `:6`; chromedriver self-skip `:20-30`. **Not in the default suite.** |
| `frogdb-server/crates/debug/src/web_ui/state.rs` | 976 | **Read-only, must NOT be edited.** Owned by 74 (7 hunks). `clock::now()` `:67` is the crate's only clock read outside `bundle/`. |
| `website/src/content/docs/operations/diagnostics.mdx` | 340 | **The change (`:279-296` only).** The documented JSON API table `:281-290`; **`:296` "Routes are defined in `frogdb-server/crates/debug/src/web_ui/routes.rs`"** — a source cite that must move with the file. 74 owns `:11`, `:298-308`, `:313`, `:322-330` — **disjoint line ranges**. |
| `frogdb-server/crates/debug/assets/index.html`, `assets/js/charts.js` | — | Read-only evidence. The frontend fetches only 9 of the 32 patterns; census in §Problem 4. |

## Problem

### 1. A second router, weaker than the first, behind the first

The request is routed twice. `observability_server.rs:232-235` registers

```rust
.route("/debug", get(debug_handler))
.route("/debug/", get(debug_handler))
.route("/debug/{*path}", get(debug_handler))
```

on an `axum::Router` — matchit does the real work — and `debug_handler` (`:293-303`) then hands
the **whole `Uri`** to `frogdb_debug::web_ui::handle_debug_request`, which throws away the routing
that just happened and starts over:

```rust
let path = full_path.strip_prefix("/debug").unwrap_or(full_path);   // routes.rs:39
let path = if path.is_empty() { "/" } else { path };                // :40
match path { … }                                                     // :42-111
```

`unwrap_or(full_path)` is unreachable: the only three routes that reach here all begin `/debug`.
It is a defensive branch guarding against a caller that the type system already excludes — the
signature `(&Uri, &DebugState, &Arc<PrometheusRecorder>)` cannot express "already prefix-matched",
so the function re-derives it.

The 31 arms (`:42-111`) carry 32 patterns, of which four are prefix captures done by hand:

| Line | Pattern | Capture |
|---|---|---|
| `:47-50` | `/assets/…` | `p.strip_prefix("/assets/").unwrap_or("")` |
| `:57-64` | `/api/bundle/…` | `p.strip_prefix("/api/bundle/").unwrap_or("")` — **74's hunk** |
| `:68-71` | `/api/cluster/node/…` | `p.strip_prefix("/api/cluster/node/").unwrap_or("")` |
| `:88-91` | `/partials/cluster/node/…` | `p.strip_prefix("/partials/cluster/node/").unwrap_or("")` |

Four spellings of `Path<String>`, each capturing *the rest of the path including slashes* (which
is `{*wildcard}` semantics, not `{param}` semantics — the difference is exactly 74's traversal).

### 2. Arm order is load-bearing, and the file cannot tell you where

Two artefacts prove that the author could not reason about the match as a whole:

**A stale ordering comment for a hazard that does not exist.** `:66` reads

```rust
// Cluster API endpoints (must be before /api/cluster catch)
```

but the arm it is guarding against (`:74`) is `"/api/server" | "/api/cluster"` — an **exact**
match. Reordering `:67-71` below `:74` changes nothing. The comment documents a hazard that was
real when `:74` was a prefix arm and is now false; nothing in the language or the lints can
detect that it went stale.

**A dead guard for a hazard that is real.** `:57-63`:

```rust
p if p.starts_with("/api/bundle/") => {
    let id = p.strip_prefix("/api/bundle/").unwrap_or("");
    if !id.is_empty() && id != "list" && id != "generate" {
```

`"/api/bundle/list"` and `"/api/bundle/generate"` are matched at `:53` and `:54`, **above** this
arm. So by the time `:59` runs, `id != "list" && id != "generate"` is **unconditionally true** —
two dead comparisons written because the author (correctly) did not trust the arm order but
(incorrectly) guarded it at the wrong place. `!id.is_empty()` is live (`/api/bundle/` → 404).
This hunk is **74's** (`routes.rs:57-64`); recorded here as evidence, the fix is 74's.

A `Router` has no arm order. `/api/bundle/list` and `/api/bundle/{id}` are distinct nodes in
matchit's tree; static segments beat parameters by construction, and there is nothing to comment
about.

### 3. LIVE — a documented endpoint returns 200 OK carrying an error

`handle_api_cluster_node` (`handlers.rs:635-641`):

```rust
let node_id: u64 = match node_id_str.parse() {
    Ok(id) => id,
    Err(_) => {
        return json_response(&serde_json::json!({"error": "Invalid node ID"}));
    }
};
```

`json_response` (`handlers.rs:15-31`) hard-codes `StatusCode::OK` at **`:18`**. So
`GET /debug/api/cluster/node/abc` → **200 OK**, `application/json`, `{"error":"Invalid node ID"}`.
The *not-found* case two lines below (**`:644-649`**) correctly returns 404; only the *malformed*
case is wrong.

**Liveness traced end to end:** `subsystems.rs:264` calls `.with_debug_state(debug_state)`
**unconditionally** (inside `if let Some(ref prometheus) = self.prometheus_recorder`, itself gated
only on `http.enabled`, which defaults **true** — `config/src/http.rs:38-40`) →
`observability_server.rs:137` sets `debug_state: self.debug_state.map(Arc::new)` → `:235` registers
`/debug/{*path}` → `:294` `if let Some(ref state) = s.debug_state` is taken → `routes.rs:68-71`
→ `handlers.rs:635`. No feature flag, no config gate. `http.token` defaults to `None`
(`config/src/http.rs:35`), so on a default build **no credential is required**. The endpoint is
documented at `diagnostics.mdx:283`. **LIVE.**

The same defect reaches the encoded case for free: `uri.path()` is **not** percent-decoded, so
`/debug/api/cluster/node/%31` (a valid encoding of `1`) also parses-fails → 200 + error body.

Nothing catches this because `/debug/api/cluster/node/:id` has **zero tests** (§4).

### 4. One 404 for three content contracts — and 17 patterns nothing exercises

`not_found()` (`:146-151`) is the terminal for the wildcard arm `:110` and for three inner
failures (`:62`, `:122`, `:141`). It emits `Content-Type: text/plain`, body `Not Found`.

But `/debug/api/*` is a **documented public JSON API** — `diagnostics.mdx:273-290` lists eleven
endpoints and shows `curl -H "Authorization: Bearer …" …/debug/api/metrics`. Every success
response is `application/json` (`handlers.rs:15-31`) and every in-handler error is
`application/json` (`:644-649`, `:1396-1402`, `:1481-1488`). Only the router's 404 is not. A client
that does `resp.json()` on any error path gets a *parse* failure instead of a structured one. There
is no seam at which this could differ per family, because there is one function and one caller
chain.

**Coverage census.** Of the 32 patterns:

| Bucket | Count | Patterns |
|---|---|---|
| Covered by `integration_debug_http.rs` | 12 | `/`, `/assets/…`, `/api/cluster`, `/api/config`, `/api/metrics`, `/api/slowlog`, `/api/latency`, `/partials/{cluster,config,metrics,slowlog,latency}` |
| Covered but **inert** | 3 | `/api/bundle/{list,generate,{id}}` — 9 tests, all behind a 503 escape hatch (`:741,775,802,839,875,909,962,1006,1032`); **74 §Files** rules every one "currently a no-op" |
| **Exercised by nothing in the default suite** | **17** | `/index.html`, `/api/server`, `/api/clients`, `/api/shard-stats`, `/api/hot-shards`, `/api/cluster/overview`, `/api/cluster/node/{id}`, `/partials/{node-badge,cluster-tab,cluster/node/{id},overview,performance,metrics-charts,clients,shard-stats,hot-shards,bundles}` |

Seven of those 17 (`node-badge`, `overview`, `performance`, `cluster-tab`, `clients`,
`metrics-charts`, `bundles`) *are* fetched by the shipped frontend (`assets/index.html`,
`assets/js/charts.js`) and would be exercised by `browser-tests/tests/debug_ui.rs` — which is
`#![cfg(feature = "browser-tests")]` (`:6`) and additionally self-skips when chromedriver is
absent (`:20-30`). **Not in `just test`.** Six of the untested ten are documented operator API
(`diagnostics.mdx:281-290`).

The reason is structural, not negligence: **`handle_debug_request` cannot be called without a
`DebugState`, and `DebugState` is built only inside `subsystems.rs:210-222`, after the shard
senders exist.** So every route test is an end-to-end server boot. `integration_debug_http.rs`
runs `TestServer::start_standalone()` **34 times** for 34 tests, and `routes.rs`'s own `mod tests`
(`:154-208`) contains **four** tests, all of `serve_index`/`serve_asset` — **zero tests of the
dispatcher itself**.

### 5. The method is decided once, for all 32 patterns, in another crate

`observability_server.rs:233-235` grants `get` (hence `HEAD`) to the whole subtree.
`handle_debug_request` never sees `Method`. That is adequate for 31 of the 32 patterns and wrong
for one: `/api/bundle/generate` (`routes.rs:54` → `handlers.rs:1414-1454`) calls
`state.generate_bundle_streaming(duration_secs)`, which runs a timed scatter-gather across every
shard and (per 74 §1) is meant to persist a zip. A `GET` that costs a cluster-wide collection and
writes to disk is not idempotent and not safe to prefetch or cache. It is documented as `GET`
(`diagnostics.mdx:306, 313`), so this is a **wire-contract** observation, not a fix this proposal
may make — recorded in §Hotfixes as FILE-only.

### 6. Verified *not* a problem (recorded so a reviewer need not re-check)

- **Asset traversal is safe in release builds, and *nearly* safe in debug builds.** `serve_asset`
  (`:127-143`) passes a raw, slash-bearing, non-decoded string to `Assets::get`. In release builds
  `rust-embed` 8.11 compiles to a **static table** (`rust-embed-impl-8.11.0/src/lib.rs:51` —
  `#[cfg(not(debug_assertions))]`); there is nothing to traverse. In debug builds
  (`lib.rs:150`, `:200`) it reads the filesystem, canonicalizes, and checks containment
  (`lib.rs:168-169`: `canonical_file_path.starts_with(canonical_folder_path)`) — **but it does not
  return `None` on failure**. `lib.rs:172-180` carries an upstream `// TODO: Currently it allows
  "path_traversal_attack" for the symlink files`: when containment fails it takes
  `symlink_metadata` and *continues* if the path is a symlink. So a symlink planted under
  `assets/` escapes containment in debug builds only. Not created by this change and not made
  worse by it in the *literal* form — but the **decoded** form is new, which is why
  `/assets/{*path}` is now a row in §Risks' widening table rather than a clean "not a problem".
- **HTML injection in the cluster-node partial is escaped.** `render_cluster_node_html`
  (`handlers.rs:842-854`) routes the invalid-id string through `html_escape` (`:1375`). The flags
  loop (`:884-899`) escapes too.
- **POST to `/debug` already 405s**, asserted by `integration_debug_http.rs:633-652`. The `Router`
  form preserves *that* assertion by construction — `/` is a `MethodRouter` carrying only `get`, so
  `POST /debug` is still 405 — and gives it per-route granularity. **But it does not preserve 405
  for *unknown* debug paths**: see §Behaviour changes row 6.

## Proposed change

### The crate boundary decision, stated first

`frogdb-debug` has **no axum dependency** (`debug/Cargo.toml:11-23`). There are two ways to build
a `Router` and they are not equivalent:

- **(A) Build it in `frogdb-server`.** Keeps `frogdb-debug` framework-agnostic — the same property
  76 correctly protects for `frogdb-telemetry`. But it puts **32 route→handler mappings two crates
  away from the 28 handlers they name**, so adding a panel means editing two crates, and the
  server crate acquires knowledge of every debug URL. **Locality gets worse, not better.**
- **(B) Add `axum` to `frogdb-debug` and export `web_ui::router()`.** The server does
  `.nest_service("/debug", …)` and knows only the mount point.

**(B) is proposed**, and the reason 76's constraint does not transfer is concrete:
`frogdb-telemetry` is consumed by `frogdb-server`, `frogdb-test-harness` and the operator/CLI
trees; `frogdb-debug` is consumed by **`frogdb-server` only** (verified: `frogdb-debug` appears in
exactly two manifests — the workspace declaration `Cargo.toml:155` and the sole consumer
`server/Cargo.toml:120`), and that binary already links
axum 0.8.8 and tower. The dependency is **new to the crate, not new to the process** — zero new
transitive crates, `axum` and `tower` are already workspace deps (`Cargo.toml:178-179`).

### The router

A new `web_ui/router.rs`:

```rust
/// Everything the debug UI's handlers need. One state type, so no handler
/// takes the recorder as a second positional argument any more.
#[derive(Clone)]
pub struct WebUiState {
    pub debug: Arc<DebugState>,
    pub recorder: Arc<PrometheusRecorder>,
}

pub fn router() -> Router<WebUiState> {
    let api = Router::new()
        .route("/server",  get(handlers::handle_api_server))
        .route("/cluster", get(handlers::handle_api_server))     // documented alias
        .route("/cluster/overview", get(handlers::handle_api_cluster_overview))
        .route("/cluster/node/{id}", get(handlers::handle_api_cluster_node))
        …
        .fallback(json_not_found);          // application/json

    let partials = Router::new()
        …
        .fallback(html_not_found);          // text/html

    Router::new()
        .route("/", get(serve_index))
        .route("/index.html", get(serve_index))
        .route("/assets/{*path}", get(serve_asset))
        .nest("/api", api)
        .nest("/partials", partials)
        .fallback(text_not_found)           // text/plain, today's behaviour
}
```

Three nested routers, **three fallbacks** — the decision that today has one home and needs three.
The `/api` fallback closes §Problem 4's contract break in one line.

The server side collapses to:

```rust
// observability_server.rs — replaces :233-235 and deletes :293-303
let debug = match &state.debug_state {
    Some(ds) => frogdb_debug::web_ui::router()
        .with_state(WebUiState { debug: ds.clone(), recorder: state.recorder.clone() }),
    None => Router::new().fallback(SubsystemUnavailable("debug UI")),   // 76's type
};
//                 ↓ nest_service, NOT nest — see the mount-form ruling below
let protected = Router::new().nest_service("/debug", debug) …
```

#### Mount form: `nest_service`, not `nest` — and why the difference is load-bearing

An earlier revision of this proposal claimed that `.nest("/debug", inner)` where `inner` has
`route("/", …)` matches both `/debug/` and `/debug`. **That is inverted.** In axum 0.8.8:

- `PathRouter::nest` re-registers each inner route at `path_for_nested_route(prefix, inner_path)`
  (`axum-0.8.8/src/routing/path_router.rs:227`), and that function returns the prefix
  **verbatim** when the inner path is `"/"` (`:535-546`, the `else if path == "/" { prefix.into() }`
  arm). So `nest("/debug", route("/"))` registers exactly **`/debug`** and registers
  **nothing at all for `/debug/`**.
- `PathRouter::nest_service` is the opposite shape: it registers `{prefix}/{*rest}` (`:259-263`),
  then the bare `prefix` (`:271-276`), then — because `prefix` does not end in `/` — `{prefix}/`
  (`:277-280`). All three forms match.

Upstream asserts exactly this. `routing/tests/nest.rs:310-343` (`nesting_with_root_inner_router`)
builds both and asserts `GET /service` → 200 and `GET /service/` → 200 for the `nest_service`
mount, against `GET /router` → 200 and **`GET /router/` → `NOT_FOUND`** for the `nest` mount.
`nest.rs:294-307` (`nest_with_and_without_trailing`) asserts the three-form match for
`nest_service`.

**The `nest` form therefore fails CI, and the earlier claim that it would go unnoticed was also
wrong.** `integration_debug_http.rs` covers *both* forms today: `test_debug_index_loads` `:14-39`
requests bare `/debug`, `test_debug_index_trailing_slash` `:41-60` requests `/debug/`. Under
`nest` the second test 404s. This is caught, not shipped — but the design as written was wrong and
the risk row that described the hazard had the polarity backwards: **it is the slashed form that
`nest` drops, not the bare one.**

Either resolution works; `nest_service` is proposed because it is one word:

| Form | `/debug` | `/debug/` | `/debug/x` |
|---|---|---|---|
| `nest("/debug", r)` where `r` has `route("/", …)` | ✅ | ❌ **404** | ✅ |
| **`nest_service("/debug", r)`** ← proposed | ✅ | ✅ | ✅ |
| `nest("/debug", r)` **plus an outer `.route("/debug/", get(serve_index))`** | ✅ | ✅ (by hand) | ✅ |

Note the third row's fix is an **outer** route, not an inner one: under `nest` there is *no*
inner path that maps to `/debug/`, because `path_for_nested_route` prefixes every non-`"/"` inner
path (so inner `"/{*rest}"` → `/debug/{*rest}`, which does not match `/debug/` either) and maps
inner `"/"` to the bare prefix. Restoring the slashed form therefore means re-introducing exactly
the kind of hand-registered sibling route this proposal exists to delete — which is the argument
for `nest_service`.

**Auth ordering survives either construction.** `Router::layer` maps the layer over
`path_router`, `fallback_router` *and* `catch_all_fallback` (`routing/mod.rs:303-317`), so the
bearer layer applied to `protected` after the mount wraps every debug route *and* every debug
fallback. There is no fallback path that escapes the 401.

`debug_handler` (`:293-303`) — the `Uri` argument, the `Option` unwrap, the hand-built
`text/plain` 503 — **deletes entirely**. Note this is the *only* place in `create_router` where
route registration depends on state, which is fine: `create_router` already takes the `HttpState`
by value (`:221`).

### Handler signatures — the part that decides whether this is worth doing

Each of the 28 `pub fn handle_*` becomes an axum handler in place:

```rust
// before  (handlers.rs:635)
pub fn handle_api_cluster_node(state: &DebugState, node_id_str: &str) -> Response<Full<Bytes>>
// after
pub async fn handle_api_cluster_node(
    State(s): State<WebUiState>,
    Path(node_id): Path<u64>,                       // ← parse moves to the boundary
) -> Response<Full<Bytes>>
```

**Four** mechanical classes, not three — the fourth was missed in revision 1 and is ruled here
because the "no handler body changes" claim and the effort estimate both rest on it:

1. **18 sync fns gain `async`** (one keyword; none blocks).
2. **4 fns drop their `&Arc<PrometheusRecorder>` parameter** and read `s.recorder`
   (`handle_api_metrics :136`, `handle_partial_metrics :1058`, `handle_partial_overview :1093`,
   `handle_partial_metrics_charts :1213` — the last already ignores it, `_recorder` at `:1215`).
3. **3 fns replace a `&str` id with `Path<T>`** (`:635`, `:677`, `:1457`).
4. **1 fn takes a query string**: `handle_api_bundle_generate` (`:1414-1417`) has the signature
   `(state: &DebugState, query: Option<&str>)`, fed from `routes.rs:36` (`uri.query()`) and
   consumed by the file-local `parse_query_param` (`:1493-1498`). It is neither a state, a
   recorder, nor a path parameter.

**Ruling for class 4: `RawQuery`, not `Query<T>`.** `axum::extract::RawQuery` yields
`Option<String>`, which is exactly `parse_query_param`'s input; the body stays byte-identical and
`parse_query_param` survives unchanged. `Query<BundleGenerateParams>` would be the more idiomatic
axum shape, but it **changes the handler body** — and that body (`:1414-1454`) sits inside **74's**
hunk. Choosing `RawQuery` is what keeps the 74/79 partition clean (74 owns bodies, 79 owns
signatures) and what makes "no handler body changes" true rather than nearly-true. If a reviewer
prefers `Query<T>`, it is a fine change but it is **74's** to make, after 74 lands, not 79's.

> Also recorded: the `duration` parameter that `parse_query_param` feeds is an unbounded
> `u64` of seconds (`:1429-1431`). That is a security property, not a routing one; it is filed
> under H5 and handed to 74, whose hunk contains the line.

**No handler body changes**, except H1's (which the `Path<u64>` rejection subsumes: axum's
`PathRejection` for a failed `u64` parse is **400**, which is the correct status and is exactly
the fix).

`Response<Full<Bytes>>` needs no adapting: axum 0.8 implements `IntoResponse` for
`http::Response<B> where B: http_body::Body<Data = Bytes>`, and `Full<Bytes>` (http-body-util
0.1, already a `frogdb-debug` dep) qualifies. **Return types are untouched.**

### Two axum facts an implementer will otherwise re-derive

Recorded here so they are not re-litigated at implementation time.

- **`with_state` is generic in its *output* state, not its input.** The signature is
  `pub fn with_state<S2>(self, state: S) -> Router<S2>` (`axum-0.8.8/src/routing/mod.rs:408`).
  That is why `frogdb_debug::web_ui::router()` — a `Router<WebUiState>` — becomes a
  `Router<()>` after `.with_state(…)` and then mounts into the server's `Router<HttpState>`
  without either crate naming the other's state type. Non-obvious, and the whole crate-boundary
  design depends on it.
- **A fallback registers at two paths, and one of them has a hole.** `set_fallback` writes the
  endpoint to `"/"` and to `FALLBACK_PARAM_PATH = "/{*__private__axum_fallback}"`
  (`path_router.rs:33-36`, `mod.rs:111`). Nesting that router at `/api` puts them at `/api`
  (verbatim — the `path_for_nested_route` rule above) and `/api/{*…}`. A wildcard does **not**
  match an empty remainder, so **`/debug/api/` — bare trailing slash — matches neither** and
  falls through to the outer `text_not_found`. The three-fallback design is sound; this one path
  is the exception, and it is a **table-test row**, not a blocker: today `/debug/api/` also
  returns `text/plain` (`routes.rs:110`), so the behaviour is unchanged there and the row exists
  to pin it rather than to fix it.

### Depth, seam, locality

- **Depth.** `router()` is ~110 lines that own three decisions for 32 routes: how each family
  404s, which methods each route accepts, and how each path parameter is validated. Today those
  are, respectively, one function shared by three contracts, one grant in another crate, and four
  ad-hoc `strip_prefix` idioms. The interface it exports is one function returning one type; the
  32-line route table is its *implementation*, not its surface.
- **Seam.** `Path<T>` is a chokepoint for path-parameter validation. After it exists, a new
  captured segment cannot reach a handler body without naming a type — which is what makes 74's
  `BundleId::parse` a *boundary* rule rather than a *handler* rule (74 §Boundary already
  anticipates exactly this: "`BundleId::parse` survives that unchanged — it moves from the
  hand-rolled match to the extractor").
- **Locality.** Adding a panel today means: an arm in `routes.rs`, a handler in `handlers.rs`, and
  a judgement about arm order. After: a `.route()` line adjacent to its siblings in the same crate,
  no ordering judgement. The `.nest_service("/debug", …)` mount point is the only thing the server crate
  learns.
- **Adapter.** The change **removes** an adapter tier rather than moving one: `debug_handler`
  (`observability_server.rs:293-303`) exists solely to bridge axum→`&Uri`, and
  `handle_debug_request` exists solely to bridge `&Uri`→handler. Both go.

### Deletion test, applied honestly

- **`router()` + the three fallbacks** — delete it and the 31-arm match, the four `strip_prefix`
  idioms, the stale ordering comment, the dead `id != "list"` guard, and the single `text/plain`
  404-for-a-JSON-API all reappear, and the two `debug_handler`/`handle_debug_request` bridge
  functions come back with them. **Earns its keep.**
- **`WebUiState`** — delete it and the recorder goes back to being threaded as a positional
  argument through the router into four handlers, i.e. today's shape. It is a two-field struct;
  the justification is that it is the axum `State` type, which must exist for the router to exist
  at all. **Earns its keep, but only as a consequence.** Recorded as such rather than sold as a
  finding.
- **The handler signature change** — this is where the proposal could fail, so it is stated as a
  precondition, not an aside. Keep the `&DebugState` signatures and the change becomes: a Router,
  plus **32 three-line async adapters**, replacing 31 match arms. That is a net *increase* in
  pass-through code with the same routing semantics — precisely the trade 76 §Proposed-change(b)
  refused for its four telemetry wrappers, and it must be refused here too. **The signature change
  is the change; without it, 79 does not land.**
- **`serve_index` / `serve_asset` / `Assets`** — unchanged, still earn their keep (rust-embed is
  not something axum replaces; `tower-http::ServeDir` serves a filesystem, not an embedded table).

## Testability improvement

This is the largest single item in the proposal, and the census in §Problem 4 is why.

**Today:** `handle_debug_request` requires a `&DebugState`, which is constructed at exactly one
place in the tree (`subsystems.rs:210-222`) after shard senders exist. Every route assertion is
therefore an end-to-end boot: **34 tests, 34 `TestServer::start_standalone()` calls**
(`integration_debug_http.rs`). `routes.rs`'s four unit tests (`:154-208`) test only the two
functions that need no state. **The dispatcher has zero unit tests, and 17 of 32 patterns have no
coverage at all in the default suite.**

**After:**

1. `web_ui::router()` returns a `Router<WebUiState>` — drivable with
   `tower::util::ServiceExt::oneshot` (needs tower's `util` feature — see §Risks) against a
   hand-built `WebUiState`. `DebugState::new(server_info, config_entries)` (**`state.rs:364`**) is
   already constructible without shard senders — it sets `shard_senders: None`,
   `bundle_store: None`, `node_state: None`, `hot_shards: None` (`:365-374`); the builders
   (`with_node_state`, `with_shard_senders`, `with_hot_shards`) are optional. So a routing test is
   a struct literal and a `oneshot` — **no listener, no port, no shard workers, no `TestServer`**.
2. The **routing table itself becomes assertable**: one table test over all 32 patterns × expected
   status × expected `Content-Type` catches (a) a route deleted by accident, (b) the JSON-API-404
   contract, (c) method restrictions, and (d) parameter rejection — in one test, at unit speed.
   Today no such test can be written at any speed. Four rows in that table exist specifically to
   pin the axum edge cases this review surfaced: **`/debug` and `/debug/` both 200** (the
   `nest`-vs-`nest_service` trap), **`/debug/api/` bare-trailing-slash is `text/plain`, not JSON**
   (the wildcard-vs-empty-remainder hole), and **`POST /debug/nope` is 404, not 405** (behaviour
   row 6).
3. The **17 uncovered patterns become cheap to cover** without adding 17 server boots. That is the
   point: they are uncovered because covering them costs a boot each, not because anyone decided
   they did not matter — six of them are documented operator API.
4. **`integration_debug_http.rs` need not shrink**, and this proposal does not propose shrinking
   it: end-to-end tests still prove the `nest` mount point, the bearer layer, and TLS. What changes
   is that *new* route coverage no longer has to be paid for at that price.
5. **H1 gets its first test.** Precisely: *after* this refactor it is a unit test. **If H1 lands
   standalone first — which is the recommendation — its first test costs one
   `TestServer::start_standalone()` boot**, because until `router()` exists there is no way to
   reach `handle_api_cluster_node` without a `DebugState`, and no way to build a `DebugState`
   with cluster data outside a booted server. Revision 1 said "as a unit test" unqualified; that
   is true only post-refactor. The boot is worth paying once — it is one test among 34 that
   already boot.

## Spec / LOCKED impact — none

- **Locked crates.** The four locked pairs are `frogdb-txn`+`frogdb-vll`,
  `frogdb-persistence`+`frogdb-recovery`, `frogdb-replication`+`frogdb-replication-runtime`,
  `frogdb-cluster`+`frogdb-cluster-runtime` (ADRs 0002–0004). This proposal edits
  **`frogdb-debug` and `frogdb-server`** only. Neither is locked; no mutation gate, no
  `just mutants-gate` owed. `just mutants-diff frogdb-debug` before push costs little and is
  worth running since `web_ui` has almost no unit coverage today.
- **FM tags.** `grep -rn "FM-"` across the full edited set — `web_ui/routes.rs`,
  `web_ui/handlers.rs`, `web_ui/mod.rs`, `observability_server.rs`,
  `tests/integration_debug_http.rs` — returns **zero matches**. No failure-mode row is forced by
  any of these files; no spec edit, no `just lint-failure-modes` delta.
- **Seam lints** (`Justfile:329`, the fourteen `lint-gates` members):
  - **`lint-clock-seam`** — `scripts/clock-seam.py`'s ALLOWLIST names three `frogdb-debug` files
    and they are all under `bundle/` (`:94` `bundle/generator.rs`, `:98` `bundle/store.rs`,
    `:103` `bundle/collector.rs`). **No `web_ui` file is allowlisted**, and grepping
    `Instant::now|SystemTime::now|clock::` across `web_ui/` returns exactly one hit —
    `state.rs:67 clock::now()`, the approved seam, in a file this proposal does not edit.
    **79 moves no allowlisted file and needs no same-commit allowlist edit.** This is the
    opposite of 74, which does move allowlisted `bundle/` code; recorded so the two are not
    confused at review.
  - **`lint-metrics-chokepoint`** — grepping `increment_counter|record_gauge|record_histogram`
    across the edited set returns zero. The debug UI *reads* the recorder
    (`handlers.rs:136`, `:1058`, **`:1093`**, `:1213` — four sites, not three; `:1093`
    `handle_partial_overview` was omitted in revision 1); it emits nothing. **Unaffected.**
  - **`lint-error-sanitize`** (`scripts/error-sanitize.py`) governs RESP responses in
    `frogdb-server/crates/protocol/src/response.rs`. HTTP bodies are out of its scope.
    **Unaffected.**
  - **`lint-turmoil-features`** — `observability_server.rs`'s `#[cfg(not(feature = "turmoil"))]`
    guards are on TLS only (`:56-60`, `:113`, `:145`, `:169`). Route registration is
    unconditional and this change adds no `cfg`. `frogdb-debug` has no turmoil feature.
    **Unaffected.**
  - The remaining ten (INFO sections, redirects, pub/sub confirmations, failover atomicity, float
    formatting, typed-store unwraps, keyspace-notify routing, the script gate, durable acks,
    figment `.nested()`, continuation locks) have no surface in an HTTP route table.
    **Unaffected.**
- **Vocabulary** (`frogdb-server/CONTEXT.md`). Route paths are a wire contract and none of the 32
  contains a banned term. **Correction to revision 1:** it claimed `handlers.rs` emits
  `"master"`/`"slave"` as JSON *values*. That is **false at HEAD** — `grep '"master"\|"slave"'`
  over `handlers.rs` returns **zero** string literals. What exists is `ServerResponse`'s field
  names `master_host` `:53` / `master_port` `:54` (JSON **keys**, mirrored at `:70-71`) and a
  local `master_html` `:203`; the `role` field is a plain `String` sourced from
  `DebugState::role()` (`state.rs:449-451` → `ReplicationView::role`), whose documented domain is
  `standalone` / `primary` / `replica` / `cluster` (`state.rs:78`, default `:93`) — already
  non-banned. The **ruling is unchanged** — vocabulary is out of scope for 79 and belongs with
  FR5 / proposal 75 — but the evidence offered for it was wrong and is withdrawn; the residual
  surface is the three `master_*` identifiers, not a role value.

## Behaviour changes (wire-visible), stated up front

| # | Change | Before | After | Risk |
|---|---|---|---|---|
| 1 | `/debug/api/*` unknown path | `404` `text/plain` `Not Found` | `404` `application/json` `{"error":…}` | Low. The three 404 tests (`integration_debug_http.rs:576-631`) assert **status only**. Status unchanged. This is the §Problem-4 fix. **Exception:** `/debug/api/` exactly (bare trailing slash) keeps `text/plain` — the wildcard-vs-empty-remainder hole documented in §"Two axum facts". |
| 2 | `/debug/partials/*` unknown path | `404` `text/plain` | `404` `text/html` | Low, same evidence, same `/debug/partials/` exception. |
| 3 | `/debug/api/cluster/node/<non-numeric>` | **`200 OK`** + error body | `400` (axum `PathRejection`) | **This is H1.** Zero tests today. Wire-visible and intended. |
| 4 | `/debug/*` when `debug_state` is `None` | `503` `text/plain` `"Debug UI not enabled"` | `503` via 76's `SubsystemUnavailable` (`application/json`) | **Unreachable in production** — `subsystems.rs:264` is unconditional (76 §Problem 3, re-verified at this HEAD; *re-verify 76's cite at land time*). |
| 5 | Percent-encoded path params | passed through raw (`%31` ≠ `1`) | decoded (`%31` → `1`) | **See §Risks — this is the ordering constraint on 74.** |
| 6 | **Non-`GET` to an *unknown* debug path** (e.g. `POST /debug/nope`, `DELETE /debug/api/nope`) | **`405`** — `/debug/{*path}` is registered `get(...)` only (`observability_server.rs:235`), so matchit matches the route and the `MethodRouter` rejects the method | **`404`** — the request matches no route and lands on a fallback, which axum registers as `any(...)` | Low, and arguably a correction: a path that does not exist should 404 regardless of method. **Unlisted in revision 1.** No test asserts it (`test_debug_method_not_allowed` `:633-652` posts to bare `/debug`, which **stays 405** — `/` is a `get`-only `MethodRouter` under either mount form). Release-note it. |

Rows 1–3 and row 6 belong in the release notes and in `diagnostics.mdx:279-296`, which today
documents no error shape for the JSON API at all.

## Risks / scope boundaries

### Boundary vs proposal 74 (FR3, Debug Bundle) — shared crate, partitioned by hunk; **74 lands first**

> **All line cites into proposal 74 in this section are as 74 stands at this HEAD, and must be
> re-verified at land time.** 74 has already been revised once since this proposal first quoted
> it, and one quote did not survive that revision (§Review ledger, N7).

74 edits `bundle/{assembler,generator,store}.rs`, `web_ui/state.rs`, `web_ui/handlers.rs` (4 body
hunks: `:1394-1408`, `:1414-1454`, `:1457-1490`, `:1505+`), `web_ui/routes.rs` (**1 hunk,
`:57-64`**), `subsystems.rs` (2 hunks), `config/mod.rs`, `connection/debug_handler.rs`,
`config/src/lib.rs:306`, `diagnostics.mdx` (`:11`, `:298-308`, `:313`, `:322-330`).

**File partition:**

| File | 74 owns | 79 owns |
|---|---|---|
| `web_ui/routes.rs` | `:57-64` (BundleId extraction) | the whole file — **`:57-64` included, but only as 74 leaves it** |
| `web_ui/handlers.rs` | 4 **bodies** (`:1394+`, `:1414+`, `:1457+`, `:1505+`) | 28 **signatures**, incl. those 4 |
| `web_ui/state.rs` | 7 hunks | **none** |
| `bundle/**`, `subsystems.rs`, `config/**`, `connection/debug_handler.rs` | all | **none** |
| `diagnostics.mdx` | `:11`, `:298-308`, `:313`, `:322-330` | `:279-296` |
| `observability_server.rs` | none | `:233-235`, `:293-303` |

**Composition with 74's extractor.** 74's revision moves `BundleId::parse` into an axum-style
extractor. Under 79 that becomes literal: the bundle route is
`.route("/bundle/{id}", get(handle_api_bundle_download))` and the handler takes
`Path(id): Path<String>` then `BundleId::parse(&id)?` — or, better, `Path(id): Path<BundleId>` once
`BundleId` implements `FromStr`, at which point the rejection is axum's and the handler body loses
the check entirely. **74's own §Boundary already rules this**, and the ruling is quoted here from
74 as it stands at this HEAD (`74:745-750`, *re-verify at land time* — revision 1 of this
proposal quoted wording that 74 has since replaced):

> **Ruling for whenever the conversion does happen (79):** `BundleId::parse` **moves into the
> extractor unchanged**. The `TryFrom<&str>`-shaped validation is exactly what a `Path<BundleId>`
> extractor needs, so the conversion is a relocation of one call, not a re-implementation, and
> the `[0-9a-f-]` constraint is never briefly absent during the rewrite.

79 adopts that ruling; it does not reopen it. Working assumption confirmed: **74 lands first**, 79
rebases its `:57-64` deletion over 74's version.

**What exactly 79 depends on, stated one way.** The prerequisite is **74's `BundleId` newtype**
(`74:409-410` — `pub struct BundleId(String)` with validating constructors), **not 74's hotfix
H2**. The distinction is load-bearing and revision 1 blurred it. 74's H2 lands a throwaway
`&str` charset check at **two** sites (`74:838` — `routes.rs:57-64` and `store.rs:89-91`); 79
**deletes `routes.rs` wholesale**, so H2's first site dies with the file and only `store.rs:89-91`
survives into a post-79 tree. H2 alone therefore does *not* protect the post-79 route — it
protects the *pre*-79 route, during the window before 79 lands, which is exactly what 74 says it
is for ("six discarded lines are the price of not leaving the traversal armed between commits",
`74:845-847`). The thing that closes the widened, percent-decoded form **after** 79 is the
newtype in the extractor. Both mentions in this proposal (here and in §Risks' widening table) now
say "74's `BundleId` newtype"; wherever an earlier revision said "74's H2 must land before 79",
read: *74 lands first, in full, including the newtype.*

**Hard ordering constraint — 79 must not precede 74's `BundleId` newtype.** This is new and it is
not in either proposal today. `routes.rs:39` reads `uri.path()`, which is the **raw**, un-decoded
request target; axum 0.8.8's `Path` extractor percent-decodes captured segments
(`axum-0.8.8/src/extract/path/mod.rs:9,166` — `PercentDecodedStr`).

**The table below describes the state *after* 74 §1 wires the bundle store, not the state at
HEAD.** Revision 1 stated it as HEAD and that was **false**: at HEAD
`handle_api_bundle_download` returns `503` at `handlers.rs:1458-1466` before it ever calls
`state.get_bundle(id)`, because `bundle_enabled()` is
`self.bundle_store.is_some() && self.shard_senders.is_some()` (`state.rs:579-581`) and **the
store is never wired** — which is precisely 74 §1, and which is why 74's own §5 is titled
*LATENT-armed-by-§1-fix* and why this proposal's own coverage census (§Problem 4) rules all three
bundle routes "currently a no-op". Revision 1 contradicted itself across two sections; the fix is
to relabel the column, not to retract the finding:

| Request | **Post-74-§1**, pre-79 → `bundle_path` | Post-74-§1, post-79 → `bundle_path` |
|---|---|---|
| `/debug/api/bundle/../../x` | `"../../x"` — **traverses** (74 §5: *latent, armed by 74 §1*) | `"../../x"` — traverses |
| `/debug/api/bundle/%2e%2e%2f%2e%2e%2fx` | `"%2e%2e%2f%2e%2e%2fx"` — **inert** | `"../../x"` — **traverses** |

The conclusion **survives, and is strengthened**: 74 §1 is what arms the *literal* form, and 79 is
what adds the *encoded* form on top of it. Neither is a live traversal at HEAD; both are live the
moment the store is wired without the newtype. 79 does not create a new *class* of vector — it
**widens** an existing one, in a change whose stated purpose is routing hygiene.
`BundleId::parse` (which rejects anything outside `[0-9a-f-]`) closes both forms. Recorded here
because it is 79's obligation to state it, not 74's to foresee it.

**A second widening row: `/assets/{*path}`.** Not only the bundle route widens. `serve_asset`
today receives `p.strip_prefix("/assets/")` off the raw `uri.path()` (`routes.rs:47-50`); after
79 it receives a `Path<String>` capture, which is percent-**decoded**. So `%2e%2e%2f` reaches
`Assets::get` as `../` where today it reaches it as the literal seven characters.

| Build mode | Pre-79 (`%2e%2e%2f`) | Post-79 (`%2e%2e%2f` → `../`) |
|---|---|---|
| release | inert (static table; `rust-embed-impl-8.11.0/src/lib.rs:51` `#[cfg(not(debug_assertions))]`) | inert — **no exposure** |
| debug | inert (never decoded) | canonicalize + `starts_with` containment (`lib.rs:168-169`) rejects it — **except** the upstream symlink escape hatch at `lib.rs:172-180`, which *continues* past a failed containment check if `symlink_metadata` says the path is a symlink |

**Ruling: not a blocker, recorded not fixed.** The release binary — the only artefact operators
run — is unaffected in both forms. The debug-build exposure requires an attacker-planted or
developer-planted symlink under `assets/`, i.e. someone who already writes to the source tree.
There is nothing for 79 to do here beyond stating it: the fix, if one is ever wanted, is upstream
in `rust-embed` or a `frogdb`-side reject-on-`..` in `serve_asset`, and neither belongs in a
routing refactor.

### Boundary vs proposal 76 (FR6, observability extractors) — shared file, complementary hunks, **76 first**

76 declares `debug/src/web_ui/routes.rs` **"Read-only, must NOT be edited"** (76 §Files) and
explicitly hands routing topology to 79 (76 §"Boundary vs future proposal 79"): *"76 keeps
entirely out of routing topology: it adds no route, moves no route, and does not touch the debug
crate."* Verified against 76's text at this HEAD — **re-verify at land time**, as with 74. So the
debug crate is 79's alone.

The shared file is `observability_server.rs`. Hunk partition:

| Hunk | 76 | 79 |
|---|---|---|
| `HttpState` `:37-44` | reads; `debug_state` becomes extractor-fed | reads |
| `create_router` `:223-229` (public routes) | untouched | untouched |
| `create_router` `:233-235` (3 × `/debug` routes) | **untouched** (76: "adds no route, moves no route") | **replaced** by one `.nest_service("/debug", …)` |
| `create_router` `:236-245` (7 admin routes) | **rewritten** (delegates directly) | untouched |
| `bearer_auth_middleware` `:256-271` | untouched (H3 filed) | untouched |
| telemetry wrappers `:277-291` | kept, ruled out on the deletion test | untouched |
| `debug_handler` `:293-303` | **shrunk** 11 → 4 lines (loses hand-built 503) | **deleted** |
| admin wrappers `:308-363` | **deleted** | untouched |

**What remains for 79 after 76 lands:** exactly the two hunks above. 76 shrinks `debug_handler` to
a 4-line body that calls 76's extractor; 79 then deletes that 4-line body outright and moves the
`Option<Arc<DebugState>>` gate into `create_router`, where it selects between the nested debug
router and a `Router::new().fallback(SubsystemUnavailable("debug UI"))`. **79 consumes 76's
`SubsystemUnavailable` rather than minting a fourth 503 encoding** — which is precisely the
hand-off 76 asked for. If 79 landed first, 76's debug hunk would simply have nothing to shrink and
76 would lose one of its nine unification sites (falling to eight, all admin/telemetry); its case
is unaffected. **76-then-79 is the cheaper order and the one assumed here.**

`.nest_service("/debug", …)` also inherits the bearer layer unchanged: the layer is applied to the
`protected` router *after* the mount (`:246-249`), and `Router::layer` maps it over
`path_router`, `fallback_router` and `catch_all_fallback` alike (`axum-0.8.8/src/routing/mod.rs:303-317`),
so an unauthenticated request still gets 401 before reaching any debug route **or any debug
fallback**. Preserved by construction under either mount form; worth one of the new `oneshot`
tests.

### Boundary vs proposals 63 / 64 (Server bundles, Subsystem trait) — disjoint

Both edit `server/{mod,init,subsystems}.rs`. **79 edits none of them** — the `with_debug_state`
call chain (`subsystems.rs:258-270`) is unchanged, because `ObservabilityServer`'s builder API is
unchanged. 64 explicitly lists `observability_server.rs` as **"not edited"** (64 §Files:89).
**No ordering constraint either way.**

### Boundary vs proposals 67 / 71 — disjoint file

Both cite `connection/debug_handler.rs:173` — the **RESP `DEBUG` command** handler, a different
module, different protocol, no shared type with `web_ui`. 74 owns two hunks there. **79 touches
it not at all.**

### Boundary vs proposals 72 / 73 / 75 / 77 / 78 (frogctl, operator, test-harness) — verified disjoint by path

`frogctl/` and `frogdb-server/ops/frogdb-admin/` and `frogdb-operator/`: **79 touches no file
under any of them.** 72 cites `observability_server.rs:236-243` as read-only evidence only (72
§:680) — the admin routes, not the debug ones. 77 §:576 records 76's file set and confirms
disjointness from the operator tree; 79's set is a subset plus the debug crate.

### Other risks

- **`frogdb-debug` gains an axum dependency.** Compile-time cost is the honest concern; the
  crate-count cost is zero (axum 0.8.8 + tower 0.5 are already in `Cargo.lock` and already linked
  by the only consumer, `server/Cargo.toml:120,157-158`). If the reviewer prefers the crate stay
  framework-agnostic, option (A) is available and its cost is stated above — but the proposal's
  recommendation is (B) and the locality argument is the reason.
- **28 signature changes in a 1605-line file.** Mechanical, compiler-verified, and colliding with
  74's four body hunks. Mitigated by ordering (74 first) and by the fact that the two edit
  different parts of the same functions. Still the largest single source of rebase pain in the
  change; it is why the effort is **M** and not **S**.
- **`Path<u64>` rejection body.** axum's default `PathRejection` renders `text/plain`, which would
  reintroduce a non-JSON error on a JSON route. The `/api` sub-router must therefore either use
  `Path<String>` + explicit parse (keeping today's body shape at a corrected status) or install a
  custom rejection — the latter composes with 76's `IntoResponse` pattern. **Named so it is not
  discovered at review**; either resolution is a few lines.
- **Mount form and trailing slashes — resolved in §Proposed change, not open.** axum 0.8 removed
  the implicit trailing-slash redirect, and today `/debug` and `/debug/` are two explicit routes
  (`:233-234`) both mapping to `"/"`. Revision 1 claimed `.nest("/debug", …)` with an inner
  `route("/", …)` matches **both**, and that only `/debug/` was covered by tests. **Both halves
  were wrong**, in the direction that fails CI: `nest` registers the prefix verbatim and drops
  the **slashed** form (`path_router.rs:535-546`; upstream `nest.rs:310-343` asserts
  `/router/` → 404), and `integration_debug_http.rs` covers **both** forms already
  (`test_debug_index_loads :14-39` → `/debug`; `test_debug_index_trailing_slash :41-60` →
  `/debug/`). The design now specifies `.nest_service("/debug", …)`, which registers prefix,
  `prefix/` and `prefix/{*rest}` (`path_router.rs:259-280`; upstream `nest.rs:294-307`). The
  residual risk is only that an implementer "simplifies" `nest_service` to `nest`; the two
  existing tests catch it, and the §Testability table test pins it at unit speed.
- **`tower` is a new dev-dependency, and it needs a feature.** The `oneshot` driver in
  §Testability is `tower::util::ServiceExt::oneshot`, which lives behind tower's **`util`**
  feature. The workspace declares `tower = "0.5"` (`Cargo.toml:179`) with **no features**, and
  `grep -r ServiceExt --include=*.rs` over the tree returns **zero hits** — nothing enables `util`
  today, so it will not arrive transitively by luck. `frogdb-debug` therefore needs
  `[dev-dependencies] tower = { workspace = true, features = ["util"] }`. **76 needs the same
  thing** for its own extractor tests; the feature should be added **once**, at the workspace
  level, by whichever of 76/79 lands first. Coordinated with 76, not duplicated.
- **HTMX partial URLs are a contract with the shipped frontend.** `assets/index.html` and
  `assets/js/charts.js` hard-code 9 paths, and `handlers.rs:846,861` emits a 10th
  (`/debug/partials/cluster-tab`) into server-rendered HTML. All 32 patterns are preserved
  verbatim; the table test in §Testability is what proves it.
- **Deferred, not claimed: the 32 patterns are undocumented as a set.** `diagnostics.mdx:281-290`
  lists the 11 `/api` endpoints (accurately, verified route-by-route) and none of the 17
  `/partials`. That is defensible — partials are an internal HTMX contract — and this proposal
  does not propose documenting them.

## Effort

| Part | Effort | Notes |
|---|---|---|
| `web_ui/router.rs` + delete the match + delete two bridge functions | **S/M** | ~110 lines added, ~90 deleted (`routes.rs:30-112`, `:146-151`; `observability_server.rs:293-303`), 4 files. |
| 28 handler signature changes in `handlers.rs` | **M** | Mechanical and compiler-verified across **four** parameter classes (async / recorder / `Path` / `RawQuery`), but it is a 1605-line file that 74 also edits. This dominates the estimate. |
| New unit tests (route table ×32 incl. the four edge rows, `/debug` vs `/debug/`, bearer-before-mount, three fallback encodings, `Path` rejection) | **S** | The first unit tests the debug UI has ever had; `tower::util::ServiceExt::oneshot`, no server boot. Costs one workspace `tower` feature flag (`util`), shared with 76. |
| `diagnostics.mdx:279-296` + release note for the three wire changes | **S** | Includes the `:296` source cite, which currently names the deleted file. |

**Overall: M.** Not L — no new subsystem, no state machine, no behaviour beyond the three rows in
§Behaviour changes. Not S — 28 signatures, a new crate dependency, three wire-visible changes and
a hard ordering constraint on a sibling.

**Ordering:** `74 → 76 → 79`. Only the `74 → 79` edge is a *correctness* constraint — 79
percent-decodes the bundle id, so it must not land before **74's `BundleId` newtype** (not merely
74's H2, which 79 deletes along with `routes.rs`; see §Risks). `76 → 79` is a cheapness constraint
(79 deletes what 76 shrinks, and consumes 76's rejection type). Both cross-proposal cites in this
document are marked **re-verify at land time**.

## Independently-landable hotfixes

**H1 — `/debug/api/cluster/node/{id}` returns 200 OK for a malformed id (LIVE, claimed).**
`handlers.rs:635-641`; `json_response` hard-codes `StatusCode::OK` at **`:18`**. Return
`StatusCode::BAD_REQUEST` with the same JSON body. **~5 lines**, plus the endpoint's first test.
**Liveness traced in §Problem 3**: unconditional wiring at `subsystems.rs:264`, route registered at
`observability_server.rs:235`, documented at `diagnostics.mdx:283`, and `http.token` defaults to
`None` (`config/src/http.rs:35`) so no credential is needed on a default build. **Standalone — no
refactor dependency**, and **zero collision with 74's four `handlers.rs` hunks** (`:1394+`,
`:1414+`, `:1457+`, `:1505+`): H1 is at `:635-641`. The refactor later subsumes it via `Path<u64>`.

Two amendments from review:

- **Test cost.** Landed standalone, H1's first test is an integration test costing one
  `TestServer::start_standalone()` boot — not a unit test. It becomes a unit test only after
  `router()` exists (§Testability item 5).
- **The partial twin is deliberately left alone.** `render_cluster_node_html`
  (`handlers.rs:842-854`) has the *same* `node_id_str.parse()` failure at `:843-853`, and it also
  returns 200 — but its 200 carries an HTML fragment with the id run through `html_escape`
  (`:851`) and a working back-link (`:849`). For an **HTMX partial** that is the *correct*
  behaviour: HTMX swaps the response body into the DOM, and a 4xx suppresses the swap by default,
  so returning 400 there would replace a helpful inline error with a silently dead panel. H1
  fixes the **JSON API** handler only. Stated explicitly so a later reader does not "finish the
  job" and regress the UI.

**H2 — the JSON API 404s as `text/plain` (LIVE contract break, non-security; claimed).**
`routes.rs:110` → `:146-151`. **Reclassified from LATENT: this is reachable today by any typo
against a documented endpoint** — nothing gates it and no fix arms it; it is simply a
wire-contract defect rather than a security one. Tracked as **N4** in the review ledger.

Until the Router lands, the minimal fix is a second terminal chosen by prefix: paths beginning
`/api/` get `application/json` `{"error":"Not Found"}`. **~8 lines.** No test asserts the body
(`integration_debug_http.rs:576-631` assert status only).

**Gap closed from review:** a prefix-sniffing terminal placed at the wildcard arm `:110` alone is
**incomplete**. `not_found()` has four call sites, and one of them — **`routes.rs:62`**, the
empty-id branch of the `/api/bundle/` arm — is on an `/api/` path but reaches the terminal from a
different call site, where no `p` is in scope to sniff. `GET /debug/api/bundle/` would keep
returning `text/plain` and the hotfix would look done. The fix must either **pass the prefix**
into the terminal or **call the JSON terminal directly at `:62`**; the latter is one line and is
recommended. (The two remaining sites, `:122` and `:141`, are asset/index paths and correctly
stay `text/plain`.) **Standalone.**

**H3 — `diagnostics.mdx:296` cites a file this proposal deletes (LATENT, claimed alongside).**
"Routes are defined in `frogdb-server/crates/debug/src/web_ui/routes.rs`." One line, must move
with the router. Not independently valuable; listed so it is not missed.

**H4 — `web_ui/mod.rs:17` names the wrong CSS framework (LATENT, claimed alongside, cosmetic).**
Says "Chota CSS for styling"; the shipped asset is `assets/css/simple.min.css` (renamed by commit
`cad54a5a`, "Simple.css"), served at `routes.rs:47-50` and asserted at
`integration_debug_http.rs:173`. One word.

**H5 — `GET /debug/api/bundle/generate` is a side-effecting, unauthenticated-by-default operation
on a safe method (SECURITY — FILE ONLY, not claimed, flagged for the user). Classification:
LATENT, armed by 74 §1.** `routes.rs:54` → `handlers.rs:1414-1454` →
`state.generate_bundle_streaming(duration)`: a timed scatter-gather across every shard plus (per
74 §1) a zip written to the bundle directory, reachable by `GET`.

**Reclassified from LIVE.** At HEAD the handler returns **`503`** at `handlers.rs:1418-1426`
before ever reaching `generate_bundle_streaming`, because `bundle_enabled()` is
`bundle_store.is_some() && shard_senders.is_some()` (`state.rs:579-581`) and the store is never
wired. The vector is armed **exactly when 74 §1 lands**, and not before.

**Severity when armed:** unauthenticated, CSRF-able cluster-wide amplification. `http.token` is
`None` by default (`config/src/http.rs:35`), so on a default node the operation needs no
credential; with a token set it remains reachable by any browser-initiated cross-origin
navigation, prefetch or link-preview while an operator's session is live, because `GET` is what
caches, prefetchers and `<img>` tags are allowed to issue.

**Added to the filing (new finding, review):** the `duration` query parameter is an **unbounded
`u64` of seconds** — `parse_query_param(query, "duration").and_then(|s| s.parse::<u64>().ok()).unwrap_or(0)`
(`:1429-1431`) — with no ceiling and no config bound. A single unauthenticated `GET` can therefore
request an arbitrarily long timed collection, which makes the armed amplification **strictly worse
than the original filing stated**. `:1429-1431` is inside 74's `:1414-1454` hunk, so **the bound
is 74's to add**; handed over with the rest.

It is documented as `GET` (`diagnostics.mdx:306, 313`), so changing it to `POST` is a wire-contract
change requiring a **product decision — flagged for the user, not decided here**. Per the standing
policy that security findings are **filed and parked**, this is recorded as an issue and **not
implemented here**. The Router makes the method fix a one-word change (`get` → `post`) whenever it
is authorised.

**H6 — unescaped URL-derived text interpolated into response headers and JSON bodies (SECURITY —
FILE ONLY, not claimed; all sinks belong to 74). Classification: LATENT, armed by 74 §1; severity
LOW-MEDIUM.** **Three** sinks, not two — revision 1 listed only the first two:

1. `handlers.rs:1481-1488` builds `format!(r#"{{"error":"Bundle '{}' not found"}}"#, id)` from the
   raw path segment. A `"` in `id` yields malformed JSON on an `application/json` response.
2. `:1448-1452` builds `format!(r#"{{"error":"{}"}}"#, e)` from an error's `Display`. Same class,
   different source.
3. **Omitted in revision 1:** `:1470-1477` builds a `Content-Disposition` **header value** from
   the raw id — `format!("attachment; filename=\"{}\"", format!("frogdb-bundle-{}.zip", id))` —
   and the builder terminates in `.body(...).unwrap()` at `:1479`. A control character in `id`
   makes `HeaderValue` construction fail, `Response::builder()` carries the error, and the
   `.unwrap()` **panics** — a latent request-triggered DoS on the connection task, not merely a
   malformed body. The identical shape exists at `:1440-1446` in `handle_api_bundle_generate`,
   where the id is server-generated and therefore safe. 74 §5 already names this site
   (`74:85` — "the two unescaped id sinks — `Content-Disposition` `:1470-1477` and the hand-built
   JSON error body `:1484-1487`"), so adding it here makes **74 the owner of all three** with no
   split ownership.

**Severity assessment: LOW-MEDIUM, not high.** Response splitting is **impossible** here — the
body type is `Full<Bytes>` and hyper frames the response itself, so no injected `\r\n` can
manufacture a second response. The realistic impact is malformed JSON / value injection into an
error body (sinks 1–2) and the `.unwrap()` panic (sink 3). All three are inside **74's** hunks
(`:1414-1454`, `:1457-1490`); 74's `BundleId::parse` charset restriction closes all three at the
source, and sinks 1–2 additionally want `serde_json` rather than `format!`. **Handed to 74, filed,
not implemented.**

**H7 — the `id != "list" && id != "generate"` guard at `routes.rs:59` is unreachable (LATENT,
evidence only, handed to 74).** Proven in §Problem 2. In 74's hunk `:57-64`, which 74 rewrites
around `BundleId::parse`; noted so that rewrite drops it rather than carrying it forward.

### Hotfix summary after revision

| # | Class | Claimed by 79? | Standalone? | Size | Owner |
|---|---|---|---|---|---|
| H1 | **LIVE** defect (200-for-malformed-id) | yes | yes — zero collision with 74's four hunks | ~5 lines + 1 booted test | 79 |
| H2 | **LIVE** contract break, non-security (**N4**) | yes | yes — but must cover `routes.rs:62`, not just `:110` | ~8 lines | 79 |
| H3 | LATENT doc cite | alongside | with the router | 1 line | 79 |
| H4 | LATENT doc cite (cosmetic) | alongside | yes | 1 word | 79 |
| H5 | **SECURITY — LATENT, armed by 74 §1** | **no — filed, parked** | n/a | product decision (`GET`→`POST`) + `duration` bound | 74 / user |
| H6 | **SECURITY — LATENT, armed by 74 §1**, LOW-MEDIUM, **three** sinks | **no — filed, parked** | n/a | closed by `BundleId::parse` + `serde_json` | 74 |
| H7 | LATENT dead guard | evidence only | n/a | dropped by 74's rewrite | 74 |

## Review ledger (adversarial review `54baa2bb`, verdict AMEND)

Recorded so no implementer or later reviewer re-litigates settled ground. Every entry was
re-verified against the working tree before being applied or refuted.

### Blocking, upheld and fixed

| # | Finding | Disposition |
|---|---|---|
| **B1** | The `nest` trailing-slash claim was **inverted**, and the "only `/debug/` is tested" claim was **wrong in both halves**. | **Upheld in full.** `path_for_nested_route` returns the prefix verbatim for inner `"/"` (`path_router.rs:541-542`), upstream `nest.rs:336` asserts `/router/` → `NOT_FOUND`, and `integration_debug_http.rs:14-39` + `:41-60` cover **both** forms. Design switched to `nest_service` (`path_router.rs:259-280`), the risk row's polarity corrected, the failure mode restated as *fails CI*, and a mount-form comparison table added. Auth ordering re-confirmed independently (`routing/mod.rs:303-317`). |
| **B2** | The percent-decoding table's "Today" column was **false at HEAD**, and contradicted this proposal's own census. | **Upheld.** `handle_api_bundle_download` 503s at `handlers.rs:1458-1466`; `bundle_enabled()` = `bundle_store.is_some() && shard_senders.is_some()` (`state.rs:579-581`); the store is unwired, which is 74 §1. Table relabelled **post-74-§1**, the literal row annotated *latent, armed by 74 §1*. **Conclusion strengthened**, not retracted. |
| **B3** | The signature inventory missed a **fourth** parameter kind. | **Upheld.** `handle_api_bundle_generate` (`handlers.rs:1414-1417`) takes `query: Option<&str>` from `routes.rs:36`, consumed by `parse_query_param` (`:1493-1498`). Ruled **`RawQuery`** over `Query<T>` — `RawQuery` leaves the body byte-identical and keeps the 74/79 partition intact, since that body is inside 74's `:1414-1454` hunk. |

### Non-blocking, applied

| # | Finding | Disposition |
|---|---|---|
| N1 | `/assets/{*path}` widens too, with a rust-embed symlink caveat | Applied. Second row in the widening table + §Problem 6 rewritten. Escape hatch verified at `rust-embed-impl-8.11.0/src/lib.rs:172-180` (upstream's own `// TODO: Currently it allows "path_traversal_attack" for the symlink files`), debug-builds only (`:51`). Ruled **recorded, not fixed**. |
| N2 | Manifest change is not one line — `tower` needs `features = ["util"]` | Applied. Verified: workspace declares `tower = "0.5"` with no features (`Cargo.toml:179`) and `grep -r ServiceExt` over the tree returns **zero** hits. Coordinated with 76 so `util` is added once. |
| N3 | Unlisted wire change: `405` → `404` for non-`GET` on unknown debug paths | Applied as **behaviour row 6**, with the note that the *existing* 405 test (`:633-652`, bare `/debug`) is unaffected because `/` remains a `get`-only `MethodRouter`. |
| N5 | Four cite/claim corrections | Applied: the `:47` test claim (fixed via B1); the recorder-read list gains `:1093`; `DebugState::new` is `state.rs:364`, not `~60`; and the `"master"`/`"slave"` **Vocabulary evidence is deleted** — see refutations below. |
| N6 | The 74 prerequisite was named two inconsistent ways | Applied. Both mentions now say **74's `BundleId` newtype**, with an explicit paragraph on why 74-H2 is *not* the precondition (79 deletes `routes.rs`, so H2's `routes.rs:57-64` site dies with the file and only `store.rs:89-91` survives). |
| N7 | The 74 quote no longer exists — 74 was revised after it was quoted | Applied. Re-quoted verbatim from `74:745-750` at this HEAD. **All** cross-proposal cites (74, 76) now carry *re-verify at land time*. |

### Recorded so implementers do not re-derive them

- **`with_state` is generic in its OUTPUT state** — `pub fn with_state<S2>(self, state: S) -> Router<S2>`
  (`axum-0.8.8/src/routing/mod.rs:408`). This is *why* a `Router<()>` nests into a
  `Router<HttpState>`, and the crate-boundary design depends on it. §"Two axum facts".
- **The three-fallback design works, with one hole.** A fallback registers at `"/"` and
  `"/{*__private__axum_fallback}"` (`path_router.rs:33-36`, `mod.rs:111`); nested at `/api` those
  become `/api` and `/api/{*…}`, and a wildcard does not match an empty remainder. So
  **`/debug/api/` (bare trailing slash) misses the JSON fallback.** Behaviour is *unchanged* from
  today at that path; pinned by a table-test row rather than fixed. §"Two axum facts",
  §Behaviour changes rows 1–2.

### Refuted / withdrawn by this revision

- **Withdrawn (own claim, not a review point):** revision 1's Vocabulary evidence that
  `handlers.rs` emits `"master"`/`"slave"` as JSON **values**. `grep '"master"\|"slave"'` over
  `handlers.rs` returns **zero** string literals. What exists is the field *names* `master_host`
  `:53` / `master_port` `:54` (JSON keys) and a local `master_html` `:203`; `role` is a `String`
  from `DebugState::role()` (`state.rs:449-451`) whose domain is
  `standalone`/`primary`/`replica`/`cluster` (`state.rs:78`, `:93`). **The out-of-scope ruling
  stands on other grounds; the evidence for it does not and has been replaced.**
- **Refuted (review-adjacent, no change owed):** the review's suggestion that the `nest` mount
  might also break the bearer-auth ordering. It does not — `Router::layer` maps over
  `path_router`, `fallback_router` and `catch_all_fallback` alike (`routing/mod.rs:303-317`), so
  auth ordering is preserved under **either** mount form. Recorded as verified, not as a risk.

### Confirmed by review, unchanged

The census (**31 arms / 32 patterns / 28 + 3 handlers**); every dead-code claim (zero callers
each — `handle_debug_request`'s `unwrap_or(full_path)`, the stale ordering comment at
`routes.rs:66`, the dead `id != "list"` guard at `:59`); the crate-boundary argument for option
**(B)**; auth-ordering preservation; the LOCKED / FM-tag / clock-seam clearance; the `74 → 76 → 79`
chain with only `74 → 79` as a correctness edge; and H1's live-bug chain traced end to end.
