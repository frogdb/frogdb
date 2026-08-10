# Proposal 76 — The observability HTTP surface: two extractors, one rejection, one parse

Round 38 · lane: frogctl / operator / telemetry · candidates **FR6 + FR11** · effort **M** (FR6)
+ **S** (FR11), independently landable · **no locked crate**, no FM tag, no seam-lint touched

**Verified at HEAD `4372082285b34079ae6c1eb0c2d135a55d91ca83`**, **re-verified after adversarial
review at `7783e82d`** (worktree `arch-round-38-99`, branch `main`). Every commit between the two
touches only `.scratch/**.md` — `git diff --stat 43720822..HEAD -- ':!*.md'` is empty — so every
line number below was re-derived against an unchanged tree, and the corrections in this revision
are authoring errors, not code drift. **Two brief claims are corrected**:

| Brief claim | Correction |
|---|---|
| "dup `not_found` (http_handlers.rs:83-89 pub dead + **routes.rs:145-151** private)" | The private twin is at `debug/src/web_ui/routes.rs:**146-151**`, and that file belongs to **proposal 74** (1 hunk) and future **79**. This proposal deletes only the telemetry copy and says so. |
| FR6 is "**Latent**" | Half right. The *duplication* is latent. Two **LIVE** defects were found in the same file set that the brief did not name: `/admin/transfer-leader` returns **200 OK** carrying an error body while both its own doc comment and the published docs say it errors, and `/admin/shutdown` is **permanently 503** while the website documents it as working. Neither is caused by the duplication; both are hidden by it. |

Four further findings the brief did not name: `HttpState`'s `Option`-ness is a **type-level lie
for two of its three optional fields** (§Problem 3), the bearer-token gate has **zero tests
anywhere in the tree** (§Problem 5), `MetricsSnapshot` (FR11) has **zero external callers of its
accessor methods** — which changes what the FR11 fix has to preserve (§Problem 7) — and
`AdminConfig`'s doc comment **contradicts its own struct** (§Problem 8, hotfix H6).

**Three of this document's own claims are also retracted or resized in this revision**, because
they did not survive verification: FR11's headline beneficiary was misattributed (§Problem 7 — it
is *not* `integration_metrics.rs`, and the count was 43, not 47), the "untestable-by-construction"
justification for FR6's sizing was **false** (§Testability — the tests are writable today), and
the `master`/`slave` carve-out rested on a wire-compat exemption that does not cover these routes
(§Spec impact). Each is corrected in place with the evidence, rather than quietly dropped.

## Summary

Two independent changes, one theme: **the observability surface has an adapter tier that carries
no decisions, and it hides the decisions that do exist.**

- **FR6 — `observability_server.rs` is 25% wrapper.** Ninety-one of its 363 lines (`:273-363`)
  are twelve handler functions that contain **no logic at all**: four project a field out of
  `HttpState` and call a `frogdb_telemetry` function, seven perform the character-identical
  `admin_state.ok_or(SERVICE_UNAVAILABLE)?` → delegate → `into_response()` triple, and one
  hand-builds a 503 with `hyper::Response::builder(…).unwrap()`. The tier exists because
  axum's state is `HttpState` while every collaborator wants one field of it. That is exactly
  what `FromRequestParts` is for. The **leverage** is not the 91 lines — it is that "this
  subsystem is absent" is currently decided at **9 sites, in 2 crates, with 3 wire encodings**
  (bare 503 empty-body ×7 and `text/plain "Debug UI not enabled"` ×1, both in `frogdb-server`;
  `application/json {"error": …}` ×1 in `frogdb-telemetry`). **Two** extractors plus **one**
  `IntoResponse` rejection type make that one decision with one encoding. Of the twelve wrappers,
  **7 delete, 1 shrinks, 4 stay** — the tier is thinned where it carried decisions and left alone
  where it does not, and §(b) argues for keeping the four rather than pretending they go away.
- **FR11 — `telemetry/testing.rs` parses the same string N times.** `MetricsSnapshot` stores
  `raw: String` (`:462`) and every accessor delegates to a free function that calls
  `parse_prometheus` from scratch (`:517-537` → `:256-261` → `:64`). A `MetricsDelta` chain of
  two assertions re-parses the whole `/metrics` payload — 95 metric families, several of them
  per-command or per-shard labelled — **four** times. The **duplication** the brief names is
  real but is not two APIs: the four `MetricsSnapshot` methods have **zero callers outside
  `testing.rs`** (verified), so they are a private delegation layer pointing the wrong way.
  Invert it — parse once in `MetricsSnapshot::new`, make the free functions the thin edge —
  and the parse-once falls out for free. A third copy of the label-match predicate
  (`:311-316`, duplicating `:232-237`) folds in. **The win is real but narrow, and this
  proposal previously oversold it**: see §Problem 7 for the honest sizing — the beneficiaries
  are the **34 external snapshot constructions** (26 `MetricsSnapshot::new` + 8
  `MetricsSnapshot::fetch`) across five test files, *not* the 43 one-shot free-function calls,
  which keep exactly today's cost by design.

Neither change alters a production code path's behaviour except where explicitly listed under
§Behaviour changes, and the two LIVE defects are carved out as **independently-landable
hotfixes** that do not wait on the refactor.

## Files involved

| Path | Lines | Role in this change |
|---|---|---|
| `frogdb-server/crates/server/src/observability_server.rs` | 363 | **Primary (FR6).** `HttpState` `:37-44` (all six fields `pub`, `:38-43`); `create_router` `:221-252`; `bearer_auth_middleware` `:256-271`; the wrapper tier `:273-363` — of its twelve wrappers **7 are deleted** (the admin seven, `:308-363`), **1 shrinks** (`debug_handler` `:293-303`, 11 → ~4 lines) and **4 are kept unchanged** (the telemetry pass-throughs `:277-291`). File has **no `#[cfg(test)]` module at all**. 9 commits of churn. |
| `frogdb-server/crates/server/src/admin/handlers.rs` | 438 | **Primary (FR6).** Seven handler signatures change `State(state): State<SharedAdminState>` → `state: SharedAdminState`; `transfer_leader` `:426-438` is the hotfix site. 5 commits. |
| `frogdb-server/crates/server/Cargo.toml` | — | Read-only evidence. `axum.workspace = true` `:157`, `tower.workspace = true` `:158` — both already present; the workspace `tower = "0.5"` (root `Cargo.toml:179`) carries **default features only**, so `ServiceExt::oneshot` needs `features = ["util"]` added (see §Testability). |
| `frogdb-server/crates/telemetry/src/http_handlers.rs` | 168 | **Primary (FR6).** Six `Response::builder()…unwrap()` chains, **8 `.unwrap()` in the 91 non-test lines**; `handle_status_json`'s `None` arm `:73-79`; **`not_found` `:84-90` deleted** (zero callers). 4 commits. |
| `frogdb-server/crates/telemetry/src/testing.rs` | 1150 | **Primary (FR11).** `MetricSample` `:33-39`; `parse_prometheus` `:64-81`; `find_metric` `:221-241`; free fns `:256-332`; `MetricsSnapshot` `:460-537`; `MetricsDelta` `:556-723`; `fetch_metrics` `:474-487`. 1 commit — never revisited since it was written. |
| `frogdb-server/crates/telemetry/src/lib.rs` | — | **Primary (FR11/FR6).** Re-export list `:49-51` (note: `not_found` is *not* re-exported — reachable only as `http_handlers::not_found`). |
| `frogdb-server/crates/telemetry/Cargo.toml` | — | Read-only evidence. **No `axum` dependency** — the constraint that shapes §Proposed change. |
| `frogdb-server/crates/server/src/server/subsystems.rs` | 930 | **Read-only evidence, must NOT be edited.** `ObservabilityServer` assembly `:258-269`; `admin_state` gate `if self.config.admin.enabled` `:240`, `AdminState` construction `:241-253`, the literal **`shutdown_tx: None` at `:251`** (H2's one fix line — `:253` is the closing `}))`), the `if let Some(admin_state)` at `:267`. Owned by **63/64/74**. |
| `frogdb-server/crates/config/src/http.rs` | — | Read-only. `HttpConfig.token` `:31-35` (`#[param(skip)]`, security-annotated); the `0.0.0.0` warning `:85`. |
| `frogdb-server/crates/config/src/admin.rs` | — | Read-only. `AdminConfig.enabled` `:16-19` — `#[serde(default)]` on `bool`, i.e. **default `false`**. Its own struct doc at `:10-11` is **wrong** — see §Problem 8. |
| `frogdb-server/crates/debug/src/web_ui/routes.rs` | 208 | **Read-only, must NOT be edited.** The private `not_found` twin `:146-151`. Owned by 74 + future 79. |
| `frogdb-server/crates/test-harness/src/server.rs` | — | Read-only evidence (FR11). `fetch_metrics` `:883-895` — a near-duplicate of `testing.rs:474-487`. |
| `frogdb-server/crates/server/tests/cluster_misc.rs` | — | The **only** admin-endpoint test in the tree: `/admin/upgrade-status` `:357`, `:431`. |
| `frogdb-server/crates/server/tests/integration_metrics.rs` | 1166 | FR11 evidence. **38** free-function call sites — the bulk of the 43 external ones — and **zero** `MetricsSnapshot`/`MetricsDelta` uses (verified: `rg -c 'MetricsSnapshot\|MetricsDelta'` returns no match). **It is therefore not a beneficiary of FR11**; see §Problem 7. |
| `website/src/content/docs/operations/clustering.md` | — | Read-only (content page). Admin endpoint table `:121-127` (**seven rows**, `/admin/health` at `:121`) — the doc side of hotfix H1/H2/H5. |
| `website/docs-spec/specs/operations/clustering.md` | — | Read-only (**spec, the source of truth**). Admin-API prose `:94-103`, structure bullet `:140-142`. H1/H5 edit this **first**; see H5. |
| `website/docs-spec/specs/architecture/clustering.md` | — | Read-only (**spec**). Admin-API ruling `:89-97`. |

New file: `frogdb-server/crates/server/src/observability_server/extract.rs` (~90 lines) — or an
inline module; see §Proposed change.

## Problem

### 1. Twelve handlers, zero decisions (FR6)

The router registers **16 routes** resolving to **12 distinct handler functions** (`:221-252`) —
`health_live_handler` serves both `/health/live` and `/healthz` (`:225`, `:227`),
`health_ready_handler` both `/health/ready` and `/readyz` (`:226`, `:228`), and `debug_handler`
all three `/debug*` routes (`:233-235`). **All twelve** live in the block `:273-363` and every one
is a pass-through. Census:

| Group | Sites | Shape | Lines |
|---|---|---|---|
| Telemetry pass-through | 4 (`:277`, `:281`, `:285`, `:289`) | `State(s)` → `handle_x(s.field)` | 12 |
| Debug pass-through | 1 (`:293-303`) | `Option` unwrap + hand-built 503 | 11 |
| Admin projection | 7 (`:308`, `:314`, `:323`, `:329`, `:335`, `:341`, `:353`) | `s.admin_state.ok_or(SERVICE_UNAVAILABLE)?` → delegate → `into_response()` | 56 |

The seven admin wrappers differ **only in the delegate's name**, plus one accident: **three** of
them (`:317-320` cluster, `:347-350` shutdown, `:359-362` transfer-leader) spell the identity
re-wrap out longhand —

```rust
match result {
    Ok(json) => Ok(json.into_response()),
    Err(status) => Err(status),
}
```

— which is `result.map(IntoResponse::into_response)`. The other **four** (`:311`, `:326`, `:332`,
`:338`) use the one-liner. Same operation, two spellings, in the same 56-line block. That is the
signature of a tier nobody reads as a whole.

The file's doc comment names the reason (`:274-275`): "These wrap the existing
framework-agnostic handlers from `frogdb_telemetry` and `frogdb_debug`, which return
`Response<Full<Bytes>>`." **The stated reason does not hold for the admin seven** — those
delegate to `crate::admin::handlers`, which is already axum-native (`axum::{Json, extract::State,
http::StatusCode}`, `admin/handlers.rs:5`). The admin tier wraps axum in axum.

### 2. "Subsystem absent" is decided at 9 sites, in 2 crates, with 3 encodings

| Site | Crate | Condition | Wire result |
|---|---|---|---|
| `observability_server.rs:309,315,324,330,336,345,357` (×7) | `frogdb-server` | `admin_state == None` | `503`, **empty body**, no content-type |
| `observability_server.rs:297-301` | `frogdb-server` | `debug_state == None` | `503`, `text/plain`, `"Debug UI not enabled"` |
| `telemetry/http_handlers.rs:73-79` | `frogdb-telemetry` | `status_collector == None` | `503`, `application/json`, `{"error": "Status collector not configured"}` |

**9 sites / 2 crates / 3 encodings** — the eight `frogdb-server` sites and the one
`frogdb-telemetry` site. This count is the single strongest argument in the proposal and is used
verbatim in the deletion test below. Three encodings of one condition. A client that wants to distinguish "this node has no admin API"
from "this node is unhealthy" gets an empty 503 in the first case and must parse a body in the
third. There is no type carrying the concept, so there is no place the third encoding could have
been made to agree with the first.

### 3. `HttpState`'s `Option`s are a type-level lie for two of three fields

`HttpState` (`:37-44`) declares `debug_state: Option<_>`, `status_collector: Option<_>`,
`admin_state: Option<_>`. Reading the sole construction path (`subsystems.rs:258-269`, inside
`if let Some(ref prometheus) = self.prometheus_recorder` at `:171`):

```rust
let mut server = ObservabilityServer::new(http_config, prometheus.clone(), self.health_checker.clone())
    .with_listener(http_listener)
    .with_debug_state(debug_state)              // :264 — unconditional
    .with_status_collector(status_collector.clone());  // :265 — unconditional
if let Some(admin_state) = admin_state {        // :267 — genuinely conditional
    server = server.with_admin_state(admin_state);
}
```

`debug_state` is built unconditionally at `:210-222` and `status_collector` at `:151-168`. So in
production **`debug_state` and `status_collector` are always `Some`** — the 503 arms at
`observability_server.rs:297-302` and `http_handlers.rs:73-79` are unreachable outside a
hypothetical second caller. Only `admin_state` is truly optional, gated on
`if self.config.admin.enabled` (`subsystems.rs:240`), which **defaults to `false`**
(`config/src/admin.rs:16-19`: `#[serde(default)]` on a `bool`). So on a default build, all seven
admin routes return the empty 503 and there is no body explaining why.

Three `Option`s, one meaning "configurable", two meaning "I didn't want to thread the type". The
wrapper tier is the tax on not distinguishing them.

### 4. Two LIVE defects the duplication hides

**H1 — `/admin/transfer-leader` reports success for an unimplemented operation.** Its own doc
comment says (`:423-425`) "This endpoint currently returns 501." The body returns
`Ok(Json(json!({"status": "error", …})))` (`:434-437`) — HTTP **200 OK**, routed through the
wrapper at `:353-363`. **Three documents disagree with the code**: the doc comment (`:424`), the
content page `website/src/content/docs/operations/clustering.md:127` ("**Not implemented** —
returns an error"), and the spec `website/docs-spec/specs/operations/clustering.md:98-99`
("**returns not-implemented** — openraft 0.9 lacks the API; state this honestly"). Any client
that checks the status code and not the body — the normal thing to do — records a successful
leadership transfer that never happened. **LIVE.** One line.

**H2 — `/admin/shutdown` is permanently 503.** `AdminState.shutdown_tx` has exactly one producer
and it is a literal `None` with the reason inline: `shutdown_tx: None, // TODO: wire up shutdown
channel from Server` (**`subsystems.rs:251`**, inside the `:241-253` construction — note `:253`
is the closing `}))`, so any issue must cite `:251`). The sole consumer is
`admin/handlers.rs:405`, whose `else` at `:409` is therefore the only reachable arm. Meanwhile
both doc pages promise the endpoint works: `operations/clustering.md:126` "Trigger a graceful
shutdown", `architecture/clustering.md:652` "Graceful shutdown". **LIVE**, but the fix line is in
`subsystems.rs` — a file this proposal must not edit (see §Boundaries). Carved out as an issue,
not claimed.

**Blast radius of H1 and H2, stated honestly.** No in-repo client calls either endpoint.
`frogctl`'s only admin-API consumers are `/admin/health` (`frogctl/src/commands/health.rs:365`)
and `/admin/upgrade-status` (`frogctl/src/commands/upgrade.rs:136`, `:277`), all through
`ConnectionContext::admin_get` (`frogctl/src/connection.rs:155-162`); nothing in the tree calls
`transfer-leader` or `shutdown`. So the severity is **"three documents lie about the wire
contract"**, not "a shipped client is broken". That is still worth fixing at one line each — a
documented endpoint that silently no-ops is exactly the class of thing an operator discovers
during an incident — but it is not an emergency, and the proposal does not claim it is.

### 5. The auth gate has no test, and the comparison is not constant-time

`bearer_auth_middleware` (`:256-271`) is the only thing standing between an unauthenticated
caller and `/admin/*` + `/debug/*`. Grepping the whole of `frogdb-server/crates` for `http.token`
/ `http_token` returns config plumbing (`config/src/http.rs`, `server/src/cli.rs`,
`config/loader.rs:157-158`, `main.rs:65`, `subsystems.rs:236`) and **no test that ever sets it**.
The authenticated path — both the accept and the reject arm — is executed by nothing in the tree.

Its comparison is also worth recording while the file is open:

```rust
let expected_header = format!("Bearer {}", expected);   // :262 — allocates per request
match req.headers().get("authorization") {
    Some(val) if val.as_bytes() == expected_header.as_bytes() => next.run(req).await,  // :264
```

`==` on `[u8]` short-circuits on first mismatch, so response latency leaks the length of the
matching prefix. Recorded, not claimed — see §Hotfixes H3.

### 6. A dead `not_found`, and a private twin in a file this proposal may not touch

`telemetry::http_handlers::not_found` (`:84-90`) is `pub` in a `pub mod` but **is not in the
crate's re-export list** (`lib.rs:49-51` names only the four `handle_*` fns). Grepping the tree
for the identifier returns its own definition, its own test (`:164-167`), and — in
`frogdb-debug` — a **character-identical private copy** at `web_ui/routes.rs:146-151` with four
call sites (`:62`, `:110`, `:122`, `:141`). The dead public one is a pure deletion. The private
one is load-bearing and lives in a file owned by proposal 74 and future 79; it stays.

### 7. FR11 — parse-per-accessor, and a delegation pointing the wrong way

`MetricsSnapshot` holds `raw: String` (`:462`). Its four accessors (`:517-537`) are one-line
delegations to the free functions, and every free function begins by re-parsing:

```rust
pub fn get_counter(text: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    let samples = parse_prometheus(text);            // :257 — full re-parse, every call
    find_metric(&samples, name, labels).map(|s| s.value).unwrap_or(0.0)
}
```

`parse_prometheus` allocates a `String` name and a `HashMap<String, String>` **per sample**
(`:35-38`, `:64-81`). FrogDB defines **95 metric families** (44 counter, 42 gauge, 9 histogram —
`types/src/metrics/definitions.rs`), several of them labelled per command or per shard, plus nine
histograms' worth of `_bucket` series; a live `/metrics` payload is hundreds to thousands of
lines. Cost per operation:

#### The census, and who actually benefits

An earlier draft of this proposal claimed "47 free-fn sites in `integration_metrics.rs`" and made
that file the headline beneficiary. **Both halves were wrong, and the correction matters enough to
resize the change.**

*Wrong count.* A tree-wide grep for `get_counter|get_gauge|get_histogram_count|get_histogram_sum|
get_histogram_buckets` outside `testing.rs` returns **43** genuine sites, not 47. The extra four
were **false positives in `telemetry/src/prometheus_recorder.rs:312, 329, 360, 361`** — those are
the `prometheus` crate's protobuf accessors `MetricFamily::get_counter()` / `get_gauge()` on a
`Metric` message, entirely unrelated to `testing.rs`'s parse helpers. Corrected external census:

| File | Free-fn sites | Uses `MetricsSnapshot`/`MetricsDelta`? |
|---|---|---|
| `tests/integration_metrics.rs` | 38 | **No** (zero hits) |
| `tests/integration_basic.rs` (`:49`, `:50`, `:63`, `:64`) | 4 | No |
| `tests/integration_pubsub.rs:4835` | 1 | No |
| **Total external** | **43** | — |

*Wrong beneficiary.* Those 43 sites get **zero improvement**, and that is by design: §Proposed
change preserves the free functions' one-shot cost exactly (`parse_prometheus(text)` once, then
`lookup`). 38 parses in `integration_metrics.rs` stay 38 parses. The file is not a beneficiary at
all — it never constructs a snapshot.

*The real win.* It is the `MetricsDelta` chain, in the **five files that do use snapshots** —
`tests/property_tests.rs`, `tests/functions.rs`, `tests/timeseries.rs`, `tests/resp3.rs`,
`tests/integration_scripting.rs`. Across them the tree holds **26 external `MetricsSnapshot::new`
sites and 8 `MetricsSnapshot::fetch` sites** (34 snapshot constructions). Each snapshot is parsed
once instead of once per accessor call, so an N-assertion delta chain goes from **2N parses to 2**:

| Operation | Full parses before | after |
|---|---|---|
| one `get_counter(&text, …)` (43 sites) | 1 | **1 — unchanged, by design** |
| `MetricsDelta::assert_counter_increased` (1 assertion) | 2 | 2 |
| the two-link chain at `tests/timeseries.rs:102-104` | 4 | **2** |
| an N-assertion chain | 2N | **2** |

That is a genuine but **modest** win: bounded by the 34 snapshot constructions and by how long the
delta chains in five files are. It is worth doing — it is ~40 lines in one file with no call-site
churn — and it is **S**, not more. The proposal states this rather than borrowing
`integration_metrics.rs`'s 38 to look bigger.

The **shape on disk is not the one the brief assumed.** Grepping for the accessor methods
(`.counter(`, `.gauge(`, `.histogram_count(`, `.histogram_sum(`) outside `testing.rs` returns
**zero hits**. `MetricsSnapshot`'s methods are consumed only by `MetricsDelta` inside the same
file. So this is not "two public APIs that must both be kept" — it is one API (the free functions,
plus the `assert_*!` macros) sitting on a private delegation that points from the type that
*could* cache toward the function that *cannot*.

The macros are a **smaller public surface than "eight public macros" suggests**: of the nine
`assert_*!` invocations in the tree, exactly **one is external** —
`tests/integration_metrics.rs:186` (`assert_gauge_gte!`). The other eight are `testing.rs`'s own
unit tests (`:964`, `:970`, `:971`, `:978`, `:1139`, `:1147`, `:1148`) plus a doc-comment example
(`:737`). They are exported and must keep compiling, but "don't break the macros" is a constraint
about one call site, not a broad compatibility surface.

Two smaller items fold in:

- **A third copy of the label-match predicate.** `find_metric` `:232-237` and
  `get_histogram_buckets` `:311-316` are the same six lines with different bindings.
- **`find_metric` is first-match-wins** (`:226`). Querying a labelled family with a partial (or
  empty) label set silently returns an arbitrary series rather than erroring or summing. No
  current test trips it — the families queried with `&[]`
  (`frogdb_connections_total:51`, `frogdb_keyspace_hits_total:113`,
  `frogdb_keyspace_misses_total:116`) are all label-free — but `frogdb_commands_total` **is**
  labelled (`:74-76`), so the trap is one careless test away. **Latent.**
- **A near-duplicate fetcher.** `testing.rs:474-487` and `test-harness/src/server.rs:883-895` are
  the same `reqwest` no-proxy GET with the same four `.unwrap()`s, differing only in how the URL
  is built. Recorded; folding them requires `test-harness` to depend on the `testing` feature of
  `frogdb-telemetry`, which is a separate call — see §Scope boundaries.

### 8. `AdminConfig`'s doc comment contradicts its own struct

Found while verifying `admin.enabled`'s default. `config/src/admin.rs:10-11` states:

```rust
// No fields are exposed as CONFIG GET/SET parameters; each carries an explicit
// `#[param(skip)]` to satisfy the per-field coverage guarantee.
```

None of the three fields carries `#[param(skip)]`. All three carry `#[param(name = …)]` —
`admin-enabled` (`:18`), `admin-port` (`:23`), `admin-bind` (`:28`) — and all three **are**
registered in the golden parameter table (`config/src/params.rs:383` extends from
`AdminConfig::PARAMS`; the rows are at `:1061`, `:1068`, `:1075`, each `mutable: false`). The
comment describes the opposite of the code it sits on, and it is the comment that is stale: the
params are real, boot-fixed, read-only. **Latent, documentation-only, one-line fix** (delete or
correct the comment). Filed as an issue candidate — this proposal does not edit `config/`.

## Proposed change

### FR6 — two extractors, one rejection, and stop wrapping axum in axum

The constraint that shapes everything: **`frogdb-telemetry` does not depend on axum**
(`telemetry/Cargo.toml`) and should not start. The `frogdb_telemetry` handlers are sync functions
returning `hyper::Response<Full<Bytes>>` — which axum 0.8 already accepts as a response body, so
they are *already* `IntoResponse`-compatible; they are simply not `Handler`s (wrong arity, not
async). So the design splits cleanly:

**(a) Exactly two fallible extractors.** A `FromRequestParts<HttpState>` impl for
`SharedAdminState` and one for `Arc<DebugState>` — **two, not three**; the `status_collector` case
is handled inline, see (c) — sharing one `Rejection` type:

```rust
/// The one wire encoding of "this node does not have that subsystem installed".
pub struct SubsystemUnavailable(&'static str);   // the subsystem's name

impl IntoResponse for SubsystemUnavailable { /* 503 + application/json + {"error": …} */ }
```

9 sites / 2 crates / 3 encodings → one impl, one encoding. This is the **seam**: after it exists,
"absent subsystem" cannot be spelled a fourth way without going around a type that every
observability route already names.

Both impls are **orphan-rule-legal even though `FromRequestParts` and `Arc` are both foreign**:
axum's trait is generic over the state type (`FromRequestParts<S>`), and the local `HttpState` is
supplied as `S`, so `impl FromRequestParts<HttpState> for Arc<DebugState>` has a local type in the
impl's type parameters and satisfies E0117. Stated because it is the first thing an implementer
would doubt, and doubting it leads to a pointless newtype detour.

With it, the seven admin handlers change their receiver from `State(state):
State<SharedAdminState>` to bare `state: SharedAdminState`, and the router references them
directly:

```rust
.route("/admin/health",  get(admin_handlers::health))
.route("/admin/cluster", get(admin_handlers::cluster_state))
…
```

All **seven admin wrappers delete** (56 lines), along with all three longhand identity re-wraps.
The debug wrapper is **not deleted** — it keeps its `Uri` argument and its shape, but takes
`debug: Arc<DebugState>` via the extractor instead of `State<HttpState>` and so loses its
hand-built 503 (11 → ~4 lines). **Net wrapper disposition: 7 deleted, 1 shrunk, 4 kept.**

**(b) The four telemetry pass-throughs stay — and this proposal says so rather than pretending
otherwise.** An extractor for `Arc<PrometheusRecorder>` / `HealthChecker` is *infallible*, so it
buys no error unification; and because `handle_metrics` and friends are sync free functions
taking owned values, a three-line async wrapper is still needed either way. Replacing
`State(s) → handle_metrics(s.recorder)` with `recorder: PrometheusRecorderOf<HttpState> →
handle_metrics(recorder.0)` is the same three lines plus an impl. **Rejected on the deletion
test** (below). The brief's "`FromRequestParts<HttpState> for SharedAdminState`" was the right
half; the generalisation to all of `HttpState` is not.

The four kept wrappers are `metrics_handler` (`:277`), `health_live_handler` (`:281`),
`health_ready_handler` (`:285`) and **`status_json_handler` (`:289`)** — see (c) for why the last
one stays a wrapper.

**(c) `handle_status_json` loses its `Option`, but does *not* get a third extractor.** The
telemetry signature changes to `handle_status_json(collector: Arc<StatusCollector>)` —
non-optional, matching the fact that it is always `Some` in production (§Problem 3) — and its
private JSON error encoding (`http_handlers.rs:73-79`, the ninth site) deletes. The absence case
moves into the **kept** `status_json_handler` wrapper as one inline line:

```rust
async fn status_json_handler(State(s): State<HttpState>) -> Result<Response, SubsystemUnavailable> {
    let collector = s.status_collector.clone()
        .ok_or(SubsystemUnavailable("status collector"))?;
    Ok(handle_status_json(collector).await.into_response())
}
```

**A third `FromRequestParts` impl was considered and rejected**, deliberately: `status_collector`
is reached from exactly one route, so an extractor would buy no de-duplication, and — the decisive
point — declaring one would contradict (b)'s deletion test, which keeps this wrapper. The inline
`ok_or` is cheaper, reuses the same rejection type (so the encoding still unifies, which is the
whole point), and leaves (a)/(b)/(c) mutually consistent: **two extractors, four kept wrappers,
one rejection type covering all 9 sites.** The telemetry crate loses an `Option` it never had a
reason to hold and one `Response::builder()` chain; it gains no dependency.

**(d) `not_found` (`http_handlers.rs:84-90`) and its test delete.** Zero callers.

#### Depth and locality

The extractor is a **shallow module by line count and a deep one by decision count**: ~90 lines
that own the single decision "what does the wire see when a subsystem is not installed", for
every current and future observability route. Locality improves in the direction that matters —
today, changing that answer means editing 9 sites in 2 crates with 3 encodings; after, one impl.
The adapter tier does not vanish (four telemetry wrappers stay, §(b)) but it **stops carrying
decisions**: the two things it was adapting (axum state shape, absent-subsystem encoding) become
two extractors and one rejection type, and what remains is three literal three-line forwardings
plus `status_json_handler`'s single `ok_or` into the same rejection type.

#### Deletion test, applied honestly

- **`SubsystemUnavailable` + the two extractor impls** — delete them and the absent-subsystem
  decision reappears at **9 sites across 2 crates in 3 encodings** (7 admin `ok_or(503)` triples
  + the debug hand-built 503, all in `frogdb-server`; plus `http_handlers.rs:73-79` in
  `frogdb-telemetry`), and there is again no place for the three encodings to agree.
  **Earns its keep.** This is the proposal's load-bearing number.
- **An extractor for `PrometheusRecorder`/`HealthChecker`** — delete it and four three-line
  wrappers reappear, which is what exists today, minus an impl. **Does not earn its keep;
  rejected.**
- **`http_handlers::not_found`** — delete it and *nothing* reappears. **Pure deletion.**

### FR11 — parse once, invert the delegation

`MetricsSnapshot` becomes the parsed thing:

```rust
pub struct MetricsSnapshot {
    samples: Vec<MetricSample>,  // parsed once in `new`
}
```

The `raw: String` field goes away entirely: `MetricsSnapshot::raw()` (`:510-512`) has **zero
callers in the tree** (verified — the tests that print a metrics payload on failure, e.g.
`integration_metrics.rs:1110-1130`, hold the harness's own `String` from
`server.fetch_metrics()`, never a snapshot). `new(raw: String)` keeps its signature (`:491`) — its
**26 external call sites**, plus the 8 that go through `MetricsSnapshot::fetch` (`:505-507`), are
unchanged — and simply parses and drops the string. Whether `raw()` is deleted or re-implemented
as a re-render is a one-line call; deletion is preferred, and it is the same class of pure
deletion as H4.

The four accessors read `self.samples` directly. The free functions keep their **exact public
signatures** (`&str` in) and become the thin edge, delegating through the same private
`(samples, name, labels)` lookup the methods use — `parse_prometheus(text)` once, then
`lookup(&samples, …)`, i.e. exactly today's cost for a one-shot call and no payload clone — so
all **43 external call sites and the one external macro use (`integration_metrics.rs:186`)
compile unchanged**, and the `assert_*!` macros (which expand to
`$crate::testing::get_counter(…)`) are untouched. A `MetricsDelta` chain of N assertions goes from
2N parses to **2**, because the parses already happened in the two `MetricsSnapshot::new` calls
the tests already write. Restated plainly: **the 43 free-fn sites are held harmless, not
improved; the 34 snapshot constructions are where the win lands.**

The label-match predicate moves to one place — `impl MetricSample { fn matches(&self, name: &str,
labels: &[(&str, &str)]) -> bool }` — consumed by `find_metric` and by
`get_histogram_buckets`'s filter, deleting the second copy.

`find_metric`'s first-match-wins stays as-is (changing it is a behaviour change to a test API with
43 external call sites and belongs in its own change), but the parsed form makes the fix
*available*: with
`samples` owned by the type, a `#[track_caller]` uniqueness assertion or a `sum` variant is a
method away rather than a re-parse away. Recorded as a follow-up, not folded in.

#### Deletion test

Delete `MetricsSnapshot` and the free functions still work — but the 34 snapshot constructions in
five test files re-write the before/after pairing by hand and the parse-once is gone. Delete the
free functions and 43 external call sites plus the `assert_*!` macro family break. Both earn their
keep; the change is which one owns the data.

## Testability — an earlier claim retracted, and a step added

**Retraction.** An earlier draft justified FR6's **M** sizing partly on moving the observability
surface from "untestable-by-construction" to unit-testable. **That claim is false**, and the
correction is worth stating precisely because it changes what the refactor is for.

The evidence that it is false, all in the current tree:

- All six `HttpState` fields are **`pub`** (`observability_server.rs:38-43`), so any test in the
  crate can build one by hand.
- Its collaborators have public constructors: `PrometheusRecorder::new` (`prometheus_recorder.rs:153`),
  `HealthChecker::new` (`health.rs:70`), and `debug_state` / `status_collector` / `admin_state` /
  `token` all accept `None`.
- `create_router` is private (`:221`) but that is not a barrier — a `#[cfg(test)] mod tests` in
  the same file calls it directly.
- `axum` and `tower` are already dependencies of `crates/server` (`Cargo.toml:157-158`).

So a router unit test is writable **today, before any refactor**. What is actually absent is the
test module: `observability_server.rs` contains **no `#[cfg(test)]` block at all**. That is an
absence of tests, and the wrapper tier did not cause it. The `HttpState` "one construction site"
observation from the earlier draft was about *production* wiring (`:134-141`, inside
`ObservabilityServer::run`) and says nothing about testability.

**What FR6 honestly buys, testing-wise**, is narrower: the seven admin handlers stop taking
`State<SharedAdminState>` and become **plain `async fn(Arc<AdminState>)`** — a signature
simplification that lets a unit test call them without naming an axum extractor type. Convenient;
not a capability unlock. FR6's justification rests on the 9-sites/2-crates/3-encodings
unification, not on this.

**Added step, and it comes first.** Because FR6 *does* change wire encoding (`IntoResponse`
rejection replaces three ad-hoc bodies), the four tests below must be written **before** the
refactor, as a regression net that pins current behaviour and then gets updated deliberately where
§Behaviour changes says so. Writing them after would mean writing tests against the new code and
learning nothing about what changed.

1. **Router `oneshot` smoke test** — build `HttpState` by hand, call `create_router`, drive it
   without a listener or port.
2. **Auth gate ×4** (§Problem 5 — the gate has zero coverage anywhere): token-set/accept,
   token-set/wrong-token, token-set/absent-header, token-unset/allow. Also pins the ordering
   guarantee that a 401 precedes any extractor, since the middleware is layered on the protected
   sub-router before `merge` (`:246-249`, `:251`).
3. **Rejection encoding** — one direct test of the 503 body, which becomes the single encoding
   replacing nine untested inline ones.
4. **Admin handlers called directly** with a hand-built `AdminState`, covering the six endpoints
   that have no test at all (the only admin test in the tree is `/admin/upgrade-status`,
   `tests/cluster_misc.rs:357`, `:431`). This is what would have caught H1 and H2.

**Mechanical note for whoever writes (1) and (2).** `tower::ServiceExt::oneshot` lives behind
tower's `util` feature, and the workspace pin is `tower = "0.5"` with **default features only**
(root `Cargo.toml:179`); there is **no `ServiceExt` usage anywhere in the tree today**, so nothing
has pulled it in. Either add `features = ["util"]` to the workspace dep (affects every consumer of
the workspace pin — the reason to mention it up front rather than discover it mid-refactor), or
avoid the trait entirely by calling `tower::Service::call` on the router with an
`axum::body::Body` request. Either is fine; the second adds no dependency surface.

FR11's improvement is different in kind: it removes a re-parse that made snapshot chaining quietly
expensive — plausibly the reason the accessor methods have zero external callers — bounded to the
34 snapshot constructions in five test files (§Problem 7). It does **not** speed up
`integration_metrics.rs`, whose 38 free-function calls are held at exactly today's cost by design.

## Spec / LOCKED impact — none

- **Locked crates.** The four locked pairs are `frogdb-txn`+`frogdb-vll`, `frogdb-persistence`+
  `frogdb-recovery`, `frogdb-replication`+`frogdb-replication-runtime`,
  `frogdb-cluster`+`frogdb-cluster-runtime` (ADRs 0002–0004). This proposal touches
  `frogdb-server` and `frogdb-telemetry` **only** — neither is locked, neither has a mutation
  gate. No `just mutants-diff` is owed, though running it on `frogdb-server` before push costs
  nothing.
- **FM tags.** Grepping `FM-[A-Z]*-[0-9]*` across the full edited file set —
  `observability_server.rs`, `telemetry/http_handlers.rs`, `telemetry/testing.rs`,
  `admin/handlers.rs` — returns **zero matches**. No failure-mode row is forced by any of these
  files, so no spec edit and no `just lint-failure-modes` change is owed.
- **Seam lints.** Verified against `agents/seam-lints.md` and the recipe list in the `Justfile`
  (`lint-gates`, `:329`):
  - **`lint-metrics-chokepoint`** ("metric emission goes through the typed handles
    `define_metrics!` generates, never a raw string-named `increment_counter`/`record_gauge`/
    `record_histogram`") — grepping those three identifiers across the edited files returns
    **zero hits**. Nothing in this proposal *emits* a metric; `handle_metrics` calls
    `recorder.encode()`, which is the read side. FR11 *parses* Prometheus text in test code,
    which the gate does not cover and should not. **Unaffected.**
  - **`lint-clock-seam`** — grepping `Instant::now` / `SystemTime::now` / `clock::` across the
    edited files returns **zero hits**. No handler reads the clock (`start_time` is threaded in
    from `subsystems.rs`, a file this proposal does not touch). **Unaffected.** Note this is the
    opposite of proposal 74's situation, which must move a clock-seam allowlist entry.
  - The remaining twelve gates concern INFO sections, redirects, pub/sub confirmations, failover
    atomicity, float formatting, typed-store unwraps, keyspace-notify routing, the script gate,
    durable acks, figment `.nested()`, error sanitisation, and continuation locks — none has a
    surface in the HTTP observability layer or in test-side metric parsing. **Unaffected.**
- **Vocabulary** (`frogdb-server/CONTEXT.md`). Prose here uses the ruled terms and avoids the
  banned ones. One recorded, deliberately-unclaimed collision: `admin/handlers.rs` emits the
  strings `"master"`/`"slave"` as JSON *values* — `/admin/role` at `:168-179`, `/admin/nodes` at
  `:213-216` (flags), `:239-243` (cluster role) and `:256`, `:262` (the **standalone** arm, which
  hard-codes `"master"` twice) — which `CONTEXT.md:128` lists under *Avoid* for new prose and
  identifiers.

  The earlier draft called these "an existing Redis-compatible wire contract". **That framing is
  too generous and is corrected here**: `CONTEXT.md:127` grants the wire-compat exemption
  narrowly, to `NodeRole`'s `Display` impl and INFO fields. `/admin/role` and `/admin/nodes` are
  **FrogDB-native HTTP endpoints with no Redis counterpart** — Redis has no such routes — so they
  are not covered by that exemption. The accurate statement is: these strings are an **existing
  FrogDB HTTP contract with one in-repo consumer** (`frogctl`, via `admin_get`), changeable only
  in lockstep with the client-side normalisation that is candidate **FR5**, owned by proposal 75.
  Same conclusion — **out of scope here** — on firmer ground, and flagged so 75's author knows the
  server side is the source.

## Behaviour changes (wire-visible), stated up front

The refactor is behaviour-preserving except for these, each of which is the *point* of the
unification rather than a side effect:

| Change | Before | After | Risk |
|---|---|---|---|
| `/admin/*` when admin disabled | `503`, empty body, no content-type | `503`, `application/json`, `{"error": "admin API not enabled"}` | Low. No test asserts the empty body; grep for `503`/`SERVICE_UNAVAILABLE` in `crates/*/tests` returns only `integration_debug_http.rs`'s bundle escape-hatches (`:741`, `:775`, …), which check the status code only. Status code is unchanged. |
| `/debug/*` when debug absent | `503`, `text/plain`, `"Debug UI not enabled"` | `503`, `application/json`, `{"error": …}` | **Unreachable in production** (§Problem 3) — `with_debug_state` is unconditional. |
| `/status/json` when collector absent | `503`, `application/json`, `{"error": "Status collector not configured"}` | same status, unified message text | **Unreachable in production** (§Problem 3). Message text changes. |

The `/admin/*` row is the only one a running node can produce today. It should be noted in the
release notes and in the docs (spec first — see H5).

**The 503-body change is proven safe for the only in-repo HTTP client, not merely assumed.** The
earlier draft rested on path-disjointness ("frogctl doesn't call those routes"), which would break
the moment it did. The stronger proof: `frogctl` reaches the admin API only through
`ConnectionContext::admin_get` (`frogctl/src/connection.rs:155-162`), which returns the raw
`reqwest::Response` without inspecting it, and **every** consumer gates on `is_success()` before
touching the body —

- `frogctl/src/commands/health.rs:365-367` — `Ok(resp) if resp.status().is_success() => resp.json()`
- `frogctl/src/commands/upgrade.rs:136-139` and `:277` — the same `is_success()` guard before
  `response.json()`

— so a 503's body is **never parsed** on any path, whatever it contains. Adding a JSON body to a
503 cannot affect `frogctl` even for routes it *does* call. Status codes are unchanged throughout,
so the guards themselves behave identically.

## Risks / scope boundaries

### Boundary vs proposal 63 (SV1, `Server` subsystem bundles) — disjoint, either order

63 partitions `Server`'s 47 fields into bundles and edits exactly three files:
`server/mod.rs`, `server/init.rs`, `server/subsystems.rs` (mechanical field retargeting). **76
edits none of them.** The public builder API of `ObservabilityServer` (`new`, `with_listener`,
`with_debug_state`, `with_status_collector`, `with_admin_state`, `with_tls`) is **unchanged by
this proposal**, so 63's `subsystems.rs:258-269` call chain compiles identically before and
after. **No ordering constraint in either direction.**

Two specific clarifications, because the lane brief conflated three similarly-named types:

- 63's bundle is `ServerTelemetry` (renamed from `ServerObservability` in its revision) with
  field `telemetry` on `Server`. It is a **field grouping inside `Server`** and never appears in
  `observability_server.rs`.
- The `ObservabilityDeps::default` that 63 cites is at `connection/deps.rs:279-292` — the
  **connection** dependency bundle, a different type in a different module, reached from
  `acceptor.rs`. 76 does not touch `deps.rs`.
- A third, pre-existing `crate::server_observability::ServerObservability` — the node's collector
  set — is the type of `ObservabilityDeps::collectors` (`deps.rs:276`) and is constructed at
  `deps.rs:290`. (An earlier draft cited `:279` for this; `:279` is the `impl Default` line.) Also
  untouched. **Three distinct types; 76 touches none of them.** If a reviewer reads "76 touches
  `ObservabilityDeps` or observability_server wiring" from the brief: it does not.

### Boundary vs proposal 67 (SV7) — the `debug_handler.rs:173` timeout is declined

67 §Risks records `connection/debug_handler.rs:173` sending `GetPubSubLimitsInfo` to
`shard_senders[0]` under a hard-coded `Duration::from_secs(5)` instead of
`self.scatter_gather_timeout`, and ruled it a **follow-up issue, not part of SV7** because it is a
different message type with its own error strings. That file is the **RESP `DEBUG` command
handler**, not the HTTP observability server; it shares no type, no module and no seam with
anything in this proposal, and proposal 74 already owns two hunks in it. **76 does not claim it.**
The ruling stands: it remains 67's follow-up issue.

### Boundary vs proposal 74 (FR3, Debug Bundle) — one shared file, read-only

74 edits `debug/src/web_ui/routes.rs` (1 hunk, `:57-64`), `web_ui/handlers.rs` (4 hunks),
`web_ui/state.rs`, the `bundle/` module, `subsystems.rs` (2 hunks), `config/mod.rs` and
`connection/debug_handler.rs`. **76 edits none of these.** The only contact point is the private
`not_found` at `routes.rs:146-151`: 76 deletes the *dead telemetry copy* and explicitly leaves
the debug copy alone. No merge conflict is possible.

### Boundary vs future proposal 79 (FR12, debug `web_ui` → axum `Router`) — deliberate hand-off

79 will convert `web_ui/routes.rs`'s ~40-arm string match (`handle_debug_request:30-111`) into an
axum `Router` and own its 404. **76 keeps entirely out of routing topology**: it adds no route,
moves no route, and does not touch the debug crate. What 76 *does* leave for 79 is the thing 79
needs — a `SubsystemUnavailable`-style `IntoResponse` type and an established `FromRequestParts`
pattern in the same server crate, so 79's 404/503 encodings can join the same seam rather than
minting a fourth. If both land, 79 should adopt 76's rejection type; if 79 lands first, 76's
extractor work is unaffected. **No ordering constraint**, but 76-then-79 is the cheaper order.

### Boundary vs proposals 72 / 73 / 75 (frogctl) — verified zero overlap

`frogctl` lives at the repo root (`frogctl/`), and `frogdb-admin` at
`frogdb-server/ops/frogdb-admin/`. This proposal touches **no file under either path**, and no
file under `frogdb-operator/`. The concurrent authors of 74 and 75 own those trees; verified
disjoint by path.

### Other risks

- **Signature change ripples in `admin/handlers.rs`.** Changing seven receivers from
  `State<SharedAdminState>` to `SharedAdminState` is source-breaking for any other caller. Grep
  confirms the only callers are the seven wrappers being deleted. Low.
- **`FromRequestParts` and route ordering.** The extractor runs *after* the bearer middleware
  (which is a `layer` on the protected sub-router, `:246-249`), so an unauthenticated request
  still gets `401` and never reaches the extractor. Preserved by construction — the middleware is
  not touched. Worth an explicit test, which §Testability adds.
- **Turmoil.** `observability_server.rs` has `#[cfg(not(feature = "turmoil"))]` on the TLS field
  and accept path only — the complete set is `:56`, `:59`, `:81`, `:83`, `:113`, `:145`, `:169`
  (an earlier draft listed five of the seven, omitting `:81` and `:83`). The wrapper tier, the
  router and every handler are unconditional; the change adds no new `cfg` and removes none.
  `just lint-turmoil-features` unaffected.
- **FR11 memory.** A parsed `Vec<MetricSample>` (a `String` name + a `HashMap<String, String>`
  per sample) is larger than the flat `String` it replaces. This is test-only code holding one
  `/metrics` payload per snapshot, and dropping `raw` (zero callers) claws some of it back; the
  trade is bounded and is made in exchange for collapsing each delta chain's 2N parses to 2 across
  34 snapshot constructions. Stated rather than hidden — and note the trade is a real cost paid
  for a modest win, which is why FR11 is **S**.
- **FR11 `fetch_metrics` duplicate is *not* folded in.** Deleting
  `test-harness/src/server.rs:883-895` in favour of `frogdb_telemetry::testing::fetch_metrics`
  would make `frogdb-test-harness` depend on the `testing` feature of `frogdb-telemetry` (which
  pulls `reqwest` + `opentelemetry_sdk/testing`). That is a dependency-graph decision, not a
  parse-once decision. Recorded as a follow-up; **out of scope**.

## Effort

| Part | Effort | Notes |
|---|---|---|
| **FR6** — 2 extractors + 1 rejection + **delete 7 admin wrappers, shrink 1 debug wrapper, keep 4 telemetry wrappers** + 7 signature changes | **M** | ~90 lines added, ~70 deleted, 4 files. The size is not the code — it is the wire-visible 503 unification (9 sites / 2 crates / 3 encodings → 1), the spec-then-page doc update (H5), and the ~7 regression tests written **first** (§Testability). Those tests are writable today; the refactor does not unlock them, it *requires* them, because it changes wire encoding. |
| **FR11** — parse-once + predicate dedup | **S** | 1 file, ~40 lines net. Public signatures unchanged, so no churn across the 43 external free-fn sites or the one external macro use. Genuinely **S** and honestly modest: the beneficiaries are the 34 snapshot constructions in five test files, not `integration_metrics.rs`. |

The two parts share **no file** and can land in either order, in either sequence with 63, 64, 67,
72, 73, 74, 75, or a future 79.

## Independently-landable hotfixes

Each is a standalone commit that does **not** wait on the refactor. LIVE/latent is called honestly,
and per §Problem 4's blast-radius note, "LIVE" here means **the documents lie**, not "a shipped
client is broken" — no in-repo caller touches `transfer-leader` or `shutdown`.

**H1 — `/admin/transfer-leader` must not return 200 (LIVE, claimed).**
`admin/handlers.rs:426-438`. The obvious fix is `Err(StatusCode::NOT_IMPLEMENTED)`, but a bare
`StatusCode` **discards the one useful thing the current response carries** — the explanatory
string "leadership transfer not yet implemented (openraft 0.9 limitation)" (`:436`), which is
exactly what an operator hitting a 501 wants to read. Preferred form:

```rust
Err((
    StatusCode::NOT_IMPLEMENTED,
    Json(json!({"error": "leadership transfer not yet implemented (openraft 0.9 limitation)"})),
))
```

— axum implements `IntoResponse` for `(StatusCode, Json<T>)`, so this is still one changed
expression and still **XS**, and it is consistent with this proposal's own FR6 thesis that a
failure should carry a typed, readable body rather than a bare status. (The return type widens
from `Result<Json<Value>, StatusCode>` to `Result<Json<Value>, (StatusCode, Json<Value>)>`, which
the wrapper at `:353-363` already handles via `into_response`.) Ships with the endpoint's first
test, and with the doc edits of H5 in the same commit. **In a file this proposal owns — claimed.**

**H2 — `/admin/shutdown` is documented but permanently 503 (LIVE, not claimed).**
The handler (`admin/handlers.rs:399-411`) is correct; the defect is that `AdminState.shutdown_tx`
is hard-wired to `None` at its one producer — the literal at **`subsystems.rs:251`**, inside the
`:241-253` construction. **Any issue filed for this must cite `:251`**: `:253` is the closing
`}))` of the `Some(Arc::new(AdminState { … }))`, and an issue pointing a fixer at a closing brace
wastes their first ten minutes. The file is owned by 63 / 64 / 74. Two honest resolutions, and the
choice is a product call rather than a refactor call: **(a)** thread the shutdown watch-sender
from `Server` and delete the TODO, or **(b)** remove the endpoint and its two website rows (plus
the corresponding spec lines). Either way, the documentation and the code must agree. **Filed as
an issue against the wiring, not folded in here** — this proposal must not edit `subsystems.rs`.

**H3 — bearer-token comparison is not constant-time, and allocates per request (LATENT,
security-adjacent, not claimed).** `observability_server.rs:262-264`. The fix is small
(pre-format the expected header once into `HttpState`, compare with a constant-time equality),
but per the standing policy that security issues are filed and parked, this is **recorded as an
issue** rather than folded into a refactor commit. The extractor work naturally re-reads this
site, so the issue should reference this proposal.

**H4 — delete dead `telemetry::http_handlers::not_found` (LATENT, claimed).** `:84-90` plus its
test `:163-167`. Zero callers, not re-exported. Pure deletion, no behaviour change; the
load-bearing private twin at `debug/src/web_ui/routes.rs:146-151` is untouched.

**H5 — the admin error shape is undocumented (LATENT, claimed alongside FR6).** The admin tables
at `website/src/content/docs/operations/clustering.md:121-127` (seven rows — `/admin/health` at
`:121` through `/admin/transfer-leader` at `:127`) and `architecture/clustering.md:647-653` list
endpoints but no error contract, so the 503-when-disabled behaviour — **the default behaviour**,
since `admin.enabled` defaults to `false` — is nowhere described. One table column, landed with
the FR6 encoding change.

**Edit order: spec first, content page second.** The content pages under
`website/src/content/docs/` are generated *from* the specs under `website/docs-spec/specs/`, which
are the single source of truth. So H1's and H5's doc work must edit, in this order:

1. `website/docs-spec/specs/operations/clustering.md:94-103` (the "Admin HTTP API — real
   endpoints" paragraph, whose `:98-99` already rules `transfer-leader` "**returns
   not-implemented** … state this honestly") and its structure bullet at `:140-142`;
2. `website/docs-spec/specs/architecture/clustering.md:89-97` (the ruling that rebuilds the admin
   table from source);
3. then the content pages `operations/clustering.md` and `architecture/clustering.md`.

Editing the page without the spec leaves the spec as the authority for a contract the page no
longer states, and the next regeneration silently reverts the fix. **Note this is discipline, not
a gate**: `website/scripts/docs-path-check.py` only verifies that repo paths cited inside code
spans and fenced blocks exist — it is path-only and would not notice a page and its spec
disagreeing about a status code.

**H6 — `AdminConfig`'s doc comment contradicts its own struct (LATENT, not claimed).**
`config/src/admin.rs:10-11` claims every field carries `#[param(skip)]` and is not a CONFIG
GET/SET parameter. All three carry `#[param(name = …)]` instead (`:18`, `:23`, `:28`) and all
three are registered in the golden table (`config/src/params.rs:383`, rows at `:1061`, `:1068`,
`:1075`, each `mutable: false`). §Problem 8. One-line documentation fix in a crate this proposal
does not edit — **issue candidate only**.
