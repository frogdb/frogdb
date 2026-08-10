# Proposal 76 — The observability HTTP surface: one extractor, one rejection, one parse

Round 38 · lane: frogctl / operator / telemetry · candidates **FR6 + FR11** · effort **M** (FR6)
+ **S** (FR11), independently landable · **no locked crate**, no FM tag, no seam-lint touched

**Verified at HEAD `4372082285b34079ae6c1eb0c2d135a55d91ca83`** (worktree `arch-round-38-99`,
branch `main`). The lane brief was written against `08c143d6`; every path, line number and count
below was re-derived by reading the tree at this SHA. **Two brief claims are corrected**:

| Brief claim | Correction |
|---|---|
| "dup `not_found` (http_handlers.rs:83-89 pub dead + **routes.rs:145-151** private)" | The private twin is at `debug/src/web_ui/routes.rs:**146-151**`, and that file belongs to **proposal 74** (1 hunk) and future **79**. This proposal deletes only the telemetry copy and says so. |
| FR6 is "**Latent**" | Half right. The *duplication* is latent. Two **LIVE** defects were found in the same file set that the brief did not name: `/admin/transfer-leader` returns **200 OK** carrying an error body while both its own doc comment and the published docs say it errors, and `/admin/shutdown` is **permanently 503** while the website documents it as working. Neither is caused by the duplication; both are hidden by it. |

Three further findings the brief did not name: `HttpState`'s `Option`-ness is a **type-level lie
for two of its three optional fields**, the bearer-token gate has **zero tests anywhere in the
tree**, and `MetricsSnapshot` (FR11) has **zero external callers of its accessor methods** —
which changes what the FR11 fix has to preserve.

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
  subsystem is absent" is currently decided in **nine places with three different wire
  encodings** (bare 503 empty-body ×7, `text/plain "Debug UI not enabled"` ×1, and — in a
  different crate — `application/json {"error": …}` ×1). One extractor plus one
  `IntoResponse` rejection type makes that one decision with one encoding, and makes the
  router unit-testable for the first time.
- **FR11 — `telemetry/testing.rs` parses the same string N times.** `MetricsSnapshot` stores
  `raw: String` (`:462`) and every accessor delegates to a free function that calls
  `parse_prometheus` from scratch (`:517-537` → `:256-261` → `:64`). A `MetricsDelta` chain of
  two assertions re-parses the whole `/metrics` payload — 95 metric families, several of them
  per-command or per-shard labelled — **four** times. The **duplication** the brief names is
  real but is not two APIs: the four `MetricsSnapshot` methods have **zero callers outside
  `testing.rs`** (verified), so they are a private delegation layer pointing the wrong way.
  Invert it — parse once in `MetricsSnapshot::new`, make the free functions the thin edge —
  and the parse-once falls out for free. A third copy of the label-match predicate
  (`:311-316`, duplicating `:232-237`) folds in.

Neither change alters a production code path's behaviour except where explicitly listed under
§Behaviour changes, and the two LIVE defects are carved out as **independently-landable
hotfixes** that do not wait on the refactor.

## Files involved

| Path | Lines | Role in this change |
|---|---|---|
| `frogdb-server/crates/server/src/observability_server.rs` | 363 | **Primary (FR6).** `HttpState` `:37-44`; `create_router` `:221-252`; `bearer_auth_middleware` `:256-271`; the wrapper tier `:273-363` (**deleted**, replaced by extractors + rejection). 9 commits of churn. |
| `frogdb-server/crates/server/src/admin/handlers.rs` | 438 | **Primary (FR6).** Seven handler signatures change `State(state): State<SharedAdminState>` → `state: SharedAdminState`; `transfer_leader` `:426-438` is the hotfix site. 5 commits. |
| `frogdb-server/crates/telemetry/src/http_handlers.rs` | 168 | **Primary (FR6).** Six `Response::builder()…unwrap()` chains, **8 `.unwrap()` in the 91 non-test lines**; `handle_status_json`'s `None` arm `:73-79`; **`not_found` `:84-90` deleted** (zero callers). 4 commits. |
| `frogdb-server/crates/telemetry/src/testing.rs` | 1150 | **Primary (FR11).** `MetricSample` `:33-39`; `parse_prometheus` `:64-81`; `find_metric` `:221-241`; free fns `:256-332`; `MetricsSnapshot` `:460-537`; `MetricsDelta` `:556-723`; `fetch_metrics` `:474-487`. 1 commit — never revisited since it was written. |
| `frogdb-server/crates/telemetry/src/lib.rs` | — | **Primary (FR11/FR6).** Re-export list `:49-51` (note: `not_found` is *not* re-exported — reachable only as `http_handlers::not_found`). |
| `frogdb-server/crates/telemetry/Cargo.toml` | — | Read-only evidence. **No `axum` dependency** — the constraint that shapes §Proposed change. |
| `frogdb-server/crates/server/src/server/subsystems.rs` | 930 | **Read-only evidence, must NOT be edited.** `ObservabilityServer` assembly `:258-270`; `shutdown_tx: None` `:253`; `admin_state` gate `:240-256`. Owned by **63/64/74**. |
| `frogdb-server/crates/config/src/http.rs` | — | Read-only. `HttpConfig.token` `:31-35` (`#[param(skip)]`, security-annotated); the `0.0.0.0` warning `:85`. |
| `frogdb-server/crates/config/src/admin.rs` | — | Read-only. `AdminConfig.enabled` `:17-19` — `#[serde(default)]` on `bool`, i.e. **default `false`**. |
| `frogdb-server/crates/debug/src/web_ui/routes.rs` | 208 | **Read-only, must NOT be edited.** The private `not_found` twin `:146-151`. Owned by 74 + future 79. |
| `frogdb-server/crates/test-harness/src/server.rs` | — | Read-only evidence (FR11). `fetch_metrics` `:883-895` — a near-duplicate of `testing.rs:474-487`. |
| `frogdb-server/crates/server/tests/cluster_misc.rs` | — | The **only** admin-endpoint test in the tree: `/admin/upgrade-status` `:357`, `:431`. |
| `frogdb-server/crates/server/tests/integration_metrics.rs` | 1166 | FR11 call sites (the bulk of the 47). |
| `website/src/content/docs/operations/clustering.md` | — | Read-only. Admin endpoint table `:122-127` — the doc side of hotfix H1/H2. |

New file: `frogdb-server/crates/server/src/observability_server/extract.rs` (~90 lines) — or an
inline module; see §Proposed change.

## Problem

### 1. Twelve handlers, zero decisions (FR6)

The router registers **16 routes** resolving to **13 handler functions** (`:221-252`). Twelve of
those thirteen are in the block `:273-363` and every one of them is a pass-through. Census:

| Group | Sites | Shape | Lines |
|---|---|---|---|
| Telemetry pass-through | 4 (`:277`, `:281`, `:285`, `:289`) | `State(s)` → `handle_x(s.field)` | 12 |
| Debug pass-through | 1 (`:293-303`) | `Option` unwrap + hand-built 503 | 11 |
| Admin projection | 7 (`:308`, `:314`, `:323`, `:329`, `:335`, `:341`, `:353`) | `s.admin_state.ok_or(SERVICE_UNAVAILABLE)?` → delegate → `into_response()` | 56 |

The seven admin wrappers differ **only in the delegate's name**, plus one accident: two of them
(`:317-320`, `:347-350`) spell the identity re-wrap out longhand —

```rust
match result {
    Ok(json) => Ok(json.into_response()),
    Err(status) => Err(status),
}
```

— which is `result.map(IntoResponse::into_response)`. The other five use the one-liner. Same
operation, two spellings, in the same 50-line block. That is the signature of a tier nobody reads
as a whole.

The file's doc comment names the reason (`:274-275`): "These wrap the existing
framework-agnostic handlers from `frogdb_telemetry` and `frogdb_debug`, which return
`Response<Full<Bytes>>`." **The stated reason does not hold for the admin seven** — those
delegate to `crate::admin::handlers`, which is already axum-native (`axum::{Json, extract::State,
http::StatusCode}`, `admin/handlers.rs:5`). The admin tier wraps axum in axum.

### 2. "Subsystem absent" is decided nine times, encoded three ways

| Site | Condition | Wire result |
|---|---|---|
| `observability_server.rs:309,315,324,330,336,345,357` (×7) | `admin_state == None` | `503`, **empty body**, no content-type |
| `observability_server.rs:297-301` | `debug_state == None` | `503`, `text/plain`, `"Debug UI not enabled"` |
| `telemetry/http_handlers.rs:73-79` | `status_collector == None` | `503`, `application/json`, `{"error": "Status collector not configured"}` |

Three encodings of one condition. A client that wants to distinguish "this node has no admin API"
from "this node is unhealthy" gets an empty 503 in the first case and must parse a body in the
third. There is no type carrying the concept, so there is no place the third encoding could have
been made to agree with the first.

### 3. `HttpState`'s `Option`s are a type-level lie for two of three fields

`HttpState` (`:37-44`) declares `debug_state: Option<_>`, `status_collector: Option<_>`,
`admin_state: Option<_>`. Reading the sole construction path (`subsystems.rs:258-270`, inside
`if let Some(ref prometheus) = self.prometheus_recorder` at `:171`):

```rust
let mut server = ObservabilityServer::new(http_config, prometheus.clone(), self.health_checker.clone())
    .with_listener(http_listener)
    .with_debug_state(debug_state)              // :264 — unconditional
    .with_status_collector(status_collector.clone());  // :265 — unconditional
if let Some(admin_state) = admin_state {        // :268 — genuinely conditional
    server = server.with_admin_state(admin_state);
}
```

`debug_state` is built unconditionally at `:210-222` and `status_collector` at `:151-168`. So in
production **`debug_state` and `status_collector` are always `Some`** — the 503 arms at
`observability_server.rs:297-302` and `http_handlers.rs:73-79` are unreachable outside a
hypothetical second caller. Only `admin_state` is truly optional, gated on `config.admin.enabled`
(`subsystems.rs:241`), which **defaults to `false`** (`config/src/admin.rs:17-19`: `#[serde(default)]`
on a `bool`). So on a default build, all seven admin routes return the empty 503 and there is no
body explaining why.

Three `Option`s, one meaning "configurable", two meaning "I didn't want to thread the type". The
wrapper tier is the tax on not distinguishing them.

### 4. Two LIVE defects the duplication hides

**H1 — `/admin/transfer-leader` reports success for an unimplemented operation.** Its own doc
comment says (`:423-425`) "This endpoint currently returns 501." The body returns
`Ok(Json(json!({"status": "error", …})))` (`:434-437`) — HTTP **200 OK**. The published docs
agree with the comment, not the code: `website/…/operations/clustering.md:127` says
"**Not implemented** — returns an error." Any client that checks the status code and not the body
— which is the normal thing to do, and what `frogctl`/an operator would do — records a successful
leadership transfer that never happened. **LIVE.** One line.

**H2 — `/admin/shutdown` is permanently 503.** `AdminState.shutdown_tx` is set to `None` at its
one construction site with the reason inline: `shutdown_tx: None, // TODO: wire up shutdown
channel from Server` (`subsystems.rs:253`). The handler's `else` branch is therefore the only
reachable one (`admin/handlers.rs:405-410`). Meanwhile both doc pages promise the endpoint works:
`operations/clustering.md:126` "Trigger a graceful shutdown", `architecture/clustering.md:652`
"Graceful shutdown". **LIVE**, but the fix line is in `subsystems.rs` — a file this proposal must
not edit (see §Boundaries). Carved out as an issue, not claimed.

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

| Operation | Full parses |
|---|---|
| one `get_counter(&text, …)` | 1 |
| `MetricsDelta::assert_counter_increased` | 2 |
| the two-link chain at `tests/timeseries.rs:102-104` | 4 |
| `tests/integration_metrics.rs` (47 free-fn sites across the file's tests) | 47 |

The **shape on disk is not the one the brief assumed.** Grepping for the accessor methods
(`.counter(`, `.gauge(`, `.histogram_count(`, `.histogram_sum(`) outside `testing.rs` returns
**zero hits**. `MetricsSnapshot`'s methods are consumed only by `MetricsDelta` inside the same
file. So this is not "two public APIs that must both be kept" — it is one public API (the free
functions + the eight `assert_*!` macros, 9 macro uses, 47 free-fn calls, 5 test files using
`MetricsSnapshot`/`MetricsDelta`) sitting on a private delegation that points from the type that
*could* cache toward the function that *cannot*.

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

## Proposed change

### FR6 — one extractor, one rejection, and stop wrapping axum in axum

The constraint that shapes everything: **`frogdb-telemetry` does not depend on axum**
(`telemetry/Cargo.toml`) and should not start. The `frogdb_telemetry` handlers are sync functions
returning `hyper::Response<Full<Bytes>>` — which axum 0.8 already accepts as a response body, so
they are *already* `IntoResponse`-compatible; they are simply not `Handler`s (wrong arity, not
async). So the design splits cleanly:

**(a) An extractor for the projections that can fail.** A `FromRequestParts<HttpState>` impl for
`SharedAdminState` and for `Arc<DebugState>`, whose `Rejection` is one type:

```rust
/// The one wire encoding of "this node does not have that subsystem installed".
pub struct SubsystemUnavailable(&'static str);   // the subsystem's name

impl IntoResponse for SubsystemUnavailable { /* 503 + application/json + {"error": …} */ }
```

Nine sites, three encodings → one impl, one encoding. This is the **seam**: after it exists,
"absent subsystem" cannot be spelled a fourth way without going around a type that every
observability route already names.

With it, the seven admin handlers change their receiver from `State(state):
State<SharedAdminState>` to bare `state: SharedAdminState`, and the router references them
directly:

```rust
.route("/admin/health",  get(admin_handlers::health))
.route("/admin/cluster", get(admin_handlers::cluster_state))
…
```

All **seven wrappers delete** (56 lines), along with both longhand identity re-wraps. The debug
wrapper keeps its `Uri` argument but loses its hand-built 503 (11 → 4 lines).

**(b) The four telemetry pass-throughs stay — and this proposal says so rather than pretending
otherwise.** An extractor for `Arc<PrometheusRecorder>` / `HealthChecker` is *infallible*, so it
buys no error unification; and because `handle_metrics` and friends are sync free functions
taking owned values, a three-line async wrapper is still needed either way. Replacing
`State(s) → handle_metrics(s.recorder)` with `recorder: PrometheusRecorderOf<HttpState> →
handle_metrics(recorder.0)` is the same three lines plus an impl. **Rejected on the deletion
test** (below). The brief's "`FromRequestParts<HttpState> for SharedAdminState`" was the right
half; the generalisation to all of `HttpState` is not.

**(c) `handle_status_json`'s `None` arm and its private JSON error encoding
(`http_handlers.rs:73-79`) move behind the same rejection** by changing the signature to
`handle_status_json(collector: Arc<StatusCollector>)` — non-optional, matching the fact that it
is always `Some` (§Problem 3) — and letting the server-side extractor produce the absence case.
The telemetry crate loses an `Option` it never had a reason to hold and one `Response::builder()`
chain; it gains no dependency.

**(d) `not_found` (`http_handlers.rs:84-90`) and its test delete.** Zero callers.

#### Depth and locality

The extractor is a **shallow module by line count and a deep one by decision count**: ~90 lines
that own the single decision "what does the wire see when a subsystem is not installed", for
every current and future observability route. Locality improves in the direction that matters —
today, changing that answer means editing two crates and three encodings; after, one impl. The
adapter tier does not move elsewhere; it **ceases to be a tier**, because the two things it was
adapting (axum state shape, absent-subsystem encoding) become one extractor and one type.

#### Deletion test, applied honestly

- **`SubsystemUnavailable` + the two extractor impls** — delete them and the `ok_or(503)` triple
  reappears **eight** times across two crates, and there is again no place for the three
  encodings to agree. **Earns its keep.**
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
`server.fetch_metrics()`, never a snapshot). `new(raw: String)` keeps its signature — its 12 test
call sites are unchanged — and simply parses and drops the string. Whether `raw()` is deleted or
re-implemented as a re-render is a one-line call; deletion is preferred, and it is the same class
of pure deletion as H4.

The four accessors read `self.samples` directly. The free functions keep their **exact public
signatures** (`&str` in) and become the thin edge, delegating through the same private
`(samples, name, labels)` lookup the methods use — `parse_prometheus(text)` once, then
`lookup(&samples, …)`, i.e. exactly today's cost for a one-shot call and no payload clone — so
all **47 call sites and 9 macro uses compile unchanged**, and the eight `assert_*!` macros
(which expand to `$crate::testing::get_counter(…)`) are untouched. A `MetricsDelta` chain of N
assertions goes from 2N parses to **2**, because the parses already happened in the two
`MetricsSnapshot::new` calls the tests already write.

The label-match predicate moves to one place — `impl MetricSample { fn matches(&self, name: &str,
labels: &[(&str, &str)]) -> bool }` — consumed by `find_metric` and by
`get_histogram_buckets`'s filter, deleting the second copy.

`find_metric`'s first-match-wins stays as-is (changing it is a behaviour change to a test API with
47 call sites and belongs in its own change), but the parsed form makes the fix *available*: with
`samples` owned by the type, a `#[track_caller]` uniqueness assertion or a `sum` variant is a
method away rather than a re-parse away. Recorded as a follow-up, not folded in.

#### Deletion test

Delete `MetricsSnapshot` and the free functions still work — but every `MetricsDelta` user
re-writes the before/after pairing by hand and the parse-once is gone. Delete the free functions
and 47 call sites plus 8 macros break. Both earn their keep; the change is which one owns the
data.

## Testability improvement

The FR6 change moves the observability surface from **untestable-by-construction** to
unit-testable, and the evidence that this matters is stark:

- **`HttpState` has exactly one construction site in the entire tree** —
  `observability_server.rs:134-141`, inside `ObservabilityServer::run`, after binding a socket.
  No test can build a router without starting a server on a port.
- **Six of the seven admin routes have zero tests.** The only admin test in the tree is
  `/admin/upgrade-status` (`tests/cluster_misc.rs:357`, `:431`). `/admin/health`, `/admin/cluster`,
  `/admin/role`, `/admin/nodes`, `/admin/shutdown`, `/admin/transfer-leader`: nothing. This is why
  H1 and H2 have survived.
- **The bearer gate has zero tests** (§Problem 5) — no test in `frogdb-server/crates` sets
  `http.token`.

After the change:

1. `create_router(state)` is callable from a unit test with a hand-built `HttpState` (the
   extractors, not the handlers, are what needed the state's shape), driven via
   `tower::ServiceExt::oneshot` — no listener, no port, no `#[tokio::test]` server boot. axum 0.8
   + `tower` are already workspace dependencies of `crates/server`.
2. The **first tests for the auth gate** become cheap: token-set/accept, token-set/reject,
   token-set/absent-header, token-unset/allow — four `oneshot` calls. Worth writing in the same
   change; the gate is a security boundary with no coverage.
3. `SubsystemUnavailable` gets one direct test for its encoding, replacing nine untested
   inline encodings.
4. The seven admin handlers, once they take `state: SharedAdminState` instead of
   `State<SharedAdminState>`, are **plain async fns over a plain `Arc`** — callable directly in a
   unit test with a hand-built `AdminState`, no axum at all. That is the cheapest route to
   covering the six untested endpoints, and it is available immediately after the signature
   change.

FR11's improvement is different in kind: it removes a **wall-clock tax from every metrics
assertion in the suite** (integration_metrics.rs alone: 47 parses of a multi-hundred-line payload
→ at most a handful), and it removes a re-parse that made snapshot chaining quietly expensive
enough to discourage — which is the likely reason the accessor methods have zero external callers.

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
  strings `"master"`/`"slave"` as JSON *values* (`:168-179` for `/admin/role`, `:213-243` for
  `/admin/nodes`), which `CONTEXT.md:128` lists under *Avoid* for new prose and identifiers.
  These are an existing Redis-compatible **wire contract**, not new identifiers, and the
  client-side normalisation of exactly these fields is candidate **FR5**, owned by proposal 75.
  Renaming them is a coordinated server+client change and is explicitly **out of scope here** —
  flagged so 75's author knows the server side is the source.

## Behaviour changes (wire-visible), stated up front

The refactor is behaviour-preserving except for these, each of which is the *point* of the
unification rather than a side effect:

| Change | Before | After | Risk |
|---|---|---|---|
| `/admin/*` when admin disabled | `503`, empty body, no content-type | `503`, `application/json`, `{"error": "admin API not enabled"}` | Low. No test asserts the empty body; grep for `503`/`SERVICE_UNAVAILABLE` in `crates/*/tests` returns only `integration_debug_http.rs`'s bundle escape-hatches (`:741`, `:775`, …), which check the status code only. Status code is unchanged. |
| `/debug/*` when debug absent | `503`, `text/plain`, `"Debug UI not enabled"` | `503`, `application/json`, `{"error": …}` | **Unreachable in production** (§Problem 3) — `with_debug_state` is unconditional. |
| `/status/json` when collector absent | `503`, `application/json`, `{"error": "Status collector not configured"}` | same status, unified message text | **Unreachable in production** (§Problem 3). Message text changes. |

The `/admin/*` row is the only one a running node can produce today. It should be noted in the
release notes and in `website/…/operations/clustering.md`'s admin table, which currently
documents no error shape at all.

## Risks / scope boundaries

### Boundary vs proposal 63 (SV1, `Server` subsystem bundles) — disjoint, either order

63 partitions `Server`'s 47 fields into bundles and edits exactly three files:
`server/mod.rs`, `server/init.rs`, `server/subsystems.rs` (mechanical field retargeting). **76
edits none of them.** The public builder API of `ObservabilityServer` (`new`, `with_listener`,
`with_debug_state`, `with_status_collector`, `with_admin_state`, `with_tls`) is **unchanged by
this proposal**, so 63's `subsystems.rs:258-270` call chain compiles identically before and
after. **No ordering constraint in either direction.**

Two specific clarifications, because the lane brief conflated three similarly-named types:

- 63's bundle is `ServerTelemetry` (renamed from `ServerObservability` in its revision) with
  field `telemetry` on `Server`. It is a **field grouping inside `Server`** and never appears in
  `observability_server.rs`.
- The `ObservabilityDeps::default` that 63 cites at `connection/deps.rs:279-293` is the
  **connection** dependency bundle — a different type, in a different module, reached from
  `acceptor.rs`. 76 does not touch `deps.rs`.
- A third, pre-existing `crate::server_observability::ServerObservability` (referenced at
  `deps.rs:279`) is the node's collector set. Also untouched. **Three distinct types; 76 touches
  none of them.** If a reviewer reads "76 touches `ObservabilityDeps` or observability_server
  wiring" from the brief: it does not.

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
- **Turmoil.** `observability_server.rs` has `#[cfg(not(feature = "turmoil"))]` on the TLS accept
  loop only (`:56-60`, `:113`, `:145`, `:169`). The wrapper tier and router are unconditional;
  the change adds no new `cfg`. `just lint-turmoil-features` unaffected.
- **FR11 memory.** A parsed `Vec<MetricSample>` (a `String` name + a `HashMap<String, String>`
  per sample) is larger than the flat `String` it replaces. This is test-only code holding one
  `/metrics` payload per snapshot, and dropping `raw` (zero callers) claws some of it back;
  the trade is bounded and is made in exchange for removing up to 47 re-parses per test file.
  Stated rather than hidden.
- **FR11 `fetch_metrics` duplicate is *not* folded in.** Deleting
  `test-harness/src/server.rs:883-895` in favour of `frogdb_telemetry::testing::fetch_metrics`
  would make `frogdb-test-harness` depend on the `testing` feature of `frogdb-telemetry` (which
  pulls `reqwest` + `opentelemetry_sdk/testing`). That is a dependency-graph decision, not a
  parse-once decision. Recorded as a follow-up; **out of scope**.

## Effort

| Part | Effort | Notes |
|---|---|---|
| **FR6** — extractors + rejection + delete 8 wrappers + signature change | **M** | ~90 lines added, ~70 deleted, 4 files. The size is not the code — it is the wire-visible 503 unification, the doc update, and the ~8 new unit tests (router `oneshot`, auth gate ×4, rejection encoding, admin handlers direct) that the change makes possible for the first time. |
| **FR11** — parse-once + predicate dedup | **S** | 1 file, ~40 lines net. Public signatures unchanged, so no call-site churn across the 47 sites + 9 macro uses. |

The two parts share **no file** and can land in either order, in either sequence with 63, 64, 67,
72, 73, 74, 75, or a future 79.

## Independently-landable hotfixes

Each is a standalone commit that does **not** wait on the refactor. LIVE/latent is called
honestly.

**H1 — `/admin/transfer-leader` must not return 200 (LIVE, claimed).**
`admin/handlers.rs:426-438`. Return `Err(StatusCode::NOT_IMPLEMENTED)` — which is what the
function's own doc comment at `:423-425` already claims it does and what
`website/…/operations/clustering.md:127` documents. One line changed, plus the
`Result<Json<Value>, StatusCode>` return type is already correct for it. Add the endpoint's first
test. **In a file this proposal owns — claimed.**

**H2 — `/admin/shutdown` is documented but permanently 503 (LIVE, not claimed).**
The handler (`admin/handlers.rs:399-411`) is correct; the defect is that
`AdminState.shutdown_tx` is hard-wired to `None` at its one construction site
(`subsystems.rs:253`), a file owned by 63 / 64 / 74. Two honest resolutions, and the choice is a
product call rather than a refactor call: **(a)** thread the shutdown watch-sender from `Server`
and delete the TODO, or **(b)** remove the endpoint and its two website rows. Either way, the
documentation and the code must agree. **Filed as an issue against the wiring, not folded in
here** — this proposal must not edit `subsystems.rs`.

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
at `website/src/content/docs/operations/clustering.md:122-127` and
`architecture/clustering.md:647-653` list endpoints but no error contract, so the 503-when-disabled
behaviour — the default behaviour, since `admin.enabled` defaults to `false` — is nowhere
described. One table column, landed with the FR6 encoding change.
