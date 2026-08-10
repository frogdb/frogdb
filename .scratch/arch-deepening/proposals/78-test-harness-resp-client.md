# 78 — Generic `RespClient<S, C>` and a `TestServerConfig` that embeds the real `Config`

**Candidates:** FR9 (S) + FR10 (M/L) · **Crates:** `frogdb-test-harness` (unlocked),
`frogdb-server` tests + `src/migrate.rs` (unlocked), `frogdb-protocol` (unlocked, additive) ·
**Order:** independent of 72/73/74/75/76; see [Sibling boundaries](#sibling-boundaries)

All paths, line numbers and counts below were re-derived against the working tree at
**HEAD `50118a53`** ("arch-deepening: revise proposal 69 (config-param-combinators)"). The two
commits between the lane brief's basis and this HEAD (`ec777993`, `50118a53`) touch
`.scratch/arch-deepening/proposals/*.md` only — `git diff --stat 8ea113a5 HEAD` shows two
markdown files and nothing else — so every code citation is current. Where the lane brief and
the tree disagree, the tree wins and the correction is stated inline.

## Summary

Two independent defects share one file.

**FR9.** The lane brief says "3 near-identical RESP clients + a 4th in `migrate.rs`". The tree
says **seven**. Beyond `TestClient` / `Resp3TestClient` / `TlsTestClient` in the harness and
`MigrateClient` in production `migrate.rs`, there is a fifth `Resp2Client` in
`tests/resp3.rs:31-56`, a sixth `RespConn` + `parse_resp_value` in `tests/simulation.rs:4336-4428`,
and **two hand-rolled byte-level RESP2 parsers** — `workload_runner.rs:665-756` and
`pubsub_runner.rs:233-347` — that re-implement `find_crlf`/`parse_at` from scratch because they
run under turmoil. `frame_to_response` exists **verbatim twice** (`server.rs:1343-1354`,
`migrate.rs:357-368`), and `simulation.rs` carries **six** separately-defined nested
`async fn round_trip` bodies (`:3197, :3412, :3572, :3737, :4015, :4179`).

The duplication has a legible cause, and it is not laziness. There are two axes of variation —
**stream type** (`tokio::net::TcpStream` / `turmoil::net::TcpStream` /
`tokio_rustls::client::TlsStream`) × **codec** (`Resp2` / `Resp3`) — and no generic to carry them,
so the matrix got written out by hand, partially, six ways. `turmoil::net::TcpStream` implements
tokio's `AsyncRead`/`AsyncWrite` (`turmoil-0.7.1/src/net/tcp/stream.rs:433,457`), so a single
`RespClient<S: AsyncRead + AsyncWrite + Unpin, C: Encoder + Decoder>` covers **all six cells** and
the shared type needs zero turmoil knowledge.

**FR10.** `TestServerConfig` (`test-harness/src/server.rs:43-225`) is a **flat, 51-field,
`Option`-everything mirror** of the sectioned `frogdb_config::Config`, hand-maintained alongside a
58-line manual `impl Clone` (`:227-284`), a **319-line** `if let Some(v) = … { config.a.b = v }`
translation block (`:429-747`), and 16 constructors. This is exactly the disease proposal 72
diagnoses in `frogctl` — *a second, hand-written copy of the schema that drifts against the real
one* — with the same symptom class already present: a **dead write** (`replication_role` set at
`:994`, read only on a branch that cannot be reached from there), a **producerless knob**
(`tls_handshake_timeout_ms`, read at `:665`, set by nobody), and **no test anywhere that parses a
TOML config file**, because the harness sets `config_source_path` without ever invoking
`ConfigLoader`.

The turmoil half of the same test tree already does the right thing:
`tests/common/sim_helpers.rs:145-176` builds a **real `Config` struct literal** with
`..Default::default()`. The fix for FR10 is to make the non-turmoil harness agree with its own
sibling.

Neither change touches a locked crate. The **spec-binding risk is real but narrow**, and is
discharged in [its own subsection](#the-riskiest-surface-failure-mode-spec-bindings).

## Files involved

| Path | Lines | Role in this proposal |
|---|---:|---|
| `frogdb-server/crates/test-harness/src/server.rs` | 1485 | **Primary.** `TestServerConfig` (FR10) + 3 clients + `frame_to_response` (FR9) |
| `frogdb-server/crates/test-harness/src/cluster_harness.rs` | 1326 | Read-mostly. Verified **facade** over `TestClient`; sole setter for 9 knobs |
| `frogdb-server/crates/test-harness/src/lib.rs` | 5 | Module list; gains one `pub mod` if the client moves |
| `frogdb-server/crates/server/src/migrate.rs` | 524 | **Production** code. `MigrateClient` `:232-354`; duplicate `frame_to_response` `:357-368` |
| `frogdb-server/crates/server/tests/resp3.rs` | 926 | 5th client `Resp2Client` `:31-56` |
| `frogdb-server/crates/server/tests/simulation.rs` | 6880 | 6th client `RespConn` `:4336-4428`; 6× `round_trip`; 108 single-`read()` sites |
| `frogdb-server/crates/server/tests/common/workload_runner.rs` | 856 | Hand-rolled RESP2 parser `:665-756` (turmoil-gated) |
| `frogdb-server/crates/server/tests/common/pubsub_runner.rs` | 386 | Hand-rolled RESP2 parser `:233-347` (turmoil-gated) |
| `frogdb-server/crates/server/tests/common/sim_helpers.rs` | 523 | `encode_command`/`parse_simple_response`/`parse_resp_array`; **`real_frogdb_server :145-176` is the FR10 exemplar** |
| `frogdb-server/crates/server/tests/common/test_server.rs` | 3 | `pub use frogdb_test_harness::server::*;` — the alias every test imports |
| `frogdb-server/crates/server/tests/common/mod.rs` | 28 | Feature gating; `pub mod test_server;` is ungated |
| `frogdb-server/crates/protocol/src/response.rs` | 1770 | Owns `WireResponse::to_resp2_frame` `:274`; **the inverse is missing** — proposed home for `frame_to_response` |
| `frogdb-server/crates/config/src/lib.rs` | — | `Config` `:88-228`: 27 sections + `config_source_path` |
| `scripts/failure-modes.py` | — | The spec↔test lint whose bindings constrain any rename |

Blast radius: **36 files** in `frogdb-server/` mention `TestServerConfig`, **plus
`frogdb-operator/tests/integration.rs`** — a separate workspace that `just check`/`just test` do
not build (`frogdb-operator/Cargo.toml:38` path-depends on `frogdb-test-harness`). `server.rs` has
37 commits, 15 of them since 2026-06-01 — this is a hot file, which is both the argument for
fixing it and the argument for landing the fix in reviewable slices.

## Problem

### FR9 — the client matrix, written out by hand

#### The two axes

| | `Resp2` codec | `Resp3` codec | raw bytes |
|---|---|---|---|
| `tokio` `TcpStream` | `TestClient` (server.rs:1148) · `Resp2Client` (resp3.rs:31) · `MigrateClient` (migrate.rs:232) | `Resp3TestClient` (server.rs:1225) | — |
| `tokio_rustls` `TlsStream` | `TlsTestClient` (server.rs:1403) | *(missing)* | — |
| `turmoil` `TcpStream` | *(no codec used)* | *(missing)* | `RespConn` (simulation.rs:4336) · `workload_runner.rs:665` · `pubsub_runner.rs:233` |

Six of nine cells are populated by six independent implementations; the three empty cells are
capabilities the harness simply does not have.

#### The plumbing is identical; only the semantics differ

`TestClient::command` (`server.rs:1155-1174`) and `TlsTestClient::command`
(`server.rs:1458-1472`) are the same eleven lines — build `BytesFrame::Array` of `BulkString`,
`framed.send`, `timeout(Duration::from_secs(15), framed.next())`, `.expect("timeout")
.expect("connection closed") .expect("frame error")`, `frame_to_response`. The **only** difference
is the `S` in `Framed<S, Resp2>`.

The asymmetry that costs is what `TlsTestClient` *lacks*. `TestClient` has five methods —
`command`, `command_raw` (`:1176-1192`), `send_only` (`:1195-1202`), `read_response`
(`:1205-1210`), `read_message` (`:1214-1216`). `TlsTestClient` has **one**. A TLS test that wants
to `send_only` cannot; nothing about TLS makes `send_only` hard, the copy just stopped at one
method. That is duplication charging interest in missing capability rather than in extra lines.

**Honest correction to the brief:** `Resp3TestClient` is *not* near-identical.
`command` (`:1245-1294`) carries genuine RESP3 push-filtering — it inspects `Resp3Frame::Push`,
matches the first element against the `(p|s)?(un)?subscribe` confirmation family, buffers
non-confirmations into `pending_pushes` and loops. That logic is protocol semantics, not
plumbing, and it must survive the refactor **as a layer on top of** the generic client, not be
flattened into it. Any proposal that claims "three near-identical clients, collapse to one" is
wrong about this file. The generic factors the transport; RESP3 push-filtering stays an adapter.

#### `frame_to_response` exists twice, verbatim

`test-harness/src/server.rs:1343-1354` and `migrate.rs:357-368` are byte-identical:

```rust
pub fn frame_to_response(frame: BytesFrame) -> Response {
    match frame {
        BytesFrame::SimpleString(s) => Response::Simple(s),
        BytesFrame::Error(e) => Response::Error(e.into_inner()),
        BytesFrame::Integer(n) => Response::Integer(n),
        BytesFrame::BulkString(b) => Response::Bulk(Some(b)),
        BytesFrame::Null => Response::Bulk(None),
        BytesFrame::Array(items) => {
            Response::Array(items.into_iter().map(frame_to_response).collect())
        }
    }
}
```

One copy is `pub` in a test crate; the other is private in **production** `migrate.rs`. Both
implement the exact inverse of a function `frogdb-protocol` already owns:
`WireResponse::to_resp2_frame` (`protocol/src/response.rs:274`). The crate that defines the
mapping in one direction does not define it in the other, so both consumers wrote it themselves.
That is a **missing seam**, not a missing helper.

#### The turmoil parsers, and why they exist

`simulation.rs:4336-4341` states the cause in its own doc comment:

> *Minimal RESP2 client for the replication sims: frames complete replies across arbitrary TCP
> chunking (the one-`read()` helpers elsewhere in this file assume single-segment replies, which
> turmoil does not guarantee).*

So the file **knows** the 108 single-`read()` call sites and the 97 `parse_simple_response` calls
are unsound under a chunking network, and the response was to add a seventh client next to them
rather than to fix them. `workload_runner.rs:688-756` and `pubsub_runner.rs:249-347` then wrote
the same buffered `find_crlf`/`parse_at` loop twice more, differing only in the value type they
parse into (`OperationResult` vs `RespVal`) — the same reason the harness has three clients, one
level down.

This is the **latent-correctness** half of FR9. Every remaining single-`read()` site is a test
that passes because turmoil happened to deliver the reply in one segment. Nothing pins that.

#### `round_trip`, six times

`simulation.rs` defines a nested `async fn round_trip` at `:3197, :3412, :3572, :3737, :4015,
:4179`. Six copies of "write a command, read one segment, string-compare". They are not shared
because each is defined inside a different `#[test]` body — a locality failure: the helper is
scoped to the test that needed it first, so the seventh test writes a seventh copy.

### FR10 — the parallel schema, and how it has already drifted

#### Shape

`TestServerConfig` (`server.rs:43-225`) is **51 `pub` fields**, all `Option<T>`, flat-named:
`persistence_batch_timeout_ms`, `replication_min_replicas_to_write`, `cluster_election_timeout_ms`,
`tls_handshake_timeout_ms`. The real `Config` (`config/src/lib.rs:88-228`) is **27 nested
sections** plus `config_source_path`. The flat name and the sectioned path are related by nothing
a compiler can check — only by the 319 lines at `:429-747`:

```rust
if let Some(v) = test_config.persistence_batch_timeout_ms {
    config.persistence.batch_timeout_ms = v;
}
```

…repeated, by hand, ~150 times. Plus a 58-line hand-written `impl Clone` (`:227-284`) that must be
edited every time a field is added — a `#[derive(Clone)]` that was manually expanded and is now a
second place to forget.

This is proposal 72's finding, in a different crate. 72 §Summary: *"`frogctl` carries a **second,
hand-written copy of the `frogdb.toml` schema**… The real schema lives in `frogdb-config`… The
copy is snake_case and names keys that do not exist."* The kinship is exact — `TestServerConfig`
is a second hand-written copy of the same schema, in the same workspace, with the same failure
mode. 72 found **11 of 20 keys wrong**. The test harness has not rotted that far, because unlike
frogctl's string literals its mapping is type-checked at the *destination* — but it is rotting by
the same mechanism, and the rot is already measurable:

#### Drift finding 1 — LIVE dead write: `replication_role`

`server.rs:994`, inside `start_replica_with_tls`:

```rust
config.replication_role = Some("replica".to_string());
```

`replication_role` is read in exactly one place, `:495-497`, inside the
**`ServerRole::Standalone`** match arm. `start_replica_with_tls` calls
`Self::start_with_config(config, ServerRole::Replica)` (`:998`), and the `Replica` arm `:502-503`
hardcodes `config.replication.role = "replica"` without reading the field. The write at `:994` has
no effect. It reads as intent — a maintainer setting the role explicitly — and the harness silently
ignores it. A reader auditing a replication test's setup would take `:994` as evidence of a
configured role.

Zero external setters: a scan of every `*.rs` in the tree for both `replication_role:` and
`.replication_role =` finds **0** sites outside `server.rs` itself.

#### Drift finding 2 — producerless knob: `tls_handshake_timeout_ms`

Declared `:174`, cloned `:267`, consumed `:665-667` (`config.tls.handshake_timeout_ms = ms`).
Set by **nobody** — the only two hits anywhere in the tree are in
`frogdb-server/crates/server/src/runtime_config.rs:438,565`, which are the *server's* field of the
same name, not the harness struct's. The knob is a mapping-block line, a `Clone` line and a struct
line maintained for a value that is never produced. Cost is small; it is cited because it is the
canonical shape of the disease: the mirror grows fields the mirror alone knows about.

#### Drift finding 3 — nine knobs whose only setter is `cluster_harness.rs`

`cluster_node_id`, `cluster_initial_nodes`, `cluster_data_dir`, `cluster_election_timeout_ms`,
`cluster_heartbeat_interval_ms`, `cluster_connect_timeout_ms`, `cluster_request_timeout_ms`
(`cluster_harness.rs:180-186`) plus the three `tls_*_file` knobs (`:193-197`) each have **exactly
one** setter in the whole tree. Fourteen more fields have exactly one setter in
`integration_replication.rs` / `integration_persistence.rs`. Roughly half the mirror exists to
serve a single caller each — the flat-`Option` shape is paying full maintenance cost for
single-use plumbing that a `with_config(|c| c.cluster.election_timeout_ms = 50)` closure would
express in place.

#### Drift finding 4 — the mirror is two layers deep

`ClusterNodeConfig` (`cluster_harness.rs:75-…`, **13 fields**) is a *third* schema copy: it mirrors
a subset of `TestServerConfig`, which mirrors `Config`. Its only translation is the nine-line
setter block at `:180-197`. Adding a cluster knob therefore costs edits in three structs plus two
mapping blocks. `frogdb-operator/tests/integration.rs:35-43` constructs `ClusterNodeConfig`
literals directly, so the third layer has an out-of-workspace consumer as well.

#### Drift finding 5 — `deny_unknown_fields` catches nothing today

`config_source_path` is set at `:646-648`:

```rust
config.config_source_path = Some(path.clone());
```

…and that is *all* that happens with it — the file at `path` is never handed to `ConfigLoader`.
`integration_admin.rs:1681-1723` (`test_config_rewrite_sanity`) writes a TOML literal using
correct kebab-case (`num-shards`) at `:1700`, and the four other `config_file_path` call sites
(`:1745, :1799, :1840, :1890`) do the same. **None of those TOML bodies is ever parsed.**

So: what would `#[serde(deny_unknown_fields)]` catch in existing tests? **Nothing — and that is
the finding.** The harness has no path that boots a server from a config *file*, so the
`deny_unknown_fields` guarantees that `ServerConfig` (`config/src/server.rs:18`) and its siblings
carry are untested by the integration suite. The TOML strings those five tests write are, today,
decorative. FR10's real prize is not deleting 319 lines — it is that embedding the real `Config`
makes "boot this server from this TOML" a one-line harness capability, which is the only way the
suite ever exercises the loader that ships.

#### Drift finding 6 — the turmoil harness already does it right

`tests/common/sim_helpers.rs:145-176`, `real_frogdb_server`, builds:

```rust
Config {
    server: ServerConfig { /* … */ },
    persistence: PersistenceConfig { enabled: false, ..Default::default() },
    http: HttpConfig { enabled: false, ..Default::default() },
    metrics: MetricsConfig { enabled: false, ..Default::default() },
    ..Default::default()
}
```

Same test tree, same workspace, real `Config`, no mirror, no mapping block. The two halves of the
suite disagree about how to configure a server, and the half that got written later is the one
that got it right. FR10 is "make `server.rs` agree with `sim_helpers.rs`", not a novel design.

#### Correction to the lane brief: no new dependency

The brief and the dispatch both frame FR10 as *"adds a harness→`frogdb-config` dep"*. It does not.
`frogdb-test-harness/Cargo.toml` already lists `frogdb-server = { path = "../server" }`, and
`server.rs:13` already imports `Config` through the server's re-export — the mapping block at
`:429-747` mutates a real `Config` instance today. FR10 changes **where the `Config` is
constructed**, not which crates are linked. Consequence: **proposal 72's `document.rs` move does
not gate this proposal in either direction.**

## Proposed change

### FR9 — one generic client, one shared adapter

**Seam 1 — `frogdb-protocol` gains the missing inverse.** `Response::from_resp2_frame(BytesFrame)`
next to the existing `WireResponse::to_resp2_frame` (`protocol/src/response.rs:274`). The crate
that owns `Response` owns both directions of its wire mapping; today it owns one. Both existing
copies (`server.rs:1343-1354`, `migrate.rs:357-368`) delete and call it. This is **additive to a
non-locked crate**, needs no new dependency (`frogdb-protocol` already has `redis-protocol` +
`tokio-util`), and is independently landable — it is hotfix H3 below.

**Seam 2 — `RespClient<S, C>` in a new `test-harness/src/resp_client.rs`.**

```rust
pub struct RespClient<S, C> {
    pub framed: Framed<S, C>,
    pub timeout: Duration,
}

impl<S, C> RespClient<S, C>
where
    S: AsyncRead + AsyncWrite + Unpin,
    C: Encoder<C::Frame> + Decoder<Item = C::Frame>,
{ /* send, recv, send_only, read_raw, with_timeout */ }
```

The two axes become the two type parameters. The **depth** argument: one module of ~120 lines
whose interface is five methods, replacing six partial implementations totalling ~430 lines whose
combined interface is thirteen differently-named methods. Depth improves because the interface
shrinks faster than the implementation.

**Locality.** The stream-specific work — rustls `ClientConfig` assembly
(`server.rs:1423-1450`), turmoil `TcpStream::connect` — stays in per-stream `connect()`
constructors. That is the part that genuinely differs; it is ~30 lines and it belongs where it
is. What moves is the part that does not differ.

**Adapters, not flattening.** Three thin type aliases plus one real adapter:

- `pub type TestClient = RespClient<TcpStream, Resp2>` — plus the `Response`-returning
  convenience methods as an `impl` block on the alias.
- `pub type TlsTestClient = RespClient<TlsStream<TcpStream>, Resp2>` — **gains
  `command_raw`/`send_only`/`read_response` for free**, closing the asymmetry noted above.
- `Resp3TestClient` **stays a struct**, wrapping `RespClient<TcpStream, Resp3>` and keeping
  `pending_pushes` + the push-confirmation loop (`:1245-1294`) verbatim. Protocol semantics do not
  belong in the transport generic. This is the single most important constraint on the refactor.
- `MigrateClient` (`migrate.rs:232-354`) keeps its `auth`/`select_db`/`restore` methods and its
  `Framed<TcpStream, Resp2>` field becomes a `RespClient`. Production code taking a dependency on
  a test crate is unacceptable, so **only `frame_to_response` moves for `migrate.rs`** (seam 1);
  its client body is left alone unless `RespClient` itself lands in `frogdb-protocol` behind a
  feature. Recorded as an explicit non-goal below.

**Turmoil cells.** `turmoil::net::TcpStream: AsyncRead + AsyncWrite`
(`turmoil-0.7.1/src/net/tcp/stream.rs:433,457`), so `RespClient<turmoil::net::TcpStream, Resp2>`
type-checks with no turmoil-specific code in the generic. `simulation.rs`'s `RespConn`
(`:4336-4428`) and both hand-rolled parsers (`workload_runner.rs:665-756`,
`pubsub_runner.rs:249-347`) become instantiations, and their `find_crlf`/`parse_at` bodies delete.
Because the generic itself never names turmoil, **`just lint-turmoil-features` is unaffected** —
verified against `Justfile:349+`.

**Deletion test.** After the change, deleting `resp_client.rs` breaks: the harness's three
clients, `resp3.rs`'s `Resp2Client`, `simulation.rs`'s `RespConn`, and both turmoil runners —
i.e. every socket-speaking test path. Deleting it today breaks nothing, because it does not
exist and every consumer carries its own copy. That asymmetry *is* the proposal.

### FR10 — embed, then mutate

**Interface.** `TestServerConfig` keeps its name (36 files import it; see the risk section) and
becomes:

```rust
pub struct TestServerConfig {
    pub config: Config,               // the real thing
    // harness-only knobs that have no Config counterpart:
    pub data_dir: Option<TempDir>,    // lifetime-owning
    pub wait_for_ready: bool,
    // …
}

impl TestServerConfig {
    pub fn with_config(mut self, f: impl FnOnce(&mut Config)) -> Self { f(&mut self.config); self }
}
```

The 319-line mapping block (`:429-747`) deletes: there is nothing left to map. The 58-line manual
`Clone` (`:227-284`) becomes `#[derive(Clone)]` on everything except the `TempDir` field, which
gets an explicit small impl. The 51 flat `Option`s delete; callers move to
`.with_config(|c| c.replication.min_replicas_to_write = 2)` — which is *shorter* at the call site
than `TestServerConfig { replication_min_replicas_to_write: Some(2), ..Default::default() }` and,
critically, is checked against the real field path by the compiler.

**Leverage.** Adding a config knob today costs four edits (`Config` field, `TestServerConfig`
field, `Clone` line, mapping line) and gives the test suite a knob nobody uses until someone adds
the plumbing. After: one edit, and every test can reach it immediately. That is the leverage
ratio the proposal buys — and it is why the mirror keeps accreting single-setter fields
(drift finding 3).

**The 16 constructors stay.** `TestServer::start`, `start_primary`, `start_replica`,
`start_with_tls`, … (`:355-1002`) are the harness's real interface and they read well. They get
*shorter*: `start_replica_with_tls` (`:988-999`) becomes four `with_config` lines with no
possibility of a dead write, because `config.replication.role` is the field the server actually
reads. **`cluster_harness.rs` is already a good facade** (`connect() :312`, `send :332`,
`try_send :348` over `TestClient`) and improves for free — its nine single-setter knobs
(`:180-197`) become direct `Config` writes and the corresponding `TestServerConfig` fields vanish.

**New capability, not just deletion.** With a real `Config` embedded, add
`TestServerConfig::from_toml(&str) -> Result<Self>` routed through `ConfigLoader`. That is the
first harness path that exercises `deny_unknown_fields` (drift finding 5), and it turns the five
decorative TOML literals in `integration_admin.rs` (`:1700, 1745, 1799, 1840, 1890`) into real
assertions.

**Deletion test.** After: deleting the `config: Config` field is impossible without deleting the
harness. Today, deleting `tls_handshake_timeout_ms` and `replication_role` from
`TestServerConfig` breaks **nothing** — proven by the setter scan. A struct with removable fields
is a struct whose shape nobody depends on.

## Testability improvement

This proposal's subject *is* the test infrastructure, so "improves testability" has to mean
something sharper than usual. Three concrete things:

1. **Chunking becomes testable, then tested.** Today 108 single-`read()` sites in
   `simulation.rs` assume single-segment replies — an assumption `simulation.rs:4338-4340`
   documents as false under turmoil. Routing them through `RespClient`'s `Framed` decoder makes
   partial-reply delivery a non-event. This converts a class of *silently flaky* tests into
   correct ones, and it is the only item here with live-correctness stakes for the sims that
   force replication and cluster failure modes.

2. **`RespClient` is unit-testable in isolation; the current clients are not.** `Framed<S, C>`
   over a `tokio_test::io::Builder` mock lets the timeout path, the connection-closed path and
   the partial-frame path be asserted directly. Today those paths are `.expect("timeout")` inside
   a struct that can only be constructed by connecting to a real socket — so the harness's own
   error handling has zero tests. A test harness with untested failure paths turns harness bugs
   into product-bug reports; hardening campaign 2's issue 32 was exactly that (a jepsen harness
   defect that read as a product defect).

3. **`deny_unknown_fields` acquires teeth.** Per drift finding 5, no integration test parses a
   config file. `from_toml` closes that, and every future config-schema change gets a cheap
   regression site.

Meta-risk, stated plainly: a bug introduced into `RespClient` is a bug in *every* test at once.
That argues for landing the generic with its own unit tests **first** (hotfix H4 below) and
migrating call sites in separate commits — not for leaving six copies in place, which merely
distributes the same risk into six places where nobody looks.

## Risks / scope boundaries

### The riskiest surface: failure-mode spec bindings

The dispatch flags this as the proposal's most dangerous edge. Findings, in order of what the
lint actually binds:

**What `scripts/failure-modes.py` binds.** Two things, and only two: (a) whole-line `FM-<AREA>-NNN`
tags in test files, matched by `FM_TAG_LINE_RE`, associated to the **next `fn` name** across a
preamble of comments/attributes/blank lines (`PREAMBLE_RE = ^\s*(//|#!?\[|$)`); and (b) backticked
test names inside each spec row's `Forced by` field (`BACKTICKED_RE`). It binds **test function
names**, not types, not file paths, not module paths.

**Census — do the specs name any harness file, type, or alias?** Grep of
`.scratch/hardening/specs/` for `test-harness`, `test_harness`, `TestServerConfig`, `TestClient`,
`cluster_harness` returns **one** hit:
`persistence-failure-modes.md:498`, the string `test_harness_crash_and_recover` inside a
`Forced by` list. That is a **false positive**: the test lives at
`frogdb-server/crates/core/src/persistence/test_harness.rs:577` — `frogdb-core`'s own persistence
test module, an unrelated namespace. **No failure-mode spec references `frogdb-test-harness`, any
of its types, or any of its files.**

**Therefore the type renames this proposal implies carry zero spec-side edits.** Renaming
`TestServerConfig`'s fields, deleting `frame_to_response`, aliasing `TestClient` — none of it can
break `just lint-failure-modes`, because the lint never looks at those names.

**Where the real hazard is.** `frogdb-server/crates/server/tests/` contains **198 `FM-…` tag
mentions across 20 files**, and `NEXTEST_FEATURE_VARIANTS` includes `("frogdb-server",
"turmoil")`, so turmoil-gated tests are in scope. The hazard is not renaming; it is that a
**mechanical rewrite of 36 files' call sites** can (i) reorder or reflow lines such that an
`FM-…` tag comment is no longer separated from its `fn` by preamble-only lines, silently
rebinding the tag to the wrong function, or (ii) rename a `fn` while migrating a test to
`with_config`, orphaning a spec's `Forced by` entry.

**Mitigations, mandatory for this proposal:**

- **No `fn` in `frogdb-server/crates/server/tests/` may be renamed or moved between files.** Call
  bodies change; signatures do not. This is a hard constraint, not a preference.
- No line may be inserted between an `FM-…` tag comment and its `fn`.
- `just lint-failure-modes` runs **per slice**, not once at the end. It is compile-free
  (`Justfile:293-294`) and cheap, and it is the only thing that can catch a silent rebind.
- The turmoil variant must be linted too — the sims carry 3 of the FM references.

**LOCKED-crate check.** The file set above contains `frogdb-test-harness`, `frogdb-server`
(server crate + its `tests/`), and `frogdb-protocol`. None of `frogdb-txn`, `frogdb-vll`,
`frogdb-persistence`, `frogdb-recovery`, `frogdb-replication`, `frogdb-replication-runtime`,
`frogdb-cluster`, `frogdb-cluster-runtime` is touched. **No mutation gate applies and
`just mutants-diff` is not required** — though the `frogdb-server` integration tests that force
locked-area rows do run through this code, which is precisely why the rename constraint above is
absolute.

**Seam-lint check.** `just lint-gates` (`Justfile:329`, 14 gates) covers clock reads, metrics
emission, redirect replies, durable-ack writes and similar production chokepoints. None of the 14
constrains test-harness client construction, and the proposal adds no production call sites
(seam 1 is a pure move into `frogdb-protocol`; `migrate.rs` loses a private fn and gains an
import).

### Other risks

- **`migrate.rs` is production code.** `MigrateClient` must not gain a dependency on
  `frogdb-test-harness`. Scope boundary: only `frame_to_response` is shared, via
  `frogdb-protocol`. Unifying `MigrateClient` onto `RespClient` requires the generic to live in
  `frogdb-protocol` (which would need a `tokio/net`-free `AsyncRead` bound — feasible, since the
  generic never names a concrete stream) and is deliberately **out of scope**.
- **`server.rs` is hot** — 37 commits, 15 since 2026-06-01, and it is shared with concurrent
  work. A single 1485-line rewrite will collide. Land in slices (see Effort) and rebase often.
- **`Resp3TestClient` push semantics.** Flattening `:1245-1294` into the generic would break
  RESP3 pub/sub tests in ways that look like server bugs. Called out twice on purpose.
- **`TestServerConfig` name churn.** 36 files import it via
  `tests/common/test_server.rs:1-3` (`pub use frogdb_test_harness::server::*;`). Keeping the
  **name** while changing the **shape** means the glob re-export keeps working and no import line
  changes — which is also what keeps the `fn`-rename constraint satisfiable.
- **Call-site volume.** ~150 mapping lines correspond to a comparable number of
  struct-literal sites across 36 files. Mechanical, but large; `sed`-scale, reviewed in slices.

### Sibling boundaries

Verified on disk at HEAD, against `.scratch/arch-deepening/proposals/`:

| Sibling | Present? | Edge |
|---|---|---|
| **72** (frogctl↔`frogdb-config`) | yes, 612 lines | **Kinship, not dependency.** Same disease (parallel hand-written schema), cited above. FR10 adds **no** new dep — `frogdb-test-harness` already reaches `Config` via `frogdb-server` — so 72's `document.rs` move does **not** order this proposal. Fully parallel. |
| **73** (frogctl ops wiring) | yes | One mention of the harness, at `73:392`, noting `frogdb_test_harness::server::TestServer` as a dev-dependency entry. Read-only. No overlap. |
| **74** (debug-bundle assembler) | yes | No harness or `migrate.rs` references. No overlap. |
| **75** (frogctl rendering role) | yes (untracked, concurrent author) | frogctl-only. No overlap. |
| **76** (observability extractors) | yes, 575 lines | **Only real contact.** `76:66` lists `test-harness/src/server.rs` as *"Read-only evidence (FR11)"* for `fetch_metrics :883-895`, and `76:518-527` **explicitly declines** to fold that duplicate (folding would force `frogdb-test-harness` to depend on `frogdb-telemetry`'s `testing` feature, pulling `reqwest` + `opentelemetry_sdk/testing`). So 76 reads the file; **78 owns it**. The declined follow-up remains declined here too — out of scope, different axis. |
| **77** (operator resources) | yes (untracked, concurrent author) | **No file overlap** — 77 confirms the same (`77:589`). But 77 surfaces a real edge this proposal must own: `frogdb-operator/Cargo.toml:38` takes a path dependency on `frogdb-test-harness`, and `frogdb-operator/tests/integration.rs:26-44` constructs **both** `TestServerConfig` and `ClusterNodeConfig` literals. `frogdb-operator` is a **separate workspace**, so `just check` and `just test` do not build it — an FR10 shape change compiles clean and breaks the operator silently. **`just operator-test` (`Justfile:970`) must run in the same commit as FR10a.** |
| **90 / CT2** (CommandSpec sweep) | **absent** | Command-identity enums; no contact with transport or config plumbing. |

Net: one file (`test-harness/src/server.rs`) is claimed read-only by 76 and read-write by 78. No
write-write conflict exists across the round.

## Effort

| Part | Effort | Notes |
|---|---:|---|
| H3: `Response::from_resp2_frame` into `frogdb-protocol`, delete both copies | **XS** | 2 files + 1 additive fn |
| H1: delete dead `replication_role` write (`:994`) + the field | **XS** | LIVE |
| H2: delete producerless `tls_handshake_timeout_ms` (3 lines) | **XS** | latent |
| H4: `RespClient<S, C>` module + unit tests, **no call-site migration** | **S** | ~120 lines, lands green |
| FR9a: migrate `TestClient`/`TlsTestClient`/`Resp3TestClient` onto it | **S** | TLS gains 4 methods |
| FR9b: migrate `Resp2Client`, `RespConn`, both turmoil parsers; fold 6× `round_trip` | **M** | touches `simulation.rs`; the 108 single-`read()` sites are a separate follow-up |
| FR10a: embed `Config`, `with_config`, delete mapping + manual `Clone` | **M** | mechanical but 36 files |
| FR10b: `from_toml` via `ConfigLoader` + assert the 5 dormant TOML literals | **S** | new capability |
| **Total** | **M/L** | brief said FR9=S, FR10=M/L; FR9 is **S only for the harness three** — the census makes the full sweep M |

### Independently-landable hotfixes

**H1 — `replication_role` dead write. LIVE.** `server.rs:994` sets a field that the
`ServerRole::Replica` arm never reads (`:502-503` hardcodes the role). Zero setters elsewhere in
the tree. *Live* because it actively misleads: a reader auditing TLS-replication test setup takes
`:994` as configuration that is in fact discarded. No behavior change to delete it (the arm
already hardcodes the same value). One-line deletion plus removing the field and its `Clone` line.

**H2 — `tls_handshake_timeout_ms` producerless knob. Latent.** Declared `:174`, cloned `:267`,
consumed `:665`, produced nowhere. No current misbehavior; it is maintenance cost and a template
for the next unused mirror field. Three-line deletion.

**H3 — duplicate `frame_to_response`. Latent.** Byte-identical at `server.rs:1343-1354` and
`migrate.rs:357-368`; the two cannot currently diverge because neither has changed, but nothing
prevents it, and the production copy is the one that would matter. Moving it to `frogdb-protocol`
beside `to_resp2_frame` is additive, non-locked, and useful independent of everything else here.

**H4 — `RespClient<S, C>` with unit tests, zero call-site migration. Latent.** Landing the module
green and unmigrated de-risks every subsequent slice and is reviewable in isolation.

**Not a hotfix, and honest about it:** the 108 single-`read()` sites in `simulation.rs` are
**latent-correctness**, not latent-tidiness — `simulation.rs:4338-4340` documents that they can
mis-frame under turmoil chunking. No observed failure is attributed to them today, so the ruling
is *latent*; but they are the one item in this proposal whose eventual payoff is test *soundness*
rather than test *maintenance*, and they should not be quietly dropped when FR9b is scoped.
