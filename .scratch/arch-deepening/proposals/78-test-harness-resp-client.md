# 78 — Generic `RespClient<S, C>` and a `TestServerConfig` that embeds the real `Config`

**Candidates:** FR9 (S/M) + FR10 (M/L) · **Crates:** `frogdb-test-harness` (unlocked),
`frogdb-server` tests + `src/migrate.rs` (unlocked), `frogdb-protocol` (unlocked, additive) ·
**Order:** independent of 72/73/74/75/76; see [Sibling boundaries](#sibling-boundaries)

Revision 2, after adversarial review `2e81506b` (verdict **AMEND**). Every citation below was
re-derived against the working tree at **HEAD `54baa2bb`**. Where revision 1 and the tree
disagree, the tree wins; every correction is recorded in the
[Review ledger](#review-ledger-revision-2) at the end, including the three places the review's
own claims did not survive verification.

## Summary

Two independent defects share one file.

**FR9.** The lane brief says "3 near-identical RESP clients + a 4th in `migrate.rs`". Revision 1
said "seven" and then enumerated eight. The true census, re-derived exhaustively, is **≥13
socket-speaking RESP sites across ≥10 files**:

| # | Site | Shape |
|---|---|---|
| 1 | `test-harness/src/server.rs:1148` `TestClient` | `Framed<TcpStream, Resp2>`, 5 methods |
| 2 | `test-harness/src/server.rs:1225` `Resp3TestClient` | `Framed<TcpStream, Resp3>` + push filter |
| 3 | `test-harness/src/server.rs:1403` `TlsTestClient` | `Framed<TlsStream<TcpStream>, Resp2>`, 1 method |
| 4 | `test-harness/src/server.rs:1080-1089` `TestServer::send_raw` | **single 4096-byte `read()`, no framing** |
| 5 | `server/src/migrate.rs:232-354` `MigrateClient` | production, `Framed<TcpStream, Resp2>` |
| 6 | `server/tests/resp3.rs:31-56` `Resp2Client` | `Framed<TcpStream, Resp2>`, returns raw frames |
| 7 | `server/tests/resp3.rs:729/742/759` `encode_resp_command`/`connect_resp3_raw`/`send_raw_command` | hand-built wire bytes + single 4096-byte `read()` |
| 8 | `server/tests/simulation.rs:4336-4428` `RespConn` + `parse_resp_value` | hand-rolled buffered parser (turmoil) |
| 9 | `server/tests/common/workload_runner.rs:665-756` | hand-rolled `find_crlf`/`parse_at` (turmoil) |
| 10 | `server/tests/common/pubsub_runner.rs:233-347` | hand-rolled `find_crlf`/`parse_at` (turmoil) |
| 11 | `server/tests/common/sim_helpers.rs:26-` `encode_command`/`parse_simple_response`/`parse_resp_array` | third raw-byte encoder/parser (turmoil) |
| 12 | `server/tests/cluster_failover.rs:562-578` and `:3332-3348` | inline `TcpStream::connect` + `Framed` + hand-built `BytesFrame::Array` |
| 13 | `server/tests/cluster_migration.rs:399-415` | same shape as 12 |

Plus `frame_to_response` **verbatim twice** (`server.rs:1343-1354`, `migrate.rs:357-368` —
byte-identical modulo `pub`) and **six** separately-defined nested `async fn round_trip` bodies in
`simulation.rs` (`:3197, :3412, :3572, :3737, :4015, :4179`).

Site 4 is the sharpest single fact in the census: `TestServer::send_raw`, **inside the harness
this proposal refactors**, does one 4096-byte `read()` with no framing — exactly the unsoundness
FR9 exists to remove.

The duplication has a legible cause, and it is not laziness. There are two axes of variation —
**stream type** (`tokio::net::TcpStream` / `turmoil::net::TcpStream` /
`tokio_rustls::client::TlsStream`) × **codec** (`Resp2` / `Resp3`) — and no generic to carry them,
so the matrix got written out by hand, partially, many times. `turmoil::net::TcpStream` implements
tokio's `AsyncRead`/`AsyncWrite` (`turmoil-0.7.1/src/net/tcp/stream.rs:433,457`), so a single
`RespClient<S, C>` covers every cell and the shared type needs zero turmoil knowledge.

**FR10.** `TestServerConfig` (`test-harness/src/server.rs:43-225`) is a **flat, 51-field,
mostly-`Option` mirror** of the sectioned `frogdb_config::Config`, hand-maintained alongside a
58-line manual `impl Clone` (`:227-284`), a translation block spanning `:441-685` (**64
`config.… = …` assignments, 37 of them behind `if let Some(v)`**), and 16 constructors. This is the
disease proposal 72 diagnoses in `frogctl` — *a second, hand-written copy of the schema that drifts
against the real one* — with two confirmed symptoms already present: a **no-op write**
(`replication_role`, `:994`) and a **producerless knob** (`tls_handshake_timeout_ms`, read at
`:665`, set by nobody).

The turmoil half of the same test tree already does the right thing:
`tests/common/sim_helpers.rs:145-176` builds a **real `Config` struct literal** with
`..Default::default()`. The fix for FR10 is to make the non-turmoil harness agree with its own
sibling.

Neither change touches a locked crate. The proposal's two genuinely dangerous edges are
**turmoil seed determinism** (§[FR9 scope ruling](#fr9-scope-ruling-turmoil-runners-are-excluded))
and **the reserved-field set in `try_start_with_config`**
(§[FR10 reserved fields](#fr10-reserved-fields-and-closure-ordering)). Both are ruled below rather
than deferred.

## Files involved

| Path | Lines | Role in this proposal |
|---|---:|---|
| `frogdb-server/crates/test-harness/src/server.rs` | 1485 | **Primary.** `TestServerConfig` (FR10) + 3 clients + `send_raw` + `frame_to_response` (FR9) |
| `frogdb-server/crates/test-harness/src/cluster_harness.rs` | 1326 | Read-mostly. Verified **facade** over `TestClient` (`connect :312`, `send :332`, `try_send :347`); sole setter for 9 knobs |
| `frogdb-server/crates/test-harness/src/lib.rs` | 5 | Module list; gains one `pub mod` if the client moves |
| `frogdb-server/crates/server/src/migrate.rs` | 524 | **Production** code. `MigrateClient` `:232-354`; duplicate `frame_to_response` `:357-368` |
| `frogdb-server/crates/server/tests/resp3.rs` | 926 | `Resp2Client` `:31-56`; raw-byte path `:729/742/759` |
| `frogdb-server/crates/server/tests/simulation.rs` | 6880 | `RespConn` `:4336-4428`; 6× `round_trip`; 110 socket `read()` sites |
| `frogdb-server/crates/server/tests/simulation/scheduler.rs` | 2790 | **Consumer of `RespConn`** (`:69` import; `:1457,1559,1565,1671,1691,1798,1807`) — the seed-sweep harness |
| `frogdb-server/crates/server/tests/cluster_failover.rs` | — | Inline `Framed` clients `:562`, `:3332`; carries 1 `FM-` tag |
| `frogdb-server/crates/server/tests/cluster_migration.rs` | — | Inline `Framed` client `:399`; carries **12** `FM-` tags |
| `frogdb-server/crates/server/tests/common/workload_runner.rs` | 856 | Hand-rolled RESP2 parser `:665-756` (turmoil-gated) |
| `frogdb-server/crates/server/tests/common/pubsub_runner.rs` | 386 | Hand-rolled RESP2 parser `:233-347` (turmoil-gated) |
| `frogdb-server/crates/server/tests/common/sim_helpers.rs` | 523 | `encode_command :26`/`parse_simple_response`/`parse_resp_array`; **`real_frogdb_server :145-176` is the FR10 exemplar** |
| `frogdb-server/crates/server/tests/common/test_server.rs` | 3 | `pub use frogdb_test_harness::server::*;` — the alias every server test imports |
| **`frogdb-server/crates/redis-regression/tests/` (15 files)** | — | **Second consumer crate** — `frogdb-redis-regression`, workspace member `Cargo.toml:34`, **inside the default nextest filter**, 27 `TestServerConfig` literals |
| **`frogctl/tests/integration_health.rs`, `integration_upgrade.rs`** | — | **Third consumer crate** — `frogctl`, workspace member, but **excluded from the default suite** (`just frogctl-test`, `Justfile:290`) |
| `frogdb-operator/tests/integration.rs` | — | **Fourth consumer** — separate workspace; `TestServerConfig` + `ClusterNodeConfig` literals |
| `frogdb-server/crates/protocol/src/response.rs` | 1770 | Owns `WireResponse::to_resp2_frame` `:274` and `Response::into_wire` `:770`; candidate home for the frame→`Response` mapper |
| `frogdb-server/crates/config/src/lib.rs` | — | `Config` `:84-228`: 27 sections + `config_source_path` `:90` |
| `scripts/failure-modes.py` | — | The spec↔test lint whose bindings constrain any rename |

**Blast radius, measured at HEAD.** `TestServerConfig` is mentioned in **39 files** tree-wide —
36 inside `frogdb-server/` (19 `server/tests/`, 15 `redis-regression/tests/`, 2 test-harness), plus
2 in `frogctl/tests/` and 1 in `frogdb-operator/tests/`. Construction sites: **213** (`grep -c
'TestServerConfig {'` = 215 minus the `struct` definition `:43` and the `impl Clone` header `:227`),
of which 100 are in `integration_replication.rs` alone. `server.rs` has 37 commits, 15 of them
since 2026-06-01 — a hot file, which is both the argument for fixing it and the argument for
landing the fix in reviewable slices.

## Problem

### FR9 — the client matrix, written out by hand

#### The two axes

| | `Resp2` codec | `Resp3` codec | raw bytes / hand-rolled |
|---|---|---|---|
| `tokio` `TcpStream` | `TestClient` (server.rs:1148) · `Resp2Client` (resp3.rs:31) · `MigrateClient` (migrate.rs:232) · `cluster_failover.rs:562`, `:3332` · `cluster_migration.rs:399` | `Resp3TestClient` (server.rs:1225) | `TestServer::send_raw` (server.rs:1080) · `resp3.rs:729/742/759` |
| `tokio_rustls` `TlsStream` | `TlsTestClient` (server.rs:1403) | *(missing)* | — |
| `turmoil` `TcpStream` | *(no codec used)* | *(missing)* | `RespConn` (simulation.rs:4336) · `workload_runner.rs:665` · `pubsub_runner.rs:233` · `sim_helpers.rs:26-` |

Two of nine cells are empty (RESP3-over-TLS, RESP3-under-turmoil) — capabilities the harness
simply does not have. The other seven are populated by **thirteen** independent implementations.

#### The plumbing is *nearly* identical; the differences are real but small

`TestClient::command` (`server.rs:1155-1174`) and `TlsTestClient::command`
(`server.rs:1458-1472`) do the same work — build `BytesFrame::Array` of `BulkString`,
`framed.send`, `timeout(15s, framed.next())`, unwrap the three layers, `frame_to_response`. They
are **not textually identical**: the TLS copy says `.expect("timeout waiting for response")`
where the TCP copy says `.expect("timeout")`, and it spells the last two steps
`.map(frame_to_response).expect("frame error")` instead of `.expect("frame error")` then a call.
Revision 1's "the same eleven lines" was an overstatement; the accurate claim is *same algorithm,
divergent incidental text* — which is the ordinary way copies begin to rot.

The asymmetry that costs is what `TlsTestClient` *lacks*. `TestClient` has five methods —
`command`, `command_raw` (`:1176-1192`), `send_only` (`:1195-1202`), `read_response`
(`:1205-1210`), `read_message` (`:1214-1216`). `TlsTestClient` has **three**, of which two are
constructors (`connect :1409`, `try_connect :1421`) and one is `command`. A TLS test that wants
`send_only` cannot; nothing about TLS makes `send_only` hard, the copy just stopped at one method.

**Timeouts are not uniform, and unifying them is a behavior change.** `TestClient::command` waits
15s (`:1166`, commented "to accommodate WAIT commands"); `TestClient::command_raw` waits 5s
(`:1186`); `TlsTestClient::command` waits 15s (`:1467`); `Resp2Client::command`
(`resp3.rs:47`) waits 5s; the raw helpers in `resp3.rs:752,765` wait 2s. Promoting the timeout to
a `RespClient` field with one default silently changes at least `command_raw` (5s → 15s) on the
hottest path in the harness. **Ruling: the timeout becomes a per-call parameter with per-method
defaults preserved verbatim** (`command` 15s, `command_raw` 5s, …). A `with_timeout` builder may
exist, but no method's current default may move. This is stated as a constraint, not a
preference, because a silently-lengthened timeout converts a hang into a slow pass.

**Honest correction to the brief:** `Resp3TestClient` is *not* near-identical.
`command` (`:1245-1294`) carries genuine RESP3 push-filtering — it inspects `Resp3Frame::Push`,
matches the first element against the `(p|s)?(un)?subscribe` confirmation family, buffers
non-confirmations into `pending_pushes` and loops. That logic is protocol semantics, not
plumbing, and it must survive the refactor **as a layer on top of** the generic client, not be
flattened into it. Any proposal that claims "three near-identical clients, collapse to one" is
wrong about this file. The generic factors the transport; RESP3 push-filtering stays an adapter.

#### `frame_to_response` exists twice, verbatim

`test-harness/src/server.rs:1343-1354` and `migrate.rs:357-368` are byte-identical modulo the
`pub`:

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

One copy is `pub` in a test crate; the other is private in **production** `migrate.rs`. That is a
duplicate that `frogdb-protocol` should own.

**What it is *not*, and revision 1 got this wrong:** it is not the inverse of
`WireResponse::to_resp2_frame` (`protocol/src/response.rs:274`). Three facts refute the
inverse claim:

1. `frame_to_response` produces **`Response`** (`response.rs:647`), the strictly larger union that
   also carries internal control-flow signals; `to_resp2_frame` consumes **`WireResponse`**.
   `Response::into_wire` (`:770`) runs `Response → WireResponse`, i.e. the *opposite* direction
   from what a `Framed` reader needs.
2. `to_resp2_frame` applies `sanitize_error_message` to `Error` and `BlobError` (`:277`, `:294`).
   A round trip through it does not preserve error payloads.
3. `to_resp2_frame` is lossy by construction: `Double`, `Boolean`, `Map`, `Set`, `VerbatimString`
   all collapse into RESP2 shapes that cannot be distinguished on the way back.

So the symmetric home would be `WireResponse::from_resp2_frame`, but callers need
`BytesFrame → Response`, and no `WireResponse → Response` conversion exists. **The destination
type is an open design decision, not a settled one** — see hotfix
[H3](#h3--duplicate-frame_to_response-latent).

#### The turmoil parsers, and why they exist

`simulation.rs:4336-4341` states the cause in its own doc comment:

> *Minimal RESP2 client for the replication sims: frames complete replies across arbitrary TCP
> chunking (the one-`read()` helpers elsewhere in this file assume single-segment replies, which
> turmoil does not guarantee).*

So the file **knows** the single-`read()` sites are unsound under a chunking network, and the
response was to add another client next to them rather than fix them. `workload_runner.rs:688-756`
and `pubsub_runner.rs:249-347` then wrote the same buffered `find_crlf`/`parse_at` loop twice more.

**Corrected count.** `simulation.rs` has **110** socket read call sites (104 `.read(&mut …)` plus
6 `stream.read(buf)` inside the six `round_trip` bodies). At least one of those — `RespConn`'s at
`:4373` — is inside a correct `loop { … if complete { return } … }`, so the number of *true
single-shot* reads is ~109, not 110. The soundness argument is unaffected by the arithmetic; the
number is corrected because revision 1 cited 108 and the honest figure is higher.

**Corrected deletion claim.** Revision 1 implied the hand-rolled parsers simply delete. They do
not, entirely. The three parsers decode into **three different domain enums** —
`RespValue` (`simulation.rs`), `OperationResult` (`sim_harness`, via `workload_runner`), and
`RespVal` (`pubsub_runner`) — and those enums are consumed downstream by the linearizability
checkers. `find_crlf`/`parse_at` delete; a `BytesFrame → <domain enum>` mapper is still required
per consumer. The win is real but it is "delete the byte-level parser", not "delete the parser".

#### `round_trip`, six times

`simulation.rs` defines a nested `async fn round_trip` at `:3197, :3412, :3572, :3737, :4015,
:4179`. Six copies of "write a command, read one segment, string-compare". They are not shared
because each is defined inside a different `#[test]` body — a locality failure: the helper is
scoped to the test that needed it first, so the seventh test writes a seventh copy.

### FR10 — the parallel schema, and how it has already drifted

#### Shape

`TestServerConfig` (`server.rs:43-225`) is **51 `pub` fields**. Revision 1 said
"`Option`-everything"; that is wrong and the correction matters: **8 are plain `bool`** —
`persistence`, `admin_enabled`, `cluster_enabled`, `tls_replication`, `tls_cluster`,
`tls_cluster_migration`, `allow_cross_slot_standalone`, `tiered_storage_enabled`. For those the
"absent means take the `Config` default" story is already broken: `false` is indistinguishable
from unset, and `try_start_with_config` writes `config.persistence.enabled =
test_config.persistence` (`:445`) unconditionally, overriding whatever the real default was. Five
of the eight are guarded by `if test_config.x { config.… = true }`, which cannot express "turn it
off". Embedding the real `Config` removes this class of ambiguity entirely, and that is a better
argument for FR10 than the line count.

The real `Config` (`config/src/lib.rs:84-228`) is **27 nested sections** plus `config_source_path`
(`:90`). The flat name and the sectioned path are related by nothing a compiler can check — only
by the mapping region at `:441-685`:

```rust
if let Some(v) = test_config.persistence_batch_timeout_ms {
    config.persistence.batch_timeout_ms = v;
}
```

**Corrected size.** Revision 1 said "319 lines, ~150 mapping lines". The measured mutation region
is `:441-685` (~245 lines) containing **64 `config.… = …` assignments**, **37** of them behind
`if let Some(…)`. The remainder of `try_start_with_config` past `:685` is listener binding,
spawning and struct assembly, not mapping. Plus a 58-line hand-written `impl Clone` (`:227-284`)
that must be edited every time a field is added.

This is proposal 72's finding, in a different crate. 72 §Summary: *"`frogctl` carries a **second,
hand-written copy of the `frogdb.toml` schema**… The real schema lives in `frogdb-config`… The
copy is snake_case and names keys that do not exist."* The kinship is exact. 72 found **11 of 20
keys wrong**. The test harness has not rotted that far, because unlike frogctl's string literals
its mapping is type-checked at the *destination* — but it is rotting by the same mechanism.

#### Drift finding 1 — no-op write: `replication_role` (LATENT, 4 deletion sites)

`server.rs:994`, inside `start_replica_with_tls`:

```rust
config.replication_role = Some("replica".to_string());
```

`replication_role` is read in exactly one place, `:495-497`, inside the **`ServerRole::Standalone`**
match arm. `start_replica_with_tls` calls `Self::start_with_config(config, ServerRole::Replica)`
(`:998`), and the `Replica` arm `:502-503` hardcodes `config.replication.role = "replica"` without
reading the field. The write at `:994` therefore has no effect.

**Reclassified LATENT, not LIVE.** Revision 1 called this LIVE. It is not: no test observes a
wrong value and no behavior differs, because the arm hardcodes the same string the dead write
sets. The cost is legibility — a reader auditing TLS-replication setup takes `:994` as
configuration — which is a latent-tidiness defect, not a live one. Revision 1 also contradicted
itself, calling it LIVE in the Summary and in H1 while describing a no-op.

**Deletion is 4 sites, not 3:** the field declaration `:89`, the `Clone` line `:239`, the dead
write `:994`, **and** the read at `:495-497`, which must become the literal `"standalone"`
(otherwise the `Standalone` arm loses its default). Zero external setters: scanning every `*.rs`
for `replication_role:` and `.replication_role =` finds hits only in `server.rs` itself
(`:89, :239, :496, :994`) and in unrelated namespaces (`config/src/replication.rs`,
`role_manager.rs:1450`).

#### Drift finding 2 — producerless knob: `tls_handshake_timeout_ms` (confirmed)

Declared `:174`, cloned `:267`, consumed `:665-667` (`config.tls.handshake_timeout_ms = ms`).
Set by **nobody** — the only other hits in the tree are
`frogdb-server/crates/server/src/runtime_config.rs:438,565`, which are the *server's* field of the
same name, not the harness struct's. The knob is a struct line, a `Clone` line and a mapping line
maintained for a value that is never produced. Small cost; cited because it is the canonical
shape of the disease.

#### Drift finding 3 — the single-setter tail (count right, enumeration corrected)

**Corrected.** Revision 1 named "nine knobs whose only setter is `cluster_harness.rs`" and listed
seven cluster knobs plus "the three `tls_*_file` knobs (`:193-197`)". Both halves of that
enumeration were wrong.

The nine cluster-harness-only knobs are the **seven** at `cluster_harness.rs:180-186`
(`cluster_node_id`, `cluster_initial_nodes`, `cluster_data_dir`, `cluster_election_timeout_ms`,
`cluster_heartbeat_interval_ms`, `cluster_connect_timeout_ms`, `cluster_request_timeout_ms`) **plus
`cluster_enabled` (`:179`) and `cluster_bus_listener` (`:187`)** — nine, as claimed, but not the
nine listed.

The TLS block at `:193-199` sets **five** file knobs, not three (`tls_cert_file`, `tls_key_file`,
`tls_ca_file`, `tls_client_cert_file`, `tls_client_key_file`), and each has **2-4** setters across
the tree, so none of them belongs in a single-setter tail.

The aggregate claim survives and is what matters: **22 of 51 fields (43%) have at most one
setter** — 1 with zero (`tls_handshake_timeout_ms`) and 21 with exactly one. Roughly half the
mirror pays full maintenance cost for single-use plumbing that a
`with_config(|c| c.cluster.election_timeout_ms = 50)` closure would express in place.

#### Drift finding 4 — the mirror is two layers deep

`ClusterNodeConfig` (`cluster_harness.rs:73-100`, **12 fields** — revision 1 said 13) is a *third*
schema copy: it mirrors a subset of `TestServerConfig`, which mirrors `Config`. Its only
translation is the setter block at `:176-199`. Adding a cluster knob therefore costs edits in
three structs plus two mapping blocks. `frogdb-operator/tests/integration.rs:35-43` constructs
`ClusterNodeConfig` literals directly, so the third layer has an out-of-workspace consumer.

#### Drift finding 5 — REFUTED, and the residue is narrow

Revision 1 claimed (a) the TOML literals in `integration_admin.rs` are never parsed, and (b)
`#[serde(deny_unknown_fields)]` is untested. **Both halves are false.**

*(a) The TOML is parsed.* `test_config_rewrite_sanity` (`integration_admin.rs:1680-1723`) writes
the TOML file, boots with `config_file_path`, runs `CONFIG SET maxmemory` then `CONFIG REWRITE`.
`CONFIG REWRITE` reaches `runtime_config.rs::rewrite_config` (`:1309-1336`), which reads the file
and calls `ConfigPersister::merge` (`config_persister.rs:64`), whose first act is
`let mut doc: DocumentMut = doc_text.parse()` (`:68-70`) with the parse error propagated as
`ERR failed to parse config file`. A malformed literal fails the test today. The literals are not
decorative.

*(b) `deny_unknown_fields` is tested.* `test_reject_unknown_fields_in_server` exists twice —
`config/src/lib.rs:429-445` and `server/src/config/mod.rs:438` — and `runtime_config.rs:5660-5669`
carries `rewrite_and_reparse`, which does `toml::from_str::<Config>` **plus `parsed.validate()`**
on every rewritten file, i.e. full deserialize-and-boot-validate coverage in unit tests.

**What survives** is one narrow claim: no *integration* test loads a TOML file into `Config`
through `ConfigLoader`. The harness sets `config.config_source_path` (`server.rs:647`) and nothing
else; there is no harness path that boots a server *from* a file. That is a real hole, but a small
one — the deserialize path, the `deny_unknown_fields` guard and boot validation are all covered by
unit tests already. **FR10b's prize is correspondingly reduced**: it buys an integration-level
"boot this server from this TOML" capability that does not exist, not first-ever coverage of the
loader's guarantees. Combined with [N13's ruling](#fr10b--from_toml-must-not-route-through-configloader)
that `from_toml` must *not* use `ConfigLoader`, FR10b is now a small convenience, ranked last, and
this proposal is fine landing without it.

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

Same test tree, same workspace, real `Config`, no mirror, no mapping block. FR10 is "make
`server.rs` agree with `sim_helpers.rs`", not a novel design. Caveat: `sim_helpers.rs` is
turmoil-gated, so it never co-compiles with the non-turmoil harness today — the exemplar is real
but the two halves have never had to agree at compile time, which is part of why they diverged.

#### Correction to the lane brief: no new dependency

The brief and the dispatch frame FR10 as *"adds a harness→`frogdb-config` dep"*. It does not.
`frogdb-test-harness/Cargo.toml` already lists `frogdb-server = { path = "../server" }`, and
`server.rs:13` already imports `Config` through the server's re-export — the mapping block mutates
a real `Config` instance today. FR10 changes **where the `Config` is constructed**, not which
crates are linked. Consequence: **proposal 72's `document.rs` move does not gate this proposal in
either direction.**

## Proposed change

### FR9 — one generic client, one shared adapter

#### FR9 scope ruling: turmoil runners are EXCLUDED

**This is the single most important scoping decision in the proposal, and revision 1 did not
contain the word "seed".**

The repo carries seed-addressed reproducers whose value depends on a fixed seed replaying the
*same execution*:

- `frogdb-server/crates/server/tests/simulation/cluster-regression-seeds.txt` — 12 pinned seeds,
  **10 of them carrying `EXPECTED-FAILURE:issue-20`**, replayed in the default suite by
  `test_cluster_scheduler_regression_seeds` and swept by `just cluster-seeds` (`Justfile:211-212`).
- `FROGDB_CONCURRENCY_SEEDS` (`Justfile:163-164`) and the pinned `mod regressions` reproducers that
  `just concurrency` (`Justfile:130-134`) exists to keep running (`Justfile:126-128`), including
  `regressions::regression_drain_capture_race_multiwaiter_ops_110_seed_0`.
- `just concurrency-repro` (`Justfile:137-138`), which replays a per-seed repro file.

Turmoil's scheduler is deterministic **per execution trace**, not per seed alone. FR9b as written
in revision 1 would rewrite the socket-read layer of `RespConn`, `workload_runner` and
`pubsub_runner` onto `Framed`, which polls the socket on a different cadence by construction — a
different number and ordering of `poll_read` wakeups. Under a deterministic scheduler that reorders
the whole run. A seed that once reproduced a bug can then reproduce nothing, pass, and look green.

The tree's existing guard does **not** cover this. The seed file's own header says the family
column is checked against `Schedule::from_seed(seed).family` by
`test_scheduler_regression_seed_file_parses`, precisely so that *"if the derivation's draw order
ever changes, a seed silently starts replaying a different scenario"* is caught. That guard
watches the **seed→schedule derivation**, not the **schedule→execution interleaving**. Changing
read cadence leaves `Schedule::from_seed` untouched and slips straight past it.

`simulation/scheduler.rs` imports `RespConn` at `:69` and uses it at `:1457, 1559, 1565, 1671,
1691, 1798, 1807` — the seed sweep's client layer *is* one of the three sites FR9b would rewrite.
And 10 of the 12 pinned seeds are asserted to **still fail**; a cadence change that quiets one of
them destroys the pin (loudly, in that case — but the two non-EXPECTED-FAILURE seeds, `2 healthy`
and `5 leader-isolation`, would flip silently).

**Ruling — option (a), exclusion.** FR9b covers `Resp2Client` (`resp3.rs`), the raw-byte helpers
in `resp3.rs:729/742/759`, `TestServer::send_raw`, and the inline `Framed` clients in
`cluster_failover.rs` / `cluster_migration.rs`. It **does not touch** `RespConn`,
`workload_runner.rs`, `pubsub_runner.rs`, `sim_helpers.rs`, or the six `round_trip` bodies. Those
keep their raw reads. The turmoil cells remain *demonstrated* (the generic type-checks over
`turmoil::net::TcpStream`; that is what makes the design right) but **unmigrated**.

**Option (b), recorded as an explicit follow-up, not taken here:** migrate the turmoil runners and
re-pin the entire seed corpus in the same commit — re-derive `cluster-regression-seeds.txt` by
re-running `just cluster-seeds` at the pre-change sweep budget, confirm each `EXPECTED-FAILURE`
seed still fails for the *same* reason (issue-20's XNODE-SLOT-1 signature, not merely "fails"),
re-run the concurrency sweep and regenerate any repro files. That is a campaign, not a slice, and
it should be filed as its own issue with the method above written into it. Taking (a) keeps the
corpus meaningful at the cost of leaving the turmoil duplication in place for now — the right
trade, because the corpus is regression coverage that cannot be recreated cheaply and the
duplication is maintenance cost that can be paid later.

**Interaction with B2's census.** `cluster_migration.rs` carries 12 `FM-` tags and
`cluster_failover.rs` carries 1, so FR9b's remaining blast radius still lands in FM-tagged files.
That is governed by the rename constraint in the [risk section](#the-riskiest-surface-failure-mode-spec-bindings),
not by the seed ruling — those two files are ordinary `#[tokio::test]`s, not turmoil sims.

#### Seam 1 — `frogdb-protocol` gains the frame→`Response` mapper

A single `BytesFrame → Response` function lands in `frogdb-protocol`, next to
`WireResponse::to_resp2_frame` and `Response::into_wire`. Both existing copies
(`server.rs:1343-1354`, `migrate.rs:357-368`) delete and call it. This is **additive to a
non-locked crate** and needs no new dependency (`frogdb-protocol` already has `redis-protocol` and
`tokio-util`; `tokio` is a **dev-dependency** only — `protocol/Cargo.toml:19` — which is fine for a
pure function but rules out anything async living there). It is independently landable — hotfix
[H3](#h3--duplicate-frame_to_response-latent). **Its destination type is an open decision the
orchestrator must rule before it lands** (see H3).

#### Seam 2 — `RespClient<S, C>` in a new `test-harness/src/resp_client.rs`

```rust
pub struct RespClient<S, C> {
    pub framed: Framed<S, C>,
}

impl<S, C> RespClient<S, C>
where
    S: AsyncRead + AsyncWrite + Unpin,
    C: Decoder + Encoder<<C as Decoder>::Item>,
{
    pub async fn send(&mut self, frame: <C as Decoder>::Item) -> …
    pub async fn recv(&mut self, timeout: Duration) -> …
    // …
}
```

**Corrected bound.** Revision 1 wrote `C: Encoder<C::Frame> + Decoder<Item = C::Frame>`. That does
not compile: `tokio_util::codec` has no associated `Frame` type. Verified against
`redis-protocol-6.0.0/src/codec.rs` — the impls are `Encoder<Resp2Frame> for Resp2` (`:208`) and
`Decoder for Resp2 { type Item = Resp2Frame }` (`:228-230`), and the RESP3 pair at `:78`/`:98-100`.
The workable formulation is `C: Decoder + Encoder<<C as Decoder>::Item>`, which is exactly the
"encodes what it decodes" constraint both codecs satisfy.

The **depth** argument: one module whose interface is a handful of methods, replacing the
transport half of thirteen partial implementations whose combined interface is a dozen
differently-named methods. Depth improves because the interface shrinks faster than the
implementation.

**Locality.** The stream-specific work — rustls `ClientConfig` assembly (`server.rs:1423-1450`),
turmoil `TcpStream::connect` — stays in per-stream `connect()` constructors. That is the part that
genuinely differs; it is ~30 lines and it belongs where it is.

**Adapters, and how they actually attach.** Revision 1 claimed `TlsTestClient` "gains 4 methods
free". There is no such thing: an inherent `impl` on a type alias attaches to the *underlying
type at that instantiation*, so an `impl RespClient<TcpStream, Resp2>` block does not reach
`RespClient<TlsStream<TcpStream>, Resp2>`. The convenience methods must be written once as

```rust
impl<S: AsyncRead + AsyncWrite + Unpin> RespClient<S, Resp2> { /* command, command_raw, send_only, … */ }
```

and *then* both aliases get them. That is the formulation; "free" is only true once it is written
this way.

- `pub type TestClient = RespClient<TcpStream, Resp2>` — plus the `Response`-returning
  convenience methods from the stream-generic `impl` block above.
- `pub type TlsTestClient = RespClient<TlsStream<TcpStream>, Resp2>` — gains
  `command_raw`/`send_only`/`read_response` from the same block, closing the asymmetry. Its two
  constructors stay as a `TlsStream`-specific `impl`.
- `Resp3TestClient` **stays a struct**, wrapping `RespClient<TcpStream, Resp3>` and keeping
  `pending_pushes` + the push-confirmation loop (`:1245-1294`) verbatim. Protocol semantics do not
  belong in the transport generic. This is the single most important constraint on the refactor.
- `resp3.rs`'s `Resp2Client` is in a **different crate** from the harness, so it cannot be extended
  by a harness `impl` block — it is **deleted and replaced** by
  `RespClient<TcpStream, Resp2>` used through its frame-returning method (it returns raw
  `Resp2Frame`, not `Response`, so the generic's frame-level `recv` is what it wants).
- `MigrateClient` (`migrate.rs:232-354`) keeps its `auth`/`select_db`/`restore` methods and its
  `Framed<TcpStream, Resp2>` field. Production code taking a dependency on a test crate is
  unacceptable, so **only the frame mapper moves for `migrate.rs`** (seam 1); its client body is
  left alone. Recorded as an explicit non-goal below.
- `TestServer::send_raw` (`:1080-1089`) keeps its byte-in/byte-out signature — several callers want
  exact wire bytes — but its body's single 4096-byte `read()` is replaced with a read-until-a-frame-
  decodes loop, so it stops truncating replies larger than 4 KiB or split across segments.

**Deletion test.** After the change, deleting `resp_client.rs` breaks: the harness's three clients,
`resp3.rs`'s clients, `send_raw`, and the inline `Framed` clients in the two cluster test files.
Deleting it today breaks nothing, because it does not exist and every consumer carries its own
copy. That asymmetry *is* the proposal.

### FR10 — embed, then mutate

**Interface.**

```rust
pub struct TestServerConfig {
    pub config: Config,               // the real thing
    // harness-only knobs with no Config counterpart:
    pub data_dir: Option<PathBuf>,
    pub cluster_bus_listener: Option<frogdb_server::net::TcpListener>,
    pub wait_for_ready: bool,
    // …
}

impl TestServerConfig {
    pub fn with_config(mut self, f: impl FnOnce(&mut Config)) -> Self { f(&mut self.config); self }
}
```

The mapping region (`:441-685`) mostly deletes: there is nothing left to map. Callers move to
`.with_config(|c| c.replication.min_replicas_to_write = 2)` — shorter at the call site than
`TestServerConfig { replication_min_replicas_to_write: Some(2), ..Default::default() }` and,
critically, checked against the real field path by the compiler.

#### FR10 reserved fields and closure ordering

`try_start_with_config` does not merely translate. It **force-writes** a set of fields that make
tests hermetic and parallel-safe, and revision 1 removed that constraint without replacing it. The
forced writes at HEAD:

| Field | Site | Why it is load-bearing |
|---|---|---|
| `server.bind = "127.0.0.1"` | `:441` | never bind a public interface from a test |
| `server.port = 0` | `:442` | OS-assigned; two tests must not fight over 6379 |
| `http.bind = "127.0.0.1"`, `http.port = 0` | `:477-478` | same |
| `admin.bind = "127.0.0.1"`, `admin.port = 0` | `:487-489` | same |
| `tls.tls_port = 0` | `:658` | same |
| `server.enable_debug_command = true` | `:643` | DEBUG SLEEP etc. are how tests drive the server |
| `persistence.data_dir` | `:446` | temp dir, auto-removed |
| `snapshot.snapshot_dir` | `:466-472` | temp dir; the config default `./snapshots` pollutes the source tree |
| `replication.role` | `:493-510` | derived from the `ServerRole` argument, not from config |
| `config.cluster.*` bus address | `:594` | derived from the pre-bound listener's `local_addr()` |

If `with_config(|c| …)` hands tests a whole `Config` with no rule, then
`.with_config(|c| c.server.port = 6379)` either silently loses (closure runs before the forced
writes) or binds a real port and breaks parallel `nextest` (closure runs after). Revision 1
specified no ordering, so both outcomes were live.

**Ruling, three parts:**

1. **Named reserved set.** The fields in the table above are *reserved*. They are documented in one
   place — a `const RESERVED: &[&str]` alongside the code that forces them — so the list cannot
   drift from the code silently.
2. **Fixed ordering: closure first, forced writes last.** `with_config` closures run against the
   `Config`, then `try_start_with_config` applies the reserved writes on top. This makes the
   harness's hermeticity guarantee unconditional: no closure can un-hermeticize a test.
3. **Assert on conflict, do not swallow.** Before applying a reserved write, compare against the
   `Config::default()` value for that field. If a closure changed it, `panic!` with the field name
   and the reserved value. A test that tries to set `server.port` gets a loud, immediately legible
   failure rather than a silent override — and the assertion is what keeps rule 2 from becoming a
   trap.

Reserved fields that need a real escape hatch (a test that genuinely wants a fixed port) get a
named harness knob, the same way `data_dir` already works today — an explicit `Option` on
`TestServerConfig`, not a `Config` write.

#### `Clone`: a manual impl stays

Revision 1 said the 58-line manual `Clone` "becomes `#[derive(Clone)]` on everything except the
`TempDir` field". Two errors: **there is no `TempDir` field** (`data_dir` is
`Option<PathBuf>`, `:52`; the owning `TempDir` lives on `TestServer`, not on the config), and the
actual blocker is `cluster_bus_listener: Option<frogdb_server::net::TcpListener>` (`:154`), which
is not `Clone` and is *deliberately* cloned-to-`None` at `:260` — the doc comment at `:41` says so
in as many words ("Cannot derive `Clone` because `TcpListener` is not `Clone` — has manual Clone
impl that sets `cluster_bus_listener` to `None`"), and the inline comment at `:259` explains the
semantics ("cloned configs always self-bind").

So: **the manual `Clone` stays as long as the listener field exists**, but it shrinks from 58 lines
to roughly five — `Self { config: self.config.clone(), cluster_bus_listener: None, ..
}` — because 51 field-by-field clone lines collapse into one `Config` clone. That is still a real
win, and it is the honest one.

#### FR10b — `from_toml` must NOT route through `ConfigLoader`

Revision 1 proposed `TestServerConfig::from_toml(&str)` "routed through `ConfigLoader`". That is
the wrong plumbing on three counts, verified at `server/src/config/loader.rs`:

1. `ConfigLoader::load` takes **twelve arguments** (`:68-81`) — config path, bind, port, shards,
   log level, log format, admin bind/port, http bind/port/token, and a `TlsCliOverrides` — because
   it is the CLI's entry point, not a parser.
2. It unconditionally merges `Env::prefixed("FROGDB_")` (`:99-106`). Any `FROGDB_*` variable in the
   ambient environment would leak into **every** harness config, making the harness non-hermetic
   under parallel `nextest` — the exact property the reserved-field ruling above exists to protect.
3. It uses `Toml::file(default_path).nested()` on the fallback path (`:91`), which is the bug
   round 2's issue 49 already filed against this function.

**Ruling:** `from_toml` is `toml::from_str::<Config>(s)` plus an optional `config.validate()` —
which is exactly the shape `runtime_config.rs:5660-5669` already uses in unit tests, and exactly
what exercises `deny_unknown_fields`. No figment, no env, no CLI arguments. Given
[drift finding 5's refutation](#drift-finding-5--refuted-and-the-residue-is-narrow), FR10b is now
a small convenience ranked last, and FR10a is fine landing without it.

**Leverage.** Adding a config knob today costs four edits (`Config` field, `TestServerConfig`
field, `Clone` line, mapping line) and gives the test suite a knob nobody uses until someone adds
the plumbing. After: one edit, and every test can reach it immediately. That is why the mirror
keeps accreting single-setter fields (drift finding 3).

**The 16 constructors stay.** `TestServer::start`, `start_primary`, `start_replica`,
`start_with_tls`, … (`:355-1002`) are the harness's real interface and they read well. They get
*shorter*: `start_replica_with_tls` (`:988-999`) becomes three `with_config` lines with no
possibility of a no-op write. **`cluster_harness.rs` is already a good facade** (`connect() :312`,
`send :332`, `try_send :347`) and improves for free — its nine single-setter knobs become direct
`Config` writes and the corresponding `TestServerConfig` fields vanish, except
`cluster_bus_listener`, which is harness-only and stays.

**Deletion test.** After: deleting the `config: Config` field is impossible without deleting the
harness. Today, deleting `tls_handshake_timeout_ms` and `replication_role` from
`TestServerConfig` breaks **nothing** — proven by the setter scan. A struct with removable fields
is a struct whose shape nobody depends on.

## Testability improvement

This proposal's subject *is* the test infrastructure, so "improves testability" has to mean
something sharper than usual. Three concrete things:

1. **The unframed reads that FR9 does reach become sound.** `TestServer::send_raw`
   (`server.rs:1080-1089`) and `resp3.rs:742/759` each do one 4096-byte `read()` and treat the
   result as a complete reply. Over loopback that is usually true and silently wrong when it is
   not — a reply larger than 4 KiB or split by the kernel truncates, and the test asserts on a
   fragment. Routing them through a decoder removes the class. The ~109 single-shot reads in
   `simulation.rs` are the larger instance of the same defect and are **explicitly deferred** by
   the [FR9 scope ruling](#fr9-scope-ruling-turmoil-runners-are-excluded); they are recorded, not
   fixed here.

2. **`RespClient` is unit-testable in isolation; the current clients are not.** `Framed<S, C>`
   over a `tokio_test::io::Builder` mock lets the timeout path, the connection-closed path and
   the partial-frame path be asserted directly. Today those paths are `.expect("timeout")` inside
   a struct that can only be constructed by connecting to a real socket — so the harness's own
   error handling has zero tests. A test harness with untested failure paths turns harness bugs
   into product-bug reports; hardening campaign 2's issue 32 was exactly that (a jepsen harness
   defect that read as a product defect).

3. **An integration-level "boot from TOML" capability.** Per the corrected drift finding 5, the
   deserialize path and `deny_unknown_fields` are already covered by unit tests; what is missing
   is an *integration* test that starts a real server from a config file. `from_toml` supplies it
   cheaply. Modest, and ranked accordingly.

Meta-risk, stated plainly: a bug introduced into `RespClient` is a bug in *every* test that uses
it at once. That argues for landing the generic with its own unit tests **first** (hotfix H4) and
migrating call sites in separate commits — not for leaving thirteen copies in place, which merely
distributes the same risk into thirteen places where nobody looks.

## Risks / scope boundaries

### The riskiest surface: failure-mode spec bindings

**What `scripts/failure-modes.py` binds.** Two things, and only two: (a) whole-line `FM-<AREA>-NNN`
tags in test files, matched by `FM_TAG_LINE_RE` (`:98`), associated to the **next `fn` name**
across a preamble of comments/attributes/blank lines (`PREAMBLE_RE = ^\s*(//|#!?\[|$)`, `:126`);
and (b) backticked test names inside each spec row's `Forced by` field. It binds **test function
names**, not types, not file paths, not module paths.

**Census — do the specs name any harness file, type, or alias?** Grep of
`.scratch/hardening/specs/` for `test-harness`, `test_harness`, `TestServerConfig`, `TestClient`,
`cluster_harness` returns **one** hit: `persistence-failure-modes.md:498`, the string
`test_harness_crash_and_recover` inside a `Forced by` list. That is a **false positive**: the test
lives at `frogdb-server/crates/core/src/persistence/test_harness.rs:577` — `frogdb-core`'s own
persistence test module, an unrelated namespace. **No failure-mode spec references
`frogdb-test-harness`, any of its types, or any of its files.** FR10a therefore cannot break a tag
binding by renaming a type.

**Corrected: renames fail LOUD, in both directions.** Revision 1 treated a silent rebind as the
principal hazard. It is not, because `annotated_fn` (`:453-467`) returns `None` the moment it hits
a line that is neither comment, attribute, nor blank — and `scan_tags` (`:435-438`) turns that
`None` into a **hard error**: `"FM tag is not attached to a test function (only comments and
attributes may follow it, then `fn`)"`. Inserting a non-preamble line between a tag and its `fn`
fails the lint loudly. Renaming a `fn` fails loudly too, from the other side: the spec's
`Forced by` entry no longer resolves.

**The one genuine silent vector** is narrow: `FN_RE = \bfn\s+([A-Za-z_]…)` (`:99`) searches every
line including comments, so a *comment* sitting between the tag and the real `fn` that happens to
contain the token `fn some_name` binds the tag to `some_name`. That is the only way a tag rebinds
without a diagnostic, and it requires writing `fn <ident>` inside an intervening comment.

**Corrected mitigations.** Revision 1's rule ("no line may be inserted between an `FM-…` tag and
its `fn`") is over-strict — the lint explicitly permits comments and attributes, and a
`#[rstest]`/`#[case]` block between tag and `fn` is normal and correct. The accurate constraint:

- **No `fn` in `frogdb-server/crates/server/tests/` or `crates/redis-regression/tests/` may be
  renamed or moved between files.** Call bodies change; signatures do not. Hard constraint.
- **No non-comment, non-attribute, non-blank line may be introduced between an `FM-…` tag and its
  `fn`** — and no intervening comment may contain the token `fn <identifier>`.
- **`just lint-failure-modes` runs per slice, not once at the end.** Corrected cost: it is **not**
  compile-free. `Justfile:322-323` says so in the repo's own words — it is the one member of the
  `lint-*` family excluded from `lint-gates` because it "builds test binaries" — and its own
  header (`Justfile:286`) records "~15-25s warm". It runs `cargo nextest list` across the crate
  list and the turmoil feature variant. Budget a test-binary build per slice; that is the price of
  the only check that can catch a bad binding.
- The turmoil variant is in `NEXTEST_FEATURE_VARIANTS` (`:83`) and must be linted too.

**Scale of the tagged surface.** `frogdb-server/crates/server/tests/` carries **198 `FM-…` mentions
across 20 files**, of which `cluster_migration.rs` has 12, `simulation.rs` 3, and
`cluster_failover.rs` 1. `workload_runner.rs` and `pubsub_runner.rs` carry **none** — but they are
excluded from FR9b anyway by the seed ruling.

**LOCKED-crate check.** The file set contains `frogdb-test-harness`, `frogdb-server` (server crate
+ its `tests/`), `frogdb-redis-regression`, `frogctl` tests, and `frogdb-protocol`. None of
`frogdb-txn`, `frogdb-vll`, `frogdb-persistence`, `frogdb-recovery`, `frogdb-replication`,
`frogdb-replication-runtime`, `frogdb-cluster`, `frogdb-cluster-runtime` is touched. **No mutation
gate applies and `just mutants-diff` is not required** — though the `frogdb-server` integration
tests that force locked-area rows do run through this code, which is precisely why the rename
constraint above is absolute.

**Seam-lint check.** `just lint-gates` (`Justfile:329`, 14 gates) covers clock reads, metrics
emission, redirect replies, durable-ack writes and similar production chokepoints. None of the 14
constrains test-harness client construction, and the proposal adds no production call sites (seam 1
is a pure move into `frogdb-protocol`; `migrate.rs` loses a private fn and gains an import).
`just lint-turmoil-features` (`Justfile:349+`) is a pure `git ls-files` manifest lint over
`Cargo.toml` files and is unaffected by anything here.

### Other risks

- **`migrate.rs` is production code.** `MigrateClient` must not gain a dependency on
  `frogdb-test-harness`. Scope boundary: only the frame mapper is shared, via `frogdb-protocol`.
  Unifying `MigrateClient` onto `RespClient` would require the generic to live in
  `frogdb-protocol` — feasible in principle (the generic never names a concrete stream) but
  `frogdb-protocol` has `tokio` only as a **dev-dependency** (`Cargo.toml:19`), so hosting an async
  client there is a real dependency change. Deliberately **out of scope**.
- **`server.rs` is hot** — 37 commits, 15 since 2026-06-01, shared with concurrent work. A single
  1485-line rewrite will collide. Land in slices and rebase often.
- **`Resp3TestClient` push semantics.** Flattening `:1245-1294` into the generic would break RESP3
  pub/sub tests in ways that look like server bugs. Called out twice on purpose.
- **`TestServerConfig` name churn.** The name is kept; the shape changes. The glob re-export in
  `tests/common/test_server.rs` keeps working and no import line changes — which is also what keeps
  the `fn`-rename constraint satisfiable.
- **Call-site volume.** **213** construction sites across **39 files** in **four crates**
  (`frogdb-server` tests, `frogdb-redis-regression`, `frogctl`, `frogdb-operator`). 100 of them are
  in `integration_replication.rs` alone. Mechanical, but large; `sed`-scale, reviewed in slices,
  and gated by all four test entry points below.
- **Timeout unification is a behavior change** if done carelessly — ruled above: per-method
  defaults preserved verbatim.

### Same-commit gates for FR10a

`TestServerConfig` has consumers that the default `just test` does not build or does not run:

| Gate | Recipe | Why it is required |
|---|---|---|
| `just test` | — | `frogdb-server` tests + `frogdb-redis-regression` (default filter) |
| `just regression` | `Justfile:282-283` | `frogdb-redis-regression` on its own — 15 files, 27 literals; run it explicitly so a shape break is attributed, not buried |
| `just frogctl-test` | `Justfile:290` | `frogctl` tests are **excluded from the default suite**; 2 files construct `TestServerConfig` |
| `just operator-test` | `Justfile:970` | `frogdb-operator` is a **separate workspace**; `just check`/`just test` never build it, so a shape change compiles clean and breaks it silently |

All four run in the same commit as FR10a. Revision 1 named only `just operator-test`.

### Sibling boundaries

Verified on disk at HEAD. Cross-proposal line numbers move as siblings are revised concurrently —
**re-verify every `NN:LLL` cite at land time**; the ones below were correct at `54baa2bb`.

| Sibling | Present? | Edge |
|---|---|---|
| **72** (frogctl↔`frogdb-config`) | yes, 939 lines | **Kinship, not dependency.** Same disease (parallel hand-written schema), cited above. FR10 adds **no** new dep, so 72's `document.rs` move does **not** order this proposal. Fully parallel. |
| **73** (frogctl ops wiring) | yes, 912 lines | One mention of the harness, at `73:617`, noting `frogdb_test_harness::server::TestServer` as a dev-dependency entry. Read-only. No overlap. **New edge:** 73 owns `frogctl`'s test surface, and this proposal now touches `frogctl/tests/integration_health.rs` + `integration_upgrade.rs` (two `TestServerConfig` literals). Contact is two lines in two files; coordinate at land time rather than ordering the proposals. |
| **74** (debug-bundle assembler) | yes | No harness or `migrate.rs` references. No overlap. |
| **75** (frogctl rendering role) | yes (untracked, concurrent author) | frogctl-only. No overlap. |
| **76** (observability extractors) | yes, 796 lines | **Only real read/write contact.** `76:82` lists `test-harness/src/server.rs` as *"Read-only evidence (FR11)"* for `fetch_metrics :883-895`, and `76:703-704` **explicitly declines** to fold that duplicate (folding would force `frogdb-test-harness` to depend on `frogdb-telemetry`'s `testing` feature, pulling `reqwest` + `opentelemetry_sdk/testing`). So 76 reads the file; **78 owns it**. The declined follow-up remains declined here too. |
| **77** (operator resources) | yes, 695 lines (untracked, concurrent author) | **No file overlap**, but 77 surfaces the edge this proposal owns: `frogdb-operator` path-depends on `frogdb-test-harness` and constructs both `TestServerConfig` and `ClusterNodeConfig` literals from a separate workspace. Handled by the `just operator-test` gate above. |
| **90 / CT2** (CommandSpec sweep) | **absent** | Command-identity enums; no contact with transport or config plumbing. |

Net: one file (`test-harness/src/server.rs`) is claimed read-only by 76 and read-write by 78. No
write-write conflict exists across the round.

## Effort

| Part | Effort | Notes |
|---|---:|---|
| H3: frame→`Response` mapper into `frogdb-protocol`, delete both copies | **XS** | 2 files + 1 additive fn — **destination type must be ruled first** |
| H1: delete no-op `replication_role` (4 sites) | **XS** | latent |
| H2: delete producerless `tls_handshake_timeout_ms` (3 lines) | **XS** | latent |
| H4: `RespClient<S, C>` module + unit tests, **no call-site migration** | **S** | lands green, unmigrated |
| FR9a: migrate `TestClient`/`TlsTestClient`/`Resp3TestClient`; fix `send_raw` | **S** | TLS gains 3 methods via the stream-generic `impl` |
| FR9b (**narrowed**): `Resp2Client` + `resp3.rs` raw helpers + the 3 inline `Framed` sites in `cluster_failover.rs`/`cluster_migration.rs` | **S** | turmoil runners **excluded** — see the scope ruling |
| FR10a: embed `Config`, `with_config` + reserved-field guard, delete mapping, shrink `Clone` | **M/L** | 213 sites, 39 files, 4 crates; gates: `test` + `regression` + `frogctl-test` + `operator-test` |
| FR10b: `from_toml` via direct `toml::from_str` + validate | **XS** | small convenience; drift finding 5 refuted, rank last |
| *(deferred)* turmoil-runner migration + full seed-corpus re-pin | **L** | separate issue; method written into the scope ruling |
| **Total** | **M/L** | brief said FR9=S, FR10=M/L; FR9 is S/M with the turmoil exclusion, FR10a is the bulk |

### Independently-landable hotfixes

#### H1 — `replication_role` no-op write. LATENT.

`server.rs:994` sets a field the `ServerRole::Replica` arm never reads (`:502-503` hardcodes the
role). Zero setters elsewhere in the tree. **Reclassified from LIVE to LATENT**: no behavior
differs, because the arm hardcodes the same value the dead write sets; the cost is a misleading
read of the test setup. **Four deletion sites**, not three: field `:89`, `Clone` line `:239`, dead
write `:994`, and the read at `:495-497` which becomes the literal `"standalone"`.

#### H2 — `tls_handshake_timeout_ms` producerless knob. LATENT.

Declared `:174`, cloned `:267`, consumed `:665`, produced nowhere. No current misbehavior; it is
maintenance cost and a template for the next unused mirror field. Three-line deletion. Approved as
written.

#### H3 — duplicate `frame_to_response`. LATENT.

Byte-identical at `server.rs:1343-1354` and `migrate.rs:357-368` (modulo `pub`); the two cannot
currently diverge because neither has changed, but nothing prevents it, and the production copy is
the one that would matter. Moving it to `frogdb-protocol` is additive and non-locked.

**Open decision — the orchestrator must rule the destination type before this lands.** Revision 1
justified the move as "the missing inverse of `WireResponse::to_resp2_frame`". That justification
is wrong (see [above](#frame_to_response-exists-twice-verbatim)): the function produces `Response`,
not `WireResponse`; `to_resp2_frame` sanitizes and is lossy, so the two are not inverses; and the
`WireResponse → Response` direction needed to make them symmetric does not exist. The three
candidates:

- **(i) `Response::from_resp2_frame(BytesFrame) -> Response`** — matches both call sites exactly,
  zero caller churn, but sits on the union type that also carries internal control-flow variants,
  so the name overpromises.
- **(ii) `WireResponse::from_resp2_frame(BytesFrame) -> WireResponse`** plus a
  `WireResponse -> Response` lift — symmetric and honest, but requires a conversion that does not
  exist and touches both callers.
- **(iii) a free `pub fn resp2_frame_to_response` in `frogdb-protocol`** — no symmetry claim at
  all, smallest change, least design content.

The move is worth doing under any of the three; the point is that the choice is a design decision
with different long-run consequences and revision 1 hid it behind a false symmetry claim. Note that
`frogdb-protocol` has `tokio` as a dev-dependency only (`Cargo.toml:19`), which constrains nothing
for a pure function.

#### H4 — `RespClient<S, C>` with unit tests, zero call-site migration. LATENT.

Landing the module green and unmigrated de-risks every subsequent slice and is reviewable in
isolation. Approved. (Note: the review's "H4" referred to the `#[derive(Clone)]` claim, which lives
in FR10's proposed change, not in this hotfix — that claim is
[rewritten above](#clone-a-manual-impl-stays) and the manual `Clone` is retained.)

#### Not a hotfix, and honest about it

The ~109 single-shot reads in `simulation.rs` are **latent-correctness**, not latent-tidiness —
`simulation.rs:4338-4340` documents that they can mis-frame under turmoil chunking. No observed
failure is attributed to them, so the ruling is *latent*. They are **deliberately out of scope**
here by the [FR9 scope ruling](#fr9-scope-ruling-turmoil-runners-are-excluded), because fixing them
means changing turmoil read cadence and therefore re-pinning the seed corpus. They should be filed
as their own issue together with the option-(b) re-pin method, not quietly dropped.

## Review ledger (revision 2)

Adversarial review `2e81506b`, verdict **AMEND**. Every point was re-verified at HEAD before being
applied. Dispositions:

### Blocking

| ID | Disposition |
|---|---|
| **B1** seed determinism | **ACCEPTED — option (a), exclusion.** FR9b no longer touches `RespConn`, `workload_runner.rs`, `pubsub_runner.rs`, `sim_helpers.rs`, or the six `round_trip` bodies. Verified the corpus: `cluster-regression-seeds.txt` = 12 seeds, **10 carrying `EXPECTED-FAILURE:issue-20`**; `just cluster-seeds` `Justfile:211-212`; `FROGDB_CONCURRENCY_SEEDS` `Justfile:163-164`; `mod regressions` rationale `Justfile:126-128`; `just concurrency-repro` `Justfile:137-138`. **Strengthened beyond the review:** `simulation/scheduler.rs:69` *imports* `RespConn` and uses it at 7 sites, so the seed sweep's own client layer was in FR9b's blast radius; and the seed file's `family` guard (`Schedule::from_seed(seed).family`) watches the seed→schedule derivation, **not** the schedule→execution interleaving, so it would not have caught a read-cadence change. Option (b) recorded as a follow-up with method. |
| **B2** census | **ACCEPTED.** All five named sites verified and added: `cluster_failover.rs:562` + `:3332`, `cluster_migration.rs:399`, `resp3.rs:729/742/759`, `server.rs:1080-1089 send_raw`. Census restated as **13 sites / 10 files**; the Summary/enumeration/matrix inconsistency ("seven" vs 8 vs "six") is gone. FM-tag reconciliation: `cluster_migration.rs` 12 tags, `cluster_failover.rs` 1, `simulation.rs` 3, both turmoil runners 0. |
| **B3** missing consumer crate | **ACCEPTED and extended.** `frogdb-redis-regression` confirmed: 15 files, 27 literals, workspace member `Cargo.toml:34`, default nextest filter. Added to the Files table and the gate list with `just regression` (`Justfile:282-283`). Literal count confirmed **213** (215 raw minus `struct` `:43` and `impl Clone` `:227`). **New finding beyond the review:** `frogctl` is a *fourth* consumer (`frogctl/tests/integration_health.rs:52`, `integration_upgrade.rs:18`) and is **excluded from the default suite** (`Justfile:290`), so `just frogctl-test` joins the same-commit gate set. Files mentioning `TestServerConfig` = 39 tree-wide, 36 inside `frogdb-server/`. |
| **B4** drift finding 5 | **ACCEPTED — both halves refuted, rewritten.** Verified: `test_config_rewrite_sanity` `integration_admin.rs:1680-1723` → `runtime_config.rs::rewrite_config :1309-1336` → `ConfigPersister::merge :64` → `let mut doc: DocumentMut = doc_text.parse()` `:68-70`, error surfaced as `ERR failed to parse config file` — **the TOML is parsed**. `deny_unknown_fields` is tested at `config/src/lib.rs:429` *and* `server/src/config/mod.rs:438`, plus `runtime_config.rs:5660-5669` `rewrite_and_reparse` does `toml::from_str::<Config>` + `validate()`. Only the narrow claim survives (no *integration* test boots from a file); FR10b restated at much-reduced weight and combined with N13. |
| **B5** reserved fields | **ACCEPTED.** All forced writes verified at HEAD: `server.bind`/`port=0` `:441-442`, `http.bind`/`port=0` `:477-478`, `admin.bind`/`port=0` `:487-489`, `tls.tls_port=0` `:658`, `enable_debug_command=true` `:643`, temp `persistence.data_dir` `:446`, temp `snapshot.snapshot_dir` `:466-472`, `replication.role` from `ServerRole` `:493-510`, cluster bus addr from the pre-bound listener `:594`. Ruled: named reserved set, closure-first/forced-writes-last ordering, panic on conflict, named harness knobs as the escape hatch. |

### Spec-binding

| ID | Disposition |
|---|---|
| **N1** | **ACCEPTED.** `lint-failure-modes` is **not** compile-free — `Justfile:322-323` excludes it from `lint-gates` explicitly because it "builds test binaries", and `Justfile:286` records "~15-25s warm". Revision 1's "(compile-free, `Justfile:293-294`)" is deleted; the per-slice cost is stated. |
| **N2** | **ACCEPTED.** Verified `annotated_fn` `:453-467` returns `None` on the first non-preamble line and `scan_tags` `:435-438` turns that into a hard error. Renames fail loud both directions. Only silent vector = `FN_RE` `:99` matching `fn <ident>` inside an intervening comment. Mitigation relaxed from "no line may be inserted" to "no non-comment, non-attribute line, and no intervening comment containing `fn <ident>`". |
| **N3** | **CONFIRMED, not blocking.** 198 `FM-` mentions / 20 files verified; `FM_TAG_LINE_RE :98` is whole-line; `persistence-failure-modes.md:498` is a false positive resolving to `core/src/persistence/test_harness.rs:577`. FR10a cannot break a tag binding silently. |

### Drift rulings

| ID | Disposition |
|---|---|
| 1 | **ACCEPTED.** Reclassified LIVE → LATENT; deletion is **4 sites** (`:89`, `:239`, `:495-497` → literal `"standalone"`, `:994`). Revision 1's LIVE/no-op self-contradiction removed from both the finding and H1. |
| 2 | **CONFIRMED**, unchanged. |
| 3 | **ACCEPTED.** Count kept (**22/51 = 43%**: 1 zero-setter + 21 single-setter), enumeration corrected: the nine are `cluster_harness.rs:180-186` (seven) **plus** `cluster_enabled :179` and `cluster_bus_listener :187` — **not** the `tls_*_file` knobs, of which there are **five** (`:193-199`), each with 2-4 setters (`tls_cert_file` measured at 5). |
| 4 | **ACCEPTED.** `ClusterNodeConfig` is **12** fields (`cluster_harness.rs:73-100`), not 13. |
| 5 | **REFUTED** — see B4. |
| 6 | **CONFIRMED**, with the review's added note that `sim_helpers.rs` is turmoil-gated and so never co-compiles with the harness today. |
| **N10** | **ACCEPTED.** Headline deflated: measured mutation region `:441-685` = **64** `config.… = …` assignments, **37** `if let Some(…)`, not "319 lines / ~150 mapping lines". **8 of 51 fields are plain `bool`** — verified — which changes the absent-means-default story; that argument now appears in the FR10 §Shape section as a *stronger* reason for embedding than line count. |

### Remaining

| ID | Disposition |
|---|---|
| **N4** | **ACCEPTED.** `C: Encoder<C::Frame>` does not compile — verified `redis-protocol-6.0.0/src/codec.rs:208,228-230` (`Encoder<Resp2Frame>`, `Decoder::Item = Resp2Frame`) and `:78,98-100` for RESP3. Bound replaced with `C: Decoder + Encoder<<C as Decoder>::Item>`. |
| **N5** | **ACCEPTED.** "TlsTestClient gains 4 methods free" replaced with the actual formulation: a stream-generic `impl<S: AsyncRead + AsyncWrite + Unpin> RespClient<S, Resp2>` block, since an inherent impl on an alias does not reach the TLS instantiation. `resp3.rs`'s `Resp2Client` is in a different crate → **replaced**, not extended. Also corrected: `TlsTestClient` has 3 methods (2 constructors + `command`), so the gain is 3, not 4. |
| **N11** | **ACCEPTED.** "same eleven lines" replaced with "same algorithm, divergent incidental text" (verified: `expect("timeout waiting for response")` vs `expect("timeout")`, `.map(frame_to_response).expect(…)` vs the reverse). Timeout unification **ruled explicitly**: per-method defaults preserved verbatim (`command` 15s `:1166`, `command_raw` 5s `:1186`, `TlsTestClient::command` 15s `:1467`, `Resp2Client` 5s, raw helpers 2s). No promotion to a single field default. |
| **N13** | **ACCEPTED.** `from_toml` must **not** route through `ConfigLoader`. Verified `server/src/config/loader.rs`: 12-arg `load` `:68-81`, `Toml::file(default_path).nested()` `:91`, unconditional `Env::prefixed("FROGDB_")` `:99-106`. Replaced with direct `toml::from_str::<Config>` + optional `validate()`, matching `runtime_config.rs:5660-5669`. Further thins B4's residual; FR10b ranked last. |
| **N14** | **ACCEPTED and corrected upward.** Measured **110** socket reads in `simulation.rs` (104 `.read(&mut …)` + 6 `stream.read(buf)` in the `round_trip` bodies), not 108; `RespConn`'s at `:4373` correctly loops → ~109 true single-shot. Soundness argument unaffected. |
| **N15** | **ACCEPTED.** Parser-deletion claim narrowed: the three hand-rolled parsers decode into three *different* domain enums (`RespValue` / `OperationResult` / `RespVal`) consumed by the linearizability checkers, so each still needs a frame→domain mapper. `find_crlf`/`parse_at` delete; the mapping does not. (Moot for this proposal's scope after the B1 exclusion, but recorded for the deferred follow-up.) |
| **N12** | **ACCEPTED.** 72/73 cites were stale from concurrent revision, not errors. Refreshed at HEAD: 72 = **939** lines; 73 = 912 lines, harness mention at **`73:617`**; 76 = **796** lines, harness read-only evidence at **`76:82`**, `fetch_metrics` decline at **`76:703-704`** (the review's own `:523-528` was itself stale); 77 = 695 lines. Sibling table now carries an explicit "re-verify at land time" note. |
| cite drift | **FIXED.** `Config` struct = `config/src/lib.rs:84-228` (was `:88`); `config_source_path` field `:90`; harness `config_source_path` write = `server.rs:647` (was `:646-648`); `cluster_harness` `try_send` = `:347` (was `:348`); `ClusterNodeConfig` = `:73-100`. |

### Confirmed and kept unchanged

Two-axes diagnosis · `Resp3TestClient` push-filtering must remain an adapter · `sim_helpers.rs`
as the FR10 exemplar · drift findings 1/2/4/6 · `turmoil::net::TcpStream: AsyncRead + AsyncWrite`
(`turmoil-0.7.1/src/net/tcp/stream.rs:433,457`) · `redis-protocol-6.0.0` codec `Encoder`/`Decoder`
availability · no new dependency · LOCKED-crate clearance · `lint-turmoil-features` unaffected
(pure `git ls-files` manifest lint) · `frame_to_response` byte-identical modulo `pub` ·
proposal 72 does not gate 78 · `just operator-test` as a same-commit gate.
