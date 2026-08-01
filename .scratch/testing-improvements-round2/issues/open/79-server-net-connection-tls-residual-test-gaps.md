# server net / connection / TLS — residual test gaps (13 findings)

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/03 — residual findings after promotion to issues 19–76
Score: 13 findings, priority range 10–15
Area: frogdb-server — `acceptor.rs`, `net.rs`, `connection.rs`, `connection/**`, `server/**`, `commands/**`, `scatter/**`, `monitor.rs`, `cursor_store.rs`, `tls.rs`, `tls_runtime.rs`, `tls_watch.rs`

## Context

This area is the server's front door: the accept loop and maxclients gate, the connection task and
its pre-dispatch gauntlet, routing and scatter/gather, MONITOR, the FT.AGGREGATE cursor store, and
the whole TLS surface including the recently added reload/multi-cert machinery. **89 files,
19108/22664 lines = 84.3 %** line coverage — roughly the workspace average, so percentage carries
no signal here; depth classes over 2473 deduplicated functions are `well-covered` 928,
`single-test` 734, `untested` 331, `monoculture` 313, `covered` 165, `hot-but-shallow` 2 (both
`hot-but-shallow` functions in the workspace are here: `routing.rs:164 dispatch_scatter` and
`scatter/strategies.rs:122 MSetStrategy`). The proposal's verdict on the shape of that coverage:
"the happy paths in this area are heavily exercised … but **every partial-failure and every
abnormal-teardown path is either untested or tested only against internal state**." Note the
proposal's `## Deprioritised` section carries no F-numbers, so nothing there is a finding — its
`connection/builder.rs` entry (0/175 lines, zero call sites, "delete it, do not test it") is
claimed by the dead-code sweep, issue 34, `.scratch/testing-improvements-round2/issues/`.

## Promoted elsewhere

- F1 → issue 61, `.scratch/testing-improvements-round2/issues/` (scatter/gather merges discard
  per-shard errors — partial failure replies as success) **and** issue 23,
  `.scratch/testing-improvements-round2/issues/` (theme T5 — partial failure reported as total
  success).
- F8 → issue 23, `.scratch/testing-improvements-round2/issues/` (theme T5 —
  `scatter_error_to_response` untested, every shard-failure reply shape unverified).
- F2 → issue 33, `.scratch/testing-improvements-round2/issues/` (§4 tests that cannot fail — 15
  COPY integration tests never exercise cross-shard COPY because `src` and `dst` both hash to
  shard 2 of 4).
- F3 → issue 65, `.scratch/testing-improvements-round2/issues/` (a client that disconnects while
  blocked leaks the connection, the shard waiter and a maxclients slot forever).
- F10 → issue 39, `.scratch/testing-improvements-round2/issues/` (`HELLO … AUTH user pass` leaks
  the password into the MONITOR feed).

## Residual findings

### F4 — maxclients accounting is per-listener, check-then-act, and not RAII

- **Severity** 4 — the limit is the only guard against connection-driven memory exhaustion. Three distinct ways to exceed it, none tested.
- **Likelihood** 3 — needs the server to be at or near the limit, which is exactly when the guard matters.
- **Effort** 3 — server integration with concurrent connects and a TLS port.
- **Priority** 15
- **Evidence**: (a) the gate loads the counter in the accept loop (`acceptor.rs:224`) but the increment happens in the spawned task *after* the TLS handshake (`acceptor.rs:322`) — N concurrent connects at the limit all pass the gate. (b) `acceptor.rs:353` decrements with a plain statement after `handler.run().await`; a panic inside the connection task unwinds past it and burns the slot permanently (contrast `ClientHandle`, which *is* RAII — `connection.rs:127` "auto-unregisters on drop"). (c) `current_connections` is minted per `bind` (`acceptor.rs:191`), which the existing test documents as intentional for the admin port (`acceptor.rs:535-540`) — but it means a TLS-enabled server enforces maxclients **twice**, once per port, and `ConnectionsCurrent::set` is written from whichever port's task ran last, so the `connections_current` gauge is wrong whenever both ports are live. The three `acceptor.rs` tests assert `Arc::ptr_eq` on struct fields and never accept a connection.
- **Proposed test**: (1) with `maxclients = N`, spawn N+8 simultaneous connects; assert exactly N succeed and the rest get `-ERR max number of clients reached`. (2) With TLS enabled and `maxclients = N`, fill the plaintext port to N, then assert the TLS port also refuses (pins the intended global-vs-per-port contract either way). (3) Assert `INFO clients`' `connected_clients` matches the true count with both ports in use.
- **Boundary**: 4 — the race is between the accept loop and the spawned task; only a real listener reproduces it. The three existing internal-state tests should be **moved up** to this level.

### F5 — CLIENT KILL is a silent no-op against a blocked or write-stalled connection

- **Severity** 4 — `CLIENT KILL` is the operator's only escape hatch for a stuck client; it returns `1` (killed) while the connection lives on. Combined with F3, there is no way to reclaim a leaked connection short of restarting the server.
- **Likelihood** 3 — killing a client stuck in `BLPOP`/`XREAD BLOCK 0`, or a subscriber whose TCP window is full, is a routine ops action.
- **Effort** 3 — server integration; needs a second (admin) connection to issue the kill.
- **Priority** 15
- **Evidence**: `core/src/client_registry/mod.rs:575 kill_by_id` (and `:616 kill_by_filter`) send only `kill_tx`; they do not touch the unblock signal. `killed()` is polled only in the top-level `tokio::select!` (`connection.rs:559`), which is not running while the connection is inside `handle_blocking_wait` (`connection/blocking.rs:59`) or inside the pubsub/invalidation/MONITOR arms awaiting `flush_responses()` on a stalled socket (`connection.rs:600`, `:681`). `CLIENT UNBLOCK` works; `CLIENT KILL` does not.
- **Proposed test**: client A issues `BLPOP k 0`; client B issues `CLIENT KILL ID <A>`; assert A's socket reaches EOF within a bound **and** `blocked_clients` returns to 0. Second case: kill a subscriber whose socket is not being read.
- **Boundary**: 4 — two live connections and a real socket close are the behaviour under test.
- **Cross-area (from the proposal)**: the fix is in `frogdb-core` — `ClientRegistry::kill_by_id` / `kill_by_filter` in `core/src/client_registry/mod.rs` do not fire the unblock signal.

### F6 — The connection-level MIGRATE handler is 174 untested regions on a delete-after-transfer path

- **Severity** 4 — MIGRATE deletes the local key after a claimed-successful `RESTORE` on the target. A misread of the target's reply is unrecoverable data loss.
- **Likelihood** 3 — MIGRATE is the manual resharding / key-relocation tool; used deliberately, but used.
- **Effort** 3 — two `TestServer`s and a MIGRATE between them; the harness already starts pairs.
- **Priority** 15
- **Evidence**: `connection/persistence_handler.rs:30 handle_migrate_command` is `untested` (174 regions, the largest untested function in this scope); it is live code, dispatched from `connection/dispatch.rs:288` (`Response::MigrateNeeded { args }`). Its doc comment lists exactly the risky steps: "Connect to the target server / Authenticate if needed / Send RESTORE command(s) / Delete local key(s) if not COPY". The whole file sits at 40.9% lines.
- **Proposed test**: MIGRATE a key (and a `KEYS`-form multi-key MIGRATE) between two `TestServer`s; assert the value and TTL land on the target and the source key is gone; with `COPY`, assert the source key survives; with `REPLACE` absent and the destination occupied, assert `BUSYKEY` and that **the source key is still present** (the data-loss case); with the target unreachable, assert an error **and** that the source key is untouched.
- **Boundary**: 4 — the handler dials a real socket to a second server.
- **Cross-area**: `migrate.rs` proper is another agent's; this is the connection-level handler only.

### F7 — The CLIENT TRACKING invalidation channel is unbounded — no slow-consumer bound, unlike pubsub

- **Severity** 4 — a tracking client that stops reading grows a per-connection `mpsc::unbounded` queue until the server OOMs. Round 1 (issue 29) closed exactly this hazard for pubsub; the sibling channel was left unbounded.
- **Likelihood** 3 — client-side caching is opt-in, but `CLIENT TRACKING ON BCAST` with a stalled or paused client (GC pause, debugger, saturated link) is an ordinary failure.
- **Effort** 3 — server integration: enable tracking, stop reading, drive writes, assert the connection is dropped (or memory stays bounded).
- **Priority** 15
- **Evidence**: `core/src/tracking.rs` — `pub type InvalidationSender = mpsc::UnboundedSender<InvalidationMessage>`. The connection's drain arm (`connection.rs:617-640`) has no overflow branch, in explicit contrast to the pubsub arm two branches above it which handles `Drained::Overflowed` and `rx.has_overflowed()` (`connection.rs:573`, `:610`) and increments `PubsubOutputBufferDisconnects`. `rg` over `server/tests/` finds no invalidation-backlog test.
- **Proposed test**: enable `CLIENT TRACKING ON BCAST` on a connection that never reads; from a second connection write M keys; assert either the tracking client is disconnected with a metric (mirroring the pubsub contract) or that server RSS/`CLIENT INFO tot-mem` stays bounded.
- **Boundary**: 4 — the bound is a property of the connection's channel wiring, and the test needs a reader that refuses to read.
- **Cross-area**: the fix (a bounded channel) is in `frogdb-core`'s tracking module.

### F9 — TLS certificate reload has never been exercised through a live listener

- **Severity** 4 — a broken reload either serves an expired/revoked certificate indefinitely or fails every new handshake — a full outage that only manifests at rotation time, i.e. when nobody is watching.
- **Likelihood** 3 — cert rotation is a routine ops event (cert-manager, Let's Encrypt); `watch_certs` is opt-in but is the feature's whole point.
- **Effort** 4 — needs harness work: `TestServerConfig` has no `watch_certs`, `TlsFixture` can only `generate()` one cert set.
- **Priority** 14
- **Evidence**: `rg watch_certs` over `server/tests/` and `test-harness/src/` returns **nothing**. `test-harness/src/server.rs:147-167` lists eleven `tls_*` config fields, none of them `watch_certs` or `additional_certs`; `test-harness/src/tls.rs` exposes exactly one function, `generate()`. All six `tls_watch.rs` tests are `single-test` and drive `TlsRuntimeHandle` in-process (`tls_watch.rs:239`, `:274`), so the watcher-spawn wiring at `server/subsystems.rs:621-623` and the `manager()`→`acceptor()`-per-connection contract at `subsystems.rs:357,572,597` are unproven end-to-end. `tls.rs:111 MaybeTlsStream::poll_shutdown` is `untested`.
- **Proposed test**: start a server with `watch_certs = true`; open a TLS connection and record the peer certificate serial; rewrite cert+key on disk with a freshly generated pair; poll until a **new** connection presents the new serial while the **pre-existing** connection keeps working; then write a corrupt key and assert new connections still get the last-good certificate (the `broken_rotation_keeps_the_old_certificate_serving` unit test, promoted to the real listener).
- **Boundary**: 4 — the unit tests already cover the decision logic correctly; what is missing is precisely the wiring, which only a real listener can show.
- **Cross-area / shared infra**: needs `TestServerConfig.tls_watch_certs` + `TlsFixture::regenerate_in_place()`.

### F11 — FT.AGGREGATE cursors have no owner and no disconnect cleanup; the only reclaim path never runs in tests

- **Severity** 3 — an abandoned `FT.AGGREGATE … WITHCURSOR` pins its entire materialised row set until the timeout; a client that disconnects mid-paging leaks it. The retain predicate that is supposed to free them has never executed under test.
- **Likelihood** 3 — cursor paging exists to stream large result sets, so abandoning one mid-page is the normal failure of the normal use.
- **Effort** 2 — `AggregateCursorStore` is a standalone type; a crate-level test can insert, advance the clock (or use a tiny timeout), and call `evict_expired`.
- **Priority** 13
- **Evidence**: `cursor_store.rs:106 evict_expired` is `well-covered` only because the 30-second background task (`server/subsystems.rs:476-483`) calls it 3714 times — but its retain **closure** at `cursor_store.rs:109` is `untested`, i.e. no test has ever had a cursor in the map when it ran. `read_cursor` validates only `state.index_name != expected_index`, never the owning connection, and is `monoculture` (2 tests, both `connection::conn_command::tests::ft_cursor_*`). Nothing in `connection/lifecycle.rs`'s teardown touches the cursor store, and there is no cap on live cursors.
- **Proposed test**: insert two cursors with a 50 ms timeout, read one to keep it fresh, sleep past the timeout, call `evict_expired`, assert exactly the stale one is gone and the fresh one still pages correctly; plus an integration test that opens a `WITHCURSOR` aggregate, drops the socket, and asserts the cursor is reclaimed.
- **Boundary**: 2 for the eviction predicate (the store is a self-contained type); 4 for the disconnect-reclaim half.

### F12 — The accept loop busy-spins on a persistent accept error (EMFILE/ENFILE)

- **Severity** 4 — under fd exhaustion the loop spins at 100% CPU emitting an `error!` per iteration, which floods logs and starves the runtime — turning a recoverable resource shortage into a hard outage.
- **Likelihood** 2 — needs fd exhaustion or an ENOBUFS-class error, but low `ulimit -n` in containers makes it reachable.
- **Effort** 3 — needs an fd-exhaustion fixture or a listener fault seam.
- **Priority** 13
- **Evidence**: `acceptor.rs:362-364` — `Err(e) => { error!(error = %e, "Failed to accept connection"); }` with no backoff and no classification of fatal vs transient. Redis handles `ANET_ERR` with a rate-limited log and continues; nginx sleeps on EMFILE.
- **Proposed test**: lower `RLIMIT_NOFILE` in a child, saturate it, assert the process makes forward progress (responds to a later connect once fds free up) and does not emit more than K log lines per second.
- **Boundary**: 4 — needs a new fault primitive (rlimit fixture). See Deprioritised if the fixture is judged too costly; the cheap alternative is a unit test on an extracted `classify_accept_error` + backoff helper, which does not exist yet.

### F16 — `CLIENT INFO`/`CLIENT LIST` always report zero pending output, hiding exactly the slow consumers this area leaks

- **Severity** 2 — wrong observability field, not wrong data. But it is the field an operator would use to diagnose F3/F5/F7, so it converts a diagnosable incident into an undiagnosable one.
- **Likelihood** 4 — every `CLIENT LIST` on every server reports it.
- **Effort** 2 — assert over the RESP-visible `CLIENT INFO` fields.
- **Priority** 12
- **Evidence**: `connection/lifecycle.rs:234 compute_client_memory` (44 regions) reads `self.framed.read_buffer().len()` for `qbuf` but hardcodes `output_list_len = 0` and `output_list_mem = 0`; the pubsub backlog, the invalidation backlog and the `Framed` write buffer are all excluded. `server/src/info/sections.rs:126` likewise hardcodes `client_recent_max_output_buffer` to `0`.
- **Proposed test**: with a subscriber that is not reading and a publisher flooding it, assert `CLIENT LIST`'s `omem`/`tot-mem` for that client grows above zero before the disconnect fires (this pairs with the round-1 pubsub bound test, which already builds the flood).
- **Boundary**: 4 — the value only becomes non-zero with a real stalled socket.
- **Note**: per the user's standing preference (observability accuracy over Redis parity), hardcoded-zero fields are worse than absent ones; the finding is as much "delete or fill" as "test".

### F13 — Multi-certificate selection is never driven by a real ClientHello

- **Severity** 3 — the wrong certificate for a client's signature algorithms means handshake failures for a subset of clients, or serving the fallback certificate where a SAN-specific one was configured.
- **Likelihood** 3 — `additional_certs` is a new feature; anyone who configures it is by definition in the mixed-algorithm case it exists for.
- **Effort** 4 — needs a second `TlsFixture` (ECDSA) and `TestServerConfig.tls_additional_certs`.
- **Priority** 11
- **Evidence**: `tls.rs:428-440` builds the resolver over `config.additional_certs`; the resolver tests (`tls.rs:748`, `:847`, `:861 server_config_builds_with_additional_certs_under_mtls`) and `tls_runtime.rs:518 additional_certs_can_be_added_at_runtime` are `monoculture` unit tests over a synthetic ClientHello / the handle's config. No integration test sets `additional_certs` — `TestServerConfig` cannot express it.
- **Proposed test**: server with an RSA primary + an ECDSA additional cert; connect with an ECDSA-only client and assert the presented certificate is the ECDSA one; connect with an RSA-only client and assert the RSA one; connect with a client matching neither and assert the documented fallback.
- **Boundary**: 4 — algorithm negotiation is rustls's job; only a real handshake proves the wiring.
- **Cross-area / shared infra**: shares the harness work with F9 (`TlsFixture` variants).

### F14 — The RESP3 write path bypasses the `Framed` buffer, so a mid-pipeline protocol switch can reorder bytes

- **Severity** 3 — bytes delivered out of order on the wire desynchronise the client's parser; the client sees replies attributed to the wrong commands.
- **Likelihood** 2 — requires `HELLO 3` (or a `RESET`) pipelined behind RESP2 commands in the same read, which real clients rarely do but connection-pool warmup code sometimes does.
- **Effort** 2 — the existing `frame_io.rs` tests already drive the feed path directly.
- **Priority** 11
- **Evidence**: `connection/frame_io.rs:139-149` — the RESP3 arm encodes into `self.resp3_buf` and calls `self.framed.get_mut().write_all(...)` directly, jumping ahead of anything already buffered in the RESP2 `Framed` write buffer. `protocol_version` is flipped mid-pipeline with no intervening flush by `ConnStateMut::set_protocol_version` (`connection/auth_conn_command.rs:54`, HELLO) and by `ConnectionState::reset` (`connection/state.rs:1278`, RESET). The two existing ordering tests (`frame_io.rs:220 resp2_null_array_feed_order_is_preserved`, `:257 resp3_null_array_feed_order_is_preserved`, both `single-test`) pin round 1's `NullArray` fix only — each runs in a single protocol.
- **Proposed test**: feed `PING`, then `HELLO 3`, then `PING` in one pipeline without an intervening flush; assert the three replies arrive in issue order (the RESP2 `+PONG` must not be overtaken by the RESP3-encoded reply). Assert on the raw byte stream, not on a parsed client.
- **Boundary**: 1 — `frame_io.rs`'s existing tests already exercise this at unit level over an in-memory duplex; keep it there.

### F15 — Shard-local command implementations reachable only from Lua are effectively 0% covered

- **Severity** 3 — `redis.call('INFO')` inside a script executes on the owning shard through a separate implementation from the connection-level INFO. Wrong or panicking output there breaks the script (and a panic on the shard worker is a much bigger event than a bad reply).
- **Likelihood** 2 — scripts that call INFO/FLUSHDB exist but are not the common case.
- **Effort** 2 — `shard_driver` can dispatch these commands directly.
- **Priority** 11
- **Evidence**: `commands/info.rs` is **3/398 lines (0.8%)** — its module doc states it "exists only for scripts (`redis.call('INFO')` executes on the owning shard)". Its section builders are among the largest untested functions in scope (74/44/37/36 regions). `commands/server.rs:76` and `:124` (`Flushdb`/`Flushall` `execute`, 14 regions each) are `untested` for the same reason — the connection-level FLUSHDB goes through `ScatterOp`, never through these.
- **Proposed test**: dispatch `INFO`, `INFO memory`, `INFO keyspace`, `INFO everything` and `FLUSHDB`/`FLUSHALL` through the `shard_driver` harness; assert each requested section header is present, that no server-level-only field is emitted as a placeholder zero (the module doc's stated invariant), and that FLUSHDB empties the shard.
- **Boundary**: 3 — real command dispatch on a real shard worker, no socket. Testing this through a full EVAL over RESP would be the anti-pattern the brief calls out.
- **Cross-area**: the scripting agent owns `EVAL`; this is the shard-local command impl only.

### F17 — Rate-limit boundary, refill and MULTI interaction are untested

- **Severity** 2 — an off-by-one at the limit or a refill that never fires means a tenant is throttled early or not throttled at all; noisy-neighbour, not corruption.
- **Likelihood** 3 — any deployment that configures per-user rate limits sits on the boundary continuously.
- **Effort** 2 — the limiter is reachable from a crate-level test; the ACL surface already has one.
- **Priority** 10
- **Evidence**: `tests/integration_ratelimit.rs`'s 9 tests all drive well past the limit and assert "eventually errors"; none asserts the Nth command succeeds and the (N+1)th fails, none crosses a refill window, none combines the command and byte limits, and none checks whether a command queued in MULTI is charged at queue time, at EXEC time, or both. `connection/guards.rs:116 is_rate_limit_exempt` and `:123 check_rate_limit` are called from `connection.rs:374`, before dispatch and after the QUIT shortcut — so the exempt list's interaction with MULTI/EXEC is entirely unpinned.
- **Proposed test**: exactly-N-then-fail at the boundary; sleep one window and assert the budget refilled to exactly N again; a `MULTI` of K commands under a limit of K asserting the documented charging point; bytes and commands limits both configured, asserting whichever trips first.
- **Boundary**: 4 for the boundary/refill assertions (the limiter state is per-ACL-user and lives in the running server); 1 for the exempt-list membership.

### F18 — PSYNC handoff returns early and skips `notify_connection_closed`

- **Severity** 3 — per-connection state registered on the shards (pubsub subscriptions, tracking registrations, blocking waiters) is never torn down for a connection that ran ordinary commands before issuing PSYNC. The shards keep sending to a channel nobody drains.
- **Likelihood** 2 — requires a connection that subscribes/tracks/blocks and *then* becomes a replica link, which is unusual but is exactly what a misbehaving or probing client does.
- **Effort** 3 — server integration with a raw PSYNC.
- **Priority** 10
- **Evidence**: `connection.rs:826-832` — the PSYNC arm `return Ok(())`s with the comment "Don't run normal cleanup - replication handler has the connection", skipping the `self.notify_connection_closed().await` at `connection.rs:835`. That call is itself conditional (`connection/lifecycle.rs`: the shard broadcast only fires `if self.state.in_pubsub_mode() || self.state.tracking().enabled`), so the intended cleanup exists and is simply bypassed here. Note also that `current_connections` **is** decremented for a handed-off connection while its socket lives on inside the replication handler, so a replica link is uncounted against maxclients.
- **Proposed test**: on one connection, `SUBSCRIBE ch`, then send `PSYNC ? -1`; assert the shards' subscriber count for `ch` drops to zero (via `PUBSUB NUMSUB` from a second connection) rather than retaining the handed-off connection.
- **Boundary**: 4 — the handoff only exists at the connection layer.

## Acceptance criteria

- [ ] F4: a test asserts that with `maxclients = N` and N+8 *simultaneous* connects exactly N succeed and the rest receive `-ERR max number of clients reached`; a second asserts the intended global-vs-per-port contract with a TLS port also at the limit; a third asserts `INFO clients`' `connected_clients` matches the true count with both ports live.
- [ ] F5: a test asserts that `CLIENT KILL ID <A>` against a connection blocked in `BLPOP k 0` drives A's socket to EOF within a bound and returns `blocked_clients` to 0, plus the same for a subscriber whose socket is not being read.
- [ ] F6: a test asserts MIGRATE between two `TestServer`s moves value and TTL and removes the source key; that `COPY` leaves the source; that an occupied destination without `REPLACE` returns `BUSYKEY` **and leaves the source key present**; and that an unreachable target errors with the source untouched.
- [ ] F7: a test asserts that a `CLIENT TRACKING ON BCAST` connection which never reads is either disconnected with a metric (mirroring the pubsub contract) or that its `CLIENT INFO tot-mem` stays bounded while a second connection writes M keys.
- [ ] F9: a test asserts that with `watch_certs = true` a rewritten cert+key on disk is presented to *new* connections (new serial) while a pre-existing connection keeps working, and that a subsequently corrupt key leaves new connections served the last-good certificate.
- [ ] F11: a test asserts `evict_expired` removes exactly the stale cursor and leaves a freshly-read one pageable, plus an integration test asserting a `WITHCURSOR` aggregate's cursor is reclaimed after the socket drops.
- [ ] F12: a test asserts the server makes forward progress after fd exhaustion (a later connect succeeds once fds free) and emits no more than K accept-error log lines per second — or, if the rlimit fixture is declined, a unit test asserts an extracted `classify_accept_error` + backoff helper.
- [ ] F16: a test asserts `CLIENT LIST`'s `omem`/`tot-mem` for a non-reading flooded subscriber grows above zero before the disconnect fires (or the hardcoded-zero fields are removed).
- [ ] F13: a test asserts an ECDSA-only client is served the ECDSA additional certificate, an RSA-only client the RSA primary, and a client matching neither the documented fallback — over a real handshake.
- [ ] F14: a `frame_io.rs` unit test asserts that `PING`, `HELLO 3`, `PING` fed in one pipeline without an intervening flush produce replies in issue order on the raw byte stream.
- [ ] F15: `shard_driver` tests assert `INFO`, `INFO memory`, `INFO keyspace`, `INFO everything` each emit the requested section headers with no server-level-only field rendered as a placeholder zero, and that `FLUSHDB`/`FLUSHALL` dispatched shard-locally empty the shard.
- [ ] F17: a test asserts the Nth command succeeds and the (N+1)th fails at the rate limit, that one window later the budget is exactly N again, that a `MULTI` of K commands is charged at the documented point, and that with both byte and command limits configured whichever trips first is the one reported.
- [ ] F18: a test asserts that a connection which `SUBSCRIBE`s and then sends `PSYNC ? -1` leaves `PUBSUB NUMSUB ch` at zero from a second connection.

## Depends on

- Infrastructure I9 (TLS harness extension — `TestServerConfig.tls_watch_certs` and
  `tls_additional_certs`; `TlsFixture` gains an ECDSA variant and an in-place regeneration helper so
  rotation can happen while the server runs) — issue 09,
  `.scratch/testing-improvements-round2/issues/`. Needed by F9 and F13; `test-harness/src/tls.rs`
  exposes exactly one function, `generate()`, and `test-harness/src/server.rs:147-167`'s eleven
  `tls_*` fields include neither knob. `INFRASTRUCTURE.md` notes the TLS-replication and cluster-TLS
  tests elsewhere likely want the same, so it should have one owner.
- F12 additionally needs a **new fault primitive** (an `RLIMIT_NOFILE` / fd-exhaustion fixture) that
  is not in the 01–18 infrastructure set. Either that fixture is scoped as new work, or the cheap
  substitute in the finding — extracting `classify_accept_error` + a backoff helper and unit-testing
  it — is taken instead.
