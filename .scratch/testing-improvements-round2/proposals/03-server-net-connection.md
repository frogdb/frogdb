# frogdb-server networking, connection lifecycle, dispatch & TLS — testing gap audit (round 2)

## Scope

Audited (`frogdb-server/crates/server/src/`): `acceptor.rs`, `net.rs`, `connection.rs`,
`connection/**`, `server/**`, `commands/**`, `scatter/**`, `monitor.rs`, `cursor_store.rs`,
`tls.rs`, `tls_runtime.rs`, `tls_watch.rs`.

Out of scope (other agents): `server/src/{admin,config,info,operations,recovery,replication,slot_migration}`,
`cluster_*.rs`, `migrate.rs`, `failure_detector.rs`, `role_manager.rs`, `replication_quorum.rs`,
`vll_adapter.rs`.

- **89 files, 19108/22664 lines = 84.3%** line coverage (roughly the workspace average, so
  percentage carries no signal here).
- Depth classes over **2473 deduplicated functions**: `well-covered` 928, `single-test` 734,
  `untested` 331, `monoculture` 313, `covered` 165, `hot-but-shallow` 2.
  (`depth.json` carries duplicate per-binary function records — one live, one zeroed. All counts
  and every class claim below are deduped on `(name, file, line_start)` keeping
  `max(test_count, exec_total)`. The raw `untested` list is wrong and lists obviously-tested
  functions such as `client_list` and `cluster_info`.)
- Worst-covered in-scope files: `connection/builder.rs` 0/175 (0.0%, and **dead code** — see
  Deprioritised), `commands/info.rs` 3/398 (0.8%), `connection/persistence_handler.rs` 40.9%,
  `commands/server.rs` 42.9%, `server/listeners.rs` 49.2%, `connection/routing.rs` 52.1%,
  `connection/monitor_conn_command.rs` 55.1%, `connection/deps.rs` 57.4%, `scatter/executor.rs` 67.7%.
- Both workspace-wide `hot-but-shallow` functions in this audit's scope are here:
  `connection/routing.rs:164 dispatch_scatter` (exec 27688, 3 tests) and
  `scatter/strategies.rs:122 MSetStrategy` (exec 9232, 3 tests) — the multi-shard write path is
  the hottest, thinnest-tested code in the server.

## Summary

The happy paths in this area are heavily exercised (the accept loop and the pre-dispatch gauntlet
run in 3700+ tests), but **every partial-failure and every abnormal-teardown path is either
untested or tested only against internal state**. Three shapes of escaping bug dominate. First,
scatter/gather *merges* discard per-shard errors, so a multi-key command that half-failed replies
`OK`/an undercounted integer/a `nil` that is indistinguishable from "absent" — silent wrong answers
on the DEL/MSET/MGET path, and the merge tests only ever feed successful shard results. Second,
resource accounting is not RAII and not global: the maxclients counter is minted per listener,
incremented after the TLS handshake in the spawned task and decremented by a plain statement after
`handler.run()`, and a client that disconnects while blocked in `BLPOP key 0` is never noticed at
all — the connection task, the shard waiter and the maxclients slot leak permanently, and
`CLIENT KILL` cannot reclaim any of it because the blocked path never polls `killed()`. Third, the
recently added TLS reload/multi-cert machinery is unit-tested against `TlsRuntimeHandle` only —
`rg watch_certs` over `server/tests/` and `test-harness/src/` returns nothing, and
`TestServerConfig` has no `watch_certs`/`additional_certs` fields, so no test has ever proved a
rotated certificate reaches a real ClientHello.

The single sharpest artefact found: `connection/routing.rs:197 execute_cross_shard_copy` (102
regions) is **`untested`** despite `tests/integration_copy.rs` containing 15 COPY tests — because
those tests use the key names `src` and `dst`, and `crc16_xmodem("src") % 16384 % 4 ==
crc16_xmodem("dst") % 16384 % 4 == 2`. Every COPY in the suite is accidentally same-shard.

## Existing test inventory

| surface | what it covers | notable strengths | notable blind spots |
|---|---|---|---|
| `acceptor.rs` inline tests (3) | `bind_threads_is_admin_per_port`, `bind_shares_deps_across_ports`, `bind_threads_tls_manager_per_port` | proves per-port dep threading | all three assert `Arc::ptr_eq` on internal struct fields (`monoculture`/`single-test`); nothing accepts a connection, nothing exercises the maxclients gate or the accept-error arm |
| `tests/integration_maxclients.rs` (6 tests) | sequential connect-then-PING up to the limit; rejection string; limit raise/lower | asserts the observable `-ERR max number of clients reached` | strictly sequential — no concurrent burst, no TLS port, no assertion that the counter is released on abrupt close (uses `drop()` + `sleep(100ms)`), no panic/leak case |
| `tests/integration_tls.rs` + `integration_tls_extended.rs` | ping/set-get over TLS, dual-port, mTLS required/optional/absent, invalid cert, config validation, HTTPS metrics/health, TLS replication & cluster | good breadth on *static* TLS config | zero coverage of reload: no `watch_certs`, no rotation-while-serving, no `additional_certs`, no ECDSA/RSA selection over a real handshake |
| `tls_watch.rs` inline tests (6) | fingerprint change detection, debounce, retry-after-failure, broken-rotation-keeps-old-cert, watcher-not-spawned-when-disabled | genuinely good logic tests; covers the same-length/mtime-preserving rotation case | all `single-test`, all against `TlsRuntimeHandle::handshake_leaf` in-process; the watcher task is never spawned by a real server, so the `subsystems.rs:621` wiring is unproven |
| `tls.rs` / `tls_runtime.rs` inline tests | build-then-swap `apply`, half-set client-identity contract, resolver picks ECDSA/falls back to primary, `set_additional_certs` | contract-level, cheap, correct level for the resolver logic | `monoculture`; resolver is driven by a synthetic `ClientHello`, never by rustls on a socket |
| `tests/integration_copy.rs` (15 tests) | COPY of every type, TTL, REPLACE, DB rejection | thorough on semantics | **entirely same-shard by accident** (`src`/`dst` both hash to shard 2 of 4) |
| `scatter/strategies.rs` inline tests | `test_mget_partition`, `test_mget_merge_preserves_order`, `test_del_merge_sums` | pins MGET ordering | happy-path only — no shard result is ever an `Error`; `UnlinkStrategy::merge` is `untested` |
| `connection/blocking/coordinator.rs` inline tests | three-way race (response / CLIENT UNBLOCK / deadline) via a mock `UnblockSignal` | correct boundary — the seam exists precisely so the race is unit-testable | the race has only three inputs; there is no socket-EOF input and no `killed()` input to test |
| `tests/integration_ratelimit.rs` (9 tests) | per-user command & byte limits, exempt commands, shared limit across connections, ACL GETUSER/LIST rendering, RESETRATELIMIT | good ACL-surface coverage | no boundary test (exactly-at-limit), no refill/window-rollover test, no MULTI/EXEC interaction |
| `tests/integration_pubsub.rs` (round 1 issues 29/30) | slow-subscriber output-buffer teardown incl. the `frogdb_pubsub_output_buffer_disconnects_total` metric; graceful `CLIENT KILL` and ungraceful raw-socket close deregistration | the *pubsub* slow-consumer path is genuinely closed | the sibling channels got no equivalent treatment: CLIENT TRACKING invalidations are `mpsc::unbounded` |
| `connection/state.rs` inline tests (~10) | transaction lifecycle, pubsub mode entry/exit, tracking enable/disable/mode-switch/BCAST overlap | dense, cheap | all `single-test`; several assert private fields directly |

## Findings

### F1: Scatter/gather merges discard per-shard errors — partial failure replies as success
- **Severity** 5 — `MSET` returns `+OK` when a shard's write errored; `DEL`/`EXISTS`/`TOUCH`/`UNLINK`
  return an undercount that the client reads as "those keys did not exist"; `MGET` returns `nil` for
  a shard that failed, indistinguishable from an absent key. A client cannot distinguish partial
  application from success — silent data loss from the caller's point of view.
- **Likelihood** 3 — needs one shard to error or drop while others succeed: OOM on one shard, a
  WRONGTYPE on one key, shard shutdown during drain, a `PartialResult` that carries an `Error`.
- **Effort** 2 — the strategies are pure functions over `HashMap<usize, HashMap<Bytes, Response>>`;
  the test is constructing a map with one `Response::Error` in it.
- **Priority** 19
- **Evidence**: `scatter/strategies.rs:153` `merge_sum_integers` —
  `.filter_map(|r| if let Response::Integer(n) = r { Some(*n) } else { None }).sum()` silently drops
  every non-integer (i.e. every error) reply. `scatter/strategies.rs:68` MGET —
  `.and_then(...).cloned().unwrap_or(Response::null())`. `scatter/strategies.rs:132-138`
  `MSetStrategy::merge` ignores `shard_results` entirely and returns `Response::ok()`.
  `scatter/executor.rs:139` only maps a *whole-scatter* `ScatterError`; a per-key error inside a
  successful shard's `PartialResult` flows straight into `merge`. `UnlinkStrategy::merge`
  (`strategies.rs:291`) is `untested`; `MSetStrategy` is one of the two `hot-but-shallow` functions
  in the whole workspace (exec 9232, 3 tests).
- **Proposed test**: table test per strategy. Feed shard 0 = `{k1: Integer(1)}`, shard 1 =
  `{k2: Error("OOM ...")}`; assert `DEL` does **not** return `Integer(1)` but surfaces the error (or,
  if the pinned design is "best effort", assert the chosen contract explicitly and document it).
  Same for `MSET` (must not be `+OK`), `MGET` (a failed shard must not be encoded as `nil`),
  `EXISTS`/`TOUCH`/`UNLINK`.
- **Boundary**: 1 — the merges are pure functions; a socket adds nothing. This is the anti-pattern
  in reverse: today the only coverage comes from full server integration runs that never inject a
  failing shard.
- **OPTIONS**: the *contract* is the real decision, and the test must pin whichever is chosen.
  (a) **Fail loudly** — any shard error propagates as the reply (Redis's multi-key commands are
  single-node and therefore atomic, so "error means nothing happened" is the closest analogue);
  (b) **Partial with a distinguishable reply** — keep the sum but return `-ERR partial` when any
  shard errored; (c) **Status quo, documented** — best-effort merge, documented as a divergence.
  Recommendation: (a) for `MSET`, (b) for the counting commands, and the unit tests above pin it.

### F2: Cross-shard COPY is 102 regions of never-executed code; the COPY suite is accidentally same-shard
- **Severity** 4 — the cross-shard path is a hand-rolled two-phase read/write that round-trips the
  value through a persistence frame and carries `expiry_ms` out of band. A bug here means COPY
  silently loses the TTL, the type, or the value, or reports success for a write that never landed.
- **Likelihood** 4 — default `num_shards = 4`; two unrelated key names land on different shards
  ~75% of the time, so most real COPYs take this path.
- **Effort** 3 — a server integration test with deliberately chosen key names (or a shard-count
  assertion helper).
- **Priority** 17
- **Evidence**: `connection/routing.rs:129-130` routes COPY to `execute_cross_shard_copy` only after
  the same-shard fast paths return (`routing.rs:81` single-key, `routing.rs:104` `SlotValidator::same_shard`)
  and only when `allow_cross_slot` is set. `connection/routing.rs:197 execute_cross_shard_copy` is
  `untested` (0 executions, 102 regions) in all four of its coverage records. `tests/integration_copy.rs`
  uses `"src"`/`"dst"` throughout; `crc16_xmodem("src") % 16384 = 13222`, `%4 = 2` and
  `crc16_xmodem("dst") % 16384 = 9394`, `%4 = 2` — same shard. Untested branches include TTL
  propagation, `REPLACE`, `PartialResult::Copy(None)` → `Integer(0)`, and both
  `"ERR source shard unavailable"` / `"ERR destination shard unavailable"` arms.
- **Proposed test**: in `integration_copy.rs`, add a cross-shard mirror of each existing case using
  key names asserted to differ in shard (e.g. assert `shard_for_key` differs, or pick `{a}k`/`{b}k`
  tags): COPY of each type across shards preserves value bytes exactly; COPY with a TTL preserves
  the TTL within ±50 ms; `REPLACE` overwrites; without `REPLACE` returns `Integer(0)` and leaves the
  destination byte-identical; missing source returns `Integer(0)` and creates nothing.
- **Boundary**: 4 — the two-phase flow lives in the connection's routing layer above the shard
  worker; `shard_driver` cannot reach it.

### F3: A client that disconnects while blocked leaks the connection, the shard waiter and a maxclients slot forever
- **Severity** 4 — permanent resource leak; repeat it and the server hits maxclients and refuses all
  new clients until restart. `blocked_clients` and `connected_clients` both drift upward with no
  operator remedy.
- **Likelihood** 4 — `BLPOP key 0` is the standard queue-consumer idiom; the consumer process being
  killed or its network dropping is an everyday event.
- **Effort** 3 — server integration: connect, `BLPOP k 0`, `wait_for_blocked_clients(1)`, drop the
  socket, assert `blocked_clients`/`connected_clients` return to 0 within a bound.
- **Priority** 17
- **Evidence**: `connection/blocking/coordinator.rs` races exactly three futures (`response_rx`,
  `unblock.unblocked()`, deadline) with `biased` — there is no socket/EOF input. The connection's
  main `tokio::select!` in `connection.rs:556` (which *would* see the read half close) is not running
  while `process_one_command` awaits the blocking handler. Shard side:
  `connection/blocking.rs:44` sets `deadline = (timeout > 0.0).then(...)`, so `BLPOP k 0` registers
  `deadline: None`, and `core/src/shard/blocking.rs` only expires waiters via
  `entry.deadline.is_some_and(|d| d <= Instant::now())` — a `None` deadline is never GC'd. The
  acceptor's `current_connections.fetch_sub` (`acceptor.rs:353`) is a plain statement after
  `handler.run().await`, so it never runs either.
- **Proposed test**: as above, plus the RESP-observable assertion that a subsequent client can still
  `LPUSH k v` and that the pushed element is **still there** (not popped by the ghost waiter) —
  which is the silent-data-loss corollary.
- **Boundary**: 4 — requires a real socket to close; the harness already has
  `wait_for_blocked_clients` (`test-harness/src/server.rs:819`).

### F4: maxclients accounting is per-listener, check-then-act, and not RAII
- **Severity** 4 — the limit is the only guard against connection-driven memory exhaustion. Three
  distinct ways to exceed it, none tested.
- **Likelihood** 3 — needs the server to be at or near the limit, which is exactly when the guard
  matters.
- **Effort** 3 — server integration with concurrent connects and a TLS port.
- **Priority** 15
- **Evidence**: (a) the gate loads the counter in the accept loop (`acceptor.rs:224`) but the
  increment happens in the spawned task *after* the TLS handshake (`acceptor.rs:322`) — N concurrent
  connects at the limit all pass the gate. (b) `acceptor.rs:353` decrements with a plain statement
  after `handler.run().await`; a panic inside the connection task unwinds past it and burns the slot
  permanently (contrast `ClientHandle`, which *is* RAII — `connection.rs:127` "auto-unregisters on
  drop"). (c) `current_connections` is minted per `bind` (`acceptor.rs:191`), which the existing test
  documents as intentional for the admin port (`acceptor.rs:535-540`) — but it means a TLS-enabled
  server enforces maxclients **twice**, once per port, and `ConnectionsCurrent::set` is written from
  whichever port's task ran last, so the `connections_current` gauge is wrong whenever both ports are
  live. The three `acceptor.rs` tests assert `Arc::ptr_eq` on struct fields and never accept a
  connection.
- **Proposed test**: (1) with `maxclients = N`, spawn N+8 simultaneous connects; assert exactly N
  succeed and the rest get `-ERR max number of clients reached`. (2) With TLS enabled and
  `maxclients = N`, fill the plaintext port to N, then assert the TLS port also refuses (pins the
  intended global-vs-per-port contract either way). (3) Assert `INFO clients`'
  `connected_clients` matches the true count with both ports in use.
- **Boundary**: 4 — the race is between the accept loop and the spawned task; only a real listener
  reproduces it. The three existing internal-state tests should be **moved up** to this level.

### F5: CLIENT KILL is a silent no-op against a blocked or write-stalled connection
- **Severity** 4 — `CLIENT KILL` is the operator's only escape hatch for a stuck client; it returns
  `1` (killed) while the connection lives on. Combined with F3, there is no way to reclaim a leaked
  connection short of restarting the server.
- **Likelihood** 3 — killing a client stuck in `BLPOP`/`XREAD BLOCK 0`, or a subscriber whose TCP
  window is full, is a routine ops action.
- **Effort** 3 — server integration; needs a second (admin) connection to issue the kill.
- **Priority** 15
- **Evidence**: `core/src/client_registry/mod.rs:575 kill_by_id` (and `:616 kill_by_filter`) send
  only `kill_tx`; they do not touch the unblock signal. `killed()` is polled only in the top-level
  `tokio::select!` (`connection.rs:559`), which is not running while the connection is inside
  `handle_blocking_wait` (`connection/blocking.rs:59`) or inside the pubsub/invalidation/MONITOR arms
  awaiting `flush_responses()` on a stalled socket (`connection.rs:600`, `:681`). `CLIENT UNBLOCK`
  works; `CLIENT KILL` does not.
- **Proposed test**: client A issues `BLPOP k 0`; client B issues `CLIENT KILL ID <A>`; assert A's
  socket reaches EOF within a bound **and** `blocked_clients` returns to 0. Second case: kill a
  subscriber whose socket is not being read.
- **Boundary**: 4 — two live connections and a real socket close are the behaviour under test.

### F6: The connection-level MIGRATE handler is 174 untested regions on a delete-after-transfer path
- **Severity** 4 — MIGRATE deletes the local key after a claimed-successful `RESTORE` on the target.
  A misread of the target's reply is unrecoverable data loss.
- **Likelihood** 3 — MIGRATE is the manual resharding / key-relocation tool; used deliberately, but
  used.
- **Effort** 3 — two `TestServer`s and a MIGRATE between them; the harness already starts pairs.
- **Priority** 15
- **Evidence**: `connection/persistence_handler.rs:30 handle_migrate_command` is `untested` (174
  regions, the largest untested function in this scope); it is live code, dispatched from
  `connection/dispatch.rs:288` (`Response::MigrateNeeded { args }`). Its doc comment lists exactly
  the risky steps: "Connect to the target server / Authenticate if needed / Send RESTORE command(s) /
  Delete local key(s) if not COPY". The whole file sits at 40.9% lines.
- **Proposed test**: MIGRATE a key (and a `KEYS`-form multi-key MIGRATE) between two `TestServer`s;
  assert the value and TTL land on the target and the source key is gone; with `COPY`, assert the
  source key survives; with `REPLACE` absent and the destination occupied, assert `BUSYKEY` and that
  **the source key is still present** (the data-loss case); with the target unreachable, assert an
  error **and** that the source key is untouched.
- **Boundary**: 4 — the handler dials a real socket to a second server.
- **Cross-area**: `migrate.rs` proper is another agent's; this is the connection-level handler only.

### F7: The CLIENT TRACKING invalidation channel is unbounded — no slow-consumer bound, unlike pubsub
- **Severity** 4 — a tracking client that stops reading grows a per-connection `mpsc::unbounded`
  queue until the server OOMs. Round 1 (issue 29) closed exactly this hazard for pubsub; the sibling
  channel was left unbounded.
- **Likelihood** 3 — client-side caching is opt-in, but `CLIENT TRACKING ON BCAST` with a stalled or
  paused client (GC pause, debugger, saturated link) is an ordinary failure.
- **Effort** 3 — server integration: enable tracking, stop reading, drive writes, assert the
  connection is dropped (or memory stays bounded).
- **Priority** 15
- **Evidence**: `core/src/tracking.rs` — `pub type InvalidationSender = mpsc::UnboundedSender<InvalidationMessage>`.
  The connection's drain arm (`connection.rs:617-640`) has no overflow branch, in explicit contrast
  to the pubsub arm two branches above it which handles `Drained::Overflowed` and
  `rx.has_overflowed()` (`connection.rs:573`, `:610`) and increments
  `PubsubOutputBufferDisconnects`. `rg` over `server/tests/` finds no invalidation-backlog test.
- **Proposed test**: enable `CLIENT TRACKING ON BCAST` on a connection that never reads; from a
  second connection write M keys; assert either the tracking client is disconnected with a metric
  (mirroring the pubsub contract) or that server RSS/`CLIENT INFO tot-mem` stays bounded.
- **Boundary**: 4 — the bound is a property of the connection's channel wiring, and the test needs a
  reader that refuses to read.
- **Cross-area**: the fix (a bounded channel) is in `frogdb-core`'s tracking module.

### F8: `scatter_error_to_response` is untested — every shard-failure reply shape is unverified
- **Severity** 3 — the error *code* drives client retry behaviour: `-BUSY` is retryable, `-ERR` is
  not. Getting these wrong turns a transient shard-busy into a hard client failure (or vice versa,
  an infinite retry loop).
- **Likelihood** 3 — every VLL lock timeout, shard-busy continuation, or shard drop during shutdown
  goes through it.
- **Effort** 1 — a pure function from `ScatterError` to `Response`.
- **Priority** 14
- **Evidence**: `scatter/executor.rs:140 scatter_error_to_response` is `untested` (32 regions). The
  six arms — `ShardUnavailable` → `-ERR shard unavailable`, `LockFailed(VllError::ShardBusy)` →
  `-BUSY shard busy with continuation lock; retry`, `LockChannelClosed`/`LockTimeout` →
  `-ERR VLL lock acquisition failed`, `ResultChannelClosed` → `-ERR shard dropped VLL result`,
  `ResultTimeout` → `-ERR VLL execution timeout` — have never executed. `scatter/executor.rs` overall
  is 67.7%.
- **Proposed test**: a table test mapping each `ScatterError` variant to its exact reply string,
  asserting the RESP error *prefix* (`BUSY` vs `ERR`) separately from the message so a message
  reword does not silently flip retryability.
- **Boundary**: 1 — pure mapping; needs neither a shard nor a socket.

### F9: TLS certificate reload has never been exercised through a live listener
- **Severity** 4 — a broken reload either serves an expired/revoked certificate indefinitely or
  fails every new handshake — a full outage that only manifests at rotation time, i.e. when nobody
  is watching.
- **Likelihood** 3 — cert rotation is a routine ops event (cert-manager, Let's Encrypt); `watch_certs`
  is opt-in but is the feature's whole point.
- **Effort** 4 — needs harness work: `TestServerConfig` has no `watch_certs`, `TlsFixture` can only
  `generate()` one cert set.
- **Priority** 14
- **Evidence**: `rg watch_certs` over `server/tests/` and `test-harness/src/` returns **nothing**.
  `test-harness/src/server.rs:147-167` lists eleven `tls_*` config fields, none of them `watch_certs`
  or `additional_certs`; `test-harness/src/tls.rs` exposes exactly one function, `generate()`. All
  six `tls_watch.rs` tests are `single-test` and drive `TlsRuntimeHandle` in-process
  (`tls_watch.rs:239`, `:274`), so the watcher-spawn wiring at `server/subsystems.rs:621-623` and the
  `manager()`→`acceptor()`-per-connection contract at `subsystems.rs:357,572,597` are unproven
  end-to-end. `tls.rs:111 MaybeTlsStream::poll_shutdown` is `untested`.
- **Proposed test**: start a server with `watch_certs = true`; open a TLS connection and record the
  peer certificate serial; rewrite cert+key on disk with a freshly generated pair; poll until a
  **new** connection presents the new serial while the **pre-existing** connection keeps working;
  then write a corrupt key and assert new connections still get the last-good certificate (the
  `broken_rotation_keeps_the_old_certificate_serving` unit test, promoted to the real listener).
- **Boundary**: 4 — the unit tests already cover the decision logic correctly; what is missing is
  precisely the wiring, which only a real listener can show.
- **Cross-area / shared infra**: needs `TestServerConfig.tls_watch_certs` +
  `TlsFixture::regenerate_in_place()`.

### F10: `HELLO … AUTH user pass` leaks the password into the MONITOR feed
- **Severity** 3 — credentials in plaintext in a stream any MONITOR-privileged client can read (and
  in any log that captures it). MONITOR is already privileged, which caps this below a true auth
  bypass.
- **Likelihood** 3 — modern clients (redis-py, Lettuce) authenticate via `HELLO … AUTH` by default
  when a username is configured, and MONITOR is a standard debugging step.
- **Effort** 1 — the redaction is a pure function of `(cmd_name, args)`.
- **Priority** 14
- **Evidence**: `monitor.rs:26 MonitorEvent::new` redacts only `if cmd_name == "AUTH"`. The
  call site (`connection.rs:396`) passes the uppercased name, so case is handled, but `HELLO`,
  `CONFIG SET requirepass`, `CONFIG SET masterauth`, `ACL SETUSER u >pass` and `MIGRATE … AUTH` all
  pass through verbatim. The only redaction test is `monitor.rs:114 test_auth_args_redacted`, which
  feeds `"AUTH"` (`single-test`).
- **Proposed test**: unit table over `MonitorEvent::new` asserting that no sensitive argument
  survives for `AUTH`, `HELLO 3 AUTH u p`, `CONFIG SET requirepass x`, `CONFIG SET masterauth x`,
  `ACL SETUSER u >p`, `MIGRATE … AUTH p` / `AUTH2 u p`; plus one integration test that runs MONITOR
  on one connection and `HELLO 3 AUTH` on another and greps the feed for the password.
- **Boundary**: 1 for the table (pure function), 4 for a single end-to-end confirmation that the
  redaction sits on the path MONITOR actually uses.

### F11: FT.AGGREGATE cursors have no owner and no disconnect cleanup; the only reclaim path never runs in tests
- **Severity** 3 — an abandoned `FT.AGGREGATE … WITHCURSOR` pins its entire materialised row set
  until the timeout; a client that disconnects mid-paging leaks it. The retain predicate that is
  supposed to free them has never executed under test.
- **Likelihood** 3 — cursor paging exists to stream large result sets, so abandoning one mid-page is
  the normal failure of the normal use.
- **Effort** 2 — `AggregateCursorStore` is a standalone type; a crate-level test can insert, advance
  the clock (or use a tiny timeout), and call `evict_expired`.
- **Priority** 13
- **Evidence**: `cursor_store.rs:106 evict_expired` is `well-covered` only because the 30-second
  background task (`server/subsystems.rs:476-483`) calls it 3714 times — but its retain **closure**
  at `cursor_store.rs:109` is `untested`, i.e. no test has ever had a cursor in the map when it ran.
  `read_cursor` validates only `state.index_name != expected_index`, never the owning connection, and
  is `monoculture` (2 tests, both `connection::conn_command::tests::ft_cursor_*`). Nothing in
  `connection/lifecycle.rs`'s teardown touches the cursor store, and there is no cap on live cursors.
- **Proposed test**: insert two cursors with a 50 ms timeout, read one to keep it fresh, sleep past
  the timeout, call `evict_expired`, assert exactly the stale one is gone and the fresh one still
  pages correctly; plus an integration test that opens a `WITHCURSOR` aggregate, drops the socket,
  and asserts the cursor is reclaimed.
- **Boundary**: 2 for the eviction predicate (the store is a self-contained type); 4 for the
  disconnect-reclaim half.

### F12: The accept loop busy-spins on a persistent accept error (EMFILE/ENFILE)
- **Severity** 4 — under fd exhaustion the loop spins at 100% CPU emitting an `error!` per
  iteration, which floods logs and starves the runtime — turning a recoverable resource shortage
  into a hard outage.
- **Likelihood** 2 — needs fd exhaustion or an ENOBUFS-class error, but low `ulimit -n` in
  containers makes it reachable.
- **Effort** 3 — needs an fd-exhaustion fixture or a listener fault seam.
- **Priority** 13
- **Evidence**: `acceptor.rs:362-364` — `Err(e) => { error!(error = %e, "Failed to accept connection"); }`
  with no backoff and no classification of fatal vs transient. Redis handles `ANET_ERR` with a
  rate-limited log and continues; nginx sleeps on EMFILE.
- **Proposed test**: lower `RLIMIT_NOFILE` in a child, saturate it, assert the process makes forward
  progress (responds to a later connect once fds free up) and does not emit more than K log lines
  per second.
- **Boundary**: 4 — needs a new fault primitive (rlimit fixture). See Deprioritised if the fixture
  is judged too costly; the cheap alternative is a unit test on an extracted
  `classify_accept_error` + backoff helper, which does not exist yet.

### F13: Multi-certificate selection is never driven by a real ClientHello
- **Severity** 3 — the wrong certificate for a client's signature algorithms means handshake
  failures for a subset of clients, or serving the fallback certificate where a SAN-specific one was
  configured.
- **Likelihood** 3 — `additional_certs` is a new feature; anyone who configures it is by definition
  in the mixed-algorithm case it exists for.
- **Effort** 4 — needs a second `TlsFixture` (ECDSA) and `TestServerConfig.tls_additional_certs`.
- **Priority** 11
- **Evidence**: `tls.rs:428-440` builds the resolver over `config.additional_certs`; the resolver
  tests (`tls.rs:748`, `:847`, `:861 server_config_builds_with_additional_certs_under_mtls`) and
  `tls_runtime.rs:518 additional_certs_can_be_added_at_runtime` are `monoculture` unit tests over a
  synthetic ClientHello / the handle's config. No integration test sets `additional_certs` —
  `TestServerConfig` cannot express it.
- **Proposed test**: server with an RSA primary + an ECDSA additional cert; connect with an
  ECDSA-only client and assert the presented certificate is the ECDSA one; connect with an
  RSA-only client and assert the RSA one; connect with a client matching neither and assert the
  documented fallback.
- **Boundary**: 4 — algorithm negotiation is rustls's job; only a real handshake proves the wiring.
- **Cross-area / shared infra**: shares the harness work with F9 (`TlsFixture` variants).

### F14: The RESP3 write path bypasses the `Framed` buffer, so a mid-pipeline protocol switch can reorder bytes
- **Severity** 3 — bytes delivered out of order on the wire desynchronise the client's parser; the
  client sees replies attributed to the wrong commands.
- **Likelihood** 2 — requires `HELLO 3` (or a `RESET`) pipelined behind RESP2 commands in the same
  read, which real clients rarely do but connection-pool warmup code sometimes does.
- **Effort** 2 — the existing `frame_io.rs` tests already drive the feed path directly.
- **Priority** 11
- **Evidence**: `connection/frame_io.rs:139-149` — the RESP3 arm encodes into `self.resp3_buf` and
  calls `self.framed.get_mut().write_all(...)` directly, jumping ahead of anything already buffered
  in the RESP2 `Framed` write buffer. `protocol_version` is flipped mid-pipeline with no intervening
  flush by `ConnStateMut::set_protocol_version` (`connection/auth_conn_command.rs:54`, HELLO) and by
  `ConnectionState::reset` (`connection/state.rs:1278`, RESET). The two existing ordering tests
  (`frame_io.rs:220 resp2_null_array_feed_order_is_preserved`, `:257 resp3_null_array_feed_order_is_preserved`,
  both `single-test`) pin round 1's `NullArray` fix only — each runs in a single protocol.
- **Proposed test**: feed `PING`, then `HELLO 3`, then `PING` in one pipeline without an intervening
  flush; assert the three replies arrive in issue order (the RESP2 `+PONG` must not be overtaken by
  the RESP3-encoded reply). Assert on the raw byte stream, not on a parsed client.
- **Boundary**: 1 — `frame_io.rs`'s existing tests already exercise this at unit level over an
  in-memory duplex; keep it there.

### F15: Shard-local command implementations reachable only from Lua are effectively 0% covered
- **Severity** 3 — `redis.call('INFO')` inside a script executes on the owning shard through a
  separate implementation from the connection-level INFO. Wrong or panicking output there breaks the
  script (and a panic on the shard worker is a much bigger event than a bad reply).
- **Likelihood** 2 — scripts that call INFO/FLUSHDB exist but are not the common case.
- **Effort** 2 — `shard_driver` can dispatch these commands directly.
- **Priority** 11
- **Evidence**: `commands/info.rs` is **3/398 lines (0.8%)** — its module doc states it "exists only
  for scripts (`redis.call('INFO')` executes on the owning shard)". Its section builders are among
  the largest untested functions in scope (74/44/37/36 regions). `commands/server.rs:76` and `:124`
  (`Flushdb`/`Flushall` `execute`, 14 regions each) are `untested` for the same reason — the
  connection-level FLUSHDB goes through `ScatterOp`, never through these.
- **Proposed test**: dispatch `INFO`, `INFO memory`, `INFO keyspace`, `INFO everything` and
  `FLUSHDB`/`FLUSHALL` through the `shard_driver` harness; assert each requested section header is
  present, that no server-level-only field is emitted as a placeholder zero (the module doc's stated
  invariant), and that FLUSHDB empties the shard.
- **Boundary**: 3 — real command dispatch on a real shard worker, no socket. Testing this through a
  full EVAL over RESP would be the anti-pattern the brief calls out.
- **Cross-area**: the scripting agent owns `EVAL`; this is the shard-local command impl only.

### F16: `CLIENT INFO`/`CLIENT LIST` always report zero pending output, hiding exactly the slow consumers this area leaks
- **Severity** 2 — wrong observability field, not wrong data. But it is the field an operator would
  use to diagnose F3/F5/F7, so it converts a diagnosable incident into an undiagnosable one.
- **Likelihood** 4 — every `CLIENT LIST` on every server reports it.
- **Effort** 2 — assert over the RESP-visible `CLIENT INFO` fields.
- **Priority** 12
- **Evidence**: `connection/lifecycle.rs:234 compute_client_memory` (44 regions) reads
  `self.framed.read_buffer().len()` for `qbuf` but hardcodes `output_list_len = 0` and
  `output_list_mem = 0`; the pubsub backlog, the invalidation backlog and the `Framed` write buffer
  are all excluded. `server/src/info/sections.rs:126` likewise hardcodes
  `client_recent_max_output_buffer` to `0`.
- **Proposed test**: with a subscriber that is not reading and a publisher flooding it, assert
  `CLIENT LIST`'s `omem`/`tot-mem` for that client grows above zero before the disconnect fires
  (this pairs with the round-1 pubsub bound test, which already builds the flood).
- **Boundary**: 4 — the value only becomes non-zero with a real stalled socket.
- **Note**: per the user's standing preference (observability accuracy over Redis parity),
  hardcoded-zero fields are worse than absent ones; the finding is as much "delete or fill" as
  "test".

### F17: Rate-limit boundary, refill and MULTI interaction are untested
- **Severity** 2 — an off-by-one at the limit or a refill that never fires means a tenant is
  throttled early or not throttled at all; noisy-neighbour, not corruption.
- **Likelihood** 3 — any deployment that configures per-user rate limits sits on the boundary
  continuously.
- **Effort** 2 — the limiter is reachable from a crate-level test; the ACL surface already has one.
- **Priority** 10
- **Evidence**: `tests/integration_ratelimit.rs`'s 9 tests all drive well past the limit and assert
  "eventually errors"; none asserts the Nth command succeeds and the (N+1)th fails, none crosses a
  refill window, none combines the command and byte limits, and none checks whether a command queued
  in MULTI is charged at queue time, at EXEC time, or both.
  `connection/guards.rs:116 is_rate_limit_exempt` and `:123 check_rate_limit` are called from
  `connection.rs:374`, before dispatch and after the QUIT shortcut — so the exempt list's interaction
  with MULTI/EXEC is entirely unpinned.
- **Proposed test**: exactly-N-then-fail at the boundary; sleep one window and assert the budget
  refilled to exactly N again; a `MULTI` of K commands under a limit of K asserting the documented
  charging point; bytes and commands limits both configured, asserting whichever trips first.
- **Boundary**: 4 for the boundary/refill assertions (the limiter state is per-ACL-user and lives in
  the running server); 1 for the exempt-list membership.

### F18: PSYNC handoff returns early and skips `notify_connection_closed`
- **Severity** 3 — per-connection state registered on the shards (pubsub subscriptions, tracking
  registrations, blocking waiters) is never torn down for a connection that ran ordinary commands
  before issuing PSYNC. The shards keep sending to a channel nobody drains.
- **Likelihood** 2 — requires a connection that subscribes/tracks/blocks and *then* becomes a
  replica link, which is unusual but is exactly what a misbehaving or probing client does.
- **Effort** 3 — server integration with a raw PSYNC.
- **Priority** 10
- **Evidence**: `connection.rs:826-832` — the PSYNC arm `return Ok(())`s with the comment "Don't run
  normal cleanup - replication handler has the connection", skipping the
  `self.notify_connection_closed().await` at `connection.rs:835`. That call is itself conditional
  (`connection/lifecycle.rs`: the shard broadcast only fires `if self.state.in_pubsub_mode() ||
  self.state.tracking().enabled`), so the intended cleanup exists and is simply bypassed here. Note
  also that `current_connections` **is** decremented for a handed-off connection while its socket
  lives on inside the replication handler, so a replica link is uncounted against maxclients.
- **Proposed test**: on one connection, `SUBSCRIBE ch`, then send `PSYNC ? -1`; assert the shards'
  subscriber count for `ch` drops to zero (via `PUBSUB NUMSUB` from a second connection) rather than
  retaining the handed-off connection.
- **Boundary**: 4 — the handoff only exists at the connection layer.

## Deprioritised

- **`connection/builder.rs` (0/175 lines, `ConnectionHandlerBuilder::build` `untested`, 21 regions)**
  — `rg ConnectionHandlerBuilder` finds only the definition and the re-export in `connection.rs`.
  It is **dead code** that duplicates `ConnectionHandler::from_deps`'s wiring and will silently
  drift. Recommendation: delete it, do not test it.
- **`tls.rs:42/50 peer_addr`/`local_addr` and `tls.rs:111 poll_shutdown` (`untested`, 9-11 regions)**
  — thin delegations; `poll_shutdown` is worth one line inside the F9 rotation test rather than its
  own case.
- **`connection/scripting/eval.rs:126 execute_cross_shard_script` (`untested`, 27 regions)** — real
  gap, but cross-shard scripting is the scripting agent's area and the effort belongs with their
  harness work. Flagged in Cross-area notes.
- **`scatter/broadcast.rs:332 find_first` (`untested`, 43 regions)** — RANDOMKEY's first-hit
  broadcast. `handle_randomkey` is `well-covered` (8 tests), so the residue is the empty-everywhere
  and single-shard-error arms only; low severity (a `nil` RANDOMKEY), fold into F1's table test if
  cheap.
- **MONITOR `Lagged` handling (`connection.rs:685`)** — correct by construction (bounded broadcast,
  the subscriber skips ahead) and matches the documented design. Worth one metric, not a test.
- **`connection/hotkeys.rs` 68.3%, `server/util.rs` 59.7%, `connection/deps.rs` 57.4%** — mostly
  construction/plumbing whose failure is a compile error or an immediate startup failure; percentage
  here is not risk.
- **A shuttle/loom model of the maxclients counter** — the race in F4 is real but the interleaving is
  trivial (load-then-add); a concurrent-burst integration test finds it for a fraction of the cost of
  a model.
- **Fuzzing the RESP2 codec** — `connection/codec.rs` is already 93.8% with 304 cold lines and dense
  limit tests (`PROTO_MAX_MULTIBULK_LEN`, `PROTO_MAX_BULK_LEN`, `PROTO_INLINE_MAX_SIZE`,
  `sdssplitargs`, the bounded `scan_for_oversized_bulk`); round 1 issue 40 already stood up a
  continuous fuzz corpus. The residue worth noting is that those three limits are **compile-time
  constants** where Redis makes `proto-max-bulk-len` configurable — a config gap, not a test gap.

## Cross-area notes

- **Shared harness work (blocks F9 and F13)**: `TestServerConfig` needs `tls_watch_certs` and
  `tls_additional_certs`; `TlsFixture` (`test-harness/src/tls.rs`, currently a single `generate()`)
  needs an ECDSA variant and an in-place regeneration helper so a rotation can happen while the
  server runs. One agent should own this; two findings here and probably the TLS-replication and
  cluster-TLS tests elsewhere all want it.
- **`frogdb-core` owns two of the fixes**: `InvalidationSender` is
  `mpsc::UnboundedSender` in `core/src/tracking.rs` (F7), and `ClientRegistry::kill_by_id` /
  `kill_by_filter` in `core/src/client_registry/mod.rs` do not fire the unblock signal (F5). The
  waiter-GC side of F3 is also core's: `core/src/shard/blocking.rs` never expires a `deadline: None`
  waiter.
- **Scripting agent**: `connection/scripting/eval.rs:126 execute_cross_shard_script` is `untested`
  (27 regions); and F15's shard-local `INFO`/`FLUSHDB` implementations are reachable *only* through
  their EVAL path, so the two audits should agree on who writes the `shard_driver` cases.
- **Coverage-data caveat for every agent**: `depth.json` contains duplicate function records, one of
  which is zeroed. Any agent reading the `untested` list without deduplicating on
  `(name, file, line_start)` will report false positives — the raw list flags `client_list`,
  `client_kill`, `cluster_info` and `PreDispatchView::queue_command` as untested. Deduplicating drops
  the workspace `untested` count by roughly 4×.
- **Round 1 overlap, stated precisely**: issue 29 (pubsub slow-subscriber bound) and issue 30 (pubsub
  disconnect deregistration) are genuinely closed and are **not** re-proposed; F7 is the *sibling*
  channel (CLIENT TRACKING invalidations) that the same reasoning was never applied to. Issue 49
  (cross-slot MULTI assert tightening) and issue 19 (cross-slot standalone MULTI invariant) cover
  MULTI over cross-slot keys; F2 is cross-shard **COPY**, a different dispatch path that neither
  touches. Issue 24 (SCAN full-iteration stress) covers SCAN cursors; F11 is the FT.AGGREGATE cursor
  *store*, which round 1 did not look at.
