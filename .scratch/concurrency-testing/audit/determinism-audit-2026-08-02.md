# Determinism audit — non-injectable side-effect / entropy sources in product code

Date: 2026-08-02
Scope: product crates only (`core`, `server`, `persistence`, `replication`, `replication-runtime`,
`cluster`, `vll`, `txn`, `recovery`, `commands`, `scripting`, `acl`, `search`, `protocol`, `types`,
`net`). Excludes `tests/`, `ops/`, `frogctl`, `frogdb-operator`, and `#[cfg(test)]` modules
(test-only hits are noted, not counted).

Driver: [`../issues/open/14-workload-harness-not-reproducible.md`](../issues/open/14-workload-harness-not-reproducible.md)
— the generated-workload harness produces a different history from the same seed, in-process and
across processes. Issue 14 named three suspects; this is the exhaustive version.

---

## 0. The load-bearing fact

turmoil 0.7.1 builds **one paused tokio runtime per simulated host**:

```
turmoil-0.7.1/src/rt.rs:231-252
    let tokio = tokio_builder
        .enable_time()
        .start_paused(true)      // <- tokio::time is virtual, per host
        .build()
```

and seeds the tokio scheduler RNG (which picks `tokio::select!` branches) **only** under
`--cfg tokio_unstable`:

```
turmoil-0.7.1/src/rt.rs:38-41, 234-246
    /// Rng used to seed the tokio runtime.
    /// Only used if the `tokio_unstable` cfg flag is set.
    pub rng: Option<SmallRng>,
    ...
    #[cfg(tokio_unstable)]
    if let Some(rng) = &mut config.rng { ... tokio_builder.rng_seed(seed); }
```

Consequences that structure the whole audit:

1. **`tokio::time::Instant` is virtual. `std::time::Instant` and `SystemTime` are not.** The two
   clocks have an arbitrary, per-host, per-run offset. FrogDB mixes them: **57 product files import
   `std::time::Instant`; exactly 2 import `tokio::time::Instant`** (`vll/src/shard.rs`,
   `replication/src/wait_coordinator.rs`). Every timer *tick* in the shard event loop is
   `tokio::time::interval` (virtual) while every *deadline and budget it compares against* is
   `std::time::Instant` (real). That is not "clock skew" — it is two unrelated timelines.
2. `--cfg tokio_unstable` is **not** set anywhere for turmoil builds (`.cargo/config.toml` has no
   `[build] rustflags`; the only occurrence in the `Justfile` is line 148, for `tokio-coz`). So
   `select!` branch order is entropy-seeded today, exactly as issue 14 reported.
3. `std::collections::HashMap`/`HashSet` (and `griddle::HashMap`, which also defaults to
   `RandomState`) are randomly keyed **per instance**, so iteration order differs between two runs
   inside one process — matching issue 14's in-process evidence.

---

## 1. Executive summary

| Class | Count | Meaning |
|---|---:|---|
| **A — decision-feeding, not injectable** | **59** | Alters control flow or client-visible data under sim. These are the bugs. |
| **B — observability only** | ~34 | Metric/log/INFO timestamps and counters. Benign. |
| **C — already injectable / virtualized / structurally safe** | ~38 | Goes through an existing seam (`frogdb-net/turmoil`, `fake-wal`, `shard-driver`, `tokio::time`, injected `now` params, `BTreeMap` ordering, content-hash SCAN cursor), or is compile-time constant / order-independent. |

A-class breakdown by source kind:

| Source kind | A-count | Worst offender |
|---|---:|---|
| Real clock in a sim-driven decision (`std::time`) | 23 | `blocking/coordinator.rs:87` — real deadline handed to a virtual-clock sleep |
| Unseeded global RNG (`rand::rng()` / `rand::random()`) | 16 | `config/mod.rs:232,262` — the sim's *own* chaos injector is unseeded |
| Hash-container iteration order feeding a decision or a reply | 11 | `pubsub.rs:712/749` — PUBLISH fanout order across subscribers |
| Non-`biased` `tokio::select!` with semantically significant branches | 5 | `shard/event_loop.rs:35` — 7-branch shard dispatch loop |
| Network/OS I/O bypassing the `frogdb-net` seam | 3 | `commands/replication.rs:133` — real DNS resolution for `REPLICAOF` |
| Real threads / blocking pool under sim | 1 | `persistence/src/rocks/reclaim.rs:124` |

**Verdict on issue 14's three suspects: all three are real, and the audit found a fourth and fifth
category the issue did not name.** In priority order the likely root causes of the reported
in-process flapping are:

1. **Clock-domain mixing in the blocking-command path** (`BLPOP`/`BLMOVE`/`BZPOPMIN` — i.e. the
   `MultiWaiter` profile). `server/src/connection/blocking.rs:44` computes the deadline as
   `std::time::Instant::now() + timeout`, and `server/src/connection/blocking/coordinator.rs:87`
   feeds it to `tokio::time::sleep_until(d.into())`. Under `start_paused(true)` the virtual clock
   is frozen near host-runtime start while the std clock keeps running at wall speed, so the
   *effective* timeout is `(real time elapsed since host start) + timeout` in virtual time — a
   quantity that depends on how long the machine took to compile/schedule the run. This alone
   explains "verdict flapping on an identical input".
2. **Unseeded chaos injection inside the turmoil feature itself** (`server/src/config/mod.rs:232`,
   `:262`). `ChaosConfigExt::get_jitter` and `should_simulate_connection_reset` are compiled
   `#[cfg(feature = "turmoil")]` and call `rand::rng()`. The fault injector the sim uses is not
   seeded by the sim's seed.
3. **Hash iteration order in client-visible replies** — `SMEMBERS`/`HGETALL`/`KEYS`/`PUBSUB
   CHANNELS` return elements in `RandomState` order, so the *result* field of the history digest
   differs run to run even with an identical operation order.
4. **Unseeded `tokio::select!`** in the shard event loop and the connection loop (issue 14 already
   established this is necessary but not sufficient — consistent with it being #4, not #1).

---

## 2. Class A — the full list

Blast radius legend: `WL` = `concurrency_workload` generated-workload sweep (all profiles),
`MW` = `MultiWaiter` profile specifically, `TX` = `TxHeavy`, `SIM` = `simulation.rs`,
`PS` = `concurrency_pubsub`, `CL` = cluster/replication turmoil suites.

### A.1 Real clock in a sim-driven decision (23)

| # | file:line | Source | Decision it feeds | Blast | Suggested seam |
|---|---|---|---|---|---|
| A1 | `server/src/connection/blocking/coordinator.rs:87` | `tokio::time::sleep_until(d.into())` where `d: std::time::Instant` | **The BLPOP/BLMOVE/BRPOPLPUSH/BZPOPMIN timeout itself.** Real deadline evaluated against the virtual clock. | MW, WL, SIM | Change the parameter type to `tokio::time::Instant` (mirrors `blocking.rs:265`, which already does this for WAIT) |
| A2 | `server/src/connection/blocking.rs:44` | `Instant::now() + Duration::from_secs_f64(timeout)` (std) | Origin of A1's deadline; also the value registered with the shard | MW, WL | `tokio::time::Instant::now()` — one-line, matches line 265 |
| A3 | `core/src/shard/blocking.rs:170` | `let now = Instant::now()` (std) in `check_waiter_timeouts` | Which waiters the shard GCs on the (virtual) 100 ms tick | MW, WL | Pass `now` in from the event loop; source it from `tokio::time::Instant` |
| A4 | `core/src/shard/blocking.rs:303` | `entry.deadline.is_some_and(\|d\| d <= Instant::now())` | **Whether a waiter is skipped rather than served** in `drive_satisfaction`. This is the FIFO-wake-order / lost-element path issue 11 flagged. | MW, WL | Same clock injection as A3 |
| A5 | `core/src/shard/event_loop.rs:177` | `self.expiry.run_cycle(&mut self.store, Instant::now())` (std) | The `now` every TTL comparison in the cycle uses, on a virtual tick | WL, SIM | `run_cycle` already takes `now` as a parameter — only the caller needs the virtual clock |
| A6 | `core/src/shard/active_expiry.rs:123,127,138,168,188` | `let start = Instant::now()` / `start.elapsed() > self.budget` (25 ms real) | **How many keys/fields get deleted per cycle** — real CPU time decides visible state | WL, SIM | Inject a `Clock` into `ActiveExpiryCoordinator`, or make the budget an op-count under `cfg(sim)` |
| A7 | `commands/src/expiry.rs:20,34,48,79,255,344` | `SystemTime::now()` + `Instant::now()` bridge in `unix_secs_to_instant` / `unix_ms_to_instant` / `instant_to_unix_secs` / `instant_to_unix_ms` | **EXPIREAT/TTL/PTTL/EXPIRETIME/PEXPIRETIME reply values** | WL, SIM | Single injected wall+monotonic pair; today every helper re-samples both clocks independently |
| A8 | `core/src/store/hashmap.rs:558` | `Instant::now()` in `backdate_expiry` | The backdated deadline the sim's *deterministic expiry* helper writes — note the doc comment at `:543` claims this enables "deterministic expiry under turmoil's virtual clock", but the file imports `std::time::Instant`, so the claim is false today | WL, SIM | Same clock seam; **fix the comment either way** |
| A9 | `core/src/store/hashmap.rs:1392` | `Instant::now()` in `purge_expired_hash_fields` | Which hash fields are purged | WL | Clock seam |
| A10 | `core/src/store/hashmap.rs:1543` | `Instant::now().duration_since(last_access)` | `OBJECT IDLETIME` reply, and LRU eviction victim ranking | WL | Clock seam |
| A11 | `core/src/store/hashmap.rs:1555` | `Instant::now().duration_since(last_access) / 60` | LFU decay minutes → `OBJECT FREQ` and LFU eviction victim | WL | Clock seam |
| A12 | `core/src/shard/execution.rs:404` | `exp.duration_since(Instant::now())` | `DUMP` payload TTL field (client-visible bytes) | WL | Clock seam |
| A13 | `core/src/shard/execution.rs:1032` | `get_expired_keys(std::time::Instant::now())` | `FLUSHDB`/`FLUSHALL` live-key count reported | WL | Clock seam |
| A14 | `core/src/shard/execution.rs:1088` | `Instant::now() + Duration::from_millis(*ms)` | `RESTORE`/`COPY` destination expiry | WL | Clock seam |
| A15 | `types/src/types/stream.rs:126,496` and `types/src/types/mod.rs:502` | `SystemTime::now()` | **`XADD *` auto-generated stream IDs** — the ID is the reply and the sort key | WL (stream family is compiled into the turmoil feature), SIM | Injected wall clock; stream IDs are the most visibly nondeterministic values in the system |
| A16 | `core/src/scripting/lua_vm.rs:45,56,141` | `start.elapsed()` vs `timeout_ms` / `+ grace_ms` | Whether a script is declared busy → `BUSY` error / `SCRIPT KILL` eligibility | TX, WL | Clock seam |
| A17 | `scripting/src/sandbox.rs:284,293` | `SystemTime::now()` inside the Lua `os.clock` / `os.time` builtins | Any script that branches on time; script-visible values | TX | Clock seam behind the sandbox builder |
| A18 | `server/src/failure_detector.rs:342,368,422` | `Instant::now()` in `record_success`, `reconcile_topology`, `has_quorum` | **Node FAIL verdicts and quorum**, i.e. whether auto-failover fires | CL | Clock seam; the probe cadence is already `tokio::time::interval` (virtual), so only the comparison is wrong-domain |
| A19 | `replication/src/tracker.rs:175,237,245,287` | `last_ack_time.elapsed()`, `Instant::now()` insert, `t.elapsed() < cooldown` | `min-replicas-max-lag` gating of writes; lag-disconnect cooldown | CL | Clock seam |

Also real-clock and decision-feeding, but lower blast radius (still class A):

| # | file:line | Decision |
|---|---|---|
| A20 | `acl/src/ratelimit.rs:20,24,191,212,225` | Token-bucket refill from real elapsed µs → whether a command is rate-limited |
| A21 | `core/src/hotkeys.rs:124,143,189` | `started_at.elapsed() >= duration_ms` → when hotkey sampling stops, i.e. the `HOTKEYS` reply |
| A22 | `server/src/cursor_store.rs:57,83,107` | Cursor TTL eviction → whether a SCAN cursor is still valid |
| A23 | `persistence/src/wal/flush.rs:427,441,584` | `since_last_flush()` drives the time-based batch boundary → WAL batch composition (bypassed under `fake-wal`; class A only for suites that use the real WAL) |

### A.2 Unseeded global RNG (16)

Every site below calls the process-global `rand::rng()` / `rand::random()`, which is seeded from the
OS at first use per thread. None takes an injectable RNG.

| # | file:line | Source | Decision it feeds | Blast | Suggested seam |
|---|---|---|---|---|---|
| A24 | `server/src/config/mod.rs:232` | `rand::rng().random_range(0..=self.jitter_ms)` — **inside `#[cfg(feature = "turmoil")]`** | Chaos delay injected between shard dispatches | WL, SIM | Thread the workload seed into `ChaosConfig` and hold a `SmallRng` |
| A25 | `server/src/config/mod.rs:262` | `rand::rng().random::<f64>() < connection_reset_probability` — also turmoil-only | **Whether a connection is reset** | WL, SIM | Same |
| A26 | `core/src/store/hashmap.rs:1504` | `rand::rng()` + `IteratorRandom::choose` over `self.data.iter()` | `RANDOMKEY` reply | WL | Shard-scoped seeded RNG |
| A27 | `core/src/store/hashmap.rs:1519` | `rand::rng()` + `IteratorRandom::sample` | Eviction candidate sampling (`allkeys-random`, `allkeys-lru` pool fill) | WL | Same |
| A28 | `core/src/noop.rs:143` | `rand::rng()` in `ExpiryIndex::sample` | Volatile eviction victims / Redis-style probabilistic expiry | WL | Same |
| A29 | `types/src/types/set.rs:284,314` | `random_range` / `SliceRandom` | `SRANDMEMBER`, `SPOP` replies | WL | RNG passed through `CommandContext` |
| A30 | `types/src/types/hash.rs:448,466` | `rand::rng()` | `HRANDFIELD` reply | WL | Same |
| A31 | `types/src/types/sorted_set.rs:854-856` | `rand::rng()` + `IteratorRandom` | `ZRANDMEMBER` reply | WL | Same |
| A32 | `types/src/topk.rs:118` | `rand::random::<f64>() < prob` | `TOPK.ADD` probabilistic decay → `TOPK.LIST`/`TOPK.QUERY` replies | WL | Same |
| A33 | `types/src/cuckoo.rs:444,451` | `rand::rng().random::<bool>()` / `random_range` | Cuckoo eviction victim → **whether `CF.ADD` succeeds or returns "filter is full"** | WL | Same |
| A34 | `types/src/vectorset.rs:163,484` | `vs.uid = rand::random()` (uid then seeds `StdRng::seed_from_u64(uid)` at `:688`) | HNSW level assignment → `VSIM` neighbour order and recall | WL | Seed the uid from the injected RNG; the downstream `StdRng` is already deterministic given a uid |
| A35 | `search/src/aggregate.rs:751,1099` | `rand::rng().random_range` reservoir sampling | `FT.AGGREGATE ... RANDOM_SAMPLE` reply | WL | Same |
| A36 | `core/src/eviction/lfu.rs:52` | `let r: f64 = rand::random()` | Probabilistic LFU counter increment → eviction victim | WL | Same |
| A37 | `replication/src/state.rs:435` | `rand::rng().fill(&mut [u8; 20])` in `generate_replication_id` | **The replication ID**, which is compared for partial-resync eligibility and echoed in `INFO`/`PSYNC` | CL, SIM | Seeded RNG on the replication identity |
| A38 | `server/src/commands/cluster/admin.rs:413` | `rand::random::<u64>()` | New node ID on `CLUSTER RESET HARD` | CL | Seeded RNG |
| A39 | `server/src/connection/scatter.rs:278` | `rand::rng().random_range(0..total_keys)` | Weighted shard selection for cross-shard `RANDOMKEY` | WL | Seeded RNG |

Also: `acl/src/parser.rs:621` (`ACL GENPASS`) — class A by the letter of the rule but almost certainly
intentional; list it, do not fix it before the others.

### A.3 Hash-container iteration order feeding a decision or a reply (11)

| # | file:line | Container | What order changes | Blast | Suggested seam |
|---|---|---|---|---|---|
| A40 | `core/src/pubsub.rs:712` (`publish`) | `channel_subs: HashMap<Bytes, HashMap<ConnId, PubSubSender>>`, iterated `conns.values()` | **Which subscriber receives a message first.** Directly observable in a per-client history. | PS, WL | `BTreeMap<ConnId, _>` — ConnId is already `Ord`, no hasher needed |
| A41 | `core/src/pubsub.rs:749` (`spublish`) | `sharded_subs`, same shape | Same, for `SPUBLISH` | PS, CL | Same |
| A42 | `core/src/pubsub.rs:658,670` | `sharded_subs.keys()` in `drain_sharded_channels_for_slot` | Order of forced `SUNSUBSCRIBE` confirmations during slot migration, and the `count` each one reports | CL | Same |
| A43 | `core/src/pubsub.rs:794,803,818,827,858` | `channel_subs`/`sharded_subs` keys | `PUBSUB CHANNELS` / `SHARDCHANNELS` / `NUMSUB` reply order | PS, WL | Same |
| A44 | `types/src/types/hash.rs:171` `HashEncoding::HashMap(HashMap<Bytes,Bytes>)` (std `RandomState`) | Hashes above the listpack threshold | **`HGETALL` / `HKEYS` / `HVALS` / `HSCAN` element order** in the reply | WL | Fixed-seed `BuildHasher` (or `IndexMap`) — small hashes are `Listpack` and already deterministic, which is why this is intermittent |
| A45 | `types/src/types/set.rs:71` `SetEncoding::HashSet(HashSet<Bytes>)` | Sets above the listpack threshold | **`SMEMBERS` / `SSCAN` / `SPOP count` / `SINTER`-family element order** | WL | Same |
| A46 | `core/src/store/hashmap.rs:1123` `all_keys()` → `commands/src/scan.rs:138` | `griddle::HashMap` keys | **`KEYS` reply order** | WL | Sort, or fixed-seed hasher on the keyspace |
| A47 | `core/src/store/hashmap.rs:1499-1528` | `self.data.iter()` piped into `choose`/`sample` | `RANDOMKEY` and eviction sampling depend on iteration order **as well as** the RNG (A26/A27) — fixing the RNG alone is not sufficient | WL | Fixed-seed hasher on the keyspace, or sample from a sorted snapshot |
| A48 | `core/src/client_registry/mod.rs:883-892` `eviction_candidates` | `clients: HashMap<u64, ClientEntry>`, `sort_by` on memory only | `sort_by` is **stable**, so equal-memory clients keep hash order → which client `try_evict_clients` kills first | WL | Add `id` as the tiebreaker in the comparator (one-line, no container change) |
| A49 | `core/src/client_registry/mod.rs:620` `kill_by_filter` | `clients.iter()` | Order in which matching connections are torn down (the *set* killed is order-independent, the *sequence* of disconnects is not) | WL | `BTreeMap<u64, ClientEntry>` |
| A50 | `core/src/client_registry/mod.rs:932` `get_all_stats`, `:527`, `:714` | `clients.iter()`/`.values()` | `CLIENT LIST` reply order | WL | Same |

### A.4 Non-`biased` `tokio::select!` with semantically significant branches (5)

Unseeded today (no `--cfg tokio_unstable`); still nondeterministic-by-design after seeding unless
`biased;` is added, but seeding makes them *reproducible*, which is what issue 14 needs.

| # | file:line | Branches | Why order matters |
|---|---|---|---|
| A51 | `core/src/shard/event_loop.rs:35` | new-conn recv / shard-message recv / 100 ms expiry tick / 10 s metrics tick / 100 ms waiter-timeout tick / 1 s search commit / continuation-lock event | The per-shard dispatch loop. A queued command vs. the waiter-timeout tick decides whether a blocking waiter times out *before or after* a racing `LPUSH` that would have satisfied it. |
| A52 | `server/src/connection.rs:558` | `killed()` / pubsub `recv_or_overflow` / tracking-invalidation recv / MONITOR recv / next command frame | Interleaving of pushed pub/sub and invalidation messages with command replies on the wire; whether `CLIENT KILL` beats an already-buffered command. |
| A53 | `replication/src/wait_coordinator.rs:223,227` | quorum-reached vs. `fence.changed()` (demotion) | Decides `WAIT` → integer count vs. `WAIT_ROLE_CHANGED` error. |
| A54 | `replication/src/replica/streaming.rs:82` | socket `read_buf` / `ack_interval.tick()` / `solicited_ack` | Which ACK reaches the primary first, which the primary's WAIT coordinator observes. |
| A55 | `server/src/scatter/broadcast.rs:190,298,347` | per-shard oneshot reply vs. shared deadline sleep | Under a *stepped* virtual clock exact ties are far more likely than in real time; the flip turns a real per-shard value into `ERR timeout` for MGET/KEYS/DBSIZE/FLUSHDB/FT.\*. |

Seven other `select!` sites already carry `biased;` and are class C (§4).

### A.5 Network / OS I/O that bypasses the `frogdb-net` seam (3)

The `frogdb-net` seam covers `TcpListener`/`TcpStream` and is compile-time-asserted (`net/src/lib.rs`
ends with `const _: () = {...}` identity checks so a `cfg` typo is a build error, not a silent
fallback). Three product paths sit outside it.

| # | file:line | Source | Decision it feeds | Blast | Suggested seam |
|---|---|---|---|---|---|
| A57 | `server/src/commands/replication.rs:133` | `(host, port).to_socket_addrs()` then `addrs.next()` in `resolve_primary` | **Which address a `REPLICAOF <host> <port>` replica dials.** Real OS resolver; multi-A-record hosts return results in resolver order. turmoil virtualizes DNS only through its own `lookup` API, which this bypasses. | CL, SIM | Add a `resolve` fn to `frogdb-net` with a `turmoil::lookup` arm, mirroring the existing `TcpStream` swap |
| A58 | `server/src/server/listeners.rs:60` and `server/src/observability_server.rs:28` | `tokio::net::TcpListener::bind(...)` used directly (the comment at `listeners.rs` notes RESP/admin/cluster-bus/TLS use the seam, HTTP does not) | HTTP/metrics/debug/admin-REST traffic is entirely outside the simulated network. Understandable — axum's `serve()` wants a real listener — but any sim that touches the observability port escapes the sim. | SIM, CL | Either route through the seam with an axum `Incoming` adapter, or `#[cfg(feature = "turmoil")]` the observability server off entirely and assert it is absent |
| A59 | `server/src/migrate.rs:11,246` | `tokio::net::TcpStream::connect` — unconditional, no turmoil branch | The outbound `MIGRATE` client dials an arbitrary target with real sockets. Unlike cluster resharding (which goes through the cluster-bus / replication seams), this has no simulated path at all. | CL | Route through `crate::net::TcpStream`; it is the same type in production |

Related but not live: `core/src/shard/connection.rs:4` types `NewConnection.socket` as
`tokio::net::TcpStream` unconditionally, but the struct is never constructed outside its own
definition (`ShardWorker::handle_new_connection` is a stub; the real accept path is
`server/src/acceptor.rs:283`, which builds a `ConnectionHandler` from `crate::net::ConnectionStream`).
Vestigial — worth deleting so it cannot become a seam hole later.

### A.6 Real threads under sim (1 confirmed + 4 conditional)

| # | file:line | Construct | Note |
|---|---|---|---|
| A56 | `persistence/src/rocks/reclaim.rs:124` | `std::thread::Builder::new()...spawn` | Post-clear space reclamation runs on a real OS thread; its completion races the simulated timeline. Reachable whenever RocksDB persistence is on. |
| — | `persistence/src/wal/writer.rs:75` | `std::thread::Builder` (WAL flush thread) | **Class C under turmoil**: `frogdb-server/turmoil` enables `frogdb-core/fake-wal`, and `shard/builder.rs:373-385` selects `FakeWalSink` for `WalMode::Fake`, which never constructs `WalWriter`. Class A for any suite that runs the real WAL. |
| — | `replication/src/replica_session.rs:489`, `replication-runtime/src/install.rs:120,175`, `server/src/tls_watch.rs:143`, `persistence/src/snapshot/rocks_coordinator.rs:250` | `tokio::task::spawn_blocking` | Each host's turmoil runtime is `new_current_thread`, but `spawn_blocking` still dispatches to the shared real blocking pool, so completion time is wall-clock. Conditional class A for full-resync / snapshot / TLS-reload suites. |
| — | `core/src/scripting/gate.rs:375` | `block_in_place` inside `catch_unwind` | On a current-thread runtime (which is what turmoil builds) this panics and is converted to `RemoteError::RuntimeUnavailable`. Deterministic *today*, but it means cross-shard scripting is silently unavailable under sim — worth a note in issue 14, not a determinism bug. |
| — | `server/src/server/init.rs:203`, `server/src/config/loader.rs:115` | `std::thread::available_parallelism()` | Shard count from host CPU count when `num_shards == 0` / `--shards auto`. The sim harness sets shards explicitly, so class C in practice; class A if a sim config ever leaves it at 0. |

---

## 3. Class B — observability only (compact)

Real-clock and hash-order uses whose only consumer is a metric, a log field, or an `INFO`/`SLOWLOG`
field. Benign; listed so a future sweep does not re-litigate them.

- `core/src/shard/event_loop.rs:43` — `envelope.enqueued_at.elapsed()` → `ShardQueueLatency::observe`
  only. **Issue 14 named this as suspect #2; it is not one.** (`core/src/shard/message.rs:53,69`
  mint the `enqueued_at` it reads.)
- `vll/src/coordinator.rs:210,435-448` — `start.elapsed()` → `frogdb_scatter_gather_duration_seconds`.
- `vll/src/queue.rs:45,58,168,174` — `enqueued_at`/`acquired_at` elapsed → VLL wait/hold-time metrics.
- `persistence/src/wal/flush.rs:583,593,621` — `WalFlushDuration` histogram.
- `core/src/scripting/executor.rs:311,346` — script duration for `SLOWLOG`/`LATENCY`.
- `core/src/slowlog.rs:102`, `core/src/latency.rs:91`, `acl/src/log.rs:72,106,139`,
  `server/src/monitor.rs:36` — entry timestamps.
- `server/src/info/sections.rs:48`, `server/src/commands/server.rs:177`,
  `server/src/commands/info.rs:161`, `core/src/client_registry/info.rs` — `INFO` uptime / `TIME`
  command / per-client age fields.
- `persistence/src/snapshot/metadata.rs:37,52`, `snapshot/mod.rs`, `snapshot/rocks_coordinator.rs:284`,
  `persistence/src/rocks/checkpoint.rs:106`, `replication/src/split_brain_log.rs:139` — recorded
  completion timestamps in metadata/log files.
- `core/src/shard/counters.rs`, `core/src/shard/diagnostics.rs:113`, `core/src/shard/post_execution.rs`,
  `core/src/client_registry/mod.rs:78,714,843,874` — counter snapshots and sums over hash containers
  (`usize`/`u64` addition is order-independent).
- `server/src/cluster_pubsub.rs:140` — `JoinSet::join_next` completion order folded into a
  commutative `total += count`.
- `server/src/server/shard_supervisor.rs:112` — `futures::future::select_all` over shard join
  handles; fail-stop path, aborts regardless of which shard is reported first.
- `types/src/skiplist.rs:140` — unseeded RNG for skiplist level. Internal structure only; ZSet
  iteration is score-then-member ordered, so no reply changes. Flag if a future `ZSCAN` ever
  iterates in skiplist-node order.

## 4. Class C — already injectable / virtualized / structurally safe (~38)

### 4.1 Environment and OS surface — swept clean

- **`std::env`**: 15 hits, 0 findings. Twelve are `env!("CARGO_PKG_VERSION")` (compile-time
  constant): `cluster/src/types.rs:97,117`, `replication/src/replica/connection.rs:201`,
  `server/src/commands/info.rs:192`, `server/src/commands/version.rs:43`,
  `server/src/server/init.rs:531,535`, `server/src/server/subsystems.rs:214`,
  `server/src/info/sections.rs:53`, `server/src/admin/handlers.rs:304`,
  `server/src/connection/auth_conn_command.rs:449`. Three are runtime `env::var` in `fn main()` /
  logging init, all feature-gated and all before any sim-relevant path: `server/src/main.rs:99`
  (`FROGDB_FLAME_OUTPUT`), `:132` (`COZ_PROFILE`), `server/src/config/loader.rs:256` (`RUST_LOG`).
- **Host / process identity**: no `hostname`, `gethostname`, `whoami` crate, MAC address, `sysinfo`,
  or machine-id anywhere. One `std::process::id()` — `server/src/config_persister.rs:99`, a
  `path.tmp.<pid>` suffix for `CONFIG REWRITE`'s atomic write, discarded on rename. (Caveat worth a
  line in the code: under turmoil every simulated node shares one real pid, so the suffix stops
  being a uniquifier — harmless unless two nodes ever write the same config path.)
- **Filesystem ordering**: 6 `read_dir` sites, **all safe**.
  `replication/src/replica_session.rs:561` sorts before streaming checkpoint files;
  `persistence/src/rocks/staged.rs:106` sorts numerically by epoch suffix (ties on identical
  timestamps fall back to OS order — one-second granularity plus serialized creation makes this
  unreachable in practice, but it is the one soft spot);
  `persistence/src/snapshot/stager.rs:202` (sum), `:224` (sorted by numeric epoch),
  `persistence/src/snapshot/rocks_coordinator.rs:111` (`.max()`),
  `replication/src/split_brain_log.rs:180` (`.any()`) are order-independent or sorted. No custom WAL
  segment enumeration exists (RocksDB owns it). ACL loading reads one explicit `aclfile` path; TLS
  cert loading uses explicit paths, no directory scan.
- **Temp paths**: `search/src/vector.rs:428` and `persistence/src/snapshot/stager.rs` use static
  `.tmp` suffixes, no pid/timestamp. All `tempfile`/`TempDir` hits are test-only.
- **ID-minting counters**: `NEXT_CONN_ID` / `NEXT_TXID` (`server/src/server/util.rs:16,19`) start at
  the literal `1`. No clock- or entropy-seeded ID counters.
- **Pointer-address ordering / hashing**: zero occurrences.
- **Float aggregation**: the only `sum::<f32>()` calls (`types/src/vectorset.rs:583,1338`) iterate a
  `Vec`, not a hash container.
- **Ephemeral ports**: `bind(0)` + `local_addr()` read-back in `server/src/server/listeners.rs` and
  `server/src/server/cluster_init.rs:448,458` goes through the `frogdb-net` seam and only matters if
  an operator configures port `0`; sim configs pin ports.

### 4.2 Seams that already work



- `frogdb-net` (`net/Cargo.toml`, `net/src/lib.rs`) — TCP primitives swap to turmoil's simulated net
  under the `turmoil` feature; two compile-time `const _: () = {...}` identity asserts make a `cfg`
  typo a build error rather than a silent fallback to real tokio, and `just lint-turmoil-features`
  enforces the feature forwarding up the graph. **The one seam that is done right** — apart from the
  three bypasses in A57–A59.
- The connect-factory dependency-injection pattern: `cluster/src/network.rs:37` and
  `replication/src/replica/mod.rs:19,87` name `tokio::net::TcpStream` as the *production-default*
  `ConnectFactory`, but `server/src/server/cluster_init.rs:253` (Raft dial) and
  `server/src/server/replication_init.rs:208` (replica→primary dial) inject a
  `turmoil::net::TcpStream::connect` factory under `#[cfg(feature = "turmoil")]`.
  `server/src/cluster_bus.rs:120` is gated the same way. Correct by design, not a gap.
- `frogdb-core/fake-wal` + `shard/builder.rs:373-385` + `shard/fake_wal_registry.rs` — deterministic
  recording WAL sink, enabled by `frogdb-server/turmoil`.
- `frogdb-core/shard-driver` (`core/Cargo.toml:25`, `shard/event_loop.rs:356,368-412`) — drives one
  active-expiry cycle and one waiter-timeout sweep synchronously, bypassing the `select!`. Exists but
  **is not enabled by the `turmoil` feature**; see remediation R6.
- `vll/src/shard.rs:18` — imports `tokio::time::Instant`; continuation-lock drain deadlines are on
  the virtual clock. Its tests assert exact virtual-time equality (`:695`, `:713`, `:1040`), which is
  the pattern the rest of the codebase should copy.
- `replication/src/wait_coordinator.rs` + `server/src/connection/blocking.rs:265` — `WAIT` deadlines
  use `tokio::time::Instant` with an explicit comment ("Timer clock, not wall clock"). Correct.
- `core/src/noop.rs:50,171` — `ExpiryIndex.by_time` / `FieldExpiryIndex.by_time` are
  `BTreeMap<(Instant, Bytes), ()>`; `get_expired_limited` returns keys in deterministic deadline
  order. (The `by_key` half is a `HashMap` — that is A28.)
- `core/src/store/hashmap.rs:37-45,1035-1099` — SCAN orders the keyspace by a fixed-seed
  `std::hash::DefaultHasher` content hash and sorts, deliberately so the cursor is independent of
  griddle's table layout. Deterministic.
- `cluster/src/state.rs:34,36,40` — `nodes`, `slot_assignment`, `migrations` are all `BTreeMap`.
  Cluster topology iteration is deterministic.
- `server/src/scatter/broadcast.rs:167,281,334,371` and `server/src/scatter/executor.rs` — shards are
  iterated by index (`senders.iter().enumerate()`), never by hash order.
- `vll/src/queue.rs:71,136,147` — VLL pending ops in a `BTreeMap<u64, _>` keyed by txid; lock grant
  order is txid order.
- `core/src/shard/active_expiry.rs:121` — `run_cycle(store, now)` already takes `now` as a parameter
  (only the *caller* and the internal budget are wrong-domain).
- Seven `select!` sites already carry `biased;`: `core/src/pubsub.rs:202`,
  `server/src/connection/blocking/coordinator.rs:91`, `server/src/connection/blocking.rs:281`,
  `replication/src/replica/mod.rs:297,322,333`, plus `server/src/server/util.rs:132` (mutually
  exclusive signals) and `replication/src/replica_session.rs:900` (converges).
- No `chrono` dependency anywhere in the product crates (`replication/src/split_brain_log.rs:54`
  does manual UTC arithmetic specifically to avoid it).
- No `futures::select!`, `select_biased!`, `FuturesUnordered`, or `futures::stream::select` in
  product code.

---

## 5. Per-class remedy, with research

### 5.1 Clocks

**Standard remedy: one injectable clock, no direct calls anywhere else.**

FoundationDB routes *all* time through `g_network()->now()` rather than `std::chrono`, so the
simulator can advance and fast-forward it; the architecture doc states plainly that "all sources of
nondeterminism and communication are abstracted, including network, disk, time, and pseudo random
number generator"
([apple.github.io/foundationdb/testing.html](https://apple.github.io/foundationdb/testing.html),
[Diving into FoundationDB's Simulation Framework](https://pierrezemb.fr/posts/diving-into-foundationdb-simulation/)).
TigerBeetle's VOPR stubs "the clock, network, and disk operations" and keys reproducibility on
`seed + git commit`
([docs/internals/vopr.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md));
TIGER_STYLE elevates determinism to "a meta principle above static allocation"
([docs/TIGER_STYLE.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md)).

turmoil's gap is documented only in its issue tracker: "now() is different in every run. And there
seems no API in turmoil to mock time" ([tokio-rs/turmoil#123](https://github.com/tokio-rs/turmoil/issues/123)).
S2.dev's DST writeup states the rule directly: "if code calls `std::time::Instant::now` or uses
`getrandom`, `quanta`, or anything that fetches entropy or time from outside tokio, the test stops
being deterministic" ([s2.dev/blog/dst](https://s2.dev/blog/dst)). Their fix was
[`mad-turmoil`](https://crates.io/crates/mad-turmoil), which layers madsim-style libc interception
(`clock_gettime`, `getrandom`, `getentropy`) on top of turmoil — the option to reach for if a
*dependency*, not FrogDB, is the offender.

**Fit for FrogDB.** FrogDB does not need libc interception: every offending call is its own. The
cheapest correct move is to make `tokio::time::Instant` the canonical monotonic type across the
product crates. That is a mechanical `use` swap (57 files), it is free in production (tokio's
`Instant` is a newtype over `std::time::Instant` in a non-paused runtime), and it makes every deadline
land in the same domain as every `tokio::time::interval` tick that already exists. `vll/src/shard.rs`
and `replication/src/wait_coordinator.rs` are the in-repo proof that this works. Wall time
(`SystemTime`, needed for `EXPIREAT`, `TIME`, stream IDs, `os.time`) has no tokio equivalent and does
need a small injected `WallClock` trait; derive it from `tokio::time::Instant` plus a fixed epoch
under sim.

### 5.2 Randomness

**Standard remedy: one seeded PRNG stream, threaded explicitly; no global entropy.**

madsim substitutes the whole dependency graph under `--cfg madsim` (`tokio`→`madsim-tokio`,
plus patched `getrandom` and `quanta`) and drives every random decision from a single global seeded
PRNG, "so with the same random seed, the same execution sequence can be produced"
([madsim-rs/madsim](https://github.com/madsim-rs/madsim),
[RisingWave part 1](https://risingwave.com/blog/deterministic-simulation-a-new-era-of-distributed-system-testing/)).
FDB's `deterministicRandom()` is the same idea in C++. Polar Signals' Rust DST writeup names the four
ingredients a DST harness must control — scheduling, time, randomness, failure injection — and
explicitly calls out mocking nondeterministic dependencies such as UUID generation
([polarsignals.com/blog/posts/2025/07/08/dst-rust](https://www.polarsignals.com/blog/posts/2025/07/08/dst-rust)).
eatonphil's checklist adds the general framing: "to 'control' randomness or time basically means you
support dependency injection… or passing the dependency as an explicit parameter"
([notes.eatonphil.com/2024-08-20-deterministic-simulation-testing.html](https://notes.eatonphil.com/2024-08-20-deterministic-simulation-testing.html)).

**Fit for FrogDB.** Two tiers:

- **Tier 1 (do first, tiny):** `server/src/config/mod.rs:232,262`. This code is already
  `#[cfg(feature = "turmoil")]`, so nothing production-facing changes — give `ChaosConfig` a
  `SmallRng` seeded from the workload seed. It is absurd that the fault injector is the one component
  the seed does not reach.
- **Tier 2:** a `&mut dyn RngCore` (or a `SmallRng` field) reachable from `CommandContext` and from
  `ShardWorker`, replacing the 13 `rand::rng()` / `rand::random()` call sites in `types`, `search`,
  `core/eviction`, and `core/store`. Seed it per shard from `(workload_seed, shard_id)`. Note A47:
  `random_key`/`sample_keys` need the hasher fix too, because `IteratorRandom::choose` consumes an
  iterator whose order is itself random.

### 5.3 Hash iteration order

**Standard remedy: `BTreeMap`, a fixed-seed `BuildHasher`, or `IndexMap` — chosen per site.**

`std::collections::hash_map::RandomState` seeds from "a high quality, secure source of randomness
provided by the host" at each `RandomState::new()`, and `HashMap`'s iterator "visit[s] all keys in
arbitrary order"
([RandomState](https://doc.rust-lang.org/std/collections/hash_map/struct.RandomState.html),
[HashMap](https://doc.rust-lang.org/std/collections/struct.HashMap.html)) — which is why order
differs *between two instances in one process*, exactly matching issue 14's in-process evidence. The
S2.dev team hit "Rust's `HashMap`s being randomized for DOS prevention" as an uncontrolled source
while hardening turmoil tests ([s2.dev/blog/dst](https://s2.dev/blog/dst)). Options:

- **`BTreeMap`/`BTreeSet`** — total order by construction. Right where the key is already `Ord` and
  small: `ConnId`-keyed pubsub subscriber maps, the client registry.
- **`rustc_hash::FxHashMap`** — deterministic with no seed at all ("same input always produces same
  hash"), not HashDoS-resistant ([rust-lang/rustc-hash](https://github.com/rust-lang/rustc-hash)).
  Fine for internal indexes; **not** fine for the keyspace or for user-supplied set/hash members,
  which are attacker-controlled.
- **`ahash::RandomState::with_seeds(k0,k1,k2,k3)`** — keeps HashDoS resistance and reproducibility
  simultaneously, by seeding from the run seed instead of host entropy ([docs.rs/ahash](https://docs.rs/ahash)).
  This is the right shape for `HashEncoding::HashMap`, `SetEncoding::HashSet`, and the `griddle`
  keyspace: production seeds from entropy, sim seeds from the workload seed.
- **`indexmap::IndexMap`** — "the iteration order of the key-value pairs is independent of the hash
  values of the keys" ([docs.rs/indexmap](https://docs.rs/indexmap)). Attractive for
  `SetEncoding::HashSet`/`HashEncoding::HashMap` because it makes large collections iterate in
  *insertion order*, matching the `Listpack` encoding they were just promoted from — which removes an
  existing, unrelated behavioural discontinuity at the listpack threshold.

Caveat worth writing into the fix: fixing the hasher fixes hash *values*, not order-independence.
Iteration order still depends on insertion/removal history, so the operation sequence must also be
deterministic — i.e. this fix only pays off once §5.1/§5.2/§5.4 land.

### 5.4 `tokio::select!` branch order

turmoil seeds tokio's scheduler RNG only under `--cfg tokio_unstable`
(`turmoil-0.7.1/src/rt.rs:38-41,234-246`); `tokio::runtime::RngSeed` is itself documented as
"Available on `tokio_unstable` and crate feature `rt` only"
([docs.rs](https://docs.rs/tokio/latest/tokio/runtime/struct.RngSeed.html), background:
[tokio-rs/tokio#4879](https://github.com/tokio-rs/tokio/issues/4879)). The same cfg also turns on
`UnhandledPanic::ShutdownRuntime` and the `LocalSet` panic forwarding turmoil wants
(`rt.rs:234-235,263-267`) — so setting it also fixes host panics being swallowed, which is issue 14's
third acceptance criterion.

For branches where order is *semantically* significant regardless of seeding, `biased;` plus an
explicit priority comment is the discipline FrogDB already uses correctly in
`connection/blocking/coordinator.rs:91` ("a value that arrives exactly at the deadline is never lost")
and `pubsub.rs:202`. Extend it to A51–A55.

### 5.5 Threads and blocking pools

FDB avoids the problem structurally: Flow's actor model is single-threaded, only `ACTOR` functions may
`wait()` ([apple.github.io/foundationdb/flow.html](https://apple.github.io/foundationdb/flow.html));
TigerBeetle is likewise single-threaded in the simulator. turmoil gives each host a
`new_current_thread` runtime, so FrogDB is *almost* there — the leaks are `spawn_blocking` (real
blocking pool) and the two raw `std::thread::Builder` spawns.

For genuinely concurrent intra-process code that turmoil structurally cannot see,
[`shuttle`](https://github.com/awslabs/shuttle) is the complementary tool — it replaces
`std::thread`/`std::sync` and schedules threads under a seeded scheduler (`check_random`,
`check_pct`); it is explicitly not sound but "scales to much larger test cases than Loom". FrogDB
already has a `shuttle` feature on `frogdb-server` and `frogdb-core`, so the split of
responsibilities is already the recommended one.

**Fit for FrogDB.** Add a `#[cfg(feature = "turmoil")]` branch that runs the `spawn_blocking` bodies
inline (they are all short and CPU-bound: `route_dataset`, `create_checkpoint`, a TLS file read), and
gate `rocks/reclaim.rs`'s thread the same way. Nothing about their semantics requires a thread.

### 5.6 Where the industry says to start

Both the eatonphil checklist and the Polar Signals four-ingredient framing land in the same place:
control randomness and time first, because they are cheap and mechanical; control scheduling next;
treat container ordering as a follow-on that only pays once the operation sequence is already stable.
Antithesis is the counter-example that proves the cost curve — it achieves determinism at the
*hypervisor* layer precisely so applications need no seams
([antithesis.com/docs/resources/deterministic_simulation_testing/](https://antithesis.com/docs/resources/deterministic_simulation_testing/),
[databases.systems/posts/open-source-antithesis-p1](https://databases.systems/posts/open-source-antithesis-p1)) —
but that is a different product, not a refactor. sled's guidance is the architectural end-state:
push I/O to the edges and keep the core a pure state machine over messages
([sled.rs/simulation.html](http://sled.rs/simulation.html)). A useful live index:
[ivanyu/awesome-deterministic-simulation-testing](https://github.com/ivanyu/awesome-deterministic-simulation-testing).

---

## 6. Proposed remediation order (cheapest first)

Each step names the issue-14 acceptance criterion it advances. **Add the byte-identical-history
assertion test (criterion 1) first**, marked `#[ignore]`, so every step below has a pass/fail signal
instead of a digest eyeballed by hand.

| # | Change | Cost | Expected effect on issue 14 |
|---|---|---|---|
| **R0** | Add the "run the same `Workload` twice, assert histories equal" test. Also fix `repro_path` to key on `(seed, profile, ops)`. | XS | Criteria 1 (harness) and 4. Without R0 the rest is unmeasurable. |
| **R1** | Set `--cfg tokio_unstable` for turmoil builds. Prefer `[build] rustflags` in `.cargo/config.toml` (it must apply to the whole dependency graph, and turmoil's block is compiled in turmoil's own crate) with the `concurrency*` recipes inheriting it. | XS | **Criterion 3, verbatim.** Also switches on `UnhandledPanic::ShutdownRuntime` so host panics fail the test. Expect: `select!` order reproducible; issue 14 already showed this alone is insufficient. |
| **R2** | Seed the chaos injector: give `ChaosConfig` a `SmallRng` from the workload seed; replace `rand::rng()` at `config/mod.rs:232,262`. | XS | Removes 2 A-sites that are *inside the turmoil feature*. Any profile with jitter or reset probability becomes reproducible. |
| **R3** | **Clock-domain unification, phase 1 — the blocking path.** `server/src/connection/blocking.rs:44` and `blocking/coordinator.rs:78,87` → `tokio::time::Instant`; thread a virtual `now` into `core/src/shard/blocking.rs:170,303` and `wait_queue::collect_expired`. | S | **The single highest-value change.** Directly targets `MultiWaiter`, the profile issue 14 shows flapping 0/1/0/0. |
| **R4** | **Clock-domain unification, phase 2 — expiry.** `event_loop.rs:177` sources `now` from `tokio::time::Instant`; `active_expiry.rs`'s 25 ms real budget becomes either a virtual-clock budget or an op-count under sim; `store/hashmap.rs:558,1392,1543,1555` take an injected `now`. Fix the false comment at `hashmap.rs:543` while there. | S–M | Removes the largest cluster of A-sites (A5–A11) and makes TTL behaviour under sim mean what the code already claims it means. |
| **R5** | Sweep the remaining `std::time::Instant` imports in product crates to `tokio::time::Instant` (55 files, mechanical `sed`), leaving `SystemTime` alone. Add a `just lint-clock-seam` grep gate, in the style of the existing `lint-turmoil-features` / `lint-metrics-chokepoint` recipes, forbidding `std::time::Instant::now` outside an allowlist. | M | Closes A18–A23 and prevents regression. This is FDB's "no direct time calls" rule, enforced the way this repo already enforces its other seams. |
| **R6** | Enable `frogdb-core/shard-driver` from the `turmoil` feature and use `run_one_expiry_cycle` / the waiter sweep from the harness where possible, instead of racing the `select!`. | S | Removes A51's expiry and waiter branches from the race entirely for driven tests. The seam already exists and is unused by the sim. |
| **R7** | Add `biased;` + a priority comment to A51–A55, matching the existing convention. | S | Makes those five loops deterministic *by construction*, not merely by seed — so a future tokio version that changes its RNG cannot silently invalidate pinned seeds. |
| **R8** | Seeded RNG through `CommandContext` / `ShardWorker` for A26–A36, A39. Per-shard seed = `(workload_seed, shard_id)`. | M | Removes the remaining 13 unseeded-RNG sites. Needed before any `SRANDMEMBER`/`SPOP`/`RANDOMKEY`/`ZRANDMEMBER` profile can be pinned. |
| **R9** | Deterministic ordering for the 11 hash-order sites: `BTreeMap<ConnId,_>` for pubsub (A40–A43) and the client registry (A49–A50); a tiebreaker on `id` in `eviction_candidates` (A48, one line); seedable `ahash::RandomState` — or `IndexMap` — for `HashEncoding::HashMap`, `SetEncoding::HashSet`, and the `griddle` keyspace (A44–A47). Sort `all_keys()` for `KEYS` (A46). | M | Removes the last class that can change a *reply value* for an identical state. Only fully effective once R3–R8 make the operation sequence stable. |
| **R10** | Inline the four `spawn_blocking` bodies and the `rocks/reclaim.rs` thread under `#[cfg(feature = "turmoil")]`. | S | Closes A56 and the conditional thread leaks; matters for full-resync / snapshot / cluster suites, not for `MultiWaiter`. |
| **R11** | Close the three `frogdb-net` bypasses: add a `resolve()` with a `turmoil::lookup` arm to `frogdb-net` and use it in `commands/replication.rs:133` (A57); route `migrate.rs:246` through `crate::net` (A59); either adapt or `cfg`-off the observability HTTP listener under turmoil (A58). Delete the vestigial `core/src/shard/connection.rs` `NewConnection`. Extend `just lint-turmoil-features` to also grep for raw `tokio::net::` outside the allowlisted default arms. | S–M | No effect on `MultiWaiter`; required before any `REPLICAOF`-by-hostname or `MIGRATE` scenario can be simulated at all, and the lint prevents the next bypass. |
| **R12** | Once R0–R9 hold: re-verify issue 11's findings and re-pin surviving classes by seed. | — | **Criterion 5.** |

**Prediction.** R1+R2+R3 should be enough to make `MultiWaiter` byte-identical across repeats; R4
extends that to any profile that touches TTLs; R8+R9 are required before profiles that use random
commands or large sets/hashes can be pinned. Criterion 2 ("the sources are identified and named")
is satisfied by this document — the answer is **all three suspects, plus unseeded chaos injection and
client-visible hash-order in replies**.

---

## 7. Method notes / caveats

- Counts are of *sites*, not of grep hits: 383 raw time hits across the product crates collapse to 23
  class-A decision sites once test modules and metric-only uses are removed.
- `#[cfg(test)]` hits were excluded from counts but are relevant to remediation: `active_expiry.rs`
  and `hashmap.rs` tests call `Instant::now()` freely, so R4/R5 will touch them.
- Floats were not deep-dived (per scope). The only order-dependent float aggregation found is
  metric histogram observation, which is class B.
- One claim not verified by execution: that `tokio::time::Instant::from(std::time::Instant)` under
  `start_paused(true)` produces the drift described in §0/A1. It follows from `start_paused` freezing
  the virtual clock at runtime-build time while the std clock advances, but it is worth confirming
  empirically with a two-line probe before R3 is written, since R3's design depends on it.
