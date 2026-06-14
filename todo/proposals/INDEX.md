# Architecture Proposals

Deepening refactors surfaced by an architecture review on 2026-06-12. Each proposal turns a shallow
module into a deep one: more behaviour behind a smaller interface, with locality (bugs and change
concentrate in one place) and leverage (callers and tests get more per unit of interface) as the
payoff. All evidence verified against the code at time of writing.

## Round 1 — implemented (2026-06-12)

Ordered by leverage:

1. [01-declarative-command-spec.md](01-declarative-command-spec.md) — **Implemented**
   (`8d573510`…`182898b6`, 13 commits): 377/377 commands declare a `CommandSpec`; dispatcher
   derives keys/events/waking/WAL; legacy opt-in methods deleted; registry-wide exhaustiveness
   tests. Found+fixed along the way: MSETEX/BITOP/XGROUP/XREADGROUP WAL bugs, ZINCRBY wake,
   STORE-destination WAL gap (`WalStrategy::Dynamic`).
2. [02-typed-store-access.md](02-typed-store-access.md) — **Implemented** (`e792ed42`,
   `a1a702ca`, `760944d1`, `b8db0087`): `StoreTypedExt` owns the WrongType invariant; unwrap
   chains in commands now 0, enforced by the `lint-no-typed-unwrap` gate (Justfile + lefthook);
   triplicated `get_or_create` + five utils wrappers deleted. Follow-up candidate: extend
   `typed_family_accessors!` to the probabilistic/extension families (tdigest, cuckoo, bloom,
   topk, cms, HLL, timeseries, vectorset, JSON) to retire ~128 remaining
   `as_X().ok_or(WrongType)` read-path sites.
3. [03-unified-post-execution-pipeline.md](03-unified-post-execution-pipeline.md) —
   **Implemented** (`0db27d82`, `bfe61d2e`): `shard/post_execution.rs` owns the canonical
   9-step `WRITE_EFFECT_ORDER`; `WalPhase`/`EffectScope` are data; `pipeline.rs` deleted; each
   effect step invoked from exactly one site; order-invariant regression tests added. (Drift was
   pre-fixed in `6e483280`/`23469adc`.)
4. [04-connection-state-encapsulation.md](04-connection-state-encapsulation.md) —
   **Implemented** (`694ab867`…`470004e7`, 5 commits): `ConnectionState` owns all transitions as
   methods; six state-machine fields now private; external field reach-ins 0 (40+31+6+3+3+3 → 0,
   enforced by the compiler); MULTI five-field reset and the subscribe-limit blocks collapsed to
   one method each; ASKING/LocalServe nuance preserved exactly; 12 socket-free state-machine
   tests. Phase 6 (tracking/auth/blocked fields) intentionally deferred.
5. [05-single-routing-decision.md](05-single-routing-decision.md) — **Implemented** (`510fec8c`):
   dead `CommandRouter`/`RouteResult`/`ScatterStrategy`/`op_to_handler` twin deleted (zero
   non-test callers confirmed); live mapping moved into `router::route_connection_level` +
   `handler_for` as pure functions; `connection_level_handler_for` is now a one-line delegation;
   dead `ConnectionLevelHandler::Cluster` variant removed; reachability + exhaustiveness tests
   added.
6. [06-recovery-orchestrator.md](06-recovery-orchestrator.md) — **Implemented** (`1904d9a8`…
   `b03a8b97`, 6 commits): `server/src/recovery/` owns `recover(RecoveryInputs) ->
   Result<RecoveredState, RecoveryError>` over six ordered phases (checkpoint install, RocksDB
   open, shard restore, functions, replication-state reconcile, cluster storage); `init_persistence`
   + the `PersistenceInitResult` alias deleted; startup collapses to one `recover()` call + wiring.
   11 socket-free seam tests (the staged-checkpoint install path previously had zero). Search-index
   recovery deferred (non-`Send` per-shard handles) — documented at the call site.

## Round 2 — proposed (2026-06-13)

A second deepening review on 2026-06-13 (fresh fan-out over the largest crates, excluding the six
already-implemented refactors). Ordered by leverage:

7. [07-scatter-gather-executor.md](07-scatter-gather-executor.md) — **Proposed**: ~46 broadcast
   handlers (KEYS, DBSIZE, SCAN, RANDOMKEY, TS.QUERYINDEX, FT.SEARCH/AGGREGATE, SCRIPT *, pubsub
   introspection) hand-roll lock-free fan-out + timeout + merge across internal shards. A
   `ScatterGather` + `MergeStrategy` seam (broadcast / cursor-paginate / sum / sorted-union) gives
   the broadcast path the depth the keyed VLL `ScatterGatherExecutor` already has. Biggest test
   win: merge strategies become unit-testable with mock shard results (today live-socket only).
8. [08-acl-enforcement-seam.md](08-acl-enforcement-seam.md) — **Proposed**: `check_command` /
   `check_command_with_key` called from 4 paths (guards/auth/transaction/routing), each re-doing
   key extraction + ACL logging + NOPERM formatting differently. The deep half (`FullAclChecker`,
   `AclError` Display = Redis NOPERM string, `AclLog` denial methods) already exists in the acl
   crate but is bypassed. A `PermissionGuard` seam owns check + log + error. Security-adjacent:
   divergent paths silently skip audit logs.
9. [09-serialization-typecodec-registry.md](09-serialization-typecodec-registry.md) —
   **Proposed**: a type's snapshot encode/decode live apart, joined only by a hand-kept `u8`
   marker; a missing decode arm compiles and breaks only on replica full-sync / restart. A
   `TypeCodec { marker, encode, decode }` registry with a non-exhaustive `TypeMarker` match makes a
   missing decode a compile error — the persistence-wire analogue of proposal 01's exhaustiveness.
   Wire bytes MUST stay identical (on-disk + replication compat).
10. [10-active-expiry-coordinator.md](10-active-expiry-coordinator.md) — **Proposed**:
    `run_active_expiry` is ~116 lines of glue inside the shard `select!` tangling 6 concerns
    (get-expired → delete → tracking → search → notifications → metrics). An
    `ActiveExpiryCoordinator::run_cycle(store, now) -> ExpiryResult` seam makes expiry
    unit-testable without spinning the event loop; side effects stay shard-side past the seam.
11. [11-probabilistic-typed-accessors.md](11-probabilistic-typed-accessors.md) — **Proposed**:
    completes proposal 02's deep module. The WrongType invariant has two homes — `StoreTypedExt`
    for the 6 core families, hand-rolled `as_X().ok_or(WrongType)?` for the rest. **66 verified
    sites** across 8 families (tdigest 13, vectorset 12, cuckoo 9, timeseries 9, bloom 8, topk 6,
    hll 5, cms 4); JSON already migrated (existence proof). Needs a `ValueType`/`DefaultValueType`
    split (none of the 8 has a parameterless default) and extends the `lint-no-typed-unwrap` gate
    to ban `.ok_or(WrongType)` (multiline-aware). Supersedes proposal 02's "~128" estimate.
12. [12-blocking-wait-coordinator.md](12-blocking-wait-coordinator.md) — **Proposed**: the
    wait/wake machinery is split across `handlers/blocking.rs` (select!/timeout/cleanup, `WaitOutcome`
    buried in a handler), `connection/state.rs` (`BlockedState`), and core `shard/blocking.rs`
    (recursive BLMOVE fanout, depth-16 cap) through an implicit interface — untestable without a
    live socket. A `BlockingWaitCoordinator` (server) + `WaiterSatisfaction` strategy (core)
    concentrate it; reusable for XREAD/WAIT/replication-ACK.
13. [13-column-family-manifest.md](13-column-family-manifest.md) — **Proposed**: `open_with_warm`
    derives the CF set to open from the current `warm_enabled` flag, not persisted state →
    warm-on-then-reopen-off fails with RocksDB's cryptic "Column families not opened". Same
    must-open-all-CFs invariant as the shard-count guard (fixed last round). Phase 1 = a
    `WarmTierMismatch` hard error mirroring that guard; Phase 2 = a `ColumnFamilyManifest` folding
    shard-count + warm + future CF invariants behind one `reconcile` seam (two adapters = real seam).

## Correctness flags found during the review

Bugs adjacent to (but separable from) the proposals:

- **Replication offset never persisted after startup** — ~~tracker only updates an `AtomicU64`;
  staged `replication_metadata.json` has no reader~~ Fixed in `17f01c9d` (primary saves at
  shutdown + pre-snapshot hook; replica reconciles from staged metadata; corrupt/missing →
  full resync).
- **Partial resync never granted; checkpoints record offset 0** — ~~stale
  `state.replication_offset` read by FULLRESYNC/`can_partial_sync`~~ Fixed in `64f15bce`
  (offset captured from tracker before checkpoint cut; offset ≤ data invariant).
- **No replication backlog — partial resync structurally ungrantable** — `+CONTINUE` would tail
  the live broadcast only, silently dropping `(requested, current]`. Gated explicitly behind
  `partial_sync_replay_supported()` (false). Implementing a backlog ring buffer (Redis
  `repl-backlog`) is the unlock; the split-brain `ReplicationRingBuffer` is a separate mechanism.
- **INFO replication `master_replid` reports zeros** — ~~built from `ctx.node_id` instead of the
  real replication id~~ Fixed in `99c8f91e` (`handle_info` patches from the shared
  `ReplicationState`). `master_replid2`/`second_repl_offset` remain `0`/`-1` — no
  failover-continuity (replid2) concept yet; needed only when partial resync across failover
  becomes possible (see backlog flag above).
- **Shard-count mismatch silently drops recovered data** — ~~`server/src/server/shards.rs:60`
  uses `unwrap_or_default()`~~ Fixed in `95da0256` (hard startup error at `RocksStore::open`,
  persisted count derived from `shard_*` column families).
- **Warm-tier toggle breaks reopen** — data dir created with warm tier enabled fails to reopen
  with it disabled (`tiered_warm_*` CFs not reopened → RocksDB "column families not opened").
  Same must-open-all-CFs constraint as the shard-count case; needs a decision on whether config
  toggling is supported.
- **Missing command behaviors** — LREM emits no keyspace event; RPOPLPUSH/LMOVE never wake blocked
  list waiters; GEORADIUS STORE destination key not extracted (see proposal 01).
- **Post-execution drift** — transaction path skips keyspace metrics and keysizes flush; scatter
  BCAST tracking invalidation omitted (see proposal 03).
- **Checkpoint staging untested** — ~~zero tests~~ Fixed in `165fc950` (8 tests: happy path,
  crash windows, idempotency, partial states). Testing found a real bug, fixed in `3e37ad7b`:
  an incomplete staged dir (no `CURRENT`) was installed anyway — live DB moved aside, fresh
  empty DB created in its place. Install now validates before touching the live dir.
- **STORE-destination WAL gap** — ~~arg-index WAL actions miss GEORADIUS/SORT STORE
  destinations~~ Fixed in `fc0dce48` (`WalStrategy::Dynamic` resolves from the command's own key
  extraction; restart-survival tests). Same-class WAL bugs also fixed: MSETEX, BITOP, XGROUP,
  XREADGROUP persisted a non-key arg or nothing. ZINCRBY waiter wake fixed in `182898b6`.
- **Missing command behaviors — class closed** — proposal 01 implemented (13 commits,
  `8d573510`…`182898b6`): 377/377 commands declare a `CommandSpec`; registry-wide exhaustiveness
  tests make silent event/WAL/wake omissions unrepresentable.
- **Cross-shard keyspace notifications lost** — SUBSCRIBE registers on shard 0 (broadcast
  coordinator) but keyspace events emit on the key-owner shard; in multi-shard mode a keyevent for
  a key not on shard 0 never reaches the subscriber.
- **Keyspace hit/miss misclassification** — ~~`track_keyspace_metrics` classifies via
  `Response::Null`, but GET/HGET misses return `Response::Bulk(None)`~~ Fixed in `23469adc`
  (lookup-level classification via `CommandContext::record_keyspace_lookup`).
- **INFO stats hardcodes `keyspace_hits:0` / `keyspace_misses:0`** — ~~never wired to the real
  counters~~ Fixed in `02de350b` (`handle_info` patches from the Prometheus registry).
- **CONFIG RESETSTAT doesn't reset keyspace_hits/misses** — Redis zeroes them; FrogDB's are
  Prometheus monotonic counters (reset would break `rate()`/`increase()` semantics). Needs a
  design decision (baseline-offset vs registry recreation) — deliberate divergence for now.
- **Keyspace-stats command coverage gap** — only GET/HGET/LINDEX/GETDEL/GETEX/MGET report
  lookups; Redis counts most read commands (LRANGE, SMEMBERS, ZRANGE, …). Enhancement, not a bug.

### Round 2 flags (2026-06-13)

Found while writing proposals 07-13. All verified against the code; grouped by proposal.

- **Broadcast gathers have no timeout (proposal 07)** — PUBSUB CHANNELS/NUMSUB/NUMPAT/SHARDCHANNELS/
  SHARDNUMSUB (`handlers/pubsub.rs:456,492,528,562,598`) and SCRIPT LOAD/EXISTS/FLUSH/KILL
  (`handlers/scripting/script.rs:76,120,133`) gather with a bare `rx.await` — unlike every
  `scatter.rs` handler, which wraps a `tokio::time::timeout`. A stalled internal shard hangs the
  connection forever. Send failures are also swallowed (`let _ = sender.send`), so a dropped shard
  silently under-reports.
- **SCRIPT LOAD can diverge the per-shard Lua cache (proposal 07)** — `handlers/scripting/script.rs:34-54`
  ignores all per-shard send results and awaits only shard 0, returning a SHA while other internal
  shards may never have received the script → EVALSHA can fail on some shards after a slow/dropped
  send.
- **Dead scatter code (proposal 07)** — `handlers/scatter.rs:714-844` (`ScatterHandler`, five
  `merge_*`, `ScanResult`) and `scatter/strategies.rs:304-462` (`KeysStrategy`/`DbSizeStrategy`/
  `FlushDbStrategy`) have zero callers; tested copies of merge logic that diverge from the live
  inline versions. Delete in Phase 1.
- **In-transaction ACL denials skip the audit log (proposal 08, SECURITY)** — queue-time key
  denials (`handlers/transaction.rs:458-462`) and channel denials (`:473-488`) return NOPERM but
  never call `log_key_denied`/`log_channel_denied`, unlike the live paths (`routing.rs:66`,
  `guards.rs:113`). EXEC does not recheck, so in-MULTI denials never reach `ACL LOG`.
- **NOPERM subcommand message uses a space, not a pipe (proposal 08)** — `guards.rs:204` formats
  the error as `'config set'`; the same function logs it as `config|set` (`:181`) and
  `AclError::NoPermissionSubcommand` uses a pipe. Diverges from Redis.
- **ACL DRYRUN mis-simulates key access (proposal 08)** — `handlers/auth.rs:458-468` uses a
  heuristic (first arg only, always `ReadWrite`) instead of `handler.keys()` +
  `key_access_type_for_flags`. Mis-reports read-only commands, multi-key commands, and commands
  whose key isn't arg 0; the audit tool disagrees with real enforcement.
- **VectorSet encode/decode bound asymmetry (proposal 09)** — `serialization/search.rs:117-134`
  decode rejects `m>512`, `ef>4096`, `dim>65536`, but `VADD` parses `M`/`EF` with unbounded
  `parse_usize` (`vectorset/vadd.rs:135,161`) and `VectorSetValue::new_inner` applies no cap. A set
  created with e.g. `M 1000` serializes fine but fails to load on restart/replica full-sync →
  silent key loss. (Reproduce — realized impact depends on usearch accepting the oversized index.)
- **TimeSeries decode coerces unknown DuplicatePolicy to `Last` (proposal 09)** —
  `serialization/timeseries.rs:96-104` catch-all `_ => DuplicatePolicy::Last`, unlike every other
  enum decode (which errors). Not a loss bug for self-produced data (only 0-5 written); consistency
  fix.
- **READONLY t-digest queries clone the digest on every call (proposal 11)** — TDIGEST.QUANTILE/
  CDF/RANK/REVRANK/TRIMMED_MEAN (`tdigest.rs:364,408,450,494,660`) are flagged `READONLY`/`wal:
  NoOp` but call `get_mut` + `as_tdigest_mut` and invoke only read methods → copy-on-write clone of
  a shared digest on every query. Migrating to the read accessor removes the clone. (Same COW-before-
  check class affects ~19 write-path probabilistic sites; see proposal 11 Class A.)
- **Core stream files still carry `.ok_or(WrongType)` remnants (proposal 11)** — proposal 02 killed
  the `.unwrap()` form but never banned `.ok_or`, so ~19 `as_*_mut().ok_or(WrongType)` survive in
  `commands/src/stream/*`. Fold into proposal 11 Phase 1 before the gate extension can go green.
- **Active expiry: field-emptied key deletion is silent and under-counted (proposal 10)** — when a
  hash is emptied by `purge_expired_hash_fields` and the key is deleted (`event_loop.rs:207-214`),
  the branch skips `emit_keyspace_notification` + `fire_key_expired` (unlike the key-level path at
  `:165-171`) and feeds only `frogdb_fields_expired_total`, not `expired_keys` (`:218-237`). So such
  expirations are invisible to keyspace subscribers/USDT probes and under-count INFO `expired_keys`.
- **Active expiry time budget excludes the scan (proposal 10)** — `get_expired_keys`/
  `get_expired_fields` clone every due entry into a `Vec` up front (`store/noop.rs:99-112,223-233`)
  before the 25ms budgeted loop (`event_loop.rs:138`), so a large TTL avalanche can stall the shard
  event loop past budget.
- **Blocking timeout returns the wrong RESP2 nil shape (proposal 12)** — the timeout/channel-drop
  paths emit `Response::Null` (`$-1`) for every op (`handlers/blocking.rs:157,170,125,130`; shard
  `core/src/shard/blocking.rs:149`), but BLPOP/BRPOP/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP/XREAD return a
  null *array* (`*-1`, `Response::NullArray` exists at `protocol/response.rs:162`) in RESP2. The
  timeout path discards the op so it can't pick the shape. RESP3 unaffected; BLMOVE/BRPOPLPUSH
  correctly want `$-1`.
- **Blocking lost-element timeout race (proposal 12)** — two independent timeout authorities (server
  `select!` vs shard's 100ms `check_waiter_timeouts`, `event_loop.rs:23`). If a push is processed by
  the shard before the server's `UnregisterWait` arrives, `complete_blocked_waiter`
  (`core/src/shard/blocking.rs:665-672`) pops the list element and sends it into the abandoned
  oneshot after the server already returned a timeout nil → element removed from the store, delivered
  to nobody. Narrow but genuine data loss.
- **Warm-tier toggle breaks reopen (proposal 13)** — `rocks/mod.rs:74-88` derives `all_cf_names`
  from the current `warm_enabled`; created-with-warm then reopened-without skips `tiered_warm_*` CFs
  → open fails at `:126-134` with "Column families not opened". (Inverse off→on is benign — `create_cf`
  is guarded at `:136`, correcting the original "duplicate CFs" hypothesis.) Untested. Fix:
  `WarmTierMismatch` hard error.
- **`list_cf(...).unwrap_or_default()` silently disables both reopen guards (proposal 13)** —
  `rocks/mod.rs:91`: a failed CF enumeration (transient I/O, permissions, damaged MANIFEST) yields
  an empty `existing_cfs` even when the DB exists, bypassing the shard-count guard
  (`count_persisted_shards(&[]) == 0`) and building an empty descriptor set → confusing open failure
  instead of an actionable error. Propagate the error.
