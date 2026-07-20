# Round-7 Follow-ups

Open items from the round-7 architecture review (proposals 53–62, implemented 2026-07-16) and its
adversarial post-implementation review. Each is independently actionable. Evidence verified against
the tree at `0324e2df`.

## 1. WAIT wake-up on replica resume-seed (review finding 57-A) — FIXED

**Problem.** Proposal 57 split resume-seeding from ACK ingestion: `seed_replica_position` →
`tracker.seed_acked_position` advances the monotonic acked offset **without** notifying WAIT
waiters (`replication/src/tracker.rs`, seed call at `replica_session.rs` `start_streaming`). The
old spurious notify was accidentally load-bearing for one corner: a WAIT blocked on target `T`
while a replica already acked ≥ `T` reconnects via partial resync mid-wait. Old code woke the
waiter instantly on seed; new code parks it until the next genuine ACK.

**Bounded, no hang:** the replica ACKs spontaneously every 1s (`replica/streaming.rs`
`ACK_INTERVAL`), so worst case ≈ 1s added latency, even for `WAIT n 0`.

**Options.**
- (a) Accept + document: add the ≤1s bound to `seed_acked_position` docs and a test pinning that a
  blocked WAIT satisfied only by a reconnecting replica resolves within the spontaneous cadence.
- (b) Notify without counting: after seeding, fire the tracker's `ack_notify` channel (waiters
  re-count from the same atomic, so a seed that satisfies quorum wakes immediately; one that
  doesn't is a harmless spurious wake — the pre-57 behavior, minus the semantic conflation).

**Recommendation:** (b) — one `ack_notify.send()` in `seed_acked_position`, plus a regression test
"seed that satisfies a blocked WAIT wakes it immediately". Keeps the ingest/seed API split intact.

**Status — FIXED** (option b): `ReplicaSession::seed_acked_position` returns the monotonic-advance
bool; the tracker fires `ack_notify` only on a genuine advance (mirrors `record_ack`, no spurious
wakes on stale seeds). Both pinning tests flipped, new regression test
`seed_acked_position_wakes_blocked_wait_for_acks`, doc comments updated in `tracker.rs`,
`offset_coordinator.rs`, `replica_session.rs`. 132/132 replication tests green.

## 2. Inline (telnet-style) command support (62-E, decision) — IMPLEMENTED

**Problem.** No inline-command path exists: non-`*` input fails in the upstream RESP2 decoder
(`server/src/connection/codec.rs`); real Redis accepts `PING\r\n` from telnet/netcat.
`redis-regression/tests/protocol_tcl.rs:6` explicitly excludes inline commands;
`tcl_unbalanced_number_of_quotes` passes only incidentally (asserts any `-ERR`).

**Decision needed:** implement for parity, or document divergence.

**If implementing** (proposal 62 Item E sketch): in `FrogDbResp2::decode`, on a first byte that is
not `*` (after the existing blank-line skip), read to `\r\n`, split on whitespace with Redis's
quote rules (`sdssplitargs` semantics: single/double quotes, `\xHH` escapes, unbalanced-quote →
`ERR Protocol error: unbalanced quotes in request`), synthesize a `BytesFrame::Array` of bulk
strings. Cap inline line length at `PROTO_INLINE_MAX_SIZE` (Redis: 64KB) with
`ERR Protocol error: too big inline request`. Unlock the excluded TCL regression tests.
**If documenting:** add divergence entry + a codec test pinning the error shape for inline input.

**Status — IMPLEMENTED.** `parse_inline_command` in `FrogDbResp2::decode` with a faithful
`sdssplitargs` port (quote rules, `\xHH` + control escapes, unbalanced-quote error), 64KB
`PROTO_INLINE_MAX_SIZE`, whitespace-only lines skipped. Decode errors now surface as Redis-exact
`-ERR Protocol error: …` (`connection.rs` uses `e.details()`). Inline TCL tests ported into
`protocol_tcl.rs` (6 new); `tcl_unbalanced_number_of_quotes` asserts the specific message. One
documented divergence: first bytes in `+ - : $` remain strict RESP (Redis treats any non-`*` as
inline; real commands never start with those). Replication apply + migration decoders unaffected
(separate strict decoders).

## 3. Cross-shard MGET returns hot expired-but-unpurged values (pre-existing) — FIXED

**Status:** fixed. `scatter_mget` and the shared `serialize_key_for_transport` producer (COPY/DUMP)
now read through `Store::get_with_expiry_check` instead of raw `Store::get`, matching the
single-shard GET/MGET path and lazily purging the key on read. `record_lookup_existence` picks up
the corrected existence verdict automatically, so a hot-expired key now counts as a miss. TOUCH/
EXISTS were already correct (`exists_unexpired`). Regression tests:
`shard::execution::scatter_effect_tests::scatter_mget_treats_hot_expired_key_as_miss`,
`scatter_copy_and_dump_of_hot_expired_key_report_missing`, and the store-level contract pin
`store::hashmap::tests::hot_expired_key_get_vs_get_with_expiry_check_contract`.

**Problem.** `scatter_mget` (`core/src/shard/execution.rs`, ~:767 region) reads via raw
`store.get()`, whose hot path (`store/hashmap.rs` `hot_value()`, ~:676) does not check
`is_expired()`. A hot key past its TTL but not yet purged is returned with its stale value (and
counted a keyspace hit) instead of nil. Single-key GET path handles expiry; cross-shard MGET
diverges. Pre-existing; round 7 preserved it verbatim (62-D kept `get()` deliberately to avoid a
silent behavior change inside a refactor).

**Fix direction.** Route the scatter read through an expiry-aware accessor (mirror whatever the
single-shard MGET/GET path uses — verify `exists_unexpired`/lazy-expire-on-read semantics), keeping
hit/miss accounting consistent with proposal 24's "existence == hit" rule. Add a regression test:
set key with short TTL, keep it hot, cross-shard MGET after expiry → nil + miss. Check TOUCH/EXISTS
scatter arms for the same hazard (they use `exists_unexpired` — already correct).

## 4. ACL requires Write on STORE-command source keys (pre-existing, Redis divergence)

**Problem.** Permission checks derive a single `KeyAccessType` from **command-level** flags and
apply it to *all* keys (`connection/permission_guard.rs` + `key_access_type_for_flags` consumers in
`guards.rs`/`routing.rs`): SINTERSTORE/ZUNIONSTORE/GEOSEARCHSTORE/etc. demand Write on read-only
source keys. Redis checks per-key access (sources need only read). Round 7's c797db26 fixed the
per-key *introspection* (`COMMAND GETKEYSANDFLAGS` now reports `Positional([OW, R])`) but not
enforcement — the per-key `KeyAccessFlag` data now exists and is accurate, making the enforcement
fix mechanical.

**Fix direction.** Permission guard consumes `keys_with_flags()` instead of
`keys()` × command-flag, mapping RW/OW/W → Write-required, R → Read-required. Cross-check every
`AccessSpec` for accuracy first (c797db26 audited the 9 migrated store commands; the rest of the
catalog was never audited for per-key accuracy because nothing consumed it). ACL DRYRUN + denial
log must agree. Tests: user with read-only ACL on source + write on dest can SINTERSTORE (Redis
parity), and the inverse still denied.

**Status — phase C (enforcement flip) DONE.** Enforcement now consumes per-key
`keys_with_flags()`:

- New `required_access_for_key_flags(flags, fallback)` helper
  (`connection/util.rs`) maps a key's `Vec<KeyAccessFlag>` → `KeyAccessType`
  (R→Read, W/OW→Write, RW or R+W→ReadWrite; empty vec falls back to the
  command-level derivation — unreachable today since every resolved key carries
  exactly one flag).
- New `PermissionGuard::check_keys_with_flags` replaces the uniform `check_keys`
  at both live enforcement seams — `route_and_execute` (`routing.rs`) and the
  MULTI queue (`guards.rs`) — and `ACL DRYRUN` (`acl_conn_command.rs`) shares the
  same helper, so DRYRUN verdicts and live denials agree by construction. The
  denial log entry + `NOPERM` reply shape are unchanged (single `check_single_key`
  construction site). `key_access_type_for_flags` is retained only as the
  empty-flags fallback.
- Tests: `permission_guard` unit (split-access allow, per-key deny), `util` helper
  table, a core `keys_with_flags`↔`keys()` order-parity assertion, and
  `acl_v2_regression` integration (SINTERSTORE + ZUNIONSTORE split-access allow,
  per-key deny, DRYRUN agreement, MULTI/EXEC queue, single-key no-regression).

**Phase B (spec-accuracy audit) DONE.** The full read-only audit of the catalog
against Redis fine key-spec flags is at `todo/proposals/acl-accessspec-audit.md`
(**37 SEVERITY-BYPASS**, **2 SEVERITY-DENIAL**, **1 MIXED** XREADGROUP, plus 2
FrogDB-native merge mismatches). All fixed:

- Root cause resolved by a new `AccessSpec::UniformRW` variant (uniform `RW` in a
  single-token spec change), applied to the 32 single/multi-key read-modify-write
  commands (INCR/DECR/pop/GETDEL/GETSET/GETEX/HINCRBY/ZINCRBY/… families, MIGRATE,
  EVAL/EVALSHA/FCALL).
- SET/BITFIELD `VARIABLE_FLAGS` implemented via `AccessSpec::Dynamic` +
  `dynamic_keys_with_flags` (plain SET stays `OW`; `SET … GET` / BITFIELD `GET`
  sub-ops add read).
- LMOVE/RPOPLPUSH/BLMOVE/BRPOPLPUSH/SMOVE → `Positional(RW, W)`; PFMERGE →
  `Positional(RW, R)`; CMS.MERGE → `Positional(OW, R)`; TDIGEST.MERGE →
  `Positional(RW, R)`.
- **XREADGROUP → `RW` (documented divergence, not Redis-parity `R`):** the stream
  key must stay in the WAL write-set (`WalStrategy::Dynamic` derives destinations
  from `write_access_keys`; the PEL mutation is a real write). `RW` closes the
  write-only bypass and keeps WAL persistence intact; FrogDB requires write where
  Redis requires read-only (stricter, not a bypass). See the audit doc header.
- Tests: `introspection2_tcl::tcl_command_getkeysandflags_acl_audit_pins` (spec
  pins) + `acl_v2_regression` enforcement tests (INCR needs read; SET…GET needs
  read; LMOVE dest write-only; XREADGROUP needs RW).

WAL note: every `OW`/`RW`→`R` demotion was checked against `write_access_keys`.
PFMERGE / CMS.MERGE / TDIGEST.MERGE sources moved to `R`, but those commands use
`WalStrategy::PersistFirstKey` (persists the dest only), and sources were never
written — so dropping them from the write-set is correct. LMOVE-family use
`WalStrategy::MoveKeys` (positional args, ignores access flags). Only XREADGROUP
consumes `Dynamic`→`write_access_keys`, hence the `RW` choice above.

## 5. Smaller carried-over items

- **Proposal 54 follow-up — DONE (2026-07-19):** flush-confirm now threaded through the
  `WalTarget` seam: trait gained `wal_sequence()` (`None` = no WAL, preserving the early-return)
  and `flush_through(after_seq)`; `persist` body extracted to a seam-generic `persist_records`
  free fn (byte-identical behavior); `TestTarget` gained independent flush-failure injection.
  Five new unit tests in `shard/persistence.rs` (snapshot-then-flush-once, FireAndForget
  never-flushes/log-and-continue, flush-error propagation, write-error aborts before flush,
  no-WAL short-circuit). frogdb-core persistence suite 89/89 green; proposal 54 deviation note
  updated.
- **Proposal 56 follow-up — DONE (2026-07-20):** combined-SHA256 ownership moved into
  `fullsync.rs` beside the codec as a `CheckpointChecksum` accumulator (`new` / `update_file` /
  `finalize`). It owns the coverage — `SHA256( name_0 || hash_0 || name_1 || hash_1 || … )`, UTF-8
  filename bytes then the raw 32-byte per-file SHA256, in wire order, no delimiters/metadata — so
  the sender and receiver can no longer drift on *what the checksum covers*. Both sides fold each
  file in as it is streamed/received (sender's second file-hash pass collapsed into the streaming
  loop; receiver's `file_checksums` Vec removed); no hand-rolled `Sha256::new()` combined hash
  remains in `replica_session.rs` or `connection.rs` (their `sha2` imports dropped). Wire bytes
  unchanged — the 12 existing codec tests incl. golden-bytes stay green; two new tests
  (`test_checkpoint_checksum_agreement` end-to-end coverage drift guard,
  `test_checkpoint_checksum_tamper_detected` payload+filename tamper). Verified: `just check` /
  `just test` / `just lint frogdb-replication` clean; `integration_replication` full-sync subset
  green.
- **Flaky test — DONE (2026-07-20):** root-caused a family of load-dependent cluster flakes
  (`test_info_gate_active_after_finalize`, `test_frogdb_finalize_success`,
  `test_auto_failover_selects_most_caught_up_replica`), all in `integration_cluster.rs`.
  Reproduced under 4× parallel worker contention (`--retries 0`); solo runs stay 10/10 green.
  Two distinct test-synchronization gaps (no product bug):
  1. **Version-fake clobber (both finalize tests).** Each node self-registers its real
     address+version (`CARGO_PKG_VERSION` = `0.1.0`) via a one-shot Raft `AddNode` after startup;
     `AddNode` apply replaces the whole `NodeInfo`. `wait_for_cluster_convergence` returns before
     those entries commit (it only checks known-node count + slots, satisfied by the initial
     guessed-port adds). So `fake_all_node_versions("0.2.0")` could be clobbered back to `0.1.0`
     by a late self-registration, and FINALIZE rejected the cluster as mixed-version
     (`Error("...node <id> is at version 0.1.0 but finalization requires 0.2.0")`). Fix: wait for
     `wait_for_address_convergence` (holds only once every self-registration `AddNode` has applied,
     making the fake the last writer) before faking; then replaced the fixed 500ms
     post-FINALIZE sleep with a bounded deadline poll (10s / 20ms) on `active_version` — followers
     apply the replicated finalize entry asynchronously after the leader's OK.
  2. **Post-election read race (failover test).** `wait_for_new_leader` only guarantees a Raft
     leader exists; the failover state machine that flips `CLUSTER INFO` back to
     `cluster_state:ok` lands a beat later, so the single immediate read saw `cluster_state:fail`.
     Fix: bounded deadline poll (10s / 20ms) for `cluster_state == "ok"`.
  Files: `frogdb-server/crates/server/tests/integration_cluster.rs`. Verified 20+ consecutive
  green per test under load with `--retries 0`; whole `integration_cluster` file green.
- **HFE expiry bound — DONE (2026-07-19):** ported upstream's bound verified against redis/redis
  unstable source: `HFE_MAX_ABS_TIME_MSEC = EB_EXPIRE_TIME_MAX >> 2 = 0x3FFF_FFFF_FFFF`
  (70368744177663 ms). New `hfe_resolve_abs_ms(val, is_seconds, basetime, cmd)` helper in
  `commands/src/utils.rs` mirrors upstream `parseExpireTime` check order (seconds pre-scale
  check, then absolute-ms bound vs basetime = now for relative arms, 0 for `*AT` arms) with
  upstream-exact `invalid expire time in '<cmd>' command` errors; wired into
  `execute_hexpire_common` (all four H*EXPIRE* commands) and HGETEX/HSETEX EX/PX/EXAT/PXAT.
  SET-family bounds untouched. 9 new unit pins + boundary accept/reject pin,
  `hsetex_px_large_value_accepted` flipped to `_rejected`, new TCL
  `tcl_hexpire_family_above_bound_validation`. frogdb-commands 114/114,
  hash_field_expire TCL 39/39 green. Known remaining divergence (report-only): upstream allows
  `val == 0` through the parser and uses the name-less negative message for HGETEX/HSETEX;
  FrogDB keeps its existing pinned zero/negative shapes. Proposal 50 notes updated.
- **SMOVE WAL gap (found during ACL phase B, pre-existing) — VERIFIED + FIXED (2026-07-19):**
  confirmed real: `PersistFirstKey` WAL'd only the source, so the destination-set mutation was
  unconditionally lost on WAL-driven restart (recovery reads only RocksDB/WAL CFs; no command-log
  fallback). Live replication was unaffected (command-level broadcast re-executes SMOVE on
  replicas), but every node had the same local crash exposure. Fix: spec flipped to
  `WalStrategy::MoveKeys` (`PersistOrDelete(source)` + `Persist(dest)`, identical shape to
  RPOPLPUSH). Regression tests `test_smove_destination_survives_restart` (also pins
  emptied-source deletion surviving restart) and
  `test_smove_destination_survives_restart_with_existing_members` in
  `integration_persistence.rs`; 6/6 SMOVE server tests green. No spec-pin tests asserted the old
  strategy.
- **`scan_for_oversized_bulk` O(n) rescan — MEASURED + FIXED (2026-07-20):** the 62-B note flagged
  that the codec rescanned the *whole* read buffer on every `*`-prefixed decode. Measured it (release
  micro-bench over the real `FrogDbResp2::decode`, baseline = upstream `Resp2` decoder alone):
  - _Realistic pipeline_ (N fixed 51-byte `SET` frames in one buffer, decoded to drain): baseline is
    linear and fast (~0.3 µs/frame → 32k frames in 3.7 ms). With the old scan, decode was
    **quadratic** — 4k frames: 1.3 ms → **1568 ms** (~1200×); 32k frames: 3.7 ms → **13 736 ms**
    (~3600×). Scan was >90% of decode time.
  - _Adversarial drip_ (4 MB legal bulk fed in K chunks): O(payload·K) quadratic — K=1024:
    **4550 ms** of pure prefix rescans.
  Verdict: not negligible — dominant and quadratic at plausible sizes → optimized. Replaced the flat
  byte-scan with a **bounded, structural scan** of only the first frame's element headers, skipping
  each bulk payload by its declared length (mirrors Redis `processMultibulkBuffer`). Now linear:
  pipeline 4k frames **1568 → 0.54 ms** (~2900×), 32k **13 736 → 4.27 ms** (~3200×), tracking the
  baseline within ~20%; drip K=1024 **4550 → 0.16 ms** (~28 000×). All existing codec table tests
  pass unchanged (observable decode contract preserved); added `scan_bounded_structural` plus
  end-to-end tests for the two beneficial divergences the structural scan introduces (an oversized
  bulk in a *later* pipelined frame is now rejected on its own frame instead of discarding a
  preceding good frame; binary bulk *data* containing the byte sequence `$<huge>\r\n` is no longer
  false-rejected — Redis accepts it). Known residual (out of original scope, bandwidth-bounded): a
  single maximal multibulk drip-fed one element-header per packet still re-walks the current
  incomplete frame's headers; a resume-offset would close it but adds cross-call state through every
  `split_to` site — deferred unless profiling flags it.
  Files: `frogdb-server/crates/server/src/connection/codec.rs`.

## Verification debt — CLEARED 2026-07-17

Full workspace suite run at `39ad07f2` (all four follow-up fixes merged): 6329 tests,
6153 passed, 1 skipped, 0 failures. 176 "timeouts" were environmental (macOS `syspolicyd`
exec-validation wedge freezing trivial unit tests at the 15s nextest cap); all 176 reran green in
targeted crate runs (frogdb-commands 105/105, frogctl 93/93, frogdb-acl 90/90,
frogdb-config 94/94). Machine-level mitigation for the wedge: add the terminal to the
Developer Tools Gatekeeper exemption, or `sudo pkill syspolicyd` when wedged (signature:
fresh binaries hang at `_dyld_start`, syspolicyd CPU-bound, amfid freshly respawned).

## Verification debt (historical note)

Post-review-fix full workspace suite (`0324e2df`) did not complete locally — two runs wedged behind
the macOS `syspolicyd` exec-validation queue and were stopped. Targeted coverage is green
(protocol 63, dispatch, frame_io, replication 131, lag 12, integration_transactions 26, plus the
pre-fix full suite 6291/6291 at `ac585330`). Run one full suite (CI, or locally after
`sudo pkill syspolicyd`) before merging this branch to main.
