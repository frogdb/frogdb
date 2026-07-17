# Round-7 Follow-ups

Open items from the round-7 architecture review (proposals 53–62, implemented 2026-07-16) and its
adversarial post-implementation review. Each is independently actionable. Evidence verified against
the tree at `0324e2df`.

## 1. WAIT wake-up on replica resume-seed (review finding 57-A)

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

## 2. Inline (telnet-style) command support (62-E, decision)

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

- **Proposal 54 follow-up:** thread flush-confirm (`Durability::Confirm`'s `sequence` +
  `flush_through`) through the `WalTarget` seam instead of reading the concrete `RocksWalWriter`,
  so confirm-path failure injection becomes unit-testable (noted in proposal 54 implementation
  notes).
- **Proposal 56 follow-up:** move combined-SHA256 ownership into `CheckpointStreamCodec` so
  checksum coverage is part of the codec contract (open question in proposal 56).
- **Flaky test:** `integration_cluster::test_info_gate_active_after_finalize` passed 2/3 in the
  round-7 final gate (pre-existing flake). Root-cause or add retry-tolerant synchronization.
- **HFE expiry bound:** upstream Redis bounds hash-field TTL at `HFE_MAX_ABS_TIME_MSEC/1000`,
  tighter than ours (round-6 proposal 50 open item).
- **`scan_for_oversized_bulk` O(n) rescan:** measured-before-optimizing note from 62-B — the codec
  rescans the full buffer per `*`-prefixed decode; now pinned by table tests, so a bounded scan can
  be proven equivalence-preserving if profiling ever flags it.

## Verification debt

Post-review-fix full workspace suite (`0324e2df`) did not complete locally — two runs wedged behind
the macOS `syspolicyd` exec-validation queue and were stopped. Targeted coverage is green
(protocol 63, dispatch, frame_io, replication 131, lag 12, integration_transactions 26, plus the
pre-fix full suite 6291/6291 at `ac585330`). Run one full suite (CI, or locally after
`sudo pkill syspolicyd`) before merging this branch to main.
