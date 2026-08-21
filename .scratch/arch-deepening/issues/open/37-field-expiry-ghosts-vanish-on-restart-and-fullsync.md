# 37 — Hash-field TTL ghosts are not durable: HTTL answers differently before/after restart and master vs fullsync-built replica

Status: needs-triage

## What to build

Hash field TTLs are stored twice — value-side in `HashValue.field_expiries` and store-side in
`HashMapStore::field_expiry_index` — and **only the value book is ever serialized**.
`encode_hash` / `encode_hash_with_field_expiry` dispatch on `hash.has_field_expiries()`
(`frogdb-server/crates/persistence/src/serialization/registry.rs:85`, `:90-93`), so the index
never reaches the WAL, an RDB, or a `DUMP` payload; `WalStrategy::PersistFirstKey` stages
`WalAction::Persist(key)` — the whole value — so a field deadline reaches disk only through
the value book. On the way back in, restore routes through `replace_entry` → `install`, whose
`core/src/store/hashmap.rs:388-396` rebuilds the index from `hash.field_expiries()` after
`uninstall` has dropped the previous key's entries wholesale.

That re-derivation is normally a virtue, but combined with the LIVE ghost producers (HMSET
`commands/src/hash.rs:294`, plus the FT.SUG pair — see issue 36) it produces an **observable
non-durability of answers**. Sequence: `HSET h f v1` / `HEXPIRE h 100 FIELDS 1 f` /
`HMSET h f v2`. `HTTL h FIELDS 1 f` now answers `100` (read from the index, which still holds
the deadline the value dropped at `types/src/types/hash.rs:262`). Restart the server, issue no
command in between, and the same `HTTL` answers `-1`. A running process and its own restored
image disagree about a field that was never touched.

The same split appears across replication, in the direction that matters most. A replica built
by **command-stream** replication replays the same handlers and reproduces the ghost
deterministically, so it agrees with the master. A replica built by **fullsync** installs the
serialized value and derives a clean index, so it answers `HTTL` differently from its master
for the same field, indefinitely. Two replicas of one master can therefore disagree with each
other depending only on how each was seeded, and a failover flips the visible answer. Nothing
in the WAL or replication stream records the discrepancy, so no consistency checker can see
it. This is LIVE on main today wherever a ghost exists.

Fix direction: the divergence is a *symptom* — its root is the dual-book design and the
handlers that write only one book. The durability finding is what forces proposal 93's fold
direction (index becomes purely derived, value book becomes the single authority), after which
pre/post-restart and master/replica answers converge by construction with no WAL or
replication path changed. Until that lands, this issue owns the **observable**: a test matrix
that pins "HTTL survives a restart unchanged" and "a fullsync-built replica answers HTTL
identically to its master", so the ghost class cannot regress silently and so 93's exit
criterion has a witness.

## Acceptance criteria

- [ ] `HTTL` (and `HPERSIST`, `HEXPIRETIME`) return the same answer for an untouched field
      across a server restart, for every hash-mutating command path.
- [ ] A fullsync-built replica and its master return identical `HTTL` answers for the same
      field after any sequence of hash writes.
- [ ] Regression test `hash_field_ttl_answer_survives_restart` in
      `crates/redis-regression/tests/hash_field_expire_tcl.rs`: seeds the HMSET ghost
      sequence, asserts `HTTL`, restarts, asserts `HTTL` unchanged. Fails at HEAD (100 → -1).
- [ ] Regression test `hash_field_ttl_master_replica_fullsync_agree`: after the same sequence
      on the master, attach a fresh replica (forcing a fullsync) and assert both answer
      `HTTL` identically. Fails at HEAD.
- [ ] `just test frogdb-redis-regression hash_field_ttl` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 93
(`.scratch/arch-deepening/proposals/93-hash-field-expiry-store-api.md`), §Problem 6 "Crash and
replica behaviour — the divergence is not durable, which is its own problem" (explicitly
flagged for the orchestrator to file; durability/consistency, not security).

## Comments
