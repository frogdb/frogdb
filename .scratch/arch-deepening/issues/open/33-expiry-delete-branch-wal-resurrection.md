# 33 — Expiry commands delete in memory but write no WAL record, so replay resurrects the key

Status: needs-triage

## What to build

All four key-family expiry commands declare `wal: WalStrategy::PersistFirstKey` —
`frogdb-server/crates/commands/src/expiry.rs:243` (`EXPIRE`), `:339` (`PEXPIRE`), `:427`
(`EXPIREAT`), `:511` (`PEXPIREAT`) — and all four can take a `ctx.store.delete(key)` branch. The
four hash-family specs do the same and delete the key when the hash empties. `PersistFirstKey` maps
to `WalAction::Persist(key)` (`core/src/command.rs:658-661`), which dispatches to
`WalTarget::write_set` (`core/src/shard/persistence.rs:108`), whose production implementation is a
**no-op when the key is absent**: `if let Some(wal) = self.persistence.wal_writer() && let
Some(value) = self.store.get_hot(key)` (`persistence.rs:143-153`). Contrast the commands that do
delete their own first key — `HDEL` at `hash.rs:199`, plus `list.rs`, `set.rs`, `blocking.rs` — all
of which use `WalStrategy::PersistOrDeleteFirstKey`, whose `contains` probe writes a tombstone
(`persistence.rs:116-122`). Read together: an expiry command that takes its delete branch removes
the key in memory and records nothing durable, so a WAL replay restores it.

The exposed set is wider than "keys with no prior TTL". A key that *had* a TTL is only caught on
recovery by `FM-PERSISTENCE-036` when the durable deadline has itself passed. The already-past
`EXPIREAT`/`PEXPIREAT` branches (`expiry.rs:472-475`, `:556-559`) fire on keys whose last durable
record carries a **still-future** deadline: `SET k v EX 3600` then `EXPIREAT k <yesterday>` deletes
in memory while the WAL still says "expires in an hour", so recovery has nothing to filter on and
the key comes back *with a live TTL* — quieter than the no-TTL case, not louder. `EXPIRE k -10` /
`PEXPIRE k -10` (`:297-300`, `:386-389`) against a key with a live durable TTL are the same shape.
There is in-tree precedent for exactly this class: `SMOVE` declared `PersistFirstKey` while mutating
two keys, lost the destination on restart, and is now pinned by
`test_smove_destination_survives_restart` (`frogdb-server/crates/server/tests/integration_persistence.rs:495`,
doc comment above it) whose text names the same "`PersistFirstKey` does not cover my deletes" shape.
And the unit test currently **pins the opposite of production**: `persist_always_writes_set`
(`core/src/shard/persistence.rs:541-553` — the proposal cited `:539-553`; the `#[tokio::test]`
attribute is at `:540` and the `fn` at `:541`) asserts `Write::Set` for both a present and an absent
key against `TestTarget`, which never consults its `present` set on the `write_set` path. The test
therefore certifies a behavior the real target does not have, which is why the gap survived.

**This claim is a static reading and has not been reproduced by execution.** Confirming it is the
first step, not an implementation detail.

**Framing — this must not ride along with proposal 92's refactor.** `frogdb-persistence` is a
LOCKED core area (mutation gate 0.85, boundary ADR `adr/0003`), so this is **spec-first** work:
a new failure-mode row in `.scratch/hardening/specs/persistence-failure-modes.md` (the highest
existing row is `FM-PERSISTENCE-052`, so the next free number applies) → a failing forcing test
named by that row → the fix. Proposal 92 is a `frogdb-commands` decision-table refactor that
deliberately changes no write-effect behavior; folding a durability change into it would smuggle a
locked-area behavior change past the spec gate and past `just mutants-diff frogdb-persistence`.
File and schedule this separately, and land it before or after 92 but never inside it.

Fix direction: the four key-family specs and the four hash-family specs move to
`WalStrategy::PersistOrDeleteFirstKey`; a restart regression in `integration_persistence.rs`
modeled on the `SMOVE` test asserts the key stays gone across a restart for both the no-TTL and the
still-future-TTL cases; and `TestTarget::write_set` is tightened to model the `get_hot` miss so the
unit test stops disagreeing with production.

## Acceptance criteria

- [ ] `SET k v` (no TTL) → `EXPIRE k -1` → restart: `EXISTS k == 0`. Same for `SET k v EX 3600` →
      `EXPIREAT k <yesterday>` → restart, and for the hash-family equivalent that empties a hash.
      All fail today (static claim — confirm the failure first, then fix).
- [ ] A new `FM-PERSISTENCE-NNN` row (next free number after `052`) is added to
      `.scratch/hardening/specs/persistence-failure-modes.md` stating that a key deleted by an
      expiry command is never resurrected by WAL replay, with its `Forced by` column naming the new
      forcing tests.
- [ ] `just lint-failure-modes` green (spec↔test agreement: every row names its forcing tests,
      every tagged test matches a row).
- [ ] Regression test `test_expiry_delete_survives_restart` in
      `frogdb-server/crates/server/tests/integration_persistence.rs`, modeled on
      `test_smove_destination_survives_restart` (`:495`), covering the no-TTL and still-future-TTL
      cases; plus a forcing test inside `frogdb-persistence`/`frogdb-recovery` itself so the row
      contributes to the owning crate's mutation score.
- [ ] `persist_always_writes_set` (`core/src/shard/persistence.rs:541-553`) no longer asserts a
      `Write::Set` for an absent key against a `TestTarget` that cannot model the `get_hot` miss —
      the double is tightened and the assertion follows production.
- [ ] `just mutants-diff frogdb-persistence` run before push (push discipline for the locked area).
- [ ] `just test frogdb-server expiry_delete_survives_restart` green.

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 92 (`.scratch/arch-deepening/proposals/92-expiry-decision-table.md`),
§Adjacent finding — flagged, not claimed, not in scope (review amendment A8, widened at revision 2:
"FILE IT (locked crate, spec-first, not in 92)").

## Comments
