# 50 — Option-grammar seam: delete the dead twin cluster, deepen the live helpers

**Status:** implemented (2026-07-16, `1224b22e`, `b16aad42`, `fb4f713d`, `e4b8d7e1`)
**Severity:** Moderate (duplication + dead code; error-message drift risk, not a live bug)
**Found:** round-6 deepening review fan-out over `frogdb-types::args` and command parsing.

## Problem

Three related shallow spots in option-grammar parsing:

1. **Dead twin cluster in the types crate.** `frogdb-server/crates/types/src/args.rs:339-505`
   holds `ScanOptions`, `ExpiryOption`, `SetCondition` (a twin of the *live*
   `SetCondition` at `types/src/types/mod.rs:531`), and `CompareCondition` — all with zero
   non-test callers (grep-verified). The live NxXx/GtLt/Scan/Limit helpers commands actually use
   live in `frogdb-server/crates/commands/src/utils.rs`. The dead cluster is a divergence trap:
   it looks canonical, is re-exported from `types/src/lib.rs:29`, and silently drifts from the
   real grammar.
2. **Expiry grammar hand-rolled 5-6×.** EX/PX/EXAT/PXAT (+ KEEPTTL/PERSIST where legal), the
   strictly-positive check, the `secs*1000` conversion, and the
   `invalid expire time in '<cmd>' command` message are re-implemented in:
   `commands/src/basic.rs:503-565` (SET), `string.rs:69-155` (SETEX/PSETEX),
   `string.rs:439-530` (GETEX), `string.rs:1451+` (MSETEX, index-based), and
   `hash.rs:1552-1601` (HGETEX/HSETEX). Each copy re-decides overflow and zero/negative
   handling — the exact class of divergence that produced past wire-parity bugs.
3. **Named-flag value boilerplate 26×.** `bloom.rs`/`cuckoo.rs`/`cms.rs`/`topk.rs` hand-roll
   "`CAPACITY requires a value`" / "`invalid CAPACITY`"-style blocks (26 sites) that differ only
   in the flag name and target type.

## Design

Deep module lives in the **commands crate** (sole consumer; `utils.rs` already holds the live
helpers). `frogdb-types::args` keeps only the `ArgParser` primitives.

**Phase 1 — delete the dead cluster.** Remove `args.rs:339-505` + the `lib.rs:29` re-exports.
Zero callers re-verified by grep at implementation time.

**Phase 2 — one validating expiry-grammar helper.** E.g.
`Expiry::parse_from(parser, cmd_name)` owning EX/PX/EXAT/PXAT + KEEPTTL/PERSIST acceptance,
mutual-exclusion, the strictly-positive check, `secs*1000` overflow, and the
`invalid expire time in '<cmd>' command` message. Migrate the 5-6 copies above. Commands opt in
to which variants are legal (SETEX takes a bare positional; GETEX allows PERSIST; MSETEX is
index-based — the helper must expose a parser-agnostic core for it).

**Phase 3 — named-flag helper.** E.g. `parser.flag_value_named::<T>(b"CAPACITY")` deriving
"`CAPACITY requires a value`" / "`invalid CAPACITY`" errors from the flag name; collapse the 26
blocks in the four probabilistic files.

**Wire compatibility:** error strings must stay byte-identical per command — pin the current
messages with tests **before** migrating each site.

## Tests

- Differential edge tests across all migrated commands: `EX 0`, negative, `secs*1000` overflow,
  `EXAT 0`, KEEPTTL/PERSIST conflicts.
- Error-message pinning tests per command (byte-identical before/after).
- Grep gates: no remaining hand-rolled `requires a value` blocks in
  `bloom.rs`/`cuckoo.rs`/`cms.rs`/`topk.rs`.

## Verify

`just test frogdb-commands` per phase + full `just test` at the end. Commit per phase.

## Implementation notes (2026-07-16)

- Phase 1 (`1224b22e`): dead cluster deleted (zero non-test callers re-verified). Correction to
  the survey: `SetCondition` was never re-exported from `lib.rs` — only
  `CompareCondition, ExpiryOption, ScanOptions` were; the live `SetCondition`
  (`types/src/types/mod.rs:531`) is untouched.
- Phase 2 (`b16aad42`): `commands/src/utils.rs` gains `checked_expire_value(raw, guard_overflow,
  ExpiryErr)` (parser-agnostic validation core), `ExpiryErr` (Named `'<cmd>'` vs the hash
  family's name-less message shape), and `ExpiryUnit` (flag→`Expiry` mapping). All six sites
  migrated (SET, SETEX/PSETEX, GETEX, MSETEX, HGETEX/HSETEX); MSETEX's four expiry arms collapse
  to one. 36 pin/edge tests written red-first against unmodified code.
- Phase 3 (`fb4f713d`): `flag_value_named(parser, flag, invalid_msg)` collapses the hand-rolled
  value blocks. Correction: the real count was **8** blocks (bloom BF.RESERVE/BF.INSERT, cuckoo
  CF.RESERVE/CF.INSERT) — the "~26" was an overcount; cms.rs/topk.rs are positional and had
  none. The BF/CF "invalid …" messages are bespoke phrases ("Invalid capacity", "Invalid bucket
  size", …) so the helper derives only the "`<flag>` requires a value" half from the token and
  takes the invalid-message explicitly. Grep gate clean.
- Parity fix (`e4b8d7e1`, follow-up found during phase 2): SETEX, SET EXAT, GETEX EXAT,
  MSETEX EX/EXAT, HGETEX/HSETEX EX/EXAT now guard the secs×1000 overflow, verified per-arm
  against Redis unstable (`t_string.c getExpireMillisecondsOrReply` — the guard is seconds-unit
  only; `t_hash.c parseExpireTime`). PSETEX and all PX/PXAT arms stay unguarded, matching
  upstream. Noted divergence left open: upstream hash-field expiry uses the tighter
  `HFE_MAX_ABS_TIME_MSEC/1000` bound; FrogDB uses `i64::MAX/1000`.
- `just test frogdb-commands`: 105 passed. `just check` + `just lint frogdb-commands`: clean.
