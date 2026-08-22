# COMMAND INFO ACL categories: 55 commands report none, 31 report a different set

Status: done

## Origin

Wave-D2 acceptance run (`.scratch/redis-feel/compare-command-metadata.py`,
FrogDB `cmd-full` vs Redis 8.6.1): after the category *ordering* fix landed,
zero commands disagree on order alone — every remaining `acl_categories`
difference is a difference in the set itself. 86 of 269 shared commands are
affected.

## What is wrong

`command_meta::command_info_categories` sorts whatever
`frogdb_acl::CommandCategory::all_for_command` returns, so the reply is only as
good as that table. Two failure shapes:

**55 commands get an empty array** — the table has no row for them at all, so
`COMMAND INFO` reports no categories and, worse, ACL rules like `+@read` or
`-@dangerous` do not cover them:

```
ASKING BZMPOP CLUSTER DELEX DIGEST EVALSHA_RO EVAL_RO FCALL FCALL_RO FUNCTION
GEORADIUSBYMEMBER_RO GEORADIUS_RO HEXPIRE HEXPIREAT HEXPIRETIME HGETDEL HGETEX
HOTKEYS HPERSIST HPEXPIRE HPEXPIREAT HPEXPIRETIME HPTTL HSETEX HTTL LATENCY LCS
LOLWUT MIGRATE MONITOR MOVE MSETEX PFDEBUG PFSELFTEST PSYNC READONLY READWRITE
REPLCONF RPOPLPUSH SUBSTR SYNC WAIT WAITAOF XACKDEL XDELEX ZDIFF ZDIFFSTORE
ZINTER ZINTERCARD ZMPOP ZRANGESTORE ZREMRANGEBYLEX ZREMRANGEBYRANK
ZREMRANGEBYSCORE ZUNION
```

Note the security-relevant ones: `MIGRATE`, `MONITOR`, `PSYNC`, `SYNC`,
`REPLCONF`, `FUNCTION`, `FCALL` — upstream puts several of these in
`@dangerous` / `@admin`, so an ACL that revokes `@dangerous` today still lets
them through.

**31 commands report a different set.** Three sub-shapes:

1. `@dangerous` is missing wholesale — `BGREWRITEAOF`, `BGSAVE`, `SAVE`,
   `RESTORE`, `ROLE`, `LASTSAVE`, `INFO`, `SORT`, `SORT_RO`.
2. `@fast` vs `@slow` disagreements, i.e. the table contradicts the
   command's own `CommandFlags::FAST`: `BITFIELD_RO`, `BZPOPMIN`, `BZPOPMAX`,
   `RENAMENX`, `SWAPDB`, `ZADD`, `ZCOUNT`, `ZLEXCOUNT`.
3. `@blocking` missing on `XREAD` / `XREADGROUP` (both really do block).

The container commands in that list (`ACL`, `CLIENT`, `CONFIG`, `MEMORY`,
`MODULE`, `OBJECT`, `PUBSUB`, `SCRIPT`, `SLOWLOG`, `XGROUP`, `XINFO`) are a
*deliberate* divergence, not a bug — see issue 15; they are listed here only so
the count reconciles.

## Why it matters

This is not cosmetic like the ordering was. `COMMAND INFO`'s category array and
`ACL SETUSER +@category` read the same table, so every gap above is
simultaneously a wrong `COMMAND INFO` reply and an ACL rule that silently fails
to cover a command.

## Candidate direction

The vendored snapshot already carries upstream's `acl_categories` per command
(`website/src/data/redis-commands-8x.json`), so this can get the same treatment
the flag parity check got in D2: a join test asserting our table agrees with
upstream's, with a named exemption list for the container class and for any
category FrogDB deliberately assigns differently (per ADR-0005 the table must
describe what FrogDB's ACL engine actually enforces, so `@fast`/`@slow` should
be reconciled against `CommandFlags::FAST` rather than copied).

Ordering is already correct and covered by `command_meta::ACL_CATEGORY_ORDER`.

## Ruling (2026-08-21)

**Vendor as source + parity gate.** The hand table
(`frogdb_acl::CommandCategory::all_for_command`) stays authoritative for live
enforcement; vendored upstream data keeps it honest via a permanent test.

1. Extend the vendor pipeline to keep upstream's `acl_categories` field:
   `website/scripts/vendor-redis-commands.py` (trim step) plus
   `scripts/gen-command-metadata.py` →
   `frogdb-server/crates/commands/src/upstream/generated.rs`. Issue 15's nested
   subcommand rows keep `acl_categories` too where upstream provides them.
   `just command-metadata-gen-check` stays green.
2. Fix the 86 disagreements: the 55 commands with no table row (MIGRATE,
   MONITOR, PSYNC, SYNC, REPLCONF, FUNCTION, FCALL must land in
   `@dangerous`/`@admin` per upstream) and the 31 with a wrong set (missing
   `@dangerous`; `@fast` vs `@slow`; `@blocking` on XREAD/XREADGROUP). FrogDB-only
   commands with no upstream row get deliberate categories chosen by analogy to
   their nearest upstream sibling, documented at the table.
3. Permanent parity gate in
   `frogdb-server/crates/server/src/server/upstream_metadata_tests.rs`,
   following the D2 flag-parity pattern: vendored `acl_categories` against
   `all_for_command` for every joined command, justified-divergence allowlist,
   shrink-only, each entry documented with why. Subcommand-level categories are
   covered only if ACL enforcement distinguishes subcommands; if enforcement is
   container-level only, compare container rows and record the gap in the
   Resolution rather than inventing enforcement.
4. `COMMAND INFO`'s `acl_categories` output must flow from the fixed table
   (`commands/src/command_meta.rs`, `command_info_categories`).

## Resolution

Implemented as ruled.

**Vendor pipeline.** `acl_categories` is now projected by
`website/scripts/vendor-redis-commands.py` (`CORE_FIELDS`, and `MODULE_FIELDS`
for symmetry — upstream module `commands.json` files have never carried the
field, see the residue note below) and emitted by
`scripts/gen-command-metadata.py` into
`frogdb-server/crates/commands/src/upstream/generated.rs`. 225 of the 263 core
rows carry a non-empty set, nested subcommand rows included.
`just command-metadata-gen-check` is green.

**Table.** `frogdb-server/crates/acl/src/categories/data.rs` went from 202 to
261 rows, none removed: **59 added**, **24 changed**.

| sub-shape | count | commands |
| --- | --- | --- |
| no row → new row | 59 | MIGRATE, MONITOR, PSYNC, SYNC, REPLCONF, FUNCTION, FCALL, FCALL_RO, … |
| gained `@dangerous` | 13 | acl, bgrewriteaof, bgsave, client, info, lastsave, memory, restore, role, save, slowlog, sort, sort_ro |
| `@fast`/`@slow` corrected | 8 | bitfield_ro, bzpopmax, bzpopmin, renamenx, swapdb, zadd, zcount, zlexcount |
| gained `@blocking` | 2 | xread, xreadgroup |
| dropped a wrong category | 1 | time (bogus `@admin`) |

FrogDB-only commands (DELEX, MSETEX, HOTKEYS, DIGEST, …) get categories by
analogy to their nearest upstream sibling, each documented inline at its row.
Three table-invariant unit tests were added in the same module: sorted+unique,
lowercase, and `@fast` xor `@slow` for every row.

**Parity gate.** `vendored_acl_categories_agree_with_our_table` in
`frogdb-server/crates/server/src/server/upstream_metadata_tests.rs` follows the
D2 flag-parity pattern. Upstream's `acl_categories` is only the *explicit* half
of what redis reports, so the expectation re-derives the implicit half from
FrogDB's own wire flags exactly as redis' `setImplicitACLCategories` does
(`write`→`@write`; `readonly` && !`@scripting`→`@read`; `admin`→`@admin`+`@dangerous`;
`pubsub`→`@pubsub`; `fast`→`@fast`; `blocking`→`@blocking`; anything not `@fast`→`@slow`),
and compares against `command_meta::command_info_categories` — the emitter, so
the gate checks the reply clients actually see rather than an inner helper.
Container rows are expected to be the union over their upstream subcommand rows,
plus `@admin`/`@dangerous` for the split-admin surfaces.

**Divergence allowlist: empty.** `ACL_CATEGORY_DIVERGENCES` is `&[]`. Every
joined core-Redis command agrees. The companion
`fast_and_slow_categories_follow_the_fast_flag` test excludes containers from
its flag-agreement half only, because a container's row answers "is any
subcommand fast" while its own spec flag answers for the dispatch as a whole.

### Recorded gaps

**Granularity: ACL categories are enforced per *container*, not per
subcommand.** `frogdb-server/crates/acl/src/permissions.rs:215-264`
(`is_command_allowed`) resolves categories via `all_for_command(&cmd_lower)` on
the container name; subcommand-level rules exist only as explicit
`+cmd|subcmd` entries. Per the ruling, no enforcement was invented: the gate
compares container rows (as the union over upstream's subcommand rows) and the
gap is recorded here. Consequence: `-@admin` denies e.g. `CLIENT SETNAME`
because `client`'s row carries the union, matching upstream's own
container-level behaviour for split-admin surfaces.

**Module families remain uncategorised.** `acl_category_gap_allowlist()` in
`frogdb-server/crates/server/src/server/register.rs` shrank from 80
always-compiled entries to 27, all `FT.*`, plus the feature-gated
`cmd-json`/`cmd-timeseries`/`cmd-bloom`/`cmd-cuckoo`/`cmd-cms`/`cmd-topk`/
`cmd-tdigest`/`cmd-vectorset`/`cmd-event-sourcing` blocks. The `cmd-geo`,
`cmd-hyperloglog` and `cmd-stream` blocks are gone — those are core Redis and
are now covered. The residue is principled rather than lazy: module
`commands.json` files publish no `acl_categories` at all (modules declare them
in C, at `RedisModule_SetCommandACLCategories` time), so there is no upstream
evidence to vendor. Closing it needs a separate decision about what categories
FrogDB assigns to module surfaces.

### Verification

- `just test frogdb-acl` — 97/97
- `just test frogdb-commands` — 128/128
- `just test frogdb-server 'upstream_metadata|acl_category|fast_and_slow|test_acl_dangerous|test_acl_command_denied'`
- `just command-metadata-gen-check`, `just lint-gates`
- New integration coverage:
  `test_acl_dangerous_covers_replication_and_migration_verbs` in
  `frogdb-server/crates/server/tests/integration_acl.rs` asserts a
  `-@dangerous` user gets NOPERM for MIGRATE, MONITOR, PSYNC, SYNC, REPLCONF
  and RESTORE while plain SET still succeeds — commands that had no table row
  at all before this change, so every `-@dangerous` rule was a silent no-op
  against them.
