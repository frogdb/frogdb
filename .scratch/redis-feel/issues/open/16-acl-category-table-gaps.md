# COMMAND INFO ACL categories: 55 commands report none, 31 report a different set

Status: needs-triage

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
