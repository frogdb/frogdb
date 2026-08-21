# 44 — Raw `Store::get`/`contains` bypass lazy expiry: BLPOP WRONGTYPE, COPY resurrection, seven create-guard refusals

Status: needs-triage

## What to build

`Store::get` (`frogdb-server/crates/core/src/store/hashmap.rs:934-943`) and `Store::contains`
(`store/mod.rs:411`, `hashmap.rs:958-960`) perform **no expiry check**. The crate says so in its own
standing unit test — `hot_expired_key_get_vs_get_with_expiry_check_contract`
(`hashmap.rs:2104-2140`) pins the bypass as intended and its comment reads *"Callers that need
Redis's 'expired reads as absent' semantics … must use `get_with_expiry_check`, never raw `get`"*.
There is also **no pre-dispatch purge**: `shard/execution.rs:191` calls the non-mutating
`exists_unexpired` (`store/mod.rs:421-423`, `&self`), increments `keyspace_misses` and fires the
`keymiss` notification, then hands the handler at `:241` a store that **still contains the key**.
Nothing between `:191` and `:241` reaps. So a key past its deadline but not yet swept by the 100 ms
index-driven cycle (`shard/event_loop.rs:24`) is visible to any handler that reads through the raw
doors. This issue collects the three wire-visible families that proposal 97's hotfixes H1/H2/H3
explicitly **do not** close. All three are LIVE on main today (verified by reading at HEAD;
reproducible with `DEBUG SET-ACTIVE-EXPIRE 0`, which 30+ existing tests already use).

**(A) B1 — blocking commands answer `-WRONGTYPE` where Redis blocks.** All eight blocking handlers
share one shape: `if let Some(value) = ctx.store.get(key) { if value.key_type() != KeyType::List {
return Err(CommandError::WrongType) } … ctx.store.get_mut(key) … }`. Raw `get` sees the corpse and
type-checks against it; `get_mut` **does** purge (`hashmap.rs:1298-1301`), so the handler holds two
mutually inconsistent views of one key inside one `if` block. Current sites (line numbers drifted
~1-2 from the proposal's, re-derived at HEAD): `commands/src/blocking.rs:67` (BLPOP), `:150`
(BRPOP), `:234` (BLMOVE, probes `source`), `:391` (BLMPOP), `:491` (BZPOPMIN), `:576` (BZPOPMAX),
`:704` (BZMPOP), `:806` (BRPOPLPUSH). Repro: `SET k somestring; PEXPIRE k 50; …; BLPOP k 0` →
`-WRONGTYPE` immediately. Redis: the key is gone, so `BLPOP` blocks. Worse, `blocking.rs:67-70`
returns **before** anything touches `get_mut`, so no number of `BLPOP`s ever reaps the key — only
the sweep will, and under `CLIENT PAUSE` (`event_loop.rs:232` returns early *after*
`set_expiry_suppressed(true)`) neither path runs. Proposal 97's H1 fixes B2 (the BLMOVE data-loss
twin) only; B1 needs the Class-B migration.

**(B) COPY resurrects an expired source into a fresh TTL-free key — permanently.**
`commands/src/generic.rs:598` reads the source through raw `get`, and `:592`
(`if !replace && ctx.store.contains(dest)`) probes the destination through the blinder `contains`.
Two defects, one command: an expired *source* is copied into a destination whose metadata is fresh,
so **the expired data outlives its deadline forever, in a new key, and reaches the WAL and replicas
as a legitimate write** (`Store::set`'s own doc, `hashmap.rs:945-948`, says a plain overwrite clears
any TTL); and an expired *destination* makes `COPY` a no-op returning `0` where Redis returns `1`.
`MSETNX` (`string.rs:918`) and the `MSET` NX/XX pair (`string.rs:1509`, `:1518`) are the same
`contains` shape. The crate's own worked fix is four lines away: `generic.rs:196` is
`let _ = ctx.store.get_with_expiry_check(new_key);` — a call kept purely for its purge side effect —
immediately above RENAME's `contains` at `:197`.

**(C) Seven "key already exists" create-guards refuse to re-create an expired sketch.**
`if ctx.store.get(key).is_some() { return Err(InvalidArgument { message: "Key already exists" }) }`
at `commands/src/bloom.rs:95`, `cuckoo.rs:84`, `topk.rs:103`, `tdigest.rs:96`, `cms.rs:41`,
`cms.rs:106`, and `timeseries.rs:134` (same shape, different message — `"TSDB: key already exists"`
— so a text-driven migration must key off the `get(key).is_some()` shape, not the string). Repro:
`BF.RESERVE bf 0.01 1000; PEXPIRE bf 50; …; EXISTS bf` → `:0` but `BF.RESERVE bf 0.01 1000` →
`-ERR Key already exists`. `EXISTS` and `BF.RESERVE` disagree about the same key, in the same
connection, one command apart; RedisBloom answers `+OK`. All seven sit in `CommandFlags::WRITE`
specs, so a purging probe is flag-legal here. The same shape sits at `json/basic.rs:79`
(`JSON.SET … NX`), `stream/basic.rs:92` (`XADD NOMKSTREAM`), `stream/consumer_groups.rs:110`
(`XGROUP CREATE MKSTREAM`), `geo.rs:417`, `timeseries.rs:1254`, `tdigest.rs:272`.

**Fix direction** (proposal 97 §(b)/§(c)): add `Store::exists_for_write` —
`!self.purge_if_expired(key) && self.contains(key)`, the missing `lookupKeyWrite` — and route (A)
onto `StoreTypedExt`'s `check_typed`/family wrappers (`store/typed.rs:114-284`, already
expiry-honoring and tested at `:484-543`) so the type probe and the mutation share one view, (B) and
(C) onto `exists_for_write`. Do **not** apply a blanket dispatch-level purge (proposal 97 R1: it
breaks the `&self` probe at `execution.rs:191` and misses keys outside the key spec — `SORT BY`,
`BITOP` sources, `ZUNION` operands, `COPY`'s source). Note the ordering constraint the proposal
records: these purges emit no WAL delete until proposal 83 lands, so a reaped key can diverge on
replicas — sequence behind 83 or accept and document.

## Acceptance criteria

- [ ] `BLPOP`/`BRPOP`/`BLMOVE`/`BLMPOP`/`BZPOPMIN`/`BZPOPMAX`/`BZMPOP`/`BRPOPLPUSH` against a
      past-deadline, unswept key of the *wrong* type block (or time out) instead of returning
      `-WRONGTYPE`, and the key is observably gone afterwards
- [ ] `COPY src dst` with a past-deadline `src` returns `0` and creates nothing; `COPY src dst` with
      a past-deadline `dst` and no `REPLACE` returns `1` and overwrites. `MSETNX` and `MSET NX/XX`
      likewise treat a past-deadline key as absent
- [ ] `BF.RESERVE` / `CF.RESERVE` / `TOPK.RESERVE` / `TDIGEST.CREATE` / `CMS.INITBYDIM` /
      `CMS.INITBYPROB` / `TS.CREATE` succeed against a past-deadline key of their own name, i.e.
      `EXISTS k` and the create guard never disagree
- [ ] Regression test `blocking_wrongtype_on_expired_key_blocks_instead_of_erroring` (level 4, a
      live server — `frogdb-commands` has no blocking harness), driven with
      `DEBUG SET-ACTIVE-EXPIRE 0` + `PEXPIRE`, table-driven over all eight commands
- [ ] Regression test `copy_from_expired_source_does_not_resurrect_the_value` asserting `EXISTS dst`
      is `0` **and** the WAL/replica stream carries no copy
- [ ] Regression test `create_guards_treat_a_past_deadline_key_as_absent`, table-driven over the
      seven create sites (needs `cmd-full`; run the `core-profile` families in the same pass)
- [ ] `just test frogdb-server expired` green; `just test frogdb-commands copy` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 97 (`.scratch/arch-deepening/proposals/97-typed-store-access.md`),
defect B1 plus the "COPY resurrect" and "seven exists-refusals" items — the three the proposal's own
hotfixes H1/H2/H3 explicitly leave open.

## Comments
