# AccessSpec per-key ACL audit — FrogDB command catalog

> **Implemented — round-7 follow-up item 4, phase B.** All 37 BYPASS, 2 DENIAL, 1 MIXED,
> and the 2 FrogDB-native merge mismatches below are fixed. Key decisions:
> - **`AccessSpec::UniformRW`** was added (new enum variant) so a command declares uniform
>   `RW` with a single-token change from `Uniform` — used for the 32 single/multi-key
>   read-modify-write commands (INCR/DECR/pop/GETDEL/GETSET/GETEX/HINCRBY/ZINCRBY/… families,
>   MIGRATE, EVAL/EVALSHA/FCALL).
> - **SET and BITFIELD** are `VARIABLE_FLAGS` in Redis. FrogDB implements true variant
>   sensitivity via `AccessSpec::Dynamic` + a `dynamic_keys_with_flags` override: plain
>   `SET k v` stays `OW` (a `%W~`-only user can still SET), `SET … GET` → `RW`; BITFIELD
>   `GET`-only → `R`, write sub-ops → `OW`, mixed → `RW`.
> - **LMOVE/RPOPLPUSH** → `Positional(RW, W)` (dest INSERT-only); **BLMOVE/BRPOPLPUSH/SMOVE**
>   → `Positional(RW, W)`; **PFMERGE** → `Positional(RW, R)`.
> - **CMS.MERGE** → `Positional(OW, R)` (handler overwrites dest without reading it);
>   **TDIGEST.MERGE** → `Positional(RW, R)` (handler merges into the existing dest unless
>   `OVERRIDE`).
> - **XREADGROUP** → `UniformRW` (**not** `R` as the Redis-parity fix would suggest). This is a
>   documented divergence: XREADGROUP carries `WalStrategy::Dynamic`, which derives its WAL
>   write-set from `write_access_keys` (`keys_with_flags` filtered to `W`/`OW`/`RW`). The
>   consumer-group PEL/last-id mutation is a genuine WAL write, so demoting the stream key to
>   `R` would drop it from the WAL write-set and silently stop persisting/replicating the PEL.
>   `RW` keeps the stream in the write-set and closes the write-only bypass; the only remaining
>   divergence is that FrogDB additionally requires **write** perm where Redis requires read
>   only (strictly stricter, not a bypass). Fully resolving to Redis-parity `R` would require
>   splitting the ACL-required-access projection from the WAL-write-set projection — deferred.
>
> Pins: `redis-regression/tests/introspection2_tcl.rs::tcl_command_getkeysandflags_acl_audit_pins`
> and enforcement tests in `acl_v2_regression.rs` (`acl_incr_requires_read_and_write`,
> `acl_set_get_option_requires_read`, `acl_lmove_dest_write_only_allowed`,
> `acl_xreadgroup_requires_read_write`).

Read-only audit. Ground truth: Redis `unstable` `src/commands/*.json` `key_specs`.
Enforcement model under audit: per key, `R`→read perm, `W`/`OW`→write perm, `RW`→read+write.

## Method / correctness note

Redis's ACL key-permission check keys off the **fine** key-spec flags, not the coarse
`RO`/`RW`/`OW` category:

- `ACCESS` present  ⇒ requires **read** perm
- `INSERT`/`UPDATE`/`DELETE` present ⇒ requires **write** perm

The coarse `RW` is only a shorthand and is *misleading for ACL*: e.g. `LPUSH` is
`[RW, INSERT]` (coarse "RW") but has **no `ACCESS`**, so Redis requires only write perm —
FrogDB's `OW` is correct. Conversely `INCR` is `[RW, ACCESS, UPDATE]` → needs read+write.
This audit derives the required-perm set from the fine flags and falls back to the coarse
category only when no fine flag is present. `PFCOUNT`/`PFDEBUG` (`[RW, ACCESS]`, ACCESS-only)
therefore correctly map to read-only.

FrogDB resolution rules (`command_spec.rs:239-266`):
- `AccessSpec::Uniform`  → **every** key gets `OW` if the command has `CommandFlags::WRITE`, else `R`.
  (Structurally cannot express `RW` — this is the root cause of the whole BYPASS class below.)
- `AccessSpec::Positional(&[..])` → the listed flag per position (last repeats).
- `AccessSpec::Dynamic` → command's `dynamic_keys_with_flags`.

## Counts

- Commands with a `CommandSpec` (unique, non-test): **371** (301 key-bearing).
- Redis-comparable key-bearing commands audited: **191**.
- FrogDB-only key-bearing commands (no Redis OSS parity): **110** (audited by semantics).
- Verified correct (Redis-comparable): **148 + 3 dynamic** = **151**.
- **SEVERITY-BYPASS: 37**
- **SEVERITY-DENIAL: 2**
- **SEVERITY-MIXED: 1** (XREADGROUP — both a false denial and a bypass)
- Dynamic-access verified correct: 3 (GEORADIUS, GEORADIUSBYMEMBER, SORT — SORT has one minor gap)

Root cause of the BYPASS class: `AccessSpec::Uniform` collapses all write-command keys to
`OW` (write-only). Every read-modify-write command that actually **reads the value** (Redis
`ACCESS` flag) — pop/getdel/getset/incr/getex/setbit families — is thereby under-marked as
write-only. Once ACL enforcement flips on, a principal holding only `%W~key` (write, no read)
could run these and, for the pop/GET* variants, **read back the stored value**.

---

## SEVERITY-BYPASS (ours weaker → ACL bypass once enforced)

`our` = perm set FrogDB requires; `redis` = perm set Redis requires (from fine flags).
All are `our={w}` (or `{r}` for scripts) ⊊ Redis requirement.

| Command | Key(s) | Ours | Redis | Redis flags | Evidence |
|---|---|---|---|---|---|
| INCR | key | w | rw | RW,ACCESS,UPDATE | commands/src/string.rs:516 |
| INCRBY | key | w | rw | RW,ACCESS,UPDATE | commands/src/string.rs:608 |
| INCRBYFLOAT | key | w | rw | RW,ACCESS,UPDATE | commands/src/string.rs:706 |
| DECR | key | w | rw | RW,ACCESS,UPDATE | commands/src/string.rs:562 |
| DECRBY | key | w | rw | RW,ACCESS,UPDATE | commands/src/string.rs:655 |
| GETSET | key | w | rw | RW,ACCESS,UPDATE | commands/src/string.rs:1153 |
| GETDEL | key | w | rw | RW,ACCESS,DELETE | commands/src/string.rs:356 |
| GETEX | key | w | rw | RW,ACCESS,UPDATE | commands/src/string.rs:401 |
| SET | key | w | rw | RW,ACCESS,UPDATE,VARIABLE_FLAGS | commands/src/basic.rs:474 |
| SETBIT | key | w | rw | RW,ACCESS,UPDATE | commands/src/bitmap.rs:28 |
| BITFIELD | key | w | rw | RW,ACCESS,UPDATE,VARIABLE_FLAGS | commands/src/bitmap.rs:383 |
| HINCRBY | key | w | rw | RW,ACCESS,UPDATE | commands/src/hash.rs:554 |
| HINCRBYFLOAT | key | w | rw | RW,ACCESS,UPDATE | commands/src/hash.rs:592 |
| HGETEX | key | w | rw | RW,ACCESS,UPDATE | commands/src/hash.rs:1733 |
| HGETDEL | key | w | rw | RW,ACCESS,DELETE | commands/src/hash.rs:1652 |
| ZINCRBY | key | w | rw | RW,ACCESS,UPDATE | commands/src/sorted_set/basic.rs:363 |
| LPOP | key | w | rw | RW,ACCESS,DELETE | commands/src/list.rs:189 |
| RPOP | key | w | rw | RW,ACCESS,DELETE | commands/src/list.rs:269 |
| SPOP | key | w | rw | RW,ACCESS,DELETE | commands/src/set.rs:841 |
| ZPOPMIN | key | w | rw | RW,ACCESS,DELETE | commands/src/sorted_set/pop.rs:21 |
| ZPOPMAX | key | w | rw | RW,ACCESS,DELETE | commands/src/sorted_set/pop.rs:86 |
| LMPOP | keys (numkeys) | w | rw | RW,ACCESS,DELETE | commands/src/list.rs:936 |
| ZMPOP | keys (numkeys) | w | rw | RW,ACCESS,DELETE | commands/src/sorted_set/pop.rs:151 |
| BLPOP | keys (all-but-last) | w | rw | RW,ACCESS,DELETE | commands/src/blocking.rs:26 |
| BRPOP | keys (all-but-last) | w | rw | RW,ACCESS,DELETE | commands/src/blocking.rs:111 |
| BZPOPMIN | keys (all-but-last) | w | rw | RW,ACCESS,DELETE | commands/src/blocking.rs:445 |
| BZPOPMAX | keys (all-but-last) | w | rw | RW,ACCESS,DELETE | commands/src/blocking.rs:528 |
| BLMPOP | keys (numkeys) | w | rw | RW,ACCESS,DELETE | commands/src/blocking.rs:309 |
| BZMPOP | keys (numkeys) | w | rw | RW,ACCESS,DELETE | commands/src/blocking.rs:610 |
| BLMOVE | src (arg0) | w | rw | RW,ACCESS,DELETE | commands/src/blocking.rs:191 |
| BRPOPLPUSH | src (arg0) | w | rw | RW,ACCESS,DELETE | commands/src/blocking.rs:753 |
| SMOVE | src (arg0) | w | rw | RW,ACCESS,DELETE | commands/src/set.rs:914 |
| PFMERGE | dest (arg0) + sources | w | rw / r | dest RW,ACCESS,INSERT; src RO,ACCESS | commands/src/hyperloglog.rs:158 |
| MIGRATE | key(s) | w | rw | RW,ACCESS,DELETE | server/src/commands/migrate_cmd.rs:20 |
| EVAL | KEYS[] | r | rw | RW,ACCESS,UPDATE | server/src/connection/scripting_conn_command.rs:78 |
| EVALSHA | KEYS[] | r | rw | RW,ACCESS,UPDATE | server/src/connection/scripting_conn_command.rs:157 |
| FCALL | KEYS[] | r | rw | RW,ACCESS,UPDATE | server/src/connection/scripting_conn_command.rs:280 |

### Notes on specific BYPASS rows

- **BLMOVE / BRPOPLPUSH / SMOVE** use `AccessSpec::Uniform` (→ both keys `OW`). Redis: source
  `RW,ACCESS,DELETE` (needs read), dest `RW,INSERT` (write-only). Only the **source** is a
  bypass (missing read). Inconsistent with the non-blocking twins **LMOVE / RPOPLPUSH**, which
  use `Positional(RW,RW)` (see DENIAL below). The correct spec is `Positional(RW, W)`.
- **PFMERGE** (`KeySpec::All`, Uniform→all `OW`): dest bypass (missing read on `RW,ACCESS`)
  **and** the *source* keys are read-only (`RO,ACCESS`) but marked `OW` — a write-only user
  passes where Redis needs read (bypass), and a read-only user is wrongly denied (denial). It
  is the multi-role analogue of the store-family; should be `Positional(RW, R)`.
- **SET / BITFIELD** carry `VARIABLE_FLAGS`: the read (`ACCESS`) applies only to the
  read-returning variants (`SET … GET`, `BITFIELD GET …`). For plain `SET k v` / write-only
  `BITFIELD` sub-ops, `OW` is correct. Bypass is real but conditional on those variants.
- **MIGRATE** keys are dynamic (single `key` or `KEYS …`); the command serializes (reads) and
  deletes the key, so read is genuinely required. Marked `OW` uniformly.
- **EVAL / EVALSHA / FCALL** are the most severe direction: keys are marked **`R`** (read-only,
  because the specs lack `CommandFlags::WRITE`), so a principal with only read perm on `KEYS[]`
  could run a script that **writes** them. Redis declares EVAL keys `RW`. **Caveat:** these are
  `ExecutionStrategy::ConnectionLevel(Scripting)`, intercepted before shard routing; if the ACL
  layer instead checks each inner command as it dispatches, the declared key flags may be moot.
  Confirm which path the new enforcement uses before treating as exploitable — but the declared
  flags are wrong on their face.

---

## SEVERITY-DENIAL (ours stronger → false denial once enforced)

| Command | Key pos | Ours | Redis | Redis flags | Evidence |
|---|---|---|---|---|---|
| LMOVE | dest (arg1) | rw | w | RW,INSERT (no ACCESS) | commands/src/list.rs:838 |
| RPOPLPUSH | dest (arg1) | rw | w | RW,INSERT (no ACCESS) | commands/src/list.rs:766 |

Both use `Positional(RW, RW)`; the **destination** does not need read perm in Redis
(`INSERT` only). A principal with write-only perm on the destination would be wrongly denied.
Correct spec: `Positional(RW, W)`. Low severity (over-restrictive, not a bypass).

---

## SEVERITY-MIXED (both a false denial and a bypass)

| Command | Key(s) | Ours | Redis | Redis flags | Evidence |
|---|---|---|---|---|---|
| XREADGROUP | streams (dynamic) | w | r | RO,ACCESS | commands/src/stream/read.rs:160 |

Redis command-flag is `WRITE`, but the per-key ACL spec is `RO,ACCESS` — Redis requires only
**read** perm on the stream (the PEL/group mutation is not surfaced as a key-write for ACL).
FrogDB `Uniform`+`WRITE` → `OW` (write). Effects:
- read-only-on-stream principal: Redis allows, FrogDB denies → **false denial** (the primary,
  likely-hit case, since XREADGROUP consumers are commonly granted read on the stream).
- write-only principal: Redis denies, FrogDB allows → **bypass**.

Correct spec: mark the stream key `R` (declare access explicitly rather than deriving from the
`WRITE` flag).

---

## Dynamic-access commands — verified

| Command | Handler | Verdict |
|---|---|---|
| GEORADIUS | geo.rs:563 `dynamic_keys_with_flags` | **Correct** — source `R`, STORE/STOREDIST dest `OW`. |
| GEORADIUSBYMEMBER | geo.rs:726 | **Correct** — same shape as GEORADIUS. |
| SORT | sort.rs:498 | **Correct for source(`R`)+STORE dest(`OW`)**; **minor gap:** the external keys referenced by `BY`/`GET` patterns (Redis 2nd key-spec block `RO,ACCESS`) are not returned by `dynamic_keys`, so `SORT BY ext_* GET ext_*` would not require read perm on those pattern keys. Low severity, unusual usage. |

(`GEORADIUS`/`GEORADIUSBYMEMBER` were flagged by the mechanical pass only because `access` is
`Dynamic`; the handler code resolves the flags correctly.)

---

## Verified-correct Redis-comparable commands (148)

Grouped for scan; all had `our == redis` required-perm set per key.

- **Strings/bitmap (write, no read — Redis `INSERT`/`UPDATE` only):** APPEND, SETRANGE, SETEX,
  PSETEX, SETNX, DELEX, MSET, MSETNX, DIGEST.
- **Reads (RO):** GET, GETBIT, GETRANGE, SUBSTR, STRLEN, BITCOUNT, BITPOS, BITFIELD_RO, LCS.
- **Store-family (dest `OW`, sources `R`) — Positional, all correct:** BITOP, COPY,
  GEOSEARCHSTORE, SINTERSTORE, SUNIONSTORE, SDIFFSTORE, ZINTERSTORE, ZUNIONSTORE, ZDIFFSTORE,
  ZRANGESTORE, RENAME (`RW`,`OW`), RENAMENX (`RW`,`W`).
- **Hash (write-only or read):** HSET, HSETNX, HSETEX, HMSET, HDEL, HGET, HGETALL, HKEYS,
  HVALS, HLEN, HMGET, HEXISTS, HSTRLEN, HRANDFIELD, HSCAN, HINCRBY* excluded (bypass),
  HEXPIRE/HEXPIREAT/HPEXPIRE/HPEXPIREAT/HPERSIST, HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME.
- **List (write-only or read):** LPUSH, LPUSHX, RPUSH, RPUSHX, LSET, LINSERT, LREM, LTRIM,
  LINDEX, LLEN, LRANGE, LPOS.
- **Set (write-only or read):** SADD, SREM, SCARD, SISMEMBER, SMISMEMBER, SMEMBERS, SINTER,
  SUNION, SDIFF, SINTERCARD, SRANDMEMBER, SSCAN.
- **Sorted-set (write-only or read):** ZADD, ZREM, ZREMRANGEBYRANK, ZREMRANGEBYSCORE,
  ZREMRANGEBYLEX, ZCARD, ZSCORE, ZMSCORE, ZRANK, ZREVRANK, ZCOUNT, ZLEXCOUNT, ZRANGE,
  ZRANGEBYSCORE, ZRANGEBYLEX, ZREVRANGE, ZREVRANGEBYSCORE, ZREVRANGEBYLEX, ZINTER, ZUNION,
  ZDIFF, ZINTERCARD, ZRANDMEMBER, ZSCAN.
- **Stream (write-only or read):** XADD, XDEL, XDELEX, XTRIM, XSETID, XACK, XACKDEL, XCLAIM,
  XAUTOCLAIM, XLEN, XRANGE, XREVRANGE, XREAD, XPENDING.
- **Expiry/generic (write-only or read):** EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT, PERSIST,
  TTL, PTTL, EXPIRETIME, PEXPIRETIME, TYPE, EXISTS, DEL, UNLINK, TOUCH, DUMP, RESTORE.
- **Geo (write-only or read):** GEOADD, GEODIST, GEOHASH, GEOPOS, GEOSEARCH, GEORADIUS_RO,
  GEORADIUSBYMEMBER_RO.
- **HLL / scripting-RO:** PFADD, PFCOUNT (`RO`), PFDEBUG (`RO`), MGET, SORT_RO, EVAL_RO,
  EVALSHA_RO, FCALL_RO.

(Full machine list of all 148 in `findings2.json` → `ok`.)

---

## FrogDB-only key-bearing commands (110) — semantic verdicts

All use `AccessSpec::Uniform`, so every write→`OW`, every read→`R`. No Redis OSS parity, judged
by handler semantics. The **same `Uniform`→`OW` limitation** applies: FrogDB-only
read-modify-write commands that read/return the stored value are under-marked `OW` (would be a
bypass under the enforcement model, exactly like the Redis-comparable class above).

**FrogDB-only commands with the same read-modify-write under-marking (write, but read value):**
- JSON: `JSON.NUMINCRBY`, `JSON.NUMMULTBY`, `JSON.ARRPOP`, `JSON.STRAPPEND`, `JSON.ARRAPPEND`,
  `JSON.ARRINSERT`, `JSON.ARRTRIM`, `JSON.TOGGLE`, `JSON.CLEAR` — compute/return from existing
  document state; `OW` omits the read requirement.
- Sketches/other: `CMS.INCRBY`, `TOPK.ADD`, `TOPK.INCRBY`, `TS.ADD`, `TS.INCRBY`, `TS.DECRBY`,
  `VADD`, `VREM` — read-modify-write, marked `OW`.
- Verdict: consistent with the systemic `Uniform` root cause; flag if strict Redis-style
  read+write ACL is desired for these. Lower priority (module commands).

**FrogDB-only MULTI-key store/merge commands with source-key mismarking (higher concern):**
- `CMS.MERGE` (`DestThenNumkeys`, Uniform→**all** keys `OW`): the source sketches are
  read-only but marked write-only. Should be `Positional(OW, R)`. Same bug class as PFMERGE.
  — commands/src/cms.rs.
- `TDIGEST.MERGE` (`DestThenNumkeys`, Uniform→all `OW`): source digests read-only, marked
  write-only. Should be `Positional(OW, R)`. — commands/src/tdigest.rs.
- `TS.CREATERULE` / `TS.DELETERULE` (`FirstTwo`, Uniform→both `OW`): source+dest series; both
  are updated when a compaction rule is (un)linked, so `OW`/`OW` is defensible.
- `TS.MADD` (`Stride{3}`): every key is written → `OW` correct.

**FrogDB-only pure writes (blind write, `OW` correct):** BF.*/CF.ADD/ADDNX/INSERT*/RESERVE/DEL,
CF.LOADCHUNK, CMS.INITBY*, TDIGEST.CREATE/ADD/RESET, TOPK.RESERVE, TS.CREATE/ALTER/DEL,
VSETATTR, ES.APPEND/SNAPSHOT/REPLAY, FT.SUGADD/SUGDEL, XGROUP, MSETEX, NOOPW, PROBEWRITE,
WALMOCK, TESTW. (`CF.DEL` returns 0/1 = existence only, not value → `OW` acceptable, like SREM.)

**FrogDB-only reads (`R` correct):** BF.EXISTS/MEXISTS/CARD/INFO/SCANDUMP, CF.COUNT/EXISTS/
MEXISTS/INFO/SCANDUMP, CMS.QUERY/INFO, TDIGEST.CDF/QUANTILE/RANK/REVRANK/MIN/MAX/TRIMMED_MEAN/
INFO, TOPK.QUERY/COUNT/LIST/INFO, TS.GET/RANGE/REVRANGE/INFO, VCARD/VDIM/VEMB/VGETATTR/VINFO/
VLINKS/VRANDMEMBER/VRANGE/VSIM, JSON.GET/MGET/TYPE/STRLEN/ARRLEN/OBJLEN/OBJKEYS/ARRINDEX/DEBUG,
ES.READ/INFO, FT.SUGGET/SUGLEN, XINFO, OBJECT, DEBUG.

Note: `JSON.MGET` uses `KeySpec::AllButLast` (last arg is the JSONPath, not a key) with
Uniform→`R`; correct.

---

## Commands not verifiable / out of scope

- **ACL enforcement path for `ConnectionLevel` commands (EVAL/EVALSHA/FCALL, XREADGROUP is
  shard-routed).** Whether the declared per-key `AccessSpec` is actually consulted for
  scripting commands, or whether inner commands are ACL-checked individually, cannot be
  determined — the enforcement code does not exist yet (this audit predates it). The EVAL-family
  BYPASS rows are reported on the declared-flags basis; confirm against the enforcement design.
- **Redis Stack modules (JSON/Bloom/Cuckoo/CMS/TopK/TDigest/TimeSeries/Search/Vector) and
  FrogDB-native commands (ES.*, DIGEST, DELEX, MSETEX, DEBUG, PROBEWRITE, etc.):** no Redis OSS
  `src/commands/*.json`, so judged by handler semantics only (verdicts above), not byte-for-byte
  against an upstream spec.
- The clone emitted `fatal: failed to store: 100001` warnings (sandbox object-store quirk) but
  the sparse checkout produced all 446 command JSONs intact and readable.

## Recommended fixes (summary)

1. Add an `RW` capability to `AccessSpec::Uniform`'s derivation, or (cleaner) give the
   read-modify-write commands explicit `Positional`/a new `AccessSpec::UniformRW` so `OW`→`RW`
   for the 37 BYPASS commands. The pop/GET*/INCR* families are the confidentiality-relevant ones.
2. `BLMOVE`/`BRPOPLPUSH`/`SMOVE` → `Positional(RW, W)` (match LMOVE/RPOPLPUSH shape but with
   dest `W`, not `RW`).
3. `LMOVE`/`RPOPLPUSH` → dest `W` not `RW` (fix the 2 DENIAL rows).
4. `PFMERGE` → `Positional(RW, R)`; `CMS.MERGE`/`TDIGEST.MERGE` → `Positional(OW, R)`.
5. `XREADGROUP` → explicit `R` for the stream key.
6. `EVAL`/`EVALSHA`/`FCALL` → `RW` per key (pending confirmation of the enforcement path).
7. `SORT` → include `BY`/`GET` pattern keys as `R` in `dynamic_keys` (minor).
