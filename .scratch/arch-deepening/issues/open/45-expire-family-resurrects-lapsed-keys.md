# 45 — `EXPIRE` on a lapsed key returns `1` and resurrects it: eight blind `contains` guards in `expiry.rs`

Status: needs-triage

## What to build

Every command in `frogdb-server/crates/commands/src/expiry.rs` opens its decision ladder with the
same guard, `if !ctx.store.contains(key) { return … }`, at `:283`, `:372`, `:449`, `:533`, `:611`,
`:664`, `:749`, `:799` (all eight verified line-exact at HEAD). `Store::contains`
(`core/src/store/mod.rs:411`, `hashmap.rs:958-960`) is `self.data.contains_key(key)` — a bare map
probe, `&self`, **no expiry logic whatsoever**, blinder even than raw `Store::get`. Nothing purges
before these guards: contrast `generic.rs:196`, where RENAME calls
`let _ = ctx.store.get_with_expiry_check(new_key);` purely for its purge side effect immediately
above its own `contains` at `:197`. So for a key past its deadline that the 100 ms index-driven
sweep (`shard/event_loop.rs:24`) has not yet reaped, `contains` returns `true`, the guard falls
through, and the ladder runs against a corpse.

**The four write commands are LIVE and resurrect the key.** `:283` (`ExpireCommand`, struct at
`:233`), `:372` (`PexpireCommand`, `:329`), `:449` (`ExpireatCommand`, `:417`), `:533`
(`PexpireatCommand`, `:501`) each fall through to `ctx.store.set_expiry(key, expires_at)`
(`expiry.rs:320`, `:408`, `:492`, `:576` respectively) and return `Integer(1)`:

```
DEBUG SET-ACTIVE-EXPIRE 0
SET k v ; PEXPIRE k 50 ; … 100 ms …
EXISTS k     → :0        ← the key does not exist
EXPIRE k 100 → :1        ← Redis: :0        the corpse is now live for 100 s
GET k        → "v"       ← Redis: (nil)
```

Redis returns `0` and leaves the key gone (`expireGenericCommand` calls `lookupKeyWrite`, which
lazy-expires first). FrogDB installs a **fresh 100 s deadline on already-deleted data**, and the
resurrection propagates: the `set_expiry` is journalled and replicated as a legitimate write, so a
key the client asked to have deleted comes back on every replica and survives recovery. This is a
strictly worse version of the BITFIELD resurrection proposal 97 promotes to hotfix H2 — it is
reachable with two ordinary commands and no sub-command syntax.

**The four read commands are blind but currently compensated — do not "fix" them blindly.** `:611`
(TTL), `:664` (PTTL), `:749` (EXPIRETIME), `:799` (PEXPIRETIME) also probe with `contains`, but each
has a downstream `if expires_at <= clock::now() { return -2 }` guard (`expiry.rs:618-620` for TTL,
`:753-758` for EXPIRETIME) landed by round-2 issue 57 under `FM-PERSISTENCE-044`, so they
already answer `-2` for a corpse. Their guards are latent, not defective. Changing them to a
**mutating** probe would make four `READONLY` commands reap the keyspace — the same flag-contract
break proposal 97 flags for `OBJECT REFCOUNT`. Use `exists_unexpired` (`store/mod.rs:421-423`,
`&self`) there if anything, and `Store::exists_for_write`
(`!self.purge_if_expired(key) && self.contains(key)`, proposal 97 §(b)) for the four write sites.

**Coordination.** The guard and proposal 92's EXPIRE decision-table work are four lines apart in
each handler; 92 must not pin the current NX/XX/GT/LT behaviour against a `contains` input that is
wrong for expired keys. Adjacent but distinct: `.scratch/testing-improvements-round2/issues/open/58-expire-gt-lt-evaluated-after-past-deadline-delete.md`
covers the *ordering* of the negative-argument delete against GT/LT in the same four handlers — same
lines, different defect; land them together to avoid rebase churn.

## Acceptance criteria

- [ ] `EXPIRE` / `PEXPIRE` / `EXPIREAT` / `PEXPIREAT` against a past-deadline, unswept key return
      `0`, install no new deadline, and leave `EXISTS k` at `0` and `GET k` at nil
- [ ] The purge is journalled — no replica or recovery run observes the resurrected key
- [ ] `TTL` / `PTTL` / `EXPIRETIME` / `PEXPIRETIME` keep answering `-2` and stay non-mutating (no
      `READONLY` command gains a keyspace reap)
- [ ] Regression test `expire_family_on_a_past_deadline_key_does_not_resurrect_it`, table-driven
      over the four write commands, driven with `DEBUG SET-ACTIVE-EXPIRE 0` + `PEXPIRE`, asserting
      the return value **and** a subsequent `GET`
- [ ] `just test frogdb-commands expire` green; `just test frogdb-server expire_tcl` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 97 (`.scratch/arch-deepening/proposals/97-typed-store-access.md`),
defect F-5 (the eight `expiry.rs` `contains` guards, ruled LIVE in rev 2).

## Comments
