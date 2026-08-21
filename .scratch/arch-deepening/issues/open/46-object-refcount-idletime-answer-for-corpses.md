# 46 — `OBJECT REFCOUNT` and `OBJECT IDLETIME` answer for a past-deadline key; both need a non-mutating expiry probe

Status: needs-triage

## What to build

Two `OBJECT` subcommands read the store through primitives with no expiry logic and therefore
report on a key that is logically gone. Both sit inside `OBJECT`'s single `CommandSpec`
(`frogdb-server/crates/commands/src/generic.rs:337`), which is `CommandFlags::READONLY` — that flag
is the load-bearing constraint on the fix, and it is why these two are called out separately from
the `contains`/`get` sites in issue 44.

**(A) `OBJECT REFCOUNT` — `contains` probe, `generic.rs:466`.** The `b"REFCOUNT"` arm
(`generic.rs:460`) is `if ctx.store.contains(key) { Ok(Response::Integer(1)) } else {
Ok(Response::null()) }`. `Store::contains` (`core/src/store/mod.rs:411`, `hashmap.rs:958-960`) is
`self.data.contains_key(key)` — `&self`, a bare map probe, no expiry at all. For a key past its
deadline that the 100 ms index-driven sweep has not reaped, FrogDB answers `1` where Redis
(`objectCommandLookupOrReply` → `lookupKeyReadWithFlags`) answers nil. The sibling subcommand
`TYPE` at `generic.rs:52` and `EXISTS` at `basic.rs:885` already use the non-mutating
`exists_unexpired` (`store/mod.rs:421-423`) for exactly this reason, with a comment at
`generic.rs:48-51` explaining why it stays non-destructive — so the correct pattern is two lines
away in the same file. **The fix is `exists_unexpired`, not `exists_for_write`.** Proposal 97 §(c)
originally swept REFCOUNT into its Class C `exists_for_write` migration; the rev-2 review caught
that this would make a `READONLY` command reap the keyspace, breaking the flag's contract, and the
reap would not even be journalled. Guard against the regression, not just the current answer.

**(B) `OBJECT IDLETIME` — `get_metadata` probe, `generic.rs:452`.** `Store::get_metadata`
(`hashmap.rs:1542-1544`) is `self.data.get(key).map(|e| e.metadata.clone())` — `&self`, no expiry —
a **fourth** blind primitive alongside `get`, `contains` and the `-metadata` door, outside the 69
raw-`get` and 14 `contains` inventories proposal 97 counts. The `b"IDLETIME"` arm reports
`meta.last_access.elapsed().as_secs()` for the corpse; Redis answers "no such key". The sibling
`OBJECT FREQ` at `generic.rs:441` reads the same primitive and has the same defect — treat them as
one fix. Fix shape: gate the unchanged `get_metadata` call behind `exists_unexpired`, keeping the
whole subcommand non-mutating.

Both are LIVE on main today (cites verified line-exact at HEAD) and reachable with
`DEBUG SET-ACTIVE-EXPIRE 0` + `PEXPIRE`, but the blast radius is introspection-only: no data loss,
no resurrection, no replicated write — the wrong answer never leaves the `OBJECT` reply. That makes
this the lowest-severity member of the family and a good candidate to land alongside issue 44 rather
than on its own. Note also that migrating the *reads* in issue 44 to `get_with_expiry_check` shifts
`OBJECT IDLETIME`/`FREQ` answers for ~40 commands, because `get_with_expiry_check` touches
`metadata.touch()` and `lfu_log_incr` (`hashmap.rs:1139-1148`) and raw `get` does not — that is a
move toward Redis's `lookupKeyRead` and belongs in the merge notes, but it means the two issues'
tests interact.

## Acceptance criteria

- [ ] `OBJECT REFCOUNT k` returns nil for a past-deadline, unswept key (today: `1`)
- [ ] `OBJECT IDLETIME k` and `OBJECT FREQ k` return nil for a past-deadline, unswept key (today: an
      idle-seconds / LFU-counter integer)
- [ ] All three stay non-mutating: after the call, the key is still physically present (the sweep,
      not `OBJECT`, reaps it), and no WAL record or replica byte is produced. `OBJECT` keeps
      `CommandFlags::READONLY`
- [ ] Regression test `object_subcommands_do_not_answer_for_a_past_deadline_key`, table-driven over
      REFCOUNT / IDLETIME / FREQ / ENCODING, with an explicit assertion that the key is *not* reaped
      by the probe — this is the arm that would catch a future `exists_for_write` migration
- [ ] `just test frogdb-commands object` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 97 (`.scratch/arch-deepening/proposals/97-typed-store-access.md`),
defect F-6 (`OBJECT REFCOUNT` at `generic.rs:466` needs `exists_unexpired`, and `OBJECT IDLETIME` at
`generic.rs:452` is the fifteenth probe of the same family).

## Comments
