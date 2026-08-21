# 51 — [SEC] `VRANDMEMBER` accepts `i64::MIN` and unbounded negative counts, hanging the shard

Status: needs-triage

**PARKED per user (security)** — do not implement without user ruling.

## What to build

`VRANDMEMBER` parses its optional count as a raw `i64` and hands it straight to
`VectorSetValue::rand_member` with no `i64::MIN` guard and no magnitude cap. On `origin/main` the
parse ladder is `frogdb-server/crates/commands/src/vectorset/vrandmember.rs:56-63` and the call is
`vrandmember.rs:65` (the proposal cites the pre-`d48e1b44` numbering `:50-59`; a `+6` doc-block
insertion landed in every vectorset file, so subtract 6 to read the proposal's numbers). The
callee, `frogdb-server/crates/types/src/vectorset.rs:478-510` (unchanged on main), implements the
negative branch at `:500-508` as `let n = (-count) as usize;` followed by
`(0..n).map(|_| { … names[i].clone() }).collect()`.

Two reachable inputs:

- `VRANDMEMBER k -9223372036854775808` — negating `i64::MIN` overflows. Debug builds panic at the
  negation; **release builds wrap back to `i64::MIN`, giving `n = 2^63`** — an unbounded loop
  cloning `Bytes` into a `Vec` on the shard thread. The shard worker is single-threaded per the
  ADR boundary, so this hangs the whole shard, not one client.
- `VRANDMEMBER k -1000000000` — needs no overflow at all. It materialises a **10^9-element `Vec`**
  before a single byte reaches the client.

Three sibling `*RANDMEMBER`-shaped commands in the same crate carry exactly this guard, each with
a comment explaining why: `commands/src/set.rs:917-923` (SRANDMEMBER — *"Redis rejects `i64::MIN`:
SRANDMEMBER negates a negative count to get `|count|`, and negating `i64::MIN` overflows. Mirrors
the ZRANDMEMBER guard."*), `commands/src/hash.rs:923-926` (HRANDFIELD, which also caps the
`WITHVALUES` magnitude), and `commands/src/sorted_set/pop.rs:366-377` (ZRANDMEMBER, which
additionally rejects `count.abs() > i64::MAX / 2`). All three reply `"value is out of range"`.
Vectorset is the fourth command of this shape and the only one without the guard — a direct Redis
parity gap as well as a DoS.

`rand_member`'s negative branch is also the family's **only** unbounded store-materialising path,
which was checked rather than assumed: `vs.range` (`vrange.rs:80`) is a `BTreeMap` iterator with
`.take(count)` and no pre-allocation (`types/src/vectorset.rs:514-526`), and `vs.search`
(`vsim.rs:221`) clamps to `count.min(self.name_to_id.len())` (`types/src/vectorset.rs:342`). Only
`rand_member` turns a client integer directly into an allocation count. **LIVE on `origin/main`
today** for any client permitted to issue `VRANDMEMBER` (the family is behind
`#[cfg(feature = "vectorset")]` at `commands/src/lib.rs:59`, so it ships only in `full`/`cmd-full`
builds — a blast-radius qualifier, not an exemption). Neither regression file nor
`testing/fuzz/fuzz_targets/vectorset_ops.rs` reaches it: the fuzz target drives `VectorSetValue`
directly with a `count: u8` field, so a negative count is structurally unrepresentable.

Fix direction, when unparked: copy the sibling convention verbatim — reject `count == i64::MIN`
with `"value is out of range"`, and add the magnitude cap the three siblings' comments describe,
so the four `*RANDMEMBER` commands answer identically.

## Acceptance criteria

- [ ] `VRANDMEMBER k -9223372036854775808` returns `ERR value is out of range` in both debug and
      release builds, with no panic and no `ShardPanicsIsolated` increment
- [ ] `VRANDMEMBER k -1000000000` does not allocate proportionally to the requested magnitude —
      it is either rejected by the magnitude cap or bounded by the set's cardinality, and the
      command returns within a bounded time on a small set
- [ ] The reply string and the rejection boundary match `SRANDMEMBER`/`HRANDFIELD`/`ZRANDMEMBER`
      for the same inputs
- [ ] Regression test `vrandmember_rejects_i64_min_and_huge_negative_counts` in
      `frogdb-server/crates/redis-regression/tests/vectorset_regression.rs`, asserted alongside
      the `SRANDMEMBER` reply for the identical count so the parity is pinned, failing on today's
      code
- [ ] `just test frogdb-redis-regression vrandmember_rejects`

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 99 (`.scratch/arch-deepening/proposals/99-vectorset-file-collapse.md`),
defect F2.

## Comments
