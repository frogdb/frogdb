# 31 — BF/CF.SCANDUMP returns iterator 0 on the first call, so a conformant client backs up nothing

Status: needs-triage

## What to build

`BF.SCANDUMP key 0` returns `Response::Array([Integer(0), bulk(data)])` on the **first** call —
whole filter, one chunk, iterator `0`
(`frogdb-server/crates/commands/src/bloom.rs:551-554`; `CF.SCANDUMP` is the same shape in
`commands/src/cuckoo.rs`). Upstream RedisBloom's protocol is the opposite: the first call with
iterator `0` returns `(1, data)` and the client loops until the returned iterator is `0` — visible
in RedisBloom's own source, where `BF.LOADCHUNK`'s initial-load branch is guarded by `iter == 1`
and `CF.LOADCHUNK`'s by `pos == 1`. The documented client loop is `while iter != 0: save(chunk)`.

Trace that loop against FrogDB: the client issues `BF.SCANDUMP key 0`, receives `(0, data)`,
evaluates `iter != 0` as false, and **breaks before saving the chunk it was just handed**. No
error, no retry — it reports a successful backup containing nothing. A defensive client that asks
for another chunk fares no better: `SCANDUMP` with a non-zero iterator answers
`[Integer(0), Null]` (`bloom.rs:557`) rather than an error, so it gets a well-formed "nothing here"
instead of a signal. That is a silent-data-loss outcome from a correctly-written client, not a
documentation preference. The reverse direction is closed too — FrogDB's `LOADCHUNK` hard-rejects
any non-zero iterator (`bloom.rs:606-610`, `cuckoo.rs:665-669`) — so a RedisBloom dump cannot be
loaded either; the formats interchange in neither direction.

The deviation is LIVE on `main` today (verified against `d48e1b44`: the `// Return iterator=0
(done) and the data` and `// Invalid iterator - return empty` branches are unchanged; the
`acc073ea` LOADCHUNK hardening did not touch the SCANDUMP encoder). It is reachable only in a
`--features cmd-full` build, since `BF`/`CF` are not in the default `core-profile`. It is also
undocumented: the generated compatibility matrix derives status from *regression-test exclusions*
(`website/scripts/matrix-gen.py:53-54`), `website/src/data/compat-exclusions.json` carries no bloom
or cuckoo entry, and `website/src/content/docs/compatibility/overview.mdx` mentions SCANDUMP
nowhere — so all four commands render as unqualified **Supported**.

**The behavioral fix is a breaking reply-shape change and needs an owner ruling before
implementation**, because the current shape is pinned by assertions, not merely observed:
`frogdb-server/crates/redis-regression/tests/bloom_regression.rs:258` and `:497` each carry the
comment *"FrogDB uses single-chunk dump: SCANDUMP with iterator 0 returns [0, data]"* followed by
`assert_eq!(unwrap_integer(&arr[0]), 0)`. Fix direction: make the first call return `(1, data)` and
have a second call with the returned iterator return `(0, "")` (or make `LOADCHUNK` accept the
upstream `iter == 1` initial-load form), moving the wire change and both pinned assertions in
lockstep. The documentation half — an own top-level `##` section on the compatibility overview page
stating that FrogDB's filter dumps do not interchange with RedisBloom in either direction — is
proposal 89's hotfix H1 and lands separately; do not hand-edit the generated matrix data files.

## Acceptance criteria

- [ ] A stock RedisBloom-style client loop (`while iter != 0: save(chunk); iter, chunk =
      SCANDUMP(key, iter)`) run against FrogDB saves the filter's bytes rather than an empty
      backup, for both `BF` and `CF`.
- [ ] `SCANDUMP` with an iterator FrogDB did not hand out returns an error rather than
      `[0, Null]`, so no client can mistake a rejection for end-of-stream.
- [ ] Regression tests `bf_scandump_client_loop_saves_data` / `cf_scandump_client_loop_saves_data`
      in `frogdb-server/crates/redis-regression/tests/bloom_regression.rs` drive the upstream loop
      shape end to end and then `LOADCHUNK` the result back into a fresh key, asserting membership
      survives. The existing round-trip tests at `:246-283` / `:487-…` and their blessing comments
      at `:258` / `:497` are updated in the same change, not left contradicting the new shape.
- [ ] `just test frogdb-server scandump` green (the bloom/cuckoo regression tests require the
      `full`/`cmd-full` feature).

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 89 (`.scratch/arch-deepening/proposals/89-probabilistic-chunk-codec.md`),
§Problem 4 — "The SCANDUMP iterator protocol silently produces empty backups, and nothing documents
it" (review note N7: deviation is a DEFECT, not a doc gap).

## Comments
