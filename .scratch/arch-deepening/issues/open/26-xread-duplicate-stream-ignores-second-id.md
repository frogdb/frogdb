# 26 — Blocking XREAD naming the same stream twice silently uses only the first after-ID

Status: needs-triage

## What to build

`BlockingXRead::satisfy` in `frogdb-server/crates/core/src/shard/blocking.rs:1089-1125` selects
which `after_id` applies to the key being woken by **position lookup**:

```rust
let key_idx = entry.keys.iter().position(|k| k == key).unwrap_or(0);   // :1097
let after_id = &after_ids[key_idx];                                    // :1098
```

`entry.keys` and `after_ids` are built as parallel vectors by the XREAD parser
(`frogdb-server/crates/commands/src/stream/read.rs:71-116`), which pushes **one entry per
`STREAMS`/ID pair with no deduplication** — `keys: keys.to_vec()` is the raw argument slice and
`resolved_ids` is appended to inside the same loop. So `XREAD BLOCK 0 STREAMS s s 5 9` parks a
waiter with `keys = [s, s]` and `after_ids = [(0,5), (0,9)]`, and `position(|k| k == key)` returns
`0` for both occurrences. The second ID is silently discarded: the waiter is satisfied against ID
5, so the client is woken with entries it explicitly asked to skip past on the second reference,
and receives one reply array where two were requested. The `unwrap_or(0)` fallback compounds it —
any future path that wakes on a key not present in `entry.keys` reads `after_ids[0]` rather than
erroring.

The mismatch is internally inconsistent as well as wrong: the **non-blocking** path in the same
parser iterates `keys.iter().zip(ids.iter())` and honours every pair independently
(`read.rs:71-101`), so `XREAD STREAMS s s 5 9` against a populated stream returns two arrays with
different contents, while the same command with `BLOCK` returns one array computed from the wrong
ID. Redis attaches the after-ID to the blocked-key record itself (`bkinfo->stream_id` stored per
key in `blockForKeys`) rather than looking it up by argument position, so it has no positional
lookup to get wrong.

This is **LIVE on main** — no panic, no error, just a wrong reply, reachable from a single ordinary
client session with no cluster or replication involved. It is the same duplicate-key family as the
`BLPOP k k` defect in proposal 81 (`wait_queue.rs` unlink filtering), but a distinct root cause: 81
is about stale indices in `waiters_by_key`, this is about a positional lookup that cannot
distinguish repeated keys. Fixing it is not a one-liner: either the parser must collapse duplicate
stream names to the appropriate single ID before parking (deciding which of the two IDs wins — the
tighter one is the defensible choice, matching Redis's dict-keyed storage where the last write
wins), or `BlockingOp::XRead` must carry `(key, after_id)` pairs so the lookup is by identity
rather than position. Proposal 84's `BlockingOp` dedupe touches `:1098` but explicitly neither
fixes nor worsens `:1097`.

## Acceptance criteria

- [ ] `XREAD BLOCK 0 STREAMS s s <id1> <id2>` that parks and is then woken by an `XADD` produces a
      reply consistent with the non-blocking form of the same command — no ID silently discarded.
- [ ] The `unwrap_or(0)` fallback is gone: the after-ID for a woken key is resolved by identity, or
      the resolution is total by construction.
- [ ] Regression test `test_blocking_xread_duplicate_stream_honours_both_ids`
      (`frogdb-server/crates/core/src/shard/blocking.rs` test module): park an `XRead` waiter with
      `keys = [s, s]` and two distinct `after_ids`, satisfy on `s`, assert the reply reflects the
      agreed semantics rather than `after_ids[0]`. Fails against today's tree.
- [ ] Integration test in `redis-regression` asserting the blocking and non-blocking forms of
      `XREAD STREAMS s s <id1> <id2>` agree.
- [ ] `just test frogdb-core blocking_xread_duplicate` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 84 (`.scratch/arch-deepening/proposals/84-blocking-op-dedupe.md`),
latent defect recorded at `core/src/shard/blocking.rs:1097` under §Other risks ("recorded, not
claimed").

## Comments
