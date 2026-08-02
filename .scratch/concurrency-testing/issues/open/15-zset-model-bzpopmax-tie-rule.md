# 15 — ZSet "not linearizable" reports are a model artifact: BZPOPMAX tie rule inverted

Status: ready-for-agent
Type: bug
Origin: post-harness-fix re-verification of issue 11 Findings B/C (2026-08-02). **Finding C is not a
product bug.**

## What happened

`ZSetModel::step` (`frogdb-server/crates/testing/src/models/zset.rs:116`) picks the popped member
for `bzpopmin`/`bzpopmax` like this:

```rust
// DIVERGENCE: on score ties we pop the lexicographically
// smallest member for both MIN and MAX (Redis' exact tie
// rule is not modeled in phase 1; workloads avoid ties).
let (member, score) = z.iter().min_by(|(am, asc), (bm, bsc)| {
    let ord = asc.partial_cmp(bsc).unwrap_or(std::cmp::Ordering::Equal);
    let ord = if want_min { ord } else { ord.reverse() };
    ord.then_with(|| am.cmp(bm))
})…
```

and then requires the server's reply to match that member **exactly**.

A Redis sorted set is ordered by `(score asc, member lex asc)`. `BZPOPMIN` takes the first element —
on a tie, the lexicographically **smallest** member (model correct). `BZPOPMAX` takes the last —
on a tie, the lexicographically **greatest** member (model inverted). FrogDB implements the Redis
rule: `SkipList::pop_last` (`frogdb-server/crates/types/src/skiplist.rs:368`) pops the tail of a
list ordered by `(score, member)`.

The comment's premise ("workloads avoid ties") is also false. The generator emits
`zadd <key> <score 0..100> m<0..5>` (`frogdb-server/crates/testing/src/workload.rs:731` and `:785`),
i.e. at most five members drawn from 100 scores, with repeated re-scoring — ties are routine, and
half of the generated pops are `bzpopmax`.

So every time a `BZPOPMAX` lands on a tied top score, the model rejects the server's (correct)
answer, no linearization survives, and the checker reports `key … (ZSet) not linearizable`.

## Evidence

Seed 19 / `MultiWaiter` / OPS=60 / 4 clients / 2 shards, one failing run's `{t13}zs1`
sub-history (times are the harness's logical timestamps; these ops do not overlap):

```
op 963 c2 [ 0, 1] zadd ["{t13}zs1","55","m0"] -> "1"
op 966 c2 [ 2, 3] zadd ["{t13}zs1","55","m1"] -> "1"     <- m0 and m1 tie at 55
op 969 c2 [ 4, 5] zadd ["{t13}zs1","47","m2"] -> "1"
op 974 c2 [ 6, 7] zadd ["{t13}zs1","56","m2"] -> "0"
op 977 c1 [ 8, 9] bzpopmax ["{t13}zs1","5"] -> "{t13}zs1|m2|56"   (56 unique — accepted)
op 978 c1 [10,11] zadd ["{t13}zs1","23","m4"] -> "1"
op 983 c3 [12,13] bzpopmax ["{t13}zs1","5"] -> "{t13}zs1|m1|55"   <- TIE: server pops m1
```

At op 983 the set is `{m0:55, m1:55, m4:23}`. Redis (and FrogDB) pop `m1` — the lexicographically
greatest of the tied maxima. The model demands `m0`, returns `None`, and the whole key is declared
non-linearizable.

The same run's `{t12}zs0` shows it too: after `zadd 71 m3`, `zadd 76 m1`, `zadd 71 m2`,
`zadd 6 m1`, the state is `{m3:71, m1:6, m2:71}` and `bzpopmax` correctly returns `m3|71` (tied with
`m2` at 71) — again rejected by the model.

Repro (nondeterministic — see issue 14; this reproduced on the first attempt at seed 19/OPS=60,
and roughly 1 run in 4 at seed 10/OPS=30):

```
FROGDB_CONCURRENCY_OPS_PER_CLIENT=60 FROGDB_CONCURRENCY_SEEDS=20 just concurrency-nightly
```

## Suggested fix

Fix the model, not the workload — the tie rule is observable Redis behaviour and worth modelling:

```rust
let ord = asc.partial_cmp(bsc).unwrap_or(Ordering::Equal);
let (ord, member_ord) = if want_min {
    (ord, am.cmp(bm))            // smallest score, then lex-smallest member
} else {
    (ord.reverse(), bm.cmp(am))  // greatest score, then lex-GREATEST member
};
ord.then(member_ord)
```

and delete the DIVERGENCE comment. Do not "fix" this by making the generator avoid ties: ties are
exactly the interesting case, and the same rule governs `ZPOPMAX`/`ZRANGE`, so the model would keep
lying about them.

While in there, check the rest of the ZSet model for the same assumption (`zpopmin`/`zpopmax` if/
when added, any range vocabulary), and check the List/Stream models for comparable "phase 1
divergence" comments whose premise no longer holds.

## Acceptance criteria

- [ ] `ZSetModel` pops the lexicographically greatest member on a `bzpopmax`/`zpopmax` score tie and
      the smallest on a `bzpopmin`/`zpopmin` tie.
- [ ] Unit test in `frogdb-server/crates/testing/src/models/zset.rs` covering both tie directions,
      including the exact `{m0:55, m1:55, m4:23}` → `bzpopmax` → `m1` case above.
- [ ] `FROGDB_CONCURRENCY_OPS_PER_CLIENT=60 FROGDB_CONCURRENCY_SEEDS=20 just concurrency-nightly`
      no longer reports `(ZSet) not linearizable` across several repeated sweeps (repeat because of
      issue 14).
- [ ] Issue 11's Finding C is annotated as a model artifact and closed out.

## References

- `frogdb-server/crates/testing/src/models/zset.rs` — `ZSetModel::step`, the DIVERGENCE comment.
- `frogdb-server/crates/types/src/skiplist.rs` — `pop_first`/`pop_last`, the server-side ordering.
- `frogdb-server/crates/testing/src/workload.rs` — `gen_zset`/`gen_multi_waiter` score and member
  generation (the tie source).
- Issue 11 — Findings B/C, where these reports were classified as product bugs.
- Issue 14 — why the repro is probabilistic.
