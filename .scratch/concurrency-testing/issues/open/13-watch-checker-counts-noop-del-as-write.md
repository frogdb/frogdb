# 13 — WATCH false-negative checker counts a no-op DEL as a write (checker false positive)

Status: ready-for-agent
Type: bug
Origin: post-harness-fix re-verification of issue 11 Finding B (2026-08-02) — one of the three
TxHeavy WATCH false-negatives reported at `ops_per_client = 60` is **not** a product bug.

## What happened

`writer_between` (`frogdb-server/crates/testing/src/conservation.rs:569`) decides whether an op
"wrote" a watched key. It is result-aware for the blocking/pop vocabulary only:

```rust
fn written_keys_of(function: &str, args: &[Bytes], result: Option<&Bytes>) -> Vec<Bytes> {
    match function {
        "lpop" | "rpop" => { if result.is_some() { … } else { Vec::new() } }
        "blpop" | "brpop" | "bzpopmin" | "bzpopmax" => { … }
        "lmove" | "blmove" => { if result.is_some() { … } else { Vec::new() } }
        _ => default_keys_of(function, args),   // <-- DEL that deleted nothing counts as a write
    }
}
```

`DEL k` on a **missing** key returns `0` and, in Redis and in FrogDB alike, does **not** touch the
key's watch version — no watcher is dirtied. The checker's default branch counts it anyway, so a
committed EXEC whose watch window contains only such a no-op DEL is reported as a WATCH
false-negative even though the server behaved correctly.

The same hole exists on the second path in `writer_between`, which parses another client's committed
EXEC via `parse_exec_commands` and extracts keys with `default_keys_of` — with no access to the
sub-command's individual result at all, so a `DEL` of a missing key *inside* another client's
transaction is likewise counted as a write.

## Evidence

Verbatim from `FROGDB_CONCURRENCY_OPS_PER_CLIENT=60 FROGDB_CONCURRENCY_SEEDS=20
just concurrency-nightly` (local, 2026-08-02):

```
seed 14 (TxHeavy) FAILED: ["WATCH false-negative: watch false-negative: exec op 29575 committed
  though op 29569 wrote watched key [123, 116, 48, 125, 107, 118, 48] after watch op 29564"]
```

`[123,116,48,125,107,118,48]` = `{t0}kv0`. The flagged "writer" (op 29569) is `del ["{t0}kv0"]`
returning `"0"` — nothing was deleted. The one genuinely-modifying DEL of that key in the vicinity
overlaps the watch boundary and is therefore correctly excluded by `writer_between`'s strict
containment rule, so with the no-op DEL discounted this history has **no** violation.

Contrast with issue 12 (seeds 3/16), whose flagged writer is a DEL returning `1` — a real
modification, and a real product bug.

## Suggested fix

Make the write vocabulary result-aware for the no-op cases, both directly and inside EXEC:

- `del` / `unlink` → a write only when the result is a non-zero count. (With multi-key DELs, a
  non-zero count does not say *which* key was removed; the workload emits single-key DELs, but the
  checker should either restrict to that case or stay conservative and treat a multi-key DEL with a
  non-zero result as a write to every argument.)
- Audit the rest of the vocabulary for the same shape: `setnx`/`msetnx` returning `0`, `getdel`
  returning nil, `srem`/`zrem`/`hdel` returning `0`, `lrem` returning `0`, `expire`/`persist`
  returning `0`, `smove` returning `0`.
- For the EXEC path, `parse_exec_commands` currently yields only `(name, args)`. Sub-command results
  are available in the EXEC's `|`-joined result string, so the parse can be extended to pair each
  sub-command with its result and reuse the same predicate.

Erring toward "counts as a write" is *not* safe here: this checker's soundness claim is
"no false positives" (it deliberately ignores same-client writes and merely-overlapping writers to
keep that claim). A spurious write breaks exactly that claim.

## Acceptance criteria

- [ ] A DEL/UNLINK that deleted nothing is not treated as a write by `writer_between`, on both the
      direct-op path and the committed-EXEC sub-command path.
- [ ] Unit tests in `frogdb-server/crates/testing` pin both: a `del`→`0` in the watch window does
      **not** flag; a `del`→`1` in the same position **does**.
- [ ] The remaining zero-effect commands listed above are either handled or explicitly documented as
      not-yet-covered with the reason.
- [ ] Re-running `FROGDB_CONCURRENCY_OPS_PER_CLIENT=60 FROGDB_CONCURRENCY_SEEDS=20
      just concurrency-nightly` no longer reports the seed-14 class (seeds 3/16 remain until
      issue 12 is fixed).

## References

- `frogdb-server/crates/testing/src/conservation.rs` — `written_keys_of`, `writer_between`,
  `check_watch_no_false_negative`, `is_write`, `parse_exec_commands`.
- Issue 12 — the genuine WATCH false-negative this must not be confused with.
- Issue 11 — Finding B, where the seed-14 report was first grouped with the real bug.
