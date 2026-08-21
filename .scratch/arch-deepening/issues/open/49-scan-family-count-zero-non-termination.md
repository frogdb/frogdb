# 49 — `COUNT 0` hangs HSCAN/SSCAN/ZSCAN forever and silently truncates SCAN

Status: needs-triage

## What to build

`hash_cursor_scan` — the shared engine behind HSCAN/SSCAN/ZSCAN — never terminates when the
client passes `COUNT 0`, starting from cursor `0`. On `origin/main` the function lives at
`frogdb-server/crates/commands/src/utils.rs:82-126` (the proposal cites the pre-HF-B numbering
`utils.rs:141-145`; HF-B deleted `simple_glob_match` and shifted everything ~−28, so the live
cite is **`utils.rs:109-113`**). The emit loop opens with `if emitted >= count { new_cursor =
hash; break; }`, so with `count == 0` the very first item satisfies `0 >= 0`: the call returns
`(hash_of_first_item, [])`. The client resumes with that hash, `partition_point`
(`utils.rs:102`) lands on the same item, and the same hash comes back. **From `HSCAN k 0 COUNT
0` onward the client loops forever** on any non-empty collection (an empty collection returns
the `new_cursor = 0` initialiser at `utils.rs:107` and terminates). Nothing upstream rejects
the value: `ScanRequest::parse` defaults `count` to `10` (`utils.rs:174`) and accepts whatever
`try_flag_usize(b"COUNT")` yields (`utils.rs:179-181`), and `scan_reply` passes it straight
through (`utils.rs:222-229`).

**Verified still LIVE on `origin/main` (post-HF-B).** HF-B replaced `simple_glob_match` with
`frogdb_core::glob_match` at `utils.rs:116` and deleted the local matcher; it did not touch the
`emitted >= count` break. The `COUNT 0` half of the defect is untouched by the `d48e1b44` batch.

**The two SCAN-family paths disagree, and that is the second half of the bug.** The plain
`SCAN`/`KEYS` scatter path does *not* loop from cursor 0: `frogdb-server/crates/server/src/
connection/scatter.rs:133`'s `while all_keys.len() < count && next_shard < self.num_shards` is
false immediately, so `next_shard`/`next_position` keep their decoded values and
`encode_final_cursor` (`scatter.rs:364-370`) returns `cursor::encode(0, 0)` — which is `0`
(`commands/src/scan.rs:29-31`). `SCAN 0 COUNT 0` therefore replies `["0", []]` and *ends*,
silently returning nothing rather than spinning. Non-termination on that path needs a **non-zero**
cursor: `SCAN <c> COUNT 0` with `c != 0` echoes `c` back forever, because encode∘decode
round-trips. So one wire grammar hangs from cursor 0, the other truncates from cursor 0 and hangs
mid-iteration — same option, three behaviours, none of them pinned. `grep` for `COUNT", "0"`
across `frogdb-server/crates` finds pins for HOTKEYS/ZMPOP/BZMPOP/LMPOP/LPOS and **none** for the
SCAN family.

**A live-Redis behavioral diff is required before any fix.** Redis's `scanGenericCommand` is
believed to reject `count < 1` with a syntax error, but that is from reading the Redis source, not
from a running server, and it has not been verified here. Establish Redis's (and Valkey's) actual
`COUNT 0` reply for `SCAN`, `HSCAN`, `SSCAN` and `ZSCAN` **first**. The fix direction has two
independent axes and the ruling must answer both: (a) rejecting `count < 1` fixes the H/S/ZSCAN
hang; (b) it *also* turns today's terminating-but-empty `SCAN 0 COUNT 0` reply into an error,
which is a separate compat question. Do not ship a bare `count < 1` reject that silently changes
axis (b). Proposal 98's shared-parser move gives one place to implement whichever answer comes
back.

Adjacent, already filed: `.scratch/testing-improvements-round2/issues/open/82-commands-core-types-residual-test-gaps.md`
finding **F10** describes the same `emitted >= count` line reaching a cursor livelock via a
different trigger (a hash-collision group larger than `COUNT`), and the per-call
collect-and-sort cost. This issue is the `COUNT 0` trigger and the SCAN-vs-per-key disagreement,
neither of which F10 covers; fix work should be coordinated with it.

## Acceptance criteria

- [ ] A live-Redis (and Valkey) behavioral diff for `SCAN 0 COUNT 0`, `SCAN <nonzero> COUNT 0`,
      `HSCAN k 0 COUNT 0`, `SSCAN k 0 COUNT 0` and `ZSCAN k 0 COUNT 0` is recorded in the issue
      before any code changes, and the chosen FrogDB semantics are stated against it
- [ ] `HSCAN k 0 COUNT 0` (and the SSCAN/ZSCAN equivalents) terminates — either by rejecting
      `count < 1` on the shared parser, or by advancing the cursor past the boundary item —
      observable as: a full HSCAN loop driven with `COUNT 0` completes in bounded calls or
      returns an error on the first call
- [ ] `SCAN` and the per-key SCAN commands agree: the same `COUNT 0` argument produces the same
      class of reply on both grammars, with the mid-iteration `SCAN <nonzero> COUNT 0` case
      covered too
- [ ] Regression test `scan_count_zero_terminates_across_family` in
      `frogdb-server/crates/redis-regression/tests/scan_regression.rs` drives all five commands
      with `COUNT 0` from both cursor 0 and a resumed non-zero cursor, asserting the pinned
      behaviour and failing on today's code
- [ ] `just test frogdb-redis-regression scan_count_zero`

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 98 (`.scratch/arch-deepening/proposals/98-scan-grammar-unify.md`),
defect F5 / ruling R2.

## Comments
