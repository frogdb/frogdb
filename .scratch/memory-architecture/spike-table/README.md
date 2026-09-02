# memarch-spike-table — THROWAWAY R5 slot-layout prototype

**This is not production code.** It is a scratch prototype written to answer the
open questions R5/R6/R9 of `.scratch/memory-architecture/PRD.md` leave behind —
what a table slot word looks like, where the inline-value threshold sits, what a
segment and its directory cost, and whether a SCAN cursor can stay stable across
splits — before issues 11 and 12 are scheduled. Nothing here is meant to be
merged into the server.

**Not a workspace member.** `Cargo.toml` carries an empty `[workspace]` table, so
cargo treats this directory as its own workspace root with its own `Cargo.lock`.
It does not participate in — or slow down — FrogDB workspace builds, and the
`just` recipes do not see it. Use plain `cargo` from inside this directory.

## What is here

| Target | Question | PRD ruling |
| --- | --- | --- |
| `src/word.rs` | What fits in a tagged slot word, and what does a wider word cost? | R5, R6 |
| `src/segment.rs` | Segment/bucket layout, and does R9's eviction state fit the header? | R5, R9 |
| `src/table.rs` | Extendible-hash directory, incremental splits, the SCAN cursor | R5 |
| `src/baseline.rs` | The incumbent `griddle::HashMap<Bytes, Entry>` to beat | — |
| `tests/cursor.rs` | Executable proof: SCAN across mid-iteration splits is exactly-once | R5 |
| `tests/table.rs` | Layout assertions, round-trips, stash behaviour, directory overhead | R5, R9 |
| `src/bin/sweep.rs` | Bytes/entry, occupancy, probe lengths, split stalls, baseline compare | R5, R6 |

Five slot layouts are swept, all over the same table code:

| Variant | Key word | Value word | Slots/bucket | What it isolates |
| --- | --- | --- | --- | --- |
| `ptr8` | 8 B, no inlining | 8 B, no inlining | 14 | The no-inline control |
| `int8` | 8 B, no inlining | 8 B, ints inline | 14 | Small-int inlining alone |
| `str7` | 8 B, ≤7 B inline | 8 B, ≤7 B inline | 14 | Short-string inlining |
| `str15w` | 16 B, ≤15 B inline | 16 B, ≤15 B inline | 7 | A wider slot at halved capacity |
| `hybrid` | 8 B, ≤7 B inline | 16 B, ≤15 B inline | 9 | Narrow keys, wide values |

## Running

```bash
cargo test --release                       # cursor proof + layout/round-trip tests
cargo test --release --test cursor -- --nocapture
cargo run  --release --bin sweep           # the full sweep (1M keys per shape)
cargo run  --release --bin sweep 200000    # smaller run
```

Findings and verdicts: [`../spike-report-table.md`](../spike-report-table.md).
