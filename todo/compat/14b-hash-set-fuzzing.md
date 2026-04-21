# 14b. Hash & Set Fuzzing (3 tests — `set_tcl.rs`, `hash_tcl.rs`)

**Status**: Not implemented
**Scope**: Port randomized correctness tests that verify set/hash operations produce correct
results under random workloads.

## Architecture

### SDIFF Fuzzing

The Redis `SDIFF fuzzing` test creates multiple sets with random members and verifies that
`SDIFF` produces the mathematically correct set difference. This validates the `SdiffCommand`
implementation (`frogdb-server/crates/commands/src/set.rs`) under adversarial random inputs.

FrogDB's SDIFF is already implemented with `Arity::AtLeast(1)`. The fuzz test exercises:
- Variable numbers of input sets
- Overlapping and disjoint member populations
- Edge cases (empty sets, single-element sets)

### Hash Fuzzing

The Redis `Hash fuzzing #1` and `#2` tests exercise hash operations (HSET, HGET, HDEL, HLEN,
HGETALL, etc.) with randomized field names and values across both listpack and hashtable
encodings. The `#1` and `#2` variants typically differ by hash size (small = listpack encoding,
large = hashtable encoding).

FrogDB's hash commands are in `frogdb-server/crates/commands/src/hash.rs`. The listpack
threshold is controlled by `ListpackThresholds` in the core crate.

## Implementation Steps

1. Port the SDIFF fuzz test logic:
   - Generate N sets with random members (strings and integers)
   - Compute expected diff in Rust (reference implementation using `HashSet`)
   - Compare against SDIFF command output
   - Run for many iterations with varying set sizes

2. Port hash fuzz tests:
   - Generate random field/value pairs
   - Apply HSET/HDEL operations randomly
   - Verify HGETALL/HLEN/HEXISTS remain consistent
   - Test with sizes below and above listpack threshold

## Integration Points

- `frogdb-server/crates/commands/src/set.rs` — `SdiffCommand` implementation
- `frogdb-server/crates/commands/src/hash.rs` — all hash commands
- `frogdb-server/crates/core/src/` — `ListpackThresholds` for encoding transitions
- Regression test harness for random seed management and reproducibility

## FrogDB Adaptations

- Encoding transitions: FrogDB uses the same listpack-to-hashtable promotion as Redis. Fuzz
  tests should verify correct behavior across the boundary.
- No structural differences — these are pure correctness tests.

## Tests

- `SDIFF fuzzing` — randomized set difference correctness
- `Hash fuzzing #1 - $size fields` — small hash (listpack encoding) correctness
- `Hash fuzzing #2 - $size fields` — large hash (hashtable encoding) correctness

## Verification

```bash
just test frogdb-server-redis-regression "SDIFF fuzzing"
just test frogdb-server-redis-regression "Hash fuzzing"
```
