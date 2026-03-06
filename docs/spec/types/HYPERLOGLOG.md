# FrogDB HyperLogLog Commands

Probabilistic cardinality estimation with fixed 12KB memory. HyperLogLog (HLL) counts unique elements with ~0.81% standard error, regardless of cardinality. Ideal for counting unique visitors, IPs, or events where approximate counts are acceptable.

## Data Structure

```rust
pub struct FrogHyperLogLog {
    /// 16384 6-bit registers (12KB dense) or sparse representation
    representation: HllRepresentation,
}

enum HllRepresentation {
    /// Sparse: list of (register_index, value) pairs
    /// Used when < ~3000 registers are set
    Sparse(Vec<(u16, u8)>),

    /// Dense: fixed 16384 registers, 6 bits each = 12288 bytes
    Dense([u8; 12288]),
}
```

**Algorithm Properties:**
- 16384 registers (2^14) with 6 bits each
- Uses first 14 bits of hash for register index
- Remaining bits determine register value (leading zeros + 1)
- Harmonic mean of register estimates gives cardinality

**Memory:**
- Sparse: ~3 bytes per unique register (efficient for small sets)
- Dense: Fixed 12KB regardless of cardinality
- Auto-promotes sparse to dense at ~3000 elements

---

## Commands

| Command | Complexity | Description |
|---------|------------|-------------|
| PFADD | O(1) | Add elements |
| PFCOUNT | O(1) or O(N) | Get cardinality estimate |
| PFMERGE | O(N) | Merge multiple HLLs |
| PFDEBUG | O(N) | Debug info (internal) |
| PFSELFTEST | O(N) | Run self-test (internal) |

---

## Command Details

### PFADD

Add elements to HyperLogLog.

```
PFADD key element [element ...]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: 1 if cardinality estimate changed, 0 otherwise |
| New key | Created with sparse representation |
| Duplicate elements | Idempotent (no effect on estimate) |

**Examples:**
```
> PFADD visitors user:1 user:2 user:3
(integer) 1

> PFADD visitors user:1 user:2
(integer) 0

> PFADD visitors user:4
(integer) 1
```

**Note:** Return value indicates if *internal registers* changed, not exact cardinality change.

### PFCOUNT

Get estimated cardinality.

```
PFCOUNT key [key ...]
```

| Aspect | Behavior |
|--------|----------|
| Single key | O(1) - returns cached estimate |
| Multiple keys | O(N) - computes union estimate on-the-fly |
| Non-existent key | Returns 0 |
| Accuracy | Standard error 0.81% |

**Examples:**
```
> PFADD day1 user:1 user:2 user:3
(integer) 1
> PFADD day2 user:2 user:3 user:4 user:5
(integer) 1

> PFCOUNT day1
(integer) 3

> PFCOUNT day2
(integer) 4

> PFCOUNT day1 day2
(integer) 5
```

**Multi-key behavior:** Computes union cardinality without modifying keys. Equivalent to merging then counting, but non-destructive.

### PFMERGE

Merge multiple HyperLogLogs into destination.

```
PFMERGE destkey sourcekey [sourcekey ...]
```

| Aspect | Behavior |
|--------|----------|
| Returns | OK |
| Merge algorithm | Per-register MAX |
| Destination | Created/overwritten |
| Empty sources | Treated as 0-cardinality HLL |

**Examples:**
```
> PFADD week1 user:1 user:2 user:3
> PFADD week2 user:3 user:4 user:5
> PFADD week3 user:5 user:6

> PFMERGE month week1 week2 week3
OK

> PFCOUNT month
(integer) 6
```

**Use case:** Aggregate hourly/daily counts into weekly/monthly totals.

---

## Cross-Shard Behavior

### Single-Key Commands

PFADD and single-key PFCOUNT route to owning shard.

### Multi-Key Commands

| Command | Constraint |
|---------|------------|
| PFCOUNT (multi-key) | All keys must be on same shard |
| PFMERGE | All keys (including dest) must be on same shard |

**Hash tag requirement:**
```
PFMERGE {analytics}:monthly {analytics}:week1 {analytics}:week2 {analytics}:week3 {analytics}:week4
```

---

## Persistence

HyperLogLog persists as opaque binary:
- Sparse representation: variable-length encoding
- Dense representation: raw 12KB bytes
- Header byte indicates representation type

---

## Implementation Notes

### HyperLogLog Algorithm

```rust
/// Add element to HLL
pub fn pfadd(&mut self, element: &[u8]) -> bool {
    let hash = murmur_hash64a(element);

    // First 14 bits = register index (0-16383)
    let index = (hash & 0x3FFF) as u16;

    // Remaining 50 bits = count leading zeros + 1
    let remaining = hash >> 14;
    let zeros = remaining.leading_zeros() as u8;
    let value = zeros.min(63) + 1; // Cap at 6 bits (63)

    self.update_register(index, value)
}

/// Estimate cardinality
pub fn pfcount(&self) -> u64 {
    let registers = self.get_all_registers();
    let m = 16384.0;

    // Harmonic mean of 2^register_value
    let sum: f64 = registers.iter()
        .map(|&r| 2.0_f64.powi(-(r as i32)))
        .sum();

    let raw_estimate = ALPHA_16384 * m * m / sum;

    // Apply bias correction for small/large cardinalities
    apply_corrections(raw_estimate, &registers)
}

const ALPHA_16384: f64 = 0.7213 / (1.0 + 1.079 / 16384.0);
```

### Sparse-to-Dense Promotion

```rust
impl FrogHyperLogLog {
    fn maybe_promote(&mut self) {
        if let HllRepresentation::Sparse(ref pairs) = self.representation {
            // Promote when sparse encoding exceeds dense size
            // or when register count > ~3000
            if pairs.len() * 3 > 12288 || pairs.len() > 3000 {
                self.promote_to_dense();
            }
        }
    }
}
```

### Serialization Format

```
+--------+------------------+
| Header | Payload          |
| 1 byte | variable/12288B  |
+--------+------------------+

Header bits:
  [7:4] = 0 (reserved)
  [3]   = 1 if dense, 0 if sparse
  [2:0] = version (currently 0)

Sparse payload: variable-length list of (u16 index, u8 value)
Dense payload: 12288 bytes, 2 registers per 1.5 bytes
```

### Crate Dependencies

```toml
# Using Murmur3 for Redis-compatible hashing
murmur3 = "0.5"
```

### Error Analysis

| Cardinality | Expected Error | 99% Confidence |
|-------------|---------------|----------------|
| 100 | +/-0.81 | +/-2.1 |
| 1,000 | +/-8.1 | +/-21 |
| 1,000,000 | +/-8,100 | +/-21,000 |
| 1,000,000,000 | +/-8,100,000 | +/-21,000,000 |

### Use Cases

- **Unique visitors**: PFADD page:home <user_id>
- **Unique search queries**: PFADD queries:daily <query_hash>
- **Distinct IPs**: PFADD ips:hourly <ip>
- **Event deduplication**: Count unique events over time windows
- **Cardinality monitoring**: Track growth of unique items
