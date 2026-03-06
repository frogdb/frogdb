# FrogDB Bloom Filter Commands

Probabilistic set membership testing with configurable false positive rate. Bloom filters answer "is X in set?" with possible false positives but no false negatives. Space-efficient for large sets where approximate answers are acceptable.

Based on RedisBloom module semantics.

## Data Structure

```rust
pub struct FrogBloomFilter {
    /// Bit array
    bits: BitVec,
    /// Number of hash functions
    num_hashes: u32,
    /// Number of items added
    items_added: u64,
    /// Configured capacity
    capacity: u64,
    /// Target false positive rate
    error_rate: f64,
}

// For scalable bloom filters
pub struct FrogScalableBloom {
    filters: Vec<FrogBloomFilter>,
    growth_rate: u8,
    initial_capacity: u64,
    error_rate: f64,
}
```

**Bloom Filter Properties:**
- No false negatives: if returns false, definitely not in set
- Possible false positives: if returns true, probably in set
- Cannot remove items (use Cuckoo filter for that)
- Size fixed at creation based on capacity and error rate

**Size Calculation:**
```
bits = -1 * (capacity * ln(error_rate)) / (ln(2)^2)
hashes = (bits / capacity) * ln(2)
```

---

## Commands

| Command | Complexity | Description |
|---------|------------|-------------|
| BF.RESERVE | O(1) | Create filter with capacity |
| BF.ADD | O(K) | Add item |
| BF.MADD | O(K*N) | Add multiple items |
| BF.EXISTS | O(K) | Check if item exists |
| BF.MEXISTS | O(K*N) | Check multiple items |
| BF.INSERT | O(K*N) | Add with auto-create options |
| BF.INFO | O(1) | Get filter info |
| BF.CARD | O(1) | Estimated cardinality |
| BF.SCANDUMP | O(N) | Incremental save |
| BF.LOADCHUNK | O(N) | Incremental load |

Where K = number of hash functions, N = number of items.

---

## Command Details

### BF.RESERVE

Create new bloom filter with specified parameters.

```
BF.RESERVE key error_rate capacity [EXPANSION expansion] [NONSCALING]
```

| Parameter | Description |
|-----------|-------------|
| error_rate | Desired false positive rate (0-1) |
| capacity | Expected number of items |
| EXPANSION | Growth rate for scalable filters (default: 2) |
| NONSCALING | Don't create sub-filters when full |

| Aspect | Behavior |
|--------|----------|
| Returns | OK |
| Key exists | Error |
| Error rate | Lower = more memory |

**Examples:**
```
> BF.RESERVE usernames 0.001 100000
OK

> BF.RESERVE emails 0.01 1000000 EXPANSION 4
OK

> BF.RESERVE fixed 0.001 10000 NONSCALING
OK
```

**Memory estimation:**

| Capacity | Error Rate | Memory |
|----------|------------|--------|
| 100,000 | 1% | ~120KB |
| 100,000 | 0.1% | ~180KB |
| 1,000,000 | 1% | ~1.2MB |
| 1,000,000 | 0.1% | ~1.8MB |

### BF.ADD

Add item to bloom filter.

```
BF.ADD key item
```

| Aspect | Behavior |
|--------|----------|
| Returns | 1 if newly added, 0 if probably existed |
| Non-existent key | Auto-creates with defaults |
| Full filter | Creates sub-filter (if scaling) |

**Examples:**
```
> BF.ADD users alice
(integer) 1

> BF.ADD users alice
(integer) 0

> BF.ADD users bob
(integer) 1
```

### BF.MADD

Add multiple items.

```
BF.MADD key item [item ...]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Array of 1/0 per item |

### BF.EXISTS

Check if item may exist.

```
BF.EXISTS key item
```

| Aspect | Behavior |
|--------|----------|
| Returns | 1 if probably exists, 0 if definitely not |
| Non-existent key | Returns 0 |

**Examples:**
```
> BF.ADD users alice
(integer) 1

> BF.EXISTS users alice
(integer) 1

> BF.EXISTS users charlie
(integer) 0
```

### BF.MEXISTS

Check multiple items.

```
BF.MEXISTS key item [item ...]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Array of 1/0 per item |

### BF.INSERT

Add items with auto-creation options.

```
BF.INSERT key [CAPACITY cap] [ERROR error] [EXPANSION exp] [NOCREATE] [NONSCALING] ITEMS item [item ...]
```

| Option | Description |
|--------|-------------|
| CAPACITY | Capacity if creating |
| ERROR | Error rate if creating |
| NOCREATE | Error if key doesn't exist |
| NONSCALING | Don't scale when full |

**Examples:**
```
> BF.INSERT myfilter CAPACITY 10000 ERROR 0.001 ITEMS foo bar baz
1) (integer) 1
2) (integer) 1
3) (integer) 1
```

### BF.INFO

Get filter information.

```
BF.INFO key [CAPACITY | SIZE | FILTERS | ITEMS | EXPANSION]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Array of field-value pairs |
| Fields | Capacity, Size, Number of filters, Items inserted, Expansion rate |

**Examples:**
```
> BF.INFO users
 1) Capacity
 2) (integer) 100000
 3) Size
 4) (integer) 120384
 5) Number of filters
 6) (integer) 1
 7) Number of items inserted
 8) (integer) 2
 9) Expansion rate
10) (integer) 2
```

### BF.CARD

Get estimated cardinality.

```
BF.CARD key
```

| Aspect | Behavior |
|--------|----------|
| Returns | Estimated number of unique items added |
| Algorithm | Based on bit fill ratio |

### BF.SCANDUMP

Incrementally save filter.

```
BF.SCANDUMP key iterator
```

| Aspect | Behavior |
|--------|----------|
| Returns | [next_iterator, data_chunk] |
| Iterator 0 | Start new dump |
| Complete | Returns iterator 0 |

### BF.LOADCHUNK

Incrementally load filter.

```
BF.LOADCHUNK key iterator data
```

| Aspect | Behavior |
|--------|----------|
| Returns | OK |
| Usage | Pair with BF.SCANDUMP for replication |

---

## Cross-Shard Behavior

All bloom filter commands are single-key operations routed to owning shard.

---

## Persistence

Bloom filters persist as binary:
- Header: capacity, error_rate, num_hashes, items_added
- Payload: raw bit array
- Scalable: array of filter headers + payloads

---

## Implementation Notes

### Hash Functions

```rust
/// Generate K hash values using double hashing
fn bloom_hashes(item: &[u8], k: u32, m: u64) -> impl Iterator<Item = u64> {
    let hash1 = murmur_hash64a(item);
    let hash2 = xxhash64(item);

    (0..k).map(move |i| {
        (hash1.wrapping_add((i as u64).wrapping_mul(hash2))) % m
    })
}
```

### Optimal Parameters

```rust
fn optimal_params(capacity: u64, error_rate: f64) -> (u64, u32) {
    let ln2 = std::f64::consts::LN_2;
    let ln2_sq = ln2 * ln2;

    // Optimal number of bits
    let bits = (-(capacity as f64) * error_rate.ln() / ln2_sq).ceil() as u64;

    // Optimal number of hash functions
    let hashes = ((bits as f64 / capacity as f64) * ln2).round() as u32;

    (bits, hashes.max(1))
}
```

### Scalable Bloom Filter

```rust
impl FrogScalableBloom {
    fn add(&mut self, item: &[u8]) -> bool {
        // Check all existing filters
        for filter in &self.filters {
            if filter.probably_contains(item) {
                return false; // Already exists
            }
        }

        // Add to last filter, create new if full
        let last = self.filters.last_mut().unwrap();
        if last.is_full() {
            let new_capacity = last.capacity * self.growth_rate as u64;
            let new_error = last.error_rate * 0.5; // Tighter for later filters
            self.filters.push(FrogBloomFilter::new(new_capacity, new_error));
        }

        self.filters.last_mut().unwrap().add(item)
    }
}
```

### Crate Dependencies

```toml
bitvec = "1.0"      # Efficient bit vector
murmur3 = "0.5"     # Hash function
xxhash-rust = "0.8" # Second hash function
```

### Use Cases

- **Username availability**: Quick check before DB query
- **Cache lookup**: Avoid DB hit for missing items
- **Duplicate detection**: Detect seen URLs/emails
- **Spam filtering**: Known bad patterns
- **Recommendation filtering**: Exclude already-seen items
