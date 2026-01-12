# FrogDB Bitmap Commands

Bit-level operations on strings. Bitmaps are not a distinct data type - they operate on `FrogString` values, treating them as bit arrays. This enables memory-efficient storage of binary flags, presence indicators, and compact integer arrays.

## Data Structure

No new `FrogValue` variant required. Bitmap operations work directly on `FrogString`:

```rust
// Bitmaps use existing FrogString
// Internally: Vec<u8> treated as bit array
// Bit 0 is MSB of byte 0, bit 7 is LSB of byte 0, bit 8 is MSB of byte 1, etc.

impl FrogString {
    /// Get bit at offset (0-indexed from MSB of first byte)
    pub fn get_bit(&self, offset: u64) -> u8;

    /// Set bit at offset, auto-extending with zeros if needed
    pub fn set_bit(&mut self, offset: u64, value: u8) -> u8; // returns old value

    /// Count bits set to 1 in byte range
    pub fn bit_count(&self, start: i64, end: i64, unit: BitUnit) -> u64;

    /// Find first bit set to value in range
    pub fn bit_pos(&self, value: u8, start: i64, end: Option<i64>, unit: BitUnit) -> Option<i64>;
}

pub enum BitUnit {
    Byte,  // Range in bytes (default)
    Bit,   // Range in bits (Redis 7.0+)
}
```

**Bit Ordering (Redis-compatible):**
- Offset 0 = bit 7 of byte 0 (MSB)
- Offset 7 = bit 0 of byte 0 (LSB)
- Offset 8 = bit 7 of byte 1 (MSB)

**Auto-Extension:**
Setting a bit beyond current length zero-extends the string to accommodate the offset.

---

## Commands

| Command | Complexity | Description |
|---------|------------|-------------|
| SETBIT | O(1) | Set or clear bit at offset |
| GETBIT | O(1) | Get bit value at offset |
| BITCOUNT | O(N) | Count bits set to 1 |
| BITOP | O(N) | Bitwise AND, OR, XOR, NOT |
| BITPOS | O(N) | Find first bit with given value |
| BITFIELD | O(1)/sub | Read/write arbitrary bit-width integers |
| BITFIELD_RO | O(1)/sub | Read-only BITFIELD variant |

Where N = string length in bytes.

---

## Command Details

### SETBIT

Set or clear the bit at offset.

```
SETBIT key offset value
```

| Parameter | Description |
|-----------|-------------|
| offset | Bit position (0-indexed), max 2^32-1 |
| value | 0 or 1 |

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: previous bit value (0 or 1) |
| Non-existent key | Created as empty string, then extended |
| Offset beyond length | String zero-extended to fit |
| Memory | Allocates bytes as needed (offset/8 + 1 bytes) |

**Examples:**
```
> SETBIT flags 7 1
(integer) 0

> SETBIT flags 0 1
(integer) 0

> GET flags
"\x81"

> SETBIT flags 100 1
(integer) 0
```

**Warning:** Setting a high offset (e.g., 2^30) allocates ~128MB. FrogDB should enforce `maxmemory` limits.

### GETBIT

Get the bit value at offset.

```
GETBIT key offset
```

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: 0 or 1 |
| Non-existent key | Returns 0 |
| Offset beyond length | Returns 0 |

### BITCOUNT

Count bits set to 1.

```
BITCOUNT key [start end [BYTE | BIT]]
```

| Parameter | Description |
|-----------|-------------|
| start/end | Range (negative = from end, like GETRANGE) |
| BYTE | Interpret range as byte offsets (default) |
| BIT | Interpret range as bit offsets (Redis 7.0+) |

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: count of 1-bits |
| No range | Counts entire string |
| Non-existent key | Returns 0 |

**Examples:**
```
> SET mykey "\xff\xf0\x00"
OK

> BITCOUNT mykey
(integer) 12

> BITCOUNT mykey 0 0
(integer) 8

> BITCOUNT mykey 1 1
(integer) 4

> BITCOUNT mykey 0 11 BIT
(integer) 12
```

**Implementation:** Use POPCNT instruction where available for O(N/64) effective complexity.

### BITOP

Perform bitwise operations between strings.

```
BITOP <AND | OR | XOR | NOT> destkey key [key ...]
```

| Operation | Keys | Description |
|-----------|------|-------------|
| AND | 1+ | Bitwise AND of all keys |
| OR | 1+ | Bitwise OR of all keys |
| XOR | 1+ | Bitwise XOR of all keys |
| NOT | 1 | Bitwise NOT (invert all bits) |

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: length of result string in bytes |
| Different lengths | Shorter strings treated as zero-padded |
| Result length | Length of longest input string |
| Non-existent keys | Treated as empty string (all zeros) |

**Examples:**
```
> SET a "\xff\x0f"
OK
> SET b "\x0f\xff"
OK

> BITOP AND result a b
(integer) 2
> GET result
"\x0f\x0f"

> BITOP OR result a b
(integer) 2
> GET result
"\xff\xff"

> BITOP NOT inverted a
(integer) 2
> GET inverted
"\x00\xf0"
```

### BITPOS

Find first bit set to specified value.

```
BITPOS key bit [start [end [BYTE | BIT]]]
```

| Parameter | Description |
|-----------|-------------|
| bit | 0 or 1 (which bit value to find) |
| start/end | Optional byte/bit range |
| BYTE/BIT | Range unit (default: BYTE) |

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: bit position, or -1 if not found |
| Empty string | -1 for bit=1, 0 for bit=0 |
| All 1s, find 0 | Returns position after last byte (conceptual 0-padding) |
| Range limits search | Only searches within range |

**Examples:**
```
> SET mykey "\x00\xff\xf0"
OK

> BITPOS mykey 1
(integer) 8

> BITPOS mykey 0
(integer) 0

> BITPOS mykey 1 2
(integer) 16

> BITPOS mykey 0 0 0
(integer) 0
```

### BITFIELD

Treat string as array of integers with arbitrary bit widths.

```
BITFIELD key [GET encoding offset] [SET encoding offset value]
             [INCRBY encoding offset increment] [OVERFLOW WRAP|SAT|FAIL] ...
```

**Encoding format:**
- `i<bits>` - Signed integer (1-64 bits)
- `u<bits>` - Unsigned integer (1-63 bits, u64 not supported)

**Offset format:**
- `N` - Literal bit offset
- `#N` - Multiplied offset (N * encoding width)

| Subcommand | Description |
|------------|-------------|
| GET | Read integer at offset |
| SET | Write integer at offset, return old value |
| INCRBY | Increment integer at offset, return new value |
| OVERFLOW | Set overflow behavior for subsequent operations |

**Overflow modes:**

| Mode | Behavior |
|------|----------|
| WRAP | Wrap around (default) - like C unsigned overflow |
| SAT | Saturate at min/max value |
| FAIL | Return nil, don't modify |

**Examples:**
```
> BITFIELD counters SET u8 0 200
1) (integer) 0

> BITFIELD counters GET u8 0
1) (integer) 200

> BITFIELD counters INCRBY u8 0 100
1) (integer) 44

> BITFIELD counters OVERFLOW SAT INCRBY u8 0 200
1) (integer) 244

> BITFIELD counters OVERFLOW SAT INCRBY u8 0 200
1) (integer) 255

> BITFIELD mykey SET u4 #0 15 SET u4 #1 14 GET u4 #0 GET u4 #1
1) (integer) 0
2) (integer) 0
3) (integer) 15
4) (integer) 14
```

**Use cases:**
- Compact counters (e.g., 4-bit counters for 16 values per byte)
- Fixed-width integer arrays without JSON/msgpack overhead
- Flags with multi-bit states

### BITFIELD_RO

Read-only variant of BITFIELD (only GET subcommand allowed).

```
BITFIELD_RO key GET encoding offset [GET encoding offset ...]
```

Useful for read replicas or when write access shouldn't be granted.

---

## Cross-Shard Behavior

### Single-Key Commands

SETBIT, GETBIT, BITCOUNT, BITPOS, BITFIELD route directly to owning shard.

### Multi-Key Commands

| Command | Constraint |
|---------|------------|
| BITOP | All keys (including destkey) must be on same shard |

**Hash tag requirement for BITOP:**
```
BITOP AND {user:1}:result {user:1}:login_jan {user:1}:login_feb
```

---

## Persistence

Bitmap operations persist like any string modification:
- SETBIT, BITFIELD SET/INCRBY trigger WAL write
- Value stored as raw bytes in RocksDB
- No special encoding - just the string bytes

---

## Implementation Notes

### Bit Ordering Diagram

```
Byte:    [    0    ] [    1    ] [    2    ]
Bits:    7654 3210   7654 3210   7654 3210
Offset:  0123 4567   8... ...15  16.. ...23
```

### Memory Efficiency

| Use Case | Bits Needed | Bytes Used |
|----------|-------------|------------|
| 1000 users online | 1000 | 125 |
| Daily active users (1M) | 1,000,000 | 125,000 |
| Feature flags (64 flags/user) | 64 per user | 8 per user |

### POPCNT Optimization

```rust
fn bit_count_fast(bytes: &[u8]) -> u64 {
    // Process 8 bytes at a time using u64::count_ones()
    let mut count = 0u64;
    let chunks = bytes.chunks_exact(8);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let val = u64::from_ne_bytes(chunk.try_into().unwrap());
        count += val.count_ones() as u64;
    }

    for &byte in remainder {
        count += byte.count_ones() as u64;
    }

    count
}
```

### Crate Dependencies

```toml
# No additional dependencies - uses standard library bit operations
```

### Use Cases

- **User presence**: SETBIT online_users <user_id> 1
- **Feature flags**: BITFIELD flags SET u2 #<user_id> <flag_value>
- **Daily active users**: SETBIT dau:2024-01-15 <user_id> 1, BITCOUNT to get total
- **Bloom filter (manual)**: Multiple SETBIT calls with hash positions
- **Rate limiting**: Sliding window with bit per time unit
