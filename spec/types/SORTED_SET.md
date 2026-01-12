# FrogDB Sorted Set Commands

Members ordered by score with O(log N) operations. Sorted sets combine the uniqueness of sets with ordering by floating-point scores, enabling efficient range queries and ranking operations.

## Data Structure

```rust
pub struct SortedSetValue {
    /// Member -> Score for O(1) score lookup
    members: HashMap<Bytes, f64>,
    /// (Score, Member) for range queries - ordered by score, then lexicographically
    scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
}
```

**Dual Index Design:**
- `members` HashMap provides O(1) score lookup by member
- `scores` BTreeMap provides O(log N) range queries by score
- Both structures must be kept in sync on mutations

**Alternative Implementation:**
Skip list could provide better cache locality during traversal, matching Redis's internal implementation.

---

## Commands

| Command | Complexity | Description |
|---------|------------|-------------|
| ZADD | O(log N) | Add members with scores |
| ZREM | O(M log N) | Remove members |
| ZSCORE | O(1) | Get score of member |
| ZMSCORE | O(N) | Get scores of multiple members |
| ZRANK | O(log N) | Get rank (ascending) |
| ZREVRANK | O(log N) | Get rank (descending) |
| ZRANGE | O(log N + M) | Get range by rank |
| ZREVRANGE | O(log N + M) | Get range by rank (descending) |
| ZRANGEBYSCORE | O(log N + M) | Get range by score |
| ZREVRANGEBYSCORE | O(log N + M) | Get range by score (descending) |
| ZRANGEBYLEX | O(log N + M) | Get range lexicographically |
| ZREVRANGEBYLEX | O(log N + M) | Get range lexicographically (descending) |
| ZCARD | O(1) | Get cardinality |
| ZCOUNT | O(log N) | Count members in score range |
| ZLEXCOUNT | O(log N) | Count members in lex range |
| ZINCRBY | O(log N) | Increment member's score |
| ZPOPMIN | O(log N * M) | Pop members with lowest scores |
| ZPOPMAX | O(log N * M) | Pop members with highest scores |
| BZPOPMIN | O(log N) | Blocking pop min |
| BZPOPMAX | O(log N) | Blocking pop max |
| ZMPOP | O(K) + O(M*log N) | Pop from multiple sorted sets |
| BZMPOP | O(K) + O(M*log N) | Blocking pop from multiple |
| ZUNION | O(N) + O(M log M) | Union of sorted sets |
| ZUNIONSTORE | O(N) + O(M log M) | Union and store |
| ZINTER | O(N*K) + O(M log M) | Intersection of sorted sets |
| ZINTERSTORE | O(N*K) + O(M log M) | Intersection and store |
| ZINTERCARD | O(N*K) | Intersection cardinality |
| ZDIFF | O(L + (N-K) log N) | Difference of sorted sets |
| ZDIFFSTORE | O(L + (N-K) log N) | Difference and store |
| ZRANGESTORE | O(log N + M) | Store range result |
| ZSCAN | O(1)/call | Iterate members |
| ZRANDMEMBER | O(N) | Get random member(s) |

Where: N = sorted set size, M = result count, K = number of input sets

---

## Command Details

### ZADD

Add members with scores to sorted set.

```
ZADD key [NX | XX] [GT | LT] [CH] [INCR] score member [score member ...]
```

| Option | Meaning |
|--------|---------|
| NX | Only add new members (don't update existing) |
| XX | Only update existing members (don't add new) |
| GT | Only update if new score > current score |
| LT | Only update if new score < current score |
| CH | Return count of changed members (added + updated) instead of only added |
| INCR | Increment score instead of setting (returns new score, single member only) |

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: members added (or changed if CH) |
| With INCR | Bulk string: new score (single member only) |
| Duplicate members | Later score wins |
| Score range | IEEE 754 double precision |

**Special scores:**
- `+inf` / `-inf` for infinity
- Scores are compared as floating point numbers

**Examples:**
```
> ZADD leaderboard 100 "alice" 200 "bob" 150 "charlie"
(integer) 3

> ZADD leaderboard XX 250 "bob"
(integer) 0

> ZADD leaderboard NX 300 "bob" 50 "dave"
(integer) 1

> ZADD leaderboard GT 180 "charlie"
(integer) 0

> ZADD leaderboard CH 180 "charlie"
(integer) 1

> ZADD leaderboard INCR 10 "alice"
"110"
```

### ZREM

Remove members from sorted set.

```
ZREM key member [member ...]
```

Returns: Integer count of members removed (ignores non-existent).

### ZSCORE

Get score of a member.

```
ZSCORE key member
```

Returns: Bulk string score, or nil if member doesn't exist.

### ZMSCORE

Get scores of multiple members.

```
ZMSCORE key member [member ...]
```

Returns: Array of scores (nil for non-existent members).

### ZRANK / ZREVRANK

Get 0-based rank of member.

```
ZRANK key member [WITHSCORE]
ZREVRANK key member [WITHSCORE]
```

| Command | Ordering |
|---------|----------|
| ZRANK | Lowest score = rank 0 |
| ZREVRANK | Highest score = rank 0 |

Returns: Integer rank, or nil if member doesn't exist. With WITHSCORE, returns array [rank, score].

### ZRANGE

Unified range command (Redis 6.2+).

```
ZRANGE key start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
```

| Mode | start/stop meaning |
|------|-------------------|
| (default) | 0-based rank indexes |
| BYSCORE | Score range (use `-inf`, `+inf`, `(exclusive`) |
| BYLEX | Lex range (use `-`, `+`, `[inclusive`, `(exclusive`) |

| Option | Meaning |
|--------|---------|
| REV | Reverse order (high to low) |
| LIMIT | Offset and count for pagination |
| WITHSCORES | Include scores in response |

**Examples:**
```
> ZADD myset 1 "one" 2 "two" 3 "three"
(integer) 3

> ZRANGE myset 0 -1
1) "one"
2) "two"
3) "three"

> ZRANGE myset 0 -1 WITHSCORES
1) "one"
2) "1"
3) "two"
4) "2"
5) "three"
6) "3"

> ZRANGE myset 0 -1 REV
1) "three"
2) "two"
3) "one"

> ZRANGE myset 1 100 BYSCORE
1) "one"
2) "two"
3) "three"

> ZRANGE myset (1 3 BYSCORE
1) "two"
2) "three"

> ZRANGE myset 0 -1 REV LIMIT 0 2
1) "three"
2) "two"
```

### ZRANGEBYSCORE / ZREVRANGEBYSCORE

Get members by score range (legacy commands, prefer ZRANGE BYSCORE).

```
ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
```

**Score range syntax:**
- `1.5` - inclusive
- `(1.5` - exclusive (greater than)
- `-inf` / `+inf` - infinity

### ZRANGEBYLEX / ZREVRANGEBYLEX

Get members lexicographically (requires all same scores).

```
ZRANGEBYLEX key min max [LIMIT offset count]
ZREVRANGEBYLEX key max min [LIMIT offset count]
```

**Lex range syntax:**
- `[value` - inclusive
- `(value` - exclusive
- `-` / `+` - negative/positive infinity

### ZCARD

Get number of members.

```
ZCARD key
```

Returns: Integer cardinality, or `0` if key doesn't exist.

### ZCOUNT

Count members in score range.

```
ZCOUNT key min max
```

Uses same range syntax as ZRANGEBYSCORE.

### ZLEXCOUNT

Count members in lexicographic range.

```
ZLEXCOUNT key min max
```

Uses same range syntax as ZRANGEBYLEX.

### ZINCRBY

Increment member's score.

```
ZINCRBY key increment member
```

| Aspect | Behavior |
|--------|----------|
| Returns | Bulk string: new score |
| Non-existent member | Created with increment as score |
| Non-existent key | Created with single member |

### ZPOPMIN / ZPOPMAX

Remove and return members with lowest/highest scores.

```
ZPOPMIN key [count]
ZPOPMAX key [count]
```

Returns: Array of [member, score] pairs.

### BZPOPMIN / BZPOPMAX

Blocking pop - wait for members if sorted set is empty.

```
BZPOPMIN key [key ...] timeout
BZPOPMAX key [key ...] timeout
```

| Aspect | Behavior |
|--------|----------|
| Returns | Array: [key, member, score] or nil on timeout |
| Timeout | Seconds (0 = wait forever) |
| Multiple keys | First non-empty wins |

**Note:** Blocking commands require special handling - see [CONCURRENCY.md](../CONCURRENCY.md).

### ZMPOP / BZMPOP

Pop from multiple sorted sets.

```
ZMPOP numkeys key [key ...] MIN | MAX [COUNT count]
BZMPOP timeout numkeys key [key ...] MIN | MAX [COUNT count]
```

Returns: Array [key, [[member, score], ...]] or nil.

### Set Operations: ZUNION, ZINTER, ZDIFF

Compute set operations across multiple sorted sets.

```
ZUNION numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM | MIN | MAX] [WITHSCORES]
ZINTER numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM | MIN | MAX] [WITHSCORES]
ZDIFF numkeys key [key ...]
```

| Option | Meaning |
|--------|---------|
| WEIGHTS | Multiply scores by weight per input set |
| AGGREGATE | How to combine scores (default: SUM) |

**Store variants:** ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE store result in destination key.

### ZINTERCARD

Get cardinality of intersection without computing full result.

```
ZINTERCARD numkeys key [key ...] [LIMIT limit]
```

Returns: Integer count (stops early if LIMIT reached).

### ZRANGESTORE

Store range result in destination key.

```
ZRANGESTORE dst src start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count]
```

Returns: Integer count of members stored.

### ZSCAN

Iterate sorted set members.

```
ZSCAN key cursor [MATCH pattern] [COUNT hint]
```

Returns: [next_cursor, [member, score, member, score, ...]]

### ZRANDMEMBER

Get random member(s).

```
ZRANDMEMBER key [count [WITHSCORES]]
```

| count | Behavior |
|-------|----------|
| Positive | Up to count distinct members |
| Negative | Exactly |count| members, may repeat |

---

## Cross-Shard Behavior

### Single-Key Commands

All single-key sorted set commands (ZADD, ZRANGE, ZSCORE, etc.) are routed directly to the owning shard.

### Multi-Key Commands

| Command | Behavior |
|---------|----------|
| ZUNION/ZINTER/ZDIFF | **Requires same shard** - use hash tags |
| ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE | **Requires same shard** for all keys |
| BZPOPMIN/BZPOPMAX | **Requires same shard** for all keys |
| ZMPOP/BZMPOP | **Requires same shard** for all keys |

### Hash Tags for Set Operations

To perform set operations across multiple sorted sets, all keys must be on the same shard:

```
ZUNIONSTORE {leaderboard}:combined 2 {leaderboard}:week1 {leaderboard}:week2
```

---

## Persistence

All sorted set operations follow FrogDB's unified persistence model:
- WAL writes for durability (ZADD, ZREM, ZINCRBY, etc.)
- Both indexes (members HashMap + scores BTreeMap) reconstructed from persisted data
- Score precision preserved across crashes

See [PERSISTENCE.md](../PERSISTENCE.md) for configuration.

---

## Implementation Notes

### Score Handling

```rust
use ordered_float::OrderedFloat;

// Scores stored as OrderedFloat for correct BTreeMap ordering
// Handles: NaN comparison, -0.0 vs 0.0, infinity
```

### Memory Overhead

Per sorted set:
- ~80 bytes base overhead
- Per member: key bytes + 8 bytes (f64 score) + HashMap/BTreeMap entry overhead (~48 bytes)

### Crate Dependencies

```toml
ordered-float = "4.2"   # Orderable f64 for BTreeMap keys
bytes = "1.5"           # Efficient byte buffers
```

### Use Cases

- **Leaderboards**: ZADD for scores, ZREVRANGE for top N
- **Priority queues**: ZADD with timestamp scores, ZPOPMIN for oldest
- **Rate limiting**: ZADD with timestamps, ZREMRANGEBYSCORE to expire old entries
- **Time series**: Score = timestamp, member = event ID
