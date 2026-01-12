# FrogDB Time Series Commands

Time series data storage optimized for metrics, events, and sensor data. Supports automatic downsampling, retention policies, and aggregation queries. Each time series stores timestamp-value pairs with optional labels for filtering.

Based on RedisTimeSeries module semantics.

## Data Structure

```rust
pub struct FrogTimeSeries {
    /// Sorted samples by timestamp
    samples: BTreeMap<i64, f64>,  // timestamp_ms -> value
    /// Labels for filtering/grouping
    labels: HashMap<String, String>,
    /// Retention period in milliseconds (0 = forever)
    retention_ms: u64,
    /// Duplicate policy
    duplicate_policy: DuplicatePolicy,
    /// Chunk size for compression
    chunk_size: usize,
}

pub enum DuplicatePolicy {
    Block,     // Error on duplicate timestamp
    First,     // Keep first value
    Last,      // Keep last value (default)
    Min,       // Keep minimum value
    Max,       // Keep maximum value
    Sum,       // Sum values
}

pub struct DownsampleRule {
    dest_key: Bytes,
    aggregation: Aggregation,
    bucket_duration_ms: u64,
}

pub enum Aggregation {
    Avg, Sum, Min, Max, Range, Count, First, Last,
    StdP, StdS, VarP, VarS, Twa,
}
```

**Time Series Properties:**
- Timestamps in milliseconds since epoch
- Values as f64
- Automatic retention enforcement
- Optional downsampling rules

---

## Commands

| Command | Complexity | Description |
|---------|------------|-------------|
| TS.CREATE | O(1) | Create time series |
| TS.ALTER | O(1) | Modify time series |
| TS.ADD | O(1) | Add sample |
| TS.MADD | O(N) | Add multiple samples |
| TS.INCRBY | O(1) | Increment last value |
| TS.DECRBY | O(1) | Decrement last value |
| TS.DEL | O(N) | Delete samples in range |
| TS.GET | O(1) | Get last sample |
| TS.MGET | O(N) | Multi-key get |
| TS.RANGE | O(N) | Query range |
| TS.REVRANGE | O(N) | Query range (reverse) |
| TS.MRANGE | O(N*M) | Multi-key range |
| TS.MREVRANGE | O(N*M) | Multi-key range (reverse) |
| TS.QUERYINDEX | O(N) | Find keys by labels |
| TS.INFO | O(1) | Get series info |
| TS.CREATERULE | O(1) | Create downsample rule |
| TS.DELETERULE | O(1) | Delete downsample rule |

---

## Command Details

### TS.CREATE

Create new time series.

```
TS.CREATE key
    [RETENTION retentionPeriod]
    [ENCODING [COMPRESSED|UNCOMPRESSED]]
    [CHUNK_SIZE size]
    [DUPLICATE_POLICY policy]
    [LABELS label value ...]
```

| Parameter | Description |
|-----------|-------------|
| RETENTION | Retention in milliseconds (0 = forever) |
| ENCODING | COMPRESSED (default) or UNCOMPRESSED |
| CHUNK_SIZE | Memory chunk size in bytes |
| DUPLICATE_POLICY | How to handle duplicate timestamps |
| LABELS | Key-value pairs for filtering |

**Examples:**
```
> TS.CREATE temperature:sensor1 RETENTION 86400000 LABELS location kitchen device thermometer
OK

> TS.CREATE cpu:host1 DUPLICATE_POLICY LAST LABELS host server1 metric cpu
OK
```

### TS.ALTER

Modify existing time series.

```
TS.ALTER key
    [RETENTION retentionPeriod]
    [CHUNK_SIZE size]
    [DUPLICATE_POLICY policy]
    [LABELS label value ...]
```

| Aspect | Behavior |
|--------|----------|
| Returns | OK |
| LABELS | Replaces all existing labels |

### TS.ADD

Add sample to time series.

```
TS.ADD key timestamp value
    [RETENTION retentionPeriod]
    [ENCODING [COMPRESSED|UNCOMPRESSED]]
    [CHUNK_SIZE size]
    [ON_DUPLICATE policy]
    [LABELS label value ...]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Timestamp of added sample |
| timestamp = * | Auto-generate from server time |
| Non-existent key | Auto-creates with specified options |
| Duplicate timestamp | Applies duplicate policy |

**Examples:**
```
> TS.ADD temperature:sensor1 1609459200000 23.5
(integer) 1609459200000

> TS.ADD temperature:sensor1 * 24.1
(integer) 1609459260000

> TS.ADD newmetric * 100 LABELS env prod
(integer) 1609459300000
```

### TS.MADD

Add samples to multiple time series.

```
TS.MADD key timestamp value [key timestamp value ...]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Array of timestamps |
| Partial failure | Some may succeed, some may fail |

### TS.INCRBY / TS.DECRBY

Increment or decrement the value at the latest timestamp.

```
TS.INCRBY key value [TIMESTAMP timestamp] [RETENTION retentionPeriod] [LABELS label value ...]
TS.DECRBY key value [TIMESTAMP timestamp] [RETENTION retentionPeriod] [LABELS label value ...]
```

| Aspect | Behavior |
|--------|----------|
| Returns | New timestamp |
| No timestamp | Uses current time |
| Non-existent key | Creates with initial value |

### TS.DEL

Delete samples in time range.

```
TS.DEL key fromTimestamp toTimestamp
```

| Aspect | Behavior |
|--------|----------|
| Returns | Number of samples deleted |
| Range | Inclusive on both ends |

### TS.GET

Get most recent sample.

```
TS.GET key [LATEST]
```

| Aspect | Behavior |
|--------|----------|
| Returns | [timestamp, value] |
| Empty series | nil |
| LATEST | Include latest from downsample sources |

### TS.MGET

Get latest samples from multiple keys by filter.

```
TS.MGET [LATEST] [WITHLABELS | SELECTED_LABELS label ...] FILTER filter ...
```

| Aspect | Behavior |
|--------|----------|
| Returns | Array of [key, labels, [timestamp, value]] |
| WITHLABELS | Include all labels |
| SELECTED_LABELS | Include specific labels |

### TS.RANGE / TS.REVRANGE

Query samples in time range.

```
TS.RANGE key fromTimestamp toTimestamp
    [LATEST]
    [FILTER_BY_TS ts ...]
    [FILTER_BY_VALUE min max]
    [COUNT count]
    [ALIGN align]
    [AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
```

| Parameter | Description |
|-----------|-------------|
| fromTimestamp | Start timestamp (- for oldest) |
| toTimestamp | End timestamp (+ for newest) |
| FILTER_BY_TS | Only specific timestamps |
| FILTER_BY_VALUE | Only values in range |
| COUNT | Limit number of samples |
| AGGREGATION | Downsample with aggregation |
| ALIGN | Align buckets to timestamp |
| BUCKETTIMESTAMP | Where to report bucket time (start/mid/end) |
| EMPTY | Include empty buckets |

**Aggregation types:**
- `avg` - Average
- `sum` - Sum
- `min` - Minimum
- `max` - Maximum
- `range` - Max - Min
- `count` - Sample count
- `first` - First value
- `last` - Last value
- `std.p` - Population standard deviation
- `std.s` - Sample standard deviation
- `var.p` - Population variance
- `var.s` - Sample variance
- `twa` - Time-weighted average

**Examples:**
```
> TS.RANGE temperature:sensor1 - + COUNT 5
1) 1) (integer) 1609459200000
   2) "23.5"
2) 1) (integer) 1609459260000
   2) "24.1"

> TS.RANGE temperature:sensor1 1609459200000 1609545600000 AGGREGATION avg 3600000
1) 1) (integer) 1609459200000
   2) "23.8"
2) 1) (integer) 1609462800000
   2) "24.2"
```

### TS.MRANGE / TS.MREVRANGE

Query multiple time series by label filter.

```
TS.MRANGE fromTimestamp toTimestamp
    [LATEST]
    [FILTER_BY_TS ts ...]
    [FILTER_BY_VALUE min max]
    [WITHLABELS | SELECTED_LABELS label ...]
    [COUNT count]
    [AGGREGATION aggregator bucketDuration]
    FILTER filter ...
```

**Filter Syntax:**

| Syntax | Description |
|--------|-------------|
| `label=value` | Exact match |
| `label!=value` | Not equal |
| `label=` | Label exists |
| `label!=` | Label doesn't exist |
| `label=(v1,v2)` | In list |
| `label!=(v1,v2)` | Not in list |

**Examples:**
```
> TS.MRANGE - + FILTER location=kitchen
1) 1) "temperature:sensor1"
   2) (empty array)
   3) 1) 1) (integer) 1609459200000
         2) "23.5"
      2) 1) (integer) 1609459260000
         2) "24.1"

> TS.MRANGE - + WITHLABELS AGGREGATION avg 3600000 FILTER metric=cpu
```

### TS.QUERYINDEX

Find keys matching label filter.

```
TS.QUERYINDEX filter ...
```

| Aspect | Behavior |
|--------|----------|
| Returns | Array of matching key names |
| No matches | Empty array |

### TS.INFO

Get time series information.

```
TS.INFO key [DEBUG]
```

**Returns:**
- Total samples
- Memory usage
- First/last timestamp
- Retention
- Chunk count
- Labels
- Downsample rules

### TS.CREATERULE

Create downsampling rule.

```
TS.CREATERULE sourceKey destKey AGGREGATION aggregator bucketDuration [alignTimestamp]
```

| Aspect | Behavior |
|--------|----------|
| Returns | OK |
| Effect | Auto-aggregate source into dest |
| Multiple rules | Source can have multiple destinations |

**Examples:**
```
> TS.CREATE temperature:sensor1:hourly LABELS location kitchen granularity hourly
OK

> TS.CREATERULE temperature:sensor1 temperature:sensor1:hourly AGGREGATION avg 3600000
OK
```

### TS.DELETERULE

Delete downsampling rule.

```
TS.DELETERULE sourceKey destKey
```

| Aspect | Behavior |
|--------|----------|
| Returns | OK |
| Non-existent rule | Error |

---

## Cross-Shard Behavior

### Single-Key Commands

TS.CREATE, TS.ADD, TS.GET, TS.RANGE route to owning shard.

### Multi-Key Commands

| Command | Behavior |
|---------|----------|
| TS.MADD | Scatter-gather to all involved shards |
| TS.MGET | Scatter-gather by label index |
| TS.MRANGE | Scatter-gather by label index |
| TS.CREATERULE | Source and dest must be on same shard |

**Hash tag requirement for rules:**
```
TS.CREATERULE {sensor1}:raw {sensor1}:hourly AGGREGATION avg 3600000
```

---

## Persistence

Time series persist as:
- Metadata: retention, labels, duplicate policy
- Samples: compressed chunks
- Rules: separate key linking source to dest

---

## Implementation Notes

### Label Index

```rust
/// Secondary index for FILTER queries
pub struct LabelIndex {
    /// label -> value -> keys
    index: HashMap<String, HashMap<String, HashSet<Bytes>>>,
}

impl LabelIndex {
    fn add(&mut self, key: &[u8], labels: &HashMap<String, String>);
    fn remove(&mut self, key: &[u8], labels: &HashMap<String, String>);
    fn query(&self, filters: &[Filter]) -> HashSet<Bytes>;
}
```

### Gorilla Compression

```rust
/// Gorilla-style timestamp/value compression
pub struct CompressedChunk {
    base_timestamp: i64,
    data: BitVec,
}

impl CompressedChunk {
    /// Delta-of-delta for timestamps
    fn encode_timestamp(&mut self, ts: i64, prev_ts: i64, prev_delta: i64);

    /// XOR for values with leading/trailing zero optimization
    fn encode_value(&mut self, value: f64, prev_value: f64);
}
```

### Retention Enforcement

```rust
impl FrogTimeSeries {
    fn enforce_retention(&mut self) {
        if self.retention_ms == 0 { return; }

        let cutoff = current_time_ms() - self.retention_ms as i64;
        self.samples = self.samples.split_off(&cutoff);
    }
}
```

### Crate Dependencies

```toml
bitvec = "1.0"  # For Gorilla compression
```

### Use Cases

- **Infrastructure monitoring**: CPU, memory, disk metrics
- **IoT sensors**: Temperature, humidity, motion
- **Application metrics**: Request latency, error rates
- **Business KPIs**: Revenue, user activity
- **Stock prices**: Time-based financial data
