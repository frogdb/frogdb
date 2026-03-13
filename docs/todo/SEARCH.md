# FrogDB Search (FT.* Commands)

RediSearch-compatible full-text search using **tantivy** as the core search engine.

## Overview

Add `FT.CREATE`, `FT.SEARCH`, and related commands to FrogDB. tantivy (Rust's Lucene equivalent)
provides the inverted index, tokenization, stemming, BM25 scoring, and segment-based storage.
FrogDB provides the Redis protocol layer, shard integration, and write-path hooks.

## Architecture Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Search engine | tantivy | Handles inverted index, tokenization, stemming, BM25, segment storage, mmap I/O |
| Index locality | Per-shard | One tantivy `Index` per shard per search index. FT.SEARCH scatter-gathers. Matches FrogDB's sharded architecture |
| Write-path | Synchronous stage 5.5 | New stage in `run_post_execution()` after WAL. `add_document()` is ~microseconds (in-memory buffer). Guarantees read-your-writes consistency |
| Schema storage | RocksDB metadata CF | Dedicated column family for search index definitions. Survives restarts. Clean separation from user data |
| Index persistence | Disk (MmapDirectory) | tantivy handles mmap/segments transparently. Survives restarts without rebuild. ~50 extra lines vs. RAM-only |
| Snapshot coordination | Skip for MVP | tantivy committed state is always consistent on disk. Can rebuild from data if needed |
| Index ownership | Separate ShardWorker field | `search_indexes: HashMap<String, ShardSearchIndex>` — not a Value variant. Indexes are metadata, not user data |
| Key-to-index matching | Linear prefix scan | Check each index's prefix list against the key. Fine for <10 indexes. Optimize to trie later if needed |
| Document identity | `__key` field in tantivy | Store Redis key as hidden field in every tantivy document. Delete old + insert new on updates |
| Replication | Command replay | Replicas fire same write-path hooks on replicated HSET/DEL. FT.CREATE also replicated as a command |
| FT.SEARCH routing | ScatterGather | New merge strategy: TopKByScore. Fan out to all shards, merge ranked results |
| FT.CREATE propagation | Broadcast to all shards | Similar to FLUSHDB. Schema persisted in metadata CF on each node |
| Commit strategy | Periodic | `add_document()` per write (cheap, in-memory), `commit()` on timer (~1s or N docs). Matches WAL periodic mode |

## What tantivy Provides

tantivy handles the hard 70% of search engine internals:

- **Inverted index** with compressed posting lists (term → doc IDs + positions)
- **Tokenization pipeline**: default, whitespace, raw, language-specific. Composable (lowercase → stemmer → stopwords)
- **Snowball stemming** for ~15 languages
- **BM25 scoring** with field boosting
- **Field types**: TEXT (tokenized), STRING (exact-match), U64/I64/F64 (numeric ranges), Date, Bytes, JSON, IpAddr
- **Query types**: term, phrase, boolean (AND/OR/NOT), range, fuzzy (Levenshtein), prefix, regex, "more like this"
- **Segment-based storage**: write to in-memory buffer → flush to immutable segments → background merge
- **Directory abstraction**: `MmapDirectory` (disk, production) or `RamDirectory` (tests)
- **Concurrent access**: `IndexWriter` is thread-safe with configurable memory budget. `IndexReader` provides snapshot-isolated searches

What tantivy does NOT provide (we build):
- RediSearch query parser (`@field:{tag}` syntax → tantivy Query)
- Write-path hooks (HSET → extract fields → add_document)
- Schema mapping (FT.CREATE args → tantivy Schema)
- Shard-local indexes + scatter-gather merge
- Index lifecycle management (FT.CREATE/DROP/INFO)
- Redis RESP response formatting

## MVP Scope

### Commands

| Command | Execution Strategy | Description |
|---------|-------------------|-------------|
| `FT.CREATE` | Broadcast (all shards) | Define index schema on hash keys |
| `FT.SEARCH` | ScatterGather | Query the index, return matching documents |
| `FT.DROPINDEX` | Broadcast (all shards) | Remove index and tantivy files |
| `FT.INFO` | ConnectionLevel | Return index metadata |
| `FT._LIST` | ConnectionLevel | List all index names |

### Field Types

| RediSearch Type | tantivy Mapping | Notes |
|----------------|-----------------|-------|
| `TEXT` | `TEXT` (tokenized + stored) | Full-text search with BM25 scoring |
| `TAG` | `STRING` (not tokenized + stored) | Exact-match with separator handling (default `,`) |
| `NUMERIC` | `F64` (stored) | Range queries `@field:[min max]` |

### Query Features (MVP)

- `hello world` — AND of term queries across all TEXT fields
- `@title:hello` — term query on specific TEXT field
- `@tags:{redis|search}` — TAG query (OR of exact matches)
- `@price:[10 100]` — numeric range (inclusive)
- `-@field:value` — negation
- `(a | b)` — boolean OR
- `LIMIT offset num` — pagination
- `SORTBY field ASC|DESC` — sort by field
- `RETURN num field1 field2` — field projection
- `NOCONTENT` — return keys only, no fields
- `WITHSCORES` — include BM25 scores

### Deferred

- `FT.AGGREGATE` (GROUP BY, REDUCE, SORT pipeline)
- `FT.ALTER` (add fields to existing index)
- `FT.SUGADD` / `FT.SUGGET` (autocomplete)
- `VECTOR` field type (similarity search)
- `GEO` field type
- Fuzzy matching (`%term%`)
- Prefix matching (`hel*`)
- Phrase queries (`"exact phrase"`)
- Highlighting (`HIGHLIGHT`)
- Synonyms (`FT.SYNUPDATE`)
- Snapshot coordination (include tantivy dirs in RocksDB checkpoints)

## New Crate: `frogdb-server/crates/search/`

```
Cargo.toml              # depends on tantivy, frogdb-core, frogdb-types
src/
├── lib.rs              # SearchEngine: owns per-shard indexes, exposes hooks
├── index.rs            # ShardSearchIndex: wraps tantivy Index + IndexWriter + IndexReader
├── schema.rs           # Parse FT.CREATE args → tantivy Schema + SearchIndexDef
├── query.rs            # Parse RediSearch query syntax → tantivy Query
├── merge.rs            # TopKByScore merge for scatter-gather results
├── commands/
│   ├── mod.rs          # Command registration
│   ├── ft_create.rs    # FT.CREATE
│   ├── ft_search.rs    # FT.SEARCH
│   ├── ft_drop.rs      # FT.DROPINDEX
│   ├── ft_info.rs      # FT.INFO
│   └── ft_list.rs      # FT._LIST
└── hooks.rs            # on_hash_write(key, fields), on_key_delete(key)
```

### Tantivy Index Directory Layout

```
data/
├── rocksdb/            # existing RocksDB data
└── search/
    └── <index_name>/
        ├── shard_0/    # tantivy MmapDirectory (segments, meta.json, etc.)
        ├── shard_1/
        ├── shard_2/
        └── shard_3/
```

## Implementation Steps

### Step 1: Crate scaffolding + tantivy dependency

- Create `frogdb-server/crates/search/` with `Cargo.toml`
- Add `tantivy` to workspace `[dependencies]`
- Define core types:
  ```rust
  pub struct SearchIndexDef {
      name: String,
      prefixes: Vec<Bytes>,       // key prefix filters
      on_type: IndexSourceType,   // Hash (future: JSON)
      fields: Vec<FieldDef>,      // field name + type + options
      tantivy_schema: Schema,     // derived tantivy Schema
  }

  pub struct ShardSearchIndex {
      def: Arc<SearchIndexDef>,
      index: tantivy::Index,
      writer: IndexWriter,
      reader: IndexReader,
  }
  ```
- Wire crate into workspace, verify `just check frogdb-search`

### Step 2: Schema parsing (FT.CREATE args → tantivy)

Parse: `FT.CREATE idx ON HASH PREFIX 1 user: SCHEMA name TEXT age NUMERIC tags TAG`

Mapping:
- `TEXT` → `tantivy::schema::TEXT | STORED` with default tokenizer
  - Options: `WEIGHT 2.0`, `NOSTEM`, `SORTABLE`
- `TAG` → `tantivy::schema::STRING | STORED` (raw, not tokenized)
  - Options: `SEPARATOR ","` (default `,`)
- `NUMERIC` → `tantivy::schema::FLOAT64 | STORED`
  - Options: `SORTABLE`
- Always add `__key` as `STRING | STORED | INDEXED` (for doc identity)

### Step 3: FT.CREATE + FT.DROPINDEX + FT.INFO + FT._LIST

- **FT.CREATE**:
  1. Parse schema (Step 2)
  2. Create tantivy Index at `data/search/<name>/shard_<N>/` using MmapDirectory
  3. Persist `SearchIndexDef` to RocksDB metadata CF (serialized with bincode or JSON)
  4. Add to `shard.search_indexes`
  5. Scan existing keys matching prefix → index in background
  6. Broadcast to all shards (execution strategy)

- **FT.DROPINDEX**:
  1. Remove from `shard.search_indexes`
  2. Drop tantivy Index (closes files)
  3. Delete `data/search/<name>/shard_<N>/` directory
  4. Remove from RocksDB metadata CF
  5. Broadcast to all shards

- **FT.INFO**: Return index name, field count, num_docs (from tantivy reader), index definition

- **FT._LIST**: Return all index names from `shard.search_indexes.keys()`

### Step 4: Write-path hooks

Add to `ShardWorker`:
```rust
pub search_indexes: HashMap<String, ShardSearchIndex>,
```

Add new stage 5.5 in `run_post_execution()` (`pipeline.rs`), after WAL persistence:
```rust
// Stage 5.5: Update search indexes
if flags.contains(CommandFlags::WRITE) && !self.search_indexes.is_empty() {
    self.update_search_indexes(handler.name(), args);
}
```

`update_search_indexes()` logic:
- For HSET/HSETNX/HMSET: extract key, check prefix match against each index, fetch all hash fields, build tantivy `Document`, delete old doc by `__key` term, add new document
- For HDEL: if all indexed fields removed, delete doc
- For DEL/UNLINK: delete doc from all matching indexes
- For RENAME: delete from old key, re-index under new key
- For EXPIRE/PEXPIRE (on expiry): delete doc

Periodic commit: spawn a Tokio interval task per shard that calls `writer.commit()` every ~1 second.

### Step 5: Query parser (RediSearch syntax → tantivy Query)

Parser for the RediSearch query dialect. Input examples and their tantivy translations:

| RediSearch Query | tantivy Translation |
|-----------------|---------------------|
| `hello world` | `BooleanQuery(AND, [TermQuery("hello"), TermQuery("world")])` across all TEXT fields |
| `@title:hello` | `TermQuery(field="title", term="hello")` |
| `@tags:{redis\|search}` | `BooleanQuery(OR, [TermQuery(field="tags", "redis"), TermQuery(field="tags", "search")])` |
| `@price:[10 100]` | `RangeQuery(field="price", 10.0..=100.0)` |
| `hello -world` | `BooleanQuery(AND, [TermQuery("hello"), NOT TermQuery("world")])` |
| `(a \| b) c` | `BooleanQuery(AND, [BooleanQuery(OR, [a, b]), c])` |

Use a recursive descent parser or `nom` combinator parser. Estimated ~500-800 lines.

### Step 6: FT.SEARCH command

Parse: `FT.SEARCH idx "query" [NOCONTENT] [WITHSCORES] [LIMIT offset num] [SORTBY field ASC|DESC] [RETURN num field ...]`

Execution flow:
1. Parse query string using Step 5 parser → tantivy `Box<dyn Query>`
2. `reader.searcher()` → execute query with `TopDocs` collector
3. For each hit: retrieve stored fields, format as RESP array
4. Return: `[total_hits, key1, [field1, val1, ...], key2, ...]`

ScatterGather merge (new variant `TopKByScore`):
- Each shard returns `(total_count, Vec<(score, key, fields)>)`
- Coordinator merges: sum totals, merge-sort by score, apply LIMIT
- Return combined result

### Step 7: Startup + persistence

On shard startup (in `ShardWorker::new()` or init):
1. Load search index definitions from RocksDB metadata CF
2. For each definition, open existing tantivy Index: `Index::open_in_dir(path)`
3. Create `IndexWriter` + `IndexReader`
4. Populate `self.search_indexes`
5. If tantivy dir missing but schema exists: recreate Index, re-scan keys to rebuild

### Step 8: Background indexing for existing data

When FT.CREATE is called and matching keys already exist:
1. Spawn async task that scans the shard store for keys matching prefix filters
2. For each match: read hash fields → build tantivy Document → add_document
3. Commit after batch completes
4. Track progress (indexed_count / total_count) for FT.INFO reporting
5. Don't block the shard event loop — yield periodically

## Key Files to Modify

| File | Change |
|------|--------|
| `frogdb-server/crates/core/src/shard/worker.rs` | Add `search_indexes` field to `ShardWorker` |
| `frogdb-server/crates/core/src/shard/pipeline.rs` | Add search index update stage 5.5 in `run_post_execution()` |
| `frogdb-server/crates/server/src/connection/router.rs` | Add ScatterGather merge variant for FT.SEARCH |
| `frogdb-server/crates/commands/src/lib.rs` | Register FT.* commands |
| `frogdb-server/crates/persistence/src/rocks.rs` | Add metadata CF for search schemas |
| Root `Cargo.toml` | Add `tantivy` workspace dependency |
| `frogdb-server/Cargo.toml` | Add search crate to workspace members |

### Existing Code to Reuse

- `define_command!()` / `define_write_command!()` macros (`core/src/command_macro.rs`) — for FT.* command definitions
- `ExecutionStrategy::ScatterGather` + `ScatterGatherMerge` (`server/src/connection/router.rs`) — for FT.SEARCH routing
- `CommandRegistry::register()` (`core/src/registry.rs`) — for command registration
- `LabelIndex` pattern (`types/src/timeseries/label_index.rs`) — reference for secondary index design
- `RocksConfig` column family setup (`persistence/src/rocks.rs`) — for metadata CF

### Post-Execution Pipeline Context

The write-path hook integrates at stage 5.5 in `shard/pipeline.rs`:

```
run_post_execution():
  Stage 1: Track keyspace metrics
  Stage 2: Increment shard_version (for WATCH)
  Stage 3: Update dirty counter
  Stage 4: Satisfy blocking waiters (BLPOP, etc.)
  Stage 5: Persist to WAL (async → background thread)
  Stage 5.5: UPDATE SEARCH INDEXES  ← NEW
  Stage 6: Broadcast to replicas (async → replica connections)
```

This runs after every write command, before the response returns to the client. The handler,
args, and response are all available. The store is already mutated at this point so hash fields
can be read directly.

## Verification

1. `just check frogdb-search` — type-checks
2. `just lint frogdb-search` — zero clippy warnings
3. `just test frogdb-search` — unit tests:
   - Schema parsing (FT.CREATE args → tantivy Schema, round-trip)
   - Query parser (RediSearch syntax → tantivy Query, edge cases)
   - TopKByScore merge logic
4. Integration tests in `frogdb-server`:
   - FT.CREATE → HSET 100 docs → FT.SEARCH returns correct results
   - TEXT, TAG, NUMERIC queries all work correctly
   - FT.DROPINDEX removes index and files
   - Multi-shard scatter-gather returns merged ranked results
   - Server restart: indexes survive, FT.SEARCH still works
   - FT.CREATE on existing data: background indexing picks up existing keys
5. `just test frogdb-server ft_` — run all FT.* integration tests
