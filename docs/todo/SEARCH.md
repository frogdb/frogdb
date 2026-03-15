# FrogDB Search — Phase 3

RediSearch-compatible full-text search using **tantivy** as the core search engine.

## Implemented (Phase 1 + Phase 2)

| Area | What's Done |
|------|-------------|
| **Commands** | FT.CREATE (broadcast), FT.SEARCH (scatter-gather), FT.DROPINDEX (broadcast), FT.INFO, FT._LIST |
| **Field types** | TEXT (WEIGHT, SORTABLE, NOINDEX, NOSTEM), TAG (SEPARATOR, SORTABLE, NOINDEX), NUMERIC (SORTABLE, NOINDEX) |
| **Query syntax** | Terms, field-specific (`@field:term`), tag OR (`@f:{a\|b}`), numeric range (`@f:[10 100]`) with exclusive bounds (`(10`, `(100`), negation (`-`), boolean OR (`\|`), parens, wildcard (`*`), phrase (`"exact phrase"`), prefix (`hel*`), fuzzy (`%term%`, `%%term%%`, `%%%term%%%`) |
| **FT.SEARCH options** | LIMIT, NOCONTENT, WITHSCORES, RETURN (shard-level), SORTBY field ASC\|DESC, INFIELDS count field1 ... |
| **Scoring** | BM25 default, WEIGHT boosting per TEXT field, SORTBY overrides scoring order |
| **Write-path** | Stage 5.5 hook: HSET, HSETNX, HMSET, HINCRBY, HINCRBYFLOAT, DEL, UNLINK, HDEL, RENAME. Expiry hooks clean up search indexes on key TTL expiration |
| **Infrastructure** | Per-shard tantivy MmapDirectory, RocksDB `search_meta` CFs, periodic 1s commit, startup recovery, background indexing on FT.CREATE, custom "simple" tokenizer (NOSTEM) |
| **Tests** | 40 unit (parser, schema, index) + 37 integration |

Code: `frogdb-server/crates/search/` (schema, index, query, error modules), commands in `server/src/commands/search.rs`, write-path in `core/src/shard/search_hook.rs`.

## Phase 3: Deferred

- `FT.AGGREGATE` (GROUP BY, REDUCE, SORT pipeline)
- `FT.ALTER` (add fields to existing index)
- `FT.SUGADD` / `FT.SUGGET` (autocomplete)
- `VECTOR` field type (similarity search)
- `GEO` field type
- Highlighting (`HIGHLIGHT`)
- Synonyms (`FT.SYNUPDATE`)
- Snapshot coordination (include tantivy dirs in RocksDB checkpoints)

## Verification

```bash
just check frogdb-search          # Type-check search crate
just lint frogdb-search            # Zero clippy warnings
just test frogdb-search            # Unit tests (parser, schema, index)
just test frogdb-server test_ft    # Integration tests
just check                         # Full workspace type-check
just lint                          # Full workspace lint
```
