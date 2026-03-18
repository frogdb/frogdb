# FrogDB Search — Current State

RediSearch-compatible full-text search using **tantivy** as the core search engine.

## Implemented

| Area | What's Done |
|------|-------------|
| **Commands** | FT.CREATE (broadcast), FT.SEARCH (scatter-gather), FT.DROPINDEX (broadcast), FT.INFO, FT._LIST, FT.ALTER, FT.AGGREGATE (GROUPBY, REDUCE, APPLY, FILTER, LOAD, SORTBY, LIMIT), FT.SYNUPDATE, FT.SYNDUMP, FT.ALIASADD, FT.ALIASDEL, FT.ALIASUPDATE, FT.TAGVALS, FT.DICTADD, FT.DICTDEL, FT.DICTDUMP, FT.CONFIG, FT.SPELLCHECK, FT.SUGADD, FT.SUGGET, FT.SUGDEL, FT.SUGLEN, FT.CURSOR, FT.EXPLAIN, FT.EXPLAINCLI, FT.PROFILE |
| **Field types** | TEXT (WEIGHT, SORTABLE, NOINDEX, NOSTEM), TAG (SEPARATOR, SORTABLE, NOINDEX, CASESENSITIVE), NUMERIC (SORTABLE, NOINDEX), GEO (radius queries), VECTOR (KNN cosine/L2/IP via usearch) |
| **Query syntax** | Terms, field-specific (`@field:term`), tag OR (`@f:{a\|b}`), numeric range (`@f:[10 100]`) with exclusive bounds, negation (`-`), boolean OR (`\|`), parens, wildcard (`*`), phrase (`"exact phrase"`), prefix (`hel*`), fuzzy (`%term%`/`%%`/`%%%`), geo radius (`@f:[lon lat r unit]`), KNN vector (`*=>[KNN k @field $param]`) |
| **FT.SEARCH options** | LIMIT, NOCONTENT, WITHSCORES, RETURN, SORTBY ASC\|DESC, INFIELDS, SLOP, HIGHLIGHT (FIELDS, TAGS), SUMMARIZE (FIELDS, FRAGS, LEN, SEPARATOR), PARAMS, TIMEOUT, DIALECT, VERBATIM, INKEYS, FILTER (numeric), GEOFILTER |
| **FT.CREATE options** | ON HASH\|JSON, PREFIX, STOPWORDS (custom/empty/default), SKIPINITIALSCAN, LANGUAGE |
| **Scoring** | BM25 default, WEIGHT boosting per TEXT field, SORTBY overrides scoring order |
| **Tag behavior** | Case-insensitive by default (matching RediSearch). Use CASESENSITIVE modifier to preserve exact case. |
| **Expression functions** | String: upper, lower, strlen, substr, contains, startswith, format, split. Math: log, log2, exp, ceil, floor, abs, sqrt. Date/time: year, month/monthofyear, day/dayofmonth, hour, minute, dayofweek, dayofyear, timefmt (ISO-8601 + full strftime), parsetime. Type: to_number, to_str. Geo: geodistance (haversine). Utility: exists |
| **Write-path** | HSET, HSETNX, HMSET, HINCRBY, HINCRBYFLOAT, DEL, UNLINK, HDEL, RENAME, JSON.SET, JSON.DEL. Expiry hooks clean up search indexes on key TTL expiration |
| **Infrastructure** | Per-shard tantivy MmapDirectory, RocksDB `search_meta` CFs, periodic 1s commit, startup recovery, background indexing on FT.CREATE, custom tokenizers, per-index stopword lists, cursor store for FT.AGGREGATE WITHCURSOR, snapshot coordination (tantivy dirs included in RocksDB checkpoints) |
| **Tests** | 113 unit (parser, schema, index, expression, spellcheck, suggest) + 90+ integration |

Code: `frogdb-server/crates/search/` (schema, index, query, expression, aggregate, spellcheck, suggest modules), commands in `server/src/commands/search.rs`, write-path in `core/src/shard/search_hook.rs`.

## Remaining Gaps

(none)

## Verification

```bash
just check frogdb-search          # Type-check search crate
just lint frogdb-search            # Zero clippy warnings
just test frogdb-search            # Unit tests (parser, schema, index, expression)
just test frogdb-server test_ft    # Integration tests
just check                         # Full workspace type-check
just lint                          # Full workspace lint
```
