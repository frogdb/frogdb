# Post-follow-up smalls (round-10 tail)

Status: done (landed 2026-07-22, branch workspace-3)

Resolution:
1. De-flaked via bounded retry (10 × 50ms) of the whole GET/SSUBSCRIBE pair; parity assert intact
   (persistent disagreement still fails); 5/5 stable.
2. `refresh_key` now dispatches on current value type (hash → reindex_hash_key, JSON →
   reindex_json_key, else delete) — COPY/RESTORE/RENAME of JSON docs into JSON-index prefixes
   index the destination (all three had the gap; red-green). Follow-up review fix: reconcile-to-
   empty first across ALL index sources (stale hash-index doc on JSON overwrite) + source guard in
   reindex_hash_key (pre-existing: hash indexed as bogus JSON doc).

Surfaced while landing `issues/15-round10-followups.md`; none block anything.

## 1. Flaky: `integration_pubsub::test_ssubscribe_redirect_matches_keyed_path`

Failed once on a loaded testbox (2026-07-22), passed on rerun and 5/5 locally. Race by
construction: the test sends GET (harness connection) then SSUBSCRIBE (fresh connection) on the
same non-owner node and asserts byte-identical MOVED targets; if the node's routing view updates
between the two calls (right after `wait_for_cluster_convergence`), the addresses differ:

```
assertion `left == right` failed: SSUBSCRIBE redirect (slot + addr) must match the keyed-path redirect
  left: (7016, "127.0.0.1:28443")
 right: (7016, "127.0.0.1:39001")
```

Fix idea: retry the GET/SSUBSCRIBE pair until the two MOVED targets agree (bounded loop), or
pin ownership before comparing. The parity property itself is worth keeping.

## 2. Pre-existing reindex gap: JSON docs via COPY/RESTORE

`ReindexAction::Refresh` reconciles hash keys; COPY/RESTORE of a *JSON* document into a
JSON-index prefix still doesn't index the destination (hash-oriented refresh skips it). SET over
a JSON-indexed key *does* de-index (refresh delete path is index-source-agnostic). Extend
Refresh to JSON index kinds.
