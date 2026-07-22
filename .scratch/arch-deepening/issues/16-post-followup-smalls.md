# Post-follow-up smalls (round-10 tail)

Status: needs-triage

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
