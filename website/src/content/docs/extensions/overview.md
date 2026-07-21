---
title: "Extensions"
description: "The non-core command families FrogDB implements, which are Redis-Stack-compatible versus FrogDB-original, and where each one's canonical documentation lives."
sidebar:
  order: 1
---

An *extension* here means a command family beyond the core Redis keyspace types
(strings, hashes, lists, sets, sorted sets, streams, and so on). FrogDB
implements two kinds of extension family:

- **Redis-Stack-compatible** — the behavior is defined and documented upstream
  by redis.io / the Redis Stack module docs. FrogDB targets that behavior and
  documents only the deltas; it does not restate the upstream reference.
- **FrogDB-original** — no upstream documentation exists, so FrogDB is the
  canonical source and documents the family in full.

This page is an index: it tells you which families exist and where their
canonical documentation lives. It does **not** list per-command support status.
Whether a specific command is supported, partial, or unsupported lives in the
generated [Command matrix](/compatibility/command-matrix/), which is built from
the running command registry and therefore always matches the binary. Deltas
from upstream behavior for the compatible families are on
[Compatibility → Overview & differences](/compatibility/overview/).

## Redis-Stack-compatible families

For these families the upstream documentation is authoritative. Use the Command
matrix to check command-level coverage — filter it by the family's command
prefix (for example `JSON.`).

| Family | What it is | Canonical docs | Status |
|--------|------------|----------------|--------|
| JSON (`JSON.*`) | Store, query, and update JSON values at a key | [redis.io — JSON](https://redis.io/docs/latest/develop/data-types/json/) | [Command matrix](/compatibility/command-matrix/) |
| Time Series (`TS.*`) | Time-series data with retention, downsampling, and aggregation | [redis.io — Time Series](https://redis.io/docs/latest/develop/data-types/timeseries/) | [Command matrix](/compatibility/command-matrix/) |
| Search (`FT.*`) | Secondary indexing, full-text and vector query, and aggregation over hashes/JSON | [redis.io — Search and query](https://redis.io/docs/latest/develop/interact/search-and-query/) | [Command matrix](/compatibility/command-matrix/) |
| Bloom filter (`BF.*`) | Probabilistic set membership (no false negatives) | [redis.io — Bloom filter](https://redis.io/docs/latest/develop/data-types/probabilistic/bloom-filter/) | [Command matrix](/compatibility/command-matrix/) |
| Cuckoo filter (`CF.*`) | Probabilistic set membership with deletion support | [redis.io — Cuckoo filter](https://redis.io/docs/latest/develop/data-types/probabilistic/cuckoo-filter/) | [Command matrix](/compatibility/command-matrix/) |
| Count-Min Sketch (`CMS.*`) | Probabilistic frequency counts over a stream of items | [redis.io — Count-min sketch](https://redis.io/docs/latest/develop/data-types/probabilistic/count-min-sketch/) | [Command matrix](/compatibility/command-matrix/) |
| Top-K (`TOPK.*`) | Probabilistic tracking of the most frequent items | [redis.io — Top-K](https://redis.io/docs/latest/develop/data-types/probabilistic/top-k/) | [Command matrix](/compatibility/command-matrix/) |
| t-digest (`TDIGEST.*`) | Probabilistic quantile/rank estimation over a distribution | [redis.io — t-digest](https://redis.io/docs/latest/develop/data-types/probabilistic/t-digest/) | [Command matrix](/compatibility/command-matrix/) |
| Vector sets (`VADD`, `VSIM`, …) | Vector similarity sets; commands are unprefixed, matching Redis vector sets | [redis.io — Vector sets](https://redis.io/docs/latest/develop/data-types/vector-sets/) | [Command matrix](/compatibility/command-matrix/) |

Vector set commands follow Redis's own unprefixed naming (`VADD`, `VSIM`,
`VCARD`, `VDIM`, `VEMB`, `VGETATTR`, `VINFO`, `VLINKS`, `VRANDMEMBER`, `VRANGE`,
`VREM`, `VSETATTR`) rather than a single `V.*` prefix.

## FrogDB-original families

These have no upstream documentation; FrogDB is the canonical source.

- **Event Sourcing (`ES.*`)** — event-sourcing primitives built on Redis
  Streams: optimistic concurrency control, version-based reads,
  snapshot-accelerated replay, idempotent appends, and a global `$all` stream.
  See the [Event Sourcing reference](/extensions/event-sourcing/) for full
  per-command documentation.
- **`HOTKEYS`** — a FrogDB-original *diagnostic* command that samples
  frequently accessed keys. It is a diagnostic, not a data family; its home is
  [Operations → Diagnostics](/operations/diagnostics/). It is listed here only
  so readers scanning for FrogDB-original surface can find it.

Stream extensions `XDELEX` and `XACKDEL` also exist in FrogDB, but they are part
of the upstream Redis stream command set for the targeted Redis version, not
FrogDB-original. They appear in the [Command matrix](/compatibility/command-matrix/)
under the stream family alongside the other `X*` commands.

## How to read extension status

The [Command matrix](/compatibility/command-matrix/) is generated from the live
command registry, so the extension-command coverage it shows is always in sync
with the binary — a command that is registered appears there, and one that is
not does not. To find a family, open the matrix and search or filter for its
command prefix (`JSON.`, `TS.`, `FT.`, `BF.`, `CF.`, `CMS.`, `TOPK.`,
`TDIGEST.`, `ES.`) or, for vector sets, the individual `V*` command names. Each
command carries a supported / partial / unsupported badge and a short note.
