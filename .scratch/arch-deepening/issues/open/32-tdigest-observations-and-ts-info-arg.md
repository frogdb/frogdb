# 32 — TDIGEST.INFO likely omits `Observations`, and TS.INFO silently discards its second argument

Status: needs-triage

## What to build

Two independent `*.INFO` compatibility gaps surfaced while verifying proposal 95's field census.
Neither is in that proposal's scope; both are filed here with the two parts distinguished.

### D1 — `TDIGEST.INFO` is likely missing the upstream `Observations` field

`TdigestInfoCommand::execute` builds a flat 8-field reply at
`frogdb-server/crates/commands/src/tdigest.rs:626-643`: `Compression`, `Capacity`, `Merged nodes`,
`Unmerged nodes`, `Merged weight`, `Unmerged weight`, `Total compressions`, `Memory usage` — in
that order. Upstream RedisBloom documents an `Observations` field **between** `Unmerged weight` and
`Total compressions`. The string `"Observations"` appears nowhere in this repository (grep over
`frogdb-server/` and `website/` returns nothing), so if upstream does emit it, every client that
indexes `TDIGEST.INFO` positionally past `Unmerged weight` reads the wrong value against FrogDB.
Worse, our own oracle *pins the divergence in place*:
`frogdb-server/crates/redis-regression/tests/tdigest_regression.rs:410-437` asserts exactly those
8 fields by name and index, with `assert_array_len(&info, 16)` at `:418`.

**Upstream was not verifiable from the review environment** — there is no vendored RedisBloom
source in the tree — so this issue must *begin* by confirming the field against RedisBloom's
`TDigest.c`/`rm_tdigest.c` reply construction, and only then move the wire shape and the oracle in
lockstep. If upstream does not emit it, close the issue with that finding recorded. Related but
separate: `Total compressions` is a hardcoded `Response::Integer(0)` at `tdigest.rs:639-640`,
self-documented as a Redis-matching placeholder — leave it alone unless the same investigation
shows it wrong.

### D2 — `TS.INFO` accepts and silently discards `args[1]`

`TsInfoCommand`'s spec declares `Arity::Range { min: 1, max: 2 }`
(`frogdb-server/crates/commands/src/timeseries.rs:941`), but `execute` (`:957`) reads
`let key = &args[0];` (`:958`) and never inspects `args[1]`. Two consequences, both live on `main`:
(a) `TS.INFO key JUNK` returns the ordinary full reply where upstream errors on an unrecognised
token — a one-line fix, reject anything that is not `DEBUG`; and (b) the one token upstream *does*
accept there, `DEBUG`, is equally ignored, and FrogDB implements none of the extra chunk-level
fields (`chunkCount`, `Chunks`, …) the `DEBUG` variant is supposed to add — so even a
correctly-spelled `TS.INFO key DEBUG` is non-conformant in reply *content*, not just in validation.
(b) is a feature gap and may be scoped out or deferred; (a) should not be.

Blast radius for both: `frogdb-commands` is not a locked area and neither file carries `FM-` tags.
`TDIGEST.INFO` is reachable only under `--features cmd-full`; `TS.INFO` ships with the timeseries
family. Neither defect is a panic or a durability concern — both are wire-compatibility gaps that
a positionally-indexing or strictly-validating client notices.

## Acceptance criteria

- [ ] D1: RedisBloom's `TDIGEST.INFO` reply is checked against upstream source and the finding is
      recorded in the issue. If `Observations` is emitted upstream, FrogDB emits it in the same
      position with the correct value.
- [ ] D1: `tdigest_regression.rs:410-437` is updated in the same change as the wire shape — the
      `assert_array_len(&info, 16)` at `:418` and the by-name/by-index assertions must not be left
      pinning a shape the server no longer produces.
- [ ] D2(a): `TS.INFO key JUNK` returns an error rather than the full info reply; `TS.INFO key` and
      `TS.INFO key DEBUG` still succeed.
- [ ] D2(b): either the `DEBUG` extra fields are implemented, or the gap is explicitly recorded as
      a documented deviation on the compatibility page (generator inputs, not hand-edited output).
- [ ] Regression tests `ts_info_rejects_unknown_second_arg` (timeseries regression suite) and, if
      D1 confirms, `tdigest_info_reports_observations`. Both fail today.
- [ ] `just test frogdb-server ts_info` and `just test frogdb-server tdigest_info` green.

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 95 (`.scratch/arch-deepening/proposals/95-bf-info-field-table.md`),
§Adjacent defects — surfaced here, to be FILED by the orchestrator, defects D1 and D2.

## Comments
