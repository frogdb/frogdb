# `TS.CREATERULE` downsampling rules are never serialised — they die on every restart

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/07 F2 + proposals/13 F7 · MASTER.md §3
Score: severity 5 · likelihood 4 · effort 3 · priority 20 (07/F2); severity 5 · likelihood 3 · effort 2 · priority 19 (13/F7)
Area: frogdb-types / timeseries + frogdb-persistence / serialization

## Context

`TimeSeriesValue.rules` is populated by `TS.CREATERULE` but is absent from the timeseries
serializer, and `from_raw` hardcodes `rules: Vec::new()`. After any restart, replica full sync,
`DUMP`/`RESTORE` or `MIGRATE` of the source key, the configured downsampling rule silently stops
firing: the source keeps growing, the destination series simply stops receiving buckets, `TS.INFO`
reports no rules, and nothing errors. The aggregated data that should have been written is
permanently lost. Found independently by two agents from opposite directions (the commands audit
and the persistence audit).

**This is a suspected live defect found by reading, not by test failure — the proposed tests fail
against today's code.** The evidence is the auditing agents' and needs confirmation before or
during the fix.

## Evidence

- `types/src/timeseries/value.rs:94-95` — `/// Downsampling rules attached to this source key.` /
  `rules: Vec<DownsampleRule>`, populated by `add_rule` (`:493`) from
  `commands/src/timeseries.rs:1259-1270` (`TS.CREATERULE`).
- `persistence/src/serialization/timeseries.rs:8-16` documents the complete payload —
  `retention_ms`, `duplicate_policy`, `chunk_size`, labels, chunks — and the encoder at `:30-63`
  writes exactly those; `rules` is absent. `serialization/timeseries.rs:26` `serialize_timeseries`
  writes no rules field.
- `types/src/timeseries/value.rs:160` `TimeSeriesValue::from_raw` hardcodes `rules: Vec::new()`;
  every `TimeSeriesValue` constructor initialises `rules: Vec::new()`, so decode always yields an
  empty rule set.
- `TS.CREATERULE` declares `WalStrategy::PersistFirstKey` (`commands/src/timeseries.rs:1208`), so
  the WAL record is the serialized source value — i.e. the rule-free one.
- **Why the existing tests pass anyway**: the only serialization test for this type asserts the
  duplicate policy; `integration_dump_restore.rs:229-237` round-trips only `TS.ADD`/`TS.GET`.

## What to fix

1. Add `rules` to the timeseries payload in `persistence/src/serialization/timeseries.rs`, both
   encoder and decoder, and update the payload doc comment at `:8-16`.
2. Give `TimeSeriesValue::from_raw` a real rules parameter instead of the hardcoded
   `rules: Vec::new()`.
3. Confirm the WAL record produced by `WalStrategy::PersistFirstKey` now carries the rules, since
   that is the AOF/replay path as well as the snapshot path.
4. Version or tolerate the older rule-less payload on decode (see issue 72's format-version gap).

## Acceptance criteria

- [ ] Codec test builds a `TimeSeriesValue`, calls
      `add_rule(DownsampleRule::new(dest, bucket_ms, agg))`, round-trips through
      `serialize`/`deserialize` and asserts `rules()` matches. **Fails today.**
- [ ] Server-level test: `TS.CREATERULE src dst AGGREGATION avg 1000`, restart, assert `TS.INFO
      src` still lists the rule **and** a subsequent `TS.ADD src` crossing a bucket boundary still
      writes to `dst`. **Fails today.**
- [ ] A `DUMP`/`RESTORE` round-trip of a source key preserves its rules.
- [ ] A pre-fix (rule-less) encoded payload still decodes without error.

## Test boundary

**1** for the codec assertion — pure encode/decode, no engine needed. **4** for the restart half,
which genuinely needs process lifecycle; fold it into the existing restart round-trip test rather
than standing up a second server (proposal 07/F6). Not level 5: a single node restart exhibits the
whole defect.

## Depends on

Nothing. Related: issue 72, `.scratch/testing-improvements-round2/issues/` (no on-disk format
version or magic) — adding a field to this payload is exactly the change that version needs to
describe.

## Re-triage 2026-08-06

**Verdict: still-valid**

Every cited fact reproduces, at essentially the cited addresses.
`persistence/src/serialization/timeseries.rs` contains **zero** occurrences of `rules`: the payload
doc comment at `:5-25` and the encoder `serialize_timeseries` at `:26-...` still write only
retention / duplicate policy / chunk size / labels / chunks / active samples.
`types/src/timeseries/value.rs:160` is still the literal `rules: Vec::new(),` inside `from_raw`, and
`:110` / `:130` are the other two constructors doing the same; `add_rule` (`:492-497`) is the only
writer and `rules()` (`:482`) the only reader. `TS.CREATERULE` still declares
`WalStrategy::PersistFirstKey` — line drift only, `commands/src/timeseries.rs:1208` → `:1210`
(`name:` is at `:1205`), with the `ts.add_rule(rule)` call now at `:1271-1274`. The hardening
campaign locked `frogdb-persistence` but timeseries is a non-core exotic type: no
`FM-PERSISTENCE-*` row mentions `rules` or timeseries downsampling, and no serialization test for
this type asserts rules. Confirmed live data-loss defect.
