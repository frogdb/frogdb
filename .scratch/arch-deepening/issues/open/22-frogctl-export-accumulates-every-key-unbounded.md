# 22 — `frogctl` backup export buffers every key name in memory before dumping anything

Status: needs-triage

## What to build

`export_dataset` in `frogctl/src/ops/backup.rs` is a three-phase operation, and phase 1 drains
the **entire** SCAN cursor into a single `Vec<String>` before a single `DUMP` is issued
(`ops/backup.rs:83-105`): `let mut all_keys = Vec::new();` at `:83`, a cursor loop that calls
`all_keys.extend(keys)` at `:97` and only breaks when `cursor == 0` at `:102-104`. Phase 2 then
iterates `all_keys.chunks(BATCH_SIZE)` (`:112`). There is no cap, no streaming, and no `limit`
parameter on the function — the `MATCH`/`COUNT`/`TYPE` arguments narrow *which* keys are
collected, not *how many* are held at once.

A 50M-key export materializes 50M `String`s in the CLI process before phase 2 starts, on top of
whatever slack `Vec` growth leaves behind. On a large keyspace `frogctl backup export` will
either OOM or balloon well past what an operator running a backup from a jump host expects, and
it does so *before* producing any output at all — so the failure wastes the entire scan sweep and
leaves no partial archive.

This is **latent today**, not live: nothing calls `export_dataset`. The whole `ops/` layer is
unreachable — there are zero `ops::` references anywhere under `frogctl/src/commands/`, and
`frogctl/src/commands/backup.rs:135` still answers `backup export` with
`anyhow::bail!("frogctl backup export: not yet implemented")` (`:138` and `:141` likewise for
import and verify). It becomes a real operator-facing failure mode the moment the adapter layer
is wired, which is what proposal 73 proposes to do. 73 fixes defects D3–D5 and D7 on the way in
but explicitly defers this one: the fix is a restructure of the three-phase shape, not a
localized correction.

Fix direction: stream the export — scan one batch, `DUMP`+`PTTL` that batch, write it, advance
the cursor, repeat — so peak memory is bounded by `BATCH_SIZE` rather than by the keyspace. That
changes the shape of the progress callbacks (`ExportProgress::Scanning { keys_found }` currently
reports a running total that would become a per-batch count) and the manifest rollup, which is
why it is its own piece of work. The cheap interim, if the streaming rewrite is deferred past
wiring, is for the adapter's clap `about` string to state the client-side memory cost, and for
the function to grow an explicit cap that fails loudly rather than by exhaustion.

## Acceptance criteria

- [ ] `frogctl backup export` holds a bounded number of key names in memory regardless of
      keyspace size — peak resident key state is O(batch), not O(keyspace)
- [ ] Export begins writing data files before the full scan sweep completes, so a large export
      is not all-or-nothing
- [ ] Regression test `export_dataset_does_not_buffer_whole_keyspace` exercises an export over a
      keyspace several batches wide and asserts the streaming property (e.g. the first data file
      exists / the first `DUMP` is issued before the scan cursor returns to 0)
- [ ] `just frogctl-test` green (`just test frogctl <pattern>` refuses — `frogctl` is excluded
      from the default nextest filter, `Justfile:81-83`, `:297-298`)

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 73
(`.scratch/arch-deepening/proposals/73-frogctl-ops-wiring.md`), defect **D8** (review item O3,
promoted to a numbered defect) — proposal `:359-366`, deferral recorded at `:563-564`.

## Comments
