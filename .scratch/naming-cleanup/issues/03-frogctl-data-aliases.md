# Remove frogctl `data` alias commands

Status: done

Decision (2026-07-17): one concept, one home. `debug memory` owns bigkeys/memkeys; `backup`
owns export/import. The `data` namespace keeps only its own concepts: `keyspace`, `pipe`,
`slot`.

Remove from `frogctl/src/commands/data.rs` (and `cli.rs` wiring):

- `data bigkeys` (alias of `debug memory bigkeys`)
- `data memkeys` (alias of `debug memory memkeys`)
- `data export` (alias of `backup export`)
- `data import` (alias of `backup import`)

Also deletes the three near-identical duplicated arg structs. Pre-release, so no deprecation
cycle needed.

## Comments

Removed the `Bigkeys`, `Memkeys`, `Export`, and `Import` variants (and their duplicated arg
structs — `PathBuf` import in `data.rs` dropped along with them since it was only used by the
removed `Export`/`Import` variants) from `DataCommand` in `frogctl/src/commands/data.rs`, and
the corresponding match arms in `data::run`. `data keyspace`, `data pipe`, `data slot` are
untouched. No changes were needed in `cli.rs` — it only wires the top-level `Data(DataCommand)`
subcommand, which is unaffected by the variant removal. `debug memory bigkeys/memkeys` and
`backup export/import` (the canonical homes) were not touched. `frogctl/tests/integration_data.rs`
only exercises `DataCommand::Slot`, so no test changes were required.

Verified with `RUSTC_WRAPPER="" just check frogctl` (clean) and
`RUSTC_WRAPPER="" just test frogctl integration_data` (2 passed).
