# Remove frogctl `data` alias commands

Status: ready-for-agent

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
