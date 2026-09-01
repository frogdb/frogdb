# `RESTORE-ASKING` is absent, breaking `redis-cli --cluster` reshard/fix into a FrogDB node

Status: ready-for-agent
Type: gap (missing command)
Area: cluster / commands

## Problem

`RESTORE-ASKING key ttl payload [REPLACE] [ABSTTL] [IDLETIME s] [FREQ f]` — Redis 3.0, arity -4,
flags `WRITE DENYOOM ASKING`, doc flag `SYSCMD` — is not registered. Clients get
`ERR unknown command 'RESTORE-ASKING'`.

It is `RESTORE` with the `ASKING` flag set, and every piece already exists:

- `RESTORE` — `frogdb-server/crates/server/src/commands/persistence.rs`
- `ASKING` — supported (compat matrix)
- `MIGRATE` — supported (`server/src/commands/migrate_cmd.rs`)

`redis-cli --cluster reshard` / `--cluster fix` and other cluster tooling issue `RESTORE-ASKING`
against the *importing* node while a slot is in `IMPORTING` state, because a plain `RESTORE` is
rejected by the slot-ownership check. Without it, third-party cluster tooling cannot move slots
into a FrogDB node even though FrogDB's own migration path works.

## Ruling

Register `RESTORE-ASKING` sharing `RESTORE`'s argument parsing and execution, with the `ASKING`
admission flag so it passes the importing-slot check the way `ASKING` + a normal command does. Do
not duplicate the payload/TTL parsing — sharing it means [issue 24](../../../redis-feel/issues/)'s
`IDLETIME`/`FREQ` fix lands in both at once.

## Acceptance criteria

- [ ] `RESTORE-ASKING` succeeds against a node with the target slot in `IMPORTING` state
- [ ] It is rejected exactly like `RESTORE` when the slot is neither importing nor owned
- [ ] Its `COMMAND INFO` row carries the `ASKING` flag and matches the vendored upstream metadata
      (`website/src/data/redis-commands-8x.json`)
- [ ] `redis-cli --cluster reshard` moves a slot into a FrogDB node end to end
- [ ] The compat matrix stops listing it as unsupported after regeneration

Size: S
