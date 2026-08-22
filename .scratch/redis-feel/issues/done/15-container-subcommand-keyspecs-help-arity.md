# Container commands need per-subcommand key specs (XGROUP/XINFO HELP rejected)

Status: done

## Origin

Wave-D1 arity cross-check (`02e776b3`): upstream models XGROUP/XINFO as
containers whose *subcommand rows* carry the key specs, with container arity
`-2` — so bare `XGROUP HELP` / `XINFO HELP` is legal. FrogDB models each
container as a single command with a key at index 1, and
`CommandSpec::validate` refuses an arity minimum that does not cover the key
(`ArityTooSmallForKeys`), so we demand at least 3 args and reject
`XGROUP HELP` / `XINFO HELP` with a wrong-arity error. D1 carries both as
explicit arity exemptions in `upstream_metadata_tests.rs`.

## What is wrong

Two entangled problems:

1. User-visible: `XGROUP HELP` and `XINFO HELP` fail where Redis answers with
   the help text.
2. Structural: single-spec containers cannot express "subcommand A has a key
   at 2, subcommand B has none" — which also degrades COMMAND INFO key-spec
   fidelity for these containers and forced the DENYOOM judgment to be
   per-container instead of per-subcommand (wave C: XGROUP carries the flag
   even though DESTROY/DELCONSUMER free memory).

## Candidate direction

Per-subcommand spec support for containers (dispatch-level subspec table:
key spec + arity + flags per subcommand, container validates against the
matched subcommand). That also unblocks vendoring upstream's subcommand rows
(the vendor script currently skips them) for INFO/DOCS fidelity. Sizeable
registry change — needs a ruling on scope before implementation.

## Acceptance

- `XGROUP HELP` / `XINFO HELP` return help text, matching Redis.
- D1 arity exemptions for XGROUP/XINFO removed (test enforces shrink-only).
- Per-subcommand key extraction truthful for at least XGROUP/XINFO; DENYOOM
  assignable per subcommand where the container mixes allocating and freeing
  subcommands.

## Ruling (2026-08-21)

**Full subspec table.** Scope:

1. **Registry support.** Containers gain per-subcommand spec rows — key spec +
   arity + flags per subcommand — for all 17 container commands.
   `CommandSpec::validate` / dispatch validation match against the resolved
   subcommand row instead of demanding container-level key coverage (that is
   what rejects `XGROUP HELP` / `XINFO HELP` today); container arity becomes
   upstream's `-2` shape.
2. **Vendor pipeline.** `website/scripts/vendor-redis-commands.py` keeps the
   upstream subcommand rows it currently skips (158 of them);
   `scripts/gen-command-metadata.py` emits them into
   `frogdb-server/crates/commands/src/upstream/generated.rs`; the drift check
   (`just command-metadata-gen-check`) stays green.
3. **Truthfulness.** The join tests in
   `frogdb-server/crates/server/src/server/upstream_metadata_tests.rs` cover
   subcommand rows (key specs, arity, flags). The XGROUP/XINFO arity exemptions
   are removed — the lists are shrink-only, so removal *is* the enforcement.
4. **Per-subcommand admission.** DENYOOM is judged per subcommand (XGROUP CREATE
   keeps it; XGROUP DESTROY/DELCONSUMER, which free memory, drop it). The OOM
   gate in `core/src/shard/execution.rs` resolves the subcommand row for
   containers. Upstream's per-subcommand flag values are the input to the
   judgment.
5. **Emission.** `COMMAND INFO` and `COMMAND DOCS` emit nested subcommand
   entries under containers, matching Redis 8.6.1's reply shape.
6. **`HOTKEYS HELP`.** Implemented, matching the standard container HELP shape
   (upstream added `hotkeys-help.json` in 8.6.1, flags `LOADING`/`STALE`).
7. **Behavior acceptance.** `XGROUP HELP` / `XINFO HELP` / `HOTKEYS HELP` return
   help text like Redis.

## Resolution

Shipped as the full subspec table the ruling called for, in four commits
(`77214635`, `69e23ddd`, `ba7aa08b`, `09eb19ca`).

**Registry.** `CONTAINER_SUBCOMMANDS` in `frogdb-server/crates/core/src/command_spec.rs`
is a side table — `&[(&str, &[SubcommandSpec])]`, 15 containers, 121 rows —
keyed by container name, the same shape `SPLIT_ADMIN_SURFACES` uses for
per-subcommand admin gating. A row carries `name`, `arity`, `flags`, `keys`,
with indices in the container's own arg vector. Adding a `subcommands` field to
`CommandSpec` instead would have touched ~600 struct literals for a property
15 commands have. Three resolvers front it: `container_subcommands`,
`subcommand_spec` and `check_arity`, the last now the single arity gate for all
five sites that used to word the rejection themselves (shard execution, the
script gate, shard scripting, connection routing, connection guards). A
container's arity became upstream's `-2` shape and its `KeySpec` `None`; the
rows carry the keys, and `CommandSpec::validate` enforces "arity reaches the
key it reads" per row via `SubcommandArityTooSmallForKeys`.

Rows override only `WRITE`/`READONLY`/`DENYOOM` (`BEHAVIORAL_FLAGS`);
`flags_over` leaves the container's `FAST`/`SKIP_SLOWLOG`/`PUBSUB`/`LOADING`
alone, so a row declaring nothing does not silently strip them. `ADMIN` stays
out of the vocabulary — it is `SPLIT_ADMIN_SURFACES`'s answer, and having two
tables answer it would be two sources of truth.

**Admission.** `Command::flags_for` resolves the row, and the OOM gate in
`shard/execution.rs` consults it. `XGROUP DESTROY`, `DELCONSUMER` and `SETID`
lost `DENYOOM` (they free memory, and upstream's `xgroup-destroy.json` et al.
carry `write` alone); `CREATE` and `CREATECONSUMER` keep it. The container's
declared flags stay the conservative union, so anything reading `spec.flags`
is unchanged.

**Vendoring and emission.** `vendor-redis-commands.py` keeps the 158 subcommand
rows it used to drop and `gen-command-metadata.py` emits them nested under
their container, so `COMMAND INFO` slot 10 and `COMMAND DOCS`' `subcommands`
map answer with real per-subcommand arity, flags and key specs instead of an
empty array.

**Truthfulness.** Four new join tests in `upstream_metadata_tests.rs` walk every
declared row against the vendored row of the same name. The XGROUP and XINFO
arity exemptions are gone (only `PSYNC` remains in `ARITY_EXEMPTIONS`). Four new
exemptions were needed and are documented at the lists:
`SUBCOMMAND_EXTENSIONS` (`CLIENT|STATS`, `LATENCY|BANDS`, `MEMORY|MALLOC-SIZE` —
FrogDB subcommands upstream does not have), `SUBCOMMAND_FLAG_EXEMPTIONS`
(7 rows: SLOWLOG/LATENCY/HOTKEYS `HELP` are admin here because those containers
are wholly admin, `SLOWLOG GET/LEN/RESET` inherit the container's
`fast`+`skip_slowlog`, `PUBSUB HELP` inherits `pubsub`) and
`SUBCOMMAND_KEY_SPEC_EXEMPTIONS` (`MEMORY|USAGE`, whose argument FrogDB does not
declare as a key). Each is a deliberate behavior difference, not a data gap:
closing them would change admin gating, `@fast` ACL membership, or apply ACL key
permissions and cluster redirection to `MEMORY USAGE`.

**Behavior.** `HOTKEYS HELP` is implemented (`connection/hotkeys.rs`) and the
unknown-subcommand hint now names it. `XGROUP HELP` and `XINFO HELP` already had
handlers — only the container-level arity stood in front of them.

## Comments

**2026-08-21 (compat target 8.6.0 → 8.6.1).** Upstream 8.6.1 adds
`src/commands/hotkeys-help.json` — `HOTKEYS HELP`, arity 2, flags `LOADING`
`STALE`, `since: 8.6.1` — closing a gap of its own (the 8.6.0 `HOTKEYS`
container shipped `START`/`STOP`/`GET`/`RESET` with no `HELP`). FrogDB's
dispatcher (`frogdb-server/crates/server/src/connection/hotkeys.rs`, the
`match subcommand_str` arm) answers `ERR unknown subcommand '<x>'. Try HOTKEYS
START|STOP|RESET|GET.` for `HELP`, so bare `HOTKEYS HELP` now diverges from the
compat target the same way `XGROUP HELP` / `XINFO HELP` do. Same class, same
fix — fold it into this issue's per-subcommand work rather than filing
separately. Note the vendor script skips subcommand rows, so `HOTKEYS HELP`
does not appear in `generated.rs` and no metadata test catches this; the
acceptance comparison against a live 8.6.1 server is the only detector today.
