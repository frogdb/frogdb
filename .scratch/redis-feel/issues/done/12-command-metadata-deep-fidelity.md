# COMMAND INFO/DOCS deep fidelity: key-spec flags, tips, flag parity, arguments

Status: done

## Rulings (2026-08-21 grill session)

- **Key-specs + arguments + history + flags**: full vendoring. Extend
  `website/scripts/vendor-redis-commands.py` to keep `key_specs`, `arguments`,
  `history`, and `flags` from redis/redis 8.6.0 core, AND vendor the module
  repos (RedisJSON / RediSearch / RedisTimeSeries / RedisBloom) for the
  extension families (json/search/timeseries/bloom groups). Codegen a
  checked-in Rust data module joined to the registry by command name, with a
  verify test asserting vendored key-specs agree with our actual key
  extraction/arity per command. Hand-write only genuinely FrogDB-only
  commands. No macros — key-specs are data; the vendor pipeline is the
  codegen.
- **denyoom**: behavior fix, not just metadata. Today OOM rejection gates on
  `is_write` (`frogdb-core/src/shard/execution.rs:180`), so memory-freeing
  commands (DEL/HDEL/LPOP/FLUSHALL) are rejected under noeviction OOM — the
  recovery path is blocked. Add `CommandFlags::DENYOOM`, gate
  `check_memory_for_write` on it, assign per Redis 8.6.1 for compat commands
  (allocating writes get it, freeing writes don't), judgment per extension.
  Regression: DEL succeeds while over limit under noeviction.
- **Tips**: audit the small set of Redis commands with nonempty tips
  (~40-60), emit only where truthful for FrogDB's routing/aggregation, omit
  with a one-line note where not. Never vendor blindly (ADR-0005).
- **Flag parity**: permanent test comparing our COMMAND INFO flags against
  vendored Redis flags, with an explicit allowlist of deliberate divergences,
  each carrying a one-line justification.
- **ACL category ordering**: fix while touching the emitters (cosmetic).

## Context

Waves 1-2 (issues 02, 03) made COMMAND INFO and COMMAND DOCS real. The wave-3
acceptance rerun of the feel test against Redis 8.6.1 shows the remaining
delta is depth, not correctness:

- **Key-spec detail**: our key-specs entries emit an empty `flags` array and no
  `notes`; Redis emits `RO`/`RW`/`OW`/`access`/`update`/`insert`/
  `variable_flags` plus occasional notes (e.g. SET: "RW and ACCESS due to the
  optional `GET` argument"). Movablekeys commands (SINTERCARD) get an empty
  key-specs array from us; Redis emits a `keynum` begin_search/find_keys spec.
- **Tips**: always empty from us; Redis emits routing tips like
  `request_policy:multi_shard` / `response_policy:all_succeeded` (MSET).
  Tips describe *proxy routing hints*, so truthful values for FrogDB may
  legitimately differ from Redis's — needs a per-command judgment, not a copy.
- **Command flags parity audit**: SET — Redis advertises `denyoom`, we
  advertise `fast`. Our flags come from each CommandSpec's real behavior
  (ADR-0005 truthfulness), but a sweep should confirm every divergence from
  Redis's flag set is deliberate (does FrogDB actually deny writes under OOM
  policy? if yes, SET should carry denyoom).
- **ACL category ordering** differs from Redis (cosmetic; clients treat it as
  a set — fix only if free while touching the emitters).
- **COMMAND DOCS `arguments`/`history`**: deliberately omitted in wave 2 (no
  structured data source). The vendored redis-commands-8x.json pipeline could
  vendor per-command `arguments` if we decide the fidelity is worth the data
  size; FrogDB extensions would need hand-written argument specs.

## Acceptance

- Key-spec entries carry truthful RO/RW/OW/access/update/variable_flags and
  keynum specs for movablekeys commands.
- Flag divergences from Redis 8.6.1 are each either fixed or recorded as
  deliberate (one-line justification at the spec site or in the compat docs).
- Ruling recorded on tips (truthful FrogDB routing hints vs omit) and on
  DOCS arguments (vendor vs stay summary-only), then implemented.
- Feel-test COMMAND INFO/DOCS section diff vs Redis 8.6.1 is empty modulo
  the recorded deliberate divergences.

## Resolution (wave D2, 2026-08-21)

Wave D1 vendored the upstream metadata and codegen'd it into
`frogdb_commands::upstream`; D2 wires it into the emitters, audits the tips,
adds the permanent flag-parity gate, and fixes ACL category ordering.

### Emitters

Everything COMMAND INFO/DOCS answers now lives in one module,
`frogdb-server/crates/commands/src/command_meta.rs` (moved out of `basic.rs`):

| What | Where |
| --- | --- |
| 10-element `COMMAND INFO` reply | `command_meta.rs:47` |
| legacy `first_key`/`last_key`/`key_step`, derived from the first index/range key spec the way Redis's `populateCommandLegacyRangeSpec` does | `command_meta.rs:78` |
| wire flag vocabulary, in `commandFlagNames` order | `command_meta.rs:131` |
| FrogDB-only wire flags (`no-propagate`) | `command_meta.rs:149` |
| ACL category order (Redis 8.6.1's own) | `command_meta.rs:193` |
| tips audit + emitter | `command_meta.rs:269`, `:444` |
| vendored key specs, with the divergence bypass | `command_meta.rs:462`, `:466` |
| key-spec flag order/casing (`RO RW OW RM access update insert delete not_key incomplete variable_flags`) | `command_meta.rs:501` |
| key specs derived from our own `KeySpec` where nothing is vendored | `command_meta.rs:597` |
| `COMMAND DOCS` reply incl. vendored `arguments` (full nesting) and `history` | `command_meta.rs:684`, `:730` |

The legacy-range derivation is what closed `SORT` (`1 1 1`), `MIGRATE`
(`3 3 1`), `GEORADIUS` and `GEORADIUSBYMEMBER`: all four are `movablekeys`, so
our `KeySpec` triplet was `0 0 0` while Redis still reports a range taken from
its first key spec.

### Tips audit

36 commands repeat upstream's tips verbatim. 8 diverge, each because the tip
would misdescribe FrogDB (full reasons at `command_meta::TIP_AUDIT`):

| Command | Emitted | Dropped | Why |
| --- | --- | --- | --- |
| KEYS | `request_policy:all_shards` | `nondeterministic_output_order` | per-shard replies are folded through `SortedUnion`, so the order is determined by the matched key set (`server/src/scatter/broadcast.rs`) |
| SCAN | `request_policy:special`, `response_policy:special` | `nondeterministic_output` | fixed-seed content-hash walk, not bucket-layout order (`core/src/store/hashmap.rs`) |
| HSCAN | — | `nondeterministic_output` | same fixed-seed cursor (`commands/src/utils.rs`) |
| SSCAN | — | `nondeterministic_output` | same as HSCAN |
| ZSCAN | — | `nondeterministic_output` | same as HSCAN |
| XTRIM | — | `nondeterministic_output` | approximate trim is a deterministic simulation of radix-node granularity (`types/src/types/stream.rs`) |
| MSETEX | — | `request_policy:multi_shard`, `response_policy:all_succeeded` | declares `requires_same_slot`; a cross-slot key set is rejected `-CROSSSLOT` rather than split (`commands/src/string.rs`) |
| WAITAOF | — | `request_policy:all_shards`, `response_policy:agg_min` | unimplemented stub that always errors (`server/src/commands/stub.rs`) |

Guarded by `tip_audit_covers_every_tipped_command` (every upstream-tipped
command has a ruling) plus two shape tests in `command_meta`.

### Flag parity

`vendored_command_flags_agree_with_command_info_flags`
(`frogdb-server/crates/server/src/server/upstream_metadata_tests.rs`) compares
every registered command's `COMMAND INFO` flags against the vendored set. A new
divergence fails; so does a stale allowlist entry.

Named per-command divergences (`FLAG_EXEMPTIONS`), 6:

- **PFDEBUG** — upstream is `write denyoom` because `TODENSE` rewrites a sparse
  HLL in place. FrogDB's HLL is always dense, so every subcommand is a pure read
  and `TODENSE` is a no-op; `write` would claim a mutation that cannot happen.
- **WAITAOF** — upstream is `blocking`; ours is a stub that errors before waiting.
- **VEMB / VGETATTR / VLINKS / VSETATTR** — the vector-sets module omits `fast`
  on these four while documenting them as O(1); upstream contradicts itself,
  since VCARD and VDIM are equally O(1) and do carry the bit. Ours are O(1), so
  `fast` is truthful and keeps `@fast` consistent with the published complexity.
  Only reachable under `cmd-full` (vector sets are outside `core-profile`), so
  the gate must be run with that feature to see them.

Flags left out of the comparison entirely (`UNCOMPARED_FLAGS`), 4:
`movablekeys` (never written into upstream's JSON — checked instead by replaying
key specs against real key extraction), and `noscript` / `loading` / `stale`
(admission gates FrogDB does not implement — issue 17).

The sweep behind the gate fixed 21 specs rather than exempting them: `denyoom`
added to SET/SETEX/PSETEX/SETBIT, `fast` corrected on
BLPOP/BRPOP/SMOVE/RENAMENX/BITFIELD_RO/XCLAIM/XAUTOCLAIM/XSETID (and added to
VLINKS/VSETATTR, which lands them in the exemption list above alongside
VEMB/VGETATTR),
`blocking` added to XREAD/XREADGROUP/WAIT, spurious `readonly` dropped from
ROLE/TIME/RANDOMKEY/MEMORY/PFSELFTEST, and `CommandFlags::RANDOM` deleted
outright (no Redis counterpart, no reader).

### ACL ordering

Fixed: `command_info_categories` sorts by Redis 8.6.1's own category order.
Zero of 269 shared commands now differ on ordering alone. The category *sets*
still differ for 86 commands — a `frogdb-acl` table problem, not an emitter one,
split out as issue 16.

### Acceptance

`.scratch/redis-feel/compare-command-metadata.py <frogdb-port> <redis-port>`
(new; raw RESP2, no client library) diffs COMMAND INFO and COMMAND DOCS
field-by-field and groups the result into classes. Run: FrogDB `cmd-full`
(391 commands) vs Homebrew `redis-server` 8.6.1 (274), 269 shared.

Residual classes, all deliberate and all recorded:

| Class | Commands | Disposition |
| --- | --- | --- |
| ACL category *sets* | 86 | issue 16 (`frogdb-acl` table gaps; ordering itself is clean) |
| Flags Redis has that FrogDB does not model (`allow_busy`, `no_auth`, `no_mandatory_keys`, `skip_monitor`, `no_async_loading`, `no_multi`) | ~30 | never fabricated (ADR-0005); silence, not disagreement |
| Unenforced admission gates (`noscript`/`loading`/`stale`) | ~30 | issue 17 |
| Container commands: FrogDB has no per-subcommand registry, so flags/categories/arity/key-specs sit on the container (Redis puts them on the subcommand and leaves the container bare); `subcommands` is always `[]` | ACL CLIENT CLUSTER CONFIG FUNCTION LATENCY MEMORY MODULE OBJECT PUBSUB SCRIPT SLOWLOG XGROUP XINFO + HOTKEYS DEBUG | issue 15 |
| Vector sets: Redis 8.6.1 ships them as a *module* (uppercase name, `module` flag, `group: module`, `since: 8.0.0`, no key-spec index on args); FrogDB implements them natively | 12 (V*) | deliberate — our reply describes a built-in because it is one |
| Vendored snapshot is `REDIS_COMPAT_TARGET` = 8.6.0, reference server is 8.6.1 | GEO unit tokens `m/km/ft/mi` → `M/KM/FT/MI`; stream `ID` → `id`; XADD `producer-id`/`idempotent-id` → `pid`/`iid`; MIGRATE `""` → empty token | upstream's own drift between the two patch releases; closes when the compat target bumps |
| Key-spec divergences | MOVE (stub, registered keyless), SUNSUBSCRIBE (`KeySpec::None`, no slot check) | MOVE: issue 07's stub policy. SUNSUBSCRIBE: issue 14, a real gap |
| PSYNC arity `3` vs `-3` | 1 | FrogDB's PSYNC takes exactly 2 arguments; already a named arity exemption |
| Commands only one side has | 122 ours (module families, ES.*, FROGDB.*), 5 theirs (FAILOVER, RESTORE-ASKING, TRIMSLOTS, VISMEMBER, XCFGSET) | surface coverage, out of scope here |

Zero unexplained differences remain: every flag diff falls into one of the
classes above, and every non-vector-set DOCS diff is 8.6.0→8.6.1 drift.

### Bug found on the way

`ClusterCommand`'s spec was named `"cluster"` in lowercase. The registry
uppercases its keys, so dispatch worked, but every vendored-metadata join keys
off `spec.name` — so `COMMAND DOCS` with no arguments panicked
(`upstream tables are keyed by ASCII-uppercase name; got "cluster"`) and
answered `ERR internal error`. Fixed, with `spec_names_match_their_registry_key`
added so a lowercase spec name can never come back.

### Follow-ups filed

- issue 16 — ACL category table: 55 commands with no categories, 31 with a
  different set.
- issue 17 — `noscript`/`loading`/`stale` advertised but never enforced.
