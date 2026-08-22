# `noscript` / `loading` / `stale` are advertised but never enforced

Status: done

## Origin

Wave-D2 flag-parity work: building the permanent
`vendored_command_flags_agree_with_command_info_flags` gate meant deciding, per
flag, whether our value and upstream's are even claims about the same thing. Three
flags had to be dropped from the comparison entirely (`UNCOMPARED_FLAGS` in
`frogdb-server/crates/server/src/server/upstream_metadata_tests.rs`) because
FrogDB has no reader for them.

## What is wrong

`CommandFlags::NOSCRIPT`, `CommandFlags::LOADING` and `CommandFlags::STALE` are
declared on ~200 command specs and emitted in `COMMAND INFO`, but a
tree-wide search finds no consumer of any of the three. Each names an admission
gate Redis implements in `processCommand`:

- **`noscript`** — Redis refuses the command when called from a script. FrogDB's
  Lua sandbox restricts globals rather than the command surface, so
  `redis.call('SUBSCRIBE', ...)` is gated (if at all) by something other than
  this flag.
- **`loading`** — Redis serves only `loading`-flagged commands while an RDB/AOF
  load is in progress. FrogDB serves every command during recovery.
- **`stale`** — Redis refuses non-`stale` commands on a replica whose link to
  the primary is down and `replica-serve-stale-data no`. FrogDB applies no
  per-command refusal there.

So `COMMAND INFO` currently advertises three admission policies FrogDB does not
have. Under ADR-0005 that is the wrong direction: the reply should describe what
FrogDB does.

## Why it matters

`loading` and `stale` are the load-bearing ones. A client or proxy that reads
`stale` to decide what it may send to a link-down replica gets an answer FrogDB
will not honor in either direction — it serves everything. `replica-serve-stale-data`
already exists as a config knob elsewhere in the tree, which makes the absent
enforcement a config that silently does nothing rather than a feature we never
claimed.

## Candidate direction

Two ends, and either is defensible as long as flag and behavior agree:

1. **Implement the gates.** `loading` and `stale` are small: one check in the
   dispatch path against the recovery/link state, keyed on the flag. That also
   gives `replica-serve-stale-data` a meaning. `noscript` needs the scripting
   crate to consult the flag before dispatching a `redis.call`.
2. **Stop advertising them**, the way `CommandFlags::RANDOM` was dropped in D2 —
   drop the bits and the wire spellings, and record the omission.

Option 1 is preferred for `loading`/`stale` (real compatibility behavior behind
an existing knob) and option 2 is the fallback if the gate is deliberately out of
scope. Once either lands, remove the flag from `UNCOMPARED_FLAGS` so the parity
gate starts covering it.

## Ruling (2026-08-21)

Per flag, not one verdict for all three.

1. **`noscript`: ENFORCE**, at the admission chokepoint from
   [issue 13](13-scripts-bypass-the-denyoom-gate.md)
   (`frogdb-server/crates/core/src/command_admission.rs`), whose `admit_command`
   already carried a marker comment naming the slot:
   `ExecOrigin::FromScript(_) && flags.contains(NOSCRIPT)` → `Refused`. Error
   text matches Redis. Per-subcommand flags resolve through
   [issue 15](15-command-info-flags-ignore-subcommands.md)'s
   `Command::flags_for()`.

2. **`stale`: ENFORCE, with a new knob, defaulted the other way from Redis.**
   Add `replica-serve-stale-data` following the existing config-param patterns,
   live-mutable. Semantics are Redis's except the default: when this node is a
   replica whose link to its primary is down and the knob is `no` (**our
   default** — a deliberate deviation; Redis defaults `yes`), refuse every
   command not flagged `STALE` with Redis's `-MASTERDOWN` error. `yes` restores
   Redis behaviour. CockroachDB/FoundationDB precedent: fail over rather than
   serve unbounded staleness. The gate lives at the admission chokepoint or the
   nearest unlocked dispatch seam; link state must be read through an existing
   exposed signal, because the replication crates are locked. Document the
   deviation in the Redis-differences docs and record per-query bounded-staleness
   follower reads as a future direction. Absorbs cluster issue 40's serve-stale
   knob proposal to the extent its scope is covered.

3. **`loading`: DOCUMENTED-VACUOUS.** Boot recovery is synchronous — the
   listeners bind only after it finishes, so no serving-while-loading state
   exists. Do not build a gate. Keep emitting the bits (truthful vacuously), note
   it where the flag is defined, and revisit only if fullsync-apply or online
   loading ever serves.

4. Remove `noscript` and `stale` from `UNCOMPARED_FLAGS` so the parity gate
   covers them. `loading` stays uncompared with the vacuous note. Exemptions
   elsewhere are shrink-only.

## Resolution

All four parts landed. Commits: `785fd51a` (noscript gate + policy + error
texts), plus the knob/gate/tests/docs commits that follow it.

### `noscript` — enforced

`admit_command` refuses a `NOSCRIPT`-flagged command from `ExecOrigin::FromScript`
before the memory gate, matching Redis, which checks `CMD_NOSCRIPT` in
`scriptCall` before anything that depends on server state. Because it sits at the
chokepoint, all three execution paths inherit it with no new seam.

Error text is verbatim upstream: **`ERR This Redis command is not allowed from
script`**. Redis's `script.c` carries the message without a code; Lua's
`luaPushErrorBuff` prepends `ERR `, which is why ours spells it with the prefix.

Forcing tests live in the mutated crate (`frogdb-core`,
`command_admission::tests`), since `cargo mutants -p <crate>` runs only that
package's own tests: `a_noscript_command_is_refused_from_a_script`,
`a_noscript_command_is_refused_from_a_shebang_script_too`,
`a_noscript_command_runs_fine_on_a_plain_connection`,
`noscript_is_refused_before_the_memory_gate`,
`a_script_without_the_flag_is_unaffected`. The end-to-end case is
`a_noscript_command_is_refused_from_a_script` in
`frogdb-server/crates/server/tests/integration_scripting.rs`.

It uses `ROLE`, not `SUBSCRIBE`: the pub/sub family is turned away earlier by the
script gate's own `is_forbidden_in_script` table, so it never reaches the
chokepoint and would have proved nothing about the flag. `ROLE` is
`NOSCRIPT`-flagged (upstream agrees), lives in the shard registry the script gate
consults, and is `ExecutionStrategy::Standard`, so the flag is the only thing
that can refuse it. (`WAIT` was the first choice and is no longer usable: the
flag-parity work below dropped its `NOSCRIPT`, which upstream does not declare.)

### `stale` — enforced, default inverted

- **Knob**: `replication.replica-serve-stale-data`, `bool`, `#[param(mutable)]`,
  default `false`. Registry row appended last in `config_param_registry()` so the
  golden snapshot's first 125 rows stay byte-identical; `GOLDEN_SNAPSHOT` gained
  the matching row and its count went 125 → 126, and `MutableParamId::ALL` 78 →
  79. Runtime lifecycle is an `Arc<AtomicBool>` on `ConfigManager` plus a
  `ConfigParam::<bool, ConfigManager>` arm — no downstream handle to publish
  into, because the gate itself is the reader.
- **Link-state seam**: `ClusterDeps.role_controller: Option<Arc<dyn
  frogdb_core::RoleController>>`, already held by `PreDispatchView` and wired
  unconditionally at `server/subsystems.rs`. `primary_target().is_some()`
  separates replica from primary, `master_link_up()` is a cheap synchronous read.
  Nothing in a locked crate was touched.
- **Placement**: the end of `PreDispatchView::run_pre_checks`, after the
  pub/sub-context gate — Redis's `processCommand` orders it the same way, so a
  client parked in subscribe mode gets the context error rather than a
  replication-state one. Rejecting there also flags an open MULTI dirty, which is
  what Redis's `rejectCommand` does through `flagTransaction`.
- **Policy home**: `frogdb_core::command_admission::stale_refusal` +
  `ReplicaLink`, next to `admit_command`, so one module owns admission policy
  while the connection gauntlet supplies the live inputs. A different symbol, so
  `scripts/command-admission.py`'s `admit_command(` pin set needed no change.
- **Error text** is verbatim upstream, trailing period included:
  **`MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to
  'no'.`** (`shared.masterdownerr`).
- **Scope note**: `master_link_up()` is false for the whole pre-streaming window
  — dialing, handshaking, full-sync transfer — not only a broken link. That is
  deliberate parity: Redis gates on `repl_state != REPL_STATE_CONNECTED`, the
  same window. Replica *apply* traffic is structurally out of reach, because the
  replication executor never builds a `PreDispatchView`.
- **Tests**: `frogdb-core` `command_admission::tests` for the policy
  (`a_healthy_link_gates_nothing`,
  `a_link_down_replica_refuses_a_non_stale_command_by_default`,
  `a_stale_flagged_command_survives_a_down_link`,
  `the_knob_restores_redis_default_behaviour`,
  `the_stale_gate_does_not_spare_writes`); and in
  `frogdb-server/crates/server/tests/integration_replication.rs`,
  `a_link_down_replica_refuses_reads_by_default` (which also covers the live
  `CONFIG SET` flip and the `-READONLY`-still-wins ordering),
  `the_serve_stale_data_knob_restores_redis_behaviour`, and
  `a_primary_is_never_stale_gated`. The harness knob is
  `TestServerConfig::replication_replica_serve_stale_data`.
- **Docs**: `website/src/content/docs/compatibility/overview.mdx` gained a
  "Replication" section stating the inverted default and why.

**Future direction**: per-query bounded-staleness follower reads. The knob is
binary — refuse everything or serve anything — where CockroachDB's follower reads
and etcd's per-request serializable reads let a *client* name the staleness it
will accept. The natural shape here is a per-connection or per-command bound
checked against the applied offset's age, which would make the knob the
node-level default rather than the only control.

### `loading` — documented-vacuous, no gate

Recovery is synchronous: the listeners bind only after it finishes, so there is
no state in which FrogDB serves a client while loading. The flag bits stay —
truthful, vacuously — and `CommandFlags::LOADING`'s doc comment says so and
points here. Revisit only if fullsync-apply or online loading ever serves
clients; the same chokepoint would host the gate.

### `UNCOMPARED_FLAGS`

`noscript` and `stale` removed; the parity gate now covers both. `movablekeys`
and `loading` remain, `loading` with a rewritten note explaining that it is the
one gate FrogDB has no *state* for rather than no code for.

Uncovering them exposed **53 real divergences** (26 whole-command, 27
subcommand) — the flags had drifted for years behind the exemption. All are
resolved: 46 by changing the spec to match upstream, 7 by exemptions with
reasons.

Spec changes (whole-command): `NOSCRIPT` added to the subscribe/unsubscribe
family (a new `SUBSCRIBE_FLAGS`, so PUBLISH/SPUBLISH and the PUBSUB container
stay script-callable as upstream declares), MULTI/EXEC/DISCARD/WATCH/UNWATCH,
QUIT, BGSAVE, BGREWRITEAOF, AUTH, MONITOR and the EVAL/EVALSHA/EVAL_RO/EVALSHA_RO
family; `STALE` added to ECHO, AUTH, MONITOR and the EVAL family; `STALE` dropped
from ASKING; `NOSCRIPT` dropped from MIGRATE (`ServerWide`, so the script gate
refuses it before any flag is read) and WAIT (its executor already answers the
deny-blocking way Redis's `CLIENT_DENY_BLOCKING` branch does).

Spec changes (containers, because `SubcommandSpec::flags_over` only lets a row
refine `WRITE`/`READONLY`/`DENYOOM` — `noscript`/`stale` are container-level
facts here): ACL widened to `NOSCRIPT|LOADING|STALE` (12 rows), `STALE` dropped
from SCRIPT (4 rows) and HOTKEYS (4 rows).

**The AUTH row was a real bug, not a metadata nit**: FrogDB's AUTH carried
neither `STALE` nor `LOADING`, so under this issue's inverted default a client
could not authenticate on exactly the link-down replica an operator needed to
inspect. ECHO, MONITOR and the ACL family had the same shape.

Exemptions added (all reported, all reasoned in-table): `PING` (keeps `STALE` —
health probes must answer on a link-down replica, which matters more here than
upstream because of the inverted default), `ACL` and `CLIENT` (the container
union vs. upstream's `SENTINEL`-only container row), and nine subcommand rows —
`ACL|HELP`, `CLIENT|HELP`, `CONFIG|HELP`, `FUNCTION|HELP`, `SCRIPT|HELP`
(upstream clears `noscript` on every HELP; a row cannot clear its container's
gate here), `MEMORY|HELP`, `OBJECT|HELP` (upstream gives HELP a `stale` the
`readonly` container must not have — marking it would admit `MEMORY USAGE` /
`OBJECT ENCODING` on a stale replica), `CLUSTER|RESET` (upstream's per-row
`noscript`; the refusal exists anyway in `is_forbidden_subcommand`) and
`SCRIPT|LOAD`.

Filed as [issue 20](../open/20-subcommand-rows-cannot-express-admission-gates.md):
the subcommand model cannot express `noscript` / `stale` per row, so those nine
exemptions are structural, not judgment calls. Letting `SubcommandSpec` refine the
two admission gates (with the gate reading the resolved row flags, which
`flags_for` already returns) would retire them.

### Handed on

- [replication-correctness issue 28](../../../replication-correctness/issues/open/28-replica-serve-stale-data-knob.md)
  — its 2026-08-13 ruling of default `yes` is **superseded**. Residue it keeps:
  `FM-REPLICATION-029`'s Invariant still claims no gate and no knob exist, and
  the stranded-promotion forcing case is not yet exercised. Not done here because
  `specs/replication.md` and its generated website mirror were held by another
  session's uncommitted work, and `frogdb-replication` is locked.
- [cluster-correctness issue 40](../../../cluster-correctness/issues/open/40-read-consistency-contract-and-serve-stale-knob.md)
  — its knob half is absorbed (do not add a second `serve-stale-reads` name). Its
  cluster half is not: the shipped gate keys on the replication link, so a node
  fenced or partitioned at the Raft layer with a healthy replication link still
  serves reads, and `specs/cluster.md` still has no read-consistency contract.
