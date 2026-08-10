# Proposal 70 — ACL subcommand vocabulary: consult the `CommandRegistry`, delete the hand-rolled tables

Round 38 · lane: ACL / command identity · effort **M** · candidate SV10 · carries hotfix **H4**

Verified against the current tree at `03efffeb` (worktree `arch-round-38-99`). Every citation
below was re-derived by reading at that SHA; **three lane-brief claims are corrected** and
**seven drifts the brief did not name were found** (see the census in §Problem).

## Summary

FrogDB describes "which subcommands does container command X accept" in **four** places, three
of which are hand-written string lists that nothing forces to agree:

1. `frogdb-acl/src/parser.rs::is_valid_subcommand` (`:17-125`, 109 lines) — the ACL rule
   vocabulary. Gates `ACL SETUSER +cmd|sub` / `-cmd|sub` at parse time.
2. `frogdb-server/.../connection/util.rs::CONTAINER_COMMANDS` (`:250-253`) — *which* commands
   have a subcommand at `args[0]`. Drives ACL **enforcement** and admin-port gating.
3. `frogdb-core/src/command_spec.rs::SPLIT_ADMIN_SURFACES` (`:584-629`) — the per-subcommand
   admin classification. (Accurate today, and fail-closed by construction — it is the reference
   that proves #1 wrong.)
4. The dispatchers themselves — ~20 `match subcommand { … }` blocks across six crates, which are
   the only description that is true by definition.

The registry (`frogdb-core/src/registry.rs`) is already the single source of command identity —
`CommandRegistry` keys ~390 entries by name and every entry carries a `CommandSpec`. It has
**no subcommand granularity at all**: `entries: HashMap<String, CommandEntry>` is keyed by
top-level name only (`registry.rs:163-167`), and `CommandSpec` (`command_spec.rs:469-507`,
13 fields) has no subcommand field. So "consult the registry" is not a pure refactor — the
registry must first *learn* subcommand identity. That is the substance of this proposal.

**Live-vs-latent headline.** The lane brief's two claims both survive, and both are worse than
stated. `"malloc-stats"` (`parser.rs:103`) is not merely a wrong list entry: it makes
`-memory|malloc-stats` an **inert deny** — an operator writes an explicit denial, `ACL SETUSER`
returns `+OK`, `ACL GETUSER` echoes the rule back, and the user runs `MEMORY MALLOC-SIZE`
anyway. That is an accepted-denied path, **severity HIGH**. The CLIENT gap costs two
subcommands, not one (`UNBLOCK` *and* `STATS`), and it fails loudly rather than silently — the
harm is operators widening to `-client`. Beyond the brief: **LATENCY** is missing three
subcommands, **SCRIPT** is missing `KILL`, **COMMAND** is missing `GETKEYSANDFLAGS`, and four
whole containers — **HOTKEYS, STATUS, FT.CONFIG, JSON.DEBUG** — are absent from the vocabulary
entirely, so no subcommand rule for any of them can be written at all.

**One out-of-scope security finding, discovered by this census and filed separately** (§Risks):
`guards.rs:364` exempts the entire `ACL` command from ACL enforcement, so `-acl`, `-acl|setuser`
and `-acl|getuser` are all inert. That is a **CRITICAL** privilege hole independent of any
vocabulary drift, and it is why the parser's (correct) ACL subcommand list has never been
observed to matter.

The structural fix: give the registry subcommand identity (a defaulted trait method next to each
dispatch `match`, **not** a `CommandSpec` field — see §Sibling edges for why that matters), have
`frogdb-acl` consult it through an inverted port (the crate graph forbids the direct
dependency), and delete tables #1 and #2 outright.

## Files involved

| path | lines | role in this proposal |
|---|---:|---|
| `frogdb-server/crates/acl/src/parser.rs` | 901 | **the change.** `is_valid_subcommand` `:17-125` — **deleted in full** (109 lines). Its two call sites `:373` (`+`) and `:422` (`-`); the double-pipe rejections `:366-371`/`:415-420`; `AclRule::{AllowSubcommand,DenySubcommand}` construction `:383-386`/`:432-435`; `apply` `:538-560`; `parse_and_apply_rules` `:574` — all become vocabulary-aware |
| `frogdb-server/crates/acl/src/manager.rs` | 623 | **the change.** `set_user` `:167` (parse at `:183`) and the aclfile loader `:281` are the two entry points that must carry the vocabulary port; `AclManager` struct `:57`, ctor `:86` |
| `frogdb-server/crates/acl/src/permissions.rs` | 630 | **read-only evidence.** `SubcommandRule` `:110-119`, `is_command_allowed` `:215-266` — subcommand rules are matched by **exact lowercase string** `:220-227` and checked **first, most-specific-wins**, which is exactly why a misspelled rule is inert rather than an error |
| `frogdb-server/crates/acl/src/lib.rs` | 66 | **the change.** Re-export the new `CommandVocabulary` port alongside `SubcommandRule` `:61` |
| `frogdb-server/crates/acl/Cargo.toml` | 20 | **read-only evidence.** Deps are `frogdb-types` only — **no `frogdb-core`**, and none can be added (see the cycle below) |
| `frogdb-server/crates/core/Cargo.toml` | — | **read-only evidence.** `frogdb-acl.workspace = true` at `:38` — `core → acl`. The direction that forces the port |
| `frogdb-server/crates/core/src/registry.rs` | 506 | **the change.** `CommandImpl` `:29-35` gains `subcommands()` (dispatching over both variants like `name()` `:45-51`); `CommandRegistry` `:163-167`; `impl CommandVocabulary for CommandRegistry` lands here |
| `frogdb-server/crates/core/src/command.rs` | 2014 | **the change.** One defaulted method on the `Command` trait (`&'static [&'static str]`, default `&[]`) |
| `frogdb-server/crates/core/src/conn_command.rs` | 1172 | **the change.** The same defaulted method on `ConnectionCommand` |
| `frogdb-server/crates/core/src/command_spec.rs` | 1778 | **the change (additive only).** `SPLIT_ADMIN_SURFACES` `:584-629` **stays** — it is policy, not identity — and gains a coherence assertion. `AdminSurface` `:519-532`, `requires_admin` `:541-551`, `admin_surface` `:637-651`, `split_admin_surface_commands` `:653-655`. **`CommandSpec` `:469-507` is deliberately NOT edited** (see §Sibling edges). FM-tagged tests at `:1609`, `:1621`, `:1632`, `:1654`, `:1667`, `:1679`, `:1689` (FM-CLUSTER-061/062/063/064) must keep their names and semantics |
| `frogdb-server/crates/server/src/connection/util.rs` | 503 | **the change.** `CONTAINER_COMMANDS` `:250-253` — **deleted**. `extract_subcommand` `:256-267` becomes registry-driven |
| `frogdb-server/crates/server/src/connection/guards.rs` | 1886 | **the change.** The ACL enforcement seam `:362-370` (incl. the `cmd_name != "ACL"` exemption at `:364`) and the admin gate `:348-360` are the two `extract_subcommand` callers. Carries FM-CLUSTER-028/030 and FM-REPLICATION-042/046 tagged tests (`:1296`, `:1567+`) — untouched by this change |
| `frogdb-server/crates/server/src/connection/acl_conn_command.rs` | 671 | **the change.** `acl_dryrun` `:233-274` — the third `extract_subcommand` caller (`:248`); `handle_acl` dispatch `:75-109` is one of the containers that declares its arms |
| `frogdb-server/crates/server/src/server/register.rs` | 922 | **the change (tests).** `full_registry()` `:288-291` and `split_admin_surfaces_agree_with_command_flags` `:558-577` (FM-CLUSTER-064) — the existing registry-walking precedent this proposal extends. The ratcheting ACL-category gate at `:580+` is the sibling gate for issue 35 |
| `frogdb-server/crates/server/src/connection/client_conn_command.rs` | 1079 | **the change (declaration).** CLIENT's 19 arms `:80-105` |
| `frogdb-server/crates/server/src/connection/observability_conn_command.rs` | 1349 | **the change (declaration).** SLOWLOG `:162-171`, MEMORY `:322-333`, LATENCY `:555-568`, STATUS `:842-848` |
| `frogdb-server/crates/server/src/connection/conn_command.rs` | 806 | **the change (declaration).** CONFIG's 5 arms `:259-269` |
| `frogdb-server/crates/server/src/connection/hotkeys.rs` | 557 | **the change (declaration).** HOTKEYS `:85-94` |
| `frogdb-server/crates/server/src/connection/scripting/script.rs` | 351 | **the change (declaration).** SCRIPT `:20-30` |
| `frogdb-server/crates/server/src/connection/scripting/function.rs` | 410 | **the change (declaration).** FUNCTION `:84-98` |
| `frogdb-server/crates/server/src/connection/pubsub_conn_command.rs` | — | **the change (declaration).** PUBSUB `:539-549` |
| `frogdb-server/crates/server/src/commands/cluster/mod.rs` | 1211 | **the change (declaration).** CLUSTER's 19 arms `:123-219` |
| `frogdb-server/crates/commands/src/basic.rs` | 1054 | **the change (declaration).** COMMAND `:145-442` (7 arms) |
| `frogdb-server/crates/commands/src/generic.rs` | 736 | **the change (declaration).** OBJECT `:355-480` (5 arms) |
| `frogdb-server/crates/commands/src/stream/info.rs` | 326 | **the change (declaration).** XINFO `:43-62` — behind `stream` |
| `frogdb-server/crates/commands/src/stream/consumer_groups.rs` | 406 | **the change (declaration).** XGROUP `:51-70` — behind `stream` |
| `frogdb-server/crates/commands/src/json/basic.rs` | 467 | **the change (declaration).** JSON.DEBUG `:424+` — behind `json` |
| `frogdb-server/crates/core/src/shard/search/config.rs` | 99 | **the change (declaration).** FT.CONFIG `:20-76` (GET/SET) |
| `frogdb-server/crates/server/src/commands/stub.rs` | — | **read-only evidence.** MODULE `:82-125` is a `NotImplemented` stub — it declares no subcommands |
| `frogdb-server/crates/redis-regression/tests/acl_tcl.rs` | — | **read-only evidence + new tests.** `+config|asdf`/`-config|asdf` must error `:559-571`; `+get|key1|key2` must error `:577-587`; `+select|0` enforced end-to-end `:1820-1840`; `+debug|a/b/c` accepted `:1874-1890`; `+memory|doctor` `:918`, `-memory|doctor` `:939`. These four pin the semantics any redesign must preserve |
| `frogdb-server/crates/server/tests/integration_acl.rs` | — | **new tests.** Existing subcommand-rule coverage is CONFIG-only (`:1474`, `:1510`, `:1564`, `:1752`) |
| `frogdb-server/crates/commands/Cargo.toml` | — | **read-only evidence.** `default = ["core-profile"]`; `stream`, `json`, `geo`, … are opt-in `:15-44`. `frogdb-server` mirrors them, `default = ["cmd-core"]` (`server/Cargo.toml:63`) |

Nothing here is in a **locked** area. `frogdb-acl`, `frogdb-core`, `frogdb-server` and
`frogdb-commands` are all outside the four locked pairs (txn/vll, persistence/recovery,
replication/replication-runtime, cluster/cluster-runtime; ADRs 0002–0004). Grepping `FM-` across
the exact file set finds tags only in `command_spec.rs` and `guards.rs`; **no ACL failure-mode
row exists** — the only `acl` string in `.scratch/hardening/specs/` is
`txn-failure-modes.md:119`, which names `test_acl_denial_in_multi_poisons_the_transaction` and
`test_acl_log_entry_for_each_denial_path_in_multi` as forcing tests for a MULTI row. Both live
in the server crate and both exercise *key/command denial inside MULTI*, not the vocabulary.
No spec edit is owed; no mutation re-gate is owed.

## Problem

### 1. The census: four descriptions of one fact

`is_valid_subcommand` (`parser.rs:17-125`) is a 17-arm `match` over lowercase command names,
each arm a `matches!` over a lowercase subcommand list, with two blanket `true` arms (`debug`,
`select`) and a `_ => false` floor. It is consulted at exactly two places — `parser.rs:373`
(`+cmd|sub`) and `parser.rs:422` (`-cmd|sub`) — and a miss is a hard parse error:

```
"Unknown command or subcommand '{}|{}'"   // parser.rs:377, :426
```

which `AclManager::set_user` (`manager.rs:183`) propagates, so the whole `ACL SETUSER` fails.

`CONTAINER_COMMANDS` (`util.rs:250-253`) is a flat 19-name list feeding `extract_subcommand`
(`util.rs:256-267`), which returns `args[0].to_uppercase()` for a listed command and `None`
otherwise. Three callers: the admin gate (`guards.rs:355`), the ACL enforcement seam
(`guards.rs:367`), and `ACL DRYRUN` (`acl_conn_command.rs:248`).

The two tables are joined by nothing. Table #1 decides whether a rule may be *written*; table #2
decides whether a written rule is ever *consulted*. A name that appears in #1 but is spelled
differently by the dispatcher parses fine and never matches.

### 2. The exhaustive diff — every drifted entry

Each row diffs `parser.rs`'s list against the dispatcher's actual `match` arms, read at HEAD.

| container | `parser.rs` list | real dispatch (file:line) | drift |
|---|---|---|---|
| `acl` | cat, deluser, dryrun, genpass, getuser, help, list, load, log, save, setuser, users, whoami (13) | same 13 — `acl_conn_command.rs:84-108` | — |
| `client` | caching, getname, getredir, help, id, info, kill, list, no-evict, no-touch, pause, reply, setname, tracking, trackinginfo, unpause, setinfo (17) | + **unblock**, **stats** (19) — `client_conn_command.rs:80-105` | **2 missing** |
| `cluster` | addslots, bumpepoch, count-failure-reports, countkeysinslot, delslots, failover, flushslots, forget, getkeysinslot, help, info, keyslot, links, meet, myid, nodes, replicate, reset, saveconfig, set-config-epoch, setslot, shards, slaves, slots (24) | 19 — `cluster/mod.rs:124-214`. All 19 are covered | **5 phantom**: bumpepoch, count-failure-reports, flushslots, links, slaves |
| `command` | count, docs, getkeys, help, info, list (6) | + **getkeysandflags** (7) — `basic.rs:145,153,180,256,292,332,400` | **1 missing** |
| `config` | get, help, resetstat, rewrite, set (5) | same 5 — `conn_command.rs:260-264` | — |
| `debug` | `true` (blanket) | whole-command `ADMIN`, many arms — `debug_conn_command.rs` | blanket; matches Redis's *first-arg* treatment (§8) |
| `function` | delete, dump, flush, help, kill, list, load, restore, stats (9) | same 9 — `function.rs:85-93` | — |
| `latency` | graph, help, history, latest, reset (5) | + **bands**, **doctor**, **histogram** (8) — `observability_conn_command.rs:556-563` | **3 missing** |
| `memory` | doctor, help, **`malloc-stats`**, purge, stats, usage (6) | doctor, help, **`MALLOC-SIZE`**, purge, stats, usage — `observability_conn_command.rs:323-328` | **1 wrong name** |
| `module` | help, list, load, loadex, unload (5) | `NotImplemented` stub — `stub.rs:82-125` | 5 phantom (whole command is a stub) |
| `object` | encoding, freq, help, idletime, refcount (5) | same 5 — `generic.rs:356,435,446,460,472` | — |
| `pubsub` | channels, help, numpat, numsub, shardchannels, shardnumsub (6) | same 6 — `pubsub_conn_command.rs:540-545` | — |
| `script` | **debug**, exists, flush, help, load (5) | exists, flush, help, **kill**, load — `script.rs:21-25` | **1 missing** (kill), **1 phantom** (debug) |
| `select` | `true` (blanket) | not a container; first-arg rule | matches Redis (§8) |
| `slowlog` | get, help, len, reset (4) | same 4 — `observability_conn_command.rs:163-166` | — |
| `xgroup` | create, createconsumer, delconsumer, destroy, help, setid (6) | same 6 — `consumer_groups.rs:52-57` | — (but `stream`-gated, §7) |
| `xinfo` | consumers, groups, help, stream (4) | same 4 — `info.rs:44-47` | — (but `stream`-gated, §7) |
| **`hotkeys`** | *absent* → `_ => false` | start, stop, reset, get — `hotkeys.rs:86-89` | **whole container missing** |
| **`status`** | *absent* → `_ => false` | json, help — `observability_conn_command.rs:843-844` | **whole container missing** |
| **`ft.config`** | *absent* → `_ => false` | get, set — `search/config.rs:21,60` | **whole container missing**, and also absent from `CONTAINER_COMMANDS` |
| **`json.debug`** | *absent* → `_ => false` | memory — `json/basic.rs:425` | **whole container missing**, and also absent from `CONTAINER_COMMANDS` |

`CONTAINER_COMMANDS` (`util.rs:250-253`) separately omits **`FT.CONFIG`** and **`JSON.DEBUG`**,
so those two need a **two-table** fix: correcting only `parser.rs` would let the rule be written
and still never enforced.

`SPLIT_ADMIN_SURFACES` (`command_spec.rs:584-629`) was diffed the same way and is **correct at
HEAD** — every `public` name for CLUSTER, CONFIG, ACL, CLIENT and MEMORY (including
`"MALLOC-SIZE"` at `:627`) is a real dispatcher arm. It is the *only* one of the three tables
that has stayed true, for a structural reason worth naming: it fails **closed**
(`requires_admin` returns `true` for `None` and for unlisted names, `command_spec.rs:545-549`),
so a stale entry over-gates and someone notices. `parser.rs` fails **open** in the direction
that matters — a stale entry produces a rule that is silently never consulted.

### 3. `malloc-stats` — LIVE, severity HIGH (accepted-denied)

`parser.rs:103` lists `"malloc-stats"`; `observability_conn_command.rs:325` dispatches
`"MALLOC-SIZE"`. Grepped repo-wide, `malloc-stats` appears **exactly once** — at
`parser.rs:103`. It is not an alias anyone implemented.

**Trace A — legitimate grant rejected.** Operator runs
`ACL SETUSER bob on >pw ~* -@all +memory|malloc-size`. `AclRule::parse` reaches `parser.rs:373`,
`is_valid_subcommand("memory","malloc-size")` returns `false`, and `set_user` fails with
`ERR ... Unknown command or subcommand 'memory|malloc-size'`. The subcommand cannot be granted
at all; the operator's only recourse is `+memory`, which also grants `MEMORY PURGE` (an
allocator-wide operation FrogDB deliberately gates as admin, `command_spec.rs:574-577`).

**Trace B — explicit deny is inert. This is the security-relevant one.** Operator runs
`ACL SETUSER bob on >pw ~* +@all -memory|malloc-stats`. Parse succeeds (`"malloc-stats"` is in
the list), `apply` stores `SubcommandRule { command: "memory", subcommand: "malloc-stats",
allowed: false }` (`parser.rs:552-559`), `ACL GETUSER` echoes `-memory|malloc-stats` back
(`permissions.rs:136-158`), and the operator believes the deny landed. At runtime
`extract_subcommand` yields `Some("MALLOC-SIZE")` (`util.rs:263`), `is_command_allowed`
lowercases to `"malloc-size"`, compares against `"malloc-stats"` at `permissions.rs:222-226`,
**does not match**, falls through the subcommand loop, hits `if self.allow_all { return true }`
at `permissions.rs:246` — and **bob runs `MEMORY MALLOC-SIZE`**. An explicit denial that returns
`+OK` and does nothing is worse than an error.

The same inert-deny shape applies to every phantom entry in the table above (`-script|debug`,
`-cluster|links`, `-module|load`, …); those are **latent** only because the subcommands do not
exist, and each becomes live the moment someone implements one under a different spelling.

### 4. CLIENT `unblock` / `stats` — LIVE, severity MEDIUM-HIGH (fails loud, harms by workaround)

`client_conn_command.rs:96-97` dispatches `UNBLOCK` and `STATS`; `parser.rs:36-54` lists neither.
Both directions fail at parse time, so there is no silent hole — but there is no way to express
the rule either:

- `-@all +client|stats` (grant read-only client statistics) → `ERR ... Unknown command or
  subcommand 'client|stats'`. Operator must grant `+client`, which includes `CLIENT KILL`,
  `CLIENT PAUSE`, `CLIENT UNBLOCK` and `CLIENT LIST`.
- `+@all -client|unblock` → same error. `CLIENT UNBLOCK` forcibly returns another connection
  from a blocking call — the single CLIENT subcommand most worth denying individually. Operator
  must use `-client`, which also removes `CLIENT ID`/`SETNAME`/`GETNAME`/`INFO`, the four
  self-directed subcommands every client library calls.

Note the asymmetry with the admin gate: `SPLIT_ADMIN_SURFACES` already classifies `UNBLOCK` and
`STATS` correctly as admin-only (they are absent from CLIENT's `public` list,
`command_spec.rs:606-623`). A deployment with an admin port is protected; a deployment relying
on ACLs alone is not.

### 5. LATENCY, SCRIPT, COMMAND

- **LATENCY — LIVE, MEDIUM.** `bands`, `doctor`, `histogram` dispatch at
  `observability_conn_command.rs:556,557,560` and are absent from `parser.rs:97-100`. Both
  traces apply. LATENCY carries whole-command `ADMIN`, so admin-port deployments are covered;
  ACL-only deployments cannot grant `+latency|latest` without also granting `+latency|reset`.
- **SCRIPT `kill` — LIVE, MEDIUM.** `script.rs:24` dispatches `KILL`; `parser.rs:114` omits it.
  `SCRIPT KILL` aborts another client's running script — a cross-tenant effect that is exactly
  what a per-subcommand deny is for, and it is unexpressible. Conversely `+script|debug`
  (`parser.rs:114`) parses and grants nothing, because no `DEBUG` arm exists — **latent**.
- **COMMAND `getkeysandflags` — LIVE, LOW.** `basic.rs:292` dispatches it; `parser.rs:85-88`
  omits it. `+command|getkeysandflags` is rejected; the grant must widen to `+command`.

### 6. Four whole containers with no vocabulary at all — LIVE

`parser.rs`'s floor is `_ => false` (`:123`), so a container it has never heard of rejects
*every* subcommand rule:

- **HOTKEYS** (`hotkeys.rs:86-89`: START/STOP/RESET/GET). `+hotkeys|get` → parse error.
  HOTKEYS carries whole-command `ADMIN` and `HOTKEYS GET` discloses key names and access
  frequencies (`command_spec.rs:579-583`), so "grant GET, deny START/STOP/RESET" is precisely
  the rule an operator wants — and cannot write.
- **STATUS** (`observability_conn_command.rs:843-844`: JSON/HELP). Same.
- **FT.CONFIG** (`search/config.rs:21,60`: GET/SET) — **the worst of the four**. `FT.CONFIG SET`
  writes global search configuration. `+ft.config|get` is rejected, so an operator wanting
  read-only search-config visibility must grant `+ft.config`, which includes `SET`. A rejected
  narrow grant that forces a wide one is privilege escalation by workaround. FT.CONFIG is
  *also* absent from `CONTAINER_COMMANDS`, so even after fixing `parser.rs` the rule would
  parse and never be consulted — this one needs both tables.
- **JSON.DEBUG** (`json/basic.rs:425`: MEMORY) — same two-table shape, behind the `json`
  feature.

### 7. Feature-flag contact

`frogdb-commands` gates command families as cargo features (`Cargo.toml:15-44`), default
`core-profile`; `frogdb-server` mirrors them with `default = ["cmd-core"]`
(`server/Cargo.toml:63`), and only `redis-regression` and `ops/docs-gen` request `cmd-full`.
So on a default build **XINFO, XGROUP and JSON.DEBUG are not registered at all** — yet
`parser.rs` accepts `+xinfo|groups` unconditionally, because a hand-rolled table cannot know
what was compiled.

That is the *current* asymmetry, and it is the design constraint registry-consult must
respect: a naive "look it up in the registry, error on miss" would make a rule that parses on a
`cmd-full` build fail to parse on a `cmd-core` one, so an `aclfile` written against one binary
would fail to load against another. The ruling is in §Proposed change (rule 3): a **registry
miss on the whole command** falls back to first-arg semantics (stored, never matches — exactly
today's behavior), and only a **known container with an unknown subcommand** errors. Behavior
under a compiled-out family is then unchanged from today.

Redis has the identical situation for module-provided commands and resolves it the other way
(hard error), but Redis has no compile-time command families — a module is loaded or not at
runtime, and `ACL LOAD` failing on an unloaded module is an operator-visible ordering problem
Redis accepts. FrogDB's build-time variance makes the softer rule the better fit; it is a
documented deviation, not an accident.

### 8. Redis / Valkey: there is no ACL subcommand table

Redis (`acl.c`) has no hand-written ACL vocabulary. `ACLSetSelector` handles `+cmd|sub` by
calling `ACLLookupCommand`, which is the same lookup command dispatch uses: it splits on `|`,
finds the parent in the command dict, and then resolves `sub` against the parent's
`subcommands_dict` — the structure the dispatcher itself is built from. The consequences,
each of which FrogDB either matches or deviates from:

| Redis behavior | FrogDB at HEAD | verdict |
|---|---|---|
| Vocabulary = the command table. A subcommand is grantable **iff** it is dispatchable | separate hand-rolled list | **the divergence this proposal closes** |
| `+cmd|sub` where `cmd` has a subcommands dict and `sub` is unknown → `ENOENT` error | error (`parser.rs:373`) — pinned by `acl_tcl.rs:559-571` (`+config|asdf`) | matches |
| `-cmd|sub`, same → `ENOENT` error | error (`parser.rs:422`) — pinned by `acl_tcl.rs:566-571` | matches |
| `+cmd|a|b` (two pipes) → error, "allowing first-args of a subcommand is not allowed" | error, same message (`parser.rs:366-371`) — pinned by `acl_tcl.rs:573-587` | matches |
| `+cmd|arg` where `cmd` has **no** subcommands → an **allowed-first-arg** rule (`ACLAddAllowedFirstArg`), e.g. `+select|0`, `+debug|a` | two blanket `true` arms hard-code exactly `select` and `debug` (`parser.rs:93`, `:115`) — pinned by `acl_tcl.rs:1820-1840` and `:1874-1890` | matches **only for those two names**; `+get|foo` is rejected where Redis accepts. Latent compat gap, LOW |
| `-cmd|arg` on a non-container → error (first-arg rules are `+`-only) | `-select|0` is accepted (blanket `true`) and stored | deviation, LOW; the proposed rule fixes it |

Valkey inherits `acl.c` unchanged here. DragonflyDB implements a narrower ACL surface (no
`cmd|sub` rules at the time of writing) and offers no useful precedent.

The takeaway is not "FrogDB has a bug in its table"; it is that **Redis never had a table**. The
per-subcommand identity lives with the dispatcher, and ACL reads it. That is the shape.

## Proposed change

### Why this shape, in the vocabulary

`is_valid_subcommand` is a **shallow** module in the purest sense: its interface
(`fn(&str,&str) -> bool`) is trivial, its implementation is 109 lines of literals, and every one
of those literals is a *copy* of information that already exists, in a form the compiler checks,
twenty lines away from the code that consumes it. It carries no logic — only a second opinion.

The **deep** version of the same interface is the registry: it already owns command identity for
~390 commands, it is already the thing `guards.rs:352` consults for flags and `routing.rs:59`
consults for keys, and it is already walked by coherence tests (`register.rs:558`). Extending it
from "which commands exist" to "which command/subcommand pairs exist" is one more fact in the
place that already holds the rest of them. **Locality**: adding a subcommand means editing the
`match` and the list beside it, in one file, and every consumer — ACL parse, ACL enforcement,
admin gating, `COMMAND DOCS` if it ever wants it — follows. **Leverage**: three tables become
one, and the one that survives is the one that cannot drift, because a lint pins it against the
`match` it sits next to.

The **seam** belongs between *command identity* (core) and *permission policy* (acl), and its
direction is forced by the crate graph, not chosen: `frogdb-core` already depends on
`frogdb-acl` (`core/Cargo.toml:38`), so `frogdb-acl` **cannot** depend on `frogdb-core`. The
consumer must therefore define the port and the producer must implement it — dependency
inversion, and the reason this is an M and not an S.

**Deletion test, both directions.** Delete `is_valid_subcommand`: nothing reappears anywhere —
every name it holds is already stated by a `match` arm the dispatcher needs regardless. Delete
`CONTAINER_COMMANDS`: same. Delete the proposed `subcommands()` method: the vocabulary
immediately reappears in **three** places (ACL parse, ACL enforcement, and the admin table's
coherence check), which is exactly the "two or more adapters" bar for introducing a seam at all.

### The change, in five parts

**1 — `frogdb-acl` defines the port** (`acl/src/lib.rs`, ~15 lines):

```rust
/// The command table, as ACL needs to see it. Implemented by `CommandRegistry`
/// in `frogdb-core` (the crate graph forbids the reverse dependency).
pub trait CommandVocabulary: Send + Sync {
    /// The subcommands this container accepts, or `None` when the command is
    /// not a container — in which case `cmd|arg` is a *first-arg* rule, as in
    /// Redis. `None` is also returned for a command the registry does not know.
    fn subcommands(&self, command: &str) -> Option<&'static [&'static str]>;
}
```

`AclManager` (`manager.rs:57`) holds `Option<Arc<dyn CommandVocabulary>>`, wired at server
construction where both `registry: Arc<CommandRegistry>` and the manager already sit side by
side (`server/mod.rs:82`). `is_valid_subcommand` is replaced by `AclRule::parse_with(vocab,
rule)`; the existing `AclRule::parse` keeps working against a `None` vocabulary.

**Fail-open at parse time is deliberate and safe.** A `None` vocabulary (unit tests, `frogctl`,
any embedding that has no registry) accepts every `cmd|sub` pair. This is not a weakening:
parse-time validation is a *usability* guard that catches typos, never a security guard —
enforcement is exact-string matching either way (`permissions.rs:220-227`), so an unvalidated
rule can only ever grant/deny a string nothing dispatches. Stating this explicitly is what lets
`frogdb-acl` keep its 30+ existing unit tests dependency-free.

**2 — `frogdb-core` gains subcommand identity**, as a defaulted trait method on both executor
traits, declared in the same file as the dispatch `match`:

```rust
// Command (command.rs) and ConnectionCommand (conn_command.rs), identically:
/// The subcommands this command dispatches, uppercase. Empty for a
/// non-container command — see `CommandVocabulary`. Must equal the arms of
/// this command's own subcommand `match` (pinned by `lint-acl-vocabulary`).
fn subcommands(&self) -> &'static [&'static str] { &[] }
```

surfaced through `CommandImpl::subcommands()` (`registry.rs`, dispatching over both variants
exactly like `name()` at `:45-51`), and closed by `impl CommandVocabulary for CommandRegistry`
in core.

**This deliberately does not touch `CommandSpec`.** A 14th field on `CommandSpec`
(`command_spec.rs:469-507`) would require editing ~390 struct literals across every command file
— which is both a merge-conflict bomb against sibling **90/CT2** and unnecessary, since only ~20
commands have anything to say. See §Sibling edges for the ordering that makes the spec-field
variant attractive *later*.

**3 — the parse rule becomes uniform, and the two special cases disappear:**

| case | rule | replaces |
|---|---|---|
| `vocab.subcommands(cmd) == Some(list)`, `sub ∈ list` | subcommand rule | the 15 explicit arms |
| `vocab.subcommands(cmd) == Some(list)`, `sub ∉ list` | **error** (Redis `ENOENT`) | `_ => false` for known containers |
| `vocab.subcommands(cmd) == None` (non-container **or unknown command**), `+` | **first-arg rule** | `"select" => true`, `"debug" => true`, and the `cmd-core` portability case (§7) |
| `vocab.subcommands(cmd) == None`, `-` | **error** (Redis: first-arg rules are `+`-only) | today's blanket accept for `-select|0` |
| two or more `|` | error, unchanged | `parser.rs:366-371` |

`+select|0` and `+debug|a` keep working — not by name, but because SELECT and DEBUG declare no
subcommands, which is exactly why Redis treats them as first-arg rules. **DEBUG must keep
declaring `&[]`** even though `debug_conn_command.rs` has a large `match`: Redis has no DEBUG
subcommands dict, and `acl_tcl.rs:1874-1890` pins `+debug|a`. The rule for what to declare is
"is this a Redis container command", not "does the dispatcher have a match statement".

**4 — enforcement stops consulting a list.** `CONTAINER_COMMANDS` is deleted and
`extract_subcommand` becomes: take `args[0]` when the registry entry declares subcommands, **or**
when the user's permission set carries any subcommand/first-arg rule for this command (which is
how first-arg rules on non-containers stay enforceable, and how `+select|0`'s NOPERM at
`acl_tcl.rs:1838` survives). All three callers (`guards.rs:355`, `guards.rs:367`,
`acl_conn_command.rs:248`) already have the registry in hand.

**5 — `SPLIT_ADMIN_SURFACES` stays, and gains a coherence assertion.** It encodes *policy*
(which subcommands a plain client port may reach), not identity, and per
`feedback_spec_enums_pure_identity` that separation is correct: identity in core's command
description, policy flags beside the dispatch. What it gains is one line inside the existing
FM-CLUSTER-064 test at `register.rs:558`: every `public` name must be a declared subcommand of
that command. Had that assertion existed, `malloc-stats` would have been impossible — the admin
table has said `MALLOC-SIZE` since it was written.

## Testability improvement

Today there is **no test anywhere** that relates the ACL vocabulary to the dispatchers. The
`parser.rs` unit tests (`:626-901`) exercise `+config|get` / `-config|set` / `+config|` only —
three strings, all from the one container that happens to be correct. Every drift in §2 is
invisible to the whole suite. Four artifacts change that:

- **T1 · `lint-acl-vocabulary` (compile-free grep gate, joins `lint-gates`).** For each file
  declaring `fn subcommands()`, extract the string-literal arms of the neighbouring
  `match subcommand…` block and assert set equality with the declared list. This is the gate
  that makes the class of bug extinct: a new subcommand cannot land dispatch-only. Modelled on
  `lint-format-float` ("exactly one definition, everything else re-exports") and sized like
  `lint-continuation-lock`'s arm parsing, whose scanner already has unit tests
  (`scripts/tests/test_continuation_lock_gate.py`, `Justfile:122`).
- **T2 · `split_admin_public_names_are_real_subcommands`** — extends the FM-CLUSTER-064 test at
  `register.rs:558-577`, walking `full_registry()`. Directly forces the `malloc-stats` class.
- **T3 · `every_declared_subcommand_is_grantable`** — for every container in `full_registry()`
  and every declared subcommand, `AclRule::parse_with(&registry, "+cmd|sub")` succeeds and
  round-trips through `ACL GETUSER`. Forces §2's *missing*-entry half (CLIENT, LATENCY, SCRIPT,
  COMMAND, HOTKEYS, STATUS, FT.CONFIG, JSON.DEBUG) in one loop that grows by itself.
- **T4 · the accepted-denied regression** (`integration_acl.rs`) — `+@all -memory|malloc-size`,
  then `MEMORY MALLOC-SIZE` must answer `NOPERM` and the denial must appear in `ACL LOG`. This
  is the end-to-end pin for §3 Trace B and is the test H4 lands with.

T3 also retires the maintenance burden the current design imposes: the vocabulary stops being
something anyone has to remember to update.

## Spec / LOCKED impact

None. No file in the set belongs to a locked crate; `frogdb-acl` has no failure-mode spec and no
`FM-` tag anywhere in `acl/src/`. The FM-tagged tests that *do* sit in touched files are
FM-CLUSTER-061/062/063/064 (`command_spec.rs:1609-1700`, `register.rs:558`) and
FM-CLUSTER-028/030 + FM-REPLICATION-042/046 (`guards.rs`). The `command_spec.rs` set is extended
(T2) but **not renamed and not weakened** — `just lint-failure-modes` checks spec↔test agreement
in both directions, so the row's `Forced by` list must still name every one of them. The
`guards.rs` set is untouched: this proposal edits `guards.rs:355`/`:367` (the two
`extract_subcommand` arguments), not the cluster-redirect logic those tags cover.
`just mutants-diff` is not owed (no locked crate); running it on `frogdb-core` is optional
diligence, not a gate.

## Risks / scope boundaries

### Sibling edges

- **90 / CT2 (`CommandSpec::DEFAULT` sweep) — same-file, resolved by design.** `CommandSpec`
  has no `Default` and no `DEFAULT` const at HEAD (verified: no `..CommandSpec::` or
  `impl Default for CommandSpec` anywhere), so every one of ~390 spec literals writes all 13
  fields. This proposal **deliberately declines the `CommandSpec` field** and uses a defaulted
  trait method instead, so it edits `command_spec.rs` only additively (a coherence assertion in
  the test module) and touches **zero** spec literals. Conflict surface with 90/CT2: one file,
  non-overlapping regions. *Sequencing note for a later round:* once `CommandSpec::DEFAULT`
  exists, moving `subcommands` onto the spec becomes a ~20-line change and is the tidier
  end-state — but it must land **after** 90/CT2, never with it.
- **68 (ExecFraming on `CommandSpec`) — ordering edge, same file.** 68 adds a field to
  `CommandSpec`; this proposal does not. If 68 lands first there is no interaction. If both are
  in flight, 68 owns the struct definition and this proposal owns the test module below it.
  State the boundary in both PRs.
- **67 (server small dedups) — disjoint.** 67's file set is `connection/builder.rs`,
  `connection.rs`, `connection/deps.rs`, `acceptor.rs`, `server/subsystems.rs`,
  `commands/search.rs`, `connection/search/helpers.rs`. Intersection with this proposal's set:
  **empty**. Land in any order.
- **Round-2 issue 35 (ACL category enforcement inert,
  `.scratch/testing-improvements-round2/issues/open/35-*.md`) — sibling, not overlap.** Same
  root cause (a `frogdb-acl` string table joined to the registry by nothing), different table:
  35 is `COMMAND_ALL_CATEGORIES` (`acl/src/categories/data.rs`, 1827 lines, 185 commands with no
  row); this is the subcommand vocabulary. The ratcheting gate for 35 already lives at
  `register.rs:580+` and its doc comment names the same fix direction ("populating the rows /
  moving categories onto `CommandSpec`"). The two share **no file** except `register.rs`'s test
  module. Doing 70 first makes 35 cheaper — the port and the registry-walking test scaffolding
  are reusable verbatim.

### Out of scope, but must be filed: the ACL-command self-exemption

`guards.rs:364` reads `if cmd_name != "ACL" && let Some(guard) = self.permission_guard()`. The
entire `ACL` command bypasses ACL enforcement. Consequences, verified by reading the three
enforcement sites (`guards.rs:362-370`, `guards.rs:586`, `routing.rs:67` — the latter two check
keys/channels only, and `acl_conn_command.rs` performs no permission check of its own,
`:172-187`):

- `-acl|setuser`, `-acl|getuser`, `-acl` and `-@admin` are all **inert against the ACL command**.
- A user authenticated as `-@all +acl|whoami` can run `ACL SETUSER` and grant themselves
  `+@all ~*`. The only mitigation is the admin port (`SPLIT_ADMIN_SURFACES` lists ACL's public
  set as `WHOAMI/CAT/GENPASS/HELP`, `command_spec.rs:604`), which is inactive unless
  `admin_enabled`.
- Redis has no such exemption; it expects operators to grant `+acl|whoami` explicitly.

This is a **CRITICAL** privilege-escalation path and it is *not* a vocabulary bug — fixing the
tables does not fix it, and this proposal's changes neither create nor worsen it. It must be
filed as its own security issue and fixed on its own timeline (the fix is narrow: drop the
exemption, add `+acl|whoami` to the default user's implicit grants). It is recorded here because
this census is what surfaced it, and because it explains why the parser's *correct* ACL arm
(`parser.rs:22-35`) has never been observed to matter.

### Other risks

- **The `-cmd|arg` tightening is a behavior change.** `-select|0` is accepted today and becomes
  an error under rule 3. No test in the tree writes it (grepped: `acl_tcl.rs:1831` uses `+`).
  It matches Redis. Call it out in the PR; it is a deviation-correcting break, which FrogDB's
  pre-production policy explicitly permits.
- **The `+get|foo` loosening is also a behavior change**, in the permissive direction: a
  first-arg rule on an arbitrary non-container command becomes writable, matching Redis. It
  grants nothing that `+get` does not already grant — a first-arg rule only ever *narrows*.
- **Declaring HOTKEYS / STATUS / FT.CONFIG / JSON.DEBUG makes previously-accepted garbage
  rejected.** `+hotkeys|nonsense` errors today (`_ => false`) and errors after; but
  `+ft.config|anything` also errors today, so no rule that works today stops working. Net: only
  new grants become possible.
- **The grep gate (T1) is heuristic.** The dispatchers are uniform (`match
  subcommand_str.as_ref()` for connection commands, `match subcommand.as_slice()` with `b"…"`
  literals for shard commands) but not identical, and `COMMAND`'s arms are spread over 250 lines
  (`basic.rs:145-400`). If a container resists the scanner, the honest fallback is the
  `lint-continuation-lock` idiom: a per-command count pin plus a named exemption, not a silent
  skip.
- **`frogctl` is unaffected.** `frogctl/src/commands/acl.rs` has no subcommand table (grepped);
  it forwards rule strings to the server, so it inherits whatever the server accepts.
- **Documentation.** Grepped `website/` and all `*.md` for `malloc-stats`: no hits. No docs
  restate the vocabulary, so no doc-sync is owed beyond a CHANGELOG entry for the two
  deviation-correcting breaks.

## Effort

**M.** Roughly: the port + `AclManager` wiring + parse rewrite (~120 lines net negative in
`frogdb-acl`), `subcommands()` on two traits + `CommandImpl` + the `CommandVocabulary` impl
(~40 lines in `frogdb-core`), ~20 one-line-list declarations across the container files,
`CONTAINER_COMMANDS` deletion + three call-site updates in `frogdb-server`, and the four test
artifacts. The scanner (T1) is the only genuinely open-ended piece; it is separable and can
land after the rest as a follow-up if it fights.

Ordering: **H4 first** (below), then the structural change, then T1. H4's regression tests are
written to survive the structural change unchanged — they assert observable ACL behavior, never
table contents.

## Independently-landable hotfixes

### H4 — correct the three tables, with regression tests (independent of everything above)

Minimal, mechanical, no design. **`frogdb-server/crates/acl/src/parser.rs`:**

| # | arm (line) | edit |
|---|---|---|
| 1 | `memory` `:101-104` | `"malloc-stats"` → `"malloc-size"` |
| 2 | `client` `:36-54` | add `"unblock"`, `"stats"` |
| 3 | `latency` `:96-99` | add `"bands"`, `"doctor"`, `"histogram"` |
| 4 | `script` `:114` | add `"kill"`; drop `"debug"` (no such arm) |
| 5 | `command` `:85-88` | add `"getkeysandflags"` |
| 6 | new arm | `"hotkeys" => matches!(sub, "start" \| "stop" \| "reset" \| "get")` |
| 7 | new arm | `"status" => matches!(sub, "json" \| "help")` |
| 8 | new arm | `"ft.config" => matches!(sub, "get" \| "set")` |
| 9 | new arm | `"json.debug" => matches!(sub, "memory")` — behind no cfg; the table is compile-agnostic today and stays so |
| 10 | `cluster` `:56-83` | **leave the five phantoms** (`bumpepoch`, `count-failure-reports`, `flushslots`, `links`, `slaves`). They are Redis subcommands FrogDB has not implemented; removing them would reject rules that will become valid, and they grant nothing today. Add a one-line comment saying so, so the next reader does not "fix" it |
| 11 | `module` `:105` | same reasoning — MODULE is a `NotImplemented` stub; leave, comment |

**`frogdb-server/crates/server/src/connection/util.rs:250-253`:** add `"FT.CONFIG"` and
`"JSON.DEBUG"` to `CONTAINER_COMMANDS` — without this, edits 8 and 9 parse and are never
enforced.

**`frogdb-server/crates/core/src/command_spec.rs`:** no edit. `SPLIT_ADMIN_SURFACES` is correct.

**Regression tests (the point of the hotfix, and the part that must not be skipped):**

1. `integration_acl.rs` — `+@all -memory|malloc-size`, then `MEMORY MALLOC-SIZE` ⇒ `NOPERM`,
   and the denial appears in `ACL LOG`. *(This test fails today at the `ACL SETUSER` step, which
   is the proof the fix is needed.)*
2. `integration_acl.rs` — `-@all +client|stats` ⇒ `CLIENT STATS` allowed, `CLIENT KILL` denied;
   and `+@all -client|unblock` ⇒ `CLIENT UNBLOCK` denied, `CLIENT ID` allowed.
3. `acl/src/parser.rs` unit tests — a table-driven case per corrected entry: every name in
   edits 1-9 parses under both `+` and `-`.
4. `integration_acl.rs` — `-@all +ft.config|get` ⇒ `FT.CONFIG GET` allowed, `FT.CONFIG SET`
   denied. (Needs `cmd-full`; if the regression harness's feature set makes this awkward, drop
   to a `parser.rs` unit test and note it — the `util.rs` half is still required.)

**Not in H4:** the `guards.rs:364` ACL self-exemption (separate security issue), the ACL
category table (round-2 issue 35), and anything structural. H4 is ~40 lines of table edits plus
~150 lines of tests, and every one of those tests stays green — untouched — through the
structural change.
