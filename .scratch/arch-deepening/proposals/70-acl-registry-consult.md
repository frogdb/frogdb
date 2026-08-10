# Proposal 70 — ACL subcommand vocabulary: consult the `CommandRegistry`, delete the hand-rolled tables

Round 38 · lane: ACL / command identity · effort **M** · candidate SV10 · carries hotfix **H4**

**Revision 2** — re-verified against `4372082285b34079ae6c1eb0c2d135a55d91ca83` (worktree
`arch-round-38-99`) after adversarial review (verdict AMEND). Every citation below was
re-derived by reading at that SHA, and re-confirmed current at `50118a53` — the intervening
commits touch only `.scratch/arch-deepening/proposals/`, no source file cited here
(`git diff --stat 4372082..50118a53` = proposal markdown only). Revision 2 **retracts the revision-1 headline** (the
`malloc-stats` "exactly once repo-wide" claim was false — see §3), **corrects three declaration
sites and one sibling-edge claim**, **makes H4 purely additive** (revision 1's edits 1 and 4 were
not standalone-safe), and **adds two merge-blocking issue preconditions** (§Risks). Revision 1's
own corrections stand: three lane-brief claims corrected, seven unnamed drifts found (§Problem).

## Summary

FrogDB describes "which subcommands does container command X accept" in **five** places, four
of which are hand-written string lists that nothing forces to agree:

1. `frogdb-acl/src/parser.rs::is_valid_subcommand` (`:17-125`, 109 lines) — the ACL rule
   vocabulary. Gates `ACL SETUSER +cmd|sub` / `-cmd|sub` at parse time.
2. `frogdb-server/.../connection/util.rs::CONTAINER_COMMANDS` (`:250-254`) — *which* commands
   have a subcommand at `args[0]`. Drives ACL **enforcement** and admin-port gating.
3. `frogdb-core/src/command_spec.rs::SPLIT_ADMIN_SURFACES` (`:584-629`) — the per-subcommand
   admin classification. (Accurate today, and fail-closed by construction — it is the reference
   that proves #1 wrong.)
4. `frogdb-core/src/command_spec.rs`'s **test-side second copy** of #3 — the `cases` literal
   inside `admin_surface_split_table_classification` (`:1692-1727`, FM-CLUSTER-064 at `:1689`),
   which re-types CONFIG/ACL/CLIENT/MEMORY's public *and* admin subcommand names by hand.
5. The dispatchers themselves — ~20 `match subcommand { … }` blocks across six crates, which are
   the only description that is true by definition.

The registry (`frogdb-core/src/registry.rs`) is already the single source of command identity —
`CommandRegistry` keys ~390 entries by name and every entry carries a `CommandSpec`. It has
**no subcommand granularity at all**: `entries: HashMap<String, CommandEntry>` is keyed by
top-level name only (`registry.rs:163-167`), and `CommandSpec` (`command_spec.rs:469-507`,
13 fields) has no subcommand field. So "consult the registry" is not a pure refactor — the
registry must first *learn* subcommand identity. That is the substance of this proposal.

**Live-vs-latent headline (rewritten in revision 2).** The headline harm is **not**
`memory|malloc-stats` — see §3 for the retraction — it is the **rejected narrow grant that
forces a wide one**, and its worst instance is **`FT.CONFIG`**. `FT.CONFIG` is absent from the
ACL vocabulary entirely (`parser.rs:123` `_ => false`), so `+ft.config|get` is a parse error and
an operator who wants read-only visibility into search configuration must grant `+ft.config`,
which carries `FT.CONFIG SET` — a global search-config **write**. A rejected narrow grant that
forces a wide one is privilege escalation by workaround, and it is **live today**. The same
shape, same severity class, applies to three more real disclosure surfaces:

- **`CLIENT|STATS`** (`client_conn_command.rs:98`) — `-@all +client|stats` is rejected; the
  operator must grant `+client`, which carries `KILL`, `PAUSE`, `UNBLOCK`, `LIST`.
- **`HOTKEYS|GET`** (`hotkeys.rs:89`) — discloses key names and access frequencies
  (`command_spec.rs:579-583`); "grant GET, deny START/STOP/RESET" is unwritable.
- **`LATENCY|LATEST`** (`observability_conn_command.rs:562`) — cannot be granted without also
  granting `LATENCY RESET`.

All four fail **loudly** at `ACL SETUSER`, so nothing is silently mis-enforced; the harm is
entirely the operator's forced widening. The accepted-denied *mechanism* — a rule that parses,
round-trips through `ACL GETUSER`, and is then never consulted because the dispatcher spells the
subcommand differently — is **confirmed live** (§3) and is what the whole class of drift can
produce; the one instance in the tree today (`memory|malloc-stats`) happens to be low-impact.

Beyond the brief: **LATENCY** is missing three subcommands, **SCRIPT** is missing `KILL`,
**COMMAND** is missing `GETKEYSANDFLAGS`, and four whole containers — **HOTKEYS, STATUS,
FT.CONFIG, JSON.DEBUG** — are absent from the vocabulary entirely.

**Two out-of-scope security findings, discovered by this census. Both must be filed as issues
*before* this proposal merges** (§Risks carries the full evidence chains):

1. `guards.rs:364` exempts the entire `ACL` command from ACL enforcement, **unconditionally** —
   a `-@all` user with zero grants can run `ACL SETUSER` on itself. **CRITICAL.**
2. `AclManager::set_user` (`manager.rs:167-191`) applies rules **in place**, so a rule list that
   fails to parse halfway leaves the user created, enabled, passworded and partially granted
   *and* returns `ERR`. **HIGH**, and it falsifies this proposal's own earlier claim that "the
   whole `ACL SETUSER` fails".

The structural fix: give the registry subcommand identity (a defaulted trait method next to each
dispatch `match`, **not** a `CommandSpec` field — see §Sibling edges for why that matters), have
`frogdb-acl` consult it through an inverted port (the crate graph forbids the direct
dependency), and delete tables #1 and #2 outright.

## Files involved

| path | lines | role in this proposal |
|---|---:|---|
| `frogdb-server/crates/acl/src/parser.rs` | 901 | **the change.** `is_valid_subcommand` `:17-125` — **deleted in full** (109 lines). Its two call sites `:373` (`+`) and `:422` (`-`); the double-pipe rejections `:366-371`/`:415-420`; `AclRule::{AllowSubcommand,DenySubcommand}` construction `:383-386`/`:432-435`; `apply` `:469-572` (subcommand arms `:538-561`); `parse_and_apply_rules` `:576-582`; `parse_acl_line` `:585-613` (rule parse at `:609` — the aclfile path, see B2 note under §Risks) — all become vocabulary-aware |
| `frogdb-server/crates/acl/src/manager.rs` | 623 | **the change.** `set_user` `:167-191` (parse at `:183`) and the aclfile loader `load` `:280-321` (per-line parse at `:300-302`) are the two entry points that must carry the vocabulary port; `AclManager` struct `:57`, ctor `:86`. **`set_user` mutates in place** (`:178-185`) — the non-atomic defect, §Risks precondition 2 |
| `frogdb-server/crates/acl/src/permissions.rs` | 630 | **read-only evidence.** `SubcommandRule` `:112-119`, `is_command_allowed` `:215-266` — subcommand rules are matched by **exact lowercase string** (`:222-223`, returning at `:225`) and checked **first, most-specific-wins** (`:219-228`), which is exactly why a misspelled rule is inert rather than an error; the fall-through lands on `if self.allow_all { return true }` at `:249`. `PermissionSet::check_command` `:432-434` is the thin wrapper the guard calls. `Display for AclCommandRule` `:145-164` is the `ACL GETUSER` echo (subcommand arms `:154-161`) |
| `frogdb-server/crates/acl/src/lib.rs` | 66 | **the change.** Re-export the new `CommandVocabulary` port alongside `SubcommandRule` `:61` |
| `frogdb-server/crates/acl/Cargo.toml` | 20 | **read-only evidence.** Deps are `frogdb-types` only — **no `frogdb-core`**, and none can be added (see the cycle below) |
| `frogdb-server/crates/core/Cargo.toml` | — | **read-only evidence.** `frogdb-acl.workspace = true` at `:38` — `core → acl`. The direction that forces the port |
| `frogdb-server/crates/core/src/registry.rs` | 506 | **the change.** `CommandImpl` `:29-36` gains `subcommands()` (dispatching over both variants like `name()` `:46-51`); `CommandRegistry` `:163-168` (`entries` at `:167`); `impl CommandVocabulary for CommandRegistry` lands here |
| `frogdb-server/crates/core/src/command.rs` | 2014 | **the change.** One defaulted method on the `Command` trait (`&'static [&'static str]`, default `&[]`) |
| `frogdb-server/crates/core/src/conn_command.rs` | 1172 | **the change.** The same defaulted method on `ConnectionCommand` |
| `frogdb-server/crates/core/src/command_spec.rs` | 1778 | **the change (additive only).** `SPLIT_ADMIN_SURFACES` `:584-629` **stays** — it is policy, not identity — and gains a coherence assertion. `AdminSurface` `:519-533`, `requires_admin` `:541-550` (fail-closed `None => true` at `:547`), `admin_surface` `:637-651`, `split_admin_surface_commands` `:653-655`. **`CommandSpec` `:469-507` is deliberately NOT edited** (see §Sibling edges). FM-tagged tests at `:1609`, `:1621`, `:1632`, `:1654`, `:1667`, `:1679`, `:1689` (FM-CLUSTER-061/062/063/064) must keep their names and semantics — note `:1689`'s body holds the **fifth** hand-written subcommand table (`cases`, `:1692-1727`) |
| `frogdb-server/crates/server/src/connection/util.rs` | 503 | **the change.** `CONTAINER_COMMANDS` `:250-254` — **deleted**. `extract_subcommand` `:257-267` is **split in two** (see §Proposed change part 4): a registry-only `container_subcommand` for the admin gate and a user-aware `acl_subcommand` for the two ACL sites |
| `frogdb-server/crates/server/src/connection/guards.rs` | 1886 | **the change.** The ACL enforcement seam `:362-371` (incl. the `cmd_name != "ACL"` exemption at `:364` — §Risks precondition 1) and the admin gate `:347-360` (`extract_subcommand` at `:355`) are two of the three `extract_subcommand` callers; the ACL one is at `:367`, feeding `guard.check_command` at `:368` → `permission_guard.rs:97` → `permissions.rs:432` → `is_command_allowed` `:215`. Carries FM-CLUSTER-028/030 and FM-REPLICATION-042/046 tagged tests (`:1296`, `:1567+`) — untouched by this change |
| `frogdb-server/crates/server/src/connection/permission_guard.rs` | — | **read-only evidence.** `check_command` `:97-125` — the single enforcement funnel between `guards.rs:368` and `frogdb-acl`; also where the `ACL LOG` denial line is written |
| `frogdb-server/crates/server/src/connection/acl_conn_command.rs` | 671 | **the change.** `acl_dryrun` `:233+` — the third `extract_subcommand` caller (`:249`); `handle_acl` dispatch `:76-110` (13 arms in the `match` at `:84-109`) is one of the containers that declares its arms. **Read-only evidence for §Risks precondition 1:** `ACL_SPEC` `:33-49` carries `flags: CommandFlags::empty()` (`:38`) — no whole-command `ADMIN` — and `acl_setuser` `:172-187` performs **no permission check of its own** |
| `frogdb-server/crates/server/src/server/register.rs` | 922 | **the change (tests).** `full_registry()` `:288-291` and `split_admin_surfaces_agree_with_command_flags` `:558-577` (FM-CLUSTER-064 tag at `:556`) — the existing registry-walking precedent this proposal extends. The ratcheting ACL-category gate (`acl_category_gap_allowlist` `:614`, `every_registered_command_has_acl_category_or_is_allowlisted` `:857`, doc `:582-589`) is the sibling gate for issue 35. ACL itself is registered at `:122` |
| `frogdb-server/crates/server/src/server/mod.rs` | — | **the change (wiring).** `registry: Arc<CommandRegistry>` `:82` and `acl_manager: Arc<AclManager>` `:134` are fields of the **same** struct — 52 lines apart, not adjacent — both populated from `infra` (`:382`, `:425`/`:480`). This is where the vocabulary port is injected into `AclManager` |
| `frogdb-server/crates/server/src/connection/client_conn_command.rs` | 1079 | **the change (declaration).** CLIENT's 19 arms `:80-105` |
| `frogdb-server/crates/server/src/connection/observability_conn_command.rs` | 1349 | **the change (declaration).** SLOWLOG `:162-171`, MEMORY `:322-333`, LATENCY `:555-568`, STATUS `:842-848` |
| `frogdb-server/crates/server/src/connection/conn_command.rs` | 806 | **the change (declaration).** CONFIG's 5 arms `:259-269` |
| `frogdb-server/crates/server/src/connection/hotkeys.rs` | 557 | **the change (declaration).** HOTKEYS `:85-94` |
| `frogdb-server/crates/server/src/connection/scripting_conn_command.rs` | — | **the change (declaration) — CROSS-FILE.** `impl ConnectionCommand for ScriptConnCommand` `:272-274` (`SCRIPT_SPEC` `:248-264`) and `impl ConnectionCommand for FunctionConnCommand` `:398-400` (`FUNCTION_SPEC` `:376-390`) live **here**, not next to their dispatch `match`es. The `subcommands()` override for SCRIPT and FUNCTION must be written in this file |
| `frogdb-server/crates/server/src/connection/scripting/script.rs` | 351 | **read-only for the declaration; the dispatch source.** SCRIPT's `match` `:20-31` (arms `:21-25`) — a *different file* from the trait impl above. T1 must resolve across this boundary |
| `frogdb-server/crates/server/src/connection/scripting/function.rs` | 410 | **read-only for the declaration; the dispatch source.** FUNCTION's `match` `:84-98` (arms `:85-93`) — same cross-file split |
| `frogdb-server/crates/server/src/connection/pubsub_conn_command.rs` | — | **the change (declaration).** PUBSUB `:539-549` |
| `frogdb-server/crates/server/src/commands/cluster/mod.rs` | 1211 | **the change (declaration).** CLUSTER's 19 arms `:123-219` |
| `frogdb-server/crates/commands/src/basic.rs` | 1054 | **the change (declaration).** COMMAND `:145-442` (7 arms) |
| `frogdb-server/crates/commands/src/generic.rs` | 736 | **the change (declaration).** OBJECT `:355-480` (5 arms) |
| `frogdb-server/crates/commands/src/stream/info.rs` | 326 | **the change (declaration).** XINFO `:43-62` — behind `stream` |
| `frogdb-server/crates/commands/src/stream/consumer_groups.rs` | 406 | **the change (declaration).** XGROUP `:51-70` — behind `stream` |
| `frogdb-server/crates/commands/src/json/basic.rs` | 467 | **the change (declaration).** JSON.DEBUG `:424+` — behind `json` |
| `frogdb-server/crates/server/src/commands/search.rs` | — | **the change (declaration) — CRATE-CROSSING.** `impl Command for FtConfigCommand` opens at `:757` (`SPEC` `:759-773`, `name: "FT.CONFIG"` `:760`) and is in **`frogdb-server`**; its dispatch `match` is in **`frogdb-core`** (next row). The `subcommands()` override must be written here, one crate away from the arms it mirrors |
| `frogdb-server/crates/core/src/shard/search/config.rs` | 99 | **read-only for the declaration; the dispatch source.** `ShardWorker::execute_ft_config` `:8-79`, `match` `:20-78` (GET `:21`, SET `:59`) |
| `frogdb-server/crates/server/src/commands/stub.rs` | — | **read-only evidence.** MODULE `:79-128`: `SPEC` `:81-95` (`name` `:82`), `is_stub` `:99-101`. It is **not** subcommand-free — `execute` `:103-127` answers `HELP` for real (`:106-125`) and only then falls through to `NotImplemented` (`:126`). So MODULE's honest declaration is `["HELP"]`, and `parser.rs:105` has **4** phantoms, not 5 |
| `frogdb-server/crates/redis-regression/tests/acl_tcl.rs` | — | **read-only evidence + new tests.** `tcl_acls_cannot_include_unknown_subcommand` `:553-570` (`+config|asdf` and `-config|asdf` must error); `tcl_acls_cannot_include_command_with_two_args` `:572-587` (`+get|key1|key2` **and** `-get|key1|key2` must error — asserts only the `ERR` prefix, never the message text, see §8); `tcl_acls_can_block_select_of_all_but_a_specific_db` `:1816-1841` (`+select|0` at `:1831`, `SELECT 1` ⇒ `NOPERM` at `:1840`); `tcl_acl_regression_memory_leaks_adding_removing_subcommands` `:1872-1889` (`+debug|a/b/c` accepted at `:1880`); `+memory|doctor` `:918`, `-memory|doctor` `:939`. These pin the semantics any redesign must preserve |
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

### 1. The census: five descriptions of one fact

`is_valid_subcommand` (`parser.rs:17-125`) is a 17-arm `match` over lowercase command names,
each arm a `matches!` over a lowercase subcommand list, with two blanket `true` arms (`debug`,
`select`) and a `_ => false` floor. It is consulted at exactly two places — `parser.rs:373`
(`+cmd|sub`) and `parser.rs:422` (`-cmd|sub`) — and a miss is a hard parse error:

```
"Unknown command or subcommand '{}|{}'"   // parser.rs:377, :426
```

which `AclManager::set_user` propagates via `?` at `manager.rs:183`, so the command answers
`ERR`. **It does not, however, undo what it already did** — see §Risks precondition 2; revision 1
of this proposal claimed "the whole `ACL SETUSER` fails" and that claim is retracted here.

`CONTAINER_COMMANDS` (`util.rs:250-254`) is a flat 19-name list feeding `extract_subcommand`
(`util.rs:257-267`), which returns `args[0].to_uppercase()` (`:263`) for a listed command and
`None` otherwise. Three callers: the admin gate (`guards.rs:355`), the ACL enforcement seam
(`guards.rs:367`), and `ACL DRYRUN` (`acl_conn_command.rs:249`).

The two tables are joined by nothing. Table #1 decides whether a rule may be *written*; table #2
decides whether a written rule is ever *consulted*. A name that appears in #1 but is spelled
differently by the dispatcher parses fine and never matches.

**Copies #3 and #4 are joined to each other by nothing either.** `SPLIT_ADMIN_SURFACES`
(`command_spec.rs:584-629`) is re-typed by hand inside its own pinning test — the `cases` literal
at `command_spec.rs:1692-1727` restates CONFIG/ACL/CLIENT/MEMORY's public *and* admin
subcommand names as string literals. That test is the **strongest available evidence that the
drift is a copy problem and not an oversight**: at `:1719` it names `"UNBLOCK"` and `"STATS"` as
CLIENT subcommands, and at `:1724` it names `"MALLOC-SIZE"` as a MEMORY subcommand — the exact
three names `parser.rs` gets wrong. Two files in `frogdb-core` have always known the truth that
`frogdb-acl` never learned.

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
| `memory` | doctor, help, **`malloc-stats`**, purge, stats, usage (6) — **this is the exact Redis MEMORY subcommand list** | doctor, help, **`MALLOC-SIZE`**, purge, stats, usage — `observability_conn_command.rs:323-328` | **1 wrong name — but the *dispatcher* is what drifted** (§3) |
| `module` | help, list, load, loadex, unload (5) | **`help` is real** — `stub.rs:106-125` answers `MODULE HELP` with a subcommand list; everything else hits `NotImplemented` at `stub.rs:126` | **4 phantom** (list, load, loadex, unload); `help` is correct |
| `object` | encoding, freq, help, idletime, refcount (5) | same 5 — `generic.rs:356,435,446,460,472` | — |
| `pubsub` | channels, help, numpat, numsub, shardchannels, shardnumsub (6) | same 6 — `pubsub_conn_command.rs:540-545` | — |
| `script` | **debug**, exists, flush, help, load (5) | exists, flush, help, **kill**, load — `script.rs:21-25` | **1 missing** (kill), **1 phantom** (debug) |
| `select` | `true` (blanket) | not a container; first-arg rule | matches Redis (§8) |
| `slowlog` | get, help, len, reset (4) | same 4 — `observability_conn_command.rs:163-166` | — |
| `xgroup` | create, createconsumer, delconsumer, destroy, help, setid (6) | same 6 — `consumer_groups.rs:52-57` | — (but `stream`-gated, §7) |
| `xinfo` | consumers, groups, help, stream (4) | same 4 — `info.rs:44-47` | — (but `stream`-gated, §7) |
| **`hotkeys`** | *absent* → `_ => false` | start, stop, reset, get — `hotkeys.rs:86-89` | **whole container missing** |
| **`status`** | *absent* → `_ => false` | json, help — `observability_conn_command.rs:843-844` | **whole container missing** |
| **`ft.config`** | *absent* → `_ => false` | get `:21`, set `:59` — `core/src/shard/search/config.rs` | **whole container missing**, and also absent from `CONTAINER_COMMANDS` |
| **`json.debug`** | *absent* → `_ => false` | memory `:426`, **help `:445`** — `json/basic.rs` | **whole container missing** (2 arms, not 1), and also absent from `CONTAINER_COMMANDS` |

`CONTAINER_COMMANDS` (`util.rs:250-254`) separately omits **`FT.CONFIG`** and **`JSON.DEBUG`**,
so those two need a **two-table** fix: correcting only `parser.rs` would let the rule be written
and still never enforced.

`SPLIT_ADMIN_SURFACES` (`command_spec.rs:584-629`) was diffed the same way and is **correct at
HEAD** — every `public` name for CLUSTER (`:588-598`), CONFIG (`:602`), ACL (`:605`), CLIENT
(`:610-623`) and MEMORY (including `"MALLOC-SIZE"` at `:627`) is a real dispatcher arm. It is the
*only* hand-written vocabulary table in the census that has stayed true, for a structural reason worth
naming: it fails **closed** (`requires_admin` returns `true` for `None` at `command_spec.rs:547`
and for unlisted names at `:546`), so a stale entry over-gates and someone notices. `parser.rs`
fails **open** in the direction that matters — a stale entry produces a rule that is silently
never consulted.

### 3. `malloc-stats` — the accepted-denied MECHANISM, at low impact (revision-1 claim retracted)

**Retraction first.** Revision 1 said `malloc-stats` "appears exactly once repo-wide, at
`parser.rs:103`. It is not an alias anyone implemented." **Both sentences are false.** Re-grepped
at HEAD, `malloc-stats` / `MALLOC-STATS` appears in **six** places:

| site | what it says |
|---|---|
| `frogdb-server/crates/acl/src/parser.rs:103` | the ACL vocabulary entry |
| `website/src/data/compat-exclusions.json:1611` | `"Coverage: MEMORY MALLOC-STATS"`, category `redis-specific`, reason **"jemalloc-only subcommand"** |
| `frogdb-server/crates/redis-regression/tests/other_tcl.rs:18` | the same exclusion, restated in the harness header |
| `frogdb-server/crates/redis-regression/tests/memefficiency_tcl.rs:25` | names `MEMORY MALLOC-STATS` as a jemalloc-coupled surface |
| `frogdb-server/crates/redis-regression/tests/memefficiency_tcl.rs:30` | "`MEMORY MALLOC-STATS` returns `ERR unknown subcommand`" — the deviation, already known |
| `.scratch/replication-cluster-rework/admin-flag-audit-2026-08-04.md:94` | classifies `MALLOC-STATS` as non-admin in a prior audit |

So the direction of the drift is the **opposite** of what revision 1 asserted:
`MEMORY MALLOC-STATS` is the **real Redis subcommand**, with a documented, deliberate FrogDB
compatibility exclusion. `parser.rs:101-104` is the **exact Redis MEMORY list** and is faithful
to it. What drifted is the **dispatcher**: `MEMORY MALLOC-SIZE` is a FrogDB invention with no
Redis counterpart — a stub that parses its argument as an `i64` and echoes it back
(`observability_conn_command.rs:375-388`, pinned as such by
`frogdb-server/crates/server/tests/integration_admin.rs:617-627` — comment "stub behavior" at
`:621` — and by `observability_conn_command.rs`'s own unit tests
`memory_malloc_size_echoes_input` `:1070` and `memory_malloc_size_bad_value_errors` `:1079`).

**The mechanism is nonetheless confirmed, and it is the reason this class of drift matters.**

*Trace A — legitimate grant rejected (real, LOW).* `+memory|malloc-size` reaches `parser.rs:373`,
`is_valid_subcommand("memory","malloc-size")` returns `false`, `AclRule::parse` returns
`ERR ... Unknown command or subcommand 'memory|malloc-size'` (`:377`). The only recourse is
`+memory`, which also grants `MEMORY PURGE` — admin-only, because MEMORY's `public` list at
`command_spec.rs:627` omits it and the surface fails closed at `:546`. Impact is low only
because the target is a stub.

*Trace B — explicit deny is inert (real, LOW impact / HIGH shape).* `+@all -memory|malloc-stats`
parses (`"malloc-stats"` is in the list), `apply` stores
`SubcommandRule { command: "memory", subcommand: "malloc-stats", allowed: false }`
(`parser.rs:550-561`), `ACL GETUSER` echoes `-memory|malloc-stats` back
(`permissions.rs:158-161`), and the operator believes the deny landed. At runtime
`extract_subcommand` yields `Some("MALLOC-SIZE")` (`util.rs:263`), `is_command_allowed`
lowercases to `"malloc-size"`, compares against `"malloc-stats"` at `permissions.rs:222-223`,
**does not match**, falls out of the loop at `:228`, and hits
`if self.allow_all { return true }` at `permissions.rs:249` — so bob runs
`MEMORY MALLOC-SIZE`. Every step of that chain is verified; the only thing that makes this
instance benign is that `MEMORY MALLOC-SIZE` discloses nothing.

**Why it still belongs in this proposal.** The rule "an unmatched subcommand rule falls out of
the loop and the decision reverts to the whole-command verdict" (`permissions.rs:219-228` then
`:249`) is a **general fail-open**: *any* future misspelling in `parser.rs` produces a `+OK`
denial that does nothing, against whatever surface that subcommand happens to guard. The class
is what is HIGH; today's single instance is LOW. That is exactly the argument for a gate (T1)
rather than a table edit.

The same inert-deny shape applies to every phantom entry in the table above (`-script|debug`,
`-cluster|links`, `-module|load`, …); those are **latent** only because the subcommands do not
exist, and each becomes live the moment someone implements one under a different spelling.

### 4. CLIENT `unblock` / `stats` — LIVE, severity MEDIUM-HIGH (fails loud, harms by workaround)

`client_conn_command.rs:97-98` dispatches `UNBLOCK` and `STATS`; `parser.rs:36-54` lists neither.
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
`command_spec.rs:610-623`) — and the FM-CLUSTER-064 pinning test names them **explicitly** as
CLIENT's admin subcommands at `command_spec.rs:1719`. Two `frogdb-core` sites have known these
names all along. A deployment with an admin port is protected; a deployment relying on ACLs
alone is not — and per §Risks precondition 1, the admin port is off by default
(`config/src/admin.rs:46`).

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
- **FT.CONFIG** (`core/src/shard/search/config.rs`: GET `:21`, SET `:59`) — **the worst of the
  four, and the headline of this proposal**. `FT.CONFIG SET` writes global search configuration.
  `+ft.config|get` is rejected, so an operator wanting read-only search-config visibility must
  grant `+ft.config`, which includes `SET`. A rejected narrow grant that forces a wide one is
  privilege escalation by workaround. FT.CONFIG is *also* absent from `CONTAINER_COMMANDS`, so
  even after fixing `parser.rs` the rule would parse and never be consulted — this one needs
  both tables.
- **JSON.DEBUG** (`json/basic.rs:425-455`: **MEMORY `:426` and HELP `:445`** — revision 1 said
  `MEMORY` only, which is wrong) — same two-table shape, behind the `json` feature.

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
| `+cmd|sub` where `cmd` has a subcommands dict and `sub` is unknown → `ENOENT` error | error (`parser.rs:373-382`) — pinned by `acl_tcl.rs:553-570` (`+config|asdf`) | matches |
| `-cmd|sub`, same → `ENOENT` error | error (`parser.rs:422-431`) — pinned by the same test (`-config|asdf`, `:566-569`) | matches |
| `+cmd|a|b` (two pipes) → error. Redis's message differs by case: "allowing first-args of a subcommand is not allowed" **only when `a` is a real subcommand of `cmd`**; `+get|a|b` (GET has no subcommands dict) is a plain `ENOENT` | error in both cases, **always with the first-args message** (`parser.rs:366-371` / `:415-420`) — pinned by `acl_tcl.rs:572-587`, which asserts only the `ERR` prefix and so cannot distinguish | **error-parity matches, message does not**. Deviation, LOW. Revision 1's "error, same message" row is retracted. Not worth closing: the registry knows which prefixes are containers, so the correct message is derivable — do it or don't, but do not claim parity |
| `+cmd|arg` where `cmd` has **no** subcommands → an **allowed-first-arg** rule (`ACLAddAllowedFirstArg`), e.g. `+select|0`, `+debug|a` | two blanket `true` arms hard-code exactly `select` and `debug` (`parser.rs:93`, `:115`) — pinned by `acl_tcl.rs:1816-1841` (`+select|0` at `:1831`) and `:1872-1889` (`+debug|a/b/c` at `:1880`) | matches **only for those two names**; `+get|foo` is rejected where Redis accepts. Latent compat gap, LOW |
| `-cmd|arg` on a non-container → error (first-arg rules are `+`-only) | `-select|0` **and** `-debug|<arg>` are accepted (blanket `true`) and stored — and `-debug|<arg>` is additionally **enforced today**, because `DEBUG` is in `CONTAINER_COMMANDS` (`util.rs:251`) | deviation, LOW; the proposed rule fixes it, but it deletes a working deny — see §Other risks |

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
place that already holds the rest of them.

**Locality — qualified, because the tree is not uniform.** For ~17 of the ~20 containers the
claim holds exactly: the trait impl and the dispatch `match` are in the same file, so adding a
subcommand means editing the `match` and the list beside it. **Three are not**, and this is a
real cost the design must carry rather than paper over:

| container | trait impl (where `subcommands()` goes) | dispatch `match` (what it must mirror) | gap |
|---|---|---|---|
| SCRIPT | `connection/scripting_conn_command.rs:272` | `connection/scripting/script.rs:20-31` | different file, same crate |
| FUNCTION | `connection/scripting_conn_command.rs:398` | `connection/scripting/function.rs:84-98` | different file, same crate |
| FT.CONFIG | `server/src/commands/search.rs:757` (**`frogdb-server`**) | `core/src/shard/search/config.rs:20-78` (**`frogdb-core`**) | different **crate** |

For these three, T1's scanner must either resolve the declaration-to-`match` link across files
(the `frogdb-server` → `frogdb-core` hop makes a purely lexical, single-file scanner impossible)
or carry a **named, three-entry exemption list** with the paired paths written down, in the
`lint-continuation-lock` idiom. Silent skipping is not an option — an unscanned container is
exactly where the next drift lands. This is sized in §Effort as part of T1's
open-endedness.

**Leverage**: five hand-written tables become one, and the one that survives is the one that
cannot drift, because a lint pins it against the `match` it describes.

The **seam** belongs between *command identity* (core) and *permission policy* (acl), and its
direction is forced by the crate graph, not chosen: `frogdb-core` already depends on
`frogdb-acl` (`core/Cargo.toml:38`), so `frogdb-acl` **cannot** depend on `frogdb-core`. The
consumer must therefore define the port and the producer must implement it — dependency
inversion, and the reason this is an M and not an S.

**Deletion test, both directions.** Delete `is_valid_subcommand`: nothing reappears anywhere —
every name it holds is already stated by a `match` arm the dispatcher needs regardless (with the
one exception of the Redis-real-but-unimplemented phantoms, which is why H4 edits 10/11 keep
them). Delete `CONTAINER_COMMANDS`: same. Delete the proposed `subcommands()` method: the
vocabulary immediately reappears in **four** places (ACL parse, ACL enforcement, the admin
table's coherence check, and that table's own pinning test at `command_spec.rs:1692-1727`),
comfortably past the "two or more adapters" bar for introducing a seam at all.

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
construction where `registry: Arc<CommandRegistry>` (`server/mod.rs:82`) and
`acl_manager: Arc<AclManager>` (`server/mod.rs:134`) are fields of the same struct — 52 lines
apart, both populated from the same `infra` (`:382`, `:425`/`:480`), so the wiring is a
constructor-order question, not a plumbing one. `is_valid_subcommand` is replaced by
`AclRule::parse_with(vocab, rule)`; the existing `AclRule::parse` keeps working against a `None`
vocabulary. Both `frogdb-acl` entry points must carry it: `set_user` (`manager.rs:167-191`,
parse at `:183`) **and** `load` (`manager.rs:280-321`, parse at `:300-302` via
`parse_acl_line` → `AclRule::parse` at `parser.rs:609`).

**Fail-open at parse time is deliberate and safe.** A `None` vocabulary (unit tests, `frogctl`,
any embedding that has no registry) accepts every `cmd|sub` pair. This is not a weakening:
parse-time validation is a *usability* guard that catches typos, never a security guard —
enforcement is exact-string matching either way (`permissions.rs:220-227`), so an unvalidated
rule can only ever grant/deny a string nothing dispatches. Stating this explicitly is what lets
`frogdb-acl` keep its 30+ existing unit tests dependency-free.

**2 — `frogdb-core` gains subcommand identity**, as a defaulted trait method on both executor
traits, declared **at the trait impl** — which is the same file as the dispatch `match` for ~17
of ~20 containers, and a different file (SCRIPT, FUNCTION) or a different crate (FT.CONFIG) for
the three named in the locality table above:

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
| two or more `\|` | error, unchanged | `parser.rs:366-371` / `:415-420` |

`+select|0` and `+debug|a` keep working — not by name, but because SELECT and DEBUG declare no
subcommands, which is exactly why Redis treats them as first-arg rules. **DEBUG must keep
declaring `&[]`** even though `debug_conn_command.rs` has a large `match` (`:116`): Redis has no
DEBUG subcommands dict, and `acl_tcl.rs:1872-1889` pins `+debug|a/b/c` (`:1880`). The rule for
what to declare is "is this a Redis container command", not "does the dispatcher have a match
statement".

**Own the loosening in row 3.** Sending an *unknown command* down the first-arg path is a real
weakening of today's behavior, and it must be stated plainly rather than folded into the
portability argument: `+cofnig|set` is a hard parse error today (`_ => false`, `parser.rs:123`)
and becomes a silently-inert first-arg rule under rule 3 — the operator's typo returns `+OK`,
`ACL GETUSER` echoes it, and it grants nothing. That is the **same fail-open shape §2 indicts**,
re-introduced at the command level instead of the subcommand level. Two options; this proposal
picks the first and the PR must defend it explicitly:

- **(chosen) Accept it, scoped by necessity.** The registry's contents are build-dependent
  (§7): `xinfo`, `xgroup`, `json.debug` and `ft.config` are absent on a `cmd-core` build, so
  "error on registry miss" would make an `aclfile` that loads against one binary fail to load
  against another — and `load` fails **the whole file** on one bad line (`manager.rs:300-302`),
  so that is a startup failure, not a warning. Typo-inertness is the smaller harm. Mitigation
  that is *not* optional: `ACL SETUSER` must emit a `WARN`-level log naming the command when it
  stores a first-arg rule for a name the registry does not know, so the typo is discoverable
  from operations rather than only from a failed penetration test.
- **(rejected) Declare feature-gated vocabulary unconditionally in core**, so the registry knows
  every container name on every build and "error on registry miss" becomes safe. This is the
  cleaner end state and it removes the deviation in §7 entirely — but it means a `#[cfg]`-free
  name table in `frogdb-core` listing commands that do not compile, which is a fifth
  hand-written table wearing a different hat. Revisit if the feature matrix ever collapses.

**4 — enforcement stops consulting a list, and `extract_subcommand` splits in two.**
`CONTAINER_COMMANDS` (`util.rs:250-254`) is deleted. Revision 1 proposed a single replacement
that took `args[0]` when the registry declares subcommands **or** when the caller's own
permission set carries a rule for this command. That second clause is necessary for ACL — it is
how first-arg rules on non-containers stay enforceable, and how `+select|0`'s `NOPERM`
(`acl_tcl.rs:1840`) survives — but it must **never** reach the admin-port gate. `guards.rs:355`
feeds `AdminSurface::requires_admin`, and a function whose output depends on the connected
user's ACL rules would make the admin-port decision take input from the very principal it is
gating. That is a fail-open shape regardless of whether a concrete exploit exists at HEAD, and
it is cheap to avoid. So:

| new function | inputs | callers |
|---|---|---|
| `container_subcommand(registry, cmd, args)` | **registry only** — `Some(args[0].to_uppercase())` iff the registry entry declares subcommands | the admin gate, `guards.rs:355` |
| `acl_subcommand(registry, perms, cmd, args)` | registry **or** the user's own subcommand/first-arg rules | the ACL seam `guards.rs:367`; `ACL DRYRUN` `acl_conn_command.rs:249` |

Both callers in `guards.rs` and the one in `acl_conn_command.rs` already have the registry in
hand (`guards.rs:353` and `acl_conn_command.rs:277` both call `registry.get_entry`), and the two
ACL sites already have the user (`ACL DRYRUN` calls `user.check_command` directly at
`acl_conn_command.rs:252`). The split costs one extra function and buys the property that
**the admin-port gate is a pure function of the command table**.

**5 — `SPLIT_ADMIN_SURFACES` stays, and gains a coherence assertion.** It encodes *policy*
(which subcommands a plain client port may reach), not identity, and per
`feedback_spec_enums_pure_identity` that separation is correct: identity in core's command
description, policy flags beside the dispatch. What it gains is one line inside the existing
FM-CLUSTER-064 test at `register.rs:558`: every `public` name must be a declared subcommand of
that command. Had that assertion existed, `malloc-stats` would have been impossible — the admin
table has said `MALLOC-SIZE` since it was written.

**And copy #4 goes away.** The hand-retyped `cases` literal inside
`admin_surface_split_table_classification` (`command_spec.rs:1692-1727`) is replaced by a walk:
for each `SPLIT_ADMIN_SURFACES` entry, assert `public ⊆ declared_subcommands(cmd)` and
`declared_subcommands(cmd) \ public` is exactly the admin half. The FM-CLUSTER-064 tag and the
test name **stay** (`just lint-failure-modes` checks spec↔test agreement in both directions), and
the fail-closed defaults FM-CLUSTER-062/063 pin (`command_spec.rs:1654`, `:1667`) are untouched —
what changes is that the expected-value side stops being typed by hand. This is a strict
improvement in forcing power: the current test passes even when the ACL vocabulary is wrong,
because it only ever compares the table to itself.

## Testability improvement

Today there is **no test anywhere** that relates the ACL vocabulary to the dispatchers. The
`parser.rs` unit tests (`:626-901`) exercise `+config|get` / `-config|set` / `+config|` only —
three strings, all from the one container that happens to be correct. Every drift in §2 is
invisible to the whole suite. Four artifacts change that:

- **T1 · `lint-acl-vocabulary` (compile-free grep gate, joins `lint-gates`).** For each site
  declaring `fn subcommands()`, extract the string-literal arms of the corresponding
  `match subcommand…` block and assert set equality with the declared list. This is the gate
  that makes the class of bug extinct: a new subcommand cannot land dispatch-only. Modelled on
  `lint-format-float` ("exactly one definition, everything else re-exports") and sized like
  `lint-continuation-lock`'s arm parsing, whose scanner already has unit tests
  (`scripts/tests/test_continuation_lock_gate.py`, `Justfile:122`).
  **Three sites are not single-file** (SCRIPT, FUNCTION, FT.CONFIG — see the locality table in
  §Why this shape), and FT.CONFIG's declaration and dispatch are in *different crates*. The gate
  must therefore ship with an explicit `declaration → dispatch` path map for those three, and
  must **fail** — not skip — on a `fn subcommands()` site it cannot pair with a `match`. A
  scanner that silently skips the hard cases gates nothing where it matters most.
- **T2 · `split_admin_public_names_are_real_subcommands`** — extends the FM-CLUSTER-064 test at
  `register.rs:558-577`, walking `full_registry()`, and **replaces the hand-typed `cases`
  literal at `command_spec.rs:1692-1727`** (part 5). Directly forces the drift class — a public
  name in the split-admin table that no registry entry declares: today's test compares the table
  to a copy of itself and passes.
- **T3 · `every_declared_subcommand_is_grantable`** — for every container in `full_registry()`
  and every declared subcommand, `AclRule::parse_with(&registry, "+cmd|sub")` succeeds and
  round-trips through `ACL GETUSER`. Forces §2's *missing*-entry half (CLIENT, LATENCY, SCRIPT,
  COMMAND, HOTKEYS, STATUS, FT.CONFIG, JSON.DEBUG) in one loop that grows by itself.
- **T4 · the accepted-denied regression** (`integration_acl.rs`) — the pair H4 lands with, led by
  `-@all +client|stats` ⇒ `CLIENT STATS` allowed / `CLIENT KILL` denied (the operator harm), with
  `+@all -memory|malloc-size` ⇒ `NOPERM` + an `ACL LOG` entry as the mechanism pin for §3
  Trace B. The `MEMORY` half pins the fail-open *shape*, not a disclosure — the subcommand is a
  stub (`observability_conn_command.rs:375-388`); the comment must say so.

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
- **67 (server small dedups) — shared file, disjoint regions.** Revision 1 claimed the
  intersection was **empty**; that is **wrong**, and the review is right to reject it. 67's SV6
  names `frogdb-server/crates/core/src/command.rs` as the new home for its shared refusal
  function *and* a `#[macro_export] macro_rules!` (67's Files table, "New home for the refusal
  function + the `#[macro_export]` macro"; discussion at 67 §SV6). This proposal also writes
  `core/src/command.rs` — one defaulted method on the `Command` trait. **67 also writes
  `server/src/commands/search.rs`**, which revision 2 adds to this proposal's file set for
  FT.CONFIG's `subcommands()` declaration (`:757`). So the correct characterisation is the same
  one used for the 68 edge: **two shared files, disjoint regions.**
  - `core/src/command.rs`: 67 adds a free function + a macro at file scope; 70 adds a method
    inside the `Command` trait body. No overlap.
  - `server/src/commands/search.rs`: 67 touches the FT.* execute paths; 70 adds a
    `fn subcommands()` override inside `impl Command for FtConfigCommand` (`:757+`). No overlap,
    but the same `impl` block region is adjacent — whichever lands second rebases, it does not
    re-derive. State the boundary in both PRs. Land in any order.
- **Round-2 issue 35 (ACL category enforcement inert,
  `.scratch/testing-improvements-round2/issues/open/35-*.md`) — sibling, not overlap.** Same
  root cause (a `frogdb-acl` string table joined to the registry by nothing), different table:
  35 is `COMMAND_ALL_CATEGORIES` (`acl/src/categories/data.rs`, 1827 lines, 185 commands with no
  row); this is the subcommand vocabulary. The ratcheting gate for 35 already lives at
  `register.rs:580+` and its doc comment names the same fix direction ("populating the rows /
  moving categories onto `CommandSpec`"). The two share **no file** except `register.rs`'s test
  module. Doing 70 first makes 35 cheaper — the port and the registry-walking test scaffolding
  are reusable verbatim.

### MERGE PRECONDITIONS — two security issues that must be FILED BEFORE 70 MERGES

Neither defect is caused by this proposal and neither is fixed by it. Both were surfaced by this
census, both are independently exploitable at HEAD
(`4372082285b34079ae6c1eb0c2d135a55d91ca83`), and both are recorded here **with complete
evidence chains so the issue author needs no re-derivation**. This proposal must not merge until
both issues exist in `.scratch/`, because merging a proposal that rewrites the ACL vocabulary
while these sit unfiled buries them under a refactor.

The orchestrator files them; this section is the source material.

---

#### Precondition 1 — `guards.rs:364`: the ACL command is UNCONDITIONALLY exempt from ACL
#### enforcement. Severity **CRITICAL** (privilege escalation to full admin from zero grants).

**The code.** `frogdb-server/crates/server/src/connection/guards.rs:362-371`:

```rust
// Check command ACL permission through the unified enforcement seam.
// Note: ACL command is exempt (users need ACL WHOAMI to check their identity).
if cmd_name != "ACL"
    && let Some(guard) = self.permission_guard()
{
    let subcommand = extract_subcommand(cmd_name, args);
    if let Err(err) = guard.check_command(cmd_name, subcommand.as_deref()) {
        return Some(err);
    }
}
```

The comment justifies the exemption by `ACL WHOAMI`. The **condition does not mention any
subcommand** — it is `cmd_name != "ACL"`, so *every* ACL subcommand, including `SETUSER`,
`DELUSER`, `GETUSER`, `LOAD` and `SAVE`, skips the seam entirely.

**Evidence chain — every gate that could stop it, and why none does:**

| gate | file:line | why it does not stop this |
|---|---|---|
| Command-ACL seam | `guards.rs:364` | the exemption itself — short-circuits before `check_command` at `:368` |
| Whole-command `ADMIN` flag | `acl_conn_command.rs:38` | `flags: CommandFlags::empty()` — ACL carries **no** `ADMIN` flag; the comment at `:36-37` says so explicitly and defers to `SPLIT_ADMIN_SURFACES` |
| Admin-port split gate | `guards.rs:351-360` | guarded by `self.admin_enabled`. `AdminConfig::default()` sets `enabled: false` (`frogdb-server/crates/config/src/admin.rs:46`); the connection-level mirror also defaults false (`connection/deps.rs:224`). **Off unless explicitly configured.** When it *is* on it works — `SPLIT_ADMIN_SURFACES` lists ACL's public set as `WHOAMI/CAT/GENPASS/HELP` (`command_spec.rs:605`) and fails closed for everything else (`:546-547`) |
| A check inside the handler | `acl_conn_command.rs:172-187` | `acl_setuser` validates arity and forwards straight to `ctx.acl_manager.set_user` (`:183`). No permission check. `acl_deluser` (`:190+`) likewise |
| Key/channel guards | `guards.rs:586-611` (MULTI queue-time), `routing.rs:66-74` (dispatch) | re-verified: both call `check_keys_with_flags` / `check_channels` only — **neither calls `check_command`** (`routing.rs:61-62` says so in a comment: "The command itself is already validated by run_pre_checks"). ACL is `KeySpec::None` (`acl_conn_command.rs:39`) and is not a pub/sub command, so neither fires. **`ACL SETUSER` inside `MULTI` is exempt too** |

**Exploit, minimal.** A user with **zero** grants:

```
ACL SETUSER mallory on >pw ~* -@all          # by an admin: no commands at all
AUTH mallory pw
ACL SETUSER mallory +@all ~* &*              # allowed — cmd_name == "ACL", seam skipped
ACL DELUSER <everyone else>                  # also allowed
```

`-acl`, `-acl|setuser`, `-acl|getuser` and `-@admin` are all inert against the ACL command, so
there is no ACL rule an operator can write that closes this.

**Redis parity.** Redis has no such exemption. Its no-auth exemption list (`CMD_NO_AUTH`) is
exactly `AUTH`, `HELLO` and `RESET`; `ACL` is a normal command subject to `ACLCheckAllPerm`, and
Redis expects operators to grant `+acl|whoami` explicitly when a client needs it.

**Suggested fix (for the issue, not for this proposal).** Drop the `cmd_name != "ACL"` clause and
add `+acl|whoami` to the default user's implicit grants, so the stated motivation for the
exemption is served without the hole. Regression test: a `-@all` user must get `NOPERM` on
`ACL SETUSER` and `+OK` on `ACL WHOAMI`.

**Relation to this proposal.** None causally — 70 neither creates nor worsens it, and fixing the
vocabulary tables does not touch it. It is worth recording *here* because it explains why
`parser.rs:22-35` (the ACL arm, which is **correct**) has never been observed to matter: no ACL
subcommand rule has ever been consulted at runtime.

---

#### Precondition 2 — `manager.rs:167-191`: `ACL SETUSER` is NOT atomic. Severity **HIGH**
#### (partial privilege grant that returns `ERR`).

**The code.** `frogdb-server/crates/acl/src/manager.rs:167-191`:

```rust
pub fn set_user(&self, username: &str, rules: &[&str]) -> Result<(), AclError> {
    // ... username validation ...
    let mut users = self.users.try_write_err()?;
    let user = users                                  // :178
        .entry(username.to_string())
        .or_insert_with(|| User::new(username));      // :180 — user EXISTS from here on
    for rule_str in rules {                           // :182
        let rule = AclRule::parse(rule_str)?;         // :183 — early return, no rollback
        rule.apply(user);                             // :184 — mutates IN PLACE
    }
    // ...
}
```

The user is inserted into the shared map at `:178-180`, **before any rule is parsed**, and every
successfully-parsed rule is applied in place at `:184`. The `?` at `:183` returns on the first
bad rule with everything before it already committed.

**Evidence chain:**

| step | file:line | effect |
|---|---|---|
| Caller | `acl_conn_command.rs:183` | `ctx.acl_manager.set_user(&username, &rules)`; `Err` → `Response::error` (`:185`). The client sees `ERR` and nothing else |
| Insert-before-parse | `manager.rs:178-180` | `entry(...).or_insert_with(User::new)` — the user now exists in `self.users` |
| Apply-in-place | `manager.rs:182-185` | each parsed rule mutates the live `User`; `on`, `>password`, `~*`, `+@all` all take effect via `AclRule::apply` (`parser.rs:469-572`) |
| No rollback | `manager.rs:186-191` | the only code after the loop updates the rate-limit registry and returns `Ok`. There is no undo path, no snapshot, no drop-guard |
| **Contrast: the load path IS atomic** | `manager.rs:280-321` | `load` builds a fresh `new_users` map (`:288`), parses every line into it (`:290-309`), and only then swaps under the write lock (`:316-318`, comment "Replace users atomically"). One bad line aborts the whole file (`:300-302`) and leaves the live map untouched. So the codebase already knows the right pattern — `set_user` just does not use it |

**Exploit / operator trap.**

```
ACL SETUSER newuser on ">hunter2" ~* +@all +nosuchcmd|nope
-> ERR ... Unknown command or subcommand 'nosuchcmd|nope'
```

The command reports failure. `newuser` nonetheless now exists, is **enabled** (`on`), has a
**known password**, has `~*` (all keys) and `+@all` (all commands). An operator who reads `ERR`
and retries with a corrected line has, in the interval, published a full-privilege account. A
scripted provisioning flow that treats `ERR` as "nothing happened" is wrong on every rule list
that fails after the first element.

**Redis parity.** Redis's `ACLSetUser` operates on a **temporary copy** of the user
(`ACLCopyUser` / `ACLSetUser` on a scratch `user` struct) and commits it only after every rule
parses; a failure leaves the live user untouched. FrogDB's `load` path matches Redis; its
`set_user` path does not.

**Suggested fix (for the issue).** Mirror `load`: clone-or-create into a local `User`, apply all
rules, and insert under the write lock only on success. ~10 lines. Regression test: a `SETUSER`
whose last rule is invalid must leave `ACL GETUSER` reporting the *prior* state (or "user not
found" if it did not exist).

**Relation to this proposal.** It **falsifies a claim revision 1 made** in §1 — "which
`AclManager::set_user` propagates, so the whole `ACL SETUSER` fails" — now retracted there. It
also raises the stakes on B2/H4: any change that makes a previously-valid vocabulary entry
invalid can, via this defect, half-apply a rule list that used to apply fully.

### Other risks

- **The `-cmd|arg` tightening is a behavior change, and it is TWO losses, not one.** Rule 3
  turns `-select|0` and `-debug|<arg>` into parse errors. Enumerated:
  - `-select|0` — accepted today (blanket `true`, `parser.rs:93`) and stored. Losing it costs
    nothing observable: SELECT's first-arg denies have no enforcement asymmetry worth keeping.
  - **`-debug|<arg>` — accepted today (blanket `true`, `parser.rs:115`) *and enforced*.**
    `DEBUG` is in `CONTAINER_COMMANDS` (`util.rs:251`), so `extract_subcommand` returns
    `Some("SEGFAULT")` and the deny rule matches by exact string at `permissions.rs:222-223`.
    `-debug|segfault`, `-debug|sleep`, `-debug|quicklist-packed-threshold` are **working, narrow
    denials today** and become unwritable. Redis agrees with the tightening (DEBUG has no
    subcommands dict, so `-debug|x` is an error there), but "Redis-correct" is not the same as
    "costless": the honest statement is that this proposal removes an enforceable deny on the
    single most dangerous command in the server in exchange for parity.
  - No test in the tree writes either form — grepped repo-wide for `-select|` and `-debug|`
    across `*.rs`, `*.tcl` and `*.md`: **zero hits**. So nothing goes red; the loss is silent,
    which is precisely why it must be in the PR description and the CHANGELOG.
  - If the DEBUG loss is judged unacceptable, the escape hatch is available and cheap: declare
    DEBUG's `match` arms (`debug_conn_command.rs:116`) as real subcommands. That **breaks**
    `acl_tcl.rs:1872-1889` (`+debug|a/b/c` would become `ENOENT`) and diverges from Redis, so
    this proposal does not take it — but the trade is explicit rather than hidden.
- **The `+get|foo` loosening is also a behavior change**, in the permissive direction: a
  first-arg rule on an arbitrary non-container command becomes writable, matching Redis. It
  grants nothing that `+get` does not already grant — a first-arg rule only ever *narrows*.
  The **unknown-command** half of the same rule is a genuine weakening and is owned explicitly
  in §Proposed change part 3 ("Own the loosening in row 3"), with the `WARN`-log mitigation
  named there as non-optional.
- **Any *subtractive* vocabulary change is an upgrade-time startup hazard.** `AclManager::load`
  (`manager.rs:280-321`) aborts the **entire aclfile** on one unparseable line (`:300-302`), and
  `parse_acl_line` (`parser.rs:585-613`) parses every rule through `AclRule::parse` (`:609`).
  So removing a name from the vocabulary does not degrade one rule — it takes the whole ACL
  configuration down at the next restart or `ACL LOAD`. This is the reason H4 is purely
  additive (below) and the reason rule 3's registry-miss fallback is soft (§7).
- **Declaring HOTKEYS / STATUS / FT.CONFIG / JSON.DEBUG makes previously-accepted garbage
  rejected.** `+hotkeys|nonsense` errors today (`_ => false`) and errors after; but
  `+ft.config|anything` also errors today, so no rule that works today stops working. Net: only
  new grants become possible.
- **The grep gate (T1) is heuristic, and three sites are structurally hard.** The dispatchers
  are uniform (`match subcommand_str.as_ref()` for connection commands, `match
  subcommand.as_slice()` with `b"…"` literals for shard commands) but not identical, and
  `COMMAND`'s arms are spread over 250 lines (`basic.rs:145-400`). On top of that, SCRIPT,
  FUNCTION and FT.CONFIG declare in one file and dispatch in another (FT.CONFIG across a crate
  boundary) — see the locality table in §Why this shape. If a container resists the scanner, the
  honest fallback is the `lint-continuation-lock` idiom: a per-command count pin plus a **named**
  exemption, never a silent skip; and the gate must fail on an unpairable `fn subcommands()`.
- **`frogctl` is unaffected.** `frogctl/src/commands/acl.rs` has no subcommand table (grepped);
  it forwards rule strings to the server, so it inherits whatever the server accepts.
- **Documentation — revision 1's grep result was wrong.** Re-grepped at HEAD: `malloc-stats`
  **does** appear under `website/`, at `website/src/data/compat-exclusions.json:1611`
  (`"Coverage: MEMORY MALLOC-STATS"`, reason "jemalloc-only subcommand"). That file is
  compatibility *exclusion* data, not a restatement of the ACL vocabulary, so it is not
  invalidated by anything here — but it is the published statement that FrogDB knowingly does
  not implement `MEMORY MALLOC-STATS`, and any H4 wording that calls `malloc-stats` a "typo"
  contradicts it. No doc file restates the subcommand vocabulary, so doc-sync is limited to: a
  CHANGELOG entry for the two deviation-correcting breaks (`-select|0`, `-debug|<arg>`), and a
  CHANGELOG note for the `+cofnig|set` loosening.

## Effort

**M.** Roughly: the port + `AclManager` wiring (`server/mod.rs:82`, `:134`) + parse rewrite
(~120 lines net negative in `frogdb-acl`), `subcommands()` on two traits + `CommandImpl` + the
`CommandVocabulary` impl (~40 lines in `frogdb-core`), ~20 one-line-list declarations across the
container files (three of which are **not** in the file the container dispatches from — see the
locality table in §Why this shape), `CONTAINER_COMMANDS` deletion plus the `extract_subcommand`
split into `container_subcommand`/`acl_subcommand` and its three call sites (`guards.rs:355`,
`:367`, `acl_conn_command.rs:249`) in `frogdb-server`, replacing the hand-typed `cases` literal
at `command_spec.rs:1692-1727` with a registry walk, and the four test artifacts T1-T4. The
scanner (T1) is the only genuinely open-ended piece — it must resolve declarations across files
and, for `FT.CONFIG`, across crates — so it is separable and can land after the rest as a
follow-up if it fights.

Ordering: **H4 first** (below), then the structural change, then T1. H4's regression tests are
written to survive the structural change unchanged — they assert observable ACL behavior, never
table contents.

## Independently-landable hotfixes

### H4 — widen the two tables, with regression tests (independent of everything above)

**H4 is PURELY ADDITIVE.** Revision 1's edits 1 and 4 removed names (`malloc-stats` → replaced;
`"debug"` dropped from `script`) and were **not standalone-safe**. The mechanism is the one
enumerated in §Other risks: `AclManager::load` (`manager.rs:280-321`) aborts the **whole
aclfile** when any line fails to parse (`:300-302`, via `parse_acl_line` `parser.rs:585-613` →
`AclRule::parse` `:609`). So an operator who has `user bob ... -memory|malloc-stats` — the
**Redis-correct spelling**, and one the current FrogDB parser accepts — or `+script|debug` in
`aclfile` gets a **total ACL load failure on the next restart**, from a change advertised as a
one-line typo fix. Worse, revision 1's own edits 10 and 11 argue the opposite policy: keep
unimplemented-Redis phantoms because "removing them would reject rules that will become valid".
`malloc-stats` and `script|debug` *are that category*. Revision 2 makes every H4 edit additive
and the contradiction disappears.

| # | arm (line) | edit |
|---|---|---|
| 1 | `memory` `:101-104` | **add** `"malloc-size"`. **KEEP `"malloc-stats"`** — it is the real Redis subcommand, deliberately unimplemented (`website/src/data/compat-exclusions.json:1611`), so it is a phantom in exactly the sense edits 10/11 protect. Comment both |
| 2 | `client` `:36-54` | add `"unblock"`, `"stats"` |
| 3 | `latency` `:96-99` | add `"bands"`, `"doctor"`, `"histogram"` |
| 4 | `script` `:114` | **add** `"kill"`. **KEEP `"debug"`** — `SCRIPT DEBUG` is a real Redis subcommand FrogDB has not implemented (`script.rs:20-31` has no `DEBUG` arm), i.e. the same phantom category. Comment it |
| 5 | `command` `:85-88` | add `"getkeysandflags"` |
| 6 | new arm | `"hotkeys" => matches!(sub, "start" \| "stop" \| "reset" \| "get")` (`hotkeys.rs:86-89`) |
| 7 | new arm | `"status" => matches!(sub, "json" \| "help")` (`observability_conn_command.rs:843-844`) |
| 8 | new arm | `"ft.config" => matches!(sub, "get" \| "set")` (`core/src/shard/search/config.rs:21`, `:59`) |
| 9 | new arm | `"json.debug" => matches!(sub, "memory" \| "help")` — **two** arms (`json/basic.rs:426`, `:445`); revision 1 listed only `memory`. Behind no cfg; the table is compile-agnostic today and stays so |
| 10 | `cluster` `:56-83` | **leave the five phantoms** (`bumpepoch`, `count-failure-reports`, `flushslots`, `links`, `slaves`). They are Redis subcommands FrogDB has not implemented; removing them would reject rules that will become valid, and they grant nothing today. Add a one-line comment saying so, so the next reader does not "fix" it |
| 11 | `module` `:105` | same reasoning, with one correction: MODULE's `help` is **real** (`stub.rs:106-125`), so only `list`/`load`/`loadex`/`unload` are phantoms. Leave all five, comment |

**Net: H4 removes nothing.** Every rule string that parses at HEAD still parses after H4. No
aclfile can fail to load because of it, and rollback is a revert with no data-shape concern.

**`frogdb-server/crates/server/src/connection/util.rs:250-254`:** add `"FT.CONFIG"` and
`"JSON.DEBUG"` to `CONTAINER_COMMANDS` — without this, edits 8 and 9 parse and are never
enforced. Also additive.

**`frogdb-server/crates/core/src/command_spec.rs`:** no edit. `SPLIT_ADMIN_SURFACES` is correct.

**Regression tests (the point of the hotfix, and the part that must not be skipped).** Ordered
by forcing power:

1. **`integration_acl.rs` — the harm test, and the one to lead with.** `-@all +client|stats` ⇒
   `CLIENT STATS` **allowed** and `CLIENT KILL` **denied**; and `+@all -client|unblock` ⇒
   `CLIENT UNBLOCK` denied with `CLIENT ID` still allowed. *This fails today at the
   `ACL SETUSER` step* (`parser.rs:373` rejects `client|stats`), and it is the test that
   demonstrates the actual operator harm: without edit 2 the only expressible grant is
   `+client`, which carries `KILL`/`PAUSE`/`UNBLOCK`/`LIST`.
2. **`integration_acl.rs` — the FT.CONFIG escalation test.** `-@all +ft.config|get` ⇒
   `FT.CONFIG GET` allowed, `FT.CONFIG SET` denied. This is the headline case (§Summary): today
   the narrow grant is unwritable and the operator must grant `FT.CONFIG SET`. Needs `cmd-full`;
   if the regression harness's feature set makes this awkward, drop to a `parser.rs` unit test
   and note it — the `util.rs` half is still required either way, and without it the rule parses
   and is never consulted.
3. **`integration_acl.rs` — the accepted-denied shape.** `+@all -memory|malloc-size`, then
   `MEMORY MALLOC-SIZE` ⇒ `NOPERM`, and the denial appears in `ACL LOG`. This also fails today
   at the `ACL SETUSER` step. Note what it does **not** claim: `MEMORY MALLOC-SIZE` is a stub
   that echoes its argument (`observability_conn_command.rs:375-388`), so this test pins the
   *mechanism*, not a disclosure. Keep it, and keep the comment saying so — revision 1 sold this
   as the headline and it is not.
4. `acl/src/parser.rs` unit tests — a table-driven case per entry: every name in edits 1-9
   parses under both `+` and `-`, **and** `malloc-stats` and `script|debug` still parse, pinning
   the additive property so a later "cleanup" cannot silently break an aclfile.

**Not in H4:** the `guards.rs:364` ACL self-exemption and the non-atomic `set_user`
(both §Risks merge preconditions — separate security issues), the ACL category table (round-2
issue 35), and anything structural. H4 is ~45 lines of table edits plus ~170 lines of tests, and
every one of those tests stays green — untouched — through the structural change.
