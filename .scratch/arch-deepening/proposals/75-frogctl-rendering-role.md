# Proposal 75 — frogctl: one Output Mode seam, one Role type, and the flags that lie

Round 38 · lane: frogctl / operator / telemetry · effort **M** · candidates FR4 + FR5 · ordered
**after** proposals 72 (FR2) and 73 (FR1); independent of 74 (FR3)

Verified against the tree at `4372082285b34079ae6c1eb0c2d135a55d91ca83` and re-confirmed at
`50118a53922bfe1aa5a7c56f494921df180d8770` (worktree `arch-round-38-99`, branch `main`). The five
commits between those SHAs touch only `.scratch/arch-deepening/proposals/*.md` — **no source file
changed**, so every line number below holds at both. Every path, line number and count was
re-derived by reading at those SHAs; the lane brief's citations were **not** trusted. **Three brief claims are
corrected** (one downgraded to latent, two upgraded to far worse than stated) and **four defects
neither the brief nor proposal 73 named** were found — see §Problem.

## Summary

`frogctl` declares one **Output Mode** (`table | json | raw`, `frogctl/CONTEXT.md:19-20`) and
asserts one relationship over it: "**Output Mode** is orthogonal to all commands: every result
renders as table, JSON, or raw" (`CONTEXT.md:59`). The crate has a seam built for exactly that —
`trait Renderable` + `print_output` (`output.rs:3-16`) — and then **six independent places that
decide what an Output Mode means**, plus a long tail of commands that never ask.

That is not a missing seam. It is a **duplicated** one, which is worse: `print_output`
(`output.rs:9-16`) and `render_value` (`output.rs:19-29`) are two `match mode { … }` blocks
twenty lines apart in the same file, and the second one silently drops `no_color` and collapses
`Raw` into `Table`. Four more hand-rolled `matches!(…, OutputMode::Json)` branches live in
`client.rs`, `acl.rs`, `search.rs` and `health.rs`. And one whole command namespace — `upgrade`,
**86 `println!`s across 421 lines** — never reads the Output Mode at all.

The sharpest consequence is not cosmetic. `frogctl debug latency --dist --output json` calls
`print_output` (`debug.rs:495`) and *then* `print_histogram` (`:497-499`), which unconditionally
`println!`s ASCII bar charts to **stdout** (`:503-543`). The command emits a JSON document
followed by ASCII art on the same stream: **`--output json` produces output that is not JSON.**

FR5 is the same failure at the vocabulary layer. `CONTEXT-MAP.md:37-39` states a workspace rule —
"Node roles are **Primary**/**Replica** everywhere in code, docs, issues, and CLI output.
`master`/`slave` appear only at the Redis wire-compat boundary" — and `CONTEXT-MAP.md:29-30` names
`frogctl` as the component that *performs* that normalization. `frogctl/CONTEXT.md:41-43` repeats
it: "frogctl remaps the wire's `master`/`slave` INFO values to Primary/Replica **in all rendered
output**." The code maps `slave` → `replica` at **four** copy-pasted sites and **never maps
`master` at all** — `frogctl health` against a primary prints `Role: master`, and
`frogctl replication status --output json` ships `connected_slaves`, `master_repl_offset`,
`master_host`, `master_link_status`, `slave_repl_offset` as document keys. The normalization the
context map advertises is, at HEAD, half-written and stringly-typed.

Both are the same architectural shape: **a chokepoint that exists, is documented as the single
answer, and is bypassed.** This proposal makes `Renderable`/`print_output` the sole exit for
stdout, gives Role a type at the one place the wire is parsed, resolves the four declared-and-
unread flags, retires four unused dependencies, and pins all of it with a `lint-gates`-family
seam lint so the bypasses cannot re-accumulate.

## Files involved

| path | lines | role in this proposal |
|---|---:|---|
| `frogctl/src/output.rs` | 173 | **the seam — primary.** `trait Renderable` `:3-7`, `print_output` `:9-16` (the seam), `render_value` `:19-29` (**the rival — deleted**, replaced by a `Renderable` newtype), `format_value` `:31-66` and `value_to_json` `:68-86` (**kept** — the crate's only RESP→text/JSON projection, 12 unit tests `:88-173`) |
| `frogctl/src/cli.rs` | 143 | **the change (CLI surface).** `--tls` `:40-42`, `--tls-cert` `:44-46`, `--tls-key` `:48-50`, `--tls-ca` `:52-54` (§Problem P4), `--output` `:64-66`, `--no-color` `:68-70`. Sibling 72 touches only `:56` (`--admin-url` help) — disjoint lines |
| `frogctl/src/connection.rs` | 193 | **the change (2 lines).** `build_url` `:22-29`; the `rediss` scheme selection at `:23` is the LIVE TLS break. Sibling 72 touches only `admin_url()` `:139-144` — disjoint lines |
| `frogctl/src/info_parser.rs` | 88 | **the change (FR5 home).** `InfoResponse::parse` `:10-33`, `get` `:35-40`, `get_parsed` `:42-44`. Gains `enum Role` + `Role::from_info`. Explicitly disclaimed by 74 (`74:519-521`) and by 72 (`72:80`) |
| `frogctl/src/commands/upgrade.rs` | 421 | **the change (largest).** 86 `println!`s, zero `print_output`, zero `ctx.global().output` reads. `run_status` `:134-201`, `run_check` `:203-266`, `run_plan` `:268-329`, `run_rollback` `:331-363`, `run_finalize` `:365-421`. Response types `UpgradeStatusResponse` `:68-76`, `VersionInfo` `:60-65` already derive `Serialize`. Hand-rolled prompt `:390-398`. Assigned to FR4 by 72 (`72:484-485`) |
| `frogctl/src/commands/replication.rs` | 250 | **the change (FR4+FR5).** `ReplicationStatus` `:47-57` (six wire-vocab JSON keys), `Renderable` `:59-105` (wire vocab in table output `:67`,`:73`,`:78`; defensive double-compare `:64`), `run_status` `:118-168` (map site `:127`), `run_promote` `:170-180` (raw `println!` `:178`), `run_topology` `:182-218` (map site `:196`, raw `println!`s `:197`,`:209-212`) |
| `frogctl/src/commands/health.rs` | 498 | **the change (FR5 + the `--json` override).** `HealthResult.role: Option<String>` `:38`, `Renderable` `:52-83` (`:65`), `FanOutResult` `:85-120` (`:102`), `--json` flag `:28-30` + override `:123-127`, `check_node_health` role logic `:200-219` (map site `:217`), `check_remote_health` `:232-282` (map site `:254`), `run_fanout` `:284-306` — **the Fan-out primitive H2 reuses** |
| `frogctl/src/commands/debug.rs` | 770 | **the change (D6 + stdout mixing).** `Latency.history` declared `:44-46`, dropped by `..` `:396-402`; `Hotshards.all` `:73-75`; `Slowlog.all` `:88-90`, dropped by `..` `:427-431`; `run_latency` `:445-501` (`print_output` `:495` then `print_histogram` `:497-499`), `print_histogram` `:503-543` (stdout ASCII), TTY guard `:450`, `:459`, `:474`; raw `println!`s `:512`, `:527`, `:537`, `:646`, `:768`. **Arm bodies are 73's and 74's** — see §Risks |
| `frogctl/src/commands/client.rs` | 204 | **the change.** `run_info` `:178-203` — a hand-rolled fourth path: `matches!(…Json)` `:183`, raw `println!`s `:187-201`, and **`OutputMode::Raw` falls through to the table branch**. `ClientInfo` already derives `Serialize`; `ClientListResult`/`Renderable` `:55-84` is the pattern. `render_value` sites `:156`, `:166`, `:175` |
| `frogctl/src/commands/acl.rs` | 406 | **the change.** 10 `render_value` sites `:178`,`:187`,`:196`,`:266`,`:279`,`:288`,`:300`,`:310`,`:327`,`:394`; hand-rolled `matches!(…Json)` `:385-392` |
| `frogctl/src/commands/search.rs` | 429 | **the change.** 6 `render_value` sites `:246`,`:337`,`:356`,`:389`,`:399`,`:418`; hand-rolled `matches!(…Table \| Raw)` `:347-361` — the only site that inverts the polarity, which is why it also duplicates `print_output`'s json arm at `:359-360` |
| `frogctl/src/commands/exec.rs` | 47 | **the change (1 line).** `render_value` `:34` — the purest passthrough, and the shape the replacement newtype is designed for |
| `frogctl/src/commands/stat.rs` | 242 | **the change (small).** No Output Mode read anywhere. Header `println!`s `:35-38`, data line `:114`/`:117`, TTY guard `:30`. The ANSI escape `\x1b[2K\r` `:114` is cursor control, **not color** |
| `frogctl/src/commands/data.rs` | 119 | **the change (small).** `run_slot` raw `println!`s `:47-53`. Sibling 73 owns `run` `:33-43` |
| `frogctl/src/ops/latency.rs` | 261 | **the change (relocation, ceded by 73 `73:498-501`).** `render_ascii_graph` `:135-185` and `render_histogram_table` `:188-203` are rendering inside an operation module. Move to `commands/debug.rs`; their 4 unit tests `:205-261` move with them. Engine fns `:27-108` untouched |
| `frogctl/src/commands/cluster.rs` | 524 | **read-only evidence + 1 flag.** `is_primary()` `:141-144` matches `f == "master"` on `CLUSTER NODES` flag tokens — **correct**, and the model for where the wire boundary belongs. `Slots { json: bool }` `:117-121` is the second per-command Output Mode override |
| `frogctl/src/commands/benchmark.rs` | 200 | **the change (1 line).** Raw `println!` `:164` on the empty-result path, before an `Ok(0)` that never reaches `print_output` `:198` |
| `frogctl/src/commands/backup.rs` | 190 | **the change (1 line).** Raw `println!` `:148` in `run_trigger`. Sibling 73 owns `run` `:130-144` |
| `frogctl/Cargo.toml` | 48 | **the change (deps).** `redis = { version = "1.1", features = ["tokio-comp"] }` `:34` — **no TLS feature** (§P4). `indicatif` `:37`, `comfy-table` `:38`, `dialoguer` `:39`, `zip` `:40` — **zero uses in the crate** (74 owns `zip`) |
| `frogctl/CONTEXT.md` | 78 | **the change (doc).** **Output Mode** `:19-20`, **Fan-out** `:26-28`, **Primary / Replica** `:41-43`, the orthogonality relationship `:59`. Its *Avoid* list is honored throughout |
| `CONTEXT-MAP.md` | 42 | **read-only evidence.** CLI→Server normalization claim `:27-30`; the shared vocabulary rule `:37-39` FR5 violates |
| `Cargo.lock` | — | **read-only evidence.** `[[package]] redis 1.1.0` `:4096-4118` — dependency list contains no `rustls`, `tokio-rustls` or `native-tls`. Proof that no feature unification rescues `--tls` |
| `frogdb-server/ops/docs-gen/src/main.rs` | — | **read-only evidence.** `generate_cli_reference(frogctl::cli::Cli::command(), …)` `:329`; `build_cli_command` `:577-593` filters **only** `"help"` — it never consults `is_hide_set()`. Decisive for the `hide = true` ruling (§Risks) |
| `website/src/content/docs/reference/frogctl.mdx` | — | **the change (hotfix H6).** `:56` publishes `frogctl --tls --tls-ca ca.crt health` — a command that cannot succeed. Same fenced block as 73's H1 at `:50` |
| `website/src/data/frogctl-cli.json` | 9,019 | **read-only evidence (generated).** Carries `tls-cert`/`tls-ca`/`tls-key` 178 times (global flags, repeated per subcommand). Regenerated by `just docs-gen`; never hand-edited |
| `Justfile` | — | **the change.** `lint-gates` `:329` gains `lint-render-seam`; `lint` `:319` inherits it by dependency |
| `scripts/` | — | **the change (new file).** `scripts/render-seam.py`, in the idiom of `clock-seam.py` / `continuation-lock-gate.py` / `error-sanitize.py` |

Nothing here is in a **locked** area — see §Spec / LOCKED.

## Problem

### P1 — The Output Mode seam is duplicated six ways

`frogctl/CONTEXT.md:59` promises orthogonality. Six places decide independently what an Output
Mode means:

| # | site | what it decides | what it gets wrong |
|---|---|---|---|
| 1 | `output.rs:10-14` `print_output` | Table / Json / Raw → `Renderable` | **the seam.** Correct by construction |
| 2 | `output.rs:20-28` `render_value` | Json → `value_to_json`; **`Table \| Raw` → one branch** | drops `no_color` (`_no_color` `:19`); `--output raw` ≡ `--output table` at all 20 call sites |
| 3 | `client.rs:183-202` `run_info` | `matches!(…Json)` → hand-serialize; else raw `println!`s | **`Raw` falls into the table branch**; re-implements `render_json` inline over a type that already derives `Serialize` |
| 4 | `acl.rs:385-392` `run_cat` | `matches!(…Json)` → `value_to_json` + pretty-print; else line-per-item | duplicates `print_output`'s json arm verbatim; `Raw` ≡ `Table` |
| 5 | `search.rs:347-361` `run_info` | `matches!(…Table \| Raw)` → `print_output`; else json | inverted polarity, so the json arm is a **third** copy of `serde_json::to_string_pretty` |
| 6 | `health.rs:123-127` | `args.json` (`--json`, `:28-30`) overrides the global `--output` | a per-command Output Mode surface the glossary does not define. `cluster slots --json` (`cluster.rs:117-121`) is a second one |

`render_value` (#2) is the load-bearing duplicate: **20 call sites** — `acl.rs` ×10, `search.rs`
×6, `client.rs` ×3, `exec.rs` ×1. Every one of them passes `ctx.global().no_color` into a
parameter named `_no_color`.

### P2 — And a long tail that never asks at all

Grepped at HEAD, `println!`/`print!` per module in `frogctl/src/`:

| module | count | Output Mode read? |
|---|---:|---|
| `commands/upgrade.rs` | 86 | **never** — all five wired arms |
| `commands/client.rs` | 10 | partially (`:183`) |
| `commands/stat.rs` | 5 | **never** |
| `commands/debug.rs` | 5 | partially — `print_histogram` is unconditional |
| `commands/data.rs` | 4 | **never** (`run_slot`) |
| `commands/replication.rs` | 3 | **never** (`run_topology`, `run_promote`) |
| `output.rs` | 2 | (inside `render_value`) |
| `acl.rs` / `watch.rs` / `subscribe.rs` / `search.rs` / `benchmark.rs` / `backup.rs` | 2/1/1/1/1/1 | mixed |

`upgrade` is the extreme case and the reason FR4 is **LIVE**: `frogctl upgrade status -o json`,
`upgrade check -o json`, `upgrade plan -o json`, `upgrade rollback -o json` and
`upgrade finalize -o json` all print a human-formatted table with box-drawing checkmarks
(`upgrade.rs:218`, `:236`, `:355`). Every response type they render already derives `Serialize`
(`:60-65`, `:68-92`) — the JSON is one `to_value` call away and was simply never wired.

**And zero `eprintln!` exists anywhere in `frogctl/src/`** (grepped; the single `eprint!` is the
confirmation prompt at `upgrade.rs:391`). Proposal 73 asks 75 to "preserve the stderr-progress /
clean-stdout invariant" — worth stating plainly: **that invariant does not exist at HEAD.** It is
something 75 establishes and 73's progress closures become the first users of.

### P3 — `--output json` emits output that is not JSON (LIVE, sharpest)

`commands/debug.rs:493-500`:

```rust
    print_output(&result, ctx.global().output, ctx.global().no_color);

    if dist {
        print_histogram(&latencies);
    }
```

`print_histogram` (`:503-543`) takes no mode and writes `println!("\nLatency Distribution:")`
plus twenty `#`-bar rows to **stdout**. So:

```
$ frogctl debug latency --dist --output json | jq .
parse error: Invalid numeric literal
```

The JSON document is emitted, then ASCII art is appended to the same stream. This is the concrete
harm behind FR4: the machine-readable mode is not machine-readable. `debug.rs:512` (`All samples:
{min}us`) is the same bug on the degenerate path.

Note the crate already knows the right idiom and applies it inconsistently: `debug.rs:450` and
`stat.rs:30` both gate cursor-control writes on `stdout().is_terminal()`. Both ANSI sites
(`stat.rs:114`, `debug.rs:474`) are correctly TTY-guarded. The histogram is not guarded by
anything.

### P4 — TLS: the brief undercalled this badly (LIVE, severe)

The lane brief says "`tls_cert`/`key`/`ca` parsed never read". True — `connection.rs` reads only
`self.global.tls` (`:23`) and nothing else. But the far larger fact is that **`--tls` itself
cannot work**:

```
frogctl/Cargo.toml:34   redis = { version = "1.1", features = ["tokio-comp"] }
```

No `tls-rustls`, no `tls-native-tls`. `build_url` (`connection.rs:22-29`) selects the `rediss`
scheme at `:23`, and redis-1.1.0's `url_to_tcp_connection_info` (`connection.rs:549-575` in the
vendored crate) reaches:

```rust
        #[cfg(not(any(feature = "tls-native-tls", feature = "tls-rustls")))]
        fail!((
            ErrorKind::InvalidClientConfig,
            "can't connect with TLS, the feature is not enabled"
        ));
```

`redis::Client::open` therefore returns `Err` before any socket is opened, and
`connection.rs:35-36` reports it as `invalid connection URL: rediss://127.0.0.1:6379`. **Every
frogctl command that touches the Data Plane fails under `--tls`, with an error message that
blames the URL.** `Cargo.lock:4096-4118` confirms no feature unification rescues it: `redis`'s
resolved dependency list contains no `rustls`, `tokio-rustls` or `native-tls`, and `redis` is
declared exactly once in the workspace (`frogctl/Cargo.toml:34`).

And it is published. `website/src/content/docs/reference/frogctl.mdx:56` reads, in the Examples
block:

```bash
# Connect with TLS
frogctl --tls --tls-ca ca.crt health
```

Two independent failures in one documented line: the connection cannot be built, and `--tls-ca`
would be ignored if it could.

### P5 — `no_color` is dead — but honestly, it is harmless (**latent**, downgraded)

The brief calls `no_color` LIVE. Re-derived, it is not, and saying otherwise would be sloppy:

- `no_color` is threaded correctly through **26** `print_output` call sites.
- `trait Renderable` declares `fn render_table(&self, no_color: bool)` (`output.rs:4`).
- **All 18 impls** in the crate write the parameter as `_no_color` — every single one discards it.
- `render_value` discards it too (`output.rs:19`).
- **The crate emits no color.** No `colored`, `owo-colors` or `termcolor` dependency exists; the
  only ANSI sequences anywhere are `\x1b[2K` line-clears at `stat.rs:114` and `debug.rs:474`,
  which are cursor control, not SGR color, and are already TTY-gated.

So `--no-color` changes nothing because there is nothing to change. **Ruling: latent.** It is real
dead weight — a published global flag, 26 threaded arguments, 18 discarded parameters — but it
misleads no operator's output today. It is fixed here because the seam work touches every one of
those signatures anyway, not because anything is broken.

### P6 — `--output raw` is a documented mode that does nothing (LIVE, low severity)

`frogctl/CONTEXT.md:19-20` defines Output Mode as `table | json | raw`. At HEAD:

- **16 of 18** `Renderable::render_raw` impls are the single line `self.render_table(true)`. The
  only two with a real raw projection are `config.rs:88-95` and `scan.rs:123-130`.
- `render_value` collapses `Table | Raw` into one arm (`output.rs:25-27`), covering all 20 of its
  sites.
- `client.rs:183` and `acl.rs:385` test only for `Json`, so `Raw` lands in the table branch.

`frogctl scan -o raw` differs from `-o table`. Nothing else in the crate does. A third of the
documented Output Mode surface is decorative.

### P7 — FR5: the role normalization the context map advertises is half-written

The contract, stated twice:

```
CONTEXT-MAP.md:37-39   Node roles are **Primary**/**Replica** everywhere in code, docs, issues,
                       and CLI output. `master`/`slave` appear only at the Redis wire-compat
                       boundary (INFO fields, `NodeRole` Display) and must not leak into new prose.
CONTEXT-MAP.md:29-30   [frogctl] normalizes the server's wire-compat vocabulary (`master`/`slave`
                       in INFO fields) to the canonical **Primary**/**Replica** in all output.
frogctl/CONTEXT.md:41-43  frogctl remaps the wire's `master`/`slave` INFO values to
                       Primary/Replica in all rendered output.
```

The implementation, at HEAD. **Four** copy-pasted map sites (the brief says three — it missed
`replication.rs:196`), each an inline closure over a `&str`:

```
health.rs:215-219        .map(|r| if r == "slave" { "replica" } else { r }).unwrap_or(&role)
health.rs:252-256        .map(|r| if r == "slave" { "replica" } else { r }).unwrap_or("master")
replication.rs:125-129   .map(|r| if r == "slave" { "replica" } else { r }).unwrap_or("unknown")
replication.rs:196       let display_role = if role == "slave" { "replica" } else { role };
```

Four defects follow directly:

**`master` is never mapped.** No site rewrites it. `frogctl health` against a Primary prints
`Role: master` (`health.rs:65`, `:69`); `frogctl health --all` prints `master` in the ROLE column
(`health.rs:102`, header `:94`); `frogctl replication topology` prints `master 127.0.0.1:6379`
(`replication.rs:197`). Two of the three fallbacks *are* the banned word — `health.rs:202` and
`health.rs:255` both `unwrap_or("master")`, so an INFO response missing the field produces
`master` out of thin air.

**Wire vocabulary is the JSON contract.** `ReplicationStatus` (`replication.rs:47-57`) derives
`Serialize` and `render_json` is `to_value(self)` (`:98-100`), so `--output json` field names are
literally `connected_slaves`, `master_repl_offset`, `master_host`, `master_port`,
`master_link_status`, `slave_repl_offset`.

**Wire vocabulary is in the *table* output too** — the thing `CONTEXT.md:41-43` says is remapped.
`render_table` writes `Master: {host}:{port}` (`:67`), `Slave Repl Offset:` (`:73`) and
`Master Repl Offset:` (`:78`) — and then `Connected Replicas:` (`:81`). One function, both
vocabularies, three lines apart.

**The type is untrusted at the point of use.** `replication.rs:64`:

```rust
        if self.role == "replica" || self.role == "slave" {
```

`run_status` already normalized `slave` away four lines of call-graph earlier (`:127`). The
defensive second comparison exists because a `String` field carries no evidence that anyone
normalized it. That is the stringly-typed cost, stated by the code itself.

By contrast `cluster.rs:141-144` — `self.flags.iter().any(|f| f == "master")` inside
`is_primary()` over `CLUSTER NODES` flag tokens — is **correct**: it is *at* the wire boundary,
and it projects into a Primary-named predicate. It is the model for where the boundary belongs,
and it stays as-is.

### P8 — D6: the declared-and-unread flag family (ceded by 73; both LIVE)

`frogctl/CONTEXT.md:26-28` defines **Fan-out** as "the `--all <addrs>` pattern where a command
queries a list of nodes and merges results (health, hotshards, slowlog)". Of the three commands
the glossary names, exactly **one** honors it:

| command | `--all` declared | dispatch | behavior |
|---|---|---|---|
| `health` | `health.rs:25-26` | `:131-133` → `run_fanout` `:284-306` | **works** |
| `debug hotshards` | `debug.rs:73-75` | `:424-426` | whole arm `bail!`s — no implementation exists |
| `debug slowlog` | `debug.rs:88-90` | `:427-431` destructures `{ count, analyze, reset, .. }` | **runs and lies** — queries the single default node, no warning |

`debug slowlog --all a:6379 b:6379` is the harmful one: it succeeds, prints one node's slow log,
and the operator believes it is the cluster's.

The second member of the family is `debug latency --history` (`debug.rs:44-46`, help "Periodic
snapshot mode (every 15s)"), destructured as `{ subcommand, samples, interval, dist, .. }` at
`:396-402` — `history` lands in the `..` and `run_latency` (`:445-451`) never sees it.
`frogctl debug latency --history` runs an ordinary one-shot sampler.

Both flags are published to `website/src/data/frogctl-cli.json` by `docs-gen`
(`main.rs:329`, `build_cli_command:577-593`).

### P9 — Dependency audit (ceded by 73)

Grepped across all of `frogctl/src/`: **`indicatif`, `comfy-table`, `dialoguer` and `zip` have
zero uses.** Not one `use`, not one path reference.

- `comfy-table` (`Cargo.toml:38`) — every table in the crate is hand-formatted with
  `format!("{:<22} {:<12} …")` (`health.rs:92-108`, `replication.rs:84-93`, `upgrade.rs:161-166`,
  `:295-316`). Two plausible futures: adopt it at the `Renderable::render_table` layer, or drop
  it. **Ruling: drop.** The hand-rolled widths are stable, `comfy-table` would change every
  rendered table's output (breaking the assertions 73's tests are about to add), and adopting a
  table engine is a separate, reviewable change — not a rider on a seam consolidation.
- `dialoguer` (`:39`) — the one interactive prompt in the crate is hand-rolled
  (`upgrade.rs:390-398`: `eprint!` + `std::io::stdin().read_line` + `trim() != "FINALIZE"`).
  **Ruling: drop.** The hand-rolled version is 8 lines, correct, and writes its prompt to stderr —
  which is exactly the invariant this proposal is establishing.
- `indicatif` (`:37`) — 73 explicitly left the progress-sink decision here, and deliberately
  writes plain stderr lines from its adapters (`73:509-515`). **Ruling: drop.** A progress bar is
  a TTY affordance with its own failure modes under redirection; plain stderr lines satisfy the
  invariant with zero dependency surface. If a bar is wanted later it is a self-contained change
  inside each adapter, exactly as 73 describes.
- `zip` (`:40`) — **74's**, not audited here.

Dropping three shrinks the shipped binary's dependency graph and removes three "someone started
this and stopped" signals from the manifest.

## Proposed change

### 1. One exit: `Renderable` + `print_output`, and nothing else writes stdout

The **interface** is already right — `print_output(&dyn Renderable, OutputMode, no_color)`
(`output.rs:9-16`). The change is to make it the *only* thing that writes to stdout, and to state
the rule where the code can be checked against it (`frogctl/CONTEXT.md`, alongside the
orthogonality claim at `:59`):

> Exactly one function writes to stdout: `print_output`. A command produces a value, implements
> `Renderable` over it, and hands it to the seam. Progress, prompts, warnings and diagnostics go
> to **stderr**. Streaming commands (`watch`, `subscribe`, `stat`) emit one `Renderable` per tick
> through the same seam.

The **locality** win is the point: today, answering "what does `--output json` do for command X"
requires reading X. After the change it requires reading `output.rs`, and X only has to answer
"what is my result type".

### 2. `render_value` becomes an implementation of the seam, not a rival

Delete `render_value` (`output.rs:19-29`). Replace it with a newtype in the same file:

```rust
pub struct RawReply<'a>(pub &'a redis::Value);

impl Renderable for RawReply<'_> {
    fn render_table(&self, _: bool) -> String { format_value(self.0, 0) }
    fn render_json(&self) -> serde_json::Value { value_to_json(self.0) }
    fn render_raw(&self) -> String { /* unquoted, one line per element */ }
}
```

All 20 call sites become `print_output(&RawReply(&value), ctx.global().output,
ctx.global().no_color)` — a mechanical rewrite (`sed`-able). `format_value` and `value_to_json`
are **kept unchanged**; they are the crate's only RESP→text/JSON projection and carry 12 unit
tests (`output.rs:88-173`) that keep passing verbatim.

This is the **adapter** move: the passthrough commands were never wrong to need untyped
rendering, they were wrong to need a *second mode dispatch* to get it. Removing the dispatch
fixes `--output raw` for all 20 sites (P6) and stops discarding `no_color` (P5) as a side effect
of having one place that decides.

The four hand-rolled `matches!` branches (`client.rs:183`, `acl.rs:385`, `search.rs:347`, and the
`health.rs:123-127` `--json` override) collapse the same way: `client.rs::run_info` becomes a
`Renderable for ClientInfo` impl (the type already derives `Serialize`), `acl.rs::run_cat` and
`search.rs::run_info` become `RawReply` or a small typed wrapper.

### 3. `upgrade` gets result types (the largest single piece)

Each of the five wired arms builds a value and renders it. The types mostly exist:
`UpgradeStatusResponse` (`:68-76`) and `VersionInfo` (`:60-65`) already derive `Serialize`, so
`render_json` is `to_value(self)`. Two new types are needed — a `PreflightReport { checks:
Vec<Check>, errors: u32, warnings: u32 }` for `run_check` (`:203-266`, whose 11 `println!`s are
already a check list with pass/fail marks) and an `UpgradePlan { mode, steps: Vec<Step> }` for
`run_plan` (`:268-329`). Exit codes are unchanged: `run_check` still returns 1 when
`errors > 0` (`:258`), `run_rollback` still returns 1 when unsafe (`:361`).

The confirmation prompt (`:390-398`) stays hand-rolled and moves entirely to stderr.

### 4. Move rendering out of `ops/latency.rs` (73 ceded this)

`render_ascii_graph` (`ops/latency.rs:135-185`) and `render_histogram_table` (`:188-203`) are
rendering inside an operation module — a direct violation of the layer rule 73 writes into
`ops/mod.rs` ("an operation module … never prints"). Both are pure `String`-returning functions
over engine types, so the move is mechanical: relocate to `commands/debug.rs` as the bodies of
the `render_table` impls 73's adapters need, and carry the four unit tests (`:205-261`) with
them. `render_json` on those adapters serializes `LatencyHistoryPoint` / `CommandHistogramEntry`,
so `--output json` never emits ASCII art — which is the same fix as P3, applied before the code
that would reintroduce it exists.

**And fix P3 itself**: `print_histogram` (`debug.rs:503-543`) folds into `LatencyResult`'s
`render_table`, so the distribution is part of the rendered table rather than an append to
whatever the seam just wrote.

### 5. FR5: give Role a type, at the one place the wire is parsed

Add to `info_parser.rs` — the module that already owns "turn an INFO response into values", and
the only place in the crate that reads `INFO replication`:

```rust
/// A node's replication role. The wire spells these `master`/`slave`;
/// this type is the boundary where that vocabulary stops.
pub enum Role { Primary, Replica, Unknown }

impl Role {
    pub fn from_info(info: &InfoResponse) -> Role { /* "master"|"primary" → Primary, "slave"|"replica" → Replica */ }
}

impl std::fmt::Display for Role { /* "Primary" | "Replica" | "unknown" */ }
```

`Serialize` renders the same strings, so `--output json` carries `"role": "Primary"`.

Four call sites collapse to one construction each: `health.rs:200-219` (which currently reads the
role twice — once from the merged INFO at `:200-203` and again from a second round-trip at
`:212-219`, a redundancy the single constructor removes), `health.rs:252-256`,
`replication.rs:125-129`, `replication.rs:189/196`. `HealthResult.role` becomes
`Option<Role>` (`health.rs:38`), `ReplicationStatus.role` becomes `Role` (`:49`), and
`replication.rs:64`'s defensive `== "replica" || == "slave"` becomes
`matches!(self.role, Role::Replica)` — the double-compare disappears because the type carries the
evidence.

Rename the leaked JSON keys and table labels to the canonical vocabulary:
`connected_slaves` → `connected_replicas`, `master_repl_offset` → `primary_repl_offset`,
`master_host`/`master_port` → `primary_host`/`primary_port`,
`master_link_status` → `primary_link_status`, `slave_repl_offset` → `replica_repl_offset`
(`replication.rs:49-56`); `Master:` → `Primary:`, `Slave Repl Offset:` → `Replica Repl Offset:`,
`Master Repl Offset:` → `Primary Repl Offset:` (`:67`, `:73`, `:78`). The `INFO` **field names**
being read (`replication.rs:130-143`) keep their wire spelling — that is the boundary, and
`cluster.rs:141-144` stays untouched for the same reason.

This is a **breaking output change** for anyone parsing `frogctl replication status --output
json`. FrogDB is pre-production and `CLAUDE.md` explicitly permits it; the alternative is
shipping a documented contract the tool does not honor.

### 6. The flags that lie: wire, reject, or delete — each ruled

- **`debug slowlog --all` → wire it.** `CONTEXT.md:26-28` names slowlog as a Fan-out command, so
  the contract says implement. The primitive is `health.rs:284-306` (`run_fanout` over
  `ConnectionContext::resp_to`, `connection.rs:52-61`) and the merge shape is `FanOutResult`
  (`health.rs:85-88`). ~30 lines.
- **`debug latency --history` → delete the flag.** Ruled here because it is the CLI surface.
  "Periodic snapshot mode (every 15s)" is already covered twice over: `frogctl stat --interval`
  *is* periodic sampling (`stat.rs:29-140`), and `debug latency graph` — which 73 wires over
  `latency_history` + `render_ascii_graph` — *is* the history view, backed by the server's
  `LATENCY HISTORY`. A third spelling of the same idea, unimplemented, on a one-shot sampler is
  drift, not a feature. Deleting it makes `frogctl debug latency --history` exit 2 with
  `unexpected argument`, which is honest and reversible.
- **`debug hotshards --all` → leave declared, arm still bails.** The whole subcommand is
  unimplemented; nothing lies today because nothing runs.
- **TLS → enable it and reject what is not wired.** Add `tls-rustls` to the `redis` dependency
  (`Cargo.toml:34`), matching the `rustls-tls` already used for the HTTP planes
  (`Cargo.toml:35`), so `--tls` works. Then, for `--tls-cert`/`--tls-key`/`--tls-ca`: redis-1.1's
  `ConnectionAddr::TcpTls` accepts `tls_params`, so wiring the CA and client certificate is real
  work with a real test requirement (a TLS-enabled `TestServer`). Until that lands, **reject the
  three paths with an explicit "not supported" error** rather than accepting and ignoring them —
  the same ruling 74 makes for `debug zip --redact`/`--nodes` (`74:508-512`), and for the same
  reason: a security-relevant flag that silently does nothing is worse than one that errors.
- **`--json` on `health` / `cluster slots` → keep, document as an alias.** They predate nothing
  and removing them breaks scripts for no architectural gain; the fix is that they set
  `OutputMode::Json` and then go through the one seam, which `health.rs:123-127` already does.

### 7. Pin it: `lint-render-seam` in the `lint-gates` family

A compile-free grep gate (`scripts/render-seam.py`, idiom of `scripts/clock-seam.py` and
`scripts/continuation-lock-gate.py`), joined to `Justfile:329` so it runs in lefthook pre-commit
and CI's `seam-gates` job, and is inherited by `just lint` via `:319`:

1. **No stdout writes outside the seam.** `println!` / `print!` are forbidden in
   `frogctl/src/commands/` and `frogctl/src/ops/`, with a named allowlist for the streaming
   commands (`watch.rs`, `subscribe.rs`, `stat.rs`) carrying a one-line justification each.
2. **No mode dispatch outside `output.rs`.** `match`/`matches!` over `OutputMode` is forbidden
   outside `frogctl/src/output.rs`, except the two `--json` alias sites, listed by name.
3. **No wire role vocabulary outside the boundary.** The tokens `"master"` / `"slave"` as string
   literals are forbidden in `frogctl/src/commands/`, except `info_parser.rs` (the parser) and
   `cluster.rs` (`CLUSTER NODES` flag tokens) — both listed with the reason.

Rule 3 is the systematic answer to FR5. A vocabulary rule stated only in `CONTEXT-MAP.md:37-39`
is a rule four separate authors already got wrong the same way; a rule in `lint-gates` is one a
fifth cannot.

### The deletion test, applied honestly

**`render_value` (`output.rs:19-29`) — deletes clean.** It is a duplicate of `print_output`'s
`match`, and every behavior it provides is recoverable from `format_value` + `value_to_json` +
one `Renderable` impl. Deleting it removes capability *from the codebase* and none from the tool.
Net: −11 lines at the definition, −20 duplicated argument lists at the call sites, +12 for the
newtype.

**`format_value` / `value_to_json` (`:31-86`) — do not delete clean.** They are the only
RESP→human/JSON projection in the crate, consumed by every passthrough command, and covered by 12
tests. They stay verbatim.

**The four hand-rolled `matches!` branches — delete clean.** Each is strictly less capable than
the seam it bypasses (three of the four mishandle `Raw`).

**`--no-color` (`cli.rs:68-70`) — deletes clean today, and that is the trap.** Nothing reads it,
so removing the flag plus 26 arguments plus 18 parameters costs zero behavior. But it is a
*published* global flag on a tool whose whole job is scripted operation, and the `NO_COLOR`
convention is one operators expect. **Ruling: keep the flag, keep the threading, and make it
load-bearing** — the seam is the natural place for the crate's first colorization, and a flag
that is honored-by-construction at one chokepoint costs nothing to carry. Removing it would be
the defensible alternative; what is not defensible is leaving 18 parameters named `_no_color`.

**`comfy-table` / `dialoguer` / `indicatif` — delete clean.** Zero references. §P9 rules each.

**FR5's `Role` enum — the *contract* is what does not delete.** Deleting the proposed enum returns
the crate to four `&str` closures, and `CONTEXT-MAP.md:37-39` plus `frogctl/CONTEXT.md:41-43`
remain, still violated. The enum is not the valuable thing; it is the cheapest way to make an
already-written contract structural instead of aspirational. **Leverage:** ~40 lines in
`info_parser.rs` retire four duplicated map sites, one defensive double-compare, two
`unwrap_or("master")` fallbacks, six leaked JSON keys and three leaked table labels — and a
`lint-gates` rule then holds the line for free.

## Testability improvement

**What cannot be tested today.** There is no assertion anywhere in `frogctl/tests/` that any
command respects `--output`. The existing integration tests assert only `exit_code == 0`
(`integration_scan.rs:34,47,66,81,97`, `integration_data.rs:15,27`, `integration_config.rs:15,28`
— a test that passes if the command prints nothing at all). Nothing could assert more, because
the property "stdout under `--output json` parses as JSON" is not owned by any function: it is
distributed across `print_output`, `render_value`, four `matches!` branches and 86 raw
`println!`s. There is nothing to point a test at.

**What the seam unlocks — one property, applied to every command.** After the change, the whole
Output Mode contract is one testable statement:

- **`--output json` produces exactly one parseable JSON document on stdout.** A table-driven test
  over every subcommand: run against a `TestServer` (`tests/common/setup.rs:1-2` supplies
  `ctx_for_server`), capture stdout, `serde_json::from_str`. Today this fails for `upgrade` ×5,
  `stat`, `data slot`, `replication topology`, `replication promote`, `debug latency --dist`,
  `benchmark` (empty path), `backup trigger` and `client info -o raw`. Afterwards it is a
  regression test for all of them, and the count is the assertion.
- **stdout carries only the document.** Progress, prompts and warnings assert on stderr — the
  invariant 73's adapters depend on and that P2 shows does not exist yet. Pinning it here is what
  makes 73's stderr progress closures a checkable contract rather than a convention.
- **`Renderable` is unit-testable without a server.** The 18 impls become pure
  `value → String` / `value → serde_json::Value` functions. `ReplicationStatus`,
  `HealthResult`, `FanOutResult`, `UpgradeStatusResponse` can be constructed literally and their
  three renderings asserted — no `TestServer`, no connection, milliseconds. This is where the
  `no_color` behavior finally becomes assertable too.
- **FR5 becomes a table test.** `Role::from_info` over the four INFO shapes (`role:master`,
  `role:slave`, missing field, unexpected value) is a pure function with four cases, sitting
  beside `info_parser.rs`'s two existing tests (`:47-88`). Today the equivalent logic is four
  inline closures inside `async fn`s that each need a live server to reach.
- **The Fan-out contract gets one home.** With `slowlog --all` wired through the same
  `run_fanout` primitive as `health --all`, one test shape covers both, and
  `frogctl/CONTEXT.md:26-28` becomes true for two of its three named commands (hotshards follows
  whenever its arm is implemented).

**Prevention, not just coverage.** `lint-render-seam` (§7) is the systematic half. The three
bypass classes this proposal removes — a second mode dispatch, a raw `println!` in a command
module, a wire role literal outside the boundary — are each a one-line grep, and each is exactly
how the current state accumulated across at least four independent authors. Tests catch the
commands that exist; the gate catches the twentieth one.

## Risks / scope boundaries vs siblings

### Boundary vs proposal 72 (FR2, config schema)

72 pins its file set explicitly and lists `frogctl/src/output.rs` and `frogctl/src/info_parser.rs`
under **"Not in this file set"** (`72:78-80`), then states the partition directly: "FR4 owns
`frogctl/src/output.rs` and `commands/upgrade.rs` … 72 adds two `Renderable` impls **inside
`commands/config.rs`** and does **not** touch `output.rs` or `print_output`'s signature. FR5 owns
`info_parser.rs`. Disjoint." (`72:484-487`). Confirmed from this side.

**Two files are shared with disjoint hunks, and both need naming:**

- `frogctl/src/connection.rs` — 72 edits `admin_url()` `:139-144` (its hotfix H1); 75 edits
  `build_url` `:22-29`. No shared lines.
- `frogctl/src/cli.rs` — 72 edits the `--admin-url` help text `:56`; 75 edits the TLS options
  `:44-54`. No shared lines.

**One real interaction:** 72's new `Renderable` impls in `commands/config.rs` will be written
against `print_output`'s current signature. This proposal does not change that signature — it
only removes *competitors* to it — so 72's impls compile unchanged in either order. If 75 lands
first, 72's impls should return a real `render_raw` rather than delegating to
`render_table(true)`; that is a one-line difference, and `config.rs:88-95` is already one of the
two impls in the crate that gets it right.

### Boundary vs proposal 73 (FR1, `ops/` wiring) — four explicit handshakes

73 hands 75 four items and this proposal accepts all four:

1. **The `render_value`/`Renderable` seam** (`73:494-496`). 73 notes every adapter it adds
   already exits through `print_output`, so it *adds no work here* and removes some. Confirmed:
   73's scan deletion drops a hand-rolled path.
2. **Relocating `render_ascii_graph` / `render_histogram_table` out of `ops/latency.rs`**
   (`73:497-501`). Accepted — §Proposed change step 4. **Ordering matters:** 73 calls both
   functions *in place* from its new `render_table` bodies. If 75 lands first, 73's adapters
   import from `commands/debug.rs` instead of `ops::latency`; if 73 lands first, the move is a
   two-function relocation plus one import change. **Preferred order: 73 first** — the functions
   should move together with the adapters that call them, and moving them into a `render_table`
   that does not exist yet is worse.
3. **Hotfix H2, `debug slowlog --all`** (`73:614-625`). Owned here. **Ruling: implement the
   Fan-out**, not `bail!` — the glossary (`CONTEXT.md:26-28`) names slowlog as a Fan-out command,
   so refusing would retract a documented capability rather than deliver it. Reuses
   `health.rs:284-306`.
4. **The H3 `debug latency --history` call** (`73:627-632`). Owned here. **Ruling: delete the
   flag** — §Proposed change step 6, on the grounds that `stat --interval` and
   `debug latency graph` already cover it twice.

**Shared file, sequenced hunks:** `commands/debug.rs` is edited by 73 (arm bodies at `:405`,
`:408`, `:411`, `:418`, `:421`), by 74 (the zip arm at `:394`), and by 75 (the two `..`
destructuring patterns at `:396-402` and `:427-431`, `print_histogram` `:503-543`, and the raw
`println!`s at `:512`, `:527`, `:537`, `:646`, `:768`). 73 already flags that fixing the
destructuring is "a one-line rebase against 73's arm-body edits" (`73:505-507`). Correct — the
patterns and the arm bodies are adjacent but not overlapping.

**One correction to 73, offered in good faith.** `73:88` cites `frogctl/Cargo.toml` as 47 lines
with `indicatif :38, comfy-table :39, dialoguer :40, zip :41`. At this SHA the file is **48**
lines and those deps are at `:37`, `:38`, `:39`, `:40` — off by one. The substance (zero uses) is
correct and independently re-derived here.

### Boundary vs proposal 74 (FR3, debug bundle)

74 pins its frogctl footprint to "**exactly one arm and one dependency line**" (`74:518-519`) —
the `debug zip` arm at `debug.rs:394` and the `zip` dependency at `Cargo.toml:40` — and
explicitly disclaims the role type: "75 … owns `frogctl`'s rendering path and the client-side role
enum in `info_parser.rs`. … [`BundleContext.role`] is a server-side value read off the `LiveMode`
seam and serialized into an archive, not a parsed CLI display type" (`74:517-523`). Accepted, and
symmetric: **this proposal does not touch `debug.rs:393-395`, `upgrade.rs`'s clap surface beyond
its render paths, or any server-side file.**

The `upgrade.rs` assignment in the task brief is worth stating precisely, since 72 and 74 both
route it here: `commands/upgrade.rs` render paths are **75's** (`72:484-485`); 74 does not claim
it. Its one `bail!` (`:127`, `upgrade node`) belongs to nobody in this round and stays.

If 74's `Role` ever moves to a shared crate, `info_parser::Role` is the natural client-side
adopter — but they are different types today (one is a serialized archive field, one is a display
projection of an INFO string) and coupling them is not proposed.

### The `hide = true` question — ruled, with evidence

The task brief asks whether to hide the remaining stub subcommands from `--help`. 73 flagged it
for the orchestrator without ruling (`73:641-645`). **Ruling: do not hide them.** The decisive
fact is mechanical, not aesthetic:

`docs-gen`'s tree walk (`frogdb-server/ops/docs-gen/src/main.rs:577-593`) filters exactly one
thing — `sc.get_name() != "help"` (`:589`). It never consults `Command::is_hide_set()` or
`Arg::is_hide_set()`. So `#[command(hide = true)]` would remove the stubs from `frogctl --help`
while leaving them fully documented in `website/src/data/frogctl-cli.json` and rendered on the
published reference page. That makes the discrepancy **worse**: the surface an operator can
discover shrinks, the surface that lies stays put, and the command still parses and still
`bail!`s. Hiding without teaching `docs-gen` about `hide` is not honesty, it is concealment from
the wrong audience.

Two secondary reasons: (a) the count moves under the siblings' feet — 28 `bail!`s at HEAD, 19
after 73 wires its nine, **15** after 72 (−3), 73 (−9) and 74 (−1) all land — so any hide list
written now is stale by the end of the round; (b) 73's `lint-frogctl-bails` count pin
(`73:334-342`) already delivers the actual goal, which is that the number can only go down and
must be reviewed to change.

**If the orchestrator wants the surface narrowed anyway**, the honest version is a separate
issue with two parts: teach `build_cli_command` to skip hidden subcommands, *then* hide. That is
a docs-gen change plus a website regeneration, not a `cli.rs` annotation, and it is not folded
in here.

### Behavioral risk of the change itself

- **`--output json` shapes change for FR5's renamed fields.** `connected_slaves` →
  `connected_replicas` and the five `master_*`/`slave_*` keys are a breaking change for anything
  parsing `frogctl replication status -o json`. Deliberate (§step 5), permitted by the project's
  pre-production stance, and the alternative is a documented contract the tool violates. It must
  be called out in the change description; there is no deprecation window worth building for a
  tool with no released consumers.
- **Rendered table output changes** wherever `Master:` becomes `Primary:` and `master` becomes
  `Primary`. 73's new integration assertions land around the same time; whichever proposal lands
  second updates the expected strings. Cheap, but it is a real merge interaction, which is
  another argument for **73 before 75**.
- **`--tls` goes from "always fails" to "connects".** Adding the `tls-rustls` feature grows the
  dependency graph (`rustls`, `tokio-rustls`, `rustls-native-certs`) — all already present via
  `reqwest`'s `rustls-tls` (`Cargo.toml:35`), so the incremental cost is small. The genuinely new
  risk is that `--tls` now reaches a real handshake and can fail in new ways; that is the correct
  failure surface and it needs a TLS-enabled `TestServer` case to be worth trusting. Rejecting
  `--tls-cert`/`--tls-key`/`--tls-ca` explicitly (rather than wiring them half-way) keeps that
  test surface honest.
- **`upgrade`'s human output is rewritten.** 86 `println!`s become `render_table` bodies. The
  rendering should stay byte-identical where it is already fine; `integration_upgrade.rs` (125
  lines) is the existing guard and should gain output assertions rather than exit-code-only ones.
- **`just docs-gen` must run.** Deleting `--history` and adding "not supported" text changes the
  clap tree, so `website/src/data/frogctl-cli.json` regenerates and `just docs-gen-check`
  (`Justfile:817`) fails otherwise.
- **Streaming commands need a decision, not a bypass.** `watch`, `subscribe` and `stat` legitimately
  emit continuously. Their allowlist entries in `lint-render-seam` must carry a reason, and the
  right long-term shape is one `Renderable` per tick (NDJSON under `--output json`) rather than a
  permanent exemption. Proposed as the allowlist's stated intent, not implemented here.

### Vocabulary

`frogctl/CONTEXT.md`'s *Avoid* list is honored throughout: **Metrics API** (never "Observability
API"), **Cluster Topology** / **Replication Topology** (never bare "topology"), **Debug Bundle**
(never "Diagnostic bundle" — and the term belongs to 74). **Output Mode** is used as defined at
`:19-20`, **Fan-out** as defined at `:26-28`, **Data Plane** / **Admin API** as defined at
`:9-16`, and **Primary** / **Replica** as defined at `:41-43` — which is the entire point of FR5.
Where `master`/`slave` appear above, they are quoted wire tokens or quoted source, per
`CONTEXT-MAP.md:37-39`.

## Spec / LOCKED

**No locked-area exposure, and no mutation-gate implications.**

- The four locked areas are `frogdb-txn`+`frogdb-vll`, `frogdb-persistence`+`frogdb-recovery`,
  `frogdb-replication`+`frogdb-replication-runtime`, `frogdb-cluster`+`frogdb-cluster-runtime`
  (ADRs `adr/0002`–`0004`). **`frogctl` is none of them.** Every changed file in the §Files table
  is under `frogctl/` except three read-only citations (`CONTEXT-MAP.md`, `docs-gen`,
  `Cargo.lock`), one generated artifact (`frogctl-cli.json`), one hand-written doc page
  (`frogctl.mdx:56`), and two build-tooling additions (`Justfile:329`, `scripts/render-seam.py`).
- **Zero `FM-` tags exist anywhere in `frogctl/`** — grepped at this SHA, count 0. No file in this
  proposal's set is named as a *Forced by* test in any `.scratch/hardening/specs/*-failure-modes.md`.
- The two spec mentions of `frogctl` are prose, not contracts on this code:
  `cluster-failure-modes.md:56` places it explicitly out of scope, and
  `replication-failure-modes.md:523` names it only as a *consumer* of the replication-identity
  fields in `INFO replication`. **FR5 touches exactly that consumption** — but on the client side
  only: `Role::from_info` reads the same `role` field the server already guarantees, and no
  server-side field name, value or guarantee changes. The spec row stays true.
- `just lint-failure-modes` is unaffected: it walks spec rows against tagged tests, and this
  change adds neither.
- **Mutation gates: none apply.** `just mutants-diff <crate>` push discipline covers the four
  locked crate pairs; `frogctl` has no gate and is not a `cargo mutants` target in any recipe.
  No gate percentage moves.
- The one new lint (`lint-render-seam`) is a compile-free grep joining `lint-gates`
  (`Justfile:329`), which runs unconditionally in lefthook pre-commit and CI's `seam-gates` job.
  It states invariants about *stdout ownership and display vocabulary*, not about a locked-area
  seam, so it needs no spec row. Its three rules follow `agents/seam-lints.md`'s chokepoint idiom:
  every X (stdout write / mode dispatch / role literal) must go through Y (`print_output` /
  `output.rs` / `info_parser::Role`), with named, justified exceptions.

## Effort

**M.**

| step | size | note |
|---|---|---|
| 1. Seam rule in `frogctl/CONTEXT.md` | XS | doc only, alongside `:59` |
| 2. `render_value` → `RawReply` newtype; rewrite 20 call sites | S | mechanical; `format_value`/`value_to_json` and their 12 tests unchanged |
| 3. Collapse the 4 hand-rolled `matches!` branches | S | `client.rs`, `acl.rs`, `search.rs`, `health.rs` |
| 4. `upgrade.rs`: 86 `println!`s → 5 result types + `Renderable` | **M** | the bulk of the work; 2 new types, 3 existing; exit codes preserved |
| 5. Raw-`println!` tail: `stat`, `data slot`, `replication topology`/`promote`, `benchmark:164`, `backup:148`, `debug:512/527/537/646/768` | S | plus the streaming-command allowlist decision |
| 6. Move `render_ascii_graph`/`render_histogram_table` out of `ops/latency.rs` (+4 tests) | XS | mechanical; **after 73** |
| 7. FR5: `Role` enum + 4 call sites + 6 JSON keys + 3 table labels | S | breaking JSON change, deliberate |
| 8. `slowlog --all` Fan-out (H2) | S | reuses `health.rs:284-306` |
| 9. TLS: add `tls-rustls`, reject cert/key/ca paths | S | `Cargo.toml:34`, `cli.rs:44-54`, `connection.rs:22-29` |
| 10. Drop `comfy-table` / `dialoguer` / `indicatif` | XS | 3 manifest lines |
| 11. `lint-render-seam` + `lint-gates` wiring | S | 3 grep rules, mirrors `clock-seam.py` |
| 12. Tests: json-parseability table over every subcommand; `Renderable` unit tests; `Role::from_info` table | **M** | where the durable value is |
| 13. `just docs-gen` + `frogctl.mdx:56` | XS | JSON regenerates (clap tree changed) |

Not L: no crate boundary moves, no new abstraction beyond one enum and one newtype, and the seam
being consolidated onto already exists with the right signature. Not S: step 4 is 421 lines of
rewrite, step 12 is a new test surface, and step 7 is a deliberate breaking change with a
vocabulary sweep behind it.

### Independently-landable hotfixes

**H4 — `frogctl --tls` cannot connect. LIVE, severe, XS→S.**
*Trace:* `cli.rs:40-42` declares `--tls` → `connection.rs:23` selects the `rediss` scheme →
`redis::Client::open` (`:35`) → redis-1.1.0 `url_to_tcp_connection_info` reaches the
`#[cfg(not(any(feature = "tls-native-tls", feature = "tls-rustls")))]` arm and fails with
`"can't connect with TLS, the feature is not enabled"` → surfaced to the operator as
`invalid connection URL: rediss://<host>:<port>`. `frogctl/Cargo.toml:34` enables only
`tokio-comp`; `Cargo.lock:4096-4118` confirms no `rustls`/`native-tls` in the resolved graph, and
`redis` is declared exactly once workspace-wide, so no feature unification applies. **Every Data
Plane command fails under `--tls`.** *Fix (XS):* add `tls-rustls` to `Cargo.toml:34`. *Fix
(complete, S):* also reject `--tls-cert`/`--tls-key`/`--tls-ca` with an explicit "not supported"
error until `tls_params` is wired. *Owner:* 75 — but `Cargo.toml:34` and `connection.rs:22-29`
are untouched by 72/73/74, so it lands standalone with zero rebase.

**H5 — `frogctl debug latency --dist --output json` emits invalid JSON. LIVE, XS.**
*Trace:* `debug.rs:495` `print_output` writes the JSON document → `:497-499` calls
`print_histogram` unconditionally → `:527-541` `println!`s ASCII bars to stdout. Piping to `jq`
fails. `:512` is the same bug on the min==max path. *Fix:* gate the call on
`matches!(ctx.global().output, OutputMode::Table | OutputMode::Raw)` — one line — or fold the
histogram into `LatencyResult::render_table` (the proper fix, §step 4). *Owner:* 75. Touches
`debug.rs:497-499`, which is inside neither 73's arm bodies (`:405-421`) nor 74's arm (`:394`).

**H6 — `website/.../reference/frogctl.mdx:56` publishes a TLS example that cannot work. XS.**
*Trace:* the Examples block reads `frogctl --tls --tls-ca ca.crt health` → fails per H4, and
`--tls-ca` is unread regardless (`connection.rs` reads only `global.tls`). *Fix:* remove the
example, or restore it once H4 lands. **Coordinate with 73's H1** (`73:606-612`), which fixes
`frogctl backup snapshot` at `:50` — *same fenced code block*, so the two edits conflict textually
if landed independently. Land them together or sequence them explicitly.

**H2 — `frogctl debug slowlog --all` silently ignores its addresses. LIVE, S.** Ceded by 73
(`73:614-625`); trace re-verified at this SHA (`debug.rs:88-90` declared → `:427-431`
destructures `{ count, analyze, reset, .. }` → `run_slowlog` `:620` queries one node).
**Ruled: implement the Fan-out** over `health.rs:284-306`, not `bail!` — `CONTEXT.md:26-28` names
slowlog as a Fan-out command. Rebases against 73's `debug.rs` edits in one line.

**H3 — `frogctl debug latency --history` silently ignores the flag. LIVE, XS.** Ceded by 73
(`73:627-632`) as "75's call"; trace re-verified (`debug.rs:44-46` declared → `:396-402` dropped
in `..` → `run_latency` `:445-451` never sees it). **Ruled: delete the flag** — `stat --interval`
and `debug latency graph` already provide periodic sampling and the history view respectively.
Requires `just docs-gen`, so it is not purely local; land it with step 13 rather than alone.

### Deliberately not hotfixed

- **`--no-color` doing nothing** (§P5). **Latent** — 18 discarded parameters, but zero color in
  the crate, so no operator sees wrong output. Real debt, not a hotfix.
- **`--output raw` ≡ `--output table`** for 16 of 18 `Renderable` impls and all 20 `render_value`
  sites (§P6). LIVE but low-severity, and fixing it properly means writing 16 `render_raw` bodies
  — that is step 2/3 work, not a standalone patch.
- **`comfy-table` / `dialoguer` / `indicatif` in `[dependencies]` with zero uses** (§P9). Build
  hygiene, not user-observable. Folds into step 10. Note this is the same class as 73's `tempfile`
  finding (`73:636-640`) and the two should be swept together.
- **`hide = true` on the remaining stubs.** Ruled against above, with the `docs-gen` evidence. If
  the orchestrator overrides, it needs its own issue covering `build_cli_command:577-593` first.
