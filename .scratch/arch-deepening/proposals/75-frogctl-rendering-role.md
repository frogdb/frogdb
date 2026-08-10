# Proposal 75 — frogctl: one Output Mode seam, one Role type, and the flags that lie

Round 38 · lane: frogctl / operator / telemetry · effort **M** · candidates FR4 + FR5 · ordered
**after** proposals 72 (FR2) and 73 (FR1); independent of 74 (FR3)

Verified against the tree at `4372082285b34079ae6c1eb0c2d135a55d91ca83`, re-confirmed at
`50118a53922bfe1aa5a7c56f494921df180d8770`, and **re-verified line-by-line at
`54baa2bb6a3d0586808fb2686c49026089793171`** after adversarial review (verdict **AMEND**, review
taken at `2e81506b`). No source file changed across those SHAs. Every path, line number and count
below was re-derived by reading at the current SHA; the lane brief's citations were **not**
trusted, and neither were this proposal's own first-draft citations — **~15 were wrong and are
corrected**, and the review found **two dispatch sites this proposal missed**, **one fix that does
not compile as written**, and **one headline test with no mechanism**. All of it is applied below;
the full accounting is in §Review ledger.

**Cross-proposal citations (`72:NNN`, `73:NNN`, `74:NNN`) are line references into sibling
proposal files that have since been revised. Every one of them must be re-derived at merge
time** — the *substance* of each handshake was re-verified against the siblings' current text,
but the line numbers drift with every revision. The 74↔75 mutual-citation loop is real and
mutually consistent in substance.

## Summary

`frogctl` declares one **Output Mode** (`table | json | raw`, `frogctl/CONTEXT.md:19-20`) and
asserts one relationship over it: "**Output Mode** is orthogonal to all commands: every result
renders as table, JSON, or raw" (`CONTEXT.md:59`). The crate has a seam built for exactly that —
`trait Renderable` + `print_output` (`output.rs:3-16`) — and then **eight independent places that
decide what an Output Mode means**, plus a long tail of commands that never ask.

That is not a missing seam. It is a **duplicated** one, which is worse: `print_output`
(`output.rs:9-16`) and `render_value` (`output.rs:19-29`) are two `match mode { … }` blocks
twenty lines apart in the same file, and the second one silently drops `no_color` and collapses
`Raw` into `Table`. **Six** more hand-rolled `matches!` branches live in `client.rs`, `acl.rs`
(×2), `search.rs` (×2) and `health.rs`. And one whole command namespace — `upgrade`,
**86 `println!`s across 421 lines** — never reads the Output Mode at all.

One of those six is the thesis in miniature. `acl.rs:206-213`:

```rust
    if matches!(ctx.global().output, crate::cli::OutputMode::Json) {
        let detail = parse_getuser_value(&value);
        print_output(&detail, ctx.global().output, ctx.global().no_color);
    } else {
        let detail = parse_getuser_value(&value);
        print_output(&detail, ctx.global().output, ctx.global().no_color);
    }
```

The two arms are **byte-identical**. An author reached for a mode conditional out of habit, in a
function that was already correct without one, and nothing in the build objected. That is what a
duplicated chokepoint costs: the bypass becomes the reflex.

The sharpest consequence is not cosmetic. `frogctl debug latency --dist --output json` calls
`print_output` (`debug.rs:495`) and *then* `print_histogram` (`:497-499`), which unconditionally
`println!`s ASCII bar charts to **stdout** (`:504-543`). The command emits a JSON document
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
| `frogctl/src/commands/debug.rs` | 770 | **the change (D6 + stdout mixing).** `Latency.history` declared `:44-46`, dropped by `..` `:396-402`; `Hotshards.all` `:73-75`; `Slowlog.all` `:88-90`, dropped by `..` `:427-432`; `run_latency` `:444-502` (`print_output` `:495` then `print_histogram` `:497-499`), `print_histogram` `:504-543` (stdout ASCII), TTY guard `:450`, `:459`, `:474`; raw `println!`s `:512`, `:527`, `:537`, `:646`, `:768`; `LatencyResult` `:169-176` (**does not carry `dist`/buckets** — see H5). **Arm bodies are 73's and 74's** — see §Risks. `ClientInfo` is declared **here**, `:198-206` `pub(crate)`, with `parse_client_line` `:577` — not in `client.rs` |
| `frogctl/src/commands/client.rs` | 204 | **the change.** `run_info` `:179-204` — a hand-rolled fourth path: `matches!(…Json)` `:185`, raw `println!`s `:187-201`, and **`OutputMode::Raw` falls through to the table branch**. `ClientInfo` (imported from `debug.rs`, `client.rs:4`) already derives `Serialize`; `ClientListResult`/`Renderable` `:55-84` is the pattern. `render_value` sites `:156`, `:166`, `:175` |
| `frogctl/src/commands/acl.rs` | 406 | **the change.** 10 `render_value` sites `:178`,`:187`,`:196`,`:266`,`:279`,`:288`,`:300`,`:310`,`:327`,`:394`; **two** hand-rolled `matches!(…Json)` sites: `run_getuser` `:206-213` (**byte-identical arms** — see §Summary) and `run_cat` `:385-392`. `run_cat`'s non-JSON arm prints `extract_string(item)` bare, one per line (`:389-391`) |
| `frogctl/src/commands/search.rs` | 429 | **the change.** 6 `render_value` sites `:246`,`:337`,`:356`,`:389`,`:399`,`:418`; **two** hand-rolled `matches!(…Table \| Raw)` sites at `:239-247` (`run_search`) and `:347-361` (`run_info`) — both invert the polarity, and `:347-361` therefore duplicates `print_output`'s json arm at `:359-360` |
| `frogctl/src/commands/exec.rs` | 47 | **the change (1 line).** `render_value` `:34` — the purest passthrough, and the shape the replacement newtype is designed for |
| `frogctl/src/commands/stat.rs` | 242 | **the change (small).** No Output Mode read anywhere. Header `println!`s `:35-38`, data line `:114`/`:117`, TTY guard `:30`. The ANSI escape `\x1b[2K\r` `:114` is cursor control, **not color** |
| `frogctl/src/commands/data.rs` | 119 | **the change (small).** `run_slot` raw `println!`s `:47`,`:48`,`:52`,`:53`. Sibling 73 owns the `run` dispatch `:36-45` |
| `frogctl/src/ops/latency.rs` | 261 | **the change (relocation, ceded by 73 `73:498-501`).** `render_ascii_graph` `:135-185` and `render_histogram_table` `:188-203` are rendering inside an operation module. Move to `commands/debug.rs`; their 4 unit tests `:205-261` move with them. Engine fns `:27-108` untouched |
| `frogctl/src/commands/cluster.rs` | 524 | **read-only evidence.** `is_primary()` `:141-144` matches `f == "master"` on `CLUSTER NODES` flag tokens — **correct**, and the model for where the wire boundary belongs. `Slots { json: bool }` `:117-121` is declared but **never read**: the arm `bail!`s at `:389-391`, and `json` appears nowhere else in the file (only `render_json` `:332-333`). **Not** an Output Mode override — a dead flag on a dead command |
| `frogctl/src/commands/benchmark.rs` | 200 | **the change (1 line).** Raw `println!` `:164` on the empty-result path, followed by `return Ok(1)` `:165` — an early exit that never reaches `print_output` `:198` |
| `frogctl/src/commands/backup.rs` | 190 | **the change (1 line).** Raw `println!` `:148` in `run_trigger`. Sibling 73 owns `run` `:130-144` |
| `frogctl/Cargo.toml` | 48 | **the change (deps).** `redis = { version = "1.1", features = ["tokio-comp"] }` `:34` — **no TLS feature** (§P4). `indicatif` `:37`, `comfy-table` `:38`, `dialoguer` `:39`, `zip` `:40` — **zero uses in the crate** (74 owns `zip`) |
| `frogctl/CONTEXT.md` | 78 | **the change (doc).** **Output Mode** `:19-20`, **Fan-out** `:26-28`, **Primary / Replica** `:41-43`, the orthogonality relationship `:59`. Its *Avoid* list is honored throughout |
| `CONTEXT-MAP.md` | 41 | **read-only evidence.** CLI→Server normalization claim `:27-30`; the shared vocabulary rule `:37-39` FR5 violates |
| `Cargo.lock` | — | **read-only evidence.** `[[package]] redis 1.1.0` `:4096-4118` — dependency list contains no `rustls`, `tokio-rustls` or `native-tls`. Proof that no feature unification rescues `--tls` |
| `frogdb-server/ops/docs-gen/src/main.rs` | — | **read-only evidence.** `generate_cli_reference(frogctl::cli::Cli::command(), …)` `:329`; `build_cli_command` `:577-593` filters **only** `"help"` — it never consults `is_hide_set()`. Decisive for the `hide = true` ruling (§Risks) |
| `website/src/content/docs/reference/frogctl.mdx` | — | **the change (hotfix H6).** `:55-56` publishes `frogctl --tls --tls-ca ca.crt health` — a command that cannot succeed. Same fenced ```bash block (`:30-60`) as 73's H1 at `:50`, five lines apart — **a true textual conflict at diff-context range**, see §Risks |
| `website/src/data/frogctl-cli.json` | 9,018 | **read-only evidence (generated).** Carries `tls-cert`/`tls-ca`/`tls-key` **267** times (global flags, repeated per subcommand). Regenerated by `just docs-gen`; never hand-edited |
| `frogctl/tests/` | — | **the change (tests).** `main.rs` `:1-11` declares 9 integration modules; `common/setup.rs` supplies `ctx_for_server` `:23-25`, **`ctx_for_server_json` `:28-33` (already exists)**, `ctx_with_metrics` `:36-41`, `ctx_with_admin` `:45-52`. Tests drive commands **in-process** (`scan::run(&args, &mut ctx)`, `integration_scan.rs:32`) — decisive for §Testability |
| `Justfile` | — | **the change.** `lint-gates` `:329` gains `lint-render-seam`; `lint` `:319` inherits it by dependency. **`just test frogctl` is hard-refused at `:80-83` (exit 2)**; the only runner for `frogctl/tests/` is `frogctl-test` `:296-298` (plus `coverage-lcov` `:104`). `docs-gen-check` `:816` |
| `.config/nextest.toml` | — | **read-only evidence (new finding).** `[profile.default]` `default-filter = 'not package(frogctl)'` `:5` — excludes **the entire `frogctl` package**, lib unit tests included, from `just test`. Decisive for §Testability Constraint 2 |
| `scripts/` | — | **the change (new file).** `scripts/render-seam.py`, in the idiom of `clock-seam.py` / `continuation-lock-gate.py` / `error-sanitize.py` |

Nothing here is in a **locked** area — see §Spec / LOCKED.

## Problem

### P1 — The Output Mode seam is duplicated eight ways

`frogctl/CONTEXT.md:59` promises orthogonality. Eight places decide independently what an Output
Mode means. The complete inventory, grepped at HEAD (`grep -rn 'OutputMode::' frogctl/src/` minus
`output.rs` and `cli.rs` returns exactly six hits, in five files):

| # | site | what it decides | what it gets wrong |
|---|---|---|---|
| 1 | `output.rs:10-14` `print_output` | Table / Json / Raw → `Renderable` | **the seam.** Correct by construction |
| 2 | `output.rs:20-28` `render_value` | Json → `value_to_json`; **`Table \| Raw` → one branch** | drops `no_color` (`_no_color` `:19`); `--output raw` ≡ `--output table` at all 20 call sites |
| 3 | `client.rs:185-203` `run_info` | `matches!(…Json)` → hand-serialize; else raw `println!`s | **`Raw` falls into the table branch**; re-implements `render_json` inline over a type that already derives `Serialize` |
| 4 | `acl.rs:206-213` `run_getuser` | `matches!(…Json)` → `print_output`; else → **the same `print_output`** | **the arms are byte-identical.** A pure no-op conditional: it decides nothing, and it still had to be written, reviewed and maintained. The most damning single instance of the thesis |
| 5 | `acl.rs:385-392` `run_cat` | `matches!(…Json)` → `value_to_json` + pretty-print; else line-per-item | duplicates `print_output`'s json arm verbatim; `Raw` ≡ `Table` |
| 6 | `search.rs:239-247` `run_search` | `matches!(…Table \| Raw)` → `print_output(&SearchResult)`; else `render_value` | inverted polarity; the JSON path abandons the typed result and re-renders the untyped RESP value instead — `-o json` and `-o table` describe **different shapes of the same reply** |
| 7 | `search.rs:347-361` `run_info` | `matches!(…Table \| Raw)` → `print_output`; else inline json | inverted polarity, so the json arm is a **third** copy of `serde_json::to_string_pretty` |
| 8 | `health.rs:123-127` | `args.json` (`--json`, `:28-30`) overrides the global `--output` | a per-command Output Mode surface the glossary does not define. It is the **only** such site — see below |

`render_value` (#2) is the load-bearing duplicate: **20 call sites** — `acl.rs` ×10, `search.rs`
×6, `client.rs` ×3, `exec.rs` ×1. Every one of them passes `ctx.global().no_color` into a
parameter named `_no_color`.

Row 8 is deliberately narrower than the first draft claimed. `cluster slots --json`
(`cluster.rs:117-121`) is **not** a second Output Mode override: the flag is declared, published,
and never read — `ClusterCommand::Slots { .. }` `bail!`s at `:389-391`, and `json` appears nowhere
else in the 524-line file. It is a dead flag on a dead command, which is the same shape as
`hotshards --all` (§P8) and gets the same ruling (§step 6). **`health.rs:123-127` is the only
`--json` alias site in the crate**, which matters because §7's lint rule 2 must name its
exceptions exactly.

### P2 — And a long tail that never asks at all

Grepped at HEAD (`grep -rcE 'println!|print!' frogctl/src`), lines containing a stdout macro, per
module:

| module | count | Output Mode read? |
|---|---:|---|
| `commands/upgrade.rs` | 87 | **never** — all five wired arms. **86 `println!` + 1 `eprint!`** (`:391`, the confirmation prompt — the crate's only stderr write) |
| `commands/client.rs` | 10 | partially (`:185`) |
| `commands/debug.rs` | 7 | partially — `print_histogram` is unconditional. 2 of the 7 are TTY-guarded cursor control (`:459`, `:474`) |
| `commands/stat.rs` | 6 | **never**. 1 of the 6 is TTY-guarded cursor control (`:114`) |
| `commands/data.rs` | 4 | **never** (`run_slot` `:47`,`:48`,`:52`,`:53`) |
| `output.rs` | 3 | the seam's own `print!` `:15` + 2 inside `render_value` |
| `commands/replication.rs` | 3 | **never** (`run_promote` `:178`, `run_topology` `:197`,`:209`) |
| `acl.rs` / `watch.rs` / `subscribe.rs` / `search.rs` / `benchmark.rs` / `backup.rs` | 2/1/1/1/1/1 | mixed |

`upgrade` is the extreme case and the reason FR4 is **LIVE**: `frogctl upgrade status -o json`,
`upgrade check -o json`, `upgrade plan -o json`, `upgrade rollback -o json` and
`upgrade finalize -o json` all print a human-formatted table with box-drawing checkmarks
(`upgrade.rs:218`, `:236`, `:355`). Every response type they render already derives `Serialize`
(`:60-65`, `:68-92`) — the JSON is one `to_value` call away and was simply never wired.

**And zero `eprintln!` exists anywhere in `frogctl/src/`** (grepped; the crate's *only* stderr
write of any kind is the confirmation prompt `eprint!` at `upgrade.rs:391`). Proposal 73 asks 75
to "preserve the stderr-progress / clean-stdout invariant" — worth stating plainly: **that
invariant does not exist at HEAD.** It is something 75 establishes and 73's progress closures
become the first users of. Note the arithmetic: upgrade's "86 `println!`s" excludes that `eprint!`
throughout this document, and the `eprint!` is the *one* line that already satisfies the invariant
— §step 3 keeps it exactly where it is rather than sweeping it with the rest.

### P3 — `--output json` emits output that is not JSON (LIVE, sharpest)

`commands/debug.rs:493-500`:

```rust
    print_output(&result, ctx.global().output, ctx.global().no_color);

    if dist {
        print_histogram(&latencies);
    }
```

`print_histogram` (`:504-543`) takes no mode and writes `println!("\nLatency Distribution:")`
(`:527`) plus twenty `#`-bar rows (`:537-541`) to **stdout**. So:

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

> **Security policy — RECORDED, PARKED.** The `--tls` family is security-relevant: an operator who
> passes `--tls-cert`/`--tls-key`/`--tls-ca` today gets them silently discarded, and would believe
> mTLS and CA pinning are in force when neither is. Under the standing project policy, security
> findings are **filed, not fixed** in this round. This section is the filing: the trace, the exact
> `Cargo.toml` feature that is missing (and the one that is *wrong* — see §step 6), and the four
> flags involved. **No part of it is proposed for implementation here**; §step 6, §step 9 and H4
> record the correct shape so that whoever unparks it does not have to re-derive it, and so that
> nobody repeats the first draft's compile error.

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

Those two fallbacks are not equally live, and the difference is worth stating precisely rather
than lumping them together:

- **`health.rs:255` is unconditionally live.** `check_remote_health` fetches
  `INFO server memory replication` (`:239-243`), so the field is normally present and the
  `slave`→`replica` map at `:254` does fire — but a Primary returns `role:master`, which falls
  through the map untouched and is rendered verbatim. Every `frogctl health --all` against a
  primary prints `master`.
- **`health.rs:200-203` takes its `unwrap_or("master")` branch *always*.** `check_node_health`
  fetches `INFO` with sections `["server", "memory"]` only (`:189`), so
  `info.get("replication", "role")` at `:200-201` can never succeed and the literal `"master"` is
  the value of `role` 100% of the time. It reaches the operator only when the *second*
  round-trip at `:213` (`ctx.info(&["replication"]).await.unwrap_or_default()`) fails or returns
  no `role`, because `:214-219` then overwrites it — and on the success path a primary still
  renders `master`, because `:217` maps only `slave`.

That second bullet is the shape FR5 removes: a redundant round-trip, a dead fetch, and a hardcoded
wire literal standing in as a default, all in fourteen lines. A single `Role::from_info` over one
INFO response collapses the whole thing.

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
    // `format_value` returns text with NO trailing newline; `print_output` uses `print!`.
    // The `\n` is added here so the 20 migrated sites keep the line ending `render_value`
    // gave them via `println!`. See "the newline" below — this is not cosmetic.
    fn render_table(&self, _: bool) -> String { format!("{}\n", format_value(self.0, 0)) }
    fn render_json(&self) -> serde_json::Value { value_to_json(self.0) }
    fn render_raw(&self) -> String { /* unquoted, one line per element, newline-terminated */ }
}
```

All 20 call sites become `print_output(&RawReply(&value), ctx.global().output,
ctx.global().no_color)` — a mechanical rewrite (`sed`-able **only after** the newline decision
below is baked into the newtype). `format_value` and `value_to_json` are **kept unchanged**; they
are the crate's only RESP→text/JSON projection and carry 12 unit tests (`output.rs:88-173`) that
keep passing verbatim.

This is the **adapter** move: the passthrough commands were never wrong to need untyped
rendering, they were wrong to need a *second mode dispatch* to get it. Removing the dispatch
fixes `--output raw` for all 20 sites (P6) and stops discarding `no_color` (P5) as a side effect
of having one place that decides.

**The newline — a real regression the "mechanical sed" framing hides, and the decision.**
The two functions do not agree on line termination:

```
output.rs:15   print_output:   print!("{text}")        ← no newline added
output.rs:23   render_value:   println!("{…}")         ← newline added
output.rs:26   render_value:   println!("{…}")         ← newline added
output.rs:31-66 format_value:  never appends a trailing '\n' (arrays join with '\n', no tail)
```

A naïve `sed` from `render_value(&v, …)` to `print_output(&RawReply(&v), …)` therefore **drops the
trailing newline at all 20 sites** — `frogctl acl list`, `frogctl exec GET k`, `frogctl search …`
and the rest would stop terminating their last line, leaving the shell prompt on it. That is
user-visible, it is a regression, and it would sail through an exit-code-only test suite.

Two ways to fix it:

| option | change | blast radius |
|---|---|---|
| **A (chosen)** — newtype appends `\n` | `RawReply::render_table`/`render_raw` return newline-terminated text | 1 new type. **Zero** effect on the 18 existing `Renderable` impls, which already emit their own trailing `\n` (`health.rs:61`, `replication.rs:76-81`, `FanOutResult:104-107` — every table row is `push_str(&format!("…\n"))`) |
| B — `print_output` becomes newline-terminating | `output.rs:15` `print!` → `println!`, or `print!("{}", text.trim_end())` + `\n` | changes the output of **all 18** existing impls, every one of which already ends in `\n`, producing a spurious blank line after every table. Would require auditing and editing 18 impls to compensate |

**Ruling: A.** The seam's contract stays "a `Renderable` returns the exact bytes to write,
including its own line termination" — which is what the 18 impls already assume and what makes
`print_output` a one-line function. B would relocate a formatting decision *into* the seam and
force 18 compensating edits to preserve current behavior, which is the opposite of the
consolidation this proposal is for. The cost of A is one `format!` and the comment above it.

**Pin it:** the first `Renderable` unit test written under §Testability asserts
`RawReply(&Value::Okay).render_table(true) == "OK\n"` — a two-line test that would have caught
this, and that exists precisely because the 20 sites now go through a type instead of a `println!`.

**The six hand-rolled `matches!` branches collapse, but not all the same way.** Three of the six
are genuinely mechanical; two need a typed wrapper because their non-JSON arm does not print what
`format_value` prints; one is a plain deletion:

- **`acl.rs:206-213` `run_getuser` — delete the conditional.** Both arms already call
  `print_output` with identical arguments. Removing the `if` is a strict no-op on behavior and
  removes the site entirely. Two lines deleted; nothing replaces them.
- **`client.rs:185-203` `run_info` — becomes `impl Renderable for ClientInfo`.** The type already
  derives `Serialize`, so `render_json` is `to_value(self)` and `render_table` is the nine
  `println!`s at `:187-201` joined with `\n`. **Placement note:** `ClientInfo` is declared in
  `commands/debug.rs:198-206` as `pub(crate)`, not in `client.rs` (`client.rs:4` imports it).
  Writing `impl Renderable for ClientInfo` in `debug.rs` puts a new hunk in the file 73 and 74 are
  both editing; writing it in `client.rs` is legal (the trait is local to the crate, so the
  coherence rules permit the impl in any module of `frogctl`) and keeps `debug.rs` untouched by
  this step. **Ruling: write the impl in `client.rs`**, immediately beside the existing
  `impl Renderable for ClientListResult` (`:55-84`) that already renders a `Vec<ClientInfo>`.
- **`search.rs:239-247` and `:347-361` — invert to `print_output`.** Both already build a typed
  result (`SearchResult`, `IndexInfo`) for the table path; the fix is to build it
  unconditionally and let the seam pick the mode. This *also* fixes a shape inconsistency §P1 row
  6 names: today `-o json` on `ft.search` returns raw RESP rather than the parsed document set.
  That is a deliberate JSON-contract change and is listed in §Risks.
- **`acl.rs:385-392` `run_cat` — a typed `AclCatResult`, not `RawReply`.** This one cannot be
  `RawReply` without changing output, so the first draft's "`RawReply` **or** a small typed
  wrapper" was hiding a user-visible change behind an "or". The non-JSON arm at `:389-391` prints
  `extract_string(item)` — a **bare category name per line**:
  ```
  keyspace
  read
  write
  ```
  `format_value` over the same `Value::Array` would print numbered, quoted entries:
  ```
  1) "keyspace"
  2) "read"
  3) "write"
  ```
  Those are different outputs. **Ruling: `struct AclCatResult { categories: Vec<String> }`** whose
  `render_table` reproduces the bare-name-per-line output byte for byte, `render_json` is the
  existing `Vec<serde_json::Value>` (`:386-387`), and `render_raw` is the same as the table.
  `run_cat`'s non-array fallback (`:394`) keeps using `RawReply`. **No `-o table` output changes.**
- **`health.rs:123-127` `--json`** stays exactly as it is: it already resolves to an `OutputMode`
  and hands it to `print_output`. It is an *alias*, not a rival dispatch, and §step 6 keeps it.

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

**And fix P3 itself**: `print_histogram` (`debug.rs:504-543`) folds into `LatencyResult`'s
`render_table`, so the distribution is part of the rendered table rather than an append to
whatever the seam just wrote.

**The fold requires a type change, and the first draft skipped it.** `LatencyResult`
(`debug.rs:169-176`) carries only `samples`, `min_us`, `max_us`, `avg_us`, `p50_us`, `p99_us` — it
has **neither the `dist` flag nor the bucket data**. Moving `print_histogram`'s body into
`render_table` unchanged would print the histogram on *every* `frogctl debug latency` invocation,
including the default one that does not pass `--dist`. That is a new bug, not a fix. The fold must
carry the state:

```rust
struct LatencyResult {
    samples: u64, min_us: u64, max_us: u64, avg_us: u64, p50_us: u64, p99_us: u64,
    /// Populated only when `--dist` was passed; `None` suppresses the distribution
    /// section in `render_table` and omits the field from `render_json`.
    distribution: Option<Vec<HistogramBucket>>,
}
```

`render_json` then serializes the buckets as data (`{lo_us, hi_us, count}`) rather than ASCII —
which is the actual fix for P3, not merely a suppression of it: `-o json --dist` gains the
distribution *as JSON* instead of losing it. The min==max degenerate path (`:511-513`, today
`println!("\nAll samples: {min}us")`) becomes an empty/one-bucket `distribution`, handled inside
`render_table`. See H5 for the one-line interim that does not require the type change.

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
- **`debug latency --history` → delete the flag.** Ruled here because it is the CLI surface — but
  the first draft's rationale was **false at HEAD and is withdrawn**. It claimed the capability is
  "covered twice over" by `stat --interval` and `debug latency graph`. Neither claim survives:
  `debug latency graph` **`bail!`s today** (`debug.rs:407-409`, "not yet implemented") and only
  becomes the history view *after 73 lands*, so under the independently-landable framing this
  proposal insists on elsewhere it cannot be cited as existing cover; and `stat --interval`
  samples `INFO` counters (keys, memory, clients, ops/sec, hit rate, net i/o — `stat.rs:96-110`),
  **not latency**, so it is a different measurement, not a second spelling of the same one.
  **The defensible ground is honesty alone, and it is enough:** the flag is published to
  `frogctl-cli.json`, documented as "Periodic snapshot mode (every 15s)", parsed, and then dropped
  into a `..` — an operator who passes it gets a one-shot sampler and no indication anything was
  ignored. Deleting it converts a silent lie into `exit 2: unexpected argument`. It is reversible
  (re-adding a clap flag is trivial), it costs no implemented capability (there is none), and if
  the 15s snapshot loop is later wanted it should be designed against `debug latency graph`'s
  server-backed `LATENCY HISTORY` rather than bolted onto the one-shot PING sampler. **Alternative
  on the table:** implement the loop. Rejected as scope — it is a new feature inside a seam
  consolidation.
- **`debug hotshards --all` → leave declared, arm still bails.** The whole subcommand is
  unimplemented; nothing lies today because nothing runs.
- **`cluster slots --json` → leave declared, arm still bails.** Same ruling as `hotshards --all`,
  for the same reason, and stated explicitly because the first draft got this one wrong in the
  other direction: it listed `cluster slots --json` as a second Output Mode override to be
  "kept and documented as an alias". It is not an override — `ClusterCommand::Slots { .. }`
  `bail!`s at `cluster.rs:389-391` and the `json` field is never read anywhere in the file. **No
  script can be using it**, so the "removing it breaks scripts" argument that carries
  `health --json` does not apply here; and equally, nothing lies today, so there is nothing to
  fix. It is left exactly as-is, and §7's lint rule 2 does **not** list it as an exception —
  because there is no dispatch there to exempt.
- **TLS → RECORDED AND PARKED (security policy). The correct shape is recorded; nothing is
  proposed.** Per §P4's policy note, the `--tls` family is a security finding: filed, not fixed.
  Two facts are recorded here so the filing is actionable when it is unparked:

  1. **The feature name in the first draft was wrong and would not have compiled.** Adding
     `tls-rustls` to `frogctl/Cargo.toml:34` alongside `tokio-comp` produces a **missing trait
     method** error, not a working TLS client. redis-1.1.0 declares `connect_tcp_tls` as a
     required method of `RedisRuntime` under
     `#[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]`
     (`aio/mod.rs:47-56`), but the Tokio implementation provides it only under
     `#[cfg(feature = "tokio-rustls-comp")]` (`aio/tokio.rs:146-147`) or
     `#[cfg(all(feature = "tokio-native-tls-comp", not(feature = "tokio-rustls-comp")))]`
     (`:119`). Bare `tls-rustls` turns the requirement on without turning any implementation on.
     **The correct feature is `tokio-rustls-comp`**, which is defined as
     `["tokio-comp", "tls-rustls", "dep:tokio-rustls"]` (redis `Cargo.toml:147-150`) — i.e. it is
     a superset of what is there today, so the line becomes
     `features = ["tokio-rustls-comp"]`.
  2. **A crypto-provider choice is mandatory, not optional.** See §Risks — `rustls 0.23` refuses
     to auto-select a provider when both backends are compiled in, and redis's rustls path calls
     `rustls::ClientConfig::builder()` (vendored `connection.rs:1190`), which **panics** in that
     situation. Whoever unparks this must install a provider explicitly.

  For `--tls-cert`/`--tls-key`/`--tls-ca`, redis-1.1's `ConnectionAddr::TcpTls` accepts
  `tls_params`, so wiring the CA and client certificate is real work with a real test requirement
  (a TLS-enabled `TestServer`). The shape 74 uses for `debug zip --redact`/`--nodes` — reject
  unwired security-relevant flags explicitly rather than accepting and ignoring them — is the
  natural precedent when this is unparked. **It is not proposed here**, and §Effort's step 9 is
  struck accordingly.
- **`--json` on `health` → keep, document as an alias.** It predates nothing and removing it
  breaks scripts for no architectural gain; the fix is that it sets `OutputMode::Json` and then
  goes through the one seam, which `health.rs:123-127` already does. This is the **only** such
  site (§P1 row 8).

### 7. Pin it: `lint-render-seam` in the `lint-gates` family

A compile-free grep gate (`scripts/render-seam.py`, idiom of `scripts/clock-seam.py` and
`scripts/continuation-lock-gate.py`), joined to `Justfile:329` so it runs in lefthook pre-commit
and CI's `seam-gates` job, and is inherited by `just lint` via `:319`:

1. **No stdout writes outside the seam.** `println!` / `print!` are forbidden across **all of
   `frogctl/src/`, minus `output.rs`** — not just `commands/` and `ops/`. The first draft scoped
   this to two directories, which would have left `main.rs`, `lib.rs`, `cli.rs`, `connection.rs`
   and `info_parser.rs` free to grow the exact bypass the rule exists to prevent; there is no
   reason a stdout write is more acceptable in `connection.rs` than in `commands/health.rs`.
   Named allowlist, one line of justification each:
   - `commands/stat.rs` — see the ruling below.
   - `commands/watch.rs:49` and `commands/subscribe.rs:58` — one site each, unbounded server-push
     streams; one `Renderable` per event is the intended end state (NDJSON under `-o json`),
     recorded as the allowlist's stated intent.
   - `commands/debug.rs:459`, `:474` and `commands/stat.rs:114` — TTY-guarded **cursor control**
     (`\r`, `\x1b[2K`), not content. Guarded by `stdout().is_terminal()` at `debug.rs:450` and
     `stat.rs:30`; the allowlist entry requires the guard to be present on the same function.
2. **No mode dispatch outside `output.rs`.** `match`/`matches!` over `OutputMode` is forbidden
   outside `frogctl/src/output.rs`, with **exactly one** named exception: `health.rs:123-127`, the
   `--json` alias, which resolves to an `OutputMode` and then defers to the seam. The first draft
   said "the two `--json` alias sites"; there is only one (§P1 row 8 — `cluster slots --json` is a
   dead flag on a `bail!`ing arm, with no dispatch to exempt).
3. **No wire role vocabulary outside the boundary.** Scope: `frogctl/src/commands/`. The rule
   matches the **exact quoted literals `"master"` and `"slave"`** — nothing looser — with one
   named exception, `cluster.rs:143` (`f == "master"` over `CLUSTER NODES` flag tokens, the
   correct boundary per §P7).

   Two corrections to the first draft, both material:
   - It exempted `info_parser.rs`. **That exemption is vacuous** — `info_parser.rs` lives at
     `frogctl/src/info_parser.rs`, *not* under `commands/`, so it was never in scope. The rule as
     written would have had exactly one real exception all along. (If rule 1's widened scope
     tempts anyone to widen rule 3 to match, note that `info_parser.rs` is then a genuine
     exemption and must be listed — but rule 3 stays at `commands/` scope here, because
     `info_parser.rs` is precisely where the wire vocabulary is *supposed* to live.)
   - **Exact-literal matching is load-bearing, not a simplification.** Genuine wire text survives
     inside `commands/` after FR5 and must not trip the gate: `replication.rs:130-143` reads the
     INFO **field names** `"connected_slaves"`, `"master_repl_offset"`, `"master_host"`,
     `"master_port"`, `"master_link_status"`, `"slave_repl_offset"`, and `:147` / `:200` build
     `format!("slave{i}")` to index the `slave0:`/`slave1:` INFO keys. Those are the server's wire
     spelling of *field identifiers* and cannot be renamed client-side. A substring grep for
     `master`/`slave` would flag all eight and the rule would be turned off within a week.
     Matching `"master"` and `"slave"` as complete quoted literals flags the display strings FR5
     removes and none of the field names FR5 must keep.

**`stat` — ruled once, here, because the first draft contradicted itself.** Step 5 listed `stat`
among the raw-`println!` sites to convert; rule 1 listed it on the allowlist; and neither noticed
that its rendering is not expressible as one `Renderable` per tick. Reading `stat.rs`:

- the header is printed **once** before the loop (`:34-40`), not per tick;
- on a TTY the data line is written as `print!("\x1b[2K\r{line}")` (`:114`) — **clear-line,
  carriage-return, no newline** — so each tick *overwrites* the previous one in place; off a TTY
  the same line goes out as `println!` (`:117`);
- two more bare `println!()`s (`:126`, `:135`) emit a single closing newline on exit, TTY-only.

`print_output` has no TTY awareness and no concept of "replace what I wrote last time". Routing
`stat` through it would either delete the in-place refresh (a real capability regression for the
tool's one live-monitoring command) or force cursor semantics into the seam.

**Ruling: `stat` is allowlisted, and step 5 does not convert it.** Its allowlist entry reads:
*"live-refresh display: the TTY path rewrites one line in place (`\x1b[2K\r`, no newline), which
the seam cannot express; the non-TTY path is line-per-tick and is the one that should become a
`Renderable` when NDJSON streaming is designed."* The non-TTY branch is the future convert target
and is recorded as such — not converted now, because doing it properly means designing the
streaming contract for `watch`/`subscribe`/`stat` together, which is explicitly out of scope
(§Risks).

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

**The six hand-rolled `matches!` branches — five delete clean, one is a strict no-op.** Each of
the five is less capable than the seam it bypasses (`client.rs:185` and `acl.rs:385` both drop
`Raw` into the table branch; `search.rs:239`/`:347` invert the polarity and, at `:239`, return a
different document shape under `-o json` than under `-o table`). The sixth, `acl.rs:206-213`, is
the cleanest deletion in the proposal: its two arms are byte-identical, so removing the
conditional cannot change behavior under any input, in any mode. It deletes with a proof.

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
(`integration_scan.rs:33,45,63,78,95` — the file is 96 lines; `integration_data.rs:15,28`;
`integration_config.rs:15,28` — a test that passes if the command prints nothing at all). Nothing
could assert more, because the property "stdout under `--output json` parses as JSON" is not owned
by any function: it is distributed across `print_output`, `render_value`, six `matches!` branches
and 86 raw `println!`s. There is nothing to point a test at.

### Two constraints the first draft ignored, and what they force

**Constraint 1 — the tests run in-process, so stdout cannot simply be "captured".** Every
`frogctl` integration test drives a command function directly:
`scan::run(&args, &mut ctx).await` (`integration_scan.rs:32`), `data::run(…)`, `config::run(…)`.
`print_output` writes to the **process-global** stdout of the test binary. Capturing that from
inside the same process needs either `std::io::set_output_capture` (unstable, nightly-only) or a
`gag`/`shh`-class fd-redirection dependency — and *both* are process-global, so they race
irreparably under `cargo nextest`, which runs tests concurrently in threads. **A "capture stdout
and parse it" test as the first draft described it is not implementable.** Two mechanisms are:

| mechanism | how | cost |
|---|---|---|
| **A — subprocess** *(chosen for the end-to-end property)* | spawn the real binary: `std::process::Command::new(env!("CARGO_BIN_EXE_frogctl")).args(["-o","json","--port",&port,…]).output()`, then `serde_json::from_slice(&out.stdout)`. `CARGO_BIN_EXE_frogctl` is provided free to integration tests by cargo — **no new dependency**, no nightly, no global state, nextest-safe | one process spawn per case (~ms against an already-running `TestServer`); asserts the *real* CLI surface including clap parsing and exit codes |
| **B — sink on `ConnectionContext`** | give `ConnectionContext` an output sink (default `stdout()`), make the seam `ctx.emit(&renderable)`, add `ctx_for_server_capture()` beside the existing helpers | fast and in-process, but it **changes the seam's calling convention at all 26 sites** |

**Mechanism B is blocked by this proposal's own boundary promise.** §Boundary vs 72 states that 75
"does not change [`print_output`'s] signature — it only removes *competitors* to it", which is
what lets 72's new `Renderable` impls in `commands/config.rs` compile in either landing order.
Moving to `ctx.emit` breaks that promise. **Ruling: take mechanism A**, which needs no
renegotiation at all. **If the orchestrator prefers B** — it is the better long-run shape, since
it makes every command's output assertable in-process — then **72 must be consulted at land time
for an explicit amendment**, because its `config.rs` call sites move with the convention. That is
a live option, not a foreclosed one; it is simply not free, and the first draft priced it at zero.

**Constraint 2 — *no* `frogctl` test runs in the default suite. Not the integration tests, and
not the unit tests either.** Three independent exclusions stack, and the first draft named none
of them:

```
Justfile:80-83            `just test frogctl` → "frogctl is excluded from the default suite" ; exit 2
frogctl/Cargo.toml:18-25  [[test]] integration  required-features = ["cli-tests"]  (feature is off by default)
.config/nextest.toml:5    [profile.default]  default-filter = 'not package(frogctl)'
```

The third is the one that bites hardest and that the review did not reach: the nextest default
filter drops **the whole package**, so `frogctl`'s `#[cfg(test)]` lib unit tests — including the
12 that already cover `format_value`/`value_to_json` (`output.rs:88-173`) — are excluded from
`just test` too. Its own comment says so: *"Drop frogctl's lib unit tests from the default `--all`
run."* The only runners are `just frogctl-test` (`Justfile:296-298`, which passes both
`--features cli-tests` and `--ignore-default-filter`) and `just coverage-lcov` (`:104`).

**A headline regression test that no default CI run executes is a document, not a gate.** 73 both
cites this exclusion and proposes fixing it (adding `just frogctl-test` to the CI `test` job, or
lifting the exclusion). **Dependency, stated plainly: every test this proposal proposes — the
subprocess property test *and* the fast `Renderable` unit tests — is a gate only once that
re-inclusion lands.** Until then they run under `just frogctl-test` and `coverage-lcov` and are
still worth writing, but the §7 lint, which runs unconditionally in lefthook pre-commit and CI's
`seam-gates` job, is the *only* enforcement this proposal adds that executes on every commit.
That is an argument for the lint carrying more weight — and for 73's re-inclusion being treated as
a dependency rather than a nicety — not for writing fewer tests.

**What the seam unlocks — one property, applied to every command.** After the change, the whole
Output Mode contract is one testable statement:

- **`--output json` produces exactly one parseable JSON document on stdout.** A table-driven test
  over every subcommand, via mechanism A: start a `TestServer`, spawn
  `env!("CARGO_BIN_EXE_frogctl") -o json --port <p> <subcommand>`, assert
  `serde_json::from_slice::<serde_json::Value>(&out.stdout).is_ok()`. Today this fails for
  `upgrade` ×5, `stat`, `data slot`, `replication topology`, `replication promote`,
  `debug latency --dist`, `benchmark` (empty path), `backup trigger` and `client info -o raw`.
  Afterwards it is a regression test for all of them, and the count is the assertion.
- **stdout carries only the document.** The same subprocess gives `out.stderr` for free, so
  "progress, prompts and warnings go to stderr" becomes `assert!(out.stdout` parses `)` **and**
  `assert!(!out.stderr.is_empty())` on the commands that report progress — the invariant 73's
  adapters depend on and that P2 shows does not exist yet. This is the one assertion mechanism B
  could not make either, since an in-process sink captures only the stdout side.
- **`Renderable` is unit-testable without a server — and this is where most of the coverage
  lives.** Pure `value → String` / `value → serde_json::Value` functions need neither a subprocess
  nor a capture: they are called directly and their return value asserted. `ReplicationStatus`,
  `HealthResult`, `FanOutResult`, `UpgradeStatusResponse`, `RawReply` and `AclCatResult` can be
  constructed literally and their three renderings asserted — no `TestServer`, no connection,
  milliseconds. This is where the `no_color` behavior finally becomes assertable too, and where
  the line-termination contract gets pinned (`RawReply(&Value::Okay).render_table(true) == "OK\n"`
  — the two-line test that catches §step 2's newline regression). These are `#[cfg(test)]` unit
  tests in `frogctl/src/` — but see the correction under Constraint 2: they do **not** run in the
  default suite either.
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

**72 has been revised since this proposal was authored; its line numbers have moved and are
re-derived here.** 72 pins its file set and lists `frogctl/src/output.rs` and
`frogctl/src/info_parser.rs` under "Not in this file set" (now `72:101-102`), then states the
partition directly: "FR4 owns `frogctl/src/output.rs` and `commands/upgrade.rs` … 72 adds two
`Renderable` impls **inside `commands/config.rs`** and does **not** touch `output.rs` or
`print_output`'s signature. FR5 owns `info_parser.rs`. Disjoint." (now `72:696-700`). Confirmed
from this side — **the substance holds; re-derive the line cites at merge.**

**Two files are shared with disjoint hunks, and both need naming:**

- `frogctl/src/connection.rs` — 72 edits `admin_url()` `:139-144` **and `metrics_url()`
  `:146-152`** (its hotfix H1; the second function is new since 72's revision); 75's only interest
  is `build_url` `:22-29`, which is now **PARKED with the rest of the TLS family** (§step 6). No
  shared lines under either reading.
- `frogctl/src/cli.rs` — 72 edits the `--admin-url` help text `:56`; 75's TLS options `:44-54` are
  PARKED, leaving 75 with **no `cli.rs` hunk at all** except `--history`'s deletion at
  `debug.rs`'s clap surface (which is not in `cli.rs`). Partition is now trivially safe.

**One real interaction:** 72's new `Renderable` impls in `commands/config.rs` will be written
against `print_output`'s current signature. This proposal does not change that signature — it
only removes *competitors* to it — so 72's impls compile unchanged in either order. If 75 lands
first, 72's impls should return a real `render_raw` rather than delegating to
`render_table(true)`; that is a one-line difference, and `config.rs:88-95` is already one of the
two impls in the crate that gets it right.

### Boundary vs proposal 73 (FR1, `ops/` wiring) — four explicit handshakes

**73 has been revised since this proposal was authored, and two of its four handshakes changed
shape.** Re-derived against its current text; **re-derive the line cites again at merge.**

1. **The `render_value`/`Renderable` seam** (now `73:855-857`). 73 notes every adapter it adds
   already exits through `print_output`, so it *adds no work here* and removes some. Confirmed:
   73's scan deletion drops a hand-rolled path. Unchanged.
2. **Relocating `render_ascii_graph` / `render_histogram_table` out of `ops/latency.rs`**
   (`:135`, `:188`; now `73:858-862`). Accepted — §Proposed change step 4. **Ordering matters:**
   73 calls both functions *in place* from its new `render_table` bodies. If 75 lands first, 73's
   adapters import from `commands/debug.rs` instead of `ops::latency`; if 73 lands first, the move
   is a two-function relocation plus one import change. **Preferred order: 73 first** — the
   functions should move together with the adapters that call them, and moving them into a
   `render_table` that does not exist yet is worse.
3. **Hotfix H2, `debug slowlog --all` — 73 has since SPLIT it.** 73 now lands a minimal `bail!`
   when `all.is_some()` in its own arm, and explicitly records the real Fan-out as 75's, noting
   that "if the orchestrator's consistency sweep prefers 75 to land first, 73 simply drops its
   bail." Owned here, ruling unchanged: **implement the Fan-out**, not `bail!` — the glossary
   (`CONTEXT.md:26-28`) names slowlog as a Fan-out command, so refusing would retract a documented
   capability rather than deliver it. Reuses `health.rs:284-306`. **Both proposals now agree
   fan-out is the end state**, so the only question is whether 73's interim bail ships at all;
   under **73-first** it does and 75 replaces it, which is the honest interim.
4. **`debug latency --history` — 73 has since downgraded this from a hotfix to "file an issue
   against 75's family".** It reasons that removing the flag regenerates `frogctl-cli.json` and is
   therefore a CLI-surface change belonging to 75. Agreed and unchanged in substance: **ruling is
   delete the flag** (§step 6) — but note the **rationale for that ruling has been rewritten**;
   the first draft's "`stat --interval` and `debug latency graph` already cover it twice" was
   false at HEAD and is withdrawn. See §step 6 for the honesty-only grounds that replace it.

**Shared file, sequenced hunks:** `commands/debug.rs` is edited by 73 (arm bodies at `:405`,
`:408`, `:411`, `:418`, `:421`), by 74 (the zip arm dispatch `:393-395`), and by 75 (the two `..`
destructuring patterns at `:396-402` and `:427-432`, `LatencyResult` `:169-176`, `print_histogram`
`:504-543`, and the raw `println!`s at `:512`, `:527`, `:537`, `:646`, `:768`). 73 already flags
that fixing the destructuring is "a one-line rebase against 73's arm-body edits". Correct — the
patterns and the arm bodies are adjacent but not overlapping. **One addition since the first
draft:** if `impl Renderable for ClientInfo` were written where `ClientInfo` is *declared*
(`debug.rs:198-206`) it would add a fourth author to this file; §step 2 rules it into `client.rs`
instead, so `debug.rs` stays a three-way merge.

**The first draft's "correction to 73" is WITHDRAWN — 73 fixed it in its own revision.** The draft
noted that 73 cited `frogctl/Cargo.toml` as 47 lines with the deps off by one. 73's current text
(`73:141`) reads 48 lines with `indicatif :37, comfy-table :38, dialoguer :39, zip :40` — correct,
and matching this proposal. Nothing to correct.

### Boundary vs proposal 74 (FR3, debug bundle)

**74 has also been revised, and it now cites *this* file by line number — a mutual-citation loop.**
74 pins its frogctl footprint to "exactly one arm and one dependency line" (now `74:714-724`) —
the `debug zip` dispatch at `debug.rs:393-395` and the `zip` dependency at `Cargo.toml:40`
(`74:99-100`) — and explicitly disclaims the role type: "75 … owns `frogctl`'s rendering path and
the client-side role enum in `info_parser.rs`. … [`BundleContext.role`] is a server-side value
read off the `LiveMode` seam and serialized into an archive, not a parsed CLI display type."
Accepted, and symmetric: **this proposal does not touch `debug.rs:393-395`, `upgrade.rs`'s clap
surface beyond its render paths, or any server-side file.**

**The loop is real and must be handled at merge.** 74 currently supports its partition by quoting
*this document's* line numbers back at it — `75:58`, `75:73`, `75:331`, `75:619`. This revision
has moved every one of them. The **substance** of each quoted claim is unchanged and still present
here (`info_parser.rs` disclaimed by 74; `zip :40` is 74's; `debug.rs:393-395` is 74's zip arm),
so nothing 74 asserts becomes false — but **74's numeric cites into 75 are stale as of this
revision and must be re-derived, not trusted.** The same applies in reverse to every `74:NNN` and
`73:NNN` above. Neither proposal should be merged on the strength of the other's line numbers.

The `upgrade.rs` assignment in the task brief is worth stating precisely, since 72 and 74 both
route it here: `commands/upgrade.rs` render paths are **75's** (72 now says so at `:696-700`); 74
does not claim it. Its one `bail!` (`:127`, `upgrade node`) belongs to nobody in this round and
stays.

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

The first draft of this section listed three breaking changes. The real count is **seven**, and
two of them are JSON-contract breaks the draft missed entirely. Enumerated exhaustively, because
a "BREAKING" list that is itself incomplete is worse than none.

**BREAKING — machine-readable contracts (`-o json`):**

1. **`replication status -o json` — field *names*.** `connected_slaves` →
   `connected_replicas`, plus the five `master_*`/`slave_*` keys (`replication.rs:47-57`). Anything
   parsing this output breaks. Deliberate (§step 5).
2. **`replication status -o json` — field *values*.** Missed by the first draft. `render_json` is
   `serde_json::to_value(self)` (`replication.rs:98-100`), so the `role` field serializes whatever
   string `run_status` computed. Today `run_status:125-129` maps `slave` → **`"replica"`** and
   passes `master` through as **`"master"`**. Under FR5 the field becomes a `Role` and serializes
   as **`"Primary"` / `"Replica"`**. So the same key changes value vocabulary *and* casing —
   `"replica"` → `"Replica"`, `"master"` → `"Primary"`. A consumer doing `role == "master"` does
   not error, it silently takes the wrong branch. This is the most dangerous item in the list and
   the one most deserving of a line in the change description.
3. **`health -o json` — role value. A second command with a changed JSON contract.**
   `HealthResult.role` is `Option<String>` (`health.rs:38`); `HealthResult::render_json` is
   `to_value(self)` (`health.rs:76-78`) and `FanOutResult::render_json` is `to_value(&self.nodes)`
   (`health.rs:113-115`), so the same string reaches JSON on both the single-node and fan-out
   paths. The value today is the raw wire token from `INFO replication` (`health.rs:215-218`,
   `:252-254`) — i.e. literally `"master"`/`"slave"`, *unmapped*, unlike `replication status`.
   Under FR5 it becomes `"Primary"`/`"Replica"`. The draft's claim that only one command's JSON
   contract moves was wrong.
4. **`search query -o json` shape.** New finding (§P1 row 6). `run_query:239-247` sends `-o json`
   down `render_value` (raw RESP array) and `-o table`/`-o raw` down `SearchResult`. Unifying on
   `Renderable` replaces the RESP-array JSON with the structured `{total, docs}` shape. This is a
   *fix* — the two modes are supposed to be the same data — but it is still a shape change for any
   existing `-o json` consumer, and it must not be smuggled in as a refactor.

**BREAKING — human-readable output:**

5. **Table output changes** wherever `Master:` becomes `Primary:` (`replication.rs:67`) and the
   `Role:` value changes (`replication.rs:62`, `health.rs:69`, `health.rs:105-108`). 73's new
   integration assertions land around the same time; whichever proposal lands second updates the
   expected strings — a real merge interaction, and another argument for **73 before 75**.
6. **`-o raw` output changes at 36 sites.** The 20 `render_value` call sites lose
   `format_value`'s quoting and `N)` numbering when they move to `RawReply`/typed renderers
   (§step 2), and the 16 existing `render_raw` bodies that currently delegate to
   `render_table(true)` are unaffected only where the ruling says so. §step 2 pins the newline
   behavior with a unit test; it does **not** pin quoting parity, because parity is not the goal —
   the goal is that `-o raw` stops being a synonym for `-o table`. Any script consuming `-o raw`
   today is consuming table text and will break. This is the widest-blast-radius item by site
   count and the least visible.
7. **`upgrade`'s human output is rewritten.** 86 `println!`s become `render_table` bodies. The
   rendering should stay byte-identical where it is already fine; `integration_upgrade.rs` (125
   lines) is the existing guard and should gain output assertions rather than exit-code-only ones.
8. **`debug latency --history` deletion changes the exit code, 0 → 2.** The flag
   (`debug.rs:46`) is accepted today and silently ignored, so `frogctl debug latency --history`
   exits 0. After deletion clap rejects the unknown argument and exits **2**. Any wrapper script
   or CI job passing the flag flips from green to red. That is the correct behavior — the flag
   never did anything — but it is an exit-code break, not a no-op cleanup, and the first draft
   filed it under "cleanup".

**NOT breaking — withdrawn from the draft's list:**

- The draft listed **`--tls` goes from "always fails" to "connects"** as a behavioral risk of this
  proposal. **Withdrawn.** Per §P4, the TLS work is **RECORDED and PARKED** under the standing
  security policy; nothing in this proposal changes `--tls` behavior, so it carries no behavioral
  risk here. The risk analysis is retained below only as part of the *filing*, for whoever
  eventually unparks it.

**Recorded for the parked TLS filing (not proposed here).** If TLS is ever enabled, the enabling
change inherits a hazard that is not obvious from the feature flag: `Cargo.lock:4456-4468` shows
`rustls 0.23.37` already in the graph **with both `aws-lc-rs` and `ring` compiled in** (pulled by
different dependents). rustls 0.23 resolves its `CryptoProvider` from a process-global default,
and with exactly one provider crate present it installs that one implicitly — but with **both**
present there is no implicit default, and the first builder call panics at runtime rather than
failing to compile. The redis client reaches that call unconditionally
(vendored `redis-1.1.0/src/connection.rs:1190`, `rustls::ClientConfig::builder()`). So the
unparking change must install an explicit provider
(`rustls::crypto::<provider>::default_provider().install_default()`) in `frogctl`'s `main` before
any connection is attempted, and must assert it with a test that actually opens a TLS connection.
A feature flag alone produces a binary that compiles, links, and panics on first `--tls` use.

**Non-breaking mechanics:**

- **`just docs-gen` must run.** Deleting `--history` and adding "not supported" text changes the
  clap tree, so `website/src/data/frogctl-cli.json` regenerates and `just docs-gen-check`
  (`Justfile:817`) fails otherwise.
- **Streaming commands need a decision, not a bypass.** `watch`, `subscribe` and `stat` legitimately
  emit continuously. Their allowlist entries in `lint-render-seam` must carry a reason (see the
  `stat` ruling in §7), and the right long-term shape is one `Renderable` per tick (NDJSON under
  `--output json`) rather than a permanent exemption. Proposed as the allowlist's stated intent,
  not implemented here.

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
| 2. `render_value` → `RawReply` newtype; rewrite 20 call sites | S | mechanical, but **not** blind: six per-site rulings in §step 2, plus the newline-parity unit test |
| 3. Collapse the **8** output-mode deciders | **M**, not S | see resizing note below |
| 4. `upgrade.rs`: 86 `println!`s + 1 `eprintln!` → 5 result types + `Renderable` | **M** | the bulk of the work; 2 new types, 3 existing; exit codes preserved |
| 5. Raw-`println!` tail: `data slot`, `replication topology`/`promote`, `benchmark:165`, `backup:148`, `debug:512/527/537/646/768` | S | `stat` is **excluded** — ruled onto the allowlist in §7, not converted |
| 6. Move `render_ascii_graph`/`render_histogram_table` out of `ops/latency.rs` (+4 tests) | XS→S | mechanical move, **but** it also requires the `LatencyResult` field change in §step 4 (`distribution: Option<Vec<HistogramBucket>>`) to fold the histogram into `render_table`. **After 73.** |
| 7. FR5: `Role` enum + 4 call sites + 6 JSON keys + 3 table labels | S | breaking JSON change (names *and* values, on **two** commands), deliberate |
| 8. `slowlog --all` Fan-out (H2) | S | reuses `health.rs:284-306` |
| ~~9. TLS: add `tls-rustls`, reject cert/key/ca paths~~ | — | **STRUCK. PARKED** under the security policy (§P4). Recorded, not implemented, not costed. |
| 10. Drop `comfy-table` / `dialoguer` / `indicatif` | XS | 3 manifest lines |
| 11. `lint-render-seam` + `lint-gates` wiring | S | 3 grep rules, mirrors `clock-seam.py`; rule 1 carries a 5-entry itemized allowlist |
| 12. Tests: json-parseability table over every subcommand; `Renderable` unit tests; `Role::from_info` table | **M** | where the durable value is — **and it must be built as subprocess tests**, see §Testability constraint 1. Also requires unbreaking the nextest exclusion (constraint 2). |
| 13. `just docs-gen` + `frogctl.mdx` Examples fence | XS | JSON regenerates (clap tree changed); sequence **after 73's H1** |

**Step 3 resizing (the first draft undercounted by half).** The draft costed step 3 as "the 4
hand-rolled `matches!` branches, S". §P1 now enumerates **8** output-mode deciders, and they are
not homogeneous: `acl.rs:206-213` deletes with a proof (byte-identical arms), `search.rs:239-247`
and `:347-361` are genuine *shape* changes requiring a ruling on which shape wins,
`health.rs:123-127` is a flag-to-mode promotion, `cluster.rs:389-391` is ruled leave-as-is, and
`data.rs:36-45` is a dispatch match that is not an output-mode decider at all once read closely.
Eight sites with six distinct dispositions is **M**, not S. This is the single largest sizing
correction in this revision.

**Net: still M overall.** Step 3 grew, step 6 grew slightly, step 9 vanished entirely. Not L: no
crate boundary moves, no new abstraction beyond one enum and one newtype, and the seam being
consolidated onto already exists with the right signature. Not S: step 4 is 421 lines of rewrite,
step 12 is a new test surface that first has to be made runnable at all, and step 7 is a
deliberate breaking change with a vocabulary sweep behind it.

### Independently-landable hotfixes

**H4 — `frogctl --tls` cannot connect. LIVE, severe. FILED — NOT A HOTFIX. PARKED.**
*Trace:* `cli.rs:40-42` declares `--tls` → `connection.rs:23` selects the `rediss` scheme →
`redis::Client::open` (`:35`) → redis-1.1.0 `url_to_tcp_connection_info` reaches the
`#[cfg(not(any(feature = "tls-native-tls", feature = "tls-rustls")))]` arm and fails with
`"can't connect with TLS, the feature is not enabled"` → surfaced to the operator as
`invalid connection URL: rediss://<host>:<port>`. `frogctl/Cargo.toml:34` enables only
`tokio-comp`; `redis` is declared exactly once workspace-wide, so no feature unification applies.
**Every Data Plane command fails under `--tls`.**

> **RECORDED, PARKED.** Under the standing project policy, security findings are filed, not
> fixed, in this round. H4 is retained here as the **filing**; it is **not** proposed for
> implementation and is **not** costed in the effort table (step 9 is struck). Two corrections
> the filing needs so it is actionable whenever it is unparked:
>
> 1. **The feature name in the first draft was wrong.** The draft said "add `tls-rustls`". The
>    async path this binary uses (`tokio-comp`) needs **`tokio-rustls-comp`**; `tls-rustls` alone
>    enables the sync TLS path and leaves the async connector still unbuilt.
> 2. **A feature flag alone yields a binary that panics.** See the crypto-provider hazard recorded
>    under §Risks → *Behavioral risk* — both `aws-lc-rs` and `ring` are already in the graph, so an
>    explicit `CryptoProvider` install is mandatory, not optional.
>
> Whoever unparks this owns both, plus a TLS-enabled `TestServer` case. Nothing here is a
> one-line change, which is itself the reason it does not belong on a hotfix list.

**H5 — `frogctl debug latency --dist --output json` emits invalid JSON. LIVE, XS.**
*Trace:* `debug.rs:495` `print_output` writes the JSON document → `:497-499` calls
`print_histogram` unconditionally → `:527-541` `println!`s ASCII bars to stdout. Piping to `jq`
fails. `:512` is the same bug on the min==max path. *Fix (hotfix, XS):* gate the call on
`matches!(ctx.global().output, OutputMode::Table | OutputMode::Raw)` — one line, no type changes.
*Fix (proper, in step 4/6):* fold the histogram into `LatencyResult::render_table`, which requires
adding `distribution: Option<Vec<HistogramBucket>>` to `LatencyResult` (`debug.rs:169-176`) — a
type change, therefore **not** hotfixable. The two are alternatives, not stages: take the one-line
gate now and let step 4 replace it. *Owner:* 75. Touches `debug.rs:497-499`, inside neither 73's
arm bodies (`:405-421`) nor 74's arm (`:394`).

**H6 — `website/.../reference/frogctl.mdx:55-56` publishes a TLS example that cannot work. XS.**
*Trace:* the Examples block reads `# Connect with TLS` / `frogctl --tls --tls-ca ca.crt health` →
fails per H4, and `--tls-ca` is unread regardless (`connection.rs` reads only `global.tls`).
**Ruled: delete both lines outright.** The first draft offered "remove the example, or restore it
once H4 lands" — the second half is **withdrawn**, because H4 is parked and a documented example
must not promise `--tls-ca` support that no one is scheduled to build. Deleting is the only option
consistent with the parked ruling.

*Sequencing with 73's H1 — the collision is real but narrower than "same edit".* 73's H1
(`73:860-871`) fixes `frogctl backup snapshot` at `frogctl.mdx:50`; H6 deletes `:55-56`. Both live
in the **same bash fence** (`:30-60`) but touch **disjoint lines**, so there is no content
conflict — only a **diff-context** collision: a 3-line-context hunk at `:50` spans `47-53` and one
at `:55-56` spans `52-59`, overlapping at `52-53`. Landed independently they will conflict on
rebase despite editing different text. **Ruled: land 73's H1 first** (one word, no dependency),
then H6 rebases trivially. This matches 73's own sequencing note, so the two proposals agree.

**H2 — `frogctl debug slowlog --all` silently ignores its addresses. LIVE, S.** Ceded by 73;
trace re-verified at this SHA (`debug.rs:88-90` declared → `:427-432` destructures
`{ count, analyze, reset, .. }` → `run_slowlog` queries one node). **Ruled: implement the
Fan-out** over `health.rs:284-306`, not `bail!` — `CONTEXT.md:26-28` names slowlog as a Fan-out
command. *Note:* 73 has since **split** its H2 (minimal `bail!` now, fan-out ceded to 75). That
does not change this ruling — it changes the sequencing. If 73's minimal `bail!` lands first, 75
replaces it with the fan-out rather than adding to it, and the `bail!` should be treated as a
placeholder with a one-line lifetime, not as a shipped behavior worth a test.

**H3 — `frogctl debug latency --history` silently ignores the flag. LIVE, XS.** Ceded by 73 as
"75's call"; trace re-verified (`debug.rs:44-46` declared → `:396-402` dropped in `..` →
`run_latency:444-502` never sees it). **Ruled: delete the flag.**

*Rationale, replaced.* The first draft justified deletion by claiming `stat --interval` and
`debug latency graph` "already provide periodic sampling and the history view respectively". That
rationale is **withdrawn**: neither is a substitute for a server-side latency *history*, and
arguing feature-equivalence for a feature that was never built is dishonest. The honest and
sufficient rationale is narrower: **the flag has never done anything.** It is declared, dropped
into `..`, and never read. Deleting it makes the CLI's surface match the CLI's behavior. If
server-side latency history is wanted later it is a real feature with a real design, and it should
arrive as a flag that works — not be squatted by a flag that lies. Note the exit-code break
(0 → 2) recorded in §Risks. Requires `just docs-gen`, so it is not purely local; land it with
step 13 rather than alone.

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
- **TLS (`--tls`, `--tls-cert`, `--tls-key`, `--tls-ca`).** Not "deferred for cost" —
  **parked by policy** (§P4, H4). Filed with the two corrections needed to act on it later.
  No part of it is proposed for implementation in this proposal.
- **`cluster slots --json`.** The declared-and-unread `json` flag on a command whose arm
  unconditionally bails (`cluster.rs:389-391`). Ruled leave-as-is, aligned with the `hide = true`
  ruling: do not decorate a command that does not run.

## Review ledger

Adversarial review taken at `2e81506b`, verdict **AMEND**. Every point was re-verified against the
tree at the SHA in the header before being applied. This section records the disposition of each,
so a later reader can tell what the review caught, what it got wrong, and what neither party had
found yet.

### Blocking items — applied

| # | item | disposition |
|---|---|---|
| B1 | Undercount: "six places" / "four `matches!`" | **Applied.** True counts are **eight** deciders and **six** hand-rolled `matches!`. §Summary, §P1 (8-row table), §step 3, §deletion test all corrected. |
| B2 | `debug.rs` / `client.rs` / `acl.rs` / `search.rs` / `cluster.rs` / `benchmark.rs` line cites wrong | **Applied.** Whole §Files table re-derived at HEAD. |
| B3 | §P2 `println!` counts wrong | **Applied.** upgrade 87 (86 `println!` + 1 `eprint`), client 10, debug 7, stat 6, data 4, output 3, replication 3, with the eprint-exclusion arithmetic shown. |
| B4 | §step 2 `render_value` replacement under-specified (newline semantics unhandled) | **Applied.** Full rewrite: `RawReply<'a>` newtype, the `print!`-vs-`println!` evidence (`output.rs:15` vs `:23`/`:26`), an A-vs-B options table, **Ruling: A**, a pinning unit test, and six per-site rulings. |
| B5 | §Testability proposes capture that cannot be implemented | **Applied.** New *Constraint 1*: in-process capture needs unstable `set_output_capture` or racy fd redirection. **Ruled: subprocess via `env!("CARGO_BIN_EXE_frogctl")`** — zero new deps, nextest-safe. Mechanism B (sink on `ConnectionContext`) priced explicitly as requiring a 72 amendment. |
| B6 | Test-surface claims overstated | **Applied and extended** — see *new findings* below. |
| B7 | BREAKING list incomplete | **Applied.** Rewritten from 3 items to **8**, split into JSON-contract / human-output / not-breaking. Adds role *value* renames on **two** commands, the `-o raw` 36-site change, and the `--history` exit-code break. |
| B8 | Lint rules 1–3 unimplementable / vacuous as written | **Applied.** Rule 1 widened to all of `frogctl/src/` minus `output.rs` with a 5-entry itemized allowlist; rule 2 reduced to exactly one exception; rule 3 rewritten around exact-quoted-literal matching, with the vacuous `info_parser.rs` exemption corrected. |
| B9 | Sibling cites (72/73/74) stale | **Applied.** All three boundary subsections re-derived; the stale "correction to 73" **withdrawn**; the 74↔75 mutual-citation loop documented, with a header warning that every `72:`/`73:`/`74:` cite must be re-derived at merge time. |
| B10 | `cluster slots --json` ruling contradicts the `hide = true` ruling | **Facts confirmed; framing partly refuted** — see below. |

### Refuted or narrowed, with evidence

- **B10's framing.** The review said the `cluster slots --json` ruling *contradicts* the
  `hide = true` ruling. The underlying facts are confirmed (`cluster.rs:389-391` bails
  unconditionally; `json` is never read — a dead flag on a dead command), but the resolution is
  not to reverse either ruling. Both rulings share one principle: **do not decorate a command that
  does not run.** Resolved by *aligning* the slots ruling with the `hide` ruling and **narrowing**
  §P1 row 8 to `health.rs:123-127` only, rather than deleting the row. No contradiction remains.
- **H4 as a "hotfix".** The review carried H4 forward on the hotfix list. Refused on policy
  grounds, not evidence grounds: security findings are **filed, not fixed**, this round. H4, §P4,
  §step 6's TLS ruling, and effort step 9 are all converted to RECORDED/PARKED. Step 9 is struck
  from the effort table entirely.
- **N4's internal contradiction** (step 5 converts `stat`; rule 1 would then forbid `stat`'s
  own writes). Resolved by **ruling `stat` onto the allowlist** with a written justification —
  `stat` is a TTY-interactive streaming dashboard using cursor control (`stat.rs:112-118`), not a
  one-shot renderer — and by removing `stat` from step 5. The review's implied fix (convert it)
  was the wrong half to keep.

### Withdrawn first-draft claims

Recorded because a proposal that quietly deletes its own wrong claims teaches nothing:

- **"frogctl `#[cfg(test)]` unit tests run in the default suite."** False — see new findings.
- **H3's rationale** ("`stat --interval` and `debug latency graph` already provide this"). Replaced
  with the honest and narrower one: the flag has never done anything.
- **"`--tls` goes from always-fails to connects" as a behavioral risk of this proposal.** Withdrawn
  — with TLS parked, this proposal changes no TLS behavior.
- **"add `tls-rustls`."** Wrong feature for the `tokio-comp` async path; corrected to
  `tokio-rustls-comp` inside the parked filing.
- **H6's "or restore it once H4 lands."** Withdrawn — the docs must not promise a parked feature.
- **The correction offered to 73** in the first draft's boundary section. Withdrawn as stale.

### New findings (reached by neither the proposal's first draft nor the review)

1. **`frogctl` tests do not run in the default test suite at all.** Three stacked exclusions:
   `Justfile:80-83`, `frogctl/Cargo.toml:18-25` (`required-features`), and
   `.config/nextest.toml:5` (`default-filter = 'not package(frogctl)'`) — the last of which is
   *package*-level, so it excludes the `#[cfg(test)]` unit tests too, not merely the integration
   targets. Recorded as §Testability *Constraint 2*. This materially changes step 12: the tests
   this proposal's value rests on must first be made runnable, or they are written and never run.
2. **`search query -o json` and `-o table` return different JSON shapes**
   (`search.rs:239-247`) — `-o json` yields a raw RESP array, `-o table`/`-o raw` yield the
   structured `SearchResult`. A live violation of the Output Mode orthogonality contract
   (`frogctl/CONTEXT.md:59`), added as §P1 row 6 and as a BREAKING item.
3. **`acl.rs:206-213` is a byte-identical no-op branch** — both arms of the `matches!` build the
   same value and call `print_output` with the same arguments. It deletes with a proof.
4. **`health.rs:200-203`'s `unwrap_or("master")` is *always* taken**, because `:189` fetches only
   `["server","memory"]` and `role` lives in the `replication` section. A hardcoded default
   masquerading as a fallback.
5. **The crypto-provider panic hazard** behind the parked TLS work (`Cargo.lock:4456-4468`:
   rustls 0.23.37 with **both** `aws-lc-rs` and `ring`; redis reaches
   `rustls::ClientConfig::builder()` unconditionally at vendored `connection.rs:1190`). Recorded
   in the filing so that unparking does not ship a binary that compiles and then panics.
6. **74 cites this document by line number** (`75:58`, `75:73`, `75:331`, `75:619`) and this
   revision has moved all four. The substance of each quoted claim survives unchanged, but 74's
   numeric cites into 75 are **stale as of this revision** and must be re-derived, not trusted.
