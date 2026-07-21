# Spec: architecture/execution.md
Status: update
Audiences: A2, A3, A5

Goal: A reader walks away understanding how FrogDB turns a parsed command into a
result and *why* the pipeline is ordered the way it is: every command is a type
implementing the `Command` trait (name, arity, flags, key extraction, execution
strategy, WAL strategy, execute), commands are declared via the `define_command!`
family of macros and registered in a `CommandRegistry`, the registry stores each
command as a `CommandEntry` that is either `Full` (has an `execute()`) or
`MetadataOnly` (arity/flags/keys for routing and the compatibility matrix, no
local execution), and validation runs in a canonical order (parse → lookup →
arity → auth → ACL → execute) so malformed commands are rejected cheaply before
auth. The reader also learns the flag set, the execution strategies, and what
`CommandContext` gives a command.

Not in scope: The scatter-gather/VLL execution mechanics (concurrency.md,
vll.md, request-flows.md); replication internals (replication.md); the
persistence/WAL path (persistence.md). The command *matrix* itself is generated
and lives in Compatibility & Correctness — this page explains the registry that
feeds it, and links to work item S1.

Sources of truth (author must read and reconcile every claim):
- `frogdb-server/crates/core/src/command.rs` — `pub trait Command: Send + Sync`
  (arity, flags, execution_strategy, wal_strategy, keyspace_event_type, keys,
  keys_with_flags) and the `CommandMetadata` trait used by MetadataOnly entries.
- `frogdb-server/crates/core/src/command_macro.rs` — `define_command!` (line
  ~109) and the sibling macros `define_readonly_command!`,
  `define_write_command!`, `define_metadata_command!`, plus the `impl_keys_*`
  helper macros. This is how commands are actually declared.
- `frogdb-server/crates/core/src/registry.rs` — `CommandRegistry` and the
  `CommandEntry` enum (`Full(Arc<dyn Command>)` / `MetadataOnly(Arc<dyn
  CommandMetadata>)`) with delegating accessors and `is_full()`/`as_full()`.
- `frogdb-server/crates/core/src/command.rs` (or wherever defined) — the
  `Arity`, `CommandFlags`, and `ExecutionStrategy` types; verify the flag names,
  arity modes, and strategy variants against source.
- Server registration: the `register_all()` / `register.rs` path referenced by
  work item S1 (locate under `frogdb-server/crates/server/` or `commands/`) —
  where entries are actually inserted.
- `CommandContext` definition — verify the struct fields against source before
  reproducing.

Existing content: `website/src/content/docs/architecture/execution.md`. Good
tables (flags, strategies, arity, validation order, subcommands); the "Command
Registry" code sample is fabricated and must be replaced.

Structure (keep existing H2s, corrected):
- ## Command Flow — the numbered walkthrough; verify step 5 routing wording
  against the real CRC16 routing (consistency with concurrency.md).
- ## Command Trait — describe the real trait methods (name, arity, flags, keys,
  keys_with_flags, execution_strategy, wal_strategy, keyspace_event_type,
  execute). Mention the `CommandMetadata` sibling trait.
- ## CommandError — keep; link architecture.md for variants.
- ## Arity — Fixed/AtLeast/Range; verify variant names against the `Arity` type.
- ## Command Flags — verify every flag name in the table exists in
  `CommandFlags`; drop or correct any that don't.
- ## Execution Strategy — verify against the real `ExecutionStrategy` enum
  (Standard, ConnectionLevel, Blocking, ScatterGather, RaftConsensus,
  AsyncExternal, ServerWide — confirm names).
- ## Flag Combination Effects / Mutual Exclusivity — keep if consistent with
  source semantics.
- ## Arity Validation Details — validation order, subcommand handling,
  subcommand table (verify subcommand lists against actual command impls).
- ## Command Registry — **rewrite** (see Drift guards) to describe
  `define_command!` + `CommandRegistry` + `CommandEntry::Full/MetadataOnly`,
  not a `lazy_static! COMMANDS` map. Explain why MetadataOnly exists (routing +
  compat matrix without a local executor).
- ## Type Handling Rules — keep.
- ## CommandContext Definition — reproduce the real struct fields; verify.
- ## Replication Integration — keep; cross-link replication.md.

Generated data: **S1 (`commands-gen`)** — the `CommandRegistry` (`register_all()`
+ server `register.rs`) is the single source of truth for the generated
`commands.json` that drives the Command matrix page. This page must state that
relationship explicitly and link to the matrix, so readers understand the
registry is authoritative and the matrix is derived, not hand-maintained.

Drift guards:
- **Command Registry section is fabricated and must be replaced.** The page
  currently shows `lazy_static! { static ref COMMANDS: HashMap<&str, Box<dyn
  Command>> }` and a `dispatch()` using `.to_uppercase()`. No `lazy_static`
  exists in the crate. The real design is: commands declared with the
  `define_command!` macro family, stored in a `CommandRegistry` as `CommandEntry`
  values (`Full` vs `MetadataOnly`). Rewrite against `registry.rs` and
  `command_macro.rs`. Do not invent code samples — quote or paraphrase real
  types.
- **Trait/flag/strategy/arity names must match source.** Verify `CommandFlags`
  variants, `ExecutionStrategy` variants, and `Arity` modes against the actual
  enums; remove any that don't exist and add any that are missing rather than
  preserving the current tables on faith.
- **CommandContext fields.** Reproduce the struct only after confirming field
  names/types against source (it changes as features land).
- **Routing consistency.** Any routing arithmetic shown must match the real
  CRC16 routing used elsewhere (concurrency.md, request-flows.md).
- **S1 link.** State that the registry is the generation source for the command
  matrix (work item S1); keep this page's flag/strategy descriptions consistent
  with whatever `commands.json` emits.
- **No unbacked numbers.** No command counts in prose (PLAN §6) — the matrix
  page owns counts, generated from the registry.
- **S7 code-path check.** Any `crates/...` path named must exist; include this
  page in the S7 scan.
