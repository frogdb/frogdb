# Proposal 32 — Carry the typed `ClusterError` across the Raft apply boundary instead of collapsing it to `ClusterResponse::Error(String)`

## Summary

`RaftStateMachine::apply` catches the rich `ClusterError` that `apply_command` returns and
immediately stringifies it: `apply_command(cmd).unwrap_or_else(|e| ClusterResponse::Error(e.to_string()))`
(`cluster/src/state.rs:347-350`). `ClusterResponse` — the `RaftTypeConfig::R` value, the *only*
value that crosses back from a Raft apply — models the failure as `Error(String)`
(`types.rs:360-368`), so the twelve-variant `ClusterError` enum (`types.rs:404-453` —
`NodeNotFound`, `SlotAlreadyAssigned`, `MigrationInProgress`, …) is flattened to a display string
the instant a metadata mutation is committed. Every consumer past that boundary sees only a
display string (`connection/cluster.rs:74`, `slot_migration/mod.rs:133`, `network.rs:684`,
`failure_detector.rs:297/432`) — the twelve-noun failure vocabulary the module worked to produce is
gone before the first consumer reads `resp.data`. This proposal widens `R` so the wire type carries
structure: `ClusterResponse::Error(ClusterError)` plus a typed `Epoch(u64)` success variant to
replace the `Value(String)` epoch encoding that callers currently ignore. The seam between the
mutation module and its Raft-boundary consumers stops being a lossy `to_string()` cast.

**This is a pure structural typed-cleanup — vocabulary preservation, not a behavioral fix.** An
earlier draft claimed the change unlocked an auto-failover retry policy (branch `MigrationInProgress`
vs. `NodeNotFound` at `failure_detector.rs:432`). That claim was **false and has been struck**: the
two failover-loop sites issue `Failover{force:true}` and `MarkNodeFailed`, and *every* error those
commands can return is permanent (`NodeNotFound`/`InvalidOperation`); `MigrationInProgress` is
produced by exactly one command they never issue (`BeginSlotMigration`). No consumer is demonstrated
to make a runtime decision on the variant today. The honest case for this change is structural — a
lossy `to_string()` adapter and a dead `Value(String)` payload — with correspondingly modest
cost/benefit, not a bug fix.

The one correction to the candidate: `ClusterError` derives `Clone` but **not** `Serialize`
(`types.rs:404` is `#[derive(Debug, Error, Clone)]`). Since `R` must be `Serialize + Deserialize`
under openraft's `serde` feature, the design must *add* those derives — trivial (every variant
holds only `NodeId`/`u16`/`String`), but not free as the candidate implied.

## Problem (concrete verified evidence)

### The typed error dies at exactly one line

`apply_command` (`cluster/src/commands.rs:9`) is the single validated mutation path; its signature
is `fn apply_command(&self, cmd: ClusterCommand) -> Result<ClusterResponse, ClusterError>`. It
returns precise failures — e.g. `Err(ClusterError::NodeNotFound(node_id))` (`commands.rs:111`),
`Err(ClusterError::InvalidOperation(...))` (`commands.rs:123`), `Err(ClusterError::MigrationInProgress(slot))`.
Its unit tests already assert on the variant: `assert!(matches!(result, Err(ClusterError::NodeNotFound(999))))`
(`state.rs:569`), `SlotNotAssigned` (`state.rs:1053`), `InvalidOperation` (`state.rs:919/941/972`),
and eight more. The typed error is real and pinned — *up to the boundary*.

The boundary is `state.rs:347-350`:

```rust
let result = self.state.apply_command(cmd).unwrap_or_else(|e| {
    tracing::warn!(error = %e, "Failed to apply cluster command");
    ClusterResponse::Error(e.to_string())   // <-- 12 variants -> 1 String, here
});
```

`ClusterResponse` (`types.rs:360-368`):

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterResponse { Ok, Value(String), Error(String) }
```

`R = ClusterResponse` is the sole state-machine return type (`types.rs:26`,
`impl RaftTypeConfig for TypeConfig`). Once `apply` runs, nothing downstream can recover the
variant — it is gone before the first consumer sees `resp.data`.

### Every downstream consumer is reduced to a string re-wrap

The full set of `ClusterResponse::Error` inspection sites (workspace grep, `crates/server` +
`crates/cluster`):

| Site | Code today | What structure it loses |
| --- | --- | --- |
| `server/src/connection/cluster.rs:74` | `if let Error(msg) = &resp.data { Response::error(format!("ERR {msg}")) }` | forwards the error verbatim to the client as `-ERR` (Redis-parity: CLUSTER errors go to the client with no server-side branching); the variant is available for diagnostics/logging but not needed for control flow |
| `server/src/connection/cluster.rs:203` (CLUSTER RESET) | same shape | same |
| `server/src/slot_migration/mod.rs:133-134` | `if let Error(msg) = &resp.data { return Response::error(format!("ERR {msg}")) }` | same — verbatim `-ERR` to the client |
| `cluster/src/network.rs:684-685` (`ForwardedWrite`) | `if let Error(msg) = resp.data { ForwardedWrite(Err(msg)) }` — flattens to `Result<(), String>` (`network.rs:120`) | the forwarded-write RPC contract is *already* string-only by design (out of scope), so it re-loses any structure regardless |
| `server/src/failure_detector.rs:432` (auto-failover) | `if let Error(msg) = &resp.data { error!(...); return; }` — gives up on any error | **can only ever receive a permanent error** (see below), so "give up on any error" is *correct* behavior here; the variant would improve only the log line, not the decision |
| `server/src/failure_detector.rs:297` (`MarkNodeFailed`) | same give-up shape | same — can only receive `NodeNotFound`, which is permanent |

**Correction (was the earlier draft's headline, now struck).** An earlier draft called
`failure_detector.rs:432` the "sharpest case" — a coin-flip policy that could retry
`MigrationInProgress` vs. abort on `NodeNotFound` once the variant survived. That is **refuted by the
code**. The site issues `ClusterCommand::Failover{old, new, force:true}` (`failure_detector.rs:422-426`);
the `Failover` apply arm returns only `InvalidOperation` (source==target, `commands.rs:150`) or
`NodeNotFound(new)` (`commands.rs:155`), and with `force:true` it *actively clears* in-progress
migrations (`commands.rs:189`) rather than inspecting them — it can never return `MigrationInProgress`.
The sibling site (`failure_detector.rs:297`) issues `MarkNodeFailed`, whose only error is
`NodeNotFound` (`commands.rs:232`). `MigrationInProgress` is produced by exactly one command,
`BeginSlotMigration`, and only when a *different* migration already holds the slot (`commands.rs:264`) —
a command neither failover site issues. So every error these two sites can receive is permanent; the
existing code comment ("retrying the same command cannot succeed") is accurate, and giving up is
correct. The typed variant here buys a better *log line*, nothing more. (Note: this file lives in the
**server** crate — `frogdb-server/crates/server/src/failure_detector.rs` — not `cluster`, correcting
the candidate's unqualified path.)

### The `Value(String)` epoch is computed, encoded as text, then discarded

`IncrementEpoch` returns `Ok(ClusterResponse::Value(inner.config_epoch.to_string()))`
(`commands.rs:139`); the graceful-`Failover` arm returns `Ok(ClusterResponse::Value(epoch.to_string()))`
(`commands.rs:225`). The candidate claims "callers re-parse" this — **they do not**. The Ok path in
`connection/cluster.rs:71-95` checks only for `Error` and then returns `Response::ok()`
(`cluster.rs:94`), dropping `resp.data` entirely; the `RaftNeeded { op: IncrementEpoch }` route
(`commands/cluster/admin.rs:465`) parses its *input* epoch argument, never the response. So the
Config Epoch is serialized to a decimal string on the apply side and thrown away on the receive
side — a dead payload, which is a *stronger* argument for a typed variant than "re-parsed" would
be. A typed `Epoch(u64)` variant makes the value both structurally honest and actually usable
(e.g. a real `CLUSTER BUMPEPOCH` reply, which today can only answer `+OK`).

### Test evidence: the loss is observable

`apply_command`-level tests see the rich error (they call the fn directly). The single test that
drives the *full* `apply` path can only assert the flattened form:
`assert!(matches!(responses[0], ClusterResponse::Error(_)))` (`state.rs:814`) — it cannot say
*which* error, because after `apply` there is no variant left to name. That test is the deletion-test
witness: the entire `ClusterError` taxonomy is invisible to anything exercising the real Raft
boundary.

### `R` never crosses the wire — so this is a pure in-process seam

Verified against openraft 0.9 (`Cargo.toml:159`, features `["serde", "storage-v2"]`): only
`D = ClusterCommand` is persisted to the Raft log/snapshot; `R` is delivered in-process by
`OneshotResponder` (`types.rs:31`) to the local `client_write` caller on the leader. The
cross-node forward path (`network.rs` `ForwardedWrite`) re-runs `client_write` on the leader and
returns its own `Result<(), String>` RPC type (`network.rs:120`) — `R` itself is not serialized
onto the bus. **Consequence:** widening `R` has *no* on-disk snapshot or rolling-upgrade
compatibility concern (and FrogDB is pre-production regardless — breaking changes are fine). The
only reason `R` must stay `Serialize + Deserialize` is openraft's trait bound under the `serde`
feature, not any wire format we control.

## Why it is shallow (architecture vocabulary)

**The Raft boundary is a lossy adapter, not a seam.** `apply_command` is a **deep module**: a
one-line interface (`ClusterCommand -> Result<ClusterResponse, ClusterError>`) over substantial
validation, returning a precise algebraic error. `apply` sits directly above it and *narrows* that
interface with a `to_string()` — the adapter throws away exactly the structure the module worked to
produce. A good seam preserves the callee's vocabulary; this one collapses twelve nouns into one
string the moment they cross.

**Vocabulary loss on the consumer side (not a leverage loss).** Because the wire type is
`Error(String)`, all six consumers above are forced into the identical `format!("ERR {msg}")` shape.
Honesty check: no consumer is demonstrated to *need* a typed decision today — the three
client-facing sites forward the error verbatim as `-ERR` (Redis parity), `network.rs` is string-only
by contract, and the two failover sites can only receive permanent errors (above). So this is not a
suppressed *capability*; it is a suppressed *vocabulary*. The value is that the callee's precise
noun set survives the boundary — available for diagnostics, richer logging, and any *future* caller
that wants to branch — instead of being irrecoverably flattened at the seam. That is a real but
modest structural gain, and it should be pitched as exactly that, not as unlocking a failover retry
policy.

**Poor locality of the success contract, too.** `Value(String)` is a second stringly-typed hole in
the same enum: the Config Epoch — a first-class glossary concept (`CONTEXT.md`: *Config Epoch*, the
monotonic topology counter owned by the **Raft Metadata Plane**) — is transported as untyped text
and then dropped. One enum, two `String` payloads, both lossy, both avoidable.

**Deletion test.** Delete the `to_string()` and hand `ClusterError` through: no consumer *breaks*
(they keep matching a catch-all and emit the identical `-ERR` bytes). None *gains* new behavior
today either — the honest reading is that the boundary was destroying vocabulary the consumers did
not yet exploit, so removing the flattening is a structural cleanup that preserves the option value
rather than a fix that changes any runtime decision.

## Proposed design (Rust interface sketch)

All changes are confined to the `cluster` crate's public types plus their `server`-crate consumers.
Signatures/types only:

```rust
// cluster/src/types.rs — ClusterError gains the derives R requires.
#[derive(Debug, Error, Clone, Serialize, Deserialize)]  // + Serialize, Deserialize (were absent)
pub enum ClusterError { /* unchanged 12 variants */ }

// cluster/src/types.rs — the wire type carries structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterResponse {
    Ok,
    Epoch(ConfigEpoch),        // replaces Value(String) for IncrementEpoch / Failover
    Error(ClusterError),       // replaces Error(String)
}
```

`R = ClusterResponse` is unchanged as an alias; only its shape widens. `apply` stops stringifying:

```rust
// cluster/src/state.rs — the boundary forwards the typed error instead of flattening it.
let result = self.state.apply_command(cmd).unwrap_or_else(|e| {
    tracing::warn!(error = %e, "Failed to apply cluster command");
    ClusterResponse::Error(e)          // was: ClusterResponse::Error(e.to_string())
});
```

Consumers keep their existing behavior; a convenience keeps the common path terse:

```rust
// cluster/src/types.rs — a display shim so callers that only want "ERR ..." stay one line,
// while a future caller that wants to branch can match the variant directly.
impl ClusterResponse {
    pub fn as_error(&self) -> Option<&ClusterError>;   // Some(e) iff Error(e)
}

// server/src/failure_detector.rs — behavior is UNCHANGED; the variant only enriches the log.
// (Every error reachable here is permanent — see the Problem section — so there is no retry
// branch to add. Prior draft's `match` on MigrationInProgress would be dead code and is dropped.)
if let Some(e) = resp.data.as_error() {
    tracing::error!(error = %e, "Auto-failover rejected by cluster state machine");
    return;
}
```

Two orthogonal seams are cleaned in one enum: `Error(String) -> Error(ClusterError)` (the primary
change) and `Value(String) -> Epoch(ConfigEpoch)` (removing the dead text payload). Keep them in
one proposal because they are the same defect — a stringly-typed `R` — but the migration steps below
allow landing them independently if desired.

**Scope guard (respecting crate direction):** `ClusterError`, `ClusterResponse`, and `ConfigEpoch`
already live in `cluster` and are re-exported through `frogdb_core::cluster`; consumers in `server`
already import them. No new dependency edge is created; `cluster` gains no dependency on anything.
The `ForwardedWrite` RPC (`network.rs:120`) stays `Result<(), String>` — it is a separate
cross-node contract and out of scope here (a future proposal could type it, but it does not gate
this one).

## Migration plan (ordered)

1. **Add `Serialize, Deserialize` to `ClusterError`** (`types.rs:404`). Compile the `cluster` crate;
   confirm openraft's `R: AppDataResponse` bound is satisfied (it needs `serde` under the enabled
   feature — this derive is what makes the *next* step legal).
2. **Change `ClusterResponse::Error(String)` → `Error(ClusterError)`** (`types.rs:367`) and update
   `apply`'s `unwrap_or_else` to pass `e` through (`state.rs:349`). Add `ClusterResponse::as_error`.
3. **Compiler-drive the consumers.** Each `if let ClusterResponse::Error(msg) = ...` site
   (`connection/cluster.rs:74,203`, `slot_migration/mod.rs:133`, `network.rs:684`,
   `failure_detector.rs:297,432`) now binds a `ClusterError`. The change is uniform:
   `format!("ERR {e}")` (its `Display` is already the `#[error(...)]` string, so RESP output is
   byte-identical) at every site. **No behavioral branch is added** — the two failover sites can only
   ever receive permanent errors (see Problem), so their "give up" behavior is already correct and
   stays as-is; the typed value only enriches their `error!` log line. There is no
   `MigrationInProgress` branch to write here (it would be dead code).
4. **Replace `Value(String)` epoch with `Epoch(ConfigEpoch)`** (`commands.rs:139,225`;
   `types.rs`). Update the `Ok`-path handler in `connection/cluster.rs` to surface the epoch where a
   real reply is wanted (or continue to `Response::ok()` — but now the value is *available*, not
   thrown away). Grep confirms no other reader of `Value` exists in `server`, so this arm has a
   single owner.
5. **Update tests** (see Test plan). Run `just test frogdb-server` and `just test cluster`; the
   whole-suite/turmoil runs go to a Blacksmith testbox per the remote-execution policy.

Steps 1-3 are the core (typed error); steps 4 can land separately. Each step is
compiler-guided — every unconverted site is a type error until fixed.

## Test plan

- **New: typed error survives the full apply path.** Extend the `apply`-level test at
  `state.rs:814` (currently `matches!(responses[0], ClusterResponse::Error(_))`) into
  `matches!(responses[0], ClusterResponse::Error(ClusterError::NodeNotFound(_)))`, driving a real
  `openraft::Entry` through `sm.apply(...)`. This is the deletion-test witness inverted: it now
  *can* name the variant, proving the boundary preserves structure.
- **New: `as_error()` accessor.** Unit-test that `ClusterResponse::Error(e).as_error()` yields
  `Some(&e)` and that `Ok`/`Epoch` yield `None`. (The earlier draft proposed a
  "`MigrationInProgress` is retryable at the failover boundary" test — **struck**: neither `Failover`
  nor `MarkNodeFailed` can ever return `MigrationInProgress`, so that state is unreachable through the
  failover code path and the test would pin a state that cannot occur. No behavioral branch exists to
  guard.)
- **New: `Epoch` variant round-trips.** `apply_command(IncrementEpoch)` returns
  `Ok(ClusterResponse::Epoch(n))` with the pre-increment/post-increment value the arm intends;
  a `serde_json` round-trip of `ClusterResponse::Error(ClusterError::SlotAlreadyAssigned(1, 2))`
  and of `Epoch(7)` (guards the new derives + openraft `R` serde bound).
- **Unchanged / must stay green:** the twelve `apply_command`-level `matches!(_, Err(ClusterError::...))`
  assertions (`state.rs:569,919,941,972,1053,1076,1103,1265,1276,...`) — they already see the typed
  error and are unaffected; they prove the *source* of the type is intact.
- **RESP parity:** assert the client-visible `-ERR ...` bytes for a representative failure
  (e.g. via the `slot_migration` path) are identical before/after, since `Display` == the prior
  `to_string()`.

## Risks & alternatives

- **Serde bound is the real gate, not a given.** Adding `Serialize/Deserialize` to `ClusterError`
  is mandatory (openraft's `R` bound) and safe (all variants are plain data), but it is a change the
  candidate assumed already existed. If any future `ClusterError` variant wraps a non-serde type
  (e.g. a `#[from] io::Error`), this derive would break — none do today; keep the enum
  serde-clean as a soft invariant.
- **No snapshot/log compat concern (verified).** `R` is in-process only; `D = ClusterCommand`
  (the persisted type) is untouched. Old Raft snapshots/logs deserialize unchanged. Pre-production
  anyway.
- **Scope creep into `ForwardedWrite`.** Tempting to also type `network.rs:120`'s
  `Result<(), String>`, but that is a cross-node RPC contract — a separate seam with its own
  serialization surface. Explicitly out of scope; this proposal deliberately stops at the
  in-process `R` boundary. (Related: it re-flattens the error at `network.rs:684`, so a follow-up
  could carry `ClusterError` over the bus once `ClusterError: Serialize` exists — which this
  proposal delivers, teeing that up.)
- **Alternative — leave `Value(String)`, only fix `Error`.** Viable (steps 1-3 alone). But
  `Value(String)` is the same defect and its epoch payload is provably dead, so folding it in costs
  little and removes a second stringly hole. Splitting is allowed if reviewers prefer minimal blast
  radius per PR.
- **Alternative — a bespoke error-code enum instead of reusing `ClusterError`.** Rejected: it would
  duplicate the taxonomy and reintroduce a mapping table (the exact fragmentation this removes).
  Reusing `ClusterError` keeps a single owner of the failure vocabulary.
- **`Display` drift.** Consumers that keep `format!("ERR {e}")` rely on `ClusterError`'s
  `#[error(...)]` strings staying stable; they are the same strings `to_string()` produced, so no
  client-visible change — but a future `#[error(...)]` edit now affects RESP output. Acceptable and
  covered by the parity test.

## Effort

**S-M.** Two enum edits + adding two derives + one `unwrap_or_else` line are trivial; the work is the
compiler-guided sweep of six consumer sites and porting/adding ~4 tests, all inside `cluster` plus a
handful of `server` call sites. No async, storage, or wire-format changes. It is S if only the
typed-`Error` half (steps 1-3) lands; M with the `Epoch` variant folded in. The consumer sweep is
purely mechanical (`format!("ERR {e}")` at every site — no behavioral branch), and the compiler
drives every edit, so nothing can be silently missed. Cost/benefit is modest: this is a structural
tidy that preserves the failure vocabulary across the seam, not a behavioral fix.

## Related

- **31** (sibling in this deepening round) — same "typed value across a boundary" family; not yet
  written at time of authoring (highest existing proposal is 30). Cross-reference on landing.
- **10 — cluster-apply-owns-events** — operates on the *same* `apply` `Normal` arm
  (`state.rs:298-384`); proposal 10 moves event *derivation* into `apply_command`, this proposal
  fixes the error *return* type. They compose cleanly (10 changes the success channel, 32 the error
  channel) and could land together, but neither depends on the other. Proposal 10's emit-on-failure
  hotfix has already landed (the `set_role_demotion` capture is now success-gated at
  `state.rs:352-362`); its structural refactor has not.
- **29 — split-brain-divergence-record** — also in the `cluster`/failover neighborhood; both
  improve the fidelity of what the Raft Metadata Plane reports to observers.
- **ADR-0001 (raft-cluster-metadata)** — unchanged. This is purely how the state machine *types its
  return value*; the data-path-never-through-Raft decision is untouched.

## Adversarial review

**Verdict: AMEND** (accepted). The review confirmed the core premise and mechanics are sound
(`Error(String)` flattens the typed `ClusterError` at the apply boundary; the change is feasible
under the borrow checker / crate direction once the serde derives are added), but refuted the
proposal's load-bearing *motivation* — a concrete auto-failover retry bug. Amendments applied:

- **[major] Failover-retry payoff is unreachable — resolved (struck).** The review's central
  finding, verified independently against the code: `failure_detector.rs:432` issues
  `Failover{force:true}` (`failure_detector.rs:422-426`) and `:297` issues `MarkNodeFailed`
  (`:294`). The `Failover` apply arm returns only `InvalidOperation` (`commands.rs:150`) or
  `NodeNotFound` (`commands.rs:155/160`), and with `force:true` it *clears* in-progress migrations
  (`commands.rs:189`) rather than inspecting them; `MarkNodeFailed` returns only `NodeNotFound`
  (`commands.rs:232`). `MigrationInProgress` is produced solely by `BeginSlotMigration`
  (`commands.rs:264`), which neither failover site issues. So both sites can only ever receive
  permanent errors; "give up on any error" is correct, and the code comment "retrying the same
  command cannot succeed" is accurate. The Summary, the consumer table, the "sharpest case"
  paragraph, and the "Why it is shallow" leverage paragraph were rewritten to strike the coin-flip /
  retry-policy framing and re-pitch the change as vocabulary preservation only.
- **[major] Impossible test (plan bullet 2) — resolved (struck).** The "`MigrationInProgress` is
  retryable at the failover boundary" test pinned an unreachable state. Replaced with a plain
  `as_error()` accessor test; the design sketch's `match` on `MigrationInProgress` (dead code) was
  removed, and migration-plan step 3's "opportunistically upgrade `failure_detector.rs:432`" dead
  branch was deleted. All references to a "behavioral win" are gone.
- **[minor] Overstated consumer leverage — resolved.** Reframed throughout: no consumer is
  demonstrated to make a runtime decision on the variant (three client-facing sites forward `-ERR`
  verbatim per Redis parity; `network.rs` is string-only by contract; the failover sites see only
  permanent errors). Pitched as a structural typed-cleanup with honest, modest cost/benefit, not a
  capability unlock. Effort section adjusted accordingly.
- **[minor] Verified-correct facts — acknowledged, no change.** The review confirmed the accurate
  parts: `ClusterError` needs added `Serialize/Deserialize` derives (`types.rs:404`);
  `ClusterResponse` shape (`types.rs:360-368`); `R` is in-process via `OneshotResponder` with no
  wire/snapshot compat concern; `Value(String)` epoch is produced and dropped; `Epoch(ConfigEpoch)`
  wording is consistent (`ConfigEpoch=u64`); `state.rs:354/366/378` `matches!(_, Error(_))` still
  compiles; no conflict with prior rounds / ADR-0001. These stand as written.

The proposal remains valid as a pure structural typed-cleanup; only its motivation and the two
artifacts that depended on the false failover claim were corrected. Path-convention note from the
review (proposal cites `cluster/src/...` / `server/src/...`; actual roots are
`frogdb-server/crates/{cluster,server}/src/...`) is a documented relative convention, retained.
