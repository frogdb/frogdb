# 77 — Operator: a `ChildResource` seam, and env names that come from the schema

Round 38 · lane: frogctl / operator / telemetry · candidates **FR7 + FR8** · effort **M** ·
crates: `frogdb-operator` (unlocked, separate workspace), `frogdb-config` (unlocked),
`frogdb-server` (unlocked, delete-only)

All paths, line numbers and counts below were re-derived against the working tree at
**HEAD `8ea113a5`** ("build: unblock agent worktrees by untracking VS Code config (#84)").
HEAD advanced to **`4c36827d`** while this was being written; the four intervening commits touch
`.scratch/arch-deepening/proposals/*.md` **only** (69, 70, 75, 76 — zero source files,
`git diff --stat 8ea113a5..4c36827d`), so every citation below still holds at `4c36827d`.
The lane brief's citations are stale; where the brief and the tree disagree, the tree wins and
the correction is stated inline. Two of the brief's three FR7 sub-claims turned out to be
*understatements* and one was simply wrong about which way the defect points — see §4.

## Summary

`frogdb-operator` is 1 526 lines of source and 1 343 lines of test, and the split between them is
the whole story: **every line that was made testable was made testable by moving it out of
`controller.rs`**, and everything left behind — the reconcile sequence, the status assembly, the
condition constructor, the timestamp — has **zero tests** (`controller.rs`, 365 lines, no
`#[cfg(test)]`). The 1 343 test lines exercise `resources/*` builders, which are pure functions,
plus a real-server round-trip of `config_gen`. Nothing exercises the part that talks to
Kubernetes, because that part has no seam.

Two candidates, one disease each.

**FR7 — the apply ritual and the status it writes.** `reconcile` (`controller.rs:65-247`) applies
five children with five verbatim copies of the same seven-line block: build → `Api::namespaced` →
`.metadata.name.as_deref().unwrap()` → `PatchParams::apply("frogdb-operator")` →
`Patch::Apply` → `?`. Five copies means five places to put the decision that is *missing from all
of them* (`.force()`), five `.unwrap()`s in a reconcile loop, and a hand-maintained child list
that has **already drifted**: five children are applied, three are watched
(`.owns(...)` ×3 at `:49-51` — the PodDisruptionBudget is not among them). The status it writes
is worse: `chrono_now()` (`:358-365`) emits `"1786483351Z"`, which is not RFC 3339, not ISO 8601
despite its own comment, and not a timestamp any Kubernetes client can parse; and because the
value is recomputed unconditionally on every pass, `lastTransitionTime` marks *the last
reconcile*, not the last transition — inverting the one guarantee the field exists to give.

**FR8 — the env plane never got the ADR-0001 treatment.** `config_gen.rs` is exemplary: it
populates real `frogdb_config` section structs and says why in its module doc ("any future
server-side rename/addition becomes a **compile error** here"). Twelve lines away,
`statefulset.rs:79-133` hand-writes **seven `FROGDB_*` variable names** — four as `EnvVar`
literals, three more as `export` lines inside a shell-script string — none of which any compiler,
test or lint connects to the schema they name. All seven are correct today, so this is latent;
but the same file already demonstrates what latent drift costs, because
`config_gen::cluster_env_toml` (`:88-107`) — the abandoned first attempt at exactly this fix — is
`#[allow(dead_code)]`, has **three tests**, and takes as parameters the three CRD fields that
reach nothing.

**That is the LIVE half of FR8, and it is worse than the brief suggests.**
`spec.cluster.electionTimeoutMs`, `heartbeatIntervalMs` and `autoFailover` (`crd.rs:262-278`) are
published in the generated CRD (`deploy/crd.json:53-77`), documented, defaulted — and read by
**nothing in the reconcile path**. The shipped example manifest sets `autoFailover: true`
(`deploy/examples/cluster.yaml:27`). An operator who applies the file FrogDB ships gets automatic
failover **off**. This is the same defect class the operator's own `CONTEXT.md:95-97` records
having found and fixed once already, for `spec.upgrade.autoFinalize`.

**One hotfix leads.** `chrono_now` is round-plan **H5** and lands first: six lines, no new
dependency, no schema change, independent of both candidates. Spec in §Effort.

## Files involved

Whole-file line counts, verified at HEAD `8ea113a5`.

| File | Lines | Role in this proposal |
|---|---|---|
| `frogdb-operator/src/controller.rs` | 365 | **Primary, both candidates.** The five apply blocks (`:96-105`, `:107-116`, `:118-126`, `:154-162`, `:164-174`) collapse into `apply_child` calls. `condition()` `:348-356`, `chrono_now()` `:358-365`, `update_status` `:318-346`, the condition assembly `:180-214`, the `.owns` chain `:48-52`. **Zero tests.** |
| `frogdb-operator/src/resources/mod.rs` | 22 | **Primary (FR7).** Destination for `ChildResource`, `apply_child`, `FIELD_MANAGER`, and `owner_ref` — today it holds only `standard_labels` `:10-19` and one const `:22`. |
| `frogdb-operator/src/resources/statefulset.rs` | 256 | **Primary (FR8).** Four `EnvVar` name literals `:80,85,90,105`; four shell `export` lines `:124,126,127,128`; the mode match `:70-143`; the `command[0] == "/bin/sh"` re-split `:145-149`; owner ref `:222`. |
| `frogdb-operator/src/config_gen.rs` | 161 | **Primary (FR8).** `generate_toml` `:40-85` (the correct pattern, gains `[cluster]`); `cluster_env_toml` `:88-107` (dead, `#[allow(dead_code)]` at `:88`); its test `:151-160`. |
| `frogdb-operator/src/resources/configmap.rs` | 70 | `owner_ref_from` `:39-50` — reached as `super::configmap::owner_ref_from` from three sibling modules; moves to `mod.rs`. |
| `frogdb-operator/src/resources/service.rs` | 85 | Two builders, owner refs `:47`, `:71`. Adapter changes only. |
| `frogdb-operator/src/resources/pdb.rs` | 39 | `build -> Option<PodDisruptionBudget>` `:12-39` — the `Option` return is already the `ChildResource` shape; owner ref `:26`. |
| `frogdb-operator/src/resources/servicemonitor.rs` | 48 | `#[allow(dead_code)]` `:10`, zero callers. Owner reference **retyped by hand as JSON** `:28-35`. |
| `frogdb-operator/src/crd.rs` | 566 | `ClusterSpec` `:262-278` (three unread fields); `FrogDBCondition` `:409-430`, `last_transition_time: Option<String>` `:421`. |
| `frogdb-operator/src/health.rs` | 49 | `check_pod_health` `:7-20` — `#[allow(dead_code)]`, zero callers. `probe_pod_is_primary` `:29-49`, 5 s timeout `:34`, live. |
| `frogdb-operator/deploy/crd.json` | 406 | **Generated** (`just operator-crd`, `Justfile:961-963`). `lastTransitionTime` schema `:317-321` — bare `"type": "string"`, no `format`. Dead cluster knobs `:53-77`. |
| `frogdb-operator/deploy/examples/cluster.yaml` | 39 | `autoFailover: true` `:27` — the shipped manifest whose value is dropped. |
| `frogdb-operator/deploy/helm/frogdb-operator/templates/clusterrole.yaml` | 36 | `servicemonitors` RBAC `:33-35`, granted for a resource never created. |
| `frogdb-operator/CONTEXT.md` | 99 | `:64` claims the CR owns an "optional ServiceMonitor". Domain vocabulary source. |
| `frogdb-operator/tests/resource_builders.rs` | 642 | 44 pure builder tests. `:114-118` asserts the env literals *by literal* — the tautology this proposal replaces. No `servicemonitor` module. |
| `frogdb-operator/tests/integration.rs` | 701 | Real-server tests. `:35-44` maps `spec.cluster.{election,heartbeat}` into a live `ClusterNodeConfig` — a wiring the operator itself does not have. `:210-219` tests dead `cluster_env_toml`. |
| `frogdb-server/crates/server/src/config/loader.rs` | 480 | `:96-105` — the figment env-name closure. The **forward** direction of the grammar FR8 needs the inverse of. |
| `frogdb-server/crates/config/src/cluster.rs` | ~270 | `ClusterConfigSection` `:17-113`, `rename_all = "kebab-case"` `:16`, **no** `deny_unknown_fields`. The type FR8 builds. |
| `frogdb-server/crates/config/src/env.rs` | *new, ~60* | Destination for the env-name grammar, both directions, one round-trip test. |
| `adr/0001-operator-imports-server-config-crate.md` | 7 | Amended. **Shared with proposal 72** — see *Sibling edges*. |

**Not in this file set** (deliberate): `frogdb-server/ops/helm/**` and
`frogdb-server/ops/deploy/helm/**` (the Helm-managed path — censused in §5, out of scope),
`frogdb-server/docker/Dockerfile*` (four more `FROGDB_*` literals, same reasoning),
`frogdb-operator/src/{main,lib,telemetry,testing}.rs`.

## Problem

### 1. Five copies of one apply, and the decision none of them makes

The block, verbatim five times (`controller.rs:99-105`, `:110-116`, `:120-126`, `:156-162`,
`:168-173`):

```rust
cm_api
    .patch(
        cm.metadata.name.as_deref().unwrap(),
        &PatchParams::apply("frogdb-operator"),
        &Patch::Apply(&cm),
    )
    .await?;
```

Three facts are restated five times: the field-manager string, the patch mode, and the assumption
that a builder always sets `.metadata.name` (five `.unwrap()`s inside a reconcile loop — true
today because every builder hardcodes it, and unenforced).

The fact that is stated **zero** times is `.force()`. `kube::api::PatchParams::apply(manager)`
leaves `force: false`, so a server-side apply that touches a field owned by another manager
returns `409 Conflict`. The `?` propagates it, `error_policy` (`:250-253`) requeues in 15 s, and
the CR never converges again. Any `kubectl apply` against a managed StatefulSet, any mutating
webhook, any HPA touching `spec.replicas` arms it permanently. **Latent** — it needs a second
field manager — but it is the textbook operator failure mode, and today fixing it means editing
five sites and hoping a sixth is never added.

The child list has already proven it drifts. Five children are applied; three are watched:

```rust
Controller::new(frogdbs, WatcherConfig::default())
    .owns(statefulsets, WatcherConfig::default())   // controller.rs:49
    .owns(services,     WatcherConfig::default())   // :50
    .owns(configmaps,   WatcherConfig::default())   // :51
```

`PodDisruptionBudget` is applied (`:164-174`) and not owned. Delete a PDB and nothing notices for
up to 30 s. **LIVE**, small in blast radius, and exactly the kind of asymmetry a hand-maintained
pair of lists produces.

Locality is the other half. `owner_ref_from` lives in `configmap.rs:39-50` — a builder module —
and the other three builders reach across for it (`statefulset.rs:222`, `service.rs:47,71`,
`pdb.rs:26`) as `super::configmap::owner_ref_from`. `servicemonitor.rs:28-35` does not reach: it
**retypes the same six fields as a JSON literal**, in camelCase, because its child is a
`serde_json::Value`. Six children, two encodings of one fact, one of them living in the wrong
module.

### 2. `chrono_now` — LIVE, and the CRD schema is why it survives

```rust
// controller.rs:358-365
fn chrono_now() -> String {
    // ISO 8601 format without chrono dependency
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    format!("{}Z", now)
}
```

The output is `"1786483351Z"`. The comment is wrong twice: that is not ISO 8601, and the operator
already links a date-time library (see the hotfix). A conformant value is RFC 3339 at second
precision, UTC — `"2026-08-10T18:22:31Z"` — which is what `metav1.Time` marshals and what every
condition-aware client parses.

**Why nothing rejects it.** `FrogDBCondition::last_transition_time` is `Option<String>`
(`crd.rs:421`), so the generated schema is a bare string with no `format` keyword
(`deploy/crd.json:317-321`). apiextensions validates `format: date-time` when it is declared;
here it is not, so the API server **stores the garbage verbatim** and every write succeeds. There
is no loud failure anywhere — which is precisely why this has survived.

**What consumes it.** Anything decoding `status.conditions` into `[]metav1.Condition` — the
shape client-go's `apimeta.FindStatusCondition` / `meta.SetStatusCondition` require —
fails `metav1.Time`'s RFC 3339 unmarshal and drops **the whole status object**, not just the
field. `kubectl` prints it raw, which is how it stays invisible in manual testing. Ruling:
**LIVE**, wrong on the wire, silent by construction.

### 3. The condition churn is a reconcile amplifier — LIVE

`condition()` (`:348-356`) calls `chrono_now()` on construction, and `reconcile` constructs its
conditions unconditionally every pass (`:180-214`). `update_status` then writes them with a merge
patch (`:341-343`), and a merge patch replaces a list wholesale. So `lastTransitionTime` advances
on **every reconcile**, whether or not anything transitioned. The field's entire contract is
"when did `status` last change", and it now answers "when did the controller last run".

The second-order effect is the interesting one. The `Controller` watches `FrogDB` with a default
config and no generation predicate (`:48`), so its own status write is an input event:

1. requeue fires at T+30 s → reconcile → patch writes `lastTransitionTime = "T30"`;
2. the object changed → watch event → reconcile;
3. that pass computes the *same* whole-second string → the patch is a no-op → no event.

So the steady state is a **doubling**, self-limited only by `chrono_now`'s second granularity.
The limit fails the moment a single pass spans a second boundary — and
`detect_primary_pod` (`:278-314`) probes pods **sequentially** with a **5 s per-pod timeout**
(`health.rs:34`), building a fresh `reqwest::Client` each time. On a standalone deployment with
unreachable replicas, one pass takes tens of seconds and every pass writes a different timestamp,
so the controller re-triggers itself continuously. The 30-second requeue at `:246` is not the
reconcile rate; it is a floor the code does not respect.

A related convention break in the same block: when an upgrade finishes, the `Upgrading` condition
is not set to `False` — it is **replaced** by a `Progressing` condition (`:196-214`), so a client
watching `Upgrading` sees the condition *vanish*. Conditions are a keyed set; entries are flipped,
not deleted. **LIVE**, same root cause (the array is rebuilt from nothing each pass instead of
being merged into the previous one, which `Arc<FrogDB>` already carries in `frogdb.status`).

### 4. ServiceMonitor — the brief has this backwards

The brief asks whether the ServiceMonitor is "emitted without checking the CRD exists". It is
not emitted at all. `servicemonitor::build` is `#[allow(dead_code)]` (`:10`) and its only
reference in the tree is the `pub mod` line at `resources/mod.rs:6`. `reconcile` never mentions
it. `resource_builders.rs` has no `servicemonitor` module — 44 builder tests, none for this one.

What *is* live is everything around it:

* `CONTEXT.md:64` states the CR "owns its StatefulSet, headless + client Services, ConfigMap, PDB,
  and optional ServiceMonitor via owner references." Four of five are true.
* `clusterrole.yaml:33-35` grants `monitoring.coreos.com/servicemonitors` `create`/`update` —
  standing permission for a resource the operator never touches.

Ruling: **LIVE as a documented-and-provisioned capability that does not exist; latent as code.**

And the gate the brief asks about is a *precondition for wiring it*, not a current bug. Applying a
`monitoring.coreos.com/v1` object to a Kubernetes cluster without the Prometheus Operator CRDs
returns `404 NotFound`; through the `?` at the end of the ritual that fails the whole reconcile,
so a single missing optional CRD would strand **every** FrogDB CR. Wiring without the gate is
strictly worse than the dead code. This is the strongest argument for `apply_child` being one
function: "optional child, absent CRD is not an error" is a rule with exactly one place to live.

The same file set carries one more pure-dead item: `health::check_pod_health` (`:7-20`),
`#[allow(dead_code)]`, zero callers.

### 5. FR8 — seven env names the schema does not know about

`config_gen.rs:1-8` states the ADR-0001 rule in its own module doc and follows it. `statefulset.rs`
sits in the same crate and does not:

| Site | Literal | Resolves to | Verdict |
|---|---|---|---|
| `:80` | `FROGDB_CLUSTER__ENABLED` | `cluster.enabled` (`cluster.rs:21`) | correct |
| `:85` | `FROGDB_CLUSTER__CLUSTER_BUS_ADDR` | `cluster.cluster-bus-addr` (`:40`) | correct |
| `:90` | `FROGDB_CLUSTER__CLIENT_ADDR` | `cluster.client-addr` (`:34`) | correct |
| `:105` | `FROGDB_CLUSTER__INITIAL_NODES` | `cluster.initial-nodes` (`:46`) | correct |
| `:124`, `:126` | `FROGDB_REPLICATION__ROLE` | `replication.role` (`replication.rs:20`) | correct |
| `:127` | `FROGDB_REPLICATION__PRIMARY_HOST` | `replication.primary-host` (`:26`) | correct |
| `:128` | `FROGDB_REPLICATION__PRIMARY_PORT` | `replication.primary-port` (`:31`) | correct |

Seven for seven, verified against the loader's mapping (`loader.rs:96-105`: strip `FROGDB_`,
protect `__` as the section separator, single `_` → `-`, match kebab-case serde). **Latent.**

Latent is not harmless here, for three reasons.

* **Nothing would catch the eighth.** The only "test" is
  `resource_builders.rs:114-118`, which asserts `env_value(c, "FROGDB_CLUSTER__ENABLED")` is
  `Some("true")` — a literal compared against the same literal. It passes for any name, including
  one the server ignores. And an ignored `FROGDB_` variable is **silent**: figment's env provider
  merges what it recognises, and `ClusterConfigSection` carries no `deny_unknown_fields`
  (`cluster.rs:16`), so a typo produces a pod that boots happily with the wrong topology.
* **Three of the names live in shell text.** `:122-132` is a `format!`-built `/bin/sh -c` script.
  Those `export` lines are not `EnvVar` values, not typed, not greppable as a pair with anything.
  (They are not an injection risk — `{name}` and `{ns}` are DNS-1123 labels — but they are the
  deepest form of the problem: config expressed as a string with no schema behind it.)
* **`just lint` never looks.** `frogdb-operator` declares its own `[workspace]`
  (`Cargo.toml:40`) and is not a member of the root workspace (`Cargo.toml:3-38`), so
  `just check`, `just lint` and `just test` do not reach it. Its only gates are
  `just operator-build` / `operator-test` / `operator-crd` (`Justfile:961-971`) and a
  **path-filtered** CI job (`test.yml:245-269`, `if: needs.changes.outputs.operator`). A rename in
  `frogdb-config` that breaks the operator's *compile* is caught (the CI filter includes
  `frogdb-operator/Cargo.lock`, and `config_gen` is typed); a rename that breaks only a
  **string** is caught by nothing, in any workspace, ever.

### 6. FR8 LIVE — three CRD fields that reach nothing

`ClusterSpec` (`crd.rs:262-278`) has four fields. `bus_port` is read (`statefulset.rs:75,86,100`,
`service.rs:33`). The other three are read by:

* `config_gen::cluster_env_toml` (`:89-107`) — `#[allow(dead_code)]`, zero non-test callers;
* `testing.rs:48-49` — a test fixture;
* `integration.rs:40-41` — which maps them into a live `ClusterNodeConfig` and stands up real
  servers with them, **asserting a wiring the operator does not have**;
* `config_gen.rs:151-160` and `integration.rs:210-219` — three tests of the dead function.

Nothing in `reconcile`, `statefulset::build`, `config_gen::generate_toml` or the ConfigMap
touches them. They are published to users in the generated CRD (`crd.json:53-77`) with
descriptions and defaults, and the shipped example sets one of them:

```yaml
# frogdb-operator/deploy/examples/cluster.yaml:23-27
  cluster:
    busPort: 16379
    electionTimeoutMs: 1000
    heartbeatIntervalMs: 250
    autoFailover: true
```

Apply FrogDB's own example and `cluster.auto-failover` is `false` on every pod, because the
server's default is `false` (`cluster.rs:83`, bare `#[serde(default)]`) and nothing overrides it.
`electionTimeoutMs`/`heartbeatIntervalMs` happen to match the server defaults (`crd.rs:284-290`
vs `cluster.rs:116-117`), so only non-default tunings are lost there. **LIVE.**

The tests make it worse rather than better: five tests assert that `cluster_env_toml` produces
correct TOML. They are true and they are certifying a function no deployment executes — the same
pattern proposal 72 found in `frogctl`'s `ops::config`, in a different crate, on the same day.

## Proposed change

Three modules, in dependency order. The first is in the schema crate and stands alone.

### 1. `frogdb-config::env` — the env-name grammar, both directions

New `frogdb-server/crates/config/src/env.rs`:

```
pub const PREFIX: &str = "FROGDB_";
pub fn var_name(section: &str, field: &str) -> String;   // ("cluster","auto-failover") -> FROGDB_CLUSTER__AUTO_FAILOVER
pub fn figment_key(env_key: &str) -> String;             // the loader's closure, by name
pub fn overrides<S: Serialize>(section: &str, value: &S) -> Vec<(String, String)>;
```

`figment_key` is `loader.rs:100-104` moved verbatim. `var_name` is its inverse, and the two are
round-trip tested against each other in the crate that owns the schema.

**This is a depth move.** The env-variable grammar is a *contract of the config crate* — it is how
`Config` is populated from the environment — and today it exists as a closure in the server binary
plus seven strings in another workspace. `overrides()` gives the operator a way to say "these are
the values I want" as a **typed `ClusterConfigSection` value**, and get back names it did not
choose. Placeholders like `$(POD_IP):16379` pass through untouched: `overrides` derives *names*
from serde idents and copies *values*, so a deferred value stays a `String` field's value.

**Deletion test.** After the move, `loader.rs:96-105`'s closure body is deleted in favour of
`.map(|k| frogdb_config::env::figment_key(k.as_str()).into())`, and the literal `"FROGDB_"`
appears in exactly one Rust file in either workspace. That is greppable, so it becomes a **seam
lint** — `lint-env-name-seam`: no `"FROGDB_` string literal in `frogdb-server/crates/**` or
`frogdb-operator/src/**` outside `config/src/env.rs`. It joins the compile-free family in
`lint-gates` (`Justfile:329`) at the cost of one `rg` invocation, and it is the first gate in the
family to police the operator at all. Invariant, in the family's own form: *every `FROGDB_*`
variable name is produced by `frogdb_config::env`, never written out.*

**Leverage.** One ~60-line module pays the server's loader, the operator's cluster overlay, the
operator's shell wrapper, and any future consumer, and it converts an untestable class of typo
into a compile error (`ClusterConfigSection` has no field `auto_failovr`).

### 2. `ChildResource` + `apply_child` in `resources/mod.rs`

```rust
pub const FIELD_MANAGER: &str = "frogdb-operator";

/// Everything a child needs beyond the CR itself.
pub struct ChildInput<'a> { pub toml: &'a str, pub config_hash: &'a str }

pub trait ChildResource: Resource<DynamicType = ()> + Serialize + DeserializeOwned + Clone + Debug {
    /// `None` = this child should not exist for this CR (PDB disabled, metrics off).
    fn build(frogdb: &FrogDB, input: &ChildInput<'_>) -> Option<Self>;
    /// `false` = the child's API group may be absent; a 404 is "skipped", not an error.
    const REQUIRED: bool = true;
}

pub async fn apply_child<K: ChildResource>(client: &Client, ns: &str, child: &K)
    -> Result<(), Error>;
```

`apply_child` is the single place that owns: the field manager, `Patch::Apply` **with
`.force()`**, the name (taken from `ResourceExt::name_any()`, so the five `.unwrap()`s go),
the error context (`kind` + `name` in the message, which today's bare `?` loses), and the
`REQUIRED == false ⇒ 404 is Ok(())` rule that makes the ServiceMonitor safe to wire.

`reconcile`'s middle becomes five calls. `owner_ref` moves from `configmap.rs:39-50` to
`mod.rs` as a free function next to `standard_labels`, and `super::configmap::` disappears from
three modules. `servicemonitor` becomes a `ChildResource` over `kube::api::DynamicObject` with
`REQUIRED = false`, deleting its hand-written JSON `ownerReferences` (`:28-35`) — six fields that
stop being a second encoding of one fact.

**Architecture vocabulary, stated plainly.** The trait is an **interface over the six child kinds**;
`apply_child` is the **chokepoint** every write goes through; `ChildInput` is the **adapter**
between "what the CR says" and "what a builder needs"; `owner_ref` moving into `mod.rs` is
**locality** (the fact belongs to *all* children, not to the ConfigMap).

**Deletion test, applied honestly.** This one does **not** pay for itself in lines. Roughly 35
lines leave `controller.rs` and roughly 45 arrive in `mod.rs`; the module-level question "could
`servicemonitor.rs` go entirely?" answers *yes, today* — and the right response is to wire it, not
delete it, because the RBAC and the CONTEXT.md claim say the project wants it. So the honest
statement of the test is: **the seam is worth it for the decisions it centralises, not for the
lines it removes.** Four decisions currently have five homes each (`force`, field manager, name
extraction, error context) and one decision has none (optional-CRD tolerance). If a reviewer
weighs those as insufficient, the trait should not land and the five blocks should keep their
copies — that is the real test, and it should be applied consciously rather than assumed.

The `.owns` drift is **not** fixed by the trait — `Controller::owns` needs a concrete type at
compile time and cannot be driven from a runtime list without a macro, which is more cleverness
than this earns. It is fixed by a test (§Testability) that compares the applied-kind list against
the watched-kind list. Simpler, and it catches the same class.

### 3. Status: merge conditions, and stop inventing timestamps

`condition()` stops calling the clock. A new pure function does the merge:

```rust
/// `previous` is `frogdb.status.as_ref().map(|s| &s.conditions)` — already in hand.
fn merge_conditions(previous: &[FrogDBCondition], desired: Vec<FrogDBCondition>, now: &str)
    -> Vec<FrogDBCondition>;
```

Rules, matching the Kubernetes condition convention: conditions are keyed by `type`; an entry
whose `status` is unchanged keeps its previous `lastTransitionTime`; a changed `status` takes
`now`; a type present before and absent from `desired` is **retained with its status flipped**,
not dropped — which is what fixes `Upgrading` vanishing. `now` is a parameter, so the function is
pure and the whole status assembly becomes testable without a `kube::Client`.

Once `lastTransitionTime` is stable across a steady-state reconcile, the merge patch at `:341-343`
becomes a genuine no-op, the self-triggered second pass stops, and `Action::requeue(30s)` becomes
the actual reconcile rate.

`chrono_now` is replaced by the H5 formatter (§Effort). The **durable** form types the field as
`k8s_openapi::apimachinery::pkg::apis::meta::v1::Time` — which is `jiff::Timestamp` in
k8s-openapi 0.27 and serializes correctly by construction — but that needs k8s-openapi's
`schemars` feature (`Cargo.toml:20` today declares only `v1_33`) and **changes the generated
CRD**, so it ships with this proposal and re-runs `just operator-crd`, never as a hotfix.

### 4. FR8 applied: static config to the ConfigMap, per-pod config through the seam

The split the operator makes accidentally today should be stated deliberately:
**identical-for-every-pod config goes in the ConfigMap; per-pod or pod-IP-derived config goes in
env.** Under that rule:

* `election-timeout-ms`, `heartbeat-interval-ms`, `auto-failover` and `enabled` are static → they
  join `generate_toml` as a `[cluster]` section, populated from `ClusterSpec` through
  `ClusterConfigSection` exactly as the other five sections already are. The three dead CRD fields
  come alive with no new mechanism. `cluster_env_toml` is either this code or is deleted — it is
  not both.
* `cluster-bus-addr`, `client-addr` and `initial-nodes` are pod-derived → they stay env, but
  built as `env::overrides("cluster", &ClusterConfigSection { .. })` rather than four literals.
* The standalone wrapper's three exports become
  `format!("export {}={}", env::var_name("replication", "role"), ..)` — still shell, but the
  names now come from the schema.

`config_gen.rs`'s module doc already argues this case for TOML; the change makes the argument true
for the crate rather than for one function.

**`adr/0001` — extended, not contradicted.** The ADR's decision is that the operator "serializes
through the server's own serde types rather than maintaining a parallel schema". Seven env-name
literals *are* a parallel schema — of names rather than of keys — that the ADR's authors did not
name because the TOML path was the one under discussion. Suggested amendment, in the ADR's terse
one-paragraph form:

> The rule covers both planes the operator configures the server on. Names on the environment
> plane (`FROGDB_<SECTION>__<KEY>`) are produced by `frogdb_config::env` from the same serde
> idents that produce the TOML keys, never written out as literals, so a section or field rename
> is a compile error on both paths. Config identical for every pod belongs in the mounted
> `frogdb.toml`; only pod-derived values are passed as environment overrides.

`frogdb-config` stays light (`config/Cargo.toml` gains nothing — `env.rs` uses `serde` and `std`),
which is the ADR's other invariant. **Proposal 72 amends the same 7-line file** — see *Sibling
edges*.

### Vocabulary

Per `frogdb-operator/CONTEXT.md`: `spec.mode` is the **Mode**; `spec.cluster` is the **Cluster
Tuning Block** and never "the cluster spec" (`:28-31`); "cluster" unqualified means the FrogDB
cluster and the orchestrator is always the **Kubernetes cluster** in full (`:33-34`); node roles
are **Primary**/**Replica** (`:36-37`); `spec.replicas > 1` in Standalone Mode is
**Primary + Replicas**, in Cluster Mode it is the member count. The image-tag restart is a
**Rolling Upgrade**, the annotation-driven one a **Config Hash Rollout** (`:43-50`) — §4's
`[cluster]` change triggers the latter, not the former, and this proposal says so in those words.
Avoid-listed: *FrogDBCluster*, *"the cluster spec"*, bare "cluster" for the orchestrator.

## Testability improvement

The operator's test suite is 1 343 lines and **none of it can reach `controller.rs`**, because
every function there either takes a `kube::Client` or is private and clock-dependent. Both
candidates move code across that line.

**New pure functions, testable with no cluster and no client:**

1. `merge_conditions(previous, desired, now)` — the assertion that does not exist today:
   *reconcile twice with unchanged inputs and `lastTransitionTime` does not move.* That single
   test pins §3 permanently and is impossible to write against the current shape.
   Second test: a type present before and absent from `desired` survives with `status: "False"`
   (the `Upgrading` regression).
2. `env::var_name` / `figment_key` round trip, in `frogdb-config`: for every section and field of
   `Config`, `figment_key(var_name(s, f))` reconstructs `s.f`. This is the test that makes the
   whole FR8 defect class unrepresentable, and it lands in a crate **inside the default nextest
   run** — unlike everything in `frogdb-operator`, which only CI's path-filtered job executes.
3. `overrides("cluster", &section)` against a real `frogdb_config::Config`: apply the pairs, load
   through the same figment chain, assert the value arrives. An end-to-end contract test for the
   env plane, which today has none at any level.
4. **Applied-vs-watched parity**: one test comparing the kind list `reconcile` applies against the
   kind list `run` watches. Catches today's PDB drift and every future one, in ~10 lines.

**Tests that are deleted, not migrated.** `resource_builders.rs:108-121`'s literal-equals-literal
env assertions are replaced by assertions against `env::var_name(..)`, so a rename moves both
sides together. `config_gen.rs:151-160` and `integration.rs:210-219` (five assertions on dead
`cluster_env_toml`) either follow the function into `generate_toml`'s `[cluster]` coverage or are
deleted with it — they must not be left asserting a function nothing calls.

**Coverage-depth note** (`just coverage-depth`, `agents/`): `frogdb-operator` would score well on
line coverage of `resources/*` and zero on `controller.rs`, while `cluster_env_toml` shows three
distinct tests on a function with no callers. Per-function **test diversity** over a
reachability filter would flag both halves. Worth a run after this lands — the operator has never
been in a depth audit.

## Risks / scope boundaries

### Spec / LOCKED / gates

* **No locked crate.** The four locked areas are txn+vll, persistence+recovery,
  replication+replication-runtime, cluster+cluster-runtime (`adr/0002`–`0004`).
  `frogdb-operator` and `frogdb-config` are in none of them; `frogdb-server/.../config/loader.rs`
  is in the server crate, also unlocked.
* **FM tags: the operator is clean — verified.** `grep -rn "FM-" frogdb-operator/` → **0 hits**
  across sources, tests and `CONTEXT.md`. `cluster-failure-modes.md:56` puts the operator out of
  scope in as many words. `just lint-failure-modes` sees no tag added, moved or removed.
* **`frogdb-config` is spec-relevant but untouched where it matters.** Its three FM tags live in
  `persistence.rs:580`, `replication.rs:489`, `replication.rs:516` and force section-level
  `validate()`/`Deserialize` behaviour. `env.rs` is new, adds no validator, and changes no
  `Deserialize` impl. `cluster.rs` is **read only** — this proposal populates
  `ClusterConfigSection`, it does not alter it.
* **Mutation gates: none apply.** No gated crate is touched, so `just mutants-diff` is not push
  discipline here.
* **Seam lints: one gate *added*, none violated.** No clock read is added to a scanned crate —
  `scripts/clock-seam.py` scans `frogdb-server/crates/**` only (`:58-59`, `:209`), so the
  operator's `SystemTime::now()` is outside it today and stays outside after the fix; note in
  passing that the operator is outside **all fifteen** gates, which is why `lint-env-name-seam` is
  proposed rather than assumed. No metrics emission, redirect reply, durable-ack write or figment
  `.nested(` is added.

### Generated files — the discipline applies twice

* `frogdb-operator/deploy/crd.json` is generated (`just operator-crd`, `Justfile:961-963`). The
  `metav1::Time` retyping changes it; regenerate, never hand-edit.
* `frogdb-server/ops/deploy/helm/frogdb/{Chart.yaml,values.yaml,values.schema.json,
  templates/configmap.yaml,dashboards/*}` are generated by `helm-gen`. This proposal touches none
  of them — and note `templates/statefulset.yaml` is **not** generated, which is why §5's Helm
  copy cannot be fixed by editing a generator today.

### The Helm copy — censused, out of scope

`frogdb-server/ops/deploy/helm/frogdb/templates/statefulset.yaml:66-72` carries the same four
`FROGDB_CLUSTER__*` names, and `frogdb-server/ops/helm/frogdb/templates/statefulset.yaml:66-72` is
a near-identical second tree (they differ only in `values.yaml`, `values.schema.json` and a
dashboards ConfigMap — `helm-gen`'s output dir defaults to `deploy/helm/frogdb`, so
`ops/helm/frogdb` appears to be the stale one, and `CONTEXT-MAP.md:29` cites the stale path).
Four more literals live in `frogdb-server/docker/Dockerfile:33-36` and
`Dockerfile.builder:164-167`.

All are **out of scope**: they are Go templates and Dockerfiles, with no Rust seam to route
through. The durable answer for them is a generated env snippet from the same
`frogdb_config::env` module — real work, a separate issue, and blocked on deciding which of the
two Helm trees is canonical. Recorded here so the census is complete and so a reviewer does not
mistake the operator fix for a workspace-wide one. `lint-env-name-seam` must scope itself to Rust
sources for exactly this reason.

### Sibling edges — verified on disk

* **72 (FR2, frogctl config schema).** **Two real edges, both benign.**
  (a) Both proposals amend `adr/0001` — a 7-line file. 72 adds a clause binding *every workspace
  tool that emits or parses `frogdb.toml`*; 77 adds a clause binding *the environment plane and
  the ConfigMap/env split*. They compose; whoever lands second appends rather than replaces.
  (b) 72 creates `frogdb-config/src/document.rs` (`default_toml()`, `to_toml()`); 77 creates
  `frogdb-config/src/env.rs`. **Separate files, deliberately** — `document.rs` renders a whole
  `Config` to a file, `env.rs` renders section *names* for an override plane, and the round-trip
  partner of `env.rs` is `loader.rs`'s figment chain, not `to_toml`. One shared line in
  `config/src/lib.rs` (the `mod` declarations). Either order.
  72 also confirms the fact this proposal builds on: the operator **already** depends on
  `frogdb-config` (`72:172-175`, verified at `frogdb-operator/Cargo.toml:18`), so FR8 adds no
  dependency — it uses the one ADR-0001 already bought.
* **74 (FR3, Debug Bundle).** 74's **H4** edits `frogdb-server/ops/deb/deb-gen`'s FHS overrides to
  emit `directory = "/var/lib/frogdb/bundles"` and regenerates `ops/deploy/deb/frogdb.toml`.
  **No overlap**: 77 touches neither `deb-gen` nor `ops/deploy/deb/**` nor the `[debug-bundle]`
  section, and 74 touches no operator file. One naming collision to flag: 74 numbers its own
  hotfixes H1–H5 internally, so "H5" means the round-plan chrono fix in *this* document and a dead
  code deletion in *that* one. Whoever maintains the round hotfix list should disambiguate.
* **73 (FR1, frogctl ops wiring).** Different crate entirely; verified no operator file in its set.
* **75 (FR4/FR5, frogctl rendering + role enum).** Different crate. Note both 75 and this proposal
  care about Primary/Replica vocabulary, but at opposite boundaries: 75 normalizes the *server's
  INFO wire text*; 77 reads `/admin/role`'s JSON (`health.rs:44-47`, `"master"`/`"slave"`) inside
  the operator. If 75's `Role` enum lands in a crate the operator can import, the operator's
  string match becomes a candidate consumer — a **follow-up**, not a dependency, and explicitly
  not claimed here.
* **76 (FR6/FR11, observability extractors — concurrent author).** Expected none, **confirmed
  none**: 76's set is `frogdb-server/crates/server/src/observability_server.rs`,
  `telemetry/http_handlers.rs`, `routes.rs`, `telemetry/testing.rs`. One indirect touchpoint worth
  stating: the operator probes `/admin/role` on the metrics port (`health.rs:30`), which 76
  re-plumbs behind a `FromRequestParts` extractor. 76 states its changes are wire-compatible; if
  that ever stops being true, `detect_primary_pod` is a consumer nobody would think to check. No
  file overlap; no ordering constraint.
* **69 (SV9, config param combinators — landed while this was written).** Same crate,
  different module: 69 builds a combinator inside `frogdb-config/src/param.rs` and states
  `params.rs` is untouched; 77 adds `frogdb-config/src/env.rs`. The one shared line is again
  `config/src/lib.rs`'s `mod` list. 69's world is `ConfigParam`/`MutableParamId` (the runtime
  mutability plane); 77's is serde idents (the load plane). No golden-snapshot interaction —
  `env.rs` adds no param, so `params.rs`'s 123-row `GOLDEN_SNAPSHOT` is unaffected.
* **70 (ACL registry consult).** Different crate set, no config or operator file. None.
* **78 (test-harness).** Expected none, **confirmed none** — but note the operator's
  `tests/integration.rs` is the only file outside the server workspace that consumes
  `frogdb-test-harness` (`Cargo.toml:38`), including `TestServerConfig` and `ClusterNodeConfig`
  (`integration.rs:26-44`), which candidate 10 proposes to restructure. That is a **compile-time**
  edge in a workspace `just check` does not build. If 78 lands, `just operator-test` must run in
  the same commit. Flagged for the round, not owned here.

### Behaviour changes, named

* **Every existing cluster-mode deployment gets a Config Hash Rollout.** Adding `[cluster]` to the
  generated `frogdb.toml` changes the content, so `config_hash` (`configmap.rs:32-36`) changes, so
  the pod-template annotation changes, so the StatefulSet rolls. This is correct — the pods have
  been running with the wrong settings — but it is a restart of every managed FrogDB on operator
  upgrade and belongs in release notes. It is also why the `[cluster]` change is **not** a hotfix.
* **`.force()` changes conflict semantics.** Fields another manager has claimed get taken over
  instead of failing. That is the intended operator behaviour and the reason the CR exists, but it
  is a behaviour change on any cluster where someone has `kubectl apply`'d a managed child.
* **Reconcile rate drops ~2× (more under slow probes).** Fewer API writes, fewer pod probes. Any
  dashboard counting reconciles will show a step change.
* **Conditions gain history.** `lastTransitionTime` stops moving and `Upgrading` stops
  disappearing — clients that (incorrectly) inferred liveness from a moving timestamp lose that
  signal. Correct, and worth one line in the CRD docs.

### Residual risk

The `ChildResource` trait is the part that could go wrong by being too clever. Three guardrails
for the implementer: `ChildInput` stays a plain struct (no associated types per child); the trait
carries no default `build` (each builder stays a readable function); and `apply_child` gains no
retry, backoff or diffing — `error_policy` already owns requeue. If the trait needs a second
generic parameter, the design has drifted past what the five copies cost.

## Effort

**M**, split roughly evenly:

* **S** — `frogdb-config/src/env.rs` + round-trip test; repoint `loader.rs:96-105`; add
  `lint-env-name-seam` to `scripts/` and `Justfile:329`. Lands independently of everything else
  and is useful even if both candidates are rescoped.
* **M** — FR7: the trait, `apply_child`, `owner_ref` relocation, five call sites, ServiceMonitor
  wired with `REQUIRED = false`, `merge_conditions` + tests, `metav1::Time` retyping +
  `just operator-crd`, `.owns(pdb)`, parity test, `CONTEXT.md:64` corrected.
* **M** — FR8: `[cluster]` into `generate_toml`, cluster overrides through `env::overrides`, the
  shell wrapper's three names, `cluster_env_toml` resolved (absorbed or deleted with its five
  tests), `resource_builders.rs` env assertions rewritten.

Sequencing: **env.rs first** (FR8 needs it, FR7 does not), then FR7 and FR8 in either order.
Within FR7, `merge_conditions` lands *after* H5, so no intermediate commit preserves a malformed
timestamp.

### H5 — LIVE, independently landable, lands first: `lastTransitionTime` is not RFC 3339

Round-plan hotfix **H5**. Confirmed live end to end:

1. Any reconcile of any `FrogDB` CR calls `condition()` (`controller.rs:348-356`).
2. `chrono_now()` (`:358-365`) returns `format!("{}Z", unix_secs)` → `"1786483351Z"`.
3. `update_status` patches it into `status.conditions[].lastTransitionTime` (`:341-343`).
4. The CRD schema for that field is bare `"type": "string"` with no `format`
   (`deploy/crd.json:317-321`), because the Rust field is `Option<String>` (`crd.rs:421`) — so
   the API server validates nothing and **stores it**.
5. Any client decoding conditions as `[]metav1.Condition` fails RFC 3339 parsing and drops the
   whole status object.

**Conformant value:** RFC 3339, UTC, second precision — `2026-08-10T18:22:31Z`.

**Fix, no new dependency.** `k8s-openapi` 0.27 re-exports its date-time crate
(`pub use jiff;`, k8s-openapi `lib.rs:237`) and `metav1::Time` is `jiff::Timestamp`
(`.../meta/v1/time.rs:5`); `jiff` 0.2.23 is already in `frogdb-operator/Cargo.lock:2309-2312`.
`Timestamp`'s `Display` honours the formatter's precision (`jiff .../timestamp.rs:2357-2368`), so
zero fractional digits is a format specifier:

```rust
fn now_rfc3339() -> String {
    // RFC 3339, UTC, second precision — what `metav1.Time` marshals and every
    // condition-aware Kubernetes client parses.
    format!("{:.0}", k8s_openapi::jiff::Timestamp::now())
}
```

**~6 lines**, replacing `chrono_now` at its single call site (`:352`). No CRD change, no schema
change, no dependency edit, no interaction with either candidate.

**Regression test** (the operator's first `controller.rs` test): assert the returned string parses
as `jiff::Timestamp` **and** matches `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$` — the second half
matters, because a value with fractional seconds parses fine and is not what `metav1.Time` emits.

### Also independently landable — LIVE, after H5

* **Condition merge (§3).** ~20 lines: thread `frogdb.status.as_ref()` into a pure
  `merge_conditions`. Ships alone, but **must follow H5** — preserving a malformed timestamp
  across reconciles makes the bad value sticky instead of merely wrong.
* **`.owns(pdb)`** (`controller.rs:48-52`): two lines plus an `Api<PodDisruptionBudget>` binding.
* **The ServiceMonitor claim** (`CONTEXT.md:64`): until the builder is wired, the sentence should
  not list it. Alternatively drop `clusterrole.yaml:33-35`. Doc-only, ~3 lines, and it stops the
  next reader from trusting a capability that does not exist.
* **Delete `health::check_pod_health`** (`health.rs:7-20`): 14 lines, zero callers, pure deletion.

### Explicitly **not** hotfix-eligible, though LIVE

* **The three dead `spec.cluster` fields (§6).** The fix changes generated `frogdb.toml`, which
  changes `config_hash`, which restarts every managed pod. A rolling restart is not a hotfix. It
  ships with FR8, with the rollout stated in the release notes.
* **Deleting `cluster_env_toml`.** It looks like free dead-code removal and is not: its shape is
  FR8's answer for the `[cluster]` section. Delete it *as part of* FR8 or absorb it; do not delete
  it first and rewrite it later.
* **`.force()` on server-side apply.** One-word change, five sites, and it silently changes
  conflict semantics on live clusters. It belongs at the `apply_child` chokepoint where it is
  visible, reviewable, and stated once.
