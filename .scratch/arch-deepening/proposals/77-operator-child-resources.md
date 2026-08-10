# 77 — Operator: the workspace nothing gates, and the env values nobody decodes

Round 38 · lane: frogctl / operator / telemetry · candidates **FR7 + FR8** · effort **M** ·
crates: `frogdb-operator` (unlocked, **separate cargo workspace**), `frogdb-config` (unlocked),
`frogdb-server` (unlocked, delete-only)

Revision 2, after adversarial review `2e81506b` (verdict **AMEND**). Every citation below was
**re-derived against the working tree at HEAD `54baa2bb`**, including the ones revision 1 got
right; the corrections the review supplied were themselves re-checked, and where a review point
did not survive contact with the tree it is refuted with evidence in the **Review ledger** at the
end of this document. Two new empirical probes were run (figment value decoding, figment
`Env` mapper ordering); both are reproduced below with their output.

The review's headline correction stands: **revision 1 led with the wrong finding.** It opened
with a trait. The finding that matters is that this workspace has no gate, and the worst
consequence of that is a **P0 that stops every operator-managed cluster from booting**.

## Summary

### The structural fact, first

`frogdb-operator` declares its own `[workspace]` (`frogdb-operator/Cargo.toml:40`) and is **not**
a member of the root workspace (`Cargo.toml:3-40`, 36 members, none of them the operator). So
`just check`, `just lint` and `just test` — the three commands every agent and every human runs —
**never compile a line of it**. Its only gates are `just operator-build` / `operator-test` /
`operator-crd` (`Justfile:961-971`), which nothing else invokes, and a **path-filtered** CI job
(`.github/workflows/test.yml:245-269`, `if: needs.changes.outputs.operator == 'true' ||
needs.changes.outputs.operator_config == 'true'`; filters at `:52-55` and `:56-59`).

Every finding in this document is downstream of that. 1 796 lines of source, 1 343 lines of test,
zero coverage of the reconcile path, a dead builder nobody noticed, three CRD fields wired to
nothing, and a hard boot failure in cluster mode — all of them survived because **no command in
the developer loop looks here**. Fix the seams below and the class comes back the moment someone
adds an eighth env var, unless the workspace itself is brought under a gate. That is the
proposal's real subject; the trait was a symptom.

### The P0

`statefulset.rs:104-108` builds `FROGDB_CLUSTER__INITIAL_NODES` as `initial_nodes.join(",")`.
The server's config loader **cannot decode that**. figment 0.10.19's value micro-parser produces a
sequence only for **bracketed** input (`value/parse.rs:74`, `peek('[') => Value::from(array()?)`);
a bare comma-separated string stays `Value::String`, and `Vec<String>` has no string coercion, so
`Figment::extract()` (`loader.rs:128`) returns a hard error and the process exits before serving.
**Every operator-managed cluster-mode FrogDB CrashLoopBackOffs on first boot.** Both Helm trees
emit the same shape (`ops/deploy/helm/frogdb/templates/statefulset.yaml:72-73` via
`_helpers.tpl:76-87`'s `join "," $peers`). Nothing catches it because the only test that sets
`initial_nodes` sets it **in-process** (`test-harness/src/server.rs:570-572`), bypassing the env
decoder entirely. Reclassified **Live P0**; it takes hotfix slot **77-H1**, ahead of everything
else in this document. Evidence and repro in §2.

### Two candidates, one disease each

**FR7 — the apply ritual and the status it writes.** `reconcile` (`controller.rs:65-247`) applies
five children with five verbatim copies of the same seven-line block. Five copies means five
places to put the decision that is missing from all of them (`.force()`), five `.unwrap()`s in a
reconcile loop, and a hand-maintained child list that **has already drifted**: five children are
applied, three are watched (`.owns(...)` ×3 at `:49-51`; the PodDisruptionBudget applied at
`:165-174` is not among them). The status it writes is worse: `chrono_now()` (`:358-365`) emits
`"1786483351Z"`, which is not RFC 3339 and not ISO 8601 despite its own comment; and because the
value is recomputed unconditionally on every pass, `lastTransitionTime` marks *the last
reconcile*, not the last transition. A third defect sits alongside: conditions are rebuilt from
nothing each pass, so entries **vanish** instead of flipping — on **three** code paths, not the
one revision 1 named.

**FR8 — the env plane never got the ADR-0001 treatment.** `config_gen.rs` is exemplary: it
populates real `frogdb_config` section structs and says why in its module doc ("any future
server-side rename/addition becomes a **compile error** here"). Twelve lines away,
`statefulset.rs:79-133` hand-writes **seven `FROGDB_*` variable names** — four as `EnvVar`
literals (`:80,85,90,105`), three as `export` lines inside a shell-script string
(`:124,126,127,128`) — none of which any compiler, test or lint connects to the schema they name.
All seven **names** are correct today. One of the seven **values** is not, and that is 77-H1.
The design consequence is stated in §6 and is the single most important amendment in this
revision: **a name-only seam would have shipped the P0 unchanged.**

The other live half of FR8: `spec.cluster.electionTimeoutMs`, `heartbeatIntervalMs` and
`autoFailover` (`crd.rs:262-278`) are published in the generated CRD
(`deploy/crd.json:53-57` and `:66-79`), documented, defaulted — and read by **nothing in the
reconcile path**. The shipped example manifest sets `autoFailover: true`
(`deploy/examples/cluster.yaml:27`). An operator who applies the file FrogDB ships gets automatic
failover **off** (`ClusterConfigSection::default()` → `auto_failover: false`, `cluster.rs:172`).

## Files involved

Whole-file line counts, verified at HEAD `54baa2bb`.

| File | Lines | Role in this proposal |
|---|---|---|
| `frogdb-operator/src/resources/statefulset.rs` | 256 | **Primary. Carries the P0** at `:104-108` (`initial_nodes.join(",")`). Four `EnvVar` name literals `:80,85,90,105`; four shell `export` lines `:124,126,127,128`; the mode match `:69-143`; owner ref `:222`. |
| `frogdb-operator/src/controller.rs` | 365 | **Primary, both candidates.** Five apply blocks (`:96-105`, `:107-116`, `:118-126`, `:154-162`, `:167-173`); `condition()` `:348-356`; `chrono_now()` `:358-365`; `update_status` `:318-346` (merge patch `:341-343`); condition assembly `:180-214`; validation early return `:72-90`; the `.owns` chain `:49-51` inside the builder block `:48-52`. **Zero tests** (no `#[cfg(test)]` in the file). |
| `frogdb-operator/src/resources/mod.rs` | 22 | **Primary (FR7).** Destination for `apply_child`, `FIELD_MANAGER`, `owner_ref` — today only `standard_labels` `:10-19` and one const `:22`. |
| `frogdb-operator/src/config_gen.rs` | 161 | **Primary (FR8).** `generate_toml` `:39-85` emits **exactly five** sections — `[server] [logging] [persistence] [metrics] [memory]`; `cluster_env_toml` `:88-107` is `#[allow(dead_code)]` with **two** tests total (`:151-160` here, `integration.rs:210-219` there). |
| `frogdb-operator/src/resources/configmap.rs` | 70 | `owner_ref_from` `:39-50` — reached as `super::configmap::owner_ref_from` from three sibling modules; moves to `mod.rs`. `config_hash` `:32-36`. |
| `frogdb-operator/src/resources/service.rs` | 85 | **Two** builders — `build_headless` `:9`, `build_client` `:61`; owner refs `:47`, `:71`. Two of the five apply sites (`controller.rs:107-116`, `:118-126`). |
| `frogdb-operator/src/resources/pdb.rs` | 39 | `build -> Option<PodDisruptionBudget>` `:12-39`; owner ref `:26`. Applied under `if let Some` at `controller.rs:165-174`. |
| `frogdb-operator/src/resources/servicemonitor.rs` | 48 | `#[allow(dead_code)]` `:10`; returns `Option<serde_json::Value>` `:11`, **not** a `DynamicObject`. Owner reference retyped by hand as JSON `:28-35`. Only reference in the tree: `pub mod servicemonitor;` at `resources/mod.rs:6`. |
| `frogdb-operator/src/crd.rs` | 566 | `ClusterSpec` `:262-278` (three unread fields), defaults `:284-290`; `FrogDBCondition` `:409-430`, `last_transition_time: Option<String>` `:421`. |
| `frogdb-operator/src/health.rs` | 49 | `check_pod_health` `:7-20` — `#[allow(dead_code)]`, **zero callers confirmed** (`grep -rn check_pod_health frogdb-operator/` → the definition only). `probe_pod_is_primary` `:29-49`: `/admin/role` URL at `:30`, `master`/`slave` parse at `:44-47`, 5 s timeout `:34`. |
| `frogdb-operator/deploy/crd.json` | 406 | **Generated** (`just operator-crd`, `Justfile:961-963`). `lastTransitionTime` schema `:317-321` — bare `"type": "string"`, no `format`. Dead cluster knobs `:53-57` (`autoFailover`) and `:66-79` (`electionTimeoutMs`, `heartbeatIntervalMs`). `observedGeneration` appears **once**, `:354`, at status top level. |
| `frogdb-operator/deploy/examples/cluster.yaml` | 39 | `autoFailover: true` `:27` — the shipped manifest whose value is dropped. |
| `frogdb-operator/deploy/helm/frogdb-operator/templates/clusterrole.yaml` | 36 | `servicemonitors` RBAC `:32-35`, granted for a resource never created. |
| `frogdb-operator/CONTEXT.md` | 99 | `:63-64` claims the CR owns an "optional ServiceMonitor". Domain vocabulary source; `:95-97` records the `autoFinalize` precedent. |
| `frogdb-operator/tests/resource_builders.rs` | 642 | **38** pure builder tests. Eleven `FROGDB_*__*` literals at `:85,86,102,103,114,117,118,127,141,153,154`; `cluster_mode_env_vars` `:107-119` asserts a literal against the same literal. No `servicemonitor` module. |
| `frogdb-operator/tests/integration.rs` | 701 | Real-server tests. `:35-44` maps `spec.cluster.{election,heartbeat}` into a live `ClusterNodeConfig` — a wiring the operator itself does not have. `:210-219` tests dead `cluster_env_toml`. Uses `frogdb-test-harness` (`Cargo.toml:38`). |
| `frogdb-operator/Cargo.toml` | 40 | `frogdb-config` path dep `:18`; `k8s-openapi = { version = "0.27", features = ["v1_33"] }` `:20`; `frogdb-test-harness` dev-dep `:38`; **`[workspace]` `:40`** — the structural fact. |
| `frogdb-server/crates/server/src/config/loader.rs` | 480 | `Env::prefixed("FROGDB_").split("__").map(closure)` `:99-105`; `figment.extract()` `:128` — where the P0 lands. |
| `frogdb-server/crates/config/src/cluster.rs` | ~270 | `ClusterConfigSection` `:17-113`, `rename_all = "kebab-case"` `:16`, **no** `deny_unknown_fields`; `initial_nodes: Vec<String>` `:46`; `Default::default()` `:159-177` with `auto_failover: false` `:172`. |
| `frogdb-server/crates/config/src/lib.rs` | — | `Config` carries `#[serde(deny_unknown_fields, rename_all = "kebab-case")]` `:83`. Load-bearing for §5's silent/loud distinction. |
| `frogdb-server/crates/config/Cargo.toml` | 23 | Already has `serde_json` `:16` and `toml` `:18` — the new `env.rs` needs **no new dependency**. |
| `frogdb-server/crates/config/src/env.rs` | *new, ~80* | Destination for the env grammar: names **and values**, both directions, round-trip tested. |
| `frogdb-server/ops/deploy/helm/frogdb/templates/statefulset.yaml` | — | `:66-73` — the same four `FROGDB_CLUSTER__*` names; `:72-73` carries the **same P0**. |
| `adr/0001-operator-imports-server-config-crate.md` | 7 | Amended. **Shared with proposal 72** — see *Sibling edges*. |

**Not in this file set** (deliberate): `frogdb-server/ops/helm/**` (the second, apparently stale
Helm tree), `frogdb-server/docker/Dockerfile*`, `frogdb-operator/src/{main,lib,telemetry,testing}.rs`.
The Helm P0 is in scope for the **hotfix** (77-H1) but the Helm *seam* is not — see §5.

## Problem

### 1. The workspace nothing gates

Stated in the Summary; restated here as the finding it is, because everything below is an
instance of it.

* `Cargo.toml:3-40` lists 36 members. `frogdb-operator` is not one; `frogdb-operator/Cargo.toml:40`
  declares `[workspace]`, making it a root of its own.
* Consequence: `just check`, `just lint`, `just test` — and therefore `cargo clippy`, `cargo
  nextest`, the mutation harness, `just lint-gates`' compile-dependent members, and every agent's
  default verification loop — **never touch it**.
* The only gates: `just operator-build`, `just operator-test`, `just operator-crd`
  (`Justfile:961-971`), plus the path-filtered `operator-tests` CI job (`test.yml:245-269`).
* The CI filter is better than it looks — `operator_config` (`:56-59`) includes
  `frogdb-server/crates/config/**`, `config-derive/**` and `frogdb-operator/Cargo.lock`, so a
  server-side config **type** change does re-run the operator job. But a change that breaks only a
  **string**, or that is correct at compile time and wrong on the wire, is caught by nothing, in
  any workspace, ever. That is the exact shape of §2.

Three of this document's findings — the dead ServiceMonitor (§4), the three dead CRD knobs (§6),
and the P0 (§2) — are the **same** root cause with three faces. A reviewer weighing whether the
seams below are worth their cost should weigh this first: the seams are how the class is caught
next time, given that the workspace itself will remain outside the default loop until someone
decides otherwise.

### 2. `FROGDB_CLUSTER__INITIAL_NODES` cannot be decoded — LIVE, P0

```rust
// statefulset.rs:96-108
let initial_nodes: Vec<String> = (0..spec.replicas)
    .map(|i| format!("{name}-{i}.{name}-headless.{ns}.svc.cluster.local:{}", cluster.bus_port))
    .collect();
env.push(EnvVar {
    name: "FROGDB_CLUSTER__INITIAL_NODES".into(),
    value: Some(initial_nodes.join(",")),   // <- :106
    ..Default::default()
});
```

The receiving field is `pub initial_nodes: Vec<String>` (`cluster.rs:46`). The path from env var
to that field is `Env::prefixed("FROGDB_").split("__").map(..)` (`loader.rs:99-105`) into
`figment.extract()` (`loader.rs:128`).

**Why it fails.** figment parses each env value with its own micro-parser before deserialization
(`providers/env.rs:614-621` → `Value::from_str` → `value/parse.rs:68-96`). A sequence is produced
by exactly one branch — `peek('[') => Value::from(array()?)` (`parse.rs:74`) — and `array` is
`delimited_collect('[', value, ',', ']')` (`:58-59`). There is no comma-splitting fallback: the
default arm returns `Value::from(value.to_string())`. figment's own `Env` doc-example makes the
contract explicit (`NUMBERS = "[1, 2, 3]"`). `value/de.rs` offers no string→seq coercion
(`Value::String(_, ref s) => v.visit_str(s)`, `:81`), so serde's `Vec<String>` visitor rejects it.

**Empirically confirmed** against pinned `figment = "0.10.19"` replicating `loader.rs:99-105`
verbatim:

```
FAIL  "a:1,b:2"   -> invalid type: found string "a:1,b:2", expected a sequence
                     for key "CLUSTER.INITIAL-NODES" in `FROGDB_` environment variable(s)
FAIL  "a:1"       -> invalid type: found string "a:1", expected a sequence …
OK    "[a:1,b:2]" -> ["a:1", "b:2"]
OK    "[a:1]"     -> ["a:1"]
```

Note the second line: **even a single node with no comma fails**, because the failure is a type
mismatch, not a delimiter mismatch. A one-node cluster is as dead as a five-node one.

**Blast radius.** `figment.extract()` is fallible and `?`-propagated at `loader.rs:128`
(`.context("Failed to load configuration")`), so the server exits during config load, before
binding a port. Every FrogDB CR with `mode: cluster` produces a StatefulSet whose pods
CrashLoopBackOff on first boot. The Helm-managed path carries the identical bug
(`ops/deploy/helm/frogdb/templates/statefulset.yaml:72-73`, value
`{{ include "frogdb.clusterPeers" . }}`, which ends `{{- join "," $peers -}}` at
`_helpers.tpl:86`).

**Why nothing caught it.** The operator's own tests assert the *string shape* only
(`resource_builders.rs:127,141,153-154` read the joined value back and count commas) — they never
feed it to a loader. The server-side tests that exercise `initial_nodes` set it **in-process**:
`test-harness/src/server.rs:570-572` assigns `config.cluster.initial_nodes = initial_nodes`
directly, bypassing the env decoder. So the two halves of the contract are each tested against
themselves and never against each other. Add §1's "no gate reaches this workspace" and the
defect has no surface at which to be observed.

**Fix (77-H1), ~3 lines.** `format!("[{}]", initial_nodes.join(","))` at `statefulset.rs:106`,
plus the two Helm templates. The regression test must go **through the real loader** — set the
env var, run `Config::load` (or the same figment chain), assert the parsed `Vec<String>`. A test
that asserts the string starts with `[` re-creates the tautology that hid this.

### 3. `chrono_now` — LIVE on the wire, hypothetical in blast radius

```rust
// controller.rs:358-365
fn chrono_now() -> String {
    // ISO 8601 format without chrono dependency
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    format!("{}Z", now)
}
```

Output: `"1786483351Z"`. The comment is wrong twice — that is not ISO 8601, and the operator
already links a date-time library (see 77-H2). A conformant value is RFC 3339 at second
precision, UTC: `"2026-08-10T18:22:31Z"`.

**Why nothing rejects it.** `FrogDBCondition::last_transition_time` is `Option<String>`
(`crd.rs:421`), so the generated schema is a bare string with no `format` keyword
(`deploy/crd.json:317-321`). apiextensions validates `format: date-time` only when declared;
here it is not, so the API server stores the value verbatim and every write succeeds.

**Blast radius — corrected downward from revision 1.** Revision 1 said clients "fail
`metav1.Time`'s unmarshal and drop the whole status object". That is what *would* happen, and
**no such consumer exists in this repository**. Verified: the operator's own type is
`FrogDBCondition` with `Option<String>` fields (`crd.rs:409-430`); every test that touches the
value string-matches it; nothing in the tree decodes `status.conditions` into
`[]metav1.Condition`. The realistic victims are third parties — client-go's
`apimeta.FindStatusCondition`/`meta.SetStatusCondition`, `kubectl` jsonpath and column printers
that expect a date, ArgoCD/Flux health assessors. **Ruling: LIVE — the wire format is wrong and
the CRD is complicit — with a hypothetical in-repo blast radius.** The fix is three lines and
carries no risk, so the downgrade changes the *justification*, not the *disposition*: it still
ships as 77-H2. It is no longer the document's lead.

### 4. Condition churn and vanishing conditions — LIVE, and worse than revision 1 said

Two defects share one root cause: the condition array is **rebuilt from nothing on every pass**
(`controller.rs:180-214`) and written with a merge patch that replaces the list wholesale
(`update_status:341-343`, `Patch::Merge`).

**(a) `lastTransitionTime` means "last reconcile".** `condition()` (`:348-356`) calls
`chrono_now()` unconditionally on construction; `reconcile` constructs its conditions
unconditionally; the merge patch replaces the array. The field's entire contract is "when did
this condition last change status", and it answers "when did the controller last run".

**Amplification — bounded, not infinite (corrected).** The `Controller` watches `FrogDB` with a
default `WatcherConfig` and **no** `predicate_filter(predicates::generation)` (`:48`), so the
controller's own status write is an input event. The loop is:

1. requeue fires at T+30 s → reconcile → patch writes `lastTransitionTime = "T30"`;
2. the object changed → watch event → reconcile;
3. that second pass computes the **same whole-second string** → the patch is byte-identical → the
   API server records no change → no event. **The loop terminates.**

So steady state is **two reconciles per 30 s tick**, not a runaway. Revision 1's claim that a
pass spanning a second boundary makes it continuous is directionally right and worth keeping as a
degradation note — `detect_primary_pod` (`:278-314`) probes pods sequentially with a 5 s per-pod
timeout (`health.rs:34`), building a fresh `reqwest::Client` each time, so a standalone
deployment with unreachable replicas can take tens of seconds per pass and write a different
timestamp every time — but the *normal* case is a bounded doubling. Fixing (a) collapses it to
one reconcile per tick, which is the honest claim: **~2× fewer reconciles, not "unbounded churn
eliminated"**.

**(b) Conditions vanish instead of flipping — three paths, not one.** Conditions are a keyed set;
entries are flipped to `False`, never deleted. All three assembly paths delete:

* `:195-206` — while an upgrade runs, `Upgrading=True` is pushed and **`Progressing` is not
  emitted at all**, so a client watching `Progressing` sees it disappear;
* `:207-213` — when the upgrade finishes, `Progressing` returns and **`Upgrading` disappears**
  (the case revision 1 named);
* **`:72-90` — the validation early return**, missed by revision 1 and the worst of the three: on
  an invalid spec it writes a **one-element** array `vec![condition("Available","False",
  "ValidationFailed",&e)]`, deleting **both** `Upgrading` and `Progressing` in a single write,
  mid-upgrade, at exactly the moment an operator is watching.

**(c) A convention inversion in the same block.** `:207-213` emits
`Progressing = "False"` with reason `ReconcileComplete` when reconciliation **succeeds**. The
Kubernetes convention is the opposite: a `Deployment` that has finished rolling reports
`Progressing=True` with reason `NewReplicaSetAvailable`; `Progressing=False` means *stalled*.
Any generic tooling that reads this — and generic tooling is the entire reason to use the
standard condition types — reads a healthy FrogDB as stalled. Fold the inversion into the same
fix: success is `Progressing=True/NewReplicaSetAvailable`, failure and mid-roll are the `False`
and `True`-with-`RollingUpgrade` cases respectively. This is a **status-semantics change** and
belongs in release notes alongside the merge.

**(d) A CRD-level fix that is cheaper than the Rust one.** The array is replaced wholesale
because JSON-merge-patch has no notion of list identity. Declaring the list keyed —
`x-kubernetes-list-type: map` with `x-kubernetes-list-map-keys: [type]` on
`status.conditions` — makes the **API server** merge by key, which is what every built-in
condition list does. That single schema change removes the "wholesale replace" half of the root
cause for free, server-side, for all writers including ones that are not this controller. It
does **not** replace the Rust merge (the controller still must not stamp `lastTransitionTime`
when nothing changed), but it makes the Rust merge simpler and it fixes (b) even if the Rust
change regresses. `schemars` attribute on the `conditions` field, then `just operator-crd`.
**Strongly recommended to land in the same change as the merge.**

**(e) Conditions carry no `observedGeneration`.** `observedGeneration` appears exactly once in the
generated CRD (`crd.json:354`), at the status top level. Per-condition `observedGeneration` is
part of `metav1.Condition` and is what lets a client tell "Available=True, and that verdict is
about the spec you just applied" from "Available=True, stale". Adding it to `FrogDBCondition` is
one field plus one assignment from `frogdb.metadata.generation`, in the same pass that already
computes it for the status top level. Cheap, same code path, take it.

### 5. ServiceMonitor — the brief has this backwards

The lane brief asks whether the ServiceMonitor is "emitted without checking the CRD exists". It
is not emitted at all. `servicemonitor::build` is `#[allow(dead_code)]` (`:10`), returns
`Option<serde_json::Value>` (`:11` — **not** a `DynamicObject`), and its only reference in the
tree is `pub mod servicemonitor;` at `resources/mod.rs:6`. `reconcile` never mentions it.
`resource_builders.rs` has no `servicemonitor` module — 38 builder tests, none for this one.

What *is* live is everything around it:

* `CONTEXT.md:63-64` states the CR "owns its StatefulSet, headless + client Services, ConfigMap,
  PDB, and optional ServiceMonitor via owner references." Four of five are true.
* `clusterrole.yaml:32-35` grants `monitoring.coreos.com/servicemonitors`
  `get,list,watch,create,update,patch,delete` — standing write permission for a resource the
  operator never touches. That is a least-privilege violation as well as a documentation one.

Ruling: **LIVE as a documented-and-provisioned capability that does not exist; latent as code.**

The gate the brief asks about is a *precondition for wiring it*, not a current bug: applying a
`monitoring.coreos.com/v1` object to a cluster without the Prometheus Operator CRDs returns
`404 NotFound`, and through the `?` at the end of the apply ritual that fails the whole reconcile,
so one missing optional CRD would strand **every** FrogDB CR. Wiring without the gate is strictly
worse than the dead code.

**Scope decision (changed from revision 1).** Revision 1 proposed wiring the ServiceMonitor as
part of FR7. That is now **out of scope** — see §B4 in the ledger for the type-level reason. The
disposition here is documentation and RBAC only (77-H5): either correct `CONTEXT.md:63-64` **and**
delete the `clusterrole.yaml:32-35` grant, or wire the builder properly as its own piece of work
with a discovery gate. Claiming a capability and granting permissions for it while shipping
neither is the worst of the three states.

One more pure-dead item in the same file set: `health::check_pod_health` (`:7-20`),
`#[allow(dead_code)]`, zero callers (verified).

### 6. FR8 — seven env names the schema does not know about

`config_gen.rs:1-8` states the ADR-0001 rule in its own module doc and follows it.
`statefulset.rs` sits in the same crate and does not:

| Site | Literal | Resolves to | Name | Value |
|---|---|---|---|---|
| `:80` | `FROGDB_CLUSTER__ENABLED` | `cluster.enabled` (`cluster.rs:21`) | correct | `"true"` — ok |
| `:85` | `FROGDB_CLUSTER__CLUSTER_BUS_ADDR` | `cluster.cluster-bus-addr` (`:40`) | correct | `$(POD_IP):…` — ok (String) |
| `:90` | `FROGDB_CLUSTER__CLIENT_ADDR` | `cluster.client-addr` (`:34`) | correct | `$(POD_IP):…` — ok (String) |
| `:105` | `FROGDB_CLUSTER__INITIAL_NODES` | `cluster.initial-nodes` (`:46`) | correct | **BROKEN — §2** |
| `:124`, `:126` | `FROGDB_REPLICATION__ROLE` | `replication.role` (`replication.rs:20`) | correct | ok |
| `:127` | `FROGDB_REPLICATION__PRIMARY_HOST` | `replication.primary-host` (`:26`) | correct | ok |
| `:128` | `FROGDB_REPLICATION__PRIMARY_PORT` | `replication.primary-port` (`:31`) | correct | ok |

**Seven for seven on names. Six for seven on values.** That distinction is the whole design
lesson of this revision and is stated as a requirement in §Proposed change: a seam that maps
`(section, field) → NAME` and stops there **would have shipped the P0 unchanged**, because the
dangerous half of an env override is the **value encoding**, not the name. The seam must own
both, and its round-trip test must assert the **value** arrives, not just that the key does.

Latent-on-names is still not harmless, for three reasons.

* **Nothing would catch the eighth name.** The only "test" is
  `resource_builders.rs:107-119`, which asserts `env_value(c, "FROGDB_CLUSTER__ENABLED")` is
  `Some("true")` — a literal compared against the same literal. It passes for any name, including
  one the server ignores.
* **Silence is field-scoped, not global (corrected).** Revision 1 said a typo is silently
  ignored. Precisely: `Config` **does** carry `#[serde(deny_unknown_fields)]`
  (`config/src/lib.rs:83`), so an unknown **section** is **loud** —
  `FROGDB_CLUSTR__ENABLED` produces `clustr.enabled` and `extract()` errors at startup. But
  `ClusterConfigSection` carries only `rename_all` and **no** `deny_unknown_fields`
  (`cluster.rs:16`), so an unknown **field inside a known section** is **silent** —
  `FROGDB_CLUSTER__AUTO_FAILOVR` is dropped and the pod boots happily with the wrong topology.
  The dangerous half is the field typo; the section typo is self-announcing. (Adding
  `deny_unknown_fields` to the section structs is a tempting adjacent fix and is **not** proposed
  here: it changes server startup behaviour for every existing deployment carrying a stale key,
  and it belongs to whoever owns the config crate's compatibility policy.)
* **Three of the names live in shell text.** `:122-132` is a `format!`-built `/bin/sh -c` script.
  Those `export` lines are not `EnvVar` values, not typed, not greppable as a pair with anything.
  (Not an injection risk — `{name}` and `{ns}` are DNS-1123 labels — but the deepest form of the
  problem: config expressed as a string with no schema behind it.)

### 7. FR8 LIVE — three CRD fields that reach nothing

`ClusterSpec` (`crd.rs:262-278`) has four fields. `bus_port` is read (`statefulset.rs:74,86,100`,
`service.rs:33`). The other three are read by:

* `config_gen::cluster_env_toml` (`:88-107`) — `#[allow(dead_code)]`, zero non-test callers;
* `testing.rs:48-49` — a test fixture;
* `integration.rs:35-44` — which maps them into a live `ClusterNodeConfig` and stands up real
  servers with them, **asserting a wiring the operator does not have**;
* `config_gen.rs:151-160` and `integration.rs:210-219` — the **two** tests of the dead function.

`generate_toml` (`:39-85`) emits exactly `[server] [logging] [persistence] [metrics] [memory]` —
no `[cluster]`. The StatefulSet pushes exactly four `FROGDB_CLUSTER__*` vars, none of them these
three. So nothing in `reconcile`, `statefulset::build`, `generate_toml` or the ConfigMap touches
them. They are published to users in the generated CRD (`crd.json:53-57`, `:66-79`) with
descriptions and defaults, and the shipped example sets one:

```yaml
# frogdb-operator/deploy/examples/cluster.yaml:23-27
  cluster:
    busPort: 16379
    electionTimeoutMs: 1000
    heartbeatIntervalMs: 250
    autoFailover: true
```

Apply FrogDB's own example and `cluster.auto-failover` is `false` on every pod:
`ClusterConfigSection::default()` sets `auto_failover: false` (`cluster.rs:172`) and nothing
overrides it. `electionTimeoutMs`/`heartbeatIntervalMs` happen to match the server defaults
(`crd.rs:284-290` vs `cluster.rs:116-117`), so only non-default tunings are lost there. **LIVE.**

This is the same defect class the operator's own `CONTEXT.md:95-97` records having found and
fixed once already, for `spec.upgrade.autoFinalize`. The tests make it worse rather than better:
two tests assert `cluster_env_toml` produces correct TOML. They are true and they are certifying
a function no deployment executes.

## Proposed change

Four pieces. The first is a three-line hotfix and is not really "change" at all; the rest are in
dependency order.

### 0. 77-H1 — encode the list (see §Effort)

Not a design change. Listed here only so the sequence is unambiguous: **nothing else in this
document lands before the bracket fix**, because everything else is polish on a workspace whose
cluster mode does not boot.

### 1. `frogdb-config::env` — the env grammar, names **and values**

New `frogdb-server/crates/config/src/env.rs`:

```
pub const PREFIX: &str = "FROGDB_";

/// ("cluster","auto-failover") -> "FROGDB_CLUSTER__AUTO_FAILOVER"
pub fn var_name(section: &str, field: &str) -> String;

/// The value encoding figment's env provider accepts, for one serde value.
/// Scalars pass through; sequences are bracketed; this is where the P0 lives.
pub fn encode_value<T: Serialize>(value: &T) -> String;

/// Convenience for the common case: `encode_value` over a list.
pub fn env_list<I: IntoIterator<Item = impl AsRef<str>>>(items: I) -> String;

/// A whole section as (NAME, VALUE) pairs, both halves derived.
pub fn overrides<S: Serialize>(section: &str, value: &S) -> Vec<(String, String)>;
```

**`encode_value`/`env_list` are the amendment.** Revision 1's seam was `var_name` +
`figment_key` — names only. A name-only seam would have produced
`("FROGDB_CLUSTER__INITIAL_NODES", "a,b")` and shipped the P0 verbatim. Value encoding is the
half that fails silently and the half no test covers, so it is the half the seam must own.
Correspondingly, the round-trip test (§Testability) asserts the **value arrives at the typed
field**, not that the key was spelled right.

Placeholders like `$(POD_IP):16379` pass through untouched: `encode_value` on a `String` is
identity (figment's parser returns `Value::String` for it, which is what the field wants), and
`overrides` derives *names* from serde idents while copying *values*, so a deferred value stays a
deferred value.

**The inverse direction — amended, one option to pick.** Revision 1 specified
`figment_key(env_key)` as "`loader.rs:100-104` moved verbatim" **and** as the inverse of
`var_name`. It cannot be both, and the round-trip test as written would have been vacuous.
Reason, empirically confirmed: `Env::split(pattern)` is implemented as
`self.map(|key| key.replace(pattern, "."))` (`figment/providers/env.rs:409-412`), and `map`
chains as `move |key| f(filter_map(key))` (`:132-142`), so **`split` runs before the loader's
closure**. Probe output, printing what the closure actually receives for
`FROGDB_DEBUG_BUNDLE__ENABLED`:

```
closure sees: "DEBUG_BUNDLE.ENABLED"
```

The `__` is already a `.`. So the closure's `replace("__", "\x00")` / `replace('\x00', "__")`
protection is **dead code in production** — it matches nothing — and a `figment_key` "moved
verbatim" would operate on a domain that never contains what it guards against.

Pick **one** of:

* **(i) Segment-scoped.** `figment_key` is specified over an **already-split segment** (input
  `DEBUG_BUNDLE.ENABLED`, output `debug-bundle.enabled`); the `\x00` dance is deleted as the dead
  code it is; the round-trip test is stated **over segments** —
  `for (s, f) in Config::sections_and_fields(): figment_key(split_form(var_name(s, f))) == "s.f"`.
  Smaller change, keeps figment's `split` doing the splitting.
* **(ii) Whole-key.** Replace `Env::prefixed("FROGDB_").split("__").map(closure)` with
  `Env::prefixed("FROGDB_").map(full_inverse)`, where `full_inverse` owns **both** steps
  (`__` → `.`, `_` → `-`) and is exactly `figment_key`. Then `figment_key` is a true inverse of
  `var_name`, the round-trip test is the obvious one, and the grammar lives in one function
  instead of being split across figment's combinator and ours.

**(ii) is recommended** — it is the version that makes the round-trip test mean what it says, and
it removes a subtlety (mapper ordering) that already produced dead code once. It is a behaviour
change to the loader only in that it can no longer diverge from `var_name`, which is the point.

**Deletion test.** After the move, `loader.rs:99-105`'s closure body is gone and the literal
`"FROGDB_"` appears in exactly one Rust file in either workspace.

**Seam lint — narrowed (amended).** Revision 1 proposed `lint-env-name-seam` as "no `\"FROGDB_`
string literal outside `config/src/env.rs`". Measured at HEAD, that pattern hits **61** lines
across 14 files, almost all unrelated: `FROGDB_SNAPSHOT_COMPLETE_v1`
(`persistence/src/snapshot/metadata.rs:7`), `FROGDB_CHECKPOINT`/`FROGDB_SNAPSHOT`
(`replication/src/fullsync.rs:110,122`), `FROGDB_FLAME_OUTPUT` (`server/src/main.rs:106`), the
`FROGDB_*` OTel attribute consts (`telemetry/src/tracing.rs:142-144`). Unusable.

Narrow to the **env-var construction shape**: `FROGDB_[A-Z0-9]+__` — the double underscore is the
section separator and appears nowhere else. That yields **19** hits at HEAD:

* 8 in `frogdb-operator/src/resources/statefulset.rs` — the production sites the lint exists to
  catch;
* 11 in `frogdb-operator/tests/resource_builders.rs` (`:85,86,102,103,114,117,118,127,141,153,154`)
  — false positives on day one;
* 1 comment in `frogdb-server/crates/server/src/server/cluster_init.rs:489`.

So the lint is viable but **not free**: it needs either a test-file exclusion plus a one-line
comment allowlist, or the 11 test assertions migrated to `env::var_name(..)`. Migration is the
better answer (a rename then moves both sides together, which is the point of the seam) and it is
**budgeted into FR8's effort** rather than assumed away. Scope the lint to Rust sources only —
Go templates and Dockerfiles have no seam to route through (§Helm copy).

It joins the compile-free family in `lint-gates` (`Justfile:329`) and would be the **first gate in
the family to police the operator at all** — which, given §1, is a larger fact than the lint
itself.

### 2. `apply_child` — a free function, no trait

**Amended: revision 1's `ChildResource` trait is withdrawn.** The evidence against it:

* **It cannot hold the shape it was given.** `fn build(frogdb, input) -> Option<Self>`, one impl
  per type, cannot express `Service`, which has **two** children from two builders
  (`service.rs:9` `build_headless`, `service.rs:61` `build_client`, applied at
  `controller.rs:107-116` and `:118-126`).
* **`apply_child` would never call `build`.** `reconcile` already holds each built object when it
  applies it. Six impl blocks whose only method the chokepoint never invokes are ceremony.
* **The bound excludes the one case it was for.** `Resource<DynamicType = ()>` structurally
  excludes `DynamicObject`: `impl Resource for DynamicObject { type DynamicType = ApiResource; }`
  (`kube-core-3.1.0/src/dynamic.rs:78-79`). And `Api::namespaced` requires
  `K::DynamicType: Default` (`kube-client-3.1/src/api/mod.rs:138,187`), which `ApiResource` does
  not satisfy — dynamic kinds need `Api::namespaced_with` (`:93`) and an explicit `ApiResource`.
  So the trait as specified contradicts the ServiceMonitor plan it was justified by.

The chokepoint is worth having; the trait is not. Replace it with a free function in
`resources/mod.rs`:

```rust
pub const FIELD_MANAGER: &str = "frogdb-operator";

pub async fn apply_child<K>(api: &Api<K>, child: &K) -> Result<(), Error>
where
    K: Resource<DynamicType = ()> + Serialize + DeserializeOwned + Clone + Debug,
{
    api.patch(
        &child.name_any(),
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(child),
    )
    .await
    .map_err(|e| Error::Apply { kind: K::kind(&()).into_owned(), name: child.name_any(), source: e })?;
    Ok(())
}
```

Five call sites; the PDB keeps its `if let Some(pdb)` (`controller.rs:165`). Net **≈ −30 / +12**
lines, and — unlike the trait — it is a real reduction rather than a relocation. It owns exactly
the decisions that are currently restated five times or not at all:

* the field manager string (five copies today);
* `Patch::Apply` **with `.force()`** (zero copies today — see §Behaviour changes);
* the name, from `ResourceExt::name_any()`, so the five `.unwrap()`s in a reconcile loop go;
* the error context (`kind` + `name`), which today's bare `?` loses.

`DynamicObject` support is simply **not needed**, because ServiceMonitor is out of FR7's scope
(§5). If it is ever wired, the honest move is a second function `apply_dynamic_child(api,
&ApiResource, &DynamicObject)` built on `Api::namespaced_with`, or a `dyn_type()` accessor and
`namespaced_with` used uniformly — a decision to make **when** the ServiceMonitor is wired, with
the 404-tolerance rule in hand, not speculatively now.

`owner_ref` still moves from `configmap.rs:39-50` to `mod.rs` as a free function next to
`standard_labels`, and `super::configmap::` disappears from three modules. That is **locality**:
the fact belongs to all children, not to the ConfigMap.

**Vocabulary, stated plainly.** `apply_child` is the **chokepoint** every child write goes
through; `owner_ref` in `mod.rs` is **locality**. There is no interface and no adapter here any
more — withdrawing them is the correction.

**The `.owns` drift is not fixed by any of this** and is promoted to its own finding — see §3
below and hotfix 77-H4.

### 3. `.owns(pdb)` — a live gap in its own right

`controller.rs:49-51` watches `StatefulSet`, `Service`, `ConfigMap`. `controller.rs:165-174`
applies a `PodDisruptionBudget`. Delete the PDB out from under a running FrogDB and **nothing
notices until the 30 s requeue** — the CR's own guard against voluntary disruption is absent for
up to half a minute after a `kubectl delete pdb`, with no event and no log. Small blast radius,
trivially fixed (two lines plus an `Api<PodDisruptionBudget>` binding), and exactly the kind of
asymmetry a hand-maintained pair of lists produces. **77-H4.**

Revision 1 buried this inside the trait's motivation and then noted the trait does not fix it.
Both halves were right; the packaging was wrong.

**On the parity test** (revision 1's §Testability item 4): a test comparing the applied-kind list
against the watched-kind list would be a **third** hand-maintained list of the same fact, and it
would drift the same way the first two did. Softened to a suggestion rather than a deliverable:
if it is written, it should derive at least one side from something structural, and if it cannot,
it is not clearly better than the two-line fix plus a comment at `:49`. `Controller::owns` needs a
concrete type at compile time and cannot be driven from a runtime list without a macro, which is
more cleverness than this earns.

### 4. Status: merge conditions, key the list, stop inventing timestamps

`condition()` stops calling the clock. A new pure function does the merge:

```rust
/// `previous` is `frogdb.status.as_ref().map(|s| &s.conditions)` — already in hand.
fn merge_conditions(previous: &[FrogDBCondition], desired: Vec<FrogDBCondition>, now: &str)
    -> Vec<FrogDBCondition>;
```

Rules, matching the Kubernetes condition convention: conditions are keyed by `type`; an entry
whose `status` is unchanged keeps its previous `lastTransitionTime`; a changed `status` takes
`now`; a type present before and absent from `desired` is **retained with its status flipped**,
not dropped. `now` is a parameter, so the function is pure and the whole status assembly becomes
testable without a `kube::Client`.

**Applied at all three sites** (§4b): the two upgrade branches at `:195-213` **and** the
validation early return at `:72-90`. The early return is the one that must not be forgotten — it
is a one-element array today and deletes two conditions at once.

**Plus the CRD-level fix** (§4d): `x-kubernetes-list-type: map` +
`x-kubernetes-list-map-keys: [type]` on `status.conditions`, so the API server merges by key even
for writers that are not this controller. Schema change → regenerate with `just operator-crd`.

**Plus the convention correction** (§4c): success reports `Progressing=True` /
`NewReplicaSetAvailable`, not `Progressing=False` / `ReconcileComplete`.

**Plus per-condition `observedGeneration`** (§4e): one field on `FrogDBCondition`, assigned from
`frogdb.metadata.generation` in the same pass that already fills the status top-level copy.

Once `lastTransitionTime` is stable across a steady-state reconcile, the merge patch at `:341-343`
becomes a genuine no-op and `Action::requeue(30s)` becomes the actual reconcile rate rather than a
floor the code doubles.

`chrono_now` is replaced by 77-H2's formatter. The **durable** form types the field as
`k8s_openapi::apimachinery::pkg::apis::meta::v1::Time` — which is
`Time(pub jiff::Timestamp)` (`k8s-openapi-0.27.1/.../meta/v1/time.rs:5`) and serializes correctly
by construction — but that needs k8s-openapi's `schemars` feature (`Cargo.toml:20` declares only
`v1_33`) and **changes the generated CRD**, so it ships with this proposal and re-runs
`just operator-crd`, never as a hotfix.

### 5. FR8 applied: static config to the ConfigMap, per-pod config through the seam

The split the operator makes accidentally today should be stated deliberately:
**identical-for-every-pod config goes in the ConfigMap; per-pod or pod-IP-derived config goes in
env.** Under that rule:

* `election-timeout-ms`, `heartbeat-interval-ms`, `auto-failover` and `enabled` are static → they
  join `generate_toml` as a `[cluster]` section, populated from `ClusterSpec` through
  `ClusterConfigSection` exactly as the other five sections already are. The three dead CRD fields
  come alive with no new mechanism. `cluster_env_toml` is either this code or is deleted — it is
  not both, and its two tests follow it either way.
* `cluster-bus-addr`, `client-addr` and `initial-nodes` are pod-derived → they stay env, but
  built as `env::overrides("cluster", &ClusterConfigSection { .. })` rather than four literals —
  which is also where the P0's bracket encoding stops being a hand-written detail and becomes
  `encode_value`'s job.
* The standalone wrapper's three exports become
  `format!("export {}={}", env::var_name("replication", "role"), env::encode_value(&role))` —
  still shell, but both halves now come from the schema.

**`adr/0001` — extended, not contradicted.** The ADR (7 lines) decides that the operator
"serializes through the server's own serde types rather than maintaining a parallel schema".
Seven env-name literals *are* a parallel schema — of names rather than of keys — that the ADR's
authors did not name because the TOML path was the one under discussion. Suggested amendment, in
the ADR's terse one-paragraph form:

> The rule covers both planes the operator configures the server on. Names **and value encodings**
> on the environment plane (`FROGDB_<SECTION>__<KEY>`) are produced by `frogdb_config::env` from
> the same serde idents and types that produce the TOML keys, never written out by hand, so a
> rename is a compile error and a mis-encoded value is a test failure on both paths. Config
> identical for every pod belongs in the mounted `frogdb.toml`; only pod-derived values are passed
> as environment overrides.

`frogdb-config` stays light — `config/Cargo.toml` gains **nothing**, because `serde_json` (`:16`)
and `toml` (`:18`) are already dependencies — which is the ADR's other invariant.
**Proposal 72 amends the same 7-line file** — see *Sibling edges*.

### Vocabulary

Per `frogdb-operator/CONTEXT.md`: `spec.mode` is the **Mode**; `spec.cluster` is the **Cluster
Tuning Block** and never "the cluster spec" (`:28-31`); "cluster" unqualified means the FrogDB
cluster and the orchestrator is always the **Kubernetes cluster** in full (`:33-34`); node roles
are **Primary**/**Replica** (`:36-37`); `spec.replicas > 1` in Standalone Mode is
**Primary + Replicas**, in Cluster Mode it is the member count. The image-tag restart is a
**Rolling Upgrade**, the annotation-driven one a **Config Hash Rollout** (`:43-50`) — §5's
`[cluster]` change triggers the latter, not the former, and this proposal says so in those words.
Avoid-listed: *FrogDBCluster*, *"the cluster spec"*, bare "cluster" for the orchestrator.

## Testability improvement

The operator's test suite is 1 343 lines and **none of it reaches `controller.rs`**, because every
function there either takes a `kube::Client` or is private and clock-dependent. And none of it —
nor any server-side test — exercises the env plane end to end, which is why §2 exists.

**New tests, in priority order:**

1. **The P0 regression, through the real loader.** Set `FROGDB_CLUSTER__INITIAL_NODES` to the
   operator's produced value, run it through `Config::load`'s figment chain, assert
   `config.cluster.initial_nodes == vec![..]`. Must not assert on the *string*. This is the test
   whose absence let a hard boot failure ship, and it belongs in `frogdb-config` (or
   `frogdb-server`), **inside the default `just test` run** — not in the operator workspace,
   which §1 says nothing reaches.
2. **`env::overrides` + `encode_value` round trip**, in `frogdb-config`: build a real
   `ClusterConfigSection`, emit the pairs, apply them as env, load through the same figment chain,
   assert the **typed values** arrive. Covers `Vec<String>`, `bool`, `u64` and `String` — the four
   shapes the operator emits — so the class of §2 becomes unrepresentable. Also in the default
   run.
3. **`var_name` / `figment_key` round trip** over whichever of option (i)/(ii) is chosen, with the
   test stated in the domain the chosen option actually operates on (see §Proposed change 1 — the
   version in revision 1 would have been vacuous).
4. **`merge_conditions(previous, desired, now)`** — three assertions that cannot be written
   against today's shape: (a) reconcile twice with unchanged inputs and `lastTransitionTime` does
   not move; (b) a type present before and absent from `desired` survives with `status: "False"`
   (the `Upgrading`/`Progressing` regression); (c) the **validation early-return path** preserves
   both other conditions rather than replacing the array with one element.
5. **77-H2's formatter** (the operator's first `controller.rs` test): the returned string parses
   as `jiff::Timestamp` **and** matches `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$` — the second half
   matters, because a value with fractional seconds parses fine and is not what `metav1.Time`
   emits.

**Tests that are migrated, not deleted.** The eleven `FROGDB_*__*` literals in
`resource_builders.rs` (`:85,86,102,103,114,117,118,127,141,153,154`) are rewritten against
`env::var_name(..)`, so a rename moves both sides together — and so `lint-env-name-seam` has no
false positives to allowlist. `config_gen.rs:151-160` and `integration.rs:210-219` (the **two**
tests of dead `cluster_env_toml`) follow the function into `generate_toml`'s `[cluster]` coverage
or are deleted with it; they must not be left asserting a function nothing calls.

**Coverage-depth note** (`just coverage-depth`, `agents/`): `frogdb-operator` would score well on
line coverage of `resources/*` and zero on `controller.rs`, while `cluster_env_toml` shows two
distinct tests on a function with no callers. Per-function **test diversity** over a reachability
filter would flag both halves. Worth a run after this lands — the operator has never been in a
depth audit, which is itself an instance of §1.

## Risks / scope boundaries

### Spec / LOCKED / gates

* **No locked crate.** The four locked areas are txn+vll, persistence+recovery,
  replication+replication-runtime, cluster+cluster-runtime (`adr/0002`–`0004`).
  `frogdb-operator` and `frogdb-config` are in none of them; `frogdb-server/.../config/loader.rs`
  is in the server crate, also unlocked.
* **FM tags: the operator is clean — verified.** `grep -rn "FM-" frogdb-operator/` → **0 hits**
  across sources, tests and `CONTEXT.md`. `just lint-failure-modes` sees no tag added, moved or
  removed.
* **`frogdb-config` is spec-relevant but untouched where it matters.** Its FM tags live in
  `persistence.rs:580`, `replication.rs:489`, `replication.rs:516` and force section-level
  `validate()`/`Deserialize` behaviour. `env.rs` is new, adds no validator, changes no
  `Deserialize` impl. `cluster.rs` is **read only** — this proposal populates
  `ClusterConfigSection`, it does not alter it. (Explicitly **not** proposed: adding
  `deny_unknown_fields` to section structs — see §6.)
* **Mutation gates: none apply.** No gated crate is touched, so `just mutants-diff` is not push
  discipline here.
* **Seam lints: one gate *added*, none violated.** `scripts/clock-seam.py` scans
  `frogdb-server/crates/**` only, so the operator's `SystemTime::now()` is outside it today and
  stays outside after the fix; the operator is outside **all fifteen** gates, which is §1 again
  and is why `lint-env-name-seam` is proposed rather than assumed. No metrics emission, redirect
  reply, durable-ack write or figment `.nested(` is added.
* **Verification commands.** `frogdb-operator` is its own workspace, so `just check
  frogdb-operator` is **not a valid command**. The probes are `just operator-build`,
  `just operator-test`, `just operator-crd` — none of which run inside root `just check` /
  `just lint` / `just test`. Whoever implements this must run them explicitly.

### Generated files — the discipline applies twice

* `frogdb-operator/deploy/crd.json` is generated (`just operator-crd`, `Justfile:961-963`). The
  `metav1::Time` retyping, the `x-kubernetes-list-type: map` annotation and per-condition
  `observedGeneration` all change it; regenerate, never hand-edit.
* `frogdb-server/ops/deploy/helm/frogdb/{Chart.yaml,values.yaml,values.schema.json,
  templates/configmap.yaml,dashboards/*}` are generated by `helm-gen`. This proposal touches none
  of them — and note `templates/statefulset.yaml` is **not** generated, which is why 77-H1 must
  hand-edit it and why §5's Helm copy cannot be fixed by editing a generator today.

### The Helm copy — in scope for the P0, out of scope for the seam

`frogdb-server/ops/deploy/helm/frogdb/templates/statefulset.yaml:66-73` carries the same four
`FROGDB_CLUSTER__*` names **and the same broken `initial_nodes` encoding** (`_helpers.tpl:76-87`,
`{{- join "," $peers -}}`). `frogdb-server/ops/helm/frogdb/templates/statefulset.yaml` is a
near-identical second tree (they differ only in `values.yaml`, `values.schema.json` and a
dashboards ConfigMap — `helm-gen`'s output dir defaults to `deploy/helm/frogdb`, so
`ops/helm/frogdb` appears to be the stale one; `CONTEXT-MAP.md:33` cites the stale path). Four more
literals live in `frogdb-server/docker/Dockerfile:33-36` and `Dockerfile.builder:164-167`.

**The bracket fix (77-H1) applies to both Helm trees** — the bug is identical and users of the
Helm path have the same dead cluster mode. **The seam does not**: they are Go templates and
Dockerfiles with no Rust seam to route through. The durable answer for them is a generated env
snippet from the same `frogdb_config::env` module — real work, a separate issue, blocked on
deciding which of the two Helm trees is canonical. `lint-env-name-seam` must scope itself to Rust
sources for exactly this reason.

### Sibling edges — verified on disk

**Proposals 72, 74 and 76 have been revised since this document was first written.** Every
cross-proposal line/section citation below must be **re-derived at merge time**; the *facts* about
this proposal's own files are the ones re-verified at HEAD `54baa2bb`.

* **72 (FR2, frogctl config schema).** **Two real edges, both benign.**
  (a) Both proposals amend `adr/0001` — a 7-line file. 72 adds a clause binding *every workspace
  tool that emits or parses `frogdb.toml`*; 77 adds a clause binding *the environment plane and
  the ConfigMap/env split*. They compose; whoever lands second appends rather than replaces.
  (b) 72 creates `frogdb-config/src/document.rs`; 77 creates `frogdb-config/src/env.rs`.
  **Separate files, deliberately** — `document.rs` renders a whole `Config` to a file, `env.rs`
  renders section names *and value encodings* for an override plane, and the round-trip partner of
  `env.rs` is `loader.rs`'s figment chain, not `to_toml`. One shared line in `config/src/lib.rs`
  (the `mod` declarations). Either order. 72 also confirms the fact this proposal builds on: the
  operator **already** depends on `frogdb-config` (`frogdb-operator/Cargo.toml:18`), so FR8 adds
  no dependency.
* **74 (FR3, Debug Bundle).** No file overlap: 77 touches neither `deb-gen` nor
  `ops/deploy/deb/**` nor the `[debug-bundle]` section, and 74 touches no operator file. **The
  label collision is resolved in this revision**: 74 numbers its hotfixes H1–H5, so this document
  now numbers its own **77-H1 … 77-H6**. No shared symbol remains.
* **76 (FR6/FR11, observability extractors).** **A real ordering constraint — upgraded from
  revision 1's "no constraint".** No file overlap, but the operator probes `/admin/role` on the
  metrics port (`health.rs:30`) and parses `"master"`/`"slave"` from its JSON (`health.rs:44-47`),
  and 76 **re-plumbs that endpoint** behind a `FromRequestParts` extractor. Two consequences:
  (a) 77's `health.rs` deletion (77-H6, `check_pod_health`) touches the same file 76's follow-on
  work reads, so it must land **before or with** 76, not after; (b) if 76's wire compatibility
  ever slips, `probe_pod_is_primary` is a consumer nobody would think to check, in a workspace
  `just test` does not build. Worth a line in 76's own risk section.
* **73 (FR1, frogctl ops wiring).** Different crate entirely; no operator file in its set. Clean.
* **75 (FR4/FR5, frogctl rendering + role enum).** Different crate. Clean. Note both 75 and this
  proposal care about Primary/Replica vocabulary at opposite boundaries: 75 normalizes the
  *server's INFO wire text*; 77 reads `/admin/role`'s JSON inside the operator. If 75's `Role`
  enum lands in a crate the operator can import, `health.rs:44-47` becomes a candidate consumer —
  a **follow-up**, not a dependency.
* **69 (SV9, config param combinators).** Same crate, different module: 69 builds a combinator in
  `frogdb-config/src/param.rs`; 77 adds `frogdb-config/src/env.rs`. Shared line is again
  `config/src/lib.rs`'s `mod` list. 69's world is `ConfigParam`/`MutableParamId` (runtime
  mutability); 77's is serde idents (the load plane). `env.rs` adds no param, so `params.rs`'s
  golden snapshot is unaffected. Clean.
* **70 (ACL registry consult).** Different crate set. Clean.
* **78 (test-harness).** **Real but additive.** The operator's `tests/integration.rs` is the only
  file outside the server workspace that consumes `frogdb-test-harness`
  (`frogdb-operator/Cargo.toml:38`), including `TestServerConfig` and `ClusterNodeConfig`
  (`integration.rs:26-44`), which 78 proposes to restructure. That is a **compile-time** edge in a
  workspace root `just check` does not build. **If 78 lands, `just operator-test` must run in the
  same commit** — another instance of §1.

### Behaviour changes, named

* **Cluster mode starts working.** 77-H1 is not a behaviour change so much as the removal of a
  total outage, but state it: pods that previously CrashLoopBackOff will now boot and form a
  cluster. Anyone whose "cluster mode" deployment is currently a set of restarting pods will see
  real traffic for the first time.
* **Every existing cluster-mode deployment gets a Config Hash Rollout.** Adding `[cluster]` to the
  generated `frogdb.toml` changes the content, so `config_hash` (`configmap.rs:32-36`) changes, so
  the pod-template annotation changes, so the StatefulSet rolls. Correct — the pods have been
  running with the wrong settings — but it is a restart of every managed FrogDB on operator
  upgrade and belongs in release notes. It is also why the `[cluster]` change is **not** a hotfix.
* **`.force()` changes conflict semantics.** Fields another manager has claimed get taken over
  instead of returning `409 Conflict`. Today the `409` propagates through `?`, `error_policy`
  (`:250-253`) requeues in 15 s, and the CR never converges again — arming it needs only one
  `kubectl apply`, one mutating webhook, or an HPA touching `spec.replicas`. Latent, but the
  textbook operator failure mode. Taking the fields over is the intended operator behaviour and
  the reason the CR exists; it is still a behaviour change on any cluster where someone has
  `kubectl apply`'d a managed child.
* **Reconcile rate halves.** From two passes per 30 s tick to one (§4a). Slower under pathological
  probe latency either way. Any dashboard counting reconciles will show a step change.
* **Condition semantics change three ways.** `lastTransitionTime` stops moving; conditions stop
  vanishing; `Progressing` inverts to the Kubernetes convention (`True`/`NewReplicaSetAvailable`
  on success). The third is the one that can break a consumer that adapted to the current
  inversion. All three belong in release notes and in the CRD field docs.

### Residual risk

With the trait withdrawn, the over-cleverness risk is largely gone. What remains: `apply_child`
must gain no retry, backoff or diffing — `error_policy` already owns requeue. And `env.rs`'s
`encode_value` must stay a thin, total function over the shapes the config schema actually uses;
the moment it needs a match arm per field, the design has drifted into a serializer and should
become one (or reuse `serde_json`, already a dependency).

## Effort

**M**, split roughly:

* **XS** — 77-H1: three lines in `statefulset.rs:106` and two Helm templates, plus one
  loader-path regression test. Lands **first**, alone, today.
* **S** — `frogdb-config/src/env.rs` (names + value encoding) + round-trip tests; repoint
  `loader.rs:99-105` per option (i) or (ii); add `lint-env-name-seam` to `scripts/` and
  `Justfile:329`. Lands independently and is useful even if both candidates are rescoped.
* **M** — FR7: `apply_child` free function + five call sites, `owner_ref` relocation,
  `merge_conditions` across all three paths + tests, `metav1::Time` retyping,
  `x-kubernetes-list-type: map`, per-condition `observedGeneration`, `Progressing` convention fix,
  `just operator-crd`, `.owns(pdb)`, `CONTEXT.md:63-64` + `clusterrole.yaml:32-35`.
* **M** — FR8: `[cluster]` into `generate_toml`, cluster overrides through `env::overrides`, the
  shell wrapper's three names, `cluster_env_toml` resolved (absorbed or deleted with its **two**
  tests), and the **eleven** `resource_builders.rs` env assertions migrated to `env::var_name`
  (budgeted, not assumed — it is what makes the lint clean).

Sequencing: **77-H1 → env.rs → FR7 and FR8 in either order.** Within FR7, `merge_conditions` lands
*after* 77-H2, so no intermediate commit preserves a malformed timestamp across reconciles.

### Hotfixes — renumbered `77-H*`

Revision 1 used the round-plan label "H5", which collides with proposal 74's internal H1–H5. All
hotfixes in this document are now prefixed `77-`.

**`77-H1` — LIVE **P0**: `FROGDB_CLUSTER__INITIAL_NODES` cannot be decoded. Lands first,
before everything.**

Chain, verified end to end in §2: `statefulset.rs:106` emits `a,b,c` → figment's parser returns
`Value::String` (bracket-only sequences, `parse.rs:74`) → `Vec<String>` deserialization fails →
`figment.extract()` (`loader.rs:128`) errors → the server exits before serving → every
operator-managed cluster-mode pod CrashLoopBackOffs. Empirically reproduced against pinned figment
0.10.19 (output in §2), including the single-node case.

Fix: `format!("[{}]", initial_nodes.join(","))` at `statefulset.rs:106`; the same bracketing in
`ops/deploy/helm/frogdb/templates/_helpers.tpl:86` (or at the `statefulset.yaml:72-73` use site)
and in the second Helm tree. **~3 lines.** Regression test through the real loader, in a crate
inside the default `just test` run.

**`77-H2` — LIVE: `lastTransitionTime` is not RFC 3339. Approved as specified.**

`condition()` (`:352`) → `chrono_now()` (`:358-365`) → `"1786483351Z"` → stored verbatim because
the CRD field is a bare string (`crd.json:317-321`, from `Option<String>` at `crd.rs:421`).
Conformant value: RFC 3339, UTC, second precision.

Fix, **no new dependency** (each link re-verified at HEAD): `k8s-openapi` 0.27.1 re-exports its
date-time crate (`pub use jiff;`, `lib.rs:237`); `metav1::Time` is
`Time(pub crate::jiff::Timestamp)` (`.../meta/v1/time.rs:5`); `jiff` 0.2.23 is already in
`frogdb-operator/Cargo.lock`; `Timestamp`'s `Display` honours the formatter's precision
(`jiff .../timestamp.rs:2357-2368`), so zero fractional digits is a format specifier:

```rust
fn now_rfc3339() -> String {
    // RFC 3339, UTC, second precision — what `metav1.Time` marshals and every
    // condition-aware Kubernetes client parses.
    format!("{:.0}", k8s_openapi::jiff::Timestamp::now())
}
```

**~6 lines** at one call site. No CRD change, no schema change, no dependency edit. Regression
test per §Testability item 5. (Blast radius is hypothetical in-repo — §3 — but the fix is free.)

**`77-H3` — LIVE: condition merge. After 77-H2.**

~25 lines: thread `frogdb.status.as_ref()` into a pure `merge_conditions`, applied at **all three**
assembly sites (`:180-214` ×2 and the validation early return `:72-90`). **Must follow 77-H2** —
preserving a malformed timestamp across reconciles makes the bad value sticky instead of merely
wrong. **Strongly consider landing the CRD `x-kubernetes-list-type: map` /
`x-kubernetes-list-map-keys: [type]` change in the same commit** (§4d): it is a schema
annotation plus `just operator-crd`, it moves keyed merging into the API server for all writers,
and it makes the Rust merge simpler. The `Progressing` convention inversion (§4c) and
per-condition `observedGeneration` (§4e) ride along cheaply here or in FR7 proper.

**`77-H4` — LIVE: `.owns(pdb)`.** Two lines plus an `Api<PodDisruptionBudget>` binding at
`controller.rs:48-52`. Closes a real gap (§3): PDB deletions currently go unreconciled for up to
30 s.

**`77-H5` — LIVE (doc + RBAC): the ServiceMonitor claim.** `CONTEXT.md:63-64` lists a capability
that does not exist. **Extended from revision 1:** also delete the `clusterrole.yaml:32-35` grant
of `create/update/patch/delete` on `servicemonitors`, or wire the builder. Documentation and
least-privilege are the same fix here — leaving standing write permission for a resource the
operator never touches is the part with security relevance. ~5 lines.

**`77-H6` — delete `health::check_pod_health`** (`health.rs:7-20`): 14 lines, zero callers
verified, pure deletion. **Must land with or before proposal 76**, not after — 76 re-plumbs the
`/admin/role` endpoint this same file probes (`health.rs:30`), and a deletion arriving after 76's
changes lands in a file 76 has moved under.

### Explicitly **not** hotfix-eligible, though LIVE

Both rulings from revision 1 re-confirmed:

* **The three dead `spec.cluster` fields (§7).** The fix changes generated `frogdb.toml`, which
  changes `config_hash` (`configmap.rs:32-36`), which restarts every managed pod. A rolling
  restart is not a hotfix. Ships with FR8, rollout stated in release notes.
* **Deleting `cluster_env_toml`.** It looks like free dead-code removal and is not: its shape is
  FR8's answer for the `[cluster]` section. Delete it *as part of* FR8 or absorb it; do not delete
  it first and rewrite it later.
* **`.force()` on server-side apply.** One word, five sites, and it silently changes conflict
  semantics on live clusters. It belongs at the `apply_child` chokepoint where it is visible,
  reviewable, and stated once.

## Review ledger — revision 1 → revision 2

Review `2e81506b`, verdict **AMEND**. Every point re-verified against HEAD `54baa2bb` before
being applied. Nothing was accepted on the review's word alone.

### Blocking

| # | Review point | Disposition |
|---|---|---|
| **B1** | `FROGDB_CLUSTER__INITIAL_NODES` comma-join cannot deserialize → hard startup failure; reclassify latent → **Live P0** | **Accepted, independently reproduced.** Ran a pinned figment 0.10.19 probe replicating `loader.rs:99-105` verbatim: `"a:1,b:2"` and `"a:1"` both fail (`invalid type: found string …, expected a sequence`), `"[a:1,b:2]"` and `"[a:1]"` succeed. Mechanism traced in source (`parse.rs:74` bracket-only sequence branch; `de.rs:81` no string→seq coercion). Now the document's lead finding, hotfix **77-H1**, ahead of the timestamp fix. Helm trees carry it too (`_helpers.tpl:86` `join ","`). Harness bypass confirmed at `test-harness/src/server.rs:570-572`. **Design consequence folded into FR8**: a name-only seam would have shipped it unchanged, so `env.rs` now specifies `encode_value`/`env_list` and the round-trip test asserts the **value**, not the key. |
| **B2** | `figment_key` cannot be both "verbatim move" and inverse of `var_name`; the `\x00` dance is dead code; proposed round-trip test vacuous | **Accepted, independently reproduced.** `Env::split` is `self.map(|k| k.replace(pattern, "."))` (`figment/providers/env.rs:409-412`) and `map` chains as `f(filter_map(key))` (`:132-142`), so split runs first. Probe printing the closure's actual input for `FROGDB_DEBUG_BUNDLE__ENABLED` yields `"DEBUG_BUNDLE.ENABLED"` — the `__` is already `.`. Amended to two explicit options, **(ii) recommended** (single `Env::prefixed().map(full_inverse)` owning both steps), with the round-trip test restated in the domain each option operates on. |
| **B3** | `lint-env-name-seam` would have ~15 false positives day one | **Accepted, with sharper numbers.** The revision-1 pattern (`"FROGDB_` literal) hits **61** lines across 14 files at HEAD — mostly `FROGDB_SNAPSHOT_COMPLETE_v1`, `FROGDB_CHECKPOINT`, `FROGDB_FLAME_OUTPUT`, OTel attribute consts — i.e. unusable, worse than the review estimated. Narrowed to the construction shape `FROGDB_[A-Z0-9]+__`: **19** hits — 8 production (`statefulset.rs`), 11 tests (`resource_builders.rs`), 1 comment (`cluster_init.rs:489`). Test migration budgeted into FR8's effort rather than allowlisted. *Minor correction to the review*: the 11 test literals span `:85-154`, not `:107-119` (`:107-119` is one test of the several). |
| **B4** | `ChildResource: Resource<DynamicType = ()>` structurally excludes `DynamicObject`, contradicting the ServiceMonitor plan | **Accepted, verified.** `impl Resource for DynamicObject { type DynamicType = ApiResource; }` (`kube-core-3.1.0/src/dynamic.rs:78-79`); `Api::namespaced` requires `K::DynamicType: Default` (`kube-client-3.1/src/api/mod.rs:138,187`); dynamic kinds need `namespaced_with` (`:93`). Also noted: today's `servicemonitor::build` returns `Option<serde_json::Value>` (`:11`), not a `DynamicObject` — so revision 1 understated the distance. **Resolved by taking the review's first option**: ServiceMonitor is **dropped from FR7 scope**; the dynamic-kind decision is deferred to whenever it is actually wired. |
| **B5** | `build() -> Option<Self>` is the wrong shape twice; a free function beats the trait | **Accepted, verified, trait withdrawn.** (1) `Service` has two builders — `service.rs:9` `build_headless`, `service.rs:61` `build_client` — applied at `controller.rs:107-116` and `:118-126`; one impl per type cannot express it. (2) `apply_child` never calls `build`: `reconcile` already holds each object. Replaced with a free `async fn apply_child<K>(api, &K)`, five call sites, PDB keeping its `if let Some`. B4 and B5.1 both evaporate with the trait. |

### Non-blocking, applied

| # | Correction | Status |
|---|---|---|
| N1 | operator src is **1 796** lines, not 1 526 | Applied (`find frogdb-operator/src -name '*.rs' \| xargs wc -l` → 1796). |
| N2 | `cluster_env_toml` has **2** tests, not 3 (and not "five assertions") | Applied. `config_gen.rs:152` and `integration.rs:212` are the only two. |
| N3 | `resource_builders.rs` has **38** tests, not 44 | Applied. |
| N4 | crd.json dead knobs are `:53-57` + `:66-79`, not `:53-77` | Applied. |
| N5 | PDB apply is `:167-173` (call), `:165-174` (if-let) | Applied. |
| N6 | `Cargo.toml` members `:3-40` | Applied. |
| N7 | `CONTEXT-MAP.md:33` cites the stale Helm path, not `:29` | Applied. |
| N8 | CI path-filter line numbers | Applied as precise ranges: `operator` filter `test.yml:52-55`, `operator_config` `:56-59`, job `:245-269`. |
| N9 | The "silent typo" claim is **field-only**; an unknown **section** is loud | Applied and stated as a distinction. `Config` carries `deny_unknown_fields` (`config/src/lib.rs:83`) → `FROGDB_CLUSTR__ENABLED` errors at startup; `ClusterConfigSection` does not (`cluster.rs:16`) → `FROGDB_CLUSTER__AUTO_FAILOVR` is silently dropped. Adding `deny_unknown_fields` to sections is explicitly **not** proposed (compatibility policy, not this proposal's call). |
| N10 | Revision 1's diff count omitted six impl blocks (≈ +48), so the real figure was ≈ −35/+93 | **Moot under B5.** The free function is ≈ −30/+12, which is stated as a genuine reduction. Recorded here so the revision-1 number is not cited later. |
| N11 | The `.owns` parity test would be a third hand-maintained list | Applied — softened to a suggestion. **And the buried live gap promoted to its own finding + hotfix 77-H4**: `.owns` at `:49-51` covers StatefulSet/Service/ConfigMap but not the PDB created at `:165-174`, so PDB deletions go unreconciled for up to 30 s. |
| N12 | `env.rs` needs no new dependencies | Applied. `frogdb-config/Cargo.toml` already has `serde_json` `:16` and `toml` `:18`. |
| N13 | Conditions lack `observedGeneration` | Applied as a new sub-finding (§4e) and folded into 77-H3/FR7. `observedGeneration` appears once in `crd.json`, at `:354`, status top level only. |

### Structural + sequencing amendments

* **Headline promoted.** `frogdb-operator` being its own cargo workspace excluded from root
  members — so `just check`/`lint`/`test` never reach it, leaving only a path-filtered CI job — is
  now §1 and the Summary's opening, above the trait. It is the common cause of the dead
  ServiceMonitor, the three dead CRD knobs, and 77-H1 surviving.
* **Hotfixes renumbered `77-H1 … 77-H6`**, killing the label collision with proposal 74's
  internal H1–H5. New order: **77-H1** bracket encoding (P0, first) → **77-H2** RFC 3339 →
  **77-H3** condition merge (all three paths, + CRD `list-type: map`) → **77-H4** `.owns(pdb)` →
  **77-H5** CONTEXT.md + clusterrole RBAC → **77-H6** delete `check_pod_health` (with/before 76).
* **Sibling 76 upgraded from "no constraint" to a real ordering constraint** (`health.rs:30`
  `/admin/role`, `:44-47` master/slave parse).
* **Sibling 78** re-stated as real-but-additive with its "run `just operator-test` in the same
  commit" requirement.
* **72 / 74 / 76 cross-citations marked "re-derive on merge"** — all three have been revised since
  revision 1 was authored.

### Refutations and corrections to the review

The review was right on all five live claims and all five blocking items. Four smaller
corrections, recorded for accuracy:

1. **The reviewer's ephemeral-raft-dir sub-hypothesis is REFUTED.** The suggestion that the
   operator leaves Raft state on ephemeral storage does not hold: `cluster_init.rs` derives the
   split-brain logger's directory from `config.persistence.data_dir` (`:669`), which
   `generate_toml` pins to the mounted PVC at `/data` (`config_gen.rs:59-60`), and
   `cluster.data_dir` has a schema default (`cluster.rs:50-52`). No finding here; not carried into
   the document.
2. **B3's location detail is off.** The 11 `FROGDB_*__*` literals in `resource_builders.rs` span
   `:85,86,102,103,114,117,118,127,141,153,154` — not "11 in `:107-119` alone"; that range holds
   one test with three of them. The magnitude of the false-positive problem is unaffected (and is
   in fact worse than estimated for the un-narrowed pattern — 61 hits, N-B3 above).
3. **B5's Service builder cites point at `controller.rs`, not `service.rs`.** The two Service
   *builders* are `service.rs:9` and `service.rs:61`; `:108`/`:119` are the two Service *apply*
   sites in `controller.rs`. The argument is unchanged and accepted; the citations are corrected.
4. **N8's "test.yml:57 not :59" has no target in revision 1** — revision 1 cited no `test.yml`
   line other than the job range `:245-269`, which is correct as written. Resolved by citing the
   filters precisely instead: `operator` `:52-55`, `operator_config` `:56-59` (`Cargo.lock` is
   `:59`, `crates/config/**` is `:57`).

### Security

Nothing in this document implements a security fix. The one item with security relevance —
`clusterrole.yaml:32-35` granting `create/update/patch/delete` on `servicemonitors` for a resource
the operator never creates — is **recorded** as part of 77-H5's least-privilege rationale and is
otherwise left to the security backlog per standing policy.
