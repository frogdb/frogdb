# status.primaryPod is an ordinal-0 heuristic, wrong after failover

Status: done

`frogdb-operator/src/controller.rs` sets `status.primary_pod = {name}-0` whenever
`replicas > 1` or mode=cluster. This conflates "ordinal-0 pod" with the actual
Primary/Raft-Leader:

- Standalone mode: wrong after a failover promotes a different ordinal.
- Cluster mode: there is no fixed primary at all; the field is misleading.

Fix options (decide in implementation):

1. Query actual role (INFO replication / cluster metadata) during reconcile and report truth.
2. Omit the field in cluster mode; keep it standalone-only with real role detection.
3. Rename to something honest (e.g. `bootstrapPod`) if role tracking is out of scope.

See `frogdb-operator/CONTEXT.md` flagged ambiguities.

## Comments

Resolved via **option 2 — real role detection, standalone-only**.

- `controller.rs::reconcile` now computes `status.primary_pod` only when
  `mode != "cluster" && replicas > 1`. In cluster mode the field is omitted (left `None`):
  Raft leadership moves and there is no fixed primary, so reporting one would be misleading.
- New `controller.rs::detect_primary_pod` lists the FrogDB's pods (by the
  `app.kubernetes.io/instance` + `managed-by` labels) and probes each running pod's replication
  role over HTTP on the metrics port. It returns the name of the pod that reports itself Primary.
- New `health.rs::probe_pod_is_primary` queries `GET /admin/role` (served by the server's
  observability HTTP server; open when no admin token is configured, which the operator does not
  set) and maps `{"role":"master"}` → primary. It returns `Option<bool>`; any failure
  (unreachable, non-2xx, unparseable) yields `None`.
- **Resilience:** all listing/probe failures are swallowed and yield `None`; the reconcile never
  fails on role detection. If the role can't be determined the field is left unset rather than
  guessing an ordinal. Metrics disabled → no HTTP surface → field unset.
- CRD doc comment for `FrogDBStatus::primary_pod` updated to state the new semantics;
  `deploy/crd.json` regenerated (also picked up previously-drifted status fields). `CONTEXT.md`
  example dialogue + flagged-ambiguity bullet rewritten.
- The ordinal-0 shell wrapper in `statefulset.rs` that seeds pod-0 as the initial Primary is
  unchanged — that's the bootstrap mechanism; detection reports the *actual* current role.

Verification: `just operator-test` — 69/69 pass; `cargo clippy --all-targets` clean.
