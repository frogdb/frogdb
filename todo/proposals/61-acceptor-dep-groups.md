# Proposal: Acceptor Stores Dep Groups (Delete the Flatten/Regroup Round Trip)

Status: proposed
Date: 2026-07-16

## Problem

The same connection configuration crosses **three shapes** on its way from server startup to
a `ConnectionHandler`, and the middle shape is pure repackaging. `AcceptorContext` already carries
its dependencies as four grouped structs (`core`, `admin`, `cluster`, `observability`), `bind`
*flattens* them into ~40 loose `Acceptor` fields, and then `run` — once per accepted connection —
*regroups* those same fields back into the identical structs to hand to `ConnectionHandler::from_deps`.

| Symptom | Where |
|---------|-------|
| `Acceptor` holds ~40 flat fields (registry, senders, ACLs, cluster handles, telemetry, config flags, TLS) | `frogdb-server/crates/server/src/acceptor.rs:52-188` |
| `AcceptorContext` already holds `core`/`admin`/`cluster`/`observability` as grouped structs | `acceptor.rs:200-248` |
| `bind` destructures those groups into the 40 flat fields (`registry: ctx.core.registry`, `cluster_state: ctx.cluster.cluster_state`, …) | `acceptor.rs:267-317` |
| `run` repacks the flat fields into `CoreDeps`/`AdminDeps`/`ClusterDeps`/`ConnectionConfig`/`ObservabilityDeps` — every connection | `acceptor.rs:419-467` |
| …handed positionally to `ConnectionHandler::from_deps`, which stores `core`/`admin`/`cluster`/`observability` verbatim and splits `config` into fields | `connection.rs:216-227`, `:244-247` |
| Sole construction site of `AcceptorContext`, in `start_subsystems` | `server/subsystems.rs:386-435` (3 `Acceptor::bind` calls, `:443`/`:462`/`:490`) |

`from_deps` consumes five groups; four of them (`CoreDeps`, `AdminDeps`, `ClusterDeps`,
`ObservabilityDeps`) exist *unchanged* inside `AcceptorContext` at bind time. The acceptor takes them
apart and puts them back together for no one — the flatten and the regroup are inverse operations
with the handler on one side and the context on the other.

The cost is a wide, redundant change-surface. Adding one grouped dependency (say a field on
`ClusterDeps`) forces **three mechanical acceptor edits** on top of the real ones (the struct field +
its population at `subsystems.rs:399-410`): a new flat field on `Acceptor`, a `bind` flatten line, and
a `run` regroup line. None of the three carries behavior. The middle layer is interface, not module.

## Design

Store the grouped form the acceptor is already being handed, and clone it per connection. The
grouped bundle already exists as a type — `ConnectionDeps` (`connection/deps.rs:278-285`) is exactly
the five groups `from_deps` wants — so the acceptor can hold one `ConnectionDeps` template.

```rust
pub struct Acceptor {
    // Per-port I/O and acceptor-local state (genuinely NOT connection deps):
    listener: TcpListener,
    assigner: RoundRobinAssigner,       // acceptor.rs:31-49, unchanged
    current_connections: Arc<AtomicI64>, // this acceptor's own counter (bind mints a fresh one)
    max_clients: Arc<AtomicU64>,         // maxclients gate; never enters the handler
    conn_monitor: Option<tokio_metrics::TaskMonitor>,
    #[cfg(not(feature = "turmoil"))] tls_manager: Option<Arc<TlsManager>>,          // per-port
    #[cfg(not(feature = "turmoil"))] tls_handshake_timeout: std::time::Duration,

    /// The five dep groups, pre-assembled once. Cloned (Arc-cheap) per connection.
    deps: ConnectionDeps,
}

impl Acceptor {
    pub fn bind(ctx: AcceptorContext, spec: PortSpec) -> Self {
        // core/admin/cluster/observability move in wholesale — already grouped.
        // Only ConnectionConfig is *assembled* here, because two of its members are
        // knowable only at bind time: spec.is_admin (per-port) and the two flags
        // derived from ConfigManager (per_request_spans, enable_debug_command).
        let config = ConnectionConfig {
            num_shards: ctx.core.shard_senders.len(),
            is_admin: spec.is_admin,
            per_request_spans: ctx.admin.config_manager.per_request_spans_flag(),
            enable_debug_command: ctx.admin.config_manager.enable_debug_command(),
            /* allow_cross_slot, scatter_gather_timeout, admin_enabled, hotshards_config,
               memory_diag_config, is_replica, chaos_config … from ctx */
            ..
        };
        Self {
            listener: spec.listener,
            deps: ConnectionDeps { core: ctx.core, admin: ctx.admin, cluster: ctx.cluster,
                                   config, observability: ctx.observability },
            current_connections: Arc::new(AtomicI64::new(0)),
            /* max_clients, conn_monitor, tls_* from ctx/spec */ ..
        }
    }
}
```

`run` loses the entire `:419-467` regroup block. Where it read a flat field it now reads through the
group — `self.deps.observability.metrics_recorder` for the accept/reject counters,
`self.deps.admin.client_registry` to register the connection, `self.deps.config.is_admin` for the
maxclients exemption — and hands the handler a clone:

```rust
let ConnectionDeps { core, admin, cluster, config, observability } = self.deps.clone();
let handler = ConnectionHandler::from_deps(
    socket, addr, conn_id, shard_id, client_handle,
    core, admin, cluster, config, observability,
);
```

All five groups already `#[derive(Clone)]` (`deps.rs:37,57,82,166,231`) and every member is an `Arc`,
`Copy`, or small owned value — the per-connection clone is the same Arc-bumping the regroup already
did, minus the field-by-field restatement.

**What stays, untouched — this is real acceptor behavior, not repackaging:** the maxclients gate
(`acceptor.rs:331-352`), the TLS handshake wrap (`:363-395`), per-connection `conn_id` assignment
(`:354`), the round-robin shard assigner (`:31-49`, called at `:355`), and connection accept/reject
metrics. The `deletion test` cuts exactly at the seam between these (which act on each socket) and the
dep bundle (which is identical for every socket on the port).

## Why this is the right depth

- **Deletion test.** Delete the ~40 flat fields and the `:419-467` regroup block and nothing
  behavioral is lost: `from_deps` receives byte-identical groups, because the groups it gets are the
  ones `AcceptorContext` already held. The removed code is inverse-pair plumbing — a flatten whose
  only consumer is the matching regroup. That is the signature of an interface masquerading as a
  module, the same failure `#39` named for WAIT's sentinel path.
- **Locality.** "What does a connection depend on?" has one answer, `ConnectionDeps`, and one home,
  `deps.rs`. Today the answer is smeared across the context groups, the acceptor's flat mirror, and
  the regroup site that reunites them. The acceptor stops being a place dependencies pass *through*.
- **Leverage.** Adding a grouped dependency collapses from four touchpoints to two: the group struct
  and its population in `subsystems.rs`. The acceptor is no longer on the change path at all — the new
  field rides the pre-built bundle straight to the handler. Removing the flat mirror removes the class
  of bug where the three copies of a field drift.
- **Deepens, does not fork.** No new type: `ConnectionDeps` already exists as the `from_deps` bundle.
  The context keeps supplying the groups; the handler keeps consuming them; the acceptor simply stops
  disassembling them in between. `PortSpec` stays the per-port seam (`is_admin`, TLS) it already is.

## Testing impact

- **Existing bind-threading tests** (`acceptor.rs:565-644`) survive, with their assertions rethreaded
  through the groups. `bind_threads_is_admin_per_port` reads `main.deps.config.is_admin` instead of
  `main.is_admin`, and `admin.deps.config.scatter_gather_timeout` (a `Duration`) instead of
  `admin.scatter_gather_timeout_ms` (`u64`) — the field moved and changed type, so the assertion is
  rewritten, not deleted. `bind_threads_tls_manager_per_port` is unaffected: `tls_manager` stays an
  acceptor-local per-port field (`main.tls_manager.is_none()` unchanged).
- **A newly feasible unit test.** The accept loop has no test today because building the deps was
  welded to a live socket accept. Pre-assembling the bundle in `bind` makes "the deps the handler
  receives" inspectable without binding: a test can assert `bind` produces a `ConnectionDeps` whose
  `config.is_admin` tracks `spec.is_admin` and whose Arc members are pointer-equal to the context's
  (`Arc::ptr_eq`) — i.e. the port shares, does not copy. The socket-driven accept loop still needs an
  integration test; the *dep construction* no longer does.

## Risks / open questions

- **`current_connections` must stay out of the cloned bundle.** It is minted fresh per `bind`
  (`acceptor.rs:281`) and shared by every connection on that port for the maxclients count and the
  fetch_add/fetch_sub on the connection lifecycle (`:402`, `:491`). If it were pulled into
  `ConnectionDeps` and cloned per connection, each connection would count against its own zeroed
  counter and the gate would never fire. It stays an explicit acceptor field — flagged because the
  refactor's whole premise is "move fields into the bundle," and this is the one that must not move.
- **`ConnectionConfig` is assembled at `bind`, not moved.** Unlike the other four groups it is not
  present in `AcceptorContext`; two members depend on `spec.is_admin` and two derive from
  `ConfigManager`. So the template is finalized per port (per `bind` call), which is already the
  granularity — each port produces one `Acceptor`. No sharing is lost.
- **`new_conn_senders` is reserved/dead** (`acceptor.rs:58-59`, `#[allow(dead_code)]`); `num_shards`
  comes from `shard_senders.len()`. It stays acceptor-local (or is finally dropped) — out of scope
  here, but worth deleting in the same pass if it is still unused when this lands.
- **Optional follow-on: `from_deps(ConnectionDeps)`.** With the acceptor holding a `ConnectionDeps`,
  the ten-argument `from_deps` (`connection.rs:216`, `#[allow(clippy::too_many_arguments)]`) could take
  the bundle directly and stop destructuring at the call site. That touches the other `from_deps`
  callers (`guards.rs:464`, `builder.rs:194`/`:220`, `scripting/script.rs:260`) and is deliberately
  left as a separate step — this proposal's deletion test holds with `from_deps` unchanged.
