# Spec: operations/security.md

Status: rewrite (correct the fabricated TLS hot-reload mechanisms and the
orchestrator ACL-sync line; the ACL/rate-limit content is largely accurate)
Audiences: A4 (primary), A2

Goal: The reader can configure authentication and ACLs (including the FrogDB
per-user rate-limit extension), TLS via rustls (per-port policy and client-cert
modes), and understands exactly which TLS management features are and are not
available. Honesty about the not-yet-wired cert hot-reload is the point.

Not in scope:
- Full Redis ACL rule syntax — documented by redis.io; document FrogDB deltas
  (no selectors, the rate-limit extension) and link upstream.
- Network/cluster admin-API auth beyond a pointer — that lives in
  [Clustering](/operations/clustering/).

Sources of truth (read before writing):
- `frogdb-server/crates/acl/src/parser.rs` — key-permission prefixes (`%R~`,
  `%W~`, `%RW~`), selector rejection, `ratelimit:cps=`/`bps=` parsing,
  `resetratelimit` rule token.
- `frogdb-server/crates/server/src/connection/handlers/auth.rs` (~269–294) — the
  real ACL subcommand set (incl. DRYRUN, HELP).
- `frogdb-server/crates/acl/src/ratelimit.rs` — token-bucket engine.
- `frogdb-server/crates/server/src/connection/guards.rs` (~54–64) — rate-limit
  exempt commands and admin-port bypass.
- `frogdb-server/crates/config/src/security.rs` — `requirepass`, `[acl] aclfile`.
- `frogdb-server/crates/config/src/tls.rs` and
  `frogdb-server/crates/server/src/tls.rs` — TLS config keys, `ClientCertMode`,
  and the (unwired) reload plumbing.

Existing content: `operations/security.md` — keep ACL command set, key-permission
prefixes, no-selectors note, rate-limit section, TLS config + per-port table;
REWRITE the "Certificate Hot-Reloading" section and fix the cluster ACL-sync line.

## Verified facts (authoritative)

**ACL.** Real subcommands (`auth.rs`): WHOAMI, LIST, USERS, GETUSER, SETUSER,
DELUSER, CAT, GENPASS, LOG, SAVE, LOAD, plus **DRYRUN** and **HELP** (both extra —
add them). Key-permission prefixes `%R~` / `%W~` / `%RW~` are real (plain `~` =
read-write). **ACL selectors (`(...)`, `clearselectors`) are explicitly rejected**
with an error — keep documenting them as unsupported (this is correct). `aclfile`
under `[acl]` is real; hashed passwords `#<sha256>` supported.

**Per-user rate limiting (FrogDB extension).** Real: `ratelimit:cps=` (commands/s)
and `ratelimit:bps=` (bytes/s) tokens on `ACL SETUSER`, backed by a token-bucket
registry. `resetratelimit` is a **rule token** used as `ACL SETUSER <user>
resetratelimit` (not a standalone command — fix the current page's `ACL SETUSER
app resetratelimit` is fine, but don't imply a top-level command). Exempt
commands: AUTH, HELLO, PING, QUIT, RESET. Admin-port connections bypass rate
limits. All correct in the current page.

**TLS.** rustls (aws-lc-rs provider), zero OpenSSL. Config keys (`[tls]`):
`enabled`, `cert-file`, `key-file`, `ca-file`, `tls-port` (default **6380**),
`protocols`, `ciphersuites`, `client-cert-file`/`client-key-file`,
`handshake-timeout-ms`. Client-cert mode key is **`require-client-cert`** with
values `none` / `optional` / `required` (`ClientCertMode`). Per-port policy keys
are real: `tls-replication`, `tls-cluster`, `tls-cluster-migration` (rolling
dual-accept), `no-tls-on-admin-port` (default true), `no-tls-on-http` (default
true).

**Certificate hot-reload — MUST be corrected (current section is largely
fabricated):**
- `CONFIG RELOAD-CERTS` — **does not exist** in source (doc-only). DELETE.
- `SIGUSR1` reload — **does not exist**; only `SIGTERM` is handled. DELETE.
- `watch-certs` / `watch-debounce-ms` — the config KEYS exist but there is **no
  file watcher implemented** (no `notify` dependency; the fields are read
  nowhere; `TlsManager::reload` is called only from a test). Cert hot-reload is
  **not functional**.
- Honest treatment: state that the ArcSwap-based reload plumbing exists internally
  but is not yet triggered by any runtime mechanism, so **certificate changes
  currently require a restart**. Do not present watcher/signal/CONFIG paths as
  working. (Optionally note `watch-certs` keys are reserved/not-yet-wired.)

**Cluster ACL sync.** The current page says "have the orchestrator push identical
ACL configuration" — the orchestrator does not exist (see Clustering). Reframe:
ACLs are per-node and not auto-synchronized; distribute the same `aclfile` to all
nodes (config management / the same ConfigMap), and reload via restart.

Structure (H2/H3 — one line each):

- Intro: FrogDB supports Redis ACLs and TLS via rustls; this page covers auth,
  ACLs, the rate-limit extension, and TLS. Link redis.io for base ACL semantics.
- **## Authentication** — `requirepass`; pointer to ACL for fine-grained control.
- **## ACLs** — the real subcommand set (incl. DRYRUN/HELP), key-permission
  prefixes, `aclfile` + hashed passwords, and the explicit no-selectors
  limitation. State plainly.
- **### Per-user rate limiting** — the `ratelimit:cps`/`bps` extension, exempt
  commands, admin bypass, `resetratelimit` rule token. This page is the canonical
  home for the rate-limit syntax and exempt list; Architecture → Connection covers
  only the enforcement mechanism and links here.
- **### ACLs across nodes** — per-node, not auto-synced; distribute the same
  aclfile (no orchestrator). Link Clustering.
- **## TLS** — rustls, config keys, a minimal `[tls]` snippet; client-cert modes
  (`require-client-cert` none/optional/required).
- **### Per-port TLS policy** — keep the port table but correct it against the
  real keys (`no-tls-on-admin-port`, `no-tls-on-http`, `tls-replication`,
  `tls-cluster`, `tls-cluster-migration`); verify the exact default port numbers
  against config before publishing (the current 6382 admin / +10000 bus figures
  need cross-checking with Clustering's `cluster-bus-addr` default 16379).
- **### Certificate management** — the HONEST version: cert changes require a
  restart today; the reload plumbing exists but no watcher/signal/CONFIG trigger
  is wired. Cut the cert-manager/Vault/ACME "SIGUSR1" table or reframe it as
  "restart on renewal" until hot-reload is wired.
- **## Network security** — binds `127.0.0.1` by default; restrict admin/metrics
  ports; use TLS across trust boundaries. Keep.
- **## See also** — Clustering (admin auth), Observability (metrics-port
  exposure), Configuration.

Generated data:
- Links to Reference → Configuration reference (`config-reference.json` for
  `[tls]`/`[acl]`/`[security]`) and Compatibility → Command matrix
  (`compatibility/command-matrix`, ACL family via S1/S2).
  No embedded generated component.

Drift guards:
- `just docs-gen-check` keeps `[tls]`/`[acl]` keys honest.
- S7 code-path check for cited `crates/acl/...`, `crates/server/src/tls.rs`.
- The ACL subcommand list and rate-limit tokens should be cross-checked against
  the command matrix (S1/S2) / parser at write time.
- Content policy: do NOT document unwired features as functional — the cert
  hot-reload section is the specific trap here; document only what is triggered at
  runtime. No orchestrator language.
