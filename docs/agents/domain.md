# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- **`CONTEXT-MAP.md`** at the repo root — it points at one `CONTEXT.md` per context. Read each one relevant to the topic.
- The per-context **`CONTEXT.md`** for the area you're working in (see the map below).
- **`docs/adr/`** — repo-wide architectural decisions. Also check `<context>/docs/adr/` for context-scoped decisions.

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The producer skill (`/grill-with-docs`) creates them lazily when terms or decisions actually get resolved.

## File structure

This is a multi-context repo (a Rust workspace). Contexts map to top-level directories, not `src/<context>/`:

```
/
├── CONTEXT-MAP.md                     ← index of contexts
├── docs/adr/                          ← workspace-wide decisions
├── frogdb-server/                     ← context: server (Redis-compatible DB engine)
│   ├── CONTEXT.md
│   ├── docs/adr/                      ← server-specific decisions
│   └── crates/                        ← config, protocol, types, cluster, persistence,
│                                        acl, scripting, commands, core, server, ...
├── frogdb-operator/                   ← context: operator (K8s operator + deploy tooling)
│   ├── CONTEXT.md
│   └── docs/adr/
└── frogctl/                           ← context: cli (frogctl client)
    ├── CONTEXT.md
    └── docs/adr/
```

The three contexts:

- **server** — the FrogDB database engine under `frogdb-server/` (protocol, commands, storage, cluster, replication, scripting). Most work lives here.
- **operator** — the Kubernetes operator (`frogdb-operator/`) and deployment tooling (`frogdb-server/ops/*`).
- **cli** — the `frogctl/` command-line client.

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in the relevant context's `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal — either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/grill-with-docs`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0007 (event-sourced orders) — but worth reopening because…_
