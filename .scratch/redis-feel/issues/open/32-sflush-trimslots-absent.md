# Redis 8.x slot-lifecycle commands `SFLUSH` and `TRIMSLOTS` are absent

Status: needs-triage
Type: gap (missing commands) / ruling needed
Area: cluster

## Problem

Two slot-scoped keyspace commands from the advertised 8.6.1 target are unregistered (clients get
`ERR unknown command`):

| command | since | arity | flags | summary |
|---|---|---|---|---|
| `SFLUSH` | 8.0.0 | -3 | `WRITE`, **`EXPERIMENTAL`** | Remove all keys from a selected range of slots |
| `TRIMSLOTS` | 8.4.0 | -5 | `WRITE` | Trim the keys that belong to specified slots (`RANGES numranges start end ...`) |

(from the vendored upstream metadata, `website/src/data/redis-commands-8x.json`; both `server`
group, ACL categories `KEYSPACE` / `DANGEROUS`.)

FrogDB owns the underlying capability — slot ownership, slot migration and per-slot key
enumeration all exist (`server/src/commands/cluster/`, `migrate_cmd.rs`) — so these are wiring,
not new subsystems.

## Why `needs-triage`

1. **`SFLUSH` is flagged `EXPERIMENTAL` upstream.** Redis may change or drop it. ADR-0005's
   "unadvertised gap is a bug" rule arguably should not extend to commands upstream itself marks
   unstable. If that is the ruling, `SFLUSH` belongs in the matrix's documented-exclusion list
   rather than in the open-gap list.
2. **`TRIMSLOTS` semantics under Raft.** In Redis it trims keys for slots the node no longer owns.
   FrogDB's slot ownership is Raft-driven and orphaned-key cleanup after a slot move may already be
   automatic — in which case `TRIMSLOTS` is either a no-op that must be *truthfully* explained or a
   manual trigger for the same sweep. Confirm what the migration path already does before deciding.
3. Both are `DANGEROUS`-category; ACL wiring must place them in the right categories.

## Acceptance criteria (draft, pending ruling)

- [ ] Ruling recorded: implement, or document as a deliberate exclusion with the reason
      (upstream-experimental / subsumed by Raft-driven cleanup)
- [ ] If implemented: slot-range parsing matches upstream arity and error strings, and both are
      rejected outside cluster mode the way other cluster-only commands are
- [ ] If excluded: `website/src/data/compat-exclusions.json` carries the reason, so the matrix
      stops reporting a bare "not implemented in FrogDB"

Size: M
