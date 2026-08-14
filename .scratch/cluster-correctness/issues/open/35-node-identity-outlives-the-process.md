# 35: Node identity outlives the process — persisted, full-entropy, collision-rejecting

Status: ready-for-agent

## Origin

Distsys-review MAJ-7 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled accept-and-file by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

Node id minting (`config/src/cluster.rs:231-243`):

```rust
let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
let random_bits = rand::random::<u16>() as u64;
(timestamp << 16) | random_bits
```

Three defects stacked:

1. **Entropy**: 16 random bits per millisecond — birthday-bounded collisions when an
   orchestrator starts nodes simultaneously (StatefulSet rollout, `docker compose up`
   — the common case).
2. **No persistence**: `node_id` is `#[param(skip)]` citing Redis's nodes.conf, but
   FrogDB has no nodes.conf equivalent — an unconfigured node mints a fresh id every
   boot. Every restart joins the cluster as a *stranger*; the old id's entry (holding
   its slot assignments) is orphaned, deterministically.
3. **Silent merge**: `AddNode` is an upsert, so an id collision does not error — two
   distinct nodes silently merge into one entry, one node's slot ownership attributed
   to the other.

Redis persists a 160-bit node id in nodes.conf; etcd persists a member id and refuses
to start a member whose id does not match its data dir. Identity outlives the process.
FrogDB's persistence layer already does this correctly for `database_id` — the pattern
exists in-repo.

## What to build (spec-first; cluster locked, gate 0.80)

1. Spec rows first:
   - Amend the node-identity State-space row: id is minted **once** (full-width random,
     ≥128 bits, **no timestamp component** — removes a wall-clock read from identity
     minting per the campaign's no-wall-clock principle), persisted in the data
     directory beside `database_id`, read back on every boot.
   - TR row: node restarts without a configured id → same id as before restart; its
     membership entry and slot assignments are its own again (no stranger-join, no
     orphaned entry).
   - TR row (boot guard, etcd shape): configured/persisted id mismatch with the data
     directory → refuse to start with a diagnostic, never silently adopt either.
   - FM row: `AddNode` for an id already present with a different announced address →
     **rejected**, never upserted-over; NOT observable: two live nodes sharing one
     membership entry.
2. Code: mint-once + persist + read-back (reuse the `database_id` persistence shape);
   widen to ≥128 random bits; `AddNode` rejects conflicting ids (distinguish
   "same node re-announcing after restart" — allowed, address may change — from
   "different node claiming an existing id" — rejected; MAJ-2's persisted replica run
   id is the same identity family, keep the surfaces consistent).
3. Forcing tests: restart-keeps-identity (unconfigured node restarts, rejoins with same
   id, retains slot assignments); collision-rejected (second node claiming a live id is
   refused); boot-guard mismatch refusal.
4. Migration note: pre-alpha — no compat shim for old 48-bit-timestamp ids (explicitly
   ruled campaign-wide: no backwards compatibility).

## Cross-references

- Retires Task 1 deferred minor: quint model's node-id-stability assumption
  (`member_of` survives restart) unstated — after this issue the assumption is true and
  the model header can state it as spec-backed.
- MAJ-2 ruling (replica run id, persisted + transmitted): same identity-outlives-process
  family; align naming and persistence location.

## Acceptance criteria

- [ ] State-space row amended; TR/FM rows landed; `just lint-spec` green
- [ ] Id: ≥128 random bits, no timestamp; persisted; read back on boot
- [ ] `AddNode` rejects conflicting ids; boot guard refuses id/data-dir mismatch
- [ ] Forcing tests fail pre-fix, pass post-fix
- [ ] `just mutants-diff` on frogdb-cluster (locked, 0.80) triaged

## Blocked by

None — can start immediately.
