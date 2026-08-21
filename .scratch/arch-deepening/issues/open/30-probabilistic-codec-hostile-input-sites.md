# 30 — Probabilistic-filter hostile-input sites: zero-bucket divide-by-zero, allocation abort, multiply wrap

Status: needs-triage

**PARKED per user (security)** — do not implement without user ruling.

## What to build

Proposal 89's §Security classification names three hostile-input sites on the `BF`/`CF` chunk
path. Two of the three were closed on `main` by the LOADCHUNK hardening hotfix
(`acc073ea`, "fix(commands): harden BF/CF.LOADCHUNK deserialization against crafted payloads",
reachable from `d48e1b44`); the third is still live and is the actionable content of this issue.
All three are recorded below so the classification survives in one place.

**(A) `CuckooLayer::primary_index` divides by zero on a zero-bucket layer — LIVE.**
`frogdb-server/crates/types/src/cuckoo.rs:83-86` is `(hash as usize) % self.num_buckets`, with no
guard on `num_buckets == 0`. `CuckooLayer::from_raw` (`:59-73`) stores the caller's `num_buckets`
verbatim rather than `buckets.len()`, and **both** decoders accept a layer that declares zero
buckets: the persistence decoder does so deliberately and asserts it in a test
(`frogdb-server/crates/persistence/src/serialization/probabilistic.rs:1004-1013`, *"an empty layer
is not a hostile one"*), and the hardened command-side decoder only rejects the
`layer_bucket_size == 0 && num_buckets > 0` combination, so a `0/0` layer still passes. Once such a
value is stored, `CuckooFilterValue::add` (`types/src/cuckoo.rs:314-317`) calls
`insert_with_displaced` on the last layer, which reaches `primary_index` at `:114` — `hash % 0`,
an arithmetic panic. `delete` (`:185`) and the contains path (`:212`) reach it the same way.
Reachability is two-stage and worth stating precisely: the poisoned value can be *created* in any
build, because `RESTORE` deserializes through
`persistence/src/serialization/registry.rs:234` → `deserialize_cuckoo_filter` (not feature-gated)
via `ShardWorker::deserialize_transport_frame` (`core/src/shard/execution.rs:413`, called at
`:1097`), but the *panic* needs `CF.ADD`/`CF.DEL`/`CF.EXISTS`, which exist only under
`--features cmd-full`. The panic is contained by `core/src/shard/panic_guard.rs` (`PanicSite::Command`)
— the client gets `-ERR internal error` and the shard survives — so this is an availability nick
reachable from a stored value, not a process kill. The fix belongs in the filter implementation,
not the codec: `primary_index`/`alt_index` should refuse (or the layer constructor should reject)
a zero-bucket layer, so that a value blessed by the decoders cannot panic a later command.

**(B) `Vec::with_capacity` from unbounded wire counts → `handle_alloc_error` → `abort()` —
RESOLVED on `main` by `acc073ea`.** The sites were `commands/src/bloom.rs:625`
(`num_layers: u32` read at `:622`; ~206 GB from a 17-byte payload) and
`commands/src/cuckoo.rs:690` / `:717`. The severity note from the proposal is worth preserving
because it generalises: a *panic* raised in a command is contained by `panic_guard.rs`, but
`Vec::with_capacity` on an allocation the allocator refuses does **not** unwind — Rust calls
`handle_alloc_error`, which aborts, and `catch_unwind` cannot see it. `acc073ea` bounds each count
against the remaining input and clamps through a new `commands/src/utils.rs safe_capacity`,
mirroring the persistence decoder's discipline. **No work is required here**; the same sites are
also tracked by `.scratch/testing-improvements-round2/issues/open/70-unbounded-allocations-four-sites.md`
site 3, whose acceptance criteria should be re-checked against `acc073ea` rather than re-fixed.

**(C) Unchecked multiply at `cuckoo.rs:710` defeated the `:711` truncation guard — RESOLVED on
`main` by `acc073ea`.** `let fp_bytes = num_buckets * layer_bucket_size as usize * 2;` wrapped in
release builds (`[profile.release]` at root `Cargo.toml:202-204` sets `lto` and `codegen-units` and
**no** `overflow-checks`), handing `:711` a small product for a huge bucket count, which then drove
the `:717` allocation. The brief's "OOB-read bypass" framing was refuted during review: the read at
`:721` is `data[offset..offset + 2]`, a bounds-checked slice index, so the correct classification
is resource exhaustion, not memory safety. `acc073ea` replaced the multiply with a `checked_mul`
chain and rewrote the truncation guards to compare against remaining bytes. Recorded for the
classification record only.

## Acceptance criteria

- [ ] `CF.ADD` (and `CF.DEL`, `CF.EXISTS`) on a cuckoo filter whose layer declares
      `num_buckets == 0` returns a `CommandError` — or the zero-bucket layer is rejected at
      construction — instead of panicking; the shard serves a subsequent `PING`.
- [ ] The persistence decoder's `"an empty layer is not a hostile one"` expectation
      (`persistence/src/serialization/probabilistic.rs:1004-1013`) is re-ruled explicitly: either it
      keeps accepting the layer and `frogdb-types` makes it non-panicking, or it starts rejecting it
      and that test is updated in the same change. No silent divergence between the two decoders.
- [ ] Regression test `cuckoo_zero_bucket_layer_does_not_panic` in `frogdb-types`
      (`crates/types/src/cuckoo.rs` test module): build a `CuckooFilterValue` whose sole layer comes
      from `CuckooLayer::from_raw(vec![], 0, 4, 0, 0)`, then call `add`/`delete`/`contains` and
      assert none unwinds. Fails today.
- [ ] Bullets (B) and (C) are confirmed closed against `acc073ea` and **not** re-implemented.
- [ ] `just test frogdb-types cuckoo` green.

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 89 (`.scratch/arch-deepening/proposals/89-probabilistic-chunk-codec.md`),
§Security classification — flagged for the user (three sites; the "Adjacent, related, not claimed
by this proposal" paragraph is bullet A).

## Comments
