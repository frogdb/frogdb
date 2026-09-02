# 17: JSON tape format replacing the serde_json tree

Status: done
Type: AFK
Origin: memory-architecture PRD phase filing, 2026-09-01 — [PRD.md](../../PRD.md) R7
Area: frogdb-types (json.rs) + commands (json family, `json` feature)
Phase: 4 — value representation

## Why

`JsonValue` (`types/src/json.rs:18`) wraps `serde_json::Value` — a boxed-enum tree where every
object is a `Map<String, Value>`, every array a `Vec<Value>`, every string its own `String`.
A stored JSON document explodes into one allocation per node plus map overhead per object; a
10KB document routinely costs 3–5x its serialized size and scatters hundreds of small
allocations. This is the worst bytes-per-payload ratio of any value type.

Prior art: RedisJSON stores a custom tree (still pointer-heavy); the interesting designs are
**tape/arena representations** — simdjson's tape (flat array of 64-bit words: type tags,
scalar payloads, container skip-offsets, string bytes in a side buffer) and serde-json-borrow /
DOM-over-arena variants. A tape gives contiguous storage, one or two allocations per document,
cheap whole-document serialization, and O(children) path steps with skip-offsets.

Same feature-gating note as [issue 16](../): `json` is a
non-default cargo feature (`frogdb-server/crates/commands/Cargo.toml:19`, in `full`) — command
tests need `--features json`/`full`, don't thrash flags in iteration loops, and verify CI runs
the JSON suite under `full`. The type itself compiles unconditionally in frogdb-types.

## What to build

### 1. Tape representation

`JsonValue` becomes: a `Vec<u64>` tape + a byte buffer for string/number text. Node kinds:
null/true/false, i64/f64 inline, string (offset+len into byte buffer), object/array headers
carrying child count and skip-offset to the node after the container. Design for **read-mostly
with whole-subtree replacement**, because that is what the JSON.* command mix does.

### 2. Mutation strategy — rebuild, don't splice

In-place tape splicing (shifting the tape on JSON.SET of a nested key) is the classic
complexity trap. Take the simple ruling: mutations rebuild the tape (stream old tape → new
tape with the edit applied, single pass, O(document)). Redis's own JSON.SET on a path is
already O(path search) + tree surgery with allocator traffic; a linear rebuild of a contiguous
tape is competitive for realistic document sizes and *vastly* simpler. If profiling later
shows large-document mutation hotspots, a chunked tape is the escape hatch — not now.

### 3. Command surface

JSON.GET/SET/DEL/TYPE/STRLEN/ARRLEN/ARRAPPEND/NUMINCRBY/OBJKEYS/TOGGLE etc. currently operate
on `serde_json::Value` via path evaluation. Introduce a cursor/visitor API over the tape
(`TapeRef` with child iteration and skip) and port the path evaluator to it. Parsing:
serde_json's parser can drive tape construction via a `Deserializer` visitor (no new parser
dependency); serialization walks the tape (current `to_vec`/`to_string` call sites at
`json.rs` ~220–232).

- Number fidelity: today's storage inherits serde_json semantics (i64/u64/f64 distinction,
  `arbitrary_precision` off). Preserve observed behavior exactly — JSON.NUMINCRBY edge cases
  are regression-tested; do not improve and break compatibility in the same change.
- `memory_size()`: tape len + byte-buffer len + constants — run-stable by construction.

### 4. Interop seams

Persistence and replication serialize documents as JSON text (verify) — the tape is invisible
to them. RESP3 replies that encode JSON stay byte-identical.

## Acceptance criteria

- [ ] `JsonValue` is tape+buffer; no `serde_json::Value` in stored state (parse-time use is
      fine).
- [ ] Full JSON regression suite green under `--features json`/`full`; CI coverage verified.
- [ ] Memory-shape test: stored size of a representative 10KB document within ~1.5x serialized
      length (record actual ratio in resolution).
- [ ] Round-trip fuzz: arbitrary JSON → tape → serialize == serde_json canonical output;
      mutation fuzz vs a `serde_json::Value` model for the path-mutation commands.
- [ ] `memory_size()` run-stable; MEMORY USAGE reflects tape size.

## Test boundary

Level 1 round-trip and model fuzz (this is the highest-value fuzz target in phase 4 — parser
adjacent). Level 2 regression under the feature flag.

## Spec rows at R15

None new; generic rows via `memory_size()`.

## Out of scope

New parser dependencies (simd parsing), arbitrary-precision numbers, chunked/spliceable tapes,
JSONPath feature extensions, changing persistence encoding.

## Depends on

Nothing in phases 3–5 — the tape shares no machinery with the listpack blocks. Filed in
phase 4 with the other R7 conversions; can proceed in parallel with
[issues 13–16](../).

## Resolution

Landed 2026-09-02 on `mem-arch-integration` (picks `cda0d82e`..`c4972526`, 6 commits).

What shipped: `JsonValue` stores a flat tape (`Vec<u64>` of tagged words + interned side
text buffer, one word per node, O(1) subtree skip) instead of a `serde_json::Value` tree.
`TapeRef` cursor API; JSONPath evaluator and all JSON commands ported; mutations rebuild in
one streaming pass (multi-match mutations now atomic — deliberate improvement). Search
extractor takes the tape directly (no per-document tree materialization). Memory: 1.38x
stored/serialized on the representative fixture, under the ~1.5x target (tree draft was
2.78x). Fuzz: round-trip vs serde's own output + a widened mutation proptest (7 ops x
Key/Nested/Wild paths against a serde model).

Review round 1 (2 Important, several Minors) fixed, re-review clean. Gates: full
`frogdb-server` suite 2116/2116 (one unrelated auto-failover flaky retry), redis-regression
2321 green, lint-gates via lefthook.

Known trade-offs carried in the report: `container_len()` O(children) by design; fragment
parse does one extra O(fragment) pass; `frogdb-protocol` float behavior for U+007F-U+009F on
the pretty-print path kept byte-identical to before.
