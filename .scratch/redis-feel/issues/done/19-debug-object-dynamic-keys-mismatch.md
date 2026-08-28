# DEBUG OBJECT: key metadata declares a key the dispatch never accepts

Status: done

## Origin

Found while working issue 15 (vendored key specs vs real key extraction). The
`DEBUG` container's key-extraction override and its dispatch disagree about
whether `DEBUG OBJECT` exists.

## What is wrong

`DebugConnCommand::dynamic_keys` claims `DEBUG OBJECT <key>` is keyed:

`frogdb-server/crates/server/src/connection/debug_conn_command.rs:89-97`

```rust
fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Option<Vec<&'a Bytes>> {
    let sub = args.first()?.to_ascii_uppercase();
    match sub.as_slice() {
        b"OBJECT" | b"EXPIRE-BACKDATE" if args.len() >= 2 => Some(vec![&args[1]]),
        ...
    }
}
```

The dispatch `match subcommand.as_slice()` at
`debug_conn_command.rs:121-263` has arms for `JMAP`, `SLEEP`, `SET-ACTIVE-EXPIRE`,
`QUICKLIST-PACKED-THRESHOLD`, `STRINGMATCH-LEN`, `CHANGE-REPL-ID`,
`EXPIRE-BACKDATE`, … but **no `b"OBJECT"` arm**. `DEBUG OBJECT k` therefore
falls to the catch-all at `debug_conn_command.rs:260`:

```
ERR Unknown DEBUG subcommand 'OBJECT'
```

So the two halves contradict each other:

- `COMMAND GETKEYS DEBUG OBJECT k` → `k` (the override answers)
- `DEBUG OBJECT k` → `ERR Unknown DEBUG subcommand 'OBJECT'`

Two further places assert the subcommand exists:

- `debug_conn_command.rs:334-335` — `debug_help()` advertises
  `"DEBUG OBJECT <key>"` / `"    Inspect key internals."`.
- `debug_conn_command.rs:1009` — unit test `dynamic_keys_extracts_object_key_only`
  pins the override, so the dead declaration is actively defended by a test.
- `debug_conn_command.rs:22-24` — the module docs say the override "supplies
  DEBUG OBJECT's key directly".

## Why it matters

Untruthful metadata, which is exactly the class the D2/issue-15 key-spec gates
exist to eliminate. A cluster-aware client that trusts `COMMAND GETKEYS` will
route `DEBUG OBJECT k` to `k`'s slot owner and get an unknown-subcommand error;
a proxy or ACL key-pattern check will grant/deny on a key the command never
reads. The `HELP` text makes it a documented promise on top of that.

The declaration is also dead code by definition: no dispatch path can ever
consume the key it extracts, so nothing in the crate exercises the `OBJECT` half
of that match arm except the unit test that pins it.

## Candidate directions

Two coherent endings, and picking one is the triage decision:

1. **Implement `DEBUG OBJECT`** — Redis reports `serializedlength`, `encoding`,
   `ql_nodes`, … for the key. The metadata then becomes true, and the `HELP`
   line stops being a lie. Cost: the reply fields are encoding-internals that
   FrogDB's storage does not have one-to-one, so the reply is a design act, not
   a port.
2. **Remove the claim** — drop `b"OBJECT"` from `dynamic_keys`, drop the `HELP`
   line, and narrow the unit test to `EXPIRE-BACKDATE`. `COMMAND GETKEYS DEBUG
   OBJECT k` then reports no keys, matching what the command actually does.

Either way the fix should come with a regression test that runs both halves
against each other (`COMMAND GETKEYS` vs actual dispatch) for the DEBUG
container, so a future subcommand cannot re-open the same gap silently.

## Ruling (2026-08-28)

Direction 1 — **implement `DEBUG OBJECT`** with a truthful-fields-only reply:

| Token | Plan |
|---|---|
| `at:<ptr>` | omit — heap address is an ASLR info leak; deviation-as-improvement, documented |
| `refcount` | emit, reuse the OBJECT REFCOUNT arm's logic |
| `encoding` | emit, reuse the OBJECT ENCODING arm's names (self-consistent) |
| `serializedlength` | emit only if the persistence value codec is cheaply reusable for a byte length; otherwise omit first pass (fabricated length is worse than an absent token) |
| `lru`, `lru_seconds_idle` | emit via the IDLETIME/FREQ logic |
| `ql_*` | omit — quicklist internals do not map to our list repr |

Missing key: match Redis (`ERR no such key`).

Mandatory regardless of reply shape: a container-wide regression test that
cross-checks every `dynamic_keys` declaration against the dispatch arms for the
DEBUG container, so a declared-but-undispatched subcommand can never reappear.
Update `debug_help()` and the module docs to match reality. Vendored regression
suites that excluded DEBUG OBJECT tests (`incr_tcl.rs`, `expire_tcl.rs`, ...)
may be revisited in a follow-up, not in this change.

## Resolution (2026-08-28)

Implemented (`76df081d`, merged `e0d31826`). `DEBUG OBJECT <key>` is a keyed
read routed like EXPIRE-BACKDATE (`shard_for_key` ->
`DebugIntrospectionMsg::ObjectInfo` -> `collect_object_info` in the shard event
loop). Reply: `refcount:<n> encoding:<name> serializedlength:<n> lru:<n>
lru_seconds_idle:<n>`.

- `at:` omitted — heap pointer is an ASLR leak; deviation-as-improvement,
  documented at the formatter. `ql_*` omitted — lists are not quicklists.
- `serializedlength` is a real measured encode: new
  `serialized_payload_len` in `frogdb-persistence` (frame minus its fixed
  24-byte header; same approach as Redis `rdbSavedObjectLen`). Forcing test
  lives in the locked crate; `mutants-diff frogdb-persistence` run before push.
- Encoding/refcount logic now shared, not copied: `Value::encoding_name()` +
  `Value::REPORTED_REFCOUNT` in the types crate; OBJECT ENCODING/REFCOUNT read
  from them (no behavior change).
- Missing or expired-but-unswept key -> `ERR no such key`; collector does not
  touch access metadata (Redis `LOOKUP_NOTOUCH` equivalent).

Container-wide cross-checks added so the gap cannot reopen:
`every_keyed_subcommand_is_dispatched`,
`every_help_advertised_subcommand_is_dispatched`
(`debug_conn_command.rs`). Integration:
`integration_debug_introspection.rs` (fields agree with OBJECT, int vs raw
encodings, missing/expired key, GETKEYS agrees with dispatch).

Follow-up (not here): revisit vendored regression suites that excluded
DEBUG OBJECT tests (`incr_tcl.rs`, `expire_tcl.rs`, ...).
