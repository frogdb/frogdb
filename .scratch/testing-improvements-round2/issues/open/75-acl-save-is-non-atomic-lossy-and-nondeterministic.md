# `ACL SAVE` is non-atomic, whitespace-lossy and nondeterministic — a restart can drop or mangle every rule

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/15 F4 · MASTER.md §3 (durability)
Score: severity 5 · likelihood 3 · effort 2 · priority 19
Area: frogdb-acl / aclfile persistence

## Context

`ACL SAVE` truncates the ACL file in place with no temp file, no rename and no fsync, so a crash
mid-save leaves a half-file — and `load()` then fails the *whole* file on any single bad line,
so the server refuses to boot or comes up with a different authorization set than the operator
configured. Independently, the parser splits on whitespace, so any key pattern or password
containing a space round-trips to a different rule set; and password hashes are emitted by
iterating a `HashSet`, so two saves of the same state produce different files. Silent
authorization drift across restart.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly.

## Evidence

`acl/src/manager.rs:256` — `save()` is `File::create(aclfile)` + `write_all`, **no temp file,
no rename, no fsync**. `acl/src/manager.rs:280` — `load()` aborts the entire file on the first
parse error. `acl/src/parser.rs` `parse_acl_line` splits on `line.split_whitespace()`, so any key
pattern or password containing a space round-trips to a different rule set (or an unparseable
line → the server cannot load its own ACL file). `acl/src/user.rs:167-173` `to_acl_string` emits
password hashes by iterating a `HashSet<[u8;32]>`, so file content is **nondeterministic between
saves**. Depth: `acl/src/manager.rs` has 21 untested + 26 single-test functions.

Why the existing test passes anyway: the one existing test (`acl/src/manager.rs` test module,
~545-623) asserts user *existence* and a single `authenticate`, not permission equivalence.

## What to fix

1. Make `save()` atomic: write to a temp file, fsync it, rename, fsync the directory.
2. Make `to_acl_string` deterministic — sort the password hashes and any other set-derived
   output.
3. Quote or escape values containing whitespace on write and handle them on parse, so the
   round-trip is lossless.
4. Decide and implement `load()`'s partial-failure behaviour so a bad line cannot silently
   change the effective ACL; leave the in-memory ACL untouched on error.

## Acceptance criteria

- [ ] A round-trip property test builds N users with adversarial content (patterns with spaces,
      `~`/`%R~`/`%W~` mixes, unicode, `&` channels, ratelimit rules, multiple passwords),
      `save()`s, does a fresh `AclManager::load()`, and asserts for every user that the **full
      permission verdict matrix** over a fixed list of (command, subcommand, key, channel) probes
      is identical — not merely that the user exists. Fails today.
- [ ] `save()` called twice produces byte-identical files. Fails today.
- [ ] A truncated file makes `load()` leave the in-memory ACL untouched and report an error
      rather than partially applying.
- [ ] One level-4 test: `ACL SETUSER` → `ACL SAVE` → restart `TestServer` → assert the NOPERM
      matrix, proving the wiring.

## Test boundary

Level 2 — `AclManager` plus a tempdir is the whole behaviour; a server adds only the RESP
wrapper. The one additional level-4 test is worth having because it proves the wiring, which
level 2 cannot: nothing below the server exercises the `ACL SAVE`/restart path end to end.

## Depends on

nothing for the level-2 work. The level-4 companion wants the `TestServer` restart-in-place
helper listed as item 4 of issue 12 (I12 — config observability seams),
`.scratch/testing-improvements-round2/issues/`, if one does not already exist.

## Re-triage 2026-08-06

**Verdict: still-valid** (one acceptance criterion turns out to be already satisfied)

ACL is outside the four locked areas and nothing changed. `AclManager::save`
(`frogdb-server/crates/acl/src/manager.rs:256-277`, cite `:256` still exact) is `File::create` +
`write_all` — no temp file, no rename, no fsync, no directory fsync. `to_acl_string` moved:
`acl/src/user.rs:167-173` → **`user.rs:122-138`**, and `:136-138` still emits password hashes by
iterating `password_hashes: HashSet<[u8; 32]>` (`user.rs:17`), so two saves of identical state
differ; `save` compounds it by iterating `users.values()` of a `HashMap` at `manager.rs:262`, so
*user order* is nondeterministic too. `parse_acl_line` (`acl/src/parser.rs:585`) still splits on
`line.split_whitespace()` at `:597`, so any pattern or password containing a space round-trips to a
different rule set. `load` (`:280-321`) still aborts the whole file on the first parse error
(`:300-303`).

Correction to the body: acceptance criterion 3 ("a truncated file makes `load()` leave the
in-memory ACL untouched") **already holds today** — `load` builds a local `new_users` map and only
swaps it in at `manager.rs:317-318`, after every line has parsed. The remaining gap there is the
*policy* half of `What to fix` item 4 (whole-file abort vs. partial application), not the
untouched-on-error property.
