# `COMMAND INFO` returns hardcoded placeholder metadata

Status: ready-for-agent
Type: bug (introspection accuracy)
Area: commands / registry

## Problem

`COMMAND INFO <name>` returns arity `-1`, key-spec `0 0 0`, and no ACL categories, in a
6-element reply. Redis 8.6 returns 10 elements, with real arity, a real first/last/step
key-spec triplet, and the command's `@`-prefixed ACL categories.

Site: `frogdb-server/crates/commands/src/basic.rs:240-247` hardcodes these values instead of
reading them from the registry.

## What data already exists

- **Arity**: `CommandImpl::arity()` (`frogdb-server/crates/core/src/registry.rs:54-59`).
- **ACL categories**: `CommandCategory::all_for_command`
  (`frogdb-server/crates/acl/src/categories/mod.rs:149-154`).
- **Key spec**: nothing derives the `(first, last, step)` triplet today — write a mapping from
  the existing `KeySpec` enum (`frogdb-server/crates/core/src/command_spec.rs:24-54`):

  | `KeySpec` variant | `(first, last, step)` |
  |---|---|
  | `First` | `(1, 1, 1)` |
  | `FirstTwo` | `(1, 2, 1)` |
  | `Index(i)` | `(i+1, i+1, 1)` |
  | `All` | `(1, -1, 1)` |
  | `Skip(n)` | `(n+1, -1, 1)` |
  | `NumkeysAt` / `Dynamic` | `(0, 0, 0)` + `movablekeys` flag set |

## Fix

Emit the full 10-element `COMMAND INFO` reply: name, arity, flags, key-spec triplet, ACL
categories, tips (empty array is fine), key-specs (structured form), subcommands (from the
registry where applicable). `COMMAND COUNT` reads a different code path and is unaffected.

## Cross-reference

[Issue 03](../) (`COMMAND DOCS`) touches the same dispatch arm in `basic.rs` — land with an eye
to that diff to avoid re-churning the same match arm twice.

## Acceptance criteria

- [ ] `COMMAND INFO get` matches Redis 8.6: arity `2`, key-spec `1 1 1`, categories include
      `@read`, 10-element reply shape
- [ ] A command with a dynamic key spec (e.g. `ZADD` with `GT`/`LT` flags or a `NumkeysAt`
      command like `EVAL`) reports `movablekeys` and `(0,0,0)` correctly
- [ ] `COMMAND COUNT` unchanged
- [ ] Regression coverage for at least one command per `KeySpec` variant

Size: S
