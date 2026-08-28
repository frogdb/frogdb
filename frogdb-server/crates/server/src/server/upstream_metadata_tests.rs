//! Join tests: the live command registry against the vendored upstream
//! metadata in [`frogdb_commands::upstream`].
//!
//! Three gates, all driven off the same `register_commands` registry the
//! server boots with:
//!
//! 1. **Coverage** — every registered command joins a vendored row, except an
//!    exact, wildcard-free allowlist of genuinely FrogDB-only commands. A new
//!    Redis-compat command with no vendored row fails here.
//! 2. **Truthfulness** — for every command upstream publishes key specs for,
//!    replaying those specs over a synthetic argv (Redis's own
//!    `getKeysUsingKeySpec` algorithm) must agree with the keys FrogDB's
//!    dispatch actually extracts from that argv.
//! 3. **Arity** — the vendored wire arity must equal what `COMMAND INFO`
//!    reports for our `CommandSpec`.
//!
//! Container commands are gated the same three ways one subcommand at a time,
//! joining each declared `SubcommandSpec` row to the vendored subcommand row of
//! the same name — a container's own vendored row carries none of this.
//!
//! Per ADR-0005 the vendored data never becomes the answer FrogDB gives; it is
//! only the thing our real behavior is checked against. Every divergence is
//! either fixed or written down as a named exemption with a reason, and a
//! stale exemption fails the test just as loudly as a new divergence.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use frogdb_commands::command_meta::command_info_arity;
use frogdb_commands::upstream::{self, BeginSearch, FindKeys, UpstreamCommand};
use frogdb_core::CommandRegistry;

use super::register::register_commands;

fn full_registry() -> CommandRegistry {
    let mut registry = CommandRegistry::new();
    register_commands(&mut registry);
    registry
}

// ---------------------------------------------------------------------------
// Coverage
// ---------------------------------------------------------------------------

/// Commands FrogDB registers that no upstream source documents, spelled out in
/// full — no prefixes, no wildcards — so that adding a command is a deliberate
/// act in one of two directions: it joins a vendored row, or it is named here.
///
/// Feature-gated exactly like `register_commands`, so the list is exhaustive
/// under both profiles this test builds with (`cmd-core` by default,
/// `cmd-full` in the workspace-unified CI run).
fn frogdb_only_commands() -> BTreeSet<&'static str> {
    #[allow(unused_mut)]
    let mut names: BTreeSet<&'static str> = [
        // Operator-facing verbs with no Redis counterpart.
        "FROGDB.FINALIZE",
        "FROGDB.HOTSHARDS",
        "FROGDB.VERSION",
        "STATUS",
    ]
    .into_iter()
    .collect();

    // Event sourcing is a FrogDB extension family in its entirety.
    #[cfg(feature = "cmd-event-sourcing")]
    names.extend([
        "ES.ALL",
        "ES.APPEND",
        "ES.INFO",
        "ES.READ",
        "ES.REPLAY",
        "ES.SNAPSHOT",
    ]);

    names
}

/// Whether `name` joins something in the vendored snapshots — either a command
/// row, or a container a module family documents only through its subcommands
/// (`FT.CONFIG` exists upstream only as `FT.CONFIG GET`/`FT.CONFIG SET`).
fn is_vendored(name: &str) -> bool {
    upstream::command(name).is_some() || upstream::is_module_container(name)
}

#[test]
fn every_registered_command_joins_vendored_metadata() {
    let allowlist = frogdb_only_commands();
    let registry = full_registry();

    let missing: Vec<&str> = registry
        .iter()
        .map(|(name, _)| name)
        .filter(|name| !allowlist.contains(name) && !is_vendored(name))
        .collect();

    assert!(
        missing.is_empty(),
        "registered commands with no vendored upstream metadata: {missing:?}. \
         Either the vendored snapshots are stale (`just redis-commands-vendor` \
         then `just command-metadata-gen`), or these are FrogDB-only commands \
         that belong in `frogdb_only_commands()`."
    );
}

#[test]
fn frogdb_only_allowlist_has_no_dead_weight() {
    let registry = full_registry();

    for name in frogdb_only_commands() {
        assert!(
            registry.get_entry(name).is_some(),
            "`frogdb_only_commands()` lists {name}, which is not registered in \
             this build — remove it"
        );
        assert!(
            !is_vendored(name),
            "{name} is now documented upstream — drop it from \
             `frogdb_only_commands()` so it is checked like everything else"
        );
    }
}

/// The registry keys itself by the ASCII-uppercase name, so a `CommandSpec`
/// whose own `name` is spelled differently still dispatches — but every
/// metadata path that joins on `spec.name` (the vendored key-spec, tip,
/// argument and history lookups in `frogdb_commands::command_meta`) silently
/// misses, and in debug builds `upstream::find` panics on the case mismatch.
/// `CLUSTER` shipped that way; this keeps it from happening again.
#[test]
fn spec_names_match_their_registry_key() {
    let registry = full_registry();

    let mismatched: Vec<(&str, &str)> = registry
        .iter()
        .map(|(key, entry)| (key, entry.spec().name))
        .filter(|(key, name)| key != name)
        .collect();

    assert!(
        mismatched.is_empty(),
        "`CommandSpec::name` must be the ASCII-uppercase wire name — the \
         registry uppercases its keys, so these dispatch fine but drop out of \
         every vendored-metadata join: {mismatched:?}"
    );
}

// ---------------------------------------------------------------------------
// Key-spec truthfulness
// ---------------------------------------------------------------------------

/// Argv for the commands whose key specs are found by scanning for a keyword.
/// A generic placeholder argv cannot exercise those — the keyword has to sit
/// where the real command's grammar puts it — so each is written out, with the
/// tokens upstream searches for in their real positions. Every element must be
/// distinct: the check maps extracted key bytes back to argv indices.
const KEYWORD_ARGV: &[(&str, &[&str])] = &[
    // `GEORADIUS key lon lat radius unit [STORE dst] [STOREDIST dst]` — STORE
    // is exercised, STOREDIST is absent (upstream finds no key for it, and
    // real GEORADIUS takes at most one destination).
    (
        "GEORADIUS",
        &["GEORADIUS", "src", "1", "2", "3", "km", "STORE", "dst"],
    ),
    // Same command shape one argument shorter (member instead of lon/lat), and
    // the other destination keyword, so both branches are covered once.
    (
        "GEORADIUSBYMEMBER",
        &[
            "GEORADIUSBYMEMBER",
            "src",
            "m1",
            "3",
            "km",
            "STOREDIST",
            "dst",
        ],
    ),
    // The single-key form. The `KEYS` form is deliberately not exercised: it
    // requires the positional key to be the empty string, which upstream's own
    // static index spec still reports as a key (Redis overrides it at runtime
    // with `migrateGetKeys`), so a static comparison there compares against a
    // spec Redis itself does not trust.
    ("MIGRATE", &["MIGRATE", "host", "6379", "mykey", "0", "100"]),
    (
        "XREAD",
        &["XREAD", "COUNT", "2", "STREAMS", "s1", "s2", "0", "1"],
    ),
    (
        "XREADGROUP",
        &[
            "XREADGROUP",
            "GROUP",
            "g1",
            "c1",
            "STREAMS",
            "s1",
            "s2",
            "0",
            "1",
        ],
    ),
];

/// Commands whose vendored key specs genuinely disagree with FrogDB's key
/// extraction, each with the reason. A stale entry (the divergence went away)
/// fails the test, so the list can only shrink by accident.
const KEY_SPEC_EXEMPTIONS: &[(&str, &str)] = &[(
    "MOVE",
    "MOVE is a deliberate stub (`commands::stub`, `is_stub()`), and stubs are \
         registered keyless because they reject before touching the keyspace",
)];

/// How many keys the synthetic argv declares for a `keynum` spec.
const SYNTHETIC_KEYNUM: i32 = 2;

/// Baseline argv length for open-ended commands — long enough that a
/// `lastkey: -1` range and a two-key `keynum` both have room.
const SYNTHETIC_ARGC: i32 = 8;

/// Build a synthetic argv for a command whose key specs are all index-based.
/// Element `i` is `ki` unless a `keynum` spec claims it, in which case it is
/// the count — `ki` never parses as an integer, so a count is unambiguous and
/// every element stays distinct.
fn synthetic_argv(cmd: &UpstreamCommand) -> Vec<Bytes> {
    let specs = cmd.key_specs.unwrap_or(&[]);
    // Exact arities are honored as-is: a longer argv is a shape the command
    // can never receive, so extracting keys from it proves nothing.
    let exact = matches!(cmd.arity, Some(arity) if arity > 0);
    let mut argc = match cmd.arity {
        Some(arity) if arity > 0 => arity,
        Some(arity) => -arity,
        None => 1,
    };
    if !exact {
        argc = argc.max(SYNTHETIC_ARGC);
        for spec in specs.iter().filter(|spec| spec.is_complete()) {
            let BeginSearch::Index { pos } = spec.begin_search else {
                continue;
            };
            argc = argc.max(match spec.find_keys {
                FindKeys::Range { lastkey, .. } if lastkey >= 0 => pos + lastkey + 1,
                FindKeys::Range { lastkey, .. } => pos + 1 - lastkey,
                FindKeys::Keynum {
                    keynumidx,
                    firstkey,
                    step,
                } => (pos + keynumidx + 1).max(pos + firstkey + (SYNTHETIC_KEYNUM - 1) * step + 1),
                FindKeys::Unknown => 0,
            });
        }
    }

    let mut argv: Vec<String> = (0..argc).map(|i| format!("k{i}")).collect();
    argv[0] = cmd.name.to_string();
    for spec in specs {
        if let (
            BeginSearch::Index { pos },
            FindKeys::Keynum {
                keynumidx,
                firstkey,
                step,
            },
        ) = (spec.begin_search, spec.find_keys)
        {
            let idx = (pos + keynumidx) as usize;
            let last = pos + firstkey + (SYNTHETIC_KEYNUM - 1) * step;
            if idx < argv.len() && last < argc {
                argv[idx] = SYNTHETIC_KEYNUM.to_string();
            }
        }
    }
    argv.into_iter().map(Bytes::from).collect()
}

/// Redis's `getKeysUsingKeySpec`, replayed over the vendored specs: which argv
/// indices does upstream say hold keys? Only complete specs contribute;
/// `Unknown` ones are reported through the returned flag so the caller can
/// weaken equality to containment.
///
/// `Err` means the synthetic argv is malformed for this spec (Redis's own
/// `invalid_spec` path) — a bug in `synthetic_argv`, not a divergence.
fn upstream_key_indices(
    cmd: &UpstreamCommand,
    argv: &[Bytes],
) -> Result<(BTreeSet<usize>, bool), String> {
    let argc = argv.len() as i32;
    let mut keys = BTreeSet::new();
    let mut incomplete = false;

    for spec in cmd.key_specs.unwrap_or(&[]) {
        if !spec.is_complete() {
            incomplete = true;
            continue;
        }
        // `NOT_KEY` specs (the shard-pubsub channels of SPUBLISH/SSUBSCRIBE/
        // SUNSUBSCRIBE) are deliberately kept: upstream marks them "not a
        // keyspace key" but still routes them by slot through the key-spec
        // machinery, and FrogDB routes them the same way — through key
        // extraction — so they belong in this comparison.
        let first = match spec.begin_search {
            BeginSearch::Index { pos } => pos,
            BeginSearch::Keyword { keyword, startfrom } => {
                let forward = startfrom > 0;
                let start = if forward { startfrom } else { argc + startfrom };
                let end = if forward { argc - 1 } else { 1 };
                let mut found = None;
                let mut i = start;
                while i != end && i > 0 && i < argc {
                    if argv[i as usize].eq_ignore_ascii_case(keyword.as_bytes()) {
                        found = Some(i + 1);
                        break;
                    }
                    i += if forward { 1 } else { -1 };
                }
                match found {
                    Some(first) => first,
                    // Redis treats "keyword absent" as "this spec yields no
                    // keys", not as an error.
                    None => continue,
                }
            }
            BeginSearch::Unknown => unreachable!("filtered by is_complete"),
        };
        if first < 1 || first >= argc {
            return Err(format!(
                "{}: first key index {first} outside argv of {argc}",
                cmd.name
            ));
        }

        match spec.find_keys {
            FindKeys::Range {
                lastkey,
                step,
                limit,
            } => {
                if step < 1 {
                    return Err(format!("{}: range step {step} < 1", cmd.name));
                }
                let last = if lastkey >= 0 {
                    first + lastkey
                } else if limit == 0 {
                    argc + lastkey
                } else {
                    first + ((argc - first) / limit + lastkey)
                };
                let mut i = first;
                while i <= last {
                    if i >= argc {
                        return Err(format!(
                            "{}: range key index {i} outside argv of {argc}",
                            cmd.name
                        ));
                    }
                    keys.insert(i as usize);
                    i += step;
                }
            }
            FindKeys::Keynum {
                keynumidx,
                firstkey,
                step,
            } => {
                if step < 1 {
                    return Err(format!("{}: keynum step {step} < 1", cmd.name));
                }
                let idx = (first + keynumidx) as usize;
                let count: i32 = std::str::from_utf8(&argv[idx])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| format!("{}: argv[{idx}] is not a key count", cmd.name))?;
                let base = first + firstkey;
                for n in 0..count {
                    let i = base + n * step;
                    if i >= argc {
                        return Err(format!(
                            "{}: keynum key index {i} outside argv of {argc}",
                            cmd.name
                        ));
                    }
                    keys.insert(i as usize);
                }
            }
            FindKeys::Unknown => unreachable!("filtered by is_complete"),
        }
    }

    Ok((keys, incomplete))
}

/// Run FrogDB's real key extraction — the same `CommandImpl::keys` dispatch
/// calls — and map the extracted keys back to argv indices.
fn frogdb_key_indices(registry: &CommandRegistry, name: &str, argv: &[Bytes]) -> BTreeSet<usize> {
    let mut by_value: BTreeMap<&[u8], usize> = BTreeMap::new();
    for (i, arg) in argv.iter().enumerate() {
        assert!(
            by_value.insert(arg.as_ref(), i).is_none(),
            "{name}: synthetic argv has a duplicate element {:?}; argv elements \
             must be distinct so extracted keys map back to positions",
            String::from_utf8_lossy(arg)
        );
    }
    let entry = registry
        .get_entry(name)
        .expect("caller checked registration");
    entry
        .keys(&argv[1..])
        .into_iter()
        .map(|key| {
            *by_value.get(key).unwrap_or_else(|| {
                panic!(
                    "{name}: extracted key {:?} is not an element of the synthetic argv",
                    String::from_utf8_lossy(key)
                )
            })
        })
        .collect()
}

#[test]
fn vendored_key_specs_agree_with_key_extraction() {
    let registry = full_registry();
    let keyword_argv: BTreeMap<&str, &[&str]> = KEYWORD_ARGV.iter().copied().collect();
    let exemptions: BTreeMap<&str, &str> = KEY_SPEC_EXEMPTIONS.iter().copied().collect();
    let mut checked = 0usize;
    let mut used_exemptions = BTreeSet::new();
    // Every command is checked before anything is asserted, so one run reports
    // the whole divergence set rather than only the alphabetically first.
    let mut divergences: Vec<String> = Vec::new();

    for (name, entry) in registry.iter() {
        let Some(cmd) = upstream::command(name) else {
            continue;
        };
        let Some(specs) = cmd.key_specs else {
            // Module families publish no key-spec data at all — there is
            // nothing to check against, and inventing one would be a claim we
            // cannot support.
            continue;
        };
        if cmd.has_subcommands() {
            // Upstream declares a container's real key specs on its subcommand
            // rows, so the container's own empty spec list means "described
            // there", not "takes no keys" — and the argv this loop synthesizes
            // (`OBJECT k1 k2 ...`) names no subcommand, so it is a shape the
            // container rejects rather than extracts keys from. The rows are
            // checked instead, one resolved subcommand at a time, by
            // `vendored_subcommand_key_specs_agree_with_key_extraction`.
            continue;
        }
        let needs_keyword_argv = specs
            .iter()
            .any(|spec| matches!(spec.begin_search, BeginSearch::Keyword { .. }));

        let argv: Vec<Bytes> = match keyword_argv.get(name) {
            Some(words) => words.iter().map(|w| Bytes::from(*w)).collect(),
            None => {
                assert!(
                    !needs_keyword_argv,
                    "{name} has a keyword-based key spec but no entry in \
                     KEYWORD_ARGV — a placeholder argv cannot place the keyword \
                     where the command's grammar expects it"
                );
                synthetic_argv(cmd)
            }
        };

        let (expected, incomplete) =
            upstream_key_indices(cmd, &argv).expect("synthetic argv is valid");
        let actual = frogdb_key_indices(&registry, name, &argv);

        // A command with an `unknown` spec (SORT's BY/GET/STORE) is not
        // statically decidable, so upstream's complete specs are only a lower
        // bound on the keys — containment, not equality.
        let agrees = if incomplete {
            actual.is_superset(&expected)
        } else {
            actual == expected
        };

        match exemptions.get(name) {
            Some(reason) => {
                if agrees {
                    divergences.push(format!(
                        "{name} is exempt from the key-spec check (\"{reason}\") but now \
                         agrees with upstream — remove the exemption"
                    ));
                }
                used_exemptions.insert(name);
            }
            None => {
                if !agrees {
                    divergences.push(format!(
                        "{name}: upstream {expected:?}{}, frogdb {actual:?} (spec {:?}) \
                         for argv {:?}",
                        if incomplete { " (lower bound)" } else { "" },
                        entry.spec().keys,
                        argv.iter()
                            .map(|a| String::from_utf8_lossy(a).into_owned())
                            .collect::<Vec<_>>(),
                    ));
                }
                checked += 1;
            }
        }
    }

    assert!(
        divergences.is_empty(),
        "upstream key specs and FrogDB key extraction disagree for {} command(s). \
         Fix the extraction, or add a KEY_SPEC_EXEMPTIONS entry with a reason.\n{}",
        divergences.len(),
        divergences.join("\n")
    );
    assert!(
        checked > 100,
        "only {checked} commands were key-spec checked — the join looks broken"
    );
    for (name, _) in KEY_SPEC_EXEMPTIONS {
        assert!(
            used_exemptions.contains(name) || registry.get_entry(name).is_none(),
            "KEY_SPEC_EXEMPTIONS lists {name}, which is registered but was never \
             reached by the check — remove the stale entry"
        );
    }
}

#[test]
fn every_keyword_argv_entry_is_live() {
    let registry = full_registry();
    for (name, words) in KEYWORD_ARGV {
        assert_eq!(
            words.first().map(|w| w.to_ascii_uppercase()),
            Some(name.to_string()),
            "KEYWORD_ARGV entry for {name} does not start with the command name"
        );
        if registry.get_entry(name).is_none() {
            continue; // family not compiled into this build
        }
        let cmd = upstream::command(name)
            .unwrap_or_else(|| panic!("KEYWORD_ARGV lists {name}, which is not vendored"));
        assert!(
            cmd.key_specs
                .unwrap_or(&[])
                .iter()
                .any(|spec| matches!(spec.begin_search, BeginSearch::Keyword { .. })),
            "KEYWORD_ARGV lists {name}, which no longer has a keyword-based key \
             spec — the generic synthetic argv covers it now"
        );
    }
}

// ---------------------------------------------------------------------------
// Arity
// ---------------------------------------------------------------------------

/// Commands whose wire arity deliberately differs from upstream's, each with
/// the reason. Stale entries fail, same discipline as the key-spec list.
const ARITY_EXEMPTIONS: &[(&str, &str)] = &[(
    "PSYNC",
    "upstream's -3 admits the trailing options Redis 8 added (FAILOVER); \
         FrogDB's handshake accepts exactly `PSYNC <replid> <offset>` and rejects \
         anything longer, so `Fixed(2)` is the honest arity for what it implements",
)];

#[test]
fn vendored_arity_agrees_with_spec_arity() {
    let registry = full_registry();
    let exemptions: BTreeMap<&str, &str> = ARITY_EXEMPTIONS.iter().copied().collect();
    let mut checked = 0usize;
    let mut used_exemptions = BTreeSet::new();
    let mut divergences: Vec<String> = Vec::new();

    for (name, entry) in registry.iter() {
        let Some(cmd) = upstream::command(name) else {
            continue;
        };
        // Only the core snapshot and the in-tree vector-sets module publish
        // arity; the out-of-tree module repos declare it in code instead.
        let Some(vendored) = cmd.arity else {
            continue;
        };
        let ours = command_info_arity(entry.spec().arity);
        let agrees = i64::from(vendored) == ours;

        match exemptions.get(name) {
            Some(reason) => {
                if agrees {
                    divergences.push(format!(
                        "{name} is exempt from the arity check (\"{reason}\") but now \
                         matches upstream ({vendored}) — remove the exemption"
                    ));
                }
                used_exemptions.insert(name);
            }
            None => {
                if !agrees {
                    divergences.push(format!(
                        "{name}: upstream {vendored}, frogdb {ours} (from {:?})",
                        entry.spec().arity
                    ));
                }
                checked += 1;
            }
        }
    }

    assert!(
        divergences.is_empty(),
        "vendored arity and FrogDB's COMMAND INFO arity disagree for {} command(s). \
         Fix the spec, or add an ARITY_EXEMPTIONS entry with a reason.\n{}",
        divergences.len(),
        divergences.join("\n")
    );
    assert!(
        checked > 100,
        "only {checked} commands were arity checked — the join looks broken"
    );
    for (name, _) in ARITY_EXEMPTIONS {
        assert!(
            used_exemptions.contains(name) || registry.get_entry(name).is_none(),
            "ARITY_EXEMPTIONS lists {name}, which is registered but was never \
             reached by the check — remove the stale entry"
        );
    }
}

// ---------------------------------------------------------------------------
// Command-flag parity
// ---------------------------------------------------------------------------

/// Commands whose `COMMAND INFO` flag set deliberately differs from what
/// upstream declares, each with the reason. Same discipline as the arity and
/// key-spec exemptions: a stale entry fails the test, so the list can only
/// shrink deliberately.
///
/// Flags are compared over the intersection of both vocabularies — a flag
/// FrogDB does not model (`may_replicate`, `no_auth`, `skip_monitor`, ...) is
/// not a divergence, it is silence, and `command_info_flags` is deliberately
/// silent rather than guessing. `movablekeys` is excluded too: Redis derives it
/// from the presence of a `getkeys_proc` in C and never writes it into the
/// JSON this snapshot is vendored from, so upstream's list is not evidence
/// either way.
const FLAG_EXEMPTIONS: &[(&str, &str)] = &[
    (
        "PFDEBUG",
        "upstream declares PFDEBUG `write denyoom` because its `TODENSE` \
         subcommand rewrites a sparse HyperLogLog in place; FrogDB's HLL is \
         always dense, so every PFDEBUG subcommand is a pure read and `TODENSE` \
         is a no-op — advertising `write` would claim a keyspace mutation that \
         cannot happen",
    ),
    (
        "WAITAOF",
        "upstream declares WAITAOF `blocking`; FrogDB's WAITAOF is an unimplemented \
         stub that replies with an error before waiting for anything, so claiming it \
         blocks would misdescribe it (same reason its upstream tips are dropped — see \
         `command_meta::TIP_AUDIT`)",
    ),
    (
        "PING",
        "upstream leaves PING without `stale`; FrogDB carries it deliberately. \
         `replica-serve-stale-data` defaults to `no` here (it defaults to `yes` \
         upstream — see redis-feel issue 17), so a link-down replica refuses every \
         command that is not `stale`-flagged. A health probe that cannot answer on \
         exactly the node an operator is diagnosing is worse than useless, so PING \
         answers and the *data* commands are what get refused",
    ),
    ("ACL", CONTAINER_UNION_VS_SENTINEL_ROW),
    ("CLIENT", CONTAINER_UNION_VS_SENTINEL_ROW),
    ("VEMB", VECTOR_SET_FAST),
    ("VGETATTR", VECTOR_SET_FAST),
    ("VLINKS", VECTOR_SET_FAST),
    ("VSETATTR", VECTOR_SET_FAST),
];

/// Shared reason for the two containers whose own vendored row carries nothing
/// but `SENTINEL`, while every subcommand under it is `noscript stale`.
const CONTAINER_UNION_VS_SENTINEL_ROW: &str = "upstream's own row for this container holds `SENTINEL` alone, which says \
     nothing about either admission gate — Redis keeps the real declaration on \
     the per-subcommand command-table entries this row only groups. FrogDB's \
     container spec is a live fallback rather than a grouping label: it is what \
     gates an invocation whose subcommand no `SubcommandSpec` row declares, so \
     it carries the union its subcommands need. The rows that differ say so \
     themselves now (`SubcommandSpec::admission`, redis-feel issue 20) and are \
     checked one by one against upstream, which is where the real comparison \
     happens";

/// Shared reason for the four vector-set commands that carry `fast` here and
/// not upstream.
const VECTOR_SET_FAST: &str = "the vector-sets module omits `fast` on this command while documenting \
     it as O(1) — upstream is inconsistent with itself here, since VCARD and \
     VDIM are O(1) too and do carry the bit. FrogDB's implementation is O(1), \
     so `fast` describes it truthfully and keeps its `@fast` ACL membership \
     consistent with the complexity we publish";

/// Flags left out of the comparison entirely, with why. Unlike
/// `FLAG_EXEMPTIONS` these are not per-command judgments — the flag itself
/// carries no comparable meaning, so comparing it would produce noise for
/// every command rather than information about any one of them.
const UNCOMPARED_FLAGS: &[(&str, &str)] = &[
    (
        "movablekeys",
        "Redis derives `movablekeys` from the presence of a `getkeys_proc` in C \
         and never writes it into the `src/commands/*.json` this snapshot is \
         vendored from, so upstream's silence is not evidence either way. Our own \
         `movablekeys` is checked instead by replaying the vendored key specs \
         against real key extraction (`vendored_key_specs_agree_with_key_extraction`)",
    ),
    (
        "loading",
        "the one admission gate FrogDB has no *state* for, rather than no code \
         for: boot recovery is synchronous — the listeners bind after it \
         finishes — so no client can reach an instance that is still loading and \
         the refusal upstream's value describes can never be needed. The bit is \
         emitted (vacuously truthful) but there is no behavior on our side for a \
         comparison to be about. Revisited only if FrogDB ever serves while \
         loading; see redis-feel issue 17",
    ),
];

/// The flag vocabulary both sides can express, lowercase.
fn comparable_flags() -> BTreeSet<&'static str> {
    frogdb_commands::command_meta::WIRE_FLAGS
        .iter()
        .map(|(_, name)| *name)
        .filter(|name| !UNCOMPARED_FLAGS.iter().any(|(flag, _)| flag == name))
        .collect()
}

#[test]
fn vendored_command_flags_agree_with_command_info_flags() {
    let registry = full_registry();
    let comparable = comparable_flags();
    let exemptions: BTreeMap<&str, &str> = FLAG_EXEMPTIONS.iter().copied().collect();
    let mut checked = 0usize;
    let mut used_exemptions = BTreeSet::new();
    let mut divergences: Vec<String> = Vec::new();

    for (name, entry) in registry.iter() {
        let Some(cmd) = upstream::command(name) else {
            continue;
        };
        // Container commands (`OBJECT`, `XINFO`, `SLOWLOG`, ...) carry no
        // `command_flags` of their own upstream: Redis puts every flag on the
        // subcommand and the container advertises an empty array. FrogDB has no
        // per-subcommand registry, so the container's own spec is the only
        // place its flags can live and it advertises the union its dispatch
        // actually enforces. There is no upstream value to compare against, so
        // these rows are skipped rather than exempted one by one; the whole
        // class is recorded as deliberate in
        // `.scratch/redis-feel/issues/done/12-command-metadata-deep-fidelity.md`.
        let Some(vendored) = cmd.command_flags else {
            continue;
        };
        let theirs: BTreeSet<String> = vendored
            .iter()
            .map(|flag| flag.to_lowercase())
            .filter(|flag| comparable.contains(flag.as_str()))
            .collect();
        let ours: BTreeSet<String> =
            frogdb_commands::command_meta::command_info_flags(entry.spec())
                .iter()
                .map(|flag| match flag {
                    frogdb_protocol::Response::Simple(bytes) => {
                        String::from_utf8_lossy(bytes).into_owned()
                    }
                    other => panic!("{name}: expected a Simple flag, got {other:?}"),
                })
                .filter(|flag| comparable.contains(flag.as_str()))
                .collect();
        let agrees = theirs == ours;

        match exemptions.get(name) {
            Some(reason) => {
                if agrees {
                    divergences.push(format!(
                        "{name} is exempt from the flag check (\"{reason}\") but now \
                         matches upstream ({theirs:?}) — remove the exemption"
                    ));
                }
                used_exemptions.insert(name);
            }
            None => {
                if !agrees {
                    let missing: Vec<&String> = theirs.difference(&ours).collect();
                    let extra: Vec<&String> = ours.difference(&theirs).collect();
                    divergences.push(format!(
                        "{name}: upstream-only {missing:?}, frogdb-only {extra:?}"
                    ));
                }
                checked += 1;
            }
        }
    }

    assert!(
        divergences.is_empty(),
        "vendored command flags and FrogDB's COMMAND INFO flags disagree for {} \
         command(s). Fix the spec, or add a FLAG_EXEMPTIONS entry with a reason.\n{}",
        divergences.len(),
        divergences.join("\n")
    );
    assert!(
        checked > 100,
        "only {checked} commands were flag checked — the join looks broken"
    );
    for (name, _) in FLAG_EXEMPTIONS {
        assert!(
            used_exemptions.contains(name) || registry.get_entry(name).is_none(),
            "FLAG_EXEMPTIONS lists {name}, which is registered but was never reached \
             by the check — remove the stale entry"
        );
    }
}

// ---------------------------------------------------------------------------
// Tips
// ---------------------------------------------------------------------------

/// Every command upstream tags with tips needs a
/// [`frogdb_commands::command_meta::TIP_AUDIT`] ruling before `COMMAND INFO`
/// says anything about it, and every ruling has to name a command that is
/// still tipped upstream. Vendoring a newer Redis that tips a new command
/// fails here rather than silently emitting an unaudited routing promise.
#[test]
fn tip_audit_covers_every_tipped_command() {
    use frogdb_commands::command_meta::{TIP_AUDIT, tip_ruling};

    let registry = full_registry();
    let mut unaudited: Vec<&str> = Vec::new();

    for (name, _) in registry.iter() {
        let Some(cmd) = upstream::command(name) else {
            continue;
        };
        if !cmd.command_tips.is_empty() && tip_ruling(name).is_none() {
            unaudited.push(name);
        }
    }

    assert!(
        unaudited.is_empty(),
        "upstream declares tips for {unaudited:?}, which have no TIP_AUDIT ruling. \
         Judge each against FrogDB's real routing/determinism and add a row — \
         repeat upstream's list verbatim, or emit a subset with the reason."
    );

    for row in TIP_AUDIT {
        if registry.get_entry(row.command).is_none() {
            continue;
        }
        let tipped = upstream::command(row.command).is_some_and(|cmd| !cmd.command_tips.is_empty());
        assert!(
            tipped,
            "TIP_AUDIT rules on {}, which upstream no longer tips — remove the row",
            row.command
        );
    }
}

/// The emitter's key-spec bypass list and this file's key-spec exemption list
/// have to name the same commands: a divergence the truthfulness gate tolerates
/// is exactly a divergence `COMMAND INFO` must not repeat as if it were ours.
#[test]
fn key_spec_divergences_match_the_emitter_bypass() {
    let exempt: BTreeSet<&str> = KEY_SPEC_EXEMPTIONS.iter().map(|(name, _)| *name).collect();
    let bypassed: BTreeSet<&str> = frogdb_commands::command_meta::KEY_SPEC_DIVERGENCES
        .iter()
        .copied()
        .collect();
    assert_eq!(
        exempt, bypassed,
        "KEY_SPEC_EXEMPTIONS (this file) and command_meta::KEY_SPEC_DIVERGENCES \
         (the emitter) disagree; they describe the same set of commands whose \
         vendored key specs do not describe FrogDB"
    );
}

// ---------------------------------------------------------------------------
// Container subcommands
// ---------------------------------------------------------------------------
//
// A container's real surface — arity, flags, key positions — is per subcommand
// on both sides: upstream keeps it on the subcommand rows vendored alongside
// the container, FrogDB keeps it in `CONTAINER_SUBCOMMANDS`. The three gates
// above are repeated here one resolved subcommand at a time, against the same
// vendored data and the same real dispatch.

/// Subcommands FrogDB declares on an otherwise-upstream container that upstream
/// has no row for, spelled `CONTAINER|SUB` in full — no prefixes, no wildcards,
/// same discipline as `frogdb_only_commands()`. A stale entry (upstream grew
/// the subcommand, or FrogDB dropped it) fails.
const SUBCOMMAND_EXTENSIONS: &[(&str, &str)] = &[
    (
        "CLIENT|STATS",
        "FrogDB extension: per-connection command, byte and latency counters, \
         which Redis exposes only in aggregate through INFO commandstats",
    ),
    (
        "LATENCY|BANDS",
        "FrogDB extension: inspects and reconfigures the latency-histogram bucket \
         boundaries, which Redis fixes at compile time",
    ),
    (
        "MEMORY|MALLOC-SIZE",
        "FrogDB extension: reports the allocator's size class for one value. \
         Redis 8.6.1 has MEMORY MALLOC-STATS, which is a different question \
         (allocator-wide statistics) and is not implemented here",
    ),
];

/// Subcommands whose emitted flags deliberately differ from upstream's, each
/// with the reason. Same shrink-only discipline as `FLAG_EXEMPTIONS`.
///
/// Every entry here is a *container-level* FrogDB decision that upstream makes
/// per subcommand. They became visible only with this join: a container's own
/// vendored row carries no `command_flags` at all, so the whole-command flag
/// gate skipped all three containers.
const SUBCOMMAND_FLAG_EXEMPTIONS: &[(&str, &str)] = &[
    (
        "SCRIPT|LOAD",
        "upstream gives SCRIPT LOAD `stale` and no other non-HELP SCRIPT \
         subcommand. FrogDB *can* now say that — `SubcommandSpec::admission` \
         (redis-feel issue 20) lets a row declare its own gates — and \
         deliberately does not: loading a script onto a replica that cannot see \
         its primary is not a read the client needs served, and the script it \
         loads would be evaluated against a keyspace of unbounded age. The \
         divergence is a choice now rather than a modelling limit, so it stays \
         here as one",
    ),
    ("HOTKEYS|HELP", WHOLE_ADMIN_HELP),
    ("LATENCY|HELP", WHOLE_ADMIN_HELP),
    (
        "PUBSUB|HELP",
        "FrogDB gates the whole PUBSUB container as `pubsub` (callable while the \
         connection is in subscriber mode); upstream clears the bit on HELP alone. \
         Ours describes what FrogDB accepts — `PUBSUB HELP` really is answered \
         inside a subscription — and static help text discloses nothing",
    ),
    ("SLOWLOG|GET", SLOWLOG_CONTAINER_FLAGS),
    (
        "SLOWLOG|HELP",
        "both of the container-level decisions at once — the whole-command admin \
         gate (see the HOTKEYS|HELP and LATENCY|HELP entries) and the container's \
         `fast`/`skip_slowlog` marks (see the SLOWLOG|GET entry)",
    ),
    ("SLOWLOG|LEN", SLOWLOG_CONTAINER_FLAGS),
    ("SLOWLOG|RESET", SLOWLOG_CONTAINER_FLAGS),
];

/// Shared reason for the three containers FrogDB gates wholly admin, where
/// upstream leaves `HELP` open.
const WHOLE_ADMIN_HELP: &str = "FrogDB gates this container with the whole-command admin flag rather than a \
     `SPLIT_ADMIN_SURFACES` entry, so HELP is admin-only here and open upstream. \
     The flag is truthful — a plain client port really does refuse it — and \
     opening HELP means moving the container into the split table, which is an \
     admin-gate change rather than a metadata one. `admin` is the only bit left \
     diverging: the admission gates now come from the row (redis-feel issue 20), \
     so this HELP is `stale`/`loading` and not `noscript`, exactly as upstream \
     declares it";

/// Shared reason for the SLOWLOG container's `fast`/`skip_slowlog` marks, which
/// upstream sets on no `SLOWLOG` subcommand.
const SLOWLOG_CONTAINER_FLAGS: &str = "FrogDB marks the whole SLOWLOG container `fast` and `skip_slowlog`; upstream \
     sets neither on any SLOWLOG subcommand. `skip_slowlog` is true of FrogDB \
     (slowlog administration is never itself logged), but `fast` overclaims — \
     SLOWLOG GET is O(N) in the returned count — and the bit also decides `@fast` \
     ACL membership, so narrowing it is an ACL-visible change rather than a \
     metadata one";

/// Subcommands whose vendored key specs deliberately do not describe FrogDB's
/// key extraction, each with the reason. Same discipline as
/// `KEY_SPEC_EXEMPTIONS`.
const SUBCOMMAND_KEY_SPEC_EXEMPTIONS: &[(&str, &str)] = &[(
    "MEMORY|USAGE",
    "upstream declares the key at index 2; FrogDB's row declares no key, which is \
     what it has always done. Declaring it would newly subject MEMORY USAGE to ACL \
     key permissions and cluster slot redirection — both matching Redis, and both \
     behavior changes rather than metadata ones",
)];

/// Every declared subcommand row of every registered container, paired with the
/// vendored row it joins (`None` for a FrogDB extension), keyed `CONTAINER|SUB`.
fn subcommand_rows(
    registry: &CommandRegistry,
) -> Vec<(
    String,
    &'static str,
    &'static frogdb_core::SubcommandSpec,
    Option<&'static UpstreamCommand>,
)> {
    let mut rows = Vec::new();
    for container in frogdb_core::subcommand_container_names() {
        if registry.get_entry(container).is_none() {
            continue; // family not compiled into this build
        }
        for row in frogdb_core::container_subcommands(container).unwrap_or_default() {
            rows.push((
                format!("{container}|{}", row.name),
                container,
                row,
                upstream::subcommand(container, row.name),
            ));
        }
    }
    rows
}

/// The nested `COMMAND INFO` entries a container reports at index 9, as
/// `container|sub` name -> (arity, flags). Read out of the real emitter rather
/// than recomputed, so what these gates check is what clients are told.
fn emitted_subcommand_info(
    registry: &CommandRegistry,
    container: &str,
) -> BTreeMap<String, (i64, BTreeSet<String>)> {
    let entry = registry
        .get_entry(container)
        .expect("caller checked registration");
    let frogdb_protocol::Response::Array(info) =
        frogdb_commands::command_meta::build_command_info(entry.spec())
    else {
        panic!("{container}: COMMAND INFO entry is not an array");
    };
    let frogdb_protocol::Response::Array(subs) = &info[9] else {
        panic!("{container}: COMMAND INFO slot 9 is not an array");
    };
    subs.iter()
        .map(|sub| {
            let frogdb_protocol::Response::Array(fields) = sub else {
                panic!("{container}: nested subcommand entry is not an array");
            };
            let frogdb_protocol::Response::Bulk(Some(name)) = &fields[0] else {
                panic!("{container}: nested subcommand name is not a bulk string");
            };
            let frogdb_protocol::Response::Integer(arity) = fields[1] else {
                panic!("{container}: nested subcommand arity is not an integer");
            };
            let frogdb_protocol::Response::Array(flags) = &fields[2] else {
                panic!("{container}: nested subcommand flags are not an array");
            };
            let flags = flags
                .iter()
                .map(|flag| match flag {
                    frogdb_protocol::Response::Simple(bytes) => {
                        String::from_utf8_lossy(bytes).into_owned()
                    }
                    other => panic!("{container}: expected a Simple flag, got {other:?}"),
                })
                .collect();
            (String::from_utf8_lossy(name).to_uppercase(), (arity, flags))
        })
        .collect()
}

#[test]
fn every_declared_subcommand_joins_vendored_metadata() {
    let registry = full_registry();
    let extensions: BTreeMap<&str, &str> = SUBCOMMAND_EXTENSIONS.iter().copied().collect();
    let mut declared = BTreeSet::new();

    let missing: Vec<String> = subcommand_rows(&registry)
        .into_iter()
        .inspect(|(key, ..)| {
            declared.insert(key.clone());
        })
        .filter(|(key, _, _, vendored)| {
            vendored.is_none() && !extensions.contains_key(key.as_str())
        })
        .map(|(key, ..)| key)
        .collect();

    assert!(
        missing.is_empty(),
        "declared subcommands with no vendored upstream row: {missing:?}. Either \
         the vendored snapshots are stale (`just redis-commands-vendor` then \
         `just command-metadata-gen`), or these are FrogDB extensions that belong \
         in SUBCOMMAND_EXTENSIONS."
    );

    for (key, reason) in SUBCOMMAND_EXTENSIONS {
        let container = key.split('|').next().expect("key is CONTAINER|SUB");
        if registry.get_entry(container).is_none() {
            continue; // family not compiled into this build
        }
        assert!(
            declared.contains(*key),
            "SUBCOMMAND_EXTENSIONS lists {key} (\"{reason}\"), which this build \
             declares no row for — remove the stale entry"
        );
    }
}

#[test]
fn vendored_subcommand_arity_agrees_with_emitted_arity() {
    let registry = full_registry();
    let mut emitted: BTreeMap<&str, BTreeMap<String, (i64, BTreeSet<String>)>> = BTreeMap::new();
    let mut checked = 0usize;
    let mut divergences: Vec<String> = Vec::new();

    for (key, container, _, vendored) in subcommand_rows(&registry) {
        let Some(vendored) = vendored else {
            continue; // FrogDB extension: nothing to compare against
        };
        let Some(theirs) = vendored.arity else {
            continue;
        };
        let info = emitted
            .entry(container)
            .or_insert_with(|| emitted_subcommand_info(&registry, container));
        let (ours, _) = info
            .get(&key)
            .unwrap_or_else(|| panic!("{key} is declared but COMMAND INFO does not nest it"));
        if i64::from(theirs) != *ours {
            divergences.push(format!("{key}: upstream {theirs}, frogdb {ours}"));
        }
        checked += 1;
    }

    assert!(
        divergences.is_empty(),
        "vendored subcommand arity and FrogDB's nested COMMAND INFO arity disagree \
         for {} subcommand(s). Fix the SubcommandSpec row.\n{}",
        divergences.len(),
        divergences.join("\n")
    );
    assert!(
        checked > 50,
        "only {checked} subcommands were arity checked — the join looks broken"
    );
}

#[test]
fn vendored_subcommand_flags_agree_with_emitted_flags() {
    let registry = full_registry();
    let comparable = comparable_flags();
    let exemptions: BTreeMap<&str, &str> = SUBCOMMAND_FLAG_EXEMPTIONS.iter().copied().collect();
    let mut emitted: BTreeMap<&str, BTreeMap<String, (i64, BTreeSet<String>)>> = BTreeMap::new();
    let mut used_exemptions = BTreeSet::new();
    let mut checked = 0usize;
    let mut divergences: Vec<String> = Vec::new();

    for (key, container, _, vendored) in subcommand_rows(&registry) {
        let Some(vendored) = vendored else {
            continue; // FrogDB extension: nothing to compare against
        };
        let Some(their_flags) = vendored.command_flags else {
            continue;
        };
        let theirs: BTreeSet<String> = their_flags
            .iter()
            .map(|flag| flag.to_lowercase())
            .filter(|flag| comparable.contains(flag.as_str()))
            .collect();
        let info = emitted
            .entry(container)
            .or_insert_with(|| emitted_subcommand_info(&registry, container));
        let (_, our_flags) = info
            .get(&key)
            .unwrap_or_else(|| panic!("{key} is declared but COMMAND INFO does not nest it"));
        let ours: BTreeSet<String> = our_flags
            .iter()
            .filter(|flag| comparable.contains(flag.as_str()))
            .cloned()
            .collect();
        let agrees = theirs == ours;

        match exemptions.get(key.as_str()) {
            Some(reason) => {
                if agrees {
                    divergences.push(format!(
                        "{key} is exempt from the subcommand flag check (\"{reason}\") \
                         but now matches upstream ({theirs:?}) — remove the exemption"
                    ));
                }
                used_exemptions.insert(key.clone());
            }
            None => {
                if !agrees {
                    let missing: Vec<&String> = theirs.difference(&ours).collect();
                    let extra: Vec<&String> = ours.difference(&theirs).collect();
                    divergences.push(format!(
                        "{key}: upstream-only {missing:?}, frogdb-only {extra:?}"
                    ));
                }
                checked += 1;
            }
        }
    }

    assert!(
        divergences.is_empty(),
        "vendored subcommand flags and FrogDB's nested COMMAND INFO flags disagree \
         for {} subcommand(s). Fix the SubcommandSpec row, or add a \
         SUBCOMMAND_FLAG_EXEMPTIONS entry with a reason.\n{}",
        divergences.len(),
        divergences.join("\n")
    );
    assert!(
        checked > 50,
        "only {checked} subcommands were flag checked — the join looks broken"
    );
    for (key, _) in SUBCOMMAND_FLAG_EXEMPTIONS {
        let container = key.split('|').next().expect("key is CONTAINER|SUB");
        assert!(
            used_exemptions.contains(*key) || registry.get_entry(container).is_none(),
            "SUBCOMMAND_FLAG_EXEMPTIONS lists {key}, which is registered but was \
             never reached by the check — remove the stale entry"
        );
    }
}

/// The synthetic argv for one subcommand invocation: upstream's subcommand rows
/// index the *container's* argv (`XGROUP CREATE key group id` puts the key at
/// index 2), so the argv is built from the subcommand row's own arity and then
/// has the container and subcommand tokens written into slots 0 and 1.
fn subcommand_argv(container: &str, vendored: &UpstreamCommand) -> Option<Vec<Bytes>> {
    let mut argv = synthetic_argv(vendored);
    if argv.len() < 2 {
        // `CONTAINER SUB` is the shortest invocation that exists; anything
        // shorter cannot carry a key for either side to disagree about.
        return None;
    }
    argv[0] = Bytes::from(container.to_string());
    argv[1] = Bytes::from(vendored.name.to_string());
    Some(argv)
}

#[test]
fn vendored_subcommand_key_specs_agree_with_key_extraction() {
    let registry = full_registry();
    let exemptions: BTreeMap<&str, &str> = SUBCOMMAND_KEY_SPEC_EXEMPTIONS.iter().copied().collect();
    let mut used_exemptions = BTreeSet::new();
    let mut checked = 0usize;
    let mut divergences: Vec<String> = Vec::new();

    for (key, container, row, vendored) in subcommand_rows(&registry) {
        let Some(vendored) = vendored else {
            continue; // FrogDB extension: nothing to compare against
        };
        if vendored.key_specs.is_none() {
            continue;
        }
        let Some(argv) = subcommand_argv(container, vendored) else {
            continue;
        };
        if vendored
            .key_specs
            .unwrap_or(&[])
            .iter()
            .any(|spec| matches!(spec.begin_search, BeginSearch::Keyword { .. }))
        {
            // No container subcommand has a keyword-based key spec today, and a
            // placeholder argv cannot place the keyword where the grammar wants
            // it — fail loudly rather than compare against a shape that cannot
            // occur.
            panic!(
                "{key} has a keyword-based key spec; teach this test how to build \
                 an argv for it (see KEYWORD_ARGV)"
            );
        }

        let (expected, incomplete) =
            upstream_key_indices(vendored, &argv).expect("synthetic argv is valid");
        let actual = frogdb_key_indices(&registry, container, &argv);
        let agrees = if incomplete {
            actual.is_superset(&expected)
        } else {
            actual == expected
        };

        match exemptions.get(key.as_str()) {
            Some(reason) => {
                if agrees {
                    divergences.push(format!(
                        "{key} is exempt from the subcommand key-spec check \
                         (\"{reason}\") but now agrees with upstream — remove the \
                         exemption"
                    ));
                }
                used_exemptions.insert(key.clone());
            }
            None => {
                if !agrees {
                    divergences.push(format!(
                        "{key}: upstream {expected:?}{}, frogdb {actual:?} (row {:?}) \
                         for argv {:?}",
                        if incomplete { " (lower bound)" } else { "" },
                        row.keys,
                        argv.iter()
                            .map(|a| String::from_utf8_lossy(a).into_owned())
                            .collect::<Vec<_>>(),
                    ));
                }
                checked += 1;
            }
        }
    }

    assert!(
        divergences.is_empty(),
        "vendored subcommand key specs and FrogDB key extraction disagree for {} \
         subcommand(s). Fix the SubcommandSpec row, or add a \
         SUBCOMMAND_KEY_SPEC_EXEMPTIONS entry with a reason.\n{}",
        divergences.len(),
        divergences.join("\n")
    );
    assert!(
        checked > 50,
        "only {checked} subcommands were key-spec checked — the join looks broken"
    );
    for (key, _) in SUBCOMMAND_KEY_SPEC_EXEMPTIONS {
        let container = key.split('|').next().expect("key is CONTAINER|SUB");
        assert!(
            used_exemptions.contains(*key) || registry.get_entry(container).is_none(),
            "SUBCOMMAND_KEY_SPEC_EXEMPTIONS lists {key}, which is registered but was \
             never reached by the check — remove the stale entry"
        );
    }
}

// ---------------------------------------------------------------------------
// ACL categories
// ---------------------------------------------------------------------------

/// Commands whose ACL category set deliberately differs from the one derived
/// from the vendored rows, each with the reason. Same shrink-only discipline as
/// `FLAG_EXEMPTIONS`: a stale entry fails just as loudly as a new divergence.
///
/// Empty today. It exists because the *next* upstream bump may introduce a
/// category FrogDB cannot honestly claim, and the honest answer then is a named
/// entry rather than a weakened comparison.
const ACL_CATEGORY_DIVERGENCES: &[(&str, &str)] = &[];

/// Categories Redis derives at registration time rather than writing into
/// `src/commands/*.json` (`setImplicitACLCategories` in `server.c`).
///
/// `flags` are **FrogDB's** wire flags, not upstream's: per ADR-0005 the table
/// has to describe what FrogDB's ACL engine gates, so a command we implement
/// differently (`PFDEBUG` is a pure read here; `WAITAOF` never blocks) earns
/// the categories its own behavior implies. The explicit half is upstream's
/// verbatim, and is passed in because Redis's `@read` rule consults it —
/// a `readonly` command already marked `@scripting` (`EVAL_RO`) does not also
/// become `@read`.
fn implied_acl_categories(
    flags: &BTreeSet<String>,
    explicit: &BTreeSet<&'static str>,
) -> BTreeSet<&'static str> {
    let mut out = BTreeSet::new();
    if flags.contains("write") {
        out.insert("write");
    }
    if flags.contains("readonly") && !explicit.contains("scripting") {
        out.insert("read");
    }
    if flags.contains("admin") {
        out.insert("admin");
        out.insert("dangerous");
    }
    if flags.contains("pubsub") {
        out.insert("pubsub");
    }
    if flags.contains("fast") {
        out.insert("fast");
    }
    if flags.contains("blocking") {
        out.insert("blocking");
    }
    if !out.contains("fast") && !explicit.contains("fast") {
        out.insert("slow");
    }
    out
}

/// Upstream's explicit categories for one row, lowercased and interned against
/// the category vocabulary FrogDB's ACL engine has. A category upstream
/// declares that FrogDB's `CommandCategory` enum cannot express would be
/// silently dropped, so it panics instead.
fn explicit_acl_categories(cmd: &UpstreamCommand) -> BTreeSet<&'static str> {
    cmd.acl_categories
        .iter()
        .map(|raw| {
            let lower = raw.to_lowercase();
            frogdb_core::CommandCategory::parse(&lower)
                .unwrap_or_else(|| {
                    panic!(
                        "{}: upstream declares ACL category {raw:?}, which \
                         `frogdb_acl::CommandCategory` cannot express — add the \
                         variant before re-vendoring",
                        cmd.name
                    )
                })
                .name()
        })
        .collect()
}

/// The wire flags `COMMAND INFO` reports for a registered command, lowercase.
fn our_wire_flags(spec: &frogdb_core::CommandSpec) -> BTreeSet<String> {
    frogdb_commands::command_meta::command_info_flags(spec)
        .iter()
        .map(|flag| match flag {
            frogdb_protocol::Response::Simple(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => panic!("expected a Simple flag, got {other:?}"),
        })
        .collect()
}

/// The category set the vendored data says a command should have.
///
/// Leaf commands are upstream's explicit list plus the half Redis derives, and
/// the derivation runs off FrogDB's own flags (see [`implied_acl_categories`]).
///
/// Container commands are the union over the subcommands upstream documents:
/// FrogDB registers one `CommandSpec` per container and its ACL engine gates
/// the container as a whole, so the container's row has to cover everything it
/// dispatches. Their implied half comes from *upstream's* subcommand flags,
/// because FrogDB has no per-subcommand flag declaration outside the behavioral
/// subset (`frogdb_core::BEHAVIORAL_FLAGS`) — `admin` in particular is declared
/// once per container. Where FrogDB splits the container's admin-port surface
/// (`split_admin_surface_commands`), `@admin`/`@dangerous` are added so
/// `-@admin` covers the half FrogDB itself refuses on a plain client port.
///
/// Known cost, recorded rather than papered over: container granularity means
/// `-@admin` denies `CLIENT SETNAME` too, which Redis — gating per subcommand —
/// allows. Closing that needs per-subcommand ACL enforcement
/// (`AclPermissions::is_command_allowed` consults only the container name), not
/// a different table.
fn expected_acl_categories(
    cmd: &UpstreamCommand,
    spec: &frogdb_core::CommandSpec,
) -> BTreeSet<&'static str> {
    if cmd.has_subcommands() {
        let mut out = BTreeSet::new();
        for sub in cmd.subcommands {
            let explicit = explicit_acl_categories(sub);
            let flags: BTreeSet<String> = sub
                .command_flags
                .unwrap_or(&[])
                .iter()
                .map(|flag| flag.to_lowercase())
                .collect();
            out.extend(implied_acl_categories(&flags, &explicit));
            out.extend(explicit);
        }
        if frogdb_core::split_admin_surface_commands().any(|name| name == cmd.name) {
            out.insert("admin");
            out.insert("dangerous");
        }
        return out;
    }
    let explicit = explicit_acl_categories(cmd);
    let mut out = implied_acl_categories(&our_wire_flags(spec), &explicit);
    out.extend(explicit);
    out
}

/// `COMMAND INFO`'s category array for a command, `@` stripped — the reply a
/// client actually sees, which `command_meta::command_info_categories` builds
/// from the same `frogdb_acl` table `ACL SETUSER +@category` enforces from.
fn emitted_acl_categories(name: &str) -> BTreeSet<String> {
    frogdb_commands::command_meta::command_info_categories(name)
        .iter()
        .map(|cat| match cat {
            frogdb_protocol::Response::Simple(bytes) => {
                let text = String::from_utf8_lossy(bytes).into_owned();
                text.strip_prefix('@')
                    .map(str::to_owned)
                    .unwrap_or_else(|| panic!("{name}: category {text:?} is not @-prefixed"))
            }
            other => panic!("{name}: expected a Simple category, got {other:?}"),
        })
        .collect()
}

/// The ACL category table agrees with the vendored rows for every core Redis
/// command FrogDB registers.
///
/// This is the gate that keeps `+@read` / `-@dangerous` honest: the table and
/// the registry are joined only by a lowercase string, so a command with a
/// missing or wrong row is simultaneously a wrong `COMMAND INFO` reply and an
/// ACL rule that silently fails to cover it (`.scratch/redis-feel/issues/done/
/// 16-acl-category-table-gaps.md`).
///
/// Module-family commands are out of scope by construction: no module
/// `commands.json` declares `acl_categories` — modules set theirs in C at
/// `RedisModule_SetCommandACLCategories` time — so the vendored rows carry no
/// evidence to check against. Their gap stays pinned by
/// `register::tests::every_registered_command_has_acl_category_or_is_allowlisted`.
#[test]
fn vendored_acl_categories_agree_with_our_table() {
    let registry = full_registry();
    let divergences_allowed: BTreeMap<&str, &str> =
        ACL_CATEGORY_DIVERGENCES.iter().copied().collect();
    let mut used_exemptions = BTreeSet::new();
    let mut checked = 0usize;
    let mut divergences: Vec<String> = Vec::new();

    for (name, entry) in registry.iter() {
        let Some(cmd) = upstream::redis_command(name) else {
            continue;
        };
        let expected = expected_acl_categories(cmd, entry.spec());
        let ours = emitted_acl_categories(name);
        let agrees = ours.iter().map(String::as_str).eq(expected.iter().copied());

        match divergences_allowed.get(name) {
            Some(reason) => {
                if agrees {
                    divergences.push(format!(
                        "{name} is exempt from the ACL category check (\"{reason}\") \
                         but now matches upstream ({expected:?}) — remove the exemption"
                    ));
                }
                used_exemptions.insert(name);
            }
            None => {
                if !agrees {
                    let missing: Vec<&str> = expected
                        .iter()
                        .copied()
                        .filter(|cat| !ours.contains(*cat))
                        .collect();
                    let extra: Vec<&str> = ours
                        .iter()
                        .map(String::as_str)
                        .filter(|cat| !expected.contains(cat))
                        .collect();
                    divergences.push(format!(
                        "{name}: upstream-only {missing:?}, frogdb-only {extra:?}"
                    ));
                }
                checked += 1;
            }
        }
    }

    assert!(
        divergences.is_empty(),
        "vendored ACL categories and FrogDB's category table disagree for {} \
         command(s). Fix the `frogdb_acl` ALL_CATEGORIES row, or add an \
         ACL_CATEGORY_DIVERGENCES entry with a reason.\n{}",
        divergences.len(),
        divergences.join("\n")
    );
    assert!(
        checked > 100,
        "only {checked} commands were ACL-category checked — the join looks broken"
    );
    for (name, _) in ACL_CATEGORY_DIVERGENCES {
        assert!(
            used_exemptions.contains(name) || registry.get_entry(name).is_none(),
            "ACL_CATEGORY_DIVERGENCES lists {name}, which is registered but was never \
             reached by the check — remove the stale entry"
        );
    }
}

/// Every registered core-Redis command carries exactly one of `@fast` / `@slow`
/// in the reply, and it is the one its own `fast` flag earns. Redis assigns
/// `@slow` to whatever is not `@fast`, so "both" and "neither" are states no
/// command can legitimately be in — and the `@fast`/`@slow` half of the table
/// was the sub-shape that drifted furthest (issue 16 found eight commands whose
/// row contradicted their own `CommandFlags::FAST`).
#[test]
fn fast_and_slow_categories_follow_the_fast_flag() {
    let registry = full_registry();

    for (name, entry) in registry.iter() {
        let Some(cmd) = upstream::redis_command(name) else {
            continue;
        };
        let cats = emitted_acl_categories(name);
        let fast = cats.contains("fast");
        let slow = cats.contains("slow");
        assert!(
            fast ^ slow,
            "{name} reports @fast={fast} @slow={slow} — exactly one must hold"
        );
        // Containers are excluded from the second half only: their row is the
        // union over subcommands, so it answers "is any of this fast", while
        // the container's own spec flag answers for the dispatch as a whole.
        if cmd.has_subcommands() {
            continue;
        }
        assert_eq!(
            fast,
            our_wire_flags(entry.spec()).contains("fast"),
            "{name}: @fast must agree with the command's own `fast` flag"
        );
    }
}
