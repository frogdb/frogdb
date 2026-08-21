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
//! Per ADR-0005 the vendored data never becomes the answer FrogDB gives; it is
//! only the thing our real behavior is checked against. Every divergence is
//! either fixed or written down as a named exemption with a reason, and a
//! stale exemption fails the test just as loudly as a new divergence.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use frogdb_commands::basic::command_info_arity;
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
const KEY_SPEC_EXEMPTIONS: &[(&str, &str)] = &[
    (
        "MOVE",
        "MOVE is a deliberate stub (`commands::stub`, `is_stub()`), and stubs are \
         registered keyless because they reject before touching the keyspace",
    ),
    (
        "SUNSUBSCRIBE",
        "FrogDB registers SUNSUBSCRIBE with `KeySpec::None` while SSUBSCRIBE/SPUBLISH \
         route their shard channels through key extraction, so no slot check runs on \
         unsubscribe — a real gap, tracked for the shard-pubsub follow-up",
    ),
];

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
        if cmd.has_subcommands {
            // Upstream declares this container's real key specs on subcommand
            // rows the vendor step skips, so its empty spec list means
            // "not vendored here", not "takes no keys".
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
const ARITY_EXEMPTIONS: &[(&str, &str)] = &[
    (
        "PSYNC",
        "upstream's -3 admits the trailing options Redis 8 added (FAILOVER); \
         FrogDB's handshake accepts exactly `PSYNC <replid> <offset>` and rejects \
         anything longer, so `Fixed(2)` is the honest arity for what it implements",
    ),
    (
        "XGROUP",
        "upstream's -2 admits `XGROUP HELP` because upstream keeps its key specs on \
         the subcommand rows; FrogDB models the container as one command with a key \
         at index 1, and `CommandSpec::validate` requires the arity minimum to cover \
         that index, so bare `XGROUP HELP` is rejected",
    ),
    (
        "XINFO",
        "same container shape as XGROUP: FrogDB's key-at-index-1 model forces \
         `AtLeast(2)` where upstream's subcommand-level key specs let the container \
         itself be -2",
    ),
];

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
