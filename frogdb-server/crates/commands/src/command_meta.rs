//! `COMMAND INFO` / `COMMAND DOCS` reply construction.
//!
//! Two sources feed these replies, and which one answers a given field is a
//! deliberate choice per ADR-0005:
//!
//! - **Our own [`CommandSpec`]** answers everything that is a claim about what
//!   FrogDB does: arity, command flags, ACL categories, summary/since/group/
//!   complexity, and the key positions for commands upstream does not describe.
//! - **The vendored upstream snapshot** ([`crate::upstream`]) answers the
//!   fields that are documentation of the *command surface* rather than of this
//!   implementation — the structured key specs, the `COMMAND DOCS` argument
//!   trees, version history and deprecation notes. Those are only repeated for
//!   commands whose real behavior was checked against them: the key-spec join
//!   test in `frogdb-server/crates/server/src/server/upstream_metadata_tests.rs`
//!   replays every vendored key spec against FrogDB's actual key extraction,
//!   and [`KEY_SPEC_DIVERGENCES`] names the handful where the two disagree.
//!
//! Tips are the one field that is neither: they are claims about *routing* and
//! *reply determinism* that a proxy acts on, so upstream's list is an input to
//! a judgment, never the answer. [`TIP_AUDIT`] records the judgment per command.

use bytes::Bytes;
use frogdb_core::{AccessSpec, Arity, CommandFlags, CommandSpec, KeyAccessFlag};
use frogdb_protocol::{Response, SafeStatus};

use crate::upstream::{self, BeginSearch, FindKeys, UpstreamArg, UpstreamKeySpec};

fn field(name: &'static str) -> Response {
    Response::bulk(Bytes::from_static(name.as_bytes()))
}

fn text(value: &'static str) -> Response {
    Response::bulk(Bytes::from_static(value.as_bytes()))
}

fn status(value: &'static str) -> Response {
    Response::Simple(SafeStatus::sanitized(value))
}

// ---------------------------------------------------------------------------
// COMMAND INFO
// ---------------------------------------------------------------------------

/// Build the full 10-element `COMMAND INFO` reply for one command: name,
/// arity, flags, first-key, last-key, key-step, ACL categories, tips,
/// key-specs, subcommands. Matches the Redis 8.x reply shape.
pub fn build_command_info(spec: &CommandSpec) -> Response {
    let (first_key, last_key, key_step) = legacy_range(spec);

    Response::Array(vec![
        Response::bulk(Bytes::from(spec.name.to_lowercase())),
        Response::Integer(command_info_arity(spec.arity)),
        Response::Array(command_info_flags(spec)),
        Response::Integer(first_key),
        Response::Integer(last_key),
        Response::Integer(key_step),
        Response::Array(command_info_categories(spec.name)),
        Response::Array(command_info_tips(spec.name)),
        Response::Array(command_info_key_specs(spec)),
        // Subcommands: no structured per-subcommand registry exists yet, so an
        // empty array is what we can say truthfully.
        Response::Array(vec![]),
    ])
}

/// The legacy `first_key`/`last_key`/`key_step` triplet `COMMAND INFO` reports
/// at indices 3-5, kept in agreement with the key-specs array at index 8.
///
/// Our `KeySpec` answers directly whenever it describes a fixed range. When it
/// does not — the movable-key commands, where `command_info_triplet` yields
/// `(0, 0, 0)` — Redis still reports a range, because it derives the legacy
/// triplet from the *first* index/range key spec rather than from the whole
/// key-extraction procedure (`populateCommandLegacyRangeSpec` in `server.c`):
/// SORT is `1 1 1` and MIGRATE `3 3 1` even though both are `movablekeys`.
/// Running the same derivation over the key specs we emit keeps the two halves
/// of our own reply consistent, and stays truthful because those specs are the
/// vendored ones the join test replays against real key extraction.
fn legacy_range(spec: &CommandSpec) -> (i64, i64, i64) {
    let (first_key, last_key, key_step, _) = spec.keys.command_info_triplet();
    if first_key != 0 {
        return (first_key, last_key, key_step);
    }
    let Some(vendored) = vendored_key_specs(spec.name) else {
        return (first_key, last_key, key_step);
    };
    vendored
        .iter()
        .find_map(|ks| match (ks.begin_search, ks.find_keys) {
            (BeginSearch::Index { pos }, FindKeys::Range { lastkey, step, .. }) => {
                let first = i64::from(pos);
                // `lastkey` is relative to `pos`; `-1` keeps its "to the last
                // argument" meaning instead of being offset.
                let last = if lastkey == -1 {
                    -1
                } else {
                    first + i64::from(lastkey)
                };
                Some((first, last, i64::from(step)))
            }
            _ => None,
        })
        .unwrap_or((first_key, last_key, key_step))
}

/// Redis's `firstkey`-relative arity encoding: exact arities (`CommandSpec::
/// arity` min == max) are positive (including the command name itself, so
/// `Arity::Fixed(1)` — one argument after the name — is wire arity `2`);
/// open-ended/ranged arities are negative minimums, matching how Redis
/// reports e.g. `SET` as `-3` and `PING` as `-1`.
///
/// Public so the upstream-metadata join test can cross-check vendored arities
/// against the very function that answers `COMMAND INFO`, rather than against
/// a second copy of this encoding.
pub fn command_info_arity(arity: Arity) -> i64 {
    match arity {
        Arity::Fixed(n) => n as i64 + 1,
        Arity::AtLeast(n) => -(n as i64 + 1),
        Arity::Range { min, max } if min == max => min as i64 + 1,
        Arity::Range { min, .. } => -(min as i64 + 1),
    }
}

/// Every `CommandFlags` bit that has a `COMMAND INFO` wire spelling, in the
/// order Redis's `addReplyFlagsForCommand` emits them (`commandFlagNames` in
/// `server.c`). Clients treat the array as a set, but matching the order keeps
/// a byte-for-byte diff against a real server meaningful.
///
/// Flags Redis has and FrogDB does not model (`may_replicate`, `skip_monitor`,
/// `no_auth`, ...) are never fabricated; flags FrogDB models with no Redis
/// counterpart are listed in [`EXTENSION_FLAGS`] and sort after all of these.
pub const WIRE_FLAGS: &[(CommandFlags, &str)] = &[
    (CommandFlags::WRITE, "write"),
    (CommandFlags::READONLY, "readonly"),
    (CommandFlags::DENYOOM, "denyoom"),
    (CommandFlags::ADMIN, "admin"),
    (CommandFlags::PUBSUB, "pubsub"),
    (CommandFlags::NOSCRIPT, "noscript"),
    (CommandFlags::BLOCKING, "blocking"),
    (CommandFlags::LOADING, "loading"),
    (CommandFlags::STALE, "stale"),
    (CommandFlags::SKIP_SLOWLOG, "skip_slowlog"),
    (CommandFlags::FAST, "fast"),
    (CommandFlags::MOVABLEKEYS, "movablekeys"),
];

/// Wire flags FrogDB advertises that no Redis version defines. Kept separate so
/// the flag-parity test can subtract them before comparing against upstream's
/// declared flag set instead of reporting each one as a divergence.
pub const EXTENSION_FLAGS: &[(&str, &str)] = &[(
    "no-propagate",
    "FrogDB-only: the command writes but is deliberately never shipped to \
     replicas (its local WAL/notification/WATCH effects still run). Redis has \
     no wire flag for this — its equivalents are compiled-in special cases.",
)];

/// Redis-wire flag strings for a command, plus the derived `movablekeys` fact
/// (true even when `CommandFlags::MOVABLEKEYS` itself is unset, for the
/// `NumkeysAt`/`DestThenNumkeys` key specs that are movable by construction).
///
/// Only flags this registry actually tracks are emitted; flags Redis has but
/// FrogDB's `CommandFlags` does not model are never fabricated.
pub fn command_info_flags(spec: &CommandSpec) -> Vec<Response> {
    let flags = effective_flags(spec);
    let mut out: Vec<Response> = WIRE_FLAGS
        .iter()
        .filter(|(bit, _)| flags.contains(*bit))
        .map(|(_, name)| status(name))
        .collect();
    if flags.contains(CommandFlags::NO_PROPAGATE) {
        out.push(status("no-propagate"));
    }
    out
}

/// The command's declared flags plus the facts its `KeySpec` implies.
///
/// `CommandSpec::validate` only forces `CommandFlags::MOVABLEKEYS` to agree
/// with `KeySpec::Dynamic`; `NumkeysAt`/`DestThenNumkeys` commands are movable
/// too (see `KeySpec::command_info_triplet`) without necessarily carrying the
/// bit themselves, so the wire fact is the union of both.
pub fn effective_flags(spec: &CommandSpec) -> CommandFlags {
    let (_, _, _, dynamic_movable) = spec.keys.command_info_triplet();
    if dynamic_movable {
        spec.flags.union(CommandFlags::MOVABLEKEYS)
    } else {
        spec.flags
    }
}

/// Redis's `ACLCommandCategories` order (`acl.c`), which is the order
/// `COMMAND INFO` lists a command's categories in. Verified against every
/// command and subcommand a Redis 8.6.1 server reports.
const ACL_CATEGORY_ORDER: &[&str] = &[
    "keyspace",
    "read",
    "write",
    "set",
    "sortedset",
    "list",
    "hash",
    "string",
    "bitmap",
    "hyperloglog",
    "geo",
    "stream",
    "pubsub",
    "admin",
    "fast",
    "slow",
    "blocking",
    "dangerous",
    "connection",
    "transaction",
    "scripting",
];

/// `@`-prefixed ACL category names for a command, from the real ACL registry
/// (`frogdb_acl::CommandCategory`) rather than a re-derivation from
/// `CommandFlags` — the two are joined only by the command's lowercase name,
/// so a command absent from the ACL category table truthfully reports no
/// categories rather than a guessed one.
///
/// The categories themselves come from our table; only their *order* is
/// borrowed from Redis, so a client diffing the two replies sees the same
/// sequence. A category FrogDB has that Redis does not sorts last.
fn command_info_categories(name: &str) -> Vec<Response> {
    let mut cats: Vec<&'static str> = frogdb_acl::CommandCategory::all_for_command(name)
        .into_iter()
        .map(|cat| cat.name())
        .collect();
    cats.sort_by_key(|name| {
        ACL_CATEGORY_ORDER
            .iter()
            .position(|c| c == name)
            .unwrap_or(ACL_CATEGORY_ORDER.len())
    });
    cats.into_iter()
        .map(|cat| Response::Simple(SafeStatus::sanitized(format!("@{cat}"))))
        .collect()
}

// ---------------------------------------------------------------------------
// Tips
// ---------------------------------------------------------------------------

/// One command's tip ruling.
///
/// A tip is a promise to a cluster-aware client: `request_policy`/
/// `response_policy` tell a proxy how to fan a command out and fold the
/// replies back together, and `nondeterministic_output`/`_order` tell a caching
/// or comparison layer that two identical calls may differ. Repeating
/// upstream's list unchecked would make promises about FrogDB's execution that
/// nobody verified, so every tipped command gets a row here.
pub struct TipRuling {
    /// ASCII-uppercase command name.
    pub command: &'static str,
    /// The tips `COMMAND INFO` emits, lowercase wire form, in upstream's order.
    pub tips: &'static [&'static str],
    /// Why `tips` is not simply upstream's whole list. `None` means we repeat
    /// upstream verbatim.
    pub omission: Option<&'static str>,
}

/// The tip audit (wave D2). Every core command Redis 8.6.1 tags with tips has a
/// row; the eight rows carrying an `omission` are the recorded deliberate
/// divergences, and `tip_audit_matches_upstream` in the server's join tests
/// fails if a row goes stale, fabricates a tip upstream never declared, or if a
/// newly-tipped upstream command has no row at all.
pub static TIP_AUDIT: &[TipRuling] = &[
    // ---- Routing: FrogDB's reply is node-local exactly as Redis's is. The
    // "scatter-gather" machinery fans out over *intra-node* shards only
    // (`server/src/connection/scatter.rs`), and cluster mode rejects
    // cross-slot key sets with -CROSSSLOT before execution
    // (`server/src/connection/guards.rs`), so a proxy must still fan out.
    verbatim(
        "DBSIZE",
        &["request_policy:all_shards", "response_policy:agg_sum"],
    ),
    verbatim(
        "DEL",
        &["request_policy:multi_shard", "response_policy:agg_sum"],
    ),
    verbatim(
        "EXISTS",
        &["request_policy:multi_shard", "response_policy:agg_sum"],
    ),
    verbatim(
        "FLUSHALL",
        &["request_policy:all_shards", "response_policy:all_succeeded"],
    ),
    verbatim(
        "FLUSHDB",
        &["request_policy:all_shards", "response_policy:all_succeeded"],
    ),
    verbatim("MGET", &["request_policy:multi_shard"]),
    verbatim(
        "MSET",
        &[
            "request_policy:multi_shard",
            "response_policy:all_succeeded",
        ],
    ),
    verbatim(
        "PING",
        &["request_policy:all_shards", "response_policy:all_succeeded"],
    ),
    verbatim(
        "TOUCH",
        &["request_policy:multi_shard", "response_policy:agg_sum"],
    ),
    verbatim(
        "UNLINK",
        &["request_policy:multi_shard", "response_policy:agg_sum"],
    ),
    verbatim(
        "WAIT",
        &["request_policy:all_shards", "response_policy:agg_min"],
    ),
    verbatim(
        "INFO",
        &[
            "nondeterministic_output",
            "request_policy:all_shards",
            "response_policy:special",
        ],
    ),
    verbatim(
        "RANDOMKEY",
        &[
            "request_policy:all_shards",
            "response_policy:special",
            "nondeterministic_output",
        ],
    ),
    // ---- Nondeterminism that holds for FrogDB too.
    verbatim("COMMAND", &["nondeterministic_output_order"]),
    verbatim("DUMP", &["nondeterministic_output"]),
    verbatim("HGETALL", &["nondeterministic_output_order"]),
    verbatim("HKEYS", &["nondeterministic_output_order"]),
    verbatim("HVALS", &["nondeterministic_output_order"]),
    verbatim("HPTTL", &["nondeterministic_output"]),
    verbatim("HTTL", &["nondeterministic_output"]),
    verbatim("HRANDFIELD", &["nondeterministic_output"]),
    verbatim("LASTSAVE", &["nondeterministic_output"]),
    verbatim("MIGRATE", &["nondeterministic_output"]),
    verbatim("PTTL", &["nondeterministic_output"]),
    verbatim("TTL", &["nondeterministic_output"]),
    verbatim("SDIFF", &["nondeterministic_output_order"]),
    verbatim("SINTER", &["nondeterministic_output_order"]),
    verbatim("SMEMBERS", &["nondeterministic_output_order"]),
    verbatim("SUNION", &["nondeterministic_output_order"]),
    verbatim("SPOP", &["nondeterministic_output"]),
    verbatim("SRANDMEMBER", &["nondeterministic_output"]),
    verbatim("ZRANDMEMBER", &["nondeterministic_output"]),
    verbatim("TIME", &["nondeterministic_output"]),
    verbatim("XADD", &["nondeterministic_output"]),
    verbatim("XAUTOCLAIM", &["nondeterministic_output"]),
    verbatim("XCLAIM", &["nondeterministic_output"]),
    verbatim("XPENDING", &["nondeterministic_output"]),
    // ---- Deliberate divergences.
    TipRuling {
        command: "KEYS",
        tips: &["request_policy:all_shards"],
        omission: Some(
            "nondeterministic_output_order dropped: FrogDB's KEYS folds the per-shard \
             replies through `SortedUnion`, which sorts the key list before replying \
             (`server/src/scatter/broadcast.rs`), so the order is fully determined by \
             the matched key set",
        ),
    },
    TipRuling {
        command: "SCAN",
        tips: &["request_policy:special", "response_policy:special"],
        omission: Some(
            "nondeterministic_output dropped: FrogDB walks each shard in a fixed-seed \
             content-hash order rather than in bucket-layout order \
             (`core/src/store/hashmap.rs`), so a repeated scan of an unchanged keyspace \
             returns the same keys in the same order",
        ),
    },
    TipRuling {
        command: "HSCAN",
        tips: &[],
        omission: Some(
            "nondeterministic_output dropped: same fixed-seed content-hash cursor as \
             SCAN (`commands/src/utils.rs`, `hash_cursor_scan`)",
        ),
    },
    TipRuling {
        command: "SSCAN",
        tips: &[],
        omission: Some("nondeterministic_output dropped: same fixed-seed cursor as HSCAN"),
    },
    TipRuling {
        command: "ZSCAN",
        tips: &[],
        omission: Some("nondeterministic_output dropped: same fixed-seed cursor as HSCAN"),
    },
    TipRuling {
        command: "XTRIM",
        tips: &[],
        omission: Some(
            "nondeterministic_output dropped: FrogDB's approximate (`~`) trim is a pure \
             function of the stream length, the threshold and LIMIT — a deterministic \
             simulation of Redis's radix-node granularity (`types/src/types/stream.rs`) \
             rather than a walk of a real node layout",
        ),
    },
    TipRuling {
        command: "MSETEX",
        tips: &[],
        omission: Some(
            "request_policy:multi_shard / response_policy:all_succeeded dropped: FrogDB \
             declares MSETEX `requires_same_slot`, so a key set spanning slots is \
             rejected with -CROSSSLOT instead of being split across shards \
             (`commands/src/string.rs`)",
        ),
    },
    TipRuling {
        command: "WAITAOF",
        tips: &[],
        omission: Some(
            "request_policy:all_shards / response_policy:agg_min dropped: WAITAOF is an \
             unimplemented stub that always replies with an error \
             (`server/src/commands/stub.rs`), so no fan-out or aggregation policy \
             describes it",
        ),
    },
];

const fn verbatim(command: &'static str, tips: &'static [&'static str]) -> TipRuling {
    TipRuling {
        command,
        tips,
        omission: None,
    }
}

/// Look up the tip ruling for an ASCII-uppercase command name.
pub fn tip_ruling(name: &str) -> Option<&'static TipRuling> {
    TIP_AUDIT.iter().find(|row| row.command == name)
}

fn command_info_tips(name: &str) -> Vec<Response> {
    tip_ruling(name)
        .map(|row| row.tips.iter().copied().map(text).collect())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Key specs
// ---------------------------------------------------------------------------

/// Commands whose vendored key specs do not describe FrogDB's real key
/// extraction, so `COMMAND INFO` derives their key-specs entry from our own
/// [`frogdb_core::KeySpec`] instead of repeating upstream's.
///
/// Kept in lockstep with `KEY_SPEC_EXEMPTIONS` in the server's join tests,
/// which is where each entry's reason lives and which asserts the two lists
/// name the same commands — a divergence the join test tolerates must never be
/// one this emitter repeats as if it were ours.
pub const KEY_SPEC_DIVERGENCES: &[&str] = &["MOVE"];

/// The vendored key specs `COMMAND INFO` may repeat for `name`, or `None` when
/// the reply has to be derived from our own key spec instead.
fn vendored_key_specs(name: &str) -> Option<&'static [UpstreamKeySpec]> {
    if KEY_SPEC_DIVERGENCES.contains(&name) {
        return None;
    }
    let cmd = upstream::command(name)?;
    if cmd.has_subcommands {
        // Upstream keeps this container's real key specs on the subcommand rows
        // the vendor step skips, so its empty list means "not vendored here".
        // FrogDB models the container as one command with its own key spec, and
        // that is the honest thing to describe.
        return None;
    }
    cmd.key_specs
}

/// Structured `COMMAND INFO` key-specs entries.
///
/// Vendored specs win where they exist, because they are the richer and
/// already-verified description: the join test replays every one of them
/// against FrogDB's actual key extraction. Everything else — the module
/// families (no upstream repo publishes key specs; they declare key positions
/// in C at `RedisModule_CreateCommand` time), the FrogDB-only commands, and
/// the container commands upstream documents through subcommands — falls back
/// to a spec derived from our own `KeySpec`, which is less detailed but true.
fn command_info_key_specs(spec: &CommandSpec) -> Vec<Response> {
    match vendored_key_specs(spec.name) {
        Some(specs) => specs.iter().map(upstream_key_spec_reply).collect(),
        None => derived_key_specs(spec),
    }
}

/// Redis's `addReplyFlagsForKeyArgs` order and casing: the access class is
/// uppercase, the individual operations are lowercase. Vendored flags are
/// upstream's JSON spelling (all uppercase, and in JSON order, which differs
/// from the reply order for at least BITFIELD), so they are re-sorted here.
const KEY_SPEC_FLAG_ORDER: &[(&str, &str)] = &[
    ("RO", "RO"),
    ("RW", "RW"),
    ("OW", "OW"),
    ("RM", "RM"),
    ("ACCESS", "access"),
    ("UPDATE", "update"),
    ("INSERT", "insert"),
    ("DELETE", "delete"),
    ("NOT_KEY", "not_key"),
    ("INCOMPLETE", "incomplete"),
    ("VARIABLE_FLAGS", "variable_flags"),
];

fn key_spec_flags_reply(flags: &[&'static str]) -> Response {
    Response::Array(
        KEY_SPEC_FLAG_ORDER
            .iter()
            .filter(|(vendored, _)| flags.contains(vendored))
            .map(|(_, wire)| status(wire))
            .collect(),
    )
}

fn upstream_key_spec_reply(spec: &UpstreamKeySpec) -> Response {
    let mut fields = Vec::with_capacity(4);
    if let Some(notes) = spec.notes {
        fields.push((field("notes"), text(notes)));
    }
    fields.push((field("flags"), key_spec_flags_reply(spec.flags)));
    fields.push((field("begin_search"), begin_search_reply(spec.begin_search)));
    fields.push((field("find_keys"), find_keys_reply(spec.find_keys)));
    Response::Map(fields)
}

fn typed_spec(kind: &'static str, spec: Vec<(Response, Response)>) -> Response {
    Response::Map(vec![
        (field("type"), text(kind)),
        (field("spec"), Response::Map(spec)),
    ])
}

fn begin_search_reply(begin: BeginSearch) -> Response {
    match begin {
        BeginSearch::Index { pos } => typed_spec(
            "index",
            vec![(field("index"), Response::Integer(pos.into()))],
        ),
        BeginSearch::Keyword { keyword, startfrom } => typed_spec(
            "keyword",
            vec![
                (field("keyword"), text(keyword)),
                (field("startfrom"), Response::Integer(startfrom.into())),
            ],
        ),
        BeginSearch::Unknown => typed_spec("unknown", vec![]),
    }
}

fn find_keys_reply(find: FindKeys) -> Response {
    match find {
        FindKeys::Range {
            lastkey,
            step,
            limit,
        } => typed_spec(
            "range",
            vec![
                (field("lastkey"), Response::Integer(lastkey.into())),
                (field("keystep"), Response::Integer(step.into())),
                (field("limit"), Response::Integer(limit.into())),
            ],
        ),
        FindKeys::Keynum {
            keynumidx,
            firstkey,
            step,
        } => typed_spec(
            "keynum",
            vec![
                (field("keynumidx"), Response::Integer(keynumidx.into())),
                (field("firstkey"), Response::Integer(firstkey.into())),
                (field("keystep"), Response::Integer(step.into())),
            ],
        ),
        FindKeys::Unknown => typed_spec("unknown", vec![]),
    }
}

/// A single index/range key spec built from our own `KeySpec`, for the
/// commands upstream publishes nothing for.
///
/// Movable-key commands (`first_key == 0`: `NumkeysAt`/`Dynamic`) and keyless
/// commands report no entries — the structured form for those needs the
/// per-command `keynum`/`unknown` metadata this registry does not carry, and
/// inventing it would be a claim we cannot support.
fn derived_key_specs(spec: &CommandSpec) -> Vec<Response> {
    let (first_key, last_key, key_step, _) = spec.keys.command_info_triplet();
    if first_key == 0 {
        return Vec::new();
    }
    // `find_keys`'s `lastkey` is relative to `begin_search`'s index (0 means
    // "the same key as first_key"); `-1` still means "to the last argument".
    let relative_last_key = if last_key == -1 {
        -1
    } else {
        last_key - first_key
    };
    vec![Response::Map(vec![
        (field("flags"), key_spec_flags_reply(derived_flags(spec))),
        (
            field("begin_search"),
            typed_spec(
                "index",
                vec![(field("index"), Response::Integer(first_key))],
            ),
        ),
        (
            field("find_keys"),
            typed_spec(
                "range",
                vec![
                    (field("lastkey"), Response::Integer(relative_last_key)),
                    (field("keystep"), Response::Integer(key_step)),
                    (field("limit"), Response::Integer(0)),
                ],
            ),
        ),
    ])]
}

/// Upstream-spelled access flags for a derived key spec, from the command's
/// [`AccessSpec`].
///
/// A derived spec covers every key with one flag list, so a `Positional` spec
/// that gives different keys different access reports no flags at all rather
/// than picking one position's answer for all of them. `Dynamic` access is
/// resolved by the command at runtime and is likewise not stated statically.
fn derived_flags(spec: &CommandSpec) -> &'static [&'static str] {
    let uniform = match spec.access {
        AccessSpec::Uniform => {
            if spec.flags.contains(CommandFlags::WRITE) {
                KeyAccessFlag::OW
            } else {
                KeyAccessFlag::R
            }
        }
        AccessSpec::UniformRW => KeyAccessFlag::RW,
        AccessSpec::Positional(flags) => match flags.split_first() {
            Some((first, rest)) if rest.iter().all(|f| f == first) => *first,
            _ => return &[],
        },
        AccessSpec::Dynamic => return &[],
    };
    match uniform {
        KeyAccessFlag::R => &["RO", "ACCESS"],
        KeyAccessFlag::W | KeyAccessFlag::OW => &["OW", "UPDATE"],
        KeyAccessFlag::RW => &["RW", "ACCESS", "UPDATE"],
    }
}

// ---------------------------------------------------------------------------
// COMMAND DOCS
// ---------------------------------------------------------------------------

/// Build the `COMMAND DOCS` value map for one command.
///
/// Field order follows Redis's `addReplyCommandDocs`: summary, since, group,
/// complexity, doc_flags, deprecated_since, replaced_by, history, arguments.
/// Fields FrogDB has no data source for are *omitted* rather than emitted
/// empty — Redis omits `complexity` the same way for commands whose upstream
/// JSON has none, and clients treat every field as optional.
///
/// `summary`/`since`/`group`/`complexity` come from our own `CommandSpec`,
/// which is what makes them true of FrogDB's implementation of the command.
/// The rest describe the command *surface* — its grammar, when options were
/// added, whether it is deprecated — and come from the vendored snapshot, so
/// commands with no vendored row (the module families and the FrogDB-only
/// verbs) simply omit them.
///
/// `module` and `subcommands` are never emitted: FrogDB implements the
/// extension families natively rather than loading them as modules, and no
/// structured per-subcommand registry exists.
pub fn build_command_docs(spec: &CommandSpec) -> Response {
    let mut fields = vec![
        (field("summary"), text(spec.docs.summary)),
        (field("since"), text(spec.docs.since)),
        (field("group"), text(spec.docs.group)),
    ];
    if let Some(complexity) = spec.docs.complexity {
        fields.push((field("complexity"), text(complexity)));
    }
    let Some(cmd) = upstream::command(spec.name) else {
        return Response::Map(fields);
    };
    if !cmd.doc_flags.is_empty() {
        fields.push((
            field("doc_flags"),
            Response::Array(
                cmd.doc_flags
                    .iter()
                    .map(|flag| Response::Simple(SafeStatus::sanitized(flag.to_lowercase())))
                    .collect(),
            ),
        ));
    }
    if let Some(since) = cmd.deprecated_since {
        fields.push((field("deprecated_since"), text(since)));
    }
    if let Some(replaced_by) = cmd.replaced_by {
        fields.push((field("replaced_by"), text(replaced_by)));
    }
    if !cmd.history.is_empty() {
        fields.push((
            field("history"),
            Response::Array(
                cmd.history
                    .iter()
                    .map(|entry| Response::Array(vec![text(entry.version), text(entry.change)]))
                    .collect(),
            ),
        ));
    }
    if !cmd.arguments.is_empty() {
        fields.push((field("arguments"), arguments_reply(cmd.arguments)));
    }
    Response::Map(fields)
}

fn arguments_reply(args: &'static [UpstreamArg]) -> Response {
    Response::Array(args.iter().map(argument_reply).collect())
}

/// One `COMMAND DOCS` argument node. Field order follows Redis's
/// `addReplyCommandArgList`: name, type, display_text, key_spec_index, token,
/// since, flags, arguments.
fn argument_reply(arg: &'static UpstreamArg) -> Response {
    let mut fields = Vec::with_capacity(8);
    if let Some(name) = arg.name {
        fields.push((field("name"), text(name)));
    }
    if let Some(kind) = arg.kind {
        fields.push((field("type"), text(kind)));
    }
    // Redis shows `display_text` for every leaf argument, defaulting it to the
    // argument name; grouping nodes (`oneof`/`block`) get one only when
    // upstream spelled an explicit override.
    let display = arg
        .display_text
        .or_else(|| arg.arguments.is_empty().then_some(arg.name).flatten());
    if let Some(display) = display {
        fields.push((field("display_text"), text(display)));
    }
    if let Some(index) = arg.key_spec_index {
        fields.push((field("key_spec_index"), Response::Integer(index.into())));
    }
    if let Some(token) = arg.token {
        fields.push((field("token"), text(token)));
    }
    if let Some(since) = arg.since {
        fields.push((field("since"), text(since)));
    }
    if let Some(since) = arg.deprecated_since {
        fields.push((field("deprecated_since"), text(since)));
    }
    let mut flags = Vec::with_capacity(3);
    if arg.optional {
        flags.push(status("optional"));
    }
    if arg.multiple {
        flags.push(status("multiple"));
    }
    if arg.multiple_token {
        flags.push(status("multiple_token"));
    }
    if !flags.is_empty() {
        fields.push((field("flags"), Response::Array(flags)));
    }
    if !arg.arguments.is_empty() {
        fields.push((field("arguments"), arguments_reply(arg.arguments)));
    }
    Response::Map(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tip_audit_is_sorted_within_its_sections_and_unique() {
        let mut seen = std::collections::BTreeSet::new();
        for row in TIP_AUDIT {
            assert!(
                seen.insert(row.command),
                "{} appears twice in TIP_AUDIT",
                row.command
            );
            assert!(
                !row.command.chars().any(|c| c.is_ascii_lowercase()),
                "TIP_AUDIT is keyed by ASCII-uppercase name; got {}",
                row.command
            );
        }
    }

    /// A row with no omission must repeat upstream's list exactly; a row with
    /// one must actually drop something. (The full join against the live
    /// registry lives in the server crate; this catches an inconsistent row
    /// without needing one.)
    #[test]
    fn tip_rulings_agree_with_their_own_omission_note() {
        for row in TIP_AUDIT {
            let upstream_tips: Vec<String> = upstream::command(row.command)
                .map(|cmd| cmd.command_tips.iter().map(|t| t.to_lowercase()).collect())
                .unwrap_or_default();
            assert!(
                !upstream_tips.is_empty(),
                "{} has a TIP_AUDIT row but upstream declares no tips",
                row.command
            );
            for tip in row.tips {
                assert!(
                    upstream_tips.iter().any(|u| u == tip),
                    "{} emits {tip:?}, which upstream never declares",
                    row.command
                );
            }
            let complete = row.tips.len() == upstream_tips.len();
            assert_eq!(
                complete,
                row.omission.is_none(),
                "{}: omission note and emitted tip count disagree (emits {:?}, upstream {:?})",
                row.command,
                row.tips,
                upstream_tips
            );
        }
    }

    #[test]
    fn key_spec_flags_are_emitted_in_redis_order() {
        // BITFIELD's vendored order is RW, UPDATE, ACCESS, VARIABLE_FLAGS;
        // Redis replies RW, access, update, variable_flags.
        let reply = key_spec_flags_reply(&["RW", "UPDATE", "ACCESS", "VARIABLE_FLAGS"]);
        assert_eq!(
            reply,
            Response::Array(vec![
                status("RW"),
                status("access"),
                status("update"),
                status("variable_flags"),
            ])
        );
    }
}
