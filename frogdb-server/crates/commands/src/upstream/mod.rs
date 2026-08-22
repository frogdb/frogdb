//! Vendored upstream command metadata, as static Rust data.
//!
//! This module is the Rust-side view of the two vendored JSON snapshots
//! `website/scripts/vendor-redis-commands.py` produces:
//!
//! - `website/src/data/redis-commands-8x.json` — `redis/redis` core commands.
//! - `website/src/data/redis-module-commands-8x.json` — the command families
//!   the pinned Redis release bundles as modules (RedisJSON, RediSearch,
//!   RedisTimeSeries, RedisBloom, vector-sets).
//!
//! The tables in [`generated`] are produced by
//! `uv run scripts/gen-command-metadata.py` (`just command-metadata-gen`) and
//! checked in. Everything is a `&'static` slice of plain data: no serde at
//! runtime, no allocation at startup, nothing parsed on the first request.
//!
//! # What this is *not*
//!
//! It is a record of what upstream declares, not a claim about what FrogDB
//! does. Per ADR-0005, FrogDB's own `COMMAND INFO`/`COMMAND DOCS` replies are
//! derived from each [`CommandSpec`](frogdb_core::CommandSpec)'s real
//! behavior. This table exists so tests can *compare* the two and force every
//! divergence to be deliberate — see
//! `frogdb-server/crates/server/src/server/upstream_metadata_tests.rs`.
//!
//! # Absent vs empty
//!
//! Upstream does not publish every field for every command, and an absent
//! field is never defaulted into a fabricated value:
//!
//! - [`UpstreamCommand::arity`] and [`UpstreamCommand::command_flags`] are
//!   `None` for the container commands core Redis leaves them off, and for
//!   every module family except vector-sets.
//! - [`UpstreamCommand::key_specs`] is `None` when the source publishes no
//!   key-spec data *at all* (every module family — modules declare key
//!   positions in C/Rust at `RedisModule_CreateCommand` time). `Some(&[])`
//!   means the source does publish key specs as data and this command has
//!   none, i.e. it genuinely takes no keys — *unless* the row is a container
//!   ([`UpstreamCommand::has_subcommands`]), where the keys live on the
//!   [`subcommands`](UpstreamCommand::subcommands) rows instead.

pub mod generated;

pub use generated::{MODULE_FAMILIES, REDIS_COMMANDS, REDIS_VERSION};

/// Where a metadata row came from — which decides how to read an absent field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataSource {
    /// `redis/redis` `src/commands/*.json`. Publishes arity, command flags and
    /// key specs for every command, so an absent `key_specs` means "no keys".
    Redis,
    /// A bundled module's root `commands.json`, named by its upstream family.
    /// Publishes no key specs at all.
    Module(&'static str),
}

/// Where the search for a key spec's first key starts, mirroring Redis's
/// `begin_search` — positions are argv indices with the command name at 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BeginSearch {
    /// The first key sits at a fixed argv index.
    Index { pos: i32 },
    /// The first key sits just after `keyword`, searched for from `startfrom`
    /// (forwards when positive, backwards from the end when negative).
    Keyword {
        keyword: &'static str,
        startfrom: i32,
    },
    /// Upstream declares the search undecidable from argv shape alone (SORT's
    /// `BY`/`GET`/`STORE`). No static check can be made against it.
    Unknown,
}

/// How the remaining keys are located once `begin_search` found the first,
/// mirroring Redis's `find_keys`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FindKeys {
    /// A range relative to the first key. `lastkey >= 0` is an offset from the
    /// first key; `lastkey < 0` counts back from the end of argv (with `limit`
    /// dividing the tail first, as XREAD's key/id split does).
    Range { lastkey: i32, step: i32, limit: i32 },
    /// A count read out of argv at `first + keynumidx`, followed by that many
    /// keys starting at `first + firstkey`.
    Keynum {
        keynumidx: i32,
        firstkey: i32,
        step: i32,
    },
    /// Upstream declares the keys undecidable from argv shape alone.
    Unknown,
}

/// One upstream key spec: which argv positions hold keys, and how they are
/// accessed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpstreamKeySpec {
    /// Upstream's prose caveat on this spec, when it has one.
    pub notes: Option<&'static str>,
    /// Upstream access flags, verbatim and uppercase (`RO`, `RW`, `OW`, `RM`,
    /// `ACCESS`, `UPDATE`, `INSERT`, `DELETE`, `NOT_KEY`, `INCOMPLETE`,
    /// `VARIABLE_FLAGS`).
    pub flags: &'static [&'static str],
    pub begin_search: BeginSearch,
    pub find_keys: FindKeys,
}

impl UpstreamKeySpec {
    /// Whether this spec is decidable from argv shape alone.
    pub fn is_complete(&self) -> bool {
        !matches!(self.begin_search, BeginSearch::Unknown)
            && !matches!(self.find_keys, FindKeys::Unknown)
    }
}

/// One node of an upstream `arguments` tree, as `COMMAND DOCS` describes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpstreamArg {
    /// `None` for the bare positional placeholder nodes RediSearch's
    /// `commands.json` contains — a `token` and nothing else, no name and no
    /// type. Carried through as absent rather than back-filled from the token.
    pub name: Option<&'static str>,
    /// Upstream's `type` (`key`, `string`, `integer`, `double`, `pattern`,
    /// `unix-time`, `pure-token`, `oneof`, `block`, `function`). `None` where
    /// upstream leaves it null — RediSearch does for some grouping nodes.
    pub kind: Option<&'static str>,
    /// The literal token that introduces this argument (`EX`, `STORE`, ...).
    pub token: Option<&'static str>,
    /// Name to show instead of `name`. Core Redis spells this field `display`
    /// and the module repos spell it `display_text`; both land here, under the
    /// name Redis's own `COMMAND DOCS` reply uses.
    pub display_text: Option<&'static str>,
    pub since: Option<&'static str>,
    pub deprecated_since: Option<&'static str>,
    /// Index into the owning command's `key_specs` when this argument is a key.
    pub key_spec_index: Option<u32>,
    pub optional: bool,
    pub multiple: bool,
    pub multiple_token: bool,
    /// Child arguments, for `oneof`/`block` nodes.
    pub arguments: &'static [UpstreamArg],
}

/// One `(version, change)` row of an upstream `history` list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryEntry {
    pub version: &'static str,
    pub change: &'static str,
}

/// Everything the vendored snapshots say about one upstream command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpstreamCommand {
    /// ASCII-uppercase command name, e.g. `"GET"`, `"JSON.SET"`.
    pub name: &'static str,
    pub source: MetadataSource,
    pub group: &'static str,
    pub since: &'static str,
    pub summary: &'static str,
    pub complexity: Option<&'static str>,
    /// Upstream documentation flags, verbatim and uppercase (`DEPRECATED`,
    /// `SYSCMD`); empty where upstream declares none.
    pub doc_flags: &'static [&'static str],
    /// Release that deprecated the command, set iff `doc_flags` says
    /// `DEPRECATED`.
    pub deprecated_since: Option<&'static str>,
    /// Markdown prose naming the replacement, alongside `deprecated_since`.
    pub replaced_by: Option<&'static str>,
    /// Redis wire arity: positive is exact, negative is "at least `-arity`",
    /// both counting the command name. `None` where upstream omits it.
    pub arity: Option<i32>,
    /// Upstream command flags, verbatim and uppercase. `None` where upstream
    /// omits them (never `Some(&[])` for a command that simply has none —
    /// upstream omits the field in that case too).
    pub command_flags: Option<&'static [&'static str]>,
    /// Upstream's **explicit** ACL categories, verbatim and uppercase
    /// (`"KEYSPACE"`, `"DANGEROUS"`, ...); empty where upstream declares none.
    ///
    /// Only half of upstream's answer. Redis folds in the rest at registration
    /// time from `command_flags` (`setImplicitACLCategories`: `write` implies
    /// `@write`, `readonly` implies `@read` unless the command is `@scripting`,
    /// `admin` implies `@admin` *and* `@dangerous`, `pubsub` implies `@pubsub`,
    /// `fast` implies `@fast` and anything left over is `@slow`, `blocking`
    /// implies `@blocking`), so the effective set is this list unioned with
    /// that derivation. The parity gate in the server crate
    /// (`upstream_metadata_tests::vendored_acl_categories_agree_with_our_table`)
    /// re-derives the implied half from FrogDB's *own* flags, so the vendored
    /// half is the only thing taken verbatim.
    ///
    /// Always empty for module rows: no module `commands.json` declares the
    /// field, because modules set their categories in C at
    /// `RedisModule_SetCommandACLCategories` time.
    pub acl_categories: &'static [&'static str],
    /// Upstream command tips, verbatim and uppercase
    /// (`REQUEST_POLICY:ALL_SHARDS`, `NONDETERMINISTIC_OUTPUT`, ...); empty
    /// where upstream declares none. Tips are claims about *routing* and
    /// *reply determinism*, so unlike arity they are not automatically true of
    /// FrogDB — see [`crate::command_meta::TIP_AUDIT`] for which ones we repeat.
    pub command_tips: &'static [&'static str],
    /// See the module docs on absent vs empty.
    pub key_specs: Option<&'static [UpstreamKeySpec]>,
    pub arguments: &'static [UpstreamArg],
    pub history: &'static [HistoryEntry],
    /// This command's subcommand rows, sorted by name, for the container
    /// commands (`ACL`, `OBJECT`, `XINFO`, ...). Upstream keeps a container's
    /// real arity, command flags, key specs and argument trees here and leaves
    /// the container row itself nearly empty, so a container's own `key_specs`
    /// being `Some(&[])` means "described on these rows", not "takes no keys".
    ///
    /// Each row's [`name`](Self::name) is the bare subcommand (`"CREATE"`), not
    /// the `XGROUP|CREATE` spelling `COMMAND INFO` replies with; its own
    /// `subcommands` is always empty (upstream nests only one level).
    pub subcommands: &'static [UpstreamCommand],
}

impl UpstreamCommand {
    /// Whether upstream documents this command through subcommand rows — i.e.
    /// whether it is a container.
    pub fn has_subcommands(&self) -> bool {
        !self.subcommands.is_empty()
    }

    /// Look up one of this command's subcommand rows. `name` must be
    /// ASCII-uppercase.
    pub fn subcommand(&self, name: &str) -> Option<&'static UpstreamCommand> {
        self.subcommands.iter().find(|sub| sub.name == name)
    }
}

/// One bundled-module family's table, with the upstream pin it was vendored
/// from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModuleFamilyTable {
    /// Upstream family name (`"RedisJSON"`, `"RediSearch"`, ...).
    pub family: &'static str,
    pub repo: &'static str,
    pub tag: &'static str,
    /// The cargo features that compile this family's commands into the
    /// registry. Empty means "always compiled" (RediSearch: `FT.*` is
    /// registered unconditionally by the server crate).
    pub features: &'static [&'static str],
    /// Top-level commands. **Empty when the family's cargo feature is off** —
    /// the table itself is always present so callers can report the pin.
    pub commands: &'static [UpstreamCommand],
    /// Container commands whose upstream metadata lives on subcommand rows
    /// (`FT.CONFIG` from `FT.CONFIG GET`, ...). Names are the containers, not
    /// the subcommands, deduplicated and sorted. Also empty when the feature
    /// is off.
    pub containers: &'static [&'static str],
}

/// Look up a core Redis command. `name` must be ASCII-uppercase.
pub fn redis_command(name: &str) -> Option<&'static UpstreamCommand> {
    find(REDIS_COMMANDS, name)
}

/// Look up a bundled-module command, returning its family table too. `name`
/// must be ASCII-uppercase. Families whose cargo feature is off never match.
pub fn module_command(
    name: &str,
) -> Option<(&'static ModuleFamilyTable, &'static UpstreamCommand)> {
    MODULE_FAMILIES
        .iter()
        .find_map(|family| find(family.commands, name).map(|cmd| (family, cmd)))
}

/// Look up a command in either snapshot. `name` must be ASCII-uppercase.
pub fn command(name: &str) -> Option<&'static UpstreamCommand> {
    redis_command(name).or_else(|| module_command(name).map(|(_, cmd)| cmd))
}

/// Look up one subcommand row of a container command. Both names must be
/// ASCII-uppercase.
pub fn subcommand(container: &str, name: &str) -> Option<&'static UpstreamCommand> {
    command(container)?.subcommand(name)
}

/// Whether `name` is a container command a module family documents only
/// through its subcommands. `name` must be ASCII-uppercase.
pub fn is_module_container(name: &str) -> bool {
    MODULE_FAMILIES
        .iter()
        .any(|family| family.containers.contains(&name))
}

fn find(table: &'static [UpstreamCommand], name: &str) -> Option<&'static UpstreamCommand> {
    debug_assert!(
        !name.chars().any(|c| c.is_ascii_lowercase()),
        "upstream tables are keyed by ASCII-uppercase name; got {name:?}"
    );
    table
        .binary_search_by(|cmd| cmd.name.cmp(name))
        .ok()
        .map(|idx| &table[idx])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_table_is_sorted_and_searchable() {
        assert!(REDIS_COMMANDS.windows(2).all(|w| w[0].name < w[1].name));
        let get = redis_command("GET").expect("GET is vendored");
        assert_eq!(get.arity, Some(2));
        assert_eq!(get.source, MetadataSource::Redis);
        assert!(redis_command("NOSUCHCOMMAND").is_none());
    }

    #[test]
    fn module_tables_are_sorted() {
        for family in MODULE_FAMILIES {
            assert!(
                family.commands.windows(2).all(|w| w[0].name < w[1].name),
                "{} table is not sorted",
                family.family
            );
        }
    }

    #[test]
    fn set_carries_the_variable_flags_key_spec() {
        let set = redis_command("SET").expect("SET is vendored");
        let specs = set.key_specs.expect("core rows publish key specs");
        assert_eq!(specs.len(), 1);
        assert!(specs[0].flags.contains(&"VARIABLE_FLAGS"));
        assert!(specs[0].notes.is_some());
        assert_eq!(specs[0].begin_search, BeginSearch::Index { pos: 1 });
    }

    #[test]
    fn sintercard_carries_a_keynum_spec() {
        let cmd = redis_command("SINTERCARD").expect("SINTERCARD is vendored");
        let specs = cmd.key_specs.expect("core rows publish key specs");
        assert_eq!(
            specs[0].find_keys,
            FindKeys::Keynum {
                keynumidx: 0,
                firstkey: 1,
                step: 1
            }
        );
    }

    #[test]
    fn containers_carry_their_subcommand_rows() {
        let xgroup = redis_command("XGROUP").expect("XGROUP is vendored");
        assert!(xgroup.has_subcommands());
        assert_eq!(xgroup.arity, Some(-2));
        assert_eq!(xgroup.key_specs, Some(&[][..]));

        let create = xgroup
            .subcommand("CREATE")
            .expect("XGROUP CREATE is vendored");
        assert_eq!(create.arity, Some(-5));
        assert!(
            create
                .command_flags
                .expect("core rows publish flags")
                .contains(&"WRITE")
        );
        let specs = create.key_specs.expect("core rows publish key specs");
        assert_eq!(specs[0].begin_search, BeginSearch::Index { pos: 2 });

        // 8.6.1 added HOTKEYS HELP; it is the reason this vendoring stopped
        // skipping subcommand rows.
        let help = subcommand("HOTKEYS", "HELP").expect("HOTKEYS HELP is vendored");
        assert_eq!(help.arity, Some(2));
        assert_eq!(help.since, "8.6.1");
    }

    #[test]
    fn subcommand_rows_are_sorted_and_one_level_deep() {
        for cmd in REDIS_COMMANDS {
            assert!(
                cmd.subcommands.windows(2).all(|w| w[0].name < w[1].name),
                "{} subcommand rows are not sorted",
                cmd.name
            );
            for sub in cmd.subcommands {
                assert!(
                    sub.subcommands.is_empty(),
                    "{} {} nests a third level, which upstream does not have",
                    cmd.name,
                    sub.name
                );
                assert_eq!(sub.source, MetadataSource::Redis);
            }
        }
    }

    #[test]
    fn module_rows_publish_no_key_specs() {
        for family in MODULE_FAMILIES {
            for cmd in family.commands {
                assert!(
                    cmd.key_specs.is_none(),
                    "{} unexpectedly carries key specs",
                    cmd.name
                );
                assert_eq!(cmd.source, MetadataSource::Module(family.family));
            }
        }
    }
}
