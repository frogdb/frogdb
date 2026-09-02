//! Allocator configuration, applied before `main` runs.
//!
//! jemalloc reads its options at initialization from a `const char *malloc_conf`
//! symbol it declares weak, so the only way to set an option that must hold from
//! the first allocation is to *define* that symbol. An environment variable
//! would work too — but only under its prefixed name `_RJEM_MALLOC_CONF`
//! (`tikv-jemalloc-sys` prefixes every symbol, and plain `MALLOC_CONF` is
//! silently ignored; measured in spike-report-linux.md) — and it is a property
//! of how the process was launched: every launcher — systemd unit, Docker
//! `CMD`, Kubernetes spec, a developer's shell, a test harness — would have to
//! set it identically or accounting would silently differ between them.
//!
//! # `narenas:1`
//!
//! By default jemalloc pre-creates `4 × ncpus` arenas and round-robins unbound
//! threads over them. Per-shard accounting (ADR-0006 §3) wants the opposite: one
//! arena per shard thread, created explicitly, plus **one** shared arena holding
//! everything else — the acceptor, the observability server, background tasks.
//! With the default pool, the non-shard remainder is smeared over dozens of
//! arenas nobody reads, so "process total minus the shards" stops being a
//! number anyone can point at. `narenas:1` makes the layout exactly
//! `1 + num_shards`, which the server checks and reports at startup
//! (`server::shards::report_arena_binding`).
//!
//! Explicitly created arenas are unaffected: `arenas.create` always makes a new
//! one regardless of the automatic pool's size.
//!
//! # `dirty_decay_ms:10000,muzzy_decay_ms:0`
//!
//! PRD R13 rules out an active defragmenter, so how fast jemalloc hands freed
//! pages back to the OS *is* FrogDB's reclamation policy. Leaving that to
//! jemalloc's built-in defaults would leave the policy implicit — identical
//! today, and silently different the day a jemalloc bump changes them.
//!
//! `dirty_decay_ms:10000` keeps a freed page mapped and resident for ten
//! seconds before madvising it away. A database's free is usually followed by
//! another allocation of a similar size, so returning pages faster buys RSS back
//! at the cost of re-faulting them moments later; ten seconds is long enough to
//! absorb the churn of a delete-then-write burst and short enough that a real
//! drop in working set shows up in RSS while an operator is still looking at it.
//!
//! `muzzy_decay_ms:0` skips the muzzy stage: a page madvised away is unmapped
//! immediately rather than parked in a third state. The muzzy stage exists to
//! save a `mmap(2)` on platforms where the madvise is lazy; keeping it here
//! would mean an arena's pages sit in a state that costs address space, shows up
//! in neither `active` nor `dirty`, and needs a second timer to explain. One
//! decay deadline to reason about is worth more than the saved syscall.
//!
//! Both are the values every arena *inherits at creation*
//! ([`DEFAULT_DECAY`]); they are not frozen there. Each arena's live setting is
//! readable and writable at runtime through `DEBUG ARENA-DECAY`, which is how an
//! operator retunes a running server.

/// The option string, NUL-terminated for jemalloc to read as a C string.
///
/// The *definition of the symbol* lives in `main.rs`, not here: a symbol may be
/// defined only once in a linked image, and this crate's own test binary links
/// the library and itself, which makes a definition in the library a
/// duplicate-symbol link error. The binary is also where the jemalloc global
/// allocator is declared, so both halves of "this process's allocator" sit
/// together.
///
/// The symbol name there carries the `_rjem_` prefix `tikv-jemalloc-sys` builds
/// jemalloc with (it renames every public symbol so a vendored jemalloc cannot
/// collide with a system one). Overriding the unprefixed `malloc_conf` would
/// link fine and do nothing at all.
pub const MALLOC_CONF: &[u8; MALLOC_CONF_LEN] =
    b"narenas:1,dirty_decay_ms:10000,muzzy_decay_ms:0\0";

/// The length of [`MALLOC_CONF`], including its NUL.
///
/// `main.rs` has to name the array type when it defines the symbol, and a
/// mismatched length there would be a compile error a long way from the string
/// it is about — so the length is stated once, here, and imported there.
pub const MALLOC_CONF_LEN: usize = 48;

/// The decay timings every arena inherits from [`MALLOC_CONF`] at creation. See
/// the module docs for why these two numbers.
pub const DEFAULT_DECAY: frogdb_telemetry::jemalloc::ArenaDecay =
    frogdb_telemetry::jemalloc::ArenaDecay {
        dirty_ms: 10_000,
        muzzy_ms: 0,
    };

/// The option string this build asks jemalloc for.
pub fn requested() -> &'static str {
    std::str::from_utf8(&MALLOC_CONF[..MALLOC_CONF.len() - 1]).unwrap_or("")
}

/// Whether jemalloc actually applied [`requested`] — read back from `opt.*`, not
/// assumed from the fact that the symbol compiled.
///
/// `None` on a build with no jemalloc to ask.
pub fn applied() -> Option<bool> {
    let narenas = frogdb_telemetry::jemalloc::configured_narenas()?;
    let decay = frogdb_telemetry::jemalloc::configured_decay()?;
    Some(narenas == 1 && decay == DEFAULT_DECAY)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The option string must be NUL-terminated: jemalloc reads it as a C
    /// string, so a missing terminator is a read past the end of the constant.
    #[test]
    fn the_option_string_is_a_c_string() {
        assert_eq!(*MALLOC_CONF.last().unwrap(), 0);
        assert_eq!(
            requested(),
            "narenas:1,dirty_decay_ms:10000,muzzy_decay_ms:0"
        );
    }

    /// The decay constant and the option string are two spellings of the same
    /// policy; a change to one that misses the other would leave the server
    /// reporting a default it does not actually run with.
    #[test]
    fn the_decay_constant_matches_the_option_string() {
        for (option, value) in [
            ("dirty_decay_ms", DEFAULT_DECAY.dirty_ms),
            ("muzzy_decay_ms", DEFAULT_DECAY.muzzy_ms),
        ] {
            assert!(
                requested().contains(&format!("{option}:{value}")),
                "malloc_conf must ask for {option}:{value}, but asks for {}",
                requested()
            );
        }
    }

    /// [`applied`] answers about the *running* process, and a library test
    /// binary does not define the override symbol — so here it reports
    /// jemalloc's own defaults, not ours. What it must never do is claim success
    /// without asking the allocator, which is what this pins. The binary's own
    /// test asserts the override took effect where the symbol actually is
    /// (`main.rs`), and the startup report warns if the live arena count does
    /// not match the shard count.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn the_applied_options_are_read_back_from_the_allocator() {
        assert!(
            frogdb_telemetry::jemalloc::configured_narenas().is_some(),
            "opt.narenas must be readable wherever jemalloc is linked"
        );
        assert!(
            frogdb_telemetry::jemalloc::configured_decay().is_some(),
            "opt.*_decay_ms must be readable wherever jemalloc is linked"
        );
        let asked_for = frogdb_telemetry::jemalloc::configured_narenas().map(|n| n == 1)
            == Some(true)
            && frogdb_telemetry::jemalloc::configured_decay() == Some(DEFAULT_DECAY);
        assert_eq!(
            applied(),
            Some(asked_for),
            "applied() must report what jemalloc says, not what was asked for"
        );
    }
}
