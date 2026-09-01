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
pub const MALLOC_CONF: &[u8; 10] = b"narenas:1\0";

/// The option string this build asks jemalloc for.
pub fn requested() -> &'static str {
    std::str::from_utf8(&MALLOC_CONF[..MALLOC_CONF.len() - 1]).unwrap_or("")
}

/// Whether jemalloc actually applied [`requested`] — read back from
/// `opt.narenas`, not assumed from the fact that the symbol compiled.
///
/// `None` on a build with no jemalloc to ask.
pub fn applied() -> Option<bool> {
    frogdb_telemetry::jemalloc::configured_narenas().map(|narenas| narenas == 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The option string must be NUL-terminated: jemalloc reads it as a C
    /// string, so a missing terminator is a read past the end of the constant.
    #[test]
    fn the_option_string_is_a_c_string() {
        assert_eq!(*MALLOC_CONF.last().unwrap(), 0);
        assert_eq!(requested(), "narenas:1");
    }

    /// [`applied`] answers about the *running* process, and a library test
    /// binary does not define the override symbol — so here it reports
    /// jemalloc's own default arena count, not ours. What it must never do is
    /// claim success without asking the allocator, which is what this pins. The
    /// binary's own test asserts the override took effect where the symbol
    /// actually is (`main.rs`), and the startup report warns if the live arena
    /// count does not match the shard count.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn the_applied_arena_count_is_read_back_from_the_allocator() {
        assert!(
            frogdb_telemetry::jemalloc::configured_narenas().is_some(),
            "opt.narenas must be readable wherever jemalloc is linked"
        );
        assert_eq!(
            applied(),
            frogdb_telemetry::jemalloc::configured_narenas().map(|n| n == 1),
            "applied() must report what jemalloc says, not what was asked for"
        );
    }
}
