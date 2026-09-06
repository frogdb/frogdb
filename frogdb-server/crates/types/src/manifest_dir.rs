//! `CARGO_MANIFEST_DIR`, resolved against the *running* checkout.
//!
//! `env!("CARGO_MANIFEST_DIR")` is baked into the binary by `rustc` at compile
//! time. That is exactly what tests want when they need a path that does not
//! move with the CWD — golden fixtures, repro dumps, source sweeps — but it
//! stops being true the moment a `target/` built in one checkout is reused by
//! another. FrogDB does that deliberately: a worktree's `target/` is seeded
//! from another checkout's (see `.scratch/build-cache/README.md`), so a
//! fingerprint-fresh test binary can be *executed* from worktree B while
//! carrying worktree A's manifest dir inside it. It would then read golden
//! files from — and write repro files into — the wrong tree.
//!
//! [`resolve`] fixes that by rebasing the compile-time path onto the workspace
//! root of the checkout the process is actually running in. Call it through
//! [`crate::manifest_dir!`], never directly with a hand-written `env!`.

use std::path::{Path, PathBuf};

/// `frogdb-types`' own compile-time manifest dir. The one place in the crate
/// (besides the macro body) where the compile-time value is read: it is the
/// anchor from which the compile-time workspace root is derived.
const TYPES_MANIFEST_DIR: &str = env!("CARGO_MANIFEST_DIR");

/// `frogdb-types`' manifest lives at `<workspace-root>/frogdb-server/crates/types`,
/// so the workspace root is the third ancestor. Not directly under the root —
/// if the crate ever moves, this constant moves with it.
const TYPES_DEPTH_BELOW_ROOT: usize = 3;

/// Resolve a compile-time `CARGO_MANIFEST_DIR` against the running checkout.
///
/// `compiled` is the compile-time manifest dir of *some* workspace crate. When
/// the process is running out of the same checkout it was compiled in (or when
/// there is nothing to say otherwise), the path is returned unchanged.
/// Otherwise the compile-time workspace-root prefix is swapped for the running
/// checkout's workspace root.
///
/// The runtime side comes from the `CARGO_MANIFEST_DIR` environment variable,
/// which `cargo test`/`cargo run`/`nextest` set to the *executing package's*
/// manifest dir. That is frequently a different package than the one that
/// compiled the call site, so it is never substituted directly — only its
/// workspace root is used.
pub fn resolve(compiled: &str) -> PathBuf {
    let compiled_root = compiled_root();
    let runtime_root = std::env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .and_then(|dir| workspace_root_from(&dir, is_workspace_root));
    relocate(Path::new(compiled), compiled_root, runtime_root.as_deref())
}

/// The workspace root this crate was compiled from.
fn compiled_root() -> &'static Path {
    Path::new(TYPES_MANIFEST_DIR)
        .ancestors()
        .nth(TYPES_DEPTH_BELOW_ROOT)
        .expect("frogdb-types' manifest dir sits three levels below the workspace root")
}

/// True when `dir` holds a `Cargo.toml` declaring a `[workspace]` table.
fn is_workspace_root(dir: &Path) -> bool {
    match std::fs::read_to_string(dir.join("Cargo.toml")) {
        Ok(text) => text
            .lines()
            .any(|line| line.trim_start().starts_with("[workspace]")),
        Err(_) => false,
    }
}

/// Walk up from `start` to the first ancestor that is a workspace root.
pub(crate) fn workspace_root_from(
    start: &Path,
    is_workspace_root: impl Fn(&Path) -> bool,
) -> Option<PathBuf> {
    start
        .ancestors()
        .find(|dir| is_workspace_root(dir))
        .map(Path::to_path_buf)
}

/// Pure core of [`resolve`]: rebase `compiled` from `compiled_root` onto
/// `runtime_root`, or hand it back untouched when there is nothing to rebase.
pub(crate) fn relocate(
    compiled: &Path,
    compiled_root: &Path,
    runtime_root: Option<&Path>,
) -> PathBuf {
    let Some(runtime_root) = runtime_root else {
        return compiled.to_path_buf();
    };
    if runtime_root == compiled_root {
        return compiled.to_path_buf();
    }
    match compiled.strip_prefix(compiled_root) {
        Ok(suffix) => runtime_root.join(suffix),
        // Not a path this workspace's layout explains — leave it alone.
        Err(_) => compiled.to_path_buf(),
    }
}

/// The caller's `CARGO_MANIFEST_DIR`, resolved against the running checkout.
///
/// Expands at the *call site*, so the `env!` picks up the calling crate's
/// manifest dir, then hands it to [`manifest_dir::resolve`](resolve). Use this
/// instead of a bare `env!("CARGO_MANIFEST_DIR")`: a `target/` seeded from
/// another checkout (`.scratch/build-cache/README.md`) makes the compile-time
/// value point at the wrong tree. Enforced by `just lint-manifest-dir`.
#[macro_export]
macro_rules! manifest_dir {
    () => {
        $crate::manifest_dir::resolve(env!("CARGO_MANIFEST_DIR"))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMPILED_ROOT: &str = "/checkouts/main";
    const COMPILED: &str = "/checkouts/main/frogdb-server/crates/cluster";
    const RUNTIME_ROOT: &str = "/checkouts/worktree-a";

    fn relocated(runtime_root: Option<&str>) -> PathBuf {
        relocate(
            Path::new(COMPILED),
            Path::new(COMPILED_ROOT),
            runtime_root.map(Path::new),
        )
    }

    #[test]
    fn unset_runtime_env_leaves_the_path_alone() {
        assert_eq!(relocated(None), PathBuf::from(COMPILED));
    }

    #[test]
    fn same_root_leaves_the_path_alone() {
        assert_eq!(relocated(Some(COMPILED_ROOT)), PathBuf::from(COMPILED));
    }

    #[test]
    fn different_root_rebases_onto_the_running_checkout() {
        assert_eq!(
            relocated(Some(RUNTIME_ROOT)),
            PathBuf::from("/checkouts/worktree-a/frogdb-server/crates/cluster")
        );
    }

    #[test]
    fn path_outside_the_compiled_root_is_left_alone() {
        let outside = Path::new("/elsewhere/frogdb-server/crates/cluster");
        assert_eq!(
            relocate(
                outside,
                Path::new(COMPILED_ROOT),
                Some(Path::new(RUNTIME_ROOT))
            ),
            outside.to_path_buf()
        );
    }

    #[test]
    fn runtime_manifest_dir_of_a_different_package_still_relocates() {
        // The binary was compiled in main's `cluster` crate but is executed by
        // a runner whose CARGO_MANIFEST_DIR names worktree-a's `server` crate —
        // a different package. Only the workspace root is taken from it.
        let runtime_manifest_dir = Path::new("/checkouts/worktree-a/frogdb-server/crates/server");
        let runtime_root =
            workspace_root_from(runtime_manifest_dir, |dir| dir == Path::new(RUNTIME_ROOT))
                .expect("walk finds the synthetic workspace root");
        assert_eq!(runtime_root, PathBuf::from(RUNTIME_ROOT));
        assert_eq!(
            relocate(
                Path::new(COMPILED),
                Path::new(COMPILED_ROOT),
                Some(&runtime_root)
            ),
            PathBuf::from("/checkouts/worktree-a/frogdb-server/crates/cluster")
        );
    }

    #[test]
    fn workspace_root_walk_stops_at_the_first_match() {
        // Both `/a` and `/a/b` qualify; the nearest ancestor wins.
        let root = workspace_root_from(Path::new("/a/b/c"), |dir| {
            dir == Path::new("/a") || dir == Path::new("/a/b")
        });
        assert_eq!(root, Some(PathBuf::from("/a/b")));
    }

    #[test]
    fn workspace_root_walk_can_find_nothing() {
        assert_eq!(workspace_root_from(Path::new("/a/b/c"), |_| false), None);
    }

    #[test]
    fn compiled_root_is_the_real_workspace_root() {
        // Guards TYPES_DEPTH_BELOW_ROOT against a crate move.
        assert!(is_workspace_root(compiled_root()));
        assert_eq!(
            compiled_root().join("frogdb-server/crates/types"),
            Path::new(TYPES_MANIFEST_DIR)
        );
    }

    #[test]
    fn resolve_returns_this_checkout_for_this_crate() {
        // Whatever checkout the test runs in, the path must exist here.
        let dir = resolve(TYPES_MANIFEST_DIR);
        assert!(
            dir.join("Cargo.toml").is_file(),
            "{dir:?} has no Cargo.toml"
        );
    }
}
