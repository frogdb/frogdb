//! Certificate file watcher.
//!
//! Operators rotate certificates by overwriting the PEM files under the
//! configured paths (cert-manager, certbot, a Kubernetes secret remount, ...).
//! Without a watcher the server keeps serving the certificates it loaded at
//! startup until it is restarted. When `tls.watch-certs` is set, this module
//! polls every file the TLS configuration references and asks
//! [`TlsRuntimeHandle::reload_current`] to re-read them once they settle.
//!
//! **Polling, not inotify.** The workspace has no filesystem-notification
//! dependency, and adding one buys little here: the watched set is a handful of
//! files, the acceptable reaction latency is seconds, and polling handles the
//! cases inotify is bad at anyway (bind-mounted secrets that are replaced by
//! renaming a directory, paths that do not exist yet at startup).
//!
//! **Debounce = one quiet poll.** A rotation is rarely atomic: the key and the
//! certificate are written as two separate files, so a reload fired between the
//! two writes sees a mismatched pair and fails. The watcher therefore requires
//! the fingerprint of the watched set to be *unchanged* for one full interval
//! before reloading, which collapses a burst of writes into a single reload.
//! Reaction latency is consequently between one and two `watch-debounce-ms`
//! intervals.
//!
//! A failed reload leaves the manager serving the previous certificates (see
//! [`crate::tls_runtime`]) and is **retried every interval until it succeeds**.
//! Retrying matters because the usual failure is a torn rotation: the reload
//! that raced the second file's write fails, and if the watcher then waited for
//! another change it would serve the stale certificate until the *next*
//! rotation, potentially past expiry.
//!
//! All file IO happens on the blocking pool: stat-ing, reading and PEM-parsing
//! certificates are synchronous, and doing them on a runtime worker would stall
//! every other task sharing that thread.

use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use frogdb_config::TlsConfig;
use tracing::{debug, info, warn};

use crate::net::{JoinHandle, spawn};
use crate::tls_runtime::TlsRuntimeHandle;

/// Lower bound on the poll interval.
///
/// `watch-debounce-ms` is operator-settable and a zero (or near-zero) value
/// would turn the watcher into a busy loop stat-ing files.
const MIN_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// What a single watched file looked like at the last poll: its length and a
/// hash of its contents.
///
/// `None` means "absent or unreadable", which is a legitimate state to observe:
/// a rotation that replaces a file via rename briefly exposes it, and an
/// optional path (`ca-file`, the client pair) may simply not exist.
///
/// Metadata alone is not enough. `(mtime, len)` misses a rotation that preserves
/// the modification time (`cp -p`, `rsync --times`, a restored backup, a
/// mount-time-quantised filesystem) and writes a same-length certificate — two
/// PEM files from the same issuer and key type are routinely byte-identical in
/// length. That rotation would then never be picked up. The watched set is a
/// handful of small PEM files polled every few hundred milliseconds, so reading
/// them is cheaper than the risk of missing a rotation.
type Fingerprint = Option<(u64, u64)>;

/// Every file the TLS configuration reads, deduplicated and ordered so two
/// snapshots are directly comparable.
fn watched_paths(config: &TlsConfig) -> Vec<PathBuf> {
    let mut paths = vec![config.cert_file.clone(), config.key_file.clone()];
    paths.extend(config.ca_file.clone());
    paths.extend(config.client_cert_file.clone());
    paths.extend(config.client_key_file.clone());
    for extra in &config.additional_certs {
        paths.push(extra.cert_file.clone());
        paths.push(extra.key_file.clone());
    }
    paths.sort();
    paths.dedup();
    paths
}

/// Fingerprint every watched file by content.
///
/// The path set is recomputed from the handle's *current* config on each poll,
/// so a CONFIG SET that repoints `tls-cert-file` moves the watcher with it.
///
/// Synchronous: callers run it on the blocking pool.
fn snapshot(config: &TlsConfig) -> BTreeMap<PathBuf, Fingerprint> {
    watched_paths(config)
        .into_iter()
        .map(|path| {
            let fingerprint = std::fs::read(&path).ok().map(|bytes| {
                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                (bytes.len() as u64, hasher.finish())
            });
            (path, fingerprint)
        })
        .collect()
}

/// Spawn the certificate watcher for `handle`, unless `tls.watch-certs` is off.
///
/// The returned task runs until aborted; the caller owns its lifetime and must
/// abort it on shutdown.
pub fn spawn_cert_watcher(handle: Arc<TlsRuntimeHandle>) -> Option<JoinHandle<()>> {
    let config = handle.current_config();
    if !config.enabled || !config.watch_certs {
        return None;
    }
    let interval = Duration::from_millis(config.watch_debounce_ms).max(MIN_POLL_INTERVAL);
    info!(
        interval_ms = interval.as_millis() as u64,
        files = watched_paths(&config).len(),
        "TLS certificate watcher started"
    );
    Some(spawn(watch_loop(handle, interval)))
}

/// Whether a poll that observed no change should still attempt a reload.
///
/// `pending` is the debounced "something changed" edge. `failures` keeps the
/// retry alive after a failed attempt even with nothing changing on disk: a
/// reload can fail for reasons outside the watched set (a transient IO error, a
/// descriptor limit), and without this the watcher would sit on stale
/// certificates until the next rotation.
fn should_attempt_reload(pending: bool, failures: u64) -> bool {
    pending || failures > 0
}

/// Run `f` on the blocking pool, or return `None` if it panicked.
///
/// A panicked poll or reload must not kill the watcher: the next interval tries
/// again.
async fn on_blocking_pool<T, F>(f: F) -> Option<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(value) => Some(value),
        Err(e) => {
            warn!(error = %e, "TLS certificate watcher: blocking task failed");
            None
        }
    }
}

/// Poll the watched set forever, reloading once it goes quiet after a change.
async fn watch_loop(handle: Arc<TlsRuntimeHandle>, interval: Duration) {
    let poll = {
        let handle = handle.clone();
        move || snapshot(&handle.current_config())
    };
    let mut previous = on_blocking_pool(poll.clone()).await.unwrap_or_default();
    // Set when a poll observes a difference; cleared by the reload that the
    // following quiet poll triggers.
    let mut pending = false;
    // How many consecutive reload attempts have failed. Non-zero keeps the
    // retry going without waiting for another on-disk change, and keeps the log
    // to one warning per failure episode rather than one per interval.
    let mut failures: u64 = 0;

    loop {
        tokio::time::sleep(interval).await;
        let Some(current) = on_blocking_pool(poll.clone()).await else {
            continue;
        };

        if current != previous {
            previous = current;
            pending = true;
            continue;
        }
        if !should_attempt_reload(pending, failures) {
            continue;
        }
        pending = false;

        let handle_for_reload = handle.clone();
        let Some(result) = on_blocking_pool(move || {
            handle_for_reload
                .reload_current()
                .map_err(|e| e.to_string())
        })
        .await
        else {
            continue;
        };

        match result {
            Ok(()) => {
                if failures > 0 {
                    info!(
                        attempts = failures + 1,
                        "Reloaded TLS certificates after earlier failures"
                    );
                } else {
                    info!("Reloaded TLS certificates after on-disk change");
                }
                failures = 0;
            }
            Err(e) => {
                failures += 1;
                if failures == 1 {
                    warn!(
                        error = %e,
                        "TLS certificate reload failed; continuing with the previously loaded \
                         certificates and retrying every poll interval"
                    );
                } else {
                    debug!(error = %e, attempts = failures, "TLS certificate reload still failing");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls_runtime::test_support::{
        handshake_leaf, leaf_der, rotate_in_place, write_identity,
    };

    /// Poll interval short enough that a rotation lands well inside the test's
    /// deadline, but long enough to still exercise the debounce.
    const TEST_DEBOUNCE_MS: u64 = 20;

    fn config_in(dir: &std::path::Path, watch: bool) -> (TlsConfig, PathBuf, PathBuf) {
        let (cert, key) = write_identity(dir, "server");
        let config = TlsConfig {
            enabled: true,
            cert_file: cert.clone(),
            key_file: key.clone(),
            watch_certs: watch,
            watch_debounce_ms: TEST_DEBOUNCE_MS,
            ..Default::default()
        };
        (config, cert, key)
    }

    /// Wait until the server serves `expected`, or fail at the deadline.
    async fn await_served(handle: &TlsRuntimeHandle, expected: &[u8], what: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if handshake_leaf(handle).await == expected {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for the server to serve {what}"
            );
            tokio::time::sleep(Duration::from_millis(TEST_DEBOUNCE_MS)).await;
        }
    }

    #[test]
    fn watched_set_covers_every_configured_file() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_identity(dir.path(), "server");
        let (client_cert, client_key) = write_identity(dir.path(), "client");
        let (extra_cert, extra_key) = write_identity(dir.path(), "extra");
        let config = TlsConfig {
            enabled: true,
            cert_file: cert.clone(),
            key_file: key.clone(),
            ca_file: Some(cert.clone()),
            client_cert_file: Some(client_cert.clone()),
            client_key_file: Some(client_key.clone()),
            additional_certs: vec![frogdb_config::AdditionalCert {
                cert_file: extra_cert.clone(),
                key_file: extra_key.clone(),
            }],
            ..Default::default()
        };

        let watched = watched_paths(&config);
        for path in [
            &cert,
            &key,
            &client_cert,
            &client_key,
            &extra_cert,
            &extra_key,
        ] {
            assert!(watched.contains(path), "{path:?} is not watched");
        }
        // `ca_file` aliases `cert_file` here, so the set must be deduplicated.
        assert_eq!(watched.len(), 6);
    }

    #[test]
    fn a_failed_attempt_keeps_retrying_without_a_new_change() {
        // Nothing changed and nothing failed: idle.
        assert!(!should_attempt_reload(false, 0));
        // Debounced change.
        assert!(should_attempt_reload(true, 0));
        // Still failing, no new change: retry anyway.
        assert!(should_attempt_reload(false, 1));
        assert!(should_attempt_reload(false, 42));
    }

    #[test]
    fn fingerprint_catches_a_same_length_mtime_preserving_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cert.pem");
        std::fs::write(&path, b"first-generation").unwrap();
        let before_meta = std::fs::metadata(&path).unwrap();
        let times = std::fs::FileTimes::new()
            .set_accessed(before_meta.accessed().unwrap())
            .set_modified(before_meta.modified().unwrap());

        let config = TlsConfig {
            enabled: true,
            cert_file: path.clone(),
            key_file: path.clone(),
            ..Default::default()
        };
        let before = snapshot(&config);

        // Same byte length, different content, original mtime restored — what
        // `cp -p`, `rsync --times` or a restored backup produces.
        std::fs::write(&path, b"secnd-generation").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(times)
            .unwrap();

        // The premise: metadata alone is now indistinguishable.
        let after_meta = std::fs::metadata(&path).unwrap();
        assert_eq!(after_meta.len(), before_meta.len());
        assert_eq!(
            after_meta.modified().unwrap(),
            before_meta.modified().unwrap(),
            "test could not preserve mtime, so it proves nothing"
        );

        assert_ne!(
            before,
            snapshot(&config),
            "a content-preserving fingerprint would miss this rotation"
        );
    }

    #[tokio::test]
    async fn watcher_is_not_spawned_when_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let (config, _, _) = config_in(dir.path(), false);
        let handle = Arc::new(TlsRuntimeHandle::new(&config).unwrap());
        assert!(spawn_cert_watcher(handle).is_none());
    }

    #[tokio::test]
    async fn rotation_in_place_is_picked_up_without_a_config_change() {
        let dir = tempfile::tempdir().unwrap();
        let (config, cert, key) = config_in(dir.path(), true);
        let handle = Arc::new(TlsRuntimeHandle::new(&config).unwrap());
        let watcher = spawn_cert_watcher(handle.clone()).expect("watcher spawns when enabled");

        assert_eq!(handshake_leaf(&handle).await, leaf_der(&cert));
        let rotated = rotate_in_place(&cert, &key);
        await_served(&handle, &rotated, "the rotated certificate").await;

        watcher.abort();
    }

    #[tokio::test]
    async fn broken_rotation_keeps_the_old_certificate_serving() {
        let dir = tempfile::tempdir().unwrap();
        let (config, cert, key) = config_in(dir.path(), true);
        let original = leaf_der(&cert);
        let handle = Arc::new(TlsRuntimeHandle::new(&config).unwrap());
        let watcher = spawn_cert_watcher(handle.clone()).expect("watcher spawns when enabled");

        // Truncating the key makes every reload attempt fail.
        std::fs::write(&key, b"-----BEGIN PRIVATE KEY-----\ngarbage\n").unwrap();
        // Give the watcher several intervals to observe the change, attempt the
        // reload and fail it.
        tokio::time::sleep(Duration::from_millis(TEST_DEBOUNCE_MS * 10)).await;
        assert_eq!(
            handshake_leaf(&handle).await,
            original,
            "a failed reload must leave the previous certificate serving"
        );

        // A subsequent good rotation still recovers: the failure is not sticky.
        let rotated = rotate_in_place(&cert, &key);
        await_served(&handle, &rotated, "the repaired certificate").await;

        watcher.abort();
    }
}
