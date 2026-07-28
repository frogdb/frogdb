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
//! A failed reload is logged and dropped: the manager keeps serving the
//! previous certificates (see [`crate::tls_runtime`]), and the next change to
//! any watched file retries.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use frogdb_config::TlsConfig;
use tracing::{info, warn};

use crate::net::{JoinHandle, spawn};
use crate::tls_runtime::TlsRuntimeHandle;

/// Lower bound on the poll interval.
///
/// `watch-debounce-ms` is operator-settable and a zero (or near-zero) value
/// would turn the watcher into a busy loop stat-ing files.
const MIN_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// What a single watched file looked like at the last poll.
///
/// `None` means "absent or unreadable", which is a legitimate state to observe:
/// a rotation that replaces a file via rename briefly exposes it, and an
/// optional path (`ca-file`, the client pair) may simply not exist.
type Fingerprint = Option<(SystemTime, u64)>;

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

/// Stat every watched file.
///
/// The path set is recomputed from the handle's *current* config on each poll,
/// so a CONFIG SET that repoints `tls-cert-file` moves the watcher with it.
fn snapshot(config: &TlsConfig) -> BTreeMap<PathBuf, Fingerprint> {
    watched_paths(config)
        .into_iter()
        .map(|path| {
            let fingerprint = std::fs::metadata(&path).ok().map(|meta| {
                (
                    meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                    meta.len(),
                )
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

/// Poll the watched set forever, reloading once it goes quiet after a change.
async fn watch_loop(handle: Arc<TlsRuntimeHandle>, interval: Duration) {
    let mut previous = snapshot(&handle.current_config());
    // Set when a poll observes a difference; cleared by the reload that the
    // following quiet poll triggers.
    let mut pending = false;

    loop {
        tokio::time::sleep(interval).await;
        let current = snapshot(&handle.current_config());

        if current != previous {
            previous = current;
            pending = true;
            continue;
        }
        if !pending {
            continue;
        }
        pending = false;

        match handle.reload_current() {
            Ok(()) => info!("Reloaded TLS certificates after on-disk change"),
            Err(e) => warn!(
                error = %e,
                "TLS certificate reload failed; continuing with the previously loaded certificates"
            ),
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
