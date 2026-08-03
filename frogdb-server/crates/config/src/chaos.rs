//! Chaos testing configuration (turmoil feature only).

use frogdb_config_derive::ConfigParams;
use serde::{Deserialize, Serialize};

/// Chaos testing configuration for latency and failure injection.
/// Only available when compiled with `turmoil` feature.
//
// No fields are exposed as CONFIG GET/SET parameters; each carries an explicit
// `#[param(skip)]` to satisfy the per-field coverage guarantee.
#[cfg(feature = "turmoil")]
#[derive(Debug, Clone, Default, Deserialize, Serialize, ConfigParams)]
#[params(section = "chaos")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct ChaosConfig {
    /// Delay (ms) between scatter sends to different shards.
    /// Useful for testing interleaving of concurrent operations.
    #[serde(default)]
    #[param(skip)]
    // skip: chaos latency injection; turmoil test feature only, never a production CONFIG param
    pub scatter_inter_send_delay_ms: u64,

    /// Per-shard latency overrides (shard_id -> delay_ms).
    /// Applied before sending to each shard.
    #[serde(default)]
    #[param(skip)] // skip: chaos latency injection; turmoil test feature only
    pub shard_delays_ms: std::collections::HashMap<usize, u64>,

    /// Random jitter range (0 to jitter_ms).
    /// Added on top of other delays for more realistic simulation.
    #[serde(default)]
    #[param(skip)] // skip: chaos latency injection; turmoil test feature only
    pub jitter_ms: u64,

    /// Delay (ms) before single-shard command execution.
    #[serde(default)]
    #[param(skip)] // skip: chaos latency injection; turmoil test feature only
    pub single_shard_delay_ms: u64,

    /// Delay (ms) before transaction EXEC processing.
    #[serde(default)]
    #[param(skip)] // skip: chaos latency injection; turmoil test feature only
    pub transaction_delay_ms: u64,

    // === Failure Injection Fields ===
    /// Shard IDs that simulate being unavailable (requests timeout).
    /// When a shard is in this set, scatter-gather operations to it will fail.
    #[serde(default)]
    #[param(skip)] // skip: chaos failure injection; turmoil test feature only
    pub unavailable_shards: std::collections::HashSet<usize>,

    /// Probability (0.0-1.0) of simulating connection reset during operations.
    /// Applied per-operation to simulate network instability.
    #[serde(default)]
    #[param(skip)] // skip: chaos failure injection; turmoil test feature only
    pub connection_reset_probability: f64,

    /// Shard IDs that return errors instead of successful responses.
    /// Maps shard_id -> error message to return.
    #[serde(default)]
    #[param(skip)] // skip: chaos failure injection; turmoil test feature only
    pub error_shards: std::collections::HashMap<usize, String>,

    /// Seed for every random draw the injector makes (jitter, connection resets).
    ///
    /// Set this to the simulation's seed. The injector used to draw from the thread-local
    /// entropy-seeded generator, so a seeded turmoil run with `jitter_ms` or
    /// `connection_reset_probability` set was not reproducible at all: the same seed produced a
    /// different delay and a different set of reset victims on every execution.
    #[serde(default)]
    #[param(skip)] // skip: chaos determinism seed; turmoil test feature only
    pub seed: u64,

    /// The injector's generator, created lazily from [`seed`](Self::seed) on first draw.
    ///
    /// Not serialized, and not part of the struct's value identity — it is the *stream*
    /// produced by `seed`, so deriving it lazily keeps `seed` the single source of truth
    /// however the config was built (TOML, `Default`, or a struct literal that sets `seed`
    /// after the fact).
    #[serde(skip)]
    #[param(skip)] // skip: derived runtime state, not a configurable value
    pub rng: ChaosRng,
}

/// The chaos injector's random stream: a lazily-seeded `SmallRng` shared by every clone of
/// the owning [`ChaosConfig`].
///
/// Shared rather than per-clone because the config is handed to the server as an `Arc` and
/// cloned into per-connection and per-shard paths; giving each clone its own generator would
/// hand identical draw sequences to every connection, which is a different distribution than
/// the one the injector is meant to model.
#[cfg(feature = "turmoil")]
#[derive(Clone, Default)]
pub struct ChaosRng(std::sync::Arc<std::sync::OnceLock<std::sync::Mutex<rand::rngs::SmallRng>>>);

#[cfg(feature = "turmoil")]
impl std::fmt::Debug for ChaosRng {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `SmallRng` is not `Debug`, and dumping generator state would be noise anyway;
        // whether the stream has been started is the only interesting bit.
        f.debug_tuple("ChaosRng")
            .field(&if self.0.get().is_some() {
                "started"
            } else {
                "unstarted"
            })
            .finish()
    }
}

#[cfg(feature = "turmoil")]
impl ChaosConfig {
    /// Run `f` against the injector's seeded generator.
    ///
    /// Every random draw in the chaos injector must go through here; a direct `rand::rng()`
    /// call reintroduces process entropy into a run that is supposed to be a pure function
    /// of its seed.
    pub fn with_rng<T>(&self, f: impl FnOnce(&mut rand::rngs::SmallRng) -> T) -> T {
        use rand::SeedableRng;
        let cell = self
            .rng
            .0
            .get_or_init(|| std::sync::Mutex::new(rand::rngs::SmallRng::seed_from_u64(self.seed)));
        // A panic inside a chaos draw must not poison every later draw into a panic of its
        // own: the generator has no invariant a panic could have broken mid-draw.
        let mut guard = cell.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut guard)
    }

    /// Set the seed, returning `self` — for builder-style construction.
    ///
    /// Only meaningful before the first draw; the generator is created once, on first use.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }
}

#[cfg(all(test, feature = "turmoil"))]
mod tests {
    use super::*;
    use rand::RngExt;

    fn draws(seed: u64) -> Vec<u64> {
        let config = ChaosConfig::default().with_seed(seed);
        (0..8)
            .map(|_| config.with_rng(|rng| rng.random_range(0..1_000_000u64)))
            .collect()
    }

    #[test]
    fn same_seed_produces_same_draws() {
        assert_eq!(draws(7), draws(7));
    }

    #[test]
    fn different_seeds_produce_different_draws() {
        assert_ne!(draws(7), draws(8));
    }

    /// Clones share one stream: the config is cloned into per-connection paths, and each
    /// clone continuing an independent copy of the same sequence would make every connection
    /// see identical "random" chaos.
    #[test]
    fn clones_share_one_stream() {
        let a = ChaosConfig::default().with_seed(3);
        let b = a.clone();
        let interleaved: Vec<u64> = (0..4)
            .flat_map(|_| {
                [
                    a.with_rng(|rng| rng.random_range(0..1_000_000u64)),
                    b.with_rng(|rng| rng.random_range(0..1_000_000u64)),
                ]
            })
            .collect();
        assert_eq!(interleaved, draws(3));
    }
}
