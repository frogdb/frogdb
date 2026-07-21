//! Compile-time identities for every CONFIG parameter.
//!
//! Each CONFIG parameter has a flat, payload-free identity enum variant, split
//! by *mutability* — the one fact that decides which server-side lifecycle
//! serves it:
//!
//! - [`MutableParamId`] — the 45 runtime-mutable parameters (31 real
//!   `ConfigParam` lifecycles + 14 Redis-compat no-ops). Served by the server's
//!   `build_typed_params`.
//! - [`ImmutableParamId`] — the 16 restart-required parameters. Served by the
//!   server's `build_param_registry`.
//!
//! # Why these are hand-written, not derived
//!
//! The metadata registry ([`crate::config_param_registry`]) *is* derived — from
//! `#[derive(ConfigParams)]` on the serde section structs plus the hand-written
//! [`crate::params::VIRTUAL_PARAMS`]. But an identity enum cannot be emitted the
//! same way: the parameters span **many** section structs plus 25 virtual params
//! that have no struct field at all, so a per-struct derive (which only sees one
//! struct at a time) can never emit a single unified enum. Rather than smear the
//! identity across 27 partial enums, the two enums are declared here as one
//! `param_id_enum!` table each — co-locating the variant, its wire name, and the
//! `ALL` roster in a single literal so they cannot drift from one another.
//!
//! The table is then pinned to the derived truth by the tests below: the enum
//! name set must equal the corresponding mutability partition of the derived
//! registry. So the enum stays hand-written but can never silently diverge from
//! the single-source registry — a missing/renamed/misplaced variant is a red
//! test, and (once the server builds each lifecycle list by an exhaustive `match`
//! over `ALL`) a missing lifecycle handler is a compile error.
//!
//! Identity here is deliberately payload-free (mirrors `core`'s `ServerWideOp` /
//! `ScatterGatherOp`): the enums carry no parse/apply/section/field data. The
//! server selects the heavy lifecycle behind an exhaustive match; this light
//! crate names only the identities.

/// Declare a flat, `Copy` CONFIG-parameter identity enum together with its
/// `ALL` roster and `name()` accessor, from a single `Variant => "wire-name"`
/// table.
///
/// Co-locating the three keeps them from drifting: you cannot add a variant
/// without giving it a name, and `ALL` is generated from the same list the
/// `match` in `name()` is. Identity only — no payloads.
macro_rules! param_id_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $( $variant:ident => $wire:literal ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        $vis enum $name {
            $( #[doc = $wire] $variant ),+
        }

        impl $name {
            /// Every identity, in declaration order. Iterated by the server to
            /// build each parameter's lifecycle through an exhaustive `match`.
            pub const ALL: &'static [$name] = &[ $( $name::$variant ),+ ];

            /// The Redis-style CONFIG parameter name for this identity.
            pub const fn name(self) -> &'static str {
                match self {
                    $( $name::$variant => $wire ),+
                }
            }
        }
    };
}

param_id_enum! {
    /// Identity of a runtime-mutable CONFIG parameter (CONFIG GET/SET).
    ///
    /// Membership in *this* enum (versus [`ImmutableParamId`]) is the single
    /// statement of a parameter's mutability: the server serves every variant
    /// here from its typed `build_typed_params` lifecycle registry. Includes the
    /// Redis-compat no-op params (they accept-and-ignore, but are still mutable
    /// at the protocol level). Order mirrors `build_typed_params` for review
    /// locality; it is not otherwise load-bearing (lookup is by name).
    pub enum MutableParamId {
        // === Memory / eviction family ===
        Maxmemory => "maxmemory",
        MaxmemoryPolicy => "maxmemory-policy",
        MaxmemorySamples => "maxmemory-samples",
        LfuLogFactor => "lfu-log-factor",
        LfuDecayTime => "lfu-decay-time",
        MaxmemoryClients => "maxmemory-clients",
        // === Logging family ===
        Loglevel => "loglevel",
        PerRequestSpans => "per-request-spans",
        // === Persistence family ===
        DurabilityMode => "durability-mode",
        WalFailurePolicy => "wal-failure-policy",
        SyncIntervalMs => "sync-interval-ms",
        BatchTimeoutMs => "batch-timeout-ms",
        // === Server family ===
        ScatterGatherTimeoutMs => "scatter-gather-timeout-ms",
        // === Replication family ===
        MinReplicasToWrite => "min-replicas-to-write",
        MinReplicasMaxLag => "min-replicas-max-lag",
        // === Slowlog family ===
        SlowlogLogSlowerThan => "slowlog-log-slower-than",
        SlowlogMaxLen => "slowlog-max-len",
        SlowlogMaxArgLen => "slowlog-max-arg-len",
        // === Encoding-threshold family (listpack atomics) ===
        SetMaxListpackEntries => "set-max-listpack-entries",
        SetMaxListpackValue => "set-max-listpack-value",
        HashMaxZiplistEntries => "hash-max-ziplist-entries",
        HashMaxZiplistValue => "hash-max-ziplist-value",
        HashMaxListpackEntries => "hash-max-listpack-entries",
        HashMaxListpackValue => "hash-max-listpack-value",
        // === Misc runtime family ===
        LuaTimeLimit => "lua-time-limit",
        Maxclients => "maxclients",
        LatencyTracking => "latency-tracking",
        LatencyTrackingInfoPercentiles => "latency-tracking-info-percentiles",
        NotifyKeyspaceEvents => "notify-keyspace-events",
        Requirepass => "requirepass",
        KeyMemoryHistograms => "key-memory-histograms",
        // === Redis-compatibility no-op parameters ===
        Save => "save",
        SetMaxIntsetEntries => "set-max-intset-entries",
        ListMaxListpackSize => "list-max-listpack-size",
        ListCompressDepth => "list-compress-depth",
        ListMaxZiplistSize => "list-max-ziplist-size",
        LatencyMonitorThreshold => "latency-monitor-threshold",
        BusyReplyThreshold => "busy-reply-threshold",
        Hz => "hz",
        Activedefrag => "activedefrag",
        CloseOnOom => "close-on-oom",
        ZsetMaxZiplistEntries => "zset-max-ziplist-entries",
        ZsetMaxZiplistValue => "zset-max-ziplist-value",
        ZsetMaxListpackEntries => "zset-max-listpack-entries",
        ZsetMaxListpackValue => "zset-max-listpack-value",
    }
}

param_id_enum! {
    /// Identity of an immutable, restart-required CONFIG parameter (CONFIG GET
    /// only).
    ///
    /// The server serves every variant here from its read-only
    /// `build_param_registry`. Order mirrors `build_param_registry`.
    pub enum ImmutableParamId {
        Bind => "bind",
        Port => "port",
        NumShards => "num-shards",
        Dir => "dir",
        PersistenceEnabled => "persistence-enabled",
        FlushCompactRange => "flush-compact-range",
        MetricsEnabled => "metrics-enabled",
        MetricsPort => "metrics-port",
        TlsPort => "tls-port",
        TlsCertFile => "tls-cert-file",
        TlsKeyFile => "tls-key-file",
        TlsCaCertFile => "tls-ca-cert-file",
        TlsAuthClients => "tls-auth-clients",
        TlsReplication => "tls-replication",
        TlsCluster => "tls-cluster",
        TlsProtocols => "tls-protocols",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_param_registry;
    use std::collections::HashSet;

    /// Sorted wire names of the registry rows matching `mutable == want_mutable`.
    fn registry_partition(want_mutable: bool) -> Vec<&'static str> {
        let mut names: Vec<&'static str> = config_param_registry()
            .iter()
            .filter(|p| p.mutable == want_mutable)
            .map(|p| p.name)
            .collect();
        names.sort_unstable();
        names
    }

    fn sorted<T: Copy + Ord>(mut v: Vec<T>) -> Vec<T> {
        v.sort_unstable();
        v
    }

    /// The mutable identity roster must equal exactly the `mutable: true`
    /// partition of the derived registry. This is what pins the hand-written
    /// enum to the single-source-of-truth registry: add/rename/misplace a
    /// mutable param and this fails.
    #[test]
    fn mutable_ids_match_registry_partition() {
        let enum_names = sorted(MutableParamId::ALL.iter().map(|id| id.name()).collect());
        assert_eq!(
            enum_names,
            registry_partition(true),
            "MutableParamId roster must equal the registry's mutable partition"
        );
    }

    /// The immutable identity roster must equal exactly the `mutable: false`
    /// partition of the derived registry.
    #[test]
    fn immutable_ids_match_registry_partition() {
        let enum_names = sorted(ImmutableParamId::ALL.iter().map(|id| id.name()).collect());
        assert_eq!(
            enum_names,
            registry_partition(false),
            "ImmutableParamId roster must equal the registry's immutable partition"
        );
    }

    /// Names are globally unique within and across the two enums (so the two
    /// rosters partition the registry rather than overlapping).
    #[test]
    fn ids_are_unique_and_disjoint() {
        let mut seen = HashSet::new();
        for name in MutableParamId::ALL
            .iter()
            .map(|id| id.name())
            .chain(ImmutableParamId::ALL.iter().map(|id| id.name()))
        {
            assert!(seen.insert(name), "duplicate CONFIG param identity: {name}");
        }
        // Every registry row is covered by exactly one identity.
        assert_eq!(seen.len(), config_param_registry().len());
    }

    /// Documents the current split so an accidental enum edit trips a test.
    #[test]
    fn id_counts_are_stable() {
        assert_eq!(MutableParamId::ALL.len(), 45);
        assert_eq!(ImmutableParamId::ALL.len(), 16);
    }
}
