# Operator generates frogdb.toml through the server's own config crate

The Kubernetes operator must emit a `frogdb.toml` the server binary accepts, and config schemas
drift silently when duplicated. We decided the operator imports `frogdb-config` (kept
deliberately light — no RocksDB/mlua/tantivy deps) and serializes through the server's own
serde types rather than maintaining a parallel schema. Any server-side config rename or
addition becomes a compile error in the operator instead of a runtime deployment failure.
