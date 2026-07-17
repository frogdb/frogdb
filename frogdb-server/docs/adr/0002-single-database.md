# Single database only (index 0)

Redis supports 16 numbered databases via SELECT; Redis Cluster itself only supports database 0,
and multi-DB complicates every subsystem (keyspace notifications, replication, persistence,
cluster routing) for a feature that is widely discouraged. We decided FrogDB supports only
database 0: `SELECT 0` is a no-op OK, any other index errors, and `MOVE`/`SWAPDB` are
unsupported stubs. Namespacing belongs in key prefixes or separate deployments.
