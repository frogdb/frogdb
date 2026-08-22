# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## Unreleased

### Breaking Changes

* **`WAIT` now blocks when the requested replica count is unreachable.** Previously a node whose
  role was `standalone` answered `WAIT <n> <timeout>` with `0` immediately, and a node in cluster
  mode answered `0` unconditionally. Both shortcuts are gone: `WAIT` counts the replicas attached to
  the node that received it and blocks until the count is reached or the timeout expires, in every
  mode. This is Redis/Valkey parity — a replica may be mid-attach, so an unreachable count is a
  reason to wait, not to give up. Clients that used `WAIT 1 5000` as a cheap no-op on a
  replica-less node will now block for the full timeout; pass `timeout` values you are willing to
  wait for.
* **`WAIT` can now return an error.** A `WAIT` parked across a demotion is released with
  `-UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)`
  instead of an acknowledgment count (Redis's `disconnectAllBlockedClients` behavior).

### Features

* Cluster-mode data replication is served by the same PSYNC plane as standalone replication, and a
  Raft-driven promotion or demotion now reaches the data path — a promoted cluster node accepts
  writes and serves `PSYNC` to its own replicas. `CLUSTER SHARDS` reports the real data-plane
  replication offset instead of the Raft last-applied index.

## [0.1.1](https://github.com/frogdb/frogdb/compare/v0.1.0...v0.1.1) (2026-08-22)


### Features

* **cluster:** hold the replica feed while a slot-handoff barrier is armed ([8d55cc4](https://github.com/frogdb/frogdb/commit/8d55cc4f26c81fd55296cdb688ec6da782d35baf))
* **cluster:** invariant catalog with self-checking state-machine hooks ([1c70cf4](https://github.com/frogdb/frogdb/commit/1c70cf481909e13ccafbabbb372dbd2ae9a08511))
* **cluster:** make the promotion staleness bound live, and document selection ([8251e3c](https://github.com/frogdb/frogdb/commit/8251e3c9ab5f781b6c031c796d17326b35def242))
* **cluster:** rank offset-unknown candidates last and filter departing replicas under handoff ([d3fbac7](https://github.com/frogdb/frogdb/commit/d3fbac7047e4e5d7a4071e012eb277aefe308205))
* **commands:** align noscript and stale flags with upstream ([d228abe](https://github.com/frogdb/frogdb/commit/d228abef8f87d7bd3157f3c0c4606222ae982cc5))
* **commands:** full key specs, argument docs and audited tips in COMMAND INFO/DOCS ([7c19b16](https://github.com/frogdb/frogdb/commit/7c19b16b19f1657718975eaebbcc2b8b6c0f10fa))
* **commands:** gate OOM write rejection on a DENYOOM flag ([aabc663](https://github.com/frogdb/frogdb/commit/aabc6632d14a0a1a3b836540c2ceed096c5cf941))
* **commands:** nested subcommand entries in COMMAND INFO/DOCS ([ba7aa08](https://github.com/frogdb/frogdb/commit/ba7aa08b99af825ba154dd328e536660650204c9))
* **commands:** per-subcommand key specs, arity and flags for container commands ([69e23dd](https://github.com/frogdb/frogdb/commit/69e23ddd09a0b5860f5d6960c7b6cd805793712c))
* **commands:** vendor upstream acl_categories alongside command metadata ([b35cdd5](https://github.com/frogdb/frogdb/commit/b35cdd57382e251eb3577e999e5a479303f6dbb3))
* **commands:** vendor upstream command metadata and cross-check it against our specs ([02e776b](https://github.com/frogdb/frogdb/commit/02e776b38bed56a30bde575ea7fd63b29877d28b))
* **commands:** vendor upstream key specs, arguments, history and tips ([386a67d](https://github.com/frogdb/frogdb/commit/386a67de0c4a9c3722e78bbbbc4b2b04359bb6fb))
* **commands:** vendor upstream subcommand rows for container commands ([7721463](https://github.com/frogdb/frogdb/commit/77214635a806421e9ff0836bd229e8dd8c0bb56c))
* **compat:** bump Redis compatibility target to 8.6.1 ([019fb0f](https://github.com/frogdb/frogdb/commit/019fb0fd0122de0d81884c490985110a6872cf05))
* **compat:** redis-feel wave 1 — real introspection, truthful shims, 8.6.0, cmd-full ship paths ([2f71b94](https://github.com/frogdb/frogdb/commit/2f71b949dabc053b5a3248ff8e8ba4d119d6f7b2))
* **compat:** redis-feel wave 2 — COMMAND DOCS from compile-enforced per-command docs (issue 03) ([81103ae](https://github.com/frogdb/frogdb/commit/81103aebae8c56fc8bd8f87891bc4d52defdfb9e))
* **core:** admit a script's writes at the shard write seam ([e3e7c81](https://github.com/frogdb/frogdb/commit/e3e7c81a6d2b06e2883dbe1937c72eaba9a7c580))
* **core:** enforce the noscript admission gate ([785fd51](https://github.com/frogdb/frogdb/commit/785fd51a65fe2a08c4bdb9bad996732940538b6f))
* **core:** per-shard coverage watermark Y_s and the full-sync drain ack ([3c19405](https://github.com/frogdb/frogdb/commit/3c194053dd2aaeef85518bff31d6d19b050cb81c))
* **debug:** DEBUG REPLICATION CHECK — live replication invariant surface ([8dca1c7](https://github.com/frogdb/frogdb/commit/8dca1c7436ea5f8c0745406c646533b358166d29))
* **docs:** generate the website Specifications section from specs/ ([b9a4c42](https://github.com/frogdb/frogdb/commit/b9a4c42f02ad675868d83ba0d2cbf771b477d790))
* **jepsen:** wire DEBUG CLUSTER CHECK invariant sweep into raft workloads ([e8d191f](https://github.com/frogdb/frogdb/commit/e8d191f5fa548b10a22a3d7757a9ccb51d24259d))
* **lint:** parse TR/LV/CO rows and reject dangling spec references ([4d267f3](https://github.com/frogdb/frogdb/commit/4d267f303f59002e908f82621b58489105258f6f))
* **lint:** require forcing tests on liveness rows ([320fc04](https://github.com/frogdb/frogdb/commit/320fc045471505c4e21ef4cbe07a351681cffb73))
* **lint:** resolve spec ids cited in quint model headers ([24f6bf2](https://github.com/frogdb/frogdb/commit/24f6bf2ce38de22921972383cc82d2cddcdf3dbc))
* **persistence:** FlushHold — pin the WAL flush engine across a full-sync cut ([6e26ca0](https://github.com/frogdb/frogdb/commit/6e26ca0bad7a5f9c6a981cb72e2080c2257a9015))
* **persistence:** move staging and backup inside the data directory (spec-gaps 21) ([05601b0](https://github.com/frogdb/frogdb/commit/05601b0553db7a699d2a36e4c082fdc10d75a9bc))
* **persistence:** name backups from a persisted monotone counter (spec-gaps 27) ([6768863](https://github.com/frogdb/frogdb/commit/676886318d16ac3d04df16261bb7c11ed117b735))
* **persistence:** verify a staged checkpoint before installing it (spec-gaps 04) ([6a08636](https://github.com/frogdb/frogdb/commit/6a0863687f9d44f486469eccc1732f534ae879f4))
* **quint:** fail quint-run on a witness observed in 0 traces ([b31b383](https://github.com/frogdb/frogdb/commit/b31b383f81a9046e7cd50b42805712956c83788f))
* **replication:** carry the per-shard coverage vector from the drain to the trailer ([84fd115](https://github.com/frogdb/frogdb/commit/84fd1154879c24d536636dc24f4fa0c774316cb5))
* **replication:** ignore frames at or below the replica's applied head ([a05e5f6](https://github.com/frogdb/frogdb/commit/a05e5f661cd4900f6c6ba76dd3e8f5571df04b3f))
* **replication:** per-shard coverage vector in the full-sync trailer ([41becd8](https://github.com/frogdb/frogdb/commit/41becd88a0692de11e1bcc6461c5e2287083156c))
* **replication:** per-shard skip floors on the replica ([e0be6a9](https://github.com/frogdb/frogdb/commit/e0be6a9048a4a4d39ba671c776fd562a9db440fd))
* **replication:** persist the coverage vector with the offset it qualifies ([a318b9d](https://github.com/frogdb/frogdb/commit/a318b9de6a4d8576791460d6a1937d1ee992af07))
* **replication:** refuse window grants over an unaccounted tail ([3f0ffdc](https://github.com/frogdb/frogdb/commit/3f0ffdc90f092944704427dad9e6a0055e1a99f4))
* **replication:** ReplicationView projection + invariant catalog + seam hooks ([00118f9](https://github.com/frogdb/frogdb/commit/00118f9235a0ee62922e7cbc8115bb03ec8fbfb9))
* **replication:** stateright model of the promotion/resume composite ([088201d](https://github.com/frogdb/frogdb/commit/088201d9bde2319c208aff1cec38abec7dff3eaa))
* **server:** refuse stale reads on a link-down replica by default ([df4c2f4](https://github.com/frogdb/frogdb/commit/df4c2f4f43011ac4ce4e4aa939d2b40edef98f7a))
* **server:** wire DEBUG CLUSTER CHECK executor + docs ([0c8d759](https://github.com/frogdb/frogdb/commit/0c8d759d7ffdd7a8331960f34036d7aeaf23ed62))
* **vll:** wound-wait, revocation and AMBIGUOUS gather for the continuation lock ([f33806f](https://github.com/frogdb/frogdb/commit/f33806f063401a0dda7ca9ee33689601ce5dce0d))


### Bug Fixes

* **acl:** correct container-subcommand table to match real command surface ([f95a697](https://github.com/frogdb/frogdb/commit/f95a69757e22570f1ef5a4fcba62ec34c3d5a473))
* **acl:** fill command category table from vendored upstream data with parity gate ([d477636](https://github.com/frogdb/frogdb/commit/d47763684a3dee36fa754594ab52654e53813ed0))
* **bench:** add flush_hold_handle to WalConfig initializers in persistence bench ([b0be259](https://github.com/frogdb/frogdb/commit/b0be2598a485dab7891c4c7fbab0cf92d6eb631b))
* **blocking:** deny-blocking context replies op-aware nil, not scalar ([cb81d19](https://github.com/frogdb/frogdb/commit/cb81d19dfb5b3908b4aaa488b86261e58488a8a1))
* **blocking:** drain plain XREAD waiters when their key turns wrong-typed ([8f70949](https://github.com/frogdb/frogdb/commit/8f70949ad25a033d1dc5a9a93e0e23993cfe2b3d))
* **blocking:** order the slot-migration drain by registration ordinal ([d5eda74](https://github.com/frogdb/frogdb/commit/d5eda74b9acd6e78d1456d499065cf66689762dc))
* **blocking:** re-park a waiter the waking write has nothing to give it ([d0f7975](https://github.com/frogdb/frogdb/commit/d0f797504a5f2360b08e68c7169679ea6a04e45d))
* **blocking:** stop signaling by dropping the wake token; row the topology cases ([8822e82](https://github.com/frogdb/frogdb/commit/8822e82e7a8a98f83f0d35f3c656411e1ace3c29))
* **blocking:** supervise a parked wait so it can see EOF and CLIENT KILL ([f73ab95](https://github.com/frogdb/frogdb/commit/f73ab95f5989527648b5c88e740dcf215772258a))
* **build:** stop quint-verify-model reporting a proven invariant as a failure ([59d2cab](https://github.com/frogdb/frogdb/commit/59d2cabc1e075b0c0774996ec788f3cd3425835e))
* **ci:** derive Quint invariant lists, right-size verify depth, split timeout from violation ([5ca27bb](https://github.com/frogdb/frogdb/commit/5ca27bb895e442dfd2f373eed18279769bdf6d94))
* **ci:** install quint in CI, un-gate the always-red quarantine job, fix quint-verify-model's tool/N1/rc handling ([07b750e](https://github.com/frogdb/frogdb/commit/07b750e53a524204017b540e61f688db2f15fb76))
* **clock:** measure elapsed time through the clock seam, not Instant::elapsed ([c62da70](https://github.com/frogdb/frogdb/commit/c62da703cee520af636a155f885085ba63e1d08b))
* **cluster-runtime:** clamp failure-detector config to safe bounds ([e183dfa](https://github.com/frogdb/frogdb/commit/e183dfaa8e978430fc4d97420d74f9f8f7b8a473))
* **cluster-runtime:** name the confirm-sink log so clippy stops at the tuple ([2757a72](https://github.com/frogdb/frogdb/commit/2757a72cab640e5ac1be3c5afe6352363bf47e26))
* **cluster:** address quint-conformance review findings C1/C2/I3-I7 ([630b318](https://github.com/frogdb/frogdb/commit/630b31831f7c7764ecbe69ae171a23f2d946f75e))
* **cluster:** carry handoff_seq through the ClusterSnapshot restore vehicle ([5ddeeb2](https://github.com/frogdb/frogdb/commit/5ddeeb248df07e6b6bce2273485453970a4a3cc0))
* **cluster:** clear rustc 1.92 clippy lints in quint_conformance test ([569237c](https://github.com/frogdb/frogdb/commit/569237c83e2e67b8992beb8abdc4b3ae66a69424))
* **cluster:** durable Raft vote and a log cache readers share ([4cb1c4b](https://github.com/frogdb/frogdb/commit/4cb1c4b42f89008138d89b9b374a216681ca63c8))
* **cluster:** observe the inbound link weakly from the cluster bus ([6568159](https://github.com/frogdb/frogdb/commit/6568159bf2a08c9eb0dd76d561d2f57c3fd9f1c9))
* **cluster:** RemoveNode prunes orphaned migrations and replicas, guard Complete insert ([aea7ca4](https://github.com/frogdb/frogdb/commit/aea7ca45c890dbfa3efee999c6de4b9a7b0913ae))
* **cluster:** shrink the Raft voter set on forced failover, spec FM-CLUSTER-101 ([7f0c04d](https://github.com/frogdb/frogdb/commit/7f0c04dc17a04f18cf1cb0eec904e6abc3f96ee9))
* **cluster:** shrink the Raft voter set when a node is removed from the topology ([9cda642](https://github.com/frogdb/frogdb/commit/9cda642ba2c62454a4295106b3debb3fbc22e3de))
* **cluster:** stop masking quint-connect conformance failures, gate the fail-terminal ITF bypass, stop discarding apply_local results ([0353241](https://github.com/frogdb/frogdb/commit/03532412fefab3bc459e9ba346cc7b645d71081e))
* **cluster:** truncate the raft log inclusively, as openraft contracts ([ec5ee93](https://github.com/frogdb/frogdb/commit/ec5ee931fc8d85abf892a0ed0facc5b047a9808c))
* **commands,scripting:** use canonical Redis glob in H/S/ZSCAN MATCH and FUNCTION LIST ([99cddcd](https://github.com/frogdb/frogdb/commit/99cddcdf954e4f65659c92444106837f9dc784e9))
* **commands:** harden BF/CF.LOADCHUNK deserialization against crafted payloads ([acc073e](https://github.com/frogdb/frogdb/commit/acc073ea735469f94b4b33b217fe917b3529603f))
* **commands:** route SUNSUBSCRIBE through shard-channel slot check ([d2dd04b](https://github.com/frogdb/frogdb/commit/d2dd04b449b6ef13b5a9571539df249d953514fa))
* **compat:** clear the two clippy test lints left by wave 1 ([d835904](https://github.com/frogdb/frogdb/commit/d835904cffa7b0ab41668690af9086e3ffb70aeb))
* **compat:** redis-feel wave 3 — LATENCY RESET integer reply; close issues 01-10, file 12 ([68f2fc6](https://github.com/frogdb/frogdb/commit/68f2fc66d736da11cc626d7e36c93b48a295055b))
* **core:** implement cluster_check on frogdb-core's own DebugProvider test stub ([f32047b](https://github.com/frogdb/frogdb/commit/f32047b5085c9cd700c35fc366afb9153751fb93))
* **core:** recognize the import target while the source still owns the slot ([28ba5ef](https://github.com/frogdb/frogdb/commit/28ba5ef2b51a2bd1b6258b786e0459eae91ee91d))
* **core:** settle the turmoil-feature clippy debts in the shard handlers ([bc62f3a](https://github.com/frogdb/frogdb/commit/bc62f3a713eeecf643e39f7971a209b6f098c8c2))
* **dev:** drop foreign Justfile hunks accidentally swept into d2955c82 ([840777e](https://github.com/frogdb/frogdb/commit/840777e0033e0b1e28f6a442b736ab4ef257e519))
* **dev:** restore lint-ship-cmd-full wiring dropped by 840777e0 ([04b5fdb](https://github.com/frogdb/frogdb/commit/04b5fdb58592ac14a18906516b348c85cc0ae364))
* **dev:** split dev_server.py build/readiness phases, fix memtier exit-2 ([d2955c8](https://github.com/frogdb/frogdb/commit/d2955c82af86c45e6e371c6c6bf3cf935e0fc176))
* **jepsen:** list-append exec-multi-ops doesn't handle a pipelined EXEC error ([99b6a06](https://github.com/frogdb/frogdb/commit/99b6a0616c18c5ff05df31977d7fa8a98ca93fc6))
* **jepsen:** stop double-counting cluster-check sweeps in the checker ([7d1632a](https://github.com/frogdb/frogdb/commit/7d1632aa7e86802f0051e64b23c12d0810065307))
* **just:** clean-stale used Make-style $$ escapes under sh ([021ab7f](https://github.com/frogdb/frogdb/commit/021ab7f3435771daafa9ced351c8944404819b65))
* **lint:** cover just release in ship-cmd-full gate ([13d4e8f](https://github.com/frogdb/frogdb/commit/13d4e8f0d86534eed0c4c11eeb937d8a93e6130d))
* **lint:** resolve quint via mise in quint-check recipe ([aacbccd](https://github.com/frogdb/frogdb/commit/aacbccd5bcc806ac6b3d42ec39f120dde22793bf))
* **operator:** emit RFC 3339 lastTransitionTime on FrogDB conditions ([6171279](https://github.com/frogdb/frogdb/commit/61712791c73988d879957894900554c1918116f3))
* **persistence:** carry the covered sequence into the WAL watermark, not a post-hoc read (spec-gaps 12) ([eedb76d](https://github.com/frogdb/frogdb/commit/eedb76d03f89210d25c2b843db4d2a8f1f36c956))
* **persistence:** checksum the WAL watermark body; pin the group-commit rule ([47c5cc3](https://github.com/frogdb/frogdb/commit/47c5cc332a5423432e5c925a9914c768461ea74c))
* **persistence:** close cross-CF flush divergence in RocksStore::flush() (spec-gaps 03) ([90c76a7](https://github.com/frogdb/frogdb/commit/90c76a78fe3c079731d881349e80bb075fdd92f3))
* **persistence:** drop the last trim before split_whitespace in the watermark test ([bcb44c2](https://github.com/frogdb/frogdb/commit/bcb44c24b62e7b1f70682e67ca565e19161ec23d))
* **persistence:** drop the redundant trim before split_whitespace ([f79dc2d](https://github.com/frogdb/frogdb/commit/f79dc2d056c6769b96c522064d1cdf4727975e89))
* **persistence:** gate the ack on sync durability (spec-gaps 01) ([477c28e](https://github.com/frogdb/frogdb/commit/477c28e9f9830ba421155da209e72e0a620213d7))
* **persistence:** give WAL failures a taxonomy — latch, truncate, fail-stop (spec-gaps 02) ([3bdbf75](https://github.com/frogdb/frogdb/commit/3bdbf75a787d61fa49fab5852f9334baadd5e726))
* **persistence:** satisfy clippy redundant_closure in current_save_elapsed ([75718e4](https://github.com/frogdb/frogdb/commit/75718e4913c7ee4888f87cb8353d15e9d994d64b))
* **persistence:** stamp the data-dir identity before the install (spec-gaps 20) ([88d62f2](https://github.com/frogdb/frogdb/commit/88d62f2e3fae3d275dae6c269a1729259bb4ec4a))
* **protocol:** sanitize dynamic simple-string replies via SafeStatus newtype ([18b7eca](https://github.com/frogdb/frogdb/commit/18b7eca8b8a464ef956c31778488f532cc1d5582))
* **quint:** address coordinator review of cluster migration/failover model ([ae7ae16](https://github.com/frogdb/frogdb/commit/ae7ae1615a4a6d5a0b887c5f005a2cbdcc49b4a4))
* **replication:** a breached full-sync flush hold aborts the sync ([ddf45af](https://github.com/frogdb/frogdb/commit/ddf45aff21918c5e5084dc0a58fd78f00c9f62cd))
* **replication:** clear clippy in the catalog and the session lag read ([e7da331](https://github.com/frogdb/frogdb/commit/e7da331ca097115e9919ad0a701bf2d2ec2b9a7f))
* **replication:** drop redundant trim() before split_whitespace in fullsync grant test ([6edb201](https://github.com/frogdb/frogdb/commit/6edb2016de2446e5cd1b9870a607a3f122ef4dee))
* **replication:** satisfy clippy redundant_closure in fullsync rate logging ([9323cf3](https://github.com/frogdb/frogdb/commit/9323cf31959d7f36ed50de1e45cd9725e2bab847))
* **replication:** split settle_at_applied so the promotion owns its hook ([07c8ce5](https://github.com/frogdb/frogdb/commit/07c8ce551f67f628eded1e35b688a6d78e5e6b9e))
* **replication:** triage the catalog's first pass to green ([12ccb16](https://github.com/frogdb/frogdb/commit/12ccb16c477defa37f05149e208ef377d01c762e))
* **scripting,server:** re-sync REDIS_VERSION_NUM and INFO section order with the 8.6.0 bump ([04b195d](https://github.com/frogdb/frogdb/commit/04b195d1d1fbd7b4c39c90374f477c8490a1b8eb))
* **scripting:** reply -BUSY when a shard refuses the EVAL continuation lock ([a9fee76](https://github.com/frogdb/frogdb/commit/a9fee7660869158b78d7ca215ba496ce9f6c98fb))
* **server:** allow the argument count on EvalKind::into_message ([3318994](https://github.com/frogdb/frogdb/commit/33189948a24b8b8eb48e8caac1837b9232b27b61))
* **server:** CLIENT PAUSE bypasses for blocking commands, honors their own deadline ([bc1a5c8](https://github.com/frogdb/frogdb/commit/bc1a5c88c1b2fef95201264c5ec9137ecca0014c))
* **server:** drop needless as_deref in stale-gate replication tests ([51f0526](https://github.com/frogdb/frogdb/commit/51f0526ece0330570526440545908884d213304b))
* **server:** set the blocked mirror before the shard registration it describes ([120d01c](https://github.com/frogdb/frogdb/commit/120d01cfb800f7ef7aec99b2809681c591bc025d))
* **server:** wire ClientRegistry::update_multi_state to real MULTI transitions ([5b84494](https://github.com/frogdb/frogdb/commit/5b844945e5a3667221d4b73e6c6808a0677a997b))
* **spec-gaps:** drop stray duplicate of issue 16 left in issues/open/ ([6fd3cda](https://github.com/frogdb/frogdb/commit/6fd3cdace2b131e779f2fc4ee50a336c657d3da6))
* **spec-gen:** stop the ref-def regex swallowing trailing blank lines ([9c33bdc](https://github.com/frogdb/frogdb/commit/9c33bdc73b0d06c00874df98bdc7cf1707963f3e))
* **spec-lint:** reject a present-but-empty Forced-by cell ([a35f2de](https://github.com/frogdb/frogdb/commit/a35f2deb4695befbeba214674f034e12409c8d20))
* **spec-tooling:** repair review findings from the phase-1 scaffolding audit ([aca3e7d](https://github.com/frogdb/frogdb/commit/aca3e7d3028a1cbe5970de932d2d352ad5ed9061))
* **txn:** bump only the shrunk hashes' slots on a field-expiry sweep ([2f839ee](https://github.com/frogdb/frogdb/commit/2f839eeb64916fbfb94594a472eca6fda06a41ba))
* **txn:** carry the routing generation to the shard and recheck watches after the pause ([f05275b](https://github.com/frogdb/frogdb/commit/f05275bf998c8786eae1f1900f8da2e8ce15c906))
* **txn:** fan WATCH out per shard instead of refusing cross-shard batches ([d3f9de6](https://github.com/frogdb/frogdb/commit/d3f9de6737184994a97da4943e8a7c80779d61c7))
* **txn:** fold only live watched shards, route dead watches per shard ([71d2bee](https://github.com/frogdb/frogdb/commit/71d2beeabfc6e0c60db075ad2bac6d982ca2dca8))
* **txn:** make EXEC's empty-queue fast path consult the watch set ([0708058](https://github.com/frogdb/frogdb/commit/0708058aefa2da247097e526f6d0d66999126a38))
* **txn:** resolve the transaction target before charging the rate limiter ([cb6c5c3](https://github.com/frogdb/frogdb/commit/cb6c5c3776295bebf476b73292fff1c01e1b7848))
* **vll:** drop an abandoned continuation park instead of holding the barrier ([d5ddbc0](https://github.com/frogdb/frogdb/commit/d5ddbc02c5146118d90a14e91de30db3c1800bb7))
* **vll:** give a scatter one absolute deadline instead of one per receiver ([7dfe28e](https://github.com/frogdb/frogdb/commit/7dfe28e60993383fdb0d7af6f0038cde2fdcb689))
* **vll:** wound-wait on the SCA path so opposite shard orders cannot deadlock ([7f3f3e5](https://github.com/frogdb/frogdb/commit/7f3f3e526b24f3dadd3a4815b4f47716dea963b7))


### Code Refactoring

* **cluster,types:** move Violation/Citation/Tier catalog vocabulary to frogdb-types ([09cf89f](https://github.com/frogdb/frogdb/commit/09cf89f5b346cd07ccf8f3af3e67f66edb80b449))
* **cluster:** drop the unused RoleController blanket link adapter ([23c0c1c](https://github.com/frogdb/frogdb/commit/23c0c1c36404e42968b152a0e081134dbf94803a))
* **lint:** rename the failure-mode lint to the spec lint ([b02506d](https://github.com/frogdb/frogdb/commit/b02506d99e0c77cd8e8052c5c042df870172695c))
* **persistence:** name the pending-sync-target pair via a type alias ([6074a06](https://github.com/frogdb/frogdb/commit/6074a06172f6ebdf17d1cf205cfc6dd662042571))
* **persistence:** name the wait target, count lost function libraries (spec-gaps 05) ([09a5c08](https://github.com/frogdb/frogdb/commit/09a5c083887805392b6ec13ce3e397d04548c4ff))
* **replication:** carve the replica session's decision half into session_machine ([173ba85](https://github.com/frogdb/frogdb/commit/173ba85390c2c13f7f6689c8c015afc3171cb750))
* **replication:** drive the replica session through the state machine ([bc79389](https://github.com/frogdb/frogdb/commit/bc793892a3af4a65f1fa9aa8975e18f5823102dd))
* **replication:** extract replica-session feed-gate sequencing as pure step (issue 26 option 1) ([e31fee9](https://github.com/frogdb/frogdb/commit/e31fee9216eb7c2d9877e71a7e6471866e8dbcaa))
* **replication:** extract the feed hold's derivation as a pure decision ([2977214](https://github.com/frogdb/frogdb/commit/29772149752f13d66cb16c5f4e93566ce41d218a))
* **replication:** extract the promotion and feed-gate decisions (issue 07 tier i) ([3866e94](https://github.com/frogdb/frogdb/commit/3866e94017ed8b74e148df23044db170bbb680b8))
* **replication:** make PSYNC arm selection a pure decision (issue 07 tier ii) ([3b71aba](https://github.com/frogdb/frogdb/commit/3b71abac13093dd309e4a3ef3df69a170cb4d543))
* **replication:** point the model and the spec at the new seams ([c114ecf](https://github.com/frogdb/frogdb/commit/c114ecf204a715fb8e6a63a502cf62912e5b30c9))
* **sim:** extract the arm-agnostic scheduler into simulation/schedule.rs ([0350c39](https://github.com/frogdb/frogdb/commit/0350c39e5c7829126e409893de5b91ff94d34601))
* **spec:** promote the six failure-mode specs to specs/ ([749cff9](https://github.com/frogdb/frogdb/commit/749cff9c93a7cc6923f2a51741681ca394e7791c))
* **specs:** re-point five cross-references the Quint split left dangling ([5055f68](https://github.com/frogdb/frogdb/commit/5055f6818e45c37267dd0c311d6c65bbf6d53285))
* **specs:** split the cluster Quint models into four files each ([8ea4089](https://github.com/frogdb/frogdb/commit/8ea40890b3c8caf1e4bbd0581d97969c83880c96))
* **vll:** pass a scatter lock request as one value, not seven arguments ([0effb30](https://github.com/frogdb/frogdb/commit/0effb30da496aaccdb3b3cdace45052ed37ba2b7))

## 0.1.0 (2026-04-02)


### Features

* add `just load` target for continuous load generation ([3555c0b](https://github.com/frogdb/frogdb/commit/3555c0bb5e1fb1f3a35940d112a68826a9de78c8))
* add 4 high-value fuzz targets for untrusted input paths ([c453cbd](https://github.com/frogdb/frogdb/commit/c453cbd8f2e33de517352c56c914eb0479d231f6))
* add 7 fuzz targets and fix OOM bugs in deserializers ([70cb1be](https://github.com/frogdb/frogdb/commit/70cb1bec82754410c13d2f9f8a37481b7a0a0ff0))
* add CI/CD pipelines, Helm chart, and Terraform modules for multi-cloud deployment ([1f89b9a](https://github.com/frogdb/frogdb/commit/1f89b9aefd7af2f98980391d2146fafdb02ae9c1))
* add cluster benchmark infrastructure for multi-shard scaling comparison ([1cd6b38](https://github.com/frogdb/frogdb/commit/1cd6b38fb4a8a99866dd68548120ef842348a845))
* add code coverage with cargo-llvm-cov and Codecov CI integration ([e7404df](https://github.com/frogdb/frogdb/commit/e7404df4abbde8a67a168b4da5575830230f6f56))
* add Debian packaging with deb-gen code generator and APT repository ([821efbf](https://github.com/frogdb/frogdb/commit/821efbf9f19ff48f03851b32b833c9f72a4bd565))
* add Docker builder image and system RocksDB verification ([2c50891](https://github.com/frogdb/frogdb/commit/2c508912d54006d7370e0e2eb494f0e7fdc66bbf))
* add frogdb-debug crate to workspace and wire into server ([5858ccd](https://github.com/frogdb/frogdb/commit/5858ccdbf9b03d934e0c8bab6ce87bbfb9db3ab5))
* add frogdb-debug crate with diagnostic web UI ([5d0bd3f](https://github.com/frogdb/frogdb/commit/5d0bd3f41fbe5185f29b31b4d966bde36ab7cf94))
* add geo_ops and ts_label_filter fuzz targets ([75385c3](https://github.com/frogdb/frogdb/commit/75385c3b975994d809dd7f0357f7e8ade56bb5d7))
* add lefthook pre-commit hooks for formatting and lint checks ([583b22e](https://github.com/frogdb/frogdb/commit/583b22e879bdbf5c9adede944cec5e33f0733376))
* add replication-mode finalization, version metrics, and observability for rolling upgrades ([02dc9b6](https://github.com/frogdb/frogdb/commit/02dc9b6d63173a23b158a78f5e51deaf35cc1c5d))
* add rolling upgrade infrastructure (version tracking, gating, CLI) ([c1d09f0](https://github.com/frogdb/frogdb/commit/c1d09f0991e3b724e894927aa214fc7ee62f9766))
* add RPOPLPUSH and ZREVRANGE, fix Redis compat across data types ([6640dd9](https://github.com/frogdb/frogdb/commit/6640dd91c1595a3f481047af0d55b1e674999e25))
* add TLS support with dual-port plaintext/TLS, mTLS, and config infrastructure ([d7d855a](https://github.com/frogdb/frogdb/commit/d7d855a957db37e30925c8b94ab19fdded443349))
* auto-generate config reference docs from Rust source code ([283c6ab](https://github.com/frogdb/frogdb/commit/283c6abe7b3e09be352680788cb182bc8b407f1a))
* bundle Grafana dashboard in Helm chart and add as release asset ([9b597f6](https://github.com/frogdb/frogdb/commit/9b597f60c5ea8e872fa94a34c1eae6950722f6fb))
* close rolling upgrade test gaps with real version gate, CLI tests, and operator awareness ([7ec1799](https://github.com/frogdb/frogdb/commit/7ec179972bf2a5762afe8755d76acdf48dadee5e))
* expand debug UI with embedded assets, JSON APIs, and config display ([da77649](https://github.com/frogdb/frogdb/commit/da776494c421c66233dae6a2a92c39c390c2e845))
* expose admin HTTP port in test harness and add admin health test ([2772249](https://github.com/frogdb/frogdb/commit/277224918723451c782c2d7bafcbb3132fb9901f))
* extend TLS to cluster bus, replication, and HTTP endpoints ([a0b5de8](https://github.com/frogdb/frogdb/commit/a0b5de88ce9500e8ccdfb33b629789accf6480c3))
* improve Redis compatibility across commands and protocol handling ([6ce7b59](https://github.com/frogdb/frogdb/commit/6ce7b596a2130e01436847fa30e61352e8ddec88))
* improve Redis compatibility across commands and protocol handling ([d0dbbd9](https://github.com/frogdb/frogdb/commit/d0dbbd992e0488b3bab5bb037cb07d125282e6fa))
* **metrics:** add debug web UI module for server inspection ([d430549](https://github.com/frogdb/frogdb/commit/d430549cf14b8047adb8939cfc3be11c80c297f5))
* per-suite Redis compat runner with crash/hang detection ([1538837](https://github.com/frogdb/frogdb/commit/1538837e8019da04ac72ea17a46a49831c91ac79))
* redesign debug dashboard with metrics charts, client tracking, and Simple.css ([cad54a5](https://github.com/frogdb/frogdb/commit/cad54a5aaf26db61e303c50322b81c33449ae596))
* switch TOML config keys to kebab-case and add CONFIG param metadata to docs ([2e15c18](https://github.com/frogdb/frogdb/commit/2e15c188bbac2f8e709432fd922e1a4f17707989))
* un-ignore 18 more redis regression tests (batch 3 quick fixes) ([a71f21f](https://github.com/frogdb/frogdb/commit/a71f21f9727913ab218012166910d1c17f22928a))
* un-ignore 31 more redis regression tests (Tier 1C + Tier 2 batch) ([74641c0](https://github.com/frogdb/frogdb/commit/74641c0bd39c2b848110162b72ac27473e647221))
* un-ignore 50 more redis regression tests (batches 4-6) ([6560f16](https://github.com/frogdb/frogdb/commit/6560f16196994a33dab32cfd7aaf5aa34e4c9482))
* wire frogdb_shard_queue_latency_seconds histogram via ShardSender/ShardReceiver newtypes ([6964d65](https://github.com/frogdb/frogdb/commit/6964d65ff6279b6d841675922dc4e82a39ff453b))
* wire PrimaryReplicationHandler through connection pipeline for PSYNC handoff ([a428859](https://github.com/frogdb/frogdb/commit/a4288593ff23fc940792dfa31cd136c116f6b9ba))


### Bug Fixes

* accept case-insensitive engine name in FUNCTION LOAD shebang ([1d779a5](https://github.com/frogdb/frogdb/commit/1d779a5179ac52da84c5d0966dd9b6efca28dd96))
* adapt to rand 0.10 API changes and resolve clippy warnings ([d7cf4ac](https://github.com/frogdb/frogdb/commit/d7cf4acd47bf1d9479ff5d63f1c5746848892d4f))
* gate TLS test modules behind cfg(not(turmoil)) ([3cb0875](https://github.com/frogdb/frogdb/commit/3cb0875193ec6fad72e750f58471d4231a227f76))
* harden config validation, wire dead config fields, and extend CONFIG SET ([adb5602](https://github.com/frogdb/frogdb/commit/adb56025d4124aba4798cca6a6aa78ee77cda27c))
* make LIBCLANG_PATH configurable for Linux compatibility ([3d046f3](https://github.com/frogdb/frogdb/commit/3d046f30420d4377a4d1f2e03a4714ab1e4a571f))
* make MULTI/EXEC transaction side effects atomic ([5160323](https://github.com/frogdb/frogdb/commit/51603233d076cea6d2d4c890129cfa94be7ebbd0))
* patch OOM and catastrophic backtracking found by new fuzz targets ([2c9cfce](https://github.com/frogdb/frogdb/commit/2c9cfcede893c42f7c2b852493395d2bb860083b))
* repair 42 broken markdown links in docs/todo ([8021b97](https://github.com/frogdb/frogdb/commit/8021b977de472d7bf5081c900c31875828c09862))
* repair broken links and stale URLs in website docs ([aa7ae5a](https://github.com/frogdb/frogdb/commit/aa7ae5a648fb565358f0308e749054548654446d))
* replace `?` with `if let` and normalize early-return patterns to satisfy clippy ([fe10537](https://github.com/frogdb/frogdb/commit/fe105376d8b70926bbee860098c11a0a55a25410))
* replace panicking lock methods with fallible error-returning variants ([cfdec13](https://github.com/frogdb/frogdb/commit/cfdec1382b6a3f50f11dc74e0e0593928349cffa))
* resolve breaking API changes from dependency bumps ([9886cfd](https://github.com/frogdb/frogdb/commit/9886cfd22e7885f3d2b250de8c560e6ec3e530f2))
* resolve CI check failures across formatting, lints, licenses, and tests ([3401b0f](https://github.com/frogdb/frogdb/commit/3401b0f0d260c90386300152b0a5bd02f8e23786))
* resolve CI failures across 6 jobs ([12df7f3](https://github.com/frogdb/frogdb/commit/12df7f34fa64d72a80a283b6e7c4518c6bb04183))
* resolve CI failures across test, build, link-check, and release workflows ([aff6a82](https://github.com/frogdb/frogdb/commit/aff6a82c4a4bd00449c474422324e673c245158e))
* resolve clippy warnings in cluster, server TLS init code ([00061cd](https://github.com/frogdb/frogdb/commit/00061cd30866c4f1f2987dd99fb708df01b48bdd))
* resolve clippy warnings in regression tests and TLS tests ([4bad580](https://github.com/frogdb/frogdb/commit/4bad5808710632f766e2e93f90d34e71f95f6812))
* resolve compilation errors when building with turmoil feature ([686520f](https://github.com/frogdb/frogdb/commit/686520f68864a149189117f3fc6b20bb00ce7477))
* resolve expression parser crash and HTTPS test race condition ([447ef2b](https://github.com/frogdb/frogdb/commit/447ef2b2ff9d8061b5079e9194727505311817b3))
* resolve remaining CI failures and fuzzer bitmap overflow ([6514304](https://github.com/frogdb/frogdb/commit/6514304a8331f6f522f90f3eeda7423dc968bf30))
* separate admin HTTP API port from admin RESP port ([73030e8](https://github.com/frogdb/frogdb/commit/73030e853a00ebbae9a27a2c8f179129c7e4ee83))
* set CC/CXX=clang in Docker builder to fix cc-rs build failure ([aec8732](https://github.com/frogdb/frogdb/commit/aec873290b4a4143e5a196d2ece16e5062c74b56))
* set initial release version to 0.1.0 ([502a5f0](https://github.com/frogdb/frogdb/commit/502a5f0bc48c0660cc23cd2451e9cfddab451b58))
* support online certificate reload for outgoing TLS connections ([7eba275](https://github.com/frogdb/frogdb/commit/7eba2757884157d13c84726de7af7c9453da3c1d))
* un-ignore 32 redis regression tests (Tier 1 quick wins) ([c3cc7a4](https://github.com/frogdb/frogdb/commit/c3cc7a47f711b6d0a0f4e9c41c5e5be0fa8534cd))


### Code Refactoring

* add frogdb-macros crate with `#[derive(Command)]` proc macro ([9db0c81](https://github.com/frogdb/frogdb/commit/9db0c8143eb573c3cbe2bb377e8d84c6fd013366))
* add ServerWideOp enum and migrate server-wide command routing ([4e3d8dc](https://github.com/frogdb/frogdb/commit/4e3d8dcbdacc3d7e0b778d2e56e82d69722c925b))
* architecture smell audit — facade methods, function splits, type decomposition ([65081ec](https://github.com/frogdb/frogdb/commit/65081ecaffbc1ed0c35d975ba0a1f6cbc9bdf6d9))
* bind all server listeners eagerly to fix TOCTOU port races ([6a5b988](https://github.com/frogdb/frogdb/commit/6a5b98807ea1f4c4ea8374dfc608603a700c865e))
* consolidate docs into website, remove sync pipeline ([67cd93b](https://github.com/frogdb/frogdb/commit/67cd93b890c083b18b65e11e48bd9587391a1a9d))
* consolidate HTTP servers and add bearer token auth ([6060d77](https://github.com/frogdb/frogdb/commit/6060d775c7a0c644600fce0bfd1469a670006627))
* consolidate metrics architecture — absorb LatencyBandTracker into MetricsRecorder and move recorder to ObservabilityDeps ([203ae46](https://github.com/frogdb/frogdb/commit/203ae465bda3ad774770223012a5bee5d2af537e))
* **core:** split shard module into focused submodules and add request flow spec ([600b71e](https://github.com/frogdb/frogdb/commit/600b71ee3a4c6acf3c19b3b0ad7094c55ee0ddde))
* expand link-check scripts to cover Snappy and zstd, rename to linkcheck-* ([f992952](https://github.com/frogdb/frogdb/commit/f99295255a595fc95849867b90c24296ddbf11eb))
* extract acl, cluster, persistence, and scripting into dedicated crates ([c52bfa8](https://github.com/frogdb/frogdb/commit/c52bfa88b10a0160d1c4fa9067883b7091531b50))
* extract CLIENT, CONFIG, LATENCY, MEMORY, and SLOWLOG handlers into submodules ([611e8ca](https://github.com/frogdb/frogdb/commit/611e8ca89eb388c428300ec536c5a3c5ed8ed033))
* extract command dispatch logic into connection/dispatch.rs ([9e41961](https://github.com/frogdb/frogdb/commit/9e41961a838abacdff8c189684297574fbdc9cfd))
* extract commands, replication, and vll into dedicated crates ([083456d](https://github.com/frogdb/frogdb/commit/083456dd038b9afbc4d2786bf577bc9306902445))
* extract connection handlers into modules and add lock safety extensions ([f777112](https://github.com/frogdb/frogdb/commit/f77711286b66898b7b21f3b75e6204bd2174149c))
* extract frogdb-types crate and remove frogdb-debug crate ([5475093](https://github.com/frogdb/frogdb/commit/54750934e05fff6ef50fc6c63829c27343040a89))
* extract RocksDB check scripts from Justfile into Python scripts ([a1f7e22](https://github.com/frogdb/frogdb/commit/a1f7e223bfa4fc68a070e071581a3de52895da15))
* extract ShardObservability::reset_stats, remove unused AdminHandler ([48185fb](https://github.com/frogdb/frogdb/commit/48185fbfe0059deb2bc29dc11140522ad1857eac))
* extract traits into dedicated modules and add observability abstractions ([49a6a49](https://github.com/frogdb/frogdb/commit/49a6a497419ff89bc986229e7c0b4cf2347f337e))
* implement std Error trait on domain errors and eliminate panics ([fa8c2c9](https://github.com/frogdb/frogdb/commit/fa8c2c94129a4f5c7704289ce9506dd8a84f3159))
* **json:** introduce macros to deduplicate JSON command boilerplate ([a626c6c](https://github.com/frogdb/frogdb/commit/a626c6c710b5ce104834f62131bea59483eb92fe))
* **metrics:** add typed metrics proc macros and dashboard codegen ([30ad227](https://github.com/frogdb/frogdb/commit/30ad227625a2697b6100a3fe75d2c94c803b4f52))
* move basic commands from server module to commands module ([8f2e848](https://github.com/frogdb/frogdb/commit/8f2e848aa79f6fd7d90c75eea153df63088c78ed))
* move docs/todo to top-level todo directory ([0c2a793](https://github.com/frogdb/frogdb/commit/0c2a793ed2faceb936194c93f963b22c118ab403))
* move replication commands into commands module ([8066ec6](https://github.com/frogdb/frogdb/commit/8066ec6aca78842b8d820d05b47d9bbe74e30b7a))
* **protocol:** split Response into WireResponse and InternalAction ([8167d24](https://github.com/frogdb/frogdb/commit/8167d2434647bc75ad4a21307bf6a02cd5a3a7e9))
* rename frog-cli to frogctl ([74cd151](https://github.com/frogdb/frogdb/commit/74cd15100e1ad6e15ee710ba0fbbfc1a5343adc3))
* rename frogdb-metrics to frogdb-telemetry and split debug crate ([85e9d27](https://github.com/frogdb/frogdb/commit/85e9d27765ee7e2369b364caf57fbb037273c8ec))
* rename frogdb-metrics to frogdb-telemetry and split debug crate ([1329984](https://github.com/frogdb/frogdb/commit/13299846d3cd8fce3d63cede2bb16a132453c374))
* rename testing/load-test to testing/load ([617f364](https://github.com/frogdb/frogdb/commit/617f364d003c4b2a768c57ae1b440d6f1bb45c40))
* replace Jepsen per-test Justfile targets with unified Python runner ([8397521](https://github.com/frogdb/frogdb/commit/839752110cb040b14cbf1f839f116d8fa18be41f))
* replace JSON with postcard binary codec and LengthDelimitedCodec for cluster RPCs ([64b5796](https://github.com/frogdb/frogdb/commit/64b5796ea1290ca8650a891d9cc8dc2535f8742c))
* replace manual RESP string parsing in replication with redis-protocol codec ([b785b44](https://github.com/frogdb/frogdb/commit/b785b44de7f6af1151d92384aa3fbd23f3b766ee))
* **server:** split server module into basic_commands, register, and util submodules ([e030c40](https://github.com/frogdb/frogdb/commit/e030c40501255c27f37d27e8d3478527f5fa3683))
* split connection handler pubsub, scatter, scripting, and transaction handlers into separate modules ([1546477](https://github.com/frogdb/frogdb/commit/154647724457a22e9e0cecd7e2b43eb58b21b133))
* split connection.rs into focused submodules ([680272f](https://github.com/frogdb/frogdb/commit/680272f6e6d9cb3ff7997f8896ec54310d24cca1))
* split large modules into submodules for acl, commands, core, and server ([2e69b8f](https://github.com/frogdb/frogdb/commit/2e69b8f0df38d202758e97fb81bd95859529ffc7))
* split sorted set commands into focused submodules ([41d3715](https://github.com/frogdb/frogdb/commit/41d37153fca9179e14c03d633d6f3c934bd1686e))
* split store and sorted_set into modules, add key extraction macros ([ed6c0e1](https://github.com/frogdb/frogdb/commit/ed6c0e15b6218e00daad8a86f30f98bc4495c8b7))
* **test:** split concurrency test mocks into dedicated modules ([76110a2](https://github.com/frogdb/frogdb/commit/76110a2fd67b896ca9d02cbee4a76e57bdcbbf88))
* **tests:** reduce boilerplate by adopting shared test helpers ([5312159](https://github.com/frogdb/frogdb/commit/5312159786b45891577ae86aa1bc296f2eb223cd))


### Performance

* add faster linking, `check` alias, and lean dev debug profile ([93ca447](https://github.com/frogdb/frogdb/commit/93ca447f18f2b149f1f6dc93b36d196671416b33))
* batch multi-response writes to reduce flush syscalls ([9ea945b](https://github.com/frogdb/frogdb/commit/9ea945be6f07f05555f38381bd9a4eb28c3db09c))
* copy-on-write store reads and RocksDB I/O tuning ([a51b7a9](https://github.com/frogdb/frogdb/commit/a51b7a91857d5358562d7896c5aa5eb7ecd69bbe))
* make cluster load test threads/clients configurable and skip cached memtier image build ([52d308a](https://github.com/frogdb/frogdb/commit/52d308a83d1ab7f2e6e539c9edbde15989e81a9b))
* reduce hot-path allocations across SCAN, RESP3 encoding, glob matching, and sorted set range removal ([f91d67c](https://github.com/frogdb/frogdb/commit/f91d67c5d64e9f20d28c6a9724e76edaab00d7dd))
* reduce per-request allocations and enable TCP_NODELAY ([97b06cd](https://github.com/frogdb/frogdb/commit/97b06cdaabb269c34f0416e50cc40abec64f9008))


### Dependencies

* bump the rust-dependencies group across 1 directory with 25 updates ([67a9ef3](https://github.com/frogdb/frogdb/commit/67a9ef3c08660aadadbb567cfe2a932bd43777f5))
* bump the rust-dependencies group across 1 directory with 8 updates ([79e98a9](https://github.com/frogdb/frogdb/commit/79e98a969503d07989b438e90159090c5eb4bbd8))

## [Unreleased]
