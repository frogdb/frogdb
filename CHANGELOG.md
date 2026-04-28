# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [0.1.1](https://github.com/frogdb/frogdb/compare/v0.1.0...v0.1.1) (2026-04-28)


### Features

* add dev-server skill with random port support and state file ([5a29113](https://github.com/frogdb/frogdb/commit/5a291135e91d7d1a14ef4b4fdcb90a15a20ef005))
* add error statistics and per-command latency histograms ([36643a5](https://github.com/frogdb/frogdb/commit/36643a593172b901c744db241e217cbc81963b69))
* add new charts, visual polish, and logo to debug UI ([cc86596](https://github.com/frogdb/frogdb/commit/cc8659617c2cb14edace86234479934baf61cedb))
* add OOM enforcement for FCALL, FUNCTION DUMP/RESTORE tests ([b990185](https://github.com/frogdb/frogdb/commit/b9901853da0a86ad5d03a9ac80dd62970f236a0d))
* add persistence/pubsub/system metrics and cluster tab to debug UI ([84dd486](https://github.com/frogdb/frogdb/commit/84dd486132313cff9dd8fb7446ab448b0a1fe1b7))
* client eviction — maxmemory-clients with per-client memory tracking ([c8ed800](https://github.com/frogdb/frogdb/commit/c8ed80078ecff28f073b230827fbdca37f421da7))
* COMMAND GETKEYSANDFLAGS, movablekeys flag, commandstats tests ([5afae8c](https://github.com/frogdb/frogdb/commit/5afae8c2cf483e7fff7f321e16521cfc6308ec5b))
* **compat:** CLIENT PAUSE, protocol edge cases, stream wake, misc fixes ([71d3cf0](https://github.com/frogdb/frogdb/commit/71d3cf06f61e072803f4b5fb8b093bebc4cf9560))
* complete RESP3 protocol support ([473aa21](https://github.com/frogdb/frogdb/commit/473aa213073b8d3d46e86d20666a311c8ea7c466))
* **config:** implement CONFIG SET/GET requirepass, un-ignore auth test ([d145463](https://github.com/frogdb/frogdb/commit/d145463ab15c153d6741e6d085910bed56030351))
* **docs:** add Redis test suite compatibility page ([7ce62b3](https://github.com/frogdb/frogdb/commit/7ce62b3e20a7c49489a9781806eab88ba7c86794))
* HOTKEYS subsystem — START/STOP/RESET/GET with key access sampling ([26e393d](https://github.com/frogdb/frogdb/commit/26e393dc2f846948a837b889d9ce9cc708b16ab8))
* implement keyspace notifications infrastructure ([c928301](https://github.com/frogdb/frogdb/commit/c9283014606f77ef81f0aa371cc2de258cc4ab72))
* improve debug page role badge clarity and add dev server recipe ([61a767d](https://github.com/frogdb/frogdb/commit/61a767d6d5ceb649ecd974f8e9969bfa8fa2409a))
* INFO keysizes — power-of-2 histograms for element counts and key memory ([d99d441](https://github.com/frogdb/frogdb/commit/d99d4418111aee027b5794c782deb973f549c2a4))
* introspection gaps — CONFIG sanity, CLIENT REPLY, RESET lib-name ([2437db3](https://github.com/frogdb/frogdb/commit/2437db39e5e0540571347fef64d1542bab45d163))
* lazyfree counters, CONFIG REWRITE, debug commands, error msg fixes ([742e741](https://github.com/frogdb/frogdb/commit/742e74196a93c9848692db6abeddcba4e99bc7f9))
* MULTI/EXEC enhancements — WATCH stale keys, OOM, script timeout tests ([8ad8a4e](https://github.com/frogdb/frogdb/commit/8ad8a4e4bc891e33b58e3d604dbb01dbbb43abd0))
* propagate CLIENT NO-TOUCH to shard, increment LFU counter on access ([56539e9](https://github.com/frogdb/frogdb/commit/56539e94ed3dcc6c6ca37d22a51256d70bd94a84))
* **redis-regression:** track *_regression.rs files in audit PORT_MAP ([b8b2259](https://github.com/frogdb/frogdb/commit/b8b2259449b8e9d10aef1cf42cab5f04555f4936))
* RESP3 completion — blocking zset scores, scripting maps, pubsub ([b4fad4b](https://github.com/frogdb/frogdb/commit/b4fad4bb1015cdf6a351e541b7f34c700d993f50))
* **scripting:** add cjson and cmsgpack libraries to Lua VM ([d25ebda](https://github.com/frogdb/frogdb/commit/d25ebdac2467f0830716c5fe1eb24c6131c78cf0))
* **scripting:** EVAL shebang flag parsing, no-cluster/no-writes enforcement, cross-slot validation ([ded179d](https://github.com/frogdb/frogdb/commit/ded179d017fd5d9b24e344984b003ec5f958f451))
* **streams:** consumer group lag computation, active-time tracking, XINFO STREAM FULL groups ([c9fba2d](https://github.com/frogdb/frogdb/commit/c9fba2d86eb8e3c32f891da58bed2a594c69d96d))


### Bug Fixes

* add header separator and balance header padding ([b1deaa3](https://github.com/frogdb/frogdb/commit/b1deaa34d11660d0c7a46d2496d6db43c74ee835))
* add UnoCSS integration for sidebar icons and reduce category size ([a376188](https://github.com/frogdb/frogdb/commit/a376188cc189edfba371150e4da0ad6c2ddbff37))
* align release-please tags with release workflow trigger ([#35](https://github.com/frogdb/frogdb/issues/35)) ([0f90d27](https://github.com/frogdb/frogdb/commit/0f90d27a322d22f92b7cddca204f914175b412ae))
* bake libclang-dev into self-hosted runner and skip apt install ([0efa436](https://github.com/frogdb/frogdb/commit/0efa4369356af1ff4ec703df73787d9fcde4787f))
* **blocking:** BRPOPLPUSH wrong-type dest and blocking-in-MULTI edge cases ([9bccabe](https://github.com/frogdb/frogdb/commit/9bccabee2ec361912a46579900d7c8968b60b370))
* **blocking:** CLIENT UNBLOCK, wake chains, type filtering, commandstats, DEBUG SLEEP gate ([b02e00b](https://github.com/frogdb/frogdb/commit/b02e00bc4b67e704c3f9efa3d3f16073c7a54d30))
* **blocking:** reject negative numkeys and invalid COUNT in BZMPOP/BLMPOP ([0788037](https://github.com/frogdb/frogdb/commit/0788037992ffbb3893c17bfaa77b02831394b5cf))
* clean up stale compat-gen exclusion comment in sort_tcl.rs ([aa6ab85](https://github.com/frogdb/frogdb/commit/aa6ab853d80834938600e2733b2f3bd9a0bce16d))
* **cluster-tests:** add missing assertions to prevent vacuous test passes ([269c6a8](https://github.com/frogdb/frogdb/commit/269c6a8adab9df78eb76899961d4feb5d6bf7458))
* **cluster-tests:** improve timeout diagnostic in failover force test ([6f6a7bd](https://github.com/frogdb/frogdb/commit/6f6a7bd09133e7539870e988e65147d12ae449e0))
* **cluster-tests:** replace sleep-based waits with polling loops in flaky tests ([c5021b5](https://github.com/frogdb/frogdb/commit/c5021b5973ccbcf854a772e48c87b1e52667a569))
* **cluster:** add dynamically-joined nodes to Raft voter set ([722a42d](https://github.com/frogdb/frogdb/commit/722a42d97c0663c408cbafaddc1da951f1955fa4))
* **cluster:** cross-shard Lua dispatch and SPUBLISH cross-slot detection ([78fa31b](https://github.com/frogdb/frogdb/commit/78fa31b5fc925d1fdd06ac654548e72fa9509231))
* **compat:** RENAME/SORT waiter signaling, approximate trim, XSETID parsing ([a9b78b9](https://github.com/frogdb/frogdb/commit/a9b78b9cdd827eff4c97d909615d201c13ea7802))
* **compat:** un-ignore 21 tests — CLIENT PAUSE, Lua edge cases, streams ([598aaf8](https://github.com/frogdb/frogdb/commit/598aaf8df0dd6235c929fe251e43c3b770384f67))
* correct 3 failing tests (SELECT error msg, EVAL undeclared key, CLUSTER NODES migrations) ([2bb3be5](https://github.com/frogdb/frogdb/commit/2bb3be52db6767c6b04de5de70203ada8b34789b))
* eliminate gray gaps in debug UI metrics grid ([1261b08](https://github.com/frogdb/frogdb/commit/1261b08cdee117b3170a20d6a927ea1ab99e4978))
* fuse nav tabs into segmented button group with proper active borders ([2e1aa90](https://github.com/frogdb/frogdb/commit/2e1aa90e161b9c11db8323b1c4a04a8140ef30b1))
* improve debug UI chart readability and durability lag display ([4f0bd88](https://github.com/frogdb/frogdb/commit/4f0bd88167a818822621778cdce7bca32451c5ee))
* prepend base path to internal docs links for GitHub Pages ([#22](https://github.com/frogdb/frogdb/issues/22)) ([bfa9c0b](https://github.com/frogdb/frogdb/commit/bfa9c0b3510e984c9cd000b0925bdee268c83da5))
* **redis-regression:** parse upstream test trailing tags + rename fuzzy-miss tests ([7ebfa4b](https://github.com/frogdb/frogdb/commit/7ebfa4b474fb11192d8187b174c4f4155e70b0cb))
* remove leftover conflict marker in zset_tcl.rs ([c7922c5](https://github.com/frogdb/frogdb/commit/c7922c597df9b444db7283b9c29346ec899e6aa2))
* remove starlight-blog plugin to fix blank homepage and use 128px header logo ([9460064](https://github.com/frogdb/frogdb/commit/946006410752ce69cb3cf9327fbe13c7a7df02ea))
* resolve 6 failing unit tests on main branch ([3d52019](https://github.com/frogdb/frogdb/commit/3d520191494e8c371cd17ac380ff4a7cc7486d55))
* restore Justfile formatting broken by prose-wrap ([7ed9a15](https://github.com/frogdb/frogdb/commit/7ed9a15033017fc0990c4f05ecc9625c69e0e01f))
* **scripting:** align Lua global protection and sandbox with Redis behavior ([8e05514](https://github.com/frogdb/frogdb/commit/8e05514e57b0104cda666e9c4dd329d2bc57ab28))
* **scripting:** allow blocking commands in Lua scripts (non-blocking) ([66796cd](https://github.com/frogdb/frogdb/commit/66796cd9e740a2204eaafa17e918639cd3f40439))
* **scripting:** fix pcall/error handling and Lua helper function registration ([eefab0a](https://github.com/frogdb/frogdb/commit/eefab0a05c8decba6c06314e38d9bf8a627d5a71))
* **scripting:** Lua compatibility fixes for 10 Redis regression tests ([eef765a](https://github.com/frogdb/frogdb/commit/eef765a1ee62d9db0fad997f15e9252ce356a405))
* **scripting:** pass is_cluster_mode to evalsha in shard handler ([30cce4e](https://github.com/frogdb/frogdb/commit/30cce4e393743129fa8983df1cb6221f01eb62a4))
* **streams:** remove WaiterWake::All from SET/DEL, fix clippy warnings ([f3cff1f](https://github.com/frogdb/frogdb/commit/f3cff1fe9b9a4635712f804ceead98fb4f4b951e))
* **streams:** XTRIM must not update max-deleted-entry-id, fix flaky xautoclaim test ([39952ba](https://github.com/frogdb/frogdb/commit/39952bae053ec7019951d24df1732b9a74395542))
* un-ignore 25 regression tests (Batch 8: ACL + FUNCTION engine) ([add6f7e](https://github.com/frogdb/frogdb/commit/add6f7eeaf06092f4dbb566d8a6ad4db009e4af6))
* un-ignore 6 more regression tests (FUNCTION engine deep fixes) ([07ebe31](https://github.com/frogdb/frogdb/commit/07ebe31324a2574320ff101a0ad388fd7e905ab3))
* wait for HTTPS listener before TLS handshake in tests ([#34](https://github.com/frogdb/frogdb/issues/34)) ([1c93801](https://github.com/frogdb/frogdb/commit/1c9380193a843806c9995e630ede3bedeea7049e))


### Code Refactoring

* remove ACL selectors v2 support ([8121bfe](https://github.com/frogdb/frogdb/commit/8121bfee1255419f9dc448ce413d06aadf66f0ce))


### Dependencies

* bump the rust-dependencies group across 1 directory with 3 updates ([#32](https://github.com/frogdb/frogdb/issues/32)) ([2c9a935](https://github.com/frogdb/frogdb/commit/2c9a935700c959bd06b04fdf5a154cece7eac5e1))
* unified dependency updates ([#30](https://github.com/frogdb/frogdb/issues/30)) ([a4645e6](https://github.com/frogdb/frogdb/commit/a4645e6166b036d3db7edf60a57e68c27bcd974c))

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
