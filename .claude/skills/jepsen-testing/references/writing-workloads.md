# Writing Jepsen Workloads for FrogDB

## File Structure

Workload files live at `testing/jepsen/frogdb/src/jepsen/frogdb/<workload>.clj`.

Each workload file provides:
1. A **client** (defrecord implementing `client/Client`)
2. A **workload function** that returns a map with `:client`, `:generator`, `:checker`

## Workload Function Signature

```clojure
(defn workload
  "Description of what this workload tests."
  [opts]
  {:client    (->MyClient nil)           ; Client record (nil conn, filled in setup)
   :generator (gen/mix [...])            ; Operation generator
   :checker   (checker/linearizable ...) ; Result checker
   })
```

The returned map is merged into the Jepsen test by `core.clj`'s `frogdb-test` function.

## Client Protocol

Clients implement `jepsen.client/Client`:

```clojure
(defrecord MyClient [conn]
  client/Client

  (open! [this test node]
    ;; Called once per worker thread. Create connection.
    ;; Return new client with connection.
    (let [conn (client/conn-spec node (:base-port test) true (:base-port test))]
      (assoc this :conn conn)))

  (setup! [this test]
    ;; Called once (on one thread). Initialize test state (e.g., set initial value).
    this)

  (invoke! [this test op]
    ;; Called for each operation. Execute and return result.
    ;; Must return op with :type set to :ok, :fail, or :info
    (client/with-error-handling op
      (case (:f op)
        :read  (let [v (client/read-register conn "mykey")]
                 (assoc op :type :ok :value v))
        :write (do (client/write-register! conn "mykey" (:value op))
                   (assoc op :type :ok)))))

  (teardown! [this test]
    ;; Called once. Cleanup test state.
    this)

  (close! [this test]
    ;; Called once per worker thread. Close connection.
    this))
```

### Error handling

Always wrap `invoke!` body with `client/with-error-handling`:

```clojure
(client/with-error-handling op
  ;; Your operation code here
  )
```

This catches `ConnectException`, `SocketTimeoutException`, `IOException`, and Redis errors,
returning appropriate `:fail` or `:info` results.

For cluster operations, also use `client/with-clusterdown-retry`:

```clojure
(client/with-clusterdown-retry 5
  (client/with-error-handling op
    ;; operation code
    ))
```

This retries up to N times on `CLUSTERDOWN` responses with 1s backoff.

## Generator Patterns

### Basic mix of operations

```clojure
;; Equal mix of reads and writes
(gen/mix [{:f :read} {:f :write, :value (rand-int 100)}])

;; Weighted mix (3:1 adds to reads)
(gen/mix (concat (repeat 3 {:f :add, :value 1})
                 [{:f :read}]))
```

### Rate limiting

Rate is controlled at the test level via `--rate N` (ops/sec). The generator just
produces operations; `core.clj` wraps it with `gen/stagger`.

### CAS operations

```clojure
{:f :cas, :value [(rand-int 5) (rand-int 5)]}  ; [expected new]
```

### Multi-key (independent) testing

For per-key linearizability, wrap generator and checker with `independent`:

```clojure
(require '[jepsen.independent :as independent])

{:generator (->> (independent/concurrent-generator
                   10    ; concurrent keys
                   (range) ; key sequence
                   (fn [k] (gen/mix [...])))
                 (gen/stagger (/ (:rate opts))))
 :checker   (independent/checker (checker/linearizable ...))}
```

## Checker Selection

### Knossos — Linearizability

Best for single-register operations (read/write/CAS).

```clojure
(checker/linearizable
  {:model     (model/cas-register)     ; or (model/register initial-value)
   :algorithm :linear})
```

### Elle — Serializability / Snapshot Isolation

Best for multi-key transactional workloads. Uses `jepsen.tests.cycle.append/test`.

```clojure
(require '[jepsen.tests.cycle.append :as append])

(append/test {:key-count        10
              :max-txn-length   4
              :max-writes-per-key 16
              :consistency-models [:strict-serializable]})
```

This returns a full workload map (client, generator, checker) — see `list_append.clj` for
how to wrap it with FrogDB's client.

### Custom checker

For workloads with domain-specific correctness criteria:

```clojure
(reify checker/Checker
  (check [this test history opts]
    (let [ok-adds (->> history
                       (filter #(and (= :add (:f %)) (= :ok (:type %))))
                       (map :value))
          final-read (->> history
                          (filter #(and (= :read (:f %)) (= :ok (:type %))))
                          last
                          :value)]
      {:valid?   (= (reduce + 0 ok-adds) final-read)
       :expected (reduce + 0 ok-adds)
       :actual   final-read})))
```

## Client Operations (from client.clj)

### String operations (registers)
- `(client/read-register conn key)` — GET
- `(client/write-register! conn key value)` — SET
- `(client/cas-register! conn key expected new-value)` — Compare-and-swap via Lua script

### Counters
- `(client/read-counter conn key)` — GET (parsed as long)
- `(client/incr-counter! conn key amount)` — INCRBY

### Lists
- `(client/rpush! conn key value)` — RPUSH
- `(client/lpush! conn key value)` — LPUSH
- `(client/lpop! conn key)` — LPOP
- `(client/lrange conn key start stop)` — LRANGE

### Sets
- `(client/sadd! conn key member)` — SADD
- `(client/smembers conn key)` — SMEMBERS

### Hashes
- `(client/hset! conn key field value)` — HSET
- `(client/hget conn key field)` — HGET
- `(client/hgetall conn key)` — HGETALL

### Transactions
- `(client/multi-set! conn kvs)` — MULTI/EXEC SET
- `(client/multi-get conn keys)` — MULTI/EXEC GET

### Replication
- `(client/wait! conn numreplicas timeout-ms)` — WAIT
- `(client/role conn)` — ROLE
- `(client/is-primary? conn)` / `(client/is-replica? conn)`

## Cluster Client Patterns (from cluster_client.clj)

For Raft cluster workloads, use the cluster client instead of the basic client.

### CRC16 slot calculation

```clojure
(require '[jepsen.frogdb.cluster-client :as cc])

(cc/crc16 "mykey")           ; Raw CRC16
(cc/key-slot "mykey")        ; CRC16 mod 16384
(cc/key-slot "{tag}mykey")   ; CRC16 of "tag" mod 16384
```

### Slot mapping and routing

```clojure
;; Get current slot→node mapping
(cc/parse-cluster-slots conn)

;; Route a key to the right node
(cc/route-key slot-mapping key)
```

### MOVED/ASK handling

The cluster client automatically handles MOVED and ASK redirects. When receiving a
MOVED response, it refreshes the slot mapping and retries. For ASK, it sends ASKING
to the target node first.

### Hash tags for co-location

Use `{tag}` in keys to force them to the same slot:

```clojure
;; These all hash to the same slot (CRC16 of "k"):
"{k}key1" "{k}key2" "{k}key3"
```

This is essential for multi-key operations in cluster mode.

## Registering a New Workload

### 1. Create the workload file

`testing/jepsen/frogdb/src/jepsen/frogdb/my_workload.clj`

### 2. Add to core.clj workload dispatch

In `core.clj`, add a require and a dispatch entry:

```clojure
;; In the ns declaration:
[jepsen.frogdb.my-workload :as my-workload]

;; In the workloads map:
(def workloads
  {...
   :my-workload my-workload/workload})
```

### 3. Add test definitions to run.py

```python
# In the TESTS tuple:
TestDefinition(
    "my-workload", "my-workload", "none", 30,
    Topology.SINGLE, suites=("single", "all")
),
TestDefinition(
    "my-workload-crash", "my-workload", "kill", 60,
    Topology.SINGLE, suites=("crash", "all")
),
```

### 4. Update CLI validation in core.clj

The workload validation in `cli-opts` automatically picks up new entries from the
`workloads` map — no changes needed.

The nemesis validation list in `cli-opts` must be updated if you add a new nemesis type.

## Example: Anatomy of register.clj

`register.clj` is the simplest and most well-documented workload — use it as a template.

Key structure:
1. **Client record** (`RegisterClient`) — `open!` creates connection, `invoke!` dispatches
   on `(:f op)` for `:read`, `:write`, `:cas`
2. **Error handling** — `with-error-handling` wraps all operations
3. **Generator** — `gen/mix` of reads, writes, CAS ops with `gen/stagger`
4. **Checker** — `checker/linearizable` with `model/cas-register`
5. **Independent mode** — Optional multi-key testing with `independent/concurrent-generator`
6. **Workload function** — Returns `{:client :generator :checker}` map
