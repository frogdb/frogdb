(ns jepsen.frogdb.cluster-replication
  "Cluster-mode synchronous-write durability workload for FrogDB.

   The cluster analogue of `replication-failover`: it answers *does `WAIT` mean
   anything in cluster mode?* — the one question no other test in the suite
   answers.

   ## Why this exists

   In FrogDB a cluster node replicates key data exactly the way a standalone
   primary does: `CLUSTER REPLICATE` attaches the replica over PSYNC, and Raft
   carries cluster metadata only (ADR-0001, no `ClusterCommand` ever carries a
   key). `WAIT` therefore counts *this node's* PSYNC replicas in cluster mode
   just as it does standalone — per-node, never cluster-wide, never redirected.
   That contract is only worth anything if an acknowledged write actually
   survives the loss of the node that acknowledged it, which is what this
   workload measures.

   ## Topology

   A freshly formed cluster is all primaries. `setup!` therefore builds the shard
   itself (`cluster-db/ensure-shard-replica!`): every workload key carries the
   same hash tag, so they all live in one slot; the primary is that slot's owner
   and one other node is attached to it as a cluster replica. The attached node
   keeps owning its *own* slots (`CLUSTER REPLICATE` changes a role, it does not
   move slots) — the workload never writes those.

   ## Shape

     1. Seed phase — interleaved tracked writes on the shard primary:
        `:write-sync`  = `SET` + `WAIT 1` (acknowledged by the replica)
        `:write-async` = `SET` only (may be lost)
     2. Failover — `:kill-primary` (`docker stop`, so the node stays down) then
        `:promote`, which runs `CLUSTER FAILOVER TAKEOVER` on the replica. The
        forced form is correct here precisely because the old primary is dead:
        it removes the dead node and moves its slots to the promoted one.
     3. Post-failover — writes against the promoted node (liveness) and reads of
        the tracked keys, routed with MOVED handling so the slot's new owner is
        followed rather than assumed.
     4. Final reads (driven by `core/frogdb-test`) enumerate every tracked key.

   ## What the checker asserts — and why it is an ordering property

   Redis's own caveat applies: `WAIT` is best-effort even at `>= 1`. An
   acknowledgement itself is solid — a replica ACKs its *applied* offset, so an
   acked write is data that replica would still hold if it were promoted — but
   failover only ranks candidates by replication offset; it does not *require*
   the replica that acknowledged a given write to be the one that wins the
   election, so no absolute per-write survival guarantee exists at `WAIT 1`. The checker asserts
   what the contract does support:

     * acknowledged writes survive *at least as often* as unacknowledged ones
       (the ordering property — `WAIT` must buy something), and
     * the acknowledged survival rate clears a floor (`:min-sync-survival`,
       default 0.9), so a run where `WAIT` bought nothing at all is red even if
       the async writes happened to fare just as badly.

   Strict inequality is deliberately *not* asserted: a run in which no write is
   lost at all is not a violation, and demanding async loss would make the
   verdict depend on the failover happening to drop something.

   Operations:
   - :write-sync  {:key k :value v} — SET + WAIT 1 on the shard primary
   - :write-async {:key k :value v} — SET on the shard primary (may be lost)
   - :write-post  {:key k :value v} — SET on the promoted primary (liveness)
   - :read        k                 — routed GET (follows MOVED)
   - :kill-primary                  — docker stop the shard primary
   - :promote                       — CLUSTER FAILOVER TAKEOVER on the replica"
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [jepsen.frogdb.cluster-client :as cluster-client]
            [jepsen.frogdb.cluster-db :as cluster-db]
            [slingshot.slingshot :refer [try+]]
            [taoensso.carmine :as car :refer [wcar]]))

;; ===========================================================================
;; Key space + shared state
;; ===========================================================================

(def key-tag
  "Hash tag shared by every workload key, so all of them land in one slot and
   therefore on one shard. Without it the keys would spread over every primary
   and `WAIT` on any single node would describe only part of the write set."
  "{jepsen-cluster-wait}")

(defn s-key [i] (str key-tag ":s:" i))
(defn a-key [i] (str key-tag ":a:" i))
(defn p-key [i] (str key-tag ":p:" i))

(def probe-key
  "Key used only to resolve which node owns the shard's slot."
  (str key-tag ":probe"))

(def default-write-count
  "Tracked writes of each kind seeded before the failover. A fixed range so the
   final-read phase can enumerate every key without shared state."
  40)

(def default-min-sync-survival
  "Floor on the fraction of `WAIT`-acknowledged writes that must survive the
   failover. Not 1.0: see the namespace docstring on why `WAIT 1` is an ordering
   property, not an absolute guarantee."
  0.9)

;; Namespace-level, because Jepsen opens one client per worker thread and they
;; must all agree on which node is currently the shard's primary. Reset in
;; `setup!`; republished by `:promote`.
(defonce shard (atom nil))
(defonce current-primary (atom nil))

(def promote-timeout-ms
  "How long `:promote` waits for the promoted node to report `role:master` on the
   data path. The takeover commits through Raft and is then reflected onto the
   data path by the promotion bridge, so this covers a Raft round trip plus the
   local role change, not a data transfer."
  30000)

(defn- data-path-role
  "`role` as reported by INFO replication (\"master\"/\"slave\"), or nil."
  [conn]
  (try+ (get (frogdb/info-replication conn) "role")
        (catch Object _ nil)))

(defn- await-promotion!
  "Poll until `conn` reports `role:master` or `timeout-ms` elapses."
  [conn timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (cond
        (= "master" (data-path-role conn)) true
        (< (System/currentTimeMillis) deadline) (do (Thread/sleep 250) (recur))
        :else false))))

;; ===========================================================================
;; Client
;; ===========================================================================

(defrecord ClusterReplicationClient [conns nodes docker-host? base-port slot-mapping]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          bp (get test :base-port cluster-db/default-base-port)
          all-nodes (or (:cluster-nodes test) (:nodes test) ["n1" "n2" "n3"])
          all-conns (cluster-db/all-node-conns all-nodes docker? bp)]
      (info "Opening cluster-replication client (nodes:" all-nodes ")")
      (assoc this
             :conns all-conns
             :nodes all-nodes
             :docker-host? docker?
             :base-port bp
             :slot-mapping (atom (cluster-client/create-slot-mapping all-nodes docker? bp)))))

  (setup! [this test]
    (let [ref-conn (get conns (first nodes))]
      (cluster-db/wait-for-cluster-ready ref-conn)
      ;; Batch runs share a JVM, so the namespace-level state has to be dropped
      ;; when a *new* test starts — otherwise this run would inherit the previous
      ;; run's (promoted, now stale) shard. Every worker's `setup!` goes through
      ;; the same lock the attach does, so exactly one of them rebuilds it.
      (locking shard
        (let [test-id (str (:start-time test))]
          (when (not= test-id (:test-id @shard))
            (reset! shard nil)
            (reset! current-primary nil))
          (let [s (cluster-db/ensure-shard-replica! shard ref-conn conns nodes
                                                    docker-host? base-port probe-key)]
            (swap! shard assoc :test-id test-id)
            (compare-and-set! current-primary nil (:primary s))
            (info "cluster-replication: shard primary" (:primary s)
                  "replica" (:replica s)
                  "attached?" (:attached? s))))))
    this)

  (invoke! [this test op]
    (case (:f op)
      ;; Synchronous write: SET + WAIT 1 on the shard primary. Only an ACK from
      ;; the attached cluster replica can satisfy it.
      :write-sync
      (frogdb/with-error-handling op
        (let [{:keys [key value]} (:value op)
              pconn (get conns @current-primary)
              result (frogdb/write-durable! pconn key value 1 5000)]
          (if (:timeout result)
            (assoc op :type :info :error :wait-timeout :acked (:acked result))
            (assoc op :type :ok :acked (:acked result)))))

      ;; Asynchronous write: no WAIT, so it may be lost with the primary.
      :write-async
      (frogdb/with-error-handling op
        (let [{:keys [key value]} (:value op)
              pconn (get conns @current-primary)]
          (wcar pconn (car/set key (str value)))
          (assoc op :type :ok)))

      ;; Post-failover liveness: the promoted node accepts writes for the slots
      ;; it took over.
      :write-post
      (frogdb/with-error-handling op
        (let [{:keys [key value]} (:value op)]
          (cluster-client/cluster-set slot-mapping key value docker-host? nodes base-port)
          (assoc op :type :ok)))

      ;; Routed read: starts at the node we believe owns the slot and follows
      ;; MOVED, so a read after the takeover reaches the new owner instead of
      ;; asserting against a stale one.
      :read
      (frogdb/with-error-handling op
        (let [key (:value op)
              v (:value (cluster-client/execute-with-redirect
                          slot-mapping @current-primary key ["GET" key]
                          docker-host? nodes base-port))]
          (assoc op :type :ok :value {:key key :value (frogdb/parse-value v)})))

      ;; Stop the shard primary so it stays down across the promotion.
      :kill-primary
      (try+
        (let [prim @current-primary
              container (cluster-db/raft-container-name prim)]
          (info "cluster-replication: stopping shard primary" prim "(" container ")")
          (cluster-db/docker-stop container)
          (assoc op :type :ok :value {:killed prim}))
        (catch Object e
          (warn "kill-primary failed:" (pr-str e))
          (assoc op :type :info :error [:kill-failed (str e)])))

      ;; Promote the attached replica with CLUSTER FAILOVER TAKEOVER.
      :promote
      (try+
        (let [{:keys [primary replica]} @shard
              rconn (get conns replica)
              resp (cluster-db/cluster-failover! rconn "TAKEOVER")
              promoted? (await-promotion! rconn promote-timeout-ms)]
          (when promoted?
            (reset! current-primary replica))
          ;; The takeover moved the slot, so every cached mapping is stale.
          (reset! slot-mapping
                  (cluster-client/refresh-slot-mapping @slot-mapping replica
                                                       docker-host? base-port))
          (info "cluster-replication: takeover on" replica
                (if promoted? "succeeded" "did NOT reach role:master")
                "(old primary" primary ")")
          (assoc op
                 :type (if promoted? :ok :fail)
                 :value {:new-primary replica
                         :old-primary primary
                         :response (str resp)}))
        (catch Object e
          (warn "promote failed:" (pr-str e))
          (assoc op :type :info :error [:promote-failed (str e)])))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (doseq [[_ c] conns] (frogdb/close-conn! c))))

(defn create-client [] (map->ClusterReplicationClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn generator
  "Self-driven cluster failover generator (runs with the `none` nemesis).

   Options:
   - :rate        - ops/sec (default 10)
   - :write-count - tracked writes of each kind (default 40)"
  [opts]
  (let [rate (get opts :rate 10)
        n (get opts :write-count default-write-count)
        dt (/ 1 (max 1 rate))
        sync-writes (map (fn [i] {:type :invoke :f :write-sync
                                  :value {:key (s-key i) :value i}})
                         (range n))
        async-writes (map (fn [i] {:type :invoke :f :write-async
                                   :value {:key (a-key i) :value i}})
                          (range n))]
    (gen/phases
      (gen/log "Phase 1: seeding WAIT-acked and async writes on the shard primary")
      (->> (interleave sync-writes async-writes) (gen/stagger dt))
      (gen/sleep 2)

      (gen/log "Phase 2: stopping the shard primary")
      (gen/once {:type :invoke :f :kill-primary})
      (gen/sleep 3)
      (gen/log "Phase 2: CLUSTER FAILOVER TAKEOVER on the shard replica")
      (gen/once {:type :invoke :f :promote})
      (gen/sleep 3)

      (gen/log "Phase 3: post-failover traffic against the promoted primary")
      (->> (gen/mix [(let [c (atom 0)]
                       (fn [] {:type :invoke :f :write-post
                               :value {:key (p-key (swap! c inc)) :value @c}}))
                     (fn [] {:type :invoke :f :read :value (s-key (rand-int n))})])
           (gen/limit 30)
           (gen/stagger dt)))))

(defn final-generator
  "Read every tracked key off the promoted primary so the checker can compute the
   two survival rates."
  [opts]
  (let [n (get opts :write-count default-write-count)]
    (->> (concat (map (fn [i] {:type :invoke :f :read :value (s-key i)}) (range n))
                 (map (fn [i] {:type :invoke :f :read :value (a-key i)}) (range n)))
         (gen/stagger 0.02))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn- written-map
  "Map of key -> written value for every op of `f` that returned :ok and passed
   `pred`."
  [history f pred]
  (reduce (fn [m op] (assoc m (get-in op [:value :key]) (get-in op [:value :value])))
          {}
          (filter #(and (= f (:f %)) (= :ok (:type %)) (pred %)) history)))

(defn- final-read-map
  "Map of key -> last successfully-read value."
  [history]
  (reduce (fn [m op] (assoc m (get-in op [:value :key]) (get-in op [:value :value])))
          {}
          (filter #(and (= :read (:f %)) (= :ok (:type %))) history)))

(defn- survivors
  "Keys of `written` whose final read still returns the written value."
  [written reads]
  (filter (fn [[k v]] (= v (get reads k ::missing))) written))

(defn- rate [num denom]
  (if (pos? denom) (double (/ num denom)) 1.0))

(defn checker
  "Assert that cluster-mode `WAIT` buys durability across a failover.

   :valid? requires
     - the failover actually happened (primary stopped + replica promoted), so a
       green verdict is never vacuous;
     - the promoted node accepted at least one write (liveness);
     - acknowledged writes survived at least as often as unacknowledged ones;
     - the acknowledged survival rate cleared :min-sync-survival.

   See the namespace docstring for why this is an ordering property with a floor
   rather than an empty-loss-set assertion: `WAIT 1` does not pin *which* replica
   wins the election, so Redis's best-effort caveat applies to FrogDB too."
  ([] (checker {}))
  ([copts]
   (let [floor (get copts :min-sync-survival default-min-sync-survival)]
     (reify checker/Checker
       (check [_ test history opts]
         (let [sync-acked (written-map history :write-sync #(>= (or (:acked %) 0) 1))
               async-ok (written-map history :write-async (constantly true))
               reads (final-read-map history)
               sync-survivors (survivors sync-acked reads)
               async-survivors (survivors async-ok reads)
               sync-rate (rate (count sync-survivors) (count sync-acked))
               async-rate (rate (count async-survivors) (count async-ok))
               sync-loss (->> sync-acked
                              (keep (fn [[k v]]
                                      (let [rv (get reads k ::missing)]
                                        (when (not= v rv)
                                          {:key k :acked v
                                           :read (if (= rv ::missing) nil rv)}))))
                              (into []))
               promote-op (first (filter #(and (= :promote (:f %)) (= :ok (:type %)))
                                         history))
               killed? (boolean (first (filter #(and (= :kill-primary (:f %))
                                                     (= :ok (:type %)))
                                               history)))
               promoted? (boolean promote-op)
               post-writes-ok (count (filter #(and (= :write-post (:f %))
                                                   (= :ok (:type %)))
                                             history))
               ;; A WAIT that timed out is not a durability failure — it is the
               ;; documented reduced-count answer — but a run where every WAIT
               ;; timed out proves nothing, so it is reported.
               wait-timeouts (count (filter #(and (= :write-sync (:f %))
                                                  (= :wait-timeout (:error %)))
                                            history))]
           {:valid? (and killed?
                         promoted?
                         (pos? post-writes-ok)
                         (pos? (count sync-acked))
                         (>= sync-rate async-rate)
                         (>= sync-rate floor))
            :failover-occurred? (and killed? promoted?)
            :new-primary (get-in promote-op [:value :new-primary])
            :sync-acked-count (count sync-acked)
            :sync-survival-rate sync-rate
            :sync-loss-count (count sync-loss)
            ;; Capped so results.edn stays readable when a run loses everything.
            :sync-loss (vec (take 20 sync-loss))
            :async-written-count (count async-ok)
            :async-survival-rate async-rate
            :min-sync-survival floor
            :wait-timeouts wait-timeouts
            :post-failover-writes-ok post-writes-ok}))))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a cluster-mode synchronous-write durability workload.

   Options:
   - :rate               - operations per second
   - :write-count        - tracked writes of each kind
   - :min-sync-survival  - floor on the acked-write survival rate"
  [opts]
  {:client (create-client)
   :generator (generator opts)
   :final-generator (final-generator opts)
   :checker (checker opts)})
