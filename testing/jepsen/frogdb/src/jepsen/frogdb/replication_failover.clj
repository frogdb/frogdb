(ns jepsen.frogdb.replication-failover
  "Failover durability workload for FrogDB replication.

   Answers the question no other test in the suite answers: *does an
   acknowledged write survive a primary kill followed by replica promotion?*

   The workload is self-driven (runs with the `none` nemesis, like
   `rolling-restart` / `migration-recovery`): it seeds tracked writes on the
   primary, then drives the failover itself through operations —

     1. Baseline: SET a fixed set of durable keys (`SET` + `WAIT 2`, i.e. every
        write is acknowledged by BOTH replicas — full replica count) plus a
        parallel set of async keys (`SET` only, no `WAIT`).
     2. Failover:
        a. `:kill-primary` — `docker stop` the current primary so it stays down
           (a fenced-out crashed primary, not a transient process bounce).
        b. `:promote` — pick the most up-to-date live replica (highest
           `slave_repl_offset`), issue `REPLICAOF NO ONE` (`promote-to-primary!`),
           re-point the other replica at the new primary (best-effort — see note
           below), and switch the client's notion of the primary to the promoted
           node.
     3. Post-failover: SET against the NEW primary (liveness — it is writable) and
        read tracked keys back.

     4. Final reads (driven by `core/frogdb-test`'s final phase via this
        workload's `:final-generator`): read every durable and async key from the
        new primary.

   ## Two variants

   The base workload (`replication-failover`) asserts acked-write *survival on the
   promoted node* and primary *writability*. It does not require the replica set to
   re-form, so it stays meaningful even when the surviving replica never re-attaches.

   The `:reattach?` variant (`replication-failover-chain`) additionally requires the
   replica set to re-form **at runtime**: after promotion it waits for every
   surviving replica to report `master_link_status:up` against the promoted node
   (which requires the promoted node to serve `PSYNC` — either a `CONTINUE` inside
   the inherited history window or a full resync), then drives `SET` + `WAIT 1` and
   reads the promoted node's history back off the replica. This is the only harness
   that exercises the post-promotion partial-resync window under real concurrency,
   where a window computed one byte too wide shows up as a replica whose history
   silently disagrees with the primary's.

   ## Invariant asserted (the documented bound)

   `consistency.md` (Replication → durability during failover) states:

     | Write replicated before failover        | Preserved on the new primary |
     | Write acknowledged but not replicated    | Lost (bounded by replication lag) — [Design intent] |

   So the checker asserts the STRONG half exactly:

     Every write that was acknowledged by the full replica count (`WAIT 2` →
     `acked >= 2`, hence replicated to BOTH replicas before the kill) MUST be
     readable, with its written value, on the promoted primary after failover.
     The loss set for these writes MUST be empty.

   The principled carve-out is the WEAK half: async writes (no `WAIT`) that were
   never replicated MAY be lost — their loss is reported as INFORMATIONAL and
   never fails the checker. `WAIT 2` (full count) is used deliberately rather
   than `WAIT 1`: with only one acknowledging replica the promoted node might be
   the OTHER replica, a legitimate loss under the documented bound that would
   make the strong invariant non-deterministic. Full-count acknowledgement
   removes that ambiguity: whichever replica is promoted already holds the write.

   Operations:
   - :write-durable {:key k :value v} — SET + WAIT 2 on the current primary
   - :write-post    {:key k :value v} — SET on the new primary (post-failover
                                        liveness: the promoted node is writable)
   - :write-async   {:key k :value v} — SET on the current primary (may be lost)
   - :read          k                 — GET k from the current primary
   - :read-all      k                 — GET k from every node (convergence diag)
   - :kill-primary                    — docker stop the current primary
   - :promote                         — promote the freshest replica

   Reattach variant only:
   - :await-reattach                  — wait for the surviving replicas to link up
                                        against the promoted node
   - :write-post-durable {:key :value} — SET + WAIT 1 on the promoted node (proves
                                        it streams to the re-attached replica)
   - :read-replica  k                 — GET k from a surviving replica"
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [jepsen.frogdb.db :as db]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

;; ===========================================================================
;; State + key spaces
;; ===========================================================================

(def default-durable-count
  "Number of pre-failover durable (WAIT 2) keys to seed. A fixed, known range so
   the final-read phase can enumerate every tracked key without shared state."
  40)

(def required-replicas
  "Replica ACK count for a durable write. 2 = full replica count in the 3-node
   (1 primary + 2 replicas) topology, so an acked write is on BOTH replicas and
   therefore on whichever replica gets promoted."
  2)

;; Internal (container-network) port that peers use to replicate from each other.
(def internal-port 6379)

;; Shared, namespace-level pointer to the node currently acting as primary.
;; Jepsen calls `open!` once per worker thread, so per-client atoms would not be
;; shared; a namespace-level atom lets every worker route writes to the same
;; primary and lets the single `:promote` worker publish the new primary. Reset
;; in `setup!`.
(defonce current-primary (atom "n1"))

;; Nodes that were replicas and stayed up across the failover (i.e. every replica
;; except the promoted one). Published by `do-failover!` for the same reason as
;; `current-primary`. Empty until a promotion happens.
(defonce surviving-replicas (atom []))

(defn d-key [i] (str "jepsen-failover:d:" i))
(defn a-key [i] (str "jepsen-failover:a:" i))
(defn p-key [i] (str "jepsen-failover:p:" i))
(defn pd-key [i] (str "jepsen-failover:pd:" i))

;; ===========================================================================
;; Failover mechanics
;; ===========================================================================

(defn- replica-nodes
  "The two nodes that are NOT the given primary, in n1<n2<n3 order."
  [primary all-nodes]
  (remove #{primary} all-nodes))

(defn- pick-promotion-target
  "Among live replicas, choose the one with the highest replication offset (most
   up to date). Ties and unreachable-offset replicas fall back to node order.
   Returns a node name."
  [conns replicas]
  (let [scored (for [n replicas]
                 [n (try+
                      (or (frogdb/get-replication-offset (get conns n)) -1)
                      (catch Object _ -1))])
        reachable (seq (filter #(>= (second %) 0) scored))]
    (if reachable
      (->> reachable (sort-by (juxt (comp - second) first)) first first)
      ;; No offsets readable — just take the first replica in node order.
      (first replicas))))

(defn- do-failover!
  "Kill nothing here — the primary was already stopped by :kill-primary. Promote
   the freshest replica and re-point the other replica at it. Returns a map with
   :new-primary / :old-primary / :repointed."
  [conns all-nodes]
  (let [old-primary @current-primary
        replicas (replica-nodes old-primary all-nodes)
        new-primary (pick-promotion-target conns replicas)
        other-replicas (remove #{new-primary} replicas)
        new-conn (get conns new-primary)
        new-ip (get db/node-ips new-primary)]
    (info "Failover: promoting" new-primary "to primary (old primary" old-primary "down)")
    ;; Promote the chosen replica to primary.
    (frogdb/promote-to-primary! new-conn)
    ;; Tell every remaining replica to follow the new primary. The promoted node
    ;; serves PSYNC, so this re-attaches the replica at runtime; the reattach
    ;; variant asserts that it does (`:await-reattach`), while the base workload
    ;; treats it as best-effort so its durability verdict does not depend on the
    ;; replica set re-forming.
    (doseq [r other-replicas]
      (try+
        (info "Failover: re-pointing" r "at new primary" new-primary "(" new-ip ")")
        (frogdb/replicaof! (get conns r) new-ip (str internal-port))
        (catch Object e
          (warn "Failover: re-point of" r "failed:" (pr-str e)))))
    ;; Publish the new primary and the surviving replica set for all workers.
    (reset! surviving-replicas (vec other-replicas))
    (reset! current-primary new-primary)
    {:new-primary new-primary
     :old-primary old-primary
     :repointed (vec other-replicas)}))

(def reattach-timeout-ms
  "How long `:await-reattach` waits for the replica set to re-form. A full resync
   streams a RocksDB checkpoint, so this is sized for a checkpoint transfer on the
   docker network, not for a partial resync (which lands in well under a second)."
  60000)

(defn- link-status
  "`master_link_status` as reported by a node, or nil when unreadable."
  [conn]
  (try+
    (get (frogdb/info-replication conn) "master_link_status")
    (catch Object _ nil)))

(defn- reattached?
  "True when every surviving replica reports an up link and the promoted node
   counts them all in `connected_slaves` — i.e. the replica set re-formed against
   the promoted node without an operator restart."
  [conns primary replicas]
  (and (seq replicas)
       (every? #(= "up" (link-status (get conns %))) replicas)
       (>= (or (try+ (frogdb/get-connected-replicas (get conns primary))
                     (catch Object _ 0))
               0)
           (count replicas))))

(defn- await-reattach!
  "Poll until [[reattached?]] holds or `timeout-ms` elapses. Returns a boolean."
  [conns primary replicas timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (cond
        (reattached? conns primary replicas) true
        (< (System/currentTimeMillis) deadline) (do (Thread/sleep 500) (recur))
        :else false))))

;; ===========================================================================
;; Client
;; ===========================================================================

(defrecord FailoverClient [conns nodes docker-host? base-port]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          bp (get test :base-port frogdb/default-base-port)
          all-nodes (or (:nodes test) ["n1" "n2" "n3"])
          all-conns (frogdb/all-node-conns-single all-nodes docker? bp)]
      (info "Opening replication-failover client (docker?:" docker? ", nodes:" all-nodes ")")
      (assoc this
             :conns all-conns
             :nodes all-nodes
             :docker-host? docker?
             :base-port bp)))

  (setup! [this test]
    ;; Reset the shared failover state for this run.
    (reset! current-primary (first (or nodes ["n1"])))
    (reset! surviving-replicas [])
    (info "replication-failover: primary reset to" @current-primary)
    this)

  (invoke! [this test op]
    (case (:f op)
      ;; Durable write: SET + WAIT (full replica count) on the current primary.
      :write-durable
      (frogdb/with-error-handling op
        (let [{:keys [key value]} (:value op)
              pconn (get conns @current-primary)
              result (frogdb/write-durable! pconn key value required-replicas 5000)]
          (if (:timeout result)
            (assoc op :type :info :error :wait-timeout :acked (:acked result))
            (assoc op :type :ok :acked (:acked result)))))

      ;; Post-failover liveness write: plain SET on the new primary. This proves
      ;; the promoted node is a writable primary. It deliberately does NOT WAIT
      ;; for a replica, so the base workload's verdict stays about acked-write
      ;; survival even if the surviving replica never re-attaches; the reattach
      ;; variant covers replication-to-the-replica through :write-post-durable.
      :write-post
      (frogdb/with-error-handling op
        (let [{:keys [key value]} (:value op)
              pconn (get conns @current-primary)]
          (wcar pconn (car/set key (str value)))
          (assoc op :type :ok)))

      ;; Async write: SET only, no WAIT — may be lost across failover.
      :write-async
      (frogdb/with-error-handling op
        (let [{:keys [key value]} (:value op)
              pconn (get conns @current-primary)]
          (wcar pconn (car/set key (str value)))
          (assoc op :type :ok)))

      ;; Read a single key from the current primary.
      :read
      (frogdb/with-error-handling op
        (let [key (:value op)
              pconn (get conns @current-primary)
              v (frogdb/read-register pconn key)]
          (assoc op :type :ok :value {:key key :value v})))

      ;; Read a key from every node (convergence diagnostics).
      :read-all
      (frogdb/with-error-handling op
        (let [key (:value op)
              results (into {} (for [[n c] conns]
                                 [n (try+ (frogdb/read-register c key)
                                          (catch Object _ :unreachable))]))]
          (assoc op :type :ok :value {:key key :nodes results})))

      ;; Stop the current primary so it stays down during failover.
      :kill-primary
      (try+
        (let [prim @current-primary
              container (db/container-name prim :replication)]
          (info "replication-failover: stopping primary" prim "(" container ")")
          (db/docker-stop container)
          (assoc op :type :ok :value {:killed prim}))
        (catch Object e
          (warn "kill-primary failed:" (pr-str e))
          (assoc op :type :info :error [:kill-failed (str e)])))

      ;; Post-failover durable write (reattach variant): SET + WAIT 1 on the
      ;; promoted node. An ack can only come from a replica that re-attached and
      ;; is consuming the promoted node's stream, so this fails closed if PSYNC
      ;; against the promoted node does not work.
      :write-post-durable
      (frogdb/with-error-handling op
        (let [{:keys [key value]} (:value op)
              pconn (get conns @current-primary)
              result (frogdb/write-durable! pconn key value 1 5000)]
          (if (:timeout result)
            (assoc op :type :info :error :wait-timeout :acked (:acked result))
            (assoc op :type :ok :acked (:acked result)))))

      ;; Read a key off a surviving replica (reattach variant): the replica's copy
      ;; of the promoted node's history.
      :read-replica
      (frogdb/with-error-handling op
        (if-let [r (first @surviving-replicas)]
          (let [key (:value op)
                v (frogdb/read-register (get conns r) key)]
            (assoc op :type :ok :value {:key key :node r :value v}))
          (assoc op :type :fail :error :no-surviving-replica)))

      ;; Wait for the surviving replicas to re-attach to the promoted node.
      :await-reattach
      (try+
        (let [prim @current-primary
              replicas @surviving-replicas
              ok (await-reattach! conns prim replicas reattach-timeout-ms)
              detail {:primary prim
                      :replicas replicas
                      :connected (try+ (frogdb/get-connected-replicas (get conns prim))
                                       (catch Object _ nil))
                      :link-status (into {} (for [r replicas]
                                              [r (link-status (get conns r))]))}]
          (info "replication-failover: reattach" (if ok "succeeded" "TIMED OUT") detail)
          (assoc op :type (if ok :ok :fail) :value detail))
        (catch Object e
          (warn "await-reattach failed:" (pr-str e))
          (assoc op :type :info :error [:await-reattach-failed (str e)])))

      ;; Promote the freshest replica to primary.
      :promote
      (try+
        (let [result (do-failover! conns nodes)]
          (assoc op :type :ok :value result))
        (catch Object e
          (warn "promote failed:" (pr-str e))
          (assoc op :type :info :error [:promote-failed (str e)])))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (doseq [[_ c] conns] (frogdb/close-conn! c))))

(defn create-client [] (map->FailoverClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn generator
  "Self-driven failover generator.

   Options:
   - :rate          - ops/sec (default 10)
   - :durable-count - number of pre-failover durable keys (default 40)
   - :reattach?     - require the surviving replicas to re-attach to the promoted
                      node and drive replicated traffic through them"
  [opts]
  (let [rate (get opts :rate 10)
        reattach? (boolean (:reattach? opts))
        n (get opts :durable-count default-durable-count)
        dt (/ 1 (max 1 rate))
        ;; Pre-failover writes: interleave durable (WAIT 2) and async writes.
        durable-writes (map (fn [i] {:type :invoke :f :write-durable
                                     :value {:key (d-key i) :value i}})
                            (range n))
        async-writes (map (fn [i] {:type :invoke :f :write-async
                                   :value {:key (a-key i) :value i}})
                          (range n))
        baseline (interleave durable-writes async-writes)]
    (gen/phases
      ;; Phase 1 — baseline: seed all tracked writes on the original primary.
      (gen/log "Phase 1: seeding durable (WAIT 2) + async writes on primary")
      (->> baseline (gen/stagger dt))
      (gen/sleep 2)

      ;; Phase 2 — failover: kill primary, then promote a replica.
      (gen/log "Phase 2: killing primary")
      (gen/once {:type :invoke :f :kill-primary})
      (gen/sleep 3)
      (gen/log "Phase 2: promoting freshest replica")
      (gen/once {:type :invoke :f :promote})
      (gen/sleep 3)

      ;; Phase 2b (reattach variant) — the replica set must re-form against the
      ;; promoted node, which requires it to serve PSYNC.
      (when reattach?
        [(gen/log "Phase 2b: waiting for the surviving replica to re-attach to the promoted node")
         (gen/once {:type :invoke :f :await-reattach})])

      ;; Phase 3 — post-failover: prove the new primary accepts durable writes
      ;; and read tracked keys back through it.
      (gen/log "Phase 3: post-failover traffic against the new primary")
      (->> (gen/mix (cond-> [(let [c (atom 0)]
                               (fn [] {:type :invoke :f :write-post
                                       :value {:key (p-key (swap! c inc)) :value @c}}))
                             (fn [] {:type :invoke :f :read :value (d-key (rand-int n))})
                             (fn [] {:type :invoke :f :read-all :value (d-key (rand-int n))})]
                      reattach?
                      (conj (let [c (atom 0)]
                              (fn [] {:type :invoke :f :write-post-durable
                                      :value {:key (pd-key (swap! c inc)) :value @c}}))
                            (fn [] {:type :invoke :f :read-replica :value (d-key (rand-int n))}))))
           (gen/limit (if reattach? 50 30))
           (gen/stagger dt)))))

(defn final-generator
  "Final-read phase (invoked by core/frogdb-test): read every tracked durable and
   async key from the new primary so the checker can compute the loss sets."
  [opts]
  (let [n (get opts :durable-count default-durable-count)
        reattach? (boolean (:reattach? opts))]
    (->> (concat (map (fn [i] {:type :invoke :f :read :value (d-key i)}) (range n))
                 (map (fn [i] {:type :invoke :f :read :value (a-key i)}) (range n))
                 ;; Reattach variant: read the same durable keys off the replica so
                 ;; the checker can compare the two copies of the history.
                 (when reattach?
                   (map (fn [i] {:type :invoke :f :read-replica :value (d-key i)})
                        (range n))))
         (gen/stagger 0.02))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn- durable-acked-map
  "Map of durable key -> written value for every write acknowledged by the full
   replica count (acked >= required-replicas)."
  [history]
  (reduce (fn [m op]
            (assoc m (get-in op [:value :key]) (get-in op [:value :value])))
          {}
          (->> history
               (filter #(and (= :write-durable (:f %))
                             (= :ok (:type %))
                             (str/starts-with? (str (get-in % [:value :key])) "jepsen-failover:d:")
                             (>= (or (:acked %) 0) required-replicas))))))

(defn- async-ok-map
  "Map of async key -> written value for every async write that returned :ok."
  [history]
  (reduce (fn [m op]
            (assoc m (get-in op [:value :key]) (get-in op [:value :value])))
          {}
          (->> history
               (filter #(and (= :write-async (:f %)) (= :ok (:type %)))))))

(defn- final-read-map
  "Map of key -> last successfully-read value (history is in order, so a plain
   reduce/assoc leaves the last read per key)."
  [history]
  (reduce (fn [m op]
            (assoc m (get-in op [:value :key]) (get-in op [:value :value])))
          {}
          (->> history
               (filter #(and (= :read (:f %)) (= :ok (:type %)))))))

(defn- replica-read-map
  "Map of key -> last value read off a surviving replica."
  [history]
  (reduce (fn [m op]
            (assoc m (get-in op [:value :key]) (get-in op [:value :value])))
          {}
          (->> history
               (filter #(and (= :read-replica (:f %)) (= :ok (:type %)))))))

(defn checker
  "Assert: every full-replica-count-acknowledged write survives failover.

   :valid? requires
     - failover actually happened (primary killed + a replica promoted), so a
       green verdict is never vacuous;
     - the new primary accepted at least one post-failover durable write
       (liveness — promotion produced a writable primary);
     - the durable-acked loss set is empty (the documented strong invariant).

   Async-write loss is reported but never fails the checker (documented weak
   half: unreplicated writes may be lost).

   With `:reattach? true` it additionally requires
     - the surviving replicas re-attached to the promoted node at runtime;
     - at least one post-failover write was acknowledged by a replica (`WAIT 1`),
       so the promoted node is proven to be streaming, not just writable;
     - no replica read returned a *different* value than the promoted node
       acknowledged for that key — the signature of a partial-resync window
       computed too wide, where the replica keeps bytes the new history never
       contained.

   A replica read of `nil` for an acked key is counted separately and reported
   informationally: it means the replica has less of the history than the primary
   (missing data), not two disagreeing histories, and it can also come from a
   resync still in flight behind an up link."
  ([] (checker {}))
  ([copts]
   (reify checker/Checker
    (check [_ test history opts]
      (let [durable-acked (durable-acked-map history)
            async-ok (async-ok-map history)
            final-reads (final-read-map history)

            ;; Strong invariant: acked-and-replicated writes must survive.
            durable-loss (->> durable-acked
                              (keep (fn [[k v]]
                                      (let [rv (get final-reads k ::missing)]
                                        (when (not= v rv)
                                          {:key k :acked v
                                           :read (if (= rv ::missing) nil rv)}))))
                              (into []))

            ;; Weak/informational: async writes may or may not survive.
            async-loss (->> async-ok
                            (keep (fn [[k v]]
                                    (let [rv (get final-reads k ::missing)]
                                      (when (not= v rv) k))))
                            (into []))

            promote-op (->> history
                            (filter #(and (= :promote (:f %)) (= :ok (:type %))))
                            first)
            killed? (boolean (->> history
                                  (filter #(and (= :kill-primary (:f %)) (= :ok (:type %))))
                                  first))
            promoted? (boolean promote-op)
            new-primary (get-in promote-op [:value :new-primary])

            ;; Liveness: the promoted node accepted writes after failover (plain
            ;; SET :ok on the new primary). Proof that promotion produced a
            ;; writable primary — not a fenced/read-only zombie.
            post-writes-ok (->> history
                                (filter #(and (= :write-post (:f %))
                                              (= :ok (:type %))))
                                count)

            ;; --- Reattach variant ---
            reattach? (boolean (:reattach? copts))
            reattached? (boolean (->> history
                                      (filter #(and (= :await-reattach (:f %))
                                                    (= :ok (:type %))))
                                      first))
            ;; Writes the promoted node got a replica ACK for (WAIT 1).
            post-durable-acked (->> history
                                    (filter #(and (= :write-post-durable (:f %))
                                                  (= :ok (:type %))
                                                  (>= (or (:acked %) 0) 1)))
                                    count)
            replica-reads (replica-read-map history)
            ;; Two disagreeing histories: the replica holds a value the primary
            ;; never acknowledged for that key. This is what a partial-resync
            ;; window granted one byte too wide looks like from the outside.
            replica-divergence (->> durable-acked
                                    (keep (fn [[k v]]
                                            (let [rv (get replica-reads k ::unread)]
                                              (when (and (not= rv ::unread)
                                                         (some? rv)
                                                         (not= v rv))
                                                {:key k :primary v :replica rv}))))
                                    (into []))
            ;; Replica simply missing an acked key (reported, never fatal).
            replica-missing (->> durable-acked
                                 (keep (fn [[k _]]
                                         (let [rv (get replica-reads k ::unread)]
                                           (when (and (not= rv ::unread) (nil? rv)) k))))
                                 (into []))]
        (cond-> {:valid? (and killed?
                              promoted?
                              (pos? post-writes-ok)
                              (empty? durable-loss)
                              (or (not reattach?)
                                  (and reattached?
                                       (pos? post-durable-acked)
                                       (empty? replica-divergence))))
                 :failover-occurred? (and killed? promoted?)
                 :new-primary new-primary
                 :durable-acked-count (count durable-acked)
                 :durable-loss-count (count durable-loss)
                 ;; Cap the reported detail so results.edn stays readable on big losses.
                 :durable-loss (vec (take 20 durable-loss))
                 :post-failover-writes-ok post-writes-ok
                 :async-acked-count (count async-ok)
                 :async-loss-count (count async-loss)}
          reattach?
          (assoc :reattached? reattached?
                 :post-failover-replica-acked post-durable-acked
                 :replica-divergence-count (count replica-divergence)
                 :replica-divergence (vec (take 20 replica-divergence))
                 :replica-missing-count (count replica-missing))))))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a replication-failover workload.

   Options:
   - :rate          - operations per second
   - :durable-count - number of pre-failover durable keys
   - :reattach?     - require the surviving replicas to re-attach to the promoted
                      node (see the namespace docstring, \"Two variants\")"
  [opts]
  {:client (create-client)
   :generator (generator opts)
   :final-generator (final-generator opts)
   :checker (checker opts)})

(defn chain-workload
  "The reattach variant: the surviving replica must re-form the replica set
   against the promoted node, and the promoted node's history must reach it."
  [opts]
  (workload (assoc opts :reattach? true)))
