(ns jepsen.frogdb.list-append
  "Elle list-append workload for FrogDB.

   Uses RPUSH/LRANGE + Elle's cycle-based checker for strict serializability
   detection. This is the most powerful consistency checker in the suite.

   Elle generates transactions containing :r (read) and :append operations
   on integer-keyed lists. The checker looks for cycles in the dependency
   graph that would violate strict serializability.

   Key mapping: Elle integer keys -> Redis keys with hash tag {elle}
   to ensure all keys co-locate to the same slot in cluster mode.

   Operations:
   - :txn - A transaction containing [:r k nil] and [:append k v] micro-ops

   Checker: jepsen.tests.cycle.append (Elle)"
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.frogdb.client :as frogdb]
            [jepsen.frogdb.cluster-db :as cluster-db]
            [jepsen.frogdb.cluster-client :as cluster-client]
            [jepsen.tests.cycle.append :as append]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

;; ===========================================================================
;; Key Mapping
;; ===========================================================================

(defn elle-key
  "Map an Elle integer key to a Redis key with hash tag for slot co-location."
  [k]
  (str "{elle}" k))

;; ===========================================================================
;; Result Parsing
;; ===========================================================================

(defn parse-list
  "Parse an LRANGE result (list of strings) into a vector of longs."
  [xs]
  (when xs
    (mapv #(Long/parseLong %) xs)))

;; ===========================================================================
;; Single-Op Execution (no MULTI overhead)
;; ===========================================================================

(defn exec-single-op
  "Execute a single micro-op outside a transaction."
  [conn [f k v :as mop]]
  (case f
    :r      (let [result (wcar conn (car/lrange (elle-key k) 0 -1))]
              [[:r k (parse-list result)]])
    :append (do (wcar conn (car/rpush (elle-key k) (str v)))
                [[:append k v]])))

;; ===========================================================================
;; Multi-Op Execution (MULTI/EXEC)
;; ===========================================================================

(defn exec-multi-ops
  "Execute multiple micro-ops inside MULTI/EXEC.
   Returns the txn with reads filled in from EXEC results.

   Carmine's wcar returns the full pipeline reply vector:
     [\"OK\" \"QUEUED\" \"QUEUED\" ... [exec-result-1 exec-result-2 ...]]
   The EXEC results are the last element."
  [conn txn]
  (let [pipeline (wcar conn
                   (car/multi)
                   (doseq [[f k v] txn]
                     (case f
                       :r      (car/lrange (elle-key k) 0 -1)
                       :append (car/rpush (elle-key k) (str v))))
                   (car/exec))
        ;; Last element of the pipeline is the EXEC result vector
        exec-results (last pipeline)]
    (when (nil? exec-results)
      (throw+ {:type :txn-aborted}))
    ;; Map EXEC results back to txn format
    (mapv (fn [[f k v] result]
            (case f
              :r      [:r k (parse-list result)]
              :append [:append k v]))
          txn
          exec-results)))

;; ===========================================================================
;; Single-Node Client
;; ===========================================================================

(defrecord ElleListAppendClient [conn node docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)]
      (info "Opening Elle list-append client to" node "(docker-host?:" docker? ")")
      (assoc this
             :conn (frogdb/conn-spec-single node frogdb/default-port docker? base-port)
             :node node
             :docker-host? docker?)))

  (setup! [this test]
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (let [txn (:value op)]
        (if (> (count txn) 1)
          ;; Multi-op: wrap in MULTI/EXEC
          (try+
            (let [results (exec-multi-ops conn txn)]
              (assoc op :type :ok :value results))
            (catch [:type :txn-aborted] _
              (assoc op :type :info :error :txn-aborted)))
          ;; Single-op: execute directly
          (let [results (exec-single-op conn (first txn))]
            (assoc op :type :ok :value results))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (frogdb/close-conn! conn)))

;; ===========================================================================
;; Cluster Client
;; ===========================================================================

(defrecord ClusterElleListAppendClient [conn nodes docker-host? base-port slot-mapping]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          all-nodes (or (:cluster-nodes test) ["n1" "n2" "n3"])
          bp (get test :base-port cluster-db/default-base-port)
          sm (atom (cluster-client/create-slot-mapping all-nodes docker? bp))
          ;; Resolve the slot owner for {elle} keys and open a persistent conn
          slot (cluster-client/slot-for-key "{elle}")
          addr (cluster-client/get-node-for-slot @sm slot)
          [host port-str] (when addr (clojure.string/split addr #":"))
          port (when port-str (Integer/parseInt port-str))]
      (info "Opening cluster Elle list-append client to" node
            "(slot-owner:" addr ")")
      (assoc this
             :conn (when host (cluster-client/make-conn-single host port))
             :nodes all-nodes
             :docker-host? docker?
             :base-port bp
             :slot-mapping sm)))

  (setup! [this test]
    (when slot-mapping
      (reset! slot-mapping
              (cluster-client/refresh-slot-mapping @slot-mapping (first nodes) docker-host? base-port)))
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      ;; All {elle} keys route to the same slot, so find the owner and
      ;; execute there directly (MULTI/EXEC works on a single node).
      (let [txn (:value op)
            use-conn conn]
        (if-not use-conn
          (assoc op :type :info :error :no-slot-owner)
          (if (> (count txn) 1)
            (try+
              (let [results (exec-multi-ops use-conn txn)]
                (assoc op :type :ok :value results))
              (catch [:type :txn-aborted] _
                (assoc op :type :info :error :txn-aborted))
              (catch clojure.lang.ExceptionInfo e
                (if-let [redirect (cluster-client/is-redirect-error? e)]
                  (case (:type redirect)
                    :moved
                    (let [raw-addr (str (:host redirect) ":" (:port redirect))
                          remapped (cluster-client/remap-addr raw-addr base-port)]
                      (swap! slot-mapping assoc-in [:slots-to-nodes (:slot redirect)] remapped)
                      ;; Retry once with ad-hoc conn to the new owner
                      (let [[h ps] (clojure.string/split remapped #":")
                            fallback (cluster-client/make-conn h (Integer/parseInt ps))]
                        (try+
                          (let [results (exec-multi-ops fallback txn)]
                            (assoc op :type :ok :value results))
                          (catch [:type :txn-aborted] _
                            (assoc op :type :info :error :txn-aborted)))))
                    :clusterdown
                    (assoc op :type :info :error :clusterdown)
                    (throw e))
                  (throw e))))
            (try+
              (let [results (exec-single-op use-conn (first txn))]
                (assoc op :type :ok :value results))
              (catch clojure.lang.ExceptionInfo e
                (if-let [redirect (cluster-client/is-redirect-error? e)]
                  (case (:type redirect)
                    :moved
                    (let [raw-addr (str (:host redirect) ":" (:port redirect))
                          remapped (cluster-client/remap-addr raw-addr base-port)]
                      (swap! slot-mapping assoc-in [:slots-to-nodes (:slot redirect)] remapped)
                      (let [[h ps] (clojure.string/split remapped #":")
                            fallback (cluster-client/make-conn h (Integer/parseInt ps))
                            results (exec-single-op fallback (first txn))]
                        (assoc op :type :ok :value results)))
                    :clusterdown
                    (assoc op :type :info :error :clusterdown)
                    (throw e))
                  (throw e)))))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (frogdb/close-conn! conn)))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct an Elle list-append workload.

   Options:
   - :key-count          - number of distinct keys (default 12)
   - :min-txn-length     - minimum micro-ops per txn (default 1)
   - :max-txn-length     - maximum micro-ops per txn (default 4)
   - :max-writes-per-key - max appends per key (default 128)
   - :cluster            - if true, use cluster-aware client
   - :rate               - ops/sec (not used by Elle generator, but
                           passed through for compatibility)"
  [opts]
  (let [elle-opts {:key-count          (get opts :key-count 12)
                   :min-txn-length     (get opts :min-txn-length 1)
                   :max-txn-length     (get opts :max-txn-length 4)
                   :max-writes-per-key (get opts :max-writes-per-key 128)
                   :consistency-models [:strict-serializable]}
        test-map (append/test elle-opts)
        cluster? (or (:cluster opts)
                     (contains? #{:cluster-formation :leader-election
                                  :slot-migration :cross-slot :key-routing}
                                (keyword (:workload opts))))]
    {:client    (if cluster?
                  (map->ClusterElleListAppendClient {})
                  (map->ElleListAppendClient {}))
     :generator (:generator test-map)
     :checker   (:checker test-map)}))
