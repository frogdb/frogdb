(ns jepsen.frogdb.elle-rw-register
  "Elle rw-register workload for FrogDB.

   Uses Elle's cycle-based checker to verify linearizability (or weaker models)
   of read/write register operations.  This is more powerful than the
   Knossos-based :register workload because:
   - Elle is significantly faster on large histories
   - Elle detects a broader class of anomalies via cycle detection
   - Elle can check for various consistency models simultaneously

   Operates on integer-keyed registers.  Single-op transactions execute as
   plain GET/SET; multi-op transactions use MULTI/EXEC.  All keys use the
   hash tag {elle-rw} so they co-locate to one slot in cluster mode.

   Operations:
   - :txn - A transaction containing [:r k nil] and [:w k v] micro-ops

   Checker: jepsen.tests.cycle.wr (Elle rw-register)"
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [jepsen.frogdb.cluster-db :as cluster-db]
            [jepsen.frogdb.cluster-client :as cluster-client]
            [jepsen.tests.cycle.wr :as wr]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

;; ===========================================================================
;; Key Mapping
;; ===========================================================================

(defn elle-key
  "Map an Elle integer key to a Redis key with hash tag for slot co-location."
  [k]
  (str "{elle-rw}" k))

;; ===========================================================================
;; Single-Op Execution
;; ===========================================================================

(defn exec-single-op
  "Execute a single micro-op outside MULTI/EXEC."
  [conn [f k v :as mop]]
  (case f
    :r (let [raw (wcar conn (car/get (elle-key k)))]
         [[:r k (frogdb/parse-value raw)]])
    :w (do (wcar conn (car/set (elle-key k) (str v)))
           [[:w k v]])))

;; ===========================================================================
;; Multi-Op Execution (MULTI/EXEC)
;; ===========================================================================

(defn exec-multi-ops
  "Execute multiple micro-ops inside MULTI/EXEC.
   Returns txn with reads filled in from EXEC results."
  [conn txn]
  (let [pipeline (wcar conn
                   (car/multi)
                   (doseq [[f k v] txn]
                     (case f
                       :r (car/get (elle-key k))
                       :w (car/set (elle-key k) (str v))))
                   (car/exec))
        exec-results (last pipeline)]
    (when (nil? exec-results)
      (throw+ {:type :txn-aborted}))
    (mapv (fn [[f k v] result]
            (case f
              :r [:r k (frogdb/parse-value result)]
              :w [:w k v]))
          txn
          exec-results)))

;; ===========================================================================
;; Single-Node Client
;; ===========================================================================

(defrecord ElleRwRegisterClient [conn node docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)]
      (info "Opening Elle rw-register client to" node "(docker-host?:" docker? ")")
      (assoc this
             :conn (frogdb/conn-spec-single node frogdb/default-port docker? base-port)
             :node node
             :docker-host? docker?)))

  (setup! [this test]
    this)

  (invoke! [this test op]
    (if (= :read (:f op))
      ;; Handle final-reads phase from core.clj — not an Elle txn, just ack it
      (assoc op :type :ok :value [])
      (frogdb/with-error-handling op
        (let [txn (:value op)]
          (if (> (count txn) 1)
            (try+
              (let [results (exec-multi-ops conn txn)]
                (assoc op :type :ok :value results))
              (catch [:type :txn-aborted] _
                (assoc op :type :info :error :txn-aborted)))
            (let [results (exec-single-op conn (first txn))]
              (assoc op :type :ok :value results)))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (frogdb/close-conn! conn)))

;; ===========================================================================
;; Cluster Client
;; ===========================================================================

(defrecord ClusterElleRwRegisterClient [conn nodes docker-host? base-port slot-mapping]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          all-nodes (or (:cluster-nodes test) ["n1" "n2" "n3"])
          bp (get test :base-port cluster-db/default-base-port)
          sm (atom (cluster-client/create-slot-mapping all-nodes docker? bp))
          ;; Resolve slot owner for {elle-rw} keys
          slot (cluster-client/slot-for-key "{elle-rw}")
          addr (cluster-client/get-node-for-slot @sm slot)
          [host port-str] (when addr (str/split addr #":"))
          port (when port-str (Integer/parseInt port-str))]
      (info "Opening cluster Elle rw-register client to" node
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
    (if (= :read (:f op))
      ;; Handle final-reads phase from core.clj — not an Elle txn, just ack it
      (assoc op :type :ok :value [])
      (frogdb/with-error-handling op
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
                      (let [[h ps] (str/split remapped #":")
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
                      (let [[h ps] (str/split remapped #":")
                            fallback (cluster-client/make-conn h (Integer/parseInt ps))
                            results (exec-single-op fallback (first txn))]
                        (assoc op :type :ok :value results)))
                    :clusterdown
                    (assoc op :type :info :error :clusterdown)
                    (throw e))
                  (throw e))))))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (frogdb/close-conn! conn)))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct an Elle rw-register workload.

   Options:
   - :key-count          - number of distinct keys (default 6)
   - :min-txn-length     - minimum micro-ops per txn (default 1)
   - :max-txn-length     - maximum micro-ops per txn (default 4)
   - :max-writes-per-key - max writes per key (default 128)
   - :consistency-models - consistency models to check (default [:strict-serializable])
   - :cluster            - if true, use cluster-aware client
   - :rate               - ops/sec"
  [opts]
  (let [elle-opts {:key-count          (get opts :key-count 6)
                   :min-txn-length     (get opts :min-txn-length 1)
                   :max-txn-length     (get opts :max-txn-length 4)
                   :max-writes-per-key (get opts :max-writes-per-key 128)
                   :consistency-models (get opts :consistency-models [:strict-serializable])}
        test-map (wr/test elle-opts)
        cluster? (or (:cluster opts)
                     (contains? #{:cluster-formation :leader-election
                                  :slot-migration :cross-slot :key-routing
                                  :migration-recovery :concurrent-migration
                                  :membership-routing :rolling-restart}
                                (keyword (:workload opts))))]
    {:client    (if cluster?
                  (map->ClusterElleRwRegisterClient {})
                  (map->ElleRwRegisterClient {}))
     :generator (:generator test-map)
     :final-generator (->> (gen/repeat {:f :txn :value (mapv (fn [k] [:r k nil])
                                                              (range (get elle-opts :key-count 6)))})
                           (gen/limit 10)
                           (gen/stagger 0.1))
     :checker   (:checker test-map)}))
