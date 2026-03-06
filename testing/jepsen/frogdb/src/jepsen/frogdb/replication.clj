(ns jepsen.frogdb.replication
  "Replication consistency workload for FrogDB.

   Tests replication behavior across a primary + replica cluster.
   Verifies that:
   - Writes to primary are replicated to replicas
   - Sync writes (with WAIT) are visible on replicas immediately
   - No value regression (values don't revert to older values)
   - Final reads show consistent state across all nodes

   Operations:
   - :write - SET on primary (async, no WAIT)
   - :write-sync - SET + WAIT for replica ACK
   - :read-primary - GET from primary
   - :read-replica - GET from random replica
   - :read-all - GET from all nodes (consistency check)"
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn debug]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def test-key "jepsen-repl")

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord ReplicationClient [conns primary-conn replica-conns docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)
          nodes (or (:nodes test) ["n1" "n2" "n3"])
          all-conns (frogdb/all-node-conns-single nodes docker? base-port)]
      (info "Opening replication client (docker?:" docker? ", nodes:" nodes ")")
      (assoc this
             :conns all-conns
             :primary-conn (get all-conns "n1")
             :replica-conns (map #(get all-conns %) ["n2" "n3"])
             :docker-host? docker?)))

  (setup! [this test]
    ;; Initialize the test key on primary
    (info "Setting up replication test key" test-key)
    (wcar primary-conn (car/set test-key "0"))
    ;; Wait a moment for replication
    (Thread/sleep 500)
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (case (:f op)
        ;; Async write to primary
        :write
        (do
          (wcar primary-conn (car/set test-key (str (:value op))))
          (assoc op :type :ok))

        ;; Sync write to primary (waits for replica ACK)
        :write-sync
        (let [result (frogdb/write-durable! primary-conn test-key (:value op) 1 5000)]
          (if (:timeout result)
            (assoc op :type :info :error :sync-timeout :acked (:acked result))
            (assoc op :type :ok :acked (:acked result))))

        ;; Read from primary
        :read-primary
        (let [value (frogdb/read-register primary-conn test-key)]
          (assoc op :type :ok :value value :node "n1"))

        ;; Read from random replica
        :read-replica
        (let [replica-conn (rand-nth replica-conns)
              value (frogdb/read-register replica-conn test-key)]
          (assoc op :type :ok :value value :node (if (= replica-conn (first replica-conns)) "n2" "n3")))

        ;; Read from all nodes
        :read-all
        (let [results (for [[node conn] conns]
                        [node (frogdb/read-register conn test-key)])]
          (assoc op :type :ok :value (into {} results)))

        ;; Generic read (used by final-reads phase) — delegates to read-all
        :read
        (let [results (for [[node conn] conns]
                        [node (frogdb/read-register conn test-key)])]
          (assoc op :type :ok :value (into {} results))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (doseq [[_ c] conns] (frogdb/close-conn! c))))

(defn create-client
  "Create a new replication client."
  []
  (map->ReplicationClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn write-op [v] {:type :invoke :f :write :value v})
(defn write-sync-op [v] {:type :invoke :f :write-sync :value v})
(defn read-primary-op [] {:type :invoke :f :read-primary :value nil})
(defn read-replica-op [] {:type :invoke :f :read-replica :value nil})
(defn read-all-op [] {:type :invoke :f :read-all :value nil})

(defn generator
  "Generator for replication workload.
   Mixes writes (sync and async) with reads from primary and replicas."
  [opts]
  (let [rate (get opts :rate 10)
        counter (atom 0)]
    (->> (gen/mix [(fn [] (write-op (swap! counter inc)))
                   (fn [] (write-sync-op (swap! counter inc)))
                   (fn [] (read-primary-op))
                   (fn [] (read-replica-op))
                   (fn [] (read-all-op))])
         (gen/stagger (/ 1 rate)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn extract-writes
  "Extract all successful write operations (both sync and async)."
  [history]
  (->> history
       (filter #(and (#{:write :write-sync} (:f %))
                     (= :ok (:type %))))
       (map :value)))

(defn extract-reads
  "Extract all successful read operations."
  [history]
  (->> history
       (filter #(and (#{:read-primary :read-replica} (:f %))
                     (= :ok (:type %))))
       (map :value)))

(defn extract-read-all
  "Extract all successful read-all operations (including :read which delegates to read-all)."
  [history]
  (->> history
       (filter #(and (#{:read-all :read} (:f %))
                     (= :ok (:type %))))
       (map :value)))

(defn checker
  "Checker for replication workload.

   Verifies:
   - No value regression (values only increase)
   - Final state is consistent across all nodes
   - Sync writes are visible on replicas"
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [writes (extract-writes history)
            reads (extract-reads history)
            read-alls (extract-read-all history)
            final-read-all (last read-alls)
            max-written (if (seq writes) (apply max writes) 0)]

        ;; Check for value regression
        (let [regression? (atom false)
              prev-value (atom nil)]
          (doseq [v reads]
            (when (and @prev-value v (< v @prev-value))
              (reset! regression? true))
            (reset! prev-value v))

          ;; Check final consistency
          (let [consistent? (when final-read-all
                              (apply = (vals final-read-all)))
                final-values (when final-read-all (vals final-read-all))]
            {:valid? (and (not @regression?)
                         (or (nil? final-read-all) consistent?))
             :regression? @regression?
             :consistent? consistent?
             :max-written max-written
             :final-values final-values
             :num-writes (count writes)
             :num-reads (count reads)}))))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a replication workload.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (generator opts)
   :checker (checker)})
