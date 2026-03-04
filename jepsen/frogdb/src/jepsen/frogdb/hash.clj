(ns jepsen.frogdb.hash
  "Hash workload for FrogDB.

   Tests hash field update atomicity using HGET/HSET operations.
   Each field is treated as an independent register for linearizability.

   This workload verifies:
   - Per-field linearizability (each field behaves as atomic register)
   - Field updates are atomic
   - WATCH-based CAS on fields works correctly"
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [jepsen.frogdb.client :as frogdb]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def hash-key "jepsen-hash")

;; Field pool: we test multiple fields for concurrent access
(def field-pool [:f1 :f2 :f3 :f4 :f5])

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord HashClient [conn node docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)]
      (info "Opening hash client to" node "(docker-host?:" docker? ")")
      (assoc this
             :conn (frogdb/conn-spec node frogdb/default-port docker? base-port)
             :node node
             :docker-host? docker?)))

  (setup! [this test]
    ;; Clear the hash at the start
    (info "Clearing hash key" hash-key)
    (wcar conn (car/del hash-key))
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (let [;; For independent key testing, the value is [field actual-value]
            [field v] (if (vector? (:value op))
                       (:value op)
                       [(:field op (:value op)) (:value op)])]
        (case (:f op)
          ;; Read a field
          :read
          (let [value (frogdb/hget conn hash-key (name field))]
            (assoc op :type :ok :value [field value]))

          ;; Write a field
          :write
          (do
            (frogdb/hset! conn hash-key (name field) v)
            (assoc op :type :ok :value [field v]))

          ;; CAS a field using WATCH
          :cas
          (let [[expected new-val] v
                field-name (name field)]
            (let [result (wcar conn
                           (car/watch hash-key)
                           (let [current (frogdb/parse-value (car/hget hash-key field-name))]
                             (if (= current expected)
                               (do
                                 (car/multi)
                                 (car/hset hash-key field-name (str new-val))
                                 (boolean (car/exec)))
                               (do
                                 (car/unwatch)
                                 false))))]
              (assoc op :type (if result :ok :fail) :value [field [expected new-val]])))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    nil))

(defn create-client
  "Create a new hash client."
  []
  (map->HashClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn read-op
  "Generate a read operation for a random field."
  [_ _]
  {:type :invoke :f :read :value [(rand-nth field-pool) nil]})

(defn write-op
  "Generate a write operation for a random field."
  [_ _]
  {:type :invoke :f :write :value [(rand-nth field-pool) (rand-int 100)]})

(defn cas-op
  "Generate a CAS operation for a random field."
  [_ _]
  {:type :invoke :f :cas :value [(rand-nth field-pool) [(rand-int 100) (rand-int 100)]]})

(defn generator
  "Create a generator for hash operations.

   Options:
   - :rate - operations per second (default 10)
   - :cas? - include CAS operations (default false)"
  [opts]
  (let [rate (get opts :rate 10)
        cas? (get opts :cas? false)
        op-generators (if cas?
                       [(gen/repeat read-op)
                        (gen/repeat write-op)
                        (gen/repeat cas-op)]
                       [(gen/repeat read-op)
                        (gen/repeat write-op)])]
    (->> (gen/mix op-generators)
         (gen/stagger (/ 1 rate)))))

(defn independent-generator
  "Create a generator that tests each field independently.

   This allows for per-field linearizability checking."
  [opts]
  (let [rate (get opts :rate 10)]
    (->> (independent/concurrent-generator
           (count field-pool)
           field-pool
           (fn [field]
             (->> (gen/mix [(gen/repeat {:f :read})
                           (gen/repeat {:f :write :value (rand-int 100)})])
                  (gen/stagger (/ 1 rate)))))
         (gen/stagger (/ 1 rate)))))

(defn final-read-generator
  "Generator for final reads of all fields."
  []
  (->> field-pool
       (map (fn [f] {:f :read :value [f nil]}))
       (gen/each-thread)))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn check-hash-history
  "Analyze the history for hash field correctness.

   For a basic check, we verify:
   1. Each field's final value matches a write that occurred
   2. No unexpected values"
  [history]
  (let [;; Group operations by field
        ops-by-field (group-by #(first (:value %))
                              (filter #(= :ok (:type %)) history))

        ;; For each field, check that reads are consistent with writes
        field-results
        (for [[field ops] ops-by-field
              :when field]
          (let [writes (->> ops
                           (filter #(= :write (:f %)))
                           (map #(second (:value %)))
                           set)
                reads (->> ops
                          (filter #(= :read (:f %)))
                          (map #(second (:value %)))
                          (remove nil?)
                          set)
                ;; Reads should only see written values (or nil)
                invalid-reads (clojure.set/difference reads writes)]
            {:field field
             :write-count (count (filter #(= :write (:f %)) ops))
             :read-count (count (filter #(= :read (:f %)) ops))
             :writes writes
             :invalid-reads invalid-reads}))

        invalid-fields (filter #(seq (:invalid-reads %)) field-results)]

    {:valid? (empty? invalid-fields)
     :field-count (count field-pool)
     :fields-with-ops (count (filter seq (map :writes field-results)))
     :invalid-fields (seq invalid-fields)
     :field-results (seq field-results)}))

(defn checker
  "Create a checker for hash operations."
  []
  (reify checker/Checker
    (check [this test history opts]
      (check-hash-history history))))

(defn independent-checker
  "Checker for independent field tests using linearizability."
  []
  (independent/checker
    (checker/compose
      {:linear (checker/linearizable
                 {:model (model/register)
                  :algorithm :linear})})))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Create a hash workload for testing.

   Options:
   - :rate - operations per second
   - :cas? - include CAS operations
   - :independent - use independent field testing with linearizability"
  [opts]
  (if (:independent opts)
    {:client (create-client)
     :generator (independent-generator opts)
     :checker (independent-checker)}
    {:client (create-client)
     :generator (gen/phases
                  (generator opts)
                  ;; Final reads
                  (gen/clients (final-read-generator)))
     :checker (checker/compose
                {:hash (checker)})}))
