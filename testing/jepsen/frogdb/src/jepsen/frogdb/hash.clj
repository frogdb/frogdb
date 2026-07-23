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
             :conn (frogdb/conn-spec-single node frogdb/default-port docker? base-port)
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
    (frogdb/close-conn! conn)))

(defn create-client
  "Create a new hash client."
  []
  (map->HashClient {}))

;; ===========================================================================
;; Independent Client
;; ===========================================================================
;;
;; The independent path splits the hash into one linearizable register per
;; field (each field of the single `hash-key` is checked independently). The
;; jepsen.independent generator wraps every op's :value in a MapEntry tuple
;; [field sub-value] and jepsen.independent/checker groups the history by that
;; tuple key. The plain HashClient replaces :value with a *vector* [field v],
;; which `jepsen.independent/tuple?` does NOT recognise (it requires a
;; clojure.lang.MapEntry) — so the linearizable subhistory grouping silently
;; collapses. This dedicated client returns proper `independent/tuple` values
;; (like register.clj's IndependentRegisterClient) so the per-field
;; linearizable checker actually gets keyed subhistories.

(defrecord IndependentHashClient [conn node docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)]
      (info "Opening independent hash client to" node "(docker-host?:" docker? ")")
      (assoc this
             :conn (frogdb/conn-spec-single node frogdb/default-port docker? base-port)
             :node node
             :docker-host? docker?)))

  (setup! [this test]
    (info "Clearing hash key" hash-key)
    (wcar conn (car/del hash-key))
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (let [[field v] (:value op)]
        (if (nil? field)
          ;; Defensive: a value-less op (e.g. a bare {:f :read}) cannot be
          ;; attributed to a field. Fail it rather than NPE on (name nil).
          (assoc op :type :fail :error :no-field)
          (let [field-name (name field)]
            (case (:f op)
              :read
              (let [value (frogdb/hget conn hash-key field-name)]
                (assoc op :type :ok :value (independent/tuple field value)))

              :write
              (do
                (frogdb/hset! conn hash-key field-name v)
                ;; Preserve the generator's MapEntry tuple in :value.
                (assoc op :type :ok))))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (frogdb/close-conn! conn)))

(defn create-independent-client
  "Create a new independent hash client."
  []
  (map->IndependentHashClient {}))

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

(defn ind-read-op
  "Independent read op (value is added by the independent generator wrapper)."
  [_ _]
  {:type :invoke :f :read})

(defn ind-write-op
  "Independent write op with a fresh random value each invocation."
  [_ _]
  {:type :invoke :f :write :value (rand-int 100)})

(defn independent-generator
  "Create a generator that tests each field independently for per-field
   linearizability.

   `:threads-per-field` (default 2) worker threads hammer each field
   concurrently — genuine concurrency per key is required for the linearizable
   checker to find anomalies. The enclosing test's :concurrency must be a
   multiple of this value (see run.py hash-independent TestDefinition, which
   sets --concurrency 10 for the 5-field pool)."
  [opts]
  (let [rate (get opts :rate 10)
        threads-per-field (get opts :threads-per-field 2)]
    (independent/concurrent-generator
      threads-per-field
      field-pool
      (fn [_field]
        (->> (gen/mix [ind-read-op ind-write-op])
             (gen/stagger (/ 1 rate)))))))

(defn independent-final-read-generator
  "Final reads for the independent path, one per field, wrapped as proper
   `independent/tuple` values so jepsen.independent/checker attributes them to
   the right per-field subhistory (a bare {:f :read} would be un-keyed)."
  []
  (->> field-pool
       (map (fn [f] {:type :invoke :f :read :value (independent/tuple f nil)}))
       (gen/each-thread)))

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
    {:client (create-independent-client)
     :generator (independent-generator opts)
     :final-generator (independent-final-read-generator)
     :checker (independent-checker)}
    {:client (create-client)
     :generator (gen/phases
                  (generator opts)
                  ;; Final reads
                  (gen/clients (final-read-generator)))
     :checker (checker/compose
                {:hash (checker)})}))
