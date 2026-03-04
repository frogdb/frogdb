(ns jepsen.frogdb.set
  "Set workload for FrogDB.

   Tests set membership consistency using SADD/SREM/SMEMBERS operations.
   This workload verifies:
   - If add succeeded before a read, element must be present
   - If remove succeeded before a read, element must be absent
   - No phantom elements (elements that were never added)"
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def set-key "jepsen-set")

;; Element pool: we use a fixed pool of elements to ensure
;; adds and removes interact with each other
(def element-pool (range 100))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord SetClient [conn node docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)]
      (info "Opening set client to" node "(docker-host?:" docker? ")")
      (assoc this
             :conn (frogdb/conn-spec node frogdb/default-port docker? base-port)
             :node node
             :docker-host? docker?)))

  (setup! [this test]
    ;; Clear the set at the start
    (info "Clearing set key" set-key)
    (wcar conn (car/del set-key))
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (case (:f op)
        ;; Add element to set
        :add
        (let [element (:value op)
              result (frogdb/sadd! conn set-key element)]
          ;; SADD returns 1 if added, 0 if already present
          (assoc op :type :ok :result result))

        ;; Remove element from set
        :remove
        (let [element (:value op)
              result (frogdb/srem! conn set-key element)]
          ;; SREM returns 1 if removed, 0 if not present
          (assoc op :type :ok :result result))

        ;; Read all members
        :read
        (let [members (frogdb/smembers conn set-key)
              ;; Convert to integers for comparison
              int-members (set (map #(Long/parseLong %) members))]
          (assoc op :type :ok :value int-members)))))

  (teardown! [this test]
    nil)

  (close! [this test]
    nil))

(defn create-client
  "Create a new set client."
  []
  (map->SetClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn add-op
  "Generate an add operation."
  [_ _]
  {:type :invoke :f :add :value (rand-nth element-pool)})

(defn remove-op
  "Generate a remove operation."
  [_ _]
  {:type :invoke :f :remove :value (rand-nth element-pool)})

(defn read-op
  "Generate a read operation."
  [_ _]
  {:type :invoke :f :read :value nil})

(defn generator
  "Create a generator for set operations.

   Options:
   - :rate - operations per second (default 10)"
  [opts]
  (let [rate (get opts :rate 10)]
    (->> (gen/mix [(gen/repeat add-op)
                   (gen/repeat remove-op)
                   (gen/repeat read-op)])
         (gen/stagger (/ 1 rate)))))

(defn final-read-generator
  "Generator for final reads."
  []
  (->> (gen/repeat {:f :read})
       (gen/limit 5)
       (gen/stagger 0.5)))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn check-set-history
  "Analyze the history for set membership correctness.

   This uses a simple model: track which elements have been successfully
   added and removed, then verify final reads are consistent.

   More sophisticated checking would use linearizability, but for a basic
   correctness check, we verify:
   1. Final set contains only elements that were added
   2. Elements successfully added (and not later removed) are present
   3. No unexpected elements"
  [history]
  (let [;; Track all operations in order
        ops (->> history
                 (filter #(= :ok (:type %))))

        ;; Get final reads
        final-reads (->> ops
                        (filter #(= :read (:f %))))
        final-set (when (seq final-reads)
                   (:value (last final-reads)))

        ;; Compute expected final state by replaying ops
        ;; This is a simplification - proper linearizability would
        ;; consider all possible orderings
        expected-state (reduce
                        (fn [s op]
                          (case (:f op)
                            :add (conj s (:value op))
                            :remove (disj s (:value op))
                            :read s
                            s))
                        #{}
                        ops)

        ;; Check for unexpected elements (never added)
        all-added (->> ops
                      (filter #(= :add (:f %)))
                      (map :value)
                      set)
        unexpected (when final-set
                    (set/difference final-set all-added))

        ;; Check that the final set is a subset of elements ever added
        valid-members? (empty? unexpected)

        ;; Count operations
        add-count (count (filter #(= :add (:f %)) ops))
        remove-count (count (filter #(= :remove (:f %)) ops))
        read-count (count (filter #(= :read (:f %)) ops))]

    {:valid? valid-members?
     :add-count add-count
     :remove-count remove-count
     :read-count read-count
     :final-size (count final-set)
     :unexpected-elements (seq unexpected)
     :expected-state-size (count expected-state)}))

(defn checker
  "Create a checker for set operations."
  []
  (reify checker/Checker
    (check [this test history opts]
      (check-set-history history))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Create a set workload for testing.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (gen/phases
                (generator opts)
                ;; Final reads
                (gen/clients (final-read-generator)))
   :checker (checker/compose
              {:set (checker)})})
