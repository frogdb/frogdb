(ns jepsen.frogdb.queue
  "Queue workload for FrogDB.

   Tests list operations for queue semantics (FIFO ordering).
   Uses RPUSH to enqueue at the tail and LPOP to dequeue from the head.

   This workload verifies:
   - Items dequeued in the order they were enqueued (FIFO)
   - No duplicate dequeues
   - No lost items (every enqueued item eventually dequeued or still in queue)"
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def queue-key "jepsen-queue")

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord QueueClient [conn node docker-host? enqueue-counter]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)]
      (info "Opening queue client to" node "(docker-host?:" docker? ")")
      (assoc this
             :conn (frogdb/conn-spec node frogdb/default-port docker? base-port)
             :node node
             :docker-host? docker?
             :enqueue-counter (atom 0))))

  (setup! [this test]
    ;; Clear the queue at the start
    (info "Clearing queue key" queue-key)
    (wcar conn (car/del queue-key))
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (case (:f op)
        ;; Enqueue: push to tail with RPUSH
        :enqueue
        (let [process (:process op)
              seq-num (swap! enqueue-counter inc)
              ;; Create unique value: "pN-M" where N is process, M is sequence
              value (str "p" process "-" seq-num)]
          (frogdb/rpush! conn queue-key value)
          (assoc op :type :ok :value value))

        ;; Dequeue: pop from head with LPOP
        :dequeue
        (let [value (frogdb/lpop! conn queue-key)]
          (if (nil? value)
            (assoc op :type :ok :value nil)
            (assoc op :type :ok :value (str value))))

        ;; Peek: look at head without removing
        :peek
        (let [value (frogdb/lindex conn queue-key 0)]
          (assoc op :type :ok :value (when value (str value))))

        ;; Read entire queue (for debugging/final verification)
        :read
        (let [values (frogdb/lrange conn queue-key 0 -1)]
          (assoc op :type :ok :value (vec values))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    nil))

(defn create-client
  "Create a new queue client."
  []
  (map->QueueClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn enqueue-op
  "Generate an enqueue operation."
  [_ _]
  {:type :invoke :f :enqueue :value nil})

(defn dequeue-op
  "Generate a dequeue operation."
  [_ _]
  {:type :invoke :f :dequeue :value nil})

(defn generator
  "Create a generator for queue operations.

   Options:
   - :rate - operations per second (default 10)
   - :enqueue-fraction - fraction of enqueues vs dequeues (default 0.6)"
  [opts]
  (let [rate (get opts :rate 10)]
    (->> (gen/mix [(gen/repeat enqueue-op)
                   (gen/repeat dequeue-op)])
         (gen/stagger (/ 1 rate)))))

(defn final-read-generator
  "Generator for final reads to see remaining queue state."
  []
  (->> (gen/repeat {:f :read})
       (gen/limit 5)
       (gen/stagger 0.5)))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn check-queue-history
  "Analyze the history for queue (FIFO) correctness.

   For a queue, we need to verify that items are dequeued in FIFO order.
   This is a relaxed consistency check since concurrent operations make
   strict FIFO difficult to verify.

   We check:
   1. No duplicate dequeues (same value dequeued twice)
   2. Every dequeued value was enqueued
   3. Ordering: if both A and B were enqueued by the same process,
      and A was enqueued before B, then A should be dequeued before B
      (unless one wasn't dequeued at all)"
  [history]
  (let [;; Get all successful enqueues
        enqueued (->> history
                      (filter #(and (= :enqueue (:f %))
                                   (= :ok (:type %))))
                      (map :value)
                      vec)

        ;; Get all successful dequeues (excluding nil/empty)
        dequeued (->> history
                      (filter #(and (= :dequeue (:f %))
                                   (= :ok (:type %))
                                   (some? (:value %))))
                      (map :value)
                      vec)

        ;; Get final queue state
        final-reads (->> history
                         (filter #(and (= :read (:f %))
                                      (= :ok (:type %)))))
        final-queue (when (seq final-reads)
                     (:value (last final-reads)))

        ;; Check for duplicates in dequeued
        dequeue-counts (frequencies dequeued)
        duplicates (filter #(> (val %) 1) dequeue-counts)

        ;; Check that all dequeued values were enqueued
        enqueued-set (set enqueued)
        unknown-dequeues (remove enqueued-set dequeued)

        ;; Check that all values are accounted for
        ;; (either dequeued or still in queue)
        dequeued-set (set dequeued)
        remaining-set (set final-queue)
        all-accounted (every? #(or (dequeued-set %)
                                   (remaining-set %))
                             enqueued)

        ;; Check per-process FIFO ordering
        ;; Group by process, check order preserved
        enqueue-order (->> history
                           (filter #(and (= :enqueue (:f %))
                                        (= :ok (:type %))))
                           (map-indexed (fn [idx op] [(:value op) idx]))
                           (into {}))

        dequeue-order (->> history
                           (filter #(and (= :dequeue (:f %))
                                        (= :ok (:type %))
                                        (some? (:value %))))
                           (map-indexed (fn [idx op] [(:value op) idx]))
                           (into {}))

        ;; For any two values from same process that were both dequeued,
        ;; if one was enqueued before the other, it should be dequeued first
        ordering-violations
        (let [;; Parse values to extract process
              parse-val (fn [v]
                         (let [[_ p s] (re-matches #"p(\d+)-(\d+)" (str v))]
                           (when (and p s)
                             {:process (Long/parseLong p)
                              :seq (Long/parseLong s)
                              :value v})))
              parsed-dequeued (keep parse-val dequeued)
              by-process (group-by :process parsed-dequeued)]
          (for [[process items] by-process
                :let [pairs (for [a items
                                  b items
                                  :when (and (< (:seq a) (:seq b))
                                            (dequeue-order (:value a))
                                            (dequeue-order (:value b)))]
                             [a b])]
                [a b] pairs
                :when (> (dequeue-order (:value a))
                        (dequeue-order (:value b)))]
            {:process process
             :first-enqueued (:value a)
             :second-enqueued (:value b)
             :but-dequeued-in-order [(:value b) (:value a)]}))]

    {:valid? (and (empty? duplicates)
                  (empty? unknown-dequeues)
                  (empty? ordering-violations)
                  all-accounted)
     :enqueue-count (count enqueued)
     :dequeue-count (count dequeued)
     :remaining-count (count final-queue)
     :duplicate-dequeues (into {} duplicates)
     :unknown-dequeues (seq unknown-dequeues)
     :ordering-violations (seq ordering-violations)
     :all-values-accounted all-accounted}))

(defn checker
  "Create a checker for queue operations."
  []
  (reify checker/Checker
    (check [this test history opts]
      (check-queue-history history))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Create a queue workload for testing.

   Options:
   - :rate - operations per second
   - :enqueue-fraction - fraction of enqueues vs dequeues"
  [opts]
  {:client (create-client)
   :generator (gen/phases
                (generator opts)
                ;; Final reads to see remaining queue state
                (gen/clients (final-read-generator)))
   :checker (checker/compose
              {:queue (checker)})})
