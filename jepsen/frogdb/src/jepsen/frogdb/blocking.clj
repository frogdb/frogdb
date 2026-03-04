(ns jepsen.frogdb.blocking
  "Blocking operations workload for FrogDB.

   Tests BLPOP/BRPOP blocking semantics with timeouts.

   This workload verifies:
   - Each pushed element is popped exactly once
   - Blocking pop returns item or times out correctly
   - No stuck clients (blocking forever beyond timeout)
   - Order preserved (FIFO for items pushed by same process)"
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def queue-key "jepsen-blocking-queue")

;; Timeout for blocking operations (in seconds)
;; Keep short to avoid test delays
(def blocking-timeout 2)

;; ===========================================================================
;; Blocking Operations
;; ===========================================================================

(defn blpop!
  "Blocking pop from left (head) of list.
   Returns [key value] if item available, nil if timeout."
  [conn key timeout]
  (let [result (wcar conn (car/blpop key timeout))]
    ;; BLPOP returns [key value] or nil on timeout
    (when (and result (sequential? result) (= 2 (count result)))
      (second result))))

(defn brpop!
  "Blocking pop from right (tail) of list.
   Returns [key value] if item available, nil if timeout."
  [conn key timeout]
  (let [result (wcar conn (car/brpop key timeout))]
    (when (and result (sequential? result) (= 2 (count result)))
      (second result))))

(defn rpush!
  "Push value to the tail of a list using RPUSH."
  [conn key value]
  (wcar conn (car/rpush key (str value))))

(defn lpush!
  "Push value to the head of a list using LPUSH."
  [conn key value]
  (wcar conn (car/lpush key (str value))))

(defn lpop!
  "Non-blocking pop from left (head) of list."
  [conn key]
  (wcar conn (car/lpop key)))

(defn llen
  "Get list length."
  [conn key]
  (wcar conn (car/llen key)))

(defn lrange
  "Get elements in range."
  [conn key start stop]
  (wcar conn (car/lrange key start stop)))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord BlockingClient [conn node docker-host? push-counter]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)]
      (info "Opening blocking client to" node "(docker-host?:" docker? ")")
      (assoc this
             :conn (frogdb/conn-spec node frogdb/default-port docker? base-port)
             :node node
             :docker-host? docker?
             :push-counter (atom 0))))

  (setup! [this test]
    ;; Clear the queue at the start
    (info "Clearing blocking queue key" queue-key)
    (wcar conn (car/del queue-key))
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (case (:f op)
        ;; Push to queue (non-blocking)
        :push
        (let [process (:process op)
              seq-num (swap! push-counter inc)
              ;; Create unique value: "pN-M" where N is process, M is sequence
              value (str "p" process "-" seq-num)]
          (rpush! conn queue-key value)
          (assoc op :type :ok :value value))

        ;; Blocking pop from head
        :blocking-pop
        (let [start-time (System/currentTimeMillis)
              value (blpop! conn queue-key blocking-timeout)
              end-time (System/currentTimeMillis)
              duration-ms (- end-time start-time)]
          (if value
            (assoc op :type :ok :value (str value) :duration-ms duration-ms)
            (assoc op :type :ok :value nil :duration-ms duration-ms :timeout true)))

        ;; Non-blocking pop (immediate)
        :immediate-pop
        (let [value (lpop! conn queue-key)]
          (if (nil? value)
            (assoc op :type :ok :value nil)
            (assoc op :type :ok :value (str value))))

        ;; Read entire queue (for debugging/final verification)
        :read
        (let [values (lrange conn queue-key 0 -1)]
          (assoc op :type :ok :value (vec values))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    nil))

(defn create-client
  "Create a new blocking client."
  []
  (map->BlockingClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn push-op
  "Generate a push operation."
  [_ _]
  {:type :invoke :f :push :value nil})

(defn blocking-pop-op
  "Generate a blocking pop operation."
  [_ _]
  {:type :invoke :f :blocking-pop :value nil})

(defn immediate-pop-op
  "Generate an immediate (non-blocking) pop operation."
  [_ _]
  {:type :invoke :f :immediate-pop :value nil})

(defn generator
  "Create a generator for blocking operations.

   Options:
   - :rate - operations per second (default 10)

   We use more pushes than pops to ensure there are items for blocking
   pops to receive, and include some blocking pops to test the blocking
   behavior."
  [opts]
  (let [rate (get opts :rate 10)]
    (->> (gen/mix [(gen/repeat push-op)
                   (gen/repeat push-op)         ; Weight pushes more heavily
                   (gen/repeat push-op)
                   (gen/repeat blocking-pop-op)
                   (gen/repeat immediate-pop-op)])
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

(defn check-blocking-history
  "Analyze the history for blocking queue correctness.

   Checks:
   1. No duplicate pops (same value popped twice)
   2. Every popped value was pushed
   3. No lost items (every pushed item eventually popped or still in queue)
   4. Per-process FIFO ordering"
  [history]
  (let [;; Get all successful operations
        ops (->> history
                 (filter #(= :ok (:type %))))

        ;; Get all successful pushes
        pushed (->> ops
                    (filter #(= :push (:f %)))
                    (map :value)
                    vec)

        ;; Get all successful pops (both blocking and immediate, excluding nil/timeout)
        popped (->> ops
                    (filter #(#{:blocking-pop :immediate-pop} (:f %)))
                    (filter #(some? (:value %)))
                    (filter #(not (:timeout %)))
                    (map :value)
                    vec)

        ;; Get final queue state
        final-reads (->> ops
                         (filter #(= :read (:f %))))
        final-queue (when (seq final-reads)
                      (:value (last final-reads)))

        ;; Check for duplicates in popped
        pop-counts (frequencies popped)
        duplicates (filter #(> (val %) 1) pop-counts)

        ;; Check that all popped values were pushed
        pushed-set (set pushed)
        unknown-pops (remove pushed-set popped)

        ;; Check that all values are accounted for
        ;; (either popped or still in queue)
        popped-set (set popped)
        remaining-set (set final-queue)
        all-accounted (every? #(or (popped-set %)
                                   (remaining-set %))
                             pushed)

        ;; Check per-process FIFO ordering (same as queue workload)
        push-order (->> ops
                        (filter #(= :push (:f %)))
                        (map-indexed (fn [idx op] [(:value op) idx]))
                        (into {}))

        pop-order (->> ops
                       (filter #(#{:blocking-pop :immediate-pop} (:f %)))
                       (filter #(some? (:value %)))
                       (filter #(not (:timeout %)))
                       (map-indexed (fn [idx op] [(:value op) idx]))
                       (into {}))

        ordering-violations
        (let [parse-val (fn [v]
                         (let [[_ p s] (re-matches #"p(\d+)-(\d+)" (str v))]
                           (when (and p s)
                             {:process (Long/parseLong p)
                              :seq (Long/parseLong s)
                              :value v})))
              parsed-popped (keep parse-val popped)
              by-process (group-by :process parsed-popped)]
          (for [[process items] by-process
                :let [pairs (for [a items
                                  b items
                                  :when (and (< (:seq a) (:seq b))
                                            (pop-order (:value a))
                                            (pop-order (:value b)))]
                             [a b])]
                [a b] pairs
                :when (> (pop-order (:value a))
                        (pop-order (:value b)))]
            {:process process
             :first-pushed (:value a)
             :second-pushed (:value b)
             :but-popped-in-order [(:value b) (:value a)]}))

        ;; Count blocking operations and their outcomes
        blocking-ops (->> ops
                         (filter #(= :blocking-pop (:f %))))
        blocking-with-value (filter #(and (some? (:value %))
                                          (not (:timeout %)))
                                   blocking-ops)
        blocking-timeouts (filter :timeout blocking-ops)

        ;; Check for stuck clients (blocking ops that took way too long)
        ;; Timeout should be ~blocking-timeout seconds (+ some tolerance)
        max-expected-duration-ms (* (+ blocking-timeout 5) 1000)
        stuck-ops (filter #(and (:duration-ms %)
                               (> (:duration-ms %) max-expected-duration-ms))
                         blocking-ops)]

    {:valid? (and (empty? duplicates)
                  (empty? unknown-pops)
                  (empty? ordering-violations)
                  (empty? stuck-ops)
                  all-accounted)
     :push-count (count pushed)
     :total-pop-count (count popped)
     :blocking-pop-count (count blocking-ops)
     :blocking-with-value (count blocking-with-value)
     :blocking-timeouts (count blocking-timeouts)
     :immediate-pop-count (count (filter #(= :immediate-pop (:f %)) ops))
     :remaining-count (count final-queue)
     :duplicate-pops (into {} duplicates)
     :unknown-pops (seq unknown-pops)
     :ordering-violations (seq ordering-violations)
     :stuck-ops (count stuck-ops)
     :all-values-accounted all-accounted}))

(defn checker
  "Create a checker for blocking operations."
  []
  (reify checker/Checker
    (check [this test history opts]
      (check-blocking-history history))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Create a blocking operations workload for testing.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (gen/phases
                (generator opts)
                ;; Wait for any pending blocking ops to timeout
                (gen/sleep (+ blocking-timeout 2))
                ;; Final reads to see remaining queue state
                (gen/clients (final-read-generator)))
   :checker (checker/compose
              {:blocking (checker)})})
