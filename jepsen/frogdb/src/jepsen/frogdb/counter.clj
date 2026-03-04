(ns jepsen.frogdb.counter
  "Counter workload for FrogDB.

   Tests the correctness of atomic increment operations (INCR/INCRBY).
   Verifies that the final counter value equals the sum of all successful
   increments, detecting any lost updates.

   Operations:
   - add: Increment the counter by a value
   - read: Read the current counter value

   Checker: Verifies that final value = sum of all successful increments."
  (:require [clojure.tools.logging :refer [info warn debug]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def counter-key "jepsen-counter")

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord CounterClient [conn node docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)]
      (info "Opening counter client to" node "(docker-host?:" docker? ")")
      (assoc this
             :conn (frogdb/conn-spec node frogdb/default-port docker?)
             :node node
             :docker-host? docker?)))

  (setup! [this test]
    ;; Initialize counter to 0
    (info "Setting up counter client, setting" counter-key "to 0")
    (wcar conn (car/set counter-key "0"))
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (case (:f op)
        :add
        (let [delta (:value op)]
          (frogdb/incr-counter! conn counter-key delta)
          (assoc op :type :ok))

        :read
        (let [value (frogdb/read-counter conn counter-key)]
          (assoc op :type :ok :value value)))))

  (teardown! [this test]
    nil)

  (close! [this test]
    nil))

(defn create-client
  "Create a new counter client."
  []
  (map->CounterClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn add-op
  "Generate an add operation with a random positive delta."
  []
  {:type :invoke :f :add :value (inc (rand-int 5))})

(defn read-op
  "Generate a read operation."
  []
  {:type :invoke :f :read :value nil})

(defn generator
  "Generator for counter operations.
   Primarily increments with occasional reads."
  [opts]
  (let [rate (get opts :rate 10)]
    (->> (gen/mix [(fn [] (add-op))
                   (fn [] (add-op))
                   (fn [] (add-op))
                   (fn [] (read-op))])
         (gen/stagger (/ 1 rate)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn extract-adds
  "Extract all successful add operations from history."
  [history]
  (->> history
       (filter #(and (= :add (:f %))
                     (= :ok (:type %))))
       (map :value)))

(defn sum-adds
  "Sum all add values from successful operations."
  [history]
  (reduce + 0 (extract-adds history)))

(defn final-reads
  "Get all successful read operations."
  [history]
  (->> history
       (filter #(and (= :read (:f %))
                     (= :ok (:type %))))
       (map :value)))

(defn checker
  "Counter checker.
   Verifies that the final counter value equals the sum of all successful adds."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [expected-sum (sum-adds history)
            reads (final-reads history)
            final-read (last reads)]
        (if (nil? final-read)
          {:valid? :unknown
           :error "No successful reads in history"}
          (let [valid? (= expected-sum final-read)]
            {:valid? valid?
             :expected expected-sum
             :actual final-read
             :difference (when (not valid?)
                           (- final-read expected-sum))
             :num-adds (count (extract-adds history))
             :num-reads (count reads)}))))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a counter workload.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (generator opts)
   :checker (checker)})
