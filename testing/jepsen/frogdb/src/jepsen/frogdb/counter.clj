(ns jepsen.frogdb.counter
  "Counter workload for FrogDB.

   Tests the correctness of atomic increment operations (INCR/INCRBY).
   Uses Jepsen's built-in counter checker which verifies monotonicity
   across ALL reads, not just the final value.

   Operations:
   - add: Increment the counter by a value
   - read: Read the current counter value

   Checker: Jepsen's built-in counter checker — verifies every read is
   consistent with the set of adds that could have completed by that point."
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
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)]
      (info "Opening counter client to" node "(docker-host?:" docker? ")")
      (assoc this
             :conn (frogdb/conn-spec-single node frogdb/default-port docker? base-port)
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
    (frogdb/close-conn! conn)))

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

(defn checker
  "Counter checker.
   Uses Jepsen's built-in counter checker which verifies that every read value
   is monotonically consistent with the set of adds that could have completed
   by that point — not just the final value."
  []
  (checker/counter))

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
