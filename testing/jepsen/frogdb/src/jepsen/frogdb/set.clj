(ns jepsen.frogdb.set
  "Set workload for FrogDB.

   Tests set membership consistency using SADD/SMEMBERS operations.
   Uses Jepsen's built-in set-full checker which properly handles concurrent
   add operations and verifies the final read is consistent with acknowledged
   vs indeterminate adds.

   This workload verifies:
   - All acknowledged adds are present in the final read
   - No phantom elements (elements that were never added)
   - Lost vs recovered elements from indeterminate operations"
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def set-key "jepsen-set")

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
             :conn (frogdb/conn-spec-single node frogdb/default-port docker? base-port)
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
          (assoc op :type :ok))

        ;; Read all members
        :read
        (let [members (frogdb/smembers conn set-key)
              ;; Convert to sorted vector for checker
              int-members (sort (map #(Long/parseLong %) members))]
          (assoc op :type :ok :value int-members)))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (frogdb/close-conn! conn)))

(defn create-client
  "Create a new set client."
  []
  (map->SetClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn generator
  "Create a generator for set operations.
   Each add uses a unique monotonically-increasing value, which is required
   by checker/set-full for proper tracking of acknowledged vs lost elements.

   Options:
   - :rate - operations per second (default 10)"
  [opts]
  (let [rate (get opts :rate 10)
        counter (atom 0)]
    (->> (gen/mix [(fn [] {:type :invoke :f :add :value (swap! counter inc)})
                   (fn [] {:type :invoke :f :add :value (swap! counter inc)})
                   (fn [] {:type :invoke :f :add :value (swap! counter inc)})
                   (fn [] {:type :invoke :f :read :value nil})])
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

(defn checker
  "Set checker using Jepsen's built-in set-full checker.
   Properly handles concurrent operations by categorizing elements as:
   - :acknowledged — add was confirmed, element MUST be in final read
   - :lost — add was confirmed but element is missing (a bug)
   - :recovered — indeterminate add that actually made it in
   - :unexpected — element present but never added (phantom)"
  []
  (checker/set-full {:linearizable? true}))

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
   :checker (checker)})
