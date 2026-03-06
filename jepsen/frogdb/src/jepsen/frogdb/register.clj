(ns jepsen.frogdb.register
  "Register workload for FrogDB.

   Tests linearizability of GET/SET operations on single keys.
   This is the fundamental consistency test for any key-value store.

   Operations:
   - read: GET the value of a register
   - write: SET the value of a register
   - cas: Compare-and-swap (using WATCH/MULTI/EXEC)

   Checker: Uses Jepsen's linearizable checker to verify that all
   operations form a valid linearizable history."
  (:require [clojure.tools.logging :refer [info warn debug]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [jepsen.frogdb.client :as frogdb]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def register-key "jepsen-register")

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord RegisterClient [conn node docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)]
      (info "Opening register client to" node "(docker-host?:" docker? ")")
      (assoc this
             :conn (frogdb/conn-spec-single node frogdb/default-port docker? base-port)
             :node node
             :docker-host? docker?)))

  (setup! [this test]
    ;; Initialize the register to nil
    (info "Setting up register client, deleting key" register-key)
    (wcar conn (car/del register-key))
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (case (:f op)
        :read
        (let [value (frogdb/read-register conn register-key)]
          (assoc op :type :ok :value value))

        :write
        (do
          (frogdb/write-register! conn register-key (:value op))
          (assoc op :type :ok))

        :cas
        (let [[expected new-value] (:value op)
              success? (frogdb/cas-register! conn register-key expected new-value)]
          (assoc op :type (if success? :ok :fail))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (frogdb/close-conn! conn)))

(defn create-client
  "Create a new register client."
  []
  (map->RegisterClient {}))

;; ===========================================================================
;; Independent Key Client
;; ===========================================================================

(defrecord IndependentRegisterClient [conn node docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)]
      (info "Opening independent register client to" node "(docker-host?:" docker? ")")
      (assoc this
             :conn (frogdb/conn-spec-single node frogdb/default-port docker? base-port)
             :node node
             :docker-host? docker?)))

  (setup! [this test]
    this)

  (invoke! [this test op]
    (let [[k v] (:value op)
          key-name (str "jepsen-reg-" k)]
      (frogdb/with-error-handling op
        (case (:f op)
          :read
          (let [value (frogdb/read-register conn key-name)]
            (assoc op :type :ok :value (independent/tuple k value)))

          :write
          (do
            (frogdb/write-register! conn key-name v)
            (assoc op :type :ok))

          :cas
          (let [[expected new-value] v
                success? (frogdb/cas-register! conn key-name expected new-value)]
            (assoc op :type (if success? :ok :fail)))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (frogdb/close-conn! conn)))

(defn create-independent-client
  "Create a new independent register client."
  []
  (map->IndependentRegisterClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn r [] {:type :invoke :f :read :value nil})
(defn w [v] {:type :invoke :f :write :value v})
(defn cas [expected new-value] {:type :invoke :f :cas :value [expected new-value]})

(defn generator
  "Generator for register operations.
   Mixes reads and writes with occasional CAS operations."
  [opts]
  (let [rate (get opts :rate 10)]
    (->> (gen/mix [(fn [] (r))
                   (fn [] (w (rand-int 100)))
                   (fn [] (cas (rand-int 100) (rand-int 100)))])
         (gen/stagger (/ 1 rate)))))

(defn independent-generator
  "Generator for independent (multi-key) register operations."
  [opts]
  (let [rate (get opts :rate 10)
        key-count (get opts :key-count 10)]
    (->> (independent/concurrent-generator
           key-count
           (range)
           (fn [k]
             (gen/mix [(fn [] (r))
                       (fn [] (w (rand-int 100)))
                       (fn [] (cas (rand-int 100) (rand-int 100)))])))
         (gen/stagger (/ 1 rate)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn checker
  "Checker for single-key register workload."
  []
  (checker/linearizable {:model (model/cas-register)
                         :algorithm :linear}))

(defn independent-checker
  "Checker for multi-key register workload."
  []
  (independent/checker
    (checker/linearizable {:model (model/cas-register)
                           :algorithm :linear})))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a register workload.

   Options:
   - :independent - if true, test multiple independent keys
   - :rate - operations per second"
  [opts]
  (if (:independent opts)
    {:client (create-independent-client)
     :generator (independent-generator opts)
     :checker (independent-checker)}
    {:client (create-client)
     :generator (generator opts)
     :checker (checker)}))
