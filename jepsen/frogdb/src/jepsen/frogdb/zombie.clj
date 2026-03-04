(ns jepsen.frogdb.zombie
  "Zombie primary detection workload for FrogDB.

   Tests scenarios where an old primary continues accepting writes
   after being partitioned (a 'zombie' primary). These writes should
   be lost when the partition heals.

   Verifies:
   - Writes with replica ACK (durable) survive partition
   - Writes without ACK (async) may be lost
   - No zombie writes appear after partition heals

   Operations:
   - :write-durable - Write with WAIT (durable if ACK received)
   - :write-async - Write without WAIT (may be lost)
   - :read-quorum - Read from majority of nodes"
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn debug]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def test-key "jepsen-zombie")

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord ZombieClient [conns primary-conn docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)
          nodes (or (:nodes test) ["n1" "n2" "n3"])
          all-conns (frogdb/all-node-conns nodes docker? base-port)
          primary (frogdb/conn-for-node "n1" docker? base-port)]
      (info "Opening zombie client (docker?:" docker? ", nodes:" nodes ")")
      (assoc this
             :conns all-conns
             :primary-conn primary
             :docker-host? docker?)))

  (setup! [this test]
    ;; Initialize the test key on primary
    (info "Setting up zombie test key" test-key)
    (wcar primary-conn (car/set test-key "0"))
    (Thread/sleep 500)
    this)

  (invoke! [this test op]
    (case (:f op)
      ;; Durable write with WAIT
      :write-durable
      (let [value (:value op)]
        (try+
          (let [result (frogdb/write-durable! primary-conn test-key value 1 2000)]
            (if (:timeout result)
              (assoc op :type :info :error :sync-timeout :acked (:acked result) :value value)
              (assoc op :type :ok :acked (:acked result) :value value)))
          (catch java.net.ConnectException e
            (assoc op :type :fail :error :connection-refused))
          (catch java.net.SocketTimeoutException e
            (assoc op :type :info :error :timeout))
          (catch Exception e
            (assoc op :type :info :error [:unexpected (.getMessage e)]))))

      ;; Async write without WAIT
      :write-async
      (let [value (:value op)]
        (try+
          (wcar primary-conn (car/set test-key (str value)))
          (assoc op :type :ok :value value :durable false)
          (catch java.net.ConnectException e
            (assoc op :type :fail :error :connection-refused))
          (catch Exception e
            (assoc op :type :info :error [:unexpected (.getMessage e)]))))

      ;; Read from majority (quorum)
      :read-quorum
      (let [results (for [[node conn] conns]
                      (try+
                        {:node node :value (frogdb/read-register conn test-key)}
                        (catch java.net.ConnectException e
                          {:node node :error :connection-refused})
                        (catch Exception e
                          {:node node :error [:unexpected (.getMessage e)]})))
            successful (filter :value results)
            values (map :value successful)]
        (if (< (count successful) 2)
          (assoc op :type :fail :error :no-quorum :results results)
          ;; Return the most common value (majority)
          (let [majority-value (->> values
                                    frequencies
                                    (sort-by val >)
                                    first
                                    key)]
            (assoc op :type :ok :value majority-value :all-values values))))

      ;; Generic read (used by final-reads phase) — delegates to read-quorum
      :read
      (let [results (for [[node conn] conns]
                      (try+
                        {:node node :value (frogdb/read-register conn test-key)}
                        (catch java.net.ConnectException e
                          {:node node :error :connection-refused})
                        (catch Exception e
                          {:node node :error [:unexpected (.getMessage e)]})))
            successful (filter :value results)
            values (map :value successful)]
        (if (< (count successful) 2)
          (assoc op :type :fail :error :no-quorum :results results)
          (let [majority-value (->> values
                                    frequencies
                                    (sort-by val >)
                                    first
                                    key)]
            (assoc op :type :ok :value majority-value :all-values values))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    nil))

(defn create-client
  "Create a new zombie client."
  []
  (map->ZombieClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn write-durable-op [value]
  {:type :invoke :f :write-durable :value value})

(defn write-async-op [value]
  {:type :invoke :f :write-async :value value})

(defn read-quorum-op []
  {:type :invoke :f :read-quorum :value nil})

(defn generator
  "Generator for zombie workload.
   Mixes durable and async writes with quorum reads."
  [opts]
  (let [rate (get opts :rate 10)
        counter (atom 0)]
    (->> (gen/mix [;; Durable writes (should survive partition)
                   (fn [] (write-durable-op (swap! counter inc)))
                   ;; Async writes (may be lost)
                   (fn [] (write-async-op (swap! counter inc)))
                   ;; Quorum reads
                   (fn [] (read-quorum-op))])
         (gen/stagger (/ 1 rate)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn extract-durable-writes
  "Extract all successful durable write operations (with ACK)."
  [history]
  (->> history
       (filter #(and (= :write-durable (:f %))
                     (= :ok (:type %))
                     (>= (or (:acked %) 0) 1)))
       (map :value)))

(defn extract-async-writes
  "Extract all successful async write operations."
  [history]
  (->> history
       (filter #(and (= :write-async (:f %))
                     (= :ok (:type %))))
       (map :value)))

(defn extract-quorum-reads
  "Extract all successful quorum read operations (including :read which delegates to read-quorum)."
  [history]
  (->> history
       (filter #(and (#{:read-quorum :read} (:f %))
                     (= :ok (:type %))))
       (map :value)))

(defn checker
  "Checker for zombie workload.

   Verifies:
   - Durable writes (with ACK) are visible in final reads
   - No 'zombie' values appear (values that were written but never ACKed)"
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [durable-writes (set (extract-durable-writes history))
            async-writes (set (extract-async-writes history))
            quorum-reads (extract-quorum-reads history)
            final-read (last quorum-reads)

            ;; Check that all durable writes are visible
            ;; (the final value should be one of the durable writes)
            final-in-durable? (or (nil? final-read)
                                  (empty? durable-writes)
                                  (contains? durable-writes final-read))

            ;; Check for zombie values
            ;; (async-only values that appear in reads after durable writes)
            async-only (clojure.set/difference async-writes durable-writes)
            zombie-reads (filter #(contains? async-only %) quorum-reads)]

        {:valid? (and final-in-durable?
                      (empty? zombie-reads))
         :durable-writes (count durable-writes)
         :async-writes (count async-writes)
         :final-value final-read
         :final-in-durable? final-in-durable?
         :zombie-reads (count zombie-reads)
         :zombie-values (take 10 zombie-reads)}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a zombie workload.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (generator opts)
   :checker (checker)})
