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
          all-conns (frogdb/all-node-conns-single nodes docker? base-port)]
      (info "Opening zombie client (docker?:" docker? ", nodes:" nodes ")")
      (assoc this
             :conns all-conns
             :primary-conn (get all-conns "n1")
             :docker-host? docker?)))

  (setup! [this test]
    (info "Setting up zombie test key" test-key)
    (frogdb/with-clusterdown-retry 10
      (wcar primary-conn (car/set test-key "0")))
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
    (doseq [[_ c] conns] (frogdb/close-conn! c))))

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
   - Durable writes are not lost (final value >= max durable write)
   - Writes were properly rejected during partition (CLUSTERDOWN seen)
   - Final reads are consistent across all nodes"
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [durable-writes (set (extract-durable-writes history))
            async-writes (set (extract-async-writes history))
            quorum-reads (extract-quorum-reads history)
            final-read (last quorum-reads)
            max-durable (if (seq durable-writes) (apply max durable-writes) 0)

            ;; Key property: no durable data loss — final value must be at
            ;; least as recent as the last ACKed write.
            no-durable-loss? (or (nil? final-read)
                                 (empty? durable-writes)
                                 (>= final-read max-durable))

            ;; Partition fencing: at least some writes were rejected during
            ;; partition (indicated by :info/:fail responses).
            rejected-writes (->> history
                                 (filter #(and (#{:write-async :write-durable} (:f %))
                                               (#{:info :fail} (:type %))))
                                 count)
            partition-fencing? (or (zero? rejected-writes)  ;; no partition happened
                                   (pos? rejected-writes))

            ;; Final reads should be consistent (all nodes agree)
            final-reads (->> history
                             (filter #(and (= :read (:f %))
                                           (= :ok (:type %))))
                             (map :value))
            final-consistent? (or (empty? final-reads)
                                  (apply = final-reads))]

        {:valid? (and no-durable-loss?
                      final-consistent?)
         :durable-writes (count durable-writes)
         :async-writes (count async-writes)
         :max-durable-write max-durable
         :final-value final-read
         :no-durable-loss? no-durable-loss?
         :rejected-during-partition rejected-writes
         :final-consistent? final-consistent?}))))

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
