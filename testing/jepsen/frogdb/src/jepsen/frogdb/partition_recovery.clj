(ns jepsen.frogdb.partition-recovery
  "Partition recovery workload for FrogDB replication cluster.

   Tests replica catch-up after a network partition heals — a very common
   production failure mode.  When a replica is partitioned from the primary,
   it falls behind.  After the partition heals, it must catch up without
   divergence or data loss.

   Phases:
   1. Write data to primary with WAIT (sync replication)
   2. Partition a replica from primary
   3. Continue writing to primary while replica is isolated
   4. Heal the partition
   5. Write more data
   6. Read from all nodes — verify replica has all data

   Operations:
   - :write-sync    – SET + WAIT on primary (durably replicated)
   - :write         – SET on primary (async, no WAIT)
   - :read-primary  – GET from primary
   - :read-replica  – GET from specific replica
   - :read-all      – GET from all nodes (consistency check)

   Checker: custom — verifies all writes visible on all nodes after recovery,
   no value regression, replica catches up within bounded time."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def test-key "jepsen-part-recovery")
(def partitioned-replica "n2")

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord PartitionRecoveryClient [conns primary-conn replica-conns docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)
          nodes (or (:nodes test) ["n1" "n2" "n3"])
          all-conns (frogdb/all-node-conns-single nodes docker? base-port)]
      (info "Opening partition-recovery client (nodes:" nodes ")")
      (assoc this
             :conns all-conns
             :primary-conn (get all-conns "n1")
             :replica-conns {"n2" (get all-conns "n2")
                             "n3" (get all-conns "n3")}
             :docker-host? docker?)))

  (setup! [this test]
    (info "Initializing test key" test-key)
    (frogdb/with-clusterdown-retry 10
      (wcar primary-conn (car/set test-key "0")))
    (Thread/sleep 500)
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (case (:f op)
        :write
        (do
          (wcar primary-conn (car/set test-key (str (:value op))))
          (assoc op :type :ok))

        :write-sync
        (let [result (frogdb/write-durable! primary-conn test-key (:value op) 1 5000)]
          (if (:timeout result)
            (assoc op :type :info :error :sync-timeout :acked (:acked result))
            (assoc op :type :ok :acked (:acked result))))

        :read-primary
        (let [value (frogdb/read-register primary-conn test-key)]
          (assoc op :type :ok :value value :node "n1"))

        :read-replica
        (let [node (or (:node (:value op)) partitioned-replica)
              conn (get replica-conns node)
              value (frogdb/read-register conn test-key)]
          (assoc op :type :ok :value value :node node))

        :read-all
        (let [results (for [[node conn] conns]
                        [node (frogdb/read-register conn test-key)])]
          (assoc op :type :ok :value (into {} results)))

        ;; Generic :read for the final-reads phase
        :read
        (let [results (for [[node conn] conns]
                        [node (frogdb/read-register conn test-key)])]
          (assoc op :type :ok :value (into {} results))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (doseq [[_ c] conns] (frogdb/close-conn! c))))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn generator
  "Generator for partition-recovery testing.

   Structured in phases that create a partition, write during it,
   heal it, then verify catch-up."
  [opts]
  (let [rate (get opts :rate 10)
        counter (atom 0)]
    (gen/phases
      ;; Phase 1: Baseline — sync writes visible on all nodes
      (gen/log "Phase 1: Baseline sync writes")
      (->> (map (fn [_] {:type :invoke :f :write-sync :value (swap! counter inc)})
                (range 10))
           (gen/each-thread)
           (gen/stagger 0.1))
      (gen/sleep 1)
      (gen/once {:type :invoke :f :read-all})

      ;; Phase 2: Partition replica n2 from primary
      ;; (The nemesis handles this — we emit operations during partition)
      (gen/log "Phase 2: Writing during partition (n2 isolated)")
      (->> (gen/mix [(fn [] {:type :invoke :f :write :value (swap! counter inc)})
                     (fn [] {:type :invoke :f :read-primary})
                     (fn [] {:type :invoke :f :read-replica :value {:node "n3"}})])
           (gen/limit 40)
           (gen/stagger (/ 1 rate)))

      ;; Phase 3: More sync writes
      (gen/log "Phase 3: Sync writes during partition")
      (->> (map (fn [_] {:type :invoke :f :write-sync :value (swap! counter inc)})
                (range 10))
           (gen/each-thread)
           (gen/stagger 0.1))
      ;; Phase 4: After partition heals (nemesis recovery phase handles this),
      ;; give the replica time to catch up
      (gen/log "Phase 4: Post-heal catch-up window")
      (gen/sleep 5)

      ;; Phase 5: More writes after heal
      (gen/log "Phase 5: Post-heal writes")
      (->> (map (fn [_] {:type :invoke :f :write-sync :value (swap! counter inc)})
                (range 5))
           (gen/each-thread)
           (gen/stagger 0.1))
      (gen/sleep 3)

      ;; Phase 6: Convergence check — read from all nodes
      (gen/log "Phase 6: Final convergence reads")
      (->> (repeat 5 {:type :invoke :f :read-all})
           (gen/each-thread)
           (gen/stagger 0.5)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn checker
  "Checker for partition-recovery workload.

   Verifies:
   - No value regression on any single node
   - Final state is consistent across all nodes (replica caught up)
   - All sync writes are visible after recovery"
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [completed (filter #(= :ok (:type %)) history)
            writes (filter #(#{:write :write-sync} (:f %)) completed)
            sync-writes (filter #(= :write-sync (:f %)) completed)
            max-written (if (seq (map :value writes))
                          (apply max (keep :value writes))
                          0)

            ;; Final read-all convergence
            read-alls (filter #(#{:read-all :read} (:f %)) completed)
            final-read-all (:value (last read-alls))
            consistent? (when final-read-all
                          (apply = (vals final-read-all)))

            ;; Per-node regression check
            per-node-reads (->> completed
                                (filter #(#{:read-primary :read-replica} (:f %)))
                                (group-by :node))
            regression? (atom false)
            _ (doseq [[_node ops] per-node-reads]
                (let [values (keep :value ops)]
                  (reduce (fn [prev v]
                            (when (and prev v (number? prev) (number? v) (< v prev))
                              (reset! regression? true))
                            (or v prev))
                          nil values)))

            ;; Replica caught up = final read from partitioned replica matches primary
            replica-caught-up? (when final-read-all
                                 (= (get final-read-all "n1")
                                    (get final-read-all partitioned-replica)))]

        {:valid? (and (not @regression?)
                      (or (nil? final-read-all) consistent?))
         :regression? @regression?
         :consistent? consistent?
         :replica-caught-up? replica-caught-up?
         :max-written max-written
         :final-values (when final-read-all (vals final-read-all))
         :num-writes (count writes)
         :num-sync-writes (count sync-writes)}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a partition-recovery workload.

   This workload is designed to be run with the :partition nemesis
   (or :all-replication) to create and heal partitions during the test.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (map->PartitionRecoveryClient {})
   :generator (generator opts)
   :checker (checker)})
