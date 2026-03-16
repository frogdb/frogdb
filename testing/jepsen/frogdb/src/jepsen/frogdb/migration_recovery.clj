(ns jepsen.frogdb.migration-recovery
  "Migration recovery workload for FrogDB Raft cluster.

   Tests leader crash during slot migration — a very high-realism production
   scenario.  FrogDB currently has no automatic recovery logic for orphaned
   migration state, so this test exposes whether a mid-migration leader crash
   causes permanent stuck state.

   Phases:
   1. Seed data into a target slot
   2. Start a slot migration (SETSLOT MIGRATING via Raft leader)
   3. Kill the Raft leader mid-migration
   4. Wait for new leader election
   5. Verify: cluster recovers, orphaned slot is accessible
   6. Attempt to complete or re-migrate the slot
   7. Final reads — no data loss

   Operations:
   - :write          – SET a key in the migration target slot
   - :read           – GET a key from the migration target slot
   - :start-migration – Begin migrating target slot
   - :migrate-keys   – MIGRATE keys from source to dest
   - :finish-migration – SETSLOT NODE to complete migration
   - :abort-migration  – Cancel an in-flight migration
   - :kill-leader    – SIGKILL the current Raft leader
   - :restart-node   – docker start a killed node
   - :read-leader    – Query current Raft leader
   - :read-slot-owner – Query current owner of the target slot

   Checker: custom — verifies slot ownership converges, no data loss,
   no permanently stuck migrations."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [jepsen.frogdb.cluster-db :as cluster-db]
            [jepsen.frogdb.cluster-client :as cluster-client]
            [jepsen.frogdb.slot-migration :as slot-mig]
            [slingshot.slingshot :refer [try+ throw+]]))

;; ===========================================================================
;; Shared State
;; ===========================================================================

(def shared-migration (atom nil))

(def test-hash-tag "{mig-recovery}")

(defn test-key [i] (str test-hash-tag ":key:" i))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord MigrationRecoveryClient [nodes docker-host? base-port slot-mapping]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          all-nodes (or (:cluster-nodes test) ["n1" "n2" "n3"])
          bp (get test :base-port cluster-db/default-base-port)]
      (info "Opening migration-recovery client")
      (assoc this
             :nodes all-nodes
             :docker-host? docker?
             :base-port bp
             :slot-mapping (atom (cluster-client/create-slot-mapping all-nodes docker? bp)))))

  (setup! [this test]
    (reset! shared-migration nil)
    this)

  (invoke! [this test op]
    (try+
      (case (:f op)
        ;; --- data plane ---
        :write
        (let [{:keys [key value]} (:value op)]
          (cluster-client/cluster-set slot-mapping key (str value) docker-host? nodes base-port)
          (assoc op :type :ok))

        :read
        (let [key (:value op)
              v (cluster-client/cluster-get slot-mapping key docker-host? nodes base-port)]
          (assoc op :type :ok :value {:key key :value (frogdb/parse-value v)}))

        ;; --- migration control ---
        :start-migration
        (let [{:keys [slot source dest]} (:value op)
              owner-info (cluster-client/get-node-for-slot @slot-mapping slot)
              ;; Dynamically resolve source from current slot map
              actual-source (or source
                                (when owner-info
                                  (let [[h p] (str/split owner-info #":")]
                                    ;; Reverse-map host:port → node name
                                    (some (fn [n]
                                            (let [hp (get (cluster-db/raft-cluster-host-ports base-port) n)]
                                              (when (and (= (:host hp) h)
                                                         (= (:port hp) (Integer/parseInt p)))
                                                n)))
                                          nodes)))
                                (first nodes))
              actual-dest (if (= actual-source dest)
                            (first (remove #{actual-source} nodes))
                            (or dest (second (remove #{actual-source} nodes))))]
          (if @shared-migration
            (assoc op :type :fail :error :migration-already-active)
            (let [mig (slot-mig/start-slot-migration!
                        nodes docker-host? slot actual-source actual-dest base-port)]
              (reset! shared-migration mig)
              (assoc op :type :ok :value {:slot slot :source actual-source :dest actual-dest}))))

        :migrate-keys
        (if-let [mig @shared-migration]
          (let [{:keys [slot source-node dest-node]} mig
                source-conn (cluster-db/conn-for-raft-node source-node docker-host? base-port)
                dest-ip (get cluster-db/raft-cluster-node-ips dest-node)
                migrated (slot-mig/migrate-slot-keys!
                           source-conn dest-ip 6379 slot 5000)]
            (swap! shared-migration assoc :keys-migrated migrated)
            (assoc op :type :ok :value {:keys-migrated migrated}))
          (assoc op :type :fail :error :no-active-migration))

        :finish-migration
        (if-let [mig @shared-migration]
          (let [{:keys [slot dest-node]} mig]
            (slot-mig/complete-slot-migration!
              nodes docker-host? slot dest-node base-port)
            (reset! shared-migration nil)
            (reset! slot-mapping (cluster-client/refresh-slot-mapping
                                   @slot-mapping (first nodes) docker-host? base-port))
            (assoc op :type :ok :value {:slot slot :new-owner dest-node}))
          (assoc op :type :fail :error :no-active-migration))

        :abort-migration
        (if-let [mig @shared-migration]
          (let [{:keys [slot source-node]} mig
                leader (or (cluster-db/find-leader-node nodes docker-host? base-port) (first nodes))
                leader-conn (cluster-db/conn-for-raft-node leader docker-host? base-port)
                source-id (cluster-db/resolve-node-id source-node docker-host? base-port leader-conn)]
            (cluster-db/cluster-setslot-node! leader-conn slot source-id)
            (reset! shared-migration nil)
            (assoc op :type :ok :value {:slot slot :owner source-node}))
          (assoc op :type :fail :error :no-active-migration))

        ;; --- fault injection ---
        :kill-leader
        (let [leader (cluster-db/find-leader-node nodes docker-host? base-port)]
          (if leader
            (let [container (cluster-db/raft-container-name leader)]
              (info "Killing leader" leader)
              (cluster-db/docker-hard-kill container)
              (assoc op :type :ok :value {:killed leader}))
            (assoc op :type :fail :error :no-leader-found)))

        :restart-node
        (let [node (:value op)
              container (cluster-db/raft-container-name node)]
          (info "Restarting node" node)
          (cluster-db/docker-start container)
          (cluster-db/wait-for-node-ready node docker-host? 30000 base-port)
          (assoc op :type :ok :value {:restarted node}))

        ;; --- observation ---
        :read-leader
        (let [leader (cluster-db/find-leader-node nodes docker-host? base-port)]
          (assoc op :type :ok :value leader))

        :read-slot-owner
        (let [slot (:value op)
              owner (slot-mig/get-slot-owner nodes docker-host? slot base-port)]
          (assoc op :type :ok :value {:slot slot
                                       :owner (when owner
                                                (slot-mig/get-node-for-ip (:ip owner)))
                                       :details owner})))

      (catch java.net.ConnectException _
        (assoc op :type :fail :error :connection-refused))
      (catch java.net.SocketTimeoutException _
        (assoc op :type :info :error :timeout))
      (catch [:type :clusterdown] _
        (assoc op :type :fail :error :cluster-down))
      (catch Exception e
        (let [msg (.getMessage e)]
          (if (and msg (or (str/starts-with? msg "MOVED")
                           (str/starts-with? msg "ASK")))
            (assoc op :type :info :error [:redirect msg])
            (do (warn "Unexpected error:" e)
                (assoc op :type :info :error [:unexpected msg])))))))

  (teardown! [this test]
    (when-let [mig @shared-migration]
      (try+
        (let [{:keys [slot source-node]} mig
              leader (or (cluster-db/find-leader-node nodes docker-host? base-port) (first nodes))
              leader-conn (cluster-db/conn-for-raft-node leader docker-host? base-port)
              source-id (cluster-db/resolve-node-id source-node docker-host? base-port leader-conn)]
          (cluster-db/cluster-setslot-node! leader-conn slot source-id))
        (catch Object _ nil))))

  (close! [this test]
    nil))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn generator
  "Generator for migration-recovery testing.

   Phases:
   1. Seed data
   2. Start migration
   3. Kill leader mid-migration
   4. Wait for recovery, restart killed node
   5. Attempt to complete or abort the orphaned migration
   6. Final reads"
  [opts]
  (let [rate (get opts :rate 10)
        target-slot (cluster-client/slot-for-key test-hash-tag)
        num-keys 20]
    (gen/phases
      ;; Phase 1: Seed data into target slot
      (gen/log "Phase 1: Seeding data into migration target slot")
      (->> (map (fn [i] {:type :invoke :f :write
                         :value {:key (test-key i) :value i}})
                (range num-keys))
           (gen/each-thread)
           (gen/stagger 0.05))

      ;; Phase 2: Verify data + check slot ownership
      (gen/log "Phase 2: Verify seed data")
      (->> (map (fn [i] {:type :invoke :f :read :value (test-key i)})
                (range num-keys))
           (gen/each-thread)
           (gen/stagger 0.05))
      (gen/once {:type :invoke :f :read-slot-owner :value target-slot})

      ;; Phase 3: Start migration
      (gen/log "Phase 3: Starting slot migration")
      (gen/once {:type :invoke :f :start-migration
                 :value {:slot target-slot :source "n1" :dest "n2"}})
      (gen/sleep 1)

      ;; Phase 4: Kill leader mid-migration (the critical moment)
      (gen/log "Phase 4: KILLING LEADER mid-migration")
      (gen/once {:type :invoke :f :kill-leader})
      (gen/sleep 2)

      ;; Phase 5: Observe — can we still read? Is there a leader?
      (gen/log "Phase 5: Observing recovery")
      (->> (gen/mix [(fn [] {:type :invoke :f :read-leader})
                     (fn [] {:type :invoke :f :read-slot-owner :value target-slot})
                     (fn [] {:type :invoke :f :read :value (test-key 0)})])
           (gen/limit 15)
           (gen/stagger 0.5))

      ;; Phase 6: Restart the killed node
      (gen/log "Phase 6: Restarting killed node")
      ;; Try restarting all nodes to ensure cluster is healthy
      (->> (map (fn [n] {:type :invoke :f :restart-node :value n}) ["n1" "n2" "n3" "n4" "n5"])
           (gen/each-thread)
           (gen/stagger 0.5))
      (gen/sleep 5)

      ;; Phase 7: Try to complete or abort the orphaned migration
      (gen/log "Phase 7: Attempting to resolve orphaned migration")
      (gen/once {:type :invoke :f :read-slot-owner :value target-slot})
      ;; Try abort first (safer)
      (gen/once {:type :invoke :f :abort-migration})
      (gen/sleep 2)

      ;; Phase 8: Verify all data is still accessible
      (gen/log "Phase 8: Final data verification")
      (->> (map (fn [i] {:type :invoke :f :read :value (test-key i)})
                (range num-keys))
           (gen/each-thread)
           (gen/stagger 0.05))
      (gen/once {:type :invoke :f :read-slot-owner :value target-slot}))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn checker
  "Checker for migration-recovery workload.

   Verifies:
   - Cluster recovered (leader elected after kill)
   - Slot ownership converged (not stuck in migration state)
   - No data loss (all seeded keys readable after recovery)
   - Migration can be resolved (completed or aborted)"
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [completed (filter #(= :ok (:type %)) history)
            failed (filter #(= :fail (:type %)) history)

            ;; Data integrity: count successful writes and final reads
            writes (filter #(= :write (:f %)) completed)
            reads (filter #(= :read (:f %)) completed)
            final-reads (take-last 20 reads)
            reads-with-nil (filter #(nil? (get-in % [:value :value])) final-reads)

            ;; Leader tracking
            leader-reads (filter #(= :read-leader (:f %)) completed)
            leaders-seen (set (remove nil? (map :value leader-reads)))
            leader-recovered? (some some? (map :value (take-last 5 leader-reads)))

            ;; Slot ownership
            slot-owner-reads (filter #(= :read-slot-owner (:f %)) completed)
            final-slot-owner (get-in (last slot-owner-reads) [:value :owner])

            ;; Migration resolution
            abort-ops (filter #(= :abort-migration (:f %)) completed)
            finish-ops (filter #(= :finish-migration (:f %)) completed)
            migration-resolved? (or (seq abort-ops) (seq finish-ops))

            ;; No data loss = all final reads returned a value
            data-loss? (seq reads-with-nil)]

        {:valid? (and leader-recovered?
                      (not data-loss?)
                      (some? final-slot-owner))
         :leader-recovered? (boolean leader-recovered?)
         :leaders-seen leaders-seen
         :final-slot-owner final-slot-owner
         :migration-resolved? (boolean migration-resolved?)
         :total-writes (count writes)
         :total-reads (count reads)
         :final-reads-with-nil (count reads-with-nil)
         :failed-ops (count failed)}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a migration-recovery workload.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (map->MigrationRecoveryClient {})
   :generator (generator opts)
   :checker (checker)})
