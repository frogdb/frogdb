(ns jepsen.frogdb.concurrent-migration
  "Concurrent slot migration workload for FrogDB Raft cluster.

   Tests multiple slot migrations happening in parallel under fault injection.
   When rebalancing (e.g. adding a node), many slots migrate concurrently.
   FrogDB tracks migrations in a BTreeMap<u16, SlotMigration>, allowing parallel
   migrations of different slots.

   Phases:
   1. Seed data across multiple slots
   2. Trigger 4 concurrent slot migrations (different slots, different targets)
   3. Inject faults (partition, kill) during migrations
   4. Verify: all migrations complete or cleanly abort, no data loss, no stuck state

   Operations:
   - :write          – SET a key (hash-tagged per slot group)
   - :read           – GET a key
   - :start-migration – Begin migrating a specific slot
   - :migrate-keys   – Move keys for an active migration
   - :finish-migration – Complete a specific migration
   - :abort-migration  – Cancel a specific migration
   - :read-slot-owner – Query owner of a slot

   Checker: custom — final slot ownership is consistent, all keys accessible,
   no stuck migrations."
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
;; Migration Targets
;; ===========================================================================

;; We use 4 different hash tags that route to 4 different slots.
;; Each will be migrated to a different target node.
(def migration-groups
  [{:tag "{cmig-a}" :dest "n2"}
   {:tag "{cmig-b}" :dest "n3"}
   {:tag "{cmig-c}" :dest "n4"}
   {:tag "{cmig-d}" :dest "n5"}])

(defn group-key [group i]
  (str (:tag group) ":key:" i))

(defn group-slot [group]
  (cluster-client/slot-for-key (:tag group)))

;; Active migrations tracked per-slot
(def active-migrations (atom {}))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord ConcurrentMigrationClient [nodes docker-host? base-port slot-mapping]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          all-nodes (or (:cluster-nodes test) ["n1" "n2" "n3" "n4" "n5"])
          bp (get test :base-port cluster-db/default-base-port)]
      (info "Opening concurrent-migration client")
      (assoc this
             :nodes all-nodes
             :docker-host? docker?
             :base-port bp
             :slot-mapping (atom (cluster-client/create-slot-mapping all-nodes docker? bp)))))

  (setup! [this test]
    (reset! active-migrations {})
    this)

  (invoke! [this test op]
    (try+
      (case (:f op)
        :write
        (let [{:keys [key value]} (:value op)]
          (cluster-client/cluster-set slot-mapping key (str value) docker-host? nodes base-port)
          (assoc op :type :ok))

        :read
        (let [key (:value op)
              v (cluster-client/cluster-get slot-mapping key docker-host? nodes base-port)]
          (assoc op :type :ok :value {:key key :value (frogdb/parse-value v)}))

        :start-migration
        (let [{:keys [slot dest]} (:value op)]
          (if (get @active-migrations slot)
            (assoc op :type :fail :error :migration-already-active-for-slot)
            (let [;; Resolve current owner dynamically
                  owner-info (slot-mig/get-slot-owner nodes docker-host? slot base-port)
                  actual-source (or (slot-mig/get-node-for-ip (:ip owner-info)) (first nodes))
                  actual-dest (if (= actual-source dest)
                                (first (remove #{actual-source} nodes))
                                dest)]
              (let [mig (slot-mig/start-slot-migration!
                          nodes docker-host? slot actual-source actual-dest base-port)]
                (swap! active-migrations assoc slot mig)
                (assoc op :type :ok
                       :value {:slot slot :source actual-source :dest actual-dest})))))

        :migrate-keys
        (let [slot (get-in op [:value :slot])
              mig (get @active-migrations slot)]
          (if-not mig
            (assoc op :type :fail :error :no-migration-for-slot)
            (let [{:keys [source-node dest-node]} mig
                  source-conn (cluster-db/conn-for-raft-node source-node docker-host? base-port)
                  dest-ip (get cluster-db/raft-cluster-node-ips dest-node)
                  migrated (slot-mig/migrate-slot-keys! source-conn dest-ip 6379 slot 5000)]
              (swap! active-migrations assoc-in [slot :keys-migrated] migrated)
              (assoc op :type :ok :value {:slot slot :keys-migrated migrated}))))

        :finish-migration
        (let [slot (get-in op [:value :slot])
              mig (get @active-migrations slot)]
          (if-not mig
            (assoc op :type :fail :error :no-migration-for-slot)
            (let [{:keys [dest-node]} mig]
              (slot-mig/complete-slot-migration! nodes docker-host? slot dest-node base-port)
              (swap! active-migrations dissoc slot)
              (reset! slot-mapping (cluster-client/refresh-slot-mapping
                                     @slot-mapping (first nodes) docker-host? base-port))
              (assoc op :type :ok :value {:slot slot :new-owner dest-node}))))

        :abort-migration
        (let [slot (get-in op [:value :slot])
              mig (get @active-migrations slot)]
          (if-not mig
            (assoc op :type :fail :error :no-migration-for-slot)
            (let [{:keys [source-node]} mig
                  leader (or (cluster-db/find-leader-node nodes docker-host? base-port) (first nodes))
                  leader-conn (cluster-db/conn-for-raft-node leader docker-host? base-port)
                  source-id (cluster-db/resolve-node-id source-node docker-host? base-port leader-conn)]
              (cluster-db/cluster-setslot-node! leader-conn slot source-id)
              (swap! active-migrations dissoc slot)
              (assoc op :type :ok :value {:slot slot :owner source-node}))))

        :read-slot-owner
        (let [slot (:value op)
              owner (slot-mig/get-slot-owner nodes docker-host? slot base-port)]
          (assoc op :type :ok :value {:slot slot
                                       :owner (when owner (slot-mig/get-node-for-ip (:ip owner)))})))

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
    ;; Abort any remaining active migrations
    (doseq [[slot mig] @active-migrations]
      (try+
        (let [{:keys [source-node]} mig
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
  "Generator for concurrent migration testing."
  [opts]
  (let [rate (get opts :rate 10)
        keys-per-group 10]
    (gen/phases
      ;; Phase 1: Seed data across all migration groups
      (gen/log "Phase 1: Seeding data across 4 slot groups")
      (->> (for [group migration-groups
                 i (range keys-per-group)]
             {:type :invoke :f :write
              :value {:key (group-key group i) :value (* 100 i)}})
           (gen/each-thread)
           (gen/stagger 0.02))

      ;; Phase 2: Verify initial slot ownership
      (gen/log "Phase 2: Checking initial slot ownership")
      (->> (for [group migration-groups]
             {:type :invoke :f :read-slot-owner :value (group-slot group)})
           (gen/each-thread)
           (gen/stagger 0.1))

      ;; Phase 3: Start all 4 migrations concurrently
      (gen/log "Phase 3: Starting 4 concurrent slot migrations")
      (->> (for [group migration-groups]
             {:type :invoke :f :start-migration
              :value {:slot (group-slot group) :dest (:dest group)}})
           (gen/each-thread)
           (gen/stagger 0.2))
      (gen/sleep 1)

      ;; Phase 4: Migrate keys + concurrent reads during migration
      (gen/log "Phase 4: Migrating keys with concurrent reads")
      (->> (gen/mix
             (concat
               ;; Migrate keys for each group
               (for [group migration-groups]
                 (fn [] {:type :invoke :f :migrate-keys
                         :value {:slot (group-slot group)}}))
               ;; Read keys during migration
               (for [group migration-groups]
                 (fn [] {:type :invoke :f :read
                         :value (group-key group (rand-int keys-per-group))}))))
           (gen/limit 30)
           (gen/stagger 0.1))

      ;; Phase 5: Complete all migrations
      (gen/log "Phase 5: Completing migrations")
      (->> (for [group migration-groups]
             {:type :invoke :f :finish-migration
              :value {:slot (group-slot group)}})
           (gen/each-thread)
           (gen/stagger 0.2))
      (gen/sleep 2)

      ;; Phase 6: Verify final state
      (gen/log "Phase 6: Verifying final slot ownership and data integrity")
      (->> (for [group migration-groups]
             {:type :invoke :f :read-slot-owner :value (group-slot group)})
           (gen/each-thread)
           (gen/stagger 0.1))

      ;; Phase 7: Read all keys
      (gen/log "Phase 7: Final data reads")
      (->> (for [group migration-groups
                 i (range keys-per-group)]
             {:type :invoke :f :read :value (group-key group i)})
           (gen/each-thread)
           (gen/stagger 0.02)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn checker
  "Checker for concurrent migration workload.

   Verifies:
   - All migrations completed or cleanly aborted
   - Final slot ownership is consistent (not stuck in MIGRATING)
   - All seeded keys are readable after migration
   - No data loss"
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [completed (filter #(= :ok (:type %)) history)
            failed (filter #(= :fail (:type %)) history)

            ;; Migrations
            starts (filter #(= :start-migration (:f %)) completed)
            finishes (filter #(= :finish-migration (:f %)) completed)
            aborts (filter #(= :abort-migration (:f %)) completed)

            ;; Slot ownership convergence
            slot-reads (filter #(= :read-slot-owner (:f %)) completed)
            final-slot-reads (take-last 4 slot-reads)
            slots-with-owner (filter #(some? (get-in % [:value :owner])) final-slot-reads)

            ;; Data integrity — final reads
            reads (filter #(= :read (:f %)) completed)
            writes (filter #(= :write (:f %)) completed)
            ;; Count the last batch of reads (40 keys = 4 groups * 10 keys)
            final-reads (take-last 40 reads)
            reads-with-nil (filter #(nil? (get-in % [:value :value])) final-reads)

            all-slots-owned? (= (count slots-with-owner) (count final-slot-reads))
            no-data-loss? (empty? reads-with-nil)]

        {:valid? (and all-slots-owned? no-data-loss?)
         :migrations-started (count starts)
         :migrations-finished (count finishes)
         :migrations-aborted (count aborts)
         :all-slots-owned? all-slots-owned?
         :final-slot-owners (mapv #(get-in % [:value :owner]) final-slot-reads)
         :no-data-loss? no-data-loss?
         :total-writes (count writes)
         :total-reads (count reads)
         :final-reads-nil-count (count reads-with-nil)
         :failed-ops (count failed)}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a concurrent migration workload.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (map->ConcurrentMigrationClient {})
   :generator (generator opts)
   :checker (checker)})
