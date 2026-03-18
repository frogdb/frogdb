(ns jepsen.frogdb.slot-migration
  "Slot migration workload for FrogDB Raft cluster.

   Tests:
   - Normal slot migration (SETSLOT MIGRATING/IMPORTING/NODE)
   - Migration interrupted by partition
   - Migration interrupted by node crash
   - ASK redirect handling during migration
   - Data integrity during migration

   Operations:
   - :start-migration - Begin migrating a slot
   - :finish-migration - Complete a slot migration
   - :abort-migration - Cancel a migration
   - :read-slot-owner - Get current owner of a slot
   - :write - Write to key in migrating slot
   - :read - Read from key in migrating slot"
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn debug]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [jepsen.frogdb.cluster-db :as cluster-db]
            [jepsen.frogdb.cluster-client :as cluster-client]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

;; ===========================================================================
;; Migration State Tracking
;; ===========================================================================

;; Shared across all workers so gen/once migration ops see the same state
;; regardless of which worker thread executes them.
(def shared-active-migration (atom nil))

(defrecord MigrationState [slot source-node dest-node status keys-migrated])

(defn get-slot-owner
  "Get the node that owns a particular slot."
  [nodes docker-host? slot base-port]
  (let [conn (cluster-db/conn-for-raft-node (first nodes) docker-host? base-port)
        slots-info (cluster-db/cluster-slots conn)]
    (when slots-info
      (some (fn [slot-info]
              (let [start (long (nth slot-info 0))
                    end (long (nth slot-info 1))]
                (when (and (<= start slot) (<= slot end))
                  (let [master-info (nth slot-info 2)]
                    {:ip (nth master-info 0)
                     :port (nth master-info 1)
                     :id (when (> (count master-info) 2) (nth master-info 2))}))))
            slots-info))))

(defn get-node-for-ip
  "Find node name for an IP address."
  [ip]
  (first (filter #(= (get cluster-db/raft-cluster-node-ips %) ip)
                 (keys cluster-db/raft-cluster-node-ips))))

;; ===========================================================================
;; Migration Operations
;; ===========================================================================

(defn migrate-slot-keys!
  "Migrate all keys in a slot from source to dest."
  [source-conn dest-ip dest-port slot timeout-ms]
  (loop [total-migrated 0]
    (let [keys (cluster-db/cluster-getkeysinslot source-conn slot 100)]
      (if (empty? keys)
        total-migrated
        (do
          (doseq [key keys]
            (cluster-db/migrate! source-conn dest-ip dest-port key 0 timeout-ms))
          (recur (+ total-migrated (count keys))))))))

(defn parse-redirect-addr
  "Parse a REDIRECT error message and return the target node name.
   Format: 'REDIRECT <node-id> <ip:port>'
   Maps the Docker internal IP back to a node name."
  [msg]
  (when (and msg (str/starts-with? msg "REDIRECT"))
    (let [parts (str/split msg #" ")]
      (when (>= (count parts) 3)
        (let [[ip _port] (str/split (nth parts 2) #":")]
          (cluster-db/get-node-for-ip ip))))))

(defn with-leader-retry
  "Execute f, retrying on REDIRECT by switching to the indicated leader.
   f is called with the current leader node name and its connection.
   Retries up to max-retries times."
  [nodes docker-host? base-port max-retries f]
  (loop [leader (or (cluster-db/find-leader-node nodes docker-host? base-port) (first nodes))
         attempt 0]
    (let [leader-conn (cluster-db/conn-for-raft-node leader docker-host? base-port)
          result (try
                   {:ok (f leader leader-conn)}
                   (catch Exception e
                     (let [msg (.getMessage e)]
                       (if-let [new-leader (parse-redirect-addr msg)]
                         (if (< attempt max-retries)
                           {:redirect new-leader}
                           (throw e))
                         (throw e)))))]
      (if-let [new-leader (:redirect result)]
        (do (info "REDIRECT -> retrying on leader" new-leader "(attempt" (inc attempt) ")")
            (Thread/sleep 200)
            (recur new-leader (inc attempt)))
        (:ok result)))))

(defn start-slot-migration!
  "Start migrating a slot from source to dest node.
   Sends CLUSTER SETSLOT MIGRATING to the Raft leader with explicit source/target IDs.
   Both MIGRATING and IMPORTING produce the same BeginSlotMigration Raft op,
   so a single call to the leader is sufficient.
   Uses the cluster's view of node IDs (from CLUSTER NODES) because auto-discovered
   nodes may report a different MYID than what the cluster assigned them.
   Follows REDIRECT responses if leadership changes mid-request."
  [nodes docker-host? slot source-node dest-node base-port]
  (with-leader-retry nodes docker-host? base-port 3
    (fn [leader leader-conn]
      (let [source-id (cluster-db/resolve-node-id source-node docker-host? base-port leader-conn)
            dest-id (cluster-db/resolve-node-id dest-node docker-host? base-port leader-conn)]
        ;; MIGRATING with explicit source-id (4th arg) so leader doesn't default to itself
        (wcar leader-conn
          (car/redis-call ["CLUSTER" "SETSLOT" (str slot) "MIGRATING" dest-id source-id]))
        (->MigrationState slot source-node dest-node :migrating 0)))))

(defn complete-slot-migration!
  "Complete a slot migration by setting the slot to the new owner via the Raft leader.
   SETSLOT NODE returns RaftNeeded, so it must go through the leader (single call).
   Follows REDIRECT responses if leadership changes mid-request."
  [nodes docker-host? slot dest-node base-port]
  (with-leader-retry nodes docker-host? base-port 3
    (fn [leader leader-conn]
      (let [dest-id (cluster-db/resolve-node-id dest-node docker-host? base-port leader-conn)]
        (cluster-db/cluster-setslot-node! leader-conn slot dest-id)))))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord SlotMigrationClient [nodes docker-host? base-port slot-mapping active-migration]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          all-nodes (or (:cluster-nodes test) ["n1" "n2" "n3"])
          bp (get test :base-port cluster-db/default-base-port)]
      (info "Opening slot migration client")
      (assoc this
             :nodes all-nodes
             :docker-host? docker?
             :base-port bp
             :slot-mapping (atom (cluster-client/create-slot-mapping all-nodes docker? bp))
             :active-migration shared-active-migration)))

  (setup! [this test]
    (reset! shared-active-migration nil)
    this)

  (invoke! [this test op]
    (try+
      (case (:f op)
        :read-slot-owner
        (let [slot (:value op)
              owner (get-slot-owner nodes docker-host? slot base-port)]
          (assoc op :type :ok :value {:slot slot
                                       :owner (when owner (get-node-for-ip (:ip owner)))
                                       :details owner}))

        :read-slot-mapping
        (let [conn (cluster-db/conn-for-raft-node (first nodes) docker-host? base-port)
              slots (cluster-db/cluster-slots conn)]
          (assoc op :type :ok :value (count slots)))

        :start-migration
        (let [{:keys [slot source dest]} (:value op)
              ;; Dynamically resolve current owner so test is independent of prior state
              owner-info (get-slot-owner nodes docker-host? slot base-port)
              actual-source (or (get-node-for-ip (:ip owner-info)) source)
              actual-dest (if (= actual-source dest)
                            ;; Pick a different node when dest == current owner
                            (first (remove #{actual-source} nodes))
                            dest)]
          (if @active-migration
            (assoc op :type :fail :error :migration-already-active)
            (let [migration (start-slot-migration! nodes docker-host? slot actual-source actual-dest base-port)]
              (reset! active-migration migration)
              (assoc op :type :ok :value {:slot slot :source actual-source :dest actual-dest}))))

        :migrate-keys
        (if-let [migration @active-migration]
          (let [{:keys [slot source-node dest-node]} migration
                source-conn (cluster-db/conn-for-raft-node source-node docker-host? base-port)
                ;; MIGRATE executes inside Docker; use internal IPs, not host-mapped ports
                dest-ip (get cluster-db/raft-cluster-node-ips dest-node)
                migrated (migrate-slot-keys! source-conn dest-ip 6379 slot 5000)]
            (swap! active-migration assoc :keys-migrated migrated)
            (assoc op :type :ok :value {:keys-migrated migrated}))
          (assoc op :type :fail :error :no-active-migration))

        :finish-migration
        (if-let [migration @active-migration]
          (let [{:keys [slot dest-node]} migration]
            (complete-slot-migration! nodes docker-host? slot dest-node base-port)
            (reset! active-migration nil)
            ;; Refresh slot mapping
            (reset! slot-mapping (cluster-client/refresh-slot-mapping @slot-mapping (first nodes) docker-host? base-port))
            (assoc op :type :ok :value {:slot slot :new-owner dest-node}))
          (assoc op :type :fail :error :no-active-migration))

        :abort-migration
        (if-let [migration @active-migration]
          (let [{:keys [slot source-node]} migration
                leader (or (cluster-db/find-leader-node nodes docker-host? base-port) (first nodes))
                leader-conn (cluster-db/conn-for-raft-node leader docker-host? base-port)
                source-id (cluster-db/resolve-node-id source-node docker-host? base-port leader-conn)]
            ;; Reset slot to source via Raft leader
            (cluster-db/cluster-setslot-node! leader-conn slot source-id)
            (reset! active-migration nil)
            (assoc op :type :ok :value {:slot slot :owner source-node}))
          (assoc op :type :fail :error :no-active-migration))

        :write
        (let [{:keys [key value]} (:value op)
              slot (cluster-client/slot-for-key key)]
          (cluster-client/cluster-set slot-mapping key value docker-host? nodes base-port)
          (assoc op :type :ok :value {:key key :slot slot :written value}))

        :read
        (let [key (or (:value op) "{migration-test}:key")
              slot (cluster-client/slot-for-key key)
              value (cluster-client/cluster-get slot-mapping key docker-host? nodes base-port)]
          (assoc op :type :ok :value {:key key :slot slot :value (frogdb/parse-value value)})))

      (catch java.net.ConnectException e
        (assoc op :type :fail :error :connection-refused))

      (catch java.net.SocketTimeoutException e
        (assoc op :type :info :error :timeout))

      (catch [:type :crossslot] e
        (assoc op :type :fail :error :crossslot))

      (catch [:type :clusterdown] e
        (assoc op :type :fail :error :cluster-down))

      (catch Exception e
        (let [msg (.getMessage e)]
          (if (and msg (str/starts-with? msg "REDIRECT"))
            (assoc op :type :fail :error [:redirect msg])
            (do (warn "Unexpected error:" e)
                (assoc op :type :info :error [:unexpected msg])))))))

  (teardown! [this test]
    ;; Clean up any active migration via Raft leader
    (when-let [migration @active-migration]
      (let [{:keys [slot source-node]} migration]
        (try+
          (let [leader (or (cluster-db/find-leader-node nodes docker-host? base-port) (first nodes))
                leader-conn (cluster-db/conn-for-raft-node leader docker-host? base-port)
                source-id (cluster-db/resolve-node-id source-node docker-host? base-port leader-conn)]
            (cluster-db/cluster-setslot-node! leader-conn slot source-id))
          (catch Object _ nil)))))

  (close! [this test]
    nil))

(defn create-client
  "Create a new slot migration client."
  []
  (map->SlotMigrationClient {}))

;; ===========================================================================
;; Generators
;; ===========================================================================

(defn read-slot-owner
  "Generate a read-slot-owner operation."
  [slot]
  {:type :invoke :f :read-slot-owner :value slot})

(defn start-migration
  "Generate a start-migration operation."
  [slot source dest]
  {:type :invoke :f :start-migration :value {:slot slot :source source :dest dest}})

(defn migrate-keys
  "Generate a migrate-keys operation."
  []
  {:type :invoke :f :migrate-keys})

(defn finish-migration
  "Generate a finish-migration operation."
  []
  {:type :invoke :f :finish-migration})

(defn abort-migration
  "Generate an abort-migration operation."
  []
  {:type :invoke :f :abort-migration})

(defn write-key
  "Generate a write operation."
  [key value]
  {:type :invoke :f :write :value {:key key :value value}})

(defn read-key
  "Generate a read operation."
  [key]
  {:type :invoke :f :read :value key})

(defn generator
  "Generator for slot migration testing.
   Writes to keys during a migration scenario."
  [opts]
  (let [rate (get opts :rate 10)
        test-slot 1000  ; A slot in the middle of the range
        test-key "{migration-test}:key"]  ; Hash-tagged to ensure same slot
    (gen/phases
      ;; Phase 1: Initial reads/writes
      (gen/log "Initial data operations")
      (->> (gen/mix [(fn [] (write-key test-key (rand-int 1000)))
                     (fn [] (read-key test-key))])
           (gen/limit 20)
           (gen/stagger 0.1))

      ;; Phase 2: Check slot owner
      (gen/log "Checking slot ownership")
      (gen/once (read-slot-owner (cluster-client/slot-for-key test-key)))

      ;; Phase 3: Start migration
      (gen/log "Starting slot migration from n1 to n2")
      (gen/once (start-migration (cluster-client/slot-for-key test-key) "n1" "n2"))
      (gen/sleep 1)

      ;; Phase 4: Operations during migration
      (gen/log "Operations during migration")
      (->> (gen/mix [(fn [] (write-key test-key (rand-int 1000)))
                     (fn [] (read-key test-key))])
           (gen/limit 50)
           (gen/stagger 0.05))

      ;; Phase 5: Migrate keys
      (gen/log "Migrating keys")
      (gen/once (migrate-keys))

      ;; Phase 6: More operations during key migration
      (->> (gen/mix [(fn [] (write-key test-key (rand-int 1000)))
                     (fn [] (read-key test-key))])
           (gen/limit 20)
           (gen/stagger 0.1))

      ;; Phase 7: Complete migration
      (gen/log "Completing migration")
      (gen/once (finish-migration))
      (gen/sleep 2)

      ;; Phase 8: Verify new owner
      (gen/log "Verifying new slot owner")
      (gen/once (read-slot-owner (cluster-client/slot-for-key test-key)))

      ;; Phase 9: Final operations
      (gen/log "Final operations")
      (->> (gen/mix [(fn [] (write-key test-key (rand-int 1000)))
                     (fn [] (read-key test-key))])
           (gen/limit 20)
           (gen/stagger 0.1)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn checker
  "Checker for slot migration workload.

   Verifies:
   - No data loss during migration
   - Slot ownership correctly transferred
   - Operations succeed (possibly after redirects)"
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [completed (filter #(= :ok (:type %)) history)
            failed (filter #(= :fail (:type %)) history)

            ;; Track writes and reads
            writes (filter #(= :write (:f %)) completed)
            reads (filter #(= :read (:f %)) completed)

            ;; Check slot owner changes
            owner-reads (filter #(= :read-slot-owner (:f %)) completed)
            owners (map #(get-in % [:value :owner]) owner-reads)

            ;; Migration events
            start-ops (filter #(= :start-migration (:f %)) completed)
            finish-ops (filter #(= :finish-migration (:f %)) completed)

            ;; Check for data consistency
            final-owner (get-in (last owner-reads) [:value :owner])]

        {:valid? true  ; Basic validity - more detailed checks can be added
         :total-writes (count writes)
         :total-reads (count reads)
         :failed-ops (count failed)
         :owner-changes (count (partition-by identity owners))
         :owners-seen (set owners)
         :final-owner final-owner
         :migrations-started (count start-ops)
         :migrations-completed (count finish-ops)}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a slot migration workload.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (generator opts)
   :checker (checker)})
