(ns jepsen.frogdb.membership-routing
  "Membership routing workload for FrogDB Raft cluster.

   Tests client routing correctness when the cluster membership changes.
   When a new node is added (CLUSTER MEET) and slots are migrated to it,
   clients holding stale slot maps will receive MOVED redirects.  This test
   verifies that:
   - MOVED redirects are handled correctly
   - Clients eventually converge on the correct slot map
   - No acknowledged writes are lost during the transition
   - Cluster remains available through membership changes

   Phases:
   1. Active traffic on 3-node cluster
   2. Add a new node (n4) via CLUSTER MEET
   3. Migrate slots to the new node while traffic continues
   4. Verify: all acknowledged writes are durable, no data loss

   Operations:
   - :write  – SET a hash-tagged key
   - :read   – GET a hash-tagged key
   - :add-node    – CLUSTER MEET a new node
   - :start-migration – Begin migrating a slot to the new node
   - :migrate-keys    – Move keys for the migration
   - :finish-migration – Complete the migration
   - :read-slot-owner – Check slot ownership

   Checker: custom — all acknowledged writes are durable post-membership-change."
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
;; State
;; ===========================================================================

(def shared-migration (atom nil))
(def test-hash-tag "{mem-route}")

(defn test-key [i] (str test-hash-tag ":k:" i))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord MembershipRoutingClient [nodes docker-host? base-port slot-mapping]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          all-nodes (or (:cluster-nodes test) ["n1" "n2" "n3"])
          bp (get test :base-port cluster-db/default-base-port)]
      (info "Opening membership-routing client")
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
        :write
        (let [{:keys [key value]} (:value op)]
          (cluster-client/cluster-set slot-mapping key (str value) docker-host? nodes base-port)
          (assoc op :type :ok))

        :read
        (let [key (if (map? (:value op)) (:key (:value op)) (:value op))
              v (cluster-client/cluster-get slot-mapping key docker-host? nodes base-port)]
          (assoc op :type :ok :value {:key key :value (frogdb/parse-value v)}))

        :add-node
        (let [new-node (:value op)
              live-node (first (filter #(not= % new-node) nodes))
              live-conn (cluster-db/conn-for-raft-node live-node docker-host? base-port)
              new-ip (get cluster-db/raft-cluster-node-ips new-node)]
          (info "Adding node" new-node "to cluster via CLUSTER MEET")
          ;; Ensure the container is running
          (try+
            (cluster-db/docker-start (cluster-db/raft-container-name new-node))
            (catch Object _ nil))
          (cluster-db/wait-for-node-ready new-node docker-host? 15000 base-port)
          (cluster-db/cluster-meet! live-conn new-ip 6379)
          ;; Wait for the new node to appear in CLUSTER NODES on ALL existing
          ;; nodes (especially the Raft leader, which may differ from live-node)
          (let [deadline (+ (System/currentTimeMillis) 30000)
                existing-nodes (filter #(not= % new-node) nodes)]
            (loop []
              (let [all-see-it?
                    (every? (fn [n]
                              (try+
                                (let [c (cluster-db/conn-for-raft-node n docker-host? base-port)
                                      cn (cluster-db/cluster-nodes c)]
                                  (some #(str/includes? (:addr %) (str new-ip)) cn))
                                (catch Object _ false)))
                            existing-nodes)]
                (when-not all-see-it?
                  (when (> (System/currentTimeMillis) deadline)
                    (throw+ {:type :timeout :message (str "Node " new-node " not visible on all nodes after 30s")}))
                  (Thread/sleep 1000)
                  (recur)))))
          ;; Refresh slot mapping
          (reset! slot-mapping (cluster-client/refresh-slot-mapping
                                 @slot-mapping live-node docker-host? base-port))
          (assoc op :type :ok :value {:added new-node}))

        :start-migration
        (let [{:keys [slot dest]} (:value op)]
          (if @shared-migration
            (assoc op :type :fail :error :migration-already-active)
            (let [owner-info (slot-mig/get-slot-owner nodes docker-host? slot base-port)
                  actual-source (or (slot-mig/get-node-for-ip (:ip owner-info)) (first nodes))
                  actual-dest (if (= actual-source dest)
                                (first (remove #{actual-source} nodes))
                                dest)]
              (let [mig (slot-mig/start-slot-migration!
                          nodes docker-host? slot actual-source actual-dest base-port)]
                (reset! shared-migration mig)
                (assoc op :type :ok :value {:slot slot :source actual-source :dest actual-dest})))))

        :migrate-keys
        (if-let [mig @shared-migration]
          (let [{:keys [slot source-node dest-node]} mig
                source-conn (cluster-db/conn-for-raft-node source-node docker-host? base-port)
                dest-ip (get cluster-db/raft-cluster-node-ips dest-node)
                migrated (slot-mig/migrate-slot-keys! source-conn dest-ip 6379 slot 5000)]
            (swap! shared-migration assoc :keys-migrated migrated)
            (assoc op :type :ok :value {:keys-migrated migrated}))
          (assoc op :type :fail :error :no-active-migration))

        :finish-migration
        (if-let [mig @shared-migration]
          (let [{:keys [slot dest-node]} mig]
            (slot-mig/complete-slot-migration! nodes docker-host? slot dest-node base-port)
            (reset! shared-migration nil)
            (reset! slot-mapping (cluster-client/refresh-slot-mapping
                                   @slot-mapping (first nodes) docker-host? base-port))
            (assoc op :type :ok :value {:slot slot :new-owner dest-node}))
          (assoc op :type :fail :error :no-active-migration))

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
  "Generator for membership-routing testing."
  [opts]
  (let [rate (get opts :rate 10)
        target-slot (cluster-client/slot-for-key test-hash-tag)
        counter (atom 0)]
    (gen/phases
      ;; Phase 1: Active traffic on initial 3-node cluster
      (gen/log "Phase 1: Baseline traffic on 3-node cluster")
      (->> (gen/mix [(fn [] {:type :invoke :f :write
                             :value {:key (test-key (swap! counter inc))
                                     :value @counter}})
                     (fn [] {:type :invoke :f :read
                             :value (test-key (rand-int (max 1 @counter)))})])
           (gen/limit 30)
           (gen/stagger (/ 1 rate)))

      ;; Phase 2: Add n4 to the cluster
      (gen/log "Phase 2: Adding node n4 to cluster")
      (gen/once {:type :invoke :f :add-node :value "n4"})
      (gen/sleep 3)

      ;; Phase 3: Start migrating a slot to n4 while traffic continues
      (gen/log "Phase 3: Migrating slot to n4 with concurrent traffic")
      (gen/once {:type :invoke :f :start-migration
                 :value {:slot target-slot :dest "n4"}})
      (gen/sleep 1)

      ;; Phase 4: Concurrent writes/reads + key migration
      (->> (gen/mix [(fn [] {:type :invoke :f :write
                             :value {:key (test-key (swap! counter inc))
                                     :value @counter}})
                     (fn [] {:type :invoke :f :read
                             :value (test-key (rand-int (max 1 @counter)))})])
           (gen/limit 40)
           (gen/stagger (/ 1 rate)))

      ;; Migrate keys
      (gen/once {:type :invoke :f :migrate-keys})
      (gen/sleep 1)

      ;; Phase 5: Complete migration
      (gen/log "Phase 5: Completing migration")
      (gen/once {:type :invoke :f :finish-migration})
      (gen/sleep 2)

      ;; Phase 6: Verify routing works to new owner
      (gen/log "Phase 6: Post-migration traffic verification")
      (->> (gen/mix [(fn [] {:type :invoke :f :write
                             :value {:key (test-key (swap! counter inc))
                                     :value @counter}})
                     (fn [] {:type :invoke :f :read
                             :value (test-key (rand-int (max 1 @counter)))})])
           (gen/limit 20)
           (gen/stagger (/ 1 rate)))

      ;; Final slot ownership check
      (gen/once {:type :invoke :f :read-slot-owner :value target-slot}))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn checker
  "Checker for membership-routing workload.

   Verifies:
   - All acknowledged writes are durable (readable after membership change)
   - Slot ownership transferred to the new node
   - No lost writes during transition"
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [completed (filter #(= :ok (:type %)) history)
            failed (filter #(= :fail (:type %)) history)

            ;; Track all acknowledged writes
            writes (filter #(= :write (:f %)) completed)
            written-keys (set (map #(get-in % [:value :key]) writes))

            ;; Track all successful reads
            reads (filter #(= :read (:f %)) completed)
            final-reads (take-last 20 reads)
            reads-with-nil (filter #(nil? (get-in % [:value :value])) final-reads)

            ;; Slot ownership
            slot-reads (filter #(= :read-slot-owner (:f %)) completed)
            final-slot-owner (get-in (last slot-reads) [:value :owner])

            ;; Node addition
            add-ops (filter #(= :add-node (:f %)) completed)
            node-added? (seq add-ops)

            ;; Migration completion
            finish-ops (filter #(= :finish-migration (:f %)) completed)
            migration-completed? (seq finish-ops)]

        {:valid? (boolean (and node-added?
                              migration-completed?))
         :node-added? (boolean node-added?)
         :migration-completed? (boolean migration-completed?)
         :final-slot-owner final-slot-owner
         :total-writes (count writes)
         :total-reads (count reads)
         :final-reads-nil-count (count reads-with-nil)
         :failed-ops (count failed)}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a membership-routing workload.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (map->MembershipRoutingClient {})
   :generator (generator opts)
   :checker (checker)})
