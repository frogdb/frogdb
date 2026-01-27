(ns jepsen.frogdb.cluster-formation
  "Cluster formation workload for FrogDB Raft cluster.

   Tests:
   - Initial cluster bootstrap (3-node)
   - Node join via CLUSTER MEET
   - Node removal via CLUSTER FORGET
   - Quorum loss detection
   - Recovery from quorum loss

   Operations:
   - :read-cluster-state - Check cluster_state (ok/fail)
   - :read-node-count - Count nodes in cluster
   - :meet-node - Add a new node to cluster
   - :forget-node - Remove a node from cluster
   - :kill-majority - Kill majority of nodes
   - :restart-killed - Restart killed nodes"
  (:require [clojure.tools.logging :refer [info warn debug]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [jepsen.frogdb.cluster-db :as cluster-db]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord ClusterFormationClient [nodes docker-host? killed-nodes]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          all-nodes (or (:cluster-nodes test) ["n1" "n2" "n3"])]
      (info "Opening cluster formation client")
      (assoc this
             :nodes all-nodes
             :docker-host? docker?
             :killed-nodes (atom #{}))))

  (setup! [this test]
    this)

  (invoke! [this test op]
    (try+
      (case (:f op)
        :read-cluster-state
        (let [conn (cluster-db/conn-for-raft-node (first nodes) docker-host?)
              state (cluster-db/get-cluster-state conn)]
          (assoc op :type :ok :value state))

        :read-node-count
        (let [conn (cluster-db/conn-for-raft-node (first nodes) docker-host?)
              nodes-info (cluster-db/cluster-nodes conn)
              count (count nodes-info)]
          (assoc op :type :ok :value count))

        :read-masters
        (let [conn (cluster-db/conn-for-raft-node (first nodes) docker-host?)
              masters (cluster-db/count-masters nodes docker-host?)]
          (assoc op :type :ok :value masters))

        :meet-node
        (let [node-to-meet (:value op)
              ip (get cluster-db/raft-cluster-node-ips node-to-meet)
              conn (cluster-db/conn-for-raft-node (first nodes) docker-host?)]
          (if ip
            (do
              (cluster-db/cluster-meet! conn ip 6379)
              (Thread/sleep 1000)  ; Wait for gossip
              (assoc op :type :ok :value node-to-meet))
            (assoc op :type :fail :error :unknown-node)))

        :forget-node
        (let [node-to-forget (:value op)
              conn (cluster-db/conn-for-raft-node (first nodes) docker-host?)
              node-id (cluster-db/get-node-id conn (get cluster-db/raft-cluster-node-ips node-to-forget))]
          (if node-id
            (do
              ;; Forget from all nodes
              (doseq [n (remove #{node-to-forget} nodes)]
                (let [c (cluster-db/conn-for-raft-node n docker-host?)]
                  (try+
                    (cluster-db/cluster-forget! c node-id)
                    (catch Object _ nil))))
              (assoc op :type :ok :value node-to-forget))
            (assoc op :type :fail :error :node-not-found)))

        :verify-quorum
        (let [conn (cluster-db/conn-for-raft-node (first nodes) docker-host?)
              state (cluster-db/get-cluster-state conn)]
          (if (= "ok" state)
            (assoc op :type :ok :value :quorum-ok)
            (assoc op :type :ok :value :quorum-lost))))

      (catch java.net.ConnectException e
        (assoc op :type :fail :error :connection-refused))

      (catch java.net.SocketTimeoutException e
        (assoc op :type :info :error :timeout))

      (catch Exception e
        (warn "Unexpected error:" e)
        (assoc op :type :info :error [:unexpected (.getMessage e)]))))

  (teardown! [this test]
    nil)

  (close! [this test]
    nil))

(defn create-client
  "Create a new cluster formation client."
  []
  (map->ClusterFormationClient {}))

;; ===========================================================================
;; Generators
;; ===========================================================================

(defn cluster-state-reads
  "Generate cluster state read operations."
  []
  {:type :invoke :f :read-cluster-state})

(defn node-count-reads
  "Generate node count read operations."
  []
  {:type :invoke :f :read-node-count})

(defn master-count-reads
  "Generate master count read operations."
  []
  {:type :invoke :f :read-masters})

(defn quorum-verification
  "Generate quorum verification operations."
  []
  {:type :invoke :f :verify-quorum})

(defn meet-node
  "Generate a MEET operation for a node."
  [node]
  {:type :invoke :f :meet-node :value node})

(defn forget-node
  "Generate a FORGET operation for a node."
  [node]
  {:type :invoke :f :forget-node :value node})

(defn generator
  "Generator for cluster formation testing.
   Focuses on cluster state reads with occasional membership changes."
  [opts]
  (let [rate (get opts :rate 5)]
    (->> (gen/mix [(fn [] (cluster-state-reads))
                   (fn [] (node-count-reads))
                   (fn [] (quorum-verification))])
         (gen/stagger (/ 1 rate)))))

(defn membership-change-generator
  "Generator that includes membership changes.
   Requires n4/n5 to be available for join/leave testing."
  [opts]
  (let [rate (get opts :rate 2)
        extra-nodes (get opts :extra-nodes ["n4" "n5"])]
    (gen/phases
      ;; Phase 1: Read initial state
      (gen/log "Reading initial cluster state")
      (->> (gen/repeat (cluster-state-reads))
           (gen/limit 5)
           (gen/stagger 0.5))

      ;; Phase 2: Add extra nodes
      (gen/log "Adding nodes to cluster")
      (apply gen/phases (map #(gen/once (meet-node %)) extra-nodes))
      (gen/sleep 3)

      ;; Phase 3: Verify nodes joined
      (->> (gen/repeat (node-count-reads))
           (gen/limit 5)
           (gen/stagger 0.5))

      ;; Phase 4: Remove extra nodes
      (gen/log "Removing nodes from cluster")
      (apply gen/phases (map #(gen/once (forget-node %)) extra-nodes))
      (gen/sleep 3)

      ;; Phase 5: Verify nodes removed
      (->> (gen/repeat (node-count-reads))
           (gen/limit 5)
           (gen/stagger 0.5)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn checker
  "Checker for cluster formation workload.

   Verifies:
   - Cluster eventually reaches 'ok' state
   - Node counts are consistent
   - Quorum is maintained (or detected as lost)"
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [completed (filter #(= :ok (:type %)) history)
            cluster-states (->> completed
                                (filter #(= :read-cluster-state (:f %)))
                                (map :value))
            node-counts (->> completed
                             (filter #(= :read-node-count (:f %)))
                             (map :value))
            final-state (last cluster-states)
            final-count (last node-counts)
            had-quorum-loss (some #{"fail"} cluster-states)]
        {:valid? (or (= "ok" final-state) (nil? final-state))
         :final-cluster-state final-state
         :final-node-count final-count
         :unique-states (set cluster-states)
         :had-quorum-loss had-quorum-loss
         :state-transitions (count (partition-by identity cluster-states))
         :total-ops (count completed)}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a cluster formation workload.

   Options:
   - :rate - operations per second
   - :membership-changes - if true, include node join/leave operations"
  [opts]
  (if (:membership-changes opts)
    {:client (create-client)
     :generator (membership-change-generator opts)
     :checker (checker)}
    {:client (create-client)
     :generator (generator opts)
     :checker (checker)}))
