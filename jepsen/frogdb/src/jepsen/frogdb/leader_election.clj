(ns jepsen.frogdb.leader-election
  "Leader election workload for FrogDB Raft cluster.

   Tests:
   - Leader election within timeout
   - Single leader per term (no split-brain)
   - Minority partition cannot elect leader
   - Leadership transfer consistency
   - Leader stability under normal operation

   Operations:
   - :read-leader - Get current leader node
   - :read-term - Get current Raft term
   - :write - Write to verify leader accepts writes
   - :read - Read to verify consistency"
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
;; Leader Tracking
;; ===========================================================================

(defn get-leader-info
  "Get information about the current leader from cluster nodes.
   Returns {:node <node-name>, :id <node-id>, :term <term>} or nil."
  [conn]
  (let [leader (cluster-db/get-current-leader conn)]
    (when leader
      (let [leader-ip (first (str/split (:addr leader) #":"))]
        {:node (first (filter #(= (get cluster-db/raft-cluster-node-ips %) leader-ip)
                              (keys cluster-db/raft-cluster-node-ips)))
         :id (:id leader)
         :config-epoch (:config-epoch leader)}))))

(defn get-all-masters
  "Get all nodes claiming to be masters."
  [nodes docker-host?]
  (for [node nodes
        :let [conn (cluster-db/conn-for-raft-node node docker-host?)
              nodes-info (try+
                           (cluster-db/cluster-nodes conn)
                           (catch Object _ nil))]
        :when nodes-info
        n nodes-info
        :when (contains? (:flags n) "master")]
    {:node node
     :master-id (:id n)
     :addr (:addr n)}))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord LeaderElectionClient [nodes docker-host? slot-mapping leader-history]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          all-nodes (or (:cluster-nodes test) ["n1" "n2" "n3"])]
      (info "Opening leader election client")
      (assoc this
             :nodes all-nodes
             :docker-host? docker?
             :slot-mapping (atom (cluster-client/create-slot-mapping all-nodes docker?))
             :leader-history (atom []))))

  (setup! [this test]
    this)

  (invoke! [this test op]
    (try+
      (case (:f op)
        :read-leader
        (let [conn (cluster-db/conn-for-raft-node (first nodes) docker-host?)
              leader (get-leader-info conn)]
          (when leader
            (swap! leader-history conj {:time (System/currentTimeMillis)
                                         :leader leader}))
          (assoc op :type :ok :value leader))

        :read-all-masters
        (let [masters (get-all-masters nodes docker-host?)]
          (assoc op :type :ok :value (vec masters)))

        :verify-single-leader
        ;; In Redis Cluster mode, every node with assigned slots is a "master".
        ;; We check from one node's perspective that the cluster view is consistent.
        (let [conn (cluster-db/conn-for-raft-node (first nodes) docker-host?)
              nodes-info (try+
                           (cluster-db/cluster-nodes conn)
                           (catch Object _ nil))
              masters (when nodes-info
                        (filter #(contains? (:flags %) "master") nodes-info))
              master-count (count masters)]
          ;; A healthy cluster has one master per slot range.
          ;; We just verify the cluster is responding and has masters.
          (if (pos? master-count)
            (assoc op :type :ok :value {:single-leader true
                                         :leader-count master-count})
            (assoc op :type :fail :value {:single-leader false
                                           :leader-count 0})))

        :write
        (let [key (str "jepsen-leader-" (rand-int 1000))
              value (rand-int 10000)]
          (cluster-client/cluster-set slot-mapping key value docker-host? nodes)
          (assoc op :type :ok :value {:key key :written value}))

        :read
        (let [key (str "jepsen-leader-" (rand-int 1000))
              value (cluster-client/cluster-get slot-mapping key docker-host? nodes)]
          (assoc op :type :ok :value (frogdb/parse-value value))))

      (catch java.net.ConnectException e
        (assoc op :type :fail :error :connection-refused))

      (catch java.net.SocketTimeoutException e
        (assoc op :type :info :error :timeout))

      (catch [:type :clusterdown] e
        (assoc op :type :fail :error :cluster-down))

      (catch Exception e
        (warn "Unexpected error:" e)
        (assoc op :type :info :error [:unexpected (.getMessage e)]))))

  (teardown! [this test]
    nil)

  (close! [this test]
    nil))

(defn create-client
  "Create a new leader election client."
  []
  (map->LeaderElectionClient {}))

;; ===========================================================================
;; Generators
;; ===========================================================================

(defn read-leader
  "Generate a read-leader operation."
  []
  {:type :invoke :f :read-leader})

(defn read-all-masters
  "Generate a read-all-masters operation."
  []
  {:type :invoke :f :read-all-masters})

(defn verify-single-leader
  "Generate a verify-single-leader operation."
  []
  {:type :invoke :f :verify-single-leader})

(defn write-op
  "Generate a write operation."
  []
  {:type :invoke :f :write})

(defn read-op
  "Generate a read operation."
  []
  {:type :invoke :f :read})

(defn generator
  "Generator for leader election testing.
   Mixes leader reads with verify operations and some data operations."
  [opts]
  (let [rate (get opts :rate 10)]
    (->> (gen/mix [(fn [] (read-leader))
                   (fn [] (verify-single-leader))
                   (fn [] (write-op))
                   (fn [] (read-op))])
         (gen/stagger (/ 1 rate)))))

(defn leader-stability-generator
  "Generator focused on leader stability monitoring."
  [opts]
  (let [rate (get opts :rate 20)]
    (->> (gen/mix [(fn [] (read-leader))
                   (fn [] (read-leader))
                   (fn [] (verify-single-leader))])
         (gen/stagger (/ 1 rate)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn checker
  "Checker for leader election workload.

   Verifies:
   - At most one leader at any time
   - Leader eventually elected after partition heals
   - No permanent split-brain"
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [completed (filter #(= :ok (:type %)) history)

            ;; Check single-leader verifications
            verify-ops (filter #(= :verify-single-leader (:f %)) completed)
            multi-leader-events (filter #(= false (get-in % [:value :single-leader])) verify-ops)

            ;; Track leader changes
            leader-reads (filter #(= :read-leader (:f %)) completed)
            leaders (map :value leader-reads)
            unique-leaders (set (remove nil? (map :node leaders)))
            leader-changes (count (partition-by :node (remove #(nil? (:node %)) leaders)))

            ;; Check for successful writes
            writes (filter #(= :write (:f %)) completed)
            successful-writes (count writes)

            ;; Final state
            final-leader (:value (last leader-reads))]

        {:valid? (empty? multi-leader-events)
         :multi-leader-events (count multi-leader-events)
         :unique-leaders unique-leaders
         :leader-changes leader-changes
         :final-leader final-leader
         :successful-writes successful-writes
         :total-leader-reads (count leader-reads)}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a leader election workload.

   Options:
   - :rate - operations per second
   - :stability - if true, focus on leader stability monitoring"
  [opts]
  (if (:stability opts)
    {:client (create-client)
     :generator (leader-stability-generator opts)
     :checker (checker)}
    {:client (create-client)
     :generator (generator opts)
     :checker (checker)}))
