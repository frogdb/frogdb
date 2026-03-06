(ns jepsen.frogdb.membership
  "Membership change nemesis for FrogDB Raft cluster.

   Dynamically adds/removes nodes from the Raft cluster during tests.
   Maintains quorum safety by never removing below 3 live nodes.

   Operations:
   - :join  - Start a stopped node's container and CLUSTER MEET it
   - :leave - CLUSTER FORGET a non-essential node on all live nodes
   - :membership-status - Return current membership state

   Initial state: n1-n3 live, n4-n5 out (available for join/leave)."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.frogdb.cluster-db :as cluster-db]
            [slingshot.slingshot :refer [try+ throw+]]))

;; ===========================================================================
;; Membership State
;; ===========================================================================

(defn initial-membership
  "Create initial membership state.
   First `n` nodes are :live, rest are :out."
  [initial-nodes all-nodes]
  (let [live-set (set initial-nodes)]
    (into {} (for [node all-nodes]
               [node (if (live-set node) :live :out)]))))

(defn live-nodes
  "Get nodes currently in :live state."
  [membership]
  (keys (filter #(= :live (val %)) membership)))

(defn out-nodes
  "Get nodes currently in :out state."
  [membership]
  (keys (filter #(= :out (val %)) membership)))

;; ===========================================================================
;; Join Operation
;; ===========================================================================

(defn join-node!
  "Join a node to the cluster:
   1. docker start the container
   2. Wait for it to be ready
   3. CLUSTER MEET from a live node
   4. Wait for gossip propagation"
  [node live-node docker-host? base-port]
  (let [container (cluster-db/raft-container-name node)
        ip (get cluster-db/raft-cluster-node-ips node)]
    (info "Joining node" node "to cluster via" live-node)
    ;; Start the container
    (try+
      (cluster-db/docker-start container)
      (catch Object e
        (warn "Container start failed (may already be running):" e)))
    ;; Wait for node to be ready
    (cluster-db/wait-for-node-ready node docker-host? 15000 base-port)
    ;; CLUSTER MEET from a live node
    (let [live-conn (cluster-db/conn-for-raft-node live-node docker-host? base-port)]
      (cluster-db/cluster-meet! live-conn ip 6379))
    ;; Wait for gossip
    (Thread/sleep 2000)
    (info "Node" node "joined successfully")))

;; ===========================================================================
;; Leave Operation
;; ===========================================================================

(defn forget-node-on-all!
  "Issue CLUSTER FORGET for a node on all other live nodes.
   Must complete within 60s gossip window or the node re-appears."
  [node-to-forget live-nodes-list docker-host? base-port]
  (let [others (remove #{node-to-forget} live-nodes-list)]
    ;; First, get the node-id of the node to forget
    (let [any-live (first others)
          any-conn (cluster-db/conn-for-raft-node any-live docker-host? base-port)
          node-ip (get cluster-db/raft-cluster-node-ips node-to-forget)
          node-id (cluster-db/get-node-id any-conn node-ip)]
      (if-not node-id
        (do (warn "Could not find node-id for" node-to-forget "at" node-ip)
            false)
        (do
          (info "Forgetting node" node-to-forget "(id:" node-id ") on" (count others) "nodes")
          ;; Issue FORGET on all live nodes in tight loop
          (doseq [n others]
            (try+
              (let [conn (cluster-db/conn-for-raft-node n docker-host? base-port)]
                (cluster-db/cluster-forget! conn node-id))
              (catch Object e
                (warn "CLUSTER FORGET failed on" n ":" e))))
          true)))))

;; ===========================================================================
;; Nemesis Implementation
;; ===========================================================================

(defn membership-nemesis
  "Creates a nemesis that dynamically changes cluster membership.

   Options:
   - :initial-nodes - nodes that start as live (default [\"n1\" \"n2\" \"n3\"])
   - :all-nodes     - all possible nodes (default [\"n1\"..\"n5\"])
   - :docker-host?  - whether running from Docker host (default true)
   - :base-port     - base host port (default 16379)"
  [opts]
  (let [initial (get opts :initial-nodes ["n1" "n2" "n3"])
        all (get opts :all-nodes ["n1" "n2" "n3" "n4" "n5"])
        docker-host? (get opts :docker-host? true)
        base-port (get opts :base-port cluster-db/default-base-port)
        membership (atom (initial-membership initial all))]
    (reify nemesis/Nemesis
      (setup! [this test]
        (reset! membership (initial-membership initial all))
        this)

      (invoke! [this test op]
        (case (:f op)
          :join
          (let [candidates (out-nodes @membership)]
            (if (empty? candidates)
              (assoc op :type :info :value :no-nodes-to-join)
              (let [node (rand-nth (vec candidates))
                    live (live-nodes @membership)]
                (if (empty? live)
                  (assoc op :type :fail :error :no-live-nodes)
                  (try+
                    (join-node! node (first live) docker-host? base-port)
                    (swap! membership assoc node :live)
                    (assoc op :type :ok :value {:joined node
                                                :live-count (count (live-nodes @membership))})
                    (catch Object e
                      (warn "Join failed for" node ":" e)
                      (assoc op :type :fail :error [:join-failed (str e)])))))))

          :leave
          (let [live (live-nodes @membership)
                ;; Never remove below 3 live nodes (quorum safety)
                removable (when (> (count live) 3)
                            ;; Prefer removing spare nodes (n4/n5) over initial members
                            (let [spares (filter #(not (contains? (set initial) %)) live)]
                              (if (seq spares)
                                spares
                                ;; Fall back to any removable node
                                live)))]
            (if (empty? removable)
              (assoc op :type :info :value :cannot-remove-below-quorum)
              (let [node (rand-nth (vec removable))]
                (try+
                  (let [success? (forget-node-on-all! node live docker-host? base-port)]
                    (if success?
                      (do
                        (swap! membership assoc node :out)
                        (assoc op :type :ok :value {:removed node
                                                    :live-count (count (live-nodes @membership))}))
                      (assoc op :type :fail :error :forget-failed)))
                  (catch Object e
                    (warn "Leave failed for" node ":" e)
                    (assoc op :type :fail :error [:leave-failed (str e)]))))))

          :membership-status
          (assoc op :type :ok :value @membership)))

      (teardown! [this test]
        ;; Ensure all nodes are started so cluster can recover for next test
        (doseq [node all]
          (try+
            (cluster-db/docker-start (cluster-db/raft-container-name node))
            (catch Object _ nil)))))))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn membership-generator
  "Generator for membership changes.
   Probabilistic selection biased toward maintaining 3-5 live nodes.
   Interval ~15s between changes.

   Options:
   - :interval - time between membership changes (default 15s)"
  [opts]
  (let [interval (get opts :interval 15)]
    (gen/cycle
      [(gen/sleep interval)
       (gen/once
         (rand-nth
           [;; Join a node
            {:type :info :f :join}
            ;; Leave a node
            {:type :info :f :leave}
            ;; Status check
            {:type :info :f :membership-status}]))
       (gen/sleep 5)])))

;; ===========================================================================
;; Nemesis Package
;; ===========================================================================

(defn membership-package
  "Nemesis package for membership changes, composed with existing
   raft-cluster nemesis for kill/pause/partition.

   Options:
   - :interval - time between membership changes (default 15s)"
  [opts]
  (let [membership-nem (membership-nemesis opts)]
    {:nemesis membership-nem
     :generator (membership-generator opts)
     :final-generator (gen/once {:type :info :f :membership-status})}))
