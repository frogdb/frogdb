(ns jepsen.frogdb.split-brain-raft
  "Split-brain detection workload for the FrogDB Raft cluster topology.

   Ports jepsen.frogdb.split-brain (replication topology: a demoted primary
   still reachable by some clients might keep accepting writes) to a real
   3-node Raft cluster (n1, n2, n3 — the `--cluster` default, see
   jepsen.frogdb.core/frogdb-test's :cluster-nodes default of 3).

   Replication topology has one shared dataset (primary + 2 replicas) and a
   ROLE (master/slave) distinction to check. Raft-cluster mode has neither:
   ADR-0001 says Raft carries cluster METADATA only, never key data, so every
   node that owns slots independently serves its own disjoint slot range and
   reports ROLE master (there is no replica in a freshly formed cluster
   shard — see leader-election.clj's :verify-single-leader, which already
   notes this). So \"split-brain\" here cannot mean \"two nodes both claim to
   own the same key\" the way it does under replication; it means the
   opposite failure of the *same* underlying property: a node that has lost
   Raft quorum (self_fence_on_quorum_loss, clustering.md) must stop accepting
   writes to the slots it owns rather than keep serving as if nothing
   happened, while nodes that retain quorum must keep serving normally
   (availability). Each node gets its own tracked key (a key hashed into that
   node's currently-owned slot range, resolved via CLUSTER SLOTS at setup!),
   and the property under test is per-node self-fencing, not cross-node value
   agreement.

   Nemesis: reuses the existing :partition nemesis's :primary-isolated op
   (jepsen.frogdb.nemesis/partition-nemesis -> frogdb-db/isolate-primary!),
   which is already topology-aware and, on the :raft topology, isolates n1
   bidirectionally from n2 and n3 by container IP (jepsen.frogdb.db/
   isolate-primary!). On the 3-node default Raft cluster this is a genuine
   2-vs-1 Raft quorum split: n1 (minority, 1 of 3) loses quorum while n2+n3
   (majority, 2 of 3) retain it. No new nemesis wiring was needed — this is
   the same nemesis already used by leader-election-partition, cross-slot-
   partition and slot-migration-partition on this topology.

   Operations:
   - :write-node {node value} - direct (non-redirecting) write to `node`'s
     own key
   - :read-node node          - direct (non-redirecting) read of `node`'s
     own key
   - :read                    - snapshot read of every node's own key (used
     for the final-reads phase and the post-heal convergence check)"
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [jepsen.frogdb.cluster-db :as cluster-db]
            [jepsen.frogdb.key-routing :as key-routing]
            [taoensso.carmine :as car :refer [wcar]]))

(def raft-nodes ["n1" "n2" "n3"])

;; The nemesis (see docstring above) always isolates n1 on this topology.
(def isolated-node "n1")

;; ===========================================================================
;; Slot/Key Resolution
;; ===========================================================================

(defn- node-first-owned-slot
  "First slot number `node` owns, read live from CLUSTER SLOTS on `ref-conn`.
   Returns nil if `node` currently owns no slots."
  [ref-conn node]
  (let [node-ip (get cluster-db/raft-cluster-node-ips node)]
    (some (fn [row]
            (let [start (long (nth row 0))
                  master (nth row 2)]
              (when (= node-ip (str (nth master 0)))
                start)))
          (cluster-db/cluster-slots ref-conn))))

(defn resolve-node-keys
  "Map of node -> a key that hashes into that node's currently-owned slot
   range, so a direct SET/GET to that node's own connection is never MOVED."
  [ref-conn nodes]
  (into {}
        (keep (fn [n]
                (when-let [slot (node-first-owned-slot ref-conn n)]
                  [n (key-routing/key-for-slot slot)]))
              nodes)))

;; ===========================================================================
;; Error Classification
;; ===========================================================================

(defn clusterdown-ex?
  "True when `e` (a clojure.lang.ExceptionInfo from Carmine) is a CLUSTERDOWN
   reply. Mirrors the :prefix/message classification in
   jepsen.frogdb.client/with-error-handling."
  [e]
  (let [data (ex-data e)
        prefix (:prefix data)
        msg (loop [t e acc (str (:message data))]
              (if t (recur (.getCause t) (str acc " " (.getMessage t))) acc))]
    (or (= prefix :clusterdown)
        (and msg (str/includes? msg "CLUSTERDOWN")))))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord SplitBrainRaftClient [conns node-keys docker-host? base-port]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          nodes (or (:cluster-nodes test) raft-nodes)
          bp (get test :base-port cluster-db/default-base-port)
          conns (into {} (for [n nodes] [n (cluster-db/conn-for-raft-node-single n docker? bp)]))
          ref-conn (get conns (first nodes))
          node-keys (resolve-node-keys ref-conn nodes)]
      (info "Opening split-brain-raft client, nodes:" nodes "keys:" node-keys)
      (assoc this :conns conns :node-keys node-keys :docker-host? docker? :base-port bp)))

  (setup! [this test]
    (doseq [[n k] node-keys]
      (try
        (wcar (get conns n) (car/set k "0"))
        (catch Exception e
          (warn "split-brain-raft setup! failed to seed" n ":" (.getMessage e)))))
    (Thread/sleep 500)
    this)

  (invoke! [this test op]
    (case (:f op)
      :write-node
      (let [{:keys [node value]} (:value op)
            conn (get conns node)
            k (get node-keys node)]
        (if (or (not conn) (not k))
          (assoc op :type :fail :error :unknown-node)
          (try
            (wcar conn (car/set k (str value)))
            (assoc op :type :ok :value {:node node :value value})
            (catch java.net.ConnectException e
              (assoc op :type :fail :error :connection-refused))
            (catch clojure.lang.ExceptionInfo e
              (if (clusterdown-ex? e)
                (assoc op :type :ok :value {:node node :fenced true})
                (assoc op :type :fail :error [:redis-error (str (:message (ex-data e)))])))
            (catch Exception e
              (assoc op :type :info :error [:unexpected (.getMessage e)])))))

      :read-node
      (let [node (:value op)
            conn (get conns node)
            k (get node-keys node)]
        (if (or (not conn) (not k))
          (assoc op :type :fail :error :unknown-node)
          (try
            (let [v (frogdb/read-register conn k)]
              (assoc op :type :ok :value {:node node :value v}))
            (catch java.net.ConnectException e
              (assoc op :type :fail :error :connection-refused))
            (catch clojure.lang.ExceptionInfo e
              (if (clusterdown-ex? e)
                (assoc op :type :ok :value {:node node :fenced true})
                (assoc op :type :fail :error [:redis-error (str (:message (ex-data e)))])))
            (catch Exception e
              (assoc op :type :info :error [:unexpected (.getMessage e)])))))

      ;; Snapshot read of every node's own key (also used by the final-reads
      ;; phase, which invokes :read).
      :read
      (let [results (for [n (keys conns)
                          :let [conn (get conns n)
                                k (get node-keys n)]]
                      (try
                        [n {:value (when k (frogdb/read-register conn k))}]
                        (catch java.net.ConnectException e [n {:error :connection-refused}])
                        (catch clojure.lang.ExceptionInfo e
                          (if (clusterdown-ex? e)
                            [n {:fenced true}]
                            [n {:error [:redis-error (str (:message (ex-data e)))]}]))
                        (catch Exception e [n {:error [:unexpected (.getMessage e)]}])))]
        (assoc op :type :ok :value (into {} results)))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (doseq [[_ c] conns] (frogdb/close-conn! c))))

(defn create-client
  "Create a new split-brain-raft client."
  []
  (map->SplitBrainRaftClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn write-node-op [node value]
  {:type :invoke :f :write-node :value {:node node :value value}})

(defn read-node-op [node]
  {:type :invoke :f :read-node :value node})

(defn read-op []
  {:type :invoke :f :read :value nil})

(defn generator
  "Generator for the split-brain-raft workload.
   Writes to the isolated node (n1) as well as the majority (n2, n3) so the
   checker can compare their fates, plus periodic full-cluster snapshots."
  [opts]
  (let [rate (get opts :rate 10)
        counter (atom 0)]
    (->> (gen/mix [(fn [] (write-node-op isolated-node (swap! counter inc)))
                   (fn [] (write-node-op (rand-nth ["n2" "n3"]) (swap! counter inc)))
                   (fn [] (read-node-op (rand-nth raft-nodes)))
                   (fn [] (read-op))])
         (gen/stagger (/ 1 rate)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn checker
  "Checker for the split-brain-raft workload.

   Verifies:
   - The majority nodes (n2, n3) never self-fence (CLUSTERDOWN) — the
     defining availability property of retaining Raft quorum.
   - No unexpected errors escape (harness/routing bugs).
   - After the test's final heal, a full-cluster snapshot shows every node
     unfenced (post-heal convergence / recovery)."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [writes (->> history
                        (filter #(and (= :write-node (:f %)) (= :ok (:type %))))
                        (map :value))
            majority-writes (filter #(not= isolated-node (:node %)) writes)
            majority-fenced (filter :fenced majority-writes)
            isolated-writes (filter #(= isolated-node (:node %)) writes)
            isolated-fenced (filter :fenced isolated-writes)
            isolated-ok (remove :fenced isolated-writes)

            unexpected-errors (->> history
                                   (filter #(and (#{:write-node :read-node :read} (:f %))
                                                 (= :info (:type %))))
                                   count)

            reads (->> history
                      (filter #(and (= :read (:f %)) (= :ok (:type %))))
                      (map :value))
            final-read (last reads)
            recovered? (or (nil? final-read)
                           (every? (fn [[_ v]] (not (:fenced v))) final-read))]

        {:valid? (and (empty? majority-fenced)
                      (zero? unexpected-errors)
                      recovered?)
         :isolated-node isolated-node
         :isolated-writes-ok (count isolated-ok)
         :isolated-writes-fenced (count isolated-fenced)
         :majority-writes-ok (count majority-writes)
         :majority-fenced-events (count majority-fenced)
         :unexpected-errors unexpected-errors
         :recovered? recovered?
         :final-read final-read}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a split-brain-raft workload.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (generator opts)
   :checker (checker)})
