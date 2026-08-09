(ns jepsen.frogdb.zombie-raft
  "Zombie ex-primary detection workload for the FrogDB Raft cluster topology.

   Ports jepsen.frogdb.zombie (replication topology: an isolated old primary
   must not keep accepting writes, and any 'zombie' writes it does accept
   must not survive after the partition heals) to a real 3-node Raft cluster
   (n1, n2, n3 — the `--cluster` default).

   Redis Cluster's classic 'zombie' scenario is a demoted PRIMARY that keeps
   serving after a REPLICA is promoted in its place. FrogDB's raft-cluster
   mode has no such handoff within a shard: ADR-0001 says Raft carries
   cluster metadata only, so a freshly formed cluster is all primaries, each
   owning a disjoint slot range with zero replicas — nothing else can ever
   take over an isolated node's slots, so there is no promoted successor to
   compare against. See split-brain-raft.clj's docstring for the same
   architectural point.

   What *does* directly port is the write-durability half of zombie.clj's
   property: a write the server told the client was REJECTED (CLUSTERDOWN, a
   self-fencing decision under self_fence_on_quorum_loss) must never actually
   have been applied — no 'phantom commit'. This workload isolates n1 (the
   nemesis's :primary-isolated target — see split-brain-raft.clj for why this
   is a genuine 2-vs-1 Raft-quorum split on the 3-node default cluster),
   hammers writes at n1's own key throughout, and after the test asserts:
     1. no value that was EVER accepted only under a fenced (CLUSTERDOWN)
        reply is later observed as n1's live value (a phantom commit — the
        server silently applied a write it told the client it rejected), and
     2. n1 is unfenced again in the final post-heal snapshot (recovered /
        rejoined consistently).

   Nemesis: the same :partition / :primary-isolated op as split-brain-raft
   (jepsen.frogdb.nemesis/partition-nemesis). No new nemesis wiring needed.

   Operations:
   - :write {value}  - write to n1's own key (the isolation target)
   - :read           - read n1's own key
   - :read-all       - snapshot read of every node's own key (used for the
     final-reads phase and the post-heal recovery check)"
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [jepsen.frogdb.cluster-db :as cluster-db]
            [jepsen.frogdb.split-brain-raft :as split-brain-raft]
            [taoensso.carmine :as car :refer [wcar]]))

(def raft-nodes ["n1" "n2" "n3"])
(def isolated-node "n1")

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord ZombieRaftClient [conns node-keys docker-host? base-port]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          nodes (or (:cluster-nodes test) raft-nodes)
          bp (get test :base-port cluster-db/default-base-port)
          conns (into {} (for [n nodes] [n (cluster-db/conn-for-raft-node-single n docker? bp)]))
          ref-conn (get conns (first nodes))
          node-keys (split-brain-raft/resolve-node-keys ref-conn nodes)]
      (info "Opening zombie-raft client, nodes:" nodes "keys:" node-keys)
      (assoc this :conns conns :node-keys node-keys :docker-host? docker? :base-port bp)))

  (setup! [this test]
    (doseq [[n k] node-keys]
      (try
        (wcar (get conns n) (car/set k "0"))
        (catch Exception e
          (warn "zombie-raft setup! failed to seed" n ":" (.getMessage e)))))
    (Thread/sleep 500)
    this)

  (invoke! [this test op]
    (case (:f op)
      :write
      (let [value (:value op)
            conn (get conns isolated-node)
            k (get node-keys isolated-node)]
        (try
          (wcar conn (car/set k (str value)))
          (assoc op :type :ok :value value)
          (catch java.net.ConnectException e
            (assoc op :type :fail :error :connection-refused))
          (catch clojure.lang.ExceptionInfo e
            (if (split-brain-raft/clusterdown-ex? e)
              (assoc op :type :ok :fenced true :value value)
              (assoc op :type :fail :error [:redis-error (str (:message (ex-data e)))])))
          (catch Exception e
            (assoc op :type :info :error [:unexpected (.getMessage e)]))))

      :read
      (let [conn (get conns isolated-node)
            k (get node-keys isolated-node)]
        (try
          (assoc op :type :ok :value (frogdb/read-register conn k))
          (catch java.net.ConnectException e
            (assoc op :type :fail :error :connection-refused))
          (catch clojure.lang.ExceptionInfo e
            (if (split-brain-raft/clusterdown-ex? e)
              (assoc op :type :ok :fenced true :value nil)
              (assoc op :type :fail :error [:redis-error (str (:message (ex-data e)))])))
          (catch Exception e
            (assoc op :type :info :error [:unexpected (.getMessage e)]))))

      :read-all
      (let [results (for [n (keys conns)
                          :let [conn (get conns n)
                                k (get node-keys n)]]
                      (try
                        [n {:value (when k (frogdb/read-register conn k))}]
                        (catch java.net.ConnectException e [n {:error :connection-refused}])
                        (catch clojure.lang.ExceptionInfo e
                          (if (split-brain-raft/clusterdown-ex? e)
                            [n {:fenced true}]
                            [n {:error [:redis-error (str (:message (ex-data e)))]}]))
                        (catch Exception e [n {:error [:unexpected (.getMessage e)]}])))]
        (assoc op :type :ok :value (into {} results)))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (doseq [[_ c] conns] (frogdb/close-conn! c))))

(defn create-client
  "Create a new zombie-raft client."
  []
  (map->ZombieRaftClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn write-op [value]
  {:type :invoke :f :write :value value})

(defn read-op []
  {:type :invoke :f :read :value nil})

(defn read-all-op []
  {:type :invoke :f :read-all :value nil})

(defn generator
  "Generator for the zombie-raft workload.
   Hammers writes/reads at the isolated node (n1) with periodic full-cluster
   snapshots (which double as the post-heal recovery signal)."
  [opts]
  (let [rate (get opts :rate 10)
        counter (atom 0)]
    (->> (gen/mix [(fn [] (write-op (swap! counter inc)))
                   (fn [] (write-op (swap! counter inc)))
                   (fn [] (read-op))
                   (fn [] (read-all-op))])
         (gen/stagger (/ 1 rate)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn checker
  "Checker for the zombie-raft workload.

   Verifies:
   - No phantom commit: a value that was ONLY ever accepted under a fenced
     (CLUSTERDOWN) reply never later appears as n1's observed live value.
   - n1 recovers (unfenced) in the final post-heal snapshot."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [writes (->> history
                        (filter #(and (= :write (:f %)) (= :ok (:type %)))))
            fenced-values (set (map :value (filter :fenced writes)))
            accepted-values (set (map :value (remove :fenced writes)))
            ;; A value that was ONLY ever fenced (never also separately
            ;; accepted, e.g. via a distinct write op) is a phantom-commit
            ;; candidate if it is later observed as n1's live value.
            phantom-candidates (set/difference fenced-values accepted-values)

            observed-values (->> history
                                 (filter #(and (#{:read :read-all} (:f %)) (= :ok (:type %))))
                                 (mapcat (fn [op]
                                           (case (:f op)
                                             :read (when-not (:fenced op) [(:value op)])
                                             :read-all (when-let [n1 (get (:value op) "n1")]
                                                         (when-not (:fenced n1) [(:value n1)])))))
                                 (remove nil?)
                                 set)

            phantom-commits (set/intersection phantom-candidates observed-values)

            unexpected-errors (->> history
                                   (filter #(and (#{:write :read :read-all} (:f %))
                                                 (= :info (:type %))))
                                   count)

            reads-all (->> history
                          (filter #(and (= :read-all (:f %)) (= :ok (:type %))))
                          (map :value))
            final-read-all (last reads-all)
            recovered? (or (nil? final-read-all)
                           (every? (fn [[_ v]] (not (:fenced v))) final-read-all))]

        {:valid? (and (empty? phantom-commits)
                      (zero? unexpected-errors)
                      recovered?)
         :isolated-node isolated-node
         :accepted-writes (count accepted-values)
         :fenced-writes (count fenced-values)
         :phantom-commits phantom-commits
         :unexpected-errors unexpected-errors
         :recovered? recovered?
         :final-read-all final-read-all}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a zombie-raft workload.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (generator opts)
   :checker (checker)})
