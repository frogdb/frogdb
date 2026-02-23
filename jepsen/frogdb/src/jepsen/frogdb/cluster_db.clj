(ns jepsen.frogdb.cluster-db
  "Database lifecycle management for FrogDB Raft cluster mode.

   Provides:
   - cluster-db: Reified db/DB for Raft cluster startup/shutdown
   - Cluster health monitoring and slot assignment
   - Leader election detection
   - Cluster state management

   Implements the Jepsen db/DB protocol for multi-node Raft clusters."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn error debug]]
            [jepsen.db :as db]
            [jepsen.frogdb.client :as client]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]])
  (:import [java.lang ProcessBuilder]
           [java.io BufferedReader InputStreamReader]))

;; ===========================================================================
;; Constants
;; ===========================================================================

(def total-slots 16384)
(def cluster-ok-timeout-ms 60000)
(def leader-election-timeout-ms 30000)

;; ===========================================================================
;; Docker Utilities (copied from db.clj for independence)
;; ===========================================================================

(defn docker-exec
  "Execute a command in a Docker container.
   Returns the output as a string, or throws on non-zero exit."
  [container & args]
  (let [cmd (into ["docker" "exec" container] args)
        pb (ProcessBuilder. ^java.util.List cmd)
        _ (.redirectErrorStream pb true)
        proc (.start pb)
        reader (BufferedReader. (InputStreamReader. (.getInputStream proc)))
        output (str/join "\n" (line-seq reader))
        exit-code (.waitFor proc)]
    (when (not= 0 exit-code)
      (throw+ {:type :docker-exec-failed
               :container container
               :command args
               :exit-code exit-code
               :output output}))
    output))

(defn docker-exec-ignore-error
  "Execute a command in a Docker container, ignoring errors."
  [container & args]
  (try+
    (apply docker-exec container args)
    (catch Object _
      nil)))

(defn docker-start
  "Start a Docker container."
  [container]
  (let [cmd ["docker" "start" container]
        pb (ProcessBuilder. ^java.util.List cmd)
        _ (.redirectErrorStream pb true)
        proc (.start pb)
        exit-code (.waitFor proc)]
    (when (not= 0 exit-code)
      (throw+ {:type :docker-start-failed
               :container container
               :exit-code exit-code}))))

(defn docker-kill-process
  "Kill the FrogDB process inside a container with the given signal."
  [container signal]
  (try+
    (docker-exec container "pkill" (str "-" signal) "frogdb")
    (catch Object _
      nil)))

(defn docker-pause-process
  "Pause the FrogDB process inside a container using SIGSTOP."
  [container]
  (docker-kill-process container "STOP"))

(defn docker-resume-process
  "Resume a paused FrogDB process inside a container using SIGCONT."
  [container]
  (docker-kill-process container "CONT"))

(defn docker-hard-kill
  "Hard kill the FrogDB process inside a container using SIGKILL."
  [container]
  (docker-kill-process container "KILL"))

;; ===========================================================================
;; Raft Cluster Node Configuration
;; ===========================================================================

(def raft-cluster-node-ips
  "Map of node names to their IP addresses in the Raft cluster Docker network."
  {"n1" "172.21.0.2"
   "n2" "172.21.0.3"
   "n3" "172.21.0.4"
   "n4" "172.21.0.5"
   "n5" "172.21.0.6"})

(def raft-cluster-host-ports
  "Map of node names to their host ports (for testing from host)."
  {"n1" {:host "localhost" :port 6379}
   "n2" {:host "localhost" :port 6380}
   "n3" {:host "localhost" :port 6381}
   "n4" {:host "localhost" :port 6382}
   "n5" {:host "localhost" :port 6383}})

(defn get-node-for-ip
  "Find node name for an IP address."
  [ip]
  (first (filter #(= (get raft-cluster-node-ips %) ip)
                 (keys raft-cluster-node-ips))))

(defn raft-container-name
  "Get the Docker container name for a Raft cluster node."
  [node]
  (str "frogdb-raft-" node))

(defn conn-for-raft-node
  "Create a connection spec for a Raft cluster node."
  [node docker-host?]
  (let [resolved (if docker-host?
                   (get raft-cluster-host-ports node {:host node :port 6379})
                   {:host node :port 6379})]
    {:pool {}
     :spec {:host (:host resolved)
            :port (:port resolved)
            :timeout-ms client/default-timeout-ms}}))

;; ===========================================================================
;; Cluster Commands
;; ===========================================================================

(defn cluster-info
  "Execute CLUSTER INFO and return as a map."
  [conn]
  (let [info-str (wcar conn (car/redis-call ["CLUSTER" "INFO"]))]
    (when info-str
      (->> (str/split info-str #"\r?\n")
           (remove str/blank?)
           (map #(str/split % #":"))
           (filter #(= 2 (count %)))
           (into {})))))

(defn cluster-nodes
  "Execute CLUSTER NODES and return as parsed data."
  [conn]
  (let [nodes-str (wcar conn (car/redis-call ["CLUSTER" "NODES"]))]
    (when nodes-str
      (->> (str/split nodes-str #"\r?\n")
           (remove str/blank?)
           (map (fn [line]
                  (let [parts (str/split line #" ")]
                    {:id (nth parts 0)
                     :addr (nth parts 1)
                     :flags (set (str/split (nth parts 2) #","))
                     :master-id (nth parts 3)
                     :ping-sent (nth parts 4)
                     :pong-recv (nth parts 5)
                     :config-epoch (nth parts 6)
                     :link-state (nth parts 7)
                     :slots (when (> (count parts) 8)
                              (subvec (vec parts) 8))})))))))

(defn cluster-slots
  "Execute CLUSTER SLOTS and return slot mapping."
  [conn]
  (wcar conn (car/redis-call ["CLUSTER" "SLOTS"])))

(defn cluster-meet!
  "Execute CLUSTER MEET to add a node to the cluster."
  [conn ip port]
  (wcar conn (car/redis-call ["CLUSTER" "MEET" ip (str port)])))

(defn cluster-forget!
  "Execute CLUSTER FORGET to remove a node from the cluster."
  [conn node-id]
  (wcar conn (car/redis-call ["CLUSTER" "FORGET" node-id])))

(defn cluster-addslots!
  "Execute CLUSTER ADDSLOTS to assign slots to current node."
  [conn slots]
  (wcar conn (car/redis-call (into ["CLUSTER" "ADDSLOTS"] (map str slots)))))

(defn cluster-setslot-node!
  "Execute CLUSTER SETSLOT <slot> NODE <node-id>."
  [conn slot node-id]
  (wcar conn (car/redis-call ["CLUSTER" "SETSLOT" (str slot) "NODE" node-id])))

(defn cluster-setslot-migrating!
  "Execute CLUSTER SETSLOT <slot> MIGRATING <node-id>."
  [conn slot dest-node-id]
  (wcar conn (car/redis-call ["CLUSTER" "SETSLOT" (str slot) "MIGRATING" dest-node-id])))

(defn cluster-setslot-importing!
  "Execute CLUSTER SETSLOT <slot> IMPORTING <node-id>."
  [conn slot source-node-id]
  (wcar conn (car/redis-call ["CLUSTER" "SETSLOT" (str slot) "IMPORTING" source-node-id])))

(defn cluster-getkeysinslot
  "Execute CLUSTER GETKEYSINSLOT to get keys in a slot."
  [conn slot count]
  (wcar conn (car/redis-call ["CLUSTER" "GETKEYSINSLOT" (str slot) (str count)])))

(defn cluster-countkeysinslot
  "Execute CLUSTER COUNTKEYSINSLOT to count keys in a slot."
  [conn slot]
  (wcar conn (car/redis-call ["CLUSTER" "COUNTKEYSINSLOT" (str slot)])))

(defn cluster-keyslot
  "Execute CLUSTER KEYSLOT to get the slot for a key."
  [conn key]
  (wcar conn (car/redis-call ["CLUSTER" "KEYSLOT" key])))

(defn cluster-myid
  "Execute CLUSTER MYID to get the node's ID."
  [conn]
  (wcar conn (car/redis-call ["CLUSTER" "MYID"])))

(defn migrate!
  "Execute MIGRATE to move a key to another node."
  [conn host port key dest-db timeout-ms]
  (wcar conn (car/redis-call ["MIGRATE" host (str port) key (str dest-db) (str timeout-ms)])))

;; ===========================================================================
;; Cluster State Helpers
;; ===========================================================================

(defn get-cluster-state
  "Get the cluster_state value from CLUSTER INFO."
  [conn]
  (get (cluster-info conn) "cluster_state"))

(defn cluster-ok?
  "Check if cluster is in 'ok' state."
  [conn]
  (= "ok" (get-cluster-state conn)))

(defn get-current-leader
  "Get the current leader/master node from the cluster.
   Returns the node info map for the leader, or nil if no leader."
  [conn]
  (let [nodes (cluster-nodes conn)]
    (first (filter #(contains? (:flags %) "master") nodes))))

(defn get-node-id
  "Get the node ID for a node by its address prefix."
  [conn addr-prefix]
  (let [nodes (cluster-nodes conn)]
    (:id (first (filter #(str/starts-with? (:addr %) addr-prefix) nodes)))))

(defn is-node-master?
  "Check if a specific node is a master based on its flags."
  [conn node-ip]
  (let [nodes (cluster-nodes conn)]
    (some #(and (str/starts-with? (:addr %) node-ip)
                (contains? (:flags %) "master"))
          nodes)))

;; ===========================================================================
;; Wait Functions
;; ===========================================================================

(defn wait-for-cluster-ready
  "Wait until cluster_state is 'ok'.
   Returns true on success, throws on timeout."
  ([conn]
   (wait-for-cluster-ready conn cluster-ok-timeout-ms))
  ([conn timeout-ms]
   (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
     (loop []
       (cond
         (cluster-ok? conn)
         (do
           (info "Cluster state is OK")
           true)

         (> (System/currentTimeMillis) deadline)
         (throw+ {:type :timeout
                  :message "Timed out waiting for cluster_state:ok"
                  :cluster-info (cluster-info conn)})

         :else
         (do
           (Thread/sleep 500)
           (recur)))))))

(defn wait-for-leader
  "Wait until a leader is elected.
   Returns the leader node info, or throws on timeout."
  ([conn]
   (wait-for-leader conn leader-election-timeout-ms))
  ([conn timeout-ms]
   (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
     (loop []
       (let [leader (get-current-leader conn)]
         (cond
           leader
           (do
             (info "Leader elected:" (:id leader))
             leader)

           (> (System/currentTimeMillis) deadline)
           (throw+ {:type :timeout
                    :message "Timed out waiting for leader election"
                    :cluster-nodes (cluster-nodes conn)})

           :else
           (do
             (Thread/sleep 500)
             (recur))))))))

(defn wait-for-node-ready
  "Wait for a specific node to be ready to accept connections."
  [node docker-host? timeout-ms]
  (let [conn (conn-for-raft-node node docker-host?)
        deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (cond
        (client/ping conn)
        true

        (> (System/currentTimeMillis) deadline)
        (throw+ {:type :timeout
                 :message (str "Timed out waiting for node " node)})

        :else
        (do
          (Thread/sleep 500)
          (recur))))))

;; ===========================================================================
;; Slot Assignment
;; ===========================================================================

(defn assign-slots-evenly
  "Distribute 16384 slots evenly across the given nodes.
   Returns a map of {node -> [slot-start slot-end]}."
  [nodes]
  (let [node-count (count nodes)
        slots-per-node (quot total-slots node-count)
        extra-slots (mod total-slots node-count)]
    (loop [remaining-nodes nodes
           current-slot 0
           result {}]
      (if (empty? remaining-nodes)
        result
        (let [node (first remaining-nodes)
              extra (if (< (count result) extra-slots) 1 0)
              node-slots (+ slots-per-node extra)
              start current-slot
              end (+ current-slot node-slots -1)]
          (recur (rest remaining-nodes)
                 (+ current-slot node-slots)
                 (assoc result node [start end])))))))

(defn assign-slots-to-node!
  "Assign a range of slots to a node."
  [conn start-slot end-slot]
  (info "Assigning slots" start-slot "-" end-slot)
  (let [slots (range start-slot (inc end-slot))]
    ;; Add slots in batches to avoid command too long
    (doseq [batch (partition-all 1000 slots)]
      (wcar conn (car/redis-call (into ["CLUSTER" "ADDSLOTS"] (map str batch)))))))

(defn setup-cluster-slots!
  "Set up slot distribution for a new cluster.
   Connects to each node and assigns its slots."
  [nodes docker-host?]
  (let [slot-assignments (assign-slots-evenly nodes)]
    (doseq [[node [start end]] slot-assignments]
      (info "Assigning slots" start "-" end "to node" node)
      (let [conn (conn-for-raft-node node docker-host?)]
        (assign-slots-to-node! conn start end)))))

;; ===========================================================================
;; Cluster Formation
;; ===========================================================================

(defn form-cluster!
  "Form a new cluster from the given nodes.
   1. Have each node meet every other node
   2. Assign slots to each node
   3. Wait for cluster to be ready."
  [nodes docker-host?]
  (info "Forming cluster with nodes:" nodes)

  ;; Step 1: Have all nodes meet each other
  (let [first-node (first nodes)
        first-conn (conn-for-raft-node first-node docker-host?)]
    (doseq [node (rest nodes)]
      (let [ip (get raft-cluster-node-ips node)]
        (info "Node" first-node "meeting node" node "at" ip)
        (cluster-meet! first-conn ip 6379))))

  ;; Wait for gossip to propagate
  (Thread/sleep 2000)

  ;; Step 2: Assign slots
  (setup-cluster-slots! nodes docker-host?)

  ;; Step 3: Wait for cluster ready
  (let [conn (conn-for-raft-node (first nodes) docker-host?)]
    (wait-for-cluster-ready conn)))

;; ===========================================================================
;; Network Partition Support
;; ===========================================================================

(defn partition-raft-node!
  "Partition a Raft cluster node from other nodes using iptables."
  [node targets]
  (let [container (raft-container-name node)]
    (doseq [target targets]
      (let [target-ip (get raft-cluster-node-ips target)]
        (when target-ip
          (info "Partitioning" node "from" target "(" target-ip ")")
          (docker-exec container "iptables" "-A" "INPUT" "-s" target-ip "-j" "DROP")
          (docker-exec container "iptables" "-A" "OUTPUT" "-d" target-ip "-j" "DROP"))))))

(defn heal-raft-partition!
  "Remove all iptables rules from a Raft cluster node."
  [node]
  (let [container (raft-container-name node)]
    (info "Healing partition on" node)
    (docker-exec-ignore-error container "iptables" "-F")))

(defn isolate-raft-leader!
  "Isolate the current leader from all followers."
  [nodes docker-host?]
  (let [conn (conn-for-raft-node (first nodes) docker-host?)
        leader (get-current-leader conn)]
    (when leader
      (let [leader-ip (first (str/split (:addr leader) #":"))]
        ;; Find which node this leader is
        (let [leader-node (first (filter #(= (get raft-cluster-node-ips %) leader-ip)
                                         (keys raft-cluster-node-ips)))]
          (when leader-node
            (let [other-nodes (remove #{leader-node} nodes)]
              (info "Isolating leader" leader-node "from" other-nodes)
              (partition-raft-node! leader-node other-nodes)
              (doseq [node other-nodes]
                (partition-raft-node! node [leader-node])))))))))

(defn heal-all-raft-partitions!
  "Heal partitions on all Raft cluster nodes."
  [nodes]
  (doseq [node nodes]
    (heal-raft-partition! node)))

;; ===========================================================================
;; Cluster Database Implementation
;; ===========================================================================

(defn cluster-db
  "Database implementation for Raft cluster testing.

   Options:
   - :initial-nodes - nodes to include in initial cluster (default [\"n1\" \"n2\" \"n3\"])
   - :docker-host? - whether running from Docker host (default true)"
  [opts]
  (let [initial-nodes (get opts :initial-nodes ["n1" "n2" "n3"])
        docker-host? (get opts :docker-host? true)]
    (reify db/DB
      (setup! [_ test node]
        (info "Setting up Raft cluster node" node)
        (let [container (raft-container-name node)]
          (try+
            (docker-start container)
            (catch Object e
              (warn "Container start failed (may already be running):" (:message e))))
          ;; Wait for node to be ready
          (wait-for-node-ready node docker-host? 30000)
          ;; If this is the last initial node, form the cluster
          (when (and (contains? (set initial-nodes) node)
                     (= node (last initial-nodes)))
            (Thread/sleep 2000)  ; Let all nodes stabilize
            (form-cluster! initial-nodes docker-host?))))

      (teardown! [_ test node]
        (info "Tearing down Raft cluster node" node)
        ;; Clear any iptables rules
        (heal-raft-partition! node))

      db/Kill
      (kill! [_ test node]
        (info "Killing FrogDB on Raft node" node)
        (docker-hard-kill (raft-container-name node)))

      (start! [_ test node]
        (info "Starting FrogDB on Raft node" node)
        (let [container (raft-container-name node)]
          (docker-start container)
          (wait-for-node-ready node docker-host? 30000)))

      db/Pause
      (pause! [_ test node]
        (info "Pausing FrogDB on Raft node" node)
        (docker-pause-process (raft-container-name node)))

      (resume! [_ test node]
        (info "Resuming FrogDB on Raft node" node)
        (docker-resume-process (raft-container-name node))))))

;; ===========================================================================
;; Utility Functions for Tests
;; ===========================================================================

(defn all-node-conns
  "Create connections to all cluster nodes.
   Returns a map of {node -> conn-spec}."
  [nodes docker-host?]
  (into {} (for [node nodes]
             [node (conn-for-raft-node node docker-host?)])))

(defn find-leader-node
  "Find which node is currently the leader.
   Returns the node name (e.g., \"n1\"), or nil if no leader."
  [nodes docker-host?]
  (let [conn (conn-for-raft-node (first nodes) docker-host?)
        leader (get-current-leader conn)]
    (when leader
      (let [leader-ip (first (str/split (:addr leader) #":"))]
        (first (filter #(= (get raft-cluster-node-ips %) leader-ip)
                       (keys raft-cluster-node-ips)))))))

(defn count-masters
  "Count the number of master nodes in the cluster."
  [nodes docker-host?]
  (let [conn (conn-for-raft-node (first nodes) docker-host?)
        all-nodes (cluster-nodes conn)]
    (count (filter #(contains? (:flags %) "master") all-nodes))))
