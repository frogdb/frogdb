(ns jepsen.frogdb.db
  "Database lifecycle management for FrogDB Jepsen tests.

   Provides implementations for different test modes:
   - local-db: FrogDB running locally (for development)
   - docker-db: Single-node Docker container
   - replication-db: 3-node replication cluster

   Implements the Jepsen db/DB protocol for setup, teardown, kill, etc."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn error debug]]
            [jepsen.db :as db]
            [jepsen.control :as c]
            [jepsen.frogdb.client :as client]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import [java.lang ProcessBuilder]
           [java.io BufferedReader InputStreamReader]))

;; ===========================================================================
;; Docker Utilities
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

(defn docker-exec-bg
  "Execute a command in a Docker container in the background.
   Does not wait for completion."
  [container & args]
  (let [cmd (into ["docker" "exec" "-d" container] args)
        pb (ProcessBuilder. ^java.util.List cmd)]
    (.start pb)))

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

(defn docker-stop
  "Stop a Docker container."
  [container]
  (let [cmd ["docker" "stop" "-t" "1" container]
        pb (ProcessBuilder. ^java.util.List cmd)
        _ (.redirectErrorStream pb true)
        proc (.start pb)
        exit-code (.waitFor proc)]
    (when (not= 0 exit-code)
      (warn "docker stop failed for" container "exit code:" exit-code))))

(defn docker-kill-process
  "Kill the FrogDB process inside a container with the given signal.
   Uses pkill to find and kill the frogdb process."
  [container signal]
  (try+
    (docker-exec container "pkill" (str "-" signal) "frogdb")
    (catch Object e
      ;; pkill returns 1 if no processes matched - that's ok
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

(defn container-name
  "Get the Docker container name for a node."
  [node]
  (str "frogdb-" node))

;; ===========================================================================
;; Local DB (No Docker)
;; ===========================================================================

(defn local-db
  "Database implementation for local testing.
   Assumes FrogDB is already running on localhost:6379.
   No setup/teardown operations - just connects to existing server."
  []
  (reify db/DB
    (setup! [_ test node]
      (info "Local DB setup for" node "(no-op, using existing FrogDB)"))

    (teardown! [_ test node]
      (info "Local DB teardown for" node "(no-op)"))

    db/Kill
    (kill! [_ test node]
      (warn "Local DB: kill! is a no-op - cannot kill local process"))

    (start! [_ test node]
      (warn "Local DB: start! is a no-op - assuming already running"))

    db/Pause
    (pause! [_ test node]
      (warn "Local DB: pause! is a no-op"))

    (resume! [_ test node]
      (warn "Local DB: resume! is a no-op"))))

;; ===========================================================================
;; Docker Single-Node DB
;; ===========================================================================

(defn docker-db
  "Database implementation for single-node Docker testing.
   Uses docker-compose to manage the FrogDB container."
  []
  (reify db/DB
    (setup! [_ test node]
      (info "Setting up FrogDB on" node)
      (let [container (container-name node)]
        (info "Starting container for" node)
        (try+
          (docker-start container)
          (catch Object e
            (warn "Container start failed (may already be running):" (:message e))))
        ;; Wait for FrogDB to be ready
        (Thread/sleep 1000)
        (client/wait-for-ready node :timeout-ms 30000)))

    (teardown! [_ test node]
      (info "Tearing down FrogDB on" node)
      ;; Don't stop the container - leave it running for inspection
      nil)

    db/Kill
    (kill! [_ test node]
      (info "Killing FrogDB on" node)
      (docker-hard-kill (container-name node)))

    (start! [_ test node]
      (info "Starting FrogDB on" node)
      ;; Restart the container
      (let [container (container-name node)]
        (docker-start container)
        (Thread/sleep 1000)
        (client/wait-for-ready node :timeout-ms 30000)))

    db/Pause
    (pause! [_ test node]
      (info "Pausing FrogDB on" node)
      (docker-pause-process (container-name node)))

    (resume! [_ test node]
      (info "Resuming FrogDB on" node)
      (docker-resume-process (container-name node)))))

;; ===========================================================================
;; Docker Replication Cluster DB
;; ===========================================================================

(defn replication-db
  "Database implementation for 3-node replication cluster testing.

   Manages a primary + 2 replica topology:
   - n1: Primary (accepts writes)
   - n2: Replica
   - n3: Replica

   Supports network partition testing via iptables when running with NET_ADMIN."
  []
  (reify db/DB
    (setup! [_ test node]
      (info "Setting up FrogDB replication node" node)
      (let [container (container-name node)]
        (try+
          (docker-start container)
          (catch Object e
            (warn "Container start failed (may already be running):" (:message e))))
        ;; Wait for FrogDB to be ready
        (Thread/sleep 1000)
        (client/wait-for-ready node :timeout-ms 30000)))

    (teardown! [_ test node]
      (info "Tearing down FrogDB replication node" node)
      ;; Clear any iptables rules we may have added
      (try+
        (docker-exec (container-name node) "iptables" "-F")
        (catch Object _
          nil))
      nil)

    db/Kill
    (kill! [_ test node]
      (info "Killing FrogDB on" node)
      (docker-hard-kill (container-name node)))

    (start! [_ test node]
      (info "Starting FrogDB on" node)
      (let [container (container-name node)]
        (docker-start container)
        (Thread/sleep 1000)
        (client/wait-for-ready node :timeout-ms 30000)))

    db/Pause
    (pause! [_ test node]
      (info "Pausing FrogDB on" node)
      (docker-pause-process (container-name node)))

    (resume! [_ test node]
      (info "Resuming FrogDB on" node)
      (docker-resume-process (container-name node)))))

;; ===========================================================================
;; Network Partition Support (for replication-db)
;; ===========================================================================

(def node-ips
  "Map of node names to their IP addresses in the Docker network."
  {"n1" "172.20.0.2"
   "n2" "172.20.0.3"
   "n3" "172.20.0.4"})

(def raft-cluster-node-ips
  "Map of node names to their IP addresses in the Raft cluster Docker network."
  {"n1" "172.21.0.2"
   "n2" "172.21.0.3"
   "n3" "172.21.0.4"
   "n4" "172.21.0.5"
   "n5" "172.21.0.6"})

(defn partition-node!
  "Partition a node from specific other nodes using iptables.
   Blocks both incoming and outgoing traffic to the target IPs."
  [node targets]
  (let [container (container-name node)]
    (doseq [target targets]
      (let [target-ip (get node-ips target)]
        (when target-ip
          (info "Partitioning" node "from" target "(" target-ip ")")
          (docker-exec container "iptables" "-A" "INPUT" "-s" target-ip "-j" "DROP")
          (docker-exec container "iptables" "-A" "OUTPUT" "-d" target-ip "-j" "DROP"))))))

(defn heal-partition!
  "Remove all iptables rules from a node, healing any partitions."
  [node]
  (let [container (container-name node)]
    (info "Healing partition on" node)
    (docker-exec container "iptables" "-F")))

(defn isolate-primary!
  "Isolate the primary (n1) from all replicas."
  []
  (partition-node! "n1" ["n2" "n3"])
  (partition-node! "n2" ["n1"])
  (partition-node! "n3" ["n1"]))

(defn partition-halves!
  "Split cluster into two halves: [n1, n2] vs [n3]."
  []
  (partition-node! "n1" ["n3"])
  (partition-node! "n2" ["n3"])
  (partition-node! "n3" ["n1" "n2"]))

(defn heal-all!
  "Heal partitions on all nodes."
  []
  (doseq [node ["n1" "n2" "n3"]]
    (heal-partition! node)))
