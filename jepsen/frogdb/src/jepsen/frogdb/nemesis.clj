(ns jepsen.frogdb.nemesis
  "Nemesis implementations for FrogDB testing.

   Provides fault injection including:
   - Process crashes (SIGKILL)
   - Process pauses (SIGSTOP/SIGCONT)
   - Graceful restarts (SIGTERM)
   - Network partitions (via iptables)
   - Disk failures (read-only filesystem, disk full)
   - Clock skew (via libfaketime)
   - Slow network (via tc/netem)
   - Memory pressure (via stress-ng)

   These nemeses help verify crash recovery, durability, and
   distributed systems properties."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.db :as db]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as nc]
            [jepsen.util :as util]
            [jepsen.frogdb.db :as frogdb-db]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import [java.lang ProcessBuilder]
           [java.io BufferedReader InputStreamReader]))

;; ===========================================================================
;; Process Kill Nemesis
;; ===========================================================================

(defn process-killer
  "Creates a nemesis that kills and restarts FrogDB processes.

   Supports operations:
   - {:f :kill} - kill FrogDB with SIGKILL
   - {:f :start} - restart FrogDB"
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      this)

    (invoke! [this test op]
      (case (:f op)
        :kill
        (let [nodes (or (:value op) (:nodes test))]
          (info "Killing FrogDB on" nodes)
          (doseq [node (util/coll nodes)]
            (db/kill! (:db test) test node))
          (assoc op :value nodes))

        :start
        (let [nodes (or (:value op) (:nodes test))]
          (info "Starting FrogDB on" nodes)
          (doseq [node (util/coll nodes)]
            (db/start! (:db test) test node))
          ;; Wait for FrogDB to be ready
          (Thread/sleep 2000)
          (assoc op :value nodes))))

    (teardown! [this test]
      nil)))

;; ===========================================================================
;; Process Pause Nemesis
;; ===========================================================================

(defn process-pauser
  "Creates a nemesis that pauses and resumes FrogDB processes.

   Supports operations:
   - {:f :pause} - pause FrogDB with SIGSTOP
   - {:f :resume} - resume FrogDB with SIGCONT"
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      this)

    (invoke! [this test op]
      (case (:f op)
        :pause
        (let [nodes (or (:value op) (:nodes test))]
          (info "Pausing FrogDB on" nodes)
          (doseq [node (util/coll nodes)]
            (db/pause! (:db test) test node))
          (assoc op :value nodes))

        :resume
        (let [nodes (or (:value op) (:nodes test))]
          (info "Resuming FrogDB on" nodes)
          (doseq [node (util/coll nodes)]
            (db/resume! (:db test) test node))
          (assoc op :value nodes))))

    (teardown! [this test]
      nil)))

;; ===========================================================================
;; Combined Nemesis
;; ===========================================================================

(defn combined-nemesis
  "Creates a nemesis that combines multiple fault types."
  []
  (nemesis/compose
    {{:kill :kill
      :start :start} (process-killer)
     {:pause :pause
      :resume :resume} (process-pauser)}))

;; ===========================================================================
;; Generators
;; ===========================================================================

(defn kill-generator
  "Generator for kill/restart cycles.

   Options:
   - :interval - time between fault events (default 10s)
   - :recover-time - time to wait after restart (default 5s)"
  [opts]
  (let [interval (get opts :interval 10)
        recover-time (get opts :recover-time 5)]
    (gen/cycle
      [(gen/sleep interval)
       {:type :info :f :kill}
       (gen/sleep recover-time)
       {:type :info :f :start}
       (gen/sleep recover-time)])))

(defn pause-generator
  "Generator for pause/resume cycles.

   Options:
   - :interval - time between fault events (default 10s)
   - :pause-duration - how long to pause (default 5s)"
  [opts]
  (let [interval (get opts :interval 10)
        pause-duration (get opts :pause-duration 5)]
    (gen/cycle
      [(gen/sleep interval)
       {:type :info :f :pause}
       (gen/sleep pause-duration)
       {:type :info :f :resume}
       (gen/sleep 2)])))

(defn mixed-generator
  "Generator that mixes kill and pause faults.

   Options:
   - :interval - base interval between faults
   - :kill-weight - relative weight of kill operations
   - :pause-weight - relative weight of pause operations"
  [opts]
  (let [interval (get opts :interval 15)
        kill-weight (get opts :kill-weight 1)
        pause-weight (get opts :pause-weight 1)]
    (gen/cycle
      [(gen/sleep interval)
       (gen/once
         (rand-nth (concat
                     (repeat kill-weight [{:type :info :f :kill}
                                         (gen/sleep 5)
                                         {:type :info :f :start}])
                     (repeat pause-weight [{:type :info :f :pause}
                                          (gen/sleep 3)
                                          {:type :info :f :resume}]))))])))

(defn rapid-kill-generator
  "Rapid kill/restart cycles for stress testing durability.

   This generator kills and restarts FrogDB at a much faster rate
   than the standard kill-generator, designed to stress test crash
   recovery and persistence mechanisms.

   Options:
   - :kill-interval - time between kills (default 3s)
   - :restart-delay - time to wait before restart (default 1s)"
  [opts]
  (let [kill-interval (get opts :kill-interval 3)
        restart-delay (get opts :restart-delay 1)]
    (gen/cycle
      [(gen/sleep kill-interval)
       {:type :info :f :kill}
       (gen/sleep restart-delay)
       {:type :info :f :start}
       (gen/sleep restart-delay)])))

;; ===========================================================================
;; Nemesis Packages
;; ===========================================================================

(defn none
  "No nemesis - for baseline testing."
  []
  {:nemesis nemesis/noop
   :generator nil
   :final-generator nil})

(defn kill
  "Kill/restart nemesis package.

   Options:
   - :interval - time between kill cycles"
  [opts]
  {:nemesis (process-killer)
   :generator (kill-generator opts)
   :final-generator (gen/once {:type :info :f :start})})

(defn pause
  "Pause/resume nemesis package.

   Options:
   - :interval - time between pause cycles"
  [opts]
  {:nemesis (process-pauser)
   :generator (pause-generator opts)
   :final-generator (gen/once {:type :info :f :resume})})

(defn all
  "Combined nemesis with both kill and pause.

   Options:
   - :interval - time between faults"
  [opts]
  {:nemesis (combined-nemesis)
   :generator (mixed-generator opts)
   :final-generator (gen/phases
                      (gen/once {:type :info :f :resume})
                      (gen/once {:type :info :f :start}))})

(defn rapid-kill
  "Rapid kill/restart nemesis package for stress testing durability.

   This is more aggressive than the standard kill nemesis, designed
   to catch durability and crash recovery bugs.

   Options:
   - :kill-interval - time between kills (default 3s)
   - :restart-delay - time before restart (default 1s)"
  [opts]
  {:nemesis (process-killer)
   :generator (rapid-kill-generator opts)
   :final-generator (gen/once {:type :info :f :start})})

;; ===========================================================================
;; Network Partition Nemesis
;; ===========================================================================

(defn partition-nemesis
  "Creates a nemesis that partitions nodes using iptables.
   Detects cluster mode from test context for correct container naming.

   Supports operations:
   - {:f :partition :value :primary-isolated} - isolate primary (n1) from replicas
   - {:f :partition :value :halves} - split into [n1,n2] vs [n3]
   - {:f :partition :value [:isolate node]} - isolate specific node
   - {:f :heal} - remove all partitions"
  []
  (let [cluster-mode? (atom false)]
    (reify nemesis/Nemesis
      (setup! [this test]
        ;; Detect cluster mode from test context
        (reset! cluster-mode? (boolean (some #{:cluster-formation :leader-election
                                               :slot-migration :cross-slot :key-routing}
                                             [(keyword (:workload test))])))
        this)

      (invoke! [this test op]
        (let [cm? @cluster-mode?
              all-nodes (if cm?
                          (vec (keys frogdb-db/raft-cluster-node-ips))
                          ["n1" "n2" "n3"])]
          (case (:f op)
            :partition
            (case (:value op)
              :primary-isolated
              (do
                (info "Partitioning: isolating primary from replicas")
                (frogdb-db/isolate-primary! cm?)
                (assoc op :value :primary-isolated))

              :halves
              (do
                (info "Partitioning: splitting cluster into halves [n1,n2] vs [n3]")
                (frogdb-db/partition-halves! cm?)
                (assoc op :value :halves))

              ;; Handle [:isolate node] pattern
              (if (and (vector? (:value op))
                       (= :isolate (first (:value op))))
                (let [node (second (:value op))]
                  (info "Partitioning: isolating node" node)
                  (frogdb-db/partition-node! node (remove #{node} all-nodes) cm?)
                  (assoc op :value [:isolated node]))
                (do
                  (warn "Unknown partition type:" (:value op))
                  (assoc op :type :fail :error :unknown-partition-type))))

            :heal
            (do
              (info "Healing all network partitions")
              (frogdb-db/heal-all! cm?)
              (assoc op :value :healed)))))

      (teardown! [this test]
        ;; Clean up any remaining partitions
        (frogdb-db/heal-all! @cluster-mode?)))))

(defn partition-generator
  "Generator for network partition cycles.

   Options:
   - :interval - time between partition events (default 10s)
   - :partition-duration - how long to maintain partition (default 10s)
   - :partition-type - :primary-isolated or :halves (default :primary-isolated)"
  [opts]
  (let [interval (get opts :interval 10)
        duration (get opts :partition-duration 10)
        partition-type (get opts :partition-type :primary-isolated)]
    (gen/cycle
      [(gen/sleep interval)
       {:type :info :f :partition :value partition-type}
       (gen/sleep duration)
       {:type :info :f :heal}
       (gen/sleep 5)])))

;; ===========================================================================
;; Docker Utilities for Nemeses
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

(defn container-name
  "Get the container name for a node (standard or Raft cluster)."
  [node cluster-mode?]
  (if cluster-mode?
    (str "frogdb-raft-" node)
    (str "frogdb-" node)))

;; ===========================================================================
;; Clock Skew Nemesis (using libfaketime via date manipulation)
;; ===========================================================================

(defn write-faketime-offset!
  "Write a faketime offset file for a container.
   The offset is in seconds (positive = future, negative = past)."
  [container offset-seconds]
  (let [offset-str (if (>= offset-seconds 0)
                     (str "+" offset-seconds)
                     (str offset-seconds))]
    ;; Write offset to /tmp/faketime in container
    (docker-exec container "sh" "-c" (str "echo '" offset-str "' > /tmp/faketime"))))

(defn clear-faketime-offset!
  "Clear the faketime offset file in a container."
  [container]
  (docker-exec-ignore-error container "rm" "-f" "/tmp/faketime"))

(defn skew-clock-via-date!
  "Skew the system clock using the date command (requires privileged container).
   This directly manipulates the system time."
  [container delta-seconds]
  (let [direction (if (>= delta-seconds 0) "+" "-")
        abs-delta (Math/abs (long delta-seconds))]
    (docker-exec container "date" "-s" (str direction (str abs-delta) " seconds"))))

(defn reset-clock-via-ntp!
  "Reset clock via NTP (if available) or by setting to host time."
  [container]
  ;; Try ntpdate first, fall back to syncing with host
  (try+
    (docker-exec container "ntpdate" "-s" "pool.ntp.org")
    (catch Object _
      ;; Fallback: get current time and set it
      (let [now (System/currentTimeMillis)
            epoch-secs (quot now 1000)]
        (docker-exec-ignore-error container "date" "-s" (str "@" epoch-secs))))))

(defn clock-skew-nemesis
  "Creates a nemesis that skews clocks on nodes.

   Uses direct date manipulation in containers (requires CAP_SYS_TIME).
   For containers without this capability, writes to /tmp/faketime
   which can be read by applications using libfaketime.

   Supports operations:
   - {:f :clock-skew :value {:node n :delta-ms ms}} - skew clock on node
   - {:f :clock-reset} - reset all clocks
   - {:f :clock-strobe :value {:node n :delta-ms ms}} - brief clock jump"
  ([]
   (clock-skew-nemesis false))
  ([cluster-mode?]
   (let [skewed-nodes (atom #{})]
     (reify nemesis/Nemesis
       (setup! [this test]
         this)

       (invoke! [this test op]
         (case (:f op)
           :clock-skew
           (let [{:keys [node delta-ms]} (:value op)
                 nodes (if node [node] (:nodes test))
                 delta-seconds (quot delta-ms 1000)]
             (doseq [n nodes]
               (let [container (container-name n cluster-mode?)]
                 (info "Skewing clock on" n "by" delta-ms "ms")
                 (try+
                   (skew-clock-via-date! container delta-seconds)
                   (swap! skewed-nodes conj n)
                   (catch Object e
                     ;; Fall back to faketime file
                     (write-faketime-offset! container delta-seconds)
                     (swap! skewed-nodes conj n)))))
             (assoc op :value {:nodes nodes :delta-ms delta-ms}))

           :clock-reset
           (let [nodes (or (:value op) @skewed-nodes)]
             (info "Resetting clocks on" nodes)
             (doseq [n nodes]
               (let [container (container-name n cluster-mode?)]
                 (try+
                   (reset-clock-via-ntp! container)
                   (catch Object _
                     (clear-faketime-offset! container)))))
             (reset! skewed-nodes #{})
             (assoc op :value :reset))

           :clock-strobe
           (let [{:keys [node delta-ms]} (:value op)
                 nodes (if node [node] (:nodes test))
                 delta-seconds (quot delta-ms 1000)]
             ;; Brief clock jump - skew then immediately reset
             (doseq [n nodes]
               (let [container (container-name n cluster-mode?)]
                 (try+
                   (skew-clock-via-date! container delta-seconds)
                   (Thread/sleep 100)
                   (skew-clock-via-date! container (- delta-seconds))
                   (catch Object _ nil))))
             (assoc op :value {:nodes nodes :strobed delta-ms}))))

       (teardown! [this test]
         ;; Reset all clocks on teardown
         (doseq [n @skewed-nodes]
           (let [container (container-name n cluster-mode?)]
             (try+
               (reset-clock-via-ntp! container)
               (catch Object _
                 (clear-faketime-offset! container))))))))))

(defn clock-skew-generator
  "Generator for clock skew cycles.

   Options:
   - :interval - time between skew events (default 15s)
   - :skew-duration - how long to maintain skew (default 10s)
   - :max-skew-ms - maximum skew in milliseconds (default 5000)"
  [opts]
  (let [interval (get opts :interval 15)
        duration (get opts :skew-duration 10)
        max-skew (get opts :max-skew-ms 5000)]
    (gen/cycle
      [(gen/sleep interval)
       {:type :info :f :clock-skew :value {:delta-ms (- (rand-int (* 2 max-skew)) max-skew)}}
       (gen/sleep duration)
       {:type :info :f :clock-reset}
       (gen/sleep 3)])))

;; ===========================================================================
;; Disk Failure Nemesis (filesystem manipulation)
;; ===========================================================================

(defn make-disk-readonly!
  "Make the /data directory read-only via remount."
  [container]
  (docker-exec container "mount" "-o" "remount,ro" "/data"))

(defn make-disk-readwrite!
  "Restore the /data directory to read-write."
  [container]
  (docker-exec container "mount" "-o" "remount,rw" "/data"))

(defn fill-disk!
  "Fill the disk by creating a large file."
  [container size-mb]
  (docker-exec container "dd" "if=/dev/zero" "of=/data/jepsen-fill"
               (str "bs=1M") (str "count=" size-mb)))

(defn clear-disk-fill!
  "Remove the disk fill file."
  [container]
  (docker-exec-ignore-error container "rm" "-f" "/data/jepsen-fill"))

(defn corrupt-file!
  "Corrupt a specific file by writing random bytes to it."
  [container filepath]
  (docker-exec container "dd" "if=/dev/urandom" (str "of=" filepath)
               "bs=1024" "count=1" "conv=notrunc"))

(defn disk-failure-nemesis
  "Creates a nemesis that simulates disk failures.

   Uses mount manipulation and file operations to simulate:
   - Read-only filesystem (simulates disk failure)
   - Disk full conditions
   - File corruption

   Requires containers to run with appropriate capabilities.

   Supports operations:
   - {:f :disk-readonly :value node} - make disk read-only
   - {:f :disk-recover :value node} - restore disk to read-write
   - {:f :disk-full :value {:node n :size-mb s}} - fill disk
   - {:f :disk-clear :value node} - clear disk fill
   - {:f :disk-corrupt :value {:node n :file f}} - corrupt a file"
  ([]
   (disk-failure-nemesis false))
  ([cluster-mode?]
   (let [affected-nodes (atom #{})]
     (reify nemesis/Nemesis
       (setup! [this test]
         this)

       (invoke! [this test op]
         (case (:f op)
           :disk-readonly
           (let [nodes (if (coll? (:value op)) (:value op) [(:value op)])]
             (doseq [node nodes]
               (let [container (container-name node cluster-mode?)]
                 (info "Making disk read-only on" node)
                 (try+
                   (make-disk-readonly! container)
                   (swap! affected-nodes conj node)
                   (catch Object e
                     (warn "Failed to make disk readonly on" node ":" e)))))
             (assoc op :value {:nodes nodes :status :readonly}))

           :disk-recover
           (let [nodes (if (coll? (:value op)) (:value op) [(:value op)])]
             (doseq [node nodes]
               (let [container (container-name node cluster-mode?)]
                 (info "Recovering disk on" node)
                 (try+
                   (make-disk-readwrite! container)
                   (swap! affected-nodes disj node)
                   (catch Object e
                     (warn "Failed to recover disk on" node ":" e)))))
             (assoc op :value {:nodes nodes :status :recovered}))

           :disk-full
           (let [{:keys [node size-mb]} (:value op)
                 size (or size-mb 100)
                 container (container-name node cluster-mode?)]
             (info "Filling disk on" node "with" size "MB")
             (try+
               (fill-disk! container size)
               (swap! affected-nodes conj node)
               (assoc op :value {:node node :filled-mb size})
               (catch Object e
                 (warn "Failed to fill disk on" node ":" e)
                 (assoc op :type :fail :error :fill-failed))))

           :disk-clear
           (let [node (:value op)
                 container (container-name node cluster-mode?)]
             (info "Clearing disk fill on" node)
             (clear-disk-fill! container)
             (swap! affected-nodes disj node)
             (assoc op :value {:node node :status :cleared}))

           :disk-corrupt
           (let [{:keys [node file]} (:value op)
                 container (container-name node cluster-mode?)]
             (info "Corrupting file" file "on" node)
             (try+
               (corrupt-file! container file)
               (assoc op :value {:node node :file file :status :corrupted})
               (catch Object e
                 (warn "Failed to corrupt file on" node ":" e)
                 (assoc op :type :fail :error :corrupt-failed))))))

       (teardown! [this test]
         ;; Recover all affected disks
         (doseq [node @affected-nodes]
           (let [container (container-name node cluster-mode?)]
             (try+
               (make-disk-readwrite! container)
               (clear-disk-fill! container)
               (catch Object _ nil)))))))))

(defn disk-failure-generator
  "Generator for disk failure cycles.

   Options:
   - :interval - time between failures (default 20s)
   - :failure-duration - how long to maintain failure (default 10s)
   - :failure-type - :readonly or :full (default :readonly)"
  [opts]
  (let [interval (get opts :interval 20)
        duration (get opts :failure-duration 10)
        failure-type (get opts :failure-type :readonly)]
    (gen/cycle
      [(gen/sleep interval)
       (case failure-type
         :readonly {:type :info :f :disk-readonly :value "n1"}
         :full {:type :info :f :disk-full :value {:node "n1" :size-mb 100}})
       (gen/sleep duration)
       (case failure-type
         :readonly {:type :info :f :disk-recover :value "n1"}
         :full {:type :info :f :disk-clear :value "n1"})
       (gen/sleep 5)])))

;; ===========================================================================
;; Slow Network Nemesis (tc/netem)
;; ===========================================================================

(defn add-network-delay!
  "Add network delay using tc/netem."
  [container delay-ms jitter-ms]
  (docker-exec container "tc" "qdisc" "add" "dev" "eth0" "root" "netem"
               "delay" (str delay-ms "ms") (str jitter-ms "ms")))

(defn add-packet-loss!
  "Add packet loss using tc/netem."
  [container loss-percent]
  (docker-exec container "tc" "qdisc" "add" "dev" "eth0" "root" "netem"
               "loss" (str loss-percent "%")))

(defn add-network-corruption!
  "Add packet corruption using tc/netem."
  [container corrupt-percent]
  (docker-exec container "tc" "qdisc" "add" "dev" "eth0" "root" "netem"
               "corrupt" (str corrupt-percent "%")))

(defn add-network-reorder!
  "Add packet reordering using tc/netem."
  [container reorder-percent delay-ms]
  (docker-exec container "tc" "qdisc" "add" "dev" "eth0" "root" "netem"
               "delay" (str delay-ms "ms") "reorder" (str reorder-percent "%")))

(defn remove-network-qdisc!
  "Remove all tc qdisc rules, restoring normal network."
  [container]
  (docker-exec-ignore-error container "tc" "qdisc" "del" "dev" "eth0" "root"))

(defn slow-network-nemesis
  "Creates a nemesis that adds network latency and packet loss using tc/netem.

   Requires containers to have NET_ADMIN capability.

   Supports operations:
   - {:f :add-latency :value {:node n :delay-ms d :jitter-ms j}} - add delay
   - {:f :packet-loss :value {:node n :percent p}} - add packet loss
   - {:f :packet-corrupt :value {:node n :percent p}} - corrupt packets
   - {:f :packet-reorder :value {:node n :percent p :delay-ms d}} - reorder packets
   - {:f :network-heal :value node} - remove all impairments
   - {:f :network-heal-all} - heal all nodes"
  ([]
   (slow-network-nemesis false))
  ([cluster-mode?]
   (let [affected-nodes (atom #{})]
     (reify nemesis/Nemesis
       (setup! [this test]
         this)

       (invoke! [this test op]
         (case (:f op)
           :add-latency
           (let [{:keys [node delay-ms jitter-ms]} (:value op)
                 nodes (if node [node] (:nodes test))
                 delay (or delay-ms 100)
                 jitter (or jitter-ms 20)]
             (doseq [n nodes]
               (let [container (container-name n cluster-mode?)]
                 (info "Adding" delay "ms latency (±" jitter "ms) to" n)
                 (try+
                   (remove-network-qdisc! container)  ; Clear existing rules
                   (add-network-delay! container delay jitter)
                   (swap! affected-nodes conj n)
                   (catch Object e
                     (warn "Failed to add latency to" n ":" e)))))
             (assoc op :value {:nodes nodes :delay-ms delay :jitter-ms jitter}))

           :packet-loss
           (let [{:keys [node percent]} (:value op)
                 nodes (if node [node] (:nodes test))
                 loss (or percent 10)]
             (doseq [n nodes]
               (let [container (container-name n cluster-mode?)]
                 (info "Adding" loss "% packet loss to" n)
                 (try+
                   (remove-network-qdisc! container)
                   (add-packet-loss! container loss)
                   (swap! affected-nodes conj n)
                   (catch Object e
                     (warn "Failed to add packet loss to" n ":" e)))))
             (assoc op :value {:nodes nodes :percent loss}))

           :packet-corrupt
           (let [{:keys [node percent]} (:value op)
                 nodes (if node [node] (:nodes test))
                 corrupt (or percent 5)]
             (doseq [n nodes]
               (let [container (container-name n cluster-mode?)]
                 (info "Adding" corrupt "% packet corruption to" n)
                 (try+
                   (remove-network-qdisc! container)
                   (add-network-corruption! container corrupt)
                   (swap! affected-nodes conj n)
                   (catch Object e
                     (warn "Failed to add corruption to" n ":" e)))))
             (assoc op :value {:nodes nodes :percent corrupt}))

           :packet-reorder
           (let [{:keys [node percent delay-ms]} (:value op)
                 nodes (if node [node] (:nodes test))
                 reorder (or percent 25)
                 delay (or delay-ms 50)]
             (doseq [n nodes]
               (let [container (container-name n cluster-mode?)]
                 (info "Adding" reorder "% packet reordering to" n)
                 (try+
                   (remove-network-qdisc! container)
                   (add-network-reorder! container reorder delay)
                   (swap! affected-nodes conj n)
                   (catch Object e
                     (warn "Failed to add reordering to" n ":" e)))))
             (assoc op :value {:nodes nodes :percent reorder :delay-ms delay}))

           :network-heal
           (let [nodes (if (coll? (:value op)) (:value op) [(:value op)])]
             (doseq [n nodes]
               (let [container (container-name n cluster-mode?)]
                 (info "Healing network on" n)
                 (remove-network-qdisc! container)
                 (swap! affected-nodes disj n)))
             (assoc op :value {:nodes nodes :status :healed}))

           :network-heal-all
           (do
             (doseq [n @affected-nodes]
               (let [container (container-name n cluster-mode?)]
                 (info "Healing network on" n)
                 (remove-network-qdisc! container)))
             (reset! affected-nodes #{})
             (assoc op :value :all-healed))))

       (teardown! [this test]
         ;; Heal all affected nodes
         (doseq [n @affected-nodes]
           (let [container (container-name n cluster-mode?)]
             (remove-network-qdisc! container))))))))

(defn slow-network-generator
  "Generator for slow network cycles.

   Options:
   - :interval - time between network impairments (default 15s)
   - :impairment-duration - how long to maintain impairment (default 10s)
   - :delay-ms - latency to add (default 200ms)
   - :jitter-ms - latency jitter (default 50ms)"
  [opts]
  (let [interval (get opts :interval 15)
        duration (get opts :impairment-duration 10)
        delay-ms (get opts :delay-ms 200)
        jitter-ms (get opts :jitter-ms 50)]
    (gen/cycle
      [(gen/sleep interval)
       {:type :info :f :add-latency :value {:delay-ms delay-ms :jitter-ms jitter-ms}}
       (gen/sleep duration)
       {:type :info :f :network-heal-all}
       (gen/sleep 3)])))

;; ===========================================================================
;; Memory Pressure Nemesis (stress-ng)
;; ===========================================================================

(defn start-memory-stress!
  "Start memory stress using stress-ng."
  [container bytes-mb]
  ;; Run stress-ng in background
  (docker-exec container "sh" "-c"
               (str "nohup stress-ng --vm 1 --vm-bytes " bytes-mb "M --vm-keep "
                    "--timeout 0 > /dev/null 2>&1 &")))

(defn stop-memory-stress!
  "Stop any running stress-ng processes."
  [container]
  (docker-exec-ignore-error container "pkill" "-9" "stress-ng"))

(defn allocate-shm!
  "Allocate memory via /dev/shm."
  [container bytes-mb]
  (docker-exec container "dd" "if=/dev/zero" "of=/dev/shm/jepsen-mem"
               "bs=1M" (str "count=" bytes-mb)))

(defn free-shm!
  "Free /dev/shm allocation."
  [container]
  (docker-exec-ignore-error container "rm" "-f" "/dev/shm/jepsen-mem"))

(defn trigger-oom-pressure!
  "Trigger OOM conditions by allocating most available memory."
  [container]
  ;; Get total memory and allocate 90% of it
  (let [mem-info (docker-exec container "cat" "/proc/meminfo")
        total-line (first (filter #(str/starts-with? % "MemTotal:")
                                  (str/split-lines mem-info)))
        total-kb (when total-line
                   (Long/parseLong (second (str/split (str/trim total-line) #"\s+"))))
        alloc-mb (when total-kb (quot (* total-kb 90) (* 100 1024)))]
    (when alloc-mb
      (start-memory-stress! container alloc-mb)
      alloc-mb)))

(defn memory-pressure-nemesis
  "Creates a nemesis that applies memory pressure using stress-ng.

   Requires stress-ng to be installed in containers.

   Supports operations:
   - {:f :memory-stress :value {:node n :mb m}} - allocate memory
   - {:f :memory-release :value node} - release memory pressure
   - {:f :oom-trigger :value node} - trigger near-OOM conditions"
  ([]
   (memory-pressure-nemesis false))
  ([cluster-mode?]
   (let [stressed-nodes (atom #{})]
     (reify nemesis/Nemesis
       (setup! [this test]
         this)

       (invoke! [this test op]
         (case (:f op)
           :memory-stress
           (let [{:keys [node mb]} (:value op)
                 nodes (if node [node] (:nodes test))
                 alloc-mb (or mb 256)]
             (doseq [n nodes]
               (let [container (container-name n cluster-mode?)]
                 (info "Applying" alloc-mb "MB memory pressure to" n)
                 (try+
                   (start-memory-stress! container alloc-mb)
                   (swap! stressed-nodes conj n)
                   (catch Object e
                     (warn "Failed to apply memory stress to" n ":" e)))))
             (assoc op :value {:nodes nodes :mb alloc-mb}))

           :memory-release
           (let [nodes (if (coll? (:value op)) (:value op) [(:value op)])]
             (doseq [n nodes]
               (let [container (container-name n cluster-mode?)]
                 (info "Releasing memory pressure on" n)
                 (stop-memory-stress! container)
                 (free-shm! container)
                 (swap! stressed-nodes disj n)))
             (assoc op :value {:nodes nodes :status :released}))

           :oom-trigger
           (let [node (:value op)
                 container (container-name node cluster-mode?)]
             (info "Triggering OOM pressure on" node)
             (try+
               (let [allocated (trigger-oom-pressure! container)]
                 (swap! stressed-nodes conj node)
                 (assoc op :value {:node node :allocated-mb allocated}))
               (catch Object e
                 (warn "Failed to trigger OOM on" node ":" e)
                 (assoc op :type :fail :error :oom-trigger-failed))))))

       (teardown! [this test]
         ;; Release all memory pressure
         (doseq [n @stressed-nodes]
           (let [container (container-name n cluster-mode?)]
             (stop-memory-stress! container)
             (free-shm! container))))))))

(defn memory-pressure-generator
  "Generator for memory pressure cycles.

   Options:
   - :interval - time between pressure events (default 20s)
   - :pressure-duration - how long to maintain pressure (default 15s)
   - :pressure-mb - memory to allocate in MB (default 256)"
  [opts]
  (let [interval (get opts :interval 20)
        duration (get opts :pressure-duration 15)
        pressure-mb (get opts :pressure-mb 256)]
    (gen/cycle
      [(gen/sleep interval)
       {:type :info :f :memory-stress :value {:mb pressure-mb}}
       (gen/sleep duration)
       {:type :info :f :memory-release :value nil}
       (gen/sleep 5)])))

;; ===========================================================================
;; Raft Cluster Combined Nemesis
;; ===========================================================================

(defn raft-cluster-partition-nemesis
  "Creates a nemesis for partitioning Raft cluster nodes.
   Uses the cluster-db module for partition operations."
  []
  (let [raft-node-ips {"n1" "172.21.0.2"
                       "n2" "172.21.0.3"
                       "n3" "172.21.0.4"
                       "n4" "172.21.0.5"
                       "n5" "172.21.0.6"}]
    (reify nemesis/Nemesis
      (setup! [this test]
        this)

      (invoke! [this test op]
        (case (:f op)
          :partition
          (case (:value op)
            :leader-isolated
            (do
              (info "Partitioning: isolating leader from cluster")
              ;; Assume n1 is leader initially; in practice would detect leader
              (let [leader "n1"
                    followers (remove #{leader} (keys raft-node-ips))]
                (doseq [f followers]
                  (let [container (str "frogdb-raft-" leader)
                        target-ip (get raft-node-ips f)]
                    (docker-exec-ignore-error container "iptables" "-A" "INPUT" "-s" target-ip "-j" "DROP")
                    (docker-exec-ignore-error container "iptables" "-A" "OUTPUT" "-d" target-ip "-j" "DROP"))
                  (let [container (str "frogdb-raft-" f)
                        leader-ip (get raft-node-ips leader)]
                    (docker-exec-ignore-error container "iptables" "-A" "INPUT" "-s" leader-ip "-j" "DROP")
                    (docker-exec-ignore-error container "iptables" "-A" "OUTPUT" "-d" leader-ip "-j" "DROP"))))
              (assoc op :value :leader-isolated))

            :minority-isolated
            (do
              (info "Partitioning: isolating minority (n4, n5)")
              (let [minority ["n4" "n5"]
                    majority ["n1" "n2" "n3"]]
                (doseq [m minority
                        maj majority]
                  (let [m-container (str "frogdb-raft-" m)
                        maj-ip (get raft-node-ips maj)]
                    (docker-exec-ignore-error m-container "iptables" "-A" "INPUT" "-s" maj-ip "-j" "DROP")
                    (docker-exec-ignore-error m-container "iptables" "-A" "OUTPUT" "-d" maj-ip "-j" "DROP"))
                  (let [maj-container (str "frogdb-raft-" maj)
                        m-ip (get raft-node-ips m)]
                    (docker-exec-ignore-error maj-container "iptables" "-A" "INPUT" "-s" m-ip "-j" "DROP")
                    (docker-exec-ignore-error maj-container "iptables" "-A" "OUTPUT" "-d" m-ip "-j" "DROP"))))
              (assoc op :value :minority-isolated))

            :asymmetric
            (do
              (info "Partitioning: asymmetric (n1 can reach n2, but n2 cannot reach n1)")
              (let [container "frogdb-raft-n2"
                    target-ip (get raft-node-ips "n1")]
                (docker-exec-ignore-error container "iptables" "-A" "OUTPUT" "-d" target-ip "-j" "DROP"))
              (assoc op :value :asymmetric))

            ;; Default: isolate a specific node
            (if (and (vector? (:value op)) (= :isolate (first (:value op))))
              (let [node (second (:value op))
                    others (remove #{node} (keys raft-node-ips))]
                (info "Partitioning: isolating" node)
                (doseq [other others]
                  (let [container (str "frogdb-raft-" node)
                        other-ip (get raft-node-ips other)]
                    (docker-exec-ignore-error container "iptables" "-A" "INPUT" "-s" other-ip "-j" "DROP")
                    (docker-exec-ignore-error container "iptables" "-A" "OUTPUT" "-d" other-ip "-j" "DROP"))
                  (let [other-container (str "frogdb-raft-" other)
                        node-ip (get raft-node-ips node)]
                    (docker-exec-ignore-error other-container "iptables" "-A" "INPUT" "-s" node-ip "-j" "DROP")
                    (docker-exec-ignore-error other-container "iptables" "-A" "OUTPUT" "-d" node-ip "-j" "DROP")))
                (assoc op :value [:isolated node]))
              (do
                (warn "Unknown partition type:" (:value op))
                (assoc op :type :fail :error :unknown-partition-type))))

          :heal
          (do
            (info "Healing all Raft cluster partitions")
            (doseq [node (keys raft-node-ips)]
              (let [container (str "frogdb-raft-" node)]
                (docker-exec-ignore-error container "iptables" "-F")))
            (assoc op :value :healed))))

      (teardown! [this test]
        (doseq [node (keys raft-node-ips)]
          (let [container (str "frogdb-raft-" node)]
            (docker-exec-ignore-error container "iptables" "-F")))))))

(defn raft-cluster-nemesis
  "Creates a combined nemesis for Raft cluster testing.
   Includes: kills, pauses, partitions, slow network, disk failures."
  []
  (nemesis/compose
    {{:kill :kill
      :start :start} (process-killer)
     {:pause :pause
      :resume :resume} (process-pauser)
     {:partition :partition
      :heal :heal} (raft-cluster-partition-nemesis)
     {:add-latency :add-latency
      :packet-loss :packet-loss
      :network-heal :network-heal
      :network-heal-all :network-heal-all} (slow-network-nemesis true)
     {:disk-readonly :disk-readonly
      :disk-recover :disk-recover
      :disk-full :disk-full
      :disk-clear :disk-clear} (disk-failure-nemesis true)
     {:clock-skew :clock-skew
      :clock-reset :clock-reset} (clock-skew-nemesis true)
     {:memory-stress :memory-stress
      :memory-release :memory-release} (memory-pressure-nemesis true)}))

(defn raft-cluster-generator
  "Generator for Raft cluster chaos testing.
   Mixes various fault types with weighted random selection.

   Options:
   - :interval - base interval between faults (default 15s)"
  [opts]
  (let [interval (get opts :interval 15)]
    (gen/cycle
      [(gen/sleep interval)
       (gen/once
         (rand-nth
           [;; Kill leader
            [{:type :info :f :kill :value ["n1"]}
             (gen/sleep 5)
             {:type :info :f :start :value ["n1"]}]
            ;; Partition leader
            [{:type :info :f :partition :value :leader-isolated}
             (gen/sleep 10)
             {:type :info :f :heal}]
            ;; Slow down nodes
            [{:type :info :f :add-latency :value {:node "n2" :delay-ms 200 :jitter-ms 50}}
             (gen/sleep 10)
             {:type :info :f :network-heal :value "n2"}]
            ;; Disk failure on follower
            [{:type :info :f :disk-readonly :value "n3"}
             (gen/sleep 10)
             {:type :info :f :disk-recover :value "n3"}]
            ;; Asymmetric partition
            [{:type :info :f :partition :value :asymmetric}
             (gen/sleep 10)
             {:type :info :f :heal}]
            ;; Pause a node
            [{:type :info :f :pause :value ["n2"]}
             (gen/sleep 5)
             {:type :info :f :resume :value ["n2"]}]]))])))

;; ===========================================================================
;; Replication Combined Nemesis
;; ===========================================================================

(defn replication-nemesis
  "Creates a combined nemesis for replication testing.
   Includes process kills, pauses, and network partitions."
  []
  (nemesis/compose
    {{:kill :kill
      :start :start} (process-killer)
     {:pause :pause
      :resume :resume} (process-pauser)
     {:partition :partition
      :heal :heal} (partition-nemesis)}))

(defn replication-generator
  "Generator for replication testing that mixes various fault types.

   Options:
   - :interval - base interval between faults"
  [opts]
  (let [interval (get opts :interval 15)]
    (gen/cycle
      [(gen/sleep interval)
       (gen/once
         (rand-nth
           [;; Kill primary
            [{:type :info :f :kill :value ["n1"]}
             (gen/sleep 5)
             {:type :info :f :start :value ["n1"]}]
            ;; Partition primary
            [{:type :info :f :partition :value :primary-isolated}
             (gen/sleep 10)
             {:type :info :f :heal}]
            ;; Kill a replica
            [{:type :info :f :kill :value ["n2"]}
             (gen/sleep 5)
             {:type :info :f :start :value ["n2"]}]
            ;; Pause primary
            [{:type :info :f :pause :value ["n1"]}
             (gen/sleep 3)
             {:type :info :f :resume :value ["n1"]}]]))])))

;; ===========================================================================
;; Nemesis Packages
;; ===========================================================================

(defn partition
  "Network partition nemesis package.

   Options:
   - :interval - time between partition cycles
   - :partition-type - :primary-isolated or :halves"
  [opts]
  {:nemesis (partition-nemesis)
   :generator (partition-generator opts)
   :final-generator (gen/once {:type :info :f :heal})})

(defn all-replication
  "Combined replication nemesis with kills, pauses, and partitions.

   Options:
   - :interval - time between faults"
  [opts]
  {:nemesis (replication-nemesis)
   :generator (replication-generator opts)
   :final-generator (gen/phases
                      (gen/once {:type :info :f :heal})
                      (gen/once {:type :info :f :resume})
                      (gen/once {:type :info :f :start}))})

;; ===========================================================================
;; Additional Nemesis Packages
;; ===========================================================================

(defn clock-skew
  "Clock skew nemesis package.

   Options:
   - :interval - time between skew cycles
   - :max-skew-ms - maximum clock skew"
  [opts]
  {:nemesis (clock-skew-nemesis)
   :generator (clock-skew-generator opts)
   :final-generator (gen/once {:type :info :f :clock-reset})})

(defn disk-failure
  "Disk failure nemesis package.

   Options:
   - :interval - time between failures
   - :failure-type - :readonly or :full"
  [opts]
  {:nemesis (disk-failure-nemesis)
   :generator (disk-failure-generator opts)
   :final-generator (gen/phases
                      (gen/once {:type :info :f :disk-recover :value "n1"})
                      (gen/once {:type :info :f :disk-clear :value "n1"}))})

(defn slow-network
  "Slow network nemesis package.

   Options:
   - :interval - time between impairments
   - :delay-ms - latency to add"
  [opts]
  {:nemesis (slow-network-nemesis)
   :generator (slow-network-generator opts)
   :final-generator (gen/once {:type :info :f :network-heal-all})})

(defn memory-pressure
  "Memory pressure nemesis package.

   Options:
   - :interval - time between pressure events
   - :pressure-mb - memory to allocate"
  [opts]
  {:nemesis (memory-pressure-nemesis)
   :generator (memory-pressure-generator opts)
   :final-generator (gen/once {:type :info :f :memory-release :value nil})})

(defn raft-cluster
  "Raft cluster combined nemesis package.

   Options:
   - :interval - time between faults"
  [opts]
  {:nemesis (raft-cluster-nemesis)
   :generator (raft-cluster-generator opts)
   :final-generator (gen/phases
                      (gen/once {:type :info :f :heal})
                      (gen/once {:type :info :f :network-heal-all})
                      (gen/once {:type :info :f :disk-recover :value "n1"})
                      (gen/once {:type :info :f :disk-recover :value "n2"})
                      (gen/once {:type :info :f :disk-recover :value "n3"})
                      (gen/once {:type :info :f :memory-release :value nil})
                      (gen/once {:type :info :f :clock-reset})
                      (gen/once {:type :info :f :resume})
                      (gen/once {:type :info :f :start}))})

(defn nemesis-package
  "Select a nemesis package by name.

   Available packages:
   - :none - no faults
   - :kill - process crashes
   - :pause - process pauses
   - :rapid-kill - aggressive kill/restart cycles
   - :partition - network partitions
   - :clock-skew - clock manipulation
   - :disk-failure - disk failures
   - :slow-network - network latency and packet loss
   - :memory-pressure - memory exhaustion
   - :all - combined single-node faults
   - :all-replication - combined replication faults
   - :raft-cluster - combined Raft cluster faults"
  [name opts]
  (case name
    :none (none)
    :kill (kill opts)
    :pause (pause opts)
    :rapid-kill (rapid-kill opts)
    :partition (partition opts)
    :clock-skew (clock-skew opts)
    :disk-failure (disk-failure opts)
    :slow-network (slow-network opts)
    :memory-pressure (memory-pressure opts)
    :all (all opts)
    :all-replication (all-replication opts)
    :raft-cluster (raft-cluster opts)
    ;; Default to none
    (none)))
