(ns jepsen.frogdb.nemesis
  "Nemesis implementations for FrogDB testing.

   Provides fault injection including:
   - Process crashes (SIGKILL)
   - Process pauses (SIGSTOP/SIGCONT)
   - Graceful restarts (SIGTERM)
   - Network partitions (via iptables)
   - Disk failures (read-only filesystem)
   - Clock skew

   These nemeses help verify crash recovery, durability, and
   distributed systems properties."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.db :as db]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as nc]
            [jepsen.util :as util]
            [jepsen.frogdb.db :as frogdb-db]))

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

   Supports operations:
   - {:f :partition :value :primary-isolated} - isolate primary (n1) from replicas
   - {:f :partition :value :halves} - split into [n1,n2] vs [n3]
   - {:f :partition :value [:isolate node]} - isolate specific node
   - {:f :heal} - remove all partitions"
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      this)

    (invoke! [this test op]
      (case (:f op)
        :partition
        (case (:value op)
          :primary-isolated
          (do
            (info "Partitioning: isolating primary from replicas")
            (frogdb-db/isolate-primary!)
            (assoc op :value :primary-isolated))

          :halves
          (do
            (info "Partitioning: splitting cluster into halves [n1,n2] vs [n3]")
            (frogdb-db/partition-halves!)
            (assoc op :value :halves))

          ;; Handle [:isolate node] pattern
          (if (and (vector? (:value op))
                   (= :isolate (first (:value op))))
            (let [node (second (:value op))]
              (info "Partitioning: isolating node" node)
              (frogdb-db/partition-node! node (remove #{node} ["n1" "n2" "n3"]))
              (assoc op :value [:isolated node]))
            (do
              (warn "Unknown partition type:" (:value op))
              (assoc op :type :fail :error :unknown-partition-type))))

        :heal
        (do
          (info "Healing all network partitions")
          (frogdb-db/heal-all!)
          (assoc op :value :healed))))

    (teardown! [this test]
      ;; Clean up any remaining partitions
      (frogdb-db/heal-all!))))

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
;; Clock Skew Nemesis (Stub - requires container timezone manipulation)
;; ===========================================================================

(defn clock-skew-nemesis
  "Creates a nemesis that skews clocks on nodes.

   Note: This is a stub implementation. Full implementation requires
   using tools like libfaketime or date manipulation in containers.

   Supports operations:
   - {:f :clock-skew :value {:node n :delta-ms ms}} - skew clock on node
   - {:f :clock-reset} - reset all clocks"
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      (warn "Clock skew nemesis is a stub - operations will be no-ops")
      this)

    (invoke! [this test op]
      (case (:f op)
        :clock-skew
        (do
          (info "Clock skew (stub): would skew" (:value op))
          (assoc op :value :skewed-stub))

        :clock-reset
        (do
          (info "Clock reset (stub): would reset clocks")
          (assoc op :value :reset-stub))))

    (teardown! [this test]
      nil)))

;; ===========================================================================
;; Disk Failure Nemesis (Stub - requires filesystem manipulation)
;; ===========================================================================

(defn disk-failure-nemesis
  "Creates a nemesis that simulates disk failures.

   Note: This is a stub implementation. Full implementation requires
   mounting the data volume as tmpfs or using device-mapper for true
   disk failure simulation.

   Supports operations:
   - {:f :disk-readonly :value node} - make disk read-only
   - {:f :disk-recover :value node} - restore disk to read-write"
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      (warn "Disk failure nemesis is a stub - operations will be no-ops")
      this)

    (invoke! [this test op]
      (case (:f op)
        :disk-readonly
        (do
          (info "Disk readonly (stub): would make disk read-only on" (:value op))
          (assoc op :value :readonly-stub))

        :disk-recover
        (do
          (info "Disk recover (stub): would restore disk on" (:value op))
          (assoc op :value :recovered-stub))))

    (teardown! [this test]
      nil)))

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
           ;; Kill primary
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
            {:type :info :f :resume :value ["n1"]}]))])))

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

(defn nemesis-package
  "Select a nemesis package by name.

   Available packages:
   - :none - no faults
   - :kill - process crashes
   - :pause - process pauses
   - :rapid-kill - aggressive kill/restart cycles
   - :partition - network partitions
   - :all - combined single-node faults
   - :all-replication - combined replication faults"
  [name opts]
  (case name
    :none (none)
    :kill (kill opts)
    :pause (pause opts)
    :rapid-kill (rapid-kill opts)
    :partition (partition opts)
    :all (all opts)
    :all-replication (all-replication opts)
    ;; Default to none
    (none)))
