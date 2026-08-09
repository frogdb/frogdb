(ns jepsen.frogdb.core
  "Main entry point for FrogDB Jepsen tests.

   Provides CLI interface for running various consistency tests
   against FrogDB, including register and counter workloads with
   optional crash testing."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.checker :as checker]
            [jepsen.cli :as cli]
            [jepsen.generator :as gen]
            [jepsen.os :as os]
            [jepsen.tests :as tests]
            [jepsen.frogdb.append :as append]
            [jepsen.frogdb.blocking :as blocking]
            [jepsen.frogdb.client :as client]
            [jepsen.frogdb.counter :as counter]
            [jepsen.frogdb.db :as db]
            [jepsen.frogdb.expiry :as expiry]
            [jepsen.frogdb.hash :as hash]
            [jepsen.frogdb.invariant :as invariant]
            [jepsen.frogdb.lag :as lag]
            [jepsen.frogdb.nemesis :as nemesis]
            [jepsen.frogdb.pubsub-order :as pubsub-order]
            [jepsen.frogdb.queue :as queue]
            [jepsen.frogdb.register :as register]
            [jepsen.frogdb.ryw :as ryw]
            [jepsen.frogdb.wc-order :as wc-order]
            [jepsen.frogdb.replication :as replication]
            [jepsen.frogdb.replication-failover :as replication-failover]
            [jepsen.frogdb.set :as set-workload]
            [jepsen.frogdb.sortedset :as sortedset]
            [jepsen.frogdb.split-brain :as split-brain]
            [jepsen.frogdb.transaction :as transaction]
            [jepsen.frogdb.zombie :as zombie]
            ;; Elle workloads
            [jepsen.frogdb.list-append :as list-append]
            ;; Raft cluster workloads
            [jepsen.frogdb.cluster-db :as cluster-db]
            [jepsen.frogdb.cluster-client :as cluster-client]
            [jepsen.frogdb.cluster-formation :as cluster-formation]
            [jepsen.frogdb.cluster-replication :as cluster-replication]
            [jepsen.frogdb.leader-election :as leader-election]
            [jepsen.frogdb.slot-migration :as slot-migration]
            [jepsen.frogdb.cross-slot :as cross-slot]
            [jepsen.frogdb.key-routing :as key-routing]
            [jepsen.frogdb.split-brain-raft :as split-brain-raft]
            [jepsen.frogdb.zombie-raft :as zombie-raft]
            ;; Gap analysis workloads
            [jepsen.frogdb.migration-recovery :as migration-recovery]
            [jepsen.frogdb.concurrent-migration :as concurrent-migration]
            [jepsen.frogdb.elle-rw-register :as elle-rw-register]
            [jepsen.frogdb.partition-recovery :as partition-recovery]
            [jepsen.frogdb.membership-routing :as membership-routing]
            [jepsen.frogdb.rolling-restart :as rolling-restart])
  (:gen-class))

;; ===========================================================================
;; OS Implementation for Docker/Local
;; ===========================================================================

(def docker-os
  "OS implementation for Docker containers.
   FrogDB runs in pre-configured containers, so most OS operations are no-ops."
  (reify os/OS
    (setup! [_ test node]
      (info "Docker OS setup for" node "(no-op)"))

    (teardown! [_ test node]
      (info "Docker OS teardown for" node "(no-op)"))))

;; ===========================================================================
;; Workload Selection
;; ===========================================================================

(def workloads
  "Available workloads for testing."
  {:register register/workload
   :counter counter/workload
   :append append/workload
   :transaction transaction/workload
   :queue queue/workload
   :set set-workload/workload
   :hash hash/workload
   :sortedset sortedset/workload
   :expiry expiry/workload
   :blocking blocking/workload
   ;; Consistency-guarantee workloads (single-connection RYW / ordering / pubsub)
   :ryw ryw/workload
   :wc-order wc-order/workload
   :pubsub-order pubsub-order/workload
   ;; Replication workloads
   :replication replication/workload
   :replication-failover replication-failover/workload
   :replication-failover-chain replication-failover/chain-workload
   :split-brain split-brain/workload
   :zombie zombie/workload
   :lag lag/workload
   ;; Raft cluster workloads
   :cluster-formation cluster-formation/workload
   :leader-election leader-election/workload
   :slot-migration slot-migration/workload
   :cross-slot cross-slot/workload
   :key-routing key-routing/workload
   :split-brain-raft split-brain-raft/workload
   :zombie-raft zombie-raft/workload
   ;; Cluster-mode data replication (per-node PSYNC inside a Raft cluster)
   :cluster-replication cluster-replication/workload
   :cluster-lag lag/cluster-workload
   ;; Elle workloads
   :list-append list-append/workload
   ;; Gap analysis workloads
   :migration-recovery migration-recovery/workload
   :concurrent-migration concurrent-migration/workload
   :elle-rw-register elle-rw-register/workload
   :partition-recovery partition-recovery/workload
   :membership-routing membership-routing/workload
   :rolling-restart rolling-restart/workload})

(defn get-workload
  "Get a workload by name with options."
  [name opts]
  (if-let [workload-fn (get workloads (keyword name))]
    (workload-fn opts)
    (throw (IllegalArgumentException.
             (str "Unknown workload: " name
                  ". Available: " (str/join ", " (map clojure.core/name (keys workloads))))))))

;; ===========================================================================
;; Checkers
;; ===========================================================================

(def stats-all-fail-ok-fs
  "Op :f values with a documented-legal all-:fail outcome, so a bare zero-:ok
   count for that :f must not sink the run's :valid?.

   :exec-queued-txn (slot-migration / slot-migration-partition) — EXEC of a
   transaction queued before/during a slot migration legitimately redirects
   with MOVED every time the migration wins the race; the workload's own
   generator emits it exactly once per run (gen/once), so a single legal
   redirect zeroes the :f. See slot_migration.clj:337-339 and :795 (the
   workload checker itself gates the property that actually matters — no
   orphaned write — and reports this outcome for visibility, not gating).
   See hardening issue 31 (\"P3 — stop gating on bare checker/stats\")."
  #{:exec-queued-txn})

(defn stats-ignoring
  "Wraps jepsen.checker/stats, recomputing :valid? while ignoring any :f in
   `ignored-fs` when deciding validity. jepsen's stock stats checker treats
   any :f with zero :ok operations as invalid (jepsen.checker/stats,
   `merge-valid` over every :by-f entry) — correct in general, but wrong for
   an :f whose only legal outcome under some fault schedules is a bare
   :fail. Per-:f counts for ignored :f's are left untouched in the report
   (:by-f, :all) so they stay visible; they just don't participate in the
   :valid? merge. Real signal for every other :f is unaffected."
  [ignored-fs]
  (reify checker/Checker
    (check [this test history opts]
      (let [result (checker/check (checker/stats) test history opts)
            gating-valids (->> (:by-f result)
                               (remove (fn [[f _]] (contains? ignored-fs f)))
                               (map (fn [[_ v]] (:valid? v))))]
        (assoc result :valid? (checker/merge-valid gating-valids))))))

;; ===========================================================================
;; Test Construction
;; ===========================================================================

(defn frogdb-test
  "Construct a Jepsen test for FrogDB.

   Options:
   - :workload - workload name (register, counter, replication, cluster-*, etc.)
   - :nemesis - nemesis type (none, kill, pause, partition, all, all-replication, raft-cluster)
   - :rate - operations per second
   - :time-limit - test duration in seconds
   - :nodes - list of nodes to test
   - :replication - if true, use 3-node replication cluster
   - :cluster - if true, use Raft cluster mode
   - :cluster-nodes - number of nodes for cluster mode (default 3)"
  [opts]
  (let [workload (get-workload (:workload opts) opts)
        nemesis-pkg (nemesis/nemesis-package (keyword (:nemesis opts)) opts)
        local? (:local opts)
        docker? (:docker opts)
        replication? (:replication opts)
        cluster? (:cluster opts)
        ;; Cluster node count — always use the explicit value or default 3.
        ;; The 3-node Raft cluster auto-bootstraps; n4/n5 are standalone
        ;; and available for membership-change tests (CLUSTER MEET).
        cluster-node-count (get opts :cluster-nodes 3)
        ;; Replication workloads default to multi-node
        replication-workload? (contains? #{:replication :replication-failover
                                           :replication-failover-chain
                                           :split-brain :zombie :lag
                                           :partition-recovery}
                                         (keyword (:workload opts)))
        ;; Cluster workloads default to cluster mode
        cluster-workload? (contains? #{:cluster-formation :leader-election :slot-migration
                                       :cross-slot :key-routing
                                       :cluster-replication :cluster-lag
                                       :migration-recovery :concurrent-migration
                                       :membership-routing :rolling-restart
                                       :split-brain-raft :zombie-raft}
                                     (keyword (:workload opts)))
        multi-node? (or replication? replication-workload?)
        cluster-mode? (or cluster? cluster-workload?)
        topology (cond cluster-mode? :raft
                       multi-node? :replication
                       :else :single)
        nodes (cond
                local? ["n1"]
                cluster-mode? (vec (map #(str "n" (inc %)) (range cluster-node-count)))
                ;; Multi-node (replication) defaults to the full 3-node set, but an
                ;; explicit --node/:nodes list pins the client to a subset. This lets a
                ;; single-key linearizable workload (register) run on the replication
                ;; topology while only ever talking to the primary — the async replicas
                ;; stay reachable by the partition nemesis (by IP) yet never serve the
                ;; client, so Knossos linearizability stays valid.
                ;;
                ;; A bare `(seq (:nodes opts))` cannot detect "explicit" here: jepsen's
                ;; own CLI (`jepsen.cli/parse-nodes`, run as part of `cli/test-opt-fn`
                ;; before this ever executes) ALWAYS populates :nodes, defaulting to
                ;; `cli/default-nodes` (a 5-node ["n1".."n5"] list sized for the 5-node
                ;; Raft topology) whenever no --node/--nodes/--nodes-file flag was
                ;; passed at all. That made this branch pick up the 5-node default for
                ;; every ordinary replication test (nothing pins client_nodes except
                ;; register-partition), which the 3-node replication Docker topology
                ;; doesn't have containers for — n4/n5 setup! spins forever waiting for
                ;; a server that never starts, timing out the whole suite (hardening
                ;; issue 32). Compare by value against cli/default-nodes (parse-nodes
                ;; rebuilds the vector via concat+vec, so it's no longer `identical?`
                ;; to the original default) to tell a real override apart from jepsen's
                ;; own unrequested default.
                multi-node? (if (and (seq (:nodes opts))
                                      (not= (vec (:nodes opts)) cli/default-nodes))
                              (vec (:nodes opts))
                              ["n1" "n2" "n3"])
                docker? ["n1"]
                :else (or (:nodes opts) ["n1"]))]
    (merge tests/noop-test
           opts
           {:topology topology
            :name (str "frogdb-" (:workload opts)
                       (when (not= "none" (:nemesis opts))
                         (str "-" (:nemesis opts)))
                       (when local? "-local")
                       (when docker? "-docker")
                       (when cluster-mode? "-cluster")
                       (when (and multi-node? (not cluster-mode?)) "-replication"))
            :nodes nodes
            :cluster-nodes nodes  ; Make available to clients
            :os docker-os
            :db (cond
                  local? (db/local-db)
                  cluster-mode? (cluster-db/cluster-db {:initial-nodes nodes
                                                         :docker-host? true
                                                         :base-port (:base-port opts)})
                  multi-node? (db/replication-db)
                  docker? (db/docker-db)
                  :else (db/docker-db))
            ;; Use dummy SSH for docker/local modes - we use docker exec instead
            :ssh (when (or local? docker? multi-node? cluster-mode?)
                   {:dummy? true})
            :client (:client workload)
            :nemesis (if cluster-mode?
                       (invariant/wrap-nemesis (:nemesis nemesis-pkg))
                       (:nemesis nemesis-pkg))
            :checker (checker/compose
                       (merge
                         {:workload (:checker workload)
                          :stats (stats-ignoring stats-all-fail-ok-fs)
                          :exceptions (checker/unhandled-exceptions)
                          :perf (checker/perf)}
                         ;; DEBUG CLUSTER CHECK invariant sweep (cluster-correctness
                         ;; issue 07) — wired on every raft-topology workload by
                         ;; default, never opt-in per workload. See
                         ;; jepsen.frogdb.invariant's namespace docstring for the
                         ;; quiesce/final hook design.
                         (when cluster-mode?
                           {:cluster-invariants (invariant/checker)})))
            :generator (gen/phases
                         ;; Main test phase: mix client operations with nemesis.
                         ;; No :cluster-check op is ever emitted here — this is
                         ;; the active-nemesis window the checker must not touch
                         ;; (issue 07's overhead constraint).
                         (->> (:generator workload)
                              (gen/nemesis (:generator nemesis-pkg))
                              (gen/time-limit (:time-limit opts)))
                         ;; Final recovery phase
                         (gen/log "Recovering from faults...")
                         (gen/nemesis (:final-generator nemesis-pkg))
                         (gen/sleep 5)
                         ;; Quiesce-point invariant sweep: every nemesis package's
                         ;; :final-generator has now fully healed the cluster, so
                         ;; no fault is active by construction.
                         (when cluster-mode?
                           (gen/nemesis (gen/once (invariant/cluster-check-op))))
                         ;; Final reads to verify state
                         (gen/log "Final reads...")
                         (gen/clients
                           (or (:final-generator workload)
                               (->> (gen/repeat {:f :read})
                                    (gen/limit 10)
                                    (gen/stagger 0.1))))
                         ;; Final invariant sweep, after the final reads.
                         (when cluster-mode?
                           (gen/nemesis (gen/once (invariant/cluster-check-op)))))})))

;; ===========================================================================
;; CLI Options
;; ===========================================================================

(def cli-opts
  "CLI options for FrogDB Jepsen tests."
  [["-w" "--workload WORKLOAD" "Workload to run"
    :default "register"
    :validate [#(contains? workloads (keyword %))
               (str "Must be one of: " (str/join ", " (map name (keys workloads))))]]

   [nil "--nemesis NEMESIS" "Nemesis type"
    :default "none"
    :validate [#(contains? #{:none :kill :pause :rapid-kill :partition
                             :clock-skew :disk-failure :slow-network :memory-pressure
                             :all :all-replication :raft-cluster
                             :raft-cluster-membership} (keyword %))
               "Must be one of: none, kill, pause, rapid-kill, partition, clock-skew, disk-failure, slow-network, memory-pressure, all, all-replication, raft-cluster, raft-cluster-membership"]]

   ["-r" "--rate RATE" "Operations per second"
    :default 10
    :parse-fn #(Double/parseDouble %)
    :validate [pos? "Must be positive"]]

   [nil "--interval INTERVAL" "Nemesis interval in seconds"
    :default 10
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--independent" "Use independent per-key linearizable testing (register: per-key registers; hash: per-field registers)"
    :default false]

   [nil "--membership-changes" "Drive cluster membership changes (node join/leave) from the workload generator. Currently consumed by the cluster-formation workload, which switches to its membership-change-generator when set."
    :default false]

   [nil "--local" "Local testing mode (FrogDB already running, no Docker)"
    :default false]

   [nil "--docker" "Docker testing mode (use docker-compose containers)"
    :default false]

   [nil "--replication" "Use 3-node replication cluster"
    :default false]

   [nil "--cluster" "Use Raft cluster mode (5-node)"
    :default false]

   [nil "--cluster-nodes NUM" "Number of cluster nodes (default 3)"
    :default 3
    :parse-fn #(Integer/parseInt %)
    :validate [#(and (>= % 1) (<= % 5)) "Must be between 1 and 5"]]

   [nil "--base-port PORT" "Base host port for Docker port mapping (default 16379)"
    :default 16379
    :parse-fn #(Integer/parseInt %)
    :validate [pos? "Must be positive"]]])

(def all-cli-opts
  "All CLI options including Jepsen's standard options."
  (concat cli-opts cli/test-opt-spec))

(def batch-cli-opts
  "Additional CLI options for batch test execution."
  [[nil "--batch-file PATH" "EDN file with batch test configurations"]])

;; ===========================================================================
;; Batch Execution
;; ===========================================================================

(defn batch-tests-fn
  "Generate test maps from a batch EDN file.
   Each entry in the EDN vector is merged with CLI options to form a test config."
  [options]
  (let [path (:batch-file options)
        configs (edn/read-string (slurp path))]
    (map #(frogdb-test (merge options %)) configs)))

;; ===========================================================================
;; Commands
;; ===========================================================================

(defn test-cmd
  "Run a single test."
  []
  {"test"
   {:opt-spec all-cli-opts
    :opt-fn cli/test-opt-fn
    :usage "Run a FrogDB Jepsen test"
    :run (fn [{:keys [options]}]
           (info "Running FrogDB Jepsen test with options:" options)
           (let [test (frogdb-test options)]
             (jepsen.core/run! test)))}})

;; ===========================================================================
;; Main Entry Point
;; ===========================================================================

(defn -main
  "Main entry point for FrogDB Jepsen tests.

   Usage:
     lein run test --workload register --nemesis none --time-limit 60
     lein run test --workload counter --nemesis kill --time-limit 120
     lein run test-all --docker --batch-file tests.edn"
  [& args]
  (cli/run!
    (merge (cli/single-test-cmd {:test-fn frogdb-test
                                 :opt-spec cli-opts})
           (cli/test-all-cmd {:tests-fn batch-tests-fn
                              :opt-spec (into cli-opts batch-cli-opts)})
           (cli/serve-cmd))
    args))
