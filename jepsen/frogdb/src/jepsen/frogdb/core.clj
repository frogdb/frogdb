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
            [jepsen.frogdb.lag :as lag]
            [jepsen.frogdb.nemesis :as nemesis]
            [jepsen.frogdb.queue :as queue]
            [jepsen.frogdb.register :as register]
            [jepsen.frogdb.replication :as replication]
            [jepsen.frogdb.set :as set-workload]
            [jepsen.frogdb.sortedset :as sortedset]
            [jepsen.frogdb.split-brain :as split-brain]
            [jepsen.frogdb.transaction :as transaction]
            [jepsen.frogdb.zombie :as zombie]
            ;; Raft cluster workloads
            [jepsen.frogdb.cluster-db :as cluster-db]
            [jepsen.frogdb.cluster-client :as cluster-client]
            [jepsen.frogdb.cluster-formation :as cluster-formation]
            [jepsen.frogdb.leader-election :as leader-election]
            [jepsen.frogdb.slot-migration :as slot-migration]
            [jepsen.frogdb.cross-slot :as cross-slot]
            [jepsen.frogdb.key-routing :as key-routing])
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
   ;; Replication workloads
   :replication replication/workload
   :split-brain split-brain/workload
   :zombie zombie/workload
   :lag lag/workload
   ;; Raft cluster workloads
   :cluster-formation cluster-formation/workload
   :leader-election leader-election/workload
   :slot-migration slot-migration/workload
   :cross-slot cross-slot/workload
   :key-routing key-routing/workload})

(defn get-workload
  "Get a workload by name with options."
  [name opts]
  (if-let [workload-fn (get workloads (keyword name))]
    (workload-fn opts)
    (throw (IllegalArgumentException.
             (str "Unknown workload: " name
                  ". Available: " (str/join ", " (map clojure.core/name (keys workloads))))))))

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
        cluster-node-count (get opts :cluster-nodes 3)
        ;; Replication workloads default to multi-node
        replication-workload? (contains? #{:replication :split-brain :zombie :lag}
                                         (keyword (:workload opts)))
        ;; Cluster workloads default to cluster mode
        cluster-workload? (contains? #{:cluster-formation :leader-election :slot-migration
                                       :cross-slot :key-routing}
                                     (keyword (:workload opts)))
        multi-node? (or replication? replication-workload?)
        cluster-mode? (or cluster? cluster-workload?)
        nodes (cond
                local? ["n1"]
                cluster-mode? (vec (map #(str "n" (inc %)) (range cluster-node-count)))
                multi-node? ["n1" "n2" "n3"]
                docker? ["n1"]
                :else (or (:nodes opts) ["n1"]))]
    (merge tests/noop-test
           opts
           {:name (str "frogdb-" (:workload opts)
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
                                                         :docker-host? true})
                  multi-node? (db/replication-db)
                  docker? (db/docker-db)
                  :else (db/docker-db))
            ;; Use dummy SSH for docker/local modes - we use docker exec instead
            :ssh (when (or local? docker? multi-node? cluster-mode?)
                   {:dummy? true})
            :client (:client workload)
            :nemesis (:nemesis nemesis-pkg)
            :checker (checker/compose
                       {:workload (:checker workload)
                        :stats (checker/stats)
                        :exceptions (checker/unhandled-exceptions)
                        :perf (checker/perf)})
            :generator (gen/phases
                         ;; Main test phase: mix client operations with nemesis
                         (->> (:generator workload)
                              (gen/nemesis (:generator nemesis-pkg))
                              (gen/time-limit (:time-limit opts)))
                         ;; Final recovery phase
                         (gen/log "Recovering from faults...")
                         (gen/nemesis (:final-generator nemesis-pkg))
                         (gen/sleep 5)
                         ;; Final reads to verify state
                         (gen/log "Final reads...")
                         (gen/clients
                           (->> (gen/repeat {:f :read})
                                (gen/limit 10)
                                (gen/stagger 0.1))))})))

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
                             :all :all-replication :raft-cluster} (keyword %))
               "Must be one of: none, kill, pause, rapid-kill, partition, clock-skew, disk-failure, slow-network, memory-pressure, all, all-replication, raft-cluster"]]

   ["-r" "--rate RATE" "Operations per second"
    :default 10
    :parse-fn #(Double/parseDouble %)
    :validate [pos? "Must be positive"]]

   [nil "--interval INTERVAL" "Nemesis interval in seconds"
    :default 10
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--independent" "Use independent key testing for register workload"
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
    :validate [#(and (>= % 1) (<= % 5)) "Must be between 1 and 5"]]])

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
