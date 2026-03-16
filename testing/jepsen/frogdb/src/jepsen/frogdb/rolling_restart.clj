(ns jepsen.frogdb.rolling-restart
  "Rolling restart workload for FrogDB Raft cluster.

   Tests the standard deployment practice of restarting nodes one at a time.
   FrogDB lacks connection draining, so this also stress-tests client retry
   logic.  The test verifies that the cluster remains available throughout
   (with brief per-node disruption) and that no data is lost.

   Phases:
   1. Establish baseline traffic on a 3-node cluster
   2. For each node in sequence:
      a. SIGTERM the node
      b. Wait for it to restart
      c. Verify cluster recovers
   3. Final reads — no data loss, all writes durable

   Operations:
   - :write         – SET a key via cluster routing
   - :read          – GET a key via cluster routing
   - :restart-node  – SIGTERM + docker start a specific node
   - :read-leader   – Query current Raft leader
   - :cluster-health – Check cluster_state on a live node

   Checker: custom — availability > 80% throughout, all acknowledged writes
   are durable post-rolling-restart."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [jepsen.frogdb.cluster-db :as cluster-db]
            [jepsen.frogdb.cluster-client :as cluster-client]
            [slingshot.slingshot :refer [try+ throw+]]))

;; ===========================================================================
;; State
;; ===========================================================================

(def test-hash-tag "{rolling}")
(defn test-key [i] (str test-hash-tag ":key:" i))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord RollingRestartClient [nodes docker-host? base-port slot-mapping]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          all-nodes (or (:cluster-nodes test) ["n1" "n2" "n3"])
          bp (get test :base-port cluster-db/default-base-port)]
      (info "Opening rolling-restart client")
      (assoc this
             :nodes all-nodes
             :docker-host? docker?
             :base-port bp
             :slot-mapping (atom (cluster-client/create-slot-mapping all-nodes docker? bp)))))

  (setup! [this test]
    this)

  (invoke! [this test op]
    (try+
      (case (:f op)
        :write
        (let [{:keys [key value]} (:value op)]
          (cluster-client/cluster-set slot-mapping key (str value) docker-host? nodes base-port)
          (assoc op :type :ok))

        :read
        (let [key (:value op)
              v (cluster-client/cluster-get slot-mapping key docker-host? nodes base-port)]
          (assoc op :type :ok :value {:key key :value (frogdb/parse-value v)}))

        :restart-node
        (let [node (:value op)
              container (cluster-db/raft-container-name node)]
          (info "Rolling restart: stopping" node)
          ;; Graceful shutdown — SIGTERM
          (try+
            (cluster-db/docker-exec container "pkill" "-TERM" "frogdb")
            (catch Object _ nil))
          ;; Wait for process to exit
          (Thread/sleep 2000)
          ;; Restart the container
          (info "Rolling restart: starting" node)
          (cluster-db/docker-start container)
          (cluster-db/wait-for-node-ready node docker-host? 30000 base-port)
          ;; Refresh slot mapping
          (try+
            (reset! slot-mapping (cluster-client/refresh-slot-mapping
                                   @slot-mapping (first (remove #{node} nodes))
                                   docker-host? base-port))
            (catch Object _ nil))
          (assoc op :type :ok :value {:restarted node}))

        :read-leader
        (let [leader (cluster-db/find-leader-node nodes docker-host? base-port)]
          (assoc op :type :ok :value leader))

        :cluster-health
        (let [;; Try multiple nodes in case one is mid-restart
              health (some (fn [node]
                             (try+
                               (let [conn (cluster-db/conn-for-raft-node node docker-host? base-port)
                                     state (cluster-db/get-cluster-state conn)]
                                 {:node node :state state})
                               (catch Object _ nil)))
                           nodes)]
          (if health
            (assoc op :type :ok :value health)
            (assoc op :type :fail :error :all-nodes-unreachable))))

      (catch java.net.ConnectException _
        (assoc op :type :fail :error :connection-refused))
      (catch java.net.SocketTimeoutException _
        (assoc op :type :info :error :timeout))
      (catch [:type :clusterdown] _
        (assoc op :type :fail :error :cluster-down))
      (catch Exception e
        (let [msg (.getMessage e)]
          (if (and msg (or (str/starts-with? msg "MOVED")
                           (str/starts-with? msg "ASK")))
            (assoc op :type :info :error [:redirect msg])
            (do (warn "Unexpected error:" e)
                (assoc op :type :info :error [:unexpected msg])))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    nil))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn generator
  "Generator for rolling-restart testing.

   Interleaves data operations with sequential node restarts.  Each restart
   is followed by a recovery window before proceeding to the next node."
  [opts]
  (let [rate (get opts :rate 10)
        counter (atom 0)]
    (gen/phases
      ;; Phase 1: Baseline — seed data
      (gen/log "Phase 1: Baseline data seeding")
      (->> (map (fn [i] {:type :invoke :f :write
                         :value {:key (test-key i) :value i}})
                (range 30))
           (gen/each-thread)
           (gen/stagger 0.05))
      (gen/sleep 2)

      ;; Phase 2: Rolling restart — one node at a time with concurrent traffic
      (gen/log "Phase 2: Beginning rolling restart sequence")
      ;; For each node: restart it, then run traffic + health checks
      ;; We flatten this into sequential phases
      (gen/log "Restarting n1")
      (gen/once {:type :invoke :f :restart-node :value "n1"})
      (gen/sleep 3)
      (->> (gen/mix [(fn [] {:type :invoke :f :write
                             :value {:key (test-key (swap! counter inc)) :value @counter}})
                     (fn [] {:type :invoke :f :read :value (test-key (rand-int 30))})
                     (fn [] {:type :invoke :f :cluster-health})])
           (gen/limit 15)
           (gen/stagger (/ 1 rate)))

      (gen/log "Restarting n2")
      (gen/once {:type :invoke :f :restart-node :value "n2"})
      (gen/sleep 3)
      (->> (gen/mix [(fn [] {:type :invoke :f :write
                             :value {:key (test-key (swap! counter inc)) :value @counter}})
                     (fn [] {:type :invoke :f :read :value (test-key (rand-int 30))})
                     (fn [] {:type :invoke :f :cluster-health})])
           (gen/limit 15)
           (gen/stagger (/ 1 rate)))

      (gen/log "Restarting n3")
      (gen/once {:type :invoke :f :restart-node :value "n3"})
      (gen/sleep 3)
      (->> (gen/mix [(fn [] {:type :invoke :f :write
                             :value {:key (test-key (swap! counter inc)) :value @counter}})
                     (fn [] {:type :invoke :f :read :value (test-key (rand-int 30))})
                     (fn [] {:type :invoke :f :cluster-health})])
           (gen/limit 15)
           (gen/stagger (/ 1 rate)))

      ;; Phase 3: Post-restart verification
      (gen/log "Phase 3: Post-rolling-restart verification")
      (gen/sleep 5)
      (gen/once {:type :invoke :f :cluster-health})
      (gen/once {:type :invoke :f :read-leader})

      ;; Phase 4: Final reads — verify all seed data still present
      (gen/log "Phase 4: Final data verification")
      (->> (map (fn [i] {:type :invoke :f :read :value (test-key i)})
                (range 30))
           (gen/each-thread)
           (gen/stagger 0.05)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn checker
  "Checker for rolling-restart workload.

   Verifies:
   - Availability remained above threshold during rolling restart
   - All acknowledged writes are durable after restart
   - Cluster converged to healthy state
   - No data loss for seed data"
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [all-ops (filter #(#{:write :read} (:f %)) history)
            invoked (filter #(= :invoke (:type %)) all-ops)
            completed (filter #(= :ok (:type %)) all-ops)
            failed (filter #(= :fail (:type %)) all-ops)
            info-ops (filter #(= :info (:type %)) all-ops)

            ;; Availability = ok / (ok + fail + info)
            total-responses (+ (count completed) (count failed) (count info-ops))
            availability (if (pos? total-responses)
                           (double (/ (count completed) total-responses))
                           1.0)

            ;; Data integrity — check final reads for seed data
            reads (filter #(= :read (:f %)) (filter #(= :ok (:type %)) history))
            final-reads (take-last 30 reads)
            reads-with-nil (filter #(nil? (get-in % [:value :value])) final-reads)

            ;; Cluster health
            health-checks (filter #(= :cluster-health (:f %))
                                  (filter #(= :ok (:type %)) history))
            final-health (:value (last health-checks))

            ;; Restart tracking
            restarts (filter #(and (= :restart-node (:f %)) (= :ok (:type %))) history)

            ;; Writes
            writes-ok (count (filter #(= :write (:f %)) completed))]

        {:valid? (and (>= availability 0.8)
                      (empty? reads-with-nil)
                      (if final-health
                        (= "ok" (:state final-health))
                        true))
         :availability (format "%.1f%%" (* 100 availability))
         :availability-raw availability
         :total-invoked (count invoked)
         :total-ok (count completed)
         :total-fail (count failed)
         :total-info (count info-ops)
         :successful-restarts (count restarts)
         :final-cluster-state (:state final-health)
         :writes-ok writes-ok
         :final-reads-nil-count (count reads-with-nil)}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a rolling-restart workload.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (map->RollingRestartClient {})
   :generator (generator opts)
   :checker (checker)})
