(ns jepsen.frogdb.lag
  "Replication lag measurement workload for FrogDB.

   Measures and validates replication latency between primary and replicas.
   This workload is primarily for observability and performance validation,
   not strict consistency testing.

   Operations:
   - :measure-lag - Write unique value, poll replica until visible
   - :burst-write - Write N values rapidly
   - :repl-info - Get INFO REPLICATION stats
   - :repl-offsets - Sample the replica's and the primary's replication offsets

   Metrics:
   - min/max/avg/p99 replication lag
   - Number of timeouts (writes that didn't replicate in time)

   ## Two topologies

   The base workload (`lag`) measures the standalone primary + replicas topology.
   `cluster-workload` (`cluster-lag`) measures the *same* thing in cluster mode:
   a cluster replica attaches over PSYNC exactly as a standalone one does (Raft
   carries metadata only, ADR-0001), so replication offsets and lag must behave
   the same way. It is the observability half of the cluster WAIT contract — a
   `connected_slaves` and a `master_repl_offset` that never move would make WAIT
   a counter wired to nothing — so its checker additionally asserts that the
   offsets are non-zero, monotone, and never show the replica ahead of its
   primary."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn debug]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [jepsen.frogdb.cluster-db :as cluster-db]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def test-key-prefix "jepsen-lag-")
(def poll-interval-ms 10)
(def max-poll-time-ms 5000)

(def cluster-key-tag
  "Hash tag that pins every cluster-mode lag key onto one slot, so all of them
   are owned by the shard whose replication link is being measured."
  "{jepsen-cluster-lag}")

(def cluster-test-key-prefix (str cluster-key-tag ":lag:"))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord LagClient [conns primary-conn replica-conns docker-host? key-counter]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)
          nodes (or (:nodes test) ["n1" "n2" "n3"])
          all-conns (frogdb/all-node-conns-single nodes docker? base-port)]
      (info "Opening lag client (docker?:" docker? ", nodes:" nodes ")")
      (assoc this
             :conns all-conns
             :primary-conn (get all-conns "n1")
             :replica-conns [(get all-conns "n2") (get all-conns "n3")]
             :docker-host? docker?
             :key-counter (atom 0))))

  (setup! [this test]
    this)

  (invoke! [this test op]
    (case (:f op)
      ;; Measure lag: write to primary, poll replica until visible
      :measure-lag
      (let [key-id (swap! key-counter inc)
            key-name (str test-key-prefix key-id)
            value (str "v" key-id)
            replica-conn (rand-nth replica-conns)
            start-time (System/currentTimeMillis)]
        (try+
          ;; Write to primary
          (wcar primary-conn (car/set key-name value))

          ;; Poll replica until value appears
          (loop [elapsed 0]
            (let [read-value (frogdb/read-register replica-conn key-name)]
              (cond
                ;; Value is visible on replica
                (= value (str read-value))
                (let [lag-ms (- (System/currentTimeMillis) start-time)]
                  (assoc op :type :ok :lag-ms lag-ms :value value))

                ;; Timeout
                (> elapsed max-poll-time-ms)
                (assoc op :type :ok :lag-ms :timeout :value value)

                ;; Keep polling
                :else
                (do
                  (Thread/sleep poll-interval-ms)
                  (recur (+ elapsed poll-interval-ms))))))

          (catch java.net.ConnectException e
            (assoc op :type :fail :error :connection-refused))
          (catch Exception e
            (assoc op :type :info :error [:unexpected (.getMessage e)]))))

      ;; Burst write: write N values rapidly
      :burst-write
      (let [n (or (:value op) 10)
            start-time (System/currentTimeMillis)
            results (doall
                      (for [i (range n)]
                        (let [key-id (swap! key-counter inc)
                              key-name (str test-key-prefix key-id)]
                          (try+
                            (wcar primary-conn (car/set key-name (str "b" key-id)))
                            {:ok true}
                            (catch Exception e
                              {:ok false :error (.getMessage e)})))))]
        (let [elapsed (- (System/currentTimeMillis) start-time)
              successes (count (filter :ok results))]
          (assoc op :type :ok :burst-count n :successes successes :elapsed-ms elapsed)))

      ;; Get replication info
      :repl-info
      (let [info (try+
                   (frogdb/info-replication primary-conn)
                   (catch Exception e
                     {:error (.getMessage e)}))]
        (assoc op :type :ok :value info))

      ;; Generic read (used by final-reads phase)
      :read
      (let [results (for [[node conn] conns]
                      [node (try+ (frogdb/read-register conn test-key-prefix)
                                  (catch Exception _ nil))])]
        (assoc op :type :ok :value (into {} results)))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (doseq [[_ c] conns] (frogdb/close-conn! c))))

(defn create-client
  "Create a new lag client."
  []
  (map->LagClient {}))

;; ===========================================================================
;; Cluster-mode Client
;; ===========================================================================

;; Namespace-level so every worker measures the same shard; rebuilt per test
;; (batch runs share a JVM). See `cluster-replication` for the same pattern.
(defonce cluster-shard (atom nil))

(def cluster-probe-key (str cluster-key-tag ":probe"))

(defrecord ClusterLagClient [conns nodes docker-host? base-port key-counter]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          bp (get test :base-port cluster-db/default-base-port)
          all-nodes (or (:cluster-nodes test) (:nodes test) ["n1" "n2" "n3"])]
      (info "Opening cluster lag client (nodes:" all-nodes ")")
      (assoc this
             :conns (cluster-db/all-node-conns all-nodes docker? bp)
             :nodes all-nodes
             :docker-host? docker?
             :base-port bp
             :key-counter (atom 0))))

  (setup! [this test]
    (let [ref-conn (get conns (first nodes))]
      (cluster-db/wait-for-cluster-ready ref-conn)
      (locking cluster-shard
        (let [test-id (str (:start-time test))]
          (when (not= test-id (:test-id @cluster-shard))
            (reset! cluster-shard nil))
          (let [s (cluster-db/ensure-shard-replica! cluster-shard ref-conn conns nodes
                                                    docker-host? base-port
                                                    cluster-probe-key)]
            (swap! cluster-shard assoc :test-id test-id)
            (info "cluster-lag: measuring" (:replica s) "->" (:primary s)
                  "attached?" (:attached? s))))))
    this)

  (invoke! [this test op]
    (let [{:keys [primary replica]} @cluster-shard
          primary-conn (get conns primary)
          replica-conn (get conns replica)]
      (case (:f op)
        ;; Write on the shard primary, poll the shard replica until the value
        ;; lands. Replica reads go through READONLY: without it the replica
        ;; answers MOVED for its primary's slots.
        :measure-lag
        (let [key-id (swap! key-counter inc)
              key-name (str cluster-test-key-prefix key-id)
              value (str "v" key-id)
              start-time (System/currentTimeMillis)]
          (try+
            (wcar primary-conn (car/set key-name value))
            (loop [elapsed 0]
              (let [read-value (cluster-db/replica-get replica-conn key-name)]
                (cond
                  (= value (str read-value))
                  (assoc op :type :ok
                         :lag-ms (- (System/currentTimeMillis) start-time)
                         :value value)

                  (> elapsed max-poll-time-ms)
                  (assoc op :type :ok :lag-ms :timeout :value value)

                  :else
                  (do (Thread/sleep poll-interval-ms)
                      (recur (+ elapsed poll-interval-ms))))))
            (catch java.net.ConnectException _
              (assoc op :type :fail :error :connection-refused))
            (catch Exception e
              (assoc op :type :info :error [:unexpected (.getMessage e)]))))

        :burst-write
        (let [n (or (:value op) 10)
              start-time (System/currentTimeMillis)
              results (doall
                        (for [_ (range n)]
                          (let [key-id (swap! key-counter inc)]
                            (try+
                              (wcar primary-conn
                                (car/set (str cluster-test-key-prefix key-id)
                                         (str "b" key-id)))
                              {:ok true}
                              (catch Exception e
                                {:ok false :error (.getMessage e)})))))]
          (assoc op :type :ok
                 :burst-count n
                 :successes (count (filter :ok results))
                 :elapsed-ms (- (System/currentTimeMillis) start-time)))

        ;; Offsets, replica first: both offsets only ever grow, so sampling the
        ;; replica before the primary means a concurrent write can never make the
        ;; sample look like the replica overtook its primary. The reverse order
        ;; would produce that artifact and the checker would call it a violation.
        :repl-offsets
        (let [replica-offset (try+ (frogdb/get-replication-offset replica-conn)
                                   (catch Object _ nil))
              primary-offset (try+ (frogdb/get-replication-offset primary-conn)
                                   (catch Object _ nil))
              connected (try+ (frogdb/get-connected-replicas primary-conn)
                              (catch Object _ nil))]
          (assoc op :type :ok
                 :value {:primary primary
                         :replica replica
                         :primary-offset primary-offset
                         :replica-offset replica-offset
                         :connected-slaves connected}))

        :repl-info
        (assoc op :type :ok
               :value (try+ (frogdb/info-replication primary-conn)
                            (catch Exception e {:error (.getMessage e)})))

        ;; Generic read (final-reads phase): the last key written, off both ends
        ;; of the link.
        :read
        (let [key-name (str cluster-test-key-prefix @key-counter)]
          (assoc op :type :ok
                 :value {:key key-name
                         :primary (try+ (frogdb/read-register primary-conn key-name)
                                        (catch Object _ nil))
                         :replica (try+ (cluster-db/replica-get replica-conn key-name)
                                        (catch Object _ nil))})))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (doseq [[_ c] conns] (frogdb/close-conn! c))))

(defn create-cluster-client
  "Create a new cluster-mode lag client."
  []
  (map->ClusterLagClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn measure-lag-op []
  {:type :invoke :f :measure-lag :value nil})

(defn burst-write-op [n]
  {:type :invoke :f :burst-write :value n})

(defn repl-info-op []
  {:type :invoke :f :repl-info :value nil})

(defn repl-offsets-op []
  {:type :invoke :f :repl-offsets :value nil})

(defn generator
  "Generator for lag workload.
   Primarily measures lag with occasional burst writes."
  [opts]
  (let [rate (get opts :rate 5)]  ; Lower rate for lag measurement
    (->> (gen/mix [(fn [] (measure-lag-op))
                   (fn [] (measure-lag-op))
                   (fn [] (measure-lag-op))
                   (fn [] (burst-write-op 10))
                   (fn [] (repl-info-op))])
         (gen/stagger (/ 1 rate)))))

(defn cluster-generator
  "Generator for the cluster-mode lag workload: the same lag/burst mix plus
   offset samples, which are what prove the cluster link is a real replication
   stream rather than a counter that never moves."
  [opts]
  (let [rate (get opts :rate 5)]
    (->> (gen/mix [(fn [] (measure-lag-op))
                   (fn [] (measure-lag-op))
                   (fn [] (measure-lag-op))
                   (fn [] (burst-write-op 10))
                   (fn [] (repl-offsets-op))
                   (fn [] (repl-offsets-op))])
         (gen/stagger (/ 1 rate)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn extract-lag-measurements
  "Extract all successful lag measurement operations."
  [history]
  (->> history
       (filter #(and (= :measure-lag (:f %))
                     (= :ok (:type %))
                     (number? (:lag-ms %))))
       (map :lag-ms)))

(defn extract-timeouts
  "Extract all lag measurements that timed out."
  [history]
  (->> history
       (filter #(and (= :measure-lag (:f %))
                     (= :ok (:type %))
                     (= :timeout (:lag-ms %))))
       count))

(defn percentile
  "Calculate the nth percentile of a sorted sequence."
  [sorted-seq n]
  (let [count (count sorted-seq)
        idx (int (* (/ n 100) (dec count)))]
    (nth sorted-seq (min idx (dec count)))))

(defn extract-offset-samples
  "Successful `:repl-offsets` samples, in history order."
  [history]
  (->> history
       (filter #(and (= :repl-offsets (:f %)) (= :ok (:type %))))
       (map :value)))

(defn- monotone?
  "True when a sequence of offsets never decreases (nils are skipped: an
   unreadable sample is missing data, not a regression)."
  [xs]
  (let [vs (keep identity xs)]
    (or (empty? vs) (apply <= vs))))

(defn offset-report
  "Summarise the offset samples: are they moving, monotone, and consistent?

   * `:offsets-advanced?` — the primary's offset grew over the run. A cluster
     primary whose offset never moves is replicating nothing.
   * `:offsets-monotone?` — neither end's offset ever went backwards.
   * `:replica-never-ahead?` — no sample showed the replica past its primary.
     Sound because the client samples the replica first (see the `:repl-offsets`
     op), so a concurrent write can only widen the gap, never invert it.
   * `:connected-slaves-seen?` — the primary counted its replica at least once,
     which is the same number WAIT counts."
  [samples]
  (let [primary-offsets (map :primary-offset samples)
        replica-offsets (map :replica-offset samples)
        readable (filter #(and (:primary-offset %) (:replica-offset %)) samples)
        known-primary (keep identity primary-offsets)]
    {:offset-samples (count samples)
     :offsets-advanced? (boolean (and (seq known-primary)
                                      (pos? (last known-primary))
                                      (> (last known-primary) (first known-primary))))
     :offsets-monotone? (and (monotone? primary-offsets) (monotone? replica-offsets))
     :replica-never-ahead? (every? #(<= (:replica-offset %) (:primary-offset %)) readable)
     :connected-slaves-seen? (boolean (some #(and (:connected-slaves %)
                                                  (pos? (:connected-slaves %)))
                                            samples))
     :first-primary-offset (first known-primary)
     :last-primary-offset (last known-primary)
     :last-replica-offset (last (keep identity replica-offsets))}))

(defn checker
  "Checker for lag workload.

   Reports:
   - min/max/avg/p50/p99 replication lag
   - Number of timeouts
   - Burst write success rate

   With `{:cluster? true}` the offset samples become assertions rather than
   observations: a cluster-mode replication link must advance the primary's
   offset, keep both offsets monotone, never show the replica ahead of its
   primary, and be counted in `connected_slaves` (the number WAIT counts). That
   is what distinguishes a real cluster replication stream from the pre-rework
   behaviour, where a cluster node reported zeroes forever."
  ([] (checker {}))
  ([copts]
   (let [cluster? (boolean (:cluster? copts))]
     (reify checker/Checker
       (check [_ test history opts]
         (let [lag-ms (extract-lag-measurements history)
               timeouts (extract-timeouts history)
               sorted-lag (sort lag-ms)
               burst-ops (->> history
                              (filter #(and (= :burst-write (:f %))
                                            (= :ok (:type %)))))
               offsets (when cluster? (offset-report (extract-offset-samples history)))]

           (if (empty? lag-ms)
             {:valid? :unknown
              :error "No lag measurements"}
             (let [total-bursts (reduce + (map :burst-count burst-ops))
                   total-burst-success (reduce + (map :successes burst-ops))]
               (cond-> {;; Standalone: a measurement workload, always "valid".
                        :valid? (if cluster?
                                  (and (:offsets-advanced? offsets)
                                       (:offsets-monotone? offsets)
                                       (:replica-never-ahead? offsets)
                                       (:connected-slaves-seen? offsets))
                                  true)
                        :measurements (count lag-ms)
                        :timeouts timeouts
                        :timeout-rate (if (pos? (+ (count lag-ms) timeouts))
                                        (double (/ timeouts (+ (count lag-ms) timeouts)))
                                        0)
                        :lag-min-ms (first sorted-lag)
                        :lag-max-ms (last sorted-lag)
                        :lag-avg-ms (double (/ (reduce + lag-ms) (count lag-ms)))
                        :lag-p50-ms (percentile sorted-lag 50)
                        :lag-p99-ms (percentile sorted-lag 99)
                        :burst-ops (count burst-ops)
                        :burst-writes total-bursts
                        :burst-success-rate (if (pos? total-bursts)
                                              (double (/ total-burst-success total-bursts))
                                              1.0)}
                 cluster? (merge offsets))))))))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a lag measurement workload.

   Options:
   - :rate - operations per second (default 5, lower than other workloads)"
  [opts]
  {:client (create-client)
   :generator (generator (merge {:rate 5} opts))
   :checker (checker)})

(defn cluster-workload
  "Construct a cluster-mode lag measurement workload.

   The workload attaches a replica to one shard's primary itself (a freshly
   formed cluster has none), then measures the same write-to-visible lag plus
   replication offsets. Unlike the standalone workload this one can fail: the
   offsets are assertions about the cluster replication stream existing at all.

   Options:
   - :rate - operations per second (default 5)"
  [opts]
  {:client (create-cluster-client)
   :generator (cluster-generator (merge {:rate 5} opts))
   :final-generator (gen/once {:type :invoke :f :read :value nil})
   :checker (checker {:cluster? true})})
