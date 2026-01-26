(ns jepsen.frogdb.lag
  "Replication lag measurement workload for FrogDB.

   Measures and validates replication latency between primary and replicas.
   This workload is primarily for observability and performance validation,
   not strict consistency testing.

   Operations:
   - :measure-lag - Write unique value, poll replica until visible
   - :burst-write - Write N values rapidly
   - :repl-info - Get INFO REPLICATION stats

   Metrics:
   - min/max/avg/p99 replication lag
   - Number of timeouts (writes that didn't replicate in time)"
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn debug]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def test-key-prefix "jepsen-lag-")
(def poll-interval-ms 10)
(def max-poll-time-ms 5000)

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord LagClient [conns primary-conn replica-conns docker-host? key-counter]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          nodes (or (:nodes test) ["n1" "n2" "n3"])
          all-conns (frogdb/all-node-conns nodes docker?)
          primary (frogdb/conn-for-node "n1" docker?)
          replicas [(frogdb/conn-for-node "n2" docker?)
                    (frogdb/conn-for-node "n3" docker?)]]
      (info "Opening lag client (docker?:" docker? ", nodes:" nodes ")")
      (assoc this
             :conns all-conns
             :primary-conn primary
             :replica-conns replicas
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
        (assoc op :type :ok :value info))))

  (teardown! [this test]
    nil)

  (close! [this test]
    nil))

(defn create-client
  "Create a new lag client."
  []
  (map->LagClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn measure-lag-op []
  {:type :invoke :f :measure-lag :value nil})

(defn burst-write-op [n]
  {:type :invoke :f :burst-write :value n})

(defn repl-info-op []
  {:type :invoke :f :repl-info :value nil})

(defn generator
  "Generator for lag workload.
   Primarily measures lag with occasional burst writes."
  [opts]
  (let [rate (get opts :rate 5)]  ; Lower rate for lag measurement
    (->> (gen/mix [(fn [_] (measure-lag-op))
                   (fn [_] (measure-lag-op))
                   (fn [_] (measure-lag-op))
                   (fn [_] (burst-write-op 10))
                   (fn [_] (repl-info-op))])
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

(defn checker
  "Checker for lag workload.

   Reports:
   - min/max/avg/p50/p99 replication lag
   - Number of timeouts
   - Burst write success rate"
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [lag-ms (extract-lag-measurements history)
            timeouts (extract-timeouts history)
            sorted-lag (sort lag-ms)
            burst-ops (->> history
                           (filter #(and (= :burst-write (:f %))
                                         (= :ok (:type %)))))]

        (if (empty? lag-ms)
          {:valid? :unknown
           :error "No lag measurements"}
          (let [total-bursts (reduce + (map :burst-count burst-ops))
                total-burst-success (reduce + (map :successes burst-ops))]
            {:valid? true  ; This is a measurement workload, always "valid"
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
                                   1.0)}))))))

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
