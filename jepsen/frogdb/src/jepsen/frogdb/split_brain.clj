(ns jepsen.frogdb.split-brain
  "Split-brain detection workload for FrogDB.

   Tests for split-brain scenarios where multiple nodes might accept
   writes as primary. This can occur during network partitions when
   a replica is promoted but the old primary is still reachable by
   some clients.

   Verifies:
   - Only one node reports role 'master' at a time
   - Writes to non-primary should fail with READONLY error
   - No divergent values after partition heals

   Operations:
   - :write-node - Attempt write to specific node
   - :check-role - ROLE command to check master/slave
   - :read-all - Read same key from all nodes"
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn debug]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def test-key "jepsen-split")

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord SplitBrainClient [conns docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          nodes (or (:nodes test) ["n1" "n2" "n3"])
          all-conns (frogdb/all-node-conns nodes docker?)]
      (info "Opening split-brain client (docker?:" docker? ", nodes:" nodes ")")
      (assoc this
             :conns all-conns
             :docker-host? docker?)))

  (setup! [this test]
    ;; Initialize the test key on primary
    (let [primary-conn (get conns "n1")]
      (info "Setting up split-brain test key" test-key)
      (wcar primary-conn (car/set test-key "0"))
      (Thread/sleep 500))
    this)

  (invoke! [this test op]
    (case (:f op)
      ;; Write to a specific node
      :write-node
      (let [{:keys [node value]} (:value op)
            conn (get conns node)]
        (if-not conn
          (assoc op :type :fail :error :unknown-node)
          (try+
            (wcar conn (car/set test-key (str value)))
            (assoc op :type :ok :value {:node node :value value})
            (catch java.net.ConnectException e
              (assoc op :type :fail :error :connection-refused))
            (catch clojure.lang.ExceptionInfo e
              (let [data (ex-data e)]
                (if (and (= (:prefix data) :carmine)
                         (str/includes? (str (:message data)) "READONLY"))
                  (assoc op :type :ok :value {:node node :readonly true})
                  (assoc op :type :fail :error [:redis-error (:message data)]))))
            (catch Exception e
              (assoc op :type :info :error [:unexpected (.getMessage e)])))))

      ;; Check role of a specific node
      :check-role
      (let [node (:value op)
            conn (get conns node)]
        (if-not conn
          (assoc op :type :fail :error :unknown-node)
          (try+
            (let [role-info (frogdb/role conn)
                  role (first role-info)]
              (assoc op :type :ok :value {:node node :role role :info role-info}))
            (catch java.net.ConnectException e
              (assoc op :type :fail :error :connection-refused))
            (catch Exception e
              (assoc op :type :info :error [:unexpected (.getMessage e)])))))

      ;; Read from all nodes
      :read-all
      (let [results (for [[node conn] conns]
                      (try+
                        [node {:value (frogdb/read-register conn test-key)}]
                        (catch java.net.ConnectException e
                          [node {:error :connection-refused}])
                        (catch Exception e
                          [node {:error [:unexpected (.getMessage e)]}])))]
        (assoc op :type :ok :value (into {} results)))

      ;; Generic read (used by final-reads phase) — delegates to read-all
      :read
      (let [results (for [[node conn] conns]
                      (try+
                        [node {:value (frogdb/read-register conn test-key)}]
                        (catch java.net.ConnectException e
                          [node {:error :connection-refused}])
                        (catch Exception e
                          [node {:error [:unexpected (.getMessage e)]}])))]
        (assoc op :type :ok :value (into {} results)))))

  (teardown! [this test]
    nil)

  (close! [this test]
    nil))

(defn create-client
  "Create a new split-brain client."
  []
  (map->SplitBrainClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn write-node-op [node value]
  {:type :invoke :f :write-node :value {:node node :value value}})

(defn check-role-op [node]
  {:type :invoke :f :check-role :value node})

(defn read-all-op []
  {:type :invoke :f :read-all :value nil})

(defn generator
  "Generator for split-brain workload.
   Writes to different nodes and checks roles/values."
  [opts]
  (let [rate (get opts :rate 10)
        counter (atom 0)
        nodes ["n1" "n2" "n3"]]
    (->> (gen/mix [;; Write to primary
                   (fn [] (write-node-op "n1" (swap! counter inc)))
                   ;; Try write to replica (should fail with READONLY)
                   (fn [] (write-node-op (rand-nth ["n2" "n3"]) (swap! counter inc)))
                   ;; Check roles
                   (fn [] (check-role-op (rand-nth nodes)))
                   ;; Read from all
                   (fn [] (read-all-op))])
         (gen/stagger (/ 1 rate)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn extract-role-checks
  "Extract all successful role check operations."
  [history]
  (->> history
       (filter #(and (= :check-role (:f %))
                     (= :ok (:type %))))
       (map :value)))

(defn extract-writes
  "Extract all successful write operations (including READONLY responses)."
  [history]
  (->> history
       (filter #(and (= :write-node (:f %))
                     (= :ok (:type %))))
       (map :value)))

(defn extract-read-all
  "Extract all successful read-all operations (including :read which delegates to read-all)."
  [history]
  (->> history
       (filter #(and (#{:read-all :read} (:f %))
                     (= :ok (:type %))))
       (map :value)))

(defn checker
  "Checker for split-brain workload.

   Verifies:
   - At most one master at any time (ideally exactly one)
   - Writes to replicas are rejected (READONLY)
   - Final values are consistent after healing"
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [role-checks (extract-role-checks history)
            writes (extract-writes history)
            read-alls (extract-read-all history)
            final-read-all (last read-alls)

            ;; Group role checks by time to detect multiple masters
            ;; For simplicity, we just check if any replica ever claimed to be master
            masters (filter #(= "master" (:role %)) role-checks)
            master-nodes (distinct (map :node masters))

            ;; Check for writes that succeeded to non-primary
            non-primary-writes (filter #(and (not= "n1" (:node %))
                                             (not (:readonly %)))
                                       writes)

            ;; Check readonly rejections
            readonly-rejections (filter :readonly writes)

            ;; Final consistency
            final-values (when final-read-all
                           (map :value (filter #(:value %) (vals final-read-all))))
            consistent? (when (seq final-values)
                          (apply = final-values))]

        {:valid? (and
                   ;; Only n1 should be master (or multiple masters is detected as error)
                   (or (= ["n1"] master-nodes)
                       (empty? master-nodes))
                   ;; No non-primary writes should succeed (except READONLY)
                   (empty? non-primary-writes)
                   ;; Final state should be consistent
                   (or (nil? final-values) consistent?))
         :master-nodes master-nodes
         :multiple-masters? (> (count master-nodes) 1)
         :non-primary-writes (count non-primary-writes)
         :readonly-rejections (count readonly-rejections)
         :consistent? consistent?
         :final-values final-values}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a split-brain workload.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (generator opts)
   :checker (checker)})
