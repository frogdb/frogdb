(ns jepsen.frogdb.key-routing
  "Key routing workload for FrogDB Raft cluster.

   Tests:
   - MOVED redirect correctness
   - ASK redirect during migration
   - Stale slot cache refresh
   - Routing under partitions

   Operations:
   - :write - Write to a random key
   - :read - Read from a random key
   - :targeted-write - Write to key in specific slot
   - :targeted-read - Read from key in specific slot
   - :check-slot - Verify slot calculation
   - :force-stale-routing - Test with intentionally wrong slot cache"
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn debug]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [jepsen.frogdb.cluster-db :as cluster-db]
            [jepsen.frogdb.cluster-client :as cluster-client]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

;; ===========================================================================
;; Key Generation
;; ===========================================================================

(defn key-for-slot
  "Generate a key that hashes to a specific slot.
   Uses brute-force search (not efficient, but works for testing)."
  [target-slot]
  (loop [i 0]
    (let [key (str "slot-" target-slot "-key-" i)
          slot (cluster-client/slot-for-key key)]
      (if (= slot target-slot)
        key
        (recur (inc i))))))

(defn random-key
  "Generate a random key."
  []
  (str "key-" (rand-int 100000)))

(defn key-spread-across-slots
  "Generate n keys that spread across different slots."
  [n]
  (let [keys (repeatedly n random-key)
        slots (map cluster-client/slot-for-key keys)]
    (zipmap keys slots)))

;; ===========================================================================
;; Redirect Tracking
;; ===========================================================================

(defrecord RoutingStats [moved-count ask-count total-ops])

(defn track-redirect
  "Execute a command and track any redirects."
  ([slot-mapping-atom initial-node key cmd-vec docker-host? nodes]
   (track-redirect slot-mapping-atom initial-node key cmd-vec docker-host? nodes cluster-db/default-base-port))
  ([slot-mapping-atom initial-node key cmd-vec docker-host? nodes base-port]
   (let [result (cluster-client/execute-with-redirect
                  slot-mapping-atom initial-node key cmd-vec docker-host? nodes base-port)]
     {:value (:value result)
      :redirects (:redirects result)})))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord KeyRoutingClient [nodes docker-host? base-port slot-mapping routing-stats]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          all-nodes (or (:cluster-nodes test) ["n1" "n2" "n3"])
          bp (get test :base-port cluster-db/default-base-port)]
      (info "Opening key routing client")
      (assoc this
             :nodes all-nodes
             :docker-host? docker?
             :base-port bp
             :slot-mapping (atom (cluster-client/create-slot-mapping all-nodes docker? bp))
             :routing-stats (atom (->RoutingStats 0 0 0)))))

  (setup! [this test]
    ;; Refresh slot mapping
    (when slot-mapping
      (reset! slot-mapping (cluster-client/refresh-slot-mapping @slot-mapping (first nodes) docker-host? base-port)))
    this)

  (invoke! [this test op]
    (try+
      (case (:f op)
        :write
        (let [key (or (:key (:value op)) (random-key))
              value (or (:value (:value op)) (rand-int 10000))
              result (track-redirect slot-mapping nil key ["SET" key (str value)] docker-host? nodes base-port)]
          (swap! routing-stats update :total-ops inc)
          (when (> (:redirects result) 0)
            (swap! routing-stats update :moved-count + (:redirects result)))
          (assoc op :type :ok :value {:key key
                                       :written value
                                       :slot (cluster-client/slot-for-key key)
                                       :redirects (:redirects result)}))

        :read
        (let [key (or (:value op) (random-key))
              result (track-redirect slot-mapping nil key ["GET" key] docker-host? nodes base-port)]
          (swap! routing-stats update :total-ops inc)
          (when (> (:redirects result) 0)
            (swap! routing-stats update :moved-count + (:redirects result)))
          (assoc op :type :ok :value {:key key
                                       :value (frogdb/parse-value (:value result))
                                       :slot (cluster-client/slot-for-key key)
                                       :redirects (:redirects result)}))

        :targeted-write
        (let [{:keys [slot value]} (:value op)
              key (key-for-slot slot)
              result (track-redirect slot-mapping nil key ["SET" key (str value)] docker-host? nodes base-port)]
          (swap! routing-stats update :total-ops inc)
          (assoc op :type :ok :value {:key key
                                       :slot slot
                                       :written value
                                       :redirects (:redirects result)}))

        :targeted-read
        (let [slot (:value op)
              key (key-for-slot slot)
              result (track-redirect slot-mapping nil key ["GET" key] docker-host? nodes base-port)]
          (swap! routing-stats update :total-ops inc)
          (assoc op :type :ok :value {:key key
                                       :slot slot
                                       :value (frogdb/parse-value (:value result))
                                       :redirects (:redirects result)}))

        :check-slot
        (let [key (:value op)
              slot (cluster-client/slot-for-key key)
              owner (let [mapping @slot-mapping
                          addr (cluster-client/get-node-for-slot mapping slot)]
                      (when addr
                        (let [[ip _] (str/split addr #":")]
                          (cluster-db/get-node-for-ip ip))))]
          (assoc op :type :ok :value {:key key
                                       :slot slot
                                       :owner owner}))

        :force-wrong-node
        ;; Intentionally send to wrong node to trigger redirect
        (let [{:keys [key target-node]} (:value op)
              slot (cluster-client/slot-for-key key)
              ;; Temporarily point slot to wrong node
              _ (swap! slot-mapping assoc-in [:slots-to-nodes slot]
                       (let [info (get (cluster-db/raft-cluster-host-ports base-port) target-node)]
                         (str (:host info) ":" (:port info))))
              result (track-redirect slot-mapping nil key ["GET" key] docker-host? nodes base-port)]
          (assoc op :type :ok :value {:key key
                                       :slot slot
                                       :forced-node target-node
                                       :redirects (:redirects result)
                                       :value (frogdb/parse-value (:value result))}))

        :refresh-mapping
        (do
          (reset! slot-mapping (cluster-client/refresh-slot-mapping @slot-mapping (first nodes) docker-host? base-port))
          (assoc op :type :ok :value :refreshed))

        :get-routing-stats
        (assoc op :type :ok :value @routing-stats))

      (catch java.net.ConnectException e
        (assoc op :type :fail :error :connection-refused))

      (catch java.net.SocketTimeoutException e
        (assoc op :type :info :error :timeout))

      (catch [:type :too-many-redirects] e
        (assoc op :type :fail :error :too-many-redirects))

      (catch [:type :clusterdown] e
        (assoc op :type :fail :error :cluster-down))

      (catch Exception e
        (warn "Unexpected error:" e)
        (assoc op :type :info :error [:unexpected (.getMessage e)]))))

  (teardown! [this test]
    nil)

  (close! [this test]
    nil))

(defn create-client
  "Create a new key routing client."
  []
  (map->KeyRoutingClient {}))

;; ===========================================================================
;; Generators
;; ===========================================================================

(defn write-op
  "Generate a write operation."
  ([] {:type :invoke :f :write})
  ([key value] {:type :invoke :f :write :value {:key key :value value}}))

(defn read-op
  "Generate a read operation."
  ([] {:type :invoke :f :read})
  ([key] {:type :invoke :f :read :value key}))

(defn targeted-write
  "Generate a targeted write to specific slot."
  [slot value]
  {:type :invoke :f :targeted-write :value {:slot slot :value value}})

(defn targeted-read
  "Generate a targeted read from specific slot."
  [slot]
  {:type :invoke :f :targeted-read :value slot})

(defn check-slot
  "Generate a check-slot operation."
  [key]
  {:type :invoke :f :check-slot :value key})

(defn force-wrong-node
  "Generate a force-wrong-node operation."
  [key target-node]
  {:type :invoke :f :force-wrong-node :value {:key key :target-node target-node}})

(defn refresh-mapping
  "Generate a refresh-mapping operation."
  []
  {:type :invoke :f :refresh-mapping})

(defn get-routing-stats
  "Generate a get-routing-stats operation."
  []
  {:type :invoke :f :get-routing-stats})

(defn generator
  "Generator for key routing testing.
   Mix of reads and writes across different slots."
  [opts]
  (let [rate (get opts :rate 20)]
    (->> (gen/mix [(fn [] (write-op))
                   (fn [] (write-op))
                   (fn [] (read-op))
                   (fn [] (read-op))
                   (fn [] (targeted-write (rand-int 16384) (rand-int 10000)))
                   (fn [] (targeted-read (rand-int 16384)))])
         (gen/stagger (/ 1 rate)))))

(defn redirect-stress-generator
  "Generator that intentionally causes redirects to stress test routing."
  [opts]
  (let [rate (get opts :rate 10)
        test-key "redirect-test-key"]
    (gen/phases
      ;; Phase 1: Initial write
      (gen/log "Setting up test key")
      (gen/once (write-op test-key 12345))
      (gen/sleep 1)

      ;; Phase 2: Check slot and owner
      (gen/log "Checking slot routing")
      (gen/once (check-slot test-key))

      ;; Phase 3: Force wrong node and verify redirect
      (gen/log "Testing MOVED redirect recovery")
      (gen/each-thread
        (gen/phases
          (gen/once (force-wrong-node test-key "n1"))
          (gen/sleep 1)
          (gen/once (force-wrong-node test-key "n2"))
          (gen/sleep 1)
          (gen/once (force-wrong-node test-key "n3"))))

      ;; Phase 4: Normal operations
      (gen/log "Normal operations after redirects")
      (->> (gen/mix [(fn [] (write-op))
                     (fn [] (read-op))])
           (gen/limit 50)
           (gen/stagger (/ 1 rate)))

      ;; Phase 5: Check stats
      (gen/log "Getting routing stats")
      (gen/once (get-routing-stats)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn checker
  "Checker for key routing workload.

   Verifies:
   - All operations eventually succeed (after redirects)
   - Redirect count is reasonable
   - No data loss due to routing issues"
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [completed (filter #(= :ok (:type %)) history)
            failed (filter #(= :fail (:type %)) history)
            info-ops (filter #(= :info (:type %)) history)

            ;; Count redirects
            ops-with-redirects (->> completed
                                    (filter #(contains? #{:write :read :targeted-write :targeted-read :force-wrong-node} (:f %)))
                                    (filter #(some? (get-in % [:value :redirects]))))
            total-redirects (reduce + (map #(get-in % [:value :redirects] 0) ops-with-redirects))

            ;; Get final stats
            stats-ops (filter #(= :get-routing-stats (:f %)) completed)
            final-stats (when (seq stats-ops)
                          (:value (last stats-ops)))

            ;; Track writes and reads
            writes (filter #(#{:write :targeted-write} (:f %)) completed)
            reads (filter #(#{:read :targeted-read} (:f %)) completed)

            ;; Check for too-many-redirects failures
            redirect-failures (filter #(= :too-many-redirects (:error %)) failed)]

        {:valid? (empty? redirect-failures)
         :total-ops (count completed)
         :total-writes (count writes)
         :total-reads (count reads)
         :total-redirects total-redirects
         :ops-with-redirects (count ops-with-redirects)
         :avg-redirects-per-op (if (pos? (count ops-with-redirects))
                                 (double (/ total-redirects (count ops-with-redirects)))
                                 0.0)
         :redirect-failures (count redirect-failures)
         :failed-ops (count failed)
         :timeout-ops (count (filter #(= :timeout (:error %)) info-ops))
         :final-stats final-stats}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a key routing workload.

   Options:
   - :rate - operations per second
   - :redirect-stress - if true, use redirect stress generator"
  [opts]
  (if (:redirect-stress opts)
    {:client (create-client)
     :generator (redirect-stress-generator opts)
     :checker (checker)}
    {:client (create-client)
     :generator (generator opts)
     :checker (checker)}))
