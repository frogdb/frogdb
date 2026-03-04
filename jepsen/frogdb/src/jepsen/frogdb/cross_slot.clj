(ns jepsen.frogdb.cross-slot
  "Cross-slot transaction workload for FrogDB Raft cluster.

   Tests:
   - Hash tag operations ({foo}:a, {foo}:b)
   - MULTI/EXEC atomicity for same-slot keys
   - CROSSSLOT error for different slots
   - Multi-key operations with hash tags

   Operations:
   - :hash-tag-write - Write multiple keys with same hash tag
   - :hash-tag-read - Read multiple keys with same hash tag
   - :cross-slot-write - Attempt write across different slots (should fail)
   - :atomic-transfer - Atomic transfer between same-slot keys"
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
;; Hash Tag Key Generation
;; ===========================================================================

(defn make-tagged-keys
  "Generate a set of keys with the same hash tag.
   All keys will route to the same slot."
  [tag suffixes]
  (mapv #(str "{" tag "}:" %) suffixes))

(defn make-unrelated-keys
  "Generate keys without hash tags that may route to different slots."
  [prefixes]
  (mapv #(str "key-" %) prefixes))

(defn keys-same-slot?
  "Check if all keys hash to the same slot."
  [keys]
  (let [slots (map cluster-client/slot-for-key keys)]
    (apply = slots)))

;; ===========================================================================
;; Multi/Exec Operations
;; ===========================================================================

(defn execute-multi-set!
  "Execute MULTI/EXEC to set multiple keys atomically.
   All keys must be in the same slot."
  [conn keys-values]
  (wcar conn
    (car/multi)
    (doseq [[k v] keys-values]
      (car/set k (str v)))
    (car/exec)))

(defn execute-multi-get
  "Execute MULTI/EXEC to get multiple keys atomically."
  [conn keys]
  (wcar conn
    (car/multi)
    (doseq [k keys]
      (car/get k))
    (let [results (car/exec)]
      (zipmap keys (map frogdb/parse-value results)))))

(defn execute-atomic-transfer!
  "Atomically transfer amount from one key to another.
   Both keys must be in the same slot."
  [conn from-key to-key amount]
  (wcar conn
    (car/watch from-key to-key)
    (let [from-val (or (frogdb/parse-value (car/get from-key)) 0)
          to-val (or (frogdb/parse-value (car/get to-key)) 0)]
      (if (>= from-val amount)
        (do
          (car/multi)
          (car/set from-key (str (- from-val amount)))
          (car/set to-key (str (+ to-val amount)))
          (let [result (car/exec)]
            (boolean result)))
        (do
          (car/unwatch)
          false)))))

;; ===========================================================================
;; Cluster-Aware Multi Operations
;; ===========================================================================

(defn find-slot-owner-conn
  "Find the node that owns a slot and return its connection."
  [nodes docker-host? slot slot-mapping-atom]
  (let [mapping @slot-mapping-atom
        addr (cluster-client/get-node-for-slot mapping slot)]
    (if addr
      (let [[host port-str] (str/split addr #":")
            port (Integer/parseInt port-str)]
        (cluster-client/make-conn host port))
      ;; Fallback: refresh and try again
      (let [new-mapping (cluster-client/refresh-slot-mapping mapping (first nodes) docker-host?)]
        (reset! slot-mapping-atom new-mapping)
        (let [addr (cluster-client/get-node-for-slot new-mapping slot)
              [host port-str] (str/split addr #":")
              port (Integer/parseInt port-str)]
          (cluster-client/make-conn host port))))))

(defn cluster-multi-set!
  "Set multiple keys atomically in a cluster.
   Keys must all hash to the same slot (use hash tags)."
  [nodes docker-host? slot-mapping-atom keys-values]
  (let [keys (map first keys-values)
        slot (cluster-client/slot-for-key (first keys))]
    ;; Verify all keys are same slot
    (when-not (keys-same-slot? keys)
      (throw+ {:type :crossslot
               :message "Keys don't hash to the same slot"}))

    (let [conn (find-slot-owner-conn nodes docker-host? slot slot-mapping-atom)]
      (execute-multi-set! conn keys-values))))

(defn cluster-multi-get
  "Get multiple keys atomically in a cluster.
   Keys must all hash to the same slot (use hash tags)."
  [nodes docker-host? slot-mapping-atom keys]
  (let [slot (cluster-client/slot-for-key (first keys))]
    (when-not (keys-same-slot? keys)
      (throw+ {:type :crossslot
               :message "Keys don't hash to the same slot"}))

    (let [conn (find-slot-owner-conn nodes docker-host? slot slot-mapping-atom)]
      (execute-multi-get conn keys))))

(defn cluster-atomic-transfer!
  "Atomically transfer between two keys in a cluster."
  [nodes docker-host? slot-mapping-atom from-key to-key amount]
  (when-not (keys-same-slot? [from-key to-key])
    (throw+ {:type :crossslot
             :message "Keys don't hash to the same slot"}))

  (let [slot (cluster-client/slot-for-key from-key)
        conn (find-slot-owner-conn nodes docker-host? slot slot-mapping-atom)]
    (execute-atomic-transfer! conn from-key to-key amount)))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord CrossSlotClient [nodes docker-host? slot-mapping]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          all-nodes (or (:cluster-nodes test) ["n1" "n2" "n3"])]
      (info "Opening cross-slot client")
      (assoc this
             :nodes all-nodes
             :docker-host? docker?
             :slot-mapping (atom (cluster-client/create-slot-mapping all-nodes docker?)))))

  (setup! [this test]
    this)

  (invoke! [this test op]
    (try+
      (case (:f op)
        :hash-tag-write
        (let [{:keys [tag keys-values]} (:value op)
              tagged-kvs (mapv (fn [[suffix value]]
                                 [(str "{" tag "}:" suffix) value])
                               keys-values)]
          (cluster-multi-set! nodes docker-host? slot-mapping tagged-kvs)
          (assoc op :type :ok :value {:tag tag
                                       :keys-written (count tagged-kvs)
                                       :slot (cluster-client/slot-for-key (str "{" tag "}:x"))}))

        :hash-tag-read
        (let [{:keys [tag suffixes]} (:value op)
              keys (make-tagged-keys tag suffixes)
              result (cluster-multi-get nodes docker-host? slot-mapping keys)]
          (assoc op :type :ok :value {:tag tag
                                       :values result
                                       :slot (cluster-client/slot-for-key (first keys))}))

        :cross-slot-attempt
        ;; Try to do a multi-key operation across slots - should fail
        (let [keys (:value op)]
          (if (keys-same-slot? keys)
            (assoc op :type :fail :error :keys-unexpectedly-same-slot)
            (try+
              (cluster-multi-get nodes docker-host? slot-mapping keys)
              ;; If we get here, it didn't throw as expected
              (assoc op :type :fail :error :should-have-failed)
              (catch [:type :crossslot] e
                (assoc op :type :ok :value :correctly-rejected)))))

        :atomic-transfer
        (let [{:keys [tag from-suffix to-suffix amount]} (:value op)
              from-key (str "{" tag "}:" from-suffix)
              to-key (str "{" tag "}:" to-suffix)
              success? (cluster-atomic-transfer! nodes docker-host? slot-mapping from-key to-key amount)]
          (if success?
            (assoc op :type :ok :value {:transferred amount})
            (assoc op :type :fail :error :insufficient-funds)))

        :verify-slot-locality
        (let [{:keys [tag suffixes]} (:value op)
              keys (make-tagged-keys tag suffixes)
              slots (map cluster-client/slot-for-key keys)
              all-same (apply = slots)]
          (assoc op :type :ok :value {:all-same-slot all-same
                                       :slot (first slots)
                                       :key-count (count keys)}))

        :single-write
        (let [{:keys [key value]} (:value op)]
          (cluster-client/cluster-set slot-mapping key value docker-host? nodes)
          (assoc op :type :ok :value {:key key :slot (cluster-client/slot-for-key key)}))

        :single-read
        (let [key (:value op)
              value (cluster-client/cluster-get slot-mapping key docker-host? nodes)]
          (assoc op :type :ok :value {:key key
                                       :value (frogdb/parse-value value)
                                       :slot (cluster-client/slot-for-key key)}))

        ;; Generic read (used by final-reads phase) — delegates to single-read
        :read
        (let [key (or (:value op) (str "{jepsen-xslot}:a"))
              value (cluster-client/cluster-get slot-mapping key docker-host? nodes)]
          (assoc op :type :ok :value {:key key
                                       :value (frogdb/parse-value value)
                                       :slot (cluster-client/slot-for-key key)})))

      (catch java.net.ConnectException e
        (assoc op :type :fail :error :connection-refused))

      (catch java.net.SocketTimeoutException e
        (assoc op :type :info :error :timeout))

      (catch [:type :crossslot] e
        (assoc op :type :fail :error :crossslot))

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
  "Create a new cross-slot client."
  []
  (map->CrossSlotClient {}))

;; ===========================================================================
;; Generators
;; ===========================================================================

(def account-tag "account")
(def account-suffixes ["balance-a" "balance-b"])

(defn hash-tag-write
  "Generate a hash-tag-write operation."
  [tag keys-values]
  {:type :invoke :f :hash-tag-write :value {:tag tag :keys-values keys-values}})

(defn hash-tag-read
  "Generate a hash-tag-read operation."
  [tag suffixes]
  {:type :invoke :f :hash-tag-read :value {:tag tag :suffixes suffixes}})

(defn cross-slot-attempt
  "Generate a cross-slot-attempt operation."
  [keys]
  {:type :invoke :f :cross-slot-attempt :value keys})

(defn atomic-transfer
  "Generate an atomic-transfer operation."
  [tag from to amount]
  {:type :invoke :f :atomic-transfer :value {:tag tag
                                              :from-suffix from
                                              :to-suffix to
                                              :amount amount}})

(defn verify-slot-locality
  "Generate a verify-slot-locality operation."
  [tag suffixes]
  {:type :invoke :f :verify-slot-locality :value {:tag tag :suffixes suffixes}})

(defn generator
  "Generator for cross-slot testing.
   Tests hash tag co-location and atomic operations."
  [opts]
  (let [rate (get opts :rate 10)]
    (gen/phases
      ;; Phase 1: Initialize accounts with hash tags
      (gen/log "Initializing account balances")
      (gen/once (hash-tag-write account-tag [["balance-a" 1000] ["balance-b" 1000]]))
      (gen/sleep 1)

      ;; Phase 2: Verify slot locality
      (gen/log "Verifying hash tag slot locality")
      (gen/once (verify-slot-locality account-tag account-suffixes))

      ;; Phase 3: Perform atomic transfers
      (gen/log "Performing atomic transfers")
      (->> (gen/mix [(fn [] (atomic-transfer account-tag "balance-a" "balance-b" (rand-int 100)))
                     (fn [] (atomic-transfer account-tag "balance-b" "balance-a" (rand-int 100)))
                     (fn [] (hash-tag-read account-tag account-suffixes))])
           (gen/limit 100)
           (gen/stagger (/ 1 rate)))

      ;; Phase 4: Test cross-slot rejection
      (gen/log "Testing cross-slot rejection")
      (gen/once (cross-slot-attempt ["key-without-tag-1" "key-without-tag-2"]))

      ;; Phase 5: Final balance read
      (gen/log "Reading final balances")
      (gen/once (hash-tag-read account-tag account-suffixes)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn checker
  "Checker for cross-slot workload.

   Verifies:
   - Total balance conserved across transfers
   - Hash-tagged keys route to same slot
   - Cross-slot operations correctly rejected"
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [completed (filter #(= :ok (:type %)) history)
            failed (filter #(= :fail (:type %)) history)

            ;; Extract balance reads
            balance-reads (->> completed
                               (filter #(= :hash-tag-read (:f %)))
                               (filter #(= account-tag (get-in % [:value :tag]))))

            ;; Get initial and final balances
            initial-balances (get-in (first balance-reads) [:value :values])
            final-balances (get-in (last balance-reads) [:value :values])

            initial-total (when initial-balances
                            (reduce + (map #(or % 0) (vals initial-balances))))
            final-total (when final-balances
                          (reduce + (map #(or % 0) (vals final-balances))))

            ;; Check transfer count
            transfers (filter #(= :atomic-transfer (:f %)) completed)
            failed-transfers (filter #(and (= :atomic-transfer (:f %))
                                           (= :fail (:type %)))
                                     (concat completed failed))

            ;; Check cross-slot rejection
            cross-slot-ops (filter #(= :cross-slot-attempt (:f %)) completed)
            correctly-rejected (filter #(= :correctly-rejected (:value %)) cross-slot-ops)

            ;; Check slot locality verifications
            locality-checks (filter #(= :verify-slot-locality (:f %)) completed)
            all-local (every? #(get-in % [:value :all-same-slot]) locality-checks)]

        {:valid? (and (or (nil? initial-total) (nil? final-total) (= initial-total final-total))
                      all-local)
         :balance-conserved (= initial-total final-total)
         :initial-total initial-total
         :final-total final-total
         :initial-balances initial-balances
         :final-balances final-balances
         :total-transfers (count transfers)
         :failed-transfers (count failed-transfers)
         :cross-slot-correctly-rejected (count correctly-rejected)
         :hash-tags-colocated all-local}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a cross-slot workload.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (generator opts)
   :checker (checker)})
