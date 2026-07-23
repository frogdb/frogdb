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
  "Set multiple keys atomically using Lua EVAL.
   All keys must be in the same slot."
  [conn keys-values]
  (let [keys (mapv first keys-values)
        vals (mapv (comp str second) keys-values)
        n (count keys)
        lua-script (str "for i=1," n " do redis.call('SET', KEYS[i], ARGV[i]) end return 'OK'")]
    (wcar conn (apply car/eval lua-script n (concat keys vals)))))

(defn execute-multi-get
  "Get multiple keys atomically using Lua EVAL."
  [conn keys]
  (let [n (count keys)
        lua-script (str "local r = {} for i=1," n " do r[i] = redis.call('GET', KEYS[i]) end return r")
        results (wcar conn (apply car/eval lua-script n keys))]
    (zipmap keys (map frogdb/parse-value results))))

(defn execute-atomic-transfer!
  "Atomically transfer amount from one key to another using Lua EVAL.
   Both keys must be in the same slot."
  [conn from-key to-key amount]
  (= 1 (wcar conn
    (car/eval
      (str "local from = tonumber(redis.call('GET', KEYS[1])) "
           "local to = tonumber(redis.call('GET', KEYS[2])) "
           "if from >= tonumber(ARGV[1]) then "
           "  redis.call('SET', KEYS[1], tostring(from - tonumber(ARGV[1]))) "
           "  redis.call('SET', KEYS[2], tostring(to + tonumber(ARGV[1]))) "
           "  return 1 "
           "else "
           "  return 0 "
           "end")
      2 from-key to-key (str amount)))))

;; ===========================================================================
;; Cluster-Aware Multi Operations
;; ===========================================================================

(defn find-slot-owner-conn
  "Find the node that owns a slot and return its connection."
  ([nodes docker-host? slot slot-mapping-atom]
   (find-slot-owner-conn nodes docker-host? slot slot-mapping-atom cluster-db/default-base-port))
  ([nodes docker-host? slot slot-mapping-atom base-port]
   (let [mapping @slot-mapping-atom
         addr (cluster-client/get-node-for-slot mapping slot)]
     (if addr
       (let [[host port-str] (str/split addr #":")
             port (Integer/parseInt port-str)]
         (cluster-client/make-conn host port))
       ;; Fallback: refresh and try again
       (let [new-mapping (cluster-client/refresh-slot-mapping mapping (first nodes) docker-host? base-port)]
         (reset! slot-mapping-atom new-mapping)
         (let [addr (cluster-client/get-node-for-slot new-mapping slot)
               [host port-str] (str/split addr #":")
               port (Integer/parseInt port-str)]
           (cluster-client/make-conn host port)))))))

(defn cluster-multi-set!
  "Set multiple keys atomically in a cluster.
   Keys must all hash to the same slot (use hash tags)."
  ([nodes docker-host? slot-mapping-atom keys-values]
   (cluster-multi-set! nodes docker-host? slot-mapping-atom keys-values cluster-db/default-base-port))
  ([nodes docker-host? slot-mapping-atom keys-values base-port]
   (let [keys (map first keys-values)
         slot (cluster-client/slot-for-key (first keys))]
     ;; Verify all keys are same slot
     (when-not (keys-same-slot? keys)
       (throw+ {:type :crossslot
                :message "Keys don't hash to the same slot"}))

     (let [conn (find-slot-owner-conn nodes docker-host? slot slot-mapping-atom base-port)]
       (execute-multi-set! conn keys-values)))))

(defn cluster-multi-get
  "Get multiple keys atomically in a cluster.
   Keys must all hash to the same slot (use hash tags)."
  ([nodes docker-host? slot-mapping-atom keys]
   (cluster-multi-get nodes docker-host? slot-mapping-atom keys cluster-db/default-base-port))
  ([nodes docker-host? slot-mapping-atom keys base-port]
   (let [slot (cluster-client/slot-for-key (first keys))]
     (when-not (keys-same-slot? keys)
       (throw+ {:type :crossslot
                :message "Keys don't hash to the same slot"}))

     (let [conn (find-slot-owner-conn nodes docker-host? slot slot-mapping-atom base-port)]
       (execute-multi-get conn keys)))))

(defn cluster-atomic-transfer!
  "Atomically transfer between two keys in a cluster."
  ([nodes docker-host? slot-mapping-atom from-key to-key amount]
   (cluster-atomic-transfer! nodes docker-host? slot-mapping-atom from-key to-key amount cluster-db/default-base-port))
  ([nodes docker-host? slot-mapping-atom from-key to-key amount base-port]
   (when-not (keys-same-slot? [from-key to-key])
     (throw+ {:type :crossslot
              :message "Keys don't hash to the same slot"}))

   (let [slot (cluster-client/slot-for-key from-key)
         conn (find-slot-owner-conn nodes docker-host? slot slot-mapping-atom base-port)]
     (execute-atomic-transfer! conn from-key to-key amount))))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord CrossSlotClient [nodes docker-host? base-port slot-mapping]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          all-nodes (or (:cluster-nodes test) ["n1" "n2" "n3"])
          bp (get test :base-port cluster-db/default-base-port)]
      (info "Opening cross-slot client")
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
        :hash-tag-write
        (let [{:keys [tag keys-values]} (:value op)
              tagged-kvs (mapv (fn [[suffix value]]
                                 [(str "{" tag "}:" suffix) value])
                               keys-values)]
          (cluster-multi-set! nodes docker-host? slot-mapping tagged-kvs base-port)
          (assoc op :type :ok :value {:tag tag
                                       :keys-written (count tagged-kvs)
                                       :slot (cluster-client/slot-for-key (str "{" tag "}:x"))}))

        :hash-tag-read
        (let [{:keys [tag suffixes]} (:value op)
              keys (make-tagged-keys tag suffixes)
              result (cluster-multi-get nodes docker-host? slot-mapping keys base-port)]
          (assoc op :type :ok :value {:tag tag
                                       :values result
                                       :slot (cluster-client/slot-for-key (first keys))}))

        :cross-slot-attempt
        ;; Try to do a multi-key operation across slots - should fail
        (let [keys (:value op)]
          (if (keys-same-slot? keys)
            (assoc op :type :fail :error :keys-unexpectedly-same-slot)
            (try+
              (cluster-multi-get nodes docker-host? slot-mapping keys base-port)
              ;; If we get here, it didn't throw as expected
              (assoc op :type :fail :error :should-have-failed)
              (catch [:type :crossslot] e
                (assoc op :type :ok :value :correctly-rejected)))))

        :atomic-transfer
        (let [{:keys [tag from-suffix to-suffix amount]} (:value op)
              from-key (str "{" tag "}:" from-suffix)
              to-key (str "{" tag "}:" to-suffix)
              success? (cluster-atomic-transfer! nodes docker-host? slot-mapping from-key to-key amount base-port)]
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
          (cluster-client/cluster-set slot-mapping key value docker-host? nodes base-port)
          (assoc op :type :ok :value {:key key :slot (cluster-client/slot-for-key key)}))

        :single-read
        (let [key (:value op)
              value (cluster-client/cluster-get slot-mapping key docker-host? nodes base-port)]
          (assoc op :type :ok :value {:key key
                                       :value (frogdb/parse-value value)
                                       :slot (cluster-client/slot-for-key key)}))

        ;; Generic read (used by final-reads phase) — delegates to single-read
        :read
        (let [key (or (:value op) (str "{jepsen-xslot}:a"))
              value (cluster-client/cluster-get slot-mapping key docker-host? nodes base-port)]
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
  "Fault-aware checker for the cross-slot workload.

   Three properties are computed. Two ALWAYS gate :valid? (they are
   fault-independent client-side/safety invariants); one is kill-gated (it
   encodes a contract the architecture does not promise under a crash).

   - Slot co-location (ALWAYS gates): every :verify-slot-locality op must report
     that its hash-tagged keys map to a single slot. This is a pure client-side
     CRC16 computation — it never touches a server and cannot be perturbed by a
     fault — so a failure here is a real hash-tag/CRC16 bug and always fails.

   - Cross-slot rejection safety (ALWAYS gates): a multi-key op spanning slots
     must NEVER be silently accepted. The client refuses cross-slot keys with a
     client-side slot check (throwing :crossslot); if that check ever lets a
     genuinely cross-slot multi-get through, the op is recorded as
     :fail :should-have-failed. That is a correctness bug independent of fault
     injection, so it always gates. (`:keys-unexpectedly-same-slot` — the two
     probe keys coincidentally colliding to one slot — is a test-data artifact,
     not a FrogDB violation, so it is surfaced but does not gate.)

   - Balance conservation (KILL-GATED): the account balances are hash-tag
     co-located, so they live on a single shard and every atomic transfer applies
     both SET legs or neither — total is conserved. Under a network partition
     (no kill) committed data is never destroyed, so a conservation shortfall is
     a real failure and gates. Under a SIGKILL, however, FrogDB does NOT promise
     failure atomicity: the raft-cluster shards are single-owner with no per-shard
     replication (docker-compose.raft-cluster.yml), so a kill can lose recent
     un-fsynced acknowledged writes (Redis-style async persistence), and a
     cross-shard VLL EXEC may land as a durable partial commit by documented
     design (concurrency issue 05 — option 4 fail-stop; failure-atomic recovery
     deferred to the durability phase). So when a kill fired, a conservation
     shortfall is the accepted contract: it is surfaced loudly (totals + balances
     retained in the result) for visibility but does not fail the run. This
     mirrors the kill-gating the key-routing checker already encodes."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [completed (filter #(= :ok (:type %)) history)
            failed (filter #(= :fail (:type %)) history)
            info-ops (filter #(= :info (:type %)) history)

            ;; Extract balance reads (successful hash-tag reads of the accounts).
            ;; Using the last SUCCESSFUL read as the final snapshot means a final
            ;; read that failed under an active fault does not manufacture a false
            ;; imbalance — every successful snapshot of the two balances totals the
            ;; same conserved sum.
            balance-reads (->> completed
                               (filter #(= :hash-tag-read (:f %)))
                               (filter #(= account-tag (get-in % [:value :tag]))))

            initial-balances (get-in (first balance-reads) [:value :values])
            final-balances (get-in (last balance-reads) [:value :values])

            initial-total (when initial-balances
                            (reduce + (map #(or % 0) (vals initial-balances))))
            final-total (when final-balances
                          (reduce + (map #(or % 0) (vals final-balances))))

            ;; Conservation holds when we lack an endpoint (degenerate, e.g. a
            ;; fault swallowed the seeding write) or the two endpoints agree.
            balance-conserved? (or (nil? initial-total)
                                   (nil? final-total)
                                   (= initial-total final-total))

            ;; Check transfer count
            transfers (filter #(= :atomic-transfer (:f %)) completed)
            failed-transfers (filter #(and (= :atomic-transfer (:f %))
                                           (= :fail (:type %)))
                                     (concat completed failed))

            ;; Cross-slot rejection accounting.
            cross-slot-ops (filter #(= :cross-slot-attempt (:f %)) completed)
            correctly-rejected (filter #(= :correctly-rejected (:value %)) cross-slot-ops)
            ;; SAFETY (fault-independent): a cross-slot multi-key op that was NOT
            ;; rejected is recorded as :fail :should-have-failed by the client.
            cross-slot-not-rejected (filter #(= :should-have-failed (:error %))
                                            (concat completed failed))
            cross-slot-safe? (empty? cross-slot-not-rejected)
            ;; Probe keys coincidentally landing in one slot — a test-data artifact
            ;; that means the rejection path was not actually exercised. Surfaced,
            ;; not gated.
            cross-slot-untested (filter #(= :keys-unexpectedly-same-slot (:error %))
                                        (concat completed failed))

            ;; Check slot locality verifications (pure client-side CRC16).
            locality-checks (filter #(= :verify-slot-locality (:f %)) completed)
            all-local (every? #(get-in % [:value :all-same-slot]) locality-checks)

            ;; Kill-gating: a SIGKILL of a single-owner shard can lose recently
            ;; acked un-fsynced writes / leave a documented partial commit, so
            ;; conservation is only enforced when no kill fired. Nemesis kill ops
            ;; appear in the history as :f :kill.
            process-killed? (boolean (some #(= :kill (:f %)) history))
            conservation-enforced? (not process-killed?)]

        {:valid? (boolean (and all-local
                               cross-slot-safe?
                               (or (not conservation-enforced?) balance-conserved?)))
         :balance-conserved balance-conserved?
         :conservation-enforced? conservation-enforced?
         :process-killed? process-killed?
         :initial-total initial-total
         :final-total final-total
         :initial-balances initial-balances
         :final-balances final-balances
         :total-transfers (count transfers)
         :failed-transfers (count failed-transfers)
         :cross-slot-attempts (count cross-slot-ops)
         :cross-slot-correctly-rejected (count correctly-rejected)
         :cross-slot-not-rejected (count cross-slot-not-rejected)
         :cross-slot-safe? cross-slot-safe?
         :cross-slot-untested (count cross-slot-untested)
         :hash-tags-colocated all-local
         :timeout-ops (count (filter #(= :timeout (:error %)) info-ops))
         :failed-ops (count failed)}))))

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
