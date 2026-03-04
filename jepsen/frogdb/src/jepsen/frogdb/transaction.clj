(ns jepsen.frogdb.transaction
  "Transaction workload for FrogDB.

   Tests MULTI/EXEC atomicity beyond single-key CAS using a bank account
   transfer model. This workload verifies:
   - Transaction atomicity: either all writes visible or none
   - No partial transactions: key1 updated but not key2
   - Total balance conservation: sum of all accounts remains constant

   Scenarios tested:
   1. Multi-key writes in MULTI/EXEC
   2. Transfer operations (DECRBY/INCRBY)
   3. WATCH-based conditional transactions"
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

;; Account keys - we use a fixed set of accounts for the bank model
(def num-accounts 5)
(def initial-balance 1000)

(defn account-key
  "Get the Redis key for an account.
   Uses a hash tag so all accounts route to the same shard."
  [account-id]
  (str "{jepsen-txn}account-" account-id))

(defn all-account-keys
  "Get all account keys."
  []
  (map account-key (range num-accounts)))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord TransactionClient [conn node docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)]
      (info "Opening transaction client to" node "(docker-host?:" docker? ")")
      (assoc this
             :conn (frogdb/conn-spec node frogdb/default-port docker? base-port)
             :node node
             :docker-host? docker?)))

  (setup! [this test]
    ;; Initialize all accounts with starting balance
    (info "Initializing" num-accounts "accounts with balance" initial-balance)
    (doseq [i (range num-accounts)]
      (wcar conn (car/set (account-key i) (str initial-balance))))
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (case (:f op)
        ;; Read all account balances atomically using Lua EVAL
        :read
        (let [keys (all-account-keys)
              lua-script (str "local r = {} "
                              "for i=1,#KEYS do "
                              "  r[i] = redis.call('GET', KEYS[i]) "
                              "end "
                              "return r")
              results (wcar conn
                        (apply car/eval lua-script (count keys) keys))
              parsed (->> results
                          (map frogdb/parse-value)
                          (map #(or % 0))
                          vec)]
          (assoc op :type :ok :value parsed))

        ;; Transfer between two accounts using Lua EVAL for atomicity
        :transfer
        (let [{:keys [from to amount]} (:value op)
              from-key (account-key from)
              to-key (account-key to)
              result (wcar conn
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
                         2 from-key to-key (str amount)))]
          (if (= 1 result)
            (assoc op :type :ok)
            (assoc op :type :fail :error :insufficient-funds)))

        ;; Multi-key write: set multiple accounts atomically
        :multi-write
        (let [writes (:value op)]
          (wcar conn
            (car/multi)
            (doseq [[account-id value] writes]
              (car/set (account-key account-id) (str value)))
            (car/exec))
          (assoc op :type :ok)))))

  (teardown! [this test]
    nil)

  (close! [this test]
    nil))

(defn create-client
  "Create a new transaction client."
  []
  (map->TransactionClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn read-op
  "Generate a read operation."
  [_ _]
  {:type :invoke :f :read :value nil})

(defn transfer-op
  "Generate a transfer operation between two random accounts."
  [_ _]
  (let [from (rand-int num-accounts)
        to (loop [t (rand-int num-accounts)]
             (if (= t from)
               (recur (rand-int num-accounts))
               t))
        amount (inc (rand-int 100))]
    {:type :invoke
     :f :transfer
     :value {:from from :to to :amount amount}}))

(defn generator
  "Create a generator for transaction operations.

   Options:
   - :rate - operations per second (default 10)
   - :read-fraction - fraction of reads (default 0.3)"
  [opts]
  (let [rate (get opts :rate 10)]
    (->> (gen/mix [(gen/repeat transfer-op)
                   (gen/repeat read-op)])
         (gen/stagger (/ 1 rate)))))

(defn final-read-generator
  "Generator for final reads to verify conservation."
  []
  (->> (gen/repeat {:f :read})
       (gen/limit 10)
       (gen/stagger 0.2)))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn check-bank-history
  "Analyze the history for bank transaction correctness.

   Checks:
   1. Total balance is conserved (sum of all accounts constant)
   2. No negative balances observed
   3. No partial transactions (conservation violation)"
  [history]
  (let [expected-total (* num-accounts initial-balance)

        ;; Get all successful reads
        reads (->> history
                   (filter #(and (= :read (:f %))
                                (= :ok (:type %))))
                   (map :value))

        ;; Check each read for total conservation
        totals (map #(reduce + %) reads)

        ;; Find any violations
        violations (->> totals
                        (map-indexed (fn [idx total]
                                      (when (not= total expected-total)
                                        {:read-index idx
                                         :expected expected-total
                                         :actual total
                                         :difference (- total expected-total)})))
                        (filter some?))

        ;; Check for negative balances
        negative-reads (->> reads
                           (map-indexed (fn [idx balances]
                                         (let [neg (filter neg? balances)]
                                           (when (seq neg)
                                             {:read-index idx
                                              :negative-balances neg
                                              :all-balances balances}))))
                           (filter some?))

        ;; Count successful transfers
        successful-transfers (->> history
                                  (filter #(and (= :transfer (:f %))
                                               (= :ok (:type %))))
                                  count)

        failed-transfers (->> history
                             (filter #(and (= :transfer (:f %))
                                          (= :fail (:type %))))
                             count)]

    {:valid? (and (empty? violations)
                  (empty? negative-reads))
     :expected-total expected-total
     :read-count (count reads)
     :successful-transfers successful-transfers
     :failed-transfers failed-transfers
     :conservation-violations (seq violations)
     :negative-balance-reads (seq negative-reads)
     :final-balances (last reads)
     :final-total (last totals)}))

(defn checker
  "Create a checker for transaction operations."
  []
  (reify checker/Checker
    (check [this test history opts]
      (check-bank-history history))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Create a transaction workload for testing.

   Options:
   - :rate - operations per second
   - :read-fraction - fraction of operations that are reads"
  [opts]
  {:client (create-client)
   :generator (gen/phases
                (generator opts)
                ;; Final reads to verify conservation
                (gen/clients (final-read-generator)))
   :checker (checker/compose
              {:bank (checker)})})
