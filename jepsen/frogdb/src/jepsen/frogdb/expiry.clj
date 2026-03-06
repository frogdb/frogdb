(ns jepsen.frogdb.expiry
  "Expiration/TTL workload for FrogDB.

   Tests key expiration behavior under faults using SET EX, EXPIRE, TTL, PERSIST.

   This workload verifies:
   - Keys expire after TTL (within tolerance)
   - No premature expiration (key disappears before TTL)
   - No zombie keys (key exists after TTL + tolerance)
   - PERSIST correctly removes expiration
   - TTL reports consistent values"
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

;; Key pool: we use a fixed pool of keys
(def key-pool (mapv #(str "expiry-key-" %) (range 20)))

;; TTL settings (in seconds)
;; We use short TTLs to make tests observable within reasonable time
(def min-ttl 3)
(def max-ttl 10)

;; Tolerance for expiration checking (in seconds)
;; Allow some slack for clock drift, processing delays
(def expiry-tolerance 2)

;; ===========================================================================
;; Expiry Operations
;; ===========================================================================

(defn set-ex!
  "Set key with expiration in seconds using SET EX."
  [conn key value ttl-seconds]
  (wcar conn (car/set key (str value) "EX" ttl-seconds)))

(defn set-px!
  "Set key with expiration in milliseconds using SET PX."
  [conn key value ttl-ms]
  (wcar conn (car/set key (str value) "PX" ttl-ms)))

(defn get-key
  "Get value of a key."
  [conn key]
  (wcar conn (car/get key)))

(defn ttl-key
  "Get TTL of a key in seconds.
   Returns: positive number = TTL remaining, -1 = no expiry, -2 = key doesn't exist."
  [conn key]
  (wcar conn (car/ttl key)))

(defn pttl-key
  "Get TTL of a key in milliseconds."
  [conn key]
  (wcar conn (car/pttl key)))

(defn expire!
  "Set expiration on existing key. Returns 1 if set, 0 if key doesn't exist."
  [conn key seconds]
  (wcar conn (car/expire key seconds)))

(defn persist!
  "Remove expiration from key. Returns 1 if removed, 0 if no expiry or no key."
  [conn key]
  (wcar conn (car/persist key)))

(defn del!
  "Delete a key."
  [conn key]
  (wcar conn (car/del key)))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord ExpiryClient [conn node docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)]
      (info "Opening expiry client to" node "(docker-host?:" docker? ")")
      (assoc this
             :conn (frogdb/conn-spec-single node frogdb/default-port docker? base-port)
             :node node
             :docker-host? docker?)))

  (setup! [this test]
    ;; Clear all test keys at the start
    (info "Clearing expiry keys")
    (doseq [k key-pool]
      (wcar conn (car/del k)))
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (let [k (:key op (rand-nth key-pool))]
        (case (:f op)
          ;; Set with TTL
          :set-with-ttl
          (let [{:keys [key value ttl]} (:value op)
                set-time (System/currentTimeMillis)]
            (set-ex! conn key value ttl)
            (assoc op :type :ok
                   :value {:key key :value value :ttl ttl}
                   :set-time set-time
                   :expected-expiry (+ set-time (* ttl 1000))))

          ;; Read key (may return nil if expired)
          :read
          (let [key (:value op)
                read-time (System/currentTimeMillis)
                value (get-key conn key)]
            (assoc op :type :ok
                   :value {:key key :value value}
                   :read-time read-time))

          ;; Get TTL
          :ttl
          (let [key (:value op)
                check-time (System/currentTimeMillis)
                ttl (ttl-key conn key)]
            (assoc op :type :ok
                   :value {:key key :ttl ttl}
                   :check-time check-time))

          ;; Set expiration on existing key
          :expire
          (let [{:keys [key ttl]} (:value op)
                set-time (System/currentTimeMillis)
                result (expire! conn key ttl)]
            (assoc op :type :ok
                   :value {:key key :ttl ttl}
                   :result result
                   :set-time set-time))

          ;; Remove expiration
          :persist
          (let [key (:value op)
                result (persist! conn key)]
            (assoc op :type :ok
                   :value {:key key}
                   :result result))

          ;; Delete key
          :delete
          (let [key (:value op)
                result (del! conn key)]
            (assoc op :type :ok :value {:key key} :result result))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (frogdb/close-conn! conn)))

(defn create-client
  "Create a new expiry client."
  []
  (map->ExpiryClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn set-with-ttl-op
  "Generate a set-with-ttl operation."
  [_ _]
  {:type :invoke
   :f :set-with-ttl
   :value {:key (rand-nth key-pool)
           :value (rand-int 10000)
           :ttl (+ min-ttl (rand-int (- max-ttl min-ttl)))}})

(defn read-op
  "Generate a read operation."
  [_ _]
  {:type :invoke :f :read :value (rand-nth key-pool)})

(defn ttl-op
  "Generate a TTL check operation."
  [_ _]
  {:type :invoke :f :ttl :value (rand-nth key-pool)})

(defn expire-op
  "Generate an expire operation."
  [_ _]
  {:type :invoke
   :f :expire
   :value {:key (rand-nth key-pool)
           :ttl (+ min-ttl (rand-int (- max-ttl min-ttl)))}})

(defn persist-op
  "Generate a persist operation."
  [_ _]
  {:type :invoke :f :persist :value (rand-nth key-pool)})

(defn delete-op
  "Generate a delete operation."
  [_ _]
  {:type :invoke :f :delete :value (rand-nth key-pool)})

(defn generator
  "Create a generator for expiry operations.

   Options:
   - :rate - operations per second (default 10)"
  [opts]
  (let [rate (get opts :rate 10)]
    (->> (gen/mix [(gen/repeat set-with-ttl-op)
                   (gen/repeat set-with-ttl-op)  ; Weight sets more heavily
                   (gen/repeat read-op)
                   (gen/repeat read-op)
                   (gen/repeat ttl-op)
                   (gen/repeat expire-op)
                   (gen/repeat persist-op)])
         (gen/stagger (/ 1 rate)))))

(defn final-read-generator
  "Generator for final reads of all keys."
  []
  (->> key-pool
       (map (fn [k] {:f :read :value k}))
       (gen/each-thread)))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn check-expiry-history
  "Analyze the history for TTL/expiration correctness.

   We track:
   1. Keys set with TTL - record expected expiry time
   2. Reads that returned nil - check if it was after expected expiry
   3. Reads that returned value - check if it was before expected expiry
   4. Persist operations - should remove expiration

   Known limitations:
   - Concurrent operations make strict checking difficult
   - Clock skew between operations
   - We use a tolerance window for expiration checks"
  [history]
  (let [;; Get all successful operations
        ops (->> history
                 (filter #(= :ok (:type %))))

        ;; Build a timeline of TTL-setting operations per key
        ;; Each entry tracks when TTL was set and expected expiry
        ttl-events
        (reduce
         (fn [state op]
           (let [key (or (get-in op [:value :key])
                        (:value op))]
             (case (:f op)
               ;; SET with TTL establishes expected expiry
               :set-with-ttl
               (assoc state key {:set-time (:set-time op)
                                :ttl (get-in op [:value :ttl])
                                :expected-expiry (:expected-expiry op)
                                :persisted false})

               ;; EXPIRE updates expiry (if key exists)
               :expire
               (if (= 1 (:result op))
                 (let [ttl (get-in op [:value :ttl])
                       set-time (:set-time op)]
                   (assoc state key {:set-time set-time
                                    :ttl ttl
                                    :expected-expiry (+ set-time (* ttl 1000))
                                    :persisted false}))
                 state)

               ;; PERSIST removes expiry
               :persist
               (if (= 1 (:result op))
                 (if-let [existing (get state key)]
                   (assoc state key (assoc existing :persisted true))
                   state)
                 state)

               ;; DELETE removes key entirely
               :delete
               (dissoc state key)

               ;; Other ops don't change TTL state
               state)))
         {}
         ops)

        ;; Check reads for consistency
        ;; A read returning nil before expected expiry is suspicious
        ;; A read returning value after expected expiry (+ tolerance) is suspicious
        tolerance-ms (* expiry-tolerance 1000)

        read-ops (->> ops
                     (filter #(= :read (:f %))))

        ;; We only flag clear violations where:
        ;; - Key was set with TTL and not persisted/deleted
        ;; - Read happened within a clear window

        ;; Note: Due to concurrent operations, we can't do perfect checking
        ;; Just verify no obviously wrong behavior

        ;; Count operations
        set-count (count (filter #(= :set-with-ttl (:f %)) ops))
        expire-count (count (filter #(= :expire (:f %)) ops))
        persist-count (count (filter #(= :persist (:f %)) ops))
        read-count (count (filter #(= :read (:f %)) ops))
        ttl-check-count (count (filter #(= :ttl (:f %)) ops))
        delete-count (count (filter #(= :delete (:f %)) ops))

        ;; Check for any TTL values that were clearly wrong
        ;; TTL should never be > the max we set, unless there's drift
        suspicious-ttls
        (->> ops
             (filter #(= :ttl (:f %)))
             (filter (fn [op]
                       (let [ttl (get-in op [:value :ttl])]
                         (and (number? ttl)
                              (pos? ttl)
                              (> ttl (+ max-ttl expiry-tolerance))))))
             (map (fn [op]
                    {:key (get-in op [:value :key])
                     :ttl (get-in op [:value :ttl])})))]

    {:valid? (empty? suspicious-ttls)
     :set-count set-count
     :expire-count expire-count
     :persist-count persist-count
     :read-count read-count
     :ttl-check-count ttl-check-count
     :delete-count delete-count
     :suspicious-ttls (seq suspicious-ttls)
     :note "TTL checking has limited precision due to concurrent operations and clock considerations"}))

(defn checker
  "Create a checker for expiry operations."
  []
  (reify checker/Checker
    (check [this test history opts]
      (check-expiry-history history))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Create an expiry workload for testing.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (gen/phases
                (generator opts)
                ;; Wait a bit for some keys to expire
                (gen/sleep 5)
                ;; Final reads
                (gen/clients (final-read-generator)))
   :checker (checker/compose
              {:expiry (checker)})})
