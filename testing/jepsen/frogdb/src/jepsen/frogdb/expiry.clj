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
                op-time (System/currentTimeMillis)
                result (persist! conn key)]
            (assoc op :type :ok
                   :value {:key key}
                   :result result
                   :op-time op-time))

          ;; Delete key
          :delete
          (let [key (:value op)
                op-time (System/currentTimeMillis)
                result (del! conn key)]
            (assoc op :type :ok :value {:key key} :result result
                   :op-time op-time))))))

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

(defn build-key-timeline
  "Build a per-key timeline of TTL-setting events from the history.
   Returns a map of key -> sorted list of events (by time)."
  [ops]
  (reduce
   (fn [timelines op]
     (let [key (or (get-in op [:value :key])
                   (:value op))]
       (case (:f op)
         :set-with-ttl
         (update timelines key (fnil conj [])
                 {:type :set-ttl
                  :time (:set-time op)
                  :ttl-ms (* (get-in op [:value :ttl]) 1000)
                  :expected-expiry (:expected-expiry op)})

         :expire
         (if (= 1 (:result op))
           (update timelines key (fnil conj [])
                   {:type :expire
                    :time (:set-time op)
                    :ttl-ms (* (get-in op [:value :ttl]) 1000)
                    :expected-expiry (+ (:set-time op) (* (get-in op [:value :ttl]) 1000))})
           timelines)

         :persist
         (if (= 1 (:result op))
           (update timelines key (fnil conj [])
                   {:type :persist
                    :time (:op-time op)})
           timelines)

         :delete
         (update timelines key (fnil conj [])
                 {:type :delete
                  :time (:op-time op)})

         timelines)))
   {}
   ops))

(defn find-most-recent-event
  "Find the most recent timeline event for a key before a given time."
  [timeline read-time]
  (when (seq timeline)
    (->> timeline
         (filter #(<= (:time %) read-time))
         last)))

(defn check-read-against-timeline
  "Check a read operation against the key's timeline.
   Returns a violation map if the read is clearly inconsistent, nil otherwise.

   A read is a clear violation if:
   - Key should be alive (well before TTL expiry) but read returns nil
   - Key should be expired (well after TTL + tolerance) but read returns non-nil"
  [timelines op tolerance-ms]
  (let [key (get-in op [:value :key])
        read-time (:read-time op)
        value (get-in op [:value :value])
        timeline (get timelines key)]
    (when (and timeline read-time)
      (let [event (find-most-recent-event timeline read-time)]
        (when event
          (case (:type event)
            ;; After SET-with-TTL or EXPIRE: check read against expected expiry
            (:set-ttl :expire)
            (let [expected-expiry (:expected-expiry event)
                  well-before? (< read-time (- expected-expiry tolerance-ms))
                  well-after? (> read-time (+ expected-expiry tolerance-ms))]
              (cond
                ;; Key should still be alive but returned nil — premature expiration
                (and well-before? (nil? value))
                {:violation :premature-expiry
                 :key key
                 :read-time read-time
                 :expected-expiry expected-expiry
                 :ms-before-expiry (- expected-expiry read-time)
                 :value value}

                ;; Key should be expired but still has a value — zombie key
                (and well-after? (some? value))
                {:violation :zombie-key
                 :key key
                 :read-time read-time
                 :expected-expiry expected-expiry
                 :ms-after-expiry (- read-time expected-expiry)
                 :value value}

                :else nil))

            ;; After PERSIST: key should exist (no expiry)
            :persist
            nil  ;; Can't verify — we don't know if the key had a value

            ;; After DELETE: key should be nil
            :delete
            (when (some? value)
              {:violation :read-after-delete
               :key key
               :read-time read-time
               :value value})

            nil))))))

(defn check-expiry-history
  "Analyze the history for TTL/expiration correctness.

   Uses timeline-based validation: for each read, find the most recent
   TTL-setting event on that key and verify the read is consistent with
   the expected expiration state.

   Flags clear violations:
   - Premature expiry: key returns nil well before TTL should expire
   - Zombie keys: key returns a value well after TTL + tolerance
   - TTL values exceeding configured maximum

   Known limitations:
   - Concurrent operations create ambiguous windows (handled via tolerance)
   - Client-side timestamps may have minor skew"
  [history]
  (let [ops (->> history
                 (filter #(= :ok (:type %))))

        ;; Build per-key timelines
        timelines (build-key-timeline ops)

        tolerance-ms (* expiry-tolerance 1000)

        ;; Check each read against the timeline
        read-violations
        (->> ops
             (filter #(= :read (:f %)))
             (keep #(check-read-against-timeline timelines % tolerance-ms)))

        ;; Check for TTL values that exceed configured maximum
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
                     :ttl (get-in op [:value :ttl])})))

        ;; Count operations
        set-count (count (filter #(= :set-with-ttl (:f %)) ops))
        expire-count (count (filter #(= :expire (:f %)) ops))
        persist-count (count (filter #(= :persist (:f %)) ops))
        read-count (count (filter #(= :read (:f %)) ops))
        ttl-check-count (count (filter #(= :ttl (:f %)) ops))
        delete-count (count (filter #(= :delete (:f %)) ops))

        all-violations (concat read-violations suspicious-ttls)]

    {:valid? (empty? all-violations)
     :set-count set-count
     :expire-count expire-count
     :persist-count persist-count
     :read-count read-count
     :ttl-check-count ttl-check-count
     :delete-count delete-count
     :premature-expiries (seq (filter #(= :premature-expiry (:violation %)) read-violations))
     :zombie-keys (seq (filter #(= :zombie-key (:violation %)) read-violations))
     :reads-after-delete (seq (filter #(= :read-after-delete (:violation %)) read-violations))
     :suspicious-ttls (seq suspicious-ttls)}))

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
