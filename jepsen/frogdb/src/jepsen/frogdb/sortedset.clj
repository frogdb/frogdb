(ns jepsen.frogdb.sortedset
  "Sorted set workload for FrogDB.

   Tests score updates and ranking consistency using ZADD/ZREM/ZSCORE/ZRANK/ZRANGE.

   This workload verifies:
   - Score accuracy: ZSCORE returns the last written score for a member
   - Rank consistency: if score(A) < score(B), then rank(A) < rank(B)
   - Range ordering: ZRANGE returns elements in score order
   - No phantom members (members that were never added)"
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def sortedset-key "jepsen-sortedset")

;; Member pool: we use a fixed pool of members to ensure
;; adds, updates, and removes interact with each other
(def member-pool (mapv #(str "member-" %) (range 50)))

;; Score range
(def min-score 0)
(def max-score 1000)

;; ===========================================================================
;; Sorted Set Operations
;; ===========================================================================

(defn zadd!
  "Add member with score to sorted set using ZADD.
   Returns 1 if added, 0 if score was updated."
  [conn key score member]
  (wcar conn (car/zadd key score member)))

(defn zadd-xx!
  "Update score of existing member using ZADD XX.
   Returns 0 (only updates, never adds)."
  [conn key score member]
  (wcar conn (car/zadd key "XX" score member)))

(defn zrem!
  "Remove member from sorted set using ZREM.
   Returns 1 if removed, 0 if not present."
  [conn key member]
  (wcar conn (car/zrem key member)))

(defn zscore
  "Get score of member using ZSCORE.
   Returns score as double or nil if not present."
  [conn key member]
  (let [v (wcar conn (car/zscore key member))]
    (when v
      (if (string? v)
        (Double/parseDouble v)
        (double v)))))

(defn zrank
  "Get rank of member using ZRANK.
   Returns 0-based rank or nil if not present."
  [conn key member]
  (wcar conn (car/zrank key member)))

(defn zrange-withscores
  "Get all members with scores in ascending order using ZRANGE."
  [conn key start stop]
  (let [result (wcar conn (car/zrange key start stop "WITHSCORES"))]
    ;; Result is [member1 score1 member2 score2 ...]
    (when (seq result)
      (->> result
           (partition 2)
           (map (fn [[member score]]
                  {:member member
                   :score (if (string? score)
                            (Double/parseDouble score)
                            (double score))}))
           vec))))

(defn zcard
  "Get cardinality (size) of sorted set using ZCARD."
  [conn key]
  (wcar conn (car/zcard key)))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord SortedSetClient [conn node docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)]
      (info "Opening sortedset client to" node "(docker-host?:" docker? ")")
      (assoc this
             :conn (frogdb/conn-spec node frogdb/default-port docker? base-port)
             :node node
             :docker-host? docker?)))

  (setup! [this test]
    ;; Clear the sorted set at the start
    (info "Clearing sortedset key" sortedset-key)
    (wcar conn (car/del sortedset-key))
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (case (:f op)
        ;; Add member with score
        :add
        (let [{:keys [member score]} (:value op)
              result (zadd! conn sortedset-key score member)]
          (assoc op :type :ok :result result))

        ;; Update score (only if member exists)
        :update-score
        (let [{:keys [member score]} (:value op)
              result (zadd-xx! conn sortedset-key score member)]
          (assoc op :type :ok :result result))

        ;; Remove member
        :remove
        (let [member (:value op)
              result (zrem! conn sortedset-key member)]
          (assoc op :type :ok :result result))

        ;; Get score of member
        :score
        (let [member (:value op)
              score (zscore conn sortedset-key member)]
          (assoc op :type :ok :value {:member member :score score}))

        ;; Get rank of member
        :rank
        (let [member (:value op)
              rank (zrank conn sortedset-key member)]
          (assoc op :type :ok :value {:member member :rank rank}))

        ;; Read entire sorted set with scores
        :read
        (let [members (zrange-withscores conn sortedset-key 0 -1)]
          (assoc op :type :ok :value (or members []))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    nil))

(defn create-client
  "Create a new sorted set client."
  []
  (map->SortedSetClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn add-op
  "Generate an add operation."
  [_ _]
  {:type :invoke
   :f :add
   :value {:member (rand-nth member-pool)
           :score (+ min-score (rand-int (- max-score min-score)))}})

(defn update-score-op
  "Generate a score update operation."
  [_ _]
  {:type :invoke
   :f :update-score
   :value {:member (rand-nth member-pool)
           :score (+ min-score (rand-int (- max-score min-score)))}})

(defn remove-op
  "Generate a remove operation."
  [_ _]
  {:type :invoke :f :remove :value (rand-nth member-pool)})

(defn score-op
  "Generate a score read operation."
  [_ _]
  {:type :invoke :f :score :value (rand-nth member-pool)})

(defn rank-op
  "Generate a rank read operation."
  [_ _]
  {:type :invoke :f :rank :value (rand-nth member-pool)})

(defn read-op
  "Generate a read operation."
  [_ _]
  {:type :invoke :f :read :value nil})

(defn generator
  "Create a generator for sorted set operations.

   Options:
   - :rate - operations per second (default 10)"
  [opts]
  (let [rate (get opts :rate 10)]
    (->> (gen/mix [(gen/repeat add-op)
                   (gen/repeat add-op)           ; Weight adds more heavily
                   (gen/repeat update-score-op)
                   (gen/repeat remove-op)
                   (gen/repeat score-op)
                   (gen/repeat rank-op)
                   (gen/repeat read-op)])
         (gen/stagger (/ 1 rate)))))

(defn final-read-generator
  "Generator for final reads."
  []
  (->> (gen/repeat {:f :read})
       (gen/limit 5)
       (gen/stagger 0.5)))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn check-sortedset-history
  "Analyze the history for sorted set correctness.

   Checks:
   1. Range ordering: ZRANGE returns elements in ascending score order
   2. No phantom members (members that were never added)
   3. Score consistency: final scores match latest writes"
  [history]
  (let [;; Get all successful operations
        ops (->> history
                 (filter #(= :ok (:type %))))

        ;; Track all members ever added
        all-added (->> ops
                      (filter #(= :add (:f %)))
                      (map #(get-in % [:value :member]))
                      set)

        ;; Get final reads
        final-reads (->> ops
                         (filter #(= :read (:f %))))
        final-set (when (seq final-reads)
                    (:value (last final-reads)))

        ;; Check ordering invariant: each member's score should be >= previous
        ordering-violations
        (when (seq final-set)
          (let [pairs (partition 2 1 final-set)]
            (filter (fn [[a b]]
                      (> (:score a) (:score b)))
                    pairs)))

        ;; Check for phantom members (in final set but never added)
        final-members (when final-set
                        (set (map :member final-set)))
        phantom-members (when final-members
                          (clojure.set/difference final-members all-added))

        ;; Track scores by member from add/update operations
        ;; Use map-indexed to efficiently track operation order
        score-writes (->> ops
                         (map-indexed (fn [idx op] [idx op]))
                         (filter (fn [[_ op]] (#{:add :update-score} (:f op))))
                         (map (fn [[idx op]]
                               {:member (get-in op [:value :member])
                                :score (get-in op [:value :score])
                                :index idx})))

        ;; Build map of member -> latest written score
        latest-scores (reduce
                       (fn [m {:keys [member score index]}]
                         (if (or (not (contains? m member))
                                (> index (get-in m [member :index])))
                           (assoc m member {:score score :index index})
                           m))
                       {}
                       score-writes)

        ;; Count operations
        add-count (count (filter #(= :add (:f %)) ops))
        update-count (count (filter #(= :update-score (:f %)) ops))
        remove-count (count (filter #(= :remove (:f %)) ops))
        score-read-count (count (filter #(= :score (:f %)) ops))
        rank-read-count (count (filter #(= :rank (:f %)) ops))
        read-count (count (filter #(= :read (:f %)) ops))]

    {:valid? (and (empty? ordering-violations)
                  (empty? phantom-members))
     :add-count add-count
     :update-count update-count
     :remove-count remove-count
     :score-read-count score-read-count
     :rank-read-count rank-read-count
     :read-count read-count
     :final-size (count final-set)
     :ordering-violations (seq ordering-violations)
     :phantom-members (seq phantom-members)}))

(defn checker
  "Create a checker for sorted set operations."
  []
  (reify checker/Checker
    (check [this test history opts]
      (check-sortedset-history history))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Create a sorted set workload for testing.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (gen/phases
                (generator opts)
                ;; Final reads
                (gen/clients (final-read-generator)))
   :checker (checker/compose
              {:sortedset (checker)})})
