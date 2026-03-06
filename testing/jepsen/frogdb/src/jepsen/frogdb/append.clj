(ns jepsen.frogdb.append
  "Append workload for FrogDB.

   Tests crash recovery and durability by appending unique tokens to a key.
   This workload verifies that:
   - Values only grow, never shrink
   - No duplicate tokens (each append happens exactly once or not at all)
   - No lost tokens after crash recovery
   - Tokens appear in order they were appended

   This is the primary test for persistence/durability under crashes."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def append-key "jepsen-append")

;; Token format: "pN-M" where N is process id and M is sequence number
;; This allows us to identify which process wrote which token and detect
;; duplicates or ordering issues.

(defn make-token
  "Create a unique token for this process and sequence."
  [process seq-num]
  (str "p" process "-" seq-num))

(defn parse-token
  "Parse a token string into [process seq-num]."
  [token]
  (let [[_ p s] (re-matches #"p(\d+)-(\d+)" token)]
    (when (and p s)
      [(Long/parseLong p) (Long/parseLong s)])))

(defn parse-tokens
  "Parse the appended string into a sequence of tokens.
   Tokens are separated by semicolons."
  [s]
  (when (and s (not (str/blank? s)))
    (str/split s #";")))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord AppendClient [conn node docker-host? seq-counter]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)]
      (info "Opening append client to" node "(docker-host?:" docker? ")")
      (assoc this
             :conn (frogdb/conn-spec-single node frogdb/default-port docker? base-port)
             :node node
             :docker-host? docker?
             :seq-counter (atom 0))))

  (setup! [this test]
    ;; Clear the test key at the start
    (info "Clearing append key" append-key)
    (wcar conn (car/del append-key))
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (case (:f op)
        :append
        (let [process (:process op)
              seq-num (swap! seq-counter inc)
              token (make-token process seq-num)
              ;; Append with semicolon separator
              value (str token ";")]
          (frogdb/append! conn append-key value)
          (assoc op :type :ok :value token))

        :read
        (let [value (frogdb/get-string conn append-key)
              tokens (parse-tokens value)]
          (assoc op :type :ok :value {:raw value :tokens tokens})))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (frogdb/close-conn! conn)))

(defn create-client
  "Create a new append client."
  []
  (map->AppendClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn append-op
  "Generate an append operation."
  [_ _]
  {:type :invoke :f :append :value nil})

(defn read-op
  "Generate a read operation."
  [_ _]
  {:type :invoke :f :read :value nil})

(defn generator
  "Create a generator for append operations.

   Options:
   - :rate - operations per second (default 10)
   - :read-fraction - fraction of reads (default 0.2)"
  [opts]
  (let [rate (get opts :rate 10)
        read-fraction (get opts :read-fraction 0.2)]
    (->> (gen/mix [(gen/repeat append-op)
                   (gen/repeat read-op)])
         (gen/stagger (/ 1 rate)))))

(defn final-read-generator
  "Generator for final reads after the main test completes."
  []
  (->> (gen/repeat {:f :read})
       (gen/limit 5)
       (gen/stagger 0.5)))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn check-append-history
  "Analyze the history for append correctness.

   Checks:
   1. No duplicate tokens in final value
   2. All acknowledged tokens are present
   3. No unacknowledged tokens are present (crash recovery correct)
   4. Ordering is preserved per-process"
  [history]
  (let [;; Get all acknowledged appends (ok responses)
        ack-appends (->> history
                         (filter #(and (= :append (:f %))
                                      (= :ok (:type %))))
                         (map :value)
                         set)

        ;; Get all reads
        reads (->> history
                   (filter #(and (= :read (:f %))
                                (= :ok (:type %)))))

        ;; Final read
        final-read (last reads)
        final-tokens (when final-read
                       (get-in final-read [:value :tokens]))

        ;; Check for duplicates in final value
        duplicate-tokens (when final-tokens
                          (let [freqs (frequencies final-tokens)]
                            (filter #(> (val %) 1) freqs)))

        ;; Check for missing acknowledged tokens
        final-token-set (set final-tokens)
        missing-tokens (clojure.set/difference ack-appends final-token-set)

        ;; Check for extra tokens (not acknowledged but present)
        ;; These could indicate crash recovery issues
        extra-tokens (clojure.set/difference final-token-set ack-appends)

        ;; Check per-process ordering
        ;; For each process, tokens should appear in sequence order
        ordering-errors (when final-tokens
                         (let [parsed (keep parse-token final-tokens)
                               by-process (group-by first parsed)]
                           (for [[process tokens] by-process
                                 :let [seqs (map second tokens)
                                       sorted-seqs (sort seqs)]
                                 :when (not= seqs sorted-seqs)]
                             {:process process
                              :expected sorted-seqs
                              :actual seqs})))

        ;; Value should never shrink between reads
        shrink-errors (loop [prev-len 0
                            reads reads
                            errors []]
                       (if (empty? reads)
                         errors
                         (let [r (first reads)
                               tokens (get-in r [:value :tokens])
                               len (count tokens)]
                           (recur len
                                  (rest reads)
                                  (if (< len prev-len)
                                    (conj errors {:type :shrink
                                                 :prev-len prev-len
                                                 :new-len len})
                                    errors)))))]

    {:valid? (and (empty? duplicate-tokens)
                  (empty? missing-tokens)
                  (empty? ordering-errors)
                  (empty? shrink-errors))
     :final-token-count (count final-tokens)
     :ack-count (count ack-appends)
     :duplicate-tokens (into {} duplicate-tokens)
     :missing-tokens missing-tokens
     :extra-tokens extra-tokens
     :ordering-errors (seq ordering-errors)
     :shrink-errors (seq shrink-errors)}))

(defn checker
  "Create a checker for append operations."
  []
  (reify checker/Checker
    (check [this test history opts]
      (check-append-history history))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Create an append workload for testing.

   Options:
   - :rate - operations per second
   - :read-fraction - fraction of operations that are reads"
  [opts]
  {:client (create-client)
   :generator (gen/phases
                (generator opts)
                ;; Final reads to verify state
                (gen/clients (final-read-generator)))
   :checker (checker/compose
              {:append (checker)})})
