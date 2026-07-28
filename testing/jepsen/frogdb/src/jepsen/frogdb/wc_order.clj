(ns jepsen.frogdb.wc-order
  "Within-connection command-ordering workload for FrogDB.

   Verifies the [Design intent] guarantee from consistency.md (Ordering ->
   Within a Single Connection): commands execute in the order sent, pipelining
   preserves order, and responses return in order.

   Design: each worker holds a dedicated single-connection pool and a private
   key. Each op pipelines a batch of N ordered RPUSH commands (payloads
   0,1,..,N-1) in a single round-trip, then reads the list back with LRANGE. Two
   independent orderings are asserted:

   - Server-observed execution order: LRANGE must return [0,1,..,N-1]. The list's
     stored order is exactly the order the server executed the pipelined pushes,
     so any reordering of execution shows up as an out-of-order list.
   - Response order: each pipelined RPUSH returns the list length after it ran, so
     the batch of return values must be [1,2,..,N] — responses arriving out of
     order (or coalesced) would break the strictly increasing sequence.

   Operation:
   - :ordered-pipeline — DEL key; pipeline N RPUSHes; LRANGE; records
                         {:n N :push-lengths [...] :list [...]}.

   Checker: custom; :valid? iff every op's list == [0..N-1] and push-lengths ==
   [1..N], with at least one op completed."
  (:require [clojure.tools.logging :refer [info warn debug]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [taoensso.carmine :as car :refer [wcar]]))

(def batch-size 50)

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord WCOrderClient [conn node docker-host? lkey]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)
          k (str "jepsen-wcorder-" (java.util.UUID/randomUUID))]
      (info "Opening within-connection-order client to" node "key" k)
      (assoc this
             :conn (frogdb/conn-spec-single node frogdb/default-port docker? base-port)
             :node node
             :docker-host? docker?
             :lkey k)))

  (setup! [this test]
    (wcar conn (car/del lkey))
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (case (:f op)
        :ordered-pipeline
        (let [n (:value op)
              ;; Fresh list, then N ordered RPUSHes pipelined in ONE round-trip.
              ;; car/return-nil trick isn't needed — wcar returns the vector of
              ;; replies in send order. The leading DEL reply is dropped.
              replies (wcar conn
                            (car/del lkey)
                            (doseq [i (range n)]
                              (car/rpush lkey (str i))))
              ;; replies = [del-reply push1 push2 ... pushN]; drop the DEL reply.
              push-lengths (mapv frogdb/parse-value (rest replies))
              stored (wcar conn (car/lrange lkey 0 -1))]
          (assoc op :type :ok
                 :value {:n n
                         :push-lengths push-lengths
                         :list (vec stored)})))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (frogdb/close-conn! conn)))

(defn create-client
  "Create a new within-connection-ordering client."
  []
  (map->WCOrderClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn pipeline-op [] {:type :invoke :f :ordered-pipeline :value batch-size})

(defn generator
  "Generator for within-connection ordering operations."
  [opts]
  (let [rate (get opts :rate 10)]
    (->> (repeatedly pipeline-op)
         (gen/stagger (/ 1 rate)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn checker
  "Within-connection ordering checker.

   For every completed op both orderings must hold exactly:
   - :list == [\"0\" \"1\" ... \"(n-1)\"]  (server executed pushes in send order)
   - :push-lengths == [1 2 ... n]          (responses returned in send order)
   Requires at least one completed op."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [oks (->> history
                     (filter #(= :ok (:type %)))
                     (filter #(= :ordered-pipeline (:f %))))
            violations
            (->> oks
                 (keep (fn [op]
                         (let [{:keys [n push-lengths list]} (:value op)
                               expected-list (mapv str (range n))
                               expected-lengths (vec (map inc (range n)))
                               list-ok? (= list expected-list)
                               lengths-ok? (= push-lengths expected-lengths)]
                           (when-not (and list-ok? lengths-ok?)
                             {:process (:process op)
                              :list-ordered? list-ok?
                              :responses-ordered? lengths-ok?
                              :list list
                              :push-lengths push-lengths}))))
                 vec)
            n (count oks)]
        {:valid? (and (pos? n) (empty? violations))
         :ordered-ops n
         :violation-count (count violations)
         :violations (vec (take 20 violations))}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a within-connection ordering workload.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (generator opts)
   ;; Own final phase so the harness's default {:f :read} recovery generator
   ;; (which this client does not implement) never runs against this workload.
   :final-generator (->> (repeatedly pipeline-op) (gen/limit 10) (gen/stagger 0.1))
   :checker (checker)})
