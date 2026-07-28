(ns jepsen.frogdb.ryw
  "Read-your-writes (RYW) workload for FrogDB.

   Verifies the [Design intent] guarantee from consistency.md: a client sees its
   own writes on the SAME connection to the same node. This follows from
   per-connection in-order execution and single-key linearizability, but had no
   dedicated test.

   Design: each worker holds a dedicated single-connection pool (conn-spec-single)
   AND a private key derived from its client id (a UUID minted in open!). Because
   no other worker ever writes that key, a read that immediately follows a write
   on the same connection MUST observe exactly the value just written — anything
   else is a read-your-writes violation. A monotonically increasing value is used
   so the checker can also confirm reads never regress on the connection
   (monotonic-reads on a single connection).

   Operation:
   - :ryw — SET key v then GET key on the same connection; records
            {:written v :read r}. The checker asserts r == v for every op.

   Checker: custom. :valid? iff every :ok op read back exactly what it wrote and
   at least one op completed (so an empty history cannot pass vacuously)."
  (:require [clojure.tools.logging :refer [info warn debug]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [taoensso.carmine :as car :refer [wcar]]))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord RYWClient [conn node docker-host? rkey counter]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)
          ;; Private key per connection: no other worker writes it, so a
          ;; same-connection read must observe this connection's last write.
          k (str "jepsen-ryw-" (java.util.UUID/randomUUID))]
      (info "Opening RYW client to" node "key" k)
      (assoc this
             :conn (frogdb/conn-spec-single node frogdb/default-port docker? base-port)
             :node node
             :docker-host? docker?
             :rkey k
             :counter (atom 0))))

  (setup! [this test]
    (wcar conn (car/del rkey))
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (case (:f op)
        :ryw
        (let [v (swap! counter inc)
              ;; SET then GET pipelined in a single round-trip on THIS connection.
              ;; Same connection, same node, no intervening op — read-your-writes
              ;; requires the GET to return exactly v.
              [_ r] (wcar conn
                          (car/set rkey (str v))
                          (car/get rkey))
              read-v (frogdb/parse-value r)]
          (assoc op :type :ok :value {:written v :read read-v})))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (frogdb/close-conn! conn)))

(defn create-client
  "Create a new RYW client."
  []
  (map->RYWClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn ryw-op [] {:type :invoke :f :ryw :value nil})

(defn generator
  "Generator for RYW operations."
  [opts]
  (let [rate (get opts :rate 10)]
    (->> (repeatedly ryw-op)
         (gen/stagger (/ 1 rate)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn checker
  "Read-your-writes checker.

   For every completed :ryw op the value read on the same connection must equal
   the value written on that connection immediately prior. Any mismatch is a RYW
   violation. Requires at least one completed op so an empty/failed history
   cannot pass vacuously."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [oks (->> history
                     (filter #(= :ok (:type %)))
                     (filter #(= :ryw (:f %))))
            violations (->> oks
                            (keep (fn [op]
                                    (let [{:keys [written read]} (:value op)]
                                      (when (not= written read)
                                        {:process (:process op)
                                         :written written
                                         :read read}))))
                            vec)
            n (count oks)]
        {:valid? (and (pos? n) (empty? violations))
         :ryw-ops n
         :violation-count (count violations)
         ;; Cap the reported sample so a pathological run doesn't dump megabytes.
         :violations (vec (take 20 violations))}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a read-your-writes workload.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (generator opts)
   ;; Own final phase so the harness's default {:f :read} recovery generator
   ;; (which this client does not implement) never runs against this workload.
   :final-generator (->> (repeatedly ryw-op) (gen/limit 10) (gen/stagger 0.1))
   :checker (checker)})
