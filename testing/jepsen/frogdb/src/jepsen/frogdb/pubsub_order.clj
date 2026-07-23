(ns jepsen.frogdb.pubsub-order
  "Pub/Sub message-ordering workload for FrogDB.

   Verifies the [Design intent] guarantee from consistency.md (Ordering ->
   Pub/Sub Message Ordering): messages are delivered in publish order per
   channel, with at-most-once delivery (messages may be lost on reconnect).

   Design: each op works on a private channel (UUID) so ops never interfere. A
   dedicated subscriber connection (Carmine pubsub listener) records every
   delivered payload in arrival order; a separate publisher connection publishes
   the monotonically increasing payloads 0,1,..,N-1 in order. The op then asserts
   the arrival sequence is a strictly increasing (in-publish-order) subsequence
   of what was published — delivery may DROP messages (at-most-once) but must
   never REORDER them.

   Two flavours share one checker:
   - :pub-order            — single subscriber, single publisher, no reconnect.
   - :pub-order (reconnect) — the subscriber closes and re-subscribes to the same
                              channel midway; messages published while
                              unsubscribed may be lost, but everything actually
                              delivered must still be strictly increasing. This
                              exercises the harder cross-reconnect case.

   Checker: custom. Each op's received payloads must be strictly increasing and
   within [0,N). :valid? additionally requires that the run delivered at least
   one message overall, so a run where the subscriber never connected cannot pass
   vacuously."
  (:require [clojure.tools.logging :refer [info warn debug]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [taoensso.carmine :as car :refer [wcar]]))

(def message-count 20)
(def subscribe-timeout-ms 3000)

;; ===========================================================================
;; Pub/Sub helpers
;; ===========================================================================

(defn- resolve-spec
  "Bare Carmine connection spec map (host/port/timeout) for a node.
   Carmine's pubsub listener wants the spec map itself, not the pool wrapper."
  [node docker-host? base-port]
  (:spec (frogdb/conn-for-node node docker-host? base-port)))

(defn- open-listener
  "Open a pubsub listener subscribed to `channel`. Payloads for 'message' events
   are appended to the `received` atom (a vector). `ready` is a promise delivered
   once the subscription is confirmed. Returns the listener."
  [spec channel received ready]
  (car/with-new-pubsub-listener spec
    {channel (fn [msg]
               (let [[kind _ch payload] msg]
                 (cond
                   (= kind "subscribe") (deliver ready true)
                   (= kind "message") (swap! received conj payload))))}
    (car/subscribe channel)))

(defn- drain!
  "Wait for the listener thread to finish delivering. Polls the received count
   until it is stable across two samples, or `max-ms` elapses."
  [received max-ms]
  (let [deadline (+ (System/currentTimeMillis) max-ms)]
    (loop [prev -1]
      (Thread/sleep 150)
      (let [now (count @received)]
        (when (and (or (not= now prev) (neg? prev))
                   (< (System/currentTimeMillis) deadline))
          (recur now))))))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord PubSubOrderClient [pub-conn node docker-host? base-port]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          bp (get test :base-port frogdb/default-base-port)]
      (info "Opening pubsub-order client to" node)
      (assoc this
             :pub-conn (frogdb/conn-spec-single node frogdb/default-port docker? bp)
             :node node
             :docker-host? docker?
             :base-port bp)))

  (setup! [this test] this)

  (invoke! [this test op]
    (case (:f op)
      :pub-order
      (let [reconnect? (:reconnect op)
            channel (str "jepsen-pubsub-" (java.util.UUID/randomUUID))
            spec (resolve-spec node docker-host? base-port)
            received (atom [])
            listener (atom nil)]
        (try
          (frogdb/with-error-handling op
            (let [ready (promise)
                  l (open-listener spec channel received ready)]
              (reset! listener l)
              (deref ready subscribe-timeout-ms nil)
              (if reconnect?
                ;; Reconnect flavour: publish first half, drop the subscription,
                ;; re-subscribe, publish the second half. Messages published while
                ;; unsubscribed are legitimately lost (at-most-once).
                (let [half (quot message-count 2)]
                  (dotimes [i half]
                    (wcar pub-conn (car/publish channel (str i))))
                  (drain! received 1500)
                  (car/close-listener l)
                  (reset! listener nil)
                  (let [ready2 (promise)
                        l2 (open-listener spec channel received ready2)]
                    (reset! listener l2)
                    (deref ready2 subscribe-timeout-ms nil)
                    (doseq [i (range half message-count)]
                      (wcar pub-conn (car/publish channel (str i))))
                    (drain! received 1500)))
                ;; Simple flavour: publish all N in order to one live subscriber.
                (do
                  (dotimes [i message-count]
                    (wcar pub-conn (car/publish channel (str i))))
                  (drain! received 1500)))
              (assoc op :type :ok
                     :value {:published message-count
                             :reconnect (boolean reconnect?)
                             :received (mapv frogdb/parse-value @received)})))
          (finally
            (when-let [l @listener]
              (try (car/close-listener l) (catch Exception _))))))))

  (teardown! [this test] nil)

  (close! [this test]
    (frogdb/close-conn! pub-conn)))

(defn create-client
  "Create a new pubsub-order client."
  []
  (map->PubSubOrderClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn pub-op [] {:type :invoke :f :pub-order})
(defn reconnect-op [] {:type :invoke :f :pub-order :reconnect true})

(defn generator
  "Generator: mostly plain publish-order ops with occasional reconnect ops."
  [opts]
  (let [rate (get opts :rate 5)]
    (->> (gen/mix [(fn [] (pub-op))
                   (fn [] (pub-op))
                   (fn [] (reconnect-op))])
         (gen/stagger (/ 1 rate)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

(defn- strictly-increasing?
  "True iff xs is strictly increasing (each element > the one before)."
  [xs]
  (every? (fn [[a b]] (< a b)) (partition 2 1 xs)))

(defn checker
  "Pub/Sub ordering checker.

   For each completed op the received payloads must be a strictly increasing
   subsequence of [0,N) — delivery may drop messages but must never reorder them
   or deliver a value outside the published range. :valid? also requires that the
   whole run delivered at least one message, so a subscriber that never connected
   cannot pass vacuously."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [oks (->> history
                     (filter #(= :ok (:type %)))
                     (filter #(= :pub-order (:f %))))
            total-received (reduce + 0 (map #(count (:received (:value %))) oks))
            violations
            (->> oks
                 (keep (fn [op]
                         (let [{:keys [published received]} (:value op)
                               in-range? (every? #(and (integer? %)
                                                       (<= 0 %) (< % published))
                                                 received)
                               ordered? (strictly-increasing? received)]
                           (when-not (and in-range? ordered?)
                             {:process (:process op)
                              :reconnect (:reconnect (:value op))
                              :in-range? in-range?
                              :ordered? ordered?
                              :received received}))))
                 vec)
            n (count oks)]
        {:valid? (and (pos? n) (pos? total-received) (empty? violations))
         :pubsub-ops n
         :total-received total-received
         :violation-count (count violations)
         :violations (vec (take 20 violations))}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a pub/sub ordering workload.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (generator opts)
   ;; Own final phase so the harness's default {:f :read} recovery generator
   ;; (which this client does not implement) never runs against this workload.
   :final-generator (->> (repeatedly pub-op) (gen/limit 5) (gen/stagger 0.2))
   :checker (checker)})
