(ns jepsen.frogdb.error-classification-test
  "Tests for client/with-error-handling's classification of RESP error replies.

   Two properties matter to the linearizable checkers:

   1. A reply the server *declined* (an admission-gate refusal) must land as
      :fail, not :info. An indeterminate op is a pending op, and pending ops on
      a single hot register multiply the Knossos search space — that is what
      forced register-partition down to --concurrency 2 historically.

   2. No reply may escape the macro. `with-error-handling` reads as though an
      unrecognised error falls through to its trailing `(catch Exception ...)`
      :unexpected arm, but a `throw` inside a catch clause propagates past its
      sibling clauses and out of the whole `try`, killing the Jepsen worker.
      SELFFENCE hit exactly this. The catch-all arm keeps a new server-side
      error prefix from reintroducing the crash."
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.frogdb.client :as frogdb]))

(defn- carmine-error
  "Build the ExceptionInfo shape Carmine raises for a RESP error reply: the
   error code lower-cased as :prefix, the human text as :message."
  [prefix message]
  (ex-info message {:prefix prefix :message message}))

(defn- classify
  "Run a body that throws `e` through with-error-handling and return the op."
  [e]
  (frogdb/with-error-handling {:type :invoke :f :write :value 1} (throw e)))

(deftest admission-refusals-are-deterministic-failures
  (testing "a declined write is :fail — the server refused before mutating state"
    (doseq [[prefix message]
            [[:clusterdown "CLUSTERDOWN The cluster is down"]
             [:readonly    "READONLY You can't write against a read only replica."]
             [:masterdown  "MASTERDOWN Link with MASTER is down"]
             [:selffence   (str "SELFFENCE writes rejected: no fresh streaming "
                                "replica (self-fence-on-replica-loss)")]
             [:noreplicas  "NOREPLICAS Not enough good replicas to write."]]]
      (let [op (classify (carmine-error prefix message))]
        (is (= :fail (:type op))
            (str prefix " must be :fail, not the indeterminate :info"))))))

(deftest selffence-is-classified-by-message-when-the-prefix-is-absent
  (testing "classification does not depend on Carmine populating :prefix"
    (let [op (classify (ex-info "SELFFENCE writes rejected: no fresh streaming replica"
                                {:message "SELFFENCE writes rejected"}))]
      (is (= :fail (:type op))))))

(deftest an-unknown-error-code-does-not-escape-the-macro
  (testing "an unrecognised prefix degrades to :info instead of killing the worker"
    ;; Regression: this arm used to `(throw e#)`. A throw from inside a catch
    ;; clause is not caught by a sibling catch clause, so it escaped the whole
    ;; try and crashed the generator's worker thread.
    (let [op (classify (carmine-error :quantumflux "QUANTUMFLUX a code from the future"))]
      (is (= :info (:type op))
          "unknown error must be indeterminate, not an escaped exception")
      (is (= :unclassified (first (:error op)))))))

(deftest transport-failures-keep-their-existing-classification
  (testing "a refused connection never applied; a timeout is indeterminate"
    (is (= :fail (:type (classify (java.net.ConnectException. "refused")))))
    (is (= :info (:type (classify (java.net.SocketTimeoutException. "timed out")))))))
