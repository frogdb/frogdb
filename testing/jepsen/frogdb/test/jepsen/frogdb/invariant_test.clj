(ns jepsen.frogdb.invariant-test
  "Deterministic tests for the live invariant-sweep checker
   (`jepsen.frogdb.invariant`), fed synthetic histories.

   The sweep itself needs a running topology, but everything the checker
   decides — how many sweeps ran, which ids gate the verdict, which are
   allowlisted against an open issue, and which surface's ops it reads — is
   pure history analysis and is pinned here.

   The `:sweeps-run` tests exist because the cluster wiring shipped with a
   real counting bug: every sweep op appears TWICE in a Jepsen history (the
   nemesis dispatch entry and its completion), both with the same `:f` and
   both `:type :info`, so filtering on `:f` alone counted each sweep once per
   phase. Replication issue 13 carries that fix over to the parameterized
   surface, so both halves are asserted directly."
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [jepsen.frogdb.invariant :as invariant]))

;; ===========================================================================
;; Synthetic history helpers
;; ===========================================================================

(defn- dispatch
  "The nemesis dispatch entry for a sweep: same :f, same :type, no :value yet."
  [surface]
  {:type :info :f (:op-f surface) :process :nemesis})

(defn- completion
  "A completed sweep, carrying the {node -> result} map."
  [surface results]
  {:type :info :f (:op-f surface) :process :nemesis :value results})

(defn- clean [] {:violations []})
(defn- violating [& ids] {:violations (mapv (fn [id] {:id id :detail (str id " detail")}) ids)})
(defn- unreachable [msg] {:error msg})

(defn- check
  [surface history]
  (checker/check (invariant/checker surface) {} history {}))

;; ===========================================================================
;; Sweep counting — the dispatch/completion double-count
;; ===========================================================================

(deftest sweeps-are-counted-once-not-once-per-phase
  (testing "a dispatch + completion pair is ONE sweep"
    (doseq [surface [invariant/cluster-surface invariant/replication-surface]]
      (let [r (check surface
                     [(dispatch surface)
                      (completion surface {"n1" (clean) "n2" (clean) "n3" (clean)})
                      (dispatch surface)
                      (completion surface {"n1" (clean) "n2" (clean) "n3" (clean)})])]
        (is (true? (:valid? r)) (str (:label surface) " clean sweeps are valid"))
        (is (= 2 (:sweeps-run r)) (str (:label surface) " counts 2 sweeps, not 4"))
        (is (= 0 (:violating-sweeps r)))))))

(deftest a-sweep-with-no-completion-is-not-counted
  (testing "a dispatch that never completed contributes no sweep"
    (let [r (check invariant/replication-surface
                   [(dispatch invariant/replication-surface)])]
      (is (true? (:valid? r)))
      (is (= 0 (:sweeps-run r))))))

;; ===========================================================================
;; Violations gate the verdict and name their ids
;; ===========================================================================

(deftest a-violation-on-any-node-fails-the-run
  (testing "one violating node in one sweep -> :valid? false, id surfaced"
    (let [s invariant/replication-surface
          r (check s [(dispatch s)
                      (completion s {"n1" (clean) "n2" (clean) "n3" (clean)})
                      (dispatch s)
                      (completion s {"n1" (violating "INV-OFFSET-3")
                                     "n2" (clean)
                                     "n3" (clean)})])]
      (is (false? (:valid? r)))
      (is (= 2 (:sweeps-run r)))
      (is (= 1 (:violating-sweeps r)))
      (is (= ["INV-OFFSET-3"] (:violation-ids r))))))

(deftest violation-ids-are-deduplicated-across-nodes-and-sweeps
  (let [s invariant/replication-surface
        r (check s [(dispatch s)
                    (completion s {"n1" (violating "INV-FENCE-1" "INV-ROLE-1")
                                   "n2" (violating "INV-FENCE-1")})
                    (dispatch s)
                    (completion s {"n1" (violating "INV-FENCE-1")})])]
    (is (false? (:valid? r)))
    (is (= 2 (:violating-sweeps r)))
    (is (= #{"INV-FENCE-1" "INV-ROLE-1"} (set (:violation-ids r))))))

;; ===========================================================================
;; Connectivity errors are reported, never gating
;; ===========================================================================

(deftest an-unreachable-node-is-reported-but-does-not-fail-the-run
  (let [s invariant/replication-surface
        r (check s [(dispatch s)
                    (completion s {"n1" (clean)
                                   "n2" (unreachable "Connection refused")})])]
    (is (true? (:valid? r)))
    (is (= 1 (:sweeps-run r)))
    (is (= 1 (:connectivity-errors r)))))

;; ===========================================================================
;; The known-violation allowlist
;; ===========================================================================

(deftest an-allowlisted-id-is-reported-but-does-not-gate
  (let [s (assoc invariant/replication-surface
                 :known {"INV-FENCE-1" "issue 19 — self-fence arms only on the write path"})
        r (check s [(dispatch s)
                    (completion s {"n1" (violating "INV-FENCE-1") "n2" (clean)})])]
    (is (true? (:valid? r)))
    (is (= 0 (:violating-sweeps r)))
    (is (= [] (:violation-ids r)))
    (is (= ["INV-FENCE-1"] (:known-violation-ids r)))
    (is (= 1 (:known-violations r)))
    (is (contains? (:known-issues r) "INV-FENCE-1"))))

(deftest an-allowlist-does-not-mask-other-ids-in-the-same-sweep
  (let [s (assoc invariant/replication-surface :known {"INV-FENCE-1" "issue 19"})
        r (check s [(dispatch s)
                    (completion s {"n1" (violating "INV-FENCE-1" "INV-BACKLOG-2")})])]
    (is (false? (:valid? r)))
    (is (= ["INV-BACKLOG-2"] (:violation-ids r)))
    (is (= ["INV-FENCE-1"] (:known-violation-ids r)))))

(deftest a-clean-run-reports-no-known-keys-at-all
  (testing "the :known-* keys stay absent when nothing was allowlisted"
    (let [s (assoc invariant/replication-surface :known {"INV-FENCE-1" "issue 19"})
          r (check s [(dispatch s) (completion s {"n1" (clean)})])]
      (is (true? (:valid? r)))
      (is (not (contains? r :known-violation-ids))))))

;; ===========================================================================
;; Surface isolation — each checker reads only its own ops
;; ===========================================================================

(deftest each-surface-reads-only-its-own-sweep-ops
  (let [cs invariant/cluster-surface
        rs invariant/replication-surface
        history [(dispatch cs) (completion cs {"n1" (violating "INV-CLUSTER-X")})
                 (dispatch rs) (completion rs {"n1" (clean)})]]
    (testing "the replication checker ignores DEBUG CLUSTER CHECK ops"
      (let [r (check rs history)]
        (is (true? (:valid? r)))
        (is (= 1 (:sweeps-run r)))))
    (testing "the cluster checker ignores DEBUG REPLICATION CHECK ops"
      (let [r (check cs history)]
        (is (false? (:valid? r)))
        (is (= 1 (:sweeps-run r)))
        (is (= ["INV-CLUSTER-X"] (:violation-ids r)))))))

(deftest surfaces-differ-in-command-op-and-result-key
  (is (= ["DEBUG" "CLUSTER" "CHECK"] (:command invariant/cluster-surface)))
  (is (= ["DEBUG" "REPLICATION" "CHECK"] (:command invariant/replication-surface)))
  (is (not= (:op-f invariant/cluster-surface) (:op-f invariant/replication-surface)))
  (is (not= (:result-key invariant/cluster-surface)
            (:result-key invariant/replication-surface))))
