(ns jepsen.frogdb.weak-checkers-test
  "Deterministic tests for the three single-node checkers strengthened in
   issue 38. Each feeds a synthetic history to the checker to prove the new
   gate fires on a bad history AND does not false-positive on a good one:

   - cluster-formation: a nil/absent final cluster-state must FAIL (it used to
     vacuously pass), and node-count consistency is now asserted.
   - sortedset: the final ZRANGE is now cross-checked against the last tracked
     score per member — score drift must FAIL.
   - hash (--independent): the per-field linearizable independent-checker now
     actually runs, because the independent client emits proper
     `independent/tuple` values (MapEntry) rather than plain vectors the
     independent checker could not key on."
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [jepsen.history :as h]
            [jepsen.independent :as independent]
            [knossos.model :as model]
            [jepsen.frogdb.cluster-formation :as cf]
            [jepsen.frogdb.sortedset :as ss]))

;; ===========================================================================
;; cluster-formation
;; ===========================================================================

(defn- state-op [v] {:type :ok :f :read-cluster-state :value v})
(defn- count-op [v] {:type :ok :f :read-node-count :value v})

(defn- cf-check
  ([history] (cf-check {:nodes ["n1" "n2" "n3"]} history))
  ([test history] (checker/check (cf/checker) test history {})))

(deftest cf-ok-state-consistent-counts-valid
  (testing "final ok + all node counts match cluster size -> valid"
    (let [r (cf-check [(state-op "ok") (count-op 3)
                       (state-op "ok") (count-op 3)])]
      (is (true? (:valid? r)))
      (is (true? (:state-ok? r)))
      (is (true? (:node-count-consistent? r))))))

(deftest cf-nil-final-state-fails
  (testing "no cluster-state read ever ran (nil final-state) must FAIL, not pass"
    ;; This is the core issue-38 hole: the old checker had
    ;; (or (= \"ok\" final-state) (nil? final-state)) so this passed green.
    (let [r (cf-check [(count-op 3) (count-op 3)])]
      (is (false? (:valid? r)) "absent final cluster-state must fail")
      (is (false? (:state-ok? r)))
      (is (nil? (:final-cluster-state r))))))

(deftest cf-fail-final-state-fails
  (testing "a final cluster-state of \"fail\" (never recovered) must FAIL"
    (let [r (cf-check [(state-op "ok") (count-op 3)
                       (state-op "fail") (count-op 3)])]
      (is (false? (:valid? r)))
      (is (false? (:state-ok? r))))))

(deftest cf-inconsistent-counts-fail
  (testing "a split node-count view on a stable cluster must FAIL"
    (let [r (cf-check [(state-op "ok") (count-op 3)
                       (state-op "ok") (count-op 2)])]
      (is (false? (:valid? r)) "3 vs 2 known-nodes view is a membership bug")
      (is (true? (:state-ok? r)))
      (is (false? (:node-count-consistent? r))))))

(deftest cf-zero-count-fails
  (testing "a zero/broken node-count view must FAIL even if it 'agrees'"
    (let [r (cf-check {:nodes []} [(state-op "ok") (count-op 0)])]
      (is (false? (:valid? r)))
      (is (false? (:node-count-consistent? r))))))

(deftest cf-no-counts-fails
  (testing "no node-count reads at all -> not consistent -> FAIL"
    (let [r (cf-check [(state-op "ok")])]
      (is (false? (:valid? r)))
      (is (false? (:node-count-consistent? r))))))

(deftest cf-membership-run-tolerates-varying-counts
  (testing "a membership-change run varies the count by design -> still valid"
    (let [r (cf-check {:nodes ["n1" "n2" "n3"] :membership-changes true}
                      [(state-op "ok") (count-op 3)
                       (count-op 4) (count-op 5)
                       (count-op 4) (state-op "ok") (count-op 3)])]
      (is (true? (:valid? r)) "varying but positive counts are ok under membership churn")
      (is (true? (:node-count-consistent? r))))))

;; ===========================================================================
;; sortedset
;; ===========================================================================

(defn- add-op [member score]
  {:type :ok :f :add :value {:member member :score score}})
(defn- update-op [member score]
  {:type :ok :f :update-score :value {:member member :score score}})
(defn- ss-read-op [pairs]
  {:type :ok :f :read
   :value (mapv (fn [[m s]] {:member m :score (double s)}) pairs)})

(deftest ss-final-scores-match-valid
  (testing "final read matches the last tracked score per member -> valid"
    (let [r (ss/check-sortedset-history
              [(add-op "member-1" 10)
               (add-op "member-2" 20)
               (update-op "member-1" 15)
               (ss-read-op [["member-1" 15] ["member-2" 20]])])]
      (is (true? (:valid? r)))
      (is (nil? (:score-drift r))))))

(deftest ss-score-drift-fails
  (testing "final read where a member's score differs from its last write -> FAIL"
    ;; member-1's last write was 15, but the final ZRANGE reports 10 -> drift.
    (let [r (ss/check-sortedset-history
              [(add-op "member-1" 10)
               (update-op "member-1" 15)
               (ss-read-op [["member-1" 10]])])]
      (is (false? (:valid? r)) "score drift must invalidate the run")
      (is (= 1 (count (:score-drift r))))
      (is (= "member-1" (:member (first (:score-drift r)))))
      (is (== 15 (:expected (first (:score-drift r)))))
      (is (== 10 (:actual (first (:score-drift r))))))))

(deftest ss-phantom-member-still-fails
  (testing "a member in the final read that was never added still FAILs"
    (let [r (ss/check-sortedset-history
              [(add-op "member-1" 10)
               (ss-read-op [["member-1" 10] ["ghost" 5]])])]
      (is (false? (:valid? r)))
      (is (seq (:phantom-members r))))))

(deftest ss-ordering-violation-still-fails
  (testing "a descending final range still FAILs"
    (let [r (ss/check-sortedset-history
              [(add-op "member-1" 10)
               (add-op "member-2" 20)
               (ss-read-op [["member-2" 20] ["member-1" 10]])])]
      (is (false? (:valid? r)))
      (is (seq (:ordering-violations r))))))

;; ===========================================================================
;; hash (--independent path)
;; ===========================================================================
;;
;; The full jepsen.independent/checker writes per-subhistory files to a store
;; (needs a live test map), so here we prove the two properties the issue-38
;; fix depends on, deterministically and without a store:
;;   1. jepsen.independent groups the history by field ONLY when op :values are
;;      proper `independent/tuple`s (MapEntry). The old hash client returned
;;      plain vectors [field v], which `tuple?` rejects, so every op was
;;      un-keyed and the per-field linearizable subhistories collapsed. The new
;;      IndependentHashClient returns `independent/tuple`s, so keying works.
;;   2. On a correctly-keyed per-field register subhistory, the linearizable
;;      (Knossos register) checker passes clean histories and FAILs anomalies.

(defn- t [k v] (independent/tuple k v))

(deftest independent-keys-require-real-tuples
  (testing "tuple-valued ops are keyed by field; plain-vector ops are not"
    (let [tuple-hist (h/history
                       [{:process 0 :type :ok :f :read :value (t :f1 1)}
                        {:process 1 :type :ok :f :read :value (t :f2 2)}])
          vector-hist (h/history
                        [{:process 0 :type :ok :f :read :value [:f1 1]}
                         {:process 1 :type :ok :f :read :value [:f2 2]}])]
      (is (= #{:f1 :f2} (independent/history-keys tuple-hist))
          "MapEntry tuples (new client) are grouped per field")
      (is (empty? (independent/history-keys vector-hist))
          "plain vectors (old client) collapse to no keys -> checker saw nothing"))))

(defn- lin-check [ops]
  ;; The per-field register model, as jepsen.independent/checker runs it on each
  ;; unwrapped subhistory.
  (checker/check (checker/linearizable {:model (model/register)
                                        :algorithm :linear})
                 {} (h/history ops) {}))

(deftest per-field-register-linearizable-valid
  (testing "write then read same value on a field -> linearizable"
    (let [r (lin-check
              [{:process 0 :type :invoke :f :write :value 1}
               {:process 0 :type :ok     :f :write :value 1}
               {:process 0 :type :invoke :f :read  :value nil}
               {:process 0 :type :ok     :f :read  :value 1}])]
      (is (true? (:valid? r))))))

(deftest per-field-register-nonlinearizable-fails
  (testing "a field read returning a never-written value -> FAIL"
    ;; only 1 was written, but 2 is read back with no concurrent write.
    (let [r (lin-check
              [{:process 0 :type :invoke :f :write :value 1}
               {:process 0 :type :ok     :f :write :value 1}
               {:process 0 :type :invoke :f :read  :value nil}
               {:process 0 :type :ok     :f :read  :value 2}])]
      (is (false? (:valid? r)) "the per-field register checker must catch the anomaly"))))
