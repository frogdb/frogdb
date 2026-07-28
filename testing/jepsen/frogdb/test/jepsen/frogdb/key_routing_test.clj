(ns jepsen.frogdb.key-routing-test
  "Deterministic tests for the key-routing data-checking checker.

   These feed synthetic histories to the checker to prove it catches the two
   data anomalies that the old redirect-only checker was blind to — wrong-value
   reads and dropped (lost) acknowledged writes — and that it does NOT raise the
   false positives the naive version would (a nil read that predates its write;
   an expected single-master loss on SIGKILL). Without a real cluster, this is
   the deterministic proof for acceptance criterion 3 of issue 06."
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [jepsen.frogdb.key-routing :as kr]))

(defn- write-op
  "Synthetic :ok write as the client would produce it."
  [k v]
  {:type :ok :f :write :value {:key k :written v :slot 0 :redirects 0}})

(defn- read-op
  "Synthetic :ok mid-run read."
  [k v]
  {:type :ok :f :read :value {:key k :value v :slot 0 :redirects 0}})

(defn- final-read-op
  "Synthetic :ok post-recovery final read (durability is asserted on these)."
  [k v]
  {:type :ok :f :final-read :value {:key k :value v :slot 0 :redirects 0}})

(def kill-op
  "Synthetic nemesis process kill."
  {:type :info :f :kill :value ["n1"] :process :nemesis})

(defn- check
  [history]
  (checker/check (kr/checker) {} history {}))

(deftest clean-history-is-valid
  (testing "acked writes read back with their own values -> valid"
    (let [r (check [(write-op "kr-1" 1)
                    (write-op "kr-2" 2)
                    (read-op "kr-1" 1)
                    (final-read-op "kr-1" 1)
                    (final-read-op "kr-2" 2)
                    ;; Final read of a never-written key is legally nil, ignored.
                    (final-read-op "kr-9" nil)])]
      (is (true? (:valid? r)))
      (is (true? (:no-lost-writes? r)))
      (is (true? (:values-correct? r)))
      (is (true? (:durability-enforced? r)))
      (is (zero? (:lost-write-count r)))
      (is (zero? (:wrong-value-count r))))))

(deftest catches-lost-write
  (testing "a final read of an acked-written key returning nil, no kill -> fail"
    (let [r (check [(write-op "kr-1" 1)
                    (final-read-op "kr-1" nil)])]
      (is (false? (:valid? r)) "dropped acked write (no kill) must fail")
      (is (false? (:no-lost-writes? r)))
      (is (true? (:durability-enforced? r)))
      (is (= ["kr-1"] (:lost-write-keys r)))
      (is (= 1 (:lost-write-count r))))))

(deftest lost-write-under-kill-is-informational
  (testing "a lost write AFTER a SIGKILL is expected single-master loss, not a fail"
    (let [r (check [(write-op "kr-1" 1)
                    kill-op
                    (final-read-op "kr-1" nil)])]
      (is (true? (:valid? r)) "un-replicated shard may lose recent acked writes on kill")
      (is (true? (:process-killed? r)))
      (is (false? (:durability-enforced? r)))
      ;; ...but the loss is still surfaced for visibility.
      (is (= 1 (:lost-write-count r))))))

(deftest mid-run-nil-before-write-is-not-lost
  (testing "a nil read that predates the key's write is not a lost write"
    (let [r (check [;; read happens first, key not yet written -> legally nil
                    (read-op "kr-1" nil)
                    (write-op "kr-1" 1)
                    (final-read-op "kr-1" 1)])]
      (is (true? (:valid? r)) "ordering: early nil read must not count as loss")
      (is (zero? (:lost-write-count r))))))

(deftest catches-wrong-value
  (testing "a read returning a value never written to that key fails, even mid-run"
    (let [r (check [(write-op "kr-2" 7)
                    ;; 99 was never written to kr-2 (routing corruption /
                    ;; cross-key contamination / fabrication).
                    (read-op "kr-2" 99)])]
      (is (false? (:valid? r)) "wrong-value read must fail the checker")
      (is (false? (:values-correct? r)))
      (is (= 1 (:wrong-value-count r)))
      (is (= "kr-2" (:key (first (:wrong-value-reads r))))))))

(deftest wrong-value-fails-even-under-kill
  (testing "value-correctness always gates, regardless of kills"
    (let [r (check [(write-op "kr-2" 7)
                    kill-op
                    (read-op "kr-2" 99)])]
      (is (false? (:valid? r)) "corruption is never excused by a kill")
      (is (false? (:values-correct? r))))))

(deftest overwrite-is-not-wrong-value
  (testing "any value that was ever written to a key is accepted"
    (let [r (check [(write-op "kr-3" 10)
                    (write-op "kr-3" 20)
                    ;; Reading either committed value is fine (no ordering
                    ;; assumption): both are in the acked set for kr-3.
                    (read-op "kr-3" 10)
                    (final-read-op "kr-3" 20)])]
      (is (true? (:valid? r)))
      (is (true? (:values-correct? r))))))

(deftest redirect-failures-still-fail
  (testing "a :too-many-redirects failure still invalidates the run"
    (let [r (check [(write-op "kr-1" 1)
                    (final-read-op "kr-1" 1)
                    {:type :fail :f :read :error :too-many-redirects}])]
      (is (false? (:valid? r)))
      (is (= 1 (:redirect-failures r))))))
