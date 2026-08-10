(ns jepsen.frogdb.invariant
  "Live cluster-invariant sweep via `DEBUG CLUSTER CHECK` (cluster-correctness
   campaign issue 06), wired into the Raft-topology test harness as issue 07.

   `DEBUG CLUSTER CHECK` returns the invariant catalog's violations for the
   node it was sent to: an empty RESP array when the node's `ClusterState` is
   clean, or one `{id, detail}` map per violation otherwise (see
   `frogdb-server/crates/server/src/connection/debug_conn_command.rs`,
   `format_cluster_check_response`). Under RESP2 (Carmine's wire protocol
   here) each violation map is flattened to `[\"id\" id-val \"detail\"
   detail-val]` — the generic RESP2 map-downgrade every FrogDB map reply gets
   (`frogdb-server/crates/protocol/src/response.rs`,
   `WireResponse::Map` -> `Resp2BytesFrame::Array`).

   Design — quiesce + final, never mid-fault:

   This is wired as a `:cluster-check` NEMESIS op (not a workload client op),
   because the nemesis process already has a natural, uniform place to run it
   without threading per-workload plumbing: `jepsen.frogdb.core/frogdb-test`
   composes exactly two points in its generator, shared by every raft
   workload regardless of which `--nemesis` package is selected —
     1. Right after the nemesis's own `:final-generator` has driven every
        fault all the way back to healed (heal/resume/start/reset — the one
        state every nemesis package guarantees at that point) and the
        follow-on settle sleep has elapsed, but BEFORE final reads start.
        This is \"nemesis quiesce\": no fault is active by construction,
        because the harness itself just finished retracting every one it
        knows how to apply.
     2. Right after the final-reads phase completes, as \"at final\".
   No check ever runs inside the main phase's `(gen/nemesis (:generator
   nemesis-pkg))` window, so it can never race or throttle an in-flight fault
   injection — the overhead constraint in issue 07 (\"calls only at quiesce,
   never during active nemesis windows\").

   `wrap-nemesis` intercepts exactly the `:cluster-check` `:f` and delegates
   every other op unchanged to the wrapped nemesis, so it composes with ANY
   nemesis package (`:none` through `:raft-cluster-membership`) with zero
   per-package changes — satisfying \"wire into ALL raft-topology workloads by
   default\", not per-workload opt-in."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.checker :as checker]
            [jepsen.nemesis :as nem]
            [jepsen.frogdb.cluster-db :as cluster-db]
            [taoensso.carmine :as car :refer [wcar]]))

;; ===========================================================================
;; DEBUG CLUSTER CHECK — single node
;; ===========================================================================

(defn- parse-violation
  "A single RESP2-flattened violation `[\"id\" id-val \"detail\" detail-val]`
   (in any key order — the wire only guarantees the pair grouping, not which
   field comes first) into `{:id ... :detail ...}`."
  [entry]
  (let [m (apply hash-map entry)]
    {:id (get m "id") :detail (get m "detail")}))

(defn check-node
  "Runs `DEBUG CLUSTER CHECK` against a single node connection. Returns
   `{:violations [...]}`  (an empty vector when clean) on success, or
   `{:error message}` if the call itself failed — connection refused, the
   node is mid-restart, cluster support disabled, etc. Uses plain try/catch,
   not slingshot's `try+`: see `jepsen.frogdb.client/with-error-handling`'s
   documented caveat — `try+` unwraps `clojure.lang.ExceptionInfo` looking
   for its own `throw+` payloads, so a plain Carmine RESP-error `ex-info`
   (every `ERR ...` reply, including the standalone-mode
   \"cluster support disabled\" error) slips past a `try+` handler uncaught."
  [conn]
  (try
    (let [reply (wcar conn (car/redis-call ["DEBUG" "CLUSTER" "CHECK"]))]
      {:violations (mapv parse-violation reply)})
    (catch Exception e
      {:error (.getMessage e)})))

;; ===========================================================================
;; DEBUG CLUSTER CHECK — every node
;; ===========================================================================

(defn check-all-nodes!
  "Runs `DEBUG CLUSTER CHECK` against every node in `nodes`, returning a map
   of `node -> check-node result`. Opens a fresh dedicated single-connection
   pool per node per call (this only ever runs at quiesce/final, a handful of
   times per test, so a connection isn't worth pooling across calls)."
  [nodes docker-host? base-port]
  (into {}
        (for [n nodes]
          [n (check-node (cluster-db/conn-for-raft-node-single n docker-host? base-port))])))

(defn invariant-sweep!
  "Runs the full-cluster `DEBUG CLUSTER CHECK` sweep for a running `test`,
   resolving nodes/docker-host?/base-port the same way the rest of the raft
   harness does (`:cluster-nodes` set by `jepsen.frogdb.core/frogdb-test`)."
  [test]
  (let [nodes (or (:cluster-nodes test) (:nodes test))
        docker-host? (get test :docker true)
        base-port (get test :base-port cluster-db/default-base-port)]
    (check-all-nodes! nodes docker-host? base-port)))

;; ===========================================================================
;; Nemesis wrapper — routes :cluster-check to the sweep, delegates the rest
;; ===========================================================================

(defn wrap-nemesis
  "Wraps `nemesis` so a `:cluster-check` op runs the invariant sweep across
   every node; every other `:f` is delegated to `nemesis` unchanged. The
   wrapped op's `:type` stays `:info` (matching every other nemesis event in
   this harness — it isn't part of the linearizability history) and its
   `:value` is the `{node -> check-node result}` map, so the
   `jepsen.frogdb.invariant/checker` below can read it straight out of the
   Jepsen history."
  [nemesis]
  (reify nem/Nemesis
    (setup! [this test]
      (wrap-nemesis (nem/setup! nemesis test)))

    (invoke! [this test op]
      (if (= :cluster-check (:f op))
        (let [results (invariant-sweep! test)
              violations (into {}
                                (keep (fn [[node result]]
                                        (when (seq (:violations result))
                                          [node (:violations result)]))
                                      results))]
          (if (seq violations)
            (warn "DEBUG CLUSTER CHECK found violations:" (pr-str violations))
            (info "DEBUG CLUSTER CHECK clean across" (count results) "nodes"))
          (assoc op :type :info :value results))
        (nem/invoke! nemesis test op)))

    (teardown! [this test]
      (nem/teardown! nemesis test))))

(defn cluster-check-op
  "The `:cluster-check` op emitted at each quiesce point. `:type :info`
   because this is a nemesis-process event, not a linearizability-checked
   client op — same convention as `:heal`/`:start`/`:resume`."
  []
  {:type :info :f :cluster-check})

;; ===========================================================================
;; Checker — fails the test on any observed violation
;; ===========================================================================

(defn checker
  "Jepsen checker consuming the `:cluster-check` ops `wrap-nemesis` recorded
   into the history. `:valid?` is false iff at least one sweep observed a
   non-empty violation list on at least one node; the violation IDs are
   surfaced directly in the checker's analysis output (`:violation-ids`),
   satisfying issue 07's \"fails the test with the violation IDs in the
   analysis\" acceptance criterion.

   A sweep's own connectivity errors (`:error` in a per-node result — e.g. a
   node still restarting) are reported for visibility (`:connectivity-errors`)
   but never gate `:valid?`: the sweep only ever runs at quiesce/final, where
   every node is expected to be reachable, so a connectivity error there is
   itself interesting operational signal, not proof of a broken invariant —
   conflating the two would make an unrelated flake fail the run on the
   invariant checker's behalf."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [;; Every :cluster-check op appears twice in the history: once as
            ;; the dispatch entry (no :value yet) and once completed (:value
            ;; the {node -> check-node result} map `wrap-nemesis` returned).
            ;; Both share :f :cluster-check and even :type :info (nemesis ops
            ;; use :info for both phases — see the :heal generator entries
            ;; alongside :cluster-check in frogdb-test's :generator), so only
            ;; `(map? (:value op))` distinguishes a completed sweep from its
            ;; own dispatch entry — filtering on :f alone would double-count
            ;; every sweep in :sweeps-run below.
            sweeps (->> history (filter #(and (= :cluster-check (:f %)) (map? (:value %)))))
            per-sweep (map (fn [op]
                             (let [results (:value op)
                                   violations (into {}
                                                     (keep (fn [[node r]]
                                                             (when (seq (:violations r))
                                                               [node (:violations r)]))
                                                           results))
                                   errors (into {}
                                                (keep (fn [[node r]]
                                                        (when (:error r) [node (:error r)]))
                                                      results))]
                               {:time (:time op) :violations violations :errors errors}))
                           sweeps)
            failing (filter #(seq (:violations %)) per-sweep)
            violation-ids (->> failing
                                (mapcat (fn [{:keys [violations]}]
                                          (mapcat (fn [[_ vs]] (map :id vs))
                                                  violations)))
                                distinct
                                vec)
            connectivity-errors (reduce + 0 (map (comp count :errors) per-sweep))]
        {:valid? (empty? failing)
         :sweeps-run (count sweeps)
         :violating-sweeps (count failing)
         :violation-ids violation-ids
         :details failing
         :connectivity-errors connectivity-errors}))))
