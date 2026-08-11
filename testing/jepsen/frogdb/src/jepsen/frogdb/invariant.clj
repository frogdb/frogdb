(ns jepsen.frogdb.invariant
  "Live invariant-catalog sweep via a `DEBUG <AREA> CHECK` command, wired into
   the test harness as a nemesis wrapper.

   Two surfaces exist today (see `cluster-surface` / `replication-surface`
   below): `DEBUG CLUSTER CHECK` on the Raft topology (cluster-correctness
   campaign issues 06/07) and `DEBUG REPLICATION CHECK` on the replication
   topology (replication-correctness issues 03/13). Everything here is
   parameterized on a *surface* map, so a third catalog costs one `def` and one
   `cond` arm in `jepsen.frogdb.core`, not a copy of this namespace.

   Both commands return the catalog's violations for the node they were sent
   to: an empty RESP array when that node's state is clean, or one `{id,
   detail}` map per violation otherwise (see
   `frogdb-server/crates/server/src/connection/debug_conn_command.rs`,
   `format_check_response`, shared by both). Under RESP2 (Carmine's wire
   protocol here) each violation map is flattened to `[\"id\" id-val \"detail\"
   detail-val]` — the generic RESP2 map-downgrade every FrogDB map reply gets
   (`frogdb-server/crates/protocol/src/response.rs`, `WireResponse::Map` ->
   `Resp2BytesFrame::Array`).

   Design — quiesce + final, never mid-fault:

   This is wired as a NEMESIS op (not a workload client op), because the
   nemesis process already has a natural, uniform place to run it without
   threading per-workload plumbing: `jepsen.frogdb.core/frogdb-test` composes
   exactly two points in its generator, shared by every workload regardless of
   which `--nemesis` package is selected —
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
   injection — the overhead constraint in cluster issue 07 (\"calls only at
   quiesce, never during active nemesis windows\"), which replication issue 13
   inherits.

   `wrap-nemesis` intercepts exactly the surface's own `:f` and delegates every
   other op unchanged to the wrapped nemesis, so it composes with ANY nemesis
   package (`:none` through `:raft-cluster-membership`) with zero per-package
   and zero per-workload changes — satisfying \"wire into ALL workloads of the
   topology by default\", not per-workload opt-in."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.checker :as checker]
            [jepsen.nemesis :as nem]
            [jepsen.frogdb.client :as client]
            [jepsen.frogdb.cluster-db :as cluster-db]
            [taoensso.carmine :as car :refer [wcar]]))

;; ===========================================================================
;; Surfaces — one per invariant catalog
;; ===========================================================================

(def cluster-surface
  "`DEBUG CLUSTER CHECK` on the Raft topology (cluster-correctness issue 07).

   :op-f       the nemesis `:f` this surface's sweep op carries
   :command    the RESP command, as Carmine `redis-call` argv
   :result-key the key the checker's analysis is composed under
   :conn-fn    (fn [node docker-host? base-port] -> conn-spec)
   :known      {violation-id -> why it is not gating}, see `checker`"
  {:op-f       :cluster-check
   :command    ["DEBUG" "CLUSTER" "CHECK"]
   :label      "DEBUG CLUSTER CHECK"
   :result-key :cluster-invariants
   :conn-fn    cluster-db/conn-for-raft-node-single
   :known      {}})

(def replication-surface
  "`DEBUG REPLICATION CHECK` on the 3-node primary+replicas topology
   (replication-correctness issue 13). Unlike the cluster command this one
   answers in every mode, so it is meaningful on a replica and on a node that
   has lost its link — there is no not-applicable case to skip."
  {:op-f       :replication-check
   :command    ["DEBUG" "REPLICATION" "CHECK"]
   :label      "DEBUG REPLICATION CHECK"
   :result-key :replication-invariants
   :conn-fn    client/conn-for-node-single
   :known      {}})

;; ===========================================================================
;; The check — single node
;; ===========================================================================

(defn- parse-violation
  "A single RESP2-flattened violation `[\"id\" id-val \"detail\" detail-val]`
   (in any key order — the wire only guarantees the pair grouping, not which
   field comes first) into `{:id ... :detail ...}`."
  [entry]
  (let [m (apply hash-map entry)]
    {:id (get m "id") :detail (get m "detail")}))

(defn check-node
  "Runs the surface's DEBUG command against a single node connection. Returns
   `{:violations [...]}`  (an empty vector when clean) on success, or
   `{:error message}` if the call itself failed — connection refused, the
   node is mid-restart, cluster support disabled, etc. Uses plain try/catch,
   not slingshot's `try+`: see `jepsen.frogdb.client/with-error-handling`'s
   documented caveat — `try+` unwraps `clojure.lang.ExceptionInfo` looking
   for its own `throw+` payloads, so a plain Carmine RESP-error `ex-info`
   (every `ERR ...` reply, including the standalone-mode
   \"cluster support disabled\" error) slips past a `try+` handler uncaught."
  [surface conn]
  (try
    (let [reply (wcar conn (car/redis-call (:command surface)))]
      {:violations (mapv parse-violation reply)})
    (catch Exception e
      {:error (.getMessage e)})))

;; ===========================================================================
;; The check — every node
;; ===========================================================================

(defn check-all-nodes!
  "Runs the surface's DEBUG command against every node in `nodes`, returning a
   map of `node -> check-node result`. Opens a fresh dedicated
   single-connection pool per node per call (this only ever runs at
   quiesce/final, a handful of times per test, so a connection isn't worth
   pooling across calls)."
  [surface nodes docker-host? base-port]
  (into {}
        (for [n nodes]
          [n (check-node surface ((:conn-fn surface) n docker-host? base-port))])))

(defn invariant-sweep!
  "Runs the full-topology sweep for a running `test`, resolving
   nodes/docker-host?/base-port the same way the rest of the harness does
   (`:cluster-nodes` is set by `jepsen.frogdb.core/frogdb-test` to whatever
   node set the test was built for — the whole Raft cluster, or the
   replication topology's nodes, or the pinned subset a client-pinned
   replication test like `register-partition` runs against)."
  [surface test]
  (let [nodes (or (:cluster-nodes test) (:nodes test))
        docker-host? (get test :docker true)
        base-port (get test :base-port cluster-db/default-base-port)]
    (check-all-nodes! surface nodes docker-host? base-port)))

;; ===========================================================================
;; Nemesis wrapper — routes the surface's :f to the sweep, delegates the rest
;; ===========================================================================

(defn wrap-nemesis
  "Wraps `nemesis` so the surface's own op runs the invariant sweep across
   every node; every other `:f` is delegated to `nemesis` unchanged. The
   wrapped op's `:type` stays `:info` (matching every other nemesis event in
   this harness — it isn't part of the linearizability history) and its
   `:value` is the `{node -> check-node result}` map, so the
   `jepsen.frogdb.invariant/checker` below can read it straight out of the
   Jepsen history."
  [surface nemesis]
  (reify nem/Nemesis
    (setup! [this test]
      (wrap-nemesis surface (nem/setup! nemesis test)))

    (invoke! [this test op]
      (if (= (:op-f surface) (:f op))
        (let [results (invariant-sweep! surface test)
              violations (into {}
                                (keep (fn [[node result]]
                                        (when (seq (:violations result))
                                          [node (:violations result)]))
                                      results))]
          (if (seq violations)
            (warn (:label surface) "found violations:" (pr-str violations))
            (info (:label surface) "clean across" (count results) "nodes"))
          (assoc op :type :info :value results))
        (nem/invoke! nemesis test op)))

    (teardown! [this test]
      (nem/teardown! nemesis test))))

(defn check-op
  "The sweep op emitted at each quiesce point. `:type :info` because this is a
   nemesis-process event, not a linearizability-checked client op — same
   convention as `:heal`/`:start`/`:resume`."
  [surface]
  {:type :info :f (:op-f surface)})

;; ===========================================================================
;; Checker — fails the test on any observed violation
;; ===========================================================================

(defn- split-known
  "Split a `[node -> violations]` map into `[gating known]` by the surface's
   `:known` allowlist."
  [known-ids violations]
  (let [known? (fn [v] (contains? known-ids (:id v)))
        pick (fn [pred]
               (into {} (keep (fn [[node vs]]
                                (when-let [kept (seq (filter pred vs))]
                                  [node (vec kept)]))
                              violations)))]
    [(pick (complement known?)) (pick known?)]))

(defn checker
  "Jepsen checker consuming the sweep ops `wrap-nemesis` recorded into the
   history. `:valid?` is false iff at least one sweep observed a non-empty,
   non-allowlisted violation list on at least one node; the violation IDs are
   surfaced directly in the checker's analysis output (`:violation-ids`),
   satisfying cluster issue 07's \"fails the test with the violation IDs in the
   analysis\" acceptance criterion.

   A sweep's own connectivity errors (`:error` in a per-node result — e.g. a
   node still restarting) are reported for visibility (`:connectivity-errors`)
   but never gate `:valid?`: the sweep only ever runs at quiesce/final, where
   every node is expected to be reachable, so a connectivity error there is
   itself interesting operational signal, not proof of a broken invariant —
   conflating the two would make an unrelated flake fail the run on the
   invariant checker's behalf.

   The surface's `:known` map allowlists specific violation ids against an
   open issue: they are counted and reported (`:known-violation-ids`,
   `:known-violations`) but do not gate `:valid?`, so a defect that is already
   filed does not mask every later regression by keeping the suite red. Each
   entry names its issue; the entry is deleted when the issue closes."
  [surface]
  (let [known-ids (set (keys (:known surface)))]
    (reify checker/Checker
      (check [_ test history opts]
        (let [;; Every sweep op appears twice in the history: once as the
              ;; dispatch entry (no :value yet) and once completed (:value the
              ;; {node -> check-node result} map `wrap-nemesis` returned).
              ;; Both share the surface's :f and even :type :info (nemesis ops
              ;; use :info for both phases — see the :heal generator entries
              ;; alongside the sweep op in frogdb-test's :generator), so only
              ;; `(map? (:value op))` distinguishes a completed sweep from its
              ;; own dispatch entry — filtering on :f alone would double-count
              ;; every sweep in :sweeps-run below.
              sweeps (->> history (filter #(and (= (:op-f surface) (:f %))
                                                (map? (:value %)))))
              per-sweep (map (fn [op]
                               (let [results (:value op)
                                     all-violations
                                     (into {}
                                           (keep (fn [[node r]]
                                                   (when (seq (:violations r))
                                                     [node (:violations r)]))
                                                 results))
                                     [violations known]
                                     (split-known known-ids all-violations)
                                     errors (into {}
                                                  (keep (fn [[node r]]
                                                          (when (:error r) [node (:error r)]))
                                                        results))]
                                 {:time (:time op)
                                  :violations violations
                                  :known known
                                  :errors errors}))
                             sweeps)
              failing (filter #(seq (:violations %)) per-sweep)
              ids-of (fn [k coll]
                       (->> coll
                            (mapcat (fn [entry]
                                      (mapcat (fn [[_ vs]] (map :id vs)) (get entry k))))
                            distinct
                            vec))
              known-seen (filter #(seq (:known %)) per-sweep)
              connectivity-errors (reduce + 0 (map (comp count :errors) per-sweep))]
          (cond-> {:valid? (empty? failing)
                   :sweeps-run (count sweeps)
                   :violating-sweeps (count failing)
                   :violation-ids (ids-of :violations failing)
                   :details failing
                   :connectivity-errors connectivity-errors}
            (seq known-seen)
            (assoc :known-violation-ids (ids-of :known known-seen)
                   :known-violations (count known-seen)
                   :known-issues (:known surface))))))))
