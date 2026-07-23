(ns jepsen.frogdb.expiry
  "Expiration/TTL workload for FrogDB.

   Tests key expiration behavior under faults using SET EX, EXPIRE, TTL, PERSIST.

   This workload verifies, in a way that is SOUND UNDER CLOCK SKEW between the
   Jepsen control node and the DB nodes:
   - No resurrection: a key once observed expired/deleted never reappears alive
     unless a later SET re-creates it (holds regardless of clock behaviour).
   - No premature expiry: a key does not disappear before its server-reported
     remaining lifetime (PTTL) elapses.
   - No zombie keys: a key does not stay alive past its server-reported lifetime.
   - PERSIST durability: a persisted key does not spontaneously expire.
   - TTL sanity: reported TTL/PTTL never exceeds the configured maximum.

   Server-time authority
   ---------------------
   Expiry in Redis/FrogDB is decided by the *server's* clock. A checker that
   predicts an expiry instant from the Jepsen client's wall clock (the TTL the
   client asked for + `System/currentTimeMillis` at SET time) is unsound the
   moment a DB node's clock diverges from the control node's: a skewed node
   legitimately expires keys early or late relative to the control clock, which
   such a checker would misreport as a bug. This checker therefore never predicts
   an expiry from a client-chosen TTL. It anchors on the *server's own* reported
   remaining lifetime (PTTL) and projects it forward using only ELAPSED
   control-node time — a duration, which is invariant under a constant clock
   OFFSET between the control node and the server. All ordering uses Jepsen's
   per-op `:time` (a single control-node monotonic clock, unaffected by server
   skew); every correctness value (alive/dead, remaining TTL) comes from the
   server."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

;; Key pool: we use a fixed pool of keys
(def key-pool (mapv #(str "expiry-key-" %) (range 20)))

;; TTL settings (in seconds)
;; We use short TTLs to make tests observable within reasonable time
(def min-ttl 3)
(def max-ttl 10)

;; Tolerance for expiration checking (in seconds).
;; Absorbs server processing latency and the coarseness of projecting a
;; server-reported PTTL forward across control-node elapsed time. Applied to
;; both edges of every projection so borderline windows never false-positive.
(def expiry-tolerance 2)

;; ===========================================================================
;; Expiry Operations
;; ===========================================================================

(defn set-ex!
  "Set key with expiration in seconds using SET EX."
  [conn key value ttl-seconds]
  (wcar conn (car/set key (str value) "EX" ttl-seconds)))

(defn set-px!
  "Set key with expiration in milliseconds using SET PX."
  [conn key value ttl-ms]
  (wcar conn (car/set key (str value) "PX" ttl-ms)))

(defn get-key
  "Get value of a key."
  [conn key]
  (wcar conn (car/get key)))

(defn ttl-key
  "Get TTL of a key in seconds.
   Returns: positive number = TTL remaining, -1 = no expiry, -2 = key doesn't exist."
  [conn key]
  (wcar conn (car/ttl key)))

(defn pttl-key
  "Get TTL of a key in milliseconds."
  [conn key]
  (wcar conn (car/pttl key)))

(defn expire!
  "Set expiration on existing key. Returns 1 if set, 0 if key doesn't exist."
  [conn key seconds]
  (wcar conn (car/expire key seconds)))

(defn persist!
  "Remove expiration from key. Returns 1 if removed, 0 if no expiry or no key."
  [conn key]
  (wcar conn (car/persist key)))

(defn del!
  "Delete a key."
  [conn key]
  (wcar conn (car/del key)))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord ExpiryClient [conn node docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (:docker test)
          base-port (get test :base-port frogdb/default-base-port)]
      (info "Opening expiry client to" node "(docker-host?:" docker? ")")
      (assoc this
             :conn (frogdb/conn-spec-single node frogdb/default-port docker? base-port)
             :node node
             :docker-host? docker?)))

  (setup! [this test]
    ;; Clear all test keys at the start
    (info "Clearing expiry keys")
    (doseq [k key-pool]
      (wcar conn (car/del k)))
    this)

  (invoke! [this test op]
    ;; No client wall-clock timestamps are recorded: the checker orders
    ;; operations by Jepsen's control-node `:time` and derives all timing from
    ;; server-reported TTL/PTTL. See the ns docstring.
    (frogdb/with-error-handling op
      (let [k (:key op (rand-nth key-pool))]
        (case (:f op)
          ;; Set with TTL
          :set-with-ttl
          (let [{:keys [key value ttl]} (:value op)]
            (set-ex! conn key value ttl)
            (assoc op :type :ok
                   :value {:key key :value value :ttl ttl}))

          ;; Read key (may return nil if expired)
          :read
          (let [key (:value op)
                value (get-key conn key)]
            (assoc op :type :ok
                   :value {:key key :value value}))

          ;; Get TTL (seconds) AND PTTL (milliseconds). PTTL is the
          ;; server-authoritative remaining-lifetime anchor the checker projects
          ;; forward; TTL is kept for the configured-maximum sanity check.
          :ttl
          (let [key (:value op)
                ttl (ttl-key conn key)
                pttl (pttl-key conn key)]
            (assoc op :type :ok
                   :value {:key key :ttl ttl :pttl pttl}))

          ;; Set expiration on existing key
          :expire
          (let [{:keys [key ttl]} (:value op)
                result (expire! conn key ttl)]
            (assoc op :type :ok
                   :value {:key key :ttl ttl}
                   :result result))

          ;; Remove expiration
          :persist
          (let [key (:value op)
                result (persist! conn key)]
            (assoc op :type :ok
                   :value {:key key}
                   :result result))

          ;; Delete key
          :delete
          (let [key (:value op)
                result (del! conn key)]
            (assoc op :type :ok :value {:key key} :result result))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (frogdb/close-conn! conn)))

(defn create-client
  "Create a new expiry client."
  []
  (map->ExpiryClient {}))

;; ===========================================================================
;; Generator
;; ===========================================================================

(defn set-with-ttl-op
  "Generate a set-with-ttl operation."
  [_ _]
  {:type :invoke
   :f :set-with-ttl
   :value {:key (rand-nth key-pool)
           :value (rand-int 10000)
           :ttl (+ min-ttl (rand-int (- max-ttl min-ttl)))}})

(defn read-op
  "Generate a read operation."
  [_ _]
  {:type :invoke :f :read :value (rand-nth key-pool)})

(defn ttl-op
  "Generate a TTL check operation."
  [_ _]
  {:type :invoke :f :ttl :value (rand-nth key-pool)})

(defn expire-op
  "Generate an expire operation."
  [_ _]
  {:type :invoke
   :f :expire
   :value {:key (rand-nth key-pool)
           :ttl (+ min-ttl (rand-int (- max-ttl min-ttl)))}})

(defn persist-op
  "Generate a persist operation."
  [_ _]
  {:type :invoke :f :persist :value (rand-nth key-pool)})

(defn delete-op
  "Generate a delete operation."
  [_ _]
  {:type :invoke :f :delete :value (rand-nth key-pool)})

(defn generator
  "Create a generator for expiry operations.

   Options:
   - :rate - operations per second (default 10)"
  [opts]
  (let [rate (get opts :rate 10)]
    (->> (gen/mix [(gen/repeat set-with-ttl-op)
                   (gen/repeat set-with-ttl-op)  ; Weight sets more heavily
                   (gen/repeat read-op)
                   (gen/repeat read-op)
                   (gen/repeat ttl-op)
                   (gen/repeat expire-op)
                   (gen/repeat persist-op)])
         (gen/stagger (/ 1 rate)))))

(defn final-read-generator
  "Generator for final reads of all keys."
  []
  (->> key-pool
       (map (fn [k] {:f :read :value k}))
       (gen/each-thread)))

;; ===========================================================================
;; Checker (server-time authoritative — see ns docstring)
;; ===========================================================================

(defn- ns->ms
  "Convert Jepsen's control-node nanosecond `:time` to milliseconds."
  [t]
  (when t (quot (long t) 1000000)))

(defn pair-completions
  "Pair each :ok completion with its :invoke, attaching :invoke-ms and
   :complete-ms (control-node monotonic time in ms). Returns the completed :ok
   ops in history order.

   Ordering and timing come exclusively from the control node's single clock, so
   they are unaffected by any skew of the DB nodes' clocks. Correctness values
   (alive/dead, TTL) are read from the server, never predicted from a client
   wall clock. Non-:ok completions (:fail/:info, including nemesis ops) are
   dropped."
  [history]
  (:done
   (reduce
    (fn [{:keys [pending] :as acc} op]
      (case (:type op)
        :invoke (assoc-in acc [:pending (:process op)] op)
        (:ok :fail :info)
        (if-let [inv (get pending (:process op))]
          (let [acc (update acc :pending dissoc (:process op))]
            (if (= :ok (:type op))
              (update acc :done conj
                      (assoc op
                             :invoke-ms (ns->ms (:time inv))
                             :complete-ms (ns->ms (:time op))))
              acc))
          acc)
        acc))
    {:pending {} :done []}
    history)))

(defn- op->event
  "Normalize a completed :ok op into a per-key event, or nil if irrelevant.

   Events carry :it / :ct = control-node invoke / complete time (ms), the flags
   :mutation? (changes the key's existence or expiry) and :create? (a SET, the
   only way a key can transition dead -> alive), and observation flags
   :alive?/:dead?."
  [op]
  (let [k (get-in op [:value :key])
        it (:invoke-ms op)
        ct (:complete-ms op)]
    (when (and k it ct)
      (case (:f op)
        :set-with-ttl
        {:key k :kind :set :mutation? true :create? true :it it :ct ct}

        :expire
        (when (= 1 (:result op))
          {:key k :kind :expire :mutation? true :it it :ct ct})

        :persist
        ;; PERSIST returns 1 only when the key existed (with a TTL) — so it both
        ;; mutates the key (removes its expiry) and observes it alive.
        (when (= 1 (:result op))
          {:key k :kind :persist :mutation? true :alive? true :it it :ct ct})

        :delete
        ;; A successful DELETE both mutates the key and observes it dead.
        {:key k :kind :delete :mutation? true :dead? true :it it :ct ct}

        :read
        (let [v (get-in op [:value :value])]
          {:key k :kind :read :alive? (some? v) :dead? (nil? v) :it it :ct ct})

        :ttl
        (let [ttl (get-in op [:value :ttl])
              pttl (get-in op [:value :pttl])]
          {:key k :kind :ttl :ttl ttl :pttl pttl
           :alive? (and (number? ttl) (or (>= ttl 0) (= -1 ttl)))
           :dead? (= ttl -2)
           :it it :ct ct})

        nil))))

(defn- mutation-overlaps?
  "True if mutation m's [it,ct] window overlaps the span [a.it, b.ct] — i.e. m
   could have executed anywhere between events a and b. Conservative (widest
   plausible window) so an ambiguous concurrent mutation suppresses a comparison
   rather than producing a false positive."
  [m a b]
  (and (< (:it m) (:ct b))
       (> (:ct m) (:it a))))

(defn- any-mutation-between?
  [muts a b]
  (boolean (some #(mutation-overlaps? % a b) muts)))

(defn- latest-before
  "The event in `evs` with the greatest :ct that still completed strictly before
   `ref`'s invocation (i.e. happens-before ref)."
  [evs ref]
  (->> evs
       (filter #(< (:ct %) (:it ref)))
       (sort-by :ct)
       last))

(defn- check-key
  "Return the vector of violations for a single key's event stream."
  [k events tol-ms max-pttl-ms]
  (let [muts        (filter :mutation? events)
        creates     (filter :create? events)
        expiry-muts (filter #(#{:set :expire :delete} (:kind %)) events)
        anchors     (filter #(and (= :ttl (:kind %))
                                  (number? (:pttl %)) (>= (:pttl %) 0))
                            events)
        reads       (filter #(= :read (:kind %)) events)
        dead-obs    (filter :dead? events)
        alive-obs   (filter :alive? events)
        persists    (filter #(= :persist (:kind %)) events)
        vs (transient [])]

    ;; (1) & (2) PTTL-anchored premature-expiry / zombie-key.
    ;; PTTL p was the server's remaining lifetime measured at some instant in
    ;; [a.it, a.ct]. The projection is only valid for a read that happens AFTER
    ;; the anchor (a PTTL says nothing about a read in its own past). Projecting
    ;; with elapsed control time (rate parity holds even under a constant offset
    ;; skew): after the anchor the key must be alive until ~anchor + p and dead
    ;; after. Both edges use the conservative anchor + the full tolerance so only
    ;; unambiguous violations are flagged, and any mutation in the span suppresses
    ;; the comparison.
    (doseq [a anchors
            r reads
            :when (and (> (:it r) (:ct a))                   ; read strictly after the anchor
                       (not (any-mutation-between? muts a r)))]
      (let [p (:pttl a)]
        (cond
          ;; Read finished while remaining lifetime (from the EARLIEST the anchor
          ;; could have been measured) was still clearly positive — yet gone.
          (and (:dead? r) (< (:ct r) (- (+ (:it a) p) tol-ms)))
          (conj! vs {:violation :premature-expiry :key k
                     :pttl-ms p :anchor-invoke-ms (:it a) :read-complete-ms (:ct r)
                     :slack-ms (- (+ (:it a) p) (:ct r))})

          ;; Read began after remaining lifetime (from the LATEST the anchor could
          ;; have been measured) was surely exhausted — yet still alive.
          (and (:alive? r) (> (:it r) (+ (:ct a) p tol-ms)))
          (conj! vs {:violation :zombie-key :key k
                     :pttl-ms p :anchor-complete-ms (:ct a) :read-invoke-ms (:it r)
                     :overshoot-ms (- (:it r) (+ (:ct a) p))}))))

    ;; (3) No resurrection. A key observed dead (read nil, TTL -2, or DELETE) can
    ;; only become alive again via a SET. If the closest preceding death has no
    ;; intervening SET, an alive observation is a resurrection. Holds under ANY
    ;; clock behaviour — a value that was gone must not reappear from nothing.
    (doseq [al alive-obs]
      (when-let [d (latest-before dead-obs al)]
        (when-not (any-mutation-between? creates d al)
          (conj! vs {:violation :resurrection :key k
                     :dead-complete-ms (:ct d) :alive-invoke-ms (:it al)
                     :alive-kind (:kind al)}))))

    ;; (4) Persisted-key durability. After a successful PERSIST (expiry removed),
    ;; with no intervening SET / EXPIRE / DELETE, the key must not be observed
    ;; dead. Server-authoritative and skew-independent.
    (doseq [r (filter :dead? reads)]
      (when-let [p (latest-before persists r)]
        (when-not (any-mutation-between? expiry-muts p r)
          (conj! vs {:violation :persisted-key-expired :key k
                     :persist-complete-ms (:ct p) :read-invoke-ms (:it r)}))))

    ;; (5) TTL/PTTL sanity: a reported remaining lifetime must never exceed the
    ;; configured maximum (server value only).
    (doseq [a (filter #(= :ttl (:kind %)) events)]
      (let [ttl (:ttl a) pttl (:pttl a)]
        (when (or (and (number? ttl) (> ttl (+ max-ttl expiry-tolerance)))
                  (and (number? pttl) (> pttl (+ max-pttl-ms tol-ms))))
          (conj! vs {:violation :suspicious-ttl :key k :ttl ttl :pttl pttl}))))

    (persistent! vs)))

(defn check-expiry-history
  "Analyze the history for TTL/expiration correctness, server-time authoritative.

   Flags:
   - :premature-expiry — key gone before its server-reported PTTL elapsed
   - :zombie-key — key alive past its server-reported PTTL
   - :resurrection — key reappeared alive after being observed dead, no SET
   - :persisted-key-expired — a persisted key spontaneously expired
   - :suspicious-ttl — reported TTL/PTTL exceeds the configured maximum

   All ordering derives from the control node's monotonic clock and all
   correctness values from the server, so a clock offset between the control node
   and the DB nodes never produces false positives (see ns docstring)."
  [history]
  (let [ok          (pair-completions history)
        by-key      (group-by :key (keep op->event ok))
        tol-ms      (* expiry-tolerance 1000)
        max-pttl-ms (* max-ttl 1000)
        violations  (mapcat (fn [[k evs]] (check-key k evs tol-ms max-pttl-ms)) by-key)
        by-type     (group-by :violation violations)
        f-count     (fn [f] (count (filter #(= f (:f %)) ok)))]
    {:valid? (empty? violations)
     :set-count (f-count :set-with-ttl)
     :expire-count (f-count :expire)
     :persist-count (f-count :persist)
     :read-count (f-count :read)
     :ttl-check-count (f-count :ttl)
     :delete-count (f-count :delete)
     :keys-tracked (count by-key)
     :violation-count (count violations)
     :premature-expiries (seq (:premature-expiry by-type))
     :zombie-keys (seq (:zombie-key by-type))
     :resurrections (seq (:resurrection by-type))
     :persisted-key-expiries (seq (:persisted-key-expired by-type))
     :suspicious-ttls (seq (:suspicious-ttl by-type))}))

(defn checker
  "Create a checker for expiry operations."
  []
  (reify checker/Checker
    (check [this test history opts]
      (check-expiry-history history))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Create an expiry workload for testing.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (gen/phases
                (generator opts)
                ;; Wait a bit for some keys to expire
                (gen/sleep 5)
                ;; Final reads
                (gen/clients (final-read-generator)))
   :checker (checker/compose
              {:expiry (checker)})})
