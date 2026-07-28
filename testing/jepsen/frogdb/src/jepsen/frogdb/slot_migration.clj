(ns jepsen.frogdb.slot-migration
  "Slot migration workload for FrogDB Raft cluster.

   Tests:
   - Normal slot migration (SETSLOT MIGRATING/IMPORTING/NODE)
   - Migration interrupted by partition
   - Migration interrupted by node crash
   - ASK redirect handling during migration
   - Data integrity during migration

   Operations:
   - :start-migration - Begin migrating a slot
   - :finish-migration - Complete a slot migration
   - :abort-migration - Cancel a migration
   - :read-slot-owner - Get current owner of a slot
   - :write - Write to key in migrating slot
   - :read - Read from key in migrating slot
   - :queued-txn - MULTI + queued writes on a pinned connection to the slot's
     current owner, left open across a migration
   - :exec-queued-txn - EXEC that pinned transaction after the slot has moved
   - :read-orphans - Read every non-owner node directly for keys of a migrated
     slot (detects writes that landed on the former owner)"
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn debug]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.frogdb.client :as frogdb]
            [jepsen.frogdb.cluster-db :as cluster-db]
            [jepsen.frogdb.cluster-client :as cluster-client]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

;; ===========================================================================
;; Migration State Tracking
;; ===========================================================================

;; Shared across all workers so gen/once migration ops see the same state
;; regardless of which worker thread executes them.
(def shared-active-migration (atom nil))

;; The one queued-but-not-executed MULTI, held across a migration. Shared for
;; the same reason as the migration state: `:queued-txn` and `:exec-queued-txn`
;; are `gen/once` ops that may land on different worker threads, but they must
;; share one *pinned* connection — MULTI is session state, and Carmine reads
;; replies eagerly, so the queued commands and the later EXEC cannot be split
;; across pooled connections.
(def shared-queued-txn (atom nil))

;; Redirect prefixes a client may legitimately receive from EXEC once the slot
;; has moved out from under a queued transaction.
(def exec-redirect-prefixes ["MOVED" "ASK" "TRYAGAIN" "CROSSSLOT" "CLUSTERDOWN"])

(defn- redirect-reply?
  "True if msg is one of the bare cluster redirects EXEC may answer with."
  [msg]
  (boolean (and msg (some #(str/starts-with? msg %) exec-redirect-prefixes))))

;; A Redis error reply always opens with an uppercase error code ("MOVED 1234
;; ...", "ERR ...", "EXECABORT ..."). Simple-status replies that happen to be
;; uppercase are not errors and are listed out explicitly.
(def ^:private redis-error-code-re #"^[A-Z][A-Z0-9_]{2,}(?: |$)")
(def ^:private non-error-statuses #{"OK" "QUEUED" "PONG" "NOKEY"})

(defn- error-string?
  "True if a raw string reply is a Redis error rather than a simple status."
  [s]
  (and (string? s)
       (not (contains? non-error-statuses s))
       (some? (re-find redis-error-code-re s))))

(defn- error-message
  "Extract the Redis error text from a Carmine reply or exception, or nil.

   Carmine surfaces an error in three shapes depending on how the reply was
   read: thrown as an ExceptionInfo carrying the server text under `:message`
   in its ex-data; handed back as a value (a map with the same `:message`, or a
   bare `Exception`); or, for pipelined/RESP3 reads, as the raw error string.
   All three must be recognized — treating one of them as a plain value scores
   a real MOVED as a successful EXEC, which is exactly the fault this workload
   exists to catch."
  [x]
  (cond
    (instance? clojure.lang.ExceptionInfo x) (or (:message (ex-data x))
                                                 (.getMessage ^Exception x))
    (instance? Exception x) (.getMessage ^Exception x)
    (map? x) (let [msg (or (:message x) (:err x) (:error x))]
               (when (string? msg) msg))
    (error-string? x) x
    :else nil))

(defn- reply-seq
  "A reply, or a pipeline of replies, as a sequence."
  [replies]
  (if (sequential? replies) replies [replies]))

(defrecord MigrationState [slot source-node dest-node status keys-migrated])

(defn get-slot-owner
  "Get the node that owns a particular slot."
  [nodes docker-host? slot base-port]
  (let [conn (cluster-db/conn-for-raft-node (first nodes) docker-host? base-port)
        slots-info (cluster-db/cluster-slots conn)]
    (when slots-info
      (some (fn [slot-info]
              (let [start (long (nth slot-info 0))
                    end (long (nth slot-info 1))]
                (when (and (<= start slot) (<= slot end))
                  (let [master-info (nth slot-info 2)]
                    {:ip (nth master-info 0)
                     :port (nth master-info 1)
                     :id (when (> (count master-info) 2) (nth master-info 2))}))))
            slots-info))))

(defn get-node-for-ip
  "Find node name for an IP address."
  [ip]
  (first (filter #(= (get cluster-db/raft-cluster-node-ips %) ip)
                 (keys cluster-db/raft-cluster-node-ips))))

;; ===========================================================================
;; Migration Operations
;; ===========================================================================

(defn migrate-slot-keys!
  "Migrate all keys in a slot from source to dest."
  [source-conn dest-ip dest-port slot timeout-ms]
  (loop [total-migrated 0]
    (let [keys (cluster-db/cluster-getkeysinslot source-conn slot 100)]
      (if (empty? keys)
        total-migrated
        (do
          (doseq [key keys]
            (cluster-db/migrate! source-conn dest-ip dest-port key 0 timeout-ms))
          (recur (+ total-migrated (count keys))))))))

(defn parse-redirect-addr
  "Parse a REDIRECT error message and return the target node name.
   Format: 'REDIRECT <node-id> <ip:port>'
   Maps the Docker internal IP back to a node name."
  [msg]
  (when (and msg (str/starts-with? msg "REDIRECT"))
    (let [parts (str/split msg #" ")]
      (when (>= (count parts) 3)
        (let [[ip _port] (str/split (nth parts 2) #":")]
          (cluster-db/get-node-for-ip ip))))))

(defn with-leader-retry
  "Execute f, retrying on REDIRECT by switching to the indicated leader.
   f is called with the current leader node name and its connection.
   Retries up to max-retries times."
  [nodes docker-host? base-port max-retries f]
  (loop [leader (or (cluster-db/find-leader-node nodes docker-host? base-port) (first nodes))
         attempt 0]
    (let [leader-conn (cluster-db/conn-for-raft-node leader docker-host? base-port)
          result (try
                   {:ok (f leader leader-conn)}
                   (catch Exception e
                     (let [msg (.getMessage e)]
                       (if-let [new-leader (parse-redirect-addr msg)]
                         (if (< attempt max-retries)
                           {:redirect new-leader}
                           (throw e))
                         (throw e)))))]
      (if-let [new-leader (:redirect result)]
        (do (info "REDIRECT -> retrying on leader" new-leader "(attempt" (inc attempt) ")")
            (Thread/sleep 200)
            (recur new-leader (inc attempt)))
        (:ok result)))))

(defn start-slot-migration!
  "Start migrating a slot from source to dest node.
   Sends CLUSTER SETSLOT MIGRATING to the Raft leader with explicit source/target IDs.
   Both MIGRATING and IMPORTING produce the same BeginSlotMigration Raft op,
   so a single call to the leader is sufficient.
   Uses the cluster's view of node IDs (from CLUSTER NODES) because auto-discovered
   nodes may report a different MYID than what the cluster assigned them.
   Follows REDIRECT responses if leadership changes mid-request."
  [nodes docker-host? slot source-node dest-node base-port]
  (with-leader-retry nodes docker-host? base-port 3
    (fn [leader leader-conn]
      (let [source-id (cluster-db/resolve-node-id source-node docker-host? base-port leader-conn)
            dest-id (cluster-db/resolve-node-id dest-node docker-host? base-port leader-conn)]
        ;; MIGRATING with explicit source-id (4th arg) so leader doesn't default to itself
        (wcar leader-conn
          (car/redis-call ["CLUSTER" "SETSLOT" (str slot) "MIGRATING" dest-id source-id]))
        (->MigrationState slot source-node dest-node :migrating 0)))))

(defn complete-slot-migration!
  "Complete a slot migration by setting the slot to the new owner via the Raft leader.
   SETSLOT NODE returns RaftNeeded, so it must go through the leader (single call).
   Follows REDIRECT responses if leadership changes mid-request."
  [nodes docker-host? slot dest-node base-port]
  (with-leader-retry nodes docker-host? base-port 3
    (fn [leader leader-conn]
      (let [dest-id (cluster-db/resolve-node-id dest-node docker-host? base-port leader-conn)]
        (cluster-db/cluster-setslot-node! leader-conn slot dest-id)))))

;; ===========================================================================
;; Client Implementation
;; ===========================================================================

(defrecord SlotMigrationClient [nodes docker-host? base-port slot-mapping active-migration]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          all-nodes (or (:cluster-nodes test) ["n1" "n2" "n3"])
          bp (get test :base-port cluster-db/default-base-port)]
      (info "Opening slot migration client")
      (assoc this
             :nodes all-nodes
             :docker-host? docker?
             :base-port bp
             :slot-mapping (atom (cluster-client/create-slot-mapping all-nodes docker? bp))
             :active-migration shared-active-migration)))

  (setup! [this test]
    (reset! shared-active-migration nil)
    (reset! shared-queued-txn nil)
    this)

  (invoke! [this test op]
    (try+
      (case (:f op)
        :read-slot-owner
        (let [slot (:value op)
              owner (get-slot-owner nodes docker-host? slot base-port)]
          (assoc op :type :ok :value {:slot slot
                                       :owner (when owner (get-node-for-ip (:ip owner)))
                                       :details owner}))

        :read-slot-mapping
        (let [conn (cluster-db/conn-for-raft-node (first nodes) docker-host? base-port)
              slots (cluster-db/cluster-slots conn)]
          (assoc op :type :ok :value (count slots)))

        :start-migration
        (let [{:keys [slot source dest]} (:value op)
              ;; Dynamically resolve current owner so test is independent of prior state
              owner-info (get-slot-owner nodes docker-host? slot base-port)
              actual-source (or (get-node-for-ip (:ip owner-info)) source)
              actual-dest (if (= actual-source dest)
                            ;; Pick a different node when dest == current owner
                            (first (remove #{actual-source} nodes))
                            dest)]
          (if @active-migration
            (assoc op :type :fail :error :migration-already-active)
            (let [migration (start-slot-migration! nodes docker-host? slot actual-source actual-dest base-port)]
              (reset! active-migration migration)
              (assoc op :type :ok :value {:slot slot :source actual-source :dest actual-dest}))))

        :migrate-keys
        (if-let [migration @active-migration]
          (let [{:keys [slot source-node dest-node]} migration
                source-conn (cluster-db/conn-for-raft-node source-node docker-host? base-port)
                ;; MIGRATE executes inside Docker; use internal IPs, not host-mapped ports
                dest-ip (get cluster-db/raft-cluster-node-ips dest-node)
                migrated (migrate-slot-keys! source-conn dest-ip 6379 slot 5000)]
            (swap! active-migration assoc :keys-migrated migrated)
            (assoc op :type :ok :value {:keys-migrated migrated}))
          (assoc op :type :fail :error :no-active-migration))

        :finish-migration
        (if-let [migration @active-migration]
          (let [{:keys [slot dest-node]} migration]
            (complete-slot-migration! nodes docker-host? slot dest-node base-port)
            (reset! active-migration nil)
            ;; Refresh slot mapping
            (reset! slot-mapping (cluster-client/refresh-slot-mapping @slot-mapping (first nodes) docker-host? base-port))
            (assoc op :type :ok :value {:slot slot :new-owner dest-node}))
          (assoc op :type :fail :error :no-active-migration))

        :abort-migration
        (if-let [migration @active-migration]
          (let [{:keys [slot source-node]} migration
                leader (or (cluster-db/find-leader-node nodes docker-host? base-port) (first nodes))
                leader-conn (cluster-db/conn-for-raft-node leader docker-host? base-port)
                source-id (cluster-db/resolve-node-id source-node docker-host? base-port leader-conn)]
            ;; Reset slot to source via Raft leader
            (cluster-db/cluster-setslot-node! leader-conn slot source-id)
            (reset! active-migration nil)
            (assoc op :type :ok :value {:slot slot :owner source-node}))
          (assoc op :type :fail :error :no-active-migration))

        :write
        (let [{:keys [key value]} (:value op)
              slot (cluster-client/slot-for-key key)]
          (cluster-client/cluster-set slot-mapping key value docker-host? nodes base-port)
          (assoc op :type :ok :value {:key key :slot slot :written value}))

        :read
        (let [key (or (:value op) "{migration-test}:key")
              slot (cluster-client/slot-for-key key)
              value (cluster-client/cluster-get slot-mapping key docker-host? nodes base-port)]
          (assoc op :type :ok :value {:key key :slot slot :value (frogdb/parse-value value)}))

        ;; Open a MULTI on a pinned connection to the slot's *current* owner and
        ;; leave it queued. The generator then migrates the slot out from under
        ;; it, so the later :exec-queued-txn tests EXEC-time slot re-validation.
        :queued-txn
        (let [{:keys [key value]} (:value op)
              slot (cluster-client/slot-for-key key)
              owner-info (get-slot-owner nodes docker-host? slot base-port)
              owner (get-node-for-ip (:ip owner-info))
              conn (cluster-db/conn-for-raft-node-single owner docker-host? base-port)]
          ;; MULTI and its queued commands must share one wcar: Carmine reads
          ;; replies eagerly, so splitting them would return the pooled
          ;; connection between the MULTI and the writes.
          ;;
          ;; The queue-time replies are inspected, not discarded. Queue-time slot
          ;; validation can legitimately refuse the SET (MOVED, if the slot moved
          ;; before we opened the MULTI), and a run where every transaction was
          ;; refused at queue time never exercises the EXEC-time path this
          ;; workload is about. Recording that distinctly is what makes the
          ;; difference visible in the report instead of scoring as :ok.
          (let [replies (try
                          (wcar conn
                            (car/redis-call ["MULTI"])
                            (car/redis-call ["SET" key (str value)]))
                          (catch Exception e e))
                errs (vec (keep error-message (reply-seq replies)))
                base {:key key :slot slot :owner owner :queued value}]
            (if (seq errs)
              ;; Nothing is pinned: close the connection so a later
              ;; :exec-queued-txn cannot pick up a half-open transaction.
              (do (frogdb/close-conn! conn)
                  (assoc op :type :fail
                         :error [:queue-refused (first errs)]
                         :value (assoc base :outcome :queue-refused
                                       :replies errs)))
              (do (reset! shared-queued-txn {:conn conn :key key :slot slot
                                             :owner owner :value value})
                  (assoc op :type :ok
                         :value (assoc base :outcome :queued))))))

        ;; EXEC the transaction queued above, now that the slot may have moved.
        ;; Both outcomes are legal; what is NOT legal is a write landing on a
        ;; node that no longer owns the slot, which :read-orphans checks.
        :exec-queued-txn
        (if-let [{:keys [conn key slot owner value]} @shared-queued-txn]
          (let [reply (try
                        (wcar conn (car/redis-call ["EXEC"]))
                        (catch Exception e e))
                outcome (if-let [msg (error-message reply)]
                          {:error msg}
                          {:replies reply})]
            (reset! shared-queued-txn nil)
            (frogdb/close-conn! conn)
            (let [base {:key key :slot slot :queued-on owner :queued value}]
              (if-let [msg (:error outcome)]
                (if (redirect-reply? msg)
                  ;; The contract being tested: EXEC declines and discards the
                  ;; queue rather than committing on the former owner.
                  (assoc op :type :fail
                         :error [:exec-redirected (first (str/split msg #" "))]
                         :value (assoc base :outcome :redirected :reply msg))
                  (assoc op :type :fail :error [:exec-error msg]
                         :value (assoc base :outcome :error :reply msg)))
                (let [replies (vec (reply-seq (:replies outcome)))
                      inner-errs (vec (keep error-message replies))]
                  (if (seq inner-errs)
                    ;; EXEC returned an array, but a queued command inside it
                    ;; failed. Not a redirect and not a clean commit.
                    (assoc op :type :fail :error [:exec-inner-error (first inner-errs)]
                           :value (assoc base :outcome :inner-error
                                         :replies (mapv str replies)))
                    (assoc op :type :ok
                           :value (assoc base :outcome :executed
                                         :replies replies)))))))
          (assoc op :type :fail :error :no-queued-txn))

        ;; Read every node that does NOT own the slot *directly* for keys still
        ;; physically present in it. The cluster client cannot see these: it
        ;; follows MOVED to the new owner, so an orphan on the former owner is
        ;; invisible to every other op in this workload.
        :read-orphans
        (let [slot (:value op)
              owner-info (get-slot-owner nodes docker-host? slot base-port)
              owner (get-node-for-ip (:ip owner-info))
              orphans (into {}
                            (for [n nodes
                                  :when (not= n owner)
                                  :let [conn (cluster-db/conn-for-raft-node n docker-host? base-port)
                                        ks (vec (cluster-db/cluster-getkeysinslot conn slot 100))]
                                  :when (seq ks)]
                              [n ks]))]
          (assoc op :type :ok :value {:slot slot :owner owner :orphans orphans})))

      (catch java.net.ConnectException e
        (assoc op :type :fail :error :connection-refused))

      (catch java.net.SocketTimeoutException e
        (assoc op :type :info :error :timeout))

      (catch [:type :too-many-redirects] e
        ;; A data op exhausted its MOVED/ASK redirect budget (cluster-client
        ;; max-redirects). Surface it as a determinate :fail so the checker can
        ;; assert on unresolved redirects, instead of letting it fall through to
        ;; the generic Exception catch and be buried as an :info :unexpected.
        (assoc op :type :fail :error :too-many-redirects))

      (catch [:type :crossslot] e
        (assoc op :type :fail :error :crossslot))

      (catch [:type :clusterdown] e
        (assoc op :type :fail :error :cluster-down))

      (catch Exception e
        (let [msg (.getMessage e)]
          (if (and msg (str/starts-with? msg "REDIRECT"))
            (assoc op :type :fail :error [:redirect msg])
            (do (warn "Unexpected error:" e)
                (assoc op :type :info :error [:unexpected msg])))))))

  (teardown! [this test]
    ;; Release a transaction the generator never got to EXEC.
    (when-let [{:keys [conn]} @shared-queued-txn]
      (try+
        (wcar conn (car/redis-call ["DISCARD"]))
        (catch Object _ nil))
      (frogdb/close-conn! conn)
      (reset! shared-queued-txn nil))
    ;; Clean up any active migration via Raft leader
    (when-let [migration @active-migration]
      (let [{:keys [slot source-node]} migration]
        (try+
          (let [leader (or (cluster-db/find-leader-node nodes docker-host? base-port) (first nodes))
                leader-conn (cluster-db/conn-for-raft-node leader docker-host? base-port)
                source-id (cluster-db/resolve-node-id source-node docker-host? base-port leader-conn)]
            (cluster-db/cluster-setslot-node! leader-conn slot source-id))
          (catch Object _ nil)))))

  (close! [this test]
    nil))

(defn create-client
  "Create a new slot migration client."
  []
  (map->SlotMigrationClient {}))

;; ===========================================================================
;; Generators
;; ===========================================================================

(defn read-slot-owner
  "Generate a read-slot-owner operation."
  [slot]
  {:type :invoke :f :read-slot-owner :value slot})

(defn start-migration
  "Generate a start-migration operation."
  [slot source dest]
  {:type :invoke :f :start-migration :value {:slot slot :source source :dest dest}})

(defn migrate-keys
  "Generate a migrate-keys operation."
  []
  {:type :invoke :f :migrate-keys})

(defn finish-migration
  "Generate a finish-migration operation."
  []
  {:type :invoke :f :finish-migration})

(defn abort-migration
  "Generate an abort-migration operation."
  []
  {:type :invoke :f :abort-migration})

(defn write-key
  "Generate a write operation."
  [key value]
  {:type :invoke :f :write :value {:key key :value value}})

(defn read-key
  "Generate a read operation."
  [key]
  {:type :invoke :f :read :value key})

(defn queued-txn
  "Generate a queued-txn operation (MULTI + queued write, left open)."
  [key value]
  {:type :invoke :f :queued-txn :value {:key key :value value}})

(defn exec-queued-txn
  "Generate an exec-queued-txn operation."
  []
  {:type :invoke :f :exec-queued-txn})

(defn read-orphans
  "Generate a read-orphans operation for a slot."
  [slot]
  {:type :invoke :f :read-orphans :value slot})

(defn generator
  "Generator for slot migration testing.
   Writes to keys during a migration scenario.

   Write values are drawn from a per-run monotonic counter so every value is
   globally unique. That lets the checker validate data: a read returning a
   value never written to the key (fabrication / cross-slot contamination) is
   detectable, and the last acknowledged write is identifiable."
  [opts]
  (let [rate (get opts :rate 10)
        test-slot 1000  ; A slot in the middle of the range
        test-key "{migration-test}:key"  ; Hash-tagged to ensure same slot
        ;; A second key in the SAME slot, used only by the queued transaction.
        ;; Kept distinct from test-key so the register's durability and
        ;; value-correctness properties are unaffected by transaction writes
        ;; that are legally refused.
        txn-key "{migration-test}:txn"
        ctr (atom 0)
        next-val (fn [] (swap! ctr inc))]
    (gen/phases
      ;; Phase 1: Initial reads/writes
      (gen/log "Initial data operations")
      (->> (gen/mix [(fn [] (write-key test-key (next-val)))
                     (fn [] (read-key test-key))])
           (gen/limit 20)
           (gen/stagger 0.1))

      ;; Phase 2: Check slot owner
      (gen/log "Checking slot ownership")
      (gen/once (read-slot-owner (cluster-client/slot-for-key test-key)))

      ;; Phase 3: Start migration
      (gen/log "Starting slot migration from n1 to n2")
      (gen/once (start-migration (cluster-client/slot-for-key test-key) "n1" "n2"))
      (gen/sleep 1)

      ;; Phase 4: Operations during migration
      (gen/log "Operations during migration")
      (->> (gen/mix [(fn [] (write-key test-key (next-val)))
                     (fn [] (read-key test-key))])
           (gen/limit 50)
           (gen/stagger 0.05))

      ;; Phase 5: Migrate keys
      (gen/log "Migrating keys")
      (gen/once (migrate-keys))

      ;; Phase 6: More operations during key migration
      (->> (gen/mix [(fn [] (write-key test-key (next-val)))
                     (fn [] (read-key test-key))])
           (gen/limit 20)
           (gen/stagger 0.1))

      ;; Phase 6b: Open a transaction against the *current* owner and leave it
      ;; queued across the ownership change below.
      (gen/log "Queuing a MULTI that will straddle the ownership change")
      (gen/once (queued-txn txn-key (next-val)))

      ;; Phase 7: Complete migration
      (gen/log "Completing migration")
      (gen/once (finish-migration))
      (gen/sleep 2)

      ;; Phase 7b: EXEC the straddling transaction. It must either be refused
      ;; with a bare redirect or execute somewhere that still owns the slot —
      ;; never commit on the node that just lost it.
      (gen/log "EXEC-ing the transaction queued before the ownership change")
      (gen/once (exec-queued-txn))
      (gen/sleep 1)

      ;; Phase 8: Verify new owner
      (gen/log "Verifying new slot owner")
      (gen/once (read-slot-owner (cluster-client/slot-for-key test-key)))

      ;; Phase 8b: Direct per-node sweep for keys orphaned on a former owner.
      (gen/log "Checking for orphaned keys on former slot owners")
      (gen/once (read-orphans (cluster-client/slot-for-key test-key)))

      ;; Phase 9: Final operations
      (gen/log "Final operations")
      (->> (gen/mix [(fn [] (write-key test-key (next-val)))
                     (fn [] (read-key test-key))])
           (gen/limit 20)
           (gen/stagger 0.1)))))

;; ===========================================================================
;; Checker
;; ===========================================================================

;; Number of trailing reads treated as the post-migration quiescent window.
;; core.clj appends a final read phase (10 single-key reads issued AFTER the
;; nemesis has healed and a 5s settle) to every workload that has no
;; :final-generator of its own — so the last reads in the history are taken
;; against a recovered, quiescent cluster with no concurrent writes. A nil or a
;; value never written, observed there, is a real post-migration data fault.
(def final-read-window 10)

(defn- redirect-leaked-as-info?
  "True if op is an :info result caused by an unresolved MOVED/ASK/REDIRECT that
   escaped invoke! and was downgraded to :info (either tagged by this workload's
   own catch as [:unexpected msg] with a redirect message, or carrying an
   :exception whose cause is a redirect reply). Determinate redirect exhaustion
   now surfaces as :fail :too-many-redirects and is counted separately; this only
   catches the residual indeterminate cases, for reporting."
  [op]
  (when (= :info (:type op))
    (let [e (:error op)
          tagged-msg (when (and (vector? e) (= :unexpected (first e))) (second e))
          ex-msg (some-> op :exception :cause str)
          redirect? (fn [m] (and m (or (str/starts-with? m "MOVED")
                                       (str/starts-with? m "ASK")
                                       (str/starts-with? m "REDIRECT"))))]
      (boolean (or (redirect? tagged-msg) (redirect? ex-msg))))))

(defn checker
  "Checker for the slot-migration workload.

   The workload writes a single hash-tagged register ({migration-test}:key)
   through a slot migration (optionally under a partition nemesis). Write values
   are globally unique per run (a monotonic counter), so a read can be correlated
   with the writes that actually produced its value. Five properties gate
   :valid? — the first three are the data/routing safety net that holds under any
   fault schedule, the fourth confirms the migration's control-plane outcome, and
   the fifth catches writes that landed on a node the cluster no longer routes to:

   - Durability (gates): every acknowledged write must remain readable. Asserted
     on the post-migration quiescent read window (the trailing `final-read-window`
     reads, which core.clj issues after the nemesis heals and the cluster settles
     — every write has completed before them). A nil there is a lost acknowledged
     write. Reads before the first acknowledged write are legally nil and are
     excluded, so this is sound with no false positives. Note the Raft cluster's
     shards are single-owner with no per-shard replication and these workloads use
     no kill nemesis (slot-migration = none, slot-migration-partition =
     partition), so a nil is genuine loss, not expected single-master kill loss.

   - Value-correctness (gates): every successful non-nil read must return a value
     that was actually written to that key. Values are globally unique, so a value
     that was never written (fabrication) or one that leaked from another slot
     (cross-slot contamination) is caught. Timing-independent — a GET must never
     return bytes that were never SET — so it always gates.

   - Unresolved redirects (gates): no data op may exhaust its MOVED/ASK redirect
     budget (:too-many-redirects). During a single-slot migration a burst of
     redirects is expected and the cluster client resolves them internally; a
     redirect that survives the client's retry budget is a routing breakdown, and
     on correct code there are none.

   - Final ownership (gates when the migration completed): if a finish-migration
     succeeded, the final slot-owner read must equal the migration's intended
     destination node. When no migration completed (e.g. finish blocked under a
     partition), ownership is reported but not asserted, so the partition variant
     does not false-positive; durability/value-correctness still gate the run.

   - No orphaned writes (gates, scoped to transaction keys): after the slot has
     changed hands, no node other than its current owner may physically hold a
     key the straddling transaction wrote. This is the property the
     `:queued-txn` / `:exec-queued-txn` pair exists to stress — a transaction
     queued before the migration and EXEC'd after it must not commit on the
     former owner. Every other op in this workload goes through the cluster
     client, which follows MOVED to the new owner and therefore *cannot* observe
     such an orphan; `:read-orphans` reads each non-owner node directly with
     `CLUSTER GETKEYSINSLOT`, which reports what a node physically stores rather
     than what it would route. Orphans belonging to *other* keys are reported
     under :orphaned-keys but do not gate: the ordinary `:write` generator also
     runs through finalization, where the bounded raft-apply window can leave a
     key on the former owner for an interval this workload does not control.

   A migration must have started at all (sanity) for the run to be meaningful.

   Not gated (reported only): whether the converged final value equals the last
   acknowledged write. On a single register under concurrent writes, the settled
   value is whichever write linearized last, which is not necessarily the highest
   counter, so an exact match is not soundly assertable without a linearizability
   model. It is surfaced as :final-read-matches-last-acked-write for visibility."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [completed (filter #(= :ok (:type %)) history)
            failed    (filter #(= :fail (:type %)) history)
            info-ops  (filter #(= :info (:type %)) history)

            writes (filter #(= :write (:f %)) completed)
            reads  (filter #(= :read (:f %)) completed)

            ;; key -> set of acknowledged written values
            acked (reduce (fn [m w]
                            (let [k (get-in w [:value :key])
                                  v (get-in w [:value :written])]
                              (if (and (some? k) (some? v))
                                (update m k (fnil conj #{}) v)
                                m)))
                          {} writes)
            written-keys (set (keys acked))
            ;; Value of the last acknowledged write in history order (reporting).
            last-acked-write (get-in (last writes) [:value :written])

            ;; Durability window: reads that occur strictly after the first
            ;; acknowledged write (so pre-write nil reads never count as loss),
            ;; then the trailing quiescent window. On this single register those
            ;; trailing reads are the post-heal, post-settle final reads.
            reads-after-first-write (->> (drop-while #(not= :write (:f %)) completed)
                                         rest
                                         (filter #(= :read (:f %))))
            final-reads (take-last final-read-window reads-after-first-write)
            lost-writes (->> final-reads
                             (filter (fn [r]
                                       (let [k (get-in r [:value :key])]
                                         (and (contains? written-keys k)
                                              (nil? (get-in r [:value :value]))))))
                             (map #(get-in % [:value :key]))
                             distinct
                             vec)
            durable? (empty? lost-writes)

            ;; Value-correctness across all reads (fabrication/contamination).
            wrong-value-reads (->> reads
                                   (keep (fn [r]
                                           (let [k  (get-in r [:value :key])
                                                 v  (get-in r [:value :value])
                                                 ks (get acked k)]
                                             (when (and (some? v) ks (not (contains? ks v)))
                                               {:key k :read v}))))
                                   vec)
            values-correct? (empty? wrong-value-reads)

            ;; Converged final register value(s) and the last-write match signal.
            final-values (->> final-reads
                              (map #(get-in % [:value :value]))
                              (remove nil?)
                              distinct
                              vec)
            final-reads-converged? (<= (count final-values) 1)
            final-read-matches-last-acked-write?
            (boolean (and (= 1 (count final-values))
                          (= (first final-values) last-acked-write)))

            ;; Unresolved redirects: data ops that exhausted the redirect budget.
            too-many-redirect-fails (filter #(= :too-many-redirects (:error %)) failed)
            redirect-count (count too-many-redirect-fails)
            redirects-ok? (zero? redirect-count)
            ;; Control-plane REDIRECT fails and info-leaked MOVED/ASK (reporting).
            control-redirect-fails (filter #(let [e (:error %)]
                                              (and (vector? e) (= :redirect (first e))))
                                            failed)
            info-redirects (filter redirect-leaked-as-info? info-ops)

            ;; Migration control-plane outcome.
            start-ops  (filter #(= :start-migration (:f %)) completed)
            finish-ops (filter #(= :finish-migration (:f %)) completed)
            migration-started?   (seq start-ops)
            migration-completed? (seq finish-ops)
            intended-dest (get-in (last start-ops) [:value :dest])

            owner-reads (filter #(= :read-slot-owner (:f %)) completed)
            owners      (map #(get-in % [:value :owner]) owner-reads)
            final-owner (get-in (last owner-reads) [:value :owner])
            ;; Assert owner == destination only when a migration actually
            ;; completed; otherwise ownership is reported, not gated.
            final-owner-correct? (or (not migration-completed?)
                                     (nil? intended-dest)
                                     (= final-owner intended-dest))

            ;; Orphaned writes: keys of a migrated slot still physically present
            ;; on a node that no longer owns it. Only :read-orphans can see
            ;; these; every other op follows MOVED away from them.
            orphan-reads (filter #(= :read-orphans (:f %)) completed)
            orphaned-keys (->> orphan-reads
                               (mapcat (fn [o]
                                         (for [[node ks] (get-in o [:value :orphans])]
                                           {:node node
                                            :slot (get-in o [:value :slot])
                                            :keys (vec ks)})))
                               vec)

            ;; The straddling transaction's outcome (reported, not gated: both
            ;; "refused with a redirect" and "executed on the current owner" are
            ;; correct; only an orphan is a fault).
            queued-txn-ops (filter #(= :queued-txn (:f %)) completed)

            ;; Gate only on keys the straddling transaction touched. The
            ;; ordinary :write generator runs straight through finalization as
            ;; well, and the documented raft-apply window (see
            ;; .scratch/replication-cluster-rework/issues/02-...) can leave one
            ;; of its keys on the former owner for a bounded interval — a known,
            ;; accepted race with nothing to do with transactions. Every orphan
            ;; is still reported; only a transactional one fails the run.
            txn-keys (->> queued-txn-ops
                          (keep #(get-in % [:value :key]))
                          set)
            orphaned-txn-keys (->> orphaned-keys
                                   (keep (fn [o]
                                           (let [ks (filterv txn-keys (:keys o))]
                                             (when (seq ks) (assoc o :keys ks)))))
                                   vec)
            no-orphans? (empty? orphaned-txn-keys)
            exec-txn-ops   (filter #(and (= :exec-queued-txn (:f %))
                                         (#{:ok :fail} (:type %)))
                                   history)
            exec-txn-outcomes (->> exec-txn-ops
                                   (keep #(get-in % [:value :outcome]))
                                   frequencies)]

        {:valid? (boolean (and migration-started?
                               durable?
                               values-correct?
                               redirects-ok?
                               final-owner-correct?
                               no-orphans?))
         :migration-started?   (boolean migration-started?)
         :migration-completed? (boolean migration-completed?)
         :durable? durable?
         :lost-write-keys lost-writes
         :lost-write-count (count lost-writes)
         :values-correct? values-correct?
         :wrong-value-reads (vec (take 20 wrong-value-reads))
         :wrong-value-count (count wrong-value-reads)
         :final-owner-correct? final-owner-correct?
         :intended-dest intended-dest
         :final-owner final-owner
         :no-orphaned-writes? no-orphans?
         :orphaned-txn-keys (vec (take 20 orphaned-txn-keys))
         :orphaned-keys (vec (take 20 orphaned-keys))
         :orphan-read-count (count orphan-reads)
         :queued-txn-count (count queued-txn-ops)
         :exec-queued-txn-outcomes exec-txn-outcomes
         :redirects-ok? redirects-ok?
         :too-many-redirect-count redirect-count
         :control-redirect-fail-count (count control-redirect-fails)
         :info-redirect-count (count info-redirects)
         :final-reads-converged? final-reads-converged?
         :final-values final-values
         :last-acked-write last-acked-write
         :final-read-matches-last-acked-write final-read-matches-last-acked-write?
         :owners-seen (set owners)
         :owner-changes (count (partition-by identity owners))
         :migrations-started (count start-ops)
         :migrations-completed (count finish-ops)
         :written-keys (count written-keys)
         :total-writes (count writes)
         :total-reads (count reads)
         :final-read-count (count final-reads)
         :failed-ops (count failed)}))))

;; ===========================================================================
;; Workload
;; ===========================================================================

(defn workload
  "Construct a slot migration workload.

   Options:
   - :rate - operations per second"
  [opts]
  {:client (create-client)
   :generator (generator opts)
   :checker (checker)})
