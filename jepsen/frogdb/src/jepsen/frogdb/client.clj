(ns jepsen.frogdb.client
  "Redis/RESP client wrapper for FrogDB using Carmine.

   FrogDB implements the RESP2/RESP3 protocol, so we can use
   standard Redis clients like Carmine to communicate with it."
  (:require [clojure.tools.logging :refer [info warn debug]]
            [jepsen.client :as client]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]]))

(def default-port 6379)
(def default-timeout-ms 5000)

;; When running in Docker mode from the host, node "n1" maps to localhost:6379
(def docker-host-map
  {"n1" {:host "localhost" :port 6379}
   "n2" {:host "localhost" :port 6380}
   "n3" {:host "localhost" :port 6381}})

(defn conn-spec
  "Create a Carmine connection spec for the given node.
   If docker-host? is true, resolves node names to localhost ports."
  ([node]
   (conn-spec node default-port false))
  ([node port]
   (conn-spec node port false))
  ([node port docker-host?]
   (let [resolved (if (and docker-host? (get docker-host-map node))
                    (get docker-host-map node)
                    {:host node :port port})]
     {:pool {}
      :spec {:host (:host resolved)
             :port (:port resolved)
             :timeout-ms default-timeout-ms}})))

(defn parse-value
  "Parse a value returned from Redis/FrogDB.
   Handles nil, strings, integers, etc."
  [v]
  (cond
    (nil? v) nil
    (string? v) (try (Long/parseLong v)
                     (catch NumberFormatException _ v))
    (integer? v) v
    :else v))

(defmacro with-error-handling
  "Wrap operations with error handling for Jepsen.
   Returns {:type :ok/:fail/:info, :error message} on failures."
  [op & body]
  `(try+
     ~@body
     (catch java.net.ConnectException e#
       (assoc ~op :type :fail :error :connection-refused))
     (catch java.net.SocketTimeoutException e#
       (assoc ~op :type :info :error :timeout))
     (catch java.io.IOException e#
       (assoc ~op :type :info :error [:io-exception (.getMessage e#)]))
     (catch clojure.lang.ExceptionInfo e#
       (let [data# (ex-data e#)]
         (if (= (:prefix data#) :carmine)
           (assoc ~op :type :fail :error [:redis-error (:message data#)])
           (throw e#))))
     (catch Exception e#
       (warn "Unexpected error:" e#)
       (assoc ~op :type :info :error [:unexpected (.getMessage e#)]))))

;; ===========================================================================
;; Register Operations (GET/SET)
;; ===========================================================================

(defn read-register
  "Read a register value using GET."
  [conn key]
  (parse-value (wcar conn (car/get key))))

(defn write-register!
  "Write a register value using SET."
  [conn key value]
  (wcar conn (car/set key (str value))))

(defn cas-register!
  "Compare-and-swap a register value.
   Returns true if successful, false if the expected value didn't match."
  [conn key expected new-value]
  ;; FrogDB may support CAS via WATCH/MULTI/EXEC or a CAS command
  ;; For now, use optimistic locking with WATCH
  (wcar conn
    (car/watch key)
    (let [current (parse-value (car/get key))]
      (if (= current expected)
        (do
          (car/multi)
          (car/set key (str new-value))
          (let [result (car/exec)]
            (boolean result)))
        (do
          (car/unwatch)
          false)))))

;; ===========================================================================
;; Counter Operations (INCR)
;; ===========================================================================

(defn read-counter
  "Read a counter value using GET."
  [conn key]
  (let [v (wcar conn (car/get key))]
    (if (nil? v)
      0
      (parse-value v))))

(defn incr-counter!
  "Increment a counter using INCRBY."
  [conn key delta]
  (wcar conn (car/incrby key delta)))

;; ===========================================================================
;; List Operations (LPUSH/RPUSH/LPOP/LINDEX/LRANGE)
;; ===========================================================================

(defn rpush!
  "Push value to the tail of a list using RPUSH."
  [conn key value]
  (wcar conn (car/rpush key (str value))))

(defn lpush!
  "Push value to the head of a list using LPUSH."
  [conn key value]
  (wcar conn (car/lpush key (str value))))

(defn lpop!
  "Pop value from the head of a list using LPOP."
  [conn key]
  (parse-value (wcar conn (car/lpop key))))

(defn lindex
  "Get element at index using LINDEX."
  [conn key index]
  (parse-value (wcar conn (car/lindex key index))))

(defn lrange
  "Get elements in range using LRANGE."
  [conn key start stop]
  (wcar conn (car/lrange key start stop)))

(defn llen
  "Get list length using LLEN."
  [conn key]
  (wcar conn (car/llen key)))

;; ===========================================================================
;; Set Operations (SADD/SREM/SMEMBERS/SISMEMBER)
;; ===========================================================================

(defn sadd!
  "Add member to set using SADD. Returns 1 if added, 0 if already present."
  [conn key member]
  (wcar conn (car/sadd key (str member))))

(defn srem!
  "Remove member from set using SREM. Returns 1 if removed, 0 if not present."
  [conn key member]
  (wcar conn (car/srem key (str member))))

(defn smembers
  "Get all members of a set using SMEMBERS."
  [conn key]
  (set (wcar conn (car/smembers key))))

(defn sismember
  "Check if member is in set using SISMEMBER."
  [conn key member]
  (= 1 (wcar conn (car/sismember key (str member)))))

;; ===========================================================================
;; Hash Operations (HSET/HGET/HGETALL)
;; ===========================================================================

(defn hset!
  "Set a hash field using HSET."
  [conn key field value]
  (wcar conn (car/hset key (str field) (str value))))

(defn hget
  "Get a hash field using HGET."
  [conn key field]
  (parse-value (wcar conn (car/hget key (str field)))))

(defn hgetall
  "Get all fields and values from a hash using HGETALL."
  [conn key]
  (let [flat (wcar conn (car/hgetall key))]
    ;; Convert flat list [k1 v1 k2 v2 ...] to map
    (apply hash-map flat)))

;; ===========================================================================
;; String Operations (APPEND)
;; ===========================================================================

(defn append!
  "Append value to a string using APPEND. Returns new length."
  [conn key value]
  (wcar conn (car/append key (str value))))

(defn get-string
  "Get a string value using GET (without parsing to integer)."
  [conn key]
  (wcar conn (car/get key)))

;; ===========================================================================
;; Transaction Operations (MULTI/EXEC)
;; ===========================================================================

(defn multi-set!
  "Set multiple keys atomically using MULTI/EXEC.
   Takes a map of {key value} pairs."
  [conn kvs]
  (wcar conn
    (car/multi)
    (doseq [[k v] kvs]
      (car/set (str k) (str v)))
    (car/exec)))

(defn multi-get
  "Get multiple keys atomically using MULTI/EXEC."
  [conn keys]
  (wcar conn
    (car/multi)
    (doseq [k keys]
      (car/get (str k)))
    (let [result (car/exec)]
      (zipmap keys (map parse-value result)))))

(defn transfer!
  "Transfer amount from one key to another atomically.
   Uses MULTI/EXEC for atomicity. Returns true if successful."
  [conn from-key to-key amount]
  ;; Use WATCH for optimistic locking
  (wcar conn
    (car/watch from-key to-key)
    (let [from-val (or (parse-value (car/get from-key)) 0)
          to-val (or (parse-value (car/get to-key)) 0)]
      (if (>= from-val amount)
        (do
          (car/multi)
          (car/set from-key (str (- from-val amount)))
          (car/set to-key (str (+ to-val amount)))
          (boolean (car/exec)))
        (do
          (car/unwatch)
          false)))))

;; ===========================================================================
;; Jepsen Client Protocol Implementation
;; ===========================================================================

(defrecord FrogDBClient [conn node]
  client/Client

  (open! [this test node]
    (info "Opening connection to" node)
    (assoc this
           :conn (conn-spec node)
           :node node))

  (setup! [this test]
    ;; Any one-time setup can go here
    this)

  (invoke! [this test op]
    ;; Dispatch is handled by the specific workload clients
    ;; This is a base client that can be extended
    (throw (ex-info "invoke! must be implemented by workload-specific client"
                    {:op op})))

  (teardown! [this test]
    ;; Cleanup
    nil)

  (close! [this test]
    ;; Carmine handles connection pooling, so nothing to explicitly close
    nil))

(defn create-client
  "Create a new FrogDB client instance."
  []
  (map->FrogDBClient {}))

;; ===========================================================================
;; Health Check
;; ===========================================================================

(defn ping
  "Send a PING to verify the connection."
  [conn]
  (try+
    (= "PONG" (wcar conn (car/ping)))
    (catch Object _
      false)))

(defn wait-for-ready
  "Wait for FrogDB to be ready to accept connections."
  [node & {:keys [timeout-ms interval-ms]
           :or {timeout-ms 30000 interval-ms 500}}]
  (let [conn (conn-spec node)
        deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (cond
        (ping conn)
        true

        (> (System/currentTimeMillis) deadline)
        (throw+ {:type :timeout
                 :message (str "Timed out waiting for FrogDB on " node)})

        :else
        (do
          (Thread/sleep interval-ms)
          (recur))))))
