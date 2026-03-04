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
(def default-base-port 16379)

(defn docker-host-map
  "Map of node names to host:port for Docker host access.
   base-port is the starting host port (default 16379)."
  ([] (docker-host-map default-base-port))
  ([base-port]
   {"n1" {:host "localhost" :port base-port}
    "n2" {:host "localhost" :port (+ base-port 1)}
    "n3" {:host "localhost" :port (+ base-port 2)}}))

(defn conn-spec
  "Create a Carmine connection spec for the given node.
   If docker-host? is true, resolves node names to localhost ports.
   base-port controls the starting host port (default 16379)."
  ([node]
   (conn-spec node default-port false))
  ([node port]
   (conn-spec node port false))
  ([node port docker-host?]
   (conn-spec node port docker-host? default-base-port))
  ([node port docker-host? base-port]
   (let [host-map (docker-host-map base-port)
         resolved (if (and docker-host? (get host-map node))
                    (get host-map node)
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
  "Compare-and-swap a register value using Lua EVAL for atomicity.
   Returns true if successful, false if the expected value didn't match."
  [conn key expected new-value]
  (= 1 (wcar conn
    (car/eval
      "if redis.call('GET',KEYS[1])==ARGV[1] then redis.call('SET',KEYS[1],ARGV[2]) return 1 else return 0 end"
      1 key (str expected) (str new-value)))))

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
  "Transfer amount from one key to another atomically using Lua EVAL.
   Returns true if successful, false if insufficient funds."
  [conn from-key to-key amount]
  (= 1 (wcar conn
    (car/eval
      (str "local from = tonumber(redis.call('GET', KEYS[1])) "
           "local to = tonumber(redis.call('GET', KEYS[2])) "
           "if from >= tonumber(ARGV[1]) then "
           "  redis.call('SET', KEYS[1], tostring(from - tonumber(ARGV[1]))) "
           "  redis.call('SET', KEYS[2], tostring(to + tonumber(ARGV[1]))) "
           "  return 1 "
           "else "
           "  return 0 "
           "end")
      2 from-key to-key (str amount)))))

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
  [node & {:keys [timeout-ms interval-ms docker-host? base-port]
           :or {timeout-ms 30000 interval-ms 500 docker-host? true base-port default-base-port}}]
  (let [conn (conn-spec node default-port docker-host? base-port)
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

;; ===========================================================================
;; Multi-Node Connection Support
;; ===========================================================================

(defn node-ports
  "Map of node names to their host ports (for multi-node testing)."
  ([] (node-ports default-base-port))
  ([base-port]
   {"n1" base-port
    "n2" (+ base-port 1)
    "n3" (+ base-port 2)}))

(defn conn-for-node
  "Create a connection spec for a specific node in multi-node mode.
   Uses docker-host-map for port resolution when running from host."
  ([node docker-host?]
   (conn-for-node node docker-host? default-base-port))
  ([node docker-host? base-port]
   (let [host-map (docker-host-map base-port)
         resolved (if docker-host?
                    (get host-map node {:host node :port default-port})
                    {:host node :port default-port})]
     {:pool {}
      :spec {:host (:host resolved)
             :port (:port resolved)
             :timeout-ms default-timeout-ms}})))

(defn all-node-conns
  "Create connections to all nodes in the cluster.
   Returns a map of {node -> conn-spec}."
  ([nodes docker-host?]
   (all-node-conns nodes docker-host? default-base-port))
  ([nodes docker-host? base-port]
   (into {} (for [node nodes]
              [node (conn-for-node node docker-host? base-port)]))))

;; ===========================================================================
;; Replication Commands
;; ===========================================================================

(defn wait!
  "WAIT command - block until numreplicas have acknowledged writes.
   Returns the number of replicas that acknowledged.

   Arguments:
   - numreplicas: Number of replicas that need to ACK (0 = return immediately)
   - timeout-ms: Timeout in milliseconds (0 = block forever)

   Returns: Integer count of replicas that acknowledged."
  [conn numreplicas timeout-ms]
  (wcar conn (car/redis-call ["WAIT" numreplicas timeout-ms])))

(defn role
  "ROLE command - get the role of the server (master/slave).
   Returns a vector with role info:
   - For master: [\"master\" offset [[replica-ip port offset] ...]]
   - For slave: [\"slave\" master-ip master-port state offset]"
  [conn]
  (wcar conn (car/redis-call ["ROLE"])))

(defn is-primary?
  "Check if the connected node is a primary (master)."
  [conn]
  (let [r (role conn)]
    (and (vector? r) (= "master" (first r)))))

(defn is-replica?
  "Check if the connected node is a replica (slave)."
  [conn]
  (let [r (role conn)]
    (and (vector? r) (= "slave" (first r)))))

(defn info-replication
  "INFO REPLICATION command - get replication info.
   Returns a map of replication-related info."
  [conn]
  (let [info-str (wcar conn (car/info "replication"))]
    (when info-str
      (->> (clojure.string/split info-str #"\r?\n")
           (remove clojure.string/blank?)
           (remove #(clojure.string/starts-with? % "#"))
           (map #(clojure.string/split % #":"))
           (filter #(= 2 (count %)))
           (into {})))))

(defn replicaof!
  "REPLICAOF command - configure replication.
   Use host/port to replicate from a master.
   Use \"NO\" \"ONE\" to promote to master (stop replicating)."
  [conn host port]
  (wcar conn (car/redis-call ["REPLICAOF" host port])))

(defn slaveof!
  "SLAVEOF command - alias for REPLICAOF (for compatibility)."
  [conn host port]
  (wcar conn (car/redis-call ["SLAVEOF" host port])))

(defn promote-to-primary!
  "Promote a replica to primary by issuing REPLICAOF NO ONE."
  [conn]
  (replicaof! conn "NO" "ONE"))

(defn get-replication-offset
  "Get the current replication offset from INFO REPLICATION."
  [conn]
  (let [info (info-replication conn)]
    (when-let [offset-str (or (get info "master_repl_offset")
                              (get info "slave_repl_offset"))]
      (Long/parseLong offset-str))))

(defn get-connected-replicas
  "Get the number of connected replicas from INFO REPLICATION."
  [conn]
  (let [info (info-replication conn)]
    (when-let [count-str (get info "connected_slaves")]
      (Integer/parseInt count-str))))

;; ===========================================================================
;; Write Durability Helpers
;; ===========================================================================

(defn write-durable!
  "Write a value with durability guarantee.
   Waits for at least one replica to acknowledge.

   Arguments:
   - key: The key to write
   - value: The value to write
   - replicas: Number of replicas to wait for (default 1)
   - timeout-ms: Wait timeout in ms (default 5000)

   Returns: {:acked n} where n is the number of replicas that acknowledged,
            or {:timeout true :acked n} if timeout occurred."
  ([conn key value]
   (write-durable! conn key value 1 5000))
  ([conn key value replicas timeout-ms]
   (wcar conn (car/set key (str value)))
   (let [acked (wait! conn replicas timeout-ms)]
     (if (>= acked replicas)
       {:acked acked}
       {:timeout true :acked acked}))))

(defn write-async!
  "Write a value without waiting for replica acknowledgment.
   This is the standard SET operation - included for API symmetry."
  [conn key value]
  (wcar conn (car/set key (str value)))
  {:acked 0})
