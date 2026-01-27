(ns jepsen.frogdb.cluster-client
  "Cluster-aware Redis/RESP client for FrogDB with MOVED/ASK redirect handling.

   Provides:
   - Smart routing based on slot mapping
   - Automatic MOVED redirect following
   - ASK redirect handling during slot migration
   - CRC16 slot calculation with hash tag support
   - Slot-to-node mapping cache with refresh

   Designed for testing FrogDB's Redis Cluster compatible mode."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn debug]]
            [jepsen.client :as client]
            [jepsen.frogdb.client :as frogdb]
            [jepsen.frogdb.cluster-db :as cluster-db]
            [slingshot.slingshot :refer [try+ throw+]]
            [taoensso.carmine :as car :refer [wcar]])
  (:import [java.util.zip CRC32]))

;; ===========================================================================
;; Constants
;; ===========================================================================

(def total-slots 16384)
(def max-redirects 16)

;; ===========================================================================
;; CRC16 Implementation (XMODEM variant for Redis)
;; ===========================================================================

(def ^:private crc16-table
  "CRC16 lookup table for XMODEM polynomial (0x1021)."
  (let [table (int-array 256)]
    (dotimes [i 256]
      (let [crc (loop [crc (bit-shift-left i 8)
                       j 0]
                  (if (< j 8)
                    (recur (if (not= 0 (bit-and crc 0x8000))
                             (bit-xor (bit-shift-left crc 1) 0x1021)
                             (bit-shift-left crc 1))
                           (inc j))
                    crc))]
        (aset table i (bit-and crc 0xFFFF))))
    table))

(defn crc16
  "Calculate CRC16 (XMODEM) of a byte array or string.
   This matches Redis's CRC16 implementation for slot calculation."
  [^bytes data]
  (let [table crc16-table]
    (loop [crc 0
           i 0]
      (if (< i (alength data))
        (recur (bit-xor (bit-shift-left crc 8)
                        (aget table (bit-and (bit-xor (bit-shift-right crc 8)
                                                       (aget data i))
                                              0xFF)))
               (inc i))
        (bit-and crc 0xFFFF)))))

(defn crc16-str
  "Calculate CRC16 of a string."
  [^String s]
  (crc16 (.getBytes s "UTF-8")))

;; ===========================================================================
;; Hash Tag Support
;; ===========================================================================

(defn extract-hash-tag
  "Extract the hash tag from a key if present.
   Hash tags are enclosed in {} and determine slot routing.
   e.g., 'user:{123}:name' -> '123'
         '{foo}bar' -> 'foo'
         'nohashtag' -> 'nohashtag'"
  [^String key]
  (let [start (.indexOf key "{")]
    (if (neg? start)
      key
      (let [end (.indexOf key "}" (inc start))]
        (if (or (neg? end) (= end (inc start)))
          key  ; Empty {} or no closing }
          (.substring key (inc start) end))))))

(defn slot-for-key
  "Calculate the slot number for a key (0-16383).
   Handles hash tags for co-locating related keys."
  [^String key]
  (let [hash-part (extract-hash-tag key)]
    (mod (crc16-str hash-part) total-slots)))

;; ===========================================================================
;; Slot Mapping Cache
;; ===========================================================================

(defrecord SlotMapping [slots-to-nodes node-conns])

(defn parse-cluster-slots
  "Parse CLUSTER SLOTS response into a slot->node map.
   Response format: [[start end [ip port node-id] [replica-ip replica-port replica-id] ...] ...]"
  [slots-response]
  (when (seq slots-response)
    (reduce (fn [acc slot-info]
              (let [start (long (nth slot-info 0))
                    end (long (nth slot-info 1))
                    master-info (nth slot-info 2)
                    master-addr (str (nth master-info 0) ":" (nth master-info 1))]
                (reduce (fn [m slot]
                          (assoc m slot master-addr))
                        acc
                        (range start (inc end)))))
            {}
            slots-response)))

(defn refresh-slot-mapping
  "Refresh the slot-to-node mapping from the cluster.
   Returns updated SlotMapping record."
  [slot-mapping node docker-host?]
  (let [conn (cluster-db/conn-for-raft-node node docker-host?)
        slots-response (cluster-db/cluster-slots conn)
        new-slots-to-nodes (parse-cluster-slots slots-response)]
    (assoc slot-mapping :slots-to-nodes new-slots-to-nodes)))

(defn create-slot-mapping
  "Create a new SlotMapping by querying cluster."
  [nodes docker-host?]
  (let [mapping (->SlotMapping {} {})]
    (refresh-slot-mapping mapping (first nodes) docker-host?)))

(defn get-node-for-slot
  "Get the node address (ip:port) for a slot from the mapping."
  [slot-mapping slot]
  (get (:slots-to-nodes slot-mapping) slot))

;; ===========================================================================
;; Redirect Parsing
;; ===========================================================================

(defn parse-redirect-error
  "Parse a MOVED or ASK error response.
   Format: 'MOVED 3999 127.0.0.1:6381' or 'ASK 3999 127.0.0.1:6381'
   Returns {:type :moved/:ask, :slot <n>, :host <ip>, :port <port>} or nil."
  [error-msg]
  (when (string? error-msg)
    (cond
      (str/starts-with? error-msg "MOVED ")
      (let [parts (str/split error-msg #" ")
            slot (Long/parseLong (nth parts 1))
            [host port] (str/split (nth parts 2) #":")]
        {:type :moved
         :slot slot
         :host host
         :port (Integer/parseInt port)})

      (str/starts-with? error-msg "ASK ")
      (let [parts (str/split error-msg #" ")
            slot (Long/parseLong (nth parts 1))
            [host port] (str/split (nth parts 2) #":")]
        {:type :ask
         :slot slot
         :host host
         :port (Integer/parseInt port)})

      (str/starts-with? error-msg "CROSSSLOT")
      {:type :crossslot}

      (str/starts-with? error-msg "CLUSTERDOWN")
      {:type :clusterdown}

      :else nil)))

(defn is-redirect-error?
  "Check if an exception represents a MOVED or ASK redirect."
  [e]
  (when (instance? clojure.lang.ExceptionInfo e)
    (let [data (ex-data e)]
      (when (= (:prefix data) :carmine)
        (parse-redirect-error (:message data))))))

;; ===========================================================================
;; Redirect-Aware Command Execution
;; ===========================================================================

(defn make-conn
  "Create a connection spec for a host:port."
  [host port]
  {:pool {}
   :spec {:host host
          :port port
          :timeout-ms frogdb/default-timeout-ms}})

(defn execute-asking
  "Send ASKING command before executing a command (for ASK redirects)."
  [conn cmd-vec]
  (wcar conn
    (car/redis-call ["ASKING"])
    (apply car/redis-call cmd-vec)))

(defn execute-with-redirect
  "Execute a command with automatic MOVED/ASK redirect handling.

   Arguments:
   - slot-mapping: Current slot mapping (atom)
   - initial-node: Node to try first (or nil to use slot mapping)
   - key: Key being operated on (for slot calculation)
   - cmd-vec: Command as a vector, e.g., [\"GET\" \"mykey\"]
   - docker-host?: Whether running from Docker host
   - nodes: All cluster nodes (for slot mapping refresh)

   Returns: {:value <result>, :redirects <count>} or throws on error."
  [slot-mapping-atom initial-node key cmd-vec docker-host? nodes]
  (let [slot (when key (slot-for-key key))
        initial-addr (or (when initial-node
                           (let [info (get cluster-db/raft-cluster-host-ports initial-node)]
                             (str (:host info) ":" (:port info))))
                         (when slot
                           (get-node-for-slot @slot-mapping-atom slot))
                         ;; Fallback to first node
                         (let [info (get cluster-db/raft-cluster-host-ports (first nodes))]
                           (str (:host info) ":" (:port info))))]
    (loop [addr initial-addr
           redirects 0
           asking? false]
      (when (>= redirects max-redirects)
        (throw+ {:type :too-many-redirects
                 :redirects redirects
                 :last-addr addr}))

      (let [[host port-str] (str/split addr #":")
            port (Integer/parseInt port-str)
            conn (make-conn host port)
            ;; Execute and capture result or redirect info
            result-or-redirect
            (try+
              {:type :success
               :value (if asking?
                        (execute-asking conn cmd-vec)
                        (wcar conn (apply car/redis-call cmd-vec)))}
              (catch clojure.lang.ExceptionInfo e
                (if-let [redirect (is-redirect-error? e)]
                  (case (:type redirect)
                    :moved
                    (do
                      (debug "MOVED redirect:" (:slot redirect) "->" (:host redirect) ":" (:port redirect))
                      ;; Update slot mapping
                      (swap! slot-mapping-atom
                             (fn [m] (assoc-in m [:slots-to-nodes (:slot redirect)]
                                               (str (:host redirect) ":" (:port redirect)))))
                      {:type :redirect
                       :addr (str (:host redirect) ":" (:port redirect))
                       :asking? false})

                    :ask
                    (do
                      (debug "ASK redirect:" (:slot redirect) "->" (:host redirect) ":" (:port redirect))
                      {:type :redirect
                       :addr (str (:host redirect) ":" (:port redirect))
                       :asking? true})

                    :crossslot
                    (throw+ {:type :crossslot
                             :message "Keys in request don't hash to the same slot"})

                    :clusterdown
                    (throw+ {:type :clusterdown
                             :message "Cluster is down or unavailable"}))
                  ;; Not a redirect error, rethrow
                  (throw e))))]
        ;; Handle result outside try block to allow recur
        (case (:type result-or-redirect)
          :success {:value (:value result-or-redirect)
                    :redirects redirects}
          :redirect (recur (:addr result-or-redirect)
                           (inc redirects)
                           (:asking? result-or-redirect)))))))

;; ===========================================================================
;; High-Level Cluster Operations
;; ===========================================================================

(defn cluster-get
  "GET a key with automatic redirect handling."
  [slot-mapping-atom key docker-host? nodes]
  (:value (execute-with-redirect slot-mapping-atom nil key ["GET" key] docker-host? nodes)))

(defn cluster-set
  "SET a key with automatic redirect handling."
  [slot-mapping-atom key value docker-host? nodes]
  (:value (execute-with-redirect slot-mapping-atom nil key ["SET" key (str value)] docker-host? nodes)))

(defn cluster-incr
  "INCR a key with automatic redirect handling."
  [slot-mapping-atom key docker-host? nodes]
  (:value (execute-with-redirect slot-mapping-atom nil key ["INCR" key] docker-host? nodes)))

(defn cluster-del
  "DEL a key with automatic redirect handling."
  [slot-mapping-atom key docker-host? nodes]
  (:value (execute-with-redirect slot-mapping-atom nil key ["DEL" key] docker-host? nodes)))

;; ===========================================================================
;; Jepsen Client Implementation
;; ===========================================================================

(defrecord ClusterClient [slot-mapping nodes docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          all-nodes (or (:cluster-nodes test) ["n1" "n2" "n3"])]
      (info "Opening cluster client to" node "(docker-host?:" docker? ")")
      (assoc this
             :nodes all-nodes
             :docker-host? docker?
             :slot-mapping (atom (create-slot-mapping all-nodes docker?)))))

  (setup! [this test]
    ;; Refresh slot mapping
    (when-let [mapping @slot-mapping]
      (reset! slot-mapping (refresh-slot-mapping mapping (first nodes) docker-host?)))
    this)

  (invoke! [this test op]
    ;; Base implementation - specific workloads will extend this
    (throw (ex-info "invoke! must be implemented by workload-specific client"
                    {:op op})))

  (teardown! [this test]
    nil)

  (close! [this test]
    nil))

(defn create-cluster-client
  "Create a new cluster-aware client instance."
  []
  (map->ClusterClient {}))

;; ===========================================================================
;; Register Operations for Cluster Mode
;; ===========================================================================

(defrecord ClusterRegisterClient [slot-mapping nodes docker-host?]
  client/Client

  (open! [this test node]
    (let [docker? (get test :docker true)
          all-nodes (or (:cluster-nodes test) ["n1" "n2" "n3"])]
      (info "Opening cluster register client to" node "(docker-host?:" docker? ")")
      (assoc this
             :nodes all-nodes
             :docker-host? docker?
             :slot-mapping (atom (create-slot-mapping all-nodes docker?)))))

  (setup! [this test]
    (when slot-mapping
      (reset! slot-mapping (refresh-slot-mapping @slot-mapping (first nodes) docker-host?)))
    this)

  (invoke! [this test op]
    (frogdb/with-error-handling op
      (let [key (str "jepsen-reg-" (get-in op [:value 0] 0))]
        (case (:f op)
          :read
          (let [value (cluster-get slot-mapping key docker-host? nodes)]
            (assoc op :type :ok :value (frogdb/parse-value value)))

          :write
          (do
            (cluster-set slot-mapping key (:value op) docker-host? nodes)
            (assoc op :type :ok))

          :cas
          (let [[expected new-value] (:value op)]
            ;; CAS requires WATCH which may not work well with redirects
            ;; For now, just do optimistic write if value matches
            (let [current (frogdb/parse-value (cluster-get slot-mapping key docker-host? nodes))]
              (if (= current expected)
                (do
                  (cluster-set slot-mapping key new-value docker-host? nodes)
                  (assoc op :type :ok))
                (assoc op :type :fail))))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    nil))

(defn create-cluster-register-client
  "Create a cluster-aware register client."
  []
  (map->ClusterRegisterClient {}))

;; ===========================================================================
;; Hash-Tagged Key Operations
;; ===========================================================================

(defn hash-tagged-key
  "Create a hash-tagged key that routes to the same slot as the tag.
   e.g., (hash-tagged-key \"user\" \"123\" \"name\") -> \"user:{123}:name\""
  [prefix tag suffix]
  (str prefix ":{" tag "}:" suffix))

(defn same-slot-keys
  "Generate n keys that all hash to the same slot using hash tags.
   Returns a vector of keys like [\"{tag}:0\" \"{tag}:1\" ...]"
  [tag n]
  (mapv #(str "{" tag "}:" %) (range n)))

(defn verify-same-slot
  "Verify that all given keys hash to the same slot."
  [keys]
  (let [slots (map slot-for-key keys)]
    (apply = slots)))
