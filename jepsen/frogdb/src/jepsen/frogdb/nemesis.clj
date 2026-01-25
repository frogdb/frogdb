(ns jepsen.frogdb.nemesis
  "Nemesis implementations for FrogDB testing.

   Provides fault injection including:
   - Process crashes (SIGKILL)
   - Process pauses (SIGSTOP/SIGCONT)
   - Graceful restarts (SIGTERM)

   These nemeses help verify crash recovery and durability properties."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.db :as db]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as nc]
            [jepsen.util :as util]))

;; ===========================================================================
;; Process Kill Nemesis
;; ===========================================================================

(defn process-killer
  "Creates a nemesis that kills and restarts FrogDB processes.

   Supports operations:
   - {:f :kill} - kill FrogDB with SIGKILL
   - {:f :start} - restart FrogDB"
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      this)

    (invoke! [this test op]
      (case (:f op)
        :kill
        (let [nodes (or (:value op) (:nodes test))]
          (info "Killing FrogDB on" nodes)
          (doseq [node (util/coll nodes)]
            (db/kill! (:db test) test node))
          (assoc op :value nodes))

        :start
        (let [nodes (or (:value op) (:nodes test))]
          (info "Starting FrogDB on" nodes)
          (doseq [node (util/coll nodes)]
            (db/start! (:db test) test node))
          ;; Wait for FrogDB to be ready
          (Thread/sleep 2000)
          (assoc op :value nodes))))

    (teardown! [this test]
      nil)))

;; ===========================================================================
;; Process Pause Nemesis
;; ===========================================================================

(defn process-pauser
  "Creates a nemesis that pauses and resumes FrogDB processes.

   Supports operations:
   - {:f :pause} - pause FrogDB with SIGSTOP
   - {:f :resume} - resume FrogDB with SIGCONT"
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      this)

    (invoke! [this test op]
      (case (:f op)
        :pause
        (let [nodes (or (:value op) (:nodes test))]
          (info "Pausing FrogDB on" nodes)
          (doseq [node (util/coll nodes)]
            (db/pause! (:db test) test node))
          (assoc op :value nodes))

        :resume
        (let [nodes (or (:value op) (:nodes test))]
          (info "Resuming FrogDB on" nodes)
          (doseq [node (util/coll nodes)]
            (db/resume! (:db test) test node))
          (assoc op :value nodes))))

    (teardown! [this test]
      nil)))

;; ===========================================================================
;; Combined Nemesis
;; ===========================================================================

(defn combined-nemesis
  "Creates a nemesis that combines multiple fault types."
  []
  (nemesis/compose
    {{:kill :kill
      :start :start} (process-killer)
     {:pause :pause
      :resume :resume} (process-pauser)}))

;; ===========================================================================
;; Generators
;; ===========================================================================

(defn kill-generator
  "Generator for kill/restart cycles.

   Options:
   - :interval - time between fault events (default 10s)
   - :recover-time - time to wait after restart (default 5s)"
  [opts]
  (let [interval (get opts :interval 10)
        recover-time (get opts :recover-time 5)]
    (gen/cycle
      [(gen/sleep interval)
       {:type :info :f :kill}
       (gen/sleep recover-time)
       {:type :info :f :start}
       (gen/sleep recover-time)])))

(defn pause-generator
  "Generator for pause/resume cycles.

   Options:
   - :interval - time between fault events (default 10s)
   - :pause-duration - how long to pause (default 5s)"
  [opts]
  (let [interval (get opts :interval 10)
        pause-duration (get opts :pause-duration 5)]
    (gen/cycle
      [(gen/sleep interval)
       {:type :info :f :pause}
       (gen/sleep pause-duration)
       {:type :info :f :resume}
       (gen/sleep 2)])))

(defn mixed-generator
  "Generator that mixes kill and pause faults.

   Options:
   - :interval - base interval between faults
   - :kill-weight - relative weight of kill operations
   - :pause-weight - relative weight of pause operations"
  [opts]
  (let [interval (get opts :interval 15)
        kill-weight (get opts :kill-weight 1)
        pause-weight (get opts :pause-weight 1)]
    (gen/cycle
      [(gen/sleep interval)
       (gen/once
         (rand-nth (concat
                     (repeat kill-weight [{:type :info :f :kill}
                                         (gen/sleep 5)
                                         {:type :info :f :start}])
                     (repeat pause-weight [{:type :info :f :pause}
                                          (gen/sleep 3)
                                          {:type :info :f :resume}]))))])))

(defn rapid-kill-generator
  "Rapid kill/restart cycles for stress testing durability.

   This generator kills and restarts FrogDB at a much faster rate
   than the standard kill-generator, designed to stress test crash
   recovery and persistence mechanisms.

   Options:
   - :kill-interval - time between kills (default 3s)
   - :restart-delay - time to wait before restart (default 1s)"
  [opts]
  (let [kill-interval (get opts :kill-interval 3)
        restart-delay (get opts :restart-delay 1)]
    (gen/cycle
      [(gen/sleep kill-interval)
       {:type :info :f :kill}
       (gen/sleep restart-delay)
       {:type :info :f :start}
       (gen/sleep restart-delay)])))

;; ===========================================================================
;; Nemesis Packages
;; ===========================================================================

(defn none
  "No nemesis - for baseline testing."
  []
  {:nemesis nemesis/noop
   :generator nil
   :final-generator nil})

(defn kill
  "Kill/restart nemesis package.

   Options:
   - :interval - time between kill cycles"
  [opts]
  {:nemesis (process-killer)
   :generator (kill-generator opts)
   :final-generator (gen/once {:type :info :f :start})})

(defn pause
  "Pause/resume nemesis package.

   Options:
   - :interval - time between pause cycles"
  [opts]
  {:nemesis (process-pauser)
   :generator (pause-generator opts)
   :final-generator (gen/once {:type :info :f :resume})})

(defn all
  "Combined nemesis with both kill and pause.

   Options:
   - :interval - time between faults"
  [opts]
  {:nemesis (combined-nemesis)
   :generator (mixed-generator opts)
   :final-generator (gen/phases
                      (gen/once {:type :info :f :resume})
                      (gen/once {:type :info :f :start}))})

(defn rapid-kill
  "Rapid kill/restart nemesis package for stress testing durability.

   This is more aggressive than the standard kill nemesis, designed
   to catch durability and crash recovery bugs.

   Options:
   - :kill-interval - time between kills (default 3s)
   - :restart-delay - time before restart (default 1s)"
  [opts]
  {:nemesis (process-killer)
   :generator (rapid-kill-generator opts)
   :final-generator (gen/once {:type :info :f :start})})

(defn nemesis-package
  "Select a nemesis package by name.

   Available packages:
   - :none - no faults
   - :kill - process crashes
   - :pause - process pauses
   - :rapid-kill - aggressive kill/restart cycles
   - :all - combined faults"
  [name opts]
  (case name
    :none (none)
    :kill (kill opts)
    :pause (pause opts)
    :rapid-kill (rapid-kill opts)
    :all (all opts)
    ;; Default to none
    (none)))
