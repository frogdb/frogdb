(defproject jepsen.frogdb "0.1.0-SNAPSHOT"
  :description "Jepsen tests for FrogDB"
  :url "https://github.com/nathanjordan/frogdb"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.5"]
                 [com.taoensso/carmine "3.3.2"]]
  :jvm-opts ["-Djava.awt.headless=true" "-Xmx8g"]
  :main jepsen.frogdb.core
  :repl-options {:init-ns jepsen.frogdb.core}
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "1.4.4"]]}})
