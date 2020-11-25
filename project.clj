(defn deps->pom-deps [deps]
  (into [] (map (fn [[dep-name {:keys [mvn/version] :as opts}]]
                  (let [opts-seq (apply concat (dissoc opts :mvn/version))]
                    (apply conj [dep-name version] opts-seq))) deps)))

(defn deps-map []
  (read-string (slurp "deps.edn")))

(defproject com.hapgood/drcfg :lein-v
  :description "Dynamic Runtime Configuration Utility based on Zookeeper"
  :url "https://github.com/cch1/drcfg"
  :plugins [[com.roomkey/lein-v "7.0.0"]]
  :middleware [lein-v.plugin/middleware]
  :license {:name "3-Clause BSD License"
            :url "https://opensource.org/licenses/BSD-3-Clause"}
  :aliases {"midje" ["with-profile" "+test" "midje"]}
  :release-tasks [["vcs" "assert-committed"]
                  ["v" "update"] ;; compute new version & tag it
                  ["vcs" "push"]
                  ["v" "abort-when-not-anchored"]
                  ["deploy" "clojars"]]
  :min-lein-version "2.8.1"
  :dependencies ~(-> (deps-map) :deps (deps->pom-deps))
  :jvm-opts ["-Djava.io.tmpdir=./tmp" "-Dclojure.core.async.go-checking=true"]
  :profiles {:dev {:dependencies ~(-> (deps-map) :aliases :dev :extra-deps (deps->pom-deps))}
             :test {:resource-paths ["test-resources"]
                    :global-vars {*warn-on-reflection* true}}
             :reflection {:global-vars {*warn-on-reflection* true}}})
