(defproject com.roomkey/drcfg :lein-v
  :description "Dynamic Runtime Configuration Utility based on Zookeeper"
  :url "https://github.com/roomkey/drcfg"
  :plugins [[com.roomkey/lein-v "7.0.0"]]
  :middleware [lein-v.plugin/middleware]
  :license {:name "Copyright Hotel JV Services LLC"
            :distribution :manual
            :comments "All rights reserved"}
  :aliases {"midje" ["with-profile" "+test" "midje"]}
  :release-tasks [["vcs" "assert-committed"]
                  ["v" "update"] ;; compute new version & tag it
                  ["vcs" "push"]
                  ["v" "abort-when-not-anchored"]
                  ["deploy" "rk-maven"]]
  :min-lein-version "2.8.1"
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [org.clojure/core.async "0.4.490"]
                 [org.apache.zookeeper/zookeeper "3.5.4-beta" :exclusions [log4j]]]
  :repositories {"rk-maven" {:url "s3p://rk-maven/releases/" :no-auth true}}
  :jvm-opts ["-Djava.io.tmpdir=./tmp"]
  :profiles {:dev {:dependencies [[midje "1.9.6"]
                                  [zookeeper-clj "0.9.4" :exclusions [org.apache.zookeeper/zookeeper commons-codec]]
                                  [org.apache.curator/curator-test "4.1.0"]
                                  [org.slf4j/slf4j-api "1.7.25"]
                                  [org.slf4j/jcl-over-slf4j "1.7.25"]
                                  [org.slf4j/slf4j-log4j12 "1.7.25"]
                                  [log4j/log4j "1.2.17"]]}
             :test {:resource-paths ["test-resources"]}})
