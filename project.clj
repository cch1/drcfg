(defproject com.roomkey/drcfg :lein-v
  :description "Dynamic Runtime Configuration Utility based on Zookeeper"
  :url "https://github.com/roomkey/drcfg"
  :plugins [[com.roomkey/lein-v "6.2.0"]]
  :license {:name "Copyright Hotel JV Services LLC"
            :distribution :manual
            :comments "All rights reserved"}
  :release-tasks [["vcs" "assert-committed"]
                  ["v" "update"] ;; compute new version & tag it
                  ["vcs" "push"]
                  ["deploy"]]
  :min-lein-version "2.5.0"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/tools.logging "0.4.0"]
                 [org.clojure/core.async "0.4.474"]
                 [zookeeper-clj "0.9.4" :exclusions [org.apache.zookeeper/zookeeper]]
                 [org.apache.zookeeper/zookeeper "3.5.3-beta"]]
  :repositories [["releases" {:url "s3://rk-maven/releases/"}]]
  :jvm-opts ["-Djava.io.tmpdir=./tmp"]
  :profiles {:dev {:dependencies [[midje "1.9.1" :exclusions [commons-codec]]
                                  [org.apache.curator/curator-test "4.0.1"]
                                  [org.slf4j/slf4j-api "1.7.25"]
                                  [org.slf4j/jcl-over-slf4j "1.7.25"]
                                  [org.slf4j/slf4j-log4j12 "1.7.25"]
                                  [log4j/log4j "1.2.17"]]}
             :test {:resource-paths ["test-resources"]}})
