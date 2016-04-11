(defproject com.roomkey/drcfg :lein-v
  :description "Dynamic Runtime Configuration Utility based on Zookeeper"
  :url "https://github.com/roomkey/drcfg"
  :plugins [[com.roomkey/lein-v "5.0.1"]]
  :license {:name "Copyright Hotel JV Services LLC"
            :distribution :manual
            :comments "All rights reserved"}
  :release-tasks [["vcs" "assert-committed"]
                  ["v" "update"] ;; compute new version & tag it
                  ["vcs" "push"]
                  ["deploy"]]
  :min-lein-version "2.5.0"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [zookeeper-clj "0.9.4" :exclusions [org.apache.zookeeper/zookeeper]]
                 [org.apache.zookeeper/zookeeper "3.5.1-alpha"]]
  :repositories [["rk-public" {:url "http://rk-maven-public.s3-website-us-east-1.amazonaws.com/releases/"}]
                 ["releases" {:url "s3://rk-maven/releases/"}]]
  :jvm-opts ["-Djava.io.tmpdir=./tmp"]
  :profiles {:dev {:dependencies [[midje "1.8.3" :exclusions [commons-codec]]
                                  [org.apache.curator/curator-test "3.1.0"]
                                  [org.slf4j/slf4j-api "1.7.21"]
                                  [org.slf4j/jcl-over-slf4j "1.7.21"]
                                  [org.slf4j/slf4j-log4j12 "1.7.21"]
                                  [log4j/log4j "1.2.17"]]}
             :test {:resource-paths ["test-resources"]}})
