(defproject com.roomkey/drcfg :lein-v
  :description "Dynamic Runtime Configuration Utility based on Zookeeper"
  :url "https://github.com/g1nn13/drcfg"
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
                 [org.apache.curator/curator-framework "3.1.0"]
                 [org.apache.curator/curator-recipes "3.1.0"]]
  :repositories [["rk-public" {:url "http://rk-maven-public.s3-website-us-east-1.amazonaws.com/releases/"}]
                 ["releases" {:url "s3://rk-maven/releases/"}]]
  :profiles {:dev {:resource-paths ["test-resources"]
                   :dependencies [[midje "1.8.3"]
                                  [org.apache.curator/curator-test "3.1.0"]
                                  [org.slf4j/slf4j-api "1.7.19"]
                                  [org.slf4j/jcl-over-slf4j "1.7.19"]
                                  [org.slf4j/slf4j-log4j12 "1.7.19"]
                                  [log4j/log4j "1.2.17"]]}})
