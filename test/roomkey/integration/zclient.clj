(ns roomkey.integration.zclient
  (:require [roomkey.zclient :refer :all :as z]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]])
  (:import [org.apache.curator.test TestingServer TestingCluster]
           [org.apache.zookeeper ZooKeeper]
           [roomkey.zclient ZClient]))

(def bogus-host "127.1.1.1:9999")
(def test-server (TestingServer. true))
(def $cstring0 (.getConnectString test-server))
(def $cstring1 "localhost:2181/drcfg")
(def sandbox "/sandbox")

(defchecker refers-to [expected]
  (checker [actual] (extended-= (deref actual) expected)))

(defchecker eventually-is-realized-as [timeout expected]
  (checker [actual]
           (loop [t timeout]
             (when (pos? t)
               (if-let [result (and (realized? actual)
                                    (extended-= (deref actual) expected))]
                 result
                 (do (Thread/sleep 200)
                     (recur (- t 200))))))))

(defchecker eventually-refers-to [timeout expected]
  (checker [actual]
           (loop [t timeout]
             (when (pos? t)
               (if-let [result (extended-= (deref actual) expected)]
                 result
                 (do (Thread/sleep 200)
                     (recur (- t 200))))))))

(defchecker bytes-of [expected]
  (checker [actual] (= (seq actual) (seq (.getBytes expected)))))

(defn- streams [n timeout c]
  "Captures the first `n` streamed elements of c subject to a timeout of `timeout` ms"
  (let [result (async/alt!!
                 (async/into [] (async/take n c 5)) ([v] (or v ::channel-closed))
                 (async/timeout timeout) ([v] (or v ::timeout)))]
    result))

(defchecker eventually-streams [n timeout expected]
  ;; The key to chatty checkers is to have the useful intermediate results be the evaluation of arguments to top-level expressions!
  (chatty-checker [actual] (extended-= (streams n timeout actual) expected)))

(fact "Can create a client, open it and then close it with proper notifications arriving on supplied channel"
      (let [events (async/chan 1)
            $c (create)]
        (connected? $c) => falsey
        (with-open [$c (open $c $cstring0 5000)]
          $c => (partial instance? ZClient)
          (async/<!! (async/tap $c events)) => (just [:roomkey.zclient/connected (partial instance? ZooKeeper)])
          $c => (refers-to (partial instance? ZooKeeper))
          (connected? $c) => truthy)))

(fact "Client can perform operations on znodes"
      (let [test-server (TestingServer. true)
            c (async/chan 1)
            $client (create)]
        (async/tap $client c)
        (with-open [$c (open $client (.getConnectString test-server) 5000)]
          c  => (eventually-streams 2 3000 (just [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])
                                                  (just [:roomkey.zclient/connected (partial instance? ZooKeeper)])]))
          (create-znode $c "/myznode" {:data (.getBytes "Hello World") :persistent? true}) => truthy
          (create-znode $c "/myznode/child" {}) => truthy
          (exists $c "/myznode" {}) => (contains {:version 0})
          (exists $c "/notmyznode" {}) => falsey
          (data $c "/myznode" {}) => (just {:data (bytes-of "Hello World") :stat (contains {:version 0})})
          (set-data $c "/myznode" (.getBytes "foo") 0 {}) => truthy
          (Thread/sleep 20)
          (children $c "/myznode" {}) => (just {:paths (one-of string?) :stat map?})
          (delete $c "/myznode/child" 0 {}) => truthy
          (delete $c "/myznode" 1 {}) => truthy)))

(fact "Can open client to unavailable server"
      (with-open [$t0 (TestingServer. false)
                  $t1 (TestingServer. (.getPort $t0) false)]
        (let [$cstring (.getConnectString $t0)
              $c (async/chan 1)
              $zclient (create)]
          (async/tap $zclient $c)
          (with-open [$client (open $zclient $cstring 5000)]
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])])
            (.start $t0)
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/connected (partial instance? ZooKeeper)])])
            (.stop $t0)
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/disconnected (partial instance? ZooKeeper)])])
            (.restart $t0)
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/connected (partial instance? ZooKeeper)])])
            (.stop $t0)
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/disconnected (partial instance? ZooKeeper)])])
            (log/info ">>>>>>>>>> About to start a new server -should trigger expiration of existing sessions <<<<<<")
            (.start $t1)
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/expired (partial instance? ZooKeeper)])])))))

(fact "Client survives session expiration"
      (with-open [$t (TestingCluster. 3)]
        (let [$cstring (.getConnectString $t)
              $c (async/chan 1)
              $zclient (create)]
          (async/tap $zclient $c)
          (with-open [$client (open $zclient $cstring 500)]
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])])
            (.start $t)
            (async/alts!! [$c (async/timeout 3500)]) => (contains [(just [:roomkey.zclient/connected (partial instance? ZooKeeper)])])
            (let [instance (.findConnectionInstance $t @$zclient)]
              (assert (.killServer $t instance) "Couldn't kill ZooKeeper server instance")
              (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/disconnected (partial instance? ZooKeeper)])])
              (async/alts!! [$c (async/timeout 3500)]) => (contains [(just [:roomkey.zclient/connected (partial instance? ZooKeeper)])])))
          (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/closed (partial instance? ZooKeeper)])]))))

(fact "Client can be stopped and restarted"
      (with-open [$t (TestingServer.)]
        (let [$c (async/chan 1)
              $zclient (create)]
          (async/tap $zclient $c)
          (with-open [$client (open $zclient (.getConnectString $t) 5000)]
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])])
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/connected (partial instance? ZooKeeper)])]))
          (async/alts!! [$c (async/timeout 4500)]) => (contains [(just [:roomkey.zclient/closed (partial instance? ZooKeeper)])])
          (with-open [$client (open $zclient (.getConnectString $t) 5000)]
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])])
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/connected (partial instance? ZooKeeper)])]))
          (async/alts!! [$c (async/timeout 4500)]) => (contains [(just [:roomkey.zclient/closed (partial instance? ZooKeeper)])]))))

(fact "Client can be stopped and restarted across disparate connections"
      (with-open [$t0 (TestingServer. false)
                  $t1 (TestingServer. false)]
        (let [$c (async/chan 1)
              $zclient (create)]
          (async/tap $zclient $c)
          (with-open [$client (open $zclient (.getConnectString $t0) 5000)]
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])])
            (.start $t0)
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/connected (partial instance? ZooKeeper)])])
            (.stop $t0)
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/disconnected (partial instance? ZooKeeper)])]))
          (async/alts!! [$c (async/timeout 4500)]) => (contains [(just [:roomkey.zclient/closed (partial instance? ZooKeeper)])])
          (with-open [$client (open $zclient (.getConnectString $t1) 5000)]
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])])
            (.start $t1)
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/connected (partial instance? ZooKeeper)])])
            (.stop $t1)
            (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:roomkey.zclient/disconnected (partial instance? ZooKeeper)])]))
          (async/alts!! [$c (async/timeout 4500)]) => (contains [(just [:roomkey.zclient/closed (partial instance? ZooKeeper)])]))))
