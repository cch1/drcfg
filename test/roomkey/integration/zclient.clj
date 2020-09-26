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
(def sandbox "/sandbox")

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
        (with-open [_ (open $c $cstring0 5000)]
          $c => (partial instance? ZClient)
          (async/<!! (async/tap $c events)) => (just [:roomkey.zclient/connected (partial instance? ZooKeeper)])
          (connected? $c) => truthy)))

(defchecker stat? [expected]
  ;; The key to chatty checkers is to have the useful intermediate results be the evaluation of arguments to top-level expressions!
  (every-checker (contains {:version int?
                            :cversion int?
                            :aversion int?
                            :ctime (partial instance? java.time.Instant)
                            :mtime (partial instance? java.time.Instant)
                            :mzxid pos-int?
                            :czxid pos-int?
                            :pzxid pos-int?
                            :numChildren int?
                            :ephemeralOwner int?
                            :dataLength int?})
                 (contains expected)))

(fact "Client can perform operations on znodes"
      (let [test-server (TestingServer. true)
            c (async/chan 1)
            $client (create)]
        (async/tap $client c)
        (with-open [_ (open $client (.getConnectString test-server) 5000)]
          c  => (eventually-streams 2 3000 (just [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])
                                                  (just [:roomkey.zclient/connected (partial instance? ZooKeeper)])]))
          (create-znode $client "/myznode" {:data (.getBytes "Hello World") :persistent? true}) => stat?
          (create-znode $client "/myznode/child" {}) => truthy
          (exists $client "/myznode" {}) => (contains {:version 0})
          (exists $client "/notmyznode" {}) => falsey
          (data $client "/myznode" {}) => (just {:data (bytes-of "Hello World") :stat (contains {:version 0})})
          (set-data $client "/myznode" (.getBytes "foo") 0 {}) => (contains {:version 1 :mtime (partial instance? java.time.Instant)})
          (set-data $client "/myznode" (.getBytes "foo") 0 {}) => nil
          (Thread/sleep 20)
          (children $client "/myznode" {}) => (just {:children (one-of string?) :stat map?})
          (delete $client "/myznode/child" 0 {}) => truthy
          (delete $client "/myznode" 1 {}) => truthy)))

(fact "Client expiration is handled gracefully"
      (with-open [$t0 (TestingServer. true)]
        (let [$cstring (.getConnectString $t0)
              $c (async/chan 1)
              $zclient (create)]
          (async/tap $zclient $c)
          (with-open [_ (open $zclient $cstring 5000)]
            $c => (eventually-streams 2 3000 (just [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])
                                                    (just [:roomkey.zclient/connected (partial instance? ZooKeeper)])]))
            ;; Provoke expiration via "official" approach: https://zookeeper.apache.org/doc/r3.5.5/api/org/apache/zookeeper/Testable.html
            (.injectSessionExpiration (.getTestable @(.client-atom $zclient)))
            $c => (eventually-streams 3 3000 (just [(just [:roomkey.zclient/expired (partial instance? ZooKeeper)])
                                                    (just [:roomkey.zclient/started (partial instance? ZooKeeper)])
                                                    (just [:roomkey.zclient/connected (partial instance? ZooKeeper)])]))))))

(fact "Can open client to unavailable server"
      (with-open [$t0 (TestingServer. false)
                  $t1 (TestingServer. (.getPort $t0) false)]
        (let [$cstring (.getConnectString $t0)
              $c (async/chan 1)
              $zclient (create)]
          (async/tap $zclient $c)
          (with-open [_ (open $zclient $cstring 5000)]
            $c => (eventually-streams 1 2500 (just [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])]))
            (.start $t0)
            $c => (eventually-streams 1 2500 (just [(just [:roomkey.zclient/connected (partial instance? ZooKeeper)])]))
            (.stop $t0)
            $c => (eventually-streams 1 2500 (just [(just [:roomkey.zclient/disconnected (partial instance? ZooKeeper)])]))
            (.restart $t0)
            $c => (eventually-streams 1 2500 (just [(just [:roomkey.zclient/connected (partial instance? ZooKeeper)])]))
            (.stop $t0)
            $c => (eventually-streams 1 2500 (just [(just [:roomkey.zclient/disconnected (partial instance? ZooKeeper)])]))
            (log/debug ">>>>>>>>>> About to start a new server -should trigger expiration of existing sessions <<<<<<")
            (.start $t1) ; Provoke expiration via our custom approach.
            $c => (eventually-streams 2 3000 (just [(just [:roomkey.zclient/expired (partial instance? ZooKeeper)])
                                                    (just [:roomkey.zclient/started (partial instance? ZooKeeper)])]))))))

(fact "Client survives session migration to alternate cluster server"
      (with-open [$t (TestingCluster. 3)]
        (let [$cstring (.getConnectString $t)
              $c (async/chan 1)
              $zclient (create)]
          (async/tap $zclient $c)
          (with-open [_ (open $zclient $cstring 500)]
            $c => (eventually-streams 1 2500 (just [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])]))
            (.start $t)
            $c => (eventually-streams 1 3500 (just [(just [:roomkey.zclient/connected (partial instance? ZooKeeper)])]))
            (let [instance (.findConnectionInstance $t @(.client-atom $zclient))]
              (assert (.killServer $t instance) "Couldn't kill ZooKeeper server instance")
              $c => (eventually-streams 2 4500 (just [(just [:roomkey.zclient/disconnected (partial instance? ZooKeeper)])
                                                      (just [:roomkey.zclient/connected (partial instance? ZooKeeper)])]))))
          $c => (eventually-streams 1 2500 (just [(just [:roomkey.zclient/closed nil])])))))

(fact "Client can be stopped and restarted"
      (with-open [$t (TestingServer.)]
        (let [$c (async/chan 1)
              $zclient (create)]
          (async/tap $zclient $c)
          (with-open [_ (open $zclient (.getConnectString $t) 5000)]
            $c => (eventually-streams 2 3500 (just [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])
                                                    (just [:roomkey.zclient/connected (partial instance? ZooKeeper)])])))
          $c => (eventually-streams 1 2500 (just [(just [:roomkey.zclient/closed nil])]))
          (with-open [_ (open $zclient (.getConnectString $t) 5000)]
            $c => (eventually-streams 2 3500 (just [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])
                                                    (just [:roomkey.zclient/connected (partial instance? ZooKeeper)])])))
          $c => (eventually-streams 1 2500 (just [(just [:roomkey.zclient/closed nil])])))))

(fact "Client can be stopped and restarted across disparate connections"
      (with-open [$t0 (TestingServer. false)
                  $t1 (TestingServer. false)]
        (let [$c (async/chan 1)
              $zclient (create)]
          (async/tap $zclient $c)
          (with-open [_ (open $zclient (.getConnectString $t0) 5000)]
            $c => (eventually-streams 1 2500 (just [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])]))
            (.start $t0)
            $c => (eventually-streams 1 2500 (just [(just [:roomkey.zclient/connected (partial instance? ZooKeeper)])]))
            (.stop $t0)
            $c => (eventually-streams 1 2500 (just [(just [:roomkey.zclient/disconnected (partial instance? ZooKeeper)])])))
          $c => (eventually-streams 1 4500 (just [(just [:roomkey.zclient/closed nil])]))
          (with-open [_ (open $zclient (.getConnectString $t1) 5000)]
            $c => (eventually-streams 1 2500 (just [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])]))
            (.start $t1)
            $c => (eventually-streams 1 2500 (just [(just [:roomkey.zclient/connected (partial instance? ZooKeeper)])]))
            (.stop $t1)
            $c => (eventually-streams 1 2500 (just [(just [:roomkey.zclient/disconnected (partial instance? ZooKeeper)])])))
          $c => (eventually-streams 1 4500 (just [(just [:roomkey.zclient/closed nil])])))))

(fact "Client renders toString nicely"
      (let [events (async/chan 1)
            $c (create)]
        (str $c) => #"ℤℂ: <No Raw Client>"
        (with-open [_ (open $c $cstring0 5000)]
          (async/tap $c events) => (eventually-streams 1 2500 (contains #{(just [:roomkey.zclient/connected (partial instance? ZooKeeper)])}))
          (str $c) => #"ℤℂ: @([0-9a-f]+) State:[A-Z]+ sessionId:0x[0-9a-f]+ server:.+:\d+")
        (Thread/sleep 500)
        (str $c) => #"ℤℂ: <No Raw Client>"))

(fact "Client support IFn"
      (let [$c (create)]
        (with-open [_ ($c $cstring0 5000)]
          (async/tap $c (async/chan 1)) => (eventually-streams 1 2500 (contains #{(just [:roomkey.zclient/connected (partial instance? ZooKeeper)])}))
          (connected? $c) => truthy)))
