(ns integration.zk.client
  (:require [zk.client :refer :all :as z]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [integration.zk.test-helper :refer [eventually-streams stat? event?]]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]])
  (:import [org.apache.curator.test TestingServer TestingCluster]
           [org.apache.zookeeper ZooKeeper data.Stat]
           [zk.client ZClient]))

(def bogus-host "127.1.1.1:9999")
(def test-server (TestingServer. true))
(def $cstring0 (.getConnectString test-server))

(fact "Can create and open a client and close it with proper events arriving and connected state available"
      (let [events (async/chan 10)
            $c (create)]
        $c => (partial instance? ZClient)
        (connected? $c) => falsey
        (let [handle (open $c $cstring0 {})]
          handle => (eventually-streams 3 100 (just [::z/connecting ::z/connected ::z/watching]))
          (connected? $c) => truthy
          (.close handle) => nil
          handle => (eventually-streams 2 1000 (just [::z/closing ::z/closed]))
          (connected? $c) => falsey
          (async/<!! handle) => nil?)))

(fact "with-open works"
      (let [$c (create)]
        (connected? $c) => falsey
        (with-open [handle (open $c $cstring0 {})]
          handle => (eventually-streams 3 100 (just [::z/connecting ::z/connected ::z/watching]))
          (connected? $c) => truthy)
        (Thread/sleep 50) ; this is non-deterministic, but should suffice 99.9999% of the time for a fast local server.
        (connected? $c) => falsey))

(fact "while-watching works"
      (let [$c (create)]
        (connected? $c) => falsey
        (while-watching [_ (open $c $cstring0 {})]
          (connected? $c) => truthy
          ::ok) => ::ok
        (connected? $c) => falsey))

(fact "Client expiration is handled gracefully"
      (let [$zclient (create)]
        (while-watching [_ (open $zclient $cstring0 {})]
          ;; Provoke expiration via "official" approach: https://zookeeper.apache.org/doc/r3.5.5/api/org/apache/zookeeper/Testable.html
          (.injectSessionExpiration (.getTestable @(.client-atom $zclient)))
          ::ok) => ::ok))

(fact "Client will retry prior to the connection and watch being established"
      (with-open [$t0 (TestingServer. true)]
        (let [$cstring (.getConnectString $t0)
              $zclient (create)]
          (with-open [_ (open $zclient $cstring {})]
            ($zclient #(.create % "/x" (.getBytes "Hi")
                                org.apache.zookeeper.ZooDefs$Ids/OPEN_ACL_UNSAFE
                                org.apache.zookeeper.CreateMode/PERSISTENT)) => string?
            ($zclient #(.create % "/x/y" (.getBytes "Hello")
                                org.apache.zookeeper.ZooDefs$Ids/OPEN_ACL_UNSAFE
                                org.apache.zookeeper.CreateMode/PERSISTENT)) => string?
            ($zclient #(.setData % "/x/y" (.getBytes "World") 0)) => stat?
            ($zclient #(.delete % "/x/y" 1)) => nil?))))

;; This test can expose issues with delays in getting the watch in place after opening the connection.  A delay
;; could lead to not capturing events provoked by commands issued immediately after the connection opens.
(fact "Client can watch nodes persistently"
      (with-open [$t0 (TestingServer. true)]
        (let [$zclient (create)]
          (while-watching [chandle (open $zclient (.getConnectString $t0) {})]
            ($zclient #(.create % "/x" (.getBytes "Hi")
                                org.apache.zookeeper.ZooDefs$Ids/OPEN_ACL_UNSAFE
                                org.apache.zookeeper.CreateMode/PERSISTENT)) => "/x"
            ($zclient #(.create % "/x/y" (.getBytes "Hello")
                                org.apache.zookeeper.ZooDefs$Ids/OPEN_ACL_UNSAFE
                                org.apache.zookeeper.CreateMode/PERSISTENT)) => "/x/y"
            (.injectSessionExpiration (.getTestable @(.client-atom $zclient)))
            ;; Race condition used to exist here where watch would not get set in time. Now we nil the raw client until watch is added
            ($zclient #(.setData % "/x/y" (.getBytes "World") 0)) => (partial instance? Stat)
            ($zclient #(.delete % "/x/y" 1)) => nil
            chandle => (eventually-streams 7 4000 (just [(event? {:type :NodeCreated :path "/x"})
                                                         (event? {:type :NodeChildrenChanged :path "/"})
                                                         (event? {:type :NodeCreated :path "/x/y"})
                                                         (event? {:type :NodeChildrenChanged :path "/x"})
                                                         (event? {:type :NodeDataChanged :path "/x/y"})
                                                         (event? {:type :NodeDeleted :path "/x/y"})
                                                         (event? {:type :NodeChildrenChanged :path "/x"})]))))))

(fact "Client handles disconnects"
      (let [$zclient (create)]
        (with-open [$t0 (TestingServer. false)]
          (with-open [$c (open $zclient (.getConnectString $t0) {})]
            $c => (eventually-streams 1 2500 (just [::z/connecting]))
            (.start $t0)
            $c => (eventually-streams 2 2500 (just [::z/connected ::z/watching]))
            (.stop $t0)
            $c => (eventually-streams 1 2500 (just [::z/reconnecting]))
            (.restart $t0)
            $c => (eventually-streams 1 2500 (just [::z/watching]))
            (.stop $t0)
            $c => (eventually-streams 1 2500 (just [::z/reconnecting]))
            (.injectSessionExpiration (.getTestable @(.client-atom $zclient)))
            $c => (eventually-streams 1 2500 (just [::z/connecting]))))))

(fact "Client survives session migration to alternate cluster server"
      (with-open [$t (TestingCluster. 3)]
        (let [$cstring (.getConnectString $t)
              $zclient (create)]
          (with-open [$c (open $zclient $cstring {})]
            $c => (eventually-streams 1 2500 (just [::z/connecting]))
            (.start $t)
            $c => (eventually-streams 2 3500 (just [::z/connected ::z/watching]))
            (let [instance (.findConnectionInstance $t @(.client-atom $zclient))]
              (assert (.killServer $t instance) "Couldn't kill ZooKeeper server instance")
              $c => (eventually-streams 2 4500 (just [::z/reconnecting ::z/watching])))))))

(future-fact "Client handles transition to read-only"
             (with-open [$t (TestingCluster. 3)]
               (let [$cstring (.getConnectString $t)
                     $zclient (create)]
                 (.start $t)
                 (with-open [$c (open $zclient $cstring {})]
                   $c => (eventually-streams 3 2500 (just [::z/connecting ::z/connected ::z/watching]))
                   (let [instance (.findConnectionInstance $t @(.client-atom $zclient))]
                     (assert (.killServer $t instance) "Couldn't kill ZooKeeper server instance"))
                   $c => (eventually-streams 2 4500 (just [::z/reconnecting ::z/watching]))
                   (let [instance (.findConnectionInstance $t @(.client-atom $zclient))]
                     (assert (.killServer $t instance) "Couldn't kill ZooKeeper server instance"))
                   $c => (eventually-streams 2 4500 (just [::z/reconnecting ::z/watching]))))))

(fact "Client can be opened, closed and reopened"
      (with-open [$t (TestingServer.)]
        (let [$zclient (create)]
          (while-watching [$c (open $zclient (.getConnectString $t) {})]
            (connected? $zclient) => truthy)
          (while-watching [$c (open $zclient (.getConnectString $t) {})]
            (connected? $zclient) => truthy))))

(fact "Client can be stopped and restarted across disparate connections"
      (with-open [$t0 (TestingServer.)
                  $t1 (TestingServer.)]
        (let [$zclient (create)]
          (while-watching [$c (open $zclient (.getConnectString $t0) {})]
            (connected? $zclient) => truthy)
          (while-watching [$c (open $zclient (.getConnectString $t1) {})]
            (connected? $zclient) => truthy))))

(fact "Client support IFn"
      (let [$zclient (create)]
        (while-watching [$c (open $zclient $cstring0 {})]
          ($zclient #(.getSessionPasswd %)) => bytes?
          ;; And retries when client is not connected and ready to go:
          (.injectSessionExpiration (.getTestable @(.client-atom $zclient)))
          ($zclient #(.getSessionPasswd %)) => bytes?)))

(fact "Client renders toString nicely"
      (let [$zclient (create)]
        (str $zclient) => #"ℤℂ: <No Raw Client>"
        (while-watching [$c (open $zclient $cstring0 {})]
          (str $zclient) => #"ℤℂ: @([0-9a-f]+) 0x[0-9a-f]+ \([A-Z]+\)")))
