(ns integration.zk.client
  (:require [zk.client :refer :all :as z]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [integration.zk.test-helper :refer [eventually-streams stat? event? refers-to]]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]])
  (:import [org.apache.curator.test TestingServer TestingCluster]
           [org.apache.zookeeper ZooKeeper data.Stat]
           [zk.client ZClient]))

(def bogus-host "127.1.1.1:9999")
(def test-server (TestingServer. true))
(def $cstring0 (.getConnectString test-server))

(defn expire!
  "Expire the client's session"
  [client]
  (if-let [z (deref @(.-zap client) 0 nil)]
    ;; Provoke expiration via "official" approach: https://zookeeper.apache.org/doc/r3.5.5/api/org/apache/zookeeper/Testable.html
    (.injectSessionExpiration (.getTestable z))
    (throw (Exception. "Unable to expire session -missing ZooKeeper client"))))

(defn kill-server!
  "Kill the client's currently connected server of the cluster"
  [client cluster]
  (if-let [z (deref @(.-zap client) 0 nil)]
    (let [instance (.findConnectionInstance cluster z)]
      (assert (.killServer cluster instance) "Couldn't kill ZooKeeper server instance"))))

(fact "Can create and open a client and close it with proper events arriving and connected state available"
      (let [events (async/chan 10)
            $c (create)]
        $c => (partial instance? ZClient)
        (connected? $c) => falsey
        (let [handle (open $c $cstring0 {})]
          handle => (refers-to (partial instance? ZooKeeper))
          (connected? $c) => truthy
          (.close handle) => nil
          handle => (refers-to nil?)
          (connected? $c) => falsey)))

(fact "Client can't be opened twice"
      (with-open [$t (TestingServer.)]
        (let [$zclient (create)]
          (open $zclient (.getConnectString $t) {}) => (partial instance? java.lang.AutoCloseable)
          (open $zclient (.getConnectString $t) {}) => (throws java.lang.AssertionError))))

(fact "Client can be opened, closed and reopened"
      (with-open [$t (TestingServer.)]
        (let [$zclient (create)]
          (while-watching [$c (open $zclient (.getConnectString $t) {})]
            (connected? $zclient) => truthy)
          (while-watching [$c (open $zclient (.getConnectString $t) {})]
            (connected? $zclient) => truthy))))

(fact "with-open works"
      (let [$c (create)]
        (connected? $c) => falsey
        (with-open [handle (open $c $cstring0 {})]
          handle => (refers-to (partial instance? ZooKeeper))
          (connected? $c) => truthy)
        (connected? $c) => falsey))

(fact "while-watching works"
      (let [$c (create)]
        (connected? $c) => falsey
        (while-watching [chandle (open $c $cstring0 {})]
          (connected? $c) => truthy
          ::ok) => ::ok
        (connected? $c) => falsey))

(fact "Client expiration is handled gracefully"
      (let [$zclient (create)]
        (while-watching [_ (open $zclient $cstring0 {})]
          (expire! $zclient)
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
            (expire! $zclient)
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
          (with-open [chandle (open $zclient (.getConnectString $t0) {})]
            (deref chandle 250 ::not-yet-connected) => ::not-yet-connected
            (.start $t0)
            chandle => (refers-to (partial instance? ZooKeeper))
            (.stop $t0)
            chandle => (refers-to (partial instance? ZooKeeper))
            (.restart $t0)
            chandle => (refers-to (partial instance? ZooKeeper))
            (.stop $t0)
            chandle => (refers-to (partial instance? ZooKeeper))
            (expire! $zclient)
            (deref chandle 250 ::expired-and-no-server-to-connect-to) => ::expired-and-no-server-to-connect-to
            (.restart $t0)
            chandle => (refers-to (partial instance? ZooKeeper))))))

(fact "Client survives session migration to alternate cluster server"
      (with-open [$t (TestingCluster. 3)]
        (let [$cstring (.getConnectString $t)
              $zclient (create)]
          (with-open [chandle (open $zclient $cstring {})]
            (deref chandle 250 ::not-yet-connected) => ::not-yet-connected
            (.start $t)
            chandle => (refers-to (partial instance? ZooKeeper))
            (kill-server! $zclient $t)
            chandle => (refers-to (partial instance? ZooKeeper))))))

(future-fact "Client handles transition to read-only"
             (with-open [$t (TestingCluster. 3)]
               (let [$cstring (.getConnectString $t)
                     $zclient (create)]
                 (.start $t)
                 (with-open [chandle (open $zclient $cstring {})]
                   chandle => (refers-to (partial instance? ZooKeeper))
                   (let [instance (.findConnectionInstance $t @(.client-atom $zclient))]
                     (assert (.killServer $t instance) "Couldn't kill ZooKeeper server instance"))
                   chandle => (refers-to (partial instance? ZooKeeper))
                   (let [instance (.findConnectionInstance $t @(.client-atom $zclient))]
                     (assert (.killServer $t instance) "Couldn't kill ZooKeeper server instance"))
                   chandle => (refers-to (partial instance? ZooKeeper))))))

(fact "Client can be stopped and restarted across disparate connections"
      (with-open [$t0 (TestingServer.)
                  $t1 (TestingServer.)]
        (let [$zclient (create)]
          (while-watching [chandle (open $zclient (.getConnectString $t0) {})]
            chandle => (refers-to (partial instance? ZooKeeper))
            (connected? $zclient) => truthy)
          (while-watching [chandle (open $zclient (.getConnectString $t1) {})]
            chandle => (refers-to (partial instance? ZooKeeper))
            (connected? $zclient) => truthy))))

(fact "Client support IFn"
      (let [$zclient (create)]
        (while-watching [_ (open $zclient $cstring0 {})]
          ($zclient #(.getSessionPasswd %)) => bytes?
          ;; And retries when client is not connected and ready to go:
          (expire! $zclient)
          ($zclient #(.getSessionPasswd %)) => bytes?)))

(fact "Client renders toString nicely"
      (let [$zclient (create)]
        (str $zclient) => #"ℤℂ: <No Raw Client>"
        (while-watching [_ (open $zclient $cstring0 {})]
          (str $zclient) => #"ℤℂ: @([0-9a-f]+) 0x[0-9a-f]+ \([A-Z]+\)")))
