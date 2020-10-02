(ns integration.zk.client
  (:require [zk.client :refer :all :as z]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]])
  (:import [org.apache.curator.test TestingServer TestingCluster]
           [org.apache.zookeeper ZooKeeper]
           [zk.client ZClient]))

(def bogus-host "127.1.1.1:9999")
(def test-server (TestingServer. true))
(def $cstring0 (.getConnectString test-server))
(def $cstring1 "localhost:2181/drcfg")
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

(fact "Can create and tap a client, then open it and close it with proper notifications arriving"
      (let [events (async/chan 10)
            $c (create)]
        $c => (partial instance? ZClient)
        (connected? $c) => falsey
        (async/tap $c events)
        (with-open [_ (open $c $cstring0 5000)]
          events => (eventually-streams 2 1000 (just [(just [::z/connecting (partial instance? ZooKeeper)])
                                                      (just [::z/connected (partial instance? ZooKeeper)])]))
          (connected? $c) => truthy)
        events => (eventually-streams 1 3000 (just [(just [::z/closed (partial instance? ZooKeeper)])]))))

(fact "with-awaited-open works"
      (let [$c (create)]
        (with-awaited-open $c $cstring0 5000
          (connected? $c) => truthy)
        (connected? $c) => falsey))

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

(defchecker event? [expected]
  ;; The key to chatty checkers is to have the useful intermediate results be the evaluation of arguments to top-level expressions!
  (every-checker (contains {:type keyword?
                            :path string?})
                 (contains expected)))

;; (fact "Client can perform operations on znodes"
;;       (let [test-server (TestingServer. true)
;;             c (async/chan 1)
;;             $client (create)]
;;         (async/tap $client c)
;;         (with-open [_ (open $client (.getConnectString test-server) 5000)]
;;           c  => (eventually-streams 2 3000 (just [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])
;;                                                  (just [:roomkey.zclient/connected (partial instance? ZooKeeper)])]))
;;           (create-znode $client "/myznode" {:data (.getBytes "Hello World") :persistent? true}) => stat?
;;           (create-znode $client "/myznode/child" {}) => truthy
;;           (exists $client "/myznode" {}) => (contains {:version 0})
;;           (exists $client "/notmyznode" {}) => falsey
;;           (data $client "/myznode" {}) => (just {:data (bytes-of "Hello World") :stat (contains {:version 0})})
;;           (set-data $client "/myznode" (.getBytes "foo") 0 {}) => (contains {:version 1 :mtime (partial instance? java.time.Instant)})
;;           (set-data $client "/myznode" (.getBytes "foo") 0 {}) => nil
;;           (Thread/sleep 20)
;;           (children $client "/myznode" {}) => (just {:children (one-of string?) :stat map?})
;;           (delete $client "/myznode/child" 0 {}) => truthy
;;           (delete $client "/myznode" 1 {}) => truthy)))

;; (fact "Client can perform async operations on znodes"
;;       (let [test-server (TestingServer. true)
;;             c (async/chan 1)
;;             $client (create)]
;;         (async/tap $client c)
;;         (with-open [_ (open $client (.getConnectString test-server) 5000)]
;;           c  => (eventually-streams 2 3000 (just [(just [:roomkey.zclient/started (partial instance? ZooKeeper)])
;;                                                   (just [:roomkey.zclient/connected (partial instance? ZooKeeper)])]))
;;           (let [c (async/chan 10)]
;;             (create! $client "/myznode" c {:data (.getBytes "Hello World") :persistent? true}) => nil?
;;             (async/<!! c) => (contains {:result :OK
;;                                         :name "/myznode"
;;                                         :path "/myznode"
;;                                         :stat stat?})))))

(fact "Client expiration is handled gracefully"
      (with-open [$t0 (TestingServer. true)]
        (let [$cstring (.getConnectString $t0)
              $c (async/chan 1)
              $zclient (create)]
          (async/tap $zclient $c)
          (with-open [_ (open $zclient $cstring 5000)]
            $c => (eventually-streams 2 3000 (just [(just [::z/connecting (partial instance? ZooKeeper)])
                                                    (just [::z/connected (partial instance? ZooKeeper)])]))
            ;; Provoke expiration via "official" approach: https://zookeeper.apache.org/doc/r3.5.5/api/org/apache/zookeeper/Testable.html
            (.injectSessionExpiration (.getTestable @(.client-atom $zclient)))
            $c => (eventually-streams 3 3000 (just [(just [::z/closed (partial instance? ZooKeeper)])
                                                    (just [::z/connecting (partial instance? ZooKeeper)])
                                                    (just [::z/connected (partial instance? ZooKeeper)])]))))))

(fact "Client can watch nodes persistently"
      (with-open [$t0 (TestingServer. true)]
        (let [$cstring (.getConnectString $t0)
              $zclient (create)
              events (.watch $zclient "/" {:persistent? true :recursive? true})]
          events => (partial instance? clojure.core.async.impl.channels.ManyToManyChannel)
          (with-awaited-open $zclient $cstring 5000
            ($zclient #(.create % "/x" (.getBytes "Hi")
                                org.apache.zookeeper.ZooDefs$Ids/OPEN_ACL_UNSAFE
                                org.apache.zookeeper.CreateMode/PERSISTENT))
            ($zclient #(.create % "/x/y" (.getBytes "Hello")
                                org.apache.zookeeper.ZooDefs$Ids/OPEN_ACL_UNSAFE
                                org.apache.zookeeper.CreateMode/PERSISTENT))
            (.injectSessionExpiration (.getTestable @(.client-atom $zclient)))
            ($zclient #(.setData % "/x/y" (.getBytes "World") 0))
            ($zclient #(.delete % "/x/y" 1)))
          events => (eventually-streams 7 1000 (just [(event? {:type :NodeChildrenChanged :path "/"})
                                                      (event? {:type :NodeCreated :path "/x"})
                                                      (event? {:type :NodeChildrenChanged :path "/x"})
                                                      (event? {:type :NodeCreated :path "/x/y"})
                                                      (event? {:type :NodeDataChanged :path "/x/y"})
                                                      (event? {:type :NodeChildrenChanged :path "/x"})
                                                      (event? {:type :NodeDeleted :path "/x/y"})])))))

(fact "Can open client to unavailable server"
      (with-open [$t0 (TestingServer. false)
                  $t1 (TestingServer. (.getPort $t0) false)]
        (let [$cstring (.getConnectString $t0)
              $c (async/chan 1)
              $zclient (create)]
          (async/tap $zclient $c)
          (with-open [_ (open $zclient $cstring 5000)]
            $c => (eventually-streams 1 2500 (just [(just [::z/connecting (partial instance? ZooKeeper)])]))
            (.start $t0)
            $c => (eventually-streams 1 2500 (just [(just [::z/connected (partial instance? ZooKeeper)])]))
            (.stop $t0)
            $c => (eventually-streams 1 2500 (just [(just [::z/reconnecting (partial instance? ZooKeeper)])]))
            (.restart $t0)
            $c => (eventually-streams 1 2500 (just [(just [::z/connected (partial instance? ZooKeeper)])]))
            (.stop $t0)
            $c => (eventually-streams 1 2500 (just [(just [::z/reconnecting (partial instance? ZooKeeper)])]))
            (log/debug ">>>>>>>>>> About to start a new server -should trigger expiration of existing sessions <<<<<<")
            (.start $t1) ; Provoke expiration via our custom approach.
            $c => (eventually-streams 2 3000 (just [(just [::z/closed (partial instance? ZooKeeper)])
                                                    (just [::z/connecting (partial instance? ZooKeeper)])]))))))

(fact "Client survives session migration to alternate cluster server"
      (with-open [$t (TestingCluster. 3)]
        (let [$cstring (.getConnectString $t)
              $c (async/chan 1)
              $zclient (create)]
          (async/tap $zclient $c)
          (with-open [_ (open $zclient $cstring 500)]
            $c => (eventually-streams 1 2500 (just [(just [::z/connecting (partial instance? ZooKeeper)])]))
            (.start $t)
            $c => (eventually-streams 1 3500 (just [(just [::z/connected (partial instance? ZooKeeper)])]))
            (let [instance (.findConnectionInstance $t @(.client-atom $zclient))]
              (assert (.killServer $t instance) "Couldn't kill ZooKeeper server instance")
              $c => (eventually-streams 2 4500 (just [(just [::z/reconnecting (partial instance? ZooKeeper)])
                                                      (just [::z/connected (partial instance? ZooKeeper)])]))))
          $c => (eventually-streams 1 2500 (just [(just [::z/closed (partial instance? ZooKeeper)])])))))

(fact "Client can be stopped and restarted"
      (with-open [$t (TestingServer.)]
        (let [$c (async/chan 1)
              $zclient (create)]
          (async/tap $zclient $c)
          (with-open [_ (open $zclient (.getConnectString $t) 5000)]
            $c => (eventually-streams 2 3500 (just [(just [::z/connecting (partial instance? ZooKeeper)])
                                                    (just [::z/connected (partial instance? ZooKeeper)])])))
          $c => (eventually-streams 1 2500 (just [(just [::z/closed (partial instance? ZooKeeper)])]))
          (with-open [_ (open $zclient (.getConnectString $t) 5000)]
            $c => (eventually-streams 2 3500 (just [(just [::z/connecting (partial instance? ZooKeeper)])
                                                    (just [::z/connected (partial instance? ZooKeeper)])])))
          $c => (eventually-streams 1 2500 (just [(just [::z/closed (partial instance? ZooKeeper)])])))))

(fact "Client can be stopped and restarted across disparate connections"
      (with-open [$t0 (TestingServer. false)
                  $t1 (TestingServer. false)]
        (let [$c (async/chan 1)
              $zclient (create)]
          (async/tap $zclient $c)
          (with-open [_ (open $zclient (.getConnectString $t0) 5000)]
            $c => (eventually-streams 1 2500 (just [(just [::z/connecting (partial instance? ZooKeeper)])]))
            (.start $t0)
            $c => (eventually-streams 1 2500 (just [(just [::z/connected (partial instance? ZooKeeper)])]))
            (.stop $t0)
            $c => (eventually-streams 1 2500 (just [(just [::z/reconnecting (partial instance? ZooKeeper)])])))
          $c => (eventually-streams 1 4500 (just [(just [::z/closed (partial instance? ZooKeeper)])]))
          (with-open [_ (open $zclient (.getConnectString $t1) 5000)]
            $c => (eventually-streams 1 2500 (just [(just [::z/connecting (partial instance? ZooKeeper)])]))
            (.start $t1)
            $c => (eventually-streams 1 2500 (just [(just [::z/connected (partial instance? ZooKeeper)])]))
            (.stop $t1)
            $c => (eventually-streams 1 2500 (just [(just [::z/reconnecting (partial instance? ZooKeeper)])])))
          $c => (eventually-streams 1 4500 (just [(just [::z/closed (partial instance? ZooKeeper)])])))))

(fact "Client support IFn"
      (let [$c (create)]
        (with-awaited-open $c $cstring0 5000
          ($c #(.getSessionPasswd %)) => bytes?
          ;; And retries when client is not connected and ready to go:
          (.injectSessionExpiration (.getTestable @(.client-atom $c)))
          ($c #(.getSessionPasswd %)) => bytes?)))

(fact "Client renders toString nicely"
      (let [events (async/chan 1)
            $c (create)]
        (str $c) => #"ℤℂ: <No Raw Client>"
        (with-awaited-open $c $cstring0 5000
          (str $c) => #"ℤℂ: @([0-9a-f]+) 0x[0-9a-f]+ \([A-Z]+\)")))
