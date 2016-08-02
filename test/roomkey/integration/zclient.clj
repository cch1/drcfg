(ns roomkey.integration.zclient
  (:require [roomkey.zclient :refer :all :as z]
            [clojure.core.async :as async]
            [zookeeper :as zoo]
            [clojure.tools.logging :as log]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]])
  (:import [org.apache.curator.test TestingServer]
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

(background (around :facts (do ?form)))

(fact "Can create a client and then close it with proper notifications arriving on supplied channel"
  (let [c (async/chan 1)]
    (with-open [$c (create $cstring0 c)]
      $c => (partial instance? ZClient)
      (async/<!! c) => (just [:SyncConnected (partial instance? ZooKeeper)]))
    (async/<!! c) => nil?))

(fact "Can open client to unavailable server"
  (with-open [$t0 (TestingServer. false)
              $t1 (TestingServer. (.getPort $t0) false)]
    (let [$cstring (.getConnectString $t0)
          $c (async/chan 1)]
      (with-open [$client (create $cstring $c)]
        (async/alts!! [$c (async/timeout 2500)]) => (contains [nil])
        (.start $t0)
        (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:SyncConnected (partial instance? ZooKeeper)])])
        (.stop $t0)
        (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:Disconnected (partial instance? ZooKeeper)])])
        (.restart $t0)
        (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:SyncConnected (partial instance? ZooKeeper)])])
        (.stop $t0)
        (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:Disconnected (partial instance? ZooKeeper)])])
        (log/info ">>>>>>>>>> About to start a new server -should trigger expiration of existing sessions <<<<<<")
        (.start $t1)
        (async/alts!! [$c (async/timeout 2500)]) => (contains [(just [:SyncConnected (partial instance? ZooKeeper)])])))))
