(ns roomkey.integration.zclient
  (:require [roomkey.zclient :refer :all :as z]
            [zookeeper :as zoo]
            [clojure.tools.logging :as log]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]])
  (:import [org.apache.curator.test TestingServer]
           [roomkey.zclient TransientClient]))

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

(fact "Can create a client and then close it"
  (with-open [$c (create $cstring0)]
    (.open $c)
    $c => (partial instance? TransientClient)
    $c => (eventually-refers-to 1000 (partial instance? org.apache.zookeeper.ZooKeeper))))

(fact "Can add watchers"
  (with-open [$c (create $cstring0)]
    (let [state (promise)]
      (add-watch $c :k (fn [k r o n] (log/warn "**************" k o n) (deliver state [k r o n])))
      (.open $c)
      state => (eventually-is-realized-as 3000
                                          (just [:k (partial instance? clojure.lang.Atom)
                                                 nil (partial instance? org.apache.zookeeper.ZooKeeper)])))))

(fact "Can open client to unavailable server"
  (with-open [$c (create bogus-host)]
    (add-watch $c :k (fn [k r o n] (log/warn "**************" k o n)))
    (.open $c)) => (partial instance? TransientClient))
