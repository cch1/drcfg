(ns integration.zk.client-test
  (:require [zk.client :refer :all :as z]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [clojure.test :refer [use-fixtures deftest testing is]])
  (:import [java.time Instant]
           [org.apache.curator.test TestingServer TestingCluster]
           [org.apache.zookeeper ZooKeeper data.Stat]
           [zk.client ZClient]))

(def ^:dynamic *connect-string*)

(use-fixtures :each (fn [f] (with-open [test-server (TestingServer. true)]
                              (binding [*connect-string* (.getConnectString test-server)]
                                (f)))))

(defn streams
  "Captures the first `n` streamed elements of c subject to a timeout of `timeout` ms"
  ;; Now returns partial results!
  [n timeout c]
  (let [to (async/timeout timeout)]
    (async/<!! (async/go-loop [out []]
                 (if (= n (count out))
                   out
                   (let [[v port] (async/alts! [to c])]
                     (if (nil? v)
                       (conj out (if (= c port) ::channel-closed ::timeout))
                       (recur (conj out v)))))))))

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

(deftest create-open-close
  (let [events (async/chan 10)
        $c (create)]
    (is (instance? ZClient $c))
    (is (not (connected? $c)))
    (let [handle (open $c *connect-string* {})]
      (is (instance? ZooKeeper @handle))
      (is (connected? $c))
      (is (nil? (.close handle)))
      (is (nil? @handle))
      (is (not (connected? $c))))))

(deftest client-can-only-be-opened-once
  (let [$zclient (create)]
    (is (instance? java.lang.AutoCloseable (open $zclient *connect-string* {})))
    (is (thrown? java.lang.AssertionError (open $zclient *connect-string* {})))))

(deftest client-can-be-reopened
  (let [$zclient (create)]
    (while-watching [$c (open $zclient *connect-string* {})]
      (is (connected? $zclient)))
    (while-watching [$c (open $zclient *connect-string* {})]
      (is (connected? $zclient)))))

(deftest with-open-works
  (let [$c (create)]
    (is (not (connected? $c)))
    (with-open [handle (open $c *connect-string* {})]
      (is (instance? ZooKeeper @handle))
      (is (connected? $c)))
    (is (not (connected? $c)))))

(deftest while-watching-works
  (let [$c (create)]
    (is (not (connected? $c)))
    (let [return (while-watching [chandle (open $c *connect-string* {})]
                   (is (connected? $c))
                   ::ok)]
      (is (= ::ok return)))
    (is (not (connected? $c))))
  (testing "client expiration is handled gracefully"
    (let [$zclient (create)
          return (while-watching [_ (open $zclient *connect-string* {})]
                   (expire! $zclient)
                   ::ok)]
      (is (= ::ok return)))))

(deftest client-retries-prior-to-connection-and-watch-being-established
  (let [$zclient (create)]
    (with-open [_ (open $zclient *connect-string* {})]
      (is (string? ($zclient #(.create % "/x" (.getBytes "Hi")
                                       org.apache.zookeeper.ZooDefs$Ids/OPEN_ACL_UNSAFE
                                       org.apache.zookeeper.CreateMode/PERSISTENT))))
      (is (string? ($zclient #(.create % "/x/y" (.getBytes "Hello")
                                       org.apache.zookeeper.ZooDefs$Ids/OPEN_ACL_UNSAFE
                                       org.apache.zookeeper.CreateMode/PERSISTENT))))
      (is (instance? Stat ($zclient #(.setData % "/x/y" (.getBytes "World") 0))))
      (is (nil? ($zclient #(.delete % "/x/y" 1)))))))

;; This test can expose issues with delays in getting the watch in place after opening the connection.  A delay
;; could lead to not capturing events provoked by commands issued immediately after the connection opens.
(deftest client-watches-nodes-persistently
  (let [$zclient (create)]
    (while-watching [chandle (open $zclient *connect-string* {})]
      (is (= "/x" ($zclient #(.create % "/x" (.getBytes "Hi")
                                      org.apache.zookeeper.ZooDefs$Ids/OPEN_ACL_UNSAFE
                                      org.apache.zookeeper.CreateMode/PERSISTENT))))
      (is (= "/x/y" ($zclient #(.create % "/x/y" (.getBytes "Hello")
                                        org.apache.zookeeper.ZooDefs$Ids/OPEN_ACL_UNSAFE
                                        org.apache.zookeeper.CreateMode/PERSISTENT))))
      (expire! $zclient)
      ;; Race condition used to exist here where watch would not get set in time. Now we nil the raw client until watch is added
      (is (instance? Stat ($zclient #(.setData % "/x/y" (.getBytes "World") 0))))
      (is (nil? ($zclient #(.delete % "/x/y" 1))))
      (let [events (vec (streams 7 4000 chandle))
            event? (fn [actual attrs] (reduce-kv (fn [acc k v] (when acc (= v (k actual))))
                                                 (and (keyword? (:type actual))
                                                      (string? (:path actual)))
                                                 attrs))]
        (is (event? (events 0) {:type :NodeCreated :path "/x"}))
        (is (event? (events 1) {:type :NodeChildrenChanged :path "/"}))
        (is (event? (events 2) {:type :NodeCreated :path "/x/y"}))
        (is (event? (events 3) {:type :NodeChildrenChanged :path "/x"}))
        (is (event? (events 4) {:type :NodeDataChanged :path "/x/y"}))
        (is (event? (events 5) {:type :NodeDeleted :path "/x/y"}))
        (is (event? (events 6) {:type :NodeChildrenChanged :path "/x"}))))))

(deftest client-handles-disconnects
  (let [$zclient (create)]
    (with-open [$t0 (TestingServer. false)]
      (with-open [chandle (open $zclient (.getConnectString $t0) {})]
        (is (= ::not-yet-connected (deref chandle 250 ::not-yet-connected)))
        (.start $t0)
        (is (instance? ZooKeeper @chandle))
        (.stop $t0)
        (is (instance? ZooKeeper @chandle))
        (.restart $t0)
        (is (instance? ZooKeeper @chandle))
        (.stop $t0)
        (is (instance? ZooKeeper @chandle))
        (expire! $zclient)
        ;; There is a race condition in here...
        (is (= ::expired-and-no-server-to-connect-to (deref chandle 250 ::expired-and-no-server-to-connect-to)))
        (.restart $t0)
        (is (instance? ZooKeeper @chandle))))))

#_ (deftest client-survives-session-migration-to-alternate-server
     (with-open [$t (TestingCluster. 3)]
       (let [$cstring (.getConnectString $t)
             $zclient (create)]
         (with-open [chandle (open $zclient $cstring {})]
           (is (= ::not-yet-connected (deref chandle 250 ::not-yet-connected)))
           (.start $t)
           (is (instance? ZooKeeper @chandle))
           (kill-server! $zclient $t)
           (is (instance? ZooKeeper @chandle))))))

#_ (future-deftest "Client handles transition to read-only"
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

(deftest client-can-be-restarted-across-disparate-connections
  (with-open [$t0 (TestingServer.)
              $t1 (TestingServer.)]
    (let [$zclient (create)]
      (while-watching [chandle (open $zclient (.getConnectString $t0) {})]
        (is (instance? ZooKeeper @chandle))
        (is (connected? $zclient)))
      (while-watching [chandle (open $zclient (.getConnectString $t1) {})]
        (is (instance? ZooKeeper @chandle))
        (is (connected? $zclient))))))

(deftest client-supports-IFn
  (let [$zclient (create)]
    (while-watching [_ (open $zclient *connect-string* {})]
      (is (bytes? ($zclient #(.getSessionPasswd %))))
      ;; And retries when client is not connected and ready to go:
      (expire! $zclient)
      (is (bytes? ($zclient #(.getSessionPasswd %)))))))

(deftest client-renders-to-string-nicely
  (let [$zclient (create)]
    (is (re-matches #"ℤℂ: <No Raw Client>" (str $zclient)))
    (while-watching [_ (open $zclient *connect-string* {})]
      (is (re-matches #"ℤℂ: @([0-9a-f]+) 0x[0-9a-f]+ \([A-Z]+\)" (str $zclient))))))

(deftest start-stop
  (let [bad-connect-string "127.1.1.1:9999"
        $c (create)
        results (for [connect-string (concat (repeat 5 *connect-string*)
                                             (repeat 5 bad-connect-string)
                                             (repeatedly 5 #(rand-nth [*connect-string* bad-connect-string])))]
                  (let [handle (open $c (str connect-string "/drcfg") 8000)]
                    (.close handle)))]
    (is (= (repeat 15 nil) results))))
