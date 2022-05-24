(ns integration.zk.client-test
  (:require [zk.client :refer :all :as z]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as impl]
            [clojure.tools.logging :as log]
            [clojure.test :refer [use-fixtures deftest testing is]]
            [testit.core :refer :all]
            [testit.eventually :refer [*eventually-timeout-ms*]])
  (:import [java.time Instant]
           [org.apache.curator.test TestingServer TestingCluster]
           [org.apache.zookeeper ZooKeeper data.Stat]
           [zk.client ZClient]
           [ch.qos.logback.classic Level Logger]))

(def ^:dynamic *connect-string*)

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
  (if-let [z (deref client)]
    ;; Provoke expiration via "official" approach: https://zookeeper.apache.org/doc/r3.5.5/api/org/apache/zookeeper/Testable.html
    (.injectSessionExpiration (.getTestable z))
    (throw (Exception. "Unable to expire session -missing ZooKeeper client"))))

(defn kill-server!
  "Kill the client's currently connected server of the cluster"
  [client cluster]
  (if-let [z (deref client)]
    (let [instance (.findConnectionInstance cluster z)]
      (assert (.killServer cluster instance) "Couldn't kill ZooKeeper server instance")
      instance)))

(defmacro stifling-server-logs
  [& body]
  (let [logger-names #{"org.apache.zookeeper.server" "org.apache.zookeeper.server.admin"}]
    `(let [initial# (reduce (fn [acc# logger-name#] (let [logger# ^Logger (org.slf4j.LoggerFactory/getLogger logger-name#)]
                                                      (assoc acc# logger# (.getLevel logger#))))
                            {}
                            ~logger-names)]
       (doseq [logger# (keys initial#)] (.setLevel logger# Level/OFF))
       (try ~@body
            (finally (doseq [[logger# level#] initial#] (.setLevel logger# level#)))))))

(use-fixtures :each (fn [f] (with-open [test-server (TestingServer. true)]
                              (binding [*eventually-timeout-ms* 2000
                                        *connect-string* (.getConnectString test-server)]
                                (f)))))

(deftest create-client
  (let [c (create)]
    (facts "created client satisfies relevant protocols"
      c => (partial instance? ZClient)
      c => (partial instance? clojure.lang.IDeref)
      c => (partial instance? clojure.lang.IBlockingDeref)
      c => (partial satisfies? Notifiable))))

(deftest open-connection
  (let [chandle (connect (create) *connect-string* {})]
    (facts "created client satisfies relevant protocols"
      chandle => (partial instance? java.lang.AutoCloseable)
      chandle => (partial satisfies? impl/ReadPort)
      chandle => (partial satisfies? impl/Channel))
    (.close chandle)))

(deftest client-can-be-dereferenced-iff-connected-and-blocks-otherwise
  (let [c (create)]
    (fact "client deref times out before connection"
      (deref c 100 nil) => nil)
    (with-open [_ (open c *connect-string*)]
      (fact "client dereferences to ZooKeeper when connected"
        (deref c) => (partial instance? ZooKeeper)))
    (fact "client deref times out after connection closed"
      (deref c 100 nil) => nil)))

(deftest while->open-is-a-useful-pattern-for-testing
  (while->open [c (open *connect-string*)]
    (is (instance? ZooKeeper (deref c 0 nil)))))

(deftest register-for-session-events
  (while->open [c (connect *connect-string* {})]
    (facts-for "it is possible to register for session events"
      (register c)
      => (partial satisfies? impl/ReadPort))))

(deftest session-event-produced
  (let [c (create)
        session-events0 (register c)
        session-events1 (register c)
        session-events2 (register c)]
    (with-open [_ (open c *connect-string*)]
      (facts "session event is produced exactly once per registered consumer after connection"
        (streams 2 500 session-events0) =in=> [(partial instance? ZooKeeper) ::timeout]
        (streams 2 500 session-events1) =in=> [(partial instance? ZooKeeper) ::timeout]
        (streams 2 500 session-events2) =in=> [(partial instance? ZooKeeper) ::timeout]))
    (facts "registering while disconnected does not produce disconnected client"
      (streams 1 1000 (register c)) =in=> [::timeout])))

(deftest client-handles-disconnects-and-session-expiration
  (let [$client (create)]
    (with-open [$t0 (TestingServer. false)
                _ (open $client (.getConnectString $t0))]
      (let [session-events (register $client)]
        (facts "nothing produced prior to connection"
          (deref $client 500 nil) => nil?
          (async/poll! session-events) => nil?)
        (.start $t0)
        (facts "unlimited clients available and a session event produced after connection"
          (deref $client 5000 nil) => (partial instance? ZooKeeper)
          (streams 2 500 session-events) =in=> [(partial instance? ZooKeeper) ::timeout])
        (.stop $t0)
        (facts "nothing available while disconnected"
          (deref $client 50 nil) =eventually=> nil?
          (streams 1 500 session-events) =in=> [::timeout])
        (.restart $t0)
        (facts "unlimited clients available again after reconnection, but no session-event produced"
          (deref $client 5000 nil) => (partial instance? ZooKeeper)
          (streams 1 500 session-events) =in=> [::timeout])
        (expire! $client)
        (facts "new client spins up and is available after session restart and session-event produced"
          (deref $client 500 nil) => (partial instance? ZooKeeper)
          (streams 2 500 session-events) =in=> [(partial instance? ZooKeeper) ::timeout])))))

;;; The following test sporadically prevents the JVM from shutting down.  Almost certainly it's
;;; the TestingCluster.
(deftest ^:testing-cluster client-survives-session-migration-to-alternate-server
  (stifling-server-logs
   (with-open [$t (TestingCluster. 3)]
     (.start $t)
     (while->open [$zclient (open (.getConnectString $t))]
       (fact (deref $zclient) =eventually=> (partial instance? ZooKeeper))
       (kill-server! $zclient $t)
       (Thread/sleep 1000)
       (fact (deref $zclient) =eventually=> (partial instance? ZooKeeper))))))

(deftest start-stop
  (let [bad-connect-string "127.1.1.1:9999"
        results (doall (for [connect-string (shuffle (concat (repeat 5 *connect-string*)
                                                             (repeat 5 bad-connect-string)))]
                         (with-open [_ (open (create) connect-string :timeout 500)]
                           true)))]
    (try (facts results =in=> (vec (repeat 10 true))))))

(deftest client-renders-to-string-nicely
  (let [$c (create)]
    (with-open [_ (open $c *connect-string*)]
      (deref $c)
      (is (re-matches #"ℤℂ: @([0-9a-f]+) 0x[0-9a-f]+ \([A-Z]+\)" (str $c))))
    (is (re-matches #"ℤℂ: <No Raw Client>" (str $c)))))
