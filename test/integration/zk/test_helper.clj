(ns integration.zk.test-helper
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]])
  (:import [org.apache.curator.test TestingServer TestingCluster]
           [org.apache.zookeeper ZooKeeper]))

(defchecker bytes-of [expected]
  (checker [actual] (= (seq actual) (seq (.getBytes expected)))))

(defn- streams
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

(defchecker eventually-streams [n timeout expected]
  ;; The key to chatty checkers is to have the useful intermediate results be the evaluation of arguments to top-level expressions!
  (chatty-checker [actual] (extended-= (streams n timeout actual) expected)))

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
