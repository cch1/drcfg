(ns integration.zk.test-helper
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]])
  (:import [java.time Instant]
           [org.apache.curator.test TestingServer TestingCluster]
           [org.apache.zookeeper ZooKeeper data.Stat]))

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

(defchecker refers-to [expected]
  (checker [actual] (extended-= (deref actual) expected)))

(defchecker eventually-refers-to [timeout expected]
  (checker [actual]
           (loop [t timeout]
             (when (pos? t)
               (if-let [result (extended-= (deref actual) expected)]
                 result
                 (do (Thread/sleep 200)
                     (recur (- t 200))))))))

(defchecker eventually-vrefers-to [timeout expected]
  (checker [actual]
           (loop [t timeout]
             (when (pos? t)
               (if-let [result (extended-= (.vDeref actual) expected)]
                 result
                 (do (Thread/sleep 200)
                     (recur (- t 200))))))))

(defchecker having-metadata [expected]
  (every-checker (chatty-checker [actual] (extended-= actual expected))
                 (chatty-checker [actual] (extended-= (meta actual) (meta expected)))))

(defchecker as-ex-info
  [expected]
  (chatty-checker [actual]
                  (extended-= [(ex-message actual) (ex-data actual) (ex-cause actual)] expected)))

(defchecker eventually-streams [n timeout expected]
  ;; The key to chatty checkers is to have the useful intermediate results be the evaluation of arguments to top-level expressions!
  (chatty-checker [actual] (extended-= (streams n timeout actual) expected)))

(defchecker stat? [expected]
  ;; The key to chatty checkers is to have the useful intermediate results be the evaluation of arguments to top-level expressions!
  (every-checker (some-checker (just {:version int?
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
                               (just {:version -1 :cversion -1 :aversion -1}))
                 (contains expected)))

(defchecker event? [expected]
  ;; The key to chatty checkers is to have the useful intermediate results be the evaluation of arguments to top-level expressions!
  (every-checker (contains {:type keyword?
                            :path string?})
                 (contains expected)))

(defn stat-to-map
  ([^Stat stat]
   ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#sc_timeInZk
   (when stat
     {:czxid (.getCzxid stat)
      :mzxid (.getMzxid stat)
      :pzxid (.getPzxid stat)
      :ctime (Instant/ofEpochMilli (.getCtime stat))
      :mtime (Instant/ofEpochMilli (.getMtime stat))
      :version (.getVersion stat)
      :cversion (.getCversion stat)
      :aversion (.getAversion stat)
      :ephemeralOwner (.getEphemeralOwner stat)
      :dataLength (.getDataLength stat)
      :numChildren (.getNumChildren stat)})))
