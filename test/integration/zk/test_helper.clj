(ns integration.zk.test-helper
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log])
  (:import [java.time Instant]
           [org.apache.curator.test TestingServer TestingCluster]
           [org.apache.zookeeper ZooKeeper data.Stat]))

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

(defn- check-map
  [expected]
  (fn [actual] (reduce-kv (fn [acc k v] (when acc (if (fn? v)
                                                    (v (k actual))
                                                    (= v (k actual)))))
                          true
                          expected)))

(defn with-metadata?
  [expected]
  (fn [v] (and (if (fn? expected) (expected v) (= expected v))
               ((check-map (meta expected)) (meta v)))))

(let [base {:version int?
            :cversion int?
            :aversion int?
            :ctime (partial instance? java.time.Instant)
            :mtime (partial instance? java.time.Instant)
            :mzxid pos-int?
            :czxid pos-int?
            :pzxid pos-int?
            :numChildren int?
            :ephemeralOwner int?
            :dataLength int?}]
  (defn *stat?
    ([] (*stat? {}))
    ([extra] (fn [actual] (and ((check-map extra) actual)
                               ((check-map base) actual))))))

(def initial-stat? {:version -1 :aversion -1 :cversion -1})

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
