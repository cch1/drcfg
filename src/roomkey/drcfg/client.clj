(ns roomkey.drcfg.client
  "Dynamic Distributed Run-Time configuration"
  (:import org.apache.zookeeper.KeeperException)
  (:require [roomkey.zkutil :as zk]))

(def zk-prefix "/drcfg")

(defn connected?
  [client]
  (and client (zk/is-connected? client)))

(defn connect
  [hosts]
  (let [connect-string (str hosts zk-prefix)
        client (zk/connect connect-string)]
    (zk/init! client)
    client))

(def zkconn!
  "Connect to zookeeper, using memoized connection if available"
  (memoize connect))
