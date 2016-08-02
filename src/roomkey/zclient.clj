(ns roomkey.zclient
  "A resilient and respawning Zookeeper client"
  (:import [org.apache.zookeeper ZooKeeper Watcher WatchedEvent
            Watcher$Event$EventType Watcher$Event$KeeperState
            KeeperException KeeperException$Code])
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]))

(defn- swap*!
  "Like clojure.core/swap, but returns previous value of atom"
  [^clojure.lang.IAtom atom f & args]
  (loop []
    (let [o (deref atom) n (apply f o args)]
      (if (compare-and-set! atom o n)
        o
        (recur)))))

(defprotocol Connectable
  (open [this] "Open the connection")
  (close [this] "Close the connection")
  (renew [this] "Renew the connection"))

(deftype ZClient [connect-string zclient ch]
  Connectable
  (open [this] ; TODO: allow parameterization of ZooKeeper instantiation
    (let [new (ZooKeeper. connect-string (int 1000) this (boolean true))]
      (swap! zclient (fn [old]
                  (assert (nil? old) "Can't open already opened ZClient")
                  new)))
    this)
  (close [this]
    (let [old (swap*! zclient (fn [old]
                           (assert old "Can't close unopened ZClient")
                           nil))]
      (async/close! ch)
      (.close old))
    this)
  (renew [this]
    (let [new (ZooKeeper. connect-string (int 1000) this (boolean true))
          old (swap*! zclient (constantly new))]
      (.close old))
    this)
  org.apache.zookeeper.Watcher
  ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html
  (process [this event]
    (let [ev (keyword (.name (.getState event)))]
      (log/debugf "ZK %s %s (%s)" ev (keyword (.name (.getType event))) (.getPath event))
      (case ev
        :AuthFailed (log/warnf "[ZK %s] SASL Authentication Failed @ %s" @zclient connect-string)
        :SaslAuthenticated (log/infof "[ZK %s] SASL Authenticated" @zclient)
        :ConnectedReadOnly (async/go (async/>! ch [ev @zclient]))
        :SyncConnected (async/go (async/>! ch [ev @zclient]))
        :Disconnected (async/go (async/>! ch [ev @zclient]))
        :Expired (do (log/warnf "Session Expired!") (.renew this))
        (:Unknown :NoSyncConnected) (throw (Exception. (format "Deprecated event: %s") ev))
        (throw (Exception. (format "Unexpected event: %s") ev))))))

;; TODO: shutdown on channel close and event arriving and dispense with close
(defn create [cstr ch]
  {:pre [(string? cstr)]}
  (.open (->ZClient cstr (atom nil) ch)))
