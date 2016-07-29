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

(deftype ZClient [connect-string zc c]
  Connectable
  ;; TODO: allow parameterization of ZooKeeper instantiation
  (open [this]
    (let [new (ZooKeeper. connect-string (int 1000) this (boolean true))]
      (swap! zc (fn [old]
                  (assert (nil? old) "Can't open already opened ZClient")
                  new)))
    this)
  (close [this]
    (let [old (swap*! zc (fn [old]
                           (assert old "Can't close unopened ZClient")
                           nil))]
      (async/close! c)
      (.close old))
    this)
  (renew [this]
    (let [new (ZooKeeper. connect-string (int 1000) this (boolean true))
          ;; TODO: Is this where we should try to renew any existing session?
          old (swap*! zc (constantly new))]
      (.close old))
    this)
  org.apache.zookeeper.Watcher
  ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html
  (process [this event]
    (let [ev (keyword (.name (.getState event)))]
      (log/debugf "ZK %s %s (%s)" ev (keyword (.name (.getType event))) (.getPath event))
      (case ev
        :AuthFailed (log/warnf "[ZK %s] SASL Authentication Failed @ %s" @zc connect-string)
        :SaslAuthenticated (log/infof "[ZK %s] SASL Authenticated" @zc)
        :ConnectedReadOnly (async/go (async/>! c [ev @zc]))
        :SyncConnected (async/go (async/>! c [ev @zc]))
        :Disconnected (async/go (async/>! c [ev @zc]))
        :Expired (do (log/warnf "Session Expired!") (async/go (async/>! c [ev @zc])) (.renew this))
        (:Unknown :NoSyncConnected) (throw (Exception. (format "Deprecated event: %s") ev))
        (throw (Exception. (format "Unexpected event: %s") ev))))))

;; TODO: Ensure root node exists
;; TODO: Track cversion of root node for a sort of heartbeat
;; TODO: shutdown on channel close and event arriving and dispense with close
(defn create [cstr c]
  {:pre [(string? cstr)]}
  (.open (->ZClient cstr (atom nil) c)))
