(ns roomkey.zclient
  "A resilient Zookeeper client"
  (:import [org.apache.zookeeper ZooKeeper Watcher WatchedEvent
            Watcher$Event$EventType Watcher$Event$KeeperState
            KeeperException KeeperException$Code])
  (:require [zookeeper :as zoo]
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

(deftype TransientClient [connect-string zc]
  Connectable
  (open [this]
    (let [new (ZooKeeper. connect-string (int 1000) this (boolean true))
          new (swap! zc (fn [old]
                          (assert (nil? old) "Can't open already opened TransientClient")
                          new))])
    this)
  (close [this]
    (let [old (swap*! zc (fn [old]
                           (assert old "Can't close unopened TransientClient")
                           nil))]
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
        (:Unknown :NoSyncConnected) (log/warnf "[ZK %s] Received deprecated event: %s" @zc ev)
        :ConnectedReadOnly (log/infof "[ZK %s] Connected Read Only @ %s" @zc connect-string)
        :AuthFailed (log/infof "[ZK %s] SASL Authentication Failed @ %s" @zc connect-string)
        :Expired (do (log/warnf "Session Expired!") (.renew this))
        ;; :SyncConnected (log/debugf "[ZK %5s] Connected @ %s" @zc connect-string)
        ;; :SaslAuthenticated (log/debugf "[ZK %s] SASL Authenticated" @zc)
        ;; :Disconnected (log/debugf "[ZK %s] Disconnected from server @ %s" @zc connect-string)
        nil)))
  clojure.lang.IDeref
  (deref [this] @zc)
  clojure.lang.IRef
  (setValidator [this f]
    (throw (RuntimeException. "Validators are not supported by TransientClient")))
  (getValidator [this] (constantly true))
  (getWatches [this] (.getWatches zc))
  (addWatch [this k f] (.addWatch zc k f) this)
  (removeWatch [this k] (.removeWatch zc k) this))

(defn create [cstr] (->TransientClient cstr (atom nil)))
