(ns roomkey.zclient
  "A resilient and respawning Zookeeper client"
  (:import [org.apache.zookeeper ZooKeeper Watcher WatchedEvent
            Watcher$Event$EventType Watcher$Event$KeeperState
            KeeperException KeeperException$Code])
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]))

(defn event-to-map
  [^WatchedEvent event]
  {:event-type (keyword (.name (.getType event)))
   :keeper-state (keyword (.name (.getState event)))
   :path (.getPath event)})

(defn ^Watcher make-watcher
  [f]
  (reify Watcher
    (process [this event]
      (f event))))

;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html
(defprotocol Connectable
  (open [this connect-string timeout] "Open the connection")
  (close [this] "Close the connection"))

(deftype ZClient [client-atom raw-client-events client-events mux]
  Connectable
  (open [this connect-string timeout] ; TODO: allow parameterization of ZooKeeper instantiation
    (let [client-watcher (make-watcher (partial async/put! raw-client-events))]
      (reset! client-atom (ZooKeeper. connect-string timeout client-watcher (boolean true)))
      (async/put! client-events [::started @client-atom])
      (async/go-loop []
        (if-let [{:keys [event-type keeper-state path] :as event} (async/<! raw-client-events)]
          (do
            (assert (and (nil? path) (= :None event-type)) (format "Received node event %s for path %s on client event handler!" event-type path))
            (log/infof "Received raw client state event %s" keeper-state)
            (case keeper-state
              :SyncConnected (do
                               (async/put! client-events [::connected @client-atom])
                               (recur))
              :Disconnected (do
                              (async/put! client-events [::disconnected @client-atom])
                              (recur))
              :Expired (do (log/warnf "Session Expired!")
                           (let [z' (ZooKeeper. connect-string timeout client-watcher (boolean true))]
                             ;; Do we need to close the old client?
                             (async/put! client-events [::expired @client-atom])
                             (swap! client-atom (constantly z'))
                             (async/put! client-events [::started @client-atom])
                             (recur)))
              (throw (Exception. (format "Unexpected event: %s" event)))))
          (do
            (log/infof "The raw client event channel has closed, shutting down")
            (.close @client-atom 1000)
            (async/put! client-events [::closed @client-atom])
            (reset! client-atom nil)
            (async/close! client-events)))))
    this)
  (close [this]
    (async/close! raw-client-events)
    this)
  clojure.lang.IDeref
  (deref [this] (deref client-atom))
  clojure.core.async.Mult
  (tap* [m ch close?] (async/tap mux ch close?))
  (untap* [m ch] (async/untap mux ch))
  (untap-all* [m] (async/untap-all mux)))

(defn create
  []
  {:pre []}
  (let [client-events (async/chan 1)
        raw-client-events (async/chan 1 (map event-to-map))]
    (->ZClient (atom nil) raw-client-events client-events (async/mult client-events))))
