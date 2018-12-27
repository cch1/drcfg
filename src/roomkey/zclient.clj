(ns roomkey.zclient
  "A resilient and respawning Zookeeper client"
  (:import [org.apache.zookeeper ZooKeeper Watcher WatchedEvent
            Watcher$Event$EventType Watcher$Event$KeeperState
            KeeperException KeeperException$Code])
  (:require [zookeeper :as zoo]
            [clojure.string :as string]
            [clojure.core.async :as async]
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

(defprotocol ZooKeeperFacing
  (create-all [this path options] "Create a ZNode at the given path")
  (data [this path options] "Fetch the data from the ZNode at the path")
  (set-data [this path data version options] "Set the data on the ZNode at the given path, asserting the current version"))

(defmacro with-zclient
  "An unhygenic macro that captures `client` and binds `zclient` to manage connection issues"
  [& body]
  (let [emessage "Lost connection while processing ZooKeeper requests"]
    `(try (if-let [~'zclient (deref ~'client)]
            ~@body
            (throw (ex-info "Client unavailable while processing ZooKeeper requests" {:path (.path ~'this)})))
          (catch KeeperException$SessionExpiredException e# ; watches are deleted on session expiration
            (throw (ex-info ~emessage {:path (.path ~'this)} e#)))
          (catch KeeperException$ConnectionLossException e# ; be patient...
            (throw (ex-info ~emessage {:path (.path ~'this)} e#))))))

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
  ZooKeeperFacing
  (create-all [this path options]
    (loop [result-path "" [dir & children] (rest (string/split path #"/"))]
      (let [result-path (str result-path "/" dir)
            created? (zoo/create @client-atom result-path :persistent? true)]
        (if-not (seq children)
          created?
          (recur result-path children)))))
  (data [this path options] (apply zoo/data @client-atom path (mapcat identity options)))
  (set-data [this path data version options]
    (boolean (try (apply zoo/set-data @client-atom path data version (mapcat identity options))
                  (catch KeeperException e
                    (when-not (= (.code e) KeeperException$Code/BADVERSION) (throw e))))))
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
