(ns roomkey.zclient
  "A resilient and respawning Zookeeper client"
  (:import [org.apache.zookeeper ZooKeeper Watcher WatchedEvent
            CreateMode
            Watcher$Event$EventType Watcher$Event$KeeperState
            KeeperException KeeperException$Code
            KeeperException$SessionExpiredException
            KeeperException$ConnectionLossException
            KeeperException$NodeExistsException
            ZooDefs$Ids
            data.Stat])
  (:require [clojure.string :as string]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]))

(def acls {:open-acl-unsafe ZooDefs$Ids/OPEN_ACL_UNSAFE ; This is a completely open ACL
           :anyone-id-unsafe ZooDefs$Ids/ANYONE_ID_UNSAFE ; This Id represents anyone
           :auth-ids ZooDefs$Ids/AUTH_IDS ; This Id is only usable to set ACLs
           :creator-all-acl ZooDefs$Ids/CREATOR_ALL_ACL ; This ACL gives the creators authentication id's all permissions
           :read-all-acl ZooDefs$Ids/READ_ACL_UNSAFE ; This ACL gives the world the ability to read
           })

(def create-modes {;; The znode will not be automatically deleted upon client's disconnect
                   {:persistent? true, :sequential? false} CreateMode/PERSISTENT
                   ;; The znode will be deleted upon the client's disconnect, and its name will be appended with a monotonically increasing number
                   {:persistent? false, :sequential? true} CreateMode/EPHEMERAL_SEQUENTIAL
                   ;; The znode will be deleted upon the client's disconnect
                   {:persistent? false, :sequential? false} CreateMode/EPHEMERAL
                   ;; The znode will not be automatically deleted upon client's disconnect, and its name will be appended with a monotonically increasing number
                   {:persistent? true, :sequential? true} CreateMode/PERSISTENT_SEQUENTIAL})

(defn- stat-to-map
  ([^Stat stat]
   ;;(long czxid, long mzxid, long ctime, long mtime, int version, int cversion, int aversion, long ephemeralOwner, int dataLength, int numChildren, long pzxid)
   (when stat
     {:czxid (.getCzxid stat)
      :mzxid (.getMzxid stat)
      :ctime (.getCtime stat)
      :mtime (.getMtime stat)
      :version (.getVersion stat)
      :cversion (.getCversion stat)
      :aversion (.getAversion stat)
      :ephemeralOwner (.getEphemeralOwner stat)
      :dataLength (.getDataLength stat)
      :numChildren (.getNumChildren stat)
      :pzxid (.getPzxid stat)})))

(defn- event-to-map
  [^WatchedEvent event]
  {:event-type (keyword (.name (.getType event)))
   :keeper-state (keyword (.name (.getState event)))
   :path (.getPath event)})

(defn- ^Watcher make-watcher
  [f]
  (reify Watcher
    (process [this event]
      (f event))))

;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html
(defprotocol Connectable
  (open [this connect-string timeout] "Open the connection")
  (connected? [this] "Is this client currently connected to the ZooKeeper cluster?"))

(defprotocol Closeable
  (close [this] "Close the resource"))

(defprotocol ZooKeeperFacing
  (create-znode [this path options] "Create a ZNode at the given path")
  (data [this path options] "Fetch the data from the ZNode at the path")
  (set-data [this path data version options] "Set the data on the ZNode at the given path, asserting the current version")
  (children [this path options] "Discover paths for all child znodes at the server (optionally at the given path)")
  (delete [this path version options] "Delete the znode at the given path, asserting its current version")
  (exists [this path options] "Determine if the ZNode at the given path exists"))

;; https://github.com/liwp/again/blob/master/src/again/core.clj
(defn with-retries
  [f]
  (loop [[delay & delays] (take 10 (iterate #(int (* 3/2 %)) 50))]
    (if-let [[result] (try
                        [(f)]
                        (catch KeeperException$ConnectionLossException e
                          (when-not delay (throw e))))]
      result
      (do
        (Thread/sleep delay)
        (recur delays)))))

(defmacro with-client
  "An unhygenic macro that captures `this` and `path` & binds `client` to manage connection issues"
  [& body]
  (let [emessage "Lost connection while processing ZooKeeper requests"]
    `(try (if-let [~'client (.connected? ~'this)]
            (with-retries (fn [] ~@body))
            (throw (ex-info "Client unavailable while processing ZooKeeper requests" {::path ~'path ::type ::unavailable})))
          (catch KeeperException$SessionExpiredException e# ; watches are deleted on session expiration
            (throw (ex-info ~emessage {::path ~'path ::type ::session-expired} e#)))
          (catch KeeperException$ConnectionLossException e# ; we've already been patient...
            (throw (ex-info ~emessage {::path ~'path ::type ::connection-lost} e#))))))

(defmacro with-connection
  "A hygenic macro that manages serious connection issues and provides a handler"
  [ehandler & body]
  `(try (do ~@body)
        (catch clojure.lang.ExceptionInfo e#
          (if-let [type# (some-> (ex-data e#) ::type)]
            (~ehandler e# type#)
            (throw e#)))))

(deftype ZClient [client-atom mux]
  Connectable
  (open [this connect-string timeout] ; TODO: allow parameterization of ZooKeeper instantiation
    (assert (nil? @client-atom) "Must close current connection before opeing a new connection!")
    (let [client-events (async/muxch* mux)
          raw-client-events (async/chan 1 (map event-to-map))
          client-watcher (make-watcher (partial async/put! raw-client-events))]
      (reset! client-atom (ZooKeeper. connect-string timeout client-watcher true))
      (async/put! client-events [::started @client-atom])
      (let [rc (async/go-loop [] ; https://zookeeper.apache.org/doc/r3.5.4-beta/zookeeperProgrammers.html#ch_zkSessions
                 (if-let [{:keys [event-type keeper-state path] :as event} (async/<! raw-client-events)]
                   (do
                     (assert (and (nil? path) (= :None event-type)) (format "Received node event %s for path %s on client event handler!" event-type path))
                     (log/debugf "Received raw client state event %s" keeper-state)
                     (case keeper-state
                       :SyncConnected (async/put! client-events [::connected @client-atom])
                       :Disconnected (async/put! client-events [::disconnected @client-atom])
                       :Expired (let [z' (ZooKeeper. connect-string timeout client-watcher true)]
                                  ;; Do we need to close the old client?
                                  (async/put! client-events [::expired @client-atom])
                                  (swap! client-atom (constantly z'))
                                  (async/put! client-events [::started @client-atom])
                                  (log/warnf "Session expired, new client created (%s)" (str this)))
                       (throw (Exception. (format "Unexpected event: %s" event))))
                     (recur))
                   (do
                     (log/debugf "Event processing closed for %s" (str this))
                     (async/put! client-events [::closed (swap! client-atom (fn [client] (when client (.close client timeout)) nil))]))))]
        (log/debugf "Event processing opened for %s" (str this))
        (reify Closeable (close [_] (async/close! raw-client-events) (async/<!! rc))))))
  (connected? [this] (when-let [client @client-atom]
                       (when (= :CONNECTED (some-> client .getState .toString keyword))
                         client)))
  ZooKeeperFacing
  (delete [this path version {:keys [async? callback context]
                              :or {async? false
                                   context path}}]
    (with-client (.delete client path version))
    true)
  (create-znode [this path {:keys [data acl persistent? sequential? context callback async?]
                            :or {persistent? false
                                 sequential? false
                                 acl (acls :open-acl-unsafe)
                                 context path
                                 async? false}}]
    (let [stat (Stat.)
          create-mode (create-modes {:persistent? persistent?, :sequential? sequential?})]
      (try (with-client (.create client path data acl create-mode))
           (catch KeeperException$NodeExistsException e
             false))))
  (data [this path {:keys [watcher watch? async? callback context]
                    :or {watch? false
                         async? false
                         context path}}]
    (let [stat (Stat.)]
      {:data (with-client (.getData client path (if watcher (make-watcher (comp watcher event-to-map)) watch?) stat))
       :stat (stat-to-map stat)}))
  (set-data [this path data version {:keys [async? callback context]
                                     :or {async? false
                                          context path}}]
    (try (stat-to-map (with-client (.setData client path data version)))
         (catch KeeperException e
           (when-not (= (.code e) KeeperException$Code/BADVERSION) (throw e)))))
  (children [this path {:keys [watcher watch? async? callback context sort?]
                        :or {watch? false
                             async? false
                             context path}}]
    (let [stat (Stat.)]
      {:paths (into () (with-client (.getChildren client path (if watcher (make-watcher (comp watcher event-to-map)) watch?) stat)))
       :stat (stat-to-map stat)}))
  (exists [this path {:keys [watcher watch? async? callback context]
                      :or {watch? false
                           async? false
                           context path}}]
    (when-let [stat (with-client (.exists client path (if watcher (make-watcher (comp watcher event-to-map)) watch?)))]
      (stat-to-map stat)))

  clojure.core.async.Mult
  (tap* [m ch close?] (async/tap* mux ch close?))
  (untap* [m ch] (async/untap* mux ch))
  (untap-all* [m] (async/untap-all* mux))

  clojure.lang.IFn
  (invoke [this connect-string timeout] (open this connect-string timeout))

  (applyTo [this args]
    (let [n (clojure.lang.RT/boundedLength args 1)]
      (case n
        2 (.invoke this (first args) (second args))
        (throw (clojure.lang.ArityException. n (.. this (getClass) (getSimpleName)))))))

  java.lang.Object
  (toString [this] (format "%s: %s"
                           (.. this (getClass) (getSimpleName))
                           (if-let [client @client-atom]
                             (let [server (last (re-find #"remoteserver:(\S+)" (.toString client))) ; FIXME: get the remote server cleanly
                                   b (bean client)]
                               (format "ZooKeeper@%08x State:%s sessionId:0x%15x server:%s"
                                       (System/identityHashCode client)
                                       (:state b)
                                       (:sessionId b)
                                       server))
                             "<No Raw Client>"))))

(defn create [] (->ZClient (atom nil) (async/mult (async/chan 1))))

(defmacro with-awaited-open-connection
  [zclient connect-string timeout & body]
  `(let [z# ~zclient
         cs# ~connect-string
         t# ~timeout
         c# (async/chan 10)]
     (async/tap z# c#)
     (let [r# (with-open [client# (open z# cs# t#)]
                (let [event# (first (async/<!! c#))] (assert (= ::started event#)))
                (let [event# (first (async/<!! c#))] (assert (= ::connected event#)))
                ~@body)]
       (let [event# (first (async/<!! c#))] (assert (= ::closed event#) (str event#)))
       r#)))
