(ns zk.client
  "A resilient and respawning Zookeeper client"
  (:import [org.apache.zookeeper ZooKeeper Watcher WatchedEvent data.Stat
            AsyncCallback$Create2Callback AsyncCallback$StatCallback AsyncCallback$VoidCallback
            AsyncCallback$DataCallback AsyncCallback$Children2Callback
            CreateMode ZooKeeper$States
            Watcher$Event$EventType Watcher$Event$KeeperState
            ZooDefs$Ids
            KeeperException KeeperException$Code
            AddWatchMode
            Watcher$WatcherType
            ;; Exceptions
            KeeperException$APIErrorException KeeperException$AuthFailedException KeeperException$BadArgumentsException
            KeeperException$BadVersionException KeeperException$ConnectionLossException KeeperException$DataInconsistencyException
            KeeperException$EphemeralOnLocalSessionException KeeperException$InvalidACLException KeeperException$InvalidCallbackException
            KeeperException$MarshallingErrorException KeeperException$NewConfigNoQuorum KeeperException$NoAuthException
            KeeperException$NoChildrenForEphemeralsException KeeperException$NodeExistsException KeeperException$NoNodeException
            KeeperException$NotEmptyException KeeperException$NotReadOnlyException KeeperException$NoWatcherException
            KeeperException$OperationTimeoutException KeeperException$ReconfigDisabledException KeeperException$ReconfigInProgress
            KeeperException$RequestTimeoutException KeeperException$RuntimeInconsistencyException KeeperException$SessionClosedRequireAuthException
            KeeperException$SessionExpiredException KeeperException$SessionMovedException KeeperException$SystemErrorException
            KeeperException$UnimplementedException KeeperException$UnknownSessionException]
           (java.nio.file Paths Path)
           (java.time Instant OffsetDateTime))
  (:require [cognitect.anomalies :as anomalies]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as impl]
            [clojure.tools.logging :as log]))

(let [anomaly-categories {KeeperException$Code/OK nil
                          KeeperException$Code/APIERROR ::anomalies/incorrect
                          KeeperException$Code/AUTHFAILED ::anomalies/forbidden
                          KeeperException$Code/BADARGUMENTS ::anomalies/incorrect
                          KeeperException$Code/BADVERSION ::anomalies/conflict
                          KeeperException$Code/CONNECTIONLOSS ::anomalies/unavailable
                          KeeperException$Code/DATAINCONSISTENCY ::anomalies/conflict
                          KeeperException$Code/EPHEMERALONLOCALSESSION ::anomalies/incorrect
                          KeeperException$Code/INVALIDACL ::anomalies/incorrect
                          KeeperException$Code/INVALIDCALLBACK ::anomalies/incorrect
                          KeeperException$Code/MARSHALLINGERROR ::anomalies/incorrect
                          KeeperException$Code/NEWCONFIGNOQUORUM ::anomalies/unavailable
                          KeeperException$Code/NOAUTH ::anomalies/FORBIDDEN
                          KeeperException$Code/NOCHILDRENFOREPHEMERALS ::anomalies/incorrect
                          KeeperException$Code/NODEEXISTS ::anomalies/conflict
                          KeeperException$Code/NONODE ::anomalies/not-found
                          KeeperException$Code/NOTEMPTY ::anomalies/incorrect
                          KeeperException$Code/NOTREADONLY ::anomalies/incorrect
                          KeeperException$Code/NOWATCHER ::anomalies/not-found
                          KeeperException$Code/OPERATIONTIMEOUT ::anomalies/busy
                          KeeperException$Code/RECONFIGDISABLED ::anomalies/forbidden
                          KeeperException$Code/RECONFIGINPROGRESS ::anomalies/busy
                          KeeperException$Code/REQUESTTIMEOUT ::anomalies/BUSY
                          KeeperException$Code/RUNTIMEINCONSISTENCY ::anomalies/fault
                          KeeperException$Code/SESSIONCLOSEDREQUIRESASLAUTH ::anomalies/forbidden ; when does this happen?  Is retry viable?
                          KeeperException$Code/SESSIONEXPIRED ::anomalies/unavailable
                          KeeperException$Code/SESSIONMOVED ::anomalies/unavailable
                          KeeperException$Code/SYSTEMERROR ::anomalies/fault
                          KeeperException$Code/UNIMPLEMENTED ::anomalies/unsupported
                          KeeperException$Code/UNKNOWNSESSION ::anomalies/incorrect}
      retryables #{::anomalies/busy ::anomalies/unavailable ::anomalies/interrupted}]
  ;; https://github.com/cognitect-labs/anomalies
  (defn- translate-return-code
    "Translate callback return codes to our semantics"
    [rc]
    (let [code (KeeperException$Code/get rc)
          category (anomaly-categories code)
          retry? (boolean (retryables category))]
      [retry? category code]))

  (defn- translate-exception
    "Translate keeper exceptions to our semantics"
    [e]
    (let [msg (.getMessage e)
          [retry? category] (translate-return-code (.getCode e))
          ex (ex-info msg {::anomalies/category category} e)]
      [retry? ex])))

(def return-codes
  "A map from return code to transparent keyword"
  (into {} (map (fn [ecode] [(.intValue ecode) ((comp keyword str) ecode)])) (KeeperException$Code/values)))

(def watch-modes {{:persistent? true :recursive? false} AddWatchMode/PERSISTENT
                  {:persistent? true :recursive? true} AddWatchMode/PERSISTENT_RECURSIVE})

(defn- event-to-map
  [^WatchedEvent event]
  {:type (keyword (.name (.getType event)))
   :state (keyword (.name (.getState event)))
   :path (.getPath event)})

(defn- synthesize-child-events
  "A transducer to Inject :NodeChildrenChanged events into the sequence-ish."
  [rf]
  (fn synthesize-child-events'
    ([] (rf))
    ([result] (rf result))
    ([result {:keys [type path] :as input}]
     (let [result (if (#{:NodeCreated :NodeDeleted} type)
                    (rf result {:type :NodeChildrenChanged :path (some-> (.getParent (Paths/get path (into-array String []))) str)})
                    result)]
       (rf result input)))))

(defn- prefix-kw [x prefix] (keyword (str (namespace x)) (str prefix (name x))))

;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html
(defprotocol Connectable
  (open [this connect-string options] "Open the connection to `connect-string` and stream client's events to `events`")
  (connected? [this] "Is this client currently connected to the ZooKeeper cluster?"))

(deftype ZClient [client-atom]
  Connectable
  (open [this connect-string {:keys [timeout recursive? can-be-read-only? path]
                              :or {timeout 2000 can-be-read-only? true recursive? true path "/"}}]
    (assert (nil? @client-atom) "Must close current connection before opening a new connection!")
    (let [watch-mode (watch-modes {:persistent? true :recursive? (boolean recursive?)})
          client-events (async/chan 8 (map event-to-map))
          node-events (async/chan 8 (map event-to-map))
          watch-tracker (async/chan 2)
          events (async/chan (async/sliding-buffer 8))
          client-watcher (reify Watcher
                           (process [_ event] (when-not (async/put! client-events event)
                                                (log/warnf "Failed to put event %s on closed client events channel" event))))
          node-watcher (reify Watcher
                         (process [_ event] (when-not (async/put! node-events event)
                                              (log/warnf "Failed to put event %s on closed node events channel" event))))
          add-node-watch (fn add-node-watch [z [backoff & backoffs]]
                           (let [cb (reify AsyncCallback$VoidCallback
                                      (processResult [this rc path ctx]
                                        (if (= :OK (return-codes rc))
                                          (async/put! watch-tracker ::watch-added)
                                          (let [[retry? category code] (translate-return-code rc)
                                                state (.getState z)]
                                            (if (and backoff retry? (not (#{ZooKeeper$States/CLOSED} state)))
                                              (do (log/warnf "Unable to add watch [%s/%s : %s], backing off %d"
                                                             code state (name category)  backoff)
                                                  (Thread/sleep backoff)
                                                  (add-node-watch z backoffs)) ; being careful with the stack.
                                              (do (log/warnf "Failed to add watch [%s : %s]." code (name category))
                                                  (async/put! watch-tracker ::failed-to-watch)))))))]
                             (.addWatch z path node-watcher watch-mode cb nil)))
          new-client (fn [] (ZooKeeper. ^String connect-string ^int timeout client-watcher can-be-read-only?))
          rc0 (async/go-loop [] ; This could be replaced with a transducer
                (when-let [{:keys [state type path] :as event} (async/<! node-events)]
                  (log/debugf "Received node event %s %s %s [%s]" state type path this)
                  (assert (or path (= :None type)) "There should always be a path for node watch events")
                  (when path
                    (async/>! events (dissoc event :state))
                    (when (#{:NodeCreated :NodeDeleted} type)
                      (async/>! events {:type :NodeChildrenChanged
                                        :path (some-> (.getParent (Paths/get path (into-array String []))) str)})))
                  (recur)))
          command (async/chan 2)
          result (async/go-loop [state ::init client nil]
                   (if-let [e (async/alt! client-events ([e] (:state e))
                                          watch-tracker ([e] e)
                                          command ([c] (if c c ::close!))
                                          (async/timeout 60000) ::heartbeat
                                          :priority true)]
                     (do (log/tracef "Received command event %15s [%12s]" (name e) (name state))
                         (let [[state' client] (case [state e] ; TODO: clojure.core.match?
                                                 [::init ::open!] [::connecting (new-client)]
                                                 ([::connecting :SyncConnected] [::connecting :ConnectedReadOnly])
                                                 , (do (add-node-watch client (take 16 (iterate #(int (* 2 %)) 1))) [::connected client])
                                                 [::connecting ::watch-added] [::connecting client] ; rare: :Expire immediately after connected->add-watch
                                                 ;; ([::connected :SyncConnected] [::connected :ConnectedReadOnly]) ::connected ; to/from read-only
                                                 ;; ([::connected :Disconnected] [::watching :Disconnected]) ::reconnecting
                                                 ([::connected :Expired] [::watching :Expired])
                                                 , (do (reset! client-atom nil) [::connecting (new-client)]) ; testing only?
                                                 [::connected ::watch-added]
                                                 , (do (reset! client-atom client) [::watching client]) ; the ideal steady-state
                                                 [::watching ::heartbeat] [::watching client] ; the ideal steady-state Part Deux
                                                 ([::connecting ::heartbeat] [::connected ::heartbeat] [::reconnecting ::heartbeat])
                                                 , [state client]
                                                 [::watching :Disconnected] [::reconnecting client]
                                                 ([::reconnecting :SyncConnected] [::reconnecting :ConnectedReadOnly])
                                                 , [::watching client] ; watches survive
                                                 [::reconnecting :Expired] (do (reset! client-atom nil) [::connecting (new-client)])
                                                 ([::connecting ::close!] [::connected ::close!] [::watching ::close!] [::reconnecting ::close!])
                                                 , (do (reset! client-atom nil)
                                                       (if (async/<! (async/thread (.close ^ZooKeeper client timeout)))
                                                         [::closing client]
                                                         [(prefix-kw state "closed-") client]))
                                                 ([::closing :SyncConnected] [::closing :ConnectedReadOnly] [::closing :Disconnected])
                                                 , [::closing client] ;; be patient
                                                 [::closing ::close!] [::closing client] ;; be patient... there can be a lot of these.
                                                 [::closing :Expired] [::expired-closing client] ; this is a terminal state
                                                 [::closing :Closed] [::closed client] ; The ideal final state (clean shutdown).
                                                 (throw (Exception. (format "Unexpected event %s while in state %s." e state))))]
                           (log/debugf "Event received: %14s [%12s -> %-14s]" (name e) (name state) (name state'))
                           (when (not= state state') (async/>! events state'))
                           (if (#{::closed ::failed-to-watch ::closed-connecting ::closed-connected ::closed-reconnecting ::expired-closing} state')
                             (do (when (#{::closed-connecting ::closed-connected ::closed-reconnecting} state')
                                   (log/warnf "%s did not shut down cleanly: %s" this state'))
                                 (async/close! node-events)
                                 (async/close! client-events)
                                 (async/<! rc0)
                                 state')
                             (recur state' client))))
                     ::closed-unexpectedly))]
      (async/>!! command ::open!)
      (log/debugf "Event processing opened for %s" (str this))
      (reify
        java.lang.AutoCloseable
        (close [this]
          (async/close! command)
          (let [result (async/<!! result)] (log/debugf "Closed with %s." result))
          (.close! this))
        impl/ReadPort
        (take! [this handler] (impl/take! events handler))
        impl/Channel
        (closed? [this] (impl/closed? events))
        (close! [this] (async/close! command) (impl/close! events)))))
  (connected? [this] (when-let [client ^ZooKeeper @client-atom]
                       (when (#{ZooKeeper$States/CONNECTED ZooKeeper$States/CONNECTEDREADONLY} (.getState client))
                         client)))

  clojure.lang.IFn
  (invoke [this f] (.invoke this f (fn [e] (log/warnf e "Failed.") (throw e))))
  (invoke [this f handler] ; resiliently invoke `f` with a raw client, calling the handler on unrecoverable exceptions
    (loop [[backoff & backoffs] (take 12 (iterate #(int (* 2 %)) 2))]
      (if-let [[result] (try (if-let [client ^ZooKeeper @client-atom]
                               [(f client)]
                               (throw (ex-info (format "Client %s unavailable while processing ZooKeeper request." this)
                                               {::anomalies/category ::anomalies/unavailable})))
                             (catch clojure.lang.ExceptionInfo ex
                               (when-not backoff [(handler ex)]))
                             (catch KeeperException e
                               (let [[retry? ex] (translate-exception e)]
                                 (when-not (and retry? backoff) [(handler ex)]))))]
        result
        (do
          (log/debugf "Backing off %dms to try again" backoff)
          (Thread/sleep backoff)
          (recur backoffs)))))

  (applyTo [this args]
    (let [n (clojure.lang.RT/boundedLength args 1)]
      (case n
        2 (.invoke this (first args) (second args))
        (throw (clojure.lang.ArityException. n (.. this (getClass) (getSimpleName)))))))

  java.lang.Object
  (toString [this] (format "ℤℂ: %s"
                           (if-let [client @client-atom]
                             (let [server (last (re-find #"remoteserver:(\S+)" (.toString ^ZooKeeper client)))]
                               (format "@%04x 0x%08x (%s)"
                                       (System/identityHashCode client)
                                       (bit-and 0x00000000FFFFFFFF (.getSessionId client)) ; just the LSBs please
                                       (.getState client)))
                             "<No Raw Client>"))))

(defn create ^zk.client.ZClient [] (->ZClient (atom nil)))

(defmacro while-watching
  "Evaluate the body after the watch has started, binding `nevents` to a channel that streams observed node events"
  [[nevents open-expression] & body]
  `(let [chandle# ~open-expression]
     (let [[~nevents cevents#] (async/split :path chandle# (async/sliding-buffer 8) (async/sliding-buffer 1))]
       (async/<!! (async/go-loop [] (when-not (= ::watching (async/<! cevents#)) (recur))))
       (try
         ~@body
         (finally
           (. chandle# close)
           (async/<!! (async/go-loop []
                        (when-not (#{::closed ::closed-connecting ::closed-connected ::expired-closing ::closed-unexpectedly}
                                   (async/<! cevents#))
                          (recur)))))))))
