(ns zk.client
  "A resilient and respawning Zookeeper client"
  (:require [cognitect.anomalies :as anomalies]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as impl]
            [clojure.tools.logging :as log]
            [zk.ephemeral :as ephemeral])
  (:import [org.apache.zookeeper ZooKeeper Watcher WatchedEvent data.Stat
            AsyncCallback$VoidCallback
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
           (java.time Instant OffsetDateTime)))

;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html

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
                          KeeperException$Code/NOAUTH ::anomalies/forbidden
                          KeeperException$Code/NOCHILDRENFOREPHEMERALS ::anomalies/incorrect
                          KeeperException$Code/NODEEXISTS ::anomalies/conflict
                          KeeperException$Code/NONODE ::anomalies/not-found
                          KeeperException$Code/NOTEMPTY ::anomalies/incorrect
                          KeeperException$Code/NOTREADONLY ::anomalies/incorrect
                          KeeperException$Code/NOWATCHER ::anomalies/not-found
                          KeeperException$Code/OPERATIONTIMEOUT ::anomalies/busy
                          KeeperException$Code/RECONFIGDISABLED ::anomalies/forbidden
                          KeeperException$Code/RECONFIGINPROGRESS ::anomalies/busy
                          KeeperException$Code/REQUESTTIMEOUT ::anomalies/busy
                          KeeperException$Code/RUNTIMEINCONSISTENCY ::anomalies/fault
                          KeeperException$Code/SESSIONCLOSEDREQUIRESASLAUTH ::anomalies/forbidden ; when does this happen?  Is retry viable?
                          KeeperException$Code/SESSIONEXPIRED ::anomalies/unavailable
                          KeeperException$Code/SESSIONMOVED ::anomalies/unavailable
                          KeeperException$Code/SYSTEMERROR ::anomalies/fault
                          KeeperException$Code/UNIMPLEMENTED ::anomalies/unsupported
                          KeeperException$Code/UNKNOWNSESSION ::anomalies/incorrect}
      retryables #{::anomalies/busy ::anomalies/unavailable ::anomalies/interrupted}]
  ;; https://github.com/cognitect-labs/anomalies
  (defn translate-return-code
    "Translate callback return codes to our semantics"
    [rc]
    (let [code (KeeperException$Code/get rc)
          category (anomaly-categories code)
          retry? (boolean (retryables category))]
      [((comp keyword str) code) retry? category])))

(defn kex-info
  "Create an ExceptionInfo from the ZooKeeper return code `rc`, message `msg`, supplemental ex-info map `m` and optional `cause`"
  ;; TODO: make this a variadic function
  [rc msg m & [cause]]
  (let [[kcode retry? category] (translate-return-code rc)
        m (merge {::anomalies/category category ::kex-code kcode} m)]
    [(if cause (ex-info msg m cause) (ex-info msg m)) retry?]))

(defn translate-exception
  "Translate keeper exceptions to our semantics"
  [e]
  (kex-info (.getCode e) (.getMessage e) {} e))

(defn event-to-map
  [^WatchedEvent event]
  {:type (keyword (.name (.getType event))) :state (keyword (.name (.getState event))) :path (.getPath event)})

(defn- new-zk
  [connect-string timeout]
  (let [client-events (async/chan 2 (map event-to-map))
        client-watcher (reify Watcher
                         (process [_ event] (when-not (async/put! client-events event)
                                              (log/warnf "Failed to put event %s on closed client events channel" event))))]
    [(ZooKeeper. ^String connect-string ^int timeout client-watcher) client-events]))

(defn- close
  "Close the given `client` (whose events arrive on channel `cevents`) and monitor for proper shutdown."
  [[client cevents]]
  (async/thread (when-not (.close client 5000) (log/warnf "Unable to close client %s" (str client))))
  (async/go-loop []
    (when-let [{state :state :as event} (async/<! cevents)]
      (log/infof "[%s] Received monitor event: %s" (.hashCode client) event)
      (if (= state :Closed)
        true
        (recur)))))

(defprotocol Notifiable
  (register [this] "Register to receive notifications from `this` on the returned channel.  Use the returned channel to deregister.")
  (deregister [this ch] "Deregister from receiving notifications on `ch`"))

(defn- connect*
  "Open a resilient connection to the cluster at `connect-string` and return a channel-like command handle that can be closed to disconnect
  the client."
  [new-z zap session]
  (let [command (async/chan)
        result (async/go-loop [state ::initial [client cevents :as cpair] (new-z)]
                 (let [[{state' :state :as event} _] (async/alts! [cevents command])]
                   (if event
                     (do (log/infof "[%s] Received primary event: %s " (.hashCode client) event)
                         (log/infof "[%s] State transition: %15s -> %15s" (.hashCode client) (name state) (name state'))
                         (let [cpair' (case state'
                                        (:SyncConnected :ConnectedReadOnly) (do (deliver @zap client)
                                                                                (when (#{:Expired ::initial} state) ; new session
                                                                                  (async/>! session client))
                                                                                cpair)
                                        :Disconnected (do (when (realized? @zap) (reset! zap (promise)))
                                                          cpair) ; be patient, the client might reconnect.
                                        :Expired (do (when (realized? @zap) (reset! zap (promise)))
                                                     (close cpair)
                                                     (new-z))
                                        ;; :Closed cpair ; Only the shutdown monitor should see this ... what happened?
                                        (throw (Exception. (format "Unexpected event state received %s while in state %s." state' state))))]
                           (recur state' cpair')))
                     (do (when (realized? @zap) (reset! zap (promise)))
                         (async/<! (close cpair))))))]
    (reify ; a duplex stream a la manifold
      java.lang.AutoCloseable
      (close [this] (async/close! this) (async/<!! this)) ; synchronized with connect loop shutdown
      impl/ReadPort
      (take! [this handler] (impl/take! result handler))
      impl/Channel
      (close! [this] (async/close! command))
      (closed? [this] (impl/closed? command)))))

(defprotocol Connectable
  (connect [this connect-string options]))

(deftype ZClient [zap m-session]
  Connectable
  (connect [this connect-string options]
    (let [{timeout :timeout :or {timeout 30000}} options
          new-zk (partial new-zk connect-string timeout)]
      (connect* new-zk zap (async/muxch* m-session))))
  Notifiable ; manage notifications for session establishment
  (register [this] (let [wch (async/chan 1 (dedupe))] ; FIXME: need generational guarantee -dedupe is not strictly adequate & distinct is expensive.
                     (async/tap m-session wch)
                     (async/thread (when-let [z (deref @zap)] (async/put! wch z))) ; maybe bootstrap the watcher
                     wch))
  (deregister [this wch] (async/untap m-session wch))
  clojure.lang.IDeref
  (deref [this] (deref @zap))
  clojure.lang.IBlockingDeref
  (deref [this timeout timeout-value] (deref @zap timeout timeout-value))
  clojure.lang.IPending
  (isRealized [this] (realized? @zap))
  clojure.lang.IFn
  (invoke [this f] (.invoke this f (fn [e] (throw e))))
  (invoke [this f handler]
    (loop [i 5]
      (let [result (try (f @@zap) ; block waiting
                        (catch KeeperException e
                          (let [[ex retry?] (translate-exception e)]
                            (when-not (and retry? (pos? i)) [(handler ex)]))))])))
  (applyTo [this args]
    (let [n (clojure.lang.RT/boundedLength args 1)]
      (case n
        2 (.invoke this (first args) (second args)))))
  java.lang.Object
  (toString [this] (format "ℤℂ: %s"
                           (if-let [z (deref @zap 0 nil)]
                             (format "@%04x 0x%08x (%s)"
                                     (System/identityHashCode z)
                                     (bit-and 0x00000000FFFFFFFF (.getSessionId z)) ; just the LSBs please
                                     (.getState z))
                             "<No Raw Client>"))))

(defn create
  []
  (->ZClient (atom (promise)) (async/mult (async/chan))))

(defn open
  [client connect-string & options]
  (connect client connect-string options))

(defn call
  [zap f]
  (if-let [z (@@zap)]
    (loop [i 5]
      (let [result (try (f z)
                        (catch KeeperException$ConnectionLossException e ::connection-loss))]
        (if (= ::connection-loss result)
          (if (pos? i)
            (recur (dec i))
            (throw (ex-info "Exceeded connection loss retry threshold" {:zap zap})))
          result)))
    (throw (ex-info "The zap is nil -client has closed!" {:zap zap}))))

(defmacro while->open
  "Creates a client binding to the `name` symbol and threads it (per `->`) into
  the given `open-expression`.  Ensures the client is actually connected before
  executing `body` and synchronously ensures the client is properly closed
  before exiting."
  [[name open-expression] & body]
  `(let [~name (create)]
     (with-open [client# (~(first open-expression) ~name ~@(rest open-expression))]
       (async/<!! (register ~name))
       ~@body)))
