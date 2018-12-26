(ns roomkey.zref
  "A Zookeeper-based reference type"
  (:import [org.apache.zookeeper ZooKeeper KeeperException KeeperException$Code
            KeeperException$SessionExpiredException
            KeeperException$ConnectionLossException
            KeeperException$NoWatcherException
            Watcher$WatcherType])
  (:require [zookeeper :as zoo]
            [roomkey.zclient :as zclient]
            [clojure.string :as string]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]))

;; https://github.com/liebke/zookeeper-clj
;; https://github.com/torsten/zookeeper-atom

(defn ^:dynamic *deserialize* [b] {:pre [(instance? (Class/forName "[B") b)]} (read-string (String. b "UTF-8")))

(defn ^:dynamic *serialize* [obj] {:post [(instance? (Class/forName "[B") %)]} (.getBytes (binding [*print-dup* true] (pr-str obj))))

(def ^:dynamic *max-update-attempts* 50)

(defn- valid?
  [validator v]
  (or (not validator) (validator v)))

(defn- validate!
  [validator v]
  (when-not (valid? validator v)
    (throw (IllegalStateException. "Invalid reference state"))))

(defn create-all
  "Create a node and all of its parents, but without the race conditions."
  [client path]
  (loop [result-path "" [dir & children] (rest (string/split path #"/"))]
    (let [result-path (str result-path "/" dir)
          created? (zoo/create client result-path :persistent? true)]
      (if-not (seq children)
        created?
        (recur result-path children)))))

;;; A Reference type persisted in a zookeeper cluster.  The semantics are similar to a Clojure Atom
;;; with the following major differences:
;;;  * The read state (accessed via deref) may lag successful write operations (e.g. swap!)
;;;  * Read-only metadata is available which represents the zookeeper Stat data structure
;;;  * No updates are possible while disconnected
;;;  * The compare-and-set semantics are tightened to insist that updates can only apply to the
;;;    current value AND current version.
;;;  * The swap operation can fail if there is too much contention for a znode.
;;;  * Simple watcher functions are wrapped to ignore the version parameter applied to full-fledged watchers.
(defprotocol UpdateableZNode
  (zInitialize [this client] "Initialize the ZooKeeper node backing this zref")
  (zConnect [this client] "Start online operations with the given client (an instance of org.apache.ZooKeeper)")
  (zDisconnect [this channel] "Stop online operations")
  (zUpdate [this client version value] "Update the znode backing this zref"))

(defprotocol VersionedUpdate
  (compareVersionAndSet [this current-version new-value] "Set to new-value only when current-version is latest"))

(defprotocol VersionedDeref
  (vDeref [this] "Return referenced value and version"))

(defprotocol VersionedWatch
  "A protocol for adding versioned watchers using the same associative storage as \"classic\" watchers"
  (vAddWatch [this k f] "Add versioned watcher that will be called with new value and version"))

(defmacro with-monitored-client
  "An unhygenic macro that captures `this` and binds `client` to manage connection issues"
  [& body]
  (let [emessage "Lost connection while processing ZooKeeper requests"]
    `(try (if-let [~'client (deref (.client-atom ~'this))]
            ~@body
            (throw (ex-info "Client unavailable while processing ZooKeeper requests" {:path (.path ~'this)})))
          (catch KeeperException$SessionExpiredException e# ; watches are deleted on session expiration
            (throw (ex-info ~emessage {:path (.path ~'this)} e#)))
          (catch KeeperException$ConnectionLossException e# ; be patient...
            (throw (ex-info ~emessage {:path (.path ~'this)} e#))))))

(deftype ZRef [path client-atom cache validator watches]
  UpdateableZNode
  (zInitialize [this client]
    (try (when (create-all client path) ; idempotent side effects
           (log/infof "Created node %s" path))
         (let [c (.zConnect this client)]
           (when (.zUpdate this client 0 (.deref this)) ; idempotent side effects
             (log/infof "Updated node %s with default value" path))
           c)
         (catch clojure.lang.ExceptionInfo e
           (log/infof e "Lost connection while initializing %s" path)
           false)))
  (zConnect [this client]
    (reset! client-atom client)
    (let [znode-events (async/chan 1)
          xform (map (fn [zdata] (let [m (:stat zdata)
                                       obj ((juxt (comp *deserialize* :data) (comp :version :stat)) zdata)]
                                   (with-meta obj m))))
          data-changed-events (async/chan 1 xform)]
      (async/go-loop [] ; start event listener loop
        (if-let [{:keys [event-type keeper-state] :as event} (async/<! znode-events)]
          (do
            (log/infof "** Event [%s:%s] received by %s" event-type keeper-state path)
            (case event-type
              :None (do (assert (nil? (:path event)) "Keeper State event received with a path!") ; should be handled by default watch on client
                        (recur))
              :NodeDeleted (log/warnf "Node %s deleted" path)
              :DataWatchRemoved (log/infof "Data watch on %s removed" path)
              (::Boot :NodeDataChanged) (do
                                          (async/put! data-changed-events
                                                      (zoo/data client path :watcher (partial async/put! znode-events)))
                                          (recur))
              (log/warnf "Unexpected event:state [%s:%s] while watching %s" event-type keeper-state path)))
          (do
            (log/infof "The znode event channel for %s has closed, shutting down" path)
            (async/close! data-changed-events))))

      (async/go-loop [] ; start data change listener loop
        (if-let [[value' version' :as n] (async/<! data-changed-events)]
          (let [[value version :as o] @cache
                delta (- version' version)]
            (log/infof "*** Got %s" n)
            (cond
              (neg? delta) (log/warnf "Received negative version delta [%d -> %d] for %s" version version' path)
              (zero? delta) (log/tracef "Received zero version delta [%d -> %d] for %s" version version' path)
              (and (> version 1) (> delta 1)) (log/infof "Received non-sequential version delta [%d -> %d] for %s"
                                                         version version' path))
            (if (valid? @validator value')
              (do (reset! cache n)
                  (when (pos? version)
                    (async/thread (doseq [[k w] @watches]
                                    (try (w k this o n)
                                         (catch Exception e (log/errorf e "Error in watcher %s" k)))))))
              (log/warnf "Watcher received invalid value [%s], ignoring update for %s" value' path))
            (recur))
          (log/infof "The data-changed-events channel has closed, shutting down")))
      (async/put! znode-events {:event-type ::Boot})
      znode-events))
  (zDisconnect [this channel]
    (async/close! channel)
    this)
  (zUpdate [this client version value]
    (validate! @validator value)
    (boolean (try (let [r (zoo/set-data client path (*serialize* value) version)]
                    (when r (log/infof "Set value for %s to %s" path value version))
                    r)
                  (catch KeeperException e
                    (when-not (= (.code e) KeeperException$Code/BADVERSION) (throw e))))))
  ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkWatches
  ;; https://www.safaribooksonline.com/library/view/zookeeper/9781449361297/ch04.html

  VersionedUpdate
  (compareVersionAndSet [this current-version newval]
    (.zUpdate this @client-atom current-version newval))
  VersionedDeref
  (vDeref [this] @cache)

  VersionedWatch
  (vAddWatch [this k f] (swap! watches assoc k f) this)

  clojure.lang.IMeta
  ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#sc_timeInZk
  (meta [this] (meta @cache))

  clojure.lang.IDeref
  (deref [this] (-> (.vDeref this) first))

  clojure.lang.IRef
  (setValidator [this f]
    (validate! f (.deref this))
    (reset! validator f)
    this)
  (getValidator [this] @validator)
  (getWatches [this] @watches)
  (addWatch [this k f] (.vAddWatch this k (fn [k r [o _] [n _]] (f k r o n))))
  (removeWatch [this k] (swap! watches dissoc k) this)

  clojure.lang.IAtom
  (reset [this value] (.compareVersionAndSet this -1 value) value)
  (compareAndSet [this oldval newval]
    (let [[value version] (.vDeref this)]
      (boolean (and (= oldval value)
                    (.compareVersionAndSet this version newval)))))
  (swap [this f]
    (loop [n 1 i *max-update-attempts*]
      (let [[value version] (.vDeref this)
            value' (f value)]
        (if (.compareVersionAndSet this version value')
          value'
          (do
            (when-not (pos? i) (throw (RuntimeException.
                                       (format "Aborting update of %s after %d failures over ~%dms"
                                               path *max-update-attempts* (* 2 n)))))
            (Thread/sleep n)
            (recur (* 2 n) (dec i)))))))
  (swap [this f x] (.swap this (fn [v] (f v x))))
  (swap [this f x y] (.swap this (fn [v] (f v x y))))
  (swap [this f x y args] (.swap this (fn [v] (apply f v x y args))))
  java.lang.Object
  (toString [this] (str (.vDeref this))))

(defn create
  [path default & options]
  (let [{validator :validator} (apply hash-map options)
        z (->ZRef path (atom nil) (atom (with-meta [default -1] {:version -1}))
                  (atom nil) (atom {}))]
    (when validator (.setValidator z validator))
    z))

(def ^:deprecated zref create)

(defn- process-client-manager-events
  [zref events]
  (let [path (.path zref)]
    (async/go-loop [booted? false] ; start event listener loop
      (if-let [[event client] (async/<! events)]
        (do
          (log/infof "* Event %s received" event)
          (recur (case event
                   ::zclient/started booted?
                   ::zclient/connected (do
                                         (when (not booted?) (.zInitialize zref client))
                                         (.zConnect zref client)
                                         true)
                   ::zclient/disconnected booted? ; be patient
                   ::zclient/expired booted?
                   ::zclient/closed false ; do we need to remove watches?
                   (log/warnf "Unexpected event [%s] while watching %s" event path))))
        (log/infof "The znode event channel for %s has closed, shutting down" path)))))

(defn link
  [zref zclient]
  (let [client-events (async/chan 1)]
    (async/tap (.mux zclient) client-events)
    (process-client-manager-events zref client-events)
    client-events))

(defn versioned-deref
  "Return the current state (value and version) of the zref `z`."
  [z]
  {:pre [(instance? roomkey.zref.ZRef z)]}
  (.vDeref z))

(defn compare-version-and-set!
  "Atomically sets the value of z to `newval` if and only if the current
  version of `z` is identical to `current-version`. Returns true if set
  happened, else false"
  [z current-version newval]
  {:pre [(instance? roomkey.zref.ZRef z) (integer? current-version)]}
  (.compareVersionAndSet z current-version newval))

(defn add-versioned-watch
  "Adds a watch function to the zref z.  The watch fn must be a fn of 4 args:
  the key, the zref, its old-state and its new-state. Whenever the zref's
  state might have been changed, any registered watches will have their
  functions called.  Note that the zref's state may have changed again
  prior to the fn call, so use old/new-state rather than derefing the zref.
  Note also that watch fns may be called from multiple threads
  simultaneously.  Keys must be unique per zref, and can be used to remove
  the watch with `remove-watch`, but are otherwise considered opaque
  by the watch mechanism."
  [z k f]
  {:pre [(instance? roomkey.zref.ZRef z) (fn? f)]}
  (.vAddWatch z k f))
