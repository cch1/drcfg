(ns roomkey.zref
  "A Zookeeper-based reference type"
  (:import [org.apache.zookeeper KeeperException KeeperException$Code
            KeeperException$SessionExpiredException
            KeeperException$ConnectionLossException])
  (:require [zookeeper :as zoo]
            [clojure.string :as string]
            [clojure.tools.logging :as log]))

;; https://github.com/liebke/zookeeper-clj
;; https://github.com/torsten/zookeeper-atom

(defn ^:dynamic *deserialize* [b] {:pre [(instance? (Class/forName "[B") b)]} (read-string (String. b "UTF-8")))

(defn ^:dynamic *serialize* [obj] {:post [(instance? (Class/forName "[B") %)]} (.getBytes (binding [*print-dup* true] (pr-str obj))))

(def ^:dynamic *max-update-attempts* 50)

(defn- validate!
  [validator v]
  (when validator
    (when-not (validator v)
      (throw (IllegalStateException. "Invalid reference state")))))

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
  (zConnect [this client] "Start online operations with the given client")
  (zDisconnect [this] "Suspend online operations")
  (zProcessUpdate [this new-zdata] "Process the zookeeper update and return this (the znode)"))

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
  (let [emessage "Not connected while processing ZooKeeper requests"]
    `(try (if-let [~'client (deref (.client ~'this))]
            ~@body
            (throw (ex-info ~emessage {:path (.path ~'this)})))
          (catch KeeperException$SessionExpiredException e#
            (.zDisconnect ~'this)
            (throw (ex-info ~emessage {:path (.path ~'this)} e#)))
          (catch KeeperException$ConnectionLossException e#
            (.zDisconnect ~'this)
            (throw (ex-info ~emessage {:path (.path ~'this)} e#))))))

(deftype ZRef [path client initialized? cache validator watches]
  UpdateableZNode
  (zConnect [this c]
    (reset! client c)
    (try (when (not @initialized?)
           (when (with-monitored-client (create-all client path)) ; idempotent side effects
             (log/debugf "Created node %s" path))
           (when (.compareVersionAndSet this 0 (.deref this)) ; idempotent side effects
             (log/infof "Updated node %s with default value" path))
           (reset! initialized? true))
         (.zProcessUpdate this {:path path :event-type ::Boot :keeper-state nil})
         this
         (catch Exception e nil)))
  (zDisconnect [this]
    (reset! client nil)
    this)
  ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkWatches
  ;; https://www.safaribooksonline.com/library/view/zookeeper/9781449361297/ch04.html
  (zProcessUpdate [this {:keys [event-type keeper-state] path' :path}]
    (log/tracef "Process update %s %s %s" path' event-type keeper-state)
    (case event-type
      :None (do (log/debugf "Keeper State event: %s (%s)" keeper-state path)  ; session changes
                (assert (nil? path') "Keeper State event received with a path"))
      :NodeDeleted (log/warnf "Node %s deleted" path)
      (:NodeDataChanged ::Boot)
      (try
        (with-monitored-client
          (when-let [new-z (update (zoo/data client path :watcher (fn [x] (.zProcessUpdate this x)))
                                   :data *deserialize*)]
            (let [f (juxt :data (comp :version :stat))
                  old-z (deref cache)
                  [old-d old-v :as o] (f old-z)
                  [new-d new-v :as n] (f new-z)]
              (validate! @validator new-d)
              (reset! cache new-z)
              (let [delta (- new-v old-v)]
                (cond
                  (neg? delta) (log/warnf "Received negative version delta [%d -> %d] for %s (%s %s)"
                                      old-v new-v path event-type keeper-state)
                  (zero? delta) (log/tracef "Received zero version delta [%d -> %d] for %s (%s %s)"
                                        old-v new-v path event-type keeper-state)
                  (and (> old-v 1) (> delta 1))
                  (log/infof "Received non-sequential version delta [%d -> %d] for %s (%s %s)"
                             old-v new-v path event-type keeper-state)))
              (future (doseq [[k w] @watches]
                        (try (w k this o n)
                             (catch Exception e (log/errorf e "Error in watcher %s" k))))))))
        (catch clojure.lang.ExceptionInfo e
          (log/infof "No connection while operating on %s" path)
          nil)
        (catch Exception e
          (log/errorf e "Error processing inbound update for %s [%s]" path event-type)
          nil))
      ;; default
      (log/warnf "Unexpected event:state [%s:%s] while watching %s" event-type keeper-state path))
    this)

  VersionedUpdate
  (compareVersionAndSet [this current-version newval]
    (validate! @validator newval)
    (with-monitored-client
      (boolean (try (let [r (zoo/set-data client path (*serialize* newval) current-version)]
                      (when r (log/tracef "Set value for %s to %s" path newval current-version))
                      r)
                    (catch KeeperException e
                      (when-not (= (.code e) KeeperException$Code/BADVERSION)
                        (throw e)))))))
  VersionedDeref
  (vDeref [this] ((juxt :data (comp :version :stat)) @cache))

  VersionedWatch
  (vAddWatch [this k f] (swap! watches assoc k f) this)

  clojure.lang.IMeta
  ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#sc_timeInZk
  (meta [this] (-> (.cache this) deref :stat))

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
  (swap [this f x y args] (.swap this (fn [v] (apply f v x y args)))))

(defn zref
  [path default & options]
  (let [{validator :validator} (apply hash-map options)
        z (->ZRef path (atom nil) (atom false) (atom {:data default :stat {:version -1}})
                 (atom nil) (atom {}))]
    (when validator (.setValidator z validator))
    z))

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
