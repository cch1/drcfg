(ns roomkey.zref
  "A Zookeeper-based reference type"
  (:import [org.apache.zookeeper KeeperException KeeperException$Code])
  (:require [zookeeper :as zoo]
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
  (zConnect [this client] "Using the given client, enable updates and start the watcher")
  (zDisconnect [this] "Disassociate the client and disable updates")
  (zProcessUpdate [this new-zdata] "Process the zookeeper update and return this (the znode)"))

(defprotocol VersionedUpdate
  (compareVersionAndSet [this current-version new-value] "Set to new-value only when current-version is latest"))

(defprotocol VersionedDeref
 (vDeref [this] "Return referenced value and version"))

(defprotocol VersionedWatch
  "A protocol for adding versioned watchers using the same associative storage as \"classic\" watchers"
 (vAddWatch [this k f] "Add versioned watcher that will be called with new value and version"))

(deftype ZRef [client path cache validator watches]
  UpdateableZNode
  (zConnect [this c]
    (reset! client c)
    (let [{v :version l :dataLength :as stat} (zoo/exists c path)]
      (when (or (nil? stat) (and (zero? l) (zero? v)))
        (let [d (.deref this)]
          (if stat
            (do
              (log/infof "Updating degenerate node %s with default value" path)
              (assert (.compareVersionAndSet this v d) "Can't update degenerate node"))
            (do
              (log/debugf "Node %s does not exist, creating it and assigning default value" path)
              (assert (zoo/create-all c path :data (-> d *serialize*) :persistent? true)
                      (format "Can't create node %s" path)))))))
    (.zProcessUpdate this {:path path :event-type ::boot}))
  (zDisconnect [this] (reset! client nil))
  ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkWatches
  (zProcessUpdate [this {:keys [event-type keeper-state] path' :path}]
    (log/debugf "Change %s %s %s" path' event-type keeper-state)
    (case [event-type keeper-state]
      [:None :SyncConnected] (log/debugf "Connected (%s)" path)
      [:None :Disconnected] (log/debugf "Disconnected (%s)" path)
      [:None :Expired] (log/debugf "Expired (%s)" path)
      [:NodeDeleted :SyncConnected] (log/warnf "Node %s deleted" path)
      ([::boot nil] [:NodeDataChanged :SyncConnected]) ; two cases, identical behavior
      (do
        (assert (= path path') (format "ZNode at path %s got event (%s %s %s)" path path' event-type keeper-state))
        (when-let [c @client]
          (try
            (let [f (juxt :data (comp :version :stat))
                  new-z (update (zoo/data c path :watcher (fn [x] (.zProcessUpdate this x)))
                                :data *deserialize*) ; memfn?
                  old-z (deref cache)
                  [old-d old-v :as o] (f old-z)
                  [new-d new-v :as n] (f new-z)]
              (validate! @validator new-d)
              (reset! cache new-z)
              (let [delta (- new-v old-v)]
                (cond
                  (neg? delta) (log/warnf "Received negative version delta [%d -> %d] for %s (%s %s)"
                                      old-v new-v path event-type keeper-state)
                  (zero? delta) (log/infof "Received zero version delta [%d -> %d] for %s (%s %s)"
                                       old-v new-v path event-type keeper-state)
                  (and (pos? old-v) (> delta 1))
                  (log/infof "Received non-sequential version delta [%d -> %d] for %s (%s %s)"
                             old-v new-v path event-type keeper-state)))
              (future (doseq [[k w] @watches] (try (w k this o n)
                                                   (catch Exception e (log/errorf e "Error in watcher %s" k))))))
            (catch Exception e
              (log/errorf e "Error processing inbound update from %s [%s]" path keeper-state)))))
      ;; default
      (log/warnf "Unexpected event:state [%s:%s] while watching %s" event-type keeper-state path))
    this)

  VersionedUpdate
  (compareVersionAndSet [this current-version newval]
    (when-not @client (throw (RuntimeException. "Not connected")))
    (validate! @validator newval)
    (boolean (try (zoo/set-data @client path (*serialize* newval) current-version)
                  (catch KeeperException e
                    (when-not (= (.code e) KeeperException$Code/BADVERSION)
                      (throw e))))))
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
        z (->ZRef (atom nil) path (atom {:data default :stat {:version -1}}) (atom nil) (atom {}))]
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

(defn client
  [cstr]
  (let [client (zoo/connect cstr)]
    (try
      (zoo/create client "/" :persistent? true)
      (catch org.apache.zookeeper.KeeperException$NodeExistsException e
        (log/debugf "Root node exists at %s" cstr))
      (catch org.apache.zookeeper.KeeperException$NoNodeException e
        (log/warnf "Can't create root node for connect string %s -does our parent node exist?" cstr)
        (throw (ex-info "The drcfg root node could not be created" {:connect-string cstr} e))))
    client))

(defn connect
  [client z]
  (.zConnect z client))

(defn disconnect
  [z]
  (.zDisconnect z))

(defn connected?
  [z]
  (boolean (when-let [c @(.client z)] (.. c getState isConnected))))

(defn path [z] (.path z))
