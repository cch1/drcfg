(ns roomkey.zref
  "Dynamic Distributed Run-Time configuration"
  (:import [org.apache.zookeeper KeeperException KeeperException$Code])
  (:require [zookeeper :as zoo]
            [clojure.string :as string]
            [clojure.tools.logging :as log]))

;; https://github.com/liebke/zookeeper-clj
;; https://github.com/torsten/zookeeper-atom

(defn- deserialize [b] (read-string (String. b "UTF-8")))

(defn- serialize [obj] (.getBytes (binding [*print-dup* true] (pr-str obj))))

(defn- validate!
  [validator v]
  (when validator
    (when-not (validator v)
      (throw (IllegalStateException. "Invalid reference state")))))

;;; A Reference type persisted in a zookeeper cluster.  The semantics are similar to a Clojure Atom
;;; with the following major differences:
;;;  * The read state (accessed via deref) may lag successful write operations (e.g. swap!)
;;;  * Read-only metadata is available which represents the zookeeper Stat data structure
;;;  * The compare-and-set semantics are tightened to insist that updates can only apply to the
;;;    current value AND current version.
;;;  * The swap operation can fail if there is too much contention for a znode.

(defprotocol VersionedUpdate
  (compareVersionAndSet [this current-version new-value]))

(defprotocol UpdateableZNode
  (zConnect [this client] "Using the given client, enable updates and start the watcher")
  (zDisconnect [this] "Disassociate the client and disable updates")
  (zProcessUpdate [this new-zdata] "Process the zookeeper update and return this (the znode)"))

(deftype ZRef [client path cache validator watches]
  UpdateableZNode
  (zConnect [this c]
    (if (zoo/exists c path)
      (log/debugf "Node %s exists")
      (do
        (log/debugf "Node %s does not exist, creating it and assigning default value" path)
        (assert (zoo/create-all c path :data (-> cache deref :data) :persistent? true)
                (format "Can't create node %s" path))))
    (reset! client c)
    (.zProcessUpdate this {:path path :event-type ::boot}))
  (zDisconnect [this] (reset! client nil))
  (zProcessUpdate [this {:keys [event-type keeper-state] path' :path}]
    (log/debugf "Change %s %s %s" path' event-type keeper-state)
    (assert (= path path') (format "Got event for wrong path: %s : %s" path path'))
    (case [event-type keeper-state]
      [:NodeDeleted :SyncConnected]
      (log/infof "Node %s deleted" path)
      ([::boot nil] [:NodeDataChanged :SyncConnected]) ; two cases, identical behavior
      (when @client
        (try (let [new-z (zoo/data @client path :watcher (fn [x] (.zProcessUpdate this x))) ; memfn?
                   old-z (deref cache)
                   new-d (-> new-z :data deserialize)
                   old-d (-> old-z :data deserialize)
                   new-v (-> new-z :stat :version)
                   old-v (-> old-z :stat :version)]
               (reset! cache new-z)
               (when (and (pos? old-v) (not= 1 (- new-v old-v)))
                 (log/warnf "Received non-sequential version [%d -> %d] for %s (%s %s)"
                            old-v new-v path event-type keeper-state))
               (doseq [[k w] @watches] (try (w k this old-d new-d)
                                            (catch Exception e (log/errorf e "Error in watcher %s" k)))))
             (catch Exception e
               (log/errorf e "Error processing inbound update from %s [%s]" path keeper-state))))
      ;; default
      (log/warnf "Unexpected event:state [%s:%s] while watching %s" event-type keeper-state path))
    this)
  clojure.lang.IDeref
  (deref [this] (-> cache deref :data deserialize))
  clojure.lang.IMeta
  ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#sc_timeInZk
  (meta [this] (-> cache deref :stat))
  ;; Observe Interface
  clojure.lang.IRef
  ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkWatches
  (setValidator [this f]
    (validate! f (.deref this))
    (reset! validator f)
    this)
  (getValidator [this] @validator)
  (getWatches [this] @watches)
  (addWatch [this k f] (swap! watches assoc k f) this)
  (removeWatch [this k] (swap! watches dissoc k) this)
  ;; Write interface
  VersionedUpdate
  (compareVersionAndSet [this current-version newval]
    (when-not @client (throw (RuntimeException. "Not connected")))
    (boolean (try (zoo/set-data @client path (serialize newval) current-version)
                  (catch KeeperException e
                    (when-not (= (.code e) KeeperException$Code/BADVERSION)
                      (throw e))))))
  clojure.lang.IAtom
  (reset [this value] (.swap this (constantly value)))
  (compareAndSet [this oldval newval]
    (let [current @cache
          version (-> current :stat :version)]
      (validate! @validator newval)
      (boolean (and (= oldval (-> current :data deserialize))
                    (.compareVersionAndSet this version newval)))))
  (swap [this f]
    (loop [i 5]
      (assert (pos? i) (format "Too many failures updating %s" path))
      (let [current @cache
            value (-> current :data deserialize f)
            version (-> current :stat :version)]
        (validate! @validator value)
        (if (.compareVersionAndSet this version value) value (recur (dec i))))))
  (swap [this f x] (.swap this (fn [v] (f v x))))
  (swap [this f x y] (.swap this (fn [v] (f v x y))))
  (swap [this f x y args] (.swap this (fn [v] (apply f v x y args)))))

(defn zref
  [path default & options]
  (let [{validator :validator} (apply hash-map options)
        z (ZRef. (atom nil) path (atom {:data (serialize default) :stat {:version -1}})
                 (atom validator) (atom {}))]
    (validate! validator default)
    z))

(defn client
  [cstr]
  (let [client (zoo/connect cstr)]
    (try
      (zoo/create client "/" :persistent? true)
      (catch org.apache.zookeeper.KeeperException$NodeExistsException e
        (log/infof "Root node exists at %s" cstr)))
    client))

(defn connect
  [client z]
  (.zConnect z client))
