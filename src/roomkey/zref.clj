(ns roomkey.zref
  "A Zookeeper-based reference type"
  (:require [roomkey.znode :as znode]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]))

(def ^:dynamic *max-update-attempts* 10)

(defn- valid?
  [validator v]
  (or (not validator) (validator v)))

(defn- validate!
  [validator v]
  (when-not (valid? validator v)
    (throw (IllegalStateException. "Invalid reference state"))))

;;; A Reference type persisted in a zookeeper cluster.  The semantics are similar to a Clojure Atom
;;; with the following major differences:
;;;  * The read state (accessed via deref) may lag successful write operations (e.g. swap!)
;;;  * Read-only metadata is available which represents the zookeeper Stat data structure
;;;  * No updates are possible while disconnected
;;;  * The compare-and-set semantics are tightened to insist that updates can only apply to the
;;;    current value AND current version.
;;;  * The swap operation can fail if there is too much contention for a znode.
;;;  * Simple watcher functions are wrapped to ignore the version parameter applied to full-fledged watchers.
(defprotocol ZNodeWatching
  (start [this] "Start online operations")
  (update! [this version value] "Update the znode backing this zref")
  (path [this] "Return the path of the backing ZNode"))

(defprotocol VersionedReference
  (compareVersionAndSet [this current-version new-value] "Set to new-value only when current-version is latest")
  (vDeref [this] "Return referenced value and version")
  (vAddWatch [this k f] "Add versioned watcher that will be called with new value and version"))

(def data-xform
  (comp (filter (comp #{:roomkey.znode/datum} :roomkey.znode/type))
        (map (fn [{::znode/keys [value stat]}] (with-meta [value (:version stat)] stat)))))

(deftype ZRef [znode cache validator watches]
  ZNodeWatching
  (path [this] (.path znode))
  (start [this]
    (let [data (async/pipe znode (async/chan 1 data-xform))]
      (async/go-loop [] ; start event listener loop
        (if-let [[value' version' :as n] (async/<! data)]
          (do
            (log/debugf "Data element @ version %d received by %s" version' (str this))
            (let [[value version :as o] @cache
                  delta (- version' version)]
              (cond
                (neg? delta) (log/warnf "Received negative version delta [%d -> %d] for %s" version version' (str this))
                (zero? delta) (log/tracef "Received zero version delta [%d -> %d] for %s" version version' (str this))
                (and (> delta 1) (not (neg? version))) (log/infof "Received non-sequential version delta [%d -> %d] for %s" version version' (str this)))
              (if (valid? @validator value')
                (do (reset! cache n)
                    (async/thread (doseq [[k w] @watches]
                                    (try (w k this o n)
                                         (catch Exception e (log/errorf e "Error in watcher %s" k))))))
                (log/warnf "Watcher received invalid value [%s], ignoring update for %s" value' (str this))))
            (recur))
          (log/debugf "The znode for %s has closed, shutting down" (str this))))))
  (update! [this version value]
    (validate! @validator value)
    (let [r (znode/compare-version-and-set! znode version value)]
      (when r (log/debugf "Set value for %s to %s" (str this) value version))
      r))
  ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkWatches
  ;; https://www.safaribooksonline.com/library/view/zookeeper/9781449361297/ch04.html

  VersionedReference
  (compareVersionAndSet [this current-version newval]
    (update! this current-version newval))
  (vDeref [this] @cache)
  (vAddWatch [this k f] (swap! watches assoc k f) this)

  clojure.lang.IMeta
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
                                               (str this) *max-update-attempts* (* 2 n)))))
            (Thread/sleep n)
            (recur (* 2 n) (dec i)))))))
  (swap [this f x] (.swap this (fn [v] (f v x))))
  (swap [this f x y] (.swap this (fn [v] (f v x y))))
  (swap [this f x y args] (.swap this (fn [v] (apply f v x y args))))
  java.lang.Object
  (toString [this] (format "%s: %s [version %d]" (.getName (class this)) (.path znode) (last (.vDeref this)))))

(defn create
  [root-znode path default & options]
  (let [{validator :validator} (apply hash-map options)
        znode (znode/add-descendant root-znode path default)
        z (->ZRef znode (atom (with-meta [default -1] {:version -1}))
                  (atom nil) (atom {}))]
    (when validator (.setValidator z validator))
    (start z)
    z))

(def ^:deprecated zref create)

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

(defmethod clojure.core/print-method ZRef
  [zref ^java.io.Writer writer]
  (.write writer (format "#<ZRef\"%s\" Version %d>" (.path zref) (last (.vDeref zref)))))
