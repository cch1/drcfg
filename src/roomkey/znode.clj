(ns roomkey.znode
  "A facade for a Zookeeper znode"
  (:require [roomkey.zclient :as zclient]
            [clojure.string :as string]
            [clojure.set :as set]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as impl]
            [clojure.tools.logging :as log]))

;; TODO: Support (de)serialization to a vector of [value metadata]
(defn ^:dynamic *deserialize* [b] {:pre [(instance? (Class/forName "[B") b)]} (read-string (String. b "UTF-8")))

(defn ^:dynamic *serialize* [obj] {:post [(instance? (Class/forName "[B") %)]} (.getBytes (binding [*print-dup* true] (pr-str obj))))

(defn- process-stat
  "Process stat structure into useful data"
  [zdata]
  (-> zdata
      ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#sc_timeInZk
      (update-in [:stat :ctime] #(java.time.Instant/ofEpochMilli %))
      (update-in [:stat :mtime] #(java.time.Instant/ofEpochMilli %))))

(defn- deserialize-data
  "Process raw zdata into usefully deserialized types and structure"
  [{node ::node :as zdata}]
  (update zdata :data (fn [ba] (try (*deserialize* ba)
                                    (catch java.lang.RuntimeException e
                                      (log/warnf "Unable to deserialize znode data [%s]" (str node))
                                      ::unable-to-deserialize)
                                    (catch java.lang.AssertionError e
                                      (log/warnf "No data: %s [%s]" ba (str node))
                                      ::no-data)))))

(defn- normalize-datum [{:keys [stat data]}] {::type ::datum ::value data ::stat stat})

(defn- tap-to-atom
  [a f]
  (fn [rf]
    (fn
      ([] (rf))
      ([result] (rf result))
      ([result input]
       (reset! a (f input))
       (rf result input)))))

(defn- zdata-xform
  [stat-atom value-atom]
  (comp (map process-stat)
        (map deserialize-data)
        (map normalize-datum)
        (tap-to-atom value-atom ::value)
        (tap-to-atom stat-atom ::stat)))

;;; A proxy for a znode in a zookeeper cluster.
;;; * While offline (before the client connects) or online, a local tree can be created:
;;;   * A tree of nodes along with default (initial) values can be created locally.
;;;   * A root node must already exist before descendants can be created.
;;;   * Descendants of the root can be created in any order.  Placeholders will be created for undefined intervening nodes.
;;; * While online (after the client connects):
;;;   * A local (sub-) tree can be persisted to the cluster with only missing nodes (with default value) created.
;;;   * Ad hoc updates pushed from the server are streamed through the ZNode which proxies a clojure.core.async channel.
;;;     NB: Even if the initial/default value matches a pushed version 0 update from the server, it is considered an "update" and streamed.
;;;   * The znode can subsequently be updated synchronously, but with the resulting update processed asynchronously only upon arrival.
;;;   * The compare-and-set semantics are tightened to insist that updates can only apply to the current value AND current version.

(defprotocol BackedZNode
  "A Proxy for a ZooKeeper znode"
  (create [this] "Create the znode backing this virtual node")
  (watch [this] "Recursively watch the ZooKeeper znode and its children, recording updates to data and children in the returned channel.
  The channel can be closed to stop processing updates")
  (compareVersionAndSet [this version value] "Update the znode with the given value asserting the current version")
  (delete [this version] "Delete this znode, asserting the current version"))

(defprotocol VirtualNode
  "A value-bearing node in a tree"
  (update-or-create-child [this path value] "Update the existing child or create a new child of this node at the given path & with the given default value")
  (overlay [this v] "Overlay the existing placeholder node's value with a concrete value"))

(declare ->ZNode)

(defn default ; NB: This operation does not update the children of the parent
  ([client path] (default client path ::unknown))
  ([client path value]
   (let [events (async/chan (async/sliding-buffer 4))]
     (->ZNode client path (atom {:version -1 :cversion -1 :aversion -1}) (atom value) (ref #{}) events))))

(defn- process-children-changes
  "Atomically update the `children` reference from `parent` with the `paths` ensuring adds and deletes are processed exactly once"
  ;; NB The implied node changes have already been persisted (created and deleted) -here we manage the proxy data and associated processing
  ;; TODO: https://tech.grammarly.com/blog/building-etl-pipelines-with-clojure
  [parent children paths]
  (let [childs' (into #{} (comp (map (fn [segment] (as-> (str (.path parent) "/" segment) s
                                                     (if (.startsWith s "//") (.substring s 1) s))))
                                (map (fn [path] (default (.client parent) path)))) paths)]
    (dosync
     (let [childs @children
           child-adds (set/difference childs' childs)
           child-dels (set/difference childs childs')]
       (doseq [child child-adds] (alter children conj child))
       (doseq [child child-dels] (alter children disj child))
       [child-adds child-dels]))))

;; http://insideclojure.org/2016/03/16/collections/
;; http://spootnik.org/entries/2014/11/06/playing-with-clojure-core-interfaces/index.html
;; https://clojure.github.io/data.priority-map//index.html
;; https://github.com/clojure/data.priority-map/blob/master/src/main/clojure/clojure/data/priority_map.clj
;; https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/APersistentMap.java
;; (filter #(.isInterface %) (ancestors (class #{})))

(defmacro with-connection
  [channel & body]
  `(try (do ~@body)
        (catch clojure.lang.ExceptionInfo e#
          (if-let [type# (some-> (ex-data e#) ::zclient/type)]
            (do (log/infof "Unrecoverable client error (%s), shutting down watch" type#)
                (async/close! ~channel))
            (throw e#)))))

(deftype ZNode [client path stat value children events]
  VirtualNode
  (overlay [this v]
    ;; This should be safe, but it is a (Clojure) code smell.  It could possibly be avoided through rewriting of the ZNode tree.
    (swap! value (fn [old-v] (if (not= old-v v)
                               (do (assert (= old-v ::placeholder) "Can't overwrite existing child")
                                   v)
                               old-v)))
    this)
  (update-or-create-child [this path v]
    (let [z' (default client path v)]
      (get (conj! this z') z')))

  BackedZNode
  (create [this] ; Synchronously create the node @ version zero, if not already present
    (when (zclient/create-znode client path {:persistent? true :data (*serialize* @value)})
      (log/debugf "Created %s" (str this))
      true))
  (delete [this version]
    (zclient/delete client path version {})
    (log/debugf "Deleted %s" (str this))
    true)
  (watch [this]
    (log/debugf "Watching %s" (str this))
    (let [znode-events (async/chan 5)
          ec (async/chan 1 (map (fn tag [e] (assoc e ::node this))))
          data-events (async/chan (async/sliding-buffer 4) (zdata-xform stat value))
          delete-events (async/chan 1 (dedupe))  ; why won't a promise channel work here?
          children-events (async/chan 5 (tap-to-atom stat ::stat))
          exists-events (async/chan 1 (tap-to-atom stat ::stat))]
      (async/pipe ec events)
      (async/pipe data-events ec true)
      (async/pipe delete-events ec true)
      (async/pipe children-events ec true)
      (async/pipe exists-events ec true)
      ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkWatches
      ;; https://www.safaribooksonline.com/library/view/zookeeper/9781449361297/ch04.html
      (async/go-loop [cze {}]
        (if-let [{:keys [event-type keeper-state] :as event} (async/<! znode-events)]
          (do
            (log/debugf "Event [%s:%s] for %s" event-type keeper-state (str this))
            (recur
             (case event-type
               :None (do (assert (nil? (:path event)) "Keeper State event received with a path!") cze) ; should be handled by default watch on client
               :NodeCreated (do
                              (when keeper-state
                                (log/debugf "Node %s created" (str this))
                                (async/>! ec {::type ::created! ::value @value}))
                              (let [cze (reduce (fn [cze child]
                                                  (assert (nil? (cze child)) "Child wants to be watched twice!")
                                                  (let [c (watch child)]  ; watch must be in place before create to generate proper events
                                                    (with-connection c (create child))
                                                    (assoc cze child c)))
                                                cze @children)]
                                (async/>! znode-events {:event-type :NodeDataChanged})
                                (async/>! znode-events {:event-type :NodeChildrenChanged})
                                cze))
               :NodeDeleted (do (log/debugf "Node %s deleted" (str this)) ; This event is generated by exists, data and child watches.
                                (async/>! delete-events {::type ::deleted!})
                                (async/close! znode-events) ; shut down properly
                                cze)
               :DataWatchRemoved (do (log/debugf "Data watch on %s removed" (str this)) cze)
               :NodeDataChanged (do
                                  (async/thread
                                    (with-connection znode-events
                                      (async/>!! data-events (assoc (log/spy :trace (zclient/data client path
                                                                                                  {:watcher (partial async/put! znode-events)}))
                                                                    ::node this))))
                                  cze)
               :ChildWatchRemoved (do (log/debugf "Child watch on %s removed" (str this)) cze)
               :NodeChildrenChanged (with-connection znode-events
                                      (let [{:keys [stat paths]} (zclient/children client path {:watcher (partial async/put! znode-events)})
                                            [child-adds child-dels] (process-children-changes this children paths)]
                                        (when (or (seq child-adds) (seq child-dels))
                                          (async/>!! children-events {::type ::children-changed ::stat stat
                                                                      ::inserted child-adds ::removed child-dels}))
                                        (as-> cze cze
                                          (reduce (fn [cze child]
                                                    (log/debugf "Processed insert for %s" (str child))
                                                    (assoc cze child (watch child))) cze child-adds)
                                          (reduce (fn [cze child]
                                                    (log/debugf "Processed remove for %s" (str child))
                                                    (async/close! (cze child))
                                                    (dissoc cze child)) cze child-dels))))
               (do (log/warnf "Unexpected znode event:state [%s:%s] while watching %s" event-type keeper-state (str this))
                   cze))))
          (do (log/debugf "The event channel for %s closed; shutting down" (str this))
              (reduce (fn [cze [child c]]
                        (async/close! c)
                        (dissoc cze child))
                      cze
                      cze) ; idempotent
              (async/>! ec {::type ::watch-stop}))))
      (async/>!! ec {::type ::watch-start})
      (with-connection znode-events (when-let [stat (zclient/exists client path {:watcher (partial async/put! znode-events)})]
                                      (async/>!! exists-events {::type ::exists ::stat stat})
                                      (async/>!! znode-events {:event-type :NodeCreated})))
      znode-events))
  (compareVersionAndSet [this version value]
    (let [stat' (zclient/set-data client path (*serialize* value) version {})]
      (when stat'
        (let [{stat' :stat} (process-stat {:stat stat'})]
          (log/debugf "Set value for %s @ %s" (str this) version)
          (reset! stat stat')
          (async/put! events {::type ::set! ::value value ::version version ::node this ::stat stat'})))
      (boolean stat')))

  clojure.lang.Named
  (getName [this] (let [segments (string/split path #"/")] (or (last segments) "")))
  (getNamespace [this] (when-let [segments (seq (remove empty? (butlast (string/split path #"/"))))]
                         (str "/" (string/join "/" segments))))

  clojure.lang.ITransientCollection
  (conj [this znode]
    (dosync (if (contains? @children znode)
              (let [v @znode]
                (when (not= v ::placeholder) (overlay (@children znode) v)))
              (alter children conj znode)))
    this)

  clojure.lang.ITransientSet
  (disjoin [this v]
    (dosync (alter children disj v))
    this)
  (contains [this v]
    (contains? @children v))
  (get [this v] ; This is used by `core/get` when ILookup is not implemented
    (get @children v))

  clojure.lang.IFn
  (invoke [this path] (if-let [[_ head tail] (re-matches #"(/[^/]+)(.*)" path)]
                        (let [abs-path (as-> (str (.path this) head) s
                                         (if (.startsWith s "//") (.substring s 1) s))]
                          (when-let [child (some #(when (= (.path %) abs-path) %) @children)]
                            (child tail)))
                        this))
  (applyTo [this args]
    (let [n (clojure.lang.RT/boundedLength args 1)]
      (case n
        0 (throw (clojure.lang.ArityException. n (.. this (getClass) (getSimpleName))))
        1 (.invoke this (first args))
        (throw (clojure.lang.ArityException. n (.. this (getClass) (getSimpleName)))))))

  clojure.lang.IMeta
  (meta [this] @stat)

  clojure.lang.IDeref
  (deref [this] @value)

  clojure.lang.Seqable
  (seq [this] (seq @children))

  clojure.lang.Counted
  (count [this] (count @children))

  ;; https://stackoverflow.com/questions/26622511/clojure-value-equality-and-sets
  ;; https://japan-clojurians.github.io/clojure-site-ja/reference/data_structures#Collections
  clojure.lang.IHashEq
  (hasheq [this] (hash [(.path this) client]))

  impl/ReadPort
  (take! [this handler] (impl/take! events handler))

  impl/Channel
  (closed? [this] (impl/closed? events))
  (close! [this] (impl/close! events))

  java.lang.Object
  (equals [this other] (and (= (class this) (class other)) (= (.path this) (.path other)) (= (.client this) (.client other))))
  (hashCode [this] (.hashCode [path client]))
  (toString [this] (format "%s: %s" (.. this (getClass) (getSimpleName)) path)))

(defn compare-version-and-set!
  "Atomically sets the value of the znode `z` to `newval` if and only if the current
  version of `z` is identical to `current-version`. Returns true if set
  happened, else false"
  [z current-version newval]
  {:pre [(instance? roomkey.znode.ZNode z) (integer? current-version)]}
  (.compareVersionAndSet z current-version newval))

(defn add-descendant
  "Add a descendant ZNode to the given parent's (sub)tree `root` ZNode at the given (relative) `path` carrying the given `value`
  creating placeholder intermediate nodes as required."
  [parent path value]
  {:pre [(re-matches #"/.*" path)]}
  (if-let [[_ head tail] (re-matches #"(/[^/]+)(.*)" path)]
    (let [abs-path (as-> (str (.path parent) head) s
                     (if (.startsWith s "//") (.substring s 1) s))]
      (if (seq tail)
        (recur (update-or-create-child parent abs-path ::placeholder) tail value)
        (update-or-create-child parent abs-path value)))
    parent))

(defn- watch-client [root client]
  (let [client-events (async/tap client (async/chan 1))]
    (async/go-loop [znode-events nil] ; start event listener loop
      (if-let [[event client] (async/<! client-events)]
        (recur (case event
                 ::zclient/connected (let [znode-events (watch root)]
                                       (create root) ; root triggers blocking recursive actualization of tree
                                       znode-events)
                 ::zclient/closed (when znode-events (async/close! znode-events)) ; failed connections start but don't connect before closing?
                 znode-events))
        (log/infof "The client event channel for %s has closed, shutting down" (.path root))))))

(defn create-root
  "Create a root znode and watch for client status changes to manage watches"
  ([] (create-root "/"))
  ([abs-path] (create-root abs-path ::root))
  ([abs-path value] (create-root abs-path value (zclient/create)))
  ([abs-path value zclient]
   (let [segments (seq (string/split abs-path #"/"))
         events (async/chan (async/sliding-buffer 4))
         z (->ZNode zclient abs-path (atom {:version -1 :cversion -1 :aversion -1}) (atom value) (ref #{}) events)]
     (watch-client z zclient)
     z)))

(defn open
  [znode & args]
  (apply zclient/open (.client znode) args))

;; https://stackoverflow.com/questions/49373252/custom-pprint-for-defrecord-in-nested-structure
;; https://stackoverflow.com/questions/15179515/pretty-printing-a-record-using-a-custom-method-in-clojure
(remove-method clojure.core/print-method roomkey.znode.ZNode)
(defmethod clojure.core/print-method ZNode
  [znode ^java.io.Writer writer]
  (.write writer (format "#<%s %s>" (.. znode (getClass) (getSimpleName)) (.path znode))))

(remove-method clojure.pprint/simple-dispatch ZNode)
(defmethod clojure.pprint/simple-dispatch ZNode
  [znode]
  (pr znode))
