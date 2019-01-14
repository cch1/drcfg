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
  [zdata]
  (update zdata :data (fn [ba] (try (*deserialize* ba)
                                    (catch java.lang.RuntimeException e
                                      (log/warnf "Unable to deserialize znode data")
                                      ::unable-to-deserialize)
                                    (catch java.lang.AssertionError e
                                      (log/warnf "No data: %s" ba)
                                      ::no-data)))))

(defn- normalize-datum [{:keys [stat data]}] {::type ::datum ::value data ::stat stat})

(def zdata-xform
  (comp (map process-stat)
        (map deserialize-data)
        (map normalize-datum)))

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
  (watch [this znode-events] "Recursively watch the ZooKeeper znode and its children, processing updates to data and children into the given channel.
  The channel can be closed to stop processing updates")
  (compareVersionAndSet [this version value] "Update the znode with the given value asserting the current version")
  (delete [this version] "Delete this znode, asserting the current version"))

(defprotocol VirtualNode
  "A value-bearing node in a tree"
  (path [this] "Return the string path of this znode")
  (create-child [this name value] "Create a child of this node with the given name, default value and children")
  (update-or-create-child [this name value] "Update the existing child node or create a new child of this node with the given name and default value")
  (overlay [this v] "Overlay the existing placeholder node's value with a concrete value")
  (children [this] "Return the immediate children of this node"))

(defn- process-child-add
  [[z c :as a] events]
  (async/>!! events {::type ::child-added ::node z})
  (log/debugf "Processed add for %s" (str z))
  (watch z c)
  a)

(defn- process-child-del
  [[z c :as a] events]
  (async/>!! events {::type ::child-deleted ::node z})
  (log/debugf "Processed delete for %s" (str z))
  (async/close! c)
  a)

(defn- process-children-changes
  "Atomically update the `children` reference from `parent` with the `paths` ensuring adds and deletes are processed exactly once"
  ;; NB The implied node changes have already been persisted (created and deleted) -here we manage the proxy data and associated processing
  [parent children paths events]
  (let [childs' (into #{} (comp (map (fn [p] (last (string/split p #"/"))))
                                (map (fn [n] (create-child parent n ::unknown)))) paths)]
    (dosync
     (let [childs (into #{} (keys @children))
           child-adds (set/difference childs' childs)
           child-dels (set/difference childs childs')]
       (doseq [child child-adds] (let [c (async/chan 1)] ; This is idempotent, modulo garbage collection
                                   (alter children assoc child c)
                                   (send (agent [child c]) process-child-add events)))
       (doseq [child child-dels] (let [c (@children child)]
                                   (alter children dissoc child)
                                   (send (agent [child c]) process-child-del events)))))))

;; http://insideclojure.org/2016/03/16/collections/
;; http://spootnik.org/entries/2014/11/06/playing-with-clojure-core-interfaces/index.html
;; https://github.com/clojure/data.priority-map/blob/master/src/main/clojure/clojure/data/priority_map.clj

(deftype ZNode [client parent name ^:volatile-mutable initial-value children events]
  VirtualNode
  (path [this]
    (let [p (str (when parent (.path parent)) "/" name)]
      (if (.startsWith p "//") (.substring p 1) p)))
  (overlay [this v]
    (assert (= initial-value ::placeholder) "Can't overwrite existing child")
    ;; This should be safe, but it is a (Clojure) code smell.  It could possibly be avoided through rewriting of the ZNode tree.
    (set! initial-value v)
    this)
  (create-child [this n v] ; NB: This operation sets the parent of the child, but does not update the children of the parent
    (let [events (async/chan (async/sliding-buffer 4))]
      (ZNode. client this n v (ref {}) events)))
  (update-or-create-child [this n v]
    (if-let [existing (get this n)]
      (if (= v ::placeholder) existing (overlay existing v))
      (let [child (create-child this n v)]
        (dosync (alter children assoc child (async/chan 1)))
        child)))
  (children [this] (keys @children))
  BackedZNode
  (create [this] ; Synchronously create the node @ version zero, if not already present
    (when (zclient/create-znode client (path this) {:persistent? true :data (*serialize* initial-value)})
      (log/debugf "Created %s" (str this))
      true))
  (delete [this version]
    (zclient/delete client (path this) version {})
    (log/debugf "Deleted %s" (str this))
    true)
  (watch [this znode-events]
    (log/debugf "Watching %s" (str this))
    (let [data-events (async/chan (async/sliding-buffer 4) zdata-xform)
          delete-events (async/chan 1 (dedupe))] ; why won't a promise channel work here?
      (async/pipe data-events events)
      (async/pipe delete-events events)
      ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkWatches
      ;; https://www.safaribooksonline.com/library/view/zookeeper/9781449361297/ch04.html
      (async/go-loop []
        (if-let [{:keys [event-type keeper-state] :as event} (async/<! znode-events)]
          (do
            (log/debugf "Event [%s:%s] for %s" event-type keeper-state (str this))
            (case event-type
              :None (do (assert (nil? (:path event)) "Keeper State event received with a path!") ; should be handled by default watch on client
                        (recur))
              :NodeCreated (do (log/debugf "Node %s created" (str this))
                               (when keeper-state (async/>!! events {::type ::created! ::value initial-value}))
                               (doseq [[z c] @children] (watch z c) (create z)) ; watch must be in place before create
                               (async/put! znode-events {:event-type :NodeDataChanged})
                               (async/put! znode-events {:event-type :NodeChildrenChanged})
                               (recur))
              :NodeDeleted (do (log/debugf "Node %s deleted" (str this)) ; This event is generated by both data and child watches.
                               (async/>!! delete-events {::type ::deleted!})
                               (async/close! znode-events) ; shut down properly
                               (recur))
              :DataWatchRemoved (log/debugf "Data watch on %s removed" (str this))
              :NodeDataChanged (do
                                 (async/thread
                                   (async/>!! data-events (log/spy :trace (zclient/data client (path this) {:watcher (partial async/put! znode-events)}))))
                                 (recur))
              :ChildWatchRemoved (log/debugf "Child watch on %s removed" (str this))
              :NodeChildrenChanged (let [{:keys [paths stat]} (zclient/children client (path this) {:watcher (partial async/put! znode-events)})]
                                     (process-children-changes this children paths events)
                                     (recur))
              (log/warnf "Unexpected znode event:state [%s:%s] while watching %s" event-type keeper-state (str this))))
          (do (log/debugf "The event channel for %s closed; shutting down" (str this))
              (dosync (doseq [[child c] @children] (alter children update child (fn [c] (async/close! c) (async/chan 1))))) ; idempotent
              (async/>!! events {::type ::watch-stop}))))
      (async/put! events {::type ::watch-start}
                  (fn [_] (when (zclient/exists client (path this) {:watcher (partial async/put! znode-events)})
                            (async/put! znode-events {:event-type :NodeCreated}))))))
  (compareVersionAndSet [this version value]
    (let [r (zclient/set-data client (path this) (*serialize* value) version {})]
      (when r
        (log/debugf "Set value for %s @ %s" (str this) version)
        (async/put! events {::type ::set! ::value value ::version version}))
      r))

  clojure.lang.Named
  (getName [this] name)
  (getNamespace [this] (when parent (path parent)))

  clojure.lang.ILookup
  (valAt [this item] (some #(let [z (key %)] (when (= (.name z) item) z)) @children))
  (valAt [this item not-found] (or (.valAt this item) not-found))

  ;; https://stackoverflow.com/questions/26622511/clojure-value-equality-and-sets
  ;; https://japan-clojurians.github.io/clojure-site-ja/reference/data_structures#Collections
  clojure.lang.IHashEq
  (hasheq [this] (hash [(path this) client]))

  impl/ReadPort
  (take! [this handler] (impl/take! events handler))

  impl/Channel
  (closed? [this] (impl/closed? events))
  (close! [this] (impl/close! events))

  java.lang.Object
  (equals [this other] (and (= (path this) (path other)) (= (.client this) (.client other))))
  (hashCode [this] (.hashCode [(path this) client]))
  (toString [this] (format "%s: %s" (.getName (class this)) (path this))))

(defn compare-version-and-set!
  "Atomically sets the value of the znode `z` to `newval` if and only if the current
  version of `z` is identical to `current-version`. Returns true if set
  happened, else false"
  [z current-version newval]
  {:pre [(instance? roomkey.znode.ZNode z) (integer? current-version)]}
  (.compareVersionAndSet z current-version newval))

(defn add-descendant
  "Add a descendant ZNode to the given (sub)tree's `root` ZNode at the given (relative) `path` carrying the given `value`
  creating placeholder intermediate nodes as required."
  [root path value]
  (loop [parent root [segment & segments] (rest (string/split path #"/"))]
    (if (seq segments)
      (recur (update-or-create-child parent segment ::placeholder) segments)
      (update-or-create-child parent segment value))))

(defn- watch-client [root client]
  (let [client-events (async/tap client (async/chan 1))]
    (async/go-loop [znode-events nil] ; start event listener loop
      (if-let [[event client] (async/<! client-events)]
        (recur (case event
                 ::zclient/connected (let [znode-events (async/chan 1)]
                                       (watch root znode-events)
                                       (create root) ; root triggers blocking recursive actualization of tree
                                       znode-events)
                 ::zclient/closed (when znode-events (async/close! znode-events)) ; failed connections start but don't connect before closing?
                 znode-events))
        (log/infof "The client event channel for %s has closed, shutting down" (path root))))))

(defn create-root
  "Create a root znode and watch for client status changes to manage watches"
  ([zclient] (create-root zclient ::root))
  ([zclient initial-value]
   (let [events (async/chan (async/sliding-buffer 4))
         z (->ZNode zclient nil "" initial-value (ref {}) events)]
     (watch-client z zclient)
     z)))
