(ns roomkey.znode
  "A facade for a Zookeeper znode"
  (:require [roomkey.zclient :as zclient]
            [clojure.string :as string]
            [clojure.set :as set]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as impl]
            [clojure.tools.logging :as log]))

(defn ^:dynamic *deserialize* [b] {:pre [(instance? (Class/forName "[B") b)]} (read-string (String. b "UTF-8")))

(defn ^:dynamic *serialize* [obj] {:post [(instance? (Class/forName "[B") %)]} (.getBytes (binding [*print-dup* true] (pr-str obj))))

;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#sc_timeInZk
(defn- process-zdata
  "Process raw zdata into usefully deserialized types and structure"
  [zdata]
  (let [m (-> (:stat zdata)
              (update :ctime #(java.time.Instant/ofEpochMilli %))
              (update :mtime #(java.time.Instant/ofEpochMilli %)))
        obj ((juxt (comp *deserialize* :data) (comp :version :stat)) zdata)]
    (with-meta obj m)))

(defn- zdata-xform
  [v]
  (comp (map process-zdata)))

;;; A proxy for a znode in a zookeeper cluster.
;;; * While offline (before the client connects) or online, a local tree can be created:
;;;   * A tree of nodes along with default (initial) values can be created locally.
;;;   * A root node must already exist before descendants can be created.
;;;   * Descendants of the root can be created in any order.  Placeholders will be created for undefined intervening nodes.
;;; * While online (after the client connects):
;;;   * A local (sub-) tree can be actualized, or persisted, to the cluster with only missing nodes (with default value) created.
;;;   * Ad hoc updates pushed from the server are streamed through the ZNode which proxies a clojure.core.async channel.
;;;     NB: if the initial/default value matches a pushed version 0 update from the server, it is not considered an "update" and is not streamed.
;;;   * The znode can subsequently be updated synchronously, but with the resulting update processed asynchronously only upon arrival.
;;;   * The compare-and-set semantics are tightened to insist that updates can only apply to the
;;;     current value AND current version.

(defprotocol BackedZNode
  "A Proxy for a ZooKeeper znode"
  (actualize [this] "Recursively create the znode backing this virtual node and its children")
  (open [this] "Recursively watch the ZooKeeper znode and its children, processing updates to data and children.
  Returns a channel that can be closed to stop processing updates")
  (compareVersionAndSet [this version value] "Update the znode with the given value asserting the current version"))

(defprotocol VirtualNode
  "A value-bearing node in a tree"
  (path [this] "Return the string path of this znode")
  (create-child [this name value] "Create a child of this node with the given name, default value and children")
  (update-or-create-child [this name value] "Update the existing child node or create a new child of this node with the given name and default value")
  (overlay [this v] "Overlay the existing placeholder node's value with a concrete value")
  (children [this] "Return the immediate children of this node"))

;; http://insideclojure.org/2016/03/16/collections/
;; http://spootnik.org/entries/2014/11/06/playing-with-clojure-core-interfaces/index.html
;; https://github.com/clojure/data.priority-map/blob/master/src/main/clojure/clojure/data/priority_map.clj

(deftype ZNode [client parent name ^:volatile-mutable initial-value children data-events]
  VirtualNode
  (path [this]
    (let [p (str (when parent (.path parent)) "/" name)]
      (if (.startsWith p "//") (.substring p 1) p)))
  (overlay [this v]
    (assert (= initial-value ::placeholder) "Can't overwrite existing child")
    ;; This should be safe, but it is a (Clojure) code smell.  It could possibly be avoided through rewriting of the ZNode tree.
    (set! initial-value v)
    this)
  (create-child [this n v]
    (let [data-events (async/chan (async/sliding-buffer 2) (zdata-xform v))]
      (ZNode. client this n v (atom #{}) data-events)))
  (update-or-create-child [this n v]
    (if-let [existing (get this n)]
      (if (= v ::placeholder) existing (overlay existing v))
      (let [c (create-child this n v)]
        (swap! children conj c)
        c)))
  BackedZNode
  (actualize [this] ; Recursively create the node and its children, each @ version zero, if not already present
    (when (zclient/create-znode client (path this) {:persistent? true :data (*serialize* initial-value)})
      (log/infof "Actualized %s" (str this)))
    (doseq [child @children] (actualize child)))
  (open [this]
    (log/debugf "Opening %s" (str this))
    (let [znode-events (async/chan 1)]
      (async/go-loop [cze (reduce (fn [cze z] (assoc cze z (.open z))) {} @children)] ; start event listener loop
        (if-let [{:keys [event-type keeper-state] :as event} (async/<! znode-events)]
          (do
            (log/debugf "Event [%s:%s] for %s" event-type keeper-state (str this))
            (case event-type
              :None (do (assert (nil? (:path event)) "Keeper State event received with a path!") ; should be handled by default watch on client
                        (recur cze))
              :NodeDeleted (log/warnf "Node %s deleted" (str this))
              :DataWatchRemoved (log/infof "Data watch on %s removed" (str this))
              :NodeDataChanged (do
                                 (async/>! data-events (zclient/data client (path this) {:watcher (partial async/put! znode-events)}))
                                 (recur cze))
              :NodeChildrenChanged (let [segments' (into #{}
                                                         (map (fn [p] (last (string/split p #"/"))))
                                                         (zclient/children client (path this) {:watcher (partial async/put! znode-events)}))
                                         segments (into #{} (map #(.name %)) @children)
                                         zadds (into #{} (map (fn [segment] (create-child this segment ::unknown)))
                                                     (set/difference segments' segments))
                                         zdeletes (into #{} (remove #(segments' (.name %))) @children)
                                         ;; TODO: children as a ref + agent send to manage adds/deletes
                                         cs (reset! children (set/union
                                                              (set/difference @children zdeletes)
                                                              zadds))]
                                     (when (not= segments segments') (log/debugf "Children of %s changed %s => %s" (str this) segments segments'))
                                     (recur (as-> cze %
                                              (reduce (fn [cze z] (assoc cze z (.open z))) % zadds)
                                              (reduce (fn [cze z] (async/close! (cze z)) (dissoc cze z)) % zdeletes))))
              (log/warnf "Unexpected znode event:state [%s:%s] while watching %s" event-type keeper-state (str this))))
          (do (log/debugf "The event channel for %s closed; shutting down" (str this))
              (doseq [[z c] cze] (async/close! c)))))
      (async/put! znode-events {:event-type :NodeDataChanged})
      (async/put! znode-events {:event-type :NodeChildrenChanged})
      znode-events))
  (compareVersionAndSet [this version value]
    (let [r (zclient/set-data client (path this) (*serialize* value) version {})]
      (when r (log/debugf "Set value for %s @ %s" (str this) version))
      r))
  ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkWatches
  ;; https://www.safaribooksonline.com/library/view/zookeeper/9781449361297/ch04.html

  clojure.lang.ILookup
  (valAt [this item] (some (fn [child] (when (= item (.name child)) child)) @children))
  (valAt [this item not-found] (or (.valAt this item) not-found))

  ;; https://stackoverflow.com/questions/26622511/clojure-value-equality-and-sets
  ;; https://japan-clojurians.github.io/clojure-site-ja/reference/data_structures#Collections
  clojure.lang.IHashEq
  (hasheq [this] (hash (path this)))

  impl/ReadPort
  (take! [this handler] (impl/take! data-events handler))

  impl/Channel
  (closed? [this] (impl/closed? data-events))
  (close! [this] (impl/close! data-events))

  java.lang.Object
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

(defn- watch-client [this client]
  (let [client-events (async/tap client (async/chan 1))]
    (async/go-loop [znode-events nil] ; start event listener loop
      (if-let [[event client] (async/<! client-events)]
        (recur (case event
                 ::zclient/connected (do
                                       (actualize this) ; root triggers blocking recursive actualization of tree
                                       (open this))
                 ::zclient/closed (async/close! znode-events)
                 znode-events))
        (log/infof "The client event channel for %s has closed, shutting down" (path this))))))

(defn create-root
  "Create a root znode and watch for client status changes to manage watches"
  ([zclient] (create-root zclient ::root))
  ([zclient initial-value]
   (let [data-events (async/chan (async/sliding-buffer 2) (zdata-xform initial-value))
         z (->ZNode zclient nil "" initial-value (atom #{}) data-events)]
     (watch-client z zclient)
     z)))
