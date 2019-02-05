(ns roomkey.znode
  "A facade for a Zookeeper znode"
  (:require [roomkey.zclient :as zclient]
            [clojure.string :as string]
            [clojure.set :as set]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as impl]
            [clojure.tools.logging :as log])
  (:import (java.time Instant OffsetDateTime)))

;; TODO: Support (de)serialization to a vector of [value metadata]
(defn ^:dynamic *deserialize* [b] {:pre [(instance? (Class/forName "[B") b)]} (read-string (String. b "UTF-8")))

(defn ^:dynamic *serialize* [obj] {:post [(instance? (Class/forName "[B") %)]} (.getBytes (binding [*print-dup* true] (pr-str obj))))

(defn- normalize-datum [{:keys [stat data]}] {::type ::datum ::value data ::stat stat})

(defn- process-stat*
  "Process stat structure into useful data"
  ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#sc_timeInZk
  [stat]
  (-> stat
      (update :ctime #(Instant/ofEpochMilli %))
      (update :mtime #(Instant/ofEpochMilli %))))

(defn- process-stat [zdata] (update zdata ::stat process-stat*))

(defn- deserialize-data
  "Process raw zdata into usefully deserialized types and structure"
  [node zdata]
  (update zdata ::value (fn [ba] (try (*deserialize* ba)
                                      (catch java.lang.RuntimeException e
                                        (log/warnf "Unable to deserialize znode data [%s]" (str node))
                                        ::unable-to-deserialize)
                                      (catch java.lang.AssertionError e
                                        (log/warnf "No data: %s [%s]" ba (str node))
                                        ::no-data)))))

(let [v1-epoch (Instant/parse "2019-01-01T00:00:00.00Z")]
  (defn- decode-datum [{{mtime :mtime :as stat} ::stat value ::value :as zdata}]
    (if (and (.isBefore v1-epoch mtime) (sequential? value))
      (update zdata ::value (fn [[value metadata]]
                              (if (instance? clojure.lang.IMeta value) (with-meta value metadata) value)))
      zdata)))

(defn- tap-children
  [node]
  (let [stat-ref (.stat node)
        children-ref (.children node)]
    (fn [rf]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result {:keys [roomkey.znode/inserted roomkey.znode/removed roomkey.znode/stat] :as input}]
         (dosync
          (ref-set stat-ref stat)
          (apply alter children-ref conj inserted)
          (apply alter children-ref disj removed))
         (rf result input))))))

(defn- tap-datum
  [node]
  (let [stat-ref (.stat node)
        value-ref (.value node)]
    (fn [rf]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result {:keys [roomkey.znode/value roomkey.znode/stat] :as input}]
         (dosync
          (ref-set stat-ref stat)
          (ref-set value-ref  value))
         (rf result input))))))

(defn- tap-stat
  [node]
  (let [stat-ref (.stat node)]
    (fn [rf]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result {:keys [roomkey.znode/stat] :as input}]
         (dosync
          (ref-set stat-ref stat))
         (rf result input))))))

(defn- zdata-xform
  [znode]
  (comp (map normalize-datum)
        (map process-stat)
        (map (partial deserialize-data znode))
        (map decode-datum)
        (tap-datum znode)))

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
  (create! [this] "Create the znode backing this virtual node")
  (delete! [this version] "Delete this znode, asserting the current version")
  (compare-version-and-set! [this current-version new-value]   "Atomically set the value of this znode to `new-value` if and only if the current
  version is identical to `current-version`. Returns true if set happened, else false")
  (watch [this] "Recursively watch the znode and its children, returning a WatchManager that can be closed to cease watching, read from to
 get the results of watching and, as seq'd, to obtain the WatchManagers of its children")
  (actualize! [this wmgr] "Recursively persist this ZNode, informing the watch manager of relevant events")
  (signature [this] "Return a (Clojure) hash equivalent to a signature of the state of the subtree at this ZNode"))

(defprotocol VirtualNode
  "A value-bearing node in a tree"
  (update-or-add-child [this path value] "Update the existing child or create a new child of this node at the given path & with the given default value")
  (overlay [this v] "Overlay the existing placeholder node's value with a concrete value"))

(defprotocol Closeable
  (close [this] "Close the resource"))

(declare ->ZNode)

(defn default ; NB: This operation does not update the children of the parent
  ([client path] (default client path ::unknown))
  ([client path value]
   (let [events (async/chan (async/sliding-buffer 4))]
     (->ZNode client path (ref {:version -1 :cversion -1 :aversion -1}) (ref value) (ref #{}) events))))

(defn- process-children-changes
  "Atomically update the `children` reference from `parent` with the `paths` ensuring adds and deletes are processed exactly once"
  ;; NB The implied node changes have already been persisted (created and deleted) -here we manage the proxy data and associated processing
  ;; TODO: https://tech.grammarly.com/blog/building-etl-pipelines-with-clojure
  [parent childs paths]
  (let [path-prefix (as-> (str (.path parent) "/") s (if (= s "//") "/" s))
        childs' (into #{} (comp (map (fn [segment] (str path-prefix segment)))
                                (map (fn [path] (or (some #(when (= path (.path %)) %) @(.children parent))
                                                    (default (.client parent) path))))) paths)]
    (let [child-adds (set/difference childs' childs)
          child-dels (set/difference childs childs')]
      [child-adds child-dels])))

;; http://insideclojure.org/2016/03/16/collections/
;; http://spootnik.org/entries/2014/11/06/playing-with-clojure-core-interfaces/index.html
;; https://clojure.github.io/data.priority-map//index.html
;; https://github.com/clojure/data.priority-map/blob/master/src/main/clojure/clojure/data/priority_map.clj
;; https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/APersistentMap.java
;; (filter #(.isInterface %) (ancestors (class #{})))

(defn- make-connection-loss-handler
  "Return a handler for client connection loss that closes the node event channel"
  [znode wmgr]
  (fn [e type]
    (log/infof "Unrecoverable client error (%s) on %s, shutting down watch" type (str znode))
    (async/close! wmgr)
    nil))

(deftype WatchManager [input output child-watch-managers]
  impl/WritePort
  (put! [port val fn1-handler] (impl/put! input val fn1-handler))

  Closeable
  (close [this] (async/close! input) (async/<!! output))

  clojure.lang.Seqable
  (seq [this] (seq child-watch-managers)))

(deftype ZNode [client path stat value children events]
  VirtualNode
  (overlay [this v]
    ;; This should be safe, but it is a (Clojure) code smell.  It could possibly be avoided through rewriting of the ZNode tree.
    (dosync (alter value (fn [old-v] (if (not= old-v v) (do (assert (= old-v ::placeholder) "Can't overwrite existing child") v) old-v))))
    this)
  (update-or-add-child [this path v]
    (let [z' (default client path v)]
      (.get (conj! this z') z')))

  BackedZNode
  (create! [this] ; Synchronously create the node @ version zero, if not already present
    (let [data [@value (meta @value)]]
      (when (zclient/create-znode client path {:persistent? true :data (*serialize* data)})
        (log/debugf "Created %s" (str this))
        true)))
  (delete! [this version]
    (zclient/delete client path version {})
    (log/debugf "Deleted %s" (str this))
    true)
  (watch [this]
    (log/debugf "Watching %s" (str this))
    (let [handle-channel-error (fn [e] (log/errorf e "Exception while processing channel event for %s" (str this)))
          znode-events (async/chan 5 identity handle-channel-error)
          data-events (async/chan (async/sliding-buffer 4) (zdata-xform this) handle-channel-error)
          delete-events (async/chan 1 (dedupe) handle-channel-error) ; why won't a promise channel work here?
          children-events (async/chan 5 (comp (map process-stat) (tap-children this)) handle-channel-error)
          exists-events (async/chan 1 (comp (map process-stat) (tap-stat this)) handle-channel-error)
          handle-connection-loss (make-connection-loss-handler this znode-events)
          watcher (partial async/put! znode-events)]
      (async/pipe data-events events false)
      (async/pipe delete-events events false)
      (async/pipe children-events events false)
      (async/pipe exists-events events false)
      ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkWatches
      ;; https://www.safaribooksonline.com/library/view/zookeeper/9781449361297/ch04.html
      (let [cwms (reduce (fn [cwms child] (assoc cwms child (watch child))) {} @children)
            rc (async/go-loop [cwms cwms]
                 (if-let [{:keys [event-type keeper-state] :as event} (async/<! znode-events)]
                   (do
                     (log/tracef "Event [%s:%s] for %s" event-type keeper-state (str this))
                     (if-let [cwms (case event-type
                                     :None cwms ; Disconnected and Expired keeper states are reported here
                                     ::Boot (do (async/>! znode-events {:event-type :NodeDataChanged})
                                                (async/>! znode-events {:event-type :NodeChildrenChanged})
                                                cwms)
                                     :NodeCreated (do (async/>! events {::type ::created!})
                                                      cwms)
                                     :NodeDeleted (do (async/>! delete-events {::type ::deleted!}) ; Generated by exists, data & child watches.
                                                      cwms)
                                     :NodeDataChanged (do (async/pipe (async/thread (zclient/with-connection handle-connection-loss
                                                                                      (zclient/data client path {:watcher watcher})))
                                                                      data-events false)
                                                          cwms)
                                     :NodeChildrenChanged (async/<!
                                                           (async/thread
                                                             (zclient/with-connection handle-connection-loss
                                                               (let [{:keys [stat paths]} (zclient/children client path {:watcher watcher})
                                                                     [child-adds child-dels] (process-children-changes this (set (keys cwms)) paths)]
                                                                 (when (or (seq child-adds) (seq child-dels))
                                                                   (async/>!! children-events {::type ::children-changed ::stat stat
                                                                                               ::inserted child-adds ::removed child-dels}))
                                                                 (as-> cwms cwms
                                                                   (reduce (fn [cwms child]
                                                                             (log/debugf "Processed insert for %s" (str child))
                                                                             (let [wmgr (watch child)]
                                                                               (async/>!! wmgr {:event-type ::Boot})
                                                                               (assoc cwms child wmgr))) cwms child-adds)
                                                                   (reduce (fn [cwms child]
                                                                             (log/debugf "Processed remove for %s" (str child))
                                                                             (.close (cwms child))
                                                                             (dissoc cwms child)) cwms child-dels))))))
                                     (do (log/warnf "Unexpected znode event:state [%s:%s] while watching %s" event-type keeper-state (str this))
                                         cwms))]
                       (recur cwms)
                       (do (async/>! events {::type ::watch-stop})
                           (let [n (transduce (map (fn [wmgr] (.close wmgr))) + 1 (vals cwms))]
                             (log/infof "The event channel closed UNEXPECTEDLY with %d nodes seen; shutting down %s" n (str this))
                             n))))
                   (do (async/>! events {::type ::watch-stop})
                       (let [n (transduce (map (fn [wmgr] (.close wmgr))) + 1 (vals cwms))]
                         (log/debugf "The event channel closed with %d nodes seen; shutting down %s" n (str this))
                         n))))]
        (async/>!! events {::type ::watch-start})
        (->WatchManager znode-events rc cwms))))
  (actualize! [this wmgr]
    (if-let [stat' (zclient/exists client path {:watcher (partial async/put! wmgr)})]
      (dosync (ref-set stat (process-stat* stat')))
      (create! this))
    (doseq [[child cwmgr] (seq wmgr)] (actualize! child cwmgr))
    (async/>!! wmgr {:event-type ::Boot})
    wmgr)
  (compare-version-and-set! [this version value]
    (let [data [value (meta value)]
          stat' (zclient/set-data client path (*serialize* data) version {})]
      (when stat'
        (log/debugf "Set value for %s @ %s" (str this) version)
        (dosync (ref-set stat (process-stat* stat'))))
      (boolean stat')))
  (signature [this] (dosync
                     (transduce (map signature)
                                (fn accumulate-independently-then-hash
                                  ([] [[nil #{}] [nil #{}]])
                                  ([acc] (mapv hash acc))
                                  ([acc child-hash] (-> acc
                                                        (update-in [0 1] conj (child-hash 0))
                                                        (update-in [1 1] conj (child-hash 1)))))
                                [[@stat #{}] [@value #{}]]
                                @children)))

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

  java.lang.Comparable
  (compareTo [this other-znode] (.compareTo path (.path other-znode)))

  clojure.lang.IFn
  (invoke [this p] (if-let [[_ head tail] (re-matches #"(/[^/]+)(.*)" p)]
                     (let [abs-path (as-> (str path head) s
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
  (hasheq [this] (hash [path client]))

  impl/ReadPort
  (take! [this handler] (impl/take! events handler))

  impl/Channel
  (closed? [this] (impl/closed? events))
  (close! [this] (impl/close! events))

  java.lang.Object
  (equals [this other] (and (= (class this) (class other)) (= (.path this) (.path other)) (= (.client this) (.client other))))
  (hashCode [this] (.hashCode [path client]))
  (toString [this] (format "%s: %s" (.. this (getClass) (getSimpleName)) path)))

(defn add-descendant
  "Add a descendant ZNode to the given parent's (sub)tree `root` ZNode at the given (relative) `path` carrying the given `value`
  creating placeholder intermediate nodes as required."
  [parent path value]
  {:pre [(re-matches #"/.*" path)]}
  (if-let [[_ head tail] (re-matches #"(/[^/]+)(.*)" path)]
    (let [abs-path (as-> (str (.path parent) head) s
                     (if (.startsWith s "//") (.substring s 1) s))]
      (if-not (empty? tail)
        (recur (update-or-add-child parent abs-path ::placeholder) tail value)
        (update-or-add-child parent abs-path value)))
    parent))

(defn open
  "Open a ZooKeeper client connection and watch for client status changes to manage watches on `root` and its descendants"
  [root & args]
  (let [client (.client root)
        client-events (async/tap client (async/chan 3))
        e-handler (fn [e type]
                    (log/warnf "Unrecoverable client error (%s) watching/actualizing drcfg znode tree, aborting." type)
                    nil)
        rc (async/go-loop [wmgr nil] ; start event listener loop
             (if-let [[event client] (async/<! client-events)]
               (do (log/debugf "Root client event %s (%s)" event wmgr)
                   (case event
                     ::zclient/connected (recur (or wmgr (let [wmgr (watch root)] ; At startup and following session expiration
                                                           (zclient/with-connection e-handler (actualize! root wmgr))
                                                           wmgr)))
                     ::zclient/expired (do (async/close! wmgr) (recur nil))
                     ::zclient/closed (when wmgr (.close wmgr)) ; failed connections start but don't connect before closing?
                     (recur wmgr)))
               (do (log/infof "The client event channel closed, shutting down root %s" (str root))
                   (if wmgr (.close wmgr) 0))))]
    (let [zclient-handle (apply zclient/open client args)]
      (reify Closeable (close [_] (.close zclient-handle) (async/<!! rc))))))

(defmacro with-connection
  [znode connect-string timeout & body]
  `(let [znode# ~znode
         z# (.client znode#)
         cs# ~connect-string
         t# ~timeout
         c# (async/chan 10)]
     (async/tap z# c#)
     (let [r# (with-open [_# (open znode# cs# t#)]
                (let [event# (first (async/<!! c#))] (assert (= ::zclient/started event#)))
                (let [event# (first (async/<!! c#))] (assert (= ::zclient/connected event#)))
                ~@body)]
       (let [event# (first (async/<!! c#))] (assert (= ::zclient/closed event#)))
       r#)))

(defn new-root
  "Create a root znode"
  ([] (new-root "/"))
  ([abs-path] (new-root abs-path ::root))
  ([abs-path value] (new-root abs-path value (zclient/create)))
  ([abs-path value zclient]
   (let [events (async/chan (async/sliding-buffer 4))]
     (->ZNode zclient abs-path (ref {:version -1 :cversion -1 :aversion -1}) (ref value) (ref #{}) events))))

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
