(ns roomkey.znode
  "A facade for a Zookeeper znode"
  (:require [roomkey.zclient :as zclient]
            [clojure.string :as string]
            [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as impl]
            [clojure.tools.logging :as log])
  (:import (java.time Instant)))

(defn- normalize-datum [{:keys [stat data]}] {::type ::datum ::value data ::stat stat})

(defn- deserialize
  [s]
  {:pre [(string? s)]}
  (edn/read-string {:readers *data-readers*
                    :default (comp #(do (log/infof "Unrecognized tagged literal: %s %s" (class %) (pr-str %)) %)
                                   tagged-literal)} s))

(defn- serialize [^Object obj] {:post [(string? %)]} (binding [*print-dup* false *print-meta* true] (pr-str obj)))

(defn- decode [^bytes b] {:pre [(instance? (Class/forName "[B") b)] :post [(string? %)]} (String. b "UTF-8"))

(defn- encode [^String s] {:pre [(string? s)] :post [(instance? (Class/forName "[B") %)]} (.getBytes ^String s))

(defn- decode-value
  "Process raw zdata into usefully deserialized types and structure"
  [zdata]
  (update zdata ::value (comp deserialize decode)))

(declare tap-children tap-datum tap-stat default)

(defn- zdata-xform
  [znode]
  (comp (map normalize-datum)
        (filter ::value)
        (map decode-value)
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
 get the results of watching and, as seq'd, to obtain the WatchManagers of its children"))

(defprotocol VirtualNode
  "A value-bearing node in a tree"
  (update-or-add-child [this path value] "Update the existing child or create a new child of this node at the given path & with the given default value")
  (overlay [this v] "Overlay the existing placeholder node's value with a concrete value")
  (signature [this] "Return a (Clojure) hash equivalent to a signature of the state of the subtree at this ZNode"))

;; http://insideclojure.org/2016/03/16/collections/
;; http://spootnik.org/entries/2014/11/06/playing-with-clojure-core-interfaces/index.html
;; https://clojure.github.io/data.priority-map//index.html
;; https://github.com/clojure/data.priority-map/blob/master/src/main/clojure/clojure/data/priority_map.clj
;; https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/APersistentMap.java
;; (filter #(.isInterface %) (ancestors (class #{})))

(defn- make-connection-loss-handler
  "Return a handler for client connection loss that closes the node event channel"
  [znode c]
  (fn [e type]
    (log/debugf "Unrecoverable client error (%s) on %s, shutting down watch" type (str znode))
    (async/close! c)
    nil))

(defn- delta
  "Compute the +/- deltas (sets) of the two provided sets"
  [items items']
  (reduce (fn [acc item] (case [(contains? items item) (contains? items' item)]
                           [true true] acc
                           [true false] (update acc 0 conj item)
                           [false true] (update acc 1 conj item)))
          [#{} #{}] (concat items items')))

(deftype ZNode [^roomkey.zclient.ZClient client ^String path stat value children events]
  VirtualNode
  (overlay [this v]
    ;; This should be safe, but it is a (Clojure) code smell.  It could possibly be avoided through rewriting of the ZNode tree.
    (dosync (alter value (fn [old-v]
                           (if (not= old-v v)
                             (if (-> stat deref :version pos?)
                               (do (log/warnf "Refusing to overwrite cluster-synchronized value at %s" (str this)) old-v)
                               (do (assert (= old-v ::placeholder) (format "Can't overwrite existing node at %s" (str this))) v))
                             old-v))))
    this)
  (update-or-add-child [this path v]
    (let [z' (default client path v)]
      (get ^clojure.lang.ITransientSet (conj! this z') z')))
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

  BackedZNode
  (create! [this] ; Synchronously create the node @ version zero, if not already present
    (when-let [stat (zclient/create-znode client path {:persistent? true :data ((comp encode serialize) @value)})]
      (log/debugf "Created %s" (str this))
      stat))
  (delete! [this version]
    (when (zclient/delete client path version {})
      (log/debugf "Deleted %s" (str this))
      true))
  (compare-version-and-set! [this version value]
    (when-let [stat (zclient/set-data client path ((comp encode serialize) value) version {})]
      (when stat
        (log/debugf "Set value for %s" (str this))
        stat)))
  (watch [this] ; This is called recursively inside parent's go block -it MUST be non-blocking.
    (log/tracef "Watching %s" (str this))
    (let [path-prefix (as-> (str path "/") s (if (= s "//") "/" s))
          handle-channel-error (fn [e] (log/errorf e "Exception while processing channel event for %s" (str this)))
          znode-events (async/chan 10 identity handle-channel-error)
          data-events (async/chan 1 (zdata-xform this) handle-channel-error)
          children-events (async/chan 1 (tap-children this) handle-channel-error)
          stat-events (async/chan 1 (tap-stat this) handle-channel-error)
          handle-connection-loss (make-connection-loss-handler this znode-events)
          watcher (zclient/make-watcher (partial async/put! znode-events))
          zop (fn [op] (async/thread (zclient/with-connection handle-connection-loss (op client path {:watcher watcher}))))]
      (async/pipe data-events events false)
      (async/pipe children-events events false)
      (async/pipe stat-events events false)
      ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkWatches
      ;; https://www.safaribooksonline.com/library/view/zookeeper/9781449361297/ch04.html
      (async/go
        (async/>! events {::type ::watch-start})
        (when-let [stat (async/<! (zop zclient/exists))]
          (async/>! stat-events {::type ::exists ::stat stat})
          (async/>! znode-events {:event-type ::Boot :keeper-state ::Boot}))
        [this (async/go-loop [cws (async/<! (async/into {} (async/merge (map watch @children))))]
                (if-let [{:keys [event-type keeper-state] :as event} (async/<! znode-events)]
                  (do
                    (log/tracef "Event [%s:%s] for %s" event-type keeper-state (str this))
                    (case event-type
                      :None (do (async/close! znode-events) (recur cws))
                      :NodeCreated (let [stat (async/<! (zop zclient/exists))]
                                     (async/>! stat-events {::type ::created! ::stat stat})
                                     (async/>! znode-events {:event-type ::Boot :keeper-state ::Boot})
                                     (recur cws))
                      :NodeDeleted (do (async/>! events {::type ::deleted!}) (async/close! znode-events) (recur cws))
                      :NodeDataChanged (do (async/pipe (zop zclient/data) data-events false) (recur cws))
                      :NodeChildrenChanged (if-let [{:keys [children stat]} (async/<! (zop zclient/children))]
                                             (let [lcl (into #{} (map key) cws)
                                                   rmt (into #{} (comp (map (partial str path-prefix))
                                                                       (map #(update-or-add-child this % ::placeholder))) children)
                                                   [lcl+ rmt+] (delta lcl rmt)
                                                   boot? (= ::Boot keeper-state)
                                                   [ins rem persist] [rmt+ (if boot? #{} lcl+) (if boot? lcl+ #{})]
                                                   cws (async/<! (async/into (apply dissoc cws rem) (async/merge (map watch rmt+))))]
                                               (async/thread (doseq [child persist] ; these should be async fire-and-forget...
                                                               (assert (zclient/with-connection handle-connection-loss (create! child)))))
                                               (when (or (seq ins) (seq rem) boot?)
                                                 (async/>! children-events {::type ::children-changed ::stat stat ::inserted ins ::removed rem}))
                                               (recur cws))
                                             (recur cws))
                      ::Boot (do (async/>! znode-events {:event-type :NodeDataChanged :keeper-state ::Boot})
                                 (async/>! znode-events {:event-type :NodeChildrenChanged :keeper-state ::Boot})
                                 (recur cws))))
                  (do (async/>! events {::type ::watch-stop})
                      (log/tracef "The event channel closed; shutting down %s" (str this))
                      (async/<! (async/reduce + 1 (async/merge (vals cws)))))))])))

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
  (compareTo [this other-znode] (.compareTo path (.path ^ZNode other-znode)))

  clojure.lang.IFn
  (invoke [this p] (if-let [[_ head tail] (re-matches #"(/[^/]+)(.*)" p)]
                     (let [abs-path (as-> (str path head) s
                                      (if (.startsWith s "//") (.substring s 1) s))]
                       (when-let [child (some (fn [^ZNode node] (when (= (.path node) abs-path) node)) @children)]
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
  (equals [this other] (and (= (class this) (class other)) (= (.path this) (.path ^ZNode other)) (= (.client this) (.client ^ZNode other))))
  (hashCode [this] (.hashCode [path client]))
  (toString [this] (str "ℤℕ:" path)))

(defn ^roomkey.znode.ZNode default ; NB: This operation does not update the children of the parent
  ([client path] (default client path ::unknown))
  ([client path value]
   (let [events (async/chan (async/sliding-buffer 4))]
     (->ZNode client path (ref {:version -1 :cversion -1 :aversion -1}) (ref value) (ref #{}) events))))

(defn- tap-children
  [^roomkey.znode.ZNode node]
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
  [^roomkey.znode.ZNode node]
  (let [stat-ref (.stat node)
        value-ref (.value node)]
    (fn [rf]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result {:keys [roomkey.znode/value roomkey.znode/stat] :as input}]
         (dosync
          (ref-set stat-ref stat)
          (ref-set value-ref value))
         (rf result input))))))

(defn- tap-stat
  [^roomkey.znode.ZNode node]
  (let [stat-ref (.stat node)]
    (fn [rf]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result {:keys [roomkey.znode/stat] :as input}]
         (dosync
          (ref-set stat-ref stat))
         (rf result input))))))

(defn ^roomkey.znode.ZNode add-descendant
  "Add a descendant ZNode to the given parent's (sub)tree `root` ZNode at the given (relative) `path` carrying the given `value`
  creating placeholder intermediate nodes as required."
  [^roomkey.znode.ZNode parent path value]
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
  [^roomkey.znode.ZNode root & args]
  (let [client (.client root)
        tap (async/tap client (async/chan 2))
        rc (async/go-loop [wmgr nil] ; start event listener loop
             (if-let [[event client] (async/<! tap)]
               (do (log/debugf "Root client event %s (%s)" event wmgr)
                   (recur (case event
                            ::zclient/connected (or wmgr (last (async/<! (watch root)))) ; At startup and following session expiration
                            ::zclient/expired nil
                            ::zclient/closed (do ; failed connections start but don't connect before closing?
                                               (async/close! tap)
                                               (log/debugf "The client event channel closed; shutting down %s" (str root))
                                               wmgr)
                            wmgr)))
               (when wmgr (async/<! wmgr))))]
    (let [zclient-handle (apply zclient/open client args)]
      (reify java.io.Closeable
        (close [_]
          (.close ^java.io.Closeable zclient-handle)
          (async/<!! rc))))))

(defmacro with-connection
  [znode connect-string timeout & body]
  `(let [^roomkey.znode.ZNode znode# ~znode
         z# (.client znode#)
         cs# ~connect-string
         t# ~timeout
         c# (async/chan 10)]
     (async/tap z# c#)
     (let [r# (with-open [^java.io.Closeable _# (open znode# cs# t#)]
                (let [event# (first (async/<!! c#))] (assert (= ::zclient/started event#)))
                (let [event# (first (async/<!! c#))] (assert (= ::zclient/connected event#)))
                ~@body)]
       (let [event# (first (async/<!! c#))] (assert (= ::zclient/closed event#)))
       r#)))

(defn ^roomkey.znode.ZNode new-root
  "Create a root znode"
  ([] (new-root "/"))
  ([abs-path] (new-root abs-path ::root))
  ([abs-path value] (new-root abs-path value (zclient/create)))
  ([abs-path value zclient] (default zclient abs-path value)))

;; https://stackoverflow.com/questions/49373252/custom-pprint-for-defrecord-in-nested-structure
;; https://stackoverflow.com/questions/15179515/pretty-printing-a-record-using-a-custom-method-in-clojure
(defmethod clojure.core/print-method ZNode
  [^roomkey.znode.ZNode znode ^java.io.Writer writer]
  (.write writer "#")
  (.write writer (.. znode (getClass) (getSimpleName)))
  (.write writer " ")
  (print-simple znode writer) #_ (.write writer (format "#<%s %s>" (.. znode (getClass) (getSimpleName)) (.path znode))))

(defmethod clojure.pprint/simple-dispatch ZNode
  [^roomkey.znode.ZNode znode]
  (pr znode))

(defn walk
  "Walk the tree defined by `connect-string` and transduce the asynchronously discovered nodes with `xform` and `f`."
  [connect-string timeout xform f init]
  (let [root (new-root "/")]
    (with-connection root connect-string timeout
      (async/<!! (async/transduce xform f init ((fn visit [z]
                                                  (let [c (async/chan)]
                                                    (async/go-loop [me nil them nil]
                                                      (if (and me them)
                                                        (async/pipe (async/merge (cons me them)) c true)
                                                        (when-let [e (::type (async/<! z))]
                                                          (case e
                                                            ::watch-start (recur (async/to-chan [z]) them)
                                                            ::children-changed (recur me (map visit (seq z)))
                                                            (recur me them)))))
                                                    c)) root))))))
