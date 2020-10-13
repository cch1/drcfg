(ns zk.node
  "An idiomatic Clojure abstraction around a Zookeeper ZNode"
  (:require [zk.client :as client]
            [cognitect.anomalies :as anomalies]
            [clojure.string :as string]
            [clojure.edn :as edn]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as impl]
            [clojure.tools.logging :as log])
  (:import [org.apache.zookeeper ZooKeeper Watcher WatchedEvent data.Stat
            AsyncCallback$Create2Callback AsyncCallback$StatCallback AsyncCallback$VoidCallback
            AsyncCallback$DataCallback AsyncCallback$Children2Callback
            CreateMode ZooKeeper$States
            Watcher$Event$EventType Watcher$Event$KeeperState
            ZooDefs$Ids
            KeeperException$Code]
           (java.nio.file Paths Path)
           (java.time Instant)))

(def acls {:open-acl-unsafe ZooDefs$Ids/OPEN_ACL_UNSAFE ; This is a completely open ACL
           :anyone-id-unsafe ZooDefs$Ids/ANYONE_ID_UNSAFE ; This Id represents anyone
           :auth-ids ZooDefs$Ids/AUTH_IDS ; This Id is only usable to set ACLs
           :creator-all-acl ZooDefs$Ids/CREATOR_ALL_ACL ; This ACL gives the creators authentication id's all permissions
           :read-all-acl ZooDefs$Ids/READ_ACL_UNSAFE ; This ACL gives the world the ability to read
           })

(def create-modes {;; The znode will not be automatically deleted upon client disconnect
                   {:persistent? true, :sequential? false} CreateMode/PERSISTENT
                   ;; The znode will be deleted upon client disconnect, and its name will be appended with a monotonically increasing number
                   {:persistent? false, :sequential? true} CreateMode/EPHEMERAL_SEQUENTIAL
                   ;; The znode will be deleted upon the client's disconnect
                   {:persistent? false, :sequential? false} CreateMode/EPHEMERAL
                   ;; The znode will not be deleted upon client disconnect, and its name will be appended with a monotonically increasing number
                   {:persistent? true, :sequential? true} CreateMode/PERSISTENT_SEQUENTIAL})

(defn- stat-to-map
  ([^Stat stat]
   ;; https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#sc_timeInZk
   (when stat
     {:czxid (.getCzxid stat)
      :mzxid (.getMzxid stat)
      :pzxid (.getPzxid stat)
      :ctime (Instant/ofEpochMilli (.getCtime stat))
      :mtime (Instant/ofEpochMilli (.getMtime stat))
      :version (.getVersion stat)
      :cversion (.getCversion stat)
      :aversion (.getAversion stat)
      :ephemeralOwner (.getEphemeralOwner stat)
      :dataLength (.getDataLength stat)
      :numChildren (.getNumChildren stat)})))

(def ^:private default-stat {:version -1 :cversion -1 :aversion -1})

(defn- deserialize
  [s]
  {:pre [(string? s)]}
  (edn/read-string {:readers *data-readers*
                    :default (comp #(do (log/infof "Unrecognized tagged literal: %s %s" (class %) (pr-str %)) %)
                                   tagged-literal)} s))

(defn- serialize [^Object obj] {:post [(string? %)]} (binding [*print-dup* false *print-meta* true] (pr-str obj)))

(defn- decode [^bytes b] {:pre [(instance? (Class/forName "[B") b)] :post [(string? %)]} (String. b "UTF-8"))

(defn- encode [^String s] {:pre [(string? s)] :post [(instance? (Class/forName "[B") %)]} (.getBytes ^String s "UTF-8"))

(defn ^AsyncCallback$Create2Callback make-create-callback
  [c tag]
  (reify AsyncCallback$Create2Callback
    (processResult [this rc path ctx name stat]
      (async/put! c {:type ::create :tag tag :rc rc :name name :stat stat}))))

(defn ^AsyncCallback$StatCallback make-stat-callback
  [c tag]
  (reify AsyncCallback$StatCallback
    (processResult [this rc path ctx stat]
      (async/put! c {:type ::stat :tag tag :rc rc :stat stat}))))

(defn ^AsyncCallback$VoidCallback make-void-callback
  [c tag]
  (reify AsyncCallback$VoidCallback
    (processResult [this rc path ctx]
      (async/put! c {:type ::void :tag tag :rc rc}))))

(defn ^AsyncCallback$DataCallback make-data-callback
  [c tag]
  (reify AsyncCallback$DataCallback
    (processResult [this rc path ctx data stat]
      (async/put! c {:type ::data :tag tag :rc rc :data data :stat stat}))))

(defn ^AsyncCallback$Children2Callback make-children-callback
  [c tag]
  (reify AsyncCallback$Children2Callback
    (processResult [this rc path ctx children stat]
      (async/put! c {:type ::children :tag tag :rc rc :children children :stat stat}))))

(defn- delta
  "Compute the +/- deltas (sets) of the two provided sets"
  [items items']
  (reduce (fn [acc item] (case [(contains? items item) (contains? items' item)]
                           [true true] acc
                           [true false] (update acc 0 conj item)
                           [false true] (update acc 1 conj item)))
          [#{} #{}] (concat items items')))

(defprotocol TreeNode
  "Operate on a Zookeeper node tree while either online or offline."
  (path [this])
  (create-child [this name value] "Create a child, but do not adopt him into the collection of children")
  (update-or-add-child [this path value] "Update the existing child with `value` or create a new child at `path` with the (default) `value`")
  (overlay [this v] "Overlay the existing placeholder node's value with the concrete value `v`")
  (signature [this] "Return a (Clojure) hash equivalent to a signature of the state of the subtree at this ZNode"))

(defprotocol ConnectedNode
  "Query and manipulate an online node connected to a ZooKeeper cluster."
  (watch [this pub] "Watch the given pub for events about this node, and recursively for all its children.")
  ;; "Mutating operations on a Zookeeper node"
  (create! [this options] [this channel options])
  (update! [this data version options] [this data version channel options])
  (delete! [this version options] [this version channel options])
  ;; "Querying operations on a Zookeeper node"
  (get-existence [this options] [this channel options])
  (get-data [this options] [this channel options])
  (get-children [this options] [this channel options]))

(deftype ZNode [client path vref cref sref events]
  TreeNode
  (path [this] (str path))
  (overlay [this v] ; this is the ugly consequence of child nodes (vars) being created before their parents (namespaces).
    (dosync (alter vref (fn [old-v]
                          (if (not= old-v v)
                            (if (-> sref deref :version pos?)
                              (do (log/warnf "Refusing to overwrite cluster-synchronized value at %s" (str this)) old-v)
                              (do (assert (= old-v ::placeholder) (format "Can't overwrite existing node at %s" (str this))) v))
                            old-v))))
    this)
  (create-child [this name v] (let [events (async/chan (async/sliding-buffer 8))
                                    p (.resolve path name)]
                                (ZNode. client p (ref v) (ref #{}) (ref default-stat) events)))
  (update-or-add-child [this name v]
    (let [child (create-child this name v)]
      (dosync (if (contains? @cref child)
                (when (not= v ::placeholder) (overlay (@cref child) v))
                (alter cref conj child)))
      (get @cref child)))

  ConnectedNode
  (watch [this pub]
    (log/tracef "Watching %s" (str this))
    (let [handle-channel-error (fn [e] (log/errorf e "Exception while processing channel event for %s" (str this)))
          callback-reports (async/chan 8 (comp) handle-channel-error)
          sub (async/chan 8)
          sync (async/chan)]

      (async/go-loop [] ; Start the node event listener loop, but don't connect to the pub yet
        (if-let [{:keys [type] :as event} (async/<! sub)]
          (do
            (log/debugf "%-20s Received watch event %25s" this type)
            (case type
              :NodeCreated nil ; processed by callback, but late-arriving subscribed event can occur
              :NodeDataChanged (get-data this callback-reports {:tag ::data-changed})
              :NodeChildrenChanged (get-children this callback-reports {:tag ::children-changed})
              :NodeDeleted (do (async/>! events {::type ::deleted!}) (async/close! sub))
              (throw (Exception. (format "Unexpected event %s for %s" event this))))
            (recur))
          (do (async/close! callback-reports)
              (log/debugf "The watch event channel closed; shutting down %s" this))))

      (async/go ; Establish the node
        (let [{:keys [rc stat] :as report} (async/<! (get-existence this (async/chan) {:tag ::exists}))
              awaiting (case (first (client/translate-return-code rc))
                         :OK (do (async/>! callback-reports report)
                                 (async/<! (async/into #{} (async/merge (mapv #(watch % pub) @cref)))) ; wait for children
                                 (async/sub pub (.path this) sub true) ; connect to the firehose of events
                                 (get-data this callback-reports {:tag ::sync-data})
                                 (get-children this callback-reports {:tag ::sync-children})
                                 #{::exists ::sync-data ::sync-children})
                         :NONODE (let [{:keys [rc stat] :as report} (async/<! (create! this (async/chan) {}))]
                                   (case (first (client/translate-return-code rc))
                                     :OK (do (async/>! callback-reports report)
                                             (async/<! (async/into #{} (async/merge (mapv #(watch % pub) @cref)))) ; wait for children
                                             (async/sub pub (.path this) sub true) ; connect to the firehose of events
                                             #{::created!})
                                     ;; :NODEEXISTS nil ; unlikely race condition ... FIXME
                                     (let [[ex _] (client/kex-info rc "Can't establish node" {::node this})] (throw ex)))))]

          (async/go-loop [watched @cref awaiting awaiting]
            (when-let [{:keys [type tag rc stat name data children] :as report} (async/<! callback-reports)]
              (let [result (first (client/translate-return-code rc))]
                (log/logf (if (= :OK result) :debug :warn) "%-18s Callback report: %16s %-11s" this (clojure.core/name tag) result)
                (when (= :OK result)
                  (let [stat (when stat (stat-to-map stat))
                        awaiting' (disj awaiting tag)
                        watched' (case type
                                   ::create (do (dosync (ref-set sref stat))
                                                (async/>! events {::type tag ::stat stat})
                                                watched)
                                   ::stat (do (dosync (ref-set sref stat))
                                              (async/>! events {::type tag ::stat stat})
                                              watched)
                                   ::data (do (when (not (awaiting' ::sync-data))
                                                (let [value ((comp deserialize decode) data)
                                                      old (dosync (ref-set sref stat)
                                                                  (let [old @vref] (ref-set vref value) old))]
                                                  (when (not (and (= value old) (= (type value) (type old))))
                                                    (async/>! events {::type tag ::stat stat ::value value}))))
                                              watched)
                                   ::children (if (not (awaiting' ::sync-children))
                                                (let [[ins rem] (dosync (ref-set sref stat)
                                                                        (let [rmt (into #{} (map #(create-child this % ::placeholder)) children)
                                                                              [ins rem] (delta rmt @cref)]
                                                                          (apply alter cref conj ins)
                                                                          (apply alter cref disj rem)
                                                                          [ins rem]))]
                                                  (log/debugf "%-20s ****A***** %s -- %s" this ins rem)
                                                  (when (or (seq ins) (seq rem))
                                                    (async/>! events {::type tag ::stat stat ::inserted ins ::removed rem}))
                                                  (let [[ins rem] (delta @cref watched)]
                                                    (log/debugf "%-20s ****W***** %s -- %s" this ins rem)
                                                    (async/<! (async/into #{} (async/merge (mapv #(watch % pub) ins))))
                                                    (apply conj (apply disj watched rem) ins)))
                                                watched)
                                   ::void (do (assert (= ::deleted tag)) watched) ; corresponding watch is responsible for housekeeping
                                   (throw (Exception. (format "Unexpected callback type %s for %s" type this))))]
                    (when (and (not (empty? awaiting)) (empty? awaiting'))
                      (async/>! events {::type ::synchronized ::stat stat})
                      (async/close! sync))
                    (recur watched' awaiting')))))))
        (async/<! sync))))

  (create! [this options] (let [{:keys [rc stat]} (async/<!! (create! this (async/chan) options))]
                            (if (= :OK (first (client/translate-return-code rc)))
                              (stat-to-map stat)
                              (throw (first (client/kex-info rc "Zookeeper call failed" {::path (str path)}))))))
  (create! [this channel {:keys [data acl persistent? sequential?]
                          :or {data @vref persistent? true sequential? false acl (acls :open-acl-unsafe)}}]
    (let [create-mode (create-modes {:persistent? (boolean persistent?), :sequential? (boolean sequential?)})
          callback (make-create-callback channel ::created!)
          data ((comp encode serialize) data)]
      (client #(.create % (.path this) data acl create-mode callback nil)) channel))
  (update! [this data version options] (let [{:keys [rc stat]} (async/<!! (update! this data version (async/chan) options))]
                                         (if (= :OK (first (client/translate-return-code rc)))
                                           (stat-to-map stat)
                                           (throw (first (client/kex-info rc "Zookeeper call failed" {::path (str path)}))))))
  (update! [this data version channel options]
    (let [callback (make-stat-callback channel ::updated!)
          data ((comp encode serialize) data)]
      (client #(.setData % (.path this) data version callback nil)) channel))
  (delete! [this version options] (let [{:keys [rc stat]} (async/<!! (delete! this version (async/chan) options))]
                                    (if (= :OK (first (client/translate-return-code rc)))
                                      true
                                      (throw (first (client/kex-info rc "Zookeeper call failed" {::path (str path)}))))))
  (delete! [this version channel options]
    (let [callback (make-void-callback channel ::deleted!)]
      (client #(.delete % (.path this) version callback nil)) channel))

  (get-existence [this channel {:keys [tag] :or {tag ::existence} :as options}]
    (let [callback (make-stat-callback channel tag)]
      (client #(.exists % (.path this) false callback nil)) channel))
  (get-data [this channel {:keys [tag] :or {tag ::data} :as options}]
    (let [callback (make-data-callback channel tag)]
      (client #(.getData % (.path this) false callback nil)) channel))
  (get-children [this channel {:keys [tag] :or {tag ::children} :as options}]
    (let [callback (make-children-callback channel tag)]
      (client #(.getChildren % (.path this) false callback nil)) channel))

  clojure.lang.Seqable
  (seq [this] (seq @cref))

  clojure.lang.IHashEq
  (hasheq [this] (hash [path client]))

  clojure.lang.IMeta
  (meta [this] @sref)

  clojure.lang.IDeref
  (deref [this] @vref)

  clojure.lang.Counted
  (count [this] (count @cref))

  clojure.lang.Named
  (getName [this] (str (.getFileName path)))
  (getNamespace [this] (some-> (.getParent path) str))

  clojure.lang.IFn
  (invoke [this descendant] (loop [parent this [head & tail] (Paths/get descendant (into-array String []))]
                              (if head
                                (let [abs-path (.resolve (.-path parent) head)]
                                  (when-let [child (some (fn [^ZNode node] (when (= (.-path node) abs-path) node)) @(.cref parent))]
                                    (recur child tail)))
                                parent)))
  (applyTo [this args]
    (let [n (clojure.lang.RT/boundedLength args 1)]
      (case n
        0 (throw (clojure.lang.ArityException. n (.. this (getClass) (getSimpleName))))
        1 (.invoke this (first args))
        (throw (clojure.lang.ArityException. n (.. this (getClass) (getSimpleName)))))))

  impl/ReadPort
  (take! [this handler] (impl/take! events handler))

  impl/Channel
  (closed? [this] (impl/closed? events))
  (close! [this] (impl/close! events))

  java.lang.Comparable
  (compareTo [this other-znode] (.compareTo (str this) (str ^ZNode other-znode)))

  java.lang.Object
  (toString [this] (str "ℤℕ:" path))
  (hashCode [this] (.hashCode [path client]))
  (equals [this other] (and (= (class this) (class other)) (= (.-path this) (.-path ^ZNode other)) (= (.client this) (.client ^ZNode other)))))

(defn ^zk.node.ZNode new-root
  "Create a root znode"
  ([] (new-root "/"))
  ([path] (new-root path ::root))
  ([path value] (new-root path value (client/create)))
  ([path value client] (let [events (async/chan (async/sliding-buffer 8))
                             p (Paths/get path (into-array String []))]
                         (->ZNode client p (ref value) (ref #{}) (ref default-stat) events))))

(defn ^zk.node.ZNode add-descendant
  "Add a descendant ZNode to the given parent's (sub)tree `root` ZNode at the given (relative) `path` carrying the given `value`
  creating placeholder intermediate nodes as required."
  [^zk.node.ZNode parent path value]
  {:pre [(re-matches #"/.*" path)]}
  (loop [parent parent [head & tail] (Paths/get path (into-array String []))]
    (if-not (empty? tail)
      (recur (update-or-add-child parent head ::placeholder) tail)
      (update-or-add-child parent head value))))

(defn open
  "Open a resilient ZooKeeper client connection and watch for events on `root` and its descendants"
  [^zk.node.ZNode root cstring & args]
  (let [chandle (client/open (.client root) cstring args)]
    (async/<!! (watch root (async/pub chandle :path)))
    chandle))

;; https://stackoverflow.com/questions/49373252/custom-pprint-for-defrecord-in-nested-structure
;; https://stackoverflow.com/questions/15179515/pretty-printing-a-record-using-a-custom-method-in-clojure
(defmethod clojure.core/print-method ZNode
  [^zk.node.ZNode znode ^java.io.Writer writer]
  (.write writer "#")
  (.write writer (.. znode (getClass) (getSimpleName)))
  (.write writer " ")
  (print-simple znode writer) #_ (.write writer (format "#<%s %s>" (.. znode (getClass) (getSimpleName)) (.path znode))))

(defmethod clojure.pprint/simple-dispatch ZNode
  [^zk.node.ZNode znode]
  (pr znode))

(defn walk
  "Walk the tree starting at `root` returning a lazy sequence of `(`f` node) for each node visited"
  ([root] (walk root identity))
  ([root f] ((fn walk [node]
               (lazy-seq
                (cons (f node) (mapcat walk (seq node))))) root)))
