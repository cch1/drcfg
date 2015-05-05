(ns roomkey.zkutil
  (:import [org.apache.curator.retry RetryNTimes
            ExponentialBackoffRetry BoundedExponentialBackoffRetry]
           [org.apache.curator.framework CuratorFramework
            CuratorFrameworkFactory]
           [org.apache.curator.framework.recipes.cache ChildData
            NodeCache NodeCacheListener
            PathChildrenCache PathChildrenCacheMode
            PathChildrenCacheListener PathChildrenCacheEvent]
           [org.apache.zookeeper CreateMode]
           [org.apache.zookeeper.KeeperException])
  (:require [clojure.string :as string]
            [clojure.tools.logging :as log]))

(def ^:private metadata-name ".metadata")

(defn- metapath [path] (str path "/" metadata-name))

(defn deserialize
  "convert bytes to object"
  [b]
  (read-string (String. b "UTF-8")))

(defn serialize
  "convert object to byte array"
  [obj]
  (.getBytes (binding [*print-dup* true] (pr-str obj))))

(defn- child-path ; code courtesy of https://github.com/qiuxiafei/zk-web
  "Get child path by parent and child name"
  [parent child]
  (str parent (when-not (.endsWith parent "/") "/") child))

(defn create ; code courtesy of https://github.com/qiuxiafei/zk-web
  "Create a node in zk with a client"
  ([client path data & {:keys [ephemeral?]}]
    (.. client
      (create)
      (creatingParentsIfNeeded)
      (withMode (if ephemeral? CreateMode/EPHEMERAL CreateMode/PERSISTENT))
      (forPath path (serialize data))))
  ([client path]
    (.. client
      (create)
      (creatingParentsIfNeeded)
      (forPath path nil))))

(defn ls-all
  "List children of a node, including hidden (metadata) nodes"
  [client path]
  (.. client (getChildren) (forPath path)))

(defn ls
  "List children of a node, excluding hidden (metadata) nodes"
  [client path]
  (remove #(= metadata-name %) (ls-all client path)))

(defn exists?
  "Check if node exists; returns path if true, nil if false"
  [client path]
  (when (.. client (checkExists) (forPath path)) path))

(defn stat
  "Get stat of a node"
  [client path]
  (when-let [stat-obj (.. client (checkExists) (forPath path))]
    (dissoc (bean stat-obj) :class)))

(defn raw-set
  "Set raw data to a node"
  [client path data]
  (.. client (setData) (forPath path data)))

(defn raw-get
  "Get raw data from a node"
  [client path]
  (when-let [v (.. client (getData) (forPath path))]
    (when (pos? (count v)) v)))

(defn nset
  "Serialize object and set as data to a node"
  [client path data]
  (raw-set client path (serialize data)))

(defn nget
  "Get data from a node and deserialize to object"
  [client path]
  (when-let [raw-value (raw-get client path)]
    (deserialize raw-value)))

(defn set-metadata
  "Set metadata for a node.  'data' must be a map; 'path' must already exist"
  [client path data]
  {:pre [(map? data)]}
  (try (.. client
           (create)
           ;; not using creatingParentsIfNeeded here because we want
           ;; the path to already exists
           (forPath (metapath path) (serialize data)))
       (catch org.apache.zookeeper.KeeperException$NodeExistsException e
         ;; expected exception -- just update existing node instead
         (nset client (metapath path) data))))

(defn get-metadata
  "Get metadata map for a node."
  [client path]
  (try (nget client (metapath path))
       (catch org.apache.zookeeper.KeeperException$NoNodeException e
         ;; no metadata -- just return nil
         nil)))

(defn get-metadata-mtime
  "Get last-modification date for metadata (epoch time)."
  [client path]
  (when-let [stat-obj (.. client (checkExists) (forPath (metapath path)))]
    (.. stat-obj (getMtime))))

(defn rm
  "Delete a node in zk with a client.  Automatically deletes metadata if it exists"
  [client path]
  (try (.. client (delete) (forPath (metapath path)))
       (catch org.apache.zookeeper.KeeperException$NoNodeException e))
  (.. client (delete) (forPath path)))

(defn rmr ; code courtesy of https://github.com/qiuxiafei/zk-web
  "Remove recursively"
  [client path]
  (doseq [child (ls client path)]
    (rmr client (child-path path child)))
  (rm client path))

(defn- connect-inner ; code courtesy of https://github.com/qiuxiafei/zk-web
  "Create a zk client using addr as connecting string"
  [addr]
  (let [client (.. (CuratorFrameworkFactory/builder)
                (connectString addr)
                ;(retryPolicy (BoundedExponentialBackoffRetry. 1000 60000 Integer/MAX_VALUE))
                (retryPolicy (RetryNTimes. Integer/MAX_VALUE 5000))
                (build))]
        (.start client)
        client))

(def zkconn!
  "Connect to zookeeper, using memoized connection if available"
  (memoize connect-inner))

(defn init!
  "Initialize a zookeeper environment (i.e. create root node if necessary)"
  [client]
  (try
    (create client "/")
    (catch org.apache.zookeeper.KeeperException$NodeExistsException e)))

(defn is-connected?
  "Test whether zookeeper client is connected"
  [client]
  (.. client (getZookeeperClient) (isConnected)))

(defn watch
  "Watch a node in zk and trigger function when it changes"
  ;; https://github.com/Netflix/curator/blob/archive/curator-recipes/src/main/java/com/netflix/curator/framework/recipes/cache/NodeCache.java
  [client path f]
  (let [cache (new NodeCache client path)]
    (.. cache (getListenable) (addListener
                               (proxy [NodeCacheListener] []
                                 (nodeChanged []
                                   (when (.. cache (getCurrentData)) ; skip nulls (deleted nodes)
                                     (f))))))
    (.. cache (start))))

(defn watch-children
  "Watch a node in zk and trigger function when it's children change"
  [client path f]
  (let [cache (new PathChildrenCache client path PathChildrenCacheMode/CACHE_PATHS_ONLY)]
    (.. cache (getListenable) (addListener
      (proxy [PathChildrenCacheListener] []
        (childEvent [& _]
          (f)))))
    (.. cache (start))))

(defn get-connect-string
  "Recall the connect string for a connected client for use in creating secondary connections"
  [client]
  (.. client (getZookeeperClient) (getCurrentConnectionString)))
