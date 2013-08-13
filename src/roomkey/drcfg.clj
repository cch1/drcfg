(ns roomkey.drcfg
  "Dynamic Distributed Run-Time configuration"
  (:import org.apache.zookeeper.KeeperException)
  (:require [roomkey.zkutil :as zk]
            [clojure.string :as string]
            [clojure.tools.logging :as log]))

;;; USAGE:  see /roomkey/README.md

(def zk-prefix "/drcfg")

(defmacro ns-path [n]
  `(str "/" (str *ns*) "/" ~n))

(defn- try-sync [a v name]
  (try (reset! a v)
       (catch java.lang.IllegalStateException e
         (log/warnf "Could not slave atom to %s (invalid state: %s) value remains %s" name v @a))))

(defn- get-or-create-znode [client name default]
  (try (zk/nget client name)
       (catch org.apache.zookeeper.KeeperException$NoNodeException e
         (try (do
                (log/infof "Assigning default value to new drcfg node.  Path: %s ; Default: %s" name default)
                (zk/create client name default))
              (catch org.apache.zookeeper.KeeperException$NodeExistsException e
                (log/warnf "NodeExistsException--somebody beat us to the creation for %s.  Continuing with new value." name)
                (zk/nget client name))))))

(defn- watch-znode
  "Create a distributed atom with the given name using the given client and
apply default or actual value"
  [client name la]
  (do
    (get-or-create-znode client name @la)
    (zk/set-metadata client name (or (meta la) {}))
    (zk/watch client name (fn []
      (when-let [new-value (zk/nget client name)]
        (log/debugf "Watched value update: old: %s; new: %s" @la new-value)
        (try-sync la new-value name))))
    (log/debugf "Created new watched-znode %s" name)))

(let [client (promise)
      unlinked (ref ())]

  (defn link-all!
    "Link (via an agent) the provided unlinked name/atom pairs"
    [unlinked]
    (doseq [[name la] unlinked]
      (watch-znode @client name la)))

  (defn connect!
    "Initiate a connection to the zookeeper service and link all previously
defined local references.  Hosts can be a comma separated list of
hostname:port values to represent a zookeeper cluster."
    [hosts]
    (let [connect-string (str hosts zk-prefix)]
      (future (deliver client (zk/zkconn! connect-string))
        (do
          (zk/init! @client)
          (dosync (alter unlinked link-all!))))))

  (defn connect-with-wait!
    "Initiate a connection to the zookeeper service and link all previously
defined local references,  waiting for the connection and linkage to complete
before returning"
    [hosts]
    (let [connect-string (str hosts zk-prefix)]
      (deliver client (zk/zkconn! connect-string))
      (zk/init! @client)
      (dosync (alter unlinked link-all!))))

  ;; NB: this function runs in the current thread and may block!
  ;; NB: the name parameter must be properly namespaced!
  (defn drset!
    "Update a distributed config value.  Name must be fully specified,
including the leading slash and namespace"
    [name v]
    (zk/nset @client name v))

  (defn >-
    "Create a config reference with the given name (must be fully specified,
including leading slash and namespace) and default value and record it for
future linking to a distributed ref. "
    [name default & options]
    (zk/serialize default) ;; throws exception if object is not serializable
    (let [la (apply atom default options)]
      (if (and (not (:ignore-prior-connection (apply hash-map options)))
               (realized? client))
        (log/errorf "New drcfg reference %s defined after connect -- will not be linked to zookeeper" name)
        (dosync (alter unlinked conj [name la])))
      la)))

(defmacro def>-
  "Def a config reference with the given atom, using the atom name as the
leaf name, and automatically prepending the namespace to determine the
zookeeper path.  NB: when refactoring, note that the namespace may change,
leaving the old values stored in zookeeper orphaned and reverting to defaults."
  [name default & options]
  (let [nstr (str name)]
    `(def ~name (>- (ns-path ~nstr) ~default ~@options))))
