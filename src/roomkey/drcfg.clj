(ns roomkey.drcfg
  "Dynamic Distributed Run-Time configuration"
  (:require [roomkey.zref :as zref]
            [roomkey.znode :as znode]
            [clojure.core.async :as async]
            [clojure.string :as string]
            [clojure.tools.logging :as log]))

;;; USAGE:  see /roomkey/README.md

(def ^:dynamic *root* (znode/new-root))

(def zk-prefix "drcfg")

(defn open
  "Open a connection with `root` in `scope` to the ZooKeeper cluster defined by `hosts`"
  ([hosts] (open hosts *root*))
  ([hosts root] (open hosts root nil))
  ([hosts root scope] (open hosts root scope 8000))
  ([hosts root scope timeout]
   (let [connect-string (string/join "/" (filter identity [hosts zk-prefix scope]))]
     (log/infof "Opening client [%s] connection to %s" root connect-string)
     (znode/open root connect-string timeout))))

(defn db-initialize!
  "Synchronously initialize a fresh drcfg database in `scope` at the ZooKeeper cluster identified by `connect-string`"
  ([connect-string] (db-initialize! connect-string nil))
  ([connect-string scope] (db-initialize! connect-string scope 8000))
  ([connect-string scope timeout]
   (let [drcfg-root (znode/new-root (str "/" zk-prefix))
         root (if scope (znode/add-descendant drcfg-root (str "/" scope) ::scoped-root) drcfg-root)
         data (async/pipe root
                          (async/chan 1 (comp (map :roomkey.znode/type)
                                              (filter #{:roomkey.znode/watch-start :roomkey.znode/exists :roomkey.znode/created!})))
                          false)]
     (with-open [^java.io.Closeable _ (znode/open drcfg-root connect-string timeout)]
       (when (async/alt!! (async/go
                            (assert (= (async/<! data) :roomkey.znode/watch-start))
                            (znode/create! drcfg-root)
                            (async/alt! data ([event] (do (case event
                                                            :roomkey.znode/created! (log/infof "Database initialized")
                                                            :roomkey.znode/exists (log/infof "Database already initialized")))))) true
                          (async/timeout (* 2 timeout)) ([_] (log/warnf "Timed out waiting for database initialization") false))
         (log/infof "Database ready at %s [%s]" connect-string (str root))
         root)))))

(defn ^roomkey.zref.ZRef >-
  "Create a config reference with the given name (must be fully specified,
  including leading slash) and default value and record it for future connecting"
  [name default & options]
  {:pre [] :post [(instance? clojure.lang.IRef %)]}
  (let [z (apply zref/create *root* name default options)]
    (add-watch z :logger (fn [k r o n] (log/tracef "Value of %s update: old: %s; new %s" name o n)))
    z))

(defmacro ns-path [n]
  `(str "/" (str *ns*) "/" ~n))

(defmacro ^:deprecated def>-
  "Def a config reference with the given name.  The current namespace will be
  automatically prepended to create the zookeeper path -when refactoring, note
  that the namespace may change, leaving the old values stored in zookeeper
  orphaned and reverting to the default value."
  [name default & options]
  (let [nstr (str name)
        {m :meta :as o} (apply hash-map options)
        options (mapcat identity
                        (select-keys (apply hash-map options) [:validator]))]
    `(let [bpath# (ns-path ~nstr)]
       (when ~m (>- (str bpath# "/.metadata") ~m))
       (def ~name (apply >- bpath# ~default ~options)))))

;; Reference: https://stackoverflow.com/questions/25478158/how-do-i-use-clojure-tools-macro-name-with-attributes
(defn name-with-attributes-and-options
  "To be used in macro definitions.
   Handles an optional docstring, an optional attribute map and optional trailing keyword/value option pairs
   for a name and its defined value. If trailing pairs start with a keyword, they are added to the
   option map and removed from the argument list.  The last of the remaining arguments becomes the value and
   it is removed from the arguments list.   If the first of the remaining arguments is a string,
   it is added as a docstring to name and removed from the argument
   list. If the first of the then remaining arguments is a map, its entries are
   added to the name's metadata map and the map is removed from the
   argument list. The return value is a vector containing the name
   with its extended metadata map, the value for name, and the options map."
  [name args]
  (let [[args opts] (let [[opt-prs args] (split-with (fn [[n :as pair]] (and (keyword? n) (= 2 (count pair))))
                                                     (map reverse (partition-all 2 (reverse args))))]
                      [(mapcat identity (reverse args)) (into {} (map vec opt-prs))])
        [args v] (let [[v & rargs] (reverse args)] [(reverse rargs) v])
        [docstring args] (if (string? (first args))
                           [(first args) (next args)]
                           [nil args])
        [attr args] (if (map? (first args))
                      [(first args) (next args)]
                      [{} args])
        attr (if docstring
               (assoc attr :doc docstring)
               attr)
        attr (if (meta name)
               (conj (meta name) attr)
               attr)]
    (assert (empty? args) "Invalid arguments")
    [(with-meta name attr) v opts]))

(defmacro def>
  "Def a config reference with the given name and default value.  The current namespace will be automatically prepended
  to create the zookeeper path -when refactoring, note that the namespace may change, leaving the old values stored in
  zookeeper orphaned and reverting to the default value.  Documentation and var metadata can be provided in the usual way
  and are stored in a related ZooKeeper node.  Options (currently just `:validator`) are provided after the default value."
  {:arglists '([symbol doc-string? attr-map? default options?])}
  [symb & args]
  (let [[symb default options] (name-with-attributes-and-options symb args)
        m (meta symb)
        options (vec (mapcat identity (select-keys options [:validator])))]
    `(let [bpath# (str "/" (string/replace (str *ns*) #"\." "/") "/" '~symb)
           ^roomkey.zref.ZRef z# (apply >- bpath# ~default ~options)]
       (when ~m (znode/add-descendant (.znode z#) "/.metadata" ~m))
       (def ~symb z#))))
