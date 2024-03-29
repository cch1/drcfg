(ns zk.drcfg
  "Dynamic Distributed Run-Time configuration"
  (:require [zk.zref :as zref]
            [zk.node :as znode]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]))

;;; USAGE:  see README.md

(def ^:dynamic *root* (znode/new-root))

(let [default-prefix "/drcfg"]
  (defn db-initialize!
    "Synchronously initialize a fresh drcfg database in `prefix` at the ZooKeeper cluster identified by `connect-string`"
    [connect-string & {:keys [prefix timeout] :or {prefix default-prefix timeout 8000} :as options}]
    (let [drcfg-root (znode/new-root prefix)
          root drcfg-root
          data (async/pipe root
                           (async/chan 1 (comp (map ::znode/type)
                                               (filter #{::znode/exists ::znode/created!})))
                           false)]
      (with-open [^java.lang.AutoCloseable chandle (znode/open drcfg-root connect-string :timeout timeout)]
        (assert (deref chandle timeout false) "Client failed to establish a connection")
        (when (async/alt!! (async/go
                             (async/alt! data ([event] (do (case event
                                                             ::znode/created! (log/infof "Database initialized")
                                                             ::znode/exists (log/infof "Database already initialized")))))) true
                           (async/timeout (* 2 timeout)) ([_] (log/warnf "Timed out waiting for database initialization") false))
          (log/infof "Database ready at %s [%s]" connect-string (str root))
          root))))

  (defn open
    "Open a connection with `root` bound to the node at `prefix` in the ZooKeeper cluster defined by `connect-string`"
    ([connect-string] (open connect-string *root*))
    ([connect-string root & {:keys [prefix timeout] :or {prefix default-prefix timeout 8000} :as options}]
     (let [connect-string (str connect-string prefix)]
       (log/infof "Opening client [%s] connection to %s" root connect-string)
       (znode/open root connect-string :timeout timeout)))))

(defn ^zk.zref.ZRef >-
  "Create a config reference with the given name (must be fully specified,
  including leading slash) and default value and record it for future connecting"
  [name default & options]
  {:pre [] :post [(instance? clojure.lang.IRef %)]}
  (let [z (apply zref/create *root* name default options)]
    (add-watch z :logger (fn [k r o n] (log/tracef "Value of %s update: old: %s; new %s" name o n)))
    z))

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
        options (mapcat identity (select-keys options [:validator]))]
    `(let [bpath# (str (znode/->path [*ns* '~symb]))
           ^zk.zref.ZRef z# (>- bpath# ~default ~@options)]
       (when ~m (znode/add-descendant (.znode z#) "/.metadata" ~m))
       (def ~symb z#))))
