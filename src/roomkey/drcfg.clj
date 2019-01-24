(ns roomkey.drcfg
  "Dynamic Distributed Run-Time configuration"
  (:require [roomkey.zref :as zref]
            [roomkey.znode :as znode]
            [clojure.core.async :as async]
            [clojure.string :as string]
            [clojure.tools.macro :refer [name-with-attributes]]
            [clojure.tools.logging :as log]))

;;; USAGE:  see /roomkey/README.md

(def ^:dynamic *root* (znode/create-root))

(def zk-prefix "drcfg")

(defn open
  "Open a connection with `root` in `scope` to the ZooKeeper cluster defined by `hosts`"
  ([hosts] (open hosts *root*))
  ([hosts root] (open hosts root nil))
  ([hosts root scope]
   ;; avoid a race condition by having mux wired up before feeding in client events
   (let [connect-string (string/join "/" (filter identity [hosts zk-prefix scope]))]
     (log/infof "Opening client [%s] connection to %s" root connect-string)
     (znode/open root connect-string 16000))))

(defn db-initialize!
  "Synchronously initialize a fresh drcfg database in `scope` at the ZooKeeper cluster identified by `connect-string`"
  ([connect-string] (db-initialize! connect-string nil))
  ([connect-string scope] (db-initialize! connect-string scope 5000))
  ([connect-string scope timeout]
   (let [drcfg-root (znode/create-root (str "/" zk-prefix))
         data (async/pipe drcfg-root
                          (async/chan 1 (comp (filter (comp #{:roomkey.znode/datum :roomkey.znode/created!} :roomkey.znode/type))
                                              (map :roomkey.znode/type)))
                          false)
         root (if-not scope drcfg-root (znode/add-descendant drcfg-root (str "/" scope) ::scoped-root))]
     (with-open [zclient (znode/open root connect-string timeout)]
       (when-let [result (async/<!! (async/go-loop []
                                      (async/alt! data ([event] (case event
                                                                  :roomkey.znode/created! (do (log/infof "Database initialized") (recur))
                                                                  :roomkey.znode/datum true))
                                                  (async/timeout 10000) ([_] (log/warnf "Timed out waiting for database initialization") false))))]
         (log/infof "Database ready at %s [%s]" connect-string (str root))
         (Thread/sleep 1000) ; let ZNode acquisition settle down solely to avoid innocuous "Lost connection while processing" errors.
         root)))))

(defn >-
  "Create a config reference with the given name (must be fully specified,
  including leading slash) and default value and record it for future connecting"
  [name default & options]
  {:pre [] :post [(instance? clojure.lang.IRef %)]}
  (let [z (apply zref/create *root* name default options)]
    (add-watch z :logger (fn [k r o n] (log/tracef "Value of %s update: old: %s; s" name o n)))
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
(defmacro def>
  "Def a config reference with the given name and default value.  The current namespace will be automatically prepended
  to create the zookeeper path -when refactoring, note that the namespace may change, leaving the old values stored in
  zookeeper orphaned and reverting to the default value.  Documentation and var metadata can be provided in the usual way
  and are stored in a related ZooKeeper node.  Options (currently just `:validator`) are provided after the default value."
  {:arglists '([symbol doc-string? attr-map? default options?])}
  [symb & args]
  (let [[symb [default & options]] (name-with-attributes symb args)
        m (meta symb)
        options (mapcat identity
                        (select-keys (apply hash-map options) [:validator]))]
    `(let [bpath# (str "/" (string/replace (str *ns*) #"\." "/") "/" '~symb)
           z# (apply >- bpath# ~default ~options)]
       (when ~m (znode/add-descendant (.znode z#) "/.metadata" ~m))
       (def ~symb z#))))
