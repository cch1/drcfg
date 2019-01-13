(ns roomkey.drcfg
  "Dynamic Distributed Run-Time configuration"
  (:require [roomkey.zref :as zref]
            [roomkey.znode :as znode]
            [roomkey.zclient :as zclient]
            [clojure.core.async :as async]
            [clojure.string :as string]
            [clojure.tools.logging :as log]))

;;; USAGE:  see /roomkey/README.md

(def ^:dynamic *client* (zclient/create))
(def ^:dynamic *root* (znode/create-root *client*))

(def zk-prefix "drcfg")

(defmacro ns-path [n]
  `(str "/" (str *ns*) "/" ~n))

(defn open
  ([hosts] (open *client* hosts))
  ([client hosts] (open client hosts nil))
  ([client hosts scope]
   ;; avoid a race condition by having mux wired up before feeding in client events
   (let [connect-string (string/join "/" (filter identity [hosts zk-prefix scope]))]
     (log/infof "Opening client [%s] connection to %s" client connect-string)
     (zclient/open client connect-string 16000))))

(defn db-initialize!
  "Synchronously initialize a fresh zookeeper database with a root node"
  ([connect-string] (db-initialize! connect-string nil))
  ([connect-string scope] (db-initialize! connect-string scope 5000))
  ([connect-string scope timeout]
   (let [client (zclient/create) ; fresh... nobody should be watching
         zroot (znode/create-root client) ; fresh... no children
         root-path (string/join "/" (filter identity ["" zk-prefix scope]))
         drcfg-root (znode/add-descendant zroot root-path ::root)
         data (async/pipe drcfg-root
                          (async/chan 1 (comp (filter (comp #{:roomkey.znode/datum :roomkey.znode/actualized!} :roomkey.znode/type))
                                              (map :roomkey.znode/type)))
                          false)]
     (with-open [zclient (zclient/open client connect-string timeout)]
       (when-let [result (async/<!! (async/go-loop []
                                      (async/alt! data ([event] (case event
                                                                  :roomkey.znode/actualized! (do (log/infof "Database initialized") (recur))
                                                                  :roomkey.znode/datum true))
                                                  (async/timeout 10000) ([_] (log/warnf "Timed out waiting for database initialization") false))))]
         (log/infof "Database ready at %s [%s]" connect-string (str drcfg-root))
         (Thread/sleep 1000) ; let ZNode acquisition settle down solely to avoid innocuous "Lost connection while processing" errors.
         result)))))

(defn >-
  "Create a config reference with the given name (must be fully specified,
  including leading slash) and default value and record it for future connecting"
  [name default & options]
  {:pre [] :post [(instance? clojure.lang.IRef %)]}
  (let [z (apply zref/create *root* name default options)]
    (add-watch z :logger (fn [k r o n] (log/tracef "Value of %s update: old: %s; s" name o n)))
    z))

(defmacro def>-
  "Def a config reference with the given name.  The current namespace will be
  automatically prepended to create the zookeeper path -when refactoring, note
  that the namespace may change, leaving the old values stored in zookeeper
  orphaned and reverting to the default value."
  [name default & options]
  (let [nstr (str name)
        {m :meta :as o} (apply hash-map options)]
    `(def ~name (let [bpath# (ns-path ~nstr)
                      bref# (apply >- bpath# ~default (mapcat identity
                                                              (select-keys (hash-map ~@options) [:validator])))]
                  (when ~m (>- (str bpath# "/.metadata") ~m))
                  bref#))))
