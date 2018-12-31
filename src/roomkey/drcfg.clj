(ns roomkey.drcfg
  "Dynamic Distributed Run-Time configuration"
  (:require [roomkey.zref :as z]
            [roomkey.zclient :as zclient]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojure.tools.macro :refer [name-with-attributes]]))

;;; USAGE:  see /roomkey/README.md

(def ^:dynamic *client* (zclient/create))

(def zk-prefix "drcfg")

(defmacro ns-path [n]
  `(str "/" (str *ns*) "/" ~n))

(defn open
  ([hosts] (open *client* hosts))
  ([client hosts] (open client hosts nil))
  ([client hosts scope]
   ;; avoid a race condition by having mux wired up before feeding in client events
   (zclient/open client (string/join "/" (filter identity [hosts zk-prefix scope])) 16000)))

(defn db-initialize!
  "Synchronously initialize a fresh zookeeper database with a root node"
  ([hosts] (db-initialize! hosts nil))
  ([hosts scope]
   (let [root (string/join "/" (filter identity ["" zk-prefix scope]))]
     (log/infof "Creating root drcfg node %s for connect string %s" root hosts)
     (with-open [zc (zclient/open *client* hosts 5000)]
       (zclient/create-all zc root {:persistent? true})))))

(defn >-
  "Create a config reference with the given name (must be fully specified,
  including leading slash) and default value and record it for future connecting"
  [name default & options]
  {:pre [] :post [(instance? clojure.lang.IRef %)]}
  (let [z (apply z/create name default *client* options)]
    (add-watch z :logger (fn [k r o n] (log/tracef "Value of %s update: old: %s; s" name o n)))
    z))

(defmacro def>-
  "Def a config reference with the given name.  The current namespace will be
  automatically prepended to create the zookeeper path -when refactoring, note
  that the namespace may change, leaving the old values stored in zookeeper
  orphaned and reverting to the default value."
  [name & body]
  (let [[sym [default & {m :meta :as options}]] (name-with-attributes name body)
        nstr (str sym)
        m (merge (meta sym) m)
        options (select-keys options [:validator])]
    `(def ~sym (let [bpath# (ns-path ~nstr)
                     bref# (apply >- bpath# ~default (mapcat identity ~options))]
                 (when ~m (>- (str bpath# "/.metadata") ~m))
                 bref#))))
