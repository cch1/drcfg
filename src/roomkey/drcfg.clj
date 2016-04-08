(ns roomkey.drcfg
  "Dynamic Distributed Run-Time configuration"
  (:require [roomkey.zref :as z]
            [clojure.string :as string]
            [clojure.tools.logging :as log]))

;;; USAGE:  see /roomkey/README.md

(def ^:dynamic *client* (promise))
(def ^:dynamic *registry* (ref #{}))
(def zk-prefix "/drcfg")

(defmacro ns-path [n]
  `(str "/" (str *ns*) "/" ~n))

(defn status
  []
  (let [realized-registry (reduce (fn [memo z]
                                    (conj memo [(z/path z) (z/connected? z) @z]))
                                  [] @*registry*)]
    (sort-by (comp second first) realized-registry)))

(let [truncl (fn [n s] (if (<= (.length s) n) s (string/reverse (subs (string/reverse s) 0 n))))]
  (defn status-report
    [& {:keys [formatter]
        :or {formatter (fn [[p l? v]] (format "%1.1s %32.32s %-45.45s" (if l? " " "*") (truncl 32 p) (pr-str v)))}}]
    (string/join "\n" (map formatter (status)))))

(defn disconnect
  []
  (doseq [z @*registry*]
    (when (z/connected? z)
      (.zDisconnect z))))

(defn connect-with-wait!
  "Initiate a connection to the zookeeper service and link all previously
  defined local references, waiting for the connection and linkage to complete
  before returning"
  [hosts & [timeout]]
  (let [c (z/client (str hosts zk-prefix))]
    (doseq [z @*registry*] (if (z/connected? z)
                             (log/warnf "Attempting to connect %s while already connected." (z/path z))
                             (z/connect c z)))
    (let [result (deref *registry*)]
      (deliver *client* c)
      result)))

(defn connect!
  "Return a future representing the blocking connect-with-wait!"
  [hosts]
  (future (connect-with-wait! hosts)))

(defn >-
  "Create a config reference with the given name (must be fully specified,
  including leading slash and namespace) and default value and record it for
  future connecting"
  [name default & options]
  {:pre [(re-matches #"/.+" name)] :post [(instance? clojure.lang.IRef %)]}
  (let [{m :meta :as o} (apply hash-map options)
        z (apply z/zref name default (mapcat identity (select-keys o [:validator])))
        zm (z/zref (str name "/.metadata") m)]
    (add-watch z :logger (fn [k r o n] (log/debugf "Watched value of %s update: old: %s; new: %s" name o n)))
    (if (realized? *client*)
      (log/errorf "New drcfg reference %s defined after connect -- will not be linked to zookeeper" name)
      (dosync (alter *registry* conj z zm)))
    z))

(defmacro def>-
  "Def a config reference with the given atom, using the atom name as the
  leaf name, and automatically prepending the namespace to determine the
  zookeeper path.  NB: when refactoring, note that the namespace may change,
  leaving the old values stored in zookeeper orphaned and reverting to defaults."
  [name default & options]
  (let [nstr (str name)]
    `(def ~name (>- (ns-path ~nstr) ~default ~@options))))
