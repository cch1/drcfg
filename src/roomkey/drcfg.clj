(ns roomkey.drcfg
  "Dynamic Distributed Run-Time configuration"
  (:require [roomkey.zref :as z]
            [roomkey.zclient :as zclient]
            [zookeeper :as zoo]
            [clojure.core.async :as async]
            [clojure.string :as string]
            [clojure.tools.logging :as log]))

;;; USAGE:  see /roomkey/README.md

(def ^:dynamic *registry* (atom #{} :validator set?))
(def zk-prefix "drcfg")

(defmacro ns-path [n]
  `(str "/" (str *ns*) "/" ~n))

(defn- link
  "Link a zref to a client-supplying channel"
  [z ch]
  ;; Need a lightweight finite-state machine abstraction here...
  (async/go-loop []
    (if-let [[ev client :as m] (async/<! ch)]
      (do
        (log/tracef "Drcfg go-loop for zref %s received: %s" (.path z) m)
        (case ev
          (:ConnectedReadOnly :SyncConnected) (.zConnect z client)
          :Disconnected (.zDisconnect z)
          (log/warnf "[%s] Received unexpected message: " m))
        (recur))
      (do ;; client input channel has closed, we're outta here
        (log/debugf "Client input channel has closed for %s, shutting down" (.path z))
        (.zDisconnect z)))))

(defn open
  ([hosts] (open (deref *registry*) hosts))
  ([registry hosts] (open registry hosts nil))
  ([registry hosts scope]
   (let [ch-source (async/chan)
         mux (async/mult ch-source)]
     (doseq [z registry]
       (let [ch-sink (async/chan 1)]
         (link z ch-sink)
         (async/tap mux ch-sink)))
     ;; avoid a race condition by having mux wired up before feeding in client events
     (zclient/create (string/join "/" (filter identity [hosts zk-prefix scope])) ch-source))))

(defn db-initialize!
  "Synchronously initialize a fresh zookeeper database with a root node"
  ([hosts] (db-initialize! hosts nil))
  ([hosts scope]
   (let [root (string/join "/" (filter identity ["" zk-prefix scope]))]
     (log/infof "Creating root drcfg node %s for connect string %s" root hosts)
     (with-open [zc (zoo/connect hosts)]
       (zoo/create-all zc root :persistent? true)))))

(defn ^:deprecated connect-with-wait!
  "Open a connection to the zookeeper service and link previously defined local references"
  [hosts & [scope]]
  (swap! *registry* (fn [r]
                      (when-let [c (::client (meta r))]
                        (.close c)) ; close any existing client
                      (with-meta r {::client (open r hosts scope)}))))

(def ^:deprecated connect! connect-with-wait!)

(defn >-
  "Create a config reference with the given name (must be fully specified,
  including leading slash) and default value and record it for future connecting"
  [name default & options]
  {:pre [] :post [(instance? clojure.lang.IRef %)]}
  (let [z (apply z/zref name default options)]
    (add-watch z :logger (fn [k r o n] (log/tracef "Value of %s update: old: %s; s" name o n)))
    (swap! *registry* conj z)
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
