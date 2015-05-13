(ns roomkey.drcfg
  "Dynamic Distributed Run-Time configuration"
  (:import org.apache.zookeeper.KeeperException)
  (:require [roomkey.zkutil :as zk]
            [roomkey.drcfg.client :as client]
            [clojure.string :as string]
            [clojure.tools.logging :as log]))

;;; USAGE:  see /roomkey/README.md

(def ^:dynamic *client* (promise))
(def ^:dynamic *registry*
  ;; Use an agent to serialize ops and ensure that linking is never retried due to concurrent registering
  (agent {} :error-handler (fn [_ e] (log/warn e "The drcfg registry agent threw an exception"))))

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
  (get-or-create-znode client name @la)
  (zk/set-metadata client name (or (meta la) {}))
  (zk/watch client name (fn []
                          (let [new-value (zk/nget client name)]
                            (try-sync la new-value name))))
  (log/tracef "Created new watched-znode %s" name))

;; NB: this function runs in the current thread and may block!
;; NB: the name parameter must be properly namespaced!
(defn drset!
  "Update a distributed config value.  Name must be fully specified,
  including the leading slash and namespace"
  [name v]
  (zk/nset @*client* name v))

(defn status
  []
  (let [realized-registry (reduce (fn [memo [p [la linked?]]]
                                    (conj memo [p (boolean linked?) @la]))
                                  [] @*registry*)]
    (sort-by (comp second first) realized-registry)))

(let [truncl (fn [n s] (if (<= (.length s) n) s (string/reverse (subs (string/reverse s) 0 n))))]
  (defn status-report
    [& {:keys [formatter]
        :or {formatter (fn [[p l? v]] (format "%1.1s %32.32s %-45.45s" (if l? " " "*") (truncl 32 p) (pr-str v)))}}]
    (string/join "\n" (map formatter (status)))))

(defn candidates-for-deletion
  [client max-age]
  (let [day (* 1000 60 60 24)
        now (System/currentTimeMillis)]
    (zk/visit client
              (fn [acc node path]
                (let [depth (count path)
                      ttouched (zk/touched-at client node)
                      age (and ttouched (float (/ (- now ttouched) 1000 60 60 24)))]
                  (log/infof "Considering %d %03.1f %s" depth age node)
                  (if (or (and (> depth 1) (nil? age)) (and age (> age max-age)))
                    (assoc acc node age)
                    (do (log/debugf "Found fresh (%2.2f) node: %s" age node)
                        acc))))
              {}
              "/")))

(defn- link
  [client n [la linked?]]
  (if linked?
    [la linked?]
    (do (watch-znode client n la)
        [la true])))

(defn- link-all!
  [client registry]
  (reduce-kv (fn [memo n v] (assoc memo n (link client n v)))
          {}
          registry))

(defn- register
  [registry n la]
  (assoc registry n [la false]))

(defn connect-with-wait!
  "Initiate a connection to the zookeeper service and link all previously
  defined local references, waiting for the connection and linkage to complete
  before returning"
  [hosts & [timeout]]
  (let [c (client/connect hosts)]
    (send *registry* (partial link-all! c))
    (let [result (await-for (or timeout 20000) *registry*)]
      (deliver *client* c)
      result)))

(defn connect!
  "Return a future representing the blocking connect-with-wait!"
  [hosts]
  (future (connect-with-wait! hosts)))

(defn >-
  "Create a config reference with the given name (must be fully specified,
  including leading slash and namespace) and default value and record it for
  future linking to a distributed ref. "
  [name default & options]
  {:pre [(re-matches #"/.+" name)] :post [(instance? clojure.lang.Atom %)]}
  (zk/serialize default) ;; throws exception if object is not serializable
  (let [la (apply atom default options)]
    (add-watch la :logger (fn [k r o n] (log/debugf "Watched value of %s update: old: %s; new: %s" name o n)))
    (if (realized? *client*)
      (log/errorf "New drcfg reference %s defined after connect -- will not be linked to zookeeper" name)
      (send *registry* register name la))
    la))

(defmacro def>-
  "Def a config reference with the given atom, using the atom name as the
  leaf name, and automatically prepending the namespace to determine the
  zookeeper path.  NB: when refactoring, note that the namespace may change,
  leaving the old values stored in zookeeper orphaned and reverting to defaults."
  [name default & options]
  (let [nstr (str name)]
    `(def ~name (>- (ns-path ~nstr) ~default ~@options))))
