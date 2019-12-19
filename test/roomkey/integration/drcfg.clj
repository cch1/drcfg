(ns roomkey.integration.drcfg
  (:require [roomkey.drcfg :refer :all]
            [roomkey.zref :as zref]
            [roomkey.znode :as znode]
            [roomkey.zclient :as zclient]
            [zookeeper :as zoo]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [clojure.core.async :as async]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]])
  (:import [org.apache.curator.test TestingServer]))

(def bogus-host "127.1.1.1:9999")
(def ^:dynamic *connect-string* (.getConnectString (TestingServer. true)))
(def sandbox "sandbox")
(def ^:dynamic *zc* (zoo/connect *connect-string*))

(let [counter (atom 0)]
  (defn next-path
    []
    (str "/n/" (swap! counter inc))))

(defn- abs-path
  ([path]
   (str (string/join "/" ["" zk-prefix sandbox]) path)))

(defn create-path!
  [path value]
  (let [data ((comp #'znode/encode #'znode/serialize) value)]
    (zoo/create-all *zc* path :persistent? true :data data)
    (zoo/set-data *zc* path data 0)))

(defn set-path!
  [path value]
  (zoo/set-data *zc* path ((comp #'znode/encode #'znode/serialize) value) -1))

(defn sync-path
  "Wait up to timeout milliseconds for v to appear at path"
  ([timeout path v]
   (let [vbs (seq ((comp #'znode/encode #'znode/serialize) v))]
     (loop [t timeout]
       (assert (pos? t) (format "Timed out after waiting %dms for %s to appear at %s" timeout v path))
       (if (and (zoo/exists *zc* path) (= vbs (seq (:data (zoo/data *zc* path)))))
         true
         (do (Thread/sleep 200)
             (recur (- t 200))))))))

(defmacro with-awaited-connection
  [zroot connect-string sandbox & body]
  `(let [c# (async/chan 1)
         z# ~zroot]
     (async/tap (.client z#) c#)
     ;; TODO: drain client channel before opening.
     (with-open [client# (open ~connect-string z# ~sandbox)]
       (let [event# (first (async/<!! c#))]
         (assert (= ::zclient/started event#)))
       (let [event# (first (async/<!! c#))]
         (assert (= ::zclient/connected event#)))
       ~@body)))

(defchecker refers-to [expected]
  (checker [actual] (extended-= (deref actual) expected)))

(defchecker eventually-refers-to [timeout expected]
  (checker [actual]
           (loop [t timeout]
             (when (pos? t)
               (if-let [result (extended-= (deref actual) expected)]
                 result
                 (do (Thread/sleep 200)
                     (recur (- t 200))))))))

(defchecker eventually-vrefers-to [timeout expected]
  (checker [actual]
           (loop [t timeout]
             (when (pos? t)
               (if-let [result (extended-= (.vDeref actual) expected)]
                 result
                 (do (Thread/sleep 200)
                     (recur (- t 200))))))))

(background [(around :contents (do ?form))
             (around :facts (with-open [server (TestingServer.)
                                        zc (zoo/connect (.getConnectString server))]
                              (binding [*zc* zc
                                        *connect-string* (.getConnectString server)
                                        roomkey.drcfg/*root* (znode/new-root)]
                                (db-initialize! *connect-string* sandbox 5000)
                                ?form)))])

(fact "Can initialize a fresh database" true => true)

(fact "can create a config value"
      (>- (next-path) "my-default-value" :validator string?) => (refers-to "my-default-value"))

(fact "can initialize a fresh database"
      (with-open [server (TestingServer.)]
        (db-initialize! (.getConnectString server) sandbox 5000) => truthy)
      (with-open [server (TestingServer.)]
        (db-initialize! (.getConnectString server)) => truthy))

(fact "Can open and close connections regardless of viability"
      (with-open [c (open bogus-host roomkey.drcfg/*root*)]
        c => (partial instance? roomkey.znode.Closeable))
      (with-open [c (open *connect-string* roomkey.drcfg/*root*)]
        c => (partial instance? roomkey.znode.Closeable)))

(fact "Slaved config value gets updated post-connect"
      (let [p "/n/p0001" ; (next-path)
            abs-path (abs-path p)
            la (>- p "V0" :validator string?)]
        (with-awaited-connection *root* *connect-string* sandbox
          (sync-path 5000 abs-path "V0")
          (set-path! abs-path "V1")
          la => (eventually-refers-to 10000 "V1"))))

(fact "Slaved config value at deep path gets updated post-connect"
      (let [p "/N/0" ; (next-path)
            abs-path (abs-path p)
            la (>- p "V0" :validator string?)]
        (with-awaited-connection *root* *connect-string* sandbox
          (sync-path 5000 abs-path "V0")
          (set-path! abs-path "V1")
          la => (eventually-refers-to 10000 "V1"))))

(fact "Children don't intefere with their parents"
      (let [n0 (next-path)
            n1 (str n0 "/child")
            abs-path0 (abs-path n0)
            abs-path1 (abs-path n1)
            la0 (>- n0 0 :validator integer?)
            la1 (>- n1 1 :validator integer?)]
        (with-awaited-connection *root* *connect-string* sandbox
          (sync-path 5000 abs-path0 0)
          (sync-path 5000 abs-path1 1)
          (set-path! abs-path0 1)
          la0 => (eventually-refers-to 10000 1))))

(fact "Serialization works"
      (let [n (next-path)
            abs-path (abs-path n)
            la (>- n 0 :validator integer?)]
        (with-awaited-connection *root* *connect-string* sandbox
          (sync-path 5000 abs-path 0)
          (set-path! abs-path 1)
          la => (eventually-refers-to 10000 1))))

(fact "Validator prevents updates to local atom"
      (let [n (next-path)
            abs-path (abs-path n)
            la (>- n 0 :validator integer?)]
        (with-awaited-connection *root* *connect-string* sandbox
          (sync-path 5000 abs-path 0)
          (set-path! abs-path "x")
          (sync-path 5000 abs-path "x")
          la => (refers-to 0))))

(fact "Without a validator, heterogenous values are allowed"
      (let [n (next-path)
            abs-path (abs-path n)
            la (>- n 0)]
        (with-awaited-connection *root* *connect-string* sandbox
          (sync-path 5000 abs-path 0)
          (set-path! abs-path "x")
          la => (eventually-refers-to 10000 "x")
          (set-path! abs-path false)
          la => (eventually-refers-to 10000 false)
          (set-path! abs-path true)
          la => (eventually-refers-to 10000 true)
          (set-path! abs-path nil)
          la => (eventually-refers-to 10000 nil)
          (set-path! abs-path #inst "2015-05-08T19:35:09.371-00:00")
          la => (eventually-refers-to 10000 #inst "2015-05-08T19:35:09.371-00:00"))))

(facts "pre-configured value gets applied"
       (let [n (next-path)
             abs-path (abs-path n)]
         (create-path! abs-path "value")
         (create-path! (str abs-path "/.metadata") {:doc "My Doc"})
         (let [la (>- n "default-value" :meta {:doc "My Default Doc"})]
           (with-awaited-connection *root* *connect-string* sandbox
             la => (eventually-refers-to 10000 "value")))))

(fact "Can redef old-style zref for proper REPL-based development"
      (def>- drref 12 :meta {:doc "My Documentation String"})
      (let [p (abs-path "/roomkey.integration.drcfg/drref")]
        (with-awaited-connection *root* *connect-string* sandbox
          (sync-path 5000 p 12)
          @drref => 12))
      (def>- drref 12 :meta {:doc "My Documentation String"})
      (ns-unmap (symbol (str *ns*)) 'drref))

(fact "Can redef new-style zref for proper REPL-based development"
      (def> ^:private drref "docstring" 12)
      (let [p (abs-path "/roomkey/integration/drcfg/drref")]
        (with-awaited-connection *root* *connect-string* sandbox
          (sync-path 5000 p 12)
          @drref => 12
          (def> ^:private drref "docstring" 12)))
      (ns-unmap (symbol (str *ns*)) 'drref))
