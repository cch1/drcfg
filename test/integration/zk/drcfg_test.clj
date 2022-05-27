(ns integration.zk.drcfg-test
  (:require [zk.drcfg :refer :all]
            [zk.zref :as zref]
            [zk.node :as znode]
            [zk.client :as zclient]
            [zookeeper :as zoo]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [clojure.core.async :as async]
            [clojure.test :refer [use-fixtures deftest testing is assert-expr]]
            [testit.core :refer :all]
            [integration.zk.test-helper :as th :refer [streams *stat? initial-stat? with-metadata?]])
  (:import [org.apache.curator.test TestingServer]))

(def bogus-host "127.1.1.1:9999")
(def ^:dynamic *connect-string*)
(def ^:dynamic *zc*)

(let [counter (atom 0)]
  (defn next-path
    []
    (str "/n/" (swap! counter inc))))

(defn- abs-path
  ([path]
   (str "/drcfg" path)))

(defn create-path!
  [path value]
  (let [data ((comp #'znode/encode #'znode/serialize) value)]
    (zoo/create-all *zc* path :persistent? true :data data)))

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
  [zroot connect-string & body]
  `(with-open [chandle# (open ~connect-string ~zroot)]
     (assert (deref chandle# 8000 nil) "No connection established")
     ~@body))

(use-fixtures :each (fn [f]
                      (with-open [server (TestingServer.)
                                  zc (zoo/connect (.getConnectString server))]
                        (binding [*zc* zc
                                  *connect-string* (.getConnectString server)
                                  zk.drcfg/*root* (znode/new-root)]
                          (db-initialize! *connect-string*)
                          (f)))))

(deftest create-config-value
  (facts (deref (>- (next-path) "my-default-value" :validator string?)) => "my-default-value"))

(deftest initialize-fresh-database
  (with-open [server (TestingServer.)]
    (is (db-initialize! (.getConnectString server))))
  (with-open [server (TestingServer.)]
    (is (db-initialize! (.getConnectString server) :prefix "/mydrcfg"))))

(deftest open-and-close-regardless-of-viability
  (with-open [c (open bogus-host zk.drcfg/*root*)]
    (facts c => (partial instance? java.lang.AutoCloseable)))
  (with-open [c (open *connect-string* zk.drcfg/*root*)]
    (facts c => (partial instance? java.lang.AutoCloseable))))

(deftest slaved-config-value-gets-updated-post-connect
  (let [p "/n/p0001" ; (next-path)
        abs-path (abs-path p)
        la (>- p "V0" :validator string?)]
    (with-awaited-connection *root* *connect-string*
      (sync-path 5000 abs-path "V0")
      (set-path! abs-path "V1")
      (facts (deref la) =eventually=> "V1"))))

(deftest slaved-config-value-at-deep-path-gets-updated-post-connect
  (let [p "/N/0" ; (next-path)
        abs-path (abs-path p)
        la (>- p "V0" :validator string?)]
    (with-awaited-connection *root* *connect-string*
      (sync-path 5000 abs-path "V0")
      (set-path! abs-path "V1")
      (facts (deref la) =eventually=> "V1"))))

(deftest child-zrefs-do-not-interfere-with-their-parents
  (let [n0 (next-path)
        n1 (str n0 "/child")
        abs-path0 (abs-path n0)
        abs-path1 (abs-path n1)
        la0 (>- n0 0 :validator integer?)
        la1 (>- n1 1 :validator integer?)]
    (with-awaited-connection *root* *connect-string*
      (sync-path 5000 abs-path0 0)
      (sync-path 5000 abs-path1 1)
      (set-path! abs-path0 1)
      (facts (deref la0) =eventually=> 1))))

(deftest serialization
  (let [n (next-path)
        abs-path (abs-path n)
        la (>- n 0 :validator integer?)]
    (with-awaited-connection *root* *connect-string*
      (sync-path 5000 abs-path 0)
      (set-path! abs-path 1)
      (facts (deref la) =eventually=> 1))))

(deftest validator-prevents-updates-to-local-zref
  (let [n (next-path)
        abs-path (abs-path n)
        la (>- n 0 :validator integer?)]
    (with-awaited-connection *root* *connect-string*
      (sync-path 5000 abs-path 0)
      (set-path! abs-path "x")
      (sync-path 5000 abs-path "x")
      (is (zero? (deref la))))))

(deftest without-validator-heterogenous-values-are-allowed
  (let [n (next-path)
        abs-path (abs-path n)
        la (>- n 0)]
    (with-awaited-connection *root* *connect-string*
      (sync-path 5000 abs-path 0)
      (set-path! abs-path "x")
      (facts (deref la) =eventually=> "x")
      (set-path! abs-path false)
      (facts (deref la) =eventually=> false)
      (set-path! abs-path true)
      (facts (deref la) =eventually=> true)
      (set-path! abs-path nil)
      (facts (deref la) =eventually=> nil)
      (set-path! abs-path #inst "2015-05-08T19:35:09.371-00:00")
      (facts (deref la) =eventually=> #inst "2015-05-08T19:35:09.371-00:00"))))

(deftest pre-configured-values-gets-pushed
  (let [n (next-path)
        abs-path (abs-path n)]
    (create-path! abs-path "value")
    (create-path! (str abs-path "/.metadata") {:doc "My Doc"})
    (let [la (>- n "default-value" :meta {:doc "My Default Doc"})]
      (with-awaited-connection *root* *connect-string*
        (facts (deref la) =eventually=> "value")))))
