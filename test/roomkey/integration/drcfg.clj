(ns roomkey.integration.drcfg
  (:require [roomkey.drcfg :refer :all]
            [roomkey.zref :as z]
            [zookeeper :as zoo]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]])
  (:import [org.apache.curator.test TestingServer]))

(def bogus-host "127.1.1.1:9999")
(def test-server (TestingServer. true))
(def connect-string (.getConnectString test-server))
(def sandbox "sandbox")
(def ^:dynamic *zc* (z/client connect-string))

(let [counter (atom 0)]
  (defn next-path
    []
    (str "/n/" (swap! counter inc))))

(defn- abs-path
  ([path]
   (str (string/join "/" ["" zk-prefix sandbox]) path)))

(defn create-path!
  [path value]
  (zoo/create-all *zc* path :data (z/*serialize* value) :persistent? true))

(defn set-path!
  [path value]
  (zoo/set-data *zc* path (z/*serialize* value) -1))

(defn sync-path
  "Wait up to timeout milliseconds for v to appear at path"
  [timeout path v]
  (let [vbs (seq (z/*serialize* v))]
    (loop [t timeout]
      (assert (pos? t) (format "Timed out after waiting %dms for %s to appear at %s" timeout v path))
      (if (and (zoo/exists *zc* path) (= vbs (seq (:data (zoo/data *zc* path)))))
        true
        (do (Thread/sleep 200)
            (recur (- t 200)))))))

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

(background [(around :facts (with-open [zc (zoo/connect connect-string)]
                              (binding [*zc* zc]
                                (zoo/delete-all *zc* (abs-path ""))
                                (binding [roomkey.drcfg/*registry* (atom #{})]
                                  ?form
                                  (stop @roomkey.drcfg/*registry*)))))])

(fact "can create a config value"
  (>- (next-path) "my-default-value" :validator string?) => (just [(refers-to "my-default-value")
                                                                   (refers-to nil)]))

(fact "Registration after connect still sets local atom"
  (connect! connect-string)
  (>- (next-path) "my-default-value" :validator string?) => (just [(refers-to "my-default-value")
                                                                   (refers-to nil)]))

(fact "connect! can continue if server not available"
  (let [[la _] (>- (next-path) "my-default-value" :validator string?)]
    (connect! bogus-host) => future?))

(fact "Slaved config value gets updated post-connect"
  (let [p "/N/0" ; (next-path)
        abs-path (abs-path p)
        [la _] (>- p "V0" :validator string?)]
    (connect-with-wait! connect-string sandbox) => set?
    (sync-path 5000 abs-path "V0")
    (set-path! abs-path "V1")
    la => (eventually-refers-to 10000 "V1")))

(fact "Children don't intefere with their parents"
  (let [n0 (next-path)
        n1 (str n0 "/child")
        abs-path0 (abs-path n0)
        abs-path1 (abs-path n1)
        [la0 _] (>- n0 0 :validator integer?)
        [la1 _] (>- n1 1 :validator integer?)]
    (connect! connect-string sandbox)
    (sync-path 5000 abs-path0 0)
    (sync-path 5000 abs-path1 1)
    (set-path! abs-path0 1)
    la0) => (eventually-refers-to 10000 1))

(fact "Serialization works"
  (let [n (next-path)
        abs-path (abs-path n)
        [la _] (>- n 0 :validator integer?)]
    (connect! connect-string sandbox)
    (sync-path 5000 abs-path 0)
    (set-path! abs-path 1)
    la => (eventually-refers-to 10000 1)))

(fact "Metadata is stored"
  (let [n (next-path)
        abs-path (abs-path n)
        [_ lam] (>- n 0 :validator integer? :meta {:doc "my doc string"})]
    (connect! connect-string sandbox)
    (sync-path 1000 (str abs-path "/.metadata") {:doc "my doc string"}) => truthy
    lam (eventually-refers-to 10000 {:doc "my doc string"})))

(fact "Validator prevents updates to local atom"
  (let [n (next-path)
        abs-path (abs-path n)
        [la _] (>- n 0 :validator integer?)]
    (connect! connect-string sandbox)
    (sync-path 5000 abs-path 0)
    (set-path! abs-path "x")
    (sync-path 5000 abs-path "x")
    la => (refers-to 0)))

(fact "Without a validator, heterogenous values are allowed"
  (let [n (next-path)
        abs-path (abs-path n)
        [la _] (>- n 0)]
    (connect! connect-string sandbox)
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
    la => (eventually-refers-to 10000 #inst "2015-05-08T19:35:09.371-00:00")))

(facts "pre-configured value gets applied"
  (let [n (next-path)
        abs-path (abs-path n)]
    (create-path! abs-path "value")
    (create-path! (str abs-path "/.metadata") {:doc "My Doc"})
    (let [[la lam] (>- n "default-value" :meta {:doc "My Default Doc"})]
      (connect! connect-string sandbox)
      la => (eventually-refers-to 10000 "value")
      lam => (eventually-refers-to 1000 {:doc "My Doc"}))))
