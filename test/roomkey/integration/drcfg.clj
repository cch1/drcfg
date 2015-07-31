(ns roomkey.integration.drcfg
  (:require [roomkey.drcfg :refer :all]
            [roomkey.zkutil :as zk]
            [roomkey.drcfg.client :as client]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]
            [clojure.tools.logging :as log])
  (:import [org.apache.curator.test TestingServer]))

(def bogus-host "127.1.1.1:9999")
(def test-server (TestingServer. true))
(def connect-string (.getConnectString test-server))
(def zconn (client/connect connect-string))

(def root "/sandbox")

(let [counter (atom 0)]
  (defn next-path
    []
    (str root "/" (swap! counter inc))))

(defn sync-path
  "Wait up to timeout milliseconds for v to appear at client's path"
  [timeout client path v]
  (loop [t timeout]
    (assert (pos? t) (format "Timed out after waiting %dms for %s to appear at %s" timeout v path))
    (if (and (zk/exists? client path) (= v (zk/nget client path)))
      true
      (do (Thread/sleep 200)
          (recur (- timeout 200))))))

(defn cleanup! []
  (let [path (str "/" *ns*)]
    (when (zk/exists? zconn path)
      (zk/rmr zconn path)))
  (when (zk/exists? zconn root)
    (zk/rmr zconn root)))

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

(background [(around :facts (if (zk/is-connected? zconn)
                              (do (cleanup!)
                                  (binding [roomkey.drcfg/*client* (promise)
                                            roomkey.drcfg/*registry* (ref {})]
                                    ?form
                                    (when (realized? roomkey.drcfg/*client*) (.close @roomkey.drcfg/*client*)))
                                  (cleanup!))
                              (log/infof "Zookeeper unavailable on %s - skipping %s" connect-string *ns*)))])

(fact "can create a config value without a connection"
  (let [n (next-path)]
    (assert (not (realized? roomkey.drcfg/*client*)))
    (>- n "my-default-value" :validator string?)) => (refers-to "my-default-value"))

(fact "Registration after connect still sets local atom"
  (connect! connect-string)
  (>- (next-path) "my-default-value" :validator string?) => (refers-to "my-default-value"))

(fact "connect! can continue if server not available"
  (let [la (>- (next-path) "my-default-value" :validator string?)]
    (connect! bogus-host) => future?))

(fact "Slaved config value gets updated post-connect"
  (let [n (next-path)
        la (>- n "V0" :validator string?)]
    (connect! connect-string)
    (sync-path 5000 zconn n "V0")
    (zk/nset zconn n "V1")
    la => (eventually-refers-to 10000 "V1")))

(fact "Serialization works"
  (let [n (next-path)
        la (>- n 0 :validator integer?)]
    (connect! connect-string)
    (sync-path 5000 zconn n 0)
    (zk/nset zconn n 1)
    la => (eventually-refers-to 10000 1)))

(fact "Metadata is stored"
  (let [n (next-path)]
    (>- n 0 :validator integer? :meta {:doc "my doc string"})
    (connect! connect-string)
    (sync-path 5000 zconn n 0)
    (zk/get-metadata zconn n) => {:doc "my doc string"}))

(fact "Validator prevents updates to local atom"
  (let [n (next-path)
        la (>- n 0 :validator integer?)]
    (connect! connect-string)
    (sync-path 5000 zconn n 0)
    (zk/nset zconn n "x")
    (sync-path 5000 @roomkey.drcfg/*client* n "x")
    la => (refers-to 0)))

(fact "Without a validator, heterogenous values are allowed"
  (let [n (next-path)
        la (>- n 0)]
    (connect! connect-string)
    (sync-path 5000 zconn n 0)
    (zk/nset zconn n "x")
    (sync-path 5000 @roomkey.drcfg/*client* n "x")
    la => (eventually-refers-to 10000 "x")
    (zk/nset zconn n false)
    (sync-path 5000 @roomkey.drcfg/*client* n false)
    la => (eventually-refers-to 10000 false)
    (zk/nset zconn n true)
    (sync-path 5000 @roomkey.drcfg/*client* n true)
    la => (eventually-refers-to 10000 true)
    (zk/nset zconn n nil)
    (sync-path 5000 @roomkey.drcfg/*client* n nil)
    la => (eventually-refers-to 10000 nil)
    (zk/nset zconn n #inst "2015-05-08T19:35:09.371-00:00")
    (sync-path 5000 @roomkey.drcfg/*client* n #inst "2015-05-08T19:35:09.371-00:00")
    la => (eventually-refers-to 10000 #inst "2015-05-08T19:35:09.371-00:00")))

(facts "pre-configured value gets applied"
  (let [n (next-path)]
    (zk/create zconn n "value")
    (let [la (>- n "default-value")]
      (connect! connect-string)
      la => (eventually-refers-to 10000 "value"))))
