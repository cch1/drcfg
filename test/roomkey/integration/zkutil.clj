(ns roomkey.integration.zkutil
  (:require [roomkey.zkutil :refer :all]
            [midje.sweet :refer :all]
            [clojure.tools.logging :as log])
  (:import [org.apache.zookeeper.KeeperException]))

(def connection-string "localhost:2181/zkutil-integration-tests")
(def test-root "/sandbox")

(def client (connect connection-string))

(defn connected?
  [timeout c]
  (loop [t timeout]
    (when (pos? t)
      (or (is-connected? c) (recur (- t 200))))))

(defn cleanup! [client]
  (when (exists? client test-root) (rmr client test-root))
  (when (not (exists? client "/")) (create client "/"))
  (when (not (exists? client test-root)) (create client test-root)))

(background [(around :facts (if (connected? 1000 client)
                              (do (cleanup! client)
                                  ?form
                                  (cleanup! client))
                              (log/infof "Zookeeper unavailable on %s - skipping %s" connection-string *ns*)))])

(facts "basic zkutil primitives work"
  (if (exists? client test-root) true false) => true

  (let [empty-node (str test-root "/empty-node")]
    (create client empty-node)
    (exists? client empty-node) => empty-node
    (nget client empty-node) => nil
    (rm client empty-node)
    (exists? client empty-node) => nil)


  (let [value-node (str test-root "/value-node")]
    (create client value-node "value")
    (exists? client value-node) => value-node
    (nget client value-node) => "value"
    (rm client value-node)
    (exists? client value-node) => nil)


  (let [deep-root (str test-root "/deep")
        deeper-node (str test-root "/deep/deeper")
        deepest-node (str test-root "/deep/deeper/deepest")
        deepest-node2 (str test-root "/deep/deeper/also-deepest")]
    (create client deepest-node)
    (exists? client deepest-node) => deepest-node
    (create client deepest-node2)
    (exists? client deepest-node2) => deepest-node2
    (count (ls client deeper-node)) => 2
    (:numChildren (stat client deeper-node)) => 2
    (rmr client deep-root)
    (exists? client deep-root) => nil))

(facts "can set metadata and remove nodes with metadata"
  (let [test-node (str test-root "/metadata-test")]
    (create client test-node)
    (exists? client test-node) => test-node
    (set-metadata client test-node {:test "value"})
    (get-metadata client test-node) => {:test "value"}
    (get-metadata-mtime client test-node)
    => (roughly (.getTime (java.util.Date.)) 20000)
    (ls client test-node) => []
    (ls-all client test-node) => [".metadata"]
    (rm client test-node)
    (exists? client test-node) => nil))

(fact "nget for missing node throws NoNodeException"
  (nget client (str test-root "no-such-node"))
  => (throws org.apache.zookeeper.KeeperException$NoNodeException))
