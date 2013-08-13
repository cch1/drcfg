(ns roomkey.integration.zkutil
  (:require [roomkey.zkutil :refer :all]
            [midje.sweet :refer :all]
            [clojure.tools.logging :as log]))

(def conn-string "localhost:2181/zkutil-integration-tests")
(def test-root "/sandbox")

(defn zk-available? []
  (let [conn (zkconn! conn-string)]
    (loop [i 10 connected false]
      (if (and (not connected) (> i 0))
        (do
          (Thread/sleep 1000)
          (recur (dec i) (is-connected? conn)))
        connected))))

(defn conn! [] (zkconn! conn-string))

(defn cleanup! []
  (when (exists? (conn!) test-root)
    (rmr (conn!) test-root))
  (when (not (exists? (conn!) "/")) (create (conn!) "/"))
  (when (not (exists? (conn!) test-root)) (create (conn!) test-root)))

(if (not (zk-available?))
  (log/infof "zookeeper unavailable - skipping " *ns*)
  (against-background [(before :facts (cleanup!)) ] ;(after :contents
                             ;(cleanup!))
    (facts "basic zkutil primitives work"

      (if (exists? (conn!) test-root) true false) => true

      (let [empty-node (str test-root "/empty-node")]
        (create (conn!) empty-node)
        (exists? (conn!) empty-node) => empty-node
        (nget (conn!) empty-node) => nil
        (rm (conn!) empty-node)
        (exists? (conn!) empty-node) => nil)


      (let [value-node (str test-root "/value-node")]
        (create (conn!) value-node "value")
        (exists? (conn!) value-node) => value-node
        (nget (conn!) value-node) => "value"
        (rm (conn!) value-node)
        (exists? (conn!) value-node) => nil)


      (let [deep-root (str test-root "/deep")
            deeper-node (str test-root "/deep/deeper")
            deepest-node (str test-root "/deep/deeper/deepest")
            deepest-node2 (str test-root "/deep/deeper/also-deepest")]
        (create (conn!) deepest-node)
        (exists? (conn!) deepest-node) => deepest-node
        (create (conn!) deepest-node2)
        (exists? (conn!) deepest-node2) => deepest-node2
        (count (ls (conn!) deeper-node)) => 2
        (:numChildren (stat (conn!) deeper-node)) => 2
        (rmr (conn!) deep-root)
        (exists? (conn!) deep-root) => nil))

    (facts "can set metadata and remove nodes with metadata"
      (let [test-node (str test-root "metadata-test")]
        (create (conn!) test-node)
        (exists? (conn!) test-node) => test-node
        (set-metadata (conn!) test-node {:test "value"})
        (get-metadata (conn!) test-node) => {:test "value"}
        (get-metadata-mtime (conn!) test-node)
        => (roughly (.getTime (java.util.Date.)) 20000)
        (ls (conn!) test-node) => []
        (ls-all (conn!) test-node) => [".metadata"]
        (rm (conn!) test-node)
        (exists? (conn!) test-node) => nil))))
