(ns roomkey.integration.znode
  (:require [roomkey.znode :refer :all :as znode]
            [roomkey.zclient :as zclient]
            [clojure.core.async :as async]
            [zookeeper :as zoo]
            [clojure.tools.logging :as log]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [detailed-extended-= extended-= as-data-laden-falsehood extended-false? extended-true?]])
  (:import [org.apache.curator.test TestingServer]))

(def bogus-host "127.1.1.1:9999")
(def test-server (TestingServer. true))
(def connect-string (.getConnectString test-server))

(def sandbox "/sandbox")

(defchecker refers-to [expected]
  (checker [actual] (extended-= (deref actual) expected)))

(defn sync-path
  "Using given `client`, Wait up to `timeout` milliseconds for znode at `path` to exist"
  [timeout client path]
  (loop [t timeout]
    (assert (pos? t) (format "Timed out after waiting %dms for node to appear at path %s" timeout path))
    (if (and (zoo/exists client path))
      true
      (do (Thread/sleep 200)
          (recur (- t 200))))))

(defn sync-znode
  "Wait up to `timeout` milliseconds for `znode` to exist"
  [timeout z]
  (let [client @(.client z)
        path (znode/path z)]
    (loop [t timeout]
      (assert (pos? t) (format "Timed out after waiting %dms for %s to appear" timeout z))
      (if (and (zoo/exists client path))
        true
        (do (Thread/sleep 200)
            (recur (- t 200)))))))

(defn- streams [n timeout c]
  "Captures the first `n` streamed elements of c subject to a timeout of `timeout` ms"
  (let [result (async/alt!!
                 (async/into () (async/take n c)) ([v] (or v ::channel-closed))
                 (async/timeout timeout) ([v] (or v ::timeout)))]
    result))

(defchecker eventually-streams [n timeout expected]
  ;; The key to chatty checkers is to have the useful intermediate results be the evaluation of arguments to top-level expressions!
  (chatty-checker [actual] (extended-= (streams n timeout actual) expected)))

(let [counter (atom 0)]
  (defn next-path
    []
    (str "/" (swap! counter inc))))

(background (around :facts (with-open [c (zoo/connect connect-string)]
                             (zoo/delete-all c sandbox)
                             ?form)))

(fact "ZNodes can be actualized and stream no data"
      (let [$zclient (zclient/create)
            $root (create-root $zclient)
            $child (add-descendant $root "/child" 0)]
        (with-open [$zclient (zclient/open $zclient (str connect-string sandbox) 500)]
          $child => (eventually-streams 1 3000 ::timeout))))

(fact "Actualized ZNodes can be updated"
      (let [$zclient (zclient/create)
            $root (create-root $zclient)
            $child (add-descendant $root "/child" 0)]
        (with-open [$zclient (zclient/open $zclient (str connect-string sandbox) 500)]
          (sync-znode 1000 $child)
          (compare-version-and-set! $child 0 1)
          $child => (eventually-streams 1 3000 (just [[1 1]])))))

(fact "Existing ZNodes are acquired and stream their current value"
      (let [$zclient (zclient/create)
            $root (create-root $zclient)
            $child (add-descendant $root "/child" 0)]
        (with-open [$zclient (zclient/open $zclient (str connect-string sandbox) 500)]
          (sync-znode 1000 $child)))

      (let [$zclient (zclient/create)
            $root (create-root $zclient)]
        (with-open [$zclient (zclient/open $zclient (str connect-string sandbox) 500)]
          (sync-path 1000 @(.client $root) "/child")
          (Thread/sleep 2000)
          (let [$child (get-in $root ["child"])]
            $child => (partial instance? roomkey.znode.ZNode)
            $child => (eventually-streams 1 1000 (just [[0 0]]))))))

(fact "Existing ZNodes stream current value at startup when version greater than zero even when values match"
      (let [$zclient (zclient/create)
            $root (create-root $zclient)
            $child (add-descendant $root "/child" 0)]
        (with-open [$zclient (zclient/open $zclient (str connect-string sandbox) 500)]
          (sync-znode 1000 $child)
          (compare-version-and-set! $child 0 1)))

      (let [$zclient (zclient/create)
            $root (create-root $zclient)
            $child (add-descendant $root "/child" 1)]
        (with-open [$zclient (zclient/open $zclient (str connect-string sandbox) 500)]
          $child => (eventually-streams 1 3000 (just [[1 1]])))))

(fact "Existing ZNodes even stream version zero value at startup when different from initial value"
      (let [$zclient (zclient/create)
            $root (create-root $zclient)
            $child (add-descendant $root "/child" 0)]
        (with-open [$zclient (zclient/open $zclient (str connect-string sandbox) 500)]
          (sync-znode 1000 $child)))

      (let [$zclient (zclient/create)
            $root (create-root $zclient)
            $child (add-descendant $root "/child" 1)]
        (with-open [$zclient (zclient/open $zclient (str connect-string sandbox) 500)]
          $child => (eventually-streams 1 3000 (just [[0 0]])))))

(fact "ZNodes stream pushed values"
      (let [$zclient0 (zclient/create)
            $root0 (create-root $zclient0)
            $child0 (add-descendant $root0 "/child" 0)
            $zclient1 (zclient/create)
            $root1 (create-root $zclient1)
            $child1 (add-descendant $root1 "/child" 0)]
        (with-open [$zclient0 (zclient/open $zclient0 (str connect-string sandbox) 500)
                    $zclient1 (zclient/open $zclient1 (str connect-string sandbox) 500)]
          (sync-znode 1000 $child0)
          (sync-znode 1000 $child1)
          (compare-version-and-set! $child0 0 1)
          $child1 => (eventually-streams 1 3000 (just [[1 1]])))))

(fact "Root node behaves like leaves"
      (let [$zclient (zclient/create)
            $root (create-root $zclient 10)]
        (with-open [$zclient (zclient/open $zclient (str connect-string sandbox) 500)]
          (sync-znode 1000 $root)))

      (let [$zclient (zclient/create)
            $root (create-root $zclient nil)]
        (with-open [$zclient (zclient/open $zclient (str connect-string sandbox) 500)]
          (sync-path 1000 @(.client $root) "/")
          (Thread/sleep 2000)
          $root => (eventually-streams 1 1000 (just [[10 0]])))))
