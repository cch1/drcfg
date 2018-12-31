(ns roomkey.integration.zref
  (:require [roomkey.zref :refer :all :as z]
            [roomkey.zclient :as zclient]
            [clojure.core.async :as async]
            [zookeeper :as zoo]
            [clojure.tools.logging :as log]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]])
  (:import [org.apache.curator.test TestingServer]))

(def bogus-host "127.1.1.1:9999")
(def test-server (TestingServer. true))
(def connect-string (.getConnectString test-server))

(def sandbox "/sandbox")

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

(background (around :facts (with-open [c (zoo/connect connect-string)]
                             (zoo/delete-all c sandbox)
                             (zoo/create-all c sandbox :persistent? true)
                             ?form)))

(let [counter (atom 0)]
  (defn next-path
    []
    (str "/" (swap! counter inc))))

(fact "Connecting a ZRef returns the closable channel"
      (let [$c (zclient/create)
            $z (zref "/myzref" "A" $c)]
        (with-open [$c (zclient/open $c (str connect-string sandbox) 5000)]
          (.zConnect $z) => (partial satisfies? clojure.core.async.impl.protocols/Channel)
          $z => (eventually-vrefers-to 2000 ["A" 1]))))

(fact "Initializing a ZRef in a virgin zookeeper creates the node with default data"
      (with-open [$c (zclient/create)]
        (let [$z (zref "/myzref" "A" $c)]
          (zclient/open $c (str connect-string sandbox) 500)
          $z => (eventually-vrefers-to 2000 ["A" 1])))
      (with-open [c (zoo/connect (str connect-string sandbox))]
        (zoo/data c "/myzref") => (contains {:data (fn [x] (= "A" (z/*deserialize* x)))
                                             :stat (contains {:version 1})})))

(fact "Can update a connected ZRef"
      (with-open [$c (zclient/create)]
        (let [$z0 (zref "/zref0" "A" $c)
              $z1 (zref "/zref1" "A" $c)
              $z2 (zref "/zref2" 1 $c)]
          (zclient/open $c (str connect-string sandbox) 5000)
          $z0 => (eventually-vrefers-to 1000 ["A" 1])
          (.compareVersionAndSet $z0 1 "B") => true
          $z0 => (eventually-vrefers-to 1000 ["B" 2])
          (.compareVersionAndSet $z0 12 "C") => false

          $z1 => (eventually-vrefers-to 1000 ["A" 1])
          (compare-and-set! $z1 "Z" "B") => false
          (compare-and-set! $z1 "A" "B") => true
          $z1 => (eventually-vrefers-to 1000 ["B" 2])
          (reset! $z1 "C") => "C"
          $z1 => (eventually-vrefers-to 1000 ["C" 3])

          $z2 => (eventually-vrefers-to 1000 [1 1])
          (swap! $z2 inc) => 2
          $z2 => (eventually-vrefers-to 1000 [2 2])
          (swap! $z2 inc) => 3
          $z2 => (eventually-vrefers-to 1000 [3 3]))))

(fact "A connected ZRef is updated by changes at the cluster"
      (with-open [$c (zclient/create)
                  c (zoo/connect (str connect-string sandbox))]
        (let [$z (zref "/myzref" "A" $c)]
          (zclient/open $c (str connect-string sandbox) 5000)
          $z => (eventually-vrefers-to 1000 ["A" 1])
          (zoo/set-data c "/myzref" (z/*serialize* "B") 1)
          $z => (eventually-vrefers-to 1000 ["B" 2]))))

(fact "A connected ZRef's watches are called when updated by changes at the cluster"
      (with-open [$c (zclient/create)
                  c (zoo/connect (str connect-string sandbox))]
        (let [$z (zref "/myzref" "A" $c)
              sync (promise)]
          (add-watch $z :sync (fn [& args] (deliver sync args)))
          (zclient/open $c (str connect-string sandbox) 5000)
          $z => (eventually-vrefers-to 1000 ["A" 1])
          (zoo/set-data c "/myzref" (z/*serialize* "B") 1)
          (deref sync 10000 :promise-never-delivered) => (just [:sync (partial instance? roomkey.zref.ZRef) "A" "B"]))))

(fact "A connected ZRef is not updated by invalid values at the cluster"
      (with-open [$c (zclient/create)
                  c (zoo/connect (str connect-string sandbox))]
        (let [$z (zref "/myzref" "A" $c :validator string?)
              sync (promise)]
          (add-watch $z :sync (fn [& args] (deliver sync args)))
          (zclient/open $c (str connect-string sandbox) 500)
          $z => (eventually-vrefers-to 1000 ["A" 1])
          (zoo/set-data c "/myzref" (z/*serialize* 23) 1)
          (deref sync 1000 ::not-delivered) => ::not-delivered
          (zoo/set-data c "/myzref" (z/*serialize* "B") 2)
          $z => (eventually-vrefers-to 1000 ["B" 3]))))

(fact "Children do not intefere with their parents"
      (with-open [$c (zclient/create)
                  c (zoo/connect (str connect-string sandbox))]
        (let [$zB (zref "/myzref/child" "B" $c :validator string?)
              $zA (zref "/myzref" "A" $c :validator string?)
              sync-a (promise)
              sync-b (promise)]
          (zclient/open $c (str connect-string sandbox) 500)
          $zA => (eventually-vrefers-to 1000 ["A" 1])
          $zB => (eventually-vrefers-to 1000 ["B" 1])
          (add-watch $zA :sync (fn [& args] (deliver sync-a args)))
          (add-watch $zB :sync (fn [& args] (deliver sync-b args)))
          (zoo/set-data c "/myzref" (z/*serialize* "a") 1)
          (zoo/set-data c "/myzref/child" (z/*serialize* "b") 1)
          @sync-a => (just [:sync (partial instance? roomkey.zref.ZRef) "A" "a"])
          @sync-b => (just [:sync (partial instance? roomkey.zref.ZRef) "B" "b"])
          $zA => (eventually-refers-to 1000 "a")
          $zB => (eventually-refers-to 1000 "b"))))

(fact "A ZRef deleted at the cluster throws exceptions on update but otherwise behaves"
      (with-open [$c (zclient/create)
                  c (zoo/connect (str connect-string sandbox))]
        (let [$z (zref "/myzref" "A" $c)]
          (zclient/open $c (str connect-string sandbox) 500)
          $z => (eventually-vrefers-to 1000 ["A" 1])
          (zoo/delete c "/myzref")
          (compare-and-set! $z "A" "B") => (throws org.apache.zookeeper.KeeperException$NoNodeException)
          $z => (refers-to "A"))))
