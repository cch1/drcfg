(ns roomkey.integration.zref
  (:require [roomkey.zref :refer :all :as z]
            [zookeeper :as zoo]
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

(background (around :facts (with-open [c (zoo/connect connect-string)]
                             (zoo/delete-all c sandbox)
                             ?form)))

(let [counter (atom 0)]
  (defn next-path
    []
    (str "/" (swap! counter inc))))

(fact "Can create a client"
  (with-open [$c (client (str connect-string sandbox))]
    $c) => (partial instance? org.apache.zookeeper.ZooKeeper)
  (with-open [$c (client connect-string)]
    (zoo/exists $c sandbox)) => truthy)

(fact "Connecting a ZRef returns the ZRef"
  (with-open [$c (client (str connect-string sandbox))]
    (connect $c (zref "/myzref" "A"))) => (partial instance? roomkey.zref.ZRef))

(fact "Connecting a ZRef in a virgin zookeeper creates the node with default data"
  (with-open [$c (client (str connect-string sandbox))]
    (connect $c (zref "/myzref" "A")) => anything)
  (with-open [c (client (str connect-string sandbox))]
    (zoo/data c "/myzref")) => (contains {:data (fn [x] (= "A" (z/*deserialize* x)))
                                          :stat (contains {:version 0})}))

(fact "Can update a connected ZRef"
  (with-open [$c (client (str connect-string sandbox))]
    (let [$z (connect $c (zref "/zref0" "A"))]
      (.compareVersionAndSet $z 0 "B") => true
      (.compareVersionAndSet $z 12 "C") => false)
    (let [$z (connect $c (zref "/zref1" "A"))]
      (compare-and-set! $z "Z" "B") => false
      (compare-and-set! $z "A" "B") => true
      (reset! $z "C") => "C")
    (let [$z (connect $c (zref "/zref2" 1))]
      (swap! $z inc) => 2
      (swap! $z inc) => 3)))

(fact "A connected ZRef is updated by changes at the cluster"
  (with-open [$c (client (str connect-string sandbox))]
    (let [$z (connect $c (zref "/myzref" "A"))
          sync (promise)]
      (add-watch $z :sync (fn [& args] (deliver sync args)))
      (with-open [c (client (str connect-string sandbox))]
        (zoo/set-data c "/myzref" (z/*serialize* "B") 0))
      @sync => (just [:sync (partial instance? roomkey.zref.ZRef) "A" "B"])
      $z)) => (refers-to "B"))

(fact "A connected ZRef is not updated by invalid values at the cluster"
  (with-open [$c (client (str connect-string sandbox))]
    (let [$z (connect $c (zref "/myzref" "A" :validator string?))
          sync (promise)]
      (add-watch $z :sync (fn [& args] (deliver sync args)))
      (with-open [c (client (str connect-string sandbox))]
        (zoo/set-data c "/myzref" (z/*serialize* 23) 0))
      (with-open [c (client (str connect-string sandbox))]
        (zoo/set-data c "/myzref" (z/*serialize* "B") 1))
      @sync
      $z)) => (refers-to "B"))

(fact "Children do not intefere with their parents"
  (with-open [$c (client (str connect-string sandbox))]
    (let [$zB (connect $c (zref "/myzref/child" "B" :validator string?))
          $zA (connect $c (zref "/myzref" "A" :validator string?))
          sync-a (promise)
          sync-b (promise)]
      (add-watch $zA :sync (fn [& args] (deliver sync-a args)))
      (add-watch $zB :sync (fn [& args] (deliver sync-b args)))
      (with-open [c (client (str connect-string sandbox))]
        (zoo/set-data c "/myzref" (z/*serialize* "a") 1)
        (zoo/set-data c "/myzref/child" (z/*serialize* "b") 0))
      @sync-a => (just [:sync (partial instance? roomkey.zref.ZRef) "A" "a"])
      @sync-b => (just [:sync (partial instance? roomkey.zref.ZRef) "B" "b"])
      $zA => (refers-to "a")
      $zB => (refers-to "b"))))

(fact "A connected ZRef is updated by deletion at the cluster"
  (with-open [$c (client (str connect-string sandbox))]
    (let [$z (connect $c (zref "/myzref" "A"))
          sync (promise)]
      (add-watch $z :sync (fn [& args] (deliver sync args)))
      (with-open [c (client (str connect-string sandbox))]
        (zoo/delete c "/myzref"))
      $z)) => (refers-to "A"))

(fact "A disconnected ZRef behaves"
  (let [$z (zref "/myzref" "A")
        sync (promise)]
    (add-watch $z :sync (fn [& args] (deliver sync args)))
    (with-open [$c (client (str connect-string sandbox))]
      (connect $c $z)
      @sync
      (.zDisconnect $z)
      @$z)) => "A")

(fact "Disconnected ZRefs are reconnected"
  (with-open [s (TestingServer. true)]
    (let [z (zref "/myzref" "A")]
      (with-open [c (client (str (.getConnectString s) "/sandbox"))]
        (connect c z)
        (.restart s)
        (Thread/sleep 1000) ; a watch on the client would remove this silliness
        (let [p (promise)]
          (add-watch z :sync (fn [& args] (deliver p args)))
          (reset! z "B")
          (deref p))
        z))) => (refers-to "B"))

(fact "Missing parent nodes throw meaningful exceptions"
  (with-open [test-server (TestingServer. true)]
    (client (str (.getConnectString test-server) "/sandbox/drcfg"))) => (throws clojure.lang.ExceptionInfo))

(fact "Non-sequential version updates are OK"
  (with-open [s (TestingServer. true)]
    (let [z0 (zref "/myzref" "A")
          z1 (zref "/myzref" "A")]
      (with-open [c0 (client (str (.getConnectString s) "/sandbox"))]
        (connect c0 z0)
        (reset! z0 "B")
        (disconnect z0))
      (with-open [c1 (client (str (.getConnectString s) "/sandbox"))]
        (connect c1 z1)
        (reset! z1 "C")
        (reset! z1 "D")
        (connect c1 z0)
        z0)) => (refers-to "D")))
