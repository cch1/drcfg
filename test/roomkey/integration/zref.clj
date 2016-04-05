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

(fact "Can create a ZRef"
  (zref "/myzref" "A") => (partial instance? roomkey.zref.ZRef))

(fact "A validator can be added"
  (let [$z (zref "/zref0" 1)]
    (set-validator! $z odd?) => nil?
    (get-validator $z) => (exactly odd?)))

(fact "New validators must validate current value"
  (let [$z (zref "/zref0" 1)]
    (set-validator! $z even?) => (throws IllegalStateException)))

(fact "Default values must validate"
  (zref "/zref0" "A" :validator pos?) => (throws ClassCastException)
  (zref "/zref1" 1 :validator even?) => (throws IllegalStateException))

(fact "ZRefs can be watched"
  (let [$z (zref "/myzref" "A")]
    (add-watch $z :key (constantly true)) => $z
    (remove-watch $z :key) => $z))

(fact "Can dereference a fresh ZRef to obtain default value"
  (zref "/myzref" "A") => (refers-to "A"))

(fact "Can query a fresh ZRef to obtain initial metadata"
  (let [$z (zref "/myzref" "A")]
    (meta $z)) => (contains {:version -1}))

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
      ;; This next step requires that the watcher update the cache quickly.
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
    (let [$zA (connect $c (zref "/myzref" "A" :validator string?))
          $zB (connect $c (zref "/myzref/child" "B" :validator string?))
          sync-a (promise)
          sync-b (promise)]
      (add-watch $zA :sync (fn [& args] (deliver sync-a args)))
      (add-watch $zB :sync (fn [& args] (deliver sync-b args)))
      (with-open [c (client (str connect-string sandbox))]
        (zoo/set-data c "/myzref" (z/*serialize* "a") 0)
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

(fact "A disconnected ZRef cannot be updated"
  (let [$z (zref "/myzref" "A")]
    (.compareVersionAndSet $z 0 "B")) => (throws RuntimeException))
