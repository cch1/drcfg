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

(fact "Can create a ZRef in a virgin zookeeper"
  (with-open [$c (client (str connect-string sandbox))]
    (zref $c "/myzref" "A")) => (partial instance? roomkey.zref.ZRef))

(fact "A validator can be added"
  (with-open [$c (client (str connect-string sandbox))]
    (let [$z (zref $c "/zref0" 1)]
      (set-validator! $z odd?) => nil?
      (get-validator $z) => (exactly odd?))))

(fact "New validators must validate current value"
  (with-open [$c (client (str connect-string sandbox))]
    (let [$z (zref $c "/zref0" 1)]
      (set-validator! $z even?) => (throws IllegalStateException))))

(fact "Default values must validate"
  (with-open [$c (client (str connect-string sandbox))]
    (zref $c "/zref0" "A" :validator pos?) => (throws ClassCastException)
    (zref $c "/zref1" 1 :validator even?) => (throws IllegalStateException)))

(fact "Can dereference a fresh ZRef to obtain default value"
  (with-open [$c (client (str connect-string sandbox))]
    (zref $c "/myzref" "A")) => (refers-to "A"))

(fact "Can query a fresh ZRef to obtain initial metadata"
  (with-open [$c (client (str connect-string sandbox))]
    (let [$z (zref $c "/myzref" "A")]
      (meta $z))) => (contains {:version 0 :ctime pos?}))

(fact "Can update ZRef"
  (with-open [$c (client (str connect-string sandbox))]
    (let [$z (zref $c "/zref0" "A")]
      (.compareVersionAndSet $z 0 "B") => true
      (.compareVersionAndSet $z 12 "C") => false)
    (let [$z (zref $c "/zref1" "A")]
      (compare-and-set! $z "Z" "B") => false
      (compare-and-set! $z "A" "B") => true
      (reset! $z "C") => "C")
    (let [$z (zref $c "/zref2" 1)]
      (swap! $z inc) => 2
      ;; This next step requires that the watcher update the cache quickly.
      (swap! $z inc) => 3)))

(fact "ZRefs can be watched"
  (with-open [$c (client (str connect-string sandbox))]
    (let [$z (zref $c "/myzref" "A")]
      (add-watch $z :key (constantly true)) => $z
      (remove-watch $z :key) => $z)))

(fact "ZRef is updated by changes at the cluster"
  (with-open [$c (client (str connect-string sandbox))]
    (let [$z (zref $c "/myzref" "A")
          sync (promise)]
      (add-watch $z :sync (fn [& args] (deliver sync args)))
      (with-open [c (client (str connect-string sandbox))]
        (zoo/set-data c "/myzref" (#'z/serialize "B") 0))
      @sync
      $z)) => (refers-to "B"))

(fact "ZRef is updated by deletion at the cluster"
  (with-open [$c (client (str connect-string sandbox))]
    (let [$z (zref $c "/myzref" "A")
          sync (promise)]
      (add-watch $z :sync (fn [& args] (deliver sync args)))
      (with-open [c (client (str connect-string sandbox))]
        (zoo/delete c "/myzref"))
      $z)) => (refers-to "A"))
