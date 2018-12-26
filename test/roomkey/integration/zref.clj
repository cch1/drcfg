(ns roomkey.integration.zref
  (:require [roomkey.zref :refer :all :as z]
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

(background (around :facts (with-open [c (zoo/connect connect-string)]
                             (zoo/delete-all c sandbox)
                             (zoo/create-all c sandbox :persistent? true)
                             ?form)))

(let [counter (atom 0)]
  (defn next-path
    []
    (str "/" (swap! counter inc))))

(fact "Pairing a ZRef and ZClient returns the closable channel"
      (with-open [$c (zoo/connect (str connect-string sandbox))]
        (.zConnect (zref "/myzref" "A") $c)) => (partial satisfies? clojure.core.async.impl.protocols/Channel))

(fact "Initializing a ZRef in a virgin zookeeper creates the node with default data"
      (with-open [$c (zoo/connect (str connect-string sandbox))]
        (.zInitialize (zref "/myzref" "A") $c) => truthy)
      (with-open [c (zoo/connect (str connect-string sandbox))]
        (zoo/data c "/myzref")) => (contains {:data (fn [x] (= "A" (z/*deserialize* x)))
                                              :stat (contains {:version 1})}))

(fact "Can update a connected ZRef"
      (with-open [$c (zoo/connect (str connect-string sandbox))]
        (let [$z (zref "/zref0" "A")]
          (.zInitialize $z $c)
          (.compareVersionAndSet $z 1 "B") => true
          (.compareVersionAndSet $z 12 "C") => false)
        (let [$z (zref "/zref1" "A")]
          (.zInitialize $z $c)
          (compare-and-set! $z "Z" "B") => false
          (compare-and-set! $z "A" "B") => true
          (reset! $z "C") => "C")
        (let [$z (zref "/zref2" 1)]
          (.zInitialize $z $c)
          (swap! $z inc) => 2
          (swap! $z inc) => 3)))

(fact "A connected ZRef is updated by changes at the cluster"
      (let [$z (zref "/myzref" "A")
            sync (promise)]
        (add-watch $z :sync (fn [& args] (deliver sync args)))
        (with-open [$c (zoo/connect (str connect-string sandbox))
                    c (zoo/connect (str connect-string sandbox))]
          (.zInitialize $z $c)
          (zoo/set-data c "/myzref" (z/*serialize* "B") 1)
          (deref sync 10000 :promise-never-delivered) => (just [:sync (partial instance? roomkey.zref.ZRef) "A" "B"])
          $z => (refers-to "B"))))

(fact "A connected ZRef is not updated by invalid values at the cluster"
      (with-open [$c (zoo/connect (str connect-string sandbox))]
        (let [$z (zref "/myzref" "A" :validator string?)
              sync (promise)]
          (.zInitialize $z $c)
          (add-watch $z :sync (fn [& args] (deliver sync args)))
          (with-open [c (zoo/connect (str connect-string sandbox))]
            (zoo/set-data c "/myzref" (z/*serialize* 23) 1))
          (with-open [c (zoo/connect (str connect-string sandbox))]
            (zoo/set-data c "/myzref" (z/*serialize* "B") 2))
          @sync
          $z)) => (refers-to "B"))

(fact "Children do not intefere with their parents"
      (with-open [$c (zoo/connect (str connect-string sandbox))]
        (let [$zB (zref "/myzref/child" "B" :validator string?)
              $zA (zref "/myzref" "A" :validator string?)
              sync-a (promise)
              sync-b (promise)]
          (.zInitialize $zB $c)
          (.zInitialize $zA $c)
          (add-watch $zA :sync (fn [& args] (deliver sync-a args)))
          (add-watch $zB :sync (fn [& args] (deliver sync-b args)))
          (with-open [c (zoo/connect (str connect-string sandbox))]
            (zoo/set-data c "/myzref" (z/*serialize* "a") 1)
            (zoo/set-data c "/myzref/child" (z/*serialize* "b") 1))
          @sync-a => (just [:sync (partial instance? roomkey.zref.ZRef) "A" "a"])
          @sync-b => (just [:sync (partial instance? roomkey.zref.ZRef) "B" "b"])
          $zA => (refers-to "a")
          $zB => (refers-to "b"))))

(fact "A connected ZRef is updated by deletion at the cluster"
      (with-open [$c (zoo/connect (str connect-string sandbox))]
        (let [$z (zref "/myzref" "A")
              sync (promise)]
          (.zInitialize $z $c)
          (add-watch $z :sync (fn [& args] (deliver sync args)))
          (with-open [c (zoo/connect (str connect-string sandbox))]
            (zoo/delete c "/myzref"))
          $z)) => (refers-to "A"))

(fact "A disconnected ZRef behaves"
      (let [$z (zref "/myzref" "A")
            sync (promise)]
        (add-watch $z :sync (fn [& args] (deliver sync args)))
        (with-open [$c (zoo/connect (str connect-string sandbox))]
          (.zInitialize $z $c)
          (let [c (.zConnect $z $c)]
            @sync
            (.zDisconnect $z c))
          @$z)) => "A")

(fact "Disconnected ZRefs are reconnected"
      (with-open [s (TestingServer. true)]
        (let [z (zref "/myzref" "A")]
          (with-open [c (zoo/connect (.getConnectString s))]
            (.zInitialize z c)
            (Thread/sleep 1000) ; a watch on the client would remove this silliness
            (let [p (promise)]
              (add-watch z :sync (fn [& args] (deliver p args)))
              (reset! z "B")
              (deref p))
            z))) => (refers-to "B"))

(fact "Non-sequential version updates are OK"
      (with-open [s (TestingServer. true)]
        (let [z0 (zref "/myzref" "A")
              z1 (zref "/myzref" "A")]
          (with-open [c0 (zoo/connect (.getConnectString s))]
            (let [c (.zInitialize z0 c0)]
              (reset! z0 "B")
              (.zDisconnect z0 c)))
          (with-open [c1 (zoo/connect (.getConnectString s))]
            (.zInitialize z1 c1)
            (reset! z1 "C")
            (reset! z1 "D")
            (.zConnect z0 c1)
            (Thread/sleep 1000)
            z0)) => (refers-to "D")))
