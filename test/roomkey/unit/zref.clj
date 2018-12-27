(ns roomkey.unit.zref
  (:require [roomkey.zref :refer :all]
            [clojure.core.async :as async]
            [zookeeper :as zoo]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]))

(defchecker refers-to [expected]
  (checker [actual] (extended-= (deref actual) expected)))

(defrecord ZClient [client-events mux])

(background (async/tap ..client.. anything) => ..events-channel..)

(fact "Can create a ZRef"
      (zref "/myzref" "A" ..client..) => (partial instance? roomkey.zref.ZRef))

(fact "A validator can be added"
      (let [$z (zref "/zref0" 1 ..client..)]
        (set-validator! $z odd?) => nil?
        (get-validator $z) => fn?))

(fact "New validators must validate current value"
      (let [$z (zref "/zref0" 1 ..client..)]
        (set-validator! $z even?) => (throws IllegalStateException)))

(fact "Default values must validate"
      (zref "/zref0" "A" ..client.. :validator pos?) => (throws ClassCastException)
      (zref "/zref1" 1 ..client.. :validator even?) => (throws IllegalStateException))

(fact "ZRefs can be watched"
      (let [$z (zref "/myzref" "A" ..client..)]
        (add-watch $z :key (constantly true)) => $z
        (remove-watch $z :key) => $z))

(fact "Can dereference a fresh ZRef to obtain default value"
      (zref "/myzref" "A" ..client..) => (refers-to "A"))

(fact "Can query a fresh ZRef to obtain initial metadata"
      (let [$z (zref "/myzref" "A" ..client..)]
        (meta $z)) => (contains {:version -1}))

(fact "A disconnected ZRef cannot be updated"
      (let [$z (zref "/myzref" "A" ..client..)]
        (.compareVersionAndSet $z 0 "B")) => (throws RuntimeException))
