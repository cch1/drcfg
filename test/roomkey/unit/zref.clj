(ns roomkey.unit.zref
  (:require [roomkey.zref :refer :all]
            [roomkey.znode :as znode]
            [roomkey.zclient :as zclient]
            [clojure.core.async :as async]
            [zookeeper :as zoo]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]))

(defchecker refers-to [expected]
  (checker [actual] (extended-= (deref actual) expected)))

(def $client (reify
               async/Mult ; need this to handle the client watching on root nodes
               (tap* [m ch close?] (async/timeout 1000))))

(fact "Can create a ZRef"
      (create (znode/create-root $client) "/myzref" "A") => (partial instance? roomkey.zref.ZRef))

(fact "A validator can be added"
      (let [$z (create (znode/create-root $client) "/zref0" 1)]
        (set-validator! $z odd?) => nil?
        (get-validator $z) => fn?))

(fact "New validators must validate current value"
      (let [$z (create (znode/create-root $client) "/zref0" 1)]
        (set-validator! $z even?) => (throws IllegalStateException)))

(fact "Default values must validate"
      (create (znode/create-root $client) "/zref0" "A" :validator pos?) => (throws ClassCastException)
      (create (znode/create-root $client) "/zref1" 1 :validator even?) => (throws IllegalStateException))

(fact "ZRefs can be watched"
      (let [$z (create (znode/create-root $client) "/myzref" "A")]
        (add-watch $z :key (constantly true)) => $z
        (remove-watch $z :key) => $z))

(fact "Can dereference a fresh ZRef to obtain default value"
      (create (znode/create-root $client) "/myzref" "A") => (refers-to "A"))

(fact "Can query a fresh ZRef to obtain initial metadata"
      (let [$z (create (znode/create-root $client) "/myzref" "A")]
        (meta $z)) => (contains {:version -1}))

(fact "A disconnected ZRef cannot be updated"
      (let [$z (create (znode/create-root (zclient/create)) "/myzref" "A")]
        (.compareVersionAndSet $z 0 "B")) => (throws RuntimeException))
