(ns roomkey.unit.drcfg
  (:require [roomkey.drcfg :refer :all]
            [roomkey.drcfg.client :as client]
            [roomkey.zkutil :as zk]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]))

(defchecker refers-to [expected]
  (checker [actual] (extended-= (deref actual) expected)))

(background (around :facts (binding [roomkey.drcfg/*client* (promise)
                                     roomkey.drcfg/*registry* (agent {})]
                             ?form)))

(facts ">- returns a local atom"
  (>- "/ns/x" 1) => (refers-to 1)
  (provided
    (zk/serialize 1) => ..whatever..)
  *client* =not=> realized?
  *registry* => (refers-to (contains {"/ns/x" (just [(partial instance? clojure.lang.Atom) falsey])})))

(fact "connect-with-wait! does its stuff"
  (binding [roomkey.drcfg/*registry* (agent {"x" [..localAtom.. false]})]
      (connect-with-wait! "hosts") => anything
      (provided
        (client/connect (as-checker string?)) => ..client..
        (#'roomkey.drcfg/watch-znode ..client.. "x" anything) => ..irrelevant..)
      *client* => (refers-to ..client..)
      *registry* => (refers-to (contains {"x" (just [..localAtom.. truthy])}))))
