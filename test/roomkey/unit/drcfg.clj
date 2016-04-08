(ns roomkey.unit.drcfg
  (:require [roomkey.drcfg :refer :all]
            [roomkey.zref :as z]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]))

(defchecker refers-to [expected]
  (checker [actual] (extended-= (deref actual) expected)))

(background (around :facts (binding [roomkey.drcfg/*client* (promise)
                                     roomkey.drcfg/*registry* (ref #{})]
                             ?form)))

(facts ">- returns a ZRef"
  (>- "/ns/x" 1) => (refers-to 1)
  (provided
    (z/zref "/ns/x" 1) => (atom 1)
    (z/zref "/ns/x/.metadata" nil) => (atom nil))
  *client* =not=> realized?
  *registry* => (refers-to (just #{(refers-to 1) (refers-to nil)})))

(fact "connect-with-wait! does its stuff"
  (binding [roomkey.drcfg/*registry* (ref #{..zRef..})]
    (connect-with-wait! "hosts") => anything
    (provided
      (z/connected? ..zRef..) => false
      (z/client (as-checker string?)) => ..client..
      (z/connect ..client.. ..zRef..) => ..irrelevant..)
    *client* => (refers-to ..client..)
    *registry* => (refers-to (just #{..zRef..}))))

(fact "status returns status"
  (binding [roomkey.drcfg/*registry* (ref #{(atom ..A..) (atom ..B..)})]
    (status) => (just [["/x/y" false ..A..] ["/y/x" true ..B..]])
    (provided
      (z/path (refers-to ..A..)) => "/x/y"
      (z/path (refers-to ..B..)) => "/y/x"
      (z/connected? (refers-to ..A..)) => false
      (z/connected? (refers-to ..B..)) => true)))

(fact "status-report reports status"
  (binding [roomkey.drcfg/*registry* (ref #{(atom ..A..) (atom ..B..)})]
    (status-report) => string?
    (provided
      (z/path (refers-to ..A..)) => "/x/y"
      (z/path (refers-to ..B..)) => "/y/x"
      (z/connected? (refers-to ..A..)) => false
      (z/connected? (refers-to ..B..)) => true)))
