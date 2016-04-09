(ns roomkey.unit.drcfg
  (:require [roomkey.drcfg :refer :all]
            [roomkey.zref :as z]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]))

(defchecker refers-to [expected]
  (checker [actual] (extended-= (deref actual) expected)))

(background (around :facts (binding [roomkey.drcfg/*registry* (atom #{})] ?form)))

(facts ">- returns a pair of ZRefs"
  (>- "/ns/x" 1) => (just [(refers-to 1) (refers-to nil)])
  (provided
    (z/zref "/ns/x" 1) => (atom 1)
    (z/zref "/ns/x/.metadata" nil) => (atom nil))
  *registry* => (refers-to (just #{(refers-to 1) (refers-to nil)})))

(fact "start starts zrefs with client specs"
  (start [..zRef1.. ..zRef2..] "cspecs") => (every-checker
                                             (just [..zRef1.. ..zRef2..])
                                             (fn [r] (= ..client.. (:roomkey.drcfg/client (meta r)))))
  (provided
    (z/client (as-checker string?)) => ..client..
    (z/connected? ..zRef1..) => false
    (z/connected? ..zRef2..) => false
    (z/connect ..client.. ..zRef1..) => ..zRef1..
    (z/connect ..client.. ..zRef2..) => ..zRef2..))

(fact "connect-with-wait! does its stuff"
  (binding [roomkey.drcfg/*registry* (atom #{..zRef..})]
    (connect-with-wait! "hosts") => anything
    (provided
      (z/connected? ..zRef..) => false
      (z/client (as-checker string?)) => ..client..
      (z/connect ..client.. ..zRef..) => ..irrelevant..)
    *registry* => (refers-to (just #{..zRef..}))))

(fact "status returns status"
  (status #{(atom ..A..) (atom ..B..)}) => (just [["/x/y" false ..A..] ["/y/x" true ..B..]])
  (provided
    (z/path (refers-to ..A..)) => "/x/y"
    (z/path (refers-to ..B..)) => "/y/x"
    (z/connected? (refers-to ..A..)) => false
    (z/connected? (refers-to ..B..)) => true))

(fact "status-report reports status"
  (status-report #{(atom ..A..) (atom ..B..)}) => string?
  (provided
    (z/path (refers-to ..A..)) => "/x/y"
    (z/path (refers-to ..B..)) => "/y/x"
    (z/connected? (refers-to ..A..)) => false
    (z/connected? (refers-to ..B..)) => true))
