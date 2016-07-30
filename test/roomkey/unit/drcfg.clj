(ns roomkey.unit.drcfg
  (:require [roomkey.drcfg :refer :all]
            [roomkey.zref :as z]
            [roomkey.zclient :as zclient]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]))

(defchecker refers-to [expected]
  (checker [actual] (extended-= (deref actual) expected)))

(background (around :facts (binding [roomkey.drcfg/*registry* (atom #{})] ?form)))

(facts ">- returns a ZRefs"
  (>- "/ns/x" 1) => (refers-to 1)
  (provided
    (z/zref "/ns/x" 1) => (atom 1))
  *registry* => (refers-to (just #{(refers-to 1)})))

(fact "open starts zrefs with client specs"
  (open [..zRef1.. ..zRef2..] "cspecs") => ..client..
  (provided
    (zclient/create (as-checker string?) anything) => ..client..))

(fact "connect-with-wait! does its stuff"
  (binding [roomkey.drcfg/*registry* (atom #{..zRef..})]
    (connect-with-wait! "hosts") => (every-checker
                                    (just [..zRef..])
                                    (fn [r] (= ..client.. (:roomkey.drcfg/client (meta r)))))
    (provided
     (zclient/create (as-checker string?) anything) => ..client..)
    *registry* => (refers-to (just #{..zRef..}))))

(future-fact "status returns status"
  (status #{(atom ..A..) (atom ..B..)}) => (just [["/x/y" false ..A..] ["/y/x" true ..B..]]))

(future-fact "status-report reports status"
  (status-report #{(atom ..A..) (atom ..B..)}) => string?)
