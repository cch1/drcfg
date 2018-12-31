(ns roomkey.unit.drcfg
  (:require [roomkey.drcfg :refer :all]
            [roomkey.zref :as z]
            [roomkey.zclient :as zclient]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]))

(defchecker refers-to [expected]
  (checker [actual] (extended-= (deref actual) expected)))

(background (around :facts (binding [] ?form)))

(facts ">- returns a ZRefs"
       (>- "/ns/x" 1) => (refers-to 1)
       (provided
        (z/create "/ns/x" 1 roomkey.drcfg/*client*) => (atom 1)))

(fact "open starts zrefs with client specs"
      (open ..client.. "cspecs") => ..client..
      (provided
       (zclient/open ..client.. (as-checker string?) anything) => ..client..))
