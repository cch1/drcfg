(ns roomkey.unit.drcfg
  (:require [roomkey.drcfg :refer :all]
            [roomkey.zref :as zref]
            [roomkey.zclient :as zclient]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]))

(defchecker refers-to [expected]
  (checker [actual] (extended-= (deref actual) expected)))

(background (around :facts (binding [] ?form)))

(facts ">- returns a ZRefs"
       (>- "/ns/x" 1) => (refers-to 1)
       (provided
        (zref/create *root* "/ns/x" 1) => (atom 1)))

(fact "open starts zrefs with client specs"
      (open "cspecs") => ..client..
      (provided
       (zclient/open (as-checker (partial instance? roomkey.zclient.ZClient)) "cspecs/drcfg" (as-checker integer?)) => ..client..))
