(ns roomkey.unit.drcfg.client
  (:require [roomkey.drcfg.client :refer :all]
            [roomkey.zkutil :as zk]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]))

(defchecker refers-to [expected]
  (checker [actual] (extended-= (deref actual) expected)))

(fact "connect does its stuff"
  (connect "hosts") => ..client..
  (provided
    (zk/connect (as-checker string?)) => ..client..
    (zk/init! ..client..) => ..irrelevantA..))
