(ns roomkey.unit.zkutil
  (:require [roomkey.zkutil :refer :all]
            [midje.sweet :refer :all]))

(let [now (java.util.Date.)]
  (tabular "can serialize and deserialize values"
           (fact (deserialize (serialize ?v)) => ?v)
           ?v
           "test"
           1
           [0 1 2]
           true
           false
           nil
           now
           "勺卉善爨"
           [{:key1 "val1"
             :key2 now
             :key3 (range 25)
             :key4 ['a 'b 'c ['d 'e ['f 'g]]]}
            {:key5 "αβγ"}]))
