(ns roomkey.unit.zkutil
  (:use [roomkey.zkutil :as zk]
        [clojure.test]
        [midje.sweet]))

(facts "serialize produces byte array"
  (instance? (Class/forName "[B") (zk/serialize "test")) => true)

(facts "can serialize and deserialize string"
  (zk/deserialize (zk/serialize "test")) => "test")

(facts "can serialize and deserialize int"
  (zk/deserialize (zk/serialize 1)) => 1)

(facts "can serialize and deserialize vector"
  (zk/deserialize (zk/serialize [1 2 3])) => [1 2 3])

(facts "can serialize and deserialize empty map"
  (zk/deserialize (zk/serialize {})) => {})

(facts "can serialize and deserialize boolean"
  (zk/deserialize (zk/serialize true)) => true)

(facts "can serialize and deserialize java date"
  (zk/deserialize (zk/serialize (new java.util.Date 1))) => (new java.util.Date 1))

(facts "can serialize and deserialize unicode string"
  (zk/deserialize (zk/serialize "勺卉善爨")) => "勺卉善爨")

(facts "can serialize and deserialize deep structure with various types"
  (def deep
             [{:key1 "val1"
               :key2 (new java.util.Date 1)
               :key3 (range 25)
               :key4 ['a 'b 'c ['d 'e ['f 'g]]]}
              {:key5 "αβγ"}
              ])
  (zk/deserialize (zk/serialize deep)) => deep)
