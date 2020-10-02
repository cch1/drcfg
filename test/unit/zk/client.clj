(ns unit.zk.client
  (:require [zk.client :refer :all]
            [midje.sweet :refer :all]))

(fact "Can create a ZClient"
      (create) => (partial instance? zk.client.ZClient))
