(ns roomkey.unit.zclient
  (:require [roomkey.zclient :refer :all]
            [clojure.core.async :as async]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]
            [clojure.test.check.generators :as gen]
            [midje.experimental :refer [for-all]]))

(fact "Can create a ZClient"
      (create) => (partial instance? roomkey.zclient.ZClient))
