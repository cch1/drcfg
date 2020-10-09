(ns unit.zk.client
  (:require [zk.client :refer :all]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]))

(defchecker as-ex-info
  [expected]
  (chatty-checker [actual]
                  (extended-= [(ex-message actual) (ex-data actual) (ex-cause actual)] expected)))

(fact "Can create a ZClient"
      (create) => (partial instance? zk.client.ZClient))

(fact "Can generate kex-info"
      (kex-info -103 "WTF" {:foo :bar}) => (just [(as-ex-info (just ["WTF" (contains {:foo :bar
                                                                                      :cognitect.anomalies/category :cognitect.anomalies/conflict
                                                                                      :zk.client/kex-code :BADVERSION})
                                                                     nil])) false]))

(future-fact "Can translate exceptions")
