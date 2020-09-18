(ns roomkey.integration.start-stop-zclient
  (:require [roomkey.zclient :refer :all]
            [midje.sweet :refer :all])
  (:import [org.apache.curator.test TestingServer]))

(def bad-connect-string "127.1.1.1:9999")
(def good-connect-string (.getConnectString (TestingServer. true)))


;; NB: this test will succede even if the server(s) are unavailable -that's kinda the point of the test
(fact "Can create a client, open it and then close it repeatedly"
      (let [$c (create)]
        (for [connect-string (concat (repeat 5 good-connect-string)
                                     (repeat 5 bad-connect-string)
                                     (repeatedly 5 #(rand-nth [good-connect-string bad-connect-string])))]
          (let [handle (open $c (str connect-string "/drcfg") 8000)]
            (.close handle))) => (n-of nil 15)))
