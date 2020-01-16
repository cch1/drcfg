(ns roomkey.integration.start-stop-zclient
  (:require [roomkey.zclient :refer :all]
            [midje.sweet :refer :all]))

(def connect-string "zk1.c0pt3r.local,zk2.c0pt3r.local,zk3.c0pt3r.local")

;; NB: this test will succede even if the server(s) are unavailable -that's kinda the point of the test
(fact "Can create a client, open it and then close it repeatedly"
      (let [$c (create)]
        (for [n (range 5)]
          (let [handle (open $c (str connect-string "/drcfg") 8000)]
            (.close handle))) => (five-of nil)))
