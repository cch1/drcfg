(ns roomkey.integration.start-stop-znode
  (:require [roomkey.znode :refer :all]
            [midje.sweet :refer :all])
  (:import [org.apache.curator.test TestingServer]))

(def bad-connect-string "127.1.1.1:9999")
(def good-connect-string (.getConnectString (TestingServer. true)))

(fact "Can create a new root, open it and then close it repeatedly"
      (let [$z (new-root)]
        (for [connect-string (concat (repeat 5 good-connect-string)
                                     (repeat 5 bad-connect-string)
                                     (repeatedly 5 #(rand-nth [good-connect-string bad-connect-string])))]
          (let [handle (open $z (str connect-string "/drcfg") 8000)]
            (Thread/sleep (rand-int 100))
            (.close handle))) => (n-of nil? 15)))
