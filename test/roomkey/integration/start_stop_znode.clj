(ns roomkey.integration.start-stop-znode
  (:require [roomkey.znode :refer :all]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]))

(def connect-string "zk1.c0pt3r.local,zk2.c0pt3r.local,zk3.c0pt3r.local")

(fact "Can create a client, open it and then close it repeatedly"
      (let [$z (new-root)]
        (for [n (range 6)]
          (let [handle (open $z (str connect-string "/drcfg") 8000)]
            (Thread/sleep 100)
            (.close handle))) => (six-of pos-int?)))
