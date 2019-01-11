(ns roomkey.integration.znode
  (:require [roomkey.znode :refer :all :as znode]
            [roomkey.zclient :as zclient]
            [clojure.core.async :as async]
            [zookeeper :as zoo]
            [clojure.tools.logging :as log]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]])
  (:import [org.apache.curator.test TestingServer]))

(def test-server (TestingServer. true))
(def connect-string (.getConnectString test-server))

(def sandbox "/sandbox")

(defn- streams [n timeout c]
  "Captures the first `n` streamed elements of c subject to a timeout of `timeout` ms"
  (let [result (async/alt!!
                 (async/into [] (async/take n c 5)) ([v] (or v ::channel-closed))
                 (async/timeout timeout) ([v] (or v ::timeout)))]
    result))

(defchecker eventually-streams [n timeout expected]
  ;; The key to chatty checkers is to have the useful intermediate results be the evaluation of arguments to top-level expressions!
  (chatty-checker [actual] (extended-= (streams n timeout actual) expected)))

(background (around :facts (with-open [c (zoo/connect connect-string)]
                             (zoo/delete-all c sandbox)
                             ?form)))

(fact "ZNodes can be actualized and stream current value"
      (let [$zclient (zclient/create)
            $root (create-root $zclient)
            $child (add-descendant $root "/child" 0)]
        (zclient/with-awaited-open-connection $zclient (str connect-string sandbox) 500
          $child => (eventually-streams 3 3000 (just [{::znode/type ::znode/actualized! ::znode/value 0}
                                                      {::znode/type ::znode/opened}
                                                      {::znode/type ::znode/datum ::znode/value 0 ::znode/version 0}])))))

(fact "Actualized ZNodes can be updated"
      (let [$zclient (zclient/create)
            $root (create-root $zclient)
            $child (add-descendant $root "/child" 0)]
        (zclient/with-awaited-open-connection $zclient (str connect-string sandbox) 500
          $child => (eventually-streams 3 5000 (just [{::znode/type ::znode/actualized! ::znode/value 0}
                                                      {::znode/type ::znode/opened}
                                                      {::znode/type ::znode/datum ::znode/value 0 ::znode/version 0}]))
          (compare-version-and-set! $child 0 1)
          $child => (eventually-streams 2 5000 (just [{::znode/type ::znode/set! ::znode/value 1 ::znode/version 0}
                                                      {::znode/type ::znode/datum ::znode/value 1 ::znode/version 1}])))))

(fact "Existing ZNodes are acquired and stream their current value"
      (let [$zclient (zclient/create)
            $root (create-root $zclient)
            $child (add-descendant $root "/child" 0)]
        (zclient/with-awaited-open-connection $zclient (str connect-string sandbox) 500
          $child => (eventually-streams 3 2000 (just [{::znode/type ::znode/actualized! ::znode/value 0}
                                                      {::znode/type ::znode/opened}
                                                      {::znode/type ::znode/datum ::znode/value 0 ::znode/version 0}]))))

      (let [$zclient (zclient/create)
            $root (create-root $zclient)]
        (zclient/with-awaited-open-connection $zclient (str connect-string sandbox) 500
          $root => (eventually-streams 1 3000 (just [{::znode/type ::znode/opened}]))
          $root => (eventually-streams 2 3000 (just #{{::znode/type ::znode/datum ::znode/value ::znode/root ::znode/version 0}
                                                      (just {::znode/type ::znode/children-changed
                                                             ::znode/added (just [(partial instance? roomkey.znode.ZNode)])
                                                             ::znode/deleted empty?})}))
          (let [$child (get-in $root ["child"])]
            $child => (partial instance? roomkey.znode.ZNode)
            $child => (eventually-streams 2 3000 (just [{::znode/type ::znode/opened}
                                                        {::znode/type ::znode/datum ::znode/value 0 ::znode/version 0}]))))))

(fact "Existing ZNodes stream current value at startup when version greater than zero even when values match"
      (let [$zclient (zclient/create)
            $root (create-root $zclient)
            $child (add-descendant $root "/child" 0)]
        (zclient/with-awaited-open-connection $zclient (str connect-string sandbox) 500
          $child => (eventually-streams 3 2000 (just [{::znode/type ::znode/actualized! ::znode/value 0}
                                                      {::znode/type ::znode/opened}
                                                      {::znode/type ::znode/datum ::znode/value 0 ::znode/version 0}]))
          (compare-version-and-set! $child 0 1)
          $child => (eventually-streams 2 2000 (just [{::znode/type ::znode/set! ::znode/value 1 ::znode/version 0}
                                                      {::znode/type ::znode/datum ::znode/value 1 ::znode/version 1}]))))

      (let [$zclient (zclient/create)
            $root (create-root $zclient)
            $child (add-descendant $root "/child" 1)]
        (zclient/with-awaited-open-connection $zclient (str connect-string sandbox) 500
          $child => (eventually-streams 2 2000 (just [{::znode/type ::znode/opened}
                                                      {::znode/type ::znode/datum ::znode/value 1 ::znode/version 1}])))))

(fact "Existing ZNodes even stream version zero value at startup when different from initial value"
      (let [$zclient (zclient/create)
            $root (create-root $zclient)
            $child (add-descendant $root "/child" 0)]
        (zclient/with-awaited-open-connection $zclient (str connect-string sandbox) 500
          $child => (eventually-streams 3 2000 (just [{::znode/type ::znode/actualized! ::znode/value 0}
                                                      {::znode/type ::znode/opened}
                                                      {::znode/type ::znode/datum ::znode/value 0 ::znode/version 0}]))))
      (let [$zclient (zclient/create)
            $root (create-root $zclient)
            $child (add-descendant $root "/child" 1)]
        (zclient/with-awaited-open-connection $zclient (str connect-string sandbox) 500
          $child => (eventually-streams 2 2000 (just [{::znode/type ::znode/opened}
                                                      {::znode/type ::znode/datum ::znode/value 0 ::znode/version 0}])))))

(fact "ZNodes stream pushed values"
      (let [$zclient0 (zclient/create)
            $root0 (create-root $zclient0)
            $child0 (add-descendant $root0 "/child" 0)
            $zclient1 (zclient/create)
            $root1 (create-root $zclient1)
            $child1 (add-descendant $root1 "/child" 0)]
        (zclient/with-awaited-open-connection $zclient0 (str connect-string sandbox) 500
          $child0 => (eventually-streams 3 2000 (just [{::znode/type ::znode/actualized! ::znode/value 0}
                                                       {::znode/type ::znode/opened}
                                                       {::znode/type ::znode/datum ::znode/value 0 ::znode/version 0}]))
          (zclient/with-awaited-open-connection $zclient1 (str connect-string sandbox) 500
            $child1 => (eventually-streams 2 2000 (just [{::znode/type ::znode/opened}
                                                         {::znode/type ::znode/datum ::znode/value 0 ::znode/version 0}]))
            (compare-version-and-set! $child0 0 1)
            $child0 => (eventually-streams 2 2000 (just [{::znode/type ::znode/set! ::znode/value 1 ::znode/version 0}
                                                         {::znode/type ::znode/datum ::znode/value 1 ::znode/version 1}]))
            $child1 => (eventually-streams 1 2000 (just [{::znode/type ::znode/datum ::znode/value 1 ::znode/version 1}]))))))

(fact "Root node behaves like leaves"
      (let [$zclient (zclient/create)
            $root (create-root $zclient 10)]
        (zclient/with-awaited-open-connection $zclient (str connect-string sandbox) 500
          $root => (eventually-streams 3 2000 (just [{::znode/type ::znode/actualized! ::znode/value 10}
                                                     {::znode/type ::znode/opened}
                                                     {::znode/type ::znode/datum ::znode/value 10 ::znode/version 0}]))))

      (let [$zclient (zclient/create)
            $root (create-root $zclient nil)]
        (zclient/with-awaited-open-connection $zclient (str connect-string sandbox) 500
          $root => (eventually-streams 2 2000 (just [{::znode/type ::znode/opened}
                                                     {::znode/type ::znode/datum ::znode/value 10 ::znode/version 0}])))))
