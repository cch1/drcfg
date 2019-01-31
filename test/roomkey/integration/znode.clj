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

(defchecker stat? [expected]
  ;; The key to chatty checkers is to have the useful intermediate results be the evaluation of arguments to top-level expressions!
  (every-checker (contains {:version int?
                            :cversion int?
                            :aversion int?
                            :ctime (partial instance? java.time.Instant)
                            :mtime (partial instance? java.time.Instant)
                            :mzxid pos-int?
                            :czxid pos-int?
                            :pzxid pos-int?
                            :numChildren int?
                            :ephemeralOwner int?
                            :dataLength int?})
                 (contains expected)))

(defchecker having-metadata [expected-value expected-metadata]
  (every-checker (chatty-checker [actual] (extended-= actual expected-value))
                 (chatty-checker [actual] (extended-= (meta actual) expected-metadata))))

(defchecker has-ref-state [expected-value]
  (let [builder (fn [actual] [(deref (.stat actual)) (deref (.value actual)) (deref (.children actual))])]
    (chatty-checker [actual] (extended-= (builder actual) (just expected-value)))))

(background (around :facts (with-open [c (zoo/connect connect-string)]
                             (zoo/delete-all c sandbox)
                             ?form)))

(fact "ZNodes can be actualized and stream current value"
      (let [$root (create-root)
            $child (add-descendant $root "/child" 0)]
        (with-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start}
                                                      (just #::znode{:type ::znode/created!})
                                                      (just #::znode{:type ::znode/datum :value 0 :stat (stat? {:version 0})})])))
        $child => (has-ref-state [(stat? {:version 0}) 0 empty?])))

(fact "Actualized ZNodes can be updated"
      (let [$root (create-root)
            $child (add-descendant $root "/child" 0)]
        (with-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 5000 (just [#::znode{:type ::znode/watch-start}
                                                      (just #::znode{:type ::znode/created!})
                                                      (just #::znode{:type ::znode/datum :value 0 :stat (stat? {:version 0})})]))
          (compare-version-and-set! $child 0 1)
          $child => (eventually-streams 1 5000 (just [(just #::znode{:type ::znode/datum :value 1 :stat (stat? {:version 1})})])))
        $child => (has-ref-state [(stat? {:version 1}) 1 empty?])))

(fact "Existing ZNodes are acquired and stream their current value"
      (let [$root (create-root)
            $child (add-descendant $root "/child" 0)
            $grandchild (add-descendant $root "/child/grandchild" 0)]
        (with-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start}
                                                      (just #::znode{:type ::znode/created!})
                                                      (just #::znode{:type ::znode/datum :value 0 :stat (stat? {:version 0})})]))
          $grandchild => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start}
                                                           (just #::znode{:type ::znode/created!})
                                                           (just #::znode{:type ::znode/datum :value 0 :stat (stat? {:version 0})})])))
        $child => (eventually-streams 1 2000 (just [#::znode{:type ::znode/watch-stop}])))
      ;; ensure spurious wrongly-pathed acquired children don't appear
      (let [$root (create-root)]
        (with-connection $root (str connect-string sandbox) 500
          $root => (eventually-streams 1 3000 (just [#::znode{:type ::znode/watch-start}]))
          $root => (eventually-streams 2 3000 (just #{(just #::znode{:type ::znode/datum :value ::znode/root :stat (stat? {:version 0})})
                                                      (just #::znode{:type ::znode/children-changed :stat (stat? {:cversion 1})
                                                                     :inserted (one-of (partial instance? roomkey.znode.ZNode))
                                                                     :removed empty?})}))
          (let [$child ($root "/child")]
            $child => (partial instance? roomkey.znode.ZNode)
            $child => (eventually-streams 1 3000 (just [#::znode{:type ::znode/watch-start}]))
            $child => (eventually-streams 2 3000 (just #{(just #::znode{:type ::znode/children-changed :stat (stat? {:cversion 1})
                                                                        :inserted (one-of (partial instance? roomkey.znode.ZNode))
                                                                        :removed empty?})
                                                         (just #::znode{:type ::znode/datum :value 0 :stat (stat? {:version 0})})}))
            (let [$grandchild ($root "/child/grandchild")]
              $grandchild => (partial instance? roomkey.znode.ZNode)
              $grandchild => (eventually-streams 2 3000 (just [#::znode{:type ::znode/watch-start}
                                                               (just #::znode{:type ::znode/datum :value 0 :stat (stat? {:version 0})})]))
              $child => (eventually-streams 1 1000 ::timeout))
            $root => (has-ref-state [(stat? {:version 0}) anything (just #{$child})])))))

(fact "Existing ZNodes stream current value at startup when version greater than zero even when values match"
      (let [$root (create-root)
            $child (add-descendant $root "/child" 0)]
        (with-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start}
                                                      (just #::znode{:type ::znode/created!})
                                                      (just #::znode{:type ::znode/datum :value 0 :stat (stat? {:version 0})})]))
          (compare-version-and-set! $child 0 1)
          $child => (eventually-streams 1 2000 (just [(just #::znode{:type ::znode/datum :value 1 :stat (stat? {:version 1})})]))))

      (let [$root (create-root)
            $child (add-descendant $root "/child" 1)]
        (with-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 2 2000 (just [#::znode{:type ::znode/watch-start}
                                                      (just #::znode{:type ::znode/datum :value 1 :stat (stat? {:version 1})})])))))

(fact "Existing ZNodes even stream version zero value at startup when different from initial value"
      (let [$root (create-root)
            $child (add-descendant $root "/child" 0)]
        (with-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start}
                                                      (just #::znode{:type ::znode/created!})
                                                      (just #::znode{:type ::znode/datum :value 0 ::znode/stat (stat? {:version 0})})]))))
      (let [$root (create-root)
            $child (add-descendant $root "/child" 1)]
        (with-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 2 2000 (just [#::znode{:type ::znode/watch-start}
                                                      (just #::znode{:type ::znode/datum :value 0 :stat (stat? {:version 0})})])))))

(fact "ZNodes stream pushed values"
      (let [$root0 (create-root)
            $child0 (add-descendant $root0 "/child" 0)
            $root1 (create-root)
            $child1 (add-descendant $root1 "/child" 0)]
        (with-connection $root0 (str connect-string sandbox) 500
          $child0 => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start}
                                                       (just #::znode{:type ::znode/created!})
                                                       (just #::znode{:type ::znode/datum :value 0 :stat (stat? {:version 0})})]))
          (with-connection $root1 (str connect-string sandbox) 500
            $child1 => (eventually-streams 2 2000 (just [#::znode{:type ::znode/watch-start}
                                                         (just #::znode{:type ::znode/datum :value 0 :stat (stat? {:version 0})})]))
            (compare-version-and-set! $child0 0 1)
            $child0 => (eventually-streams 1 2000 (just [(just #::znode{:type ::znode/datum :value 1 :stat (stat? {:version 1})})]))
            $child1 => (eventually-streams 1 2000 (just [(just #::znode{:type ::znode/datum :value 1 :stat (stat? {:version 1})})]))))))

(fact "Existing ZNodes stream pushed values exactly once"
      (let [$root0 (create-root)
            $child0 (add-descendant $root0 "/child" 0)
            $root1 (create-root)
            $child1 (add-descendant $root1 "/child" 0)]
        (with-connection $root0 (str connect-string sandbox) 500
          $child0 => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start}
                                                       (just #::znode{:type ::znode/created!})
                                                       (just #::znode{:type ::znode/datum :value 0 :stat (stat? {:version 0})})])))
        $child0 => (eventually-streams 1 2000 [#::znode{:type ::znode/watch-stop}])
        (with-connection $root1 (str connect-string sandbox) 500
          $child1 => (eventually-streams 2 2000 (just [#::znode{:type ::znode/watch-start}
                                                       (just #::znode{:type ::znode/datum :value 0 :stat (stat? {:version 0})})]))
          $child1 => (eventually-streams 1 2000 ::timeout)
          $root1 => (has-ref-state [(stat? {:version 0}) anything (just #{$child1})]))))

(fact "Root node behaves like leaves"
      (let [$root (create-root "/" 10)]
        (with-connection $root (str connect-string sandbox) 500
          $root => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start}
                                                     (just #::znode{:type ::znode/created!})
                                                     (just #::znode{:type ::znode/datum :value 10 :stat (stat? {:version 0})})]))))

      (let [$root (create-root "/" nil)]
        (with-connection $root (str connect-string sandbox) 500
          $root => (eventually-streams 2 2000 (just [#::znode{:type ::znode/watch-start}
                                                     (just #::znode{:type ::znode/datum :value 10 :stat (stat? {:version 0})})])))))

(fact "ZNodes can be deleted"
      (let [$root (create-root)
            $child (add-descendant $root "/child" 0)]
        (with-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start}
                                                      (just #::znode{:type ::znode/created!})
                                                      (just #::znode{:type ::znode/datum :value 0 :stat (stat? {:version 0})})]))
          (delete $child 0) => truthy
          $child => (eventually-streams 2 3000 (just #{#::znode{:type ::znode/deleted!}
                                                       #::znode{:type ::znode/watch-stop}})))
        $root => (has-ref-state [(stat? {:cversion 2}) anything empty?])))

(fact "Existing ZNodes can be deleted"
      (let [$root (create-root)
            $child (add-descendant $root "/child" 0)]
        (with-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start}
                                                      (just #::znode{:type ::znode/created!})
                                                      (just #::znode{:type ::znode/datum :value 0 :stat (stat? {:version 0})})])))
        $child => (eventually-streams 1 1000 [#::znode{:type ::znode/watch-stop}])
        (with-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 2 3000 (just [#::znode{:type ::znode/watch-start}
                                                      (just #::znode{:type ::znode/datum :value 0 :stat (stat? {:version 0})})]))
          (delete $child 0) => truthy
          $child => (eventually-streams 2 3000 (just #{#::znode{:type ::znode/deleted!}
                                                       #::znode{:type ::znode/watch-stop}})))))

(fact "Children do not intefere with their parents, regardless of insertion sequence"
      (let [$root (create-root)
            $zB (add-descendant $root "/myzref/child" "B")
            $zA (add-descendant $root "/myzref" "A")]
        (with-connection $root (str connect-string sandbox) 500
          $zA => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start}
                                                   (just #::znode{:type ::znode/created!})
                                                   (just #::znode{:type ::znode/datum :value "A" :stat (stat? {:version 0})})]))
          $zB => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start}
                                                   (just #::znode{:type ::znode/created!})
                                                   (just #::znode{:type ::znode/datum :value "B" :stat (stat? {:version 0})})])))
        $zA => (eventually-streams 1 2000 (just [#::znode{:type ::znode/watch-stop}]))
        $zB => (eventually-streams 1 2000 (just [#::znode{:type ::znode/watch-stop}]))))

(fact "A ZNode can persist metadata"
      (let [$root (create-root)
            $z0 (add-descendant $root "/myzref0" "A")
            $z1 (add-descendant $root "/myzref1" (with-meta {:my "znode" } {:foo 1}))
            $z2 (add-descendant $root "/myzref2" [])]
        (with-open [c (zoo/connect (str connect-string sandbox))]
          (with-connection $root (str connect-string sandbox) 500
            $z0 => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start}
                                                     (just #::znode{:type ::znode/created!})
                                                     (contains #::znode{:type ::znode/datum :value "A"})]))
            $z1 => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start}
                                                     (just #::znode{:type ::znode/created!})
                                                     (contains  #::znode{:type ::znode/datum :value (having-metadata {:my "znode"} {:foo 1})})]))
            $z2 => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start}
                                                     (just #::znode{:type ::znode/created!})
                                                     (contains #::znode{:type ::znode/datum :value (having-metadata [] nil)})]))
            (zoo/set-data c "/myzref0" (znode/*serialize* [["B"] {:my 1}]) 0)
            $z0 => (eventually-streams 1 3000 (just [(contains #::znode{:type ::znode/datum :value (having-metadata ["B"] {:my 1})})]))))))

(fact "Children are remembered across sessions"
      (let [$root (create-root)]
        (with-open [c (zoo/connect (str connect-string sandbox))]
          (with-connection $root (str connect-string sandbox) 500
            $root => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start}
                                                       (just #::znode{:type ::znode/created!})
                                                       (contains #::znode{:type ::znode/datum})]))
            (zoo/create c "/child" :data (znode/*serialize* [["B"] {:my 1}]))
            (Thread/sleep 200))
          $root => (eventually-streams 2 3000 (just [(contains #::znode{:type ::znode/children-changed})
                                                     #::znode{:type ::znode/watch-stop}]))
          (with-connection $root (str connect-string sandbox) 500
            $root => (eventually-streams 2 3000 (just [#::znode{:type ::znode/watch-start}
                                                       (contains #::znode{:type ::znode/datum})]))
            (Thread/sleep 200))
          $root => (eventually-streams 1 3000 (just [(contains #::znode{:type ::znode/watch-stop})])))))

(fact "Signature can be computed"
      (let [$root (create-root)
            $child0 (add-descendant $root "/child0" 0)
            $child1 (add-descendant $root "/child1" (with-meta #{1 2 3} {:foo "bar"}))
            $grandchild (add-descendant $root "/child0/grandchild" 0)]
        (with-connection $root (str connect-string sandbox) 500
          $root => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start}
                                                     (just #::znode{:type ::znode/created!})
                                                     (contains #::znode{:type ::znode/datum :value ::znode/root})]))
          $child0 => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start}
                                                       (just #::znode{:type ::znode/created!})
                                                       (contains #::znode{:type ::znode/datum :value 0})]))
          $child1 => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start}
                                                       (just #::znode{:type ::znode/created!})
                                                       (contains #::znode{:type ::znode/datum :value #{1 2 3}})]))
          $grandchild => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start}
                                                           (just #::znode{:type ::znode/created!})
                                                           (contains #::znode{:type ::znode/datum :value 0})]))
          (signature $grandchild) => (just [integer? -1249580007])
          (signature $root) => (just [integer? -1188681409]))))
