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

(defmacro with-awaited-open-connection
  [zroot connect-string timeout & body]
  `(let [client# (.client ~zroot)]
     (zclient/with-awaited-open-connection client# ~connect-string ~timeout ~@body)))

(background (around :facts (with-open [c (zoo/connect connect-string)]
                             (zoo/delete-all c sandbox)
                             ?form)))

(fact "ZNodes can be actualized and stream current value"
      (let [$root (create-root)
            $child (add-descendant $root "/child" 0)]
        (with-awaited-open-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start :node $child}
                                                      #::znode{:type ::znode/created! :value 0 :node $child}
                                                      (just #::znode{:type ::znode/datum :value 0 :stat (contains {:version 0}) :node $child})])))))

(fact "Actualized ZNodes can be updated"
      (let [$root (create-root)
            $child (add-descendant $root "/child" 0)]
        (with-awaited-open-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 5000 (just [#::znode{:type ::znode/watch-start :node $child}
                                                      #::znode{:type ::znode/created! :node $child :value 0}
                                                      (just #::znode{:type ::znode/datum :node $child :value 0 :stat (contains {:version 0})})]))
          (compare-version-and-set! $child 0 1)
          $child => (eventually-streams 2 5000 (just [#::znode{:type ::znode/set! :node $child :value 1 :version 0}
                                                      (just #::znode{:type ::znode/datum :node $child :value 1 :stat (contains {:version 1})})])))))

(fact "Existing ZNodes are acquired and stream their current value"
      (let [$root (create-root)
            $child (add-descendant $root "/child" 0)
            $grandchild (add-descendant $root "/child/grandchild" 0)]
        (with-awaited-open-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start :node $child}
                                                      #::znode{:type ::znode/created! :node $child :value 0}
                                                      (just #::znode{:type ::znode/datum :node $child :value 0 :stat (contains {:version 0})})]))
          $grandchild => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start :node $grandchild}
                                                           #::znode{:type ::znode/created! :node $grandchild :value 0}
                                                           (just #::znode{:type ::znode/datum :node $grandchild :value 0 :stat (contains {:version 0})})])))
        $child => (eventually-streams 1 2000 (just [#::znode{:type ::znode/watch-stop :node $child}])))
      ;; ensure spurious wrongly-pathed acquired children don't appear
      (let [$root (create-root)]
        (with-awaited-open-connection $root (str connect-string sandbox) 500
          $root => (eventually-streams 1 3000 (just [#::znode{:type ::znode/watch-start :node $root}]))
          $root => (eventually-streams 3 3000 (just #{(just #::znode{:type ::znode/exists :node $root :stat (contains {:version 0})})
                                                      (just #::znode{:type ::znode/datum :node $root :value ::znode/root :stat (contains {:version 0})})
                                                      (just #::znode{:type ::znode/children-changed :node $root :stat (contains {:cversion 1})
                                                                     :inserted (one-of (partial instance? roomkey.znode.ZNode))
                                                                     :removed empty?})}))
          (let [$child (get-in $root ["child"])]
            $child => (partial instance? roomkey.znode.ZNode)
            $child => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start :node $child}
                                                        (just #::znode{:type ::znode/exists :node $child :stat (contains {:version 0})})
                                                        (just #::znode{:type ::znode/datum :node $child :value 0 :stat (contains {:version 0})})]))
            (let [$grandchild (get-in $root ["child" "grandchild"])]
              $grandchild => (partial instance? roomkey.znode.ZNode)
              $grandchild => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start :node $grandchild}
                                                               (just #::znode{:type ::znode/exists :node $grandchild :stat (contains {:version 0})})
                                                               (just #::znode{:type ::znode/datum :node $grandchild :value 0 :stat (contains {:version 0})})]))
              $child => (eventually-streams 1 1000 (just #{(just #::znode{:type ::znode/children-changed :node $child :stat (contains {:cversion 1})
                                                                          :inserted (just $grandchild)
                                                                          :removed empty?})}))
              $child => (eventually-streams 1 1000 ::timeout))))))

(fact "Existing ZNodes stream current value at startup when version greater than zero even when values match"
      (let [$root (create-root)
            $child (add-descendant $root "/child" 0)]
        (with-awaited-open-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start :node $child}
                                                      #::znode{:type ::znode/created! :node $child :value 0}
                                                      (just #::znode{:type ::znode/datum :node $child :value 0 :stat (contains {:version 0})})]))
          (compare-version-and-set! $child 0 1)
          $child => (eventually-streams 2 2000 (just [#::znode{:type ::znode/set! :node $child :value 1 :version 0}
                                                      (just #::znode{:type ::znode/datum :node $child :value 1 :stat (contains {:version 1})})]))))

      (let [$root (create-root)
            $child (add-descendant $root "/child" 1)]
        (with-awaited-open-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start :node $child}
                                                      (just #::znode{:type ::znode/exists :node $child :stat (contains {:version 1})})
                                                      (just #::znode{:type ::znode/datum :node $child :value 1 :stat (contains {:version 1})})])))))

(fact "Existing ZNodes even stream version zero value at startup when different from initial value"
      (let [$root (create-root)
            $child (add-descendant $root "/child" 0)]
        (with-awaited-open-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start :node $child}
                                                      #::znode{:type ::znode/created! :node $child :value 0}
                                                      (just #::znode{:type ::znode/datum :node $child :value 0 ::znode/stat (contains {:version 0})})]))))
      (let [$root (create-root)
            $child (add-descendant $root "/child" 1)]
        (with-awaited-open-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start :node $child}
                                                      (just #::znode{:type ::znode/exists :node $child :stat (contains {:version 0})})
                                                      (just #::znode{:type ::znode/datum :node $child :value 0 :stat (contains {:version 0})})])))))

(fact "ZNodes stream pushed values"
      (let [$root0 (create-root)
            $child0 (add-descendant $root0 "/child" 0)
            $root1 (create-root)
            $child1 (add-descendant $root1 "/child" 0)]
        (with-awaited-open-connection $root0 (str connect-string sandbox) 500
          $child0 => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start :node $child0}
                                                       #::znode{:type ::znode/created! :node $child0 :value 0}
                                                       (just #::znode{:type ::znode/datum :node $child0 :value 0 :stat (contains {:version 0})})]))
          (with-awaited-open-connection $root1 (str connect-string sandbox) 500
            $child1 => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start :node $child1}
                                                         (just #::znode{:type ::znode/exists :node $child1 :stat (contains {:version 0})})
                                                         (just #::znode{:type ::znode/datum :node $child1 :value 0 :stat (contains {:version 0})})]))
            (compare-version-and-set! $child0 0 1)
            $child0 => (eventually-streams 2 2000 (just [#::znode{:type ::znode/set! :node $child0 :value 1 :version 0}
                                                         (just #::znode{:type ::znode/datum :node $child0 :value 1 :stat (contains {:version 1})})]))
            $child1 => (eventually-streams 1 2000 (just [(just #::znode{:type ::znode/datum :node $child1 :value 1 :stat (contains {:version 1})})]))))))

(fact "Existing ZNodes stream pushed values exactly once"
      (let [$root0 (create-root)
            $child0 (add-descendant $root0 "/child" 0)
            $root1 (create-root)
            $child1 (add-descendant $root1 "/child" 0)]
        (with-awaited-open-connection $root0 (str connect-string sandbox) 500
          $child0 => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start :node $child0}
                                                       #::znode{:type ::znode/created! :node $child0 :value 0}
                                                       (just #::znode{:type ::znode/datum :node $child0 :value 0 :stat (contains {:version 0})})])))
        $child0 => (eventually-streams 1 1000 [#::znode{:type ::znode/watch-stop :node $child0}])
        (with-awaited-open-connection $root1 (str connect-string sandbox) 500
          $child1 => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start :node $child1}
                                                       (just #::znode{:type ::znode/exists :node $child1 :stat (contains {:version 0})})
                                                       (just #::znode{:type ::znode/datum :node $child1 :value 0 :stat (contains {:version 0})})]))
          $child1 => (eventually-streams 1 2000 ::timeout))))

(fact "Root node behaves like leaves"
      (let [$root (create-root "/" 10)]
        (with-awaited-open-connection $root (str connect-string sandbox) 500
          $root => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start :node $root}
                                                     #::znode{:type ::znode/created! :node $root :value 10}
                                                     (just #::znode{:type ::znode/datum :node $root :value 10 :stat (contains {:version 0})})]))))

      (let [$root (create-root "/" nil)]
        (with-awaited-open-connection $root (str connect-string sandbox) 500
          $root => (eventually-streams 3 2000 (just [#::znode{:type ::znode/watch-start :node $root}
                                                     (just #::znode{:type ::znode/exists :node $root :stat (contains {:version 0})})
                                                     (just #::znode{:type ::znode/datum :node $root :value 10 :stat (contains {:version 0})})])))))

(fact "ZNodes can be deleted"
      (let [$root (create-root)
            $child (add-descendant $root "/child" 0)]
        (with-awaited-open-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start :node $child}
                                                      #::znode{:type ::znode/created! :node $child :value 0}
                                                      (just #::znode{:type ::znode/datum :node $child :value 0 :stat (contains {:version 0})})]))
          (delete $child 0) => truthy
          $child => (eventually-streams 2 3000 (just #{#::znode{:type ::znode/deleted! :node $child}
                                                       #::znode{:type ::znode/watch-stop :node $child}})))))

(fact "Existing ZNodes can be deleted"
      (let [$root (create-root)
            $child (add-descendant $root "/child" 0)]
        (with-awaited-open-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start :node $child}
                                                      #::znode{:type ::znode/created! :node $child :value 0}
                                                      (just #::znode{:type ::znode/datum :node $child :value 0 :stat (contains {:version 0})})])))
        $child => (eventually-streams 1 1000 [#::znode{:type ::znode/watch-stop :node $child}])
        (with-awaited-open-connection $root (str connect-string sandbox) 500
          $child => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start :node $child}
                                                      (just #::znode{:type ::znode/exists :node $child :stat (contains {:version 0})})
                                                      (just #::znode{:type ::znode/datum :node $child :value 0 :stat (contains {:version 0})})]))
          (delete $child 0) => truthy
          $child => (eventually-streams 2 3000 (just #{#::znode{:type ::znode/deleted! :node $child}
                                                       #::znode{:type ::znode/watch-stop :node $child}})))))

(fact "Children do not intefere with their parents, regardless of insertion sequence"
      (let [$root (create-root)
            $zB (add-descendant $root "/myzref/child" "B")
            $zA (add-descendant $root "/myzref" "A")]
        (with-awaited-open-connection $root (str connect-string sandbox) 500
          $zA => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start :node $zA}
                                                   #::znode{:type ::znode/created! :node $zA :value "A"}
                                                   (just #::znode{:type ::znode/datum :node $zA :value "A" :stat (contains {:version 0})})]))
          $zB => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start :node $zB}
                                                   #::znode{:type ::znode/created! :node $zB :value "B"}
                                                   (just #::znode{:type ::znode/datum :node $zB :value "B" :stat (contains {:version 0})})])))
        $zA => (eventually-streams 1 2000 (just [#::znode{:type ::znode/watch-stop :node $zA}]))
        $zB => (eventually-streams 1 2000 (just [#::znode{:type ::znode/watch-stop :node $zB}]))))

(fact "A connected ZRef's watches are called when updated by changes at the cluster"
      (let [$root (create-root)
            $z (add-descendant $root "/myzref" "A")]
        (with-open [c (zoo/connect (str connect-string sandbox))]
          (with-awaited-open-connection $root (str connect-string sandbox) 500
            $z => (eventually-streams 3 3000 (just [#::znode{:type ::znode/watch-start :node $z}
                                                    #::znode{:type ::znode/created! :node $z :value "A"}
                                                    (just #::znode{:type ::znode/datum :node $z :value "A" :stat (contains {:version 0})})]))
            (zoo/set-data c "/myzref" (znode/*serialize* "B") 0)
            $z => (eventually-streams 1 3000 (just [(just #::znode{:type ::znode/datum :node $z :value "B" :stat (contains {:version 1})})]))))))
