(ns integration.zk.node
  (:require [zk.node :refer :all :as znode]
            [zk.client :as zclient]
            [clojure.core.async :as async]
            [zookeeper :as zoo]
            [clojure.tools.logging :as log]
            [integration.zk.test-helper :refer [eventually-streams stat? event?] :as th]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]])
  (:import [org.apache.curator.test TestingServer TestingCluster]
           [org.apache.zookeeper ZooKeeper]
           [zk.client ZClient]))

(def test-server (TestingServer. true))
(def connect-string (.getConnectString test-server))

(def sandbox "/sandbox")

(defchecker having-metadata [expected-value expected-metadata]
  (every-checker (chatty-checker [actual] (extended-= actual expected-value))
                 (chatty-checker [actual] (extended-= (meta actual) expected-metadata))))

(defchecker has-ref-state [expected-value]
  (let [builder (fn [actual] (dosync [(deref (.sref actual)) (deref (.vref actual)) (deref (.cref actual))]))]
    (chatty-checker [actual] (extended-= (builder actual) (just expected-value)))))

(background (around :facts (with-open [c (zoo/connect connect-string)]
                             (zoo/delete-all c sandbox)
                             (zoo/create c sandbox :persistent? true :async? false :data (.getBytes ":zk.node/root"))
                             ?form)))

(fact "Can open and close a ZNode"
      (let [$root (new-root)
            chandle (open $root (str connect-string sandbox))]
        chandle => (partial instance? java.lang.AutoCloseable)
        (.close chandle) => nil?))

(fact "Root ZNode streams exists data"
      (let [$root (new-root)]
        (with-open [_ (open $root (str connect-string sandbox))]
          $root => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/exists :stat (stat? {:version 0})})
                                                     (just #::znode{:type ::znode/synchronized :stat (stat? {})})])))
        $root => (has-ref-state [(stat? {:version 0}) ::znode/root empty?])))

(fact "ZNodes can be actualized and stream current value"
      (let [$root (new-root)
            $child (add-descendant $root "/child" 0)]
        (with-open [_ (open $root (str connect-string sandbox))]
          $root => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/exists :stat (stat? {:version 0})})
                                                     (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
          $child => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                      (just #::znode{:type ::znode/synchronized :stat (stat? {})})])))
        $root => (has-ref-state [(stat? {:version 0 :numChildren 1}) :zk.node/root (one-of (partial instance? zk.node.ZNode))])
        $child => (has-ref-state [(stat? {:version 0}) 0 #{}])))

(fact "Actualized ZNodes can be updated"
      (let [$root (new-root)
            $child (add-descendant $root "/child" 0)]
        (with-open [_ (open $root (str connect-string sandbox))]
          $child => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                      (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
          $child => (has-ref-state [(stat? {:version 0}) 0 empty?])
          (update! $child 1 0 {}) => (stat? {})
          $child => (eventually-streams 1 5000 (just [(just #::znode{:type ::znode/data-changed :value 1 :stat (stat? {:version 1})})])))
        $child => (has-ref-state [(stat? {:version 1}) 1 empty?])))

(fact "Existing ZNodes are acquired and stream their current value"
      (let [$root (new-root)
            $child (add-descendant $root "/child" 0)
            $grandchild (add-descendant $root "/child/grandchild" 0)]
        (with-open [_ (open $root (str connect-string sandbox))]
          $child => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                      (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
          $grandchild => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                           (just #::znode{:type ::znode/synchronized :stat (stat? {})})])))
        ;; ensure spurious wrongly-pathed acquired children don't appear
        (let [$root (new-root)]
          (with-open [_ (open $root (str connect-string sandbox))]
            $root => (eventually-streams 3 2000 (just [(just #::znode{:type ::znode/exists :stat (stat? {:version 0})})
                                                       (just #::znode{:type ::znode/sync-children :stat (stat? {:cversion 1})
                                                                      :inserted (one-of (partial instance? zk.node.ZNode))
                                                                      :removed empty?})
                                                       (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
            (let [$child ($root "/child")]
              $child => (partial instance? zk.node.ZNode)
              $child => (eventually-streams 3 2000 (just [(just #::znode{:type ::znode/exists :stat (stat? {:version 0})})
                                                          (just #::znode{:type ::znode/sync-data :value 0 :stat (stat? {:version 0})})
                                                          (just #::znode{:type ::znode/sync-children :stat (stat? {:cversion 1})
                                                                         :inserted (one-of (partial instance? zk.node.ZNode))
                                                                         :removed empty?})]))
              (let [$grandchild ($root "/child/grandchild")]
                $grandchild => (partial instance? zk.node.ZNode)
                $grandchild => (eventually-streams 2 2000 (just [(just #::znode{:type ::znode/exists :stat (stat? {:version 0})})
                                                                 (just #::znode{:type ::znode/sync-data :value 0 :stat (stat? {:version 0})})]))
                $child => (eventually-streams 1 1000 (just [(just #::znode{:type ::znode/synchronized :stat (stat? {})})])))
              $root => (has-ref-state [(stat? {:version 0}) anything (just #{$child})]))))))

(fact "Existing ZNodes do not stream confirmed value at startup when local and remote values are equal"
      (let [$root (new-root)
            $child (add-descendant $root "/child" 0)]
        (with-open [_ (open $root (str connect-string sandbox))]
          $child => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                      (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
          (update! $child 1 0 {})
          $child => (eventually-streams 1 1000 (just [(just #::znode{:type ::znode/data-changed :value 1 :stat (stat? {:version 1})})]))))

      (let [$root (new-root)
            $child (add-descendant $root "/child" 1)]
        (with-open [_ (open $root (str connect-string sandbox))]
          $child => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/exists :stat (stat? {:version 1})})
                                                      (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
          $child => (has-ref-state [(stat? {:version 1}) 1 #{}]))))

(fact "Existing ZNodes even stream version zero value at startup when different from initial value"
      (let [$root (new-root)
            $child (add-descendant $root "/child" 0)]
        (with-open [_ (open $root (str connect-string sandbox))]
          $child => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                      (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))))
      (let [$root (new-root)
            $child (add-descendant $root "/child" 1)]
        (with-open [_ (open $root (str connect-string sandbox))]
          $child => (eventually-streams 3 1000 (just [(just #::znode{:type ::znode/exists :stat (stat? {:version 0})})
                                                      (just #::znode{:type ::znode/sync-data :value 0 :stat (stat? {:version 0})})
                                                      (just #::znode{:type ::znode/synchronized :stat (stat? {})})])))))

(fact "ZNodes stream pushed values"
      (let [$root0 (new-root)
            $child0 (add-descendant $root0 "/child" 0)
            $root1 (new-root)
            $child1 (add-descendant $root1 "/child" 0)]
        (with-open [_ (open $root0 (str connect-string sandbox))]
          $child0 => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                       (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
          (with-open [_ (open $root1 (str connect-string sandbox))]
            $child1 => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/exists :stat (stat? {:version 0})})
                                                         (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
            (update! $child0 1 0 {})
            $child0 => (eventually-streams 1 2000 (just [(just #::znode{:type ::znode/data-changed :value 1 :stat (stat? {:version 1})})]))
            $child1 => (eventually-streams 1 2000 (just [(just #::znode{:type ::znode/data-changed :value 1 :stat (stat? {:version 1})})]))))))

(fact "Existing ZNodes stream pushed values exactly once"
      (let [$root0 (new-root)
            $child0 (add-descendant $root0 "/child" 0)
            $root1 (new-root)
            $child1 (add-descendant $root1 "/child" 0)]
        (with-open [_ (open $root0 (str connect-string sandbox))]
          $child0 => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                       (just #::znode{:type ::znode/synchronized :stat (stat? {})})])))
        (with-open [_ (open $root1 (str connect-string sandbox))]
          $child1 => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/exists :stat (stat? {:version 0})})
                                                       (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
          $root1 => (has-ref-state [(stat? {:version 0}) anything (just #{$child1})]))))

(fact "Root node behaves like a leaf"
      (let [$root (new-root "/" 10)]
        (with-open [_ (open $root (str connect-string sandbox))]
          $root => (eventually-streams 3 1000 (just [(just #::znode{:type ::znode/exists :stat (stat? {})})
                                                     (just #::znode{:type ::znode/sync-data :value ::znode/root :stat (stat? {:version 0})})
                                                     (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
          (update! $root 9 0 {}) => (stat? {})))

      (let [$root (new-root "/" nil)]
        (with-open [_ (open $root (str connect-string sandbox))]
          $root => (eventually-streams 3 1000 (just [(just #::znode{:type ::znode/exists :stat (stat? {:version 1})})
                                                     (just #::znode{:type ::znode/sync-data :value 9 :stat (stat? {:version 1})})
                                                     (just #::znode{:type ::znode/synchronized :stat (stat? {})})])))))

(fact "ZNodes can be deleted"
      (let [$root (new-root)
            $child (add-descendant $root "/child" 0)]
        (with-open [_ (open $root (str connect-string sandbox))]
          $child => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                      (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
          (delete! $child 0 {}) => truthy
          $child => (eventually-streams 1 1000 (just [#::znode{:type ::znode/deleted!}])))
        $root => (has-ref-state [(stat? {:cversion 2}) anything empty?])))

(fact "Existing ZNodes can be deleted"
      (let [$root (new-root)
            $child (add-descendant $root "/child" 0)]
        (with-open [_ (open $root (str connect-string sandbox))]
          $child => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                      (just #::znode{:type ::znode/synchronized :stat (stat? {})})])))
        (with-open [_ (open $root (str connect-string sandbox))]
          $child => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/exists :stat (stat? {:version 0})})
                                                      (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
          (delete! $child 0 {}) => truthy
          $child => (eventually-streams 1 1000 (just #{#::znode{:type ::znode/deleted!}})))))

(fact "Children do not intefere with their parents, regardless of insertion sequence"
      (let [$root (new-root)
            $zB (add-descendant $root "/myzref/child" "B")
            $zA (add-descendant $root "/myzref" "A")]
        (with-open [_ (open $root (str connect-string sandbox))]
          $zA => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                   (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
          $zB => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                   (just #::znode{:type ::znode/synchronized :stat (stat? {})})])))))

(fact "A ZNode can persist metadata"
      (let [$root (new-root)
            $z0 (add-descendant $root "/myzref0" "A")
            $z1 (add-descendant $root "/myzref1" (with-meta {:my "znode" } {:foo 1}))
            $z2 (add-descendant $root "/myzref2" [])]
        (with-open [c (zoo/connect (str connect-string sandbox))]
          (with-open [_ (open $root (str connect-string sandbox))]
            $z0 => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                     (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
            $z1 => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                     (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
            $z2 => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                     (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
            (zoo/set-data c "/myzref0" (.getBytes "^{:my 1} [\"B\"]") 0)
            $z0 => (eventually-streams 1 3000 (just [(contains #::znode{:type ::znode/data-changed :value (having-metadata ["B"] {:my 1})})]))))))

(fact "Children are retained outside sessions"
      (let [$root (new-root)]
        (with-open [c (zoo/connect (str connect-string sandbox))]
          (with-open [_ (open $root (str connect-string sandbox))]
            $root => (eventually-streams 2 2000 (just [(just #::znode{:type ::znode/exists :stat (stat? {})})
                                                       (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
            (zoo/create c "/child" :data (.getBytes "^{:my 1} [\"B\"]"))
            $root => (eventually-streams 1 3000 (just [(contains #::znode{:type ::znode/children-changed})]))
            (Thread/sleep 100)) ; prevent new child's boot from being interrupted and generating exceptions
          $root => (has-ref-state [(stat? {}) ::znode/root (just #{(partial instance? zk.node.ZNode)})]))))

(future-fact "Signature can be computed"
             (let [$root (new-root)
                   $child0 (add-descendant $root "/child0" 0)
                   $child1 (add-descendant $root "/child1" (with-meta #{1 2 3} {:foo "bar"}))
                   $grandchild (add-descendant $root "/child0/grandchild" 0)]
               (with-connection $root (str connect-string sandbox) 500
                 $root => (eventually-streams 3 2000 (just [(just #::znode{:type ::znode/exists :stat (stat? {})})
                                                            (contains #::znode{:type ::znode/datum :value ::znode/root})
                                                            (just #::znode{:type ::znode/children-changed :stat (stat? {:version 0})
                                                                           :inserted #{} :removed #{}})]))
                 $child0 => (eventually-streams 3 2000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                              (contains #::znode{:type ::znode/datum :value 0})
                                                              (just #::znode{:type ::znode/children-changed :stat (stat? {:version 0})
                                                                             :inserted #{} :removed #{}})]))
                 $child1 => (eventually-streams 3 2000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                              (contains #::znode{:type ::znode/datum :value #{1 2 3}})
                                                              (just #::znode{:type ::znode/children-changed :stat (stat? {:version 0})
                                                                             :inserted #{} :removed #{}})]))
                 $grandchild => (eventually-streams 3 3000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                                  (contains #::znode{:type ::znode/datum :value 0})
                                                                  (just #::znode{:type ::znode/children-changed :stat (stat? {:version 0})
                                                                                 :inserted #{} :removed #{}})]))
                 (signature $grandchild) => (just [integer? -1249580007])
                 (signature $root) => (just [integer? -1188681409]))))

(fact "Added descendant ZNodes can be created and will merge into watched tree"
      (let [$root (new-root)
            $child (add-descendant $root "/child" 0)]
        (with-open [_ (open $root (str connect-string sandbox))]
          $root => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/exists :stat (stat? {})})
                                                     (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
          $child => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/created! :stat (stat? {})})
                                                      (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
          (let [$grandchild (add-descendant $root "/child/grandchild" (with-meta #{1 2 3} {:foo "bar"}))]
            (create! $grandchild {}) => (stat? {})
            $grandchild => (eventually-streams 2 1000 (just [(just #::znode{:type ::znode/exists :stat (stat? {:version 0})})
                                                             (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))))
        (let [$grandchild ($root "/child/grandchild")]
          $grandchild => (eventually-streams 1 1000 [::th/timeout]))))

(defchecker znode-at [expected-path]
  (checker [actual] (= (.path actual) expected-path)))

(future-fact "The tree can be walked"
             (let [$root (new-root)
                   $child0 (add-descendant $root "/child0" 0)
                   $child1 (add-descendant $root "/child1" (with-meta #{1 2 3} {:foo "bar"}))
                   $grandchild (add-descendant $root "/child0/grandchild" 0)]
               (with-open [_ (open $root (str connect-string sandbox))]
                 $root => (eventually-streams 4 3000 (contains #{#::znode{:type ::znode/watch-start}}))
                 $child0 => (eventually-streams 4 3000 (contains #{#::znode{:type ::znode/watch-start}}))
                 $child1 => (eventually-streams 4 3000 (contains #{#::znode{:type ::znode/watch-start}}))
                 $grandchild => (eventually-streams 4 3000 (contains #{#::znode{:type ::znode/watch-start}})))
               (znode/walk (str connect-string sandbox) 500 identity conj ()) => (contains #{(znode-at "/") (znode-at "/child0")
                                                                                             (znode-at "/child1") (znode-at "/child0/grandchild")})))

(defchecker named? [expected]
  (checker [actual] (= (name actual) expected)))

(future-fact "The tree can be walked with proper treatment of transducer and scope"
             (let [$root (new-root)
                   $c0 (add-descendant $root "/c0" "c0")
                   $gc10 (add-descendant $c0 "/gc10" "gc10")
                   $gc11 (add-descendant $c0 "/gc11" "gc11")
                   $blacksheep (add-descendant $c0 "/blacksheep" "baa")]
               (with-open [_ (open $root (str connect-string sandbox) {:timeout 5000})]
                 $gc10 => (eventually-streams 4 3000 (contains #{#::znode{:type ::znode/watch-start}}))
                 $gc11 => (eventually-streams 4 3000 (contains #{#::znode{:type ::znode/watch-start}}))
                 $blacksheep => (eventually-streams 4 3000 (contains #{#::znode{:type ::znode/watch-start}})))
               (walk (str connect-string sandbox "/c0") 500 (remove #(re-find #"b" (name %))) conj ())
               => (every-checker (has every? (partial instance? roomkey.znode.ZNode))
                                 (just #{(named? "") (named? "gc10") (named? "gc11")}))))
