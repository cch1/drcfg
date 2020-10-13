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

(defchecker named? [expected]
  (checker [actual] (= (name actual) expected)))

(defchecker znode-at [expected-path]
  (checker [actual] (= (.path actual) expected-path)))

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
          $child => (eventually-streams 1 1000 (just [(just #::znode{:type ::znode/data-changed :value 1 :stat (stat? {:version 1})})])))
        $child => (has-ref-state [(stat? {:version 1}) 1 empty?])))

(fact "Existing ZNodes are acquired and stream their current value"
      (let [$root (new-root)]
        (add-descendant $root "/child" 0)
        (add-descendant $root "/child/grandchild" 0)
        (with-open [_ (open $root (str connect-string sandbox))]))
      (let [$root (new-root)]
        (with-open [_ (open $root (str connect-string sandbox))]
          $root => (eventually-streams 3 2000 (just [(just #::znode{:type ::znode/exists :stat (stat? {:version 0})})
                                                     (just #::znode{:type ::znode/sync-children :stat (stat? {:cversion 1})
                                                                    :inserted (just #{(named? "child")})
                                                                    :removed empty?})
                                                     (just #::znode{:type ::znode/synchronized :stat (stat? {})})]))
          (let [$child ($root "/child")]
            $child => (eventually-streams 3 2000 (just [(just #::znode{:type ::znode/exists :stat (stat? {:version 0})})
                                                        (just #::znode{:type ::znode/sync-data :value 0 :stat (stat? {:version 0})})
                                                        (just #::znode{:type ::znode/sync-children :stat (stat? {:cversion 1})
                                                                       :inserted (just #{(named? "grandchild")})
                                                                       :removed empty?})]))
            (let [$grandchild ($root "/child/grandchild")]
              $grandchild => (partial instance? zk.node.ZNode)
              $grandchild => (eventually-streams 2 2000 (just [(just #::znode{:type ::znode/exists :stat (stat? {:version 0})})
                                                               (just #::znode{:type ::znode/sync-data :value 0 :stat (stat? {:version 0})})]))
              $child => (eventually-streams 1 1000 (just [(just #::znode{:type ::znode/synchronized :stat (stat? {})})])))
            $root => (has-ref-state [(stat? {:version 0}) anything (just #{$child})])))))

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

(fact "Cluster-only children will be adopted even while local-only nodes are persisted."
      (let [$root (new-root)
            $child0 (add-descendant $root "/child0" 0)
            $child1 (add-descendant $root "/child1" 1)]
        (with-open [_ (open $root (str connect-string sandbox))]))
      (let [$root (new-root)
            $child2 (add-descendant $root "/child2" 2)
            $child3 (add-descendant $root "/child3" 3)]
        (with-open [_ (open $root (str connect-string sandbox))]
          $root => (eventually-streams 3 2000 (just [(just #::znode{:type ::znode/exists :stat (stat? {})})
                                                     (just #::znode{:type ::znode/sync-children :stat (stat? {})
                                                                    :inserted (just #{(named? "child0") (named? "child1")}) :removed #{}})
                                                     (just #::znode{:type ::znode/synchronized :stat (stat? {:numChildren 4})})])))
        $root => (has-ref-state [(stat? {:numChildren 4}) ::znode/root (just #{(named? "child0") (named? "child1") (named? "child2") (named? "child3")})])))

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

(fact "The tree can be walked"
      (let [root (new-root)
            child0 (add-descendant root "/child0" 0)
            child1 (add-descendant root "/child1" (with-meta #{1 2 3} {:foo "bar"}))
            grandchild (add-descendant root "/child0/grandchild" 0)]
        (walk root) => (just #{root child0 child1 grandchild}) ; walk the tree offline
        (with-open [_ (open root (str connect-string sandbox))])  ; actualize the tree
        (let [root' (new-root)]
          (with-open [_ (open root' (str connect-string sandbox))] ; walk the discovered tree
            (walk root' name) => (just #{"" "child0" "child1" "grandchild"})))))
