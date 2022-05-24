(ns integration.zk.node-test
  (:require [zk.node :refer :all :as znode]
            [zk.client :as zclient]
            [clojure.core.async :as async]
            [zookeeper :as zoo]
            [clojure.tools.logging :as log]
            [clojure.test :refer [use-fixtures deftest testing is assert-expr]]
            [testit.core :refer :all]
            [integration.zk.test-helper :as th :refer [streams *stat? initial-stat? with-metadata?]])
  (:import [org.apache.curator.test TestingServer TestingCluster]
           [org.apache.zookeeper ZooKeeper]
           [zk.client ZClient]))


(def sandbox "/sandbox")
(def ^:dynamic *c*)
(def ^:dynamic *connect-string*)

(defn ref-state
  [actual]
  (dosync
   [(deref (.sref actual))
    (deref (.vref actual))
    (deref (.cref actual))]))

(defn named-nodes? [names]
  (fn [actual]
    (= names (set (map name actual)))))

(use-fixtures :once (fn [f] (with-open [test-server (TestingServer. true)]
                              (binding [*connect-string* (.getConnectString test-server)]
                                (f)))))

(use-fixtures :each (fn [f] (with-open [c (zoo/connect *connect-string*)]
                              (binding [*c* c]
                                (zoo/delete-all c sandbox)
                                (zoo/create c sandbox :persistent? true :async? false :data (.getBytes ":zk.node/root"))
                                (f)))))

(deftest open-and-close-node
  (let [$root (new-root)
        chandle (open $root (str *connect-string* sandbox))]
    (is (instance? java.lang.AutoCloseable chandle))
    (is (nil? (.close chandle)))))

(deftest root-node-streams-exists-data
  (let [$root (new-root)]
    (with-open [_ (open $root (str *connect-string* sandbox))]
      (facts
          (streams 2 1000 $root) =in=> [#::znode{:type ::znode/exists :stat (*stat? {:version 0})}
                                        #::znode{:type ::znode/synchronized :stat (*stat?)}])
      (facts
          (ref-state $root) =in=> [(*stat? {:version 0}) ::znode/root #{}]))))

(deftest actualized-nodes-stream-current-value
  (let [$root (new-root)
        $child (add-descendant $root "/child" 0)]
    (with-open [_ (open $root (str *connect-string* sandbox))]
      (facts (streams 2 1000 $root) =in=> [#::znode{:type ::znode/exists :stat (*stat? {:version 0})}
                                           #::znode{:type ::znode/synchronized :stat (*stat? {})}]
             (streams 2 1000 $child) =in=> [#::znode{:type ::znode/created! :stat (*stat? {})}
                                            #::znode{:type ::znode/synchronized :stat (*stat? {})}]))
    (facts (ref-state $root) =in=> [(*stat? {:version 0 :numChildren 1}) :zk.node/root (comp (partial instance? zk.node.ZNode) first)]
           (ref-state $child) =in=> [(*stat? {:version 0}) 0 #{}])))

(deftest actualized-nodes-can-be-updated
  (let [$root (new-root)
        $child (add-descendant $root "/child" 0)]
    (with-open [_ (open $root (str *connect-string* sandbox))]
      (facts (streams 2 1000 $child) =in=> [#::znode{:type ::znode/created! :stat (*stat? {})}
                                            #::znode{:type ::znode/synchronized :stat (*stat? {})}]
             (ref-state $child) =in=> [(*stat? {:version 0}) 0 #{}]
             (update! $child 1 0 {}) =in=> (*stat? {})
             (streams 1 1000 $child) =in=> [#::znode{:type ::znode/data-changed :value 1 :stat (*stat? {:version 1})}]
             (ref-state $child) =in=> [(*stat? {:version 1}) 1 #{}]))))

(deftest existing-nodes-are-acquired-and-stream-their-current-value
  (let [$root (new-root)]
    (add-descendant $root "/child" 0)
    (add-descendant $root "/child/grandchild" 0)
    (with-open [_ (open $root (str *connect-string* sandbox))]))
  (let [$root (new-root)]
    (with-open [_ (open $root (str *connect-string* sandbox))]
      (facts (streams 3 2000 $root) =in=> [#::znode{:type ::znode/exists :stat (*stat? {:version 0})}
                                           #::znode{:type ::znode/sync-children :stat (*stat? {:cversion 1})
                                                    :inserted (named-nodes? #{"child"})
                                                    :removed #{}}
                                           #::znode{:type ::znode/synchronized :stat (*stat? {})}])
      (let [$child ($root "/child")]
        (facts (streams 3 2000 $child) =in=> [#::znode{:type ::znode/exists :stat (*stat? {:version 0})}
                                              #::znode{:type ::znode/sync-data :value 0 :stat (*stat? {:version 0})}
                                              #::znode{:type ::znode/sync-children :stat (*stat? {:cversion 1})
                                                       :inserted (named-nodes? #{"grandchild"})
                                                       :removed #{}}])
        (let [$grandchild ($root "/child/grandchild")]
          (facts $grandchild => (partial instance? zk.node.ZNode)
                 (streams 2 2000 $grandchild) =in=> [#::znode{:type ::znode/exists :stat (*stat? {:version 0})}
                                                     #::znode{:type ::znode/sync-data :value 0 :stat (*stat? {:version 0})}]
                 (streams 1 1000 $child) =in=> [#::znode{:type ::znode/synchronized :stat (*stat? {})}]
                 (ref-state $root) =in=> [(*stat? {:version 0}) any #{$child}]))))))

(deftest existing-nodes-do-not-stream-confirmed-value-at-startup-when-local-and-remote-values-are-equal
  (let [$root (new-root)
        $child (add-descendant $root "/child" 0)]
    (with-open [_ (open $root (str *connect-string* sandbox))]
      (facts (streams 2 1000 $child) =in=> [#::znode{:type ::znode/created! :stat (*stat? {})}
                                            #::znode{:type ::znode/synchronized :stat (*stat? {})}])
      (update! $child 1 0 {})
      (facts (streams 1 1000 $child) =in=> [#::znode{:type ::znode/data-changed :value 1 :stat (*stat? {:version 1})}])))

  (let [$root (new-root)
        $child (add-descendant $root "/child" 1)]
    (with-open [_ (open $root (str *connect-string* sandbox))]
      (facts (streams 2 1000 $child) =in=> [#::znode{:type ::znode/exists :stat (*stat? {:version 1})}
                                            #::znode{:type ::znode/synchronized :stat (*stat? {})}]
             (ref-state $child) =in=> [(*stat? {:version 1}) 1 #{}]))))

(deftest existing-nodes-stream-version-zero-value-at-startup-when-it-differs-from-initial-value
  (let [$root (new-root)
        $child (add-descendant $root "/child" 0)]
    (with-open [_ (open $root (str *connect-string* sandbox))]
      (facts (streams 2 1000 $child) =in=> [#::znode{:type ::znode/created! :stat (*stat? {})}
                                            #::znode{:type ::znode/synchronized :stat (*stat? {})}])))
  (let [$root (new-root)
        $child (add-descendant $root "/child" 1)]
    (with-open [_ (open $root (str *connect-string* sandbox))]
      (facts (streams 3 1000 $child) =in=> [#::znode{:type ::znode/exists :stat (*stat? {:version 0})}
                                            #::znode{:type ::znode/sync-data :value 0 :stat (*stat? {:version 0})}
                                            #::znode{:type ::znode/synchronized :stat (*stat? {})}]))))

(deftest nodes-stream-pushed-values
  (let [$root0 (new-root)
        $child0 (add-descendant $root0 "/child" 0)
        $root1 (new-root)
        $child1 (add-descendant $root1 "/child" 0)]
    (with-open [_ (open $root0 (str *connect-string* sandbox))]
      (facts (streams 2 1000 $child0) =in=> [#::znode{:type ::znode/created! :stat (*stat? {})}
                                             #::znode{:type ::znode/synchronized :stat (*stat? {})}])
      (with-open [_ (open $root1 (str *connect-string* sandbox))]
        (facts (streams 2 1000 $child1) =in=> [#::znode{:type ::znode/exists :stat (*stat? {:version 0})}
                                               #::znode{:type ::znode/synchronized :stat (*stat? {})}])
        (update! $child0 1 0 {})
        (facts (streams 1 2000 $child0) =in=> [#::znode{:type ::znode/data-changed :value 1 :stat (*stat? {:version 1})}]
               (streams 1 2000 $child1) =in=> [#::znode{:type ::znode/data-changed :value 1 :stat (*stat? {:version 1})}])))))

(deftest existing-nodes-stream-pushed-values-exactly-once
  (let [$root0 (new-root)
        $child0 (add-descendant $root0 "/child" 0)
        $root1 (new-root)
        $child1 (add-descendant $root1 "/child" 0)]
    (with-open [_ (open $root0 (str *connect-string* sandbox))]
      (facts (streams 2 1000 $child0) =in=> [#::znode{:type ::znode/created! :stat (*stat? {})}
                                             #::znode{:type ::znode/synchronized :stat (*stat? {})}]))
    (with-open [_ (open $root1 (str *connect-string* sandbox))]
      (facts (streams 2 1000 $child1) =in=> [#::znode{:type ::znode/exists :stat (*stat? {:version 0})}
                                             #::znode{:type ::znode/synchronized :stat (*stat? {})}]
             (ref-state $root1) =in=> [(*stat? {:version 0}) any #{$child1}]))))

(deftest root-node-behaves-like-a-leaf
  (let [$root (new-root "/" 10)]
    (with-open [_ (open $root (str *connect-string* sandbox))]
      (facts (streams 3 1000 $root) =in=> [#::znode{:type ::znode/exists :stat (*stat? {})}
                                           #::znode{:type ::znode/sync-data :value ::znode/root :stat (*stat? {:version 0})}
                                           #::znode{:type ::znode/synchronized :stat (*stat? {})}]
             (update! $root 9 0 {}) => (*stat? {}))))

  (let [$root (new-root "/" nil)]
    (with-open [_ (open $root (str *connect-string* sandbox))]
      (facts (streams 3 1000 $root) =in=> [#::znode{:type ::znode/exists :stat (*stat? {:version 1})}
                                           #::znode{:type ::znode/sync-data :value 9 :stat (*stat? {:version 1})}
                                           #::znode{:type ::znode/synchronized :stat (*stat? {})}]))))

(deftest nodes-can-be-deleted
  (let [$root (new-root)
        $child (add-descendant $root "/child" 0)]
    (with-open [_ (open $root (str *connect-string* sandbox))]
      (facts (streams 2 1000 $child) =in=> [#::znode{:type ::znode/created! :stat (*stat? {})}
                                            #::znode{:type ::znode/synchronized :stat (*stat? {})}]
             (delete! $child 0 {}) => truthy
             (streams 1 1000 $child) =in=> [#::znode{:type ::znode/deleted! :stat initial-stat?}]))
    (facts (ref-state $root) =in=> [(*stat? {:cversion 2}) any #{}])))

(deftest existing-nodes-can-be-deleted
  (let [$root (new-root)
        $child (add-descendant $root "/child" 0)]
    (with-open [_ (open $root (str *connect-string* sandbox))]
      (facts (streams 2 1000 $child) =in=> [#::znode{:type ::znode/created! :stat (*stat? {})}
                                            #::znode{:type ::znode/synchronized :stat (*stat? {})}]))
    (with-open [_ (open $root (str *connect-string* sandbox))]
      (facts (streams 2 1000 $child) =in=> [#::znode{:type ::znode/exists :stat (*stat? {:version 0})}
                                            #::znode{:type ::znode/synchronized :stat (*stat? {})}]
             (delete! $child 0 {}) => truthy
             (streams 1 1000 $child) =in=> [#::znode{:type ::znode/deleted! :stat initial-stat?}]))))

(deftest child-nodes-can-be-inserted-out-of-topological-order
  (let [$root (new-root)
        $zB (add-descendant $root "/myzref/child" "B")
        $zA (add-descendant $root "/myzref" "A")]
    (with-open [_ (open $root (str *connect-string* sandbox))]
      (facts (streams 2 1000 $zA) =in=> [#::znode{:type ::znode/created! :stat (*stat? {})}
                                         #::znode{:type ::znode/synchronized :stat (*stat? {})}]
             (streams 2 1000 $zB) =in=> [#::znode{:type ::znode/created! :stat (*stat? {})}
                                         #::znode{:type ::znode/synchronized :stat (*stat? {})}]))))

(deftest nodes-persist-metadata
  (let [$root (new-root)
        $z0 (add-descendant $root "/myzref0" "A")
        $z1 (add-descendant $root "/myzref1" (with-meta {:my "znode" } {:foo 1}))
        $z2 (add-descendant $root "/myzref2" [])]
    (with-open [c (zoo/connect (str *connect-string* sandbox))]
      (with-open [_ (open $root (str *connect-string* sandbox))]
        (facts (streams 2 1000 $z0) =in=> [#::znode{:type ::znode/created! :stat (*stat? {})}
                                           #::znode{:type ::znode/synchronized :stat (*stat? {})}]
               (streams 2 1000 $z1) =in=> [#::znode{:type ::znode/created! :stat (*stat? {})}
                                           #::znode{:type ::znode/synchronized :stat (*stat? {})}]
               (streams 2 1000 $z2) =in=> [#::znode{:type ::znode/created! :stat (*stat? {})}
                                           #::znode{:type ::znode/synchronized :stat (*stat? {})}])
        (zoo/set-data c "/myzref0" (.getBytes "^{:my 1} [\"B\"]") 0)
        (facts (streams 1 3000 $z0) =in=> [#::znode{:type ::znode/data-changed :value (with-metadata? ^{:my 1} ["B"])}])))))

(deftest child-nodes-are-retained-outside-sessions
  (let [$root (new-root)]
    (with-open [c (zoo/connect (str *connect-string* sandbox))]
      (with-open [_ (open $root (str *connect-string* sandbox))]
        (facts (streams 2 2000 $root) =in=> [#::znode{:type ::znode/exists :stat (*stat? {})}
                                             #::znode{:type ::znode/synchronized :stat (*stat? {})}])
        (zoo/create c "/child" :data (.getBytes "^{:my 1} [\"B\"]"))
        (facts (streams 1 3000 $root) =in=> [#::znode{:type ::znode/children-changed}])
        (Thread/sleep 100)) ; prevent new child's boot from being interrupted and generating exceptions
      (facts (ref-state $root) =in=> [(*stat? {}) ::znode/root (named-nodes? #{"child"})]))))

#_(future-deftest "Signature can be computed"
                  (let [$root (new-root)
                        $child0 (add-descendant $root "/child0" 0)
                        $child1 (add-descendant $root "/child1" (with-meta #{1 2 3} {:foo "bar"}))
                        $grandchild (add-descendant $root "/child0/grandchild" 0)]
                    (with-connection $root (str *connect-string* sandbox) 500
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

(deftest cluster-only-child-nodes-will-be-adopted-while-local-child-nodes-are-persisted
  (let [$root (new-root)
        $child0 (add-descendant $root "/child0" 0)
        $child1 (add-descendant $root "/child1" 1)]
    (with-open [_ (open $root (str *connect-string* sandbox))]))
  (let [$root (new-root)
        $child2 (add-descendant $root "/child2" 2)
        $child3 (add-descendant $root "/child3" 3)]
    (with-open [_ (open $root (str *connect-string* sandbox))]
      (facts (streams 3 2000 $root) =in=> [#::znode{:type ::znode/exists :stat (*stat? {})}
                                           #::znode{:type ::znode/sync-children :stat (*stat? {})
                                                    :inserted (named-nodes? #{"child0" "child1"}) :removed #{}}
                                           #::znode{:type ::znode/synchronized :stat (*stat? {:numChildren 4})}]))
    (facts (ref-state $root) =in=> [(*stat? {:numChildren 4}) ::znode/root (named-nodes? #{"child0" "child1" "child2" "child3"})])))

(deftest created-nodes-will-merge-into-watched-tree
  (let [$root (new-root)
        $child (add-descendant $root "/child" 0)]
    (with-open [_ (open $root (str *connect-string* sandbox))]
      (facts (streams 2 1000 $root) =in=> [#::znode{:type ::znode/exists :stat (*stat? {})}
                                           #::znode{:type ::znode/synchronized :stat (*stat? {})}]
             (streams 2 1000 $child) =in=> [#::znode{:type ::znode/created! :stat (*stat? {})}
                                            #::znode{:type ::znode/synchronized :stat (*stat? {})}])
      (let [$grandchild (add-descendant $root "/child/grandchild" (with-meta #{1 2 3} {:foo "bar"}))]
        (facts (create! $grandchild {}) => (*stat? {})
               (streams 2 1000 $grandchild) =in=> [#::znode{:type ::znode/exists :stat (*stat? {:version 0})}
                                                   #::znode{:type ::znode/synchronized :stat (*stat? {})}])))
    (let [$grandchild ($root "/child/grandchild")]
      (facts (streams 1 1000 $grandchild) =in=> [::th/timeout]))))

(deftest node-tree-can-be-walked
  (let [root (new-root)
        child0 (add-descendant root "/child0" 0)
        child1 (add-descendant root "/child1" (with-meta #{1 2 3} {:foo "bar"}))
        grandchild (add-descendant root "/child0/grandchild" 0)]
    (facts (walk root) =in=> ^:in-any-order [root child0 child1 grandchild]) ; walk the tree offline
    (with-open [_ (open root (str *connect-string* sandbox))])  ; actualize the tree
    (let [root' (new-root)]
      (with-open [_ (open root' (str *connect-string* sandbox))] ; walk the discovered tree
        (facts (walk root' name) =in=> ^:in-any-order ["" "child0" "child1" "grandchild"])))))

(deftest data-less-nodes-do-not-wreak-havoc
  (zoo/create *c* (str sandbox "/my-node"))
  (let [root (new-root)]
    (with-open [_ (open root (str *connect-string* sandbox))]
      (facts (ref-state (root "/my-node")) =in=> [(*stat? {:dataLength 0}) ::znode/placeholder #{}]))))

(deftest start-stop
  (let [$z (new-root)]
    (is (= (repeat 5 true) (for [connect-string (repeat 5 *connect-string*)]
                             (with-open [handle (open $z (str connect-string "/drcfg") :timeout 1000)]
                               true))))))

(deftest throws-on-sync-timeout
  (let [$z (new-root)]
    (is (thrown? clojure.lang.ExceptionInfo (open $z "127.1.1.1:9999" :timeout 1000)))))
