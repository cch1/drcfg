(ns unit.zk.node-test
  (:require [zk.node :refer :all :as znode]
            [clojure.core.async :as async]
            [clojure.test :refer [deftest testing is]]))

(deftest create-root-znode
  (let [n (new-root)]
    (is (instance? zk.node.ZNode n))
    (is (= (path n) "/"))))

(deftest nice-string-representation
  (let [n (add-descendant (new-root) "/a" "a")]
    (is (re-matches #".*/a" (str n)))))

(deftest create-psuedo-root
  (let [n (new-root "/myroot")]
    (is (instance? zk.node.ZNode n))
    (is (= (path n) "/myroot"))))

(deftest create-descendants-of-root
  (let [n (new-root)]
    (is (instance? zk.node.ZNode (add-descendant n "/a0" "a0")))
    (is (instance? zk.node.ZNode (add-descendant n "/a1" "a1")))
    (is (instance? zk.node.ZNode (add-descendant n "/a1/b1" "b1")))
    (is (instance? zk.node.ZNode (add-descendant n "/a2/b2/c1/d1/e1" "e1")))))

(deftest create-descendants-of-arbitrary-znodes
  (let [n (add-descendant (new-root) "/a" "a")]
    (is (instance? zk.node.ZNode (add-descendant n "/b" "b")))))

(deftest existing-children-are-never-overwritten
  (let [$root (new-root)
        $z0 (add-descendant $root "/0" "0")]
    (is (thrown-with-msg? AssertionError #"Can't overwrite" (add-descendant $root "/0" "1")))
    (is (= 1 (count $root)))))

(deftest existing-placeholders-may-be-overlaid
  (let [$root (new-root)]
    (add-descendant $root "/1/0" "10") ; Node "/1" is a placeholder node
    (is (instance? zk.node.ZNode (add-descendant $root "/1" "1")))
    (is (= 1 (count $root)))))

(deftest znodes-know-their-path
  (let [n (new-root)]
    (is (= "/" (path n)))
    (is (= "/a/b/c/d" (path (add-descendant n "/a/b/c/d" 4)))))
  (let [n (add-descendant (new-root) "/a" "a")]
    (is (= "/a" (path n)))
    (is (= "/a/b1/c/d" (path (add-descendant n "/b1/c/d" 4)))))
  (let [n (new-root "/a/b/c")]
    (is (= "/a/b/c" (path n)))
    (is (= "/a/b/c/d/e" (path (add-descendant n "/d/e" 4)))))
  (let [n (add-descendant (new-root "/a/b/c") "/d" "a")]
    (is (= "/a/b/c/d" (path n)))
    (is (= "/a/b/c/d/e1/f/g" (path (add-descendant n "/e1/f/g" 4))))))

(deftest znode-implements-Named
  (let [n (new-root)]
    (is (= "" (name n)))
    (is (= nil (namespace n))))
  (let [n (add-descendant (new-root) "/a0" 0)]
    (is (= "a0" (name n)))
    (is (nil?  (namespace n))))
  (let [n (add-descendant (new-root) "/a1/b/c/d" 1)]
    (is (= "d" (name n)))
    (is (= "a1.b.c" (namespace n))))
  (let [n (add-descendant (new-root) "/a1/b/c.d" 1)]
    (is (= "c.d" (name n)))
    (is (= "a1.b" (namespace n))))
  (let [n (new-root "/a/b/c")]
    (is (= "c" (name n)))
    (is (= "a.b" (namespace n))))
  (let [n (add-descendant (new-root "/a/b/c") "/d" 0)]
    (is (= "d" (name n)))
    (is (= "a.b.c" (namespace n))))
  (let [n (add-descendant (new-root "/a/b/c") "/d/e/f/g" 1)]
    (is (= "g" (name n)))
    (is (= "a.b.c.d.e.f" (namespace n)))))

(deftest znode-implements-IMeta
  (let [$root (new-root)
        $child (add-descendant $root "/a0" 0)]
    (is (= -1 (-> $root meta :version)))
    (is (= -1 (-> $root meta :cversion)))
    (is (= -1 (-> $root meta :aversion)))))

(deftest znode-implements-IDeref
  (let [n (add-descendant (new-root) "/a0" 0)]
    (is (zero? (deref n)))))

(deftest znode-implements-Seqable
  (let [$root (new-root)
        $child0 (add-descendant $root "/a0" 0)
        $child1 (add-descendant $root "/a1" 0)]
    ;; TODO: the order of children should be deterministic
    (is (or (= #{$child1 $child0} (set (seq $root)))))))

(deftest znode-implements-Counted
  (let [$root (new-root)
        $child0 (add-descendant $root "/a0" 0)
        $child1 (add-descendant $root "/a1" 0)]
    (is (= 2 (count $root)))))

#_ (deftest znode-implements-ITransientCollection-and-ITransientSet
     (let [$root (new-root)
           z0 (default (.client $root) "/a1")
           z1 (default (.client $root) "/a1")]
       (conj! $root z0) => $root
       (contains? $root z0) => true
       (get $root z0) => z0 ; short-circuits to ILookup/valAt with string key
       (disj! $root z0) => $root
       (contains? $root z0) => false
       (get $root z0) => falsey))

(deftest znode-implements-IFn
  (let [$root (new-root)
        $child (add-descendant $root "/a/b/c" 0)]
    (is (instance? zk.node.ZNode ($root "/a")))
    (is (nil? ($root "/a/dddb/c")))
    (is (instance? zk.node.ZNode (apply $root ["/a/b"])))
    (is (= $child ($root "/a/b/c")))
    (is (= $child ($root :a.b/c)))
    (is (= $child ($root 'a.b/c)))
    (is (= $child ($root ["a.b" :c])))))

#_ (future-deftest "ZNode supports `impl/ReadPort` and `impl/Channel`"
                   (let [$root (new-root)
                         c (async/chan)
                         p (async/pub c identity)]
                     (watch $root p) ; This is going to fire up core.async state machines...
                     (async/close! c)))

#_ (future-deftest "ZNodes have two kinds of \"consistent\" signatures" ; which are hopefully stable across many useful instances of the Clojure runtime
                   (let [$root (new-root)
                         $child (add-descendant $root "/a1" "a1")]
                     (add-descendant $root "/a" "a")
                     (add-descendant $root "/a/b0" "b0")
                     (add-descendant $root "/a/b1" "b1")
                     (let [[stat-signature value-signature :as s] (signature $root)
                           [stat-signature-child value-signature-child :as s-child] (signature $child)]
                       (signature $root) => s
                       (add-descendant $root "/a/b1/c1" "c1")
                       (signature $child) => s-child)))

(deftest znodes-are-comparable
  (let [$root (new-root)
        $child0 (add-descendant $root "/child0" 0)
        $child1 (add-descendant $root "/child1" (with-meta #{1 2 3} {:foo "bar"}))
        $grandchild (add-descendant $root "/child0/grandchild" 0)]
    (is (neg? (compare $child0 $child1)))
    (is (pos? (compare $child1 $child0)))
    (is (zero? (compare $child0 $child0)))
    (is (= [$root $child0 $grandchild $child1] (sort (shuffle [$root $child0 $grandchild $child1]))))))

(deftest znode-identity-derives-from-path-and-client
  (let [$root (new-root)
        $child0 (add-descendant $root "/0" 0)
        $child1 (add-descendant $root "/1" 1)
        x ($root "/0")]
    (is (not (identical? $child0 $child1)))
    (is (not= $child0 $child1))
    (is (not= (hash $child0) (hash $child1)))
    (is (= #{$child0} (clojure.set/difference #{$child0} #{$child1})))
    (is (identical? x $child0))
    (is (= x $child0))
    (is (= (hash x) (hash $child0)))
    (is (= #{$child0} (clojure.set/difference #{$child0 $child1} #{$child1})))))

(deftest znode-trees-can-be-walked
  (let [root (new-root)
        child0 (add-descendant root "/child0" 0)
        child1 (add-descendant root "/child1" (with-meta #{1 2 3} {:foo "bar"}))
        grandchild (add-descendant root "/child0/grandchild" 0)]
    (is (= #{root child0 child1 grandchild} (set (walk root)))))) ; there is a partial order, but midje is clunky at testing.
