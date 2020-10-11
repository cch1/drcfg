(ns unit.zk.node
  (:require [zk.node :refer :all :as znode]
            [clojure.core.async :as async]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]
            [clojure.test.check.generators :as gen]
            [midje.experimental :refer [for-all]]))

(fact "Can create a root ZNode"
      (let [$root (new-root)]
        $root => (partial instance? zk.node.ZNode)
        (path $root) => "/"))

(fact "Znodes have a nice string representation"
      (let [$root (new-root)
            $child (add-descendant $root "/a" "a")]
        (str $child) => #"/a"))

(fact "Can create a psuedo-root ZNode"
      (let [$root (new-root "/myroot")]
        $root => (partial instance? zk.node.ZNode)
        (path $root) => "/myroot"))

(fact "Can create descendants of the root ZNode"
      (let [$root (new-root)]
        (add-descendant $root "/a0" "a0") => (partial instance? zk.node.ZNode)
        (add-descendant $root "/a1" "a1") => (partial instance? zk.node.ZNode)
        (add-descendant $root "/a1/b1" "b1") => (partial instance? zk.node.ZNode)
        (add-descendant $root "/a2/b2/c1/d1/e1" "e1") => (partial instance? zk.node.ZNode)))

(fact "Can create descendants of arbitrary ZNodes"
      (let [$root (new-root)
            $child (add-descendant $root "/a" "a")]
        (add-descendant $child "/b" "b") => (partial instance? zk.node.ZNode)))

(fact "Existing children are never overwritten but placeholders may be overlaid"
      (let [$root (new-root)
            $z0 (add-descendant $root "/0" "0")]
        (add-descendant $root "/0" "1") => (throws AssertionError #"Can't overwrite")
        (count $root) => 1
        (let [$z10 (add-descendant $root "/1/0" "10")]
          (add-descendant $root "/1" "1") => truthy
          (count $root) => 2)))

(fact "ZNodes know their path in the tree"
      (let [$root (new-root)
            $child (add-descendant $root "/a" "a")]
        (path $root) => "/"
        (path $child) => "/a"
        (path (add-descendant $root "/a/b/c/d" 4)) => "/a/b/c/d"
        (path (add-descendant $child "/b1/c/d" 4)) => "/a/b1/c/d")
      (let [$root (new-root "/a/b/c")
            $child (add-descendant $root "/d" "a")]
        (path $root) => "/a/b/c"
        (path $child) => "/a/b/c/d"
        (path (add-descendant $root "/d/e" 4)) => "/a/b/c/d/e"
        (path (add-descendant $child "/e1/f/g" 4)) => "/a/b/c/d/e1/f/g"))

(fact "ZNode supports `cloure.lang.Named`"
      (let [$root (new-root)
            $child (add-descendant $root "/a0" 0)
            $g4 (add-descendant $root "/a1/b/c/d" 1)]
        (name $root) => ""
        (namespace $root) => nil
        (name $child) => "a0"
        (namespace $child) => "/"
        (name $g4) => "d"
        (namespace $g4) => "/a1/b/c")
      (let [$psuedo-root (new-root "/a/b/c")
            $child (add-descendant $psuedo-root "/d" 0)
            $g4 (add-descendant $psuedo-root "/d/e/f/g" 1)]
        (name $psuedo-root) => "c"
        (namespace $psuedo-root) => "/a/b"
        (name $child) => "d"
        (namespace $child) => "/a/b/c"
        (name $g4) => "g"
        (namespace $g4) => "/a/b/c/d/e/f"))

(fact "ZNode supports `cloure.lang.IMeta`"
      (let [$root (new-root)
            $child (add-descendant $root "/a0" 0)]
        (meta $root) => (contains {:version -1 :cversion -1 :aversion -1})))

(fact "ZNode supports `cloure.lang.IDeref`"
      (let [$root (new-root)
            $child (add-descendant $root "/a0" 0)]
        (deref $child) => 0))

(fact "ZNode supports `cloure.lang.Seqable`"
      (let [$root (new-root)
            $child0 (add-descendant $root "/a0" 0)
            $child1 (add-descendant $root "/a1" 0)]
        (seq $root) => (just #{$child0 $child1})))

(fact "ZNode supports `cloure.lang.Counted`"
      (let [$root (new-root)
            $child0 (add-descendant $root "/a0" 0)
            $child1 (add-descendant $root "/a1" 0)]
        (count $root) => 2))

(future-fact "ZNode supports `ITransientCollection` and `ITransientSet`"
             (let [$root (new-root)
                   z0 (default (.client $root) "/a1")
                   z1 (default (.client $root) "/a1")]
               (conj! $root z0) => $root
               (contains? $root z0) => true
               (get $root z0) => z0 ; short-circuits to ILookup/valAt with string key
               (disj! $root z0) => $root
               (contains? $root z0) => false
               (get $root z0) => falsey))

(fact "ZNode supports `IFn`"
      (let [$root (new-root)
            $child (add-descendant $root "/a/b/c" 0)]
        ($root "/a") => (partial instance? zk.node.ZNode)
        ($root "/a/b/c") => $child
        ($root "/a/dddb/c") => nil?
        (apply $root ["/a/b"]) => (partial instance? zk.node.ZNode)))

(future-fact "ZNode supports `impl/ReadPort` and `impl/Channel`"
             (let [$root (new-root)
                   c (async/chan)
                   p (async/pub c identity)]
               (watch $root p) ; This is going to fire up core.async state machines...
               (async/close! c)))

(future-fact "ZNodes have two kinds of \"consistent\" signatures" ; which are hopefully stable across many useful instances of the Clojure runtime
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

(fact "ZNodes are comparable"
      (let [$root (new-root)
            $child0 (add-descendant $root "/child0" 0)
            $child1 (add-descendant $root "/child1" (with-meta #{1 2 3} {:foo "bar"}))
            $grandchild (add-descendant $root "/child0/grandchild" 0)]
        (compare $child0 $child1) => neg?
        (compare $child1 $child0) => pos?
        (compare $child0 $child0) => zero?
        (sort (shuffle [$root $child0 $grandchild $child1])) => (just [$root $child0 $grandchild $child1])))

(fact "ZNodes have identity from their path (and client)"
      (let [$root (new-root)
            $child0 (add-descendant $root "/0" 0)
            $child1 (add-descendant $root "/1" 1)
            x ($root "/0")]
        (identical? $child0 $child1) => false
        (= $child0 $child1) => false
        (= (hash $child0) (hash $child1)) => false
        (clojure.set/difference #{$child0} #{$child1}) => #{$child0}
        (identical? x $child0) => true
        (= x $child0) => true
        (= (hash x) (hash $child0)) => true
        (clojure.set/difference #{$child0 $child1} #{$child1}) => #{$child0}))
