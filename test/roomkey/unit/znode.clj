(ns roomkey.unit.znode
  (:require [roomkey.znode :refer :all]
            [clojure.core.async :as async]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]
            [clojure.test.check.generators :as gen]
            [midje.experimental :refer [for-all]]))

(fact "Can create a root ZNode"
      (let [$root (create-root)]
        $root => (partial instance? roomkey.znode.ZNode)
        (.path $root) => "/"
        (seq $root) => empty?))

(fact "Can create a psuedo-root ZNode"
      (let [$root (create-root "/myroot")]
        $root => (partial instance? roomkey.znode.ZNode)
        (.path $root) => "/myroot"
        (seq $root) => empty?))

(fact "Can create and identify children of a node"
      (let [$root (create-root)]
        (add-descendant $root "/a0" "a0") => (partial instance? roomkey.znode.ZNode)
        (add-descendant $root "/a1" "a1") => (partial instance? roomkey.znode.ZNode)
        (seq $root) => (two-of (partial instance? roomkey.znode.ZNode))))

(for-all
 [int gen/int]
 {:num-tests 1000}
 (fact "ZNodes know their identity"
       (let [$root (create-root)
             $child0 (add-descendant $root "/a" int)
             $child1 (add-descendant $root "/a11/b" 1)
             $child2 (add-descendant $root "/a11/b" 1)
             z0 (default (.client $root) "/a1" 1)
             z1 (default (.client $root) "/a1" 1)
             z2 (default (.client $root) "/a1" 2)
             z3 (default (.client $root) "/a2" 1)]
         (identical? z0 z1) => false
         (identical? z0 z3) => false
         (clojure.set/difference #{z0} #{z1}) => #{}
         (clojure.set/difference #{z0} #{z2}) => #{}
         (clojure.set/difference #{z1} #{z2}) => #{}
         (clojure.set/difference #{z2} #{z1}) => #{}
         (= z0 z1) => true
         (= z0 z2) => true
         (not= z0 z3) => true
         (= (hash z0) (hash z1)) => true
         (= (hash z0) (hash z2)) => true
         (not= (hash z0) (hash z3)) => true
         (= (.hashCode z0) (.hashCode z1)) => true
         (= (.hashCode z0) (.hashCode z2)) => true
         (not= (.hashCode z0) (.hashCode z3)) => true
         (identical? $child1 $child2) => true)))

(for-all
 [int gen/int]
 {:num-tests 1000}
 (fact "Existing children are never overwritten but may be overlaid"
       (let [$root (create-root)
             $child0 (add-descendant $root "/a" int)
             $child1 (add-descendant $root "/a1/b" 1)
             $child2 (add-descendant $root "/a2/b" 2)
             z0 (default (.client $root) "/a1")
             z1 (default (.client $root) "/a1")
             z2 (default (.client $root) "/a2")]
         (identical? z0 z1) => false
         (= z0 z1) => true
         (not= z0 z2) => true
         (= (hash z0) (hash z1)) => true
         (not= (hash z0) (hash z2)) => true
         (= (.hashCode z0) (.hashCode z1)) => true
         (not= (.hashCode z0) (.hashCode z2)) => true
         (.path $child0) => "/a"
         (.path $child1) => "/a1/b"
         (.path $child2) => "/a2/b"
         (identical? $child1 $child2) => false
         )))

(fact "Can create descendants of the root ZNode"
      (let [$root (create-root)]
        (add-descendant $root "/a0" "a0") => (partial instance? roomkey.znode.ZNode)
        (add-descendant $root "/a1" "a1") => (partial instance? roomkey.znode.ZNode)
        (add-descendant $root "/a1/b1" "b1") => (partial instance? roomkey.znode.ZNode)
        (add-descendant $root "/a2/b2/c1/d1/e1" "e1") => (partial instance? roomkey.znode.ZNode)))

(fact "Can create descendants of arbitrary znodes"
      (let [$root (create-root)
            $child (add-descendant $root "/a" "a")]
        (add-descendant $child "/b" "b") => (partial instance? roomkey.znode.ZNode)))

(fact "ZNodes know their path in the tree"
      (let [$root (create-root)
            $child (add-descendant $root "/a" "a")]
        (.path $root) => "/"
        (.path $child) => "/a"
        (.path (add-descendant $root "/a/b/c/d" 4)) => "/a/b/c/d"
        (.path (add-descendant $child "/b1/c/d" 4)) => "/a/b1/c/d")
      (let [$root (create-root "/a/b/c")
            $child (add-descendant $root "/d" "a")]
        (.path $root) => "/a/b/c"
        (.path $child) => "/a/b/c/d"
        (.path (add-descendant $root "/d/e" 4)) => "/a/b/c/d/e"
        (.path (add-descendant $child "/e1/f/g" 4)) => "/a/b/c/d/e1/f/g"))

(fact "ZNode supports `cloure.lang.Named`"
      (let [$root (create-root)
            $child (add-descendant $root "/a0" 0)
            $g4 (add-descendant $root "/a1/b/c/d" 1)]
        (name $root) => ""
        (namespace $root) => nil
        (name $child) => "a0"
        (namespace $child) => nil
        (name $g4) => "d"
        (namespace $g4) => "/a1/b/c")
      (let [$psuedo-root (create-root "/a/b/c")
            $child (add-descendant $psuedo-root "/d" 0)
            $g4 (add-descendant $psuedo-root "/d/e/f/g" 1)]
        (name $psuedo-root) => "c"
        (namespace $psuedo-root) => "/a/b"
        (name $child) => "d"
        (namespace $child) => "/a/b/c"
        (name $g4) => "g"
        (namespace $g4) => "/a/b/c/d/e/f"))

(fact "ZNode supports `cloure.lang.IMeta`"
      (let [$root (create-root)
            $child (add-descendant $root "/a0" 0)]
        (meta $root) => (contains {:version -1 :cversion -1 :aversion -1})))

(fact "ZNode supports `cloure.lang.IDeref`"
      (let [$root (create-root)
            $child (add-descendant $root "/a0" 0)]
        (deref $child) => 0))

(fact "ZNode supports `cloure.lang.Seqable`"
      (let [$root (create-root)
            $child0 (add-descendant $root "/a0" 0)
            $child1 (add-descendant $root "/a1" 0)]
        (seq $root) => (just #{$child0 $child1})))

(fact "ZNode supports `cloure.lang.Seqable`"
      (let [$root (create-root)
            $child0 (add-descendant $root "/a0" 0)
            $child1 (add-descendant $root "/a1" 0)]
        (count $root) => 2))

(fact "ZNode supports `ITransientCollection` and `ITransientSet`"
      (let [$root (create-root)
            z0 (default (.client $root) "/a1")
            z1 (default (.client $root) "/a1")]
        (conj! $root z0) => $root
        (contains? $root z0) => true
        (get $root z0) => z0 ; short-circuits to ILookup/valAt with string key
        (disj! $root z0) => $root
        (contains? $root z0) => false
        (get $root z0) => falsey))

(fact "ZNode supports `IFn`"
      (let [$root (create-root)
            $child (add-descendant $root "/a/b/c" 0)]
        ($root "/a") => (partial instance? roomkey.znode.ZNode)
        ($root "/a/b/c") => $child
        ($root "/a/dddb/c") => nil?
        (apply $root ["/a/b"]) => (partial instance? roomkey.znode.ZNode)))
