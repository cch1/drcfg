(ns roomkey.unit.znode
  (:require [roomkey.znode :refer :all]
            [clojure.core.async :as async]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]))

(fact "Can create a root ZNode"
      (let [$root (create-root)]
        $root => (partial instance? roomkey.znode.ZNode)
        (path $root) => "/"
        (seq $root) => empty?))

(fact "Can create a psuedo-root ZNode"
      (let [$root (create-root "/myroot")]
        $root => (partial instance? roomkey.znode.ZNode)
        (path $root) => "/myroot"
        (seq $root) => empty?))

(fact "Can create and identify children of a node"
      (let [$root (create-root)]
        (add-descendant $root "/a0" "a0") => (partial instance? roomkey.znode.ZNode)
        (add-descendant $root "/a1" "a1") => (partial instance? roomkey.znode.ZNode)
        (seq $root) => (two-of (partial instance? roomkey.znode.ZNode))))

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
        (path $root) => "/"
        (path $child) => "/a"
        (path (add-descendant $root "/a/b/c/d" 4)) => "/a/b/c/d"
        (path (add-descendant $child "/b1/c/d" 4)) => "/a/b1/c/d")
      (let [$root (create-root "/a/b/c")
            $child (add-descendant $root "/d" "a")]
        (path $root) => "/a/b/c"
        (path $child) => "/a/b/c/d"
        (path (add-descendant $root "/d/e" 4)) => "/a/b/c/d/e"
        (path (add-descendant $child "/e1/f/g" 4)) => "/a/b/c/d/e1/f/g"))

(fact "ZNodes can be accessed by path"
      (let [$root (create-root)
            $child (add-descendant $root "/a0" 0)
            $g4 (add-descendant $root "/a1/b/c/d" 1)]
        (get-in $root ["a0"]) => $child
        (get-in $root ["a1" "b" "c" "d"]) => $g4
        (get-in $root ["a9"]) => nil
        (get-in $root ["a9"] ::not-found) => ::not-found))

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

(fact "ZNode supports `cloure.lang.ILookup`"
      (let [$root (create-root)
            $child (add-descendant $root "/a0" 0)
            $g4 (add-descendant $root "/a1/b/c/d" 1)]
        (get $root "a0") => $child
        (get $root "a") => nil?
        (get-in $root ["a1" "b" "c" "d"]) => $g4))

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
