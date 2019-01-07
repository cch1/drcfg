(ns roomkey.unit.znode
  (:require [roomkey.znode :refer :all]
            [roomkey.zclient :as zclient]
            [clojure.core.async :as async]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]))

(def $client (reify
               clojure.core.async/Mult ; need this to handle the client watching on root nodes
               (tap* [m ch close?] (async/timeout 1000))))

(fact "Can create a root ZNode"
      (create-root $client) => (partial instance? roomkey.znode.ZNode))

(fact "Can create descendants of the root ZNode"
      (let [$root (create-root $client)]
        ;; (.zConnect $z) => (partial satisfies? clojure.core.async.impl.protocols/Channel)
        (add-descendant $root "/a0" "a0") => (partial instance? roomkey.znode.ZNode)
        (add-descendant $root "/a1" "a1") => (partial instance? roomkey.znode.ZNode)
        (add-descendant $root "/a1/b1" "b1") => (partial instance? roomkey.znode.ZNode)
        (add-descendant $root "/a2/b2/c1/d1/e1" "e1") => (partial instance? roomkey.znode.ZNode)))

(fact "Can create descendants of arbitrary znodes"
      (let [$root (create-root $client)
            $child (add-descendant $root "/a" "a")]
        (add-descendant $child "/b" "b") => (partial instance? roomkey.znode.ZNode)))

(fact "ZNodes know their path in the tree"
      (let [$root (create-root $client)
            $child (add-descendant $root "/a" "a")]
        (path $root) => "/"
        (path $child) => "/a"
        (path (add-descendant $root "/a/b/c/d" 4)) => "/a/b/c/d"
        (path (add-descendant $child "/b1/c/d" 4)) => "/a/b1/c/d"))

(fact "ZNodes can be accessed by path"
      (let [$root (create-root $client)
            $child (add-descendant $root "/a0" 0)
            $g4 (add-descendant $root "/a1/b/c/d" 1)]
        (get-in $root ["a0"]) => $child
        (get-in $root ["a1" "b" "c" "d"]) => $g4
        (get-in $root ["a9"]) => nil
        (get-in $root ["a9"] ::not-found) => ::not-found))
