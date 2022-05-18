(ns integration.zk.zref-test
  (:require [zk.zref :refer :all :as z]
            [zk.client :as zclient]
            [zk.node :as znode]
            [clojure.core.async :as async]
            [zookeeper :as zoo]
            [clojure.tools.logging :as log]
            [clojure.test :refer [use-fixtures deftest testing is assert-expr]]
            [testit.core :refer :all]
            [integration.zk.test-helper :as th :refer [streams *stat? initial-stat? with-metadata?]])
  (:import [org.apache.curator.test TestingServer]))

(def bogus-host "127.1.1.1:9999")
(def ^:dynamic *connect-string*)
(def sandbox "/sandbox")

(defmacro with-awaited-connection
  [root & body]
  `(let [root# ~root]
     (with-open [chandle# (znode/open root# (str *connect-string* sandbox))]
       @chandle#
       ~@body)))

(use-fixtures :once (fn [f] (with-open [test-server (TestingServer. true)]
                              (binding [*connect-string* (.getConnectString test-server)]
                                (f)))))

(use-fixtures :each (fn [f] (with-open [c (zoo/connect *connect-string*)]
                              (do ; binding [*c* c]
                                (zoo/delete-all c sandbox)
                                (zoo/create c sandbox :persistent? true :async? false :data (.getBytes ":zk.node/root"))
                                (f)))))

(deftest zref-reflects-initial-default-value-upon-creation
  (let [$root (znode/new-root)
        v (with-meta ["A"] {:foo true}) ; fuck midje
        $z (create $root "/myzref" v)]
    (facts (versioned-deref $z) =in=> [(with-metadata? ^:foo ["A"]) -1]
           (meta $z) =in=> {:version -1})))

(deftest zref-reflects-initial-default-value-upon-actualization
  (let [$root (znode/new-root)
        v (with-meta ["A"] {:foo true})
        $z (create $root "/myzref" v)]
    (with-awaited-connection $root
      (facts (.vDeref $z) =eventually-in=> [(with-metadata? ^:foo ["A"]) 0]
             (meta $z) =in=> {:version 0}))))

(deftest update-connected-zref
  (let [$root (znode/new-root)
        $z0 (create $root "/zref0" "A")
        $z1 (create $root "/zref1" "A")
        $z2 (create $root "/zref2" 1)]
    (with-awaited-connection $root
      (facts (.vDeref $z0) =eventually-in=> ["A" 0]
             (.compareVersionAndSet $z0 0 "B") => truthy
             (.vDeref $z0) =eventually-in=> ["B" 1]
             (.compareVersionAndSet $z0 12 "C") => falsey

             (.vDeref $z1) =eventually-in=> ["A" 0]
             (compare-and-set! $z1 "Z" "B") => false
             (compare-and-set! $z1 "A" "B") => true
             (.vDeref $z1) =eventually-in=> ["B" 1]
             (reset! $z1 "C") => "C"
             (.vDeref $z1) =eventually-in=> ["C" 2]

             (.vDeref $z2) =eventually-in=> [1 0]
             (swap! $z2 inc) => 2
             (.vDeref $z2) =eventually-in=> [2 1]
             (swap! $z2 inc) => 3
             (.vDeref $z2) =eventually-in=> [3 2]))))

(deftest connected-zref-updated-by-pushed-values
  (let [$root (znode/new-root)
        $z (create $root "/myzref" "A")]
    (with-awaited-connection $root
      (let [c (zoo/connect (str *connect-string* sandbox))]
        (facts (.vDeref $z) =eventually-in=> ["A" 0])
        (zoo/set-data c "/myzref" (.getBytes "^{:foo 2} [\"B\"]") 0)
        (facts (.vDeref $z) =eventually-in=> [(with-metadata? ^{:foo 2} ["B"]) 1])
        (meta $z) => {:version 1}))))

(deftest connected-zref-watches-called-when-values-pushed
  (let [$root (znode/new-root)
        v (with-meta [:A] {:foo true})
        $z (create $root "/myzref" v)
        sync (promise)
        v-sync (promise)]
    (with-open [c (zoo/connect (str *connect-string* sandbox))]
      (with-awaited-connection $root
        (facts (.vDeref $z) =eventually-in=> [(with-metadata? ^:foo [:A]) 0])
        (add-watch $z :sync (fn [& args] (deliver sync args)))
        (add-versioned-watch $z :v-sync (fn [& args] (deliver v-sync args)))
        (zoo/set-data c "/myzref" (.getBytes "^{:bar true} [:B]") 0)
        (facts (.vDeref $z) =eventually-in=> [[:B] 1])
        (deref v-sync 10000 :promise-never-delivered) =in=> [:v-sync $z [[:A] 0] [[:B] 1]]
        (deref sync 10000 :promise-never-delivered) =in=> [:sync $z [:A] [:B]]))))

(deftest connected-zref-is-not-updated-by-invalid-pushed-values
  (let [$root (znode/new-root)
        $z (create $root "/myzref" "A" :validator string?)
        sync (promise)]
    (with-open [c (zoo/connect (str *connect-string* sandbox))]
      (with-awaited-connection $root
        (facts (.vDeref $z) =eventually-in=> ["A" 0])
        (add-watch $z :sync (fn [& args] (deliver sync args)))
        (zoo/set-data c "/myzref" (.getBytes "23") 0)
        (facts (deref sync 1000 ::not-delivered) => ::not-delivered)
        (zoo/set-data c "/myzref" (.getBytes "\"B\"") 1)
        (facts (.vDeref $z) =eventually-in=> ["B" 2])))))

(deftest child-zrefs-do-not-interfere-with-their-parents
  (let [$root (znode/new-root)
        $zB (create $root "/myzref/child" "B" :validator string?)
        $zA (create $root "/myzref" "A" :validator string?)
        sync-a (promise)
        sync-b (promise)]
    (with-open [c (zoo/connect (str *connect-string* sandbox))]
      (with-awaited-connection $root
        (facts (.vDeref $zA) =eventually-in=> ["A" 0]
               (.vDeref $zB) =eventually-in=> ["B" 0])
        (add-watch $zA :sync (fn [& args] (deliver sync-a args)))
        (add-watch $zB :sync (fn [& args] (deliver sync-b args)))
        (zoo/set-data c "/myzref" (.getBytes "\"a\"") 0)
        (zoo/set-data c "/myzref/child" (.getBytes "\"b\"") 0)
        (facts @sync-a =in=> [:sync (partial instance? zk.zref.ZRef) "A" "a"]
               @sync-b =in=> [:sync (partial instance? zk.zref.ZRef) "B" "b"]
               (deref $zA) => "a"
               (deref $zB) => "b")))))

(deftest zref-deleted-at-cluster-throws-on-update-but-otherwise-behaves
  (let [$root (znode/new-root)
        $z (create $root "/myzref" "A")
        sync (promise)]
    (with-open [c (zoo/connect (str *connect-string* sandbox))]
      (with-awaited-connection $root
        (facts (.vDeref $z) =eventually-in=> ["A" 0])
        (zoo/delete c "/myzref")
        (facts (compare-and-set! $z "A" "B") =throws=> (ex-info? #".*" {::znode/path "/myzref"})
               (deref $z) => "A")))))
