(ns integration.zk.zref
  (:require [zk.zref :refer :all :as z]
            [zk.client :as zclient]
            [zk.node :as znode]
            [clojure.core.async :as async]
            [zookeeper :as zoo]
            [clojure.tools.logging :as log]
            [integration.zk.test-helper :refer [refers-to having-metadata as-ex-info]]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]])
  (:import [org.apache.curator.test TestingServer]))

(def bogus-host "127.1.1.1:9999")
(def test-server (TestingServer. true))
(def connect-string (.getConnectString test-server))

(def sandbox "/sandbox")

(defchecker eventually-refers-to [timeout expected]
  (checker [actual]
           (loop [t timeout]
             (when (pos? t)
               (if-let [result (extended-= (deref actual) expected)]
                 result
                 (do (Thread/sleep 200)
                     (recur (- t 200))))))))

(defchecker eventually-vrefers-to [timeout expected]
  (checker [actual]
           (loop [t timeout]
             (when (pos? t)
               (if-let [result (extended-= (.vDeref actual) expected)]
                 result
                 (do (Thread/sleep 200)
                     (recur (- t 200))))))))

(defmacro with-awaited-connection
  [root & body]
  `(let [root# ~root]
     (with-open [chandle# (znode/open root# (str connect-string sandbox))]
       @chandle#
       ~@body)))

(background (around :facts (with-open [c (zoo/connect connect-string)]
                             (zoo/delete-all c sandbox)
                             (zoo/create c sandbox :persistent? true :async? false :data (.getBytes ":roomkey.znode/root"))
                             ?form)))

(let [counter (atom 0)]
  (defn next-path
    []
    (str "/" (swap! counter inc))))

(fact "A ZRef reflects the initial default value upon creation"
      (let [$root (znode/new-root)
            v (with-meta ["A"] {:foo true}) ; fuck midje
            $z (create $root "/myzref" v)]
        (versioned-deref $z) => (just [(having-metadata ^:foo ["A"]) -1])
        (meta $z) => (contains {:version -1})))

(fact "A ZRef reflects the persisted version of the initial default value upon actualization"
      (let [$root (znode/new-root)
            v (with-meta ["A"] {:foo true})
            $z (create $root "/myzref" v)]
        (with-awaited-connection $root
          $z => (eventually-vrefers-to 2000 (just [(having-metadata ^:foo ["A"]) 0]))
          (meta $z) => (contains {:version 0}))))

(fact "Can update a connected ZRef"
      (let [$root (znode/new-root)
            $z0 (create $root "/zref0" "A")
            $z1 (zref $root "/zref1" "A")
            $z2 (zref $root "/zref2" 1)]
        (with-awaited-connection $root
          $z0 => (eventually-vrefers-to 1000 ["A" 0])
          (.compareVersionAndSet $z0 0 "B") => truthy
          $z0 => (eventually-vrefers-to 1000 ["B" 1])
          (.compareVersionAndSet $z0 12 "C") => falsey

          $z1 => (eventually-vrefers-to 1000 ["A" 0])
          (compare-and-set! $z1 "Z" "B") => false
          (compare-and-set! $z1 "A" "B") => true
          $z1 => (eventually-vrefers-to 1000 ["B" 1])
          (reset! $z1 "C") => "C"
          $z1 => (eventually-vrefers-to 1000 ["C" 2])

          $z2 => (eventually-vrefers-to 1000 [1 0])
          (swap! $z2 inc) => 2
          $z2 => (eventually-vrefers-to 1000 [2 1])
          (swap! $z2 inc) => 3
          $z2 => (eventually-vrefers-to 1000 [3 2]))))

(fact "A connected ZRef is updated by changes at the cluster"
      (let [$root (znode/new-root)
            $z (zref $root "/myzref" "A")]
        (with-awaited-connection $root
          (let [c (zoo/connect (str connect-string sandbox))]
            $z => (eventually-vrefers-to 1000 ["A" 0])
            (zoo/set-data c "/myzref" (.getBytes "^{:foo 2} [\"B\"]") 0)
            $z => (eventually-vrefers-to
                   1000 (just [(having-metadata ^{:foo 2} ["B"]) 1]))
            (meta $z) => (contains {:version 1})))))

(fact "A connected ZRef's watches are called when updated by changes at the cluster"
      (let [$root (znode/new-root)
            v (with-meta [:A] {:foo true})
            $z (zref $root "/myzref" v)
            sync (promise)
            v-sync (promise)]
        (with-open [c (zoo/connect (str connect-string sandbox))]
          (with-awaited-connection $root
            $z => (eventually-vrefers-to 1000 (just [(having-metadata ^:foo [:A]) 0]))
            (add-watch $z :sync (fn [& args] (deliver sync args)))
            (add-versioned-watch $z :v-sync (fn [& args] (deliver v-sync args)))
            (zoo/set-data c "/myzref" (.getBytes "^{:bar true} [:B]") 0)
            $z => (eventually-vrefers-to 1000 [[:B] 1])
            (deref v-sync 10000 :promise-never-delivered) => (just [:v-sync $z [[:A] 0] [[:B] 1]])
            (deref sync 10000 :promise-never-delivered) => (just [:sync $z [:A] [:B]])))))

(fact "A connected ZRef is not updated by invalid values at the cluster"
      (let [$root (znode/new-root)
            $z (zref $root "/myzref" "A" :validator string?)
            sync (promise)]
        (with-open [c (zoo/connect (str connect-string sandbox))]
          (with-awaited-connection $root
            $z => (eventually-vrefers-to 1000 ["A" 0])
            (add-watch $z :sync (fn [& args] (deliver sync args)))
            (zoo/set-data c "/myzref" (.getBytes "23") 0)
            (deref sync 1000 ::not-delivered) => ::not-delivered
            (zoo/set-data c "/myzref" (.getBytes "\"B\"") 1)
            $z => (eventually-vrefers-to 1000 ["B" 2])))))

(fact "Children do not intefere with their parents"
      (let [$root (znode/new-root)
            $zB (create $root "/myzref/child" "B" :validator string?)
            $zA (create $root "/myzref" "A" :validator string?)
            sync-a (promise)
            sync-b (promise)]
        (with-open [c (zoo/connect (str connect-string sandbox))]
          (with-awaited-connection $root
            $zA => (eventually-vrefers-to 1000 ["A" 0])
            $zB => (eventually-vrefers-to 1000 ["B" 0])
            (add-watch $zA :sync (fn [& args] (deliver sync-a args)))
            (add-watch $zB :sync (fn [& args] (deliver sync-b args)))
            (zoo/set-data c "/myzref" (.getBytes "\"a\"") 0)
            (zoo/set-data c "/myzref/child" (.getBytes "\"b\"") 0)
            @sync-a => (just [:sync (partial instance? zk.zref.ZRef) "A" "a"])
            @sync-b => (just [:sync (partial instance? zk.zref.ZRef) "B" "b"])
            $zA => (eventually-refers-to 1000 "a")
            $zB => (eventually-refers-to 1000 "b")))))

(fact "A ZRef deleted at the cluster throws exceptions on update but otherwise behaves"
      (let [$root (znode/new-root)
            $z (zref $root "/myzref" "A")
            sync (promise)]
        (with-open [c (zoo/connect (str connect-string sandbox))]
          (with-awaited-connection $root
            $z => (eventually-vrefers-to 1000 ["A" 0])
            (zoo/delete c "/myzref")
            (compare-and-set! $z "A" "B") => (throws (as-ex-info (just [string? (contains {::znode/path "/myzref"}) anything])))
            $z => (refers-to "A")))))
