(ns unit.zk.drcfg-test
  (:require [zk.drcfg :refer :all]
            [zk.node :as znode]
            [zk.zref :as zref]
            [zk.client :as zclient]
            [clojure.string :as string]
            [clojure.test :refer [deftest testing is]]))

(deftest >--returns-zref
  (with-redefs [zref/create (constantly (atom 1))]
    (is (= 1 @(>- "/ns/x" 1)))))

#_(deftest open-starts-zrefs-with-client-specs
    (with-redefs [zclient/open (constantly true)]
      (is (instance? java.io.Closeable (open "cspecs")))))

;;; The cognitect test runner runs loaded tests with `user` as the current namespace so any `def`s inside a deftest.  Running tests directly from a REPL
;;; respects *ns*.  The best way to have a test work in both situations is to *not* def test vars from within the deftest form.
(def> ^:foo y1 "My Docstring" 2222)

(deftest def>-can-be-evaluted
  (let [v #'y1]
    (is (instance? clojure.lang.Var v))
    (is (= (str (znode/->path (symbol v))) (zref/path (var-get v))))))

(deftest def>-installs-the-optional-validate-on-the-zref
  (is (fn? (get-validator (var-get (def> ^:foo y2 "My Docstring" 2222 :validator integer?))))))
