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

(deftest def>-can-be-evaluted
  (is (instance? clojure.lang.Var (def> ^:foo y1 "My Docstring" 2222))))

(deftest def>-installs-the-optional-validate-on-the-zref
  (is (fn? (get-validator (var-get (def> ^:foo y2 "My Docstring" 2222 :validator integer?))))))
