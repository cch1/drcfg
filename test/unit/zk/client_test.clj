(ns unit.zk.client-test
  (:require [zk.client :refer :all]
            [cognitect.anomalies :as anomalies]
            [clojure.test :refer [deftest is]]))

(deftest create-client
  (is (instance? zk.client.ZClient (create))))

(deftest generate-kex-info
  (let [e (java.lang.AssertionError. "Why")
        [ex retry?] (kex-info -103 "WTF" {:foo :bar} e)]
    (is (not retry?))
    (is (instance? clojure.lang.ExceptionInfo ex))
    (is (= {:foo :bar
            ::anomalies/category ::anomalies/conflict :zk.client/kex-code :BADVERSION}
           (ex-data ex)))
    (is (= "WTF" (ex-message ex)))
    (is (= e (ex-cause ex)))))

(deftest can-translate-exception)
