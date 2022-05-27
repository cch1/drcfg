(ns unit.zk.zref-test
  (:require [zk.zref :refer :all :as zref]
            [zk.node :as znode]
            [zk.client :as zclient]
            [clojure.core.async :as async]
            [clojure.test :refer [deftest testing is]]))

(deftest create-a-zref
  (is (instance? zk.zref.ZRef (create (znode/new-root) "/myzref" "A"))))

(deftest validator-can-be-added
  (let [$z (create (znode/new-root) "/zref0" 1)]
    (is (nil? (set-validator! $z odd?)))
    (is (= odd? (get-validator $z)))))

(deftest new-validators-must-validate-current-value
  (let [$z (create (znode/new-root) "/zref0" 1)]
    (is (thrown? IllegalStateException (set-validator! $z even?)))))

(deftest default-values-must-validate
  (is (thrown? ClassCastException (create (znode/new-root) "/zref0" "A" :validator pos?)))
  (is (thrown? IllegalStateException (create (znode/new-root) "/zref1" 1 :validator even?))))

(deftest zrefs-can-be-watched
  (let [$z (create (znode/new-root) "/myzref" "A")]
    (is (=  $z (add-watch $z :key (constantly true))))
    (is (= $z (remove-watch $z :key)))))

(deftest can-deref-fresh-zref-for-default-value
  (is (= "A" @(create (znode/new-root) "/myzref" "A"))))

(deftest can-get-metadata-of-fresh-zref
  (let [$z (create (znode/new-root) "/myzref" "A")]
    (is (= -1 (:version (meta $z))))))
