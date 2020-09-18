(ns roomkey.unit.drcfg
  (:require [roomkey.drcfg :refer :all]
            [roomkey.znode :as znode]
            [roomkey.zref :as zref]
            [roomkey.zclient :as zclient]
            [clojure.string :as string]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]))

(defchecker refers-to [expected]
  (checker [actual] (extended-= (deref actual) expected)))

(background (around :facts (binding [] ?form)))

(facts ">- returns a ZRefs"
       (>- "/ns/x" 1) => (refers-to 1)
       (provided
        (zref/create *root* "/ns/x" 1) => (atom 1)))

(fact "open starts zrefs with client specs"
      (open "cspecs") => (partial instance? java.io.Closeable)
      (provided
       (zclient/open (as-checker (partial instance? roomkey.zclient.ZClient)) "cspecs/drcfg" (as-checker integer?)) => ..client..))

(fact "def>- expands correctly"
      (macroexpand-1 `(def>- y [12] :validator identity :meta {:doc "My Documentation"}))
      => (just `(let ~(just [symbol? (just `(ns-path #".+/y"))])
                  ~(just `(when ~truthy
                            ~(just `(>- ~(just `(str ~symbol? "/.metadata")) ~(just {:doc "My Documentation"})))))
                  ~(just `(def y ~(just `(apply >- ~symbol? [12] (:validator identity))))))))

(fact "def>- can be evaluated"
      (def>- y [12] :validator identity :meta {:doc "My Documentation"}) => (partial instance? clojure.lang.Var))

(fact "def> expands correctly"
      (macroexpand-1 `(def> y {} :validator identity))
      => (just `(let ~(just [symbol? (just `(str "/" ~(just `(string/replace ~(just `(str *ns*)) #"\." "/")) "/" 'y))
                             symbol? (just `(>- ~symbol? {} :validator identity))])
                  ~(just `(when ~truthy
                            ~(just `(znode/add-descendant ~(just `(.znode ~symbol?)) "/.metadata" ~(just {})))))
                  ~(just `(def y ~symbol?))))
      (macroexpand-1 `(def> ^:foo y "My Documentation" [12] :validator identity))
      => (just `(let ~(just [symbol? (just `(str "/" ~(just `(string/replace ~(just `(str *ns*)) #"\." "/")) "/" 'y))
                             symbol? (just `(>- ~symbol? [12] :validator identity))])
                  ~(just `(when ~truthy
                            ~(just `(znode/add-descendant ~(just `(.znode ~symbol?)) "/.metadata" ~(just {:doc "My Documentation" :foo true})))))
                  ~(just `(def y ~symbol?)))))

(fact "def> can be evaluated"
      (def> ^:foo y "My Docstring" 2222) => (partial instance? clojure.lang.Var))

(fact "def> installs the optional validator on the zref"
      (get-validator (var-get (def> ^:foo y "My Docstring" 2222 :validator integer?))) => fn?)
