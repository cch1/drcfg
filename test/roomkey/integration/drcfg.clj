(ns roomkey.integration.drcfg
  (:require [roomkey.zkutil :as zk]
            [roomkey.drcfg :refer :all]
            [clojure.test :refer :all]
            [midje.sweet :refer :all]
            [clojure.tools.logging :as log]))


(def drcfg-host "localhost:2181")
(def bogus-host "localhost:2182")
(defn zconn! [] (zk/zkconn! (str drcfg-host zk-prefix)))

;; NOTE: to run these integration tests, you must have access to
;; zookeeper via localhost:2181
(defn zk-available? []
  (loop [i 10 connected false]
    (log/info "Waiting for zookeeper connection.  Remaining retries: " i)
    (if (and (not connected) (> i 0))
      (do
        (Thread/sleep 1000)
        (recur (dec i) (zk/is-connected? (zconn!))))
      connected)))

(defn wait-for-value [atom value timeout]
  (when (and (not= @atom value) (> timeout 0))
    (Thread/sleep 200)
    (wait-for-value atom value (- timeout 200))))

(defn cleanup! []
  (let [path (str "/" *ns*)]
    (when (zk/exists? (zconn!) path)
      (zk/rmr (zconn!) path))))

(facts "can create a config value without a connection"
  (def>- int-local "my-default-value" :validator string?)
  int-local => #(instance? clojure.lang.Atom %)
  @int-local => "my-default-value")

(comment
  ;; this test needs to be run separately from other tests because
  ;; once a connection is attempted, there is no way to override it
  (facts "can continue if server not available"
    (drcfg/def>- int-noserver "my-default-value" :validator string?)
    (drcfg/connect! bogus-host)
    (Thread/sleep 1000)
    @int-noserver => "my-default-value")
) ; end comment block


(if (not (zk-available?))
  (log/infof "zookeeper unavailable - skipping " *ns*)
  (against-background [(before :facts (cleanup!)) (after :contents (cleanup!))]
    (facts "def after connect still sets local atom"
      (connect! drcfg-host)
      (def>- intlatedef "my-default-value" :validator string?) => anything
      @intlatedef => "my-default-value"
      ;; this 'provided' check worked at one point to verify that an
      ;; error was being logged, but it has become unreliable for
      ;; some reason -- commented out for now
      ;; (provided (log/log* anything :error anything anything)
      ;; => anything)
      )

    (facts "slaved config value gets updated using async connect"
      (def>- int-string "my-default-value" :validator string?
        :ignore-prior-connection true)
      (connect! drcfg-host)
      (Thread/sleep 1000)       ; give connect time to trigger linking
      ;; default applied
      (zk/nget (zconn!) (ns-path "int-string")) => "my-default-value"
      @int-string => "my-default-value" ; confirm atom unchanged
      (drset! (ns-path "int-string") "new-value")
      (zk/nget (zconn!) (ns-path "int-string")) => "new-value" ; zk updated
      (wait-for-value int-string "new-value" 3000)
      @int-string => "new-value"        ; confirm slaved update
      ;; now make sure watches keep working after the first update
      (drset! (ns-path "int-string") "second-value") ; second update
      (zk/nget (zconn!) (ns-path "int-string")) => "second-value" ; zk updated
      (wait-for-value int-string "second-value" 3000)
      @int-string => "second-value")    ; confirm second slaved update

    (facts "slaved config for int value gets updated"
      (def>- int-int 1 :validator integer? :ignore-prior-connection true
        :meta {:doc ..doc-string..})
      (connect-with-wait! drcfg-host)
      (zk/nget (zconn!) (ns-path "int-int")) => 1 ; default applied
      @int-int => 1                     ; confirm atom unchanged
      (drset! (ns-path "int-int") 2)
      (zk/nget (zconn!) (ns-path "int-int")) => 2 ; zk updated
      (wait-for-value int-int 2 3000)
      @int-int => 2)                    ; confirm slaved update

    (facts "metadata is applied"
      (def>- int-meta 1 :validator integer? :ignore-prior-connection true
        :meta {:doc "my doc string"})
      (connect-with-wait! drcfg-host)
      (zk/nget (zconn!) (ns-path "int-meta")) => 1 ; default applied
      (zk/get-metadata (zconn!)
                       (ns-path "int-meta"))
      => {:doc "my doc string"})

    (facts "prevent updating value for int with non-int with validator"
      (def>- int-int2 1 :validator integer? :ignore-prior-connection true)
      (connect-with-wait! drcfg-host)
      (zk/nget (zconn!) (ns-path "int-int2")) => 1 ; default applied
      @int-int2 => 1                    ; confirm atom unchanged
      (drset! (ns-path "int-int2") "test")
      (zk/nget (zconn!) (ns-path "int-int2")) => "test" ; confirm that zk was updated
      (wait-for-value int-int2 2 3000)
      @int-int2 => 1)                   ; confirm no slaved update

    (facts "allow updating value for int with non-int without validator"
      (def>- int-int3 1 :ignore-prior-connection true)
      (connect-with-wait! drcfg-host)
      (zk/nget (zconn!) (ns-path "int-int3")) => 1 ; default applied
      @int-int3 => 1                    ; confirm atom unchanged
      (drset! (ns-path "int-int3") "test")
      (zk/nget (zconn!)
               (ns-path "int-int3")) => "test" ; zk updated
      (wait-for-value int-int3 "test" 3000)
      @int-int3 => "test")              ; confirm slaved update

    (facts "slaved config for vector value gets updated"
      (def>- int-vec [1 2 "three"] :validator vector?
        :ignore-prior-connection true)
      (connect-with-wait! drcfg-host)
      (zk/nget (zconn!) (ns-path "int-vec")) => [1 2 "three"] ; default applied
      @int-vec => [1 2 "three"]         ; confirm atom unchanged
      (drset! (ns-path "int-vec") [1 3 "five"])
      (zk/nget (zconn!) (ns-path "int-vec")) => [1 3 "five"] ; zk updated
      (wait-for-value int-vec [1 3 "five"] 3000)
      @int-vec => [1 3 "five"])         ; confirm slaved update

    (facts "slaved config for java object gets updated"
      (def>- int-obj (new java.util.Date 1) :ignore-prior-connection true)
      (connect-with-wait! drcfg-host)
      (zk/nget (zconn!) (ns-path "int-obj"))
      => (new java.util.Date 1) ; confirm default
      @int-obj => (new java.util.Date 1) ; atom is unchanged when creating new
      (drset! (ns-path "int-obj") (new java.util.Date 1))
      (zk/nget (zconn!) (ns-path "int-obj"))
      => (new java.util.Date 1) ; zk updated
      (wait-for-value int-obj (new java.util.Date 1) 3000)
      @int-obj => (new java.util.Date 1)) ; confirm slaved update

    (facts "pre-configured value gets applied"
      (zk/create (zconn!) (ns-path "int-preconfig") "zk-value")
      (def>- int-preconfig "my-default-value" :validator string?
        :ignore-prior-connection true)
      (connect! drcfg-host)
      ;; value still isn't changed immediately with async connect
      @int-preconfig => "my-default-value"
      (wait-for-value int-preconfig "zk-value" 3000)
      @int-preconfig => "zk-value")   ; value is updated after linkage

    (facts ">- syntax works"
      (let [mystring (>- (ns-path "int-string3") nil
                         :ignore-prior-connection true)]
        @mystring => nil))

    (facts ">- syntax works with connect and update"
      (let [mystring (>- (ns-path "int-string4") nil :ignore-prior-connection
                         true)]
        @mystring => nil
        (connect-with-wait! drcfg-host)
        (drset! (ns-path "int-string4") "new-value")
        (wait-for-value mystring "new-value" 3000)
        @mystring => "new-value"))))
