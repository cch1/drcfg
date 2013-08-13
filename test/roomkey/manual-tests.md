It is difficult to fully automate testing of zookeeper scenarios where the
server stops and starts in the midst of running.  Instead, I have documented
these test scenarios as a set of steps run in a REPL, with comments describing
points at which the zookeeper server should be stopped or started

```
;; test with zk server unavailable at startup, but later becomes available
;; *** stop zk server
(use '[roomkey.zkutil :as zk] :reload-all)
(use '[roomkey.drcfg :as drcfg] :reload-all)
(drcfg/def>- int-string "my-default-value" :validator string? :ignore-prior-connection true)
@int-string
(drcfg/connect! "localhost:2181")
;; wait a while
@int-string ; should still be my-default-value
;; *** start zk server
@int-string ; should still be my-default-value
(drcfg/drset! (drcfg/ns-path "int-string") "new-value")
@int-string ; should be new-value --> the watch is working
```

```
;; test with zk server available at startup, becomes available, becomes unavailable again
;; *** start zk server
(use '[roomkey.zkutil :as zk] :reload-all)
(use '[roomkey.drcfg :as drcfg] :reload-all)
(drcfg/def>- int-string "my-default-value" :validator string? :ignore-prior-connection true)
@int-string
(drcfg/connect! "localhost:2181")
@int-string ; should still be my-default-value
;; *** stop zk server
;; wait a while
@int-string ; should still be my-default-value
;; *** start zk server
(drcfg/drset! (drcfg/ns-path "int-string") "new-value")
@int-string ; should be new-value --> the watch is still working after stop/start
```