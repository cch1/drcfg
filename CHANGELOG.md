## Version 6.4.4
* Handle :Closed keeper-state in ZClient

## Version 6.4.3
* Prevent deadlock during shutdown of a ZNode tree. The Closable
  returned by ZNode/watch now returns a channel upon invokation,
  instead of blocking.

## Version 6.4.2
* Fix `roomkey.drcfg/def>` to properly support using a map as the default value

## Version 6.4.1
* Bump dependencies, namely:
  org.clojure/clojure 1.10.0 -> 1.10.1
  org.clojure/core.async 0.4.490 -> 0.4.500
  org.clojure/tools.logging 0.4.1 -> 0.5.0
  org.apache.zookeeper/zookeeper 3.5.4-beta -> 3.5.5

## Version 6.4.0
* Return stat data structures from `znode/create!` and `znode/compare-version-and-set!`.  This requires newish versions of Zookeeper.

## Version 6.3.1
* Add missing dependency on `org.clojure/tools.macro`

## Version 6.3.0
* Fix bad behavior that allowed sync'd ZNodes to be re-initialized with default values

## Version 6.2.1
* Add type hints and decrease reflection.
* Tweak logging.

## Version 6.2.0
* Add clojure.lang.IFn support on ZClient.
* Overhaul ZNode startup and boot process.
* Refactor for performance, simplicity.

## Version 6.1.0
* Overhaul open and close to squash out race conditions
* Tweak logging

## Version 6.0.0
### Big changes to ZRefs and ZClient, but only small changes to API of drcfg
* Deprecate `zref/zref` in favor of `zref/create`
* Promote Stat metadata to `java.time.Instant` from millis.
* Introduce light-weight ZNode for proxying hierarchy of ZooKeeper nodes, and refactor ZRefs to leverage ZNode
* Bind zrefs to a znode (and indirectly to a drcfg client -not just a ZooKeeper client).
* Use znode attribute as sole coordination point between clients and zrefs.
* Validate inbound updates from server.
* zclient implements IDeref and dereferencing zclient yields current ZooKeeper client.
* zclient implements Mult and can be tapped for client events.
* Complete overhaul of integration testing for more deterministic results.
* Refactor ZRef cache for efficiency.
* Add ZNode metadata to return of vDeref method.
* Add `drcfg/def>` with "standard" Clojure metadata support and breaking down period-separted Clojure namespaces into ZooKeeper paths.
