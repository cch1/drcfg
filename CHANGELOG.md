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
