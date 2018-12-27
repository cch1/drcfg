* Bind zrefs to a drcfg client (not just a ZooKeeper client) at creation.
* Use bound zclient as sole coordination point between clients and zrefs.
* Validate inbound updates from server.
* zclient implements IDeref and dereferencing zclient yields current ZooKeeper client.
* zclient implements Mult and can be tapped for client events.
* Complete overhaul of integration testing for more deterministic results.
* Refactor ZRef cache for efficiency.
* Add ZNode metadata to return of vDeref method.
