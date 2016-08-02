# drcfg

This [Clojure](http://www.clojure.org) library provides a distributed run-time configuation capability.  The primary abstraction is
the *ZRef*, which is analagous to Clojure's atom but supports a distributed data persistence layer
through the use of [Apache Zookeeper](http://zookeeper.apache.org/).

## Repository Owner
[Chris Hapgood](mailto:chapgood@roomkey.com)

## License

Copyright (C) 2016 [RoomKey](http://www.roomkey.com)

## drcfg notes

### Design Objectives
1. Provide a run-time distributed data element -the ZRef.
2. Provide high resiliency -including reasonable operation when no zookeeper node is available at all (such as airplane mode).
3. To the extent possible, implement the interfaces of Clojure's own atom on a ZRef:
4. Expose the Zookeeper [stat](https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#sc_zkStatStructure) data through metadata on the ZRef.
5. Support updates using both Clojure's own native ref-updating functions (`swap!`, `reset!`, etc) as well as functions that leverage ZooKeeper's innate versioned updates.
5. Support arbitrary metadata through an auxilliary ZRef (stored in a child path `.metadata`)
6. Avoid the weight of additional support libraries such as [Apache Curator](https://curator.apache.org/) while still providing client resiliency and rollover.

### About the drcfg zookeeper node namespace

When using the `def>-` macro, drcfg stores values in zookeeper at the path `/drcfg/<*ns*>/<varname>` where `*ns*` is the namespace of the module that defines the var.

When using the `def>-` macro, drcfg will also creates a `.metadata` zookeeper node under the defined node containing any metadata (hopefully including a doc string as described below).  If no metadata is provided, the node is not created.  Note that normal metadata on the reference object is represented by the ZooKeeper stat data structure.

### drcfg usage

For basic usage, the calling app simply needs to use drcfg's `def>-` like this:

``` Clojure
(ns my.name.space
  (:require [roomkey.drcfg :refer [def>-]]))

(def>- myz "default" :validator? string?)
```

This will create a var whose value is a ZRef associated with the ZooKeeper node `/drcfg/my.name.space/myz`.  Until a connection to the ZooKeeper cluster is established, dereferencing `myz` will yield the default value:

    @myz => "default"

and the metadata on the ZRef (not the var) will be truncated:

	(meta myz) => {:verison -1}

At some later time, the application should open a connection to the ZooKeeper cluster so that ZRefs can be updated with their persisted values:

``` Clojure
(ns my.init
  (:require [roomkey.drcfg :as drcfg]))

(def zclient (atom nil))

(reset! zclient (drcfg/open "zk1.my.co:2181,zk2.my.co:2181,zk3.my.co:2181"))
```

This will create a `ZClient` ZooKeeper client (which is stored in `zclient`) and open a connection to the specified ZooKeeper cluster.  Upon connection, all previously created ZRefs will be linked to their cooresponding ZooKeeper nodes.  The client should be retained for eventual cleanup.  Note that the prefix `"drcfg"` is automatically added to the provided ZooKeeper connection string.

During linking, if not already set, a default value of `"default"` will be set on the node `/drcfg/my.name.space/myz`.  If the node already has a value set (e.g. `"foo"`), it will update the ZRef, causing subsequent dereferencing to yield the updated value:

	@myz => "foo"

Metadata too is updated:

	(meta myz) => {:dataLength 5, :numChildren 1, :pzxid 64, :aversion 0, :ephemeralOwner 0, :cversion 1, :ctime 1412349368172, :czxid 63, :mzxid 4295232539, :version 6, :mtime 1469675813189}

From this point on, a ZooKeeper watch is kept on the associated node and any updates will be reflected in `myz`.

When the application shuts down, you should release resources associated with the previously created ZClient:

	(.close @zclient)

Note that the opening and closing of the ZClient can be neatly managed by state management tools from Clojure's `with-open` through complete systems like [component](https://github.com/stuartsierra/component).

#### Writes
ZRefs can be updated in the same fashion as Clojure's own atom.  Updates are written *synchronously* to the cluster.  See the section below on protocols for enhanced usage with versioning semantics.

### Dependencies

The current version of drcfg has dropped Apache Curator in favor of direct use of the [official java ZooKeeper library](http://zookeeper.apache.org/releases.html#download) and [zookeeper-clj](https://github.com/liebke/zookeeper-clj).  In addition, Clojure's [core.async](https://clojure.github.io/core.async/index.html) is used to manage communication of connectivity events between a ZRef and its ZClient.

### Global State
The `def>-` macro (obviously) adds a var to the current namespace.  If you prefer to create a local binding to a ZRef, you can use the `roomkey.drcfg/>-` function.

In addition to creating the var, the `def>-` macro and `>-` function adds their ZRef value to, `roomkey.drcfg/*registry*`, a set of ZRefs that should be linked to their corrsponding nodes when a connection to the ZooKeeper cluster is established.  This state is required because `def>-` is intended to be used in top-level forms in Clojure code, **and** because the connection to a ZooKeeper cluster is unlikely to be established at that time, it is important to track the ZRefs for eventual connection later in the application's startup.  If you prefer to manage the linking of ZRefs to ZClients yourself, the `roomkey.drcfg/open` command has an arity that allows the developer to supply a collection of ZRefs.

### Monitoring/Admin
http://zookeeper.apache.org/doc/r3.5.1-alpha/zookeeperAdmin.html#sc_zkCommands

``` bash
echo wchp | nc 127.0.0.1 2181 | grep drcfg | sort
```

### Interface/Protocol Support
#### clojure.lang.IDeref
* deref : complete support.  Note that the read interface may lag successful writes.

#### clojure.lang.IMeta
* meta : complete support, the metadata returned is the stat data structure from Zookeeper

#### clojure.lang.IRef
* setValidator : complete support.  Validation is performed on inbound reads and outbound writes.
* getValidator : complete support
* getWatches : complete support
* addWatch : complete support
* removeWatch : partial support.  A watch may trigger one time after being removed.

#### clojure.lang.IAtom
* reset : complete support
* compareAndSet : complete support.  Note that the implementation actually requries a version match as well as a value match.
* swap : complete support.  Note that a swap operation may fail if there is too much contention on the node at the cluster.

In addition to the above standard clojure interfaces, ZRefs support several additional protocols that leverage ZooKeeper's strengths and accommodate its peculiarities:

#### roomkey.zref.UpdateableZNode
* zPair - associate the ZRef with a provided client
* zConnect - start online operations, including synchronization state with the corresponding Zookeeper node
* zDisconnect - suspend online operations
* zProcessUpdate - process an inbound update from the cluster

#### roomkey.zref.VersionedUpdate
* compareVersionAndSet - similar to `compareAndSet` but requiring a version match in the store to effect an update.

####  roomkey.zref.VersionedDeref
* vDeref - Return referenced value and version

#### roomkey.zref.VersionedWatch
* vAddWatch - Add a versioned watcher that will be called with new value and its version

### Avoiding incessant logging from curator and zookeeper

Zookeeper really, really wants your requests to zookeeper to get where
they were going.  If you are a developer without a connection to zookeeper, this logging
will quickly overwhelm any useful info in the output.

These log4j.properties settings can help:

```
# settings to avoid incessant logging of missing connection when 
# zookeeper is unavailable
log4j.logger.org.apache.zookeeper.ClientCnxn=ERROR
log4j.logger.org.apache.zookeeper.ZooKeeper=WARN
# additionally, avoid tons of DEBUG logging from zk
# when rootLogger is set to DEBUG
log4j.logger.org.apache.zookeeper.ClientCnxnSocketNIO=INFO
```

Alternatively, it can also be helpful to change the layout to use
EnhancedPatternLayout and add %throwable{n} to the ConversionPattern
to limit each logged stacktrace to n lines.

### running a local zookeeper

If you want to run a local instance of ZooKeeper, you can set it up as follows:

``` bash
wget http://mirror.symnds.com/software/Apache/zookeeper/zookeeper-3.4.5/zookeeper-3.4.5.tar.gz
tar xvfz zookeeper-3.4.5.tar.gz
cp zookeeper-3.4.5/conf/zoo_sample.cfg zookeeper-3.4.5/conf/zoo.cfg
zookeeper-3.4.5/bin/zkServer.sh start
```

drcfg should take care of any necessary bootstrapping of the root
zookeeper node and will initialize referenced config nodes with their
default values.

Note that the sample config stores data in /tmp/zookeeper, which will
get blown away on a reboot. If you want to retain this data, change
zoo.cfg to point dataDir to a more persistent directory.
