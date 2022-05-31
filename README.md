# drcfg

This [Clojure](http://www.clojure.org) library provides a distributed run-time configuation capability.  The primary abstractions are
the *ZRef* and *ZNode*.  ZRefs are analagous to Clojure's atom but support a distributed data persistence layer through the use
of [Apache Zookeeper](http://zookeeper.apache.org/).  ZNodes are a lighter-weight abstraction of a ZooKeeper node suitable for proxying
an entire ZooKeeper (sub-)tree.

## Repository Owner
[Chris Hapgood](mailto:cch1@hapgood.com)

## License

See LICENSE for full license text.

## drcfg notes

### Design Objectives
1. Provide a run-time distributed data element (the ZNode) and a further abstraction mirroring Clojure's Atom (the ZRef).
2. Provide high resiliency -including reasonable operation when no zookeeper node is available at all (such as airplane mode).
3. To the extent possible, implement the interfaces of Clojure's own atom on a ZRef: watches, validators, etc.
4. Support evaluation of ZRefs as namespaced vars implicitly belonging to a single virtual tree.  This requires automatic creation of intervening nodes representing the namespace hierarchy of a ZRef.
5. The tree construction phase can occur while offline (at read time) and an eventual reconciliation/sync phase is required immediately upon connection.  Reconciliation must respect the authority of the cluster for _values_ but merge the local-only and remote-only ZNodes into a single coherent system.
6. Expose the Zookeeper [stat](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_zkStatStructure) data through metadata
on the ZRef and ZNode.
7. Support updates using both Clojure's own native ref-updating functions (`swap!`, `reset!`, etc) as well as functions that leverage ZooKeeper's innate versioned updates.
8. Support standard Clojure metadata on persisted values.
9. Support arbitrary var-matching metadata through an auxilliary ZNode (stored in a child path `.metadata`).
10. Avoid the weight of additional support libraries such as [Apache Curator](https://curator.apache.org/) or [zookeeper-clj](https://github.com/liebke/zookeeper-clj) while still providing rich functionality with client resiliency and rollover.

### About the drcfg zookeeper node namespace

When using the `def>` macro, drcfg stores values in ZooKeeper at the path `/drcfg/<ClojureNamespaceParent>/.../<ClojureNamespaceChild>/<ClojureVarName>` where the namespace components are derived from the dot-separated namespace hierarchy of the *ns* value when invoking the macro.

When using the `def>` macro drcfg will also create a `.metadata` zookeeper node under the defined node containing any var metadata (hopefully including a doc string as described below).  If no metadata is provided, the node is not created.  Note that normal metadata on the reference object contains the ZooKeeper Stat data structure.

### drcfg usage

For basic usage, the calling app simply needs to use drcfg's `def>` like this:

``` Clojure
(ns my.name.space
  (:require [zk.drcfg :refer [def>]]))

(def> ^{:doc "My documenation about this node"} myz "default" :validator? string?)
```

This will create a var whose value is a ZRef associated with the ZooKeeper node `/drcfg/my/name/space/myz`.  Nodes representing ancestor namespace are created automatically.  Until a connection to the ZooKeeper cluster is established, dereferencing `myz` will yield the default value:

    @myz => "default"

and the metadata on the ZRef (not the var) will be a truncated/default represtantion of the ZooKeeper Stat data structure:

	(meta myz) => {:verison -1 :cversion -1 :aversion -1}

Each ZRef is backed by a ZNode which proxies the persisted ZooKeeper znode.  All cluster connectivity is managed by ZNodes.

The root ZNode (`zk.drcfg/*root*`) is the starting point to which all child nodes are attached.

At run time (after all drcfg vars have been evaluated and interned), the application should open a connection to the ZooKeeper cluster
via the root ZNode using the `zk.drcfg/open` function.

``` Clojure
(ns my.init
  (:require [zk.drcfg :as drcfg]))

(def handle (drcfg/open "zk1.my.co:2181,zk2.my.co:2181,zk3.my.co:2181")
```

This will open a connection to the specified ZooKeeper cluster.   The returned handle (an instance of `java.lang.AutoCloseable`) should be retained for eventual release of any connection resources.  Note that the prefix `"drcfg"` is automatically added to the provided ZooKeeper connection string to effectively scope the drcfg nodes.

Upon connection, all previously created ZNodes will be linked to their cooresponding ZooKeeper nodes and any ZRefs will transitively be connected.   Immediately upon connecting, ZNodes read their current value from the cluster.  If the ZNode does exist, the value stored at the cluster will be read and subsequently update the ZRef, causing dereferencing to yield the updated value:

	@myz => "foo"

Metadata too is updated:

	(meta myz) => {:dataLength 5, :numChildren 1, :pzxid 64, :aversion 0, :ephemeralOwner 0, :cversion 1, :ctime 1412349368172, :czxid 63, :mzxid 4295232539, :version 6, :mtime 1469675813189}

When a root ZNode initially establishes a connection to the ZooKeeper cluster, it will merge the local and remote subtrees.  Any local-only nodes will be persisted to the tree using their default value.  Any remote-only nodes are proxied by a new local ZNode, but no corresponding ZRef is created.

From this point on, a ZooKeeper watch is kept on the associated node and any updates to its data or children will be reflected in `myz` and its child ZNodes.

When the application shuts down, you should release resources associated with the handle:

	(.close handle)

Note that the opening and closing of the ZClient can be neatly managed by state management tools like Clojure's `with-open` or through complete systems like [component](https://github.com/stuartsierra/component) or [integrant](https://github.com/weavejester/integrant).

#### Writes
ZRefs can be updated in the same fashion as Clojure's own atom.  Updates are written *synchronously* to the cluster.  See the section below on protocols for enhanced usage with versioning semantics.  **Keep in mind that reads will not reflect the current value until the watch on the ZooKeeper node has been fired and the client has received the update.**  This typically happens in milliseconds but is obviously dependent on your specific implementation of the ZooKeeper cluster.

#### Database Initialization
A drcfg tree of znodes must be initialized by creating the root ZNode.  The `drcfg/db-initialize!` function will perform this operation.

### Dependencies
The current version of drcfg depends directly on the [official java ZooKeeper library](http://zookeeper.apache.org/releases.html#download).  In addition, Clojure's [core.async](https://clojure.github.io/core.async/index.html) is used to manage communication of connectivity events between ZRefs and their paired ZNodes as well as between ZNodes and the ZClient.

### Global State
The `def>` and `def>-` macros add a var to the current namespace.  If you prefer to create a local binding to a ZRef, you can use the `zk.drcfg/>-` function.

In addition to creating the var, the `def>` macro as well as the `>-` function add one (or, if metadata is specified, two) ZNode to the global tree of ZNodes (rooted at `drcfg/*root*`).  This state is required because `def>` are intended to be used in top-level forms in Clojure code, **and** because the connection to a ZooKeeper cluster is unlikely to be established at that time, it is important to track the ZNodes for eventual connection later in the application's startup.  If you prefer to manage a (non-global) hierarchy of ZNodes, the `zk.node` namespace provides all the necessary functionality.

### Monitoring/Admin
http://zookeeper.apache.org/doc/r3.5.1-alpha/zookeeperAdmin.html#sc_zkCommands

``` bash
echo wchp | nc 127.0.0.1 2181 | grep drcfg | sort
```

### Interface/Protocol Support for ZRef
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
* reset : complete support.
* compareAndSet : complete support.
* swap : complete support.  Note that a swap operation may fail if there is too much contention on the node at the cluster.

In addition to the above standard clojure interfaces, ZRefs support several additional protocols that leverage ZooKeeper's strengths and accommodate its peculiarities:

#### zk.zref.ZNodeWatching
* start  - Start online operations
* update! - Update the cluster znode backing this zref
* path - Return the path of the backing ZNode in the tree

#### zk.zref.VersionedReference
* compareVersionAndSet - similar to `compareAndSet` but requiring a version match against the persisted state to effect an update.
* vDeref - Return referenced value and version.
* vAddWatch - Add a versioned watcher that will be called with new value and its version.

### Interface/Protocol Support for ZNode
#### clojure.lang.Named
* getName - Returns the final segment in the path of the ZNode.
* getNamespace - Returns the path of the ZNode without the final segment.

#### java.lang.Comparable
compareTo - compares the ZNode to the given ZNode based on the alphabetical sorting of their paths.

#### clojure.lang.IFn
invoke - ZNodes are functions of the paths of their descendants.
applyTo - further support for ZNodes as functions of the paths of their descendants.

#### clojure.lang.IDeref
* deref : Returns the persisted value of the node.  Note that reads may lag successful writes.

#### clojure.lang.IMeta
* meta : Returns the stat data structure from ZooKeeper

#### clojure.lang.Seqable
* seq - Returns a sequence of the children of the ZNode.

#### clojure.lang.Counted
* count - Returns the number of children of the ZNode.

#### clojure.core.async.impl.protocols.ReadPort
* take! - Allows a ZNode to act as a read-only async channel yielding events.

#### clojure.core.async.impl.protocols.Channel
* close! - Allows a ZNode to cease watching the cluster-persisted ZNode
* closed? - Further support allowing a ZNode to cease watching the cluster-persisted ZNode.

#### java.lang.Object
* equals - Further support for equality based on path and client alone.
* hashCode - Further support for equality based on path and client alone.
* toString - Friendly rendering of a ZNode to a string.

In addition to the above standard clojure interfaces, ZNodes also support several additional protocols that leverage ZooKeeper's strengths and accommodate its peculiarities:

#### zk.node.TreeNode
* path - Return the string path of the ZNode
* create-child - Create a child _without_ connecting him to his parent.
* update-or-add-child - Update the existing child or create a new child of the ZNode at the given path.
* signature - Return a (Clojure hash equivalent) signature of the state of the subtree at this ZNode.

#### zk.node.BackedZNode
* create! - Create the ZNode backing this virtual node
* delete! - Delete the ZNode, asserting the current version
* compare-version-and-set! - Atomically set the value of the ZNode if and only if the current version is supplied.
* watch - Recursively watch the ZNode and its children.  During the boot phase of a ZNode, children are persisted to the cluster if they are found to be missing.  Thereafter, children _removed_ at the cluster are also removed locally.

### Avoiding incessant logging from curator and zookeeper

Zookeeper really, really wants your requests to zookeeper to get where
they were going.  If you are a developer without a connection to zookeeper, this logging
will quickly overwhelm any useful info in the output.  Current versions of ZooKeeper use 
logback for logging.  Here is a sample logback configuration that will help restore peace:

``` xml
<configuration>
  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <!-- encoders are assigned the type
         ch.qos.logback.classic.encoder.PatternLayoutEncoder by default -->
    <encoder>
      <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
    </encoder>
  </appender>

  <logger name="org.apache.zookeeper" level="WARN"/>
  <logger name="org.apache.zookeeper.ClientCnxn" level="ERROR"/>
  <logger name="org.apache.zookeeper.server" level="WARN"/>
  <logger name="org.apache.zookeeper.server.ServerCnxnFactory" level="ERROR"/>
  <logger name="org.apache.zookeeper.server.NIOServerCnxn" level="ERROR"/>
  <logger name="org.apache.curator" level="WARN"/>

  <root level="INFO">
    <appender-ref ref="STDOUT" />
  </root>
</configuration>

```

### History
The drcfg library started as an internal project at Roomkey in early 2013.  It was broken out into a standalone library in mid-2013 and released as open-source after Roomkey ceased operating in 2020.  My colleagues at Roomkey were instrumental in producing drcfg.  My management was also supportive of the concept, its implementation and evolution.

### Contributors
These are the people that have contributed to drcfg.  Without them drcfg would have more bugs, fewer features and scant documentation.

* Chris Hapgood
* Laverne Schrock
* Adam Frey
* David Sison
