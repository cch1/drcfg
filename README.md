# roomkey

This library provides a distributed run-time configuation capability.  The primary abstraction is
the *ZRef*, which is analagous to Clojure's atom but supports a distributed data persistence layer
through the use of Apache Zookeeper.

## Repository Owner
Chris Hapgood
chapgood@roomkey.com

## License

Copyright (C) 2016 RoomKey

## drcfg notes

### Design Objectives
1. Provide a run-time distributed data element -the ZRef.
2. To the extent possible, implement the interfaces of Clojure's own atom on a ZRef.
   
   clojure.lang.IDeref
	   * deref : complete support.  Note that the read interface may lag successful writes.
   clojure.lang.IMeta
	   * meta : complete support, the metadata returned is the stat data structure from Zookeeper
   clojure.lang.IRef
	   * setValidator : complete support.  Validation is performed on inbound reads and outbound writes.
	   * getValidator : complete support
	   * getWatches : complete support
	   * addWatch : complete support
	   * removeWatch : partial support.  A watch may trigger one time after being removed.
   clojure.lang.IAtom
	   * reset : full support
	   * compareAndSet : full support.  Note that the implementation actually requries a version match
	   as well as a value match.
	   * swap : partial support.  A swap operation may fail if there is too much contention on the node
	   at the cluster.
   roomkey.zref.Versionedupdate
	   * compareVersionAndSet - similar to `compareAndSet` but requiring a version match in the store
	   to effect an update.
   roomkey.zref.UpdateableZNode
	   * zConnect - start synchronization of the local ZRef with the corresponding Zookeeper node
	   * zDisconnect - stop synchronization.
	   * zProcessUpdate - process an inbound update from the cluster
	   
   3. Expose the Zookeeper version through metadata on the ZRef.
   4. Support classic arbitrary metadata through an auxilliary ZRef (stored a child path `.metadata`)

### Monitoring/Admin
http://zookeeper.apache.org/doc/r3.5.1-alpha/zookeeperAdmin.html#sc_zkCommands

### Support libraries

The current version of drcfg has dropped curator in favor of [https://github.com/liebke/zookeeper-clj](zookeeper-clj)

The old drcfg used avout to manage the backing zookeeper store. The
new drcfg implementation stores values in zookeeper directly (with
some help from the Netflix Curator library)

A few differences in the new drcfg implementation:

* in the old drcfg, it was necessary to create the root /drcfg code
  via direct zookeeper calls if it did not already exist. In new
  drcfg, any necessary directories are automatically created in the
  node structure.

* in the old drcfg, the only way to set values in zookeeper was to use
  update!. In new drcfg, if no node exists in zookeeper, then the
  structure will be created and the default value from the def>- call
  will be stored there.

* update! has been renamed to drset! since update implies different
  behavior in clojure

* in the old drcfg, if def>- came after connect it would continue
  silently without warning that it was not properly linked. In drcfg,
  an exception is thrown if def>- is called after connect. An option
  is available to override this exception, but that is expected only
  to be used for drcfg integration tests.

### drcfg zookeeper namespace

drcfg stores values in zookeeper at the path /drcfg/*ns*/varname where
*ns* is the namespace of the module that defines the var. Values are
serialized by clojure (print-dup) with the resulting string converted
to a byte array for storage in zookeeper. The serialize/deserialize
functions are defined in roomkey.zkutil.

drcfg also creates a .metadata zookeeper node under the defined node
containing any metadata (hopefully, including a doc string as
described below).

### drcfg usage

For basic usage, the calling app simply needs to use drcfg's `def>-`
macro to def the ZRef that will hold the config value and provide a
default. The atom name is automatically prepended with the `*ns*` value
when used in this way.

If a corresponding node is found in zookeeper, the application's atom
will reflect the value from this node. If no existing node exists,
then a new node is created for the variable and the default value is
stored there..

A watch is applied to the node so that if the zookeeper value is
updated while the application that referenced it is still running, the
application's ZRef will be automatically updated to reflect the new
value.

ZRefs can be updated in the same fashion as Clojure's own atom.  Updates
are written *synchronously* to the cluster.

### drcfg sample code

```
(ns (your project)
  (:require [roomkey.drcfg :as drcfg]))

;; basic usage (with optional validation)
(drcfg/def>- yourvariable "default-value" :validator string?
	:meta {:doc "Description of your variable here.  Should be descriptive
enough to allow an ops user to know what your variable does when they
see it in adminsuite."})

;; immediately after the def>-, your variable has the default value
@yourvariable
;; returns=> "default-value"

;; hosts is a comma separated list of zookeeper hosts including port 
;; or 'localhost:2181' for local dev.  The calling application 
;; will typically get it from the ZK_HOSTS environment variable
(defn- zk-connect! []
	(when-let [hosts (or (System/getProperty "ZK_HOSTS")
                       (get (System/getenv) "ZK_HOSTS")
                       (System/getProperty "PARAM2")
                       (get {:development "localhost:2181"} (stage/stage)))]
    (drcfg/connect! hosts)))

;; dereferencing your variable provides the latest value.  Note that 
;; while def>- and connect! return immediately, it can take a second
;; or so for the linked variables to have the zookeeper value
@yourvariable
;; returns=> "whatever-value-was-in-zookeeper" or "default-value" if
;; this is a new node

;; if you need to update the value stored in zookeeper, use conventional
;; clojure commands for updating an atom (swap!, reset!, compare-and-set!)
;; after connecting.

;; within a second or two after an update, all atoms referencing that
;; value, across the distributed environment should be updated
@yourvariable
;; returns=> "new-value"

```
### avoiding incessant logging from curator and zookeeper

Zookeeper really, really wants your requests to zookeeper to get where
they were going. Curator layers that with still more goodness. Any
time zookeeper is down, they want you to know, so they log lots of
exceptions and retry again and again to reestablish the connections.
If you are a developer without a connection to zookeeper, this logging
will quickly overwhelm any useful info in the output.

These log4j.properties settings can help:

```
# settings to avoid incessant logging of missing connection when 
# zookeeper is unavailable
log4j.logger.org.apache.zookeeper.ClientCnxn=ERROR
log4j.logger.org.apache.zookeeper.ZooKeeper=WARN
log4j.logger.com.netflix.curator.ConnectionState=FATAL
# additionally, avoid tons of DEBUG logging from zk and curator 
# when rootLogger is set to DEBUG
log4j.logger.org.apache.zookeeper.ClientCnxnSocketNIO=INFO
log4j.logger.com.netflix.curator.framework.imps.CuratorFrameworkImpl=INFO
log4j.logger.com.netflix.curator.RetryLoop=INFO
```

Alternatively, it can also be helpful to change the layout to use
EnhancedPatternLayout and add %throwable{n} to the ConversionPattern
to limit each logged stacktrace to n lines.

### running a local zookeeper

If you want to run a local instance of ZooKeeper, you can set it up as follows:

```bash
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
