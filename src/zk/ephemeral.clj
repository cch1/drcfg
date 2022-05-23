(ns zk.ephemeral
  "Support for clojure.core.async channels whose content can be asynchronously purged."
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as impl]))

(deftype EphemeralBuffer [marker ^:unsynchronized-mutable val]
  impl/UnblockingBuffer
  impl/Buffer
  (full? [_] false)
  (remove! [_] val)
  (add!* [this itm] (set! val itm) this)
  (close-buf! [_] (set! val nil))
  clojure.lang.Counted
  (count [_] (if (identical? marker val) 0 1)))

(defn- buffer
  "Return a new empty EphemeralBuffer whose contents are effectively cleared
  upon receipt of `marker`."
  [marker]
  (EphemeralBuffer. marker marker))

(defn channel
  "Creates an ephemeral channel with an optional transducer `xform` and an
  optional exception-handler `ex-handler`.  Like a channel based on a sliding
  buffer of size one, puts will complete but only the newest item will be
  retained.  Furthermore, putting the `marker` value onto the channel causes
  the currently buffered item (if any) to be purged and the channel reverts
  to being empty.

  Consumers will block until either a value is placed in the channel or the
  channel is closed.  Consumers will receive the most recently put value until
  the channel is purged or closed.  Note that unlike a fulfilled promise channel
  an ephemeral channel will return `nil` when closed.

  See `chan` for the semantics of `xform` and `ex-handler`."
  ([marker] (channel marker nil))
  ([marker xform] (channel marker xform nil))
  ([marker xform ex-handler]
   (async/chan (buffer marker) xform ex-handler)))
