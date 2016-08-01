(ns user
  (:import [org.apache.curator.test TestingServer TestingCluster KillSession])
  (:require [roomkey.drcfg :refer :all :as drcfg]
            [roomkey.zclient :as zclient]
            [roomkey.zref :as zref]
            [zookeeper :as zoo]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
            [clojure.string :as string]))

(def $connect-string "localhost:2181")
(def $path "/apij.controllers.interstitial.rk-int-alg/template-filters")

;; Setup tunnels to dev zookeeper cluster first or this will throw an exception
(defn ex0 []
  (let [connect-string (str $connect-string "/drcfg")
        z (zref/zref $path {})
        r (zref/zref "/" nil)]
    (with-open [c (zoo/connect connect-string)]
      (.zConnect (.zPair z c))
      (.zConnect (.zPair r c))
      (comment >break!))))

;; Upgrade to resilient zclient with go-loop monitoring its events and updating zref
(defn ex1 []
  (let [connect-string (str $connect-string "/drcfg")
        z (zref/zref $path {})
        ch (async/chan 1)]
    (async/go-loop [paired? false]
      (if-let [[ev client :as m] (async/<! ch)]
        (do
          (log/debugf "Drcfg go-loop for zref %s received: %s" (.path z) m)
          (recur
           (case ev
           (:ConnectedReadOnly :SyncConnected)
           (let [state (or paired? (boolean (.zPair z client)))]
             (.zConnect z)
             state)
           :Disconnected (do (.zDisconnect z) paired?)
           :Expired false
           (do (log/warnf "[%s] Received unexpected message: " paired? m) paired?))))
        (do ;; client input channel has closed, we're outta here
          (.zDisconnect z)
          (log/debugf "Client input channel has closed for %s, shutting down" (.path z)))))
    (with-open [c (zclient/create connect-string ch)]
      (comment >break!))))

;; Use public drcfg vars
(defn ex2 []
  (binding [roomkey.drcfg/*registry* (atom #{})]
    (let [r (>- "/" nil)
          z (>- $path {})]
      (with-open [c (open @roomkey.drcfg/*registry* $connect-string)]
        (comment >break!)))))
