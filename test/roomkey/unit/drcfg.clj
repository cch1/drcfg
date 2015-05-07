(ns roomkey.unit.drcfg
  (:require [roomkey.drcfg :refer :all]
            [roomkey.zkutil :as zk]
            [midje.sweet :refer :all]
            [midje.checking.core :refer [extended-=]]))

(defchecker refers-to [expected]
  (checker [actual] (extended-= (deref actual) expected)))

(facts ">- returns a local atom"
  (>- "x" 1) => (refers-to 1)
  (provided
    (zk/serialize 1) => ..whatever..))

(future-fact "connect-with-wait! does half its stuff if there a no local atoms to link"
  (>- "x" 1) => anything
  (connect-with-wait! "hosts") => anything
  (provided
    (zk/zkconn! (as-checker string?)) => ..client..
    (zk/init! ..client..) => ..irrelevantA..))

(future-fact "connect-with-wait! does half its stuff if there a no local atoms to link"
  (>- "x" 1) => anything
  (connect-with-wait! "hosts") => anything
  (provided
    (zk/zkconn! (as-checker string?)) => ..client..
    (zk/init! ..client..) => ..irrelevantA..
    (zk/nget ..client.. anything) => ..irrlevantAA..
    (zk/set-metadata ..client.. anything anything) => ..irrelevantB..
    (zk/watch ..client.. anything fn?) => ..irrelevantC..))
