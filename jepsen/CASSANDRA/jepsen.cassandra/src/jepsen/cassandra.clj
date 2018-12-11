(ns jepsen.cassandra
(:require   
	    [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
		    [control :as c :refer [| lit]]
                    [db :as db]
		    [checker :as checker]
		    [client :as client]
		    [tests :as tests]
		    [nemesis :as nemesis]
		    [generator :as gen]
		    [util      :as util :refer [meh timeout]]]
            [jepsen.control.util :as cu]
	    [jepsen.control.net :as net]
	    [jepsen.checker.timeline :as timeline]
	    [jepsen.os.debian :as debian]
	    [slingshot.slingshot :refer [try+]]
	    [knossos.model :as model]
	    [knossos.op :as op]
            [jepsen.constants :as consts]
)
(:import (clojure.lang ExceptionInfo)
           (java.net InetAddress)
	   (SeatsClient))
)

(load "cassandra-db")
(load "cassandra-model")
(load "cassandra-client")


;;====================================================================================
(defn db
  "Cassandra for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing cassandra" version)
      ;(wipe! node)
      (doto node      
	(install! version)
	(initJava! version)
	(configure! test)
	(start! test)
))
    (teardown! [_ test node]
      (info node "tearing down cassandra")
      ;(wipe! node)
    )
     db/LogFiles
      (log-files [_ test node]
         [logfile])))

;;====================================================================================
(defn cassandra-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
	 {:name "cassandra"
          :os   debian/os
          :db   (db "3.11.3")
	  :checker (checker/compose
                    {;:perf   (checker/perf)
                     :linear (myChecker)
		     ;:timeline  (timeline/html)
                     })
	  :model      (my-register)
	  :client (Client. nil)
	  :generator (->> (gen/mix [i d])
                          (gen/stagger 1/100)
                          (gen/nemesis nil)
                          (gen/time-limit (:time-limit opts)))}))


;;====================================================================================
(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn cassandra-test})
                   (cli/serve-cmd))
            args))
