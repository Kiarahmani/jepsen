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
(def cli-opts
  "Additional command line options."
    [["-i" "--init-db" "wipes down any excisting data and creates a fresh cluster"]
     ["-j" "--init-java" "installs java in freshly created jepsen nodes"]
     ])



;;====================================================================================
(defn db
  "Cassandra for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (info "KIRKIRKIRKIRKIR")      
        (when (:init-db test)
          (do (info node "<<initDB>> installing cassandra" version)
              (wipe! node)
	      (when (:init-java test)
                (initJava! node version))
	      (configure! node test)))
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
  (cli/run! (merge (cli/single-test-cmd {:test-fn cassandra-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
