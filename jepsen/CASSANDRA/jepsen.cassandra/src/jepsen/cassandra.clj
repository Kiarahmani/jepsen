(ns jepsen.cassandra
(:require   
	    [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
		    [control :as c :refer [| lit]]
                    [db :as db]
                    [client :as client]
		    [tests :as tests]
		    [generator :as gen]
		    [util      :as util :refer [meh timeout]]]
            ;[jepsen.control.util :as net/util]
            [jepsen.control.net :as net]
	    [jepsen.os.debian :as debian]
)
(:import (clojure.lang ExceptionInfo)
           (java.net InetAddress)
	   (App))
)


;; UTILS
;;====================================================================================
(defn cached-install?
  [src]
  (try (c/exec :grep :-s :-F :-x (lit src) (lit ".download"))
       true
       (catch RuntimeException _ false)))
;
(defn disable-hints?
  "Returns true if Jepsen tests should run without hints"
  []
  (not (System/getenv "JEPSEN_DISABLE_HINTS")))
;
(defn phi-level
  "Returns the value to use for phi in the failure detector"
  []
  (or (System/getenv "JEPSEN_PHI_VALUE")
      8))
;
(defn coordinator-batchlog-disabled?
  "Returns whether to disable the coordinator batchlog for MV"
  []
  (boolean (System/getenv "JEPSEN_DISABLE_COORDINATOR_BATCHLOG")))
;
(defn compressed-commitlog?
  "Returns whether to use commitlog compression"
  []
  (= (some-> (System/getenv "JEPSEN_COMMITLOG_COMPRESSION") (clojure.string/lower-case))
     "true"))


;;====================================================================================
(defn initJava!
	"Installs Java on the given node"
	[node version]
	(if false ;;TODO: automate java detection	
	(do 
	(info node "Installing Java...")
	(c/su
   	(c/cd
	(c/exec :echo
     		"deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main"
     		:>"/etc/apt/sources.list.d/webupd8team-java.list")
    	(c/exec :echo
     		"deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main"
     		:>> "/etc/apt/sources.list.d/webupd8team-java.list")
    		(try (c/exec :apt-key :adv :--keyserver "hkp://keyserver.ubuntu.com:80"
              		:--recv-keys "EEA14886")
        		(debian/update!)
        	 	(catch RuntimeException e
           			(info "Error updating caused by" e)))
    		(c/exec :echo
            		"debconf shared/accepted-oracle-license-v1-1 select true"
            		| :debconf-set-selections)
    		(debian/install [:oracle-java8-installer]))))
) 
)




;;====================================================================================
(defn install!
	"Installs Cassandra on the given node"
	[node version]
	(info "starting installation...")
 (c/su
   (c/cd
    "/tmp"
    (let [tpath (System/getenv "CASSANDRA_TARBALL_PATH")
          url (or tpath
                  (System/getenv "CASSANDRA_TARBALL_URL")
                  (str "http://apache.claz.org/cassandra/" version
                       "/apache-cassandra-" version "-bin.tar.gz"))]
      (info node "installing Cassandra from" url)
      (if (cached-install? url)
        (info "Used cached install on node" node)
        (do (if tpath ;; else: if not found locally, download it
              (c/upload tpath "/tmp/cassandra.tar.gz")
              (c/exec :wget :-O "cassandra.tar.gz" url (lit ";")))
            (c/exec :tar :xzvf "cassandra.tar.gz" :-C "~")
            (c/exec :rm :-r :-f (lit "~/cassandra"))
            (c/exec :mv (lit "~/apache* ~/cassandra"))
            (c/exec :echo url :> (lit ".download"))))))))


;;====================================================================================
(defn dns-resolve
  "Gets the address of a hostname"
  [hostname]
  (.getHostAddress (InetAddress/getByName (name hostname))))


;;====================================================================================


(defn configure!
  "Uploads configuration files to the given node."
  [node test]
  (info node "configuring Cassandra...")
  (c/su
 	(doseq [rep ["\"s/#MAX_HEAP_SIZE=.*/MAX_HEAP_SIZE='512M'/g\""
                     "\"s/#HEAP_NEWSIZE=.*/HEAP_NEWSIZE='128M'/g\""
                     "\"s/LOCAL_JMX=yes/LOCAL_JMX=no/g\""
                (str "'s/# JVM_OPTS=\"$JVM_OPTS -Djava.rmi.server.hostname="
                     "<public name>\"/JVM_OPTS=\"$JVM_OPTS -Djava.rmi.server.hostname="
                     (name node) "\"/g'")
                (str "'s/JVM_OPTS=\"$JVM_OPTS -Dcom.sun.management.jmxremote"
                     ".authenticate=true\"/JVM_OPTS=\"$JVM_OPTS -Dcom.sun.management"
                     ".jmxremote.authenticate=false\"/g'")
                "'/JVM_OPTS=\"$JVM_OPTS -Dcassandra.mv_disable_coordinator_batchlog=.*\"/d'"]]
     (c/exec :sed :-i (lit rep) "~/cassandra/conf/cassandra-env.sh"))
     (doseq [rep (into ["\"s/cluster_name: .*/cluster_name: 'jepsen'/g\""
                      "\"s/row_cache_size_in_mb: .*/row_cache_size_in_mb: 20/g\""
		      "\"s/endpoint_snitch: .*/endpoint_snitch: GossipingPropertyFileSnitch/g\""
                      "\"s/seeds: .*/seeds: 'n1,n2'/g\""
                      (str "\"s/listen_address: .*/listen_address: " (dns-resolve node)
                           "/g\"")
                      (str "\"s/rpc_address: .*/rpc_address: " (dns-resolve node) "/g\"")
                      (str "\"s/broadcast_rpc_address: .*/broadcast_rpc_address: "
                           (net/local-ip) "/g\"")
                      "\"s/internode_compression: .*/internode_compression: none/g\""
                      (str "\"s/hinted_handoff_enabled:.*/hinted_handoff_enabled: "
                           (disable-hints?) "/g\"")
                      "\"s/commitlog_sync: .*/commitlog_sync: batch/g\""
                      (str "\"s/# commitlog_sync_batch_window_in_ms: .*/"
                           "commitlog_sync_batch_window_in_ms: 1.0/g\"")
                      "\"s/commitlog_sync_period_in_ms: .*/#/g\""
                      (str "\"s/# phi_convict_threshold: .*/phi_convict_threshold: " (phi-level)
                           "/g\"")
                       "\"/auto_bootstrap: .*/d\""]
                      (when (compressed-commitlog?)
                      ["\"s/#commitlog_compression.*/commitlog_compression:/g\""
                      (str "\"s/#   - class_name: LZ4Compressor/"
                      "    - class_name: LZ4Compressor/g\"")]))]
     		      (c/exec :sed :-i (lit rep) "~/cassandra/conf/cassandra.yaml"))
     (doseq [rep (into [(str "\"s/dc=dc1/dc=dc_" node "/g\"")
			(str "\"s/rack=rack1/rack=rack_" node "/g\"")])]
     		      (c/exec :sed :-i (lit rep) "~/cassandra/conf/cassandra-rackdc.properties"))

     (c/exec :echo (str "JVM_OPTS=\"$JVM_OPTS -Dcassandra.mv_disable_coordinator_batchlog="
                      	(coordinator-batchlog-disabled?) "\"") 
     			:>> "~/cassandra/conf/cassandra-env.sh")
     (c/exec :sed :-i (lit "\"s/INFO/DEBUG/g\"") "~/cassandra/conf/logback.xml")
;     (c/exec :echo (str "auto_bootstrap: " (-> test :bootstrap deref node boolean))
;           		:>> "~/cassandra/conf/cassandra.yaml")
;TODO : understand why the above command is necessary and why it fails
(info node "configured CASSANDRA")
))

;; CLIENT
;;====================================================================================
(defn r   [_ _] {:type :invoke, :f :readTxn, :value nil})
(defn w   [_ _] {:type :invoke, :f :writeTxn, :value (rand-int 5)})


(defrecord Client [conn]
  client/Client
  (open! [this test node]
	(assoc this :conn (info "OPENNING A CONNECTION" node))
	)    
  (setup! [this test])
  (invoke! [this test op]
	(case (:f op)
        :readTxn (assoc op :type :ok, :value 12)
	:writeTxn (do (info conn)
			
			(App/testFunction )
                            (assoc op :type, :ok))
))
  (teardown! [this test])
  (close! [_ test]))

;;====================================================================================
(defn start!
  "Starting Cassandra..."
  [node test]
  (info node "starting Cassandra")
  (c/su
   (c/exec (lit "~/cassandra/bin/cassandra -R"))
))

(defn stop!
  "Stopping Cassandra..."
  [node]
  (info node "stopping Cassandra")
  (c/su
   (meh (c/exec :killall :java))
   (while (.contains (c/exec :ps :-ef) "java")
     (Thread/sleep 100)))
  (info node "has stopped Cassandra"))

(defn wipe!
  "Shuts down Cassandra and wipes data."
  [node]
  (stop! node)
  (info node "deleting data files")
  (c/su
   (meh (c/exec :rm :-r "~/cassandra/logs"))
   (meh (c/exec :rm :-r "~/cassandra/data/data"))
   (meh (c/exec :rm :-r "~/cassandra/data/hints"))
   (meh (c/exec :rm :-r "~/cassandra/data/commitlog"))
   (meh (c/exec :rm :-r "~/cassandra/data/saved_caches"))))

(def dir     "~/cassandra/logs")
(def logfile (str dir "/system.log"))




;;====================================================================================

;;====================================================================================
(defn db
  "Cassandra for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing cassandra" version)
      (wipe! node)
      (doto node      
	(install! version)
	(initJava! version)
	(configure! test)
	;(start! test)
))


    (teardown! [_ test node]
      (info node "tearing down cassandra")
 ;     (wipe! node)
    )
     db/LogFiles
      (log-files [_ test node]
         [logfile])
))

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
	  :client (Client. nil)
	  :generator (->> (gen/mix [r w])
                          (gen/stagger 1)
                          (gen/nemesis nil)
                          (gen/time-limit 5))}))

;;====================================================================================
(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn cassandra-test})
                   (cli/serve-cmd))
            args))
