(ns jepsen.cassandra
(:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
		    [control :as c :refer [| lit]]
                    [db :as db]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))



(defn cached-install?
  [src]
  (try (c/exec :grep :-s :-F :-x (lit src) (lit ".download"))
       true
       (catch RuntimeException _ false)))

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
        (do (if tpath ;;else (not found locally, download it)
              (c/upload tpath "/tmp/cassandra.tar.gz")
              (c/exec :wget :-O "cassandra.tar.gz" url (lit ";")))
            (c/exec :tar :xzvf "cassandra.tar.gz" :-C "~")
            (c/exec :rm :-r :-f (lit "~/cassandra"))
            (c/exec :mv (lit "~/apache* ~/cassandra"))
            (c/exec :echo url :> (lit ".download")))))
)))



(defn db
  "Cassandra for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing cassandra" version)
      (doto node      
	(install! version)))

    (teardown! [_ test node]
      (info node "tearing down cassandra"))))




(defn cassandra-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
	 {:name "cassandra"
          :os   debian/os
          :db   (db "3.11.3")}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn cassandra-test})
                   (cli/serve-cmd))
            args))
