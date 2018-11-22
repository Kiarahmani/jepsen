(defproject jepsen.cassandra "0.1.0-SNAPSHOT"
  :description "A Jepsen test for Cassandra"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.cassandra
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [jepsen "0.1.8"]
		 [environ "1.1.0"]]
  :java-source-paths ["/root/java/"]
)
