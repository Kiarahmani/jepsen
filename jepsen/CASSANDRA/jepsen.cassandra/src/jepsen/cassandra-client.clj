(in-ns 'jepsen.cassandra)

;; CLIENT
;;====================================================================================
(def callMap [{:f :openConn, :javaFunc (fn [ip] (SeatsClient/getConnection ip))}
              {:f :closeConn, :javaFunc (fn [conn] (SeatsClient/closeConnection conn))}
              {:f :incTxn, :javaFunc (fn [conn op] (SeatsClient/writeTransaction conn (:mkey op) (:value op))), :freq 50} , 
              {:f :decTxn, :javaFunc (fn [conn op] (SeatsClient/readTransaction  conn (:mkey op) (:value op))), :freq 50}])





(defrecord Client [conn]
  client/Client
  (open! [this test node]
	(assoc this :conn ((:javaFunc (first (filter (fn [m] (= (:f m) :openConn)) callMap))) (dns-resolve node))))    
  (setup! [this test]
  )
  (invoke! [this test op]
      (let [txn (:javaFunc (first (filter (fn [m] (= (:f m) (:f op))) callMap)))
            retStatus (txn conn op)]  
                  (assoc op :type :ok, :value retStatus)))
        
  (teardown! [this test]
	(do ((:javaFunc (first (filter (fn [m] (= (:f m) :closeConn)) callMap))) conn)))
  (close! [_ test]))


