(in-ns 'jepsen.cassandra)

;; CLIENT
;;====================================================================================
(defn d   [_ _] {:type :invoke, :f :decTxn, :value (rand-int 50), :mkey (rand-int consts/_NUM_KEYS) })
(defn i   [_ _] {:type :invoke, :f :incTxn, :value (rand-int 50), :mkey (rand-int consts/_NUM_KEYS) })

;;====================================================================================
(defrecord Client [conn]
  client/Client
  (open! [this test node]
	(assoc this :conn (SeatsClient/getConnection (dns-resolve node))))    
  (setup! [this test]
    ; initial work required by clients goes here  
    )
  (invoke! [this test op]
	(case (:f op)
        :incTxn (assoc op :type :ok, :value (SeatsClient/incTransaction conn (:mkey op) (:value op)))
	:decTxn (let [retStatus (SeatsClient/decTransaction conn (:mkey op) (:value op))]  
                  (if (not (= retStatus 0))  
                  (assoc op :type :ok, :value (str "Some Invariant is Broken in " op "  status:" retStatus))
                  (assoc op :type :ok, :value 0))
                  )))
        
  (teardown! [this test]
	(do (SeatsClient/closeConnection conn)))
  (close! [_ test]))


