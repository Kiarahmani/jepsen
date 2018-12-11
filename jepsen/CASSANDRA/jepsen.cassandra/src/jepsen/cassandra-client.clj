(in-ns 'jepsen.cassandra)

;; CLIENT
;;====================================================================================
;(defn r   [_ _] {:type :invoke, :f :readTxn, :value nil, :mkey (rand-int consts/_NUM_ASSERTIONS)})
;(defn w   [_ _] {:type :invoke, :f :writeTxn, :value (rand-int 500), :mkey (rand-int consts/_NUM_ASSERTIONS) })
(defn d   [_ _] {:type :invoke, :f :decTxn, :value (rand-int 50), :mkey (rand-int consts/_NUM_KEYS) })
(defn i   [_ _] {:type :invoke, :f :incTxn, :value (rand-int 50), :mkey (rand-int consts/_NUM_KEYS) })

;;====================================================================================
(defrecord Client [conn]
  client/Client
  (open! [this test node]
	(assoc this :conn (SeatsClient/getConnection (dns-resolve node))))    
  (setup! [this test]
      (do (dotimes [i consts/_NUM_KEYS] (SeatsClient/initTransaction conn i))
	  (Thread/sleep 1000)))
  (invoke! [this test op]
	(case (:f op)
        ;:readTxn (assoc op :type :ok, :value (SeatsClient/readTransaction conn (:mkey op)), :mkey (:mkey op) )
        :incTxn (assoc op :type :ok, :value (SeatsClient/incTransaction conn (:mkey op) (:value op)))
	:decTxn (let [retStatus (SeatsClient/decTransaction conn (:mkey op) (:value op))]  
                  (if (not (= retStatus 0))  
                  (assoc op :type :ok, :value (str "Some Invariant is Broken in " op "  status:" retStatus))
                  (assoc op :type :ok, :value 0))
                  )))
        
  (teardown! [this test]
	(do (SeatsClient/closeConnection conn)))
  (close! [_ test]))


