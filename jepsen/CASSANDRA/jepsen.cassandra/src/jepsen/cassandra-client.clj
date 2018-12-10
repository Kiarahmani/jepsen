(in-ns 'jepsen.cassandra)

;; CLIENT
;;====================================================================================
(defn r   [_ _] {:type :invoke, :f :readTxn, :value nil, :mkey (rand-int consts/_NUM_ASSERTIONS)})
(defn w   [_ _] {:type :invoke, :f :writeTxn, :value (rand-int 500), :mkey (rand-int consts/_NUM_ASSERTIONS) })

;;====================================================================================
(defrecord Client [conn]
  client/Client
  (open! [this test node]
	(assoc this :conn (SeatsClient/getConnection (dns-resolve node))))    
  (setup! [this test]
      (do (dotimes [i consts/_NUM_ASSERTIONS] (SeatsClient/writeTransaction conn i -10))
	  (Thread/sleep 1000)))
  (invoke! [this test op]
	(case (:f op)
        :readTxn (assoc op :type :ok, :value (SeatsClient/readTransaction conn (:mkey op)), :mkey (:mkey op) )
	:writeTxn (do (SeatsClient/writeTransaction conn (:mkey op) (:value op) )
                      (assoc op :type, :ok))))
  (teardown! [this test]
	(do (SeatsClient/closeConnection conn)))
  (close! [_ test]))


