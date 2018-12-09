(in-ns 'jepsen.cassandra)

;; CLIENT
;;====================================================================================
(defn r   [_ _] {:type :invoke, :f :readTxn, :value nil})
(defn w   [_ _] {:type :invoke, :f :writeTxn, :value (rand-int 500)})

;;====================================================================================
(defrecord Client [conn]
  client/Client
  (open! [this test node]
	(assoc this :conn (SeatsClient/getConnection (dns-resolve node))))    
  (setup! [this test]
      (do (SeatsClient/writeTxn conn -10)
	    (Thread/sleep 1000)))
  (invoke! [this test op]
	(case (:f op)
        :readTxn (assoc op :type :ok, :value (SeatsClient/readTxn conn))
	:writeTxn (do (SeatsClient/writeTxn conn (:value op))
                      (assoc op :type, :ok))))
  (teardown! [this test]
	(do (SeatsClient/closeConnection conn)))
  (close! [_ test]))


