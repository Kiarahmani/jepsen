(in-ns 'jepsen.cassandra)

;; CLIENT
;;====================================================================================
(defn r   [_ _] {:type :invoke, :f :readTxn, :value nil})
(defn w   [_ _] {:type :invoke, :f :writeTxn, :value (rand-int 500)})

;;====================================================================================
(defrecord Client [conn]
  client/Client
  (open! [this test node]
	(assoc this :conn (App/getConnection (dns-resolve node))))    
  (setup! [this test]
      (do (App/writeTxn conn -10)
	    (Thread/sleep 1000)))
  (invoke! [this test op]
	(case (:f op)
        :readTxn (assoc op :type :ok, :value (App/readTxn conn))
	:writeTxn (do (App/writeTxn conn (:value op))
                      (assoc op :type, :ok))))
  (teardown! [this test]
	(do (App/closeConnection conn)))
  (close! [_ test]))


