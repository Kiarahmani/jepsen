(in-ns 'jepsen.cassandra)


(def closeConnection (fn [conn]    (SeatsClient/closeConnection conn)))
(def openConnection  (fn [ip]      (SeatsClient/getConnection ip)))

(def operationMap [{:n 1, :f :writeTxn,   :javaFunc (fn [conn args] (SeatsClient/writeTransaction conn (nth args 0) (nth args 1) )), :freq 0.33} ,
                   {:n 2, :f :readTxn,    :javaFunc (fn [conn args] (SeatsClient/readTransaction  conn (nth args 0) (nth args 1) )), :freq 0.67}])


;====================================================================================================
(defn getNextArgs
  "generates input arguments for the requested transaction"
  [txnNo]
  (condp = txnNo
    1 [(rand-int consts/_NUM_KEYS),(rand-int 50)]
    2 [(rand-int consts/_NUM_KEYS),(rand-int 50)]
    )
)


