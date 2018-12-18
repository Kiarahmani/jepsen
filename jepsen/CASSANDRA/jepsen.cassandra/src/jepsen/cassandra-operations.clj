(in-ns 'jepsen.cassandra)


(def closeConnection (fn [conn]    (SeatsClient/closeConnection conn)))
(def openConnection  (fn [ip]      (SeatsClient/getConnection ip)))

(def operationMap [{:n 1, :f :incTxn,    :javaFunc (fn [conn op] (SeatsClient/writeTransaction conn (nth (:args op) 0) (nth (:args op) 1) )), :freq 0.95} ,
                   {:n 2, :f :decTxn,    :javaFunc (fn [conn op] (SeatsClient/readTransaction  conn (nth (:args op) 0) (nth (:args op) 1))), :freq 0.05}])


;====================================================================================================
(defn getNextArgs
  "generates input arguments for the requested transaction"
  [txnNo]
  (condp = txnNo
  1 [(rand-int consts/_NUM_KEYS),(rand-int 50)]
  2 [(rand-int consts/_NUM_KEYS),(rand-int 50)])
)


