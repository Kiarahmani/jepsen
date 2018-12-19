(in-ns 'jepsen.cassandra)


(def closeConnection (fn [conn]    (SeatsClient/closeConnection conn)))
(def openConnection  (fn [ip]      (SeatsClient/getConnection ip)))

(def operationMap [{:n 1, :f :DELETE_RESERVATION_TXN, 
                          :javaFunc (fn [conn args] (SeatsClient/deleteReservation conn (nth args 0)(nth args 1)(nth args 2)(nth args 3)(nth args 4))), 
                          :freq 1}
                   
                   ])


;====================================================================================================
; generate a new flight id
(defn gen_flight
  []
  {:f_id (rand-int consts/_FLIGHT_COUNT)})

; generate a new customer id
(defn gen_cust
  [canBeNull]
  (if (and canBeNull (> (rand) consts/_CUST_BY_STR_PROB))
      {:c_id -1,
       :c_id_str (str (rand-int consts/_COSTUMER_COUNT))}
      {:c_id (rand-int consts/_COSTUMER_COUNT),
        :c_id_str ""}))

; generate a new airline id
(defn gen_al
  []
  {:al_id (rand-int consts/_AIRLINE_COUNT)})

; generate a new frequent flyer id
(defn gen_ff
  []
  {:ff_c_id_str (str (:c_id (gen_cust false))),
   :ff_al_id (:al_id (gen_al))})
;
;
;
(defn getNextArgs
  "generates input arguments for the requested transaction"
  [txnNo]
  (condp = txnNo
    ;deleteReservation
    1 (let [cust (gen_cust true) 
            ff   (gen_ff)
            f    (gen_flight)]
      [(:f_id f),(:c_id cust), (:c_id_str cust), (:ff_c_id_str ff), (:ff_al_id ff)])
    ))

;====================================================================================================
(defn errorMap
  [errorCode]
  (condp = errorCode 
    -1 "Generic exception thrown."
     1 "No Customer information based on the given c_id_str found."
       "unknown error.")
  )


