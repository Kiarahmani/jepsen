(in-ns 'jepsen.cassandra)


(def closeConnection (fn [conn]    (SeatsClient/closeConnection conn)))
(def openConnection  (fn [ip]      (SeatsClient/getConnection ip)))

(def operationMap [{:n 1, :f :DR-TXN, 
                          :javaFunc (fn [conn args] (SeatsClient/deleteReservation conn (nth args 0)(nth args 1)(nth args 2)(nth args 3)(nth args 4))), 
                          :freq 10/100},
                   {:n 2, :f :FF-TXN, 
                          :javaFunc (fn [conn args] (SeatsClient/findFlights conn (nth args 0)(nth args 1)(nth args 2)(nth args 3)(nth args 4))), 
                          :freq 10/100},
                   {:n 3, :f :FOS-TXN, 
                          :javaFunc (fn [conn args] (SeatsClient/findOpenSeats conn (nth args 0))), 
                          :freq 35/100},
                   {:n 4, :f :NR-TXN, 
                          :javaFunc (fn [conn args] (SeatsClient/newReservation conn (nth args 0)(nth args 1)(nth args 2)(nth args 3)(nth args 4)(nth args 5))), 
                          :freq 20/100},
                   {:n 5, :f :UC-TXN, 
                          :javaFunc (fn [conn args] (SeatsClient/updateCustomer conn (nth args 0)(nth args 1)(nth args 2)(nth args 3)(nth args 4)(nth args 5)(nth args 6))), 
                          :freq 10/100},
                   {:n 6, :f :UR-TXN, 
                          :javaFunc (fn [conn args] (SeatsClient/updateReservation conn (nth args 0)(nth args 1)(nth args 2)(nth args 3)(nth args 4)(nth args 5))), 
                          :freq 15/100}

                   ])


;====================================================================================================
; generate a new flight id
(defn gen_flight
  [index]
  {:f_id (SeatsUtils/getExistingResFlightId index)})

; generate a new customer id
(defn gen_cust
  [canBeNull, index]
  (if (and canBeNull (< (rand) consts/_CUST_BY_STR_PROB))
      ; generates a customer with null (=-1) id with a string containing the id
      {:c_id -1,
       :c_id_str (str (SeatsUtils/getExistingResCustomerId index))}
      ; generates a customer with a valid id
      {:c_id (SeatsUtils/getExistingResCustomerId index),
        :c_id_str ""}))

; generate a new airline id
(defn gen_al
  []
  {:al_id (SeatsUtils/getNextAirlineId)})

; generate a new frequent flyer id
(defn gen_ff
  []
  {:ff_c_id_str (str (:c_id (gen_cust false (SeatsUtils/getRandomResIndex)))),
   :ff_al_id (:al_id (gen_al))})
;
;
;
(defn getNextArgs
  "generates input arguments for the requested transaction"
  [txnNo]
  (condp = txnNo
    ;deleteReservation
    1 (let [index (SeatsUtils/getRandomResIndex)
            cust (gen_cust true index) 
            ff   (gen_ff)
            f    (gen_flight index)]
      [(:f_id f),(:c_id cust), (:c_id_str cust), (:ff_c_id_str ff), (:ff_al_id ff)])
    ;FindFlights
    2  [1,1,1,1,1]
    ;FindOpenSeats
    3  [1]
    ;NewReservation
    4  [1,1,1,1,1,(make-array Integer/TYPE 3)]
    ;UpdateCustomer
    5  [1,1,"1",1,1,1,1]
    ;UpdateReservation
    6  [1,1,1,1,1,1]
    (info "ERROR!! ---> UNKNOWN txnNo")
    ))

;====================================================================================================
(defn errorMap
  [errorCode]
  (condp = errorCode 
    -1 "Generic exception thrown."
     1 "No Customer information based on the given c_id_str found."
       "unknown error.")
  )


