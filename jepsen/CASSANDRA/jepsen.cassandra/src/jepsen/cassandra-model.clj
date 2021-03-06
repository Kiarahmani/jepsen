(in-ns 'jepsen.cassandra)

(defprotocol Model
  (step [model op]))

;; UTIL DEFS
;;====================================================================================
;; A model. We define it to represent an invalid model
(defrecord Inconsistent [msg]
  Model
  (step [this op] this)
  Object
  (toString [this] msg))

;; A function to generate an inconsitent model. It takes a message and returns a (inconsistent) model.
(defn inconsistent
  "Represents an invalid termination of a model; e.g. that an operation could
  not have taken place."
  [msg]
  (Inconsistent. msg))

;; A function to check if a model is inconsistent or not. It does so by checking if it is an instance of Inonsistent model abstraction or not
(defn inconsistent?
  "Is a model inconsistent?"
  [model]
  (instance? Inconsistent model))

;; DATA STRUCTURES
;;====================================================================================
(defrecord MyRegister [status]
  Model
  (step [r op]
  (let [opReturnStatus (:value op)]        
	 (condp = (:f op)
          :decTxn (cond (= opReturnStatus 0)
          (MyRegister. opReturnStatus)
           true 
           (inconsistent (str "invariant violated: " opReturnStatus)))
          :incTxn (MyRegister. opReturnStatus)))) 
  Object
  (toString [this] (pr-str status)))

(defn my-register []
  (MyRegister. [0]))

;; CHECKERS
;;====================================================================================
(defn myChecker
  "Ensures that no read returns the first value written by a txn"
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [completed (filter op/ok? history)]
	(loop [s model
	       history completed]
	  (if (empty? history)
	    ;; We've checked every operation in the history
	    {:valid? true
	     :model s}
	    (let [op (first history)
		  s' (step s op)]
	      (if (inconsistent? s')
		{:valid? false
		 :error (:msg s')}
		(recur s' (rest history)))))))

) )) 
;)








