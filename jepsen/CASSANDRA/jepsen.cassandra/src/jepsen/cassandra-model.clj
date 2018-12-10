(in-ns 'jepsen.cassandra)

(defprotocol Model
  (step [model op]))

;; UTIL DEFS
;;====================================================================================
(defrecord Inconsistent [msg]
  Model
  (step [this op] this)
  Object
  (toString [this] msg))

(defn inconsistent
  "Represents an invalid termination of a model; e.g. that an operation could
  not have taken place."
  [msg]
  (Inconsistent. msg))

(defn inconsistent?
  "Is a model inconsistent?"
  [model]
  (instance? Inconsistent model))

;; DATA STRUCTURES
;;====================================================================================
(defrecord MyRegister [key value]
  Model
  (step [r op]
    (let [v'   (:value op)
	  rk (:mkey op)]
        (condp = (:f op)
          :writeTxn (MyRegister. key (* 2 v'))
          :readTxn  (cond
                   ;; Read the expected value of the register,
                   ;; update the last known position
                   (or (nil? v')
		       (= -20 v')
                       (= value v'))
                   (MyRegister. key value)
                   true (inconsistent (str "read value: " v'
                                                 " -- expected: " value))))))
  Object
  (toString [this] (pr-str value)))

(defn my-register []
  (MyRegister. 0 0))

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
		(recur s' (rest history)))))))) ))








