(ns jepsen.constants)

(def _NUM_KEYS 5)


; constants
(def _CUST_BY_STR_PROB 0.05)
(def _FF_SHOULD_BE_UPDATED 0.5)
(def _KEYSPACE_NAME "seats")


; test scale knobs (make sure appropriate snapshots will be used)
(def _COSTUMER_COUNT 100)
(def _MAXIMUM_ACCEPTABLE_DISTANCE 100)
(def _FLIGHT_COUNT   5)
(def _AIRLINE_COUNT   2)
(def _AIRPORT_COUNT 286)
