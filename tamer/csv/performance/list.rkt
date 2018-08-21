#lang typed/racket/base

(require "../csv.rkt")

(define StateDepartment.csv : Path (build-path (#%dir) "2015_StateDepartment.csv"))

(printf "v: ")
(define vectors : (Listof (Vectorof CSV-Field))
  (with-logging-to-port (current-error-port)
    (λ [] (time (read-csv StateDepartment.csv 28 #true)))
    'debug))

(printf "l: ")
(define lists : (Listof (Listof CSV-Field))
  (with-logging-to-port (current-error-port)
    (λ [] (time (read-csv* StateDepartment.csv #true)))
    'debug))
