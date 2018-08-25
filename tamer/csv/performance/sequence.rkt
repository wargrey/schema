#lang typed/racket

(require "../csv.rkt")

(define StateDepartment.csv : Path (build-path (#%dir) "StateDepartment.csv"))

(newline)

(printf "v: ")
(define vectors : (Listof (Vectorof CSV-Field))
  (with-logging-to-port (current-error-port)
    (λ [] (time (read-csv StateDepartment.csv 28 #true)))
    'debug))

(printf "s: ")
((inst with-logging-to-port Void)
 (current-error-port)
 (λ [] (void (time (sequence->list (in-csv StateDepartment.csv 28 #true)))))
 'debug)
