#lang typed/racket

(require "../csv.rkt")

(define StateDepartment.csv : Path (build-path (#%dir) "StateDepartment.csv"))

(printf "seq: ")
((inst with-logging-to-port Void)
 (current-error-port)
 (λ [] (void (time (sequence->list (in-csv StateDepartment.csv 28 #true)))))
 'debug)
