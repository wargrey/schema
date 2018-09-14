#lang typed/racket/base

(require "../csv.rkt")

(define StateDepartment.csv : Path (build-path (#%dir) "StateDepartment.csv"))

(printf "lst: ")
(define lists : (Listof (Listof CSV-Field))
  (with-logging-to-port (current-error-port)
    (λ [] (time (read-csv* StateDepartment.csv #true)))
    'debug))
