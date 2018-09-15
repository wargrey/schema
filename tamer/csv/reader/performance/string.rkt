#lang typed/racket/base

(require "../csv.rkt")

(define StateDepartment.csv : Path (build-path (#%dir) "StateDepartment.csv"))

(printf "str: ")
(define vectors : (Listof (Vectorof CSV-Field))
  (with-logging-to-port (current-error-port)
    (λ [] (time (let ([/dev/strin (file->string StateDepartment.csv)])
                  (read-csv /dev/strin 28 #true))))
    'debug))
