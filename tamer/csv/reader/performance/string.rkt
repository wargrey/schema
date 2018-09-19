#lang typed/racket/base

(require "../csv.rkt")

(define StateDepartment.csv : Path (build-path (#%dir) "StateDepartment.csv"))

(printf "str: ")
(define vectors : (Listof (Vectorof CSV-Field))
  (with-logging-to-port (current-error-port)
    (λ [] (let ([/dev/strin (time (file->string StateDepartment.csv))])
            (printf "     ")
            (time (read-csv /dev/strin 28 #true))))
    'debug))
