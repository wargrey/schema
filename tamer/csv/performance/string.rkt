#lang typed/racket/base

(require "../csv.rkt")

(require racket/file)

(define StateDepartment.csv : Path (build-path (#%dir) "StateDepartment.csv"))

(printf "str: ")
(define vectors : (Listof (Vectorof CSV-Field))
  (with-logging-to-port (current-error-port)
    (λ [] (time (read-csv (file->string StateDepartment.csv) 28 #true)))
    'debug))
