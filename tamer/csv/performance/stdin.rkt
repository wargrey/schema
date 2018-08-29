#lang typed/racket/base

(require "../csv.rkt")
(require "../../../digitama/exchange/csv/readline.rkt")

(require racket/string)
(require racket/port)

(define StateDepartment.csv : Path (build-path (#%dir) "StormEvents.csv"))

(define vectors : (Listof (Vectorof CSV-Field))
  (with-logging-to-port (current-error-port)
    (λ [] (time (read-csv StateDepartment.csv 11 #true)))
    'debug))