#lang typed/racket

(require "csv.rkt")

(require racket/logging)

(port-count-lines-enabled #true)

(define unix.csv : Path-String (#%csv))

((inst with-logging-to-port (U (Listof (Listof CSV-Field)) (Listof (Vectorof CSV-Field))))
 (current-error-port)
 (λ [] (sequence->list (in-csv* unix.csv #false #:dialect csv::unix)))
 'debug)
