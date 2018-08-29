#lang typed/racket/base

(require "csv.rkt")

(require racket/logging)

(define fixed-row.csv : Path-String (#%csv))

((inst with-logging-to-port (Listof (Vectorof CSV-Field)))
 (current-error-port)
 (λ [] (for/list : (Listof (Vectorof CSV-Field)) ([row (in-csv fixed-row.csv 5 #true)])
         row))
 'debug)

((inst with-logging-to-port (Listof (Vectorof CSV-Field)))
 (current-error-port)
 (λ [] (for/list : (Listof (Vectorof CSV-Field)) ([row (in-csv fixed-row.csv 5 #true)])
         row))
 'debug)
