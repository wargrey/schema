#lang typed/racket/base

(require "csv.rkt")

(require racket/logging)

(define fixed-row.csv : Path-String (#%csv))

csv::rfc

(displayln '===================================================================)
(displayln 'read-port)
(parameterize ([port-count-lines-enabled #true])
  ((inst with-logging-to-port (Listof (Vectorof CSV-Field)))
   (current-error-port)
   (λ [] (for/list : (Listof (Vectorof CSV-Field)) ([row (in-csv fixed-row.csv 5 #true)])
           row))
   'debug))

(displayln '===================================================================)
(displayln 'read-line)
(parameterize ([port-count-lines-enabled #false])
  ((inst with-logging-to-port (Listof (Vectorof CSV-Field)))
   (current-error-port)
   (λ [] (for/list : (Listof (Vectorof CSV-Field)) ([row (in-csv fixed-row.csv 5 #true)])
           row))
   'debug))
