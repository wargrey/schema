#lang typed/racket

(require "csv.rkt")

(require racket/logging)

(define unix.csv : Path-String (#%csv))

csv::unix

(displayln '===================================================================)
(displayln 'read-port)
(parameterize ([port-count-lines-enabled #true])
  ((inst with-logging-to-port (U (Listof (Listof CSV-Field)) (Listof (Vectorof CSV-Field))))
   (current-error-port)
   (λ [] (sequence->list (in-csv* unix.csv #false #:dialect csv::unix)))
   'debug))


(displayln '===================================================================)
(displayln 'read-line)
(parameterize ([port-count-lines-enabled #false])
  ((inst with-logging-to-port (U (Listof (Listof CSV-Field)) (Listof (Vectorof CSV-Field))))
   (current-error-port)
   (λ [] (sequence->list (in-csv* unix.csv #false #:dialect csv::unix)))
   'debug))