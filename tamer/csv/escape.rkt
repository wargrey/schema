#lang typed/racket

(require "csv.rkt")

(require racket/logging)

(define escape.csv : Path-String (#%csv))

(define csv::escape (make-csv-dialect #:escape-char #\\))

(displayln '===================================================================)
(displayln 'read-port)
(parameterize ([port-count-lines-enabled #true])
  ((inst with-logging-to-port (U (Listof (Listof CSV-Field)) (Listof (Vectorof CSV-Field))))
   (current-error-port)
   (λ [] (sequence->list (in-csv* escape.csv #false #:dialect csv::escape)))
   'debug))


(displayln '===================================================================)
(displayln 'read-line)
(parameterize ([port-count-lines-enabled #false])
  ((inst with-logging-to-port (U (Listof (Listof CSV-Field)) (Listof (Vectorof CSV-Field))))
   (current-error-port)
   (λ [] (sequence->list (in-csv* escape.csv #false #:dialect csv::escape)))
   'debug))