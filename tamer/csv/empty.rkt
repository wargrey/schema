#lang typed/racket

(require "csv.rkt")

(require racket/logging)

(define empty.csv : Path-String (#%csv))

(displayln '===================================================================)
(displayln 'read-port)
(parameterize ([port-count-lines-enabled #true])
  (list ((inst with-logging-to-port (U (Listof (Listof CSV-Field)) (Listof (Vectorof CSV-Field))))
         (current-error-port)
         (λ [] (sequence->list (in-csv empty.csv 2 #false #:dialect csv::rfc #:skip-empty-line? #false)))
         'debug)
        
        (sequence->list (in-csv* empty.csv #false #:dialect csv::rfc #:skip-empty-line? #false))
        (read-csv empty.csv 2 #false #:dialect csv::rfc #:skip-empty-line? #false)
        (read-csv* empty.csv #false #:dialect csv::rfc #:skip-empty-line? #false)))


(displayln '===================================================================)
(displayln 'read-line)
(parameterize ([port-count-lines-enabled #false])
  (list ((inst with-logging-to-port (U (Listof (Listof CSV-Field)) (Listof (Vectorof CSV-Field))))
         (current-error-port)
         (λ [] (sequence->list (in-csv empty.csv 2 #false #:dialect csv::rfc #:skip-empty-line? #false)))
         'debug)
        
        (sequence->list (in-csv* empty.csv #false #:dialect csv::rfc #:skip-empty-line? #false))
        (read-csv empty.csv 2 #false #:dialect csv::rfc #:skip-empty-line? #false)
        (read-csv* empty.csv #false #:dialect csv::rfc #:skip-empty-line? #false)))
