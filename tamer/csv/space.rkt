#lang typed/racket

(require "csv.rkt")

(require racket/logging)

(define space.csv : Path-String (#%csv))

(define csv::trim (make-csv-dialect #:skip-leading-space? #true #:skip-trailing-space? #true))

(displayln '===================================================================)
(displayln 'read-port)
(parameterize ([port-count-lines-enabled #true])
  ((inst with-logging-to-port (U (Listof (Listof CSV-Field)) (Listof (Vectorof CSV-Field))))
   (current-error-port)
   (λ [] (append (for/list : (Listof (Listof CSV-Field)) ([row (in-csv* space.csv #false #:dialect csv::rfc)]
                                                          #:when (equal? (last row) "#false")) row)
                 (for/list : (Listof (Listof CSV-Field)) ([row (in-csv* space.csv #false #:dialect csv::trim)]
                                                          #:when (equal? (last row) "#true")) row)))
   'debug))


(displayln '===================================================================)
(displayln 'read-line)
(parameterize ([port-count-lines-enabled #false])
  ((inst with-logging-to-port (U (Listof (Listof CSV-Field)) (Listof (Vectorof CSV-Field))))
   (current-error-port)
   (λ [] (append (for/list : (Listof (Listof CSV-Field)) ([row (in-csv* space.csv #false #:dialect csv::rfc)]
                                                          #:when (equal? (last row) "#false")) row)
                 (for/list : (Listof (Listof CSV-Field)) ([row (in-csv* space.csv #false #:dialect csv::trim)]
                                                          #:when (equal? (last row) "#true")) row)))
   'debug))