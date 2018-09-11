#lang typed/racket

(require "csv.rkt")

(define skip-header.csv : Path-String (#%csv))

(displayln '===================================================================)
(displayln 'read-port)
(parameterize ([port-count-lines-enabled #true])
  ((inst with-logging-to-port (U (Listof (Listof CSV-Field)) (Listof (Vectorof CSV-Field))))
   (current-error-port)
   (λ [] (for/list : (Listof (Listof CSV-Field)) ([row (in-csv* skip-header.csv #true)]) row))
   'debug))

(displayln '===================================================================)
(displayln 'read-line)
(parameterize ([port-count-lines-enabled #false])
  ((inst with-logging-to-port (U (Listof (Listof CSV-Field)) (Listof (Vectorof CSV-Field))))
   (current-error-port)
   (λ [] (for/list : (Listof (Listof CSV-Field)) ([row (in-csv* skip-header.csv #true)]) row))
   'debug))

(displayln '===================================================================)
(displayln 'read-string)
((inst with-logging-to-port (U (Listof (Listof CSV-Field)) (Listof (Vectorof CSV-Field))))
 (current-error-port)
 (λ [] (for/list : (Listof (Listof CSV-Field)) ([row (in-csv* (file->string skip-header.csv) #true)]) row))
 'debug)
