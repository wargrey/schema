#lang typed/racket

(require "csv.rkt")

(require racket/logging)

(require syntax/location)

(port-count-lines-enabled #true)

(define passwd.csv : Path-String (#%csv))

((inst with-logging-to-port (U (Listof (Listof CSV-Field)) (Listof (Vectorof CSV-Field))))
 (current-error-port)
 (λ []
   (call-with-input-file passwd.csv
     (λ [[/dev/csvin : Input-Port]]
       (read-csv* /dev/csvin #false #:dialect csv::unix))))
 'debug)
