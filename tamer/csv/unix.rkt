#lang typed/racket

(require "../../csv.rkt")

(require racket/logging)

(require syntax/location)

(define passwd.csv : Path-String (assert (file-name-from-path (path-replace-extension (quote-source-file) #".csv")) path?))

((inst with-logging-to-port (Listof (Vectorof CSV-Field)))
 (current-error-port)
 (λ []
   (call-with-input-file passwd.csv
     (λ [[/dev/csvin : Input-Port]]
       (read-csv /dev/csvin 7 #false #:dialect csv::unix))))
 'debug)
