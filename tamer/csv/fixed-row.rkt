#lang typed/racket/base

(require "csv.rkt")

(require racket/logging)

(define fixed-row.csv : Path-String (#%csv))

((inst with-logging-to-port (Listof (Vectorof CSV-Field)))
 (current-error-port)
 (λ [] (call-with-input-file* fixed-row.csv
         (λ [[/dev/csvin : Input-Port]]
           (read-csv /dev/csvin 5 #true))))
 'debug)
