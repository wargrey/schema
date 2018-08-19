#lang typed/racket/base

(require "../../csv.rkt")

(require racket/logging)
(require racket/path)

(require syntax/location)

(port-count-lines-enabled #true)

(define fixed-row.csv : Path-String (assert (file-name-from-path (path-replace-extension (quote-source-file) #".csv")) path?))

((inst with-logging-to-port (Listof (Vectorof CSV-Field)))
 (current-error-port)
 (λ [] (call-with-input-file* fixed-row.csv
         (λ [[/dev/csvin : Input-Port]]
           (read-csv /dev/csvin 5 #true))))
 'debug)
