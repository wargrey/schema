#lang typed/racket/base

(require "../../csv.rkt")

(define StateDepartment.csv : Path (build-path (#%dir) 'up "StateDepartment.csv"))

(printf "bstr:  ")
(define vectors : (Listof (Vectorof CSV-Field))
  (with-logging-to-port (current-error-port)
    (λ [] (time (read-csv (bytes->string/utf-8 (file->bytes StateDepartment.csv)) 28 #true)))
    'debug))
