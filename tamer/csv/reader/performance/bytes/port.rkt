#lang typed/racket/base

(require "../../csv.rkt")

(port-count-lines-enabled #false)

(define StateDepartment.csv : Path (build-path (#%dir) 'up "StateDepartment.csv"))

(printf "bprt:  ")
(define vectors : (Listof (Vectorof CSV-Field))
  (with-logging-to-port (current-error-port)
    (λ [] (time (read-csv (open-input-bytes (file->bytes StateDepartment.csv)) 28 #true)))
    'debug))
