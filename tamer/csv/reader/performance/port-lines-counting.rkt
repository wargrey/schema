#lang typed/racket/base

(require "../csv.rkt")

(require racket/file)

(port-count-lines-enabled #true)

(define StateDepartment.csv : Path (build-path (#%dir) "StateDepartment.csv"))

(printf "vct: ")
(define vectors : (Listof (Vectorof CSV-Field))
  (with-logging-to-port (current-error-port)
    (λ [] (with-input-from-file StateDepartment.csv
            (λ [] (time (read-csv (current-input-port) 28 #true)))))
    'debug))

(printf "lst: ")
(define lists : (Listof (Listof CSV-Field))
  (with-logging-to-port (current-error-port)
    (λ [] (with-input-from-file StateDepartment.csv
            (λ [] (time (read-csv* (current-input-port) #true)))))
    'debug))
