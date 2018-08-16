#lang typed/racket

(require "../../csv.rkt")

(define times : Index 10)
(define /etc/passwd : Path-String "/etc/passwd")

(when (file-exists? /etc/passwd)
  (define /dev/csvin : Input-Port
    (apply input-port-append #true
           (build-list 10 (λ [[i : Index]] : Input-Port
                            (open-input-file /etc/passwd)))))

  (time (read-csv /dev/csvin 7 #false #:dialect csv::unix))
  (close-input-port /dev/csvin))
