#lang typed/racket

(require "../../csv.rkt")

(define times : Index 10)
(define passwd : Path-String "/etc/passwd")

(when (file-exists? passwd)
  (define /dev/csv : Input-Port
    (apply input-port-append #true
           (build-list 10 (λ [[i : Index]] : Input-Port
                            (open-input-file passwd)))))

  (time (read-csv /dev/csv 7 #false #:delimiter #\: #:comment-char #\#))
  (close-input-port /dev/csv))
