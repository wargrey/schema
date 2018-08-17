#lang typed/racket

(require "../../csv.rkt")

(require racket/logging)

(define /etc/passwd : Path-String "/etc/passwd")

(when (file-exists? /etc/passwd)
  ((inst with-logging-to-port (Listof (Vectorof CSV-Field)))
   (current-error-port)
   (λ []
     (call-with-input-file /etc/passwd
       (λ [[/dev/csvin : Input-Port]]
         (read-csv /dev/csvin 7 #false #:dialect csv::unix))))
   'debug))
