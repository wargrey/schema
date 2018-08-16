#lang typed/racket

(require "../../csv.rkt")

(require racket/logging)

(define /etc/group : Path-String "/etc/group")

(when (file-exists? /etc/group)
  ((inst with-logging-to-port (Listof (Vectorof CSV-Field)))
   (current-error-port)
   (λ []
     (call-with-input-file /etc/group
       (λ [[/dev/csvin : Input-Port]]
         (read-csv /dev/csvin 4 #false #:dialect csv::unix))))
   'debug))
