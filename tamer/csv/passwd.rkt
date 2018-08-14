#lang typed/racket

(require "../../csv.rkt")

(define passwd : Path-String "/etc/passwd")

(read-csv "/etc/passwd" #true #:delimiter #\: #:comment-char #\#)
