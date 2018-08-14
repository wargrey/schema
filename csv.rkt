#lang typed/racket/base

(provide (all-defined-out))

(require "digitama/exchange/csv.rkt")

(define read-csv : (-> (U Path-String Input-Port) Boolean [#:delimiter Char] [#:quotes Char] [#:comment-char (Option Char)]
                       (Listof (Listof CSV-Field)))
  (lambda [/dev/csvin skip-header? #:delimiter [delimiter #\,] #:quotes [quotes #\"] #:comment-char [maybe-comment-char #false]]
    (cond [(input-port? /dev/csvin)
           (port-count-lines! /dev/csvin)
           (csv-read /dev/csvin skip-header? delimiter quotes maybe-comment-char)]
          [(not (file-exists? /dev/csvin)) null]
          [else (call-with-input-file* /dev/csvin
                  (λ [[/dev/csvin : Input-Port]]
                    (read-csv #:delimiter delimiter #:quotes quotes #:comment-char maybe-comment-char
                               /dev/csvin skip-header?)))])))
