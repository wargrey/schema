#lang typed/racket/base

(provide (all-defined-out) CSV-Field)

(require "digitama/exchange/csv.rkt")

(define read-csv : (-> (U Path-String Input-Port) Positive-Integer Boolean
                       [#:dialect (Option CSV-Dialect)] [#:strict? Boolean]
                       (Listof (Vectorof CSV-Field)))
  (lambda [/dev/csvin n skip-header? #:dialect [dialect #false] #:strict? [strict? #false]]
    (cond [(input-port? /dev/csvin)
           (port-count-lines! /dev/csvin)
           (reverse (csv-read/reversed /dev/csvin (assert n index?) skip-header? (or dialect csv::wargrey) strict?))]
          [(not (file-exists? /dev/csvin)) null]
          [else (call-with-input-file* /dev/csvin
                  (λ [[/dev/csvin : Input-Port]]
                    (read-csv #:dialect dialect #:strict? strict?
                              /dev/csvin n skip-header?)))])))

(define read-csv* : (-> (U Path-String Input-Port) Boolean
                        [#:dialect (Option CSV-Dialect)] [#:strict? Boolean]
                        (Listof (Listof CSV-Field)))
  (lambda [/dev/csvin skip-header? #:dialect [dialect #false] #:strict? [strict? #false]]
    (cond [(input-port? /dev/csvin)
           (port-count-lines! /dev/csvin)
           (reverse (csv-read*/reversed /dev/csvin skip-header? (or dialect csv::wargrey) strict?))]
          [(not (file-exists? /dev/csvin)) null]
          [else (call-with-input-file* /dev/csvin
                  (λ [[/dev/csvin : Input-Port]]
                    (read-csv* #:dialect dialect #:strict? strict?
                               /dev/csvin skip-header?)))])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define make-csv-dialect : (-> [#:delimiter Char] [#:quote-char Char] [#:comment-char (Option Char)] [#:escape-char (Option Char)]
                               [#:skip-leading-space? Boolean] [#:skip-tailing-space? Boolean]
                               CSV-Dialect)
  (lambda [#:delimiter [delimiter #\,] #:quote-char [quotes #\"] #:comment-char [comment-char #false] #:escape-char [escape-char #false]
           #:skip-leading-space? [trim-left? #false] #:skip-tailing-space? [trim-right? #false]]
    (csv-dialect delimiter quotes comment-char escape-char trim-left? trim-right?)))

(define csv::wargrey : CSV-Dialect (make-csv-dialect))
(define csv::unix : CSV-Dialect (make-csv-dialect #:delimiter #\: #:comment-char #\# #:escape-char #\\))
