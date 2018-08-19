#lang typed/racket/base

(provide (all-defined-out))
(provide CSV-Field CSV-Dialect)

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

(define csv-empty-line? : (-> (Listof CSV-Field) Boolean)
  (lambda [row]
    (eq? row empty-row)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define make-csv-dialect : (-> [#:delimiter Char] [#:quote-char Char] [#:escape-char (Option Char)] [#:comment-char (Option Char)]
                               [#:skip-empty-line? Boolean] [#:skip-leading-space? Boolean] [#:skip-trailing-space? Boolean]
                               CSV-Dialect)
  (let ([<eq?>-char? : (-> (Option Char) Boolean) (λ [ch] (or (not ch) (char<? ch #\Ā)))])
    (lambda [#:delimiter [<:> #\,] #:quote-char [</> #\"] #:escape-char [<\> #false] #:comment-char [<#> #false]
             #:skip-empty-line? [skip-empty-line? #true] #:skip-leading-space? [trim-left? #false] #:skip-trailing-space? [trim-right? #false]]
      (assert <:> <eq?>-char?)
      (assert </> <eq?>-char?)
      (assert <\> <eq?>-char?)
      (assert <#> <eq?>-char?)
      
      (CSV-Dialect <:> </> (if (eq? </> <\>) #false <\>) <#>
                   skip-empty-line? trim-left? trim-right?))))

(define csv::wargrey : CSV-Dialect (make-csv-dialect))
(define csv::unix : CSV-Dialect (make-csv-dialect #:delimiter #\: #:comment-char #\# #:escape-char #\\))
