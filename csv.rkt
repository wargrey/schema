#lang typed/racket/base

(provide (all-defined-out))
(provide CSV-Field CSV-Dialect CSV-Row CSV-Row*)

(require "digitama/exchange/csv/reader.rkt")
(require "digitama/exchange/csv/readline.rkt")
(require "digitama/exchange/csv/misc.rkt")

;;; Note
; 1. `file->bytes` does not improve the performance significantly
; 2. `reverse` is not harmful for performance
; 3. the performance of `read-csv` and `sequence->list . in-csv` can be considered identity
;     (e.g. for a 60MB CSV, the difference is within (-200ms, +200ms)).

(define read-csv : (-> CSV-StdIn Positive-Integer Boolean [#:dialect (Option CSV-Dialect)] [#:strict? Boolean] (Listof (Vectorof CSV-Field)))
  (lambda [/dev/stdin n skip-header? #:dialect [dialect #false] #:strict? [strict? #false]]
    (parameterize ([current-custodian (make-custodian)])
      (dynamic-wind
       (λ [] (void))
       (λ [] (let ([/dev/csvin (csv-input-port /dev/stdin skip-header? #false)])
               (reverse (if (csv-readline? /dev/csvin)
                            (csv-readline/reverse /dev/csvin (assert n index?) (or dialect csv::rfc) strict?)
                            (csv-read/reverse /dev/csvin (assert n index?) (or dialect csv::rfc) strict?)))))
       (λ [] (custodian-shutdown-all (current-custodian)))))))

(define read-csv* : (-> CSV-StdIn Boolean [#:dialect (Option CSV-Dialect)] [#:strict? Boolean] (Listof (Pairof CSV-Field (Listof CSV-Field))))
  (lambda [/dev/stdin skip-header? #:dialect [dialect #false] #:strict? [strict? #false]]
    (parameterize ([current-custodian (make-custodian)])
      (dynamic-wind
       (λ [] (void))
       (λ [] (let ([/dev/csvin (csv-input-port /dev/stdin skip-header? #false)])
               (reverse (if (csv-readline? /dev/csvin)
                            (csv-readline*/reverse /dev/csvin (or dialect csv::rfc) strict?)
                            (csv-read*/reverse /dev/csvin (or dialect csv::rfc) strict?)))))
       (λ [] (custodian-shutdown-all (current-custodian)))))))

(define in-csv : (-> CSV-StdIn Positive-Integer Boolean [#:dialect (Option CSV-Dialect)] [#:strict? Boolean] (Sequenceof (Vectorof CSV-Field)))
  (lambda [/dev/stdin n skip-header? #:dialect [dialect #false] #:strict? [strict? #false]]
    (define /dev/csvin : Input-Port (csv-input-port /dev/stdin skip-header? #true))
    (if (csv-readline? /dev/csvin)
        (in-csv-line-port /dev/csvin (assert n index?) (or dialect csv::rfc) strict?)
        (in-csv-port /dev/csvin (assert n index?) (or dialect csv::rfc) strict?))))

(define in-csv* : (-> CSV-StdIn Boolean [#:dialect (Option CSV-Dialect)] [#:strict? Boolean] (Sequenceof (Pairof CSV-Field (Listof CSV-Field))))
  (lambda [/dev/stdin skip-header? #:dialect [dialect #false] #:strict? [strict? #false]]
    (define /dev/csvin : Input-Port (csv-input-port /dev/stdin skip-header? #true))
    (if (csv-readline? /dev/csvin)
        (in-csv-line-port* /dev/csvin (or dialect csv::rfc) strict?)
        (in-csv-port* /dev/csvin (or dialect csv::rfc) strict?))))

(define csv-empty-line? : (-> (Listof CSV-Field) Boolean)
  (lambda [row]
    (eq? row empty-row)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define make-csv-dialect : (-> [#:delimiter Char] [#:quote-char (Option Char)] [#:escape-char (Option Char)] [#:comment-char (Option Char)]
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

(define csv::rfc : CSV-Dialect (make-csv-dialect))
(define csv::unix : CSV-Dialect (make-csv-dialect #:delimiter #\: #:quote-char #false #:comment-char #\# #:escape-char #\\))
