#lang typed/racket/base

(provide (all-defined-out))
(provide CSV-Field CSV-Dialect CSV-Row*)

(require "digitama/exchange/csv/reader.rkt")

;;; Note
; 1. `file->bytes` does not improve the performance significantly
; 2. `reverse` is not harm for performance 

(define read-csv : (-> CSV-StdIn Positive-Integer Boolean [#:dialect (Option CSV-Dialect)] [#:strict? Boolean] (Listof (Vectorof CSV-Field)))
  (lambda [/dev/stdin n skip-header? #:dialect [dialect #false] #:strict? [strict? #false]]
    (if (input-port? /dev/stdin)
        (reverse (csv-read/reversed /dev/stdin (assert n index?) skip-header? (or dialect csv::rfc) strict?))
        (let ([/dev/csvin (csv-input-port /dev/stdin)])
          (dynamic-wind
           (λ [] (void))
           (λ [] (read-csv /dev/csvin n skip-header? #:dialect dialect #:strict? strict?))
           (λ [] (close-input-port /dev/csvin)))))))

(define read-csv* : (-> CSV-StdIn Boolean [#:dialect (Option CSV-Dialect)] [#:strict? Boolean] (Listof (Pairof CSV-Field (Listof CSV-Field))))
  (lambda [/dev/stdin skip-header? #:dialect [dialect #false] #:strict? [strict? #false]]
    (if (input-port? /dev/stdin)
        (reverse (csv-read*/reversed /dev/stdin skip-header? (or dialect csv::rfc) strict?))
        (let ([/dev/csvin (csv-input-port /dev/stdin)])
          (dynamic-wind
           (λ [] (void))
           (λ [] (read-csv* /dev/csvin skip-header? #:dialect dialect #:strict? strict?))
           (λ [] (close-input-port /dev/csvin)))))))

(define in-csv : (-> CSV-StdIn Positive-Integer Boolean [#:dialect (Option CSV-Dialect)] [#:strict? Boolean] (Sequenceof (Vectorof CSV-Field)))
  (lambda [/dev/stdin n skip-header? #:dialect [dialect #false] #:strict? [strict? #false]]
    (in-csv-port /dev/stdin (assert n index?) skip-header? (or dialect csv::rfc) strict?)))

(define in-csv* : (-> CSV-StdIn Boolean [#:dialect (Option CSV-Dialect)] [#:strict? Boolean] (Sequenceof (Pairof CSV-Field (Listof CSV-Field))))
  (lambda [/dev/stdin skip-header? #:dialect [dialect #false] #:strict? [strict? #false]]
    (in-csv-port* /dev/stdin skip-header? (or dialect csv::rfc) strict?)))

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
