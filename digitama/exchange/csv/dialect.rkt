#lang typed/racket/base

(provide (all-defined-out))

(struct CSV-Dialect
  ([delimiter : Char]
   [quote-char : (Option Char)]
   [escape-char : (Option Char)]
   [comment-char : (Option Char)]
   [skip-leading-space? : Boolean]
   [skip-trailing-space? : Boolean])
  #:constructor-name unsafe-csv-dialect
  #:transparent)

(define make-csv-dialect : (-> [#:delimiter Char] [#:quote-char (Option Char)] [#:escape-char (Option Char)] [#:comment-char (Option Char)]
                               [#:skip-leading-space? Boolean] [#:skip-trailing-space? Boolean]
                               CSV-Dialect)
  (let ([<eq?>-char? : (-> (Option Char) Boolean) (λ [ch] (or (not ch) (char<? ch #\Ā)))])
    (lambda [#:delimiter [<:> #\,] #:quote-char [</> #\"] #:escape-char [<\> #false] #:comment-char [<#> #false]
             #:skip-leading-space? [trim-left? #false] #:skip-trailing-space? [trim-right? #false]]
      (assert <:> <eq?>-char?)
      (assert </> <eq?>-char?)
      (assert <\> <eq?>-char?)
      (assert <#> <eq?>-char?)
      
      (unsafe-csv-dialect <:> </> (if (eq? </> <\>) #false <\>) <#> trim-left? trim-right?))))

(define csv::rfc : CSV-Dialect (make-csv-dialect))
(define csv::unix : CSV-Dialect (make-csv-dialect #:delimiter #\: #:quote-char #false #:comment-char #\# #:escape-char #\\))
