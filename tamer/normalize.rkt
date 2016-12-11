#lang typed/racket

(require (for-syntax "../digitama/normalize.rkt"))

(define-syntax (normalize stx)
  (syntax-case stx []
    [(_ name ...)
     (with-syntax ([(dbname ...) (for/list ([n (in-list (syntax->list #'(name ...)))]) (name->sql (syntax-e n)))])
       #'(begin (displayln dbname) ...))]))

(normalize
 sqlite-master
 tableof-key/values
 deleted?)
