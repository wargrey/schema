#lang typed/racket/base

(provide (all-defined-out))

(define-type Numeric Integer)
(define-type Decimal Integer)

(define-type Nan Flonum-Nan)
(define-type Infinity +inf.0)
(define-type -Infinity -inf.0)

(define-type Character Char)
(define-type Varchar String)
(define-type BLOB Bytes)
