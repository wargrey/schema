#lang typed/racket/base

(provide (all-defined-out))

(require racket/format)
(require typed/db/base)

(require "../misc.rkt")
(require "../normalize.rkt")

(define-predicate sql-datum? SQL-Datum)

(define racket->sql-pk : (-> Any SQL-Datum)
  (lambda [v]
    (cond [(sql-datum? v) v]
          [else (~s v)])))

(define racket->sql : (-> Any Symbol SQL-Datum)
  (lambda [v dbname]
    (cond [(not v) sql-null]
          [(boolean? v) (if (memq dbname '(mysql sqlite3)) 1 v)]
          [(sql-datum? v) v]
          [else (~s v)])))

;;; TODO: how to deal with `Boolean`?
(define sql->racket : (->* (SQL-Datum) ((-> String Any)) Any)
  (lambda [v [->racket void]]
    (cond [(sql-null? v) #false]
          [(string? v) (->racket v)]
          [else v])))
