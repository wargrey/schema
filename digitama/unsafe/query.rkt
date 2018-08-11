#lang typed/racket/base

(provide (all-defined-out))

(require typed/db/base)
(require typed/racket/unsafe)

(module unsafe racket/base
  (provide (all-defined-out))
  
  (require racket/sequence)
  (require (only-in db in-query))

  (define in-query-rows
    (lambda [dbc size statement [argl null]]
      (sequence-map (λ multi-columns multi-columns)
                    (cond [(null? argl) (in-query dbc statement #:fetch size)]
                          [else (apply in-query dbc statement #:fetch size argl)])))))


(unsafe-require/typed/provide
 (submod "." unsafe)
 [in-query-rows (->* (Connection (U Positive-Integer +inf.0) Statement)
                     ((Listof SQL-Datum))
                     (Sequenceof (Listof SQL-Datum)))])


  