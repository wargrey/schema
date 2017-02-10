#lang racket/base

(provide (all-defined-out))

(require (only-in db in-query))
(require racket/sequence)

(define in-query-rows
  (lambda [dbc size statement [argl null]]
    (sequence-map (λ multi-columns multi-columns)
                  (cond [(null? argl) (in-query dbc statement #:fetch size)]
                        [else (apply in-query dbc statement #:fetch size argl)]))))
