#lang typed/racket/base

(provide (all-defined-out))
(provide (rename-out [object-name struct-name]))

(require typed/db/base)
(require typed/racket/unsafe)

(require "misc.rkt")

(require (for-syntax racket/syntax))

(unsafe-require/typed racket/base
                      [object-name (-> (U Struct-TypeTop exn) Symbol)])

(require (for-syntax racket/base))

(define-syntax (struct: stx)
  (syntax-case stx [:]
    [(_ id : ID rest ...)
     (with-syntax ([ID? (format-id #'id "~a?" (syntax-e #'ID))])
       #'(begin (define-type ID id)
                (struct id rest ... #:transparent)
                (define-predicate ID? id)))]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define rest->message : (-> (U String (Pairof String (Listof Any))) String)
  (lambda [messages]
    (cond [(string? messages) ""]
          [else (apply format (format "~a" (car messages)) (cdr messages))])))

(define exn->info : (-> exn (Listof (Pairof Symbol Any)))
  (let ([info++ (λ [[e : exn] [info : (Listof (Pairof Symbol Any))]] (cons (cons 'message (exn-message e)) info))])
    (lambda [e]
      (cond [(exn:schema? e) (info++ e (exn:fail:sql-info e))]
            [(exn:fail:sql? e) (exn:fail:sql-info e)]
            [else (info++ e (list (cons 'struct (object-name e))))]))))
