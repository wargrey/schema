#lang typed/racket/base

(provide (all-defined-out))

(require typed/db/base)

(require (for-syntax racket/base))
(require (for-syntax syntax/parse))

(define-syntax (require/provide stx)
  (syntax-case stx []
    [(_ spec ...)
     #'(begin (provide (all-from-out spec)) ...
              (require spec) ...)]))

(define-syntax (#%function stx) ; class method has a symbol name looks like "[name] method in [class%]"
  #'(let use-next-id : Symbol ([stacks (continuation-mark-set->context (current-continuation-marks))])
      (if (null? stacks) 'λ
          (or (caar stacks)
              (use-next-id (cdr stacks))))))

(define-syntax (throw stx)
  (syntax-parse stx
    [(_ st:id rest ...)
     #'(throw [st] rest ...)]
    [(_ [st:id argl ...] frmt:str v ...)
     #'(throw [st argl ...] (#%function) frmt v ...)]
    [(_ [st:id argl ...] src frmt:str v ...)
     #'(raise (st (format (string-append "~s: " frmt) src v ...) (current-continuation-marks) argl ...))]))

(define-syntax (schema-throw stx)
  (syntax-parse stx
    [(_ [st:id sqlstat info] frmt:str v ...)
     #'(schema-throw [st sqlstat info] (#%function) frmt v ...)]
    [(_ [st:id sqlstat info] src frmt:str v ...)
     #'(let ([message (format (string-append "~s: " frmt) src v ...)])
         (raise (st message (current-continuation-marks)
                    sqlstat (list* (cons 'code sqlstat)
                                   (cons 'message message)
                                   info))))]))

(define-type (Listof+ schema) (Pairof schema (Listof schema)))

(struct exn:schema exn:fail:sql () #:extra-constructor-name make-exn:schema)
