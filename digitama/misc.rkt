#lang typed/racket/base

(provide (all-defined-out))

(require typed/db/base)

(require (for-syntax racket/base))
(require (for-syntax syntax/parse))

(define-syntax (require/provide stx)
  (syntax-case stx []
    [(_ spec ...)
     (syntax/loc stx
       (begin (provide (all-from-out spec)) ...
              (require spec) ...))]))

(define-syntax (#%function stx) ; class method has a symbol name looks like "[name] method in [class%]"
  (syntax/loc stx
    (let use-next-id : Symbol ([stacks (continuation-mark-set->context (current-continuation-marks))])
      (if (null? stacks) 'λ
          (or (caar stacks)
              (use-next-id (cdr stacks)))))))

(define-syntax (make-schema-error stx)
  (syntax-parse stx
    [(_ [st:id sqlstat info] frmt:str v ...)
     (syntax/loc stx (make-schema-error [st sqlstat info] (#%function) frmt v ...))]
    [(_ [st:id sqlstat info] src frmt:str v ...)
     (syntax/loc stx
       (let ([message (format (string-append "~s: " frmt) src v ...)])
         (st message (current-continuation-marks)
             sqlstat (list* (cons 'code sqlstat)
                            (cons 'message message)
                            info))))]))

(define-syntax (throw stx)
  (syntax-parse stx
    [(_ st:id rest ...)
     (syntax/loc stx (throw [st] rest ...))]
    [(_ [st:id argl ...] frmt:str v ...)
     (syntax/loc stx (throw [st argl ...] (#%function) frmt v ...))]
    [(_ [st:id argl ...] src frmt:str v ...)
     (syntax/loc stx (raise (st (format (string-append "~s: " frmt) src v ...) (current-continuation-marks) argl ...)))]))

(define-syntax (schema-throw stx)
  (syntax-parse stx
    [(_ [st:id sqlstat info] frmt:str v ...)
     (syntax/loc stx (raise (make-schema-error [st sqlstat info] frmt v ...)))]
    [(_ [st:id sqlstat info] src frmt:str v ...)
     (syntax/loc stx (raise (make-schema-error [st sqlstat info] src frmt v ...)))]))

(define-type (Listof+ schema) (Pairof schema (Listof schema)))

(struct exn:schema exn:fail:sql () #:extra-constructor-name make-exn:schema)
