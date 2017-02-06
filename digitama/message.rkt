#lang digimon

(provide (all-defined-out))

(require typed/db/base)

(define-type Schema-Message msg:schema)

(struct exn:schema exn:fail:sql () #:extra-constructor-name make-exn:schema)
(struct msg:schema msg:log ([maniplation : Symbol]) #:prefab)

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

(define make-schema-message : (-> (U Struct-TypeTop Symbol) Symbol (U SQL-Datum exn) Any * Schema-Message)
  (lambda [table maniplation urgent . messages]
    (define-values (level message info) (schema-message-smart-info urgent messages))
    (msg:schema level message info (if (symbol? table) table (value-name table)) maniplation)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define schema-message-smart-info : (-> (U SQL-Datum exn) (Listof Any) (Values Log-Level String Any))
  (lambda [urgent messages]
    (define (info++ [e : exn] [info : (Listof (Pairof Symbol Any))]) (cons (cons 'message (exn-message e)) info))
    (define-values (smart-level smart-brief)
      (cond [(exn? urgent) (values 'error (exn-message urgent))]
            [else (values 'info "")]))
    (values smart-level
            (cond [(null? messages) smart-brief]
                  [else (apply format (~a (car messages)) (cdr messages))])
            (cond [(not (exn? urgent)) urgent]
                  [(exn:schema? urgent) (info++ urgent (exn:fail:sql-info urgent))]
                  [(exn:fail:sql? urgent) (exn:fail:sql-info urgent)]
                  [else (info++ urgent (list (cons 'struct (object-name urgent))))]))))
