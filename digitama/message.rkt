#lang typed/racket/base

(provide (all-defined-out))
(provide (rename-out [object-name struct-name]))

(require typed/db/base)

(require/typed racket/base
               [object-name (-> (U Struct-TypeTop exn) Symbol)])

(require (for-syntax racket/base))
(require (for-syntax syntax/parse))

(define-syntax (struct: stx)
  (syntax-case stx [:]
    [(_ id : ID rest ...)
     #'(begin (struct id rest ... #:prefab)
              (define-type ID id))]))

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

(define-type Continuation-Stack (Pairof Symbol (Option (Vector (U String Symbol) Integer Integer))))

(struct exn:schema exn:fail:sql () #:extra-constructor-name make-exn:schema)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define rest->message : (-> (Listof Any) String)
  (lambda [messages]
    (cond [(null? messages) ""]
          [else (apply format (format "~a" (car messages)) (cdr messages))])))

(define exn->info : (-> exn (Listof (Pairof Symbol Any)))
  (let ([info++ (λ [[e : exn] [info : (Listof (Pairof Symbol Any))]] (cons (cons 'message (exn-message e)) info))])
    (lambda [e]
      (cond [(exn:schema? e) (info++ e (exn:fail:sql-info e))]
            [(exn:fail:sql? e) (exn:fail:sql-info e)]
            [else (info++ e (list (cons 'struct (object-name e))))]))))

(define continuation-mark->stacks : (->* () ((U Continuation-Mark-Set Thread)) (Listof Continuation-Stack))
  (lambda [[cm (current-continuation-marks)]]
    ((inst map (Pairof Symbol (Option (Vector (U String Symbol) Integer Integer))) (Pairof (Option Symbol) Any))
     (λ [[stack : (Pairof (Option Symbol) Any)]]
       (define maybe-srcinfo (cdr stack))
       (cons (or (car stack) 'λ)
             (and (srcloc? maybe-srcinfo)
                  (let ([src (srcloc-source maybe-srcinfo)]
                        [line (srcloc-line maybe-srcinfo)]
                        [column (srcloc-column maybe-srcinfo)])
                    (vector (if (symbol? src) src (format "~a" src))
                            (or line -1)
                            (or column -1))))))
     (cond [(continuation-mark-set? cm) (continuation-mark-set->context cm)]
           [else (continuation-mark-set->context (continuation-marks cm))]))))
