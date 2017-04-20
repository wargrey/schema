#lang typed/racket/base

(provide (all-defined-out))
(provide (struct-out exn:fail:sql) (struct-out exn:schema))

(require typed/db/base)
(require "digitama/message.rkt")

(struct: msg:schema : Schema-Message ([level : Log-Level] [message : String] [topic : Symbol]))

(struct: msg:schema:table : Schema-Table-Message msg:schema
  ([maniplation : Symbol]
   [raw : (U Bytes (Listof Bytes))]
   [urgent : Any]))

(struct: msg:schema:error : Schema-Error-Message msg:schema
  ([state : (U String Symbol)]
   [detail :(Listof (Pairof Symbol Any))]
   #;[stacks : (Listof Continuation-Stack)]))

(define make-schema-message : (-> (U Struct-TypeTop Symbol) Symbol (U Bytes (Listof Bytes)) Any Any * Schema-Message)
  (lambda [table maniplation raw urgent . messages]
    (cond [(exn? urgent) (exn->schema-message urgent table maniplation)]
          [else (msg:schema:table 'info (rest->message messages)
                                  (if (symbol? table) table (struct-name table))
                                  maniplation raw urgent)])))

(define exn->schema-message : (->* (exn) ((U Struct-TypeTop Symbol) Symbol #:level Log-Level) Schema-Error-Message)
  (lambda [e [table '||] [maniplation '||] #:level [level 'error]]
    (msg:schema:error level (exn-message e) (if (symbol? table) table (struct-name table))
                      (if (exn:fail:sql? e) (exn:fail:sql-sqlstate e) (struct-name e)) (exn->info e)
                      #;(continuation-mark->stacks (exn-continuation-marks e)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define exn:sql-info-ref : (->* ((U exn:fail:sql Schema-Error-Message) Symbol) ((-> Any Any)) Any)
  (lambda [e key [-> values]]
    (define info : (Listof (Pairof Any Any))
      (cond [(exn:fail:sql? e) (exn:fail:sql-info e)]
            [else (msg:schema:error-detail e)]))
    (define pinfo : (Option (Pairof Any Any)) (assq key info))
    (and pinfo (-> (cdr pinfo)))))
