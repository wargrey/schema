#lang digimon

(provide (all-defined-out) (struct-out exn:fail:sql))
(provide (struct-out exn:schema) make-schema-message Schema-Message)

(require typed/db/base)
(require "digitama/message.rkt")

(define-type Schema-Message msg:schema)

(struct msg:query msg:log ([rows : (Listof (Vectorof SQL-Datum))]) #:prefab)

(define make-schema-message : (-> (U Struct-TypeTop Symbol) Symbol (U SQL-Datum exn) Any * Schema-Message)
  (lambda [table maniplation urgent . messages]
    (define-values (level message info) (schema-message-smart-info urgent messages))
    (msg:schema level message info (if (symbol? table) table (value-name table)) maniplation)))

(define make-query-message : (-> Connection Statement Any Symbol SQL-Datum * Log-Message)
  (lambda [dbc sql detail topic . argl]
    (with-handlers ([exn? exn:schema->message])
      (msg:query 'info (~a sql) detail topic
                 (apply query-rows dbc sql argl)))))

(define exn:schema->message : (-> exn [#:level Log-Level] Log-Message)
  (lambda [e #:level [level #false]]
    (cond [(not (exn:fail:sql? e)) (exn->message e #:level (or level 'error))]
          [else (exn->message e #:detail (exn:fail:sql-info e) #:level (or level 'error))])))

(define exn:sql-info-ref : (->* ((U exn:fail:sql Log-Message) Symbol) ((-> Any Any)) Any)
  (lambda [e key [-> values]]
    (define info : (Listof (Pairof Any Any))
      (cond [(exn:fail:sql? e) (exn:fail:sql-info e)]
            [else (let ([detail : Any (msg:log-detail e)])
                    (if (list? detail) (filter pair? detail) null))]))
    (define pinfo : (Option (Pairof Any Any)) (assq key info))
    (and pinfo (-> (cdr pinfo)))))
