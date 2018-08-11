#lang typed/racket/base

(provide (all-defined-out))

(require racket/list)

(require typed/db)

(require "../virtual-sql.rkt")
(require "../misc.rkt")

(define-type Schema-Serialize (-> Symbol (Listof Symbol) (Listof Any) Any))
(define-type Schema-Deserialize (-> Symbol Bytes (Listof Symbol) (Listof (-> Any)) (Listof Any)))

(define check-constraint : (-> Symbol Symbol (Listof Symbol) (Listof Any) (Listof Any) Any * Void)
  (lambda [table fields literals contracts  . givens]
    (when (memq #false contracts)
      (define expected : (Listof Any)
        (for/list ([result (in-list contracts)]
                   [expected (in-list literals)]
                   #:when (not result))
          expected))
      (define ?fields : (Listof Symbol) (remove-duplicates (filter symbol? (flatten expected))))
      (define given : HashTableTop
        (for/hasheq ([f (in-list fields)]
                     [v (in-list givens)]
                     #:when (memq f ?fields))
            (values f v)))
      (schema-throw [exn:schema 'contract `((struct . ,table) (expected . ,expected) (given . ,given))]
                    table "constraint violation"))))

(define check-default-value : (All (a) (-> Symbol Symbol (U a Void) a))
  (lambda [func field defval]
    (when (void? defval) (error func "missing value for field '~a'" field))
    defval))

(define check-selected-row : (All (a) (-> Symbol Symbol (-> Any Boolean : #:+ a) (Listof SQL-Datum) (Listof (-> String Any)) a))
  (lambda [func table table-row? fields guards]
    (define metrics : (Listof Any) (map sql->racket fields guards))
    (cond [(table-row? metrics) metrics]
          [else (schema-throw [exn:schema 'assertion `((struct . ,table) (got . ,metrics))]
                              func "maybe the database is penetrated")])))

(define check-row : (All (a) (-> Symbol (Listof Any) (-> Any Boolean : #:+ a) String Any * a))
  (lambda [func metrics table-row? errfmt . errmsg]
    (cond [(table-row? metrics) metrics]
          [else (apply error func errfmt errmsg)])))

(define check-example : (-> Any (-> (Listof Any)) (Listof Any))
  (lambda [example mkdefval]
    (cond [(null? example) (mkdefval)]
          [(list? example) example]
          [else (list example)])))

(define field-value : (All (a b c) (-> Symbol Symbol (Option a) (-> a b) (U b Void) (-> (U c Void)) (U b c)))
  (lambda [func field self table-field value mkdefval]
    (cond [(not (void? value)) value]
          [(not self) (check-default-value func field (mkdefval))]
          [else (table-field self)])))
