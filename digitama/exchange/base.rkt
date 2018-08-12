#lang typed/racket/base

(provide (all-defined-out))

(require racket/list)

(require "../misc.rkt")

(define-type Schema-Serialize (-> Symbol (Listof Symbol) (Listof Any) Any))
(define-type Schema-Deserialize (-> Symbol Bytes (Listof Symbol) (Listof (-> Any)) (Listof Any)))

(define check-constraint : (-> Symbol Symbol (Listof Symbol) (Listof Any) (Listof Any) (Listof Any) Void)
  (lambda [func table fields literals contracts givens]
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
                    func "constraint violation"))))

(define check-default-value : (All (a) (-> Symbol Symbol (U a Void) a))
  (lambda [func field defval]
    (when (void? defval) (error func "missing value for field '~a'" field))
    defval))

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
