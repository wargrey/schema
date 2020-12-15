#lang typed/racket

(provide (all-from-out typed/db/base))
(provide (for-syntax (all-defined-out)))
(provide (for-syntax (all-from-out "normalize.rkt")))

(require typed/db/base)

(require (for-syntax racket/list))
(require (for-syntax syntax/parse))
(require (for-syntax racket/syntax))
(require (for-syntax racket/symbol))

(require (for-syntax "normalize.rkt"))

(begin-for-syntax 
  (define (parse-field-definition tablename rowids stx)
    (syntax-parse stx
      [(field Type (~or (~optional (~seq #:check contract:expr) #:name "#:check")
                        (~optional (~or (~seq #:default defval) (~seq #:auto generate)) #:name "#:default or #:auto")
                        (~optional (~seq #:guard guard) #:name "#:guard")
                        (~optional (~seq (~and #:not-null not-null)) #:name "#:not-null")
                        (~optional (~seq (~and #:unique unique)) #:name "#:unique")
                        (~optional (~seq #:% comments) #:name "#:%")) ...)
       (define-values (DataType SQLType)
         (syntax-parse #'Type
           [(Racket #:as SQL) (values #'Racket (id->sql #'SQL 'raw))]
           [Racket:id (values #'Racket (id->sql #'Racket 'type))]
           [Racket (values #'Racket #'"VARCHAR")]))
       (define-values (primary? not-null?) (values (and (member (syntax-e #'field) rowids) #true) (attribute not-null)))
       (define table-field (format-id #'field "~a-~a" tablename (syntax-e #'field)))
       (define ?table-field (format-id #'field "?~a-~a" tablename (syntax-e #'field)))
       (values (and primary? table-field)
               (list (datum->syntax #'field (string->keyword (symbol->immutable-string (syntax-e #'field))))
                     table-field ?table-field
                     (if (or primary? (attribute not-null)) DataType #`(Option #,DataType))
                     (or (attribute contract) #'#true)
                     (or (attribute generate) #'(void))
                     (cond [(attribute defval) #'(defval)]
                           [(attribute generate) #'(generate)]
                           [(or primary? not-null?) #'()]
                           [else (syntax-case DataType [Listof]
                                   [(Listof _) #'(null)]
                                   [_ #'(#false)])])
                     (or (attribute comments) #'null)
                     (id->sql #'field)
                     SQLType
                     (or (attribute guard)
                         (syntax-case DataType [String Symbol]
                           [String #'values]
                           [Symbol #'string->symbol]
                           [_ #'(λ [[sql : String]] (call-with-input-string sql read))]))
                     (and not-null? #'#true)
                     (and (attribute unique) #'#true)))]))

  (define (parse-table-name stx)
    (syntax-parse stx
      [r:id (list #'r (id->sql #'r))]
      [(r db) (list #'r (id->sql #'db))]))
  
  (define (parse-primary-key stx)
    ;;; NOTE
    ; 1. primary keys may not contained in the defining struct in which case the struct is treated as a temporary view
    ; 2. (U SQL-Datum (Vector SQL-Datum)) makes typed racket hard...
    (syntax-parse stx
      [id:id (list #'(Vector SQL-Datum) (list #'id (id->sql #'id)))]
      [(id0 id ...) (let* ([ids (syntax->list #'(id0 id ...))])
                      (cons (datum->syntax stx (cons 'Vector (make-list (length ids) 'SQL-Datum)))
                            (map (λ [<id>] (list <id> (id->sql <id>))) ids)))]))

  (define (parse-order-by stx fields <table>)
    (define (order-id <order> fields)
      (define order (syntax-e <order>))
      (cond [(memq order fields) order]
            [else (raise-syntax-error 'parse-order-by "#:order-by column is not defined" <order>)]))
    (when (null? fields)
      (raise-syntax-error 'define-table "a table or view should have at least one column" <table>))
    (syntax-parse stx
      [id:id (datum->syntax stx (order-id #'id fields))]
      [sexp #'#false])))
