#lang typed/racket

(provide (for-syntax (all-defined-out)))
(provide (for-syntax (all-from-out "normalize.rkt")))

(require (for-syntax syntax/parse))
(require (for-syntax racket/syntax))

(require (for-syntax "normalize.rkt"))

(require "virtual-sql.rkt")

(begin-for-syntax 
  (define (parse-field-definition tablename rowid racket? stx)
    (syntax-parse stx
      [(field Type (~or (~optional (~seq #:check contract:expr) #:name "#:check")
                        (~optional (~or (~seq #:default defval) (~seq #:auto generate)) #:name "#:default or #:auto")
                        (~optional (~seq #:guard guard) #:name "#:guard")
                        (~optional (~seq (~and #:not-null not-null)) #:name "#:not-null")
                        (~optional (~seq (~and #:unique unique)) #:name "#:unique")
                        (~optional (~seq (~and #:hide hide)) #:name "#:hide")
                        (~optional (~seq #:% comment) #:name "#:%")) ...)
       (define-values (DataType SQLType)
         (syntax-parse #'Type
           [(R #:as SQL) (values #'R (id->sql #'SQL 'raw))]
           [R:id (values #'R (id->sql #'R 'type))]
           [R (values #'R #'"VARCHAR")]))
       (define-values (primary-field? not-null?) (values (eq? (syntax-e #'field) rowid) (attribute not-null)))
       (define table-field (format-id #'field "~a-~a" tablename (syntax-e #'field)))
       (values (and primary-field? (list table-field DataType))
               (list (datum->syntax #'field (string->keyword (symbol->string (syntax-e #'field))))
                     table-field (if (attribute contract) #'contract #'#true)
                     DataType (if (or primary-field? (attribute not-null)) DataType #|DataType may not builtin|# #'False)
                     (if (attribute generate) #'generate #'(void))
                     (cond [(attribute defval) #'(defval)]
                           [(attribute generate) #'(generate)]
                           [(or primary-field? not-null?) #'()]
                           [else (syntax-case DataType [Listof]
                                   [(Listof _) #'(null)]
                                   [_ #'(#false)])]))
               (unless (and racket? (attribute hide))
                 (list #'field (id->sql #'field)
                       table-field SQLType
                       (or (attribute guard) #'racket->sql)
                       (and not-null? #'#true)
                       (and (attribute unique) #'#true))))])))
