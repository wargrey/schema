#lang typed/racket

(provide (all-defined-out))
(provide Schema-Serialize Schema-Deserialize)
(provide table->racket racket->table)

(require "../message.rkt")
(require "syntax.rkt")
(require "shadow.rkt")
(require "misc.rkt")

(require "exchange/base.rkt")
(require "exchange/racket.rkt")
(require "exchange/sql.rkt")

(require (for-syntax racket/base))
(require (for-syntax syntax/parse))
(require (for-syntax racket/syntax))
(require (for-syntax racket/sequence))

(define-type Schema schema)
(struct schema () #:transparent)

(define-syntax (define-table stx)
  (syntax-parse stx #:datum-literals [:]
    [(_ tbl #:as Table #:with primary-key
        (~optional (~seq #:order-by order-by) #:defaults ([order-by #'#false]))
        ([field : DataType constraints ...] ...)
        (~or (~optional (~seq #:check record-contract:expr) #:name "#:check" #:defaults ([record-contract #'#true]))) ...)
     (with-syntax* ([(table dbtable) (parse-table-name #'tbl)]
                    [(RowidType [rowid dbrowid] ...) (parse-primary-key #'primary-key)]
                    [default-order-by (parse-order-by #'order-by (map syntax-e (syntax->list #'(field ...))) #'tbl)]
                    [([view? table-rowid ...]
                      [(FieldDatum :field table-field list-table-field FieldType field-contract on-update [defval ...] field-examples
                                   dbfield DBType field-guard not-null unique) ...]
                      [#%Table Table-List Table-Field]
                      [table? table-list? #%table make-table-message make-table->message table->hash hash->table
                              table->list list->table table->row row->table]
                      [unsafe-table make-table remake-table create-table insert-table delete-table update-table in-table select-table seek-table]
                      [table-serialize table-deserialize table-aggregate table-examples])
                     (let ([pkids (let ([pk (syntax->datum #'primary-key)]) (if (list? pk) pk (list pk)))]
                           [TableName (syntax-e #'Table)]
                           [tablename (syntax-e #'table)])
                       (define-values (sdleif sdiwor)
                         (for/fold ([sdleif null] [sdiwor null])
                                   ([stx (in-syntax #'([field DataType constraints ...] ...))])
                           (define-values (maybe-pkref field-info) (parse-field-definition tablename pkids stx))
                           (values (cons (cons #'SQL-Datum field-info) sdleif)
                                   (if maybe-pkref (cons maybe-pkref sdiwor) sdiwor))))
                       (list (cons (< (length sdiwor) (length pkids)) (reverse sdiwor))
                             (reverse sdleif)
                             (for/list ([fmt (in-list (list "#%~a" "~a-List" "~a-Field"))])
                               (format-id #'Table fmt TableName))
                             (for/list ([fmt (in-list (list "~a?" "~a-list?" "#%~a" "make-~a-message" "make-~a->message"
                                                            "~a->hash" "hash->~a" "~a->list" "list->~a" "~a->row" "row->~a"))])
                               (format-id #'table fmt tablename))
                             (for/list ([prefix (in-list (list 'unsafe 'make 'remake 'create 'insert 'delete 'update 'in 'select 'seek))])
                               (format-id #'table "~a-~a" prefix tablename))
                             (for/list ([suffix (in-list (list 'serialize 'deserialize 'aggregate 'examples))])
                               (format-id #'table "~a-~a" tablename suffix))))]
                    [([mkargs ...] [reargs ...])
                     (for/fold ([syns (list null null)])
                               ([:fld (in-syntax #'(:field ...))]
                                [mkarg (in-syntax #'([field : FieldType defval ...] ...))]
                                [rearg (in-syntax #'([field : (U FieldType Void) on-update] ...))])
                       (list (cons :fld (cons mkarg (car syns)))
                             (cons :fld (cons rearg (cadr syns)))))]
                    [contract-literals #'(list 'field-contract ... 'record-contract)]
                    [table-rowid-body (if (syntax-e #'view?)
                                          #'(throw exn:fail:unsupported '#%table "temporary view has no primary keys")
                                          #'(vector (racket->sql-pk (table-rowid self)) ...))])
       #'(begin (define-type Table table)
                (define-type #%Table RowidType)
                (define-type Table-List (List FieldType ...))
                (define-type Table-Field (U 'field ...))
                (struct table schema ([field : FieldType] ...) #:transparent #:constructor-name unsafe-table)
                (define-predicate Table? table)
                (define-predicate table-list? Table-List)
                
                (define #%table : (-> Table RowidType)
                  (lambda [self]
                    table-rowid-body))
                
                (define (make-table #:unsafe? [unsafe? : Boolean #false] mkargs ...) : Table
                  (when (not unsafe?)
                    (check-constraint 'make-table 'table '(field ...) contract-literals
                                      (list field-contract ... record-contract)
                                      (list field ...)))
                  (unsafe-table field ...))
                
                (define (remake-table [self : (Option Table)] #:unsafe? [unsafe? : Boolean #false] reargs ...) : Table
                  (let ([field ((inst field-value Table FieldType FieldType)
                                'remake-table 'field self table-field field (thunk (void) defval ...))]
                        ...)
                    (when (not unsafe?)
                      (check-constraint 'remake-table 'table '(field ...) contract-literals
                                        (list field-contract ... record-contract)
                                        (list field ...)))
                    (unsafe-table field ...)))
                
                (define table->list : (-> Table Table-List)
                  (lambda [self]
                    (list (table-field self) ...)))
                
                (define list->table : (-> (Listof Any) [#:unsafe? Boolean] [#:alt-fname Symbol] [#:alt-message String] Table)
                  (lambda [metrics #:unsafe? [unsafe? #false]
                                   #:alt-fname [fname 'list->table]
                                   #:alt-message [errmsg "unexpected source metrics"]]
                    (cond [(table-list? metrics)
                           (when (not unsafe?)
                             (match-let ([(list field ...) metrics]) ;;; extract fields for user defined constraints
                               (check-constraint fname 'table '(field ...) contract-literals
                                                 (list field-contract ... record-contract) metrics)))
                           (apply unsafe-table metrics)]
                          [else (schema-throw [exn:schema 'assertion `((struct . table) (got . ,metrics))] fname "~a" errmsg)])))
                
                (define table->row : (->* (Table) (Symbol) (Vector FieldDatum ...))
                  (lambda [self [dbname 'sqlite3]]
                    (vector (racket->sql (table-field self) dbname) ...)))
                
                (define row->table : (-> (U (Listof SQL-Datum) (Vectorof SQL-Datum))
                                         [#:strict? Boolean] [#:alt-fname Symbol] [#:alt-message String] Table)
                  (lambda [metrics #:strict? [strict? #false]
                                   #:alt-fname [fname 'row->table]
                                   #:alt-message [errmsg "maybe the database is penetrated"]]
                    (list->table #:unsafe? (not strict?) #:alt-fname fname #:alt-message errmsg
                                 (for/list : (Listof Any) ([sql (if (list? metrics) (in-list metrics) (in-vector metrics))]
                                                           [guard (list field-guard ...)])
                                   (sql->racket sql guard)))))
                
                (define table->hash : (-> Table [#:skip-null? Boolean] (Immutable-HashTable Symbol (U FieldType ...)))
                  (lambda [self #:skip-null? [skip? #true]]
                    ((inst make-dict (U FieldType ...))
                     '(field ...) (list (table-field self) ...) skip?)))
                
                (define hash->table : (-> HashTableTop [#:unsafe? Boolean] Table)
                  (lambda [src #:unsafe? [unsafe? #false]]
                    (list->table #:unsafe? unsafe? #:alt-fname 'hash->table
                                 (dict->record 'hash->table src
                                               '(field ...)
                                               (list (thunk (void) defval ...) ...)))))
                  
                (define table-serialize : (->* (Table) (Schema-Serialize) Bytes)
                  (lambda [self [serialize table->racket]]
                    (define v : Any (serialize 'table '(field ...) (list (table-field self) ...)))
                    (cond [(bytes? v) v]
                          [(string? v) (string->bytes/utf-8 v)]
                          [else (string->bytes/utf-8 (~s v))])))
                  
                (define table-deserialize : (->* (Bytes) (Schema-Deserialize #:unsafe? Boolean) Table)
                  (lambda [src [deserialize racket->table] #:unsafe? [unsafe? #false]]
                    (list->table #:unsafe? unsafe? #:alt-fname 'table-deserialize
                                 (deserialize 'table src '(field ...) (list (thunk (void) defval ...) ...)))))
                  
                (define make-table-message : (-> Symbol (U Table exn) Schema-Serialize [#:urgent Any] Schema-Message)
                  (lambda [manipulation occurrence [serialize table->racket] #:urgent [urgent #false]]
                    (cond [(exn? occurrence) (exn->schema-message occurrence 'table manipulation)]
                          [else (make-schema-message #:urgent (or urgent (if view? (#%table occurrence) +nan.0))
                                                     'table manipulation (table-serialize occurrence serialize))])))
                  
                (define make-table->message : (-> Symbol Schema-Serialize [#:urgent Any] (-> (U Table exn) Schema-Message))
                  (lambda [manipulation [serialize table->racket] #:urgent [urgent #false]]
                    (λ [[occurrence : (U Table exn)]]
                      (make-table-message manipulation occurrence serialize #:urgent urgent))))
                  
                (define create-table : (-> Connection [#:if-not-exists? Boolean] Void)
                  (lambda [dbc #:if-not-exists? [silent? #false]]
                    (do-create-table 'create-table view? silent? dbc
                                     dbtable '(dbrowid ...) '(dbfield ...) '(DBType ...)
                                     '(not-null ...) '(unique ...))))
                  
                (define insert-table : (-> Connection (U Table (Listof Table)) [#:or-replace? Boolean] Void)
                  (lambda [dbc selves #:or-replace? [replace? #false]]
                    (do-insert-table 'insert-table view? replace? dbtable '(dbfield ...) dbc
                                     (if (table? selves) (in-value selves) (in-list selves))
                                     (list table-field ...))))
                  
                (define delete-table : (-> Connection (U Table (Listof Table)) Void)
                  (lambda [dbc selves]
                    (do-delete-from-table 'delete-table view? dbtable '(dbrowid ...) dbc
                                          (if (table? selves) (in-value selves) (in-list selves))
                                          (list table-rowid ...))))
                  
                (define select-table : (-> Connection
                                           [#:where (U RowidType (Pairof String (Listof Any)) False)]
                                           [#:fetch (U Positive-Integer +inf.0)]
                                           [#:order-by (Option Table-Field)] [#:asc? Boolean]
                                           [#:limit Natural] [#:offset Natural]
                                           (Listof (U Table exn)))
                  (let ([mkrow : (-> (Listof SQL-Datum) Table) (λ [src] (row->table src #:strict? #false #:alt-fname 'select-table))])
                    (lambda [dbc #:where [where #false] #:fetch [size +inf.0]
                                 #:order-by [order-field 'default-order-by] #:asc? [asc? #true]
                                 #:limit [limit 0] #:offset [offset 0]]
                      (sequence->list (do-select-table dbtable where '(dbrowid ...) '(dbfield ...) mkrow
                                                       dbc size order-field asc? limit offset)))))
                  
                (define in-table : (-> Connection
                                       [#:where (U RowidType (Pairof String (Listof Any)) False)]
                                       [#:fetch (U Positive-Integer +inf.0)]
                                       [#:order-by (Option Table-Field)] [#:asc? Boolean]
                                       [#:limit Natural] [#:offset Natural]
                                       (Sequenceof (U Table exn)))
                  (let ([mkrow : (-> (Listof SQL-Datum) Table) (λ [src] (row->table src #:strict? #false #:alt-fname 'in-table))])
                    (lambda [dbc #:where [where #false] #:fetch [size +inf.0]
                                 #:order-by [order-field 'default-order-by] #:asc? [asc? #true]
                                 #:limit [limit 0] #:offset [offset 0]]
                      (do-select-table dbtable where '(dbrowid ...) '(dbfield ...) mkrow
                                       dbc size order-field asc? limit offset))))
                  
                (define seek-table : (-> Connection #:where (U RowidType (Pairof String (Listof Any))) (Option Table))
                  (let ([mkrow : (-> (Vectorof SQL-Datum) Table) (λ [src] (row->table src #:strict? #false #:alt-fname 'seek-table))])
                    (lambda [dbc #:where where]
                      (do-seek-table dbtable where '(dbrowid ...) '(dbfield ...) mkrow dbc))))
                  
                (define update-table : (-> Connection (U Table (Listof Table)) [#:check-first? Boolean] Void)
                  (lambda [dbc selves #:check-first? [check? #true]]
                    (do-update-table 'update-table view? 'table check? dbtable '(dbrowid ...) '(dbfield ...)
                                     dbc (if (table? selves) (in-value selves) (in-list selves))
                                     (list table-field ...) (list table-rowid ...))))
                  
                (define table-aggregate : (-> Connection Symbol Table-Field [#:distinct? Boolean] (Option (U Flonum Integer)))
                  (lambda [dbc function column #:distinct? [distinct? #false]]
                    (do-table-aggregate dbtable function column distinct? dbc)))
                  
                ;;; TODO: define a DSL for `where` clause
                (define list-table-field : (-> Connection
                                               [#:order-by (Option Table-Field)] [#:asc? Boolean]
                                               [#:limit Natural] [#:offset Natural]
                                               (Listof (U FieldType exn)))
                  (let ([fieldtype? (make-predicate FieldType)])
                    (lambda [dbc #:order-by [order-field 'default-order-by] #:asc? [asc? #true] #:limit [limit 0] #:offset [offset 0]]
                      (do-select-column 'list-table-field 'table 'field 'FieldType
                                        dbtable dbfield field-guard fieldtype?
                                        dbc order-field asc? limit offset))))
                  
                ...

                (define table-examples : (->* () ((Option Symbol)) (Listof Any))
                  (lambda [[fname #false]]
                    (case fname
                      [(field) (check-example field-examples (thunk (list defval ...)))] ...
                      [else (map table-examples '(field ...))])))))]))

(define-syntax (define-schema stx)
  (syntax-parse stx
    [(_ Table-Datum (define-table id #:as ID rest ...) ...)
     #'(begin (define-type Table-Datum (U ID ...))
              (define-table id #:as ID rest ...) ...)]))
