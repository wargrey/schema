#lang typed/racket

(provide (all-defined-out))

(require "../message.rkt")
(require "virtual-sql.rkt")
(require "syntax.rkt")
(require "shadow.rkt")
(require "misc.rkt")

(require (for-syntax racket/base))
(require (for-syntax syntax/parse))
(require (for-syntax racket/syntax))
(require (for-syntax racket/sequence))

(define-type Schema schema)
(struct schema () #:transparent)

(define-syntax (define-table stx)
  (syntax-parse stx #:datum-literals [:]
    [(_ tbl #:as Table #:with primary-key ([field : DataType constraints ...] ...)
        (~or (~optional (~seq #:check record-contract:expr) #:name "#:check" #:defaults ([record-contract #'#true]))) ...)
     (with-syntax* ([([table dbtable] #%Table) (list (parse-table-name #'tbl) (format-id #'Table "#%~a" #'Table))]
                    [(RowidType [rowid dbrowid] ...) (parse-primary-key #'primary-key)]
                    [([view? table-rowid ...]
                      [(:field table-field field-contract FieldType MaybeNull on-update [defval ...] field-examples
                               dbfield DBType field-guard not-null unique) ...]
                      [Table? table? table-row? #%table make-table-message make-table->message table->hash hash->table
                              table-serialize table-deserialize table-examples force-create force-insert check-record]
                      [unsafe-table make-table remake-table create-table insert-table delete-table update-table
                                    in-table select-table seek-table])
                     (let ([pkids (let ([pk (syntax->datum #'primary-key)]) (if (list? pk) pk (list pk)))]
                           [tablename (syntax-e #'table)])
                       (define-values (sdleif sdiwor)
                         (for/fold ([sdleif null] [sdiwor null])
                                   ([stx (in-syntax #'([field DataType constraints ...] ...))])
                           (define-values (maybe-pkref field-info) (parse-field-definition tablename pkids stx))
                           (values (cons field-info sdleif) (if maybe-pkref (cons maybe-pkref sdiwor) sdiwor))))
                       (list (cons (< (length sdiwor) (length pkids)) (reverse sdiwor)) (reverse sdleif)
                             (cons (format-id #'table "~a?" (syntax-e #'Table))
                                   (for/list ([fmt (in-list (list "~a?" "~a-row?" "#%~a" "make-~a-message" "make-~a->message"
                                                                  "~a->hash" "hash->~a" "~a-serialize" "~a-deserialize" "~a-examples"
                                                                  "create-~a-if-not-exists" "insert-~a-or-replace" "check-~a-rowid"))])
                                     (format-id #'table fmt tablename)))
                             (for/list ([prefix (in-list (list 'unsafe 'make 'remake 'create 'insert 'delete 'update 'in 'select 'seek))])
                               (format-id #'table "~a-~a" prefix tablename))))]
                    [([mkargs ...] [reargs ...])
                     (for/fold ([syns (list null null)])
                               ([:fld (in-syntax #'(:field ...))]
                                [mkarg (in-syntax #'([field : (U FieldType MaybeNull) defval ...] ...))]
                                [rearg (in-syntax #'([field : (U FieldType MaybeNull Void) on-update] ...))])
                       (list (cons :fld (cons mkarg (car syns)))
                             (cons :fld (cons rearg (cadr syns)))))]
                    [contract-literals #'(list 'field-contract ... 'record-contract)]
                    [table-rowid-body (if (syntax-e #'view?)
                                          #'(throw exn:fail:unsupported '#%table "temporary view has no primary keys")
                                          #'(vector (racket->sql-pk (table-rowid self)) ...))])
       #'(begin (define-type Table table)
                (define-type #%Table RowidType)
                (struct table schema ([field : (U FieldType MaybeNull)] ...) #:transparent #:constructor-name unsafe-table)
                (define-predicate Table? table)
                (define-predicate table-row? (List (U FieldType MaybeNull) ...))

                (define (#%table [self : Table]) : RowidType
                  table-rowid-body)

                (define (make-table #:unsafe? [unsafe? : Boolean #false] mkargs ...) : Table
                  (when (not unsafe?)
                    (check-constraint 'make-table 'table '(field ...) contract-literals
                                      (list field-contract ... record-contract) field ...))
                  (unsafe-table field ...))

                (define (remake-table [self : (Option Table)] #:unsafe? [unsafe? : Boolean #false] reargs ...) : Table
                  (let ([field ((inst field-value Table (U FieldType MaybeNull) (U FieldType MaybeNull))
                                'remake-table 'field self table-field field (thunk (void) defval ...))]
                        ...)
                    (when (not unsafe?)
                      (check-constraint 'remake-table 'table '(field ...) contract-literals
                                        (list field-contract ... record-contract) field ...))
                    (unsafe-table field ...)))

                (define (table->hash [self : Table] #:skip-null? [skip? : Boolean #true])
                  : (HashTable Symbol (U FieldType ... MaybeNull ...))
                  ((inst make-dict (U FieldType ... MaybeNull ...))
                   '(field ...) (list (table-field self) ...) skip?))

                (define (hash->table [src : HashTableTop] #:unsafe? [unsafe? : Boolean #false]) : Table
                  (define record (dict->record 'hash->table src '(field ...) (list (thunk (void) defval ...) ...) table-row?))
                  (cond [(and unsafe?) (apply unsafe-table record)]
                        [else (match-let ([(list field ...) record])
                                (check-constraint 'hash->table 'table '(field ...) contract-literals
                                                  (list field-contract ... record-contract) field ...)
                                (unsafe-table field ...))]))

                (define (table-serialize [self : Table]) : Bytes
                  (string->bytes/utf-8
                   (~s (hash-set (table->hash self #:skip-null? #false)
                                 '|.| 'table))))

                (define (table-deserialize [src : Bytes] #:unsafe? [unsafe? : Boolean #false]) : Table
                  (define maybe : Any (read (open-input-bytes src)))
                  (cond [(not (hash? maybe))
                         (schema-throw [exn:schema 'assertion `((struct . table) (got . ,src))]
                                       'table-deserialize "not an occurrence of ~a" 'table)]
                        [(not (table-dict? maybe '(|.| field ...)))
                         (schema-throw [exn:schema 'assertion `((struct . table) (got . ,src))]
                                       'table-deserialize "not an accurate occurrence of ~a" 'table)]
                        [else (hash->table maybe #:unsafe? unsafe?)]))
                
                (: table-examples (->* () ((Option Symbol)) (Listof Any)))
                (define (table-examples [fname #false])
                  (case fname
                    [(field) (check-example field-examples (thunk (list defval ...)))] ...
                    [else (map table-examples '(field ...))]))

                (define (make-table-message [manipulation : Symbol] [occurrence : (U Table exn)]
                                            [->bytes : (-> Table Bytes) table-serialize]
                                            #:urgent [urgent : Any #false]) : Schema-Message
                  (cond [(exn? occurrence) (exn->schema-message occurrence 'table manipulation)]
                        [else (make-schema-message #:urgent (or urgent (if view? (#%table occurrence) +nan.0))
                                                   'table manipulation (->bytes occurrence))]))
                
                (define (make-table->message [manipulation : Symbol] [->bytes : (-> Table Bytes) table-serialize]
                                             #:urgent [urgent : Any #false]) : (-> (U Table exn) Schema-Message)
                  (λ [[occurrence : (U Table exn)]]
                    (make-table-message manipulation occurrence ->bytes #:urgent urgent)))
                
                (define (create-table [dbc : Connection] #:if-not-exists? [silent? : Boolean #false]) : Void
                  (do-create-table (and view? 'create-table) 'create-table (and silent? 'force-create)
                                   dbc dbtable '(dbrowid ...) '(dbfield ...) '(DBType ...) '(not-null ...) '(unique ...)))

                (define (insert-table [dbc : Connection] [selves : (U Table (Listof Table))]
                                      #:or-replace? [replace? : Boolean #false]) : Void
                  (do-insert-table (and view? 'insert-table) 'insert-table (and replace? 'force-insert) dbtable '(dbfield ...)
                                   dbc (if (table? selves) (in-value selves) (in-list selves)) (list table-field ...)))
                
                (define (delete-table [dbc : Connection] [selves : (U Table (Listof Table))]) : Void
                  (do-delete-from-table 'delete-table view? dbtable '(dbrowid ...)
                                        dbc (if (table? selves) (in-value selves) (in-list selves)) (list table-rowid ...)))

                (define-syntax (select-table stx) (syntax-case stx [] [(_ argl [... ...]) #'(sequence->list (in-table argl [... ...]))]))
                (define (in-table [dbc : Connection]
                                  #:where [where : (U RowidType (Pairof String (Listof Any)) False) #false]
                                  #:fetch [size : (U Positive-Integer +inf.0) +inf.0]) : (Sequenceof (U Table exn))
                  (define (read-row [fields : (Listof SQL-Datum)]) : Table
                    (apply unsafe-table (check-selected-row 'select-table 'table table-row? fields (list field-guard ...))))
                  (do-select-table 'in-table 'select-table dbtable where '(dbrowid ...) '(dbfield ...) read-row dbc size))

                (define (seek-table [dbc : Connection] #:where [where : (U RowidType (Pairof String (Listof Any)))]) : (Option Table)
                  (define (read-row [fields : (Listof SQL-Datum)]) : Table
                    (apply unsafe-table (check-selected-row 'seek-table 'table table-row? fields (list field-guard ...))))
                  (do-seek-table 'select-table dbtable where '(dbrowid ...) '(dbfield ...) read-row dbc))

                (define (update-table [dbc : Connection] [selves : (U Table (Listof Table))]
                                      #:check-first? [check? : Boolean #true]) : Void
                  (do-update-table 'update-table view? 'table (and check? 'check-record) dbtable '(dbrowid ...) '(dbfield ...)
                                   dbc (if (table? selves) (in-value selves) (in-list selves))
                                   (list table-field ...) (list table-rowid ...)))))]))

(define-syntax (define-schema stx)
  (syntax-parse stx
    [(_ Table-Datum (define-table id #:as ID rest ...) ...)
     #'(begin (define-type Table-Datum (U ID ...))
              (define-table id #:as ID rest ...) ...)]))
