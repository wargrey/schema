#lang digimon

(provide (all-defined-out) (struct-out msg:schema))

(require "virtual-sql.rkt")
(require "message.rkt")
(require "syntax.rkt")
(require "shadow.rkt")

(define-type Schema schema)
(struct schema () #:prefab)

(define-syntax (define-table stx)
  (syntax-parse stx #:datum-literals [:]
    [(_ tbl #:as Table #:with primary-key ([field : DataType constraints ...] ...)
        (~or (~optional (~seq #:check record-contract:expr) #:name "#:check" #:defaults ([record-contract #'#true]))) ...)
     (with-syntax* ([___ (format-id #'id "...")]
                    [([table dbtable] #%Table) (list (parse-table-name #'tbl) (format-id #'Table "#%~a" #'Table))]
                    [(RowidType [rowid dbrowid] ...) (parse-primary-key #'primary-key)]
                    [([view? table-rowid ...]
                      [(:field table-field field-contract FieldType MaybeNull on-update [defval ...] field-examples
                               dbfield DBType field-guard not-null unique) ...]
                      [table? table-row? #%table msg:schema:table make-table-message table->hash hash->table table-examples
                              force-create force-insert check-record]
                      [unsafe-table make-table remake-table create-table insert-table delete-table in-table select-table update-table])
                     (let ([pkids (let ([pk (syntax->datum #'primary-key)]) (if (list? pk) pk (list pk)))]
                           [tablename (syntax-e #'table)])
                       (define-values (sdleif sdiwor)
                         (for/fold ([sdleif null] [sdiwor null])
                                   ([stx (in-syntax #'([field DataType constraints ...] ...))])
                           (define-values (maybe-pkref field-info) (parse-field-definition tablename pkids stx))
                           (values (cons field-info sdleif) (if maybe-pkref (cons maybe-pkref sdiwor) sdiwor))))
                       (list (cons (< (length sdiwor) (length pkids)) (reverse sdiwor)) (reverse sdleif)
                             (for/list ([fmt (in-list (list "~a?" "~a-row?" "#%~a" "msg:schema:~a"
                                                            "make-~a-message" "~a->hash" "hash->~a" "~a-examples"
                                                            "create-~a-if-not-exists" "insert-~a-or-replace" "check-~a-rowid"))])
                               (format-id #'table fmt tablename))
                             (for/list ([prefix (in-list (list 'unsafe 'make 'remake 'create 'insert 'delete 'in 'select 'update))])
                               (format-id #'table "~a-~a" prefix tablename))))]
                    [([mkargs ...] [reargs ...])
                     (for/fold ([syns (list null null)])
                               ([:fld (in-syntax #'(:field ...))]
                                [mkarg (in-syntax #'([field : (U FieldType MaybeNull) defval ...] ...))]
                                [rearg (in-syntax #'([field : (U FieldType MaybeNull Void) on-update] ...))])
                       (list (cons :fld (cons mkarg (car syns)))
                             (cons :fld (cons rearg (cadr syns)))))]
                    [contract-literals #'(list 'field-contract ... 'record-contract)]
                    [define-table-rowid (cond [(syntax-e #'view?) #'(void)]
                                              [else #'(define (#%table [self : Table]) : RowidType
                                                        (vector (racket->sql-pk (table-rowid self)) ...))])])
       #'(begin (define-type Table table)
                (define-type #%Table RowidType)
                (struct table schema ([field : (U FieldType MaybeNull)] ...) #:prefab #:constructor-name unsafe-table)
                (struct msg:schema:table msg:schema ([occurrences : (U Table (Listof Table))]) #:prefab)
                (define-predicate table-row? (List (U FieldType MaybeNull) ...))

                define-table-rowid

                (define (make-table #:unsafe? [unsafe? : Boolean #false] mkargs ...) : Table
                  (when (not unsafe?)
                    (check-constraint 'make-table 'table '(field ...) contract-literals
                                      (list field-contract ... record-contract) field ...))
                  (unsafe-table field ...))

                (define (remake-table [self : (Option Table)] #:unsafe? [unsafe? : Boolean #false] reargs ...) : Table
                  (let ([field (field-value 'remake-table 'field self table-field field (thunk (void) defval ...))] ...)
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

                (: table-examples (->* () ((Option Symbol)) (Listof Any)))
                (define (table-examples [fname #false]) : (Listof Any)
                  (case fname
                    [(field) (check-example field-examples (thunk (list defval ...)))] ...
                    [else (map table-examples '(field ...))]))
                
                (define make-table-message : (case-> [Symbol -> (-> (U Table (Listof Table) exn) Any * Schema-Message)]
                                                     [Symbol (U Table (Listof Table) exn) Any * -> Schema-Message])
                  (case-lambda
                    [(maniplation) (λ [occurrences . messages] (apply make-table-message maniplation occurrences messages))]
                    [(maniplation occurrences . messages)
                     (if (exn? occurrences)
                         (let-values ([(level message info) (schema-message-smart-info occurrences messages)])
                           (msg:schema level message info 'table maniplation))
                         (let-values ([(level message info) (schema-message-smart-info #false messages)])
                           (msg:schema:table level message info 'table maniplation occurrences)))]))
                
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

                (define-syntax (select-table stx) (syntax-case stx [] [(_ argl ___) #'(sequence->list (in-table argl ___))]))
                (define (in-table [dbc : Connection]
                                  #:where [where : (U RowidType (Pairof String (Listof Any)) False) #false]
                                  #:fetch [size : (U Positive-Integer +inf.0) +inf.0]) : (Sequenceof (U Table exn))
                  (define (read-row [fields : (Listof SQL-Datum)]) : Table
                    (apply unsafe-table (check-selected-row 'select-table 'table table-row? fields (list field-guard ...))))
                  (do-select-table 'in-table 'select-table dbtable where '(dbrowid ...) '(dbfield ...) read-row dbc size))

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
