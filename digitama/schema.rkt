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
    [(_ tbl #:as Table #:with primary-key (~optional entity-attribute-mode) ([field : DataType constraints ...] ...)
        (~or (~optional (~seq #:check record-contract:expr) #:name "#:check" #:defaults ([record-contract #'#true]))
             (~optional (~seq #:serialize serialize) #:name "#:serialize")
             (~optional (~seq #:deserialize deserialize) #:name "#:deserialize")) ...)
     (with-syntax* ([___ (format-id #'id "...")]
                    [([table dbtable] Table-Rowid) (list (parse-table-name #'tbl) (format-id #'Table "~a-Rowid" #'Table))]
                    [(RowidType [rowid dbrowid] ...) (parse-primary-key #'primary-key)]
                    [eam (if (attribute entity-attribute-mode) (id->sql #'entity-attribute-mode) #'#false)]
                    [([view? table-rowid ...]
                      [(:field table-field field-contract FieldType MaybeNull on-update [defval ...] field-examples) ...]
                      [(column table-column DBType column-guard column-not-null column-unique) ...]
                      [table? table-row? table-rowid-ref msg:schema:table make-table-message table->hash hash->table table-examples
                              force-create force-insert check-record]
                      [unsafe-table make-table remake-table create-table insert-table delete-table in-table select-table update-table])
                     (let ([pkids (let ([pk (syntax->datum #'primary-key)]) (if (list? pk) pk (list pk)))]
                           [tablename (syntax-e #'table)]
                           [eam? (and (syntax-e #'eam) #true)])
                       (define-values (sdleif snmuloc sdiwor)
                         (for/fold ([sdleif null] [snmuloc null] [sdiwor null])
                                   ([stx (in-syntax #'([field DataType constraints ...] ...))])
                           (define-values (pk-info field-info column-info) (parse-field-definition tablename pkids eam? stx))
                           (cond [(void? column-info) (values (cons field-info sdleif) snmuloc sdiwor)]
                                 [else (values (cons field-info sdleif)
                                               (cons column-info snmuloc)
                                               (if pk-info (cons pk-info sdiwor) sdiwor))])))
                       (list (cons (< (length sdiwor) (length pkids)) (reverse sdiwor)) (reverse sdleif) (reverse snmuloc)
                             (for/list ([fmt (in-list (list "~a?" "~a-row?" "~a-rowid" "msg:schema:~a"
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
                    [serialize (or (attribute serialize) #'(λ [[raw : Table]] : SQL-Datum (~s (table->hash raw))))]
                    [deserialize (or (attribute deserialize) #'(λ [[raw : SQL-Datum]] : Table (hash->table (read:+? raw hash?))))]
                    [contract-literals #'(list 'field-contract ... 'record-contract)]
                    [define-table-rowid (cond [(syntax-e #'view?) #'(void)]
                                              [else #'(define (table-rowid-ref [self : Table]) : RowidType
                                                        (vector (table-rowid self) ...))])])
       #'(begin (define-type Table table)
                (define-type Table-Rowid RowidType)
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
                                   dbc dbtable '(dbrowid ...) eam '(column ...) '(DBType ...)
                                   '(column-not-null ...) '(column-unique ...)))

                (define (insert-table [dbc : Connection] [selves : (U Table (Listof Table))]
                                      #:or-replace? [replace? : Boolean #false]) : Void
                  (do-insert-table (and view? 'insert-table) 'insert-table (and replace? 'force-insert) dbtable eam '(column ...)
                                   dbc (if (table? selves) (in-value selves) (in-list selves))
                                   (list table-column ...) serialize))
                
                (define (delete-table [dbc : Connection] [selves : (U Table (Listof Table))]) : Void
                  (do-delete-from-table 'delete-table view? dbtable '(dbrowid ...)
                                        dbc (if (table? selves) (in-value selves) (in-list selves))
                                        (list table-rowid ...)))

                (define-syntax (select-table stx) (syntax-case stx [] [(_ argl ___) #'(sequence->list (in-table argl ___))]))
                (define (in-table [dbc : Connection]
                                  #:where [where : (U RowidType (Pairof String (Listof Any)) False) #false]
                                  #:fetch [size : (U Positive-Integer +inf.0) +inf.0]) : (Sequenceof (U Table exn))
                  (define (read-row [fields : (Listof SQL-Datum)]) : Table
                    (apply unsafe-table (check-selected-row 'select-table 'table table-row? fields (list column-guard ...))))
                  (do-select-table 'in-table 'select-table dbtable where '(dbrowid ...) eam '(column ...)
                                   deserialize read-row dbc size))

                (define (update-table [dbc : Connection] [selves : (U Table (Listof Table))]
                                      #:check-first? [check? : Boolean #true]) : Void
                  (do-update-table 'update-table view? 'table (and check? 'check-record) dbtable '(dbrowid ...) eam '(column ...)
                                   dbc (if (table? selves) (in-value selves) (in-list selves))
                                   (list table-column ...) (list table-rowid ...) serialize))))]))

(define-syntax (define-schema stx)
  (syntax-parse stx
    [(_ Table-Datum (define-table id #:as ID rest ...) ...)
     #'(begin (define-type Table-Datum (U ID ...))
              (define-table id #:as ID rest ...) ...)]))
