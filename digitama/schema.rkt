#lang digimon

(provide (all-defined-out))

(require typed/db/base)

(require "virtual-sql.rkt")
(require "message.rkt")

(require (for-syntax "normalize.rkt"))

(define-type Schema schema)
(struct schema () #:prefab)

(define-syntax (define-table stx)
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
                       (and (attribute unique) #'#true))))]))
  (syntax-parse stx #:datum-literals [:]
    [(_ tbl #:as Table #:with primary-key (~optional index-only) ([field : DataType constraints ...] ...)
        (~or (~optional (~seq #:check record-contract:expr) #:name "#:check" #:defaults ([record-contract #'#true]))
             (~optional (~seq #:serialize serialize) #:name "#:serialize")
             (~optional (~seq #:deserialize deserialize) #:name "#:deserialize")) ...)
     (with-syntax* ([(rowid ___) (list (id->sql #'primary-key) (format-id #'id "..."))]
                    [(table dbtable) (syntax-parse #'tbl [r:id (list #'r (id->sql #'r))] [(r db) (list #'r (id->sql #'db))])]
                    [racket (if (attribute index-only) (id->sql #'index-only) #'#false)]
                    [([(table-rowid RowidType) (:field table-field field-contract FieldType MaybeNull on-update [defval ...]) ...]
                      [(column-id column table-column DBType column-guard column-not-null column-unique) ...]
                      [check-fields table.sql] [table? table-row? msg:schema:table make-table-message table->hash hash->table]
                      [unsafe-table make-table remake-table create-table insert-table delete-table in-table select-table update-table])
                     (let ([tablename (syntax-e #'table)]
                           [pkname (syntax-e #'primary-key)]
                           [racket? (and (syntax-e #'racket) #true)])
                       (define-values (sdleif snmuloc rowid-info)
                         (for/fold ([sdleif null] [snmuloc null] [rowid-info #false])
                                   ([stx (in-syntax #'([field DataType constraints ...] ...))])
                           (define-values (pk-info field-info column-info) (parse-field-definition tablename pkname racket? stx))
                           (cond [(void? column-info) (values (cons field-info sdleif) snmuloc rowid-info)]
                                 [else (values (cons field-info sdleif) (cons column-info snmuloc) (or rowid-info pk-info))])))
                       (list (cons (or rowid-info (list #'#false #'Any)) (reverse sdleif)) (reverse snmuloc)
                             (for/list ([idx (in-range 2)]) (datum->syntax #'table (gensym tablename)))
                             (for/list ([fmt (in-list (list "~a?" "~a-row?" "msg:schema:~a" "make-~a-message" "~a->hash" "hash->~a"))])
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
                    [deserialize (or (attribute deserialize) #'(λ [[raw : SQL-Datum]] : Table (hash->table (read:+? raw hash?))))])
       #'(begin (define-type Table table)
                (struct table schema ([field : (U FieldType MaybeNull)] ...) #:prefab #:constructor-name unsafe-table)
                (struct msg:schema:table msg:schema ([occurrences : (U Table (Listof Table))]) #:prefab)
                (define-predicate table-row? (List (U FieldType MaybeNull) ...))
                (define table.sql : (HashTable Symbol Statement) (make-hasheq))
                
                (define-syntax (check-fields stx)
                  (syntax-case stx []
                    [(_ func field ...)
                     #'(unless (and field-contract ... record-contract)
                         (define expected : (Listof Any)
                           (for/list ([result (in-list (list record-contract field-contract ...))]
                                      [expected (in-list (list 'record-contract 'field-contract ...))]
                                      #:when (false? result)) expected))
                         (define given : HashTableTop
                           (let ([?fields (remove-duplicates (filter symbol? (flatten expected)))])
                             (for/hasheq ([f (in-list (list 'field ...))] [v (in-list (list field ...))] #:when (memq f ?fields))
                               (values f v))))
                         (throw [exn:schema 'contract `((struct . table) (expected . ,(~s expected)) (given . ,given))]
                                func "constraint violation"))]))

                (define (make-table #:unsafe? [unsafe? : Boolean #false] mkargs ...) : Table
                  (when (not unsafe?) (check-fields 'make-table field ...))
                  (unsafe-table field ...))

                (define (remake-table [self : (Option Table)] reargs ...) : Table
                  (let ([field : (U FieldType MaybeNull)
                               (cond [(not (void? field)) field]
                                     [(table? self) (table-field self)]
                                     [else (let ([?dv (list defval ...)])
                                             (when (null? ?dv) (error 'remake-table "missing value for field '~a'" 'field))
                                             (car ?dv))])] ...)
                    (check-fields 'remake-table field ...)
                    (unsafe-table field ...)))

                (define (table->hash [self : Table] #:skip-null? [skip? #true]) : (HashTable Symbol (U FieldType ... MaybeNull ...))
                  (if (not skip?)
                      ((inst make-immutable-hasheq Symbol (U FieldType ... MaybeNull ...)) (list (cons 'field (table-field self)) ...))
                      (for/hasheq : (HashTable Symbol (U FieldType ... MaybeNull ...))
                        ([key (in-list (list 'field ...))]
                         [val (in-list (list (table-field self) ...))]
                         #:when val)
                        (values key val))))

                (define (hash->table [src : HashTableTop] #:unsafe? [unsafe? : Boolean #false]) : Table
                  (define record : (Listof Any)
                    (list (hash-ref src 'field
                                    (thunk (let ([?dv (list defval ...)])
                                             (when (null? ?dv) (error 'hash->table "missing value for field '~a'" 'field))
                                             (car ?dv)))) ...))
                  (cond [(not (table-row? record)) (error 'hash->table "mismatched source")]
                        [(and unsafe?) (apply unsafe-table record)]
                        [else (match-let ([(list field ...) record])
                                (check-fields 'hash->table field ...)
                                (unsafe-table field ...))]))
                
                (define make-table-message : (case-> [Symbol -> (-> (U Table (Listof Table) exn) Any * Schema-Message)]
                                                     [Symbol (U Table (Listof Table) exn) Any * -> Schema-Message])
                  (case-lambda
                    [(maniplation)
                     (λ [occurrences . messages]
                       (if (exn? occurrences)
                           (let-values ([(level message info) (schema-message-smart-info occurrences messages)])
                             (msg:schema level message info 'table maniplation))
                           (let-values ([(level message info) (schema-message-smart-info #false messages)])
                             (msg:schema:table level message info 'table maniplation occurrences))))]
                    [(maniplation occurrences . messages)
                     (if (exn? occurrences)
                         (let-values ([(level message info) (schema-message-smart-info occurrences messages)])
                           (msg:schema level message info 'table maniplation))
                         (let-values ([(level message info) (schema-message-smart-info #false messages)])
                           (msg:schema:table level message info 'table maniplation occurrences)))]))
                
                (define (create-table [dbc : Connection] #:if-not-exists? [silent? : Boolean #false]) : Void
                  (when (false? table-rowid) (throw exn:fail:unsupported 'create-table "cannot create a temporary view"))
                  (define (virtual.sql) : Virtual-Statement
                    (create-table.sql silent? dbtable rowid racket '(column ...) '(DBType ...)
                                      '(column-not-null ...) '(column-unique ...)))
                  (query-exec dbc (hash-ref! table.sql (if silent? 'create-if-not-exists 'create) virtual.sql)))

                (define (insert-table [dbc : Connection] [selves : (U Table (Listof Table))]
                                      #:or-replace? [replace? : Boolean #false]) : Void
                  (define (virtual.sql) : Virtual-Statement (insert-into.sql replace? dbtable racket '(column ...)))
                  (when (false? table-rowid) (throw exn:fail:unsupported 'insert-table "cannot insert records into a temporary view"))
                  (define dbsys : Symbol (dbsystem-name (connection-dbsystem dbc)))
                  (define insert.sql : Statement (hash-ref! table.sql (if replace? 'replace 'insert) virtual.sql))
                  (for ([row (if (list? selves) (in-list selves) (in-value selves))])
                    (define column-id : SQL-Datum (column-guard 'column-id (table-column row) dbsys)) ...
                    (cond [(false? racket) (query-exec dbc insert.sql column-id ...)]
                          [else (query-exec dbc insert.sql column-id ... (serialize row))])))
       
                (define (delete-table [dbc : Connection] [selves : (U Table (Listof Table))]) : Void
                  (define (virtual.sql) : Virtual-Statement (delete.sql dbtable rowid))
                  (cond [(false? table-rowid) (throw exn:fail:unsupported 'delete-table "cannot delete records from a temporary view")]
                        [else (for ([self (if (list? selves) (in-list selves) (in-value selves))])
                                (query-exec dbc (hash-ref! table.sql 'delete-table-by-rowid virtual.sql) (table-rowid self)))]))

                (define-syntax (select-table stx) (syntax-case stx [] [(_ argl ___) #'(sequence->list (in-table argl ___))]))
                (define (in-table [dbc : Connection]
                                  #:where [where : (U False SQL-Datum) #false]
                                  #:fetch [size : (U Positive-Integer +inf.0) 1]) : (Sequenceof (U Table exn))
                  (define (virtual.sql [which : Symbol]) : (-> Virtual-Statement)
                    (thunk (simple-select.sql which dbtable rowid racket '(column ...))))
                  (define (read-table [sexp : SQL-Datum]) : (U Table exn)
                    (with-handlers ([exn? (λ [[e : exn]] e)])
                      (deserialize sexp)))
                  (define (read-by-pk [pk : SQL-Datum]) : (U Table exn)
                    (with-handlers ([exn? (λ [[e : exn]] e)])
                      (define record : (Listof SQL-Datum)
                        (for/list ([row (in-vector (query-row dbc (hash-ref table.sql 'select-row (virtual.sql 'row)) pk))])
                          (if (sql-null? row) #false row)))
                      (cond [(table-row? record) (apply unsafe-table record)]
                            [else (throw [exn:schema 'assertion `((struct . table) (record . ,pk) (got . ,record))]
                                         'select-table "maybe the database is penetrated")])))
                  (define-values (key fmap) (if (and racket) (values 'select-racket read-table) (values 'select-rowid read-by-pk)))
                  (define raw : (Sequenceof SQL-Datum)
                    (cond [(false? where) (in-query dbc #:fetch size (hash-ref table.sql key (virtual.sql 'nowhere)))]
                          [else (in-query dbc #:fetch size (hash-ref table.sql 'select-where-rowid (virtual.sql 'byrowid)) where)]))
                  (sequence-map fmap raw))

                (define (update-table [dbc : Connection] [selves : (U Table (Listof Table))]
                                      #:check-first? [check? : Boolean #true]) : Void
                  (define (virtual.sql [ensure? : Boolean]) : (-> Virtual-Statement)
                    (thunk (cond [(not ensure?) (update.sql dbtable rowid racket '(column ...))]
                                 [else (simple-select.sql 'ckrowid dbtable rowid racket '(column ...))])))
                  (cond [(false? table-rowid) (throw exn:fail:unsupported 'update-table "cannot update records of a temporary view")]
                        [else (let ([dbsys (dbsystem-name (connection-dbsystem dbc))])
                                (define update.sql : Statement (hash-ref! table.sql 'update (virtual.sql #false)))
                                (define check.sql : Statement (hash-ref! table.sql 'check-rowid (virtual.sql #true)))
                                (for ([self (if (list? selves) (in-list selves) (in-value selves))])
                                  (define pk : RowidType (table-rowid self))
                                  (when (and check? (false? (query-maybe-value dbc check.sql pk)))
                                    (throw [exn:schema 'norow `((struct . table) (record . ,pk))]
                                           'update "no such record found in the table"))
                                  (define column-id : SQL-Datum (column-guard 'column-id (table-column self) dbsys)) ...
                                  (cond [(false? racket) (query dbc update.sql column-id ... pk)]
                                        [else (query dbc update.sql column-id ... (serialize self) pk)])))]))))]))

(define-syntax (define-schema stx)
  (syntax-parse stx
    [(_ Table-Datum (define-table id #:as ID rest ...) ...)
     #'(begin (define-type Table-Datum (U ID ...))
              (define-table id #:as ID rest ...) ...)]))
